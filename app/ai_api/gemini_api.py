"""
===============================================================================
文件描述与核心功能梳理 (阅读指南)
===============================================================================
本文件是一个针对 Google Gemini API (新版 google-genai SDK) 的高级封装模块。
主要实现了多模态（文本、图片、视频）的 API 调用，并具备高并发场景下的稳定性保障。

【核心处理的文件及其作用说明】：
1. config/config_google.json (外部读取)
   - 作用：存储 Google API Key 的配置文件，支持多个账号和多个 Key。
2. api_key_usage.json & api_key_usage.json.lock (自动生成)
   - 作用：实现多进程/多线程安全的 API Key 负载均衡。
   - 原理：每次调用 API 前，通过文件排他锁读取此统计文件，挑出“使用次数最少”的 Key，
     增加计数后写回。这避免了单 Key 触发并发限制或配额耗尽。
3. prohibited_videos.json & prohibited_videos.json.lock (自动生成)
   - 作用：违禁视频黑名单缓存。
   - 原理：如果某个视频（如 test.mp4）上传后被 Google 判定为违规内容 (PROHIBITED_CONTENT)，
     该文件会将其路径记录下来。下次再遇到此视频时直接跳过，防止浪费 API Key 资源和时间。
4. 临时上传的视频文件 (Google Server 端)
   - 作用：视频过大，必须先调用 client.files.upload 上传至 Google 服务器，
     轮询等待其处理完成 (ACTIVE) 后再交由大模型分析，最后在 finally 块中删除以节省云端空间。

【核心业务逻辑】：
- 负载均衡与自动轮询：遇到配额超限(429/overloaded)时，自动休眠并切换下一个 Key。
- 代理保护：通过 @with_proxy 装饰器自动挂载本地代理。
- 安全策略绕过：默认将所有安全审查阈值设置为 BLOCK_NONE，避免模型过度拒绝回答。
===============================================================================
"""

import io
import os
import time
import json
import functools
from PIL import Image  # 仅用于类型提示与可选检测，不强依赖 PIL 的上传流程

from filelock import FileLock

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# 新版 SDK（pip install google-genai）
from google import genai
from google.genai import types

from common.common_utils import read_json, get_config, CONFIG_DIR, get_proxy_url

config_path = str(CONFIG_DIR / "gemini_web.json")
import base64 as _local_base64
import mimetypes as _local_mimetypes
import os as _local_os
import time as _local_time
from html import escape as _local_xml_escape

import requests as _local_requests
import base64 as _local_base64
import functools as _local_functools
import inspect as _local_inspect
import logging as _local_logging
import mimetypes as _local_mimetypes
import os as _local_os
import time as _local_time
import uuid as _local_uuid
from contextvars import ContextVar as _LocalContextVar
from html import escape as _local_xml_escape

# ==================== 本地接口配置（独立于 Google API Key） ====================
LOCAL_API_BASE_URL = _local_os.getenv(
    "GEMINI_LOCAL_API_BASE_URL",
    "http://127.0.0.1:8083/v1/chat/completions",
)
LOCAL_API_KEY = get_config("local_gemini_api_key")
LOCAL_MODELS = ("gpt-5.6", "gemini-pro-latest")

# ========== API Key 读取与管理 ==========

def build_api_key_map():
    google_config = read_json(config_path)
    cookie_list = google_config.get('cookie_list', [])

    api_key_map = {}
    accounts_with_keys_count = 0  # 记录带有 API Key 的账号数量

    for account_info in cookie_list:
        account_name = account_info.get('name', 'unknown_account')
        api_key_list = account_info.get('api_key_list', [])

        # 如果这个账号的 api_key_list 不为空，则有Key账号数 +1
        if api_key_list:
            accounts_with_keys_count += 1

        for index, api_key in enumerate(api_key_list):
            key = f'{account_name}_{index}' if index > 0 else account_name
            api_key_map[key] = api_key

    # 一行简洁的统计输出
    print(
        f"[INFO] 账号统计: 共 {len(cookie_list)} 个，其中 {accounts_with_keys_count} 个有Key，总计 {len(api_key_map)} 个 API Key。")

    return api_key_map


class ApiKeyManager:
    """
    通过原子性的“检出”操作，实现线程/进程安全的 API Key 负载均衡。
    """

    def __init__(self, api_key_map):
        self.api_key_map = api_key_map
        self.stats_file = config_path.replace('.json', '_api_key_usage.json')
        self.lock_file = self.stats_file + '.lock'
        # 增加超时以防高并发场景下的锁等待
        self.lock = FileLock(self.lock_file, timeout=20)
        self._initialize_stats()

    def _initialize_stats(self):
        with self.lock:
            if not os.path.exists(self.stats_file) or os.path.getsize(self.stats_file) == 0:
                initial_stats = {key: {} for key in self.api_key_map.keys()}
                with open(self.stats_file, 'w') as f:
                    json.dump(initial_stats, f, indent=4)

    def _read_stats_safely(self):
        """内部辅助函数，用于在锁内安全地读取和验证统计数据。"""
        try:
            with open(self.stats_file, 'r') as f:
                stats = json.load(f)
            # 兼容旧格式或修复损坏的数据
            if stats and isinstance(next(iter(stats.values()), None), int):
                raise TypeError("Old stats format detected. Resetting.")

            # 确保所有当前的key都存在于统计文件中
            for key in self.api_key_map.keys():
                if key not in stats or not isinstance(stats[key], dict):
                    stats[key] = {}
            return stats
        except (FileNotFoundError, json.JSONDecodeError, TypeError) as e:
            print(f"[WARN] 无法读取或解析统计文件 ({e})，正在重新初始化。")
            return {key: {} for key in self.api_key_map.keys()}

    def checkout_key(self, model_name: str) -> str | None:
        """
        原子性地获取并标记一个使用次数最少的 Key。这是解决并发问题的核心。
        """
        with self.lock:
            stats = self._read_stats_safely()

            # 1. 找到使用次数最少的 key (仅在当前配置的 api_key_map 中寻找)
            valid_keys = [k for k in self.api_key_map.keys()]
            if not valid_keys:
                return None

            selected_key = min(valid_keys, key=lambda k: stats.get(k, {}).get(model_name, 0))

            # 2. 立即增加其使用次数
            stats[selected_key][model_name] = stats.get(selected_key, {}).get(model_name, 0) + 1

            # 3. 写回文件
            with open(self.stats_file, 'w') as f:
                json.dump(stats, f, indent=4)

            print(
                f"[INFO] 检出 Key: '{selected_key}' 用于模型 '{model_name}'。新计数: {stats[selected_key][model_name]}. 时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")

            # 4. 返回被选中的 key
            return selected_key

    def get_ordered_keys(self, model_name: str):
        """
        获取一个基于当前使用计数的有序列表。
        注意：此方法本身不是为并发选择key而设计的，主要用于非并发场景或调试。
        """
        with self.lock:
            stats = self._read_stats_safely()

        # 仅对存在于当前配置中的 key 进行排序
        sorted_keys = sorted(
            [k for k in self.api_key_map.keys()],
            key=lambda k: stats.get(k, {}).get(model_name, 0)
        )
        print(f"[INFO] 针对模型 '{model_name}'，API 密钥当前的使用顺序 (仅供参考): {sorted_keys}")
        return sorted_keys

    def record_success(self, key_name: str, model_name: str):
        """
        在新的 checkout 模式下，此方法是多余的，因为计数已在检出时增加。
        保留此方法以兼容旧代码，但它不执行任何操作。
        """
        pass  # 在 checkout 模式下，计数在检出时完成。


# ==================== 新增：违禁视频管理器 ====================
class ProhibitedVideoManager:
    """
    通过文件锁，实现一个进程安全的、持久化的违禁视频路径列表。
    """

    def __init__(self):
        module_dir = os.path.dirname(os.path.abspath(__file__))
        self.list_file = os.path.join(module_dir, 'prohibited_videos.json')
        self.lock_file = self.list_file + '.lock'
        self.lock = FileLock(self.lock_file, timeout=20)
        self._initialize_list()

    def _initialize_list(self):
        """如果列表文件不存在，则创建一个包含空列表的初始文件。"""
        with self.lock:
            if not os.path.exists(self.list_file) or os.path.getsize(self.list_file) == 0:
                with open(self.list_file, 'w') as f:
                    json.dump([], f)

    def _read_list_safely(self) -> list:
        """在锁内安全地读取视频列表，处理文件不存在或格式错误的情况。"""
        try:
            with open(self.list_file, 'r') as f:
                data = json.load(f)
                # 确保读取到的是一个列表
                return data if isinstance(data, list) else []
        except (FileNotFoundError, json.JSONDecodeError):
            # 如果文件损坏或不存在，返回空列表
            return []

    def add_video(self, video_path: str):
        """原子性地将一个视频路径添加到违禁列表中。"""
        with self.lock:
            video_list = self._read_list_safely()
            # 确保不重复添加
            if video_path not in video_list:
                video_list.append(video_path)
                with open(self.list_file, 'w') as f:
                    json.dump(video_list, f, indent=4)
                print(f"[INFO] 已将违禁视频 '{video_path}' 添加到记录中。")

    def is_prohibited(self, video_path: str) -> bool:
        """原子性地检查一个视频路径是否在违禁列表中。"""
        with self.lock:
            video_list = self._read_list_safely()
            return video_path in video_list


API_KEY_MAP = build_api_key_map()
api_key_manager = ApiKeyManager(API_KEY_MAP)
# 新增：实例化违禁视频管理器，使其在整个应用中可用
prohibited_video_manager = ProhibitedVideoManager()


# ========== 统一的思考预算与调用工具函数 ==========

def build_generate_content_config(model_name: str | None) -> types.GenerateContentConfig:
    """
    统一生成 GenerateContentConfig：
    - 默认 thinking_budget=24567
    - 若 model_name 包含 'pro'（不区分大小写），则为 32678
    - 统一 response_mime_type 为 'text/plain'
    """
    budget = 24567
    if model_name and ('pro' in model_name.lower()):
        budget = 32678
    safety_settings = [
        types.SafetySetting(
            category=types.HarmCategory.HARM_CATEGORY_HARASSMENT,
            threshold=types.HarmBlockThreshold.BLOCK_NONE
        ),
        types.SafetySetting(
            category=types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
            threshold=types.HarmBlockThreshold.BLOCK_NONE
        ),
        types.SafetySetting(
            category=types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
            threshold=types.HarmBlockThreshold.BLOCK_NONE
        ),
        types.SafetySetting(
            category=types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
            threshold=types.HarmBlockThreshold.BLOCK_NONE
        )
    ]

    return types.GenerateContentConfig(
        thinking_config=types.ThinkingConfig(thinking_budget=budget),
        response_mime_type="text/plain",
        safety_settings=safety_settings
    )


def safe_generate_content(client: genai.Client, model: str, contents, config: types.GenerateContentConfig,
                          timeout: int | None = None):
    """
    统一模型调用，兼容部分 SDK 版本不接受 timeout 的情况。
    """
    try:
        if timeout is not None:
            return client.models.generate_content(model=model, contents=contents, config=config, timeout=timeout)
        else:
            return client.models.generate_content(model=model, contents=contents, config=config)
    except TypeError:
        # 某些版本不接受 timeout
        return client.models.generate_content(model=model, contents=contents, config=config)


def wait_until_file_ready(client: genai.Client, file_obj, poll_interval: int = 10):
    """
    轮询等待文件处理完成（PROCESSING -> ACTIVE/FAILED）。
    """
    while getattr(file_obj, "state", None) and getattr(file_obj.state, "name", None) == "PROCESSING":
        time.sleep(poll_interval)
        file_obj = client.files.get(name=file_obj.name)
    if getattr(file_obj.state, "name", None) == "FAILED":
        raise RuntimeError(f"文件处理失败：{getattr(file_obj, 'name', '未知')}")
    return file_obj


# ========== 代理装饰器（保持行为） ==========

def with_proxy(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        proxy_url = get_proxy_url()
        if proxy_url:
            os.environ['HTTP_PROXY'] = proxy_url
            os.environ['HTTPS_PROXY'] = proxy_url
        try:
            return func(*args, **kwargs)
        finally:
            if proxy_url:
                if 'HTTP_PROXY' in os.environ:
                    del os.environ['HTTP_PROXY']
                if 'HTTPS_PROXY' in os.environ:
                    del os.environ['HTTPS_PROXY']

    return wrapper


# ========== 业务函数（修改核心视频处理逻辑） ==========

@with_proxy
def get_llm_content_gemini_flash_video(
        prompt: str = '视频中的内容是什么',
        video_path: str = 'test.mp4',
        model_name: str = "gemini-flash-latest",
        max_attempts: int = 10
) -> str:
    # ==================== 新增：前置检查 ====================
    # 在处理前，先检查该视频是否已在违禁列表中
    if prohibited_video_manager.is_prohibited(video_path):
        error_msg = f"视频处理被跳过：'{video_path}' 已被标记为包含禁止内容。"
        print(f"[ERROR] {error_msg}")
        # 直接抛出异常，避免浪费资源
        raise ValueError(error_msg)
    # ======================================================

    last_error = None

    # 尝试次数不能超过可用 key 的数量
    num_available_keys = len(API_KEY_MAP)
    attempts = min(max_attempts, num_available_keys)

    for attempt in range(attempts):
        key_name = api_key_manager.checkout_key(model_name=model_name)
        if not key_name:
            print("[ERROR] 无法检出任何 API Key，停止尝试。")
            break

        api_key = API_KEY_MAP.get(key_name)
        # 这个检查理论上多余，因为 checkout_key 基于 API_KEY_MAP，但作为安全措施保留
        if not api_key:
            continue

        client = genai.Client(api_key=api_key)
        if not os.path.exists(video_path):
            return f"错误: 视频文件未找到 -> {video_path}"

        video_file = None
        try:
            # 日志更新以反映新的尝试逻辑
            print(
                f"[INFO] 第 {attempt + 1}/{attempts} 次尝试。使用 Key “{key_name}” prompt length: {len(prompt)} 上传视频… {model_name}， {video_path}")
            # 不再需要手动调用 record_success

            video_file = client.files.upload(file=video_path)
            video_file = wait_until_file_ready(client, video_file, poll_interval=10)

            config = build_generate_content_config(model_name)
            response = safe_generate_content(
                client=client,
                model=model_name,
                contents=[video_file, prompt],
                config=config,
                timeout=1200
            )
            if not response.text:
                # 将 response.prompt_feedback 转换为字符串进行通用检查
                feedback_str = str(response.prompt_feedback)
                if 'PROHIBITED_CONTENT' in feedback_str:
                    # 现在只要反馈信息中包含关键字，就能触发
                    print(f"[PROHIBITED_CONTENT] 检测到禁止内容于视频: {video_path}。正在记录并停止尝试。")
                    prohibited_video_manager.add_video(video_path)
                    # 抛出一个明确的异常，通知上层调用者这是一个不可恢复的错误
                    raise ValueError(f"PROHIBITED_CONTENT '{video_path}' contains prohibited content.")

                # 对于其他原因导致的空响应，保持原有逻辑，并确保返回字符串
                print(f"[WARN] 模型返回了空响应{feedback_str} {video_path}")
                return feedback_str
            # 成功则直接返回
            return response.text
        except Exception as e:
            if 'overloaded' in str(e) or 'An internal error has occurred' in str(e):
                last_error = e
                print(f"[WARN] Key “{key_name}” 调用失败：{e}，切换下一个…{video_path}")
                time.sleep(600)

                # 继续循环以检出下一个key
            else:
                print(f"[ERROR] Key “{key_name}” 调用失败：{e}，停止尝试。 {video_path}")
                raise e  # 对于不可恢复的错误，直接抛出
        finally:
            if video_file is not None:
                try:
                    print(f"[INFO] 删除临时文件 {video_file.name}…")
                    client.files.delete(name=video_file.name)
                except Exception as de:
                    print(f"[ERROR] 删除文件 {video_file.name} 失败：{de}")

    return f"所有 API Key 均尝试失败 ({attempts}次)。最后一次错误：{last_error} {video_path}"


def get_llm_content_gemini2flash(prompt: str = '你好，Gemini！请介绍一下你自己。') -> str:
    last_error = None
    model_name = "gemini-flash-latest"
    last_key_name = None

    # 尝试所有可用的key
    num_available_keys = len(API_KEY_MAP)
    for attempt in range(num_available_keys):
        key_name = api_key_manager.checkout_key(model_name=model_name)
        if not key_name:
            print("[ERROR] 无法检出任何 API Key，停止尝试。")
            break

        last_key_name = key_name
        api_key = API_KEY_MAP.get(key_name)
        if not api_key:
            continue
        try:
            print(f"[INFO] 正在使用名为 '{key_name}' 的 API Key... prompt length: {len(prompt)}")
            client = genai.Client(api_key=api_key)
            contents = [types.Content(role="user", parts=[types.Part.from_text(text=prompt)])]
            config = build_generate_content_config(model_name)
            response = safe_generate_content(client, model_name, contents, config, timeout=None)

            # 不再需要手动调用 record_success
            return response.text
        except Exception as e:
            if 'overloaded' in str(e) or 'An internal error has occurred' in str(e):
                last_error = e
                print(f"[WARN] 名为 '{key_name}' 的 API Key 调用失败: {e.__class__.__name__}. 正在尝试下一个...")
                time.sleep(600)

                continue
            else:
                print(f"[ERROR] 名为 '{key_name}' 的 API Key 调用失败: {e.__class__.__name__}. 停止尝试。 {e}")
                raise e
    return f"所有 API Key 均尝试失败。最后一次错误 (来自密钥 '{last_key_name}')：{last_error}"


def get_llm_content_sub(prompt: str = '你好，Gemini！请介绍一下你自己。',
                        model_name: str = "gemini-flash-latest") -> str:
    print(f"[INFO] 使用模型: {model_name}")
    last_error = None

    # 尝试所有可用的key
    num_available_keys = len(API_KEY_MAP)
    for attempt in range(num_available_keys):
        key_name = api_key_manager.checkout_key(model_name=model_name)
        if not key_name:
            print("[ERROR] 无法检出任何 API Key，停止尝试。")
            break

        api_key = API_KEY_MAP.get(key_name)
        if not api_key:
            continue
        try:
            # 不再需要手动调用 record_success
            print(f"[INFO] 正在使用名为 '{key_name}' 的 API Key... prompt length: {len(prompt)}")
            client = genai.Client(api_key=api_key)
            contents = [types.Content(role="user", parts=[types.Part.from_text(text=prompt)])]
            config = build_generate_content_config(model_name)
            response = safe_generate_content(client, model_name, contents, config, timeout=None)

            text = response.text
            if not text:
                print(f"模型返回了空响应{response.prompt_feedback}")
                return response.prompt_feedback
            return text
        except Exception as e:
            if 'overloaded' in str(e) or 'An internal error has occurred' in str(e):
                print(f"[WARN] 名为 '{key_name}' 的 API Key 调用失败: {e.__class__.__name__}. 正在尝试下一个... {e}")
                last_error = e
                time.sleep(600)

                continue
            else:
                print(f"[ERROR] 名为 '{key_name}' 的 API Key 调用失败: {e.__class__.__name__}. 停止尝试。 {e}")
                raise e
    raise last_error if last_error else Exception("所有 API Key 均尝试失败且未记录特定错误。")


@with_proxy
def get_llm_content(prompt: str = '你好，Gemini！请介绍一下你自己。', model_name: str = "gemini-flash-latest", back_model="gemini-flash-lite-latest") -> str | None:
    try:
        try:
            return get_llm_content_sub(prompt, model_name)
        except Exception as e1:
            print(f"[WARN] 主模型失败: {e1}")
            try:
                return get_llm_content_sub(prompt, back_model)
            except Exception as e2:
                print(f"[WARN] 备用模型失败: {e2}")
                return get_llm_content_gemini2flash(prompt)
    except Exception as e:
        print(f"[ERROR] 内容生成失败: {e}")
        print("[TIPS] 请检查以下内容：\n - API 密钥是否正确\n - 网络连接及代理设置\n - 是否安装了 `google-genai`")
        return None


@with_proxy
def valid_all_api_keys():
    """
    测试所有 API Key 的有效性。
    此函数按顺序测试，不涉及并发，因此使用 get_ordered_keys 是合适的。
    """
    failed_key_list = []
    success_key_list = []
    test_model =  "gemini-3.1-flash-lite"
    # 这里使用 get_ordered_keys 保持原样，以便按使用频率顺序测试
    ordered_keys = api_key_manager.get_ordered_keys(model_name=test_model)
    results = {}
    for key_name in ordered_keys:
        api_key = API_KEY_MAP.get(key_name)
        if not api_key:
            results[key_name] = "无效（未配置）"
            continue
        try:
            print(f"[TEST] 正在测试名为 '{key_name}' 的 API Key...")
            client = genai.Client(api_key=api_key)
            contents = [types.Content(role="user", parts=[types.Part.from_text(text="你好")])]
            config = build_generate_content_config(test_model)
            response = safe_generate_content(client, test_model, contents, config, timeout=None)
            results[key_name] = "有效"
            print(f"[SUCCESS] Key '{key_name}' 有效，模型响应: {response.text[:30]}...")
            success_key_list.append(key_name)
        except Exception as e:
            results[key_name] = f"无效 {api_key}（{e.__class__.__name__}: {e})"
            print(f"[FAIL] Key '{key_name}' 无效: {e}")
            failed_key_list.append(key_name)
    print("\n=== API Key 测试结果 ===")
    for k, v in results.items():
        print(f"- {k}: {v}")

    print(f"\n总计: {len(ordered_keys)} 个 Key, 成功: {len(success_key_list)}, 失败: {len(failed_key_list)}")
    print("失败的 Key 列表:", failed_key_list)
    print("成功的 Key 列表:", success_key_list)


@with_proxy
def analyze_images_gemini(
        prompt: str = '每张图片的内容是什么',
        image_paths=['a.jpg'],
        model_name="gemini-3-flash-preview"

) -> str:
    """
    分析本地图片内容。适配新版 SDK 与 ApiKeyManager。
    """
    # 默认使用 flash 模型处理图片，速度快且支持多模态

    last_error = None
    # 尝试次数限制为 Key 的数量
    num_available_keys = len(API_KEY_MAP)

    # 1. 预先校验文件是否存在
    valid_paths = []
    for path in image_paths:
        if not os.path.exists(path):
            return f"错误: 图片文件未找到 -> {path}"
        valid_paths.append(path)

    # 2. 循环尝试 Key
    for attempt in range(num_available_keys):
        # 检出 Key
        key_name = api_key_manager.checkout_key(model_name=model_name)
        if not key_name:
            print("[ERROR] 无法检出任何 API Key，停止尝试。")
            break

        api_key = API_KEY_MAP.get(key_name)
        if not api_key: continue

        try:
            print(
                f"[INFO] 正在使用名为 '{key_name}' 的 API Key 尝试分析图片... prompt length: {len(prompt)}, 图片数量: {len(valid_paths)}")

            # 初始化客户端 (新版 SDK)
            client = genai.Client(api_key=api_key)

            # 构建 Prompt Parts (混合文本和图片)
            parts = [
                types.Part.from_text(text=prompt),
                types.Part.from_text(
                    text="\n【系统提示】：下面将提供多组候选图片。每组图片都会被 <image_data> 标签严格包裹，标签内包含 <file_name> (文件名) 和真实的图片内容。请在评估时严格确保你分析的图片内容与 <file_name> 里的名字绝对对应，绝不能张冠李戴！\n")
            ]

            for path in valid_paths:
                file_name = os.path.basename(path)

                # 使用 XML 标签开启包裹，并注入文件名
                parts.append(
                    types.Part.from_text(text=f"\n<image_data>\n<file_name>{file_name}</file_name>\n<image_content>\n"))

                # 读取并添加图片 (保持原逻辑不变)
                try:
                    # 1. 读取图片
                    img = Image.open(path)

                    # 2. 转为二进制
                    byte_stream = io.BytesIO()
                    # 获取格式，默认为 JPEG
                    fmt = img.format if img.format else 'JPEG'
                    img.save(byte_stream, format=fmt)
                    image_bytes = byte_stream.getvalue()

                    # 3. 确定 MIME 类型
                    mime_type = f"image/{fmt.lower()}"
                    if mime_type == "image/jpg": mime_type = "image/jpeg"

                    # 4. 这里的 image_bytes 就是 raw bytes，和你发的官方示例中 b64decode 的结果类型一致
                    parts.append(types.Part.from_bytes(data=image_bytes, mime_type=mime_type))

                    # 闭合 XML 标签，彻底隔离下一张图
                    parts.append(types.Part.from_text(text="\n</image_content>\n</image_data>\n"))

                except Exception as img_err:
                    return f"读取图片失败: {path} -> {img_err}"

            # 构建请求配置 (复用全局配置函数)
            contents = [types.Content(role="user", parts=parts)]
            config = build_generate_content_config(model_name)

            # 调用模型 (复用全局安全调用函数)
            response = safe_generate_content(
                client=client,
                model=model_name,
                contents=contents,
                config=config,
                timeout=600
            )

            if not response.text:
                feedback = getattr(response, 'prompt_feedback', 'No text returned')
                print(f"[WARN] 模型返回了空响应: {feedback}")
                return str(feedback)

            return response.text

        except Exception as e:
            # 错误处理逻辑 (参考您的视频处理函数逻辑)
            if 'overloaded' in str(e) or 'An internal error has occurred' in str(e) or '429' in str(e):
                last_error = e
                print(f"[WARN] 名为 '{key_name}' 的 API Key 调用失败: {e}. 正在尝试下一个...")
                time.sleep(2)
                continue
            else:
                print(f"[ERROR] 名为 '{key_name}' 的 API Key 调用失败: {e.__class__.__name__}: {e}")
                last_error = e
                # 遇到非网络错误继续尝试下一个 key，或者您可以选择在这里 raise e
                continue

    return f"所有 API Key 均尝试失败。最后一次错误: {last_error}"
# -*- coding: utf-8 -*-
"""
Gemini 本地接口新增代码（Python 3.10+）

集成方法：
1. 安装依赖：python -m pip install requests
2. 将本文件全部内容复制到原文件的 if __name__ == "__main__": 之前。
   仅新增代码，不修改、覆盖或重新绑定原有函数。
3. 配置 GEMINI_LOCAL_API_KEY 环境变量，或填写下方 LOCAL_API_KEY 的默认值。
4. 在调用位置使用下面的新入口。原来的函数名仍保持原来的行为。

新增入口：
- chat_completion_local(...)：单次请求，返回 success/content/reasoning/raw 等字段。
- get_llm_content_local(...)：本地纯文本调用，成功返回 str，失败抛出异常。
- analyze_images_local(...)：本地单图/多图调用，成功返回 str，失败抛出异常。
- get_llm_content_auto(...)：本地主备模型调用失败后，调用原 get_llm_content。
- analyze_images_auto(...)：本地主备模型调用失败后，调用原 analyze_images_gemini。

本地模型名称沿用你提供的网关配置，不作为 Google 官方模型名称使用。
每个本地候选模型最多请求一次；临时错误会切换到备用模型。
auto 入口遇到文件/参数错误或明确拒绝时不回退。
auto 入口需与原代码处于同一模块，并沿用原函数的返回值与异常行为。
原代码在模块导入时仍会读取 Google 配置；本新增块单独使用 local 入口时无此依赖。
本文件没有自动运行的测试代码，也没有给新增函数添加 @with_proxy。

用法（复制到原模块后）：
    text = get_llm_content_local("你好，请介绍一下你自己。")
    text = analyze_images_local("比较这些图片", [r"C:\\images\\a.jpg", r"C:\\images\\b.png"])
    text = get_llm_content_auto("请解释一下量化交易。")
    result = chat_completion_local("gemini-3.1-pro", "描述图片", image_path=r"C:\\images\\a.jpg")
"""



# ==================== 本地日志（不配置应用的 root logger） ====================
LOCAL_PROMPT_PREVIEW_LENGTH = 20
LOCAL_RESPONSE_PREVIEW_LENGTH = 100
LOCAL_LOGGER = _local_logging.getLogger("gemini.local_api")
if not LOCAL_LOGGER.handlers:
    _local_handler = _local_logging.StreamHandler()
    _local_handler.setFormatter(_local_logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    ))
    LOCAL_LOGGER.addHandler(_local_handler)
    LOCAL_LOGGER.setLevel(_local_logging.INFO)
    LOCAL_LOGGER.propagate = False

_LOCAL_LOG_CONTEXT = _LocalContextVar("gemini_local_log_context", default=None)


def _local_log_preview(value, limit: int, secret=None) -> str:
    text = "" if value is None else str(value)
    if isinstance(secret, str) and secret:
        text = text.replace(secret, "***")
    text = " ".join(text.split())
    if limit <= 0:
        return "已关闭预览"
    return text[:limit] + ("..." if len(text) > limit else "")


def _local_log_event(level: str, event: str, model, detail: str):
    context = _LOCAL_LOG_CONTEXT.get()
    request_id = context["request_id"] if context else "-"
    getattr(LOCAL_LOGGER, level)(
        f"[{event}] 请求ID: [{request_id}] | 模型: [{model}] | {detail}"
    )


def _local_log_scope(function):
    """每个最外层入口分配 ID；嵌套入口、重试、回退复用同一上下文。"""
    @_local_functools.wraps(function)
    def wrapper(*args, **kwargs):
        if _LOCAL_LOG_CONTEXT.get() is not None:
            return function(*args, **kwargs)
        token = _LOCAL_LOG_CONTEXT.set({
            "request_id": _local_uuid.uuid4().hex[:12],
            "started_at": _local_time.perf_counter(),
            "attempt": 0,
        })
        try:
            return function(*args, **kwargs)
        finally:
            _LOCAL_LOG_CONTEXT.reset(token)
    return wrapper


def _local_log_request(function):
    """记录每次本地请求；保留原参数、返回字典和异常传播方式。"""
    signature = _local_inspect.signature(function)

    @_local_log_scope
    @_local_functools.wraps(function)
    def wrapper(*args, **kwargs):
        bound = signature.bind(*args, **kwargs)
        bound.apply_defaults()
        arguments = bound.arguments
        model, prompt = arguments["model"], arguments["prompt"]
        api_key = arguments["api_key"]
        api_key = LOCAL_API_KEY if api_key is None else api_key
        context = _LOCAL_LOG_CONTEXT.get()
        context["attempt"] += 1
        attempt = context["attempt"]
        started_at = _local_time.perf_counter()
        attachments = "尚未解析"
        try:
            # 将迭代器只展开一次，日志与实际请求使用同一份路径。
            if arguments["image_paths"] is not None:
                arguments["image_paths"] = _local_normalize_paths(arguments["image_paths"])
            paths = arguments["image_paths"]
            if arguments["image_path"] is not None:
                paths = _local_normalize_paths(arguments["image_path"])
            attachments = _local_log_preview(paths or "无", 500, api_key)
            prompt_length = len(prompt) if isinstance(prompt, str) else "未知"
            _local_log_event("info", "任务启动", model,
                f"提交 Gemini 本地请求 | 第 [{attempt}] 次 | 附件: [{attachments}] | "
                f"Prompt长度: [{prompt_length}] | "
                f"Prompt预览: [{_local_log_preview(prompt, LOCAL_PROMPT_PREVIEW_LENGTH, api_key)}]")
            result = function(*bound.args, **bound.kwargs)
        except Exception as exc:
            _local_log_event("error", "任务失败", model,
                f"第 [{attempt}] 次 | 附件: [{attachments}] | "
                f"耗时: [{_local_time.perf_counter() - started_at:.2f} 秒] | "
                f"错误类型: [{type(exc).__name__}] | 错误: [{_local_log_preview(exc, 1000, api_key)}]")
            raise

        elapsed = _local_time.perf_counter() - started_at
        total_elapsed = _local_time.perf_counter() - context["started_at"]
        detail = (f"第 [{attempt}] 次 | 附件: [{attachments}] | "
                  f"HTTP: [{result.get('status_code') or '未收到响应'}] | "
                  f"耗时: [{elapsed:.2f} 秒] | 累计耗时: [{total_elapsed:.2f} 秒]")
        if result["success"]:
            text = result["content"]
            detail += (f" | 响应长度: [{len(text)}] | "
                       f"响应预览: [{_local_log_preview(text, LOCAL_RESPONSE_PREVIEW_LENGTH, api_key)}]")
            raw = result.get("raw")
            usage = raw.get("usage") if isinstance(raw, dict) else None
            if isinstance(usage, dict) and usage:
                detail += (f" | Token: [输入={usage.get('prompt_tokens', '-')} / "
                           f"输出={usage.get('completion_tokens', '-')} / 合计={usage.get('total_tokens', '-')}]")
            _local_log_event("info", "任务完成", model, detail)
        else:
            _local_log_event("error", "任务失败", model,
                f"{detail} | 错误: [{_local_log_preview(result.get('error'), 1000, api_key)}]")
        return result
    return wrapper


def _local_call_google_with_logs(function, *, prompt, model_name, **kwargs):
    """仅补充原 Google 入口的调用日志，不推断其字符串返回值是否为成功。"""
    started_at = _local_time.perf_counter()
    attachments = _local_log_preview(kwargs.get("image_paths") or "无", 500)
    try:
        result = function(prompt=prompt, model_name=model_name, **kwargs)
    except Exception as exc:
        _local_log_event("error", "回退失败", model_name,
            f"附件: [{attachments}] | 耗时: [{_local_time.perf_counter() - started_at:.2f} 秒] | "
            f"错误类型: [{type(exc).__name__}] | 错误: [{_local_log_preview(exc, 1000, LOCAL_API_KEY)}]")
        raise
    context = _LOCAL_LOG_CONTEXT.get()
    total_elapsed = _local_time.perf_counter() - (context["started_at"] if context else started_at)
    _local_log_event("info", "回退返回", model_name,
        f"原 Gemini 入口已返回 | 附件: [{attachments}] | "
        f"耗时: [{_local_time.perf_counter() - started_at:.2f} 秒] | 累计耗时: [{total_elapsed:.2f} 秒] | "
        f"返回类型: [{type(result).__name__}] | 响应长度: [{len(result) if isinstance(result, str) else '-'}] | "
        f"响应预览: [{_local_log_preview(result, LOCAL_RESPONSE_PREVIEW_LENGTH, LOCAL_API_KEY)}]")
    return result


class LocalGeminiAPIError(RuntimeError):
    """本地服务调用失败；result 中保留错误详情，不把错误字符串当作生成内容。"""

    def __init__(self, result: dict):
        self.result = result
        super().__init__(result.get("error") or "本地 API 调用失败")


def encode_image_to_base64_local(image_path: str) -> tuple[str, str]:
    """读取图片原始字节，返回 MIME 与 Base64；文件错误直接抛出。"""
    if not _local_os.path.isfile(image_path):
        raise FileNotFoundError(f"未找到指定的图片文件: {image_path}")
    mime_type, _ = _local_mimetypes.guess_type(image_path)
    if not mime_type or not mime_type.startswith("image/"):
        mime_type = "image/jpeg"
    with open(image_path, "rb") as image_file:
        image_bytes = image_file.read()
    if not image_bytes:
        raise ValueError(f"图片文件为空: {image_path}")
    return mime_type, _local_base64.b64encode(image_bytes).decode("ascii")


def _local_normalize_paths(image_paths) -> list[str]:
    if image_paths is None:
        return []
    if isinstance(image_paths, (str, _local_os.PathLike)):
        image_paths = [image_paths]
    return [_local_os.fspath(path) for path in image_paths]


def _local_failure(model, error, *, status_code=None, raw=None,
                   retryable=False, blocked=False, retry_after=None) -> dict:
    return {
        "success": False, "content": "", "reasoning": None, "raw": raw,
        "model": model, "error": error, "status_code": status_code,
        "retryable": retryable, "blocked": blocked, "retry_after": retry_after,
    }


@_local_log_request
def chat_completion_local(
        model: str,
        prompt: str,
        image_path: str | None = None,
        api_base_url: str | None = None,
        api_key: str | None = None,
        temperature: float = 0.7,
        timeout: float | tuple[float, float] = 360,
        *,
        image_paths=None,
) -> dict:
    """
    单次调用本地 OpenAI 兼容接口（底层执行器，保持签名与 @_local_log_request 完全兼容）。

    核心增强：
    1. 放宽网络异常重试范围：所有 RequestException（含流中断 ChunkedEncodingError 等）均可重试。
    2. 扩展 HTTP 可重试状态码：覆盖 408, 409, 425, 429, 500, 502, 503, 504 及反代常见的 520-524。
    3. 本地网关容错：将 HTTP 200 但非 JSON、缺少 choices、或返回空 content 标记为 retryable=True，
       防止本地网关偶发返回空包时直接中断整个重试与降级链路。
    """
    api_base_url = LOCAL_API_BASE_URL if api_base_url is None else api_base_url
    api_key = LOCAL_API_KEY if api_key is None else api_key
    if not isinstance(api_key, str) or not api_key.strip():
        raise ValueError("请配置 LOCAL_API_KEY / GEMINI_LOCAL_API_KEY，或传入 api_key。")
    if not isinstance(api_base_url, str) or not api_base_url.strip():
        raise ValueError("api_base_url 不能为空。")
    if not isinstance(model, str) or not model.strip():
        raise ValueError("model 不能为空。")
    if not isinstance(prompt, str):
        raise TypeError("prompt 必须是字符串。")
    if image_path is not None and image_paths is not None:
        raise ValueError("image_path 和 image_paths 不能同时提供。")

    paths = _local_normalize_paths(image_path if image_path is not None else image_paths)
    if paths:
        content = [
            {"type": "text", "text": prompt},
            {
                "type": "text",
                "text": "每个 image_data 块对应一张图片。请按编号与 file_name 对应分析，避免混淆。",
            },
        ]
        for index, path in enumerate(paths, 1):
            mime_type, encoded = encode_image_to_base64_local(path)
            filename = _local_xml_escape(_local_os.path.basename(path))
            content.extend([
                {"type": "text", "text": f'<image_data index="{index}"><file_name>{filename}</file_name>'},
                {"type": "image_url", "image_url": {"url": f"data:{mime_type};base64,{encoded}"}},
                {"type": "text", "text": "</image_data>"},
            ])
    else:
        content = prompt

    payload = {
        "model": model,
        "messages": [{"role": "user", "content": content}],
        "temperature": temperature,
        "stream": False,
    }
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}

    try:
        with _local_requests.Session() as session:
            session.trust_env = False
            response = session.post(
                api_base_url,
                headers=headers,
                json=payload,
                timeout=timeout,
                allow_redirects=False,
            )
    except _local_requests.exceptions.RequestException as exc:
        detail = str(exc).replace(api_key, "***")
        # 只要不是 URL 格式错误等配置级异常，网络传输层面的异常全部允许重试
        is_config_error = isinstance(exc, (
            _local_requests.exceptions.InvalidURL,
            _local_requests.exceptions.InvalidHeader,
            _local_requests.exceptions.MissingSchema,
            _local_requests.exceptions.InvalidSchema,
        ))
        return _local_failure(
            model,
            f"本地网络请求异常 ({type(exc).__name__}): {detail}",
            retryable=not is_config_error,
        )

    status_code = response.status_code
    retryable_status_codes = {408, 409, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524}
    if not 200 <= status_code < 300:
        detail = response.text.replace(api_key, "***")[:1000]
        retry_after = None
        try:
            retry_after = max(0.0, float(response.headers.get("Retry-After", "")))
        except (TypeError, ValueError):
            pass
        return _local_failure(
            model,
            f"本地接口 HTTP {status_code}: {detail}",
            status_code=status_code,
            retryable=status_code in retryable_status_codes,
            retry_after=retry_after,
        )

    try:
        result = response.json()
    except ValueError:
        return _local_failure(
            model, "本地接口返回了非 JSON 响应。", status_code=status_code, retryable=True
        )
    if not isinstance(result, dict):
        return _local_failure(
            model, "本地接口 JSON 顶层不是对象。", raw=result, status_code=status_code, retryable=True
        )
    choices = result.get("choices")
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
        return _local_failure(
            model, "本地接口缺少有效的 choices[0]，请查看 raw。",
            raw=result, status_code=status_code, retryable=True,
        )
    choice = choices[0]
    message = choice.get("message")
    if not isinstance(message, dict):
        return _local_failure(
            model, "本地接口缺少有效的 message。", raw=result, status_code=status_code, retryable=True
        )
    if choice.get("finish_reason") == "content_filter" or message.get("refusal"):
        return _local_failure(
            model, "本地模型明确拒绝了请求 (content_filter / refusal)。",
            raw=result, status_code=status_code, blocked=True, retryable=False,
        )

    text = message.get("content")
    if isinstance(text, list):
        text = "".join(
            part["text"] for part in text
            if isinstance(part, dict) and isinstance(part.get("text"), str)
        )
    if not isinstance(text, str) or not text.strip():
        failure = _local_failure(
            model,
            "本地接口未返回正文 content（空回复），请查看 reasoning / raw。",
            raw=result,
            status_code=status_code,
            retryable=True,  # 空回复在本地逆向网关中属于典型偶发故障，允许进入重试
        )
        failure["reasoning"] = message.get("reasoning_content")
        return failure

    return {
        "success": True,
        "content": text,
        "reasoning": message.get("reasoning_content"),
        "raw": result,
        "model": model,
        "status_code": status_code,
        "error": None,
        "retryable": False,
        "blocked": False,
        "retry_after": None,
    }


@_local_log_scope
def get_llm_content_local(
        prompt: str = "你好，请介绍一下你自己。",
        image_paths=None,
        model_name: str = LOCAL_MODELS[0],
        back_model: str | list[str] | tuple[str, ...] | None = LOCAL_MODELS[1],
        *,
        image_path: str | None = None,
        max_retries_per_model: int = 3,
        retry_delay: float = 2.0,
        backoff_factor: float = 1.5,
        max_retry_delay: float = 30.0,
        raise_on_error: bool = True,
        **request_options,
) -> str | None:
    """
    完备的本地多模态统一调用入口（同时支持纯文本、单图、多图，内置多级重试与主备模型自动降级）。

    参数说明：
    - prompt: 提示词文本。
    - image_paths: 可选。支持传 None（纯文本）、单张图片路径字符串/Path、或多张图片路径列表/可迭代对象。
    - model_name: 主模型名称，默认 LOCAL_MODELS[0]。
    - back_model: 备用模型，支持传单个模型名字符串、多个备用模型列表/元组，或 None（禁用备用模型）。
    - image_path: 关键字参数，兼容单图调用习惯（与 image_paths 二选一即可）。
    - max_retries_per_model: 单个模型最大尝试次数（默认 3 次，即首次调用 + 最多 2 次同模型重试）。
    - retry_delay: 初始重试等待秒数（默认 2.0 秒）。
    - backoff_factor: 指数退避乘数（默认 1.5，每次重试等待时间按 retry_delay * (backoff_factor ** n) 增长）。
    - max_retry_delay: 单次重试常规指数退避上限（默认 30.0 秒；含服务端 Retry-After 在内的单次最终冷却硬上限为 600.0 秒）。
    - raise_on_error: 全部尝试均失败时，True 抛出 LocalGeminiAPIError，False 则记录日志并返回 None。
    - **request_options: 透传给 chat_completion_local 的底层参数（如 timeout, temperature, api_key, api_base_url）。
    """
    # 1. 基础参数校验
    if not isinstance(prompt, str):
        raise TypeError("prompt 必须是字符串。")
    if not isinstance(model_name, str) or not model_name.strip():
        raise ValueError("model_name 不能为空。")
    if max_retries_per_model < 1:
        raise ValueError("max_retries_per_model 必须大于等于 1。")
    if retry_delay < 0 or backoff_factor < 1.0 or max_retry_delay < 0:
        raise ValueError("重试延迟参数不合法：请确保 retry_delay >= 0, backoff_factor >= 1.0, max_retry_delay >= 0。")
    if image_path is not None and image_paths is not None:
        raise ValueError("image_path 和 image_paths 不能同时提供。")

    # 2. 构建去重后的候选模型链（支持单个或多个备用模型）
    candidate_models = [model_name.strip()]
    if back_model is not None:
        if isinstance(back_model, str):
            back_list = [back_model]
        elif isinstance(back_model, (list, tuple)):
            back_list = list(back_model)
        else:
            raise ValueError("back_model 必须是非空字符串、字符串列表/元组或 None。")

        for bm in back_list:
            if not isinstance(bm, str) or not bm.strip():
                raise ValueError("back_model 中包含空或非字符串的模型名称。")
            bm_clean = bm.strip()
            if bm_clean not in candidate_models:
                candidate_models.append(bm_clean)

    # 3. 统一归一化并预校验图片路径（避免带着不存在的文件进入重试循环）
    raw_paths = image_path if image_path is not None else image_paths
    normalized_paths = _local_normalize_paths(raw_paths)
    if normalized_paths:
        for path in normalized_paths:
            if not _local_os.path.isfile(path):
                error_msg = f"未找到指定的图片文件: {path}"
                _local_log_event("error", "参数预检失败", candidate_models[0], error_msg)
                raise FileNotFoundError(error_msg)
            if _local_os.path.getsize(path) == 0:
                error_msg = f"图片文件为空 (0 字节): {path}"
                _local_log_event("error", "参数预检失败", candidate_models[0], error_msg)
                raise ValueError(error_msg)

    mode_desc = f"多模态图片({len(normalized_paths)}张)" if normalized_paths else "纯文本"
    total_max_attempts = len(candidate_models) * max_retries_per_model
    # _local_log_event(
    #     "info",
    #     "调度初始化",
    #     " -> ".join(candidate_models),
    #     f"模式: [{mode_desc}] | 候选模型数: [{len(candidate_models)}] | "
    #     f"单模型最大尝试: [{max_retries_per_model}] | 总最大尝试上限: [{total_max_attempts}]",
    # )

    # 4. 多模型 × 单模型多轮重试主循环
    MAX_SINGLE_COOLDOWN = 600.0  # 单次冷却硬性上限：10分钟 (600秒)
    last_result = None
    error_history = []
    global_attempt = 0

    for model_idx, current_model in enumerate(candidate_models):
        for retry_idx in range(max_retries_per_model):
            global_attempt += 1
            result = chat_completion_local(
                model=current_model,
                prompt=prompt,
                image_paths=normalized_paths if normalized_paths else None,
                **request_options,
            )

            # 调用成功：直接返回正文
            if result["success"]:
                if global_attempt > 1:
                    _local_log_event(
                        "info",
                        "重试恢复",
                        current_model,
                        f"在第 [{global_attempt}/{total_max_attempts}] 次总尝试 "
                        f"(模型 [{current_model}] 第 [{retry_idx + 1}/{max_retries_per_model}] 次) 成功恢复！",
                    )
                return result["content"]

            last_result = result
            err_msg = result.get("error") or "未知错误"
            status_code = result.get("status_code") or "无状态码"
            error_history.append(
                f"[{current_model}#尝试{retry_idx + 1}(HTTP:{status_code})]: {err_msg}"
            )

            # 若触发明确的安全拒绝 (blocked)，属于内容本身违规，停止一切重试与模型切换
            if result.get("blocked"):
                _local_log_event(
                    "error",
                    "请求被拒",
                    current_model,
                    f"模型明确拒绝请求 (blocked=True)，终止后续重试 | 原因: [{_local_log_preview(err_msg, 500)}]",
                )
                if raise_on_error:
                    raise LocalGeminiAPIError(result)
                return None

            # 若属于不可重试错误（例如 401 密钥错误、400 参数格式错误、404 模型不存在）
            if not result.get("retryable"):
                # 如果是 404/400 等可能与特定模型名绑定的错误，且还有备用模型，则直接跳出当前模型尝试下一个模型
                if model_idx < len(candidate_models) - 1:
                    next_model = candidate_models[model_idx + 1]
                    _local_log_event(
                        "warning",
                        "模型降级",
                        current_model,
                        f"遇到当前模型不可重试错误 (HTTP: [{status_code}])，跳过本模型剩余重试，"
                        f"立即切换备用模型: [{next_model}] | 原因: [{_local_log_preview(err_msg, 500)}]",
                    )
                    break
                else:
                    _local_log_event(
                        "error",
                        "终止重试",
                        current_model,
                        f"遇到不可重试错误且已无备用模型 | 原因: [{_local_log_preview(err_msg, 500)}]",
                    )
                    if raise_on_error:
                        raise LocalGeminiAPIError(result)
                    return None

            # 计算退避等待时间（单次等待强制不超过 600 秒 / 10 分钟）
            has_more_retries_in_current = (retry_idx < max_retries_per_model - 1)
            has_next_model = (model_idx < len(candidate_models) - 1)

            if has_more_retries_in_current:
                # 同模型内指数退避：retry_delay * (backoff_factor ** retry_idx)，且最终单次等待不超过 600 秒
                computed_delay = min(max_retry_delay, retry_delay * (backoff_factor ** retry_idx))
                delay = min(MAX_SINGLE_COOLDOWN, max(computed_delay, result.get("retry_after") or 0.0))
                _local_log_event(
                    "warning",
                    "同模型重试",
                    current_model,
                    f"本模型第 [{retry_idx + 1}/{max_retries_per_model}] 次失败 "
                    f"(总第 [{global_attempt}/{total_max_attempts}] 次) | "
                    f"等待 [{delay:.2f} 秒] 后进行本模型第 [{retry_idx + 2}] 次尝试 | "
                    f"原因: [{_local_log_preview(err_msg, 500)}]",
                )
                if delay > 0:
                    _local_time.sleep(delay)

            elif has_next_model:
                # 当前模型次数已用尽，切换到下一个备用模型，单次等待同样不超过 600 秒
                next_model = candidate_models[model_idx + 1]
                delay = min(MAX_SINGLE_COOLDOWN, max(retry_delay, result.get("retry_after") or 0.0))
                _local_log_event(
                    "warning",
                    "跨模型切换",
                    current_model,
                    f"模型 [{current_model}] 的 [{max_retries_per_model}] 次尝试已全部耗尽 | "
                    f"等待 [{delay:.2f} 秒] 后切换至备用模型: [{next_model}] | "
                    f"最后原因: [{_local_log_preview(err_msg, 500)}]",
                )
                if delay > 0:
                    _local_time.sleep(delay)

    # 5. 所有候选模型与重试次数全部耗尽
    summary_error = f"所有本地模型均调用失败（共尝试 {global_attempt} 次）。轨迹: {' -> '.join(error_history)}"
    _local_log_event(
        "error",
        "任务彻底失败",
        " -> ".join(candidate_models),
        _local_log_preview(summary_error, 1500),
    )

    if last_result is None:
        last_result = _local_failure(model_name, summary_error)
    else:
        last_result = dict(last_result)
        last_result["error"] = summary_error
        last_result["error_history"] = error_history

    if raise_on_error:
        raise LocalGeminiAPIError(last_result)
    return None


def _local_generate_text(prompt, image_paths, model_name, back_model,
                         retry_delay=2.0, **request_options) -> str:
    """临时错误时切换备用模型；去重后每个模型最多尝试一次。"""
    if not isinstance(model_name, str) or not model_name.strip():
        raise ValueError("model_name 不能为空。")
    if back_model is not None and (not isinstance(back_model, str) or not back_model.strip()):
        raise ValueError("back_model 必须是非空字符串或 None。")
    if retry_delay < 0:
        raise ValueError("retry_delay 不能小于 0。")
    models = [model_name]
    if back_model is not None and back_model != model_name:
        models.append(back_model)
    paths = _local_normalize_paths(image_paths)
    for index, model in enumerate(models):
        result = chat_completion_local(
            model=model, prompt=prompt, image_paths=paths, **request_options
        )
        if result["success"]:
            return result["content"]
        if result["blocked"] or not result["retryable"] or index == len(models) - 1:
            raise LocalGeminiAPIError(result)
        delay = max(retry_delay, result.get("retry_after") or 0.0)
        _local_log_event("warning", "任务重试", model,
            f"第 [{index + 1}/{len(models)}] 次失败 | "
            f"下一模型: [{models[index + 1]}] | 等待: [{delay:g} 秒] | "
            f"原因: [{_local_log_preview(result.get('error'), 1000)}]")
        if delay:
            _local_time.sleep(delay)
    raise RuntimeError("未配置本地模型。")


@_local_log_scope
def analyze_images_local(
        prompt: str = "每张图片的内容是什么？",
        image_paths=None,
        model_name: str = LOCAL_MODELS[0],
        back_model: str | None = LOCAL_MODELS[1],
        **request_options,
) -> str:
    """本地图片入口；支持单个路径或路径列表，调用时必须提供图片。"""
    paths = _local_normalize_paths(image_paths)
    if not paths:
        raise ValueError("请通过 image_paths 提供至少一张图片。")
    return _local_generate_text(prompt, paths, model_name, back_model, **request_options)


@_local_log_scope
def get_llm_content_auto(
        prompt: str = "你好，请介绍一下你自己。",
        model_name: str = LOCAL_MODELS[0],
        back_model: str | None = LOCAL_MODELS[1],
        *,
        google_model_name: str = "gemini-flash-latest",
        google_back_model: str = "gemini-flash-lite-latest",
        **request_options,
) -> str | None:
    """先调用本地服务；失败时调用原有 get_llm_content，返回值沿用原函数。"""
    try:
        return get_llm_content_local(prompt, model_name, back_model, **request_options)
    except LocalGeminiAPIError as exc:
        if exc.result.get("blocked"):
            raise
        google_function = globals().get("get_llm_content")
        if not callable(google_function):
            raise RuntimeError("找不到原 get_llm_content，请将新增代码放入原模块。") from exc
        _local_log_event("warning", "任务回退", google_model_name,
            f"本地调用失败，切换原 Gemini 文本入口 | 原因: [{_local_log_preview(exc, 1000)}]")
        return _local_call_google_with_logs(
            google_function, prompt=prompt, model_name=google_model_name, back_model=google_back_model
        )


@_local_log_scope
def analyze_images_auto(
        prompt: str = "每张图片的内容是什么？",
        image_paths=None,
        model_name: str = LOCAL_MODELS[0],
        back_model: str | None = LOCAL_MODELS[1],
        *,
        google_model_name: str = "gemini-3-flash-preview",
        **request_options,
) -> str:
    """先调用本地服务；失败时调用原有 analyze_images_gemini。"""
    paths = _local_normalize_paths(image_paths)
    try:
        return analyze_images_local(prompt, paths, model_name, back_model, **request_options)
    except LocalGeminiAPIError as exc:
        if exc.result.get("blocked"):
            raise
        google_function = globals().get("analyze_images_gemini")
        if not callable(google_function):
            raise RuntimeError("找不到原 analyze_images_gemini，请将新增代码放入原模块。") from exc
        _local_log_event("warning", "任务回退", google_model_name,
            f"本地调用失败，切换原 Gemini 图片入口 | 原因: [{_local_log_preview(exc, 1000)}]")
        return _local_call_google_with_logs(
            google_function, prompt=prompt, image_paths=paths, model_name=google_model_name
        )


if __name__ == "__main__":
    # valid_all_api_keys()
    #
    # print("\n" + "=" * 20 + " 开始测试 " + "=" * 20)
    # print("[TEST] 正在测试 get_llm_content (这将触发第一次动态排序)")
    # start_time = time.time()
    # result = get_llm_content(prompt="再给我讲个笑话吧", model_name="gemini-flash-latest")
    # if result:
    #     print("\n[RESULT] 模型输出：\n", result)
    # else:
    #     print(f"\n[FAIL] 内容生成失败{result}")
    # print(f"[INFO] 执行时间: {time.time() - start_time:.2f} 秒")

    # 1. 本地纯文本调用
    text = get_llm_content_local(
        prompt="你好，请介绍一下你自己。",
    )
    print(text)

    # 2. 本地图片分析，支持多张图片
    text = get_llm_content_local(
        prompt="请分别描述这些图片，并标明对应的文件名。",
        image_paths=[
            r"C:\Users\zxh\Desktop\temp\test.jpg",
        ],
        back_model=None,  # 只使用指定模型
    )
    print(text)
