# -- coding: utf-8 --
""":authors:
    zhuxiaohu
:create_date:
    2026/4/7 0:32
:last_date:
    2026/4/7 0:32
:description:

"""
import ast
import asyncio
import hashlib
import json
import logging
import os
import re
import socket
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Optional, Union
from urllib.parse import urlparse

from filelock import FileLock, Timeout

# ==============================================================================
# 统一项目路径常量 (以 ai_content_pipeline 目录为独立项目根目录)
# ==============================================================================
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
PROMPT_DIR = PROJECT_ROOT / "prompt"
DEFAULT_LOG_DIR = PROJECT_ROOT / "logs"
DEFAULT_MEDIA_DIR = PROJECT_ROOT / "media_downloads"
DEFAULT_FORENSICS_DIR = PROJECT_ROOT / "forensics_logs"
DEFAULT_SESSION_ROOT = PROJECT_ROOT / "browser_sessions"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def resolve_cross_platform_path(raw_path: Optional[str], fallback_path: Union[str, Path]) -> str:
    """
    将可能带有 Windows 盘符的配置路径安全解析为当前操作系统可用的路径。
    - 如果在 Windows 上且对应盘符（如 W:\\）存在，则保留原路径以兼容本地已有数据；
    - 如果在 Linux/macOS 上，或当前 Windows 机器不存在该盘符，则自动回退到 fallback_path；
    - 如果 raw_path 是相对路径，则基于 PROJECT_ROOT 解析。
    """
    fallback_str = str(fallback_path)
    if not raw_path or not str(raw_path).strip():
        return fallback_str

    raw_str = str(raw_path).strip()
    is_win_drive = len(raw_str) >= 2 and raw_str[0].isalpha() and raw_str[1] == ":"
    if is_win_drive:
        if os.name == "nt":
            drive_root = raw_str[:2] + "\\"
            if os.path.exists(drive_root):
                return raw_str
        return fallback_str

    p = Path(os.path.expanduser(raw_str))
    if not p.is_absolute():
        p = (PROJECT_ROOT / p).resolve()
    return str(p)


def setup_logger(log_dir: Optional[str] = None, app_name: str = "BinanceBot"):
    """
    初始化全局日志配置。
    支持在多个文件中重复调用，但实际只会初始化一次。
    """
    log_dir = resolve_cross_platform_path(log_dir, DEFAULT_LOG_DIR)
    os.makedirs(log_dir, exist_ok=True)

    # 获取根记录器
    logger = logging.getLogger()

    # 【核心安全阀】：如果已经有 Handler，说明被其他文件初始化过了，直接跳过！
    # 这一步极其重要，否则你在 10 个文件里调用，一行日志就会被打印 10 次。
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    # 包含 文件名.函数名:行号 的终极溯源格式
    formatter = logging.Formatter(
        '%(asctime)s,%(msecs)03d | %(levelname)s | [%(funcName)s] | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 按天切割日志
    log_file_path = os.path.join(log_dir, f"{app_name}.log")
    file_handler = TimedRotatingFileHandler(
        filename=log_file_path,
        when="midnight",
        interval=1,
        backupCount=30,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)

    # 控制台输出
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.propagate = False

    return logger


logger = setup_logger()


def read_json(json_path):
    """
    读取 JSON 文件并返回内容。

    Args:
        json_path (str): JSON 文件的路径。

    Returns:
        dict: 解析后的 JSON 内容。
    """
    if not os.path.exists(json_path):
        return {}

    with open(json_path, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
            return data
        except json.JSONDecodeError as e:
            raise ValueError(f"无法解析 JSON 文件 '{json_path}': {e}")


def save_json(json_path, data):
    dir_path = os.path.dirname(json_path)
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)

    # 锁文件通常以 .lock 结尾
    lock_path = json_path + ".lock"

    # FileLock 会在文件系统层面创建锁，支持多进程和多线程安全
    with FileLock(lock_path):
        # 原子写入
        tmp_path = json_path + ".tmp"
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4, default=str)
        os.replace(tmp_path, json_path)


def read_file_to_str(filepath,
                     encoding: str = "utf-8",
                     errors: str = "strict") -> str:
    """
    读取文件并返回整个内容的字符串。

    参数:
        filepath: 文件路径（str 或 pathlib.Path）。
        encoding: 文本编码（默认 'utf-8'）。
        errors: 解码错误处理策略（'strict'|'replace'|'ignore' 等，默认 'strict'）。
                'strict' 会在遇到无法解码的字节时抛出 UnicodeDecodeError，
                'replace' 会用替代字符替换无法解码的字节，'ignore' 则忽略它们。

    返回:
        文件内容（str）。

    抛出:
        FileNotFoundError 如果文件不存在。
        UnicodeDecodeError 如果 decoding 失败且 errors='strict'。
    """
    p = Path(filepath)
    with p.open("r", encoding=encoding, errors=errors) as f:
        return f.read()


def string_to_object(input_str: str):
    """
    从字符串中提取并解析出 Python 列表或字典对象，设计得更加健壮。

    该函数增强了对不规范格式的容忍度，特别适合处理来自 LLM 的输出。

    核心功能：
    1.  **智能提取**: 自动在整个字符串中定位 JSON/Python 对象的边界（从第一个 '{' 或 '[' 到最后一个 '}' 或 ']），
        忽略前导和尾随的无关文本（例如 "当然，这是您要的JSON："）。
    2.  **兼容 Markdown**: 能够处理被 ```json ... ``` 代码块包裹的内容。
    3.  **错误修正**:
        - 自动移除常见的行内 (//) 和块级 (/* */) 注释。
        - 自动移除导致 JSON 解析失败的尾随逗号 (trailing commas)。
    4.  **双引擎解析**:
        - 首先尝试使用 `json.loads`，因为它更符合标准，速度更快。
        - 如果失败，则回退到 `ast.literal_eval`，以支持 Python 特有的字面量
          （如 `None`, `True`, `False` 以及单引号字符串）。

    如果无法找到或解析出有效的对象，则抛出 ValueError 异常。

    :param input_str: 包含列表或字典的输入字符串。
    :return: 解析后的 Python 列表或字典。
    :raises ValueError: 如果无法从字符串中找到或解析出有效的对象。
    :raises TypeError: 如果输入不是字符串。
    """
    # 0. 输入校验：处理 None 或非字符串输入
    if not isinstance(input_str, str):
        # 抛出 TypeError 更符合 Python 语义，但根据您的要求统一为 ValueError 也可以
        raise TypeError(f"输入必须是字符串，但收到了 {type(input_str).__name__}。")

    # 创建一个统一的错误信息生成器
    def _create_error_message(reason: str) -> str:
        # 预览原始输入的前50个字符
        preview = (input_str[:50] + '...') if len(input_str) > 50 else input_str
        return f"{reason} | 输入内容预览: '{preview}'"

    # 1. 智能提取：在字符串中寻找对象边界 (重构后，逻辑更清晰)
    first_bracket = input_str.find('[')
    first_curly = input_str.find('{')

    # 确定第一个开括号的位置
    if first_bracket == -1 and first_curly == -1:
        raise ValueError(_create_error_message("输入字符串中未找到疑似列表或字典的起始符号 '[' 或 '{'"))

    if first_bracket == -1:
        start_pos = first_curly
    elif first_curly == -1:
        start_pos = first_bracket
    else:
        start_pos = min(first_bracket, first_curly)

    # 确定最后一个闭括号的位置
    end_pos = max(input_str.rfind(']'), input_str.rfind('}'))

    if end_pos <= start_pos:
        raise ValueError(_create_error_message("未找到与起始括号匹配的结束括号 ']' 或 '}'"))

    # 提取出最可能包含对象的子字符串
    potential_obj_str = input_str[start_pos: end_pos + 1]

    # 2. 错误修正：清理提取出的字符串
    # 移除 JavaScript/JSONC 风格的注释
    potential_obj_str = re.sub(r"//.*", "", potential_obj_str)
    potential_obj_str = re.sub(r"/\*[\s\S]*?\*/", "", potential_obj_str, flags=re.MULTILINE)
    # 移除尾随逗号 (例如, [1, 2,])
    potential_obj_str = re.sub(r",\s*([}\]])", r"\1", potential_obj_str)
    cleaned_str = potential_obj_str.strip()

    # 3. 双引擎解析
    try:
        # 首先尝试使用 json.loads (更标准，通常更快)
        return json.loads(cleaned_str)
    except json.JSONDecodeError:
        # 如果 json.loads 失败，回退到 ast.literal_eval (更宽容，支持 Python 语法)
        try:
            return ast.literal_eval(cleaned_str)
        except (ValueError, SyntaxError, MemoryError) as e:
            # 如果两种方法都失败，则抛出最终的异常，并提供丰富的上下文信息
            cleaned_preview = (cleaned_str[:150] + '...') if len(cleaned_str) > 150 else cleaned_str
            error_reason = f"无法将提取的内容解析为列表或字典，解析器错误: {e}"
            # 最终的错误信息包含：原因，原始输入预览，以及尝试解析的内容预览
            raise ValueError(f"{_create_error_message(error_reason)}\n"
                             f"尝试解析的内容 (清理后): '''{cleaned_preview}'''")


_MISSING = object()


def get_config(key: str, default: Any = _MISSING):
    """
    从 config.json 文件中获取指定字段的值
    :param key: 配置字段名
    :param default: 可选默认值；若未传且字段不存在则抛出 KeyError（保持向后兼容）
    :return: 配置字段值
    """
    config_file = str(CONFIG_DIR / "config.json")

    if not os.path.exists(config_file):
        raise FileNotFoundError(f"配置文件 '{config_file}' 不存在，请检查文件路径。")

    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"配置文件 '{config_file}' 格式错误: {e}")

    if key not in config_data:
        if default is not _MISSING:
            return default
        raise KeyError(f"配置文件中缺少字段: {key}")

    return config_data[key]


def get_proxy_url(default: Optional[str] = "http://127.0.0.1:7890") -> Optional[str]:
    """
    统一获取代理地址，支持跨平台配置与无代理直连模式：
    1. 优先读取环境变量 AI_PIPELINE_PROXY
    2. 其次读取 config/config.json 中的 "proxy_url"
    3. 若配置为 "" / None / "none" / "direct" / False，则返回 None（直连不走代理）
    """
    env_proxy = os.environ.get("AI_PIPELINE_PROXY")
    if env_proxy is not None:
        val = env_proxy.strip()
        if val.lower() in ("", "none", "null", "direct", "false"):
            return None
        return val

    try:
        raw = get_config("proxy_url", default=_MISSING)
        if raw is not _MISSING:
            if raw is None or str(raw).strip().lower() in ("", "none", "null", "direct", "false"):
                return None
            return str(raw).strip()
    except Exception:
        pass

    return default


def get_requests_proxies(default: Optional[str] = "http://127.0.0.1:7890") -> Optional[Dict[str, str]]:
    """
    返回 requests 库可直接使用的 proxies 字典；若禁用代理则返回 None。
    """
    proxy_url = get_proxy_url(default=default)
    if not proxy_url:
        return None
    return {
        "http": proxy_url,
        "https": proxy_url,
    }


def resolve_session_dir(config_key: str, default_subdir: str) -> str:
    """
    解析浏览器持久化会话目录（支持 Windows 原有目录平滑兼容 + Linux/跨平台自动回退）。
    """
    raw_val = get_config(config_key, default=None)
    session_root_cfg = get_config("browser_session_root", default=None)
    if session_root_cfg:
        base_root = Path(resolve_cross_platform_path(session_root_cfg, DEFAULT_SESSION_ROOT))
    else:
        base_root = DEFAULT_SESSION_ROOT
    fallback = base_root / default_subdir
    return resolve_cross_platform_path(raw_val, fallback)


def get_media_save_dir() -> str:
    """
    获取媒体文件下载保存目录，默认存放在 ai_content_pipeline/media_downloads。
    """
    raw_val = get_config("media_save_dir", default=None)
    return resolve_cross_platform_path(raw_val, DEFAULT_MEDIA_DIR)


def compute_image_content_id(image_input) -> Optional[str]:
    """
    根据图片视觉像素内容生成纯粹的唯一标识（128-bit 双向感知差分哈希，32位十六进制字符串）。
    无论图片 URL 如何变化、或者经过平台 CDN 重压缩/重编码，只要画面内容一致，返回的标识完全相同。
    :param image_input: 图片二进制 bytes，或本地图片路径 (str / Path)
    :return: 32位十六进制内容标识；若非有效图片则返回 None
    """
    import io
    from PIL import Image

    try:
        if isinstance(image_input, (bytes, bytearray)):
            img_ctx = Image.open(io.BytesIO(image_input))
        else:
            img_ctx = Image.open(image_input)

        with img_ctx as img:
            gray_h = img.convert("L").resize((9, 8), Image.Resampling.LANCZOS)
            px_h = list(gray_h.getdata())
            bits_h = 0
            for r in range(8):
                row_offset = r * 9
                for c in range(8):
                    bits_h = (bits_h << 1) | (1 if px_h[row_offset + c] > px_h[row_offset + c + 1] else 0)

            gray_v = img.convert("L").resize((8, 9), Image.Resampling.LANCZOS)
            px_v = list(gray_v.getdata())
            bits_v = 0
            for r in range(8):
                for c in range(8):
                    bits_v = (bits_v << 1) | (1 if px_v[r * 8 + c] > px_v[(r + 1) * 8 + c] else 0)

            if bits_h == 0 and bits_v == 0:
                rgb_bytes = img.convert("RGB").resize((16, 16), Image.Resampling.LANCZOS).tobytes()
                return hashlib.md5(rgb_bytes).hexdigest()

            return f"{bits_h:016x}{bits_v:016x}"
    except Exception:
        return None


def get_image_content_id(file_path: str) -> str:
    """
    获取本地图片文件的纯内容唯一标识：
    若物理文件存在则优先计算其视觉内容标识，否则回退提取文件名主干。
    """
    if not file_path:
        return ""
    p = Path(str(file_path).strip())
    if p.is_file():
        cid = compute_image_content_id(p)
        if cid:
            return cid
    return p.stem.lower()


async def _download_media_async(url: str, save_dir: Optional[str] = None, proxy: Optional[str] = None) -> Optional[str]:
    """
    异步核心：先下载媒体内容字节，再基于【图片视觉内容唯一标识（dHash）】或【文件二进制内容哈希】生成唯一文件名。
    彻底摒弃基于 URL 的 MD5 命名，杜绝同图异链或平台二次上传换链导致的重复落地与自产自销复用。
    """
    import aiofiles
    import httpx

    save_dir = resolve_cross_platform_path(save_dir, get_media_save_dir())
    if proxy == "http://127.0.0.1:7890":
        proxy = get_proxy_url(default=proxy)

    suffix = Path(urlparse(url).path).suffix.lower()
    if suffix not in (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".mp4", ".mov", ".webm"):
        suffix = ".jpg"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    try:
        chunks = []
        async with httpx.AsyncClient(proxy=proxy, verify=False) as client:
            async with client.stream("GET", url, headers=headers, timeout=30.0) as response:
                response.raise_for_status()
                async for chunk in response.aiter_bytes():
                    if chunk:
                        chunks.append(chunk)

        raw_bytes = b"".join(chunks)
        if not raw_bytes:
            return None

        # 优先按图片视觉内容生成唯一标识；非图片媒体（如视频）按二进制内容生成 SHA256 标识
        content_id = compute_image_content_id(raw_bytes)
        if not content_id:
            content_id = hashlib.sha256(raw_bytes).hexdigest()[:32]

        save_dir_path = Path(save_dir)
        save_dir_path.mkdir(parents=True, exist_ok=True)

        # 若目标目录下已存在相同内容标识的图片（无论后缀是 .jpg/.png/.webp），直接复用已有文件
        for ext in (suffix, ".jpg", ".jpeg", ".png", ".webp"):
            candidate = save_dir_path / f"{content_id}{ext}"
            if candidate.exists() and candidate.stat().st_size > 0:
                return str(candidate.resolve())

        save_path = save_dir_path / f"{content_id}{suffix}"
        async with aiofiles.open(save_path, "wb") as f:
            await f.write(raw_bytes)

        return str(save_path.resolve())

    except Exception as e:
        print(f"[ERROR] 下载失败 {url}: {e}")
        return None


def download_web_media(url: str, save_dir: Optional[str] = None, proxy: Optional[str] = None) -> Optional[str]:
    """同步入口：下载网络媒体文件并按纯内容唯一标识命名，返回本地绝对路径。"""
    return asyncio.run(_download_media_async(url, save_dir, proxy=proxy))
