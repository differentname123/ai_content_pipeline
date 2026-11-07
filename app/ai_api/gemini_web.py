import asyncio
import json
import os
import threading
import time
import re
import traceback
import random
import sys
from datetime import datetime
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from gemini_webapi import GeminiClient, set_log_level

from common.common_utils import save_json, CONFIG_DIR, get_proxy_url

# 设置日志级别
set_log_level("INFO")

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_FILE = str(CONFIG_DIR / "gemini_web.json")
STATS_FILE = CONFIG_FILE.replace(".json", "_stats.json")


# ==========================================
# 1. 基础工具类：文件锁 (保持不变)
# ==========================================

class SimpleFileLock:
    def __init__(self, lock_file, timeout=10):
        self.lock_file = lock_file
        self.timeout = timeout

    def __enter__(self):
        start_time = time.time()
        while True:
            try:
                self.fd = os.open(self.lock_file, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                return self
            except FileExistsError:
                if time.time() - start_time > self.timeout:
                    raise TimeoutError(f"获取文件锁超时: {self.lock_file}")
                time.sleep(0.1)

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            os.close(self.fd)
            os.remove(self.lock_file)
        except OSError:
            pass


# ==========================================
# 2. 核心修改：账号管理器 (保持不变)
# ==========================================

class GeminiAccountManager:
    def __init__(self, config_path, stats_path):
        self.config_path = config_path
        self.stats_path = stats_path
        self.lock_path = str(stats_path) + ".lock"

    def _read_json_safe(self, path):
        if not os.path.exists(path):
            return {}
        try:
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                return json.loads(content) if content else {}
        except Exception:
            return {}

    # 【修改 1】: 将超时时间从 300s 调整为 600s (10分钟)
    def _check_and_reset_stuck_accounts(self, stats_data, timeout_seconds=600):
        """检查并重置僵死账号，默认超时时间为10分钟"""
        now = datetime.now()
        for name, info in stats_data.items():
            if info.get('status') == 'using':
                last_time_str = info.get('last_used_time', '')
                if last_time_str:
                    try:
                        last_time = datetime.strptime(last_time_str, "%Y-%m-%d %H:%M:%S")
                        if (now - last_time).total_seconds() > timeout_seconds:
                            print(f"[Manager] 账号 {name} 'using'状态超时({timeout_seconds}s)，强制重置为'idle'")
                            info['status'] = 'idle'
                            info['last_error_info'] = "System: Force reset due to timeout"
                    except ValueError:
                        # 如果时间格式不正确，也将其重置以避免永久锁定
                        info['status'] = 'idle'
                        info['last_error_info'] = "System: Force reset due to invalid time format"
        return stats_data

    def allocate_account(self, model_name):
        """
        申请账号：
        1. 同步 Config 与 Stats (删除废弃账号，添加新账号)。
        2. 找到 idle 账号。
        3. 立即增加调用计数。
        4. 返回 name 和 cookie (cookie 来自 config)。
        """
        with SimpleFileLock(self.lock_path):
            # 1. 读取 Config (作为 Cookie 和账号存在性的权威来源)
            raw_config = self._read_json_safe(self.config_path)
            config_list = raw_config.get('cookie_list', [])

            # 构建 config 映射: name -> cookie
            # 构建 config 映射: name -> cookie
            # [修改点] 增加过滤条件：必须存在 name，且 cookie_str 存在且去除空格后不为空
            valid_accounts_map = {
                item['name']: item['cookie_str']
                for item in config_list
                if item.get('name') and item.get('cookie_str', '').strip()
            }
            # ==========================================
            # 【新增】打印账号统计日志
            # ==========================================
            total_accounts = len(config_list)
            valid_accounts = len(valid_accounts_map)
            invalid_accounts = total_accounts - valid_accounts
            print(
                f"[Manager] 账号池状态: 配置总数 {total_accounts} 个 | 有效可用 {valid_accounts} 个 | 自动过滤无效 {invalid_accounts} 个")
            # ==========================================
            # 2. 读取 Stats
            stats = self._read_json_safe(self.stats_path)

            # 3. [关键调整] 同步逻辑：Stats 必须完全匹配 Config 的 key

            # 3.1 删除：Config 里没有的，Stats 里也要删掉
            current_stats_keys = list(stats.keys())
            for name in current_stats_keys:
                if name not in valid_accounts_map:
                    print(f"[Manager] 检测到账号 {name} 已从配置中移除，同步删除统计信息。")
                    del stats[name]

            # 3.2 新增：Config 里有的，Stats 里没有的，初始化
            for name in valid_accounts_map:
                if name not in stats:
                    stats[name] = {
                        "status": "idle",
                        "last_used_time": "",
                        "last_error_info": None,
                        "model_usage": {}
                    }

            # 4. 清理僵死状态
            stats = self._check_and_reset_stuck_accounts(stats)

            # 5. 筛选可用账号
            candidates = []
            for name, info in stats.items():
                # 【修复】必须检查状态是否为 idle
                if info.get('status') != 'idle':
                    continue

                # 下面的逻辑保持不变
                count = info.get('model_usage', {}).get(model_name, {}).get('count', 0)
                candidates.append({
                    'name': name,
                    'count': count
                })

            if not candidates:
                save_json(self.stats_path, stats)
                return None, None

            # 6. 排序与选择
            random.shuffle(candidates)
            best_account = sorted(candidates, key=lambda x: x['count'])[0]
            target_name = best_account['name']

            # 7. [关键调整] 状态更新：立即增加计数，不再等 release
            target_info = stats[target_name]
            target_info['status'] = 'using'
            target_info['last_used_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            if 'model_usage' not in target_info:
                target_info['model_usage'] = {}
            if model_name not in target_info['model_usage']:
                target_info['model_usage'][model_name] = {'count': 0}

            # 立即 +1
            target_info['model_usage'][model_name]['count'] += 1

            # 8. 写回 Stats (不含 cookie)
            with open(self.stats_path, 'w', encoding='utf-8') as f:
                json.dump(stats, f, indent=4, ensure_ascii=False)

            # 9. 返回 Name 和 Cookie (从 Config 字典中取)
            return target_name, valid_accounts_map[target_name]

    def release_account(self, account_name, error_info=None):
        """
        释放账号：
        只更新状态和错误信息，不处理计数（因为 allocate 时已经加过了）。
        """
        with SimpleFileLock(self.lock_path):
            stats = self._read_json_safe(self.stats_path)

            if account_name in stats:
                info = stats[account_name]
                info['status'] = 'idle'
                info['last_used_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 更新为结束时间

                if error_info:
                    info['last_error_info'] = str(error_info)[0:500]
                else:
                    info['last_error_info'] = None

            with open(self.stats_path, 'w', encoding='utf-8') as f:
                json.dump(stats, f, indent=4, ensure_ascii=False)


# ==========================================
# 3. 异步请求逻辑 (核心部分修改：增加图片保存支持)
# ==========================================

async def _do_gemini_request(cookie_str, prompt, model_name, files, output_img_dir="output_images"):
    """
    修改点：支持保存响应中的图片，并返回 (text, image_paths)
    """
    PROXY_URL = get_proxy_url()

    def _get_val(k, t):
        m = re.search(f"{re.escape(k)}=([^;]+)", t)
        return m.group(1) if m else None

    psid = _get_val("__Secure-1PSID", cookie_str)
    psidts = _get_val("__Secure-1PSIDTS", cookie_str)

    if not psid:
        raise ValueError("Cookie 无效：缺少 __Secure-1PSID")

    # 保持与原始代码一致，添加文件存在性检查
    if files:
        for f in files:
            if not os.path.exists(f):
                raise FileNotFoundError(f"文件不存在: {f}")

    client = GeminiClient(psid, psidts, proxy=PROXY_URL)

    try:
        await client.init(timeout=600, auto_close=False, auto_refresh=True)
        response = await client.generate_content(prompt, model=model_name, files=files)

        # === 新增：处理图片生成与保存逻辑 ===
        saved_image_paths = []
        if response.images:
            # 确保输出目录存在 (使用当前文件的同级或相对目录均可)
            save_dir = os.path.join(BASE_DIR, output_img_dir)
            os.makedirs(save_dir, exist_ok=True)

            for i, image in enumerate(response.images):
                # 生成唯一文件名防止并发覆盖
                filename = f"gen_img_{int(time.time() * 1000)}_{i}.png"
                # 调用底层的异步 save 方法保存图片
                await image.save(path=save_dir + "/", filename=filename, verbose=False)
                # 记录绝对路径
                saved_image_paths.append(os.path.join(save_dir, filename))

        # 返回 文本 和 成功保存的图片路径列表
        return response.text, saved_image_paths
    except Exception as e:
        # 向上抛出异常，由外部统一处理
        raise e


# ==========================================
# 4. 对外接口 (修改：返回接收结构)
# ==========================================


manager = GeminiAccountManager(str(CONFIG_FILE), str(STATS_FILE))


def generate_gemini_content_managed(prompt, model_name="gemini-3.0-pro", files=None, wait_timeout=600,
                                    output_img_dir="output_images"):
    """
    对外提供的统一接口。

    Args:
        prompt (str): 提示词。
        model_name (str): 模型名称。gemini-3.0-pro, gemini-2.5-pro, gemini-2.5-flash
        files (list, optional): 文件路径列表。
        wait_timeout (int): 当无可用账号时，最长等待时间（秒）。
        output_img_dir (str): 生成图片的保存目录。

    Returns:
        tuple: (error_info, response_str, saved_images)
    """
    pid = os.getpid()
    tid = threading.get_ident()
    log_prefix = f"[System][PID:{pid},TID:{tid}]"
    # ====================================

    start_time = time.time()
    account_name, cookie = None, None

    # 1. 循环申请账号，直到成功或超时
    while time.time() - start_time < wait_timeout:
        account_name, cookie = manager.allocate_account(model_name)
        if account_name:
            break

        elapsed = int(time.time() - start_time)
        print(f"{log_prefix} 无可用账号，进入等待... (已等待 {elapsed}s / {wait_timeout}s)")
        time.sleep(random.uniform(10, 20))

    if not account_name:
        return f"System Busy: 等待 {wait_timeout} 秒后仍无可用账号。", None, None

    print(f"{log_prefix} 分配账号: {account_name}")

    error_detail = None
    result_text = None
    saved_images = []

    try:
        # === 修改点：同时接收 result_text 和 saved_images ===
        result_text, saved_images = asyncio.run(_do_gemini_request(cookie, prompt, model_name, files, output_img_dir))
    except Exception as e:
        error_detail = f"发生错误: {str(e)}\n\n堆栈追踪:\n{traceback.format_exc()}"
    finally:
        print(f"{log_prefix} 释放账号: {account_name}")
        manager.release_account(account_name, error_detail)

    # === 修改点：返回 3个参数 ===
    return error_detail, result_text, saved_images


# ==========================================
# 测试部分 (修改以适配新的返回值格式)
# ==========================================

if __name__ == "__main__":

    print("开始测试...")
    test_file = [r"C:\Users\zxh\Desktop\temp\test.mp4"]

    # 明确在 prompt 中包含“生成一张”以触发模型画图工具
    prompt = "你是谁？"

    for i in range(1):  # 测试1次即可验证图片生成
        try:
            # === 修改点：接收 err, res, images 三个返回值 ===
            err, res, images = generate_gemini_content_managed(
                prompt=prompt,
                model_name="gemini-3-pro-advanced",
                # files=test_file
            )

            if err:
                print("\n======== 调用失败 ========")
                print(err)
            else:
                print("\n======== 调用成功 ========")
                print(f"文本回复:\n{res}")

                # === 新增：打印图片结果 ===
                if images:
                    print(f"\n成功生成了 {len(images)} 张图片，保存在以下路径：")
                    for img in images:
                        print(f" - {img}")
                else:
                    print("\n[注] 模型未返回任何图片。")

        except Exception as e:
            print(f"测试过程中发生异常: {str(e)}")