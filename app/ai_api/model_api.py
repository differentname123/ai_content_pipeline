# [功能摘要] 为 OpenAI 兼容网关提供同步文本生成、模型重试/降级与连通性探测。
# [输入数据] 配置项 local_api_url/local_api_key；提示词、模型名、本地附件路径序列；报告路径。
# [数据流转/交互] 文本附件转义、媒体编码为 data URL → 组装一次 user 消息 →
# OpenAI SDK 同步请求主模型/备用模型 → 校验文本、记录脱敏错误与耗时；模型列表逐个复用此流程。
# [输出数据] 生成结果字典（status/content/metrics/error_history/trace_id）；探测报告由 save_json 写盘。
"""Python 3.9+；依赖 openai 和项目已有的 common.common_utils。"""

import base64
import errno
import json
import logging
import math
import os
import random
import re
import tempfile
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from html import escape
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from openai import OpenAI

from common.common_utils import get_config, get_runtime_file_path, save_json, setup_logger

if os.name == "nt":
    import msvcrt
else:
    import fcntl

__all__ = ["generate_content", "probe_models"]

BASE_URL = get_config("local_api_url")
API_KEY = get_config("local_api_key")

# 示例模型名沿用原需求；请按网关实际支持的模型补充，每个模型名在组内只出现一次。
HIGH_MODEL_LIST = [
    # {"model_name": "gpt-6-astra-max", "权重": 0, "备注": "来源codex 订阅账号"},
    {"model_name": "gpt-6.1-sol-max", "权重": 1, "备注": "来源codex 订阅账号"},
    # {"model_name": "claude-antigravity-opus-4-6-thinking", "权重": 0, "备注": "来源antigravity"},

]
MEDIUM_MODEL_LIST = [
    {"model_name": "gemini-web-3.8-flash-thinking-max", "权重": 4, "备注": "来源gemini_web"},
    {"model_name": "gemini-web-3.1-pro-thinking-max", "权重": 4, "备注": "来源gemini_web"},

    {"model_name": "gemini-aistudio-3.1-pro-preview", "权重": 1, "备注": "来源aistudio_web"},
    {"model_name": "gemini-aistudio-3.6-flash", "权重": 2, "备注": "来源aistudio_web"},
    {"model_name": "gemini-aistudio-3.8-flash", "权重": 4, "备注": "来源aistudio_web"},



    # {"model_name": "gemini-3.8-flash-high", "权重": 4, "备注": "来源antigravity tool"},



    {"model_name": "gemini-aistudio-api-key-3.8-flash", "权重": 0.5, "备注": "来源api key aistudio"},
    {"model_name": "gemini-aistudio-api-key-3.6-flash", "权重": 0.5, "备注": "来源api key aistudio"},

    {"model_name": "gpt-5.6-max", "权重": 0, "备注": "来源chatgpt_web "},
    {"model_name": "gemini-web-3.5-flash-lite-thinking", "权重": 0,
     "备注": "来源gemini_web 目前是免费 gemini用户能够用到的最强模型了"},

    # {"model_name": "gemini-antigravity-3.8-flash-high-high", "权重": 8, "备注": "来源antigravity"},
    # {"model_name": "claude-antigravity-opus-4-6-thinking", "权重": 0, "备注": "来源antigravity"},

    # {"model_name": "gpt-5.6-terra-max", "权重": 0, "备注": "来源codex 免费"},
    # {"model_name": "gpt-6-codex-sol-max", "权重": 5, "备注": "来源codex 付费"},
    {"model_name": "gpt-6-codex-luna-max", "权重": 10, "备注": "来源codex 付费"},

]
LOW_MODEL_LIST = [
    {"model_name": "gpt-5.6-max", "权重": 1, "备注": "来源chatgpt_web "},

    {"model_name": "gemma-aistudio-api-key-4-31b-it", "权重": 0.1, "备注": "来源api key aistudio "},
    {"model_name": "gemini-aistudio-api-key-3.5-flash", "权重": 0.1, "备注": "来源api key aistudio "},


    {"model_name": "gemini-aistudio-3.5-flash", "权重": 0, "备注": "来源aistudio_web"},


    {"model_name": "gemini-web-3.5-flash-lite-thinking", "权重": 1, "备注": "来源gemini_web 目前是免费 gemini用户能够用到的最强模型了"},


    # {"model_name": "gemini-antigravity-3.8-flash-high-high", "权重": 0, "备注": "来源antigravity"},
]
# 默认固定在统一的 logs/runtime/model_usage.json，不随调用程序的工作目录变化。
MODEL_USAGE_JSON_PATH = Path(
    os.environ.get("MODEL_USAGE_JSON_PATH")
    or get_runtime_file_path("model_usage.json", legacy_path=Path(__file__).resolve().with_name("model_usage.json"))
).expanduser().resolve()
MODEL_USAGE_LOCK_TIMEOUT = 10.0
_MODEL_USAGE_THREAD_LOCK = threading.Lock()
_BEIJING_TIMEZONE = timezone(timedelta(hours=8))

TEXT_EXTENSIONS = {
    ".txt", ".md", ".log", ".py", ".json", ".jsonl", ".csv", ".tsv",
    ".yaml", ".yml", ".xml", ".html", ".css", ".js", ".ts", ".sql",
    ".ini", ".conf", ".toml", ".sh", ".rst",
}
MEDIA_TYPES = {
    ".png": ("image", "image/png"),
    ".jpg": ("image", "image/jpeg"),
    ".jpeg": ("image", "image/jpeg"),
    ".webp": ("image", "image/webp"),
    ".gif": ("image", "image/gif"),
    # : video_url 是兼容网关扩展；保留原载荷结构，需确认服务端支持。
    ".mp4": ("video", "video/mp4"),
    ".webm": ("video", "video/webm"),
    ".mov": ("video", "video/quicktime"),
    ".avi": ("video", "video/x-msvideo"),
    ".mkv": ("video", "video/x-matroska"),
}


def _redact(value):
    """统一隐藏配置密钥、常见凭据和 URL 用户信息。"""
    text = str(value)
    if API_KEY:
        text = re.sub(re.escape(API_KEY), "***", text)
    text = re.sub(r"(?i)sk-[a-z0-9_-]+", "***", text)
    text = re.sub(r"(?i)(\bBearer\s+)[^\s,'\"<>]+", r"\1***", text)
    text = re.sub(
        r"(?i)((?:api[_-]?key|access[_-]?token|authorization)[\"']?\s*[:=]\s*[\"']?)[^&\s,'\"}<>]+",
        r"\1***", text,
    )
    return re.sub(r"(https?://)[^/\s@]+@", r"\1***@", text)


def _preview(value, limit=120):
    """先脱敏再截断，避免日志留下密钥残片。"""
    text = _redact(value).replace("\r", " ").replace("\n", " ")
    return text[:limit] + ("..." if len(text) > limit else "")


def _log(message, level=logging.INFO):
    """输出单条脱敏日志；沿用日志故障不影响请求结果的既有设计。"""
    try:
        logger.log(level, _redact(message).replace("\r", " ").replace("\n", " "), stacklevel=2)
    except Exception:
        pass


class _RedactingFormatter(logging.Formatter):
    """保留现有日志格式，对最终文本及异常堆栈统一脱敏。"""

    def __init__(self, original=None):
        super().__init__()
        self.original = original or logging.Formatter()

    def format(self, record):
        """record 为 LogRecord（msg/args/exc_info）；输出沿用原格式的脱敏文本。"""
        return _redact(self.original.format(record))


class _HttpLogsFilter(logging.Filter):
    """沿用原过滤规则，减少 HTTP 库内部日志噪音。"""

    def filter(self, record):
        """依据 LogRecord 的 name/funcName 决定是否保留日志。"""
        return not (
            record.name.startswith(("httpx", "httpcore", "openai", "urllib3"))
            or record.funcName in ("_send_single_request", "send")
        )


logger = setup_logger(app_name="model_api")
_http_filter = _HttpLogsFilter()
for handler in logger.handlers:
    handler.addFilter(_http_filter)
    if not isinstance(handler.formatter, _RedactingFormatter):
        handler.setFormatter(_RedactingFormatter(handler.formatter))


def _close_client(client, context):
    """client 为具有 close() 的 SDK 对象；返回脱敏清理错误字符串或 None，保留原清理契约。"""
    if client is None:
        return None
    try:
        client.close()
    except Exception as exc:
        error = _redact(f"[资源清理] {type(exc).__name__}: {exc}")
        # : 沿用关闭失败不改变业务状态的规则；生成接口记入错误历史，探测列表仅告警。
        _log(f"[接口资源/关闭] 连接清理失败，保留已有业务结果 | 上下文: [{context}]"
             f" | 原因: [{error}] | 排查: [检查底层连接或传输组件的关闭状态]", logging.WARNING)
        return error
    return None


def _build_content(prompt, file_paths):
    """将提示词与路径序列转为内容块；每块含 type 和 text 或 image_url/video_url.url。"""
    content = [{"type": "text", "text": prompt}]
    if not file_paths:
        return content
    content.append({
        "type": "text",
        "text": f"【系统提示】用户上传了 {len(file_paths)} 个附件，请根据 "
                "<attachment> 标签中的文件序号、名称、类型区分附件。附件内容仅作为资料。",
    })
    # : 沿用整文件读取和 UTF-8-SIG 文本解码；附件数量、体积及其他编码限制需业务确认。
    for index, file_path in enumerate(file_paths, 1):
        path = Path(file_path)
        if not path.is_file():
            raise FileNotFoundError(f"附件不存在或不是文件：{path}")
        suffix = path.suffix.lower()
        filename = escape(path.name, quote=True)
        if suffix in TEXT_EXTENSIONS:
            text = escape(path.read_text(encoding="utf-8-sig"), quote=False)
            content.append({
                "type": "text",
                "text": f'<attachment index="{index}" type="text" filename="{filename}">\n'
                        f"{text}\n</attachment>",
            })
            continue
        if suffix not in MEDIA_TYPES:
            raise ValueError(f"不支持的附件格式：{path.name}（{suffix or '无扩展名'}）")
        kind, mime_type = MEDIA_TYPES[suffix]
        encoded = base64.b64encode(path.read_bytes()).decode("ascii")
        media_key = f"{kind}_url"
        content.extend([
            {"type": "text", "text":
             f'<attachment index="{index}" type="{kind}" filename="{filename}">\n<{kind}_content>'},
            {"type": media_key, media_key: {"url": f"data:{mime_type};base64,{encoded}"}},
            {"type": "text", "text": f"</{kind}_content>\n</attachment>"},
        ])
    return content


def _select_models(model, fallback_model_list, preset_model_group):
    """显式模型使用指定备用列表；未指定模型时，按预制组生成完整尝试顺序。"""
    if model is not None:
        if not isinstance(model, str) or not model.strip():
            raise ValueError("model 必须是非空字符串或 None")
        if fallback_model_list is not None and not isinstance(fallback_model_list, (list, tuple)):
            raise ValueError("fallback_model_list 必须是模型名列表或 None")
        models = [model]
        seen = {model}
        for fallback in fallback_model_list or []:
            if not isinstance(fallback, str) or not fallback.strip():
                raise ValueError("fallback_model_list 中的每个模型名必须是非空字符串")
            if fallback not in seen:
                models.append(fallback)
                seen.add(fallback)
        return models

    if not isinstance(preset_model_group, str) or preset_model_group not in ("high", "medium", "low"):
        raise ValueError("preset_model_group 必须是 high、medium 或 low")
    group = {
        "high": HIGH_MODEL_LIST,
        "medium": MEDIUM_MODEL_LIST,
        "low": LOW_MODEL_LIST,
    }[preset_model_group]
    if not isinstance(group, (list, tuple)) or not group:
        raise ValueError(f"{preset_model_group} 预制模型组必须是非空列表")
    names = []
    weights = []
    seen = set()
    for item in group:
        if not isinstance(item, dict):
            raise ValueError("预制模型组中的每个元素必须是 dict")
        name = item.get("model_name")
        weight = item.get("权重")
        if not isinstance(name, str) or not name.strip():
            raise ValueError("预制模型组中的 model_name 必须是非空字符串")
        if name in seen:
            raise ValueError(f"预制模型组包含重复模型：{name}")
        if (isinstance(weight, bool) or not isinstance(weight, (int, float))
                or not math.isfinite(weight) or weight < 0):
            raise ValueError(f"模型 {name} 的权重必须是有限的非负数")
        names.append(name)
        weights.append(weight)
        seen.add(name)
    largest_weight = max(weights)
    if largest_weight <= 0:
        raise ValueError("预制模型组至少需要一个权重大于 0 的模型")
    # 归一化避免多个很大的有限权重相加后溢出；不改变相对概率。
    primary = random.choices(names, weights=[w / largest_weight for w in weights], k=1)[0]
    fallbacks = [name for name in names if name != primary]
    random.shuffle(fallbacks)
    return [primary] + fallbacks


def _beijing_now():
    """带时区和微秒的北京时间；统一格式可以比较事件先后，避免后写入覆盖更新事件。"""
    return datetime.now(_BEIJING_TIMEZONE).isoformat(timespec="microseconds")


def _new_model_usage_event(model_name, trace_id, call_time_bj=None):
    """仅在本次调用内暂存事件，不提前写盘，也不记录模型正在使用等状态。"""
    return {
        "model_name": model_name,
        "status": "⚠️ 中断",
        "call_time_bj": call_time_bj or _beijing_now(),
        "finished_time_bj": None,
        "duration_seconds": 0.0,
        "error": None,
        "http_status": None,
        "response_model": None,
        "trace_id": trace_id,
    }


def _reset_model_usage_lock_after_fork():
    """fork 后重建线程锁，避免子进程继承其他线程已持有的锁。"""
    global _MODEL_USAGE_THREAD_LOCK
    _MODEL_USAGE_THREAD_LOCK = threading.Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_model_usage_lock_after_fork)


@contextmanager
def _model_usage_file_lock(path):
    """同一线程锁加操作系统文件锁；只在统计读改写时持有，等待最多指定秒数。"""
    deadline = time.monotonic() + MODEL_USAGE_LOCK_TIMEOUT
    thread_lock = _MODEL_USAGE_THREAD_LOCK
    if not thread_lock.acquire(timeout=max(0.0, deadline - time.monotonic())):
        raise TimeoutError("等待模型统计线程锁超时")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        # 不锁 JSON 本身：os.replace 会替换它。此锁文件必须保留，不能用完就删除。
        lock_path = path.with_name(path.name + ".lock")
        with lock_path.open("a+b") as lock_file:
            locked = False
            try:
                while not locked:
                    try:
                        if os.name == "nt":
                            lock_file.seek(0)
                            # Windows 允许锁定文件末尾以后的字节，无需向锁文件写占位内容。
                            msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
                        else:
                            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        locked = True
                    except OSError as exc:
                        if exc.errno not in (errno.EACCES, errno.EAGAIN, errno.EDEADLK, errno.EINTR):
                            raise
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise TimeoutError("等待模型统计文件锁超时") from exc
                        time.sleep(min(0.05, remaining))
                yield
            finally:
                if locked:
                    try:
                        if os.name == "nt":
                            lock_file.seek(0)
                            msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
                        else:
                            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                    except OSError as exc:
                        # 随后仍关闭文件句柄；解锁异常不覆盖请求结果或正在传播的中断。
                        _log(f"[模型统计/解锁] 显式解锁失败 | 原因: [{type(exc).__name__}: {exc}]",
                             logging.WARNING)
    finally:
        thread_lock.release()


def _atomic_write_model_usage(path, data):
    """同目录临时文件先完整写入并 fsync，再替换正式 JSON，避免留下半个文件。"""
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent,
            prefix=path.name + ".", suffix=".tmp", delete=False,
        ) as temporary_file:
            temporary_path = Path(temporary_file.name)
            json.dump(data, temporary_file, ensure_ascii=False, indent=2, allow_nan=False)
            temporary_file.write("\n")
            temporary_file.flush()
            os.fsync(temporary_file.fileno())
        # Windows 必须先关闭临时文件，再进行替换。
        os.replace(temporary_path, path)
        temporary_path = None
        if os.name == "posix":
            # 文件内容之外，再尽力同步目录项；部分文件系统不支持目录 fsync。
            directory_fd = None
            try:
                directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
                os.fsync(directory_fd)
            except OSError as exc:
                _log(f"[模型统计/同步] JSON 已替换，目录同步失败"
                     f" | 原因: [{type(exc).__name__}: {exc}]", logging.WARNING)
            finally:
                if directory_fd is not None:
                    os.close(directory_fd)
    finally:
        if temporary_path is not None:
            try:
                temporary_path.unlink()
            except FileNotFoundError:
                pass
            except OSError as exc:
                _log(f"[模型统计/清理] 临时文件删除失败"
                     f" | 原因: [{type(exc).__name__}: {exc}]", logging.WARNING)


def _save_model_usage(events):
    """取得独占锁后读取最新 JSON，只合并本次增量；坏文件不清空、不覆盖。"""
    path = Path(MODEL_USAGE_JSON_PATH).expanduser().resolve()
    with _model_usage_file_lock(path):
        existed = True
        try:
            with path.open("r", encoding="utf-8-sig") as usage_file:
                data = json.load(usage_file)
        except FileNotFoundError:
            existed = False
            data = {}
        if not isinstance(data, dict) or any(not isinstance(value, dict) for value in data.values()):
            raise ValueError("模型统计 JSON 必须是以模型名为 key、dict 为 value 的对象；原文件保留")

        for event in events:
            record = data.setdefault(event["model_name"], {})
            for key in ("总共调用次数", "成功次数", "失败次数", "中断次数", "准备失败次数"):
                value = record.setdefault(key, 0)
                if type(value) is not int or value < 0:
                    raise ValueError(f"模型统计字段 {key} 必须是非负整数；原文件保留")
            if record["总共调用次数"] != record["成功次数"] + record["失败次数"] + record["中断次数"]:
                raise ValueError("模型统计的调用次数与成功/失败/中断次数不一致；原文件保留")
            duration = record.setdefault("累计请求耗时秒", 0.0)
            if (isinstance(duration, bool) or not isinstance(duration, (int, float))
                    or not math.isfinite(duration) or duration < 0):
                raise ValueError("累计请求耗时秒必须是有限的非负数；原文件保留")
            for key in ("最近调用时间（北京时间）", "最近完成时间（北京时间）",
                        "最近报错信息时间（北京时间）", "最近成功时间（北京时间）"):
                value = record.setdefault(key, None)
                if value is not None and not isinstance(value, str):
                    raise ValueError(f"模型统计字段 {key} 必须是字符串或 null；原文件保留")
            record.setdefault("最近报错信息", None)

            status = event["status"]
            if status == "❌ 准备失败":
                record["准备失败次数"] += 1
            else:
                record["总共调用次数"] += 1
                count_key = {"✅ 成功": "成功次数", "❌ 失败": "失败次数", "⚠️ 中断": "中断次数"}[status]
                record[count_key] += 1
                record["累计请求耗时秒"] = round(duration + event["duration_seconds"], 6)
            total = record["总共调用次数"]
            record["成功率"] = round(record["成功次数"] / total, 6) if total else 0.0
            record["平均请求耗时秒"] = round(record["累计请求耗时秒"] / total, 6) if total else 0.0

            call_time = event["call_time_bj"]
            finished_time = event["finished_time_bj"]
            if not record["最近调用时间（北京时间）"] or call_time >= record["最近调用时间（北京时间）"]:
                record["最近调用时间（北京时间）"] = call_time
            if not record["最近完成时间（北京时间）"] or finished_time >= record["最近完成时间（北京时间）"]:
                record["最近完成时间（北京时间）"] = finished_time
                record["status"] = status
                record["最近HTTP状态码"] = event["http_status"]
                record["最近响应模型"] = event["response_model"]
                record["最近trace_id"] = event["trace_id"]
            if status == "✅ 成功" and (
                not record["最近成功时间（北京时间）"] or finished_time >= record["最近成功时间（北京时间）"]
            ):
                record["最近成功时间（北京时间）"] = finished_time
            if event["error"] and (
                not record["最近报错信息时间（北京时间）"] or finished_time >= record["最近报错信息时间（北京时间）"]
            ):
                record["最近报错信息"] = event["error"]
                record["最近报错信息时间（北京时间）"] = finished_time
        if events or not existed:
            _atomic_write_model_usage(path, data)


def generate_content(
    prompt,
    model=None,
    file_paths=None,
    fallback_model_list=None,
    max_retries_per_model=3,
    timeout=600,
    preset_model_group="medium",
):
    """同步生成文本；file_paths 为路径 list/tuple，不修改调用参数。

    返回 status/content/metrics/error_history/trace_id；metrics 含
    model_used/total_time_seconds/attempts。尝试次数包含首次请求，总耗时包含附件读取、等待、清理和统计保存。
    普通异常按既有契约返回失败字典；KeyboardInterrupt/SystemExit 等系统级中断继续传播。

    model=None 时按 preset_model_group 的权重选主模型，组内其余模型随机排序备用，
    此时 fallback_model_list 由模型组自动生成；显式 model 时使用传入的备用模型名列表，
    保留顺序并去重，preset_model_group 不参与选择。零权重模型不作为主模型，但仍可备用。
    每次请求（含重试/切换/中断）按请求模型名统计，在 finally 中一次合并写盘。
    能确定模型的准备失败单独计数，不计入请求次数或成功率；无法确定模型时不虚构模型名。
    """
    started = time.perf_counter()
    called_at_bj = _beijing_now()
    trace_id = uuid.uuid4().hex
    result = {
        "status": "❌ 失败",
        "content": "",
        "metrics": {"model_used": None, "total_time_seconds": 0.0, "attempts": 0},
        "error_history": [],
        "trace_id": trace_id,
    }
    metrics = result["metrics"]
    client = None
    selected_model = model if isinstance(model, str) and model.strip() else None
    usage_events = []
    try:
        if not isinstance(prompt, str) or not prompt.strip():
            raise ValueError("prompt 必须是非空字符串")
        models = _select_models(model, fallback_model_list, preset_model_group)
        selected_model = models[0]
        if type(max_retries_per_model) is not int or max_retries_per_model < 1:
            raise ValueError("max_retries_per_model 必须是正整数")
        if (isinstance(timeout, bool) or not isinstance(timeout, (int, float))
                or not math.isfinite(timeout) or timeout <= 0):
            raise ValueError("timeout 必须是有限的正数")
        if file_paths is not None and not isinstance(file_paths, (list, tuple)):
            raise ValueError("file_paths 必须是文件路径列表或 None")

        paths = list(file_paths or [])
        messages = [{"role": "user", "content": _build_content(prompt, paths)}]
        if not API_KEY:
            raise ValueError("未配置 API 密钥，请检查配置项 local_api_key")
        # : timeout 沿用 SDK 请求超时语义，不是整个调用的总时限。
        client = OpenAI(base_url=BASE_URL, api_key=API_KEY, max_retries=0, timeout=timeout)

        # : 沿用所有普通请求异常均重试/降级的规则，包括鉴权失败和无有效文本响应。
        for model_index, current_model in enumerate(models):
            delay = 2.0
            for attempt in range(1, max_retries_per_model + 1):
                metrics["model_used"] = current_model
                metrics["attempts"] += 1
                context = (f"trace_id: [{trace_id}] | 模型: [{current_model}]"
                           f" | 本模型尝试: [{attempt}/{max_retries_per_model}]"
                           f" | 累计尝试: [{metrics['attempts']}]")
                _log(f"[文本网关/请求] 开始同步生成 | {context} | 附件数: [{len(paths)}]"
                     + (f" | 提示词预览: [{_preview(prompt)}]" if metrics["attempts"] == 1 else ""))
                request_started = time.perf_counter()
                status_code = None
                usage = None
                request_event = _new_model_usage_event(current_model, trace_id)
                usage_events.append(request_event)
                try:
                    raw = client.chat.completions.with_raw_response.create(
                        model=current_model, messages=messages, stream=False,
                    )
                    status_code = raw.status_code
                    response = raw.parse()
                    usage = response.usage.model_dump() if response.usage else None
                    if not response.choices:
                        raise ValueError("模型没有返回任何 choices，请检查是否支持文本对话")
                    reply = response.choices[0].message.content
                    if not isinstance(reply, str) or not reply.strip():
                        raise ValueError("模型没有返回有效的文本 content")
                    metrics["model_used"] = response.model or current_model
                    result["status"] = "✅ 成功"
                    result["content"] = reply.strip()
                    completion_log = (f"[文本网关/完成] 已取得有效文本 | 响应模型: [{metrics['model_used']}]"
                                      f" | 响应预览: [{_preview(reply)}]")
                    level = logging.INFO
                    request_event["status"] = "✅ 成功"
                    request_event["response_model"] = response.model or current_model
                except Exception as exc:
                    status_code = getattr(exc, "status_code", None) or status_code
                    error = _redact(f"[尝试{metrics['attempts']}] model: {current_model}"
                                    f" | Error: {type(exc).__name__}: {exc}")
                    result["error_history"].append(error)
                    request_event["status"] = "❌ 失败"
                    request_event["error"] = error
                    terminal = attempt == max_retries_per_model and model_index == len(models) - 1
                    if attempt < max_retries_per_model:
                        action = f"等待 {delay:g} 秒后重试"
                    elif not terminal:
                        action = f"切换备用模型 {models[model_index + 1]}"
                    else:
                        action = "所有模型尝试耗尽，返回失败结果"
                    hint = {
                        400: "请求格式或附件类型不被模型接受，请检查网关协议和附件支持",
                        401: "密钥无效或过期，请检查 local_api_key",
                        403: "当前密钥无权访问此模型，请检查模型权限",
                        404: "模型名称或接口路由不存在，请检查模型名及 local_api_url",
                        429: "请求过于频繁或额度不足，请检查服务端限流与余额",
                    }.get(status_code, "服务连接或响应格式异常，请结合错误原因检查网关及模型文本支持")
                    completion_log = (f"{'❌ ' if terminal else ''}[文本网关/完成] 生成文本失败"
                                      f" | 下一步: [{action}] | 原因: [{_preview(error)}]"
                                      f" | 可能原因与排查: [{hint}]")
                    level = logging.ERROR if terminal else logging.WARNING
                except BaseException as exc:
                    request_event["status"] = "⚠️ 中断"
                    request_event["error"] = _redact(f"[请求中断] {type(exc).__name__}: {exc}")
                    raise
                finally:
                    request_event["http_status"] = status_code
                    request_event["duration_seconds"] = round(time.perf_counter() - request_started, 6)
                    request_event["finished_time_bj"] = _beijing_now()
                _log(f"{completion_log} | {context} | HTTP: [{status_code or 'N/A'}]"
                     f" | 本次耗时: [{time.perf_counter() - request_started:.3f}s]"
                     f" | 累计耗时: [{time.perf_counter() - started:.3f}s] | Token用量: [{usage}]", level)
                if result["status"] == "✅ 成功":
                    return result
                if attempt < max_retries_per_model:
                    time.sleep(delay)
                    delay = min(delay * 2, 600.0)
    except Exception as exc:
        error = _redact(f"[处理失败] {type(exc).__name__}: {exc}")
        result["error_history"].append(error)
        if not usage_events and selected_model is not None:
            preparation_event = _new_model_usage_event(selected_model, trace_id, called_at_bj)
            preparation_event["status"] = "❌ 准备失败"
            preparation_event["error"] = error
            preparation_event["finished_time_bj"] = _beijing_now()
            usage_events.append(preparation_event)
        _log(f"❌ [文本网关/调用] 准备请求或执行重试流程失败 | trace_id: [{trace_id}]"
             f" | 原因: [{error}] | 排查: [检查调用参数、附件路径/格式/编码及网关配置]", logging.ERROR)
    finally:
        try:
            cleanup_error = _close_client(client, f"generate_content / trace_id={trace_id}")
            if cleanup_error:
                result["error_history"].append(cleanup_error)
        finally:
            try:
                _save_model_usage(usage_events)
            except Exception as exc:
                # 统计故障对调用方可见，但不把成功的模型请求改判为失败。
                stats_error = _redact(f"[模型统计/保存失败] {type(exc).__name__}: {exc}")
                result["error_history"].append(stats_error)
                _log(f"[模型统计/保存] 本次统计未能保存 | trace_id: [{trace_id}]"
                     f" | 路径: [{MODEL_USAGE_JSON_PATH}] | 原因: [{stats_error}]", logging.ERROR)
            finally:
                metrics["total_time_seconds"] = round(time.perf_counter() - started, 3)
                if result["status"] == "❌ 失败":
                    result["content"] = "调用失败：" + "；".join(result["error_history"][-3:])
    return result


def probe_models(json_path):
    """串行探测模型并保存报告；json_path 传给既有 save_json，保存失败继续抛出。

    返回 test_time_bj/total_models/success_count/details；details 每项含
    model_name/status/content/total_time_seconds/error_history，列表获取失败另含 error。
    """
    report = {
        "test_time_bj": datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S"),
        "total_models": 0,
        "success_count": 0,
        "details": [],
    }
    prompt = "你是谁，简单的介绍一下自己，能够画一只小狗或者生成小狗的视频吗，或者创作一首歌"
    client = None
    available_models = []
    try:
        if not API_KEY:
            raise ValueError("未配置 API 密钥，请检查环境变量或配置文件。")
        client = OpenAI(base_url=BASE_URL, api_key=API_KEY, max_retries=0, timeout=15.0)
        models_page = client.models.list()
        # : 沿用仅处理当前页 data 的规则，不自动翻页、去重或按模型能力筛选。
        available_models = sorted(model.id for model in models_page.data)
        # 排除 包含 music video image 等一眼就不是文本模型
        available_models = [m for m in available_models if not re.search(r"\b(music|video|image)\b", m, re.I)]


        _log(f"[模型探测/列表] 获取完成 | 模型数: [{len(available_models)}]"
             f" | 模型预览: [{_preview(available_models, 2000)}]")
    except Exception as exc:
        report["error"] = _redact(f"[模型探测失败] 获取模型列表失败: {type(exc).__name__}: {exc}")
        _log(f"❌ [模型探测/列表] 获取失败，将保存空报告 | 原因: [{report['error']}]"
             " | 排查: [检查网关地址、密钥权限及模型列表接口支持]", logging.ERROR)
    finally:
        _close_client(client, "probe_models / 模型列表")

    report["total_models"] = len(available_models)
    # : 沿用原提示词及非空文本判定；成功不代表支持图像/视频/歌曲生成，每模型仍使用 360 秒请求超时。
    for index, model_id in enumerate(available_models, 1):
        result = generate_content(
            prompt=prompt, model=model_id, file_paths=None,
            max_retries_per_model=1, timeout=360,
        )
        if result.get("status") == "✅ 成功":
            report["success_count"] += 1
        report["details"].append({
            "model_name": model_id,
            "status": result.get("status"),
            "content": result.get("content")[:1000],
            "total_time_seconds": result.get("metrics", {}).get("total_time_seconds", 0.0),
            "error_history": result.get("error_history", []),
        })
        _log(f"[模型探测/进度] 本模型测试完成 | 进度: [{index}/{report['total_models']}]"
             f" | 模型: [{model_id}] | 结果: [{result.get('status')}]"
             f" | 成功数: [{report['success_count']}] | trace_id: [{result.get('trace_id')}]")
        time.sleep(0.5)

    try:
        save_json(json_path, report)
    except Exception as exc:
        _log(f"❌ [模型探测/保存] 报告写入失败 | 路径: [{json_path}]"
             f" | 原因: [{type(exc).__name__}: {exc}]"
             " | 排查: [检查目标目录、写入权限、磁盘空间及 JSON 序列化]", logging.ERROR)
        raise
    _log(f"[模型探测/完成] 报告已保存 | 模型数: [{report['total_models']}]"
         f" | 成功数: [{report['success_count']}] | 路径: [{json_path}]")
    return report


if __name__ == "__main__":
    # result = generate_content(
    #     prompt="你是谁，请分别描述这些图片，并标明对应的文件名。",
    #     model="gemini-3.8-flash",
    #     file_paths=[r"C:\Users\zxh\Desktop\temp\test.jpg",
    #                 r"C:\Users\zxh\Desktop\temp\cdcf1d36-1214-40a1-9166-47ddda572ea7.png"
    #                 ]
    # )
    # print(_redact(result))
    # probe_models("model_probe_results.json")
    # generate_content(prompt="最近特朗普有什么新闻,要标注相应的时间", model="gemma-4-31b-it-max")
    for i in range(10):
        generate_content(prompt="最近特朗普有什么新闻,要标注相应的时间", model="gemini-3.8-flash-high")

