# -- coding: utf-8 --
""":authors:
    zhuxiaohu
:create_date:
    2026/4/7 0:32
:last_date:
    2026/11/7
:description:
    全局基础设施：统一路径解析、配置缓存、多模块独立日志、JSON 原子读写、LLM 输出解析、图片视觉哈希(dHash)与异步媒体下载。
"""
import ast
import asyncio
import hashlib
import json
import logging
import os
import re
import sys
import threading
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union
from urllib.parse import urlparse

from filelock import FileLock

# ==============================================================================
# 统一项目路径常量 (以 ai_content_pipeline 目录为独立项目根目录)
# 所有日志、故障取证快照、运行期状态文件统一收拢到 logs/ 目录下
# ==============================================================================
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
PROMPT_DIR = PROJECT_ROOT / "prompt"
DEFAULT_LOG_DIR = PROJECT_ROOT / "logs"
DEFAULT_FORENSICS_DIR = DEFAULT_LOG_DIR / "forensics"
DEFAULT_RUNTIME_DIR = DEFAULT_LOG_DIR / "runtime"
DEFAULT_MEDIA_DIR = PROJECT_ROOT / "media_downloads"
DEFAULT_SESSION_ROOT = PROJECT_ROOT / "browser_sessions"

for _d in (DEFAULT_LOG_DIR, DEFAULT_FORENSICS_DIR, DEFAULT_RUNTIME_DIR):
    _d.mkdir(parents=True, exist_ok=True)

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


def get_runtime_file_path(filename: str, legacy_path: Optional[Union[str, Path]] = None) -> Path:
    """
    获取运行期状态文件在 logs/runtime/ 下的标准路径。
    若新路径尚不存在且旧路径存在，则自动将旧文件复制迁移至新路径，确保平滑过渡。
    """
    target = DEFAULT_RUNTIME_DIR / filename
    if not target.exists() and legacy_path:
        legacy = Path(legacy_path)
        if legacy.exists() and legacy.is_file():
            try:
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(legacy.read_bytes())
            except Exception:
                pass
    return target


# ==============================================================================
# 日志系统：支持 root logger 默认挂载 + 按 app_name 独立分文件记录，彻底解决日志冲突
# ==============================================================================
_LOGGER_LOCK = threading.Lock()
_LOG_FORMATTER = logging.Formatter(
    "%(asctime)s,%(msecs)03d | %(levelname)s | [%(name)s:%(funcName)s] | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_NOISY_LOGGER_PREFIXES = (
    "httpx",
    "httpx2",
    "httpcore",
    "urllib3",
    "openai",
    "charset_normalizer",
    "filelock",
    "asyncio",
)
_NOISY_FUNC_NAMES = (
    "_send_single_request",
    "_send_handling_auth",
    "_send_handling_redirects",
)


class _ThirdPartyNoiseFilter(logging.Filter):
    """全局拦截底层 HTTP 客户端（如 httpx/httpx2/httpcore/openai）的请求流水噪音日志。"""

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno < logging.WARNING:
            if record.name.startswith(_NOISY_LOGGER_PREFIXES) or record.funcName in _NOISY_FUNC_NAMES:
                return False
        return True


_NOISE_FILTER = _ThirdPartyNoiseFilter()


def _attach_handlers(target_logger: logging.Logger, log_dir: str, app_name: str):
    log_file_path = os.path.join(log_dir, f"{app_name}.log")
    file_handler = TimedRotatingFileHandler(
        filename=log_file_path,
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8",
    )
    file_handler.setFormatter(_LOG_FORMATTER)
    file_handler.addFilter(_NOISE_FILTER)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(_LOG_FORMATTER)
    console_handler.addFilter(_NOISE_FILTER)

    target_logger.addFilter(_NOISE_FILTER)
    target_logger.addHandler(file_handler)
    target_logger.addHandler(console_handler)


def setup_logger(log_dir: Optional[str] = None, app_name: str = "BinanceBot") -> logging.Logger:
    """
    初始化日志记录器：
    - 默认 app_name="BinanceBot" 时初始化 root logger（供 logging.getLogger(__name__) 使用）；
    - 传入特定 app_name（如 "model_api"、"media_format"、"binance_playwright"）时，
      返回独立命名的 logger 并写入 logs/{app_name}.log，不再被 root logger 短路吞没。
    """
    resolved_dir = resolve_cross_platform_path(log_dir, DEFAULT_LOG_DIR)
    os.makedirs(resolved_dir, exist_ok=True)

    for noisy in _NOISY_LOGGER_PREFIXES:
        logging.getLogger(noisy).setLevel(logging.WARNING)

    with _LOGGER_LOCK:
        root_logger = logging.getLogger()
        root_logger.addFilter(_NOISE_FILTER)
        if not root_logger.handlers:
            root_logger.setLevel(logging.INFO)
            _attach_handlers(root_logger, resolved_dir, "BinanceBot")
            root_logger.propagate = False
        else:
            for h in root_logger.handlers:
                h.addFilter(_NOISE_FILTER)

        if not app_name or app_name == "BinanceBot":
            return root_logger

        named_logger = logging.getLogger(app_name)
        named_logger.addFilter(_NOISE_FILTER)
        if not named_logger.handlers:
            named_logger.setLevel(logging.INFO)
            _attach_handlers(named_logger, resolved_dir, app_name)
            named_logger.propagate = False
        else:
            for h in named_logger.handlers:
                h.addFilter(_NOISE_FILTER)
        return named_logger


logger = setup_logger()


# ==============================================================================
# 文件与 JSON 工具函数
# ==============================================================================
def read_json(json_path: Union[str, Path]) -> dict:
    """读取 JSON 文件并返回内容，不存在时返回空字典。"""
    path_str = str(json_path)
    if not os.path.exists(path_str):
        return {}

    with open(path_str, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"无法解析 JSON 文件 '{path_str}': {e}")


def save_json(json_path: Union[str, Path], data: Any):
    """使用跨进程文件锁 + 临时文件替换，安全原子写入 JSON 文件。"""
    path_str = str(json_path)
    dir_path = os.path.dirname(path_str)
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)

    lock_path = path_str + ".lock"
    with FileLock(lock_path):
        tmp_path = path_str + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4, default=str)
        os.replace(tmp_path, path_str)


def read_file_to_str(
    filepath: Union[str, Path],
    encoding: str = "utf-8",
    errors: str = "strict",
) -> str:
    """读取文本文件并返回完整字符串。"""
    p = Path(filepath)
    with p.open("r", encoding=encoding, errors=errors) as f:
        return f.read()


def string_to_object(input_str: str):
    """
    从字符串中提取并解析出 Python 列表或字典对象（支持 Markdown 代码块、注释剥离、尾随逗号修复、json/ast 双引擎解析）。
    """
    if not isinstance(input_str, str):
        raise TypeError(f"输入必须是字符串，但收到了 {type(input_str).__name__}。")

    def _create_error_message(reason: str) -> str:
        preview = (input_str[:50] + "...") if len(input_str) > 50 else input_str
        return f"{reason} | 输入内容预览: '{preview}'"

    first_bracket = input_str.find("[")
    first_curly = input_str.find("{")

    if first_bracket == -1 and first_curly == -1:
        raise ValueError(_create_error_message("输入字符串中未找到疑似列表或字典的起始符号 '[' 或 '{'"))

    if first_bracket == -1:
        start_pos = first_curly
    elif first_curly == -1:
        start_pos = first_bracket
    else:
        start_pos = min(first_bracket, first_curly)

    end_pos = max(input_str.rfind("]"), input_str.rfind("}"))
    if end_pos <= start_pos:
        raise ValueError(_create_error_message("未找到与起始括号匹配的结束括号 ']' 或 '}'"))

    potential_obj_str = input_str[start_pos : end_pos + 1]
    potential_obj_str = re.sub(r"//.*", "", potential_obj_str)
    potential_obj_str = re.sub(r"/\*[\s\S]*?\*/", "", potential_obj_str, flags=re.MULTILINE)
    potential_obj_str = re.sub(r",\s*([}\]])", r"\1", potential_obj_str)
    cleaned_str = potential_obj_str.strip()

    try:
        return json.loads(cleaned_str)
    except json.JSONDecodeError:
        try:
            return ast.literal_eval(cleaned_str)
        except (ValueError, SyntaxError, MemoryError) as e:
            cleaned_preview = (cleaned_str[:150] + "...") if len(cleaned_str) > 150 else cleaned_str
            error_reason = f"无法将提取的内容解析为列表或字典，解析器错误: {e}"
            raise ValueError(
                f"{_create_error_message(error_reason)}\n尝试解析的内容 (清理后): '''{cleaned_preview}'''"
            )


# ==============================================================================
# 配置读取与内存缓存（基于文件 mtime 自动失效，避免高频磁盘 I/O）
# ==============================================================================
_MISSING = object()
_CONFIG_CACHE_LOCK = threading.Lock()
_CONFIG_CACHE: Tuple[float, Dict[str, Any]] = (0.0, {})


def _load_cached_config() -> Dict[str, Any]:
    global _CONFIG_CACHE
    config_file = CONFIG_DIR / "config.json"
    if not config_file.exists():
        raise FileNotFoundError(f"配置文件 '{config_file}' 不存在，请检查文件路径。")

    mtime = config_file.stat().st_mtime
    with _CONFIG_CACHE_LOCK:
        cached_mtime, cached_data = _CONFIG_CACHE
        if cached_data and cached_mtime == mtime:
            return cached_data

        try:
            with open(config_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"配置文件 '{config_file}' 格式错误: {e}")

        _CONFIG_CACHE = (mtime, data)
        return data


def get_config(key: str, default: Any = _MISSING) -> Any:
    """
    从 config.json 文件中获取指定字段的值（带 mtime 内存缓存）。
    :param key: 配置字段名
    :param default: 可选默认值；若未传且字段不存在则抛出 KeyError（保持向后兼容）
    """
    config_data = _load_cached_config()
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
    """返回 requests 库可直接使用的 proxies 字典；若禁用代理则返回 None。"""
    proxy_url = get_proxy_url(default=default)
    if not proxy_url:
        return None
    return {
        "http": proxy_url,
        "https": proxy_url,
    }


def resolve_session_dir(config_key: str, default_subdir: str) -> str:
    """解析浏览器持久化会话目录（支持 Windows 原有目录平滑兼容 + Linux/跨平台自动回退）。"""
    raw_val = get_config(config_key, default=None)
    session_root_cfg = get_config("browser_session_root", default=None)
    if session_root_cfg:
        base_root = Path(resolve_cross_platform_path(session_root_cfg, DEFAULT_SESSION_ROOT))
    else:
        base_root = DEFAULT_SESSION_ROOT
    fallback = base_root / default_subdir
    return resolve_cross_platform_path(raw_val, fallback)


def get_media_save_dir() -> str:
    """获取媒体文件下载保存目录，默认存放在 ai_content_pipeline/media_downloads。"""
    raw_val = get_config("media_save_dir", default=None)
    return resolve_cross_platform_path(raw_val, DEFAULT_MEDIA_DIR)


# ==============================================================================
# 图片视觉内容唯一标识 (128-bit 双向感知差分哈希 dHash) 与媒体下载
# ==============================================================================
def compute_image_content_id(image_input: Any) -> Optional[str]:
    """
    根据图片视觉像素内容生成纯粹的唯一标识（128-bit 双向感知差分哈希，32位十六进制字符串）。
    无论图片 URL 如何变化、或者经过平台 CDN 重压缩/重编码，只要画面内容一致，返回的标识完全相同。
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
    if proxy in (None, "http://127.0.0.1:7890"):
        proxy = get_proxy_url(default=proxy or "http://127.0.0.1:7890")

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

        content_id = compute_image_content_id(raw_bytes)
        if not content_id:
            content_id = hashlib.sha256(raw_bytes).hexdigest()[:32]

        save_dir_path = Path(save_dir)
        save_dir_path.mkdir(parents=True, exist_ok=True)

        for ext in (suffix, ".jpg", ".jpeg", ".png", ".webp"):
            candidate = save_dir_path / f"{content_id}{ext}"
            if candidate.exists() and candidate.stat().st_size > 0:
                return str(candidate.resolve())

        save_path = save_dir_path / f"{content_id}{suffix}"
        async with aiofiles.open(save_path, "wb") as f:
            await f.write(raw_bytes)

        return str(save_path.resolve())

    except Exception as e:
        logger.debug(f"[MediaDownload] 下载失败 {url}: {e}")
        return None


def download_web_media(
    url: str,
    save_dir: Optional[str] = None,
    proxy: Optional[str] = None,
    proxies: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    """
    同步入口：下载网络媒体文件并按纯内容唯一标识命名，返回本地绝对路径。
    同时兼容 proxy (str) 与 proxies (dict) 两种代理传参方式。
    """
    effective_proxy = proxy
    if not effective_proxy and isinstance(proxies, dict):
        effective_proxy = proxies.get("https") or proxies.get("http")
    return asyncio.run(_download_media_async(url, save_dir, proxy=effective_proxy))


# ==============================================================================
# 跨平台统一概念：平台标识、自营矩阵账号白/黑名单、通用帖子 Schema 与清洗原子工具
# ==============================================================================
PLATFORM_BINANCE = "binance"
PLATFORM_OKX = "okx"
PLATFORM_X = "x"
PLATFORM_REDDIT = "reddit"
PLATFORM_POLYMARKET = "polymarket"
PLATFORM_HACKERNEWS = "hackernews"
POST_SOURCES = (PLATFORM_BINANCE, PLATFORM_OKX)
ALL_POST_SOURCES = (
    PLATFORM_BINANCE,
    PLATFORM_OKX,
    PLATFORM_X,
    PLATFORM_REDDIT,
    PLATFORM_POLYMARKET,
    PLATFORM_HACKERNEWS,
)

BINANCE_ACCOUNTS = ["qiqi", "ruru"]
OKX_ACCOUNTS = ["myself", "mama", "nana", "ruru"]
PLATFORM_ACCOUNTS = {
    PLATFORM_BINANCE: BINANCE_ACCOUNTS,
    PLATFORM_OKX: OKX_ACCOUNTS,
}

OKX_ACCOUNT_UID_MAP = {
    "myself": "872834593500823552",
    "mama": "872896855657885696",
    "nana": "1035697745975390208",
    "ruru": "950093749101883392",
}

BINANCE_ACCOUNT_USERNAME_MAP = {
    "qiqi": "insights_anchor",
    "ruru": "ruivision",
}

SELF_BINANCE_USERNAMES = {
    "sleepwell_zzz",
    "square-creator-7174ab4dc04d4",
    "square-creator-7ca94046afc63",
    "ruivision",
    "insights_anchor",
    "square-creator-70197d46eb4f8",
    "easyeverything",
    "square-creator-9912ff4d73e93",
    "square-creator-ff5e108f93e48",
    "square-creator-c733fa800b1e1",
}

SELF_BINANCE_AUTHOR_NAMES = {
    "稳定短线",
    "马到功成6",
    "来去之间Zzz",
    "瑞见未来",
}

SELF_OKX_AUTHOR_IDS = set(OKX_ACCOUNT_UID_MAP.values())
SELF_OKX_AUTHOR_NAMES = {
    "稳定短线",
    "马到功成6",
    "来去之间Zzz",
    "瑞见未来",
}

SELF_AUTHOR_IDS = {str(x).lower() for x in (SELF_OKX_AUTHOR_IDS | SELF_BINANCE_USERNAMES)}
SELF_AUTHOR_NAMES = SELF_OKX_AUTHOR_NAMES | SELF_BINANCE_AUTHOR_NAMES

_INVISIBLE_CHARS_RE = re.compile(r"[\u200b-\u200f\ufeff\u202a-\u202e]")
_MULTI_NEWLINE_NORM_RE = re.compile(r"\n{3,}")


def is_self_binance_post(raw_item: dict) -> bool:
    """判断币安原始帖子或清洗后帖子是否属于本方自营矩阵账号"""
    if not isinstance(raw_item, dict):
        return False
    author_obj = raw_item.get("author") or {}
    username = str(
        raw_item.get("username")
        or author_obj.get("username")
        or author_obj.get("author_id")
        or ""
    ).strip().lower()
    author_name = str(
        raw_item.get("authorName")
        or raw_item.get("displayName")
        or author_obj.get("author_name")
        or author_obj.get("name")
        or author_obj.get("displayName")
        or ""
    ).strip()
    return (username in SELF_BINANCE_USERNAMES) or (author_name in SELF_BINANCE_AUTHOR_NAMES)


def is_self_okx_post(content_data: dict) -> bool:
    """判断 OKX 原始 contentData 或清洗后帖子是否属于本方自营矩阵账号"""
    if not isinstance(content_data, dict):
        return False
    author_obj = content_data.get("author") or {}
    author_id = str(
        author_obj.get("authorId")
        or author_obj.get("userId")
        or author_obj.get("uid")
        or author_obj.get("author_id")
        or content_data.get("authorId")
        or content_data.get("userId")
        or content_data.get("uid")
        or content_data.get("author_id")
        or ""
    ).strip()
    nick_name = str(
        author_obj.get("nickName")
        or author_obj.get("author_name")
        or author_obj.get("name")
        or content_data.get("nickName")
        or content_data.get("author_name")
        or ""
    ).strip()
    return (author_id in SELF_OKX_AUTHOR_IDS) or (nick_name in SELF_OKX_AUTHOR_NAMES)


def create_standard_post_schema(
    source: str = PLATFORM_BINANCE,
    card_type: str = "UNKNOWN",
) -> Dict[str, Any]:
    """
    跨平台 100% 纯粹通用帖子数据骨架 (Universal Post Schema Template)：
    - 顶层与各标准子字典（author / content / media / engagement / metadata）只保留所有社区帖子共有的通用概念；
    - 任何特定平台独有的扩展属性（如 Reddit 的 subreddit、X 的 quoted_post、HN 的 external_url）
      一律收纳进 `metadata.platform_extra`，绝不污染外层通用字段结构。
    """
    return {
        "post_id": None,
        "publish_time": 0,
        "author_id": None,
        "card_type": card_type,
        "source": source,
        "metadata": {
            "url": None,
            "lang": None,
            "is_ai_generated": False,
            "platform_extra": {},
        },
        "author": {
            "username": None,
            "author_name": None,
            "is_verified": False,
            "followers_count": 0,
        },
        "content": {
            "title": None,
            "text_content": "",
            "hashtags": [],
            "mentioned_coins": [],
        },
        "media": {
            "cover_image": None,
            "images": [],
            "inline_images": [],
            "video_url": None,
            "video_duration": 0,
            "local_mapping": {},
        },
        "engagement": {
            "view_count": 0,
            "like_count": 0,
            "comment_count": 0,
            "share_count": 0,
            "quote_count": 0,
            "bookmark_count": 0,
            "viral_score": 0.0,
            "likes_per_hour": 0.0,
            "views_per_hour": 0.0,
            "delta_likes": 0,
            "delta_views": 0,
            "poll_count": 1,
        },
        "engagement_history": [],
        "comments": [],
    }


def safe_int(val: Any, default: int = 0) -> int:
    """安全转为整型，兼容字符串数字、浮点数与 None。"""
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def clean_post_text(raw: Any, unescape_html: bool = False, strip_html_comments: bool = False) -> str:
    """统一清理帖子文本中的不可见字符、可选 HTML 转义/注释与多余空行。"""
    if not raw:
        return ""
    text = str(raw)
    if unescape_html:
        import html
        text = html.unescape(text)
    if strip_html_comments:
        text = re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL)
    text = _INVISIBLE_CHARS_RE.sub("", text)
    if unescape_html or strip_html_comments:
        text = _MULTI_NEWLINE_NORM_RE.sub("\n\n", text)
    return text.strip()


def clean_post_title(
    raw: Any,
    unescape_html: bool = False,
    strip_html_comments: bool = False,
) -> Optional[str]:
    """统一清理帖子标题：空值返回 None，否则去除不可见字符与首尾空白。"""
    if not raw:
        return None
    title = clean_post_text(
        raw,
        unescape_html=unescape_html,
        strip_html_comments=strip_html_comments,
    )
    return title if title else None


def normalize_publish_timestamp(val: Any, fallback: Optional[int] = None) -> int:
    """
    将各平台异构时间格式统一转换为 10 位 UTC 秒级时间戳 (int)：
    - 13位毫秒级时间戳 (如 Binance/OKX: 1794367260000) -> 除以 1000
    - 10位秒级时间戳 (如 HN/X: 1794367260) -> 直接转为 int
    - RFC-2822 字符串 (如 X: 'Wed Nov 11 03:21:00 +0000 2026')
    - ISO-8601 字符串 (如 Reddit/Polymarket: '2026-11-11T03:21:00+00:00' 或 '...Z')
    """
    import time as _time
    from datetime import datetime as _dt
    from email.utils import parsedate_to_datetime as _parse_rfc2822

    default_ts = int(_time.time()) if fallback is None else int(fallback)
    if val is None or isinstance(val, bool):
        return default_ts

    if isinstance(val, (int, float)):
        ts = float(val)
        if ts <= 0:
            return default_ts
        return int(ts / 1000) if ts > 1e11 else int(ts)

    raw_str = str(val).strip()
    if not raw_str:
        return default_ts

    if raw_str.isdigit():
        ts = float(raw_str)
        return int(ts / 1000) if ts > 1e11 else int(ts)

    try:
        iso_str = raw_str[:-1] + "+00:00" if raw_str.endswith("Z") else raw_str
        return int(_dt.fromisoformat(iso_str).timestamp())
    except Exception:
        pass

    try:
        return int(_parse_rfc2822(raw_str).timestamp())
    except Exception:
        return default_ts


def _is_video_url(url: str) -> bool:
    """判断 URL 是否为视频流文件（.mp4 / .mov / .webm / .m3u8），采集层只下载图片不下载视频文件。"""
    if not url or not isinstance(url, str):
        return False
    path_lower = urlparse(url).path.lower()
    return path_lower.endswith((".mp4", ".mov", ".webm", ".m3u8", ".ts"))


def normalize_comment_item(
    raw_comment: Any,
    parent_post_id: str = "",
    default_ts: int = 0,
) -> Optional[Dict[str, Any]]:
    """
    将任意平台（X / Reddit / HackerNews / Binance / OKX）的单条评论统一为标准字典：
    1. 支持保留评论区图片（`images` 与本地内容哈希映射 `local_mapping`），纯图片梗图评论也不会被误丢弃；
    2. 确保下游通过 `comment['content']` 无感读取文字与 `[插图: url]` 标记。
    """
    if isinstance(raw_comment, str):
        txt = clean_post_text(raw_comment)
        if not txt:
            return None
        return {
            "reply_id": "",
            "parent_id": str(parent_post_id or ""),
            "create_time": int(default_ts or 0),
            "author_uid": "",
            "author_name": "anonymous",
            "content": txt,
            "likes": 0,
            "replies": 0,
            "views": 0,
            "images": [],
            "local_mapping": {},
        }

    if not isinstance(raw_comment, dict):
        return None

    # 提取评论附带的图片列表
    raw_imgs = (
        raw_comment.get("images")
        or raw_comment.get("image_urls")
        or raw_comment.get("media_urls")
        or []
    )
    comment_images = []
    if isinstance(raw_imgs, list):
        for img_item in raw_imgs:
            u = img_item.get("url") if isinstance(img_item, dict) else img_item
            if isinstance(u, str) and u.startswith("http") and not _is_video_url(u) and u not in comment_images:
                comment_images.append(u)

    content = clean_post_text(
        raw_comment.get("content")
        or raw_comment.get("text")
        or raw_comment.get("bodyTextOnly")
        or raw_comment.get("text_content")
        or ""
    )
    if not content and not comment_images:
        return None
    if not content and comment_images:
        content = "[图片评论]"

    for img_u in comment_images:
        if img_u not in content:
            content = f"{content}\n[插图: {img_u}]".strip()

    reply_id = str(
        raw_comment.get("reply_id")
        or raw_comment.get("comment_id")
        or raw_comment.get("id")
        or ""
    ).strip()
    author_uid = str(
        raw_comment.get("author_uid")
        or raw_comment.get("username")
        or raw_comment.get("author")
        or ""
    ).strip()
    author_name = str(
        raw_comment.get("author_name")
        or raw_comment.get("displayName")
        or raw_comment.get("username")
        or author_uid
        or "anonymous"
    ).strip()
    create_time = normalize_publish_timestamp(
        raw_comment.get("create_time") or raw_comment.get("publish_time"),
        fallback=default_ts,
    )

    local_map = dict(raw_comment.get("local_mapping") or {})

    return {
        "reply_id": reply_id,
        "parent_id": str(raw_comment.get("parent_id") or parent_post_id or ""),
        "create_time": create_time,
        "author_uid": author_uid,
        "author_name": author_name,
        "content": content,
        "likes": safe_int(raw_comment.get("likes") or raw_comment.get("like_count"), 0),
        "replies": safe_int(raw_comment.get("replies") or raw_comment.get("reply_count"), 0),
        "views": safe_int(raw_comment.get("views") or raw_comment.get("view_count"), 0),
        "images": comment_images,
        "local_mapping": local_map,
    }


def compute_cross_platform_engagement(
    source: str,
    raw_engagement: Dict[str, Any],
    publish_time: int = 0,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    跨平台互动量纲对齐与初始蹿升速率计算器：
    1. 为无原生曝光量字段的平台（Reddit / HackerNews）折算等效 `view_count`；
    2. 计算跨平台统一的 `viral_score` (0~100)；
    3. 计算自发布以来的平均每小时涨赞/涨曝光速率 (`likes_per_hour`, `views_per_hour`)。
    """
    import math as _math
    import time as _time

    eng = dict(raw_engagement or {})
    meta = metadata or {}

    views = safe_int(eng.get("view_count"), 0)
    likes = safe_int(eng.get("like_count"), 0)
    comments = safe_int(eng.get("comment_count"), 0)
    shares = safe_int(eng.get("share_count"), 0)
    quotes = safe_int(eng.get("quote_count"), 0)
    bookmarks = safe_int(eng.get("bookmark_count"), 0)

    src = str(source or "").lower().strip()
    if src == PLATFORM_REDDIT:
        if views <= 0:
            views = max(likes * 80 + comments * 200, 15000)
        if likes <= 0 and comments > 0:
            likes = comments * 8
    elif src == PLATFORM_HACKERNEWS:
        if views <= 0:
            views = max(likes * 120 + comments * 150, 10000)
    elif src == PLATFORM_POLYMARKET:
        extra = (meta.get("platform_extra") or {}) if isinstance(meta, dict) else {}
        vol_24h = float(meta.get("volume_24h_usd") or extra.get("volume_24h_usd") or views or 0.0)
        if views <= 0:
            views = max(int(vol_24h), 10000)
        if likes <= 0 and vol_24h > 0:
            likes = max(int(vol_24h / 200), 20)
    else:
        if views <= 0 and (likes > 0 or comments > 0):
            views = likes * 50 + comments * 120 + shares * 150

    weighted_heat = views + likes * 25 + comments * 60 + bookmarks * 80 + shares * 50 + quotes * 70
    viral_score = round(min(100.0, _math.log10(max(weighted_heat, 1)) * 14.5), 2)

    now_ts = int(_time.time())
    age_hours = max((now_ts - publish_time) / 3600.0, 0.1) if publish_time > 0 else 1.0
    likes_per_hour = round(likes / age_hours, 2)
    views_per_hour = round(views / age_hours, 2)

    return {
        "view_count": views,
        "like_count": likes,
        "comment_count": comments,
        "share_count": shares,
        "quote_count": quotes,
        "bookmark_count": bookmarks,
        "viral_score": viral_score,
        "likes_per_hour": safe_int(eng.get("likes_per_hour"), 0) or likes_per_hour,
        "views_per_hour": safe_int(eng.get("views_per_hour"), 0) or views_per_hour,
        "delta_likes": safe_int(eng.get("delta_likes"), 0),
        "delta_views": safe_int(eng.get("delta_views"), 0),
        "poll_count": max(safe_int(eng.get("poll_count"), 1), 1),
    }


def normalize_and_enrich_post_schema(
    post: Dict[str, Any],
    download_media: bool = False,
    media_save_dir: Optional[str] = None,
    proxies: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    跨平台单条帖子终极归一化与媒体富化入口：
    1. 严格通用字段白名单：顶层及 `author`/`content`/`media`/`engagement`/`metadata` 只保留通用字段，
       任何平台独有字段（如 `subreddit`, `quoted_post`, `external_url`）自动归入 `metadata.platform_extra`。
    2. 时间对齐：强制将 `publish_time` 归一化为 10 位 UTC 秒级整型时间戳。
    3. 只下载图片（基于视觉内容哈希 dHash 命名），过滤掉 `.mp4` 视频下载以节省带宽与磁盘。
    4. 评论按最高赞（最热）排序，并保留评论区图片 (`comment.images` 与 `comment.local_mapping`)。
    """
    src = str(post.get("source") or PLATFORM_BINANCE).strip().lower()
    card_type = str(post.get("card_type") or "BUZZ_SHORT").strip()
    base = create_standard_post_schema(source=src, card_type=card_type)

    raw_pid = post.get("post_id")
    base["post_id"] = str(raw_pid).strip() if raw_pid is not None else ""
    base["author_id"] = str(post.get("author_id")).strip() if post.get("author_id") is not None else None

    raw_ts = post.get("publish_time") or post.get("publish_time_str")
    pub_ts = normalize_publish_timestamp(raw_ts)
    base["publish_time"] = pub_ts

    # 1. Author 严格通用字段提取
    raw_author = post.get("author") if isinstance(post.get("author"), dict) else {}
    base["author"] = {
        "username": raw_author.get("username") or None,
        "author_name": raw_author.get("author_name") or raw_author.get("username") or None,
        "is_verified": bool(raw_author.get("is_verified", False)),
        "followers_count": safe_int(raw_author.get("followers_count"), 0),
    }

    # 2. Metadata 严格通用字段提取 + 平台独有字段收敛至 `platform_extra`
    raw_meta = dict(post.get("metadata") or {}) if isinstance(post.get("metadata"), dict) else {}
    platform_extra = dict(raw_meta.pop("platform_extra", None) or {})
    # 剔除临时传递用的 top_comments，其余非通用字段全部放入 platform_extra
    meta_comments = list(raw_meta.pop("top_comments", None) or [])
    universal_meta_keys = {"url", "lang", "is_ai_generated"}
    for mk, mv in list(raw_meta.items()):
        if mk not in universal_meta_keys and mv is not None:
            platform_extra[mk] = mv

    base["metadata"] = {
        "url": raw_meta.get("url") or None,
        "lang": raw_meta.get("lang") or None,
        "is_ai_generated": bool(raw_meta.get("is_ai_generated", False)),
        "platform_extra": platform_extra,
    }

    # 3. 统一评论列表：合并、保留评论区图片，并按点赞数 (`likes`) 从高到低（最热优先）排序
    raw_comments = list(post.get("comments") or [])
    normalized_comments = []
    seen_comment_keys = set()
    for c in raw_comments + meta_comments:
        nc = normalize_comment_item(c, parent_post_id=base["post_id"], default_ts=pub_ts)
        if not nc:
            continue
        dedup_k = nc["reply_id"] or nc["content"][:80]
        if dedup_k in seen_comment_keys:
            continue
        seen_comment_keys.add(dedup_k)
        normalized_comments.append(nc)
    normalized_comments.sort(key=lambda x: safe_int(x.get("likes"), 0), reverse=True)
    base["comments"] = normalized_comments

    # 4. Media 与 Content：只收集图片（排除 .mp4 视频下载），并在正文补齐 `[插图: url]`
    raw_media = post.get("media") if isinstance(post.get("media"), dict) else {}
    all_images = []
    for img_u in (raw_media.get("images") or []) + (raw_media.get("inline_images") or []):
        if isinstance(img_u, str) and img_u.startswith("http") and not _is_video_url(img_u) and img_u not in all_images:
            all_images.append(img_u)
    cover_u = raw_media.get("cover_image")
    if isinstance(cover_u, str) and cover_u.startswith("http") and not _is_video_url(cover_u):
        if cover_u not in all_images and not raw_media.get("video_url"):
            all_images.insert(0, cover_u)
    else:
        cover_u = all_images[0] if all_images else None

    # 过滤掉 local_mapping 中可能混入的视频文件，只保留纯图片本地路径
    raw_local_map = raw_media.get("local_mapping") if isinstance(raw_media.get("local_mapping"), dict) else {}
    clean_local_map = {
        u: lp for u, lp in raw_local_map.items()
        if isinstance(u, str) and not _is_video_url(u) and isinstance(lp, str) and not lp.lower().endswith((".mp4", ".mov", ".webm"))
    }

    if download_media:
        for img_url in all_images:
            existing_local = clean_local_map.get(img_url)
            if existing_local and os.path.exists(existing_local):
                continue
            local_path = download_web_media(img_url, save_dir=media_save_dir, proxies=proxies)
            if local_path:
                clean_local_map[img_url] = local_path

        for c in base["comments"]:
            c_map = dict(c.get("local_mapping") or {})
            for c_img in c.get("images") or []:
                if c_map.get(c_img) and os.path.exists(c_map[c_img]):
                    continue
                lp = clean_local_map.get(c_img) or download_web_media(c_img, save_dir=media_save_dir, proxies=proxies)
                if lp:
                    c_map[c_img] = lp
            c["local_mapping"] = c_map

    base["media"] = {
        "cover_image": cover_u or (all_images[0] if all_images else None),
        "images": all_images,
        "inline_images": [u for u in (raw_media.get("inline_images") or []) if isinstance(u, str) and not _is_video_url(u)],
        "video_url": raw_media.get("video_url") or None,
        "video_duration": safe_int(raw_media.get("video_duration"), 0),
        "local_mapping": clean_local_map,
    }

    raw_content = post.get("content") if isinstance(post.get("content"), dict) else {}
    text_content = clean_post_text(raw_content.get("text_content") or "")
    title = clean_post_title(raw_content.get("title"))
    if title and title not in text_content:
        text_content = f"{title}\n{text_content}".strip() if text_content else title

    for img_url in all_images:
        if img_url not in text_content:
            text_content = f"{text_content}\n\n[插图: {img_url}]".strip()

    base["content"] = {
        "title": title,
        "text_content": text_content,
        "hashtags": list(raw_content.get("hashtags") or []),
        "mentioned_coins": list(raw_content.get("mentioned_coins") or []),
    }

    # 5. Engagement 与历史序列初始化
    raw_eng = dict(post.get("engagement") or {}) if isinstance(post.get("engagement"), dict) else {}
    if safe_int(raw_eng.get("comment_count"), 0) < len(normalized_comments):
        raw_eng["comment_count"] = len(normalized_comments)
    base["engagement"] = compute_cross_platform_engagement(
        source=src,
        raw_engagement=raw_eng,
        publish_time=pub_ts,
        metadata=base["metadata"],
    )
    base["engagement_history"] = list(post.get("engagement_history") or [])

    return base


def normalize_and_enrich_posts_batch(
    posts: list,
    download_media: bool = True,
    max_workers: int = 12,
    media_save_dir: Optional[str] = None,
    proxies: Optional[Dict[str, str]] = None,
) -> list:
    """
    批量并发将多平台异构帖子归一化，并将「主帖配图 + 评论区配图」统一按视觉内容哈希 (dHash)
    并发下载至本地 `media_downloads/`，输出可直接写入 `HarvestedPostManager`。
    """
    from concurrent.futures import ThreadPoolExecutor

    if not posts:
        return []

    # 1. 先做纯内存结构归一化与 `[插图: url]` 注入
    normalized_list = []
    seen_keys = set()
    for p in posts:
        if not isinstance(p, dict):
            continue
        item = normalize_and_enrich_post_schema(p, download_media=False, media_save_dir=media_save_dir, proxies=proxies)
        if not item.get("post_id") or not item.get("source"):
            continue
        ukey = (item["source"], item["post_id"])
        if ukey in seen_keys:
            continue
        seen_keys.add(ukey)
        normalized_list.append(item)

    if not download_media:
        return normalized_list

    # 2. 收集全部待下载的图片 URL（涵盖主帖图片 + 评论区图片，严格排除视频文件）
    urls_to_download = []
    seen_urls = set()
    for item in normalized_list:
        lmap = item["media"].get("local_mapping") or {}
        for u in item["media"].get("images") or []:
            if u and u.startswith("http") and not _is_video_url(u) and u not in seen_urls:
                if not (lmap.get(u) and os.path.exists(lmap[u])):
                    seen_urls.add(u)
                    urls_to_download.append(u)
        for c in item.get("comments") or []:
            cmap = c.get("local_mapping") or {}
            for cu in c.get("images") or []:
                if cu and cu.startswith("http") and not _is_video_url(cu) and cu not in seen_urls:
                    if not (cmap.get(cu) and os.path.exists(cmap[cu])):
                        seen_urls.add(cu)
                        urls_to_download.append(cu)

    if urls_to_download:
        url_to_local = {}
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            future_map = {
                pool.submit(download_web_media, u, media_save_dir, None, proxies): u
                for u in urls_to_download
            }
            for fut, u in future_map.items():
                try:
                    lp = fut.result()
                    if lp:
                        url_to_local[u] = lp
                except Exception:
                    pass

        for item in normalized_list:
            lmap = dict(item["media"].get("local_mapping") or {})
            for u in item["media"].get("images") or []:
                if u in url_to_local:
                    lmap[u] = url_to_local[u]
            item["media"]["local_mapping"] = lmap

            for c in item.get("comments") or []:
                cmap = dict(c.get("local_mapping") or {})
                for cu in c.get("images") or []:
                    if cu in url_to_local:
                        cmap[cu] = url_to_local[cu]
                c["local_mapping"] = cmap

    return normalized_list




