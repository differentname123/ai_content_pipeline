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

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(_LOG_FORMATTER)

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

    for noisy in ("httpx", "httpcore", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    with _LOGGER_LOCK:
        root_logger = logging.getLogger()
        if not root_logger.handlers:
            root_logger.setLevel(logging.INFO)
            _attach_handlers(root_logger, resolved_dir, "BinanceBot")
            root_logger.propagate = False

        if not app_name or app_name == "BinanceBot":
            return root_logger

        named_logger = logging.getLogger(app_name)
        if not named_logger.handlers:
            named_logger.setLevel(logging.INFO)
            _attach_handlers(named_logger, resolved_dir, app_name)
            named_logger.propagate = False
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
POST_SOURCES = (PLATFORM_BINANCE, PLATFORM_OKX)

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
    跨平台统一帖子数据骨架 (Schema Template)：
    确保 Binance、OKX、X 等所有平台的帖子拥有一致的字段结构，下游零感知切换。
    """
    return {
        "post_id": None,
        "publish_time": 0,
        "author_id": None,
        "card_type": card_type,
        "source": source,
        "metadata": {
            "url": None,
            "is_ai_generated": False,
        },
        "author": {
            "username": None,
            "author_name": None,
            "is_verified": False,
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
        },
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


