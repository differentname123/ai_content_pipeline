# -*- coding: utf-8 -*-
"""
热点原帖采集、媒体校验与流水线公共常量模块 (`parser/post_ingestion.py`)

统一提供：
1. `parser/` 各下游环节共享的核心常量、提示词文件名、正则与发布规则表；
2. 原帖查询 (`_find_posts_by_sources`)、时间戳归一化 (`_publish_time_seconds`)、
   图片完整性校验 (`_is_valid_image_file`)、自营矩阵防回流判定 (`_is_self_post`)、
   媒体占位符归一化 (`normalize_post_media`, `build_source_image_mapping`)；
3. Binance / OKX 热榜与推荐流独立采集守护循环 (`fetch_all_hot_posts_once`, `fetch_all_hot_posts`)。
"""
import math
import os
import re
import stat
import threading
import time
from functools import lru_cache
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from PIL import Image

from common.common_utils import (
    PLATFORM_BINANCE,
    PLATFORM_OKX,
    POST_SOURCES,
    SELF_AUTHOR_IDS,
    SELF_AUTHOR_NAMES,
    download_web_media,
    is_self_post as _common_is_self_post,
)
from common.daemon import run_daemon_loop
from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import UniversalPostManager
from parser.config import (
    ALLOWED_IMAGE_FORMATS,
    ANALYSIS_EDITOR_PROMPT_FILE_PATH,
    ANALYSIS_EDITOR_PROMPT_NAME,
    ARTICLE_CONCURRENCY,
    ARTICLE_GENERATION_INTERVAL_SECONDS,
    ARTICLE_MATERIAL_LIMIT,
    ARTICLE_MAX_CHARS,
    ARTICLE_MIN_MATERIALS,
    ARTICLE_POST_USAGE_LIMIT,
    ARTICLE_PROMPT_FILE_PATH,
    ARTICLE_PROMPT_NAME,
    ARTICLE_TYPE_ANALYSIS,
    ARTICLE_TYPE_VIRAL,
    ARTICLE_VARIANTS,
    BINANCE_SOURCE,
    DELETE_INTERVAL_SEC,
    IMAGE_DEDUP_LOOKBACK_HOURS,
    INTERACTION_INTERVAL_SECONDS,
    LLM_MAX_RETRIES,
    MAX_CONCURRENCY,
    MAX_MEDIA_COUNT,
    MAX_REPLAY_DAYS,
    MEDIA_PATTERN,
    MLU_EXTRACT_PROMPT_NAME,
    MLU_FORMAT_MAX_PER_ROUND,
    OKX_SOURCE,
    POST_MAX_AGE_HOURS,
    POST_QUERY_LIMIT,
    PRE_SCREEN_BATCH_SIZE,
    PRE_SCREEN_CONCURRENCY,
    PRE_SCREEN_MAX_PER_ROUND,
    PRE_SCREEN_PROMPT_FILE_PATH,
    PRE_SCREEN_PROMPT_NAME,
    PROMPT_FILE_PATH,
    PUBLISH_MAX_ATTEMPTS,
    PUBLISH_POLL_SECONDS,
    PUBLISH_RULES,
    SELECTION_PROMPT_VERSION,
    SHELF_LIFE_SECONDS,
    VIRAL_EDITOR_PROMPT_FILE_PATH,
    VIRAL_EDITOR_PROMPT_NAME,
    VIRAL_MIN_VIEW_COUNTS,
    VIRAL_MIN_VIEW_COUNT_BINANCE,
    VIRAL_MIN_VIEW_COUNT_OKX,
    VIRAL_POST_MAX_AGE_HOURS,
    VIRAL_PUBLISH_MAX_AGE_HOURS,
    VIRAL_STAGE1_BATCH_SIZE,
    VIRAL_STAGE1_PROMPT_FILE_PATH,
    VIRAL_STAGE1_PROMPT_NAME,
    VIRAL_STAGE2_PROMPT_FILE_PATH,
    VIRAL_STAGE2_PROMPT_NAME,
    prompt_version,
)
from parser.platform_adapters import (
    BEIJING_TZ,
    COIN_TAG_PATTERN,
    PLATFORMS,
    _log,
    logger,
)


# ==============================================================================
# 公共基础工具：查询 / 时间归一化 / 契约检查 / 图片有效性 / 自家帖拦截 / 占位符整理
# ==============================================================================
def _find_posts_by_sources(
    post_manager: Any,
    sources: Iterable[str] = POST_SOURCES,
    limit: int = POST_QUERY_LIMIT,
    extra_query: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """统一按多个 source 查询原帖列表（覆盖 Binance 与 OKX）。"""
    posts: List[Dict[str, Any]] = []
    for source in sources:
        posts.extend(post_manager.find_posts_by_source(source, limit=limit, extra_query=extra_query) or [])
    return posts


def _publish_time_seconds(value: Any) -> float:
    """秒或毫秒时间戳 -> 秒；拒绝非有限数值。"""
    try:
        timestamp = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("发布时间不是有效的秒/毫秒时间戳") from exc
    if not math.isfinite(timestamp):
        raise ValueError("发布时间不能是 NaN 或无穷大")
    return timestamp / 1000 if timestamp > 1e11 else timestamp


def _key_error(data: Any, expected_keys: set, label: str) -> str:
    """校验字典是否严格匹配精确字段集合，返回可读错误描述或空串。"""
    if not isinstance(data, dict):
        return f"{label} 必须是字典"
    missing, extra = expected_keys - data.keys(), data.keys() - expected_keys
    if missing:
        return f"{label} 缺少字段: {', '.join(sorted(missing))}"
    if extra:
        return f"{label} 包含未定义字段: {', '.join(sorted(map(str, extra)))}"
    return ""


@lru_cache(maxsize=32768)
def _verify_image_integrity(abs_path: str, mtime_ns: int, file_size: int) -> bool:
    """校验图片容器头与解码像素有效性（带文件状态缓存键）。"""
    try:
        with Image.open(abs_path) as image:
            if (image.format or "").upper() not in ALLOWED_IMAGE_FORMATS:
                return False
            if min(image.size) <= 0:
                return False
            image.verify()
        with Image.open(abs_path) as image:
            image.load()
        return True
    except Exception:
        return False


def _is_valid_image_file(path: Any) -> bool:
    """校验本地路径是否存在、非空且为可解码的合法静态图片。"""
    if not isinstance(path, str) or not path.strip():
        return False
    try:
        abs_path = os.path.abspath(path)
        stat_info = os.stat(abs_path)
    except OSError:
        return False
    if not stat.S_ISREG(stat_info.st_mode) or stat_info.st_size <= 0:
        return False
    return _verify_image_integrity(abs_path, stat_info.st_mtime_ns, stat_info.st_size)


def _is_self_post(post: Any, self_post_ids: Optional[set] = None) -> bool:
    """判断原帖是否来自自家矩阵账号或已发帖 ID（防止自产自销死循环）。"""
    if not isinstance(post, dict):
        return False
    post_id = str(post.get("post_id", "") or "").strip()
    if self_post_ids and post_id in self_post_ids:
        return True
    if _common_is_self_post(post):
        return True
    author = post.get("author") or {}
    author_id = str(
        post.get("author_id")
        or author.get("author_id")
        or author.get("id")
        or author.get("uid")
        or author.get("username")
        or ""
    ).strip()
    if author_id and author_id.lower() in SELF_AUTHOR_IDS:
        return True
    author_name = str(
        post.get("author_name")
        or author.get("name")
        or author.get("nickName")
        or author.get("displayName")
        or ""
    ).strip()
    return bool(author_name and author_name in SELF_AUTHOR_NAMES)


_FAILED_MEDIA_LOCK = threading.Lock()
_FAILED_MEDIA_UNTIL: Dict[str, float] = {}
_FAILED_MEDIA_TTL_SECONDS = 1800.0
_FAILED_MEDIA_MAX_ENTRIES = 4096


def _is_valid_raw_post(post: Dict[str, Any], *, require_local_media: bool = True) -> bool:
    """校验原帖基础结构、48h 时效、非视频、媒体数量及（可选）本地图片完整性约束。"""
    if _is_self_post(post):
        return False
    post_id = post.get("post_id")
    if post_id is None or not str(post_id).strip():
        return False
    media = post.get("media") or {}
    video_duration = media.get("video_duration")
    if video_duration and video_duration > 0:
        return False
    text = (post.get("content") or {}).get("text_content") or ""
    if not text.strip():
        return False
    media_matches = list(MEDIA_PATTERN.finditer(text))
    if any(match.group(1) == "视频" for match in media_matches):
        return False
    unique_urls = {match.group(2) for match in media_matches}
    if len(unique_urls) >= MAX_MEDIA_COUNT or len(media.get("images") or []) >= MAX_MEDIA_COUNT:
        return False
    publish_time = _publish_time_seconds(post.get("publish_time", 0))
    if (time.time() - publish_time) / 3600 > POST_MAX_AGE_HOURS:
        return False
    if not require_local_media:
        return True
    mapping = media.get("local_mapping") or {}
    paths = list(mapping.values())
    if len(paths) >= MAX_MEDIA_COUNT or any(not path for path in paths):
        return False
    if any(not mapping.get(match.group(2)) for match in media_matches):
        return False
    return all(_is_valid_image_file(path) for path in paths)


def ensure_post_media_downloaded(post: Dict[str, Any]) -> bool:
    """按需下载并校验原帖缺失的配图至 `post['media']['local_mapping']`（复用磁盘 URL 缓存）。"""
    if not _is_valid_raw_post(post, require_local_media=False):
        return False
    text = (post.get("content") or {}).get("text_content") or ""
    seen_urls = set()
    urls: List[str] = []
    for match in MEDIA_PATTERN.finditer(text):
        if match.group(1) == "视频":
            continue
        url = match.group(2)
        if url and url not in seen_urls:
            seen_urls.add(url)
            urls.append(url)
    if not urls:
        return _is_valid_raw_post(post, require_local_media=True)

    media = post.setdefault("media", {})
    if not isinstance(media, dict):
        return False
    mapping = media.setdefault("local_mapping", {})
    if not isinstance(mapping, dict):
        return False

    for url in urls:
        existing = mapping.get(url)
        if existing and _is_valid_image_file(existing):
            continue
        now = time.monotonic()
        with _FAILED_MEDIA_LOCK:
            failed_until = _FAILED_MEDIA_UNTIL.get(url, 0.0)
        if failed_until > now:
            return False

        local_path = download_web_media(url=url)
        if local_path and _is_valid_image_file(local_path):
            mapping[url] = local_path
            continue

        with _FAILED_MEDIA_LOCK:
            if len(_FAILED_MEDIA_UNTIL) >= _FAILED_MEDIA_MAX_ENTRIES:
                expired = [k for k, v in _FAILED_MEDIA_UNTIL.items() if v <= now]
                for k in expired:
                    _FAILED_MEDIA_UNTIL.pop(k, None)
                if len(_FAILED_MEDIA_UNTIL) >= _FAILED_MEDIA_MAX_ENTRIES:
                    oldest_key = min(_FAILED_MEDIA_UNTIL, key=_FAILED_MEDIA_UNTIL.get)
                    _FAILED_MEDIA_UNTIL.pop(oldest_key, None)
            _FAILED_MEDIA_UNTIL[url] = now + _FAILED_MEDIA_TTL_SECONDS
        return False

    return _is_valid_raw_post(post, require_local_media=True)


def normalize_post_media(post_data: Dict[str, Any]) -> Tuple[str, List[str], Dict[str, str]]:
    """将正文中的 `[插图/封面/视频: url]` 转为顺序 `[IMAGE_n]` / `[VIDEO_n]` 占位符。"""
    text = (post_data.get("content") or {}).get("text_content") or ""
    mapping = (post_data.get("media") or {}).get("local_mapping") or {}
    paths, placeholders, counters = [], {}, {"IMAGE": 0, "VIDEO": 0}

    def replace_match(match: re.Match) -> str:
        kind = "VIDEO" if match.group(1) == "视频" else "IMAGE"
        counters[kind] += 1
        placeholder = f"[{kind}_{counters[kind]}]"
        path = mapping.get(match.group(2), "")
        paths.append(path)
        placeholders[placeholder] = path
        return placeholder

    return MEDIA_PATTERN.sub(replace_match, text), paths, placeholders


def build_source_image_mapping(post: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """提取 `[IMAGE_n]` 到原帖占位符、URL 与本地路径的溯源映射。"""
    _, _, normalized = normalize_post_media(post)
    text = (post.get("content") or {}).get("text_content") or ""
    return {
        image_id: {
            "original_placeholder": match.group(0),
            "original_url": match.group(2),
            "local_path": path or None,
        }
        for (image_id, path), match in zip(normalized.items(), MEDIA_PATTERN.finditer(text))
        if image_id.startswith("[IMAGE_")
    }


# ==============================================================================
# 热榜与综合流采集调度
# ==============================================================================
def _fetch_hot_posts(
    post_manager: Any,
    token_list: Sequence[Any],
    fetch_feed: Callable[..., List[Dict[str, Any]]],
    platform: str,
) -> None:
    started = time.monotonic()
    total_posts, token_count = 0, 0
    for token in token_list:
        token_count += 1
        for order in (1, 2):
            batch = fetch_feed(token=token, count=100, orderBy=order, download_media=False)
            if batch:
                post_manager.upsert_posts(batch)
                total_posts += len(batch)
    rec_batch = fetch_feed(count=200, download_media=False)
    if rec_batch:
        post_manager.upsert_posts(rec_batch)
        total_posts += len(rec_batch)
    _log(
        "采集/完成",
        "单平台全流采集入库完成",
        平台=platform,
        扫描币种数=token_count,
        累计入库帖数=total_posts,
        耗时=f"{time.monotonic() - started:.2f}s",
    )


def fetch_binance_hot_post(post_manager: Any, token_list: Sequence[Any]) -> None:
    _fetch_hot_posts(post_manager, token_list, PLATFORMS[PLATFORM_BINANCE].fetch_feed, PLATFORM_BINANCE)


def fetch_okx_hot_post(post_manager: Any, token_list: Sequence[Any]) -> None:
    _fetch_hot_posts(post_manager, token_list, PLATFORMS[PLATFORM_OKX].fetch_feed, PLATFORM_OKX)


def fetch_all_hot_posts_once(post_manager: Optional[Any] = None) -> None:
    """单轮执行双平台热门币种流与推荐流数据拉取（单平台异常互不阻塞）。"""
    if post_manager is None:
        post_manager = UniversalPostManager(gen_db_object())
    for platform, fetcher in (
        (PLATFORM_BINANCE, fetch_binance_hot_post),
        (PLATFORM_OKX, fetch_okx_hot_post),
    ):
        try:
            fetcher(post_manager, PLATFORMS[platform].hot_coins())
        except Exception:
            logger.exception(
                f"❌ [采集/单平台] {platform} 采集任务异常，继续下一平台 "
                "| 排查方向: [检查该平台接口、代理与网络]"
            )


def fetch_all_hot_posts(
    stop_event: Optional[threading.Event] = None,
    max_rounds: int = 0,
) -> None:
    """独立的持续拉取数据任务：分批实时入库，每轮完成后休眠 1 小时。"""

    def _step(_round_idx: int) -> Dict[str, Any]:
        started = time.monotonic()
        post_manager = UniversalPostManager(gen_db_object())
        fetch_all_hot_posts_once(post_manager)
        delay = 3600
        logger.info(
            f"[采集/轮询] 本轮双平台采集全部结束 | 总耗时: [{time.monotonic() - started:.2f}s] "
            f"| 下次采集: [{delay}s后]"
        )
        return {"next_sleep_seconds": delay}

    run_daemon_loop(
        _step,
        interval_seconds=3600,
        error_backoff_seconds=60,
        name="采集/轮询",
        logger_obj=logger,
        max_rounds=max_rounds,
        stop_event=stop_event,
    )


__all__ = [
    "ALLOWED_IMAGE_FORMATS",
    "ANALYSIS_EDITOR_PROMPT_FILE_PATH",
    "ANALYSIS_EDITOR_PROMPT_NAME",
    "ARTICLE_CONCURRENCY",
    "ARTICLE_GENERATION_INTERVAL_SECONDS",
    "ARTICLE_MATERIAL_LIMIT",
    "ARTICLE_MAX_CHARS",
    "ARTICLE_MIN_MATERIALS",
    "ARTICLE_POST_USAGE_LIMIT",
    "ARTICLE_PROMPT_FILE_PATH",
    "ARTICLE_PROMPT_NAME",
    "ARTICLE_TYPE_ANALYSIS",
    "ARTICLE_TYPE_VIRAL",
    "ARTICLE_VARIANTS",
    "BEIJING_TZ",
    "BINANCE_SOURCE",
    "COIN_TAG_PATTERN",
    "DELETE_INTERVAL_SEC",
    "IMAGE_DEDUP_LOOKBACK_HOURS",
    "INTERACTION_INTERVAL_SECONDS",
    "LLM_MAX_RETRIES",
    "MAX_CONCURRENCY",
    "MAX_MEDIA_COUNT",
    "MAX_REPLAY_DAYS",
    "MEDIA_PATTERN",
    "MLU_EXTRACT_PROMPT_NAME",
    "MLU_FORMAT_MAX_PER_ROUND",
    "OKX_SOURCE",
    "POST_MAX_AGE_HOURS",
    "POST_QUERY_LIMIT",
    "PRE_SCREEN_BATCH_SIZE",
    "PRE_SCREEN_CONCURRENCY",
    "PRE_SCREEN_MAX_PER_ROUND",
    "PRE_SCREEN_PROMPT_FILE_PATH",
    "PRE_SCREEN_PROMPT_NAME",
    "PROMPT_FILE_PATH",
    "PUBLISH_MAX_ATTEMPTS",
    "PUBLISH_POLL_SECONDS",
    "PUBLISH_RULES",
    "SELECTION_PROMPT_VERSION",
    "SHELF_LIFE_SECONDS",
    "VIRAL_EDITOR_PROMPT_FILE_PATH",
    "VIRAL_EDITOR_PROMPT_NAME",
    "VIRAL_MIN_VIEW_COUNTS",
    "VIRAL_MIN_VIEW_COUNT_BINANCE",
    "VIRAL_MIN_VIEW_COUNT_OKX",
    "VIRAL_POST_MAX_AGE_HOURS",
    "VIRAL_PUBLISH_MAX_AGE_HOURS",
    "VIRAL_STAGE1_BATCH_SIZE",
    "VIRAL_STAGE1_PROMPT_FILE_PATH",
    "VIRAL_STAGE1_PROMPT_NAME",
    "VIRAL_STAGE2_PROMPT_FILE_PATH",
    "VIRAL_STAGE2_PROMPT_NAME",
    "_find_posts_by_sources",
    "_is_self_post",
    "_is_valid_image_file",
    "_is_valid_raw_post",
    "_key_error",
    "_publish_time_seconds",
    "build_source_image_mapping",
    "ensure_post_media_downloaded",
    "fetch_all_hot_posts",
    "fetch_all_hot_posts_once",
    "fetch_binance_hot_post",
    "fetch_okx_hot_post",
    "normalize_post_media",
    "prompt_version",
]
