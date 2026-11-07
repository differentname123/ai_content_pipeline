"""
[功能摘要] 采集 Binance/OKX 原帖，生成单币分析文章或单帖重塑文章，再按平台发布、可配置互动并提供历史/指定时间段清理。
[输入数据] MongoDB 原帖 {source,post_id,publish_time,content,media,pre_screen,logic_mul,viral_screen}；
文章 {type,topic,stance,article_info,publish_records}；本地提示词、币种文件、浏览器配置及账号状态 JSON。
[数据流转/交互] 热榜/CCXT 行情 → 双平台采集入库（自动拦截自家矩阵账号防自产自销） → 每批 50 帖文本初筛 → 图片编号与多模态论据提取；
论据按币种/立场/时效/引用次数选材 → 模型生成并校验 analysis；24h 且高浏览的图文帖
→ 文本粗筛 → 同事件限额/单帖去重 → 原图验货与重塑 → 保存 viral（淘汰也保存 skip）；
分析帖近6h、流量帖近24h → 平台内基于图片内容唯一标识(dhash)去重/评分排序/独立账号冷却 → 浏览器发布
→ 分别回写账号文件和 publish_records；近24h已发帖 → 按平台 INTERACTION_CONFIG 执行可配置点赞/互评 → 回写结果；
支持指定账号与时间段（默认最近3天）批量删帖，以及按逆向行情/时间边界清理历史帖。
"""

import json
import math
import os
import random
import re
import stat
import threading
import time
from collections import Counter, defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from functools import lru_cache

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import ccxt
from PIL import Image

from app.ai_api.model_api import generate_content
from biance.biance_playwright import (
    comment_on_binance_post, create_binance_post, get_auth_tokens_robust,
)
from biance.biance_squre_api import (
    SELF_BINANCE_AUTHOR_NAMES, SELF_BINANCE_USERNAMES,
    delete_binance_square_content, fetch_binance_feed, fetch_binance_future_hot_coins,
    fetch_binance_hot_hashtags, fetch_binance_spot_hot_coins,
    fetch_binance_square_replies, get_latest_crypto_price, like_and_bookmark,
)
from biance.okx_playwright import comment_on_okx_post, create_okx_post, delete_okx_posts
from biance.okx_squre_api import (
    SELF_OKX_AUTHOR_IDS, SELF_OKX_AUTHOR_NAMES, fetch_okx_feed, fetch_okx_hot_coins,
    fetch_okx_user_timeline,
)
from common.common_utils import (
    PROMPT_DIR, get_config, get_image_content_id, get_requests_proxies,
    read_file_to_str, read_json, resolve_session_dir, save_json,
    setup_logger, string_to_object,
)
from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import GeneratedArticleManager, UniversalPostManager

logger = setup_logger(app_name="media_format")

# 一、全局配置与常量
prompt_version = "1016-1"
BINANCE_SOURCE = "binance"
OKX_SOURCE = "okx"
POST_SOURCES = (BINANCE_SOURCE, OKX_SOURCE)
PLATFORM_BINANCE = "binance"
PLATFORM_OKX = "okx"
ACCOUNTS = ["qiqi", "ruru"]
OKX_ACCOUNTS = ["myself", "mama", "nana", "ruru"]
PLATFORM_ACCOUNTS = {PLATFORM_BINANCE: ACCOUNTS, PLATFORM_OKX: OKX_ACCOUNTS}

# OKX 矩阵账号与唯一 author_id 映射（用于防自产自销与按账号删帖）
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
SELF_AUTHOR_IDS = {str(x).lower() for x in (set(SELF_OKX_AUTHOR_IDS) | set(SELF_BINANCE_USERNAMES))}
SELF_AUTHOR_NAMES = set(SELF_OKX_AUTHOR_NAMES) | set(SELF_BINANCE_AUTHOR_NAMES)

PRE_SCREEN_PROMPT_FILE_PATH = str(PROMPT_DIR / "内容生成方案_分析类MLU初筛.txt")
PROMPT_FILE_PATH = str(PROMPT_DIR / "内容生成方案_分析类MLU提取.txt")
ARTICLE_PROMPT_FILE_PATH = str(PROMPT_DIR / "内容生成方案_分析类文章生成.txt")
VIRAL_STAGE1_PROMPT_FILE_PATH = str(PROMPT_DIR / "内容生成方案_娱乐类_单帖可用性筛选.txt")
VIRAL_STAGE2_PROMPT_FILE_PATH = str(PROMPT_DIR / "内容生成方案_娱乐类_单帖洗稿和神评生成.txt")

POST_QUERY_LIMIT = 50000
POST_MAX_AGE_HOURS = 48
MAX_MEDIA_COUNT = 10
MAX_CONCURRENCY = 20
LLM_MAX_RETRIES = 3
PRE_SCREEN_BATCH_SIZE = 50
PRE_SCREEN_CONCURRENCY = 5
VIRAL_STAGE1_BATCH_SIZE = 50
VIRAL_POST_MAX_AGE_HOURS = 24
VIRAL_PUBLISH_MAX_AGE_HOURS = 24
VIRAL_MIN_VIEW_COUNT_BINANCE = 1000
VIRAL_MIN_VIEW_COUNT_OKX = 10000

ARTICLE_TYPE_ANALYSIS = "analysis"
ARTICLE_TYPE_VIRAL = "viral"
ARTICLE_MATERIAL_LIMIT = 10
ARTICLE_MIN_MATERIALS = 5
ARTICLE_POST_USAGE_LIMIT = 2
ARTICLE_MAX_CHARS = 260
ARTICLE_VARIANTS = 2
ARTICLE_CONCURRENCY = 5
ARTICLE_GENERATION_INTERVAL_SECONDS = 600

# 流水线各模块独立启停控制面板
TASK_SWITCHES = {
    "fetch_and_screen": True,         # 采集与初筛/提取
    "single_analysis_gen": True,      # 单币分析生成
    "single_analysis_pub": True,      # 单币分析发布
    "viral_gen": True,                # 娱乐帖生成
    "viral_pub": True,                # 娱乐帖发布
    "hudong": True,                   # 评论区互动
    "delete_old_post": True,          # 历史清理
}

# 各平台互动（点赞与互评）独立灵活配置
# 当前默认取消 OKX 的互评和点赞，且默认互评账号数设为 1 个随机账号
INTERACTION_CONFIG = {
    PLATFORM_OKX: {
        "enable_like": False,
        "enable_comment": False,
        "max_comment_accounts": 1,
    },
    PLATFORM_BINANCE: {
        "enable_like": True,
        "enable_comment": True,
        "max_comment_accounts": 1,
    },
}

STATE_FILE = str(Path(__file__).resolve().with_name("account_publish_state.json"))
STATE_FILE_LOCK = threading.Lock()
ACCOUNT_COOLDOWN_SECONDS = 3600
OKX_ACCOUNT_COOLDOWN_SECONDS = 1300
ACCOUNT_TOPIC_COOLDOWN_SECONDS = 2 * 3600
PUBLISH_MAX_ATTEMPTS = 3
PUBLISH_POLL_SECONDS = 600
INTERACTION_INTERVAL_SECONDS = 1200
MAX_REPLAY_DAYS = 7
DELETE_INTERVAL_SEC = 3600
IMAGE_DEDUP_LOOKBACK_HOURS = 168  # 7天内基于图片视觉内容唯一ID严格去重
BEIJING_TZ = timezone(timedelta(hours=8))
ALLOWED_IMAGE_FORMATS = {"JPEG", "PNG", "WEBP", "GIF", "BMP", "MPO"}
MEDIA_PATTERN = re.compile(r"\[(插图|长文封面|视频封面|视频):\s*(https?://[^\]]+)\]")
COIN_TAG_PATTERN = re.compile(r"\$[A-Za-z0-9_]*[A-Za-z][A-Za-z0-9_]*")
SHELF_LIFE_SECONDS = {
    "hours": 2 * 3600, "days": 24 * 3600, "weeks": 7 * 24 * 3600,
    "long": 15 * 24 * 3600, "unknown": 24 * 3600,
}
PUBLISH_RULES = {
    ARTICLE_TYPE_ANALYSIS: {
        "action": "分析发布", "hours": 6, "text": "text",
        "images": "image_placeholders", "score": "score", "state_prefix": "",
    },
    ARTICLE_TYPE_VIRAL: {
        "action": "流量发布", "hours": VIRAL_PUBLISH_MAX_AGE_HOURS, "text": "post_text",
        "images": "selected_images", "score": "viral_score", "state_prefix": "viral_",
    },
}


# 二、平台适配：仅此处封装会话、热榜、发帖和评论的接口差异，不改外部参数契约。

# action/message、任意关键字段 → 一条聚合日志；只在最终故障边界标记红叉。
def _log(action, message, level="info", exc_info=False, **fields):
    details = " | ".join(f"{key}: [{value}]" for key, value in fields.items())
    prefix = "❌ " if level in ("error", "exception") else ""
    getattr(logger, level)(
        "%s[%s] %s%s", prefix, action, message,
        f" | {details}" if details else "", exc_info=exc_info or level == "exception",
    )


class BinancePlatform:
    # 账号名 → 原项目配置中的浏览器会话目录（支持跨平台自动回退）。
    def session_dir(self, account):
        return resolve_session_dir(f"{account}_browser_session_dir", f"biance_{account}")

    # 返回原热榜的币种字符串列表；保留统一调用参数，不额外改变Binance查询。
    def hot_coins(self, for_publish=False):
        return get_hot_coin()

    # 正文/图片路径/主题/立场 → 原发布接口的 (error,success,post_id)。
    def publish(self, session_dir, text, image_paths, topic=None, stance=None):
        return create_binance_post(
            content=text, image_path_list=image_paths, user_data_dir=session_dir,
            chart_info={"coin": topic, "bridge": "USDT", "type": "future"} if topic else None,
            sentiment=stance, debug=False, text_list=_build_publish_text_list(text),
        )

    def comment(self, session_dir, post_id, text, stance, like=False):
        return comment_on_binance_post(
            post_url=f"https://www.binance.com/zh-CN/square/post/{post_id}",
            comment=text, user_data_dir=session_dir, sentiment=stance, debug=False,
        )

    def decorate_analysis(self, text, topic):
        try:
            return add_hot_topic_to_article(text, topic)
        except Exception as exc:
            _log("发布/标签", "拼装分析帖时获取热门标签失败，使用主题标签继续发布", level="warning",
                 主题=topic, 原因=exc, 排查="检查 Binance 热榜接口与网络")
            return f"{text}\n\n#{topic}\n${topic} "


class OkxPlatform:
    # 配置缺失/读取异常自动回退跨平台会话目录，保持既有容错边界。
    def session_dir(self, account):
        return resolve_session_dir(f"okx_{account}_browser_session_dir", f"okx_{account}")

    # 返回原接口的[str或{symbol/coin}]；采集不传num，发布仍传num=25。
    def hot_coins(self, for_publish=False):
        return fetch_okx_hot_coins(num=25) if for_publish else fetch_okx_hot_coins()

    def publish(self, session_dir, text, image_paths, topic=None, stance=None):
        return create_okx_post(
            content=text, image_path_list=image_paths, user_data_dir=session_dir, debug=False,
        )

    def comment(self, session_dir, post_id, text, stance, like=False):
        url = str(post_id) if str(post_id).startswith("http") else f"https://www.okx.com/zh-hans/orbit/post/{post_id}"
        return comment_on_okx_post(
            target_post_url=url, content=text, user_data_dir=session_dir, like=like, debug=False,
        )

    def decorate_analysis(self, text, topic):
        return f"{text}\n\n#{topic}\n${topic} "


PLATFORMS = {PLATFORM_BINANCE: BinancePlatform(), PLATFORM_OKX: OkxPlatform()}


def _get_account_session_dir(platform, account):
    return PLATFORMS.get(platform, PLATFORMS[PLATFORM_BINANCE]).session_dir(account)


# 仅允许外部已支持的平台；保留 Binance 与 OKX 各自的实际参数集合。
def _execute_platform_post(platform, session_dir, text, image_paths, topic=None, stance=None):
    if platform not in PLATFORMS:
        raise ValueError(f"不支持的发布平台: {platform}")
    return PLATFORMS[platform].publish(session_dir, text, image_paths, topic, stance)


# 三、原帖筛选与模型字段契约


# 统一按多个 source 查询原帖列表，覆盖 binance 与 okx 双数据源。
def _find_posts_by_sources(post_manager, sources=POST_SOURCES, limit=POST_QUERY_LIMIT):
    posts = []
    for source in sources:
        posts.extend(post_manager.find_posts_by_source(source, limit=limit) or [])
    return posts


# 秒/毫秒数值 → 秒；拒绝无效与非有限值，避免错误时间进入业务比较。
def _publish_time_seconds(value):
    try:
        timestamp = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("发布时间不是有效的秒/毫秒时间戳") from exc
    if not math.isfinite(timestamp):
        raise ValueError("发布时间不能是 NaN 或无穷大")
    return timestamp / 1000 if timestamp > 1e11 else timestamp


# 任意值、精确字段集合 → 错误文本或空串；集中表达模型数据的字段契约。
def _key_error(data, expected_keys, label):
    if not isinstance(data, dict):
        return f"{label} 必须是字典"
    missing, extra = expected_keys - data.keys(), data.keys() - expected_keys
    if missing:
        return f"{label} 缺少字段: {', '.join(sorted(missing))}"
    if extra:
        return f"{label} 包含未定义字段: {', '.join(sorted(map(str, extra)))}"
    return ""


# prompt、校验回调、模型参数 → 校验后的dict或list；原设计重试耗尽返回{}。
def _generate_validated_info(action, prompt, validator, context, **model_kwargs):
    for attempt in range(1, LLM_MAX_RETRIES + 1):
        raw_response = ""
        try:
            result = generate_content(prompt=prompt, **model_kwargs)
            raw_response = result.get("content", "")
            data = string_to_object(raw_response)
            valid, error = validator(data)
            if not valid:
                raise ValueError(error)
            return data
        except Exception as exc:
            exhausted = attempt == LLM_MAX_RETRIES
            delay = 0 if exhausted else 2 ** attempt
            log = logger.error if exhausted else logger.warning
            log(
                "%s[%s] 模型调用或数据校验失败 | 上下文: [%s] | 尝试: [%s/%s] "
                "| 原因: [%s] | 响应摘要: [%s] | 后续: [%s] "
                "| 排查: [检查模型服务、提示词字段及媒体读取]",
                "❌ " if exhausted else "", action, context, attempt, LLM_MAX_RETRIES,
                exc, repr(raw_response)[:500],
                "返回空数据，等待下轮扫描" if exhausted else f"{delay} 秒后重试",
                exc_info=exhausted,
            )
            if exhausted:
                return {}
            time.sleep(delay)
    return {}


# (绝对路径,mtime_ns,文件大小) → bool；缓存键随文件变化失效，检查容器后再解码像素。
@lru_cache(maxsize=32768)
def _verify_image_integrity(abs_path, mtime_ns, file_size):
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
        # 原设计把所有图片解析异常判为不可用；不在逐文件筛选处刷屏或抛出。
        return False


# 本地图片路径 → bool；校验文件存在、非0字节且为未损坏的合法图片。
def _is_valid_image_file(path):
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


# 判断原帖是否来自自家矩阵账号（斩断“自产自销死循环”的第二道防线）
def _is_self_post(post, self_post_ids=None):
    if not isinstance(post, dict):
        return False
    post_id = str(post.get("post_id", "") or "").strip()
    if self_post_ids and post_id in self_post_ids:
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
    if author_name and author_name in SELF_AUTHOR_NAMES:
        return True
    return False


# post{post_id,publish_time,content.text_content,media{local_mapping,video_duration}} → bool。
# : 映射项数>=10即淘汰；视频只按正时长排除；自家矩阵帖一律拦截。
def _is_valid_raw_post(post):
    if _is_self_post(post):
        return False
    post_id = post.get("post_id")
    if post_id is None or not str(post_id).strip():
        return False
    media = post.get("media") or {}
    video_duration = media.get("video_duration")
    if video_duration and video_duration > 0:
        return False
    mapping = media.get("local_mapping") or {}
    paths = list(mapping.values())
    if len(paths) >= MAX_MEDIA_COUNT or any(not path for path in paths):
        return False
    text = (post.get("content") or {}).get("text_content") or ""
    if not text.strip() or any(not mapping.get(match.group(2)) for match in MEDIA_PATTERN.finditer(text)):
        return False
    publish_time = _publish_time_seconds(post.get("publish_time", 0))
    if (time.time() - publish_time) / 3600 > POST_MAX_AGE_HOURS:
        return False
    return all(_is_valid_image_file(path) for path in paths)


# 原帖{logic_mul,pre_screen{prompt_version,no_evidence},content,media,publish_time} → bool。
def is_need_pre_screening(post):
    if post.get("logic_mul"):
        return False
    screen = post.get("pre_screen")
    if (isinstance(screen, dict) and screen.get("prompt_version") == prompt_version
            and isinstance(screen.get("no_evidence"), bool)):
        return False
    return _is_valid_raw_post(post)


# post{logic_mul,pre_screen{prompt_version,no_evidence},content,media,publish_time} → bool；只精提初筛通过的帖子。
def is_need_formatting(post):
    screen = post.get("pre_screen")
    if (post.get("logic_mul") or not isinstance(screen, dict)
            or screen.get("prompt_version") != prompt_version or screen.get("no_evidence") is not False):
        return False
    return _is_valid_raw_post(post)


# post{viral_screen,engagement.view_count,publish_time,content,media} → bool；仅筛查24h内且浏览量>1000的纯图文帖。
def is_need_viral_screening(post):
    screen = post.get("viral_screen")
    if (isinstance(screen, dict) and screen.get("prompt_version") == prompt_version
            and isinstance(screen.get("keep"), bool)):
        return False
    engagement = post.get("engagement") or {}
    view_count = engagement.get("view_count", 0)
    VIRAL_MIN_VIEW_COUNT = 1000
    source = post.get("source")
    if source == OKX_SOURCE:
        VIRAL_MIN_VIEW_COUNT = VIRAL_MIN_VIEW_COUNT_OKX
    elif source == BINANCE_SOURCE:
        VIRAL_MIN_VIEW_COUNT = VIRAL_MIN_VIEW_COUNT_BINANCE
    if not isinstance(view_count, (int, float)) or isinstance(view_count, bool) or view_count <= VIRAL_MIN_VIEW_COUNT:
        return False
    try:
        publish_time = _publish_time_seconds(post.get("publish_time", 0))
    except ValueError:
        return False
    age_hours = (time.time() - publish_time) / 3600
    if age_hours > VIRAL_POST_MAX_AGE_HOURS or age_hours < -1:
        return False
    text = (post.get("content") or {}).get("text_content") or ""
    if any(match.group(1) == "视频" for match in MEDIA_PATTERN.finditer(text)):
        return False
    return _is_valid_raw_post(post)


# post{content.text_content,media.local_mapping} → (正文,路径列表,{占位符:路径})；重复URL保留出现顺序。
def normalize_post_media(post_data):
    text = (post_data.get("content") or {}).get("text_content") or ""
    mapping = (post_data.get("media") or {}).get("local_mapping") or {}
    paths, placeholders, counters = [], {}, {"IMAGE": 0, "VIDEO": 0}

    def replace_match(match):
        kind = "VIDEO" if match.group(1) == "视频" else "IMAGE"
        counters[kind] += 1
        placeholder = f"[{kind}_{counters[kind]}]"
        path = mapping.get(match.group(2), "")
        paths.append(path)
        placeholders[placeholder] = path
        return placeholder

    return MEDIA_PATTERN.sub(replace_match, text), paths, placeholders


# 原帖{content.text_content,media.local_mapping} → {IMAGE编号:{original_placeholder,original_url,local_path}}。
def build_source_image_mapping(post):
    _, _, normalized = normalize_post_media(post)
    text = (post.get("content") or {}).get("text_content") or ""
    return {
        image_id: {"original_placeholder": match.group(0), "original_url": match.group(2), "local_path": path or None}
        for (image_id, path), match in zip(normalized.items(), MEDIA_PATTERN.finditer(text))
        if image_id.startswith("[IMAGE_")
    }


# post.content.text_content → {post_id,text_content,images:[image1,...]}；初筛只传编号，不上传媒体。
def build_pre_screen_item(post):
    text = (post.get("content") or {}).get("text_content") or ""
    image_count = sum(match.group(1) != "视频" for match in MEDIA_PATTERN.finditer(text))
    return {
        "post_id": str(post.get("post_id", "")).strip(),
        "text_content": MEDIA_PATTERN.sub("", text).strip(),
        "images": [f"image{i}" for i in range(1, image_count + 1)],
    }


# {post_id:{no_evidence:bool,reason:str}}、预期ID序列 → (bool,错误文本)；额外帖子ID仍允许。
def check_pre_screen_info(json_data, expected_post_ids):
    if not isinstance(json_data, dict):
        return False, "初筛返回最外层必须是字典"
    expected = {str(pid) for pid in expected_post_ids}
    missing = expected - json_data.keys()
    if missing:
        return False, f"初筛返回缺少 post_id: {', '.join(sorted(missing)[:10])}"
    for pid in expected:
        item, label = json_data[pid], f"post[{pid}]"
        error = _key_error(item, {"no_evidence", "reason"}, label)
        if error:
            return False, error
        if not isinstance(item["no_evidence"], bool):
            return False, f"{label}.no_evidence 必须是布尔值"
        if not isinstance(item["reason"], str) or not item["reason"].strip():
            return False, f"{label}.reason 必须是非空字符串"
    return True, ""


# 原帖列表、提示词 → {字符串post_id:{no_evidence,reason}}；校验和重试由公共模型链路处理。
def gen_pre_screen_batch_info(batch_posts, prompt_template):
    items = [build_pre_screen_item(post) for post in batch_posts]
    ids = [item["post_id"] for item in items]
    data = _generate_validated_info(
        "初筛/批次", f"{prompt_template}\n{json.dumps(items, ensure_ascii=False)}",
        lambda response: check_pre_screen_info(response, ids),
        f"批次大小={len(items)}, 首帖ID={ids[0] if ids else 'NONE'}",
        preset_model_group="low",
    )
    return {str(key): value for key, value in data.items()}


# 原帖列表 → 每帖回写 pre_screen{no_evidence,reason,prompt_version,screen_time}；返回(通过数,淘汰数)。
def process_and_save_pre_screen_batch(batch_posts, post_manager, prompt_template):
    started = time.monotonic()
    screen_map = gen_pre_screen_batch_info(batch_posts, prompt_template)
    if not screen_map:
        return 0, 0
    updates, filtered = [], 0
    for post in batch_posts:
        result = screen_map.get(str(post.get("post_id", "")).strip())
        if not result:
            continue
        post["pre_screen"] = {
            "no_evidence": result["no_evidence"], "reason": result["reason"].strip(),
            "prompt_version": prompt_version, "screen_time": time.time(),
        }
        updates.append(post)
        filtered += result["no_evidence"]
    if updates:
        post_manager.upsert_posts(updates)
    passed = len(updates) - filtered
    logger.info(
        "[初筛/回写] 批次已落库 | 处理: [%s] | 通过: [%s] | 淘汰: [%s] | 耗时: [%.2f 秒]",
        len(updates), passed, filtered, time.monotonic() - started,
    )
    return passed, filtered


# {evidences:[{target_coin,stance,dimension,core_fact,logic_link,raw_hook,impact_weight,
# shelf_life,images:[{image_id,image_type,context,usable,unusable_reason}],impact_reason}]}
# 与原图占位符集合 → (bool,错误文本)。
# : impact_reason/context仍不校验类型，不擅自收紧原模型契约。
def check_format_info(json_data, placeholders):
    if not isinstance(json_data, dict):
        return False, "最外层必须是字典"
    if "evidences" not in json_data:
        return False, "最外层缺少 evidences"
    if not isinstance(json_data["evidences"], list):
        return False, "evidences 必须是列表"
    evidence_keys = {
        "target_coin", "stance", "dimension", "core_fact", "logic_link", "raw_hook",
        "impact_weight", "shelf_life", "images", "impact_reason",
    }
    image_keys = {"image_id", "image_type", "context", "usable", "unusable_reason"}
    enums = {
        "dimension": ("K线形态", "技术指标", "链上数据", "资金流", "消息面", "基本面", "情绪判断"),
        "stance": ("看多", "看空", "震荡"),
        "shelf_life": ("hours", "days", "weeks", "long", "unknown"),
    }
    image_types = ("盘面截图", "数据图表", "新闻截图", "社交截图", "收益截图", "梗图表情", "实拍照片", "其他")
    for index, evidence in enumerate(json_data["evidences"], 1):
        label = f"evidences[{index}]"
        error = _key_error(evidence, evidence_keys, label)
        if error:
            return False, error
        coin = evidence["target_coin"]
        if not isinstance(coin, str) or not coin.strip():
            return False, f"{label}.target_coin 必须是非空字符串"
        if coin != coin.upper():
            return False, f"{label}.target_coin 必须为大写英文字符串: {coin!r}"
        if not isinstance(evidence["core_fact"], str) or not evidence["core_fact"].strip():
            return False, f"{label}.core_fact 必须是非空字符串"
        for field in ("logic_link", "raw_hook"):
            if not isinstance(evidence[field], str):
                return False, f"{label}.{field} 必须是字符串（允许为空字符串）"
        weight = evidence["impact_weight"]
        if not isinstance(weight, int) or isinstance(weight, bool) or not 1 <= weight <= 5:
            return False, f"{label}.impact_weight 必须是 1—5 的整数，不能是布尔值"
        for field, allowed in enums.items():
            if evidence[field] not in allowed:
                return False, f"{label}.{field} 取值不合法: {evidence[field]!r}"
        if not isinstance(evidence["images"], list):
            return False, f"{label}.images 必须是列表"
        for image_index, image in enumerate(evidence["images"], 1):
            image_label = f"{label}.images[{image_index}]"
            error = _key_error(image, image_keys, image_label)
            if error:
                return False, error
            if image["image_id"] not in placeholders:
                return False, f"{image_label}.image_id 未出现在原文: {image['image_id']!r}"
            if image["image_type"] not in image_types:
                return False, f"{image_label}.image_type 不在允许枚举内"
            usable, reason = image["usable"], image["unusable_reason"]
            if not isinstance(usable, bool) or not isinstance(reason, str):
                return False, f"{image_label} 的 usable 必须是布尔值，unusable_reason 必须是字符串"
            if usable and reason.strip():
                return False, f"{image_label} 可用时 unusable_reason 必须为空"
            if not usable and not reason.strip():
                return False, f"{image_label} 不可用时必须说明 unusable_reason"
    return True, ""


# 原帖 → {evidences:[论据]}；媒体编号、上传参数和前置文件读取异常行为保持不变。
def gen_media_format_info(post):
    text, paths, mapping = normalize_post_media(post)
    prompt = f"{read_file_to_str(PROMPT_FILE_PATH)}\n{text}"
    return _generate_validated_info(
        "论据/提取", prompt, lambda response: check_format_info(response, list(mapping)),
        f"帖子ID={post.get('post_id', 'UNKNOWN_ID')}", file_paths=paths,
    )


# post{post_id,content.text_content,media.local_mapping}、管理器 → 回写logic_mul；保留单帖异常隔离。
def process_and_save_single_post(post, post_manager):
    started = time.monotonic()
    post_id = post.get("post_id", "UNKNOWN_ID") if isinstance(post, dict) else "UNKNOWN_ID"
    try:
        info = gen_media_format_info(post)
        if not info:
            return
        info["prompt_version"] = prompt_version
        post["logic_mul"] = info
        post_manager.upsert_posts([post])
        logger.info(
            "[帖子/回写] 论据已保存 | 帖子: [%s] | 论据: [%s 条] | 耗时: [%.2f 秒]",
            post_id, len(info["evidences"]), time.monotonic() - started,
        )
    except Exception:
        logger.exception(
            "❌ [帖子/处理] 当前帖子失败 | 帖子: [%s] | 后续: [继续其他任务] "
            "| 排查: [检查帖子字段、提示词、媒体路径及数据库]", post_id,
        )


# 高流量第一阶段批量粗筛输出 [{post_id, keep, event_key}]、预期ID序列 → (bool,错误文本)。
def check_viral_stage1_info(json_data, expected_post_ids):
    if not isinstance(json_data, list):
        return False, "高流量一阶段粗筛返回最外层必须是列表(JSON Array)"
    expected = {str(pid).strip() for pid in expected_post_ids}
    seen = set()
    for index, item in enumerate(json_data, 1):
        label = f"viral_stage1[{index}]"
        error = _key_error(item, {"post_id", "keep", "event_key"}, label)
        if error:
            return False, error
        pid = str(item.get("post_id", "")).strip()
        if not pid:
            return False, f"{label}.post_id 不能为空"
        if not isinstance(item["keep"], bool):
            return False, f"{label}.keep 必须是布尔值"
        if not isinstance(item["event_key"], str):
            return False, f"{label}.event_key 必须是字符串"
        seen.add(pid)
    missing = expected - seen
    if missing:
        return False, f"高流量一阶段粗筛返回缺少 post_id: {', '.join(sorted(missing)[:10])}"
    return True, ""


# 候选原帖批次、提示词模板 → {post_id: {keep: bool, event_key: str}}；仅传带[IMAGE_n]占位符的文本，不传图片文件。
def gen_viral_stage1_info(batch_posts, prompt_template):
    items = []
    for post in batch_posts:
        normalized_text, _, placeholders = normalize_post_media(post)
        items.append({
            "post_id": str(post.get("post_id", "")).strip(),
            "image_count": len(placeholders),
            "text": normalized_text.strip(),
        })
    ids = [item["post_id"] for item in items]
    data = _generate_validated_info(
        "高流量/批量粗筛",
        f"{prompt_template}\n{json.dumps(items, ensure_ascii=False)}",
        lambda response: check_viral_stage1_info(response, ids),
        f"批次大小={len(items)}, 首帖ID={ids[0] if ids else 'NONE'}",
        preset_model_group="low",
    )
    if not isinstance(data, list):
        return {}
    return {
        str(item["post_id"]).strip(): {
            "keep": item["keep"],
            "event_key": item["event_key"].strip(),
        }
        for item in data if isinstance(item, dict) and "post_id" in item
    }


# 单批高流量候选原帖 → 执行第一阶段批量纯文本粗筛并回写 viral_screen；返回 (保留数, 淘汰数)。
def process_and_save_single_viral_post(batch_posts, post_manager, prompt_template):
    started = time.monotonic()
    screen_map = gen_viral_stage1_info(batch_posts, prompt_template)
    if not screen_map:
        return 0, 0
    updates, kept_count = [], 0
    now = time.time()
    for post in batch_posts:
        pid = str(post.get("post_id", "")).strip()
        result = screen_map.get(pid)
        if not result:
            continue
        post["viral_screen"] = {
            "keep": result["keep"],
            "event_key": result["event_key"],
            "prompt_version": prompt_version,
            "screen_time": now,
        }
        updates.append(post)
        kept_count += int(result["keep"])
    if updates:
        post_manager.upsert_posts(updates)
    rejected_count = len(updates) - kept_count
    logger.info(
        "[高流量/粗筛回写] 批次已保存 | 处理: [%s] | 放行(keep=True): [%s] | 淘汰: [%s] | 耗时: [%.2f 秒]",
        len(updates), kept_count, rejected_count, time.monotonic() - started,
    )
    return kept_count, rejected_count


# posts:[原帖]、筛选/处理回调 → 并发回写筛查字段；各批次失败独立处理，返回None。
def _screen_posts(posts, post_manager, predicate, processor, prompt_path, batch_size, action):
    pending = [post for post in posts if predicate(post)]
    if not pending:
        _log(f"{action}/计划", "当前无待筛新帖", 扫描=len(posts))
        return
    prompt = read_file_to_str(prompt_path)
    if not isinstance(prompt, str) or not prompt.strip():
        raise ValueError(f"{action}提示词为空或读取失败: {prompt_path}")
    batches = [pending[index:index + batch_size] for index in range(0, len(pending), batch_size)]
    _log(f"{action}/计划", "批量文本筛查就绪", 扫描=len(posts), 待筛=len(pending),
         跳过=len(posts) - len(pending), 批次=len(batches), 单批上限=batch_size,
         并发=PRE_SCREEN_CONCURRENCY)
    started = time.monotonic()
    passed = rejected = failed = 0
    with ThreadPoolExecutor(max_workers=PRE_SCREEN_CONCURRENCY) as executor:
        futures = {executor.submit(processor, batch, post_manager, prompt): batch for batch in batches}
        for future in as_completed(futures):
            batch = futures[future]
            try:
                kept, dropped = future.result()
                passed += kept
                rejected += dropped
                failed += kept + dropped == 0
            except Exception:
                failed += 1
                _log(f"{action}/执行", "当前批次处理或回写失败，继续其他批次", level="exception",
                     首帖=batch[0].get("post_id"), 帖数=len(batch),
                     排查="检查模型、帖子字段与数据库；核对是否已部分写入")
    _log(f"{action}/汇总", "本轮筛查结束", 待筛=len(pending), 通过=passed,
         淘汰=rejected, 失败批次=failed, 耗时=f"{time.monotonic() - started:.2f}秒")


# 原帖列表 → 每批50帖初筛；成功结果原地更新，供本轮精提直接读取。
def batch_pre_screen_posts(posts, post_manager):
    _screen_posts(posts, post_manager, is_need_pre_screening, process_and_save_pre_screen_batch,
                  PRE_SCREEN_PROMPT_FILE_PATH, PRE_SCREEN_BATCH_SIZE, "初筛")


# 原帖列表 → 24h且浏览>1000的文本粗筛；保持单批失败隔离。
def batch_screen_viral_posts(posts, post_manager):
    _screen_posts(posts, post_manager, is_need_viral_screening, process_and_save_single_viral_post,
                  VIRAL_STAGE1_PROMPT_FILE_PATH, VIRAL_STAGE1_BATCH_SIZE, "高流量粗筛")


# 四、采集与论据提取调度


# top_k → {top_gainers/top_losers:[{symbol,percentage}]}；安全释放 CCXT 客户端，禁止释放异常阻断正常返回。
def get_top_k_movers(top_k=5):
    ccxt_cfg = {
        "enableRateLimit": True, "options": {"defaultType": "future"},
    }
    proxies = get_requests_proxies()
    if proxies:
        ccxt_cfg["proxies"] = proxies
    exchange = ccxt.binance(ccxt_cfg)
    try:
        tickers = exchange.fetch_tickers()
        ranked = sorted(
            ({"symbol": symbol, "percentage": ticker["percentage"]}
             for symbol, ticker in tickers.items()
             if symbol.endswith(":USDT") and ticker.get("percentage") is not None),
            key=lambda item: item["percentage"], reverse=True,
        )
        return {"top_gainers": ranked[:top_k], "top_losers": ranked[-top_k:][::-1]}
    except Exception as exc:
        _log("行情/获取", "获取涨跌榜异常，降级返回空榜单", level="warning", 原因=exc)
        return {"top_gainers": [], "top_losers": []}
    finally:
        try:
            close_fn = getattr(exchange, "close", None)
            if callable(close_fn):
                close_fn()
        except Exception:
            pass



# 合约/现货热榜与涨跌榜各前2项 → 去重币种列表；沿用集合的无序返回。
def get_hot_coin():
    top_k, coins = 2, []
    for fetch in (fetch_binance_future_hot_coins, fetch_binance_spot_hot_coins):
        coins.extend(item.get("symbol").replace("USDT", "").replace("USDC", "")
                     for item in fetch()[:top_k])
    movers = get_top_k_movers(top_k)
    coins.extend(item["symbol"].split("/")[0] for item in movers["top_gainers"] + movers["top_losers"])
    coins = list(set(coins))
    logger.info("[币种/热榜] 已合并热门币种 | 数量: [%s] | 币种: [%s]", len(coins), coins)
    return coins


# 币种序列、平台feed函数 → 按热门/最新及综合流采集，原样批量upsert，保留重复项。
def _fetch_hot_posts(post_manager, token_list, fetch_feed, platform):
    started = time.monotonic()
    posts, token_count = [], 0
    for token in token_list:
        token_count += 1
        for order in (1, 2):
            posts.extend(fetch_feed(token=token, count=100, orderBy=order))
    posts.extend(fetch_feed(count=200))
    if posts:
        post_manager.upsert_posts(posts)
    _log("采集/完成", "推荐流已入库", 平台=platform, 币种数=token_count,
         帖数=f"{len(posts)}，含重复", 耗时=f"{time.monotonic() - started:.2f}秒")


def fetch_binance_hot_post(post_manager, token_list):
    _fetch_hot_posts(post_manager, token_list, fetch_binance_feed, PLATFORM_BINANCE)


def fetch_okx_hot_post(post_manager, token_list):
    _fetch_hot_posts(post_manager, token_list, fetch_okx_feed, PLATFORM_OKX)


# 采集 → 批量初筛原地回写 → 并发精提 → 高流量第一阶段筛查；保留无帖60秒、满批5秒、普通批次1小时的轮询节奏。
def format_image_article():
    while True:
        try:
            started = time.monotonic()
            post_manager = UniversalPostManager(gen_db_object())
            fetch_binance_hot_post(post_manager, PLATFORMS[PLATFORM_BINANCE].hot_coins())
            fetch_okx_hot_post(post_manager, PLATFORMS[PLATFORM_OKX].hot_coins())
            source_batches = [
                post_manager.find_posts_by_source(source, limit=POST_QUERY_LIMIT) or []
                for source in POST_SOURCES
            ]
            posts = [post for batch in source_batches for post in batch]
            if not posts:
                logger.info("[帖子/轮询] 暂无原帖 | 下次扫描: [60 秒后]")
                time.sleep(60)
                continue
            batch_pre_screen_posts(posts, post_manager)
            valid_posts = [post for post in posts if is_need_formatting(post)]
            logger.info(
                "[帖子/计划] 初筛后精提 | 扫描: [%s] | 待精提: [%s] | 跳过: [%s] | 并发上限: [%s]",
                len(posts), len(valid_posts), len(posts) - len(valid_posts), MAX_CONCURRENCY,
            )
            if valid_posts:
                with ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as executor:
                    futures = [executor.submit(process_and_save_single_post, post, post_manager)
                               for post in valid_posts]
                    for future in as_completed(futures):
                        future.result()
            batch_screen_viral_posts(posts, post_manager)
            delay = 5 if any(len(batch) >= POST_QUERY_LIMIT for batch in source_batches) else 3600
            logger.info(
                "[帖子/完成] 本轮线程结束 | 扫描: [%s] | 已提交: [%s] | 耗时: [%.2f 秒] "
                "| 下次扫描: [%s 秒后] | 单帖结果: [见帖子回写/失败日志]",
                len(posts), len(valid_posts), time.monotonic() - started, delay,
            )
            time.sleep(delay)
        except Exception:
            logger.exception(
                "❌ [帖子/轮询] 本轮中断 | 后续: [60 秒后重新初始化] "
                "| 排查: [检查热榜、采集服务、提示词、代理、帖子字段及数据库]"
            )
            time.sleep(60)


# 无入参 → 查询范围内非None的 logic_mul/pre_screen 置空并批量回写；保留手动清理入口。
def clear_all_media_format_batch():
    manager = UniversalPostManager(gen_db_object())
    posts = _find_posts_by_sources(manager, limit=POST_QUERY_LIMIT)
    updates = [post for post in posts if post.get("logic_mul") is not None or post.get("pre_screen") is not None]
    for post in updates:
        post["logic_mul"] = post["pre_screen"] = None
    if updates:
        manager.upsert_posts(updates)
    logger.info("[数据/清理] 提取与初筛字段已清空 | 扫描: [%s] | 更新: [%s]", len(posts), len(updates))


# 五、选材、生成与落库：共享重试机制，保留两类文章各自的业务契约


# 查询数据库所有自家矩阵已成功发布的 post_id 集合，彻底防止自家帖子作为原帖回流被二次引用
def _get_all_self_published_post_ids(article_manager=None):
    if article_manager is None:
        article_manager = GeneratedArticleManager(gen_db_object())
    articles = article_manager.find_articles(
        query={
            "$or": [
                {f"publish_records.{PLATFORM_BINANCE}.post_id": {"$exists": True, "$ne": None}},
                {f"publish_records.{PLATFORM_OKX}.post_id": {"$exists": True, "$ne": None}},
            ]
        },
        limit=0,
    ) or []
    self_ids = set()
    for art in articles:
        pub = art.get("publish_records") or {}
        for plat in (PLATFORM_BINANCE, PLATFORM_OKX):
            pid = (pub.get(plat) or {}).get("post_id")
            if pid and str(pid).strip() and str(pid).strip() != "None":
                self_ids.add(str(pid).strip())
    return self_ids


# 论据[{core_fact,logic_link,raw_hook,images[{image_id,usable,context}],source_post_id,source_image_mapping}]
# → ([{id,fact,underlying_logic,raw_hook,visual_evidence}],{ASSET编号:来源})；visual为None/单图/有序图组。
def transform_mlus(mlu_list):
    materials, image_mapping, asset_by_source = [], {}, {}
    for index, evidence in enumerate(mlu_list, 1):
        coin = str(evidence.get("target_coin") or "").strip().upper()
        material = {
            "id": f"M{index}", "coin": coin, "target_coin": coin, "fact": evidence.get("core_fact", ""),
            "underlying_logic": evidence.get("logic_link", ""), "raw_hook": evidence.get("raw_hook", ""),
            "visual_evidence": None,
        }
        materials.append(material)
        images = [image for image in evidence.get("images", []) if image.get("usable") is True]
        if not images:
            continue
        source_mapping = evidence.get("source_image_mapping")
        if source_mapping is None:
            source_mapping = build_source_image_mapping({
                "content": {"text_content": evidence.get("source_text_content", "")},
                "media": {"local_mapping": evidence.get("source_local_mapping") or {}},
            })
        if not all(image.get("image_id") in source_mapping
                   and isinstance(image.get("context"), str) and image["context"].strip() for image in images):
            logger.warning(
                "[文章/配图] 当前论据保留文字、取消整组配图 | 帖子: [%s] "
                "| 原因: [至少一图缺少来源或描述] | 排查: [核对原文编号、映射与context]",
                evidence.get("source_post_id"),
            )
            continue
        visual_items, seen_ids = [], set()
        ev_source = evidence.get("source") or BINANCE_SOURCE
        for image in images:
            image_id = image["image_id"]
            if image_id in seen_ids:
                continue
            seen_ids.add(image_id)
            meta = source_mapping[image_id]
            local_path = meta.get("local_path") or ""
            content_id = get_image_content_id(local_path)
            # 基于图片内容唯一标识(dhash)优先去重，同内容图片在同篇文章中共享同一个占位符
            source_key = ("content_id", content_id) if content_id else (ev_source, evidence.get("source_post_id"), image_id)
            placeholder = asset_by_source.get(source_key)
            if placeholder is None:
                placeholder = f"[ASSET_IMG_{len(asset_by_source) + 1}]"
                asset_by_source[source_key] = placeholder
                image_mapping[placeholder] = {
                    "source": ev_source, "source_post_id": evidence.get("source_post_id"),
                    "original_image_id": image_id, "original_image_id_list": [image_id],
                    "content_id": content_id, **meta,
                }
            visual_items.append({"placeholder": placeholder, "what_it_shows": image["context"]})
        material["visual_evidence"] = visual_items[0] if len(visual_items) == 1 else visual_items
    return materials, image_mapping


# 当前版本logic_mul.evidences → {coin:{stance:[富化论据]}}；补齐source/原帖/图片溯源。
# : 任一图片不可用淘汰整条论据，无图论据可用；严格排除自家矩阵发布的帖子。
def extract_and_group_valid_evidences(article_manager=None):
    binance_hot_coins = {coin.upper() for coin in get_hot_coin()}
    okx_hot_coins = {coin.upper() for coin in PLATFORMS[PLATFORM_OKX].hot_coins()}
    hot_coins = binance_hot_coins.union(okx_hot_coins)
    started = time.monotonic()
    manager = UniversalPostManager(gen_db_object())
    posts = _find_posts_by_sources(manager, limit=POST_QUERY_LIMIT)
    self_post_ids = _get_all_self_published_post_ids(article_manager)
    grouped = defaultdict(lambda: defaultdict(list))
    now, invalid_times, self_skipped = time.time(), 0, 0
    for post in posts:
        if _is_self_post(post, self_post_ids):
            self_skipped += 1
            continue
        logic, raw_time = post.get("logic_mul"), post.get("publish_time")
        if (not isinstance(logic, dict) or logic.get("prompt_version") != prompt_version
                or not logic.get("evidences") or not raw_time):
            continue
        try:
            publish_time = _publish_time_seconds(raw_time)
        except ValueError:
            invalid_times += 1
            continue
        age = now - publish_time
        if age > 15 * 24 * 3600 or age < -3600:
            continue
        source_mapping = build_source_image_mapping(post)
        post_source = post.get("source") or BINANCE_SOURCE
        for evidence in logic["evidences"]:
            if any(image.get("usable") is not True for image in evidence.get("images", [])):
                continue
            duration = SHELF_LIFE_SECONDS.get(evidence.get("shelf_life", "unknown"), 24 * 3600)

            target_coin = str(evidence.get("target_coin") or "").strip().upper()
            if not target_coin and isinstance(evidence.get("coins"), list):
                candidate_coins = [str(coin).strip().upper() for coin in evidence["coins"] if str(coin).strip()]
            else:
                candidate_coins = [target_coin] if target_coin else []
            stance = evidence.get("stance")
            if age > duration or not candidate_coins or stance not in ("看多", "看空"):
                continue
            enriched = {
                **evidence, "source": post_source, "source_post_id": post.get("post_id", "UNKNOWN"),
                "publish_time": publish_time,
                "source_text_content": (post.get("content") or {}).get("text_content", ""),
                "source_image_mapping": source_mapping,
            }
            for coin in candidate_coins:
                if coin and coin in hot_coins:
                    grouped[coin][stance].append(enriched)
    for stances in grouped.values():
        for evidences in stances.values():
            evidences.sort(key=lambda item: item.get("impact_weight", 0), reverse=True)
    result = json.loads(json.dumps(grouped))
    log = logger.warning if invalid_times else logger.info
    log(
        "[论据/筛选] 已分组并按权重排序 | 扫描: [%s] | 币种: [%s] | 论据: [%s，含跨组重复] "
        "| 自家帖拦截: [%s] | 无效时间跳过: [%s] | 耗时: [%.2f 秒]",
        len(posts), len(result), sum(len(items) for stances in result.values() for items in stances.values()),
        self_skipped, invalid_times, time.monotonic() - started,
    )
    return result


# source与post_id序列 → {post_id:成功发布引用次数}；同文章同帖只计一次。
def get_post_usage_counts(article_manager, source, post_ids):
    unique_ids = list(dict.fromkeys(post_ids))
    counts = dict.fromkeys(unique_ids, 0)
    if not unique_ids:
        return counts
    source_query = {"$in": list(source)} if isinstance(source, (list, tuple, set)) else source
    articles = article_manager.find_articles(
        query={
            "source": source_query,
            "status": "ok",
            "post_id_list": {"$in": unique_ids},
            "$or": [
                {f"publish_records.{PLATFORM_BINANCE}.status": "success"},
                {f"publish_records.{PLATFORM_OKX}.status": "success"},
            ],
        },
        limit=0,
    )
    for article in articles:
        for post_id in set(article.get("post_id_list", [])):
            if post_id in counts:
                counts[post_id] += 1
    return counts


# source/topic/stance → 近期文章首行列表；保留生成成功即参与、查询限额先于去重的规则。
def get_recent_openings(article_manager, source, topic, stance, limit=5):
    if limit <= 0:
        return []
    articles = article_manager.find_articles(
        query={"source": source, "topic": topic, "stance": stance, "status": "ok"},
        sort=[("created_at", -1)], limit=limit,
    )
    openings = []
    for article in articles:
        text = (article.get("article_info") or {}).get("text", "")
        if isinstance(text, str) and text.strip():
            opening = text.strip().splitlines()[0][:80]
            if opening not in openings:
                openings.append(opening)
    return openings


# record{source,topic,stance,status,article_info.used_material_ids,material_post_mapping}
# → 补齐默认字段（含多平台发布记录容器 publish_records）的浅拷贝。
def prepare_article_record_for_db(article_data):
    record = dict(article_data)
    record.setdefault("type", ARTICLE_TYPE_ANALYSIS)
    for key in ("source", "topic", "stance"):
        if not isinstance(record.get(key), str) or not record[key].strip():
            raise ValueError(f"文章缺失有效字段: {key}")
    status = record.get("status")
    if status not in ("processing", "ok", "skip", "error"):
        raise ValueError(f"不支持的文章状态: {status}")
    post_ids = []
    if status == "ok":
        info, mapping = record.get("article_info"), record.get("material_post_mapping")
        if not isinstance(info, dict) or info.get("status") != "ok":
            raise ValueError("成功记录必须包含 status=ok 的 article_info")
        if not isinstance(mapping, dict):
            raise ValueError("成功记录必须包含 material_post_mapping")
        used_ids = info.get("used_material_ids")
        if not isinstance(used_ids, list) or not used_ids:
            raise ValueError("成功文章的 used_material_ids 不能为空")
        for material_id in used_ids:
            if not isinstance(material_id, str) or material_id not in mapping:
                raise ValueError(f"实际采用素材无法反查原帖: {material_id!r}")
            post_id = mapping[material_id]
            if (not isinstance(post_id, (str, int)) or isinstance(post_id, bool)
                    or post_id in (None, "", "UNKNOWN")):
                raise ValueError(f"素材对应的原帖 ID 无效: {material_id}")
            if post_id not in post_ids:
                post_ids.append(post_id)
    elif status == "skip" and record.get("type") == ARTICLE_TYPE_VIRAL:
        post_ids = [
            pid for pid in (record.get("post_id_list") or [])
            if pid not in (None, "", "UNKNOWN")
        ]
    record["post_id_list"] = post_ids
    for key, default in {
        "article_info": None, "error_message": None, "error_history": [],
        "raw_response": None, "attempt_count": 0, "publish_records": {},
    }.items():
        record.setdefault(key, default)
    return record


# article_info{status,text,god_comments,image_placeholders,used_material_ids,score,reason}、
# materials[{id,visual_evidence}]、{ASSET编号:来源} → (bool,错误文本)；整组图片必须保持证据顺序。
def check_article_info(article_info, materials, image_mapping, max_chars):
    keys = {"status", "text", "god_comments", "image_placeholders", "used_material_ids", "score", "reason"}
    error = _key_error(article_info, keys, "文章")
    if error:
        return False, error
    if article_info["status"] not in ("ok", "skip"):
        return False, "status 必须为 ok 或 skip"
    if not isinstance(article_info["text"], str):
        return False, "text 必须是字符串"
    for key in ("god_comments", "image_placeholders", "used_material_ids"):
        values = article_info[key]
        if not isinstance(values, list) or any(not isinstance(value, str) or not value for value in values):
            return False, f"{key} 必须是非空字符串组成的列表，列表本身可为空"
        if key != "god_comments" and len(values) != len(set(values)):
            return False, f"{key} 不允许重复"
    if article_info["status"] == "skip":
        if (article_info["text"] != "" or article_info["image_placeholders"] or article_info["used_material_ids"]
                or article_info["god_comments"] or article_info["score"] is not None):
            return False, "skip 必须清空正文、图片、神评、采用素材，score 必须为 null"
        if not isinstance(article_info["reason"], str) or not article_info["reason"].strip():
            return False, "skip 必须说明 reason"
        return True, ""
    text = article_info["text"]
    if not text.strip():
        return False, "ok 正文不能为空"
    if len(re.sub(r"\s", "", text)) > max_chars:
        return False, f"正文非空白字符数超过 {max_chars}"
    if re.search(r"\[(?:ASSET_IMG|IMAGE|VIDEO)_\d+\]", text):
        return False, "正文不能包含图片或视频占位符"
    if type(article_info["score"]) not in (int, float) or not 0 <= article_info["score"] <= 10:
        return False, "score 必须是 0—10 的数值，不能是布尔值"
    if article_info["reason"] is not None:
        return False, "ok 的 reason 必须为 null"
    if not article_info["god_comments"]:
        return False, "ok 状态下 god_comments 必须至少包含 1 条神评"
    material_by_id = {item["id"]: item for item in materials}
    used_ids, selected_images = article_info["used_material_ids"], article_info["image_placeholders"]
    if not used_ids or any(item not in material_by_id for item in used_ids):
        return False, "used_material_ids 必须引用实际存在的素材且不能为空"
    if len(selected_images) > 3:
        return False, "最多使用 3 张图片"
    allowed_images = set()
    for material_id in used_ids:
        visual = material_by_id[material_id]["visual_evidence"]
        if not visual:
            continue
        chain = visual if isinstance(visual, list) else [visual]
        chain_ids = [item["placeholder"] for item in chain]
        allowed_images.update(chain_ids)
        chosen = [item for item in selected_images if item in chain_ids]
        if chosen and chosen != chain_ids:
            return False, f"素材 {material_id} 的图片证据链必须整组使用并保留输入顺序"
    if any(item not in allowed_images or item not in image_mapping for item in selected_images):
        return False, "图片必须来自已采用素材，且存在真实来源映射"
    return True, ""


# coin/stance、论据[{source,source_post_id,core_fact,dimension,shelf_life,impact_weight}]
# → {is_skip,record,coin,stance,reason?}；可执行任务另含full_prompt/materials/image_mapping/model_name。
def build_article_task(coin, stance, ev_list, article_manager):
    brief = {"task": {"topic": coin, "stance": stance, "max_chars": ARTICLE_MAX_CHARS}, "materials": []}
    model_name = "gemini-3.1-pro-preview"
    record = {
        "source": BINANCE_SOURCE, "type": ARTICLE_TYPE_ANALYSIS, "topic": coin, "stance": stance,
        "status": "processing", "creation_brief": brief, "material_post_mapping": {}, "post_id_list": [],
        "prompt_file_path": ARTICLE_PROMPT_FILE_PATH, "model_name": model_name,
        "prompt_version": prompt_version, "article_info": None, "error_message": None,
        "error_history": [], "raw_response": None, "attempt_count": 0,
    }
    candidates = [item for item in ev_list
                  if isinstance(item, dict) and isinstance(item.get("source_post_id"), (str, int))
                  and not isinstance(item.get("source_post_id"), bool) and item["source_post_id"] not in ("", "UNKNOWN")
                  and isinstance(item.get("core_fact"), str) and item["core_fact"].strip()]
    usage = get_post_usage_counts(
        article_manager, POST_SOURCES, list(dict.fromkeys(item["source_post_id"] for item in candidates)),
    )
    groups = defaultdict(list)
    for item in candidates:
        if usage.get(item["source_post_id"], 0) < ARTICLE_POST_USAGE_LIMIT:
            groups[(item.get("dimension"), item.get("shelf_life"))].append(item)
    groups = {
        key: deque(sorted(items, key=lambda item: (item.get("impact_weight", 0), random.random()), reverse=True))
        for key, items in groups.items()
    }
    selected = []
    while groups and len(selected) < ARTICLE_MATERIAL_LIMIT:
        keys = sorted(groups, key=lambda key: (groups[key][0].get("impact_weight", 0), random.random()), reverse=True)
        for key in keys:
            if len(selected) >= ARTICLE_MATERIAL_LIMIT:
                break
            selected.append(groups[key].popleft())
            if not groups[key]:
                del groups[key]
    if len(selected) < ARTICLE_MIN_MATERIALS:
        reason = (f"可用论据不足 {ARTICLE_MIN_MATERIALS} 条（当前 {len(selected)} 条）；"
                  f"需检查论据有效性及原帖已发布引用次数是否小于 {ARTICLE_POST_USAGE_LIMIT}。")
        record["status"] = "skip"
        record["article_info"] = {
            "status": "skip", "text": "", "god_comments": [], "image_placeholders": [], "used_material_ids": [],
            "score": None, "reason": reason, "image_mapping": {},
        }
        return {"is_skip": True, "record": record, "coin": coin, "stance": stance, "reason": reason}
    materials, image_mapping = transform_mlus(selected)
    brief["materials"] = materials
    record["material_post_mapping"] = {
        material["id"]: evidence["source_post_id"] for material, evidence in zip(materials, selected)
    }
    prompt = read_file_to_str(ARTICLE_PROMPT_FILE_PATH)
    if not isinstance(prompt, str) or not prompt.strip():
        raise ValueError(f"文章提示词为空或读取失败: {ARTICLE_PROMPT_FILE_PATH}")
    return {
        "is_skip": False, "record": record, "full_prompt": f"{prompt}\n{json.dumps(brief, ensure_ascii=False)}",
        "materials": materials, "image_mapping": image_mapping, "coin": coin, "stance": stance,
        "model_name": model_name,
    }


# 管理器、来源、候选post_id序列 → 已执行过二阶段(含 status=ok 与 status=skip)的原帖ID字符串集合。
def get_remodeled_viral_post_ids(article_manager, source, post_ids):
    raw_ids = list(dict.fromkeys(post_ids))
    if not raw_ids:
        return set()
    str_ids = [str(pid).strip() for pid in raw_ids if str(pid).strip()]
    int_ids = [int(pid) for pid in str_ids if pid.isdigit()]
    query_ids = list(dict.fromkeys([*raw_ids, *str_ids, *int_ids]))
    source_query = {"$in": list(source)} if isinstance(source, (list, tuple, set)) else source
    articles = article_manager.find_articles(
        query={
            "source": source_query,
            "type": ARTICLE_TYPE_VIRAL,
            "status": {"$in": ["ok", "skip"]},
            "post_id_list": {"$in": query_ids},
        },
        limit=0,
    ) or []
    used = set()
    for article in articles:
        for pid in article.get("post_id_list", []):
            if pid is not None:
                used.add(str(pid).strip())
    return used


# 当前版本viral_screen.keep=True原帖 → 待重塑列表，按浏览+点赞*20+评论*50排序。
def extract_valid_viral_posts(article_manager):
    started = time.monotonic()
    post_manager = UniversalPostManager(gen_db_object())
    posts = _find_posts_by_sources(post_manager, limit=POST_QUERY_LIMIT)
    self_post_ids = _get_all_self_published_post_ids(article_manager)
    now = time.time()

    recent_viral_articles = (
        article_manager.find_articles(
            query={
                "source": {"$in": list(POST_SOURCES)},
                "type": ARTICLE_TYPE_VIRAL,
                "status": "ok",
                "created_at": {"$gte": datetime.now(timezone.utc) - timedelta(hours=48)},
            },
            limit=0,
        )
        or []
    )
    used_event_keys = Counter()
    for art in recent_viral_articles:
        ek = (
            ((art.get("creation_brief") or {}).get("stage1_screen") or {}).get("event_key") or ""
        ).strip()
        if ek:
            used_event_keys[ek] += 1

    candidates = []
    for post in posts:
        if _is_self_post(post, self_post_ids):
            continue
        screen = post.get("viral_screen")
        if not isinstance(screen, dict) or screen.get("prompt_version") != prompt_version:
            continue
        if screen.get("keep") is not True:
            continue
        engagement = post.get("engagement") or {}
        view_count = engagement.get("view_count", 0)
        like_count = engagement.get("like_count", 0) or 0
        comment_count = engagement.get("comment_count", 0) or 0

        source = post.get("source")
        VIRAL_MIN_VIEW_COUNT = 1000
        if source == OKX_SOURCE:
            VIRAL_MIN_VIEW_COUNT = VIRAL_MIN_VIEW_COUNT_OKX
        elif source == BINANCE_SOURCE:
            VIRAL_MIN_VIEW_COUNT = VIRAL_MIN_VIEW_COUNT_BINANCE
        else:
            logger.info(f"{source} not in {POST_SOURCES}")

        if not isinstance(view_count, (int, float)) or isinstance(view_count, bool) or view_count <= VIRAL_MIN_VIEW_COUNT:
            continue
        try:
            publish_time = _publish_time_seconds(post.get("publish_time", 0))
        except ValueError:
            continue
        age_hours = (now - publish_time) / 3600
        if age_hours > VIRAL_POST_MAX_AGE_HOURS or age_hours < -1:
            continue
        text = (post.get("content") or {}).get("text_content") or ""
        if any(match.group(1) == "视频" for match in MEDIA_PATTERN.finditer(text)):
            continue

        if not (post.get("media") or {}).get("images"):
            continue

        if not _is_valid_raw_post(post):
            continue
        hot_weight = view_count + like_count * 20 + comment_count * 50
        candidates.append((post, hot_weight, publish_time))

    candidate_ids = [item[0].get("post_id") for item in candidates if item[0].get("post_id") is not None]
    processed_ids = get_remodeled_viral_post_ids(article_manager, POST_SOURCES, candidate_ids)
    unprocessed = [
        item for item in candidates
        if str(item[0].get("post_id", "")).strip() not in processed_ids
    ]

    unprocessed.sort(key=lambda item: (item[1], item[2]), reverse=True)

    result, event_skipped = [], 0
    for post, _, _ in unprocessed:
        event_key = ((post.get("viral_screen") or {}).get("event_key") or "").strip()

        if event_key and used_event_keys[event_key] >= 2:
            event_skipped += 1
            continue
        if event_key:
            used_event_keys[event_key] += 1
        result.append(post)

    logger.info(
        "[高流量/选材] 已筛选待重塑候选帖 | 扫描: [%s] | 粗筛达标: [%s] | 已处理跳过: [%s] "
        "| 同源热点冷却跳过: [%s] | 本轮待精改: [%s] | 耗时: [%.2f 秒]",
        len(posts), len(candidates), len(candidates) - len(unprocessed),
        event_skipped, len(result), time.monotonic() - started,
    )
    return result


# raw{image_semantics,selected_images,global_truth,feasibility_audit,usable,topic_tag,
# expression_strategy,post_text,bait_comments,viral_score}、原图占位符集合 → (bool,错误文本)。
def check_viral_article_info(raw_info, placeholders, max_chars):
    expected_keys = {
        "image_semantics", "selected_images", "global_truth",
        "feasibility_audit", "usable", "topic_tag",
        "expression_strategy", "post_text", "bait_comments", "viral_score",
    }
    error = _key_error(raw_info, expected_keys, "高流量二阶段重塑")
    if error:
        return False, error

    image_semantics = raw_info["image_semantics"]
    if not isinstance(image_semantics, list):
        return False, "image_semantics 必须是列表"

    parsed_ids = []
    kept_ids_in_semantics = set()
    for item in image_semantics:
        if not isinstance(item, dict):
            return False, "image_semantics 中的元素必须是字典对象"
        img_id = item.get("id")
        role = item.get("semantic_role")
        keep = item.get("keep")
        if not isinstance(img_id, str) or img_id not in placeholders:
            return False, f"image_semantics.id ({img_id!r}) 不在原帖真实图片占位符 {sorted(placeholders)} 中"
        if not isinstance(role, str) or not role.strip():
            return False, f"图片 {img_id} 的 semantic_role 必须是非空字符串"
        if not isinstance(keep, bool):
            return False, f"图片 {img_id} 的 keep 必须是布尔值"
        parsed_ids.append(img_id)
        if keep:
            kept_ids_in_semantics.add(img_id)

    if len(parsed_ids) != len(set(parsed_ids)) or set(parsed_ids) != set(placeholders):
        return False, f"image_semantics 必须不重不漏地覆盖全部原帖图片: 期望 {sorted(placeholders)}, 实际 {sorted(parsed_ids)}"

    global_truth = raw_info["global_truth"]
    if not isinstance(global_truth, str) or not global_truth.strip():
        return False, "global_truth 必须是非空字符串"

    feasibility_audit = raw_info["feasibility_audit"]
    if not isinstance(feasibility_audit, str) or not feasibility_audit.strip():
        return False, "feasibility_audit 必须是非空字符串（放行或淘汰均需说明依据）"

    usable = raw_info["usable"]
    if not isinstance(usable, bool):
        return False, "usable 必须是布尔值"

    if not usable:
        return True, ""

    allowed_tags = (
        "搞钱与盈亏", "情感与两性", "奇葩与吃瓜",
        "人性与潜规则", "职场与生存", "搞笑与整活",
    )
    if raw_info["topic_tag"] not in allowed_tags:
        return False, f"topic_tag 取值不合法: {raw_info['topic_tag']!r}"

    selected_images = raw_info["selected_images"]
    if not isinstance(selected_images, list) or any(not isinstance(item, str) or not item for item in selected_images):
        return False, "selected_images 必须是非空字符串组成的列表"
    if len(selected_images) != len(set(selected_images)):
        return False, "selected_images 不允许重复"
    if len(selected_images) > 5:
        return False, "selected_images 最多保留 5 张图片"
    if any(item not in placeholders for item in selected_images):
        return False, f"selected_images 必须来自原帖真实图片占位符 {sorted(placeholders)}"
    if any(item not in kept_ids_in_semantics for item in selected_images):
        return False, "selected_images 中包含了在 image_semantics 里被标记为 keep=false 的废图"

    expression_strategy = raw_info["expression_strategy"]
    if not isinstance(expression_strategy, str) or not expression_strategy.strip():
        return False, "usable 为 true 时 expression_strategy 必须是非空字符串"

    text = raw_info["post_text"]
    if not isinstance(text, str) or not text.strip():
        return False, "post_text 必须是非空字符串"
    if len(re.sub(r"\s", "", text)) > max_chars:
        return False, f"post_text 非空白字符数超过 {max_chars}"
    if re.search(r"\[(?:ASSET_IMG|IMAGE|VIDEO)_\d+\]", text):
        return False, "post_text 不能包含图片或视频占位符"

    comments = raw_info["bait_comments"]
    if (not isinstance(comments, list) or not 1 <= len(comments) <= 3
            or any(not isinstance(item, str) or not item.strip() for item in comments)):
        return False, "bait_comments 必须是包含 1—3 条非空字符串的列表"
    if any(len(re.sub(r"\s", "", item)) > 24 for item in comments):
        return False, "bait_comments 单条评论字数过长（需控制在 24 字以内）"

    viral_score = raw_info["viral_score"]
    if isinstance(viral_score, bool) or not isinstance(viral_score, (int, float)):
        return False, "viral_score 必须是数字"
    if not (0.0 < float(viral_score) <= 10.0):
        return False, "usable 为 true 时 viral_score 必须在 (0, 10] 区间内"

    return True, ""


# post{source,post_id,content,media,comments:[{content}],viral_screen} → 第二阶段任务
# {record,full_prompt,file_paths,placeholders,image_mapping,post_id,topic,model_name}。
def build_viral_article_task(post):
    post_id = post.get("post_id")
    post_id_str = str(post_id or "").strip()
    post_source = post.get("source") or BINANCE_SOURCE
    screen = post.get("viral_screen") or {}
    normalized_text, file_paths, placeholders = normalize_post_media(post)
    source_mapping = build_source_image_mapping(post)
    image_mapping = {
        img_id: {
            "source": post_source,
            "source_post_id": post_id,
            "original_image_id": img_id,
            "original_image_id_list": [img_id],
            "content_id": get_image_content_id(meta.get("local_path") or ""),
            **meta,
        }
        for img_id, meta in source_mapping.items()
    }

    comments = post.get("comments", [])
    comments = [comment_info["content"] for comment_info in comments]
    comments = comments[:20]

    brief = {
        "current_year": datetime.now(BEIJING_TZ).year,
        "original_post": {
            "post_id": post_id_str,
            "text_content": normalized_text.strip(),
            "comments": comments,
            "available_images": list(placeholders.keys()),
        },
        "stage1_screen": {
            "event_key": screen.get("event_key", ""),
        },
    }
    model_name = "gemini-3.1-pro-preview"
    record = {
        "source": post_source,
        "type": ARTICLE_TYPE_VIRAL,
        "topic": "奇葩与吃瓜",
        "stance": "无主语直抛",
        "status": "processing",
        "creation_brief": brief,
        "material_post_mapping": {"M1": post_id},
        "post_id_list": [post_id],
        "prompt_file_path": VIRAL_STAGE2_PROMPT_FILE_PATH,
        "model_name": model_name,
        "prompt_version": prompt_version,
        "article_info": None,
        "error_message": None,
        "error_history": [],
        "raw_response": None,
        "attempt_count": 0,
    }
    prompt = read_file_to_str(VIRAL_STAGE2_PROMPT_FILE_PATH)
    if not isinstance(prompt, str) or not prompt.strip():
        raise ValueError(f"高流量文章提示词为空或读取失败: {VIRAL_STAGE2_PROMPT_FILE_PATH}")
    return {
        "record": record,
        "full_prompt": f"{prompt}\n{json.dumps(brief['original_post'], ensure_ascii=False)}",
        "file_paths": file_paths,
        "placeholders": set(source_mapping.keys()),
        "image_mapping": image_mapping,
        "post_id": post_id_str,
        "topic": "奇葩与吃瓜",
        "model_name": model_name,
    }


# task{record,full_prompt}、校验/转换回调 → record{status,article_info,raw_response,error_history}。
def _generate_article_record(task, validator, convert, action, context, **model_kwargs):
    record = task["record"]
    for attempt in range(1, LLM_MAX_RETRIES + 1):
        record["attempt_count"], record["raw_response"] = attempt, None
        try:
            result = generate_content(prompt=task["full_prompt"], **model_kwargs)
            record["raw_response"] = result.get("content", "")
            raw_info = string_to_object(record["raw_response"])
            valid, error = validator(raw_info)
            if not valid:
                raise ValueError(error)
            info = convert(raw_info)
            record.update(article_info=info, status=info["status"], error_message=None)
            return record
        except Exception as exc:
            error_message = f"{type(exc).__name__}: {exc}"
            record["error_history"].append({
                "attempt": attempt, "error_message": error_message, "raw_response": record["raw_response"],
            })
            exhausted = attempt == LLM_MAX_RETRIES
            if exhausted:
                record.update(status="error", article_info=None, error_message=error_message)
            _log(action, "生成文章时模型调用或结果校验失败",
                 level="error" if exhausted else "warning", exc_info=exhausted,
                 上下文=context, 尝试=f"{attempt}/{LLM_MAX_RETRIES}", 原因=exc,
                 后续="返回error记录，不入库" if exhausted else f"{2 ** attempt}秒后重试",
                 排查="检查模型服务、字段契约和素材/图片引用")
            if not exhausted:
                time.sleep(2 ** attempt)
    return record


# task{record,is_skip,coin,stance,materials,image_mapping,full_prompt} → 生成记录；只保存ok。
def execute_article_task(task_data, article_manager):
    coin, stance = task_data["coin"], task_data["stance"]
    if task_data.get("is_skip"):
        _log("文章/跳过", "选材不足", 币种=coin, 立场=stance, 原因=task_data["reason"])
        return task_data["record"]
    started = time.monotonic()
    mapping = task_data["image_mapping"]

    def convert(info):
        info["image_mapping"] = {key: dict(mapping[key]) for key in info["image_placeholders"]}
        return info

    record = _generate_article_record(
        task_data, lambda info: check_article_info(info, task_data["materials"], mapping, ARTICLE_MAX_CHARS),
        convert, "文章/生成", f"币种={coin},立场={stance}", preset_model_group="medium",
    )
    if record["status"] != "ok":
        if record["status"] == "skip":
            _log("文章/跳过", "模型决定不生成文章", 币种=coin, 立场=stance,
                 原因=(record.get("article_info") or {}).get("reason", "未说明"))
        return record
    saved = prepare_article_record_for_db(record)
    article_manager.upsert_articles([saved])
    _log("文章/完成", "已校验并入库", 币种=coin, 立场=stance,
         尝试=record["attempt_count"], 耗时=f"{time.monotonic() - started:.2f}秒")
    return saved


# 二阶段原始模型字典 → 下游article_info；保留既有字段名及usable=False的精简落库形状。
def _convert_viral_article_info(raw, task):
    record = task["record"]
    usable = raw["usable"]
    record["topic"] = raw["topic_tag"] if usable else "无效噪音"
    info = {
        "status": "ok" if usable else "skip", "usable": usable,
        "decision_reason": raw["feasibility_audit"].strip(),
        "viral_analysis": raw["global_truth"].strip(),
        "image_semantics": raw["image_semantics"], "topic_tag": record["topic"],
        "selected_images": [], "image_role": "", "post_text": "",
        "bait_comments": [], "viral_score": 0.0, "image_mapping": {},
    }
    if not usable:
        return info
    selected = raw["selected_images"]
    info.update(
        selected_images=selected, image_role=raw["expression_strategy"].strip(),
        post_text=raw["post_text"].strip(), used_material_ids=["M1"],
        bait_comments=[comment.strip() for comment in raw["bait_comments"]],
        viral_score=round(float(raw["viral_score"]), 1),
        image_mapping={key: dict(task["image_mapping"][key]) for key in selected},
    )
    return info


# task{record,post_id,full_prompt,file_paths,placeholders,image_mapping} → 保存ok或skip，error不入库。
def execute_viral_article_task(task_data, article_manager):
    started = time.monotonic()
    post_id = task_data["post_id"]
    record = _generate_article_record(
        task_data, lambda info: check_viral_article_info(info, task_data["placeholders"], ARTICLE_MAX_CHARS),
        lambda info: _convert_viral_article_info(info, task_data), "高流量/精改", f"原帖={post_id}",
        preset_model_group="medium", file_paths=task_data["file_paths"],
    )
    if record["status"] == "error":
        return record
    saved = prepare_article_record_for_db(record)
    article_manager.upsert_articles([saved])
    info = saved["article_info"]
    _log("高流量/完成", "原图验货与重塑结果已入库", 原帖=post_id,
         状态=saved["status"], 赛道=saved["topic"], 评分=info["viral_score"],
         选图=len(info["selected_images"]), 原因=info["decision_reason"],
         尝试=record["attempt_count"], 耗时=f"{time.monotonic() - started:.2f}秒")
    return saved


# tasks:[生成任务] → (正常返回记录列表,抛异常任务数,模型跳过数)；按完成顺序收集并隔离单任务故障。
def _execute_article_tasks(tasks, worker, manager, action):
    results, raised, model_skips = [], 0, 0
    if not tasks:
        return results, raised, model_skips
    with ThreadPoolExecutor(max_workers=ARTICLE_CONCURRENCY) as executor:
        futures = {executor.submit(worker, task, manager): task for task in tasks}
        for future in as_completed(futures):
            task = futures[future]
            try:
                result = future.result()
                results.append(result)
                model_skips += result.get("status") == "skip" and not task.get("is_skip")
            except Exception:
                raised += 1
                _log(f"{action}/执行", "任务异常结束，继续其他任务", level="exception",
                     上下文=task.get("post_id") or f"{task.get('coin')}/{task.get('stance')}",
                     排查="检查任务字段与数据库；保存结果未知时先核对记录")
    return results, raised, model_skips


# 当前有效论据{coin:{stance:[论据]}} → 每组2个任务 → 并发生成，返回含ok/skip/error的记录列表。
def generate_analysis_articles_once():
    started = time.monotonic()
    manager = GeneratedArticleManager(gen_db_object())
    grouped = extract_and_group_valid_evidences(manager)
    groups = [(coin, stance, items) for coin, stances in grouped.items() for stance, items in stances.items()]
    tasks = [build_article_task(coin, stance, items, manager) for coin, stance, items in groups
             for _ in range(ARTICLE_VARIANTS)]
    local_skips = sum(bool(task.get("is_skip")) for task in tasks)
    _log("文章/计划", "选材完成", 币种=len(grouped), 分组=len(groups), 任务=len(tasks),
         本地跳过=local_skips, 模型调用上限=(len(tasks) - local_skips) * LLM_MAX_RETRIES,
         选材=f"引用次数<{ARTICLE_POST_USAGE_LIMIT}，素材{ARTICLE_MIN_MATERIALS}—{ARTICLE_MATERIAL_LIMIT}条")
    results, raised, model_skips = _execute_article_tasks(tasks, execute_article_task, manager, "文章")
    counts = Counter(item.get("status") for item in results)
    _log("文章/完成", "本轮任务结束", 任务=len(tasks), 成功=counts["ok"],
         失败=counts["error"] + raised, 跳过=counts["skip"], 本地跳过=local_skips,
         模型跳过=model_skips, 耗时=f"{time.monotonic() - started:.2f}秒")
    return results


# 粗筛通过的单帖列表 → 一对一多模态任务 → 并发生成并保存ok/skip。
def generate_viral_articles_once():
    started = time.monotonic()
    manager = GeneratedArticleManager(gen_db_object())
    tasks = [build_viral_article_task(post) for post in extract_valid_viral_posts(manager)]
    _log("高流量/计划", "第二阶段多模态精改就绪", 任务=len(tasks), 并发=ARTICLE_CONCURRENCY,
         模型调用上限=len(tasks) * LLM_MAX_RETRIES)
    results, raised, _ = _execute_article_tasks(tasks, execute_viral_article_task, manager, "高流量")
    counts = Counter(item.get("status") for item in results)
    _log("高流量/汇总", "本轮精改结束", 任务=len(tasks), 成功=counts["ok"],
         二阶段淘汰=counts["skip"], 失败=counts["error"] + raised,
         耗时=f"{time.monotonic() - started:.2f}秒")
    return results


# 原设计：有返回记录（包括skip/error）等600秒，无记录或异常等60秒。
def _poll_generation(task, action):
    while True:
        try:
            results = task()
            time.sleep(ARTICLE_GENERATION_INTERVAL_SECONDS if results else 60)
        except Exception:
            _log(f"{action}/轮询", "本轮中断，60秒后重新查询", level="exception",
                 排查="检查原帖/论据、提示词与数据库；核对已生成记录")
            time.sleep(60)


def generate_analysis_articles():
    _poll_generation(generate_analysis_articles_once, "单币文章")


def generate_viral_articles():
    _poll_generation(generate_viral_articles_once, "高流量")


# 六、发布：文章类型决定策略，平台适配器负责外部交互


# text/topic → 追加热门话题与币种标签后的正文；保留原标签顺序及重复标签。
def add_hot_topic_to_article(text, topic, tag_count=2):
    hashtags = [item["hashtag"] for item in fetch_binance_hot_hashtags()]
    tags = hashtags[:1] + [f"#{topic}", f"${topic} "]
    hot_coins = fetch_binance_future_hot_coins()
    if hot_coins:
        symbol = hot_coins[0]["symbol"].replace("USDT", "").replace("USDC", "")
        tags.extend([f"#{symbol}", f"${symbol} "])
    logger.info("[文章/标签] 标签已准备 | 主题: [%s] | 标签: [%s]", topic, tags)
    return text + "\n\n\n" + "\n".join(tags)


# 正文 → [{text,delay}]；币种片段delay=2、其后普通片段强制一个换行，其他delay=0。
def _build_publish_text_list(text):
    if not text:
        return []
    result, last_end, after_coin = [], 0, False
    for match in [*COIN_TAG_PATTERN.finditer(text), None]:
        segment = text[last_end:match.start() if match else len(text)]
        if after_coin:
            segment = "\n" + segment.lstrip(" \t\r\n")
        if segment:
            result.append({"text": segment, "delay": 0})
        if match is None:
            break
        result.append({"text": match.group(0), "delay": 2})
        last_end, after_coin = match.end(), True
    return result


# 平台名称 → 该平台对应的账号发帖冷却秒数。
def _get_platform_cooldown_seconds(platform):
    if platform == PLATFORM_OKX:
        return OKX_ACCOUNT_COOLDOWN_SECONDS
    return ACCOUNT_COOLDOWN_SECONDS


# [str或{symbol/coin}] → 有序币种字符串列表；保留大小写、重复项及USDT/USDC替换规则。
def _normalize_hot_coins(hot_coins):
    coins = []
    for item in hot_coins or []:
        value = (item.get("symbol") or item.get("coin") or "") if isinstance(item, dict) else (item or "")
        coin = str(value).replace("USDT", "").replace("USDC", "").strip()
        if coin:
            coins.append(coin)
    return coins


# article → (全部URL/路径/图片内容唯一ID集合, 统计用主标识列表)；同时兼容分析帖与流量帖。
def _extract_article_image_addresses(article):
    info = article.get("article_info")
    if not isinstance(info, dict):
        return set(), []
    mapping = info.get("image_mapping")
    if not isinstance(mapping, dict) or not mapping:
        return set(), []
    placeholders = info.get("selected_images") if article.get("type") == ARTICLE_TYPE_VIRAL else info.get(
        "image_placeholders")
    selected = ([mapping[key] for key in placeholders if key in mapping]
                if isinstance(placeholders, list) and placeholders else mapping.values())
    addresses, primary = set(), []
    for item in selected:
        if not isinstance(item, dict):
            continue
        url = (item.get("original_url") or "").strip()
        path = (item.get("local_path") or "").strip()
        content_id = (item.get("content_id") or "").strip() or get_image_content_id(path)
        addresses.update(value for value in (url, path, content_id) if value)
        primary_key = content_id or path or url
        if primary_key:
            primary.append(primary_key)
    return addresses, primary


# 平台/小时窗口 → (全部URL/路径/内容唯一ID集合, 统计用主标识列表, 已发文章数)；跨文章类型严格按视觉内容去重。
def _get_recent_published_image_usage(article_manager, platform, hours=IMAGE_DEDUP_LOOKBACK_HOURS, now=None):
    recent = article_manager.find_articles(query={
        "status": "ok", f"publish_records.{platform}.status": "success",
        f"publish_records.{platform}.publish_time": {"$gte": (time.time() if now is None else now) - hours * 3600},
    }) or []
    addresses, primary = set(), []
    for article in recent:
        used, first = _extract_article_image_addresses(article)
        addresses.update(used)
        primary.extend(first)
    return addresses, primary, len(recent)


# 账号状态、候选三元组列表 → 可发布索引或None；同主题冷却适用于单币分析帖。
def _select_publish_candidate(account_state, candidates, now, used_addresses, article_type):
    history = account_state.get("topic_publish_history") or {}
    for index, (article, addresses, _) in enumerate(candidates):
        if article_type == ARTICLE_TYPE_ANALYSIS:
            coins_to_check = article.get("coins") or ([article.get("topic")] if article.get("topic") else [])
            if any(now - history.get(c, 0) < ACCOUNT_TOPIC_COOLDOWN_SECONDS for c in coins_to_check if c):
                continue
        if addresses and addresses & used_addresses:
            continue
        return index
    return None


# article{topic,article_info{text,image_placeholders,image_mapping}} → (最终正文,有序图片路径)。
def _format_analysis_post_content(article, platform=PLATFORM_BINANCE):
    info = article["article_info"]
    topic, text = (article.get("topic") or "").strip(), info.get("text", "").strip()
    if topic:
        text = re.sub(rf"(?<!\$)\b{re.escape(topic)}\b", lambda _: f"${topic} ", text, flags=re.IGNORECASE)
    adapter = PLATFORMS[PLATFORM_BINANCE if platform == PLATFORM_BINANCE else PLATFORM_OKX]
    text = adapter.decorate_analysis(text, topic) + "\n\n\n\n\n\n👇"
    if topic and not text.upper().startswith(f"${topic}".upper()):
        text = f"${topic} {text}"
    mapping = info.get("image_mapping") or {}
    paths = [mapping[key].get("local_path") for key in info.get("image_placeholders") or []
             if isinstance(mapping.get(key), dict) and mapping[key].get("local_path")]
    return text, paths


# article_info{post_text,selected_images,image_mapping}、匹配币种 → (加币种前缀的正文,有序图片路径)。
def _format_viral_post_content(info, matched_coin):
    text = info.get("post_text", "").strip()
    prefix = f"${matched_coin}"
    if not text.upper().startswith(prefix.upper()):
        text = f"{prefix} {text}" if text else prefix
    mapping = info.get("image_mapping") or {}
    paths = [mapping[key].get("local_path") for key in info.get("selected_images") or []
             if isinstance(mapping.get(key), dict) and mapping[key].get("local_path")]
    return text, paths


# 安全获取发布时刻币种价格，异常时降级返回 None，不阻断发布主流程。
def _safe_get_latest_price(platform, topic):
    if not topic:
        return None
    try:
        return get_latest_crypto_price(topic)
    except Exception as exc:
        logger.warning(
            "[发布/行情] 获取发布时刻价格失败 | 平台: [%s] | 主题: [%s] | 异常: [%s] | 后续: [价格记为 None 继续落库]",
            platform, topic, exc,
        )
        return None


# 状态文件 → {platform:{account:状态}}；损坏的顶层结构仍按原设计重置为空字典。
def _read_publish_state():
    state = (read_json(STATE_FILE) or {}) if os.path.exists(STATE_FILE) else {}
    return state if isinstance(state, dict) else {}


# state:{platform:{account:dict}} → 目标账号可变字典；补齐两类发布计数及各自的冷却/错误字段。
def _ensure_account_state(state, platform, account, normalize_history=False):
    platform_state = state.setdefault(platform, {})
    if not isinstance(platform_state, dict):
        platform_state = state[platform] = {}
    account_state = platform_state.setdefault(account, {})
    if not isinstance(account_state, dict):
        account_state = platform_state[account] = {}
    defaults = {
        "total_success": 0, "last_publish_time": 0, "last_error_msg": "", "last_error_time": 0,
        "topic_publish_history": {},
        "viral_total_success": 0, "viral_last_publish_time": 0,
        "viral_last_error_msg": "", "viral_last_error_time": 0,
    }
    for key, value in defaults.items():
        account_state.setdefault(key, dict(value) if isinstance(value, dict) else value)
    if normalize_history and not isinstance(account_state["topic_publish_history"], dict):
        account_state["topic_publish_history"] = {}
    return account_state


# 读取快照并初始化已配置账号；只返回数据，不主动落盘。
def _load_and_init_publish_state():
    with STATE_FILE_LOCK:
        state = _read_publish_state()
        for platform, accounts in PLATFORM_ACCOUNTS.items():
            if platform not in state or not isinstance(state[platform], dict):
                state[platform] = {}
            for account in accounts:
                _ensure_account_state(state, platform, account, normalize_history=True)
        return state


# field_updates:{状态字段:新值}、topic_update=(主题,时间)或None → 加锁局部更新并保存。
def _update_account_publish_state(platform, account, field_updates, topic_update=None):
    with STATE_FILE_LOCK:
        state = _read_publish_state()
        account_state = _ensure_account_state(state, platform, account)
        account_state.update(field_updates)
        if topic_update:
            topic, published_at = topic_update
            if not isinstance(account_state.get("topic_publish_history"), dict):
                account_state["topic_publish_history"] = {}
            account_state["topic_publish_history"][topic] = published_at
        save_json(STATE_FILE, state)


# article/plat_record/账号快照、远端确认结果 → 回写账号文件与数据库；返回失败说明或空串。
def _persist_publish_result(article_manager, platform, account, account_state, article, plat_record,
                            success, post_id, attempts, now, topic, latest_price, err_msg,
                            article_type, matched_coin=None):
    viral = article_type == ARTICLE_TYPE_VIRAL
    prefix = PUBLISH_RULES[article_type]["state_prefix"]
    error_detail, topic_update = "", None
    if success:
        state_updates = {
            f"{prefix}total_success": account_state.get(f"{prefix}total_success", 0) + 1,
            f"{prefix}last_publish_time": now,
        }
        account_state.update(state_updates)
        if not viral and topic:
            all_topics = article.get("coins") or ([topic] if topic else [])
            for c in all_topics:
                if c:
                    account_state.setdefault("topic_publish_history", {})[c] = now
            topic_update = (topic, now)
        plat_record.update(status="success", published_by=account, publish_time=now, post_id=post_id,
                           attempts=attempts, last_error=None, last_attempt_time=now, publish_price=latest_price)
        plat_record.setdefault("interacted", False)
        plat_record.setdefault("interaction_time", None)
        plat_record.setdefault("comment_interact_info", {})
    else:
        error_detail = f"发布接口未确认成功；可能是会话失效、网络异常、限流或内容校验失败；详情: {err_msg}"
        state_updates = {f"{prefix}last_error_msg": error_detail, f"{prefix}last_error_time": now}
        account_state.update(state_updates)
        plat_record.update(status="failed", last_error=error_detail, attempts=attempts,
                           last_attempt_time=now, publish_price=latest_price)
    if viral:
        plat_record["matched_coin"] = matched_coin
    problems, saved = [], []
    try:
        _update_account_publish_state(platform, account, state_updates, topic_update=topic_update)
        saved.append("账号状态")
    except Exception as exc:
        problems.append(("写入账号状态", exc))
    try:
        if not viral and article.get("_id") is not None:
            latest = article_manager.find_articles(query={"_id": article["_id"]}, limit=1) or []
            if latest and isinstance(latest[0].get("publish_records"), dict):
                article["publish_records"] = dict(latest[0]["publish_records"])
        if not isinstance(article.get("publish_records"), dict):
            article["publish_records"] = {}
        article["publish_records"][platform] = plat_record

        article_id = article.get("article_id")
        if article_id and hasattr(article_manager, "db") and hasattr(article_manager.db, "get_collection"):
            col = article_manager.db.get_collection(article_manager.collection_name)
            col.update_one(
                {"article_id": article_id},
                {"$set": {f"publish_records.{platform}": plat_record, "updated_at": datetime.now(timezone.utc)}}
            )
        else:
            article_manager.upsert_articles([article])
        saved.append("数据库状态")
    except Exception as exc:
        problems.append(("回写数据库", exc))
    if problems:
        details = "; ".join(f"{label}: {type(exc).__name__}: {exc}" for label, exc in problems)
        raise RuntimeError(f"{details} | 已保存: [{', '.join(saved) or '无'}]") from problems[0][1]
    return error_detail


def _collect_publish_candidates(article_manager, platform, hot_coins, used_addresses, article_type):
    rule = PUBLISH_RULES[article_type]
    viral = article_type == ARTICLE_TYPE_VIRAL
    type_query = ARTICLE_TYPE_VIRAL if viral else ARTICLE_TYPE_ANALYSIS

    raw = article_manager.find_articles(query={
        "type": type_query,
        "created_at": {"$gte": datetime.now(timezone.utc) - timedelta(hours=rule["hours"])},
        "status": "ok", f"publish_records.{platform}.status": {"$ne": "success"},
    }) or []
    coins = _normalize_hot_coins(hot_coins)
    hot_set = {coin.lower() for coin in coins}
    patterns = [
        (coin.upper(), re.compile(rf"(?<![A-Za-z]){re.escape(coin)}(?:USDT|USDC)?(?![A-Za-z])", re.IGNORECASE))
        for coin in coins
    ] if viral else []
    candidates, stats = [], Counter()
    for article in raw:
        info = article.get("article_info")
        if article.get("prompt_version") != prompt_version:
            stats["version"] += 1
            continue

        text_content = info.get(rule["text"]) if isinstance(info, dict) else None

        if (not isinstance(info, dict) or info.get("status") != "ok"
                or (viral and info.get("usable") is not True)
                or not isinstance(text_content, str) or not text_content.strip()
                or not isinstance(info.get(rule["images"], []), list)
                or isinstance(info.get(rule["score"]), bool)
                or not isinstance(info.get(rule["score"]), (int, float))):
            stats["structure"] += 1
            continue

        comments_list = info.get("bait_comments", []) if viral else info.get("god_comments", [])
        comments_text = " ".join([str(c) for c in comments_list if c])
        full_check_text = (text_content + " " + comments_text).lower()

        if platform == PLATFORM_BINANCE and ("okx" in full_check_text or "欧易" in full_check_text):
            stats["rival_keyword"] += 1
            continue
        elif platform == PLATFORM_OKX and ("binance" in full_check_text or "币安" in full_check_text):
            stats["rival_keyword"] += 1
            continue

        record = (article.get("publish_records") or {}).get(platform) or {}
        if record.get("attempts", 0) >= PUBLISH_MAX_ATTEMPTS:
            stats["attempts"] += 1
            continue

        if viral and platform == PLATFORM_OKX:
            article_source = str(article.get("source") or "").strip().lower()
            if article_source == OKX_SOURCE.lower():
                stats["source_conflict"] += 1
                continue

        if viral:
            original_post = (article.get("creation_brief") or {}).get("original_post") or {}
            original_text = original_post.get("text_content")
            clean = MEDIA_PATTERN.sub(" ", re.sub(
                r"\[(?:ASSET_IMG|IMAGE|VIDEO)_\d+\]", " ",
                original_text if isinstance(original_text, str) else "",
            ))
            matched_coin = next((coin for coin, pattern in patterns if pattern.search(clean)), None)
        else:
            topic = (article.get("topic") or "").strip()
            matched_coin = topic if topic and topic.lower() in hot_set else None
        if not matched_coin:
            stats["no_hot_coin"] += 1
            continue
        mapping = info.get("image_mapping") or {}
        if any(not isinstance(mapping.get(key), dict)
               or not _is_valid_image_file(mapping[key].get("local_path"))
               for key in info.get(rule["images"]) or []):
            stats["invalid_image"] += 1
            continue
        addresses, _ = _extract_article_image_addresses(article)
        if addresses and addresses & used_addresses:
            stats["image_dup"] += 1
            continue
        candidates.append((article, addresses, matched_coin))
    candidates.sort(key=lambda item: float(item[0]["article_info"].get(rule["score"], 0.0)), reverse=True)
    return candidates, len(raw), stats


def _publish_articles_for_platform(article_manager, platform, hot_coins, article_type):
    accounts = PLATFORM_ACCOUNTS.get(platform) or []
    if not accounts:
        return
    rule, now = PUBLISH_RULES[article_type], time.time()
    viral = article_type == ARTICLE_TYPE_VIRAL
    platform_state = _load_and_init_publish_state()[platform]
    cooldown = _get_platform_cooldown_seconds(platform)
    dedup_hours = max(rule["hours"], IMAGE_DEDUP_LOOKBACK_HOURS)
    used_addresses, used_primary, recent_count = _get_recent_published_image_usage(
        article_manager, platform, hours=dedup_hours, now=now,
    )
    candidates, raw_count, stats = _collect_publish_candidates(
        article_manager, platform, hot_coins, used_addresses, article_type,
    )

    _log(f"{rule['action']}/计划", "候选已按评分降序排列", 平台=platform, 窗口=f"{rule['hours']}小时",
         图片去重窗口=f"{dedup_hours}小时", 查询=raw_count, 候选=len(candidates),
         过滤=f"版本{stats['version']}/结构{stats['structure']}/尝试超限{stats['attempts']}/来源冲突{stats['source_conflict']}/竞品词{stats['rival_keyword']}/非热币{stats['no_hot_coin']}/图片缺失{stats['invalid_image']}/图片重复{stats['image_dup']}",
         已发=recent_count, 占用图片=len(set(used_primary)),
         账号数=len(accounts), 冷却=f"{cooldown}秒，按文章类型独立计算",
         主题冷却="无" if viral else f"{ACCOUNT_TOPIC_COOLDOWN_SECONDS}秒")

    for account in accounts:
        if not candidates:
            break
        account_state = platform_state[account]
        remaining = cooldown - (now - account_state.get(f"{rule['state_prefix']}last_publish_time", 0))
        if remaining > 0:
            _log(f"{rule['action']}/账号", "账号仍在冷却中", 平台=platform, 账号=account, 剩余=f"{int(remaining)}秒")
            continue
        index = _select_publish_candidate(account_state, candidates, now, used_addresses, article_type)
        if index is None:
            _log(f"{rule['action']}/账号", "候选受主题冷却或已发图片冲突限制", 平台=platform, 账号=account)
            continue
        article, addresses, matched_coin = candidates[index]
        info = article["article_info"]
        topic = (article.get("topic") or info.get("topic_tag") or "") if viral else (article.get("topic") or "").strip()
        stance = article.get("stance", "")
        record = dict((article.get("publish_records") or {}).get(platform) or {})
        started = time.monotonic()
        stage, api_result = "准备正文、媒体及配置", "尚未调用"
        try:
            if viral:
                text, paths = _format_viral_post_content(info, matched_coin)
            else:
                text, paths = _format_analysis_post_content(article, platform=platform)

            session_dir = _get_account_session_dir(platform, account)
            attempts = record.get("attempts", 0) + 1
            stage, api_result = "调用发布接口", "未知"
            chart_coin = None if viral else (article.get("primary_coin") or topic)
            sentiment_tag = None if viral else stance
            err, success, post_id = _execute_platform_post(
                platform, session_dir, text, paths, topic=chart_coin, stance=sentiment_tag,
            )
            post_id, api_result = str(post_id), "成功" if success else "失败"
            latest_price = None if viral else _safe_get_latest_price(platform, topic)
            if success:
                used_addresses.update(addresses)
            stage = "保存已确认的发布结果"
            error = _persist_publish_result(
                article_manager, platform, account, account_state, article, record,
                success, post_id, attempts, now, topic, latest_price, err, article_type, matched_coin,
            )
        except Exception as exc:
            raise RuntimeError(
                f"{rule['action']}阶段[{stage}]失败 | 平台: [{platform}] | 账号: [{account}] "
                f"| 文章: [{article.get('_id')}] | 主题: [{topic}] | 接口结果: [{api_result}] "
                f"| 请核对远端结果与本地状态 | 原因: [{exc}]"
            ) from exc
        if success:
            candidates.pop(index)
        _log(f"{rule['action']}/文章", "本次处理结束", level="info" if success else "error",
             平台=platform, 账号=account, 文章=article.get("_id"), 主题=topic, 热币=matched_coin,
             评分=info.get(rule["score"]), 尝试=f"{attempts}/{PUBLISH_MAX_ATTEMPTS}",
             耗时=f"{time.monotonic() - started:.2f}秒", 结果=api_result,
             说明="远端发布与本地回写完成" if success else error)


def _publish_analysis_articles_for_platform(article_manager, platform, hot_coins):
    _publish_articles_for_platform(article_manager, platform, hot_coins, ARTICLE_TYPE_ANALYSIS)


def _publish_viral_articles_for_platform(article_manager, platform, hot_coins):
    _publish_articles_for_platform(article_manager, platform, hot_coins, ARTICLE_TYPE_VIRAL)


# 管理器、单平台处理回调、动作名 → 按原顺序调度；平台异常只在这里输出一次最终故障日志。
def _publish_on_platforms(article_manager, publish_one_platform, action):
    for platform, adapter in PLATFORMS.items():
        try:
            hot_coins = adapter.hot_coins(for_publish=True) or ["BTC", "ETH", "BNB", "SOL"]
            publish_one_platform(article_manager, platform, hot_coins)
        except Exception:
            _log(f"{action}/平台隔离", "当前平台本轮发布异常，继续其他平台", level="exception",
                 平台=platform, 排查="检查会话、网络及数据库；先核对远端是否已经发布")


def _publish_articles_once(article_manager):
    _publish_on_platforms(article_manager, _publish_analysis_articles_for_platform, "单币分析发布")


def _publish_viral_articles_once(article_manager):
    _publish_on_platforms(article_manager, _publish_viral_articles_for_platform, "流量发布")


# 单轮函数、管理器、间隔 → 持续轮询；仅在调度边界打印一次未处理异常，保留失败后继续的策略。
def _poll_task(task, manager, interval, action):
    while True:
        try:
            task(manager)
        except Exception:
            logger.exception(
                "❌ [%s/轮询] 本轮中断 | 后续: [%s 秒后重新扫描] "
                "| 排查: [检查账号配置、外部接口与数据库；远端操作成功时先核对回写结果]",
                action, interval,
            )
        finally:
            time.sleep(interval)


# 独立文章管理器 → 单币分析帖发布轮询；每轮结束后等待600秒。
def auto_publish_articles():
    _poll_task(
        _publish_articles_once,
        GeneratedArticleManager(gen_db_object()),
        PUBLISH_POLL_SECONDS,
        "单币分析发布",
    )


# 独立文章管理器 → 流量帖发布轮询；每轮结束后等待600秒。
def auto_publish_viral_articles():
    _poll_task(
        _publish_viral_articles_once,
        GeneratedArticleManager(gen_db_object()),
        PUBLISH_POLL_SECONDS,
        "流量发布",
    )


# 七、互动与历史/指定时间段清理


# action、是否保留用户信息 → ({account:{cookies,csrf_token,user_info?}},异常账号数)；逐账号隔离故障。
def _load_account_credentials(action, with_user_info=False):
    credentials, errors = {}, 0
    for account in ACCOUNTS:
        try:
            session_dir = get_config(f"{account}_browser_session_dir")
            cookies, token, user_info = get_auth_tokens_robust(session_dir, debug=False)
            if not cookies or not token:
                logger.warning(
                    "[%s/账号] 凭证不完整 | 账号: [%s] | 后续: [跳过当前账号] "
                    "| 排查: [检查登录状态与浏览器会话]", action, account,
                )
                continue
            credentials[account] = {"cookies": cookies, "csrf_token": token}
            if with_user_info:
                credentials[account]["user_info"] = user_info or {}
        except Exception:
            errors += 1
            logger.exception(
                "❌ [%s/账号] 获取凭证失败 | 账号: [%s] | 后续: [继续其他账号] "
                "| 排查: [检查会话目录、浏览器启动与登录状态]", action, account,
            )
    return credentials, errors


# 评论列表/账号列表/帖子/立场/平台 → {评论文本:{account,comment_id,success,error_info}}。
# 默认 max_count=1（每次随机抽 1 个非发布者账号评论），并支持透传 like 参数控制是否顺带点赞。
def interact_comment(
    god_comments,
    other_account_list,
    post_id,
    stance,
    platform=PLATFORM_BINANCE,
    max_count=1,
    like=False,
):
    results, unconfirmed, raised = {}, 0, 0
    if not other_account_list or not god_comments or max_count <= 0:
        return results
    accounts = random.sample(other_account_list, len(other_account_list))[:max_count]

    for account, text in zip(accounts, god_comments):
        session_dir = _get_account_session_dir(platform, account)
        try:
            if platform not in PLATFORMS:
                raise ValueError(f"不支持的评论互动平台: {platform}")
            error, success, comment_id = PLATFORMS[platform].comment(
                session_dir, post_id, text, stance, like=like,
            )
            results[f"{text}"] = {
                "account": account, "comment_id": str(comment_id) if comment_id else "",
                "success": success, "error_info": error,
            }
            unconfirmed += not success
        except Exception:
            raised += 1
            _log("互动/评论", "发送异常，远端结果未知，继续其他评论", level="exception",
                 平台=platform, 帖子=post_id, 账号=account, 排查="检查登录、网络及远端评论状态")
    if accounts and god_comments:
        _log("互动/评论汇总", "评论尝试结束", level="warning" if unconfirmed or raised else "info",
             平台=platform, 帖子=post_id, 配对=min(len(accounts), len(god_comments)),
             未确认=unconfirmed, 调用异常=raised, 结果条目=len(results),
             排查="未确认时检查结果字典中的error_info及远端评论")
    return results


# 近24小时已发布文章 → 按 INTERACTION_CONFIG 执行可配置点赞与互评（默认取消 OKX 互评与点赞，互评账号数默认 1）。
def _interact_for_platform(article_manager, platform, cutoff_ts):
    started = time.monotonic()
    cfg = INTERACTION_CONFIG.get(platform) or {}
    enable_like = bool(cfg.get("enable_like", False))
    enable_comment = bool(cfg.get("enable_comment", False))
    max_comment_accounts = int(cfg.get("max_comment_accounts", 1))

    if not enable_like and not enable_comment:
        _log("互动/跳过", "当前平台已在 INTERACTION_CONFIG 中关闭点赞与互评", 平台=platform)
        return

    binance = platform == PLATFORM_BINANCE
    credentials = {}
    accounts = ACCOUNTS if binance else list(PLATFORM_ACCOUNTS.get(platform) or [])
    if binance and enable_like:
        credentials, _ = _load_account_credentials("互动")
        if not credentials:
            _log("互动/跳过", "无可用凭证，暂不处理该平台", level="warning",
                 平台=platform, 账号数=len(accounts), 排查="恢复账号登录与浏览器会话")
            return
    if not accounts:
        _log("互动/跳过", "未配置互动账号", level="warning", 平台=platform)
        return
    articles = article_manager.find_articles(query={
        "status": "ok", f"publish_records.{platform}.status": "success",
        f"publish_records.{platform}.publish_time": {"$gte": cutoff_ts},
    }) or []
    pending, post_ids = [], []
    for article in articles:
        record = (article.get("publish_records") or {}).get(platform) or {}
        post_id = record.get("post_id")
        if not record.get("interacted") and post_id and str(post_id) != "None":
            pending.append(article)
            post_ids.append(str(post_id))
    if not pending:
        _log("互动/完成", "没有可提交帖子", 平台=platform, 待处理=0,
             耗时=f"{time.monotonic() - started:.2f}秒")
        return
    if binance and enable_like:
        try:
            like_and_bookmark(post_ids, credentials)
        except Exception:
            _log("互动/执行", "点赞收藏接口异常，保留未互动状态等待下轮", level="exception",
                 平台=platform, 帖数=len(post_ids), 排查="检查凭证、网络与限流；核对远端已完成操作")
            return
    interacted_at = time.time()
    for article in pending:
        record = article["publish_records"][platform]
        other_accounts = [account for account in accounts if account != record.get("published_by")]
        info = article.get("article_info") or {}
        comments = info.get("bait_comments") if article.get("type") == ARTICLE_TYPE_VIRAL else info.get("god_comments")
        raw_stance = article.get("stance")
        sentiment = raw_stance if binance and raw_stance in ("看多", "看空") else None

        if enable_comment and max_comment_accounts > 0:
            if other_accounts:
                record["comment_interact_info"] = interact_comment(
                    comments or [],
                    other_accounts,
                    record.get("post_id"),
                    sentiment,
                    platform=platform,
                    max_count=max_comment_accounts,
                    like=(enable_like and not binance),
                )
            else:
                _log("互动/跳过", "排除发帖账号后没有其他互动账号", level="warning",
                     平台=platform, 帖子=record.get("post_id"), 发布者=record.get("published_by"))
                record["comment_interact_info"] = {}
        elif not binance and enable_like and other_accounts:
            # OKX 仅开启点赞、未开启评论时，随机选 max(1, max_comment_accounts) 个其他账号执行纯点赞
            like_accounts = random.sample(other_accounts, len(other_accounts))[:max(1, max_comment_accounts)]
            like_results = {}
            for acc in like_accounts:
                session_dir = _get_account_session_dir(platform, acc)
                err, ok, _ = PLATFORMS[platform].comment(session_dir, record.get("post_id"), "", sentiment, like=True)
                like_results[f"like_by_{acc}"] = {"account": acc, "success": ok, "error_info": err}
            record["comment_interact_info"] = like_results
        else:
            record["comment_interact_info"] = {}

        record["interacted"], record["interaction_time"] = True, interacted_at
    try:
        article_manager.upsert_articles(pending)
    except Exception:
        _log("互动/回写", "远端互动已尝试，状态保存失败", level="exception", 平台=platform,
             记录=len(pending), 排查="检查数据库；重试前核对已完成的远端互动")
        return
    _log("互动/完成", "接口已返回且状态已保存", 平台=platform, 帖数=len(pending),
         点赞=enable_like, 互评=enable_comment, 互评账号上限=max_comment_accounts,
         耗时=f"{time.monotonic() - started:.2f}秒")


def _hudong_once(article_manager):
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).timestamp()
    _interact_for_platform(article_manager, PLATFORM_BINANCE, cutoff)
    _interact_for_platform(article_manager, PLATFORM_OKX, cutoff)


# 独立文章管理器 → 互动轮询；每轮结束后等待1200秒。
def hudong():
    _poll_task(_hudong_once, GeneratedArticleManager(gen_db_object()), INTERACTION_INTERVAL_SECONDS, "互动")


# 解析时间边界参数（支持 None、秒/毫秒时间戳、datetime、"YYYY-MM-DD" 或 "YYYY-MM-DD HH:MM:SS"）
def _parse_time_bound(value, default_ts, is_end=False):
    if value is None:
        return default_ts
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return _publish_time_seconds(value)
    if isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=BEIJING_TZ)
        return dt.timestamp()
    if isinstance(value, str):
        text = value.strip()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(text, fmt).replace(tzinfo=BEIJING_TZ)
                if fmt == "%Y-%m-%d" and is_end:
                    dt = dt + timedelta(days=1) - timedelta(seconds=1)
                return dt.timestamp()
            except ValueError:
                continue
    raise ValueError(f"无法解析时间参数: {value!r}")


def delete_posts_by_account_and_time(
    platform=PLATFORM_OKX,
    accounts=None,
    start_time=None,
    end_time=None,
    days=3,
    extra_post_ids=None,
    sync_db=True,
    debug=False,
):
    """删除指定平台、指定账号在指定时间段内的所有帖子（默认删除最近 3 天的所有帖子）。

    参数:
        platform (str): 目标平台，"okx" 或 "binance"，默认 "okx"。
        accounts (list[str] | str | None): 指定账号列表（如 ["myself", "mama"] 或 "myself"），None 表示该平台全部账号。
        start_time (str | float | datetime | None): 起始时间（支持 "YYYY-MM-DD"、"YYYY-MM-DD HH:MM:SS" 或时间戳），
            为 None 时默认取 `end_time - days * 86400`（即最近 days 天）。
        end_time (str | float | datetime | None): 结束时间，为 None 时默认取当前时间。
        days (float): 当 start_time 为 None 时，默认向前追溯的天数（默认 3 天）。
        extra_post_ids (list[str] | dict[str, list[str]] | None): 额外补充要强制删除的 post_id 列表或 {account: [post_ids]} 映射。
        sync_db (bool): 删除成功后是否同步在 MongoDB generated_articles 中标记 delete_time 并清理 social_media_posts 中的自爬脏数据。
        debug (bool): 是否开启浏览器可视化调试模式。

    返回:
        dict: 各账号的删除统计汇总 `{account: {"target_count": int, "deleted": list, "failed": list}}`。
    """
    started = time.monotonic()
    now = time.time()
    end_ts = _parse_time_bound(end_time, now, is_end=True)
    start_ts = _parse_time_bound(start_time, end_ts - float(days) * 86400.0, is_end=False)
    start_str = datetime.fromtimestamp(start_ts, BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
    end_str = datetime.fromtimestamp(end_ts, BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(accounts, str):
        target_accounts = [accounts.strip()] if accounts.strip() else []
    elif isinstance(accounts, (list, tuple, set)):
        target_accounts = [str(a).strip() for a in accounts if str(a).strip()]
    else:
        target_accounts = list(PLATFORM_ACCOUNTS.get(platform) or [])

    if not target_accounts:
        _log("按时间删帖/跳过", "未找到有效目标账号", level="warning", 平台=platform)
        return {}

    article_manager = GeneratedArticleManager(gen_db_object())
    post_manager = UniversalPostManager(gen_db_object())

    # 1. 从 generated_articles 收集各账号在 [start_ts, end_ts] 内发布的 post_id
    account_post_ids = {acc: [] for acc in target_accounts}
    articles_by_post_id = defaultdict(list)

    all_published_articles = article_manager.find_articles(
        query={
            f"publish_records.{platform}.post_id": {"$exists": True, "$ne": None},
        },
        limit=0,
    ) or []

    for art in all_published_articles:
        rec = ((art.get("publish_records") or {}).get(platform)) or {}
        pid = str(rec.get("post_id") or "").strip()
        if not pid or pid == "None":
            continue
        articles_by_post_id[pid].append(art)
        if rec.get("delete_time"):
            continue
        pub_by = str(rec.get("published_by") or "").strip()
        if pub_by not in account_post_ids:
            continue
        raw_pub_time = rec.get("publish_time")
        if raw_pub_time is not None:
            try:
                pub_ts = _publish_time_seconds(raw_pub_time)
            except ValueError:
                pub_ts = None
        else:
            created_at = art.get("created_at")
            pub_ts = created_at.timestamp() if isinstance(created_at, datetime) else None
        if pub_ts is not None and start_ts <= pub_ts <= end_ts:
            account_post_ids[pub_by].append(pid)

    # 2. 从 social_media_posts 补充扫描自家账号在 [start_ts, end_ts] 内被抓回来的帖子（防止 publish_records 漏记或被覆盖）
    uid_to_account = {}
    if platform == PLATFORM_OKX:
        uid_to_account = {uid: acc for acc, uid in OKX_ACCOUNT_UID_MAP.items() if acc in account_post_ids}
    elif platform == PLATFORM_BINANCE:
        uid_to_account = {uname: acc for acc, uname in BINANCE_ACCOUNT_USERNAME_MAP.items() if acc in account_post_ids}

    if uid_to_account:
        raw_posts = _find_posts_by_sources(post_manager, sources=(platform,), limit=POST_QUERY_LIMIT)
        for p in raw_posts:
            pid = str(p.get("post_id") or "").strip()
            if not pid:
                continue
            author = p.get("author") or {}
            aid = str(
                p.get("author_id")
                or author.get("author_id")
                or author.get("id")
                or author.get("uid")
                or author.get("username")
                or ""
            ).strip()
            acc = uid_to_account.get(aid)
            if not acc:
                continue
            try:
                p_ts = _publish_time_seconds(p.get("publish_time", 0))
            except ValueError:
                continue
            if start_ts <= p_ts <= end_ts:
                account_post_ids[acc].append(pid)

    # 3. 合并 extra_post_ids
    if isinstance(extra_post_ids, dict):
        for acc, pids in extra_post_ids.items():
            if acc in account_post_ids and pids:
                account_post_ids[acc].extend(str(x).strip() for x in pids if str(x).strip())
    elif isinstance(extra_post_ids, (list, tuple, set)) and target_accounts:
        for pid in extra_post_ids:
            if str(pid).strip():
                account_post_ids[target_accounts[0]].append(str(pid).strip())

    # 对每个账号的 post_id 保序去重
    for acc in target_accounts:
        account_post_ids[acc] = list(dict.fromkeys(account_post_ids[acc]))

    _log(
        "按时间删帖/计划",
        "已汇总待删帖清单",
        平台=platform,
        时间段=f"{start_str} ~ {end_str}",
        账号清单={acc: len(pids) for acc, pids in account_post_ids.items()},
    )

    summary = {}
    credentials = {}
    if platform == PLATFORM_BINANCE:
        credentials, _ = _load_account_credentials("按时间删帖", with_user_info=True)

    for acc in target_accounts:
        pids = account_post_ids.get(acc) or []
        deleted_list, failed_list = [], []

        if platform == PLATFORM_OKX:
            okx_uid = OKX_ACCOUNT_UID_MAP.get(acc)
            if okx_uid:
                try:
                    remote_okx_items = fetch_okx_user_timeline(
                        author_id=okx_uid,
                        max_pages=60,
                        start_ts=start_ts,
                        end_ts=end_ts,
                    ) or []
                    for item in remote_okx_items:
                        rid = str(item.get("post_id") or "").strip()
                        if rid and rid not in pids:
                            pids.append(rid)
                except Exception as exc:
                    _log("按时间删帖/远端扫描", "拉取 OKX 远端主页帖子列表异常，继续按已知ID删除", level="warning", 账号=acc, 原因=exc)
            if not pids:
                summary[acc] = {"target_count": 0, "deleted": [], "failed": []}
                continue
            session_dir = _get_account_session_dir(PLATFORM_OKX, acc)
            _, _, detail = delete_okx_posts(post_id_list=pids, user_data_dir=session_dir, debug=debug)
            detail = detail or {}
            deleted_list.extend(str(x) for x in (detail.get("success_ids") or []))
            failed_list.extend(str(x) for x in ((detail.get("failed_ids") or {}).keys()))
        elif platform == PLATFORM_BINANCE:
            cred = credentials.get(acc)
            if not cred:
                _log("按时间删帖/跳过", "Binance 账号凭证不可用", level="warning", 账号=acc)
                summary[acc] = {"target_count": len(pids), "deleted": [], "failed": pids}
                continue
            # 同时扫描远端历史列表在 [start_ts, end_ts] 内的帖子
            square_uid = (cred.get("user_info") or {}).get("squareUid")
            if square_uid:
                try:
                    remote_items = fetch_binance_square_replies(
                        target_square_uid=square_uid,
                        cookies=cred["cookies"],
                        csrf_token=cred["csrf_token"],
                        limit=1000,
                        time_offset=int(end_ts * 1000),
                    ) or []
                    for item in remote_items:
                        rid = str(item.get("reply_id") or "").strip()
                        r_ts = item.get("create_time") or item.get("publish_time")
                        if rid:
                            if r_ts is not None:
                                try:
                                    if not (start_ts <= _publish_time_seconds(r_ts) <= end_ts):
                                        continue
                                except ValueError:
                                    pass
                            if rid not in pids:
                                pids.append(rid)
                except Exception as exc:
                    _log("按时间删帖/远端扫描", "拉取 Binance 远端帖子列表异常，继续按已知ID删除", level="warning", 账号=acc, 原因=exc)

            for pid in pids:
                try:
                    ok = delete_binance_square_content(
                        content_id=pid, cookies=cred["cookies"], csrf_token=cred["csrf_token"],
                    )
                    if ok:
                        deleted_list.append(pid)
                    else:
                        failed_list.append(pid)
                except Exception:
                    failed_list.append(pid)

        summary[acc] = {
            "target_count": len(pids),
            "deleted": deleted_list,
            "failed": failed_list,
        }

        if sync_db and deleted_list:
            deleted_at = time.time()
            checked_at = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
            reason_str = f"按时间段批量清理 ({start_str} ~ {end_str})"
            updated_articles = []
            for pid in deleted_list:
                for art in articles_by_post_id.get(pid, []):
                    p_rec = art.setdefault("publish_records", {}).setdefault(platform, {})
                    p_rec.update(
                        delete_time=deleted_at,
                        delete_reason=reason_str,
                        check_time=checked_at,
                    )
                    updated_articles.append(art)
            if updated_articles:
                try:
                    article_manager.upsert_articles(updated_articles)
                except Exception as exc:
                    _log("按时间删帖/回写", "回写 generated_articles 删除状态异常", level="warning", 账号=acc, 原因=exc)
            try:
                if hasattr(post_manager, "collection"):
                    q_ids = list(deleted_list) + [int(x) for x in deleted_list if x.isdigit()]
                    post_manager.collection.delete_many({"post_id": {"$in": q_ids}})
            except Exception as exc:
                _log("按时间删帖/清理原帖", "清理 social_media_posts 自爬记录异常", level="warning", 账号=acc, 原因=exc)

        _log(
            "按时间删帖/账号完成",
            "账号时间段帖子清理完毕",
            平台=platform,
            账号=acc,
            目标数=len(pids),
            成功删除=len(deleted_list),
            失败数=len(failed_list),
        )

    _log(
        "按时间删帖/汇总",
        "全部指定账号删帖任务结束",
        平台=platform,
        时间段=f"{start_str} ~ {end_str}",
        总成功=sum(len(v["deleted"]) for v in summary.values()),
        总失败=sum(len(v["failed"]) for v in summary.values()),
        耗时=f"{time.monotonic() - started:.2f}秒",
    )
    return summary


# article{stance, publish_records.binance}、当前秒、prices=None或(发布价,现价) → (删除原因或None,是否更新涨跌幅)。
def _article_delete_reason(article, now, prices):
    b_rec = ((article.get("publish_records") or {}).get(PLATFORM_BINANCE)) or {}
    price_updated = prices is not None
    if article.get("type") == ARTICLE_TYPE_VIRAL:
        prices = None
        price_updated = False
    elif price_updated:
        base_price, current_price = prices
        difference = (current_price - base_price) / base_price
        profit = difference if article.get("stance") == "看多" else -difference
        b_rec["profit_pct"] = round(profit, 4)
        threshold = 0.2 if article.get("stance") == "看多" else 0.5
        if profit <= -threshold:
            return f"逆向 {abs(profit) * 100:.2f}% (基准: {threshold * 100:.0f}%)", True
    publish_time = b_rec.get("publish_time")
    if publish_time is None:
        return None, price_updated
    try:
        elapsed = now - _publish_time_seconds(publish_time)
        if elapsed >= MAX_REPLAY_DAYS * 24 * 3600:
            return f"超时 (已发布 {elapsed / (24 * 3600):.2f} 天, 基准: {MAX_REPLAY_DAYS} 天)", price_updated
    except Exception:
        logger.warning(
            "[历史清理/时间] 无法判断帖子是否超时 | 帖子: [%s] | 发布时间: [%s] "
            "| 后续: [保留当前时效状态] | 排查: [核对秒/毫秒时间戳]",
            b_rec.get("post_id"), publish_time, exc_info=True,
        )
    return None, price_updated


# 已发Binance文章/远端历史 → 按逆向行情或7天时效删除，回写publish_records.binance。
def _delete_old_posts_once(article_manager):
    started, now = time.monotonic(), time.time()
    reverse_count = timeout_count = failed = 0
    deleted_ids = set()
    credentials, account_errors = _load_account_credentials("历史清理", with_user_info=True)
    try:
        articles = article_manager.find_articles(query={
            f"publish_records.{PLATFORM_BINANCE}.status": "success",
            f"publish_records.{PLATFORM_BINANCE}.post_id": {"$exists": True, "$ne": None},
            f"publish_records.{PLATFORM_BINANCE}.delete_time": {"$in": [None, False]},
        }) or []
        logger.info("[历史清理/巡检] 已查询待检测文章 | 文章: [%s] | 可用账号: [%s]", len(articles), len(credentials))
        for article in articles:
            b_rec = ((article.get("publish_records") or {}).get(PLATFORM_BINANCE)) or {}
            if b_rec.get("delete_time"):
                continue
            post_id = str(b_rec.get("post_id") or "").strip()
            if not post_id:
                continue
            account, topic, stance = b_rec.get("published_by"), article.get("topic"), article.get("stance")
            prices = None
            try:
                if topic and stance in ("看多", "看空") and b_rec.get("publish_price") is not None:
                    base_price = float(b_rec["publish_price"])
                    if base_price > 0:
                        latest = get_latest_crypto_price(topic)
                        if latest is not None:
                            prices = base_price, float(latest)
            except Exception:
                logger.warning(
                    "[历史清理/行情] 读取或转换价格失败 | 帖子: [%s] | 币种: [%s] "
                    "| 后续: [继续时效检查] | 排查: [检查价格字段、行情接口与网络]",
                    post_id, topic, exc_info=True,
                )
            reason, price_updated = _article_delete_reason(article, now, prices)
            deleted = False
            credential = credentials.get(account)
            if reason and not credential:
                logger.warning(
                    "[历史清理/无法删除] 已触发删除条件，但账号凭证不可用 | 账号: [%s] | 帖子: [%s] "
                    "| 原因: [%s] | 排查: [恢复对应账号登录与会话]", account, post_id, reason,
                )
            if reason and credential:
                stage = "请求远端删除"
                try:
                    success = delete_binance_square_content(
                        content_id=post_id, cookies=credential["cookies"], csrf_token=credential["csrf_token"],
                    )
                    if success:
                        deleted = True
                        deleted_ids.add(post_id)
                        b_rec.update(
                            delete_time=time.time(), delete_reason=reason,
                            check_time=datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S"),
                        )
                        stage = "回写已删除状态"
                        article_manager.upsert_articles([article])
                        reverse_count += reason.startswith("逆向")
                        timeout_count += not reason.startswith("逆向")
                        logger.info(
                            "[历史清理/删除] 远端删除并回写成功 | 帖子: [%s] | 账号: [%s] | 原因: [%s]",
                            post_id, account, reason,
                        )
                    else:
                        failed += 1
                        logger.warning(
                            "[历史清理/删除] 接口未确认成功 | 帖子: [%s] | 账号: [%s] "
                            "| 后续: [下轮再检查] | 排查: [检查凭证、限流与远端帖子是否存在]", post_id, account,
                        )
                except Exception:
                    failed += 1
                    logger.exception(
                        "❌ [历史清理/删除] 帖子处理失败 | 阶段: [%s] | 帖子: [%s] | 账号: [%s] "
                        "| 远端已确认删除: [%s] | 后续: [继续其他帖子] "
                        "| 排查: [检查删除接口或数据库，先核对远端结果]", stage, post_id, account, deleted,
                    )
            if not deleted and price_updated:
                try:
                    b_rec["check_time"] = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
                    article_manager.upsert_articles([article])
                except Exception:
                    logger.warning(
                        "[历史清理/行情回写] 涨跌幅保存失败 | 帖子: [%s] | 后续: [继续其他帖子] "
                        "| 排查: [检查数据库连接与写入权限]", post_id, exc_info=True,
                    )
    except Exception:
        logger.exception(
            "❌ [历史清理/巡检] 数据库扫描或处理异常 | 后续: [继续远端兜底清理] "
            "| 排查: [检查数据库连接、文章结构与删除结果]"
        )
    try:
        cutoff_ms = int((now - MAX_REPLAY_DAYS * 24 * 3600) * 1000)

        for account in ACCOUNTS:
            credential = credentials.get(account)
            if not credential:
                continue
            square_uid = (credential.get("user_info") or {}).get("squareUid")
            if not square_uid:
                continue
            stage = "拉取历史帖子与回复"
            try:
                items = fetch_binance_square_replies(
                    target_square_uid=square_uid, cookies=credential["cookies"], csrf_token=credential["csrf_token"],
                    limit=1000, time_offset=cutoff_ms, filter_type="REPLY",
                ) or []
                items.extend(fetch_binance_square_replies(
                    target_square_uid=square_uid, cookies=credential["cookies"], csrf_token=credential["csrf_token"],
                    limit=1000, time_offset=cutoff_ms,
                ) or [])
                for item in items:
                    raw_id = item.get("reply_id")
                    if not raw_id or str(raw_id) in deleted_ids:
                        continue
                    post_id = str(raw_id)
                    try:
                        success = delete_binance_square_content(
                            content_id=post_id, cookies=credential["cookies"], csrf_token=credential["csrf_token"],
                        )
                    except Exception:
                        failed += 1
                        logger.exception(
                            "❌ [历史清理/远端删除] 删除请求异常 | 账号: [%s] | 帖子: [%s] "
                            "| 后续: [继续其他记录] | 排查: [检查凭证、网络与限流，核对远端结果]", account, post_id,
                        )
                        continue
                    if not success:
                        failed += 1
                        logger.warning(
                            "[历史清理/远端删除] 接口未确认成功 | 账号: [%s] | 帖子: [%s] "
                            "| 排查: [检查帖子是否存在、登录与接口响应]", account, post_id,
                        )
                        continue
                    timeout_count += 1
                    deleted_ids.add(post_id)
                    stage = "回写远端已删除记录"
                    matched = article_manager.find_articles(
                        query={f"publish_records.{PLATFORM_BINANCE}.post_id": post_id}
                    )
                    if not matched:
                        continue
                    deleted_at = time.time()
                    checked_at = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
                    for article in matched:
                        b_rec = article.setdefault("publish_records", {}).setdefault(PLATFORM_BINANCE, {})
                        b_rec.update(
                            delete_time=deleted_at,
                            delete_reason=f"超时 (基准: {MAX_REPLAY_DAYS} 天)",
                            check_time=checked_at,
                        )
                    article_manager.upsert_articles(matched)
                    logger.info("[历史清理/远端回写] 已保存删除状态 | 帖子: [%s] | 记录: [%s]", post_id, len(matched))
            except Exception:
                account_errors += 1
                logger.exception(
                    "❌ [历史清理/远端账号] 当前账号处理失败 | 账号: [%s] | 阶段: [%s] "
                    "| 后续: [继续其他账号] | 排查: [检查历史接口、用户信息及数据库；核对已完成的删除]", account, stage,
                )
    except Exception:
        logger.exception(
            "❌ [历史清理/兜底] 远端清理本轮中断 | 后续: [下轮重试] "
            "| 排查: [检查账号用户信息、历史列表与数据库]"
        )
    logger.info(
        "[历史清理/汇总] 本轮结束 | 逆向删除: [%s] | 超时删除: [%s] | 失败: [%s] "
        "| 账号异常: [%s] | 耗时: [%.2f 秒] | 下次检查: [%s 秒后]",
        reverse_count, timeout_count, failed, account_errors, time.monotonic() - started, DELETE_INTERVAL_SEC,
    )


# 独立文章管理器 → 每小时执行数据库巡检和远端兜底；通过 _poll_task 接入异常守护轮询。
def delete_old_post():
    _poll_task(_delete_old_posts_once, GeneratedArticleManager(gen_db_object()), DELETE_INTERVAL_SEC, "历史清理")


# 八、人工查询与复盘入口


# 文章列表{article_info,creation_brief} → 仅含正文/神评/配图/素材ID与创作brief的列表。
def clean_article_records(raw_records):
    results = []
    for item in raw_records:
        if not isinstance(item, dict):
            continue
        info, brief = item.get("article_info") or {}, item.get("creation_brief") or {}
        results.append({
            "article_info": {key: info.get(key, "" if key == "text" else [])
                             for key in ("text", "god_comments", "image_placeholders", "used_material_ids")},
            "creation_brief": {"task": brief.get("task", {}), "materials": brief.get("materials", [])},
        })
    return results


# 近6小时当前版本未发布分析文章随机抽10篇 → (清洗文章列表,[{origin_content,logic_mul}])。
def get_need_check_article_info():
    manager = GeneratedArticleManager(gen_db_object())
    candidates = manager.find_articles(query={
        "created_at": {"$gte": datetime.now(timezone.utc) - timedelta(hours=6)},
        "status": "ok",
        f"publish_records.{PLATFORM_BINANCE}.status": {"$ne": "success"},
        f"publish_records.{PLATFORM_OKX}.status": {"$ne": "success"},
    }) or []
    candidates = [article for article in candidates if article.get("prompt_version") == prompt_version]
    random.shuffle(candidates)
    selected = candidates[:10]
    data = clean_article_records(selected)
    raw_ids = set()
    for article in selected:
        mapping = article.get("material_post_mapping") or {}
        if isinstance(mapping, dict):
            raw_ids.update(str(post_id).strip() for post_id in mapping.values() if post_id)
    source_posts = []
    if raw_ids:
        post_manager = UniversalPostManager(gen_db_object())
        ids = list(raw_ids)
        query_ids = ids + [int(post_id) for post_id in ids if post_id.isdigit()]
        raw_posts = post_manager.find_posts(query={"post_id": {"$in": query_ids}}, limit=0) or []
        source_posts = [{"origin_content": (post.get("content") or {}).get("text_content", ""),
                         "logic_mul": post.get("logic_mul")} for post in raw_posts if isinstance(post, dict)]
    logger.info(
        "[文章/待审查] 审查数据已组装 | 候选: [%s] | 随机抽取: [%s] | 关联原帖: [%s]",
        len(candidates), len(data), len(source_posts),
    )
    return data, source_posts


# 关键字、小时数、最低浏览量 → [{text_content,images,engagement{view_count,like_count,comment_count}}]。
def query_hot_posts_by_keyword(target_key, hours=24, min_views=1000):
    manager = UniversalPostManager(gen_db_object())
    start_time = time.time() - hours * 3600
    query = {
        "source": {"$in": list(POST_SOURCES)}, "publish_time": {"$gte": start_time},
        "engagement.view_count": {"$gte": min_views},
        "content.text_content": {"$regex": target_key, "$options": "i"},
    }
    posts = manager.find_posts(query=query, sort=[("engagement.view_count", -1)], limit=0) or []
    results = []
    for post in posts:
        engagement = post.get("engagement") or {}
        results.append({
            "text_content": (post.get("content") or {}).get("text_content", ""),
            "images": (post.get("media") or {}).get("images", []),
            "engagement": {key: engagement.get(key, 0) for key in ("view_count", "like_count", "comment_count")},
        })
    logger.info(
        "[数据/查询] 热门帖查询完成 | 关键字: [%s] | 时长: [%s 小时] | 匹配: [%s]",
        target_key, hours, len(results),
    )
    return results


# 筛选条件/抽样上限/可选save_path → [{post_id,text_content,images,no_evidence,reason}]。
def get_need_check_pre_screen_info(
        limit=100,
        no_evidence_filter=None,
        only_current_version=True,
        random_sample=True,
        save_path=None,
):
    post_manager = UniversalPostManager(gen_db_object())
    query = {
        "source": {"$in": list(POST_SOURCES)},
        "pre_screen": {"$exists": True, "$ne": None},
    }
    if only_current_version:
        query["pre_screen.prompt_version"] = prompt_version
    if isinstance(no_evidence_filter, bool):
        query["pre_screen.no_evidence"] = no_evidence_filter

    raw_posts = post_manager.find_posts(query=query, limit=0) or []
    total_matched = len(raw_posts)
    total_no_ev = sum(1 for p in raw_posts if (p.get("pre_screen") or {}).get("no_evidence") is True)
    total_has_ev = total_matched - total_no_ev

    if random_sample:
        random.shuffle(raw_posts)
    selected_posts = raw_posts[:limit] if (limit and limit > 0) else raw_posts

    review_list = []
    for post in selected_posts:
        if not isinstance(post, dict):
            continue
        base_item = build_pre_screen_item(post)
        pre_screen = post.get("pre_screen") or {}
        review_list.append({
            "post_id": base_item["post_id"],
            "text_content": base_item["text_content"],
            "images": base_item["images"],
            "no_evidence": pre_screen.get("no_evidence"),
            "reason": pre_screen.get("reason", ""),
        })

    if save_path:
        save_json(save_path, review_list)

    logger.info(
        "[初筛/审查] 已导出初筛复盘数据 | 匹配总数: [%s (淘汰: %s / 保留: %s)] "
        "| 本次抽取: [%s] | 过滤条件(no_evidence): [%s] | 导出文件: [%s]",
        total_matched, total_no_ev, total_has_ev,
        len(review_list), no_evidence_filter, save_path or "未落盘",
    )
    return review_list


# 抽样条件 → [{source_post:{post_id,text,images,engagement},remodeled_article:{usable,...}}]。
def get_need_check_viral_article_info(
        limit=100,
        hours=12,
        only_current_version=True,
        random_sample=True,
        save_path=None,
):
    article_manager = GeneratedArticleManager(gen_db_object())
    query = {
        "type": ARTICLE_TYPE_VIRAL,
        f"publish_records.{PLATFORM_BINANCE}.status": {"$ne": "success"},
        f"publish_records.{PLATFORM_OKX}.status": {"$ne": "success"},
    }
    if hours and hours > 0:
        query["created_at"] = {"$gte": datetime.now(timezone.utc) - timedelta(hours=hours)}
    if only_current_version:
        query["prompt_version"] = prompt_version

    candidates = article_manager.find_articles(query=query, sort=[("created_at", -1)]) or []
    if not candidates:
        logger.info("[高流量/审查] 未找到待审查文章 | 筛选时间: [近 %s 小时]", hours)
        return []

    if random_sample:
        random.shuffle(candidates)
    selected_articles = candidates[:limit] if (limit and limit > 0) else candidates

    post_ids = set()
    for art in selected_articles:
        for pid in art.get("post_id_list") or []:
            if pid is not None and str(pid).strip():
                post_ids.add(str(pid).strip())
                if str(pid).strip().isdigit():
                    post_ids.add(int(pid))

    post_map = {}
    if post_ids:
        post_manager = UniversalPostManager(gen_db_object())
        raw_posts = post_manager.find_posts(query={"post_id": {"$in": list(post_ids)}}, limit=0) or []
        for p in raw_posts:
            if isinstance(p, dict) and p.get("post_id") is not None:
                post_map[str(p["post_id"]).strip()] = p

    review_list = []
    for article in selected_articles:
        p_list = article.get("post_id_list") or []
        target_post_id = str(p_list[0]).strip() if p_list else ""

        raw_p = post_map.get(target_post_id, {})
        info = article.get("article_info") or {}
        engagement = raw_p.get("engagement") or {}

        clean_text, _, mapping = normalize_post_media(raw_p) if raw_p else ("", [], {})
        image_placeholders = list(mapping.keys())

        source_post_data = {
            "post_id": str(raw_p.get("post_id") or target_post_id).strip(),
            "text": clean_text.strip(),
            "images": image_placeholders,
            "engagement": {
                "view_count": engagement.get("view_count", 0),
                "like_count": engagement.get("like_count", 0),
                "comment_count": engagement.get("comment_count", 0),
            },
        }

        usable = info.get("usable", False)
        if not usable:
            remodeled_article_data = {
                "usable": False,
                "image_semantics": info.get("image_semantics", []),
                "viral_analysis": info.get("viral_analysis", ""),
                "decision_reason": info.get("decision_reason", ""),
            }
        else:
            remodeled_article_data = {
                "usable": True,
                "topic_tag": info.get("topic_tag", ""),
                "viral_score": info.get("viral_score", 0.0),
                "image_semantics": info.get("image_semantics", []),
                "selected_images": info.get("selected_images", []),
                "viral_analysis": info.get("viral_analysis", ""),
                "decision_reason": info.get("decision_reason", ""),
                "image_role": info.get("image_role", ""),
                "post_text": info.get("post_text", ""),
                "bait_comments": info.get("bait_comments", []),
            }

        remodeled_article_data = {
            k: v for k, v in remodeled_article_data.items()
            if k == "usable" or v not in (None, "", [], {})
        }

        review_list.append({
            "source_post": source_post_data,
            "remodeled_article": remodeled_article_data,
        })

    if save_path:
        save_json(save_path, review_list)

    logger.info(
        "[高流量/审查] 精简复盘数据组装完成 | 候选: [%s] | 抽样返回: [%s] | 导出文件: [%s]",
        len(candidates), len(review_list), save_path or "未落盘",
    )
    return review_list


# 九、进程入口：根据 TASK_SWITCHES 启动流水线任务


# 无参任务 → 当前线程执行；未被业务边界处理的异常记录上下文后重抛，不自动重启线程。
def _run_task(task):
    try:
        task()
    except Exception:
        logger.exception(
            "❌ [任务/退出] 后台任务异常结束 | 任务: [%s] | 结果: [当前线程停止] "
            "| 排查: [检查对应链路的数据、文件权限与外部服务]", task.__name__,
        )
        raise


def main():
    # 手动按账号与时间段删帖示例（默认删除最近 3 天的所有帖子）：
    # delete_posts_by_account_and_time(platform=PLATFORM_OKX, days=3)

    task_map = {
        "fetch_and_screen": format_image_article,
        "single_analysis_gen": generate_analysis_articles,
        "single_analysis_pub": auto_publish_articles,
        "viral_gen": generate_viral_articles,
        # "viral_pub": auto_publish_viral_articles,
        "hudong": hudong,
        # "delete_old_post": delete_old_post,
    }

    tasks = [task_func for key, task_func in task_map.items() if TASK_SWITCHES.get(key, False)]
    logger.info(
        "[主控/启动] 正在根据 TASK_SWITCHES 启动流水线线程 | 激活任务数: [%s] | 任务清单: %s",
        len(tasks), [t.__name__ for t in tasks],
    )

    threads = [threading.Thread(target=_run_task, args=(task,), name=task.__name__) for task in tasks]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


if __name__ == "__main__":
    main()

