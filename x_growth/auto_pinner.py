# -*- coding: utf-8 -*-
"""
X (Twitter) 72 小时最高转化率自动置顶系统 (`x_growth/auto_pinner.py`)

极简、高内聚、全自动闭环架构（零命令行参数，顶部直接配置，默认使用 `x_myself`）：
- 【核心目标】：每 6 小时（可配置）自动巡检启用账号最近 72 小时内的原创/引用主帖，
  结合 X 官方单帖分析接口（`TweetActivityQuery`：展示量/总参与/详情展开/个人资料访问/涨粉/外链点击）
  与公开时间线互动指标计算平滑转化率，精准选出 72 小时内转化效果最好的帖子；
  若该帖当前尚未置顶，则自动执行置顶并回到主页二次闭环核验，确保主页始终有且仅有 1 条最优帖子处于置顶状态。
- 【数据与隔离】：
  只读旁路嗅探 + 持久化独立集合 `x_pin_snapshots`（每帖最新转化快照）与 `x_pin_history`（置顶决策与切换台账），
  绝不触碰任何历史旧表，并通过底层目录文件锁与发帖/互关/自动回复安全共存。
"""
import base64
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import json
import os
from pathlib import Path
import random
import re
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple
from urllib.parse import unquote

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.common_utils import (
    clean_post_text,
    get_config,
    get_proxy_url,
    resolve_session_dir,
    safe_int,
    setup_logger,
)
from common.daemon import run_daemon_loop
from common.mongo_db.mongo_base import gen_db_object
from platforms.playwright_base import (
    UserDataDirLockTimeoutError,
    _dismiss_overlays,
    _robust_click,
    check_for_crash,
    persistent_browser_session,
)
from platforms.x_playwright import (
    _locate_focal_tweet,
    _wait_x_post_detail_ready,
    open_browser_for_manual_use,
)

# ==============================================================================
# 一、运维配置区（直接修改下方变量并重启即可生效，无需任何命令行参数）
# ==============================================================================

# 0. 运行模式开关（按需把 RUN_MODE 改成下面任一字符串即可）：
#   "daemon"   -> 【默认挂机】每 6 小时自动巡检一次：分析 72h 转化率并自动置顶最优主帖
#   "run_once" -> 【单次执行】立即分析并按需置顶 1 轮后打印看板退出
#   "preview"  -> 【只读预检】打开浏览器抓分析数据并算榜，但不执行点击置顶（验证排榜效果最安全）
#   "status"   -> 【只看报表】不开浏览器，仅从数据库打印当前置顶状态、72h 转化榜单与切换历史
#   "login"    -> 【人工登录】打开可见 Chrome 供手动登录并固化浏览器目录会话
RUN_MODE: str = "daemon"

# 1. 需要自动维护置顶帖的账号列表（核心必填项：user_data_dir 浏览器会话目录；默认使用 x_myself）
ACCOUNTS: List[Dict[str, Any]] = [
    {
        "name": "x_myself",               # 数据库分账号隔离标识
        "user_data_dir": resolve_session_dir("x_myself_browser_session_dir", "x_myself"),  # 已登录该 X 账号的浏览器数据目录
        "screen_name": str(get_config("x_myself_screen_name", "peterzhu1003") or "peterzhu1003").strip().lstrip("@"),  # 账号 @用户名（不带 @；可留空自动识别）
        "enabled": True,                  # True=启用该账号；False=临时停用
    },
    # 如需追加其他账号，按下方模板复制一段即可：
    # {
    #     "name": "x_account_2",
    #     "user_data_dir": r"W:\temp\x_account_2",
    #     "screen_name": "",
    #     "enabled": True,
    # },
]

# 2. 定时节奏、持锁上限与风控冷却（单位：秒；默认 21600 秒 = 每 6 小时运行一轮）
INTERVAL_SECONDS: int = 21600             # 轮询间隔（秒；21600=6 小时）
JITTER_SECONDS: Tuple[int, int] = (120, 360)  # 每轮额外随机抖动 (最小秒, 最大秒)，避开整点机械指纹
# 【保留过滤原因说明】`platforms/playwright_base.py` 跨进程目录锁超时为 300s；单次持锁上限设为 150s，给采集详情+置顶核验与同目录其他任务留足余量
SESSION_MAX_HOLD_SECONDS: int = 150       # 单次打开浏览器的最长持锁秒数（预留最后 30s 专用置顶与核验）
RISK_COOLDOWN_SECONDS: int = 21600        # 遇安全验证或登录失效时静默冷却秒数（21600=6 小时）
DRY_RUN: bool = False                     # True=只采集计算与入库，不真实点击置顶；False=真实置顶
BROWSER_VISIBLE: bool = False             # False=后台静默无弹窗；True=弹出 Chrome 窗口便于观察过程

# 3. 72 小时评估窗口、抓取范围与选优平滑参数
WINDOW_HOURS: int = 72                    # 仅评选最近多少小时内发布的本人原创/引用主帖（默认 72 小时）
# 【保留过滤原因说明】最多滚屏 6 次覆盖近 72 小时主帖（且扫到早于 72h 的非置顶帖即提前停滚），避免无意义翻看陈年旧帖引发风控
MAX_PROFILE_SCROLLS: int = 6              # 主页最多向下滚屏次数
FETCH_TWEET_ANALYTICS: bool = True        # True=逐条打开官方帖子分析页抓取《展示/参与/详情展开/个人资料访问/涨粉/外链点击》；False=仅凭公开互动量算分
# 【保留过滤原因说明】单轮最多采集 25 条 72h 主帖的官方分析页（按曝光从高到低优先），控制浏览页数防高频风控
MAX_ANALYTICS_POSTS_PER_RUN: int = 25     # 单轮最多访问多少条主帖的分析页
# 【保留过滤原因说明】优先在曝光量达到一定基数且发布满 30 分钟的帖子中评选冠军，防止刚发几分钟只有 1 个赞的小样本刷出虚高转化率；若全员不足则自动放宽兜底，确保必有 1 帖置顶
MIN_IMPRESSIONS: int = 20                 # 优先门槛：最低有效曝光量（全部不足时自动降级放宽）
MIN_POST_AGE_MINUTES: int = 30            # 优先门槛：发布满多少分钟后参与竞争（全部不足时自动降级放宽）
SMOOTHING_PRIOR_IMPRESSIONS: int = 30     # 贝叶斯平滑先验曝光基数（分母=有效曝光+30），有效抑制低曝光极小样本偶然偏高
MIN_SWITCH_IMPROVEMENT_RATIO: float = 0.0 # 切换迟滞阈值（0.0=只要严格高于当前置顶帖即切换；如设为 0.10 则需高出 10% 才换置顶）

# 4. 转化率口径与各行为权重（无需改逻辑，直接调权重或模式即可换口径）
#   "weighted_conversion" -> 【默认】综合加权转化率（强意向转化：涨粉>个人资料访问>外链点击>收藏/转推/引用>回复>点赞>总参与，再平滑除以曝光）
#   "profile_follow"      -> 高意向引流转化率（专注涨粉 + 个人资料访问 + 外链点击；若全为 0 自动回退综合加权转化率作并列决胜）
#   "engagement_rate"     -> 官方总参与率（官方 Engagements ÷ 平滑曝光）
#   "public_interaction"  -> 纯公开互动率（(赞+转+评+引+藏) ÷ 平滑曝光）
CONVERSION_METRIC_MODE: str = "weighted_conversion"
CONVERSION_WEIGHTS: Dict[str, float] = {
    "follows": 20.0,         # 带来新关注（最高转化价值）
    "profile_visits": 8.0,   # 点击进入个人主页（核心引流动作）
    "link_clicks": 4.0,      # 链接点击
    "bookmarks": 3.0,        # 收藏（高价值认可）
    "retweets": 3.0,         # 转推扩散
    "quotes": 3.0,           # 引用二次传播
    "replies": 2.5,          # 评论互动
    "likes": 1.5,            # 点赞
    "engagements": 1.0,      # 官方总参与兜底基线
}

# 5. 独立 MongoDB 集合名（物理隔离历史旧表与其他模块业务表）
PIN_SNAPSHOTS_COLLECTION: str = "x_pin_snapshots"  # 各帖最新转化指标快照表
PIN_HISTORY_COLLECTION: str = "x_pin_history"      # 每轮评选与置顶变更记录表

# ==============================================================================
# 二、基础工具、数据库索引与 X 置顶/分析嗅探器
# ==============================================================================

TZ_CN = timezone(timedelta(hours=8))
X_HOME_URL = "https://x.com/home"

_PROFILE_OPS = ("UserTweets", "UserOriginalsTimeline")
_USER_PROFILE_OPS = ("UserByScreenName", "UserOriginalsTimeline", "UserTweets")
_PIN_MUTATION_OPS = ("PinTweet", "CreatePin")

_RE_PIN_GQL = re.compile(
    r"/graphql/[^/]+/(UserByScreenName|UserOriginalsTimeline|UserTweets|TweetActivityQuery|TweetDetail|PinTweet|CreatePin|UnpinTweet)(?:\?|$)",
    re.IGNORECASE,
)
_RE_PIN_REST = re.compile(
    r"/i/api/1\.1/account/(pin_tweet|unpin_tweet)\.json(?:\?|$)",
    re.IGNORECASE,
)
_RE_VAR_TWEET_ID = re.compile(r'"(?:rest_id|tweetId|tweet_id)"\s*:\s*"(\d+)"')
_RE_B64_TWEET_ID = re.compile(r"(?:Tweet|TweetResults):(\d+)")
_RE_STATUS_ID = re.compile(r"/status/(\d+)")
_RE_PINNED_CONTEXT = re.compile(r"(置顶|Pinned|固定)", re.IGNORECASE)
_RE_PIN_MENU_TEXT = re.compile(r"(置顶到你的个人资料|置顶|Pin to your profile|Pin to profile)", re.IGNORECASE)
_RE_UNPIN_MENU_TEXT = re.compile(r"(从个人资料取消置顶|取消置顶|Unpin from profile|Unpin)", re.IGNORECASE)

_RISK_HALT_STATUSES = {"locked_challenge", "not_logged_in", "suspended", "cooldown_required"}
_INDEXED_DBS: Set[int] = set()
_LOGGER: Optional[Any] = None


def _get_logger() -> Any:
    global _LOGGER
    if _LOGGER is None:
        _LOGGER = setup_logger(app_name="x_growth_pinner")
    return _LOGGER


def _now_iso(now_ts: Optional[int] = None) -> str:
    dt = datetime.fromtimestamp(int(now_ts), tz=TZ_CN) if now_ts is not None else datetime.now(TZ_CN)
    return dt.isoformat(timespec="seconds")


def _clean_handle(handle: Any) -> str:
    return str(handle or "").strip().lstrip("@").split("/")[0].split("?")[0].strip()


def _parse_x_ts(raw: Any) -> int:
    if isinstance(raw, (int, float)):
        v = int(raw)
        return (v // 1000 if v > 10_000_000_000 else v) if v > 0 else 0
    s = str(raw or "").strip()
    if not s:
        return 0
    if s.isdigit():
        v = int(s)
        return v // 1000 if v > 10_000_000_000 else v
    try:
        dt = parsedate_to_datetime(s)
        return int((dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp())
    except Exception:
        return 0


def _clip_text(text: Any, max_len: int = 38) -> str:
    clean = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(clean) <= max_len:
        return clean
    return clean[: max(1, max_len - 1)] + "…"


def _default_cfg() -> Dict[str, Any]:
    return {
        "interval_seconds": INTERVAL_SECONDS,
        "jitter_seconds": JITTER_SECONDS,
        "session_max_hold_seconds": SESSION_MAX_HOLD_SECONDS,
        "risk_cooldown_seconds": RISK_COOLDOWN_SECONDS,
        "dry_run": DRY_RUN,
        "browser_visible": BROWSER_VISIBLE,
        "window_hours": WINDOW_HOURS,
        "max_profile_scrolls": MAX_PROFILE_SCROLLS,
        "fetch_tweet_analytics": FETCH_TWEET_ANALYTICS,
        "max_analytics_posts_per_run": MAX_ANALYTICS_POSTS_PER_RUN,
        "min_impressions": MIN_IMPRESSIONS,
        "min_post_age_minutes": MIN_POST_AGE_MINUTES,
        "smoothing_prior_impressions": SMOOTHING_PRIOR_IMPRESSIONS,
        "min_switch_improvement_ratio": MIN_SWITCH_IMPROVEMENT_RATIO,
        "conversion_metric_mode": CONVERSION_METRIC_MODE,
        "conversion_weights": dict(CONVERSION_WEIGHTS),
    }


def _ensure_indexes(db: Any) -> None:
    if db is None:
        return
    key = id(getattr(db, "db", db))
    if key in _INDEXED_DBS:
        return
    ci = getattr(db, "create_index", None)
    if callable(ci):
        ci(PIN_SNAPSHOTS_COLLECTION, [("account_name", 1), ("tweet_id", 1)], unique=True)
        ci(PIN_SNAPSHOTS_COLLECTION, [("account_name", 1), ("publish_ts", -1)], unique=False)
        ci(PIN_SNAPSHOTS_COLLECTION, [("account_name", 1), ("conversion_score", -1)], unique=False)
        ci(PIN_HISTORY_COLLECTION, [("account_name", 1), ("executed_at_ts", -1)], unique=False)
    _INDEXED_DBS.add(key)


def resolve_accounts(accounts: Optional[Sequence[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    """归一化已启用的账号配置（按浏览器会话目录去重）。"""
    resolved: List[Dict[str, Any]] = []
    seen_dirs: Set[str] = set()
    for item in (accounts if accounts is not None else ACCOUNTS):
        if not isinstance(item, dict) or item.get("enabled") is False:
            continue
        raw_dir = str(item.get("user_data_dir") or "").strip()
        if not raw_dir:
            continue
        session_path = str(Path(raw_dir).expanduser().resolve())
        norm_key = os.path.normcase(session_path)
        if norm_key in seen_dirs:
            continue
        seen_dirs.add(norm_key)
        resolved.append(
            {
                **item,
                "name": str(item.get("name") or Path(session_path).name).strip(),
                "screen_name": _clean_handle(item.get("screen_name")),
                "user_data_dir": session_path,
            }
        )
    return resolved


def inspect_x_account_status(page: Any) -> Tuple[str, Optional[str]]:
    """检查当前真实 Chrome 页面是否处于正常登录态，若遇验证或掉线立即阻断。"""
    cur_url = str(page.url or "").lower()
    if "/account/access" in cur_url or "arkoselabs" in cur_url:
        return "locked_challenge", f"当前账号触发 X 安全验证解锁页 ({page.url})，等待冷却后重试。"
    if "/i/flow/login" in cur_url or cur_url.rstrip("/").endswith("/login"):
        return "not_logged_in", f"当前浏览器会话未登录 X ({page.url})，请先执行人工登录。"

    for sel, st, msg in (
        ('iframe[src*="arkoselabs"]', "locked_challenge", "页面检测到人机验证组件"),
        ('form[action*="/account/access"]', "locked_challenge", "页面检测到安全验证表单"),
        ('input[autocomplete="username"]', "not_logged_in", "页面检测到未登录用户名输入框"),
        ('a[data-testid="loginButton"]', "not_logged_in", "页面检测到未登录按钮"),
    ):
        try:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible():
                return st, msg
        except Exception:
            continue
    return "ok", None


def extract_logged_in_identity(context: Any, page: Any) -> Tuple[str, str]:
    """从浏览器上下文提取当前登录账号 UID 与小写 @用户名。"""
    self_uid = ""
    self_handle = ""
    try:
        for ck in context.cookies(["https://x.com"]):
            if ck.get("name") == "twid":
                raw_twid = unquote(str(ck.get("value") or "")).strip().strip('"')
                self_uid = raw_twid[2:] if raw_twid.startswith("u=") else raw_twid
                break
    except Exception:
        pass

    try:
        prof_link = page.locator('a[data-testid="AppTabBar_Profile_Link"]').first
        if prof_link.count() > 0:
            href = str(prof_link.get_attribute("href") or "").strip().strip("/")
            if href and "/" not in href and href.lower() not in ("i", "home", "explore", "notifications", "messages", "search"):
                self_handle = href.lower()
    except Exception:
        pass

    return self_uid, self_handle


def _resolve_session_identity(context: Any, page: Any, fallback_handle: str) -> Tuple[str, str]:
    self_uid, detected_handle = extract_logged_in_identity(context, page)
    if not detected_handle and not fallback_handle:
        try:
            page.locator('a[data-testid="AppTabBar_Profile_Link"]').first.wait_for(
                state="attached", timeout=5000
            )
            self_uid, detected_handle = extract_logged_in_identity(context, page)
        except Exception:
            pass
    return self_uid, (detected_handle or fallback_handle)


def _unwrap_user_result(raw_user: Any) -> Dict[str, Any]:
    if not isinstance(raw_user, dict):
        return {}
    if raw_user.get("__typename") == "UserWithVisibilityResults" and isinstance(raw_user.get("user"), dict):
        return raw_user["user"]
    return raw_user


def _clean_x_text_with_entities(raw_text: str, entities: Dict[str, Any], ext_entities: Dict[str, Any]) -> str:
    txt = str(raw_text or "")
    for u_obj in (entities.get("urls") or []):
        if isinstance(u_obj, dict) and u_obj.get("url"):
            txt = txt.replace(u_obj["url"], u_obj.get("expanded_url") or "")
    for m_obj in (ext_entities.get("media") or []) + (entities.get("media") or []):
        if isinstance(m_obj, dict) and m_obj.get("url"):
            txt = txt.replace(m_obj["url"], "")
    return clean_post_text(txt)


def parse_gql_tweet_node(tw: Any) -> Optional[Dict[str, Any]]:
    """将 X GraphQL 推文节点规范化为包含互动/曝光字段的轻量字典。"""
    if not isinstance(tw, dict):
        return None
    if tw.get("__typename") == "TweetWithVisibilityResults" and isinstance(tw.get("tweet"), dict):
        tw = tw["tweet"]
    legacy = tw.get("legacy") or {}
    if not isinstance(legacy, dict) or not legacy:
        return None
    tweet_id = str(tw.get("rest_id") or legacy.get("id_str") or "").strip()
    if not tweet_id:
        return None

    user_node = _unwrap_user_result(((tw.get("core") or {}).get("user_results") or {}).get("result"))
    u_core = user_node.get("core") or {}
    u_leg = user_node.get("legacy") or {}
    author_id = str(user_node.get("rest_id") or legacy.get("user_id_str") or "").strip()
    author_handle = _clean_handle(u_core.get("screen_name") or u_leg.get("screen_name"))

    note_text = (((tw.get("note_tweet") or {}).get("note_tweet_results") or {}).get("result") or {}).get("text")
    raw_text = str(note_text or legacy.get("full_text") or legacy.get("text") or "")
    entities = legacy.get("entities") or {}
    ext_entities = legacy.get("extended_entities") or {}
    text = _clean_x_text_with_entities(raw_text, entities, ext_entities)

    views_obj = tw.get("views") or {}
    views_cnt = safe_int(views_obj.get("count") if isinstance(views_obj, dict) else views_obj, 0)
    media_list = ext_entities.get("media") or entities.get("media") or []

    return {
        "tweet_id": tweet_id,
        "author_id": author_id,
        "author_handle": author_handle,
        "author_handle_lower": author_handle.lower(),
        "text": text,
        "publish_ts": _parse_x_ts(legacy.get("created_at")),
        "views": views_cnt,
        "likes": safe_int(legacy.get("favorite_count"), 0),
        "retweets": safe_int(legacy.get("retweet_count"), 0),
        "replies": safe_int(legacy.get("reply_count"), 0),
        "quotes": safe_int(legacy.get("quote_count"), 0),
        "bookmarks": safe_int(legacy.get("bookmark_count"), 0),
        "in_reply_to_status_id": str(legacy.get("in_reply_to_status_id_str") or "").strip(),
        "is_retweet": bool(
            legacy.get("retweeted_status_result")
            or legacy.get("retweeted_status_id_str")
            or legacy.get("retweeted_status")
        ),
        "has_media": bool(media_list),
        "url": f"https://x.com/{author_handle}/status/{tweet_id}" if author_handle else f"https://x.com/i/status/{tweet_id}",
    }


def _walk_gql_tweet_nodes(payload: Any) -> List[Dict[str, Any]]:
    """递归提取 GraphQL JSON 中的全部 Tweet 节点（跳过推广广告与引用嵌套内部）。"""
    found: List[Dict[str, Any]] = []

    def _walk(obj: Any) -> None:
        if isinstance(obj, dict):
            if "promotedMetadata" in obj:
                return
            tn = obj.get("__typename")
            if tn == "TweetWithVisibilityResults" and isinstance(obj.get("tweet"), dict):
                _walk(obj["tweet"])
                return
            if isinstance(obj.get("legacy"), dict) and (obj.get("rest_id") or obj["legacy"].get("id_str")) and (
                tn == "Tweet" or isinstance(obj.get("core"), dict)
            ):
                found.append(obj)
                return
            for v in obj.values():
                _walk(v)
        elif isinstance(obj, list):
            for v in obj:
                _walk(v)

    _walk(payload)
    return found


def _extract_first_digit_id(obj: Any) -> Optional[str]:
    if isinstance(obj, str) and obj.strip().isdigit():
        return obj.strip()
    if isinstance(obj, list):
        for item in obj:
            hit = _extract_first_digit_id(item)
            if hit:
                return hit
    elif isinstance(obj, dict):
        for k in ("rest_id", "id_str", "tweet_id"):
            val = str(obj.get(k) or "").strip()
            if val.isdigit():
                return val
        for v in obj.values():
            hit = _extract_first_digit_id(v)
            if hit:
                return hit
    return None


def _extract_pinned_id_from_profile_payload(op: str, payload: Any) -> Optional[str]:
    """从 `UserByScreenName` 或时间线 `TimelinePinEntry` 中提取当前唯一置顶推文 ID。"""
    if not isinstance(payload, dict):
        return None
    if op == "UserByScreenName":
        u_res = _unwrap_user_result(((payload.get("data") or {}).get("user") or {}).get("result"))
        legacy_pins = (u_res.get("legacy") or {}).get("pinned_tweet_ids_str")
        hit = _extract_first_digit_id(legacy_pins)
        if hit:
            return hit
        p_items = u_res.get("pinned_items")
        if p_items:
            t_nodes = _walk_gql_tweet_nodes(p_items)
            if t_nodes:
                parsed = parse_gql_tweet_node(t_nodes[0])
                if parsed:
                    return parsed["tweet_id"]
            if isinstance(p_items, list):
                hit2 = _extract_first_digit_id(p_items)
                if hit2:
                    return hit2
            elif isinstance(p_items, dict):
                for key in ("tweet_ids", "pinned_tweet_ids", "items"):
                    if key in p_items:
                        hit2 = _extract_first_digit_id(p_items.get(key))
                        if hit2:
                            return hit2
        return None

    # TimelinePinEntry 或 entryId="pinned-..." 的节点
    pin_hits: List[str] = []

    def _walk_pins(obj: Any) -> None:
        if pin_hits:
            return
        if isinstance(obj, dict):
            if (
                obj.get("type") == "TimelinePinEntry"
                or obj.get("__typename") == "TimelinePinEntry"
                or str(obj.get("entryId") or "").lower().startswith("pinned")
            ):
                nodes = _walk_gql_tweet_nodes(obj)
                if nodes:
                    parsed = parse_gql_tweet_node(nodes[0])
                    if parsed:
                        pin_hits.append(parsed["tweet_id"])
                        return
                hit = _extract_first_digit_id(obj)
                if hit:
                    pin_hits.append(hit)
                    return
            for v in obj.values():
                _walk_pins(v)
        elif isinstance(obj, list):
            for v in obj:
                _walk_pins(v)

    _walk_pins(payload)
    return pin_hits[0] if pin_hits else None


def _decode_b64_tweet_id(encoded: Any) -> Optional[str]:
    raw = str(encoded or "").strip()
    if not raw:
        return None
    try:
        padded = raw + "=" * (-len(raw) % 4)
        decoded = base64.b64decode(padded).decode("utf-8", errors="ignore")
        m = _RE_B64_TWEET_ID.search(decoded)
        if m:
            return m.group(1)
    except Exception:
        pass
    return None


def _parse_tweet_activity_payload(
    payload: Any,
    *,
    resp_url: str = "",
    fallback_tweet_id: str = "",
) -> Tuple[Optional[str], Optional[Dict[str, int]]]:
    """解析 `TweetActivityQuery` 官方转化指标（展示/总参与/详情展开/主页访问/涨粉/外链点击）。"""
    if not isinstance(payload, dict):
        return None, None
    data = payload.get("data") or {}
    t_holder = data.get("tweet_result_by_rest_id") or data.get("tweetResult") or {}
    t_res = (t_holder.get("result") or {}) if isinstance(t_holder, dict) else {}
    if t_res.get("__typename") == "TweetWithVisibilityResults" and isinstance(t_res.get("tweet"), dict):
        t_res = t_res["tweet"]
    if not isinstance(t_res, dict):
        return None, None

    grid = t_res.get("datapoints_grid")
    if not isinstance(grid, list):
        return None, None

    tid = (
        str(t_res.get("rest_id") or "").strip()
        or _decode_b64_tweet_id(t_res.get("id"))
        or _decode_b64_tweet_id(t_holder.get("id") if isinstance(t_holder, dict) else "")
    )
    if not tid and resp_url:
        m_var = _RE_VAR_TWEET_ID.search(unquote(resp_url))
        if m_var:
            tid = m_var.group(1)
    if not tid:
        tid = str(fallback_tweet_id or "").strip()
    if not tid:
        return None, None

    metric_map = {
        "Impressions": "impressions",
        "Engagements": "engagements",
        "DetailExpands": "detail_expands",
        "ProfileVisits": "profile_visits",
        "Follows": "follows",
        "LinkClicks": "link_clicks",
    }
    parsed: Dict[str, int] = {v: 0 for v in metric_map.values()}
    for entry in grid:
        if not isinstance(entry, dict):
            continue
        m_type = str(entry.get("metric_type") or "").strip()
        dst_key = metric_map.get(m_type)
        if dst_key:
            parsed[dst_key] = max(0, safe_int(entry.get("metric_value"), 0))
    return tid, parsed


def detect_pinned_tweet_id_from_dom(page: Any, *, self_handle_lower: str = "") -> Optional[str]:
    """个人主页页面兜底检测：读取首条推文的置顶标识并提取其推文 ID。"""
    try:
        first_art = page.locator('article[data-testid="tweet"]').first
        if first_art.count() == 0:
            return None
        ctx_loc = first_art.locator('[data-testid="socialContext"]').first
        if ctx_loc.count() == 0:
            return None
        ctx_txt = str(ctx_loc.inner_text() or "").strip()
        if not _RE_PINNED_CONTEXT.search(ctx_txt):
            return None
        links = first_art.locator('a[href*="/status/"]').all()
        for link in links:
            href = str(link.get_attribute("href") or "")
            if "/analytics" in href or "/photo/" in href:
                continue
            m = _RE_STATUS_ID.search(href)
            if m:
                if self_handle_lower and f"/{self_handle_lower}/status/" in href.lower():
                    return m.group(1)
        for link in links:
            href = str(link.get_attribute("href") or "")
            m = _RE_STATUS_ID.search(href)
            if m:
                return m.group(1)
    except Exception:
        pass
    return None


def _is_same_account(tweet: Dict[str, Any], *, self_handle_lower: str, self_uid: str = "") -> bool:
    th = str(tweet.get("author_handle_lower") or "").strip().lower()
    tuid = str(tweet.get("author_id") or "").strip()
    if self_handle_lower and th and th == self_handle_lower:
        return True
    if self_uid and tuid and tuid == self_uid:
        return True
    return False


class XPinAnalyticsSniffer:
    """只读旁路嗅探器：提取个人主页主帖、当前置顶 ID、`TweetActivityQuery` 转化数据与置顶接口回执。"""

    def __init__(self) -> None:
        self.op_counts: Dict[str, int] = {}
        self.own_posts_by_id: Dict[str, Dict[str, Any]] = {}
        self.analytics_by_id: Dict[str, Dict[str, int]] = {}
        self.pinned_tweet_id_from_api: Optional[str] = None
        self.current_analytics_tweet_id: str = ""
        self.pin_api_ok_count: int = 0

    def _register_own_post(self, tw: Dict[str, Any]) -> None:
        tid = tw["tweet_id"]
        prev = self.own_posts_by_id.get(tid)
        if prev is None:
            self.own_posts_by_id[tid] = tw
            return
        merged = dict(tw if len(tw.get("text") or "") >= len(prev.get("text") or "") else prev)
        for k in ("views", "likes", "retweets", "replies", "quotes", "bookmarks"):
            merged[k] = max(safe_int(prev.get(k), 0), safe_int(tw.get(k), 0))
        self.own_posts_by_id[tid] = merged

    def ingest_payload(self, op: str, payload: Any, *, resp_url: str = "") -> None:
        op_low = op.lower()
        if op_low in ("pintweet", "createpin", "pin_tweet"):
            self.pin_api_ok_count += 1
            return

        if not isinstance(payload, dict):
            return

        if op == "TweetActivityQuery":
            tid, metrics = _parse_tweet_activity_payload(
                payload,
                resp_url=resp_url,
                fallback_tweet_id=self.current_analytics_tweet_id,
            )
            if tid and metrics:
                prev_m = self.analytics_by_id.get(tid) or {}
                self.analytics_by_id[tid] = {
                    k: max(safe_int(prev_m.get(k), 0), safe_int(metrics.get(k), 0))
                    for k in metrics
                }
            return

        if op in ("UserByScreenName", "UserOriginalsTimeline", "UserTweets"):
            pin_id = _extract_pinned_id_from_profile_payload(op, payload)
            if pin_id:
                self.pinned_tweet_id_from_api = pin_id

        if op in ("UserOriginalsTimeline", "UserTweets", "TweetDetail"):
            for node in _walk_gql_tweet_nodes(payload):
                p = parse_gql_tweet_node(node)
                if p and (not p["is_retweet"]) and (not p["in_reply_to_status_id"]):
                    self._register_own_post(p)

    def on_response(self, resp: Any) -> None:
        try:
            url = resp.url or ""
            if resp.status != 200:
                return
            m_gql = _RE_PIN_GQL.search(url)
            if m_gql:
                op = m_gql.group(1)
                if op.lower() in ("pintweet", "createpin"):
                    self.ingest_payload(op, {}, resp_url=url)
                else:
                    self.ingest_payload(op, resp.json(), resp_url=url)
                self.op_counts[op] = self.op_counts.get(op, 0) + 1
                return
            m_rest = _RE_PIN_REST.search(url)
            if m_rest:
                op_rest = m_rest.group(1)
                self.ingest_payload(op_rest, {}, resp_url=url)
                self.op_counts[op_rest] = self.op_counts.get(op_rest, 0) + 1
        except Exception:
            pass

    def wait_for_ops(self, page: Any, ops: Sequence[str], prev_total: int, timeout_ms: int = 12000) -> bool:
        deadline = time.time() + timeout_ms / 1000.0
        while time.time() < deadline:
            if sum(self.op_counts.get(op, 0) for op in ops) > prev_total:
                return True
            page.wait_for_timeout(250)
        return False

    def wait_for_analytics(self, page: Any, tweet_id: str, timeout_ms: int = 8000) -> bool:
        deadline = time.time() + timeout_ms / 1000.0
        while time.time() < deadline:
            if tweet_id in self.analytics_by_id:
                return True
            page.wait_for_timeout(250)
        return tweet_id in self.analytics_by_id


# ==============================================================================
# 三、72 小时转化率计算与最优置顶评选引擎
# ==============================================================================

def compute_post_conversion_metrics(
    post: Dict[str, Any],
    analytics: Optional[Dict[str, int]] = None,
    *,
    metric_mode: str = CONVERSION_METRIC_MODE,
    weights: Optional[Dict[str, float]] = None,
    prior_impressions: int = SMOOTHING_PRIOR_IMPRESSIONS,
) -> Dict[str, Any]:
    """结合时间线公开互动量与 `TweetActivityQuery` 官方分析指标，计算平滑转化率与综合转化分。"""
    w = {**CONVERSION_WEIGHTS, **(weights or {})}
    act = dict(analytics or {})
    analytics_fetched = analytics is not None and bool(analytics)

    views = max(0, safe_int(post.get("views"), 0))
    impressions = max(0, safe_int(act.get("impressions"), 0))
    eff_impr = max(views, impressions)

    likes = max(0, safe_int(post.get("likes"), 0))
    retweets = max(0, safe_int(post.get("retweets"), 0))
    replies = max(0, safe_int(post.get("replies"), 0))
    quotes = max(0, safe_int(post.get("quotes"), 0))
    bookmarks = max(0, safe_int(post.get("bookmarks"), 0))
    public_interactions = likes + retweets + replies + quotes + bookmarks

    follows = max(0, safe_int(act.get("follows"), 0))
    profile_visits = max(0, safe_int(act.get("profile_visits"), 0))
    link_clicks = max(0, safe_int(act.get("link_clicks"), 0))
    detail_expands = max(0, safe_int(act.get("detail_expands"), 0))
    raw_engagements = max(0, safe_int(act.get("engagements"), 0))
    eff_engagements = max(raw_engagements, public_interactions) if analytics_fetched else public_interactions

    denom = max(1, eff_impr + max(0, safe_int(prior_impressions, 0)))

    weighted_num = (
        float(w.get("follows", 20.0)) * follows
        + float(w.get("profile_visits", 8.0)) * profile_visits
        + float(w.get("link_clicks", 4.0)) * link_clicks
        + float(w.get("bookmarks", 3.0)) * bookmarks
        + float(w.get("retweets", 3.0)) * retweets
        + float(w.get("quotes", 3.0)) * quotes
        + float(w.get("replies", 2.5)) * replies
        + float(w.get("likes", 1.5)) * likes
        + float(w.get("engagements", 1.0)) * eff_engagements
    )
    weighted_score = round((weighted_num / denom) * 100.0, 3) if (eff_impr > 0 or weighted_num > 0) else 0.0

    profile_follow_num = (
        float(w.get("follows", 20.0)) * follows
        + float(w.get("profile_visits", 8.0)) * profile_visits
        + float(w.get("link_clicks", 4.0)) * link_clicks
    )
    profile_follow_score = round((profile_follow_num / denom) * 100.0, 3) if (eff_impr > 0 or profile_follow_num > 0) else 0.0

    mode_norm = str(metric_mode or "weighted_conversion").strip().lower()
    if mode_norm == "profile_follow":
        primary_score = profile_follow_score
    elif mode_norm == "engagement_rate":
        primary_score = round((eff_engagements / denom) * 100.0, 3) if (eff_impr > 0 or eff_engagements > 0) else 0.0
    elif mode_norm == "public_interaction":
        primary_score = round((public_interactions / denom) * 100.0, 3) if (eff_impr > 0 or public_interactions > 0) else 0.0
    else:
        mode_norm = "weighted_conversion"
        primary_score = weighted_score

    return {
        **post,
        "analytics_fetched": analytics_fetched,
        "impressions": impressions,
        "effective_impressions": eff_impr,
        "engagements": eff_engagements,
        "detail_expands": detail_expands,
        "profile_visits": profile_visits,
        "follows": follows,
        "link_clicks": link_clicks,
        "public_interactions": public_interactions,
        "engagement_rate_pct": round((eff_engagements / eff_impr) * 100.0, 2) if eff_impr > 0 else 0.0,
        "profile_visit_rate_pct": round((profile_visits / eff_impr) * 100.0, 2) if eff_impr > 0 else 0.0,
        "follow_rate_pct": round((follows / eff_impr) * 100.0, 2) if eff_impr > 0 else 0.0,
        "public_interaction_rate_pct": round((public_interactions / eff_impr) * 100.0, 2) if eff_impr > 0 else 0.0,
        "weighted_conversion_score": weighted_score,
        "profile_follow_score": profile_follow_score,
        "conversion_score": primary_score,
        "conversion_mode": mode_norm,
    }


def _rank_key(item: Dict[str, Any]) -> Tuple[Any, ...]:
    return (
        float(item.get("conversion_score") or 0.0),
        float(item.get("weighted_conversion_score") or 0.0),
        safe_int(item.get("follows"), 0),
        safe_int(item.get("profile_visits"), 0),
        safe_int(item.get("engagements"), 0),
        safe_int(item.get("effective_impressions"), 0),
        safe_int(item.get("publish_ts"), 0),
        str(item.get("tweet_id") or ""),
    )


def select_best_conversion_post(
    all_posts: Sequence[Dict[str, Any]],
    *,
    analytics_by_id: Optional[Dict[str, Dict[str, int]]] = None,
    current_pinned_id: Optional[str] = None,
    now_ts: Optional[int] = None,
    window_hours: int = WINDOW_HOURS,
    min_impressions: int = MIN_IMPRESSIONS,
    min_post_age_minutes: int = MIN_POST_AGE_MINUTES,
    metric_mode: str = CONVERSION_METRIC_MODE,
    weights: Optional[Dict[str, float]] = None,
    prior_impressions: int = SMOOTHING_PRIOR_IMPRESSIONS,
    min_switch_improvement_ratio: float = MIN_SWITCH_IMPROVEMENT_RATIO,
) -> Dict[str, Any]:
    """从 72 小时主帖池中选出转化率最高且满足稳定阈值（带降级兜底）的唯一最优置顶候选。"""
    now = int(now_ts if now_ts is not None else time.time())
    win_sec = max(3600, int(window_hours) * 3600)
    min_age_sec = max(0, int(min_post_age_minutes) * 60)
    cur_pin = str(current_pinned_id or "").strip() or None
    a_map = analytics_by_id or {}

    enriched_all = [
        compute_post_conversion_metrics(
            p,
            a_map.get(p["tweet_id"]),
            metric_mode=metric_mode,
            weights=weights,
            prior_impressions=prior_impressions,
        )
        for p in all_posts
        if isinstance(p, dict) and p.get("tweet_id")
    ]

    window_posts = [
        p for p in enriched_all
        if safe_int(p.get("publish_ts"), 0) > 0 and 0 <= (now - safe_int(p.get("publish_ts"), 0)) <= win_sec
    ]

    fallback_reason = ""
    pool = window_posts
    if not pool:
        if cur_pin:
            return {
                "decision": "keep_existing_no_window_posts",
                "winner": None,
                "ranked": [],
                "window_count": 0,
                "eligible_count": 0,
                "fallback_reason": "no_window_posts_keep_current_pin",
            }
        if not enriched_all:
            return {
                "decision": "no_posts_found",
                "winner": None,
                "ranked": [],
                "window_count": 0,
                "eligible_count": 0,
                "fallback_reason": "empty_profile",
            }
        pool = enriched_all
        fallback_reason = "no_72h_posts_fallback_all"

    mature_pool = [p for p in pool if (now - safe_int(p.get("publish_ts"), 0)) >= min_age_sec] if min_age_sec > 0 else list(pool)
    if not mature_pool:
        mature_pool = list(pool)
        fallback_reason = fallback_reason or "below_min_age_fallback"

    impr_pool = [p for p in mature_pool if safe_int(p.get("effective_impressions"), 0) >= int(min_impressions)]
    if not impr_pool:
        impr_pool = list(mature_pool)
        fallback_reason = fallback_reason or "below_min_impressions_fallback"

    ranked = sorted(impr_pool, key=_rank_key, reverse=True)
    winner = ranked[0]

    # 若启用迟滞切换门槛，当前置顶帖也在 72h 窗口内且未被拉开足够差距，则维持原置顶减少抖动
    if cur_pin and float(min_switch_improvement_ratio or 0.0) > 0.0 and winner["tweet_id"] != cur_pin:
        cur_item = next((p for p in ranked if p["tweet_id"] == cur_pin), None)
        if cur_item is not None:
            cur_score = float(cur_item.get("conversion_score") or 0.0)
            win_score = float(winner.get("conversion_score") or 0.0)
            threshold = cur_score * (1.0 + float(min_switch_improvement_ratio))
            if cur_score > 0 and win_score < threshold:
                winner = cur_item
                fallback_reason = "within_hysteresis_margin"

    return {
        "decision": "already_pinned" if cur_pin == winner["tweet_id"] else ("replace_pin" if cur_pin else "pin_new"),
        "winner": winner,
        "ranked": ranked,
        "window_count": len(window_posts),
        "eligible_count": len(ranked),
        "fallback_reason": fallback_reason,
    }


# ==============================================================================
# 四、浏览器端置顶动作与闭环核验 (`_pin_tweet_and_verify_on_page` + `sync_best_pinned_post_for_account`)
# ==============================================================================

def _pin_tweet_and_verify_on_page(
    page: Any,
    sniffer: XPinAnalyticsSniffer,
    *,
    target_id: str,
    eff_handle: str,
    logger: Any,
) -> Tuple[str, Optional[str], Optional[str]]:
    """
    打开目标帖子详情页 -> 点击菜单「置顶到你的个人资料」 -> 二次确认 -> 回到主页双途径核验有且仅置顶该帖。
    返回 `(action_status, verified_pinned_id, warning)`。
    """
    target_url = f"https://x.com/{eff_handle}/status/{target_id}"
    page.goto(target_url, timeout=45000, wait_until="domcontentloaded")
    _wait_x_post_detail_ready(page, timeout_ms=15000)
    page.wait_for_timeout(random.randint(1200, 2000))
    _dismiss_overlays(page, aggressive=False, desc="x-pinner-detail")
    check_for_crash(page)

    st, msg = inspect_x_account_status(page)
    if st != "ok":
        return st, None, msg

    focal = _locate_focal_tweet(page, post_id=target_id)
    caret_btn = focal.locator('button[data-testid="caret"]').first
    if caret_btn.count() == 0:
        return "pin_failed", None, f"未能定位目标帖 {target_id} 右上角更多菜单按钮 (caret)。"

    _robust_click(caret_btn)
    page.wait_for_timeout(900)

    # 先检查是否已经处于置顶态（菜单显示「取消置顶」）
    unpin_item = page.locator('div[role="menuitem"][data-testid="unpin"]').first
    if unpin_item.count() == 0:
        unpin_item = page.locator('div[role="menuitem"]').filter(has_text=_RE_UNPIN_MENU_TEXT).first
    if unpin_item.count() > 0 and unpin_item.is_visible():
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass
        return "already_pinned", target_id, None

    pin_item = page.locator('div[role="menuitem"][data-testid="pin"]').first
    if pin_item.count() == 0:
        pin_item = page.locator('div[role="menuitem"]').filter(has_text=_RE_PIN_MENU_TEXT).first
    if pin_item.count() == 0 or not pin_item.is_visible():
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass
        return "pin_failed", None, f"帖子 {target_id} 菜单中未找到『置顶到你的个人资料』按钮。"

    _robust_click(pin_item)
    confirm_btn = page.locator('button[data-testid="confirmationSheetConfirm"]').first
    try:
        confirm_btn.wait_for(state="visible", timeout=6000)
    except Exception:
        return "pin_failed", None, f"点击置顶后未弹出确认框 (confirmationSheetConfirm)。"

    prev_pin_ok = sniffer.pin_api_ok_count
    _robust_click(confirm_btn)

    deadline = time.time() + 8.0
    while time.time() < deadline:
        if sniffer.pin_api_ok_count > prev_pin_ok:
            break
        page.wait_for_timeout(250)
    page.wait_for_timeout(1200)

    # 闭环核验：重新打开个人主页确认顶部有且仅有该帖置顶（缓存滞后时多等并重载 1 次）
    verified_id: Optional[str] = None
    for attempt in range(2):
        sniffer.pinned_tweet_id_from_api = None
        prev_u = sum(sniffer.op_counts.get(op_k, 0) for op_k in _USER_PROFILE_OPS)
        page.goto(f"https://x.com/{eff_handle}", timeout=45000, wait_until="domcontentloaded")
        sniffer.wait_for_ops(page, _USER_PROFILE_OPS, prev_u, timeout_ms=10000)
        page.wait_for_timeout(1800 if attempt == 0 else 2800)
        _dismiss_overlays(page, aggressive=False, desc="x-pinner-verify")
        verified_id = sniffer.pinned_tweet_id_from_api or detect_pinned_tweet_id_from_dom(
            page, self_handle_lower=eff_handle
        )
        if verified_id == target_id:
            break
        if attempt == 0:
            logger.debug(f"[自动置顶/核验等待] 主页首次读取到置顶={verified_id}，等待缓存同步后复查一次...")

    if verified_id == target_id:
        return "pinned", verified_id, None
    if sniffer.pin_api_ok_count > prev_pin_ok and verified_id is None:
        # 接口已确认 HTTP 200 成功且主页列表刷新存在延迟时仍做显式提醒并记录接口确认
        return "pinned", target_id, "Pin 接口已返回成功，主页列表可能存在短时缓存延迟。"
    return (
        "pin_unverified",
        verified_id,
        f"已确认提交置顶 {target_id}，但复核主页当前置顶为 {verified_id}（pin_api_ok={sniffer.pin_api_ok_count > prev_pin_ok}）。",
    )


def _persist_snapshots_and_history(
    db_obj: Any,
    *,
    acc_name: str,
    eff_handle: str,
    now_ts: int,
    ranked_posts: Sequence[Dict[str, Any]],
    winner: Optional[Dict[str, Any]],
    prev_pinned_id: Optional[str],
    verified_pinned_id: Optional[str],
    action_status: str,
    metric_mode: str,
    fallback_reason: str,
    dry_run: bool,
) -> None:
    snap_col = db_obj.get_collection(PIN_SNAPSHOTS_COLLECTION)
    hist_col = db_obj.get_collection(PIN_HISTORY_COLLECTION)
    now_s = _now_iso(now_ts)

    # 先清理同一账号此前标记的旧置顶状态，保证快照表与看板中永远至多只有 1 条 is_currently_pinned=True
    clear_fn = getattr(snap_col, "update_many", None)
    if callable(clear_fn):
        clear_filter: Dict[str, Any] = {"account_name": acc_name, "is_currently_pinned": True}
        if verified_pinned_id:
            clear_filter["tweet_id"] = {"$ne": verified_pinned_id}
        clear_fn(clear_filter, {"$set": {"is_currently_pinned": False, "updated_at": now_s, "updated_at_ts": now_ts}})

    for rank_idx, p in enumerate(ranked_posts, start=1):
        tid = p["tweet_id"]
        is_pinned_now = bool(verified_pinned_id and tid == verified_pinned_id)
        snap_doc = {
            "account_name": acc_name,
            "account_handle": eff_handle,
            "tweet_id": tid,
            "url": p["url"],
            "text": p["text"],
            "publish_ts": safe_int(p.get("publish_ts"), 0),
            "publish_at": _now_iso(p["publish_ts"]) if p.get("publish_ts") else None,
            "has_media": bool(p.get("has_media")),
            "analytics_fetched": bool(p.get("analytics_fetched")),
            "views": safe_int(p.get("views"), 0),
            "impressions": safe_int(p.get("impressions"), 0),
            "effective_impressions": safe_int(p.get("effective_impressions"), 0),
            "engagements": safe_int(p.get("engagements"), 0),
            "detail_expands": safe_int(p.get("detail_expands"), 0),
            "profile_visits": safe_int(p.get("profile_visits"), 0),
            "follows": safe_int(p.get("follows"), 0),
            "link_clicks": safe_int(p.get("link_clicks"), 0),
            "likes": safe_int(p.get("likes"), 0),
            "retweets": safe_int(p.get("retweets"), 0),
            "replies": safe_int(p.get("replies"), 0),
            "quotes": safe_int(p.get("quotes"), 0),
            "bookmarks": safe_int(p.get("bookmarks"), 0),
            "public_interactions": safe_int(p.get("public_interactions"), 0),
            "engagement_rate_pct": float(p.get("engagement_rate_pct") or 0.0),
            "profile_visit_rate_pct": float(p.get("profile_visit_rate_pct") or 0.0),
            "follow_rate_pct": float(p.get("follow_rate_pct") or 0.0),
            "public_interaction_rate_pct": float(p.get("public_interaction_rate_pct") or 0.0),
            "conversion_score": float(p.get("conversion_score") or 0.0),
            "conversion_mode": str(p.get("conversion_mode") or metric_mode),
            "rank_in_last_run": rank_idx,
            "is_currently_pinned": is_pinned_now,
            "updated_at": now_s,
            "updated_at_ts": now_ts,
        }
        snap_col.update_one({"account_name": acc_name, "tweet_id": tid}, {"$set": snap_doc}, upsert=True)

    hist_doc = {
        "account_name": acc_name,
        "account_handle": eff_handle,
        "executed_at": now_s,
        "executed_at_ts": now_ts,
        "dry_run": bool(dry_run),
        "status": action_status,
        "conversion_mode": metric_mode,
        "fallback_reason": fallback_reason,
        "previous_pinned_id": prev_pinned_id,
        "winner_tweet_id": (winner["tweet_id"] if winner else None),
        "winner_score": (float(winner["conversion_score"]) if winner else 0.0),
        "winner_effective_impressions": (safe_int(winner.get("effective_impressions"), 0) if winner else 0),
        "winner_engagements": (safe_int(winner.get("engagements"), 0) if winner else 0),
        "winner_profile_visits": (safe_int(winner.get("profile_visits"), 0) if winner else 0),
        "winner_follows": (safe_int(winner.get("follows"), 0) if winner else 0),
        "winner_text": (_clip_text(winner.get("text"), 80) if winner else ""),
        "verified_pinned_id": verified_pinned_id,
        "candidates_evaluated": len(ranked_posts),
    }
    hist_col.insert_one(hist_doc)


def sync_best_pinned_post_for_account(
    account: Dict[str, Any],
    *,
    db: Any = None,
    dry_run: Optional[bool] = None,
    config: Optional[Dict[str, Any]] = None,
    logger: Any = None,
) -> Dict[str, Any]:
    """【主控函数】分析指定账号 72h 内转化率最高主帖并闭环维护唯一置顶。"""
    log = logger or _get_logger()
    cfg = {**_default_cfg(), **(config or {})}
    eff_dry = bool(cfg.get("dry_run", DRY_RUN) if dry_run is None else dry_run)
    db_obj = db if db is not None else gen_db_object()
    _ensure_indexes(db_obj)

    acc_name = str(account["name"])
    acc_handle = _clean_handle(account.get("screen_name")).lower()
    session_path = str(Path(str(account["user_data_dir"])).expanduser().resolve())

    if not os.path.isdir(session_path):
        warn = f"浏览器会话目录不存在: {session_path}"
        log.error(f"🚨 [自动置顶/目录缺失 | {acc_name}] {warn}")
        return {"account_name": acc_name, "status": "session_missing", "warning": warn, "browser_opened": False}

    proxy_server = get_proxy_url()
    sniffer = XPinAnalyticsSniffer()
    hold_limit = max(60, safe_int(cfg.get("session_max_hold_seconds"), SESSION_MAX_HOLD_SECONDS))
    win_hours = max(1, safe_int(cfg.get("window_hours"), WINDOW_HOURS))
    max_scrolls = max(0, safe_int(cfg.get("max_profile_scrolls"), MAX_PROFILE_SCROLLS))
    fetch_analytics = bool(cfg.get("fetch_tweet_analytics", FETCH_TWEET_ANALYTICS))
    max_analytics_n = max(1, safe_int(cfg.get("max_analytics_posts_per_run"), MAX_ANALYTICS_POSTS_PER_RUN))
    min_impr = max(0, safe_int(cfg.get("min_impressions"), MIN_IMPRESSIONS))
    min_age_m = max(0, safe_int(cfg.get("min_post_age_minutes"), MIN_POST_AGE_MINUTES))
    prior_impr = max(0, safe_int(cfg.get("smoothing_prior_impressions"), SMOOTHING_PRIOR_IMPRESSIONS))
    hysteresis = max(0.0, float(cfg.get("min_switch_improvement_ratio") or 0.0))
    metric_mode = str(cfg.get("conversion_metric_mode") or CONVERSION_METRIC_MODE).strip() or "weighted_conversion"
    weights = dict(cfg.get("conversion_weights") or CONVERSION_WEIGHTS)

    try:
        with persistent_browser_session(
            session_path,
            debug=bool(cfg.get("browser_visible", BROWSER_VISIBLE)),
            proxy=proxy_server,
            use_stealth_chrome=True,
            reuse_existing_page=True,
            default_timeout_ms=45000,
            navigation_timeout_ms=45000,
            install_guard=True,
        ) as (context, page):
            lock_t0 = time.monotonic()
            page.on("response", sniffer.on_response)
            try:
                # 1. 打开个人主页，识别身份、当前置顶ID与最近主帖列表
                entry_url = f"https://x.com/{acc_handle}" if acc_handle else X_HOME_URL
                page.goto(entry_url, timeout=45000, wait_until="domcontentloaded")
                if not acc_handle:
                    page.wait_for_timeout(1500)
                st0, msg0 = inspect_x_account_status(page)
                if st0 != "ok":
                    log.error(f"🚨 [自动置顶/账号阻断 | {acc_name}] status={st0} | {msg0}")
                    return {"account_name": acc_name, "status": st0, "warning": msg0, "browser_opened": True}

                self_uid, eff_handle = _resolve_session_identity(context, page, acc_handle)
                if not eff_handle:
                    warn = f"未能识别浏览器登录账号用户名 ({session_path})，请确认已登录。"
                    log.error(f"🚨 [自动置顶/身份缺失 | {acc_name}] {warn}")
                    return {"account_name": acc_name, "status": "not_logged_in", "warning": warn, "browser_opened": True}

                if not acc_handle:
                    prev_u0 = sum(sniffer.op_counts.get(op_k, 0) for op_k in _USER_PROFILE_OPS)
                    page.goto(f"https://x.com/{eff_handle}", timeout=45000, wait_until="domcontentloaded")
                    sniffer.wait_for_ops(page, _USER_PROFILE_OPS, prev_u0, timeout_ms=12000)
                else:
                    sniffer.wait_for_ops(page, _USER_PROFILE_OPS, 0, timeout_ms=12000)

                page.wait_for_timeout(random.randint(1500, 2400))
                _dismiss_overlays(page, aggressive=False, desc="x-pinner-profile")
                check_for_crash(page)

                st_prof, msg_prof = inspect_x_account_status(page)
                if st_prof != "ok":
                    log.error(f"🚨 [自动置顶/主页阻断 | {acc_name}] status={st_prof} | {msg_prof}")
                    return {"account_name": acc_name, "status": st_prof, "warning": msg_prof, "browser_opened": True}

                # 显式空采防护：未捕获任何主页接口响应则重载 1 次，仍无响应拒绝静默伪装 0 帖
                if sum(sniffer.op_counts.get(k, 0) for k in _USER_PROFILE_OPS) == 0:
                    page.reload(timeout=45000, wait_until="domcontentloaded")
                    sniffer.wait_for_ops(page, _USER_PROFILE_OPS, 0, timeout_ms=10000)
                    page.wait_for_timeout(1800)
                if sum(sniffer.op_counts.get(k, 0) for k in _USER_PROFILE_OPS) == 0:
                    warn = f"账号 @{eff_handle} 未捕获到个人主页接口响应，疑似网络或页面异常。"
                    log.warning(f"⚠️ [自动置顶/空采告警 | {acc_name}] {warn}")
                    return {"account_name": acc_name, "status": "empty_capture", "warning": warn, "browser_opened": True}

                current_pinned_id = sniffer.pinned_tweet_id_from_api or detect_pinned_tweet_id_from_dom(
                    page, self_handle_lower=eff_handle
                )

                # 向下滚屏直至覆盖 72h 窗口（排除旧置顶帖本身，避免置顶老帖误触发提前停滚）
                cutoff_ts = int(time.time()) - win_hours * 3600
                for _ in range(max_scrolls):
                    if time.monotonic() - lock_t0 >= (hold_limit - 35):
                        break
                    non_pinned_ts = [
                        safe_int(p.get("publish_ts"), 0)
                        for pid, p in sniffer.own_posts_by_id.items()
                        if pid != current_pinned_id
                        and safe_int(p.get("publish_ts"), 0) > 0
                        and _is_same_account(p, self_handle_lower=eff_handle, self_uid=self_uid)
                    ]
                    if non_pinned_ts and min(non_pinned_ts) <= cutoff_ts:
                        break
                    page.mouse.wheel(0, random.randint(1800, 2600))
                    page.wait_for_timeout(random.randint(1200, 1800))

                # 再次刷新 API 置顶信息
                current_pinned_id = sniffer.pinned_tweet_id_from_api or current_pinned_id

                own_posts = [
                    p for p in sniffer.own_posts_by_id.values()
                    if _is_same_account(p, self_handle_lower=eff_handle, self_uid=self_uid)
                ]
                now_ts = int(time.time())
                win_sec = win_hours * 3600
                window_posts = [
                    p for p in own_posts
                    if safe_int(p.get("publish_ts"), 0) > 0 and 0 <= (now_ts - safe_int(p.get("publish_ts"), 0)) <= win_sec
                ]
                # 待采集官方分析的候选（有 72h 帖就采 72h，完全没有且未置顶才采全部最近主帖）
                analytics_pool = window_posts if window_posts else ([] if current_pinned_id else own_posts)
                analytics_targets = sorted(
                    analytics_pool,
                    key=lambda x: (safe_int(x.get("views"), 0), safe_int(x.get("likes"), 0), safe_int(x.get("publish_ts"), 0)),
                    reverse=True,
                )[:max_analytics_n]

                # 2. 逐条抓官方单帖分析指标（预留至少 30s 给最终置顶与回主页核验）
                analytics_fetched_cnt = 0
                if fetch_analytics and analytics_targets:
                    reserve_sec = 0 if eff_dry else 30
                    for cand in analytics_targets:
                        if time.monotonic() - lock_t0 >= max(10, hold_limit - reserve_sec):
                            log.info(f"⏹️ [自动置顶/预留置顶时间 | {acc_name}] 已采 {analytics_fetched_cnt}/{len(analytics_targets)} 帖分析，提前进入评选。")
                            break
                        tid = cand["tweet_id"]
                        sniffer.current_analytics_tweet_id = tid
                        try:
                            page.goto(f"https://x.com/{eff_handle}/status/{tid}/analytics", timeout=30000, wait_until="domcontentloaded")
                            if sniffer.wait_for_analytics(page, tid, timeout_ms=7000):
                                analytics_fetched_cnt += 1
                            page.wait_for_timeout(random.randint(350, 700))
                        except Exception as an_e:
                            log.debug(f"[自动置顶/单帖分析略过] tweet_id={tid}: {str(an_e)[:100]}")

                # 3. 按 72h 窗口转化率公式算分与选优
                selection = select_best_conversion_post(
                    own_posts,
                    analytics_by_id=sniffer.analytics_by_id,
                    current_pinned_id=current_pinned_id,
                    now_ts=now_ts,
                    window_hours=win_hours,
                    min_impressions=min_impr,
                    min_post_age_minutes=min_age_m,
                    metric_mode=metric_mode,
                    weights=weights,
                    prior_impressions=prior_impr,
                    min_switch_improvement_ratio=hysteresis,
                )

                winner = selection["winner"]
                ranked = selection["ranked"]
                if not winner:
                    action_status = selection["decision"]
                    verified_pin = current_pinned_id
                    _persist_snapshots_and_history(
                        db_obj,
                        acc_name=acc_name,
                        eff_handle=eff_handle,
                        now_ts=now_ts,
                        ranked_posts=[],
                        winner=None,
                        prev_pinned_id=current_pinned_id,
                        verified_pinned_id=verified_pin,
                        action_status=action_status,
                        metric_mode=metric_mode,
                        fallback_reason=selection["fallback_reason"],
                        dry_run=eff_dry,
                    )
                    log.info(
                        f"ℹ️ [自动置顶 | @{eff_handle}] 72h主帖=0 | 当前置顶={current_pinned_id or '无'} | 动作={action_status}"
                    )
                    return {
                        "account_name": acc_name,
                        "screen_name": eff_handle,
                        "status": action_status,
                        "browser_opened": True,
                        "previous_pinned_id": current_pinned_id,
                        "winner": None,
                        "verified_pinned_id": verified_pin,
                        "analytics_fetched": analytics_fetched_cnt,
                        "ranked": [],
                    }

                winner_id = winner["tweet_id"]
                if eff_dry:
                    action_status = "preview_already_pinned" if current_pinned_id == winner_id else "preview_would_pin"
                    verified_pin = current_pinned_id
                    err_warn = None
                elif current_pinned_id == winner_id:
                    action_status = "already_pinned"
                    verified_pin = current_pinned_id
                    err_warn = None
                else:
                    action_status, verified_pin, err_warn = _pin_tweet_and_verify_on_page(
                        page,
                        sniffer,
                        target_id=winner_id,
                        eff_handle=eff_handle,
                        logger=log,
                    )

                _persist_snapshots_and_history(
                    db_obj,
                    acc_name=acc_name,
                    eff_handle=eff_handle,
                    now_ts=now_ts,
                    ranked_posts=ranked,
                    winner=winner,
                    prev_pinned_id=current_pinned_id,
                    verified_pinned_id=verified_pin,
                    action_status=action_status,
                    metric_mode=metric_mode,
                    fallback_reason=selection["fallback_reason"],
                    dry_run=eff_dry,
                )

                hold_sec = round(time.monotonic() - lock_t0, 1)
                fb_txt = f" | 兜底={selection['fallback_reason']}" if selection["fallback_reason"] else ""
                log_fn = log.warning if action_status in ("pin_failed", "pin_unverified") else log.info
                log_fn(
                    f"📌 [自动置顶 | @{eff_handle}] 动作={action_status} | "
                    f"72h候选={selection['window_count']}(已查分析={analytics_fetched_cnt}) | "
                    f"原置顶={current_pinned_id or '无'} -> 最优={winner_id}"
                    f"(转化分={winner['conversion_score']}, 曝光={winner['effective_impressions']}, "
                    f"参与={winner['engagements']}, 主页访={winner['profile_visits']}, 涨粉={winner['follows']}) | "
                    f"复核置顶={verified_pin or '无'}{fb_txt} | 持锁={hold_sec}s"
                )
                for idx, item in enumerate(ranked[:3], start=1):
                    mark = "★置顶" if item["tweet_id"] == (verified_pin if action_status in ("pinned", "already_pinned") else winner_id) else "   "
                    log.info(
                        f"   {mark} #{idx} {item['tweet_id']} 分={item['conversion_score']} "
                        f"[曝{item['effective_impressions']} 参{item['engagements']} "
                        f"访{item['profile_visits']} 粉{item['follows']} 赞{item['likes']}] "
                        f"「{_clip_text(item['text'], 34)}」"
                    )

                return {
                    "account_name": acc_name,
                    "screen_name": eff_handle,
                    "status": action_status,
                    "warning": err_warn,
                    "browser_opened": True,
                    "previous_pinned_id": current_pinned_id,
                    "winner": winner,
                    "verified_pinned_id": verified_pin,
                    "window_count": selection["window_count"],
                    "analytics_fetched": analytics_fetched_cnt,
                    "ranked": ranked,
                    "hold_seconds": hold_sec,
                }
            finally:
                try:
                    page.remove_listener("response", sniffer.on_response)
                except Exception:
                    pass
    except UserDataDirLockTimeoutError as lock_e:
        log.warning(f"⏳ [自动置顶/目录锁等待超时 | {acc_name}] 下轮再试: {lock_e}")
        return {"account_name": acc_name, "status": "lock_timeout", "warning": str(lock_e), "browser_opened": False}


# ==============================================================================
# 五、状态看板与 6 小时自动守护循环 (`run_pinner_daemon` + `main`)
# ==============================================================================

def build_status_report(
    db: Any,
    accounts: Sequence[Dict[str, Any]],
    *,
    now_ts: Optional[int] = None,
) -> Dict[str, Any]:
    _ensure_indexes(db)
    snap_col = db.get_collection(PIN_SNAPSHOTS_COLLECTION)
    hist_col = db.get_collection(PIN_HISTORY_COLLECTION)
    eff_now = int(now_ts if now_ts is not None else time.time())
    cutoff_72h = eff_now - WINDOW_HOURS * 3600

    acc_reports: List[Dict[str, Any]] = []
    for acc in accounts:
        name = str(acc["name"])
        latest_hist = hist_col.find_one({"account_name": name}, {"_id": 0}, sort=[("executed_at_ts", -1)])
        all_snaps = list(snap_col.find({"account_name": name}, {"_id": 0}))
        recent_72h = sorted(
            [s for s in all_snaps if safe_int(s.get("publish_ts"), 0) >= cutoff_72h],
            key=lambda x: float(x.get("conversion_score") or 0.0),
            reverse=True,
        )
        pinned_snap = next((s for s in all_snaps if s.get("is_currently_pinned")), None)
        cur_pin_id = (
            (pinned_snap or {}).get("tweet_id")
            or (latest_hist or {}).get("verified_pinned_id")
        )
        recent_hist = sorted(
            list(hist_col.find({"account_name": name}, {"_id": 0})),
            key=lambda x: safe_int(x.get("executed_at_ts"), 0),
            reverse=True,
        )[:5]

        acc_reports.append(
            {
                "account_name": name,
                "screen_name": _clean_handle(acc.get("screen_name")),
                "current_pinned_id": cur_pin_id,
                "last_status": (latest_hist or {}).get("status"),
                "last_executed_at": (latest_hist or {}).get("executed_at"),
                "snapshots_total": len(all_snaps),
                "top_72h": [
                    {
                        "tweet_id": s.get("tweet_id"),
                        "score": float(s.get("conversion_score") or 0.0),
                        "impressions": safe_int(s.get("effective_impressions"), 0),
                        "engagements": safe_int(s.get("engagements"), 0),
                        "profile_visits": safe_int(s.get("profile_visits"), 0),
                        "follows": safe_int(s.get("follows"), 0),
                        "is_pinned": bool(s.get("is_currently_pinned")),
                        "text": _clip_text(s.get("text"), 44),
                    }
                    for s in recent_72h[:5]
                ],
                "recent_history": [
                    {
                        "executed_at": h.get("executed_at"),
                        "status": h.get("status"),
                        "prev": h.get("previous_pinned_id"),
                        "verified": h.get("verified_pinned_id"),
                        "score": h.get("winner_score"),
                    }
                    for h in recent_hist
                ],
            }
        )
    return {"generated_at": _now_iso(eff_now), "accounts": acc_reports}


def format_status_text(report: Dict[str, Any]) -> str:
    lines = ["=" * 78, f"X 72小时最高转化自动置顶看板 | {report['generated_at']}", "-" * 78]
    for a in report.get("accounts") or []:
        lines.append(
            f"[{a['account_name']} @{a['screen_name'] or '自动识别'}] 当前置顶={a['current_pinned_id'] or '未知'} | "
            f"上次状态={a['last_status'] or '未运行'} ({a['last_executed_at'] or '-'}) | 历史快照={a['snapshots_total']}条"
        )
        for idx, item in enumerate(a["top_72h"], start=1):
            pin_tag = "📌当前置顶" if item["is_pinned"] else f"#{idx}       "
            lines.append(
                f"   [{pin_tag}] {item['tweet_id']} 转化分={item['score']} | "
                f"曝={item['impressions']} 参={item['engagements']} 访={item['profile_visits']} 粉={item['follows']} | "
                f"{item['text']}"
            )
        for h in a["recent_history"][:3]:
            lines.append(
                f"   历史: {h['executed_at']} [{h['status']}] 原={h['prev'] or '无'} -> 终={h['verified'] or '无'} (分={h['score']})"
            )
    lines.append("=" * 78)
    return "\n".join(lines)


def run_pinner_daemon(
    accounts: Optional[Sequence[Dict[str, Any]]] = None,
    *,
    stop_event: Optional[threading.Event] = None,
    max_rounds: int = 0,
    config: Optional[Dict[str, Any]] = None,
) -> int:
    """【默认常驻入口】每 6 小时自动运行一轮 72 小时转化率评选与唯一置顶同步。"""
    log = _get_logger()
    cfg = {**_default_cfg(), **(config or {})}
    pool = resolve_accounts(accounts)
    if not pool:
        log.error("🚨 [AutoPinner] ACCOUNTS 中没有启用的有效账号，退出。")
        return 0

    interval_sec = max(60, safe_int(cfg.get("interval_seconds"), INTERVAL_SECONDS))
    raw_jitter = cfg.get("jitter_seconds", JITTER_SECONDS)
    jitter_pair = (
        (max(0, int(raw_jitter[0])), max(int(raw_jitter[0]), int(raw_jitter[1])))
        if isinstance(raw_jitter, (list, tuple)) and len(raw_jitter) == 2
        else (0, 0)
    )
    risk_cd_sec = max(300, safe_int(cfg.get("risk_cooldown_seconds"), RISK_COOLDOWN_SECONDS))
    cooldown_until: Dict[str, float] = {}

    log.info(
        f"🚀 [AutoPinner/守护启动] 账号={[a['name'] + '(@' + (a['screen_name'] or '自动') + ')' for a in pool]} | "
        f"周期={interval_sec}s ({round(interval_sec / 3600, 1):g}h) | 评估窗口={cfg.get('window_hours', WINDOW_HOURS)}h | "
        f"转化口径={cfg.get('conversion_metric_mode', CONVERSION_METRIC_MODE)}"
    )

    def _step(round_no: int) -> Dict[str, Any]:
        db = gen_db_object()
        any_empty = False
        for acc in pool:
            acc_dir, acc_name = acc["user_data_dir"], acc["name"]
            rem = cooldown_until.get(acc_dir, 0.0) - time.time()
            if rem > 0:
                log.info(f"⏸️ [AutoPinner | #{round_no} | {acc_name}] 独立风控冷却中（剩余 {int(rem)}s），跳过。")
                continue
            res = sync_best_pinned_post_for_account(acc, db=db, config=cfg, logger=log)
            st = str(res.get("status") or "ok")
            if st in _RISK_HALT_STATUSES:
                cooldown_until[acc_dir] = time.time() + risk_cd_sec
                log.error(f"⏳ [AutoPinner | {acc_name}] 触发风控冷却 {risk_cd_sec}s | status={st}")
            elif st == "empty_capture":
                any_empty = True
            elif st in ("pinned", "already_pinned", "preview_already_pinned", "preview_would_pin", "keep_existing_no_window_posts"):
                cooldown_until.pop(acc_dir, None)

        if any_empty:
            return {"next_sleep_seconds": min(interval_sec, 600)}
        active_rems = [max(0.0, cooldown_until.get(a["user_data_dir"], 0.0) - time.time()) for a in pool]
        if pool and all(r > 0 for r in active_rems):
            return {"next_sleep_seconds": max(interval_sec, int(min(active_rems)))}
        return {}

    return run_daemon_loop(
        _step,
        interval_seconds=interval_sec,
        additive_jitter_range=jitter_pair,
        name="AutoPinner",
        logger_obj=log,
        max_rounds=max_rounds,
        stop_event=stop_event,
    )


def main(mode: Optional[str] = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    eff_mode = (mode or RUN_MODE).strip().lower()
    log = _get_logger()
    pool = resolve_accounts()

    if eff_mode == "login":
        for acc in pool:
            Path(acc["user_data_dir"]).mkdir(parents=True, exist_ok=True)
            open_browser_for_manual_use(user_data_dir=acc["user_data_dir"], home_url=X_HOME_URL)
        return 0

    db = gen_db_object()
    if eff_mode == "status":
        print(format_status_text(build_status_report(db, pool)))
        return 0

    if eff_mode == "preview":
        for acc in pool:
            sync_best_pinned_post_for_account(acc, db=db, dry_run=True, logger=log)
        print(format_status_text(build_status_report(db, pool)))
        return 0

    if eff_mode == "run_once":
        for acc in pool:
            sync_best_pinned_post_for_account(acc, db=db, dry_run=DRY_RUN, logger=log)
        print(format_status_text(build_status_report(db, pool)))
        return 0

    run_pinner_daemon(accounts=pool)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(RUN_MODE))
