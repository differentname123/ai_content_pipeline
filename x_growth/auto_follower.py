# -*- coding: utf-8 -*-
"""
X 平台极简高内聚自动互关系统 (`x_growth/auto_follower.py`)

零外部业务依赖，结构一目了然，全部日常参数与运行模式集中在本文件顶部与底部 `main()`：
- 【功能一 `harvest_potential_users`】：用宝贵小号每小时低频逛 3 屏首页 + 2 条讨论帖评论区 + 2 个高潜力蓝 V 的 `/verified_followers`（已认证粉丝圈），
  把捕获到的所有公开用户画像全部存进独立表 `x_user_profiles`，并计算 0~100 分经验回关潜力分（越可能回关分越高）。
- 【功能二 `follow_for_account` + 自动取关】：按 `TARGET_ACCOUNTS` 配置读取目标账号，从 `x_user_profiles` 按分数从高到低挑选当前账号未处理过的合格蓝 V，
  顺路核验己方最新粉丝是否回关，逐个打开对方主页实时复核并关注；单次浏览持锁超过 `SESSION_MAX_HOLD_SECONDS=180s` 立即收手放锁防超时，
  并对关注满 `UNFOLLOW_AFTER_HOURS=48h` 仍未回关的账号自动清理取关。
"""
from collections import Counter
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import os
from pathlib import Path
import random
import re
import statistics
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import unquote

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.common_utils import clean_post_text, get_config, get_proxy_url, get_runtime_file_path, read_json, resolve_session_dir, safe_int, save_json, setup_logger
from common.daemon import run_daemon_loop
from common.mongo_db.mongo_base import gen_db_object
from common.post_utils import AccountIsolationError, assert_not_x_myself_dir
from platforms.playwright_base import _dismiss_overlays, _robust_click, check_for_crash, persistent_browser_session
from platforms.x_playwright import _wait_x_home_ready, open_browser_for_manual_use

# ==============================================================================
# 一、运维常用配置区（直接改这里，改完即生效，无需敲命令行参数）
# ==============================================================================

# 0. 运行模式开关（直接在下面选一种即可）：
#   "daemon"        -> 【日常挂机】每小时自动循环：功能一(小号采用户) -> 功能二(主号关注 + 清理48h未回关)
#   "harvest_once"  -> 【只采一次】只跑一遍功能一，把推荐流/评论区/蓝V粉丝圈入池打分后退出
#   "follow_once"   -> 【只关一轮】不跑小号采集，按当前用户表高分名单跑一轮关注（含到期未回关清理）后退出
#   "unfollow_once" -> 【只取关一轮】仅清理关注满 48 小时仍未回关的账号后退出
#   "preview"       -> 【零浏览器预览】不开 Chrome，只打印各账号下一批将关注/待取关名单与效果看板
#   "status"        -> 【只看报表】打印用户表存量与累计真实回关效果分析
#   "login"         -> 【人工登录】打开可见 Chrome 给启用账号扫码登录固化会话
RUN_MODE: str = "daemon"

# 1. 需要执行自动关注的目标账号（想加号直接按注释模板复制一段即可）
TARGET_ACCOUNTS: List[Dict[str, Any]] = [
    {
        "name": "x_myself",
        "screen_name": str(get_config("x_myself_screen_name", "peterzhu1003") or "peterzhu1003").strip().lstrip("@"),
        "user_data_dir": resolve_session_dir("x_myself_browser_session_dir", "x_myself"),
        "enabled": True,
        # 【保留过滤原因说明】单轮 20 人配合 5~10s 间隔约耗时 3.5 分钟，避免长时间独占浏览器目录触发同目录发评/发帖 300s 锁超时；
        # 每日上限设为 400（贴近 X 官方上限）；设为 0 表示本地不限制，一直跑到 X 提示上限自动冷却恢复。
        "max_follow_per_run": 20,
        "max_follow_per_day": 400,
    },
    # {
    #     "name": "x_account_2",
    #     "screen_name": "another_handle",
    #     "user_data_dir": resolve_session_dir("x_account_2_browser_session_dir", "x_account_2"),
    #     "enabled": True,
    #     "max_follow_per_run": 20,
    #     "max_follow_per_day": 400,
    # },
]

# 2. 定时节奏、持锁止损与风控冷却（单位：秒）
INTERVAL_SECONDS: int = 3600                # 每小时跑一轮（3600 秒）
JITTER_SECONDS: Tuple[int, int] = (120, 360)# 每轮随机加 2~6 分钟抖动，避开整点机械指纹
ACTION_DELAY_RANGE: Tuple[int, int] = (3, 5)# 同轮两次关注/取关动作的随机停顿秒数
# 【保留过滤原因说明】`platforms/playwright_base.py` 跨进程目录锁超时为 300s；单次浏览会话持锁满 180s 立即收手放锁，给同目录发帖/神评留足 120s 窗口。
SESSION_MAX_HOLD_SECONDS: int = 180
LIMIT_COOLDOWN_SECONDS: int = 7200          # 撞 X 关注上限(code=88/161 或 429)后该号暂停 2 小时，届满自动再试
RISK_COOLDOWN_SECONDS: int = 21600          # 遇安全验证(code=326)或登录失效(401/403)暂停 6 小时，绝不硬撞
BROWSER_VISIBLE: bool = False               # False=后台无头静默运行；True=弹出 Chrome 窗口便于观察调试

# 3. 【功能一】小号轻采集参数（每次总共仅开 ~5 个页面，物尽其用兼顾护号）
COLLECTOR_USER_DATA_DIR: str = resolve_session_dir("x_xiaohao_browser_session_dir", "x_xiaohao")
COLLECTOR_HOME_SCROLLS: int = 3             # 首页「为你推荐」向下轻滚 3 屏
COLLECTOR_THREAD_CLICKS: int = 2            # 顺路点开 2 条活跃讨论帖抓评论区（避开千评骂战帖）
COLLECTOR_THREAD_SCROLLS: int = 2           # 每条讨论帖内轻滚 2 次加载评论者
COLLECTOR_THREAD_REPLY_RANGE: Tuple[int, int] = (5, 500)
COLLECTOR_THREAD_AVOID_SECONDS: int = 86400 # 24 小时内不重复点开同一条讨论帖
COLLECTOR_SEED_VERIFIED_FOLLOWERS: int = 2  # 每小时挑 2 个高潜力蓝 V 种子，扫其 `/verified_followers`（100%蓝V互关圈）
COLLECTOR_SEED_SCROLLS: int = 2             # 每个种子账号的已认证粉丝页轻滚 2 次
COLLECTOR_SEED_AVOID_DAYS: int = 7          # 同一个种子账号 7 天内不重复扫粉丝页
COLLECTOR_SEED_MIN_SCORE: float = 65.0      # 至少 65 分以上的高潜力蓝 V 才作为种子展开
COLLECTOR_SEED_MIN_FOLLOWERS: int = 150     # 种子账号至少 150 粉，保证已认证粉丝列表中有效体量

# 4. 【筛选硬门槛与评分参数】
REQUIRE_VERIFIED: bool = True               # True=只关注蓝 V 认证账号（非蓝 V 直接淘汰）
MIN_FOLLOW_SCORE: float = 50.0              # 仅从用户表里挑选潜力分 >= 50 的人去关注
MIN_FOLLOWERS: int = 30                     # 【保留过滤原因说明】过滤 30 粉以下极小空壳新号
MAX_FOLLOWERS: int = 10000                  # 【保留过滤原因说明】过滤万人以上高冷大V（3k~1w 通过评分平滑降权）
MIN_FF_RATIO: float = 0.45                  # 【保留过滤原因说明】过滤几乎不回关的纯单向号 (following/followers < 0.45)
MAX_FF_RATIO: float = 4.00                  # 【保留过滤原因说明】过滤无差别狂扫垃圾机器人 (> 4.0)
MAX_FOLLOWING_CAP: int = 4700               # 【保留过滤原因说明】避开接近 X 5000 关注上限无法再回关的账号
MIN_STATUSES: int = 5                       # 【保留过滤原因说明】至少发过 5 条推文，过滤空白僵尸号
MAX_ACTIVE_DAYS: int = 14                   # 【保留过滤原因说明】仅关注近 14 天内有发帖/评论或新关注动态的活人
ALLOWED_LANGUAGES: Optional[Sequence[str]] = None  # None=不限语种；想限定英日可改为 ("en", "ja")

# 5. 【功能二】回关核验、自动取关与重试
FOLLOW_BACK_SYNC_INTERVAL_SECONDS: int = 3000  # 每轮顺路看 1 次己方 `/followers` 实时记录谁回关了
FOLLOW_BACK_SYNC_SCROLLS: int = 2
FAILED_RETRY_COOLDOWN_SECONDS: int = 86400    # 点击超时/找不到按钮失败的账号，24 小时后允许重试
UNFOLLOW_ENABLED: bool = True                 # True=自动取关关注满 48 小时仍未回关的账号
UNFOLLOW_AFTER_HOURS: int = 48                # 关注满 48 小时（2 天）仍未回关即自动取关
UNFOLLOW_MAX_PER_RUN: int = 20                # 每轮最多取关 20 人（同样受 SESSION_MAX_HOLD_SECONDS=180 兜底）
UNFOLLOW_RETRY_COOLDOWN_SECONDS: int = 21600  # 单人取关失败 6 小时后允许重试，避免死循环卡在异常账号

# 6. 独立 MongoDB 表与运行时文件（与旧表/帖子表物理隔离）
USER_PROFILES_COLLECTION: str = "x_user_profiles"      # 新建公共表：全部捕获用户的完整资料与潜力分
FOLLOW_RECORDS_COLLECTION: str = "x_follow_records"    # 分账号记录表：关注状态、跳过原因、回关结果与耗时
VISITED_THREADS_FILE: str = "x_follower_visited_threads.json"
STRATEGY_VERSION: str = "scored_v3_verified_circle"

# ==============================================================================
# 二、基础工具、风控检查与 GraphQL 用户嗅探器
# ==============================================================================

TZ_CN = timezone(timedelta(hours=8))
X_HOME_URL = "https://x.com/home"

# 【保留过滤原因说明】先验排除明确拒互关宣言（优先级高于互关词）
ANTI_MUTUAL_BIO_RE = re.compile(
    r"(?:no\s*(?:f4f|ifb|follow(?:\s+you)?\s*back)|(?:don'?t|dont|do\s+not|not|never|won'?t|wont)\s+follow(?:\s+you)?\s*back"
    r"|one[\s-]*way\s*account|不互关|拒互关|绝不互关|拒绝互关|不回关|フォロバしません|相互フォロー不可|相互不可)",
    re.IGNORECASE,
)
MUTUAL_BIO_RE = re.compile(
    r"(?:follow(?:\s+you)?\s*back|follow\s*4\s*follow|follow\s*for\s*follow|\bf4f\b|\bifb\b|\bfb100\b|\bmutuals?\b"
    r"|互\s*fo(?![a-z0-9_])|互关|互粉|有关必回|关注必回|互关必回|回关|フォロバ|相互フォロー|フォローバック)",
    re.IGNORECASE,
)
_UNDETERMINED_LANGS = {"", "unknown", "und", "qme", "qam", "zxx", "art"}
_BRAND_VERIFIED_TYPES = {"business", "government"}
_PERMANENT_RECORD_STATUSES = {"followed", "unfollowed", "already_following", "already_mutual", "skipped_protected", "skipped_unavailable", "skipped_ineligible_live"}
_RISK_HALT_STATUSES = {"locked_challenge", "not_logged_in", "suspended"}
_LIMIT_HALT_STATUSES = {"rate_limited"}

_RE_HARVEST_GQL = re.compile(
    r"/graphql/[^/]+/(HomeTimeline|HomeLatestTimeline|TweetDetail|BlueVerifiedFollowers|VerifiedFollowers|Followers|UserByScreenName|UserTweets|UserOriginalsTimeline|ConnectTabTimeline)(?:\?|$)",
    re.IGNORECASE,
)
_RE_FOCAL_ID = re.compile(r'"focalTweetId"\s*:\s*"(\d+)"')
_RE_FOLLOWERS_GQL = re.compile(r"/graphql/[^/]+/(Followers|BlueVerifiedFollowers|VerifiedFollowers)(?:\?|$)", re.IGNORECASE)
_RE_FRIENDSHIP_API = re.compile(r"(?:friendships/create|CreateFriendship)", re.IGNORECASE)
_RE_UNFOLLOW_API = re.compile(r"(?:friendships/destroy|DestroyFriendship)", re.IGNORECASE)
_LIMIT_TOAST_TOKENS = ("unable to follow more", "rate limit", "temporarily limited", "limit reached", "无法关注更多", "已达关注上限", "操作受限")
_INDEXED_DBS: Set[int] = set()
_LOGGER: Optional[Any] = None


def _get_logger() -> Any:
    global _LOGGER
    if _LOGGER is None:
        _LOGGER = setup_logger(app_name="x_growth_follower")
    return _LOGGER


def _now_iso(now_ts: Optional[int] = None) -> str:
    return (datetime.fromtimestamp(int(now_ts), tz=TZ_CN) if now_ts is not None else datetime.now(TZ_CN)).isoformat(timespec="seconds")


def _clean_handle(h: Any) -> str:
    return str(h or "").strip().lstrip("@").split("/")[0].split("?")[0].strip()


def _norm_lang(lang: Any) -> str:
    base = str(lang or "").strip().lower().replace("_", "-").split("-", 1)[0].strip()
    return base or "unknown"


def _norm_langs(raw: Any) -> List[str]:
    seq = [raw] if isinstance(raw, str) else (raw if isinstance(raw, (list, tuple, set)) else [])
    return sorted({c for item in seq for c in [_norm_lang(item)] if c not in _UNDETERMINED_LANGS})


def _norm_dir(p: Any) -> str:
    raw = str(p or "").strip()
    return os.path.normcase(os.path.abspath(str(Path(raw).expanduser()))) if raw else ""


def _parse_x_ts(raw: Any) -> int:
    if isinstance(raw, (int, float)):
        return int(raw) if raw > 0 else 0
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


def _lerp(x: float, x0: float, x1: float, y0: float, y1: float) -> float:
    return y1 if x1 <= x0 else y0 + max(0.0, min(1.0, (x - x0) / (x1 - x0))) * (y1 - y0)


def _sleep_jitter(r: Sequence[float]) -> None:
    lo, hi = float(r[0]), float(r[1])
    if hi > 0:
        time.sleep(random.uniform(max(1.0, lo), max(lo + 1.0, hi)))


def _default_cfg() -> Dict[str, Any]:
    return {
        "max_follow_per_run": 20, "max_follow_per_day": 400, "action_delay_range": ACTION_DELAY_RANGE,
        "session_max_hold_seconds": SESSION_MAX_HOLD_SECONDS,
        "limit_cooldown_seconds": LIMIT_COOLDOWN_SECONDS, "risk_cooldown_seconds": RISK_COOLDOWN_SECONDS,
        "browser_visible": BROWSER_VISIBLE, "collector_user_data_dir": COLLECTOR_USER_DATA_DIR,
        "collector_home_scrolls": COLLECTOR_HOME_SCROLLS, "collector_thread_clicks": COLLECTOR_THREAD_CLICKS,
        "collector_thread_scrolls": COLLECTOR_THREAD_SCROLLS, "collector_thread_reply_range": COLLECTOR_THREAD_REPLY_RANGE,
        "collector_thread_avoid_seconds": COLLECTOR_THREAD_AVOID_SECONDS, "collector_seed_verified_followers": COLLECTOR_SEED_VERIFIED_FOLLOWERS,
        "collector_seed_scrolls": COLLECTOR_SEED_SCROLLS, "collector_seed_avoid_days": COLLECTOR_SEED_AVOID_DAYS,
        "collector_seed_min_score": COLLECTOR_SEED_MIN_SCORE, "collector_seed_min_followers": COLLECTOR_SEED_MIN_FOLLOWERS,
        "require_verified": REQUIRE_VERIFIED, "min_follow_score": MIN_FOLLOW_SCORE, "min_followers": MIN_FOLLOWERS,
        "max_followers": MAX_FOLLOWERS, "min_ff_ratio": MIN_FF_RATIO, "max_ff_ratio": MAX_FF_RATIO,
        "max_following_cap": MAX_FOLLOWING_CAP, "min_statuses": MIN_STATUSES, "max_active_days": MAX_ACTIVE_DAYS,
        "allowed_languages": ALLOWED_LANGUAGES, "follow_back_sync_interval_seconds": FOLLOW_BACK_SYNC_INTERVAL_SECONDS,
        "follow_back_sync_scrolls": FOLLOW_BACK_SYNC_SCROLLS, "failed_retry_cooldown_seconds": FAILED_RETRY_COOLDOWN_SECONDS,
        "unfollow_enabled": UNFOLLOW_ENABLED, "unfollow_after_hours": UNFOLLOW_AFTER_HOURS,
        "unfollow_max_per_run": UNFOLLOW_MAX_PER_RUN, "unfollow_retry_cooldown_seconds": UNFOLLOW_RETRY_COOLDOWN_SECONDS,
    }


def _ensure_indexes(db: Any) -> None:
    if db is None:
        return
    key = id(getattr(db, "db", db))
    if key in _INDEXED_DBS:
        return
    ci = getattr(db, "create_index", None)
    if callable(ci):
        ci(USER_PROFILES_COLLECTION, [("screen_name_lower", 1)], unique=True)
        ci(USER_PROFILES_COLLECTION, [("is_eligible", 1), ("follow_back_score", -1), ("last_active_ts", -1)], unique=False)
        ci(USER_PROFILES_COLLECTION, [("last_seen_ts", -1)], unique=False)
        ci(FOLLOW_RECORDS_COLLECTION, [("account_name", 1), ("target_handle_lower", 1)], unique=True)
        ci(FOLLOW_RECORDS_COLLECTION, [("account_name", 1), ("status", 1), ("followed_at_ts", -1)], unique=False)
        ci(FOLLOW_RECORDS_COLLECTION, [("account_name", 1), ("followed_back", 1)], unique=False)
    _INDEXED_DBS.add(key)


def resolve_target_accounts(accounts: Optional[Sequence[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    resolved: List[Dict[str, Any]] = []
    seen_dirs: Set[str] = set()
    for item in (accounts if accounts is not None else TARGET_ACCOUNTS):
        if not isinstance(item, dict) or item.get("enabled") is False or not str(item.get("user_data_dir") or "").strip():
            continue
        session_path = str(Path(str(item["user_data_dir"])).expanduser().resolve())
        nk = _norm_dir(session_path)
        if nk in seen_dirs:
            continue
        seen_dirs.add(nk)
        resolved.append({**item, "name": str(item.get("name") or Path(session_path).name).strip(), "screen_name": _clean_handle(item.get("screen_name")), "user_data_dir": session_path})
    return resolved


def assert_safe_collector_dir(collector_dir: str, accounts: Optional[Sequence[Dict[str, Any]]] = None) -> None:
    """物理隔离守卫：严禁使用主号 `x_myself` 或任何待关注运营号目录执行采集。"""
    assert_not_x_myself_dir(collector_dir, context="x_growth.auto_follower.collector")
    nc = _norm_dir(collector_dir)
    if not nc:
        return
    forbidden = {_norm_dir(a.get("user_data_dir")) for a in (accounts or TARGET_ACCOUNTS) if isinstance(a, dict) and a.get("user_data_dir")}
    if nc in forbidden:
        raise AccountIsolationError(f"安全隔离红线拦截：禁止使用待关注账号目录 `{collector_dir}` 充当采集小号！")


def inspect_x_account_status(page: Any) -> Tuple[str, Optional[str]]:
    url = str(page.url or "").lower()
    if "/account/access" in url or "arkoselabs" in url:
        return "locked_challenge", f"检测到安全验证解锁页 ({page.url})"
    if "/i/flow/login" in url or url.rstrip("/").endswith("/login"):
        return "not_logged_in", f"当前会话未登录 X ({page.url})"
    for sel, st, msg in (
        ('iframe[src*="arkoselabs"]', "locked_challenge", "页面出现 Arkose 验证框"),
        ('form[action*="/account/access"]', "locked_challenge", "页面出现验证挑战表单"),
        ('input[autocomplete="username"]', "not_logged_in", "页面出现未登录输入框"),
        ('a[data-testid="loginButton"]', "not_logged_in", "页面出现登录按钮"),
    ):
        try:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible():
                return st, msg
        except Exception:
            pass
    return "ok", None


def extract_logged_in_handle(page: Any) -> str:
    try:
        link = page.locator('a[data-testid="AppTabBar_Profile_Link"]').first
        if link.count() > 0:
            href = str(link.get_attribute("href") or "").strip().strip("/")
            if href and "/" not in href and not href.startswith("i"):
                return href.lower()
    except Exception:
        pass
    return ""


def extract_user_from_gql_node(raw: Any) -> Optional[Dict[str, Any]]:
    """兼容 X 新旧 GraphQL User 节点，提取全部关键字段（含认证类型、点赞数与默认头像标记）。"""
    if not isinstance(raw, dict):
        return None
    node = raw.get("user") if raw.get("__typename") == "UserWithVisibilityResults" and isinstance(raw.get("user"), dict) else raw
    if not isinstance(node, dict) or node.get("__typename") == "UserUnavailable":
        return None
    core, leg, rel_c, rel_p = node.get("core") or {}, node.get("legacy") or {}, node.get("relationship_counts") or {}, node.get("relationship_perspectives") or {}
    bio_obj, tw_c, priv, verif, av = node.get("profile_bio") or {}, node.get("tweet_counts") or {}, node.get("privacy") or {}, node.get("verification") or {}, node.get("avatar") or {}
    uid = str(node.get("rest_id") or "").strip()
    sn = _clean_handle(core.get("screen_name") or leg.get("screen_name"))
    if not sn or sn.lower() == "unknown" or (not uid and not leg and not core):
        return None
    is_blue = bool(node.get("is_blue_verified"))
    ver_type = str(verif.get("verified_type") or leg.get("verified_type") or "").strip() or None
    av_url = str(av.get("image_url") or leg.get("profile_image_url_https") or "")
    loc = node.get("location")
    desc = bio_obj.get("description") if bio_obj.get("description") is not None else leg.get("description")
    return {
        "user_id": uid, "screen_name": sn, "screen_name_lower": sn.lower(),
        "name": str(core.get("name") or leg.get("name") or sn).strip(),
        "description": clean_post_text(str(desc or "")),
        "location": str((loc.get("location") if isinstance(loc, dict) else loc) or leg.get("location") or "").strip(),
        "is_blue_verified": is_blue, "is_verified": bool(is_blue or verif.get("verified") or leg.get("verified")), "verified_type": ver_type,
        "is_protected": bool(priv.get("protected") if "protected" in priv else leg.get("protected")),
        "default_profile_image": bool(leg.get("default_profile_image")) or ("default_profile" in av_url.lower()),
        "followers_count": safe_int(rel_c.get("followers") if "followers" in rel_c else leg.get("followers_count"), 0),
        "following_count": safe_int(rel_c.get("following") if "following" in rel_c else leg.get("friends_count"), 0),
        "statuses_count": safe_int(tw_c.get("tweets") if "tweets" in tw_c else leg.get("statuses_count"), 0),
        "favourites_count": safe_int(leg.get("favourites_count"), 0),
        "listed_count": safe_int(leg.get("listed_count"), 0),
        "account_created_ts": _parse_x_ts(core.get("created_at") or leg.get("created_at")),
        "is_following": bool(rel_p.get("following") if "following" in rel_p else leg.get("following")),
        "is_followed_by": bool(rel_p.get("followed_by") if "followed_by" in rel_p else leg.get("followed_by")),
    }


def _walk_gql_nodes(payload: Any) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    tweets, users = [], []

    def _walk(obj: Any) -> None:
        if isinstance(obj, dict):
            if "promotedMetadata" in obj:
                return
            tn = obj.get("__typename")
            if tn == "TweetWithVisibilityResults" and isinstance(obj.get("tweet"), dict):
                _walk(obj["tweet"])
                return
            if tn == "Tweet" and isinstance(obj.get("legacy"), dict) and obj.get("rest_id"):
                tweets.append(obj)
                return
            if tn == "UserWithVisibilityResults" and isinstance(obj.get("user"), dict):
                users.append(obj["user"])
                return
            if tn == "User":
                users.append(obj)
            for v in obj.values():
                _walk(v)
        elif isinstance(obj, list):
            for v in obj:
                _walk(v)

    _walk(payload)
    return tweets, users


class XUserSniffer:
    """只读旁路 GraphQL 嗅探器：一次浏览收集首页、评论区与 `/verified_followers` 全部公开用户画像。"""

    def __init__(self) -> None:
        self.op_counts: Dict[str, int] = {}
        self.users_by_handle: Dict[str, Dict[str, Any]] = {}
        self.discovered_threads: Dict[str, Dict[str, Any]] = {}

    def register_user(self, prof: Optional[Dict[str, Any]], *, role: str = "discovered_user", tweet_ts: int = 0, lang: str = "") -> None:
        if not prof or not prof.get("screen_name"):
            return
        hl = _clean_handle(prof["screen_name"]).lower()
        if not hl or hl == "unknown":
            return
        prev = self.users_by_handle.get(hl) or {}
        merged = {**prev, **prof}
        if prev.get("description") and not merged.get("description"):
            merged["description"] = prev["description"]
        if safe_int(prev.get("account_created_ts"), 0) > 0 and safe_int(merged.get("account_created_ts"), 0) <= 0:
            merged["account_created_ts"] = prev["account_created_ts"]
        roles = set(prev.get("source_roles") or [])
        if role:
            roles.add(role)
        langs = set(prev.get("languages") or [])
        nl = _norm_lang(lang)
        if nl not in _UNDETERMINED_LANGS:
            langs.add(nl)
        merged.update({"screen_name_lower": hl, "source_roles": sorted(roles), "languages": sorted(langs), "last_active_ts": max(safe_int(prev.get("last_active_ts"), 0), safe_int(tweet_ts, 0))})
        self.users_by_handle[hl] = merged

    def _ingest_tweet(self, tw: Dict[str, Any], *, op: str, focal_id: str = "", focal_author_low: str = "") -> None:
        leg = tw.get("legacy") or {}
        rid = str(tw.get("rest_id") or leg.get("id_str") or "").strip()
        if not rid:
            return
        ts, lang, is_rt, is_rep = _parse_x_ts(leg.get("created_at")), str(leg.get("lang") or ""), bool(leg.get("retweeted_status_result")), bool(leg.get("in_reply_to_status_id_str"))
        ap = extract_user_from_gql_node(((tw.get("core") or {}).get("user_results") or {}).get("result"))
        al = (ap or {}).get("screen_name_lower") or ""
        role = "retweet_author" if is_rt else ("comment_author" if is_rep or (op == "TweetDetail" and focal_id and rid != focal_id and al != focal_author_low) else "post_author")
        if ap:
            self.register_user(ap, role=role, tweet_ts=ts, lang=lang)
        if isinstance((leg.get("retweeted_status_result") or {}).get("result"), dict):
            self._ingest_tweet(leg["retweeted_status_result"]["result"], op=op)
        qt = (tw.get("quoted_status_result") or {}).get("result")
        if isinstance(qt, dict):
            qt = qt["tweet"] if qt.get("__typename") == "TweetWithVisibilityResults" and isinstance(qt.get("tweet"), dict) else qt
            q_prof = extract_user_from_gql_node(((qt.get("core") or {}).get("user_results") or {}).get("result"))
            if q_prof:
                self.register_user(q_prof, role="quoted_author", tweet_ts=_parse_x_ts((qt.get("legacy") or {}).get("created_at")), lang=str((qt.get("legacy") or {}).get("lang") or ""))
        if op in ("HomeTimeline", "HomeLatestTimeline") and (not is_rt) and (not is_rep) and ap:
            self.discovered_threads[rid] = {"post_id": rid, "url": f"https://x.com/{ap['screen_name']}/status/{rid}", "reply_count": safe_int(leg.get("reply_count"), 0)}

    def ingest_payload(self, op: str, payload: Any, *, resp_url: str = "") -> None:
        if not isinstance(payload, dict):
            return
        tweets, users = _walk_gql_nodes(payload)
        focal_id, focal_author = "", ""
        if op == "TweetDetail" and tweets:
            m = _RE_FOCAL_ID.search(unquote(resp_url or ""))
            focal_id = m.group(1) if m else next((str(t.get("rest_id") or "") for t in tweets if not (t.get("legacy") or {}).get("in_reply_to_status_id_str")), "")
            for t in tweets:
                if str(t.get("rest_id") or "") == focal_id:
                    fp = extract_user_from_gql_node(((t.get("core") or {}).get("user_results") or {}).get("result"))
                    focal_author = (fp or {}).get("screen_name_lower") or ""
                    break
        for t in tweets:
            self._ingest_tweet(t, op=op, focal_id=focal_id, focal_author_low=focal_author)
        user_role = "verified_follower_of_peer" if op in ("BlueVerifiedFollowers", "VerifiedFollowers", "Followers") else "discovered_user"
        for unode in users:
            up = extract_user_from_gql_node(unode)
            if up:
                self.register_user(up, role=user_role, tweet_ts=0, lang="")

    def on_response(self, resp: Any) -> None:
        try:
            url = resp.url or ""
            m = _RE_HARVEST_GQL.search(url)
            if m and resp.status == 200:
                self.ingest_payload(m.group(1), resp.json(), resp_url=url)
                self.op_counts[m.group(1)] = self.op_counts.get(m.group(1), 0) + 1
        except Exception:
            pass

    def wait_for_ops(self, page: Any, ops: Sequence[str], prev_total: int, timeout_ms: int = 10000) -> bool:
        deadline = time.time() + timeout_ms / 1000.0
        while time.time() < deadline:
            if sum(self.op_counts.get(o, 0) for o in ops) > prev_total:
                return True
            page.wait_for_timeout(250)
        return False


# ==============================================================================
# 三、经验回关潜力评分引擎（硬门槛淘汰 + 5 维可解释 0~100 分制）
# ==============================================================================

def _score_ratio(r: float) -> float:
    if 0.95 <= r <= 1.60:
        return 30.0
    if 0.80 <= r < 0.95:
        return _lerp(r, 0.80, 0.95, 22.0, 30.0)
    if 1.60 < r <= 2.20:
        return 30.0 - _lerp(r, 1.60, 2.20, 0.0, 8.0)
    if 0.60 <= r < 0.80:
        return _lerp(r, 0.60, 0.80, 10.0, 22.0)
    if 2.20 < r <= 3.00:
        return 22.0 - _lerp(r, 2.20, 3.00, 0.0, 12.0)
    return _lerp(r, 0.45, 0.60, 2.0, 10.0) if r < 0.60 else 10.0 - _lerp(r, 3.00, 4.00, 0.0, 8.0)


def _score_followers(fc: int) -> float:
    if 150 <= fc <= 1500:
        return 20.0
    if 1500 < fc <= 3000:
        return 20.0 - _lerp(fc, 1500, 3000, 0.0, 6.0)
    if 80 <= fc < 150:
        return _lerp(fc, 80, 150, 12.0, 20.0)
    if 3000 < fc <= 5000:
        return 14.0 - _lerp(fc, 3000, 5000, 0.0, 7.0)
    return _lerp(fc, 30, 80, 2.0, 12.0) if fc < 80 else 7.0 - _lerp(fc, 5000, 10000, 0.0, 5.0)


def _score_recency(last_active_ts: int, last_seen_ts: int, roles: Sequence[str], statuses: int, now_ts: int) -> float:
    if last_active_ts > 0:
        h = max(0.0, (now_ts - last_active_ts) / 3600.0)
        if h <= 3.0:
            return 20.0
        if h <= 12.0:
            return 20.0 - _lerp(h, 3.0, 12.0, 0.0, 4.0)
        if h <= 24.0:
            return 16.0 - _lerp(h, 12.0, 24.0, 0.0, 3.0)
        if h <= 72.0:
            return 13.0 - _lerp(h, 24.0, 72.0, 0.0, 5.0)
        return 8.0 - _lerp(h, 72.0, 168.0, 0.0, 4.0) if h <= 168.0 else 4.0 - _lerp(h, 168.0, 336.0, 0.0, 3.0)
    if "verified_follower_of_peer" in roles:
        # X 粉丝页按时间倒序排列，近期被抓到代表近期仍有关注行为
        sh = max(0.0, (now_ts - last_seen_ts) / 3600.0)
        if sh <= 24.0:
            return 14.0
        if sh <= 72.0:
            return 14.0 - _lerp(sh, 24.0, 72.0, 0.0, 4.0)
        return 10.0 - _lerp(sh, 72.0, 168.0, 0.0, 4.0) if sh <= 168.0 else 6.0 - _lerp(sh, 168.0, 336.0, 0.0, 4.0)
    return 3.0 if statuses >= 50 else 1.0


def score_user_potential(user: Dict[str, Any], *, now_ts: Optional[int] = None, config: Optional[Dict[str, Any]] = None) -> Tuple[bool, Optional[str], float, Dict[str, float], Dict[str, Any]]:
    """评估公开用户回关潜力 (0~100 分)，返回 `(是否合格, 淘汰原因, 总分, 各维分数, 辅助字段)`。"""
    cfg = {**_default_cfg(), **(config or {})}
    now = int(now_ts if now_ts is not None else time.time())
    hl = _clean_handle(user.get("screen_name") or user.get("screen_name_lower")).lower()
    desc = str(user.get("description") or "")
    fc, fg = safe_int(user.get("followers_count"), 0), safe_int(user.get("following_count"), 0)
    statuses, favs = safe_int(user.get("statuses_count"), 0), safe_int(user.get("favourites_count"), 0)
    ff_ratio = round(fg / max(fc, 1), 3)
    ver_type = str(user.get("verified_type") or "").strip().lower() or None
    is_ver = bool(user.get("is_blue_verified") or user.get("is_verified"))
    roles = list(user.get("source_roles") or [])
    last_active_ts, last_seen_ts = safe_int(user.get("last_active_ts"), 0), safe_int(user.get("last_seen_ts"), now)
    langs = _norm_langs(user.get("languages") or user.get("primary_lang"))
    mb_m = MUTUAL_BIO_RE.search(desc)
    zero_bd = {"ratio_pts": 0.0, "size_pts": 0.0, "recency_pts": 0.0, "mutual_bio_pts": 0.0, "engagement_pts": 0.0, "cap_penalty": 0.0}
    meta = {"ff_ratio": ff_ratio, "has_mutual_bio": bool(mb_m), "mutual_bio_keyword": mb_m.group(0) if mb_m else None, "bio_len": len(desc), "primary_lang": (langs or ["unknown"])[0], "languages": langs}

    # 【保留过滤原因说明】硬门禁 1：排除无效/注销/锁推/官方机构号，且按运维要求仅关注蓝 V 认证号
    if not hl or hl == "unknown":
        return False, "invalid_handle", 0.0, zero_bd, meta
    if bool(user.get("account_unavailable")):
        return False, "unavailable", 0.0, zero_bd, meta
    if bool(user.get("is_protected")):
        return False, "protected", 0.0, zero_bd, meta
    if ver_type in _BRAND_VERIFIED_TYPES:
        return False, "official_or_brand", 0.0, zero_bd, meta
    if bool(cfg.get("require_verified", REQUIRE_VERIFIED)) and not is_ver:
        return False, "not_verified", 0.0, zero_bd, meta

    # 【保留过滤原因说明】硬门禁 2：先验排除明确拒互关账号
    if ANTI_MUTUAL_BIO_RE.search(desc):
        meta.update({"has_mutual_bio": False, "mutual_bio_keyword": None})
        return False, "anti_mutual_bio", 0.0, zero_bd, meta

    # 【保留过滤原因说明】硬门禁 3：避开关注总数已逼近 X 5000 上限无法回关的账号
    max_cap = safe_int(cfg.get("max_following_cap"), MAX_FOLLOWING_CAP)
    if max_cap > 0 and fg >= max_cap and not (fc >= max_cap and ff_ratio <= 1.05):
        return False, "near_5k_follow_cap", 0.0, zero_bd, meta

    # 【保留过滤原因说明】硬门禁 4：过滤极端粉丝数(<30或>10000)、三无低活跃号(<5推)、极端比值(<0.45或>4.0)及超14天沉睡号
    if fc < safe_int(cfg.get("min_followers"), MIN_FOLLOWERS) or fc > safe_int(cfg.get("max_followers"), MAX_FOLLOWERS):
        return False, "followers_out_of_range", 0.0, zero_bd, meta
    if statuses < safe_int(cfg.get("min_statuses"), MIN_STATUSES):
        return False, "low_activity_shell", 0.0, zero_bd, meta
    if ff_ratio < float(cfg.get("min_ff_ratio", MIN_FF_RATIO)) or ff_ratio > float(cfg.get("max_ff_ratio", MAX_FF_RATIO)):
        return False, "ratio_out_of_range", 0.0, zero_bd, meta
    ref_ts = last_active_ts if last_active_ts > 0 else last_seen_ts
    if ref_ts > 0 and (now - ref_ts) > max(1, safe_int(cfg.get("max_active_days"), MAX_ACTIVE_DAYS)) * 86400:
        return False, "inactive_over_window", 0.0, zero_bd, meta

    ratio_pts = round(_score_ratio(ff_ratio), 1)
    size_pts = round(_score_followers(fc), 1)
    recency_pts = round(_score_recency(last_active_ts, last_seen_ts, roles, statuses, now), 1)
    mutual_pts = 15.0 if mb_m else 0.0
    eng_pts = round(
        (7.0 if is_ver else 0.0)
        + (4.0 if ("comment_author" in roles or "verified_follower_of_peer" in roles) else 0.0)
        + (2.0 if len(desc.strip()) >= 10 else 0.0)
        + (2.0 if favs >= 20 else 0.0)
        + (-6.0 if bool(user.get("default_profile_image")) else 0.0),
        1,
    )
    cap_pen = round(-5.0 - _lerp(fg, 4000, max_cap, 0.0, 5.0), 1) if (4000 <= fg < max_cap and fc < 4200) else 0.0
    total = round(max(0.0, min(100.0, ratio_pts + size_pts + recency_pts + mutual_pts + eng_pts + cap_pen)), 1)
    bd = {"ratio_pts": ratio_pts, "size_pts": size_pts, "recency_pts": recency_pts, "mutual_bio_pts": mutual_pts, "engagement_pts": eng_pts, "cap_penalty": cap_pen}
    return True, None, total, bd, meta


# ==============================================================================
# 四、功能一：小号轻采集（首页 + 评论区 + 蓝V认证粉丝圈）-> x_user_profiles + 评分
# ==============================================================================

def _merge_profile_doc(old_doc: Optional[Dict[str, Any]], inc: Dict[str, Any], *, now_ts: int, config: Dict[str, Any], inc_seen: bool) -> Dict[str, Any]:
    old = old_doc or {}
    merged = {**old, **{k: v for k, v in inc.items() if k not in ("is_following", "is_followed_by", "source_roles", "languages") and v is not None}}
    sn = _clean_handle(merged.get("screen_name") or merged.get("screen_name_lower"))
    merged.update({
        "screen_name": sn, "screen_name_lower": sn.lower(),
        "source_roles": sorted(set(old.get("source_roles") or []) | set(inc.get("source_roles") or [])),
        "languages": _norm_langs(list(old.get("languages") or []) + list(inc.get("languages") or [])),
        "last_active_ts": max(safe_int(old.get("last_active_ts"), 0), safe_int(inc.get("last_active_ts"), 0)),
        "first_seen_ts": safe_int(old.get("first_seen_ts"), now_ts),
        "first_seen_at": old.get("first_seen_at") or _now_iso(safe_int(old.get("first_seen_ts"), now_ts)),
        "last_seen_ts": now_ts if inc_seen else safe_int(old.get("last_seen_ts"), now_ts),
        "seen_count": max(1, safe_int(old.get("seen_count"), 0) + (1 if inc_seen else 0)),
    })
    merged["last_active_at"] = _now_iso(merged["last_active_ts"]) if merged["last_active_ts"] > 0 else None
    merged["last_seen_at"] = _now_iso(merged["last_seen_ts"])
    elig, reason, score, bd, meta = score_user_potential(merged, now_ts=now_ts, config=config)
    merged.update({**meta, "is_eligible": elig, "ineligible_reason": reason, "follow_back_score": score, "score_breakdown": bd, "scored_at": _now_iso(now_ts), "scored_at_ts": now_ts, "scoring_version": STRATEGY_VERSION})
    merged.pop("_id", None)
    return merged


def upsert_and_score_users(db: Any, users: Iterable[Dict[str, Any]], *, now_ts: Optional[int] = None, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    _ensure_indexes(db)
    cfg = {**_default_cfg(), **(config or {})}
    now = int(now_ts if now_ts is not None else time.time())
    col = db.get_collection(USER_PROFILES_COLLECTION)
    dedup: Dict[str, Dict[str, Any]] = {}
    for u in users or []:
        if not isinstance(u, dict):
            continue
        hl = _clean_handle(u.get("screen_name") or u.get("screen_name_lower")).lower()
        if not hl or hl == "unknown":
            continue
        prev = dedup.get(hl) or {}
        dedup[hl] = {
            **prev, **u,
            "source_roles": sorted(set(prev.get("source_roles") or []) | set(u.get("source_roles") or [])),
            "languages": _norm_langs(list(prev.get("languages") or []) + list(u.get("languages") or [])),
            "last_active_ts": max(safe_int(prev.get("last_active_ts"), 0), safe_int(u.get("last_active_ts"), 0)),
        }
    if not dedup:
        return {"captured_total": 0, "new_users": 0, "updated_users": 0, "eligible_captured": 0, "comment_authors": 0, "verified_circle": 0, "rej": {}, "top5": []}

    existing = {str(d.get("screen_name_lower") or "").lower(): d for d in col.find({"screen_name_lower": {"$in": list(dedup.keys())}}) if d.get("screen_name_lower")}
    new_c, upd_c, elig_c, cmt_c, vf_c, rej, elig_docs = 0, 0, 0, 0, 0, Counter(), []
    for hl, inc in dedup.items():
        old = existing.get(hl)
        doc = _merge_profile_doc(old, inc, now_ts=now, config=cfg, inc_seen=True)
        col.update_one({"screen_name_lower": hl}, {"$set": doc}, upsert=True)
        new_c, upd_c = new_c + int(old is None), upd_c + int(old is not None)
        cmt_c += int("comment_author" in doc["source_roles"])
        vf_c += int("verified_follower_of_peer" in doc["source_roles"])
        if doc["is_eligible"]:
            elig_c += 1
            elig_docs.append(doc)
        else:
            rej[str(doc["ineligible_reason"] or "filtered")] += 1

    # 顺便对库内近期用户按当前时间做时间衰减重算
    cutoff = now - max(1, safe_int(cfg.get("max_active_days"), MAX_ACTIVE_DAYS)) * 86400
    for old_d in col.find({"last_seen_ts": {"$gte": cutoff}, "screen_name_lower": {"$nin": list(dedup.keys())}}):
        col.update_one({"screen_name_lower": old_d["screen_name_lower"]}, {"$set": _merge_profile_doc(old_d, {}, now_ts=now, config=cfg, inc_seen=False)})

    elig_docs.sort(key=lambda d: (-float(d["follow_back_score"]), -safe_int(d.get("last_active_ts"), 0)))
    top5 = [{"handle": f"@{d['screen_name']}", "score": d["follow_back_score"], "followers": d["followers_count"], "ratio": d["ff_ratio"], "roles": d["source_roles"], "lang": d["primary_lang"]} for d in elig_docs[:5]]
    return {"captured_total": len(dedup), "new_users": new_c, "updated_users": upd_c, "eligible_captured": elig_c, "comment_authors": cmt_c, "verified_circle": vf_c, "rej": dict(rej), "top5": top5}


def pick_verified_follower_seeds(db: Any, *, max_seeds: int = 2, now_ts: Optional[int] = None, config: Optional[Dict[str, Any]] = None, fallback_users: Optional[Iterable[Dict[str, Any]]] = None) -> List[str]:
    """挑选适合展开 `/verified_followers` 的高潜力蓝 V 种子账号（7 天内不重复展开同一个种子）。"""
    if max_seeds <= 0:
        return []
    cfg = {**_default_cfg(), **(config or {})}
    now = int(now_ts if now_ts is not None else time.time())
    avoid_sec = max(1, safe_int(cfg.get("collector_seed_avoid_days"), COLLECTOR_SEED_AVOID_DAYS)) * 86400
    cutoff = now - avoid_sec
    min_sc, min_fc = float(cfg.get("collector_seed_min_score", COLLECTOR_SEED_MIN_SCORE)), safe_int(cfg.get("collector_seed_min_followers"), COLLECTOR_SEED_MIN_FOLLOWERS)

    col = db.get_collection(USER_PROFILES_COLLECTION)
    pool = [
        d for d in col.find({"is_eligible": True, "follow_back_score": {"$gte": min_sc}})
        if safe_int(d.get("followers_count"), 0) >= min_fc and safe_int(d.get("seed_expanded_at_ts"), 0) < cutoff
    ]
    pool.sort(key=lambda d: (0 if d.get("has_mutual_bio") else 1, -float(d.get("follow_back_score") or 0.0), -safe_int(d.get("followers_count"), 0)))
    seeds = [str(d["screen_name"]) for d in pool[:max_seeds] if d.get("screen_name")]
    if len(seeds) < max_seeds and fallback_users:
        seen_low = {s.lower() for s in seeds}
        scored_fb = []
        for u in fallback_users:
            ok, _, sc, _, meta = score_user_potential(u, now_ts=now, config=cfg)
            if ok and sc >= min_sc and safe_int(u.get("followers_count"), 0) >= min_fc and u["screen_name_lower"] not in seen_low:
                scored_fb.append((0 if meta["has_mutual_bio"] else 1, -sc, u["screen_name"]))
        scored_fb.sort()
        seeds.extend(x[2] for x in scored_fb[: max_seeds - len(seeds)])
    return seeds[:max_seeds]


def harvest_potential_users(*, db: Any = None, collector_dir: Optional[str] = None, accounts: Optional[Sequence[Dict[str, Any]]] = None, config: Optional[Dict[str, Any]] = None, logger: Any = None) -> Dict[str, Any]:
    """【功能一入口】小号轻采首页 + 评论区 + 蓝V粉丝圈 -> 存入 `x_user_profiles` 并更新回关潜力分。"""
    log, cfg, t0 = logger or _get_logger(), {**_default_cfg(), **(config or {})}, time.perf_counter()
    cdir = str(collector_dir or cfg["collector_user_data_dir"] or "").strip()
    assert_safe_collector_dir(cdir, accounts=accounts)
    session_path = str(Path(cdir).expanduser().resolve())
    if not os.path.isdir(session_path):
        warn = f"采集小号会话目录不存在: {session_path}"
        log.error(f"🚨 [功能一/小号缺失] {warn}")
        return {"status": "not_logged_in", "warning": warn, "captured_total": 0}

    db_obj = db if db is not None else gen_db_object()
    avoid_sec = max(0, safe_int(cfg.get("collector_thread_avoid_seconds"), COLLECTOR_THREAD_AVOID_SECONDS))
    th_path = get_runtime_file_path(VISITED_THREADS_FILE)
    raw_th = read_json(th_path) if (avoid_sec > 0 and th_path.is_file()) else {}
    visited_threads = {str(k) for k, ts in (raw_th.items() if isinstance(raw_th, dict) else []) if safe_int(ts, 0) >= int(time.time()) - avoid_sec}
    sniffer, opened_threads, expanded_seeds, self_handle = XUserSniffer(), [], [], ""

    with persistent_browser_session(session_path, debug=bool(cfg["browser_visible"]), proxy=get_proxy_url(), use_stealth_chrome=True, reuse_existing_page=True, default_timeout_ms=45000, navigation_timeout_ms=45000, install_guard=True) as (context, page):
        page.on("response", sniffer.on_response)
        try:
            for att in range(2):
                prev = sniffer.op_counts.get("HomeTimeline", 0) + sniffer.op_counts.get("HomeLatestTimeline", 0)
                page.goto(X_HOME_URL, timeout=45000, wait_until="domcontentloaded") if att == 0 else page.reload(timeout=45000, wait_until="domcontentloaded")
                _wait_x_home_ready(page, timeout_ms=16000)
                sniffer.wait_for_ops(page, ("HomeTimeline", "HomeLatestTimeline"), prev, timeout_ms=12000)
                page.wait_for_timeout(random.randint(1200, 2000))
                _dismiss_overlays(page, aggressive=False, desc="x-harvest-home")
                check_for_crash(page)
                st, warn = inspect_x_account_status(page)
                if st != "ok":
                    log.error(f"🚨 [功能一/小号风控阻断] status={st} | {warn}")
                    return {"status": st, "warning": warn, "captured_total": 0}
                if sum(sniffer.op_counts.values()) > 0:
                    break

            self_handle = extract_logged_in_handle(page)
            try:
                tab = page.locator('div[role="tablist"] div[role="presentation"] a[role="tab"]').first
                if tab.count() > 0 and str(tab.get_attribute("aria-selected") or "").lower() == "false":
                    tab.click(timeout=3000)
            except Exception:
                pass
            for _ in range(max(1, safe_int(cfg["collector_home_scrolls"], 3))):
                page.mouse.wheel(0, random.randint(1800, 2800))
                page.wait_for_timeout(random.randint(1800, 2800))

            # Part A: 顺路进 2 条活跃讨论帖收评论区作者（24h 内看过的帖子不再重复看）
            lo_r, hi_r = cfg["collector_thread_reply_range"][0], cfg["collector_thread_reply_range"][1]
            cands_th = [t for tid, t in sniffer.discovered_threads.items() if tid not in visited_threads and lo_r <= t["reply_count"] <= hi_r]
            if len(cands_th) < safe_int(cfg["collector_thread_clicks"], 2):
                seen_tid = {t["post_id"] for t in cands_th}
                cands_th.extend(t for tid, t in sniffer.discovered_threads.items() if tid not in visited_threads and tid not in seen_tid and t["reply_count"] >= 3)
            random.shuffle(cands_th)
            for th in cands_th[: max(0, safe_int(cfg["collector_thread_clicks"], 2))]:
                prev_d = sniffer.op_counts.get("TweetDetail", 0)
                try:
                    page.goto(th["url"], timeout=35000, wait_until="domcontentloaded")
                    sniffer.wait_for_ops(page, ("TweetDetail",), prev_d, timeout_ms=10000)
                    page.wait_for_timeout(random.randint(1200, 2000))
                    for _ in range(max(1, safe_int(cfg["collector_thread_scrolls"], 2))):
                        page.mouse.wheel(0, random.randint(1100, 1700))
                        page.wait_for_timeout(random.randint(1200, 1900))
                    opened_threads.append(th["post_id"])
                except Exception:
                    pass

            # Part B: 逛高潜力蓝 V 的 `/verified_followers`（直接抓 100% 蓝 V 互关圈用户）
            seeds = pick_verified_follower_seeds(db_obj, max_seeds=safe_int(cfg["collector_seed_verified_followers"], 2), config=cfg, fallback_users=sniffer.users_by_handle.values())
            for seed in seeds:
                prev_vf = sum(sniffer.op_counts.get(k, 0) for k in ("BlueVerifiedFollowers", "VerifiedFollowers", "Followers"))
                try:
                    page.goto(f"https://x.com/{seed}/verified_followers", timeout=35000, wait_until="domcontentloaded")
                    sniffer.wait_for_ops(page, ("BlueVerifiedFollowers", "VerifiedFollowers", "Followers"), prev_vf, timeout_ms=10000)
                    page.wait_for_timeout(random.randint(1200, 1900))
                    for _ in range(max(1, safe_int(cfg["collector_seed_scrolls"], 2))):
                        page.mouse.wheel(0, random.randint(1200, 1800))
                        page.wait_for_timeout(random.randint(1200, 1900))
                    expanded_seeds.append(seed)
                    st2, warn2 = inspect_x_account_status(page)
                    if st2 != "ok":
                        log.error(f"🚨 [功能一/粉丝圈中途触发风控] status={st2} | {warn2}")
                        return {"status": st2, "warning": warn2, "captured_total": len(sniffer.users_by_handle)}
                except Exception:
                    pass
        finally:
            try:
                page.remove_listener("response", sniffer.on_response)
            except Exception:
                pass

    now_ts = int(time.time())
    if opened_threads and avoid_sec > 0:
        merged_th = {k: v for k, v in (raw_th.items() if isinstance(raw_th, dict) else []) if safe_int(v, 0) >= now_ts - avoid_sec}
        for tid in opened_threads:
            merged_th[tid] = now_ts
        save_json(th_path, merged_th)

    if not sniffer.users_by_handle and sum(sniffer.op_counts.values()) == 0:
        msg = f"小号浏览未捕获到 GraphQL 响应 (gql_ops={sniffer.op_counts})，请检查网络或账号状态"
        log.error(f"🚨 [功能一/空采告警] @{self_handle or 'x_xiaohao'} | {msg}")
        return {"status": "empty_capture", "warning": msg, "captured_total": 0}

    stats = upsert_and_score_users(db_obj, sniffer.users_by_handle.values(), now_ts=now_ts, config=cfg)
    for seed in expanded_seeds:
        db_obj.get_collection(USER_PROFILES_COLLECTION).update_one({"screen_name_lower": seed.lower()}, {"$set": {"seed_expanded_at_ts": now_ts}})

    ready_cnt = int(db_obj.get_collection(USER_PROFILES_COLLECTION).count_documents({"is_eligible": True, "follow_back_score": {"$gte": float(cfg["min_follow_score"])}}))
    elapsed = round(time.perf_counter() - t0, 2)
    log.info(
        f"🌾 [功能一完成 | @{self_handle or 'x_xiaohao'}] 耗时={elapsed}s | 进讨论帖={len(opened_threads)} | 展开蓝V粉丝圈={expanded_seeds or '无'} | "
        f"捕获={stats['captured_total']}人(新={stats['new_users']}, 评论区={stats['comment_authors']}, 粉丝圈={stats['verified_circle']}) | "
        f"合格={stats['eligible_captured']}人 | 达标可用池(>={cfg['min_follow_score']}分)={ready_cnt}人"
    )
    return {"status": "ok", "collector_handle": self_handle, "threads_opened": len(opened_threads), "seeds_expanded": expanded_seeds, "gql_ops": sniffer.op_counts, **stats, "pool_ready_count": ready_cnt, "elapsed_seconds": elapsed}


# ==============================================================================
# 五、功能二：按账号选高分蓝 V 候选 + 顺路核验回关 + 关注执行
# ==============================================================================

def count_recent_follows(db: Any, account_name: str, *, window_seconds: int = 86400, now_ts: Optional[int] = None) -> int:
    _ensure_indexes(db)
    cutoff = int(now_ts if now_ts is not None else time.time()) - max(1, int(window_seconds))
    return int(db.get_collection(FOLLOW_RECORDS_COLLECTION).count_documents({"account_name": account_name, "status": {"$in": ["followed", "unfollowed"]}, "followed_at_ts": {"$gte": cutoff}}))


def select_candidates_for_account(db: Any, account_cfg: Dict[str, Any], *, limit: int = 40, config: Optional[Dict[str, Any]] = None, now_ts: Optional[int] = None) -> Tuple[List[Dict[str, Any]], int, Dict[str, Any]]:
    """从 `x_user_profiles` 选出当前账号未处理过、最高分的一批蓝 V 候选。"""
    _ensure_indexes(db)
    cfg = {**_default_cfg(), **(config or {}), **account_cfg}
    now, acc_name = int(now_ts if now_ts is not None else time.time()), str(account_cfg.get("name") or "")
    failed_cd = max(0, safe_int(cfg.get("failed_retry_cooldown_seconds"), FAILED_RETRY_COOLDOWN_SECONDS))

    excluded: Set[str] = set()
    for rec in db.get_collection(FOLLOW_RECORDS_COLLECTION).find({"account_name": acc_name}, {"target_handle_lower": 1, "status": 1, "updated_at_ts": 1, "_id": 0}):
        th, st = _clean_handle(rec.get("target_handle_lower")).lower(), str(rec.get("status") or "")
        if th and (st in _PERMANENT_RECORD_STATUSES or st != "failed" or (now - safe_int(rec.get("updated_at_ts"), 0)) < failed_cd):
            excluded.add(th)

    min_sc = float(cfg.get("min_follow_score", MIN_FOLLOW_SCORE))
    req_ver = cfg.get("require_verified")
    allowed_langs = set(_norm_langs(cfg.get("allowed_languages"))) if cfg.get("allowed_languages") else set()
    all_elig = list(db.get_collection(USER_PROFILES_COLLECTION).find({"is_eligible": True, "follow_back_score": {"$gte": min_sc}}).sort([("follow_back_score", -1), ("last_active_ts", -1)]))

    picked, skip_rec, skip_acc = [], 0, Counter()
    for u in all_elig:
        hl = _clean_handle(u.get("screen_name_lower") or u.get("screen_name")).lower()
        if not hl:
            continue
        if hl in excluded:
            skip_rec += 1
            continue
        if req_ver is True and not bool(u.get("is_blue_verified") or u.get("is_verified")):
            skip_acc["not_verified"] += 1
            continue
        if allowed_langs:
            ul = set(_norm_langs(u.get("languages") or u.get("primary_lang")))
            if ul and not (allowed_langs & ul):
                skip_acc["language"] += 1
                continue
        picked.append(u)

    picked.sort(key=lambda d: (-float(d.get("follow_back_score") or 0.0), -safe_int(d.get("last_active_ts"), 0)))
    cap = max(1, int(limit)) if limit and int(limit) > 0 else len(picked)
    return picked[:cap], len(picked), {"pool_eligible": len(all_elig), "excluded_recorded": skip_rec, "acc_filtered": dict(skip_acc), "available": len(picked)}


def record_follow_outcome(db: Any, *, account_name: str, account_handle: str, account_dir: str, target_profile: Dict[str, Any], status: str, error: Optional[str] = None, followed_back: bool = False, candidate_rank: Optional[int] = None, eligible_pool_size: Optional[int] = None, now_ts: Optional[int] = None) -> None:
    _ensure_indexes(db)
    now = int(now_ts if now_ts is not None else time.time())
    now_s, th = _now_iso(now), _clean_handle(target_profile.get("screen_name"))
    if not th:
        return
    fc, fg = safe_int(target_profile.get("followers_count"), 0), safe_int(target_profile.get("following_count"), 0)
    langs = _norm_langs(target_profile.get("languages"))
    desc = str(target_profile.get("description") or "")
    mb_m = MUTUAL_BIO_RE.search(desc)
    last_post = safe_int(target_profile.get("last_active_ts"), 0)
    set_fields: Dict[str, Any] = {
        "strategy_version": STRATEGY_VERSION, "account_name": account_name, "account_handle": _clean_handle(account_handle), "account_dir": str(account_dir or ""),
        "target_handle": th, "target_handle_lower": th.lower(), "target_user_id": str(target_profile.get("user_id") or ""),
        "followers_count": fc, "following_count": fg, "ff_ratio": round(fg / max(fc, 1), 3), "statuses_count": safe_int(target_profile.get("statuses_count"), 0),
        "is_blue_verified": bool(target_profile.get("is_blue_verified") or target_profile.get("is_verified")), "source_roles": list(target_profile.get("source_roles") or []),
        "primary_lang": str(target_profile.get("primary_lang") or (langs[0] if langs else "unknown")).lower(), "languages": langs,
        "has_mutual_bio": bool(target_profile.get("has_mutual_bio") if "has_mutual_bio" in target_profile else mb_m),
        "last_post_ts": last_post, "post_age_hours_at_follow": round(max(0, now - last_post) / 3600.0, 2) if last_post > 0 else None,
        "follow_back_score": target_profile.get("follow_back_score"), "score_breakdown": target_profile.get("score_breakdown"),
        "candidate_rank": int(candidate_rank) if candidate_rank is not None else None, "eligible_pool_size": int(eligible_pool_size) if eligible_pool_size is not None else None,
        "status": status, "last_error": str(error)[:240] if error else None, "updated_at": now_s, "updated_at_ts": now,
    }
    if status == "followed":
        set_fields.update({"followed_at": now_s, "followed_at_ts": now, "followed_hour_cn": datetime.fromtimestamp(now, tz=TZ_CN).hour})
    if followed_back:
        set_fields.update({"followed_back": True, "followed_back_at": now_s, "followed_back_at_ts": now})
    on_ins: Dict[str, Any] = {"created_at": now_s, "created_at_ts": now}
    if not followed_back:
        on_ins.update({"follow_back_check_count": 0, "last_follow_back_check_at": None, "last_follow_back_check_ts": None, "followed_back": False, "followed_back_at": None, "followed_back_at_ts": None, "follow_back_latency_hours": None})
    db.get_collection(FOLLOW_RECORDS_COLLECTION).update_one({"account_name": account_name, "target_handle_lower": th.lower()}, {"$set": set_fields, "$setOnInsert": on_ins}, upsert=True)


def mark_follow_backs(db: Any, *, account_name: str, follower_handles: Iterable[str], now_ts: Optional[int] = None) -> Dict[str, Any]:
    _ensure_indexes(db)
    now, observed = int(now_ts if now_ts is not None else time.time()), {_clean_handle(h).lower() for h in (follower_handles or []) if _clean_handle(h)}
    now_s, rec_col = _now_iso(now), db.get_collection(FOLLOW_RECORDS_COLLECTION)
    pending = list(rec_col.find({"account_name": account_name, "status": {"$in": ["followed", "already_following"]}, "followed_back": {"$ne": True}}))
    new_hits: List[str] = []
    for d in pending:
        th_low = _clean_handle(d.get("target_handle_lower")).lower()
        if not th_low:
            continue
        upd: Dict[str, Any] = {"follow_back_check_count": safe_int(d.get("follow_back_check_count"), 0) + 1, "last_follow_back_check_at": now_s, "last_follow_back_check_ts": now}
        if th_low in observed:
            base_ts = safe_int(d.get("followed_at_ts") or d.get("created_at_ts"), now)
            upd.update({"followed_back": True, "followed_back_at": now_s, "followed_back_at_ts": now, "follow_back_latency_hours": round(max(0, now - base_ts) / 3600.0, 2), "updated_at": now_s, "updated_at_ts": now})
            new_hits.append(str(d.get("target_handle") or th_low))
        rec_col.update_one({"account_name": account_name, "target_handle_lower": th_low}, {"$set": upd})
    return {"observed_followers": len(observed), "checked_pending": len(pending), "newly_followed_back": len(new_hits), "new_handles": new_hits}


def _sync_follow_backs(page: Any, self_handle: str, *, db: Any, account_name: str, scrolls: int, log: Any) -> Dict[str, Any]:
    if not self_handle:
        return {"observed_followers": 0, "newly_followed_back": 0}
    observed: Set[str] = set()

    def _on_r(resp: Any) -> None:
        try:
            if resp.status == 200 and _RE_FOLLOWERS_GQL.search(resp.url or ""):
                _, users = _walk_gql_nodes(resp.json())
                for u in users:
                    p = extract_user_from_gql_node(u)
                    if p:
                        observed.add(p["screen_name_lower"])
        except Exception:
            pass

    page.on("response", _on_r)
    try:
        page.goto(f"https://x.com/{self_handle}/followers", timeout=40000, wait_until="domcontentloaded")
        try:
            page.locator('div[data-testid="UserCell"], div[data-testid="empty_state_header_text"]').first.wait_for(state="visible", timeout=10000)
        except Exception:
            pass
        _dismiss_overlays(page, aggressive=False, desc="x-followers")
        for _ in range(max(1, scrolls)):
            page.mouse.wheel(0, random.randint(1200, 1800))
            page.wait_for_timeout(random.randint(900, 1400))
            for href in page.locator('[data-testid="primaryColumn"] div[data-testid="UserCell"] a[href^="/"]').evaluate_all("els => els.map(e => e.getAttribute('href') || '')") or []:
                seg = str(href or "").strip().lstrip("/")
                if seg and "/" not in seg and seg.lower() not in ("i", "home", "explore", "notifications", "messages", "search"):
                    observed.add(seg.lower())
    except Exception as exc:
        log.warning(f"[功能二/回关核验跳过] @{self_handle}: {exc}")
    finally:
        try:
            page.remove_listener("response", _on_r)
        except Exception:
            pass

    observed.discard(self_handle.lower())
    res = mark_follow_backs(db, account_name=account_name, follower_handles=observed)
    log.info(f"[功能二/回关核验 | {account_name}] 最新粉丝扫描={res['observed_followers']}人 | 新确认回关={res['newly_followed_back']}人 {res['new_handles'] or ''}")
    return res


def _sync_live_user(db: Any, prof: Dict[str, Any], *, unavailable: bool = False, config: Dict[str, Any]) -> None:
    if db is None:
        return
    hl = _clean_handle(prof.get("screen_name") or prof.get("screen_name_lower")).lower()
    if hl:
        col = db.get_collection(USER_PROFILES_COLLECTION)
        patch = {**prof, "account_unavailable": True} if unavailable else prof
        col.update_one({"screen_name_lower": hl}, {"$set": _merge_profile_doc(col.find_one({"screen_name_lower": hl}), patch, now_ts=int(time.time()), config=config, inc_seen=False)}, upsert=True)


def _open_and_check_account_home(page: Any, *, desc: str) -> Tuple[str, Optional[str]]:
    page.goto(X_HOME_URL, timeout=45000, wait_until="domcontentloaded")
    _wait_x_home_ready(page, timeout_ms=16000)
    page.wait_for_timeout(random.randint(1200, 2000))
    _dismiss_overlays(page, aggressive=False, desc=desc)
    check_for_crash(page)
    return inspect_x_account_status(page)


def _goto_user_profile(page: Any, sniffer: XUserSniffer, handle: str, *, desc: str, pause_ms: Tuple[int, int] = (800, 1400)) -> Any:
    prev_u = sniffer.op_counts.get("UserByScreenName", 0)
    resp = page.goto(f"https://x.com/{handle}", timeout=45000, wait_until="domcontentloaded")
    sniffer.wait_for_ops(page, ("UserByScreenName",), prev_u, timeout_ms=10000)
    try:
        page.locator('div[data-testid="UserName"], div[data-testid="empty_state_header_text"], input[autocomplete="username"], a[data-testid="loginButton"]').first.wait_for(state="visible", timeout=12000)
    except Exception:
        pass
    page.wait_for_timeout(random.randint(pause_ms[0], pause_ms[1]))
    _dismiss_overlays(page, aggressive=False, desc=desc)
    check_for_crash(page)
    return resp


def _make_friendship_listener(api_re: re.Pattern, api_state: Dict[str, Any]) -> Any:
    def _on_resp(r: Any) -> None:
        try:
            if not api_re.search(r.url or ""):
                return
            body = None
            try:
                body = r.json()
            except Exception:
                pass
            errs = body.get("errors") if isinstance(body, dict) else None
            if isinstance(errs, list) and errs:
                e = errs[0] if isinstance(errs[0], dict) else {"message": str(errs[0])}
                code, em = safe_int(e.get("code"), 0), str(e.get("message") or errs)
                if code == 326:
                    api_state["halt"] = ("locked_challenge", f"code={code}: {em}")
                elif code in (88, 161):
                    api_state["halt"] = ("rate_limited", f"code={code}: {em}")
                elif code == 160:
                    api_state["skip"] = ("skipped_protected", em)
                elif code in (108, 162):
                    api_state["skip"] = ("skipped_unavailable", em)
                else:
                    api_state["error"] = f"code={code}: {em}"
                return
            if r.status in (401, 403, 429):
                api_state["halt"] = ("not_logged_in" if r.status == 401 else "rate_limited", f"HTTP {r.status}")
            elif r.status == 200:
                api_state["ok"] = True
        except Exception:
            pass

    return _on_resp


def _check_limit_toast(page: Any) -> Optional[str]:
    toast = page.locator('div[data-testid="toast"]').first
    if toast.count() > 0 and toast.is_visible():
        txt = toast.inner_text() or ""
        if any(t in txt.lower() for t in _LIMIT_TOAST_TOKENS):
            return txt[:120]
    return None


def _follow_one(page: Any, sniffer: XUserSniffer, cand: Dict[str, Any], *, db: Any, config: Dict[str, Any], log: Any) -> Dict[str, Any]:
    handle = _clean_handle(cand.get("screen_name"))
    resp = _goto_user_profile(page, sniffer, handle, desc="x-follow-profile", pause_ms=(800, 1400))

    st, msg = inspect_x_account_status(page)
    if st != "ok":
        return {"outcome": "halted", "halt_status": st, "halt_warning": msg, "live_profile": cand, "error": msg}
    if resp and resp.status in (401, 403, 429):
        return {"outcome": "halted", "halt_status": "not_logged_in" if resp.status == 401 else "rate_limited", "halt_warning": f"HTTP {resp.status}", "live_profile": cand, "error": f"http_{resp.status}"}
    if resp and resp.status >= 400:
        _sync_live_user(db, cand, unavailable=True, config=config)
        return {"outcome": "skipped_unavailable", "live_profile": cand, "error": f"http_{resp.status}"}
    try:
        eh = page.locator('div[data-testid="empty_state_header_text"]').first
        if eh.count() > 0 and eh.is_visible():
            _sync_live_user(db, cand, unavailable=True, config=config)
            return {"outcome": "skipped_unavailable", "live_profile": cand, "error": (eh.inner_text() or "empty")[:100]}
    except Exception:
        pass

    live_p = sniffer.users_by_handle.get(handle.lower())
    merged = {
        **cand,
        **(live_p or {}),
        "last_active_ts": max(safe_int(cand.get("last_active_ts"), 0), safe_int((live_p or {}).get("last_active_ts"), 0)),
        "source_roles": sorted(set(cand.get("source_roles") or []) | set((live_p or {}).get("source_roles") or [])),
        "languages": _norm_langs(list(cand.get("languages") or []) + list((live_p or {}).get("languages") or [])),
    }
    if isinstance(live_p, dict):
        ok, reason, sc, bd, meta = score_user_potential(merged, config=config)
        merged.update({"follow_back_score": sc, "score_breakdown": bd, **meta})
        _sync_live_user(db, merged, config=config)
        if live_p.get("is_protected"):
            return {"outcome": "skipped_protected", "live_profile": merged, "error": "protected_live"}
        if live_p.get("is_following"):
            return {"outcome": "already_mutual" if live_p.get("is_followed_by") else "already_following", "live_profile": merged, "error": None}
        if not ok:
            return {"outcome": "skipped_ineligible_live", "live_profile": merged, "error": f"{reason}_live"}

    pl = page.locator('[data-testid="primaryColumn"]').first.locator('div[data-testid="placementTracking"]')
    if pl.locator('button[data-testid$="-unfollow"]').first.count() > 0 and pl.locator('button[data-testid$="-unfollow"]').first.is_visible():
        return {"outcome": "already_mutual" if (live_p or {}).get("is_followed_by") else "already_following", "live_profile": merged, "error": None}
    if pl.locator('button[data-testid$="-cancel"]').first.count() > 0 and pl.locator('button[data-testid$="-cancel"]').first.is_visible():
        return {"outcome": "skipped_protected", "live_profile": merged, "error": "pending_cancel_btn"}
    btn = pl.locator('button[data-testid$="-follow"]').first
    if btn.count() == 0:
        btn = page.locator('[data-testid="primaryColumn"]').first.locator('button[data-testid$="-follow"]').first
    if btn.count() == 0 or not btn.is_visible():
        return {"outcome": "failed", "live_profile": merged, "error": "follow_btn_not_found"}

    api_state: Dict[str, Any] = {"ok": False, "halt": None, "skip": None, "error": None}
    _on_f = _make_friendship_listener(_RE_FRIENDSHIP_API, api_state)
    page.on("response", _on_f)
    try:
        _robust_click(btn)
        deadline = time.time() + 10.0
        while time.time() < deadline and not (api_state["halt"] or api_state["skip"] or api_state["ok"]):
            try:
                if pl.locator('button[data-testid$="-unfollow"]').first.is_visible():
                    api_state["ok"] = True
                    break
                if pl.locator('button[data-testid$="-cancel"]').first.is_visible():
                    api_state["skip"] = ("skipped_protected", "follow_requested_pending")
                    break
                toast_msg = _check_limit_toast(page)
                if toast_msg:
                    api_state["halt"] = ("rate_limited", toast_msg)
                    break
            except Exception:
                pass
            page.wait_for_timeout(300)
    finally:
        try:
            page.remove_listener("response", _on_f)
        except Exception:
            pass

    if api_state["halt"]:
        log.error(f"🚨 [功能二/达上限或风控停手] @{handle} | {api_state['halt'][0]}: {api_state['halt'][1]}")
        return {"outcome": "halted", "halt_status": api_state["halt"][0], "halt_warning": api_state["halt"][1], "live_profile": merged, "error": api_state["halt"][1]}
    if api_state["skip"]:
        return {"outcome": api_state["skip"][0], "live_profile": merged, "error": api_state["skip"][1]}
    if api_state["ok"]:
        page.wait_for_timeout(random.randint(600, 1000))
        return {"outcome": "followed", "live_profile": merged, "error": None}
    return {"outcome": "failed", "live_profile": merged, "error": api_state["error"] or "follow_unconfirmed"}


def follow_for_account(account_cfg: Dict[str, Any], *, db: Any, dry_run: bool = False, follow_back_last_sync: Optional[Dict[str, float]] = None, config: Optional[Dict[str, Any]] = None, logger: Any = None) -> Dict[str, Any]:
    """【功能二入口】为单个账号从用户表挑最高分蓝 V -> 核验回关 -> 实时复核关注（含 180s 持锁止损）。"""
    log = logger or _get_logger()
    cfg = {**_default_cfg(), **(config or {}), **account_cfg}
    acc_name, acc_handle, session_path = str(account_cfg["name"]), _clean_handle(account_cfg.get("screen_name")), str(account_cfg["user_data_dir"])
    max_run, max_day = max(1, safe_int(cfg.get("max_follow_per_run"), 20)), max(0, safe_int(cfg.get("max_follow_per_day"), 400))
    max_hold = max(30, safe_int(cfg.get("session_max_hold_seconds"), SESSION_MAX_HOLD_SECONDS))
    follows_24h = count_recent_follows(db, acc_name, window_seconds=86400)

    if (not dry_run) and max_day > 0 and follows_24h >= max_day:
        log.info(f"⏸️ [功能二/每日上限 | {acc_name}] 近 24h 已关注 {follows_24h}/{max_day} 人，本轮跳过。")
        return {"account_name": acc_name, "screen_name": acc_handle, "user_data_dir": session_path, "status": "daily_limit_reached", "follows_24h": follows_24h, "max_follow_per_day": max_day, "followed_count": 0, "followed_items": [], "hold_seconds": 0.0, "stopped_by_hold_limit": False, "browser_opened": False}

    target_limit = min(max_run, max(1, max_day - follows_24h)) if (not dry_run and max_day > 0) else max_run
    cands, total_avail, funnel = select_candidates_for_account(db, account_cfg, limit=max(target_limit * 3, 25), config=cfg)
    day_lbl = str(max_day) if max_day > 0 else "无上限"
    log.info(f"📊 [功能二/选人漏斗 | {acc_name}] 高分蓝V池={funnel['pool_eligible']} | 已处理跳过={funnel['excluded_recorded']} -> 本轮可用={total_avail}人")

    if dry_run:
        prev = [{"handle": f"@{c['screen_name']}", "score": c.get("follow_back_score"), "followers": c.get("followers_count"), "following": c.get("following_count"), "ratio": c.get("ff_ratio"), "roles": c.get("source_roles"), "lang": c.get("primary_lang"), "bio": str(c.get("description") or "").replace("\n", " ")[:55]} for c in cands[:target_limit]]
        for i, it in enumerate(prev, 1):
            log.info(f"[功能二/预览 | {acc_name}] #{i}/{target_limit} {it['handle']} | 潜力分={it['score']} | 粉={it['followers']} 关注={it['following']} 比值={it['ratio']} | 角色={it['roles']} | {it['lang']} | {it['bio']!r}")
        return {"account_name": acc_name, "screen_name": acc_handle, "user_data_dir": session_path, "status": "ok", "dry_run": True, "follows_24h": follows_24h, "max_follow_per_day": max_day, "available_count": total_avail, "followed_count": len(prev), "followed_items": prev, "hold_seconds": 0.0, "stopped_by_hold_limit": False, "browser_opened": False}

    if not os.path.isdir(session_path):
        warn = f"账号 {acc_name} (@{acc_handle}) 会话目录不存在: {session_path}"
        log.warning(f"⚠️ [功能二/会话缺失] {warn}")
        return {"account_name": acc_name, "screen_name": acc_handle, "user_data_dir": session_path, "status": "session_missing", "warning": warn, "follows_24h": follows_24h, "max_follow_per_day": max_day, "followed_count": 0, "followed_items": [], "hold_seconds": 0.0, "stopped_by_hold_limit": False, "browser_opened": False}

    need_sync = (time.time() - float((follow_back_last_sync or {}).get(acc_name, 0.0))) >= safe_int(cfg.get("follow_back_sync_interval_seconds"), FOLLOW_BACK_SYNC_INTERVAL_SECONDS)
    if not cands and not need_sync:
        log.info(f"ℹ️ [功能二 | {acc_name}] 当前暂无可用高分候选，本轮免开浏览器。")
        return {"account_name": acc_name, "screen_name": acc_handle, "user_data_dir": session_path, "status": "ok", "follows_24h": follows_24h, "max_follow_per_day": max_day, "followed_count": 0, "followed_items": [], "hold_seconds": 0.0, "stopped_by_hold_limit": False, "browser_opened": False}

    sniffer, followed, skips, halt_st, halt_warn = XUserSniffer(), [], Counter(), None, None
    hold_sec, hold_stopped = 0.0, False
    with persistent_browser_session(session_path, debug=bool(cfg["browser_visible"]), proxy=get_proxy_url(), use_stealth_chrome=True, reuse_existing_page=True, default_timeout_ms=45000, navigation_timeout_ms=45000, install_guard=True) as (_ctx, page):
        lock_started_at = time.monotonic()
        page.on("response", sniffer.on_response)
        try:
            st, warn = _open_and_check_account_home(page, desc="x-follow-home")
            if st != "ok":
                hold_sec = round(time.monotonic() - lock_started_at, 1)
                log.error(f"🚨 [功能二/账号异常 | {acc_name}] status={st} | {warn}")
                return {"account_name": acc_name, "screen_name": acc_handle, "user_data_dir": session_path, "status": st, "warning": warn, "follows_24h": follows_24h, "max_follow_per_day": max_day, "followed_count": 0, "followed_items": [], "hold_seconds": hold_sec, "stopped_by_hold_limit": False, "browser_opened": True}

            eff_handle = extract_logged_in_handle(page) or acc_handle
            if need_sync and eff_handle:
                _sync_follow_backs(page, eff_handle, db=db, account_name=acc_name, scrolls=safe_int(cfg.get("follow_back_sync_scrolls"), 2), log=log)
                if follow_back_last_sync is not None:
                    follow_back_last_sync[acc_name] = time.time()

            for idx, cand in enumerate(cands):
                if len(followed) >= target_limit:
                    break
                elapsed_lock = time.monotonic() - lock_started_at
                if elapsed_lock >= max_hold:
                    hold_stopped = True
                    log.warning(f"⏱️ [功能二/持锁止损 | {acc_name}] 已持锁 {elapsed_lock:.1f}s >= {max_hold}s，提前收手释放主号锁 (本轮已关注 {len(followed)}/{target_limit})")
                    break
                if idx > 0:
                    _sleep_jitter(cfg.get("action_delay_range") or ACTION_DELAY_RANGE)
                res = _follow_one(page, sniffer, cand, db=db, config=cfg, log=log)
                out = res["outcome"]
                if out == "halted":
                    halt_st, halt_warn = res.get("halt_status") or "rate_limited", res.get("halt_warning") or res.get("error")
                    break
                lp = res.get("live_profile") or cand
                record_follow_outcome(db, account_name=acc_name, account_handle=eff_handle, account_dir=session_path, target_profile=lp, status=out, error=res.get("error"), followed_back=(out == "already_mutual"), candidate_rank=idx + 1, eligible_pool_size=total_avail)
                if out == "followed":
                    fc, fg = safe_int(lp.get("followers_count"), 0), safe_int(lp.get("following_count"), 0)
                    followed.append({"handle": f"@{cand['screen_name']}", "score": lp.get("follow_back_score"), "followers": fc, "ratio": round(fg / max(fc, 1), 2)})
                    log.info(f"✅ [功能二/关注成功 | @{eff_handle}] #{len(followed)}/{target_limit}(rank=#{idx + 1}) @{cand['screen_name']} | 潜力分={lp.get('follow_back_score')} | 粉={fc} 比值={round(fg / max(fc, 1), 2)} | 24h={follows_24h + len(followed)}/{day_lbl}")
                else:
                    skips[out] += 1
                    log.info(f"⏭️ [功能二/跳过 | @{eff_handle}] @{cand['screen_name']} | {out} ({res.get('error') or ''})")

            hold_sec = round(time.monotonic() - lock_started_at, 1)
            log.info(f"📝 [功能二小结 | {acc_name}] 成功={len(followed)} | 跳过={dict(skips) or '无'} | 24h={follows_24h + len(followed)}/{day_lbl} | 持锁={hold_sec}s")
        finally:
            try:
                page.remove_listener("response", sniffer.on_response)
            except Exception:
                pass

    return {"account_name": acc_name, "screen_name": acc_handle, "user_data_dir": session_path, "status": halt_st or "ok", "warning": halt_warn, "follows_24h": follows_24h + len(followed), "max_follow_per_day": max_day, "followed_count": len(followed), "followed_items": followed, "hold_seconds": hold_sec, "stopped_by_hold_limit": hold_stopped, "browser_opened": True}


def select_unfollow_candidates(db: Any, account_cfg: Dict[str, Any], *, limit: Optional[int] = None, config: Optional[Dict[str, Any]] = None, now_ts: Optional[int] = None) -> Tuple[List[Dict[str, Any]], int]:
    """挑选该账号关注满 `UNFOLLOW_AFTER_HOURS`(48h) 且仍未回关的到期记录，按最早关注时间排序。"""
    _ensure_indexes(db)
    cfg = {**_default_cfg(), **(config or {}), **account_cfg}
    now, acc_name = int(now_ts if now_ts is not None else time.time()), str(account_cfg.get("name") or "")
    after_sec = max(1, safe_int(cfg.get("unfollow_after_hours"), UNFOLLOW_AFTER_HOURS)) * 3600
    cutoff = now - after_sec
    retry_cd = max(0, safe_int(cfg.get("unfollow_retry_cooldown_seconds"), UNFOLLOW_RETRY_COOLDOWN_SECONDS))

    due: List[Dict[str, Any]] = []
    for rec in db.get_collection(FOLLOW_RECORDS_COLLECTION).find({"account_name": acc_name, "status": "followed", "followed_back": {"$ne": True}}):
        fts = safe_int(rec.get("followed_at_ts"), 0)
        if fts <= 0 or fts > cutoff:
            continue
        if retry_cd > 0 and (now - safe_int(rec.get("unfollow_last_error_ts"), 0)) < retry_cd:
            continue
        due.append(rec)

    due.sort(key=lambda r: safe_int(r.get("followed_at_ts"), 0))
    cap = max(1, int(limit)) if (limit is not None and int(limit) > 0) else len(due)
    return due[:cap], len(due)


def _unfollow_one(page: Any, sniffer: XUserSniffer, rec: Dict[str, Any], *, log: Any) -> Dict[str, Any]:
    """打开对方主页实时复核是否已回关；确认未回关再点击取关并二次弹窗确认。"""
    handle = _clean_handle(rec.get("target_handle") or rec.get("target_handle_lower"))
    if not handle:
        return {"outcome": "failed", "reason": None, "live_profile": None, "error": "invalid_handle"}

    resp = _goto_user_profile(page, sniffer, handle, desc="x-unfollow-profile", pause_ms=(600, 1100))
    st, msg = inspect_x_account_status(page)
    if st != "ok":
        return {"outcome": "halted", "halt_status": st, "halt_warning": msg, "live_profile": None, "error": msg}
    if resp and resp.status in (401, 403, 429):
        return {"outcome": "halted", "halt_status": "not_logged_in" if resp.status == 401 else "rate_limited", "halt_warning": f"HTTP {resp.status}", "live_profile": None, "error": f"http_{resp.status}"}
    if resp and resp.status >= 400:
        return {"outcome": "unfollowed", "reason": "account_unavailable", "live_profile": None, "error": None}
    try:
        eh = page.locator('div[data-testid="empty_state_header_text"]').first
        if eh.count() > 0 and eh.is_visible():
            return {"outcome": "unfollowed", "reason": "account_unavailable", "live_profile": None, "error": None}
    except Exception:
        pass

    live_p = sniffer.users_by_handle.get(handle.lower())
    dom_follows_you = False
    try:
        ind = page.locator('[data-testid="primaryColumn"] [data-testid="userFollowIndicator"]').first
        dom_follows_you = ind.count() > 0 and ind.is_visible()
    except Exception:
        pass
    if (isinstance(live_p, dict) and live_p.get("is_followed_by") is True) or dom_follows_you:
        return {"outcome": "kept_followed_back", "reason": "followed_back_live", "live_profile": live_p, "error": None}

    if isinstance(live_p, dict) and live_p.get("is_following") is False:
        return {"outcome": "unfollowed", "reason": "already_not_following", "live_profile": live_p, "error": None}

    pl = page.locator('[data-testid="primaryColumn"]').first.locator('div[data-testid="placementTracking"]')
    btn = pl.locator('button[data-testid$="-unfollow"]').first
    if btn.count() == 0:
        btn = page.locator('[data-testid="primaryColumn"]').first.locator('button[data-testid$="-unfollow"]').first
    if btn.count() == 0 or not btn.is_visible():
        f_btn = pl.locator('button[data-testid$="-follow"]').first
        if f_btn.count() > 0 and f_btn.is_visible():
            return {"outcome": "unfollowed", "reason": "already_not_following", "live_profile": live_p, "error": None}
        return {"outcome": "failed", "reason": None, "live_profile": live_p, "error": "unfollow_btn_not_found"}

    api_state: Dict[str, Any] = {"ok": False, "halt": None, "skip": None, "error": None}
    _on_u = _make_friendship_listener(_RE_UNFOLLOW_API, api_state)
    page.on("response", _on_u)
    try:
        _robust_click(btn)
        confirm = page.locator('button[data-testid="confirmationSheetConfirm"]').first
        try:
            confirm.wait_for(state="visible", timeout=5000)
        except Exception:
            pass
        if confirm.count() == 0 or not confirm.is_visible():
            return {"outcome": "failed", "reason": None, "live_profile": live_p, "error": "unfollow_confirm_not_found"}
        _robust_click(confirm)
        deadline = time.time() + 10.0
        while time.time() < deadline and not (api_state["halt"] or api_state["ok"]):
            try:
                if pl.locator('button[data-testid$="-follow"]').first.is_visible():
                    api_state["ok"] = True
                    break
                toast_msg = _check_limit_toast(page)
                if toast_msg:
                    api_state["halt"] = ("rate_limited", toast_msg)
                    break
            except Exception:
                pass
            page.wait_for_timeout(300)
    finally:
        try:
            page.remove_listener("response", _on_u)
        except Exception:
            pass

    if api_state["halt"]:
        log.error(f"🚨 [功能二/取关达上限或风控停手] @{handle} | {api_state['halt'][0]}: {api_state['halt'][1]}")
        return {"outcome": "halted", "halt_status": api_state["halt"][0], "halt_warning": api_state["halt"][1], "live_profile": live_p, "error": api_state["halt"][1]}
    if api_state["ok"]:
        page.wait_for_timeout(random.randint(500, 900))
        return {"outcome": "unfollowed", "reason": "no_follow_back_48h", "live_profile": live_p, "error": None}
    return {"outcome": "failed", "reason": None, "live_profile": live_p, "error": api_state["error"] or "unfollow_unconfirmed"}


def unfollow_non_followers_for_account(account_cfg: Dict[str, Any], *, db: Any, dry_run: bool = False, config: Optional[Dict[str, Any]] = None, logger: Any = None) -> Dict[str, Any]:
    """【功能二自动取关】清理关注满 48 小时仍未回关的账号；逐个打开主页复核，确保零误取关已回关用户。"""
    log = logger or _get_logger()
    cfg = {**_default_cfg(), **(config or {}), **account_cfg}
    acc_name, acc_handle, session_path = str(account_cfg["name"]), _clean_handle(account_cfg.get("screen_name")), str(account_cfg["user_data_dir"])
    if not bool(cfg.get("unfollow_enabled", UNFOLLOW_ENABLED)):
        return {"account_name": acc_name, "screen_name": acc_handle, "user_data_dir": session_path, "status": "disabled", "due_count": 0, "unfollowed_count": 0, "rescued_count": 0, "failed_count": 0, "hold_seconds": 0.0, "stopped_by_hold_limit": False, "browser_opened": False, "items": []}

    max_run = max(1, safe_int(cfg.get("unfollow_max_per_run"), UNFOLLOW_MAX_PER_RUN))
    max_hold = max(30, safe_int(cfg.get("session_max_hold_seconds"), SESSION_MAX_HOLD_SECONDS))
    after_h = max(1, safe_int(cfg.get("unfollow_after_hours"), UNFOLLOW_AFTER_HOURS))
    due, total_due = select_unfollow_candidates(db, account_cfg, limit=max_run, config=cfg)

    if not due:
        if dry_run:
            log.info(f"ℹ️ [功能二/待取关预览 | {acc_name}] 当前暂无关注满 {after_h}h 仍未回关的待取关账号。")
        return {"account_name": acc_name, "screen_name": acc_handle, "user_data_dir": session_path, "status": "ok", "due_count": 0, "unfollowed_count": 0, "rescued_count": 0, "failed_count": 0, "hold_seconds": 0.0, "stopped_by_hold_limit": False, "browser_opened": False, "items": []}

    if dry_run:
        now_dry = int(time.time())
        log.info(f"🧹 [功能二/待取关预览 | {acc_name}] 满{after_h}h未回关到期={total_due}人 (本轮最多清理 {max_run} 人)")
        prev_items = []
        for i, rec in enumerate(due, 1):
            hrs = round(max(0, now_dry - safe_int(rec.get("followed_at_ts"), now_dry)) / 3600.0, 1)
            th = _clean_handle(rec.get("target_handle") or rec.get("target_handle_lower"))
            prev_items.append({"handle": f"@{th}", "hours_since_follow": hrs, "score": rec.get("follow_back_score")})
            log.info(f"[功能二/待取关预览 | {acc_name}] #{i}/{len(due)} @{th} | 已关注={hrs}h | 潜力分={rec.get('follow_back_score')}")
        return {"account_name": acc_name, "screen_name": acc_handle, "user_data_dir": session_path, "status": "ok", "dry_run": True, "due_count": total_due, "unfollowed_count": 0, "rescued_count": 0, "failed_count": 0, "hold_seconds": 0.0, "stopped_by_hold_limit": False, "browser_opened": False, "items": prev_items}

    if not os.path.isdir(session_path):
        warn = f"账号 {acc_name} (@{acc_handle}) 会话目录不存在: {session_path}"
        log.warning(f"⚠️ [功能二/取关会话缺失] {warn}")
        return {"account_name": acc_name, "screen_name": acc_handle, "user_data_dir": session_path, "status": "session_missing", "warning": warn, "due_count": total_due, "unfollowed_count": 0, "rescued_count": 0, "failed_count": 0, "hold_seconds": 0.0, "stopped_by_hold_limit": False, "browser_opened": False, "items": []}

    sniffer, unfollowed_items = XUserSniffer(), []
    rescued_cnt, failed_cnt, halt_st, halt_warn = 0, 0, None, None
    hold_sec, hold_stopped = 0.0, False
    rec_col = db.get_collection(FOLLOW_RECORDS_COLLECTION)
    log.info(f"🧹 [功能二/自动取关启动 | {acc_name}] 满{after_h}h未回关到期={total_due}人 -> 本轮计划清理 {len(due)} 人")

    with persistent_browser_session(session_path, debug=bool(cfg["browser_visible"]), proxy=get_proxy_url(), use_stealth_chrome=True, reuse_existing_page=True, default_timeout_ms=45000, navigation_timeout_ms=45000, install_guard=True) as (_ctx, page):
        lock_started_at = time.monotonic()
        page.on("response", sniffer.on_response)
        try:
            st, warn = _open_and_check_account_home(page, desc="x-unfollow-home")
            if st != "ok":
                hold_sec = round(time.monotonic() - lock_started_at, 1)
                log.error(f"🚨 [功能二/取关账号异常 | {acc_name}] status={st} | {warn}")
                return {"account_name": acc_name, "screen_name": acc_handle, "user_data_dir": session_path, "status": st, "warning": warn, "due_count": total_due, "unfollowed_count": 0, "rescued_count": 0, "failed_count": 0, "hold_seconds": hold_sec, "stopped_by_hold_limit": False, "browser_opened": True, "items": []}

            eff_handle = extract_logged_in_handle(page) or acc_handle
            for idx, rec in enumerate(due):
                elapsed_lock = time.monotonic() - lock_started_at
                if elapsed_lock >= max_hold:
                    hold_stopped = True
                    log.warning(f"⏱️ [功能二/取关持锁止损 | {acc_name}] 已持锁 {elapsed_lock:.1f}s >= {max_hold}s，提前收手释放主号锁 (已取关 {len(unfollowed_items)}/{len(due)})")
                    break
                if idx > 0:
                    _sleep_jitter(cfg.get("action_delay_range") or ACTION_DELAY_RANGE)

                th = _clean_handle(rec.get("target_handle") or rec.get("target_handle_lower"))
                th_low = _clean_handle(rec.get("target_handle_lower") or th).lower()
                res = _unfollow_one(page, sniffer, rec, log=log)
                out = res["outcome"]
                if out == "halted":
                    halt_st, halt_warn = res.get("halt_status") or "rate_limited", res.get("halt_warning") or res.get("error")
                    break

                now_ts = int(time.time())
                now_s = _now_iso(now_ts)
                if out == "kept_followed_back":
                    base_ts = safe_int(rec.get("followed_at_ts") or rec.get("created_at_ts"), now_ts)
                    rec_col.update_one(
                        {"account_name": acc_name, "target_handle_lower": th_low},
                        {"$set": {"followed_back": True, "followed_back_at": now_s, "followed_back_at_ts": now_ts, "follow_back_latency_hours": round(max(0, now_ts - base_ts) / 3600.0, 2), "updated_at": now_s, "updated_at_ts": now_ts}},
                    )
                    rescued_cnt += 1
                    log.info(f"💚 [功能二/取关前复核已回关 | @{eff_handle}] 保留关注 @{th}")
                elif out == "unfollowed":
                    reason = str(res.get("reason") or "no_follow_back_48h")
                    hrs = round(max(0, now_ts - safe_int(rec.get("followed_at_ts"), now_ts)) / 3600.0, 1)
                    rec_col.update_one(
                        {"account_name": acc_name, "target_handle_lower": th_low},
                        {"$set": {"status": "unfollowed", "unfollowed_at": now_s, "unfollowed_at_ts": now_ts, "unfollow_reason": reason, "updated_at": now_s, "updated_at_ts": now_ts}},
                    )
                    unfollowed_items.append({"handle": f"@{th}", "reason": reason, "hours_since_follow": hrs})
                    log.info(f"🧹 [功能二/取关成功 | @{eff_handle}] #{len(unfollowed_items)}/{len(due)} @{th} | 已关注 {hrs}h 未回关 | reason={reason}")
                else:
                    err_s = str(res.get("error") or "unfollow_failed")[:240]
                    rec_col.update_one(
                        {"account_name": acc_name, "target_handle_lower": th_low},
                        {"$set": {"unfollow_last_error": err_s, "unfollow_last_error_ts": now_ts, "updated_at": now_s, "updated_at_ts": now_ts}},
                    )
                    failed_cnt += 1
                    log.warning(f"⚠️ [功能二/取关失败 | @{eff_handle}] @{th} | {err_s}")

            hold_sec = round(time.monotonic() - lock_started_at, 1)
            log.info(f"📝 [功能二/取关小结 | {acc_name}] 取关={len(unfollowed_items)} | 复核已回关保留={rescued_cnt} | 失败={failed_cnt} | 持锁={hold_sec}s")
        finally:
            try:
                page.remove_listener("response", sniffer.on_response)
            except Exception:
                pass

    return {"account_name": acc_name, "screen_name": acc_handle, "user_data_dir": session_path, "status": halt_st or "ok", "warning": halt_warn, "due_count": total_due, "unfollowed_count": len(unfollowed_items), "rescued_count": rescued_cnt, "failed_count": failed_cnt, "hold_seconds": hold_sec, "stopped_by_hold_limit": hold_stopped, "browser_opened": True, "items": unfollowed_items}


# ==============================================================================
# 六、效果分析看板（检验评分与高回关策略真实效果）
# ==============================================================================

def _rate(items: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    n = len(items)
    fb = sum(1 for d in items if d.get("followed_back") is True)
    return {"followed": n, "followed_back": fb, "rate_pct": round((fb / n) * 100.0, 1) if n else 0.0}


def build_status_report(db: Any, accounts: Sequence[Dict[str, Any]], *, config: Optional[Dict[str, Any]] = None, now_ts: Optional[int] = None) -> Dict[str, Any]:
    _ensure_indexes(db)
    cfg, now = {**_default_cfg(), **(config or {})}, int(now_ts if now_ts is not None else time.time())
    min_sc = float(cfg.get("min_follow_score", MIN_FOLLOW_SCORE))
    pcol, rcol = db.get_collection(USER_PROFILES_COLLECTION), db.get_collection(FOLLOW_RECORDS_COLLECTION)
    user_pool = {
        "total": int(pcol.count_documents({})), "eligible_blue": int(pcol.count_documents({"is_eligible": True})),
        "ready": int(pcol.count_documents({"is_eligible": True, "follow_back_score": {"$gte": min_sc}})),
        "high_75": int(pcol.count_documents({"is_eligible": True, "follow_back_score": {"$gte": 75.0}})),
        "comment_authors": int(pcol.count_documents({"is_eligible": True, "source_roles": "comment_author"})),
        "verified_circle": int(pcol.count_documents({"is_eligible": True, "source_roles": "verified_follower_of_peer"})),
    }
    after_h = max(1, safe_int(cfg.get("unfollow_after_hours"), UNFOLLOW_AFTER_HOURS))
    acc_stats, all_followed = [], []
    for acc in accounts:
        name = str(acc["name"])
        docs = sorted([d for d in rcol.find({"account_name": name, "status": {"$in": ["followed", "unfollowed"]}}, {"_id": 0})], key=lambda d: safe_int(d.get("followed_at_ts"), 0), reverse=True)
        all_followed.extend(docs)
        r = _rate(docs)
        _, unfollow_due = select_unfollow_candidates(db, acc, limit=1, config=cfg, now_ts=now)
        acc_stats.append({
            "account_name": name, "screen_name": _clean_handle(acc.get("screen_name")),
            "follows_24h": sum(1 for d in docs if safe_int(d.get("followed_at_ts"), 0) >= now - 86400),
            "max_follow_per_day": safe_int(acc.get("max_follow_per_day"), 400),
            "unfollowed": sum(1 for d in docs if d.get("status") == "unfollowed"),
            "unfollow_due": unfollow_due,
            "unfollow_after_hours": after_h,
            **r, "recent": [{"target": f"@{d.get('target_handle')}", "score": d.get("follow_back_score"), "ff_ratio": d.get("ff_ratio"), "followed_back": bool(d.get("followed_back")), "followed_at": d.get("followed_at")} for d in docs[:5]],
        })

    sb, srcb, mb, fcb, rb = {">=80": [], "65~79": [], "50~64": [], "<50": [], "legacy": []}, {"comment_author": [], "verified_circle": [], "post_only": []}, {"mutual_bio": [], "normal": []}, {"30~499": [], "500~1499": [], "1500~3000": [], ">3000": []}, {"<0.8": [], "0.8~1.0": [], "1.0~1.5": [], "1.5~3.0": [], ">3.0": []}
    for d in all_followed:
        sc, roles, fc = d.get("follow_back_score"), set(d.get("source_roles") or []), safe_int(d.get("followers_count"), 0)
        sb["legacy" if sc is None else (">=80" if float(sc) >= 80 else ("65~79" if float(sc) >= 65 else ("50~64" if float(sc) >= 50 else "<50")))].append(d)
        srcb["comment_author" if "comment_author" in roles else ("verified_circle" if "verified_follower_of_peer" in roles else "post_only")].append(d)
        mb["mutual_bio" if d.get("has_mutual_bio") else "normal"].append(d)
        fcb["30~499" if fc < 500 else ("500~1499" if fc < 1500 else ("1500~3000" if fc <= 3000 else ">3000"))].append(d)
        rv = d.get("ff_ratio") if d.get("ff_ratio") is not None else (d.get("follow_telemetry") or {}).get("ff_ratio")
        if rv is not None:
            r = float(rv)
            rb["<0.8" if r < 0.8 else ("0.8~1.0" if r < 1.0 else ("1.0~1.5" if r < 1.5 else ("1.5~3.0" if r <= 3.0 else ">3.0")))].append(d)
    lats = [float(d["follow_back_latency_hours"]) for d in all_followed if d.get("followed_back") is True and d.get("follow_back_latency_hours") is not None]
    _, avail_p, _ = select_candidates_for_account(db, accounts[0], limit=1, config=cfg, now_ts=now) if accounts else ([], 0, {})
    return {
        "generated_at": _now_iso(now), "min_follow_score": min_sc, "user_pool": user_pool, "available_for_primary": avail_p, "accounts": acc_stats,
        "analytics": {**_rate(all_followed), "mature_24h": _rate([d for d in all_followed if safe_int(d.get("followed_at_ts"), 0) > 0 and (now - safe_int(d.get("followed_at_ts"), 0)) >= 86400]), "median_latency_h": round(statistics.median(lats), 2) if lats else None, "by_score": {k: _rate(v) for k, v in sb.items()}, "by_source": {k: _rate(v) for k, v in srcb.items()}, "by_mutual_bio": {k: _rate(v) for k, v in mb.items()}, "by_followers": {k: _rate(v) for k, v in fcb.items()}, "by_ff_ratio": {k: _rate(v) for k, v in rb.items()}},
    }


def _fmt_buckets(b: Dict[str, Dict[str, Any]], keys: Sequence[str]) -> str:
    return " | ".join(f"{k}={b[k]['followed_back']}/{b[k]['followed']}({b[k]['rate_pct']}%)" for k in keys if k in b)


def format_status_text(rep: Dict[str, Any]) -> str:
    up, an = rep["user_pool"], rep["analytics"]
    lines = [
        "=" * 76,
        f"X 自动互关看板 | {rep['generated_at']}",
        f"用户表({USER_PROFILES_COLLECTION}): 总库={up['total']} | 合格蓝V={up['eligible_blue']} | 达标(>={rep['min_follow_score']}分)={up['ready']} | 75+高潜={up['high_75']} | 评论区={up['comment_authors']} | 粉丝圈={up['verified_circle']} | 主号当前可用={rep['available_for_primary']}人",
        "-" * 76,
    ]
    for a in rep["accounts"]:
        dl = str(a["max_follow_per_day"]) if a["max_follow_per_day"] > 0 else "无上限"
        after_h = safe_int(a.get("unfollow_after_hours"), UNFOLLOW_AFTER_HOURS)
        lines.append(f"[{a['account_name']} @{a['screen_name']}] 24h={a['follows_24h']}/{dl} | 累计关注={a['followed']} | 已回关={a['followed_back']} ({a['rate_pct']}%) | 已取关={a.get('unfollowed', 0)} | 满{after_h}h待取关={a.get('unfollow_due', 0)}")
        for it in a["recent"]:
            sc = f"分={it['score']} " if it.get("score") is not None else ""
            lines.append(f"   - {it['target']} ({sc}ratio={it.get('ff_ratio')}) {'已回关' if it['followed_back'] else '待观察'} | {it['followed_at']}")
    m24, lat = an["mature_24h"], f"{an['median_latency_h']}h" if an["median_latency_h"] is not None else "暂无"
    lines.extend([
        "-" * 76,
        f"效果归因: 累计={an['followed_back']}/{an['followed']}({an['rate_pct']}%) | 满24h={m24['followed_back']}/{m24['followed']}({m24['rate_pct']}%) | 中位回关耗时={lat}",
        f"  潜力分: {_fmt_buckets(an['by_score'], ('>=80', '65~79', '50~64', '<50', 'legacy'))}",
        f"  来源圈: {_fmt_buckets(an['by_source'], ('comment_author', 'verified_circle', 'post_only'))} | 互关词: {_fmt_buckets(an['by_mutual_bio'], ('mutual_bio', 'normal'))}",
        f"  粉丝段: {_fmt_buckets(an['by_followers'], ('30~499', '500~1499', '1500~3000', '>3000'))}",
        f"  关注比: {_fmt_buckets(an['by_ff_ratio'], ('<0.8', '0.8~1.0', '1.0~1.5', '1.5~3.0', '>3.0'))}",
        "=" * 76,
    ])
    return "\n".join(lines)


# ==============================================================================
# 七、核心调度一轮与直白清晰的 main() 主入口
# ==============================================================================

def run_one_cycle(*, run_harvest: bool = True, run_follow: bool = True, run_unfollow: bool = True, dry_run: bool = False, cooldown_until: Optional[Dict[str, float]] = None, fb_sync_map: Optional[Dict[str, float]] = None, collector_state: Optional[Dict[str, Optional[float]]] = None, accounts: Optional[Sequence[Dict[str, Any]]] = None, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """清晰执行单轮流水线：第 1 步 功能一小号采用户评分 -> 第 2 步 功能二关注高分蓝V与清理 48h 未回关。"""
    log, cfg, db = _get_logger(), {**_default_cfg(), **(config or {})}, gen_db_object()
    pool = list(accounts) if accounts is not None else resolve_target_accounts()
    cd_map = cooldown_until if cooldown_until is not None else {}
    col_state = collector_state if collector_state is not None else {"cooldown_until": None}

    harvest_res: Dict[str, Any] = {"status": "skipped", "captured_total": 0}
    if run_harvest and not dry_run:
        col_cd = float(col_state.get("cooldown_until") or 0.0)
        if col_cd > time.time():
            harvest_res = {"status": "cooldown", "remaining_s": int(col_cd - time.time()), "captured_total": 0}
            log.info(f"⏸️ [功能一/小号冷却中] 剩余 {harvest_res['remaining_s']}s，跳过采集，直接消耗库存。")
        else:
            harvest_res = harvest_potential_users(db=db, accounts=pool, config=cfg, logger=log)
            if str(harvest_res.get("status")) in (_RISK_HALT_STATUSES | _LIMIT_HALT_STATUSES):
                sec = safe_int(cfg["risk_cooldown_seconds"], RISK_COOLDOWN_SECONDS)
                col_state["cooldown_until"] = time.time() + sec
                log.error(f"⚠️ [功能一/小号独立冷却 {sec}s] status={harvest_res.get('status')}")
            elif harvest_res.get("status") == "ok":
                col_state["cooldown_until"] = None

    def _apply_acc_cooldown(acc_name: str, acc_dir: str, status_val: str, phase: str) -> bool:
        if dry_run:
            return False
        if status_val in _LIMIT_HALT_STATUSES:
            sec = safe_int(cfg["limit_cooldown_seconds"], LIMIT_COOLDOWN_SECONDS)
            cd_map[acc_dir] = time.time() + sec
            log.warning(f"⏳ [功能二/{phase}触达上限或限流 | {acc_name}] 安全休息 {sec}s 后恢复。")
            return True
        if status_val in _RISK_HALT_STATUSES:
            sec = safe_int(cfg["risk_cooldown_seconds"], RISK_COOLDOWN_SECONDS)
            cd_map[acc_dir] = time.time() + sec
            log.error(f"⚠️ [功能二/{phase}账号风控冷却 | {acc_name}] status={status_val} -> 暂停 {sec}s")
            return True
        if status_val == "ok":
            cd_map.pop(acc_dir, None)
        return status_val == "session_missing"

    acc_results: List[Dict[str, Any]] = []
    if run_follow or run_unfollow:
        for acc in pool:
            acc_dir, acc_name = acc["user_data_dir"], acc["name"]
            rem = float(cd_map.get(acc_dir, 0.0) or 0.0) - time.time()
            if (not dry_run) and rem > 0:
                log.info(f"⏸️ [功能二/冷却跳过 | {acc_name}] 剩余 {int(rem)}s")
                acc_results.append({"account_name": acc_name, "screen_name": acc.get("screen_name"), "status": "cooldown", "follows_24h": count_recent_follows(db, acc_name), "followed_count": 0})
                continue

            res: Optional[Dict[str, Any]] = None
            halt_after_follow = False
            if run_follow:
                res = follow_for_account(acc, db=db, dry_run=dry_run, follow_back_last_sync=fb_sync_map, config=cfg, logger=log)
                acc_results.append(res)
                halt_after_follow = _apply_acc_cooldown(acc_name, acc_dir, str(res.get("status") or "ok"), "关注")

            if run_unfollow and bool(cfg.get("unfollow_enabled", UNFOLLOW_ENABLED)) and not halt_after_follow:
                if (not dry_run) and res and res.get("browser_opened"):
                    _, due_cnt = select_unfollow_candidates(db, acc, limit=1, config=cfg)
                    if due_cnt > 0:
                        time.sleep(2.0)
                uf_res = unfollow_non_followers_for_account(acc, db=db, dry_run=dry_run, config=cfg, logger=log)
                if res is not None:
                    res["unfollow"] = uf_res
                else:
                    acc_results.append({"account_name": acc_name, "screen_name": acc.get("screen_name"), "status": str(uf_res.get("status") or "ok"), "follows_24h": count_recent_follows(db, acc_name), "followed_count": 0, "unfollow": uf_res})
                uf_st = str(uf_res.get("status") or "ok")
                if uf_st in (_LIMIT_HALT_STATUSES | _RISK_HALT_STATUSES):
                    _apply_acc_cooldown(acc_name, acc_dir, uf_st, "取关")

    return {"harvest": harvest_res, "accounts": acc_results, "report": build_status_report(db, pool, config=cfg)}


def preview_and_status(*, dry_run: bool = True) -> None:
    pool, db = resolve_target_accounts(), gen_db_object()
    if dry_run:
        res = run_one_cycle(run_harvest=False, run_follow=True, run_unfollow=True, dry_run=True, accounts=pool)
        print(format_status_text(res["report"]))
    else:
        print(format_status_text(build_status_report(db, pool)))


def main(mode: Optional[str] = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass
    eff_mode = (mode or RUN_MODE).strip().lower()
    log = _get_logger()

    if eff_mode == "login":
        for acc in resolve_target_accounts():
            Path(acc["user_data_dir"]).mkdir(parents=True, exist_ok=True)
            open_browser_for_manual_use(user_data_dir=acc["user_data_dir"], home_url=X_HOME_URL)
        return 0
    if eff_mode == "status":
        preview_and_status(dry_run=False)
        return 0
    if eff_mode == "preview":
        preview_and_status(dry_run=True)
        return 0
    if eff_mode == "harvest_once":
        res = run_one_cycle(run_harvest=True, run_follow=False, run_unfollow=False)
        print(format_status_text(res["report"]))
        return 0
    if eff_mode == "follow_once":
        res = run_one_cycle(run_harvest=False, run_follow=True, run_unfollow=True)
        print(format_status_text(res["report"]))
        return 0
    if eff_mode == "unfollow_once":
        res = run_one_cycle(run_harvest=False, run_follow=False, run_unfollow=True)
        print(format_status_text(res["report"]))
        return 0

    # ── 默认 "daemon" 模式：每小时自动依次跑完「功能一采集打分 -> 功能二关注 + 满48h未回关自动取关」──
    pool = resolve_target_accounts()
    j_lo, j_hi = sorted((max(0, int(JITTER_SECONDS[0])), max(0, int(JITTER_SECONDS[1]))))
    cd_map: Dict[str, float] = {}
    fb_map: Dict[str, float] = {}
    col_state: Dict[str, Optional[float]] = {"cooldown_until": None}
    log.info(f"⏰ [AutoFollower/每小时守护启动] 模式={eff_mode} | 账号={[a['name'] + '(@' + a['screen_name'] + ')' for a in pool]} | 周期={INTERVAL_SECONDS}s + {j_lo}~{j_hi}s")

    def _hourly_job(round_no: int) -> Dict[str, Any]:
        log.info(f"========== [轮次 #{round_no}] 开始：1) 功能一小号轻采评分 -> 2) 功能二高分关注 -> 3) 清理48h未回关 ==========")
        res = run_one_cycle(run_harvest=True, run_follow=True, run_unfollow=True, cooldown_until=cd_map, fb_sync_map=fb_map, collector_state=col_state, accounts=pool)
        log.info("\n" + format_status_text(res["report"]))
        sleep_s = max(120, INTERVAL_SECONDS) + (random.randint(j_lo, j_hi) if j_hi > 0 else 0)
        next_at = (datetime.now(TZ_CN) + timedelta(seconds=sleep_s)).strftime("%Y-%m-%d %H:%M:%S")
        log.info(f"💤 [休眠] {sleep_s}s 后于 [{next_at}] 运行下一轮...")
        return {"next_sleep_seconds": sleep_s}

    run_daemon_loop(_hourly_job, interval_seconds=max(120, INTERVAL_SECONDS), additive_jitter_range=(j_lo, j_hi), name="AutoFollower", logger_obj=log)
    return 0


if __name__ == "__main__":
    # 直接右键运行本文件即按顶部 `RUN_MODE` 执行：
    #   "daemon"        -> 每小时自动运行（先小号采推荐流/评论区/蓝V粉丝圈入池打分，再主号关注高分蓝V并清理满48h未回关）
    #   "harvest_once"  -> 只跑一次功能一（小号采集 + 更新 x_user_profiles + 打分）
    #   "follow_once"   -> 只跑一次功能二（主号挑高分蓝V关注 + 顺路核验回关 + 清理满48h未回关）
    #   "unfollow_once" -> 只跑一次自动取关（仅清理关注满48h仍未回关的账号）
    #   "preview"       -> 不开浏览器预览下一批高分待关注与待取关名单
    #   "status"        -> 查看当前用户池存量与真实回关率分析看板
    raise SystemExit(main(RUN_MODE))
