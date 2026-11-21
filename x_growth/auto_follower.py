# -*- coding: utf-8 -*-
"""
X 平台极简自动互关守护脚本 (`x_growth/auto_follower.py`)

核心原则（80/20 极简可靠 · 运维一目了然）：
1. 直接复用 `harvested_social_posts` 最近 7 天作者画像（单一事实来源，不新建冗余候选表），库内充足或刚采过则直接免开小号；
2. 只做 4 条大概率有效的硬筛（蓝V认证 / 100~3000粉 / 关注粉丝比 0.8~3.0 且未触 5000 上限 / 近7天活跃且不拒互关），按「简介明确互关优先 -> 最近发帖活跃优先」排序；
3. 每小时每号仅开 1 次真实桌面 Chrome 完成回关核验与慢速关注，遇限流/验证码独立冷却 6 小时，全程结果与状态实时落盘可感知。
"""
import argparse
import json
import os
import random
import re
import statistics
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.common_utils import get_config, get_proxy_url, get_runtime_file_path, read_json, resolve_session_dir, safe_int, save_json
from common.daemon import run_daemon_loop
from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import HARVESTED_POSTS_COLLECTION
from common.post_utils import AccountIsolationError, assert_not_x_myself_dir
from platforms.playwright_base import _dismiss_overlays, _robust_click, check_for_crash, persistent_browser_session
from platforms.x_playwright import _wait_x_home_ready, open_browser_for_manual_use
from x_growth.growth_config import TZ_CN, get_configured_self_handles, get_growth_logger, normalize_language_code, now_iso_cn
from x_growth.x_browser_collector import (
    X_HOME_URL,
    XPassiveGraphQLSniffer,
    collect_and_save_posts,
    extract_logged_in_identity,
    inspect_x_page_account_status,
)

# ==============================================================================
# 一、常用运维配置区（所有日常调整项均集中在此，改完即生效）
# ==============================================================================

# 1. 需要自动去关注别人的账号列表（默认仅配置主号 peterzhu1003；严禁填入采集小号 x_xiaohao）
TARGET_ACCOUNTS: List[Dict[str, Any]] = [
    {
        "name": "x_myself",
        "screen_name": str(get_config("x_myself_screen_name", "peterzhu1003") or "peterzhu1003").strip().lstrip("@"),
        "user_data_dir": resolve_session_dir("x_myself_browser_session_dir", "x_myself"),
        "enabled": True,
        "max_follow_per_run": 25,    # 每小时单轮最多关注人数
        "max_follow_per_day": 24 * 25,   # 滚动 24 小时最多关注人数
    },
    # 如后续需要新增其他运营号，按下方格式取消注释即可：
    # {
    #     "name": "x_account_2",
    #     "screen_name": "another_handle",
    #     "user_data_dir": resolve_session_dir("x_account_2_browser_session_dir", "x_account_2"),
    #     "enabled": True,
    #     "max_follow_per_run": 8,
    #     "max_follow_per_day": 80,
    # },
]

# 2. 定时节奏与关注执行参数
CONTINUOUS_LOOP: bool = True                # True=每小时常驻运行；False=跑一轮退出
INTERVAL_SECONDS: int = 3600                # 基础巡检周期（3600 秒 = 1 小时）
JITTER_SECONDS: Tuple[int, int] = (120, 360)# 每轮随机正抖动秒数（2~6 分钟，避免固定整点指纹）
DEFAULT_MAX_FOLLOW_PER_RUN: int = 8         # 账号未单独配置时的每轮默认上限
DEFAULT_MAX_FOLLOW_PER_DAY: int = 80        # 账号未单独配置时的 24h 默认上限
ACTION_DELAY_RANGE: Tuple[int, int] = (8, 16)  # 同一轮内两次关注之间的随机等待秒数
RISK_COOLDOWN_SECONDS: int = 21600          # 遇限流/验证码/登录失效时独立冷却秒数（6 小时）
BROWSER_VISIBLE: bool = False               # False=后台无头静默运行；True=弹出 Chrome 窗口便于观察

# 3. 候选人筛选条件（4 条简单高概率规则，拒绝过度复杂）
REQUIRE_VERIFIED: bool = True               # True=只关注蓝V创作者（同体量冲量号互关率更高）
MIN_FOLLOWERS: int = 100                    # 最小粉丝数（避开僵尸/新建空壳号）
MAX_FOLLOWERS: int = 3000                   # 最大粉丝数（避开高冷大V）
MIN_FF_RATIO: float = 0.80                  # following / followers 下限（证明对方确有回关习惯）
MAX_FF_RATIO: float = 3.00                  # following / followers 上限（过滤无差别狂扫垃圾号）
MAX_FOLLOWING_CAP: int = 4500               # 避开已接近 X 5000 关注上限无法再回关的账号
ACTIVE_WITHIN_DAYS: int = 7                 # 只关注近 N 天仍有发帖的活人账号
ALLOWED_LANGUAGES: Optional[Sequence[str]] = None  # None=不限语种；如只想要英日区可填 ("en", "ja")

# 4. 协同补采与回关核验参数
COLLECTOR_USER_DATA_DIR: str = resolve_session_dir("x_xiaohao_browser_session_dir", "x_xiaohao")
COLLECTOR_REUSE_WINDOW_SECONDS: int = 3000  # 50 分钟内已有浏览器新采数据且池子充足时免开小号
MIN_CANDIDATE_POOL_TRIGGER: int = 30        # 库内活跃合格候选低于该人数时才唤起小号补采
FOLLOW_BACK_SYNC_INTERVAL_SECONDS: int = 14400  # 每 4 小时顺路打开一次己方粉丝页核验回关
FAILED_RETRY_COOLDOWN_SECONDS: int = 86400  # 单次页面/网络失败的目标 24 小时后再试
SELF_FOLLOWING_SAFETY_CAP: int = 4800       # 己方关注数逼近 X 5000 上限时自动保护停关

# 5. 存储集合与运维心跳文件
FOLLOW_RECORDS_COLLECTION: str = "x_follow_records"
STATUS_RUNTIME_FILE: str = "x_follow_status.json"
STRATEGY_VERSION: str = "simple_v1_blue_ratio_active"

# ==============================================================================
# 二、规则常量与正则
# ==============================================================================

# 【保留过滤原因说明】先验排除明确拒互关宣言（优先级高于互关词，避免 "no follow back" / "不互关" 被误判）
ANTI_MUTUAL_BIO_RE = re.compile(
    r"(?:no\s*(?:f4f|ifb|follow(?:\s+you)?\s*back)"
    r"|(?:don'?t|dont|do\s+not|not|never|won'?t|wont)\s+follow(?:\s+you)?\s*back"
    r"|one[\s-]*way\s*account|不互关|拒互关|绝不互关|拒绝互关|不回关|フォロバしません|相互フォロー不可|相互不可)",
    re.IGNORECASE,
)
MUTUAL_BIO_RE = re.compile(
    r"(?:follow(?:\s+you)?\s*back|follow\s*4\s*follow|follow\s*for\s*follow|\bf4f\b|\bifb\b|\bfb100\b|\bmutuals?\b"
    r"|互关|互粉|有关必回|关注必回|互关必回|回关|フォロバ|相互フォロー|フォローバック)",
    re.IGNORECASE,
)

_UNDETERMINED_LANGS = {"", "unknown", "und", "qme", "qam", "zxx", "art"}
_COOLDOWN_TRIGGER_STATUSES = {"locked_challenge", "not_logged_in", "rate_limited", "suspended"}
_RE_FOLLOWERS_GQL = re.compile(r"/graphql/[^/]+/(Followers|BlueVerifiedFollowers|VerifiedFollowers)(?:\?|$)", re.IGNORECASE)
_RE_FRIENDSHIP_API = re.compile(r"(?:friendships/create|CreateFriendship)", re.IGNORECASE)
_TOAST_RATE_LIMIT_TOKENS = ("unable to follow more", "rate limit", "temporarily limited", "limit reached", "无法关注更多", "已达关注上限", "操作受限")
_INDEXED_RECORD_DB_KEYS: Set[int] = set()


def _clean_handle(handle: Any) -> str:
    return str(handle or "").strip().lstrip("@").split("/")[0].split("?")[0].strip()


def _normalize_langs(raw_langs: Any) -> List[str]:
    raw_iter = [raw_langs] if isinstance(raw_langs, str) else (raw_langs if isinstance(raw_langs, (list, tuple, set)) else [])
    out: List[str] = []
    for item in raw_iter:
        norm = normalize_language_code(str(item or ""))
        if norm and norm not in _UNDETERMINED_LANGS and norm not in out:
            out.append(norm)
    return sorted(out)


def _ts_to_iso_cn(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=TZ_CN).isoformat(timespec="seconds")


def _norm_dir(path_str: Any) -> str:
    raw = str(path_str or "").strip()
    return os.path.normcase(os.path.abspath(str(Path(raw).expanduser()))) if raw else ""


def _sleep_jitter(delay_range: Sequence[float]) -> None:
    try:
        low, high = float(delay_range[0]), float(delay_range[1])
    except Exception:
        low, high = 8.0, 16.0
    if high > 0:
        time.sleep(random.uniform(max(1.0, low), max(low + 1.0, high)))


def _default_filter_config() -> Dict[str, Any]:
    return {
        "require_verified": REQUIRE_VERIFIED,
        "min_followers": MIN_FOLLOWERS,
        "max_followers": MAX_FOLLOWERS,
        "min_ff_ratio": MIN_FF_RATIO,
        "max_ff_ratio": MAX_FF_RATIO,
        "max_following_cap": MAX_FOLLOWING_CAP,
        "active_within_days": ACTIVE_WITHIN_DAYS,
        "allowed_languages": ALLOWED_LANGUAGES,
        "failed_retry_cooldown_seconds": FAILED_RETRY_COOLDOWN_SECONDS,
        "max_follow_per_run": DEFAULT_MAX_FOLLOW_PER_RUN,
        "max_follow_per_day": DEFAULT_MAX_FOLLOW_PER_DAY,
        "action_delay_range": ACTION_DELAY_RANGE,
        "risk_cooldown_seconds": RISK_COOLDOWN_SECONDS,
        "browser_visible": BROWSER_VISIBLE,
        "collector_user_data_dir": COLLECTOR_USER_DATA_DIR,
        "collector_reuse_window_seconds": COLLECTOR_REUSE_WINDOW_SECONDS,
        "min_candidate_pool_trigger": MIN_CANDIDATE_POOL_TRIGGER,
        "follow_back_sync_interval_seconds": FOLLOW_BACK_SYNC_INTERVAL_SECONDS,
        "self_following_safety_cap": SELF_FOLLOWING_SAFETY_CAP,
    }


def _ensure_record_indexes(db: Any) -> None:
    if db is None:
        return
    db_key = id(getattr(db, "db", db))
    if db_key in _INDEXED_RECORD_DB_KEYS:
        return
    create_index = getattr(db, "create_index", None)
    if callable(create_index):
        create_index(FOLLOW_RECORDS_COLLECTION, [("account_name", 1), ("target_handle_lower", 1)], unique=True)
        create_index(FOLLOW_RECORDS_COLLECTION, [("account_name", 1), ("status", 1), ("followed_at_ts", -1)], unique=False)
        create_index(FOLLOW_RECORDS_COLLECTION, [("account_name", 1), ("followed_back", 1)], unique=False)
    _INDEXED_RECORD_DB_KEYS.add(db_key)


def resolve_target_accounts(
    accounts: Optional[Sequence[Dict[str, Any]]] = None,
    *,
    account_filter: Optional[str] = None,
    overrides: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """解析启用中的目标关注账号（默认仅 `x_myself`），支持 CLI 临时覆盖参数。"""
    raw_list = accounts if accounts is not None else TARGET_ACCOUNTS
    tokens = {t.strip().lstrip("@").lower() for t in str(account_filter or "").split(",") if t.strip()}
    resolved: List[Dict[str, Any]] = []
    seen_dirs: Set[str] = set()
    for item in raw_list:
        if not isinstance(item, dict) or item.get("enabled") is False:
            continue
        raw_dir = str(item.get("user_data_dir") or "").strip()
        if not raw_dir:
            continue
        session_path = str(Path(raw_dir).expanduser().resolve())
        norm_key = _norm_dir(session_path)
        if norm_key in seen_dirs:
            continue
        name = str(item.get("name") or Path(session_path).name).strip()
        screen_name = _clean_handle(item.get("screen_name"))
        if tokens and name.lower() not in tokens and screen_name.lower() not in tokens:
            continue
        seen_dirs.add(norm_key)
        merged = {**item, "name": name, "screen_name": screen_name, "user_data_dir": session_path}
        if overrides:
            for k, v in overrides.items():
                if v is not None:
                    merged[k] = v
        resolved.append(merged)
    return resolved


def assert_safe_collector_dir(collector_dir: str, accounts: Optional[Sequence[Dict[str, Any]]] = None) -> None:
    """物理隔离守卫：严禁使用 `x_myself` 或任何关注主号目录执行被动采集。"""
    assert_not_x_myself_dir(collector_dir, context="x_growth.auto_follower.collector")
    norm_col = _norm_dir(collector_dir)
    if not norm_col:
        return
    forbidden = {_norm_dir(a.get("user_data_dir")) for a in (accounts or TARGET_ACCOUNTS) if isinstance(a, dict) and a.get("user_data_dir")}
    if norm_col in forbidden:
        raise AccountIsolationError(f"安全隔离红线拦截：禁止使用关注主号目录 `{collector_dir}` 充当采集账号！")


# ==============================================================================
# 三、高互关候选筛选（4 条规则 + 明确互关优先排序）
# ==============================================================================

def is_worth_following(
    author: Dict[str, Any],
    *,
    now_ts: Optional[int] = None,
    self_handles: Optional[Iterable[str]] = None,
    config: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, Optional[str], bool, Optional[str]]:
    """
    判断作者是否值得关注（大概率回关）：
    返回 `(是否通过, 淘汰原因, 是否明确互关简介, 命中的互关关键词)`。
    """
    cfg = {**_default_filter_config(), **(config or {})}
    now = int(now_ts if now_ts is not None else time.time())
    h_low = _clean_handle(author.get("screen_name") or author.get("username")).lower()
    eff_self = {h.lower() for h in get_configured_self_handles() if h}
    if self_handles:
        eff_self.update(_clean_handle(h).lower() for h in self_handles if _clean_handle(h))

    # 【保留过滤原因说明】规则 1：排除己方账号、私密锁推号与未认证号，聚焦公开成长中的蓝V创作者
    if not h_low or h_low == "unknown" or h_low in eff_self:
        return False, "self_or_invalid", False, None
    if bool(author.get("is_protected")):
        return False, "protected", False, None
    is_verified = bool(author.get("is_blue_verified") or author.get("is_verified"))
    if bool(cfg.get("require_verified", True)) and not is_verified:
        return False, "not_verified", False, None

    # 【保留过滤原因说明】规则 2：限定 100~3000 粉同体量成长区间，避开新建死号与高冷大V
    followers = safe_int(author.get("followers_count"), 0)
    if followers < safe_int(cfg.get("min_followers"), 100) or followers > safe_int(cfg.get("max_followers"), 3000):
        return False, "followers_out_of_range", False, None

    # 【保留过滤原因说明】规则 3：关注/粉丝比 0.80~3.00 且关注数 <=4500，确保对方有回关习惯且未卡死 X 5000 上限
    following = safe_int(author.get("following_count"), 0)
    ff_ratio = following / max(followers, 1)
    max_fg_cap = safe_int(cfg.get("max_following_cap"), 4500)
    if max_fg_cap > 0 and following > max_fg_cap and not (followers >= max_fg_cap and ff_ratio <= 1.05):
        return False, "near_5k_follow_cap", False, None
    if ff_ratio < float(cfg.get("min_ff_ratio", 0.80)) or ff_ratio > float(cfg.get("max_ff_ratio", 3.00)):
        return False, "ratio_out_of_range", False, None

    # 【保留过滤原因说明】规则 4：近 7 天仍有公开发推的活人账号，且简介不含明确拒互关宣言（拒互关优先拦截）
    last_active_ts = safe_int(author.get("last_active_ts") or author.get("latest_pub") or author.get("checked_at_ts"), 0)
    active_window = max(1, safe_int(cfg.get("active_within_days"), 7)) * 86400
    if last_active_ts <= 0 or (now - last_active_ts) > active_window:
        return False, "inactive_over_window", False, None

    desc = str(author.get("description") or "")
    if ANTI_MUTUAL_BIO_RE.search(desc):
        return False, "anti_mutual_bio", False, None

    allowed_langs_raw = cfg.get("allowed_languages")
    if allowed_langs_raw:
        allowed_set = set(_normalize_langs(allowed_langs_raw))
        author_langs = set(_normalize_langs(author.get("languages") or author.get("langs") or author.get("lang")))
        if allowed_set and author_langs and not (allowed_set & author_langs):
            return False, "language_mismatch", False, None

    mb_match = MUTUAL_BIO_RE.search(desc)
    return True, None, bool(mb_match), (mb_match.group(0) if mb_match else None)


def find_follow_candidates(
    db: Any,
    account_cfg: Dict[str, Any],
    *,
    limit: int = 30,
    config: Optional[Dict[str, Any]] = None,
    now_ts: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], int, Dict[str, Any]]:
    """直接从 `harvested_social_posts` 聚合近 N 天活跃作者，返回 `(排序后候选, 合格总数, 漏斗统计)`。"""
    _ensure_record_indexes(db)
    cfg = {**_default_filter_config(), **(config or {}), **account_cfg}
    now = int(now_ts if now_ts is not None else time.time())
    acc_name = str(account_cfg.get("name") or "")
    acc_handle = _clean_handle(account_cfg.get("screen_name"))
    active_cutoff = now - max(1, safe_int(cfg.get("active_within_days"), 7)) * 86400
    failed_cd = max(0, safe_int(cfg.get("failed_retry_cooldown_seconds"), 86400))

    rec_col = db.get_collection(FOLLOW_RECORDS_COLLECTION)
    excluded: Set[str] = set()
    for rec in rec_col.find({"account_name": acc_name}, {"target_handle_lower": 1, "status": 1, "updated_at_ts": 1, "_id": 0}):
        th = _clean_handle(rec.get("target_handle_lower")).lower()
        if not th:
            continue
        if str(rec.get("status") or "") != "failed" or (now - safe_int(rec.get("updated_at_ts"), 0)) < failed_cd:
            excluded.add(th)

    posts_col = db.get_collection(HARVESTED_POSTS_COLLECTION)
    pipeline = [
        {"$match": {"source": "x", "metadata.platform_extra.author_profile": {"$exists": True}, "publish_time": {"$gte": active_cutoff}}},
        {"$sort": {"publish_time": -1}},
        {
            "$group": {
                "_id": {"$toLower": "$author.username"},
                "screen_name": {"$first": "$author.username"},
                "user_id": {"$first": "$author_id"},
                "name": {"$first": "$metadata.platform_extra.author_profile.name"},
                "followers_count": {"$first": "$metadata.platform_extra.author_profile.followers_count"},
                "following_count": {"$first": "$metadata.platform_extra.author_profile.following_count"},
                "statuses_count": {"$first": "$metadata.platform_extra.author_profile.statuses_count"},
                "is_blue_verified": {"$first": "$metadata.platform_extra.author_profile.is_blue_verified"},
                "is_verified": {"$first": "$metadata.platform_extra.author_profile.is_verified"},
                "is_protected": {"$first": "$metadata.platform_extra.author_profile.is_protected"},
                "description": {"$first": "$metadata.platform_extra.author_profile.description"},
                "langs": {"$addToSet": "$metadata.lang"},
                "last_active_ts": {"$max": "$publish_time"},
            }
        },
    ]

    self_extra = [acc_handle] if acc_handle else None
    eligible: List[Dict[str, Any]] = []
    reject_counts: Counter = Counter()
    lang_counts: Counter = Counter()
    scanned_count = 0
    excluded_count = 0
    mutual_bio_count = 0

    for row in posts_col.aggregate(pipeline):
        scanned_count += 1
        sn = _clean_handle(row.get("screen_name"))
        if not sn:
            reject_counts["self_or_invalid"] += 1
            continue
        if sn.lower() in excluded:
            excluded_count += 1
            continue

        langs = _normalize_langs(row.get("langs"))
        primary_lang = langs[0] if langs else "unknown"
        last_ts = safe_int(row.get("last_active_ts"), 0)
        desc = str(row.get("description") or "")
        author_obj = {
            "screen_name": sn,
            "user_id": str(row.get("user_id") or "").strip(),
            "name": str(row.get("name") or sn).strip(),
            "followers_count": safe_int(row.get("followers_count"), 0),
            "following_count": safe_int(row.get("following_count"), 0),
            "statuses_count": safe_int(row.get("statuses_count"), 0),
            "is_blue_verified": bool(row.get("is_blue_verified")),
            "is_verified": bool(row.get("is_blue_verified") or row.get("is_verified")),
            "is_protected": bool(row.get("is_protected")),
            "description": desc,
            "languages": langs,
            "primary_lang": primary_lang,
            "last_active_ts": last_ts,
        }
        ok, reason, has_mutual_bio, mutual_kw = is_worth_following(
            author_obj, now_ts=now, self_handles=self_extra, config=cfg
        )
        if not ok:
            reject_counts[reason or "filtered"] += 1
            continue

        author_obj["ff_ratio"] = round(author_obj["following_count"] / max(author_obj["followers_count"], 1), 2)
        author_obj["has_mutual_bio"] = has_mutual_bio
        author_obj["mutual_bio_keyword"] = mutual_kw
        author_obj["bio_len"] = len(desc)
        author_obj["post_age_hours"] = round(max(0, now - last_ts) / 3600.0, 2) if last_ts > 0 else None
        eligible.append(author_obj)
        lang_counts[primary_lang] += 1
        if has_mutual_bio:
            mutual_bio_count += 1

    eligible.sort(key=lambda d: (0 if d["has_mutual_bio"] else 1, -safe_int(d.get("last_active_ts"), 0)))
    funnel = {
        "scanned_7d_authors": scanned_count,
        "excluded_already_recorded": excluded_count,
        "reject_counts": dict(reject_counts),
        "eligible_total": len(eligible),
        "eligible_mutual_bio": mutual_bio_count,
        "eligible_top_langs": dict(lang_counts.most_common(5)),
    }
    cap = max(1, int(limit)) if limit and int(limit) > 0 else len(eligible)
    return eligible[:cap], len(eligible), funnel


# ==============================================================================
# 四、按需小号补采与单账号关注执行
# ==============================================================================

def _datetime_to_epoch(val: Any) -> int:
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, datetime):
        return int((val if val.tzinfo is not None else val.replace(tzinfo=timezone.utc)).timestamp())
    return 0


def refresh_harvest_if_needed(
    db: Any,
    *,
    accounts: Sequence[Dict[str, Any]],
    force_fetch: bool = False,
    allow_browser_harvest: bool = True,
    collector_cooldown_until: Optional[float] = None,
    config: Optional[Dict[str, Any]] = None,
    logger: Any = None,
) -> Dict[str, Any]:
    """若库内近 50 分钟已有浏览器采集且合格池充足则直接复用；否则仅用小号 `x_xiaohao` 补采一次。"""
    log = logger or get_growth_logger("x_growth_follower")
    cfg = {**_default_filter_config(), **(config or {})}
    now_ts = int(time.time())
    collector_dir = str(cfg["collector_user_data_dir"])
    assert_safe_collector_dir(collector_dir, accounts=accounts)

    posts_col = db.get_collection(HARVESTED_POSTS_COLLECTION)
    latest_doc = posts_col.find_one(
        {"source": "x", "$or": [{"metadata.platform_extra.category": {"$regex": "^x_browser_"}}, {"metadata.category": {"$regex": "^x_browser_"}}]},
        {"db_update_time": 1, "_id": 0},
        sort=[("db_update_time", -1)],
    )
    last_ts = _datetime_to_epoch((latest_doc or {}).get("db_update_time"))
    age_sec = max(0, now_ts - last_ts) if last_ts > 0 else None

    primary_acc = accounts[0] if accounts else {"name": "x_myself", "screen_name": ""}
    _, pool_count, _ = find_follow_candidates(db, primary_acc, limit=1, config=cfg, now_ts=now_ts)

    reuse_win = max(0, safe_int(cfg.get("collector_reuse_window_seconds"), 3000))
    min_trigger = max(0, safe_int(cfg.get("min_candidate_pool_trigger"), 30))
    if not allow_browser_harvest:
        return {"fetch_mode": "dry_run_db_direct", "last_capture_age_sec": age_sec, "pool_count": pool_count, "collector_cooldown_until": collector_cooldown_until}
    if (not force_fetch) and (age_sec is not None) and (age_sec <= reuse_win) and (pool_count >= min_trigger):
        log.info(f"[AutoFollower/复用采集] 距上次浏览器采集仅 {age_sec}s (<= {reuse_win}s) 且合格候选={pool_count}人，免开小号直接执行。")
        return {"fetch_mode": "reused_recent_harvest", "last_capture_age_sec": age_sec, "pool_count": pool_count, "collector_cooldown_until": collector_cooldown_until}
    if collector_cooldown_until and collector_cooldown_until > time.time():
        rem = int(collector_cooldown_until - time.time())
        log.warning(f"⚠️ [AutoFollower/小号冷却降级] 小号仍在冷却（剩余 {rem}s），直接复用库内 {pool_count} 名候选。")
        return {"fetch_mode": "collector_cooldown_fallback", "last_capture_age_sec": age_sec, "pool_count": pool_count, "collector_cooldown_until": collector_cooldown_until}

    log.info(f"[AutoFollower/触发补采] 唤起采集小号刷新帖子池 (force={force_fetch}, age={age_sec}s, pool={pool_count})...")
    col_res = collect_and_save_posts(user_data_dir=collector_dir, browser_visible=bool(cfg.get("browser_visible", False)))
    st = str(col_res.get("status") or "ok")
    next_cd = collector_cooldown_until
    if st in ("locked_challenge", "suspended", "not_logged_in"):
        cd_sec = max(600, safe_int(cfg.get("risk_cooldown_seconds"), 21600))
        next_cd = time.time() + cd_sec
        log.error(f"🚨 [AutoFollower/小号阻断(冷却{cd_sec}s)] {col_res.get('account_warning')} -> 降级复用库内候选")
    elif st != "ok":
        log.warning(f"⚠️ [AutoFollower/小号采集告警] status={st} | {col_res.get('account_warning')}")
    else:
        next_cd = None

    _, new_pool, _ = find_follow_candidates(db, primary_acc, limit=1, config=cfg, now_ts=int(time.time()))
    return {"fetch_mode": "fresh_browser_harvest" if st == "ok" else f"collector_{st}_fallback", "last_capture_age_sec": age_sec, "pool_count": new_pool, "collector_cooldown_until": next_cd}


def count_recent_follows(db: Any, account_name: str, *, window_seconds: int = 86400, now_ts: Optional[int] = None) -> int:
    _ensure_record_indexes(db)
    cutoff = int(now_ts if now_ts is not None else time.time()) - max(1, int(window_seconds))
    return int(db.get_collection(FOLLOW_RECORDS_COLLECTION).count_documents({"account_name": account_name, "status": "followed", "followed_at_ts": {"$gte": cutoff}}))


def record_follow_outcome(
    db: Any,
    *,
    account_name: str,
    account_handle: str,
    account_dir: str,
    target_profile: Dict[str, Any],
    status: str,
    error: Optional[str] = None,
    followed_back: bool = False,
    candidate_rank: Optional[int] = None,
    eligible_pool_size: Optional[int] = None,
    now_ts: Optional[int] = None,
) -> None:
    """持久化单次关注/跳过结果的完整平面化埋点，便于回关核验与策略复盘。"""
    _ensure_record_indexes(db)
    now = int(now_ts if now_ts is not None else time.time())
    now_str = _ts_to_iso_cn(now)
    target_handle = _clean_handle(target_profile.get("screen_name"))
    target_lower = target_handle.lower()
    if not target_lower:
        return

    fc = safe_int(target_profile.get("followers_count"), 0)
    fg = safe_int(target_profile.get("following_count"), 0)
    langs = _normalize_langs(target_profile.get("languages"))
    primary_lang = str(target_profile.get("primary_lang") or (langs[0] if langs else "unknown")).lower()
    desc_str = str(target_profile.get("description") or "")
    mb_match = MUTUAL_BIO_RE.search(desc_str)
    has_mb = bool(target_profile.get("has_mutual_bio") or mb_match)
    mb_kw = target_profile.get("mutual_bio_keyword") or (mb_match.group(0) if mb_match else None)
    last_post_ts = safe_int(target_profile.get("last_active_ts"), 0)
    post_age_h = target_profile.get("post_age_hours")
    if post_age_h is None and last_post_ts > 0:
        post_age_h = round(max(0, now - last_post_ts) / 3600.0, 2)

    set_fields: Dict[str, Any] = {
        "strategy_version": STRATEGY_VERSION,
        "account_name": account_name,
        "account_handle": _clean_handle(account_handle),
        "account_dir": str(account_dir or ""),
        "target_handle": target_handle,
        "target_handle_lower": target_lower,
        "target_user_id": str(target_profile.get("user_id") or "").strip(),
        "followers_count": fc,
        "following_count": fg,
        "ff_ratio": target_profile.get("ff_ratio") if target_profile.get("ff_ratio") is not None else round(fg / max(fc, 1), 2),
        "statuses_count": safe_int(target_profile.get("statuses_count"), 0),
        "primary_lang": primary_lang,
        "languages": langs,
        "has_mutual_bio": has_mb,
        "mutual_bio_keyword": mb_kw,
        "bio_len": safe_int(target_profile.get("bio_len"), len(desc_str)),
        "last_post_ts": last_post_ts,
        "post_age_hours_at_follow": post_age_h,
        "candidate_rank": int(candidate_rank) if candidate_rank is not None else None,
        "eligible_pool_size": int(eligible_pool_size) if eligible_pool_size is not None else None,
        "status": status,
        "last_error": str(error)[:240] if error else None,
        "updated_at": now_str,
        "updated_at_ts": now,
    }
    if status == "followed":
        set_fields["followed_at"] = now_str
        set_fields["followed_at_ts"] = now
        set_fields["followed_hour_cn"] = datetime.fromtimestamp(now, tz=TZ_CN).hour
    if followed_back:
        set_fields["followed_back"] = True
        set_fields["followed_back_at"] = now_str
        set_fields["followed_back_at_ts"] = now

    set_on_insert: Dict[str, Any] = {"created_at": now_str, "created_at_ts": now}
    if not followed_back:
        set_on_insert.update(
            {
                "follow_back_check_count": 0,
                "last_follow_back_check_at": None,
                "last_follow_back_check_ts": None,
                "followed_back": False,
                "followed_back_at": None,
                "followed_back_at_ts": None,
                "follow_back_latency_hours": None,
            }
        )
    db.get_collection(FOLLOW_RECORDS_COLLECTION).update_one(
        {"account_name": account_name, "target_handle_lower": target_lower},
        {"$set": set_fields, "$setOnInsert": set_on_insert},
        upsert=True,
    )


def mark_follow_backs(db: Any, *, account_name: str, follower_handles: Iterable[str], now_ts: Optional[int] = None) -> Dict[str, Any]:
    _ensure_record_indexes(db)
    now = int(now_ts if now_ts is not None else time.time())
    now_str = _ts_to_iso_cn(now)
    observed = {_clean_handle(h).lower() for h in (follower_handles or []) if _clean_handle(h)}
    rec_col = db.get_collection(FOLLOW_RECORDS_COLLECTION)
    pending = list(
        rec_col.find(
            {"account_name": account_name, "status": {"$in": ["followed", "already_following"]}, "followed_back": {"$ne": True}},
            {
                "target_handle_lower": 1,
                "target_handle": 1,
                "followed_at_ts": 1,
                "created_at_ts": 1,
                "follow_back_check_count": 1,
                "followers_count": 1,
                "ff_ratio": 1,
                "primary_lang": 1,
                "post_age_hours_at_follow": 1,
                "has_mutual_bio": 1,
                "follow_telemetry": 1,
                "_id": 0,
            },
        )
    )

    new_handles: List[str] = []
    matched_details: List[Dict[str, Any]] = []
    for doc in pending:
        th_low = _clean_handle(doc.get("target_handle_lower")).lower()
        if not th_low:
            continue
        next_check_cnt = safe_int(doc.get("follow_back_check_count"), 0) + 1
        update_fields: Dict[str, Any] = {
            "follow_back_check_count": next_check_cnt,
            "last_follow_back_check_at": now_str,
            "last_follow_back_check_ts": now,
        }
        if th_low in observed:
            base_ts = safe_int(doc.get("followed_at_ts") or doc.get("created_at_ts"), now)
            latency_h = round(max(0, now - base_ts) / 3600.0, 2)
            target_display = str(doc.get("target_handle") or th_low)
            new_handles.append(target_display)
            update_fields.update(
                {
                    "followed_back": True,
                    "followed_back_at": now_str,
                    "followed_back_at_ts": now,
                    "follow_back_latency_hours": latency_h,
                    "updated_at": now_str,
                    "updated_at_ts": now,
                }
            )
            tel = doc.get("follow_telemetry") or {}
            matched_details.append(
                {
                    "target_handle": target_display,
                    "latency_hours": latency_h,
                    "followers_count": doc.get("followers_count") if doc.get("followers_count") is not None else tel.get("followers_count"),
                    "ff_ratio": doc.get("ff_ratio") if doc.get("ff_ratio") is not None else tel.get("ff_ratio"),
                    "primary_lang": doc.get("primary_lang") or ((tel.get("languages") or [None])[0]) or "unknown",
                    "post_age_hours_at_follow": doc.get("post_age_hours_at_follow") if doc.get("post_age_hours_at_follow") is not None else tel.get("activity_age_hours"),
                    "has_mutual_bio": bool(doc.get("has_mutual_bio") if "has_mutual_bio" in doc else tel.get("has_mutual_bio")),
                }
            )
        rec_col.update_one({"account_name": account_name, "target_handle_lower": th_low}, {"$set": update_fields})

    return {
        "observed_followers": len(observed),
        "checked_pending": len(pending),
        "newly_followed_back": len(new_handles),
        "new_handles": new_handles,
        "matched_details": matched_details,
    }


def sync_follow_backs_in_session(page: Any, self_handle: str, *, db: Any, account_name: str, logger: Any) -> Dict[str, Any]:
    clean_self = _clean_handle(self_handle)
    if not clean_self:
        return {"observed_followers": 0, "checked_pending": 0, "newly_followed_back": 0, "new_handles": [], "matched_details": []}
    observed: Set[str] = set()

    def _on_resp(resp: Any) -> None:
        try:
            if resp.status != 200 or not _RE_FOLLOWERS_GQL.search(resp.url or ""):
                return
            stack = [resp.json()]
            while stack:
                cur = stack.pop()
                if isinstance(cur, dict):
                    if cur.get("__typename") == "User":
                        sn = _clean_handle((cur.get("core") or {}).get("screen_name") or (cur.get("legacy") or {}).get("screen_name") or cur.get("screen_name")).lower()
                        if sn:
                            observed.add(sn)
                    stack.extend(cur.values())
                elif isinstance(cur, list):
                    stack.extend(cur)
        except Exception:
            pass

    page.on("response", _on_resp)
    try:
        page.goto(f"https://x.com/{clean_self}/followers", timeout=40000, wait_until="domcontentloaded")
        try:
            page.locator('div[data-testid="UserCell"], div[data-testid="empty_state_header_text"]').first.wait_for(state="visible", timeout=12000)
        except Exception:
            pass
        page.wait_for_timeout(random.randint(1200, 2000))
        _dismiss_overlays(page, aggressive=False, desc="x-follow-back-sync")
        try:
            page.mouse.wheel(0, random.randint(1200, 1800))
            page.wait_for_timeout(1200)
            for href in page.locator('[data-testid="primaryColumn"] div[data-testid="UserCell"] a[href^="/"]').evaluate_all("els => els.map(e => e.getAttribute('href') || '')") or []:
                seg = str(href or "").strip().lstrip("/")
                if seg and "/" not in seg and seg.lower() not in ("i", "home", "explore", "notifications", "messages", "search"):
                    observed.add(seg.lower())
        except Exception:
            pass
    except Exception as exc:
        logger.warning(f"[AutoFollower/回关核验跳过] @{clean_self}: {exc}")
    finally:
        try:
            page.remove_listener("response", _on_resp)
        except Exception:
            pass

    observed.discard(clean_self.lower())
    res = mark_follow_backs(db, account_name=account_name, follower_handles=observed)
    for d in res.get("matched_details") or []:
        logger.info(
            f"🎉 [AutoFollower/回关命中 | {account_name}] @{d['target_handle']} 已回关！耗时={d['latency_hours']}h | "
            f"关注时特征: 粉={d.get('followers_count')} ratio={d.get('ff_ratio')} "
            f"推龄={d.get('post_age_hours_at_follow')}h 语种={d.get('primary_lang')} 互关简介={d.get('has_mutual_bio')}"
        )

    rec_col = db.get_collection(FOLLOW_RECORDS_COLLECTION)
    total_f = int(rec_col.count_documents({"account_name": account_name, "status": "followed"}))
    total_fb = int(rec_col.count_documents({"account_name": account_name, "status": "followed", "followed_back": True}))
    rate = round((total_fb / total_f) * 100.0, 1) if total_f else 0.0
    logger.info(
        f"[AutoFollower/回关核验 | {account_name}] 扫描粉丝={res['observed_followers']}人 | "
        f"本次待核验={res['checked_pending']}人 | 新确认回关={res['newly_followed_back']}人 | "
        f"累计回关={total_fb}/{total_f} ({rate}%)"
    )
    return res


def follow_one_in_session(
    page: Any,
    sniffer: XPassiveGraphQLSniffer,
    candidate: Dict[str, Any],
    *,
    config: Dict[str, Any],
    logger: Any,
) -> Dict[str, Any]:
    """打开候选人主页做实时二次核验并执行关注（同时校验 API 返回与按钮/Toast 状态）。"""
    handle = _clean_handle(candidate.get("screen_name"))
    h_low = handle.lower()
    prev_u = sniffer.op_counts.get("UserByScreenName", 0)
    resp = page.goto(f"https://x.com/{handle}", timeout=45000, wait_until="domcontentloaded")
    sniffer.wait_for_op_increment(page, ("UserByScreenName",), prev_u, timeout_ms=10000)
    try:
        page.locator('div[data-testid="UserName"], div[data-testid="empty_state_header_text"], input[autocomplete="username"], a[data-testid="loginButton"]').first.wait_for(state="visible", timeout=15000)
    except Exception:
        pass
    page.wait_for_timeout(random.randint(1200, 2200))
    _dismiss_overlays(page, aggressive=False, desc="x-follow-profile")
    check_for_crash(page)

    st, msg = inspect_x_page_account_status(page)
    if st != "ok":
        return {"outcome": "halted", "halt_status": st, "halt_warning": msg, "live_profile": candidate, "error": msg}
    if resp and resp.status in (401, 403, 429):
        return {"outcome": "halted", "halt_status": "not_logged_in" if resp.status == 401 else "rate_limited", "halt_warning": f"HTTP {resp.status}", "live_profile": candidate, "error": f"http_{resp.status}"}
    if resp and resp.status >= 400:
        return {"outcome": "skipped_unavailable", "live_profile": candidate, "error": f"http_{resp.status}"}

    try:
        empty_hdr = page.locator('div[data-testid="empty_state_header_text"]').first
        if empty_hdr.count() > 0 and empty_hdr.is_visible():
            return {"outcome": "skipped_unavailable", "live_profile": candidate, "error": (empty_hdr.inner_text() or "")[:120]}
    except Exception:
        pass

    live_prof = sniffer.users_by_handle.get(h_low)
    merged = {**candidate, **(live_prof or {})}
    if isinstance(live_prof, dict):
        if bool(live_prof.get("is_protected")):
            return {"outcome": "skipped_protected", "live_profile": merged, "error": "protected_live"}
        if bool(live_prof.get("is_following")):
            return {"outcome": "already_mutual" if bool(live_prof.get("is_followed_by")) else "already_following", "live_profile": merged, "error": None}
        if bool(config.get("require_verified", True)) and not (live_prof.get("is_blue_verified") or live_prof.get("is_verified")):
            return {"outcome": "skipped_ineligible_live", "live_profile": merged, "error": "not_verified_live"}
        live_fc, live_fg = safe_int(live_prof.get("followers_count"), 0), safe_int(live_prof.get("following_count"), 0)
        cap = safe_int(config.get("max_following_cap"), 4500)
        if cap > 0 and live_fg > cap and not (live_fc >= cap and (live_fg / max(live_fc, 1)) <= 1.05):
            return {"outcome": "skipped_ineligible_live", "live_profile": merged, "error": "near_5k_follow_cap_live"}

    primary_col = page.locator('[data-testid="primaryColumn"]').first
    header_pl = primary_col.locator('div[data-testid="placementTracking"]')
    if header_pl.locator('button[data-testid$="-unfollow"]').first.count() > 0 and header_pl.locator('button[data-testid$="-unfollow"]').first.is_visible():
        return {"outcome": "already_mutual" if bool((live_prof or {}).get("is_followed_by")) else "already_following", "live_profile": merged, "error": None}
    if header_pl.locator('button[data-testid$="-cancel"]').first.count() > 0 and header_pl.locator('button[data-testid$="-cancel"]').first.is_visible():
        return {"outcome": "skipped_protected", "live_profile": merged, "error": "pending_cancel_btn"}

    follow_btn = header_pl.locator('button[data-testid$="-follow"]').first
    if follow_btn.count() == 0:
        follow_btn = primary_col.locator('button[data-testid$="-follow"]').first
    if follow_btn.count() == 0 or not follow_btn.is_visible():
        return {"outcome": "failed", "live_profile": merged, "error": "follow_btn_not_found"}

    api_state: Dict[str, Any] = {"ok": False, "halt": None, "skip": None, "error": None}

    def _on_follow_resp(r: Any) -> None:
        try:
            if not _RE_FRIENDSHIP_API.search(r.url or ""):
                return
            body = None
            try:
                body = r.json()
            except Exception:
                pass
            errors = body.get("errors") if isinstance(body, dict) else None
            if isinstance(errors, list) and errors:
                err = errors[0] if isinstance(errors[0], dict) else {"message": str(errors[0])}
                code, emsg = safe_int(err.get("code"), 0), str(err.get("message") or errors)
                if code == 326:
                    api_state["halt"] = ("locked_challenge", f"code={code}: {emsg}")
                elif code in (88, 161):
                    api_state["halt"] = ("rate_limited", f"code={code}: {emsg}")
                elif code == 160:
                    api_state["skip"] = ("skipped_protected", emsg)
                elif code in (108, 162):
                    api_state["skip"] = ("skipped_unavailable", emsg)
                else:
                    api_state["error"] = f"code={code}: {emsg}"
                return
            if r.status in (401, 403, 429):
                api_state["halt"] = ("not_logged_in" if r.status == 401 else "rate_limited", f"HTTP {r.status}")
            elif r.status == 200:
                api_state["ok"] = True
        except Exception:
            pass

    page.on("response", _on_follow_resp)
    try:
        _robust_click(follow_btn)
        deadline = time.time() + 10.0
        while time.time() < deadline and not (api_state["halt"] or api_state["skip"] or api_state["ok"]):
            try:
                if header_pl.locator('button[data-testid$="-unfollow"]').first.is_visible():
                    api_state["ok"] = True
                    break
                if header_pl.locator('button[data-testid$="-cancel"]').first.is_visible():
                    api_state["skip"] = ("skipped_protected", "follow_requested_pending")
                    break
                toast = page.locator('div[data-testid="toast"]').first
                if toast.count() > 0 and toast.is_visible():
                    txt = (toast.inner_text() or "").strip()
                    if any(tok in txt.lower() for tok in _TOAST_RATE_LIMIT_TOKENS):
                        api_state["halt"] = ("rate_limited", txt[:120])
                        break
            except Exception:
                pass
            page.wait_for_timeout(300)
    finally:
        try:
            page.remove_listener("response", _on_follow_resp)
        except Exception:
            pass

    if api_state["halt"]:
        h_st, h_msg = api_state["halt"]
        logger.error(f"🚨 [AutoFollower/风控熔断] @{handle} | {h_st}: {h_msg}")
        return {"outcome": "halted", "halt_status": h_st, "halt_warning": h_msg, "live_profile": merged, "error": h_msg}
    if api_state["skip"]:
        return {"outcome": api_state["skip"][0], "live_profile": merged, "error": api_state["skip"][1]}
    if api_state["ok"]:
        page.wait_for_timeout(random.randint(800, 1400))
        return {"outcome": "followed", "live_profile": merged, "error": None}
    return {"outcome": "failed", "live_profile": merged, "error": api_state["error"] or "follow_unconfirmed"}


def run_account_follow(
    account_cfg: Dict[str, Any],
    *,
    db: Any,
    dry_run: bool = False,
    force_sync_followbacks: bool = False,
    follow_back_last_sync: Optional[Dict[str, float]] = None,
    config: Optional[Dict[str, Any]] = None,
    logger: Any = None,
) -> Dict[str, Any]:
    """执行单个配置账号的本轮候选挑选、回关核验与关注。"""
    log = logger or get_growth_logger("x_growth_follower")
    cfg = {**_default_filter_config(), **(config or {}), **account_cfg}
    acc_name, acc_handle, session_path = str(account_cfg["name"]), _clean_handle(account_cfg.get("screen_name")), str(account_cfg["user_data_dir"])
    max_per_run = max(1, safe_int(cfg.get("max_follow_per_run"), DEFAULT_MAX_FOLLOW_PER_RUN))
    max_per_day = max(0, safe_int(cfg.get("max_follow_per_day"), DEFAULT_MAX_FOLLOW_PER_DAY))
    follows_24h = count_recent_follows(db, acc_name, window_seconds=86400)

    if (not dry_run) and max_per_day > 0 and follows_24h >= max_per_day:
        log.info(f"⏸️ [AutoFollower/每日上限 | {acc_name}] 近 24h 已关注 {follows_24h}/{max_per_day} 人，本轮跳过。")
        return {"account_name": acc_name, "screen_name": acc_handle, "user_data_dir": session_path, "status": "daily_limit_reached", "dry_run": False, "warning": None, "follows_24h": follows_24h, "max_follow_per_day": max_per_day, "eligible_count": 0, "followed_count": 0, "followed_items": []}

    target_limit = min(max_per_run, max(1, max_per_day - follows_24h)) if (not dry_run and max_per_day > 0) else max_per_run
    candidates, total_eligible, funnel = find_follow_candidates(db, account_cfg, limit=max(target_limit * 3, 18), config=cfg)

    reject_str = ", ".join(f"{k}={v}" for k, v in sorted(funnel["reject_counts"].items(), key=lambda x: -x[1])[:5]) or "无"
    langs_str = ", ".join(f"{k}:{v}" for k, v in funnel["eligible_top_langs"].items()) or "无"
    log.info(
        f"📊 [AutoFollower/候选漏斗 | {acc_name}] 7天活跃={funnel['scanned_7d_authors']} | "
        f"已处理跳过={funnel['excluded_already_recorded']} | 筛除({reject_str}) -> "
        f"合格={total_eligible}人 (互关简介={funnel['eligible_mutual_bio']}, 语种={langs_str})"
    )

    if dry_run:
        preview = [
            {
                "handle": f"@{c['screen_name']}",
                "followers": c["followers_count"],
                "following": c["following_count"],
                "ff_ratio": c["ff_ratio"],
                "primary_lang": c.get("primary_lang"),
                "post_age_hours": c.get("post_age_hours"),
                "has_mutual_bio": c["has_mutual_bio"],
                "mutual_bio_keyword": c.get("mutual_bio_keyword"),
                "bio": str(c.get("description") or "").replace("\n", " ")[:60],
            }
            for c in candidates[:target_limit]
        ]
        for rank_idx, it in enumerate(preview, start=1):
            kw = it.get("mutual_bio_keyword") or "无"
            log.info(
                f"[AutoFollower/预览 | {acc_name}] #{rank_idx}/{target_limit} {it['handle']} | "
                f"粉={it['followers']} 关注={it['following']} ratio={it['ff_ratio']} | "
                f"推龄={it.get('post_age_hours')}h | 语种={it.get('primary_lang')} | 互关词={kw} | {it['bio']!r}"
            )
        return {"account_name": acc_name, "screen_name": acc_handle, "user_data_dir": session_path, "status": "ok", "dry_run": True, "warning": None, "follows_24h": follows_24h, "max_follow_per_day": max_per_day, "eligible_count": total_eligible, "funnel": funnel, "followed_count": len(preview), "followed_items": preview}

    if not os.path.isdir(session_path):
        warn = f"账号 {acc_name} (@{acc_handle}) 会话目录不存在: {session_path}，请先运行 `python x_growth/auto_follower.py --login {acc_name}` 登录"
        log.warning(f"⚠️ [AutoFollower/会话缺失] {warn}")
        return {"account_name": acc_name, "screen_name": acc_handle, "user_data_dir": session_path, "status": "session_missing", "dry_run": False, "warning": warn, "follows_24h": follows_24h, "max_follow_per_day": max_per_day, "eligible_count": total_eligible, "funnel": funnel, "followed_count": 0, "followed_items": []}

    last_fb = (follow_back_last_sync or {}).get(acc_name, 0.0)
    should_sync_fb = bool(force_sync_followbacks) or ((time.time() - float(last_fb)) >= safe_int(cfg.get("follow_back_sync_interval_seconds"), 14400))
    if not candidates and not should_sync_fb:
        log.info(f"ℹ️ [AutoFollower/暂无候选 | {acc_name}] 当前无可关注候选，本轮免开浏览器。")
        return {"account_name": acc_name, "screen_name": acc_handle, "user_data_dir": session_path, "status": "ok", "dry_run": False, "warning": None, "follows_24h": follows_24h, "max_follow_per_day": max_per_day, "eligible_count": 0, "funnel": funnel, "followed_count": 0, "followed_items": []}

    sniffer = XPassiveGraphQLSniffer()
    followed_items: List[Dict[str, Any]] = []
    skip_reasons: Counter = Counter()
    halt_status: Optional[str] = None
    halt_warning: Optional[str] = None
    fb_sync: Optional[Dict[str, Any]] = None

    with persistent_browser_session(
        session_path,
        debug=bool(cfg.get("browser_visible", False)),
        proxy=get_proxy_url(),
        use_stealth_chrome=True,
        reuse_existing_page=True,
        default_timeout_ms=45000,
        navigation_timeout_ms=45000,
        install_guard=True,
    ) as (context, page):
        page.on("response", sniffer.on_response)
        try:
            page.goto(X_HOME_URL, timeout=45000, wait_until="domcontentloaded")
            _wait_x_home_ready(page, timeout_ms=18000)
            page.wait_for_timeout(random.randint(1500, 2400))
            _dismiss_overlays(page, aggressive=False, desc="x-follow-home")
            check_for_crash(page)

            acc_st, acc_msg = inspect_x_page_account_status(page)
            if acc_st != "ok":
                log.error(f"🚨 [AutoFollower/账号异常 | {acc_name}] status={acc_st} | {acc_msg}")
                return {"account_name": acc_name, "screen_name": acc_handle, "user_data_dir": session_path, "status": acc_st, "dry_run": False, "warning": acc_msg, "follows_24h": follows_24h, "max_follow_per_day": max_per_day, "eligible_count": total_eligible, "funnel": funnel, "followed_count": 0, "followed_items": []}

            _, live_handle = extract_logged_in_identity(context, page)
            eff_handle = live_handle or acc_handle

            if should_sync_fb and eff_handle:
                fb_sync = sync_follow_backs_in_session(page, eff_handle, db=db, account_name=acc_name, logger=log)
                if follow_back_last_sync is not None:
                    follow_back_last_sync[acc_name] = time.time()

            self_prof = sniffer.users_by_handle.get(eff_handle.lower()) if eff_handle else None
            self_cap = safe_int(cfg.get("self_following_safety_cap"), 4800)
            if isinstance(self_prof, dict) and self_cap > 0:
                s_fg, s_fc = safe_int(self_prof.get("following_count"), 0), safe_int(self_prof.get("followers_count"), 0)
                if s_fg >= self_cap and (s_fg / max(s_fc, 1)) > 1.05:
                    warn = f"己方 @{eff_handle} 关注数已达 {s_fg}/{self_cap} (粉={s_fc})，触发防 5000 卡死保护"
                    log.warning(f"⏸️ [AutoFollower/上限保护] {warn}")
                    return {"account_name": acc_name, "screen_name": eff_handle, "user_data_dir": session_path, "status": "self_cap_reached", "dry_run": False, "warning": warn, "follows_24h": follows_24h, "max_follow_per_day": max_per_day, "eligible_count": total_eligible, "funnel": funnel, "followed_count": 0, "followed_items": [], "follow_back_sync": fb_sync}

            for idx, cand in enumerate(candidates):
                if len(followed_items) >= target_limit:
                    break
                if idx > 0:
                    _sleep_jitter(cfg.get("action_delay_range") or ACTION_DELAY_RANGE)

                cand_rank = idx + 1
                res = follow_one_in_session(page, sniffer, cand, config=cfg, logger=log)
                outcome = res["outcome"]
                if outcome == "halted":
                    halt_status, halt_warning = res.get("halt_status") or "rate_limited", res.get("halt_warning") or res.get("error")
                    break

                live_prof = res.get("live_profile") or cand
                record_follow_outcome(
                    db,
                    account_name=acc_name,
                    account_handle=eff_handle,
                    account_dir=session_path,
                    target_profile=live_prof,
                    status=outcome,
                    error=res.get("error"),
                    followed_back=(outcome == "already_mutual"),
                    candidate_rank=cand_rank,
                    eligible_pool_size=total_eligible,
                )
                if outcome == "followed":
                    cur_24h = follows_24h + len(followed_items) + 1
                    kw_or_none = cand.get("mutual_bio_keyword") or "无"
                    followed_items.append(
                        {
                            "handle": f"@{cand['screen_name']}",
                            "followers": live_prof.get("followers_count"),
                            "following": live_prof.get("following_count"),
                            "ff_ratio": cand.get("ff_ratio"),
                            "primary_lang": cand.get("primary_lang"),
                            "post_age_hours": cand.get("post_age_hours"),
                            "has_mutual_bio": cand.get("has_mutual_bio"),
                            "mutual_bio_keyword": cand.get("mutual_bio_keyword"),
                        }
                    )
                    log.info(
                        f"✅ [AutoFollower/关注成功 | @{eff_handle}] #{cand_rank}/{target_limit} @{cand['screen_name']} | "
                        f"粉={live_prof.get('followers_count')} 关注={live_prof.get('following_count')} ratio={cand.get('ff_ratio')} | "
                        f"推龄={cand.get('post_age_hours')}h | 语种={cand.get('primary_lang') or 'unknown'} | "
                        f"互关词={kw_or_none} | 24h={cur_24h}/{max_per_day}"
                    )
                else:
                    skip_reasons[outcome] += 1
                    log.info(
                        f"⏭️ [AutoFollower/跳过 | @{eff_handle}] @{cand['screen_name']} | "
                        f"reason={outcome} ({res.get('error') or ''}) | 粉={live_prof.get('followers_count')} "
                        f"ratio={cand.get('ff_ratio')} 语种={cand.get('primary_lang') or 'unknown'}"
                    )

            skip_cnt = sum(skip_reasons.values())
            skip_desc = ", ".join(f"{k}={v}" for k, v in skip_reasons.items()) or "无"
            log.info(
                f"📝 [AutoFollower/本轮小结 | {acc_name}] 成功={len(followed_items)} 跳过={skip_cnt}({skip_desc}) | "
                f"24h={follows_24h + len(followed_items)}/{max_per_day}"
            )
        finally:
            try:
                page.remove_listener("response", sniffer.on_response)
            except Exception:
                pass

    return {"account_name": acc_name, "screen_name": acc_handle, "user_data_dir": session_path, "status": halt_status or "ok", "dry_run": False, "warning": halt_warning, "follows_24h": follows_24h + len(followed_items), "max_follow_per_day": max_per_day, "eligible_count": total_eligible, "funnel": funnel, "followed_count": len(followed_items), "followed_items": followed_items, "follow_back_sync": fb_sync}


# ==============================================================================
# 五、运维状态看板、心跳落盘与每小时守护循环
# ==============================================================================

def _bucket_rate(items: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    n = len(items)
    fb = sum(1 for d in items if d.get("followed_back") is True)
    return {"followed": n, "followed_back": fb, "rate_pct": round((fb / n) * 100.0, 1) if n else 0.0}


def _compute_follow_analytics(followed_docs: Sequence[Dict[str, Any]], now_ts: int) -> Dict[str, Any]:
    """按互关简介、语种、粉丝段、关注比与推文新鲜度汇总回关效果，直接支撑策略评估与调参。"""
    total = _bucket_rate(followed_docs)
    mature_docs = [
        d for d in followed_docs
        if safe_int(d.get("followed_at_ts"), 0) > 0 and (now_ts - safe_int(d.get("followed_at_ts"), 0)) >= 86400
    ]
    mature = _bucket_rate(mature_docs)
    latencies = [
        float(d["follow_back_latency_hours"])
        for d in followed_docs
        if d.get("followed_back") is True and d.get("follow_back_latency_hours") is not None
    ]
    median_lat = round(statistics.median(latencies), 2) if latencies else None

    bio_buckets: Dict[str, List[Dict[str, Any]]] = {"mutual_bio": [], "normal": []}
    lang_buckets: Dict[str, List[Dict[str, Any]]] = {"en": [], "ja": [], "zh": [], "other": []}
    fc_buckets: Dict[str, List[Dict[str, Any]]] = {"100~499": [], "500~1499": [], "1500~3000": []}
    ratio_buckets: Dict[str, List[Dict[str, Any]]] = {"0.8~1.0": [], "1.0~1.5": [], "1.5~3.0": []}
    age_buckets: Dict[str, List[Dict[str, Any]]] = {"<=24h": [], "24~72h": [], ">72h": []}

    for d in followed_docs:
        tel = d.get("follow_telemetry") or {}
        has_mb = bool(d.get("has_mutual_bio") if "has_mutual_bio" in d else tel.get("has_mutual_bio"))
        bio_buckets["mutual_bio" if has_mb else "normal"].append(d)

        raw_langs = d.get("languages") or tel.get("languages") or []
        lang = str(d.get("primary_lang") or (raw_langs[0] if raw_langs else "unknown")).lower()
        lang_buckets[lang if lang in ("en", "ja", "zh") else "other"].append(d)

        fc_raw = d.get("followers_count") if d.get("followers_count") is not None else tel.get("followers_count")
        fc = safe_int(fc_raw, 0)
        if 100 <= fc < 500:
            fc_buckets["100~499"].append(d)
        elif 500 <= fc < 1500:
            fc_buckets["500~1499"].append(d)
        elif 1500 <= fc <= 3000:
            fc_buckets["1500~3000"].append(d)

        ratio = d.get("ff_ratio") if d.get("ff_ratio") is not None else tel.get("ff_ratio")
        if ratio is not None:
            r_val = float(ratio)
            if r_val < 1.0:
                ratio_buckets["0.8~1.0"].append(d)
            elif r_val < 1.5:
                ratio_buckets["1.0~1.5"].append(d)
            else:
                ratio_buckets["1.5~3.0"].append(d)

        age_h = d.get("post_age_hours_at_follow") if d.get("post_age_hours_at_follow") is not None else tel.get("activity_age_hours")
        if age_h is None and safe_int(d.get("last_post_ts"), 0) > 0 and safe_int(d.get("followed_at_ts"), 0) > 0:
            age_h = max(0, safe_int(d.get("followed_at_ts"), 0) - safe_int(d.get("last_post_ts"), 0)) / 3600.0
        if age_h is not None:
            a_val = float(age_h)
            if a_val <= 24.0:
                age_buckets["<=24h"].append(d)
            elif a_val <= 72.0:
                age_buckets["24~72h"].append(d)
            else:
                age_buckets[">72h"].append(d)

    return {
        "total_followed": total["followed"],
        "followed_back": total["followed_back"],
        "rate_pct": total["rate_pct"],
        "mature_24h_followed": mature["followed"],
        "mature_24h_followed_back": mature["followed_back"],
        "mature_24h_rate_pct": mature["rate_pct"],
        "median_follow_back_latency_hours": median_lat,
        "by_mutual_bio": {k: _bucket_rate(v) for k, v in bio_buckets.items()},
        "by_lang": {k: _bucket_rate(v) for k, v in lang_buckets.items()},
        "by_followers": {k: _bucket_rate(v) for k, v in fc_buckets.items()},
        "by_ff_ratio": {k: _bucket_rate(v) for k, v in ratio_buckets.items()},
        "by_post_age": {k: _bucket_rate(v) for k, v in age_buckets.items()},
    }


def _fmt_bucket_line(buckets: Dict[str, Dict[str, Any]], order: Optional[Sequence[str]] = None) -> str:
    keys = list(order) if order else list(buckets.keys())
    return " | ".join(
        f"{k}={buckets[k]['followed_back']}/{buckets[k]['followed']}({buckets[k]['rate_pct']}%)"
        for k in keys
        if k in buckets
    )


def build_status_report(db: Any, accounts: Sequence[Dict[str, Any]], *, config: Optional[Dict[str, Any]] = None, now_ts: Optional[int] = None) -> Dict[str, Any]:
    _ensure_record_indexes(db)
    cfg = {**_default_filter_config(), **(config or {})}
    now = int(now_ts if now_ts is not None else time.time())
    day_cutoff = now - 86400
    rec_col = db.get_collection(FOLLOW_RECORDS_COLLECTION)

    primary_acc = accounts[0] if accounts else {"name": "x_myself", "screen_name": ""}
    _, pool_count, funnel = find_follow_candidates(db, primary_acc, limit=1, config=cfg, now_ts=now)

    acc_stats: List[Dict[str, Any]] = []
    all_followed_docs: List[Dict[str, Any]] = []
    for acc in accounts:
        name = str(acc["name"])
        max_day = safe_int(acc.get("max_follow_per_day"), DEFAULT_MAX_FOLLOW_PER_DAY)
        docs = list(rec_col.find({"account_name": name}, {"_id": 0}))
        followed = [d for d in docs if d.get("status") == "followed"]
        followed.sort(key=lambda d: safe_int(d.get("followed_at_ts"), 0), reverse=True)
        all_followed_docs.extend(followed)
        follows_24h = sum(1 for d in followed if safe_int(d.get("followed_at_ts"), 0) >= day_cutoff)
        analytics = _compute_follow_analytics(followed, now_ts=now)
        recent = [
            {
                "target": f"@{d.get('target_handle')}",
                "ff_ratio": d.get("ff_ratio") if d.get("ff_ratio") is not None else (d.get("follow_telemetry") or {}).get("ff_ratio"),
                "followed_back": bool(d.get("followed_back")),
                "followed_at": d.get("followed_at"),
            }
            for d in followed[:5]
        ]
        acc_stats.append(
            {
                "account_name": name,
                "screen_name": _clean_handle(acc.get("screen_name")),
                "follows_24h": follows_24h,
                "max_follow_per_day": max_day,
                "total_followed": analytics["total_followed"],
                "followed_back": analytics["followed_back"],
                "follow_back_rate_pct": analytics["rate_pct"],
                "analytics": analytics,
                "recent_followed": recent,
            }
        )

    return {
        "generated_at": now_iso_cn(),
        "eligible_pool_count": pool_count,
        "funnel": funnel,
        "accounts": acc_stats,
        "analytics": _compute_follow_analytics(all_followed_docs, now_ts=now),
    }


def format_status_text(report: Dict[str, Any], runtime_status: Optional[Dict[str, Any]] = None) -> str:
    lines = [
        "=" * 76,
        f"X 自动互关运维看板 | 更新时间: {report.get('generated_at', '')}",
        f"当前可关注活跃合格池: {report.get('eligible_pool_count', 0)} 人 (蓝V + 100~3000粉 + 比值0.8~3.0 + 近7天活跃)",
        "-" * 76,
    ]
    if runtime_status:
        lines.append(f"守护心跳: round=#{runtime_status.get('round_no', '?')} | 上次={runtime_status.get('last_run_at', '未知')} | 下次={runtime_status.get('next_run_at', '未排期')}")
    rt_accounts = (runtime_status or {}).get("accounts") or {}
    for acc in report.get("accounts") or []:
        rt = rt_accounts.get(acc["account_name"]) or {}
        rt_tag = f" | 状态={rt.get('status')}" + (f" ({str(rt['warning'])[:50]})" if rt.get("warning") else "") if rt else ""
        lines.append(f"[{acc['account_name']} @{acc['screen_name']}] 24h关注={acc['follows_24h']}/{acc['max_follow_per_day']} | 累计关注={acc['total_followed']} | 已回关={acc['followed_back']} ({acc['follow_back_rate_pct']}%){rt_tag}")
        for it in acc.get("recent_followed") or []:
            lines.append(f"   - {it['target']} (ratio={it.get('ff_ratio')}) {'已回关' if it.get('followed_back') else '待观察'} | {it.get('followed_at')}")

    an = report.get("analytics") or {}
    if an:
        lat_txt = f"{an['median_follow_back_latency_hours']}h" if an.get("median_follow_back_latency_hours") is not None else "暂无"
        lines.extend(
            [
                "-" * 76,
                "策略效果分析（用于评估与调参）",
                f"  总体表现: 累计={an.get('followed_back', 0)}/{an.get('total_followed', 0)} ({an.get('rate_pct', 0.0)}%) | "
                f"满24h成熟样本={an.get('mature_24h_followed_back', 0)}/{an.get('mature_24h_followed', 0)} ({an.get('mature_24h_rate_pct', 0.0)}%) | "
                f"中位回关耗时={lat_txt}",
                f"  互关简介对比: {_fmt_bucket_line(an.get('by_mutual_bio') or {}, ('mutual_bio', 'normal'))}",
                f"  分语种回关率: {_fmt_bucket_line(an.get('by_lang') or {}, ('en', 'ja', 'zh', 'other'))}",
                f"  分粉丝段回关: {_fmt_bucket_line(an.get('by_followers') or {}, ('100~499', '500~1499', '1500~3000'))}",
                f"  分关注比回关: {_fmt_bucket_line(an.get('by_ff_ratio') or {}, ('0.8~1.0', '1.0~1.5', '1.5~3.0'))}",
                f"  分推龄回关率: {_fmt_bucket_line(an.get('by_post_age') or {}, ('<=24h', '24~72h', '>72h'))}",
            ]
        )
    lines.append("=" * 76)
    return "\n".join(lines)


def _save_status_heartbeat(*, round_no: int, refresh_res: Dict[str, Any], account_results: Sequence[Dict[str, Any]], next_run_at: Optional[str], report: Dict[str, Any]) -> None:
    path = get_runtime_file_path(STATUS_RUNTIME_FILE)
    existing = read_json(path) if path.is_file() else {}
    merged = dict((existing if isinstance(existing, dict) else {}).get("accounts") or {})
    for res in account_results:
        merged[str(res["account_name"])] = {
            "screen_name": res.get("screen_name"),
            "status": res.get("status"),
            "warning": res.get("warning"),
            "follows_24h": res.get("follows_24h", 0),
            "max_follow_per_day": res.get("max_follow_per_day", 0),
            "followed_this_round": res.get("followed_count", 0),
            "updated_at": now_iso_cn(),
        }
    save_json(
        path,
        {
            "round_no": int(round_no),
            "last_run_at": now_iso_cn(),
            "next_run_at": next_run_at,
            "fetch_mode": refresh_res.get("fetch_mode"),
            "eligible_pool_count": report.get("eligible_pool_count", 0),
            "accounts": merged,
            "analytics": report.get("analytics"),
        },
    )


def run_follow_round(
    *,
    round_no: int = 1,
    accounts: Optional[Sequence[Dict[str, Any]]] = None,
    dry_run: bool = False,
    force_fetch: bool = False,
    force_sync_followbacks: bool = False,
    cooldown_until: Optional[Dict[str, float]] = None,
    follow_back_last_sync: Optional[Dict[str, float]] = None,
    collector_state: Optional[Dict[str, Optional[float]]] = None,
    config: Optional[Dict[str, Any]] = None,
    next_run_at: Optional[str] = None,
    logger: Any = None,
) -> Dict[str, Any]:
    log = logger or get_growth_logger("x_growth_follower")
    cfg = {**_default_filter_config(), **(config or {})}
    pool = list(accounts) if accounts is not None else resolve_target_accounts()
    db = gen_db_object()

    col_state = collector_state if collector_state is not None else {"cooldown_until": None}
    refresh_res = refresh_harvest_if_needed(db, accounts=pool, force_fetch=force_fetch, allow_browser_harvest=(not dry_run), collector_cooldown_until=col_state.get("cooldown_until"), config=cfg, logger=log)
    col_state["cooldown_until"] = refresh_res.get("collector_cooldown_until")

    cd_map = cooldown_until if cooldown_until is not None else {}
    cd_sec = max(600, safe_int(cfg.get("risk_cooldown_seconds"), RISK_COOLDOWN_SECONDS))
    results: List[Dict[str, Any]] = []

    for acc in pool:
        acc_dir, acc_name = acc["user_data_dir"], acc["name"]
        rem = float(cd_map.get(acc_dir, 0.0) or 0.0) - time.time()
        if (not dry_run) and rem > 0:
            log.info(f"⏸️ [AutoFollower/冷却跳过 | {acc_name}] 剩余 {int(rem)}s")
            results.append({"account_name": acc_name, "screen_name": acc.get("screen_name"), "user_data_dir": acc_dir, "status": "cooldown", "dry_run": False, "warning": f"冷却中剩余 {int(rem)}s", "follows_24h": count_recent_follows(db, acc_name), "max_follow_per_day": safe_int(acc.get("max_follow_per_day"), DEFAULT_MAX_FOLLOW_PER_DAY), "followed_count": 0, "followed_items": []})
            continue

        res = run_account_follow(acc, db=db, dry_run=dry_run, force_sync_followbacks=force_sync_followbacks, follow_back_last_sync=follow_back_last_sync, config=cfg, logger=log)
        results.append(res)
        st = str(res.get("status") or "ok")
        if (not dry_run) and st in _COOLDOWN_TRIGGER_STATUSES:
            cd_map[acc_dir] = time.time() + cd_sec
            log.error(f"⚠️ [AutoFollower/独立冷却 {cd_sec}s | {acc_name}] status={st}")
        elif (not dry_run) and st == "ok":
            cd_map.pop(acc_dir, None)

    report = build_status_report(db, pool, config=cfg)
    if not dry_run:
        _save_status_heartbeat(round_no=round_no, refresh_res=refresh_res, account_results=results, next_run_at=next_run_at, report=report)
    return {"status": "ok", "round_no": round_no, "dry_run": dry_run, "refresh": refresh_res, "accounts": results, "report": report}


def run_daemon(*, account_filter: Optional[str] = None, interval_seconds: Optional[int] = None, max_rounds: int = 0, force_fetch: bool = False, force_sync_followbacks: bool = False, overrides: Optional[Dict[str, Any]] = None) -> None:
    log = get_growth_logger("x_growth_follower")
    base_interval = max(120, int(interval_seconds or INTERVAL_SECONDS))
    j_low, j_high = sorted((max(0, int(JITTER_SECONDS[0])), max(0, int(JITTER_SECONDS[1]))))
    pool = resolve_target_accounts(account_filter=account_filter, overrides=overrides)
    if not pool:
        log.error("❌ [AutoFollower] 未找到已启用的目标账号。")
        return

    desc = ", ".join(f"{a['name']}(@{a['screen_name']}|每轮{safe_int(a.get('max_follow_per_run'), DEFAULT_MAX_FOLLOW_PER_RUN)}/日{safe_int(a.get('max_follow_per_day'), DEFAULT_MAX_FOLLOW_PER_DAY)})" for a in pool)
    log.info(f"⏰ [AutoFollower/每小时守护启动] 账号=[{desc}] | 周期={base_interval}s + 抖动({j_low}~{j_high}s)")
    cd_map: Dict[str, float] = {}
    fb_sync_map: Dict[str, float] = {}
    col_state: Dict[str, Optional[float]] = {"cooldown_until": None}

    def _step(round_no: int) -> Dict[str, Any]:
        log.info(f"========== [AutoFollower/轮次 #{round_no}] 开始 ==========")
        sleep_sec = base_interval + (random.randint(j_low, j_high) if j_high > 0 else 0)
        next_at = (datetime.now(TZ_CN) + timedelta(seconds=sleep_sec)).strftime("%Y-%m-%d %H:%M:%S")
        res = run_follow_round(round_no=round_no, accounts=pool, force_fetch=(force_fetch and round_no == 1), force_sync_followbacks=(force_sync_followbacks and round_no == 1), cooldown_until=cd_map, follow_back_last_sync=fb_sync_map, collector_state=col_state, next_run_at=next_at, logger=log)
        log.info("\n" + format_status_text(res["report"]))
        log.info(f"💤 [AutoFollower/休眠] {sleep_sec}s 后于 [{next_at}] 执行下一轮...")
        return {"next_sleep_seconds": sleep_sec}

    run_daemon_loop(_step, interval_seconds=base_interval, additive_jitter_range=(j_low, j_high), name="AutoFollower", logger_obj=log, max_rounds=max_rounds)


def main(argv: Optional[Sequence[str]] = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    parser = argparse.ArgumentParser(description="X 平台极简高互关筛选与每小时自动关注脚本")
    parser.add_argument("--once", action="store_true", help="仅运行单轮后退出")
    parser.add_argument("--dry-run", action="store_true", help="零浏览器纯查库预览待关注候选名单")
    parser.add_argument("--status", action="store_true", help="只读查看候选池、24h关注与回关率看板")
    parser.add_argument("--check-followbacks", action="store_true", help="强制本轮顺路核验粉丝列表回关")
    parser.add_argument("--force-fetch", action="store_true", help="强制唤起采集小号刷新一轮帖子池")
    parser.add_argument("--account", default=None, help="指定账号名或用户名（默认读取 TARGET_ACCOUNTS）")
    parser.add_argument("--max-follow", type=int, default=None, help="临时覆盖单轮最大关注人数")
    parser.add_argument("--interval-seconds", type=int, default=None, help="临时覆盖守护轮询周期秒数")
    parser.add_argument("--visible", action="store_true", help="显示 Chrome 窗口运行")
    parser.add_argument("--login", default=None, help="打开指定账号的可见 Chrome 供人工扫码登录")
    args = parser.parse_args(argv)

    if args.login:
        pool = resolve_target_accounts(account_filter=args.login)
        if not pool:
            print(f"❌ 未找到账号 `{args.login}`；可选: {[a['name'] for a in resolve_target_accounts()]}")
            return 2
        Path(pool[0]["user_data_dir"]).mkdir(parents=True, exist_ok=True)
        open_browser_for_manual_use(user_data_dir=pool[0]["user_data_dir"], home_url=X_HOME_URL)
        return 0

    if args.status:
        db = gen_db_object()
        pool = resolve_target_accounts(account_filter=args.account)
        rt_path = get_runtime_file_path(STATUS_RUNTIME_FILE)
        rt_data = read_json(rt_path) if rt_path.is_file() else None
        print(format_status_text(build_status_report(db, pool), runtime_status=rt_data if isinstance(rt_data, dict) else None))
        return 0

    overrides = {"max_follow_per_run": max(1, args.max_follow) if args.max_follow is not None else None, "browser_visible": True if args.visible else None}
    if args.once or args.dry_run or (not CONTINUOUS_LOOP):
        pool = resolve_target_accounts(account_filter=args.account, overrides=overrides)
        res = run_follow_round(round_no=1, accounts=pool, dry_run=bool(args.dry_run), force_fetch=bool(args.force_fetch), force_sync_followbacks=bool(args.check_followbacks))
        print(format_status_text(res["report"]))
        print(json.dumps({"refresh": res["refresh"], "accounts": res["accounts"]}, ensure_ascii=False, indent=2, default=str))
        return 0

    run_daemon(account_filter=args.account, interval_seconds=args.interval_seconds, force_fetch=bool(args.force_fetch), force_sync_followbacks=bool(args.check_followbacks), overrides=overrides)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
