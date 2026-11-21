# -*- coding: utf-8 -*-
"""
X 平台高互关潜力账号筛选、评分、暖线索识别与观测台账 (`x_growth/follow_selector.py`)

核心原则：
1. 零玄学、零 LLM 消耗：仅使用客观已验证的高概率信号（蓝V同体量、following/followers 比值、近期活跃度、明确互关声明、本号既往正向互动）；
2. 账目分明、多号互不污染：候选池统一沉淀在 `x_follow_candidates`，各运营账号的关注动作与回关验证独立记录在 `x_follow_records`；
3. 严禁触碰历史旧表（`social_media_posts`、`generated_articles`）或向 `harvested_social_posts` 写入单号关注脏字段。
"""
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from pymongo import UpdateOne

from common.common_utils import safe_int
from common.mongo_db.mongo_manager import HARVESTED_POSTS_COLLECTION
from x_growth.growth_config import (
    AUTO_COMMENT_FIELD,
    FOLLOW_CANDIDATES_COLLECTION_NAME,
    FOLLOW_RECORDS_COLLECTION_NAME,
    FOLLOWER_DEFAULTS,
    TZ_CN,
    get_configured_self_handles,
    normalize_language_code,
    now_iso_cn,
)

FOLLOW_STRATEGY_VERSION = "follow_v1_verified_ratio_active"
TIER_ORDER: Dict[str, int] = {"S": 0, "A": 1, "B": 2}

# 【保留过滤原因说明】先验排查拒互关宣言（优先级高于互关关键词，避免 "no follow back" / "不互关" 被误判为互关词）
ANTI_MUTUAL_BIO_RE = re.compile(
    r"(?:"
    r"no\s*(?:f4f|ifb|follow(?:\s+you)?\s*back)"
    r"|(?:don'?t|dont|do\s+not|not|never|won'?t|wont)\s+follow(?:\s+you)?\s*back"
    r"|one[\s-]*way\s*account"
    r"|不互关|拒互关|绝不互关|拒绝互关|不回关"
    r"|フォロバしません|相互フォロー不可|相互不可"
    r")",
    re.IGNORECASE,
)

MUTUAL_BIO_RE = re.compile(
    r"(?:"
    r"follow(?:\s+you)?\s*back"
    r"|follow\s*4\s*follow"
    r"|follow\s*for\s*follow"
    r"|\bf4f\b|\bifb\b|\bfb100\b|\bmutuals?\b"
    r"|互关|互粉|有关必回|关注必回|互关必回|回关"
    r"|フォロバ|相互フォロー|フォローバック"
    r")",
    re.IGNORECASE,
)

_UNDETERMINED_LANGS = {"", "unknown", "und", "qme", "qam", "zxx", "art"}
_INDEXED_FOLLOW_DB_KEYS: Set[int] = set()


def _clean_handle(handle: Any) -> str:
    return str(handle or "").strip().lstrip("@").split("/")[0].split("?")[0].strip()


def _clean_handle_lower(handle: Any) -> str:
    return _clean_handle(handle).lower()


def _normalize_langs(raw_langs: Any) -> List[str]:
    items: List[str] = []
    if isinstance(raw_langs, str):
        raw_iter: Iterable[Any] = [raw_langs]
    elif isinstance(raw_langs, (list, tuple, set)):
        raw_iter = raw_langs
    else:
        raw_iter = []
    for item in raw_iter:
        norm = normalize_language_code(str(item or ""))
        if norm and norm not in _UNDETERMINED_LANGS and norm not in items:
            items.append(norm)
    return sorted(items)


def _ts_to_iso_cn(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=TZ_CN).isoformat(timespec="seconds")


def ensure_follow_indexes(db: Any) -> None:
    """幂等创建关注候选池与关注账本集合索引（同库进程内仅执行一次）。"""
    if db is None:
        return
    db_key = id(getattr(db, "db", db))
    if db_key in _INDEXED_FOLLOW_DB_KEYS:
        return
    create_index = getattr(db, "create_index", None)
    if callable(create_index):
        create_index(FOLLOW_CANDIDATES_COLLECTION_NAME, [("screen_name_lower", 1)], unique=True)
        create_index(
            FOLLOW_CANDIDATES_COLLECTION_NAME,
            [("worth_following", 1), ("tier", 1), ("follow_score", -1), ("last_active_ts", -1)],
            unique=False,
        )
        create_index(
            FOLLOW_RECORDS_COLLECTION_NAME,
            [("account_name", 1), ("target_handle_lower", 1)],
            unique=True,
        )
        create_index(
            FOLLOW_RECORDS_COLLECTION_NAME,
            [("account_name", 1), ("status", 1), ("followed_at_ts", -1)],
            unique=False,
        )
        create_index(
            FOLLOW_RECORDS_COLLECTION_NAME,
            [("account_name", 1), ("followed_back", 1)],
            unique=False,
        )
    _INDEXED_FOLLOW_DB_KEYS.add(db_key)


def evaluate_follow_candidate(
    profile: Dict[str, Any],
    *,
    now_ts: Optional[int] = None,
    config: Optional[Dict[str, Any]] = None,
    self_handles: Optional[Iterable[str]] = None,
    has_prior_interaction: bool = False,
) -> Dict[str, Any]:
    """
    纯函数确定性评估候选账号是否值得关注及回关概率评分（0~100 / S·A·B 分档）。

    6 道确定性硬门禁（每道均按高概率真实互关原则设定，不采纳未经验证的复杂猜想）：
    1. 非己方账号且用户名合法；
    2. 非私密锁推号；
    3. 蓝V认证（可配置）；
    4. 100~3000 粉同体量区间；
    5. 主动关注 >=50、未触 X 5000 关注天花板、且 following/followers 在 0.80~3.00（暖线索放宽下限至 0.50）；
    6. 发帖 >=20、近 7 天活跃、且简介不含明确拒互关声明。
    """
    cfg = {**FOLLOWER_DEFAULTS, **(config or {})}
    now = int(now_ts if now_ts is not None else time.time())

    screen_name = _clean_handle(profile.get("screen_name") or profile.get("username"))
    h_low = screen_name.lower()
    followers = safe_int(profile.get("followers_count"), 0)
    following = safe_int(profile.get("following_count"), 0)
    statuses = safe_int(profile.get("statuses_count"), 0)
    ff_ratio = round(following / max(followers, 1), 4)
    is_blue = bool(profile.get("is_blue_verified"))
    is_verified = bool(is_blue or profile.get("is_verified"))
    is_protected = bool(profile.get("is_protected"))
    desc = str(profile.get("description") or "")

    last_active_ts = safe_int(
        profile.get("last_active_ts") or profile.get("latest_pub") or profile.get("checked_at_ts"),
        0,
    )
    active_days = max(1, safe_int(cfg.get("active_within_days"), 7))
    active_window_sec = active_days * 86400
    age_sec = max(0, now - last_active_ts) if last_active_ts > 0 else active_window_sec + 1
    activity_age_hours = round(age_sec / 3600.0, 2)

    eff_self: Set[str] = {h.lower() for h in get_configured_self_handles() if h}
    if self_handles:
        eff_self.update(_clean_handle_lower(h) for h in self_handles if _clean_handle_lower(h))

    def _reject(reason: str) -> Dict[str, Any]:
        return {
            "screen_name": screen_name,
            "screen_name_lower": h_low,
            "worth_following": False,
            "reject_reason": reason,
            "follow_score": 0,
            "tier": None,
            "ff_ratio": ff_ratio,
            "activity_age_hours": activity_age_hours,
            "has_mutual_bio": False,
            "has_prior_interaction": bool(has_prior_interaction),
            "signals": [],
            "strategy_version": FOLLOW_STRATEGY_VERSION,
        }

    # 【保留过滤原因说明】门禁 1：排除空用户名与己方矩阵账号（严禁自关注或误关注小号）
    if not h_low or h_low == "unknown" or h_low in eff_self:
        return _reject("self_or_invalid_handle")

    # 【保留过滤原因说明】门禁 2：锁推/私密账号需要人工审批且通常不对外公开互关，跳过
    if is_protected:
        return _reject("protected")

    # 【保留过滤原因说明】门禁 3：蓝V同体量创作者更具公开成长与互关意愿
    if bool(cfg.get("require_verified", True)) and not is_verified:
        return _reject("not_verified")

    # 【保留过滤原因说明】门禁 4：锁定同体量成长区间（默认 100~3000 粉），过滤早期空壳与高冷大V
    min_followers = safe_int(cfg.get("min_followers"), 100)
    max_followers = safe_int(cfg.get("max_followers"), 3000)
    if followers < min_followers or followers > max_followers:
        return _reject("followers_out_of_range")

    # 【保留过滤原因说明】门禁 5：过滤从不关注他人的冷漠号、逼近 X 5000 关注上限卡死号以及狂扫垃圾号
    min_following = safe_int(cfg.get("min_following"), 50)
    if following < min_following:
        return _reject("following_too_low")

    max_fg_cap = safe_int(cfg.get("max_following_cap"), 4500)
    if max_fg_cap > 0 and following > max_fg_cap and not (followers >= max_fg_cap and ff_ratio <= 1.05):
        return _reject("near_5k_follow_cap")

    min_ratio = float(
        cfg.get("warm_min_ff_ratio", 0.50) if has_prior_interaction else cfg.get("min_ff_ratio", 0.80)
    )
    max_ratio = float(cfg.get("max_ff_ratio", 3.00))
    if ff_ratio < min_ratio or ff_ratio > max_ratio:
        return _reject("ratio_out_of_range")

    # 【保留过滤原因说明】门禁 6：要求历史发推足量、近 7 天仍有公开发推，且简介不包含明确拒互关声明
    min_statuses = safe_int(cfg.get("min_statuses_count"), 20)
    if statuses < min_statuses:
        return _reject("statuses_too_low")

    if last_active_ts <= 0 or age_sec > active_window_sec:
        return _reject("inactive_over_window")

    if ANTI_MUTUAL_BIO_RE.search(desc):
        return _reject("anti_mutual_bio")

    # ── 透明加分打分（0~100）与 S/A/B 分档 ──
    score = 40
    signals: List[str] = ["verified_peer" if is_verified else "peer_range"]

    if 0.95 <= ff_ratio <= 1.80:
        score += 25
        signals.append("prime_ratio")
    elif (0.85 <= ff_ratio < 0.95) or (1.80 < ff_ratio <= 2.30):
        score += 18
        signals.append("good_ratio")
    else:
        score += 10
        signals.append("ok_ratio")

    if activity_age_hours <= 24.0:
        score += 15
        signals.append("active_24h")
    elif activity_age_hours <= 72.0:
        score += 10
        signals.append("active_72h")
    else:
        score += 4
        signals.append("active_7d")

    has_mutual_bio = bool(MUTUAL_BIO_RE.search(desc))
    if has_mutual_bio:
        score += 20
        signals.append("mutual_bio")

    if has_prior_interaction:
        score += 25
        signals.append("warm_interaction")

    score = min(100, score)
    if has_prior_interaction or has_mutual_bio:
        tier = "S"
    elif (0.95 <= ff_ratio <= 1.80) and activity_age_hours <= 72.0 and (150 <= followers <= 2500):
        tier = "A"
    else:
        tier = "B"

    return {
        "screen_name": screen_name,
        "screen_name_lower": h_low,
        "worth_following": True,
        "reject_reason": None,
        "follow_score": int(score),
        "tier": tier,
        "ff_ratio": ff_ratio,
        "activity_age_hours": activity_age_hours,
        "has_mutual_bio": has_mutual_bio,
        "has_prior_interaction": bool(has_prior_interaction),
        "signals": signals,
        "strategy_version": FOLLOW_STRATEGY_VERSION,
    }


def upsert_discovered_users(
    db: Any,
    users: Sequence[Dict[str, Any]],
    *,
    config: Optional[Dict[str, Any]] = None,
    now_ts: Optional[int] = None,
) -> Dict[str, Any]:
    """将浏览器嗅探或帖子聚合得到的去重作者画像写入 `x_follow_candidates` 候选池。"""
    if db is None or not users:
        return {"upserted": 0, "qualified": 0, "tiers": {"S": 0, "A": 0, "B": 0}}

    ensure_follow_indexes(db)
    now = int(now_ts if now_ts is not None else time.time())
    now_str = _ts_to_iso_cn(now)
    cfg = {**FOLLOWER_DEFAULTS, **(config or {})}

    dedup: Dict[str, Dict[str, Any]] = {}
    for raw in users:
        if not isinstance(raw, dict):
            continue
        sn = _clean_handle(raw.get("screen_name") or raw.get("username"))
        h_low = sn.lower()
        if not h_low or h_low == "unknown":
            continue
        prev = dedup.get(h_low) or {}
        langs = set(_normalize_langs(prev.get("languages"))) | set(
            _normalize_langs(raw.get("languages") or raw.get("observed_langs") or raw.get("lang"))
        )
        roles = set(prev.get("source_roles") or [])
        if raw.get("source_role"):
            roles.add(str(raw["source_role"]))
        for r in raw.get("source_roles") or []:
            if r:
                roles.add(str(r))

        merged = {**prev, **raw}
        merged["screen_name"] = sn
        merged["screen_name_lower"] = h_low
        merged["languages"] = sorted(langs)
        merged["source_roles"] = sorted(roles)
        merged["last_active_ts"] = max(
            safe_int(prev.get("last_active_ts"), 0),
            safe_int(raw.get("last_active_ts") or raw.get("latest_pub") or raw.get("checked_at_ts"), 0),
            now if raw.get("checked_at_ts") is None and raw.get("last_active_ts") is None and raw.get("latest_pub") is None else 0,
        )
        dedup[h_low] = merged

    if not dedup:
        return {"upserted": 0, "qualified": 0, "tiers": {"S": 0, "A": 0, "B": 0}}

    col = db.get_collection(FOLLOW_CANDIDATES_COLLECTION_NAME)
    existing_docs = list(
        col.find(
            {"screen_name_lower": {"$in": list(dedup.keys())}},
            {"screen_name_lower": 1, "discovered_at": 1, "discovered_at_ts": 1, "last_active_ts": 1, "languages": 1, "source_roles": 1, "_id": 0},
        )
    )
    existing_map = {str(d.get("screen_name_lower")): d for d in existing_docs if d.get("screen_name_lower")}

    ops: List[UpdateOne] = []
    tiers = {"S": 0, "A": 0, "B": 0}
    qualified = 0

    for h_low, item in dedup.items():
        old = existing_map.get(h_low) or {}
        last_active_ts = max(
            safe_int(old.get("last_active_ts"), 0),
            safe_int(item.get("last_active_ts"), 0),
        )
        if last_active_ts <= 0:
            last_active_ts = safe_int(item.get("checked_at_ts"), now)

        languages = sorted(set(_normalize_langs(old.get("languages"))) | set(_normalize_langs(item.get("languages"))))
        source_roles = sorted(set(old.get("source_roles") or []) | set(item.get("source_roles") or []))

        eval_input = {
            **item,
            "last_active_ts": last_active_ts,
        }
        ev = evaluate_follow_candidate(eval_input, now_ts=now, config=cfg)
        if ev["worth_following"]:
            qualified += 1
            if ev["tier"] in tiers:
                tiers[ev["tier"]] += 1

        doc = {
            "screen_name": ev["screen_name"],
            "screen_name_lower": h_low,
            "user_id": str(item.get("user_id") or "").strip(),
            "name": str(item.get("name") or ev["screen_name"]).strip(),
            "is_blue_verified": bool(item.get("is_blue_verified")),
            "is_verified": bool(item.get("is_blue_verified") or item.get("is_verified")),
            "is_protected": bool(item.get("is_protected")),
            "followers_count": safe_int(item.get("followers_count"), 0),
            "following_count": safe_int(item.get("following_count"), 0),
            "statuses_count": safe_int(item.get("statuses_count"), 0),
            "ff_ratio": ev["ff_ratio"],
            "description": str(item.get("description") or ""),
            "languages": languages,
            "source_roles": source_roles,
            "last_active_ts": last_active_ts,
            "last_active_at": _ts_to_iso_cn(last_active_ts),
            "checked_at_ts": safe_int(item.get("checked_at_ts"), now),
            "worth_following": ev["worth_following"],
            "reject_reason": ev["reject_reason"],
            "follow_score": ev["follow_score"],
            "tier": ev["tier"],
            "has_mutual_bio": ev["has_mutual_bio"],
            "signals": ev["signals"],
            "strategy_version": FOLLOW_STRATEGY_VERSION,
            "discovered_at": old.get("discovered_at") or now_str,
            "discovered_at_ts": safe_int(old.get("discovered_at_ts"), now),
            "updated_at": now_str,
            "updated_at_ts": now,
        }
        ops.append(UpdateOne({"screen_name_lower": h_low}, {"$set": doc}, upsert=True))

    if ops:
        col.bulk_write(ops, ordered=False)

    return {"upserted": len(ops), "qualified": qualified, "tiers": tiers}


def sync_candidates_from_posts_db(
    db: Any,
    *,
    lookback_days: int = FOLLOWER_DEFAULTS["sync_posts_lookback_days"],
    config: Optional[Dict[str, Any]] = None,
    now_ts: Optional[int] = None,
) -> Dict[str, Any]:
    """从 `harvested_social_posts` 聚合近 N 天 X 原帖作者画像并同步到 `x_follow_candidates`。"""
    ensure_follow_indexes(db)
    now = int(now_ts if now_ts is not None else time.time())
    cutoff_ts = now - max(1, int(lookback_days)) * 86400

    posts_col = db.get_collection(HARVESTED_POSTS_COLLECTION)
    pipeline = [
        {
            "$match": {
                "source": "x",
                "metadata.platform_extra.author_profile": {"$exists": True},
                "publish_time": {"$gte": cutoff_ts},
            }
        },
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
                "checked_at_ts": {"$first": "$metadata.platform_extra.author_profile.checked_at_ts"},
                "langs": {"$addToSet": "$metadata.lang"},
                "last_active_ts": {"$max": "$publish_time"},
            }
        },
    ]
    rows = list(posts_col.aggregate(pipeline))
    users: List[Dict[str, Any]] = []
    for row in rows:
        sn = _clean_handle(row.get("screen_name"))
        if not sn or sn.lower() == "unknown":
            continue
        users.append(
            {
                "screen_name": sn,
                "user_id": str(row.get("user_id") or "").strip(),
                "name": str(row.get("name") or sn).strip(),
                "followers_count": safe_int(row.get("followers_count"), 0),
                "following_count": safe_int(row.get("following_count"), 0),
                "statuses_count": safe_int(row.get("statuses_count"), 0),
                "is_blue_verified": bool(row.get("is_blue_verified")),
                "is_verified": bool(row.get("is_blue_verified") or row.get("is_verified")),
                "is_protected": bool(row.get("is_protected")),
                "description": str(row.get("description") or ""),
                "checked_at_ts": safe_int(row.get("checked_at_ts"), now),
                "languages": _normalize_langs(row.get("langs")),
                "last_active_ts": safe_int(row.get("last_active_ts"), 0),
                "source_role": "posts_db_sync",
            }
        )

    upsert_res = upsert_discovered_users(db, users, config=config, now_ts=now)
    return {
        "scanned_authors": len(users),
        **upsert_res,
    }


def get_warm_interaction_handles_for_account(
    db: Any,
    *,
    account_dir: str = "",
    account_handle: str = "",
    lookback_days: int = FOLLOWER_DEFAULTS["warm_interaction_lookback_days"],
    now_ts: Optional[int] = None,
) -> Set[str]:
    """
    查询仅属于当前执行账号自己的暖线索作者（近 N 天被我评过且对我评论点过赞或回复过）：
    严格按 `consumed_dir` / `consumed_by` 隔离，A 号暖线索绝不泄漏给 B 号。
    """
    or_filters: List[Dict[str, Any]] = []
    if account_dir:
        resolved_dir = str(Path(account_dir).expanduser().resolve())
        or_filters.append({f"{AUTO_COMMENT_FIELD}.consumed_dir": resolved_dir})
    clean_h = _clean_handle(account_handle)
    if clean_h:
        or_filters.append({f"{AUTO_COMMENT_FIELD}.consumed_by": {"$regex": f"^@{re.escape(clean_h)}$", "$options": "i"}})
    if not or_filters:
        return set()

    now = int(now_ts if now_ts is not None else time.time())
    cutoff_ts = now - max(1, int(lookback_days)) * 86400
    posts_col = db.get_collection(HARVESTED_POSTS_COLLECTION)
    query: Dict[str, Any] = {
        "source": "x",
        f"{AUTO_COMMENT_FIELD}.consumed": True,
        f"{AUTO_COMMENT_FIELD}.consumed_at_ts": {"$gte": cutoff_ts},
        "$and": [
            {"$or": or_filters},
            {
                "$or": [
                    {f"{AUTO_COMMENT_FIELD}.comment_engagement.like_count": {"$gt": 0}},
                    {f"{AUTO_COMMENT_FIELD}.comment_engagement.comment_count": {"$gt": 0}},
                ]
            },
        ],
    }
    docs = list(posts_col.find(query, {"author.username": 1, "_id": 0}))
    return {
        _clean_handle_lower((d.get("author") or {}).get("username"))
        for d in docs
        if _clean_handle_lower((d.get("author") or {}).get("username"))
    }


def select_follow_candidates_for_account(
    db: Any,
    *,
    account_name: str,
    account_dir: str = "",
    account_handle: str = "",
    limit: int = 10,
    config: Optional[Dict[str, Any]] = None,
    now_ts: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    为指定运营账号挑选待关注最优候选列表：
    - 仅读取与过滤当前 `account_name` 的历史记录（未取关/未关注过），多账号共享候选池互不干扰；
    - 叠加本号独有的暖线索（`has_prior_interaction=True`）；
    - 按 S -> A -> B、高分优先、最近活跃优先排序。
    """
    ensure_follow_indexes(db)
    cfg = {**FOLLOWER_DEFAULTS, **(config or {})}
    now = int(now_ts if now_ts is not None else time.time())
    active_cutoff = now - max(1, safe_int(cfg.get("active_within_days"), 7)) * 86400
    failed_cooldown = max(0, safe_int(cfg.get("failed_retry_cooldown_seconds"), 86400))

    rec_col = db.get_collection(FOLLOW_RECORDS_COLLECTION_NAME)
    existing_records = list(
        rec_col.find(
            {"account_name": account_name},
            {"target_handle_lower": 1, "status": 1, "updated_at_ts": 1, "_id": 0},
        )
    )
    excluded_handles: Set[str] = set()
    for rec in existing_records:
        th = _clean_handle_lower(rec.get("target_handle_lower"))
        if not th:
            continue
        st = str(rec.get("status") or "")
        if st == "failed":
            if (now - safe_int(rec.get("updated_at_ts"), 0)) < failed_cooldown:
                excluded_handles.add(th)
        else:
            excluded_handles.add(th)

    warm_handles = get_warm_interaction_handles_for_account(
        db,
        account_dir=account_dir,
        account_handle=account_handle,
        lookback_days=safe_int(cfg.get("warm_interaction_lookback_days"), 30),
        now_ts=now,
    )

    cand_col = db.get_collection(FOLLOW_CANDIDATES_COLLECTION_NAME)
    cand_or: List[Dict[str, Any]] = [{"worth_following": True}]
    if warm_handles:
        cand_or.append({"screen_name_lower": {"$in": list(warm_handles)}})

    raw_candidates = list(
        cand_col.find(
            {
                "last_active_ts": {"$gte": active_cutoff},
                "$or": cand_or,
            },
            {"_id": 0},
        )
    )

    allowed_langs_raw = cfg.get("allowed_languages")
    allowed_langs: Optional[Set[str]] = None
    if allowed_langs_raw:
        parsed_langs = set(_normalize_langs(allowed_langs_raw))
        if parsed_langs:
            allowed_langs = parsed_langs

    min_score = safe_int(cfg.get("min_score"), 50)
    self_extra = [account_handle] if account_handle else None

    eligible: List[Dict[str, Any]] = []
    tiers = {"S": 0, "A": 0, "B": 0}
    warm_available = 0

    for cand in raw_candidates:
        h_low = _clean_handle_lower(cand.get("screen_name_lower") or cand.get("screen_name"))
        if not h_low or h_low in excluded_handles:
            continue

        if allowed_langs is not None:
            c_langs = set(_normalize_langs(cand.get("languages")))
            if c_langs and not (c_langs & allowed_langs):
                continue

        is_warm = h_low in warm_handles
        ev = evaluate_follow_candidate(
            cand,
            now_ts=now,
            config=cfg,
            self_handles=self_extra,
            has_prior_interaction=is_warm,
        )
        if not ev["worth_following"] or ev["follow_score"] < min_score:
            continue

        merged = {
            **cand,
            **ev,
        }
        eligible.append(merged)
        if ev["tier"] in tiers:
            tiers[ev["tier"]] += 1
        if is_warm:
            warm_available += 1

    eligible.sort(
        key=lambda d: (
            TIER_ORDER.get(str(d.get("tier") or "B"), 9),
            -safe_int(d.get("follow_score"), 0),
            float(d.get("activity_age_hours") or 9999.0),
            -safe_int(d.get("followers_count"), 0),
        )
    )

    cap = max(1, int(limit)) if limit and int(limit) > 0 else len(eligible)
    stats = {
        "total_eligible": len(eligible),
        "tiers": tiers,
        "warm_leads_available": warm_available,
        "already_recorded_count": len(excluded_handles),
    }
    return eligible[:cap], stats


def record_follow_outcome(
    db: Any,
    *,
    account_name: str,
    account_handle: str,
    account_dir: str,
    target_profile: Dict[str, Any],
    status: str,
    evaluation: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
    followed_back: bool = False,
    now_ts: Optional[int] = None,
) -> Dict[str, Any]:
    """幂等记录单账号对单个目标用户的关注结果与完整埋点快照至 `x_follow_records`。"""
    ensure_follow_indexes(db)
    now = int(now_ts if now_ts is not None else time.time())
    now_str = _ts_to_iso_cn(now)
    ev = evaluation or {}

    target_handle = _clean_handle(target_profile.get("screen_name") or ev.get("screen_name"))
    target_handle_lower = target_handle.lower()
    if not target_handle_lower:
        return {}

    telemetry = {
        "strategy_version": ev.get("strategy_version") or FOLLOW_STRATEGY_VERSION,
        "tier": ev.get("tier"),
        "follow_score": safe_int(ev.get("follow_score"), 0),
        "followers_count": safe_int(target_profile.get("followers_count"), 0),
        "following_count": safe_int(target_profile.get("following_count"), 0),
        "ff_ratio": ev.get("ff_ratio") if ev.get("ff_ratio") is not None else round(
            safe_int(target_profile.get("following_count"), 0) / max(safe_int(target_profile.get("followers_count"), 0), 1),
            4,
        ),
        "activity_age_hours": ev.get("activity_age_hours"),
        "has_mutual_bio": bool(ev.get("has_mutual_bio")),
        "has_prior_interaction": bool(ev.get("has_prior_interaction")),
        "signals": list(ev.get("signals") or []),
        "languages": _normalize_langs(target_profile.get("languages")),
    }

    set_fields: Dict[str, Any] = {
        "account_name": account_name,
        "account_handle": _clean_handle(account_handle),
        "account_dir": str(account_dir or ""),
        "target_handle": target_handle,
        "target_handle_lower": target_handle_lower,
        "target_user_id": str(target_profile.get("user_id") or "").strip(),
        "status": status,
        "follow_telemetry": telemetry,
        "last_error": str(error)[:240] if error else None,
        "updated_at": now_str,
        "updated_at_ts": now,
    }
    if status == "followed":
        set_fields["followed_at"] = now_str
        set_fields["followed_at_ts"] = now
    if followed_back:
        set_fields["followed_back"] = True
        set_fields["followed_back_at"] = now_str
        set_fields["followed_back_at_ts"] = now

    rec_col = db.get_collection(FOLLOW_RECORDS_COLLECTION_NAME)
    set_on_insert = {
        "created_at": now_str,
        "created_at_ts": now,
    }
    if not followed_back:
        set_on_insert["followed_back"] = False
        set_on_insert["followed_back_at"] = None
        set_on_insert["followed_back_at_ts"] = None
        set_on_insert["follow_back_latency_hours"] = None

    rec_col.update_one(
        {"account_name": account_name, "target_handle_lower": target_handle_lower},
        {"$set": set_fields, "$setOnInsert": set_on_insert},
        upsert=True,
    )
    return set_fields


def mark_follow_backs(
    db: Any,
    *,
    account_name: str,
    follower_handles: Iterable[str],
    now_ts: Optional[int] = None,
) -> Dict[str, Any]:
    """根据己方当前粉丝名单核验历史已关注用户是否回关，并计算回关耗时。"""
    ensure_follow_indexes(db)
    now = int(now_ts if now_ts is not None else time.time())
    now_str = _ts_to_iso_cn(now)
    observed = {_clean_handle_lower(h) for h in (follower_handles or []) if _clean_handle_lower(h)}

    rec_col = db.get_collection(FOLLOW_RECORDS_COLLECTION_NAME)
    pending_docs = list(
        rec_col.find(
            {
                "account_name": account_name,
                "status": {"$in": ["followed", "already_following"]},
                "followed_back": {"$ne": True},
            },
            {"target_handle_lower": 1, "target_handle": 1, "followed_at_ts": 1, "created_at_ts": 1, "_id": 0},
        )
    )

    newly_matched: List[str] = []
    ops: List[UpdateOne] = []
    for doc in pending_docs:
        th_low = _clean_handle_lower(doc.get("target_handle_lower"))
        if not th_low:
            continue
        if th_low in observed:
            base_ts = safe_int(doc.get("followed_at_ts") or doc.get("created_at_ts"), now)
            latency_h = round(max(0, now - base_ts) / 3600.0, 2)
            newly_matched.append(str(doc.get("target_handle") or th_low))
            ops.append(
                UpdateOne(
                    {"account_name": account_name, "target_handle_lower": th_low},
                    {
                        "$set": {
                            "followed_back": True,
                            "followed_back_at": now_str,
                            "followed_back_at_ts": now,
                            "follow_back_latency_hours": latency_h,
                            "follow_back_checked_at_ts": now,
                            "updated_at": now_str,
                            "updated_at_ts": now,
                        }
                    },
                )
            )
        else:
            ops.append(
                UpdateOne(
                    {"account_name": account_name, "target_handle_lower": th_low},
                    {"$set": {"follow_back_checked_at_ts": now}},
                )
            )

    if ops:
        rec_col.bulk_write(ops, ordered=False)

    return {
        "account_name": account_name,
        "observed_followers": len(observed),
        "checked_pending": len(pending_docs),
        "newly_followed_back": len(newly_matched),
        "new_handles": newly_matched,
    }


def count_recent_follows(
    db: Any,
    account_name: str,
    *,
    window_seconds: int = 86400,
    now_ts: Optional[int] = None,
) -> int:
    """统计指定账号在最近滚动时间窗内成功执行新关注的人数。"""
    ensure_follow_indexes(db)
    now = int(now_ts if now_ts is not None else time.time())
    cutoff = now - max(1, int(window_seconds))
    rec_col = db.get_collection(FOLLOW_RECORDS_COLLECTION_NAME)
    return int(
        rec_col.count_documents(
            {
                "account_name": account_name,
                "status": "followed",
                "followed_at_ts": {"$gte": cutoff},
            }
        )
    )


def build_follow_ops_summary(
    db: Any,
    accounts: Sequence[Dict[str, Any]],
    *,
    config: Optional[Dict[str, Any]] = None,
    now_ts: Optional[int] = None,
) -> Dict[str, Any]:
    """汇总候选池库存、各账号 24h/累计关注战绩、各档/各信号真实回关率与最近关注明细。"""
    ensure_follow_indexes(db)
    cfg = {**FOLLOWER_DEFAULTS, **(config or {})}
    now = int(now_ts if now_ts is not None else time.time())
    active_cutoff = now - max(1, safe_int(cfg.get("active_within_days"), 7)) * 86400
    day_cutoff = now - 86400

    cand_col = db.get_collection(FOLLOW_CANDIDATES_COLLECTION_NAME)
    active_cands = list(
        cand_col.find(
            {"worth_following": True, "last_active_ts": {"$gte": active_cutoff}},
            {"tier": 1, "_id": 0},
        )
    )
    pool_tiers = {"S": 0, "A": 0, "B": 0}
    for c in active_cands:
        t = str(c.get("tier") or "B")
        if t in pool_tiers:
            pool_tiers[t] += 1

    rec_col = db.get_collection(FOLLOW_RECORDS_COLLECTION_NAME)
    account_stats: List[Dict[str, Any]] = []
    tier_stats: Dict[str, Dict[str, int]] = {
        "S": {"followed": 0, "followed_back": 0},
        "A": {"followed": 0, "followed_back": 0},
        "B": {"followed": 0, "followed_back": 0},
    }
    signal_stats: Dict[str, Dict[str, int]] = {
        "warm_interaction": {"followed": 0, "followed_back": 0},
        "mutual_bio": {"followed": 0, "followed_back": 0},
        "prime_ratio": {"followed": 0, "followed_back": 0},
    }

    for acc in accounts:
        acc_name = str(acc.get("name") or "")
        acc_handle = _clean_handle(acc.get("screen_name"))
        max_per_day = safe_int((acc.get("overrides") or {}).get("max_follow_per_day") or acc.get("max_follow_per_day"), cfg["max_follow_per_day"])
        docs = list(rec_col.find({"account_name": acc_name}, {"_id": 0}))

        followed_docs = [d for d in docs if d.get("status") == "followed"]
        follows_24h = sum(1 for d in followed_docs if safe_int(d.get("followed_at_ts"), 0) >= day_cutoff)
        followed_backs = sum(1 for d in followed_docs if d.get("followed_back") is True)
        rate_pct = round((followed_backs / len(followed_docs)) * 100.0, 1) if followed_docs else 0.0

        for d in followed_docs:
            tel = d.get("follow_telemetry") or {}
            t = str(tel.get("tier") or "")
            is_fb = d.get("followed_back") is True
            if t in tier_stats:
                tier_stats[t]["followed"] += 1
                if is_fb:
                    tier_stats[t]["followed_back"] += 1
            sigs = set(tel.get("signals") or [])
            for sig_key in signal_stats:
                if sig_key in sigs or bool(tel.get(f"has_{sig_key}")):
                    signal_stats[sig_key]["followed"] += 1
                    if is_fb:
                        signal_stats[sig_key]["followed_back"] += 1

        followed_docs.sort(key=lambda d: safe_int(d.get("followed_at_ts"), 0), reverse=True)
        recent_items = [
            {
                "target": f"@{d.get('target_handle')}",
                "tier": (d.get("follow_telemetry") or {}).get("tier"),
                "score": (d.get("follow_telemetry") or {}).get("follow_score"),
                "ff_ratio": (d.get("follow_telemetry") or {}).get("ff_ratio"),
                "followed_back": bool(d.get("followed_back")),
                "followed_at": d.get("followed_at"),
            }
            for d in followed_docs[:5]
        ]

        account_stats.append(
            {
                "account_name": acc_name,
                "screen_name": acc_handle,
                "user_data_dir": str(acc.get("user_data_dir") or ""),
                "follows_24h": follows_24h,
                "max_follow_per_day": max_per_day,
                "total_followed": len(followed_docs),
                "followed_back": followed_backs,
                "follow_back_rate_pct": rate_pct,
                "total_records": len(docs),
                "recent_followed": recent_items,
            }
        )

    def _with_rate(raw_map: Dict[str, Dict[str, int]]) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for k, v in raw_map.items():
            f_cnt = v["followed"]
            fb_cnt = v["followed_back"]
            out[k] = {
                "followed": f_cnt,
                "followed_back": fb_cnt,
                "rate_pct": round((fb_cnt / f_cnt) * 100.0, 1) if f_cnt else 0.0,
            }
        return out

    return {
        "generated_at": now_iso_cn(),
        "candidate_pool": {
            "total_active_qualified": len(active_cands),
            "tiers": pool_tiers,
        },
        "accounts": account_stats,
        "by_tier": _with_rate(tier_stats),
        "by_signal": _with_rate(signal_stats),
    }


def format_ops_summary_text(summary: Dict[str, Any], runtime_status: Optional[Dict[str, Any]] = None) -> str:
    """将关注系统状态渲染为人类直观可读的运维看板文本。"""
    pool = summary.get("candidate_pool") or {}
    tiers = pool.get("tiers") or {}
    lines = [
        "=" * 76,
        f"X 自动互关增长系统运维看板 | 更新时间: {summary.get('generated_at', '')}",
        f"活跃合格候选池: {pool.get('total_active_qualified', 0)} 人 "
        f"(S档={tiers.get('S', 0)} / A档={tiers.get('A', 0)} / B档={tiers.get('B', 0)})",
        "-" * 76,
    ]

    rt_accounts = ((runtime_status or {}).get("accounts") or {})
    if runtime_status:
        lines.append(
            f"守护状态: round=#{runtime_status.get('round_no', '?')} | "
            f"上次运行={runtime_status.get('last_run_at', '未知')} | "
            f"下次计划={runtime_status.get('next_run_at', '未排期')}"
        )

    for acc in summary.get("accounts") or []:
        name = acc["account_name"]
        rt = rt_accounts.get(name) or {}
        rt_tag = ""
        if rt:
            rt_tag = f" | 最近状态={rt.get('status', '?')}"
            if rt.get("warning"):
                rt_tag += f" ({str(rt['warning'])[:60]})"
        lines.append(
            f"[{name} @{acc.get('screen_name') or '?'}] "
            f"24h关注={acc['follows_24h']}/{acc['max_follow_per_day']} | "
            f"累计关注={acc['total_followed']} | 已回关={acc['followed_back']} "
            f"({acc['follow_back_rate_pct']}%){rt_tag}"
        )
        for item in acc.get("recent_followed") or []:
            fb_mark = "已回关" if item.get("followed_back") else "待观察"
            lines.append(
                f"   - {item['target']} [{item.get('tier') or '-'}|{item.get('score') or 0}分|ratio={item.get('ff_ratio')}] "
                f"{fb_mark} ({item.get('followed_at')})"
            )

    by_tier = summary.get("by_tier") or {}
    tier_parts = [f"{t}:{by_tier[t]['followed_back']}/{by_tier[t]['followed']}({by_tier[t]['rate_pct']}%)" for t in ("S", "A", "B") if t in by_tier]
    by_sig = summary.get("by_signal") or {}
    sig_parts = [f"{s}:{by_sig[s]['followed_back']}/{by_sig[s]['followed']}({by_sig[s]['rate_pct']}%)" for s in ("warm_interaction", "mutual_bio", "prime_ratio") if s in by_sig]
    lines.append("-" * 76)
    lines.append("分档回关率: " + " | ".join(tier_parts))
    lines.append("信号回关率: " + " | ".join(sig_parts))
    lines.append("=" * 76)
    return "\n".join(lines)
