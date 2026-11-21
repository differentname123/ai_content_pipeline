# -*- coding: utf-8 -*-
"""
X 平台高互关潜力账号每小时自动拉取与安全关注守护进程 (`x_growth/auto_follower.py`)

核心工程原则（80/20 · 稳定可感知 · 账号物理隔离）：
1. 采集与执行严格分离：
   - 若近 50 分钟内已有浏览器采集数据且合格池充足，直接复用库内数据；否则仅调度小号 `x_xiaohao` 补采；
   - 严禁将任何主号/待涨粉号（`x_myself`、`x_dahao` 等）用作采集号；
2. 每小时每号仅开一次真实桌面 Chrome：
   - 校验登录与风控状态 -> 定期顺路核验粉丝列表统计真实回关率 -> 单线程慢速访问候选主页二次复核并执行关注；
   - 默认仅配置 `x_myself` (@peterzhu1003) 与 `x_dahao` (@2434954452qqc)，小号 `x_xiaohao` 严禁参与关注；
3. 遇风控/限流立即对该账号独立冷却 6h，未登录或目录缺失明确提示 `--login <account>`，绝不拖垮其他账号；
4. 运维可感知：每轮输出直观战报并原子持久化至 `logs/runtime/x_follow_status.json`，支持 `--status` 与 `--dry-run`。
"""
import argparse
import json
import os
import random
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.common_utils import get_proxy_url, get_runtime_file_path, read_json, resolve_session_dir, safe_int, save_json
from common.daemon import run_daemon_loop
from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import HARVESTED_POSTS_COLLECTION
from common.post_utils import AccountIsolationError, assert_not_x_myself_dir
from platforms.playwright_base import _dismiss_overlays, _robust_click, check_for_crash, persistent_browser_session
from platforms.x_playwright import _wait_x_home_ready, open_browser_for_manual_use
from x_growth.follow_selector import (
    FOLLOW_CANDIDATES_COLLECTION_NAME,
    build_follow_ops_summary,
    count_recent_follows,
    format_ops_summary_text,
    mark_follow_backs,
    record_follow_outcome,
    select_follow_candidates_for_account,
    sync_candidates_from_posts_db,
)
from x_growth.growth_config import (
    FOLLOWER_DEFAULTS,
    TZ_CN,
    get_growth_logger,
    now_iso_cn,
)
from x_growth.x_browser_collector import (
    X_HOME_URL,
    XPassiveGraphQLSniffer,
    collect_and_save_posts,
    extract_logged_in_identity,
    inspect_x_page_account_status,
)

STATUS_RUNTIME_FILE = "x_follow_status.json"
_COOLDOWN_TRIGGER_STATUSES = {"locked_challenge", "not_logged_in", "rate_limited", "suspended"}
_RE_FOLLOWERS_GQL = re.compile(r"/graphql/[^/]+/(Followers|BlueVerifiedFollowers|VerifiedFollowers)(?:\?|$)", re.IGNORECASE)
_RE_FRIENDSHIP_API = re.compile(r"(?:friendships/create|CreateFriendship)", re.IGNORECASE)
_RATE_LIMIT_CODES = {88, 161}
_CHALLENGE_CODES = {326}
_PROTECTED_CODES = {160}
_UNAVAILABLE_CODES = {108, 162}
_TOAST_RATE_LIMIT_TOKENS = (
    "unable to follow more",
    "rate limit",
    "temporarily limited",
    "limit reached",
    "无法关注更多",
    "已达关注上限",
    "操作受限",
)

_FOLLOWER_OVERRIDE_KEYS: Tuple[str, ...] = (
    "max_follow_per_run",
    "max_follow_per_day",
    "allowed_languages",
    "min_followers",
    "max_followers",
    "min_following",
    "max_following_cap",
    "min_ff_ratio",
    "warm_min_ff_ratio",
    "max_ff_ratio",
    "min_statuses_count",
    "active_within_days",
    "min_score",
    "require_verified",
    "action_delay_range",
    "browser_visible",
    "dry_run",
    "self_following_safety_cap",
)


def _sleep_jitter(delay_range: Sequence[float]) -> None:
    try:
        low, high = float(delay_range[0]), float(delay_range[1])
    except Exception:
        low, high = 15.0, 30.0
    if high > 0:
        time.sleep(random.uniform(max(1.0, low), max(low + 1.0, high)))


def _norm_dir(path_str: Any) -> str:
    raw = str(path_str or "").strip()
    if not raw:
        return ""
    return os.path.normcase(os.path.abspath(str(Path(raw).expanduser())))


def _resolve_follower_pool(
    *,
    account_pool: Optional[Sequence[Any]] = None,
    account_filter: Optional[str] = None,
    overrides: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """解析启用中的关注执行账号池，支持按别名/用户名过滤与 CLI 参数覆盖。"""
    configured = account_pool if account_pool is not None else FOLLOWER_DEFAULTS.get("account_pool") or []
    filter_tokens: Set[str] = set()
    if account_filter:
        filter_tokens = {
            tok.strip().lstrip("@").lower()
            for tok in str(account_filter).split(",")
            if tok.strip()
        }

    resolved: List[Dict[str, Any]] = []
    seen_dirs: Set[str] = set()
    for item in configured:
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
        screen_name = str(item.get("screen_name") or "").strip().lstrip("@")
        if filter_tokens and name.lower() not in filter_tokens and screen_name.lower() not in filter_tokens:
            continue
        seen_dirs.add(norm_key)

        item_overrides = {k: item[k] for k in _FOLLOWER_OVERRIDE_KEYS if k in item}
        if overrides:
            item_overrides.update({k: v for k, v in overrides.items() if v is not None})

        resolved.append(
            {
                "name": name,
                "screen_name": screen_name,
                "user_data_dir": session_path,
                "overrides": item_overrides,
            }
        )
    return resolved


def _assert_safe_collector_dir(collector_dir: str, account_pool: Optional[Sequence[Dict[str, Any]]] = None) -> None:
    """采集安全硬守卫：严禁使用 `x_myself` 或任何关注执行主号目录进行被动嗅探采集。"""
    assert_not_x_myself_dir(collector_dir, context="x_growth.auto_follower.collector")
    norm_collector = _norm_dir(collector_dir)
    if not norm_collector:
        return
    forbidden_dirs: Set[str] = {_norm_dir(resolve_session_dir("x_dahao_browser_session_dir", "x_dahao"))}
    for acc in account_pool or FOLLOWER_DEFAULTS.get("account_pool") or []:
        if isinstance(acc, dict) and acc.get("user_data_dir"):
            forbidden_dirs.add(_norm_dir(acc["user_data_dir"]))
    if norm_collector in forbidden_dirs:
        raise AccountIsolationError(
            f"安全隔离红线拦截 [x_growth.auto_follower.collector]：禁止使用主号/互关执行目录 `{collector_dir}` 充当采集账号！"
        )


def _datetime_to_epoch(val: Any) -> int:
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, datetime):
        dt = val if val.tzinfo is not None else val.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    return 0


def refresh_follow_candidates(
    db: Any,
    *,
    force_fetch: bool = False,
    allow_browser_harvest: bool = True,
    collector_cooldown_until: Optional[float] = None,
    account_pool: Optional[Sequence[Dict[str, Any]]] = None,
    config: Optional[Dict[str, Any]] = None,
    logger: Any = None,
) -> Dict[str, Any]:
    """
    智能同步与按需补采互关候选池：
    1. 先检查 `harvested_social_posts` 最近浏览器采集中断年龄及 `x_follow_candidates` 活跃库存；
    2. 若 50 分钟内已有新鲜采集且库存充足（或处于 dry-run/冷却中），直接复用库内数据；
    3. 否则安全调度 `x_xiaohao` 补采一轮，再聚合同步近 7 天作者画像入池。
    """
    log = logger or get_growth_logger("x_growth_follower")
    cfg = {**FOLLOWER_DEFAULTS, **(config or {})}
    now_ts = int(time.time())
    lookback_days = max(1, safe_int(cfg.get("sync_posts_lookback_days"), 7))
    active_days = max(1, safe_int(cfg.get("active_within_days"), 7))
    reuse_window = max(0, safe_int(cfg.get("collector_reuse_window_seconds"), 3000))
    min_pool_trigger = max(0, safe_int(cfg.get("min_candidate_pool_trigger"), 30))

    posts_col = db.get_collection(HARVESTED_POSTS_COLLECTION)
    latest_browser_doc = posts_col.find_one(
        {
            "source": "x",
            "$or": [
                {"metadata.platform_extra.category": {"$regex": "^x_browser_"}},
                {"metadata.category": {"$regex": "^x_browser_"}},
            ],
        },
        {"db_update_time": 1, "metadata.platform_extra.author_profile.checked_at_ts": 1, "_id": 0},
        sort=[("db_update_time", -1)],
    )
    last_capture_ts = 0
    if isinstance(latest_browser_doc, dict):
        last_capture_ts = _datetime_to_epoch(latest_browser_doc.get("db_update_time"))
        if last_capture_ts <= 0:
            last_capture_ts = safe_int(
                (((latest_browser_doc.get("metadata") or {}).get("platform_extra") or {}).get("author_profile") or {}).get("checked_at_ts"),
                0,
            )
    last_capture_age_sec = max(0, now_ts - last_capture_ts) if last_capture_ts > 0 else None

    cand_col = db.get_collection(FOLLOW_CANDIDATES_COLLECTION_NAME)
    active_cutoff = now_ts - active_days * 86400
    pre_sync_res: Optional[Dict[str, Any]] = None
    qualified_count = int(cand_col.count_documents({"worth_following": True, "last_active_ts": {"$gte": active_cutoff}}))
    if qualified_count == 0:
        pre_sync_res = sync_candidates_from_posts_db(db, lookback_days=lookback_days, config=cfg, now_ts=now_ts)
        qualified_count = int(cand_col.count_documents({"worth_following": True, "last_active_ts": {"$gte": active_cutoff}}))

    collector_dir = str(cfg.get("collector_user_data_dir") or resolve_session_dir("x_xiaohao_browser_session_dir", "x_xiaohao"))
    _assert_safe_collector_dir(collector_dir, account_pool=account_pool)

    fetch_mode = "db_only"
    collect_status = "skipped"
    collect_warning: Optional[str] = None
    next_collector_cooldown = collector_cooldown_until

    fresh_enough = (last_capture_age_sec is not None) and (last_capture_age_sec <= reuse_window) and (qualified_count >= min_pool_trigger)
    if not allow_browser_harvest:
        fetch_mode = "dry_run_db_sync"
    elif (not force_fetch) and fresh_enough:
        fetch_mode = "reused_recent_harvest"
        log.info(
            f"[AutoFollower/候选复用] 距上次浏览器采集仅 {last_capture_age_sec}s (<= {reuse_window}s) "
            f"且活跃合格候选={qualified_count}人 (>= {min_pool_trigger})，跳过小号补采直接同步库内池。"
        )
    elif collector_cooldown_until and collector_cooldown_until > time.time():
        rem = int(collector_cooldown_until - time.time())
        fetch_mode = "collector_cooldown_fallback"
        collect_status = "cooldown"
        collect_warning = f"采集小号仍在风控冷却中（剩余 {rem}s），降级使用已有库内候选池"
        log.warning(f"⚠️ [AutoFollower/小号冷却降级] {collect_warning}")
    else:
        log.info(
            f"[AutoFollower/触发补采] 启动小号被动采集刷新候选池 | force={force_fetch} | "
            f"last_capture_age={last_capture_age_sec}s | qualified={qualified_count}"
        )
        collect_res = collect_and_save_posts(
            user_data_dir=collector_dir,
            browser_visible=bool(cfg.get("browser_visible", False)),
        )
        collect_status = str(collect_res.get("status") or "ok")
        collect_warning = collect_res.get("account_warning")
        if collect_status in ("locked_challenge", "suspended", "not_logged_in"):
            risk_cd = max(600, safe_int(cfg.get("risk_cooldown_seconds"), 21600))
            next_collector_cooldown = time.time() + risk_cd
            fetch_mode = f"collector_{collect_status}_fallback"
            log.error(
                f"🚨 [AutoFollower/小号采集阻断(冷却{risk_cd}s)] status={collect_status} | "
                f"{collect_warning} -> 仍继续复用现有库内池执行主号关注。"
            )
        elif collect_status != "ok":
            fetch_mode = f"collector_{collect_status}_fallback"
            log.warning(f"⚠️ [AutoFollower/小号采集告警] status={collect_status} | {collect_warning}")
        else:
            next_collector_cooldown = None
            fetch_mode = "fresh_browser_harvest"

    sync_res = sync_candidates_from_posts_db(db, lookback_days=lookback_days, config=cfg, now_ts=now_ts)
    if pre_sync_res and sync_res.get("upserted", 0) == 0:
        sync_res = pre_sync_res
    final_qualified = int(cand_col.count_documents({"worth_following": True, "last_active_ts": {"$gte": active_cutoff}}))

    return {
        "fetch_mode": fetch_mode,
        "last_capture_age_sec": last_capture_age_sec,
        "collect_status": collect_status,
        "collect_warning": collect_warning,
        "collector_cooldown_until": next_collector_cooldown,
        "sync_res": sync_res,
        "qualified_count": final_qualified,
    }


def _extract_handles_from_followers_payload(payload: Any) -> Set[str]:
    found: Set[str] = set()

    def _walk(obj: Any) -> None:
        if isinstance(obj, dict):
            tname = obj.get("__typename")
            if tname == "User":
                sn = str(
                    (obj.get("core") or {}).get("screen_name")
                    or (obj.get("legacy") or {}).get("screen_name")
                    or obj.get("screen_name")
                    or ""
                ).strip().lstrip("@")
                if sn:
                    found.add(sn.lower())
            for v in obj.values():
                _walk(v)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item)

    _walk(payload)
    return found


def sync_follow_backs_in_session(
    page: Any,
    self_handle: str,
    *,
    db: Any,
    account_name: str,
    logger: Any = None,
) -> Dict[str, Any]:
    """在当前已打开浏览器会话中顺路打开一次粉丝列表，被动核验真实回关情况。"""
    log = logger or get_growth_logger("x_growth_follower")
    clean_self = str(self_handle or "").strip().lstrip("@")
    if not clean_self:
        return {"status": "skipped_no_self_handle", "observed_followers": 0, "newly_followed_back": 0, "new_handles": []}

    observed_handles: Set[str] = set()

    def _on_followers_resp(resp: Any) -> None:
        try:
            url = resp.url or ""
            if resp.status != 200 or not _RE_FOLLOWERS_GQL.search(url):
                return
            data = resp.json()
            observed_handles.update(_extract_handles_from_followers_payload(data))
        except Exception:
            pass

    page.on("response", _on_followers_resp)
    try:
        page.goto(f"https://x.com/{clean_self}/followers", timeout=40000, wait_until="domcontentloaded")
        try:
            page.locator('div[data-testid="UserCell"], div[data-testid="empty_state_header_text"]').first.wait_for(
                state="visible", timeout=12000
            )
        except Exception:
            pass
        page.wait_for_timeout(random.randint(1200, 2200))
        _dismiss_overlays(page, aggressive=False, desc="x-follow-back-sync")
        try:
            page.mouse.wheel(0, random.randint(1200, 2000))
            page.wait_for_timeout(random.randint(1200, 1800))
        except Exception:
            pass

        # DOM UserCell 兜底提取（防止首屏缓存命中未走新 GraphQL 分页）
        try:
            hrefs = page.locator('[data-testid="primaryColumn"] div[data-testid="UserCell"] a[href^="/"]').evaluate_all(
                "els => els.map(e => e.getAttribute('href') || '')"
            )
            for href in hrefs or []:
                seg = str(href or "").strip().lstrip("/")
                if seg and "/" not in seg and seg.lower() not in ("i", "home", "explore", "notifications", "messages", "search"):
                    observed_handles.add(seg.lower())
        except Exception:
            pass
    except Exception as exc:
        log.warning(f"[AutoFollower/回关核验跳过] @{clean_self}: {exc}")
    finally:
        try:
            page.remove_listener("response", _on_followers_resp)
        except Exception:
            pass

    observed_handles.discard(clean_self.lower())
    res = mark_follow_backs(db, account_name=account_name, follower_handles=observed_handles)
    if res.get("newly_followed_back", 0) > 0:
        log.info(
            f"🎉 [AutoFollower/回关喜报 | {account_name}] 扫描粉丝={res['observed_followers']}人 -> "
            f"新确认回关 {res['newly_followed_back']} 人: {res['new_handles']}"
        )
    else:
        log.info(
            f"[AutoFollower/回关核验 | {account_name}] 扫描粉丝={res['observed_followers']}人 | "
            f"待回关检查={res['checked_pending']}人 | 新增回关=0人"
        )
    return {"status": "ok", **res}


def follow_candidate_in_session(
    page: Any,
    sniffer: XPassiveGraphQLSniffer,
    candidate: Dict[str, Any],
    *,
    config: Optional[Dict[str, Any]] = None,
    logger: Any = None,
) -> Dict[str, Any]:
    """
    在当前已打开浏览器会话中访问候选人主页：
    1. 监听 `UserByScreenName` 获取实时完整画像并做二次复核（锁推/已互关/已关注/超关注上限）；
    2. 点击关注按钮并同时监听 `friendships/create` 接口响应 + DOM 按钮切换 + Toast 限流提示。
    """
    log = logger or get_growth_logger("x_growth_follower")
    cfg = {**FOLLOWER_DEFAULTS, **(config or {})}
    handle = str(candidate.get("screen_name") or "").strip().lstrip("@")
    h_low = handle.lower()
    if not handle:
        return {"outcome": "skipped_unavailable", "halt_status": None, "halt_warning": None, "live_profile": None, "error": "empty_handle"}

    profile_url = f"https://x.com/{handle}"
    prev_u = sniffer.op_counts.get("UserByScreenName", 0)
    resp = page.goto(profile_url, timeout=45000, wait_until="domcontentloaded")
    sniffer.wait_for_op_increment(page, ("UserByScreenName",), prev_u, timeout_ms=10000)
    try:
        page.locator(
            'div[data-testid="UserName"], div[data-testid="empty_state_header_text"], '
            'input[autocomplete="username"], a[data-testid="loginButton"]'
        ).first.wait_for(state="visible", timeout=15000)
    except Exception:
        pass
    page.wait_for_timeout(random.randint(1400, 2400))
    _dismiss_overlays(page, aggressive=False, desc="x-follow-profile")
    check_for_crash(page)

    step_status, step_msg = inspect_x_page_account_status(page)
    if step_status != "ok":
        return {"outcome": "halted", "halt_status": step_status, "halt_warning": step_msg, "live_profile": None, "error": step_msg}

    if resp and resp.status in (401, 403, 429):
        h_status = "not_logged_in" if resp.status == 401 else "rate_limited"
        msg = f"打开 @{handle} 主页返回 HTTP {resp.status}"
        return {"outcome": "halted", "halt_status": h_status, "halt_warning": msg, "live_profile": None, "error": msg}
    if resp and resp.status >= 400:
        return {"outcome": "skipped_unavailable", "halt_status": None, "halt_warning": None, "live_profile": None, "error": f"http_{resp.status}"}

    try:
        empty_hdr = page.locator('div[data-testid="empty_state_header_text"]').first
        if empty_hdr.count() > 0 and empty_hdr.is_visible():
            err_txt = (empty_hdr.inner_text() or "").strip()[:120]
            return {"outcome": "skipped_unavailable", "halt_status": None, "halt_warning": None, "live_profile": None, "error": err_txt}
    except Exception:
        pass

    live_prof = sniffer.users_by_handle.get(h_low)
    merged_prof = {**candidate, **(live_prof or {})}
    if isinstance(live_prof, dict):
        if bool(live_prof.get("is_protected")):
            return {"outcome": "skipped_protected", "halt_status": None, "halt_warning": None, "live_profile": merged_prof, "error": "protected_live"}
        if bool(live_prof.get("is_following")) and bool(live_prof.get("is_followed_by")):
            return {"outcome": "already_mutual", "halt_status": None, "halt_warning": None, "live_profile": merged_prof, "error": None}
        if bool(live_prof.get("is_following")):
            return {"outcome": "already_following", "halt_status": None, "halt_warning": None, "live_profile": merged_prof, "error": None}

        is_v = bool(live_prof.get("is_blue_verified") or live_prof.get("is_verified"))
        if bool(cfg.get("require_verified", True)) and not is_v:
            return {"outcome": "skipped_ineligible_live", "halt_status": None, "halt_warning": None, "live_profile": merged_prof, "error": "not_verified_live"}

        live_fc = safe_int(live_prof.get("followers_count"), 0)
        live_fg = safe_int(live_prof.get("following_count"), 0)
        max_fg_cap = safe_int(cfg.get("max_following_cap"), 4500)
        live_ratio = live_fg / max(live_fc, 1)
        if max_fg_cap > 0 and live_fg > max_fg_cap and not (live_fc >= max_fg_cap and live_ratio <= 1.05):
            return {"outcome": "skipped_ineligible_live", "halt_status": None, "halt_warning": None, "live_profile": merged_prof, "error": "near_5k_follow_cap_live"}

    primary_col = page.locator('[data-testid="primaryColumn"]').first
    header_pl = primary_col.locator('div[data-testid="placementTracking"]')

    try:
        already_unfollow = header_pl.locator('button[data-testid$="-unfollow"]').first
        if already_unfollow.count() > 0 and already_unfollow.is_visible():
            out = "already_mutual" if bool((live_prof or {}).get("is_followed_by")) else "already_following"
            return {"outcome": out, "halt_status": None, "halt_warning": None, "live_profile": merged_prof, "error": None}
        already_cancel = header_pl.locator('button[data-testid$="-cancel"]').first
        if already_cancel.count() > 0 and already_cancel.is_visible():
            return {"outcome": "skipped_protected", "halt_status": None, "halt_warning": None, "live_profile": merged_prof, "error": "pending_cancel_btn"}
    except Exception:
        pass

    follow_btn = header_pl.locator('button[data-testid$="-follow"]').first
    if follow_btn.count() == 0:
        follow_btn = primary_col.locator('button[data-testid$="-follow"]').first
    if follow_btn.count() == 0 or not follow_btn.is_visible():
        return {"outcome": "failed", "halt_status": None, "halt_warning": None, "live_profile": merged_prof, "error": "follow_btn_not_found"}

    api_state: Dict[str, Any] = {"ok": False, "halt": None, "skip": None, "error": None}

    def _on_follow_resp(r: Any) -> None:
        try:
            u = r.url or ""
            if not _RE_FRIENDSHIP_API.search(u):
                return
            status = r.status
            body: Any = None
            try:
                body = r.json()
            except Exception:
                body = None

            errors = body.get("errors") if isinstance(body, dict) else None
            if errors and isinstance(errors, list):
                first_err = errors[0] if isinstance(errors[0], dict) else {"message": str(errors[0])}
                code = safe_int(first_err.get("code"), 0)
                msg = str(first_err.get("message") or str(errors))
                if code in _CHALLENGE_CODES:
                    api_state["halt"] = ("locked_challenge", f"X 安全锁定 (code={code}): {msg}")
                elif code in _RATE_LIMIT_CODES:
                    api_state["halt"] = ("rate_limited", f"X 关注频率/数量受限 (code={code}): {msg}")
                elif code in _PROTECTED_CODES:
                    api_state["skip"] = ("skipped_protected", msg)
                elif code in _UNAVAILABLE_CODES:
                    api_state["skip"] = ("skipped_unavailable", msg)
                else:
                    api_state["error"] = f"code={code}: {msg}"
                return

            if status in (401, 403, 429):
                h_st = "not_logged_in" if status == 401 else "rate_limited"
                api_state["halt"] = (h_st, f"friendships/create 返回 HTTP {status}")
                return

            if status == 200:
                api_state["ok"] = True
        except Exception:
            pass

    page.on("response", _on_follow_resp)
    try:
        _robust_click(follow_btn)
        deadline = time.time() + 10.0
        while time.time() < deadline:
            if api_state["halt"] or api_state["skip"] or api_state["ok"]:
                break
            try:
                unfollow_now = header_pl.locator('button[data-testid$="-unfollow"]').first
                if unfollow_now.count() > 0 and unfollow_now.is_visible():
                    api_state["ok"] = True
                    break
                cancel_now = header_pl.locator('button[data-testid$="-cancel"]').first
                if cancel_now.count() > 0 and cancel_now.is_visible():
                    api_state["skip"] = ("skipped_protected", "follow_requested_pending")
                    break
                toast_loc = page.locator('div[data-testid="toast"]').first
                if toast_loc.count() > 0 and toast_loc.is_visible():
                    toast_txt = (toast_loc.inner_text() or "").strip()
                    toast_low = toast_txt.lower()
                    if any(tok in toast_low for tok in _TOAST_RATE_LIMIT_TOKENS):
                        api_state["halt"] = ("rate_limited", f"页面弹出关注限流提示: {toast_txt[:120]}")
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
        log.error(f"🚨 [AutoFollower/触发风控熔断] @{handle} | {h_st}: {h_msg}")
        return {"outcome": "halted", "halt_status": h_st, "halt_warning": h_msg, "live_profile": merged_prof, "error": h_msg}
    if api_state["skip"]:
        s_out, s_err = api_state["skip"]
        return {"outcome": s_out, "halt_status": None, "halt_warning": None, "live_profile": merged_prof, "error": s_err}
    if api_state["ok"]:
        page.wait_for_timeout(random.randint(900, 1600))
        return {"outcome": "followed", "halt_status": None, "halt_warning": None, "live_profile": merged_prof, "error": None}

    return {
        "outcome": "failed",
        "halt_status": None,
        "halt_warning": None,
        "live_profile": merged_prof,
        "error": api_state["error"] or "follow_unconfirmed",
    }


def execute_follows_for_account(
    account_cfg: Dict[str, Any],
    *,
    db: Any,
    dry_run: bool = False,
    force_sync_followbacks: bool = False,
    follow_back_last_sync: Optional[Dict[str, float]] = None,
    config: Optional[Dict[str, Any]] = None,
    logger: Any = None,
) -> Dict[str, Any]:
    """为单个配置好的运营账号执行本轮候选筛选、回关核验与关注动作。"""
    log = logger or get_growth_logger("x_growth_follower")
    eff_cfg = {**FOLLOWER_DEFAULTS, **(config or {}), **(account_cfg.get("overrides") or {})}
    if dry_run:
        eff_cfg["dry_run"] = True

    acc_name = str(account_cfg.get("name") or "")
    acc_handle = str(account_cfg.get("screen_name") or "").strip().lstrip("@")
    session_path = str(account_cfg.get("user_data_dir") or "")
    is_dry = bool(eff_cfg.get("dry_run", False))
    max_per_run = max(1, safe_int(eff_cfg.get("max_follow_per_run"), 2))
    max_per_day = max(0, safe_int(eff_cfg.get("max_follow_per_day"), 20))
    browser_visible = bool(eff_cfg.get("browser_visible", False))
    action_delay = eff_cfg.get("action_delay_range") or (15, 30)
    self_cap = safe_int(eff_cfg.get("self_following_safety_cap"), 4800)
    fb_interval = max(300, safe_int(eff_cfg.get("follow_back_sync_interval_seconds"), 14400))

    follows_24h = count_recent_follows(db, acc_name, window_seconds=86400)
    if (not is_dry) and max_per_day > 0 and follows_24h >= max_per_day:
        log.info(f"⏸️ [AutoFollower/每日上限 | {acc_name}] 近24h已关注 {follows_24h}/{max_per_day} 人，本轮跳过。")
        return {
            "account_name": acc_name,
            "screen_name": acc_handle,
            "user_data_dir": session_path,
            "status": "daily_limit_reached",
            "dry_run": False,
            "warning": None,
            "follows_24h": follows_24h,
            "max_follow_per_day": max_per_day,
            "eligible_count": 0,
            "followed_count": 0,
            "followed_items": [],
            "skipped_items": [],
            "follow_back_sync": None,
        }

    remaining_daily = (max_per_day - follows_24h) if max_per_day > 0 else max_per_run
    target_limit = min(max_per_run, max(1, remaining_daily)) if (not is_dry and max_per_day > 0) else max_per_run
    buffer_limit = max(target_limit * 4, 12)

    candidates, sel_stats = select_follow_candidates_for_account(
        db,
        account_name=acc_name,
        account_dir=session_path,
        account_handle=acc_handle,
        limit=buffer_limit,
        config=eff_cfg,
    )

    if is_dry:
        preview_items = []
        for c in candidates[:target_limit]:
            desc_short = str(c.get("description") or "").replace("\n", " ").strip()[:60]
            preview_items.append(
                {
                    "handle": f"@{c['screen_name']}",
                    "tier": c.get("tier"),
                    "score": c.get("follow_score"),
                    "followers": c.get("followers_count"),
                    "following": c.get("following_count"),
                    "ff_ratio": c.get("ff_ratio"),
                    "languages": c.get("languages"),
                    "signals": c.get("signals"),
                    "bio": desc_short,
                }
            )
            log.info(
                f"[AutoFollower/预览 | {acc_name}] [{c.get('tier')}|{c.get('follow_score')}分] @{c['screen_name']} "
                f"(粉={c.get('followers_count')}, 关注={c.get('following_count')}, ratio={c.get('ff_ratio')}) "
                f"signals={c.get('signals')} | {desc_short!r}"
            )
        return {
            "account_name": acc_name,
            "screen_name": acc_handle,
            "user_data_dir": session_path,
            "status": "ok",
            "dry_run": True,
            "warning": None,
            "follows_24h": follows_24h,
            "max_follow_per_day": max_per_day,
            "eligible_count": sel_stats["total_eligible"],
            "tier_counts": sel_stats["tiers"],
            "warm_leads_available": sel_stats["warm_leads_available"],
            "followed_count": len(preview_items),
            "followed_items": preview_items,
            "skipped_items": [],
            "follow_back_sync": None,
        }

    if not os.path.isdir(session_path):
        warn_msg = (
            f"账号 {acc_name} (@{acc_handle or '未登录'}) 会话目录不存在: {session_path}；"
            f"请先运行 `python x_growth/auto_follower.py --login {acc_name}` 人工扫码登录后自动纳入轮换"
        )
        log.warning(f"⚠️ [AutoFollower/会话缺失 | {acc_name}] {warn_msg}")
        return {
            "account_name": acc_name,
            "screen_name": acc_handle,
            "user_data_dir": session_path,
            "status": "session_missing",
            "dry_run": False,
            "warning": warn_msg,
            "follows_24h": follows_24h,
            "max_follow_per_day": max_per_day,
            "eligible_count": sel_stats["total_eligible"],
            "followed_count": 0,
            "followed_items": [],
            "skipped_items": [],
            "follow_back_sync": None,
        }

    last_fb_ts = (follow_back_last_sync or {}).get(acc_name, 0.0)
    should_sync_fb = bool(force_sync_followbacks) or ((time.time() - float(last_fb_ts)) >= fb_interval)
    if not candidates and not should_sync_fb:
        log.info(f"ℹ️ [AutoFollower/暂无候选 | {acc_name}] 库内当前无可关注候选且未到回关核验周期，本轮免开浏览器。")
        return {
            "account_name": acc_name,
            "screen_name": acc_handle,
            "user_data_dir": session_path,
            "status": "ok",
            "dry_run": False,
            "warning": None,
            "follows_24h": follows_24h,
            "max_follow_per_day": max_per_day,
            "eligible_count": 0,
            "followed_count": 0,
            "followed_items": [],
            "skipped_items": [],
            "follow_back_sync": None,
        }

    proxy_server = get_proxy_url()
    sniffer = XPassiveGraphQLSniffer()
    followed_items: List[Dict[str, Any]] = []
    skipped_items: List[Dict[str, Any]] = []
    fb_sync_res: Optional[Dict[str, Any]] = None
    halt_status: Optional[str] = None
    halt_warning: Optional[str] = None

    with persistent_browser_session(
        session_path,
        debug=browser_visible,
        proxy=proxy_server,
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
            page.wait_for_timeout(random.randint(1500, 2600))
            _dismiss_overlays(page, aggressive=False, desc="x-follow-home")
            check_for_crash(page)

            acc_status, acc_msg = inspect_x_page_account_status(page)
            if acc_status != "ok":
                log.error(f"🚨 [AutoFollower/账号状态阻断 | {acc_name}] status={acc_status} | {acc_msg}")
                return {
                    "account_name": acc_name,
                    "screen_name": acc_handle,
                    "user_data_dir": session_path,
                    "status": acc_status,
                    "dry_run": False,
                    "warning": acc_msg,
                    "follows_24h": follows_24h,
                    "max_follow_per_day": max_per_day,
                    "eligible_count": sel_stats["total_eligible"],
                    "followed_count": 0,
                    "followed_items": [],
                    "skipped_items": [],
                    "follow_back_sync": None,
                }

            _self_uid, live_handle = extract_logged_in_identity(context, page)
            eff_self_handle = live_handle or acc_handle

            if should_sync_fb and eff_self_handle:
                fb_sync_res = sync_follow_backs_in_session(
                    page, eff_self_handle, db=db, account_name=acc_name, logger=log
                )
                if follow_back_last_sync is not None:
                    follow_back_last_sync[acc_name] = time.time()

            # 己方账号 5,000 关注上限安全保护
            self_prof = sniffer.users_by_handle.get(eff_self_handle.lower()) if eff_self_handle else None
            if isinstance(self_prof, dict) and self_cap > 0:
                s_fg = safe_int(self_prof.get("following_count"), 0)
                s_fc = safe_int(self_prof.get("followers_count"), 0)
                if s_fg >= self_cap and (s_fg / max(s_fc, 1)) > 1.05:
                    cap_warn = f"己方账号 @{eff_self_handle} 关注数已达 {s_fg}/{self_cap} (粉丝={s_fc})，触发 5000 关注上限防卡死保护"
                    log.warning(f"⏸️ [AutoFollower/关注上限保护 | {acc_name}] {cap_warn}")
                    return {
                        "account_name": acc_name,
                        "screen_name": eff_self_handle,
                        "user_data_dir": session_path,
                        "status": "self_cap_reached",
                        "dry_run": False,
                        "warning": cap_warn,
                        "follows_24h": follows_24h,
                        "max_follow_per_day": max_per_day,
                        "eligible_count": sel_stats["total_eligible"],
                        "followed_count": 0,
                        "followed_items": [],
                        "skipped_items": [],
                        "follow_back_sync": fb_sync_res,
                    }

            attempted = 0
            for cand in candidates:
                if len(followed_items) >= target_limit:
                    break
                if attempted > 0:
                    _sleep_jitter(action_delay)
                attempted += 1

                res = follow_candidate_in_session(page, sniffer, cand, config=eff_cfg, logger=log)
                outcome = res["outcome"]
                live_prof = res.get("live_profile") or cand

                if outcome == "halted":
                    halt_status = res.get("halt_status") or "rate_limited"
                    halt_warning = res.get("halt_warning") or res.get("error")
                    break

                record_follow_outcome(
                    db,
                    account_name=acc_name,
                    account_handle=eff_self_handle,
                    account_dir=session_path,
                    target_profile=live_prof,
                    status=outcome,
                    evaluation=cand,
                    error=res.get("error"),
                    followed_back=(outcome == "already_mutual"),
                )

                if outcome == "followed":
                    followed_items.append(
                        {
                            "handle": f"@{cand['screen_name']}",
                            "tier": cand.get("tier"),
                            "score": cand.get("follow_score"),
                            "ff_ratio": cand.get("ff_ratio"),
                            "signals": cand.get("signals"),
                        }
                    )
                    log.info(
                        f"✅ [AutoFollower/关注成功 | {acc_name}(@{eff_self_handle})] "
                        f"[{cand.get('tier')}|{cand.get('follow_score')}分] @{cand['screen_name']} "
                        f"(粉={live_prof.get('followers_count')}, 关注={live_prof.get('following_count')}, ratio={cand.get('ff_ratio')}) "
                        f"signals={cand.get('signals')}"
                    )
                else:
                    skipped_items.append({"handle": f"@{cand['screen_name']}", "reason": outcome, "detail": res.get("error")})
                    log.info(f"⏭️ [AutoFollower/跳过 | {acc_name}] @{cand['screen_name']} -> {outcome} ({res.get('error') or ''})")
        finally:
            try:
                page.remove_listener("response", sniffer.on_response)
            except Exception:
                pass

    return {
        "account_name": acc_name,
        "screen_name": acc_handle,
        "user_data_dir": session_path,
        "status": halt_status or "ok",
        "dry_run": False,
        "warning": halt_warning,
        "follows_24h": follows_24h + len(followed_items),
        "max_follow_per_day": max_per_day,
        "eligible_count": sel_stats["total_eligible"],
        "followed_count": len(followed_items),
        "followed_items": followed_items,
        "skipped_items": skipped_items,
        "follow_back_sync": fb_sync_res,
    }


def _save_runtime_status_heartbeat(
    *,
    round_no: int,
    refresh_res: Dict[str, Any],
    account_results: Sequence[Dict[str, Any]],
    next_run_at: Optional[str],
    summary: Dict[str, Any],
) -> None:
    status_path = get_runtime_file_path(STATUS_RUNTIME_FILE)
    existing = {}
    if status_path.is_file():
        try:
            existing = read_json(status_path) or {}
        except Exception:
            existing = {}

    merged_accounts = dict(existing.get("accounts") or {})
    for res in account_results:
        name = str(res.get("account_name") or "")
        if not name:
            continue
        merged_accounts[name] = {
            "screen_name": res.get("screen_name"),
            "user_data_dir": res.get("user_data_dir"),
            "status": res.get("status"),
            "warning": res.get("warning"),
            "follows_24h": res.get("follows_24h", 0),
            "max_follow_per_day": res.get("max_follow_per_day", 0),
            "followed_this_round": res.get("followed_count", 0),
            "followed_items": res.get("followed_items") or [],
            "updated_at": now_iso_cn(),
        }

    payload = {
        "round_no": int(round_no),
        "last_run_at": now_iso_cn(),
        "next_run_at": next_run_at,
        "fetch_mode": refresh_res.get("fetch_mode"),
        "candidate_pool": summary.get("candidate_pool"),
        "accounts": merged_accounts,
        "by_tier": summary.get("by_tier"),
        "by_signal": summary.get("by_signal"),
    }
    save_json(status_path, payload)


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
    """执行单轮完整互关巡检（刷新候选池 -> 按账号逐一执行关注 -> 输出并持久化运维战报）。"""
    log = logger or get_growth_logger("x_growth_follower")
    cfg = {**FOLLOWER_DEFAULTS, **(config or {})}
    pool = list(accounts) if accounts is not None else _resolve_follower_pool()
    db = gen_db_object()

    col_state = collector_state if collector_state is not None else {"cooldown_until": None}
    refresh_res = refresh_follow_candidates(
        db,
        force_fetch=force_fetch,
        allow_browser_harvest=(not dry_run),
        collector_cooldown_until=col_state.get("cooldown_until"),
        account_pool=pool,
        config=cfg,
        logger=log,
    )
    col_state["cooldown_until"] = refresh_res.get("collector_cooldown_until")

    cooldown_map = cooldown_until if cooldown_until is not None else {}
    risk_cd = max(600, safe_int(cfg.get("risk_cooldown_seconds"), 21600))
    account_results: List[Dict[str, Any]] = []

    for idx, acc in enumerate(pool):
        acc_name = acc["name"]
        acc_dir = acc["user_data_dir"]
        rem = float(cooldown_map.get(acc_dir, 0.0) or 0.0) - time.time()
        if (not dry_run) and rem > 0:
            rem_sec = int(rem)
            log.info(f"⏸️ [AutoFollower/风控冷却 | {acc_name}] 独立冷却中（剩余 {rem_sec}s），本轮跳过。")
            account_results.append(
                {
                    "account_name": acc_name,
                    "screen_name": acc.get("screen_name"),
                    "user_data_dir": acc_dir,
                    "status": "cooldown",
                    "dry_run": False,
                    "warning": f"风控/登录冷却中，剩余 {rem_sec}s",
                    "follows_24h": count_recent_follows(db, acc_name, window_seconds=86400),
                    "max_follow_per_day": safe_int(
                        (acc.get("overrides") or {}).get("max_follow_per_day"), cfg["max_follow_per_day"]
                    ),
                    "eligible_count": 0,
                    "followed_count": 0,
                    "followed_items": [],
                    "skipped_items": [],
                    "follow_back_sync": None,
                }
            )
            continue

        if idx > 0 and (not dry_run):
            _sleep_jitter((8, 16))

        res = execute_follows_for_account(
            acc,
            db=db,
            dry_run=dry_run,
            force_sync_followbacks=force_sync_followbacks,
            follow_back_last_sync=follow_back_last_sync,
            config=cfg,
            logger=log,
        )
        account_results.append(res)

        st = str(res.get("status") or "ok")
        if (not dry_run) and st in _COOLDOWN_TRIGGER_STATUSES:
            cooldown_map[acc_dir] = time.time() + risk_cd
            log.error(
                f"⚠️ [AutoFollower/独立冷却生效 | {acc_name}] status={st} -> 暂停该账号 {risk_cd}s（不影响其他账号）"
            )
        elif (not dry_run) and st == "ok":
            cooldown_map.pop(acc_dir, None)

    summary = build_follow_ops_summary(db, pool, config=cfg)
    if not dry_run:
        _save_runtime_status_heartbeat(
            round_no=round_no,
            refresh_res=refresh_res,
            account_results=account_results,
            next_run_at=next_run_at,
            summary=summary,
        )

    return {
        "status": "ok",
        "round_no": round_no,
        "dry_run": dry_run,
        "refresh": refresh_res,
        "accounts": account_results,
        "summary": summary,
    }


def run_auto_follower_daemon(
    *,
    account_pool: Optional[Sequence[Any]] = None,
    account_filter: Optional[str] = None,
    interval_seconds: Optional[int] = None,
    jitter_seconds: Optional[Tuple[int, int]] = None,
    max_rounds: int = 0,
    dry_run: bool = False,
    force_fetch: bool = False,
    force_sync_followbacks: bool = False,
    overrides: Optional[Dict[str, Any]] = None,
) -> None:
    """常驻每小时自动互关守护进程：采集协同 -> 双号轮换关注 -> 心跳落盘 -> 运维看板打印。"""
    log = get_growth_logger("x_growth_follower")
    base_interval = max(120, int(interval_seconds or FOLLOWER_DEFAULTS.get("interval_seconds", 3600)))
    raw_jitter = jitter_seconds if jitter_seconds is not None else FOLLOWER_DEFAULTS.get("jitter_seconds", (180, 480))
    if isinstance(raw_jitter, (list, tuple)) and len(raw_jitter) == 2:
        j_low, j_high = max(0, int(raw_jitter[0])), max(0, int(raw_jitter[1]))
        if j_low > j_high:
            j_low, j_high = j_high, j_low
    else:
        j_low, j_high = 0, 0

    pool = _resolve_follower_pool(account_pool=account_pool, account_filter=account_filter, overrides=overrides)
    if not pool:
        log.error("❌ [AutoFollower/启动失败] 未解析到任何已启用的关注账号。")
        return

    pool_desc = ", ".join(f"{a['name']}(@{a['screen_name'] or '?'})" for a in pool)
    log.info(
        f"⏰ [AutoFollower/常驻守护启动] 执行账号({len(pool)}个)=[{pool_desc}] | "
        f"周期={base_interval}s + 抖动({j_low}~{j_high}s) | 小号目录={FOLLOWER_DEFAULTS['collector_user_data_dir']}"
    )

    cooldown_until: Dict[str, float] = {}
    follow_back_last_sync: Dict[str, float] = {}
    collector_state: Dict[str, Optional[float]] = {"cooldown_until": None}

    def _step(round_no: int) -> Dict[str, Any]:
        log.info(f"========== [AutoFollower/轮次 #{round_no}] 开始执行 ==========")
        sleep_sec = base_interval + (random.randint(j_low, j_high) if j_high > 0 else 0)
        next_run_at = (datetime.now(TZ_CN) + timedelta(seconds=sleep_sec)).strftime("%Y-%m-%d %H:%M:%S")

        res = run_follow_round(
            round_no=round_no,
            accounts=pool,
            dry_run=dry_run,
            force_fetch=(force_fetch and round_no == 1),
            force_sync_followbacks=(force_sync_followbacks and round_no == 1),
            cooldown_until=cooldown_until,
            follow_back_last_sync=follow_back_last_sync,
            collector_state=collector_state,
            next_run_at=next_run_at,
            logger=log,
        )

        acc_brief = [
            f"{a['account_name']}:{a['status']}(+{a['followed_count']}, 24h={a['follows_24h']}/{a['max_follow_per_day']})"
            for a in res["accounts"]
        ]
        log.info(
            f"✅ [AutoFollower/轮次 #{round_no} 完成] 候选来源={res['refresh'].get('fetch_mode')} | "
            f"活跃池={res['refresh'].get('qualified_count', 0)}人 | {' | '.join(acc_brief)}"
        )
        log.info("\n" + format_ops_summary_text(res["summary"]))
        log.info(f"💤 [AutoFollower/休眠] 休眠 {sleep_sec} 秒，下一轮将于 [{next_run_at}] 执行...")
        return {"next_sleep_seconds": sleep_sec}

    run_daemon_loop(
        _step,
        interval_seconds=base_interval,
        additive_jitter_range=(j_low, j_high),
        name="AutoFollower",
        logger_obj=log,
        max_rounds=max_rounds,
    )


def print_ops_status(*, account_filter: Optional[str] = None) -> Dict[str, Any]:
    """只读打印当前候选池库存、各账号 24h/累计关注与各档真实回关率看板。"""
    db = gen_db_object()
    pool = _resolve_follower_pool(account_filter=account_filter)
    sync_candidates_from_posts_db(db, lookback_days=FOLLOWER_DEFAULTS["sync_posts_lookback_days"])
    summary = build_follow_ops_summary(db, pool)
    status_path = get_runtime_file_path(STATUS_RUNTIME_FILE)
    rt_status = read_json(status_path) if status_path.is_file() else None
    print(format_ops_summary_text(summary, runtime_status=rt_status))
    return summary


def main(argv: Optional[Sequence[str]] = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    parser = argparse.ArgumentParser(description="X 平台高互关账号筛选与每小时自动关注系统")
    parser.add_argument("--once", action="store_true", help="仅运行一轮后退出（不进入每小时常驻循环）。")
    parser.add_argument("--dry-run", action="store_true", help="零浏览器纯查库预览待关注候选名单，不写入关注记录。")
    parser.add_argument("--status", action="store_true", help="只读打印候选池、24h配额与各档真实回关率运维看板。")
    parser.add_argument("--check-followbacks", action="store_true", help="强制本轮顺路打开粉丝列表核验最新回关。")
    parser.add_argument("--force-fetch", action="store_true", help="强制触发一次小号被动采集刷新候选池。")
    parser.add_argument("--account", default=None, help="指定单个或多个账号别名/用户名（逗号分隔，如 x_myself）。")
    parser.add_argument("--max-follow", type=int, default=None, help="覆盖单轮单账号最大关注人数。")
    parser.add_argument("--interval-seconds", type=int, default=None, help="覆盖常驻轮询基础间隔秒数（默认 3600）。")
    parser.add_argument("--visible", action="store_true", help="显示 Chrome 窗口运行（调试或人工观察）。")
    parser.add_argument("--login", default=None, help="打开指定账号（如 x_dahao）的可见浏览器供人工扫码/登录固化会话。")
    args = parser.parse_args(argv)

    if args.login:
        pool = _resolve_follower_pool(account_filter=args.login)
        if not pool:
            print(f"❌ 未在配置中找到账号 `{args.login}`；可选账号: {[a['name'] for a in _resolve_follower_pool()]}")
            return 2
        target = pool[0]
        Path(target["user_data_dir"]).mkdir(parents=True, exist_ok=True)
        print(f"🔑 正在为账号 [{target['name']} @{target.get('screen_name')}] 打开浏览器，请完成登录后直接关闭窗口保存会话: {target['user_data_dir']}")
        open_browser_for_manual_use(user_data_dir=target["user_data_dir"], home_url=X_HOME_URL)
        return 0

    if args.status:
        print_ops_status(account_filter=args.account)
        return 0

    cli_overrides: Dict[str, Any] = {}
    if args.max_follow is not None:
        cli_overrides["max_follow_per_run"] = max(1, int(args.max_follow))
    if args.visible:
        cli_overrides["browser_visible"] = True
    if args.dry_run:
        cli_overrides["dry_run"] = True

    run_once = args.once or args.dry_run or (not FOLLOWER_DEFAULTS.get("continuous_loop", True))
    if run_once:
        pool = _resolve_follower_pool(account_filter=args.account, overrides=cli_overrides)
        res = run_follow_round(
            round_no=1,
            accounts=pool,
            dry_run=bool(args.dry_run),
            force_fetch=bool(args.force_fetch),
            force_sync_followbacks=bool(args.check_followbacks),
        )
        print(format_ops_summary_text(res["summary"]))
        print(json.dumps({"refresh": res["refresh"], "accounts": res["accounts"]}, ensure_ascii=False, indent=2, default=str))
        return 0

    run_auto_follower_daemon(
        account_filter=args.account,
        interval_seconds=args.interval_seconds or FOLLOWER_DEFAULTS.get("interval_seconds", 3600),
        jitter_seconds=FOLLOWER_DEFAULTS.get("jitter_seconds", (180, 480)),
        force_fetch=bool(args.force_fetch),
        force_sync_followbacks=bool(args.check_followbacks),
        overrides=cli_overrides,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
