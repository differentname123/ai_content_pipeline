# -*- coding: utf-8 -*-
"""
Pure Passive X Action Executor & Queue Daemon (`platforms/x_ops_daemon.py`)

Single-instance headed Chrome executor built on `platforms/x_browser.py` + `platforms/x_playwright.py`:
- Zero autonomous polling, zero embedded LLM logic, zero default actions on its own
- Lazy single-Chrome reuse: stays completely idle until upper applications enqueue tasks via `enqueue_x_action`
- Extensible `x_ops_queue` atomic action registry (`post`, `reply`, `follow`, `pin`, `sniff_notifications`, or custom handlers)
- Passive safety guardrails (daily/hourly quotas + 0-second 48h persistent `XAccountCircuitBreaker`)
"""
import json
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import random
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
import uuid

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.common_utils import get_config, resolve_session_dir, safe_int, setup_logger
from common.daemon import run_daemon_loop
from common.mongo_db.mongo_base import gen_db_object
from platforms.x_browser import (
    BusinessErrorException,
    ChallengeRequiredError,
    DEFAULT_FREEZE_SECONDS,
    DEFAULT_X_STATE_COLLECTION,
    XAccountCircuitBreaker,
    XBrowserSession,
    assert_x_page_healthy,
    human_pause,
)
from platforms.x_playwright import (
    _wait_x_home_ready,
    follow_on_x_page,
    open_browser_for_manual_use,
    pin_on_x_page,
    post_on_x_page,
    reply_on_x_page,
    sniff_notifications_on_page,
)

logger = setup_logger(app_name="x_ops_daemon")

# ==============================================================================
# 一、常用运行配置区（改完保存后直接运行本文件即可生效，无需任何命令行参数）
# ==============================================================================

# 1. 运行模式开关（修改这里的值后，直接运行本文件即可切换模式）：
#   - "status"       : 只读查看当前账号配额、熔断状态与待执行队列（不开浏览器）
#   - "daemon"       : 纯被动监听队列执行器（只等待并执行上层投递来的任务，自身不做任何默认主动动作；队列空时不拉起浏览器）
#   - "once"         : 执行一轮当前队列里到期的任务后立即退出
#   - "smoke_test"   : 打开本地正版 Chrome 做只读体检（检测反风控指纹、登录态、通知嗅探，不发帖）
#   - "login"        : 打开同一个本地正版 Chrome 人工登录或过验证（关掉窗口即自动保存登录态并退出）
#   - "clear_freeze" : 确认风控解除后，手动清除 48 小时熔断冷却锁
RUN_MODE: str = "daemon"

# 2. 浏览器是否走全局代理（True = 使用全局配置代理，False = 直连）
USE_PROXY: bool = True

# 3. 当前操作的 X 账号配置（当前严格默认小号/测试号 x_myself，严禁在风控未解除前启用大号）
DEFAULT_ACCOUNT: Dict[str, Any] = {
    "name": "x_myself",
    "user_data_dir": resolve_session_dir("x_myself_browser_session_dir", "x_myself"),
    "screen_name": str(get_config("x_myself_screen_name", "peterzhu1003") or "peterzhu1003")
    .strip()
    .lstrip("@"),
}
# 【日后切换大号模板】等大号风控完全解除、且普通浏览器已退出大号登录后，再把上面注释掉、打开下面这段并修改 screen_name：
# DEFAULT_ACCOUNT = {
#     "name": "x_dahao",
#     "user_data_dir": resolve_session_dir("x_dahao_browser_session_dir", "x_dahao"),
#     "screen_name": str(get_config("x_dahao_screen_name", "你的大号Handle不含@") or "")
#     .strip()
#     .lstrip("@"),
# }

# 4. 发帖防刷底线（单日最多几条主帖、任意两条主帖至少间隔多少秒，7200 秒 = 2 小时）
# 【保留过滤原因说明】单日主帖上限 5 篇、任意两帖间隔 >= 2 小时，避开 AuthorDiversityScorer 同作者自相杀与刷屏特征
MAX_POSTS_PER_DAY: int = 5
MIN_POST_INTERVAL_SECONDS: int = 7200

# 5. 回复防刷底线（每日上限 / 每小时上限 / 同一主帖最多回几条 / 超过几小时的老帖不回）
# 【保留过滤原因说明】聚焦黄金 24h 主帖的高价值互动，限制单帖<=3、1h<=4、24h<=15，吃满算法权重并远离刷评红线
MAX_REPLIES_PER_DAY: int = 15
MAX_REPLIES_PER_HOUR: int = 4
MAX_REPLIES_PER_POST: int = 3
REPLY_PARENT_MAX_AGE_HOURS: int = 24

# 6. 辅助动作防刷底线（每日最多关注几人 / 两次关注至少间隔秒数 / 每日最多置顶几次）
# 【保留过滤原因说明】低频辅助动作硬上限（日关<=3、两次关注间隔>=15分钟、日置顶<=1），远离 Follow Bot 突发特征
MAX_FOLLOWS_PER_DAY: int = 3
MIN_FOLLOW_INTERVAL_SECONDS: int = 900
MAX_PINS_PER_DAY: int = 1

# 7. 守护轮询、写动作间距、内存回收与熔断（查队列秒数 / 同批次相邻写动作随机停顿秒数 / 空闲重启小时 / 冻结秒数）
POLL_LOOP_SECONDS: int = 15
# 【保留过滤原因说明】同一批次相邻写动作之间随机停顿 35~90s，避免 0 秒连射触发反垃圾突发风控模型
INTER_WRITE_DELAY_SECONDS: Tuple[int, int] = (35, 90)
# 【保留过滤原因说明】长驻浏览器满 8 小时在空闲期温和重启一次，清理页面长期运行的内存累积
SESSION_REFRESH_HOURS: int = 8
# 【保留过滤原因说明】触发任意安全验证或风控挑战立即 0 秒冻结 48h，绝不继续撞墙
FREEZE_ON_CHALLENGE_SECONDS: int = DEFAULT_FREEZE_SECONDS

# ==============================================================================
# 内部固定常量（通常无需改动）
# ==============================================================================
TZ_CN = timezone(timedelta(hours=8))
QUEUE_COLLECTION = "x_ops_queue"
STATE_COLLECTION = DEFAULT_X_STATE_COLLECTION
_WRITE_ACTION_TYPES = frozenset({"post", "reply", "follow", "pin"})

_INDEXED_DBS: set = set()


def _now_iso(now_ts: Optional[int] = None) -> str:
    ts = int(time.time()) if now_ts is None else int(now_ts)
    return datetime.fromtimestamp(ts, tz=TZ_CN).isoformat(timespec="seconds")


def _resolve_col(db: Any, name: str) -> Any:
    if db is None:
        return gen_db_object().get_collection(name)
    get_col = getattr(db, "get_collection", None)
    if callable(get_col):
        return get_col(name)
    if hasattr(db, "db") and db.db is not None:
        return db.db[name]
    return db[name]


def _ensure_indexes(db: Any) -> None:
    key = id(getattr(db, "db", db)) if db is not None else 0
    if key and key in _INDEXED_DBS:
        return
    try:
        col = _resolve_col(db, QUEUE_COLLECTION)
        ci = getattr(col, "create_index", None)
        if callable(ci):
            ci([("account_name", 1), ("status", 1), ("scheduled_at_ts", 1)])
    except Exception:
        pass
    if key:
        _INDEXED_DBS.add(key)


class XAccountStateStore:
    """Stores per-account action timestamps, per-post reply counts, and replied IDs in `x_ops_state`."""

    def __init__(self, db: Any = None, collection_name: str = STATE_COLLECTION) -> None:
        self._db = db
        self.collection_name = collection_name

    def _col(self) -> Any:
        return _resolve_col(self._db, self.collection_name)

    @staticmethod
    def _doc_id(account_name: str) -> str:
        return f"account:{str(account_name or '').strip() or 'default'}"

    def get_state(self, account_name: str) -> Dict[str, Any]:
        return self._col().find_one({"_id": self._doc_id(account_name)}) or {}

    def count_recent_actions(
        self,
        account_name: str,
        kind: str,
        window_seconds: int,
        *,
        now_ts: Optional[int] = None,
    ) -> int:
        now = int(time.time()) if now_ts is None else int(now_ts)
        doc = self.get_state(account_name)
        ts_list = ((doc.get("action_timestamps") or {}).get(kind)) or []
        cutoff = now - max(1, int(window_seconds))
        return sum(1 for t in ts_list if safe_int(t, 0) > cutoff and safe_int(t, 0) <= now)

    def last_action_ts(self, account_name: str, kind: str) -> int:
        doc = self.get_state(account_name)
        ts_list = ((doc.get("action_timestamps") or {}).get(kind)) or []
        return max((safe_int(t, 0) for t in ts_list), default=0)

    def oldest_action_ts_in_window(
        self,
        account_name: str,
        kind: str,
        window_seconds: int,
        *,
        now_ts: Optional[int] = None,
    ) -> int:
        now = int(time.time()) if now_ts is None else int(now_ts)
        doc = self.get_state(account_name)
        ts_list = ((doc.get("action_timestamps") or {}).get(kind)) or []
        cutoff = now - max(1, int(window_seconds))
        in_win = [safe_int(t, 0) for t in ts_list if cutoff < safe_int(t, 0) <= now]
        return min(in_win) if in_win else 0

    def record_action(self, account_name: str, kind: str, *, now_ts: Optional[int] = None) -> None:
        now = int(time.time()) if now_ts is None else int(now_ts)
        doc = self.get_state(account_name)
        all_ts = dict(doc.get("action_timestamps") or {})
        cutoff = now - 48 * 3600
        kept = [safe_int(t, 0) for t in (all_ts.get(kind) or []) if safe_int(t, 0) > cutoff]
        kept.append(now)
        all_ts[kind] = kept[-200:]
        self._col().update_one(
            {"_id": self._doc_id(account_name)},
            {
                "$set": {
                    "account_name": account_name,
                    "action_timestamps": all_ts,
                    "updated_at_ts": now,
                    "updated_at_iso": _now_iso(now),
                }
            },
            upsert=True,
        )

    def get_post_reply_count(self, account_name: str, post_id: str) -> int:
        pid = str(post_id or "").strip()
        if not pid:
            return 0
        doc = self.get_state(account_name)
        counts = doc.get("post_reply_counts") or {}
        return safe_int(counts.get(pid), 0)

    def is_reply_recorded(self, account_name: str, reply_id: str) -> bool:
        doc = self.get_state(account_name)
        return str(reply_id or "").strip() in set(doc.get("replied_ids") or [])

    def record_reply_for_post(
        self,
        account_name: str,
        post_id: str,
        reply_id: str,
        *,
        now_ts: Optional[int] = None,
    ) -> None:
        now = int(time.time()) if now_ts is None else int(now_ts)
        doc = self.get_state(account_name)
        replied_ids = [str(x) for x in (doc.get("replied_ids") or [])]
        rid = str(reply_id or "").strip()
        if rid and rid not in replied_ids:
            replied_ids.append(rid)
        replied_ids = replied_ids[-500:]

        counts = {str(k): safe_int(v, 0) for k, v in (doc.get("post_reply_counts") or {}).items() if k}
        pid = str(post_id or "").strip()
        if pid:
            next_cnt = counts.pop(pid, 0) + 1
            counts[pid] = next_cnt
            if len(counts) > 100:
                counts = dict(list(counts.items())[-100:])

        self._col().update_one(
            {"_id": self._doc_id(account_name)},
            {
                "$set": {
                    "account_name": account_name,
                    "replied_ids": replied_ids,
                    "post_reply_counts": counts,
                    "updated_at_ts": now,
                    "updated_at_iso": _now_iso(now),
                }
            },
            upsert=True,
        )


def enqueue_x_action(
    account_name: str,
    action_type: str,
    payload: Optional[Dict[str, Any]] = None,
    *,
    scheduled_at_ts: Optional[int] = None,
    db: Any = None,
    now_ts: Optional[int] = None,
) -> str:
    """Enqueue an X browser action into `x_ops_queue` and return its document `_id`."""
    _ensure_indexes(db)
    now = int(time.time()) if now_ts is None else int(now_ts)
    sched = int(scheduled_at_ts) if scheduled_at_ts is not None else now
    item_id = f"{action_type}_{now}_{uuid.uuid4().hex[:8]}"
    doc = {
        "_id": item_id,
        "account_name": str(account_name or DEFAULT_ACCOUNT["name"]).strip(),
        "action_type": str(action_type or "").strip(),
        "payload": dict(payload or {}),
        "status": "pending",
        "scheduled_at_ts": sched,
        "created_at_ts": now,
        "created_at_iso": _now_iso(now),
        "executed_at_ts": 0,
        "executed_at_iso": "",
        "result": None,
        "error": "",
    }
    _resolve_col(db, QUEUE_COLLECTION).insert_one(doc)
    return item_id


def get_x_action(item_id: str, *, db: Any = None) -> Optional[Dict[str, Any]]:
    """Fetch a queued action document from `x_ops_queue` (contains `status`, `result`, `error`)."""
    doc = _resolve_col(db, QUEUE_COLLECTION).find_one({"_id": str(item_id or "").strip()})
    return dict(doc) if isinstance(doc, dict) else None


def wait_x_action(
    item_id: str,
    *,
    timeout_seconds: float = 120.0,
    poll_interval: float = 1.0,
    db: Any = None,
) -> Dict[str, Any]:
    """Poll `x_ops_queue` until the action completes (`done` / `skipped` / `failed`), then return the doc."""
    clean_id = str(item_id or "").strip()
    deadline = time.monotonic() + max(0.0, float(timeout_seconds))
    sleep_sec = max(0.01, float(poll_interval))
    while True:
        doc = get_x_action(clean_id, db=db)
        if doc is None:
            raise KeyError(f"Queue action not found: {clean_id}")
        if str(doc.get("status") or "pending") != "pending":
            return doc
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"Timed out waiting for queue action: {clean_id}")
        time.sleep(min(sleep_sec, remaining))


class DaemonContext:
    """Execution context passed to every registered atomic action handler."""

    def __init__(
        self,
        *,
        account: Dict[str, Any],
        page: Any,
        session: Any,
        db: Any,
        state_store: XAccountStateStore,
        breaker: XAccountCircuitBreaker,
        cfg: Dict[str, Any],
        now_ts: int,
    ) -> None:
        self.account = account
        self.page = page
        self.session = session
        self.db = db
        self.state_store = state_store
        self.breaker = breaker
        self.cfg = cfg
        self.now_ts = now_ts


ActionHandler = Callable[[DaemonContext, Dict[str, Any]], Dict[str, Any]]
_ACTION_HANDLERS: Dict[str, ActionHandler] = {}


def register_action_handler(action_type: str, handler_fn: ActionHandler) -> None:
    """Register or override a pluggable action handler for `XOpsDaemon`."""
    clean = str(action_type or "").strip()
    if not clean or not callable(handler_fn):
        raise ValueError("action_type 与 handler_fn 均不能为空")
    _ACTION_HANDLERS[clean] = handler_fn


def get_registered_action_handlers() -> Dict[str, ActionHandler]:
    return dict(_ACTION_HANDLERS)


def _cooldown_outcome(payload: Dict[str, Any], *, defer_until_ts: int, error: str) -> Dict[str, Any]:
    """冷却类临时限制默认顺延排期（保持 pending）；传入 defer_on_cooldown=False 时才直接跳过。"""
    if defer_until_ts > 0 and bool(payload.get("defer_on_cooldown", True)):
        return {"status": "deferred", "defer_until_ts": int(defer_until_ts), "error": error}
    return {"status": "skipped", "error": error}


def _handle_post(ctx: DaemonContext, item: Dict[str, Any]) -> Dict[str, Any]:
    acc_name = ctx.account["name"]
    payload = item.get("payload") or {}
    cfg = ctx.cfg

    day_cnt = ctx.state_store.count_recent_actions(acc_name, "post", 24 * 3600, now_ts=ctx.now_ts)
    if day_cnt >= cfg["max_posts_per_day"]:
        return {"status": "skipped", "error": f"post_daily_cap_reached ({day_cnt}/{cfg['max_posts_per_day']})"}

    last_ts = ctx.state_store.last_action_ts(acc_name, "post")
    if last_ts > 0 and (ctx.now_ts - last_ts) < cfg["min_post_interval_seconds"]:
        rem = cfg["min_post_interval_seconds"] - (ctx.now_ts - last_ts)
        return _cooldown_outcome(
            payload,
            defer_until_ts=last_ts + int(cfg["min_post_interval_seconds"]) + 5,
            error=f"post_interval_cooldown ({rem}s remaining)",
        )

    post_id = post_on_x_page(
        ctx.page,
        content=payload.get("content", ""),
        image_path_list=payload.get("image_path_list"),
        text_list=payload.get("text_list"),
        account_name=acc_name,
        breaker=ctx.breaker,
        scroll_before=bool(payload.get("scroll_before", True)),
    )
    ctx.state_store.record_action(acc_name, "post", now_ts=ctx.now_ts)
    return {"status": "done", "result": {"post_id": post_id}}


def _decode_x_snowflake_ts(tweet_id: str) -> int:
    s = str(tweet_id or "").strip()
    if not (s.isdigit() and 15 <= len(s) <= 22):
        return 0
    try:
        ts = ((int(s) >> 22) + 1288834974657) // 1000
        return ts if ts >= 1_500_000_000 else 0
    except Exception:
        return 0


def _check_reply_budget(ctx: DaemonContext, root_post_id: str, parent_pub_ts: int) -> Tuple[Optional[str], int]:
    acc_name = ctx.account["name"]
    cfg = ctx.cfg
    eff_parent_ts = safe_int(parent_pub_ts, 0) or _decode_x_snowflake_ts(root_post_id)
    if eff_parent_ts > 0 and (ctx.now_ts - eff_parent_ts) > cfg["reply_parent_max_age_hours"] * 3600:
        return f"parent_post_too_old (>{cfg['reply_parent_max_age_hours']}h)", 0

    if root_post_id:
        replied_on_post = ctx.state_store.get_post_reply_count(acc_name, root_post_id)
        if replied_on_post >= cfg["max_replies_per_post"]:
            return f"per_post_reply_cap ({replied_on_post}/{cfg['max_replies_per_post']})", 0

    day_cnt = ctx.state_store.count_recent_actions(acc_name, "reply", 24 * 3600, now_ts=ctx.now_ts)
    if day_cnt >= cfg["max_replies_per_day"]:
        return f"reply_daily_cap ({day_cnt}/{cfg['max_replies_per_day']})", 0

    hr_cnt = ctx.state_store.count_recent_actions(acc_name, "reply", 3600, now_ts=ctx.now_ts)
    if hr_cnt >= cfg["max_replies_per_hour"]:
        oldest_hr = ctx.state_store.oldest_action_ts_in_window(acc_name, "reply", 3600, now_ts=ctx.now_ts)
        defer_until = (oldest_hr + 3615) if oldest_hr > 0 else (ctx.now_ts + 600)
        return f"reply_hourly_cap ({hr_cnt}/{cfg['max_replies_per_hour']})", defer_until

    return None, 0


def _handle_reply(ctx: DaemonContext, item: Dict[str, Any]) -> Dict[str, Any]:
    acc_name = ctx.account["name"]
    payload = item.get("payload") or {}
    target = str(payload.get("target_post_url") or payload.get("reply_id") or payload.get("post_id") or "").strip()
    root_pid = str(payload.get("root_post_id") or payload.get("post_id") or "").strip()
    parent_pub_ts = safe_int(payload.get("parent_publish_ts"), 0)

    dedup_id = str(payload.get("reply_id") or target).strip()
    if dedup_id and ctx.state_store.is_reply_recorded(acc_name, dedup_id):
        return {"status": "skipped", "error": f"already_replied ({dedup_id})"}

    deny_reason, defer_until = _check_reply_budget(ctx, root_pid, parent_pub_ts)
    if deny_reason:
        return _cooldown_outcome(payload, defer_until_ts=defer_until, error=deny_reason)

    # 【保留过滤原因说明】彻底关闭回复前自动顺手点赞，消除双写自动化序列特征
    comment_id = reply_on_x_page(
        ctx.page,
        target_post_url=target,
        content=payload.get("content", ""),
        image_path_list=payload.get("image_path_list"),
        like=False,
        text_list=payload.get("text_list"),
        account_name=acc_name,
        breaker=ctx.breaker,
    )
    ctx.state_store.record_action(acc_name, "reply", now_ts=ctx.now_ts)
    ctx.state_store.record_reply_for_post(
        acc_name,
        root_pid or target,
        dedup_id,
        now_ts=ctx.now_ts,
    )
    return {"status": "done", "result": {"reply_id": comment_id}}


def _handle_follow(ctx: DaemonContext, item: Dict[str, Any]) -> Dict[str, Any]:
    acc_name = ctx.account["name"]
    cfg = ctx.cfg
    payload = item.get("payload") or {}
    day_cnt = ctx.state_store.count_recent_actions(acc_name, "follow", 24 * 3600, now_ts=ctx.now_ts)
    if day_cnt >= cfg["max_follows_per_day"]:
        return {"status": "skipped", "error": f"follow_daily_cap ({day_cnt}/{cfg['max_follows_per_day']})"}

    last_ts = ctx.state_store.last_action_ts(acc_name, "follow")
    if last_ts > 0 and (ctx.now_ts - last_ts) < cfg["min_follow_interval_seconds"]:
        rem = cfg["min_follow_interval_seconds"] - (ctx.now_ts - last_ts)
        return _cooldown_outcome(
            payload,
            defer_until_ts=last_ts + int(cfg["min_follow_interval_seconds"]) + 5,
            error=f"follow_interval_cooldown ({rem}s remaining)",
        )

    target = str(payload.get("screen_name") or payload.get("target_screen_name") or "").strip()
    status = follow_on_x_page(
        ctx.page,
        target_screen_name=target,
        account_name=acc_name,
        breaker=ctx.breaker,
    )
    if status == "FOLLOWED":
        ctx.state_store.record_action(acc_name, "follow", now_ts=ctx.now_ts)
    return {"status": "done", "result": {"screen_name": target, "follow_status": status}}


def _handle_pin(ctx: DaemonContext, item: Dict[str, Any]) -> Dict[str, Any]:
    acc_name = ctx.account["name"]
    cfg = ctx.cfg
    payload = item.get("payload") or {}
    day_cnt = ctx.state_store.count_recent_actions(acc_name, "pin", 24 * 3600, now_ts=ctx.now_ts)
    if day_cnt >= cfg["max_pins_per_day"]:
        return {"status": "skipped", "error": f"pin_daily_cap ({day_cnt}/{cfg['max_pins_per_day']})"}

    target_post_id = str(payload.get("post_id") or payload.get("tweet_id") or "").strip()
    status = pin_on_x_page(
        ctx.page,
        target_post_id=target_post_id,
        eff_handle=ctx.account.get("screen_name", ""),
        account_name=acc_name,
        breaker=ctx.breaker,
    )
    if status == "PINNED":
        ctx.state_store.record_action(acc_name, "pin", now_ts=ctx.now_ts)
    return {"status": "done", "result": {"post_id": target_post_id, "pin_status": status}}


def _handle_sniff_notifications(ctx: DaemonContext, item: Dict[str, Any]) -> Dict[str, Any]:
    acc_name = ctx.account["name"]
    payload = item.get("payload") or {}
    sniffed = sniff_notifications_on_page(
        ctx.page,
        own_handle=ctx.account.get("screen_name", ""),
        scroll_rounds=safe_int(payload.get("scroll_rounds"), 1),
        account_name=acc_name,
        breaker=ctx.breaker,
    )
    if ctx.session is not None:
        try:
            ctx.session.goto_home(wait_ms=900)
        except Exception:
            pass
    return {"status": "done", "result": sniffed}


register_action_handler("post", _handle_post)
register_action_handler("reply", _handle_reply)
register_action_handler("follow", _handle_follow)
register_action_handler("pin", _handle_pin)
register_action_handler("sniff_notifications", _handle_sniff_notifications)


class XOpsDaemon:
    """Passive single-Chrome executor: performs ONLY actions explicitly enqueued by upper callers."""

    def __init__(
        self,
        account: Optional[Dict[str, Any]] = None,
        *,
        db: Any = None,
        session: Optional[XBrowserSession] = None,
        state_store: Optional[XAccountStateStore] = None,
        breaker: Optional[XAccountCircuitBreaker] = None,
        cfg: Optional[Dict[str, Any]] = None,
        stop_event: Optional[threading.Event] = None,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        self.account = dict(account or DEFAULT_ACCOUNT)
        self._db = db
        self.state_store = state_store or XAccountStateStore(db)
        self.breaker = breaker or XAccountCircuitBreaker(db, default_freeze_seconds=FREEZE_ON_CHALLENGE_SECONDS)
        self.stop_event = stop_event
        self.sleep_fn = sleep_fn
        self.cfg = {
            "max_posts_per_day": MAX_POSTS_PER_DAY,
            "min_post_interval_seconds": MIN_POST_INTERVAL_SECONDS,
            "max_replies_per_day": MAX_REPLIES_PER_DAY,
            "max_replies_per_hour": MAX_REPLIES_PER_HOUR,
            "max_replies_per_post": MAX_REPLIES_PER_POST,
            "reply_parent_max_age_hours": REPLY_PARENT_MAX_AGE_HOURS,
            "max_follows_per_day": MAX_FOLLOWS_PER_DAY,
            "min_follow_interval_seconds": MIN_FOLLOW_INTERVAL_SECONDS,
            "max_pins_per_day": MAX_PINS_PER_DAY,
            "inter_write_delay_seconds": INTER_WRITE_DELAY_SECONDS,
            "session_refresh_hours": SESSION_REFRESH_HOURS,
            "poll_loop_seconds": POLL_LOOP_SECONDS,
            **(cfg or {}),
        }
        self.session = session or XBrowserSession(self.account["user_data_dir"], use_proxy=USE_PROXY)
        self._session_started_ts: int = 0

    def _queue_col(self) -> Any:
        _ensure_indexes(self._db)
        return _resolve_col(self._db, QUEUE_COLLECTION)

    def _close_session_if_open(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass
        self._session_started_ts = 0

    def _update_queue_item(
        self,
        item_id: str,
        *,
        status: str,
        now_ts: int,
        result: Any = None,
        error: str = "",
    ) -> None:
        self._queue_col().update_one(
            {"_id": item_id},
            {
                "$set": {
                    "status": status,
                    "executed_at_ts": now_ts,
                    "executed_at_iso": _now_iso(now_ts),
                    "result": result,
                    "error": str(error or ""),
                }
            },
        )

    def _defer_queue_item(
        self,
        item_id: str,
        *,
        defer_until_ts: int,
        now_ts: int,
        error: str = "",
    ) -> None:
        self._queue_col().update_one(
            {"_id": item_id},
            {
                "$set": {
                    "status": "pending",
                    "scheduled_at_ts": int(defer_until_ts),
                    "deferred_until_iso": _now_iso(int(defer_until_ts)),
                    "updated_at_ts": now_ts,
                    "error": str(error or ""),
                }
            },
        )

    def run_once(self, *, now_ts: Optional[int] = None) -> Dict[str, Any]:
        now = int(time.time()) if now_ts is None else int(now_ts)
        acc_name = self.account["name"]

        # 1. Circuit breaker guard
        frozen, rem_sec, reason = self.breaker.is_frozen(acc_name, now_ts=now)
        if frozen:
            self._close_session_if_open()
            logger.warning(f"⏸️ [XOpsDaemon | {acc_name}] 账号处于熔断冻结中（剩余 {rem_sec}s）| 原因: {reason}")
            return {"status": "frozen", "remaining_seconds": rem_sec, "next_sleep_seconds": min(rem_sec, 1800)}

        # 2. Gentle session refresh after N hours
        refresh_sec = max(3600, int(self.cfg["session_refresh_hours"]) * 3600)
        if self.session.is_alive() and self._session_started_ts > 0 and (now - self._session_started_ts) >= refresh_sec:
            logger.info(f"🔄 [XOpsDaemon | {acc_name}] 长驻会话满 {self.cfg['session_refresh_hours']}h，温和重启回收内存...")
            self._close_session_if_open()

        # 3. Fetch due actions from x_ops_queue
        q_col = self._queue_col()
        due_items = list(
            q_col.find(
                {"account_name": acc_name, "status": "pending", "scheduled_at_ts": {"$lte": now}}
            ).sort([("scheduled_at_ts", 1), ("created_at_ts", 1)])
        )
        if not due_items:
            return {"status": "idle", "due": 0, "executed": 0, "skipped": 0, "deferred": 0, "failed": 0}

        # 4. Lazily start/reuse single headed Chrome only when tasks exist
        was_alive = self.session.is_alive()
        page = self.session.ensure_alive()
        if not was_alive:
            self._session_started_ts = now
            try:
                self.session.goto_home(wait_ms=1200)
                _wait_x_home_ready(page)
                assert_x_page_healthy(page, account_name=acc_name, breaker=self.breaker)
            except ChallengeRequiredError as ch_err:
                self._close_session_if_open()
                return {"status": "frozen_challenge", "error": str(ch_err), "executed": 0}
            except BusinessErrorException as auth_err:
                self._close_session_if_open()
                return {"status": "not_logged_in", "error": str(auth_err), "executed": 0}

        executed = 0
        failed = 0
        skipped = 0
        deferred = 0
        writes_executed = 0
        for item in due_items:
            if self.stop_event is not None and self.stop_event.is_set():
                break

            item_now = int(time.time()) if now_ts is None else int(now_ts)
            frozen_mid, _, reason_mid = self.breaker.is_frozen(acc_name, now_ts=item_now)
            if frozen_mid:
                self._close_session_if_open()
                return {
                    "status": "frozen_challenge",
                    "error": reason_mid,
                    "executed": executed,
                    "skipped": skipped,
                    "deferred": deferred,
                    "failed": failed,
                }

            item_id = str(item.get("_id"))
            action_type = str(item.get("action_type") or "").strip()
            handler = _ACTION_HANDLERS.get(action_type)
            if handler is None:
                self._update_queue_item(item_id, status="failed", now_ts=item_now, error=f"unknown_action_type: {action_type}")
                failed += 1
                continue

            if writes_executed > 0 and action_type in _WRITE_ACTION_TYPES:
                lo_d, hi_d = self.cfg["inter_write_delay_seconds"]
                if float(hi_d) > 0:
                    delay_s = random.uniform(max(0.0, float(lo_d)), max(float(lo_d), float(hi_d)))
                    self.sleep_fn(delay_s)
                    if now_ts is None:
                        item_now = int(time.time())

            ctx = DaemonContext(
                account=self.account,
                page=page,
                session=self.session,
                db=self._db,
                state_store=self.state_store,
                breaker=self.breaker,
                cfg=self.cfg,
                now_ts=item_now,
            )
            try:
                outcome = handler(ctx, item)
                st = str(outcome.get("status") or "done")
                if st == "deferred":
                    defer_until = safe_int(outcome.get("defer_until_ts"), item_now + 60)
                    self._defer_queue_item(
                        item_id,
                        defer_until_ts=defer_until,
                        now_ts=item_now,
                        error=str(outcome.get("error") or ""),
                    )
                    deferred += 1
                else:
                    self._update_queue_item(
                        item_id,
                        status=st,
                        now_ts=item_now,
                        result=outcome.get("result"),
                        error=str(outcome.get("error") or ""),
                    )
                    if st == "done":
                        executed += 1
                        if action_type in _WRITE_ACTION_TYPES:
                            writes_executed += 1
                    elif st == "skipped":
                        skipped += 1
                    else:
                        failed += 1
            except ChallengeRequiredError as ch_err:
                frozen_now, _, _ = self.breaker.is_frozen(acc_name, now_ts=item_now)
                if not frozen_now:
                    self.breaker.trip(acc_name, reason=str(ch_err), url=str(getattr(page, "url", "") or ""), now_ts=item_now)
                self._update_queue_item(item_id, status="failed", now_ts=item_now, error=str(ch_err))
                self._close_session_if_open()
                return {
                    "status": "frozen_challenge",
                    "error": str(ch_err),
                    "executed": executed,
                    "skipped": skipped,
                    "deferred": deferred,
                    "failed": failed + 1,
                }
            except Exception as exc:
                self._update_queue_item(item_id, status="failed", now_ts=item_now, error=str(exc))
                failed += 1
                logger.exception(f"❌ [XOpsDaemon | {acc_name}] 执行任务 {item_id}({action_type}) 异常: {exc}")
                frozen_after, _, reason_after = self.breaker.is_frozen(acc_name, now_ts=item_now)
                if frozen_after:
                    self._close_session_if_open()
                    return {
                        "status": "frozen_challenge",
                        "error": reason_after or str(exc),
                        "executed": executed,
                        "skipped": skipped,
                        "deferred": deferred,
                        "failed": failed,
                    }

        return {
            "status": "ok",
            "due": len(due_items),
            "executed": executed,
            "skipped": skipped,
            "deferred": deferred,
            "failed": failed,
        }

    def run_daemon(self, max_rounds: int = 0) -> int:
        logger.info(
            f"🚀 [XOpsDaemon 启动] 账号={self.account['name']}(@{self.account.get('screen_name')}) "
            f"| 纯被动排队执行模式 | 轮询={self.cfg['poll_loop_seconds']}s"
        )
        try:
            return run_daemon_loop(
                lambda _rnd: self.run_once(),
                interval_seconds=self.cfg["poll_loop_seconds"],
                jitter_ratio=0.15,
                name=f"x_ops_daemon:{self.account['name']}",
                logger_obj=logger,
                max_rounds=max_rounds,
                stop_event=self.stop_event,
            )
        finally:
            self._close_session_if_open()

    def smoke_test(self) -> Dict[str, Any]:
        """Non-destructive diagnostic: launch real headed Chrome on configured session dir, inspect stealth & health."""
        acc_name = self.account["name"]
        try:
            with XBrowserSession(self.account["user_data_dir"], use_proxy=USE_PROXY) as sess:
                page = sess.page
                page.goto("https://x.com/home", timeout=60000, wait_until="domcontentloaded")
                _wait_x_home_ready(page)
                human_pause(900, 1500, page=page)
                fp = page.evaluate(
                    """() => ({
                        webdriver: navigator.webdriver,
                        ua: navigator.userAgent,
                        brands: navigator.userAgentData ? navigator.userAgentData.brands : null,
                        visibilityState: document.visibilityState,
                        guardTimer: typeof window.__bnGuardTimer,
                    })"""
                )
                health_st, health_msg = assert_status_tuple(page)
                notif_res = None
                if health_st == "ok":
                    notif_res = sniff_notifications_on_page(
                        page,
                        own_handle=self.account.get("screen_name", ""),
                        scroll_rounds=1,
                        account_name=acc_name,
                        breaker=None,
                    )
                return {
                    "account_name": acc_name,
                    "session_dir": self.account["user_data_dir"],
                    "fingerprint": fp,
                    "health_status": health_st,
                    "health_message": health_msg,
                    "notif_sniff": (
                        {
                            "own_handle": notif_res.get("own_handle"),
                            "own_uid": notif_res.get("own_uid"),
                            "notifications_count": notif_res.get("notifications_count"),
                            "items_count": len(notif_res.get("items") or []),
                        }
                        if notif_res is not None
                        else None
                    ),
                }
        finally:
            self._close_session_if_open()

    def format_status_report(self, *, now_ts: Optional[int] = None) -> str:
        now = int(time.time()) if now_ts is None else int(now_ts)
        acc_name = self.account["name"]
        frozen, rem, reason = self.breaker.is_frozen(acc_name, now_ts=now)
        p_day = self.state_store.count_recent_actions(acc_name, "post", 24 * 3600, now_ts=now)
        r_hr = self.state_store.count_recent_actions(acc_name, "reply", 3600, now_ts=now)
        r_day = self.state_store.count_recent_actions(acc_name, "reply", 24 * 3600, now_ts=now)
        f_day = self.state_store.count_recent_actions(acc_name, "follow", 24 * 3600, now_ts=now)
        pin_day = self.state_store.count_recent_actions(acc_name, "pin", 24 * 3600, now_ts=now)
        q_col = self._queue_col()
        pending_cnt = q_col.count_documents({"account_name": acc_name, "status": "pending"})
        return (
            f"=== XOpsDaemon Status ({acc_name} / @{self.account.get('screen_name')}) ===\n"
            f"Session Dir : {self.account['user_data_dir']}\n"
            f"Circuit     : {'FROZEN ' + str(rem) + 's (' + reason + ')' if frozen else 'OK (not frozen)'}\n"
            f"Quotas      : post_24h={p_day}/{self.cfg['max_posts_per_day']} | reply_1h={r_hr}/{self.cfg['max_replies_per_hour']} "
            f"| reply_24h={r_day}/{self.cfg['max_replies_per_day']} | follow_24h={f_day}/{self.cfg['max_follows_per_day']} "
            f"| pin_24h={pin_day}/{self.cfg['max_pins_per_day']}\n"
            f"Queue       : pending={pending_cnt}"
        )


def assert_status_tuple(page: Any) -> Tuple[str, str]:
    from platforms.x_browser import inspect_x_page_health

    return inspect_x_page_health(page)


def main(mode: Optional[str] = None) -> None:
    """按顶部 RUN_MODE（或显式传入的 mode）执行，完全不依赖任何命令行参数。"""
    selected = (mode or RUN_MODE).strip().lower()
    daemon = XOpsDaemon()
    if selected == "status":
        print(daemon.format_status_report())
    elif selected == "daemon":
        daemon.run_daemon()
    elif selected == "once":
        try:
            print(daemon.run_once())
        finally:
            daemon._close_session_if_open()
    elif selected == "smoke_test":
        res = daemon.smoke_test()
        print(json.dumps(res, ensure_ascii=False, indent=2))
    elif selected == "login":
        open_browser_for_manual_use(daemon.account["user_data_dir"], use_proxy=USE_PROXY)
    elif selected == "clear_freeze":
        daemon.breaker.clear(daemon.account["name"])
        print(daemon.format_status_report())
    else:
        raise ValueError(
            f"Unsupported RUN_MODE: {selected!r} "
            "(valid: 'status', 'daemon', 'once', 'smoke_test', 'login', 'clear_freeze')"
        )


if __name__ == "__main__":
    # 默认直接运行按顶部 RUN_MODE 执行（本模块是纯执行器，绝不会自行触发默认操作，只有在接到队列任务时才会动作）。
    # 如需单独跑下方某一个测试示例：先注释掉 `main()`，再取消对应示例块的注释后直接运行本文件即可。
    main()

    # ==========================================================================
    # 示例 1：只读真机体检（验证 Chrome 指纹、登录态、通知嗅探，不会发任何内容）
    # ==========================================================================
    # main("smoke_test")

    # ==========================================================================
    # 示例 2：打开正版 Chrome 窗口人工登录 X 账号或处理安全验证（关窗自动保存 Session）
    # ==========================================================================
    # main("login")

    # ==========================================================================
    # 示例 3：往队列塞一条主帖（纯文字或带图）并立即执行一次（并取回 post_id）
    # 提示：当后台已常驻跑 RUN_MODE="daemon" 时，外部模块只需 enqueue_x_action(...)，需要结果再用 wait_x_action(qid)
    # ==========================================================================
    # daemon = XOpsDaemon()
    # qid = enqueue_x_action(
    #     daemon.account["name"],
    #     "post",
    #     {
    #         "content": f"测试发帖：保持节奏，持续输出一手观察。({_now_iso()})",
    #         "image_path_list": [
    #             # "example.jpg",  # 支持 media_downloads 目录下的文件名、完整本地路径或图片 URL
    #         ],
    #     },
    # )
    # print("已入队主帖任务 ID:", qid)
    # try:
    #     print("执行汇总:", daemon.run_once())
    #     print("任务详情:", get_x_action(qid))
    #     # 若后台已有另一进程在跑 daemon，上层模块改为直接等结果：
    #     # print("等待守护完成:", wait_x_action(qid, timeout_seconds=120))
    # finally:
    #     daemon._close_session_if_open()

    # ==========================================================================
    # 示例 4：回复指定帖子/评论（强制不点赞）并立即执行一次
    # ==========================================================================
    # daemon = XOpsDaemon()
    # qid = enqueue_x_action(
    #     daemon.account["name"],
    #     "reply",
    #     {
    #         "target_post_url": "https://x.com/peterzhu1003/status/请替换为目标帖子ID",
    #         "root_post_id": "请替换为目标主帖ID",  # 用于限制单主帖最多回复 MAX_REPLIES_PER_POST 条及 <24h 校验
    #         "content": "感谢分享思路，这个角度很实用。",
    #     },
    # )
    # print("已入队回复任务 ID:", qid)
    # try:
    #     print(daemon.run_once())
    #     print(get_x_action(qid))
    # finally:
    #     daemon._close_session_if_open()

    # ==========================================================================
    # 示例 5：纯只读拉取一次站内通知列表（只返回原始数据给上层应用，不生成文案也不自动回复）
    # ==========================================================================
    # daemon = XOpsDaemon()
    # qid = enqueue_x_action(daemon.account["name"], "sniff_notifications", {"scroll_rounds": 1})
    # print("已入队只读拉评任务 ID:", qid)
    # try:
    #     print(daemon.run_once())
    #     res_doc = get_x_action(qid) or {}
    #     print("拉到的通知条数:", len(((res_doc.get("result") or {}).get("items")) or []))
    # finally:
    #     daemon._close_session_if_open()

    # ==========================================================================
    # 示例 6：低频辅助动作 —— 置顶自己某条主帖 / 关注某个账号
    # ==========================================================================
    # daemon = XOpsDaemon()
    # qid_pin = enqueue_x_action(daemon.account["name"], "pin", {"post_id": "请替换为要置顶的帖子ID"})
    # # qid_follow = enqueue_x_action(daemon.account["name"], "follow", {"screen_name": "对方Handle不含@"})
    # print("已入队置顶任务 ID:", qid_pin)
    # try:
    #     print(daemon.run_once())
    # finally:
    #     daemon._close_session_if_open()

    # ==========================================================================
    # 示例 7：人工过验证后手动解除 48h 熔断 + 注册自定义扩展动作示例
    # ==========================================================================
    # main("clear_freeze")
    #
    # def _my_custom_action(ctx: DaemonContext, item: Dict[str, Any]) -> Dict[str, Any]:
    #     print("收到自定义扩展任务参数:", item.get("payload"), "当前浏览器URL:", ctx.page.url)
    #     return {"status": "done", "result": {"ok": True}}
    #
    # register_action_handler("custom_demo", _my_custom_action)
    # daemon = XOpsDaemon()
    # enqueue_x_action(daemon.account["name"], "custom_demo", {"hello": "x_ops"})
    # try:
    #     print(daemon.run_once())
    # finally:
    #     daemon._close_session_if_open()
