# -*- coding: utf-8 -*-
"""
Unified Persistent X Operations Daemon & Action Queue (`platforms/x_ops_daemon.py`)

Single-instance headed Chrome architecture built on `platforms/x_browser.py` + `platforms/x_playwright.py`:
- Zero `--headless=new`, zero repeated cold-starts per action
- Extensible `x_ops_queue` action plugin registry (`post`, `reply`, `sync_and_reply`, `follow`, `pin`)
- Zero-auth comment sentinel (`fetch_x_tweet_zero_auth`) polls `comment_count` deltas with zero account risk
- Global 0-second persistent circuit breaker (`XAccountCircuitBreaker`)
- Rolling 1h/24h safety quotas (time-of-day rest scheduling belongs to upper callers)
"""
import json
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import random
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
import uuid

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.common_utils import get_config, resolve_session_dir, safe_int, setup_logger
from common.daemon import run_daemon_loop
from common.llm_gateway import call_llm_json, render_prompt_template
from common.mongo_db.mongo_base import gen_db_object
from common.x_text import calc_x_weighted_length
from platforms.x_api import fetch_x_tweet_zero_auth
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
#   - "status"       : 只读查看当前账号配额、熔断状态与待执行队列（默认值，误点运行也不会打开浏览器或发帖）
#   - "daemon"       : 长驻后台守护运行（持续执行队列任务 + 自动巡检评论）
#   - "once"         : 只跑一轮当前队列里到期的任务（顺带检查一次评论哨兵）然后自动退出
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

# 4. 发帖频率控制（单日最多几条主帖、任意两条主帖至少间隔多少秒，7200 秒 = 2 小时）
# 【保留过滤原因说明】单日主帖上限 5 篇、任意两帖间隔 >= 2 小时，避开 AuthorDiversityScorer 同作者自相杀与刷屏特征
MAX_POSTS_PER_DAY: int = 5
MIN_POST_INTERVAL_SECONDS: int = 7200

# 5. 自动回复控制（每日上限 / 每小时上限 / 同一主帖最多回几条 / 超过几小时的老帖不回 / 连续回复随机间隔秒数）
# 【保留过滤原因说明】聚焦黄金 24h 主帖的前排高价值互动，限制单帖<=3、1h<=4、24h<=15，吃满算法权重并远离刷评红线
MAX_REPLIES_PER_DAY: int = 15
MAX_REPLIES_PER_HOUR: int = 4
MAX_REPLIES_PER_POST: int = 3
REPLY_PARENT_MAX_AGE_HOURS: int = 24
INTER_REPLY_DELAY_RANGE: Tuple[int, int] = (45, 110)

# 6. 低频辅助动作上限（每日最多关注几人 / 每日最多置顶几次）
# 【保留过滤原因说明】低频辅助动作硬上限（日关<=3、日置顶<=1），远离 Follow Bot 与高频资料改动阈值
MAX_FOLLOWS_PER_DAY: int = 3
MAX_PINS_PER_DAY: int = 1

# 7. 巡检与保活节奏（评论哨兵免登轮询间隔秒数 / 守护循环每轮等待秒数 / 浏览器连续运行几小时重启清内存）
# 【保留过滤原因说明】免登评论哨兵每 40 分钟仅轮询近 24h 自发主帖的公开 comment_count，0 账号风控且按需触发嗅探
SENTINEL_INTERVAL_SECONDS: int = 2400
POLL_LOOP_SECONDS: int = 60
# 【保留过滤原因说明】长驻浏览器满 8 小时在空闲期温和重启一次，清理页面长期运行的内存累积
SESSION_REFRESH_HOURS: int = 8

# 8. 触发人机验证 / 安全挑战后的自动冻结时长（秒，默认 48 小时，防止继续操作把小限制打成深降权）
# 【保留过滤原因说明】触发任意安全验证或风控挑战立即 0 秒冻结 48h，绝不继续撞墙
FREEZE_ON_CHALLENGE_SECONDS: int = DEFAULT_FREEZE_SECONDS

# 9. 自动生成回复内容使用的模型分组（"low" / "medium" / "high"，严禁硬编码具体模型名）
GENERATOR_MODEL_GROUP: str = "low"

# ==============================================================================
# 内部固定常量（通常无需改动）
# ==============================================================================
TZ_CN = timezone(timedelta(hours=8))
QUEUE_COLLECTION = "x_ops_queue"
STATE_COLLECTION = DEFAULT_X_STATE_COLLECTION
REPLY_PROMPT_REL_PATH = "x_growth/auto_reply_comment.md"
TRACKED_POST_WINDOW_HOURS: int = 24
MAX_REPLY_WEIGHTED_CHARS: int = 240

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
    """Stores per-account action timestamps, tracked posts, and replied IDs in `x_ops_state`."""

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

    def track_own_post(
        self,
        account_name: str,
        post_id: str,
        publish_ts: int,
        *,
        text: str = "",
        comment_count: int = 0,
        now_ts: Optional[int] = None,
    ) -> None:
        pid = str(post_id or "").strip()
        if not pid:
            return
        now = int(time.time()) if now_ts is None else int(now_ts)
        doc = self.get_state(account_name)
        tracked = dict(doc.get("tracked_posts") or {})
        prev = dict(tracked.get(pid) or {})
        tracked[pid] = {
            "post_id": pid,
            "publish_ts": safe_int(publish_ts, now) or now,
            "text": str(text or prev.get("text") or "")[:400],
            "last_comment_count": max(safe_int(prev.get("last_comment_count"), 0), safe_int(comment_count, 0)),
            "replied_count": safe_int(prev.get("replied_count"), 0),
        }
        # Prune oldest beyond 30 tracked posts to bound state doc growth
        if len(tracked) > 30:
            sorted_keys = sorted(tracked.keys(), key=lambda k: safe_int(tracked[k].get("publish_ts"), 0), reverse=True)
            tracked = {k: tracked[k] for k in sorted_keys[:30]}
        self._col().update_one(
            {"_id": self._doc_id(account_name)},
            {
                "$set": {
                    "account_name": account_name,
                    "tracked_posts": tracked,
                    "updated_at_ts": now,
                    "updated_at_iso": _now_iso(now),
                }
            },
            upsert=True,
        )

    def update_tracked_post_sentinel(
        self,
        account_name: str,
        post_id: str,
        comment_count: int,
        *,
        now_ts: Optional[int] = None,
    ) -> None:
        now = int(time.time()) if now_ts is None else int(now_ts)
        pid = str(post_id or "").strip()
        if not pid:
            return
        doc = self.get_state(account_name)
        tracked = dict(doc.get("tracked_posts") or {})
        entry = dict(tracked.get(pid) or {"post_id": pid, "publish_ts": now, "replied_count": 0})
        entry["last_comment_count"] = safe_int(comment_count, 0)
        tracked[pid] = entry
        self._col().update_one(
            {"_id": self._doc_id(account_name)},
            {
                "$set": {
                    "account_name": account_name,
                    "tracked_posts": tracked,
                    "updated_at_ts": now,
                    "updated_at_iso": _now_iso(now),
                }
            },
            upsert=True,
        )

    def get_tracked_post(self, account_name: str, post_id: str) -> Dict[str, Any]:
        doc = self.get_state(account_name)
        return dict((doc.get("tracked_posts") or {}).get(str(post_id or "").strip()) or {})

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
        tracked = dict(doc.get("tracked_posts") or {})
        pid = str(post_id or "").strip()
        if pid:
            entry = dict(tracked.get(pid) or {"post_id": pid, "publish_ts": now, "last_comment_count": 0})
            entry["replied_count"] = safe_int(entry.get("replied_count"), 0) + 1
            tracked[pid] = entry
        self._col().update_one(
            {"_id": self._doc_id(account_name)},
            {
                "$set": {
                    "account_name": account_name,
                    "replied_ids": replied_ids,
                    "tracked_posts": tracked,
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


def check_comment_sentinel(
    account_name: str,
    *,
    db: Any = None,
    state_store: Optional[XAccountStateStore] = None,
    fetch_fn: Callable[[str], Optional[Dict[str, Any]]] = fetch_x_tweet_zero_auth,
    window_hours: int = TRACKED_POST_WINDOW_HOURS,
    max_replies_per_post: int = MAX_REPLIES_PER_POST,
    now_ts: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Zero-auth comment sentinel: checks public `comment_count` of recent tracked posts via `x_api`.
    Only enqueues `sync_and_reply` when a post has new unread comments.
    """
    now = int(time.time()) if now_ts is None else int(now_ts)
    store = state_store or XAccountStateStore(db)
    doc = store.get_state(account_name)
    tracked = dict(doc.get("tracked_posts") or {})
    cutoff = now - max(1, int(window_hours)) * 3600

    checked = 0
    changed_posts: List[str] = []
    for pid, meta in tracked.items():
        pub_ts = safe_int((meta or {}).get("publish_ts"), 0)
        replied_cnt = safe_int((meta or {}).get("replied_count"), 0)
        if pub_ts > 0 and pub_ts < cutoff:
            continue
        if replied_cnt >= max_replies_per_post:
            continue
        checked += 1
        try:
            std = fetch_fn(pid)
        except Exception as exc:
            logger.warning(f"[免登哨兵 | {account_name}] 拉取主帖 {pid} 状态异常: {exc}")
            continue
        if not isinstance(std, dict):
            continue
        live_cnt = safe_int((std.get("engagement") or {}).get("comment_count"), 0)
        prev_cnt = safe_int((meta or {}).get("last_comment_count"), 0)
        if live_cnt > prev_cnt:
            store.update_tracked_post_sentinel(account_name, pid, live_cnt, now_ts=now)
            changed_posts.append(pid)

    enqueued_id = ""
    if changed_posts:
        q_col = _resolve_col(db, QUEUE_COLLECTION)
        existing = q_col.find_one(
            {"account_name": account_name, "action_type": "sync_and_reply", "status": "pending"}
        )
        if not existing:
            enqueued_id = enqueue_x_action(
                account_name,
                "sync_and_reply",
                {"trigger": "zero_auth_sentinel", "post_ids": changed_posts},
                db=db,
                now_ts=now,
            )
            logger.info(
                f"🛰️ [免登哨兵 | {account_name}] 检测到主帖新评论增量: {changed_posts} -> 已入队 sync_and_reply ({enqueued_id})"
            )

    return {"checked": checked, "changed_posts": changed_posts, "enqueued_id": enqueued_id}


def _generate_reply_via_llm(own_handle: str, candidate: Dict[str, Any], *, now_ts: int) -> Dict[str, Any]:
    reply_ts = safe_int(candidate.get("reply_created_ts"), 0)
    age_desc = f"{max(0, (now_ts - reply_ts) // 60)} 分钟前" if reply_ts > 0 else "近期"
    parent_ctx = "读者直接回复了你的上述主帖。"
    if candidate.get("in_reply_to_status_id") and candidate.get("post_id") and (
        candidate["in_reply_to_status_id"] != candidate["post_id"]
    ):
        parent_ctx = "读者在你的上述主帖评论区继续追评回复了你。"

    rendered, meta = render_prompt_template(
        REPLY_PROMPT_REL_PATH,
        {
            "ACCOUNT_HANDLE": own_handle or DEFAULT_ACCOUNT["screen_name"],
            "MAX_WEIGHTED_CHARS": MAX_REPLY_WEIGHTED_CHARS,
            "MY_POST_TEXT": candidate.get("parent_text") or "(原帖正文暂不可见，仅结合读者评论谨慎回应)",
            "MY_POST_HAS_MEDIA": "是" if candidate.get("parent_has_media") else "否",
            "TIME_CONTEXT_BLOCK": f"当前距读者评论约 {age_desc}",
            "PARENT_CONTEXT_BLOCK": parent_ctx,
            "REPLY_AUTHOR": f"@{candidate.get('reply_author') or 'unknown'}",
            "REPLY_HAS_MEDIA": "是" if candidate.get("reply_has_media") else "否",
            "REPLY_TEXT": candidate.get("reply_text") or "",
        },
    )

    def _validate(obj: Any) -> Optional[str]:
        if not isinstance(obj, dict):
            return "输出必须为 JSON 对象"
        if bool(obj.get("skip")):
            return None
        txt = str(obj.get("reply_text") or "").strip()
        if not txt:
            return "skip=false 时 reply_text 不能为空"
        if calc_x_weighted_length(txt) > MAX_REPLY_WEIGHTED_CHARS:
            return f"reply_text 超出 {MAX_REPLY_WEIGHTED_CHARS} X 加权字符限制"
        return None

    data, llm_meta = call_llm_json(
        prompt=rendered,
        prompt_meta=meta,
        model=None,
        preset_model_group=GENERATOR_MODEL_GROUP,
        validator=_validate,
        timeout=120,
        schema_retries=2,
        task_label=f"x_ops_reply:{candidate.get('reply_id')}",
    )
    out = dict(data or {}) if isinstance(data, dict) else {"skip": True, "skip_reason": "llm_empty"}
    out["model_used"] = llm_meta.get("model_used") or ""
    return out


class DaemonContext:
    """Execution context passed to every registered action handler."""

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
        reply_generator_fn: Callable[[str, Dict[str, Any], int], Dict[str, Any]] = None,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        self.account = account
        self.page = page
        self.session = session
        self.db = db
        self.state_store = state_store
        self.breaker = breaker
        self.cfg = cfg
        self.now_ts = now_ts
        self.reply_generator_fn = reply_generator_fn or (
            lambda handle, cand, ts: _generate_reply_via_llm(handle, cand, now_ts=ts)
        )
        self.sleep_fn = sleep_fn


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
        return {"status": "skipped", "error": f"post_interval_cooldown ({rem}s remaining)"}

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
    ctx.state_store.track_own_post(
        acc_name,
        post_id,
        ctx.now_ts,
        text=str(payload.get("content") or ""),
        comment_count=0,
        now_ts=ctx.now_ts,
    )
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


def _check_reply_budget(ctx: DaemonContext, root_post_id: str, parent_pub_ts: int) -> Optional[str]:
    acc_name = ctx.account["name"]
    cfg = ctx.cfg
    eff_parent_ts = safe_int(parent_pub_ts, 0) or _decode_x_snowflake_ts(root_post_id)
    if eff_parent_ts > 0 and (ctx.now_ts - eff_parent_ts) > cfg["reply_parent_max_age_hours"] * 3600:
        return f"parent_post_too_old (>{cfg['reply_parent_max_age_hours']}h)"

    if root_post_id:
        tracked = ctx.state_store.get_tracked_post(acc_name, root_post_id)
        t_pub = safe_int(tracked.get("publish_ts"), 0)
        if t_pub > 0 and (ctx.now_ts - t_pub) > cfg["reply_parent_max_age_hours"] * 3600:
            return f"tracked_root_post_too_old (>{cfg['reply_parent_max_age_hours']}h)"
        if safe_int(tracked.get("replied_count"), 0) >= cfg["max_replies_per_post"]:
            return f"per_post_reply_cap ({tracked.get('replied_count')}/{cfg['max_replies_per_post']})"

    hr_cnt = ctx.state_store.count_recent_actions(acc_name, "reply", 3600, now_ts=ctx.now_ts)
    if hr_cnt >= cfg["max_replies_per_hour"]:
        return f"reply_hourly_cap ({hr_cnt}/{cfg['max_replies_per_hour']})"

    day_cnt = ctx.state_store.count_recent_actions(acc_name, "reply", 24 * 3600, now_ts=ctx.now_ts)
    if day_cnt >= cfg["max_replies_per_day"]:
        return f"reply_daily_cap ({day_cnt}/{cfg['max_replies_per_day']})"

    return None


def _handle_reply(ctx: DaemonContext, item: Dict[str, Any]) -> Dict[str, Any]:
    acc_name = ctx.account["name"]
    payload = item.get("payload") or {}
    target = str(payload.get("target_post_url") or payload.get("reply_id") or payload.get("post_id") or "").strip()
    root_pid = str(payload.get("root_post_id") or payload.get("post_id") or "").strip()
    parent_pub_ts = safe_int(payload.get("parent_publish_ts"), 0)

    deny_reason = _check_reply_budget(ctx, root_pid, parent_pub_ts)
    if deny_reason:
        return {"status": "skipped", "error": deny_reason}

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
        str(payload.get("reply_id") or target),
        now_ts=ctx.now_ts,
    )
    return {"status": "done", "result": {"reply_id": comment_id}}


def _handle_sync_and_reply(ctx: DaemonContext, item: Dict[str, Any]) -> Dict[str, Any]:
    acc_name = ctx.account["name"]
    cfg = ctx.cfg
    payload = item.get("payload") or {}

    sniffed = sniff_notifications_on_page(
        ctx.page,
        own_handle=ctx.account.get("screen_name", ""),
        scroll_rounds=safe_int(payload.get("scroll_rounds"), 2),
        account_name=acc_name,
        breaker=ctx.breaker,
    )
    own_handle = sniffed.get("own_handle") or ctx.account.get("screen_name", "")
    candidates = list(sniffed.get("items") or [])

    sent: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    for cand in candidates:
        hr_cnt = ctx.state_store.count_recent_actions(acc_name, "reply", 3600, now_ts=ctx.now_ts)
        day_cnt = ctx.state_store.count_recent_actions(acc_name, "reply", 24 * 3600, now_ts=ctx.now_ts)
        if hr_cnt >= cfg["max_replies_per_hour"] or day_cnt >= cfg["max_replies_per_day"] or len(sent) >= 3:
            break

        rid = str(cand.get("reply_id") or "").strip()
        root_pid = str(cand.get("post_id") or "").strip()
        if not rid or ctx.state_store.is_reply_recorded(acc_name, rid):
            continue

        reply_text_raw = str(cand.get("reply_text") or "").strip()
        if not reply_text_raw:
            skipped.append({"reply_id": rid, "reason": "empty_reply_text"})
            continue

        reply_ts = safe_int(cand.get("reply_created_ts"), 0)
        parent_ts = safe_int(cand.get("parent_publish_ts"), 0) or reply_ts
        deny_reason = _check_reply_budget(ctx, root_pid, parent_ts)
        if deny_reason:
            skipped.append({"reply_id": rid, "reason": deny_reason})
            continue

        gen = ctx.reply_generator_fn(own_handle, cand, ctx.now_ts)
        if gen.get("skip"):
            skipped.append({"reply_id": rid, "reason": str(gen.get("skip_reason") or "llm_skip")})
            continue

        reply_body = str(gen.get("reply_text") or "").strip()
        if not reply_body:
            skipped.append({"reply_id": rid, "reason": "empty_generated_reply"})
            continue

        if sent:
            lo_d, hi_d = cfg["inter_reply_delay_range"]
            ctx.sleep_fn(random.randint(int(lo_d), int(hi_d)))

        new_id = reply_on_x_page(
            ctx.page,
            target_post_url=cand.get("reply_url") or rid,
            content=reply_body,
            like=False,
            account_name=acc_name,
            breaker=ctx.breaker,
        )
        ctx.state_store.record_action(acc_name, "reply", now_ts=ctx.now_ts)
        ctx.state_store.record_reply_for_post(acc_name, root_pid or rid, rid, now_ts=ctx.now_ts)
        sent.append({"target_reply_id": rid, "sent_reply_id": new_id, "post_id": root_pid, "text": reply_body})

    if ctx.session is not None:
        try:
            ctx.session.goto_home(wait_ms=900)
        except Exception:
            pass

    return {"status": "done", "result": {"sent_count": len(sent), "sent": sent, "skipped": skipped}}


def _handle_follow(ctx: DaemonContext, item: Dict[str, Any]) -> Dict[str, Any]:
    acc_name = ctx.account["name"]
    cfg = ctx.cfg
    payload = item.get("payload") or {}
    day_cnt = ctx.state_store.count_recent_actions(acc_name, "follow", 24 * 3600, now_ts=ctx.now_ts)
    if day_cnt >= cfg["max_follows_per_day"]:
        return {"status": "skipped", "error": f"follow_daily_cap ({day_cnt}/{cfg['max_follows_per_day']})"}

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


register_action_handler("post", _handle_post)
register_action_handler("reply", _handle_reply)
register_action_handler("sync_and_reply", _handle_sync_and_reply)
register_action_handler("follow", _handle_follow)
register_action_handler("pin", _handle_pin)


class XOpsDaemon:
    """Persistent single-Chrome orchestrator executing queued X actions under unified safety controls."""

    def __init__(
        self,
        account: Optional[Dict[str, Any]] = None,
        *,
        db: Any = None,
        session: Optional[XBrowserSession] = None,
        state_store: Optional[XAccountStateStore] = None,
        breaker: Optional[XAccountCircuitBreaker] = None,
        sentinel_fetch_fn: Callable[[str], Optional[Dict[str, Any]]] = fetch_x_tweet_zero_auth,
        reply_generator_fn: Optional[Callable[[str, Dict[str, Any], int], Dict[str, Any]]] = None,
        cfg: Optional[Dict[str, Any]] = None,
        stop_event: Optional[threading.Event] = None,
    ) -> None:
        self.account = dict(account or DEFAULT_ACCOUNT)
        self._db = db
        self.state_store = state_store or XAccountStateStore(db)
        self.breaker = breaker or XAccountCircuitBreaker(db, default_freeze_seconds=FREEZE_ON_CHALLENGE_SECONDS)
        self.sentinel_fetch_fn = sentinel_fetch_fn
        self.reply_generator_fn = reply_generator_fn
        self.stop_event = stop_event
        self.cfg = {
            "max_posts_per_day": MAX_POSTS_PER_DAY,
            "min_post_interval_seconds": MIN_POST_INTERVAL_SECONDS,
            "max_replies_per_day": MAX_REPLIES_PER_DAY,
            "max_replies_per_hour": MAX_REPLIES_PER_HOUR,
            "max_replies_per_post": MAX_REPLIES_PER_POST,
            "reply_parent_max_age_hours": REPLY_PARENT_MAX_AGE_HOURS,
            "inter_reply_delay_range": INTER_REPLY_DELAY_RANGE,
            "max_follows_per_day": MAX_FOLLOWS_PER_DAY,
            "max_pins_per_day": MAX_PINS_PER_DAY,
            "sentinel_interval_seconds": SENTINEL_INTERVAL_SECONDS,
            "tracked_post_window_hours": TRACKED_POST_WINDOW_HOURS,
            "session_refresh_hours": SESSION_REFRESH_HOURS,
            "poll_loop_seconds": POLL_LOOP_SECONDS,
            **(cfg or {}),
        }
        self.session = session or XBrowserSession(self.account["user_data_dir"], use_proxy=USE_PROXY)
        self._session_started_ts: int = 0
        self._last_sentinel_ts: int = 0

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

    def run_once(
        self,
        *,
        now_ts: Optional[int] = None,
        check_sentinel: bool = True,
    ) -> Dict[str, Any]:
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

        # 3. Zero-auth comment sentinel
        if check_sentinel and (now - self._last_sentinel_ts) >= int(self.cfg["sentinel_interval_seconds"]):
            self._last_sentinel_ts = now
            check_comment_sentinel(
                acc_name,
                db=self._db,
                state_store=self.state_store,
                fetch_fn=self.sentinel_fetch_fn,
                window_hours=self.cfg["tracked_post_window_hours"],
                max_replies_per_post=self.cfg["max_replies_per_post"],
                now_ts=now,
            )

        # 4. Fetch due actions from x_ops_queue
        q_col = self._queue_col()
        due_items = list(
            q_col.find(
                {"account_name": acc_name, "status": "pending", "scheduled_at_ts": {"$lte": now}}
            ).sort([("scheduled_at_ts", 1), ("created_at_ts", 1)])
        )

        # Keep single headed Chrome warm while daemon is active
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
        for item in due_items:
            if self.stop_event is not None and self.stop_event.is_set():
                break
            item_id = str(item.get("_id"))
            action_type = str(item.get("action_type") or "").strip()
            handler = _ACTION_HANDLERS.get(action_type)
            if handler is None:
                self._update_queue_item(item_id, status="failed", now_ts=now, error=f"unknown_action_type: {action_type}")
                failed += 1
                continue

            ctx = DaemonContext(
                account=self.account,
                page=page,
                session=self.session,
                db=self._db,
                state_store=self.state_store,
                breaker=self.breaker,
                cfg=self.cfg,
                now_ts=now,
                reply_generator_fn=self.reply_generator_fn,
            )
            try:
                outcome = handler(ctx, item)
                st = str(outcome.get("status") or "done")
                self._update_queue_item(
                    item_id,
                    status=st,
                    now_ts=now,
                    result=outcome.get("result"),
                    error=str(outcome.get("error") or ""),
                )
                if st == "done":
                    executed += 1
                elif st == "skipped":
                    skipped += 1
                else:
                    failed += 1
            except ChallengeRequiredError as ch_err:
                frozen_now, _, _ = self.breaker.is_frozen(acc_name, now_ts=now)
                if not frozen_now:
                    self.breaker.trip(acc_name, reason=str(ch_err), url=str(getattr(page, "url", "") or ""), now_ts=now)
                self._update_queue_item(item_id, status="failed", now_ts=now, error=str(ch_err))
                self._close_session_if_open()
                return {"status": "frozen_challenge", "error": str(ch_err), "executed": executed, "failed": failed + 1}
            except Exception as exc:
                self._update_queue_item(item_id, status="failed", now_ts=now, error=str(exc))
                failed += 1
                logger.exception(f"❌ [XOpsDaemon | {acc_name}] 执行任务 {item_id}({action_type}) 异常: {exc}")

        return {"status": "ok", "due": len(due_items), "executed": executed, "skipped": skipped, "failed": failed}

    def run_daemon(self, max_rounds: int = 0) -> int:
        logger.info(
            f"🚀 [XOpsDaemon 启动] 账号={self.account['name']}(@{self.account.get('screen_name')}) "
            f"| 轮询={self.cfg['poll_loop_seconds']}s"
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
        tracked = (self.state_store.get_state(acc_name) or {}).get("tracked_posts") or {}
        return (
            f"=== XOpsDaemon Status ({acc_name} / @{self.account.get('screen_name')}) ===\n"
            f"Session Dir : {self.account['user_data_dir']}\n"
            f"Circuit     : {'FROZEN ' + str(rem) + 's (' + reason + ')' if frozen else 'OK (not frozen)'}\n"
            f"Quotas      : post_24h={p_day}/{self.cfg['max_posts_per_day']} | reply_1h={r_hr}/{self.cfg['max_replies_per_hour']} "
            f"| reply_24h={r_day}/{self.cfg['max_replies_per_day']} | follow_24h={f_day}/{self.cfg['max_follows_per_day']} "
            f"| pin_24h={pin_day}/{self.cfg['max_pins_per_day']}\n"
            f"Queue/Track : pending={pending_cnt} | tracked_posts={len(tracked)}"
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
    # 默认直接运行按顶部 RUN_MODE 执行（默认 "status" 为纯只读状态检查，不会开浏览器）。
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
    # 示例 3：往队列塞一条主帖（纯文字或带图）并立即执行一次
    # 提示：日常后台跑 RUN_MODE = "daemon" 时，外部模块只需调用 enqueue_x_action(...) 入队即可
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
    #     print(daemon.run_once(check_sentinel=False))
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
    #     print(daemon.run_once(check_sentinel=False))
    # finally:
    #     daemon._close_session_if_open()

    # ==========================================================================
    # 示例 5：免登评论哨兵巡检（无风险）+ 站内拉取未读通知并 AI 自动回复
    # ==========================================================================
    # daemon = XOpsDaemon()
    # sentinel_res = check_comment_sentinel(daemon.account["name"])
    # print("免登哨兵巡检结果:", sentinel_res)
    # # 若想跳过哨兵直接去站内拉一次通知并尝试 AI 回复，取消下面注释即可：
    # # qid = enqueue_x_action(daemon.account["name"], "sync_and_reply", {"scroll_rounds": 1})
    # # print("已入队自动拉评回复任务 ID:", qid)
    # try:
    #     print(daemon.run_once(check_sentinel=False))
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
    #     print(daemon.run_once(check_sentinel=False))
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
    #     print(daemon.run_once(check_sentinel=False))
    # finally:
    #     daemon._close_session_if_open()
