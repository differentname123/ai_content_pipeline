# -*- coding: utf-8 -*-
"""
Clean X (Twitter) Browser Engine (`platforms/x_browser.py`)

Built on Patchright + local headed Google Chrome (`channel="chrome"`, `headless=False`, `no_viewport=True`):
- Zero `--headless=new`, zero `Emulation.setUserAgentOverride`, zero fake Client Hints
- Zero `Runtime.enable` / `Console.enable` CDP leaks (eliminated by Patchright)
- Zero JS watchdog injection (`window.__bnGuardTimer`), zero cache deletion (`Service Worker` preserved)
- Zero `isTrusted: false` synthetic JS clicks or clipboard hijacking
- Global 0-second persistent circuit breaker (`XAccountCircuitBreaker` on `x_ops_state`)
"""
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import random
import time
from typing import Any, Callable, Dict, Optional, Sequence, Tuple

from patchright.sync_api import (
    TimeoutError as PatchrightTimeoutError,
    expect,
    sync_playwright,
)

from common.common_utils import (
    DEFAULT_SESSION_ROOT,
    get_proxy_url,
    resolve_cross_platform_path,
    setup_logger,
)
from common.mongo_db.mongo_base import gen_db_object
from platforms.playwright_base import (
    BusinessErrorException,
    ChallengeRequiredError,
    PageCrashedException,
    USER_DATA_LOCK_TIMEOUT_SEC,
    UserDataDirLockTimeoutError,
    _BaseApiWatcher,
    _alert_failure_scene,
    _dismiss_overlays,
    _force_caret_to_tail,
    _forensics,
    _hit_test,
    _interact_fallback_locators,
    _normalize_text_list,
    _parse_api_json,
    _pick_trusted_send_button,
    _user_data_dir_lock,
)

logger = setup_logger(app_name="x_browser")

PlaywrightTimeoutError = PatchrightTimeoutError

TZ_CN = timezone(timedelta(hours=8))
X_HOME_URL = "https://x.com/home"
DEFAULT_X_STATE_COLLECTION = "x_ops_state"

# 【保留过滤原因说明】触发安全验证 / 人机挑战 / 封号后立即冻结 48h，避免无意义撞墙把临时限制升级成深度降权
DEFAULT_FREEZE_SECONDS = 48 * 3600

_CHALLENGE_URL_HINTS = (
    "/account/access",
    "arkoselabs",
    "challenges.cloudflare.com",
)
_CHALLENGE_SELECTORS = (
    'iframe[src*="arkoselabs"]',
    'iframe[id*="arkose"]',
    'form[action*="/account/access"]',
    'iframe[src*="challenges.cloudflare.com"]',
)
_LOGIN_SELECTORS = (
    'input[autocomplete="username"]',
    'a[data-testid="loginButton"]',
)
_SUSPENDED_HINTS = (
    "account suspended",
    "your account is suspended",
    "账号已被冻结",
    "您的账号已被冻结",
)
_PUNCTUATION_PAUSE_CHARS = frozenset("，。！？,.!?;；:：")

__all__ = [
    "BusinessErrorException",
    "ChallengeRequiredError",
    "DEFAULT_FREEZE_SECONDS",
    "DEFAULT_X_STATE_COLLECTION",
    "PageCrashedException",
    "PatchrightTimeoutError",
    "PlaywrightTimeoutError",
    "USER_DATA_LOCK_TIMEOUT_SEC",
    "UserDataDirLockTimeoutError",
    "XAccountCircuitBreaker",
    "XBrowserSession",
    "X_HOME_URL",
    "_BaseApiWatcher",
    "_alert_failure_scene",
    "_dismiss_overlays",
    "_force_caret_to_tail",
    "_forensics",
    "_hit_test",
    "_interact_fallback_locators",
    "_normalize_text_list",
    "_parse_api_json",
    "_pick_trusted_send_button",
    "_user_data_dir_lock",
    "assert_x_page_healthy",
    "check_for_crash",
    "expect",
    "human_click",
    "human_pause",
    "human_scroll",
    "human_type",
    "inspect_x_page_health",
    "sync_playwright",
]


def _now_iso(now_ts: Optional[int] = None) -> str:
    ts = int(time.time()) if now_ts is None else int(now_ts)
    return datetime.fromtimestamp(ts, tz=TZ_CN).isoformat(timespec="seconds")


def check_for_crash(page: Any) -> None:
    """Detect browser tab crash (`重新加载` / `Reload` button) using Patchright TimeoutError."""
    try:
        page.get_by_role("button", name="重新加载").first.wait_for(state="visible", timeout=500)
    except PatchrightTimeoutError:
        return
    raise PageCrashedException("页面 DOM 渲染崩溃，检测到【重新加载】按钮")


def _resolve_collection(db: Any, collection_name: str) -> Any:
    if db is None:
        return gen_db_object().get_collection(collection_name)
    get_col = getattr(db, "get_collection", None)
    if callable(get_col):
        return get_col(collection_name)
    if hasattr(db, "db") and db.db is not None:
        return db.db[collection_name]
    return db[collection_name]


class XAccountCircuitBreaker:
    """Per-account 0-second persistent freeze gate stored in MongoDB `x_ops_state`."""

    def __init__(
        self,
        db: Any = None,
        *,
        collection_name: str = DEFAULT_X_STATE_COLLECTION,
        default_freeze_seconds: int = DEFAULT_FREEZE_SECONDS,
    ) -> None:
        self._db = db
        self.collection_name = collection_name
        self.default_freeze_seconds = max(60, int(default_freeze_seconds))

    def _col(self) -> Any:
        return _resolve_collection(self._db, self.collection_name)

    @staticmethod
    def _doc_id(account_name: str) -> str:
        clean = str(account_name or "").strip() or "default"
        return f"account:{clean}"

    def is_frozen(
        self,
        account_name: str,
        *,
        now_ts: Optional[int] = None,
    ) -> Tuple[bool, int, str]:
        now = int(time.time()) if now_ts is None else int(now_ts)
        doc = self._col().find_one({"_id": self._doc_id(account_name)}) or {}
        until_ts = int(doc.get("frozen_until_ts") or 0)
        if until_ts > now:
            reason = str(doc.get("freeze_reason") or "account_frozen")
            return True, until_ts - now, reason
        return False, 0, ""

    def trip(
        self,
        account_name: str,
        reason: str,
        *,
        url: str = "",
        freeze_seconds: Optional[int] = None,
        now_ts: Optional[int] = None,
    ) -> int:
        now = int(time.time()) if now_ts is None else int(now_ts)
        duration = self.default_freeze_seconds if freeze_seconds is None else max(60, int(freeze_seconds))
        until_ts = now + duration
        until_iso = _now_iso(until_ts)
        clean_reason = str(reason or "risk_challenge_triggered").strip()
        clean_url = str(url or "").strip()

        self._col().update_one(
            {"_id": self._doc_id(account_name)},
            {
                "$set": {
                    "account_name": str(account_name or "").strip(),
                    "frozen_until_ts": until_ts,
                    "frozen_until_iso": until_iso,
                    "freeze_reason": clean_reason,
                    "freeze_trigger_url": clean_url,
                    "updated_at_ts": now,
                    "updated_at_iso": _now_iso(now),
                }
            },
            upsert=True,
        )
        logger.error(
            "\n" + "!" * 88 + "\n"
            f"🚨 [全局 0 秒熔断] 账号 [{account_name}] 已立即冻结至 {until_iso} ({duration}s)\n"
            f"触发原因: {clean_reason}\n"
            f"触发页面: {clean_url or 'N/A'}\n"
            + "!" * 88
        )
        return until_ts

    def clear(self, account_name: str, *, now_ts: Optional[int] = None) -> None:
        now = int(time.time()) if now_ts is None else int(now_ts)
        self._col().update_one(
            {"_id": self._doc_id(account_name)},
            {
                "$set": {
                    "account_name": str(account_name or "").strip(),
                    "frozen_until_ts": 0,
                    "frozen_until_iso": "",
                    "freeze_reason": "",
                    "freeze_trigger_url": "",
                    "updated_at_ts": now,
                    "updated_at_iso": _now_iso(now),
                }
            },
            upsert=True,
        )
        logger.info(f"[熔断解除] 账号 [{account_name}] 冻结状态已手动清除。")


def inspect_x_page_health(page: Any) -> Tuple[str, str]:
    """
    Inspect current X page for security challenges, suspended banners, or logged-out redirects.
    Returns `(status, message)` where status is one of:
    `"ok"`, `"locked_challenge"`, `"not_logged_in"`, `"suspended"`.
    """
    raw_url = str(getattr(page, "url", "") or "")
    cur_url = raw_url.lower()

    for hint in _CHALLENGE_URL_HINTS:
        if hint in cur_url:
            return "locked_challenge", f"检测到 X 安全验证拦截页 ({raw_url})"

    if "/account/suspended" in cur_url:
        return "suspended", f"检测到 X 账号封禁提示页 ({raw_url})"

    if "/i/flow/login" in cur_url or cur_url.rstrip("/").endswith("/login"):
        return "not_logged_in", f"检测到页面跳转至 X 登录页 ({raw_url})"

    for sel in _CHALLENGE_SELECTORS:
        try:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible():
                return "locked_challenge", f"页面检测到安全验证组件 ({sel})"
        except Exception:
            continue

    for sel in _LOGIN_SELECTORS:
        try:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible():
                return "not_logged_in", f"页面检测到未登录组件 ({sel})"
        except Exception:
            continue

    for sel in ('div[data-testid="empty_state_header_text"]', '[role="alert"]', 'main[role="main"]'):
        try:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible():
                txt = loc.inner_text()
                if isinstance(txt, str):
                    low_txt = txt.lower()
                    for hint in _SUSPENDED_HINTS:
                        if hint in low_txt:
                            return "suspended", f"页面检测到账号冻结提示 ({hint})"
        except Exception:
            continue

    return "ok", ""


def assert_x_page_healthy(
    page: Any,
    *,
    account_name: Optional[str] = None,
    breaker: Optional[XAccountCircuitBreaker] = None,
) -> None:
    """Verify page is healthy; on block/challenge, trip circuit breaker immediately and raise."""
    status, msg = inspect_x_page_health(page)
    if status == "ok":
        return

    if breaker is not None and account_name:
        breaker.trip(
            account_name,
            reason=f"{status}: {msg}",
            url=str(getattr(page, "url", "") or ""),
        )

    if status in ("locked_challenge", "suspended"):
        raise ChallengeRequiredError(msg)
    raise BusinessErrorException(msg)


def human_pause(min_ms: int, max_ms: int, page: Any = None) -> None:
    """Randomized natural pause; pumps Playwright event loop when `page` exposes `wait_for_timeout`."""
    lo = max(0, int(min_ms))
    hi = max(lo, int(max_ms))
    ms = random.randint(lo, hi) if hi > lo else lo
    if ms <= 0:
        return
    wait_fn = getattr(page, "wait_for_timeout", None)
    if callable(wait_fn):
        try:
            wait_fn(ms)
            return
        except Exception:
            pass
    time.sleep(ms / 1000.0)


def _is_valid_box(box: Any) -> bool:
    if not isinstance(box, dict):
        return False
    try:
        return (
            isinstance(box.get("x"), (int, float))
            and isinstance(box.get("y"), (int, float))
            and float(box.get("width", 0)) > 2.0
            and float(box.get("height", 0)) > 2.0
        )
    except Exception:
        return False


def human_click(page: Any, locator: Any, timeout_ms: int = 8000) -> None:
    """
    Physical mouse click (`isTrusted === true` guaranteed):
    - Wait visible & scroll into view
    - Move mouse with multi-step trajectory to randomized interior offset
    - Pause on hover -> mouse down -> brief press hold -> mouse up
    Never uses JS `element.click()` or synthetic `dispatchEvent`.
    """
    locator.wait_for(state="visible", timeout=timeout_ms)
    try:
        locator.scroll_into_view_if_needed(timeout=min(timeout_ms, 5000))
    except Exception:
        pass
    human_pause(150, 350, page=page)

    box = None
    try:
        box = locator.bounding_box()
    except Exception:
        box = None

    mouse = getattr(page, "mouse", None)
    if _is_valid_box(box) and mouse is not None:
        tx = float(box["x"]) + float(box["width"]) * random.uniform(0.25, 0.75)
        ty = float(box["y"]) + float(box["height"]) * random.uniform(0.25, 0.75)
        mouse.move(tx, ty, steps=random.randint(8, 16))
        human_pause(70, 180, page=page)
        mouse.down()
        human_pause(45, 120, page=page)
        mouse.up()
        return

    locator.click(timeout=timeout_ms)


def human_type(
    page: Any,
    editor_locator: Any,
    text: str,
    *,
    dismiss_typeahead_fn: Optional[Callable[[Any], Any]] = None,
) -> None:
    """
    Variable-speed chunked keyboard typing with punctuation pauses and zero clipboard/insert_text hacks.
    """
    if not text:
        return
    normalized = str(text).replace("\r\n", "\n").replace("\r", "\n")
    lines = normalized.split("\n")

    for line_idx, line in enumerate(lines):
        pos = 0
        line_len = len(line)
        while pos < line_len:
            step = random.randint(1, 3)
            chunk = line[pos : pos + step]
            pos += len(chunk)
            delay_ms = random.randint(45, 135)
            try:
                editor_locator.press_sequentially(chunk, delay=delay_ms, timeout=60000)
            except AttributeError:
                editor_locator.type(chunk, delay=delay_ms, timeout=60000)

            if chunk and chunk[-1] in _PUNCTUATION_PAUSE_CHARS:
                human_pause(200, 520, page=page)
            elif pos < line_len and random.random() < 0.15:
                human_pause(90, 220, page=page)

        if callable(dismiss_typeahead_fn):
            dismiss_typeahead_fn(page)

        if line_idx < len(lines) - 1:
            if callable(dismiss_typeahead_fn):
                dismiss_typeahead_fn(page)
            try:
                editor_locator.press("Enter", timeout=3000)
            except Exception:
                page.keyboard.press("Enter")
            human_pause(80, 180, page=page)

    if callable(dismiss_typeahead_fn):
        dismiss_typeahead_fn(page)


def human_scroll(
    page: Any,
    rounds: int = 1,
    *,
    dy_range: Tuple[int, int] = (350, 750),
    pause_range_ms: Tuple[int, int] = (600, 1400),
) -> None:
    """Gentle wheel scrolling with human reading pauses."""
    dy_lo, dy_hi = int(dy_range[0]), int(dy_range[1])
    if dy_lo > dy_hi:
        dy_lo, dy_hi = dy_hi, dy_lo
    p_lo, p_hi = int(pause_range_ms[0]), int(pause_range_ms[1])
    for _ in range(max(0, int(rounds))):
        page.mouse.wheel(0, random.randint(dy_lo, dy_hi))
        human_pause(p_lo, p_hi, page=page)


class XBrowserSession:
    """
    Unified real headed Google Chrome session for X automation via Patchright.
    Supports both context-manager (`with XBrowserSession(...) as s:`) and persistent reuse (`start()` / `ensure_alive()`).
    """

    def __init__(
        self,
        user_data_dir: str,
        *,
        use_proxy: bool = True,
        proxy: Optional[str] = None,
        args: Optional[Sequence[str]] = None,
        lock_timeout: int = USER_DATA_LOCK_TIMEOUT_SEC,
        default_timeout_ms: int = 60000,
        navigation_timeout_ms: int = 60000,
        reuse_existing_page: bool = True,
    ) -> None:
        fallback_name = Path(str(user_data_dir).replace("\\", "/")).name or "x_myself"
        self.user_data_dir = resolve_cross_platform_path(
            user_data_dir,
            DEFAULT_SESSION_ROOT / fallback_name,
        )
        self.proxy = proxy if proxy is not None else (get_proxy_url() if use_proxy else None)
        self.extra_args = list(args or [])
        self.lock_timeout = int(lock_timeout)
        self.default_timeout_ms = int(default_timeout_ms)
        self.navigation_timeout_ms = int(navigation_timeout_ms)
        self.reuse_existing_page = bool(reuse_existing_page)

        self._lock_cm: Any = None
        self._pw_cm: Any = None
        self.context: Any = None
        self.page: Any = None

    def _build_launch_args(self) -> list:
        base = [
            "--disable-restore-session-state",
            "--no-default-browser-check",
            "--mute-audio",
        ]
        seen = set(base)
        for item in self.extra_args:
            s = str(item or "").strip()
            if not s or s.startswith("--headless") or s in seen:
                continue
            seen.add(s)
            base.append(s)
        return base

    def start(self) -> "XBrowserSession":
        if self.is_alive():
            return self
        self.close()

        os.makedirs(self.user_data_dir, exist_ok=True)
        self._lock_cm = _user_data_dir_lock(self.user_data_dir, timeout=self.lock_timeout)
        self._lock_cm.__enter__()
        try:
            self._pw_cm = sync_playwright()
            p = self._pw_cm.__enter__()
            launch_kwargs: Dict[str, Any] = {
                "user_data_dir": self.user_data_dir,
                "channel": "chrome",
                "headless": False,
                "no_viewport": True,
                "args": self._build_launch_args(),
            }
            if isinstance(self.proxy, str) and self.proxy.strip():
                launch_kwargs["proxy"] = {"server": self.proxy.strip()}
            elif isinstance(self.proxy, dict) and self.proxy.get("server"):
                launch_kwargs["proxy"] = self.proxy

            self.context = p.chromium.launch_persistent_context(**launch_kwargs)
            if self.default_timeout_ms:
                self.context.set_default_timeout(self.default_timeout_ms)
            if self.navigation_timeout_ms:
                self.context.set_default_navigation_timeout(self.navigation_timeout_ms)

            if self.reuse_existing_page and self.context.pages:
                self.page = self.context.pages[0]
            else:
                self.page = self.context.new_page()

            for old_page in list(self.context.pages):
                if old_page != self.page:
                    try:
                        old_page.close()
                    except Exception:
                        pass

            return self
        except BaseException:
            self.close()
            raise

    def is_alive(self) -> bool:
        if self.context is None or self.page is None:
            return False
        try:
            return not self.page.is_closed()
        except Exception:
            return False

    def ensure_alive(self) -> Any:
        if not self.is_alive():
            self.start()
        return self.page

    def goto_home(self, wait_ms: int = 1200) -> Any:
        page = self.ensure_alive()
        cur = str(getattr(page, "url", "") or "").lower().rstrip("/")
        if cur != "https://x.com/home":
            page.goto(X_HOME_URL, timeout=self.navigation_timeout_ms, wait_until="domcontentloaded")
        if wait_ms > 0:
            human_pause(max(300, wait_ms // 2), wait_ms, page=page)
        return page

    def close(self) -> None:
        try:
            if self.context is not None:
                try:
                    self.context.close()
                except Exception:
                    pass
                self.context = None
                self.page = None
        finally:
            if self._pw_cm is not None:
                try:
                    self._pw_cm.__exit__(None, None, None)
                except Exception:
                    pass
                self._pw_cm = None
            if self._lock_cm is not None:
                try:
                    self._lock_cm.__exit__(None, None, None)
                except Exception:
                    pass
                self._lock_cm = None

    def __enter__(self) -> "XBrowserSession":
        return self.start()

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        self.close()
        return False
