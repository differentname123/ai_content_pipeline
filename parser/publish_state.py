# -*- coding: utf-8 -*-
"""
发帖账号状态与退避冷却中心 (`parser/publish_state.py`)

统一维护 `logs/runtime/account_publish_state.json`：
1. 与既有状态字段 100% 向后兼容（`last_publish_time` / `viral_last_publish_time` / `topic_publish_history` / `last_error_*`）；
2. 引入线程安全的 `RLock` 与原子落盘（`save_json`）；
3. 对登录失效（6h）、HTTP 405 / WAF 人机验证（60m）、普通临时异常（10m）自动分级退避，
   使多账号发布前可零成本预检，在全部账号不可用时跳过 DB 查询、指标同步与总编大模型调用。
"""
import os
import threading
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

from common.common_utils import (
    PLATFORM_ACCOUNTS,
    PLATFORM_OKX,
    read_json,
    save_json,
)
from parser.config import (
    ACCOUNT_COOLDOWN_SECONDS,
    ACCOUNT_TOPIC_COOLDOWN_SECONDS,
    AUTH_BACKOFF_SECONDS,
    OKX_ACCOUNT_COOLDOWN_SECONDS,
    STATE_FILE,
    TRANSIENT_BACKOFF_SECONDS,
    WAF_BACKOFF_SECONDS,
)

_WAF_KEYWORDS = (
    "405",
    "waf",
    "人机验证",
    "确认您是人类",
    "challengerequired",
    "challenge_required",
)
_AUTH_KEYWORDS = (
    "登录入口",
    "登录页",
    "login 按钮",
    "会话已超时",
    "执行安全验证",
    "cookie",
    "会话目录不存在",
    "缺少用户环境",
    "not_logged_in",
    "auth_expired",
    "code=805",
    "自动登出",
)


def get_platform_cooldown_seconds(platform: str) -> int:
    """获取平台对应的普通单账号发帖间隔冷却秒数。"""
    if platform == PLATFORM_OKX:
        return OKX_ACCOUNT_COOLDOWN_SECONDS
    return ACCOUNT_COOLDOWN_SECONDS


def classify_publish_error(error_msg: str) -> Tuple[str, int]:
    """
    将平台发帖失败原因分级并返回 `(category, backoff_seconds)`：
    - WAF / HTTP 405 / 人机验证 -> `("waf_challenge", 3600)` (60m)
    - 登录态 / Cookie / 会话目录失效 -> `("auth_expired", 21600)` (6h)
    - 其余临时异常 -> `("transient", 600)` (10m)
    """
    text = str(error_msg or "").lower()
    if any(kw in text for kw in _WAF_KEYWORDS):
        return "waf_challenge", WAF_BACKOFF_SECONDS
    if any(kw in text for kw in _AUTH_KEYWORDS):
        return "auth_expired", AUTH_BACKOFF_SECONDS
    return "transient", TRANSIENT_BACKOFF_SECONDS


def _state_prefix(is_viral: bool) -> str:
    return "viral_" if is_viral else ""


def ensure_account_state(
    state: Dict[str, Any],
    platform: str,
    account: str,
    normalize_history: bool = True,
) -> Dict[str, Any]:
    """确保 `state[platform][account]` 具备完整默认结构并返回可变引用。"""
    platform_state = state.setdefault(platform, {})
    if not isinstance(platform_state, dict):
        platform_state = state[platform] = {}
    account_state = platform_state.setdefault(account, {})
    if not isinstance(account_state, dict):
        account_state = platform_state[account] = {}

    defaults = {
        "total_success": 0,
        "last_publish_time": 0,
        "last_error_msg": "",
        "last_error_time": 0,
        "backoff_until": 0.0,
        "backoff_reason": "",
        "consecutive_failures": 0,
        "topic_publish_history": {},
        "viral_total_success": 0,
        "viral_last_publish_time": 0,
        "viral_last_error_msg": "",
        "viral_last_error_time": 0,
        "viral_backoff_until": 0.0,
        "viral_backoff_reason": "",
        "viral_consecutive_failures": 0,
    }
    for key, value in defaults.items():
        account_state.setdefault(key, dict(value) if isinstance(value, dict) else value)
    if normalize_history and not isinstance(account_state.get("topic_publish_history"), dict):
        account_state["topic_publish_history"] = {}
    return account_state


class AccountPublishStateStore:
    """线程安全的发帖账号状态、主题冷却与错误分级退避管理器。"""

    def __init__(self, state_file: str = STATE_FILE):
        self.state_file = str(state_file)
        self._lock = threading.RLock()

    def _read_raw(self) -> Dict[str, Any]:
        if not os.path.exists(self.state_file):
            return {}
        data = read_json(self.state_file) or {}
        return data if isinstance(data, dict) else {}

    def load_state(self, accounts_by_platform: Optional[Dict[str, Sequence[str]]] = None) -> Dict[str, Any]:
        """线程安全地读取并补齐默认字段（只读快照，不主动覆盖落盘）。"""
        mapping = PLATFORM_ACCOUNTS if accounts_by_platform is None else accounts_by_platform
        with self._lock:
            state = self._read_raw()
            for platform, accounts in mapping.items():
                if platform not in state or not isinstance(state[platform], dict):
                    state[platform] = {}
                for account in accounts:
                    ensure_account_state(state, platform, account, normalize_history=True)
            return state

    def get_account_state(self, platform: str, account: str) -> Dict[str, Any]:
        with self._lock:
            state = self._read_raw()
            return dict(ensure_account_state(state, platform, account, normalize_history=True))

    def can_publish(
        self,
        platform: str,
        account: str,
        *,
        is_viral: bool = False,
        now: Optional[float] = None,
        cooldown_seconds: Optional[int] = None,
    ) -> Tuple[bool, str]:
        """
        检查账号当前是否允许发布指定类型文章：
        同时校验普通间隔冷却与失败分级退避 (`backoff_until` / `viral_backoff_until`)。
        返回 `(eligible, reason_if_blocked)`。
        """
        now_ts = time.time() if now is None else float(now)
        prefix = _state_prefix(is_viral)
        cooldown = get_platform_cooldown_seconds(platform) if cooldown_seconds is None else int(cooldown_seconds)
        account_state = self.get_account_state(platform, account)

        backoff_until = float(account_state.get(f"{prefix}backoff_until") or 0.0)
        if backoff_until > now_ts:
            rem = int(backoff_until - now_ts)
            reason = str(account_state.get(f"{prefix}backoff_reason") or "退避冷却")
            return False, f"故障退避中[{reason}] (剩余{rem}秒)"

        last_pub = float(account_state.get(f"{prefix}last_publish_time") or 0.0)
        remaining = cooldown - (now_ts - last_pub)
        if remaining > 0:
            return False, f"发布冷却中 (剩余{int(remaining)}秒)"

        return True, ""

    def get_eligible_accounts(
        self,
        platform: str,
        accounts: Sequence[str],
        *,
        is_viral: bool = False,
        now: Optional[float] = None,
        cooldown_seconds: Optional[int] = None,
    ) -> Tuple[List[str], Dict[str, str]]:
        """返回 `(eligible_accounts, blocked_reasons_by_account)`，用于发布前零成本预检。"""
        now_ts = time.time() if now is None else float(now)
        eligible: List[str] = []
        blocked: Dict[str, str] = {}
        with self._lock:
            for account in accounts:
                ok, reason = self.can_publish(
                    platform,
                    account,
                    is_viral=is_viral,
                    now=now_ts,
                    cooldown_seconds=cooldown_seconds,
                )
                if ok:
                    eligible.append(account)
                else:
                    blocked[account] = reason
        return eligible, blocked

    def is_topic_in_cooldown(
        self,
        platform: str,
        account: str,
        topic: Any,
        *,
        now: Optional[float] = None,
        account_state: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """检查单币主题在账号维度的 2 小时冷却窗口。支持单个币种字符串或币种序列。"""
        now_ts = time.time() if now is None else float(now)
        state_obj = account_state if account_state is not None else self.get_account_state(platform, account)
        history = state_obj.get("topic_publish_history") or {}
        if isinstance(topic, (list, tuple, set)):
            coins = [str(c).strip() for c in topic if str(c or "").strip()]
        else:
            c_str = str(topic or "").strip()
            coins = [c_str] if c_str else []
        return any((now_ts - float(history.get(c) or 0)) < ACCOUNT_TOPIC_COOLDOWN_SECONDS for c in coins)

    def record_success(
        self,
        platform: str,
        account: str,
        *,
        is_viral: bool = False,
        topic: Optional[str] = None,
        topics: Optional[Sequence[str]] = None,
        now: Optional[float] = None,
    ) -> Dict[str, Any]:
        """记录成功发布并清除对应类型的故障退避状态，同时更新币种冷却历史。"""
        now_ts = time.time() if now is None else float(now)
        prefix = _state_prefix(is_viral)
        with self._lock:
            state = self._read_raw()
            account_state = ensure_account_state(state, platform, account, normalize_history=True)
            account_state[f"{prefix}total_success"] = int(account_state.get(f"{prefix}total_success") or 0) + 1
            account_state[f"{prefix}last_publish_time"] = now_ts
            account_state[f"{prefix}backoff_until"] = 0.0
            account_state[f"{prefix}backoff_reason"] = ""
            account_state[f"{prefix}consecutive_failures"] = 0

            if not is_viral:
                coin_list: List[str] = []
                if topics:
                    coin_list.extend(str(c).strip() for c in topics if str(c or "").strip())
                elif topic:
                    coin_list.append(str(topic).strip())
                history = account_state.setdefault("topic_publish_history", {})
                for coin in dict.fromkeys(coin_list):
                    if coin:
                        history[coin] = now_ts

            save_json(self.state_file, state)
            return dict(account_state)

    def record_failure(
        self,
        platform: str,
        account: str,
        error_msg: str,
        *,
        is_viral: bool = False,
        now: Optional[float] = None,
        wrapped_detail: Optional[str] = None,
    ) -> Tuple[str, int, str]:
        """
        记录发布失败并应用分级退避策略：
        - HTTP 405 / WAF / 人机验证 -> 60m
        - 登录 / Cookie 失效 -> 6h
        - 常规失败 -> 10m
        返回 `(category, backoff_seconds, error_detail)`。
        """
        now_ts = time.time() if now is None else float(now)
        prefix = _state_prefix(is_viral)
        raw_err = str(error_msg or "")
        category, backoff_sec = classify_publish_error(raw_err)
        error_detail = wrapped_detail if wrapped_detail is not None else raw_err

        with self._lock:
            state = self._read_raw()
            account_state = ensure_account_state(state, platform, account, normalize_history=True)
            account_state[f"{prefix}last_error_msg"] = error_detail
            account_state[f"{prefix}last_error_time"] = now_ts
            account_state[f"{prefix}backoff_until"] = now_ts + backoff_sec
            account_state[f"{prefix}backoff_reason"] = category
            account_state[f"{prefix}consecutive_failures"] = (
                int(account_state.get(f"{prefix}consecutive_failures") or 0) + 1
            )
            save_json(self.state_file, state)
        return category, backoff_sec, error_detail


default_state_store = AccountPublishStateStore()
