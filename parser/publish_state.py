# -*- coding: utf-8 -*-
"""
发帖账号状态与冷却中心 (`parser/publish_state.py`)

统一维护 `logs/runtime/account_publish_state.json`：
1. 与既有状态字段 100% 向后兼容（`last_publish_time` / `viral_last_publish_time` / `topic_publish_history` / `last_error_*`）；
2. 引入线程安全的 `RLock`、原子落盘（`save_json`）与进程内同账号并发占用守卫（`try_claim_account` / `release_account`）；
3. 实行账号级全局发帖间隔冷却（合并分析帖与流量帖，避免双线程短时间连发被限流）与同币主题冷却；
   仅对 HTTP 405/WAF 人机验证与登录态失效触发单账号独立、可自动恢复的基础设施退避冷却，普通超时与业务错误不设阻塞退避。
"""
import os
import threading
import time
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from common.common_utils import (
    PLATFORM_ACCOUNTS,
    PLATFORM_OKX,
    read_json,
    save_json,
)
from parser.config import (
    ACCOUNT_COOLDOWN_SECONDS,
    ACCOUNT_TOPIC_COOLDOWN_SECONDS,
    AUTH_EXPIRED_BACKOFF_SECONDS,
    OKX_ACCOUNT_COOLDOWN_SECONDS,
    STATE_FILE,
    WAF_BACKOFF_SCHEDULE_SECONDS,
)


_WAF_CHALLENGE_MARKERS = (
    "[waf/人机验证]",
    "http 405",
    "人机验证",
    "人机安全验证",
    "challengerequirederror",
)
_AUTH_EXPIRED_MARKERS = (
    "登录入口或会话已超时",
    "cookie 可能已过期失效",
    "请先重新登录",
    "登录会话已超时",
    "session has expired",
)


def classify_infra_backoff_error(error_msg: Any) -> Optional[str]:
    """识别须触发单账号临时退避的基础设施拦截（WAF 人机验证 / 登录态失效），普通异常返回 None。"""
    text = str(error_msg or "").strip()
    if not text:
        return None
    low = text.lower()
    if any(marker in low for marker in _WAF_CHALLENGE_MARKERS):
        return "waf_challenge"
    if any(marker in low for marker in _AUTH_EXPIRED_MARKERS):
        return "auth_expired"
    return None


def get_platform_cooldown_seconds(platform: str) -> int:
    """获取平台对应的普通单账号发帖间隔冷却秒数。"""
    if platform == PLATFORM_OKX:
        return OKX_ACCOUNT_COOLDOWN_SECONDS
    return ACCOUNT_COOLDOWN_SECONDS


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
        "account_last_publish_time": 0,
        "account_backoff_until": 0,
        "account_backoff_reason": "",
        "account_infra_failures": 0,
        "last_publish_type": "",
        "last_error_msg": "",
        "last_error_time": 0,
        "consecutive_failures": 0,
        "topic_publish_history": {},
        "viral_total_success": 0,
        "viral_last_publish_time": 0,
        "viral_last_error_msg": "",
        "viral_last_error_time": 0,
        "viral_consecutive_failures": 0,
    }
    for key, value in defaults.items():
        account_state.setdefault(key, dict(value) if isinstance(value, dict) else value)
    if normalize_history and not isinstance(account_state.get("topic_publish_history"), dict):
        account_state["topic_publish_history"] = {}
    return account_state


def _get_account_last_publish_time(account_state: Dict[str, Any]) -> float:
    """取账号维度（合并分析帖、流量帖及历史字段）最近一次成功发布时间戳。"""
    return max(
        float(account_state.get("account_last_publish_time") or 0.0),
        float(account_state.get("last_publish_time") or 0.0),
        float(account_state.get("viral_last_publish_time") or 0.0),
    )


class AccountPublishStateStore:
    """线程安全的发帖账号状态、账号级全局冷却与主题冷却管理器。"""

    def __init__(self, state_file: str = STATE_FILE):
        self.state_file = str(state_file)
        self._lock = threading.RLock()
        self._in_flight: Set[Tuple[str, str]] = set()

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
        检查账号当前是否允许发布（账号级全局冷却，合并分析帖与流量帖）：
        1. 检查进程内是否已有同一 `(platform, account)` 任务正在并发执行；
        2. 校验距该账号任意类型上一次成功发布的全局间隔（忽略历史失败退避字段）。
        返回 `(eligible, reason_if_blocked)`。
        """
        del is_viral  # 账号冷却已升级为账号全局合并冷却，保留参数确保调用签名 100% 兼容
        now_ts = time.time() if now is None else float(now)
        cooldown = get_platform_cooldown_seconds(platform) if cooldown_seconds is None else int(cooldown_seconds)
        with self._lock:
            if (platform, account) in self._in_flight:
                return False, "同账号另一发布任务正在执行中"
            account_state = self.get_account_state(platform, account)

        last_pub = _get_account_last_publish_time(account_state)
        remaining = cooldown - (now_ts - last_pub)
        if remaining > 0:
            return False, f"账号全局发布冷却中 (剩余{int(remaining)}秒)"

        backoff_until = float(account_state.get("account_backoff_until") or 0.0)
        backoff_remaining = backoff_until - now_ts
        if backoff_remaining > 0:
            backoff_reason = str(account_state.get("account_backoff_reason") or "infra_backoff")
            return False, f"账号风控/会话退避冷却中 [{backoff_reason}] (剩余{int(backoff_remaining)}秒)"

        return True, ""

    def try_claim_account(
        self,
        platform: str,
        account: str,
        *,
        is_viral: bool = False,
        now: Optional[float] = None,
        cooldown_seconds: Optional[int] = None,
    ) -> Tuple[bool, str]:
        """在线程锁内原子复查全局冷却并占用 `(platform, account)`，防止分析/流量双线程并发连发撞车。"""
        with self._lock:
            ok, reason = self.can_publish(
                platform,
                account,
                is_viral=is_viral,
                now=now,
                cooldown_seconds=cooldown_seconds,
            )
            if not ok:
                return False, reason
            self._in_flight.add((platform, account))
            return True, ""

    def release_account(self, platform: str, account: str) -> None:
        """释放 `(platform, account)` 的进程内并发发布占用。"""
        with self._lock:
            self._in_flight.discard((platform, account))

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
        """记录成功发布、清零连续失败计数并更新币种冷却历史（同时清理历史遗留退避字段）。"""
        now_ts = time.time() if now is None else float(now)
        prefix = _state_prefix(is_viral)
        with self._lock:
            state = self._read_raw()
            account_state = ensure_account_state(state, platform, account, normalize_history=True)
            account_state[f"{prefix}total_success"] = int(account_state.get(f"{prefix}total_success") or 0) + 1
            account_state[f"{prefix}last_publish_time"] = now_ts
            account_state["account_last_publish_time"] = now_ts
            account_state["last_publish_type"] = "viral" if is_viral else "analysis"
            account_state["account_backoff_until"] = 0
            account_state["account_backoff_reason"] = ""
            account_state["account_infra_failures"] = 0
            account_state.pop(f"{prefix}backoff_until", None)
            account_state.pop(f"{prefix}backoff_reason", None)
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
    ) -> str:
        """
        记录发布失败明细与连续失败计数；仅当属于 WAF/人机验证或登录态失效时设置账号级可恢复退避窗口，
        并清理旧遗留按类型分开的退避字段。返回 `error_detail`。
        """
        now_ts = time.time() if now is None else float(now)
        prefix = _state_prefix(is_viral)
        raw_err = str(error_msg or "")
        error_detail = wrapped_detail if wrapped_detail is not None else raw_err
        infra_kind = classify_infra_backoff_error(raw_err or error_detail)

        with self._lock:
            state = self._read_raw()
            account_state = ensure_account_state(state, platform, account, normalize_history=True)
            account_state[f"{prefix}last_error_msg"] = error_detail
            account_state[f"{prefix}last_error_time"] = now_ts
            account_state.pop(f"{prefix}backoff_until", None)
            account_state.pop(f"{prefix}backoff_reason", None)
            account_state[f"{prefix}consecutive_failures"] = (
                int(account_state.get(f"{prefix}consecutive_failures") or 0) + 1
            )
            if infra_kind == "waf_challenge":
                infra_fails = int(account_state.get("account_infra_failures") or 0) + 1
                schedule_idx = min(infra_fails - 1, len(WAF_BACKOFF_SCHEDULE_SECONDS) - 1)
                delay_sec = int(WAF_BACKOFF_SCHEDULE_SECONDS[max(0, schedule_idx)])
                account_state["account_infra_failures"] = infra_fails
                account_state["account_backoff_until"] = now_ts + delay_sec
                account_state["account_backoff_reason"] = "waf_challenge"
            elif infra_kind == "auth_expired":
                infra_fails = int(account_state.get("account_infra_failures") or 0) + 1
                account_state["account_infra_failures"] = infra_fails
                account_state["account_backoff_until"] = now_ts + int(AUTH_EXPIRED_BACKOFF_SECONDS)
                account_state["account_backoff_reason"] = "auth_expired"
            save_json(self.state_file, state)
        return error_detail


default_state_store = AccountPublishStateStore()
