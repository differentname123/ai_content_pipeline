# -*- coding: utf-8 -*-
"""
统一后台守护进程循环引擎 (`common/daemon.py`)

提供可中断休眠、防指纹固定周期抖动（Jitter）、异常显式告警保护与动态冷却休眠支持。
"""
import logging
import random
import threading
import time
from typing import Any, Callable, Optional, Tuple


def interruptible_sleep(
    seconds: float,
    stop_event: Optional[threading.Event] = None,
    step: float = 1.0,
) -> bool:
    """
    可响应 `stop_event` 或 `KeyboardInterrupt` 的分片休眠。
    完整睡满返回 `True`，提前被中断或触发退出事件返回 `False`。
    """
    remaining = max(0.0, float(seconds or 0.0))
    if remaining <= 0:
        return not (stop_event is not None and stop_event.is_set())

    slice_sec = max(0.05, float(step or 1.0))
    deadline = time.monotonic() + remaining
    try:
        while True:
            if stop_event is not None and stop_event.is_set():
                return False
            left = deadline - time.monotonic()
            if left <= 0:
                return True
            wait_chunk = min(slice_sec, left)
            if stop_event is not None:
                if stop_event.wait(timeout=wait_chunk):
                    return False
            else:
                time.sleep(wait_chunk)
    except KeyboardInterrupt:
        return False


def compute_jittered_interval(
    base_seconds: float,
    jitter_ratio: float = 0.0,
    additive_jitter_range: Optional[Tuple[int, int]] = None,
) -> float:
    """计算带随机抖动的轮次间隔秒数（支持比例抖动或加性秒数区间抖动）。"""
    base = max(0.0, float(base_seconds or 0.0))
    if additive_jitter_range is not None and len(additive_jitter_range) == 2:
        low, high = int(additive_jitter_range[0]), int(additive_jitter_range[1])
        if low > high:
            low, high = high, low
        if high > 0:
            return max(1.0, base + random.randint(max(0, low), high))
    if jitter_ratio > 0 and base > 0:
        r = min(0.9, float(jitter_ratio))
        return max(1.0, base * random.uniform(1.0 - r, 1.0 + r))
    return base


def run_daemon_loop(
    step_fn: Callable[[int], Any],
    *,
    interval_seconds: float,
    jitter_ratio: float = 0.0,
    additive_jitter_range: Optional[Tuple[int, int]] = None,
    name: str = "daemon",
    logger_obj: Optional[logging.Logger] = None,
    max_rounds: int = 0,
    stop_event: Optional[threading.Event] = None,
    error_backoff_seconds: Optional[float] = None,
    sleep_step_seconds: float = 1.0,
) -> int:
    """
    通用后台守护循环：
    - 严禁静默吞异常：每轮未捕获异常通过 `logger.exception` 全量保留堆栈后进入安全退避；
    - 支持 `step_fn(round_idx)` 返回含 `"next_sleep_seconds"` 的字典动态控制本轮休眠时长（如精准对齐冷却到期）；
    - 支持 `max_rounds > 0` 单次/有限次执行以及 `stop_event` 优雅停机；
    - 返回已完成执行的轮次数。
    """
    log = logger_obj or logging.getLogger(name)
    round_idx = 0

    while True:
        if stop_event is not None and stop_event.is_set():
            log.info(f"[{name}] 检测到退出信号，守护循环安全停止。")
            break

        round_idx += 1
        sleep_override: Optional[float] = None
        had_error = False

        try:
            result = step_fn(round_idx)
            if isinstance(result, dict):
                override = result.get("next_sleep_seconds")
                if isinstance(override, (int, float)) and override > 0:
                    sleep_override = float(override)
        except KeyboardInterrupt:
            log.info(f"[{name}] 收到手动中断信号，守护循环安全停止。")
            break
        except Exception as exc:
            had_error = True
            log.exception(f"❌ [{name}] 第 {round_idx} 轮执行发生未预期异常（已隔离保护，下轮自动恢复）: {exc}")

        if max_rounds > 0 and round_idx >= max_rounds:
            break
        if stop_event is not None and stop_event.is_set():
            log.info(f"[{name}] 检测到退出信号，守护循环安全停止。")
            break

        if had_error and error_backoff_seconds is not None:
            wait_sec = float(error_backoff_seconds)
        elif sleep_override is not None:
            wait_sec = sleep_override
        else:
            wait_sec = compute_jittered_interval(
                interval_seconds,
                jitter_ratio=jitter_ratio,
                additive_jitter_range=additive_jitter_range,
            )

        if wait_sec > 0:
            if not interruptible_sleep(wait_sec, stop_event=stop_event, step=sleep_step_seconds):
                log.info(f"[{name}] 休眠期间收到中断或退出信号，安全停止。")
                break

    return round_idx
