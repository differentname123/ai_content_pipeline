# -*- coding: utf-8 -*-
"""
轻量级结构化埋点工具 (`parser/telemetry.py`)

将关键漏斗、发布归因、流量曲线采样与生成汇总事件以有界轮转 JSONL 落盘
（默认 `logs/runtime/telemetry_events.jsonl`，单文件 5MB × 最多 4 份，<=20MB），
任何 I/O 或序列化异常均静默隔离，绝不阻断业务主流程。
"""
import json
import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Optional

from common.common_utils import get_runtime_file_path

_BEIJING_TZ = timezone(timedelta(hours=8))
TELEMETRY_LOG_FILE = str(
    get_runtime_file_path(
        "telemetry_events.jsonl",
        legacy_path=None,
    )
)

_TELEMETRY_LOCK = threading.Lock()
_TELEMETRY_LOGGER = logging.getLogger("pipeline.telemetry")
_TELEMETRY_LOGGER.setLevel(logging.INFO)
_TELEMETRY_LOGGER.propagate = False
_CURRENT_LOG_PATH: Optional[str] = None


def _ensure_telemetry_handler(log_path: str) -> logging.Logger:
    global _CURRENT_LOG_PATH
    with _TELEMETRY_LOCK:
        if _CURRENT_LOG_PATH != log_path or not _TELEMETRY_LOGGER.handlers:
            for handler in list(_TELEMETRY_LOGGER.handlers):
                _TELEMETRY_LOGGER.removeHandler(handler)
                try:
                    handler.close()
                except Exception:
                    pass
            target = Path(log_path)
            target.parent.mkdir(parents=True, exist_ok=True)
            handler = RotatingFileHandler(
                str(target),
                maxBytes=5 * 1024 * 1024,
                backupCount=3,
                encoding="utf-8",
            )
            handler.setFormatter(logging.Formatter("%(message)s"))
            _TELEMETRY_LOGGER.addHandler(handler)
            _CURRENT_LOG_PATH = log_path
    return _TELEMETRY_LOGGER


def reset_telemetry_handler_for_test(log_path: Optional[str] = None) -> str:
    """测试专用：将埋点 JSONL 路径重定向到临时文件并重置文件句柄。"""
    global TELEMETRY_LOG_FILE, _CURRENT_LOG_PATH
    target = str(log_path or TELEMETRY_LOG_FILE)
    with _TELEMETRY_LOCK:
        for handler in list(_TELEMETRY_LOGGER.handlers):
            _TELEMETRY_LOGGER.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass
        _CURRENT_LOG_PATH = None
        TELEMETRY_LOG_FILE = target
    return target


def emit_telemetry_event(
    event_type: str,
    payload: Optional[Dict[str, Any]] = None,
    *,
    now_ts: Optional[float] = None,
    log_path: Optional[str] = None,
) -> bool:
    """写入一条带北京时间时间戳的结构化 JSONL 埋点事件；异常静默降级返回 False。"""
    try:
        ts = time.time() if now_ts is None else float(now_ts)
        record: Dict[str, Any] = {
            "ts": int(ts),
            "at": datetime.fromtimestamp(ts, tz=_BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S"),
            "event": str(event_type or "unknown"),
        }
        if isinstance(payload, dict):
            for key, value in payload.items():
                if value is not None:
                    record[key] = value
        line = json.dumps(record, ensure_ascii=False, default=str)
        target_logger = _ensure_telemetry_handler(str(log_path or TELEMETRY_LOG_FILE))
        target_logger.info(line)
        return True
    except Exception:
        return False


__all__ = [
    "TELEMETRY_LOG_FILE",
    "emit_telemetry_event",
    "reset_telemetry_handler_for_test",
]
