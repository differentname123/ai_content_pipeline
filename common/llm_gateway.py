# -*- coding: utf-8 -*-
"""
统一提示词渲染与结构化 JSON 大模型调用网关 (`common/llm_gateway.py`)

核心原则：
1. 默认强制 `model=None` 并透传 `preset_model_group`（`"low"` / `"medium"` / `"high"`），
   完全复用 `app.ai_api.model_api.generate_content` 的负载均衡、重试与跨模型降级机制；
2. 统一提示词加载与变量替换（同时安全支持 `{{KEY}}` 与 `{KEY}` 明确变量占位，不误伤 JSON 示例花括号）；
3. 统一 JSON 解析（Markdown 围栏剥离、注释/尾逗号容错）、类型与 Schema 校验自愈重试，并忠实透传 `model_used`。
"""
import hashlib
import logging
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

from app.ai_api.model_api import generate_content
from common.common_utils import PROMPT_DIR, read_file_to_str, string_to_object

logger = logging.getLogger(__name__)


def render_prompt_template(
    prompt_filename: Union[str, Path],
    variables: Optional[Dict[str, Any]] = None,
    prompt_dir: Union[str, Path] = PROMPT_DIR,
) -> Tuple[str, Dict[str, str]]:
    """
    加载提示词模板并按明确键名替换 `{{KEY}}` / `{KEY}` 占位符，返回 `(rendered_prompt, prompt_meta)`。
    """
    p_path = Path(prompt_filename)
    if not p_path.is_absolute():
        candidate = (Path(prompt_dir) / p_path).resolve()
        if candidate.is_file():
            p_path = candidate
        else:
            p_path = p_path.resolve()
    if not p_path.is_file():
        raise FileNotFoundError(f"提示词模板文件不存在: {p_path}")

    raw_template = read_file_to_str(p_path)
    prompt_sha1 = hashlib.sha1(raw_template.encode("utf-8")).hexdigest()[:12]

    rendered = raw_template
    for key, val in (variables or {}).items():
        val_str = str(val if val is not None else "")
        rendered = rendered.replace("{{" + str(key) + "}}", val_str)
        rendered = rendered.replace("{" + str(key) + "}", val_str)

    prompt_meta = {
        "prompt_file": p_path.name,
        "prompt_path": str(p_path),
        "prompt_sha1": prompt_sha1,
    }
    return rendered, prompt_meta


def parse_llm_json(raw_text: str) -> Optional[Any]:
    """容错解析大模型输出的结构化 JSON（失败返回 None）。"""
    if not isinstance(raw_text, str) or not raw_text.strip():
        return None
    cleaned = raw_text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("\n", 1)[-1]
        if cleaned.rstrip().endswith("```"):
            cleaned = cleaned.rstrip()[:-3].rstrip()
    try:
        return string_to_object(cleaned)
    except Exception:
        return None


def _run_validator(validator: Optional[Callable[[Any], Any]], data: Any) -> Optional[str]:
    if validator is None:
        return None
    res = validator(data)
    if res is None or res is True or res == "":
        return None
    if res is False:
        return "Schema 校验未通过"
    if isinstance(res, tuple) and len(res) >= 2:
        ok, msg = bool(res[0]), str(res[1] or "")
        return None if ok else (msg or "Schema 校验未通过")
    if isinstance(res, str):
        return res
    return str(res)


def call_llm_json(
    prompt: str,
    prompt_meta: Optional[Dict[str, str]] = None,
    model: Optional[str] = None,
    fallback_models: Optional[List[str]] = None,
    file_paths: Optional[Sequence[str]] = None,
    timeout: int = 600,
    preset_model_group: str = "medium",
    validator: Optional[Callable[[Any], Any]] = None,
    expected_type: Optional[Union[type, Tuple[type, ...]]] = dict,
    schema_retries: int = 2,
    max_attempts: Optional[int] = None,
    retry_times: int = 3,
    task_label: str = "",
    logger_obj: Optional[logging.Logger] = None,
) -> Tuple[Optional[Any], Dict[str, Any]]:
    """
    发起大模型调用并解析、校验结构化 JSON 输出。
    默认 `model=None` + `preset_model_group="medium"`，严禁业务层硬编码具体模型名。
    """
    log = logger_obj or logger
    p_meta = dict(prompt_meta or {})

    valid_files: List[str] = []
    for fp in file_paths or []:
        if fp and Path(fp).is_file():
            valid_files.append(str(Path(fp).resolve()))

    normalized_model = model.strip() if isinstance(model, str) and model.strip() else None
    max_outer = max(1, int(max_attempts if max_attempts is not None else schema_retries))
    accumulated_errors: List[str] = []
    last_trace: Dict[str, Any] = {
        "ok": False,
        "call_status": "failed",
        "trace_id": "",
        "prompt_file": p_meta.get("prompt_file"),
        "prompt_sha1": p_meta.get("prompt_sha1"),
        "model_requested": normalized_model or f"group:{preset_model_group}",
        "preset_model_group": preset_model_group,
        "model_used": "",
        "duration_seconds": 0.0,
        "elapsed_ms": 0,
        "attempts": 0,
        "schema_attempt": 0,
        "attachment_count": len(valid_files),
        "attachment_paths": valid_files,
        "error_history": [],
        "parse_error": None,
        "error": "",
        "raw_response": "",
    }

    t0 = time.perf_counter()
    for outer_attempt in range(1, max_outer + 1):
        effective_prompt = prompt
        if outer_attempt > 1 and accumulated_errors:
            effective_prompt = (
                f"{prompt}\n\n"
                f"【系统格式强校验纠错警告】你上一次的输出未通过严格的结构化 JSON 校验，失败原因：{accumulated_errors[-1]}。\n"
                f"请立即严格按照提示词规定的精确键名与字段类型重新输出纯 JSON，严禁附加任何解释文字或代码围栏！"
            )
        resp = generate_content(
            prompt=effective_prompt,
            model=normalized_model,
            file_paths=valid_files if valid_files else None,
            fallback_model_list=fallback_models if normalized_model else None,
            max_retries_per_model=max(1, int(retry_times)),
            timeout=timeout,
            preset_model_group=preset_model_group,
        )

        metrics = resp.get("metrics") or {}
        raw_content = str(resp.get("content") or "").strip()
        call_ok = resp.get("status") == "✅ 成功"
        model_used = str(metrics.get("model_used") or "").strip()
        elapsed_ms = int((time.perf_counter() - t0) * 1000)

        last_trace = {
            "ok": False,
            "call_status": "ok" if call_ok else "failed",
            "trace_id": resp.get("trace_id") or "",
            "prompt_file": p_meta.get("prompt_file"),
            "prompt_sha1": p_meta.get("prompt_sha1"),
            "model_requested": normalized_model or f"group:{preset_model_group}",
            "preset_model_group": preset_model_group,
            "model_used": model_used,
            "duration_seconds": metrics.get("total_time_seconds", 0.0),
            "elapsed_ms": elapsed_ms,
            "attempts": metrics.get("attempts", 0),
            "schema_attempt": outer_attempt,
            "attachment_count": len(valid_files),
            "attachment_paths": valid_files,
            "error_history": accumulated_errors + list(resp.get("error_history") or []),
            "parse_error": None,
            "error": "",
            "raw_response": raw_content,
        }

        if not call_ok:
            last_trace["error"] = "; ".join(str(x) for x in (resp.get("error_history") or [raw_content[:200]]))
            return None, last_trace

        try:
            parsed = string_to_object(raw_content)
            if expected_type is dict and isinstance(parsed, list) and len(parsed) == 1 and isinstance(parsed[0], dict):
                parsed = parsed[0]
            if expected_type is not None and not isinstance(parsed, expected_type):
                exp_name = (
                    ", ".join(t.__name__ for t in expected_type)
                    if isinstance(expected_type, tuple)
                    else expected_type.__name__
                )
                raise ValueError(f"期望返回 JSON 类型 ({exp_name})，实际得到 {type(parsed).__name__}")
            schema_err = _run_validator(validator, parsed)
            if schema_err:
                raise ValueError(f"Schema校验未通过: {schema_err}")
            last_trace["ok"] = True
            return parsed, last_trace
        except Exception as parse_exc:
            err_msg = f"[Schema尝试{outer_attempt}] {model_used or preset_model_group}: {type(parse_exc).__name__}: {parse_exc}"
            accumulated_errors.append(err_msg)
            last_trace["ok"] = False
            last_trace["call_status"] = "parse_failed"
            last_trace["parse_error"] = err_msg
            last_trace["error"] = err_msg
            last_trace["error_history"] = accumulated_errors + list(resp.get("error_history") or [])
            if task_label and outer_attempt < max_outer:
                log.warning(f"[LLM网关/{task_label}] {err_msg}，准备重试第 {outer_attempt + 1}/{max_outer} 次...")

    return None, last_trace
