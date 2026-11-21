# -*- coding: utf-8 -*-
"""
通用提示词模板渲染与结构化大模型调用网关 (`llm_gateway.py`)

核心职责：
1. 从 `prompt/pipeline_v2/` 加载外部提示词模板，计算 `prompt_sha1` 版本指纹。
2. 默认保持 `model=None` 并透传 `preset_model_group`（`"low"` / `"medium"` / `"high"`），
   完全复用 `app.ai_api.model_api.generate_content` 的加权负载均衡、重试与跨渠道自动降级机制。
3. 使用 `common.common_utils.string_to_object` 解析 JSON 字典，并支持轻量级字段契约校验 (`validator`)
   自动重试自愈，同时输出完整的 `llm_trace` 审计对象。
"""
import hashlib
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from app.ai_api.model_api import generate_content
from common.common_utils import read_file_to_str, string_to_object
from content_pipeline.pipeline_config import PIPELINE_PROMPT_DIR


def render_prompt_template(
    prompt_filename: str,
    variables: Dict[str, Any],
    prompt_dir: Path = PIPELINE_PROMPT_DIR,
) -> Tuple[str, Dict[str, str]]:
    """
    加载并渲染提示词模板（使用稳健的 `{{KEY}}` 占位符替换，避免正文中花括号与 Python `.format()` 冲突）。
    返回 `(rendered_prompt, prompt_meta)`。
    """
    p_path = Path(prompt_filename)
    if not p_path.is_absolute():
        p_path = (prompt_dir / p_path).resolve()
    if not p_path.is_file():
        raise FileNotFoundError(f"提示词模板文件不存在: {p_path}")

    raw_template = read_file_to_str(p_path)
    prompt_sha1 = hashlib.sha1(raw_template.encode("utf-8")).hexdigest()[:12]

    rendered = raw_template
    for key, val in (variables or {}).items():
        placeholder = "{{" + str(key) + "}}"
        rendered = rendered.replace(placeholder, str(val if val is not None else ""))

    prompt_meta = {
        "prompt_file": p_path.name,
        "prompt_path": str(p_path),
        "prompt_sha1": prompt_sha1,
    }
    return rendered, prompt_meta


def call_llm_json(
    prompt: str,
    prompt_meta: Dict[str, str],
    model: Optional[str] = None,
    fallback_models: Optional[List[str]] = None,
    file_paths: Optional[Sequence[str]] = None,
    timeout: int = 600,
    preset_model_group: str = "medium",
    validator: Optional[Callable[[Dict[str, Any]], Optional[str]]] = None,
    schema_retries: int = 2,
) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    """
    发起大模型调用并将输出解析校验为 Python `dict`。
    - 默认 `model=None` + `timeout=600`，交由 `model_api.py` 的 `preset_model_group` 统一选模与降级。
    - 若传入 `validator(parsed_dict) -> error_str_or_None`，当 JSON 缺失关键字段时自动重调修复（最多 `schema_retries` 次）。
    """
    valid_files: List[str] = []
    for fp in file_paths or []:
        if fp and Path(fp).is_file():
            valid_files.append(str(Path(fp).resolve()))

    normalized_model = model.strip() if isinstance(model, str) and model.strip() else None
    max_outer = max(1, int(schema_retries))
    accumulated_errors: List[str] = []
    last_trace: Dict[str, Any] = {}

    for outer_attempt in range(1, max_outer + 1):
        effective_prompt = prompt
        if outer_attempt > 1 and accumulated_errors:
            effective_prompt = (
                f"{prompt}\n\n"
                f"【系统格式强校验纠错警告】你上一次的输出未通过严格的 JSON 格式校验，失败原因：{accumulated_errors[-1]}。\n"
                f"请立即严格按照提示词规定的精确键名与字段类型重新输出纯 JSON 对象，严禁擅自更改字段名或在字符串数组中嵌套对象！"
            )
        resp = generate_content(
            prompt=effective_prompt,
            model=normalized_model,
            file_paths=valid_files if valid_files else None,
            fallback_model_list=fallback_models if normalized_model else None,
            max_retries_per_model=3,
            timeout=timeout,
            preset_model_group=preset_model_group,
        )

        metrics = resp.get("metrics") or {}
        raw_content = str(resp.get("content") or "").strip()
        call_ok = resp.get("status") == "✅ 成功"

        last_trace = {
            "call_status": "ok" if call_ok else "failed",
            "trace_id": resp.get("trace_id"),
            "prompt_file": prompt_meta.get("prompt_file"),
            "prompt_sha1": prompt_meta.get("prompt_sha1"),
            "model_requested": normalized_model or f"group:{preset_model_group}",
            "preset_model_group": preset_model_group,
            "model_used": metrics.get("model_used"),
            "duration_seconds": metrics.get("total_time_seconds", 0.0),
            "attempts": metrics.get("attempts", 0),
            "schema_attempt": outer_attempt,
            "attachment_count": len(valid_files),
            "attachment_paths": valid_files,
            "error_history": accumulated_errors + list(resp.get("error_history") or []),
            "parse_error": None,
            "raw_response": raw_content,
        }

        if not call_ok:
            return None, last_trace

        try:
            parsed = string_to_object(raw_content)
            if isinstance(parsed, list) and len(parsed) == 1 and isinstance(parsed[0], dict):
                parsed = parsed[0]
            if not isinstance(parsed, dict):
                raise ValueError(f"期望返回 JSON 对象(dict)，实际得到 {type(parsed).__name__}")
            if validator is not None:
                schema_err = validator(parsed)
                if schema_err:
                    raise ValueError(f"Schema校验未通过: {schema_err}")
            return parsed, last_trace
        except Exception as parse_exc:
            err_msg = f"[Schema尝试{outer_attempt}] {last_trace['model_used']}: {type(parse_exc).__name__}: {parse_exc}"
            accumulated_errors.append(err_msg)
            last_trace["call_status"] = "parse_failed"
            last_trace["parse_error"] = err_msg
            last_trace["error_history"] = accumulated_errors + list(resp.get("error_history") or [])

    return None, last_trace
