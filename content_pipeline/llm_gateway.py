# -*- coding: utf-8 -*-
"""
通用提示词模板渲染与结构化大模型调用网关 (`llm_gateway.py`)

核心职责：
1. 从 `prompt/pipeline_v2/` 加载外部提示词模板，计算 `prompt_sha1` 版本指纹（当你修改提示词后，轨迹里能精确区分版本）。
2. 基于 `app.ai_api.model_api.generate_content` 发起文本或多模态（带本地图片附件）调用。
3. 使用 `common.common_utils.string_to_object` 解析 JSON 字典，并产出完整的 `llm_trace` 审计对象，便于全链路问题定位与提示词调优。
"""
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

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
    timeout: int = 150,
    preset_model_group: str = "medium",
) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    """
    发起大模型调用并将输出解析为 Python `dict`。
    返回 `(parsed_dict_or_None, llm_trace)`，任何异常均被安全捕获并记录于 `llm_trace` 中。
    """
    valid_files: List[str] = []
    for fp in file_paths or []:
        if fp and Path(fp).is_file():
            valid_files.append(str(Path(fp).resolve()))

    resp = generate_content(
        prompt=prompt,
        model=model,
        file_paths=valid_files if valid_files else None,
        fallback_model_list=fallback_models,
        max_retries_per_model=2,
        timeout=timeout,
        preset_model_group=preset_model_group,
    )

    metrics = resp.get("metrics") or {}
    raw_content = str(resp.get("content") or "").strip()
    call_ok = resp.get("status") == "✅ 成功"

    llm_trace: Dict[str, Any] = {
        "call_status": "ok" if call_ok else "failed",
        "trace_id": resp.get("trace_id"),
        "prompt_file": prompt_meta.get("prompt_file"),
        "prompt_sha1": prompt_meta.get("prompt_sha1"),
        "model_requested": model or f"group:{preset_model_group}",
        "model_used": metrics.get("model_used"),
        "account_used": metrics.get("account_used"),
        "duration_seconds": metrics.get("total_time_seconds", 0.0),
        "attempts": metrics.get("attempts", 0),
        "attachment_count": len(valid_files),
        "attachment_paths": valid_files,
        "error_history": list(resp.get("error_history") or []),
        "parse_error": None,
        "raw_response": raw_content,
    }

    if not call_ok:
        return None, llm_trace

    try:
        parsed = string_to_object(raw_content)
        if isinstance(parsed, list) and len(parsed) == 1 and isinstance(parsed[0], dict):
            parsed = parsed[0]
        if not isinstance(parsed, dict):
            raise ValueError(f"期望返回 JSON 对象(dict)，实际得到 {type(parsed).__name__}")
        return parsed, llm_trace
    except Exception as parse_exc:
        llm_trace["call_status"] = "parse_failed"
        llm_trace["parse_error"] = f"{type(parse_exc).__name__}: {parse_exc}"
        return None, llm_trace
