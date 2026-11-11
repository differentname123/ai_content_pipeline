# -*- coding: utf-8 -*-
"""
多模态视觉配图工作室 (`visual_studio.py`)

支持 Step 4 的三条自适应视觉路径：
1. `reuse`（精选原图复用）：直接选用干净高冲击力的原帖本地高清图，复制到独立成品视觉目录。
2. `edit`（多模态 AI 图片编辑重制）：将原帖本地图 + 英文编辑指令输入多模态图像模型（如 `gemini-3.1-flash-image`），
   去除社交平台杂边/水印并重构为清爽配图；若远端图像模型偶发超时，自动平滑降级为本地 Pillow 边缘微裁切优化。
3. `generate`（多模态 AI 全新生图）：针对无图热帖（如 HackerNews / 纯文字突发帖），根据主题视觉提示词直接生成高点击率配图。
"""
import base64
import io
import shutil
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from openai import OpenAI
from PIL import Image, ImageEnhance

from app.ai_api.model_api import _build_content
from common.common_utils import get_config
from content_pipeline.pipeline_config import PIPELINE_VISUAL_DIR


def _extract_data_url_bytes(raw_text: str) -> Optional[bytes]:
    """
    使用线性字符串定位（避免在百万字节级 base64 字符串上使用复杂正则回溯），
    高效提取 `data:image/...;base64,...` 中的二进制图像载荷。
    """
    if not raw_text:
        return None
    marker = "data:image/"
    idx = raw_text.find(marker)
    if idx == -1:
        return None
    b64_sep = ";base64,"
    sep_idx = raw_text.find(b64_sep, idx)
    if sep_idx == -1:
        return None

    payload_start = sep_idx + len(b64_sep)
    # 终止符通常是 ')' (Markdown 图片)、'"'、'\''、'>' 或空白字符
    end_idx = len(raw_text)
    for stop_ch in (")", '"', "'", ">", " ", "\n", "\r"):
        pos = raw_text.find(stop_ch, payload_start)
        if pos != -1 and pos < end_idx:
            end_idx = pos

    b64_str = raw_text[payload_start:end_idx].strip()
    if len(b64_str) < 64:
        return None
    # 补齐可能缺失的 '=' padding
    pad = (-len(b64_str)) % 4
    if pad:
        b64_str += "=" * pad
    try:
        return base64.b64decode(b64_str)
    except Exception:
        return None


def _save_validated_image_bytes(img_bytes: bytes, dest_path: Path) -> Dict[str, Any]:
    """用 Pillow 校验解码后的图像完整性并统一保存为高质量 JPG。"""
    with Image.open(io.BytesIO(img_bytes)) as im:
        width, height = im.size
        rgb = im.convert("RGB")
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        rgb.save(dest_path, format="JPEG", quality=93, optimize=True)
    return {"width": width, "height": height, "size_bytes": dest_path.stat().st_size}


def _pillow_clean_fallback(source_path: Path, dest_path: Path) -> Dict[str, Any]:
    """当多模态改图接口不可用时的稳健本地降级：四周轻微内缩 3% 去边角杂讯 + 微调对比度刷新视觉指纹。"""
    with Image.open(source_path) as im:
        rgb = im.convert("RGB")
        w, h = rgb.size
        if w >= 200 and h >= 200:
            dx, dy = int(w * 0.03), int(h * 0.03)
            rgb = rgb.crop((dx, dy, w - dx, h - dy))
        rgb = ImageEnhance.Contrast(rgb).enhance(1.02)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        rgb.save(dest_path, format="JPEG", quality=93, optimize=True)
        final_w, final_h = rgb.size
    return {"width": final_w, "height": final_h, "size_bytes": dest_path.stat().st_size}


def _call_gateway_image_model(
    prompt: str,
    image_models: Sequence[str],
    source_image_path: Optional[Path] = None,
    timeout: int = 90,
) -> Dict[str, Any]:
    """通过本地统一 OpenAI 兼容网关调用多模态生图或带原图输入的改图模型。"""
    base_url = get_config("local_api_url")
    api_key = get_config("local_api_key")
    if not base_url or not api_key:
        return {"ok": False, "error": "缺少 local_api_url 或 local_api_key 配置", "errors": []}

    file_list = [str(source_image_path)] if (source_image_path and source_image_path.is_file()) else []
    msg_content: Any = _build_content(prompt, file_list) if file_list else prompt

    errors: List[str] = []
    client = OpenAI(base_url=base_url, api_key=api_key, max_retries=0, timeout=timeout)
    try:
        for model_name in image_models:
            t0 = time.perf_counter()
            try:
                resp = client.chat.completions.create(
                    model=model_name,
                    messages=[{"role": "user", "content": msg_content}],
                    stream=False,
                )
                cost = round(time.perf_counter() - t0, 2)
                if not resp.choices:
                    raise ValueError("无 choices 返回")
                raw_msg = resp.choices[0].message.content
                if isinstance(raw_msg, list):
                    raw_str = "\n".join(str(part) for part in raw_msg)
                else:
                    raw_str = str(raw_msg or "")

                img_bytes = _extract_data_url_bytes(raw_str)
                if not img_bytes:
                    raise ValueError(f"响应中未解析出有效 base64 图像数据 (长度={len(raw_str)})")

                return {
                    "ok": True,
                    "img_bytes": img_bytes,
                    "model_used": resp.model or model_name,
                    "duration_seconds": cost,
                    "errors": errors,
                }
            except Exception as exc:
                cost = round(time.perf_counter() - t0, 2)
                errors.append(f"{model_name}({cost}s): {type(exc).__name__}: {str(exc)[:140]}")
    finally:
        try:
            client.close()
        except Exception:
            pass

    return {"ok": False, "error": " | ".join(errors[-3:]), "errors": errors}


def produce_visual_asset(
    creation_id: str,
    visual_plan: Dict[str, Any],
    candidate_image_map: Dict[str, Dict[str, Any]],
    fallback_topic_title: str,
    image_models: Sequence[str],
    timeout: int = 90,
    output_dir: Path = PIPELINE_VISUAL_DIR,
) -> Dict[str, Any]:
    """
    根据 Step 4 创作模型给出的 `visual_plan` 执行配图生产，并返回完整的 `visual_trace`。
    """
    started = time.perf_counter()
    requested_action = str((visual_plan or {}).get("action") or "").strip().lower()
    selected_id = str((visual_plan or {}).get("selected_image_id") or "").strip()
    image_prompt = str((visual_plan or {}).get("image_prompt") or "").strip()
    visual_reason = str((visual_plan or {}).get("visual_reason") or "").strip()

    # 智能校准动作：若请求 reuse/edit 但没有对应的有效本地原图，则若有候选图取第一张，否则自动转 generate
    source_entry = candidate_image_map.get(selected_id)
    if requested_action in ("reuse", "edit") and not source_entry:
        if candidate_image_map:
            first_key = next(iter(candidate_image_map.keys()))
            selected_id = first_key
            source_entry = candidate_image_map[first_key]
        else:
            requested_action = "generate"

    if requested_action not in ("reuse", "edit", "generate"):
        requested_action = "reuse" if source_entry else "generate"

    source_path = Path(source_entry["local_path"]) if source_entry and source_entry.get("local_path") else None
    output_dir.mkdir(parents=True, exist_ok=True)

    trace: Dict[str, Any] = {
        "requested_action": (visual_plan or {}).get("action"),
        "executed_action": requested_action,
        "selected_image_id": selected_id or None,
        "source_image_path": str(source_path) if source_path else None,
        "visual_reason": visual_reason,
        "image_prompt": image_prompt,
        "image_model_used": None,
        "final_image_path": None,
        "image_meta": None,
        "fallback_triggered": False,
        "error_history": [],
        "duration_seconds": 0.0,
    }

    # 路径 A：直接复用干净原图
    if requested_action == "reuse" and source_path and source_path.is_file():
        suffix = source_path.suffix.lower() or ".jpg"
        dest = output_dir / f"{creation_id}_reuse{suffix}"
        shutil.copy2(source_path, dest)
        with Image.open(dest) as im:
            w, h = im.size
        trace["final_image_path"] = str(dest)
        trace["image_meta"] = {"width": w, "height": h, "size_bytes": dest.stat().st_size}
        trace["duration_seconds"] = round(time.perf_counter() - started, 2)
        return trace

    # 路径 B：多模态 AI 改图重制（带本地 Pillow 兜底）
    if requested_action == "edit" and source_path and source_path.is_file():
        effective_edit_prompt = image_prompt or (
            "Keep the core subject and factual visual elements of this image intact, "
            "remove any social media UI borders, watermarks, or cluttered overlays, "
            "and refine it into a clean, high-clarity editorial illustration."
        )
        trace["image_prompt"] = effective_edit_prompt
        dest = output_dir / f"{creation_id}_edited.jpg"
        gw_res = _call_gateway_image_model(
            prompt=effective_edit_prompt,
            image_models=image_models,
            source_image_path=source_path,
            timeout=timeout,
        )
        trace["error_history"] = gw_res.get("errors") or []
        if gw_res.get("ok"):
            meta = _save_validated_image_bytes(gw_res["img_bytes"], dest)
            trace["image_model_used"] = gw_res.get("model_used")
            trace["final_image_path"] = str(dest)
            trace["image_meta"] = meta
        else:
            # 平滑降级到本地 Pillow 边缘清洗优化，确保成品必定有干净配图
            trace["fallback_triggered"] = True
            trace["executed_action"] = "edit_pillow_fallback"
            trace["image_model_used"] = "local_pillow_clean"
            meta = _pillow_clean_fallback(source_path, dest)
            trace["final_image_path"] = str(dest)
            trace["image_meta"] = meta
        trace["duration_seconds"] = round(time.perf_counter() - started, 2)
        return trace

    # 路径 C：多模态 AI 全新生图
    effective_gen_prompt = image_prompt or (
        f"Create a striking, high-contrast editorial hero illustration representing: {fallback_topic_title}. "
        "Clean modern digital art style, cinematic lighting, 16:9 aspect ratio, no text clutter."
    )
    trace["executed_action"] = "generate"
    trace["image_prompt"] = effective_gen_prompt
    dest = output_dir / f"{creation_id}_generated.jpg"
    gw_res = _call_gateway_image_model(
        prompt=effective_gen_prompt,
        image_models=image_models,
        source_image_path=None,
        timeout=timeout,
    )
    trace["error_history"] = gw_res.get("errors") or []
    if gw_res.get("ok"):
        meta = _save_validated_image_bytes(gw_res["img_bytes"], dest)
        trace["image_model_used"] = gw_res.get("model_used")
        trace["final_image_path"] = str(dest)
        trace["image_meta"] = meta
    elif source_path and source_path.is_file():
        # 若生图所有模型均超时但恰有原帖图，兜底使用原图清洗版
        trace["fallback_triggered"] = True
        trace["executed_action"] = "generate_fallback_to_source"
        trace["image_model_used"] = "local_pillow_clean"
        meta = _pillow_clean_fallback(source_path, dest)
        trace["final_image_path"] = str(dest)
        trace["image_meta"] = meta

    trace["duration_seconds"] = round(time.perf_counter() - started, 2)
    return trace
