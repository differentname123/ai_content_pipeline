# -*- coding: utf-8 -*-
"""
真实像素视觉工作室 (`visual_studio.py`)

核心红线与 80/20 实证原则：
1. 严禁凭空 AI 生图 (`generate`) 与破坏原图像素的全图重绘——杜绝一眼假的 AI 插画。
2. 100% 基于采集到的真实原帖图片，执行高可靠、零失真的确定性像素编辑：
   - `none`  ：无合适真实原图时直接发纯文本推文（`final_image_paths = []`）；
   - `reuse` ：直接复用 1~4 张画面干净的真实原图（零损复制）；
   - `crop`  ：按多模态模型输出的归一化坐标 `[ymin, xmin, ymax, xmax]` (0..1000) 精准裁掉手机状态栏、
               社交软件顶底栏、原作者昵称及边缘水印，100% 保留主体原始清晰像素；
   - `stitch`：将 2~3 张强关联真实原图（支持先各自裁剪核心区）按等高横向或等宽纵向无损比例拼接，
               自动规避极端细长条比例，适配 X 信息流最佳展示尺寸；
   - `mask_boxes`（可选局部去小水印）：仅针对画面保留区内面积 `<= 8%` 的边角小水印执行平滑拉普拉斯边界插值消除，
               内置面积硬护栏，绝不允许涂抹画面中心主体。
"""
import shutil
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
from PIL import Image

from content_pipeline.pipeline_config import PIPELINE_VISUAL_DIR

MAX_X_PUBLISH_IMAGES = 4
MAX_MASK_AREA_RATIO = 0.08
MAX_MASK_HEIGHT_RATIO = 0.22
STITCH_GAP_PX = 8
STITCH_BG_COLOR = (22, 24, 28)


def _parse_norm_box(raw_box: Any) -> Optional[Tuple[float, float, float, float]]:
    """解析并校准 `0..1000` 归一化坐标框 `[ymin, xmin, ymax, xmax]`。"""
    if not isinstance(raw_box, (list, tuple)) or len(raw_box) != 4:
        return None
    try:
        ymin, xmin, ymax, xmax = (float(v) for v in raw_box)
    except (TypeError, ValueError):
        return None
    ymin = max(0.0, min(1000.0, ymin))
    xmin = max(0.0, min(1000.0, xmin))
    ymax = max(0.0, min(1000.0, ymax))
    xmax = max(0.0, min(1000.0, xmax))
    if ymin > ymax:
        ymin, ymax = ymax, ymin
    if xmin > xmax:
        xmin, xmax = xmax, xmin
    if (ymax - ymin) < 10.0 or (xmax - xmin) < 10.0:
        return None
    return (ymin, xmin, ymax, xmax)


def _norm_box_to_pixels(
    norm_box: Tuple[float, float, float, float],
    width: int,
    height: int,
) -> Tuple[int, int, int, int]:
    """将 `(ymin, xmin, ymax, xmax)` 转换为 Pillow 像素框 `(left, top, right, bottom)`。"""
    ymin, xmin, ymax, xmax = norm_box
    left = max(0, min(width - 1, int(round(xmin / 1000.0 * width))))
    top = max(0, min(height - 1, int(round(ymin / 1000.0 * height))))
    right = max(left + 1, min(width, int(round(xmax / 1000.0 * width))))
    bottom = max(top + 1, min(height, int(round(ymax / 1000.0 * height))))
    return (left, top, right, bottom)


def _is_full_frame_box(norm_box: Optional[Tuple[float, float, float, float]]) -> bool:
    if norm_box is None:
        return True
    ymin, xmin, ymax, xmax = norm_box
    return ymin <= 12.0 and xmin <= 12.0 and ymax >= 988.0 and xmax >= 988.0


def _validate_crop_pixel_box(
    px_box: Tuple[int, int, int, int],
    orig_w: int,
    orig_h: int,
) -> Tuple[bool, str]:
    """校验裁剪框物理尺寸与长宽比合理性，防止异常坐标把图裁成残缺细条。"""
    left, top, right, bottom = px_box
    cw, ch = right - left, bottom - top
    if cw < 160 or ch < 120:
        return False, f"裁剪后尺寸过小({cw}x{ch})"
    orig_area = max(1, orig_w * orig_h)
    if (cw * ch) / orig_area < 0.14:
        return False, f"裁剪面积占比过低({(cw * ch) / orig_area:.1%})"
    aspect = cw / max(1, ch)
    if aspect < 0.22 or aspect > 4.5:
        return False, f"裁剪长宽比极端({aspect:.2f})"
    return True, ""


def _heal_small_watermark_boxes(
    img_rgb: Image.Image,
    raw_mask_boxes: Sequence[Any],
    kept_px_box: Tuple[int, int, int, int],
) -> Tuple[Image.Image, List[str]]:
    """
    使用边界中值初始化 + 向量化拉普拉斯谐波插值（Laplace Harmonic Inpainting），
    平滑消除画面边角的小尺寸文字/台标水印。
    严格护栏：单框面积不得超过保留画面面积的 8%，高度不得超过保留高度的 22%。
    """
    notes: List[str] = []
    if not raw_mask_boxes:
        return img_rgb, notes

    w, h = img_rgb.size
    k_left, k_top, k_right, k_bottom = kept_px_box
    kept_w = max(1, k_right - k_left)
    kept_h = max(1, k_bottom - k_top)
    kept_area = float(kept_w * kept_h)

    arr = np.asarray(img_rgb, dtype=np.float32).copy()
    modified = False

    for idx, raw_mb in enumerate(raw_mask_boxes):
        nb = _parse_norm_box(raw_mb)
        if nb is None:
            continue
        x1, y1, x2, y2 = _norm_box_to_pixels(nb, w, h)
        # 计算与保留裁剪区的交集，若该水印本就已被裁剪框切掉则无需涂抹
        ix1, iy1 = max(x1, k_left), max(y1, k_top)
        ix2, iy2 = min(x2, k_right), min(y2, k_bottom)
        if ix2 - ix1 < 4 or iy2 - iy1 < 4:
            notes.append(f"mask#{idx + 1}:outside_crop_skipped")
            continue

        box_area = float((ix2 - ix1) * (iy2 - iy1))
        if (box_area / kept_area) > MAX_MASK_AREA_RATIO or (iy2 - iy1) > (kept_h * MAX_MASK_HEIGHT_RATIO):
            notes.append(f"mask#{idx + 1}:oversized_skipped({box_area / kept_area:.1%})")
            continue

        # 微扩 2px 覆盖文字抗锯齿边缘
        pad_inner = 2
        mx1 = max(0, x1 - pad_inner)
        my1 = max(0, y1 - pad_inner)
        mx2 = min(w, x2 + pad_inner)
        my2 = min(h, y2 + pad_inner)

        ring = 4
        px1 = max(0, mx1 - ring)
        py1 = max(0, my1 - ring)
        px2 = min(w, mx2 + ring)
        py2 = min(h, my2 + ring)

        patch = arr[py1:py2, px1:px2].copy()
        mask = np.zeros((py2 - py1, px2 - px1), dtype=bool)
        mask[(my1 - py1):(my2 - py1), (mx1 - px1):(mx2 - px1)] = True

        border_pixels = patch[~mask]
        if border_pixels.size == 0:
            continue

        init_color = np.median(border_pixels, axis=0)
        patch[mask] = init_color

        # 60 次向量化四邻域拉普拉斯松弛，毫秒级无缝过渡背景渐变或纯色底板
        for _ in range(60):
            up = np.vstack([patch[0:1, :], patch[:-1, :]])
            down = np.vstack([patch[1:, :], patch[-1:, :]])
            left = np.hstack([patch[:, 0:1], patch[:, :-1]])
            right = np.hstack([patch[:, 1:], patch[:, -1:]])
            patch[mask] = 0.25 * (up + down + left + right)[mask]

        arr[py1:py2, px1:px2] = patch
        modified = True
        notes.append(f"mask#{idx + 1}:healed({mx2 - mx1}x{my2 - my1}px)")

    if not modified:
        return img_rgb, notes
    return Image.fromarray(np.clip(arr, 0, 255).astype(np.uint8), mode="RGB"), notes


def _save_crisp_image(img: Image.Image, dest_base: Path, prefer_png: bool = False) -> Tuple[Path, Dict[str, Any]]:
    """
    高保真保存：截图/表格类 PNG 保持无损 PNG（防止小字边缘出现 JPEG 噪点），照片类保存为 95 质量无色度抽样 JPG。
    """
    dest_base.parent.mkdir(parents=True, exist_ok=True)
    rgb = img.convert("RGB")
    w, h = rgb.size
    if prefer_png:
        final_path = dest_base.with_suffix(".png")
        rgb.save(final_path, format="PNG", optimize=True)
    else:
        final_path = dest_base.with_suffix(".jpg")
        rgb.save(final_path, format="JPEG", quality=95, subsampling=0, optimize=True)
    return final_path, {"width": w, "height": h, "size_bytes": final_path.stat().st_size}


def _process_single_panel(
    source_path: Path,
    raw_crop_box: Any,
    raw_mask_boxes: Sequence[Any],
) -> Tuple[Image.Image, bool, List[str]]:
    """对单张真实原图执行可选局部去水印与裁剪，返回 `(处理后RGB图像, 是否发生了实质修改, 审计说明列表)`。"""
    notes: List[str] = []
    with Image.open(source_path) as im:
        rgb = im.convert("RGB")

    orig_w, orig_h = rgb.size
    norm_crop = _parse_norm_box(raw_crop_box)
    kept_px = (0, 0, orig_w, orig_h)
    did_crop = False

    if norm_crop is not None and not _is_full_frame_box(norm_crop):
        cand_px = _norm_box_to_pixels(norm_crop, orig_w, orig_h)
        ok, err_reason = _validate_crop_pixel_box(cand_px, orig_w, orig_h)
        if ok:
            kept_px = cand_px
            did_crop = True
            notes.append(f"crop:{cand_px[0]},{cand_px[1]}->{cand_px[2]},{cand_px[3]}")
        else:
            notes.append(f"crop_skipped({err_reason})")

    healed_rgb, mask_notes = _heal_small_watermark_boxes(rgb, raw_mask_boxes or [], kept_px)
    notes.extend(mask_notes)
    did_heal = any(":healed" in n for n in mask_notes)

    if did_crop:
        healed_rgb = healed_rgb.crop(kept_px)

    return healed_rgb, (did_crop or did_heal), notes


def _stitch_panels(
    panels: List[Image.Image],
    requested_layout: str = "auto",
) -> Tuple[Image.Image, str]:
    """
    将 2~3 张真实画面按原比例无变形拼接为一张高可读性组合图：
    - 竖屏/方图默认横向并排 (`horizontal`)，横屏宽图默认纵向叠放 (`vertical`)；
    - 若纵向叠放会导致总高宽比 `> 2.5`（在 X 信息流会被严重上下截断），自动转为横向并排。
    """
    layout = str(requested_layout or "auto").strip().lower()
    avg_aspect = sum(im.size[0] / max(1, im.size[1]) for im in panels) / len(panels)

    if layout not in ("horizontal", "vertical"):
        layout = "horizontal" if avg_aspect < 1.18 else "vertical"

    if layout == "vertical":
        target_w = min(1400, max(im.size[0] for im in panels))
        projected_h = sum(int(round(im.size[1] * (target_w / max(1, im.size[0])))) for im in panels) + STITCH_GAP_PX * (len(panels) - 1)
        if projected_h / max(1, target_w) > 2.5:
            layout = "horizontal"

    if layout == "horizontal":
        target_h = min(1350, max(im.size[1] for im in panels))
        resized = [
            im.resize((max(1, int(round(im.size[0] * (target_h / max(1, im.size[1]))))), target_h), Image.Resampling.LANCZOS)
            for im in panels
        ]
        total_w = sum(r.size[0] for r in resized) + STITCH_GAP_PX * (len(resized) - 1)
        canvas = Image.new("RGB", (total_w, target_h), color=STITCH_BG_COLOR)
        x_cursor = 0
        for r in resized:
            canvas.paste(r, (x_cursor, 0))
            x_cursor += r.size[0] + STITCH_GAP_PX
        return canvas, "horizontal"

    target_w = min(1400, max(im.size[0] for im in panels))
    resized = [
        im.resize((target_w, max(1, int(round(im.size[1] * (target_w / max(1, im.size[0])))))), Image.Resampling.LANCZOS)
        for im in panels
    ]
    total_h = sum(r.size[1] for r in resized) + STITCH_GAP_PX * (len(resized) - 1)
    canvas = Image.new("RGB", (target_w, total_h), color=STITCH_BG_COLOR)
    y_cursor = 0
    for r in resized:
        canvas.paste(r, (0, y_cursor))
        y_cursor += r.size[1] + STITCH_GAP_PX
    return canvas, "vertical"


def produce_visual_asset(
    creation_id: str,
    visual_plan: Dict[str, Any],
    candidate_image_map: Dict[str, Dict[str, Any]],
    output_dir: Path = PIPELINE_VISUAL_DIR,
) -> Dict[str, Any]:
    """
    根据 Step 4 输出的 `visual_plan` 执行确定性真实原图加工，绝不调用任何 AI 生图接口。
    返回完整的 `visual_trace`（含 `final_image_paths` 列表与首图 `final_image_path`）。
    """
    started = time.perf_counter()
    plan = visual_plan if isinstance(visual_plan, dict) else {}
    requested_action = str(plan.get("action") or "none").strip().lower()
    raw_items = plan.get("items") if isinstance(plan.get("items"), list) else []
    stitch_layout = str(plan.get("stitch_layout") or "auto").strip().lower()
    reason = str(plan.get("reason") or plan.get("visual_reason") or "").strip()

    trace: Dict[str, Any] = {
        "requested_action": requested_action,
        "executed_action": "none",
        "reason": reason,
        "selected_image_ids": [],
        "source_image_paths": [],
        "final_image_paths": [],
        "final_image_path": None,
        "image_metas": [],
        "operation_notes": [],
        "duration_seconds": 0.0,
    }

    if not candidate_image_map or requested_action == "none":
        trace["duration_seconds"] = round(time.perf_counter() - started, 3)
        return trace

    # 解析有效的候选图条目（按请求顺序去重，最多 4 张）
    valid_specs: List[Dict[str, Any]] = []
    seen_ids = set()
    for it in raw_items:
        if not isinstance(it, dict):
            continue
        img_id = str(it.get("image_id") or "").strip()
        if not img_id or img_id in seen_ids or img_id not in candidate_image_map:
            continue
        src_p = Path(candidate_image_map[img_id]["local_path"])
        if not src_p.is_file():
            continue
        try:
            with Image.open(src_p) as im_chk:
                sw, sh = im_chk.size
            if min(sw, sh) < 120 or max(sw, sh) < 240:
                trace["operation_notes"].append(f"{img_id}:tiny_thumbnail_skipped({sw}x{sh})")
                continue
        except Exception:
            continue
        seen_ids.add(img_id)
        valid_specs.append(
            {
                "image_id": img_id,
                "source_path": src_p,
                "crop_box": it.get("crop_box"),
                "mask_boxes": it.get("mask_boxes") if isinstance(it.get("mask_boxes"), list) else [],
            }
        )
        if len(valid_specs) >= MAX_X_PUBLISH_IMAGES:
            break

    if not valid_specs:
        # 若模型动作非 none 但未正确填写 items，安全回退为不配图（绝不乱塞无关图或 AI 生图）
        trace["operation_notes"].append("no_valid_items_matched_fallback_to_none")
        trace["duration_seconds"] = round(time.perf_counter() - started, 3)
        return trace

    output_dir.mkdir(parents=True, exist_ok=True)
    trace["selected_image_ids"] = [s["image_id"] for s in valid_specs]
    trace["source_image_paths"] = [str(s["source_path"]) for s in valid_specs]

    # 路径 1：多图拼接 (`stitch`，需至少 2 张有效图；若仅 1 张则自动平滑转为单图 `crop` / `reuse`)
    if requested_action == "stitch" and len(valid_specs) >= 2:
        panels: List[Image.Image] = []
        prefer_png = False
        for s in valid_specs[:3]:
            if s["source_path"].suffix.lower() == ".png":
                prefer_png = True
            panel_im, _, p_notes = _process_single_panel(
                s["source_path"],
                s["crop_box"],
                s["mask_boxes"],
            )
            panels.append(panel_im)
            trace["operation_notes"].extend(f"{s['image_id']}:{n}" for n in p_notes)

        stitched_im, used_layout = _stitch_panels(panels, requested_layout=stitch_layout)
        dest_path, meta = _save_crisp_image(
            stitched_im,
            output_dir / f"{creation_id}_stitch_{used_layout}",
            prefer_png=prefer_png,
        )
        trace["executed_action"] = f"stitch_{used_layout}"
        trace["final_image_paths"] = [str(dest_path)]
        trace["final_image_path"] = str(dest_path)
        trace["image_metas"] = [meta]
        trace["duration_seconds"] = round(time.perf_counter() - started, 3)
        return trace

    # 路径 2：单图或多图独立复用/裁剪/去边角水印 (`reuse` / `crop`)
    final_paths: List[str] = []
    metas: List[Dict[str, Any]] = []
    any_modified = False

    for idx, s in enumerate(valid_specs, 1):
        src_path: Path = s["source_path"]
        wants_crop = requested_action in ("crop", "stitch") and s["crop_box"] is not None
        wants_mask = bool(s["mask_boxes"])

        if not wants_crop and not wants_mask:
            suffix = src_path.suffix.lower() or ".jpg"
            dest = output_dir / f"{creation_id}_reuse_{idx}{suffix}"
            shutil.copy2(src_path, dest)
            with Image.open(dest) as im:
                w, h = im.size
            final_paths.append(str(dest))
            metas.append({"width": w, "height": h, "size_bytes": dest.stat().st_size})
            trace["operation_notes"].append(f"{s['image_id']}:lossless_copy")
        else:
            processed_im, modified, p_notes = _process_single_panel(
                src_path,
                s["crop_box"] if wants_crop else None,
                s["mask_boxes"],
            )
            trace["operation_notes"].extend(f"{s['image_id']}:{n}" for n in p_notes)
            if modified:
                any_modified = True
                dest, meta = _save_crisp_image(
                    processed_im,
                    output_dir / f"{creation_id}_clean_{idx}",
                    prefer_png=(src_path.suffix.lower() == ".png"),
                )
            else:
                suffix = src_path.suffix.lower() or ".jpg"
                dest = output_dir / f"{creation_id}_reuse_{idx}{suffix}"
                shutil.copy2(src_path, dest)
                with Image.open(dest) as im:
                    w, h = im.size
                meta = {"width": w, "height": h, "size_bytes": dest.stat().st_size}
            final_paths.append(str(dest))
            metas.append(meta)

    trace["executed_action"] = "crop" if any_modified else "reuse"
    trace["final_image_paths"] = final_paths
    trace["final_image_path"] = final_paths[0] if final_paths else None
    trace["image_metas"] = metas
    trace["duration_seconds"] = round(time.perf_counter() - started, 3)
    return trace
