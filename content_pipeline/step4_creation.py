# -*- coding: utf-8 -*-
"""
Module 3 (Step 4)：X 平台原生推文创作与真实原图编辑生产模块 (`step4_creation.py`)

本质职责：
1. 围绕 Step 3 选定的话题（1~N 条原帖原文 + Step 2 图文还原摘要 + 高价值评论），生成零废话、图文互补的 X 原生推文 (`post_text`)；
2. 仅将 Step 2 体检合格 (`usable_as_visual=True`) 的真实主帖原图按 1:1 附件顺序喂给多模态模型看图决策，
   驱动本地 `visual_studio.py` 执行确定性的 `reuse`（复用）/ `crop`（去杂边水印裁剪）/ `stitch`（多图拼接）/ `none`（纯文本推文）。

可独立运行调试：直接在 IDE 运行本文件，即可消费库中 `pending_creation` 状态的选题并产出成品至 `pipeline_creations`。
"""
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from PIL import Image

from content_pipeline.llm_gateway import call_llm_json, render_prompt_template
from content_pipeline.pipeline_config import (
    ACTIVE_PROFILE,
    CONTENT_PROFILES,
    STEP4_CONFIG,
)
from content_pipeline.pipeline_db import PipelineDB
from content_pipeline.pipeline_logger import get_pipeline_logger
from content_pipeline.step2_comprehension import strip_inline_media_placeholders
from content_pipeline.visual_studio import produce_visual_asset


def _validate_step4_json(data: Dict[str, Any]) -> Optional[str]:
    post_text = data.get("post_text")
    if not isinstance(post_text, str) or not post_text.strip():
        return "缺少非空字符串字段 `post_text`"
    vp = data.get("visual_plan")
    if not isinstance(vp, dict):
        return "缺少对象字段 `visual_plan`"
    action = str(vp.get("action") or "").strip().lower()
    if action not in ("none", "reuse", "crop", "stitch"):
        return f"`visual_plan.action` 必须是 none/reuse/crop/stitch 之一，当前为 {action!r}"
    if action != "none" and not isinstance(vp.get("items"), list):
        return f"当 action={action!r} 时，`visual_plan.items` 必须是数组"
    return None


def _assemble_topic_creation_bundle(
    topic: Dict[str, Any],
    raw_map: Dict[str, Dict[str, Any]],
    digest_map: Dict[str, Dict[str, Any]],
    max_candidate_images: int = 4,
) -> Tuple[Dict[str, str], Dict[str, Dict[str, Any]], List[str]]:
    """
    组装零冗余的写作与视觉决策素材包：
    - 不传 Step 3 的 `why_selected` 或任何提纲指令，不传原帖浏览/赞数；
    - 不重复堆砌原始水评（仅传 Step 2 提炼出的 `valuable_comments`）；
    - 候选图片严格限定为 `usable_as_visual=True` 的主帖真实原图，且 `IMG_1..IMG_k` 与物理上传的 `file_paths` 100% 顺序一致。
    """
    topic_label = str(topic.get("topic_summary") or topic.get("topic_title") or "").strip()
    topic_info = f"- 核心事件/看点概述: {topic_label}"

    source_blocks: List[str] = []
    candidate_image_map: Dict[str, Dict[str, Any]] = {}
    image_manifest_lines: List[str] = []
    attached_image_files: List[str] = []
    seen_img_paths = set()

    for idx, pk in enumerate(topic.get("post_keys") or [], 1):
        raw_p = raw_map.get(pk) or {}
        dig = digest_map.get(pk) or {}
        content_obj = raw_p.get("content") or {}

        raw_title = str(content_obj.get("title") or "").strip()
        raw_body = strip_inline_media_placeholders(str(content_obj.get("text_content") or ""))
        clean_raw_text = f"{raw_title}\n{raw_body}".strip() if (raw_title and raw_title not in raw_body) else raw_body

        dig_summary = str(dig.get("summary") or dig.get("complete_summary") or "").strip()
        val_comments = dig.get("valuable_comments") or dig.get("comment_essence") or []

        block = [f"### 素材源 #{idx} (来源平台: {raw_p.get('source') or dig.get('source') or 'unknown'})"]
        if dig_summary:
            block.append(f"- 图文完整事实还原: {dig_summary}")
        if clean_raw_text:
            block.append(f"- 原帖原文:\n{clean_raw_text[:2000]}")
        if val_comments:
            block.append("- 评论区关键补充/神吐槽:\n  - " + "\n  - ".join(str(c) for c in val_comments))
        source_blocks.append("\n".join(block))

        # 收集通过视觉体检的主帖真实原图，保证每一张列进清单的 IMG_k 都真实作为多模态附件上传给模型查看
        for img_item in (dig.get("images") or []):
            if len(attached_image_files) >= max_candidate_images:
                break
            if not isinstance(img_item, dict) or not img_item.get("usable_as_visual", True):
                continue
            lp = str(img_item.get("local_path") or "").strip()
            if not lp or lp in seen_img_paths or not Path(lp).is_file():
                continue
            try:
                with Image.open(lp) as im_probe:
                    w, h = im_probe.size
                if min(w, h) < 120 or max(w, h) < 240:
                    continue
            except Exception:
                continue

            seen_img_paths.add(lp)
            attached_image_files.append(lp)
            img_id = f"IMG_{len(attached_image_files)}"
            has_ui = bool(img_item.get("has_ui_or_watermark", False))
            img_desc = str(img_item.get("desc") or "").strip()

            candidate_image_map[img_id] = {
                "image_id": img_id,
                "from_post_key": pk,
                "local_path": lp,
                "width": w,
                "height": h,
                "desc": img_desc,
                "has_ui_or_watermark": has_ui,
            }
            ui_hint = "含软件顶底栏/昵称/边角水印（建议用 crop 裁出核心区）" if has_ui else "画面较干净（可直接 reuse 或按需微裁）"
            image_manifest_lines.append(
                f"- `{img_id}` (对应第 {len(attached_image_files)} 张上传附件 | 分辨率 {w}x{h} | 状态: {ui_hint}): {img_desc}"
            )

    images_manifest_str = (
        "\n".join(image_manifest_lines)
        if image_manifest_lines
        else "（本话题无可用的真实原帖配图，`visual_plan.action` 必须填 `\"none\"`，`items` 填 `[]`）"
    )

    slots = {
        "TOPIC_INFO": topic_info,
        "SOURCE_MATERIALS": "\n\n".join(source_blocks),
        "AVAILABLE_IMAGES_MANIFEST": images_manifest_str,
    }
    return slots, candidate_image_map, attached_image_files


def create_single_work(
    topic: Dict[str, Any],
    raw_map: Dict[str, Dict[str, Any]],
    digest_map: Dict[str, Dict[str, Any]],
    profile_name: str,
    profile_cfg: Dict[str, Any],
    step4_cfg: Dict[str, Any],
    run_id: str,
) -> Dict[str, Any]:
    """围绕单个精选话题生产一篇 X 原生推文与真实像素配图。"""
    creation_id = f"work_{int(time.time())}_{uuid4().hex[:8]}"
    slots, candidate_image_map, preview_files = _assemble_topic_creation_bundle(
        topic=topic,
        raw_map=raw_map,
        digest_map=digest_map,
        max_candidate_images=int(step4_cfg.get("max_candidate_images_per_topic", 4)),
    )
    slots["STYLE_GUIDELINES"] = profile_cfg.get("style_guidelines", "")

    rendered_prompt, prompt_meta = render_prompt_template(profile_cfg["step4_prompt_file"], slots)
    parsed, llm_trace = call_llm_json(
        prompt=rendered_prompt,
        prompt_meta=prompt_meta,
        model=step4_cfg.get("model"),
        fallback_models=step4_cfg.get("fallback_models"),
        file_paths=preview_files,
        timeout=int(step4_cfg.get("timeout", 600)),
        preset_model_group=str(step4_cfg.get("preset_model_group") or "medium"),
        validator=_validate_step4_json,
    )

    now_ts = int(time.time())
    topic_summary = str(topic.get("topic_summary") or topic.get("topic_title") or "").strip()

    base_doc: Dict[str, Any] = {
        "creation_id": creation_id,
        "run_id": run_id,
        "topic_id": topic.get("topic_id"),
        "topic_summary": topic_summary,
        "topic_mode": topic.get("mode"),
        "profile_name": profile_name,
        "language": profile_cfg.get("language", "zh"),
        "source_post_keys": list(topic.get("post_keys") or []),
        "created_at_ts": now_ts,
        "publish_status": "ready",
        "llm_trace": llm_trace,
    }

    if parsed is None:
        base_doc.update(
            {
                "status": "failed",
                "error": llm_trace.get("parse_error") or "主笔模型调用失败",
                "post_text": "",
                "char_count": 0,
                "visual_plan": {},
                "visual_trace": {},
                "final_image_paths": [],
                "final_image_path": None,
            }
        )
        return base_doc

    post_text = str(parsed.get("post_text") or "").strip()
    visual_plan = parsed.get("visual_plan") if isinstance(parsed.get("visual_plan"), dict) else {"action": "none", "items": []}

    visual_trace: Dict[str, Any] = {
        "requested_action": visual_plan.get("action"),
        "executed_action": "none",
        "final_image_paths": [],
        "final_image_path": None,
    }
    if step4_cfg.get("enable_visual_studio", True) and post_text:
        visual_trace = produce_visual_asset(
            creation_id=creation_id,
            visual_plan=visual_plan,
            candidate_image_map=candidate_image_map,
        )

    final_image_paths = list(visual_trace.get("final_image_paths") or [])
    final_image_path = visual_trace.get("final_image_path")

    base_doc.update(
        {
            "status": "ok" if post_text else "failed",
            "post_text": post_text,
            "char_count": len(post_text),
            "visual_plan": visual_plan,
            "visual_trace": visual_trace,
            "final_image_paths": final_image_paths,
            "final_image_path": final_image_path,
            "candidate_images_count": len(candidate_image_map),
        }
    )
    return base_doc


def run_step4_creation(
    db: Optional[PipelineDB] = None,
    topics: Optional[List[Dict[str, Any]]] = None,
    profile_name: str = ACTIVE_PROFILE,
    step4_cfg: Optional[Dict[str, Any]] = None,
    run_id: str = "manual_step4",
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    执行 Step 4 原生推文与真实配图批量生产。
    返回 `(step4_summary, creation_records)`。
    """
    logger = get_pipeline_logger()
    db = db or PipelineDB()
    cfg = dict(STEP4_CONFIG)
    if step4_cfg:
        cfg.update(step4_cfg)

    profile_cfg = CONTENT_PROFILES.get(profile_name) or CONTENT_PROFILES[ACTIVE_PROFILE]
    max_creations = int(cfg.get("max_creations_per_run", 5))

    target_topics = (
        list(topics)[:max_creations]
        if topics is not None
        else db.fetch_pending_topics(limit=max_creations)
    )

    t0 = time.perf_counter()
    if not target_topics:
        logger.info("[Step4/跳过] 当前没有待创作的选题。")
        return (
            {
                "status": "skipped_empty",
                "elapsed_seconds": 0.0,
                "target_count": 0,
                "created_count": 0,
                "failed_count": 0,
                "creations_preview": [],
            },
            [],
        )

    all_post_keys: List[str] = []
    for t in target_topics:
        for pk in t.get("post_keys") or []:
            if pk not in all_post_keys:
                all_post_keys.append(pk)

    raw_map = db.fetch_raw_posts_by_keys(all_post_keys)
    digest_map = db.fetch_digests_by_keys(all_post_keys)

    model_label = cfg.get("model") or f"group:{cfg.get('preset_model_group', 'medium')}"
    logger.info(
        f"[Step4/启动] 待生产选题={len(target_topics)}个 | Profile={profile_name} | "
        f"路由={model_label} | 真实视觉编辑={'开启' if cfg.get('enable_visual_studio', True) else '关闭'}"
    )

    creations: List[Dict[str, Any]] = []
    workers = max(1, min(int(cfg.get("concurrency", 2)), len(target_topics)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        fut_map = {
            pool.submit(
                create_single_work,
                topic,
                raw_map,
                digest_map,
                profile_name,
                profile_cfg,
                cfg,
                run_id,
            ): topic
            for topic in target_topics
        }
        for fut in as_completed(fut_map):
            topic_item = fut_map[fut]
            work_doc = fut.result()
            creations.append(work_doc)
            if work_doc.get("status") == "ok":
                topic_item["status"] = "completed"
                topic_item["creation_id"] = work_doc["creation_id"]
            else:
                topic_item["status"] = "creation_failed"

    rank_order = {t["topic_id"]: t.get("rank", 99) for t in target_topics}
    creations.sort(key=lambda c: rank_order.get(c.get("topic_id"), 99))

    db.upsert_creations(creations)
    db.upsert_topics(target_topics)

    elapsed = round(time.perf_counter() - t0, 2)
    ok_works = [c for c in creations if c.get("status") == "ok"]
    failed_works = [c for c in creations if c.get("status") != "ok"]

    summary = {
        "status": "ok" if ok_works else "failed",
        "elapsed_seconds": elapsed,
        "target_count": len(target_topics),
        "created_count": len(ok_works),
        "failed_count": len(failed_works),
        "creations_preview": [
            {
                "creation_id": c["creation_id"],
                "topic_id": c.get("topic_id"),
                "topic_summary": c.get("topic_summary"),
                "char_count": c.get("char_count", 0),
                "first_line": (c.get("post_text") or "").splitlines()[0][:80] if c.get("post_text") else "",
                "text_model": (c.get("llm_trace") or {}).get("model_used"),
                "visual_action": (c.get("visual_trace") or {}).get("executed_action"),
                "visual_notes": (c.get("visual_trace") or {}).get("operation_notes"),
                "final_image_paths": c.get("final_image_paths") or [],
                "final_image_path": c.get("final_image_path"),
                "source_post_keys": c.get("source_post_keys"),
            }
            for c in ok_works
        ],
    }

    logger.info(
        f"[Step4/完成] 耗时={elapsed:.2f}s | 成功产出={len(ok_works)}/{len(target_topics)}篇 | "
        + " ; ".join(
            f"[{c.get('char_count')}字 | 视觉:{(c.get('visual_trace') or {}).get('executed_action')}] "
            f"{((c.get('post_text') or '').splitlines()[0][:36])}"
            for c in ok_works
        )
    )
    return summary, creations


if __name__ == "__main__":
    s4_res, works_out = run_step4_creation(run_id="standalone_step4_verify")
    for w in works_out:
        print(f"\n================ [{w.get('creation_id')}] ({w.get('status')} | {w.get('char_count')}字) ================")
        print(f"选题概述: {w.get('topic_summary')}")
        print(f"关联原帖: {w.get('source_post_keys')}")
        print(f"视觉动作: {(w.get('visual_trace') or {}).get('executed_action')} -> {w.get('final_image_paths')}")
        print("-" * 72)
        print(w.get("post_text"))
