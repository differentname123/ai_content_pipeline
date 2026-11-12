# -*- coding: utf-8 -*-
"""
阶段 4 (Step 4)：原生创作与确定性原图编辑模块 (`step4_creation.py`)

唯一固定职责：
1. 基于话题关联的全部原始文本素材、单帖还原档案与全部主帖原图（不截断候选图张数、不设像素门槛），
   生成零背景读者自洽、语感自然的发布正文 (`post_text`)；
2. 驱动本地 `visual_studio.py` 对选用的真实原图执行确定性编辑（`reuse` / `crop` / `stitch` / 小角标消除 / `none`）；
3. 支持在 Step 5 零背景读者盲测给出 `revise` 反馈时，结合原始事实素材执行最多 1 次定点打磨 (`revise_single_work`)。
"""
import sys
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from uuid import uuid4

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from PIL import Image

from content_pipeline.llm_gateway import call_llm_json, render_prompt_template
from content_pipeline.pipeline_config import (
    ACTIVE_PROFILE,
    CONTENT_PROFILES,
    STAGE_SWITCHES,
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


def assemble_topic_creation_bundle(
    post_keys: Sequence[str],
    topic_summary: str,
    raw_map: Dict[str, Dict[str, Any]],
    digest_map: Dict[str, Dict[str, Any]],
) -> Tuple[Dict[str, str], Dict[str, Dict[str, Any]], List[str]]:
    """
    组装写作与视觉决策素材包：
    - 不传 Step 3 的 `why_selected` 或任何写作提纲，不传原帖浏览/点赞数；
    - 挂载关联帖子的【全部】本地主帖原图（`IMG_1 .. IMG_N` 与物理附件列表 1:1 顺序对齐，不截断张数、不设像素门槛）。
    """
    topic_info = f"- 核心事件/议题概述: {str(topic_summary or '').strip()}"

    source_blocks: List[str] = []
    candidate_image_map: Dict[str, Dict[str, Any]] = {}
    image_manifest_lines: List[str] = []
    attached_image_files: List[str] = []
    seen_img_paths = set()

    for idx, pk in enumerate(post_keys or [], 1):
        raw_p = raw_map.get(pk) or {}
        dig = digest_map.get(pk) or {}
        content_obj = raw_p.get("content") or {}

        raw_title = str(content_obj.get("title") or "").strip()
        raw_body = strip_inline_media_placeholders(str(content_obj.get("text_content") or ""))
        clean_raw_text = f"{raw_title}\n{raw_body}".strip() if (raw_title and raw_title not in raw_body) else raw_body

        dig_summary = str(dig.get("summary") or "").strip()
        val_comments = dig.get("valuable_comments") or []

        block = [f"### 素材源 #{idx} (来源平台: {raw_p.get('source') or dig.get('source') or 'unknown'})"]
        if dig_summary:
            block.append(f"- 图文完整事实还原: {dig_summary}")
        if clean_raw_text:
            block.append(f"- 原帖原文:\n{clean_raw_text}")
        if val_comments:
            block.append("- 评论区关键补充:\n  - " + "\n  - ".join(str(c) for c in val_comments))
        source_blocks.append("\n".join(block))

        # 收集该帖的全部本地主帖原图（若 digest.images 为空则回退读取 digest.local_images）
        img_entries = dig.get("images") or dig.get("local_images") or []
        for img_item in img_entries:
            if not isinstance(img_item, dict):
                continue
            lp = str(img_item.get("local_path") or "").strip()
            if not lp or lp in seen_img_paths or not Path(lp).is_file():
                continue
            try:
                with Image.open(lp) as im_probe:
                    w, h = im_probe.size
                if w < 2 or h < 2:
                    continue
            except Exception:
                continue

            seen_img_paths.add(lp)
            attached_image_files.append(lp)
            img_id = f"IMG_{len(attached_image_files)}"
            has_ui = bool(img_item.get("has_ui_or_watermark", False))
            img_desc = str(img_item.get("desc") or "原帖配图").strip()

            candidate_image_map[img_id] = {
                "image_id": img_id,
                "from_post_key": pk,
                "local_path": lp,
                "width": w,
                "height": h,
                "desc": img_desc,
                "has_ui_or_watermark": has_ui,
            }
            ui_hint = "四周带有软件界面栏或角标水印" if has_ui else "画面边缘较干净"
            image_manifest_lines.append(
                f"- `{img_id}` (对应第 {len(attached_image_files)} 张上传附件 | 尺寸 {w}x{h} | 边缘状态: {ui_hint}): {img_desc}"
            )

    images_manifest_str = (
        "\n".join(image_manifest_lines)
        if image_manifest_lines
        else "（本话题无关联原帖图片，`visual_plan.action` 请填 `\"none\"`，`items` 填 `[]`）"
    )

    slots = {
        "TOPIC_INFO": topic_info,
        "SOURCE_MATERIALS": "\n\n".join(source_blocks),
        "AVAILABLE_IMAGES_MANIFEST": images_manifest_str,
        "REVISION_CONTEXT": "",
    }
    return slots, candidate_image_map, attached_image_files


def _build_revision_context_block(
    language: str,
    prev_post_text: str,
    prev_visual_plan: Dict[str, Any],
    reader_takeaway: str,
    reader_feedback: Sequence[str],
) -> str:
    """当 Step 5 零背景读者盲测给出 `revise` 时，构造供主笔定点打磨的反馈区块。"""
    fb_lines = "\n".join(f"- {str(x).strip()}" for x in (reader_feedback or []) if str(x).strip())
    plan_json = json.dumps(prev_visual_plan or {}, ensure_ascii=False)

    if str(language).lower().startswith("en"):
        return (
            "\n## 4. Cold-Reader Blind Review Feedback (Targeted Revision Required)\n"
            "An ordinary reader with zero background read your previous draft and reported the following friction points:\n"
            f"- Reader's first-read takeaway: {reader_takeaway}\n"
            f"- Specific reader feedback:\n{fb_lines}\n"
            f"- Previous draft `post_text`:\n{prev_post_text}\n"
            f"- Previous draft `visual_plan`: {plan_json}\n\n"
            "Revision Mandate: Strictly grounded in the verified source materials above (never inventing unstated facts), "
            "resolve every reader confusion, unnatural phrasing, or visual selection/cropping flaw identified above, "
            "and output the improved complete JSON (`post_text` and `visual_plan`).\n"
        )

    return (
        "\n## 4. 零背景普通网友盲测反馈（请执行定点打磨）\n"
        "一位完全没看过原始素材的普通读者阅读了上一版初稿后，给出了如下真实阅读感受与卡壳点：\n"
        f"- 读者第一遍读懂的意思：{reader_takeaway}\n"
        f"- 读者指出的具体问题：\n{fb_lines}\n"
        f"- 上一版初稿正文 (`post_text`)：\n{prev_post_text}\n"
        f"- 上一版视觉方案 (`visual_plan`)：{plan_json}\n\n"
        "打磨要求：严格基于上方原始素材中的真实事实（绝不凭空编造未提及的信息），针对性补齐读者感到困惑的背景衔接、"
        "消除不自然的审稿腔或机器味、修正配图选用或裁剪瑕疵，输出打磨后的完整 JSON（`post_text` 与 `visual_plan`）。\n"
    )


def create_single_work(
    topic: Dict[str, Any],
    raw_map: Dict[str, Dict[str, Any]],
    digest_map: Dict[str, Dict[str, Any]],
    profile_name: str,
    profile_cfg: Dict[str, Any],
    step4_cfg: Dict[str, Any],
    run_id: str,
    await_blind_review: bool = True,
) -> Dict[str, Any]:
    """围绕单个话题生产初稿正文与真实像素编辑配图。"""
    creation_id = f"work_{int(time.time())}_{uuid4().hex[:8]}"
    topic_summary = str(topic.get("topic_summary") or topic.get("topic_title") or "").strip()
    post_keys = list(topic.get("post_keys") or [])

    slots, candidate_image_map, preview_files = assemble_topic_creation_bundle(
        post_keys=post_keys,
        topic_summary=topic_summary,
        raw_map=raw_map,
        digest_map=digest_map,
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
    base_doc: Dict[str, Any] = {
        "creation_id": creation_id,
        "run_id": run_id,
        "topic_id": topic.get("topic_id"),
        "topic_summary": topic_summary,
        "topic_mode": topic.get("mode"),
        "profile_name": profile_name,
        "language": profile_cfg.get("language", "zh"),
        "source_post_keys": post_keys,
        "created_at_ts": now_ts,
        "publish_status": "pending_review" if await_blind_review else "ready",
        "revision_count": 0,
        "llm_trace": llm_trace,
    }

    if parsed is None:
        base_doc.update(
            {
                "status": "failed",
                "publish_status": "failed",
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


def revise_single_work(
    work_doc: Dict[str, Any],
    blind_review_round1: Dict[str, Any],
    raw_map: Dict[str, Dict[str, Any]],
    digest_map: Dict[str, Dict[str, Any]],
    profile_cfg: Dict[str, Any],
    step4_cfg: Dict[str, Any],
) -> Tuple[bool, Dict[str, Any]]:
    """
    基于 Step 5 普通读者盲测反馈 `blind_review_round1`，结合原始事实素材与全部候选原图执行单次定点返修。
    保留 `work_doc["initial_draft"]` 快照以便全链路前后对比审计。
    """
    creation_id = str(work_doc.get("creation_id") or "")
    post_keys = list(work_doc.get("source_post_keys") or [])
    topic_summary = str(work_doc.get("topic_summary") or "")
    language = str(profile_cfg.get("language") or work_doc.get("language") or "zh")

    slots, candidate_image_map, preview_files = assemble_topic_creation_bundle(
        post_keys=post_keys,
        topic_summary=topic_summary,
        raw_map=raw_map,
        digest_map=digest_map,
    )
    slots["STYLE_GUIDELINES"] = profile_cfg.get("style_guidelines", "")
    slots["REVISION_CONTEXT"] = _build_revision_context_block(
        language=language,
        prev_post_text=str(work_doc.get("post_text") or ""),
        prev_visual_plan=work_doc.get("visual_plan") or {},
        reader_takeaway=str(blind_review_round1.get("reader_takeaway") or ""),
        reader_feedback=blind_review_round1.get("reader_feedback") or [],
    )

    rendered_prompt, prompt_meta = render_prompt_template(profile_cfg["step4_prompt_file"], slots)
    parsed, rev_llm_trace = call_llm_json(
        prompt=rendered_prompt,
        prompt_meta=prompt_meta,
        model=step4_cfg.get("model"),
        fallback_models=step4_cfg.get("fallback_models"),
        file_paths=preview_files,
        timeout=int(step4_cfg.get("timeout", 600)),
        preset_model_group=str(step4_cfg.get("preset_model_group") or "medium"),
        validator=_validate_step4_json,
    )

    work_doc["revision_llm_trace"] = rev_llm_trace
    if parsed is None:
        return False, work_doc

    new_post_text = str(parsed.get("post_text") or "").strip()
    if not new_post_text:
        return False, work_doc

    # 保存初稿完整快照供对比分析
    work_doc["initial_draft"] = {
        "post_text": work_doc.get("post_text"),
        "char_count": work_doc.get("char_count"),
        "visual_plan": work_doc.get("visual_plan"),
        "visual_trace": work_doc.get("visual_trace"),
        "final_image_paths": work_doc.get("final_image_paths"),
    }

    new_visual_plan = parsed.get("visual_plan") if isinstance(parsed.get("visual_plan"), dict) else {"action": "none", "items": []}
    new_visual_trace: Dict[str, Any] = {
        "requested_action": new_visual_plan.get("action"),
        "executed_action": "none",
        "final_image_paths": [],
        "final_image_path": None,
    }
    if step4_cfg.get("enable_visual_studio", True):
        new_visual_trace = produce_visual_asset(
            creation_id=f"{creation_id}_rev1",
            visual_plan=new_visual_plan,
            candidate_image_map=candidate_image_map,
        )

    final_image_paths = list(new_visual_trace.get("final_image_paths") or [])
    work_doc.update(
        {
            "post_text": new_post_text,
            "char_count": len(new_post_text),
            "visual_plan": new_visual_plan,
            "visual_trace": new_visual_trace,
            "final_image_paths": final_image_paths,
            "final_image_path": new_visual_trace.get("final_image_path"),
            "revision_count": int(work_doc.get("revision_count") or 0) + 1,
        }
    )
    return True, work_doc


def run_step4_creation(
    db: Optional[PipelineDB] = None,
    topics: Optional[List[Dict[str, Any]]] = None,
    profile_name: str = ACTIVE_PROFILE,
    step4_cfg: Optional[Dict[str, Any]] = None,
    run_id: str = "manual_step4",
    await_blind_review: Optional[bool] = None,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    执行 Step 4 原生创作与真实原图确定性编辑批量生产。
    返回 `(step4_summary, creation_records)`。
    """
    logger = get_pipeline_logger()
    db = db or PipelineDB()
    cfg = dict(STEP4_CONFIG)
    if step4_cfg:
        cfg.update(step4_cfg)

    if await_blind_review is None:
        await_blind_review = bool(STAGE_SWITCHES.get("step5_blind_review", True))

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
        f"[Step4/启动] 待创作选题={len(target_topics)}个 | Profile={profile_name} | "
        f"路由={model_label} | 真实原图编辑={'开启' if cfg.get('enable_visual_studio', True) else '关闭'}"
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
                await_blind_review,
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
                "candidate_images_count": c.get("candidate_images_count", 0),
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
        f"[Step4/完成] 耗时={elapsed:.2f}s | 初稿产出={len(ok_works)}/{len(target_topics)}篇 | "
        + " ; ".join(
            f"[候选图:{c.get('candidate_images_count', 0)}张 -> 视觉:{(c.get('visual_trace') or {}).get('executed_action')} | {c.get('char_count')}字] "
            f"{((c.get('post_text') or '').splitlines()[0][:36])}"
            for c in ok_works
        )
    )
    return summary, creations


if __name__ == "__main__":
    s4_res, works_out = run_step4_creation(run_id="standalone_step4_verify", await_blind_review=False)
    for w in works_out:
        print(f"\n================ [{w.get('creation_id')}] ({w.get('status')} | {w.get('char_count')}字) ================")
        print(f"选题概述: {w.get('topic_summary')}")
        print(f"关联原帖: {w.get('source_post_keys')}")
        print(f"视觉动作: {(w.get('visual_trace') or {}).get('executed_action')} -> {w.get('final_image_paths')}")
        print("-" * 72)
        print(w.get("post_text"))
