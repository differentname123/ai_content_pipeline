# -*- coding: utf-8 -*-
"""
Module 3 (Step 4)：原创高网感正文撰写与多模态视觉生产模块 (`step4_creation.py`)

本质职责：
围绕 Step 3 选定的爆款选题（1~N 条跨平台原帖 + 图文情报卡 + 网友神评），一次性完成：
1. 原创正文创作：融合多源事实与神评洞察，生成无搬运痕迹、开头抓人的高网感作品；
2. 智能视觉生产：结合候选原图视觉说明与实图预览，自动执行「直接复用干净原图 (`reuse`)」、
   「多模态 AI 去痕/重制改图 (`edit`)」或「多模态 AI 全新生图 (`generate`)」，输出图文双全的成品入库 (`pipeline_creations`)。

可独立运行调试：直接在 IDE 运行本文件，即可自动消费库中 `pending_creation` 状态的选题并生成图文成品。
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

from content_pipeline.llm_gateway import call_llm_json, render_prompt_template
from content_pipeline.pipeline_config import (
    ACTIVE_PROFILE,
    CONTENT_PROFILES,
    STEP4_CONFIG,
)
from content_pipeline.pipeline_db import PipelineDB
from content_pipeline.pipeline_logger import get_pipeline_logger
from content_pipeline.visual_studio import produce_visual_asset


def _assemble_topic_creation_bundle(
    topic: Dict[str, Any],
    raw_map: Dict[str, Dict[str, Any]],
    digest_map: Dict[str, Dict[str, Any]],
) -> Tuple[Dict[str, str], Dict[str, Dict[str, Any]], List[str]]:
    """
    组装单个选题的完整写作素材包、候选图片映射表 `IMG_1..IMG_k` 以及传给主笔模型的参考图片附件列表。
    """
    topic_info = (
        f"- 选题标题: {topic.get('topic_title')}\n"
        f"- 所属领域: {topic.get('domain')} | 素材模式: {topic.get('mode')}\n"
        f"- 总编入选理由: {topic.get('selection_reason')}\n"
        f"- 建议切入角度: {topic.get('creative_angle')}"
    )

    source_blocks: List[str] = []
    candidate_image_map: Dict[str, Dict[str, Any]] = {}
    image_manifest_lines: List[str] = []
    writer_preview_files: List[str] = []

    img_counter = 0
    for idx, pk in enumerate(topic.get("post_keys") or [], 1):
        raw_p = raw_map.get(pk) or {}
        dig = digest_map.get(pk) or {}
        eng = raw_p.get("engagement") or {}
        content_obj = raw_p.get("content") or {}

        raw_title = str(content_obj.get("title") or "").strip()
        raw_body = str(content_obj.get("text_content") or "").strip()
        full_raw_text = f"{raw_title}\n{raw_body}".strip() if raw_title and raw_title not in raw_body else raw_body

        # 汇总前排原始高赞评论 + Step 2 提炼的神评精华
        raw_comments = sorted(
            [c for c in (raw_p.get("comments") or []) if isinstance(c, dict)],
            key=lambda x: int(x.get("likes") or 0),
            reverse=True,
        )
        comm_snippets = [
            f"[{c.get('author_name') or '网友'} | 赞:{c.get('likes', 0)}] {str(c.get('content') or '').strip()[:220]}"
            for c in raw_comments[:5]
            if str(c.get("content") or "").strip()
        ]
        essence_list = dig.get("comment_essence") or []

        block = [
            f"### 素材帖 #{idx} ({pk})",
            f"- 互动热度: 浏览={eng.get('view_count', 0)}, 点赞={eng.get('like_count', 0)}, 评论={eng.get('comment_count', 0)}, 增速={eng.get('views_per_hour', 0)}浏览/h",
            f"- 多模态完整摘要: {dig.get('complete_summary') or '无'}",
            f"- 原始文本:\n{full_raw_text[:1500] or '（无正文文字）'}",
        ]
        if essence_list:
            block.append("- 神评提炼: " + " ； ".join(essence_list))
        if comm_snippets:
            block.append("- 评论区原声摘录:\n  " + "\n  ".join(comm_snippets))
        source_blocks.append("\n".join(block))

        # 整理该原帖下所有可用本地图片及 Step 2 产出的视觉体检信息
        img_descs = dig.get("image_descriptions") or []
        local_imgs = dig.get("local_images") or []
        for i_pos, loc_item in enumerate(local_imgs):
            lp = loc_item.get("local_path")
            if not lp or not Path(lp).is_file():
                continue
            img_counter += 1
            img_id = f"IMG_{img_counter}"
            matched_desc = img_descs[i_pos] if i_pos < len(img_descs) else {}
            v_fact = matched_desc.get("visual_fact") or "本地原帖配图"
            v_role = matched_desc.get("role") or "news_visual"
            has_wm = bool(matched_desc.get("has_watermark_or_ui", False))

            candidate_image_map[img_id] = {
                "image_id": img_id,
                "from_post_key": pk,
                "local_path": lp,
                "visual_fact": v_fact,
                "role": v_role,
                "has_watermark_or_ui": has_wm,
            }
            origin_tag = "主帖原图" if loc_item.get("origin") != "comment" else "热评配图"
            image_manifest_lines.append(
                f"- `{img_id}` (来自 {pk} [{origin_tag}] | 角色:{v_role} | 含水印或平台UI边框:{'是' if has_wm else '否'}): {v_fact}"
            )
            if len(writer_preview_files) < 3:
                writer_preview_files.append(lp)

    images_manifest_str = (
        "\n".join(image_manifest_lines)
        if image_manifest_lines
        else "（本选题关联原帖均无本地图片，visual_plan.action 请务必选择 `generate` 并编写高质量英文生图提示词）"
    )

    slots = {
        "TOPIC_INFO": topic_info,
        "SOURCE_MATERIALS": "\n\n".join(source_blocks),
        "AVAILABLE_IMAGES_MANIFEST": images_manifest_str,
    }
    return slots, candidate_image_map, writer_preview_files


def create_single_work(
    topic: Dict[str, Any],
    raw_map: Dict[str, Dict[str, Any]],
    digest_map: Dict[str, Dict[str, Any]],
    profile_name: str,
    profile_cfg: Dict[str, Any],
    step4_cfg: Dict[str, Any],
    run_id: str,
) -> Dict[str, Any]:
    """围绕单个精选话题生产一篇完整原创作品（文案 + 多模态视觉配图）。"""
    creation_id = f"work_{int(time.time())}_{uuid4().hex[:8]}"
    slots, candidate_image_map, preview_files = _assemble_topic_creation_bundle(
        topic=topic,
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
        timeout=int(step4_cfg.get("timeout", 210)),
    )

    now_ts = int(time.time())
    base_doc: Dict[str, Any] = {
        "creation_id": creation_id,
        "run_id": run_id,
        "topic_id": topic.get("topic_id"),
        "topic_title": topic.get("topic_title"),
        "domain": topic.get("domain"),
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
                "title": "",
                "body": "",
                "opening_hook": "",
                "closing_question": "",
                "visual_plan": {},
                "visual_trace": {},
                "final_image_path": None,
            }
        )
        return base_doc

    title = str(parsed.get("title") or topic.get("topic_title") or "").strip()
    body = str(parsed.get("body") or "").strip()
    opening_hook = str(parsed.get("opening_hook") or (body.splitlines()[0] if body else "")).strip()
    closing_question = str(parsed.get("closing_question") or "").strip()
    visual_plan = parsed.get("visual_plan") if isinstance(parsed.get("visual_plan"), dict) else {}

    known_keys = {"title", "body", "opening_hook", "closing_question", "visual_plan"}
    extra_fields = {k: v for k, v in parsed.items() if k not in known_keys}

    visual_trace: Dict[str, Any] = {}
    final_image_path: Optional[str] = None
    if step4_cfg.get("enable_visual_studio", True) and body:
        visual_trace = produce_visual_asset(
            creation_id=creation_id,
            visual_plan=visual_plan,
            candidate_image_map=candidate_image_map,
            fallback_topic_title=title or str(topic.get("topic_title") or ""),
            image_models=step4_cfg.get("image_models") or ["gemini-3.1-flash-image"],
            timeout=int(step4_cfg.get("image_timeout", 90)),
        )
        final_image_path = visual_trace.get("final_image_path")

    base_doc.update(
        {
            "status": "ok" if body else "failed",
            "title": title,
            "body": body,
            "body_length": len(body),
            "opening_hook": opening_hook,
            "closing_question": closing_question,
            "visual_plan": visual_plan,
            "visual_trace": visual_trace,
            "final_image_path": final_image_path,
            "candidate_images_count": len(candidate_image_map),
            "extra_fields": extra_fields,
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
    执行 Step 4 原创图文批量生产。
    若未显式传入 `topics`，则自动从数据库拉取 `pending_creation` 状态的待产选题。
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

    logger.info(
        f"[Step4/启动] 待创作选题={len(target_topics)}个 | Profile={profile_name} | "
        f"主笔模型={cfg.get('model')} | 视觉工作室={'开启' if cfg.get('enable_visual_studio', True) else '关闭'}"
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

    # 按原选题 rank 顺序整理输出
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
                "domain": c.get("domain"),
                "title": c.get("title"),
                "body_length": c.get("body_length", 0),
                "opening_hook": c.get("opening_hook"),
                "closing_question": c.get("closing_question"),
                "text_model": (c.get("llm_trace") or {}).get("model_used"),
                "visual_action": (c.get("visual_trace") or {}).get("executed_action"),
                "image_model": (c.get("visual_trace") or {}).get("image_model_used"),
                "final_image_path": c.get("final_image_path"),
                "source_post_keys": c.get("source_post_keys"),
            }
            for c in ok_works
        ],
    }

    logger.info(
        f"[Step4/完成] 耗时={elapsed:.2f}s | 成功产出={len(ok_works)}/{len(target_topics)}篇 | "
        + " ; ".join(
            f"《{c['title']}》(配图:{(c.get('visual_trace') or {}).get('executed_action')})"
            for c in ok_works
        )
    )
    return summary, creations


if __name__ == "__main__":
    s4_res, works_out = run_step4_creation(run_id="standalone_step4_verify")
    for w in works_out:
        print(f"\n================ 《{w.get('title')}》 ({w.get('status')}) ================")
        print(f"关联原帖: {w.get('source_post_keys')}")
        print(f"视觉配图: {(w.get('visual_trace') or {}).get('executed_action')} -> {w.get('final_image_path')}")
        print("-" * 72)
        print(w.get("body"))
