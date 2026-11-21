# -*- coding: utf-8 -*-
"""
模块 7（可插拔质量总开关）：100% 上下文物理隔离的普通网友盲测模块 (`blind_reviewer.py`)

核心隔离准则：
1. `review_single_creation_blind()` 在函数签名层严格只接收 `post_text` 与 `final_image_paths`，
   物理上绝不接触任何原帖全文、理解档案、方向描述或内部上下文，100% 模拟陌生网友刷到该帖的第一阅读反应。
2. `review_and_polish_creation()` 可对任意单篇成稿执行「盲测 -> 可选最多 1 次定点打磨 -> 二次盲测确认」。
3. 精确区分 `dropped_count`（因内容质量被淘汰）与 `error_count`（因接口异常导致 `review_failed`），杜绝指标混淆。
"""
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from content_pipeline.content_creator import (
    assemble_creation_materials,
    revise_creation_with_feedback,
)
from content_pipeline.llm_gateway import call_llm_json, render_prompt_template
from content_pipeline.pipeline_config import (
    ACTIVE_PROFILE,
    CONTENT_PROFILES,
    CREATOR_CONFIG,
    REVIEWER_CONFIG,
)
from content_pipeline.pipeline_db import PipelineDB
from content_pipeline.pipeline_logger import get_pipeline_logger


def _validate_reviewer_json(data: Dict[str, Any]) -> Optional[str]:
    takeaway = data.get("reader_takeaway")
    if not isinstance(takeaway, str) or not takeaway.strip():
        return "缺少非空字符串字段 `reader_takeaway`"
    vis_check = data.get("visual_alignment_check")
    if not isinstance(vis_check, str) or not vis_check.strip():
        return "缺少非空字符串字段 `visual_alignment_check`"
    verdict = str(data.get("verdict") or "").strip().lower()
    if verdict not in ("pass", "revise", "drop"):
        return f"`verdict` 必须是 pass/revise/drop 之一，当前为 {verdict!r}"
    feedback = data.get("reader_feedback")
    if not isinstance(feedback, list):
        return "`reader_feedback` 必须是数组"
    if verdict == "revise" and not any(isinstance(x, str) and x.strip() for x in feedback):
        return "当 `verdict='revise'` 时，`reader_feedback` 必须包含具体读者反馈条目"
    return None


def review_single_creation_blind(
    post_text: str,
    final_image_paths: Sequence[str],
    language: str = "zh",
    prompt_file: str = "step5_cold_reader_review.md",
    reviewer_cfg: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    """
    100% 物理上下文隔离的纯盲测函数：仅传入最终可见的正文与配图。
    """
    cfg = dict(REVIEWER_CONFIG)
    if reviewer_cfg:
        cfg.update(reviewer_cfg)

    valid_images: List[str] = [
        str(Path(p).resolve())
        for p in (final_image_paths or [])
        if p and Path(p).is_file()
    ]

    if valid_images:
        manifest_lines = [
            f"- 最终配图 #{idx}: 对应第 {idx} 张上传图片附件"
            for idx in range(1, len(valid_images) + 1)
        ]
        images_manifest = "\n".join(manifest_lines)
    else:
        images_manifest = "（本帖为纯文字帖，无附带配图）"

    lang_desc = (
        "目标读者为英文社交平台普通用户，正文应为自然地道的英文。"
        if str(language).lower().startswith("en")
        else "目标读者为中文社交平台普通网友，正文应为自然地道的简体中文。"
    )

    now_date_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d (UTC)")
    slots = {
        "CURRENT_DATE_CONTEXT": now_date_str,
        "LANGUAGE_EXPECTATION": lang_desc,
        "POST_TEXT": str(post_text or "").strip(),
        "FINAL_IMAGES_MANIFEST": images_manifest,
    }

    rendered_prompt, prompt_meta = render_prompt_template(prompt_file, slots)
    parsed, llm_trace = call_llm_json(
        prompt=rendered_prompt,
        prompt_meta=prompt_meta,
        model=cfg.get("model"),
        fallback_models=cfg.get("fallback_models"),
        file_paths=valid_images,
        timeout=int(cfg.get("timeout", 600)),
        preset_model_group=str(cfg.get("preset_model_group") or "medium"),
        validator=_validate_reviewer_json,
    )
    if parsed is not None:
        parsed = {
            "reader_takeaway": str(parsed.get("reader_takeaway") or "").strip(),
            "visual_alignment_check": str(parsed.get("visual_alignment_check") or "").strip(),
            "verdict": str(parsed.get("verdict") or "").strip().lower(),
            "reader_feedback": [
                str(x).strip()
                for x in (parsed.get("reader_feedback") or [])
                if isinstance(x, str) and x.strip()
            ],
        }
    return parsed, llm_trace


def review_and_polish_creation(
    work_doc: Dict[str, Any],
    materials_bundle: Optional[Dict[str, Any]] = None,
    profile_name: str = ACTIVE_PROFILE,
    creator_cfg: Optional[Dict[str, Any]] = None,
    reviewer_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    模块 7 单篇独立入口：对单篇成品执行盲测；若返回 `revise` 且提供了 `materials_bundle`，执行最多 1 次定点打磨再盲测确认。
    """
    c_cfg = dict(CREATOR_CONFIG)
    if creator_cfg:
        c_cfg.update(creator_cfg)
    r_cfg = dict(REVIEWER_CONFIG)
    if reviewer_cfg:
        r_cfg.update(reviewer_cfg)

    profile_cfg = CONTENT_PROFILES.get(profile_name) or CONTENT_PROFILES[ACTIVE_PROFILE]
    prompt_file = str(profile_cfg.get("review_prompt_file") or profile_cfg.get("step5_prompt_file") or "step5_cold_reader_review.md")
    language = str(profile_cfg.get("language") or work_doc.get("language") or "zh")
    max_revisions = max(0, min(1, int(r_cfg.get("max_revisions", 1))))

    r1_parsed, r1_trace = review_single_creation_blind(
        post_text=str(work_doc.get("post_text") or ""),
        final_image_paths=work_doc.get("final_image_paths") or [],
        language=language,
        prompt_file=prompt_file,
        reviewer_cfg=r_cfg,
    )

    if r1_parsed is None:
        work_doc["publish_status"] = "review_failed"
        work_doc["blind_review"] = {
            "final_outcome": "review_failed",
            "round_1": {"result": None, "llm_trace": r1_trace},
            "round_2": None,
        }
        return work_doc

    v1 = r1_parsed["verdict"]
    if v1 == "pass":
        work_doc["publish_status"] = "ready"
        work_doc["blind_review"] = {
            "final_outcome": "passed_direct",
            "round_1": {"result": r1_parsed, "llm_trace": r1_trace},
            "round_2": None,
        }
        return work_doc

    if v1 == "drop" or max_revisions < 1 or materials_bundle is None:
        work_doc["publish_status"] = "dropped"
        work_doc["blind_review"] = {
            "final_outcome": "dropped_direct" if v1 == "drop" else "dropped_unrevised",
            "round_1": {"result": r1_parsed, "llm_trace": r1_trace},
            "round_2": None,
        }
        return work_doc

    rev_ok, work_doc = revise_creation_with_feedback(
        work_doc=work_doc,
        blind_review_round1=r1_parsed,
        materials_bundle=materials_bundle,
        profile_name=profile_name,
        creator_cfg=c_cfg,
    )
    if not rev_ok:
        work_doc["publish_status"] = "dropped"
        work_doc["blind_review"] = {
            "final_outcome": "dropped_revision_failed",
            "round_1": {"result": r1_parsed, "llm_trace": r1_trace},
            "round_2": None,
        }
        return work_doc

    r2_parsed, r2_trace = review_single_creation_blind(
        post_text=str(work_doc.get("post_text") or ""),
        final_image_paths=work_doc.get("final_image_paths") or [],
        language=language,
        prompt_file=prompt_file,
        reviewer_cfg=r_cfg,
    )
    if r2_parsed is None:
        work_doc["publish_status"] = "review_failed"
        final_outcome = "review_failed_round2"
    elif r2_parsed["verdict"] == "pass":
        work_doc["publish_status"] = "ready"
        final_outcome = "passed_after_revision"
    else:
        work_doc["publish_status"] = "dropped"
        final_outcome = "dropped_after_revision"

    work_doc["blind_review"] = {
        "final_outcome": final_outcome,
        "round_1": {"result": r1_parsed, "llm_trace": r1_trace},
        "round_2": {"result": r2_parsed, "llm_trace": r2_trace},
    }
    return work_doc


def run_blind_review_batch(
    db: Optional[PipelineDB] = None,
    creations: Optional[List[Dict[str, Any]]] = None,
    profile_name: str = ACTIVE_PROFILE,
    creator_cfg: Optional[Dict[str, Any]] = None,
    reviewer_cfg: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """批量执行零背景普通网友盲测终审闭环并持久化。"""
    logger = get_pipeline_logger()
    db = db or PipelineDB()

    c_cfg = dict(CREATOR_CONFIG)
    if creator_cfg:
        c_cfg.update(creator_cfg)
    r_cfg = dict(REVIEWER_CONFIG)
    if reviewer_cfg:
        r_cfg.update(reviewer_cfg)

    target_works = (
        [w for w in creations if w.get("status") == "ok"]
        if creations is not None
        else db.fetch_creations_pending_review(limit=10)
    )

    t0 = time.perf_counter()
    if not target_works:
        logger.info("[Module7/盲测跳过] 当前没有待盲测的成品。")
        return (
            {
                "status": "skipped_empty",
                "elapsed_seconds": 0.0,
                "reviewed_count": 0,
                "ready_count": 0,
                "passed_direct_count": 0,
                "passed_after_revision_count": 0,
                "dropped_count": 0,
                "error_count": 0,
                "reviews_preview": [],
            },
            creations or [],
        )

    all_post_keys: List[str] = []
    for w in target_works:
        for pk in w.get("source_post_keys") or []:
            if pk not in all_post_keys:
                all_post_keys.append(pk)
    raw_map = db.fetch_raw_posts_by_keys(all_post_keys)
    digest_map = db.fetch_digests_by_keys(all_post_keys)
    max_raw_cmts = int(c_cfg.get("max_raw_comments_per_post", 15))

    model_label = r_cfg.get("model") or f"group:{r_cfg.get('preset_model_group', 'medium')}"
    logger.info(
        f"[Module7/盲测启动] 待盲测成品={len(target_works)}篇 | 上下文物理隔离=100% | "
        f"最大返修轮数={r_cfg.get('max_revisions', 1)} | 路由={model_label}"
    )

    def _review_one(w_doc: Dict[str, Any]) -> Dict[str, Any]:
        bundle = assemble_creation_materials(
            post_keys=w_doc.get("source_post_keys") or [],
            db=db,
            direction_hint=str(w_doc.get("direction_summary") or w_doc.get("topic_summary") or ""),
            raw_map=raw_map,
            digest_map=digest_map,
            max_raw_comments_per_post=max_raw_cmts,
        )
        return review_and_polish_creation(
            work_doc=w_doc,
            materials_bundle=bundle,
            profile_name=profile_name,
            creator_cfg=c_cfg,
            reviewer_cfg=r_cfg,
        )

    order_index = {w["creation_id"]: idx for idx, w in enumerate(target_works)}
    reviewed_ok: List[Dict[str, Any]] = []
    workers = max(1, min(int(r_cfg.get("concurrency", 3)), len(target_works)))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = [pool.submit(_review_one, w) for w in target_works]
        for fut in as_completed(futs):
            reviewed_ok.append(fut.result())

    reviewed_ok.sort(key=lambda w: order_index.get(w["creation_id"], 99))
    db.upsert_creations(reviewed_ok)

    elapsed = round(time.perf_counter() - t0, 2)
    passed_direct = [w for w in reviewed_ok if (w.get("blind_review") or {}).get("final_outcome") == "passed_direct"]
    passed_rev = [w for w in reviewed_ok if (w.get("blind_review") or {}).get("final_outcome") == "passed_after_revision"]
    ready_works = [w for w in reviewed_ok if w.get("publish_status") == "ready"]
    dropped_works = [w for w in reviewed_ok if w.get("publish_status") == "dropped"]
    error_works = [w for w in reviewed_ok if w.get("publish_status") == "review_failed"]

    reviews_preview: List[Dict[str, Any]] = []
    for w in reviewed_ok:
        br = w.get("blind_review") or {}
        r1 = (br.get("round_1") or {}).get("result") or {}
        r2 = (br.get("round_2") or {}).get("result") or {}
        reviews_preview.append(
            {
                "creation_id": w.get("creation_id"),
                "direction_summary": w.get("direction_summary") or w.get("topic_summary"),
                "publish_status": w.get("publish_status"),
                "final_outcome": br.get("final_outcome"),
                "revision_count": w.get("revision_count", 0),
                "round1_verdict": r1.get("verdict"),
                "round1_takeaway": r1.get("reader_takeaway"),
                "round1_feedback": r1.get("reader_feedback") or [],
                "round1_model": ((br.get("round_1") or {}).get("llm_trace") or {}).get("model_used"),
                "round2_verdict": r2.get("verdict") if r2 else None,
                "round2_takeaway": r2.get("reader_takeaway") if r2 else None,
                "round2_feedback": r2.get("reader_feedback") if r2 else [],
                "round2_model": ((br.get("round_2") or {}).get("llm_trace") or {}).get("model_used"),
                "final_char_count": w.get("char_count", 0),
                "final_first_line": (w.get("post_text") or "").splitlines()[0][:80] if w.get("post_text") else "",
                "final_image_paths": w.get("final_image_paths") or [],
            }
        )

    summary = {
        "status": "ok" if not error_works or ready_works or dropped_works else "failed",
        "elapsed_seconds": elapsed,
        "reviewed_count": len(reviewed_ok),
        "ready_count": len(ready_works),
        "passed_direct_count": len(passed_direct),
        "passed_after_revision_count": len(passed_rev),
        "dropped_count": len(dropped_works),
        "error_count": len(error_works),
        "reviews_preview": reviews_preview,
    }

    logger.info(
        f"[Module7/盲测完成] 耗时={elapsed:.2f}s | 盲测={len(reviewed_ok)}篇 -> 准予发布(ready)={len(ready_works)}篇 "
        f"(一次直通:{len(passed_direct)}, 返修后通过:{len(passed_rev)}) | 质量淘汰(dropped)={len(dropped_works)}篇 "
        f"| 接口异常(review_failed)={len(error_works)}篇"
    )
    return summary, reviewed_ok
