# -*- coding: utf-8 -*-
"""
阶段 5 (Step 5 · 质量总开关)：零背景「普通网友第一视角盲测」模块 (`step5_blind_review.py`)

核心物理隔离铁律：
1. `review_single_creation_blind()` 在函数签名层严格只接收最终 `post_text` 与最终处理后的配图路径 `final_image_paths`；
   物理上绝不传入任何原帖全文、Step 2 摘要、Step 3 选题描述或内部上下文，100% 还原陌生网友刷到该帖的真实阅读处境。
2. 三态闭环流转（最多微调 1 次，绝不死循环）：
   - `pass`   -> 直接通过定稿 (`publish_status = "ready"`)；
   - `revise` -> 将读者的困惑与不适反馈连同原始事实素材交回 Step 4 执行最多 1 次定点打磨，随后再次盲测确认
                 （二次盲测 `pass` -> `"ready"`，否则 -> `"dropped"`）；
   - `drop`   -> 直接淘汰 (`publish_status = "dropped"`)。
"""
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from content_pipeline.llm_gateway import call_llm_json, render_prompt_template
from content_pipeline.pipeline_config import (
    ACTIVE_PROFILE,
    CONTENT_PROFILES,
    STEP4_CONFIG,
    STEP5_CONFIG,
)
from content_pipeline.pipeline_db import PipelineDB
from content_pipeline.pipeline_logger import get_pipeline_logger
from content_pipeline.step4_creation import revise_single_work


def _validate_step5_json(data: Dict[str, Any]) -> Optional[str]:
    takeaway = data.get("reader_takeaway")
    if not isinstance(takeaway, str) or not takeaway.strip():
        return "缺少非空字符串字段 `reader_takeaway`"
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
    language: str,
    prompt_file: str,
    step5_cfg: Dict[str, Any],
) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    """
    100% 上下文物理隔离的普通网友盲测调用：
    仅传入最终可见的 `post_text` 与 `final_image_paths`，绝不接触任何幕后素材。
    """
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

    slots = {
        "LANGUAGE_EXPECTATION": lang_desc,
        "POST_TEXT": str(post_text or "").strip(),
        "FINAL_IMAGES_MANIFEST": images_manifest,
    }

    rendered_prompt, prompt_meta = render_prompt_template(prompt_file, slots)
    parsed, llm_trace = call_llm_json(
        prompt=rendered_prompt,
        prompt_meta=prompt_meta,
        model=step5_cfg.get("model"),
        fallback_models=step5_cfg.get("fallback_models"),
        file_paths=valid_images,
        timeout=int(step5_cfg.get("timeout", 600)),
        preset_model_group=str(step5_cfg.get("preset_model_group") or "medium"),
        validator=_validate_step5_json,
    )
    if parsed is not None:
        parsed = {
            "reader_takeaway": str(parsed.get("reader_takeaway") or "").strip(),
            "verdict": str(parsed.get("verdict") or "").strip().lower(),
            "reader_feedback": [
                str(x).strip()
                for x in (parsed.get("reader_feedback") or [])
                if isinstance(x, str) and x.strip()
            ],
        }
    return parsed, llm_trace


def _process_single_work_gate(
    work_doc: Dict[str, Any],
    raw_map: Dict[str, Dict[str, Any]],
    digest_map: Dict[str, Dict[str, Any]],
    profile_cfg: Dict[str, Any],
    step4_cfg: Dict[str, Any],
    step5_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    """对单篇成品执行零背景读者盲测 -> 可选单次返修 -> 终审闭环。"""
    prompt_file = str(profile_cfg.get("step5_prompt_file") or "step5_cold_reader_review.md")
    language = str(profile_cfg.get("language") or work_doc.get("language") or "zh")
    max_revisions = max(0, min(1, int(step5_cfg.get("max_revisions", 1))))

    r1_parsed, r1_trace = review_single_creation_blind(
        post_text=str(work_doc.get("post_text") or ""),
        final_image_paths=work_doc.get("final_image_paths") or [],
        language=language,
        prompt_file=prompt_file,
        step5_cfg=step5_cfg,
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

    if v1 == "drop" or max_revisions < 1:
        work_doc["publish_status"] = "dropped"
        work_doc["blind_review"] = {
            "final_outcome": "dropped_direct" if v1 == "drop" else "dropped_unrevised",
            "round_1": {"result": r1_parsed, "llm_trace": r1_trace},
            "round_2": None,
        }
        return work_doc

    # v1 == "revise"：交回握有完整原始素材的主笔执行最多 1 次定点打磨
    rev_ok, work_doc = revise_single_work(
        work_doc=work_doc,
        blind_review_round1=r1_parsed,
        raw_map=raw_map,
        digest_map=digest_map,
        profile_cfg=profile_cfg,
        step4_cfg=step4_cfg,
    )
    if not rev_ok:
        work_doc["publish_status"] = "dropped"
        work_doc["blind_review"] = {
            "final_outcome": "dropped_revision_failed",
            "round_1": {"result": r1_parsed, "llm_trace": r1_trace},
            "round_2": None,
        }
        return work_doc

    # 对返修后的新成品执行最终零背景盲测确认
    r2_parsed, r2_trace = review_single_creation_blind(
        post_text=str(work_doc.get("post_text") or ""),
        final_image_paths=work_doc.get("final_image_paths") or [],
        language=language,
        prompt_file=prompt_file,
        step5_cfg=step5_cfg,
    )
    if r2_parsed is not None and r2_parsed["verdict"] == "pass":
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


def run_step5_blind_review(
    db: Optional[PipelineDB] = None,
    creations: Optional[List[Dict[str, Any]]] = None,
    profile_name: str = ACTIVE_PROFILE,
    step4_cfg: Optional[Dict[str, Any]] = None,
    step5_cfg: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    执行 Step 5 零背景普通网友盲测终审闭环。
    返回 `(step5_summary, reviewed_creations)`。
    """
    logger = get_pipeline_logger()
    db = db or PipelineDB()

    s4_cfg = dict(STEP4_CONFIG)
    if step4_cfg:
        s4_cfg.update(step4_cfg)

    s5_cfg = dict(STEP5_CONFIG)
    if step5_cfg:
        s5_cfg.update(step5_cfg)

    profile_cfg = CONTENT_PROFILES.get(profile_name) or CONTENT_PROFILES[ACTIVE_PROFILE]

    target_works = (
        [w for w in creations if w.get("status") == "ok"]
        if creations is not None
        else db.fetch_creations_pending_review(limit=10)
    )

    t0 = time.perf_counter()
    if not target_works:
        logger.info("[Step5/跳过] 当前没有待盲测的成品。")
        return (
            {
                "status": "skipped_empty",
                "elapsed_seconds": 0.0,
                "reviewed_count": 0,
                "ready_count": 0,
                "passed_direct_count": 0,
                "passed_after_revision_count": 0,
                "dropped_count": 0,
                "reviews_preview": [],
            },
            creations or [],
        )

    # 预加载关联原始素材（仅供出现 `revise` 时交回 Step 4 主笔使用，盲测函数本身完全不可见）
    all_post_keys: List[str] = []
    for w in target_works:
        for pk in w.get("source_post_keys") or []:
            if pk not in all_post_keys:
                all_post_keys.append(pk)
    raw_map = db.fetch_raw_posts_by_keys(all_post_keys)
    digest_map = db.fetch_digests_by_keys(all_post_keys)

    model_label = s5_cfg.get("model") or f"group:{s5_cfg.get('preset_model_group', 'medium')}"
    logger.info(
        f"[Step5/启动] 待盲测成品={len(target_works)}篇 | 物理上下文隔离=100% | "
        f"最大返修轮数={s5_cfg.get('max_revisions', 1)} | 路由={model_label}"
    )

    order_index = {w["creation_id"]: idx for idx, w in enumerate(target_works)}
    reviewed_ok: List[Dict[str, Any]] = []
    workers = max(1, min(int(s5_cfg.get("concurrency", 3)), len(target_works)))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        fut_map = {
            pool.submit(
                _process_single_work_gate,
                work,
                raw_map,
                digest_map,
                profile_cfg,
                s4_cfg,
                s5_cfg,
            ): work
            for work in target_works
        }
        for fut in as_completed(fut_map):
            reviewed_ok.append(fut.result())

    reviewed_ok.sort(key=lambda w: order_index.get(w["creation_id"], 99))
    db.upsert_creations(reviewed_ok)

    elapsed = round(time.perf_counter() - t0, 2)
    passed_direct = [w for w in reviewed_ok if (w.get("blind_review") or {}).get("final_outcome") == "passed_direct"]
    passed_rev = [w for w in reviewed_ok if (w.get("blind_review") or {}).get("final_outcome") == "passed_after_revision"]
    ready_works = [w for w in reviewed_ok if w.get("publish_status") == "ready"]
    dropped_works = [w for w in reviewed_ok if w.get("publish_status") != "ready"]

    reviews_preview: List[Dict[str, Any]] = []
    for w in reviewed_ok:
        br = w.get("blind_review") or {}
        r1 = (br.get("round_1") or {}).get("result") or {}
        r2 = (br.get("round_2") or {}).get("result") or {}
        reviews_preview.append(
            {
                "creation_id": w.get("creation_id"),
                "topic_summary": w.get("topic_summary"),
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
        "status": "ok",
        "elapsed_seconds": elapsed,
        "reviewed_count": len(reviewed_ok),
        "ready_count": len(ready_works),
        "passed_direct_count": len(passed_direct),
        "passed_after_revision_count": len(passed_rev),
        "dropped_count": len(dropped_works),
        "reviews_preview": reviews_preview,
    }

    logger.info(
        f"[Step5/完成] 耗时={elapsed:.2f}s | 盲测={len(reviewed_ok)}篇 -> 准予发布(ready)={len(ready_works)}篇 "
        f"(一次直通:{len(passed_direct)}, 返修后通过:{len(passed_rev)}) | 淘汰(dropped)={len(dropped_works)}篇"
    )
    return summary, reviewed_ok


if __name__ == "__main__":
    s5_res, works_reviewed = run_step5_blind_review()
    for item in s5_res.get("reviews_preview", []):
        print(
            f"\n=== [{item['creation_id']}] outcome={item['final_outcome']} -> status={item['publish_status']} ==="
        )
        print(f"首轮盲测 ({item['round1_verdict']}): {item['round1_takeaway']}")
        if item["round1_feedback"]:
            print(f"网友反馈: {item['round1_feedback']}")
        if item["round2_verdict"]:
            print(f"返修再测 ({item['round2_verdict']}): {item['round2_takeaway']}")
