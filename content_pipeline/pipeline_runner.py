# -*- coding: utf-8 -*-
"""
模块化多模态内容生产全链路编排总控 (`pipeline_runner.py`)

================================================================================
【三种即插即用运行模式】
1. 全自动双环流水线（默认直接点击运行）：
   广谱单帖理解 -> 热点方向发现 -> 按方向关键词跨五平台「指哪打哪」深挖与理解 -> 方向素材精选去噪 -> 解耦原创图文创作 -> 零背景普通网友盲测终审
2. 人工指定方向「指哪打哪」定向生产：
   `execute_pipeline_round(manual_directions=[{"direction_summary": "...", "search_keywords": ["..."]}])`
   自动跳过广谱发现，直接去五大平台搜索该方向 -> 理解 -> 精选 -> 创作 -> 盲测
3. 纯素材直通解耦创作（跳过一切发现与搜索）：
   `execute_pipeline_round(direct_post_keys=["x:123", "hackernews:456"], direct_hint="...")`
   或直接调用 `content_creator.create_from_materials(...)`
================================================================================
"""
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence
from uuid import uuid4

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from content_pipeline.blind_reviewer import run_blind_review_batch
from content_pipeline.content_creator import run_content_creation_batch
from content_pipeline.direction_expander import (
    normalize_manual_or_radar_direction,
    run_direction_expansion_batch,
)
from content_pipeline.material_curator import run_material_curation_batch
from content_pipeline.pipeline_config import (
    ACTIVE_PROFILE,
    SCHEDULE_CONFIG,
    STAGE_SWITCHES,
)
from content_pipeline.pipeline_db import PipelineDB
from content_pipeline.pipeline_logger import (
    TZ_CN,
    format_pipeline_execution_report,
    get_pipeline_logger,
    save_run_audit_bundle,
)
from content_pipeline.step2_comprehension import (
    comprehend_posts_by_keys,
    run_step2_comprehension,
)
from content_pipeline.topic_radar import run_topic_radar
from platforms.harvester_runner import harvest_broad_feeds


def execute_pipeline_round(
    profile_name: str = ACTIVE_PROFILE,
    switches: Optional[Dict[str, bool]] = None,
    manual_directions: Optional[Sequence[Dict[str, Any]]] = None,
    direct_post_keys: Optional[Sequence[str]] = None,
    direct_hint: str = "",
    step2_override: Optional[Dict[str, Any]] = None,
    radar_override: Optional[Dict[str, Any]] = None,
    expander_override: Optional[Dict[str, Any]] = None,
    curator_override: Optional[Dict[str, Any]] = None,
    creator_override: Optional[Dict[str, Any]] = None,
    reviewer_override: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """执行完整一轮（或按开关/参数自由组合的模块化子链路）生产流程，并生成全链路审计档案。"""
    logger = get_pipeline_logger()
    db = PipelineDB()
    active_switches = dict(STAGE_SWITCHES)
    if switches:
        active_switches.update(switches)

    started_ts = int(time.time())
    t0 = time.perf_counter()
    run_id = f"run_{datetime.now(TZ_CN).strftime('%H%M%S')}_{uuid4().hex[:6]}"

    logger.info(
        f"▶ [模块化流水线启动] RunID={run_id} | Profile={profile_name} | 开关={active_switches}"
    )

    harvest_summary = None
    step2_summary = None
    newly_digested: List[Dict[str, Any]] = []
    radar_summary = None
    expansion_summary = None
    curation_summary = None
    creation_summary = None
    review_summary = None

    directions: List[Dict[str, Any]] = []
    digests_by_did: Dict[str, List[Dict[str, Any]]] = {}
    created_works: List[Dict[str, Any]] = []

    # --------------------------------------------------------------------------
    # 模式 C：直接给定一组 post_keys 直通创作（与上游发现/检索完全解耦）
    # --------------------------------------------------------------------------
    if direct_post_keys is not None:
        clean_direct_keys = [str(k).strip() for k in direct_post_keys if str(k).strip()]
        if active_switches.get("step2_comprehension", True) and clean_direct_keys:
            step2_summary, _ = comprehend_posts_by_keys(
                post_keys=clean_direct_keys,
                db=db,
                profile_name=profile_name,
                step2_cfg=step2_override,
                run_id=run_id,
            )
        directions = [
            {
                "direction_id": f"direct_{started_ts}_{uuid4().hex[:6]}",
                "run_id": run_id,
                "profile_name": profile_name,
                "rank": 1,
                "status": "pending_creation",
                "created_at_ts": started_ts,
                "direction_summary": direct_hint,
                "curated_post_keys": clean_direct_keys,
            }
        ]
    else:
        # ----------------------------------------------------------------------
        # 第一环：广谱采集与热点方向发现（当人工直接传入 manual_directions 时自动跳过）
        # ----------------------------------------------------------------------
        if manual_directions is not None:
            directions = [
                normalize_manual_or_radar_direction(d, rank=idx, profile_name=profile_name, run_id=run_id)
                for idx, d in enumerate(manual_directions, 1)
            ]
            logger.info(f"[人工指定方向模式] 跳过广谱发现，直接处理 {len(directions)} 个指定方向。")
        else:
            if active_switches.get("harvest_broad", False):
                harvest_summary = harvest_broad_feeds(round_no=1)

            if active_switches.get("step2_comprehension", True):
                step2_summary, newly_digested = run_step2_comprehension(
                    db=db,
                    profile_name=profile_name,
                    step2_cfg=step2_override,
                    run_id=run_id,
                )

            if active_switches.get("step3_discover_directions", True):
                radar_summary, directions = run_topic_radar(
                    db=db,
                    profile_name=profile_name,
                    radar_cfg=radar_override,
                    run_id=run_id,
                )

        # ----------------------------------------------------------------------
        # 第二环：按方向关键词跨平台「指哪打哪」深挖 + 新帖理解 + 素材精选去噪
        # ----------------------------------------------------------------------
        if directions and active_switches.get("step4_targeted_search", True):
            expansion_summary, directions, digests_by_did = run_direction_expansion_batch(
                directions=directions,
                db=db,
                profile_name=profile_name,
                expander_cfg=expander_override,
                step2_cfg=step2_override,
                run_id=run_id,
            )

        if directions and active_switches.get("step5_curate_materials", True):
            curation_summary, directions = run_material_curation_batch(
                directions=directions,
                digests_by_direction_id=digests_by_did if digests_by_did else None,
                db=db,
                profile_name=profile_name,
                curator_cfg=curator_override,
            )

    # --------------------------------------------------------------------------
    # 模块 6：解耦原创图文创作 + 真实原图确定性编辑
    # --------------------------------------------------------------------------
    enable_review = bool(active_switches.get("step7_blind_review", True))
    if active_switches.get("step6_create_content", True):
        viable_dirs = (
            [d for d in directions if d.get("status") in ("pending_creation", "expanded", "discovered") and d.get("curated_post_keys")]
            if (directions or radar_summary is not None or manual_directions is not None or direct_post_keys is not None)
            else None
        )
        creation_summary, created_works = run_content_creation_batch(
            curated_directions=viable_dirs,
            db=db,
            profile_name=profile_name,
            creator_cfg=creator_override,
            run_id=run_id,
            await_blind_review=enable_review,
        )

    # --------------------------------------------------------------------------
    # 模块 7：可插拔 100% 上下文物理隔离普通网友盲测终审
    # --------------------------------------------------------------------------
    if enable_review:
        creations_input = created_works if active_switches.get("step6_create_content", True) else None
        review_summary, reviewed_works = run_blind_review_batch(
            db=db,
            creations=creations_input,
            profile_name=profile_name,
            creator_cfg=creator_override,
            reviewer_cfg=reviewer_override,
        )
        if creations_input is None:
            created_works = reviewed_works
        else:
            by_cid = {w["creation_id"]: w for w in reviewed_works}
            created_works = [by_cid.get(c["creation_id"], c) for c in created_works]

    total_elapsed = round(time.perf_counter() - t0, 2)
    finished_bj = datetime.now(TZ_CN).strftime("%Y-%m-%d %H:%M:%S")

    run_record: Dict[str, Any] = {
        "run_id": run_id,
        "profile_name": profile_name,
        "switches": active_switches,
        "started_at_ts": started_ts,
        "finished_at_bj": finished_bj,
        "total_elapsed_seconds": total_elapsed,
        "harvest_summary": harvest_summary,
        "step2_summary": step2_summary,
        "radar_summary": radar_summary,
        "expansion_summary": expansion_summary,
        "curation_summary": curation_summary,
        "creation_summary": creation_summary,
        "review_summary": review_summary,
        "audit_details": {
            "newly_digested": [
                {
                    "post_key": d.get("post_key"),
                    "source": d.get("source"),
                    "publish_time": d.get("publish_time"),
                    "is_usable": d.get("is_usable"),
                    "has_external_article": d.get("has_external_article"),
                    "reject_reason": d.get("reject_reason"),
                    "summary": d.get("summary"),
                    "valuable_comments": d.get("valuable_comments"),
                    "images": d.get("images"),
                    "model_used": (d.get("llm_trace") or {}).get("model_used"),
                }
                for d in newly_digested
            ],
            "directions": [
                {
                    "direction_id": d.get("direction_id"),
                    "rank": d.get("rank"),
                    "status": d.get("status"),
                    "direction_summary": d.get("direction_summary"),
                    "why_worth_creating": d.get("why_worth_creating"),
                    "search_keywords": d.get("search_keywords"),
                    "seed_post_keys": d.get("seed_post_keys"),
                    "searched_post_keys": d.get("searched_post_keys"),
                    "curated_post_keys": d.get("curated_post_keys"),
                    "expansion_stats": d.get("expansion_stats"),
                    "curation_result": d.get("curation_result"),
                }
                for d in directions
            ],
            "creations": [
                {
                    "creation_id": c.get("creation_id"),
                    "direction_id": c.get("direction_id"),
                    "direction_summary": c.get("direction_summary"),
                    "status": c.get("status"),
                    "publish_status": c.get("publish_status"),
                    "revision_count": c.get("revision_count", 0),
                    "post_text": c.get("post_text"),
                    "char_count": c.get("char_count"),
                    "source_post_keys": c.get("source_post_keys"),
                    "candidate_images_count": c.get("candidate_images_count", 0),
                    "visual_plan": c.get("visual_plan"),
                    "visual_trace": c.get("visual_trace"),
                    "final_image_paths": c.get("final_image_paths"),
                    "final_image_path": c.get("final_image_path"),
                    "initial_draft": c.get("initial_draft"),
                    "blind_review": c.get("blind_review"),
                    "text_model": (c.get("llm_trace") or {}).get("model_used"),
                }
                for c in created_works
            ],
        },
    }

    trace_path = save_run_audit_bundle(run_record)
    db.save_run_trace(run_record)

    report_text = format_pipeline_execution_report(run_record, trace_path)
    logger.info(report_text)
    return run_record


def main() -> None:
    logger = get_pipeline_logger()
    continuous = bool(SCHEDULE_CONFIG.get("continuous_loop", False))
    interval_sec = max(60, int(SCHEDULE_CONFIG.get("interval_seconds", 3600)))

    round_idx = 0
    while True:
        round_idx += 1
        try:
            execute_pipeline_round()
        except KeyboardInterrupt:
            logger.info("[流水线退出] 收到手动中断信号，安全退出。")
            break
        except Exception as exc:
            logger.exception(f"❌ [流水线异常] 第 {round_idx} 轮运行出现未捕获异常: {exc}")

        if not continuous:
            break

        logger.info(f"[常驻调度] 第 {round_idx} 轮已完成，休眠 {interval_sec} 秒后开启下一轮...")
        try:
            time.sleep(interval_sec)
        except KeyboardInterrupt:
            logger.info("[流水线退出] 休眠期间收到手动中断信号，安全退出。")
            break


if __name__ == "__main__":
    main()
