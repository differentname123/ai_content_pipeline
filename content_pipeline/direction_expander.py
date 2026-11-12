# -*- coding: utf-8 -*-
"""
模块 4：按方向跨平台「指哪打哪」深挖与即时理解模块 (`direction_expander.py`)

高内聚 · 低耦合契约：
- 输入：任意创作方向对象 `direction`（既可来自 `topic_radar.py` 自动发现，也可由人工直接构造传入，如 `{"direction_summary": "...", "search_keywords": ["..."]}`）；
- 处理：
  1) 调用模块 1 的 `harvest_by_keywords` 并发检索五大平台（X / Reddit / HackerNews / Binance / OKX），拉回正文、外链原文、高赞评论与配图并入库；
  2) 调用模块 2 的 `comprehend_posts_by_keys` 对新搜到的非视频帖执行高并发多模态理解（已有种子帖自动走缓存 0 开销）；
- 输出：扩充后的方向对象及该方向候选池的全部可用单帖理解档案 `candidate_digests`。
"""
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from uuid import uuid4

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from content_pipeline.pipeline_config import (
    ACTIVE_PROFILE,
    EXPANDER_CONFIG,
)
from content_pipeline.pipeline_db import PipelineDB
from content_pipeline.pipeline_logger import get_pipeline_logger
from content_pipeline.step2_comprehension import comprehend_posts_by_keys
from platforms.harvester_runner import harvest_by_keywords


def normalize_manual_or_radar_direction(
    raw_direction: Dict[str, Any],
    rank: int = 1,
    profile_name: str = ACTIVE_PROFILE,
    run_id: str = "manual_direction",
) -> Dict[str, Any]:
    """将人工传入或雷达产出的方向字典统一规范为标准结构。"""
    now_ts = int(time.time())
    d = dict(raw_direction or {})
    summary = str(d.get("direction_summary") or d.get("topic_summary") or "").strip()
    kws = [str(k).strip() for k in (d.get("search_keywords") or []) if str(k).strip()]
    if not kws and summary:
        kws = [summary]
    seeds = [str(k).strip() for k in (d.get("seed_post_keys") or d.get("post_keys") or []) if str(k).strip()]
    return {
        "direction_id": d.get("direction_id") or f"dir_{now_ts}_{rank}_{uuid4().hex[:6]}",
        "run_id": d.get("run_id") or run_id,
        "profile_name": d.get("profile_name") or profile_name,
        "rank": int(d.get("rank") or rank),
        "status": d.get("status") or "discovered",
        "created_at_ts": int(d.get("created_at_ts") or now_ts),
        "direction_summary": summary,
        "why_worth_creating": str(d.get("why_worth_creating") or d.get("why_selected") or "人工指定或热点方向").strip(),
        "search_keywords": kws,
        "seed_post_keys": seeds,
        "searched_post_keys": list(d.get("searched_post_keys") or []),
        "curated_post_keys": list(d.get("curated_post_keys") or seeds),
    }


def expand_direction_across_platforms(
    direction: Dict[str, Any],
    db: Optional[PipelineDB] = None,
    profile_name: str = ACTIVE_PROFILE,
    expander_cfg: Optional[Dict[str, Any]] = None,
    step2_cfg: Optional[Dict[str, Any]] = None,
    run_id: str = "manual_expand",
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    模块 4 单方向独立入口：按 `direction["search_keywords"]` 跨平台搜索并对新帖完成单帖理解。
    返回 `(updated_direction, usable_candidate_digests)`。
    """
    logger = get_pipeline_logger()
    db = db or PipelineDB()
    cfg = dict(EXPANDER_CONFIG)
    if expander_cfg:
        cfg.update(expander_cfg)

    norm_dir = normalize_manual_or_radar_direction(direction, profile_name=profile_name, run_id=run_id)
    keywords = norm_dir["search_keywords"]
    seed_keys = list(norm_dir["seed_post_keys"])

    t0 = time.perf_counter()
    harvest_res = harvest_by_keywords(
        keywords=keywords,
        platforms=cfg.get("platforms"),
        per_platform_limit=int(cfg.get("per_platform_limit", 8)),
        hours_back=int(cfg.get("hours_back", 72)),
        fetch_comments=bool(cfg.get("fetch_comments", True)),
        fetch_external_links=bool(cfg.get("fetch_external_links", True)),
        download_media=bool(cfg.get("download_media", True)),
        save_to_mongo=True,
    )
    searched_keys = list(harvest_res.get("post_keys") or [])

    combined_keys: List[str] = []
    for pk in seed_keys + searched_keys:
        if pk and pk not in combined_keys:
            combined_keys.append(pk)

    comp_summary, digest_map = comprehend_posts_by_keys(
        post_keys=combined_keys,
        db=db,
        profile_name=profile_name,
        step2_cfg=step2_cfg,
        force_recompute=False,
        run_id=run_id,
    )

    usable_digests: List[Dict[str, Any]] = []
    for pk in combined_keys:
        dig = digest_map.get(pk)
        if dig and dig.get("status") == "ok" and dig.get("is_usable"):
            usable_digests.append(dig)

    elapsed = round(time.perf_counter() - t0, 2)
    ops_alerts = list(harvest_res.get("ops_alerts") or [])
    norm_dir.update(
        {
            "status": "expanded",
            "searched_post_keys": searched_keys,
            "candidate_usable_keys": [d["post_key"] for d in usable_digests],
            "expansion_stats": {
                "elapsed_seconds": elapsed,
                "by_platform_counts": harvest_res.get("by_platform_counts", {}),
                "platform_diagnosis": harvest_res.get("platform_diagnosis", {}),
                "ops_alerts": ops_alerts,
                "total_searched": len(searched_keys),
                "newly_comprehended": comp_summary.get("newly_processed_count", 0),
                "excluded_videos": comp_summary.get("excluded_video_count", 0),
                "usable_candidates_total": len(usable_digests),
            },
        }
    )

    if ops_alerts:
        for alert_msg in ops_alerts:
            logger.warning(f"[Module4/平台异常感知] {alert_msg}")

    logger.info(
        f"[Module4/指哪打哪深挖] 方向=「{norm_dir['direction_summary'][:42]}」 | 搜索词={keywords} "
        f"| 搜回={len(searched_keys)}条 {harvest_res.get('by_platform_counts')} "
        f"| 新理解={comp_summary.get('newly_processed_count', 0)}条 -> 候选可用池={len(usable_digests)}条 ({elapsed}s)"
    )
    return norm_dir, usable_digests


def run_direction_expansion_batch(
    directions: Sequence[Dict[str, Any]],
    db: Optional[PipelineDB] = None,
    profile_name: str = ACTIVE_PROFILE,
    expander_cfg: Optional[Dict[str, Any]] = None,
    step2_cfg: Optional[Dict[str, Any]] = None,
    run_id: str = "manual_expand_batch",
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
    """
    批量按方向跨平台深挖并入库保存。
    返回 `(batch_summary, expanded_directions, candidate_digests_by_direction_id)`。
    """
    db = db or PipelineDB()
    t0 = time.perf_counter()
    expanded_dirs: List[Dict[str, Any]] = []
    digests_by_did: Dict[str, List[Dict[str, Any]]] = {}

    for idx, d in enumerate(directions or [], 1):
        norm_d = normalize_manual_or_radar_direction(d, rank=idx, profile_name=profile_name, run_id=run_id)
        exp_d, cand_digs = expand_direction_across_platforms(
            direction=norm_d,
            db=db,
            profile_name=profile_name,
            expander_cfg=expander_cfg,
            step2_cfg=step2_cfg,
            run_id=run_id,
        )
        expanded_dirs.append(exp_d)
        digests_by_did[exp_d["direction_id"]] = cand_digs

    if expanded_dirs:
        db.upsert_directions(expanded_dirs)

    elapsed = round(time.perf_counter() - t0, 2)
    summary = {
        "status": "ok" if expanded_dirs else "skipped_empty",
        "elapsed_seconds": elapsed,
        "directions_count": len(expanded_dirs),
        "total_searched_posts": sum(len(d.get("searched_post_keys") or []) for d in expanded_dirs),
        "total_newly_comprehended": sum(
            int((d.get("expansion_stats") or {}).get("newly_comprehended", 0)) for d in expanded_dirs
        ),
        "directions_expansion_preview": [
            {
                "direction_id": d["direction_id"],
                "direction_summary": d["direction_summary"],
                "search_keywords": d["search_keywords"],
                "seed_count": len(d["seed_post_keys"]),
                "searched_count": len(d["searched_post_keys"]),
                "by_platform_counts": (d.get("expansion_stats") or {}).get("by_platform_counts", {}),
                "platform_diagnosis": (d.get("expansion_stats") or {}).get("platform_diagnosis", {}),
                "ops_alerts": (d.get("expansion_stats") or {}).get("ops_alerts", []),
                "usable_candidates_total": len(d.get("candidate_usable_keys") or []),
            }
            for d in expanded_dirs
        ],
    }
    return summary, expanded_dirs, digests_by_did
