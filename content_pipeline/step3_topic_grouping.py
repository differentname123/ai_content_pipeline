# -*- coding: utf-8 -*-
"""
Module 2 (Step 3)：全局单窗口按短编号归堆与选题模块 (`step3_topic_grouping.py`)

本质职责：
1. 将各平台公平轮询入选的合格情报卡，连同实时互动/热榜排名与可用原图描述，一次性放入同一个上下文窗口；
2. 由模型在全局视野内按短编号（`#1..#N`）合并同事件多帖（含带图帖）或挑出独立好帖；
3. 严守阶段边界：只输出选题与关联原帖映射，绝不生成 `creative_angle` 等长文提纲干扰下游原生创作。

可独立运行调试：直接在 IDE 运行本文件，即可基于库中已有的 `pipeline_post_digests` 执行一次全局归堆选题。
"""
import sys
import time
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
    STEP3_CONFIG,
)
from content_pipeline.pipeline_db import PipelineDB, round_robin_interleave_by_source
from content_pipeline.pipeline_logger import get_pipeline_logger


def _normalize_ref_token(raw_ref: Any) -> str:
    """统一将 `#12`、`[#12]`、`12` 归一化为 `#12`，做到 100% 确定性容错映射。"""
    token = str(raw_ref or "").strip().strip("[]()")
    if not token:
        return ""
    if token.startswith("#"):
        token = token[1:].strip()
    return f"#{token}" if token.isdigit() else ""


def _validate_step3_json(data: Dict[str, Any]) -> Optional[str]:
    topics = data.get("selected_topics")
    if not isinstance(topics, list):
        return "缺少数组字段 `selected_topics`"
    for idx, item in enumerate(topics, 1):
        if not isinstance(item, dict):
            return f"selected_topics[{idx}] 必须是对象"
        if not isinstance(item.get("post_refs"), list) or not item.get("post_refs"):
            return f"selected_topics[{idx}].post_refs 必须是非空编号数组"
        if not str(item.get("topic_summary") or "").strip():
            return f"selected_topics[{idx}].topic_summary 不能为空"
    return None


def _format_engagement_label(source: str, raw_post: Optional[Dict[str, Any]]) -> str:
    """客观呈现各平台真实热度信号，避免将不返回公开赞数的 Reddit RSS 误标为 0 赞冷帖。"""
    p = raw_post or {}
    eng = p.get("engagement") or {}
    views = int(eng.get("view_count") or 0)
    likes = int(eng.get("like_count") or 0)
    comments = int(eng.get("comment_count") or 0)
    shares = int(eng.get("share_count") or 0)
    l_vel = float(eng.get("likes_per_hour") or 0.0)
    v_vel = float(eng.get("views_per_hour") or 0.0)

    if str(source).lower() == "reddit" and views == 0 and likes == 0:
        extra = ((p.get("metadata") or {}).get("platform_extra") or {})
        sub = extra.get("subreddit") or "r/reddit"
        rank = extra.get("rank_in_feed")
        rank_str = f"24h多板块热榜第 #{rank} 名" if rank else "24h热榜入选"
        return f"{sub} {rank_str} (RSS无公开赞数)"

    parts = []
    if views > 0:
        parts.append(f"浏览:{views}")
    parts.append(f"赞:{likes}")
    parts.append(f"评:{comments}")
    if shares > 0:
        parts.append(f"转:{shares}")
    if v_vel >= 50:
        parts.append(f"增速:{v_vel:.0f}浏览/h")
    elif l_vel >= 1.0:
        parts.append(f"增速:{l_vel:.1f}赞/h")
    return ", ".join(parts)


def _format_candidate_card_block(
    alias: str,
    digest: Dict[str, Any],
    live_raw_post: Optional[Dict[str, Any]],
) -> str:
    """将单张情报卡、实时平台热度与可用主帖原图说明拼装为高信噪比审阅卡片。"""
    source = str(digest.get("source") or "unknown")
    heat_str = _format_engagement_label(source, live_raw_post)

    usable_imgs = [
        im for im in (digest.get("images") or [])
        if isinstance(im, dict) and im.get("usable_as_visual") and im.get("local_path")
    ]
    if usable_imgs:
        img_descs = "；".join(str(im.get("desc") or "").strip()[:90] for im in usable_imgs[:2])
        visual_str = f"可用原图({len(usable_imgs)}张): {img_descs}"
    else:
        visual_str = "可用原图: 无"

    lines = [
        f"[{alias}] 平台:{source} | 热度({heat_str}) | {visual_str}",
        f"  - 事实与看点: {digest.get('summary') or digest.get('complete_summary') or ''}",
    ]
    val_comms = digest.get("valuable_comments") or digest.get("comment_essence") or []
    if val_comms:
        lines.append("  - 评论补充/神评: " + "；".join(str(c) for c in val_comms[:3]))
    return "\n".join(lines)


def run_step3_topic_selection(
    db: Optional[PipelineDB] = None,
    profile_name: str = ACTIVE_PROFILE,
    step3_cfg: Optional[Dict[str, Any]] = None,
    run_id: str = "manual_step3",
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    执行 Step 3 单窗口全局归堆与爆款选题。
    返回 `(step3_summary, selected_topic_records)`。
    """
    logger = get_pipeline_logger()
    db = db or PipelineDB()
    cfg = dict(STEP3_CONFIG)
    if step3_cfg:
        cfg.update(step3_cfg)

    profile = CONTENT_PROFILES.get(profile_name) or CONTENT_PROFILES[ACTIVE_PROFILE]
    prompt_file = profile["step3_prompt_file"]

    t0 = time.perf_counter()
    lookback_hours = float(cfg.get("lookback_hours", 36))
    usable_digests = db.fetch_usable_digests(lookback_hours=lookback_hours, limit=500)

    dedup_hours = float(cfg.get("dedup_used_posts_hours", 72))
    used_post_keys, recent_created_topics = db.fetch_recent_creation_memory(
        profile_name=profile_name,
        lookback_hours=dedup_hours,
    )

    excluded_used_count = 0
    fresh_digests: List[Dict[str, Any]] = []
    for d in usable_digests:
        if d.get("post_key") in used_post_keys:
            excluded_used_count += 1
            continue
        fresh_digests.append(d)

    if not fresh_digests:
        elapsed = round(time.perf_counter() - t0, 2)
        logger.warning("[Step3/跳过] 当前窗口内没有未消费的合格情报卡，跳过选题。")
        return (
            {
                "status": "skipped_empty",
                "elapsed_seconds": elapsed,
                "candidate_count": 0,
                "excluded_used_posts": excluded_used_count,
                "selected_count": 0,
                "model_used": None,
                "selected_topics_preview": [],
                "runner_up_notes": [],
            },
            [],
        )

    # 关联最新实时互动数据，并按跨平台公平轮询截取单窗口上限（防止 Reddit/HN 被高浏览基数平台淹没）
    live_raw_map = db.fetch_raw_posts_by_keys([d["post_key"] for d in fresh_digests])
    max_window = int(cfg.get("max_candidates_in_window", 120))
    window_digests = round_robin_interleave_by_source(
        fresh_digests,
        max_total=max_window,
        raw_lookup=live_raw_map,
    )

    alias_to_digest: Dict[str, Dict[str, Any]] = {}
    card_blocks: List[str] = []
    for idx, d in enumerate(window_digests, 1):
        alias = f"#{idx}"
        alias_to_digest[alias] = d
        card_blocks.append(_format_candidate_card_block(alias, d, live_raw_map.get(d["post_key"])))

    target_count = int(cfg.get("target_topic_count", 3))
    recent_topics_str = (
        "\n".join(f"- {t}" for t in recent_created_topics[:20])
        if recent_created_topics
        else "（近期暂无已发布历史话题）"
    )

    slots = {
        "TARGET_TOPIC_COUNT": str(target_count),
        "PROFILE_PREFERENCE": profile.get("topic_preference", ""),
        "RECENT_CREATED_TOPICS": recent_topics_str,
        "CANDIDATE_COUNT": str(len(window_digests)),
        "CANDIDATE_DIGEST_LIST": "\n\n".join(card_blocks),
    }

    model_label = cfg.get("model") or f"group:{cfg.get('preset_model_group', 'medium')}"
    logger.info(
        f"[Step3/启动] 单窗口跨平台候选={len(window_digests)}条 (防重剔除旧帖={excluded_used_count}) -> "
        f"上限精选={target_count}个 | 路由={model_label}"
    )

    rendered_prompt, prompt_meta = render_prompt_template(prompt_file, slots)
    parsed, llm_trace = call_llm_json(
        prompt=rendered_prompt,
        prompt_meta=prompt_meta,
        model=cfg.get("model"),
        fallback_models=cfg.get("fallback_models"),
        timeout=int(cfg.get("timeout", 600)),
        preset_model_group=str(cfg.get("preset_model_group") or "medium"),
        validator=_validate_step3_json,
    )

    elapsed = round(time.perf_counter() - t0, 2)
    if parsed is None:
        logger.error(
            f"❌ [Step3/失败] 全局选题调用异常 | 耗时={elapsed:.2f}s | 原因={llm_trace.get('parse_error') or llm_trace.get('error_history')}"
        )
        return (
            {
                "status": "failed",
                "elapsed_seconds": elapsed,
                "candidate_count": len(window_digests),
                "excluded_used_posts": excluded_used_count,
                "selected_count": 0,
                "model_used": llm_trace.get("model_used"),
                "selected_topics_preview": [],
                "runner_up_notes": [],
                "llm_trace": llm_trace,
            },
            [],
        )

    raw_topics = parsed.get("selected_topics") or []
    raw_runner_ups = parsed.get("runner_up_notes") or []

    now_ts = int(time.time())
    topic_records: List[Dict[str, Any]] = []
    used_in_this_round = set()

    for rank_idx, item in enumerate(raw_topics, 1):
        if not isinstance(item, dict):
            continue
        resolved_keys: List[str] = []
        resolved_aliases: List[str] = []
        for r_tok in item.get("post_refs") or []:
            norm = _normalize_ref_token(r_tok)
            if norm in alias_to_digest:
                pk = alias_to_digest[norm]["post_key"]
                if pk not in resolved_keys and pk not in used_in_this_round:
                    resolved_keys.append(pk)
                    resolved_aliases.append(norm)

        if not resolved_keys:
            continue

        used_in_this_round.update(resolved_keys)
        # 由确定性代码计算多源合并或单帖模式，无需消耗大模型注意力
        actual_mode = "multi_post_event" if len(resolved_keys) >= 2 else "single_viral_story"
        topic_id = f"topic_{now_ts}_{len(topic_records) + 1}_{uuid4().hex[:6]}"
        topic_summary = str(item.get("topic_summary") or "").strip()

        topic_records.append(
            {
                "topic_id": topic_id,
                "run_id": run_id,
                "profile_name": profile_name,
                "rank": len(topic_records) + 1,
                "status": "pending_creation",
                "created_at_ts": now_ts,
                "topic_summary": topic_summary,
                "mode": actual_mode,
                "post_keys": resolved_keys,
                "window_aliases": resolved_aliases,
                "why_selected": str(item.get("why_selected") or "").strip(),
                "candidate_pool_size": len(window_digests),
                "llm_trace": llm_trace if len(topic_records) == 0 else {"shared_with_rank_1_trace_id": llm_trace.get("trace_id")},
            }
        )
        if len(topic_records) >= target_count:
            break

    runner_up_clean: List[Dict[str, Any]] = []
    for ru in raw_runner_ups:
        if not isinstance(ru, dict):
            continue
        ru_keys = []
        for r_tok in ru.get("post_refs") or []:
            norm = _normalize_ref_token(r_tok)
            if norm in alias_to_digest:
                ru_keys.append(alias_to_digest[norm]["post_key"])
        runner_up_clean.append(
            {
                "post_keys": ru_keys,
                "topic_brief": str(ru.get("topic_brief") or "").strip(),
                "skip_reason": str(ru.get("skip_reason") or "").strip(),
            }
        )

    if topic_records:
        db.upsert_topics(topic_records)

    summary = {
        "status": "ok" if topic_records else "empty_selection",
        "elapsed_seconds": elapsed,
        "candidate_count": len(window_digests),
        "excluded_used_posts": excluded_used_count,
        "selected_count": len(topic_records),
        "model_used": llm_trace.get("model_used"),
        "prompt_sha1": llm_trace.get("prompt_sha1"),
        "trace_id": llm_trace.get("trace_id"),
        "selected_topics_preview": [
            {
                "topic_id": t["topic_id"],
                "topic_summary": t["topic_summary"],
                "mode": t["mode"],
                "post_keys": t["post_keys"],
                "why_selected": t["why_selected"],
            }
            for t in topic_records
        ],
        "runner_up_notes": runner_up_clean,
    }

    logger.info(
        f"[Step3/完成] 耗时={elapsed:.2f}s | 模型={llm_trace.get('model_used')} | "
        f"候选={len(window_digests)} -> 入选={len(topic_records)}个: "
        + " ; ".join(f"{t['topic_summary']}({len(t['post_keys'])}帖)" for t in topic_records)
    )
    return summary, topic_records


if __name__ == "__main__":
    s3_res, topics_out = run_step3_topic_selection(run_id="standalone_step3_verify")
    for t in topics_out:
        print(f"\n=== 选题 #{t['rank']} ({t['mode']}): {t['topic_summary']} ===")
        print(f"关联原帖: {t['post_keys']}")
        print(f"入选依据: {t['why_selected']}")
