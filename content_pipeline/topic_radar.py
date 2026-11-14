# -*- coding: utf-8 -*-
"""
模块 3：热点事件与创作方向发现模块 (`topic_radar.py`)

高内聚 · 低耦合契约：
- 输入：近期可用的单帖多模态理解档案列表 `digests`（无需耦合采集器细节或下游创作模板）
- 处理：
  1) 当候选条数 `<= batch_size` 时，在单次全局窗口中无截断通览全部档案；
  2) 当候选条数 `> batch_size` 时，自动切分为多个批次并发扫描，再执行一次轻量全局归并去重，绝不丢弃任何候选帖；
  3) 自动参考近期已产出话题摘要，避免连续轮次重复炒冷饭；
- 输出：值得深挖创作的方向列表 `directions`（每项包含 `direction_id`、`direction_summary`、`search_keywords`、`seed_post_keys`、`why_worth_creating`）。
"""
import json
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from uuid import uuid4

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from content_pipeline.llm_gateway import call_llm_json, render_prompt_template
from content_pipeline.pipeline_config import (
    ACTIVE_PROFILE,
    CONTENT_PROFILES,
    RADAR_CONFIG,
)
from content_pipeline.pipeline_db import PipelineDB, round_robin_interleave_by_source
from content_pipeline.pipeline_logger import get_pipeline_logger


def normalize_ref_token(raw_ref: Any) -> str:
    """统一将 `#12`、`[#12]`、`12` 归一化为 `#12`。"""
    token = str(raw_ref or "").strip().strip("[]()")
    if not token:
        return ""
    if token.startswith("#"):
        token = token[1:].strip()
    return f"#{token}" if token.isdigit() else ""


def format_engagement_label(source: str, raw_post: Optional[Dict[str, Any]]) -> str:
    """客观呈现各平台真实热度信号。"""
    p = raw_post or {}
    eng = p.get("engagement") or {}
    views = int(eng.get("view_count") or 0)
    likes = int(eng.get("like_count") or 0)
    comments = int(eng.get("comment_count") or 0)
    shares = int(eng.get("share_count") or 0)
    l_vel = float(eng.get("likes_per_hour") or 0.0)
    v_vel = float(eng.get("views_per_hour") or 0.0)

    if str(source).lower() == "reddit":
        extra = ((p.get("metadata") or {}).get("platform_extra") or {})
        sub = extra.get("subreddit") or "r/reddit"
        rank = extra.get("rank_in_feed")
        rank_part = f"热榜#{rank}" if rank else "热榜"
        if likes > 0 or comments > 0:
            return f"{sub} {rank_part}, 赞:{likes}, 评:{comments}"
        return f"{sub} {rank_part}"

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


def format_digest_card(
    alias: str,
    digest: Dict[str, Any],
    live_raw_post: Optional[Dict[str, Any]] = None,
) -> str:
    """无截断组装单帖理解卡片（完整保留图片描述与全部有价值评论增量，消除信息损失）。"""
    source = str(digest.get("source") or "unknown")
    heat_str = format_engagement_label(source, live_raw_post)
    ext_flag = " | 含外链全文" if digest.get("has_external_article") else ""

    post_imgs = [
        im for im in (digest.get("images") or [])
        if isinstance(im, dict) and im.get("local_path")
    ]
    if post_imgs:
        img_descs = "；".join(
            f"[{im.get('origin_label') or '配图'}] {str(im.get('desc') or '').strip()}"
            for im in post_imgs
        )
        visual_str = f"附带本地真图({len(post_imgs)}张): {img_descs}"
    else:
        visual_str = "附带本地真图: 0张"

    lines = [
        f"[{alias}] 平台:{source}{ext_flag} | 热度({heat_str}) | {visual_str}",
        f"  - 完整内容还原: {digest.get('summary') or ''}",
    ]
    val_comms = [str(c).strip() for c in (digest.get("valuable_comments") or []) if str(c).strip()]
    if val_comms:
        lines.append("  - 评论区关键增量: " + "；".join(val_comms))
    return "\n".join(lines)


def _validate_radar_json(data: Dict[str, Any]) -> Optional[str]:
    directions = data.get("directions")
    if not isinstance(directions, list):
        return "缺少数组字段 `directions`"
    for idx, item in enumerate(directions, 1):
        if not isinstance(item, dict):
            return f"directions[{idx}] 必须是对象"
        if not str(item.get("direction_summary") or "").strip():
            return f"directions[{idx}].direction_summary 不能为空"
        if not isinstance(item.get("seed_refs"), list) or not item.get("seed_refs"):
            return f"directions[{idx}].seed_refs 必须是非空编号数组"
        kws = item.get("search_keywords")
        if not isinstance(kws, list) or not [x for x in kws if isinstance(x, str) and x.strip()]:
            return f"directions[{idx}].search_keywords 必须包含至少 1 个非空搜索词"
    return None


def _run_radar_window(
    window_digests: List[Dict[str, Any]],
    raw_lookup: Dict[str, Dict[str, Any]],
    recent_block: str,
    prompt_file: str,
    profile_preference: str,
    max_directions: int,
    cfg: Dict[str, Any],
    batch_index: int = 1,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    alias_to_digest: Dict[str, Dict[str, Any]] = {}
    cards: List[str] = []
    for idx, d in enumerate(window_digests, 1):
        alias = f"#{idx}"
        alias_to_digest[alias] = d
        cards.append(format_digest_card(alias, d, raw_lookup.get(d.get("post_key", ""))))

    now_utc_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    slots = {
        "CURRENT_TIME_UTC": now_utc_str,
        "MAX_DIRECTIONS": str(max_directions),
        "PROFILE_PREFERENCE": profile_preference,
        "RECENT_CREATED_BLOCK": recent_block,
        "CANDIDATE_COUNT": str(len(window_digests)),
        "CANDIDATE_DIGEST_LIST": "\n\n".join(cards),
    }
    rendered_prompt, prompt_meta = render_prompt_template(prompt_file, slots)
    parsed, llm_trace = call_llm_json(
        prompt=rendered_prompt,
        prompt_meta=prompt_meta,
        model=cfg.get("model"),
        fallback_models=cfg.get("fallback_models"),
        timeout=int(cfg.get("timeout", 600)),
        preset_model_group=str(cfg.get("preset_model_group") or "medium"),
        validator=_validate_radar_json,
    )
    if parsed is None:
        return [], [], llm_trace

    resolved_dirs: List[Dict[str, Any]] = []
    used_keys = set()
    for item in parsed.get("directions") or []:
        if not isinstance(item, dict):
            continue
        seed_keys: List[str] = []
        for r_tok in item.get("seed_refs") or []:
            norm = normalize_ref_token(r_tok)
            if norm in alias_to_digest:
                pk = alias_to_digest[norm]["post_key"]
                if pk not in seed_keys and pk not in used_keys:
                    seed_keys.append(pk)
        if not seed_keys:
            continue
        used_keys.update(seed_keys)
        clean_kws: List[str] = []
        seen_kw = set()
        for kw in item.get("search_keywords") or []:
            s = str(kw or "").strip()
            if s and s.lower() not in seen_kw:
                seen_kw.add(s.lower())
                clean_kws.append(s)
        resolved_dirs.append(
            {
                "batch_index": batch_index,
                "rank_in_batch": len(resolved_dirs) + 1,
                "direction_summary": str(item.get("direction_summary") or "").strip(),
                "why_worth_creating": str(item.get("why_worth_creating") or "").strip(),
                "search_keywords": clean_kws[:5],
                "seed_post_keys": seed_keys,
            }
        )

    runner_ups: List[Dict[str, Any]] = []
    for ru in parsed.get("runner_up_notes") or []:
        if not isinstance(ru, dict):
            continue
        ru_keys = [
            alias_to_digest[normalize_ref_token(t)]["post_key"]
            for t in (ru.get("seed_refs") or [])
            if normalize_ref_token(t) in alias_to_digest
        ]
        runner_ups.append(
            {
                "post_keys": ru_keys,
                "direction_brief": str(ru.get("direction_brief") or "").strip(),
                "skip_reason": str(ru.get("skip_reason") or "").strip(),
            }
        )

    return resolved_dirs, runner_ups, llm_trace


def _merge_batch_directions(
    batch_candidates: List[Dict[str, Any]],
    max_directions: int,
    profile_preference: str,
    recent_block: str,
    cfg: Dict[str, Any],
    digest_lookup: Optional[Dict[str, Dict[str, Any]]] = None,
    merge_prompt_file: str = "step3_radar_merge.md",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """当分批扫描产生多个候选方向时，基于客观跨平台实证指标执行全局归并去重并优选前 `max_directions` 个。"""
    eff_digests = digest_lookup or {}
    valid_dids = {f"D{i}" for i in range(1, len(batch_candidates) + 1)}
    items_desc = []
    for idx, c in enumerate(batch_candidates, 1):
        s_keys = c.get("seed_post_keys") or []
        plat_counts: Counter = Counter()
        ext_art_count = 0
        for pk in s_keys:
            dig = eff_digests.get(pk) or {}
            src = str(dig.get("source") or (pk.split(":", 1)[0] if ":" in pk else "unknown")).lower()
            plat_counts[src] += 1
            if dig.get("has_external_article"):
                ext_art_count += 1
        plat_str = ", ".join(f"{p}:{cnt}" for p, cnt in plat_counts.most_common())
        b_idx = c.get("batch_index", 1)
        b_rank = c.get("rank_in_batch", idx)
        items_desc.append(
            f"[D{idx}] 概述: {c['direction_summary']}\n"
            f"  - 客观实证指标: 批次#{b_idx}排位第{b_rank} | 关联种子帖={len(s_keys)}条 | "
            f"覆盖独立平台={len(plat_counts)}个({plat_str}) | 含外链全文报道={ext_art_count}篇\n"
            f"  - 搜索词: {json.dumps(c['search_keywords'], ensure_ascii=False)}\n"
            f"  - 价值说明: {c['why_worth_creating']}"
        )

    now_utc_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    slots = {
        "CURRENT_TIME_UTC": now_utc_str,
        "MAX_DIRECTIONS": str(max_directions),
        "PROFILE_PREFERENCE": profile_preference,
        "RECENT_CREATED_BLOCK": recent_block,
        "CANDIDATE_COUNT": str(len(batch_candidates)),
        "BATCH_CANDIDATES_BLOCK": "\n\n".join(items_desc),
    }
    merge_prompt, prompt_meta = render_prompt_template(merge_prompt_file, slots)

    def _val_merge(d: Dict[str, Any]) -> Optional[str]:
        m_list = d.get("merged_directions")
        if not isinstance(m_list, list):
            return "缺少数组 `merged_directions`"
        for i, elem in enumerate(m_list, 1):
            if not isinstance(elem, dict):
                return f"merged_directions[{i}] 必须是对象"
            sids = elem.get("source_ids")
            if not isinstance(sids, list) or not [x for x in sids if str(x).strip().upper() in valid_dids]:
                return f"merged_directions[{i}].source_ids 必须包含有效候选编号（D1..D{len(batch_candidates)}）"
            if not str(elem.get("direction_summary") or "").strip():
                return f"merged_directions[{i}].direction_summary 不能为空"
            kws = elem.get("search_keywords")
            if not isinstance(kws, list) or not [x for x in kws if isinstance(x, str) and x.strip()]:
                return f"merged_directions[{i}].search_keywords 必须包含至少 1 个非空搜索词"
        return None

    parsed, trace = call_llm_json(
        prompt=merge_prompt,
        prompt_meta=prompt_meta,
        model=cfg.get("model"),
        fallback_models=cfg.get("fallback_models"),
        timeout=int(cfg.get("timeout", 600)),
        preset_model_group=str(cfg.get("preset_model_group") or "medium"),
        validator=_val_merge,
    )
    if parsed is None:
        return batch_candidates[:max_directions], trace

    by_did = {f"D{i}": c for i, c in enumerate(batch_candidates, 1)}
    merged_out: List[Dict[str, Any]] = []
    used_post_keys = set()

    for m in parsed.get("merged_directions") or []:
        if not isinstance(m, dict):
            continue
        sids = [str(x).strip().upper() for x in (m.get("source_ids") or []) if str(x).strip()]
        seed_keys: List[str] = []
        fallback_kws: List[str] = []
        for sid in sids:
            orig = by_did.get(sid)
            if not orig:
                continue
            for pk in orig["seed_post_keys"]:
                if pk not in seed_keys and pk not in used_post_keys:
                    seed_keys.append(pk)
            for kw in orig["search_keywords"]:
                if kw not in fallback_kws:
                    fallback_kws.append(kw)
        if not seed_keys:
            continue
        used_post_keys.update(seed_keys)
        clean_kws: List[str] = []
        seen_kw = set()
        for kw in (m.get("search_keywords") or fallback_kws):
            s = str(kw or "").strip()
            if s and s.lower() not in seen_kw:
                seen_kw.add(s.lower())
                clean_kws.append(s)
        merged_out.append(
            {
                "direction_summary": str(m.get("direction_summary") or "").strip(),
                "why_worth_creating": str(m.get("why_worth_creating") or "").strip(),
                "search_keywords": (clean_kws or fallback_kws)[:5],
                "seed_post_keys": seed_keys,
            }
        )
        if len(merged_out) >= max_directions:
            break

    return (merged_out or batch_candidates[:max_directions]), trace


def discover_hot_directions(
    digests: List[Dict[str, Any]],
    raw_lookup: Optional[Dict[str, Dict[str, Any]]] = None,
    recent_created_topics: Optional[Sequence[str]] = None,
    profile_name: str = ACTIVE_PROFILE,
    radar_cfg: Optional[Dict[str, Any]] = None,
    run_id: str = "manual_radar",
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    模块 3 纯函数式入口：从输入的 `digests` 列表中发现热点方向并输出跨平台搜索词。
    自动适配「单窗口全量通览」与「超量分批并发提取 + 二次归并」。
    """
    logger = get_pipeline_logger()
    cfg = dict(RADAR_CONFIG)
    if radar_cfg:
        cfg.update(radar_cfg)

    profile = CONTENT_PROFILES.get(profile_name) or CONTENT_PROFILES[ACTIVE_PROFILE]
    prompt_file = str(profile.get("radar_prompt_file") or "step3_topic_radar.md")
    merge_prompt_file = str(profile.get("radar_merge_prompt_file") or "step3_radar_merge.md")
    profile_pref = str(profile.get("topic_preference") or "")
    max_directions = max(1, int(cfg.get("max_directions", 3)))
    batch_size = max(15, int(cfg.get("batch_size", 80)))

    t0 = time.perf_counter()
    usable_only = [d for d in (digests or []) if isinstance(d, dict) and d.get("is_usable", True)]
    if not usable_only:
        return (
            {
                "status": "skipped_empty",
                "elapsed_seconds": 0.0,
                "candidate_count": 0,
                "batch_count": 0,
                "selected_count": 0,
                "directions_preview": [],
            },
            [],
        )

    effective_raw = raw_lookup or {}
    ordered_digests = round_robin_interleave_by_source(
        usable_only,
        max_total=len(usable_only),
        raw_lookup=effective_raw if effective_raw else None,
    )
    digest_lookup = {
        str(d.get("post_key") or ""): d
        for d in ordered_digests
        if isinstance(d, dict) and d.get("post_key")
    }

    recent_list = [str(s).strip() for s in (recent_created_topics or []) if str(s).strip()]
    recent_block = (
        "\n".join(f"- {s}" for s in recent_list)
        if recent_list
        else "（近期暂无已发布创作，无重复顾虑）"
    )

    batches = [ordered_digests[i : i + batch_size] for i in range(0, len(ordered_digests), batch_size)]
    logger.info(
        f"[Module3/方向雷达] 全量可用候选={len(ordered_digests)}条 | 分批数={len(batches)} (batch_size={batch_size}) "
        f"| 目标上限={max_directions}个 | 近期防重参考={len(recent_list)}条"
    )

    all_batch_dirs: List[Dict[str, Any]] = []
    all_runner_ups: List[Dict[str, Any]] = []
    traces: List[Dict[str, Any]] = []

    if len(batches) == 1:
        b_dirs, b_rus, b_trace = _run_radar_window(
            window_digests=batches[0],
            raw_lookup=effective_raw,
            recent_block=recent_block,
            prompt_file=prompt_file,
            profile_preference=profile_pref,
            max_directions=max_directions,
            cfg=cfg,
            batch_index=1,
        )
        all_batch_dirs.extend(b_dirs)
        all_runner_ups.extend(b_rus)
        traces.append(b_trace)
        final_raw_dirs = all_batch_dirs[:max_directions]
    else:
        workers = max(1, min(int(cfg.get("batch_concurrency", 4)), len(batches)))
        per_batch_cap = max(2, max_directions)
        batch_results: List[Tuple[int, List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]] = []
        with ThreadPoolExecutor(max_workers=workers) as pool:
            fut_to_bidx = {
                pool.submit(
                    _run_radar_window,
                    batch,
                    effective_raw,
                    recent_block,
                    prompt_file,
                    profile_pref,
                    per_batch_cap,
                    cfg,
                    b_idx,
                ): b_idx
                for b_idx, batch in enumerate(batches, 1)
            }
            for fut in as_completed(fut_to_bidx):
                b_idx = fut_to_bidx[fut]
                b_dirs, b_rus, b_trace = fut.result()
                batch_results.append((b_idx, b_dirs, b_rus, b_trace))

        batch_results.sort(key=lambda x: x[0])
        for _, b_dirs, b_rus, b_trace in batch_results:
            all_batch_dirs.extend(b_dirs)
            all_runner_ups.extend(b_rus)
            traces.append(b_trace)

        if len(all_batch_dirs) > max_directions:
            final_raw_dirs, merge_trace = _merge_batch_directions(
                batch_candidates=all_batch_dirs,
                max_directions=max_directions,
                profile_preference=profile_pref,
                recent_block=recent_block,
                cfg=cfg,
                digest_lookup=digest_lookup,
                merge_prompt_file=merge_prompt_file,
            )
            traces.append(merge_trace)
        else:
            final_raw_dirs = all_batch_dirs

    now_ts = int(time.time())
    direction_records: List[Dict[str, Any]] = []
    for rank_idx, item in enumerate(final_raw_dirs[:max_directions], 1):
        did = f"dir_{now_ts}_{rank_idx}_{uuid4().hex[:6]}"
        direction_records.append(
            {
                "direction_id": did,
                "run_id": run_id,
                "profile_name": profile_name,
                "rank": rank_idx,
                "status": "discovered",
                "created_at_ts": now_ts,
                "direction_summary": item["direction_summary"],
                "why_worth_creating": item["why_worth_creating"],
                "search_keywords": item["search_keywords"],
                "seed_post_keys": item["seed_post_keys"],
                "searched_post_keys": [],
                "curated_post_keys": list(item["seed_post_keys"]),
                "candidate_pool_size": len(ordered_digests),
                "llm_trace": traces[-1] if traces else {},
            }
        )

    elapsed = round(time.perf_counter() - t0, 2)
    models_used = [t.get("model_used") for t in traces if isinstance(t, dict) and t.get("model_used")]
    summary = {
        "status": "ok" if direction_records else ("failed" if not traces else "empty_selection"),
        "elapsed_seconds": elapsed,
        "candidate_count": len(ordered_digests),
        "batch_count": len(batches),
        "selected_count": len(direction_records),
        "models_used": models_used,
        "directions_preview": [
            {
                "direction_id": d["direction_id"],
                "direction_summary": d["direction_summary"],
                "search_keywords": d["search_keywords"],
                "seed_post_keys": d["seed_post_keys"],
                "why_worth_creating": d["why_worth_creating"],
            }
            for d in direction_records
        ],
        "runner_up_notes": all_runner_ups[:6],
    }

    logger.info(
        f"[Module3/方向雷达完成] 耗时={elapsed:.2f}s | 候选={len(ordered_digests)}条({len(batches)}批) "
        f"-> 发现方向={len(direction_records)}个: "
        + " ; ".join(
            f"{d['direction_summary']} [词:{','.join(d['search_keywords'])} | 种子:{len(d['seed_post_keys'])}帖]"
            for d in direction_records
        )
    )
    return summary, direction_records


def run_topic_radar(
    db: Optional[PipelineDB] = None,
    profile_name: str = ACTIVE_PROFILE,
    radar_cfg: Optional[Dict[str, Any]] = None,
    run_id: str = "manual_radar",
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """模块 3 数据库便捷入口：自动从 `PipelineDB` 读取近期可用理解档案与近期已写话题并持久化发现结果。"""
    db = db or PipelineDB()
    cfg = dict(RADAR_CONFIG)
    if radar_cfg:
        cfg.update(radar_cfg)

    lookback_hours = float(cfg.get("lookback_hours", 48))
    dedup_hours = float(cfg.get("recent_created_dedup_hours", 72))

    usable_digests = db.fetch_usable_digests(lookback_hours=lookback_hours, limit=1000)
    raw_lookup = db.fetch_raw_posts_by_keys([d["post_key"] for d in usable_digests]) if usable_digests else {}
    recent_summaries = db.fetch_recent_created_summaries(lookback_hours=dedup_hours)

    summary, directions = discover_hot_directions(
        digests=usable_digests,
        raw_lookup=raw_lookup,
        recent_created_topics=recent_summaries,
        profile_name=profile_name,
        radar_cfg=cfg,
        run_id=run_id,
    )
    if directions:
        db.upsert_directions(directions)
    return summary, directions


if __name__ == "__main__":
    res_sum, dirs = run_topic_radar(run_id="standalone_radar_verify")
    for d in dirs:
        print(f"\n=== 方向 #{d['rank']}: {d['direction_summary']} ===")
        print(f"搜索词: {d['search_keywords']}")
        print(f"种子帖: {d['seed_post_keys']}")
        print(f"理由  : {d['why_worth_creating']}")
