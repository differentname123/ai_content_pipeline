# -*- coding: utf-8 -*-
"""
模块 5：方向素材精选过滤模块 (`material_curator.py`)

高内聚 · 低耦合契约：
- 输入：目标方向 `direction` + 候选单帖理解档案列表 `candidate_digests`（含种子帖与跨平台关键词搜回的新帖）；
- 处理：由模型逐条核验相关性，精准剔除因关键词碰撞混入的跑题帖、擦边帖或空洞重复噪音；
- 输出：真正契合该方向、可直接送入独立创作模块的 `curated_post_keys` 列表。
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
    CURATOR_CONFIG,
)
from content_pipeline.pipeline_db import PipelineDB
from content_pipeline.pipeline_logger import get_pipeline_logger
from content_pipeline.topic_radar import format_digest_card


def _normalize_candidate_id(raw_token: Any) -> str:
    """严格规范化候选编号：仅接受形如 `C1`, `C12` 的纯字符串编号。"""
    if not isinstance(raw_token, str):
        return ""
    tok = raw_token.strip().upper()
    if tok.startswith("C") and tok[1:].isdigit():
        return f"C{int(tok[1:])}"
    return ""


def _build_curation_validator(valid_ids: set):
    """构建零容忍严格 Schema 校验器：锁死顶层 4 键与 curated_items 内每个 {id, reason} 标准对象。"""
    expected_keys = {"rejected_summary", "curation_note", "is_viable", "curated_items"}
    expected_item_keys = {"id", "reason"}

    def _validate(data: Dict[str, Any]) -> Optional[str]:
        actual_keys = set(data.keys())
        missing = expected_keys - actual_keys
        extra = actual_keys - expected_keys
        if missing:
            return f"缺少必填顶层字段: {sorted(missing)}（实际顶层字段为 {sorted(actual_keys)}）"
        if extra:
            return f"存在不允许的额外顶层字段: {sorted(extra)}，顶层只能包含 {sorted(expected_keys)}"

        if not isinstance(data.get("rejected_summary"), str) or not data["rejected_summary"].strip():
            return "字段 `rejected_summary` 必须是非空字符串（无剔除可填 '无噪音剔除'）"
        if not isinstance(data.get("curation_note"), str) or not data["curation_note"].strip():
            return "字段 `curation_note` 必须是非空字符串"
        if not isinstance(data.get("is_viable"), bool):
            return "字段 `is_viable` 必须是布尔值 (true/false)"

        raw_items = data.get("curated_items")
        if not isinstance(raw_items, list):
            return "字段 `curated_items` 必须是数组，元素格式严格为 {\"id\": \"C1\", \"reason\": \"...\"}"

        for idx, elem in enumerate(raw_items):
            if not isinstance(elem, dict):
                return (
                    f"curated_items[{idx}] 必须是包含 {{\"id\", \"reason\"}} 的字典对象，"
                    f"严禁返回 {type(elem).__name__}！"
                )
            item_keys = set(elem.keys())
            if item_keys != expected_item_keys:
                return (
                    f"curated_items[{idx}] 键名违规（实际为 {sorted(item_keys)}），"
                    f"必须且只能包含 [\"id\", \"reason\"] 两个键！"
                )
            raw_id = elem.get("id")
            if not isinstance(raw_id, str):
                return f"curated_items[{idx}].id 必须是纯字符串编号（如 \"C1\"）"
            norm_id = _normalize_candidate_id(raw_id)
            if not norm_id or norm_id not in valid_ids:
                return f"curated_items[{idx}].id={raw_id!r} 不是有效的候选编号（合法范围 C1~C{len(valid_ids)}）"
            raw_reason = elem.get("reason")
            if not isinstance(raw_reason, str) or len(raw_reason.strip()) < 2:
                return f"curated_items[{idx}].reason 必须写出该卡片的具体事实锚点与入选理由，不能为空"

        if data["is_viable"]:
            if not raw_items:
                return "当 `is_viable=true` 时，`curated_items` 必须至少包含 1 个入选对象"
        else:
            if raw_items:
                return "当 `is_viable=false` 时，`curated_items` 必须为空数组 []"

        return None

    return _validate


def curate_materials_for_direction(
    direction: Dict[str, Any],
    candidate_digests: Sequence[Dict[str, Any]],
    raw_lookup: Optional[Dict[str, Dict[str, Any]]] = None,
    profile_name: str = ACTIVE_PROFILE,
    curator_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    模块 5 单方向独立入口：核验并精选属于 `direction` 的真实高价值素材 `curated_post_keys`。
    返回更新后的 `direction` 字典（含 `status`, `curated_post_keys`, `curation_result`）。
    """
    cfg = dict(CURATOR_CONFIG)
    if curator_cfg:
        cfg.update(curator_cfg)

    profile = CONTENT_PROFILES.get(profile_name) or CONTENT_PROFILES[ACTIVE_PROFILE]
    prompt_file = str(profile.get("curation_prompt_file") or "step4_material_curation.md")

    out_dir = dict(direction)
    usable_cands = [d for d in (candidate_digests or []) if isinstance(d, dict) and d.get("is_usable", True)]
    if not usable_cands:
        out_dir["status"] = "dropped_empty_materials"
        out_dir["curated_post_keys"] = []
        out_dir["curation_result"] = {
            "is_viable": False,
            "curation_note": "无可用候选单帖档案",
            "rejected_summary": "",
            "curated_items_detail": [],
        }
        return out_dir

    seed_set = set(out_dir.get("seed_post_keys") or [])
    effective_raw = raw_lookup or {}
    alias_to_key: Dict[str, str] = {}
    card_blocks: List[str] = []

    for idx, dig in enumerate(usable_cands, 1):
        alias = f"C{idx}"
        pk = str(dig.get("post_key") or "")
        alias_to_key[alias] = pk
        origin_tag = "初始种子帖" if pk in seed_set else "关键词深挖新增帖"
        card_str = format_digest_card(f"候选编号:{alias} | {origin_tag}", dig, effective_raw.get(pk))
        card_blocks.append(card_str)

    slots = {
        "DIRECTION_SUMMARY": str(out_dir.get("direction_summary") or ""),
        "WHY_WORTH_CREATING": str(out_dir.get("why_worth_creating") or ""),
        "CANDIDATE_COUNT": str(len(usable_cands)),
        "CANDIDATE_CARDS": "\n\n".join(card_blocks),
    }

    rendered_prompt, prompt_meta = render_prompt_template(prompt_file, slots)
    parsed, llm_trace = call_llm_json(
        prompt=rendered_prompt,
        prompt_meta=prompt_meta,
        model=cfg.get("model"),
        fallback_models=cfg.get("fallback_models"),
        timeout=int(cfg.get("timeout", 600)),
        preset_model_group=str(cfg.get("preset_model_group") or "medium"),
        validator=_build_curation_validator(set(alias_to_key.keys())),
        schema_retries=3,
    )

    if parsed is None:
        # 容灾回退：若精选调用异常且存在初始种子帖，至少保底使用初始种子帖不中断流水线
        fallback_keys = [k for k in (out_dir.get("seed_post_keys") or []) if k in alias_to_key.values()]
        out_dir["curated_post_keys"] = fallback_keys
        out_dir["status"] = "pending_creation" if fallback_keys else "curation_failed"
        out_dir["curation_result"] = {
            "is_viable": bool(fallback_keys),
            "curation_note": f"精选调用异常回退种子帖: {llm_trace.get('parse_error') or llm_trace.get('call_status')}",
            "rejected_summary": "",
            "curated_items_detail": [],
            "llm_trace": llm_trace,
        }
        return out_dir

    curated_keys: List[str] = []
    curated_items_detail: List[Dict[str, str]] = []
    for raw_item in parsed.get("curated_items") or []:
        norm = _normalize_candidate_id(raw_item.get("id"))
        reason_txt = str(raw_item.get("reason") or "").strip()
        if norm in alias_to_key and alias_to_key[norm] not in curated_keys:
            pk_val = alias_to_key[norm]
            curated_keys.append(pk_val)
            curated_items_detail.append({
                "id": norm,
                "post_key": pk_val,
                "reason": reason_txt,
            })

    is_viable = bool(parsed.get("is_viable", False)) and bool(curated_keys)
    out_dir["curated_post_keys"] = curated_keys if is_viable else []
    out_dir["status"] = "pending_creation" if is_viable else "dropped_by_curator"
    out_dir["curation_result"] = {
        "is_viable": is_viable,
        "candidate_count": len(usable_cands),
        "kept_count": len(out_dir["curated_post_keys"]),
        "rejected_count": len(usable_cands) - len(out_dir["curated_post_keys"]),
        "curation_note": str(parsed.get("curation_note") or "").strip(),
        "rejected_summary": str(parsed.get("rejected_summary") or "").strip(),
        "curated_items_detail": curated_items_detail if is_viable else [],
        "llm_trace": llm_trace,
    }
    return out_dir


def run_material_curation_batch(
    directions: Sequence[Dict[str, Any]],
    digests_by_direction_id: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    db: Optional[PipelineDB] = None,
    profile_name: str = ACTIVE_PROFILE,
    curator_cfg: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """批量对多个方向并发执行素材精选并持久化到 `pipeline_directions`。"""
    logger = get_pipeline_logger()
    db = db or PipelineDB()
    cfg = dict(CURATOR_CONFIG)
    if curator_cfg:
        cfg.update(curator_cfg)

    t0 = time.perf_counter()
    if not directions:
        return (
            {
                "status": "skipped_empty",
                "elapsed_seconds": 0.0,
                "input_directions": 0,
                "viable_directions": 0,
                "curation_preview": [],
            },
            [],
        )

    # 收集全部关联 post_keys 以读取最新热度与缺失的 digest
    all_keys: List[str] = []
    for d in directions:
        keys_for_d = (
            d.get("candidate_usable_keys")
            or list(dict.fromkeys((d.get("seed_post_keys") or []) + (d.get("searched_post_keys") or [])))
        )
        for pk in keys_for_d:
            if pk not in all_keys:
                all_keys.append(pk)

    raw_lookup = db.fetch_raw_posts_by_keys(all_keys)
    fallback_digest_map = db.fetch_digests_by_keys(all_keys) if not digests_by_direction_id else {}

    order_map = {d["direction_id"]: idx for idx, d in enumerate(directions)}
    curated_dirs: List[Dict[str, Any]] = []
    workers = max(1, min(int(cfg.get("concurrency", 3)), len(directions)))

    def _curate_one(d_obj: Dict[str, Any]) -> Dict[str, Any]:
        did = d_obj["direction_id"]
        if digests_by_direction_id and did in digests_by_direction_id:
            cands = digests_by_direction_id[did]
        else:
            keys_d = d_obj.get("candidate_usable_keys") or list(
                dict.fromkeys((d_obj.get("seed_post_keys") or []) + (d_obj.get("searched_post_keys") or []))
            )
            cands = [
                fallback_digest_map[k]
                for k in keys_d
                if k in fallback_digest_map and fallback_digest_map[k].get("is_usable")
            ]
        return curate_materials_for_direction(
            direction=d_obj,
            candidate_digests=cands,
            raw_lookup=raw_lookup,
            profile_name=profile_name,
            curator_cfg=cfg,
        )

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = [pool.submit(_curate_one, d) for d in directions]
        for fut in as_completed(futs):
            curated_dirs.append(fut.result())

    curated_dirs.sort(key=lambda x: order_map.get(x["direction_id"], 99))
    db.upsert_directions(curated_dirs)

    viable_list = [d for d in curated_dirs if d.get("status") == "pending_creation" and d.get("curated_post_keys")]
    elapsed = round(time.perf_counter() - t0, 2)

    summary = {
        "status": "ok" if viable_list else "empty_after_curation",
        "elapsed_seconds": elapsed,
        "input_directions": len(curated_dirs),
        "viable_directions": len(viable_list),
        "curation_preview": [
            {
                "direction_id": d["direction_id"],
                "direction_summary": d["direction_summary"],
                "status": d["status"],
                "candidate_count": (d.get("curation_result") or {}).get("candidate_count", 0),
                "kept_count": len(d.get("curated_post_keys") or []),
                "curated_post_keys": d.get("curated_post_keys") or [],
                "curation_note": (d.get("curation_result") or {}).get("curation_note", ""),
                "rejected_summary": (d.get("curation_result") or {}).get("rejected_summary", ""),
                "curated_items_detail": (d.get("curation_result") or {}).get("curated_items_detail") or [],
                "rejected_count": (d.get("curation_result") or {}).get("rejected_count", 0),
            }
            for d in curated_dirs
        ],
    }

    logger.info(
        f"[Module5/素材精选完成] 耗时={elapsed:.2f}s | 输入方向={len(curated_dirs)} -> 合格方向={len(viable_list)} | "
        + " ; ".join(
            f"{d['direction_summary'][:32]}(候选{(d.get('curation_result') or {}).get('candidate_count', 0)}->"
            f"精选{len(d.get('curated_post_keys') or [])}帖)"
            for d in curated_dirs
        )
    )
    return summary, curated_dirs
