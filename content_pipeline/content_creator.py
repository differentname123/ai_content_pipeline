# -*- coding: utf-8 -*-
"""
模块 6：100% 解耦的高内聚原创图文创作模块 (`content_creator.py`)

架构解耦铁律：
1. 本模块与上游的「热点方向发现 (`topic_radar`)」、「关键词深挖 (`direction_expander`)」、「素材精选 (`material_curator`)」**100% 解耦**。
2. 核心函数 `create_from_materials(materials_bundle, ...)` **只认传入的素材包 (`materials_bundle`)**：
   无论这批素材是上游自动流转来的、人工随手指定几个 `post_keys` 组装的，甚至外部直接传入的一段自定义图文素材字典，均可直接独立生成原创正文与确定性像素修图。
3. 最新原始评论直通注入：`assemble_creation_materials()` 直接从 `harvested_social_posts` 提取最新的原始高赞评论（连同外链原文全文、单帖理解摘要、全部归属清晰的本地真图），无需因新增评论重跑阶段 2 理解即可获得 100% 最新评论区增量。
"""
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
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
    CREATOR_CONFIG,
    STAGE_SWITCHES,
)
from content_pipeline.pipeline_db import PipelineDB
from content_pipeline.pipeline_logger import get_pipeline_logger
from content_pipeline.step2_comprehension import (
    ORIGIN_DISPLAY_NAMES,
    extract_ordered_local_images,
    get_unified_post_text,
    strip_inline_media_placeholders,
)
from content_pipeline.visual_studio import produce_visual_asset


def _validate_creator_json(data: Dict[str, Any]) -> Optional[str]:
    post_text = data.get("post_text")
    if not isinstance(post_text, str) or not post_text.strip():
        return "缺少非空字符串字段 `post_text`"
    if "selected_image_ids" in data:
        sids = data.get("selected_image_ids")
        if not isinstance(sids, list):
            return "`selected_image_ids` 必须是数组（如 `[\"IMG_1\"]` 或 `[]`）"
        return None
    vp = data.get("visual_plan")
    if not isinstance(vp, dict):
        return "缺少数组字段 `selected_image_ids`"
    action = str(vp.get("action") or "").strip().lower()
    if action not in ("none", "reuse", "crop", "stitch"):
        return f"`visual_plan.action` 必须是 none/reuse/crop/stitch 之一，当前为 {action!r}"
    if action != "none" and not isinstance(vp.get("items"), list):
        return f"当 action={action!r} 时，`visual_plan.items` 必须是数组"
    return None


def _normalize_visual_plan_from_output(parsed: Dict[str, Any], image_mode: str = "select_only") -> Dict[str, Any]:
    """
    将模型输出统一归一化为视觉执行计划。
    默认 `image_mode == "select_only"` 时，严格只做真实原图直接挑选（`reuse` 或 `none`），强制禁用任何裁剪、拼接或去水印操作。
    """
    select_only = str(image_mode or "select_only").strip().lower() == "select_only"

    if isinstance(parsed.get("selected_image_ids"), list):
        clean_ids: List[str] = []
        for raw_id in parsed.get("selected_image_ids") or []:
            s = str(raw_id or "").strip()
            if s and s not in clean_ids:
                clean_ids.append(s)
        reason = str(parsed.get("image_selection_reason") or parsed.get("reason") or "").strip()
        return {
            "action": "reuse" if clean_ids else "none",
            "items": [{"image_id": iid, "crop_box": None, "mask_boxes": []} for iid in clean_ids],
            "reason": reason,
        }

    vp = parsed.get("visual_plan") if isinstance(parsed.get("visual_plan"), dict) else {"action": "none", "items": []}
    if not select_only:
        return vp

    clean_ids = []
    for it in (vp.get("items") or []):
        if isinstance(it, dict):
            iid = str(it.get("image_id") or "").strip()
            if iid and iid not in clean_ids:
                clean_ids.append(iid)
    return {
        "action": "reuse" if clean_ids else "none",
        "items": [{"image_id": iid, "crop_box": None, "mask_boxes": []} for iid in clean_ids],
        "reason": str(vp.get("reason") or "").strip(),
    }


def assemble_creation_materials(
    post_keys: Sequence[str],
    db: Optional[PipelineDB] = None,
    direction_hint: str = "",
    raw_map: Optional[Dict[str, Dict[str, Any]]] = None,
    digest_map: Optional[Dict[str, Dict[str, Any]]] = None,
    max_raw_comments_per_post: int = 15,
) -> Dict[str, Any]:
    """
    独立素材包组装器：根据任意一组 `post_keys` 提取并拼装完整无损的创作素材包：
    - 单帖多模态理解摘要（若存在）
    - 原帖完整标题与正文
    - 外链提取原文正文（若存在，保留核心细节）
    - `harvested_social_posts` 中的**最新原始前排高赞评论** + 单帖理解提炼评论
    - 全部真实有效的本地图片（`IMG_1 .. IMG_N` 与附件顺序严格 1:1 对齐，并标明图片归属来源）
    """
    clean_keys = [str(k).strip() for k in (post_keys or []) if str(k).strip()]
    if (raw_map is None or digest_map is None) and clean_keys:
        db = db or PipelineDB()
        if raw_map is None:
            raw_map = db.fetch_raw_posts_by_keys(clean_keys)
        if digest_map is None:
            digest_map = db.fetch_digests_by_keys(clean_keys)

    effective_raw = raw_map or {}
    effective_dig = digest_map or {}

    source_blocks: List[str] = []
    candidate_image_map: Dict[str, Dict[str, Any]] = {}
    image_manifest_lines: List[str] = []
    attachment_paths: List[str] = []
    seen_img_paths = set()

    for idx, pk in enumerate(clean_keys, 1):
        raw_p = effective_raw.get(pk) or {}
        dig = effective_dig.get(pk) or {}
        src_name = raw_p.get("source") or dig.get("source") or "unknown"
        author_obj = raw_p.get("author") or {}
        author_lbl = author_obj.get("author_name") or author_obj.get("username") or dig.get("author_name") or "匿名"

        clean_raw_text = get_unified_post_text(raw_p)
        dig_summary = str(dig.get("summary") or "").strip()
        val_comments = [str(c).strip() for c in (dig.get("valuable_comments") or []) if str(c).strip()]

        # 直接读取 `harvested_social_posts` 的最新原始评论（即使后续复抓新增了神评且未重跑 Step 2，此处也能 100% 拿到）
        raw_comments = [c for c in (raw_p.get("comments") or []) if isinstance(c, dict)]
        raw_comments.sort(key=lambda x: int(x.get("likes") or 0), reverse=True)
        latest_comment_lines: List[str] = []
        for c in raw_comments[:max_raw_comments_per_post]:
            c_txt = strip_inline_media_placeholders(str(c.get("content") or ""))
            if not c_txt:
                continue
            c_user = c.get("author_name") or c.get("author_uid") or "网友"
            c_likes = int(c.get("likes") or 0)
            like_str = f" | 赞:{c_likes}" if c_likes > 0 else ""
            latest_comment_lines.append(f"  - [{c_user}{like_str}] {c_txt}")

        block = [f"### 素材源 #{idx} (来源平台: {src_name} | 作者: {author_lbl})"]
        if dig_summary:
            block.append(f"- 多模态理解还原:\n{dig_summary}")
        if clean_raw_text:
            block.append(f"- 原帖全文:\n{clean_raw_text}")
        if val_comments:
            block.append("- 理解阶段提炼的高价值评论要点:\n  - " + "\n  - ".join(val_comments))
        if latest_comment_lines:
            block.append("- 最新抓取原始前排评论:\n" + "\n".join(latest_comment_lines))

        source_blocks.append("\n".join(block))

        # 收集该帖的全部本地真实原图（若有理解档案优先用带客观描述的 images，否则直接从 raw_post 提取）
        img_entries = dig.get("images") or dig.get("local_images") or extract_ordered_local_images(raw_p)
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
            attachment_paths.append(lp)
            img_id = f"IMG_{len(attachment_paths)}"
            has_ui = bool(img_item.get("has_ui_or_watermark", False))
            origin = str(img_item.get("origin") or "post")
            origin_lbl = str(img_item.get("origin_label") or ORIGIN_DISPLAY_NAMES.get(origin, "原帖配图"))
            img_desc = str(img_item.get("desc") or origin_lbl).strip()

            candidate_image_map[img_id] = {
                "image_id": img_id,
                "from_post_key": pk,
                "origin": origin,
                "origin_label": origin_lbl,
                "local_path": lp,
                "width": w,
                "height": h,
                "desc": img_desc,
                "has_ui_or_watermark": has_ui,
            }
            ui_hint = "四周带有软件界面栏或角标水印" if has_ui else "画面边缘较干净"
            image_manifest_lines.append(
                f"- `{img_id}` (对应第 {len(attachment_paths)} 张上传附件 | 归属: {origin_lbl} | 尺寸 {w}x{h} | 边缘状态: {ui_hint}): {img_desc}"
            )

    images_manifest_str = (
        "\n".join(image_manifest_lines)
        if image_manifest_lines
        else "（本组素材无关联本地原图，`selected_image_ids` 请填 `[]`）"
    )

    clean_hint = str(direction_hint or "").strip()
    return {
        "direction_hint": clean_hint,
        "post_keys": clean_keys,
        "source_materials_text": "\n\n".join(source_blocks) if source_blocks else "（无可用文本素材）",
        "image_manifest_text": images_manifest_str,
        "candidate_image_map": candidate_image_map,
        "attachment_paths": attachment_paths,
    }


def _build_prompt_slots_from_bundle(
    materials_bundle: Dict[str, Any],
    style_guidelines: str,
    revision_context: str = "",
) -> Dict[str, str]:
    now_utc_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    hint = str(materials_bundle.get("direction_hint") or "").strip()
    if hint:
        topic_info = (
            f"- 当前系统基准时间: {now_utc_str}\n"
            f"- 目标议题范围标识（仅用于锁定所属核心事件范围；具体事实定性、叙事主线与表达语态一律以原始素材实证及风格基调为准，严禁照搬任何审稿提纲式措辞）: {hint}"
        )
    else:
        topic_info = (
            f"- 当前系统基准时间: {now_utc_str}\n"
            "- 目标议题范围标识: （未预设固定范围，请直接提炼以下素材最具价值的核心事实与主线进行原创表达）"
        )
    return {
        "STYLE_GUIDELINES": style_guidelines,
        "TOPIC_INFO": topic_info,
        "SOURCE_MATERIALS": str(materials_bundle.get("source_materials_text") or ""),
        "AVAILABLE_IMAGES_MANIFEST": str(materials_bundle.get("image_manifest_text") or ""),
        "REVISION_CONTEXT": revision_context,
    }


def build_revision_context_block(
    language: str,
    prev_post_text: str,
    prev_visual_plan: Dict[str, Any],
    reader_takeaway: str,
    reader_feedback: Sequence[str],
) -> str:
    """
    构造零背景网友盲测反馈定点打磨指令（纯抽象三大铁律：外科手术消障保语感、零幕后元叙述、图文变更原子级同步）。
    """
    fb_lines = "\n".join(f"- {str(x).strip()}" for x in (reader_feedback or []) if str(x).strip())
    prev_ids = [
        str(it.get("image_id") or "").strip()
        for it in ((prev_visual_plan or {}).get("items") or [])
        if isinstance(it, dict) and str(it.get("image_id") or "").strip()
    ]
    prev_ids_json = json.dumps(prev_ids, ensure_ascii=False)

    if str(language).lower().startswith("en"):
        return (
            "\n## 4. Cold-Reader Blind Review Feedback (Targeted Polish)\n"
            "An ordinary timeline reader with zero prior context read your previous draft and reported:\n"
            f"- Reader's first-read takeaway: {reader_takeaway}\n"
            f"- Specific blocking friction points:\n{fb_lines}\n"
            f"- Previous draft `post_text`:\n{prev_post_text}\n"
            f"- Previous draft `selected_image_ids`: {prev_ids_json}\n\n"
            "Mandatory Abstract Revision Rules:\n"
            "1) Surgical Clarity Fix & Voice Preservation: Resolve the exact comprehension, scope, or attribution friction points grounded strictly in verified facts from the source bundle, while preserving the sharp opening tension and authentic human creator voice of the draft—never flatten vivid public storytelling into a dry, defensive corporate bulletin.\n"
            "2) Zero Meta-Audit Leakage: If a reader question involves an unexplained visual element or tangential detail unsupported by the source bundle, never insert meta-editorial explanations about source limitations or verification boundaries; instead, adjust `selected_image_ids` or trim the unsupported tangent cleanly so the post reads effortlessly on its own.\n"
            "3) Atomic Text-Image Synchronization: Whenever `selected_image_ids` is changed, reordered, or emptied compared to the previous draft, you MUST audit the entire `post_text` line by line and rewrite or remove every reference to any replaced or dropped chart, visual scope, or image detail so `post_text` and the final `selected_image_ids` match 100%.\n"
            "4) Output the complete polished JSON (`post_text`, `selected_image_ids`, `image_selection_reason`).\n"
        )

    return (
        "\n## 4. 零背景普通网友盲测反馈（请执行定点打磨）\n"
        "一位完全没看过原始素材的普通网友阅读了上一版初稿后，给出了如下真实理解与阅读硬伤反馈：\n"
        f"- 网友第一遍读懂的意思：{reader_takeaway}\n"
        f"- 网友指出的具体硬伤：\n{fb_lines}\n"
        f"- 上一版初稿正文 (`post_text`)：\n{prev_post_text}\n"
        f"- 上一版选用原图 (`selected_image_ids`)：{prev_ids_json}\n\n"
        "定点打磨三大铁律：\n"
        "1) 【外科手术式消障与原生锐度保持】：严格基于上方原始素材中的真实事实，精准补齐缺失的主体身份背景、校准统计时间或影响范围口径；同时完整保留初稿鲜活利落的公众创作者语感与开篇张力，严禁因修补局部问题而退化为干瘪保守的公文通报腔。\n"
        "2) 【零幕后审稿元叙述】：若网友疑问涉及原始素材未覆盖的画面细枝末节或次要旁支，严禁在正文中向读者解释素材局限、核查过程或定性边界；正确做法是直接调整 `selected_image_ids` 配图选择或果断删去悬空的旁支线索，让成稿浑然一体、干净自洽。\n"
        "3) 【图文变更原子级同步】：凡本次打磨更换、增删或清空了 `selected_image_ids`，必须逐段通读并同步改写或彻底删去 `post_text` 中所有指向旧配图画面、旧统计图表或旧视觉细节的表述，确保打磨后的正文与最终选定的 `selected_image_ids` 严丝合缝、零脱节。\n"
        "4) 输出打磨后的完整 JSON（`post_text`、`selected_image_ids`、`image_selection_reason`）。\n"
    )


def create_from_materials(
    materials_bundle: Dict[str, Any],
    profile_name: str = ACTIVE_PROFILE,
    creator_cfg: Optional[Dict[str, Any]] = None,
    run_id: str = "manual_create",
    await_blind_review: bool = False,
    metadata_extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    模块 6 核心纯契约入口：仅接收 `materials_bundle`，独立产出原创正文与真实像素编辑配图。
    """
    cfg = dict(CREATOR_CONFIG)
    if creator_cfg:
        cfg.update(creator_cfg)

    profile_cfg = CONTENT_PROFILES.get(profile_name) or CONTENT_PROFILES[ACTIVE_PROFILE]
    prompt_file = str(profile_cfg.get("creation_prompt_file") or profile_cfg.get("step4_prompt_file") or "step4_creation_zh_viral.md")

    creation_id = f"work_{int(time.time())}_{uuid4().hex[:8]}"
    candidate_image_map = materials_bundle.get("candidate_image_map") or {}
    attachment_paths = list(materials_bundle.get("attachment_paths") or [])
    post_keys = list(materials_bundle.get("post_keys") or [])
    direction_hint = str(materials_bundle.get("direction_hint") or "").strip()

    slots = _build_prompt_slots_from_bundle(
        materials_bundle=materials_bundle,
        style_guidelines=str(profile_cfg.get("style_guidelines") or ""),
    )

    rendered_prompt, prompt_meta = render_prompt_template(prompt_file, slots)
    parsed, llm_trace = call_llm_json(
        prompt=rendered_prompt,
        prompt_meta=prompt_meta,
        model=cfg.get("model"),
        fallback_models=cfg.get("fallback_models"),
        file_paths=attachment_paths,
        timeout=int(cfg.get("timeout", 600)),
        preset_model_group=str(cfg.get("preset_model_group") or "medium"),
        validator=_validate_creator_json,
    )

    now_ts = int(time.time())
    extra = metadata_extra or {}
    base_doc: Dict[str, Any] = {
        "creation_id": creation_id,
        "run_id": run_id,
        "direction_id": extra.get("direction_id"),
        "topic_id": extra.get("topic_id") or extra.get("direction_id"),
        "direction_summary": direction_hint,
        "topic_summary": direction_hint,
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
                "error": llm_trace.get("parse_error") or "创作模型调用失败",
                "post_text": "",
                "char_count": 0,
                "visual_plan": {},
                "visual_trace": {},
                "final_image_paths": [],
                "final_image_path": None,
                "candidate_images_count": len(candidate_image_map),
            }
        )
        return base_doc

    post_text = str(parsed.get("post_text") or "").strip()
    image_mode = str(cfg.get("image_mode") or "select_only")
    visual_plan = _normalize_visual_plan_from_output(parsed, image_mode=image_mode)

    visual_trace: Dict[str, Any] = {
        "requested_action": visual_plan.get("action"),
        "executed_action": "none",
        "final_image_paths": [],
        "final_image_path": None,
    }
    if cfg.get("enable_visual_studio", True) and post_text:
        visual_trace = produce_visual_asset(
            creation_id=creation_id,
            visual_plan=visual_plan,
            candidate_image_map=candidate_image_map,
        )

    final_image_paths = list(visual_trace.get("final_image_paths") or [])
    base_doc.update(
        {
            "status": "ok" if post_text else "failed",
            "post_text": post_text,
            "char_count": len(post_text),
            "visual_plan": visual_plan,
            "visual_trace": visual_trace,
            "final_image_paths": final_image_paths,
            "final_image_path": visual_trace.get("final_image_path"),
            "candidate_images_count": len(candidate_image_map),
        }
    )
    return base_doc


def revise_creation_with_feedback(
    work_doc: Dict[str, Any],
    blind_review_round1: Dict[str, Any],
    materials_bundle: Dict[str, Any],
    profile_name: str = ACTIVE_PROFILE,
    creator_cfg: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, Dict[str, Any]]:
    """根据盲测反馈与原始 `materials_bundle` 执行单次定点打磨，并保留 `initial_draft` 快照。"""
    cfg = dict(CREATOR_CONFIG)
    if creator_cfg:
        cfg.update(creator_cfg)

    profile_cfg = CONTENT_PROFILES.get(profile_name) or CONTENT_PROFILES[ACTIVE_PROFILE]
    prompt_file = str(profile_cfg.get("creation_prompt_file") or profile_cfg.get("step4_prompt_file") or "step4_creation_zh_viral.md")
    language = str(profile_cfg.get("language") or work_doc.get("language") or "zh")
    creation_id = str(work_doc.get("creation_id") or "")

    rev_ctx = build_revision_context_block(
        language=language,
        prev_post_text=str(work_doc.get("post_text") or ""),
        prev_visual_plan=work_doc.get("visual_plan") or {},
        reader_takeaway=str(blind_review_round1.get("reader_takeaway") or ""),
        reader_feedback=blind_review_round1.get("reader_feedback") or [],
    )
    slots = _build_prompt_slots_from_bundle(
        materials_bundle=materials_bundle,
        style_guidelines=str(profile_cfg.get("style_guidelines") or ""),
        revision_context=rev_ctx,
    )

    rendered_prompt, prompt_meta = render_prompt_template(prompt_file, slots)
    parsed, rev_llm_trace = call_llm_json(
        prompt=rendered_prompt,
        prompt_meta=prompt_meta,
        model=cfg.get("model"),
        fallback_models=cfg.get("fallback_models"),
        file_paths=list(materials_bundle.get("attachment_paths") or []),
        timeout=int(cfg.get("timeout", 600)),
        preset_model_group=str(cfg.get("preset_model_group") or "medium"),
        validator=_validate_creator_json,
    )

    work_doc["revision_llm_trace"] = rev_llm_trace
    if parsed is None:
        return False, work_doc

    new_post_text = str(parsed.get("post_text") or "").strip()
    if not new_post_text:
        return False, work_doc

    work_doc["initial_draft"] = {
        "post_text": work_doc.get("post_text"),
        "char_count": work_doc.get("char_count"),
        "visual_plan": work_doc.get("visual_plan"),
        "visual_trace": work_doc.get("visual_trace"),
        "final_image_paths": work_doc.get("final_image_paths"),
    }

    image_mode = str(cfg.get("image_mode") or "select_only")
    new_visual_plan = _normalize_visual_plan_from_output(parsed, image_mode=image_mode)
    new_visual_trace: Dict[str, Any] = {
        "requested_action": new_visual_plan.get("action"),
        "executed_action": "none",
        "final_image_paths": [],
        "final_image_path": None,
    }
    if cfg.get("enable_visual_studio", True):
        new_visual_trace = produce_visual_asset(
            creation_id=f"{creation_id}_rev1",
            visual_plan=new_visual_plan,
            candidate_image_map=materials_bundle.get("candidate_image_map") or {},
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


def run_content_creation_batch(
    curated_directions: Optional[Sequence[Dict[str, Any]]] = None,
    db: Optional[PipelineDB] = None,
    profile_name: str = ACTIVE_PROFILE,
    creator_cfg: Optional[Dict[str, Any]] = None,
    run_id: str = "manual_create_batch",
    await_blind_review: Optional[bool] = None,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    批量将已备好素材列表（`curated_post_keys` 或 `post_keys`）的任务组装为独立素材包并并发创作入库。
    """
    logger = get_pipeline_logger()
    db = db or PipelineDB()
    cfg = dict(CREATOR_CONFIG)
    if creator_cfg:
        cfg.update(creator_cfg)

    if await_blind_review is None:
        await_blind_review = bool(STAGE_SWITCHES.get("step7_blind_review", True))

    max_creations = int(cfg.get("max_creations_per_run", 5))
    max_raw_cmts = int(cfg.get("max_raw_comments_per_post", 15))

    if curated_directions is not None:
        targets = [
            d for d in curated_directions
            if (d.get("curated_post_keys") or d.get("post_keys"))
        ][:max_creations]
    else:
        targets = db.fetch_directions_by_status(status="pending_creation", limit=max_creations)

    t0 = time.perf_counter()
    if not targets:
        logger.info("[Module6/创作跳过] 当前没有待创作的精选素材包。")
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

    all_keys: List[str] = []
    for t in targets:
        for pk in (t.get("curated_post_keys") or t.get("post_keys") or []):
            if pk not in all_keys:
                all_keys.append(pk)

    raw_map = db.fetch_raw_posts_by_keys(all_keys)
    digest_map = db.fetch_digests_by_keys(all_keys)

    model_label = cfg.get("model") or f"group:{cfg.get('preset_model_group', 'medium')}"
    logger.info(
        f"[Module6/解耦创作启动] 任务数={len(targets)}个 | Profile={profile_name} | 路由={model_label}"
    )

    def _run_one(item: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        keys = list(item.get("curated_post_keys") or item.get("post_keys") or [])
        hint = str(item.get("direction_summary") or item.get("topic_summary") or "").strip()
        bundle = assemble_creation_materials(
            post_keys=keys,
            db=db,
            direction_hint=hint,
            raw_map=raw_map,
            digest_map=digest_map,
            max_raw_comments_per_post=max_raw_cmts,
        )
        work = create_from_materials(
            materials_bundle=bundle,
            profile_name=profile_name,
            creator_cfg=cfg,
            run_id=run_id,
            await_blind_review=bool(await_blind_review),
            metadata_extra={
                "direction_id": item.get("direction_id"),
                "topic_id": item.get("topic_id") or item.get("direction_id"),
            },
        )
        return item, work

    creations: List[Dict[str, Any]] = []
    workers = max(1, min(int(cfg.get("concurrency", 3)), len(targets)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = [pool.submit(_run_one, t) for t in targets]
        for fut in as_completed(futs):
            t_item, work_doc = fut.result()
            creations.append(work_doc)
            if work_doc.get("status") == "ok":
                t_item["status"] = "completed"
                t_item["creation_id"] = work_doc["creation_id"]
            else:
                t_item["status"] = "creation_failed"

    rank_order = {(t.get("direction_id") or t.get("topic_id")): t.get("rank", 99) for t in targets}
    creations.sort(key=lambda c: rank_order.get(c.get("direction_id") or c.get("topic_id"), 99))

    db.upsert_creations(creations)
    if all(t.get("direction_id") for t in targets):
        db.upsert_directions(list(targets))

    elapsed = round(time.perf_counter() - t0, 2)
    ok_works = [c for c in creations if c.get("status") == "ok"]
    failed_works = [c for c in creations if c.get("status") != "ok"]

    summary = {
        "status": "ok" if ok_works else "failed",
        "elapsed_seconds": elapsed,
        "target_count": len(targets),
        "created_count": len(ok_works),
        "failed_count": len(failed_works),
        "creations_preview": [
            {
                "creation_id": c["creation_id"],
                "direction_id": c.get("direction_id"),
                "direction_summary": c.get("direction_summary"),
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
        f"[Module6/解耦创作完成] 耗时={elapsed:.2f}s | 初稿产出={len(ok_works)}/{len(targets)}篇 | "
        + " ; ".join(
            f"[候选图:{c.get('candidate_images_count', 0)}张 -> 视觉:{(c.get('visual_trace') or {}).get('executed_action')} | {c.get('char_count')}字] "
            f"{((c.get('post_text') or '').splitlines()[0][:36])}"
            for c in ok_works
        )
    )
    return summary, creations
