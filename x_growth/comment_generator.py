# -*- coding: utf-8 -*-
"""
模块 1B：纯查库 15 条/波循环生成评论与独立盲测打分 (`x_growth/comment_generator.py`)

高内聚 · 低耦合原则：
1. **唯一职责**：只从 `harvested_social_posts` 读取满足客观条件（`<24h + 蓝V + 100~3000粉 + 英/日 + 非视频 + 有纯文本 + 未打分`）的原创主帖，生成同语种评论并盲测打分落库；
2. **零浏览器耦合**：100% 物理隔离 Playwright / 浏览器目录，不发起任何网络爬取；
3. **15 条/波循环清零（0 积压浪费）**：
   每波取最多 `15` 条合格待评帖（内部按 5 条/批调 `low` 模型生成 + 6 条/批调 `medium` 模型独立盲评），
   每波完成后立即写入数据库保存进度，并自动继续下一波，**直至库内 `<24h` 合格待评帖 100% 清零**；
4. **支持独立直接运行**：在 IDE 中直接右键运行本文件，无需打开浏览器即可一次性清空库内所有待评合格帖。
"""
import json
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from pymongo import UpdateOne

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.mongo_db.mongo_base import gen_db_object
from content_pipeline.comment_creator import calc_x_weighted_length
from content_pipeline.llm_gateway import call_llm_json, render_prompt_template
from content_pipeline.pipeline_db import is_video_post
from content_pipeline.step2_comprehension import get_unified_post_text
from x_growth.growth_config import (
    AUTO_COMMENT_FIELD,
    GENERATOR_DEFAULTS,
    POST_CATEGORIES,
    POSTS_COLLECTION_NAME,
    SUPPORTED_COMMENT_LANGUAGES,
    TZ_CN,
    X_GROWTH_PROMPT_DIR,
    format_category_list_for_prompt,
    get_configured_self_handles,
    get_growth_logger,
    normalize_language_code,
    now_iso_cn,
)

_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
_INLINE_MEDIA_TAG_RE = re.compile(r"\[插图:[^\]]*\]")
_HASHTAG_RE = re.compile(r"(?:^|\s)#[\w\u4e00-\u9fff]+", re.UNICODE)
_LANG_CODE_RE = re.compile(r"^[a-z]{2,3}$")
_X_UNDETERMINED_LANGS = {"", "unknown", "und", "qme", "qam", "zxx", "art"}
_JP_KANA_RE = re.compile(r"[\u3040-\u30ff]")
_CJK_HAN_RE = re.compile(r"[\u4e00-\u9fff]")
_NON_LATIN_SCRIPT_RE = re.compile(r"[\u0400-\u04ff\u0600-\u06ff\u0900-\u097f\u0e00-\u0e7f\uac00-\ud7af]")
_LATIN_ALPHA_RE = re.compile(r"[A-Za-z]")
_INDEX_ENSURED = False


def strip_urls_and_media_tags(text: str) -> str:
    """剥离所有外部链接与 `[插图: ...]` 标记，还原干净纯文本。"""
    s = _INLINE_MEDIA_TAG_RE.sub(" ", str(text or ""))
    s = _URL_RE.sub(" ", s)
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def has_standalone_pure_text(post: Dict[str, Any]) -> bool:
    """排除纯表情、纯链接或纯晒图无字帖。"""
    pure = strip_urls_and_media_tags(get_unified_post_text(post))
    alnum_chars = re.sub(r"[\s\W_]+", "", pure, flags=re.UNICODE)
    media = post.get("media") or {}
    has_media = bool(media.get("images"))
    if has_media and len(alnum_chars) < 10:
        return False
    return len(alnum_chars) >= 6


def _infer_text_lang_fallback(text: str) -> str:
    if not text:
        return "unknown"
    if _JP_KANA_RE.search(text):
        return "ja"
    if _CJK_HAN_RE.search(text) or _NON_LATIN_SCRIPT_RE.search(text):
        return "other"
    latin_cnt = len(_LATIN_ALPHA_RE.findall(text))
    if latin_cnt >= 12:
        return "en"
    return "unknown"


def is_post_in_allowed_languages(post: Dict[str, Any], allowed_langs_set: Set[str]) -> bool:
    """检查帖子语种是否属于目标语种集合（如 `{"en", "ja"}`）。"""
    if not allowed_langs_set:
        return True
    raw_lang = normalize_language_code(str(((post.get("metadata") or {}).get("lang")) or ""))
    if raw_lang not in _X_UNDETERMINED_LANGS:
        return raw_lang in allowed_langs_set
    pure = strip_urls_and_media_tags(get_unified_post_text(post))
    inferred = _infer_text_lang_fallback(pure)
    return inferred in allowed_langs_set


def ensure_auto_comment_index(db: Any) -> None:
    global _INDEX_ENSURED
    if _INDEX_ENSURED:
        return
    db.create_index(
        POSTS_COLLECTION_NAME,
        [
            ("source", 1),
            (f"{AUTO_COMMENT_FIELD}.consumed", 1),
            (f"{AUTO_COMMENT_FIELD}.language", 1),
            (f"{AUTO_COMMENT_FIELD}.best_score", -1),
            ("publish_time", -1),
        ],
        unique=False,
    )
    _INDEX_ENSURED = True


def _normalize_allowed_languages(allowed_languages: Optional[Sequence[str]]) -> List[str]:
    langs: List[str] = []
    for item in (allowed_languages or SUPPORTED_COMMENT_LANGUAGES):
        code = normalize_language_code(item)
        if code and code != "unknown" and code not in langs:
            langs.append(code)
    return langs or list(SUPPORTED_COMMENT_LANGUAGES)


def _build_language_directive(allowed_languages: Sequence[str]) -> str:
    clean_langs = _normalize_allowed_languages(allowed_languages)
    langs_str = " / ".join(f"`{x}`" for x in clean_langs)
    return (
        f"系统仅针对主体语言属于 {langs_str} 的帖子生成与原帖主体自然语言完全一致的评论；"
        f"若原帖主体语言不属于 {langs_str}，请直接将 `\"skip\"` 设为 `true`，严禁跨语种强行回复。"
    )


def _format_pure_text_post_block(post_ref: str, post: Dict[str, Any], max_comments: int = 8) -> str:
    """严格只组装纯文本信息：账号名称、是否含未展示配图标记、帖子纯文本、已有评论纯文本。"""
    author = post.get("author") or {}
    handle = str(author.get("username") or "").strip().lstrip("@")
    display_name = str(author.get("author_name") or handle).strip()
    account_label = f"@{handle} ({display_name})" if display_name and display_name != handle else f"@{handle}"
    media = post.get("media") or {}
    media_flag = "是" if (bool(media.get("images")) or is_video_post(post)) else "否"

    post_text = strip_urls_and_media_tags(get_unified_post_text(post)) or "（无正文文本）"

    comment_lines: List[str] = []
    for c in (post.get("comments") or []):
        if not isinstance(c, dict):
            continue
        c_txt = strip_urls_and_media_tags(str(c.get("content") or c.get("text") or ""))
        if not c_txt:
            continue
        c_author = str(c.get("author_uid") or c.get("author") or c.get("username") or "").strip().lstrip("@")
        is_post_author = bool(c.get("is_post_author")) or (handle and c_author.lower() == handle.lower())
        prefix = "[原帖作者补充]" if is_post_author else (f"@{c_author}" if c_author else "网友")
        comment_lines.append(f"  - {prefix}: {c_txt}")
        if len(comment_lines) >= max_comments:
            break

    comments_section = "\n".join(comment_lines) if comment_lines else "  （暂无已有评论）"
    return (
        f"### [{post_ref}]\n"
        f"- 账号名称: {account_label}\n"
        f"- 含未展示配图/视频: {media_flag}\n"
        f"- 帖子文本:\n{post_text}\n"
        f"- 相关评论:\n{comments_section}"
    )


def _select_unscored_wave_from_db(
    col: Any,
    *,
    cutoff_ts: int,
    min_followers: int,
    max_followers: int,
    allowed_langs_set: Set[str],
    self_handles: Set[str],
    exclude_pids: Set[str],
    wave_size: int,
    force_rescore: bool,
) -> List[Dict[str, Any]]:
    """
    无截断扫描库内 `<24h + 蓝V + 100~3000粉 + 英/日 + 非视频 + 有独立纯文本 + 未打分` 的原创主帖：
    按 `publish_time` 升序（FIFO，优先处理更早发布、离 24h 过期更近的合格帖）取出最多 `wave_size` 条（默认 15 条）。
    """
    query: Dict[str, Any] = {
        "source": "x",
        "publish_time": {"$gte": cutoff_ts},
        "author.is_verified": True,
        "author.followers_count": {"$gte": int(min_followers), "$lte": int(max_followers)},
    }
    if not force_rescore:
        query[f"{AUTO_COMMENT_FIELD}.scored_at_ts"] = {"$exists": False}
    if exclude_pids:
        query["post_id"] = {"$nin": list(exclude_pids)}

    target_cap = max(1, int(wave_size))
    picked: List[Dict[str, Any]] = []

    cursor = col.find(query, {"_id": 0}).sort("publish_time", 1)
    for doc in cursor:
        meta = doc.get("metadata") or {}
        pextra = meta.get("platform_extra") or {}
        if bool(meta.get("is_retweet") or pextra.get("is_retweet")) or bool(
            meta.get("is_reply") or pextra.get("is_reply")
        ):
            continue
        if is_video_post(doc):
            continue
        if not is_post_in_allowed_languages(doc, allowed_langs_set):
            continue
        if not has_standalone_pure_text(doc):
            continue
        h_low = str((doc.get("author") or {}).get("username") or "").strip().lstrip("@").lower()
        if not h_low or h_low in self_handles:
            continue
        picked.append(doc)
        if len(picked) >= target_cap:
            break

    return picked


def _batch_generate_comments_and_categories(
    posts: Sequence[Dict[str, Any]],
    batch_size: int,
    allowed_languages: Sequence[str],
    max_weighted_chars: int,
    generator_model_group: str,
    timeout: int,
) -> Dict[str, Dict[str, Any]]:
    """按 `batch_size`（默认 5 条/批）纯文本生成目标语种候选评论并标记预定义分类。"""
    logger = get_growth_logger()
    clean_allowed = _normalize_allowed_languages(allowed_languages)
    allowed_langs_set = set(clean_allowed)
    category_list_text = format_category_list_for_prompt()
    lang_directive = _build_language_directive(clean_allowed)
    valid_categories = set(POST_CATEGORIES.keys())
    gen_map: Dict[str, Dict[str, Any]] = {}

    step = max(1, int(batch_size))
    chunks = [(idx, posts[start_idx : start_idx + step]) for idx, start_idx in enumerate(range(0, len(posts), step), 1)]

    def _run_gen_chunk(batch_no: int, chunk: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        ref_to_post: Dict[str, Dict[str, Any]] = {}
        blocks: List[str] = []
        for i, p in enumerate(chunk, 1):
            ref = f"P{i}"
            ref_to_post[ref] = p
            blocks.append(_format_pure_text_post_block(ref, p))

        prompt_text, prompt_meta = render_prompt_template(
            "batch_comment_and_classify.md",
            {
                "CATEGORY_LIST_TEXT": category_list_text,
                "LANGUAGE_DIRECTIVE": lang_directive,
                "BATCH_POSTS_TEXT": "\n\n".join(blocks),
            },
            prompt_dir=X_GROWTH_PROMPT_DIR,
        )

        def _validate_gen(data: Dict[str, Any]) -> Optional[str]:
            items = data.get("results")
            if not isinstance(items, list) or not items:
                return "缺少非空数组 `results`"
            seen_refs: Set[str] = set()
            for idx, item in enumerate(items, 1):
                if not isinstance(item, dict):
                    return f"`results[{idx}]` 必须为对象"
                pref = str(item.get("post_ref") or "").strip().upper()
                if pref not in ref_to_post:
                    return f"`results[{idx}].post_ref` ({pref}) 不在输入编号 {list(ref_to_post.keys())} 内"
                seen_refs.add(pref)
                cat = str(item.get("category") or "").strip()
                if cat not in valid_categories:
                    return f"`results[{idx}].category` ({cat}) 不在预定义分类集合内"
                lang_norm = normalize_language_code(str(item.get("language") or ""))
                if not bool(item.get("skip")):
                    if lang_norm == "unknown" or not _LANG_CODE_RE.match(lang_norm):
                        return f"`results[{idx}].language` ({item.get('language')}) 必须是合法的双字母小写语言代码"
                    if allowed_langs_set and lang_norm not in allowed_langs_set:
                        return f"`results[{idx}].language` ({lang_norm}) 不在允许语种 {sorted(allowed_langs_set)} 内"
                if bool(item.get("skip")):
                    continue
                cmts = item.get("comments")
                if not isinstance(cmts, list) or not cmts:
                    return f"`results[{idx}]` 未标记 skip 时 `comments` 不能为空"
                for c_idx, c_obj in enumerate(cmts, 1):
                    if not isinstance(c_obj, dict):
                        return f"`results[{idx}].comments[{c_idx}]` 必须为对象"
                    txt = str(c_obj.get("reply_text") or "").strip()
                    if not txt:
                        return f"`results[{idx}].comments[{c_idx}].reply_text` 不能为空"
                    if _URL_RE.search(txt):
                        return f"`results[{idx}].comments[{c_idx}]` 严禁包含外部 URL"
                    if _HASHTAG_RE.search(txt):
                        return f"`results[{idx}].comments[{c_idx}]` 严禁包含 #Hashtag"
                    w_len = calc_x_weighted_length(txt)
                    if max_weighted_chars > 0 and w_len > max_weighted_chars:
                        return f"`results[{idx}].comments[{c_idx}]` 加权长度 {w_len} 超过上限 {max_weighted_chars}"
            missing = set(ref_to_post.keys()) - seen_refs
            if missing:
                return f"输出遗漏了帖子编号: {sorted(missing)}"
            return None

        parsed, trace = call_llm_json(
            prompt=prompt_text,
            prompt_meta=prompt_meta,
            model=None,
            file_paths=None,
            timeout=timeout,
            preset_model_group=generator_model_group,
            validator=_validate_gen,
        )
        if not parsed:
            logger.warning(
                f"[CommentGenerator/批量生成失败] 批次 {batch_no}: {trace.get('parse_error') or trace.get('error_history')}"
            )
            return {}

        local_out: Dict[str, Dict[str, Any]] = {}
        gen_model_used = str(trace.get("model_used") or f"group:{generator_model_group}")
        gen_ts = int(time.time())
        for item in parsed.get("results") or []:
            pref = str(item.get("post_ref") or "").strip().upper()
            p_doc = ref_to_post.get(pref)
            if not p_doc:
                continue
            pid = str(p_doc.get("post_id") or "")
            is_skip = bool(item.get("skip"))
            fallback_lang = str(((p_doc.get("metadata") or {}).get("lang")) or "")
            lang_code = normalize_language_code(str(item.get("language") or fallback_lang))
            skip_reason = str(item.get("skip_reason") or "").strip()
            if allowed_langs_set and lang_code not in allowed_langs_set:
                is_skip = True
                skip_reason = skip_reason or f"non_target_language:{lang_code}"
            candidates: List[Dict[str, Any]] = []
            if not is_skip:
                for idx_c, c_obj in enumerate(item.get("comments") or [], 1):
                    r_txt = str(c_obj.get("reply_text") or "").strip().strip('"').strip("“”")
                    if not r_txt:
                        continue
                    c_ref = str(c_obj.get("comment_ref") or f"C{idx_c}").strip().upper()
                    candidates.append(
                        {
                            "comment_ref": c_ref,
                            "comment_text": r_txt,
                            "weighted_length": calc_x_weighted_length(r_txt),
                        }
                    )
            local_out[pid] = {
                "category": str(item.get("category") or "other").strip(),
                "language": lang_code,
                "skip": is_skip or (len(candidates) == 0),
                "skip_reason": skip_reason,
                "candidates": candidates,
                "generator_model": gen_model_used,
                "generated_at_ts": gen_ts,
            }
        return local_out

    with ThreadPoolExecutor(max_workers=min(2, max(1, len(chunks)))) as pool:
        futs = [pool.submit(_run_gen_chunk, b_no, ch) for b_no, ch in chunks]
        for fut in as_completed(futs):
            gen_map.update(fut.result() or {})

    return gen_map


def _batch_score_with_strong_model(
    posts: Sequence[Dict[str, Any]],
    gen_map: Dict[str, Dict[str, Any]],
    score_batch_size: int,
    scorer_model_group: str,
    timeout: int,
) -> None:
    """将候选评论送入独立第三方 `medium` 模型组横向盲测打分。"""
    logger = get_growth_logger()
    scorable_posts = [
        p
        for p in posts
        if str(p.get("post_id") or "") in gen_map
        and not gen_map[str(p.get("post_id") or "")].get("skip")
        and gen_map[str(p.get("post_id") or "")].get("candidates")
    ]
    if not scorable_posts:
        return

    step = max(1, int(score_batch_size))
    chunks = [
        (idx, scorable_posts[start_idx : start_idx + step])
        for idx, start_idx in enumerate(range(0, len(scorable_posts), step), 1)
    ]

    def _run_score_chunk(batch_no: int, chunk: Sequence[Dict[str, Any]]) -> Optional[Tuple[Dict[str, str], Dict[str, Any], str]]:
        ref_to_pid: Dict[str, str] = {}
        expected_crefs_by_pref: Dict[str, Set[str]] = {}
        item_blocks: List[str] = []

        for i, p in enumerate(chunk, 1):
            pref = f"P{i}"
            pid = str(p.get("post_id") or "")
            ref_to_pid[pref] = pid
            base_block = _format_pure_text_post_block(pref, p)
            cands = gen_map[pid]["candidates"]
            expected_crefs_by_pref[pref] = {c["comment_ref"] for c in cands}
            cand_lines = "\n".join(f"  - [{c['comment_ref']}] {c['comment_text']}" for c in cands)
            item_blocks.append(f"{base_block}\n- 待评分候选评论:\n{cand_lines}")

        prompt_text, prompt_meta = render_prompt_template(
            "batch_score_comments.md",
            {"ITEMS_TO_SCORE_TEXT": "\n\n".join(item_blocks)},
            prompt_dir=X_GROWTH_PROMPT_DIR,
        )

        def _validate_score(data: Dict[str, Any]) -> Optional[str]:
            evals = data.get("evaluations")
            if not isinstance(evals, list) or not evals:
                return "缺少非空数组 `evaluations`"
            seen_prefs: Set[str] = set()
            for idx, ev in enumerate(evals, 1):
                if not isinstance(ev, dict):
                    return f"`evaluations[{idx}]` 必须为对象"
                pref = str(ev.get("post_ref") or "").strip().upper()
                if pref not in ref_to_pid:
                    return f"`evaluations[{idx}].post_ref` ({pref}) 不在输入编号内"
                seen_prefs.add(pref)
                scores = ev.get("scores")
                if not isinstance(scores, list) or not scores:
                    return f"`evaluations[{idx}].scores` 不能为空"
                seen_crefs: Set[str] = set()
                for s_idx, s_obj in enumerate(scores, 1):
                    if not isinstance(s_obj, dict):
                        return f"`evaluations[{idx}].scores[{s_idx}]` 必须为对象"
                    cref = str(s_obj.get("comment_ref") or "").strip().upper()
                    if cref not in expected_crefs_by_pref[pref]:
                        return f"`evaluations[{idx}].scores[{s_idx}].comment_ref` ({cref}) 不匹配 {expected_crefs_by_pref[pref]}"
                    seen_crefs.add(cref)
                    raw_sc = s_obj.get("score")
                    if not isinstance(raw_sc, (int, float)) or not (0 <= float(raw_sc) <= 100):
                        return f"`evaluations[{idx}].scores[{s_idx}].score` ({raw_sc}) 必须是 0~100 之间的数值"
                missing_c = expected_crefs_by_pref[pref] - seen_crefs
                if missing_c:
                    return f"`evaluations[{idx}]` 漏评了候选评论: {sorted(missing_c)}"
            missing_p = set(ref_to_pid.keys()) - seen_prefs
            if missing_p:
                return f"打分结果遗漏了帖子编号: {sorted(missing_p)}"
            return None

        parsed, trace = call_llm_json(
            prompt=prompt_text,
            prompt_meta=prompt_meta,
            model=None,
            file_paths=None,
            timeout=timeout,
            preset_model_group=scorer_model_group,
            validator=_validate_score,
        )
        if not parsed:
            logger.warning(
                f"[CommentGenerator/盲测打分失败] 批次 {batch_no}: {trace.get('parse_error') or trace.get('error_history')}"
            )
            return None
        scorer_model_used = str(trace.get("model_used") or f"group:{scorer_model_group}")
        return ref_to_pid, parsed, scorer_model_used

    with ThreadPoolExecutor(max_workers=min(2, max(1, len(chunks)))) as pool:
        futs = [pool.submit(_run_score_chunk, b_no, ch) for b_no, ch in chunks]
        for fut in as_completed(futs):
            res_tuple = fut.result()
            if not res_tuple:
                continue
            ref_to_pid, parsed, scorer_model_used = res_tuple
            scored_ts = int(time.time())
            for ev in parsed.get("evaluations") or []:
                pref = str(ev.get("post_ref") or "").strip().upper()
                pid = ref_to_pid.get(pref)
                if not pid or pid not in gen_map:
                    continue
                score_lookup: Dict[str, Tuple[int, str]] = {}
                for s_obj in ev.get("scores") or []:
                    cref = str(s_obj.get("comment_ref") or "").strip().upper()
                    score_lookup[cref] = (
                        int(round(float(s_obj.get("score") or 0))),
                        str(s_obj.get("reason") or "").strip(),
                    )

                entry = gen_map[pid]
                for c in entry["candidates"]:
                    sc, rsn = score_lookup.get(c["comment_ref"], (0, ""))
                    c["score"] = sc
                    c["score_reason"] = rsn

                preferred_ref = str(ev.get("best_comment_ref") or "").strip().upper()
                entry["candidates"].sort(
                    key=lambda x: (int(x.get("score", 0)), 1 if x.get("comment_ref") == preferred_ref else 0),
                    reverse=True,
                )
                best_cand = entry["candidates"][0]
                entry["best_comment"] = best_cand["comment_text"]
                entry["best_score"] = int(best_cand.get("score", 0))
                entry["best_reason"] = str(best_cand.get("score_reason") or "")
                entry["scorer_model"] = scorer_model_used
                entry["scored_at_ts"] = scored_ts


def _process_and_persist_single_wave(
    col: Any,
    wave_posts: Sequence[Dict[str, Any]],
    *,
    clean_allowed_langs: Sequence[str],
    comment_batch_size: int,
    score_batch_size: int,
    max_weighted_chars: int,
    generator_model_group: str,
    scorer_model_group: str,
    llm_timeout: int,
) -> Dict[str, Any]:
    """处理单波帖子（最多 15 条）：5 帖/批生成 -> 独立模型盲评 -> 立即写入 MongoDB 保存当波成果。"""
    gen_map = _batch_generate_comments_and_categories(
        posts=wave_posts,
        batch_size=comment_batch_size,
        allowed_languages=clean_allowed_langs,
        max_weighted_chars=max_weighted_chars,
        generator_model_group=generator_model_group,
        timeout=llm_timeout,
    )

    _batch_score_with_strong_model(
        posts=wave_posts,
        gen_map=gen_map,
        score_batch_size=score_batch_size,
        scorer_model_group=scorer_model_group,
        timeout=llm_timeout,
    )

    pids = [str(p.get("post_id") or "") for p in wave_posts if p.get("post_id")]
    existing_auto_map: Dict[str, Dict[str, Any]] = {
        str(d.get("post_id")): (d.get(AUTO_COMMENT_FIELD) or {})
        for d in col.find({"source": "x", "post_id": {"$in": pids}}, {"post_id": 1, AUTO_COMMENT_FIELD: 1, "_id": 0})
    }

    bulk_ops: List[UpdateOne] = []
    wave_items: List[Dict[str, Any]] = []
    scored_count = 0
    skipped_count = 0

    for p in wave_posts:
        pid = str(p.get("post_id") or "")
        g_info = gen_map.get(pid)
        if not g_info:
            continue
        if not g_info.get("skip") and "best_score" not in g_info:
            continue

        prev_meta = existing_auto_map.get(pid) or {}
        nested_payload: Dict[str, Any] = {
            "category": g_info.get("category", "other"),
            "language": g_info.get("language", "unknown"),
            "skip": bool(g_info.get("skip", False)),
            "skip_reason": g_info.get("skip_reason", ""),
            "candidates": g_info.get("candidates", []),
            "best_comment": g_info.get("best_comment", ""),
            "best_score": g_info.get("best_score"),
            "best_reason": g_info.get("best_reason", ""),
            "generator_model": g_info.get("generator_model", ""),
            "scorer_model": g_info.get("scorer_model", ""),
            "generated_at_ts": g_info.get("generated_at_ts"),
            "scored_at_ts": g_info.get("scored_at_ts") or g_info.get("generated_at_ts"),
            "updated_at": now_iso_cn(),
            # 多账号单次消费留痕字段（任一运营账号发布成功后置 consumed=True 并写入账号与时间）
            "consumed": bool(prev_meta.get("consumed", False)),
            "consumed_by": prev_meta.get("consumed_by"),
            "consumed_dir": prev_meta.get("consumed_dir"),
            "consumed_at": prev_meta.get("consumed_at"),
            "consumed_at_ts": prev_meta.get("consumed_at_ts"),
            "published_comment_id": prev_meta.get("published_comment_id"),
            "published_comment_text": prev_meta.get("published_comment_text"),
        }

        bulk_ops.append(
            UpdateOne(
                {"source": "x", "post_id": pid},
                {"$set": {AUTO_COMMENT_FIELD: nested_payload}},
            )
        )
        if nested_payload["skip"]:
            skipped_count += 1
        else:
            scored_count += 1
            author_obj = p.get("author") or {}
            wave_items.append(
                {
                    "post_id": pid,
                    "author": f"@{author_obj.get('username', '')}",
                    "followers": int(author_obj.get("followers_count") or 0),
                    "category": nested_payload["category"],
                    "language": nested_payload["language"],
                    "best_score": nested_payload["best_score"],
                    "best_comment": nested_payload["best_comment"],
                    "score_reason": nested_payload["best_reason"],
                    "generator_model": nested_payload["generator_model"],
                    "scorer_model": nested_payload["scorer_model"],
                }
            )

    if bulk_ops:
        col.bulk_write(bulk_ops, ordered=False)

    return {
        "scored_count": scored_count,
        "skipped_count": skipped_count,
        "items": wave_items,
    }


def generate_all_pending_comments(
    *,
    min_followers: int = GENERATOR_DEFAULTS["min_followers"],
    max_followers: int = GENERATOR_DEFAULTS["max_followers"],
    post_max_age_seconds: int = GENERATOR_DEFAULTS["post_max_age_seconds"],
    allowed_languages: Sequence[str] = GENERATOR_DEFAULTS["allowed_languages"],
    wave_size: int = GENERATOR_DEFAULTS["wave_size"],
    max_waves: int = GENERATOR_DEFAULTS["max_waves_per_run"],
    comment_batch_size: int = GENERATOR_DEFAULTS["comment_batch_size"],
    score_batch_size: int = GENERATOR_DEFAULTS["score_batch_size"],
    extra_self_handles: Optional[Sequence[str]] = None,
    force_rescore: bool = GENERATOR_DEFAULTS["force_rescore"],
) -> Dict[str, Any]:
    """
    高内聚纯数据库生成入口（零浏览器依赖）：
    按每波 `wave_size`（默认 15 条）自动循环执行「选帖 -> 生成 -> 盲评 -> 落库」，
    直到数据库内 `<24h` 所有符合客观条件的未打分帖子 **100% 清零**！
    """
    logger = get_growth_logger()
    t0 = time.perf_counter()

    clean_allowed_langs = _normalize_allowed_languages(allowed_languages)
    allowed_langs_set = set(clean_allowed_langs)
    self_handles: Set[str] = set(get_configured_self_handles())
    for h in (extra_self_handles or []):
        h_clean = str(h or "").strip().lstrip("@").lower()
        if h_clean:
            self_handles.add(h_clean)

    db = gen_db_object()
    ensure_auto_comment_index(db)
    col = db.get_collection(POSTS_COLLECTION_NAME)

    attempted_pids: Set[str] = set()
    all_summary_items: List[Dict[str, Any]] = []
    waves_executed = 0
    total_selected = 0
    total_scored = 0
    total_skipped = 0

    eff_wave_size = max(1, int(wave_size))
    eff_max_waves = max(1, int(max_waves))

    while waves_executed < eff_max_waves:
        cutoff_ts = int(time.time()) - int(post_max_age_seconds)
        wave_posts = _select_unscored_wave_from_db(
            col=col,
            cutoff_ts=cutoff_ts,
            min_followers=min_followers,
            max_followers=max_followers,
            allowed_langs_set=allowed_langs_set,
            self_handles=self_handles,
            exclude_pids=attempted_pids,
            wave_size=eff_wave_size,
            force_rescore=force_rescore,
        )
        if not wave_posts:
            break

        waves_executed += 1
        for p in wave_posts:
            pid = str(p.get("post_id") or "")
            if pid:
                attempted_pids.add(pid)

        total_selected += len(wave_posts)
        logger.info(
            f"[CommentGenerator/波次 #{waves_executed}] 本波取出待评合格帖={len(wave_posts)}条 (每波上限={eff_wave_size}条)"
        )

        wave_res = _process_and_persist_single_wave(
            col=col,
            wave_posts=wave_posts,
            clean_allowed_langs=clean_allowed_langs,
            comment_batch_size=comment_batch_size,
            score_batch_size=score_batch_size,
            max_weighted_chars=int(GENERATOR_DEFAULTS["max_weighted_chars"]),
            generator_model_group=str(GENERATOR_DEFAULTS["generator_model_group"]),
            scorer_model_group=str(GENERATOR_DEFAULTS["scorer_model_group"]),
            llm_timeout=int(GENERATOR_DEFAULTS["llm_timeout"]),
        )
        total_scored += int(wave_res["scored_count"])
        total_skipped += int(wave_res["skipped_count"])
        all_summary_items.extend(wave_res["items"])

        logger.info(
            f"[CommentGenerator/波次 #{waves_executed} 落库] 本波有效评论={wave_res['scored_count']}条 | "
            f"缺图上下文跳过={wave_res['skipped_count']}条"
        )

        # 若本波查出的帖子已不足 wave_size，说明库内合格待评帖已全部取尽，无需再做一次空查询
        if len(wave_posts) < eff_wave_size:
            break

    all_summary_items.sort(key=lambda x: int(x.get("best_score") or 0), reverse=True)
    elapsed = round(time.perf_counter() - t0, 2)
    logger.info(
        f"[CommentGenerator/清零完成] 耗时={elapsed}s | 共执行={waves_executed}波 | "
        f"累计处理合格帖={total_selected}条 -> 产出可发评论={total_scored}条 (安全跳过={total_skipped}条)"
    )
    return {
        "status": "ok",
        "waves_executed": waves_executed,
        "target_posts_selected": total_selected,
        "newly_scored_posts": total_scored,
        "skipped_posts": total_skipped,
        "elapsed_seconds": elapsed,
        "items": all_summary_items,
    }


def run_generator_daemon(
    interval_seconds: Optional[int] = None,
    jitter_seconds: Optional[Tuple[int, int]] = None,
) -> None:
    """独立常驻查库清零循环（不开浏览器，定期将数据库内新进的合格帖全部生成并打分清零）。"""
    logger = get_growth_logger()
    base_interval = max(60, int(interval_seconds or GENERATOR_DEFAULTS.get("interval_seconds", 900)))
    raw_jitter = jitter_seconds if jitter_seconds is not None else GENERATOR_DEFAULTS.get("jitter_seconds", (30, 120))
    if isinstance(raw_jitter, (list, tuple)) and len(raw_jitter) == 2:
        j_low, j_high = max(0, int(raw_jitter[0])), max(0, int(raw_jitter[1]))
        if j_low > j_high:
            j_low, j_high = j_high, j_low
    else:
        j_low, j_high = 0, 0

    logger.info(
        f"⏰ [CommentGenerator/独立常驻生成启动] 周期={base_interval}s + 抖动({j_low}~{j_high}s) | "
        f"每波={GENERATOR_DEFAULTS['wave_size']}条循环清零"
    )
    round_no = 0
    while True:
        round_no += 1
        logger.info(f"========== [CommentGenerator/轮次 #{round_no}] 开始扫描待评合格帖 ==========")
        try:
            generate_all_pending_comments(force_rescore=False)
        except KeyboardInterrupt:
            logger.info("[CommentGenerator/常驻退出] 收到手动中断信号，安全停止。")
            break
        except Exception as exc:
            logger.exception(f"❌ [CommentGenerator/轮次 #{round_no} 异常] {exc}")

        sleep_sec = base_interval + (random.randint(j_low, j_high) if j_high > 0 else 0)
        next_run_time = (datetime.now(TZ_CN) + timedelta(seconds=sleep_sec)).strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"💤 [CommentGenerator/休眠] 休眠 {sleep_sec} 秒，下一轮将于 [{next_run_time}] 执行...")
        try:
            time.sleep(sleep_sec)
        except KeyboardInterrupt:
            logger.info("[CommentGenerator/常驻退出] 休眠期间收到中断信号，安全停止。")
            break


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    if GENERATOR_DEFAULTS.get("continuous_loop", False):
        run_generator_daemon(
            interval_seconds=GENERATOR_DEFAULTS.get("interval_seconds", 900),
            jitter_seconds=GENERATOR_DEFAULTS.get("jitter_seconds", (30, 120)),
        )
    else:
        res = generate_all_pending_comments(
            force_rescore=bool(GENERATOR_DEFAULTS.get("force_rescore", False)),
        )
        print(json.dumps(res, ensure_ascii=False, indent=2))
