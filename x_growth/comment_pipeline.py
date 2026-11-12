# -*- coding: utf-8 -*-
"""
模块一：潜力用户挖掘、今日新帖入库、5帖批量纯文本评论生成与独立中模型打分 (`x_growth/comment_pipeline.py`)

完整单向流水线（唯一核心入参：`user_data_dir`）：
1. 用 `user_data_dir` 登录态拉取推荐流 (`For You`) 帖子及其评论区用户；
2. 筛选出【蓝 V 认证 + 粉丝数在设定区间内 + 尚未关注我且我也未关注他】的高潜力回关用户；
3. 拉取每位潜力用户最近 24h 内发布的最新 1 条原创有字帖子（今天没发则直接跳过），拉齐完整文本与评论并存入 `harvested_social_posts`；
4. 仅提取纯文本（账号名称 + 帖子文本 + 相关评论），按 5 条/批调用低模型组（`low`）批量生成评论并从预定义分类中打标；
5. 将本轮全部生成结果打包交给独立的第三方中模型组（`medium`）横向客观打分；
6. 将分类、候选评论、评分结果统一写入 `harvested_social_posts` 文档下的单一嵌套字段 `auto_comment`。
"""
import json
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from pymongo import UpdateOne

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.common_utils import resolve_session_dir, safe_int
from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import HarvestedPostManager
from content_pipeline.comment_creator import calc_x_weighted_length
from content_pipeline.llm_gateway import call_llm_json, render_prompt_template
from content_pipeline.pipeline_db import is_video_post
from content_pipeline.step2_comprehension import get_unified_post_text
from platforms.harvesters.x_harvester import XHarvester
from x_growth.growth_config import (
    AUTO_COMMENT_FIELD,
    PIPELINE_DEFAULTS,
    POST_CATEGORIES,
    POSTS_COLLECTION_NAME,
    SUPPORTED_COMMENT_LANGUAGES,
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
_INDEX_ENSURED = False


def _ensure_auto_comment_index(db) -> None:
    global _INDEX_ENSURED
    if _INDEX_ENSURED:
        return
    db.create_index(
        POSTS_COLLECTION_NAME,
        [
            ("source", 1),
            (f"{AUTO_COMMENT_FIELD}.published", 1),
            (f"{AUTO_COMMENT_FIELD}.language", 1),
            (f"{AUTO_COMMENT_FIELD}.best_score", -1),
            ("publish_time", -1),
        ],
        unique=False,
    )
    _INDEX_ENSURED = True


def _strip_urls(text: str) -> str:
    cleaned = _INLINE_MEDIA_TAG_RE.sub("", str(text or ""))
    cleaned = _URL_RE.sub("", cleaned)
    cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def _post_has_media(post: Dict[str, Any]) -> bool:
    media = post.get("media") or {}
    return bool(media.get("images")) or is_video_post(post)


def _has_standalone_pure_text(post: Dict[str, Any]) -> bool:
    """确定性前置检查：剔除链接与插图占位符后，是否仍包含非空正文文本。"""
    pure_txt = _strip_urls(get_unified_post_text(post))
    return bool(re.sub(r"\s+", "", pure_txt))


def _normalize_allowed_languages(allowed_languages: Optional[Sequence[str]]) -> List[str]:
    langs: List[str] = []
    for item in (allowed_languages or SUPPORTED_COMMENT_LANGUAGES):
        code = normalize_language_code(item)
        if code and code != "unknown" and code not in langs:
            langs.append(code)
    return langs or list(SUPPORTED_COMMENT_LANGUAGES)


def _is_post_in_allowed_languages(post: Dict[str, Any], allowed_langs_set: Set[str]) -> bool:
    """
    基于 X 原始帖子 `metadata.lang` 执行确定性前置语种过滤：
    - 明确属于允许语种（`en`/`ja`）的直接放行；
    - X 未标注具体语种（`und`/`qme`/`qam` 等）的暂放行交由生成模型判定（非 `en`/`ja` 则 `skip: true`）；
    - 明确标注为其他语种（如 `zh`/`es`/`tr`/`hi` 等）的直接拦截，零浪费接口与模型额度。
    """
    if not allowed_langs_set:
        return True
    raw_lang = normalize_language_code(str(((post.get("metadata") or {}).get("lang")) or ""))
    if raw_lang in allowed_langs_set:
        return True
    if raw_lang in _X_UNDETERMINED_LANGS:
        return True
    return False


def _build_language_directive(allowed_languages: Sequence[str]) -> str:
    clean_langs = _normalize_allowed_languages(allowed_languages)
    langs_str = " / ".join(f"`{x}`" for x in clean_langs)
    return (
        f"系统仅针对主体语言属于 {langs_str} 的帖子生成与原帖主体自然语言完全一致的评论；"
        f"若原帖主体语言不属于 {langs_str}，请直接将 `\"skip\"` 设为 `true`，严禁跨语种强行回复。"
    )


def _format_pure_text_post_block(post_ref: str, post: Dict[str, Any], max_comments: int = 8) -> str:
    """
    严格只组装纯文本信息：账号名称、是否含未展示媒体标记、帖子纯文本、相关评论纯文本（不含链接或互动数字）。
    """
    author = post.get("author") or {}
    handle = str(author.get("username") or "").strip().lstrip("@")
    display_name = str(author.get("author_name") or handle).strip()
    account_label = f"@{handle} ({display_name})" if display_name and display_name != handle else f"@{handle}"
    media_flag = "是" if _post_has_media(post) else "否"

    post_text = _strip_urls(get_unified_post_text(post)) or "（无正文文本）"

    raw_comments = post.get("comments") or []
    comment_lines: List[str] = []
    for c in raw_comments:
        if not isinstance(c, dict):
            continue
        c_txt = _strip_urls(str(c.get("content") or c.get("text") or ""))
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


def _discover_qualified_users(
    harvester: XHarvester,
    feed_fetch_count: int,
    detail_concurrency: int,
    min_followers: int,
    max_followers: int,
    self_uid: str,
    self_handles: Set[str],
) -> Tuple[List[Dict[str, Any]], int, Dict[str, Dict[str, Any]]]:
    """
    阶段 1：拉取推荐流 (For You) 帖子 -> 温和拉取评论区 -> 提取作者与评论区用户 ->
    筛选【认证蓝V + 粉丝数在区间内 + 我未关注他且他也未关注我】的高潜力用户。
    """
    feed_posts = harvester.fetch_hot_or_recommended(count=max(10, feed_fetch_count), latest_following=False)
    posts_with_replies = [
        p for p in feed_posts if safe_int((p.get("engagement") or {}).get("comment_count"), 0) > 0
    ][:8]

    enriched_by_id: Dict[str, Dict[str, Any]] = {str(p.get("post_id")): p for p in feed_posts if p.get("post_id")}

    if posts_with_replies:
        def _pull_detail(p_obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            time.sleep(random.uniform(0.35, 0.75))
            tid = str(p_obj.get("post_id") or "")
            uname = str((p_obj.get("author") or {}).get("username") or "")
            return harvester.fetch_tweet_with_comments(tid, screen_name=uname, max_comments=20)

        with ThreadPoolExecutor(max_workers=max(1, min(detail_concurrency, len(posts_with_replies)))) as pool:
            futs = {pool.submit(_pull_detail, p): str(p.get("post_id")) for p in posts_with_replies}
            for fut in as_completed(futs):
                pid = futs[fut]
                try:
                    det = fut.result()
                    if det:
                        enriched_by_id[pid] = det
                except Exception:
                    pass

    # 收集唯一候选账号（优先纳入正在评论区活跃发言的用户，其次推荐帖作者）
    raw_users: Dict[str, Dict[str, Any]] = {}

    def _collect(prof: Optional[Dict[str, Any]], source_role: str) -> None:
        if not isinstance(prof, dict):
            return
        handle = str(prof.get("screen_name") or "").strip().lstrip("@")
        if not handle or handle.lower() == "unknown":
            return
        h_low = handle.lower()
        uid = str(prof.get("user_id") or "").strip()
        if h_low in self_handles or (self_uid and uid == self_uid):
            return
        if h_low not in raw_users:
            entry = dict(prof)
            entry["source_role"] = source_role
            raw_users[h_low] = entry

    for p in enriched_by_id.values():
        pextra = ((p.get("metadata") or {}).get("platform_extra")) or {}
        for c_prof in (pextra.get("comment_authors") or []):
            _collect(c_prof, "commenter")

    for p in enriched_by_id.values():
        pextra = ((p.get("metadata") or {}).get("platform_extra")) or {}
        _collect(pextra.get("author_profile"), "post_author")

    # 若少数蓝 V 节点缺失粉丝计数或关系字段，按需温和补齐精确画像
    need_profile_handles = [
        h
        for h, u in raw_users.items()
        if bool(u.get("is_blue_verified") or u.get("is_verified"))
        and (
            (safe_int(u.get("followers_count"), 0) == 0 and safe_int(u.get("following_count"), 0) == 0)
            or ("is_following" not in u)
            or ("is_followed_by" not in u)
        )
    ][:12]
    for h in need_profile_handles:
        try:
            time.sleep(random.uniform(0.25, 0.5))
            det = harvester.fetch_user_profile_detail(raw_users[h]["screen_name"])
            if det:
                det["source_role"] = raw_users[h].get("source_role", "")
                raw_users[h] = det
        except Exception:
            pass

    qualified: List[Dict[str, Any]] = []
    for u in raw_users.values():
        is_blue = bool(u.get("is_blue_verified") if "is_blue_verified" in u else u.get("is_verified"))
        if not is_blue:
            continue
        followers = safe_int(u.get("followers_count"), 0)
        if followers < min_followers or followers > max_followers:
            continue
        # 过滤掉已经关注了的人 (is_following=True) 以及已经关注我的人 (is_followed_by=True)
        if bool(u.get("is_following")) or bool(u.get("is_followed_by")):
            continue
        qualified.append(u)

    return qualified, len(feed_posts), enriched_by_id


def _build_rel_snapshot(u_prof: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "screen_name": str(u_prof.get("screen_name") or ""),
        "user_id": str(u_prof.get("user_id") or ""),
        "followers_count": safe_int(u_prof.get("followers_count"), 0),
        "following_count": safe_int(u_prof.get("following_count"), 0),
        "is_blue_verified": True,
        "is_following": bool(u_prof.get("is_following")),
        "is_followed_by": bool(u_prof.get("is_followed_by")),
        "source_role": str(u_prof.get("source_role") or ""),
        "checked_at_ts": int(time.time()),
    }


def _fetch_latest_today_posts_for_users(
    harvester: XHarvester,
    users: Sequence[Dict[str, Any]],
    cutoff_ts: int,
    allowed_langs_set: Set[str],
    feed_posts_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """
    阶段 2：温和串行拉取每位潜力用户最近 24h 内的最新 1 条原创有效目标语种主帖，并补全完整帖子信息与评论。
    - 仅保留目标语种（默认 `en`/`ja`）帖子；
    - 若推荐流本身已命中该用户的 24h 内原创有效帖，直接复用免除额外请求；
    - 一旦检测到账号触发 X `code=326` 安全挑战，立即熔断停止请求，保护账号安全。
    """
    selected_posts: List[Dict[str, Any]] = []
    user_rel_by_post_id: Dict[str, Dict[str, Any]] = {}
    if not users:
        return [], {}

    feed_latest_by_author: Dict[str, Dict[str, Any]] = {}
    for fp in (feed_posts_map or {}).values():
        if safe_int(fp.get("publish_time"), 0) < cutoff_ts:
            continue
        if bool((fp.get("metadata") or {}).get("is_retweet")):
            continue
        if not _is_post_in_allowed_languages(fp, allowed_langs_set):
            continue
        if not _has_standalone_pure_text(fp):
            continue
        ah = str((fp.get("author") or {}).get("username") or "").strip().lstrip("@").lower()
        if not ah:
            continue
        prev = feed_latest_by_author.get(ah)
        if not prev or safe_int(fp.get("publish_time"), 0) > safe_int(prev.get("publish_time"), 0):
            feed_latest_by_author[ah] = fp

    seen_pids: Set[str] = set()
    for idx, u_prof in enumerate(users):
        handle = str(u_prof.get("screen_name") or "").strip().lstrip("@")
        if not handle:
            continue
        h_low = handle.lower()

        chosen: Optional[Dict[str, Any]] = feed_latest_by_author.get(h_low)
        if chosen is None:
            if getattr(harvester, "_burner_locked_326", False):
                break
            if idx > 0:
                time.sleep(random.uniform(0.65, 1.25))
            recent_posts = harvester.fetch_user_posts(
                handle,
                count=8,
                exclude_retweets=True,
                exclude_replies=True,
                force_burner=True,
                known_uid=str(u_prof.get("user_id") or ""),
            )
            if getattr(harvester, "_burner_locked_326", False) and not recent_posts:
                break
            if not recent_posts:
                continue
            recent_posts.sort(key=lambda x: safe_int(x.get("publish_time"), 0), reverse=True)
            for p in recent_posts:
                if safe_int(p.get("publish_time"), 0) < cutoff_ts:
                    continue
                if not _is_post_in_allowed_languages(p, allowed_langs_set):
                    continue
                if not _has_standalone_pure_text(p):
                    continue
                chosen = p
                break

        if not chosen:
            continue

        pid = str(chosen.get("post_id") or "")
        if not pid or pid in seen_pids:
            continue

        if chosen.get("comments"):
            final_post = chosen
        else:
            time.sleep(random.uniform(0.35, 0.7))
            detailed = harvester.fetch_tweet_with_comments(pid, screen_name=handle, max_comments=15)
            final_post = detailed if (detailed and _has_standalone_pure_text(detailed)) else chosen

        seen_pids.add(pid)
        selected_posts.append(final_post)
        user_rel_by_post_id[pid] = _build_rel_snapshot(u_prof)

    selected_posts.sort(key=lambda x: safe_int(x.get("publish_time"), 0), reverse=True)
    return selected_posts, user_rel_by_post_id


def _supplement_recent_unscored_peer_posts_from_db(
    col,
    harvester: XHarvester,
    cutoff_ts: int,
    min_followers: int,
    max_followers: int,
    allowed_langs_set: Set[str],
    self_uid: str,
    self_handles: Set[str],
    existing_posts: List[Dict[str, Any]],
    user_rel_map: Dict[str, Dict[str, Any]],
    force_rescore: bool,
    target_total: int = 12,
) -> int:
    """
    当本轮实时 UserTweets 新拉帖子不足时，从 `harvested_social_posts` 复用 24h 内已入库但尚未评分（或 `--force`）
    的目标语种同体量蓝V帖子，并逐一通过 `fetch_user_profile_detail` 实时核验【蓝V + 粉丝区间 + 双向未关注】及【每人最多1帖】。
    """
    if len(existing_posts) >= target_total:
        return 0

    seen_pids: Set[str] = {str(p.get("post_id") or "") for p in existing_posts if p.get("post_id")}
    seen_authors: Set[str] = {
        str((p.get("author") or {}).get("username") or "").strip().lstrip("@").lower()
        for p in existing_posts
        if (p.get("author") or {}).get("username")
    }

    db_query: Dict[str, Any] = {
        "source": "x",
        "publish_time": {"$gte": cutoff_ts},
        "author.is_verified": True,
        "author.followers_count": {"$gte": min_followers, "$lte": max_followers},
        "metadata.is_retweet": {"$ne": True},
    }
    if not force_rescore:
        db_query[f"{AUTO_COMMENT_FIELD}.scored_at_ts"] = {"$exists": False}

    db_docs = list(col.find(db_query, {"_id": 0}).sort("publish_time", -1).limit(80))
    added = 0
    for doc in db_docs:
        if len(existing_posts) >= target_total:
            break
        pid = str(doc.get("post_id") or "").strip()
        if not pid or pid in seen_pids:
            continue
        if not _is_post_in_allowed_languages(doc, allowed_langs_set):
            continue
        handle = str((doc.get("author") or {}).get("username") or "").strip().lstrip("@")
        h_low = handle.lower()
        author_uid = str(doc.get("author_id") or "").strip()
        if not handle or h_low in seen_authors or h_low in self_handles or (self_uid and author_uid == self_uid):
            continue
        if not _has_standalone_pure_text(doc):
            continue

        time.sleep(random.uniform(0.25, 0.5))
        prof = harvester.fetch_user_profile_detail(handle)
        if not prof:
            continue
        is_blue = bool(prof.get("is_blue_verified") or prof.get("is_verified"))
        followers = safe_int(prof.get("followers_count"), 0)
        if not is_blue or followers < min_followers or followers > max_followers:
            continue
        if bool(prof.get("is_following")) or bool(prof.get("is_followed_by")):
            continue

        prof["source_role"] = "db_recent_peer"
        if not doc.get("comments") and safe_int((doc.get("engagement") or {}).get("comment_count"), 0) > 0:
            time.sleep(random.uniform(0.3, 0.6))
            det = harvester.fetch_tweet_with_comments(pid, screen_name=handle, max_comments=15)
            if det and _has_standalone_pure_text(det):
                doc = det

        seen_pids.add(pid)
        seen_authors.add(h_low)
        existing_posts.append(doc)
        user_rel_map[pid] = _build_rel_snapshot(prof)
        added += 1

    return added


def _batch_generate_comments_and_categories(
    posts: Sequence[Dict[str, Any]],
    batch_size: int,
    allowed_languages: Sequence[str],
    max_weighted_chars: int,
    generator_model_group: str,
    timeout: int,
) -> Dict[str, Dict[str, Any]]:
    """
    阶段 3：纯文本按 `batch_size`（默认 5 条/批）批量生成目标语种（`en`/`ja`）候选评论 + 标记预定义帖子分类。
    """
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
                        return f"`results[{idx}].language` ({lang_norm}) 不在允许语种 {sorted(allowed_langs_set)} 内，非目标语种原帖请设 skip=true"
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
            file_paths=None,  # 100% 纯文本，绝不传图片或非文本附件
            timeout=timeout,
            preset_model_group=generator_model_group,
            validator=_validate_gen,
        )

        if not parsed:
            logger.warning(
                f"[CommentPipeline/批量生成失败] 批次 {batch_no} 调用异常: {trace.get('parse_error') or trace.get('error_history')}"
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
    """
    阶段 4：将本轮生成的全部帖子与评论送入**独立第三方强模型组 (`preset_model_group="high"`)** 统一客观打分，
    原地更新 `gen_map[pid]` 中的候选得分、`best_comment`、`best_score` 与 `scorer_model`。
    """
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
                f"[CommentPipeline/强模型打分失败] 批次 {batch_no}: {trace.get('parse_error') or trace.get('error_history')}"
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


def run_comment_pipeline(
    user_data_dir: str,
    *,
    min_followers: int = PIPELINE_DEFAULTS["min_followers"],
    max_followers: int = PIPELINE_DEFAULTS["max_followers"],
    feed_fetch_count: int = PIPELINE_DEFAULTS["feed_fetch_count"],
    max_target_users: int = PIPELINE_DEFAULTS["max_target_users_per_run"],
    comment_batch_size: int = PIPELINE_DEFAULTS["comment_batch_size"],
    allowed_languages: Sequence[str] = PIPELINE_DEFAULTS["allowed_languages"],
    force_rescore: bool = False,
) -> Dict[str, Any]:
    """
    模块一唯一主入口：传入登录会话目录 `user_data_dir`，完成：
    推荐页挖潜力蓝 V -> 拉每人 24h 最新 1 帖（仅限 en / ja）入库 -> 5 帖纯文本批量生成评论+分类+语种 -> 独立 `medium` 模型盲测打分落库。
    """
    logger = get_growth_logger()
    t0 = time.perf_counter()

    clean_allowed_langs = _normalize_allowed_languages(allowed_languages)
    allowed_langs_set = set(clean_allowed_langs)

    session_path = str(Path(user_data_dir).expanduser().resolve())
    harvester = XHarvester(burner_user_data_dir=session_path)
    if not harvester.has_burner_account:
        raise RuntimeError(f"指定的登录目录未检测到有效 X 登录态 Cookie (auth_token/ct0): {session_path}")

    db = gen_db_object()
    _ensure_auto_comment_index(db)
    post_manager = HarvestedPostManager(db, collection_name=POSTS_COLLECTION_NAME)

    self_uid = str(harvester.burner_account_uid or "").strip()
    self_handles = set(get_configured_self_handles())
    now_ts = int(time.time())
    cutoff_ts = now_ts - int(PIPELINE_DEFAULTS["post_max_age_seconds"])

    logger.info(
        f"[Module1/启动] 会话={harvester.burner_account_summary} | "
        f"筛选条件=(蓝V认证, 粉丝 {min_followers}~{max_followers}, 双向未关注, 语种={clean_allowed_langs}, 24h内最新1帖)"
    )

    # 1. 推荐流挖掘潜力认证用户
    qualified_users, feed_count, feed_posts_map = _discover_qualified_users(
        harvester=harvester,
        feed_fetch_count=feed_fetch_count,
        detail_concurrency=int(PIPELINE_DEFAULTS["feed_detail_concurrency"]),
        min_followers=min_followers,
        max_followers=max_followers,
        self_uid=self_uid,
        self_handles=self_handles,
    )
    target_users = qualified_users[: max(1, int(max_target_users))]
    logger.info(
        f"[Module1/用户筛选] 推荐流帖子={feed_count}条 -> 命中高潜力未连接蓝V用户={len(qualified_users)}人 "
        f"(本轮取前 {len(target_users)} 人拉取今日新帖)"
    )

    # 2. 拉取每位潜力用户最近 24h 内的最新 1 条目标语种完整有字帖子并存入 harvested_social_posts
    latest_posts, user_rel_map = _fetch_latest_today_posts_for_users(
        harvester=harvester,
        users=target_users,
        cutoff_ts=cutoff_ts,
        allowed_langs_set=allowed_langs_set,
        feed_posts_map=feed_posts_map,
    )
    live_harvested_count = len(latest_posts)
    if latest_posts:
        post_manager.upsert_posts(latest_posts)

    col = db.get_collection(POSTS_COLLECTION_NAME)
    db_supplement_count = _supplement_recent_unscored_peer_posts_from_db(
        col=col,
        harvester=harvester,
        cutoff_ts=cutoff_ts,
        min_followers=min_followers,
        max_followers=max_followers,
        allowed_langs_set=allowed_langs_set,
        self_uid=self_uid,
        self_handles=self_handles,
        existing_posts=latest_posts,
        user_rel_map=user_rel_map,
        force_rescore=force_rescore,
        target_total=12,
    )
    if db_supplement_count > 0:
        post_manager.upsert_posts(latest_posts)

    account_warning = (
        "当前会话触发 X 安全验证临时挑战 (code=326, https://x.com/account/access)，已自动熔断 UserTweets 并降级使用推荐流/库内24h新鲜同体量帖子；真实发评前请先在浏览器打开该目录解锁账号。"
        if getattr(harvester, "_burner_locked_326", False)
        else None
    )
    if account_warning:
        logger.warning(f"[Module1/账号提醒] {account_warning}")

    logger.info(
        f"[Module1/新帖汇总] 实时拉取今日主帖={live_harvested_count}条 | 库内24h未评同体量蓝V补齐={db_supplement_count}条 | 本轮合计={len(latest_posts)}条"
    )
    if not latest_posts:
        return {
            "status": "ok",
            "user_data_dir": session_path,
            "allowed_languages": clean_allowed_langs,
            "account_warning": account_warning,
            "qualified_users_count": len(qualified_users),
            "today_posts_harvested": 0,
            "db_supplemented_posts": 0,
            "newly_scored_posts": 0,
            "elapsed_seconds": round(time.perf_counter() - t0, 2),
            "items": [],
        }

    # 幂等检查：已打过分（或已发布）的帖子默认不重复消耗模型额度
    all_pids = [str(p.get("post_id") or "") for p in latest_posts if p.get("post_id")]
    existing_docs = list(
        col.find(
            {"source": "x", "post_id": {"$in": all_pids}},
            {"post_id": 1, AUTO_COMMENT_FIELD: 1, "_id": 0},
        )
    )
    existing_auto_map: Dict[str, Dict[str, Any]] = {
        str(d.get("post_id")): (d.get(AUTO_COMMENT_FIELD) or {}) for d in existing_docs
    }

    posts_to_generate = [
        p
        for p in latest_posts
        if force_rescore or not existing_auto_map.get(str(p.get("post_id") or ""), {}).get("scored_at_ts")
    ]
    if not posts_to_generate:
        logger.info("[Module1/幂等跳过] 本轮拉取到的今日新帖均已完成评论生成与评分。")
        return {
            "status": "ok",
            "user_data_dir": session_path,
            "allowed_languages": clean_allowed_langs,
            "account_warning": account_warning,
            "qualified_users_count": len(qualified_users),
            "today_posts_harvested": live_harvested_count,
            "db_supplemented_posts": db_supplement_count,
            "newly_scored_posts": 0,
            "elapsed_seconds": round(time.perf_counter() - t0, 2),
            "items": [],
        }

    # 3. 纯文本 5 帖/批生成目标语种评论 + 标记帖子分类与语种 (low 模型组)
    gen_map = _batch_generate_comments_and_categories(
        posts=posts_to_generate,
        batch_size=comment_batch_size,
        allowed_languages=clean_allowed_langs,
        max_weighted_chars=int(PIPELINE_DEFAULTS["max_weighted_chars"]),
        generator_model_group=str(PIPELINE_DEFAULTS["generator_model_group"]),
        timeout=int(PIPELINE_DEFAULTS["llm_timeout"]),
    )

    # 4. 独立第三方中模型组 (medium 模型组) 横向反人机味盲测打分
    _batch_score_with_strong_model(
        posts=posts_to_generate,
        gen_map=gen_map,
        score_batch_size=int(PIPELINE_DEFAULTS["score_batch_size"]),
        scorer_model_group=str(PIPELINE_DEFAULTS["scorer_model_group"]),
        timeout=int(PIPELINE_DEFAULTS["llm_timeout"]),
    )

    # 5. 统一收纳写入 harvested_social_posts.<auto_comment> 单一嵌套字段下
    bulk_ops: List[UpdateOne] = []
    summary_items: List[Dict[str, Any]] = []
    scored_count = 0

    for p in posts_to_generate:
        pid = str(p.get("post_id") or "")
        g_info = gen_map.get(pid)
        if not g_info:
            continue
        # 若未标记 skip 但强模型打分因异常缺失，则暂不写入半成品，留待下轮补评
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
            "author_relationship": user_rel_map.get(pid, {}),
            "generated_at_ts": g_info.get("generated_at_ts"),
            "scored_at_ts": g_info.get("scored_at_ts") or g_info.get("generated_at_ts"),
            "updated_at": now_iso_cn(),
            "published": bool(prev_meta.get("published", False)),
            "published_at_ts": prev_meta.get("published_at_ts"),
            "published_at": prev_meta.get("published_at"),
            "published_by_dir": prev_meta.get("published_by_dir"),
            "published_comment_id": prev_meta.get("published_comment_id"),
        }

        bulk_ops.append(
            UpdateOne(
                {"source": "x", "post_id": pid},
                {"$set": {AUTO_COMMENT_FIELD: nested_payload}},
            )
        )
        if not nested_payload["skip"]:
            scored_count += 1
            author_handle = str((p.get("author") or {}).get("username") or "")
            summary_items.append(
                {
                    "post_id": pid,
                    "author": f"@{author_handle}",
                    "followers": (user_rel_map.get(pid) or {}).get("followers_count", 0),
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

    summary_items.sort(key=lambda x: int(x.get("best_score") or 0), reverse=True)
    elapsed = round(time.perf_counter() - t0, 2)
    logger.info(
        f"[Module1/完成] 耗时={elapsed}s | 潜力蓝V={len(qualified_users)}人 | "
        f"本轮候选帖={len(latest_posts)}条 | 完成生成+中模型盲测评分入库={scored_count}条"
    )
    return {
        "status": "ok",
        "user_data_dir": session_path,
        "allowed_languages": clean_allowed_langs,
        "account_warning": account_warning,
        "qualified_users_count": len(qualified_users),
        "today_posts_harvested": live_harvested_count,
        "db_supplemented_posts": db_supplement_count,
        "newly_scored_posts": scored_count,
        "elapsed_seconds": elapsed,
        "items": summary_items,
    }


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    # 支持命令行直接传登录目录路径（可选 `--force` 强制重评已有帖子），未传时默认读取 x_xiaohao_browser_session_dir
    cli_args = [a for a in sys.argv[1:] if not a.startswith("-")]
    force_flag = "--force" in sys.argv
    target_dir = cli_args[0] if cli_args else resolve_session_dir("x_xiaohao_browser_session_dir", "x_xiaohao")
    res = run_comment_pipeline(target_dir, force_rescore=force_flag)
    print(json.dumps(res, ensure_ascii=False, indent=2))
