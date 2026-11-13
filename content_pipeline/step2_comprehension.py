# -*- coding: utf-8 -*-
"""
模块 2：单帖多模态高并发无损理解模块 (`step2_comprehension.py`)

唯一职责与解耦设计：
1. 接收标准化非视频原帖（既支持扫描全库时间窗 `run_step2_comprehension`，也支持按任意 `post_keys` 定向即时理解 `comprehend_posts_by_keys`）；
2. 将「原帖正文 + 外链提取原文（若有）+ 明确标注归属的全部本地图片（主帖图 / 引用原帖图 / 外链头图 / 评论配图）+ 前排评论」一并送入多模态模型组；
3. 最小化信息损失地转化为结构化文字档案存入 `pipeline_post_digests`；
4. 严格幂等缓存：已理解过的帖子绝不因后续新增评论重复消耗算力重跑，增量评论在下游创作阶段直读 `harvested_social_posts` 补齐。
"""
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, urlsplit

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from content_pipeline.llm_gateway import call_llm_json, render_prompt_template
from content_pipeline.pipeline_config import (
    ACTIVE_PROFILE,
    CONTENT_PROFILES,
    STEP2_CONFIG,
)
from content_pipeline.pipeline_db import (
    PipelineDB,
    is_video_post,
    make_post_key,
    round_robin_interleave_by_source,
)
from content_pipeline.pipeline_logger import get_pipeline_logger

SUPPORTED_IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
ORIGIN_DISPLAY_NAMES = {
    "post": "主帖配图",
    "quoted_post": "被引用原帖配图",
    "external_og": "外链文章头图",
    "comment": "评论区配图",
}


def strip_inline_media_placeholders(text: str) -> str:
    """剥离归一化阶段追加在文末的 `[插图: https://...]` / `[封面图: https://...]` 占位符，保留 100% 原始正文语句。"""
    if not text:
        return ""
    cleaned_lines: List[str] = []
    for line in str(text).splitlines():
        stripped = line.strip()
        if (stripped.startswith("[插图:") or stripped.startswith("[封面图:")) and stripped.endswith("]"):
            continue
        cleaned_lines.append(line)
    return "\n".join(cleaned_lines).strip()


def extract_external_article_dict(raw_post: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """兼容旧记录：若历史采集记录仍将外链存在 `metadata.external_article` 中，安全提取以便统一归入原帖全文。"""
    meta = raw_post.get("metadata") or {}
    pextra = meta.get("platform_extra") or {}
    ext = meta.get("external_article") or pextra.get("external_article")
    if isinstance(ext, dict) and str(ext.get("text") or "").strip():
        return ext
    return None


def get_unified_post_text(raw_post: Dict[str, Any]) -> str:
    """
    获取完整的单一「原帖全文」：
    阶段 1 采集时已将外链博客/新闻全文直接补全进 `content.text_content`；
    对兼容期尚未合并的旧记录自动在读取时无缝合一，下游模块不再区分“原帖正文”与“外链正文”两类。
    """
    content_obj = raw_post.get("content") or {}
    title = str(content_obj.get("title") or "").strip()
    body = strip_inline_media_placeholders(str(content_obj.get("text_content") or ""))
    if title and title not in body:
        unified = f"【标题】{title}\n{body}".strip() if body else f"【标题】{title}"
    else:
        unified = body

    if "【原帖关联外链全文" not in unified:
        ext_art = extract_external_article_dict(raw_post)
        if ext_art:
            ext_title = str(ext_art.get("title") or "").strip()
            ext_domain = str(ext_art.get("site_domain") or ext_art.get("url") or "").strip()
            ext_body = str(ext_art.get("text") or "").strip()[:10000]
            hdr = f"【原帖关联外链全文 | 标题: {ext_title} | 来源: {ext_domain}】" if ext_title else f"【原帖关联外链全文 | 来源: {ext_domain}】"
            unified = f"{unified}\n\n{hdr}\n{ext_body}".strip() if unified else f"{hdr}\n{ext_body}"

    return unified


def _filter_duplicate_reddit_thumbnails(urls: List[str]) -> List[str]:
    """若同时存在同一 Reddit 资源的裁剪缩略图与原图，自动去除重复缩略图。"""
    if len(urls) <= 1:
        return urls
    non_crop_assets = set()
    for u in urls:
        sp = urlsplit(u)
        if "redd.it" in sp.netloc and "crop" not in parse_qs(sp.query):
            non_crop_assets.add(sp.path.rsplit("/", 1)[-1].lower())

    if not non_crop_assets:
        return urls

    filtered: List[str] = []
    for u in urls:
        sp = urlsplit(u)
        qs = parse_qs(sp.query)
        asset = sp.path.rsplit("/", 1)[-1].lower()
        if "redd.it" in sp.netloc and "crop" in qs and asset in non_crop_assets:
            continue
        filtered.append(u)
    return filtered


def extract_ordered_local_images(
    raw_post: Dict[str, Any],
    max_comments: int = 10,
    include_comment_images: bool = True,
) -> List[Dict[str, Any]]:
    """
    提取该帖关联的全部本地已下载图片（不设张数截断），并精确标注每张图的归属来源：
    - `post`        : 主帖自身配图
    - `quoted_post` : 被引用推文/原帖配图
    - `external_og` : 外链新闻/博客头图
    - `comment`     : 前排高赞评论附带的评论配图（消除 `[图片评论]` 盲区）
    """
    media = raw_post.get("media") or {}
    local_map = media.get("local_mapping") or {}
    meta = raw_post.get("metadata") or {}
    pextra = meta.get("platform_extra") or {}

    main_imgs_set = set(str(u) for u in (meta.get("main_images") or pextra.get("main_images") or []) if u)
    qp = meta.get("quoted_post") or pextra.get("quoted_post") or {}
    quoted_imgs_set = set(str(u) for u in (qp.get("images") or []) if isinstance(qp, dict) and u)
    og_img_url = str(meta.get("og_image_url") or pextra.get("og_image_url") or "").strip()

    ordered_urls: List[str] = []
    for u in media.get("images") or []:
        if u and u not in ordered_urls:
            ordered_urls.append(u)
    cover = media.get("cover_image")
    if cover and cover not in ordered_urls and not media.get("video_url"):
        ordered_urls.append(cover)

    if str(raw_post.get("source") or "").lower() == "reddit":
        ordered_urls = _filter_duplicate_reddit_thumbnails(ordered_urls)

    results: List[Dict[str, Any]] = []
    seen_paths = set()

    for url in ordered_urls:
        local_p = local_map.get(url)
        if not local_p:
            continue
        p = Path(local_p)
        if p.is_file() and p.suffix.lower() in SUPPORTED_IMAGE_SUFFIXES:
            resolved = str(p.resolve())
            if resolved not in seen_paths:
                seen_paths.add(resolved)
                if url in quoted_imgs_set and url not in main_imgs_set:
                    origin = "quoted_post"
                elif og_img_url and url == og_img_url:
                    origin = "external_og"
                else:
                    origin = "post"
                results.append(
                    {
                        "origin": origin,
                        "origin_label": ORIGIN_DISPLAY_NAMES[origin],
                        "remote_url": str(url),
                        "local_path": resolved,
                    }
                )

    if include_comment_images:
        raw_comments = [c for c in (raw_post.get("comments") or []) if isinstance(c, dict)]
        raw_comments.sort(key=lambda x: int(x.get("likes") or 0), reverse=True)
        for c_idx, c in enumerate(raw_comments[:max_comments], 1):
            c_lmap = c.get("local_mapping") or {}
            if not isinstance(c_lmap, dict) or not c_lmap:
                continue
            c_author = c.get("author_name") or c.get("author_uid") or "网友"
            c_urls = list(c.get("images") or []) or list(c_lmap.keys())
            for c_url in c_urls:
                lp = c_lmap.get(c_url)
                if not lp:
                    continue
                cp = Path(lp)
                if cp.is_file() and cp.suffix.lower() in SUPPORTED_IMAGE_SUFFIXES:
                    resolved = str(cp.resolve())
                    if resolved not in seen_paths:
                        seen_paths.add(resolved)
                        results.append(
                            {
                                "origin": "comment",
                                "origin_label": f"评论区配图(来自评论#{c_idx} {c_author})",
                                "comment_rank": c_idx,
                                "remote_url": str(c_url),
                                "local_path": resolved,
                            }
                        )

    return results


def _validate_step2_json(data: Dict[str, Any]) -> Optional[str]:
    if "is_usable" not in data or not isinstance(data.get("is_usable"), bool):
        return "缺少布尔字段 `is_usable`"
    if "summary" not in data or not isinstance(data.get("summary"), str):
        return "缺少字符串字段 `summary`"
    if data["is_usable"] and not data["summary"].strip():
        return "当 `is_usable=true` 时 `summary` 不能为空"
    if "valuable_comments" in data and not isinstance(data["valuable_comments"], list):
        return "`valuable_comments` 必须是数组"
    if "images" in data and not isinstance(data["images"], list):
        return "`images` 必须是数组"
    return None


def _build_post_comprehension_inputs(
    raw_post: Dict[str, Any],
    image_items: List[Dict[str, Any]],
    max_comments: int = 10,
) -> Dict[str, str]:
    """组装无流量偏见的提示词插槽字典（融合原帖文字、外链原文、明确归属的配图清单与前排评论）。"""
    source = raw_post.get("source") or "unknown"
    author = raw_post.get("author") or {}
    author_label = author.get("author_name") or author.get("username") or raw_post.get("author_id") or "匿名"

    pub_ts = int(raw_post.get("publish_time") or 0)
    pub_str = (
        datetime.fromtimestamp(pub_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        if pub_ts > 0
        else "未知时间"
    )

    meta_text = (
        f"- 来源平台: {source} | 作者: {author_label} | 发布时间: {pub_str}\n"
        f"- 已上传本地图片总数: {len(image_items)} 张"
    )

    unified_text = get_unified_post_text(raw_post)
    post_text = unified_text or "（原帖无文字，请结合上传图片理解）"

    comment_rank_to_att_indices: Dict[int, List[int]] = {}
    if image_items:
        manifest_lines: List[str] = []
        for idx, item in enumerate(image_items, 1):
            lbl = item.get("origin_label") or ORIGIN_DISPLAY_NAMES.get(item.get("origin", "post"), "配图")
            manifest_lines.append(f"- 图片 #{idx} [{lbl}]: 对应第 {idx} 张上传附件")
            c_rank = item.get("comment_rank")
            if isinstance(c_rank, int):
                comment_rank_to_att_indices.setdefault(c_rank, []).append(idx)
        image_manifest = "\n".join(manifest_lines)
    else:
        image_manifest = "（本帖无图片附件，`images` 数组请输出 `[]`）"

    raw_comments = [c for c in (raw_post.get("comments") or []) if isinstance(c, dict)]
    raw_comments.sort(key=lambda x: int(x.get("likes") or 0), reverse=True)
    comment_lines: List[str] = []
    for idx, c in enumerate(raw_comments[:max_comments], 1):
        c_text = strip_inline_media_placeholders(str(c.get("content") or ""))
        att_idxs = comment_rank_to_att_indices.get(idx) or []
        if not c_text and not att_idxs:
            continue
        c_author = c.get("author_name") or c.get("author_uid") or "网友"
        c_likes = int(c.get("likes") or 0)
        like_tag = f" | 赞:{c_likes}" if c_likes > 0 else ""
        img_tag = f" [附评论配图 -> 见上传附件 #{', #'.join(str(i) for i in att_idxs)}]" if att_idxs else ""
        comment_lines.append(f"{idx}. [{c_author}{like_tag}]{img_tag} {c_text or '（纯图片评论）'}")

    top_comments = "\n".join(comment_lines) if comment_lines else "（暂无抓取到的评论）"

    return {
        "POST_META": meta_text,
        "POST_TEXT": post_text,
        "IMAGE_MANIFEST": image_manifest,
        "TOP_COMMENTS": top_comments,
    }


def comprehend_single_post(
    raw_post: Dict[str, Any],
    prompt_file: str,
    step2_cfg: Dict[str, Any],
    run_id: str,
    prev_fail_count: int = 0,
) -> Dict[str, Any]:
    """
    对单条原帖执行多模态无损理解，返回可直接入库 `pipeline_post_digests` 的标准记录。
    当累计失败次数达到 `max_comprehension_attempts`（默认 3 次）时自动标记为 `abandoned`，后续轮次不再重复尝试。
    """
    source = str(raw_post.get("source") or "").strip()
    post_id = str(raw_post.get("post_id") or "").strip()
    post_key = make_post_key(source, post_id)
    max_comments = int(step2_cfg.get("max_comments_per_post", 10))
    max_attempts = max(1, int(step2_cfg.get("max_comprehension_attempts", 3)))

    image_items = extract_ordered_local_images(raw_post, max_comments=max_comments, include_comment_images=True)
    attachment_paths = [item["local_path"] for item in image_items]
    slots = _build_post_comprehension_inputs(
        raw_post,
        image_items=image_items,
        max_comments=max_comments,
    )

    rendered_prompt, prompt_meta = render_prompt_template(prompt_file, slots)
    parsed, llm_trace = call_llm_json(
        prompt=rendered_prompt,
        prompt_meta=prompt_meta,
        model=step2_cfg.get("model"),
        fallback_models=step2_cfg.get("fallback_models"),
        file_paths=attachment_paths,
        timeout=int(step2_cfg.get("timeout", 150)),
        preset_model_group=str(step2_cfg.get("preset_model_group") or "medium"),
        validator=_validate_step2_json,
    )

    now_ts = int(time.time())
    content_obj = raw_post.get("content") or {}
    author_obj = raw_post.get("author") or {}
    unified_body = get_unified_post_text(raw_post)

    base_record: Dict[str, Any] = {
        "source": source,
        "post_id": post_id,
        "post_key": post_key,
        "run_id": run_id,
        "publish_time": int(raw_post.get("publish_time") or 0),
        "digested_at_ts": now_ts,
        "author_name": author_obj.get("author_name") or author_obj.get("username") or "",
        "post_url": (raw_post.get("metadata") or {}).get("url") or "",
        "raw_title": content_obj.get("title"),
        "raw_text_preview": unified_body[:600],
        "has_external_article": ("【原帖关联外链全文" in unified_body),
        "local_images": image_items,
        "has_local_images": bool(image_items),
        "llm_trace": llm_trace,
    }

    if parsed is None:
        new_fail_count = int(prev_fail_count or 0) + 1
        exhausted = new_fail_count >= max_attempts
        base_record.update(
            {
                "status": "abandoned" if exhausted else "failed",
                "fail_count": new_fail_count,
                "is_usable": False,
                "reject_reason": (
                    f"累计{new_fail_count}次理解失败已放弃: {llm_trace.get('parse_error') or llm_trace.get('call_status')}"
                    if exhausted
                    else f"LLM理解失败(第{new_fail_count}/{max_attempts}次): {llm_trace.get('parse_error') or llm_trace.get('call_status')}"
                ),
                "summary": "",
                "valuable_comments": [],
                "images": [],
            }
        )
        return base_record

    raw_imgs = parsed.get("images") if isinstance(parsed.get("images"), list) else []
    desc_by_idx: Dict[int, Dict[str, Any]] = {}
    for pos, desc_item in enumerate(raw_imgs):
        if not isinstance(desc_item, dict):
            continue
        img_idx = int(desc_item.get("index") or (pos + 1))
        if 1 <= img_idx <= len(image_items) and img_idx not in desc_by_idx:
            desc_by_idx[img_idx] = desc_item

    aligned_images: List[Dict[str, Any]] = []
    for idx_1, matched_file in enumerate(image_items, 1):
        d_obj = desc_by_idx.get(idx_1) or {}
        origin = matched_file.get("origin") or "post"
        origin_label = matched_file.get("origin_label") or ORIGIN_DISPLAY_NAMES.get(origin, "配图")
        aligned_images.append(
            {
                "index": idx_1,
                "origin": origin,
                "origin_label": origin_label,
                "local_path": matched_file["local_path"],
                "remote_url": matched_file["remote_url"],
                "desc": str(d_obj.get("desc") or origin_label).strip(),
                "has_ui_or_watermark": bool(d_obj.get("has_ui_or_watermark", False)),
            }
        )

    summary_text = str(parsed.get("summary") or "").strip()
    is_usable = bool(parsed.get("is_usable", False)) and bool(summary_text)
    reject_reason = str(parsed.get("reject_reason") or "").strip()
    valuable_comments = [
        str(x).strip()
        for x in (parsed.get("valuable_comments") or [])
        if isinstance(x, str) and x.strip()
    ]

    base_record.update(
        {
            "status": "ok",
            "fail_count": 0,
            "is_usable": is_usable,
            "reject_reason": reject_reason if not is_usable else "",
            "summary": summary_text,
            "valuable_comments": valuable_comments,
            "images": aligned_images,
        }
    )
    return base_record


def _execute_comprehension_batch(
    pending_posts: List[Dict[str, Any]],
    prompt_file: str,
    cfg: Dict[str, Any],
    run_id: str,
    fail_counts_map: Optional[Dict[str, int]] = None,
    log_prefix: str = "Step2",
    db: Optional[PipelineDB] = None,
) -> List[Dict[str, Any]]:
    logger = get_pipeline_logger()
    digests: List[Dict[str, Any]] = []
    if not pending_posts:
        return digests

    fc_lookup = fail_counts_map or {}
    workers = max(1, min(int(cfg.get("concurrency", 12)), len(pending_posts)))
    unflushed: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        fut_map = {
            pool.submit(
                comprehend_single_post,
                post,
                prompt_file,
                cfg,
                run_id,
                fc_lookup.get(make_post_key(post.get("source", ""), post.get("post_id", "")), 0),
            ): post
            for post in pending_posts
        }
        for idx, fut in enumerate(as_completed(fut_map), 1):
            rec = fut.result()
            digests.append(rec)
            unflushed.append(rec)
            if idx % 10 == 0 or idx == len(pending_posts):
                if db is not None and unflushed:
                    db.upsert_digests(unflushed)
                    unflushed.clear()
                ok_so_far = sum(1 for d in digests if d["status"] == "ok")
                usable_so_far = sum(1 for d in digests if d.get("is_usable"))
                logger.info(
                    f"[{log_prefix}/进度] 已完成 {idx}/{len(pending_posts)} "
                    f"(成功:{ok_so_far}, 可用:{usable_so_far}, 带图:{sum(1 for d in digests if d['has_local_images'])})"
                )
    return digests


def comprehend_posts_by_keys(
    post_keys: Sequence[str],
    db: Optional[PipelineDB] = None,
    profile_name: str = ACTIVE_PROFILE,
    step2_cfg: Optional[Dict[str, Any]] = None,
    force_recompute: bool = False,
    run_id: str = "targeted_step2",
) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]:
    """
    模块 2 独立编程接口 A：给定任意一批 `post_keys`（如 `harvest_by_keywords` 定向搜回的帖子），
    确定性过滤视频帖后，复用已有理解缓存并仅对未理解（且失败未满 3 次）的新帖高并发执行多模态理解。
    返回 `(summary_dict, digest_map_by_key)`。
    """
    logger = get_pipeline_logger()
    db = db or PipelineDB()
    cfg = dict(STEP2_CONFIG)
    if step2_cfg:
        cfg.update(step2_cfg)

    clean_keys: List[str] = []
    seen = set()
    for pk in post_keys or []:
        s = str(pk or "").strip()
        if s and s not in seen:
            seen.add(s)
            clean_keys.append(s)

    t0 = time.perf_counter()
    if not clean_keys:
        return (
            {
                "status": "ok",
                "elapsed_seconds": 0.0,
                "requested_count": 0,
                "excluded_video_count": 0,
                "cache_hit_count": 0,
                "newly_processed_count": 0,
                "usable_count": 0,
            },
            {},
        )

    raw_map = db.fetch_raw_posts_by_keys(clean_keys)
    exclude_videos = bool(cfg.get("exclude_video_posts", True))
    max_attempts = max(1, int(cfg.get("max_comprehension_attempts", 3)))
    eligible_posts: List[Dict[str, Any]] = []
    excluded_video_count = 0

    for pk in clean_keys:
        rp = raw_map.get(pk)
        if not rp:
            continue
        if exclude_videos and is_video_post(rp):
            excluded_video_count += 1
            continue
        eligible_posts.append(rp)

    eligible_keys = [make_post_key(p.get("source", ""), p.get("post_id", "")) for p in eligible_posts]
    existing_map = {} if force_recompute else db.fetch_digests_by_keys(eligible_keys)
    terminal_keys = {
        k
        for k, v in existing_map.items()
        if v.get("status") in ("ok", "abandoned") or int(v.get("fail_count") or 0) >= max_attempts
    }
    fail_counts_map = {
        k: int(v.get("fail_count") or (1 if v.get("status") == "failed" else 0))
        for k, v in existing_map.items()
    }

    pending_posts = [
        p for p in eligible_posts if make_post_key(p.get("source", ""), p.get("post_id", "")) not in terminal_keys
    ]

    profile = CONTENT_PROFILES.get(profile_name) or CONTENT_PROFILES[ACTIVE_PROFILE]
    prompt_file = profile["step2_prompt_file"]

    new_digests: List[Dict[str, Any]] = []
    if pending_posts:
        logger.info(
            f"[Step2/定向理解] 请求={len(clean_keys)} (排除视频={excluded_video_count}, 缓存/已满3次跳过={len(terminal_keys)}) "
            f"-> 新理解={len(pending_posts)}条"
        )
        new_digests = _execute_comprehension_batch(
            pending_posts=pending_posts,
            prompt_file=prompt_file,
            cfg=cfg,
            run_id=run_id,
            fail_counts_map=fail_counts_map,
            log_prefix="Step2/定向",
            db=db,
        )
        db.upsert_digests(new_digests)

    final_map: Dict[str, Dict[str, Any]] = {
        k: v for k, v in existing_map.items() if v.get("status") == "ok"
    }
    for d in new_digests:
        if d.get("status") == "ok":
            final_map[d["post_key"]] = d

    usable_total = sum(1 for d in final_map.values() if d.get("is_usable"))
    elapsed = round(time.perf_counter() - t0, 2)
    summary = {
        "status": "ok",
        "elapsed_seconds": elapsed,
        "requested_count": len(clean_keys),
        "excluded_video_count": excluded_video_count,
        "cache_hit_count": len(terminal_keys),
        "newly_processed_count": len(new_digests),
        "usable_count": usable_total,
    }
    return summary, final_map


def run_step2_comprehension(
    db: Optional[PipelineDB] = None,
    profile_name: str = ACTIVE_PROFILE,
    step2_cfg: Optional[Dict[str, Any]] = None,
    run_id: str = "manual_step2",
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    模块 2 独立编程接口 B：扫描时间窗口内未理解的非视频原帖，按平台热度排序与公平轮询抽取后高并发执行多模态理解。
    单帖最多尝试 `max_comprehension_attempts`（默认 3）次，满 3 次仍失败则永久跳过。
    返回 `(step2_summary, newly_processed_digests)`。
    """
    logger = get_pipeline_logger()
    db = db or PipelineDB()
    cfg = dict(STEP2_CONFIG)
    if step2_cfg:
        cfg.update(step2_cfg)

    profile = CONTENT_PROFILES.get(profile_name) or CONTENT_PROFILES[ACTIVE_PROFILE]
    prompt_file = profile["step2_prompt_file"]

    t0 = time.perf_counter()
    lookback_hours = float(cfg.get("lookback_hours", 48))
    exclude_videos = bool(cfg.get("exclude_video_posts", True))
    max_attempts = max(1, int(cfg.get("max_comprehension_attempts", 3)))

    raw_posts, entry_stats = db.fetch_recent_raw_posts_with_stats(
        lookback_hours=lookback_hours,
        per_source_limit=200,
        exclude_video_posts=exclude_videos,
    )
    all_keys = [make_post_key(p.get("source", ""), p.get("post_id", "")) for p in raw_posts]

    force_recompute = bool(cfg.get("force_recompute", False))
    existing_keys = set() if force_recompute else db.get_existing_digest_keys(all_keys, max_attempts=max_attempts)

    pending_posts = [
        p for p in raw_posts if make_post_key(p.get("source", ""), p.get("post_id", "")) not in existing_keys
    ]

    max_per_run = int(cfg.get("max_new_posts_per_run", 40))
    if max_per_run > 0 and len(pending_posts) > max_per_run:
        pending_posts = round_robin_interleave_by_source(pending_posts, max_total=max_per_run)

    pending_keys = [make_post_key(p.get("source", ""), p.get("post_id", "")) for p in pending_posts]
    fail_counts_map = {} if force_recompute else db.get_digest_fail_counts(pending_keys)

    model_label = cfg.get("model") or f"group:{cfg.get('preset_model_group', 'medium')}"
    logger.info(
        f"[Step2/启动] 时间窗原帖={entry_stats['in_time_window_total']} "
        f"(入口排除视频帖={entry_stats['excluded_video_count']} -> 候选={len(raw_posts)}) | "
        f"缓存/满{max_attempts}次失败跳过={len(existing_keys)} | 本轮待理解={len(pending_posts)} | 并发={cfg.get('concurrency', 12)} | 路由={model_label}"
    )

    digests = _execute_comprehension_batch(
        pending_posts=pending_posts,
        prompt_file=prompt_file,
        cfg=cfg,
        run_id=run_id,
        fail_counts_map=fail_counts_map,
        log_prefix="Step2",
        db=db,
    )
    if digests:
        db.upsert_digests(digests)

    elapsed = round(time.perf_counter() - t0, 2)
    ok_list = [d for d in digests if d.get("status") == "ok"]
    usable_list = [d for d in ok_list if d.get("is_usable")]
    filtered_list = [d for d in ok_list if not d.get("is_usable")]
    failed_list = [d for d in digests if d.get("status") != "ok"]
    with_images = [d for d in digests if d.get("has_local_images")]
    with_ext = [d for d in digests if d.get("has_external_article")]
    total_imgs_fed = sum(len(d.get("local_images") or []) for d in digests)

    source_counter = Counter(d.get("source") or "unknown" for d in usable_list)
    model_counter = Counter((d.get("llm_trace") or {}).get("model_used") or "unknown" for d in ok_list)

    summary: Dict[str, Any] = {
        "status": "ok" if (not pending_posts or ok_list) else "failed",
        "elapsed_seconds": elapsed,
        "in_time_window_total": entry_stats["in_time_window_total"],
        "excluded_video_count": entry_stats["excluded_video_count"],
        "window_raw_count": len(raw_posts),
        "already_digested_count": len(existing_keys),
        "processed_count": len(digests),
        "success_count": len(ok_list),
        "failed_count": len(failed_list),
        "with_images_count": len(with_images),
        "with_external_article_count": len(with_ext),
        "total_images_fed": total_imgs_fed,
        "usable_count": len(usable_list),
        "filtered_count": len(filtered_list),
        "usable_by_source": dict(source_counter),
        "models_used": dict(model_counter),
        "filtered_samples": [
            f"{d['post_key']}({d.get('reject_reason') or '无实质信息'})" for d in filtered_list[:10]
        ],
        "failed_samples": [
            f"{d['post_key']}({d.get('reject_reason')})" for d in failed_list[:5]
        ],
    }

    logger.info(
        f"[Step2/完成] 耗时={elapsed:.2f}s | 处理={len(digests)} -> 可用={len(usable_list)}, "
        f"空壳拦截={len(filtered_list)}, 失败={len(failed_list)} | 上传图片={total_imgs_fed}张 | 外链文={len(with_ext)}篇 | "
        f"可用来源={dict(source_counter)} | 模型={dict(model_counter)}"
    )
    return summary, digests


if __name__ == "__main__":
    summary_res, sample_digests = run_step2_comprehension(
        step2_cfg={"max_new_posts_per_run": 8, "concurrency": 6},
        run_id="standalone_step2_verify",
    )
    for item in sample_digests:
        print(
            f"\n--- [{item['post_key']}] usable={item['is_usable']} | "
            f"imgs={len(item['local_images'])} | ext={item.get('has_external_article')} ---"
        )
        print(f"Summary: {item['summary']}")
