# -*- coding: utf-8 -*-
"""
Module 1 (Step 2)：单帖「图文 + 热评」多模态忠实还原与自洽性体检模块 (`step2_comprehension.py`)

本质职责：
1. 将「正文 + 本地主帖配图（严禁混入评论区无关表情包）+ 前排评论」一次性送入多模态模型组；
2. 不传入原帖浏览/点赞量（消除高流量光环偏见），客观告知是否含未下载视频；
3. 输出零冗余的 5 字段情报卡 (`is_usable`, `reject_reason`, `summary`, `valuable_comments`, `images`) 存入 `pipeline_post_digests`。

可独立运行调试：直接在 IDE 运行本文件，即可对未理解的新帖执行增量多模态理解并打印抽样结果。
"""
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
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
    make_post_key,
    round_robin_interleave_by_source,
)
from content_pipeline.pipeline_logger import get_pipeline_logger

SUPPORTED_IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp", ".gif"}


def strip_inline_media_placeholders(text: str) -> str:
    """剥离归一化阶段追加在文末的 `[插图: https://...]` URL 占位噪音，保留 100% 原始正文语句。"""
    if not text:
        return ""
    cleaned_lines: List[str] = []
    for line in str(text).splitlines():
        stripped = line.strip()
        if stripped.startswith("[插图:") and stripped.endswith("]"):
            continue
        cleaned_lines.append(line)
    return "\n".join(cleaned_lines).strip()


def detect_undownloaded_video(raw_post: Dict[str, Any]) -> bool:
    """
    基于平台标准化字段客观判断原帖是否包含未下载到本地的视频流：
    - `media.video_url` 非空 或 `media.video_duration > 0` 或 `card_type == "BUZZ_VIDEO"`
    - 或配图 URL 为 Twitter 视频静态封面 (`amplify_video_thumb` / `ext_tw_video_thumb` / `tweet_video_thumb`)
    - 或正文中直接引用了 `/video/` 视频状态链接
    """
    media = raw_post.get("media") or {}
    if media.get("video_url") or int(media.get("video_duration") or 0) > 0:
        return True
    if str(raw_post.get("card_type") or "").upper() == "BUZZ_VIDEO":
        return True
    for img_u in media.get("images") or []:
        u_low = str(img_u or "").lower()
        if any(tok in u_low for tok in ("amplify_video_thumb", "ext_tw_video_thumb", "tweet_video_thumb")):
            return True
    body_txt = str((raw_post.get("content") or {}).get("text_content") or "")
    if "/video/" in body_txt:
        return True
    return False


def _filter_duplicate_reddit_thumbnails(urls: List[str]) -> List[str]:
    """兼容历史已入库 Reddit 帖子：若同时存在同一资源的 140px `crop=1:1` 缩略图与高清原图，自动剔除缩略图。"""
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
    max_images: int = 4,
) -> List[Dict[str, str]]:
    """
    严格只按主帖图片出现顺序提取本地已下载的图片文件列表（绝不把评论区表情包冒充为主帖配图）。
    每项结构: `{"origin": "post", "remote_url": str, "local_path": str}`
    """
    media = raw_post.get("media") or {}
    local_map = media.get("local_mapping") or {}
    ordered_urls: List[str] = []

    for u in media.get("images") or []:
        if u and u not in ordered_urls:
            ordered_urls.append(u)
    cover = media.get("cover_image")
    if cover and cover not in ordered_urls and not media.get("video_url"):
        ordered_urls.append(cover)

    if str(raw_post.get("source") or "").lower() == "reddit":
        ordered_urls = _filter_duplicate_reddit_thumbnails(ordered_urls)

    results: List[Dict[str, str]] = []
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
                results.append({"origin": "post", "remote_url": str(url), "local_path": resolved})
                if len(results) >= max_images:
                    return results

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
    image_items: List[Dict[str, str]],
    max_comments: int = 10,
) -> Tuple[Dict[str, str], bool]:
    """组装高洁净度、无流量偏见的提示词插槽字典。"""
    source = raw_post.get("source") or "unknown"
    author = raw_post.get("author") or {}
    author_label = author.get("author_name") or author.get("username") or raw_post.get("author_id") or "匿名"

    pub_ts = int(raw_post.get("publish_time") or 0)
    pub_str = (
        datetime.fromtimestamp(pub_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        if pub_ts > 0
        else "未知时间"
    )

    has_unfetched_video = detect_undownloaded_video(raw_post)
    video_note = (
        "是（注意：系统未下载视频流；若上传的图片只是静态视频封面且单靠文字+静态图看不出完整事件或笑点，必须判 is_usable=false）"
        if has_unfetched_video
        else "否"
    )

    meta_text = (
        f"- 来源平台: {source} | 作者: {author_label} | 发布时间: {pub_str}\n"
        f"- 本地主帖图片数: {len(image_items)} 张 | 原帖是否包含未下载视频: {video_note}"
    )

    content_obj = raw_post.get("content") or {}
    title = str(content_obj.get("title") or "").strip()
    body = strip_inline_media_placeholders(str(content_obj.get("text_content") or ""))
    if title and title not in body:
        post_text = f"【标题】{title}\n【正文】\n{body or '（正文无文字，请重点查看上传的主帖图片）'}"
    else:
        post_text = body or "（正文无文字，请重点查看上传的主帖图片）"

    if image_items:
        manifest_lines = [
            f"- 主帖图片 #{idx}: 对应第 {idx} 张上传附件"
            for idx in range(1, len(image_items) + 1)
        ]
        image_manifest = "\n".join(manifest_lines)
    else:
        image_manifest = "（本帖无本地主帖图片附件，`images` 数组请输出 `[]`）"

    raw_comments = [c for c in (raw_post.get("comments") or []) if isinstance(c, dict)]
    raw_comments.sort(key=lambda x: int(x.get("likes") or 0), reverse=True)
    comment_lines: List[str] = []
    for idx, c in enumerate(raw_comments[:max_comments], 1):
        c_text = strip_inline_media_placeholders(str(c.get("content") or ""))
        if not c_text:
            continue
        c_author = c.get("author_name") or c.get("author_uid") or "网友"
        c_likes = int(c.get("likes") or 0)
        like_tag = f" | 赞:{c_likes}" if c_likes > 0 else ""
        comment_lines.append(f"{idx}. [{c_author}{like_tag}] {c_text[:2000]}")

    top_comments = "\n".join(comment_lines) if comment_lines else "（暂无抓取到的评论）"

    slots = {
        "POST_META": meta_text,
        "POST_TEXT": post_text,
        "IMAGE_MANIFEST": image_manifest,
        "TOP_COMMENTS": top_comments,
    }
    return slots, has_unfetched_video


def comprehend_single_post(
    raw_post: Dict[str, Any],
    prompt_file: str,
    step2_cfg: Dict[str, Any],
    run_id: str,
) -> Dict[str, Any]:
    """对单条原帖执行多模态忠实理解，返回可直接入库 `pipeline_post_digests` 的精简标准记录。"""
    source = str(raw_post.get("source") or "").strip()
    post_id = str(raw_post.get("post_id") or "").strip()
    post_key = make_post_key(source, post_id)

    image_items = extract_ordered_local_images(
        raw_post,
        max_images=int(step2_cfg.get("max_images_per_post", 4)),
    )
    attachment_paths = [item["local_path"] for item in image_items]
    slots, has_unfetched_video = _build_post_comprehension_inputs(
        raw_post,
        image_items=image_items,
        max_comments=int(step2_cfg.get("max_comments_per_post", 10)),
    )

    rendered_prompt, prompt_meta = render_prompt_template(prompt_file, slots)
    parsed, llm_trace = call_llm_json(
        prompt=rendered_prompt,
        prompt_meta=prompt_meta,
        model=step2_cfg.get("model"),
        fallback_models=step2_cfg.get("fallback_models"),
        file_paths=attachment_paths,
        timeout=int(step2_cfg.get("timeout", 600)),
        preset_model_group=str(step2_cfg.get("preset_model_group") or "medium"),
        validator=_validate_step2_json,
    )

    now_ts = int(time.time())
    content_obj = raw_post.get("content") or {}
    author_obj = raw_post.get("author") or {}
    clean_body = strip_inline_media_placeholders(str(content_obj.get("text_content") or ""))

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
        "raw_text_preview": clean_body[:600],
        "has_unfetched_video": has_unfetched_video,
        "local_images": image_items,
        "has_local_images": bool(image_items),
        "llm_trace": llm_trace,
    }

    if parsed is None:
        base_record.update(
            {
                "status": "failed",
                "is_usable": False,
                "reject_reason": f"LLM理解失败: {llm_trace.get('parse_error') or llm_trace.get('call_status')}",
                "summary": "",
                "valuable_comments": [],
                "images": [],
                "has_usable_visual": False,
            }
        )
        return base_record

    # 将模型输出的图片说明与真实本地主帖图片路径严格对齐
    raw_imgs = parsed.get("images")
    aligned_images: List[Dict[str, Any]] = []
    if isinstance(raw_imgs, list) and image_items:
        for idx, desc_item in enumerate(raw_imgs):
            if not isinstance(desc_item, dict):
                continue
            img_idx = int(desc_item.get("index") or (idx + 1))
            if not (1 <= img_idx <= len(image_items)):
                continue
            matched_file = image_items[img_idx - 1]
            is_high_enough_res = True
            try:
                from PIL import Image

                with Image.open(matched_file["local_path"]) as im_probe:
                    pw, ph = im_probe.size
                    if min(pw, ph) < 120 or max(pw, ph) < 240:
                        is_high_enough_res = False
            except Exception:
                is_high_enough_res = False

            aligned_images.append(
                {
                    "index": img_idx,
                    "local_path": matched_file["local_path"],
                    "remote_url": matched_file["remote_url"],
                    "desc": str(desc_item.get("desc") or "").strip(),
                    "usable_as_visual": bool(desc_item.get("usable_as_visual", True)) and is_high_enough_res,
                    "has_ui_or_watermark": bool(desc_item.get("has_ui_or_watermark", False)),
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
            "is_usable": is_usable,
            "reject_reason": reject_reason if not is_usable else "",
            "summary": summary_text,
            "valuable_comments": valuable_comments,
            "images": aligned_images,
            "has_usable_visual": any(img.get("usable_as_visual") for img in aligned_images),
        }
    )
    return base_record


def run_step2_comprehension(
    db: Optional[PipelineDB] = None,
    profile_name: str = ACTIVE_PROFILE,
    step2_cfg: Optional[Dict[str, Any]] = None,
    run_id: str = "manual_step2",
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    执行 Step 2 批量增量多模态理解。
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
    raw_posts = db.fetch_recent_raw_posts(lookback_hours=lookback_hours, per_source_limit=200)
    all_keys = [make_post_key(p.get("source", ""), p.get("post_id", "")) for p in raw_posts]

    force_recompute = bool(cfg.get("force_recompute", False))
    existing_keys = set() if force_recompute else db.get_existing_digest_keys(all_keys)

    pending_posts = [
        p for p in raw_posts if make_post_key(p.get("source", ""), p.get("post_id", "")) not in existing_keys
    ]

    max_per_run = int(cfg.get("max_new_posts_per_run", 60))
    if max_per_run > 0 and len(pending_posts) > max_per_run:
        pending_posts = round_robin_interleave_by_source(pending_posts, max_total=max_per_run)

    model_label = cfg.get("model") or f"group:{cfg.get('preset_model_group', 'medium')}"
    logger.info(
        f"[Step2/启动] 窗口原帖={len(raw_posts)} | 已缓存跳过={len(existing_keys)} | "
        f"本轮待理解={len(pending_posts)} | 并发={cfg.get('concurrency', 5)} | 路由={model_label}"
    )

    digests: List[Dict[str, Any]] = []
    if pending_posts:
        workers = max(1, min(int(cfg.get("concurrency", 5)), len(pending_posts)))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            fut_map = {
                pool.submit(comprehend_single_post, post, prompt_file, cfg, run_id): post
                for post in pending_posts
            }
            for idx, fut in enumerate(as_completed(fut_map), 1):
                rec = fut.result()
                digests.append(rec)
                if idx % 10 == 0 or idx == len(pending_posts):
                    ok_so_far = sum(1 for d in digests if d["status"] == "ok")
                    usable_so_far = sum(1 for d in digests if d.get("is_usable"))
                    logger.info(
                        f"[Step2/进度] 已完成 {idx}/{len(pending_posts)} "
                        f"(成功:{ok_so_far}, 可用:{usable_so_far}, 带主帖图:{sum(1 for d in digests if d['has_local_images'])})"
                    )

        db.upsert_digests(digests)

    elapsed = round(time.perf_counter() - t0, 2)
    ok_list = [d for d in digests if d.get("status") == "ok"]
    usable_list = [d for d in ok_list if d.get("is_usable")]
    filtered_list = [d for d in ok_list if not d.get("is_usable")]
    failed_list = [d for d in digests if d.get("status") != "ok"]
    with_images = [d for d in digests if d.get("has_local_images")]
    total_imgs_fed = sum(len(d.get("local_images") or []) for d in digests)

    source_counter = Counter(d.get("source") or "unknown" for d in usable_list)
    model_counter = Counter((d.get("llm_trace") or {}).get("model_used") or "unknown" for d in ok_list)

    summary: Dict[str, Any] = {
        "status": "ok" if (not pending_posts or ok_list) else "failed",
        "elapsed_seconds": elapsed,
        "window_raw_count": len(raw_posts),
        "already_digested_count": len(existing_keys),
        "processed_count": len(digests),
        "success_count": len(ok_list),
        "failed_count": len(failed_list),
        "with_images_count": len(with_images),
        "total_images_fed": total_imgs_fed,
        "usable_count": len(usable_list),
        "filtered_count": len(filtered_list),
        "usable_by_source": dict(source_counter),
        "models_used": dict(model_counter),
        "filtered_samples": [
            f"{d['post_key']}({d.get('reject_reason') or '素材不自洽/无实质信息'})" for d in filtered_list[:10]
        ],
        "failed_samples": [
            f"{d['post_key']}({d.get('reject_reason')})" for d in failed_list[:5]
        ],
    }

    logger.info(
        f"[Step2/完成] 耗时={elapsed:.2f}s | 处理={len(digests)} -> 可用={len(usable_list)}, "
        f"拦截={len(filtered_list)}, 失败={len(failed_list)} | 读主帖图={total_imgs_fed}张 | "
        f"可用来源分布={dict(source_counter)} | 命中模型={dict(model_counter)}"
    )
    return summary, digests


if __name__ == "__main__":
    summary_res, sample_digests = run_step2_comprehension(
        step2_cfg={"max_new_posts_per_run": 8, "concurrency": 4},
        run_id="standalone_step2_verify",
    )
    for item in sample_digests:
        print(
            f"\n--- [{item['post_key']}] usable={item['is_usable']} | "
            f"post_imgs={len(item['local_images'])} | model={(item.get('llm_trace') or {}).get('model_used')} ---"
        )
        if not item["is_usable"]:
            print(f"Reject : {item['reject_reason']}")
        print(f"Summary: {item['summary']}")
        if item["images"]:
            print(f"Images : {item['images']}")
        if item["valuable_comments"]:
            print(f"ValComm: {item['valuable_comments']}")
