# -*- coding: utf-8 -*-
"""
Module 1 (Step 2)：单帖「图文 + 神评」多模态完整理解模块 (`step2_comprehension.py`)

本质职责：
将社交平台上“文字极短、核心信息藏在图片或神评里”的散装原始帖子，通过多模态大模型一次性看图+读文+看热评，
还原为一张信息 100% 自洽的结构化情报卡 (`pipeline_post_digests`)，供下游零信息损耗地做全局选题与原创生产。

可独立运行调试：直接在 IDE 运行本文件，即可对当前库中未理解的新帖执行增量多模态理解并打印抽样结果。
"""
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from content_pipeline.llm_gateway import call_llm_json, render_prompt_template
from content_pipeline.pipeline_config import (
    ACTIVE_PROFILE,
    CONTENT_PROFILES,
    STEP2_CONFIG,
)
from content_pipeline.pipeline_db import PipelineDB, make_post_key
from content_pipeline.pipeline_logger import get_pipeline_logger

SUPPORTED_IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp", ".gif"}


def extract_ordered_local_images(
    raw_post: Dict[str, Any],
    max_images: int = 3,
    include_comment_image_fallback: bool = True,
) -> List[Dict[str, str]]:
    """
    按原帖图片出现顺序提取本地已下载且真实存在的图片文件列表。
    每项结构: `{"origin": "post" | "comment", "remote_url": str, "local_path": str}`
    """
    media = raw_post.get("media") or {}
    local_map = media.get("local_mapping") or {}
    ordered_urls: List[str] = []

    for u in media.get("images") or []:
        if u and u not in ordered_urls:
            ordered_urls.append(u)
    cover = media.get("cover_image")
    if cover and cover not in ordered_urls:
        ordered_urls.append(cover)
    for u in local_map.keys():
        if u and u not in ordered_urls:
            ordered_urls.append(u)

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

    # 若主帖本身无图，但评论区热评带有本地图片，可选取最高赞的 1 张评论图辅助理解
    if not results and include_comment_image_fallback:
        comments = sorted(
            [c for c in (raw_post.get("comments") or []) if isinstance(c, dict)],
            key=lambda x: int(x.get("likes") or 0),
            reverse=True,
        )
        for c in comments:
            c_map = c.get("local_mapping") or {}
            for url, local_p in c_map.items():
                p = Path(local_p)
                if p.is_file() and p.suffix.lower() in SUPPORTED_IMAGE_SUFFIXES:
                    results.append(
                        {"origin": "comment", "remote_url": str(url), "local_path": str(p.resolve())}
                    )
                    return results

    return results


def _build_post_comprehension_inputs(
    raw_post: Dict[str, Any],
    image_items: List[Dict[str, str]],
    max_comments: int = 8,
) -> Dict[str, str]:
    """将标准化原帖组装为清晰易读的提示词插槽字典。"""
    source = raw_post.get("source") or "unknown"
    post_id = raw_post.get("post_id") or ""
    author = raw_post.get("author") or {}
    author_label = author.get("author_name") or author.get("username") or raw_post.get("author_id") or "匿名"
    eng = raw_post.get("engagement") or {}
    meta = raw_post.get("metadata") or {}

    pub_ts = int(raw_post.get("publish_time") or 0)
    pub_str = (
        datetime.fromtimestamp(pub_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        if pub_ts > 0
        else "未知时间"
    )

    meta_text = (
        f"- 来源平台: {source} | 帖子ID: {post_id} | 作者: {author_label}\n"
        f"- 发布时间: {pub_str} | 原文链接: {meta.get('url') or '无'}\n"
        f"- 当前互动表现: 浏览={eng.get('view_count', 0)}, 点赞={eng.get('like_count', 0)}, "
        f"评论={eng.get('comment_count', 0)}, 分享/转发={eng.get('share_count', 0)}, "
        f"收藏={eng.get('bookmark_count', 0)}, 综合热力分={eng.get('viral_score', 0)}"
    )

    content_obj = raw_post.get("content") or {}
    title = str(content_obj.get("title") or "").strip()
    body = str(content_obj.get("text_content") or "").strip()
    if title and title not in body:
        post_text = f"【标题】{title}\n【正文】\n{body or '（正文无文字，请重点查看图片）'}"
    else:
        post_text = body or "（正文无文字，请重点查看图片附件）"

    if image_items:
        manifest_lines = []
        for idx, item in enumerate(image_items, 1):
            origin_label = "主帖配图" if item["origin"] == "post" else "前排热评配图"
            manifest_lines.append(
                f"- 附件图片 #{idx} ({origin_label}): 文件名 `{Path(item['local_path']).name}`"
            )
        image_manifest = "\n".join(manifest_lines)
    else:
        image_manifest = "（本帖无本地图片附件）"

    raw_comments = [c for c in (raw_post.get("comments") or []) if isinstance(c, dict)]
    raw_comments.sort(key=lambda x: int(x.get("likes") or 0), reverse=True)
    comment_lines = []
    for idx, c in enumerate(raw_comments[:max_comments], 1):
        c_text = str(c.get("content") or "").strip()
        if not c_text:
            continue
        c_author = c.get("author_name") or c.get("author_uid") or "网友"
        c_likes = int(c.get("likes") or 0)
        comment_lines.append(f"{idx}. [{c_author} | 赞:{c_likes}] {c_text[:300]}")

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
) -> Dict[str, Any]:
    """对单条原帖执行多模态完整理解，返回可直接入库 `pipeline_post_digests` 的标准记录。"""
    source = str(raw_post.get("source") or "").strip()
    post_id = str(raw_post.get("post_id") or "").strip()
    post_key = make_post_key(source, post_id)

    image_items = extract_ordered_local_images(
        raw_post,
        max_images=int(step2_cfg.get("max_images_per_post", 3)),
        include_comment_image_fallback=bool(
            step2_cfg.get("include_top_comment_image_if_post_no_image", True)
        ),
    )
    attachment_paths = [item["local_path"] for item in image_items]
    slots = _build_post_comprehension_inputs(
        raw_post,
        image_items=image_items,
        max_comments=int(step2_cfg.get("max_comments_per_post", 8)),
    )

    rendered_prompt, prompt_meta = render_prompt_template(prompt_file, slots)
    parsed, llm_trace = call_llm_json(
        prompt=rendered_prompt,
        prompt_meta=prompt_meta,
        model=step2_cfg.get("model"),
        fallback_models=step2_cfg.get("fallback_models"),
        file_paths=attachment_paths,
        timeout=int(step2_cfg.get("timeout", 120)),
    )

    now_ts = int(time.time())
    content_obj = raw_post.get("content") or {}
    author_obj = raw_post.get("author") or {}

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
        "raw_text_preview": str(content_obj.get("text_content") or "")[:500],
        "local_images": image_items,
        "has_local_images": bool(image_items),
        "llm_trace": llm_trace,
    }

    if parsed is None:
        base_record.update(
            {
                "status": "failed",
                "is_usable": False,
                "filter_reason": f"LLM理解失败: {llm_trace.get('parse_error') or llm_trace.get('call_status')}",
                "domain": "未知",
                "complete_summary": "",
                "core_hook": "",
                "key_entities": [],
                "image_descriptions": [],
                "comment_essence": [],
            }
        )
        return base_record

    # 将模型输出的图片说明与真实本地路径按顺序对齐绑定，供 Step 4 直接精准选图/改图
    raw_img_descs = parsed.get("image_descriptions")
    aligned_img_descs: List[Dict[str, Any]] = []
    if isinstance(raw_img_descs, list):
        for idx, desc_item in enumerate(raw_img_descs):
            if not isinstance(desc_item, dict):
                continue
            img_idx = int(desc_item.get("image_index") or (idx + 1))
            matched_file = image_items[img_idx - 1] if 1 <= img_idx <= len(image_items) else None
            aligned_img_descs.append(
                {
                    "image_index": img_idx,
                    "origin": matched_file["origin"] if matched_file else "post",
                    "local_path": matched_file["local_path"] if matched_file else None,
                    "remote_url": matched_file["remote_url"] if matched_file else None,
                    "visual_fact": str(desc_item.get("visual_fact") or "").strip(),
                    "role": str(desc_item.get("role") or "news_visual").strip(),
                    "has_watermark_or_ui": bool(desc_item.get("has_watermark_or_ui", False)),
                }
            )

    known_keys = {
        "is_usable",
        "filter_reason",
        "domain",
        "complete_summary",
        "core_hook",
        "key_entities",
        "image_descriptions",
        "comment_essence",
    }
    extra_fields = {k: v for k, v in parsed.items() if k not in known_keys}

    is_usable = bool(parsed.get("is_usable", True))
    complete_summary = str(parsed.get("complete_summary") or "").strip()
    if not complete_summary:
        is_usable = False

    base_record.update(
        {
            "status": "ok",
            "is_usable": is_usable,
            "filter_reason": str(parsed.get("filter_reason") or "").strip(),
            "domain": str(parsed.get("domain") or "综合").strip(),
            "complete_summary": complete_summary,
            "core_hook": str(parsed.get("core_hook") or "").strip(),
            "key_entities": [str(x).strip() for x in (parsed.get("key_entities") or []) if str(x).strip()],
            "image_descriptions": aligned_img_descs,
            "comment_essence": [
                str(x).strip() for x in (parsed.get("comment_essence") or []) if str(x).strip()
            ],
            "extra_fields": extra_fields,
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
    raw_posts = db.fetch_recent_raw_posts(lookback_hours=lookback_hours, limit=600)
    all_keys = [make_post_key(p.get("source", ""), p.get("post_id", "")) for p in raw_posts]

    force_recompute = bool(cfg.get("force_recompute", False))
    existing_keys = set() if force_recompute else db.get_existing_digest_keys(all_keys)

    pending_posts = [
        p for p in raw_posts if make_post_key(p.get("source", ""), p.get("post_id", "")) not in existing_keys
    ]

    max_per_run = int(cfg.get("max_new_posts_per_run", 40))
    if max_per_run > 0 and len(pending_posts) > max_per_run:
        # 优先确保各平台高热帖均衡覆盖，再按 viral_score 补齐，避免单一高产平台占满名额
        by_src: Dict[str, List[Dict[str, Any]]] = {}
        for p in pending_posts:
            by_src.setdefault(str(p.get("source") or "other"), []).append(p)
        balanced: List[Dict[str, Any]] = []
        round_idx = 0
        while len(balanced) < max_per_run:
            added_any = False
            for src in sorted(by_src.keys()):
                bucket = by_src[src]
                if round_idx < len(bucket):
                    balanced.append(bucket[round_idx])
                    added_any = True
                    if len(balanced) >= max_per_run:
                        break
            if not added_any:
                break
            round_idx += 1
        pending_posts = balanced

    logger.info(
        f"[Step2/启动] 窗口原帖={len(raw_posts)} | 已缓存跳过={len(existing_keys)} | "
        f"本轮待理解={len(pending_posts)} | 并发={cfg.get('concurrency', 4)} | 主模型={cfg.get('model')}"
    )

    digests: List[Dict[str, Any]] = []
    if pending_posts:
        workers = max(1, min(int(cfg.get("concurrency", 4)), len(pending_posts)))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            fut_map = {
                pool.submit(comprehend_single_post, post, prompt_file, cfg, run_id): post
                for post in pending_posts
            }
            for idx, fut in enumerate(as_completed(fut_map), 1):
                rec = fut.result()
                digests.append(rec)
                # 每完成 10 条或全部完成时输出一行轻量心跳，绝不刷屏
                if idx % 10 == 0 or idx == len(pending_posts):
                    ok_so_far = sum(1 for d in digests if d["status"] == "ok")
                    logger.info(
                        f"[Step2/进度] 已完成 {idx}/{len(pending_posts)} (成功:{ok_so_far}, "
                        f"带图:{sum(1 for d in digests if d['has_local_images'])})"
                    )

        db.upsert_digests(digests)

    elapsed = round(time.perf_counter() - t0, 2)
    ok_list = [d for d in digests if d.get("status") == "ok"]
    usable_list = [d for d in ok_list if d.get("is_usable")]
    filtered_list = [d for d in ok_list if not d.get("is_usable")]
    failed_list = [d for d in digests if d.get("status") != "ok"]
    with_images = [d for d in digests if d.get("has_local_images")]
    total_imgs_fed = sum(len(d.get("local_images") or []) for d in digests)

    domain_counter = Counter(d.get("domain") or "未分类" for d in usable_list)
    model_counter = Counter(
        (d.get("llm_trace") or {}).get("model_used") or "unknown" for d in ok_list
    )

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
        "domain_distribution": dict(domain_counter),
        "models_used": dict(model_counter),
        "filtered_samples": [
            f"{d['post_key']}({d.get('filter_reason') or '无实质内容'})" for d in filtered_list[:8]
        ],
        "failed_samples": [
            f"{d['post_key']}({d.get('filter_reason')})" for d in failed_list[:5]
        ],
    }

    logger.info(
        f"[Step2/完成] 耗时={elapsed:.2f}s | 处理={len(digests)} -> 可用={len(usable_list)}, "
        f"过滤={len(filtered_list)}, 失败={len(failed_list)} | 读图={total_imgs_fed}张 | 领域={dict(domain_counter)}"
    )
    return summary, digests


if __name__ == "__main__":
    # 独立验证入口：默认先跑 6 条跨平台真实样本快速检验看图还原质量
    summary_res, sample_digests = run_step2_comprehension(
        step2_cfg={"max_new_posts_per_run": 6, "concurrency": 3},
        run_id="standalone_step2_verify",
    )
    for item in sample_digests:
        print(
            f"\n--- [{item['post_key']}] usable={item['is_usable']} | domain={item['domain']} | "
            f"images={len(item['local_images'])} ---"
        )
        print(f"Hook   : {item['core_hook']}")
        print(f"Summary: {item['complete_summary']}")
        if item["image_descriptions"]:
            print(f"ImgFact: {item['image_descriptions'][0]['visual_fact']}")
        if item["comment_essence"]:
            print(f"Comment: {item['comment_essence']}")
