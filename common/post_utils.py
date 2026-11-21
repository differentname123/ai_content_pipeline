# -*- coding: utf-8 -*-
"""
帖子通用解析、媒体判定与账号安全隔离守卫 (`common/post_utils.py`)

收拢跨模块复用的纯函数，避免活跃业务模块（如 `x_growth/`、`platforms/`）依赖已淘汰的实验包。
"""
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit


class AccountIsolationError(RuntimeError):
    """当采集或只读爬虫逻辑违规尝试读取主号目录 `x_myself` 时抛出。"""


_SKIP_EXTERNAL_DOMAINS = (
    "news.ycombinator.com",
    "reddit.com",
    "redd.it",
    "imgur.com",
    "x.com",
    "twitter.com",
    "t.co",
    "youtube.com",
    "youtu.be",
    "tiktok.com",
    "instagram.com",
    "facebook.com",
    "binance.com",
    "okx.com",
)

_SKIP_EXTERNAL_SUFFIXES = (
    ".pdf",
    ".zip",
    ".tar",
    ".gz",
    ".mp4",
    ".webm",
    ".mov",
    ".mp3",
    ".wav",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".svg",
    ".ico",
)

_VIDEO_THUMB_TOKENS = (
    "amplify_video_thumb",
    "ext_tw_video_thumb",
    "tweet_video_thumb",
    "v.redd.it",
)


def assert_not_x_myself_dir(user_data_dir: Optional[str], *, context: str = "") -> None:
    """
    物理隔离硬红线守卫：
    严禁任何采集、爬取或 Cookie 提取代码触碰主号目录 `x_myself`。
    空字符串（代表 100% 免登模式）允许放行。
    """
    raw = str(user_data_dir or "").strip()
    if not raw:
        return
    norm_raw = raw.replace("\\", "/").lower()
    norm_abs = os.path.normcase(os.path.abspath(raw)).replace("\\", "/").lower()
    parts_raw = {p for p in Path(norm_raw).parts if p}
    parts_abs = {p for p in Path(norm_abs).parts if p}
    if (
        "x_myself" in parts_raw
        or "x_myself" in parts_abs
        or "/x_myself" in norm_raw
        or "/x_myself" in norm_abs
        or "x_myself" in norm_raw
    ):
        ctx = f" [{context}]" if context else ""
        raise AccountIsolationError(
            f"安全隔离红线拦截{ctx}：禁止采集或只读读取模块触碰主号目录 `{raw}` (x_myself)！"
        )


def is_fetchable_external_article_url(url: str) -> bool:
    """判断 URL 是否为值得识别提取正文的外部文章网页链接（排除社交站内链及二进制文件）。"""
    u = str(url or "").strip()
    if not u.startswith(("http://", "https://")):
        return False
    sp = urlsplit(u)
    host = sp.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    if not host:
        return False
    if any(host == d or host.endswith("." + d) for d in _SKIP_EXTERNAL_DOMAINS):
        return False
    path_low = sp.path.lower()
    if any(path_low.endswith(ext) for ext in _SKIP_EXTERNAL_SUFFIXES):
        return False
    return True


def is_video_post(raw_post: Dict[str, Any]) -> bool:
    """
    100% 确定性判断帖子是否为含视频帖：
    - `media.video_url` 非空 或 `media.video_duration > 0`
    - `card_type == "BUZZ_VIDEO"`
    - 被引用推文标记包含视频 (`metadata.quoted_post.has_video` / `metadata.platform_extra.quoted_post.has_video`)
    - 媒体或原链接为平台视频缩略图/视频资源 (`amplify_video_thumb` / `ext_tw_video_thumb` / `tweet_video_thumb` / `v.redd.it`)
    """
    if not isinstance(raw_post, dict):
        return False
    media = raw_post.get("media") or {}
    if media.get("video_url") or int(media.get("video_duration") or 0) > 0:
        return True
    if str(raw_post.get("card_type") or "").upper() == "BUZZ_VIDEO":
        return True

    meta = raw_post.get("metadata") or {}
    extra = meta.get("platform_extra") or {}
    qp = meta.get("quoted_post") or extra.get("quoted_post") or {}
    if isinstance(qp, dict) and qp.get("has_video"):
        return True

    url_candidates = list(media.get("images") or []) + [
        media.get("cover_image"),
        meta.get("url"),
        meta.get("external_url"),
    ]
    for u in url_candidates:
        u_low = str(u or "").lower()
        if any(tok in u_low for tok in _VIDEO_THUMB_TOKENS):
            return True
    return False


def strip_inline_media_placeholders(text: str) -> str:
    """剥离归一化阶段追加在文末的 `[插图: https://...]` / `[封面图: https://...]` 占位符，保留原始正文。"""
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
    """兼容旧记录：若采集记录将外链存在 `metadata.external_article`，安全提取以合并全文。"""
    meta = raw_post.get("metadata") or {}
    pextra = meta.get("platform_extra") or {}
    ext = meta.get("external_article") or pextra.get("external_article")
    if isinstance(ext, dict) and str(ext.get("text") or "").strip():
        return ext
    return None


def get_unified_post_text(
    raw_post: Dict[str, Any],
    *,
    strip_media_markers: bool = True,
    include_comments: bool = False,
    max_comments: int = 5,
) -> str:
    """
    获取统一的帖子正文全文：合并 `content.title`、`content.text_content` 与历史 `external_article` 字段，
    默认剥离归一化末尾 `[插图: ...]` 标记。
    """
    content_obj = raw_post.get("content") or {}
    title = str(content_obj.get("title") or "").strip()
    raw_body = str(content_obj.get("text_content") or "")
    body = strip_inline_media_placeholders(raw_body) if strip_media_markers else raw_body.strip()
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
            hdr = (
                f"【原帖关联外链全文 | 标题: {ext_title} | 来源: {ext_domain}】"
                if ext_title
                else f"【原帖关联外链全文 | 来源: {ext_domain}】"
            )
            unified = f"{unified}\n\n{hdr}\n{ext_body}".strip() if unified else f"{hdr}\n{ext_body}"

    if include_comments:
        comments = raw_post.get("comments") or []
        lines: List[str] = []
        for c in comments:
            if not isinstance(c, dict):
                continue
            c_txt = strip_inline_media_placeholders(str(c.get("content") or c.get("text") or ""))
            if not c_txt:
                continue
            lines.append(c_txt)
            if len(lines) >= max_comments:
                break
        if lines:
            unified = f"{unified}\n\n【热门评论】\n" + "\n".join(f"- {ln}" for ln in lines)

    return unified.strip()
