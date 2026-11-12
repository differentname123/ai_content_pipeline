# -*- coding: utf-8 -*-
"""
外链正文与头图安全提取模块 (`platforms/external_link_extractor.py`)

核心职责：
针对 HackerNews、Reddit 等「标题 + 外部新闻/博客链接」形态的帖子，自动并发抓取外链网页的干净正文与 `og:image` 头图，
使外链帖在第一阶段即成为信息完整的标准帖子，而非孤立的一句话标题。

严苛防污染准则（实测验证）：
1. 仅处理真正的外部网页链接（自动跳过平台自身域名、纯图片/视频/PDF 二进制链接）。
2. 严格要求 `HTTP 200` 且 `Content-Type` 为 HTML，提取出的有效正文不少于 `200` 字符。
3. 严密拦截 Cloudflare / WAF / IP 限制 / 强制登录 / 404 报错页文本（一旦触发拦截特征，宁可不附外链，绝不把报错垃圾混入帖子）。
4. 仅使用项目已有的 `curl_cffi`（回退 `requests`）+ Python 标准库 `html.parser`，零额外第三方解析库依赖。
"""
import html
from concurrent.futures import ThreadPoolExecutor, as_completed
from html.parser import HTMLParser
import logging
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, urlsplit

import requests

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    cffi_requests = None

from common.common_utils import clean_post_text, get_requests_proxies

logger = logging.getLogger(__name__)

SKIP_DOMAINS = (
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

SKIP_SUFFIXES = (
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

IGNORED_TAGS = {
    "script",
    "style",
    "noscript",
    "svg",
    "nav",
    "footer",
    "header",
    "aside",
    "form",
    "button",
    "iframe",
    "select",
    "option",
}

BLOCK_SIGNATURES = (
    "your ip (internet address) has been temporarily restricted",
    "enable javascript and cookies to continue",
    "attention required! | cloudflare",
    "just a moment...",
    "verify you are human",
    "please enable js and disable any ad blocker",
    "access to this page has been denied",
    "403 forbidden",
    "error 403",
    "cf-browser-verification",
)


def is_fetchable_external_article_url(url: str) -> bool:
    """判断 URL 是否为值得尝试抓取正文的外部文章网页链接。"""
    u = str(url or "").strip()
    if not u.startswith(("http://", "https://")):
        return False
    sp = urlsplit(u)
    host = sp.netloc.lower().removeprefix("www.")
    if not host:
        return False
    if any(host == d or host.endswith("." + d) for d in SKIP_DOMAINS):
        return False
    path_low = sp.path.lower()
    if any(path_low.endswith(ext) for ext in SKIP_SUFFIXES):
        return False
    return True


class _CleanArticleExtractor(HTMLParser):
    """基于标准库 HTMLParser 提取网页标题、og:image 与正文自然段。"""

    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self._ignore_depth = 0
        self._in_title = False
        self._current_block_tag: Optional[str] = None
        self._current_buf: List[str] = []

        self.page_title_parts: List[str] = []
        self.og_title: str = ""
        self.og_description: str = ""
        self.og_image: str = ""
        self.blocks: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        t = tag.lower()
        if t in IGNORED_TAGS:
            self._ignore_depth += 1
            return

        if t == "meta":
            ad = {str(k or "").lower(): str(v or "").strip() for k, v in attrs}
            prop = ad.get("property", "").lower() or ad.get("name", "").lower()
            content = html.unescape(ad.get("content", ""))
            if not content:
                return
            if prop == "og:title" and not self.og_title:
                self.og_title = content
            elif prop in ("og:description", "description") and not self.og_description:
                self.og_description = content
            elif prop in ("og:image", "twitter:image", "twitter:image:src") and not self.og_image:
                resolved = urljoin(self.base_url, content)
                if resolved.startswith(("http://", "https://")) and not resolved.lower().endswith((".svg", ".ico")):
                    self.og_image = resolved
            return

        if self._ignore_depth > 0:
            return

        if t == "title":
            self._in_title = True
            return

        if t in ("p", "h1", "h2", "h3", "h4", "li", "blockquote", "pre"):
            self._flush_block()
            self._current_block_tag = t
        elif t == "br" and self._current_block_tag:
            self._current_buf.append("\n")

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()
        if t in IGNORED_TAGS:
            if self._ignore_depth > 0:
                self._ignore_depth -= 1
            return
        if t == "title":
            self._in_title = False
            return
        if self._ignore_depth == 0 and t == self._current_block_tag:
            self._flush_block()

    def handle_data(self, data: str) -> None:
        if self._ignore_depth > 0 or not data:
            return
        if self._in_title:
            self.page_title_parts.append(data)
            return
        if self._current_block_tag:
            self._current_buf.append(data)

    def _flush_block(self) -> None:
        if not self._current_buf:
            self._current_block_tag = None
            return
        raw = html.unescape("".join(self._current_buf)).strip()
        self._current_buf = []
        tag = self._current_block_tag
        self._current_block_tag = None
        if not raw:
            return
        cleaned = " ".join(raw.split()) if tag != "pre" else raw.strip()
        # 过滤导航短碎片（标题除外）
        if tag in ("h1", "h2", "h3", "h4"):
            if len(cleaned) >= 4:
                self.blocks.append(f"### {cleaned}")
        elif len(cleaned) >= 25:
            self.blocks.append(cleaned)


def _looks_like_blocked_or_error_page(title: str, body: str) -> bool:
    combined_low = f"{title}\n{body[:1500]}".lower()
    if any(sig in combined_low for sig in BLOCK_SIGNATURES):
        return True
    title_low = title.lower()
    if any(tok in title_low for tok in ("404", "page not found", "access denied", "just a moment", "forbidden")) and len(body) < 1200:
        return True
    return False


def fetch_external_article(
    url: str,
    timeout: int = 10,
    max_chars: int = 15000,
    proxies: Optional[Dict[str, str]] = None,
) -> Optional[Dict[str, Any]]:
    """
    抓取单条外部网页链接，校验通过后返回结构化文章字典：
    `{"url": str, "title": str, "description": str, "text": str, "og_image": Optional[str], "char_count": int}`
    若非 200、非 HTML、疑似拦截页或有效正文不足 200 字符，安全返回 `None`。
    """
    if not is_fetchable_external_article_url(url):
        return None

    effective_proxies = proxies if proxies is not None else (get_requests_proxies() or {})
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
    }

    try:
        if cffi_requests is not None:
            resp = cffi_requests.get(
                url,
                headers=headers,
                impersonate="chrome",
                proxies=effective_proxies,
                timeout=timeout,
                allow_redirects=True,
            )
        else:
            resp = requests.get(
                url,
                headers=headers,
                proxies=effective_proxies,
                timeout=timeout,
                allow_redirects=True,
            )
    except Exception as e:
        logger.debug(f"[外链提取] 请求失败 {url}: {e}")
        return None

    if resp.status_code != 200:
        return None

    ctype = str(resp.headers.get("content-type") or "").lower()
    if ctype and not any(t in ctype for t in ("text/html", "application/xhtml")):
        return None

    raw_html = resp.text or ""
    if len(raw_html) < 250:
        return None

    final_url = str(getattr(resp, "url", None) or url)
    extractor = _CleanArticleExtractor(base_url=final_url)
    try:
        extractor.feed(raw_html)
        extractor.close()
        extractor._flush_block()
    except Exception as e:
        logger.debug(f"[外链提取] HTML解析异常 {url}: {e}")
        return None

    dedup_blocks: List[str] = []
    seen_b = set()
    for b in extractor.blocks:
        if b not in seen_b:
            seen_b.add(b)
            dedup_blocks.append(b)

    article_text = clean_post_text("\n\n".join(dedup_blocks))
    if not article_text and extractor.og_description and len(extractor.og_description) >= 200:
        article_text = clean_post_text(extractor.og_description)

    title = clean_post_text(extractor.og_title or "".join(extractor.page_title_parts))
    if len(article_text) < 200:
        return None
    if _looks_like_blocked_or_error_page(title, article_text):
        return None

    if max_chars > 0 and len(article_text) > max_chars:
        article_text = article_text[:max_chars].rstrip()

    return {
        "url": final_url,
        "title": title,
        "description": clean_post_text(extractor.og_description),
        "text": article_text,
        "og_image": extractor.og_image or None,
        "char_count": len(article_text),
    }


def _get_post_external_url(post: Dict[str, Any]) -> str:
    meta = post.get("metadata") or {}
    extra = meta.get("platform_extra") or {}
    ext = str(meta.get("external_url") or extra.get("external_url") or "").strip()
    return ext if is_fetchable_external_article_url(ext) else ""


EXTERNAL_TEXT_MARKER = "【原帖关联外链全文"


def enrich_posts_with_external_articles(
    posts: List[Dict[str, Any]],
    max_workers: int = 8,
    timeout: int = 10,
    attach_og_image_when_empty: bool = True,
    proxies: Optional[Dict[str, str]] = None,
) -> int:
    """
    批量并发为携带外部新闻/博客链接的帖子抓取干净正文与头图，并**直接合并补全进原帖正文 `content.text_content`**：
    - 让原本“只有一行标题+外链”的帖子在阶段 1 即成为完整的原帖全文，下游模块无需区分或特殊处理“外链文”；
    - 若原帖本身无任何配图且非视频帖，自动将外链 `og:image` 补充进 `media.images` 供后续下载与参考。
    返回成功补全外链正文的帖子篇数。
    """
    if not posts:
        return 0

    targets: List[Tuple[Dict[str, Any], str]] = []
    for p in posts:
        if not isinstance(p, dict):
            continue
        meta = p.setdefault("metadata", {})
        extra = meta.get("platform_extra") or {}
        cur_txt = str((p.get("content") or {}).get("text_content") or "")
        if EXTERNAL_TEXT_MARKER in cur_txt or meta.get("external_link_enriched") or extra.get("external_link_enriched"):
            continue
        ext_url = _get_post_external_url(p)
        if ext_url:
            targets.append((p, ext_url))

    if not targets:
        return 0

    enriched_count = 0
    with ThreadPoolExecutor(max_workers=max(1, min(max_workers, len(targets)))) as pool:
        fut_map = {
            pool.submit(fetch_external_article, ext_url, timeout=timeout, proxies=proxies): p
            for p, ext_url in targets
        }
        for fut in as_completed(fut_map):
            post = fut_map[fut]
            try:
                art = fut.result()
            except Exception:
                art = None
            if not art or not str(art.get("text") or "").strip():
                continue

            enriched_count += 1
            content_obj = post.setdefault("content", {})
            cur_text = str(content_obj.get("text_content") or "").strip()
            ext_text = str(art["text"]).strip()[:10000]
            ext_title = str(art.get("title") or "").strip()
            domain = urlparse(str(art.get("url") or "")).netloc.lower().removeprefix("www.")
            header = (
                f"{EXTERNAL_TEXT_MARKER} | 标题: {ext_title} | 来源: {domain}】"
                if ext_title
                else f"{EXTERNAL_TEXT_MARKER} | 来源: {domain}】"
            )
            if EXTERNAL_TEXT_MARKER not in cur_text:
                content_obj["text_content"] = f"{cur_text}\n\n{header}\n{ext_text}".strip() if cur_text else f"{header}\n{ext_text}"

            meta = post.setdefault("metadata", {})
            meta["external_link_enriched"] = True
            if isinstance(meta.get("platform_extra"), dict):
                meta["platform_extra"]["external_link_enriched"] = True

            # 若原帖无主帖图片且非视频帖，将真实 og:image 纳入 media.images 以便统一按 dHash 下载落地
            media = post.setdefault("media", {})
            has_video = bool(media.get("video_url") or int(media.get("video_duration") or 0) > 0)
            og_img = art.get("og_image")
            if attach_og_image_when_empty and og_img and not has_video and not (media.get("images") or []):
                media["images"] = [og_img]
                media["cover_image"] = og_img
                meta["og_image_url"] = og_img
                if isinstance(meta.get("platform_extra"), dict):
                    meta["platform_extra"]["og_image_url"] = og_img

    return enriched_count
