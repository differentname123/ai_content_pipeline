# -*- coding: utf-8 -*-
"""
Reddit 平台专用采集与差异抹平适配器 (RedditHarvester)

================================================================================
【运维速查手册 (Ops Quick Guide) —— 日常巡检与修改必读】
================================================================================
1. 是否使用登录账号？
   - **否（100% 免登录 / 0 账号模式）**。无需准备任何 Reddit 账号或 Cookie，零封号风险。

2. 当前到底在拉取谁？
   - 使用 `+` 号单请求聚合拉取 `harvester_runner.py` -> `PLATFORM_HARVEST_CONFIGS["reddit"]["subreddits"]`
     配置的 10 个核心英文板块（默认：`r/wallstreetbets`, `r/CryptoCurrency`, `r/stocks`, `r/options`,
     `r/FluentInFinance`, `r/technology`, `r/singularity`, `r/algotrading`, `r/PersonalFinance`, `r/antiwork`）
     过去 24 小时最高赞热帖 (`sort="top"`, `time_filter="day"`)。

3. 平常想修改应该怎么改？
   - 【增删监控板块 (Subreddit)】：修改 `platforms/harvester_runner.py` 中 `"reddit"` 配置下的 `"subreddits"` 列表（直接填板块名，不带 `r/`）。
   - 【切换热榜排序或时间窗口】：修改 `"reddit"` 配置下的 `"sort"`（可选 `"top"` / `"hot"` / `"rising"`）与 `"time_filter"`（可选 `"hour"` / `"day"` / `"week"`）。
   - 【按关键词过滤】：在 `"reddit"` 配置下增加 `"keywords": ["loss", "million", "yolo"]`。

================================================================================
【Reddit 接口探索与踩坑档案 (Exploration & Pitfall Log)】
维护必读：以下记录了我们在 2026 年实测过的所有 Reddit 采集路径。请勿重复尝试已标记为 ❌ 的废路！
================================================================================

✅ 1. [已采用 / 主力通道A] 多板块 `+` 拼接单请求 Atom RSS (`/r/sub1+sub2+.../{sort}/.rss?t=day&limit=30`)
   - 原理：利用 Reddit 原生支持用 `+` 号把 10~15 个 Subreddit 合并为一个虚拟板块的特性，
           配合桌面端 Firefox `User-Agent` 发起 **1 次 HTTP GET 请求**，直接拉回所有板块过去 24h 的最高赞爆款帖。
   - 优势：100% 免登录、0 账号成本、包含标题、正文、外链高清图 (`i.redd.it` / `imgur.com`)、作者与所属板块，单次请求永不触发 429！

✅ 2. [已采用 / 主力通道B] 单帖神评树 RSS (`{post_url}/.rss?limit=15`) 与用户发帖流 (`/user/{username}/submitted/.rss`)
   - 原理：在任意 Reddit 帖子链接末尾加上 `/.rss`，第 0 个 `<entry>` 是主帖，第 1~N 个 `<entry>` 即为按热度排序的前排神评！
           同理，访问 `/user/{username}/submitted/.rss` 可免登拉取任意 Reddit 传奇交易员（如 `DeepFuckingValue`）的历史与最新发帖。
   - 实测状态：均稳定返回 `HTTP 200`。

❌ 3. [已废弃 / 禁止踩坑] 在 URL 后追加 `.json` 接口 (`https://www.reddit.com/r/wallstreetbets/top.json`)
   - 失败原因：Reddit 防火墙自 2024~2026 年起对未携带官方 OAuth2 令牌的机房/代理 IP 请求 `.json` 接口实施一刀切拦截，
               实测直接返回 `HTTP 403 Blocked`（即使更换 User-Agent 依然 403）。

❌ 4. [已废弃 / 禁止踩坑] 全局搜索 `/search.rss?q=...` 或板块搜索 `/r/sub/search.rss?q=...`
   - 失败原因：实测全局 `/search.rss` 直接返回 `HTTP 403`，板块内 `/r/sub/search.rss` 直接返回 `HTTP 429 Too Many Requests`。
   - 替代方案：本适配器的 `search_or_topic_posts()` 采用「单请求拉取多板块 `top/.rss` 或 `hot/.rss` 50 条热帖 + 本地关键词/正则匹配」，
               100% 绕过服务端搜索限流，稳定可靠。

❌ 5. [已废弃 / 禁止踩坑] 循环逐个请求单个 Subreddit 或紧跟主请求后立即密集拉取多帖评论
   - 失败原因：实测抓取响应头发现，Reddit 对未登录代理 IP 的 `.rss` 接口实施严格的 **`1 次请求 / 60 秒` 令牌桶限流**
               （响应头返回 `x-ratelimit-used: 1`, `x-ratelimit-remaining: 0.0`, `x-ratelimit-reset: 40~60`）。
   - 核心铁律：
     ① 板块热帖拉取**必须且只需**使用 `/r/sub1+sub2+...+sub10/top/.rss` **单次请求拿齐所有板块热帖**（消耗 1 个令牌即拿回 25~30 条爆款图文，100% 返回 `HTTP 200`）；
     ② 定时任务中默认只做这 1 次聚合拉取（`fetch_comments_for_top_n=0`），若单独调用 `fetch_post_comments()` 则自动遵守令牌桶余量或等待冷却重置。
================================================================================
"""
import html
from html.parser import HTMLParser
import logging
import time
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional, Tuple

import requests

from common.common_utils import (
    PLATFORM_REDDIT,
    clean_post_text,
    normalize_publish_timestamp,
    safe_int,
)
from platforms.harvesters.base_harvester import BasePlatformHarvester

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 15
FIREFOX_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0"

# 默认高价值金钱爽文、暴富暴负、科技暗黑吃瓜 10 大黄金板块
DEFAULT_GOLDMINE_SUBREDDITS = [
    "wallstreetbets",       # 期权豪赌、暴富与破产交割单圣地
    "CryptoCurrency",       # 加密圈最大英文主战场
    "stocks",               # 美股科技巨头与财报大瓜
    "options",              # 真实期权盈亏截图与复盘
    "FluentInFinance",      # 极易引发转发与争论的财富认知图
    "technology",           # 科技圈丑闻与商业政策巨震
    "singularity",          # AI 突破与硅谷内幕
    "algotrading",          # 量化交易与策略实盘故事
    "PersonalFinance",      # 极端财务困境、遗产暴富与奇葩金钱故事
    "antiwork",             # 裁员、高管荒诞邮件与职场金钱冲突
]


class _RedditHTMLParser(HTMLParser):
    """使用 Python 标准库 HTMLParser 解析 Reddit Atom `<content>`，零正则提取正文、配图与视频链接。"""

    def __init__(self) -> None:
        super().__init__()
        self.text_parts: List[str] = []
        self.image_urls: List[str] = []
        self.video_urls: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        attr_dict = dict(attrs)
        candidate_url = ""
        if tag == "a":
            candidate_url = attr_dict.get("href") or ""
        elif tag == "img":
            candidate_url = attr_dict.get("src") or ""
        if candidate_url.startswith(("http://", "https://")):
            clean_u = html.unescape(candidate_url)
            u_low = clean_u.lower()
            if "v.redd.it" in u_low and clean_u not in self.video_urls:
                self.video_urls.append(clean_u)
            elif ("i.redd.it" in u_low or "preview.redd.it" in u_low or "imgur.com" in u_low) and clean_u not in self.image_urls:
                self.image_urls.append(clean_u)
        if tag in ("p", "br", "div", "li", "tr"):
            self.text_parts.append("\n")

    def handle_data(self, data: str) -> None:
        if data:
            self.text_parts.append(data)


def _strip_reddit_prefix(raw: str) -> str:
    s = str(raw or "").strip()
    for prefix in ("/u/", "u/", "/r/", "r/"):
        if s.lower().startswith(prefix):
            return s[len(prefix):].strip()
    return s


def _extract_reddit_base36_id(raw_target: str) -> str:
    """从完整帖子 URL 或 `reddit_xxx` / `t3_xxx` 中提取纯净 base36 ID。"""
    s = str(raw_target or "").strip()
    if "/comments/" in s:
        after = s.split("/comments/", 1)[1].strip("/")
        return after.split("/", 1)[0].strip()
    if "/gallery/" in s:
        after = s.split("/gallery/", 1)[1].strip("/")
        return after.split("/", 1)[0].strip()
    return s.replace("reddit_", "").replace("t3_", "").strip("/ ")


def _deduplicate_reddit_image_urls(urls: List[str]) -> List[str]:
    """
    Reddit Atom HTML 往往对同一张图输出 `<img src="https://preview.redd.it/xxx.png?width=140&height=51...">`
    低清缩略图（有时内嵌图甚至不带 `<a href="https://i.redd.it/xxx.png">` 链接）。
    1) 对 `preview.redd.it/<asset>.(png|jpg|jpeg|webp|gif)` 顶层用户原图，直接无损升级为 `https://i.redd.it/<asset>` 原始高清图；
    2) 按底层资源文件名去重，优先保留 `i.redd.it` 原图或非 `crop=1:1` 的高清大图，彻底杜绝 140px 邮票缩略图入库。
    """
    from urllib.parse import parse_qs, urlsplit

    def _upgrade_preview_url(u: str) -> str:
        sp = urlsplit(u)
        if (
            sp.netloc.lower() == "preview.redd.it"
            and sp.path.count("/") == 1
            and sp.path.lower().endswith((".png", ".jpg", ".jpeg", ".webp", ".gif"))
        ):
            return f"https://i.redd.it{sp.path}"
        return u

    def _score_url(u: str) -> int:
        sp = urlsplit(u)
        qs = parse_qs(sp.query)
        if "i.redd.it" in sp.netloc:
            return 100_000
        if "crop" in qs:
            return 10
        widths = qs.get("width") or ["0"]
        try:
            return max(100, int(widths[0]))
        except ValueError:
            return 500

    best_by_asset: Dict[str, str] = {}
    asset_order: List[str] = []
    for raw_u in urls:
        u = _upgrade_preview_url(raw_u)
        sp = urlsplit(u)
        asset_name = sp.path.rsplit("/", 1)[-1].lower() if ("redd.it" in sp.netloc) else u
        if not asset_name:
            asset_name = u
        if asset_name not in best_by_asset:
            asset_order.append(asset_name)
            best_by_asset[asset_name] = u
        elif _score_url(u) > _score_url(best_by_asset[asset_name]):
            best_by_asset[asset_name] = u

    return [best_by_asset[k] for k in asset_order]


def _parse_reddit_html(raw_html: str) -> Tuple[str, List[str], List[str]]:
    """从 Reddit Atom HTML 内容中提取纯文本、去重高清图片列表与视频链接列表，剔除固定页脚。"""
    if not raw_html:
        return "", [], []
    parser = _RedditHTMLParser()
    parser.feed(raw_html)
    parser.close()
    raw_text = "".join(parser.text_parts)
    # 剥离 Reddit RSS 固定页脚 "submitted by /u/... [link] [comments]"
    footer_idx = raw_text.lower().rfind("submitted by")
    if footer_idx != -1 and ("[link]" in raw_text[footer_idx:].lower() or "[comments]" in raw_text[footer_idx:].lower()):
        raw_text = raw_text[:footer_idx]
    return clean_post_text(raw_text), _deduplicate_reddit_image_urls(parser.image_urls), parser.video_urls


def _extract_urls_from_plain_text(text: str) -> List[str]:
    """从评论纯文本中提取直接附带的 Reddit / Imgur 图片链接。"""
    found: List[str] = []
    for tok in str(text or "").split():
        clean_tok = tok.strip("()[]<>\"',.;")
        low = clean_tok.lower()
        if low.startswith(("http://", "https://")) and any(d in low for d in ("i.redd.it/", "preview.redd.it/", "i.imgur.com/")):
            if clean_tok not in found:
                found.append(clean_tok)
    return _deduplicate_reddit_image_urls(found)


class RedditHarvester(BasePlatformHarvester):
    """
    Reddit 平台采集与归一化适配器（100% 免登录、0 账号成本）。
    主帖流走单次多板块 Atom RSS 聚合请求，并结合免登公开镜像接口补齐真实赞评数、多图 Gallery 全量原图、视频标记与高赞神评树。
    """

    SOURCE_NAME = PLATFORM_REDDIT
    ARCTIC_SHIFT_BASE = "https://arctic-shift.photon-reddit.com/api"

    def __init__(self, proxies: Optional[Dict[str, str]] = None):
        super().__init__(proxies=proxies)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": FIREFOX_UA})
        self._last_rss_rl_remaining: float = 1.0

    def _parse_atom_entries(self, xml_text: str) -> List[Dict[str, Any]]:
        """内部核心：解析 Reddit Atom XML 中的 `<entry>` 列表并转换为统一标准 Schema"""
        root = ET.fromstring(xml_text)
        ns = {"atom": "http://www.w3.org/2005/Atom", "media": "http://search.yahoo.com/mrss/"}
        entries = root.findall("atom:entry", ns)
        results = []

        for idx, ent in enumerate(entries):
            raw_id = (ent.find("atom:id", ns).text or "") if ent.find("atom:id", ns) is not None else ""
            if "t1_" in raw_id:
                continue
            post_id = raw_id.replace("t3_", "").strip()
            if not post_id:
                continue

            title = html.unescape(ent.find("atom:title", ns).text or "") if ent.find("atom:title", ns) is not None else ""
            link_el = ent.find("atom:link", ns)
            post_url = link_el.attrib.get("href", "") if link_el is not None else ""
            cat_el = ent.find("atom:category", ns)
            sub_name = _strip_reddit_prefix(cat_el.attrib.get("term", "reddit")) if cat_el is not None else "reddit"
            author_el = ent.find("atom:author/atom:name", ns)
            author_name = _strip_reddit_prefix(author_el.text or "") if author_el is not None else "redditor"
            updated_str = ent.find("atom:updated", ns).text if ent.find("atom:updated", ns) is not None else ""

            content_el = ent.find("atom:content", ns)
            raw_html = html.unescape(content_el.text or "") if content_el is not None else ""
            body_txt, imgs, vids = _parse_reddit_html(raw_html)

            video_url = vids[0] if vids else (post_url if "v.redd.it" in post_url.lower() else None)
            full_text = f"【Reddit r/{sub_name}】{title}\n{body_txt}".strip() if body_txt else f"【Reddit r/{sub_name}】{title}"

            card_type = "BUZZ_VIDEO" if video_url else ("BUZZ_LONG" if len(body_txt) > 500 else "BUZZ_SHORT")
            item = self.create_empty_post(card_type=card_type)
            item["post_id"] = f"reddit_{post_id}"
            item["publish_time"] = normalize_publish_timestamp(updated_str)
            item["author_id"] = author_name
            item["author"].update({
                "username": author_name,
                "author_name": author_name,
                "is_verified": False,
            })
            item["content"].update({
                "title": title,
                "text_content": full_text,
                "hashtags": [sub_name] if sub_name else [],
                "mentioned_coins": [],
            })
            item["media"].update({
                "cover_image": imgs[0] if imgs else None,
                "images": imgs,
                "video_url": video_url,
                "video_duration": 1 if video_url else 0,
            })
            item["engagement"].update({
                "view_count": 0,
                "like_count": 0,
                "comment_count": 0,
            })
            item["metadata"].update({
                "url": post_url,
                "subreddit": f"r/{sub_name}",
                "rank_in_feed": idx + 1,
            })
            results.append(self.normalize_post(item, download_media=False))

        return results

    def _enrich_posts_via_arctic_shift(self, posts: List[Dict[str, Any]]) -> None:
        """
        单次批量请求公开镜像 `posts/ids`，无损补齐：
        1) 真实点赞数 (`score`) 与评论数 (`num_comments`)；
        2) 多图相册 (`/gallery/...`) 在 RSS 中被折叠的全部高清原图；
        3) 精确视频帖标记 (`is_video` / `reddit_video` / `v.redd.it`)。
        若镜像偶发超时，静默跳过，完全不影响主流程。
        """
        if not posts:
            return
        id_map: Dict[str, Dict[str, Any]] = {}
        for p in posts:
            b36 = _extract_reddit_base36_id(p.get("post_id", ""))
            if b36:
                id_map[b36] = p
        if not id_map:
            return

        mime_to_ext = {
            "image/png": "png",
            "image/jpg": "jpg",
            "image/jpeg": "jpg",
            "image/webp": "webp",
            "image/gif": "gif",
        }

        try:
            resp = self.session.get(
                f"{self.ARCTIC_SHIFT_BASE}/posts/ids",
                params={"ids": ",".join(id_map.keys())},
                proxies=self.proxies,
                timeout=12,
            )
            if resp.status_code != 200:
                return
            for meta in (resp.json().get("data") or []):
                if not isinstance(meta, dict):
                    continue
                pid = str(meta.get("id") or "").strip()
                item = id_map.get(pid)
                if not item:
                    continue

                eng = item.setdefault("engagement", {})
                eng["like_count"] = max(safe_int(eng.get("like_count"), 0), safe_int(meta.get("score"), 0))
                eng["comment_count"] = max(safe_int(eng.get("comment_count"), 0), safe_int(meta.get("num_comments"), 0))

                # 精确识别 Reddit 原生视频帖
                rv = ((meta.get("media") or {}).get("reddit_video")) or ((meta.get("secure_media") or {}).get("reddit_video")) or {}
                ext_url = str(meta.get("url") or "")
                is_vid = bool(
                    meta.get("is_video")
                    or rv
                    or str(meta.get("domain") or "").lower() == "v.redd.it"
                    or "v.redd.it" in ext_url.lower()
                )
                if is_vid:
                    v_url = rv.get("fallback_url") or (ext_url if "v.redd.it" in ext_url.lower() else f"https://v.redd.it/{pid}")
                    item["card_type"] = "BUZZ_VIDEO"
                    item["media"]["video_url"] = v_url
                    item["media"]["video_duration"] = max(1, safe_int(rv.get("duration"), 1))

                # 补齐 Reddit 多图相册 (Gallery) 的全部高清原图
                mm = meta.get("media_metadata") or {}
                if isinstance(mm, dict) and mm:
                    ordered_mids: List[str] = []
                    g_items = ((meta.get("gallery_data") or {}).get("items")) or []
                    for gi in g_items:
                        if isinstance(gi, dict) and not gi.get("is_deleted") and gi.get("media_id"):
                            ordered_mids.append(str(gi["media_id"]))
                    for k in mm.keys():
                        if k not in ordered_mids:
                            ordered_mids.append(str(k))

                    gallery_urls: List[str] = []
                    for mid in ordered_mids:
                        m_obj = mm.get(mid) or {}
                        if m_obj.get("e") == "RedditVideo":
                            item["card_type"] = "BUZZ_VIDEO"
                            item["media"]["video_url"] = item["media"].get("video_url") or f"https://v.redd.it/{mid}"
                            continue
                        ext = mime_to_ext.get(str(m_obj.get("m") or "").lower(), "")
                        if ext:
                            gallery_urls.append(f"https://i.redd.it/{mid}.{ext}")
                        else:
                            s_url = html.unescape(str(((m_obj.get("s") or {}).get("u")) or ""))
                            if s_url:
                                gallery_urls.append(s_url)

                    if gallery_urls:
                        merged_imgs = _deduplicate_reddit_image_urls(gallery_urls + list(item["media"].get("images") or []))
                        item["media"]["images"] = merged_imgs
                        item["media"]["cover_image"] = merged_imgs[0] if merged_imgs else None
        except Exception as e:
            logger.debug(f"[Reddit适配器] 批量元数据富化跳过: {e}")

    # ==========================================================================
    # 四大通用原语实现
    # ==========================================================================
    def fetch_hot_or_recommended(
        self,
        count: int = 25,
        subreddits: Optional[List[str]] = None,
        sort: str = "top",
        time_filter: str = "day",
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        原语 1：使用 `+` 多板块聚合语法，单次 HTTP 请求拉取多个板块的 24h 爆款帖。
        `sort` 可选: "top" | "hot" | "rising" | "new"
        `time_filter` 可选: "hour" | "day" | "week" | "month"
        """
        subs = subreddits or DEFAULT_GOLDMINE_SUBREDDITS
        combined = "+".join(_strip_reddit_prefix(s) for s in subs if _strip_reddit_prefix(s))
        url = f"https://www.reddit.com/r/{combined}/{sort}/.rss?t={time_filter}&limit={max(count, 25)}"
        try:
            resp = self.session.get(url, proxies=self.proxies, timeout=REQUEST_TIMEOUT)
            try:
                self._last_rss_rl_remaining = float(resp.headers.get("x-ratelimit-remaining", "1.0"))
            except ValueError:
                self._last_rss_rl_remaining = 0.0
            if resp.status_code != 200:
                logger.warning(f"[Reddit适配器] 多板块 RSS 返回 HTTP {resp.status_code}")
                return []
            posts = self._parse_atom_entries(resp.text)[:count]
            self._enrich_posts_via_arctic_shift(posts)
            return posts
        except Exception as e:
            logger.warning(f"[Reddit适配器] 拉取多板块热榜异常: {e}")
            return []

    def fetch_user_posts(self, user_id: str, count: int = 15, **kwargs) -> List[Dict[str, Any]]:
        """原语 2：免登拉取指定 Reddit 用户 (`/u/username`) 发布的帖子"""
        clean_user = _strip_reddit_prefix(user_id)
        if not clean_user:
            return []
        url = f"https://www.reddit.com/user/{clean_user}/submitted/.rss?limit={count}"
        try:
            resp = self.session.get(url, proxies=self.proxies, timeout=REQUEST_TIMEOUT)
            if resp.status_code != 200:
                return []
            posts = self._parse_atom_entries(resp.text)[:count]
            self._enrich_posts_via_arctic_shift(posts)
            return posts
        except Exception as e:
            logger.warning(f"[Reddit适配器] 拉取用户 u/{clean_user} 异常: {e}")
            return []

    def search_or_topic_posts(
        self,
        query: str,
        count: int = 20,
        subreddits: Optional[List[str]] = None,
        time_filter: str = "week",
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        原语 3：按关键词或垂直板块搜索高价值帖子。
        注：为避开 `/search.rss` 的 403/429 封锁，直接单次拉取目标板块 `top` 大池并在本地做多关键词匹配（支持 `|` 分隔）。
        """
        raw_q = str(query or "").replace(",", "|")
        keywords = [w.strip().lower() for w in raw_q.split("|") if w.strip()]
        pool = self.fetch_hot_or_recommended(
            count=50,
            subreddits=subreddits,
            sort="top",
            time_filter=time_filter,
        )
        if not keywords:
            return pool[:count]

        matched = []
        for p in pool:
            txt = ((p.get("content") or {}).get("text_content") or "").lower()
            if any(k in txt for k in keywords):
                matched.append(p)
        return matched[:count]

    def fetch_post_comments(self, post_id: str, max_comments: int = 10, **kwargs) -> List[Dict[str, Any]]:
        """
        原语 4：免登提取指定帖子的前排高赞神评（自动过滤机器人与删除楼层，带真实点赞数且零 429 限流）。
        优先走公开镜像 `comments/tree`，若未命中且当前 RSS 令牌桶尚有余量则回退 `.rss`。
        """
        raw_target = str(post_id or "").strip()
        clean_id = _extract_reddit_base36_id(raw_target)
        if not clean_id:
            return []

        parent_pid = f"reddit_{clean_id}"

        # 通道 A：零限流公开镜像评论树（原生携带真实点赞 score）
        try:
            resp = self.session.get(
                f"{self.ARCTIC_SHIFT_BASE}/comments/tree",
                params={"link_id": clean_id, "limit": max(25, max_comments * 3)},
                proxies=self.proxies,
                timeout=10,
            )
            if resp.status_code == 200:
                nodes = resp.json().get("data") or []
                parsed_comments: List[Dict[str, Any]] = []
                for node in nodes:
                    if not isinstance(node, dict):
                        continue
                    d = node.get("data") if isinstance(node.get("data"), dict) else node
                    author = _strip_reddit_prefix(str(d.get("author") or ""))
                    if not author or author.lower() in ("automoderator", "visualmod", "[deleted]", "none"):
                        continue
                    body = clean_post_text(html.unescape(str(d.get("body") or "")))
                    if not body or body.lower() in ("[deleted]", "[removed]"):
                        continue
                    c_imgs = _extract_urls_from_plain_text(body)
                    parsed_comments.append({
                        "reply_id": str(d.get("id") or ""),
                        "parent_id": parent_pid,
                        "create_time": safe_int(d.get("created_utc"), int(time.time())),
                        "author_uid": author,
                        "author_name": author,
                        "content": body,
                        "likes": safe_int(d.get("score"), 0),
                        "images": c_imgs,
                    })
                if parsed_comments:
                    parsed_comments.sort(key=lambda x: x["likes"], reverse=True)
                    return self.normalize_comments(parsed_comments[:max_comments], parent_post_id=parent_pid)
        except Exception as e:
            logger.debug(f"[Reddit适配器] 镜像评论树 {clean_id} 异常: {e}")

        # 通道 B：仅在独立单帖调试且 RSS 令牌桶未耗尽时回退 `.rss`
        if self._last_rss_rl_remaining <= 0.0:
            return []
        rss_url = (
            raw_target.rstrip("/") + f"/.rss?limit={max_comments + 4}"
            if raw_target.startswith("http")
            else f"https://www.reddit.com/comments/{clean_id}/.rss?limit={max_comments + 4}"
        )
        try:
            resp = self.session.get(rss_url, proxies=self.proxies, timeout=12)
            try:
                self._last_rss_rl_remaining = float(resp.headers.get("x-ratelimit-remaining", "0.0"))
            except ValueError:
                self._last_rss_rl_remaining = 0.0
            if resp.status_code != 200:
                return []
            root = ET.fromstring(resp.text)
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            entries = root.findall("atom:entry", ns)
            raw_comments = []
            for ent in entries[1:]:
                c_id = (ent.find("atom:id", ns).text or "").replace("t1_", "") if ent.find("atom:id", ns) is not None else ""
                author_el = ent.find("atom:author/atom:name", ns)
                author = _strip_reddit_prefix(author_el.text or "") if author_el is not None else "redditor"
                if author.lower() in ("automoderator", "visualmod"):
                    continue
                updated_str = ent.find("atom:updated", ns).text if ent.find("atom:updated", ns) is not None else ""
                content_el = ent.find("atom:content", ns)
                raw_html = html.unescape(content_el.text or "") if content_el is not None else ""
                txt, c_imgs, _ = _parse_reddit_html(raw_html)
                if not txt and not c_imgs:
                    continue
                raw_comments.append({
                    "reply_id": c_id,
                    "parent_id": parent_pid,
                    "create_time": normalize_publish_timestamp(updated_str),
                    "author_uid": author,
                    "author_name": author,
                    "content": txt,
                    "likes": 0,
                    "images": c_imgs,
                })
                if len(raw_comments) >= max_comments:
                    break
            return self.normalize_comments(raw_comments, parent_post_id=parent_pid)
        except Exception as e:
            logger.debug(f"[Reddit适配器] 拉取评论 {rss_url} 异常: {e}")
            return []

    # ==========================================================================
    # 标准调度入口：供上层定时任务调用
    # ==========================================================================
    def harvest(self, config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        cfg = config or {}
        subreddits = cfg.get("subreddits") or DEFAULT_GOLDMINE_SUBREDDITS
        sort = cfg.get("sort", "top")
        time_filter = cfg.get("time_filter", "day")
        limit = safe_int(cfg.get("limit"), 20)
        top_n_comments = safe_int(cfg.get("fetch_comments_for_top_n"), 15)
        keywords = cfg.get("keywords")
        download_media = bool(cfg.get("download_media", True))

        self.last_channel_stats: Dict[str, int] = {}
        if keywords:
            query_str = "|".join(keywords) if isinstance(keywords, list) else str(keywords)
            posts = self.search_or_topic_posts(query=query_str, count=limit, subreddits=subreddits, time_filter=time_filter)
        else:
            posts = self.fetch_hot_or_recommended(count=limit, subreddits=subreddits, sort=sort, time_filter=time_filter)

        valuable_posts = [p for p in posts if self.is_valuable_post(p, cfg)]
        for p in valuable_posts:
            sub = str(((p.get("metadata") or {}).get("platform_extra") or {}).get("subreddit") or "r/reddit")
            self.last_channel_stats[sub] = self.last_channel_stats.get(sub, 0) + 1

        if top_n_comments > 0 and valuable_posts:
            for item in valuable_posts[:top_n_comments]:
                comments = self.fetch_post_comments(item["post_id"], max_comments=8)
                if comments:
                    item["comments"] = comments
                    item["engagement"]["comment_count"] = max(item["engagement"].get("comment_count", 0), len(comments))

        return self.normalize_batch(valuable_posts, download_media=download_media)
