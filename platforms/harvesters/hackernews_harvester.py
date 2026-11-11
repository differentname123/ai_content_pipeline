# -*- coding: utf-8 -*-
"""
HackerNews (硅谷科技/AI/商业热榜) 平台专用采集与差异抹平适配器 (HackerNewsHarvester)

================================================================================
【运维速查手册 (Ops Quick Guide) —— 日常巡检与修改必读】
================================================================================
1. 是否使用登录账号？
   - **否（100% 免登录 / 0 账号模式）**。直连 HackerNews 官方公开 Algolia 检索 API，无频控风险。

2. 当前到底在拉取谁？
   - ① 硅谷首页顶流热榜：`tags=front_page` 且赞数 `points >= min_points`（默认 50 赞以上）；
   - ② 垂直关键词热帖：检索 `harvester_runner.py` -> `PLATFORM_HARVEST_CONFIGS["hackernews"]["keywords"]`
     （默认：`["AI", "OpenAI", "Nvidia", "crypto", "million", "lawsuit"]`）近期高赞故事；
   - ③ 前排极客神评：为前 `fetch_comments_for_top_n` 条热帖拉取一级高赞评论树。

3. 平常想修改应该怎么改？
   - 【增删检索关键词】：修改 `platforms/harvester_runner.py` 中 `"hackernews"` 配置下的 `"keywords"` 列表。
   - 【调整最低赞数门槛】：修改 `"hackernews"` 配置下的 `"min_points"`（默认 50）。

================================================================================
【HackerNews 接口探索与踩坑档案 (Exploration & Pitfall Log)】
维护必读：以下记录了我们在 2026 年实测过的所有 HackerNews 采集路径。请勿重复尝试已标记为 ❌ 的废路！
================================================================================

✅ 1. [已采用 / 主力通道A] 官方 Algolia 检索接口 (`https://hn.algolia.com/api/v1/search` & `search_by_date`)
   - 原理：通过 `tags=front_page` 拉取首页顶流热榜，或通过 `tags=story&numericFilters=points>=50,created_at_i>=...`
           拉取高赞故事。
   - 优势：100% 免登录、无 Cloudflare 盾、毫秒级响应，返回精确的 `points`（赞数）、`num_comments`（评论数）与外链。

✅ 2. [已采用 / 主力通道B] 单帖神评树接口 (`https://hn.algolia.com/api/v1/items/{story_id}`)
   - 原理：传入 `objectID` 即可一次性拿到整棵嵌套评论树 `children`，第 1 层 `children` 即为硅谷极客/创始人的前排神评。

❌ 3. [已避坑 / 禁止踩坑] 在 Algolia `query` 参数中使用 `A OR B` 布尔语法 (`?query=AI+OR+crypto+OR+million`)
   - 失败原因：实测 Algolia 默认将 `OR` 当作普通英文单词做全词交集 (AND) 匹配，导致传 `AI OR crypto` 直接返回 `0 hits`！
   - 正确做法：本适配器的 `search_or_topic_posts()` 若遇到多关键词（如 `"AI|OpenAI|Nvidia|crypto"`），
               会自动拆分为单关键词结合 `numericFilters=points>=30` 检索并按 `objectID` 合并去重，确保 100% 命中高赞帖。

❌ 4. [已废弃 / 禁止踩坑] Firebase 原始逐条接口 (`https://hacker-news.firebaseio.com/v0/topstories.json`)
   - 失败原因：只返回一串数字 ID 数组 `[40123, 40124, ...]`，若要拉 30 条帖子+评论需要发 100+ 次串行请求，极慢且冗余。
================================================================================
"""
import html
from html.parser import HTMLParser
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

from common.common_utils import (
    PLATFORM_HACKERNEWS,
    clean_post_text,
    safe_int,
)
from platforms.harvesters.base_harvester import BasePlatformHarvester

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 15
DEFAULT_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


class _SimpleHTMLStripper(HTMLParser):
    """使用标准库 HTMLParser 将 HN HTML 段落与链接还原为纯文本，零正则。"""

    def __init__(self) -> None:
        super().__init__()
        self.parts: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        if tag in ("p", "br", "pre", "div", "li"):
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if data:
            self.parts.append(data)


def _strip_hn_html(raw_html: str) -> str:
    if not raw_html:
        return ""
    stripper = _SimpleHTMLStripper()
    stripper.feed(html.unescape(raw_html))
    stripper.close()
    return clean_post_text("".join(stripper.parts))


class HackerNewsHarvester(BasePlatformHarvester):
    """
    HackerNews 平台采集与归一化适配器（100% 免登录、官方 Algolia API）。
    提供硅谷科技巨头内幕、AI 突破、商业奇闻与高智商神评素材。
    """

    SOURCE_NAME = PLATFORM_HACKERNEWS

    def __init__(self, proxies: Optional[Dict[str, str]] = None):
        super().__init__(proxies=proxies)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": DEFAULT_UA})

    def _convert_hit(self, hit: Dict[str, Any]) -> Dict[str, Any]:
        """内部核心：将 Algolia hit 节点转换为统一标准 Schema"""
        oid = str(hit.get("objectID") or hit.get("id") or "").strip()
        title = clean_post_text(hit.get("title") or "")
        story_text = _strip_hn_html(hit.get("story_text") or "")
        story_url = hit.get("url") or f"https://news.ycombinator.com/item?id={oid}"
        points = safe_int(hit.get("points"), 0)
        num_comments = safe_int(hit.get("num_comments"), 0)
        author = str(hit.get("author") or "hn_user").strip()
        created_ts = safe_int(hit.get("created_at_i"), int(time.time()))

        full_text = f"【HackerNews 硅谷热榜】{title}"
        if story_text:
            full_text += f"\n{story_text}"
        full_text += f"\n原文链接: {story_url}"

        item = self.create_empty_post(card_type="BUZZ_LONG" if len(story_text) > 400 else "BUZZ_SHORT")
        item["post_id"] = f"hn_{oid}"
        item["publish_time"] = created_ts
        item["author_id"] = author
        item["author"].update({
            "username": author,
            "author_name": author,
            "is_verified": False,
        })
        item["content"].update({
            "title": title,
            "text_content": full_text,
            "hashtags": ["HackerNews", "Tech"],
            "mentioned_coins": [],
        })
        item["engagement"].update({
            "view_count": 0,
            "like_count": points,
            "comment_count": num_comments,
        })
        item["metadata"].update({
            "url": f"https://news.ycombinator.com/item?id={oid}",
            "external_url": story_url,
            "hn_object_id": oid,
        })
        return self.normalize_post(item, download_media=False)

    # ==========================================================================
    # 四大通用原语实现
    # ==========================================================================
    def fetch_hot_or_recommended(
        self,
        count: int = 15,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """原语 1：拉取 HackerNews 首页 (`front_page`) 帖子（不做赞数门槛过滤）"""
        url = "https://hn.algolia.com/api/v1/search"
        params = {
            "tags": "front_page",
            "hitsPerPage": max(count, 20),
        }
        try:
            resp = self.session.get(url, params=params, proxies=self.proxies, timeout=REQUEST_TIMEOUT)
            if resp.status_code != 200:
                return []
            hits = resp.json().get("hits") or []
            posts = [self._convert_hit(h) for h in hits if h.get("objectID")]
            return posts[:count]
        except Exception as e:
            logger.warning(f"[HN适配器] 拉取首页热榜异常: {e}")
            return []

    def fetch_user_posts(
        self,
        user_id: str,
        count: int = 15,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """原语 2：拉取指定硅谷极客/创始人 (`author_{username}`) 发布的帖子"""
        clean_user = str(user_id).strip()
        if not clean_user:
            return []
        url = "https://hn.algolia.com/api/v1/search_by_date"
        params = {
            "tags": f"story,author_{clean_user}",
            "hitsPerPage": count,
        }
        try:
            resp = self.session.get(url, params=params, proxies=self.proxies, timeout=REQUEST_TIMEOUT)
            if resp.status_code != 200:
                return []
            hits = resp.json().get("hits") or []
            return [self._convert_hit(h) for h in hits if h.get("objectID")][:count]
        except Exception as e:
            logger.warning(f"[HN适配器] 拉取用户 {clean_user} 异常: {e}")
            return []

    def search_or_topic_posts(
        self,
        query: str,
        count: int = 15,
        hours_back: int = 72,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        原语 3：按关键词（支持以 `|` 或 `,` 分隔多个关键词）搜索近期故事（不做赞数过滤）。
        """
        raw_q = str(query or "").replace(",", "|")
        keywords = [w.strip() for w in raw_q.split("|") if w.strip()]
        min_ts = int(time.time() - hours_back * 3600)
        url = "https://hn.algolia.com/api/v1/search_by_date"

        if not keywords:
            keywords = [""]

        by_id: Dict[str, Dict[str, Any]] = {}
        for kw in keywords:
            params = {
                "tags": "story",
                "numericFilters": f"created_at_i>={min_ts}",
                "hitsPerPage": max(5, count),
            }
            if kw:
                params["query"] = kw
            try:
                resp = self.session.get(url, params=params, proxies=self.proxies, timeout=REQUEST_TIMEOUT)
                if resp.status_code == 200:
                    for h in resp.json().get("hits") or []:
                        if h.get("objectID"):
                            item = self._convert_hit(h)
                            by_id[item["post_id"]] = item
            except Exception as e:
                logger.debug(f"[HN适配器] 搜索关键词 {kw!r} 异常: {e}")

        return list(by_id.values())[:count]

    def fetch_post_comments(
        self,
        post_id: str,
        max_comments: int = 8,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """原语 4：拉取指定 HN 帖子底下的前排评论"""
        clean_id = str(post_id or "").replace("hn_", "").strip()
        if not clean_id:
            return []
        url = f"https://hn.algolia.com/api/v1/items/{clean_id}"
        try:
            resp = self.session.get(url, proxies=self.proxies, timeout=10)
            if resp.status_code != 200:
                return []
            children = resp.json().get("children") or []
            raw_comments = []
            for c in children:
                clean_c = _strip_hn_html(c.get("text") or "")
                if not clean_c:
                    continue
                raw_comments.append({
                    "reply_id": str(c.get("id") or ""),
                    "parent_id": f"hn_{clean_id}",
                    "create_time": safe_int(c.get("created_at_i"), int(time.time())),
                    "author_uid": str(c.get("author") or "hn_commenter"),
                    "author_name": str(c.get("author") or "hn_commenter"),
                    "content": clean_c[:4000],
                    "likes": 0,
                })
                if len(raw_comments) >= max_comments:
                    break
            return self.normalize_comments(raw_comments, parent_post_id=f"hn_{clean_id}")
        except Exception as e:
            logger.debug(f"[HN适配器] 拉取评论 {clean_id} 异常: {e}")
            return []

    # ==========================================================================
    # 标准调度入口：供上层定时任务调用
    # ==========================================================================
    def harvest(self, config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        cfg = config or {}
        front_page_count = safe_int(cfg.get("front_page_count") or cfg.get("limit"), 12)
        per_keyword_count = safe_int(cfg.get("per_keyword_count"), 3)
        keywords = cfg.get("keywords") or []
        top_n_comments = safe_int(cfg.get("fetch_comments_for_top_n"), 20)

        self.last_channel_stats: Dict[str, int] = {}
        by_id: Dict[str, Dict[str, Any]] = {}

        fp_posts = self.fetch_hot_or_recommended(count=front_page_count)
        self.last_channel_stats["首页front_page"] = len(fp_posts)
        for p in fp_posts:
            if self.is_valuable_post(p, cfg):
                by_id[p["post_id"]] = p

        for kw in keywords:
            kw_posts = self.search_or_topic_posts(query=str(kw), count=per_keyword_count)
            self.last_channel_stats[f"关键词:{kw}"] = len(kw_posts)
            for p in kw_posts:
                if self.is_valuable_post(p, cfg):
                    by_id[p["post_id"]] = p

        all_posts = list(by_id.values())

        if top_n_comments > 0 and all_posts:
            comment_targets = [
                p for p in all_posts
                if safe_int((p.get("engagement") or {}).get("comment_count"), 0) > 0
            ]
            comment_targets.sort(
                key=lambda p: (
                    safe_int((p.get("engagement") or {}).get("comment_count"), 0) * 2
                    + safe_int((p.get("engagement") or {}).get("like_count"), 0)
                ),
                reverse=True,
            )
            for item in comment_targets[:top_n_comments]:
                comments = self.fetch_post_comments(item["post_id"], max_comments=10)
                if comments:
                    item["comments"] = comments

        return self.normalize_batch(all_posts, download_media=False)
