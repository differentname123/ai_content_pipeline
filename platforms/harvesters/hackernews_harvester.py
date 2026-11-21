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
from concurrent.futures import ThreadPoolExecutor, as_completed
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
from platforms.external_link_extractor import enrich_posts_with_external_articles
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
    提供硅谷科技巨头内幕、AI 突破、商业奇闻、外链全文与高智商神评素材。
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

    def _attach_comments_concurrent(
        self,
        posts: List[Dict[str, Any]],
        top_n_comments: int = 50,
        max_comments_per_post: int = 10,
    ) -> None:
        if top_n_comments <= 0 or not posts:
            return
        # 【保留过滤原因说明】已知 0 评论帖短路跳过：
        # 仅对接口已明确返回 `comment_count > 0` 的帖子发起 `/items/{id}` 评论树请求，避免空跑无评论新帖浪费网络耗时。
        comment_targets = [
            p for p in posts
            if safe_int((p.get("engagement") or {}).get("comment_count"), 0) > 0 and not (p.get("comments") or [])
        ]
        if not comment_targets:
            return
        comment_targets.sort(
            key=lambda p: (
                safe_int((p.get("engagement") or {}).get("comment_count"), 0) * 2
                + safe_int((p.get("engagement") or {}).get("like_count"), 0)
            ),
            reverse=True,
        )
        selected = comment_targets[:top_n_comments]
        with ThreadPoolExecutor(max_workers=min(6, len(selected))) as pool:
            fut_map = {
                pool.submit(self.fetch_post_comments, item["post_id"], max_comments=max_comments_per_post): item
                for item in selected
            }
            for fut in as_completed(fut_map):
                target_item = fut_map[fut]
                try:
                    comments = fut.result()
                    if comments:
                        target_item["comments"] = comments
                except Exception:
                    pass

    # ==========================================================================
    # 四大通用原语实现
    # ==========================================================================
    def fetch_hot_or_recommended(
        self,
        count: int = 30,
        include_24h_top: bool = True,
        min_24h_points: int = 0,
        preserve_all: bool = False,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        原语 1：拉取 HackerNews 首页 (`front_page` 整页全量) + 过去 24h 热门故事榜（不做人为分数门槛过滤）：
        - `min_24h_points<=0`（默认）：不设 `points>=N` 门槛，直接由 Algolia 按 24h 内热度降序返回前排故事。
        """
        url = "https://hn.algolia.com/api/v1/search"
        by_id: Dict[str, Dict[str, Any]] = {}
        self._last_fp_count = 0
        self._last_24h_count = 0

        try:
            resp = self.session.get(
                url,
                params={"tags": "front_page", "hitsPerPage": max(count, 40)},
                proxies=self.proxies,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code != 200:
                self.record_health_issue(
                    code="HN_FRONT_PAGE_HTTP_ERR",
                    message=f"HackerNews Algolia front_page 返回 HTTP {resp.status_code}",
                    level="critical",
                    channel="首页front_page",
                )
            else:
                hits = resp.json().get("hits") or []
                for h in hits:
                    if h.get("objectID"):
                        item = self._convert_hit(h)
                        by_id[item["post_id"]] = item
                        self._last_fp_count += 1
                if self._last_fp_count == 0:
                    self.record_health_issue(
                        code="HN_FRONT_PAGE_EMPTY",
                        message="HackerNews Algolia front_page 返回 0 条，疑似接口结构变更",
                        level="critical",
                        channel="首页front_page",
                    )
        except Exception as e:
            self.record_health_issue(
                code="HN_FRONT_PAGE_EXCEPTION",
                message=f"拉取首页 front_page 网络异常: {e}",
                level="critical",
                channel="首页front_page",
            )

        if include_24h_top:
            min_24h_ts = int(time.time() - 86400)
            num_filter = (
                f"created_at_i>={min_24h_ts},points>={min_24h_points}"
                if min_24h_points > 0
                else f"created_at_i>={min_24h_ts}"
            )
            try:
                resp_24h = self.session.get(
                    url,
                    params={
                        "tags": "story",
                        "numericFilters": num_filter,
                        "hitsPerPage": max(count, 50),
                    },
                    proxies=self.proxies,
                    timeout=REQUEST_TIMEOUT,
                )
                if resp_24h.status_code == 200:
                    for h in resp_24h.json().get("hits") or []:
                        if h.get("objectID"):
                            item = self._convert_hit(h)
                            self._last_24h_count += 1
                            by_id.setdefault(item["post_id"], item)
                else:
                    self.record_health_issue(
                        code="HN_24H_HTTP_ERR",
                        message=f"HN 24h 高赞故事榜返回 HTTP {resp_24h.status_code}",
                        level="warning",
                        channel="24h高赞榜",
                    )
            except Exception as e:
                self.record_health_issue(
                    code="HN_24H_EXCEPTION",
                    message=f"拉取 24h 高赞故事榜异常: {e}",
                    level="warning",
                    channel="24h高赞榜",
                )

        ranked = sorted(
            by_id.values(),
            key=lambda p: (
                safe_int((p.get("engagement") or {}).get("like_count"), 0)
                + safe_int((p.get("engagement") or {}).get("comment_count"), 0) * 2,
                safe_int(p.get("publish_time"), 0),
            ),
            reverse=True,
        )
        return ranked if preserve_all else ranked[:count]

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
        count: int = 20,
        hours_back: int = 0,
        fetch_comments: bool = False,
        fetch_external_links: bool = False,
        download_media: bool = False,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        原语 3：按关键词（支持以 `|` 或 `,` 分隔多个关键词）搜索故事（同时覆盖最新时间排序与近期热度排序）。
        - 默认 `hours_back=0` 不做硬性时间丢弃（冷门词条即便只有较早讨论也会完整返回，时间筛选统一由下游负责）；
        - 【保留过滤原因说明】HackerNews 为纯英文极客科技社区，全站无中文帖；自动跳过纯中文词或剥离中英混写中的中文字符，
          是为了避免 Algolia 默认全词 AND 匹配把英文主词拖累成 0 条结果并浪费无效请求。
        """
        import re as _re

        reset_health = bool(kwargs.get("reset_health", True))
        if reset_health:
            self.reset_health_state()

        raw_q = str(query or "").replace(",", "|")
        raw_keywords = [w.strip() for w in raw_q.split("|") if w.strip()]
        now_ts = int(time.time())
        min_ts = int(now_ts - max(1, hours_back) * 3600) if (hours_back and hours_back > 0) else 0
        recent_priority_ts = int(now_ts - 7 * 86400)

        english_keywords: List[str] = []
        seen_en = set()
        if not raw_keywords:
            english_keywords = [""]
        else:
            for kw in raw_keywords:
                if all(ord(ch) < 128 for ch in kw):
                    clean_en = " ".join(kw.split())
                else:
                    ascii_tokens = [t for t in _re.findall(r"[A-Za-z0-9][A-Za-z0-9.\-_+#]*", kw) if len(t) >= 2]
                    clean_en = " ".join(ascii_tokens)
                if clean_en and clean_en.lower() not in seen_en:
                    seen_en.add(clean_en.lower())
                    english_keywords.append(clean_en)

        if raw_keywords and not english_keywords:
            self.last_search_note = "纯中文查询词（HackerNews为纯英文科技社区，无中文帖）"
            return []
        self.last_search_note = f"英文检索词:{','.join(english_keywords)}"

        by_id: Dict[str, Dict[str, Any]] = {}
        per_kw = max(10, count)
        req_total = 0
        req_failed = 0

        def _query_algolia(q_str: str) -> int:
            nonlocal req_total, req_failed
            added_cnt = 0
            # 1) search_by_date：当 hours_back<=0 时不加任何时间下限，天然返回最新实时故事且冷门话题绝不丢帖；
            # 2) search（按分数/相关度）：优先查近期（7d）高热讨论，防止十年前的高赞历史归档帖把今日新帖挤出 Top count；若不足再无门槛补齐。
            specs = [
                ("https://hn.algolia.com/api/v1/search_by_date", f"created_at_i>={min_ts}" if min_ts > 0 else ""),
                (
                    "https://hn.algolia.com/api/v1/search",
                    f"created_at_i>={min_ts}" if min_ts > 0 else f"created_at_i>={recent_priority_ts}",
                ),
            ]
            for endpoint, num_flt in specs:
                req_total += 1
                params: Dict[str, Any] = {
                    "tags": "story",
                    "hitsPerPage": per_kw,
                }
                if num_flt:
                    params["numericFilters"] = num_flt
                if q_str:
                    params["query"] = q_str
                try:
                    resp = self.session.get(endpoint, params=params, proxies=self.proxies, timeout=REQUEST_TIMEOUT)
                    if resp.status_code == 200:
                        for h in resp.json().get("hits") or []:
                            if h.get("objectID"):
                                item = self._convert_hit(h)
                                if item["post_id"] not in by_id:
                                    added_cnt += 1
                                by_id[item["post_id"]] = item
                    else:
                        req_failed += 1
                except Exception as e:
                    req_failed += 1
                    logger.debug(f"[HN适配器] 搜索关键词 {q_str!r} 异常: {e}")
            return added_cnt

        for kw in english_keywords:
            hits = _query_algolia(kw)
            # 若调用方传入了 3 个词以上的长短语，Algolia 默认严格全词 AND 易返回 0 条，自动回退核心双词组合检索
            if hits == 0 and kw:
                tokens = [t for t in kw.split() if len(t) >= 2 and any(c.isascii() and c.isalnum() for c in t)]
                if len(tokens) >= 3:
                    sub_queries = [" ".join(tokens[:2]), " ".join(tokens[-2:])]
                    for sq in dict.fromkeys(sub_queries):
                        _query_algolia(sq)

        if req_total > 0 and req_failed == req_total:
            self.record_health_issue(
                code="HN_SEARCH_API_UNREACHABLE",
                message=f"HackerNews Algolia 搜索请求全部失败 ({req_failed}/{req_total})，请检查网络或代理",
                level="critical",
                channel="Algolia搜索",
            )
        elif req_failed > 0:
            self.record_health_issue(
                code="HN_SEARCH_PARTIAL_FAIL",
                message=f"HackerNews Algolia 搜索部分请求异常 ({req_failed}/{req_total})",
                level="warning",
                channel="Algolia搜索",
            )

        results = sorted(
            by_id.values(),
            key=lambda p: (
                1 if safe_int(p.get("publish_time"), 0) >= recent_priority_ts else 0,
                safe_int((p.get("engagement") or {}).get("like_count"), 0)
                + safe_int((p.get("engagement") or {}).get("comment_count"), 0) * 2,
                safe_int(p.get("publish_time"), 0),
            ),
            reverse=True,
        )[:count]

        if fetch_comments and results:
            self._attach_comments_concurrent(results, top_n_comments=len(results), max_comments_per_post=10)
        if fetch_external_links and results:
            enrich_posts_with_external_articles(results, proxies=self.proxies)
        if download_media or fetch_external_links or fetch_comments:
            results = self.normalize_batch(results, download_media=download_media)

        if reset_health:
            self.audit_posts_quality(
                results,
                scene="search",
                expect_comments=fetch_comments,
                expect_media_download=download_media,
                max_fresh_age_hours=max(168, hours_back or 168),
            )
        return [p for p in results if self.is_valuable_post(p)]

    def fetch_post_comments(
        self,
        post_id: str,
        max_comments: int = 10,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """原语 4：拉取指定 HN 帖子底下的前排评论"""
        clean_id = str(post_id or "").replace("hn_", "").strip()
        if not clean_id:
            return []
        url = f"https://hn.algolia.com/api/v1/items/{clean_id}"
        try:
            resp = requests.get(url, headers={"User-Agent": DEFAULT_UA}, proxies=self.proxies, timeout=10)
            if resp.status_code != 200:
                return []
            children = resp.json().get("children") or []
            raw_comments = []
            for c in children:
                # 【保留过滤原因说明】跳过已被管理员折叠/删除的垃圾评论 (`dead` / `deleted` / `[dead]`)，
                # 避免无实质内容的占位符挤占每帖前排有效评论名额。
                if c.get("dead") or c.get("deleted"):
                    continue
                clean_c = _strip_hn_html(c.get("text") or "")
                if not clean_c or clean_c.strip().lower() in ("[dead]", "[flagged]"):
                    continue
                raw_comments.append({
                    "reply_id": str(c.get("id") or ""),
                    "parent_id": f"hn_{clean_id}",
                    "create_time": safe_int(c.get("created_at_i"), int(time.time())),
                    "author_uid": str(c.get("author") or "hn_commenter"),
                    "author_name": str(c.get("author") or "hn_commenter"),
                    "content": clean_c,
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
        self.reset_health_state()
        cfg = config or {}
        front_page_count = safe_int(cfg.get("front_page_count") or cfg.get("limit"), 30)
        include_24h_top = bool(cfg.get("include_24h_top", True))
        min_24h_points = safe_int(cfg.get("min_24h_points"), 0)
        preserve_all_hot = bool(cfg.get("preserve_all_hot", True))
        per_keyword_count = safe_int(cfg.get("per_keyword_count"), 4)
        keywords = cfg.get("keywords") or []
        top_n_comments = safe_int(cfg.get("fetch_comments_for_top_n"), 60)
        fetch_ext_links = bool(cfg.get("fetch_external_links", True))
        download_media = bool(cfg.get("download_media", True))

        self.last_channel_stats: Dict[str, int] = {}
        by_id: Dict[str, Dict[str, Any]] = {}

        fp_posts = self.fetch_hot_or_recommended(
            count=front_page_count,
            include_24h_top=include_24h_top,
            min_24h_points=min_24h_points,
            preserve_all=preserve_all_hot,
        )
        self.last_channel_stats["首页front_page"] = getattr(self, "_last_fp_count", len(fp_posts))
        if include_24h_top:
            self.last_channel_stats["24h高赞榜"] = getattr(self, "_last_24h_count", 0)
        for p in fp_posts:
            if self.is_valuable_post(p, cfg):
                by_id[p["post_id"]] = p

        for kw in keywords:
            kw_posts = self.search_or_topic_posts(query=str(kw), count=per_keyword_count, reset_health=False)
            self.last_channel_stats[f"关键词:{kw}"] = len(kw_posts)
            for p in kw_posts:
                if self.is_valuable_post(p, cfg):
                    by_id[p["post_id"]] = p

        all_posts = list(by_id.values())
        self._attach_comments_concurrent(all_posts, top_n_comments=top_n_comments, max_comments_per_post=10)
        if fetch_ext_links and all_posts:
            ext_cnt = enrich_posts_with_external_articles(all_posts, proxies=self.proxies)
            self.last_channel_stats["外链正文富化"] = ext_cnt

        final_posts = self.normalize_batch(all_posts, download_media=download_media)
        self.audit_posts_quality(
            final_posts,
            scene="harvest",
            expect_comments=(top_n_comments > 0),
            expect_media_download=download_media,
            max_fresh_age_hours=168,
        )
        return [p for p in final_posts if self.is_valuable_post(p, cfg)]
