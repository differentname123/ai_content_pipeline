# -*- coding: utf-8 -*-
"""
多平台高价值内容拉取架构 —— 抽象基类与统一契约 (BasePlatformHarvester)

设计哲学：
1. 极简可靠：不引入复杂脆弱的框架，所有平台适配器只继承一个基类 `BasePlatformHarvester`。
2. 职责内聚：每个平台适配器内部负责「拉取本平台数据 + 抹平本平台字段差异」，对外统一输出标准帖子结构。
3. 四大通用原语：任何社区/内容平台（X、Reddit、HackerNews、币安、OKX、知乎、小红书）在业务上最常用的无非是 4 个动作：
   - `fetch_hot_or_recommended`: 拉取平台首页推荐流 / 热榜爆款流
   - `fetch_user_posts`:         拉取指定大 V / 博主的最新发帖
   - `search_or_topic_posts`:    按关键词、话题标签、垂直板块或币种搜索帖子
   - `fetch_post_comments`:      拉取单条爆款帖底下的前排高赞神评（用于借梗与反转提炼）
4. 质量优先：内置通用价值门槛过滤 (`filter_high_value_posts`)，剔除无意义水帖，绝不为了拉取而拉取垃圾数据。
"""
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from common.common_utils import (
    clean_post_text,
    create_standard_post_schema,
    get_requests_proxies,
    normalize_and_enrich_post_schema,
    normalize_and_enrich_posts_batch,
     normalize_comment_item,
    safe_int,
)

logger = logging.getLogger(__name__)

# 全新独立的 MongoDB 集合名称，与历史 `social_media_posts` 完全物理隔离，互不产生交集
NEW_HARVESTED_COLLECTION_NAME = "harvested_social_posts"


class BasePlatformHarvester(ABC):
    """
    所有平台拉取适配器的统一基类。
    新增任何平台（如知乎 `zhihu`、小红书 `xiaohongshu`）只需继承此类并实现对应方法。
    """

    SOURCE_NAME: str = "unknown"

    def __init__(self, proxies: Optional[Dict[str, str]] = None):
        self.proxies = proxies if proxies is not None else (get_requests_proxies() or {})

    # ==========================================================================
    # 四大通用采集原语（各平台按自身能力实现，不支持的原语可安全返回空列表）
    # ==========================================================================
    @abstractmethod
    def fetch_hot_or_recommended(self, count: int = 20, **kwargs) -> List[Dict[str, Any]]:
        """原语 1：拉取平台热榜 / 首页算法推荐流"""
        raise NotImplementedError

    def fetch_user_posts(self, user_id: str, count: int = 15, **kwargs) -> List[Dict[str, Any]]:
        """原语 2：拉取指定博主 / 大 V 主页的最新帖子"""
        logger.debug(f"[{self.SOURCE_NAME}] 当前适配器未启用 fetch_user_posts (user_id={user_id})")
        return []

    def search_or_topic_posts(self, query: str, count: int = 20, **kwargs) -> List[Dict[str, Any]]:
        """原语 3：按关键词、话题标签、垂直板块或标的搜索高价值帖子"""
        logger.debug(f"[{self.SOURCE_NAME}] 当前适配器未启用 search_or_topic_posts (query={query})")
        return []

    def fetch_post_comments(self, post_id: str, max_comments: int = 10, **kwargs) -> List[Dict[str, Any]]:
        """原语 4：拉取指定帖子底下的前排高赞评论（神评树）"""
        logger.debug(f"[{self.SOURCE_NAME}] 当前适配器未启用 fetch_post_comments (post_id={post_id})")
        return []

    # ==========================================================================
    # 本平台差异抹平与价值过滤通用工具
    # ==========================================================================
    def create_empty_post(self, card_type: str = "BUZZ_SHORT") -> Dict[str, Any]:
        """创建带有当前平台 `source` 标识的标准空白帖子骨架"""
        return create_standard_post_schema(source=self.SOURCE_NAME, card_type=card_type)

    def normalize_post(self, post: Dict[str, Any], download_media: bool = False) -> Dict[str, Any]:
        """将单条帖子抹平平台差异（统一时间戳、内嵌 `[插图: url]`、统一 `comments`、对齐互动量纲）"""
        post["source"] = self.SOURCE_NAME
        return normalize_and_enrich_post_schema(
            post,
            download_media=download_media,
            proxies=self.proxies,
        )

    def normalize_batch(self, posts: List[Dict[str, Any]], download_media: bool = True) -> List[Dict[str, Any]]:
        """批量并发抹平差异并下载配图至本地 `media_downloads/`"""
        for p in posts:
            if isinstance(p, dict):
                p["source"] = self.SOURCE_NAME
        return normalize_and_enrich_posts_batch(
            posts,
            download_media=download_media,
            proxies=self.proxies,
        )

    def normalize_comments(self, raw_comments: List[Any], parent_post_id: str = "", default_ts: int = 0) -> List[Dict[str, Any]]:
        """将本平台原始评论列表统一转化为标准评论结构（含 `content`, `likes`, `author_name`）"""
        out = []
        seen = set()
        for c in raw_comments or []:
            nc = normalize_comment_item(c, parent_post_id=parent_post_id, default_ts=default_ts)
            if not nc:
                continue
            key = nc["reply_id"] or nc["content"][:80]
            if key in seen:
                continue
            seen.add(key)
            out.append(nc)
        return out

    def is_valuable_post(self, post: Dict[str, Any], config: Dict[str, Any]) -> bool:
        """
        通用质量守门员：拒绝无意义水帖，确保入库素材具备真实创作价值。
        支持按 `min_text_length`、`min_likes`、`min_views`、`require_media` 过滤。
        """
        if not isinstance(post, dict) or not post.get("post_id"):
            return False

        text = clean_post_text((post.get("content") or {}).get("text_content") or "")
        images = (post.get("media") or {}).get("images") or []
        min_len = safe_int(config.get("min_text_length"), 15)

        # 无图且文字极短的纯水帖直接淘汰
        if not images and len(text) < min_len:
            return False

        if config.get("require_media") and not images:
            return False

        eng = post.get("engagement") or {}
        likes = safe_int(eng.get("like_count"), 0)
        views = safe_int(eng.get("view_count"), 0)
        comments = safe_int(eng.get("comment_count"), 0)

        min_likes = safe_int(config.get("min_likes"), 0)
        min_views = safe_int(config.get("min_views"), 0)
        min_comments = safe_int(config.get("min_comments"), 0)

        # 只要满足最低点赞、最低浏览或最低评论之一（或均未设严苛门槛）即放行
        if min_likes > 0 and likes < min_likes and views < max(min_views, min_likes * 50):
            return False
        if min_views > 0 and views < min_views and likes < max(min_likes, 20):
            return False
        if min_comments > 0 and comments < min_comments and likes < max(min_likes, 30):
            return False

        return True

    # ==========================================================================
    # 标准调度入口：供上层定时任务统一调用
    # ==========================================================================
    @abstractmethod
    def harvest(self, config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        按平台配置执行完整的一轮高价值采集（拉取 -> 质量过滤 -> 补充神评 -> 差异抹平与配图下载），
        返回可直接入库的标准格式帖子列表。
        """
        raise NotImplementedError
