# -*- coding: utf-8 -*-
"""
币安广场 (Binance Square) 专用采集与差异抹平适配器 (BinanceHarvester)

================================================================================
【运维速查手册 (Ops Quick Guide) —— 日常巡检与修改必读】
================================================================================
1. 是否使用登录账号？
   - **否（100% 免登录 / 0 账号模式）**。直连币安广场公开 Web 接口 (`/bapi/composite/.../friendly/pgc/...`)，
     同时自动过滤配置在 `common/config_local.py` (`SELF_BINANCE_USERNAMES`) 中的自家矩阵发帖号，防止自产自销。

2. 当前到底在拉取谁？
   - ① 首页算法推荐爆款流：`recommend_count` 条（默认 15 条）；
   - ② 关键词情绪流：`PLATFORM_HARVEST_CONFIGS["binance"]["keywords"]`（如 `["爆仓", "实盘", "巨鲸"]`）；
   - ③ 指定币种讨论流：`PLATFORM_HARVEST_CONFIGS["binance"]["coins"]`（如 `["BTC", "ETH", "SOL"]`）；
   - ④ 自动富化：每条入选帖自动拉取完整长文、前 10 条热评 (`sort_by=1` 最热排序) 并下载图片至 `media_downloads/`。

3. 平常想修改应该怎么改？
   - 【增删搜索关键词 / 监控币种】：直接修改 `platforms/harvester_runner.py` 中 `"binance"` 配置下的 `"keywords"` 与 `"coins"` 列表。
   - 【新增自家矩阵账号排除名单】：在 `common/config_local.py` 的 `SELF_BINANCE_USERNAMES` 中添加账号名。

================================================================================
【币安广场 (Binance Square) 接口探索与踩坑档案 (Exploration & Pitfall Log)】
维护必读：以下记录了我们在币安广场实测过的所有采集路径与踩坑经验。
================================================================================

✅ 1. [已采用 / 主力通道A] 币安友好公共流接口 (`/bapi/composite/v9/friendly/pgc/feed/feed-recommend/list` 等)
   - 原理：通过 POST 携带动态生成的 `X-UI-REQUEST-TRACE` 与 `X-TRACE-ID` 请求头，支持：
           ① 首页推荐流 (`feed-recommend/list`)
           ② 关键词搜索流 (`content/search/list`)
           ③ 指定币种讨论流 (`feed/token-feed/list`)
   - 优势：100% 免登录、无封号风险，已内置长文富文本 (`BUZZ_LONG` 的 `ViewInstance0` JSON 树) 深度解析、
           媒体 `dHash` 本地去重下载与神评并发拉取。

✅ 2. [已采用 / 主力通道B] 单帖高赞神评接口 (`/bapi/composite/v2/friendly/pgc/reply/list`)
   - 原理：免登拉取指定 `contentId` 下的热门回复，并针对非中文回复自动提取 `translatedData` 译文。

❌ 3. [已避坑 / 禁止踩坑] 漏过滤自营矩阵账号导致「自产自销死循环」
   - 风险说明：如果采集器不把自家矩阵账号 (`SELF_BINANCE_USERNAMES` / `SELF_BINANCE_AUTHOR_NAMES`) 过滤掉，
               之前发出去的 AI 帖子会被重新抓回素材库二次洗稿。底层 `clean_universal_posts` 已强制内嵌自营拦截。

❌ 4. [已避坑 / 禁止踩坑] 直接使用币安返回的 13 位毫秒时间戳 (`date: 1794367260000`) 与其他平台混排
   - 风险说明：币安原始 `date` 字段为毫秒级，若不转为 10 位秒级时间戳，在跨平台按 `publish_time` 倒序排列时
               币安帖子会永远排在 10 位秒级平台（如 X、Reddit、HN）前面。本适配器通过 `normalize_batch` 强制统一为 10 位秒级整型。
================================================================================
"""
import logging
from typing import Any, Dict, List, Optional

from common.common_utils import PLATFORM_BINANCE, safe_int
from platforms.binance_api import (
    clean_binance_replies,
    fetch_binance_feed,
    fetch_binance_replies,
)
from platforms.harvesters.base_harvester import BasePlatformHarvester

logger = logging.getLogger(__name__)


class BinanceHarvester(BasePlatformHarvester):
    """
    币安广场 (Binance Square) 采集与归一化适配器（100% 免登录）。
    专门提供中文加密圈高情绪实盘交割单、暴富暴负故事与评论区神评素材。
    """

    SOURCE_NAME = PLATFORM_BINANCE

    def fetch_hot_or_recommended(self, count: int = 20, **kwargs) -> List[Dict[str, Any]]:
        """原语 1：拉取币安广场首页推荐爆款流（含自动长文解析、配图本地下载与评论富化）"""
        raw_posts = fetch_binance_feed(count=count) or []
        return self.normalize_batch(raw_posts, download_media=False)

    def fetch_user_posts(self, user_id: str, count: int = 15, **kwargs) -> List[Dict[str, Any]]:
        """原语 2：按币安作者名或关键词定向搜索该博主的帖子"""
        if not user_id:
            return []
        raw_posts = fetch_binance_feed(count=count, keyword=str(user_id).strip()) or []
        return self.normalize_batch(raw_posts, download_media=False)

    def search_or_topic_posts(
        self,
        query: str,
        count: int = 20,
        is_coin_token: bool = False,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        原语 3：按关键词 (`keyword`) 或币种代码 (`token="BTC"`) 拉取币安广场垂直内容流。
        """
        q = str(query or "").strip()
        if not q:
            return self.fetch_hot_or_recommended(count=count)
        if is_coin_token or (q.isupper() and 2 <= len(q) <= 8):
            raw_posts = fetch_binance_feed(count=count, token=q) or []
        else:
            raw_posts = fetch_binance_feed(count=count, keyword=q) or []
        return self.normalize_batch(raw_posts, download_media=False)

    def fetch_post_comments(
        self,
        post_id: str,
        max_comments: int = 10,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """原语 4：拉取指定币安帖子底下的前排高赞神评"""
        if not post_id:
            return []
        raw_replies = fetch_binance_replies(content_id=post_id, sort_by=1, required_count=max_comments) or []
        cleaned = clean_binance_replies(raw_replies)
        return self.normalize_comments(cleaned, parent_post_id=str(post_id))

    def harvest(self, config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        cfg = config or {}
        recommend_count = safe_int(cfg.get("recommend_count"), 15)
        keywords = cfg.get("keywords") or []
        coins = cfg.get("coins") or []
        download_media = bool(cfg.get("download_media", True))

        logger.info(
            f"📋 [运维状态/Binance] 账号状态: [免登录模式 (0账号公开接口 + 已启用自营号过滤)] "
            f"| 采集范围: [推荐流({recommend_count}条) + 关键词({', '.join(keywords)}) + 币种({', '.join(coins)})]"
        )

        by_id: Dict[str, Dict[str, Any]] = {}

        # 1. 首页推荐流
        if recommend_count > 0:
            for p in self.fetch_hot_or_recommended(count=recommend_count):
                if self.is_valuable_post(p, cfg):
                    by_id[p["post_id"]] = p

        # 2. 关键词搜索流
        for kw in keywords:
            for p in self.search_or_topic_posts(query=kw, count=safe_int(cfg.get("per_keyword_count"), 10), is_coin_token=False):
                if self.is_valuable_post(p, cfg):
                    by_id[p["post_id"]] = p

        # 3. 热门币种流
        for coin in coins:
            for p in self.search_or_topic_posts(query=coin, count=safe_int(cfg.get("per_coin_count"), 10), is_coin_token=True):
                if self.is_valuable_post(p, cfg):
                    by_id[p["post_id"]] = p

        final_posts = self.normalize_batch(list(by_id.values()), download_media=download_media)
        logger.info(
            f"[币安适配器] 本轮采集完成 | 推荐流: [{recommend_count}] | 关键词: [{len(keywords)}] | 币种: [{len(coins)}] "
            f"| 最终入库候选: [{len(final_posts)}条]"
        )
        return final_posts
