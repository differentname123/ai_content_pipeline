# -*- coding: utf-8 -*-
"""
欧易 (OKX Orbit) 专用采集与差异抹平适配器 (OkxHarvester)

================================================================================
【运维速查手册 (Ops Quick Guide) —— 日常巡检与修改必读】
================================================================================
1. 是否使用登录账号？
   - **否（100% 免登录 / 0 账号模式）**。直连欧易 Orbit 公开聚合流接口 (`/priapi/v2/orbit/agg-card/list`)，
     同时自动过滤配置在 `common/config_local.py` (`SELF_OKX_AUTHOR_IDS`) 中的自家矩阵发帖号。

2. 当前到底在拉取谁？
   - ① 欧易广场推荐流：`recommend_count` 条（默认 12 条）；
   - ② 热门币种讨论流：`PLATFORM_HARVEST_CONFIGS["okx"]["coins"]`（默认 `["BTC", "ETH", "SOL"]`）；
   - ③ 指定交易员时间线：`PLATFORM_HARVEST_CONFIGS["okx"]["author_ids"]` 中的博主数字 ID。

3. 平常想修改应该怎么改？
   - 【增删监控币种或指定交易员】：直接修改 `platforms/harvester_runner.py` 中 `"okx"` 配置下的 `"coins"` 或 `"author_ids"` 列表。
   - 【新增自家矩阵账号排除名单】：在 `common/config_local.py` 的 `SELF_OKX_AUTHOR_IDS` 中添加账号 ID。

================================================================================
【欧易 (OKX Orbit) 接口探索与踩坑档案 (Exploration & Pitfall Log)】
维护必读：以下记录了我们在欧易 Orbit 广场实测过的所有采集路径与踩坑经验。
================================================================================

✅ 1. [已采用 / 主力通道A] OKX Orbit 聚合卡片接口 (`/priapi/v2/orbit/agg-card/list` & SSR Hydration)
   - 原理：请求头必须包含 `app-type: web` 与 `x-utc: 8`，支持拉取广场推荐流、指定币种流 (`token="BTC"`)
           以及指定大 V 的主页时间线 (`fetch_okx_user_timeline`)。
   - 优势：100% 免登录、无封号风险，已内置自营矩阵账号拦截 (`SELF_OKX_AUTHOR_IDS`)、配图下载与神评富化。

✅ 2. [已采用 / 主力通道B] 单帖公共评论接口 (`/priapi/v5/content/public/comments`)
   - 原理：通过 `contentId` 游标分页拉取高赞神评，内置 `clean_okx_replies` 清洗。

❌ 3. [已避坑 / 禁止踩坑] 缺少 `app-type: web` 请求头或直接使用毫秒级时间戳
   - 失败原因：OKX `priapi` 接口若缺少 `app-type: web` 会返回空数据或错误码；
               同时需通过 `normalize_batch` 确保 `publish_time` 统一为 10 位秒级整型并补齐标准 `[插图: url]` 标签。
================================================================================
"""
import logging
from typing import Any, Dict, List, Optional

from common.common_utils import PLATFORM_OKX, safe_int
from platforms.harvesters.base_harvester import BasePlatformHarvester
from platforms.okx_api import (
    fetch_okx_feed,
    fetch_okx_replies,
    fetch_okx_user_timeline,
)

logger = logging.getLogger(__name__)


class OkxHarvester(BasePlatformHarvester):
    """
    欧易 (OKX Orbit) 采集与归一化适配器（100% 免登录）。
    提供中文交易员实盘盈亏、合约故事与高互动讨论素材。
    """

    SOURCE_NAME = PLATFORM_OKX

    def fetch_hot_or_recommended(self, count: int = 15, **kwargs) -> List[Dict[str, Any]]:
        """原语 1：拉取欧易 Orbit 广场推荐流"""
        raw_posts = fetch_okx_feed(count=count) or []
        return self.normalize_batch(raw_posts, download_media=False)

    def fetch_user_posts(self, user_id: str, count: int = 15, **kwargs) -> List[Dict[str, Any]]:
        """原语 2：拉取指定 OKX 大 V (`author_id`) 主页时间线"""
        if not user_id:
            return []
        raw_posts = fetch_okx_user_timeline(author_id=str(user_id).strip(), count=count) or []
        return self.normalize_batch(raw_posts, download_media=False)

    def search_or_topic_posts(self, query: str, count: int = 15, **kwargs) -> List[Dict[str, Any]]:
        """原语 3：按币种标的（如 `BTC`, `ETH`, `SOL`）拉取欧易广场讨论流"""
        coin = str(query or "").strip().upper().lstrip("$")
        if not coin:
            return self.fetch_hot_or_recommended(count=count)
        raw_posts = fetch_okx_feed(count=count, token=coin) or []
        return self.normalize_batch(raw_posts, download_media=False)

    def fetch_post_comments(self, post_id: str, max_comments: int = 10, **kwargs) -> List[Dict[str, Any]]:
        """原语 4：拉取指定 OKX 帖子底下的热门评论列表"""
        if not post_id:
            return []
        cleaned = fetch_okx_replies(content_id=str(post_id).strip(), required_count=max_comments) or []
        return self.normalize_comments(cleaned, parent_post_id=str(post_id))

    def harvest(self, config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        cfg = config or {}
        recommend_count = safe_int(cfg.get("recommend_count"), 12)
        coins = cfg.get("coins") or []
        author_ids = cfg.get("author_ids") or []
        download_media = bool(cfg.get("download_media", True))

        logger.info(
            f"📋 [运维状态/OKX] 账号状态: [免登录模式 (0账号公开接口 + 已启用自营号过滤)] "
            f"| 采集范围: [推荐流({recommend_count}条) + 币种({', '.join(coins)}) + 指定博主({len(author_ids)}个)] "
            f"| 门槛: [赞>={cfg.get('min_likes', 2)} 或 阅>={cfg.get('min_views', 200)}]"
        )

        by_id: Dict[str, Dict[str, Any]] = {}

        if recommend_count > 0:
            for p in self.fetch_hot_or_recommended(count=recommend_count):
                if self.is_valuable_post(p, cfg):
                    by_id[p["post_id"]] = p

        for coin in coins:
            for p in self.search_or_topic_posts(query=coin, count=safe_int(cfg.get("per_coin_count"), 8)):
                if self.is_valuable_post(p, cfg):
                    by_id[p["post_id"]] = p

        for uid in author_ids:
            for p in self.fetch_user_posts(user_id=uid, count=safe_int(cfg.get("per_user_count"), 8)):
                if self.is_valuable_post(p, cfg):
                    by_id[p["post_id"]] = p

        final_posts = self.normalize_batch(list(by_id.values()), download_media=download_media)
        logger.info(
            f"[欧易适配器] 本轮采集完成 | 推荐流: [{recommend_count}] | 币种: [{len(coins)}] | 指定博主: [{len(author_ids)}] "
            f"| 最终入库候选: [{len(final_posts)}条]"
        )
        return final_posts
