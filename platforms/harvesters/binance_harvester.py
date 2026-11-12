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
import time
from typing import Any, Dict, List, Optional

from common.common_utils import PLATFORM_BINANCE, safe_int
from platforms.binance_api import (
    clean_binance_replies,
    fetch_binance_feed,
    fetch_binance_future_hot_coins,
    fetch_binance_hot_hashtags,
    fetch_binance_replies,
)
from platforms.harvesters.base_harvester import BasePlatformHarvester
from platforms.harvesters.okx_harvester import CRYPTO_KEYWORD_TO_TICKER

logger = logging.getLogger(__name__)


def _strip_quote_asset(symbol: str) -> str:
    """将合约交易对符号（如 `AGTUSDT`、`BTCUSDC`）还原为基础币种代码（`AGT`、`BTC`）。"""
    s = str(symbol or "").strip().upper()
    for suffix in ("USDT", "USDC", "FDUSD", "BUSD"):
        if s.endswith(suffix) and len(s) > len(suffix):
            return s[: -len(suffix)]
    return s


class BinanceHarvester(BasePlatformHarvester):
    """
    币安广场 (Binance Square) 采集与归一化适配器（100% 免登录）。
    覆盖「官方热门话题榜 (`hot-list`) + 合约热搜币榜 (`topSearchList`) + 首页推荐流 + 关键词/币种精准检索」，
    并内置接口异常、空流与数据质量自检。
    """

    SOURCE_NAME = PLATFORM_BINANCE

    def fetch_hot_or_recommended(self, count: int = 20, **kwargs) -> List[Dict[str, Any]]:
        """原语 1：拉取币安广场首页推荐爆款流（延迟统一阶段下载图片，避免重复阻塞）"""
        try:
            raw_posts = fetch_binance_feed(count=count, download_media=False) or []
            if count > 0 and not raw_posts:
                self.record_health_issue(
                    code="BINANCE_RECOMMEND_EMPTY",
                    message="币安广场首页推荐流接口返回 0 条数据，疑似接口变更或代理网络受阻",
                    level="critical",
                    channel="推荐流",
                )
            return self.normalize_batch(raw_posts, download_media=False)
        except Exception as e:
            self.record_health_issue(
                code="BINANCE_RECOMMEND_ERROR",
                message=f"币安广场首页推荐流请求异常: {e}",
                level="critical",
                channel="推荐流",
            )
            return []

    def fetch_hot_hashtags_posts(
        self,
        max_hashtags: int = 5,
        posts_per_hashtag: int = 4,
    ) -> List[Dict[str, Any]]:
        """拉取币安广场官方实时热门话题榜 (`hashtag/hot-list`) 前排话题下的高热帖子。"""
        self.last_hot_hashtags: List[str] = []
        try:
            hot_items = fetch_binance_hot_hashtags() or []
        except Exception as e:
            self.record_health_issue(
                code="BINANCE_HOT_HASHTAGS_ERROR",
                message=f"拉取币安官方热门话题榜异常: {e}",
                level="warning",
                channel="官方热搜话题榜",
            )
            return []

        if not hot_items:
            self.record_health_issue(
                code="BINANCE_HOT_HASHTAGS_EMPTY",
                message="币安官方热门话题榜 (`hashtag/hot-list`) 返回为空，疑似榜单接口变更",
                level="warning",
                channel="官方热搜话题榜",
            )
            return []

        by_id: Dict[str, Dict[str, Any]] = {}
        for item in hot_items[: max(1, max_hashtags)]:
            if not isinstance(item, dict):
                continue
            tag = str(item.get("hashtag") or item.get("hashtagNormalized") or "").strip()
            if not tag:
                continue
            self.last_hot_hashtags.append(tag)
            raw_posts = fetch_binance_feed(count=max(2, posts_per_hashtag), keyword=tag, download_media=False) or []
            for p in self.normalize_batch(raw_posts, download_media=False):
                if p.get("post_id"):
                    by_id[p["post_id"]] = p

        return list(by_id.values())

    def fetch_user_posts(self, user_id: str, count: int = 15, **kwargs) -> List[Dict[str, Any]]:
        """原语 2：按币安作者名或关键词定向搜索该博主的帖子"""
        if not user_id:
            return []
        raw_posts = fetch_binance_feed(count=count, keyword=str(user_id).strip(), download_media=False) or []
        return self.normalize_batch(raw_posts, download_media=False)

    def search_or_topic_posts(
        self,
        query: str,
        count: int = 25,
        is_coin_token: bool = False,
        hours_back: int = 72,
        download_media: bool = False,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        原语 3：按关键词（支持 `|` 或 `,` 分隔多词）或币种代码 (`token="BTC"`) 拉取币安广场垂直内容流。
        候选检索阶段关闭同步图片下载以大幅提速，筛选出最终 Top `count` 后再并发下载真实原图。
        """
        reset_health = bool(kwargs.get("reset_health", True))
        if reset_health:
            self.reset_health_state()

        raw_q = str(query or "").replace(",", "|")
        keywords = [w.strip() for w in raw_q.split("|") if w.strip()]
        if not keywords:
            return self.fetch_hot_or_recommended(count=count)

        min_ts = int(time.time() - max(1, hours_back) * 3600) if (hours_back and hours_back > 0) else 0
        by_id: Dict[str, Dict[str, Any]] = {}
        per_kw = max(8, min(count, 25))
        search_errors = 0

        for kw in keywords:
            clean_coin = kw.lstrip("$").strip()
            mapped_ticker = CRYPTO_KEYWORD_TO_TICKER.get(clean_coin.lower())
            is_ticker = is_coin_token or bool(mapped_ticker) or (
                clean_coin.isascii() and clean_coin.isalnum() and clean_coin.isupper() and 2 <= len(clean_coin) <= 8
            )
            target_ticker = mapped_ticker or clean_coin.upper()
            try:
                if is_ticker:
                    raw_posts = fetch_binance_feed(count=per_kw, token=target_ticker, download_media=False) or []
                    # 对中英文币名（如“比特币”、“Ethereum”），除币种专流外再补充关键词全文搜索以兼顾长文深度帖
                    if not is_coin_token and not clean_coin.isupper():
                        raw_posts.extend(fetch_binance_feed(count=max(6, per_kw // 2), keyword=kw, download_media=False) or [])
                else:
                    raw_posts = fetch_binance_feed(count=per_kw, keyword=kw, download_media=False) or []
                    # 币安广场搜索对空格分隔的多词长短语（如 "黄仁勋 出轨"）可能只命中精确连写，补充去空格或核心词检索
                    if len(raw_posts) < 3 and " " in kw:
                        compact_kw = "".join(kw.split())
                        if not compact_kw.isascii():
                            raw_posts.extend(fetch_binance_feed(count=per_kw, keyword=compact_kw, download_media=False) or [])
                        if not raw_posts:
                            sub_terms = [t.strip() for t in kw.split() if len(t.strip()) >= 2][:2]
                            for st in sub_terms:
                                raw_posts.extend(fetch_binance_feed(count=per_kw, keyword=st, download_media=False) or [])
            except Exception as e:
                search_errors += 1
                self.record_health_issue(
                    code="BINANCE_SEARCH_ERROR",
                    message=f"币安广场搜索关键词 {kw!r} 抛出异常: {e}",
                    level="warning",
                    channel=f"搜索:{kw}",
                )
                continue

            normalized = self.normalize_batch(raw_posts, download_media=False)
            for item in normalized:
                pid = str(item.get("post_id") or "")
                pub_ts = safe_int(item.get("publish_time"), 0)
                if not pid:
                    continue
                if min_ts > 0 and pub_ts > 0 and pub_ts < min_ts:
                    continue
                by_id[pid] = item

        if search_errors > 0 and search_errors == len(keywords):
            self.record_health_issue(
                code="BINANCE_SEARCH_ALL_FAILED",
                message=f"币安广场全部 {len(keywords)} 个关键词搜索均异常失败",
                level="critical",
                channel="搜索流",
            )

        results = sorted(
            by_id.values(),
            key=lambda p: (
                safe_int((p.get("engagement") or {}).get("like_count"), 0)
                + safe_int((p.get("engagement") or {}).get("comment_count"), 0) * 2
                + safe_int((p.get("engagement") or {}).get("quote_count"), 0) * 2,
                safe_int(p.get("publish_time"), 0),
            ),
            reverse=True,
        )[:count]

        if download_media and results:
            results = self.normalize_batch(results, download_media=True)

        if reset_health:
            self.audit_posts_quality(
                results,
                scene="search",
                expect_comments=True,
                expect_media_download=download_media,
                max_fresh_age_hours=max(72, hours_back or 72),
            )
            valid_ids = {p["post_id"] for p in results if self.is_valuable_post(p)}
            results = [p for p in results if p["post_id"] in valid_ids]
        return results

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
        self.reset_health_state()
        cfg = config or {}
        recommend_count = safe_int(cfg.get("recommend_count"), 15)
        include_hot_hashtags = bool(cfg.get("include_hot_hashtags", True))
        hot_hashtags_count = safe_int(cfg.get("hot_hashtags_count"), 5)
        posts_per_hashtag = safe_int(cfg.get("posts_per_hashtag"), 4)
        include_hot_future_coins = bool(cfg.get("include_hot_future_coins", True))
        hot_future_coins_count = safe_int(cfg.get("hot_future_coins_count"), 4)
        keywords = cfg.get("keywords") or []
        configured_coins = [str(c).strip().upper() for c in (cfg.get("coins") or []) if str(c).strip()]
        download_media = bool(cfg.get("download_media", True))

        self.last_channel_stats: Dict[str, int] = {}
        self.last_hot_hashtags: List[str] = []
        self.last_hot_coins: List[str] = []
        by_id: Dict[str, Dict[str, Any]] = {}

        # 1. 首页推荐流
        if recommend_count > 0:
            rec_posts = self.fetch_hot_or_recommended(count=recommend_count)
            self.last_channel_stats["推荐流"] = len(rec_posts)
            for p in rec_posts:
                if self.is_valuable_post(p, cfg):
                    by_id[p["post_id"]] = p

        # 2. 币安官方实时热搜话题榜 (`hashtag/hot-list`)
        if include_hot_hashtags and hot_hashtags_count > 0:
            tag_posts = self.fetch_hot_hashtags_posts(
                max_hashtags=hot_hashtags_count,
                posts_per_hashtag=posts_per_hashtag,
            )
            self.last_channel_stats["官方热门话题榜"] = len(tag_posts)
            for p in tag_posts:
                if self.is_valuable_post(p, cfg):
                    by_id[p["post_id"]] = p

        # 3. 固定配置关键词搜索流
        for kw in keywords:
            kw_posts = self.search_or_topic_posts(
                query=kw,
                count=safe_int(cfg.get("per_keyword_count"), 8),
                is_coin_token=False,
                download_media=False,
                reset_health=False,
            )
            self.last_channel_stats[f"关键词:{kw}"] = len(kw_posts)
            for p in kw_posts:
                if self.is_valuable_post(p, cfg):
                    by_id[p["post_id"]] = p

        # 4. 币安合约热搜榜异动币 (`topSearchList`) + 固定主流币去重合并
        merged_coins: List[str] = []
        if include_hot_future_coins and hot_future_coins_count > 0:
            try:
                raw_hot_futures = fetch_binance_future_hot_coins() or []
                if not raw_hot_futures:
                    self.record_health_issue(
                        code="BINANCE_HOT_FUTURES_EMPTY",
                        message="币安合约热搜币榜 (`topSearchList`) 返回为空",
                        level="warning",
                        channel="合约热搜币榜",
                    )
                for fc in raw_hot_futures:
                    if not isinstance(fc, dict):
                        continue
                    base_sym = _strip_quote_asset(fc.get("symbol") or "")
                    if base_sym and base_sym not in self.last_hot_coins:
                        self.last_hot_coins.append(base_sym)
                        if len(self.last_hot_coins) >= hot_future_coins_count:
                            break
                for hc in self.last_hot_coins:
                    if hc not in merged_coins:
                        merged_coins.append(hc)
            except Exception as e:
                self.record_health_issue(
                    code="BINANCE_HOT_FUTURES_ERROR",
                    message=f"拉取币安合约热搜币榜异常: {e}",
                    level="warning",
                    channel="合约热搜币榜",
                )

        for cc in configured_coins:
            if cc not in merged_coins:
                merged_coins.append(cc)

        per_coin_cnt = safe_int(cfg.get("per_coin_count"), 5)
        for coin in merged_coins:
            c_posts = self.search_or_topic_posts(
                query=coin,
                count=per_coin_cnt,
                is_coin_token=True,
                download_media=False,
                reset_health=False,
            )
            prefix = "热搜币" if coin in self.last_hot_coins else "币种"
            self.last_channel_stats[f"{prefix}:{coin}"] = len(c_posts)
            for p in c_posts:
                if self.is_valuable_post(p, cfg):
                    by_id[p["post_id"]] = p

        final_posts = self.normalize_batch(list(by_id.values()), download_media=download_media)
        self.audit_posts_quality(
            final_posts,
            scene="harvest",
            expect_comments=True,
            expect_media_download=download_media,
            max_fresh_age_hours=168,
        )
        return [p for p in final_posts if self.is_valuable_post(p, cfg)]
