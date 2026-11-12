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

❌ 4. [已避坑 / 禁止踩坑] 误用 `/priapi/v5/content/public/search` 假搜索接口
   - 实测发现：该接口对任意 keyword（如 `'黄仁勋'`、`'OpenAI'`、`'ETH'`）均返回 HTTP 200 但内容是同一批默认推荐帖！
   - 正确做法：币种标的走 `fetch_okx_feed(token=...)` 并校验提及关系，通用关键词走广场大池本地精确匹配，绝不混入假命中噪音。
================================================================================
"""
import logging
import time
from typing import Any, Dict, List, Optional

from common.common_utils import PLATFORM_OKX, safe_int
from platforms.harvesters.base_harvester import BasePlatformHarvester
from platforms.okx_api import (
    fetch_okx_feed,
    fetch_okx_hot_coins,
    fetch_okx_replies,
    fetch_okx_user_timeline,
)

logger = logging.getLogger(__name__)

# 中英文主流加密币种别名 -> OKX 官方交易对基础符号映射（确保搜“比特币/Ethereum/狗狗币”直达对应 K 线讨论流）
CRYPTO_KEYWORD_TO_TICKER: Dict[str, str] = {
    "bitcoin": "BTC",
    "比特币": "BTC",
    "大饼": "BTC",
    "btc": "BTC",
    "ethereum": "ETH",
    "以太坊": "ETH",
    "二饼": "ETH",
    "eth": "ETH",
    "solana": "SOL",
    "索拉纳": "SOL",
    "sol": "SOL",
    "dogecoin": "DOGE",
    "狗狗币": "DOGE",
    "doge": "DOGE",
    "ripple": "XRP",
    "瑞波": "XRP",
    "瑞波币": "XRP",
    "xrp": "XRP",
    "binance coin": "BNB",
    "币安币": "BNB",
    "bnb": "BNB",
    "okb": "OKB",
    "pepe": "PEPE",
    "sui": "SUI",
    "trump": "TRUMP",
    "川普币": "TRUMP",
    "cardano": "ADA",
    "艾达币": "ADA",
    "ada": "ADA",
    "avalanche": "AVAX",
    "avax": "AVAX",
    "chainlink": "LINK",
    "link": "LINK",
    "toncoin": "TON",
    "ton": "TON",
    "zcash": "ZEC",
    "zec": "ZEC",
    "bitcoin cash": "BCH",
    "bch": "BCH",
    "litecoin": "LTC",
    "莱特币": "LTC",
    "ltc": "LTC",
}


def _resolve_crypto_tickers_from_keywords(keywords: List[str]) -> List[str]:
    """从查询关键词列表中识别出对应的加密代币符号（支持 `$BTC`、`BTC`、`比特币`、`Ethereum` 等）。"""
    tickers: List[str] = []
    seen = set()
    for kw in keywords:
        clean_kw = str(kw or "").lstrip("$").strip()
        if not clean_kw:
            continue
        low_kw = clean_kw.lower()
        mapped = CRYPTO_KEYWORD_TO_TICKER.get(low_kw)
        if not mapped:
            for token_part in low_kw.split():
                t_clean = token_part.lstrip("$").strip()
                if t_clean in CRYPTO_KEYWORD_TO_TICKER:
                    mapped = CRYPTO_KEYWORD_TO_TICKER[t_clean]
                    break
        if not mapped and clean_kw.isascii() and clean_kw.isalnum() and clean_kw.isupper() and 2 <= len(clean_kw) <= 7:
            mapped = clean_kw
        if mapped and mapped not in seen:
            seen.add(mapped)
            tickers.append(mapped)
    return tickers


def _post_matches_any_keyword(post: Dict[str, Any], keywords: List[str]) -> bool:
    if not keywords:
        return True
    cnt = post.get("content") or {}
    tags = " ".join(str(t) for t in (cnt.get("hashtags") or []))
    coins = {str(c).upper() for c in (cnt.get("mentioned_coins") or []) if c}
    blob = f"{cnt.get('title') or ''} {cnt.get('text_content') or ''} {tags}".lower()
    for kw in keywords:
        clean_kw = kw.strip()
        if not clean_kw:
            continue
        mapped_coin = CRYPTO_KEYWORD_TO_TICKER.get(clean_kw.lstrip("$").lower()) or clean_kw.lstrip("$").upper()
        if mapped_coin in coins:
            return True
        tokens = [t for t in clean_kw.lower().split() if t]
        if tokens and all(tok in blob for tok in tokens):
            return True
    return False


class OkxHarvester(BasePlatformHarvester):
    """
    欧易 (OKX Orbit) 采集与归一化适配器（100% 免登录）。
    提供中文交易员实盘盈亏、合约故事与高互动讨论素材。
    注：OKX Orbit 属于纯垂直加密货币交易社区，圈外娱乐/泛社会话题天然极少，搜索未命中时诚实返回真实条数。
    """

    SOURCE_NAME = PLATFORM_OKX

    def fetch_hot_or_recommended(self, count: int = 15, **kwargs) -> List[Dict[str, Any]]:
        """原语 1：拉取欧易 Orbit 广场推荐流（已自动附带前排评论）"""
        try:
            raw_posts = fetch_okx_feed(count=count, download_media=False) or []
            if count > 0 and not raw_posts:
                self.record_health_issue(
                    code="OKX_RECOMMEND_EMPTY",
                    message="欧易 Orbit 广场推荐流返回 0 条，疑似接口变更或站点风控",
                    level="critical",
                    channel="推荐流",
                )
            return self.normalize_batch(raw_posts, download_media=False)
        except Exception as e:
            self.record_health_issue(
                code="OKX_RECOMMEND_ERROR",
                message=f"欧易 Orbit 广场推荐流请求异常: {e}",
                level="critical",
                channel="推荐流",
            )
            return []

    def fetch_user_posts(self, user_id: str, count: int = 15, **kwargs) -> List[Dict[str, Any]]:
        """原语 2：拉取指定 OKX 大 V (`author_id`) 主页时间线"""
        if not user_id:
            return []
        raw_posts = fetch_okx_user_timeline(author_id=str(user_id).strip(), count=count, full_schema=True) or []
        return self.normalize_batch(raw_posts, download_media=False)

    def search_or_topic_posts(
        self,
        query: str,
        count: int = 20,
        is_coin_token: bool = False,
        hours_back: int = 72,
        download_media: bool = False,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        原语 3：按币种标的（如 `BTC`, `比特币`, `Ethereum`, `SOL`）或关键词组（`|` 分隔）精确检索欧易广场讨论流。
        严禁调用假命中的 `/public/search`，确保所有返回结果 100% 真实匹配对应币种流或关键词。
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

        if is_coin_token and len(keywords) == 1:
            coin = keywords[0].lstrip("$").strip().upper()
            raw_posts = fetch_okx_feed(count=count, token=coin, download_media=False) or []
            for p in self.normalize_batch(raw_posts, download_media=False):
                pub_ts = safe_int(p.get("publish_time"), 0)
                if min_ts > 0 and pub_ts > 0 and pub_ts < min_ts:
                    continue
                if p.get("post_id"):
                    by_id[p["post_id"]] = p
            self.last_search_note = f"币种专流:{coin}"
        else:
            ticker_kws = _resolve_crypto_tickers_from_keywords(keywords)
            for coin in ticker_kws[:4]:
                raw_coin_posts = fetch_okx_feed(count=max(count, 12), token=coin, download_media=False) or []
                if not raw_coin_posts:
                    self.record_health_issue(
                        code="OKX_COIN_FEED_EMPTY",
                        message=f"欧易币种流 `{coin}` 返回 0 条，请核查该代币是否已在 OKX 上线或接口异常",
                        level="warning",
                        channel=f"币种流:{coin}",
                    )
                for p in self.normalize_batch(raw_coin_posts, download_media=False):
                    pub_ts = safe_int(p.get("publish_time"), 0)
                    if min_ts > 0 and pub_ts > 0 and pub_ts < min_ts:
                        continue
                    if p.get("post_id"):
                        by_id[p["post_id"]] = p

            if not ticker_kws or len(by_id) < max(5, count // 2):
                pool_raw = fetch_okx_feed(count=50, download_media=False) or []
                if not pool_raw and not ticker_kws:
                    self.record_health_issue(
                        code="OKX_SEARCH_POOL_EMPTY",
                        message="欧易广场推荐池拉取返回 0 条，底层接口异常",
                        level="critical",
                        channel="广场匹配池",
                    )
                for p in self.normalize_batch(pool_raw, download_media=False):
                    pub_ts = safe_int(p.get("publish_time"), 0)
                    if min_ts > 0 and pub_ts > 0 and pub_ts < min_ts:
                        continue
                    if p.get("post_id") and _post_matches_any_keyword(p, keywords):
                        by_id[p["post_id"]] = p

            if ticker_kws:
                self.last_search_note = f"命中加密标的流:{','.join(ticker_kws[:4])}"
            elif not by_id:
                self.last_search_note = "非加密货币话题（OKX Orbit为纯币圈交易社区，天然少圈外内容）"
            else:
                self.last_search_note = "广场近期流精确匹配"

        results = sorted(
            by_id.values(),
            key=lambda p: (
                safe_int((p.get("engagement") or {}).get("like_count"), 0)
                + safe_int((p.get("engagement") or {}).get("comment_count"), 0) * 2,
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
            results = [p for p in results if self.is_valuable_post(p)]
        return results

    def fetch_post_comments(self, post_id: str, max_comments: int = 10, **kwargs) -> List[Dict[str, Any]]:
        """原语 4：拉取指定 OKX 帖子底下的热门评论列表"""
        if not post_id:
            return []
        cleaned = fetch_okx_replies(content_id=str(post_id).strip(), required_count=max_comments) or []
        return self.normalize_comments(cleaned, parent_post_id=str(post_id))

    def harvest(self, config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        self.reset_health_state()
        cfg = config or {}
        recommend_count = safe_int(cfg.get("recommend_count"), 15)
        include_hot_coins_rank = bool(cfg.get("include_hot_coins_rank", True))
        hot_coins_rank_count = safe_int(cfg.get("hot_coins_rank_count"), 6)
        configured_coins = [str(c).strip().upper() for c in (cfg.get("coins") or []) if str(c).strip()]
        author_ids = cfg.get("author_ids") or []
        download_media = bool(cfg.get("download_media", True))

        self.last_channel_stats: Dict[str, int] = {}
        self.last_hot_coins: List[str] = []
        by_id: Dict[str, Dict[str, Any]] = {}

        # 1. 欧易广场算法推荐流
        if recommend_count > 0:
            rec_posts = self.fetch_hot_or_recommended(count=recommend_count)
            self.last_channel_stats["推荐流"] = len(rec_posts)
            for p in rec_posts:
                if self.is_valuable_post(p, cfg):
                    by_id[p["post_id"]] = p

        # 2. 官方实时热门币榜 (hot-rank) + 固定核心币种去重合并
        merged_coins: List[str] = []
        if include_hot_coins_rank and hot_coins_rank_count > 0:
            hot_rank_coins = fetch_okx_hot_coins(num=max(12, hot_coins_rank_count * 2)) or []
            if not hot_rank_coins:
                self.record_health_issue(
                    code="OKX_HOT_RANK_EMPTY",
                    message="欧易官方热门币榜 (`rubik/web/public/hot-rank`) 返回为空",
                    level="warning",
                    channel="官方热门币榜",
                )
            self.last_hot_coins = hot_rank_coins[:hot_coins_rank_count]
            for hc in self.last_hot_coins:
                if hc and hc not in merged_coins:
                    merged_coins.append(hc)
        for cc in configured_coins:
            if cc and cc not in merged_coins:
                merged_coins.append(cc)

        per_coin = safe_int(cfg.get("per_coin_count"), 4)
        for coin in merged_coins:
            c_posts = self.search_or_topic_posts(
                query=coin,
                count=per_coin,
                is_coin_token=True,
                hours_back=0,
                download_media=False,
                reset_health=False,
            )
            tag_prefix = "热榜币" if coin in self.last_hot_coins else "币种"
            self.last_channel_stats[f"{tag_prefix}:{coin}"] = len(c_posts)
            for p in c_posts:
                if self.is_valuable_post(p, cfg):
                    by_id[p["post_id"]] = p

        # 3. 可选指定交易员时间线
        for uid in author_ids:
            u_posts = self.fetch_user_posts(user_id=uid, count=safe_int(cfg.get("per_user_count"), 8))
            self.last_channel_stats[f"博主:{uid}"] = len(u_posts)
            for p in u_posts:
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
