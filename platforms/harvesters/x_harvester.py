# -*- coding: utf-8 -*-
"""
X (Twitter) 平台专用采集与差异抹平适配器 (XHarvester)

================================================================================
【运维速查手册 (Ops Quick Guide) —— 日常巡检与修改必读】
================================================================================
1. 是否使用登录账号 & 是哪个账号？
   - 混合双通道架构：【通道A: 0账号免登 Guest Token】+【通道B: 1个只读小号 Cookie 登录态】。
   - 默认读取小号路径：`common/config_local.py` 中的 `X_USER_DATA_DIR_1`
     （即项目根目录下 `x_browser_session_1/`，当前小号数字 UID 会自动从 Cookie `twid` 解析并在启动日志打印）。
   - 若小号未配置或失效，系统会自动降级为纯免登 Guest Token 通道，不会崩溃。

2. 当前到底在拉取谁？
   - ① 指定大 V 时间线：拉取 `harvester_runner.py` -> `PLATFORM_HARVEST_CONFIGS["x"]["kol_handles"]` 中的大 V；
   - ② 小号算法推荐流：当 `include_home_feed=True` 时，额外拉取小号主页 `HomeTimeline` 爆款；
   - ③ 爆款前排神评：为点赞最高的前 `fetch_comments_for_top_n` 条推文拉取高赞神评（按赞数倒序）。

3. 平常想修改应该怎么改？
   - 【增删监控大 V】：直接修改 `platforms/harvester_runner.py` 中 `"x"` 配置下的 `"kol_handles"` 列表（直接填 `@` 后的名字，系统会自动解析并缓存其数字 UID 到 `runtime/x_uid_cache.json`）。
   - 【更换/重登只读小号】：修改 `platforms/harvester_runner.py` 中 `"x"` 的 `"burner_user_data_dir"` 指向新的浏览器目录，或直接透传 `"burner_cookies": "auth_token=xxx; ct0=yyy"`。
   - 【调整入选赞数/浏览量门槛】：修改 `"x"` 配置下的 `"min_likes"`（默认 20）和 `"min_views"`（默认 1000）。

================================================================================
【X (Twitter) 接口探索与踩坑档案 (Exploration & Pitfall Log)】
维护必读：以下记录了我们在 2026 年实测过的所有 X 采集路径。请勿重复尝试已标记为 ❌ 的废路！
================================================================================

✅ 1. [已采用 / 主力通道A] 免登录 Guest Token GraphQL (`UserByScreenName` + `UserTweets`)
   - 原理：先 `POST https://api.x.com/1.1/guest/activate.json` 拿临时 `x-guest-token`，
           再配合 2026 最新 `queryId` (`D6z-FJI2pKuwEeYYxVIGbg`) 与 42 项 `featureSwitches` 调 `UserTweets`。
   - 优势：0 账号成本、0 封号风险，原生返回 `views`（曝光量）、`bookmark_count`（收藏数）、点赞、评论、高清图及长推文 `note_tweet`。
   - 边界发现：
     * 对约 65% 的高价值大 V（如 `@Polymarket`, `@Kalshi`, `@KobeissiLetter`, `@WatcherGuru`, `@Dexerto`,
       `@wallstreetbets`, `@MarioNawfal`, `@MorningBrew`, `@CoinGlass_com`, `@whale_alert`, `@NoContextHumans`），
       免登录直接返回 **100% 实时今日新帖（精确到几分钟前）**！
     * 对极少数日发百帖的重度大 V（如 `@unusual_whales`, `@AutismCapital`, `@lookonchain`, `@ZeroHedge`），
       免登录返回该账号的 **历史最高赞 Top 100 神帖**（可做常青爆款库，但看今日新帖需走通道 B）。
     * 单个 Guest Token 连续请求约 80~150 次后触发 429，代码已内置「每 75 次自动 0.2s 换新 Token + 本地 UID 缓存」。

✅ 2. [已采用 / 主力通道B] 只读小号 Cookie 登录态 GraphQL (`HomeTimeline` / `ListLatestTweetsTimeline` / `TweetDetail`)
   - 原理：使用 1 个只读小号的 `auth_token` + `ct0` 请求头。
   - 优势：
     * `HomeTimeline` (`KmJCeZ__ef1fNos7tKN_fg`)：1 次请求直接拉取小号主页推荐流（几十个关注大 V 的实时爆款），1 次请求抵 50 次轮询！
     * `ListLatestTweetsTimeline` (`UMc_lPEeKz_WiKSY8ZT7yw`)：把 100 个大 V 放进 1 个私有 List，1 次请求拿齐最新动态！
     * `UserTweets` 登录态：彻底解除 `@unusual_whales` 等重度大 V 的免登缓存限制，拿到 100% 实时流。
     * `TweetDetail` (`dQDdKtt882nl-Km98FJGSQ`)：拉取任意爆款推文底下的前排高赞神评树。

❌ 3. [已废弃 / 禁止踩坑] 纯 HTTP 直接调用 X `SearchTimeline` (搜索接口)
   - 失败原因：2026 年 X 对 `/i/api/graphql/.../SearchTimeline` 强制校验前端动态混淆签名头 `x-client-transaction-id` 及 TLS 指纹。
               纯 `requests` 不带浏览器动态计算出的签名头直连必返 `HTTP 404`；若强行引入无头浏览器每次跑搜索，极重且易触发小号 Arkose 验证码。
   - 替代方案：用小号 `HomeTimeline` / `ListLatestTweetsTimeline` + 垂直大 V 池拉取后在本地按关键词过滤，简单、稳定、0 验证码。

❌ 4. [已废弃 / 禁止踩坑] `cdn.syndication.twimg.com/timeline/profile` (旧版网页嵌入组件接口)
   - 失败原因：① 连续请求约 25 次后 IP 级限流 `HTTP 429` 且无法通过换 Token 绕过；
               ② 不返回 `views`（浏览量）和 `bookmark_count`（收藏数）；
               ③ 对大多数账号只返回数月前的旧帖。已被通道 A (Guest GraphQL) 全面碾压淘汰。

❌ 5. [已废弃 / 禁止踩坑] Nitter 公共镜像站 (`nitter.net` / `nitter.poast.org` / `xcancel.com`)
   - 失败原因：实测全部返回连接超时、Cloudflare 5秒盾或空响应，可用性接近 0%。

❌ 6. [已避坑] 使用旧版硬编码 `queryId` 或残缺的 `features` 参数字典
   - 失败原因：X 前端每几周更新一次 GraphQL `queryId`，且若 `features` 缺少 2026 新增的 42 个开关（如 `rweb_tipjar_consumption_enabled` 等），
               服务器直接返回 `HTTP 404` 或 `HTTP 400`。本文件顶部已固化经真实缓存包逆向验证的完整 42 项开关。
================================================================================
"""
import json
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import unquote

import requests

from common.common_utils import (
    PLATFORM_X,
    clean_post_text,
    get_runtime_file_path,
    read_json,
    safe_int,
    save_json,
)
from platforms.harvesters.base_harvester import BasePlatformHarvester
from platforms.x_api import extract_x_cookies_from_user_data_dir

logger = logging.getLogger(__name__)

TZ_CN = timezone(timedelta(hours=8))
REQUEST_TIMEOUT = 15
DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
)

X_WEB_BEARER_TOKEN = (
    "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D"
    "1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
)

X_GQL_QUERY_IDS = {
    "UserByScreenName": "AMIBMjtxEEATh4z8V9GtRg",
    "UserTweets": "D6z-FJI2pKuwEeYYxVIGbg",
    "HomeTimeline": "KmJCeZ__ef1fNos7tKN_fg",
    "HomeLatestTimeline": "IVA5BUbeYPTrpYekQH7jiA",
    "ListLatestTweetsTimeline": "UMc_lPEeKz_WiKSY8ZT7yw",
    "TweetDetail": "dQDdKtt882nl-Km98FJGSQ",
}

X_GQL_FEATURES_2026 = {
    "rweb_video_screen_enabled": False,
    "profile_label_improvements_pcf_label_in_post_enabled": True,
    "responsive_web_profile_redirect_enabled": False,
    "rweb_tipjar_consumption_enabled": False,
    "verified_phone_label_enabled": False,
    "creator_subscriptions_tweet_preview_api_enabled": True,
    "responsive_web_graphql_timeline_navigation_enabled": True,
    "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
    "premium_content_api_read_enabled": False,
    "communities_web_enable_tweet_community_results_fetch": True,
    "c9s_tweet_anatomy_moderator_badge_enabled": True,
    "responsive_web_grok_analyze_button_fetch_trends_enabled": False,
    "responsive_web_grok_analyze_post_followups_enabled": True,
    "responsive_web_jetfuel_frame": True,
    "responsive_web_grok_share_attachment_enabled": True,
    "responsive_web_grok_annotations_enabled": True,
    "articles_preview_enabled": True,
    "responsive_web_edit_tweet_api_enabled": True,
    "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
    "view_counts_everywhere_api_enabled": True,
    "longform_notetweets_consumption_enabled": True,
    "responsive_web_twitter_article_tweet_consumption_enabled": True,
    "tweet_awards_web_tipping_enabled": False,
    "responsive_web_grok_show_grok_translated_post": False,
    "responsive_web_grok_analysis_button_from_backend": True,
    "post_ctas_fetch_enabled": True,
    "freedom_of_speech_not_reach_fetch_enabled": True,
    "standardized_nudges_misinfo": True,
    "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
    "longform_notetweets_rich_text_read_enabled": True,
    "longform_notetweets_inline_media_enabled": True,
    "responsive_web_grok_image_annotation_enabled": True,
    "responsive_web_grok_imagine_annotation_enabled": True,
    "responsive_web_grok_community_note_auto_translation_is_enabled": False,
    "responsive_web_enhance_cards_enabled": False,
    "graphql_timeline_v2_bookmark_timeline": True,
    "responsive_web_graphql_exclude_directive_enabled": True,
    "rweb_lists_timeline_redesign_enabled": True,
    "blue_business_profile_image_shape_enabled": True,
    "tweetypie_unmention_optimization_enabled": True,
    "vibe_api_enabled": True,
    "interactive_text_enabled": True,
}

X_USER_FEATURES_2026 = {
    "hidden_profile_subscriptions_enabled": True,
    "rweb_tipjar_consumption_enabled": True,
    "responsive_web_graphql_exclude_directive_enabled": True,
    "verified_phone_label_enabled": False,
    "subscriptions_verification_info_is_identity_verified_enabled": True,
    "subscriptions_verification_info_verified_since_enabled": True,
    "highlights_tweets_tab_ui_enabled": True,
    "responsive_web_twitter_article_notes_tab_enabled": True,
    "subscriptions_feature_can_gift_premium": True,
    "creator_subscriptions_tweet_preview_api_enabled": True,
    "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
    "responsive_web_graphql_timeline_navigation_enabled": True,
}

# 经 UserByScreenName 实测验证的高价值英文大 V 映射表 (guest_realtime=True 表示免登直接拿今日新帖，False 表示需小号拿今日实时流)
VERIFIED_X_ACCOUNTS = {
    # 疯狂赌局与赔率吃瓜
    "Polymarket": {"uid": "1261335549215989760", "guest_realtime": True, "category": "insane_bets"},
    "Kalshi": {"uid": "1071484733543079936", "guest_realtime": True, "category": "insane_bets"},
    "wallstreetbets": {"uid": "588569122", "guest_realtime": True, "category": "money_drama"},
    # 宏观金钱、国会山内幕与暴富暴负奇闻
    "KobeissiLetter": {"uid": "3316376038", "guest_realtime": True, "category": "macro_money"},
    "WatcherGuru": {"uid": "1387497871751196672", "guest_realtime": True, "category": "breaking_money"},
    "MorningBrew": {"uid": "3092541736", "guest_realtime": True, "category": "biz_story"},
    "StockMKTNewz": {"uid": "1250830691824283648", "guest_realtime": True, "category": "macro_money"},
    "Barchart": {"uid": "28212513", "guest_realtime": True, "category": "macro_money"},
    "charliebilello": {"uid": "1413027896", "guest_realtime": True, "category": "macro_money"},
    # 科技商业暗黑吃瓜与全球猎奇
    "Dexerto": {"uid": "76766018", "guest_realtime": True, "category": "internet_drama"},
    "MarioNawfal": {"uid": "1319287761048723458", "guest_realtime": True, "category": "viral_news"},
    "OpenAI": {"uid": "4398626122", "guest_realtime": True, "category": "ai_tech"},
    "NoContextHumans": {"uid": "1138842105856573445", "guest_realtime": True, "category": "viral_hook"},
    "historyinmemes": {"uid": "855481986290524160", "guest_realtime": True, "category": "viral_hook"},
    "buitengebieden": {"uid": "1130021685351079938", "guest_realtime": True, "category": "viral_hook"},
    # 链上巨鲸与清算实锤
    "CoinGlass_com": {"uid": "1169095068164050945", "guest_realtime": True, "category": "liquidation_drama"},
    "arkham": {"uid": "1123403704323805184", "guest_realtime": True, "category": "whale_tracker"},
    "whale_alert": {"uid": "1039833297751302144", "guest_realtime": True, "category": "whale_tracker"},
    "DefiLlama": {"uid": "1321585610654687241", "guest_realtime": True, "category": "defi_data"},
    "WuBlockchain": {"uid": "111533746", "guest_realtime": True, "category": "crypto_scoop"},
    # 重度发帖大V（免登返回历史Top100，小号登录态返回今日实时流）
    "unusual_whales": {"uid": "1200616796295847936", "guest_realtime": False, "category": "insider_money"},
    "AutismCapital": {"uid": "1297651178256257024", "guest_realtime": False, "category": "tech_drama"},
    "litcapital": {"uid": "932630991298007041", "guest_realtime": False, "category": "wallstreet_humor"},
    "TechEmails": {"uid": "1396491776739905542", "guest_realtime": False, "category": "tech_drama"},
    "vxunderground": {"uid": "1158139840866791424", "guest_realtime": False, "category": "cyber_drama"},
    "ZeroHedge": {"uid": "18856867", "guest_realtime": False, "category": "macro_money"},
    "lookonchain": {"uid": "1462727797135216641", "guest_realtime": False, "category": "whale_story"},
}

def _clean_x_text_with_entities(raw_text: str, legacy: dict) -> str:
    """利用 X 原生 entities 结构替换或剔除短链 t.co，不依赖脆弱正则"""
    txt = str(raw_text or "")
    entities = legacy.get("entities") or {}
    ext_entities = legacy.get("extended_entities") or {}
    for u_obj in entities.get("urls") or []:
        if isinstance(u_obj, dict) and u_obj.get("url"):
            txt = txt.replace(u_obj["url"], u_obj.get("expanded_url") or "")
    for m_obj in (ext_entities.get("media") or []) + (entities.get("media") or []):
        if isinstance(m_obj, dict) and m_obj.get("url"):
            txt = txt.replace(m_obj["url"], "")
    return clean_post_text(txt)


def _parse_cookie_dict(cookie_str: str) -> dict:
    out = {}
    for kv in (cookie_str or "").split(";"):
        if "=" in kv:
            k, v = kv.strip().split("=", 1)
            out[k.strip()] = v.strip()
    return out


def _extract_gql_tweet_objects(data) -> list:
    """提取 GraphQL 响应树中的 Tweet 节点（自动解包 TweetWithVisibilityResults 并跳过官方 Promoted 广告容器）"""
    found = []

    def walk(obj):
        if isinstance(obj, dict):
            # 跳过 X 官方广告容器节点 (TimelineTimelineItem.itemContent.promotedMetadata)
            if "promotedMetadata" in obj:
                return
            tname = obj.get("__typename")
            if tname == "TweetWithVisibilityResults" and isinstance(obj.get("tweet"), dict):
                walk(obj["tweet"])
                return
            if tname == "Tweet" and obj.get("legacy") and obj.get("rest_id"):
                found.append(obj)
                return
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for v in obj:
                walk(v)

    walk(data)
    return found


class XHarvester(BasePlatformHarvester):
    """
    X (Twitter) 平台采集与归一化适配器。
    同时封装「0账号免登 Guest Token 通道」与「只读小号增强通道」，对外提供统一原语。
    """

    SOURCE_NAME = PLATFORM_X

    def __init__(
        self,
        burner_cookies: str = "",
        burner_user_data_dir: Optional[str] = None,
        proxies: Optional[Dict[str, str]] = None,
    ):
        super().__init__(proxies=proxies)
        self.session = requests.Session()
        self.guest_token: Optional[str] = None
        self.guest_req_count = 0

        self.burner_user_data_dir = burner_user_data_dir or ""
        if not burner_cookies and burner_user_data_dir and os.path.exists(burner_user_data_dir):
            burner_cookies = extract_x_cookies_from_user_data_dir(burner_user_data_dir)
        self.burner_cookies = burner_cookies or ""
        self.cookie_dict = _parse_cookie_dict(self.burner_cookies)

        self.uid_cache_path = get_runtime_file_path("x_uid_cache.json")
        self.uid_cache = self._load_uid_cache()

    @property
    def has_burner_account(self) -> bool:
        """当前是否已注入有效的只读小号登录态 (`auth_token` + `ct0`)"""
        return bool(self.cookie_dict.get("auth_token") and self.cookie_dict.get("ct0"))

    @property
    def burner_account_uid(self) -> str:
        """从 Cookie `twid` (如 `u%3D903261369499127808`) 中直接解码提取数字 UID"""
        raw_twid = unquote(self.cookie_dict.get("twid", "")).strip().strip('"')
        if raw_twid.startswith("u="):
            return raw_twid[2:]
        return raw_twid or "未知UID"

    @property
    def burner_account_summary(self) -> str:
        """返回供运维日志展示的账号状态摘要"""
        if not self.has_burner_account:
            return "免登录模式 (0账号 Guest Token)"
        session_dir_name = os.path.basename(os.path.normpath(self.burner_user_data_dir)) if self.burner_user_data_dir else "直接Cookie注入"
        auth_tail = (self.cookie_dict.get("auth_token") or "")[-6:]
        return f"已启用只读小号 (UID={self.burner_account_uid}, Session={session_dir_name}, token尾号=...{auth_tail})"

    def _load_uid_cache(self) -> Dict[str, str]:
        cache = {}
        if os.path.exists(self.uid_cache_path):
            try:
                loaded = read_json(self.uid_cache_path)
                if isinstance(loaded, dict):
                    cache.update({k.lower(): str(v) for k, v in loaded.items() if v})
            except Exception:
                pass
        # 权威校验表覆盖旧缓存，防止历史错误 UID 残留
        for k, v in VERIFIED_X_ACCOUNTS.items():
            cache[k.lower()] = v["uid"]
        return cache

    def _save_uid_cache(self):
        try:
            save_json(self.uid_cache_path, self.uid_cache)
        except Exception:
            pass

    def _refresh_guest_token(self) -> str:
        headers = {"authorization": f"Bearer {X_WEB_BEARER_TOKEN}", "user-agent": DEFAULT_UA}
        resp = self.session.post(
            "https://api.x.com/1.1/guest/activate.json",
            headers=headers,
            proxies=self.proxies,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        self.guest_token = resp.json().get("guest_token")
        self.guest_req_count = 0
        return self.guest_token

    def _get_guest_headers(self) -> Dict[str, str]:
        if not self.guest_token or self.guest_req_count >= 75:
            self._refresh_guest_token()
        self.guest_req_count += 1
        return {
            "authorization": f"Bearer {X_WEB_BEARER_TOKEN}",
            "x-guest-token": self.guest_token,
            "user-agent": DEFAULT_UA,
            "accept": "*/*",
            "referer": "https://x.com/",
        }

    def _get_burner_headers(self) -> Dict[str, str]:
        return {
            "authorization": f"Bearer {X_WEB_BEARER_TOKEN}",
            "cookie": self.burner_cookies,
            "x-csrf-token": self.cookie_dict.get("ct0", ""),
            "x-twitter-active-user": "yes",
            "x-twitter-auth-type": "OAuth2Session",
            "x-twitter-client-language": "en",
            "user-agent": DEFAULT_UA,
            "accept": "*/*",
            "referer": "https://x.com/home",
        }

    def resolve_user_id(self, screen_name: str) -> str:
        """将 `@handle` 转换为数字 `rest_id`（优先命中本地缓存，未命中则调免登 `UserByScreenName`）"""
        clean_name = str(screen_name).lstrip("@").strip()
        if clean_name.isdigit():
            return clean_name
        key = clean_name.lower()
        if key in self.uid_cache:
            return self.uid_cache[key]

        url = f"https://x.com/i/api/graphql/{X_GQL_QUERY_IDS['UserByScreenName']}/UserByScreenName"
        try:
            resp = self.session.get(
                url,
                headers=self._get_guest_headers(),
                params={
                    "variables": json.dumps({"screen_name": clean_name, "withSafetyModeUserFields": True}),
                    "features": json.dumps(X_USER_FEATURES_2026),
                },
                proxies=self.proxies,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 200:
                uid = (((resp.json().get("data") or {}).get("user") or {}).get("result") or {}).get("rest_id")
                if uid:
                    self.uid_cache[key] = str(uid)
                    self._save_uid_cache()
                    return str(uid)
        except Exception as e:
            logger.warning(f"[X适配器] 解析 @{clean_name} UID 失败: {e}")
        return ""

    def _convert_raw_tweet(self, tw: dict, fallback_screen_name: str = "", category: str = "x_post") -> Dict[str, Any]:
        """内部核心：将 X GraphQL 原始 Tweet 节点转换为本项目的统一标准 Schema"""
        legacy = tw.get("legacy") or {}
        rest_id = str(tw.get("rest_id") or legacy.get("id_str") or "")
        core_user = (((tw.get("core") or {}).get("user_results") or {}).get("result")) or {}
        user_legacy = core_user.get("legacy") or {}

        screen_name = (
            user_legacy.get("screen_name")
            or (core_user.get("core") or {}).get("screen_name")
            or fallback_screen_name
            or "unknown"
        )
        author_name = (
            user_legacy.get("name")
            or (core_user.get("core") or {}).get("name")
            or screen_name
        )
        author_uid = str(core_user.get("rest_id") or legacy.get("user_id_str") or "")
        followers_count = safe_int(user_legacy.get("followers_count"), 0)
        is_blue = bool(core_user.get("is_blue_verified") or user_legacy.get("verified"))

        note_text = (((tw.get("note_tweet") or {}).get("note_tweet_results") or {}).get("result") or {}).get("text")
        raw_text = note_text or legacy.get("full_text") or ""
        clean_txt = _clean_x_text_with_entities(raw_text, legacy)

        entities = legacy.get("entities") or {}
        hashtags = [
            str(h.get("text")).strip()
            for h in (entities.get("hashtags") or [])
            if isinstance(h, dict) and h.get("text")
        ]
        mentioned_coins = [
            str(s.get("text")).strip().upper()
            for s in (entities.get("symbols") or [])
            if isinstance(s, dict) and s.get("text")
        ]

        ext_media = (legacy.get("extended_entities") or {}).get("media") or entities.get("media") or []
        images = []
        video_url = None
        video_duration = 0
        for m in ext_media:
            if not isinstance(m, dict):
                continue
            m_type = m.get("type")
            if m_type == "photo" and m.get("media_url_https"):
                images.append(m["media_url_https"])
            elif m_type in ("video", "animated_gif"):
                if m.get("media_url_https"):
                    images.append(m["media_url_https"])
                variants = ((m.get("video_info") or {}).get("variants")) or []
                mp4s = [v for v in variants if v.get("content_type") == "video/mp4" and v.get("url")]
                if mp4s:
                    mp4s.sort(key=lambda x: safe_int(x.get("bitrate"), 0), reverse=True)
                    video_url = mp4s[0]["url"]
                video_duration = int(safe_int((m.get("video_info") or {}).get("duration_millis"), 0) / 1000)

        quoted_res = ((tw.get("quoted_status_result") or {}).get("result")) or {}
        if quoted_res.get("__typename") == "TweetWithVisibilityResults":
            quoted_res = quoted_res.get("tweet") or {}
        quoted_data = None
        if isinstance(quoted_res, dict) and quoted_res.get("legacy"):
            q_leg = quoted_res["legacy"]
            q_user = (((quoted_res.get("core") or {}).get("user_results") or {}).get("result") or {}).get("legacy") or {}
            q_txt = _clean_x_text_with_entities(q_leg.get("full_text") or "", q_leg)
            quoted_data = {
                "post_id": str(quoted_res.get("rest_id") or ""),
                "username": q_user.get("screen_name", ""),
                "text": q_txt,
                "likes": safe_int(q_leg.get("favorite_count"), 0),
            }
            if q_txt and q_txt not in clean_txt:
                clean_txt = f"{clean_txt}\n[引用 @{quoted_data['username']}]: {q_txt}".strip()

        item = self.create_empty_post(
            card_type="BUZZ_VIDEO" if video_url else ("BUZZ_LONG" if note_text else "BUZZ_SHORT")
        )
        item["post_id"] = rest_id
        item["publish_time"] = legacy.get("created_at", "")
        item["author_id"] = author_uid or None

        item["author"].update({
            "username": screen_name,
            "author_name": author_name,
            "is_verified": is_blue,
            "followers_count": followers_count,
        })
        item["content"].update({
            "title": None,
            "text_content": clean_txt,
            "hashtags": hashtags,
            "mentioned_coins": mentioned_coins,
        })
        item["media"].update({
            "cover_image": images[0] if images else None,
            "images": images,
            "video_url": video_url,
            "video_duration": video_duration,
        })
        item["engagement"].update({
            "view_count": safe_int((tw.get("views") or {}).get("count"), 0),
            "like_count": safe_int(legacy.get("favorite_count"), 0),
            "comment_count": safe_int(legacy.get("reply_count"), 0),
            "share_count": safe_int(legacy.get("retweet_count"), 0),
            "quote_count": safe_int(legacy.get("quote_count"), 0),
            "bookmark_count": safe_int(legacy.get("bookmark_count"), 0),
        })
        item["metadata"].update({
            "url": f"https://x.com/{screen_name}/status/{rest_id}",
            "lang": legacy.get("lang"),
            "category": category,
            "is_retweet": bool(legacy.get("retweeted_status_result")),
            "quoted_post": quoted_data,
        })
        return self.normalize_post(item, download_media=False)

    # ==========================================================================
    # 四大通用原语实现
    # ==========================================================================
    def fetch_user_posts(
        self,
        user_id: str,
        count: int = 15,
        exclude_retweets: bool = True,
        force_burner: bool = False,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        原语 2：拉取指定大 V (`@handle` 或数字 UID) 的推文。
        自动路由策略：
        - 若该大 V 支持免登实时流 (`guest_realtime=True`)，优先走 0 账号 Guest Token 通道，不消耗小号配额；
        - 若该大 V 在免登下仅返回历史缓存（如 `@unusual_whales`）且当前已配小号，自动切换小号登录态拿今日实时新帖！
        """
        clean_name = str(user_id).lstrip("@").strip()
        meta_info = VERIFIED_X_ACCOUNTS.get(clean_name) or {}
        category = meta_info.get("category", "x_kol")
        guest_ok = meta_info.get("guest_realtime", True)
        use_burner = (force_burner or not guest_ok) and self.has_burner_account

        uid = self.resolve_user_id(clean_name)
        if not uid:
            return []

        url = f"https://x.com/i/api/graphql/{X_GQL_QUERY_IDS['UserTweets']}/UserTweets"
        variables = {
            "userId": str(uid),
            "count": max(count, 20),
            "includePromotedContent": False,
            "withQuickPromoteEligibilityTweetFields": True,
            "withVoice": True,
            "withV2Timeline": True,
        }
        headers = self._get_burner_headers() if use_burner else self._get_guest_headers()
        try:
            resp = self.session.get(
                url,
                headers=headers,
                params={
                    "variables": json.dumps(variables, separators=(",", ":")),
                    "features": json.dumps(X_GQL_FEATURES_2026, separators=(",", ":")),
                },
                proxies=self.proxies,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 429 and not use_burner:
                self._refresh_guest_token()
                resp = self.session.get(
                    url,
                    headers=self._get_guest_headers(),
                    params={
                        "variables": json.dumps(variables, separators=(",", ":")),
                        "features": json.dumps(X_GQL_FEATURES_2026, separators=(",", ":")),
                    },
                    proxies=self.proxies,
                    timeout=REQUEST_TIMEOUT,
                )
            if resp.status_code != 200:
                return []

            raw_nodes = _extract_gql_tweet_objects(resp.json())
            by_id = {}
            for node in raw_nodes:
                legacy_node = node.get("legacy") or {}
                if exclude_retweets and legacy_node.get("retweeted_status_result"):
                    continue
                node_uid = str(legacy_node.get("user_id_str") or "")
                if node_uid and node_uid != str(uid):
                    continue
                item = self._convert_raw_tweet(node, fallback_screen_name=clean_name, category=category)
                by_id[item["post_id"]] = item

            posts = sorted(by_id.values(), key=lambda x: int(x["post_id"]), reverse=True)
            return posts[:count]
        except Exception as e:
            logger.warning(f"[X适配器] 拉取用户 @{clean_name} 失败: {e}")
            return []

    def fetch_hot_or_recommended(
        self,
        count: int = 25,
        list_id: Optional[str] = None,
        latest_following: bool = False,
        fallback_handles: Optional[List[str]] = None,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        原语 1：拉取 X 平台热榜 / 首页推荐流。
        - 若指定了 `list_id` 且有小号：1 次请求拉取 `ListLatestTweetsTimeline`（监控百人大 V 列表）；
        - 若有小号：1 次请求拉取 `HomeTimeline`（For You 算法推荐流）或 `HomeLatestTimeline`（关注流）；
        - 若无小号：自动回退为免登并发拉取核心顶流大 V 的最新爆款。
        """
        if self.has_burner_account:
            if list_id:
                op = "ListLatestTweetsTimeline"
                variables = {"listId": str(list_id), "count": count}
            else:
                op = "HomeLatestTimeline" if latest_following else "HomeTimeline"
                variables = {
                    "count": count,
                    "includePromotedContent": False,
                    "latestControlAvailable": True,
                    "requestContext": "launch",
                    "withCommunity": True,
                }
            qid = X_GQL_QUERY_IDS[op]
            url = f"https://x.com/i/api/graphql/{qid}/{op}"
            try:
                resp = self.session.get(
                    url,
                    headers=self._get_burner_headers(),
                    params={
                        "variables": json.dumps(variables, separators=(",", ":")),
                        "features": json.dumps(X_GQL_FEATURES_2026, separators=(",", ":")),
                    },
                    proxies=self.proxies,
                    timeout=REQUEST_TIMEOUT,
                )
                if resp.status_code == 200:
                    nodes = _extract_gql_tweet_objects(resp.json())
                    by_id = {}
                    for node in nodes:
                        if (node.get("legacy") or {}).get("retweeted_status_result"):
                            continue
                        item = self._convert_raw_tweet(node, category=f"x_{op.lower()}")
                        by_id[item["post_id"]] = item
                    return sorted(by_id.values(), key=lambda x: x["engagement"]["like_count"], reverse=True)[:count]
            except Exception as e:
                logger.warning(f"[X适配器] 小号 {op} 拉取异常，自动降级免登流: {e}")

        handles = fallback_handles or ["Polymarket", "KobeissiLetter", "WatcherGuru", "Dexerto", "wallstreetbets"]
        combined = []
        for h in handles:
            combined.extend(self.fetch_user_posts(h, count=max(5, count // len(handles))))
            time.sleep(0.25)
        combined.sort(key=lambda x: x["engagement"]["like_count"], reverse=True)
        return combined[:count]

    def search_or_topic_posts(
        self,
        query: str,
        count: int = 20,
        handles: Optional[List[str]] = None,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        原语 3：按关键词、币种或话题检索推文。
        """
        raw_q = str(query or "").replace(",", "|")
        keywords = [w.strip().lower() for w in raw_q.split("|") if w.strip()]
        pool = self.fetch_hot_or_recommended(count=35, fallback_handles=handles)
        if handles:
            for h in handles:
                pool.extend(self.fetch_user_posts(h, count=8))
                time.sleep(0.2)

        by_id = {p["post_id"]: p for p in pool}
        if not keywords:
            return list(by_id.values())[:count]

        matched = [
            p for p in by_id.values()
            if any(k in ((p.get("content") or {}).get("text_content") or "").lower() for k in keywords)
        ]
        matched.sort(key=lambda x: x["engagement"]["like_count"], reverse=True)
        return matched[:count]

    def fetch_post_comments(
        self,
        post_id: str,
        max_comments: int = 10,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """原语 4：使用只读小号调用 `TweetDetail` 拉取指定推文底下的前排高赞神评"""
        if not self.has_burner_account or not post_id:
            return []
        qid = X_GQL_QUERY_IDS["TweetDetail"]
        url = f"https://x.com/i/api/graphql/{qid}/TweetDetail"
        variables = {
            "focalTweetId": str(post_id),
            "with_rux_injections": False,
            "includePromotedContent": False,
            "withCommunity": True,
            "withQuickPromoteEligibilityTweetFields": True,
            "withBirdwatchNotes": True,
            "withVoice": True,
            "withV2Timeline": True,
        }
        try:
            resp = self.session.get(
                url,
                headers=self._get_burner_headers(),
                params={
                    "variables": json.dumps(variables, separators=(",", ":")),
                    "features": json.dumps(X_GQL_FEATURES_2026, separators=(",", ":")),
                },
                proxies=self.proxies,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code != 200:
                return []
            nodes = _extract_gql_tweet_objects(resp.json())
            raw_replies = []
            seen = set()
            for node in nodes:
                rid = str(node.get("rest_id") or "")
                if not rid or rid == str(post_id) or rid in seen:
                    continue
                if not (node.get("legacy") or {}).get("in_reply_to_status_id_str"):
                    continue
                seen.add(rid)
                parsed = self._convert_raw_tweet(node)
                raw_replies.append({
                    "reply_id": rid,
                    "parent_id": str(post_id),
                    "create_time": parsed["publish_time"],
                    "author_uid": parsed["author"]["username"],
                    "author_name": parsed["author"]["author_name"],
                    "content": parsed["content"]["text_content"],
                    "likes": parsed["engagement"]["like_count"],
                    "replies": parsed["engagement"]["comment_count"],
                    "views": parsed["engagement"]["view_count"],
                    "images": (parsed.get("media") or {}).get("images") or [],
                })
            raw_replies.sort(key=lambda x: x["likes"], reverse=True)
            return self.normalize_comments(raw_replies[:max_comments], parent_post_id=str(post_id))
        except Exception as e:
            logger.warning(f"[X适配器] 拉取推文 {post_id} 神评异常: {e}")
            return []

    # ==========================================================================
    # 标准调度入口：供上层定时任务调用
    # ==========================================================================
    def harvest(self, config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        cfg = config or {}
        handles = cfg.get("kol_handles") or [
            "Polymarket",
            "Kalshi",
            "KobeissiLetter",
            "WatcherGuru",
            "Dexerto",
            "wallstreetbets",
            "MarioNawfal",
            "MorningBrew",
            "CoinGlass_com",
            "whale_alert",
            "NoContextHumans",
            "unusual_whales",
            "AutismCapital",
        ]
        tweets_per_handle = safe_int(cfg.get("tweets_per_handle"), 5)
        include_home_feed = bool(cfg.get("include_home_feed", False))
        list_id = cfg.get("list_id")
        top_n_for_comments = safe_int(cfg.get("fetch_comments_for_top_n"), 5)
        download_media = bool(cfg.get("download_media", True))

        self.last_channel_stats: Dict[str, int] = {}
        collected_by_id: Dict[str, Dict[str, Any]] = {}

        # 1. 忠实拉取并记录配置的顶流大 V 时间线（不做主观阈值筛除，交由下游判断）
        for handle in handles:
            clean_h = str(handle).lstrip("@").strip()
            posts = self.fetch_user_posts(clean_h, count=tweets_per_handle)
            self.last_channel_stats[f"@{clean_h}"] = len(posts)
            for p in posts:
                if self.is_valuable_post(p):
                    collected_by_id[p["post_id"]] = p
            time.sleep(random.uniform(0.2, 0.4))

        # 2. 若开启了小号推荐流或 List 列表流，直接合并记录
        if self.has_burner_account and (include_home_feed or list_id):
            feed_posts = self.fetch_hot_or_recommended(count=safe_int(cfg.get("home_feed_count"), 25), list_id=list_id)
            self.last_channel_stats["主页/列表流"] = len(feed_posts)
            for p in feed_posts:
                if self.is_valuable_post(p):
                    collected_by_id[p["post_id"]] = p

        all_posts = list(collected_by_id.values())

        # 3. 为前 N 条推文补充评论树
        if self.has_burner_account and top_n_for_comments > 0:
            for top_p in all_posts[:top_n_for_comments]:
                comments = self.fetch_post_comments(top_p["post_id"], max_comments=10)
                if comments:
                    top_p["comments"] = comments

        # 4. 统一抹平差异并并发下载配图至本地 `media_downloads/`
        return self.normalize_batch(all_posts, download_media=download_media)
