# -*- coding: utf-8 -*-
"""
X (Twitter) 平台专用采集与差异抹平适配器 (XHarvester)

================================================================================
【运维速查手册 (Ops Quick Guide) —— 日常巡检与修改必读】
================================================================================
1. 是否使用登录账号 & 账号安全隔离铁律？
   - **专用爬虫小号会话隔离**：默认绑定 `config/config.json` 中的 `x_xiaohao_browser_session_dir`
     （即 `W:\\temp\\x_xiaohao`），**严禁触碰主号 `x_myself` (`W:\\temp\\x_myself`)**！
   - 若外部误传 `x_myself` 路径，初始化守卫会自动拦截并重定向回 `x_xiaohao` 同时发出健康预警。
   - 若 `x_xiaohao` 未登录或登录态失效，系统会立即抛出 `CRITICAL` 健康告警 (`X_BURNER_AUTH_MISSING` / `X_SESSION_LOGGED_OUT`)，绝不静默伪装正常。

2. 两大核心采集能力（热门榜单 + 指定关键词搜索）：
   - **能力 A：热门榜单与广谱爆款 (`harvest` / `fetch_hot_or_recommended` / `fetch_trending_topics`)**
     ① 小号 `HomeTimeline` 算法推荐流；
     ② X 官方全球/美国实时热搜榜 (`/1.1/trends/place.json`, WOEID `23424977` & `1`) -> 自动穿透抓取热搜词下的高赞爆款帖；
     ③ 核心垂直大 V 实时时间线 (`UserTweets` 登录态跳过置顶旧帖)。
   - **能力 B：指定关键词精准搜索 (`search_or_topic_posts`)**
     ① 主通道：复用 `x_xiaohao` 持久化无头浏览器访问 X 搜索页 (`f=top` 热门 + `f=live` 最新)，直接拦截官方 `SearchTimeline` GraphQL 响应包，几秒内拿齐高赞真实推文；
     ② 兜底补充通道：仅当小号未就绪或主通道结果不足时，启用 Yahoo/Bing `site:x.com` 索引 + `TweetDetail` 补齐，并明确记录降级告警。

3. 异常感知与数据质量自检：
   - 全程监控 HTTP `401/403`（登录掉线）、`/i/flow/login` 重定向、`/account/access` 风控锁号、`400/404`（GraphQL `queryId` 漂移）、`429` 限流、评论接口失灵及图片下载失败，统一汇总至 `get_health_summary()`。
================================================================================
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import quote, unquote

import requests

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    cffi_requests = None

from common.common_utils import (
    PLATFORM_X,
    clean_post_text,
    get_runtime_file_path,
    normalize_publish_timestamp,
    read_json,
    resolve_session_dir,
    safe_int,
    save_json,
)
from platforms.external_link_extractor import (
    enrich_posts_with_external_articles,
    is_fetchable_external_article_url,
)
from platforms.harvesters.base_harvester import BasePlatformHarvester
from platforms.x_api import (
    extract_x_cookies_from_user_data_dir,
    invalidate_burner_cookie_cache,
)

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


def _extract_gql_tweet_objects(data, skip_pinned: bool = False) -> list:
    """提取 GraphQL 响应树中的 Tweet 节点（自动解包 TweetWithVisibilityResults，跳过广告容器与可选置顶旧帖节点）"""
    found = []

    def walk(obj):
        if isinstance(obj, dict):
            # 跳过 X 官方广告容器节点 (TimelineTimelineItem.itemContent.promotedMetadata)
            if "promotedMetadata" in obj:
                return
            tname = obj.get("__typename")
            if skip_pinned and (
                obj.get("type") == "TimelinePinEntry"
                or tname == "TimelinePinEntry"
                or str(obj.get("entryId") or "").startswith("pinned")
            ):
                return
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

        default_xiaohao_dir = resolve_session_dir("x_xiaohao_browser_session_dir", "x_xiaohao")
        effective_dir = default_xiaohao_dir if burner_user_data_dir is None else str(burner_user_data_dir).strip()
        if effective_dir and "x_myself" in os.path.normcase(os.path.abspath(effective_dir)):
            self.record_health_issue(
                code="X_MYSELF_ISOLATION_GUARD",
                message=(
                    f"检测到爬虫尝试使用主号目录 `{effective_dir}`，已强制拦截并重定向至专用爬虫小号目录 `{default_xiaohao_dir}`"
                ),
                level="warning",
                channel="init",
                persistent=True,
            )
            effective_dir = default_xiaohao_dir

        self.burner_user_data_dir = effective_dir
        if not burner_cookies and self.burner_user_data_dir and os.path.exists(self.burner_user_data_dir):
            burner_cookies = extract_x_cookies_from_user_data_dir(self.burner_user_data_dir)
        self.burner_cookies = burner_cookies or ""
        self.cookie_dict = _parse_cookie_dict(self.burner_cookies)

        if not self.has_burner_account:
            self.record_health_issue(
                code="X_BURNER_AUTH_MISSING",
                message=(
                    f"专用爬虫小号目录 `{self.burner_user_data_dir or '未配置'}` 未检测到有效 X 登录态 (auth_token/ct0)！"
                    f"原生搜索、热搜榜与神评拉取将受限，请在 `x_xiaohao` 浏览器窗口重新登录小号。"
                ),
                level="critical",
                channel="auth",
                persistent=True,
            )

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
            return f"🚨小号未就绪(降级免登 Guest Token, dir={self.burner_user_data_dir or '未配置'})"
        session_dir_name = os.path.basename(os.path.normpath(self.burner_user_data_dir)) if self.burner_user_data_dir else "直接Cookie注入"
        auth_tail = (self.cookie_dict.get("auth_token") or "")[-6:]
        return f"专用小号在线 (UID={self.burner_account_uid}, Session={session_dir_name}, token尾号=...{auth_tail})"

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

    def fetch_user_profile_detail(self, screen_name: str) -> Optional[Dict[str, Any]]:
        """
        拉取指定 `@screen_name` 的精确账号画像与双向关注状态：
        优先走当前小号登录态请求头（可准确返回 `is_following` 与 `is_followed_by`），失败时自动回退免登请求头。
        """
        clean_name = str(screen_name or "").lstrip("@").strip()
        if not clean_name:
            return None
        url = f"https://x.com/i/api/graphql/{X_GQL_QUERY_IDS['UserByScreenName']}/UserByScreenName"
        req_params = {
            "variables": json.dumps({"screen_name": clean_name, "withSafetyModeUserFields": True}, separators=(",", ":")),
            "features": json.dumps(X_USER_FEATURES_2026, separators=(",", ":")),
        }
        headers_chain = (
            [self._get_burner_headers(), self._get_guest_headers()]
            if self.has_burner_account
            else [self._get_guest_headers()]
        )
        for hdrs in headers_chain:
            try:
                resp = self.session.get(
                    url,
                    headers=hdrs,
                    params=req_params,
                    proxies=self.proxies,
                    timeout=REQUEST_TIMEOUT,
                )
                if resp.status_code != 200:
                    continue
                res = (((resp.json().get("data") or {}).get("user") or {}).get("result")) or {}
                if res.get("__typename") == "UserUnavailable" or not res.get("rest_id"):
                    continue
                uleg = res.get("legacy") or {}
                ucore = res.get("core") or {}
                rel_counts = res.get("relationship_counts") or {}
                rel = res.get("relationship_perspectives") or {}
                bio_obj = res.get("profile_bio") or {}
                tw_counts = res.get("tweet_counts") or {}
                uid = str(res.get("rest_id") or "")
                uname = str(ucore.get("screen_name") or uleg.get("screen_name") or clean_name).strip()
                if uid and uname:
                    self.uid_cache[uname.lower()] = uid
                    self._save_uid_cache()
                return {
                    "user_id": uid,
                    "screen_name": uname,
                    "name": str(ucore.get("name") or uleg.get("name") or uname).strip(),
                    "is_blue_verified": bool(res.get("is_blue_verified")),
                    "is_verified": bool(res.get("is_blue_verified") or (res.get("verification") or {}).get("verified") or uleg.get("verified")),
                    "followers_count": safe_int(rel_counts.get("followers") if "followers" in rel_counts else uleg.get("followers_count"), 0),
                    "following_count": safe_int(rel_counts.get("following") if "following" in rel_counts else uleg.get("friends_count"), 0),
                    "statuses_count": safe_int(tw_counts.get("tweets") if "tweets" in tw_counts else uleg.get("statuses_count"), 0),
                    "description": clean_post_text(str(bio_obj.get("description") or uleg.get("description") or "")),
                    "is_following": bool(rel.get("following") if "following" in rel else uleg.get("following")),
                    "is_followed_by": bool(rel.get("followed_by") if "followed_by" in rel else uleg.get("followed_by")),
                    "can_dm": bool(uleg.get("can_dm")),
                }
            except Exception as e:
                logger.debug(f"[X适配器] 拉取用户画像 @{clean_name} 异常: {e}")
        return None

    def _convert_raw_tweet(self, tw: dict, fallback_screen_name: str = "", category: str = "x_post") -> Dict[str, Any]:
        """内部核心：将 X GraphQL 原始 Tweet 节点转换为本项目的统一标准 Schema"""
        legacy = tw.get("legacy") or {}
        rest_id = str(tw.get("rest_id") or legacy.get("id_str") or "")
        core_user = (((tw.get("core") or {}).get("user_results") or {}).get("result")) or {}
        user_legacy = core_user.get("legacy") or {}
        rel_counts = core_user.get("relationship_counts") or {}
        rel_persp = core_user.get("relationship_perspectives") or {}
        bio_obj = core_user.get("profile_bio") or {}
        tw_counts = core_user.get("tweet_counts") or {}

        screen_name = (
            (core_user.get("core") or {}).get("screen_name")
            or user_legacy.get("screen_name")
            or fallback_screen_name
            or "unknown"
        )
        author_name = (
            (core_user.get("core") or {}).get("name")
            or user_legacy.get("name")
            or screen_name
        )
        author_uid = str(core_user.get("rest_id") or legacy.get("user_id_str") or "")
        followers_count = safe_int(
            rel_counts.get("followers") if "followers" in rel_counts else user_legacy.get("followers_count"),
            0,
        )
        following_count = safe_int(
            rel_counts.get("following") if "following" in rel_counts else user_legacy.get("friends_count"),
            0,
        )
        statuses_count = safe_int(
            tw_counts.get("tweets") if "tweets" in tw_counts else user_legacy.get("statuses_count"),
            0,
        )
        is_blue_only = bool(core_user.get("is_blue_verified"))
        is_blue = bool(
            is_blue_only
            or (core_user.get("verification") or {}).get("verified")
            or user_legacy.get("verified")
        )
        author_profile_snapshot = {
            "user_id": author_uid,
            "screen_name": screen_name,
            "name": author_name,
            "is_blue_verified": is_blue_only,
            "is_verified": is_blue,
            "followers_count": followers_count,
            "following_count": following_count,
            "statuses_count": statuses_count,
            "description": clean_post_text(str(bio_obj.get("description") or user_legacy.get("description") or "")),
            "is_following": bool(rel_persp.get("following") if "following" in rel_persp else user_legacy.get("following")),
            "is_followed_by": bool(rel_persp.get("followed_by") if "followed_by" in rel_persp else user_legacy.get("followed_by")),
        }

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

        external_url = ""
        for u_obj in entities.get("urls") or []:
            if isinstance(u_obj, dict):
                exp_u = str(u_obj.get("expanded_url") or "").strip()
                if is_fetchable_external_article_url(exp_u):
                    external_url = exp_u
                    break

        ext_media = (legacy.get("extended_entities") or {}).get("media") or entities.get("media") or []
        images = []
        main_images = []
        video_url = None
        video_duration = 0
        for m in ext_media:
            if not isinstance(m, dict):
                continue
            m_type = m.get("type")
            if m_type == "photo" and m.get("media_url_https"):
                images.append(m["media_url_https"])
                main_images.append(m["media_url_https"])
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
            q_note = (((quoted_res.get("note_tweet") or {}).get("note_tweet_results") or {}).get("result") or {}).get("text")
            q_txt = _clean_x_text_with_entities(q_note or q_leg.get("full_text") or "", q_leg)
            if not external_url:
                for qu_obj in (q_leg.get("entities") or {}).get("urls") or []:
                    if isinstance(qu_obj, dict):
                        q_exp = str(qu_obj.get("expanded_url") or "").strip()
                        if is_fetchable_external_article_url(q_exp):
                            external_url = q_exp
                            break
            q_media = (q_leg.get("extended_entities") or {}).get("media") or (q_leg.get("entities") or {}).get("media") or []
            q_images = []
            q_has_video = False
            for qm in q_media:
                if not isinstance(qm, dict):
                    continue
                qm_type = qm.get("type")
                qm_img = qm.get("media_url_https")
                if qm_type == "photo" and qm_img:
                    q_images.append(qm_img)
                    if qm_img not in images:
                        images.append(qm_img)
                elif qm_type in ("video", "animated_gif"):
                    q_has_video = True
                    if qm_img and qm_img not in images:
                        images.append(qm_img)
                    q_variants = ((qm.get("video_info") or {}).get("variants")) or []
                    q_mp4s = [v for v in q_variants if v.get("content_type") == "video/mp4" and v.get("url")]
                    if q_mp4s and not video_url:
                        q_mp4s.sort(key=lambda x: safe_int(x.get("bitrate"), 0), reverse=True)
                        video_url = q_mp4s[0]["url"]
                    elif not video_url:
                        video_url = qm_img or "https://x.com/i/videos/quoted"
                    if not video_duration:
                        video_duration = max(1, int(safe_int((qm.get("video_info") or {}).get("duration_millis"), 0) / 1000))

            quoted_data = {
                "post_id": str(quoted_res.get("rest_id") or ""),
                "username": q_user.get("screen_name", ""),
                "text": q_txt,
                "likes": safe_int(q_leg.get("favorite_count"), 0),
                "images": q_images,
                "has_video": q_has_video,
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
        meta_payload: Dict[str, Any] = {
            "url": f"https://x.com/{screen_name}/status/{rest_id}",
            "lang": legacy.get("lang"),
            "category": category,
            "is_retweet": bool(legacy.get("retweeted_status_result")),
            "main_images": main_images,
            "quoted_post": quoted_data,
            "author_profile": author_profile_snapshot,
        }
        if external_url:
            meta_payload["external_url"] = external_url
        item["metadata"].update(meta_payload)
        return self.normalize_post(item, download_media=False)

    # ==========================================================================
    # 四大通用原语实现
    # ==========================================================================
    def fetch_user_posts(
        self,
        user_id: str,
        count: int = 15,
        exclude_retweets: bool = True,
        exclude_replies: bool = False,
        force_burner: bool = False,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        原语 2：拉取指定大 V (`@handle` 或数字 UID) 的推文。
        自动路由策略：
        - 若当前已配只读小号 (`has_burner_account=True`)，默认统一走小号登录态并跳过主页置顶旧帖 (`skip_pinned=True`)；
        - 若小号接口异常，明确记录健康降级预警后再平滑回退免登 Guest Token 通道。
        """
        clean_name = str(user_id).lstrip("@").strip()
        meta_info = VERIFIED_X_ACCOUNTS.get(clean_name) or {}
        category = meta_info.get("category", "x_kol")
        prefer_guest = bool(kwargs.get("prefer_guest", False)) and not force_burner
        use_burner = self.has_burner_account and not prefer_guest

        known_uid = str(kwargs.get("known_uid") or "").strip()
        uid = known_uid if known_uid.isdigit() else self.resolve_user_id(clean_name)
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
        req_params = {
            "variables": json.dumps(variables, separators=(",", ":")),
            "features": json.dumps(X_GQL_FEATURES_2026, separators=(",", ":")),
        }
        headers = self._get_burner_headers() if use_burner else self._get_guest_headers()
        try:
            resp = self.session.get(
                url,
                headers=headers,
                params=req_params,
                proxies=self.proxies,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code != 200 and use_burner:
                if resp.status_code in (401, 403):
                    invalidate_burner_cookie_cache(self.burner_user_data_dir)
                    self.record_health_issue(
                        code="X_USER_AUTH_DEGRADED",
                        message=f"X 小号拉取 UserTweets 返回 HTTP {resp.status_code}，已回退免登 Guest Token（请检查小号登录态）",
                        level="critical",
                        channel="UserTweets",
                    )
                elif resp.status_code in (400, 404):
                    self.record_health_issue(
                        code="X_USERTWEETS_GQL_DRIFT",
                        message=f"X UserTweets 接口返回 HTTP {resp.status_code}，疑似 GraphQL queryId 或 features 已过期",
                        level="critical",
                        channel="UserTweets",
                    )
                elif resp.status_code == 429:
                    self.record_health_issue(
                        code="X_USER_RATE_LIMITED",
                        message="X 小号拉取 UserTweets 触发 HTTP 429 限流，已临时回退免登 Guest Token",
                        level="warning",
                        channel="UserTweets",
                    )
                resp = self.session.get(
                    url,
                    headers=self._get_guest_headers(),
                    params=req_params,
                    proxies=self.proxies,
                    timeout=REQUEST_TIMEOUT,
                )
            elif resp.status_code == 429 and not use_burner:
                self._refresh_guest_token()
                resp = self.session.get(
                    url,
                    headers=self._get_guest_headers(),
                    params=req_params,
                    proxies=self.proxies,
                    timeout=REQUEST_TIMEOUT,
                )
            if resp.status_code != 200:
                if resp.status_code in (400, 404):
                    self.record_health_issue(
                        code="X_USERTWEETS_GQL_DRIFT",
                        message=f"X UserTweets 免登接口返回 HTTP {resp.status_code}，疑似 queryId 漂移",
                        level="critical",
                        channel="UserTweets",
                    )
                return []

            payload = resp.json()
            raw_nodes = _extract_gql_tweet_objects(payload, skip_pinned=True)
            if not raw_nodes and use_burner and isinstance(payload, dict) and payload.get("errors"):
                err_codes = {safe_int(e.get("code"), 0) for e in payload.get("errors") if isinstance(e, dict)}
                if 326 in err_codes:
                    setattr(self, "_burner_locked_326", True)
                    self.record_health_issue(
                        code="X_ACCOUNT_LOCKED_326",
                        message="X 登录态触发安全验证临时锁定 (code=326, https://x.com/account/access)，请打开对应浏览器目录解锁账号",
                        level="critical",
                        channel="UserTweets",
                    )
                    guest_resp = self.session.get(
                        url,
                        headers=self._get_guest_headers(),
                        params=req_params,
                        proxies=self.proxies,
                        timeout=REQUEST_TIMEOUT,
                    )
                    if guest_resp.status_code == 200:
                        raw_nodes = _extract_gql_tweet_objects(guest_resp.json(), skip_pinned=True)
            by_id = {}
            for node in raw_nodes:
                legacy_node = node.get("legacy") or {}
                if exclude_retweets and legacy_node.get("retweeted_status_result"):
                    continue
                if exclude_replies and legacy_node.get("in_reply_to_status_id_str"):
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

    def fetch_trending_topics(
        self,
        woeid_list: Sequence[int] = (23424977, 1),
        limit: int = 8,
    ) -> List[Dict[str, Any]]:
        """
        拉取 X 官方实时热搜榜 (`/1.1/trends/place.json`)：
        默认合并美国热搜 (`WOEID=23424977`) 与全球热搜 (`WOEID=1`)，自动过滤广告推广词。
        """
        if not self.has_burner_account:
            return []

        merged: List[Dict[str, Any]] = []
        seen_names = set()
        got_200 = False

        for woeid in woeid_list:
            try:
                resp = self.session.get(
                    "https://api.x.com/1.1/trends/place.json",
                    headers=self._get_burner_headers(),
                    params={"id": int(woeid)},
                    proxies=self.proxies,
                    timeout=REQUEST_TIMEOUT,
                )
                if resp.status_code in (401, 403):
                    invalidate_burner_cookie_cache(self.burner_user_data_dir)
                    self.record_health_issue(
                        code="X_TRENDS_AUTH_EXPIRED",
                        message=f"X 热搜榜接口 (/1.1/trends/place.json) 返回 HTTP {resp.status_code}，小号登录态疑似过期",
                        level="critical",
                        channel="trends",
                    )
                    break
                if resp.status_code == 429:
                    self.record_health_issue(
                        code="X_TRENDS_RATE_LIMITED",
                        message=f"X 热搜榜接口触发 HTTP 429 限流 (woeid={woeid})",
                        level="warning",
                        channel="trends",
                    )
                    continue
                if resp.status_code != 200:
                    self.record_health_issue(
                        code="X_TRENDS_HTTP_ERROR",
                        message=f"X 热搜榜接口返回 HTTP {resp.status_code} (woeid={woeid})",
                        level="warning",
                        channel="trends",
                    )
                    continue

                got_200 = True
                payload = resp.json()
                blocks = payload if isinstance(payload, list) else []
                for blk in blocks:
                    if not isinstance(blk, dict):
                        continue
                    for rank_idx, tr in enumerate(blk.get("trends") or []):
                        if not isinstance(tr, dict) or tr.get("promoted_content"):
                            continue
                        name = str(tr.get("name") or "").strip()
                        if not name or len(name) < 2:
                            continue
                        key = name.lower()
                        if key in seen_names:
                            continue
                        seen_names.add(key)
                        merged.append({
                            "name": name,
                            "tweet_volume": safe_int(tr.get("tweet_volume"), 0),
                            "woeid": int(woeid),
                            "rank": rank_idx + 1,
                        })
            except Exception as e:
                self.record_health_issue(
                    code="X_TRENDS_EXCEPTION",
                    message=f"拉取 X 热搜榜异常 (woeid={woeid}): {e}",
                    level="warning",
                    channel="trends",
                )

        if got_200 and not merged:
            self.record_health_issue(
                code="X_TRENDS_EMPTY_SCHEMA",
                message="X 热搜榜接口返回 HTTP 200 但解析出 0 个热搜词，疑似返回结构变更",
                level="warning",
                channel="trends",
            )

        merged.sort(key=lambda x: (1 if x["tweet_volume"] > 0 else 0, x["tweet_volume"], -x["rank"]), reverse=True)
        return merged[: max(1, limit)]

    def _search_x_native_via_xiaohao(
        self,
        keywords: Sequence[str],
        per_kw_count: int = 15,
        hours_back: int = 72,
        include_latest: bool = True,
        exclude_retweets: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        使用专用爬虫小号会话 (`x_xiaohao_browser_session_dir`) 无头启动持久化浏览器，
        访问 X 原生搜索页 (`f=top` 热门榜 + 可选 `f=live` 实时流) 并拦截官方 `SearchTimeline` 响应包。
        内置登录重定向 (`/i/flow/login`)、风控解锁挑战 (`/account/access`)、GraphQL 400/404 漂移与空解析自检。
        """
        if not self.has_burner_account or not self.burner_user_data_dir or not os.path.isdir(self.burner_user_data_dir):
            return []

        clean_kws: List[str] = []
        seen_kw = set()
        for raw_k in keywords or []:
            k = str(raw_k or "").strip()
            if k and k.lower() not in seen_kw:
                seen_kw.add(k.lower())
                clean_kws.append(k)
        if not clean_kws:
            return []

        min_ts = int(time.time() - max(1, hours_back) * 3600)
        collected_by_id: Dict[str, Dict[str, Any]] = {}
        current_kw_items: Dict[str, Dict[str, Any]] = {}
        auth_failed = False

        try:
            from playwright.sync_api import sync_playwright
            from platforms.playwright_base import _launch_persistent
        except Exception as e:
            self.record_health_issue(
                code="X_PLAYWRIGHT_UNAVAILABLE",
                message=f"无法加载 Playwright 执行 X 原生搜索: {e}",
                level="critical",
                channel="x_native_search",
            )
            return []

        proxy_server = self.proxies.get("http") if isinstance(self.proxies, dict) else None

        try:
            with sync_playwright() as p:
                context = _launch_persistent(
                    p,
                    user_data_dir=self.burner_user_data_dir,
                    args=["--disable-blink-features=AutomationControlled"],
                    debug=False,
                    proxy=proxy_server,
                )
                try:
                    page = context.pages[0] if context.pages else context.new_page()

                    def _on_response(resp) -> None:
                        nonlocal auth_failed
                        r_url = resp.url or ""
                        if "SearchTimeline" not in r_url:
                            return
                        status = resp.status
                        if status in (401, 403):
                            auth_failed = True
                            invalidate_burner_cookie_cache(self.burner_user_data_dir)
                            self.record_health_issue(
                                code="X_SEARCH_AUTH_EXPIRED",
                                message=f"X SearchTimeline 返回 HTTP {status}，小号登录态已失效！",
                                level="critical",
                                channel="x_native_search",
                            )
                            return
                        if status == 429:
                            self.record_health_issue(
                                code="X_SEARCH_RATE_LIMITED",
                                message="X SearchTimeline 触发 HTTP 429 频率限制",
                                level="warning",
                                channel="x_native_search",
                            )
                            return
                        if status in (400, 404):
                            self.record_health_issue(
                                code="X_SEARCH_GQL_DRIFT",
                                message=f"X SearchTimeline 返回 HTTP {status}，疑似搜索接口签名或参数异常",
                                level="critical",
                                channel="x_native_search",
                            )
                            return
                        if status != 200:
                            return
                        try:
                            data = resp.json()
                        except Exception:
                            return
                        nodes = _extract_gql_tweet_objects(data, skip_pinned=True)
                        if not nodes and isinstance(data, dict) and data.get("errors"):
                            self.record_health_issue(
                                code="X_SEARCH_GQL_ERRORS",
                                message=f"X SearchTimeline 返回 GraphQL errors: {str(data.get('errors'))[:160]}",
                                level="warning",
                                channel="x_native_search",
                            )
                            return
                        for node in nodes:
                            legacy_node = node.get("legacy") or {}
                            if exclude_retweets and legacy_node.get("retweeted_status_result"):
                                continue
                            parsed = self._convert_raw_tweet(node, category="x_search")
                            pid = str(parsed.get("post_id") or "")
                            if not pid or not self.is_valuable_post(parsed):
                                continue
                            if safe_int(parsed.get("publish_time"), 0) < min_ts:
                                continue
                            current_kw_items[pid] = parsed
                            collected_by_id[pid] = parsed

                    page.on("response", _on_response)

                    def _check_session_redirect() -> bool:
                        cur_u = (page.url or "").lower()
                        if "/i/flow/login" in cur_u or cur_u.rstrip("/").endswith("/login"):
                            invalidate_burner_cookie_cache(self.burner_user_data_dir)
                            self.record_health_issue(
                                code="X_SESSION_LOGGED_OUT",
                                message=f"X 小号访问搜索页被重定向至登录页 ({page.url})，登录态已掉线！请重新打开 x_xiaohao 登录。",
                                level="critical",
                                channel="x_native_search",
                            )
                            return True
                        if "/account/access" in cur_u or "arkose" in cur_u:
                            self.record_health_issue(
                                code="X_ACCOUNT_CHALLENGED",
                                message=f"X 小号触发安全解锁验证 ({page.url})！请人工打开 x_xiaohao 解锁。",
                                level="critical",
                                channel="x_native_search",
                            )
                            return True
                        return False

                    for kw in clean_kws[:6]:
                        if auth_failed:
                            break
                        current_kw_items = {}
                        top_url = f"https://x.com/search?q={quote(kw)}&src=typed_query&f=top"
                        try:
                            with page.expect_response(lambda r: "SearchTimeline" in r.url, timeout=9000):
                                page.goto(top_url, wait_until="domcontentloaded", timeout=15000)
                        except Exception:
                            pass
                        page.wait_for_timeout(300)
                        if _check_session_redirect():
                            auth_failed = True
                            break

                        # 若单关键词期望条数较多且首屏有结果，向下滚动一屏追加高赞推文
                        if per_kw_count > 12 and 4 <= len(current_kw_items) < per_kw_count:
                            try:
                                with page.expect_response(lambda r: "SearchTimeline" in r.url, timeout=3500):
                                    page.mouse.wheel(0, 2600)
                            except Exception:
                                pass
                            page.wait_for_timeout(200)

                        # 若 Top 榜近期新帖较少且开启了实时流补充，再看一眼 Latest 流
                        if include_latest and not auth_failed and len(current_kw_items) < max(6, per_kw_count // 2):
                            live_url = f"https://x.com/search?q={quote(kw)}&src=typed_query&f=live"
                            try:
                                with page.expect_response(lambda r: "SearchTimeline" in r.url, timeout=7500):
                                    page.goto(live_url, wait_until="domcontentloaded", timeout=15000)
                            except Exception:
                                pass
                            page.wait_for_timeout(250)
                            if _check_session_redirect():
                                auth_failed = True
                                break

                    # 顺手同步最新 ct0 / auth_token 到内存，保持后续 TweetDetail 评论请求新鲜
                    try:
                        fresh_cookies = context.cookies(["https://x.com", "https://twitter.com"])
                        fresh_str = "; ".join(f"{c['name']}={c['value']}" for c in fresh_cookies if c.get("name"))
                        fresh_dict = _parse_cookie_dict(fresh_str)
                        if fresh_dict.get("auth_token") and fresh_dict.get("ct0"):
                            self.burner_cookies = fresh_str
                            self.cookie_dict = fresh_dict
                    except Exception:
                        pass
                finally:
                    context.close()
        except Exception as e:
            self.record_health_issue(
                code="X_NATIVE_SEARCH_ERROR",
                message=f"X 专用小号无头浏览器原生搜索异常: {e}",
                level="warning",
                channel="x_native_search",
            )

        return list(collected_by_id.values())

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
        - 若指定了 `list_id` 且有小号：1 次请求拉取 `ListLatestTweetsTimeline`；
        - 若有小号：1 次请求拉取 `HomeTimeline`（For You 算法推荐流）或 `HomeLatestTimeline`（关注流）；
        - 若无小号或主通道异常：明确记录健康告警后再回退免登大 V 流。
        """
        if self.has_burner_account:
            if list_id:
                op = "ListLatestTweetsTimeline"
                variables = {"listId": str(list_id), "count": max(count, 25)}
            else:
                op = "HomeLatestTimeline" if latest_following else "HomeTimeline"
                variables = {
                    "count": max(count, 30),
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
                        if self.is_valuable_post(item):
                            by_id[item["post_id"]] = item
                    if not by_id:
                        self.record_health_issue(
                            code="X_HOME_EMPTY_PARSE",
                            message=f"X 小号 {op} 返回 HTTP 200 但解析有效推文为 0 条，疑似返回结构变化或推荐流异常",
                            level="warning",
                            channel=op,
                        )
                    else:
                        return sorted(by_id.values(), key=lambda x: x["engagement"]["like_count"], reverse=True)[:count]
                elif resp.status_code in (401, 403):
                    invalidate_burner_cookie_cache(self.burner_user_data_dir)
                    self.record_health_issue(
                        code="X_HOME_AUTH_EXPIRED",
                        message=f"X 小号 {op} 返回 HTTP {resp.status_code}，小号登录态已过期，已降级免登流！",
                        level="critical",
                        channel=op,
                    )
                elif resp.status_code in (400, 404):
                    self.record_health_issue(
                        code="X_HOME_GQL_DRIFT",
                        message=f"X 小号 {op} 返回 HTTP {resp.status_code}，疑似 GraphQL queryId ({qid}) 过期！",
                        level="critical",
                        channel=op,
                    )
                else:
                    self.record_health_issue(
                        code="X_HOME_HTTP_DEGRADED",
                        message=f"X 小号 {op} 返回 HTTP {resp.status_code}，已降级免登流",
                        level="warning",
                        channel=op,
                    )
            except Exception as e:
                self.record_health_issue(
                    code="X_HOME_EXCEPTION",
                    message=f"X 小号 {op} 请求异常，已降级免登流: {e}",
                    level="warning",
                    channel=op,
                )

        handles = fallback_handles or ["Polymarket", "KobeissiLetter", "WatcherGuru", "Dexerto", "wallstreetbets"]
        combined = []
        for h in handles:
            combined.extend(self.fetch_user_posts(h, count=max(5, count // len(handles))))
            time.sleep(0.25)
        combined.sort(key=lambda x: x["engagement"]["like_count"], reverse=True)
        return combined[:count]

    def _convert_fxtwitter_tweet(self, tw: Dict[str, Any], fallback_screen_name: str = "") -> Optional[Dict[str, Any]]:
        """免登兜底：将公开镜像 `api.fxtwitter.com` 的 tweet 对象转换为项目统一标准 Schema。"""
        if not isinstance(tw, dict) or not tw.get("id"):
            return None
        tid = str(tw["id"]).strip()
        author = tw.get("author") or {}
        screen_name = str(author.get("screen_name") or fallback_screen_name or "unknown").strip()
        author_name = str(author.get("name") or screen_name).strip()
        text = clean_post_text(str(tw.get("text") or ""))

        media_obj = tw.get("media") or {}
        photos = media_obj.get("photos") or []
        videos = media_obj.get("videos") or []
        main_images = [p["url"] for p in photos if isinstance(p, dict) and p.get("url")]
        images = list(main_images)
        video_url = videos[0].get("url") if videos and isinstance(videos[0], dict) else None
        video_duration = int(safe_int(videos[0].get("duration"), 1)) if video_url else 0

        quoted_data = None
        qt = tw.get("quote")
        if isinstance(qt, dict) and qt.get("id"):
            q_author = qt.get("author") or {}
            q_txt = clean_post_text(str(qt.get("text") or ""))
            q_media = qt.get("media") or {}
            q_photos = [p["url"] for p in (q_media.get("photos") or []) if isinstance(p, dict) and p.get("url")]
            q_vids = q_media.get("videos") or []
            for qp in q_photos:
                if qp not in images:
                    images.append(qp)
            if q_vids and not video_url:
                video_url = q_vids[0].get("url") or "https://x.com/i/videos/quoted"
                video_duration = max(1, int(safe_int(q_vids[0].get("duration"), 1)))
            quoted_data = {
                "post_id": str(qt.get("id")),
                "username": str(q_author.get("screen_name") or ""),
                "text": q_txt,
                "likes": safe_int(qt.get("likes"), 0),
                "images": q_photos,
                "has_video": bool(q_vids),
            }
            if q_txt and q_txt not in text:
                text = f"{text}\n[引用 @{quoted_data['username']}]: {q_txt}".strip()

        item = self.create_empty_post(card_type="BUZZ_VIDEO" if video_url else "BUZZ_SHORT")
        item["post_id"] = tid
        item["publish_time"] = safe_int(tw.get("created_timestamp"), 0) or normalize_publish_timestamp(tw.get("created_at"))
        item["author_id"] = str(author.get("id") or "") or None
        item["author"].update({
            "username": screen_name,
            "author_name": author_name,
            "is_verified": False,
            "followers_count": safe_int(author.get("followers"), 0),
        })
        item["content"].update({
            "title": None,
            "text_content": text,
            "hashtags": [],
            "mentioned_coins": [],
        })
        item["media"].update({
            "cover_image": images[0] if images else None,
            "images": images,
            "video_url": video_url,
            "video_duration": video_duration,
        })
        item["engagement"].update({
            "view_count": safe_int(tw.get("views"), 0),
            "like_count": safe_int(tw.get("likes"), 0),
            "comment_count": safe_int(tw.get("replies"), 0),
            "share_count": safe_int(tw.get("retweets"), 0),
            "quote_count": safe_int(tw.get("quotes"), 0),
            "bookmark_count": safe_int(tw.get("bookmarks"), 0),
        })
        item["metadata"].update({
            "url": str(tw.get("url") or f"https://x.com/{screen_name}/status/{tid}"),
            "lang": tw.get("lang"),
            "category": "x_search",
            "main_images": main_images,
            "quoted_post": quoted_data,
        })
        return self.normalize_post(item, download_media=False)

    def fetch_tweet_with_comments(
        self,
        tweet_id: str,
        screen_name: str = "",
        max_comments: int = 10,
    ) -> Optional[Dict[str, Any]]:
        """
        根据单条推文 ID 一次请求同时拿齐「完整主帖正文 + 全部高清原图 + 引用原帖 + 前排高赞评论」：
        - 若启用只读小号：单次请求 `TweetDetail` 同时解析主推文节点与前排评论节点；
        - 若无小号或临时受限：自动回退公开镜像 `api.fxtwitter.com` 免登拉取完整推文。
        """
        tid = str(tweet_id or "").strip()
        if not tid.isdigit():
            return None

        if self.has_burner_account:
            qid = X_GQL_QUERY_IDS["TweetDetail"]
            url = f"https://x.com/i/api/graphql/{qid}/TweetDetail"
            variables = {
                "focalTweetId": tid,
                "with_rux_injections": False,
                "includePromotedContent": False,
                "withCommunity": True,
                "withQuickPromoteEligibilityTweetFields": True,
                "withBirdwatchNotes": True,
                "withVoice": True,
                "withV2Timeline": True,
            }
            try:
                resp = requests.get(
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
                    focal_item: Optional[Dict[str, Any]] = None
                    raw_replies = []
                    comment_authors_map: Dict[str, Dict[str, Any]] = {}
                    seen = set()
                    for node in nodes:
                        rid = str(node.get("rest_id") or "")
                        if not rid:
                            continue
                        if rid == tid and focal_item is None:
                            focal_item = self._convert_raw_tweet(
                                node, fallback_screen_name=screen_name, category="x_search"
                            )
                            continue
                        if rid in seen or not (node.get("legacy") or {}).get("in_reply_to_status_id_str"):
                            continue
                        seen.add(rid)
                        parsed = self._convert_raw_tweet(node)
                        c_prof = ((parsed.get("metadata") or {}).get("platform_extra") or {}).get("author_profile")
                        if isinstance(c_prof, dict):
                            c_handle = str(c_prof.get("screen_name") or "").strip().lower()
                            if c_handle and c_handle != "unknown" and c_handle not in comment_authors_map:
                                prof_copy = dict(c_prof)
                                prof_copy["sample_comment"] = str((parsed.get("content") or {}).get("text_content") or "")[:240]
                                prof_copy["source_post_id"] = tid
                                comment_authors_map[c_handle] = prof_copy
                        raw_replies.append({
                            "reply_id": rid,
                            "parent_id": tid,
                            "create_time": parsed["publish_time"],
                            "author_uid": parsed["author"]["username"],
                            "author_name": parsed["author"]["author_name"],
                            "content": parsed["content"]["text_content"],
                            "likes": parsed["engagement"]["like_count"],
                            "replies": parsed["engagement"]["comment_count"],
                            "views": parsed["engagement"]["view_count"],
                            "images": (parsed.get("media") or {}).get("images") or [],
                        })
                    if focal_item is not None:
                        if comment_authors_map:
                            pextra = (focal_item.setdefault("metadata", {})).setdefault("platform_extra", {})
                            pextra["comment_authors"] = list(comment_authors_map.values())
                        if max_comments > 0 and raw_replies:
                            focal_username = str(
                                (focal_item.get("author") or {}).get("username")
                                or focal_item.get("author_id")
                                or ""
                            ).strip().lower()
                            author_self_replies = [
                                r for r in raw_replies
                                if focal_username and str(r.get("author_uid") or "").strip().lower() == focal_username
                            ]
                            other_replies = [
                                r for r in raw_replies
                                if not (focal_username and str(r.get("author_uid") or "").strip().lower() == focal_username)
                            ]
                            author_self_replies.sort(key=lambda x: safe_int(x.get("create_time"), 0))
                            other_replies.sort(key=lambda x: safe_int(x.get("likes"), 0), reverse=True)
                            merged_replies = author_self_replies + other_replies[:max_comments]
                            focal_item["comments"] = self.normalize_comments(
                                merged_replies, parent_post_id=tid
                            )
                        return focal_item
            except Exception as e:
                logger.debug(f"[X适配器] TweetDetail 单帖拉取 {tid} 回退 fxtwitter: {e}")

        # 0账号免登兜底：api.fxtwitter.com
        handle_seg = str(screen_name or "i").strip() or "i"
        try:
            rfx = requests.get(
                f"https://api.fxtwitter.com/{handle_seg}/status/{tid}",
                headers={"User-Agent": DEFAULT_UA},
                proxies=self.proxies,
                timeout=10,
            )
            if rfx.status_code == 200:
                tw_obj = (rfx.json() or {}).get("tweet")
                if isinstance(tw_obj, dict):
                    return self._convert_fxtwitter_tweet(tw_obj, fallback_screen_name=screen_name)
        except Exception as e:
            logger.debug(f"[X适配器] fxtwitter 拉取 {tid} 失败: {e}")
        return None

    def _discover_x_tweets_via_yahoo(self, keywords: List[str], per_kw_limit: int = 12) -> List[Tuple[str, str]]:
        """利用 Yahoo + Bing RSS 双搜索引擎发现全站公开推文 `(screen_name, tweet_id)`，无需接触 X 搜索风控。"""
        discovered: List[Tuple[str, str]] = []
        seen_tids = set()

        search_terms: List[str] = []
        seen_terms = set()
        for raw_kw in keywords[:6]:
            kw = str(raw_kw or "").strip()
            if not kw:
                continue
            if kw.lower() not in seen_terms:
                seen_terms.add(kw.lower())
                search_terms.append(kw)
            parts = [p for p in kw.split() if p]
            if len(parts) >= 3:
                for short_kw in (" ".join(parts[:2]), " ".join(parts[-2:])):
                    if short_kw.lower() not in seen_terms:
                        seen_terms.add(short_kw.lower())
                        search_terms.append(short_kw)

        def _fetch_text(target_url: str) -> str:
            if cffi_requests is not None:
                rs = cffi_requests.get(
                    target_url,
                    headers={"User-Agent": DEFAULT_UA},
                    impersonate="chrome",
                    proxies=self.proxies,
                    timeout=10,
                )
            else:
                rs = requests.get(
                    target_url,
                    headers={"User-Agent": DEFAULT_UA},
                    proxies=self.proxies,
                    timeout=10,
                )
            return unquote(rs.text or "") if rs.status_code == 200 else ""

        def _collect_pairs(text_blob: str, max_add: int) -> int:
            added = 0
            for handle, tid in re.findall(r"(?:x\.com|twitter\.com)/([A-Za-z0-9_]+)/status/(\d{18,22})", text_blob):
                if handle.lower() in ("i", "intent", "share", "home", "search"):
                    continue
                if tid in seen_tids:
                    continue
                seen_tids.add(tid)
                discovered.append((handle, tid))
                added += 1
                if added >= max_add:
                    break
            return added

        for kw in search_terms[:6]:
            hits_for_kw = 0
            for btf in ("&btf=w", ""):
                search_url = f"https://search.yahoo.com/search?p={quote('site:x.com ' + kw)}{btf}&n=30"
                try:
                    decoded = _fetch_text(search_url)
                    if decoded:
                        hits_for_kw += _collect_pairs(decoded, max(1, per_kw_limit - hits_for_kw))
                    if hits_for_kw >= max(4, per_kw_limit // 2):
                        break
                except Exception:
                    continue

            bing_url = f"https://www.bing.com/search?q={quote('site:x.com ' + kw)}&format=rss&count=30"
            try:
                decoded_bing = _fetch_text(bing_url)
                if decoded_bing:
                    _collect_pairs(decoded_bing, per_kw_limit)
            except Exception:
                pass

        return discovered

    def _attach_comments_concurrent(
        self,
        posts: List[Dict[str, Any]],
        top_n_for_comments: int = 80,
        max_comments_per_post: int = 10,
        max_workers: int = 4,
    ) -> None:
        if not self.has_burner_account or top_n_for_comments <= 0 or not posts:
            return
        candidates = [
            p for p in posts
            if safe_int((p.get("engagement") or {}).get("comment_count"), 0) > 0 and not (p.get("comments") or [])
        ]
        if not candidates:
            return
        candidates.sort(
            key=lambda p: (
                safe_int((p.get("engagement") or {}).get("like_count"), 0) * 2
                + safe_int((p.get("engagement") or {}).get("comment_count"), 0) * 5
            ),
            reverse=True,
        )
        selected = candidates[:top_n_for_comments]

        def _worker(post_obj: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
            time.sleep(random.uniform(0.05, 0.2))
            cmts = self.fetch_post_comments(post_obj["post_id"], max_comments=max_comments_per_post)
            return post_obj, cmts

        with ThreadPoolExecutor(max_workers=max(1, min(max_workers, len(selected)))) as pool:
            futures = [pool.submit(_worker, p) for p in selected]
            for fut in as_completed(futures):
                try:
                    post_obj, cmts = fut.result()
                    if cmts:
                        post_obj["comments"] = cmts
                except Exception:
                    pass

    def search_or_topic_posts(
        self,
        query: str,
        count: int = 20,
        handles: Optional[List[str]] = None,
        hours_back: int = 72,
        fetch_comments: bool = True,
        fetch_external_links: bool = False,
        download_media: bool = False,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        原语 3：按关键词全站定向检索推文（「指哪打哪」多级通道 + 异常降级感知）：
        1) 主力通道 1：专用爬虫小号 (`x_xiaohao`) 无头持久化浏览器直接拦截官方 `SearchTimeline` (`f=top` 热门 + `f=live` 最新)；
        2) 兜底补充通道 2：仅当小号未就绪或原生搜索结果较少时，启用 Yahoo + Bing RSS 全网索引发现 -> `TweetDetail` 补齐；
        3) 可选指定博主补充：若调用方传入 `handles` 则额外补充对应博主近期匹配帖。
        """
        reset_health = bool(kwargs.pop("_reset_health", True))
        if reset_health:
            self.reset_health_state()
            self.last_search_note = ""

        raw_q = str(query or "").replace(",", "|")
        raw_keywords = [w.strip() for w in raw_q.split("|") if w.strip()]
        low_keywords = [w.lower() for w in raw_keywords]
        min_ts = int(time.time() - max(1, hours_back) * 3600)

        if not raw_keywords:
            return self.fetch_hot_or_recommended(count=count, fallback_handles=handles)

        by_id: Dict[str, Dict[str, Any]] = {}
        native_hit_count = 0

        # 通道 1（主力）：专用爬虫小号原生 SearchTimeline 检索
        if self.has_burner_account:
            native_posts = self._search_x_native_via_xiaohao(
                keywords=raw_keywords,
                per_kw_count=max(10, count),
                hours_back=hours_back,
                include_latest=True,
                exclude_retweets=True,
            )
            for np in native_posts:
                if self.is_valuable_post(np) and safe_int(np.get("publish_time"), 0) >= min_ts:
                    by_id[np["post_id"]] = np
            native_hit_count = len(by_id)

        # 通道 2（补充/兜底）：仅当无小号或原生搜索命中不足一半时，用 Yahoo/Bing 搜索引擎补充
        yahoo_hit_count = 0
        if len(by_id) < max(5, count // 2):
            discovered_pairs = self._discover_x_tweets_via_yahoo(raw_keywords, per_kw_limit=max(10, count))
            if discovered_pairs:
                max_cmts = 10 if fetch_comments else 0
                candidate_pairs = [
                    (h, tid) for h, tid in discovered_pairs if tid not in by_id
                ][: max(count * 2, 24)]
                if candidate_pairs:
                    with ThreadPoolExecutor(max_workers=min(6, len(candidate_pairs))) as pool:
                        fut_map = {
                            pool.submit(self.fetch_tweet_with_comments, tid, handle, max_cmts): tid
                            for handle, tid in candidate_pairs
                        }
                        for fut in as_completed(fut_map):
                            try:
                                tw_item = fut.result()
                            except Exception:
                                tw_item = None
                            if tw_item and self.is_valuable_post(tw_item):
                                if safe_int(tw_item.get("publish_time"), 0) >= min_ts:
                                    by_id[tw_item["post_id"]] = tw_item
                                    yahoo_hit_count += 1

            if self.has_burner_account and native_hit_count == 0 and yahoo_hit_count > 0:
                self.record_health_issue(
                    code="X_NATIVE_SEARCH_FALLBACK",
                    message=(
                        f"X 原生搜索关键词 {raw_keywords} 返回 0 条，但外部搜索引擎发现了 {yahoo_hit_count} 条近期推文！"
                        f"疑似小号搜索受限或 SearchTimeline 页面结构变更"
                    ),
                    level="warning",
                    channel="x_native_search",
                )

        # 通道 3：调用方显式指定 handles 时补充匹配
        if handles:
            pool_posts: List[Dict[str, Any]] = []
            for h in handles:
                pool_posts.extend(self.fetch_user_posts(h, count=6))
            keyword_token_groups = [[t for t in k.split() if t] for k in low_keywords if k.strip()]
            for p in pool_posts:
                if p["post_id"] in by_id or safe_int(p.get("publish_time"), 0) < min_ts:
                    continue
                txt = ((p.get("content") or {}).get("text_content") or "").lower()
                if any(all(tok in txt for tok in grp) for grp in keyword_token_groups if grp):
                    by_id[p["post_id"]] = p

        results = sorted(
            by_id.values(),
            key=lambda x: (
                safe_int((x.get("engagement") or {}).get("like_count"), 0) * 2
                + safe_int((x.get("engagement") or {}).get("comment_count"), 0) * 5,
                safe_int(x.get("publish_time"), 0),
            ),
            reverse=True,
        )[:count]

        self.last_search_note = f"原生搜索命中={native_hit_count}条, 索引补充={yahoo_hit_count}条, 最终保留={len(results)}条"

        if fetch_comments and results:
            self._attach_comments_concurrent(results, top_n_for_comments=len(results), max_comments_per_post=10)
        if fetch_external_links and results:
            enrich_posts_with_external_articles(results, proxies=self.proxies)
        if download_media or fetch_external_links or fetch_comments:
            results = self.normalize_batch(results, download_media=download_media)

        if reset_health:
            self.audit_posts_quality(
                results,
                scene="search",
                expect_comments=(fetch_comments and self.has_burner_account),
                expect_media_download=download_media,
                max_fresh_age_hours=max(168, hours_back * 2),
            )
            results = [p for p in results if self.is_valuable_post(p)]
        return results

    def fetch_post_comments(
        self,
        post_id: str,
        max_comments: int = 10,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """原语 4：使用只读小号调用 `TweetDetail` 拉取指定推文底下的前排高赞神评（线程安全 + 异常感知）"""
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
            resp = requests.get(
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
                if resp.status_code in (401, 403):
                    invalidate_burner_cookie_cache(self.burner_user_data_dir)
                    self.record_health_issue(
                        code="X_COMMENTS_AUTH_EXPIRED",
                        message=f"X TweetDetail 评论接口返回 HTTP {resp.status_code}，小号登录态已失效",
                        level="critical",
                        channel="TweetDetail",
                    )
                elif resp.status_code in (400, 404):
                    self.record_health_issue(
                        code="X_TWEETDETAIL_GQL_DRIFT",
                        message=f"X TweetDetail 评论接口返回 HTTP {resp.status_code}，疑似 queryId ({qid}) 漂移",
                        level="warning",
                        channel="TweetDetail",
                    )
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
    # 标准调度入口：供上层定时任务调用（热门榜单 + 推荐流 + 垂直大V + 数据体检）
    # ==========================================================================
    def harvest(self, config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        self.reset_health_state()
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
        include_home_feed = bool(cfg.get("include_home_feed", True))
        include_trending = bool(cfg.get("include_trending", True))
        trending_topic_count = safe_int(cfg.get("trending_topic_count"), 5)
        tweets_per_trend = safe_int(cfg.get("tweets_per_trend"), 5)
        list_id = cfg.get("list_id")
        top_n_for_comments = safe_int(cfg.get("fetch_comments_for_top_n"), 80)
        fetch_ext_links = bool(cfg.get("fetch_external_links", True))
        download_media = bool(cfg.get("download_media", True))

        self.last_channel_stats = {}
        collected_by_id: Dict[str, Dict[str, Any]] = {}

        # 1. 小号主页推荐流 / List 列表流（广谱热点底座）
        if self.has_burner_account and (include_home_feed or list_id):
            feed_posts = self.fetch_hot_or_recommended(count=safe_int(cfg.get("home_feed_count"), 30), list_id=list_id)
            self.last_channel_stats["主页推荐流"] = len(feed_posts)
            for p in feed_posts:
                if self.is_valuable_post(p):
                    collected_by_id[p["post_id"]] = p

        # 2. X 官方全球/美国实时热搜榜 (`/1.1/trends/place.json`) -> 穿透抓取热搜爆款推文
        if self.has_burner_account and include_trending and trending_topic_count > 0:
            trends = self.fetch_trending_topics(limit=trending_topic_count)
            trend_names = [t["name"] for t in trends if t.get("name")]
            if trend_names:
                trend_posts = self._search_x_native_via_xiaohao(
                    keywords=trend_names,
                    per_kw_count=max(3, tweets_per_trend),
                    hours_back=48,
                    include_latest=False,
                    exclude_retweets=True,
                )
                self.last_channel_stats[f"官方热搜榜({len(trend_names)}词)"] = len(trend_posts)
                for tp in trend_posts:
                    if self.is_valuable_post(tp):
                        collected_by_id[tp["post_id"]] = tp
            else:
                self.last_channel_stats["官方热搜榜"] = 0

        # 3. 核心垂直大 V 实时时间线（容忍个别博主当日未发帖，仅当 >=50% 集体返回 0 条时发出通道退化告警）
        zero_kols: List[str] = []
        for handle in handles:
            clean_h = str(handle).lstrip("@").strip()
            posts = self.fetch_user_posts(clean_h, count=tweets_per_handle)
            self.last_channel_stats[f"@{clean_h}"] = len(posts)
            if not posts:
                zero_kols.append(f"@{clean_h}")
            for p in posts:
                if self.is_valuable_post(p):
                    collected_by_id[p["post_id"]] = p
            time.sleep(random.uniform(0.12, 0.25))

        if handles and len(zero_kols) >= max(3, len(handles) // 2):
            self.record_health_issue(
                code="X_KOL_BATCH_DEGRADED",
                message=f"多达 {len(zero_kols)}/{len(handles)} 个 X 监控大 V 返回 0 条推文 ({', '.join(zero_kols[:6])})，疑似 UserTweets 限流或接口退化",
                level="warning",
                channel="UserTweets",
            )

        all_posts = list(collected_by_id.values())

        # 4. 并发为有评论的推文拉齐前排评论树
        self._attach_comments_concurrent(
            all_posts,
            top_n_for_comments=top_n_for_comments,
            max_comments_per_post=10,
            max_workers=4,
        )

        # 5. 对推文中携带的外部新闻/Substack/博客长文链接提取正文统一拼入 text_content
        if fetch_ext_links and all_posts:
            ext_cnt = enrich_posts_with_external_articles(all_posts, proxies=self.proxies)
            if ext_cnt > 0:
                self.last_channel_stats["外链正文富化"] = ext_cnt

        # 6. 统一抹平差异、并发下载原图至本地 `media_downloads/`，并执行严苛数据质量体检
        normalized_posts = self.normalize_batch(all_posts, download_media=download_media)
        self.audit_posts_quality(
            normalized_posts,
            scene="harvest",
            expect_comments=self.has_burner_account,
            expect_media_download=download_media,
            max_fresh_age_hours=168,
        )
        return [p for p in normalized_posts if self.is_valuable_post(p)]
