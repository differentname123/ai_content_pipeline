# -*- coding: utf-8 -*-
"""
X (Twitter) 平台专用采集与差异抹平适配器 (XHarvester)

================================================================================
【运维速查手册 (Ops Quick Guide) —— 免登主干 + 登录态最小特权架构】
================================================================================
1. 核心设计原则（零账号优先 + 登录态最小特权）：
   - **免登零风控主干（100% 不用小号，永不触发 326 锁号）**：
     ① 实时热搜榜：`getdaytrends.com`（美国榜 + 全球榜，备选 `trends24.in`）；
     ② 核心大 V 实时流：免登 `Guest Token` + `curl_cffi` Chrome 指纹直连秒级同步大 V；
     ③ 全站关键词搜索与热搜穿透：`Yahoo Search (site:x.com)` 24h/7d 多页分时检索 + 雪花 ID 秒级时间预筛；
     ④ 单帖完整正文/原图/引用帖/外链/互动数据：`api.fxtwitter.com`（支持 X Article 长文）+ `cdn.syndication.twimg.com` 双镜像热备。
   - **小号最小特权增强（仅做必须登录且风控极低的两件事）**：
     ① 首页推荐流 (`HomeTimeline`)：小号可用时每轮仅请求 **1 次**，不可用时自动静默回退免登聚合热帖；
     ② 重点帖慢速补神评 (`TweetDetail`)：单线程串行慢速（随机间隔 `1.5~2.8s`），广谱仅限 Top 8 帖、深挖每方向仅限 Top 5 帖。
   - **严守账号安全隔离铁律**：
     默认仅读取 `x_xiaohao_browser_session_dir` (`W:\\temp\\x_xiaohao`) 的 Cookie，**彻底废除 X 无头浏览器搜索**，**严禁触碰主号 `x_myself`**！

2. 自动风控熔断与静默自愈 (`logs/runtime/x_burner_health_state.json`)：
   - 全部 HTTP 请求统一走 `curl_cffi` (`impersonate="chrome"`) 真实浏览器 TLS 指纹；
   - 一旦检测到 `code=326` 临时验证锁或 `401/403` 掉线，立即写入 6 小时静默冷却状态，后续模块与轮次自动跳过登录态步骤、100% 依靠免登主干无感运行，绝不反复撞击风控页；
   - 若检测到 `x_xiaohao` Cookie 已刷新解锁或冷却期满探活成功，自动恢复增强能力。
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
BURNER_COOLDOWN_SECONDS = 6 * 3600
MAX_SAFE_COMMENT_POSTS_HARD_CAP = 10

DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
)

CHROME_BASE_HEADERS = {
    "user-agent": DEFAULT_UA,
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

X_WEB_BEARER_TOKEN = (
    "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D"
    "1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
)


def _snowflake_to_unix_ts(tweet_id: str) -> int:
    """利用 X (Twitter) Snowflake ID 零网络开销直接换算精确发布秒级时间戳。"""
    s = str(tweet_id or "").strip()
    if not s.isdigit():
        return 0
    val = int(s)
    if val < 300000000000000000:
        return 0
    return int(((val >> 22) + 1288834974657) // 1000)

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

# 经 UserByScreenName 与 2026 免登 Guest Token 实测验证的高价值英文大 V 映射表
# (guest_realtime=True 表示免登直接返回实时时间线新帖，False 表示免登返回历史 Top100 归档旧帖)
VERIFIED_X_ACCOUNTS = {
    # 疯狂赌局与赔率吃瓜
    "Polymarket": {"uid": "1261335549215989760", "guest_realtime": True, "category": "insane_bets"},
    "Kalshi": {"uid": "1071484733543079936", "guest_realtime": True, "category": "insane_bets"},
    "wallstreetbets": {"uid": "588569122", "guest_realtime": True, "category": "money_drama"},
    # 宏观金钱、国会山内幕与暴富暴负奇闻
    "KobeissiLetter": {"uid": "3316376038", "guest_realtime": True, "category": "macro_money"},
    "WatcherGuru": {"uid": "1387497871751196672", "guest_realtime": True, "category": "breaking_money"},
    "MorningBrew": {"uid": "3092541736", "guest_realtime": True, "category": "biz_story"},
    "Barchart": {"uid": "28212513", "guest_realtime": True, "category": "macro_money"},
    # 科技商业暗黑吃瓜与全球猎奇
    "Dexerto": {"uid": "76766018", "guest_realtime": True, "category": "internet_drama"},
    "OpenAI": {"uid": "4398626122", "guest_realtime": True, "category": "ai_tech"},
    # 链上巨鲸与清算实锤
    "CoinGlass_com": {"uid": "1169095068164050945", "guest_realtime": True, "category": "liquidation_drama"},
    "arkham": {"uid": "1123403704323805184", "guest_realtime": True, "category": "whale_tracker"},
    "WuBlockchain": {"uid": "111533746", "guest_realtime": True, "category": "crypto_scoop"},
    # 免登返回历史 Top100 旧帖的大 V（仅小号登录态返回今日实时流，不放入默认免登监控池）
    "StockMKTNewz": {"uid": "1250830691824283648", "guest_realtime": False, "category": "macro_money"},
    "charliebilello": {"uid": "1413027896", "guest_realtime": False, "category": "macro_money"},
    "MarioNawfal": {"uid": "1319287761048723458", "guest_realtime": False, "category": "viral_news"},
    "NoContextHumans": {"uid": "1138842105856573445", "guest_realtime": False, "category": "viral_hook"},
    "historyinmemes": {"uid": "855481986290524160", "guest_realtime": False, "category": "viral_hook"},
    "buitengebieden": {"uid": "1130021685351079938", "guest_realtime": False, "category": "viral_hook"},
    "whale_alert": {"uid": "1039833297751302144", "guest_realtime": False, "category": "whale_tracker"},
    "DefiLlama": {"uid": "1321585610654687241", "guest_realtime": False, "category": "defi_data"},
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
        self.burner_health_path = get_runtime_file_path("x_burner_health_state.json")
        self._burner_cooldown_entry: Optional[Dict[str, Any]] = None

        explicit_zero_auth = burner_user_data_dir is not None and str(burner_user_data_dir).strip() == ""
        default_xiaohao_dir = resolve_session_dir("x_xiaohao_browser_session_dir", "x_xiaohao")
        effective_dir = "" if explicit_zero_auth else (
            default_xiaohao_dir if burner_user_data_dir is None else str(burner_user_data_dir).strip()
        )
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

        if not explicit_zero_auth and not self.has_burner_account:
            self.record_health_issue(
                code="X_BURNER_AUTH_MISSING",
                message=(
                    f"专用小号目录 `{self.burner_user_data_dir or '未配置'}` 未检测到登录态，已自动运行在纯免登主干模式（仅跳过推荐流与补评）。"
                ),
                level="info",
                channel="auth",
                persistent=True,
            )

        self.uid_cache_path = get_runtime_file_path("x_uid_cache.json")
        self.uid_cache = self._load_uid_cache()

    def _http_get(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        timeout: int = REQUEST_TIMEOUT,
    ):
        """统一走 curl_cffi Chrome TLS 指纹发起线程安全 GET 请求，消除 Python urllib3 指纹暴露。"""
        merged_headers = dict(CHROME_BASE_HEADERS)
        if headers:
            merged_headers.update(headers)
        if cffi_requests is not None:
            return cffi_requests.get(
                url,
                headers=merged_headers,
                params=params,
                impersonate="chrome",
                proxies=self.proxies,
                timeout=timeout,
            )
        return self.session.get(
            url,
            headers=merged_headers,
            params=params,
            proxies=self.proxies,
            timeout=timeout,
        )

    def _http_post(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        data: Optional[Any] = None,
        timeout: int = REQUEST_TIMEOUT,
    ):
        """统一走 curl_cffi Chrome TLS 指纹发起线程安全 POST 请求。"""
        merged_headers = dict(CHROME_BASE_HEADERS)
        if headers:
            merged_headers.update(headers)
        if cffi_requests is not None:
            return cffi_requests.post(
                url,
                headers=merged_headers,
                data=data,
                impersonate="chrome",
                proxies=self.proxies,
                timeout=timeout,
            )
        return self.session.post(
            url,
            headers=merged_headers,
            data=data,
            proxies=self.proxies,
            timeout=timeout,
        )

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
    def _burner_state_fingerprint(self) -> str:
        """结合 token 尾号与浏览器 Cookies 文件修改时间，一旦人工解锁关闭窗口即可自动感知解除冷却。"""
        auth_tail = (self.cookie_dict.get("auth_token") or "")[-6:]
        ct0_tail = (self.cookie_dict.get("ct0") or "")[-8:]
        mtime_int = 0
        if self.burner_user_data_dir and os.path.isdir(self.burner_user_data_dir):
            for rel in (os.path.join("Default", "Network", "Cookies"), os.path.join("Default", "Cookies")):
                p = os.path.join(self.burner_user_data_dir, rel)
                if os.path.exists(p):
                    try:
                        mtime_int = max(mtime_int, int(os.path.getmtime(p)))
                    except Exception:
                        pass
        return f"{auth_tail}:{ct0_tail}:{mtime_int}"

    def _load_burner_health_map(self) -> Dict[str, Any]:
        if os.path.exists(self.burner_health_path):
            try:
                data = read_json(self.burner_health_path)
                if isinstance(data, dict):
                    return data
            except Exception:
                pass
        return {}

    def _save_burner_health_map(self, data: Dict[str, Any]) -> None:
        try:
            save_json(self.burner_health_path, data)
        except Exception:
            pass

    @property
    def is_burner_usable(self) -> bool:
        """判断当前小号是否可安全调用（既有 Cookie 且未处于 326/掉线静默冷却期）。"""
        if not self.has_burner_account:
            return False
        now_ts = int(time.time())
        if self._burner_cooldown_entry and now_ts < safe_int(self._burner_cooldown_entry.get("cooldown_until"), 0):
            return False

        uid_key = self.burner_account_uid
        state_map = self._load_burner_health_map()
        entry = state_map.get(uid_key)
        if not isinstance(entry, dict):
            self._burner_cooldown_entry = None
            return True

        cur_fp = self._burner_state_fingerprint
        saved_fp = str(entry.get("fingerprint") or "")
        cooldown_until = safe_int(entry.get("cooldown_until"), 0)
        if saved_fp and saved_fp != cur_fp:
            # 检测到浏览器会话目录已更新（例如用户已顺手点过验证），立即自动解除冷却
            state_map.pop(uid_key, None)
            self._save_burner_health_map(state_map)
            self._burner_cooldown_entry = None
            return True

        if now_ts < cooldown_until:
            self._burner_cooldown_entry = entry
            return False

        self._burner_cooldown_entry = None
        return True

    def _mark_burner_cooldown(
        self,
        status: str,
        reason: str,
        channel: str,
        cooldown_sec: int = BURNER_COOLDOWN_SECONDS,
    ) -> None:
        now_ts = int(time.time())
        entry = {
            "status": status,
            "reason": reason,
            "channel": channel,
            "locked_at": now_ts,
            "cooldown_until": now_ts + max(300, int(cooldown_sec)),
            "fingerprint": self._burner_state_fingerprint,
        }
        self._burner_cooldown_entry = entry
        state_map = self._load_burner_health_map()
        state_map[self.burner_account_uid] = entry
        self._save_burner_health_map(state_map)
        self.record_health_issue(
            code="X_BURNER_COOLDOWN_ACTIVE",
            message=f"{reason}（已进入 {cooldown_sec // 3600}h 静默保护，热榜/大V/搜索自动走免登主干不受影响）",
            level="warning",
            channel=channel,
        )

    def _mark_burner_healthy(self) -> None:
        self._burner_cooldown_entry = None
        state_map = self._load_burner_health_map()
        if self.burner_account_uid in state_map:
            state_map.pop(self.burner_account_uid, None)
            self._save_burner_health_map(state_map)

    def _check_gql_burner_response(self, resp: Any, payload: Optional[Dict[str, Any]], channel: str) -> bool:
        """检查小号 GraphQL 响应是否触发 326 安全验证或 401/403 鉴权失效；若异常则自动转入静默冷却。"""
        status_code = safe_int(getattr(resp, "status_code", 0), 0)
        err_codes = set()
        if isinstance(payload, dict) and isinstance(payload.get("errors"), list):
            err_codes = {safe_int(e.get("code"), 0) for e in payload["errors"] if isinstance(e, dict)}

        if 326 in err_codes:
            self._mark_burner_cooldown(
                status="locked_326",
                reason=f"X 小号 (UID={self.burner_account_uid}) 处于安全验证锁定期 (code=326)",
                channel=channel,
            )
            return False

        if status_code in (401, 403) or (err_codes & {32, 64, 89}):
            invalidate_burner_cookie_cache(self.burner_user_data_dir)
            self._mark_burner_cooldown(
                status="auth_expired",
                reason=f"X 小号 (UID={self.burner_account_uid}) 登录态不可用 (HTTP {status_code}, codes={sorted(err_codes)})",
                channel=channel,
            )
            return False

        return status_code == 200

    @property
    def burner_account_summary(self) -> str:
        """返回供运维日志展示的账号与通道状态摘要"""
        if not self.has_burner_account:
            return "纯免登主干模式 (热榜+大V+多页搜索+fxtwitter，未配小号)"
        session_dir_name = os.path.basename(os.path.normpath(self.burner_user_data_dir)) if self.burner_user_data_dir else "直接Cookie注入"
        auth_tail = (self.cookie_dict.get("auth_token") or "")[-6:]
        if not self.is_burner_usable:
            rem_min = max(
                1,
                (safe_int((self._burner_cooldown_entry or {}).get("cooldown_until"), 0) - int(time.time())) // 60,
            )
            return (
                f"免登主干运行中 | 小号静默冷却保护 (UID={self.burner_account_uid}, "
                f"Session={session_dir_name}, 剩余{rem_min}分钟)"
            )
        return f"免登主干 + 小号低频增强在线 (UID={self.burner_account_uid}, Session={session_dir_name}, token尾号=...{auth_tail})"

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
        headers = {"authorization": f"Bearer {X_WEB_BEARER_TOKEN}"}
        resp = self._http_post(
            "https://api.x.com/1.1/guest/activate.json",
            headers=headers,
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
            "x-guest-token": self.guest_token or "",
            "x-twitter-active-user": "yes",
            "x-twitter-client-language": "en",
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
            resp = self._http_get(
                url,
                headers=self._get_guest_headers(),
                params={
                    "variables": json.dumps({"screen_name": clean_name, "withSafetyModeUserFields": True}),
                    "features": json.dumps(X_USER_FEATURES_2026),
                },
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
        若小号健康可用则优先走小号请求头（可返回 `is_following`/`is_followed_by`），否则直接走免登请求头。
        """
        clean_name = str(screen_name or "").lstrip("@").strip()
        if not clean_name:
            return None
        url = f"https://x.com/i/api/graphql/{X_GQL_QUERY_IDS['UserByScreenName']}/UserByScreenName"
        req_params = {
            "variables": json.dumps({"screen_name": clean_name, "withSafetyModeUserFields": True}, separators=(",", ":")),
            "features": json.dumps(X_USER_FEATURES_2026, separators=(",", ":")),
        }
        modes = ["burner", "guest"] if self.is_burner_usable else ["guest"]
        for mode in modes:
            hdrs = self._get_burner_headers() if mode == "burner" else self._get_guest_headers()
            try:
                resp = self._http_get(
                    url,
                    headers=hdrs,
                    params=req_params,
                    timeout=REQUEST_TIMEOUT,
                )
                payload = None
                try:
                    payload = resp.json()
                except Exception:
                    payload = None
                if mode == "burner" and not self._check_gql_burner_response(resp, payload, "UserByScreenName"):
                    continue
                if resp.status_code != 200 or not isinstance(payload, dict):
                    continue
                res = (((payload.get("data") or {}).get("user") or {}).get("result")) or {}
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
        安全路由策略：
        - 默认 (`force_burner=False`) 100% 走免登 Guest Token + Chrome TLS 指纹，零小号消耗；
        - 默认 `max_age_hours=0` 不在拉取层做时间丢弃（时间窗口筛选统一由下游流水线处理）；
        - 仅在调用方显式指定 `force_burner=True` 且小号健康可用时才使用登录态，遇异常立即转冷却并回退免登。
        """
        clean_name = str(user_id).lstrip("@").strip()
        meta_info = VERIFIED_X_ACCOUNTS.get(clean_name) or {}
        category = meta_info.get("category", "x_kol")
        max_age_hours = safe_int(kwargs.get("max_age_hours"), 0)
        min_ts = int(time.time() - max_age_hours * 3600) if max_age_hours > 0 else 0
        use_burner = bool(force_burner and self.is_burner_usable)

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
            resp = self._http_get(
                url,
                headers=headers,
                params=req_params,
                timeout=REQUEST_TIMEOUT,
            )
            payload = None
            try:
                payload = resp.json()
            except Exception:
                payload = None

            if use_burner and not self._check_gql_burner_response(resp, payload, "UserTweets"):
                resp = self._http_get(
                    url,
                    headers=self._get_guest_headers(),
                    params=req_params,
                    timeout=REQUEST_TIMEOUT,
                )
                payload = resp.json() if resp.status_code == 200 else None
            elif resp.status_code == 429 and not use_burner:
                self._refresh_guest_token()
                resp = self._http_get(
                    url,
                    headers=self._get_guest_headers(),
                    params=req_params,
                    timeout=REQUEST_TIMEOUT,
                )
                payload = resp.json() if resp.status_code == 200 else None

            if resp.status_code != 200 or not isinstance(payload, dict):
                if resp.status_code in (400, 404):
                    self.record_health_issue(
                        code="X_USERTWEETS_GQL_DRIFT",
                        message=f"X UserTweets 免登接口返回 HTTP {resp.status_code}，疑似 queryId 漂移",
                        level="critical",
                        channel="UserTweets",
                    )
                return []

            # 【保留过滤原因说明】跳过主页置顶帖 (`skip_pinned=True`)：
            # 大 V 主页置顶多为常年不换的个人简介、返佣链接或数年前旧帖，每轮重复拉取会浪费外链/图片富化时间。
            raw_nodes = _extract_gql_tweet_objects(payload, skip_pinned=True)
            by_id = {}
            for node in raw_nodes:
                legacy_node = node.get("legacy") or {}
                # 【保留过滤原因说明】跳过无个人评述的纯转发 (`retweeted_status_result`) 与无上下文纯回复：
                # 带个人观点的引用转发 (Quote Tweet) 会正常保留；纯一键转推与他人评论区碎片回复无独立信息量且浪费富化耗时。
                if exclude_retweets and legacy_node.get("retweeted_status_result"):
                    continue
                if exclude_replies and legacy_node.get("in_reply_to_status_id_str"):
                    continue
                node_uid = str(legacy_node.get("user_id_str") or "")
                if node_uid and node_uid != str(uid):
                    continue
                item = self._convert_raw_tweet(node, fallback_screen_name=clean_name, category=category)
                pub_ts = safe_int(item.get("publish_time"), 0) or _snowflake_to_unix_ts(item.get("post_id", ""))
                if min_ts > 0 and pub_ts > 0 and pub_ts < min_ts:
                    continue
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
        100% 免登拉取 X 全球/美国实时热搜榜：
        主源抓取 `getdaytrends.com`（美国榜 + 全球榜），二级免登热备 `trends24.in`，零账号消耗、永不触发风控。
        """
        import html as _html

        merged: List[Dict[str, Any]] = []
        seen_names = set()

        def _append_trends(raw_names: Sequence[str], woeid_val: int) -> int:
            added_cnt = 0
            for idx, raw_n in enumerate(raw_names):
                name = _html.unescape(str(raw_n or "")).strip()
                if not name or len(name) < 2:
                    continue
                key = name.lower()
                if key in seen_names:
                    continue
                seen_names.add(key)
                merged.append({
                    "name": name,
                    "tweet_volume": 0,
                    "woeid": int(woeid_val),
                    "rank": idx + 1,
                })
                added_cnt += 1
            return added_cnt

        primary_sources = []
        for woeid in woeid_list:
            if int(woeid) == 23424977:
                primary_sources.append((23424977, "https://getdaytrends.com/united-states/", r'<a[^>]+href="/united-states/trend/[^"]+"[^>]*>([^<]+)</a>'))
            elif int(woeid) == 1:
                primary_sources.append((1, "https://getdaytrends.com/", r'<a[^>]+href="/trend/[^"]+"[^>]*>([^<]+)</a>'))

        if not primary_sources:
            primary_sources.append((23424977, "https://getdaytrends.com/united-states/", r'<a[^>]+href="/united-states/trend/[^"]+"[^>]*>([^<]+)</a>'))

        for w_id, t_url, pattern in primary_sources:
            try:
                resp = self._http_get(t_url, timeout=10)
                if resp.status_code == 200 and resp.text:
                    hits = re.findall(pattern, resp.text)
                    _append_trends(hits[:50], w_id)
            except Exception as e:
                logger.debug(f"[X适配器] getdaytrends 拉取异常 ({t_url}): {e}")

        # 二级免登备选：trends24.in
        if not merged:
            backup_sources = [
                (23424977, "https://trends24.in/united-states/"),
                (1, "https://trends24.in/"),
            ]
            for w_id, b_url in backup_sources:
                try:
                    resp = self._http_get(b_url, timeout=10)
                    if resp.status_code == 200 and resp.text:
                        hits = re.findall(r'<a[^>]*class=[^>]*trend-link[^>]*>([^<]+)</a>', resp.text)
                        _append_trends(hits[:50], w_id)
                except Exception as e:
                    logger.debug(f"[X适配器] trends24 备选拉取异常 ({b_url}): {e}")

        if not merged:
            self.record_health_issue(
                code="X_PUBLIC_TRENDS_UNAVAILABLE",
                message="免登公开热搜源 (getdaytrends / trends24) 均返回 0 个热搜词，请检查代理连通性",
                level="warning",
                channel="trends",
            )

        merged.sort(key=lambda x: x["rank"])
        return merged if limit <= 0 else merged[:limit]

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
        - 若小号健康可用 (`is_burner_usable=True`)：仅发 1 次 `curl_cffi` 请求拉取 `HomeTimeline`（或 `ListLatestTweetsTimeline`）；
        - 若无小号或处于静默冷却保护期：自动回退免登头部实时大 V 热帖聚合，全程零阻塞。
        """
        if self.is_burner_usable:
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
                resp = self._http_get(
                    url,
                    headers=self._get_burner_headers(),
                    params={
                        "variables": json.dumps(variables, separators=(",", ":")),
                        "features": json.dumps(X_GQL_FEATURES_2026, separators=(",", ":")),
                    },
                    timeout=REQUEST_TIMEOUT,
                )
                payload = None
                try:
                    payload = resp.json()
                except Exception:
                    payload = None

                if self._check_gql_burner_response(resp, payload, op) and isinstance(payload, dict):
                    nodes = _extract_gql_tweet_objects(payload)
                    by_id = {}
                    for node in nodes:
                        # 【保留过滤原因说明】推荐流跳过无个人评述的纯转推 (`RT @xxx`)，保留带观点的引用转发 (Quote Tweet)，防止重复噪音。
                        if (node.get("legacy") or {}).get("retweeted_status_result"):
                            continue
                        item = self._convert_raw_tweet(node, category=f"x_{op.lower()}")
                        if self.is_valuable_post(item):
                            by_id[item["post_id"]] = item
                    if by_id:
                        self._mark_burner_healthy()
                        return sorted(by_id.values(), key=lambda x: x["engagement"]["like_count"], reverse=True)[:count]
                    self.record_health_issue(
                        code="X_HOME_EMPTY_PARSE",
                        message=f"X 小号 {op} 返回 HTTP 200 但解析有效推文为 0 条，已自动回退免登大 V 热帖流",
                        level="warning",
                        channel=op,
                    )
                elif resp.status_code in (400, 404):
                    self.record_health_issue(
                        code="X_HOME_GQL_DRIFT",
                        message=f"X 小号 {op} 返回 HTTP {resp.status_code}，疑似 GraphQL queryId ({qid}) 过期，已回退免登流",
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

        handles = fallback_handles or [
            "Polymarket",
            "KobeissiLetter",
            "WatcherGuru",
            "Dexerto",
            "Barchart",
            "MorningBrew",
            "wallstreetbets",
            "OpenAI",
        ]
        combined = []
        for h in handles:
            combined.extend(self.fetch_user_posts(h, count=max(5, count // len(handles))))
            time.sleep(0.15)
        combined.sort(key=lambda x: x["engagement"]["like_count"], reverse=True)
        return combined[:count]

    def _convert_fxtwitter_tweet(self, tw: Dict[str, Any], fallback_screen_name: str = "") -> Optional[Dict[str, Any]]:
        """免登主力详情转换：将公开镜像 `api.fxtwitter.com` 的 tweet 对象（含长文 Article、引用帖与外链）转换为标准 Schema。"""
        if not isinstance(tw, dict) or not tw.get("id"):
            return None
        tid = str(tw["id"]).strip()
        author = tw.get("author") or {}
        screen_name = str(author.get("screen_name") or fallback_screen_name or "unknown").strip()
        author_name = str(author.get("name") or screen_name).strip()

        raw_text_str = str(tw.get("text") or "")
        article_obj = tw.get("article")
        title_val: Optional[str] = None
        is_long_article = False
        if isinstance(article_obj, dict):
            art_title = clean_post_text(str(article_obj.get("title") or "")).strip()
            blocks = ((article_obj.get("content") or {}).get("blocks")) or []
            art_paragraphs = [
                str(b.get("text") or "").strip()
                for b in blocks
                if isinstance(b, dict) and str(b.get("text") or "").strip()
            ]
            art_body = "\n".join(art_paragraphs).strip() or str(article_obj.get("preview_text") or "").strip()
            if art_title:
                title_val = art_title
            if art_body and len(art_body) > len(raw_text_str):
                raw_text_str = art_body
                is_long_article = True

        text = clean_post_text(raw_text_str)

        hashtags: List[str] = []
        mentioned_coins: List[str] = []
        external_url = ""
        facets = ((tw.get("raw_text") or {}).get("facets")) or []
        for fc in facets:
            if not isinstance(fc, dict):
                continue
            ftype = str(fc.get("type") or "").lower()
            if ftype == "url":
                cand_u = str(fc.get("replacement") or fc.get("original") or "").strip()
                if not external_url and is_fetchable_external_article_url(cand_u):
                    external_url = cand_u
            elif ftype == "hashtag":
                ht = str(fc.get("original") or "").lstrip("#").strip()
                if ht and ht not in hashtags:
                    hashtags.append(ht)
            elif ftype in ("cashtag", "symbol"):
                sym = str(fc.get("original") or "").lstrip("$").strip().upper()
                if sym and sym not in mentioned_coins:
                    mentioned_coins.append(sym)

        if not external_url:
            for u_match in re.findall(r"https?://[^\s<>\"')]+", text):
                if is_fetchable_external_article_url(u_match):
                    external_url = u_match
                    break

        media_obj = tw.get("media") or {}
        photos = media_obj.get("photos") or []
        videos = media_obj.get("videos") or []
        main_images = [p["url"] for p in photos if isinstance(p, dict) and p.get("url")]
        images = list(main_images)
        video_url = videos[0].get("url") if videos and isinstance(videos[0], dict) else None
        video_duration = int(safe_int(videos[0].get("duration"), 1)) if video_url else 0

        if isinstance(article_obj, dict):
            cov_url = (((article_obj.get("cover_media") or {}).get("media_info") or {}).get("original_img_url"))
            if cov_url and cov_url not in images:
                images.insert(0, cov_url)
                main_images.insert(0, cov_url)

        quoted_data = None
        qt = tw.get("quote")
        if isinstance(qt, dict) and qt.get("id"):
            q_author = qt.get("author") or {}
            q_txt = clean_post_text(str(qt.get("text") or ""))
            if not external_url:
                for qfc in ((qt.get("raw_text") or {}).get("facets") or []):
                    if isinstance(qfc, dict) and str(qfc.get("type") or "").lower() == "url":
                        q_u = str(qfc.get("replacement") or "").strip()
                        if is_fetchable_external_article_url(q_u):
                            external_url = q_u
                            break
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

        card_type = "BUZZ_VIDEO" if video_url else ("BUZZ_LONG" if (is_long_article or len(text) >= 500) else "BUZZ_SHORT")
        item = self.create_empty_post(card_type=card_type)
        item["post_id"] = tid
        item["publish_time"] = (
            safe_int(tw.get("created_timestamp"), 0)
            or normalize_publish_timestamp(tw.get("created_at"))
            or _snowflake_to_unix_ts(tid)
        )
        item["author_id"] = str(author.get("id") or "") or None
        item["author"].update({
            "username": screen_name,
            "author_name": author_name,
            "is_verified": False,
            "followers_count": safe_int(author.get("followers"), 0),
        })
        item["content"].update({
            "title": title_val,
            "text_content": text,
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
            "view_count": safe_int(tw.get("views"), 0),
            "like_count": safe_int(tw.get("likes"), 0),
            "comment_count": safe_int(tw.get("replies"), 0),
            "share_count": safe_int(tw.get("retweets"), 0),
            "quote_count": safe_int(tw.get("quotes"), 0),
            "bookmark_count": safe_int(tw.get("bookmarks"), 0),
        })
        meta_payload: Dict[str, Any] = {
            "url": str(tw.get("url") or f"https://x.com/{screen_name}/status/{tid}"),
            "lang": tw.get("lang"),
            "category": "x_search",
            "main_images": main_images,
            "quoted_post": quoted_data,
        }
        if external_url:
            meta_payload["external_url"] = external_url
        item["metadata"].update(meta_payload)
        return self.normalize_post(item, download_media=False)

    def _convert_syndication_tweet(self, syn: Dict[str, Any], fallback_screen_name: str = "") -> Optional[Dict[str, Any]]:
        """免登二级详情转换：将官方嵌入接口 `cdn.syndication.twimg.com/tweet-result` 响应转为标准 Schema。"""
        if not isinstance(syn, dict) or not syn.get("id_str"):
            return None
        tid = str(syn["id_str"]).strip()
        user_obj = syn.get("user") or {}
        screen_name = str(user_obj.get("screen_name") or fallback_screen_name or "unknown").strip()
        author_name = str(user_obj.get("name") or screen_name).strip()

        entities = syn.get("entities") or {}
        text = _clean_x_text_with_entities(str(syn.get("text") or ""), {"entities": entities})

        external_url = ""
        for u_obj in entities.get("urls") or []:
            if isinstance(u_obj, dict):
                exp_u = str(u_obj.get("expanded_url") or "").strip()
                if is_fetchable_external_article_url(exp_u):
                    external_url = exp_u
                    break

        main_images: List[str] = []
        for p in syn.get("photos") or []:
            if isinstance(p, dict) and p.get("url") and p["url"] not in main_images:
                main_images.append(p["url"])
        for m in syn.get("mediaDetails") or []:
            if isinstance(m, dict) and m.get("type") == "photo" and m.get("media_url_https"):
                if m["media_url_https"] not in main_images:
                    main_images.append(m["media_url_https"])
        images = list(main_images)

        video_url = None
        video_duration = 0
        vid_obj = syn.get("video")
        if isinstance(vid_obj, dict):
            poster = vid_obj.get("poster")
            if poster and poster not in images:
                images.append(poster)
            mp4s = [
                v for v in (vid_obj.get("variants") or [])
                if isinstance(v, dict) and v.get("type") == "video/mp4" and v.get("src")
            ]
            if mp4s:
                video_url = mp4s[-1]["src"]
            video_duration = max(1, int(safe_int(vid_obj.get("durationMs"), 0) / 1000)) if video_url else 0

        quoted_data = None
        qt = syn.get("quoted_tweet")
        if isinstance(qt, dict) and qt.get("id_str"):
            q_user = qt.get("user") or {}
            q_txt = _clean_x_text_with_entities(str(qt.get("text") or ""), {"entities": qt.get("entities") or {}})
            q_photos = [
                p["url"] for p in (qt.get("photos") or [])
                if isinstance(p, dict) and p.get("url")
            ]
            for qp in q_photos:
                if qp not in images:
                    images.append(qp)
            quoted_data = {
                "post_id": str(qt.get("id_str")),
                "username": str(q_user.get("screen_name") or ""),
                "text": q_txt,
                "likes": safe_int(qt.get("favorite_count"), 0),
                "images": q_photos,
                "has_video": bool(qt.get("video")),
            }
            if q_txt and q_txt not in text:
                text = f"{text}\n[引用 @{quoted_data['username']}]: {q_txt}".strip()

        item = self.create_empty_post(card_type="BUZZ_VIDEO" if video_url else "BUZZ_SHORT")
        item["post_id"] = tid
        item["publish_time"] = normalize_publish_timestamp(syn.get("created_at")) or _snowflake_to_unix_ts(tid)
        item["author_id"] = str(user_obj.get("id_str") or "") or None
        item["author"].update({
            "username": screen_name,
            "author_name": author_name,
            "is_verified": bool(user_obj.get("is_blue_verified") or user_obj.get("verified")),
            "followers_count": 0,
        })
        item["content"].update({
            "title": None,
            "text_content": text,
            "hashtags": [
                str(h.get("text")).strip()
                for h in (entities.get("hashtags") or [])
                if isinstance(h, dict) and h.get("text")
            ],
            "mentioned_coins": [],
        })
        item["media"].update({
            "cover_image": images[0] if images else None,
            "images": images,
            "video_url": video_url,
            "video_duration": video_duration,
        })
        item["engagement"].update({
            "view_count": 0,
            "like_count": safe_int(syn.get("favorite_count"), 0),
            "comment_count": safe_int(syn.get("conversation_count"), 0),
            "share_count": 0,
            "quote_count": 0,
            "bookmark_count": 0,
        })
        meta_payload: Dict[str, Any] = {
            "url": f"https://x.com/{screen_name}/status/{tid}",
            "lang": syn.get("lang"),
            "category": "x_search",
            "main_images": main_images,
            "quoted_post": quoted_data,
        }
        if external_url:
            meta_payload["external_url"] = external_url
        item["metadata"].update(meta_payload)
        return self.normalize_post(item, download_media=False)

    def _fetch_tweet_zero_auth(self, tweet_id: str, screen_name: str = "") -> Optional[Dict[str, Any]]:
        """100% 免登拉取单条推文全量详情：主走 `api.fxtwitter.com`，失败自动热备切换 `cdn.syndication.twimg.com`。"""
        tid = str(tweet_id or "").strip()
        if not tid.isdigit():
            return None
        handle_seg = str(screen_name or "i").strip() or "i"

        try:
            rfx = self._http_get(f"https://api.fxtwitter.com/{handle_seg}/status/{tid}", timeout=10)
            if rfx.status_code == 200:
                tw_obj = (rfx.json() or {}).get("tweet")
                if isinstance(tw_obj, dict):
                    parsed = self._convert_fxtwitter_tweet(tw_obj, fallback_screen_name=screen_name)
                    if parsed:
                        return parsed
        except Exception as e:
            logger.debug(f"[X适配器] fxtwitter 拉取 {tid} 异常，尝试 syndication 备份: {e}")

        try:
            rsyn = self._http_get(
                f"https://cdn.syndication.twimg.com/tweet-result?id={tid}&lang=en&token=42",
                timeout=10,
            )
            if rsyn.status_code == 200:
                syn_obj = rsyn.json()
                if isinstance(syn_obj, dict) and syn_obj.get("id_str"):
                    return self._convert_syndication_tweet(syn_obj, fallback_screen_name=screen_name)
        except Exception as e:
            logger.debug(f"[X适配器] syndication 备份拉取 {tid} 失败: {e}")

        return None

    def fetch_tweet_with_comments(
        self,
        tweet_id: str,
        screen_name: str = "",
        max_comments: int = 10,
    ) -> Optional[Dict[str, Any]]:
        """
        根据单条推文 ID 拿齐推文详情与（可选）前排神评：
        - 当 `max_comments <= 0` 或小号处于冷却/未配状态时：100% 走免登 `_fetch_tweet_zero_auth`，零小号消耗；
        - 仅当调用方显式要求拿评论 (`max_comments > 0`) 且小号健康可用时，才使用 `curl_cffi` 调 1 次 `TweetDetail`。
        """
        tid = str(tweet_id or "").strip()
        if not tid.isdigit():
            return None

        if max_comments > 0 and self.is_burner_usable:
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
                resp = self._http_get(
                    url,
                    headers=self._get_burner_headers(),
                    params={
                        "variables": json.dumps(variables, separators=(",", ":")),
                        "features": json.dumps(X_GQL_FEATURES_2026, separators=(",", ":")),
                    },
                    timeout=REQUEST_TIMEOUT,
                )
                payload = None
                try:
                    payload = resp.json()
                except Exception:
                    payload = None

                if self._check_gql_burner_response(resp, payload, "TweetDetail") and isinstance(payload, dict):
                    nodes = _extract_gql_tweet_objects(payload)
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
                        self._mark_burner_healthy()
                        if comment_authors_map:
                            pextra = (focal_item.setdefault("metadata", {})).setdefault("platform_extra", {})
                            pextra["comment_authors"] = list(comment_authors_map.values())
                        if raw_replies:
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
                logger.debug(f"[X适配器] TweetDetail 单帖拉取 {tid} 回退免登镜像: {e}")

        return self._fetch_tweet_zero_auth(tid, screen_name=screen_name)

    def _discover_x_tweets_zero_auth(
        self,
        keywords: Sequence[str],
        per_kw_limit: int = 15,
        hours_back: int = 0,
    ) -> List[Tuple[str, str]]:
        """
        100% 免登全站推文发现引擎：
        - 不设人为关键词个数上限（传入多少词即可并发检索多少词）；
        - 优先检索 `Yahoo Search (site:x.com)` 的 24h (`btf=d`) 与 7d (`btf=w`) 实时页以保障新帖命中率；若冷门词近期页无结果，再回退全时段页；
        - 默认 `hours_back=0` 不在采集层硬删旧帖（仅在调用方显式指定 `hours_back>0` 时按 Snowflake 时间戳过滤）。
        """
        min_ts = int(time.time() - max(1, hours_back) * 3600) if (hours_back and hours_back > 0) else 0

        en_terms: List[str] = []
        cn_terms: List[str] = []
        seen_terms = set()

        def _add_term(term: str, bucket: List[str]) -> None:
            t = str(term or "").strip()
            if len(t) < 2:
                return
            low = t.lower()
            if low not in seen_terms:
                seen_terms.add(low)
                bucket.append(t)

        for raw_kw in keywords or []:
            kw = str(raw_kw or "").strip()
            if not kw:
                continue
            has_cjk = bool(re.search(r"[\u4e00-\u9fff]", kw))
            if not has_cjk:
                _add_term(kw, en_terms)
                # 仅在单/少量长词查询时补充首尾双词拆解，避免 25 个热搜词批量传入时成倍膨胀冗余子词
                parts = [p for p in kw.split() if p]
                if len(parts) >= 3 and len(keywords or []) <= 6:
                    _add_term(" ".join(parts[:2]), en_terms)
                    _add_term(" ".join(parts[-2:]), en_terms)
            else:
                # 混合词组（如 '支付风控 Chargeback'）中提取英文专有名词优先查 X，同时保留精简中文词
                ascii_chunks = re.findall(r"[A-Za-z0-9][A-Za-z0-9_.\- ]{1,40}[A-Za-z0-9]", kw)
                for ac in ascii_chunks:
                    _add_term(ac.strip(), en_terms)
                cjk_only = re.sub(r"[A-Za-z0-9_.\-]+", " ", kw).strip()
                if 2 <= len(cjk_only) <= 10:
                    _add_term(cjk_only, cn_terms)
                elif len(kw) <= 10:
                    _add_term(kw, cn_terms)

        search_terms = en_terms + cn_terms
        if not search_terms:
            return []

        def _search_single_term(term: str) -> List[Tuple[str, str]]:
            local_pairs: List[Tuple[str, str]] = []
            local_seen = set()
            q_encoded = quote(f"site:x.com {term}")
            # 依次查 24h 最新窗 -> 7d 热门第 1 页 -> 若仍不足翻第 2 页 -> 若冷门词完全无近期帖再兜底不限时间页
            page_specs = [
                f"https://search.yahoo.com/search?p={q_encoded}&btf=d",
                f"https://search.yahoo.com/search?p={q_encoded}&btf=w",
                f"https://search.yahoo.com/search?p={q_encoded}&btf=w&b=8",
                f"https://search.yahoo.com/search?p={q_encoded}",
            ]
            for idx, target_url in enumerate(page_specs):
                if idx >= 1 and len(local_pairs) >= per_kw_limit:
                    break
                if idx == 3 and (len(local_pairs) > 0 or min_ts > 0):
                    break
                try:
                    rs = self._http_get(target_url, timeout=10)
                    if rs.status_code != 200 or not rs.text:
                        continue
                    decoded = unquote(rs.text)
                    for handle, tid in re.findall(r"(?:x\.com|twitter\.com)/([A-Za-z0-9_]+)/status/(\d{18,22})", decoded):
                        if handle.lower() in ("i", "intent", "share", "home", "search", "hashtag"):
                            continue
                        if tid in local_seen:
                            continue
                        est_ts = _snowflake_to_unix_ts(tid)
                        if min_ts > 0 and est_ts > 0 and est_ts < min_ts:
                            continue
                        local_seen.add(tid)
                        local_pairs.append((handle, tid))
                        if len(local_pairs) >= per_kw_limit * 2:
                            break
                except Exception as e:
                    logger.debug(f"[X适配器] Yahoo 检索词 `{term}` 异常: {e}")
            return local_pairs

        discovered: List[Tuple[str, str]] = []
        seen_tids = set()
        with ThreadPoolExecutor(max_workers=min(4, len(search_terms))) as pool:
            ordered_results = list(pool.map(_search_single_term, search_terms))
            for pairs in ordered_results:
                for handle, tid in pairs:
                    if tid not in seen_tids:
                        seen_tids.add(tid)
                        discovered.append((handle, tid))

        return discovered

    def _discover_x_tweets_via_yahoo(self, keywords: List[str], per_kw_limit: int = 12) -> List[Tuple[str, str]]:
        """向后兼容别名：转发至免登多页发现引擎（默认不做硬性时间丢弃）。"""
        return self._discover_x_tweets_zero_auth(keywords=keywords, per_kw_limit=per_kw_limit, hours_back=0)

    def _attach_comments_safely(
        self,
        posts: List[Dict[str, Any]],
        top_n_for_comments: int = 8,
        max_comments_per_post: int = 10,
    ) -> int:
        """
        登录态最小特权补评：
        【保留过滤原因说明】
        1) X 平台唯独单帖评论接口 `TweetDetail` 必须携带小号 Cookie，因此硬限制单轮最多补评 `MAX_SAFE_COMMENT_POSTS_HARD_CAP=10` 帖，
           并采用「单线程串行 + 1.5~2.8s 真人随机间隔」，防止高频并发把小号打入 326 验证锁；
        2) 短路跳过 `comment_count == 0` 的无评论帖，避免白白消耗宝贵的小号请求配额与等待耗时。
        """
        if not self.is_burner_usable or top_n_for_comments <= 0 or not posts:
            return 0

        effective_n = min(safe_int(top_n_for_comments, 8), MAX_SAFE_COMMENT_POSTS_HARD_CAP)
        candidates = [
            p for p in posts
            if safe_int((p.get("engagement") or {}).get("comment_count"), 0) > 0 and not (p.get("comments") or [])
        ]
        if not candidates:
            return 0

        candidates.sort(
            key=lambda p: (
                safe_int((p.get("engagement") or {}).get("like_count"), 0) * 2
                + safe_int((p.get("engagement") or {}).get("comment_count"), 0) * 5
            ),
            reverse=True,
        )
        selected = candidates[:effective_n]
        attached_posts = 0

        for idx, post_obj in enumerate(selected):
            if not self.is_burner_usable:
                break
            if idx > 0:
                time.sleep(random.uniform(1.5, 2.8))
            cmts = self.fetch_post_comments(post_obj["post_id"], max_comments=max_comments_per_post)
            if cmts:
                post_obj["comments"] = cmts
                attached_posts += 1

        return attached_posts

    def _attach_comments_concurrent(
        self,
        posts: List[Dict[str, Any]],
        top_n_for_comments: int = 8,
        max_comments_per_post: int = 10,
        max_workers: int = 1,
    ) -> None:
        """兼容旧调用签名，内部强制走安全的单线程慢速点穴补评。"""
        self._attach_comments_safely(
            posts=posts,
            top_n_for_comments=top_n_for_comments,
            max_comments_per_post=max_comments_per_post,
        )

    def search_or_topic_posts(
        self,
        query: str,
        count: int = 20,
        handles: Optional[List[str]] = None,
        hours_back: int = 0,
        fetch_comments: bool = True,
        fetch_external_links: bool = False,
        download_media: bool = False,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        原语 3：按关键词全站定向检索推文（100% 免登搜索主力 + 可选重点帖低频补评）：
        1) 主力通道（0 账号消耗）：Yahoo 24h/7d 多页分时发现 + `fxtwitter/syndication` 并发全量富化（无关键词个数上限，默认 `hours_back=0` 不做时间硬删）；
        2) 指定大 V 补充（0 账号消耗）：若传入 `handles` 则通过免登 Guest Token 补充匹配；
        3) 重点帖慢速补评（最小特权）：仅在小号健康可用时，为热度排名前 5 的核心主帖单线程慢速补齐评论。
        """
        reset_health = bool(kwargs.pop("_reset_health", True))
        if reset_health:
            self.reset_health_state()
            self.last_search_note = ""

        raw_q = str(query or "").replace(",", "|")
        raw_keywords = [w.strip() for w in raw_q.split("|") if w.strip()]
        low_keywords = [w.lower() for w in raw_keywords]
        now_ts = int(time.time())
        min_ts = int(now_ts - max(1, hours_back) * 3600) if (hours_back and hours_back > 0) else 0
        recent_priority_ts = int(now_ts - 7 * 86400)

        if not raw_keywords:
            return self.fetch_hot_or_recommended(count=count, fallback_handles=handles)

        by_id: Dict[str, Dict[str, Any]] = {}
        enriched_hit_count = 0

        # 根据传入词数自适应计算每词目标条数：单词深挖、多词（如 25 个热搜词一次性传入）轻量穿透
        adaptive_per_kw = max(5, ((max(1, count) + len(raw_keywords) - 1) // len(raw_keywords)) * 2)
        discovered_pairs = self._discover_x_tweets_zero_auth(
            keywords=raw_keywords,
            per_kw_limit=adaptive_per_kw,
            hours_back=hours_back,
        )
        if discovered_pairs:
            candidate_pairs = discovered_pairs[: max(count * 2, len(raw_keywords) * 4, 40)]
            with ThreadPoolExecutor(max_workers=min(6, len(candidate_pairs))) as pool:
                fut_map = {
                    pool.submit(self._fetch_tweet_zero_auth, tid, handle): tid
                    for handle, tid in candidate_pairs
                }
                for fut in as_completed(fut_map):
                    try:
                        tw_item = fut.result()
                    except Exception:
                        tw_item = None
                    if tw_item and self.is_valuable_post(tw_item):
                        if min_ts <= 0 or safe_int(tw_item.get("publish_time"), 0) >= min_ts:
                            by_id[tw_item["post_id"]] = tw_item
                            enriched_hit_count += 1

        # 通道 2：调用方显式指定 handles 时免登补充匹配
        if handles:
            pool_posts: List[Dict[str, Any]] = []
            for h in handles:
                pool_posts.extend(self.fetch_user_posts(h, count=6, force_burner=False, max_age_hours=hours_back))
            keyword_token_groups = [[t for t in k.split() if t] for k in low_keywords if k.strip()]
            for p in pool_posts:
                if p["post_id"] in by_id or (min_ts > 0 and safe_int(p.get("publish_time"), 0) < min_ts):
                    continue
                txt = ((p.get("content") or {}).get("text_content") or "").lower()
                if any(all(tok in txt for tok in grp) for grp in keyword_token_groups if grp):
                    by_id[p["post_id"]] = p

        results = sorted(
            by_id.values(),
            key=lambda x: (
                1 if safe_int(x.get("publish_time"), 0) >= recent_priority_ts else 0,
                safe_int((x.get("engagement") or {}).get("like_count"), 0) * 2
                + safe_int((x.get("engagement") or {}).get("comment_count"), 0) * 5
                + safe_int((x.get("engagement") or {}).get("quote_count"), 0) * 4
                + safe_int((x.get("engagement") or {}).get("bookmark_count"), 0) * 3,
                safe_int(x.get("publish_time"), 0),
            ),
            reverse=True,
        )[:count]

        cmt_posts_cnt = 0
        top_n_cmts = safe_int(kwargs.get("top_n_for_comments"), 5)
        if fetch_comments and results and self.is_burner_usable and top_n_cmts > 0:
            cmt_posts_cnt = self._attach_comments_safely(
                results,
                top_n_for_comments=min(top_n_cmts, len(results)),
                max_comments_per_post=10,
            )

        cmt_suffix = f", 重点帖补评={cmt_posts_cnt}帖" if self.has_burner_account else ""
        self.last_search_note = (
            f"免登发现候选帖={len(discovered_pairs)}条, 详情富化={enriched_hit_count}条 -> 保留={len(results)}条{cmt_suffix}"
        )

        if fetch_external_links and results:
            enrich_posts_with_external_articles(results, proxies=self.proxies)
        if download_media or fetch_external_links or cmt_posts_cnt > 0:
            results = self.normalize_batch(results, download_media=download_media)

        if reset_health:
            self.audit_posts_quality(
                results,
                scene="search",
                expect_comments=(fetch_comments and self.is_burner_usable and top_n_cmts > 0),
                expect_media_download=download_media,
                max_fresh_age_hours=max(168, (hours_back or 168) * 2),
            )
            results = [p for p in results if self.is_valuable_post(p)]
        return results

    def fetch_post_comments(
        self,
        post_id: str,
        max_comments: int = 10,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """原语 4：仅在小号健康可用时，使用 `curl_cffi` Chrome 指纹拉取指定重点帖的前排神评；遇 326 自动转静默冷却。"""
        if not self.is_burner_usable or not post_id:
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
            resp = self._http_get(
                url,
                headers=self._get_burner_headers(),
                params={
                    "variables": json.dumps(variables, separators=(",", ":")),
                    "features": json.dumps(X_GQL_FEATURES_2026, separators=(",", ":")),
                },
                timeout=REQUEST_TIMEOUT,
            )
            payload = None
            try:
                payload = resp.json()
            except Exception:
                payload = None

            if not self._check_gql_burner_response(resp, payload, "TweetDetail"):
                return []
            if resp.status_code != 200 or not isinstance(payload, dict):
                if resp.status_code in (400, 404):
                    self.record_health_issue(
                        code="X_TWEETDETAIL_GQL_DRIFT",
                        message=f"X TweetDetail 评论接口返回 HTTP {resp.status_code}，疑似 queryId ({qid}) 漂移",
                        level="warning",
                        channel="TweetDetail",
                    )
                return []

            self._mark_burner_healthy()
            nodes = _extract_gql_tweet_objects(payload)
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
                reply_txt = str(parsed["content"].get("text_content") or "").strip()
                reply_imgs = (parsed.get("media") or {}).get("images") or []
                # 【保留过滤原因说明】剔除评论区里「仅 @人名 圈人但无任何实质文字且无配图」的刷屏水评，避免浪费每帖前排神评名额。
                stripped_mentions = re.sub(r"@[A-Za-z0-9_]+", "", reply_txt).strip()
                if not stripped_mentions and not reply_imgs:
                    continue
                raw_replies.append({
                    "reply_id": rid,
                    "parent_id": str(post_id),
                    "create_time": parsed["publish_time"],
                    "author_uid": parsed["author"]["username"],
                    "author_name": parsed["author"]["author_name"],
                    "content": reply_txt,
                    "likes": parsed["engagement"]["like_count"],
                    "replies": parsed["engagement"]["comment_count"],
                    "views": parsed["engagement"]["view_count"],
                    "images": reply_imgs,
                })
            raw_replies.sort(key=lambda x: x["likes"], reverse=True)
            return self.normalize_comments(raw_replies[:max_comments], parent_post_id=str(post_id))
        except Exception as e:
            logger.warning(f"[X适配器] 拉取推文 {post_id} 神评异常: {e}")
            return []

    # ==========================================================================
    # 标准调度入口：供上层定时任务调用（免登热搜+免登大V主干 + 可选小号1次推荐流与Top8补评）
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
            "MorningBrew",
            "Barchart",
            "OpenAI",
            "CoinGlass_com",
            "WuBlockchain",
            "arkham",
        ]
        tweets_per_handle = safe_int(cfg.get("tweets_per_handle"), 5)
        include_home_feed = bool(cfg.get("include_home_feed", True))
        include_trending = bool(cfg.get("include_trending", True))
        trending_topic_count = safe_int(cfg.get("trending_topic_count"), 25)
        tweets_per_trend = safe_int(cfg.get("tweets_per_trend"), 3)
        list_id = cfg.get("list_id")
        top_n_for_comments = min(safe_int(cfg.get("fetch_comments_for_top_n"), 8), MAX_SAFE_COMMENT_POSTS_HARD_CAP)
        fetch_ext_links = bool(cfg.get("fetch_external_links", True))
        download_media = bool(cfg.get("download_media", True))

        self.last_channel_stats = {}
        collected_by_id: Dict[str, Dict[str, Any]] = {}

        # 1. 小号主页推荐流（每轮仅在健康可用时请求 1 次；若冷却中则静默跳过，绝不硬撞）
        if (include_home_feed or list_id) and self.is_burner_usable:
            feed_posts = self.fetch_hot_or_recommended(
                count=safe_int(cfg.get("home_feed_count"), 40),
                list_id=list_id,
            )
            self.last_channel_stats["主页推荐流"] = len(feed_posts)
            for p in feed_posts:
                if self.is_valuable_post(p):
                    collected_by_id[p["post_id"]] = p

        # 2. 100% 免登全球/美国实时热搜榜 (`getdaytrends` / `trends24`) -> 一次性并发穿透全部热搜词（无 8 词截断、无时间过滤）
        if include_trending and trending_topic_count != 0:
            trends = self.fetch_trending_topics(limit=trending_topic_count)
            trend_names = [t["name"] for t in trends if t.get("name")]
            if trend_names:
                total_trend_target = max(15, len(trend_names) * max(2, tweets_per_trend))
                trend_posts = self.search_or_topic_posts(
                    query="|".join(trend_names),
                    count=total_trend_target,
                    hours_back=0,
                    fetch_comments=False,
                    fetch_external_links=False,
                    download_media=False,
                    _reset_health=False,
                )
                trend_added = 0
                for tp in trend_posts:
                    if self.is_valuable_post(tp):
                        collected_by_id[tp["post_id"]] = tp
                        trend_added += 1
                self.last_channel_stats[f"实时热搜榜({len(trend_names)}词)"] = trend_added
            else:
                self.last_channel_stats["实时热搜榜"] = 0

        # 3. 100% 免登核心垂直大 V 实时时间线（Guest Token + Chrome TLS，不消耗小号配额，不做人为时间丢弃）
        zero_kols: List[str] = []
        for handle in handles:
            clean_h = str(handle).lstrip("@").strip()
            posts = self.fetch_user_posts(clean_h, count=tweets_per_handle, force_burner=False)
            self.last_channel_stats[f"@{clean_h}"] = len(posts)
            if not posts:
                zero_kols.append(f"@{clean_h}")
            for p in posts:
                if self.is_valuable_post(p):
                    collected_by_id[p["post_id"]] = p
            time.sleep(random.uniform(0.10, 0.22))

        if handles and len(zero_kols) >= max(4, len(handles) // 2):
            self.record_health_issue(
                code="X_KOL_BATCH_DEGRADED",
                message=f"多达 {len(zero_kols)}/{len(handles)} 个 X 监控大 V 返回 0 条近期推文 ({', '.join(zero_kols[:6])})，疑似 Guest Token 限流",
                level="warning",
                channel="UserTweets",
            )

        all_posts = list(collected_by_id.values())

        # 4. 仅在小号健康可用时，为互动最高的 Top N（默认 8 条）重点帖单线程慢速补齐评论
        if self.is_burner_usable and top_n_for_comments > 0:
            self._attach_comments_safely(
                all_posts,
                top_n_for_comments=top_n_for_comments,
                max_comments_per_post=10,
            )

        # 5. 对推文中携带的外部新闻/Substack/博客长文链接提取正文统一拼入 text_content
        if fetch_ext_links and all_posts:
            ext_cnt = enrich_posts_with_external_articles(all_posts, proxies=self.proxies)
            if ext_cnt > 0:
                self.last_channel_stats["外链正文富化"] = ext_cnt

        # 6. 统一抹平差异、并发下载原图至本地 `media_downloads/`，并执行数据质量体检
        normalized_posts = self.normalize_batch(all_posts, download_media=download_media)
        self.audit_posts_quality(
            normalized_posts,
            scene="harvest",
            expect_comments=(self.is_burner_usable and top_n_for_comments > 0),
            expect_media_download=download_media,
            max_fresh_age_hours=168,
        )
        return [p for p in normalized_posts if self.is_valuable_post(p)]
