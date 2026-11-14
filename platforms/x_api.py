# -*- coding: utf-8 -*-
"""
X (Twitter) 个人主页帖子内容与数据表现（浏览量/点赞/评论/转发/收藏/引用）拉取与清洗模块。
输出 Schema 与 fetch_binance_feed / fetch_okx_feed 保持 100% 兼容，并支持一键导出 CSV 报表与生成选帖提示词。
"""
import concurrent.futures
import csv
import glob
import json
import logging
import os
import random
import re
import shutil
import sqlite3
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import requests

from common.common_utils import (
    PLATFORM_X,
    PROMPT_DIR,
    clean_post_text,
    create_standard_post_schema,
    download_web_media,
    get_config,
    get_media_save_dir,
    get_requests_proxies,
    get_runtime_file_path,
    read_json,
    safe_int,
    save_json,
    setup_logger,
)
from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import GeneratedArticleManager

setup_logger()
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# ============================================================
# 全局常量与可插拔多账号配置（优先读取 config/config.json）
# ============================================================
PROXIES = get_requests_proxies() or {}
REQUEST_TIMEOUT = 15
MAX_RETRIES = 3
MAX_WORKERS = 10

DEFAULT_SCREEN_NAME = "2434954452qqc"
DEFAULT_ACCOUNT_CONFIG_MAP = {
    "dahao": ("x_dahao_screen_name", "2434954452qqc"),
    "myself": ("x_myself_screen_name", "peterzhu1003"),
}
DEFAULT_FETCH_ACCOUNTS = ["dahao", "myself"]
DEFAULT_PROMPT_ACCOUNTS = ["dahao", "myself"]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MEDIA_SAVE_DIR = get_media_save_dir()

X_WEB_BEARER_TOKEN = (
    "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs"
    "%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
)
X_GQL_USER_BY_SCREEN_NAME = "xmU6X_CKVnQ5lSrCbAmJsg"
X_GQL_USER_TWEETS = "E3opETHurmVJflFsUBVuUQ"

DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

_CASHTAG_RE = re.compile(r"\$([A-Za-z]{2,10})\b")
_HASHTAG_RE = re.compile(r"#([^\s#]+)")
_TCO_URL_RE = re.compile(r"https?://t\.co/[A-Za-z0-9]+")
_TWEET_ID_RE = re.compile(r"(?:status/)?(\d{15,22})")

TZ_CN = timezone(timedelta(hours=8))
_safe_int = safe_int


def get_standard_schema():
    """获取与币安广场 / OKX Orbit 100% 兼容的通用帖子 Schema，额外扩展 X 专属收藏、互动率与账号归属标识"""
    schema = create_standard_post_schema(source=PLATFORM_X, card_type="BUZZ_SHORT")
    schema["publish_time_str"] = ""
    schema["metadata"].update({
        "lang": None,
        "replying_to": None,
        "account_alias": None,
        "account_screen_name": None,
    })
    schema["author"]["followers_count"] = 0
    schema["content"]["text_length"] = 0
    schema["engagement"].update({
        "bookmark_count": 0,
        "engagement_rate_pct": 0.0,
        "bookmark_rate_pct": 0.0,
    })
    return schema


def _clean_text(raw):
    if not raw:
        return ""
    cleaned = _TCO_URL_RE.sub("", str(raw))
    return clean_post_text(cleaned)


def _clean_screen_name(name):
    return str(name or "").lstrip("@").strip()


def _extract_tweet_id(raw):
    text = str(raw or "").strip()
    if not text:
        return ""
    if text.isdigit() and 15 <= len(text) <= 22:
        return text
    m = _TWEET_ID_RE.search(text)
    return m.group(1) if m else ""


def _is_forbidden_myself_dir(path_str):
    """物理隔离守卫：采集与缓存探测绝不允许读取或触碰主号目录 `x_myself`。"""
    if not path_str:
        return False
    norm = os.path.normcase(os.path.abspath(str(path_str).strip()))
    return "x_myself" in norm


def _parse_cookie_dict(cookies_str):
    cookie_dict = {}
    if not cookies_str:
        return cookie_dict
    for part in str(cookies_str).split(";"):
        if "=" in part:
            k, v = part.strip().split("=", 1)
            cookie_dict[k.strip()] = v.strip()
    return cookie_dict


def get_configured_x_account_registry():
    """
    读取可配置的监控账号别名映射 `{alias: clean_screen_name}`：
    1) 自动加载 config/config.json 中的 x_dahao_screen_name / x_myself_screen_name；
    2) 自动合并可选的 x_monitor_accounts 扩展映射，支持随时增删任意监控号。
    """
    registry = {}
    for alias, (cfg_key, fallback_name) in DEFAULT_ACCOUNT_CONFIG_MAP.items():
        try:
            raw_val = get_config(cfg_key, default=fallback_name)
        except Exception:
            raw_val = fallback_name
        clean_sn = _clean_screen_name(raw_val or fallback_name)
        if clean_sn:
            registry[alias.lower()] = clean_sn

    try:
        extra_cfg = get_config("x_monitor_accounts", default=None)
    except Exception:
        extra_cfg = None

    if isinstance(extra_cfg, dict):
        for k, v in extra_cfg.items():
            alias = str(k or "").strip().lower()
            clean_sn = _clean_screen_name(v)
            if alias and clean_sn:
                registry[alias] = clean_sn
    elif isinstance(extra_cfg, list):
        for item in extra_cfg:
            if isinstance(item, dict):
                alias = str(item.get("alias") or "").strip().lower()
                clean_sn = _clean_screen_name(item.get("screen_name"))
                if clean_sn:
                    registry[alias or clean_sn.lower()] = clean_sn
            else:
                clean_sn = _clean_screen_name(item)
                if clean_sn:
                    registry.setdefault(clean_sn.lower(), clean_sn)

    return registry


def get_configured_fetch_accounts():
    """读取配置中要无登录拉取维护的账号列表（支持别名或 @handle）。"""
    try:
        cfg_val = get_config("x_fetch_accounts", default=DEFAULT_FETCH_ACCOUNTS)
    except Exception:
        cfg_val = DEFAULT_FETCH_ACCOUNTS
    return list(cfg_val) if isinstance(cfg_val, (list, tuple)) else [cfg_val]


def get_configured_prompt_accounts():
    """读取配置中要组合拼入提示词的账号列表（支持别名或 @handle）。"""
    try:
        cfg_val = get_config("x_prompt_accounts", default=DEFAULT_PROMPT_ACCOUNTS)
    except Exception:
        cfg_val = DEFAULT_PROMPT_ACCOUNTS
    return list(cfg_val) if isinstance(cfg_val, (list, tuple)) else [cfg_val]


def resolve_x_accounts(accounts=None, default_accounts=None):
    """
    将任意形态的账号入参归一化为去重后的标准列表 `[{"alias": ..., "screen_name": ...}]`。
    支持传入：
    - None：读取 default_accounts 或 config.json 中的 x_fetch_accounts；
    - str：单个别名（如 "myself"）或用户名（如 "@peterzhu1003"）；
    - list/tuple：别名、用户名、或 {"alias": ..., "screen_name": ...} 混合列表；
    - dict：{"dahao": "2434954452qqc", "myself": "peterzhu1003"}。
    """
    registry = get_configured_x_account_registry()
    reverse_registry = {sn.lower(): alias for alias, sn in registry.items()}

    if accounts is None:
        raw_items = default_accounts if default_accounts is not None else get_configured_fetch_accounts()
    elif isinstance(accounts, dict):
        if "screen_name" in accounts:
            raw_items = [accounts]
        else:
            raw_items = [{"alias": k, "screen_name": v} for k, v in accounts.items()]
    elif isinstance(accounts, str):
        raw_items = [accounts]
    else:
        raw_items = list(accounts)

    resolved = []
    seen_handles = set()
    for item in raw_items:
        alias = ""
        raw_handle = ""
        if isinstance(item, dict):
            alias = str(item.get("alias") or "").strip()
            raw_handle = item.get("screen_name") or ""
        else:
            token = str(item or "").strip()
            if not token:
                continue
            token_low = token.lstrip("@").lower()
            if not token.startswith("@") and token_low in registry:
                alias = token_low
                raw_handle = registry[token_low]
            else:
                raw_handle = token

        clean_sn = _clean_screen_name(raw_handle)
        if not clean_sn:
            continue
        sn_low = clean_sn.lower()
        if sn_low in seen_handles:
            continue
        seen_handles.add(sn_low)
        final_alias = alias or reverse_registry.get(sn_low) or clean_sn
        resolved.append({"alias": final_alias, "screen_name": clean_sn})

    return resolved


def _get_post_account_identity(post):
    """提取帖子的规范化账号归属 `(alias, screen_name, display_label)`。"""
    meta = (post or {}).get("metadata") or {}
    author = (post or {}).get("author") or {}
    sn = _clean_screen_name(meta.get("account_screen_name") or author.get("username") or "")
    alias = str(meta.get("account_alias") or "").strip()
    if not alias and sn:
        registry = get_configured_x_account_registry()
        reverse_registry = {v.lower(): k for k, v in registry.items()}
        alias = reverse_registry.get(sn.lower(), "")

    if alias and sn and alias.lower() != sn.lower():
        label = f"{alias}(@{sn})"
    elif sn:
        label = f"@{sn}"
    else:
        label = alias or "unknown"
    return alias, sn, label


def _get_post_account_summary(post):
    """提取客观账号信息 `(account_label, author_name, followers, objective_header)`。"""
    author = (post or {}).get("author") or {}
    _, _, account_label = _get_post_account_identity(post)
    author_name = str(author.get("author_name") or "").strip()
    followers = _safe_int(author.get("followers_count"), 0)
    parts = [f"账号: {account_label}"]
    if author_name:
        parts.append(f"名称: {author_name}")
    parts.append(f"粉丝={followers}")
    return account_label, author_name, followers, " | ".join(parts)


# ============================================================
# 1. 账号基本信息与单帖实时数据拉取
# ============================================================
def fetch_x_user_profile(screen_name=DEFAULT_SCREEN_NAME, proxies=PROXIES):
    """获取 X 账号基础画像数据（粉丝数、总帖数、简介、注册时间、User Rest ID 等）"""
    clean_name = str(screen_name).lstrip("@").strip()
    url = f"https://api.fxtwitter.com/{clean_name}"
    headers = {"User-Agent": DEFAULT_UA, "Accept": "application/json"}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=headers, proxies=proxies, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()
                user_info = data.get("user") or {}
                if user_info:
                    logger.info(
                        f"[X平台/账号画像] 账号画像拉取完成 | 账号: [@{clean_name}] | 昵称: [{user_info.get('name')}] "
                        f"| UID: [{user_info.get('id')}] | 粉丝: [{user_info.get('followers')}] "
                        f"| 总帖: [{user_info.get('tweets')}]"
                    )
                    return user_info
        except Exception as e:
            logger.warning(
                f"[X平台/账号画像] 拉取账号画像异常 | 账号: [@{clean_name}] | 尝试: [{attempt}/{MAX_RETRIES}] "
                f"| 原因: [{e}] | 排查方向: [检查 fxtwitter 接口或网络代理]"
            )
            time.sleep(1.0)
    return {}


def fetch_x_single_post_raw(screen_name, post_id, session=None, proxies=PROXIES):
    """通过 fxtwitter 接口拉取单条 X 帖子的完整实时数据"""
    clean_name = str(screen_name).lstrip("@").strip()
    clean_id = str(post_id).strip()
    url = f"https://api.fxtwitter.com/{clean_name}/status/{clean_id}"
    headers = {"User-Agent": DEFAULT_UA, "Accept": "application/json"}
    client = session if session else requests

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = client.get(url, headers=headers, proxies=proxies, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                jd = resp.json()
                return jd.get("tweet")
            elif resp.status_code == 404:
                return None
        except Exception as e:
            if attempt == MAX_RETRIES:
                logger.debug(f"[X平台/单帖拉取] 拉取单帖失败 | 帖子ID: [{clean_id}] | 原因: [{e}]")
            time.sleep(random.uniform(0.3, 1.0))
    return None


# ============================================================
# 2. 候选帖子 ID 自动发现机制（SSR + 浏览器历史 + 本地缓存库）
# ============================================================
def _get_known_ids_cache_path(screen_name):
    clean_name = str(screen_name).lstrip("@").strip().lower()
    filename = f"x_known_ids_{clean_name}.json"
    legacy_path = _PROJECT_ROOT / "biance" / filename
    return get_runtime_file_path(filename, legacy_path=legacy_path)


def _discover_tweet_ids_from_ssr(screen_name, proxies=PROXIES):
    """从 X 网页端主页与 Jina 镜像页提取最新公开渲染的帖子 ID"""
    clean_name = str(screen_name).lstrip("@").strip()
    found_ids = set()
    headers = {"User-Agent": DEFAULT_UA}

    try:
        resp = requests.get(f"https://x.com/{clean_name}", headers=headers, proxies=proxies, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            for m in re.finditer(r"status/(\d{18,20})", resp.text):
                found_ids.add(m.group(1))
    except Exception as e:
        logger.debug(f"[X平台/SSR探测] x.com 主页请求异常 | 原因: [{e}]")

    try:
        resp_jina = requests.get(
            f"https://r.jina.ai/https://x.com/{clean_name}",
            headers={"User-Agent": DEFAULT_UA, "X-With-Links-Summary": "true", "X-Return-Format": "html"},
            proxies=proxies,
            timeout=20,
        )
        if resp_jina.status_code == 200:
            for m in re.finditer(r"(?:status/|in_reply_to=)(\d{18,20})", resp_jina.text):
                found_ids.add(m.group(1))
    except Exception as e:
        logger.debug(f"[X平台/SSR探测] Jina 快照请求异常 | 原因: [{e}]")

    return found_ids


def _discover_tweet_ids_from_local_browsers(screen_name):
    """只读扫描本机 Chrome / Edge 及 W:\\temp 下持久化浏览器目录的 History 数据库"""
    clean_name = str(screen_name).lstrip("@").strip()
    found_ids = set()

    local_app_data = os.environ.get("LOCALAPPDATA", r"C:\Users\zxh\AppData\Local")
    roots = [
        os.path.join(local_app_data, r"Google\Chrome\User Data"),
        os.path.join(local_app_data, r"Microsoft\Edge\User Data"),
    ]
    if os.path.exists(r"W:\temp"):
        roots.extend(glob.glob(r"W:\temp\*"))

    pattern = re.compile(rf"{re.escape(clean_name)}/status/(\d{{15,22}})", re.IGNORECASE)

    for root in roots:
        # 【保留过滤原因说明】严禁采集逻辑读取或触碰主号浏览器目录 `x_myself`（遵守主号物理隔离红线）。
        if _is_forbidden_myself_dir(root) or not os.path.isdir(root):
            continue
        for sub in ["", "Default", "Profile 1", "Profile 2", "Profile 3"]:
            prof_dir = os.path.join(root, sub) if sub else root
            hist_path = os.path.join(prof_dir, "History")
            if not os.path.exists(hist_path):
                continue
            tmp_db = None
            try:
                fd, tmp_db = tempfile.mkstemp(suffix=".db", prefix="x_hist_")
                os.close(fd)
                shutil.copy2(hist_path, tmp_db)
                conn = sqlite3.connect(tmp_db)
                cur = conn.cursor()
                cur.execute("SELECT url FROM urls WHERE url LIKE ?", (f"%{clean_name}/status/%",))
                for (url,) in cur.fetchall():
                    m = pattern.search(url or "")
                    if m:
                        found_ids.add(m.group(1))
                conn.close()
            except Exception:
                pass
            finally:
                if tmp_db and os.path.exists(tmp_db):
                    try:
                        os.remove(tmp_db)
                    except Exception:
                        pass

    return found_ids


# ============================================================
# 3. 登录态 GraphQL UserTweets 游标翻页拉取（可选增强通道）
# ============================================================
def _get_chromium_cookie_db_mtime(user_data_dir: str) -> float:
    """获取 Chromium 会话目录下 Cookies 数据库文件的最新修改时间（若未初始化则返回 0.0）。"""
    if not user_data_dir or not os.path.isdir(user_data_dir):
        return 0.0
    mtimes = []
    for rel in (
        os.path.join("Default", "Network", "Cookies"),
        os.path.join("Default", "Cookies"),
        os.path.join("Network", "Cookies"),
    ):
        fp = os.path.join(user_data_dir, rel)
        if os.path.isfile(fp):
            try:
                mtimes.append(os.path.getmtime(fp))
            except Exception:
                pass
    return max(mtimes) if mtimes else 0.0


def invalidate_burner_cookie_cache(user_data_dir: str = "") -> None:
    """清理指定会话目录（或全部）的本地 Cookie 缓存（供人工重新登录后立即生效）。"""
    cache_path = get_runtime_file_path("burner_cookies_cache.json")
    if not os.path.exists(cache_path):
        return
    try:
        if not user_data_dir:
            save_json(cache_path, {})
            return
        norm_dir = os.path.normcase(os.path.abspath(str(user_data_dir)))
        data = read_json(cache_path)
        if isinstance(data, dict):
            cleaned = {k: v for k, v in data.items() if not k.startswith(norm_dir + "|")}
            save_json(cache_path, cleaned)
    except Exception:
        pass


def extract_domain_cookies_from_user_data_dir(
    user_data_dir: str,
    urls: list,
    required_any_names: tuple = (),
    ttl_seconds: int = 21600,
    force_refresh: bool = False,
) -> str:
    """
    从指定的 Playwright Chrome `user_data_dir` 安全无头提取指定域名 Cookie，并带基于 Cookies 文件 mtime + TTL 的持久化缓存：
    1) 若会话目录或底层 Cookies 数据库文件尚不存在（例如小号尚未首次登录），0 毫秒直接返回空串，绝不误建空浏览器目录；
    2) 命中缓存时 `<1ms` 毫秒级返回，避免每次实例化 Harvester 反复拉起 Chrome 进程；
    3) 需要提取时统一复用 `_launch_persistent(..., debug=False)` 独占锁与无头模式，零弹窗闪烁、零并发锁冲突。
    """
    if not user_data_dir or not os.path.isdir(user_data_dir) or not urls:
        return ""
    # 【保留过滤原因说明】严禁采集代码读取主号浏览器目录 `x_myself` 的登录态 Cookie（主号物理隔离红线）。
    if _is_forbidden_myself_dir(user_data_dir):
        logger.warning(f"[凭证提取] 检测到尝试读取主号目录 <{user_data_dir}>，已物理拦截并返回空凭证")
        return ""
    cookie_mtime = _get_chromium_cookie_db_mtime(user_data_dir)
    if cookie_mtime <= 0.0:
        return ""

    norm_dir = os.path.normcase(os.path.abspath(str(user_data_dir)))
    cache_key = f"{norm_dir}|{','.join(sorted(str(u) for u in urls))}"
    cache_path = get_runtime_file_path("burner_cookies_cache.json")
    now_ts = time.time()

    if not force_refresh and os.path.exists(cache_path):
        try:
            cache_obj = read_json(cache_path)
            entry = cache_obj.get(cache_key) if isinstance(cache_obj, dict) else None
            if isinstance(entry, dict):
                cached_str = str(entry.get("cookies") or "")
                cached_at = float(entry.get("cached_at") or 0.0)
                cached_mtime = float(entry.get("cookie_mtime") or 0.0)
                # 当底层 Cookie 库文件未变化，或仍在有效期内且已包含关键登录态时直接复用
                if (now_ts - cached_at) < ttl_seconds and (
                    abs(cached_mtime - cookie_mtime) < 1.0
                    or (cached_str and (not required_any_names or any(f"{n}=" in cached_str for n in required_any_names)))
                ):
                    return cached_str
        except Exception:
            pass

    try:
        from playwright.sync_api import sync_playwright
        from platforms.playwright_base import _launch_persistent

        proxy_server = PROXIES.get("http") if isinstance(PROXIES, dict) else None
        with sync_playwright() as p:
            context = _launch_persistent(
                p,
                user_data_dir=user_data_dir,
                args=["--disable-blink-features=AutomationControlled"],
                debug=False,
                proxy=proxy_server,
            )
            try:
                raw_cookies = context.cookies(list(urls))
                cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in raw_cookies if c.get("name"))
            finally:
                context.close()

        new_mtime = _get_chromium_cookie_db_mtime(user_data_dir) or cookie_mtime
        try:
            cache_obj = read_json(cache_path) if os.path.exists(cache_path) else {}
            if not isinstance(cache_obj, dict):
                cache_obj = {}
            cache_obj[cache_key] = {
                "cookies": cookie_str,
                "cookie_mtime": new_mtime,
                "cached_at": now_ts,
            }
            save_json(cache_path, cache_obj)
        except Exception:
            pass
        return cookie_str
    except Exception as e:
        logger.warning(
            f"[凭证提取] 从会话目录提取 Cookie 失败 | 目录: <{user_data_dir}> | 域名: {urls} "
            f"| 原因: [{e}] | 排查方向: [检查浏览器会话目录或关闭外部手工打开的同目录 Chrome]"
        )
        return ""


def extract_x_cookies_from_user_data_dir(user_data_dir, force_refresh: bool = False):
    """从指定的 Playwright Chrome user_data_dir 提取 X (x.com) 登录 Cookie（带无头锁保护与缓存）"""
    return extract_domain_cookies_from_user_data_dir(
        user_data_dir=user_data_dir,
        urls=["https://x.com", "https://twitter.com"],
        required_any_names=("auth_token",),
        force_refresh=force_refresh,
    )


def fetch_x_tweets_via_graphql(user_rest_id, cookies, count=100, include_replies=False, proxies=PROXIES):
    """通过 X 官方 GraphQL UserTweets 接口游标翻页拉取帖子"""
    cookie_dict = _parse_cookie_dict(cookies)
    auth_token = cookie_dict.get("auth_token")
    ct0 = cookie_dict.get("ct0")
    if not auth_token or not ct0:
        logger.warning("[X平台/GraphQL] 传入 Cookie 缺少 auth_token 或 ct0，跳过 GraphQL 通道 | 排查方向: [检查 X 账号登录状态]")
        return []

    headers = {
        "authorization": f"Bearer {X_WEB_BEARER_TOKEN}",
        "cookie": cookies,
        "x-csrf-token": ct0,
        "x-twitter-active-user": "yes",
        "x-twitter-auth-type": "OAuth2Session",
        "x-twitter-client-language": "zh-cn",
        "user-agent": DEFAULT_UA,
        "accept": "*/*",
        "referer": "https://x.com/",
    }

    features = {
        "rweb_tipjar_consumption_enabled": True,
        "responsive_web_graphql_exclude_directive_enabled": True,
        "verified_phone_label_enabled": False,
        "creator_subscriptions_tweet_preview_api_enabled": True,
        "responsive_web_graphql_timeline_navigation_enabled": True,
        "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
        "communities_web_enable_tweet_community_results_fetch": True,
        "c9s_tweet_anatomy_moderator_badge_enabled": True,
        "articles_preview_enabled": True,
        "responsive_web_edit_tweet_api_enabled": True,
        "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
        "view_counts_everywhere_api_enabled": True,
        "longform_notetweets_consumption_enabled": True,
        "responsive_web_twitter_article_tweet_consumption_enabled": True,
        "tweet_awards_web_tipping_enabled": False,
        "creator_subscriptions_quote_tweet_preview_enabled": False,
        "freedom_of_speech_not_reach_fetch_enabled": True,
        "standardized_nudges_misinfo": True,
        "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
        "rweb_video_timestamps_enabled": True,
        "longform_notetweets_rich_text_read_enabled": True,
        "longform_notetweets_inline_media_enabled": True,
        "responsive_web_enhance_cards_enabled": False,
    }

    url = f"https://x.com/i/api/graphql/{X_GQL_USER_TWEETS}/UserTweets"
    discovered_ids = []
    cursor = None
    page = 1

    with requests.Session() as session:
        while len(discovered_ids) < count and page <= 25:
            variables = {
                "userId": str(user_rest_id),
                "count": 20,
                "includePromotedContent": False,
                "withQuickPromoteEligibilityTweetFields": True,
                "withVoice": True,
                "withV2Timeline": True,
            }
            if cursor:
                variables["cursor"] = cursor

            try:
                resp = session.get(
                    url,
                    headers=headers,
                    params={
                        "variables": json.dumps(variables, separators=(",", ":")),
                        "features": json.dumps(features, separators=(",", ":")),
                    },
                    proxies=proxies,
                    timeout=REQUEST_TIMEOUT,
                )
                if resp.status_code != 200:
                    logger.warning(
                        f"[X平台/GraphQL] 分页请求返回非 200 状态 | 页码: [{page}] "
                        f"| HTTP: [{resp.status_code}] | 响应: [{resp.text[:200]}]"
                    )
                    break

                body_text = resp.text
                page_ids = re.findall(r'"rest_id"\s*:\s*"(\d{18,20})"', body_text)
                new_cnt = 0
                for pid in page_ids:
                    if pid != str(user_rest_id) and pid not in discovered_ids:
                        discovered_ids.append(pid)
                        new_cnt += 1

                next_cursors = re.findall(r'"value"\s*:\s*"([^"]+)"\s*,\s*"cursorType"\s*:\s*"Bottom"', body_text)
                if not next_cursors:
                    next_cursors = re.findall(r'"cursorType"\s*:\s*"Bottom"\s*,\s*"value"\s*:\s*"([^"]+)"', body_text)

                logger.debug(
                    f"[X平台/GraphQL] 单页提取完成 | 页码: [{page}] | 新增ID: [{new_cnt}] | 累计ID: [{len(discovered_ids)}]"
                )
                if not next_cursors or new_cnt == 0:
                    break
                cursor = next_cursors[0]
                page += 1
                time.sleep(random.uniform(0.8, 1.8))
            except Exception as e:
                logger.warning(
                    f"[X平台/GraphQL] 分页请求异常 | 页码: [{page}] | 原因: [{e}] | 排查方向: [检查网络代理与 Cookie 有效性]"
                )
                break

    return discovered_ids[:count]


def _tag_posts_account_metadata(posts, account_alias="", screen_name=""):
    """为帖子列表补齐统一的账号归属元数据（account_alias / account_screen_name），便于跨号拼接分析。"""
    registry = get_configured_x_account_registry()
    reverse_registry = {sn.lower(): alias for alias, sn in registry.items()}
    clean_fallback_sn = _clean_screen_name(screen_name)

    tagged = []
    for p in posts or []:
        if not isinstance(p, dict):
            continue
        author = p.setdefault("author", {})
        meta = p.setdefault("metadata", {})
        sn = _clean_screen_name(meta.get("account_screen_name") or author.get("username") or clean_fallback_sn)
        alias = str(account_alias or meta.get("account_alias") or "").strip()
        if not alias and sn:
            alias = reverse_registry.get(sn.lower(), sn)
        if sn:
            meta["account_screen_name"] = sn
            if not author.get("username"):
                author["username"] = sn
        if alias:
            meta["account_alias"] = alias
        tagged.append(p)
    return tagged


# ============================================================
# 4. 数据清洗：转换为通用标准 Schema
# ============================================================
def clean_x_posts(raw_tweets, default_screen_name=None, account_alias=None, fallback_followers=0):
    """将 fxtwitter 返回的原始 tweet 字典列表清洗为统一的 get_standard_schema() 格式，并附带账号归属标识"""
    cleaned_list = []
    registry = get_configured_x_account_registry()
    reverse_registry = {sn.lower(): alias for alias, sn in registry.items()}
    fallback_sn = _clean_screen_name(default_screen_name) or DEFAULT_SCREEN_NAME
    fallback_fol_int = _safe_int(fallback_followers, 0)

    for tw in raw_tweets:
        if not isinstance(tw, dict):
            continue
        post_id = str(tw.get("id") or "").strip()
        if not post_id:
            continue

        item = get_standard_schema()
        author = tw.get("author") or {}
        screen_name = _clean_screen_name(author.get("screen_name")) or fallback_sn
        resolved_alias = str(account_alias or "").strip() or reverse_registry.get(screen_name.lower()) or screen_name

        ts = _safe_int(tw.get("created_timestamp"), 0)
        dt_str = (
            datetime.fromtimestamp(ts, TZ_CN).strftime("%Y-%m-%d %H:%M:%S")
            if ts
            else (tw.get("created_at") or "")
        )

        item["post_id"] = post_id
        item["publish_time"] = ts
        item["publish_time_str"] = dt_str
        item["author_id"] = str(author.get("id") or "") or None

        media_obj = tw.get("media") or {}
        photos = media_obj.get("photos") or []
        videos = media_obj.get("videos") or []

        image_urls = [p.get("url") for p in photos if isinstance(p, dict) and p.get("url")]
        video_url = None
        video_duration = 0
        if videos and isinstance(videos[0], dict):
            video_url = videos[0].get("url")
            video_duration = _safe_int(videos[0].get("duration"), 0)

        if video_url:
            item["card_type"] = "BUZZ_VIDEO"
        elif tw.get("is_note_tweet"):
            item["card_type"] = "BUZZ_LONG"
        else:
            item["card_type"] = "BUZZ_SHORT"

        item["media"].update({
            "cover_image": image_urls[0] if image_urls else (videos[0].get("thumbnail_url") if videos else None),
            "images": image_urls,
            "video_url": video_url,
            "video_duration": video_duration,
        })

        item["metadata"].update({
            "url": tw.get("url") or f"https://x.com/{screen_name}/status/{post_id}",
            "is_ai_generated": False,
            "lang": tw.get("lang"),
            "replying_to": tw.get("replying_to"),
            "account_alias": resolved_alias,
            "account_screen_name": screen_name,
        })
        item["author"].update({
            "username": screen_name,
            "author_name": author.get("name") or screen_name,
            "is_verified": bool(author.get("verification") or author.get("verified")),
            "followers_count": _safe_int(author.get("followers"), 0) or fallback_fol_int,
        })

        raw_text = tw.get("text") or ""
        clean_txt = _clean_text(raw_text)
        coins = sorted(set(m.upper() for m in _CASHTAG_RE.findall(clean_txt)))
        hashtags = sorted(set(_HASHTAG_RE.findall(clean_txt)))

        item["content"].update({
            "title": None,
            "text_content": clean_txt,
            "text_length": len(clean_txt),
            "hashtags": hashtags,
            "mentioned_coins": coins,
        })

        views = _safe_int(tw.get("views"), 0)
        likes = _safe_int(tw.get("likes"), 0)
        replies = _safe_int(tw.get("replies"), 0)
        retweets = _safe_int(tw.get("retweets"), 0)
        quotes = _safe_int(tw.get("quotes"), 0)
        bookmarks = _safe_int(tw.get("bookmarks"), 0)

        total_interactions = likes + replies + retweets + quotes + bookmarks
        eng_rate = round((total_interactions / views) * 100, 2) if views > 0 else 0.0
        bm_rate = round((bookmarks / views) * 100, 2) if views > 0 else 0.0

        item["engagement"].update({
            "view_count": views,
            "like_count": likes,
            "comment_count": replies,
            "share_count": retweets,
            "quote_count": quotes,
            "bookmark_count": bookmarks,
            "engagement_rate_pct": eng_rate,
            "bookmark_rate_pct": bm_rate,
        })

        cleaned_list.append(item)

    cleaned_list.sort(key=lambda x: x["publish_time"], reverse=True)
    return cleaned_list


# ============================================================
# 5. 导出 CSV 报表、单账号拉取与可插拔多账号拼接维护
# ============================================================
def export_x_posts_to_csv(posts, csv_path, sort_by="views"):
    """将清洗后的 X 帖子列表导出为带 UTF-8-SIG BOM 头的 CSV 文件（含发布账号标识）"""
    if not posts:
        logger.warning("[X平台/报表导出] 帖子列表为空，跳过 CSV 导出")
        return None

    sorted_posts = list(posts)
    if sort_by == "views":
        sorted_posts.sort(key=lambda x: x.get("engagement", {}).get("view_count", 0), reverse=True)
    else:
        sorted_posts.sort(key=lambda x: x.get("publish_time", 0), reverse=True)

    os.makedirs(os.path.dirname(os.path.abspath(csv_path)), exist_ok=True)

    headers = [
        "排名",
        "发布账号",
        "帖子ID",
        "发布时间(北京时间)",
        "浏览量(Views)",
        "点赞数(Likes)",
        "评论数(Replies)",
        "转发数(Retweets)",
        "收藏数(Bookmarks)",
        "引用数(Quotes)",
        "综合互动率(%)",
        "收藏率(%)",
        "卡片类型",
        "配图张数",
        "正文字数",
        "提及币种",
        "话题标签",
        "正文内容",
        "帖子链接",
        "配图链接",
    ]

    with open(csv_path, mode="w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for idx, p in enumerate(sorted_posts, 1):
            eng = p.get("engagement") or {}
            cnt = p.get("content") or {}
            med = p.get("media") or {}
            meta = p.get("metadata") or {}
            imgs = med.get("images") or []
            _, _, account_label = _get_post_account_identity(p)
            writer.writerow([
                idx,
                account_label,
                f"'{p.get('post_id')}",
                p.get("publish_time_str", ""),
                eng.get("view_count", 0),
                eng.get("like_count", 0),
                eng.get("comment_count", 0),
                eng.get("share_count", 0),
                eng.get("bookmark_count", 0),
                eng.get("quote_count", 0),
                eng.get("engagement_rate_pct", 0.0),
                eng.get("bookmark_rate_pct", 0.0),
                p.get("card_type", ""),
                len(imgs),
                cnt.get("text_length", 0),
                ",".join(cnt.get("mentioned_coins") or []),
                ",".join(cnt.get("hashtags") or []),
                (cnt.get("text_content") or "").replace("\r\n", "\n"),
                meta.get("url", ""),
                " | ".join(imgs),
            ])

    logger.info(f"[X平台/报表导出] CSV 报表导出完成 | 条数: [{len(sorted_posts)}] | 路径: <{csv_path}>")
    return csv_path


def fetch_x_user_posts(
    screen_name=DEFAULT_SCREEN_NAME,
    count=200,
    cookies="",
    user_data_dir=None,
    extra_tweet_ids=None,
    include_replies=False,
    download_media=False,
    save_csv_path=None,
    save_json_path=None,
    sort_by="views",
    account_alias=None,
):
    """无登录优先一键拉取指定 X 账号的帖子内容与实时数据表现（单账号原子能力）"""
    started = time.monotonic()
    specs = resolve_x_accounts(screen_name)
    clean_name = specs[0]["screen_name"] if specs else _clean_screen_name(screen_name)
    resolved_alias = str(account_alias or (specs[0]["alias"] if specs else "") or clean_name).strip()

    if user_data_dir and _is_forbidden_myself_dir(user_data_dir):
        logger.warning(
            f"[X平台/安全隔离] 检测到传入主号目录 <{user_data_dir}>，已物理拦截并自动切换纯无登录模式"
        )
        user_data_dir = None

    profile = fetch_x_user_profile(clean_name)
    user_rest_id = profile.get("id")

    cache_path = _get_known_ids_cache_path(clean_name)
    cached_ids = set()
    if os.path.exists(cache_path):
        try:
            loaded = read_json(cache_path)
            if isinstance(loaded, list):
                for x in loaded:
                    tid = _extract_tweet_id(x)
                    if tid:
                        cached_ids.add(tid)
        except Exception:
            pass

    candidate_ids = set(cached_ids)
    if extra_tweet_ids:
        for x in extra_tweet_ids:
            tid = _extract_tweet_id(x)
            if tid:
                candidate_ids.add(tid)

    if not cookies and user_data_dir:
        cookies = extract_x_cookies_from_user_data_dir(user_data_dir)

    if cookies and user_rest_id:
        gql_ids = fetch_x_tweets_via_graphql(
            user_rest_id=user_rest_id,
            cookies=cookies,
            count=count,
            include_replies=include_replies,
        )
        candidate_ids.update(gql_ids)

    ssr_ids = _discover_tweet_ids_from_ssr(clean_name)
    hist_ids = _discover_tweet_ids_from_local_browsers(clean_name)
    candidate_ids.update(ssr_ids)
    candidate_ids.update(hist_ids)

    if user_rest_id:
        candidate_ids.discard(str(user_rest_id))
    valid_candidate_ids = sorted(
        [cid for cid in candidate_ids if cid.isdigit() and 15 <= len(cid) <= 22],
        key=lambda x: int(x),
        reverse=True,
    )

    logger.info(
        f"[X平台/候选探测] 候选帖子 ID 汇总完成 | 账号: [{resolved_alias}(@{clean_name})] | 本地缓存: [{len(cached_ids)}] "
        f"| 主页SSR: [{len(ssr_ids)}] | 浏览器历史: [{len(hist_ids)}] | 去重待拉取: [{len(valid_candidate_ids)}]"
    )

    raw_tweets = []
    verified_author_ids = set()

    with requests.Session() as session:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_map = {
                executor.submit(fetch_x_single_post_raw, clean_name, tid, session): tid
                for tid in valid_candidate_ids[: max(count * 2, 100)]
            }
            for future in concurrent.futures.as_completed(future_map):
                tid = future_map[future]
                try:
                    tw = future.result()
                    if not tw:
                        continue
                    author_sn = (tw.get("author") or {}).get("screen_name", "")
                    if author_sn.lower() != clean_name.lower():
                        continue
                    verified_author_ids.add(str(tw.get("id")))
                    if not include_replies and tw.get("replying_to"):
                        continue
                    raw_tweets.append(tw)
                except Exception as e:
                    logger.debug(f"[X平台/单帖处理] 处理推文异常 | 帖子ID: [{tid}] | 原因: [{e}]")

    updated_cache_ids = sorted(cached_ids | verified_author_ids, key=lambda x: int(x), reverse=True)
    save_json(cache_path, updated_cache_ids)

    cleaned_posts = clean_x_posts(
        raw_tweets,
        default_screen_name=clean_name,
        account_alias=resolved_alias,
        fallback_followers=_safe_int(profile.get("followers"), 0),
    )
    if sort_by == "views":
        cleaned_posts.sort(key=lambda x: x["engagement"]["view_count"], reverse=True)
    else:
        cleaned_posts.sort(key=lambda x: x["publish_time"], reverse=True)

    cleaned_posts = cleaned_posts[:count]

    if download_media and cleaned_posts:
        os.makedirs(MEDIA_SAVE_DIR, exist_ok=True)
        for p in cleaned_posts:
            imgs = p.get("media", {}).get("images") or []
            mapping = {}
            for img_url in imgs:
                local_path = download_web_media(img_url, save_dir=MEDIA_SAVE_DIR, proxies=PROXIES)
                if local_path:
                    mapping[img_url] = local_path
            p["media"]["local_mapping"] = mapping

    if save_json_path is None:
        save_json_path = get_runtime_file_path(
            f"x_posts_{clean_name}.json", legacy_path=_PROJECT_ROOT / "biance" / f"x_posts_{clean_name}.json"
        )
    if save_csv_path is None:
        save_csv_path = get_runtime_file_path(
            f"x_posts_{clean_name}.csv", legacy_path=_PROJECT_ROOT / "biance" / f"x_posts_{clean_name}.csv"
        )

    if save_json_path:
        save_json(save_json_path, cleaned_posts)
        logger.debug(f"[X平台/落盘] JSON 数据已保存 | 路径: <{save_json_path}>")

    if save_csv_path:
        export_x_posts_to_csv(cleaned_posts, save_csv_path, sort_by=sort_by)

    total_views = sum(p["engagement"]["view_count"] for p in cleaned_posts)
    total_likes = sum(p["engagement"]["like_count"] for p in cleaned_posts)
    total_replies = sum(p["engagement"]["comment_count"] for p in cleaned_posts)
    total_shares = sum(p["engagement"]["share_count"] for p in cleaned_posts)
    total_bookmarks = sum(p["engagement"]["bookmark_count"] for p in cleaned_posts)
    avg_views = total_views / len(cleaned_posts) if cleaned_posts else 0

    logger.info(
        f"[X平台/采集汇总] 账号帖子与互动数据采集完成 | 账号: [{resolved_alias}(@{clean_name})] | 原创帖: [{len(cleaned_posts)}] "
        f"| 总浏览: [{total_views:,}] | 篇均浏览: [{avg_views:,.1f}] "
        f"| 点赞/评论/转发/收藏: [{total_likes}/{total_replies}/{total_shares}/{total_bookmarks}] "
        f"| 耗时: [{time.monotonic() - started:.2f}s]"
    )

    return cleaned_posts


def load_cached_x_posts(accounts=None, sort_by="time"):
    """直接从本地缓存 JSON 读取指定账号列表的历史帖子（不发起网络请求），并自动补全账号标识"""
    specs = resolve_x_accounts(accounts, default_accounts=get_configured_fetch_accounts())
    posts_by_account = {}

    for spec in specs:
        clean_name = spec["screen_name"]
        alias = spec["alias"]
        json_path = get_runtime_file_path(
            f"x_posts_{clean_name}.json",
            legacy_path=_PROJECT_ROOT / "biance" / f"x_posts_{clean_name}.json",
        )
        posts = []
        if os.path.exists(json_path):
            try:
                loaded = read_json(json_path)
                if isinstance(loaded, list):
                    posts = _tag_posts_account_metadata(loaded, account_alias=alias, screen_name=clean_name)
            except Exception as e:
                logger.warning(f"[X平台/缓存读取] 读取缓存失败 | 账号: [@{clean_name}] | 原因: [{e}]")

        if sort_by == "views":
            posts.sort(key=lambda x: (x.get("engagement") or {}).get("view_count", 0), reverse=True)
        else:
            posts.sort(key=lambda x: x.get("publish_time", 0), reverse=True)
        posts_by_account[clean_name] = posts

    return posts_by_account


def merge_x_account_posts(
    posts_source,
    accounts=None,
    total_count=None,
    sort_by="time",
):
    """
    可拔插账号数据拼接器：
    - 支持传入 `fetch_multi_x_user_posts` / `load_cached_x_posts` 返回的 `{screen_name: posts}` 字典或扁平帖子列表；
    - 支持通过 `accounts` 自由挑选要组合哪些账号（传别名如 ["dahao", "myself"] 或 @handle）；
    - 不做按号配额均衡与题材过滤，直接按时间（或浏览量）全局排序并截取 `total_count` 条客观历史记录。
    """
    selected_specs = resolve_x_accounts(accounts, default_accounts=get_configured_prompt_accounts()) if accounts is not None else []
    selected_sn_set = {s["screen_name"].lower() for s in selected_specs}
    alias_by_sn = {s["screen_name"].lower(): s["alias"] for s in selected_specs}
    total_cap = int(total_count) if (total_count and int(total_count) > 0) else 0

    buckets = []
    if isinstance(posts_source, dict):
        if selected_specs:
            lower_map = {_clean_screen_name(k).lower(): v for k, v in posts_source.items()}
            for spec in selected_specs:
                sn_low = spec["screen_name"].lower()
                raw_posts = lower_map.get(sn_low)
                if raw_posts is None:
                    raw_posts = lower_map.get(spec["alias"].lower()) or []
                buckets.append((spec["alias"], spec["screen_name"], raw_posts))
        else:
            for key, raw_posts in posts_source.items():
                sn_clean = _clean_screen_name(key)
                buckets.append((alias_by_sn.get(sn_clean.lower(), ""), sn_clean, raw_posts or []))
    elif isinstance(posts_source, list):
        for p in _tag_posts_account_metadata(posts_source):
            _, sn, _ = _get_post_account_identity(p)
            sn_low = sn.lower()
            if selected_sn_set and sn_low not in selected_sn_set:
                continue
            buckets.append((alias_by_sn.get(sn_low, ""), sn, [p]))

    merged = []
    seen_keys = set()
    for alias, sn, raw_posts in buckets:
        for p in _tag_posts_account_metadata(list(raw_posts), account_alias=alias, screen_name=sn):
            _, p_sn, _ = _get_post_account_identity(p)
            dedup_key = (p_sn.lower(), str(p.get("post_id") or ""))
            if dedup_key[1] and dedup_key not in seen_keys:
                seen_keys.add(dedup_key)
                merged.append(p)

    if sort_by == "views":
        merged.sort(key=lambda x: (x.get("engagement") or {}).get("view_count", 0), reverse=True)
    else:
        merged.sort(key=lambda x: x.get("publish_time", 0), reverse=True)

    if total_cap > 0:
        merged = merged[:total_cap]
    return merged


def fetch_multi_x_user_posts(
    accounts=None,
    count=200,
    include_replies=False,
    download_media=False,
    sort_by="time",
    extra_tweet_ids_by_account=None,
    save_merged_json_path=None,
    save_merged_csv_path=None,
):
    """
    批量无登录拉取并维护多账号 X 帖子数据（返回 `{screen_name: posts}`）：
    - `accounts` 支持传别名（["dahao", "myself"]）、@handle 或留空读取 config.json 的 x_fetch_accounts；
    - 各账号独立增量维护各自的 `x_known_ids_{handle}.json` / `x_posts_{handle}.json` / `.csv`；
    - 多于 1 个账号时自动额外落盘合并总表 `x_posts_merged.json` / `.csv`（传 `""` 或 `False` 可关闭）。
    """
    specs = resolve_x_accounts(accounts, default_accounts=get_configured_fetch_accounts())
    extra_map = {}
    if isinstance(extra_tweet_ids_by_account, dict):
        for k, v in extra_tweet_ids_by_account.items():
            extra_map[_clean_screen_name(k).lower()] = v

    posts_by_account = {}
    for spec in specs:
        clean_name = spec["screen_name"]
        alias = spec["alias"]
        extra_ids = extra_map.get(clean_name.lower()) or extra_map.get(alias.lower())
        try:
            posts_by_account[clean_name] = fetch_x_user_posts(
                screen_name=clean_name,
                count=count,
                cookies="",
                user_data_dir=None,
                extra_tweet_ids=extra_ids,
                include_replies=include_replies,
                download_media=download_media,
                sort_by=sort_by,
                account_alias=alias,
            )
        except Exception as e:
            logger.warning(
                f"[X平台/多号采集] 拉取账号失败已跳过 | 账号: [{alias}(@{clean_name})] | 原因: [{e}]"
            )
            posts_by_account[clean_name] = []

    if len(specs) > 1 or save_merged_json_path or save_merged_csv_path:
        merged_posts = merge_x_account_posts(posts_by_account, accounts=specs, sort_by=sort_by)
        if save_merged_json_path is None and len(specs) > 1:
            save_merged_json_path = get_runtime_file_path("x_posts_merged.json")
        if save_merged_csv_path is None and len(specs) > 1:
            save_merged_csv_path = get_runtime_file_path("x_posts_merged.csv")
        if save_merged_json_path:
            save_json(save_merged_json_path, merged_posts)
        if save_merged_csv_path and merged_posts:
            export_x_posts_to_csv(merged_posts, save_merged_csv_path, sort_by=sort_by)

    return posts_by_account


def print_x_posts_summary_table(posts, top_n=None, show_full_text=True):
    """在终端直观打印帖子表现与完整文案（含客观账号名称与粉丝信息）"""
    if not posts:
        print("暂无帖子数据。")
        return

    display_posts = posts[:top_n] if top_n else posts
    print("\n" + "=" * 115)
    print(f"📋 X 账号帖子表现与完整文案清单 (共 {len(display_posts)} 条)")
    print("=" * 115)
    for idx, p in enumerate(display_posts, 1):
        eng = p["engagement"]
        cnt = p["content"]
        imgs = p["media"]["images"]
        _, _, _, account_header = _get_post_account_summary(p)
        coins_str = ", ".join(f"${c}" for c in (cnt.get("mentioned_coins") or [])) or "无"
        text_body = cnt.get("text_content") or ""
        if not show_full_text:
            text_body = text_body.replace("\n", " ")[:60] + "..."

        print(
            f"【#{idx:02d}】{account_header} | "
            f"发布时间: {p['publish_time_str']} | ID: {p['post_id']} | 链接: {p['metadata']['url']}\n"
            f" 📊 数据表现: 浏览={eng['view_count']:,} | 点赞={eng['like_count']} | 评论={eng['comment_count']} | "
            f"转发={eng['share_count']} | 收藏={eng['bookmark_count']} | 互动率={eng['engagement_rate_pct']:.2f}% | "
            f"配图={len(imgs)}张 | 字数={cnt['text_length']} | 币种={coins_str}\n"
            f" 📝 完整文案:\n{text_body}"
        )
        print("-" * 115)


DEFAULT_X_PROMPT_TEMPLATE_PATH = str(PROMPT_DIR / "X平台_基于历史表现选帖与改写提示词.txt")


def format_historical_x_posts_for_prompt(
    posts,
    recent_count=50,
    include_account_summary=True,
):
    """将已拉取的 X 帖子列表格式化为纯客观历史数据块（不打绩效或冷启动标签，按时间取最新 N 条并标明账号信息）"""
    if not posts:
        return "暂无历史已发帖子数据。"

    tagged_posts = _tag_posts_account_metadata(list(posts))
    by_time = sorted(tagged_posts, key=lambda x: x.get("publish_time", 0), reverse=True)
    if recent_count and int(recent_count) > 0:
        by_time = by_time[: int(recent_count)]

    account_stats = {}
    blocks = []

    for idx, p in enumerate(by_time, 1):
        eng = p.get("engagement") or {}
        cnt = p.get("content") or {}
        imgs = (p.get("media") or {}).get("images") or []
        views = eng.get("view_count", 0)
        account_label, _, followers, account_header = _get_post_account_summary(p)

        stat = account_stats.setdefault(
            account_label,
            {"header": account_header, "followers": followers, "count": 0, "total_views": 0},
        )
        stat["count"] += 1
        stat["total_views"] += views
        if followers >= stat["followers"]:
            stat["followers"] = followers
            stat["header"] = account_header

        text_body = (cnt.get("text_content") or "").strip()
        blocks.append(
            f"【#{idx:02d}】【{account_header}】"
            f"发布时间: {p.get('publish_time_str', '')} | "
            f"浏览={views:,} | 点赞={eng.get('like_count', 0)} | 评论={eng.get('comment_count', 0)} | "
            f"转发={eng.get('share_count', 0)} | 收藏={eng.get('bookmark_count', 0)} | "
            f"互动率={eng.get('engagement_rate_pct', 0.0):.2f}% | 配图={len(imgs)}张\n"
            f"文案:\n{text_body}"
        )

    header_block = ""
    if include_account_summary and account_stats:
        summary_lines = [
            f"账号客观数据概览（共 {len(account_stats)} 个账号，按发布时间从新到旧共 {len(by_time)} 条最新历史帖）："
        ]
        for st in account_stats.values():
            avg_v = st["total_views"] / st["count"] if st["count"] else 0.0
            summary_lines.append(
                f"- {st['header']} | 本批纳入={st['count']}条 | 篇均浏览={avg_v:,.1f}"
            )
        header_block = "\n".join(summary_lines) + "\n" + ("=" * 90) + "\n"

    return "\n" + header_block + ("\n" + "-" * 90 + "\n").join(blocks) + "\n"


def _format_candidate_posts_literal(obj, level=0):
    """将候选帖子列表格式化为标准 2 空格缩进的 Python 字面量文本"""
    pad = "  " * level
    next_pad = "  " * (level + 1)
    if isinstance(obj, list):
        if not obj:
            return "[]"
        items = [f"{next_pad}{_format_candidate_posts_literal(item, level + 1)}" for item in obj]
        return "[\n" + ",\n".join(items) + f"\n{pad}]"
    if isinstance(obj, dict):
        if not obj:
            return "{}"
        items = [
            f"{next_pad}{repr(k)}: {_format_candidate_posts_literal(v, level + 1)}"
            for k, v in obj.items()
        ]
        return "{\n" + ",\n".join(items) + f"\n{pad}}}"
    return repr(obj)


def fetch_recent_viral_candidates(hours=6, limit=0, only_usable=True):
    """
    查询最近 hours 小时内的高流量重塑帖子 (type='viral')，
    清洗为 [{'remodeled_article': {...}}] 标准候选列表供 X 选帖提示词直接使用。
    """
    try:
        article_manager = GeneratedArticleManager(gen_db_object())
        query = {"type": "viral"}
        if only_usable:
            query["status"] = "ok"
        if hours and hours > 0:
            query["created_at"] = {"$gte": datetime.now(timezone.utc) - timedelta(hours=hours)}

        candidates = article_manager.find_articles(
            query=query,
            sort=[("created_at", -1)],
            limit=limit if (limit and limit > 0) else 0,
        ) or []
    except Exception as e:
        logger.warning(
            f"[X平台/候选选材] 查询近 {hours}h 流量重塑帖异常 | 原因: [{e}] | 排查方向: [检查 MongoDB 连接状态]"
        )
        return []

    cleaned_list = []
    for article in candidates:
        if not isinstance(article, dict):
            continue
        info = article.get("article_info") or {}
        usable = bool(info.get("usable", False))
        if only_usable and not usable:
            continue

        p_list = article.get("post_id_list") or []
        target_post_id = (
            str(p_list[0]).strip()
            if p_list
            else str(
                ((article.get("creation_brief") or {}).get("original_post") or {}).get("post_id")
                or article.get("article_id")
                or ""
            ).strip()
        )

        remodeled_article_data = {
            "usable": usable,
            "id": target_post_id,
            "topic_tag": info.get("topic_tag", ""),
            "viral_score": info.get("viral_score", 0.0),
            "image_semantics": info.get("image_semantics", []),
            "selected_images": info.get("selected_images", []),
            "viral_analysis": info.get("viral_analysis", ""),
            "decision_reason": info.get("decision_reason", ""),
            "image_role": info.get("image_role", ""),
            "post_text": info.get("post_text", ""),
            "bait_comments": info.get("bait_comments", []),
        }
        remodeled_article_data = {
            k: v
            for k, v in remodeled_article_data.items()
            if k == "usable" or v not in (None, "", [], {})
        }
        cleaned_list.append({"remodeled_article": remodeled_article_data})

    logger.info(
        f"[X平台/候选选材] 流量候选帖查询与清洗完成 | 时间窗口: [近{hours}h] "
        f"| 命中记录: [{len(candidates)}] | 有效候选: [{len(cleaned_list)}]"
    )
    return cleaned_list


def build_x_selection_prompt(
    accounts=None,
    screen_name=None,
    recent_count=50,
    candidate_posts=None,
    candidate_hours=6,
    posts_data=None,
    posts_by_account=None,
    use_cached_only=False,
    prompt_template_path=DEFAULT_X_PROMPT_TEMPLATE_PATH,
    save_prompt_path=None,
):
    """
    可拼接生成选帖与改写提示词：
    1) 可通过 `accounts`（或 config.json 的 x_prompt_accounts）自由决定组合哪些监控账号；
    2) 支持直接传入 `posts_by_account`（来自 fetch_multi_x_user_posts / load_cached_x_posts）、
       扁平 `posts_data`，或自动按需拉取/读缓存；
    3) 不区分账号配额与题材冲突，统一按发布时间从新到旧截取最新 `recent_count` 条（默认 50 条）客观记录并标明发帖账号信息。
    """
    explicit_selector = accounts if accounts is not None else screen_name
    target_specs = resolve_x_accounts(
        explicit_selector,
        default_accounts=get_configured_prompt_accounts(),
    )

    if posts_data is not None:
        selected_posts = merge_x_account_posts(
            posts_data,
            accounts=target_specs if explicit_selector is not None else None,
            total_count=recent_count,
            sort_by="time",
        )
    else:
        if posts_by_account is None:
            if use_cached_only:
                posts_by_account = load_cached_x_posts(accounts=target_specs, sort_by="time")
            else:
                fetch_cap = max(_safe_int(recent_count, 50), 100)
                posts_by_account = fetch_multi_x_user_posts(
                    accounts=target_specs,
                    count=fetch_cap,
                    sort_by="time",
                )
        selected_posts = merge_x_account_posts(
            posts_by_account,
            accounts=target_specs,
            total_count=recent_count,
            sort_by="time",
        )

    historical_block = format_historical_x_posts_for_prompt(
        selected_posts,
        recent_count=recent_count,
        include_account_summary=True,
    )

    if candidate_posts is None:
        candidate_posts = fetch_recent_viral_candidates(hours=candidate_hours)

    if isinstance(candidate_posts, str):
        candidate_block = "以下是我待筛选的候选帖子列表：\n" + candidate_posts.strip() + "\n"
    else:
        candidate_block = (
            "以下是我待筛选的候选帖子列表：\n"
            + _format_candidate_posts_literal(candidate_posts)
            + "\n"
        )

    if prompt_template_path and os.path.exists(prompt_template_path):
        with open(prompt_template_path, "r", encoding="utf-8") as f:
            template_str = f.read()
    else:
        raise FileNotFoundError(f"未找到 X 提示词模板文件: {prompt_template_path}")

    full_prompt = template_str.replace("{HISTORICAL_X_POSTS}", historical_block.strip()).replace(
        "{CANDIDATE_POSTS}", candidate_block.strip()
    )

    if save_prompt_path is None:
        if len(target_specs) == 1:
            prompt_tag = target_specs[0]["screen_name"]
            legacy_path = _PROJECT_ROOT / "biance" / f"x_selection_prompt_{prompt_tag}.txt"
        elif 1 < len(target_specs) <= 3:
            prompt_tag = "_".join(s["alias"] for s in target_specs)
            legacy_path = None
        else:
            prompt_tag = "merged"
            legacy_path = None
        save_prompt_path = get_runtime_file_path(
            f"x_selection_prompt_{prompt_tag}.txt",
            legacy_path=legacy_path,
        )

    if save_prompt_path:
        with open(save_prompt_path, "w", encoding="utf-8") as f:
            f.write(full_prompt)
        candidate_cnt = len(candidate_posts) if isinstance(candidate_posts, list) else "自定义"
        accounts_desc = ", ".join(f"{s['alias']}(@{s['screen_name']})" for s in target_specs) or "自定义"
        logger.info(
            f"[X平台/提示词组装] 选帖与改写完整提示词已生成 | 组合账号: [{accounts_desc}] "
            f"| 历史表现帖: [{len(selected_posts)}] "
            f"| 流量候选帖: [{candidate_cnt} (近{candidate_hours}h)] | 保存路径: <{save_prompt_path}>"
        )

    return full_prompt


if __name__ == "__main__":
    # 阶段一：按配置无登录拉取并维护目标账号列表（默认读取 config/config.json 的 x_fetch_accounts）
    fetch_accounts = get_configured_fetch_accounts()
    # 阶段二：按配置选取要汇总拼入提示词的账号列表（默认读取 config/config.json 的 x_prompt_accounts）
    prompt_accounts = get_configured_prompt_accounts()

    posts_by_acc = fetch_multi_x_user_posts(
        accounts=fetch_accounts,
        count=200,
        sort_by="time",
    )
    # 阶段三：按时间从新到旧截取全局最新 50 条客观数据并组装提示词
    full_prompt_text = build_x_selection_prompt(
        accounts=prompt_accounts,
        recent_count=50,
        candidate_posts=None,
        candidate_hours=6,
        posts_by_account=posts_by_acc,
    )
    print("\n" + "=" * 115)
    print(f"🚀 已生成拼接好【账号 {prompt_accounts} 最新 50 条客观历史数据 + 近 6 小时流量候选帖】的完整提示词：")
    print("=" * 115 + "\n")
    print(full_prompt_text)

