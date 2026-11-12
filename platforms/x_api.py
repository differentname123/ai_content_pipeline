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
# 全局常量配置（统一从 common_utils 获取代理与目录）
# ============================================================
PROXIES = get_requests_proxies() or {}
REQUEST_TIMEOUT = 15
MAX_RETRIES = 3
MAX_WORKERS = 10

DEFAULT_SCREEN_NAME = "2434954452qqc"
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

TZ_CN = timezone(timedelta(hours=8))
_safe_int = safe_int


def get_standard_schema():
    """获取与币安广场 / OKX Orbit 100% 兼容的通用帖子 Schema，额外扩展 X 专属收藏与互动率指标"""
    schema = create_standard_post_schema(source=PLATFORM_X, card_type="BUZZ_SHORT")
    schema["publish_time_str"] = ""
    schema["metadata"].update({
        "lang": None,
        "replying_to": None,
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


def _parse_cookie_dict(cookies_str):
    cookie_dict = {}
    if not cookies_str:
        return cookie_dict
    for part in str(cookies_str).split(";"):
        if "=" in part:
            k, v = part.strip().split("=", 1)
            cookie_dict[k.strip()] = v.strip()
    return cookie_dict


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
        if not os.path.isdir(root):
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


# ============================================================
# 4. 数据清洗：转换为通用标准 Schema
# ============================================================
def clean_x_posts(raw_tweets):
    """将 fxtwitter 返回的原始 tweet 字典列表清洗为统一的 get_standard_schema() 格式"""
    cleaned_list = []

    for tw in raw_tweets:
        if not isinstance(tw, dict):
            continue
        post_id = str(tw.get("id") or "").strip()
        if not post_id:
            continue

        item = get_standard_schema()
        author = tw.get("author") or {}
        screen_name = author.get("screen_name") or DEFAULT_SCREEN_NAME
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
        })
        item["author"].update({
            "username": screen_name,
            "author_name": author.get("name") or screen_name,
            "is_verified": bool(author.get("verification") or author.get("verified")),
            "followers_count": _safe_int(author.get("followers"), 0),
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
# 5. 导出 CSV 报表与一键拉取主入口
# ============================================================
def export_x_posts_to_csv(posts, csv_path, sort_by="views"):
    """将清洗后的 X 帖子列表导出为带 UTF-8-SIG BOM 头的 CSV 文件"""
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
            writer.writerow([
                idx,
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
):
    """一键拉取指定 X 账号的帖子内容与实时数据表现"""
    started = time.monotonic()
    clean_name = str(screen_name).lstrip("@").strip()

    profile = fetch_x_user_profile(clean_name)
    user_rest_id = profile.get("id")

    cache_path = _get_known_ids_cache_path(clean_name)
    cached_ids = set()
    if os.path.exists(cache_path):
        try:
            loaded = read_json(cache_path)
            if isinstance(loaded, list):
                cached_ids.update(str(x) for x in loaded if x)
        except Exception:
            pass

    candidate_ids = set(cached_ids)
    if extra_tweet_ids:
        candidate_ids.update(str(x) for x in extra_tweet_ids if x)

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
        f"[X平台/候选探测] 候选帖子 ID 汇总完成 | 账号: [@{clean_name}] | 本地缓存: [{len(cached_ids)}] "
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

    cleaned_posts = clean_x_posts(raw_tweets)
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
        f"[X平台/采集汇总] 账号帖子与互动数据采集完成 | 账号: [@{clean_name}] | 原创帖: [{len(cleaned_posts)}] "
        f"| 总浏览: [{total_views:,}] | 篇均浏览: [{avg_views:,.1f}] "
        f"| 点赞/评论/转发/收藏: [{total_likes}/{total_replies}/{total_shares}/{total_bookmarks}] "
        f"| 耗时: [{time.monotonic() - started:.2f}s]"
    )

    return cleaned_posts


def print_x_posts_summary_table(posts, top_n=None, show_full_text=True):
    """在终端直观打印帖子表现与完整文案"""
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
        coins_str = ", ".join(f"${c}" for c in (cnt.get("mentioned_coins") or [])) or "无"
        text_body = cnt.get("text_content") or ""
        if not show_full_text:
            text_body = text_body.replace("\n", " ")[:60] + "..."

        print(
            f"【#{idx:02d}】发布时间: {p['publish_time_str']} | ID: {p['post_id']} | 链接: {p['metadata']['url']}\n"
            f" 📊 数据表现: 浏览={eng['view_count']:,} | 点赞={eng['like_count']} | 评论={eng['comment_count']} | "
            f"转发={eng['share_count']} | 收藏={eng['bookmark_count']} | 互动率={eng['engagement_rate_pct']:.2f}% | "
            f"配图={len(imgs)}张 | 字数={cnt['text_length']} | 币种={coins_str}\n"
            f" 📝 完整文案:\n{text_body}"
        )
        print("-" * 115)


DEFAULT_X_PROMPT_TEMPLATE_PATH = str(PROMPT_DIR / "X平台_基于历史表现选帖与改写提示词.txt")


def format_historical_x_posts_for_prompt(posts, recent_count=50):
    """将已拉取的 X 帖子列表提取最近 recent_count 条，格式化为高信息密度文本块"""
    if not posts:
        return "暂无历史已发帖子数据。"

    by_time = sorted(posts, key=lambda x: x.get("publish_time", 0), reverse=True)[:recent_count]
    now_ts = int(time.time())

    blocks = []
    for idx, p in enumerate(by_time, 1):
        eng = p.get("engagement") or {}
        cnt = p.get("content") or {}
        imgs = (p.get("media") or {}).get("images") or []
        views = eng.get("view_count", 0)
        pub_ts = p.get("publish_time", 0)
        age_hours = max(0.0, (now_ts - pub_ts) / 3600.0) if pub_ts else 999.0

        if age_hours < 6 and views < 500:
            perf_tag = "⏳[刚发布冷启动中]"
        elif views >= 10000:
            perf_tag = "🔥[万级爆款]"
        elif views >= 1000:
            perf_tag = "📈[千级破圈]"
        elif views < 200:
            perf_tag = "❄️[低迷池<200]"
        else:
            perf_tag = "💧[基础池]"

        text_body = (cnt.get("text_content") or "").strip()
        blocks.append(
            f"【#{idx:02d}】{perf_tag} 发布时间: {p.get('publish_time_str', '')} | "
            f"浏览={views:,} | 点赞={eng.get('like_count', 0)} | 评论={eng.get('comment_count', 0)} | "
            f"转发={eng.get('share_count', 0)} | 收藏={eng.get('bookmark_count', 0)} | "
            f"互动率={eng.get('engagement_rate_pct', 0.0):.2f}% | 配图={len(imgs)}张\n"
            f"文案:\n{text_body}"
        )

    return "\n" + ("\n" + "-" * 90 + "\n").join(blocks) + "\n"


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
    screen_name=DEFAULT_SCREEN_NAME,
    recent_count=50,
    candidate_posts=None,
    candidate_hours=6,
    posts_data=None,
    prompt_template_path=DEFAULT_X_PROMPT_TEMPLATE_PATH,
    save_prompt_path=None,
):
    """拼接【账号最近 50 条 X 帖子真实表现数据】+【提示词规则】+【近 6 小时流量候选帖子数据】"""
    clean_name = str(screen_name).lstrip("@").strip()
    if posts_data is None:
        posts_data = fetch_x_user_posts(screen_name=clean_name, count=max(recent_count, 100), sort_by="time")

    historical_block = format_historical_x_posts_for_prompt(posts_data, recent_count=recent_count)

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
        save_prompt_path = get_runtime_file_path(
            f"x_selection_prompt_{clean_name}.txt",
            legacy_path=_PROJECT_ROOT / "biance" / f"x_selection_prompt_{clean_name}.txt",
        )

    if save_prompt_path:
        with open(save_prompt_path, "w", encoding="utf-8") as f:
            f.write(full_prompt)
        candidate_cnt = len(candidate_posts) if isinstance(candidate_posts, list) else "自定义"
        logger.info(
            f"[X平台/提示词组装] 选帖与改写完整提示词已生成 | 账号: [@{clean_name}] "
            f"| 历史表现帖: [{min(recent_count, len(posts_data))}] "
            f"| 流量候选帖: [{candidate_cnt} (近{candidate_hours}h)] | 保存路径: <{save_prompt_path}>"
        )

    return full_prompt


if __name__ == "__main__":
    target_account = "2434954452qqc"
    all_x_posts = fetch_x_user_posts(
        screen_name=target_account,
        count=200,
        sort_by="time",
    )
    full_prompt_text = build_x_selection_prompt(
        screen_name=target_account,
        recent_count=50,
        candidate_posts=None,
        candidate_hours=6,
        posts_data=all_x_posts,
    )
    print("\n" + "=" * 115)
    print("🚀 已为您生成拼接好【最近 50 条 X 帖子数据 + 近 6 小时流量候选帖】的完整提示词：")
    print("=" * 115 + "\n")
    print(full_prompt_text)

