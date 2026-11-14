# -*- coding: utf-8 -*-
"""
模块 1A：真实桌面 Chrome 纯拉取落库模块 (`x_growth/x_browser_collector.py`)

高内聚 · 低耦合原则：
1. **唯一职责**：只负责打开真实桌面 Chrome 模拟自然浏览，将顺路见到的全部原创主帖与热评 100% 无损存入 `harvested_social_posts`；
2. **零大模型耦合**：不导入任何 LLM / 提示词 / 评分模块，不管评论生成；
3. **零裸 HTTP 请求**：严禁调用 `requests` / `curl_cffi` / `XHarvester`，仅通过 `page.on("response")` 旁路接收官方前端签名的 GraphQL JSON；
4. **支持独立直接运行**：在 IDE 中直接右键运行本文件即可单独完成帖子拉取入库（或独立常驻定时拉取）。
"""
import json
import os
import random
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple
from urllib.parse import unquote

from playwright.sync_api import sync_playwright

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.common_utils import (
    PLATFORM_X,
    clean_post_text,
    create_standard_post_schema,
    get_proxy_url,
    get_runtime_file_path,
    normalize_and_enrich_post_schema,
    normalize_comment_item,
    safe_int,
)
from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import HarvestedPostManager
from platforms.external_link_extractor import is_fetchable_external_article_url
from platforms.playwright_base import (
    USER_DATA_LOCK_TIMEOUT_SEC,
    _dismiss_overlays,
    _launch_persistent,
    _user_data_dir_lock,
    check_for_crash,
    install_overlay_guard,
)
from x_growth.growth_config import (
    COLLECTOR_DEFAULTS,
    POSTS_COLLECTION_NAME,
    SUPPORTED_COMMENT_LANGUAGES,
    TZ_CN,
    get_configured_self_handles,
    get_growth_logger,
    normalize_language_code,
)

X_HOME_URL = "https://x.com/home"

_RE_GQL_OP = re.compile(
    r"/graphql/[^/]+/(HomeTimeline|HomeLatestTimeline|TweetDetail|UserOriginalsTimeline|UserTweets|UserTweetsAndReplies|UserByScreenName)(?:\?|$)",
    re.IGNORECASE,
)
_RE_FOCAL_ID = re.compile(r'"focalTweetId"\s*:\s*"(\d+)"')
_X_UNDETERMINED_LANGS = {"", "unknown", "und", "qme", "qam", "zxx", "art"}


def build_real_chrome_launch_options(browser_visible: bool = False) -> Tuple[List[str], Optional[Dict[str, int]]]:
    """
    构造真实桌面 Chrome 启动参数：
    - `browser_visible=False`（默认）：启用现代 Chrome `--headless=new` 原生无头引擎（不传 `--disable-gpu`，
      100% 保留本机真实物理显卡 D3D11 WebGL 指纹，同时不创建任何系统桌面窗口、零任务栏图标、100% 零焦点抢占）；
    - `browser_visible=True`：显示正常最大化窗口便于人工观察。
    """
    args = [
        "--disable-blink-features=AutomationControlled",
        "--no-sandbox",
        "--disable-infobars",
        "--disable-dev-shm-usage",
        "--mute-audio",
    ]
    if browser_visible:
        args.append("--start-maximized")
        return args, None
    args.extend(
        [
            "--headless=new",
            "--window-size=1440,900",
            "--disable-renderer-backgrounding",
            "--disable-background-timer-throttling",
            "--disable-backgrounding-occluded-windows",
        ]
    )
    return args, {"width": 1440, "height": 900}


def apply_stealth_headless_emulation(context: Any, page: Any, browser_visible: bool = False) -> Optional[Any]:
    """
    在 `--headless=new` 模式下，于导航前通过 CDP 将默认 `HeadlessChrome/<ver>` 动态还原为本机真实 `Chrome/<ver>`，
    同步对齐 HTTP `User-Agent`、`Sec-CH-UA*` 请求头与 `navigator.userAgentData`，返回需保持存活的 CDP 会话对象。
    """
    if browser_visible:
        return None
    try:
        raw_ua = str(page.evaluate("() => navigator.userAgent") or "")
        clean_ua = raw_ua.replace("HeadlessChrome/", "Chrome/")
        m_ver = re.search(r"Chrome/([\d.]+)", clean_ua)
        ver_full = m_ver.group(1) if m_ver else "135.0.0.0"
        ver_major = ver_full.split(".", 1)[0]

        if sys.platform == "darwin":
            nav_platform, ch_platform, ch_platform_ver, arch = "MacIntel", "macOS", "14.5.0", "arm"
        elif sys.platform.startswith("linux"):
            nav_platform, ch_platform, ch_platform_ver, arch = "Linux x86_64", "Linux", "6.8.0", "x86"
        else:
            nav_platform, ch_platform, ch_platform_ver, arch = "Win32", "Windows", "15.0.0", "x86"

        cdp = context.new_cdp_session(page)
        cdp.send(
            "Emulation.setUserAgentOverride",
            {
                "userAgent": clean_ua,
                "platform": nav_platform,
                "userAgentMetadata": {
                    "brands": [
                        {"brand": "Google Chrome", "version": ver_major},
                        {"brand": "Not-A.Brand", "version": "8"},
                        {"brand": "Chromium", "version": ver_major},
                    ],
                    "fullVersionList": [
                        {"brand": "Google Chrome", "version": ver_full},
                        {"brand": "Not-A.Brand", "version": "8.0.0.0"},
                        {"brand": "Chromium", "version": ver_full},
                    ],
                    "fullVersion": ver_full,
                    "platform": ch_platform,
                    "platformVersion": ch_platform_ver,
                    "architecture": arch,
                    "model": "",
                    "mobile": False,
                    "bitness": "64",
                    "wow64": False,
                },
            },
        )
        return cdp
    except Exception:
        return None


def _clean_x_text_with_entities(raw_text: str, legacy: Dict[str, Any]) -> str:
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


def _extract_gql_tweet_objects(data: Any, skip_pinned: bool = False) -> List[Dict[str, Any]]:
    found: List[Dict[str, Any]] = []

    def walk(obj: Any) -> None:
        if isinstance(obj, dict):
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


def extract_user_profile_from_gql_result(res: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(res, dict) or res.get("__typename") == "UserUnavailable":
        return None
    uleg = res.get("legacy") or {}
    ucore = res.get("core") or {}
    rel_counts = res.get("relationship_counts") or {}
    rel_persp = res.get("relationship_perspectives") or {}
    bio_obj = res.get("profile_bio") or {}
    tw_counts = res.get("tweet_counts") or {}

    uid = str(res.get("rest_id") or "").strip()
    screen_name = str(ucore.get("screen_name") or uleg.get("screen_name") or "").strip()
    if not uid or not screen_name:
        return None

    is_blue_only = bool(res.get("is_blue_verified"))
    is_verified = bool(
        is_blue_only
        or (res.get("verification") or {}).get("verified")
        or uleg.get("verified")
    )
    return {
        "user_id": uid,
        "screen_name": screen_name,
        "name": str(ucore.get("name") or uleg.get("name") or screen_name).strip(),
        "is_blue_verified": is_blue_only,
        "is_verified": is_verified,
        "followers_count": safe_int(
            rel_counts.get("followers") if "followers" in rel_counts else uleg.get("followers_count"),
            0,
        ),
        "following_count": safe_int(
            rel_counts.get("following") if "following" in rel_counts else uleg.get("friends_count"),
            0,
        ),
        "statuses_count": safe_int(
            tw_counts.get("tweets") if "tweets" in tw_counts else uleg.get("statuses_count"),
            0,
        ),
        "description": clean_post_text(str(bio_obj.get("description") or uleg.get("description") or "")),
        "is_following": bool(rel_persp.get("following") if "following" in rel_persp else uleg.get("following")),
        "is_followed_by": bool(
            rel_persp.get("followed_by") if "followed_by" in rel_persp else uleg.get("followed_by")
        ),
        "checked_at_ts": int(time.time()),
    }


def convert_gql_tweet_node(
    tw: Dict[str, Any],
    fallback_screen_name: str = "",
    category: str = "x_browser_feed",
) -> Optional[Dict[str, Any]]:
    """纯内存解析：将浏览器被动捕获的 X GraphQL Tweet 节点转换为标准 `harvested_social_posts` 结构。"""
    legacy = tw.get("legacy") or {}
    rest_id = str(tw.get("rest_id") or legacy.get("id_str") or "").strip()
    if not rest_id:
        return None

    core_user = (((tw.get("core") or {}).get("user_results") or {}).get("result")) or {}
    prof_snap = extract_user_profile_from_gql_result(core_user)
    user_legacy = core_user.get("legacy") or {}

    screen_name = (prof_snap or {}).get("screen_name") or fallback_screen_name or "unknown"
    author_name = (prof_snap or {}).get("name") or screen_name
    author_uid = (prof_snap or {}).get("user_id") or str(legacy.get("user_id_str") or "")
    followers_count = safe_int((prof_snap or {}).get("followers_count"), 0)
    is_blue = bool((prof_snap or {}).get("is_verified"))

    if prof_snap is None:
        prof_snap = {
            "user_id": author_uid,
            "screen_name": screen_name,
            "name": author_name,
            "is_blue_verified": bool(core_user.get("is_blue_verified")),
            "is_verified": is_blue,
            "followers_count": followers_count,
            "following_count": safe_int(user_legacy.get("friends_count"), 0),
            "statuses_count": safe_int(user_legacy.get("statuses_count"), 0),
            "description": "",
            "is_following": bool(user_legacy.get("following")),
            "is_followed_by": bool(user_legacy.get("followed_by")),
            "checked_at_ts": int(time.time()),
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
    images: List[str] = []
    main_images: List[str] = []
    video_url: Optional[str] = None
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
            else:
                video_url = m.get("media_url_https") or "https://x.com/i/videos/tweet"
            video_duration = int(safe_int((m.get("video_info") or {}).get("duration_millis"), 0) / 1000)

    quoted_res = ((tw.get("quoted_status_result") or {}).get("result")) or {}
    if quoted_res.get("__typename") == "TweetWithVisibilityResults":
        quoted_res = quoted_res.get("tweet") or {}
    quoted_data = None
    if isinstance(quoted_res, dict) and quoted_res.get("legacy"):
        q_leg = quoted_res["legacy"]
        q_core_user = (((quoted_res.get("core") or {}).get("user_results") or {}).get("result")) or {}
        q_user_leg = q_core_user.get("legacy") or {}
        q_uname = (q_core_user.get("core") or {}).get("screen_name") or q_user_leg.get("screen_name") or ""
        q_note = (((quoted_res.get("note_tweet") or {}).get("note_tweet_results") or {}).get("result") or {}).get("text")
        q_txt = _clean_x_text_with_entities(q_note or q_leg.get("full_text") or "", q_leg)
        q_media = (q_leg.get("extended_entities") or {}).get("media") or (q_leg.get("entities") or {}).get("media") or []
        q_images: List[str] = []
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
                if not video_url:
                    video_url = qm_img or "https://x.com/i/videos/quoted"
        quoted_data = {
            "post_id": str(quoted_res.get("rest_id") or ""),
            "username": q_uname,
            "text": q_txt,
            "likes": safe_int(q_leg.get("favorite_count"), 0),
            "images": q_images,
            "has_video": q_has_video,
        }
        if q_txt and q_txt not in clean_txt:
            clean_txt = f"{clean_txt}\n[引用 @{q_uname}]: {q_txt}".strip()

    item = create_standard_post_schema(
        source=PLATFORM_X,
        card_type="BUZZ_VIDEO" if video_url else ("BUZZ_LONG" if note_text else "BUZZ_SHORT"),
    )
    item["post_id"] = rest_id
    item["publish_time"] = legacy.get("created_at", "")
    item["author_id"] = author_uid or None
    item["author"].update(
        {
            "username": screen_name,
            "author_name": author_name,
            "is_verified": is_blue,
            "followers_count": followers_count,
        }
    )
    item["content"].update(
        {
            "title": None,
            "text_content": clean_txt,
            "hashtags": hashtags,
            "mentioned_coins": mentioned_coins,
        }
    )
    item["media"].update(
        {
            "cover_image": images[0] if images else None,
            "images": images,
            "video_url": video_url,
            "video_duration": video_duration,
        }
    )
    item["engagement"].update(
        {
            "view_count": safe_int((tw.get("views") or {}).get("count"), 0),
            "like_count": safe_int(legacy.get("favorite_count"), 0),
            "comment_count": safe_int(legacy.get("reply_count"), 0),
            "share_count": safe_int(legacy.get("retweet_count"), 0),
            "quote_count": safe_int(legacy.get("quote_count"), 0),
            "bookmark_count": safe_int(legacy.get("bookmark_count"), 0),
        }
    )
    meta_payload: Dict[str, Any] = {
        "url": f"https://x.com/{screen_name}/status/{rest_id}",
        "lang": legacy.get("lang"),
        "category": category,
        "is_retweet": bool(legacy.get("retweeted_status_result")),
        "is_reply": bool(legacy.get("in_reply_to_status_id_str")),
        "main_images": main_images,
        "quoted_post": quoted_data,
        "author_profile": prof_snap,
    }
    if external_url:
        meta_payload["external_url"] = external_url
    item["metadata"].update(meta_payload)
    return normalize_and_enrich_post_schema(item, download_media=False)


class XPassiveGraphQLSniffer:
    """只读旁路监听器：挂载于 `page.on("response")`，零侵入提取 X 官方前端自然加载的 GraphQL JSON。"""

    def __init__(self) -> None:
        self.op_counts: Dict[str, int] = {}
        self.feed_posts_by_id: Dict[str, Dict[str, Any]] = {}
        self.detailed_posts_by_id: Dict[str, Dict[str, Any]] = {}
        self.user_posts_by_handle: Dict[str, List[Dict[str, Any]]] = {}
        self.users_by_handle: Dict[str, Dict[str, Any]] = {}

    def register_user(
        self,
        prof: Optional[Dict[str, Any]],
        source_role: str = "feed",
        post_lang: str = "",
    ) -> None:
        if not prof or not prof.get("screen_name"):
            return
        h_low = str(prof["screen_name"]).strip().lstrip("@").lower()
        if not h_low or h_low == "unknown":
            return
        prev = self.users_by_handle.get(h_low) or {}
        merged = dict(prev)
        obs_langs: Set[str] = set(merged.get("observed_langs") or set())
        norm_lang = normalize_language_code(post_lang)
        if norm_lang and norm_lang not in _X_UNDETERMINED_LANGS:
            obs_langs.add(norm_lang)
        merged.update(prof)
        merged["observed_langs"] = obs_langs
        if not merged.get("source_role") or source_role == "comment_author":
            merged["source_role"] = source_role
        self.users_by_handle[h_low] = merged

    def on_response(self, resp: Any) -> None:
        try:
            url = resp.url or ""
            m = _RE_GQL_OP.search(url)
            if not m or resp.status != 200:
                return
            op = m.group(1)
            payload = resp.json()
            if not isinstance(payload, dict):
                return

            if op in ("HomeTimeline", "HomeLatestTimeline"):
                self._ingest_home_timeline(payload)
            elif op == "TweetDetail":
                self._ingest_tweet_detail(payload, resp_url=url)
            elif op == "UserByScreenName":
                self._ingest_user_by_screen_name(payload)
            elif op in ("UserOriginalsTimeline", "UserTweets", "UserTweetsAndReplies"):
                self._ingest_user_tweets(payload)

            self.op_counts[op] = self.op_counts.get(op, 0) + 1
        except Exception:
            pass

    def _ingest_home_timeline(self, payload: Dict[str, Any]) -> None:
        nodes = _extract_gql_tweet_objects(payload, skip_pinned=False)
        for node in nodes:
            leg = node.get("legacy") or {}
            if leg.get("retweeted_status_result"):
                continue
            post = convert_gql_tweet_node(node, category="x_browser_home")
            if not post:
                continue
            pid = str(post.get("post_id") or "")
            if pid:
                self.feed_posts_by_id[pid] = post
            prof = ((post.get("metadata") or {}).get("platform_extra") or {}).get("author_profile")
            p_lang = str(((post.get("metadata") or {}).get("lang")) or "")
            self.register_user(prof, source_role="post_author", post_lang=p_lang)

    def _ingest_user_by_screen_name(self, payload: Dict[str, Any]) -> None:
        res = (((payload.get("data") or {}).get("user") or {}).get("result")) or {}
        prof = extract_user_profile_from_gql_result(res)
        if prof:
            self.register_user(prof, source_role="profile_lookup")

    def _ingest_user_tweets(self, payload: Dict[str, Any]) -> None:
        nodes = _extract_gql_tweet_objects(payload, skip_pinned=True)
        for node in nodes:
            leg = node.get("legacy") or {}
            if leg.get("retweeted_status_result") or leg.get("in_reply_to_status_id_str"):
                continue
            post = convert_gql_tweet_node(node, category="x_browser_usertweets")
            if not post:
                continue
            prof = ((post.get("metadata") or {}).get("platform_extra") or {}).get("author_profile")
            p_lang = str(((post.get("metadata") or {}).get("lang")) or "")
            self.register_user(prof, source_role="profile_author", post_lang=p_lang)
            handle = str((post.get("author") or {}).get("username") or "").strip().lstrip("@").lower()
            if not handle or handle == "unknown":
                continue
            bucket = self.user_posts_by_handle.setdefault(handle, [])
            if all(p.get("post_id") != post.get("post_id") for p in bucket):
                bucket.append(post)

    def _ingest_tweet_detail(self, payload: Dict[str, Any], resp_url: str = "") -> None:
        nodes = _extract_gql_tweet_objects(payload, skip_pinned=False)
        if not nodes:
            return

        url_focal_id = ""
        if resp_url:
            m_fid = _RE_FOCAL_ID.search(unquote(resp_url))
            if m_fid:
                url_focal_id = m_fid.group(1)

        parsed_nodes: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
        for node in nodes:
            p = convert_gql_tweet_node(node, category="x_browser_detail")
            if p:
                parsed_nodes.append((node, p))

        if not parsed_nodes:
            return

        focal_post: Optional[Dict[str, Any]] = None
        if url_focal_id:
            for _node, p in parsed_nodes:
                if str(p.get("post_id") or "") == url_focal_id:
                    focal_post = p
                    break
        if focal_post is None:
            for node, p in parsed_nodes:
                if not (node.get("legacy") or {}).get("in_reply_to_status_id_str"):
                    focal_post = p
                    break
        if focal_post is None:
            focal_post = parsed_nodes[0][1]

        focal_id = str(focal_post.get("post_id") or "")
        focal_prof = ((focal_post.get("metadata") or {}).get("platform_extra") or {}).get("author_profile")
        f_lang = str(((focal_post.get("metadata") or {}).get("lang")) or "")
        self.register_user(focal_prof, source_role="post_author", post_lang=f_lang)

        raw_replies: List[Dict[str, Any]] = []
        seen_rids: Set[str] = {focal_id}
        for node, p in parsed_nodes:
            rid = str(p.get("post_id") or "")
            if not rid or rid in seen_rids:
                continue
            if not (node.get("legacy") or {}).get("in_reply_to_status_id_str"):
                continue
            seen_rids.add(rid)
            c_prof = ((p.get("metadata") or {}).get("platform_extra") or {}).get("author_profile")
            c_lang = str(((p.get("metadata") or {}).get("lang")) or "")
            self.register_user(c_prof, source_role="comment_author", post_lang=c_lang)
            raw_replies.append(
                {
                    "reply_id": rid,
                    "parent_id": focal_id,
                    "create_time": p.get("publish_time"),
                    "author_uid": (p.get("author") or {}).get("username"),
                    "author_name": (p.get("author") or {}).get("author_name"),
                    "content": (p.get("content") or {}).get("text_content"),
                    "likes": (p.get("engagement") or {}).get("like_count", 0),
                    "replies": (p.get("engagement") or {}).get("comment_count", 0),
                    "views": (p.get("engagement") or {}).get("view_count", 0),
                    "images": (p.get("media") or {}).get("images") or [],
                }
            )

        if raw_replies:
            focal_username = str((focal_post.get("author") or {}).get("username") or "").strip().lower()
            author_self_replies = [
                r for r in raw_replies if focal_username and str(r.get("author_uid") or "").strip().lower() == focal_username
            ]
            other_replies = [
                r for r in raw_replies if not (focal_username and str(r.get("author_uid") or "").strip().lower() == focal_username)
            ]
            author_self_replies.sort(key=lambda x: safe_int(x.get("create_time"), 0))
            other_replies.sort(key=lambda x: safe_int(x.get("likes"), 0), reverse=True)

            norm_cmts: List[Dict[str, Any]] = []
            seen_ckeys: Set[str] = set()
            for rc in author_self_replies + other_replies[:15]:
                nc = normalize_comment_item(rc, parent_post_id=focal_id)
                if not nc:
                    continue
                if focal_username and str(nc.get("author_uid") or "").strip().lower() == focal_username:
                    nc["is_post_author"] = True
                ckey = str(nc.get("reply_id") or nc.get("content", "")[:80])
                if ckey not in seen_ckeys:
                    seen_ckeys.add(ckey)
                    norm_cmts.append(nc)
            focal_post["comments"] = norm_cmts

        if focal_id:
            self.detailed_posts_by_id[focal_id] = focal_post
            if focal_id in self.feed_posts_by_id:
                self.feed_posts_by_id[focal_id] = focal_post

    def export_all_captured_root_posts(
        self,
        *,
        skip_peer_authors: Optional[Set[str]] = None,
        max_posts_per_author: int = 0,
        peer_follower_range: Optional[Tuple[int, int]] = None,
    ) -> List[Dict[str, Any]]:
        """汇总结算本轮顺路捕获到的非转推、非回复原创主帖（含 24h 已拉取目标作者避让与单作者均匀上限）。"""
        merged_by_id: Dict[str, Dict[str, Any]] = {}
        skip_set: Set[str] = {str(x).strip().lstrip("@").lower() for x in (skip_peer_authors or set()) if x}
        per_author_cap = max(0, int(max_posts_per_author))
        peer_author_counts: Dict[str, int] = {}

        def _is_peer_range_post(post_obj: Dict[str, Any]) -> bool:
            if not peer_follower_range:
                return False
            low_fc, high_fc = int(peer_follower_range[0]), int(peer_follower_range[1])
            au = post_obj.get("author") or {}
            fc = safe_int(au.get("followers_count"), 0)
            return bool(au.get("is_verified")) and (low_fc <= fc <= high_fc)

        def _put(post_obj: Optional[Dict[str, Any]], *, is_user_profile_source: bool = False, is_clicked_detail: bool = False) -> None:
            if not isinstance(post_obj, dict):
                return
            pid = str(post_obj.get("post_id") or "").strip()
            if not pid:
                return
            meta = post_obj.get("metadata") or {}
            pextra = meta.get("platform_extra") or {}
            # 【保留过滤原因说明】转推与评论回复并非独立原创主帖，仅保留根主帖入库
            if bool(meta.get("is_retweet") or pextra.get("is_retweet")) or bool(
                meta.get("is_reply") or pextra.get("is_reply")
            ):
                return

            h_low = str((post_obj.get("author") or {}).get("username") or "").strip().lstrip("@").lower()
            enforce_peer_uniformity = (not is_clicked_detail) and (is_user_profile_source or _is_peer_range_post(post_obj))
            if enforce_peer_uniformity and h_low:
                # 【保留过滤原因说明】24h 内已拉取过该同体量作者帖子则直接避让不再重复拉取，使落库作者分布均匀
                if h_low in skip_set and pid not in merged_by_id:
                    return
                # 【保留过滤原因说明】单次访问新作者主页 X 接口会返回约 20 条推文，限制单作者单次最多保留最新 N 条，防止高频水贴号单人挤占评论池
                if per_author_cap > 0 and pid not in merged_by_id and peer_author_counts.get(h_low, 0) >= per_author_cap:
                    return

            existing = merged_by_id.get(pid)
            if existing is None:
                merged_by_id[pid] = post_obj
                if enforce_peer_uniformity and h_low:
                    peer_author_counts[h_low] = peer_author_counts.get(h_low, 0) + 1
            elif post_obj.get("comments") and not existing.get("comments"):
                merged_by_id[pid] = post_obj

        for dp in self.detailed_posts_by_id.values():
            _put(dp, is_clicked_detail=True)

        for handle, u_list in self.user_posts_by_handle.items():
            for up in sorted(u_list, key=lambda x: safe_int(x.get("publish_time"), 0), reverse=True):
                _put(up, is_user_profile_source=True)

        sorted_feed = sorted(
            self.feed_posts_by_id.values(),
            key=lambda x: safe_int(x.get("publish_time"), 0),
            reverse=True,
        )
        for fp in sorted_feed:
            _put(fp, is_user_profile_source=False)

        out = list(merged_by_id.values())
        out.sort(key=lambda x: safe_int(x.get("publish_time"), 0), reverse=True)
        return out

    def wait_for_op_increment(self, page: Any, ops: Sequence[str], prev_total: int, timeout_ms: int = 12000) -> bool:
        deadline = time.time() + timeout_ms / 1000.0
        while time.time() < deadline:
            if sum(self.op_counts.get(op, 0) for op in ops) > prev_total:
                return True
            page.wait_for_timeout(250)
        return False


def inspect_x_page_account_status(page: Any) -> Tuple[str, Optional[str]]:
    """检查当前真实 Chrome 页面是否处于正常登录状态。"""
    cur_url = str(page.url or "").lower()
    if "/account/access" in cur_url or "arkoselabs" in cur_url:
        return (
            "locked_challenge",
            f"当前账号处于 X 安全验证解锁页 ({page.url})，请打开该浏览器目录人工完成一次解锁后再运行。",
        )
    if "/i/flow/login" in cur_url or cur_url.rstrip("/").endswith("/login"):
        return (
            "not_logged_in",
            f"当前浏览器目录未登录 X 账号 ({page.url})，请先登录保存会话后再运行。",
        )

    for sel in ('iframe[src*="arkoselabs"]', 'iframe[id*="arkose"]', 'form[action*="/account/access"]'):
        try:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible():
                return (
                    "locked_challenge",
                    "页面检测到 X 安全验证挑战组件 (Arkose / account access)，请先打开浏览器目录人工解锁。",
                )
        except Exception:
            continue

    for sel in ('input[autocomplete="username"]', 'a[data-testid="loginButton"]'):
        try:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible():
                return (
                    "not_logged_in",
                    f"页面检测到未登录组件 ({sel})，该浏览器目录登录态已失效，请重新登录。",
                )
        except Exception:
            continue

    return "ok", None


def extract_logged_in_identity(context: Any, page: Any) -> Tuple[str, str]:
    """纯浏览器内提取当前登录账号的 UID 与 @screen_name。"""
    self_uid = ""
    self_handle = ""
    try:
        for ck in context.cookies(["https://x.com"]):
            if ck.get("name") == "twid":
                raw_twid = unquote(str(ck.get("value") or "")).strip().strip('"')
                self_uid = raw_twid[2:] if raw_twid.startswith("u=") else raw_twid
                break
    except Exception:
        pass

    try:
        prof_link = page.locator('a[data-testid="AppTabBar_Profile_Link"]').first
        if prof_link.count() > 0:
            href = str(prof_link.get_attribute("href") or "").strip().strip("/")
            if href and "/" not in href and not href.startswith("i"):
                self_handle = href.lower()
    except Exception:
        pass

    return self_uid, self_handle


def _ensure_for_you_tab(page: Any) -> None:
    """确保首页停留在第一个推荐标签页 (For You)。"""
    try:
        first_tab = page.locator('div[role="tablist"] div[role="presentation"] a[role="tab"]').first
        if first_tab.count() > 0 and first_tab.is_visible():
            if str(first_tab.get_attribute("aria-selected") or "").lower() == "false":
                first_tab.click(timeout=3000)
                page.wait_for_timeout(random.randint(1500, 2600))
    except Exception:
        pass


_VISITED_AUTHORS_RUNTIME_FILE = "x_growth_visited_authors.json"


def _load_recent_visited_authors(avoid_seconds: int) -> Set[str]:
    """读取最近 `avoid_seconds`（默认 24h）内访问过主页的作者名单（含主页无新帖的纯评论号，防重复空跑）。"""
    if avoid_seconds <= 0:
        return set()
    p = get_runtime_file_path(_VISITED_AUTHORS_RUNTIME_FILE)
    if not p.is_file():
        return set()
    now_ts = int(time.time())
    cutoff = now_ts - int(avoid_seconds)
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return set()
        return {
            str(k).strip().lstrip("@").lower()
            for k, ts in raw.items()
            if str(k).strip() and safe_int(ts, 0) >= cutoff
        }
    except Exception:
        return set()


def _record_visited_authors(handles: Sequence[str], avoid_seconds: int) -> None:
    """持久化记录本轮已访问过主页的作者时间戳，并自动清理超过 `avoid_seconds` 的过期条目。"""
    clean_handles = [str(h).strip().lstrip("@").lower() for h in (handles or []) if str(h).strip()]
    if not clean_handles or avoid_seconds <= 0:
        return
    p = get_runtime_file_path(_VISITED_AUTHORS_RUNTIME_FILE)
    now_ts = int(time.time())
    cutoff = now_ts - int(avoid_seconds)
    merged: Dict[str, int] = {}
    if p.is_file():
        try:
            old_data = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(old_data, dict):
                for k, ts in old_data.items():
                    k_low = str(k).strip().lstrip("@").lower()
                    ts_int = safe_int(ts, 0)
                    if k_low and ts_int >= cutoff:
                        merged[k_low] = ts_int
        except Exception:
            pass
    for h_low in clean_handles:
        merged[h_low] = now_ts
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp_p = p.with_suffix(".tmp")
        tmp_p.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_p.replace(p)
    except Exception:
        pass


def _get_light_browse_hints(
    col: Any,
    cutoff_ts: int,
    min_followers: int,
    max_followers: int,
    author_avoid_seconds: int = 86400,
) -> Tuple[Set[str], Set[str]]:
    """
    轻量 24h 去重避让提示：
    1. `skip_detailed_pids`：已有评论列表的近期热帖，避免每次点开同一条热帖看评论；
    2. `skip_recent_authors`：24h 内已经拉取过帖子入库或已访问过主页的目标作者，本轮直接避让不再重复拉取。
    """
    detail_docs = col.find(
        {"source": "x", "publish_time": {"$gte": cutoff_ts - 2 * 86400}, "comments.0": {"$exists": True}},
        {"post_id": 1, "_id": 0},
    ).limit(1500)
    skip_detailed_pids = {str(d.get("post_id")) for d in detail_docs if d.get("post_id")}

    now_ts = int(time.time())
    eff_avoid_sec = max(0, int(author_avoid_seconds))
    avoid_cutoff_ts = now_ts - eff_avoid_sec
    avoid_cutoff_utc = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=eff_avoid_sec)

    skip_recent_authors: Set[str] = _load_recent_visited_authors(eff_avoid_sec)
    if eff_avoid_sec > 0:
        recent_usernames = col.distinct(
            "author.username",
            {
                "source": "x",
                "author.is_verified": True,
                "author.followers_count": {
                    "$gte": max(0, int(min_followers) - 50),
                    "$lte": int(max_followers) + 200,
                },
                "$or": [
                    {"publish_time": {"$gte": avoid_cutoff_ts}},
                    {"db_update_time": {"$gte": avoid_cutoff_utc}},
                    {"metadata.platform_extra.author_profile.checked_at_ts": {"$gte": avoid_cutoff_ts}},
                ],
            },
        )
        for u_name in recent_usernames or []:
            h_low = str(u_name or "").strip().lstrip("@").lower()
            if h_low:
                skip_recent_authors.add(h_low)

    return skip_detailed_pids, skip_recent_authors


def persist_browsed_posts_to_db(
    post_manager: HarvestedPostManager,
    col: Any,
    all_posts: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    将顺路浏览捕获到的全部原创主帖无损沉淀至 `harvested_social_posts`：
    - 新帖：完整写入正文、配图、热评与作者客观画像；
    - 已存在的旧帖：仅追加 `engagement_history` 互动快照并合并新评论，绝不覆盖 `auto_comment`、`pipeline` 或已富化的外链长文。
    """
    if not all_posts:
        return {"total": 0, "new_count": 0, "updated_count": 0, "cost_ms": 0.0}

    dedup_map: Dict[str, Dict[str, Any]] = {}
    for p in all_posts:
        if not isinstance(p, dict):
            continue
        pid = str(p.get("post_id") or "").strip()
        if not pid:
            continue
        prev = dedup_map.get(pid)
        if prev is None or (p.get("comments") and not prev.get("comments")):
            dedup_map[pid] = p

    posts_list = list(dedup_map.values())
    pids = list(dedup_map.keys())
    existing_docs = list(
        col.find(
            {"source": "x", "post_id": {"$in": pids}},
            {"post_id": 1, "content": 1, "media.images": 1, "metadata": 1, "_id": 0},
        )
    )
    existing_by_pid = {str(d.get("post_id")): d for d in existing_docs if d.get("post_id")}

    for item in posts_list:
        pid = str(item.get("post_id") or "")
        old_doc = existing_by_pid.get(pid)
        if not old_doc:
            continue

        old_meta = old_doc.get("metadata") or {}
        cur_meta = item.setdefault("metadata", {})
        old_pextra = old_meta.get("platform_extra") or {}
        cur_pextra = cur_meta.setdefault("platform_extra", {})

        old_cat = str(old_pextra.get("category") or old_meta.get("category") or "").strip()
        cur_cat = str(cur_pextra.get("category") or cur_meta.get("category") or "").strip()
        if old_cat and not old_cat.startswith("x_browser_") and cur_cat.startswith("x_browser_"):
            cur_pextra["category"] = old_cat

        if old_pextra.get("external_link_enriched"):
            old_txt = str((old_doc.get("content") or {}).get("text_content") or "")
            cur_txt = str((item.get("content") or {}).get("text_content") or "")
            if len(old_txt) > len(cur_txt):
                item.setdefault("content", {})["text_content"] = old_txt
            old_imgs = list((old_doc.get("media") or {}).get("images") or [])
            if old_imgs:
                cur_imgs = list((item.setdefault("media", {})).get("images") or [])
                for u in old_imgs:
                    if u not in cur_imgs:
                        cur_imgs.append(u)
                item["media"]["images"] = cur_imgs
            for k, v in old_pextra.items():
                if k not in cur_pextra:
                    cur_pextra[k] = v

    return post_manager.upsert_posts(posts_list)


def collect_posts_via_browser(
    user_data_dir: str,
    *,
    min_followers: int = 100,
    max_followers: int = 3000,
    allowed_langs_set: Set[str],
    skip_detailed_pids: Optional[Set[str]] = None,
    skip_recent_authors: Optional[Set[str]] = None,
    browser_visible: bool = False,
    home_scroll_rounds: int = 4,
    thread_clicks: int = 3,
    profile_visits: int = 6,
    author_avoid_seconds: int = 86400,
    max_posts_per_author: int = 2,
    avoid_connected_authors: bool = True,
) -> Dict[str, Any]:
    """
    极简 3 步真人浏览收集（纯返回捕获列表，不碰大模型）：
    1) 打开 `x.com/home` 慢速滚屏 `home_scroll_rounds` 次，被动接收推荐流热帖；
    2) 点开 `thread_clicks` 条英/日活跃热帖，顺便收下高赞热评 + 发现评论区里的 `100~3000` 粉活跃蓝 V；
    3) 点进最多 `profile_visits` 个 24h 内未拉取过的 `100~3000` 粉蓝 V 主页，均匀收下他们自己发的最近原创主帖。
    """
    logger = get_growth_logger()
    session_path = str(Path(user_data_dir).expanduser().resolve())
    seen_detail_pids: Set[str] = set(skip_detailed_pids or set())
    skip_authors: Set[str] = {str(x).strip().lstrip("@").lower() for x in (skip_recent_authors or set()) if x}

    if not os.path.isdir(session_path):
        return {
            "account_status": "not_logged_in",
            "account_message": f"浏览器会话目录不存在: {session_path}",
            "self_handle": "",
            "all_captured_posts": [],
            "peer_users_discovered": 0,
            "threads_clicked": 0,
            "profiles_visited": 0,
        }

    proxy_server = get_proxy_url()
    chrome_args, viewport_cfg = build_real_chrome_launch_options(browser_visible=browser_visible)
    sniffer = XPassiveGraphQLSniffer()
    self_handles: Set[str] = set(get_configured_self_handles())

    with _user_data_dir_lock(session_path, timeout=USER_DATA_LOCK_TIMEOUT_SEC):
        with sync_playwright() as p:
            context = None
            page = None
            try:
                context = _launch_persistent(
                    p,
                    session_path,
                    args=chrome_args,
                    viewport=viewport_cfg,
                    hide_automation=True,
                    debug=True,
                    proxy=proxy_server,
                )
                context.set_default_timeout(45000)
                context.set_default_navigation_timeout(45000)

                page = context.pages[0] if context.pages else context.new_page()
                for extra_p in list(context.pages):
                    if extra_p != page:
                        try:
                            extra_p.close()
                        except Exception:
                            pass

                _stealth_cdp = apply_stealth_headless_emulation(context, page, browser_visible=browser_visible)
                page.on("response", sniffer.on_response)
                install_overlay_guard(page)

                # ── 第 1 步：刷首页推荐流 ──
                prev_home_ops = sniffer.op_counts.get("HomeTimeline", 0) + sniffer.op_counts.get("HomeLatestTimeline", 0)
                page.goto(X_HOME_URL, timeout=45000, wait_until="domcontentloaded")
                sniffer.wait_for_op_increment(page, ("HomeTimeline", "HomeLatestTimeline"), prev_home_ops, timeout_ms=15000)
                page.wait_for_timeout(random.randint(1800, 3200))
                _dismiss_overlays(page, aggressive=False, desc="x-growth-home")
                check_for_crash(page)

                acc_status, acc_msg = inspect_x_page_account_status(page)
                if acc_status != "ok":
                    return {
                        "account_status": acc_status,
                        "account_message": acc_msg,
                        "self_handle": "",
                        "all_captured_posts": [],
                        "peer_users_discovered": 0,
                        "threads_clicked": 0,
                        "profiles_visited": 0,
                    }

                self_uid, self_handle = extract_logged_in_identity(context, page)
                if self_handle:
                    self_handles.add(self_handle.lower())

                _ensure_for_you_tab(page)
                try:
                    page.mouse.move(720, 450)
                except Exception:
                    pass
                for _ in range(max(1, int(home_scroll_rounds))):
                    page.mouse.wheel(0, random.randint(2000, 3200))
                    page.wait_for_timeout(random.randint(2000, 3500))

                # ── 第 2 步：顺路随机点开几条有讨论的英/日帖子（不按最高评论数排序，避开千评骂战帖） ──
                eligible_threads = [
                    fp
                    for fp in sniffer.feed_posts_by_id.values()
                    if str(fp.get("post_id") or "") not in seen_detail_pids
                    and (
                        not allowed_langs_set
                        or normalize_language_code(str(((fp.get("metadata") or {}).get("lang")) or "")) in allowed_langs_set
                    )
                    and safe_int((fp.get("engagement") or {}).get("comment_count"), 0) >= 4
                ]
                # 【保留过滤原因说明】优先从 <=500 评的常规讨论帖中随机点开看评论（若无则退回 >=4 评），避免一头扎进几千评的极端引战帖；不影响主帖全量入库
                active_threads = [
                    fp for fp in eligible_threads
                    if safe_int((fp.get("engagement") or {}).get("comment_count"), 0) <= 500
                ] or eligible_threads
                random.shuffle(active_threads)
                clicked_threads = 0
                for t_post in active_threads[: max(0, int(thread_clicks))]:
                    t_url = str((t_post.get("metadata") or {}).get("url") or "").strip()
                    if not t_url:
                        continue
                    prev_det = sniffer.op_counts.get("TweetDetail", 0)
                    try:
                        page.goto(t_url, timeout=35000, wait_until="domcontentloaded")
                        sniffer.wait_for_op_increment(page, ("TweetDetail",), prev_det, timeout_ms=10000)
                        page.wait_for_timeout(random.randint(1500, 2600))
                        page.mouse.wheel(0, random.randint(350, 650))
                        page.wait_for_timeout(random.randint(1200, 2000))
                        clicked_threads += 1
                    except Exception as nav_e:
                        logger.debug(f"[PostCollector] 打开热帖详情略过 ({t_url}): {nav_e}")

                # ── 第 3 步：点进客观符合 100~3000 粉的英/日蓝 V 主页，收集他们自己发的原创主帖 ──
                def _is_objective_peer_user(u: Dict[str, Any]) -> bool:
                    if not bool(u.get("is_blue_verified") or u.get("is_verified")):
                        return False
                    uid = str(u.get("user_id") or "").strip()
                    h_low = str(u.get("screen_name") or "").strip().lstrip("@").lower()
                    if not h_low or h_low == "unknown":
                        return False
                    # 【保留过滤原因说明】排除采集号自身与主号，以及 24h 内已拉取过帖子或访问过主页的作者，保证拉取作者均匀不重复
                    if (self_uid and uid == self_uid) or (h_low in self_handles) or (h_low in skip_authors):
                        return False
                    # 【保留过滤原因说明】避让已关注/已关注当前号的熟人，将有限主页访问配额全部留给未触达新作者
                    if avoid_connected_authors and (bool(u.get("is_following")) or bool(u.get("is_followed_by"))):
                        return False
                    fc = safe_int(u.get("followers_count"), 0)
                    if fc < min_followers or fc > max_followers:
                        return False
                    obs_langs: Set[str] = set(u.get("observed_langs") or set())
                    if obs_langs and allowed_langs_set and not (obs_langs & allowed_langs_set):
                        return False
                    return True

                peer_users = [u for u in sniffer.users_by_handle.values() if _is_objective_peer_user(u)]
                random.shuffle(peer_users)

                visited_profiles = 0
                visited_handles: List[str] = []
                user_timeline_ops = ("UserOriginalsTimeline", "UserTweets", "UserTweetsAndReplies")
                for u_prof in peer_users[: max(0, int(profile_visits))]:
                    handle = str(u_prof.get("screen_name") or "").strip().lstrip("@")
                    if not handle:
                        continue
                    visited_profiles += 1
                    visited_handles.append(handle.lower())
                    prev_ut = sum(sniffer.op_counts.get(op_k, 0) for op_k in user_timeline_ops)
                    try:
                        page.goto(f"https://x.com/{handle}", timeout=35000, wait_until="domcontentloaded")
                        sniffer.wait_for_op_increment(page, user_timeline_ops, prev_ut, timeout_ms=11000)
                        page.wait_for_timeout(random.randint(1800, 3000))
                    except Exception as prof_e:
                        logger.debug(f"[PostCollector] 访问用户主页略过 (@{handle}): {prof_e}")

                if visited_handles and author_avoid_seconds > 0:
                    _record_visited_authors(visited_handles, author_avoid_seconds)

                all_captured_posts = sniffer.export_all_captured_root_posts(
                    skip_peer_authors=skip_authors,
                    max_posts_per_author=max_posts_per_author,
                    peer_follower_range=(min_followers, max_followers),
                )
                logger.info(
                    f"[PostCollector/浏览完成] 采集号=@{self_handle or self_uid or '已登录'} | "
                    f"gql_ops={sniffer.op_counts} | 24h避让作者={len(skip_authors)}人 | 点热帖={clicked_threads}条 | "
                    f"发现新100~3000粉蓝V={len(peer_users)}人(进主页={visited_profiles}人) | "
                    f"共捕获原创主帖={len(all_captured_posts)}条(单作者上限={max_posts_per_author}条)"
                )
                return {
                    "account_status": "ok",
                    "account_message": None,
                    "self_handle": self_handle,
                    "all_captured_posts": all_captured_posts,
                    "peer_users_discovered": len(peer_users),
                    "threads_clicked": clicked_threads,
                    "profiles_visited": visited_profiles,
                }
            finally:
                if page is not None:
                    try:
                        page.remove_listener("response", sniffer.on_response)
                    except Exception:
                        pass
                if context is not None:
                    try:
                        context.close()
                    except Exception:
                        pass


def collect_and_save_posts(
    user_data_dir: str = COLLECTOR_DEFAULTS["user_data_dir"],
    *,
    min_followers: int = COLLECTOR_DEFAULTS["min_followers"],
    max_followers: int = COLLECTOR_DEFAULTS["max_followers"],
    browser_visible: bool = COLLECTOR_DEFAULTS["browser_visible"],
    home_scroll_rounds: int = COLLECTOR_DEFAULTS["home_scroll_rounds"],
    thread_clicks: int = COLLECTOR_DEFAULTS["thread_clicks_per_run"],
    profile_visits: int = COLLECTOR_DEFAULTS["profile_visits_per_run"],
    author_avoid_seconds: int = COLLECTOR_DEFAULTS["author_avoid_seconds"],
    max_posts_per_author: int = COLLECTOR_DEFAULTS["max_posts_per_author_per_fetch"],
    avoid_connected_authors: bool = COLLECTOR_DEFAULTS["avoid_connected_authors"],
    post_max_age_seconds: int = COLLECTOR_DEFAULTS["post_max_age_seconds"],
    allowed_languages: Sequence[str] = COLLECTOR_DEFAULTS["allowed_languages"],
) -> Dict[str, Any]:
    """
    高内聚独立拉取入口：启动真实桌面 Chrome 完成浏览并将捕获主帖与热评落库，24h 内已拉取作者自动避让，不触发任何大模型。
    """
    logger = get_growth_logger()
    t0 = time.perf_counter()
    session_path = str(Path(user_data_dir).expanduser().resolve())
    allowed_langs_set = {
        normalize_language_code(x)
        for x in (allowed_languages or SUPPORTED_COMMENT_LANGUAGES)
        if normalize_language_code(x) != "unknown"
    }

    db = gen_db_object()
    post_manager = HarvestedPostManager(db, collection_name=POSTS_COLLECTION_NAME)
    col = db.get_collection(POSTS_COLLECTION_NAME)
    cutoff_ts = int(time.time()) - max(0, int(post_max_age_seconds))

    skip_detailed_pids, skip_recent_authors = _get_light_browse_hints(
        col=col,
        cutoff_ts=cutoff_ts,
        min_followers=min_followers,
        max_followers=max_followers,
        author_avoid_seconds=author_avoid_seconds,
    )

    browser_res = collect_posts_via_browser(
        user_data_dir=session_path,
        min_followers=min_followers,
        max_followers=max_followers,
        allowed_langs_set=allowed_langs_set,
        skip_detailed_pids=skip_detailed_pids,
        skip_recent_authors=skip_recent_authors,
        browser_visible=browser_visible,
        home_scroll_rounds=home_scroll_rounds,
        thread_clicks=thread_clicks,
        profile_visits=profile_visits,
        author_avoid_seconds=author_avoid_seconds,
        max_posts_per_author=max_posts_per_author,
        avoid_connected_authors=avoid_connected_authors,
    )

    acc_status = str(browser_res.get("account_status") or "ok")
    acc_warning = browser_res.get("account_message")
    if acc_status != "ok":
        logger.error(f"🚨 [PostCollector/账号状态阻断] status={acc_status} | {acc_warning}")
        return {
            "status": acc_status,
            "user_data_dir": session_path,
            "account_warning": acc_warning,
            "self_handle": "",
            "browsed_posts_saved": 0,
            "browsed_new_in_db": 0,
            "browsed_updated_in_db": 0,
            "peer_users_discovered": 0,
            "threads_clicked": 0,
            "profiles_visited": 0,
            "elapsed_seconds": round(time.perf_counter() - t0, 2),
        }

    upsert_stats = persist_browsed_posts_to_db(
        post_manager=post_manager,
        col=col,
        all_posts=browser_res.get("all_captured_posts") or [],
    )
    elapsed = round(time.perf_counter() - t0, 2)
    logger.info(
        f"[PostCollector/入库完成] 耗时={elapsed}s | 顺路沉淀主帖={upsert_stats.get('total', 0)}条 "
        f"(新增={upsert_stats.get('new_count', 0)}条, 刷新={upsert_stats.get('updated_count', 0)}条)"
    )
    return {
        "status": "ok",
        "user_data_dir": session_path,
        "account_warning": None,
        "self_handle": browser_res.get("self_handle") or "",
        "browsed_posts_saved": int(upsert_stats.get("total", 0)),
        "browsed_new_in_db": int(upsert_stats.get("new_count", 0)),
        "browsed_updated_in_db": int(upsert_stats.get("updated_count", 0)),
        "peer_users_discovered": int(browser_res.get("peer_users_discovered", 0)),
        "threads_clicked": int(browser_res.get("threads_clicked", 0)),
        "profiles_visited": int(browser_res.get("profiles_visited", 0)),
        "elapsed_seconds": elapsed,
    }


def run_collector_daemon(
    user_data_dir: str = COLLECTOR_DEFAULTS["user_data_dir"],
    interval_seconds: Optional[int] = None,
    jitter_seconds: Optional[Tuple[int, int]] = None,
) -> None:
    """独立常驻定时拉取循环（只拉取落库，不调大模型）。"""
    logger = get_growth_logger()
    base_interval = max(60, int(interval_seconds or COLLECTOR_DEFAULTS.get("interval_seconds", 3600)))
    raw_jitter = jitter_seconds if jitter_seconds is not None else COLLECTOR_DEFAULTS.get("jitter_seconds", (120, 600))
    if isinstance(raw_jitter, (list, tuple)) and len(raw_jitter) == 2:
        j_low, j_high = max(0, int(raw_jitter[0])), max(0, int(raw_jitter[1]))
        if j_low > j_high:
            j_low, j_high = j_high, j_low
    else:
        j_low, j_high = 0, 0

    session_path = str(Path(user_data_dir).expanduser().resolve())
    logger.info(
        f"⏰ [PostCollector/独立常驻拉取启动] 目录={session_path} | 周期={base_interval}s + 抖动({j_low}~{j_high}s)"
    )
    round_no = 0
    while True:
        round_no += 1
        logger.info(f"========== [PostCollector/拉取轮次 #{round_no}] 开始 ==========")
        try:
            res = collect_and_save_posts(user_data_dir=session_path)
            if res.get("status") != "ok":
                logger.error(f"⚠️ [PostCollector/轮次 #{round_no} 待处理] {res.get('account_warning')}")
        except KeyboardInterrupt:
            logger.info("[PostCollector/常驻退出] 收到手动中断信号，安全停止。")
            break
        except Exception as exc:
            logger.exception(f"❌ [PostCollector/轮次 #{round_no} 异常] {exc}")

        sleep_sec = base_interval + (random.randint(j_low, j_high) if j_high > 0 else 0)
        next_run_time = (datetime.now(TZ_CN) + timedelta(seconds=sleep_sec)).strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"💤 [PostCollector/休眠] 休眠 {sleep_sec} 秒，下一轮将于 [{next_run_time}] 执行...")
        try:
            time.sleep(sleep_sec)
        except KeyboardInterrupt:
            logger.info("[PostCollector/常驻退出] 休眠期间收到中断信号，安全停止。")
            break


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    target_dir = str(COLLECTOR_DEFAULTS["user_data_dir"])
    if COLLECTOR_DEFAULTS.get("continuous_loop", False):
        run_collector_daemon(
            user_data_dir=target_dir,
            interval_seconds=COLLECTOR_DEFAULTS.get("interval_seconds", 3600),
            jitter_seconds=COLLECTOR_DEFAULTS.get("jitter_seconds", (120, 600)),
        )
    else:
        res = collect_and_save_posts(user_data_dir=target_dir)
        print(json.dumps(res, ensure_ascii=False, indent=2))
