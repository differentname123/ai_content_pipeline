# -*- coding: utf-8 -*-
"""
OKX Orbit (欧易广场) 推荐流、币种流、热门榜与评论数据抓取、清洗、多模态富化封装模块。
输出 Schema 与 fetch_binance_feed 保持 100% 兼容。
"""
import concurrent.futures
import json
import logging
import random
import sys
import time
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import requests
from requests.adapters import HTTPAdapter

from common.common_utils import (
    PLATFORM_OKX,
    SELF_OKX_AUTHOR_IDS,
    SELF_OKX_AUTHOR_NAMES,
    clean_post_text,
    clean_post_title,
    create_standard_post_schema,
    download_web_media,
    get_media_save_dir,
    get_requests_proxies,
    is_self_okx_post,
    read_json,
    safe_int,
    save_json,
    setup_logger,
)

setup_logger()
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# ============================================================
# 全局常量
# ============================================================
PROXIES = get_requests_proxies() or {}
PAGE_SIZE = 20
MAX_RETRIES = 5
REQUEST_TIMEOUT = 15
MEDIA_SAVE_DIR = get_media_save_dir()

DEFAULT_OKX_DEVID = "2f579465-c748-4469-9ca9-9e13f0df47dc"
DEFAULT_OKX_SITE_INFO = "=03M3ojI5RXa05WZiwiIMFkQPx0Rfh1SPJiOiUGZvNmIsIiULJiOi42bpdWZyJye"

_is_self_okx_post = is_self_okx_post
_safe_int = safe_int


def _clean_text(raw):
    """清理 HTML 转义、OKX 内嵌 HTML 注释占位符、不可见字符与多余空行"""
    return clean_post_text(raw, strip_html_comments=True)


def _clean_title(raw):
    """清理标题，空值返回 None"""
    return clean_post_title(raw, strip_html_comments=True)


def _build_okx_headers(
    referer="https://www.okx.com/zh-hans/orbit",
    cookies="",
    extra_headers=None,
    timestamp_ms=None,
):
    """构造 OKX 接口通用请求头"""
    now_ms = str(timestamp_ms) if timestamp_ms else str(int(time.time() * 1000))
    headers = {
        "accept": "application/json",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "cache-control": "no-cache",
        "devid": DEFAULT_OKX_DEVID,
        "platform": "web",
        "pragma": "no-cache",
        "priority": "u=1, i",
        "referer": referer,
        "sec-ch-ua": '"Chromium";v="154", "Google Chrome";v="154", "Not A(Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/154.0.0.0 Safari/537.36"
        ),
        "x-cdn": "https://www.okx.com",
        "x-id-group": f"{now_ms}0050001-c-{random.randint(10, 99)}",
        "x-locale": "zh_CN",
        "x-request-timestamp": now_ms,
        "x-simulated-trading": "undefined",
        "x-site-info": DEFAULT_OKX_SITE_INFO,
        "x-utc": "8",
        "x-zkdex-env": "0",
    }
    if cookies:
        headers["cookie"] = cookies
    if extra_headers and isinstance(extra_headers, dict):
        headers.update(extra_headers)
    return headers


# ============================================================
# 1. OKX 热门币榜单采集
# ============================================================
def fetch_okx_hot_coins(
    num=25,
    quote_type="USDT",
    country_filter=1,
    rank=0,
    cookies="",
    extra_headers=None,
    session=None,
):
    """查询 OKX 最新热门币榜单（仅拉取第一页），返回纯币种符号列表"""
    url = "https://www.okx.com/priapi/v5/rubik/web/public/hot-rank"
    referer = "https://www.okx.com/zh-hans/markets/rankings/spot/hot-crypto"
    label = "OKX热门币榜"

    req_client = session if session else requests
    retry_count = 0

    while retry_count < MAX_RETRIES:
        now_ms = str(int(time.time() * 1000))
        params = {
            "countryFilter": country_filter,
            "rank": rank,
            "type": quote_type,
            "pageNum": 1,
            "num": num,
            "t": now_ms,
        }
        headers = _build_okx_headers(
            referer=referer, cookies=cookies, extra_headers=extra_headers, timestamp_ms=now_ms
        )
        response = None

        try:
            response = req_client.get(
                url, headers=headers, params=params, proxies=PROXIES, timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            json_resp = response.json()

            if str(json_resp.get("code")) != "0":
                logger.error(f"❌ [{label}] API 业务报错 | code={json_resp.get('code')} | msg={json_resp.get('msg')}")
                return []

            data_obj = json_resp.get("data") or {}
            rank_list = data_obj.get("data") or []
            if not rank_list:
                logger.warning(f"⚠️ [{label}] 返回的榜单列表为空")
                return []

            hot_coins = []
            seen_coins = set()
            for item in rank_list:
                if not isinstance(item, dict):
                    continue
                coin = (item.get("ccyV2") or "").strip().upper()
                if not coin:
                    inst_id = (item.get("instId") or "").strip().upper()
                    if "-" in inst_id:
                        coin = inst_id.split("-")[0].strip()
                if coin and coin not in seen_coins:
                    seen_coins.add(coin)
                    hot_coins.append(coin)

            logger.info(f"🔥 [{label}] 拉取成功 | 共获取 {len(hot_coins)} 个热门币种: {hot_coins}")
            return hot_coins

        except Exception as e:
            detail = ""
            if response is not None and response.status_code >= 400:
                detail = f" | HTTP {response.status_code} | 返回: {response.text[:300]}"
            retry_count += 1
            logger.warning(f"🚨 [{label}] 请求失败 ({retry_count}/{MAX_RETRIES}){detail} | 异常: {e}")
            if retry_count >= MAX_RETRIES:
                logger.error(f"❌ [{label}] 连续失败达到 {MAX_RETRIES} 次上限，放弃采集")
                break
            time.sleep(random.uniform(1.0, 2.5))

    return []


# ============================================================
# 2. OKX 通用游标翻页引擎 (推荐流 & 币种流共享)
# ============================================================
def _paginate_okx_cursor_feed(
    url: str,
    referer: str,
    label: str,
    required_count: int,
    initial_cursor: str,
    build_params,
    cookies: str = "",
    extra_headers=None,
    existing_ids=None,
    wrap_content_data: bool = True,
):
    """通用 OKX 游标分页拉取引擎，统一处理翻页、重试、去重与自营拦截"""
    all_items = []
    seen_ids = set(existing_ids) if existing_ids else set()
    cursor = str(initial_cursor) if initial_cursor is not None else ""
    page_index = 1
    retry_count = 0
    max_pages = max(15, (required_count // 10) * 3)

    with requests.Session() as session:
        while len(all_items) < required_count and page_index <= max_pages:
            now_ms = str(int(time.time() * 1000))
            params = build_params(cursor, now_ms)
            headers = _build_okx_headers(
                referer=referer, cookies=cookies, extra_headers=extra_headers, timestamp_ms=now_ms
            )
            response = None

            try:
                response = session.get(
                    url, headers=headers, params=params, proxies=PROXIES, timeout=REQUEST_TIMEOUT
                )
                response.raise_for_status()
                json_resp = response.json()

                if str(json_resp.get("code")) != "0":
                    logger.error(
                        f"❌ [{label}] API 业务报错 | code={json_resp.get('code')} | msg={json_resp.get('msg')}"
                    )
                    break

                data_obj = json_resp.get("data") or {}
                raw_list = data_obj.get("contentDataList") or data_obj.get("dataList") or []
                if not raw_list:
                    logger.debug(f"⚠️ [{label}] 第 {page_index} 页帖子列表为空，采集结束")
                    break

                new_in_page = 0
                for item in raw_list:
                    if not isinstance(item, dict):
                        continue
                    content_data = item.get("contentData") if "contentData" in item else item
                    if not isinstance(content_data, dict) or _is_self_okx_post(content_data):
                        continue
                    item_id = str(content_data.get("contentId") or content_data.get("id") or "")
                    if not item_id or item_id in seen_ids:
                        continue
                    seen_ids.add(item_id)
                    if wrap_content_data and "contentData" not in item:
                        all_items.append({"contentData": item})
                    else:
                        all_items.append(item)
                    new_in_page += 1

                next_cursor = str(data_obj.get("nextCursor") or "")
                logger.debug(
                    f"📥 [{label}] 第 {page_index} 页成功 | 本页返回:{len(raw_list)}条 | "
                    f"净增未见:{new_in_page}条 | 累计净增:{len(all_items)}/{required_count} | nextCursor={next_cursor}"
                )

                if not next_cursor or next_cursor == "0" or next_cursor == cursor:
                    break

                cursor = next_cursor
                page_index += 1
                retry_count = 0
                time.sleep(random.uniform(0.5, 1.4))

            except Exception as e:
                detail = ""
                if response is not None and response.status_code >= 400:
                    detail = f" | HTTP {response.status_code} | 返回: {response.text[:300]}"
                retry_count += 1
                logger.warning(
                    f"🚨 [{label}] 第 {page_index} 页请求失败 ({retry_count}/{MAX_RETRIES}){detail} | 异常: {e}"
                )
                if retry_count >= MAX_RETRIES:
                    logger.error(f"❌ [{label}] 连续失败达到 {MAX_RETRIES} 次上限，终止采集")
                    break
                time.sleep(random.uniform(1.0, 2.5))

    return all_items[:required_count]


def fetch_okx_coin_feed(
    coin_name,
    required_count=20,
    orderBy=2,
    group="USDT",
    initial_cursor="",
    cookies="",
    extra_headers=None,
    existing_ids=None,
):
    """抓取 OKX 指定币种关联的 K 线帖子流 (kline-feed)"""
    clean_coin = str(coin_name).strip().upper()
    url = "https://www.okx.com/priapi/v5/content/public/kline-feed/1"
    referer = f"https://www.okx.com/zh-hans/trade-swap/{clean_coin.lower()}-{group.lower()}-swap"

    if str(orderBy).lower() in ["1", "latest", "new", "time"]:
        exp_method = 1
        order_label = "最新"
    else:
        exp_method = 2
        order_label = "推荐"

    return _paginate_okx_cursor_feed(
        url=url,
        referer=referer,
        label=f"OKX币种流[{clean_coin}-{order_label}]",
        required_count=required_count,
        initial_cursor=initial_cursor,
        build_params=lambda cursor, now_ms: {
            "size": min(PAGE_SIZE, 20),
            "coinName": clean_coin,
            "cursor": cursor,
            "group": group,
            "experimentMethod": exp_method,
            "t": now_ms,
        },
        cookies=cookies,
        extra_headers=extra_headers,
        existing_ids=existing_ids,
        wrap_content_data=True,
    )


def fetch_okx_feed_recommend(
    required_count=20,
    initial_cursor="1",
    cookies="",
    extra_headers=None,
    existing_ids=None,
):
    """不断拉取 OKX Orbit 推荐流数据，直到满足目标数量或无更多数据"""
    return _paginate_okx_cursor_feed(
        url="https://www.okx.com/priapi/v5/rubik/public/content/feed/1",
        referer="https://www.okx.com/zh-hans/orbit",
        label="OKX推荐流",
        required_count=required_count,
        initial_cursor=initial_cursor if initial_cursor is not None else "1",
        build_params=lambda cursor, now_ms: {
            "size": min(PAGE_SIZE, 20),
            "cursor": cursor,
            "t": now_ms,
        },
        cookies=cookies,
        extra_headers=extra_headers,
        existing_ids=existing_ids,
        wrap_content_data=False,
    )


def fetch_okx_feed(
    count=20,
    token=None,
    orderBy=2,
    existing_ids=None,
    cookies="",
    extra_headers=None,
    download_media=True,
    initial_cursor=None,
):
    """统一的 OKX 广场流数据拉取总入口 (支持全网推荐流、指定币种推荐/最新流)"""
    coin_name = token
    caller_existing_ref = existing_ids
    normalized_existing = set(str(i) for i in existing_ids) if existing_ids else set()

    is_coin_mode = bool(coin_name and str(coin_name).strip())
    clean_coin_name = str(coin_name).strip().upper() if is_coin_mode else None

    is_latest = str(orderBy).lower() in ["1", "latest", "new", "time"]
    order_label = "最新" if is_latest else "推荐"
    task_label = f"OKX币种流[{clean_coin_name}-{order_label}]" if is_coin_mode else "OKX广场推荐流"

    logger.info(f"🚀 开始探测: [{task_label}] | 目标净增条数: {count}")

    if is_coin_mode:
        cur = initial_cursor if initial_cursor is not None else ""
        raw_feed_list = fetch_okx_coin_feed(
            coin_name=clean_coin_name,
            required_count=count,
            orderBy=orderBy,
            initial_cursor=cur,
            cookies=cookies,
            extra_headers=extra_headers,
            existing_ids=normalized_existing,
        )
    else:
        cur = initial_cursor if initial_cursor is not None else "1"
        raw_feed_list = fetch_okx_feed_recommend(
            required_count=count,
            initial_cursor=cur,
            cookies=cookies,
            extra_headers=extra_headers,
            existing_ids=normalized_existing,
        )

    filtered_feed_list = []
    intercepted_count = 0

    for raw_item in raw_feed_list:
        content_data = raw_item.get("contentData") if "contentData" in raw_item else raw_item
        if not isinstance(content_data, dict):
            continue
        if _is_self_okx_post(content_data):
            intercepted_count += 1
            continue
        item_id = str(content_data.get("contentId") or content_data.get("id") or "")
        if item_id and item_id not in normalized_existing:
            filtered_feed_list.append(raw_item)
            normalized_existing.add(item_id)
            if isinstance(caller_existing_ref, set):
                caller_existing_ref.add(item_id)
            elif isinstance(caller_existing_ref, list):
                caller_existing_ref.append(item_id)
        else:
            intercepted_count += 1

    if not filtered_feed_list:
        logger.info(
            f"🛑 [{task_label}] 探测完毕 | 抓取:{len(raw_feed_list)}条 | "
            f"🛡️历史/自营拦截:{intercepted_count}条 | 净增:0条 -> 跳过后续处理"
        )
        return []

    logger.info(
        f"📥 [{task_label}] 探测完毕 | 抓取:{len(raw_feed_list)}条 | "
        f"🛡️历史/自营拦截:{intercepted_count}条 | 净增:{len(filtered_feed_list)}条新帖"
    )

    clean_feed_list = clean_okx_posts(filtered_feed_list)
    return update_okx_posts_in_place(
        clean_feed_list,
        cookies=cookies,
        extra_headers=extra_headers,
        download_media=download_media,
    )


# ============================================================
# 3. 统一数据骨架与 OKX 帖子清洗器
# ============================================================
def get_standard_schema():
    """获取统一帖子数据骨架"""
    return create_standard_post_schema(source=PLATFORM_OKX, card_type="UNKNOWN")


def _extract_okx_coins(content_data):
    """多维提取 OKX 帖子关联币种 (cashTagList / tokens / pluginInfo)"""
    coins = set()

    tag_list_obj = content_data.get("tagList") or {}
    for cash_item in (tag_list_obj.get("cashTagList") or []):
        if isinstance(cash_item, dict):
            c_name = cash_item.get("coinName") or cash_item.get("displayName") or cash_item.get("tag", "")
            c_name = c_name.replace("$", "").strip().upper()
            if c_name:
                coins.add(c_name)

    for token_item in (content_data.get("tokens") or []):
        if isinstance(token_item, dict):
            c_name = (token_item.get("coinName") or token_item.get("displayName") or "").strip().upper()
            if c_name:
                coins.add(c_name)

    for plugin in (content_data.get("pluginInfo") or []):
        if not isinstance(plugin, dict):
            continue
        display_id = plugin.get("displayId") or ""
        if "-" in display_id:
            base_coin = display_id.split("-")[0].strip().upper()
            if base_coin:
                coins.add(base_coin)

        meta_str = plugin.get("metaData")
        if meta_str and isinstance(meta_str, str):
            try:
                meta_json = json.loads(meta_str)
                inst_names = meta_json.get("aiData", {}).get("instNames") or []
                for group in inst_names:
                    if isinstance(group, list):
                        for name in group:
                            if name:
                                coins.add(str(name).strip().upper())
            except Exception:
                pass

    return list(coins)


def _extract_okx_hashtags(content_data):
    """提取并清理 OKX Hashtags"""
    tag_list_obj = content_data.get("tagList") or {}
    hash_list = tag_list_obj.get("hashTagList") or []
    tags = []
    for item in hash_list:
        if isinstance(item, dict):
            tag_str = (item.get("tag") or "").strip()
            if tag_str:
                tags.append(tag_str)
    return list(set(tags))


def _infer_card_type(content_data):
    """将 OKX 的帖子形态映射为与币安一致的 card_type 枚举"""
    if content_data.get("videoUrl") or content_data.get("videoList"):
        return "BUZZ_VIDEO"
    if _safe_int(content_data.get("articleSchemaVersion"), 0) > 0 or content_data.get("formatType") == 1:
        return "BUZZ_LONG"
    return "BUZZ_SHORT"


def clean_okx_posts(raw_data_list):
    """将 OKX 原始 dataList 清洗并转换为统一的 get_standard_schema() 结构"""
    cleaned_list = []
    failed = 0

    for raw_wrapper in raw_data_list:
        try:
            content_data = raw_wrapper.get("contentData") if "contentData" in raw_wrapper else raw_wrapper
            if not isinstance(content_data, dict) or _is_self_okx_post(content_data):
                continue

            post_id = str(content_data.get("contentId") or content_data.get("id") or "")
            if not post_id:
                continue

            data = get_standard_schema()
            card_type = _infer_card_type(content_data)

            data["post_id"] = post_id
            raw_pub_time = _safe_int(content_data.get("publishTime"), 0)
            data["publish_time"] = raw_pub_time // 1000 if raw_pub_time > 10_000_000_000 else raw_pub_time
            author_obj = content_data.get("author") or {}
            author_id = str(author_obj.get("authorId") or "")
            data["author_id"] = author_id if author_id else None
            data["card_type"] = card_type

            share_url = content_data.get("shareUrl") or ""
            clean_url = share_url.split("?")[0] if share_url else f"https://www.okx.com/zh-hans/orbit/post/{post_id}"
            bot_types = content_data.get("botTypeList") or []
            data["metadata"].update({
                "url": clean_url,
                "is_ai_generated": len(bot_types) > 0,
            })

            is_verified = (
                str(author_obj.get("officialStatus", "0")) != "0"
                or bool(author_obj.get("certBadge"))
                or bool(author_obj.get("badgeList"))
            )
            data["author"].update({
                "username": author_id or author_obj.get("nickName"),
                "author_name": author_obj.get("nickName"),
                "is_verified": is_verified,
            })

            auto_translate = bool(content_data.get("autoTranslate"))
            if auto_translate and content_data.get("translatedTitle"):
                raw_title = content_data.get("translatedTitle")
            else:
                raw_title = content_data.get("title") or content_data.get("enTitle")

            if auto_translate and content_data.get("translatedContent"):
                raw_text = content_data.get("translatedContent")
            else:
                raw_text = content_data.get("content") or content_data.get("excerpt") or ""

            text_content = _clean_text(raw_text)
            data["content"].update({
                "title": _clean_title(raw_title),
                "text_content": text_content,
                "hashtags": _extract_okx_hashtags(content_data),
                "mentioned_coins": _extract_okx_coins(content_data),
            })

            image_urls = []
            for img in (content_data.get("imageList") or []):
                if isinstance(img, dict) and img.get("url"):
                    image_urls.append(img.get("url"))
                elif isinstance(img, str) and img:
                    image_urls.append(img)

            cover_photo = content_data.get("coverPhoto")
            cover_url = None
            if isinstance(cover_photo, dict):
                cover_url = cover_photo.get("url")
            elif isinstance(cover_photo, str) and cover_photo:
                cover_url = cover_photo

            data["media"]["cover_image"] = cover_url
            if card_type == "BUZZ_LONG":
                data["media"]["inline_images"] = image_urls
            else:
                data["media"]["images"] = image_urls

            data["engagement"].update({
                "view_count": _safe_int(content_data.get("viewCount"), 0),
                "like_count": _safe_int(content_data.get("likeCount"), 0),
                "comment_count": _safe_int(content_data.get("commentNum"), 0),
                "share_count": _safe_int(content_data.get("retweetTotal"), 0),
                "quote_count": _safe_int(content_data.get("quoteCount"), 0),
            })

            cleaned_list.append(data)
        except Exception as e:
            failed += 1
            logger.error(f"[OKX_CLEAN] 帖子清洗失败 | error={e}", exc_info=True)

    logger.info(f"🗂️ [OKX清洗] 完成 | 成功={len(cleaned_list)}条 | 失败={failed}条")
    return cleaned_list


# ============================================================
# 4. OKX 评论采集与多模态富化
# ============================================================
def _format_single_okx_comment(item, default_parent_id):
    """格式化单条 OKX 评论字典"""
    auto_translate = bool(item.get("autoTranslate"))
    original_text = item.get("comment") or ""
    translated_text = item.get("translatedComment") or ""
    content = translated_text if (auto_translate and translated_text) else original_text

    reply_root_id = str(item.get("replyRootId") or "0")
    parent_id = default_parent_id if reply_root_id == "0" else reply_root_id

    return {
        "reply_id": str(item.get("commentId") or ""),
        "parent_id": str(parent_id),
        "create_time": _safe_int(item.get("commentTime"), 0),
        "author_uid": str(item.get("authorId") or ""),
        "author_name": item.get("userName") or "",
        "content": _clean_text(content),
        "likes": _safe_int(item.get("likeNum"), 0),
        "replies": _safe_int(item.get("commentNum"), 0),
        "views": 0,
    }


def clean_okx_replies(raw_replies, content_id=""):
    """清洗 OKX 评论列表，同时拍平二级楼中楼回复"""
    cleaned_data = []
    seen_reply_ids = set()

    for item in raw_replies:
        if not isinstance(item, dict) or str(item.get("isDel", "0")) == "1":
            continue

        main_comment = _format_single_okx_comment(item, default_parent_id=content_id)
        if main_comment["reply_id"] and main_comment["reply_id"] not in seen_reply_ids:
            seen_reply_ids.add(main_comment["reply_id"])
            cleaned_data.append(main_comment)

        replies_container = item.get("replies") or {}
        sub_reply_list = replies_container.get("replyList") or []
        for sub_item in sub_reply_list:
            if not isinstance(sub_item, dict) or str(sub_item.get("isDel", "0")) == "1":
                continue
            sub_comment = _format_single_okx_comment(
                sub_item, default_parent_id=main_comment["reply_id"] or content_id
            )
            if sub_comment["reply_id"] and sub_comment["reply_id"] not in seen_reply_ids:
                seen_reply_ids.add(sub_comment["reply_id"])
                cleaned_data.append(sub_comment)

    return cleaned_data


def fetch_okx_replies(content_id, required_count=10, cookies="", extra_headers=None, session=None):
    """获取 OKX 指定帖子的评论列表"""
    url = "https://www.okx.com/priapi/v5/content/public/comments"
    referer = f"https://www.okx.com/zh-hans/orbit/post/{content_id}"

    req_client = session if session else requests
    all_raw_comments = []
    cursor = None
    page_index = 1
    retry_count = 0
    label = f"OKX评论流:{content_id}"

    while len(all_raw_comments) < required_count:
        now_ms = str(int(time.time() * 1000))
        params = {
            "contentId": str(content_id),
            "count": min(20, max(10, required_count)),
            "t": now_ms,
        }
        if cursor and cursor != "0":
            params["cursor"] = cursor

        headers = _build_okx_headers(referer=referer, cookies=cookies, extra_headers=extra_headers)
        response = None

        try:
            response = req_client.get(
                url, headers=headers, params=params, proxies=PROXIES, timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            json_resp = response.json()

            if str(json_resp.get("code")) != "0":
                logger.error(f"❌ [{label}] API 业务错误 | code={json_resp.get('code')} | msg={json_resp.get('msg')}")
                break

            data_obj = json_resp.get("data") or {}
            comment_list = data_obj.get("commentList") or []
            if not comment_list:
                break

            all_raw_comments.extend(comment_list)
            next_cursor = str(data_obj.get("nextCursor") or "0")
            if not next_cursor or next_cursor == "0" or next_cursor == cursor:
                break

            cursor = next_cursor
            page_index += 1
            retry_count = 0
            time.sleep(random.uniform(0.4, 1.0))

        except Exception as e:
            detail = ""
            if response is not None:
                detail = f" | HTTP {response.status_code} | 返回: {response.text[:300]}"
            retry_count += 1
            logger.warning(
                f"🚨 [{label}] 第 {page_index} 页请求失败 ({retry_count}/{MAX_RETRIES}){detail} | 异常: {e}"
            )
            if retry_count >= MAX_RETRIES:
                logger.error(f"❌ [{label}] 连续失败达到 {MAX_RETRIES} 次上限，终止评论采集。")
                break
            time.sleep(random.uniform(1.0, 2.5))

    cleaned_replies = clean_okx_replies(all_raw_comments, content_id=str(content_id))
    return cleaned_replies[:required_count]


def _enrich_single_okx_post(item, session, cookies="", extra_headers=None, download_media=True):
    """单条 OKX 帖子的多模态标记融合、图片下载与评论拉取"""
    stats = {
        "long_updated": 0,
        "other_updated": 0,
        "comments_fetched": 0,
        "total_download_urls": 0,
        "total_download_success": 0,
        "failed": 0,
    }

    post_id = item.get("post_id")
    if not post_id:
        return item, stats

    try:
        media_info = item.get("media", {})
        media_marks = []

        if media_info.get("cover_image"):
            media_marks.append(f"[封面图: {media_info['cover_image']}]")
        for img_url in media_info.get("images", []):
            if img_url:
                media_marks.append(f"[插图: {img_url}]")
        for img_url in media_info.get("inline_images", []):
            if img_url:
                media_marks.append(f"[插图: {img_url}]")

        if media_marks:
            original_text = item.get("content", {}).get("text_content", "").strip()
            marks_text = "\n".join(media_marks)
            if marks_text not in original_text:
                item["content"]["text_content"] = (
                    f"{original_text}\n\n{marks_text}" if original_text else marks_text
                )

        stats["other_updated"] = 1

        if "local_mapping" not in media_info:
            media_info["local_mapping"] = {}

        if download_media:
            urls_to_download = set()
            if media_info.get("cover_image"):
                urls_to_download.add(media_info["cover_image"])
            for img_url in media_info.get("images", []):
                if img_url:
                    urls_to_download.add(img_url)
            for img_url in media_info.get("inline_images", []):
                if img_url:
                    urls_to_download.add(img_url)

            stats["total_download_urls"] = len(urls_to_download)
            proxy_url = PROXIES.get("http")

            for url in urls_to_download:
                try:
                    local_path = download_web_media(url=url, save_dir=MEDIA_SAVE_DIR, proxy=proxy_url)
                    if local_path:
                        media_info["local_mapping"][url] = local_path
                        stats["total_download_success"] += 1
                except Exception as dl_e:
                    logger.error(f"[OKX_DOWNLOAD] 媒体下载失败 | post_id={post_id} | url={url} | error={dl_e}")

        try:
            comment_count = item.get("engagement", {}).get("comment_count", 0)
            if comment_count <= 0:
                item["comments"] = []
            else:
                replies = fetch_okx_replies(
                    content_id=post_id,
                    required_count=10,
                    cookies=cookies,
                    extra_headers=extra_headers,
                    session=session,
                )
                item["comments"] = replies
                if replies:
                    stats["comments_fetched"] = 1
        except Exception as reply_e:
            logger.error(f"[OKX_COMMENTS] 评论拉取失败 | post_id={post_id} | error={reply_e}")

    except Exception as e:
        stats["failed"] = 1
        logger.error(f"[OKX_UPDATE] 就地富化失败 | post_id={post_id} | error={e}", exc_info=True)

    return item, stats


def update_okx_posts_in_place(clean_list, cookies="", extra_headers=None, download_media=True):
    """多线程并发富化 OKX 帖子"""
    if not clean_list:
        return []

    total_stats = {
        "long_updated": 0,
        "other_updated": 0,
        "comments_fetched": 0,
        "total_download_urls": 0,
        "total_download_success": 0,
        "failed": 0,
    }
    max_workers = min(10, len(clean_list))
    logger.debug(f"⏳ [OKX并发富化] 开始处理 {len(clean_list)} 条数据 (线程数={max_workers})...")

    with requests.Session() as session:
        adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_item = {
                executor.submit(
                    _enrich_single_okx_post, item, session, cookies, extra_headers, download_media
                ): item
                for item in clean_list
            }
            for future in concurrent.futures.as_completed(future_to_item):
                try:
                    _, task_stats = future.result()
                    for key in total_stats:
                        total_stats[key] += task_stats.get(key, 0)
                except Exception as exc:
                    logger.error(f"🚨 [OKX并发富化] 线程异常: {exc}", exc_info=True)
                    total_stats["failed"] += 1

    logger.info(
        f"✅ [OKX并发富化] 完毕 | 媒体拼接:{total_stats['other_updated']} | "
        f"下载成功:{total_stats['total_download_success']}/{total_stats['total_download_urls']} | "
        f"评论拉取:{total_stats['comments_fetched']} | 失败:{total_stats['failed']}"
    )
    return clean_list


# ============================================================
# 5. OKX 用户主页时间线查询 (供按账号与时间段删帖枚举使用)
# ============================================================
def fetch_okx_user_timeline(
    author_id,
    max_pages=100,
    start_ts=None,
    end_ts=None,
    cookies="",
    extra_headers=None,
):
    """拉取 OKX 指定用户主页的动态时间线帖子列表 (/priapi/v5/content/public/timeline/1, type=3)"""
    clean_uid = str(author_id or "").strip()
    if not clean_uid:
        return []

    url = "https://www.okx.com/priapi/v5/content/public/timeline/1"
    referer = f"https://www.okx.com/zh-hans/orbit/user/{clean_uid}"
    results = []
    seen_ids = set()
    cursor = ""
    page_index = 1
    retry_count = 0

    with requests.Session() as session:
        while page_index <= max_pages:
            now_ms = str(int(time.time() * 1000))
            params = {
                "size": 50,
                "queryName": clean_uid,
                "type": 3,
                "cursor": cursor,
                "t": now_ms,
            }
            headers = _build_okx_headers(
                referer=referer, cookies=cookies, extra_headers=extra_headers, timestamp_ms=now_ms
            )
            try:
                resp = session.get(
                    url, headers=headers, params=params, proxies=PROXIES, timeout=REQUEST_TIMEOUT
                )
                resp.raise_for_status()
                json_resp = resp.json()
                if str(json_resp.get("code")) != "0":
                    logger.warning(
                        f"⚠️ [OKX用户主页流:{clean_uid}] 接口返回非0 | code={json_resp.get('code')} | msg={json_resp.get('msg')}"
                    )
                    break

                data_obj = json_resp.get("data") or {}
                raw_list = data_obj.get("contentDataList") or data_obj.get("dataList") or []
                if not raw_list:
                    break

                new_in_page = 0
                reached_older_than_start = False
                for item in raw_list:
                    if not isinstance(item, dict):
                        continue
                    c_data = item.get("contentData") if "contentData" in item else item
                    if not isinstance(c_data, dict):
                        continue
                    cid = str(c_data.get("contentId") or c_data.get("id") or "").strip()
                    if not cid or cid in seen_ids:
                        continue
                    seen_ids.add(cid)
                    new_in_page += 1

                    raw_pt = _safe_int(c_data.get("publishTime"), 0)
                    pub_ts = raw_pt / 1000.0 if raw_pt > 10_000_000_000 else float(raw_pt)
                    if end_ts is not None and pub_ts > 0 and pub_ts > float(end_ts):
                        continue
                    if start_ts is not None and pub_ts > 0 and pub_ts < float(start_ts):
                        reached_older_than_start = True
                        continue

                    results.append({
                        "post_id": cid,
                        "publish_time": pub_ts,
                        "title": str(c_data.get("title") or "")[:80],
                    })

                next_cursor = str(data_obj.get("nextCursor") or "")
                if (
                    reached_older_than_start
                    or not next_cursor
                    or next_cursor == "0"
                    or next_cursor == cursor
                    or new_in_page == 0
                ):
                    break

                cursor = next_cursor
                page_index += 1
                retry_count = 0
                time.sleep(random.uniform(0.15, 0.35))

            except Exception as e:
                retry_count += 1
                logger.warning(
                    f"🚨 [OKX用户主页流:{clean_uid}] 第 {page_index} 页请求异常 ({retry_count}/{MAX_RETRIES}): {e}"
                )
                if retry_count >= MAX_RETRIES:
                    break
                time.sleep(random.uniform(0.8, 1.5))

    return results


if __name__ == "__main__":
    hot_coins = fetch_okx_hot_coins(num=25)
    okx_recommend_posts = fetch_okx_feed(count=10)
    btw_posts = fetch_okx_feed(count=40, token="BTW", download_media=False)
    logger.info(f"BTW 代币帖子获取成功: {len(btw_posts)} 条")
    if btw_posts:
        logger.info(f"BTW 首条帖子文本: {btw_posts[0]['content']['text_content'][:100]}...")
    logger.info(f"🎉 OKX 推荐流抓取完成 | 共获取标准格式帖子: {len(okx_recommend_posts)} 条")
    if okx_recommend_posts:
        logger.info(f"🔎 首条标准帖子结构预览:\n{json.dumps(okx_recommend_posts[0], ensure_ascii=False, indent=2)}")
