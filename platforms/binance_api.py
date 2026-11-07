# -*- coding: utf-8 -*-
"""
币安广场 (Binance Square) 接口与行情采集、清洗、多模态富化及互动管理模块。
"""
import concurrent.futures
import json
import logging
import random
import re
import sys
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import quote

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import ccxt
import requests
from requests.adapters import HTTPAdapter

from common.common_utils import (
    PLATFORM_BINANCE,
    SELF_BINANCE_AUTHOR_NAMES,
    SELF_BINANCE_USERNAMES,
    clean_post_text,
    clean_post_title,
    create_standard_post_schema,
    download_web_media,
    get_config,
    get_media_save_dir,
    get_proxy_url,
    get_requests_proxies,
    is_self_binance_post,
    read_json,
    save_json,
    setup_logger,
)

setup_logger()
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# ============================================================
# 全局常量与预编译正则
# ============================================================
PROXIES = get_requests_proxies() or {}
MEDIA_SAVE_DIR = get_media_save_dir()
PAGE_SIZE = 20
MAX_RETRIES = 5
REQUEST_TIMEOUT = 15

_INVISIBLE_RE = re.compile(r"[\u200b-\u200f\ufeff\u202a-\u202e]")
_MULTI_NEWLINE_RE = re.compile(r"\n{3,}")

_is_self_binance_post = is_self_binance_post

DEFAULT_WEB_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
)


def _build_private_headers(cookies: str, csrf_token: str, referer: str = "https://www.binance.com/") -> dict:
    """构造币安广场需要登录态鉴权的通用请求头"""
    return {
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "clienttype": "web",
        "content-type": "application/json",
        "lang": "zh-CN",
        "cookie": cookies,
        "csrftoken": csrf_token,
        "origin": "https://www.binance.com",
        "referer": referer,
        "user-agent": DEFAULT_WEB_UA,
    }


def _build_public_headers(referer: str = None) -> dict:
    """构造币安广场公开接口通用请求头"""
    headers = {
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "clienttype": "web",
        "content-type": "application/json",
        "lang": "zh-CN",
        "user-agent": DEFAULT_WEB_UA,
    }
    if referer:
        headers["referer"] = referer
    return headers


# ============================================================
# 1. 发帖与互动操作接口
# ============================================================
def publish_to_binance_square(api_key, text_content):
    """
    根据 Binance Skills Hub 规范向币安广场发送 OpenAPI 发帖请求。
    """
    url = "https://www.binance.com/bapi/composite/v1/public/pgc/openApi/content/add"
    headers = {
        "Content-Type": "application/json",
        "clienttype": "binanceSkill",
        "X-Square-OpenAPI-Key": api_key,
    }
    payload = {"bodyTextOnly": text_content}

    masked_key = f"{api_key[:5]}...{api_key[-4:]}" if len(api_key) > 10 else "***"
    text_summary = text_content[:20].replace("\n", " ") + ("..." if len(text_content) > 20 else "")

    try:
        logger.info(f"⏳ 开始向币安广场发帖 | Key: {masked_key} | 内容摘要: {text_summary}")
        response = requests.post(url, headers=headers, json=payload, proxies=PROXIES, timeout=REQUEST_TIMEOUT)
        result = response.json()

        if result.get("success") or str(result.get("code")) == "000000":
            post_id = result.get("data", {}).get("id")
            post_url = f"https://www.binance.com/square/post/{post_id}" if post_id else "未知链接"
            logger.info(f"✅ 发帖成功 | 帖子ID: {post_id} | 直达链接: {post_url}")
            return True
        else:
            error_code = str(result.get("code"))
            hint = " (💡提示: 已达每日发帖上限 Daily limit)" if error_code == "220009" else ""
            logger.error(f"❌ 发帖失败 | 错误码: {error_code}{hint} | 完整响应: {result}")
            return False

    except requests.exceptions.RequestException as e:
        logger.error(f"🚨 发帖网络请求异常 | Key: {masked_key} | 异常信息: {e}")
        return False


def _post_private_action(url: str, label: str, headers: dict, payload: dict, session=None) -> bool:
    """内部通用：执行单次币安私密接口 POST 操作并校验 success 字段"""
    req_client = session if session else requests
    response = None
    try:
        response = req_client.post(
            url, headers=headers, json=payload, proxies=PROXIES, timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        json_resp = response.json()
        if json_resp.get("success"):
            logger.info(f"✅ [{label}] 操作成功！")
            return True
        code = json_resp.get("code")
        msg = json_resp.get("message", "未知错误")
        logger.error(f"❌ [{label}] API 业务错误 | Code: {code} | Msg: {msg}")
        return False
    except Exception as e:
        detail = ""
        if response is not None:
            detail = f" | HTTP {response.status_code} | 返回数据: {response.text[:200]}"
        logger.error(f"🚨 [{label}] 网络或请求异常{detail} | 报错: {e}")
        return False


def toggle_binance_follow(target_uid, action, cookies, csrf_token, session=None):
    """操作币安广场用户的关注状态 ('follow' 或 'unfollow')"""
    if action not in ["follow", "unfollow"]:
        logger.error("❌ 无效的操作类型，只能是 'follow' 或 'unfollow'")
        return False
    url = f"https://www.binance.com/bapi/composite/v2/private/pgc/user/{action}"
    label = f"{'关注' if action == 'follow' else '取消关注'}: {target_uid}"
    headers = _build_private_headers(cookies, csrf_token)
    return _post_private_action(url, label, headers, {"targetSquareUid": target_uid}, session=session)


def toggle_binance_like(post_id, action, cookies, csrf_token, card_type="BUZZ_SHORT", session=None):
    """操作币安广场帖子的点赞状态 ('like' 或 'unlike')"""
    if action not in ["like", "unlike"]:
        logger.error("❌ 无效的操作类型，只能是 'like' 或 'unlike'")
        return False
    url = f"https://www.binance.com/bapi/composite/v1/private/pgc/content/{action}"
    label = f"{'点赞' if action == 'like' else '取消点赞'}帖子: {post_id}"
    headers = _build_private_headers(
        cookies, csrf_token, referer=f"https://www.binance.com/zh-CN/square/post/{post_id}"
    )
    return _post_private_action(url, label, headers, {"id": int(post_id), "cardType": card_type}, session=session)


def toggle_binance_bookmark(post_id, action, cookies, csrf_token, session=None):
    """操作币安广场帖子的收藏状态 ('add' 或 'cancel')"""
    if action not in ["add", "cancel"]:
        logger.error("❌ 无效的操作类型，只能是 'add' 或 'cancel'")
        return False
    url = f"https://www.binance.com/bapi/composite/v1/private/pgc/content/{action}/bookmark"
    label = f"{'收藏' if action == 'add' else '取消收藏'}帖子: {post_id}"
    headers = _build_private_headers(
        cookies, csrf_token, referer=f"https://www.binance.com/zh-CN/square/post/{post_id}"
    )
    return _post_private_action(url, label, headers, {"contentId": int(post_id)}, session=session)


# ============================================================
# 2. 统一 Schema 与多类型帖子清洗器
# ============================================================
def get_standard_schema():
    """获取统一帖子数据骨架"""
    return create_standard_post_schema(source=PLATFORM_BINANCE, card_type="UNKNOWN")


def _clean_text(raw):
    """清理不可见字符并去首尾空格"""
    if not raw:
        return ""
    return _INVISIBLE_RE.sub("", str(raw)).strip()


def _clean_title(raw):
    """标题清理：空值返回 None"""
    if not raw:
        return None
    cleaned = _INVISIBLE_RE.sub("", str(raw)).strip()
    return cleaned if cleaned else None


def _extract_coins(raw_item):
    """提取提到的币种：合并 coinPairList 与 tradingPairsV2[].code"""
    coins = [coin.replace("$", "").strip() for coin in (raw_item.get("coinPairList") or []) if coin]
    for pair in (raw_item.get("tradingPairsV2") or []):
        code = pair.get("code")
        if code:
            coins.append(code.strip())
    return list(set(coin.upper() for coin in coins if coin))


def _extract_hashtags(raw_item, merge_keys=None):
    """提取并清理 Hashtags"""
    tags = list(raw_item.get("hashtagList") or [])
    if merge_keys:
        for key in merge_keys:
            tags += (raw_item.get(key) or [])
        return list(set(ht.strip() for ht in tags if ht))
    return [ht.strip() for ht in tags if ht]


def _assemble_common(raw_item, card_type_default, title, text_content, hashtags):
    """套用统一 Schema，组装 metadata / author / content / engagement"""
    data = get_standard_schema()

    data["post_id"] = raw_item.get("id")
    data["publish_time"] = raw_item.get("date", 0)
    data["author_id"] = raw_item.get("squareAuthorId")
    data["card_type"] = raw_item.get("cardType", card_type_default)

    data["metadata"].update({
        "url": raw_item.get("webLink"),
        "is_ai_generated": raw_item.get("isCreatedByAI", False),
    })

    data["author"].update({
        "username": raw_item.get("username"),
        "author_name": raw_item.get("authorName"),
        "is_verified": (raw_item.get("authorVerificationType") or 0) > 0,
    })

    data["content"].update({
        "title": title,
        "text_content": text_content,
        "hashtags": hashtags,
        "mentioned_coins": _extract_coins(raw_item),
    })

    data["engagement"].update({
        "view_count": raw_item.get("viewCount", 0),
        "like_count": raw_item.get("likeCount", 0),
        "comment_count": raw_item.get("commentCount", 0),
        "share_count": raw_item.get("shareCount", 0),
        "quote_count": raw_item.get("quoteCount", 0),
    })

    return data


def clean_short_posts(raw_data_list):
    """清洗 BUZZ_SHORT (短帖) 类型数据"""
    cleaned_list = []
    failed = 0
    for raw_item in raw_data_list:
        try:
            data = _assemble_common(
                raw_item,
                card_type_default=None,
                title=_clean_title(raw_item.get("title")),
                text_content=_clean_text(raw_item.get("content")),
                hashtags=_extract_hashtags(raw_item),
            )
            data["media"]["images"] = raw_item.get("images", [])
            cleaned_list.append(data)
        except Exception as e:
            failed += 1
            logger.error(f"[BUZZ_SHORT] 帖子清洗失败 | post_id={raw_item.get('id')} | error={e}", exc_info=True)
    logger.debug(f"[BUZZ_SHORT] 清洗完成 | 成功={len(cleaned_list)} 失败={failed}")
    return cleaned_list


def clean_video_posts(raw_data_list):
    """清洗 BUZZ_VIDEO (视频帖) 类型数据"""
    cleaned_list = []
    failed = 0
    for raw_item in raw_data_list:
        try:
            data = _assemble_common(
                raw_item,
                card_type_default="BUZZ_VIDEO",
                title=_clean_title(raw_item.get("title")),
                text_content=_clean_text(raw_item.get("content")),
                hashtags=_extract_hashtags(raw_item),
            )
            data["media"]["images"] = raw_item.get("images", [])
            video_vo = raw_item.get("videoVO") or {}
            video_link = raw_item.get("videoLink") or video_vo.get("videoLink")
            if video_link:
                cover_meta = raw_item.get("coverMeta") or {}
                data["media"]["video_url"] = video_link
                data["media"]["cover_image"] = raw_item.get("coverLight") or cover_meta.get("url")
                data["media"]["video_duration"] = raw_item.get("videoTimeSeconds") or video_vo.get(
                    "videoTimeSeconds", 0
                )
            cleaned_list.append(data)
        except Exception as e:
            failed += 1
            logger.error(f"[BUZZ_VIDEO] 帖子清洗失败 | post_id={raw_item.get('id')} | error={e}", exc_info=True)
    logger.debug(f"[BUZZ_VIDEO] 清洗完成 | 成功={len(cleaned_list)} 失败={failed}")
    return cleaned_list


def clean_long_posts(raw_data_list):
    """清洗 BUZZ_LONG (长文帖) 类型数据"""
    cleaned_list = []
    failed = 0
    for raw_item in raw_data_list:
        try:
            if raw_item.get("cardType") != "BUZZ_LONG":
                continue
            data = _assemble_common(
                raw_item,
                card_type_default="BUZZ_LONG",
                title=_clean_title(raw_item.get("title")),
                text_content=_clean_text(raw_item.get("content") or raw_item.get("subTitle")),
                hashtags=_extract_hashtags(raw_item, merge_keys=["hashtagIdentifyList"]),
            )
            cover_image = None
            cover_meta = raw_item.get("coverMeta")
            if cover_meta and isinstance(cover_meta, dict):
                cover_image = cover_meta.get("url")
            elif raw_item.get("coverLight"):
                cover_image = raw_item.get("coverLight")
            data["media"]["cover_image"] = cover_image
            data["media"]["inline_images"] = raw_item.get("images") or []
            cleaned_list.append(data)
        except Exception as e:
            failed += 1
            logger.error(f"[BUZZ_LONG] 帖子清洗失败 | post_id={raw_item.get('id')} | error={e}", exc_info=True)
    logger.debug(f"[BUZZ_LONG] 清洗完成 | 成功={len(cleaned_list)} 失败={failed}")
    return cleaned_list


def clean_reply_posts(raw_data_list):
    """清洗 BUZZ_REPLY_POST_LIST (串烧/盖楼帖) 类型数据"""
    cleaned_list = []
    container_failed = 0
    processor_map = {
        "BUZZ_SHORT": clean_short_posts,
        "BUZZ_VIDEO": clean_video_posts,
        "BUZZ_LONG": clean_long_posts,
    }
    for raw_container in raw_data_list:
        try:
            if raw_container.get("cardType") != "BUZZ_REPLY_POST_LIST":
                continue
            reply_post_list = raw_container.get("replyPostList", [])
            child_group_map = {}
            for raw_item in reply_post_list:
                c_type = raw_item.get("cardType", "BUZZ_SHORT")
                child_group_map.setdefault(c_type, []).append(raw_item)
            for c_type, items in child_group_map.items():
                processor_func = processor_map.get(c_type, clean_short_posts)
                child_cleaned_list = processor_func(items)
                if child_cleaned_list:
                    cleaned_list.extend(child_cleaned_list)
        except Exception as e:
            container_failed += 1
            logger.error(
                f"[BUZZ_REPLY] 容器解析失败 | container_id={raw_container.get('id')} | error={e}",
                exc_info=True,
            )
    logger.debug(
        f"[BUZZ_REPLY] 容器解析及子帖清洗完成 | 共提取有效子帖={len(cleaned_list)} 条 | 容器失败={container_failed}"
    )
    return cleaned_list


def clean_long_posts_detail(raw_data_list):
    """针对带有 `body` 富文本结构的长文详情，做核心内容与内嵌媒体的深度解析"""
    cleaned_list = []
    failed = 0
    for raw_item in raw_data_list:
        try:
            body_str = raw_item.get("body")
            text_content = ""
            inline_images = []

            if body_str:
                try:
                    body_data = json.loads(body_str)
                    order_list = body_data.get("layout", {}).get("ViewInstance0", [])
                    hash_dict = body_data.get("hash", {})
                    full_text_parts = []

                    def extract_node(node):
                        if not isinstance(node, dict):
                            return ""
                        node_id = node.get("id")
                        config = node.get("config", {})
                        if node_id == "RichTextImage":
                            src = config.get("src")
                            if not src:
                                return ""
                            inline_images.append(src)
                            caption = config.get("caption", "")
                            img_mark = f"\n\n[插图: {src}"
                            if caption:
                                img_mark += f" | 描述: {caption}"
                            return img_mark + "]\n\n"
                        if node_id in ("RichTextText", "RichTextHashTag", "RichTextCoinPair"):
                            return config.get("content", "")
                        if node_id == "RichTextHardBreak":
                            return "\n"
                        content_list = config.get("content", [])
                        if not isinstance(content_list, list):
                            content_list = []
                        res = "".join(extract_node(child) for child in content_list)
                        if node_id == "RichTextListItem":
                            res = "• " + res + "\n"
                        return res

                    for uid in order_list:
                        block = hash_dict.get(uid, {})
                        if block.get("empty"):
                            full_text_parts.append("\n")
                            continue
                        block_text = extract_node(block)
                        if not block_text:
                            continue
                        if block.get("id") in ("RichTextParagraph", "RichTextQuote", "RichTextHeader", "RichTextList"):
                            full_text_parts.append(block_text + "\n")
                        else:
                            full_text_parts.append(block_text)

                    raw_parsed_text = "".join(full_text_parts)
                    raw_text_content = _MULTI_NEWLINE_RE.sub("\n\n", raw_parsed_text).strip()
                    text_content = _INVISIBLE_RE.sub("", raw_text_content)
                except json.JSONDecodeError:
                    text_content = _clean_text(
                        raw_item.get("bodyTextOnly") or raw_item.get("content") or raw_item.get("subTitle")
                    )
            else:
                text_content = _clean_text(
                    raw_item.get("bodyTextOnly") or raw_item.get("content") or raw_item.get("subTitle")
                )

            cover_image = None
            cover_meta = raw_item.get("coverMeta")
            if cover_meta and isinstance(cover_meta, dict):
                cover_image = cover_meta.get("url")
            elif raw_item.get("cover"):
                cover_image = raw_item.get("cover")

            cleaned_list.append({
                "core_text_content": text_content,
                "media": {
                    "cover_image": cover_image,
                    "inline_images": inline_images,
                },
            })
        except Exception as e:
            failed += 1
            logger.error(f"[LONG_DETAIL] 长文详情解析失败 | post_id={raw_item.get('id')} | error={e}", exc_info=True)

    if failed:
        logger.warning(f"[LONG_DETAIL] 详情解析存在失败 | 成功={len(cleaned_list)} 失败={failed}")
    return cleaned_list


def clean_universal_posts(final_clean_list):
    """通用清洗调度：去重 + 自营拦截 + 按 cardType 分发"""
    card_type_counts = {}
    group_map = {}
    seen_post_ids = set()
    unique_count = 0

    for item in final_clean_list:
        if _is_self_binance_post(item):
            continue
        post_id = item.get("id")
        if post_id:
            if post_id in seen_post_ids:
                continue
            seen_post_ids.add(post_id)

        unique_count += 1
        card_type = item.get("cardType", "UNKNOWN")
        card_type_counts[card_type] = card_type_counts.get(card_type, 0) + 1
        group_map.setdefault(card_type, []).append(item)

    logger.debug(
        f"🗂️ 分组去重完成 | 唯一记录={unique_count} | cardType种类={len(card_type_counts)} | 分布={card_type_counts}"
    )

    all_clean_list = []
    processor_map = {
        "BUZZ_SHORT": clean_short_posts,
        "BUZZ_REPLY_POST_LIST": clean_reply_posts,
        "BUZZ_VIDEO": clean_video_posts,
        "BUZZ_LONG": clean_long_posts,
    }
    for card_type, processor_func in processor_map.items():
        if card_type in group_map:
            result_list = processor_func(group_map[card_type])
            if result_list:
                all_clean_list.extend(result_list)

    logger.info(
        f"🗂️ [清洗分发] 完成 | 共处理:{unique_count}条 | 分布:{card_type_counts} | 有效产出:{len(all_clean_list)}条"
    )
    return all_clean_list


# ============================================================
# 3. 多模态详情富化、媒体下载与评论并发拉取
# ============================================================
def _enrich_single_post(item, session):
    """单条帖子数据的多模态丰富处理 (无锁设计原子函数)"""
    stats = {
        "long_updated": 0,
        "other_updated": 0,
        "comments_fetched": 0,
        "total_download_urls": 0,
        "total_download_success": 0,
        "failed": 0,
    }

    post_id = item.get("post_id")
    card_type = item.get("card_type")
    if not post_id:
        return item, stats

    try:
        if card_type == "BUZZ_LONG":
            post_detail = fetch_binance_post_detail(post_id, session=session)
            if post_detail:
                cleaned_details = clean_long_posts_detail([post_detail])
                if cleaned_details:
                    detail_item = cleaned_details[0]
                    core_text = detail_item.get("core_text_content", "")
                    cover_image = detail_item.get("media", {}).get("cover_image")
                    if cover_image:
                        cover_mark = f"[长文封面: {cover_image}]"
                        if cover_mark not in core_text:
                            core_text = f"{cover_mark}\n\n{core_text}" if core_text else cover_mark
                    if core_text:
                        item["content"]["text_content"] = core_text
                    item["media"]["cover_image"] = cover_image
                    item["media"]["inline_images"] = detail_item.get("media", {}).get("inline_images", [])
                    stats["long_updated"] = 1
        else:
            media_info = item.get("media", {})
            media_marks = []
            for img_url in media_info.get("images", []):
                if img_url:
                    media_marks.append(f"[插图: {img_url}]")
            video_url = media_info.get("video_url")
            if video_url:
                cover_image = media_info.get("cover_image")
                if cover_image:
                    media_marks.append(f"[视频封面: {cover_image}]")
                media_marks.append(f"[视频: {video_url}]")
            if media_marks:
                original_text = item.get("content", {}).get("text_content", "").strip()
                marks_text = "\n".join(media_marks)
                if marks_text not in original_text:
                    item["content"]["text_content"] = (
                        f"{original_text}\n\n{marks_text}" if original_text else marks_text
                    )
            stats["other_updated"] = 1

        media_info = item.get("media", {})
        if "local_mapping" not in media_info:
            media_info["local_mapping"] = {}

        urls_to_download = set()
        if media_info.get("cover_image"):
            urls_to_download.add(media_info["cover_image"])
        if media_info.get("video_url"):
            video_duration = media_info.get("video_duration", 0)
            if video_duration < 300:
                urls_to_download.add(media_info["video_url"])
            else:
                logger.warning(
                    f"[DOWNLOAD] 视频过长，跳过下载 | post_id={post_id} | duration={video_duration}s | url={media_info['video_url']}"
                )
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
                logger.error(f"[DOWNLOAD] 媒体资源下载失败 | post_id={post_id} | url={url} | error={dl_e}")

        try:
            comment_count = item.get("engagement", {}).get("comment_count", 0)
            if comment_count == 0:
                item["comments"] = []
            else:
                replies = fetch_binance_replies(content_id=post_id, sort_by=1, required_count=10, session=session)
                item["comments"] = replies
                if replies:
                    stats["comments_fetched"] = 1
        except Exception as reply_e:
            logger.error(f"[COMMENTS] 评论拉取失败 | post_id={post_id} | error={reply_e}")

    except Exception as e:
        stats["failed"] = 1
        logger.error(f"[UPDATE] 就地更新多模态信息失败 | post_id={post_id} | error={e}", exc_info=True)

    return item, stats


def update_posts_in_place(final_clean_list):
    """就地并发更新帖子内容、下载媒体与拉取评论"""
    if not final_clean_list:
        return []

    total_stats = {
        "long_updated": 0,
        "other_updated": 0,
        "comments_fetched": 0,
        "total_download_urls": 0,
        "total_download_success": 0,
        "failed": 0,
    }
    max_workers = min(10, len(final_clean_list))
    logger.debug(f"⏳ 开始并发执行 {len(final_clean_list)} 条数据的多模态详情更新、媒体下载与评论拉取...")

    with requests.Session() as session:
        adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_item = {
                executor.submit(_enrich_single_post, item, session): item
                for item in final_clean_list
            }
            for future in concurrent.futures.as_completed(future_to_item):
                try:
                    _, task_stats = future.result()
                    for key in total_stats:
                        total_stats[key] += task_stats.get(key, 0)
                except Exception as exc:
                    logger.error(f"🚨 线程执行发生严重异常: {exc}", exc_info=True)
                    total_stats["failed"] += 1

    logger.info(
        f"✅ [并发拉取] 融合完毕 | 长文更新:{total_stats['long_updated']} | "
        f"媒体拼接:{total_stats['other_updated']} | "
        f"下载成功:{total_stats['total_download_success']}/{total_stats['total_download_urls']} | "
        f"评论拉取:{total_stats['comments_fetched']} | "
        f"失败:{total_stats['failed']}"
    )
    return final_clean_list


# ============================================================
# 4. 币安广场分页采集流 (推荐流 / 搜索流 / 币种流)
# ============================================================
def _extract_vos(res_data):
    """安全解析币安响应体，兼容 data 为 dict / list / 空 的多种结构"""
    data_field = res_data.get("data")
    if isinstance(data_field, dict):
        return data_field.get("vos") or data_field.get("list") or []
    if isinstance(data_field, list):
        return data_field
    return []


def _dedup_vos(all_vos, label):
    """基于 id / contentId 去重"""
    seen_ids = set()
    deduped = []
    for item in all_vos:
        item_id = item.get("id") or item.get("contentId")
        if item_id not in seen_ids:
            seen_ids.add(item_id)
            deduped.append(item)
    logger.debug(
        f"✅ [{label}] 去重完成 | 抓取 {len(all_vos)} 条 | "
        f"移除重复 {len(all_vos) - len(deduped)} 条 | 最终保留 {len(deduped)} 条"
    )
    return deduped


def _paginate_feed(url, headers, build_payload, required_count, label, on_page=None):
    """币安广场分页采集核心引擎（POST 分页流通用）"""
    all_vos = []
    page_index = 1
    retry_count = 0

    while len(all_vos) < required_count:
        payload = build_payload(page_index)
        response = None
        try:
            response = requests.post(
                url, headers=headers, json=payload, proxies=PROXIES, timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            vos = _extract_vos(response.json())
            if not vos:
                break
            all_vos.extend(vos)
            if on_page:
                on_page(vos)
            page_index += 1
            retry_count = 0
            time.sleep(random.uniform(0.5, 1.5))
        except Exception as e:
            detail = ""
            if response is not None and response.status_code >= 400:
                detail = f" | HTTP {response.status_code} | 服务器返回: {response.text[:500]}"
            retry_count += 1
            logger.warning(
                f"🚨 [{label}] 第 {page_index} 页请求失败 (第 {retry_count}/{MAX_RETRIES} 次){detail} | 异常: {e}"
            )
            if retry_count >= MAX_RETRIES:
                logger.error(f"❌ [{label}] 连续失败达到 {MAX_RETRIES} 次上限，终止采集")
                break
            time.sleep(random.uniform(0.5, 1.5))

    return _dedup_vos(all_vos, label)


def fetch_binance_feed_recommend(required_count, content_ids=None):
    """拉取币安广场推荐流数据"""
    if content_ids is None:
        content_ids = []
    url = "https://www.binance.com/bapi/composite/v9/friendly/pgc/feed/feed-recommend/list"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:152.0) Gecko/20100101 Firefox/152.0",
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,zh-TW;q=0.8,zh-HK;q=0.7,en-US;q=0.6,en;q=0.5",
        "lang": "zh-CN",
        "X-UI-REQUEST-TRACE": str(uuid.uuid4()),
        "X-TRACE-ID": str(uuid.uuid4()),
        "Content-Type": "application/json",
        "clienttype": "web",
        "versioncode": "web",
        "BNC-Time-Zone": "Asia/Shanghai",
        "referrer": "https://www.binance.com/zh-CN/square",
    }

    def build_payload(page_index):
        return {
            "pageIndex": page_index,
            "pageSize": PAGE_SIZE,
            "scene": "web-homepage",
            "contentIds": content_ids,
        }

    def collect_ids(vos):
        for item in vos:
            item_id = item.get("id") or item.get("contentId")
            if item_id:
                content_ids.append(str(item_id))

    return _paginate_feed(url, headers, build_payload, required_count, "推荐流", on_page=collect_ids)


def fetch_binance_feed_search(keyword, required_count, search_type=1):
    """拉取币安广场搜索结果"""
    url = "https://www.binance.com/bapi/composite/v2/friendly/pgc/feed/search/list"
    encoded_keyword = quote(keyword)
    headers = {
        "User-Agent": DEFAULT_WEB_UA,
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "device-info": "",
        "lang": "zh-CN",
        "Content-Type": "application/json",
        "clienttype": "web",
        "versioncode": "2.61.0",
        "BNC-UUID": str(uuid.uuid4()),
        "referrer": f"https://www.binance.com/zh-CN/square/search?s={encoded_keyword}",
    }

    def build_payload(page_index):
        return {
            "scene": "web",
            "pageIndex": page_index,
            "pageSize": PAGE_SIZE,
            "searchContent": keyword,
            "type": search_type,
        }

    return _paginate_feed(url, headers, build_payload, required_count, f"搜索:{keyword}")


def get_binance_feed_token(token="DOGE", required_count=20, orderBy=2):
    """获取币安指定币种的社区 Feed 数据 (orderBy: 1=热门, 2=最新)"""
    url = "https://www.binance.com/bapi/composite/v4/friendly/pgc/feed/trade/list"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:152.0) Gecko/20100101 Firefox/152.0",
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,zh-TW;q=0.8,zh-HK;q=0.7,en-US;q=0.6,en;q=0.5",
        "lang": "zh-CN",
        "Content-Type": "application/json",
        "clienttype": "web",
        "device-info": "",
        "BNC-UUID": str(uuid.uuid4()),
        "X-UI-REQUEST-TRACE": str(uuid.uuid4()),
        "X-TRACE-ID": str(uuid.uuid4()),
        "csrftoken": "d41d8cd98f00b204e9800998ecf8427e",
        "BNC-Time-Zone": "Asia/Shanghai",
        "referrer": f"https://www.binance.com/zh-CN/square/community?token={token}",
    }

    logger.info(f"⏳ [币种:{token}] 开始采集 | 目标条数: {required_count} | 排序: {orderBy}")

    def build_payload(page_index):
        return {
            "token": token,
            "pageIndex": page_index,
            "pageSize": PAGE_SIZE,
            "scene": 2,
            "orderBy": orderBy,
        }

    return _paginate_feed(url, headers, build_payload, required_count, f"币种:{token}")


def fetch_binance_feed(count=20, keyword=None, token=None, existing_ids=None, **kwargs):
    """统一的币安广场数据拉取接口（智能路由 + 增量拦截 + 自营拦截 + 清洗富化）"""
    if existing_ids is None:
        existing_ids = set()
    elif isinstance(existing_ids, list):
        existing_ids = set(existing_ids)

    task_label = keyword if keyword else (token if token else "推荐流")

    if keyword:
        logger.info(f"🚀 开始探测: [搜索流] 关键词: {keyword} | 目标条数: {count}")
        search_type = kwargs.get("search_type", 1)
        feed_list = fetch_binance_feed_search(keyword=keyword, required_count=count, search_type=search_type)
    elif token:
        logger.info(f"🚀 开始探测: [币种流] 币种: {token} | 目标条数: {count}")
        order_by = kwargs.get("orderBy", 2)
        feed_list = get_binance_feed_token(token=token, required_count=count, orderBy=order_by)
    else:
        logger.info(f"🚀 开始探测: [推荐流] | 目标条数: {count}")
        content_ids = kwargs.get("content_ids", [])
        feed_list = fetch_binance_feed_recommend(required_count=count, content_ids=content_ids)

    filtered_feed_list = []
    intercepted_count = 0

    for item in feed_list:
        if _is_self_binance_post(item):
            intercepted_count += 1
            continue
        item_id = str(item.get("id") or item.get("contentId"))
        if item_id and item_id not in existing_ids:
            filtered_feed_list.append(item)
            existing_ids.add(item_id)
        else:
            intercepted_count += 1

    if not filtered_feed_list:
        logger.info(
            f"🛑 [{task_label}] 探测完毕 | 抓取:{len(feed_list)}条 | 🛡️历史拦截:{intercepted_count}条 | 净增:0条 -> 跳过后续处理"
        )
        return []

    logger.info(
        f"📥 [{task_label}] 探测完毕 | 抓取:{len(feed_list)}条 | 🛡️历史拦截:{intercepted_count}条 | 净增:{len(filtered_feed_list)}条新帖"
    )

    clean_feed_list = clean_universal_posts(filtered_feed_list)
    return update_posts_in_place(clean_feed_list)


def fetch_binance_post_detail(post_id, session=None):
    """根据文章 ID 获取币安广场帖子的详细内容"""
    url = f"https://www.binance.com/bapi/composite/v3/friendly/pgc/special/content/detail/{post_id}"
    headers = {
        "User-Agent": DEFAULT_WEB_UA,
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "lang": "zh-CN",
        "X-UI-REQUEST-TRACE": str(uuid.uuid4()),
        "X-TRACE-ID": str(uuid.uuid4()),
        "Content-Type": "application/json",
        "clienttype": "web",
        "BNC-Time-Zone": "Asia/Shanghai",
        "referrer": f"https://www.binance.com/zh-CN/square/post/{post_id}",
    }

    req_client = session if session else requests
    retry_count = 0

    while retry_count < MAX_RETRIES:
        response = None
        try:
            response = req_client.get(url, headers=headers, proxies=PROXIES, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data_field = response.json().get("data")
            if not data_field:
                logger.warning(f"⚠️ [详情:{post_id}] 接口未返回有效数据")
            else:
                logger.debug(f"✅ [详情:{post_id}] 获取文章详情成功")
            return data_field
        except Exception as e:
            detail = ""
            if response is not None and response.status_code >= 400:
                detail = f" | HTTP {response.status_code} | 服务器返回: {response.text[:500]}"
            retry_count += 1
            logger.warning(
                f"🚨 [详情:{post_id}] 请求失败 (第 {retry_count}/{MAX_RETRIES} 次){detail} | 异常: {e}"
            )
            if retry_count >= MAX_RETRIES:
                logger.error(f"❌ [详情:{post_id}] 连续失败达到 {MAX_RETRIES} 次上限，终止")
                break
            time.sleep(random.uniform(0.5, 1.5))

    return None


def clean_binance_replies(raw_replies):
    """清洗币安广场评论数据"""
    cleaned_data = []
    for item in raw_replies or []:
        if not isinstance(item, dict):
            continue
        create_time_ms = item.get("createTime")
        original_text = item.get("bodyTextOnly") or ""
        detected_lang = str(item.get("detectedLang") or "")
        is_chinese = detected_lang.startswith("zh") or item.get("lan") == "cn"
        if is_chinese:
            content = original_text
        else:
            translated_data = item.get("translatedData") or {}
            translated_text = translated_data.get("content") or ""
            content = translated_text if translated_text else original_text

        cleaned_data.append({
            "reply_id": item.get("id"),
            "parent_id": item.get("parentContentId"),
            "create_time": create_time_ms,
            "author_uid": item.get("squareUid"),
            "author_name": item.get("displayName"),
            "content": str(content).strip() if content else "",
            "likes": item.get("likeCount", 0),
            "replies": item.get("replyCount", 0),
            "views": item.get("viewCount", 0),
        })
    return cleaned_data


def fetch_binance_replies(content_id, sort_by=1, required_count=10, session=None):
    """获取币安广场帖子的评论列表"""
    url = "https://www.binance.com/bapi/composite/v1/friendly/pgc/replyPost/list"
    headers = _build_public_headers()
    all_replies = []
    page_index = 1
    retry_count = 0
    label = f"评论流: {content_id}"
    req_client = session if session else requests

    while len(all_replies) < required_count:
        payload = {
            "pageIndex": page_index,
            "pageSize": 20,
            "sortBy": sort_by,
            "contentId": int(content_id),
            "authorSquareUid": "",
        }
        response = None
        try:
            response = req_client.post(
                url, headers=headers, json=payload, proxies=PROXIES, timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            json_resp = response.json()
            if not json_resp.get("success"):
                logger.error(f"❌ [{label}] API 业务错误: {json_resp.get('message')}")
                break
            data = json_resp.get("data") or []
            page_items = data if isinstance(data, list) else (data.get("list") or [])
            if not page_items:
                break
            all_replies.extend(page_items)
            page_index += 1
            retry_count = 0
            time.sleep(random.uniform(0.5, 1.5))
        except Exception as e:
            detail = ""
            if response is not None:
                detail = f" | HTTP {response.status_code} | 服务器返回: {response.text[:500]}"
            retry_count += 1
            logger.warning(
                f"🚨 [{label}] 第 {page_index} 页请求失败 (第 {retry_count}/{MAX_RETRIES} 次){detail} | 异常: {e}"
            )
            if retry_count >= MAX_RETRIES:
                logger.error(f"❌ [{label}] 连续失败达到 {MAX_RETRIES} 次上限，终止采集。")
                break
            time.sleep(random.uniform(1.0, 3.0))

    return clean_binance_replies(all_replies)


def fetch_binance_relations(target_username, relation_type, required_count, session=None):
    """聚合获取币安广场指定用户的关注/粉丝列表 ("following" 或 "followers")"""
    if relation_type not in ["following", "followers"]:
        logger.error("❌ relation_type 只能是 'following' 或 'followers'")
        return []

    if relation_type == "following":
        url = "https://www.binance.com/bapi/composite/v3/friendly/pgc/user/following"
        label = f"关注列表: {target_username}"
    else:
        url = "https://www.binance.com/bapi/composite/v3/friendly/pgc/user/queryFollowers"
        label = f"粉丝列表: {target_username}"

    headers = _build_public_headers(
        referer=f"https://www.binance.com/zh-CN/square/profile/{target_username}/{relation_type}"
    )
    headers["origin"] = "https://www.binance.com"

    all_items = []
    page_index = 1
    page_size = 20
    retry_count = 0
    req_client = session if session else requests

    while len(all_items) < required_count:
        if relation_type == "following":
            payload = {
                "targetUsername": target_username,
                "pageIndex": page_index,
                "pageSize": page_size,
            }
        else:
            payload = {
                "username": target_username,
                "pageIndex": page_index,
                "pageSize": page_size,
                "offset": (page_index - 1) * page_size,
            }
        response = None
        try:
            response = req_client.post(
                url, headers=headers, json=payload, proxies=PROXIES, timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            json_resp = response.json()
            if not json_resp.get("success"):
                error_code = str(json_resp.get("code", ""))
                error_msg = json_resp.get("message", "未知错误")
                if error_code == "10003" and page_index > 100:
                    logger.info(f"✅ [{label}] 触及币安最大翻页限制 (第 {page_index} 页)，已获取公开允许的最大数据量。")
                    break
                logger.error(f"❌ [{label}] API 业务错误: {error_msg} | code={error_code}")
                break

            data_obj = json_resp.get("data")
            if not data_obj:
                break
            page_items = data_obj.get("followers", []) if isinstance(data_obj, dict) else []
            if not page_items:
                break
            all_items.extend(page_items)
            page_index += 1
            retry_count = 0
            time.sleep(random.uniform(0.5, 1.5))
        except Exception as e:
            detail = ""
            if response is not None:
                detail = f" | HTTP {response.status_code} | 服务器返回: {response.text[:500]}"
            retry_count += 1
            logger.warning(
                f"🚨 [{label}] 第 {page_index} 页请求失败 (第 {retry_count}/{MAX_RETRIES} 次){detail} | 异常: {e}"
            )
            if retry_count >= MAX_RETRIES:
                logger.error(f"❌ [{label}] 连续失败达到 {MAX_RETRIES} 次上限，终止当前采集。")
                break
            time.sleep(random.uniform(2.0, 4.0))

    return all_items[:required_count]


def fetch_binance_user_profile(username, session=None, timeout=10, max_retries=3):
    """获取币安广场用户的公开主页信息"""
    url = "https://www.binance.com/bapi/composite/v3/friendly/pgc/user/client"
    headers = _build_public_headers()
    payload = {
        "username": username,
        "getFollowCount": True,
        "queryFollowersInfo": True,
        "queryRelationTokens": True,
    }
    label = f"用户主页: {username}"
    req_client = session if session else requests
    retry_count = 0

    while retry_count < max_retries:
        response = None
        try:
            response = req_client.post(url, headers=headers, json=payload, proxies=PROXIES, timeout=timeout)
            response.raise_for_status()
            json_resp = response.json()
            if not json_resp.get("success"):
                logger.error(f"❌ [{label}] API 业务错误: {json_resp.get('message')}")
                return {}
            return json_resp.get("data") or {}
        except Exception as e:
            detail = ""
            if response is not None:
                detail = f" | HTTP {response.status_code} | 服务器返回: {response.text[:500]}"
            retry_count += 1
            logger.warning(f"🚨 [{label}] 请求失败 (第 {retry_count}/{max_retries} 次){detail} | 异常: {e}")
            if retry_count >= max_retries:
                logger.error(f"❌ [{label}] 连续失败达到 {max_retries} 次上限，终止采集。")
                break
            time.sleep(random.uniform(1.0, 3.0))

    return {}


# ============================================================
# 5. 批量互动、删帖与历史内容查询
# ============================================================
def like_and_bookmark(target_post_id_list, cookie_map_info):
    """执行多账号批量点赞与收藏交互"""
    if not target_post_id_list:
        logger.info("[互动任务/过滤] 目标帖子列表为空，无操作执行。")
        return
    if not cookie_map_info:
        logger.error("[互动任务/鉴权] 未提供有效的 cookie_map_info | 结果: [当前批次互动任务中止]")
        return

    total_posts = len(target_post_id_list)
    total_accounts = len(cookie_map_info)
    global_like_success = 0
    global_bookmark_success = 0
    global_like_fail = 0
    global_bookmark_fail = 0

    logger.info(f"[互动任务/开始] 准备处理 {total_posts} 个帖子，动用 {total_accounts} 个账号参与互动")

    for index, post_id in enumerate(target_post_id_list, 1):
        post_like_success = 0
        post_bookmark_success = 0
        for acc, info in cookie_map_info.items():
            try:
                cookies = info.get("cookies")
                csrf_token = info.get("csrf_token")
                if not cookies or not csrf_token:
                    logger.warning(f"[互动任务/鉴权] 账号 {acc} 缺少完整的 cookies 或 csrf_token | 结果: [跳过该账号]")
                    continue
                bookmark_res = toggle_binance_bookmark(post_id, "add", cookies, csrf_token)
                like_res = toggle_binance_like(post_id, "like", cookies, csrf_token)
                if bookmark_res:
                    post_bookmark_success += 1
                    global_bookmark_success += 1
                else:
                    global_bookmark_fail += 1
                if like_res:
                    post_like_success += 1
                    global_like_success += 1
                else:
                    global_like_fail += 1
            except Exception as e:
                global_bookmark_fail += 1
                global_like_fail += 1
                logger.error(f"[互动任务/执行异常] 账号: {acc}, 目标ID: {post_id} | 详情: {e}")

        logger.info(
            f"[互动任务/进度] ({index}/{total_posts}) 帖子ID: {post_id} | "
            f"🌟收藏成功: {post_bookmark_success}/{total_accounts} | "
            f"💖点赞成功: {post_like_success}/{total_accounts}"
        )

    logger.info(
        f"========== 🏁 互动任务总览 ==========\n"
        f" 🎯 处理帖子总数: {total_posts}\n"
        f" 👥 动用账号总数: {total_accounts}\n"
        f" 🌟 累计收藏: {global_bookmark_success} 成功 / {global_bookmark_fail} 失败\n"
        f" 💖 累计点赞: {global_like_success} 成功 / {global_like_fail} 失败\n"
        f"======================================"
    )


def delete_binance_square_content(
    content_id: str,
    cookies: str,
    csrf_token: str,
    content_type: int = 1,
) -> bool:
    """删除币安广场的指定内容/回复"""
    url = "https://www.binance.com/bapi/composite/v1/private/pgc/content/delete"
    headers = _build_private_headers(cookies, csrf_token)
    payload = {"contentId": int(content_id), "contentType": content_type}
    logger.info(f"正在尝试删除广场内容: contentId={content_id}...")

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10, proxies=PROXIES)
        if response.status_code != 200:
            logger.error(f"删除请求失败! HTTP 状态码: {response.status_code}, 响应: {response.text[:150]}")
            return False
        try:
            res_json = response.json()
        except Exception:
            logger.error(f"响应无法解析为 JSON，可能触发了 WAF 拦截: {response.text[:200]}")
            return False

        biz_code = res_json.get("code")
        if biz_code == "000000" and res_json.get("success") is True:
            logger.info(f"内容 contentId={content_id} 删除成功！")
            return True
        else:
            logger.warning(
                f"删除失败: code={biz_code}, message={res_json.get('message')}, detail={res_json.get('messageDetail')}"
            )
            return False
    except Exception as e:
        logger.exception(f"删除操作执行期间发生未捕获异常 (contentId={content_id}): {e}")
        return False


def fetch_binance_square_replies(
    target_square_uid: str,
    cookies: str,
    csrf_token: str,
    limit: int = 0,
    time_offset: int = -1,
    filter_type: str = "ALL",
) -> list:
    """查询币安广场指定用户的动态列表并精简数据"""
    base_url = "https://www.binance.com/bapi/composite/v2/friendly/pgc/content/queryUserProfilePageContentsWithFilter"
    headers = _build_private_headers(cookies, csrf_token)
    results = []
    current_offset = time_offset
    page = 1

    try:
        while True:
            params = {
                "targetSquareUid": target_square_uid,
                "timeOffset": current_offset,
                "filterType": filter_type,
            }
            logger.info(f"正在拉取第 {page} 页数据, timeOffset={current_offset}...")
            try:
                response = requests.get(base_url, headers=headers, params=params, timeout=10, proxies=PROXIES)
            except requests.exceptions.RequestException as e:
                logger.error(f"网络请求发生异常: {e}")
                break

            if response.status_code != 200:
                logger.warning(f"HTTP 请求失败! 状态码: {response.status_code}, 响应: {response.text[:150]}")
                break

            try:
                res_json = response.json()
            except Exception:
                logger.error(f"响应解析 JSON 失败，返回内容: {response.text[:200]}")
                break

            biz_code = res_json.get("code")
            if biz_code != "000000":
                logger.warning(f"币安接口业务报错: code={biz_code}, msg={res_json.get('message')}")
                break

            data = res_json.get("data") or {}
            contents = data.get("contents") or []
            if not contents:
                logger.info(f"第 {page} 页没有查询到回复内容 (contents 为空)，停止翻页。")
                break

            logger.info(f"第 {page} 页成功获取到 {len(contents)} 组互动数据。")

            for item in contents:
                reply_post_list = item.get("replyPostList") or []
                if reply_post_list:
                    outer_id = item.get("id")
                    my_reply = {}
                    origin_post = {}
                    target_idx = -1

                    for i, post in enumerate(reply_post_list):
                        if post.get("id") == outer_id:
                            my_reply = post
                            target_idx = i
                            break
                    if target_idx == -1:
                        for i, post in enumerate(reply_post_list):
                            if post.get("squareUid") == target_square_uid:
                                my_reply = post
                                target_idx = i
                                break
                    if target_idx == -1:
                        my_reply = reply_post_list[-1]
                        target_idx = len(reply_post_list) - 1

                    origin_post = reply_post_list[target_idx - 1] if target_idx > 0 else reply_post_list[0]
                    origin_post = origin_post or {}
                    my_reply = my_reply or {}

                    raw_hyperlinks = my_reply.get("hyperlinkList") or []
                    clean_links = [
                        link.get("url") for link in raw_hyperlinks if isinstance(link, dict) and link.get("url")
                    ]
                    simplified_item = {
                        "reply_id": my_reply.get("id"),
                        "reply_text": my_reply.get("bodyTextOnly") or "",
                        "reply_time": my_reply.get("firstReleaseTime"),
                        "reply_links": clean_links,
                        "parent_post_id": my_reply.get("parentContentId") or origin_post.get("id"),
                        "parent_post_author": origin_post.get("displayName") or origin_post.get("username") or "",
                        "parent_post_text": origin_post.get("bodyTextOnly") or "",
                    }
                else:
                    my_reply = item or {}
                    raw_hyperlinks = my_reply.get("hyperlinkList") or []
                    clean_links = [
                        link.get("url") for link in raw_hyperlinks if isinstance(link, dict) and link.get("url")
                    ]
                    simplified_item = {
                        "reply_id": my_reply.get("id"),
                        "reply_text": my_reply.get("bodyTextOnly") or "",
                        "reply_time": my_reply.get("firstReleaseTime"),
                        "reply_links": clean_links,
                        "parent_post_id": "",
                        "parent_post_author": "",
                        "parent_post_text": "",
                    }

                results.append(simplified_item)
                if 0 < limit <= len(results):
                    logger.info(f"已达到设定的拉取限制 limit={limit}，停止抓取。")
                    return results

            next_offset = data.get("timeOffset")
            if not next_offset or next_offset == current_offset:
                logger.info("已无下一页游标 (timeOffset 耗尽)，拉取完毕。")
                break

            current_offset = next_offset
            page += 1
            time.sleep(0.5)

    except Exception as e:
        logger.exception(f"处理数据时发生意外错误: {e}")
        return results

    logger.info(f"抓取完成，共提取到 {len(results)} 条精简数据。")
    return results


# ============================================================
# 6. 公开榜单与 CCXT 实时价格/涨跌榜查询 (带连接缓存优化)
# ============================================================
def _fetch_public_get_list(url: str, label: str, params=None, inner_data_key=None, session=None, max_retries=3):
    """内部通用：GET 拉取公开榜单接口"""
    headers = _build_public_headers()
    req_client = session if session else requests
    retry_count = 0

    while retry_count < max_retries:
        response = None
        try:
            response = req_client.get(
                url, headers=headers, params=params, proxies=PROXIES, timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            json_resp = response.json()
            if json_resp.get("code") == "000000" and json_resp.get("success"):
                data = json_resp.get("data", [])
                if inner_data_key and isinstance(data, dict):
                    data = data.get(inner_data_key, [])
                logger.info(f"✅ [{label}] 成功 | 提取到 {len(data)} 条记录")
                return data
            logger.error(f"❌ [{label}] API 业务错误 | Code: {json_resp.get('code')} | Msg: {json_resp.get('message')}")
            return []
        except Exception as e:
            detail = ""
            if response is not None:
                detail = f" | HTTP {response.status_code} | 服务器返回: {response.text[:200]}"
            retry_count += 1
            logger.warning(f"🚨 [{label}] 请求失败 (第 {retry_count}/{max_retries} 次){detail} | 异常: {e}")
            if retry_count >= max_retries:
                logger.error(f"❌ [{label}] 连续失败达到 {max_retries} 次上限，终止采集。")
                break
            time.sleep(random.uniform(1.0, 3.0))

    return []


def fetch_binance_hot_hashtags(session=None, max_retries=3):
    """获取币安广场热门话题 (Hashtag) 列表"""
    return _fetch_public_get_list(
        url="https://www.binance.com/bapi/composite/v2/public/pgc/hashtag/hot-list",
        label="获取热门话题",
        inner_data_key="data",
        session=session,
        max_retries=max_retries,
    )


def fetch_binance_spot_hot_coins(currency="USD", session=None, max_retries=3):
    """获取币安现货热门币种 (Hot Coins) 列表"""
    return _fetch_public_get_list(
        url="https://www.binance.com/bapi/apex/v1/public/apex/market/spot/hot-coins",
        label=f"现货热门币种:{currency}",
        params={"currency": currency},
        session=session,
        max_retries=max_retries,
    )


def fetch_binance_future_hot_coins(business_enum="USDT_FUTURES", session=None, max_retries=3):
    """获取币安合约热搜榜数据"""
    return _fetch_public_get_list(
        url="https://www.binance.com/bapi/composite/v1/public/future/external/topSearchList",
        label=f"合约热搜榜:{business_enum}",
        params={"businessEnum": business_enum},
        session=session,
        max_retries=max_retries,
    )


_CCXT_CLIENTS = {}
_CCXT_LOCK = threading.Lock()


def _get_ccxt_binance_client(market_type: str = "future", proxy: str = None):
    """获取或复用缓存的 CCXT Binance 客户端实例，避免高频重复初始化"""
    key = (market_type, proxy or "")
    with _CCXT_LOCK:
        client = _CCXT_CLIENTS.get(key)
        if client is None:
            cfg = {
                "timeout": 10000,
                "enableRateLimit": True,
                "options": {"defaultType": market_type},
            }
            if proxy:
                cfg["proxies"] = {"http": proxy, "https": proxy}
            client = ccxt.binance(cfg)
            _CCXT_CLIENTS[key] = client
        return client


def get_latest_crypto_price(coin_name: str, proxy: str = "http://127.0.0.1:7890") -> float | None:
    """使用 ccxt 获取指定币种最新价格（优先查询U本位合约，查不到则降级查询现货）"""
    if not coin_name or not isinstance(coin_name, str):
        logger.warning("⚠️ [价格查询] 传入的币种名称无效")
        return None

    clean_name = coin_name.replace("$", "").strip().upper()
    quote_candidates = ["USDT", "USDC", "FDUSD", "BUSD"]
    base_coin = clean_name
    specified_quote = None

    if "/" in clean_name:
        parts = clean_name.split("/")
        base_coin, specified_quote = parts[0], parts[1]
    elif ":" in clean_name:
        base_coin = clean_name.split(":")[0].split("/")[0]
    else:
        for q in quote_candidates:
            if clean_name.endswith(q) and len(clean_name) > len(q):
                base_coin = clean_name[: -len(q)]
                specified_quote = q
                break

    quotes_to_try = [specified_quote] if specified_quote else ["USDT", "USDC"]
    if proxy == "http://127.0.0.1:7890":
        proxy = get_proxy_url(default=proxy)

    # 阶段一：优先探测 U本位永续合约
    try:
        future_exchange = _get_ccxt_binance_client("future", proxy)
        for q in quotes_to_try:
            for sym in (f"{base_coin}/{q}:{q}", f"{base_coin}/{q}"):
                try:
                    ticker = future_exchange.fetch_ticker(sym)
                    price = ticker.get("last") or ticker.get("close")
                    if price:
                        price = float(price)
                        logger.info(f"✅ [价格查询-合约命中] {coin_name} -> 交易对: {sym} | 最新价: {price}")
                        return price
                except (ccxt.BadSymbol, ccxt.OrderBookEmpty):
                    continue
                except Exception as e:
                    logger.debug(f"🔍 [价格查询] 合约探测 {sym} 失败: {e}")
    except Exception as e:
        logger.debug(f"🔍 [价格查询] 合约通道查询失败: {e}")

    # 阶段二：降级探测 现货 (Spot)
    try:
        spot_exchange = _get_ccxt_binance_client("spot", proxy)
        for q in quotes_to_try:
            sym = f"{base_coin}/{q}"
            try:
                ticker = spot_exchange.fetch_ticker(sym)
                price = ticker.get("last") or ticker.get("close")
                if price:
                    price = float(price)
                    logger.info(f"✅ [价格查询-现货命中] {coin_name} -> 交易对: {sym} | 最新价: {price}")
                    return price
            except (ccxt.BadSymbol, ccxt.OrderBookEmpty):
                continue
            except Exception as e:
                logger.debug(f"🔍 [价格查询] 现货探测 {sym} 失败: {e}")
    except Exception as e:
        logger.debug(f"🔍 [价格查询] 现货通道查询失败: {e}")

    logger.warning(f"❌ [价格查询失败] {coin_name} 合约与现货均未找到报价")
    return None


def get_top_k_movers(top_k: int = 5) -> dict:
    """获取币安 U本位合约 24h 涨幅榜与跌幅榜前 top_k 名"""
    proxy = get_proxy_url()
    try:
        exchange = _get_ccxt_binance_client("future", proxy)
        tickers = exchange.fetch_tickers()
        ranked = sorted(
            (
                {"symbol": symbol, "percentage": ticker["percentage"]}
                for symbol, ticker in tickers.items()
                if symbol.endswith(":USDT") and ticker.get("percentage") is not None
            ),
            key=lambda item: item["percentage"],
            reverse=True,
        )
        return {"top_gainers": ranked[:top_k], "top_losers": ranked[-top_k:][::-1]}
    except Exception as exc:
        logger.warning(f"⚠️ [行情/获取] 获取涨跌榜异常，降级返回空榜单 | 原因: {exc}")
        return {"top_gainers": [], "top_losers": []}


if __name__ == "__main__":
    logger.info("========== 🚀 币安广场接口快速联通性测试 ==========")
    hot_future_coins = fetch_binance_future_hot_coins()
    hot_spot_coins = fetch_binance_spot_hot_coins()
    recommend_data = fetch_binance_feed(count=10)
    logger.info(f"========== 🏁 测试完成 | 推荐流获取: {len(recommend_data)} 条 ==========")
