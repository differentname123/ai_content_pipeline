# -*- coding: utf-8 -*-
""":authors:
    zhuxiaohu
:create_date:
    2026/4/7 0:23
:last_date:
    2026/4/7 0:23
:description:

"""
import json
import logging
import random
import re
import time
import uuid

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import ccxt
import requests

from biance.biance_playwright import get_auth_tokens_robust
from common.common_utils import (
    get_config, setup_logger, save_json, read_json, download_web_media,
    get_requests_proxies, get_proxy_url, get_media_save_dir
)
from urllib.parse import quote

setup_logger()
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
# 拿到属于当前文件的专属 logger
logger = logging.getLogger(__name__)
import concurrent.futures

# ============================================================
# 全局常量：集中管理代理、分页、重试策略与正则，避免散落重复
# ============================================================
PROXIES = get_requests_proxies() or {}
MEDIA_SAVE_DIR = get_media_save_dir()
PAGE_SIZE = 20
MAX_RETRIES = 5
REQUEST_TIMEOUT = 15

# 模块级预编译正则（性能优化：避免逐字段/逐条重复编译）
_INVISIBLE_RE = re.compile(r'[\u200b-\u200f\ufeff\u202a-\u202e]')  # 零宽字符、排版控制符等
_MULTI_NEWLINE_RE = re.compile(r'\n{3,}')

# 币安自营矩阵账号黑名单：严禁将本方矩阵发布的帖子作为外部素材抓取入库（防止自产自销死循环）
SELF_BINANCE_USERNAMES = {
    "sleepwell_zzz",
    "square-creator-7174ab4dc04d4",
    "square-creator-7ca94046afc63",
    "ruivision",
    "insights_anchor",
    "square-creator-70197d46eb4f8",
    "easyeverything",
    "square-creator-9912ff4d73e93",
    "square-creator-ff5e108f93e48",
    "square-creator-c733fa800b1e1",
}
SELF_BINANCE_AUTHOR_NAMES = {
    "稳定短线",
    "马到功成6",
    "来去之间Zzz",
    "瑞见未来",
}


def _is_self_binance_post(raw_item: dict) -> bool:
    """判断币安原始帖子或清洗后帖子是否属于本方自营矩阵账号"""
    if not isinstance(raw_item, dict):
        return False
    author_obj = raw_item.get("author") or {}
    username = str(
        raw_item.get("username")
        or author_obj.get("username")
        or author_obj.get("author_id")
        or ""
    ).strip().lower()
    author_name = str(
        raw_item.get("authorName")
        or raw_item.get("displayName")
        or author_obj.get("name")
        or author_obj.get("displayName")
        or ""
    ).strip()
    return (username in SELF_BINANCE_USERNAMES) or (author_name in SELF_BINANCE_AUTHOR_NAMES)



def publish_to_binance_square(api_key, text_content):
    """
    根据 Binance Skills Hub 最新规范向币安广场发送发帖请求。

    :param api_key: 币安广场创作者中心生成的 OpenAPI Key
    :param text_content: 帖子正文内容
    """
    # 官方文档指定的 API 端点
    url = "https://www.binance.com/bapi/composite/v1/public/pgc/openApi/content/add"

    # 构造请求头 (Header) - 严格按照规范说明
    headers = {
        "Content-Type": "application/json",
        "clienttype": "binanceSkill",
        "X-Square-OpenAPI-Key": api_key
    }

    # 核心修改：将 "content" 改为官方最新要求的 "bodyTextOnly"
    payload = {
        "bodyTextOnly": text_content
    }

    # 隐藏掉中间的 API Key 字符，避免在控制台全量打印（官方安全规范）
    masked_key = f"{api_key[:5]}...{api_key[-4:]}" if len(api_key) > 10 else "***"
    # 提取内容摘要，防止内容过长刷屏
    text_summary = text_content[:20].replace('\n', ' ') + ("..." if len(text_content) > 20 else "")

    try:
        logger.info(f"⏳ 开始向币安广场发帖 | Key: {masked_key} | 内容摘要: {text_summary}")

        # 使用全局 proxies 和 timeout 配置
        response = requests.post(url, headers=headers, json=payload, proxies=PROXIES, timeout=REQUEST_TIMEOUT)
        result = response.json()

        # 判断业务是否成功
        if result.get('success') or str(result.get('code')) == '000000':
            post_id = result.get('data', {}).get('id')
            post_url = f"https://www.binance.com/square/post/{post_id}" if post_id else "未知链接"
            # 聚合成功日志
            logger.info(f"✅ 发帖成功 | 帖子ID: {post_id} | 直达链接: {post_url}")
            return True

        else:
            # 聚合失败信息、错误码解读、接口原报文至一行，避免多行打印被并发冲散
            error_code = str(result.get('code'))
            hint = " (💡提示: 已达每日发帖上限 Daily limit)" if error_code == '220009' else ""
            logger.error(f"❌ 发帖失败 | 错误码: {error_code}{hint} | 完整响应: {result}")
            return False

    except requests.exceptions.RequestException as e:
        logger.error(f"🚨 发帖网络请求异常 | Key: {masked_key} | 异常信息: {e}")
        return False


def toggle_binance_follow(target_uid, action, cookies, csrf_token, session=None):
    """
    操作币安广场用户的关注状态，单次请求不重试
    """
    if action not in ["follow", "unfollow"]:
        logger.error("❌ 无效的操作类型，只能是 'follow' 或 'unfollow'")
        return False

    url = f"https://www.binance.com/bapi/composite/v2/private/pgc/user/{action}"
    label = f"{'关注' if action == 'follow' else '取消关注'}: {target_uid}"

    # 基础 Headers
    headers = {
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "clienttype": "web",
        "content-type": "application/json",
        "lang": "zh-CN",
        "cookie": cookies,
        "csrftoken": csrf_token,
        "origin": "https://www.binance.com",
        "referer": "https://www.binance.com/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
    }

    payload = {
        "targetSquareUid": target_uid
    }

    req_client = session if session else requests

    try:
        # 直接发起单次请求，带上代理
        response = req_client.post(
            url,
            headers=headers,
            json=payload,
            proxies=PROXIES,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()

        json_resp = response.json()

        if json_resp.get("success"):
            logger.info(f"✅ [{label}] 操作成功！")
            return True
        else:
            code = json_resp.get("code")
            msg = json_resp.get("message", "未知错误")
            logger.error(f"❌ [{label}] API 业务错误 | Code: {code} | Msg: {msg}")
            return False

    except Exception as e:
        detail = ""
        if 'response' in locals() and response is not None:
            detail = f" | HTTP {response.status_code} | 返回数据: {response.text[:200]}"

        logger.error(f"🚨 [{label}] 网络或请求异常{detail} | 报错: {e}")
        return False


def toggle_binance_like(post_id, action, cookies, csrf_token, card_type="BUZZ_SHORT", session=None):
    """
    操作币安广场帖子的点赞状态，单次请求不重试

    :param post_id: 帖子ID (如: 352727546639281)
    :param action: 操作类型，"like" (点赞) 或 "unlike" (取消点赞)
    :param cookies: 登录态 Cookie
    :param csrf_token: CSRF Token
    :param card_type: 卡片类型，默认为 "BUZZ_SHORT"
    :param session: requests.Session 对象 (可选)
    """
    if action not in ["like", "unlike"]:
        logger.error("❌ 无效的操作类型，只能是 'like' 或 'unlike'")
        return False

    # 动态拼接是 /like 还是 /unlike
    url = f"https://www.binance.com/bapi/composite/v1/private/pgc/content/{action}"
    label = f"{'点赞' if action == 'like' else '取消点赞'}帖子: {post_id}"

    # 基础 Headers
    headers = {
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "clienttype": "web",
        "content-type": "application/json",
        "lang": "zh-CN",
        "cookie": cookies,
        "csrftoken": csrf_token,
        "origin": "https://www.binance.com",
        # 动态生成 Referer
        "referer": f"https://www.binance.com/zh-CN/square/post/{post_id}",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
    }

    # 请求体 Payload (like 和 unlike 的 payload 是完全相同的)
    payload = {
        "id": int(post_id),
        "cardType": card_type  # 对应 curl 中的 "BUZZ_SHORT"
    }

    req_client = session if session else requests

    try:
        # 直接发起单次请求，带上代理
        response = req_client.post(
            url,
            headers=headers,
            json=payload,
            proxies=PROXIES,  # 假定全局已定义
            timeout=REQUEST_TIMEOUT  # 假定全局已定义
        )
        response.raise_for_status()

        json_resp = response.json()

        if json_resp.get("success"):
            logger.info(f"✅ [{label}] 操作成功！")
            return True
        else:
            code = json_resp.get("code")
            msg = json_resp.get("message", "未知错误")
            logger.error(f"❌ [{label}] API 业务错误 | Code: {code} | Msg: {msg}")
            return False

    except Exception as e:
        detail = ""
        if 'response' in locals() and response is not None:
            detail = f" | HTTP {response.status_code} | 返回数据: {response.text[:200]}"

        logger.error(f"🚨 [{label}] 网络或请求异常{detail} | 报错: {e}")
        return False


def toggle_binance_bookmark(post_id, action, cookies, csrf_token, session=None):
    """
    操作币安广场帖子的收藏状态，单次请求不重试

    :param post_id: 帖子ID (如: 352727546639281)
    :param action: 操作类型，"add" (收藏) 或 "cancel" (取消收藏)
    :param cookies: 登录态 Cookie
    :param csrf_token: CSRF Token
    :param session: requests.Session 对象 (可选)
    """
    # 根据你提供的 curl，取消收藏对应的是 cancel
    if action not in ["add", "cancel"]:
        logger.error("❌ 无效的操作类型，只能是 'add' 或 'cancel'")
        return False

    # 动态拼接是 /add/bookmark 还是 /cancel/bookmark
    url = f"https://www.binance.com/bapi/composite/v1/private/pgc/content/{action}/bookmark"
    label = f"{'收藏' if action == 'add' else '取消收藏'}帖子: {post_id}"

    # 基础 Headers
    headers = {
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "clienttype": "web",
        "content-type": "application/json",
        "lang": "zh-CN",
        "cookie": cookies,
        "csrftoken": csrf_token,
        "origin": "https://www.binance.com",
        # 动态生成 Referer
        "referer": f"https://www.binance.com/zh-CN/square/post/{post_id}",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
    }

    # 请求体 Payload
    payload = {
        "contentId": int(post_id)
    }

    req_client = session if session else requests

    try:
        # 直接发起单次请求，带上代理
        response = req_client.post(
            url,
            headers=headers,
            json=payload,
            proxies=PROXIES,  # 假定全局已定义
            timeout=REQUEST_TIMEOUT  # 假定全局已定义
        )
        response.raise_for_status()

        json_resp = response.json()

        if json_resp.get("success"):
            logger.info(f"✅ [{label}] 操作成功！")
            return True
        else:
            code = json_resp.get("code")
            msg = json_resp.get("message", "未知错误")
            logger.error(f"❌ [{label}] API 业务错误 | Code: {code} | Msg: {msg}")
            return False

    except Exception as e:
        detail = ""
        if 'response' in locals() and response is not None:
            detail = f" | HTTP {response.status_code} | 返回数据: {response.text[:200]}"

        logger.error(f"🚨 [{label}] 网络或请求异常{detail} | 报错: {e}")
        return False


def get_standard_schema():
    """
    统一的数据骨架 (Schema Template)
    确保所有类型的帖子拥有绝对一致的字段结构，防止后续入库或分析时出现 KeyError
    """
    return {
        # --- 提取到第一层的核心索引与高频过滤字段 ---
        "post_id": None,
        "publish_time": 0,
        "author_id": None,
        "card_type": "UNKNOWN",
        "source": "binance",

        # --- 降级后的内层容器 ---
        "metadata": {
            "url": None,
            "is_ai_generated": False
        },
        "author": {
            "username": None,
            "author_name": None,
            "is_verified": False
        },
        "content": {
            "title": None,
            "text_content": "",
            "hashtags": [],
            "mentioned_coins": []
        },
        "media": {
            "cover_image": None,
            "images": [],
            "inline_images": [],
            "video_url": None,
            "video_duration": 0,
            "local_mapping": {}  # 【新增】用于保存网络 URL -> 本地文件路径 的映射关系
        },
        "engagement": {
            "view_count": 0,
            "like_count": 0,
            "comment_count": 0,
            "share_count": 0,
            "quote_count": 0
        },
        "comments": []
    }


# ============================================================
# 通用原子工具 (跨清洗器复用，保持扁平不深套)
# ============================================================
def _clean_text(raw):
    """清理不可见字符并去首尾空格；空值安全返回空串"""
    if not raw:
        return ""
    return _INVISIBLE_RE.sub('', raw).strip()


def _clean_title(raw):
    """标题清理：空值返回 None，否则清理不可见字符并去空格"""
    if not raw:
        return None
    return _INVISIBLE_RE.sub('', raw).strip()


def _extract_coins(raw_item):
    """
    提取提到的币种：完全依赖 API 结构化字段（摒弃正则防脏数据）
    来源1: coinPairList (去 $ 符号)
    来源2: tradingPairsV2[].code (防漏抓)
    最终合并、去重、统一大写
    """
    coins = [coin.replace('$', '').strip() for coin in (raw_item.get("coinPairList") or []) if coin]

    for pair in (raw_item.get("tradingPairsV2") or []):
        code = pair.get("code")
        if code:
            coins.append(code.strip())

    return list(set(coin.upper() for coin in coins if coin))


def _extract_hashtags(raw_item, merge_keys=None):
    """
    提取并清理 Hashtags（去首尾空格防脏数据）
    默认仅取 hashtagList；传入 merge_keys 时额外合并并去重（长文场景）
    """
    tags = list(raw_item.get("hashtagList") or [])

    if merge_keys:
        for key in merge_keys:
            tags += (raw_item.get(key) or [])
        return list(set(ht.strip() for ht in tags if ht))

    return [ht.strip() for ht in tags if ht]


def _assemble_common(raw_item, card_type_default, title, text_content, hashtags):
    """
    套用统一 Schema，组装所有类型共有的四大区块
    (metadata / author / content / engagement)
    差异化的 media 由各清洗器在返回后自行填充
    """
    data = get_standard_schema()

    # 1. 顶层核心字段直接赋值
    data["post_id"] = raw_item.get("id")
    data["publish_time"] = raw_item.get("date", 0)
    data["author_id"] = raw_item.get("squareAuthorId")
    data["card_type"] = raw_item.get("cardType", card_type_default)

    # 2. 剩余的基础元数据与作者信息
    data["metadata"].update({
        "url": raw_item.get("webLink"),
        "is_ai_generated": raw_item.get("isCreatedByAI", False)
    })

    data["author"].update({
        "username": raw_item.get("username"),
        "author_name": raw_item.get("authorName"),
        "is_verified": raw_item.get("authorVerificationType", 0) > 0
    })

    # 3. 内容与媒体与互动数据组装保持不变
    data["content"].update({
        "title": title,
        "text_content": text_content,
        "hashtags": hashtags,
        "mentioned_coins": _extract_coins(raw_item)
    })

    data["engagement"].update({
        "view_count": raw_item.get("viewCount", 0),
        "like_count": raw_item.get("likeCount", 0),
        "comment_count": raw_item.get("commentCount", 0),
        "share_count": raw_item.get("shareCount", 0),
        "quote_count": raw_item.get("quoteCount", 0)
    })

    return data


# ============================================================
# 各类型专用清洗器 (仅保留差异化的 media 处理)
# ============================================================
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
                hashtags=_extract_hashtags(raw_item)
            )
            data["media"]["images"] = raw_item.get("images", [])
            cleaned_list.append(data)
        except Exception as e:
            failed += 1
            logger.error(f"[BUZZ_SHORT] 帖子清洗失败 | post_id={raw_item.get('id')} | error={e}", exc_info=True)

    logger.debug(f"[BUZZ_SHORT] 清洗完成 | 成功={len(cleaned_list)} 失败={failed}")
    return cleaned_list


def clean_reply_posts(raw_data_list):
    """
    清洗 BUZZ_REPLY_POST_LIST (串烧/盖楼帖) 类型数据
    需先剥离容器，再根据内部真实子帖的类型，动态分发给对应的专用清洗器，防止多模态数据丢失
    """
    cleaned_list = []
    container_failed = 0

    # 建立局部路由字典，复用已有的各类型专用清洗器
    # 这样子帖中的视频、长文就不会丢失独有的媒体提取逻辑
    processor_map = {
        'BUZZ_SHORT': clean_short_posts,
        'BUZZ_VIDEO': clean_video_posts,
        'BUZZ_LONG': clean_long_posts
    }

    for raw_container in raw_data_list:
        try:
            # 仅处理盖楼帖容器
            if raw_container.get("cardType") != "BUZZ_REPLY_POST_LIST":
                continue

            # 获取容器内包裹的子帖列表
            reply_post_list = raw_container.get("replyPostList", [])

            # 将当前容器内的子帖按 cardType 进行分组
            child_group_map = {}
            for raw_item in reply_post_list:
                # 若子帖无类型，兜底降级为短帖处理
                c_type = raw_item.get("cardType", "BUZZ_SHORT")
                child_group_map.setdefault(c_type, []).append(raw_item)

            # 分发给对应的专属清洗器处理
            for c_type, items in child_group_map.items():
                # 如果遇到未知类型，同样使用短帖清洗器兜底
                processor_func = processor_map.get(c_type, clean_short_posts)
                child_cleaned_list = processor_func(items)

                if child_cleaned_list:
                    cleaned_list.extend(child_cleaned_list)

        except Exception as e:
            container_failed += 1
            logger.error(f"[BUZZ_REPLY] 容器解析失败 | container_id={raw_container.get('id')} | error={e}",
                         exc_info=True)

    # 注：子帖清洗的失败日志会由专属清洗器自行打印，这里只统计最终产出和容器解析失败数
    logger.debug(
        f"[BUZZ_REPLY] 容器解析及子帖清洗完成 | 共提取有效子帖={len(cleaned_list)} 条 | 容器失败={container_failed}")
    return cleaned_list


def clean_video_posts(raw_data_list):
    """清洗 BUZZ_VIDEO (视频帖) 类型数据，平铺视频专属资源到 media"""
    cleaned_list = []
    failed = 0

    for raw_item in raw_data_list:
        try:
            data = _assemble_common(
                raw_item,
                card_type_default="BUZZ_VIDEO",
                title=_clean_title(raw_item.get("title")),
                text_content=_clean_text(raw_item.get("content")),
                hashtags=_extract_hashtags(raw_item)
            )

            data["media"]["images"] = raw_item.get("images", [])

            # 修复原潜在 Bug：videoVO / coverMeta 存在但值为 None 时的 AttributeError
            video_vo = raw_item.get("videoVO") or {}
            video_link = raw_item.get("videoLink") or video_vo.get("videoLink")
            if video_link:
                cover_meta = raw_item.get("coverMeta") or {}
                data["media"]["video_url"] = video_link
                data["media"]["cover_image"] = raw_item.get("coverLight") or cover_meta.get("url")
                data["media"]["video_duration"] = raw_item.get("videoTimeSeconds") or video_vo.get("videoTimeSeconds",
                                                                                                   0)

            cleaned_list.append(data)
        except Exception as e:
            failed += 1
            logger.error(f"[BUZZ_VIDEO] 帖子清洗失败 | post_id={raw_item.get('id')} | error={e}", exc_info=True)

    logger.debug(f"[BUZZ_VIDEO] 清洗完成 | 成功={len(cleaned_list)} 失败={failed}")
    return cleaned_list


def clean_long_posts(raw_data_list):
    """
    无损清洗 BUZZ_LONG (长文帖) 类型数据
    忠实保留原始文本，精细化分离封面图与内嵌图
    """
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
                hashtags=_extract_hashtags(raw_item, merge_keys=["hashtagIdentifyList"])
            )

            # 精细化分离封面图 (coverMeta.url 优先，其次 coverLight)
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


def clean_long_posts_detail(raw_data_list):
    """
    针对带有 `body` 富文本结构的长文详情，做核心内容与内嵌媒体的深度解析
    解析失败时优雅降级到 bodyTextOnly / content / subTitle
    """
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
                        """递归解析单个富文本节点"""
                        if not isinstance(node, dict):
                            return ""

                        node_id = node.get("id")
                        config = node.get("config", {})

                        # 图片节点：记录 src 并生成阅读标记
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

                        # 叶子文本节点
                        if node_id in ("RichTextText", "RichTextHashTag", "RichTextCoinPair"):
                            return config.get("content", "")

                        if node_id == "RichTextHardBreak":
                            return "\n"

                        # 容器节点：递归拼接子节点
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
                    raw_text_content = _MULTI_NEWLINE_RE.sub('\n\n', raw_parsed_text).strip()
                    # 清理富文本提取出的不可见字符（此处不能再 strip 破坏内部排版，故单独处理）
                    text_content = _INVISIBLE_RE.sub('', raw_text_content)

                except json.JSONDecodeError:
                    text_content = _clean_text(
                        raw_item.get("bodyTextOnly") or raw_item.get("content") or raw_item.get("subTitle"))
            else:
                text_content = _clean_text(
                    raw_item.get("bodyTextOnly") or raw_item.get("content") or raw_item.get("subTitle"))

            # 提取封面图 (coverMeta.url 优先，其次 cover)
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
                    "inline_images": inline_images
                }
            })

        except Exception as e:
            failed += 1
            logger.error(f"[LONG_DETAIL] 长文详情解析失败 | post_id={raw_item.get('id')} | error={e}", exc_info=True)

    if failed:
        logger.warning(f"[LONG_DETAIL] 详情解析存在失败 | 成功={len(cleaned_list)} 失败={failed}")
    return cleaned_list


# ============================================================
# 主流程编排
# ============================================================
def clean_universal_posts(final_clean_list):
    """
    通用清洗调度：
    1. 按 post_id 去重（节省算力）
    2. 按 cardType 分组统计
    3. 分发到各专用清洗器
    4. 合并结果返回
    """
    card_type_counts = {}
    group_map = {}
    seen_post_ids = set()
    unique_count = 0

    # ---------- 1. 去重 + 自营拦截 + 分组统计 ----------
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
        f"🗂️ 分组去重完成 | 唯一记录={unique_count} | cardType种类={len(card_type_counts)} | 分布={card_type_counts}")

    # ---------- 2. 分发处理与合并 ----------
    all_clean_list = []
    processor_map = {
        'BUZZ_SHORT': clean_short_posts,
        'BUZZ_REPLY_POST_LIST': clean_reply_posts,
        'BUZZ_VIDEO': clean_video_posts,
        'BUZZ_LONG': clean_long_posts
    }

    for card_type, processor_func in processor_map.items():
        if card_type in group_map:
            result_list = processor_func(group_map[card_type])
            if result_list:
                all_clean_list.extend(result_list)

    logger.info(
        f"🗂️ [清洗分发] 完成 | 共处理:{unique_count}条 | 分布:{card_type_counts} | 有效产出:{len(all_clean_list)}条")
    return all_clean_list


def _enrich_single_post(item, session):
    """
    单条帖子数据的多模态丰富处理 (无锁设计原子函数)
    返回: (处理后的item, 统计结果字典)
    """
    stats = {
        'long_updated': 0,
        'other_updated': 0,
        'comments_fetched': 0,
        'total_download_urls': 0,
        'total_download_success': 0,
        'failed': 0
    }

    post_id = item.get('post_id')
    card_type = item.get('card_type')

    if not post_id:
        return item, stats

    try:
        # ---------- 1. 长文：回源深度处理 ----------
        if card_type == 'BUZZ_LONG':
            post_detail = fetch_binance_post_detail(post_id, session=session)
            if post_detail:
                cleaned_details = clean_long_posts_detail([post_detail])
                if cleaned_details:
                    detail_item = cleaned_details[0]
                    core_text = detail_item.get('core_text_content', '')

                    # 补齐长文封面标记供大模型阅读
                    cover_image = detail_item.get('media', {}).get('cover_image')
                    if cover_image:
                        cover_mark = f"[长文封面: {cover_image}]"
                        if cover_mark not in core_text:
                            core_text = f"{cover_mark}\n\n{core_text}" if core_text else cover_mark

                    if core_text:
                        item['content']['text_content'] = core_text

                    item['media']['cover_image'] = cover_image
                    item['media']['inline_images'] = detail_item.get('media', {}).get('inline_images', [])
                    stats['long_updated'] = 1

        # ---------- 2. 非长文：多模态标记融合 ----------
        else:
            media_info = item.get('media', {})
            media_marks = []

            # A. 普通图片
            for img_url in media_info.get('images', []):
                if img_url:
                    media_marks.append(f"[插图: {img_url}]")

            # B. 视频及其封面
            video_url = media_info.get('video_url')
            if video_url:
                cover_image = media_info.get('cover_image')
                if cover_image:
                    media_marks.append(f"[视频封面: {cover_image}]")
                media_marks.append(f"[视频: {video_url}]")

            # C. 拼接到正文尾部
            if media_marks:
                original_text = item.get('content', {}).get('text_content', '').strip()
                marks_text = "\n".join(media_marks)
                if marks_text not in original_text:
                    item['content']['text_content'] = (
                        f"{original_text}\n\n{marks_text}" if original_text else marks_text)

            stats['other_updated'] = 1

        # ---------- 3. 统一拦截媒体资源，下载并建立映射 ----------
        media_info = item.get('media', {})
        if 'local_mapping' not in media_info:
            media_info['local_mapping'] = {}

        urls_to_download = set()

        if media_info.get('cover_image'):
            urls_to_download.add(media_info['cover_image'])
        if media_info.get('video_url'):
            video_duration = media_info.get('video_duration', 0)
            if video_duration < 300:
                urls_to_download.add(media_info['video_url'])
            else:
                logger.warning(
                    f"[DOWNLOAD] 视频过长，跳过下载 | post_id={post_id} | duration={video_duration}s | url={media_info['video_url']}")

        for img_url in media_info.get('images', []):
            if img_url:
                urls_to_download.add(img_url)
        for img_url in media_info.get('inline_images', []):
            if img_url:
                urls_to_download.add(img_url)

        stats['total_download_urls'] = len(urls_to_download)
        proxy_url = PROXIES.get("http")

        for url in urls_to_download:
            try:
                local_path = download_web_media(url=url,
                                                save_dir=MEDIA_SAVE_DIR,
                                                proxy=proxy_url)
                if local_path:
                    media_info['local_mapping'][url] = local_path
                    stats['total_download_success'] += 1
            except Exception as dl_e:
                logger.error(f"[DOWNLOAD] 媒体资源下载失败 | post_id={post_id} | url={url} | error={dl_e}")

        # ---------- 4. 【智能拦截】拉取帖子评论数据 ----------
        try:
            # 前置读取由 _assemble_common 清洗好的评论数
            comment_count = item.get('engagement', {}).get('comment_count', 0)

            if comment_count == 0:
                # 若无评论，直接赋值空列表，严禁发起无意义网络请求
                item['comments'] = []
            else:
                # 若有评论，传入 session 并拉取
                replies = fetch_binance_replies(content_id=post_id, sort_by=1, required_count=10, session=session)
                item['comments'] = replies
                if replies:
                    stats['comments_fetched'] = 1
        except Exception as reply_e:
            logger.error(f"[COMMENTS] 评论拉取失败 | post_id={post_id} | error={reply_e}")

    except Exception as e:
        stats['failed'] = 1
        logger.error(f"[UPDATE] 就地更新多模态信息失败 | post_id={post_id} | error={e}", exc_info=True)

    return item, stats


def update_posts_in_place(final_clean_list):
    """
    就地更新(In-place)帖子内容：
    - 多线程并发重构版：启用 TCP Keep-Alive 与无锁聚合统计，彻底解决同步 I/O 阻塞。
    """
    if not final_clean_list:
        return []

    # 初始化主线程负责的全局聚合计数器
    total_stats = {
        'long_updated': 0,
        'other_updated': 0,
        'comments_fetched': 0,
        'total_download_urls': 0,
        'total_download_success': 0,
        'failed': 0
    }

    logger.debug(f"⏳ 开始并发执行 {len(final_clean_list)} 条数据的多模态详情更新、媒体下载与评论拉取...")

    # 启用全局 Session 以实现 TCP 连接复用，大幅降低网络延迟
    with requests.Session() as session:
        # 控制并发粒度，最多 10 个线程，防止触发风控或本地内存溢出
        max_workers = min(10, len(final_clean_list))

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 将每个 item 的处理任务提交给线程池
            future_to_item = {
                executor.submit(_enrich_single_post, item, session): item
                for item in final_clean_list
            }

            # 无论任务谁先完成，安全地回收数据并汇总统计
            for future in concurrent.futures.as_completed(future_to_item):
                try:
                    _, task_stats = future.result()
                    # 主线程安全累加，无竞态条件
                    for key in total_stats:
                        total_stats[key] += task_stats.get(key, 0)
                except Exception as exc:
                    logger.error(f"🚨 线程执行发生严重异常: {exc}", exc_info=True)
                    total_stats['failed'] += 1

    # 最终结果高密度日志输出
    logger.info(
        f"✅ [并发拉取] 融合完毕 | 长文更新:{total_stats['long_updated']} | "
        f"媒体拼接:{total_stats['other_updated']} | "
        f"下载成功:{total_stats['total_download_success']}/{total_stats['total_download_urls']} | "
        f"评论拉取:{total_stats['comments_fetched']} | "
        f"失败:{total_stats['failed']}"
    )

    return final_clean_list


def _extract_vos(res_data):
    """安全解析币安响应体，兼容 data 为 dict / list / 空 的多种结构"""
    data_field = res_data.get("data")
    if isinstance(data_field, dict):
        return data_field.get("vos") or data_field.get("list") or []
    if isinstance(data_field, list):
        return data_field
    return []


def _dedup_vos(all_vos, label):
    """基于 id / contentId 去重，并打印一条高密度的去重结果日志"""
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
    """
    币安广场分页采集核心引擎（POST 分页流通用）

    :param url:            接口地址
    :param headers:        端点专属请求头
    :param build_payload:  接收 page_index，返回该页 payload 的构造函数
    :param required_count: 目标采集条数
    :param label:          业务标签，用于日志上下文（如 "推荐流"、"搜索:ETH"）
    :param on_page:        每页成功后的回调 (vos)，用于推荐流回填 content_ids 等场景
    :return:               去重后的 vos 列表
    """
    all_vos = []
    page_index = 1
    retry_count = 0

    while len(all_vos) < required_count:
        payload = build_payload(page_index)
        response = None
        try:
            response = requests.post(
                url, headers=headers, json=payload,
                proxies=PROXIES, timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()

            vos = _extract_vos(response.json())
            if not vos:
                # logger.debug(
                #     f"⚠️ [{label}] 第 {page_index} 页无更多数据，采集结束（已获取 {len(all_vos)} 条）"
                # )
                break

            all_vos.extend(vos)
            if on_page:
                on_page(vos)

            # logger.debug(
            #     f"📥 [{label}] 第 {page_index} 页成功 | "
            #     f"新增 {len(vos)} 条 | 累计 {len(all_vos)}/{required_count}"
            # )

            page_index += 1
            retry_count = 0
            time.sleep(random.uniform(0.5, 1.5))  # 防风控休眠

        except Exception as e:
            # 聚合式单条错误日志：一行内还原完整排查上下文
            detail = ""
            if response is not None and response.status_code >= 400:
                detail = f" | HTTP {response.status_code} | 服务器返回: {response.text[:500]}"
            retry_count += 1
            logger.warning(
                f"🚨 [{label}] 第 {page_index} 页请求失败 "
                f"(第 {retry_count}/{MAX_RETRIES} 次){detail} | 异常: {e}"
            )
            if retry_count >= MAX_RETRIES:
                logger.error(f"❌ [{label}] 连续失败达到 {MAX_RETRIES} 次上限，终止采集")
                break
            time.sleep(random.uniform(0.5, 1.5))  # 异常退避休眠，避免高频空转轰炸

    return _dedup_vos(all_vos, label)


def fetch_binance_feed_recommend(required_count, content_ids=None):
    """
    不断拉取币安广场的推荐数据，直到满足需要的数量

    :param required_count: 需要拉取的目标数据总数 (vos 的数量)
    :param content_ids: 已经抓取过的内容ID列表，用于去重和向下滑动参数
    :return: 包含目标数据的列表
    """
    # 修复 Python 危险的默认可变参数问题
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
        "referrer": "https://www.binance.com/zh-CN/square"
    }

    def build_payload(page_index):
        return {
            "pageIndex": page_index,
            "pageSize": PAGE_SIZE,
            "scene": "web-homepage",
            "contentIds": content_ids
        }

    def collect_ids(vos):
        # 推荐流独有：将本页 ID 回填至 content_ids，作为下一页的下滑去重参数
        for item in vos:
            item_id = item.get("id") or item.get("contentId")
            if item_id:
                content_ids.append(str(item_id))

    return _paginate_feed(url, headers, build_payload, required_count, "推荐流", on_page=collect_ids)


def fetch_binance_feed_search(keyword, required_count, search_type=1):
    """
    不断拉取币安广场的搜索结果，直到满足需要的数量或没有更多数据

    :param keyword: 搜索关键词 (例如 "doge")
    :param required_count: 需要拉取的目标数据总数
    :param search_type: 搜索类型，默认为1 (通常代表综合或文章)
    :return: 包含目标搜索结果的列表
    """
    url = "https://www.binance.com/bapi/composite/v2/friendly/pgc/feed/search/list"
    encoded_keyword = quote(keyword)
    headers = {
        "User-Agent": "Mozilla/5.0 ...",
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9...",
        "device-info": "",
        "lang": "zh-CN",
        "Content-Type": "application/json",
        "clienttype": "web",
        "versioncode": "2.61.0",
        "BNC-UUID": str(uuid.uuid4()),
        "referrer": f"https://www.binance.com/zh-CN/square/search?s={encoded_keyword}"
    }

    def build_payload(page_index):
        return {
            "scene": "web",
            "pageIndex": page_index,
            "pageSize": PAGE_SIZE,
            "searchContent": keyword,
            "type": search_type
        }

    return _paginate_feed(url, headers, build_payload, required_count, f"搜索:{keyword}")


def get_binance_feed_token(token="DOGE", required_count=20, orderBy=2):
    """
    获取币安指定币种的社区 Feed 数据

    :param token: 币种名称 (如 "DOGE")
    :param required_count: 期望获取的数据条数，默认为 20
    :param orderBy: 1 代表热门，2 代表最新
    :return: 目标 vos 列表，失败或无数据时返回 []
    """
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
        "referrer": f"https://www.binance.com/zh-CN/square/community?token={token}"
    }

    logger.info(f"⏳ [币种:{token}] 开始采集 | 目标条数: {required_count} | 排序: {orderBy}")

    def build_payload(page_index):
        return {
            "token": token,
            "pageIndex": page_index,
            "pageSize": PAGE_SIZE,
            "scene": 2,
            "orderBy": orderBy
        }

    return _paginate_feed(url, headers, build_payload, required_count, f"币种:{token}")


def fetch_binance_feed(count=20, keyword=None, token=None, existing_ids=None, **kwargs):
    """
    统一的币安广场数据拉取接口（智能路由版 + 增量拦截）
    """
    # 确保 existing_ids 是一个集合（Set），保证 O(1) 的极速查询效率
    if existing_ids is None:
        existing_ids = set()
    elif isinstance(existing_ids, list):
        existing_ids = set(existing_ids)

    # 提取标签供后续聚合日志使用
    task_label = keyword if keyword else (token if token else "推荐流")

    # 1. 优先判断是否是“搜索流” (传了 keyword)
    if keyword:
        logger.info(f"🚀 开始探测: [搜索流] 关键词: {keyword} | 目标条数: {count}")
        search_type = kwargs.get("search_type", 1)
        feed_list = fetch_binance_feed_search(keyword=keyword, required_count=count, search_type=search_type)

    # 2. 判断是否是“指定币种流” (传了 token)
    elif token:
        logger.info(f"🚀 开始探测: [币种流] 币种: {token} | 目标条数: {count}")
        order_by = kwargs.get("orderBy", 2)
        feed_list = get_binance_feed_token(token=token, required_count=count, orderBy=order_by)

    # 3. 如果什么目标都没传，默认走“推荐流”
    else:
        logger.info(f"🚀 开始探测: [推荐流] | 目标条数: {count}")
        content_ids = kwargs.get("content_ids", [])
        feed_list = fetch_binance_feed_recommend(required_count=count, content_ids=content_ids)

    # ==========================================
    # 🛡️ 核心改造：在这里建立“早期拦截护城河”
    # ==========================================
    filtered_feed_list = []
    intercepted_count = 0

    for item in feed_list:
        if _is_self_binance_post(item):
            intercepted_count += 1
            continue
        # 兼容两种常见的 ID 字段名，并统一转为字符串处理
        item_id = str(item.get("id") or item.get("contentId"))

        if item_id and item_id not in existing_ids:
            filtered_feed_list.append(item)
            # 顺手把新 ID 加进护城河，防止同一批次内出现重复
            existing_ids.add(item_id)
        else:
            intercepted_count += 1

    # 零数据阻断（Early Exit），极大减少不必要的日志和计算
    if len(filtered_feed_list) == 0:
        logger.info(
            f"🛑 [{task_label}] 探测完毕 | 抓取:{len(feed_list)}条 | 🛡️历史拦截:{intercepted_count}条 | 净增:0条 -> 跳过后续处理")
        return []
    else:
        logger.info(
            f"📥 [{task_label}] 探测完毕 | 抓取:{len(feed_list)}条 | 🛡️历史拦截:{intercepted_count}条 | 净增:{len(filtered_feed_list)}条新帖")
    # ==========================================

    # 只有【纯净的新数据】才有资格进入下面极其耗时的清洗和下载流程
    clean_feed_list = clean_universal_posts(filtered_feed_list)
    final_clean_feed_list = update_posts_in_place(clean_feed_list)

    return final_clean_feed_list


def fetch_binance_post_detail(post_id, session=None):
    """
    根据文章 ID 获取币安广场帖子的详细内容 (支持传入 Session 复用连接)
    """
    url = f"https://www.binance.com/bapi/composite/v3/friendly/pgc/special/content/detail/{post_id}"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "lang": "zh-CN",
        "X-UI-REQUEST-TRACE": str(uuid.uuid4()),
        "X-TRACE-ID": str(uuid.uuid4()),
        "Content-Type": "application/json",
        "clienttype": "web",
        "BNC-Time-Zone": "Asia/Shanghai",
        "referrer": f"https://www.binance.com/zh-CN/square/post/{post_id}"
    }

    # 动态选择请求客户端 (外部传入了 session 则复用，否则降级用 requests)
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
    """
    清洗币安广场评论数据，提取高价值字段并进行扁平化处理

    :param raw_replies: 从 API 获取的原始 JSON 列表
    :return: 清洗后的精简字典列表
    """
    cleaned_data = []

    for item in raw_replies:
        # 1. 格式化时间 (将 13 位毫秒级时间戳转换为直观字符串)
        create_time_ms = item.get("createTime")

        # 2. 文本内容策略：中文直接用原文，非中文优先使用自带的翻译数据
        original_text = item.get("bodyTextOnly", "")
        detected_lang = item.get("detectedLang", "")

        # 判断原文是否为中文 (包含 zh-CN, zh-TW 等)
        is_chinese = detected_lang.startswith("zh") or item.get("lan") == "cn"

        if is_chinese:
            content = original_text
        else:
            # 非中文时，尝试获取 translatedData 中的 content
            translated_data = item.get("translatedData") or {}
            translated_text = translated_data.get("content", "")

            # 如果有翻译结果，使用翻译结果；如果没有（比如系统没来得及翻译），则用原文兜底
            content = translated_text if translated_text else original_text

        # 3. 构建极简数据结构
        clean_item = {
            "reply_id": item.get("id"),
            "parent_id": item.get("parentContentId"),
            "create_time": create_time_ms,

            # 作者信息
            "author_uid": item.get("squareUid"),
            "author_name": item.get("displayName"),

            # 统一处理后的正文内容
            "content": content.strip() if content else "",

            # 互动数据
            "likes": item.get("likeCount", 0),
            "replies": item.get("replyCount", 0),
            "views": item.get("viewCount", 0)
        }

        cleaned_data.append(clean_item)

    return cleaned_data


def fetch_binance_replies(content_id, sort_by=1, required_count=10, session=None):
    """
    获取币安广场帖子的评论列表 (支持传入 Session 复用连接)
    """
    url = "https://www.binance.com/bapi/composite/v1/friendly/pgc/replyPost/list"

    headers = {
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "lang": "zh-CN",
        "clienttype": "web",
        "content-type": "application/json",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }

    all_replies = []
    page_index = 1
    retry_count = 0
    label = f"评论流: {content_id}"

    # 动态选择请求客户端
    req_client = session if session else requests

    while len(all_replies) < required_count:
        payload = {
            "pageIndex": page_index,
            "pageSize": 20,
            "sortBy": sort_by,
            "contentId": int(content_id),
            "authorSquareUid": ""
        }

        response = None
        try:
            response = req_client.post(
                url,
                headers=headers,
                json=payload,
                proxies=PROXIES,
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()

            json_resp = response.json()

            if not json_resp.get("success"):
                logger.error(f"❌ [{label}] API 业务错误: {json_resp.get('message')}")
                break

            data = json_resp.get("data", [])
            page_items = data if isinstance(data, list) else data.get("list", [])

            if not page_items:
                # logger.debug(f"⚠️ [{label}] 第 {page_index} 页无更多数据，采集结束 (已获取 {len(all_replies)} 条)")
                break

            all_replies.extend(page_items)

            # logger.debug(
            #     f"📥 [{label}] 第 {page_index} 页成功 | "
            #     f"新增 {len(page_items)} 条 | 累计 {len(all_replies)}/{required_count}"
            # )

            page_index += 1
            retry_count = 0
            time.sleep(random.uniform(0.5, 1.5))

        except Exception as e:
            detail = ""
            if response is not None:
                detail = f" | HTTP {response.status_code} | 服务器返回: {response.text[:500]}"

            retry_count += 1
            logger.warning(
                f"🚨 [{label}] 第 {page_index} 页请求失败 "
                f"(第 {retry_count}/{MAX_RETRIES} 次){detail} | 异常: {e}"
            )

            if retry_count >= MAX_RETRIES:
                logger.error(f"❌ [{label}] 连续失败达到 {MAX_RETRIES} 次上限，终止采集。")
                break

            time.sleep(random.uniform(1.0, 3.0))

    clean_all_replies = clean_binance_replies(all_replies)

    return clean_all_replies


def fetch_binance_relations(target_username, relation_type, required_count, session=None):
    """
    聚合获取币安广场指定用户的关注/粉丝列表 (无需登录鉴权)
    参数:
        - relation_type: "following" (获取关注) 或 "followers" (获取粉丝)
    """
    if relation_type not in ["following", "followers"]:
        logger.error("❌ relation_type 只能是 'following' 或 'followers'")
        return []

    # 根据不同类型分配 URL 和日志标签
    if relation_type == "following":
        url = "https://www.binance.com/bapi/composite/v3/friendly/pgc/user/following"
        label = f"关注列表: {target_username}"
    else:
        url = "https://www.binance.com/bapi/composite/v3/friendly/pgc/user/queryFollowers"
        label = f"粉丝列表: {target_username}"

    headers = {
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "clienttype": "web",
        "content-type": "application/json",
        "lang": "zh-CN",
        "origin": "https://www.binance.com",
        "referer": f"https://www.binance.com/zh-CN/square/profile/{target_username}/{relation_type}",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
    }

    all_items = []
    page_index = 1
    page_size = 20
    retry_count = 0

    req_client = session if session else requests

    while len(all_items) < required_count:
        # 构造差异化的 Payload
        if relation_type == "following":
            payload = {
                "targetUsername": target_username,
                "pageIndex": page_index,
                "pageSize": page_size
            }
        else:
            payload = {
                "username": target_username,
                "pageIndex": page_index,
                "pageSize": page_size,
                "offset": (page_index - 1) * page_size
            }

        response = None
        try:
            response = req_client.post(
                url,
                headers=headers,
                json=payload,
                proxies=PROXIES,
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()

            json_resp = response.json()

            if not json_resp.get("success"):
                # ================= 修改区域开始 =================
                error_code = str(json_resp.get("code", ""))
                error_msg = json_resp.get('message', '未知错误')

                # 适配健壮性：处理币安 API 官方的最大翻页限制 (通常限制 100 页)
                if error_code == "10003" and page_index > 100:
                    logger.info(f"✅ [{label}] 触及币安最大翻页限制 (第 {page_index} 页)，已获取公开允许的最大数据量。")
                    break

                # 增强日志，打印详尽的请求上下文以便排查真实的"参数错误"或其它异常
                logger.error(
                    f"❌ [{label}] API 业务错误: {error_msg}\n"
                    f"▼▼▼▼▼ 详细排查上下文 ▼▼▼▼▼\n"
                    f"🔗 请求URL: {url}\n"
                    f"📦 请求Payload: {payload}\n"
                    f"📋 请求Headers: {headers}\n"
                    f"🖨️ HTTP状态码: {response.status_code}\n"
                    f"📄 服务器完整返回: {response.text}\n"
                    f"▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲"
                )
                break
                # ================= 修改区域结束 =================

            data_obj = json_resp.get("data")
            if not data_obj:
                logger.info(f"⚠️ [{label}] 第 {page_index} 页无数据，采集提前结束")
                break

            # 差异化提取返回数据
            page_items = data_obj.get("followers", []) if isinstance(data_obj, dict) else []
            if not page_items:
                # logger.info(f"⚠️ [{label}] 第 {page_index} 页无更多数据，采集结束 (已获取 {len(all_items)} 条)")
                break

            all_items.extend(page_items)

            # logger.info(
            #     f"📥 [{label}] 第 {page_index} 页成功 | "
            #     f"新增 {len(page_items)} 条 | 累计 {len(all_items)}/{required_count}"
            # )

            page_index += 1
            retry_count = 0
            time.sleep(random.uniform(0.5, 1.5))

        except Exception as e:
            detail = ""
            if response is not None:
                detail = f" | HTTP {response.status_code} | 服务器返回: {response.text[:500]}"

            retry_count += 1
            logger.warning(
                f"🚨 [{label}] 第 {page_index} 页请求失败 "
                f"(第 {retry_count}/{MAX_RETRIES} 次){detail} | 异常: {e}"
            )

            if retry_count >= MAX_RETRIES:
                logger.error(f"❌ [{label}] 连续失败达到 {MAX_RETRIES} 次上限，终止当前采集。")
                break

            time.sleep(random.uniform(2.0, 4.0))

    return all_items[:required_count]


def fetch_binance_user_profile(username, session=None, timeout=10, max_retries=3):
    """
    获取币安广场用户的公开主页信息

    :param username: 目标用户名 (例如: "Insights_Anchor")
    :param session: 外部传入的 requests.Session() 对象，用于连接池复用
    :param proxies: 代理配置，例如 {"http": "...", "https": "..."}
    :param timeout: 请求超时时间
    :param max_retries: 最大重试次数
    :return: 包含用户信息的字典，失败则返回 None
    """
    url = "https://www.binance.com/bapi/composite/v3/friendly/pgc/user/client"

    # 精简 Headers：保留基础的语言、客户端类型和 User-Agent 即可满足公共接口校验
    headers = {
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "lang": "zh-CN",
        "clienttype": "web",
        "content-type": "application/json",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
    }

    # 构造请求体，默认开启所有的查询开关
    payload = {
        "username": username,
        "getFollowCount": True,
        "queryFollowersInfo": True,
        "queryRelationTokens": True
    }

    label = f"用户主页: {username}"
    req_client = session if session else requests
    retry_count = 0

    # 采用和你参考代码一致的重试逻辑，但去掉了分页的 while
    while retry_count < max_retries:
        response = None
        try:
            response = req_client.post(
                url,
                headers=headers,
                json=payload,
                proxies=PROXIES,
                timeout=timeout
            )
            response.raise_for_status()

            json_resp = response.json()

            # 校验币安 API 业务层的 success 字段
            if not json_resp.get("success"):
                logger.error(f"❌ [{label}] API 业务错误: {json_resp.get('message')}")
                return {}

            data = json_resp.get("data")
            logger.debug(f"📥 [{label}] 数据获取成功")
            return data

        except Exception as e:
            detail = ""
            if response is not None:
                detail = f" | HTTP {response.status_code} | 服务器返回: {response.text[:500]}"

            retry_count += 1
            logger.warning(
                f"🚨 [{label}] 请求失败 "
                f"(第 {retry_count}/{max_retries} 次){detail} | 异常: {e}"
            )

            if retry_count >= max_retries:
                logger.error(f"❌ [{label}] 连续失败达到 {max_retries} 次上限，终止采集。")
                break

            # 失败后随机休眠 1~3 秒再重试，防封控
            time.sleep(random.uniform(1.0, 3.0))

    return {}


def like_and_bookmark(target_post_id_list, cookie_map_info):
    """
    执行核心点赞与收藏交互。

    :param target_post_id_list: 包含目标帖子或评论 ID (字符串/数字) 的列表。
    :param cookie_map_info: 字典，包含各个账号的凭证信息。
                            格式要求: {"account_name": {"cookies": "...", "csrf_token": "..."}}
    """
    if not target_post_id_list:
        logger.info("[互动任务/过滤] 目标帖子列表为空，无操作执行。")
        return

    if not cookie_map_info:
        logger.error("[互动任务/鉴权] 未提供有效的 cookie_map_info | 结果: [当前批次互动任务中止]")
        return

    total_posts = len(target_post_id_list)
    total_accounts = len(cookie_map_info)

    # 全局统计指标
    global_like_success = 0
    global_bookmark_success = 0
    global_like_fail = 0
    global_bookmark_fail = 0

    logger.info(f"[互动任务/开始] 准备处理 {total_posts} 个帖子，动用 {total_accounts} 个账号参与互动")

    for index, post_id in enumerate(target_post_id_list, 1):
        # 单个帖子的统计指标
        post_like_success = 0
        post_bookmark_success = 0

        for acc, info in cookie_map_info.items():
            try:
                cookies = info.get("cookies")
                csrf_token = info.get("csrf_token")

                if not cookies or not csrf_token:
                    logger.warning(f"[互动任务/鉴权] 账号 {acc} 缺少完整的 cookies 或 csrf_token | 结果: [跳过该账号]")
                    continue

                # 执行收藏与点赞，并接收返回值以统计成功数量
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
                logger.error(
                    f"[互动任务/执行异常] 账号: {acc}, 目标ID: {post_id} | 详情: {e}"
                )

        # 每一个帖子处理完毕后，输出一行进度与统计日志
        logger.info(
            f"[互动任务/进度] ({index}/{total_posts}) 帖子ID: {post_id} | "
            f"🌟收藏成功: {post_bookmark_success}/{total_accounts} | "
            f"💖点赞成功: {post_like_success}/{total_accounts}"
        )

    # 所有帖子处理完毕，输出最终的总览统计日志
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
    """删除币安广场的指定内容/回复

    :param content_id: 要删除的内容/回复ID (即 query 出来的 reply_id)
    :param cookies: 请求所需的完整 Cookie 字符串 (需含登录态与防爬token)
    :param csrf_token: 请求所需的 CSRF Token
    :param content_type: 内容类型，普通回复/帖子默认为 1
    :return: 成功返回 True，失败返回 False
    """
    url = "https://www.binance.com/bapi/composite/v1/private/pgc/content/delete"

    # 精简至必要的核心 Headers
    headers = {
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "clienttype": "web",
        "content-type": "application/json",
        "lang": "zh-CN",
        "cookie": cookies,
        "csrftoken": csrf_token,
        "origin": "https://www.binance.com",
        "referer": "https://www.binance.com/",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36"
        ),
    }

    # 请求载荷
    payload = {"contentId": int(content_id), "contentType": content_type}

    logger.info(f"正在尝试删除广场内容: contentId={content_id}...")

    try:
        response = requests.post(
            url, headers=headers, json=payload, timeout=10, proxies=PROXIES
        )

        # 1. 校验 HTTP 状态
        if response.status_code != 200:
            logger.error(
                f"删除请求失败! HTTP 状态码: {response.status_code}, 响应: {response.text[:150]}"
            )
            return False

        # 2. 校验 JSON 解析
        try:
            res_json = response.json()
        except Exception:
            logger.error(
                f"响应无法解析为 JSON，可能触发了 WAF 拦截: {response.text[:200]}"
            )
            return False

        # 3. 校验业务 Code
        biz_code = res_json.get("code")
        if biz_code == "000000" and res_json.get("success") is True:
            logger.info(f"内容 contentId={content_id} 删除成功！")
            return True
        else:
            logger.warning(
                f"删除失败: code={biz_code}, message={res_json.get('message')}, "
                f"detail={res_json.get('messageDetail')}"
            )
            return False

    except Exception as e:
        logger.exception(
            f"删除操作执行期间发生未捕获异常 (contentId={content_id}): {e}"
        )
        return False


def fetch_binance_square_replies(
        target_square_uid: str,
        cookies: str,
        csrf_token: str,
        limit: int = 0,
        time_offset: int = -1,
        filter_type: str = "ALL",  # 【修改1】增加 filter_type 参数，默认改为 ALL。还有 REPLY 这个值
) -> list:
    """查询币安广场指定用户的动态列表并精简数据"""
    base_url = "https://www.binance.com/bapi/composite/v2/friendly/pgc/content/queryUserProfilePageContentsWithFilter"

    headers = {
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "clienttype": "web",
        "content-type": "application/json",
        "lang": "zh-CN",
        "cookie": cookies,
        "csrftoken": csrf_token,
        "origin": "https://www.binance.com",
        "referer": "https://www.binance.com/",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
        ),
    }

    results = []
    current_offset = time_offset
    page = 1

    try:
        while True:
            params = {
                "targetSquareUid": target_square_uid,
                "timeOffset": current_offset,
                "filterType": filter_type,  # 【修改1】此处应用动态参数
            }

            logger.info(f"正在拉取第 {page} 页数据, timeOffset={current_offset}...")

            try:
                response = requests.get(
                    base_url, headers=headers, params=params, timeout=10, proxies=PROXIES
                )
            except requests.exceptions.RequestException as e:
                logger.error(f"网络请求发生异常: {e}")
                break

            # 1. HTTP 状态码校验
            if response.status_code != 200:
                logger.warning(
                    f"HTTP 请求失败! 状态码: {response.status_code}, 响应: {response.text[:150]}"
                )
                break

            # 2. JSON 解析校验
            try:
                res_json = response.json()
            except Exception:
                logger.error(f"响应解析 JSON 失败，返回内容: {response.text[:200]}")
                break

            # 3. 业务状态码校验
            biz_code = res_json.get("code")
            if biz_code != "000000":
                logger.warning(
                    f"币安接口业务报错: code={biz_code}, msg={res_json.get('message')}"
                )
                break

            data = res_json.get("data") or {}
            contents = data.get("contents") or []

            if not contents:
                logger.info(f"第 {page} 页没有查询到回复内容 (contents 为空)，停止翻页。")
                break

            logger.info(f"第 {page} 页成功获取到 {len(contents)} 组互动数据。")

            # 4. 数据解析与精简（强化多层级逻辑与空指针防护）
            for item in contents:
                reply_post_list = item.get("replyPostList") or []

                # 【修改2】：增加分支判断，兼容 ALL (包含原创) 和 REPLY (纯回复)
                if reply_post_list:
                    # ==========================================
                    # 分支 A：有嵌套回复列表，走原来的完整解析逻辑
                    # ==========================================
                    outer_id = item.get("id")  # 当前互动事件的精确ID
                    my_reply = {}
                    origin_post = {}
                    target_idx = -1

                    # 策略 A: 先通过外层精确 ID 匹配目标回复节点 (最准确)
                    for i, post in enumerate(reply_post_list):
                        if post.get("id") == outer_id:
                            my_reply = post
                            target_idx = i
                            break

                    # 策略 B: 如果 ID 匹配失败（防范 API 变更），降级通过 target_square_uid 匹配
                    if target_idx == -1:
                        for i, post in enumerate(reply_post_list):
                            if post.get("squareUid") == target_square_uid:
                                my_reply = post
                                target_idx = i
                                break

                    # 策略 C: 实在匹配不到，采用保守后备方案：最后一条当自己，倒数第二条当父级
                    if target_idx == -1:
                        my_reply = reply_post_list[-1]
                        target_idx = len(reply_post_list) - 1

                    # 定位父节点 (origin_post)
                    if target_idx > 0:
                        origin_post = reply_post_list[target_idx - 1]
                    else:
                        origin_post = reply_post_list[0]

                    origin_post = origin_post or {}
                    my_reply = my_reply or {}

                    raw_hyperlinks = my_reply.get("hyperlinkList") or []
                    clean_links = [
                        link.get("url")
                        for link in raw_hyperlinks
                        if isinstance(link, dict) and link.get("url")
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
                    # ==========================================
                    # 分支 B：非回复内容 (原创帖/文章)，直接读取本体
                    # ==========================================
                    my_reply = item or {}

                    raw_hyperlinks = my_reply.get("hyperlinkList") or []
                    clean_links = [
                        link.get("url")
                        for link in raw_hyperlinks
                        if isinstance(link, dict) and link.get("url")
                    ]

                    # 格式保持 100% 统一，缺失的父节点信息用空字符串填充
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

            # 5. 分页游标校验
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


def fetch_binance_hot_hashtags(session=None, max_retries=3):
    """
    获取币安广场热门话题 (Hashtag) 列表 (公开接口，无需登录)。

    :param session: 外部传入的 requests.Session() 对象，用于连接池复用
    :param max_retries: 最大重试次数
    :return: 包含热门话题详情的列表，即原始响应中 data 里面的 data 字段
    """
    url = "https://www.binance.com/bapi/composite/v2/public/pgc/hashtag/hot-list"

    # 精简 Headers：不需要携带 Cookie，满足公开接口即可
    headers = {
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "clienttype": "web",
        "content-type": "application/json",
        "lang": "zh-CN",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36"
    }

    req_client = session if session else requests
    retry_count = 0
    label = "获取热门话题"

    while retry_count < max_retries:
        response = None
        try:
            # 该接口通常是 GET 请求获取数据
            response = req_client.get(
                url,
                headers=headers,
                proxies=PROXIES,
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()

            json_resp = response.json()

            # 校验业务层状态
            if json_resp.get("code") == "000000" and json_resp.get("success"):
                # 直接提取内层 data 列表
                hot_list = json_resp.get("data", {}).get("data", [])
                logger.info(f"✅ [{label}] 成功 | 提取到 {len(hot_list)} 个热门话题")
                return hot_list
            else:
                logger.error(
                    f"❌ [{label}] API 业务错误 | Code: {json_resp.get('code')} | Msg: {json_resp.get('message')}")
                return []

        except Exception as e:
            detail = ""
            if response is not None:
                detail = f" | HTTP {response.status_code} | 服务器返回: {response.text[:200]}"

            retry_count += 1
            logger.warning(
                f"🚨 [{label}] 请求失败 "
                f"(第 {retry_count}/{max_retries} 次){detail} | 异常: {e}"
            )

            if retry_count >= max_retries:
                logger.error(f"❌ [{label}] 连续失败达到 {max_retries} 次上限，终止采集。")
                break

            # 失败后随机休眠 1~3 秒再重试，防风控
            time.sleep(random.uniform(1.0, 3.0))

    return []

def fetch_binance_spot_hot_coins(currency="USD", session=None, max_retries=3):
    """
    获取币安现货热门币种 (Hot Coins) 列表 (公开接口，无需登录)。

    :param currency: 计价货币，默认为 "USD"
    :param session: 外部传入的 requests.Session() 对象，用于连接池复用
    :param max_retries: 最大重试次数
    :return: 包含热门币种的列表，即原始响应中 data 字段 (List格式)
    """
    url = "https://www.binance.com/bapi/apex/v1/public/apex/market/spot/hot-coins"

    # 将 URL 中的 query 参数优雅地提取到 params 字典中
    params = {
        "currency": currency
    }

    # 精简 Headers：过滤掉 curl 中杂乱的个人特征 (cookie, uuid等)，只保留过 WAF 必要的标识
    headers = {
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "clienttype": "web",
        "content-type": "application/json",
        "lang": "zh-CN",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36"
    }

    req_client = session if session else requests
    retry_count = 0
    label = f"现货热门币种:{currency}"

    while retry_count < max_retries:
        response = None
        try:
            # 这是一个标准的 GET 请求
            response = req_client.get(
                url,
                headers=headers,
                params=params,
                proxies=PROXIES,
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()

            json_resp = response.json()

            # 校验业务层状态 (code 为 000000 且 success 为 true)
            if json_resp.get("code") == "000000" and json_resp.get("success"):
                # 按照需求：仅提取并返回内层 data 列表
                data_list = json_resp.get("data", [])
                logger.info(f"✅ [{label}] 成功 | 提取到 {len(data_list)} 个热门币种")
                return data_list
            else:
                logger.error(
                    f"❌ [{label}] API 业务错误 | Code: {json_resp.get('code')} | Msg: {json_resp.get('message')}"
                )
                return []

        except Exception as e:
            detail = ""
            if response is not None:
                detail = f" | HTTP {response.status_code} | 服务器返回: {response.text[:200]}"

            retry_count += 1
            logger.warning(
                f"🚨 [{label}] 请求失败 "
                f"(第 {retry_count}/{max_retries} 次){detail} | 异常: {e}"
            )

            if retry_count >= max_retries:
                logger.error(f"❌ [{label}] 连续失败达到 {max_retries} 次上限，终止采集。")
                break

            # 失败后随机休眠 1~3 秒再重试，防风控
            import time, random
            time.sleep(random.uniform(1.0, 3.0))

    return []


def get_latest_crypto_price(coin_name: str, proxy: str = "http://127.0.0.1:7890") -> float | None:
    """
    使用 ccxt 获取指定币种最新价格（优先查询U本位合约，查不到则降级查询现货）

    :param coin_name: 币种名称/交易对，例如: "BTC", "$BTC", "BTC/USDT", "BTCUSDT", "PEPE"
    :param proxy: HTTP代理地址，默认 "http://127.0.0.1:7890"，若传 None 或空串则直连
    :return: 浮点型最新价格，若均查询不到则返回 None
    """
    if not coin_name or not isinstance(coin_name, str):
        logger.warning("⚠️ [价格查询] 传入的币种名称无效")
        return None

    # 1. 清洗币种名称
    clean_name = coin_name.replace('$', '').strip().upper()

    # 2. 统一提取基础币 (base) 与计价币 (quote)
    quote_candidates = ["USDT", "USDC", "FDUSD", "BUSD"]
    base_coin = clean_name
    specified_quote = None

    if '/' in clean_name:
        parts = clean_name.split('/')
        base_coin, specified_quote = parts[0], parts[1]
    elif ':' in clean_name:  # 兼容 ccxt 合约写法如 BTC/USDT:USDT
        base_coin = clean_name.split(':')[0].split('/')[0]
    else:
        for q in quote_candidates:
            if clean_name.endswith(q) and len(clean_name) > len(q):
                base_coin = clean_name[:-len(q)]
                specified_quote = q
                break

    # 3. 构造优先级尝试的计价币列表
    quotes_to_try = [specified_quote] if specified_quote else ["USDT", "USDC"]

    # 4. 配置 ccxt 基础参数（含代理）
    if proxy == "http://127.0.0.1:7890":
        proxy = get_proxy_url(default=proxy)
    exchange_config = {
        'timeout': 10000,
        'enableRateLimit': True,
    }
    if proxy:
        exchange_config['proxies'] = {
            'http': proxy,
            'https': proxy
        }

    # ============================================================
    # 阶段一：优先探测 U本位永续合约 (Swap / Future)
    # ============================================================
    try:
        future_exchange = ccxt.binance({
            **exchange_config,
            'options': {'defaultType': 'future'}
        })

        for q in quotes_to_try:
            # ccxt 合约标准格式通常为 BTC/USDT:USDT
            future_symbols = [f"{base_coin}/{q}:{q}", f"{base_coin}/{q}"]
            for sym in future_symbols:
                try:
                    ticker = future_exchange.fetch_ticker(sym)
                    price = ticker.get('last') or ticker.get('close')
                    if price:
                        price = float(price)
                        logger.info(f"✅ [价格查询-合约命中] {coin_name} -> 交易对: {sym} | 最新价: {price}")
                        return price
                except (ccxt.BadSymbol, ccxt.OrderBookEmpty):
                    continue
                except Exception as e:
                    logger.debug(f"🔍 [价格查询] 合约探测 {sym} 失败: {e}")
    except Exception as e:
        logger.debug(f"🔍 [价格查询] 合约通道初始化或查询失败: {e}")

    # ============================================================
    # 阶段二：降级探测 现货 (Spot)
    # ============================================================
    try:
        spot_exchange = ccxt.binance({
            **exchange_config,
            'options': {'defaultType': 'spot'}
        })

        for q in quotes_to_try:
            sym = f"{base_coin}/{q}"
            try:
                ticker = spot_exchange.fetch_ticker(sym)
                price = ticker.get('last') or ticker.get('close')
                if price:
                    price = float(price)
                    logger.info(f"✅ [价格查询-现货命中] {coin_name} -> 交易对: {sym} | 最新价: {price}")
                    return price
            except (ccxt.BadSymbol, ccxt.OrderBookEmpty):
                continue
            except Exception as e:
                logger.debug(f"🔍 [价格查询] 现货探测 {sym} 失败: {e}")
    except Exception as e:
        logger.debug(f"🔍 [价格查询] 现货通道初始化或查询失败: {e}")

    logger.warning(f"❌ [价格查询失败] {coin_name} 合约与现货均未找到报价")
    return None

def fetch_binance_future_hot_coins(business_enum="USDT_FUTURES", session=None, max_retries=3):
    """
    获取币安合约热搜榜数据 (公开接口，无需登录)。

    :param business_enum: 业务类型，默认 "USDT_FUTURES" (U本位合约)
    :param session: 外部传入的 requests.Session() 对象，用于连接池复用
    :param max_retries: 最大重试次数
    :return: 包含热搜币种的列表，即原始响应中 data 字段 (List格式)
    """
    url = "https://www.binance.com/bapi/composite/v1/public/future/external/topSearchList"

    # 将 url 中的参数提取到 params 字典中，代码更优雅
    params = {
        "businessEnum": business_enum
    }

    # 精简 Headers：过滤掉 curl 中杂乱的个人特征 (cookie, token等)，只保留过 WAF 必要的伪装
    headers = {
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "clienttype": "web",
        "content-type": "application/json",
        "lang": "zh-CN",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36"
    }

    req_client = session if session else requests
    retry_count = 0
    label = f"合约热搜榜:{business_enum}"

    while retry_count < max_retries:
        response = None
        try:
            # 这是一个标准的 GET 请求
            response = req_client.get(
                url,
                headers=headers,
                params=params,
                proxies=PROXIES,
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()

            json_resp = response.json()

            # 校验业务层状态 (code 为 000000 且 success 为 true)
            if json_resp.get("code") == "000000" and json_resp.get("success"):
                # 直接提取内层 data 列表并返回
                data_list = json_resp.get("data", [])
                logger.info(f"✅ [{label}] 成功 | 提取到 {len(data_list)} 个热搜币种")
                return data_list
            else:
                logger.error(
                    f"❌ [{label}] API 业务错误 | Code: {json_resp.get('code')} | Msg: {json_resp.get('message')}")
                return []

        except Exception as e:
            detail = ""
            if response is not None:
                detail = f" | HTTP {response.status_code} | 服务器返回: {response.text[:200]}"

            retry_count += 1
            logger.warning(
                f"🚨 [{label}] 请求失败 "
                f"(第 {retry_count}/{max_retries} 次){detail} | 异常: {e}"
            )

            if retry_count >= max_retries:
                logger.error(f"❌ [{label}] 连续失败达到 {max_retries} 次上限，终止采集。")
                break

            # 失败后随机休眠 1~3 秒再重试，防风控
            import time, random
            time.sleep(random.uniform(1.0, 3.0))

    return []

if __name__ == "__main__":
    logger.info("========== 🚀 币安广场接口快速联通性测试 ==========")
    hot_future_coins = fetch_binance_future_hot_coins()
    hot_spot_coins = fetch_binance_spot_hot_coins()
    recommend_data = fetch_binance_feed(count=10)
    logger.info(f"========== 🏁 测试完成 | 推荐流获取: {len(recommend_data)} 条 ==========")



