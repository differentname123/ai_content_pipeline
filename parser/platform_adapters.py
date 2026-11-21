# -*- coding: utf-8 -*-
"""
内容流平台适配器与格式/指标中心 (`parser/platform_adapters.py`)

统一收口 Binance 广场与 OKX Orbit 的差异化能力：
1. 行情热币聚合（带 20s 缓存防多线程启动雪崩）、热门话题标签拼装与 `$COIN` 分段键入序列；
2. 平台专属发帖、评论、Feed 拉取与竞品词检查 (`has_rival_keyword`)；
3. 分析帖/流量帖最终发布文案与配图路径排版；
4. 已发布历史文章的免登只读互动指标增量同步 (`sync_published_metrics`)。
"""
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from common.common_utils import (
    PLATFORM_BINANCE,
    PLATFORM_OKX,
    resolve_session_dir,
    setup_logger,
)
from platforms.binance_api import (
    fetch_binance_feed,
    fetch_binance_future_hot_coins,
    fetch_binance_hot_hashtags,
    fetch_binance_post_detail,
    fetch_binance_spot_hot_coins,
    get_latest_crypto_price,
    get_top_k_movers,
)
from platforms.binance_playwright import comment_on_binance_post, create_binance_post
from platforms.okx_api import (
    fetch_okx_feed,
    fetch_okx_hot_coins,
    fetch_okx_post_detail,
)
from platforms.okx_playwright import comment_on_okx_post, create_okx_post

logger = setup_logger(app_name="media_format")

BEIJING_TZ = timezone(timedelta(hours=8))
COIN_TAG_PATTERN = re.compile(r"\$[A-Za-z0-9_]*[A-Za-z][A-Za-z0-9_]*")

_HOT_COIN_CACHE: Dict[str, Any] = {"ts": 0.0, "coins": []}
_HOT_COIN_LOCK = threading.Lock()


def _log(action: str, message: str, level: str = "info", exc_info: bool = False, **fields: Any) -> None:
    """统一输出 `[Action] Message | K: [V]` 结构化流水线日志。"""
    details = " | ".join(f"{key}: [{value}]" for key, value in fields.items())
    prefix = "❌ " if level in ("error", "exception") else ""
    suffix = f" | {details}" if details else ""
    getattr(logger, level)(
        f"{prefix}[{action}] {message}{suffix}",
        exc_info=exc_info or level == "exception",
        stacklevel=2,
    )


def get_hot_coin() -> List[str]:
    """合约/现货热榜与涨跌榜各前 2 项 -> 去重币种列表（20 秒内复用缓存避免多线程雪崩请求）。"""
    with _HOT_COIN_LOCK:
        if _HOT_COIN_CACHE["coins"] and (time.monotonic() - _HOT_COIN_CACHE["ts"]) < 20.0:
            return list(_HOT_COIN_CACHE["coins"])

    top_k, coins = 2, []
    for fetch in (fetch_binance_future_hot_coins, fetch_binance_spot_hot_coins):
        coins.extend(
            str(item.get("symbol") or "").replace("USDT", "").replace("USDC", "")
            for item in (fetch() or [])[:top_k]
            if isinstance(item, dict) and item.get("symbol")
        )
    movers = get_top_k_movers(top_k) or {}
    for item in (movers.get("top_gainers") or []) + (movers.get("top_losers") or []):
        sym = str((item or {}).get("symbol") or "")
        if sym:
            coins.append(sym.split("/")[0])
    coins = [c for c in dict.fromkeys(coins) if c]

    with _HOT_COIN_LOCK:
        _HOT_COIN_CACHE["ts"] = time.monotonic()
        _HOT_COIN_CACHE["coins"] = list(coins)

    logger.info(f"[行情热榜/币安] 热门与异动币种合并完成 | 币种数: [{len(coins)}] | 列表: [{', '.join(coins)}]")
    return coins


def _normalize_hot_coins(hot_coins: Optional[Sequence[Any]]) -> List[str]:
    """[str 或 {symbol/coin}] -> 有序币种字符串列表。"""
    coins: List[str] = []
    for item in hot_coins or []:
        value = (item.get("symbol") or item.get("coin") or "") if isinstance(item, dict) else (item or "")
        coin = str(value).replace("USDT", "").replace("USDC", "").strip()
        if coin:
            coins.append(coin)
    return coins


def has_rival_keyword(platform: str, text: str) -> bool:
    """检查文本是否包含目标平台禁止提及的竞品关键词。"""
    lower = str(text or "").lower()
    if platform == PLATFORM_BINANCE:
        return "okx" in lower or "欧易" in lower
    if platform == PLATFORM_OKX:
        return "binance" in lower or "币安" in lower
    return False


def add_hot_topic_to_article(text: str, topic: str, tag_count: int = 2) -> str:
    """为分析正文尾部追加 Binance 热门话题、主币种标签与合约热榜首币标签。"""
    hashtags = [item["hashtag"] for item in (fetch_binance_hot_hashtags() or []) if isinstance(item, dict) and item.get("hashtag")]
    tags = hashtags[:1] + [f"#{topic}", f"${topic} "]
    hot_coins = fetch_binance_future_hot_coins() or []
    if hot_coins and isinstance(hot_coins[0], dict) and hot_coins[0].get("symbol"):
        symbol = hot_coins[0]["symbol"].replace("USDT", "").replace("USDC", "")
        tags.extend([f"#{symbol}", f"${symbol} "])
    logger.info(f"[文章/标签] 标签已准备 | 主题: [{topic}] | 标签: [{tags}]")
    return text + "\n\n\n" + "\n".join(tags)


def _build_publish_text_list(text: str) -> List[Dict[str, Any]]:
    """正文 -> `[{text, delay}]`；`$COIN` 片段 `delay=2` 且其后普通片段强制以单个换行开头。"""
    if not text:
        return []
    result, last_end, after_coin = [], 0, False
    for match in [*COIN_TAG_PATTERN.finditer(text), None]:
        segment = text[last_end : match.start() if match else len(text)]
        if after_coin:
            segment = "\n" + segment.lstrip(" \t\r\n")
        if segment:
            result.append({"text": segment, "delay": 0})
        if match is None:
            break
        result.append({"text": match.group(0), "delay": 2})
        last_end, after_coin = match.end(), True
    return result


class BinancePlatform:
    """Binance 广场适配器。"""

    def session_dir(self, account: str) -> str:
        return resolve_session_dir(f"{account}_browser_session_dir", f"biance_{account}")

    def hot_coins(self, for_publish: bool = False) -> List[str]:
        coins = list(get_hot_coin() or [])
        if for_publish:
            return coins
        return list(dict.fromkeys([*coins, "BTC", "ETH", "SOL"]))

    def fetch_feed(self, **kwargs: Any) -> List[Dict[str, Any]]:
        return fetch_binance_feed(**kwargs)

    def publish(
        self,
        session_dir: str,
        text: str,
        image_paths: Sequence[str],
        topic: Optional[str] = None,
        stance: Optional[str] = None,
    ) -> Tuple[Optional[str], bool, Optional[str]]:
        return create_binance_post(
            content=text,
            image_path_list=list(image_paths),
            user_data_dir=session_dir,
            chart_info={"coin": topic, "bridge": "USDT", "type": "future"} if topic else None,
            sentiment=stance,
            debug=False,
            text_list=_build_publish_text_list(text),
        )

    def comment(
        self,
        session_dir: str,
        post_id: Any,
        text: str,
        stance: Optional[str],
        like: bool = False,
    ) -> Tuple[Optional[str], bool, Optional[str]]:
        return comment_on_binance_post(
            post_url=f"https://www.binance.com/zh-CN/square/post/{post_id}",
            comment=text,
            user_data_dir=session_dir,
            sentiment=stance,
            debug=False,
        )

    def decorate_analysis(self, text: str, topic: str) -> str:
        try:
            return add_hot_topic_to_article(text, topic)
        except Exception as exc:
            _log(
                "发布/标签",
                "拼装分析帖时获取热门标签失败，使用主题标签继续发布",
                level="warning",
                主题=topic,
                原因=exc,
                排查="检查 Binance 热榜接口与网络",
            )
            return f"{text}\n\n#{topic}\n${topic} "


class OkxPlatform:
    """OKX 星球 (Orbit) 适配器。"""

    def session_dir(self, account: str) -> str:
        return resolve_session_dir(f"okx_{account}_browser_session_dir", f"okx_{account}")

    def hot_coins(self, for_publish: bool = False) -> List[Any]:
        return fetch_okx_hot_coins(num=25) if for_publish else fetch_okx_hot_coins()

    def fetch_feed(self, **kwargs: Any) -> List[Dict[str, Any]]:
        return fetch_okx_feed(**kwargs)

    def publish(
        self,
        session_dir: str,
        text: str,
        image_paths: Sequence[str],
        topic: Optional[str] = None,
        stance: Optional[str] = None,
    ) -> Tuple[Optional[str], bool, Optional[str]]:
        return create_okx_post(
            content=text,
            image_path_list=list(image_paths),
            user_data_dir=session_dir,
            debug=False,
        )

    def comment(
        self,
        session_dir: str,
        post_id: Any,
        text: str,
        stance: Optional[str],
        like: bool = False,
    ) -> Tuple[Optional[str], bool, Optional[str]]:
        url = str(post_id) if str(post_id).startswith("http") else f"https://www.okx.com/zh-hans/orbit/post/{post_id}"
        return comment_on_okx_post(
            target_post_url=url,
            content=text,
            user_data_dir=session_dir,
            like=like,
            debug=False,
        )

    def decorate_analysis(self, text: str, topic: str) -> str:
        return f"{text}\n\n#{topic}\n${topic} "


PLATFORMS: Dict[str, Any] = {
    PLATFORM_BINANCE: BinancePlatform(),
    PLATFORM_OKX: OkxPlatform(),
}


def _get_account_session_dir(platform: str, account: str) -> str:
    return PLATFORMS.get(platform, PLATFORMS[PLATFORM_BINANCE]).session_dir(account)


def _execute_platform_post(
    platform: str,
    session_dir: str,
    text: str,
    image_paths: Sequence[str],
    topic: Optional[str] = None,
    stance: Optional[str] = None,
) -> Tuple[Optional[str], bool, Optional[str]]:
    if platform not in PLATFORMS:
        raise ValueError(f"不支持的发布平台: {platform}")
    return PLATFORMS[platform].publish(session_dir, text, image_paths, topic, stance)


def publish_to_platform(
    platform: str,
    account: str,
    *,
    text: str,
    image_paths: Sequence[str],
    topic: Optional[str] = None,
    stance: Optional[str] = None,
    session_dir: Optional[str] = None,
) -> Tuple[Optional[str], bool, Optional[str]]:
    """统一平台发帖入口，自动解析账号会话目录并委托对应适配器。"""
    resolved_dir = session_dir or _get_account_session_dir(platform, account)
    return _execute_platform_post(
        platform,
        resolved_dir,
        text,
        image_paths,
        topic=topic,
        stance=stance,
    )


def _format_analysis_post_content(article: Dict[str, Any], platform: str = PLATFORM_BINANCE) -> Tuple[str, List[str]]:
    """分析文章 -> `(最终发布正文, 有序本地图片路径列表)`。"""
    info = article["article_info"]
    topic, text = (article.get("topic") or "").strip(), info.get("text", "").strip()
    if topic:
        text = re.sub(rf"(?<!\$)\b{re.escape(topic)}\b", lambda _: f"${topic} ", text, flags=re.IGNORECASE)
    adapter = PLATFORMS[PLATFORM_BINANCE if platform == PLATFORM_BINANCE else PLATFORM_OKX]
    text = adapter.decorate_analysis(text, topic)
    if platform == PLATFORM_BINANCE:
        text += "\n\n\n\n\n\n👇"
    if topic and not text.upper().startswith(f"${topic}".upper()):
        text = f"${topic} {text}"
    mapping = info.get("image_mapping") or {}
    paths = [
        mapping[key].get("local_path")
        for key in (info.get("image_placeholders") or [])
        if isinstance(mapping.get(key), dict) and mapping[key].get("local_path")
    ]
    return text, paths


def _format_viral_post_content(info: Dict[str, Any], matched_coin: str) -> Tuple[str, List[str]]:
    """流量文章 `article_info` + 热币前缀 -> `(最终发布正文, 有序本地图片路径列表)`。"""
    text = str(info.get("post_text") or "").strip()
    prefix = f"${matched_coin}"
    if not text.upper().startswith(prefix.upper()):
        text = f"{prefix} {text}" if text else prefix
    mapping = info.get("image_mapping") or {}
    paths = [
        mapping[key].get("local_path")
        for key in (info.get("selected_images") or [])
        if isinstance(mapping.get(key), dict) and mapping[key].get("local_path")
    ]
    return text, paths


def _safe_get_latest_price(platform: str, topic: Optional[str]) -> Optional[float]:
    """安全获取发布时刻币种价格，异常时降级返回 None，不阻断发布主流程。"""
    if not topic:
        return None
    try:
        return get_latest_crypto_price(topic)
    except Exception as exc:
        logger.warning(
            f"[发布/行情] 获取发布时刻价格失败 | 平台: [{platform}] | 主题: [{topic}] "
            f"| 异常: [{exc}] | 后续: [价格记为 None 继续落库]"
        )
        return None


def sync_published_metrics(
    article_manager: Any,
    platform: str,
    docs: Sequence[Dict[str, Any]],
    *,
    now_ts: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """
    为发布超过 10 分钟且尚未记录浏览量的历史文章免登只读同步真实阅读/点赞/评论数，
    回写至数据库后返回供总编擂台参考的历史战报列表。
    """
    cur_ts = time.time() if now_ts is None else float(now_ts)
    history_items: List[Dict[str, Any]] = []
    need_upsert: List[Dict[str, Any]] = []

    for d in docs:
        rec = (d.get("publish_records") or {}).get(platform) or {}
        info = d.get("article_info") or {}
        text = (info.get("post_text") or info.get("text") or "").strip().replace("\n", " ")
        if not text:
            continue

        pid = rec.get("post_id")
        views = rec.get("view_count")
        likes = rec.get("like_count")
        comments = rec.get("comment_count")
        pub_time = rec.get("publish_time", 0) or 0

        if pid and views is None and (cur_ts - pub_time > 600):
            try:
                if platform == PLATFORM_BINANCE:
                    detail = fetch_binance_post_detail(str(pid))
                    if detail and detail.get("contentStatus") == 2:
                        views = rec["view_count"] = detail.get("viewCount", 0)
                        likes = rec["like_count"] = detail.get("likeCount", 0)
                        comments = rec["comment_count"] = detail.get("commentCount", 0)
                        need_upsert.append(d)
                elif platform == PLATFORM_OKX:
                    detail = fetch_okx_post_detail(str(pid))
                    if detail and detail.get("reviewState") == 2:
                        views = rec["view_count"] = detail.get("viewCount", 0)
                        likes = rec["like_count"] = detail.get("likeCount", 0)
                        comments = rec["comment_count"] = detail.get("commentNum", 0)
                        need_upsert.append(d)
            except Exception as exc:
                logger.debug(f"[发布/指标同步] 拉取 {platform} 帖子 {pid} 详情失败(已降级跳过): {exc}")

        time_str = datetime.fromtimestamp(pub_time, tz=BEIJING_TZ).strftime("%Y-%m-%d %H:%M") if pub_time else "未知"
        history_items.append(
            {
                "account": rec.get("published_by"),
                "topic": d.get("topic"),
                "stance": d.get("stance"),
                "published_at": time_str,
                "views": views if views is not None else "统计中",
                "likes": likes if likes is not None else 0,
                "comments": comments if comments is not None else 0,
                "content": text[:150],
            }
        )

    if need_upsert:
        try:
            article_manager.upsert_articles(need_upsert)
        except Exception as exc:
            logger.warning(f"[发布/指标同步] 批量回写阅读指标至数据库失败: {exc}")

    return history_items
