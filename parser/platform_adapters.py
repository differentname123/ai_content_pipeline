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
_BINANCE_RIVAL_OKB_PATTERN = re.compile(r"(?<![a-z0-9])okb(?![a-z0-9])")
_BINANCE_RIVAL_ENGLISH_BRANDS = re.compile(r"(?<![a-z0-9])(okx|bybit|bitget|htx|mexc|gate\.io)(?![a-z0-9])")
BINANCE_CORE_ANALYSIS_COINS = ("BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "FIL", "DOT")
_EMPTY_EXCHANGE_UI_VALUES = {"none", "null", "unknown", "n/a", "na", "无", "无交易所", "未知"}
_EXCHANGE_UI_ALIASES = {
    "binance": "binance",
    "币安": "binance",
    "bn": "binance",
    "okx": "okx",
    "okex": "okx",
    "欧易": "okx",
    "欧意": "okx",
}
_VALID_EXCHANGE_UI_RE = re.compile(r"^[a-z0-9_-]+$")

METRICS_FIRST_SYNC_MIN_AGE_SECONDS = 600
METRICS_MATURE_AGE_SECONDS = 24 * 3600
METRICS_REFRESH_INTERVAL_SECONDS = 2 * 3600
METRICS_MAX_REFRESH_AGE_SECONDS = 48 * 3600

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


def normalize_exchange_ui(value: Any) -> str:
    """将图片识别到的交易平台界面标识归一化为标准小写平台名（无明显交易平台界面返回空串）。"""
    if value is None or isinstance(value, bool):
        return ""
    raw = str(value).strip().lower()
    if not raw or raw in _EMPTY_EXCHANGE_UI_VALUES:
        return ""
    if raw in _EXCHANGE_UI_ALIASES:
        return _EXCHANGE_UI_ALIASES[raw]
    compact = re.sub(r"\s+", "", raw)
    if compact in _EXCHANGE_UI_ALIASES:
        return _EXCHANGE_UI_ALIASES[compact]
    if _VALID_EXCHANGE_UI_RE.fullmatch(compact):
        return compact
    return "other"


def has_rival_exchange_ui(platform: str, exchange_ui_list: Sequence[Any]) -> bool:
    """仅对交易所内容平台（Binance / OKX）拦截明确露出竞品交易所界面的配图；X 等非交易所平台放行。"""
    target = str(platform or "").strip().lower()
    if target not in (PLATFORM_BINANCE, PLATFORM_OKX) or not exchange_ui_list:
        return False
    items = (exchange_ui_list,) if isinstance(exchange_ui_list, str) else exchange_ui_list
    for item in items:
        ui = normalize_exchange_ui(item)
        if ui and ui != target:
            return True
    return False


def has_rival_keyword(platform: str, text: str) -> bool:
    """检查文本是否包含目标平台禁止提及的竞品关键词。"""
    lower = str(text or "").lower()
    if platform == PLATFORM_BINANCE:
        return (
            "欧易" in lower
            or "欧意" in lower
            or "火币" in lower
            or bool(_BINANCE_RIVAL_ENGLISH_BRANDS.search(lower))
            or bool(_BINANCE_RIVAL_OKB_PATTERN.search(lower))
        )
    if platform == PLATFORM_OKX:
        return "binance" in lower or "币安" in lower
    return False


def add_hot_topic_to_article(text: str, topic: str, tag_count: int = 2) -> str:
    """仅追加与主题精准匹配的单一币种标签；无准确币种（如空值或 MARKET）则不强加标签。"""
    clean_topic = str(topic or "").strip().upper()
    if not clean_topic or clean_topic == "MARKET":
        return text
    # 1 字符超短代币（如 $H）停顿 2 秒键入极易被联想前缀错选，仅保留 #COIN 话题标签而不强加 $COIN
    tags = [f"#{clean_topic}", f"${clean_topic} "] if len(clean_topic) >= 2 else [f"#{clean_topic}"]
    logger.info(f"[文章/标签] 精准标签已准备 | 主题: [{clean_topic}] | 标签: [{tags}]")
    return text + "\n\n" + "\n".join(tags)


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
            return list(dict.fromkeys([*coins, *BINANCE_CORE_ANALYSIS_COINS]))
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
        return add_hot_topic_to_article(text, topic)


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
        return add_hot_topic_to_article(text, topic)


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
    has_coin_topic = bool(topic and topic.upper() != "MARKET" and len(topic) >= 2)
    if has_coin_topic:
        # 使用 ASCII 字符边界匹配（兼容如 "ZEC破800" 紧贴中文），避免与前缀叠加出 "$ZEC ZEC" 重复
        text = re.sub(
            rf"(?<![A-Za-z0-9$]){re.escape(topic)}(?![A-Za-z0-9]) ?",
            lambda _: f"${topic} ",
            text,
            flags=re.IGNORECASE,
        )
    adapter = PLATFORMS[PLATFORM_BINANCE if platform == PLATFORM_BINANCE else PLATFORM_OKX]
    text = adapter.decorate_analysis(text, topic)
    if platform == PLATFORM_BINANCE:
        text += "\n\n👇"
    if has_coin_topic and not text.upper().startswith(f"${topic}".upper()):
        text = f"${topic} {text}"
    mapping = info.get("image_mapping") or {}
    paths = [
        mapping[key].get("local_path")
        for key in (info.get("image_placeholders") or [])
        if isinstance(mapping.get(key), dict) and mapping[key].get("local_path")
    ]
    return text, paths


def _format_viral_post_content(info: Dict[str, Any], matched_coin: str) -> Tuple[str, List[str]]:
    """流量文章 `article_info` + 热币标签 -> `(最终发布正文, 有序本地图片路径列表)`。"""
    text = str(info.get("post_text") or "").strip()
    clean_coin = str(matched_coin or "").strip().upper()
    # 实证防误补全：1 字符超短代币（如 $H）在广场编辑器停顿 2 秒时极易被前缀联想自动补全错选（如变成 $HYPE），不主动追加
    prefix = f"${clean_coin}" if len(clean_coin) >= 2 else ""
    if prefix:
        has_prefix = bool(
            re.search(rf"(?<![A-Za-z0-9]){re.escape(prefix)}(?![A-Za-z0-9])", text, re.IGNORECASE)
        )
        if not text:
            text = prefix
        elif not has_prefix and not COIN_TAG_PATTERN.search(text):
            # 正文尚未包含任何币种标签时追加至文末，避免强插在句首破坏前 3 秒钩子
            text = f"{text}\n\n{prefix}"
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
    为已发布历史文章免登只读同步真实阅读/点赞/评论数（48h 内周期刷新、成熟前不冻僵），
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
        raw_pub_time = rec.get("publish_time", 0) or 0
        pub_time = (raw_pub_time / 1000.0) if raw_pub_time > 1e11 else float(raw_pub_time)
        age_sec = max(0.0, cur_ts - pub_time) if pub_time > 0 else 0.0
        last_synced = float(rec.get("metrics_synced_at") or 0.0)

        need_sync = bool(
            pid
            and str(pid) != "None"
            and pub_time > 0
            and (
                (views is None and age_sec >= METRICS_FIRST_SYNC_MIN_AGE_SECONDS)
                or (
                    METRICS_FIRST_SYNC_MIN_AGE_SECONDS <= age_sec <= METRICS_MAX_REFRESH_AGE_SECONDS
                    and (cur_ts - last_synced) >= METRICS_REFRESH_INTERVAL_SECONDS
                )
                or (
                    age_sec > METRICS_MAX_REFRESH_AGE_SECONDS
                    and (
                        (last_synced > 0 and (last_synced - pub_time) < METRICS_MATURE_AGE_SECONDS)
                        or (views is not None and last_synced == 0.0)
                    )
                )
            )
        )

        if need_sync:
            try:
                if platform == PLATFORM_BINANCE:
                    detail = fetch_binance_post_detail(str(pid))
                    if isinstance(detail, dict):
                        content_status = detail.get("contentStatus")
                        if content_status == 2:
                            views = rec["view_count"] = detail.get("viewCount", 0)
                            likes = rec["like_count"] = detail.get("likeCount", 0)
                            comments = rec["comment_count"] = detail.get("commentCount", 0)
                        if content_status is not None:
                            rec["content_status"] = content_status
                            rec["recommend"] = detail.get("recommend")
                            rec["lan"] = detail.get("lan")
                            rec["detected_lang"] = detail.get("detectedLang")
                            rec["metrics_synced_at"] = cur_ts
                            need_upsert.append(d)
                elif platform == PLATFORM_OKX:
                    detail = fetch_okx_post_detail(str(pid))
                    if detail and detail.get("reviewState") == 2:
                        views = rec["view_count"] = detail.get("viewCount", 0)
                        likes = rec["like_count"] = detail.get("likeCount", 0)
                        comments = rec["comment_count"] = detail.get("commentNum", 0)
                        rec["metrics_synced_at"] = cur_ts
                        need_upsert.append(d)
            except Exception as exc:
                logger.debug(f"[发布/指标同步] 拉取 {platform} 帖子 {pid} 详情失败(已降级跳过): {exc}")

        time_str = datetime.fromtimestamp(pub_time, tz=BEIJING_TZ).strftime("%Y-%m-%d %H:%M") if pub_time > 0 else "未知"
        age_hours = round(age_sec / 3600.0, 1) if pub_time > 0 else None
        is_mature = bool(pub_time > 0 and age_sec >= 6 * 3600)
        if pub_time > 0 and age_sec < 2 * 3600:
            display_views: Any = "爬坡中(发布<2h)"
        elif views is not None:
            display_views = views
        else:
            display_views = "统计中"

        history_items.append(
            {
                "account": rec.get("published_by"),
                "topic": d.get("topic"),
                "stance": d.get("stance"),
                "published_at": time_str,
                "age_hours": age_hours,
                "is_mature": is_mature,
                "views": display_views,
                "likes": likes if likes is not None else 0,
                "comments": comments if comments is not None else 0,
                "content": text[:150],
            }
        )

    if need_upsert:
        try:
            if (
                hasattr(article_manager, "db")
                and hasattr(article_manager.db, "get_collection")
                and hasattr(article_manager, "collection_name")
                and all(isinstance(doc, dict) and doc.get("article_id") for doc in need_upsert)
            ):
                col = article_manager.db.get_collection(article_manager.collection_name)
                for doc in need_upsert:
                    col.update_one(
                        {"article_id": doc["article_id"]},
                        {"$set": {f"publish_records.{platform}": doc["publish_records"][platform]}},
                    )
            else:
                article_manager.upsert_articles(need_upsert)
        except Exception as exc:
            logger.warning(f"[发布/指标同步] 批量回写阅读指标至数据库失败: {exc}")

    return history_items
