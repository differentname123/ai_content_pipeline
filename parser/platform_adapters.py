# -*- coding: utf-8 -*-
"""
内容流平台适配器与格式/指标中心 (`parser/platform_adapters.py`)

统一收口 Binance 广场与 OKX Orbit 的差异化能力：
1. 行情热币聚合（带 20s 缓存防多线程启动雪崩）、热门话题标签拼装与 `$COIN` 分段键入序列；
2. 平台专属发帖、评论、Feed 拉取与竞品词检查 (`has_rival_keyword`)；
3. 分析帖/流量帖最终发布文案与配图路径排版；
4. 已发布历史文章的免登只读互动指标增量同步 (`sync_published_metrics`)。
"""
import inspect
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
from parser.config import ARTICLE_TYPE_ANALYSIS, ARTICLE_TYPE_VIRAL
from parser.telemetry import emit_telemetry_event
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
_BINANCE_RIVAL_ENGLISH_BRANDS = re.compile(r"(?<![a-z0-9])(okx|okex|bybit|bitget|htx|mexc|gate\.io)(?![a-z0-9])")
_OKX_RIVAL_ENGLISH_BRANDS = re.compile(r"(?<![a-z0-9])(binance|bybit|bitget|htx|mexc|gate\.io)(?![a-z0-9])")
_DESC_UI_RULES: Tuple[Tuple[str, Tuple[str, ...], Optional[re.Pattern]], ...] = (
    ("binance", ("币安",), re.compile(r"(?<![a-z0-9])binance(?![a-z0-9])")),
    ("okx", ("欧易", "欧意"), re.compile(r"(?<![a-z0-9])(okx|okex)(?![a-z0-9])")),
    ("bybit", (), re.compile(r"(?<![a-z0-9])bybit(?![a-z0-9])")),
    ("bitget", (), re.compile(r"(?<![a-z0-9])bitget(?![a-z0-9])")),
    ("htx", ("火币",), re.compile(r"(?<![a-z0-9])htx(?![a-z0-9])")),
    ("mexc", (), re.compile(r"(?<![a-z0-9])mexc(?![a-z0-9])")),
    ("gate.io", (), re.compile(r"(?<![a-z0-9])gate\.io(?![a-z0-9])")),
)
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
METRICS_EARLY_REFRESH_INTERVAL_SECONDS = 900      # 发布后 10m ~ 60m：每 15 分钟加密采样冷启动曲线
METRICS_MID_REFRESH_INTERVAL_SECONDS = 3600       # 发布后 1h ~ 6h：每 60 分钟跟踪二次推荐走势
METRICS_MATURE_AGE_SECONDS = 24 * 3600
METRICS_REFRESH_INTERVAL_SECONDS = 2 * 3600
METRICS_MAX_REFRESH_AGE_SECONDS = 48 * 3600
METRICS_TIMELINE_MAX_POINTS = 24
METRICS_TIMELINE_MIN_GAP_SECONDS = 480
METRICS_MISSING_CONFIRM_COUNT = 2
METRICS_SUSPECT_RETRY_INTERVAL_SECONDS = 600

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


def infer_exchange_ui_from_description(description: Any) -> str:
    """当结构化 `exchange_ui` 漏标为空时，从图片语义描述中兜底识别明确露出的交易所品牌名。"""
    if not isinstance(description, str) or not description.strip():
        return ""
    lower = description.lower()
    for ui, zh_terms, en_pattern in _DESC_UI_RULES:
        if any(term in lower for term in zh_terms) or (en_pattern is not None and en_pattern.search(lower)):
            return ui
    return ""


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
        return (
            "币安" in lower
            or "火币" in lower
            or bool(_OKX_RIVAL_ENGLISH_BRANDS.search(lower))
        )
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

    def score_candidate_form(self, article_type: str, img_cnt: int, clean_len: int) -> float:
        """
        【保留过滤/加权原因说明】币安广场形态与篇幅实证分布加权（仅作用于发布选优阶段，解耦于内容生成）：
        - 基于 30,000 条非蓝 V 原帖与自有账号实发统计：`>=2` 图从极短（<30 字）到 259 字均显著高于单图；
          单图真正低谷位于不短不长的 `50~99` 字半截区间（中位曝光 77 / 自身号 91），而 `<50` 字单图神梗不预惩罚、交由总编结合画面视觉冲击裁判；
          超过 `260` 字后曝光开始回落，予以温和衰减。
        - 分析帖保持 `>=2` 图主力实锤加分、`140~255` 字饱满区间加分及超长 `>260` 字温和衰减。
        """
        if article_type == ARTICLE_TYPE_VIRAL:
            if clean_len > 260:
                return -0.5 + (1.0 if img_cnt >= 2 else 0.0)
            if img_cnt >= 2:
                return 1.0
            return -0.5 if 50 <= clean_len < 100 else 0.0
        if article_type == ARTICLE_TYPE_ANALYSIS:
            bonus = 0.0
            if img_cnt >= 2:
                bonus += 0.8
            if 140 <= clean_len <= 255:
                bonus += 0.3
            elif clean_len > 260:
                bonus -= 0.5
            return bonus
        return 0.0

    def score_candidate(self, article_type: str, text: str, img_cnt: int) -> Tuple[float, Dict[str, Any]]:
        """
        【同语义单一真源】币安广场候选综合适配评分：
        集中承载形态甜区、尾问号降权与移动端版式呼吸感评估，使上游生成层保持平台中立。
        """
        layout = _inspect_text_layout(text)
        clean_len = layout["clean_len"]
        form_score = float(self.score_candidate_form(article_type, img_cnt, clean_len))
        notes: List[str] = []

        if clean_len > 400:
            form_score -= 0.5
            notes.append(f"偏长({clean_len}字>400，曝光衰减)")
        elif clean_len > 260:
            notes.append(f"超出260字甜区({clean_len}字)")

        if article_type == ARTICLE_TYPE_ANALYSIS and 0 < clean_len < 110:
            form_score -= 0.3
            notes.append(f"分析偏短({clean_len}字<110)")
        elif article_type == ARTICLE_TYPE_VIRAL and img_cnt <= 1 and 50 <= clean_len < 100:
            notes.append("单图半截区间(50~99字)")

        if img_cnt >= 2:
            notes.append(f"多图加权({img_cnt}图)")

        question_score = -0.4 if layout["trailing_question"] else 0.0
        if layout["trailing_question"]:
            notes.append("尾句带问号(降权)")

        readability_score = 0.0
        if clean_len >= 130 and layout["max_para_len"] >= 130:
            readability_score = -0.4
            notes.append(f"单段过密文字墙(最长段{layout['max_para_len']}字)")
        elif clean_len >= 130 and layout["para_count"] >= 2 and layout["max_para_len"] <= 115:
            readability_score = 0.2
            notes.append("分段呼吸感佳")
        elif clean_len < 75 and layout["para_count"] > 1:
            readability_score = -0.3
            notes.append("短梗强行拆段")

        total = round(form_score + question_score + readability_score, 2)
        return total, {
            "form": round(form_score, 2),
            "question": round(question_score, 2),
            "readability": round(readability_score, 2),
            **layout,
            "img_cnt": img_cnt,
            "notes": notes,
        }


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

    def score_candidate_form(self, article_type: str, img_cnt: int, clean_len: int) -> float:
        """OKX 分发形态偏置保持中性（0.0），避免币安广场经验权重跨平台误污染。"""
        return 0.0

    def score_candidate(self, article_type: str, text: str, img_cnt: int) -> Tuple[float, Dict[str, Any]]:
        """OKX 分发保持对币安形态与问号规律完全中立，仅在极端过长单段时做轻微可读性提示。"""
        layout = _inspect_text_layout(text)
        readability_score = -0.3 if (layout["clean_len"] >= 220 and layout["max_para_len"] >= 220) else 0.0
        notes = [f"极长单段({layout['max_para_len']}字)"] if readability_score < 0 else []
        return round(readability_score, 2), {
            "form": 0.0,
            "question": 0.0,
            "readability": round(readability_score, 2),
            **layout,
            "img_cnt": img_cnt,
            "notes": notes,
        }


PLATFORMS: Dict[str, Any] = {
    PLATFORM_BINANCE: BinancePlatform(),
    PLATFORM_OKX: OkxPlatform(),
}


def _inspect_text_layout(text: str) -> Dict[str, Any]:
    """提取正文长度、分段节奏与尾句问号等客观版式特征。"""
    raw = str(text or "")
    paras = [p.strip() for p in re.split(r"\n\s*\n", raw.strip()) if p.strip()]
    return {
        "clean_len": len("".join(raw.split())),
        "para_count": len(paras),
        "max_para_len": max((len("".join(p.split())) for p in paras), default=0),
        "trailing_question": raw.rstrip().endswith(("?", "？")),
    }


def score_platform_candidate_form(platform: str, article_type: str, img_cnt: int, clean_len: int) -> float:
    """按目标平台返回图文形态加减权分值；未知平台安全降级为 0.0。"""
    target = str(platform or "").strip().lower()
    adapter = PLATFORMS.get(target)
    if adapter is None or not hasattr(adapter, "score_candidate_form"):
        return 0.0
    return float(adapter.score_candidate_form(article_type, img_cnt, clean_len))


def score_platform_candidate(platform: str, article_type: str, text: str, img_cnt: int) -> Tuple[float, Dict[str, Any]]:
    """分发层平台适配总评分入口（形态 + 版式可读性 + 平台特征）；未知平台安全降级为 0.0。"""
    target = str(platform or "").strip().lower()
    adapter = PLATFORMS.get(target)
    if adapter is not None and hasattr(adapter, "score_candidate"):
        return adapter.score_candidate(article_type, text, img_cnt)
    layout = _inspect_text_layout(text)
    return 0.0, {"form": 0.0, "question": 0.0, "readability": 0.0, **layout, "img_cnt": img_cnt, "notes": []}


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
    body_has_topic_tag = bool(
        has_coin_topic
        and re.search(rf"(?<![A-Za-z0-9])\${re.escape(topic)}(?![A-Za-z0-9])", text, re.IGNORECASE)
    )
    adapter = PLATFORMS[PLATFORM_BINANCE if platform == PLATFORM_BINANCE else PLATFORM_OKX]
    text = adapter.decorate_analysis(text, topic)
    if platform == PLATFORM_BINANCE:
        text += "\n\n👇"
    if has_coin_topic and not body_has_topic_tag:
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


def _required_refresh_interval_seconds(age_sec: float) -> int:
    """按帖龄自适应调整回访频率：前 1 小时 15m 加密采样，1~6 小时 60m，6~48 小时 2h。"""
    if age_sec < 3600:
        return METRICS_EARLY_REFRESH_INTERVAL_SECONDS
    if age_sec < 6 * 3600:
        return METRICS_MID_REFRESH_INTERVAL_SECONDS
    return METRICS_REFRESH_INTERVAL_SECONDS


def _append_metrics_timeline_point(
    rec: Dict[str, Any],
    *,
    cur_ts: float,
    age_sec: float,
    views: Optional[Any],
    likes: Optional[Any],
    comments: Optional[Any],
    shares: Optional[Any],
    recommend: Optional[Any],
    status_code: Optional[Any],
    live_state: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
    """追加有界流量轨迹采样点（最多保留 24 点；8 分钟内状态未变不重复写）。"""
    timeline = rec.get("metrics_timeline")
    if not isinstance(timeline, list):
        timeline = []
        rec["metrics_timeline"] = timeline

    prev_numeric_v: Optional[int] = None
    for item in reversed(timeline):
        if isinstance(item, dict):
            raw_v = item.get("v")
            if isinstance(raw_v, (int, float)) and not isinstance(raw_v, bool):
                prev_numeric_v = int(raw_v)
                break

    delta_v: Optional[int] = None
    if isinstance(views, (int, float)) and not isinstance(views, bool) and prev_numeric_v is not None:
        delta_v = int(views) - prev_numeric_v

    if timeline and isinstance(timeline[-1], dict):
        last = timeline[-1]
        same_state = (
            last.get("status") == status_code
            and last.get("state") == live_state
            and last.get("rec") == recommend
        )
        if (cur_ts - float(last.get("ts") or 0.0)) < METRICS_TIMELINE_MIN_GAP_SECONDS and same_state:
            return None, delta_v

    point: Dict[str, Any] = {
        "ts": int(cur_ts),
        "at": datetime.fromtimestamp(cur_ts, tz=BEIJING_TZ).strftime("%m-%d %H:%M"),
        "age_m": round(age_sec / 60.0, 1),
        "v": views,
        "l": likes,
        "c": comments,
        "s": shares,
        "rec": recommend,
        "status": status_code,
        "state": live_state,
    }
    timeline.append(point)
    if len(timeline) > METRICS_TIMELINE_MAX_POINTS:
        del timeline[:-METRICS_TIMELINE_MAX_POINTS]
    return point, delta_v


def _call_post_detail_fetcher(fetcher: Any, pid: str) -> Any:
    """调用帖子详情抓取器；若支持 raise_on_error 参数则开启，避免把瞬态异常当作已删。"""
    target = fetcher.side_effect if callable(getattr(fetcher, "side_effect", None)) else fetcher
    try:
        params = inspect.signature(target).parameters
    except (TypeError, ValueError):
        return fetcher(pid)
    if "raise_on_error" in params or any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
        return fetcher(pid, raise_on_error=True)
    return fetcher(pid)


def _handle_missing_post_detail(
    rec: Dict[str, Any],
    pub_records: Dict[str, Any],
    doc: Dict[str, Any],
    need_upsert: List[Dict[str, Any]],
    *,
    platform: str,
    pid: str,
    cur_ts: float,
    age_sec: float,
    views: Any,
    missing_state: str,
) -> None:
    """处理详情接口明确返回空值(None)：曾存活帖子先疑诊二次确认，避免单次瞬态空响应误锁。"""
    was_live = (views is not None) or (rec.get("live_state") == "ok")
    next_miss = int(rec.get("missing_confirm_count") or 0) + 1
    rec["missing_confirm_count"] = next_miss
    rec["metrics_synced_at"] = cur_ts

    if was_live and next_miss < METRICS_MISSING_CONFIRM_COUNT:
        logger.warning(
            f"[发布/指标同步] {platform} 帖子 {pid} 本次返回空详情(疑诊 {next_miss}/{METRICS_MISSING_CONFIRM_COUNT})，"
            "暂保留原存活状态等待复核"
        )
        pub_records[platform] = rec
        need_upsert.append(doc)
        return

    if platform == PLATFORM_OKX:
        rec["review_state"] = None
    rec["live_state"] = missing_state
    _, delta_v = _append_metrics_timeline_point(
        rec,
        cur_ts=cur_ts,
        age_sec=age_sec,
        views=None,
        likes=None,
        comments=None,
        shares=None,
        recommend=None,
        status_code=None,
        live_state=missing_state,
    )
    pub_records[platform] = rec
    emit_telemetry_event(
        "metrics_point",
        {
            "platform": platform,
            "account": rec.get("published_by"),
            "article_type": doc.get("type"),
            "article_id": doc.get("article_id"),
            "post_id": str(pid),
            "age_m": round(age_sec / 60.0, 1),
            "delta_v": delta_v,
            "live_state": missing_state,
        },
        now_ts=cur_ts,
    )
    need_upsert.append(doc)


def sync_published_metrics(
    article_manager: Any,
    platform: str,
    docs: Sequence[Dict[str, Any]],
    *,
    now_ts: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """
    为已发布历史文章免登只读同步真实阅读/点赞/评论/状态与成长曲线 (`metrics_timeline`)，
    48h 内按帖龄自适应加密采样、成熟前不冻僵，回写至数据库后返回历史战报列表。
    """
    cur_ts = time.time() if now_ts is None else float(now_ts)
    history_items: List[Dict[str, Any]] = []
    need_upsert: List[Dict[str, Any]] = []

    for d in docs:
        pub_records = d.setdefault("publish_records", {}) if isinstance(d, dict) else {}
        rec = pub_records.get(platform)
        if not isinstance(rec, dict):
            rec = {}
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
        refresh_interval = _required_refresh_interval_seconds(age_sec)
        miss_cnt = int(rec.get("missing_confirm_count") or 0)
        unconfirmed_missing = rec.get("live_state") in ("deleted", "missing") and miss_cnt < METRICS_MISSING_CONFIRM_COUNT
        needs_missing_recheck = (0 < miss_cnt < METRICS_MISSING_CONFIRM_COUNT) or unconfirmed_missing
        # 【保留过滤原因说明】仅当连续多次空返回确认缺失(missing_confirm_count >= 2)且帖龄超过 2h 时停止轮询，
        # 避免对确已删除帖子无效请求，同时防止单次瞬态抖动或历史单次误判永久锁定存活帖子。
        settled_missing = (
            rec.get("live_state") in ("deleted", "missing")
            and miss_cnt >= METRICS_MISSING_CONFIRM_COUNT
            and age_sec >= 2 * 3600
        )
        effective_interval = (
            min(refresh_interval, METRICS_SUSPECT_RETRY_INTERVAL_SECONDS)
            if needs_missing_recheck
            else refresh_interval
        )

        need_sync = bool(
            pid
            and str(pid) != "None"
            and pub_time > 0
            and not settled_missing
            and (
                (
                    views is None
                    and age_sec >= METRICS_FIRST_SYNC_MIN_AGE_SECONDS
                    and (last_synced == 0.0 or (cur_ts - last_synced) >= effective_interval)
                )
                or (
                    METRICS_FIRST_SYNC_MIN_AGE_SECONDS <= age_sec <= METRICS_MAX_REFRESH_AGE_SECONDS
                    and (cur_ts - last_synced) >= effective_interval
                )
                or (
                    needs_missing_recheck
                    and age_sec >= METRICS_FIRST_SYNC_MIN_AGE_SECONDS
                    and (cur_ts - last_synced) >= effective_interval
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
                    detail = _call_post_detail_fetcher(fetch_binance_post_detail, str(pid))
                    if isinstance(detail, dict):
                        rec["missing_confirm_count"] = 0
                        content_status = detail.get("contentStatus")
                        recommend = detail.get("recommend")
                        shares = detail.get("shareCount", 0) if content_status == 2 else rec.get("share_count")
                        if content_status == 2:
                            views = rec["view_count"] = detail.get("viewCount", 0)
                            likes = rec["like_count"] = detail.get("likeCount", 0)
                            comments = rec["comment_count"] = detail.get("commentCount", 0)
                            rec["share_count"] = shares
                        if content_status is not None:
                            live_state = "ok" if content_status == 2 else ("hidden" if content_status == 5 else f"status_{content_status}")
                            rec["content_status"] = content_status
                            rec["recommend"] = recommend
                            rec["lan"] = detail.get("lan")
                            rec["detected_lang"] = detail.get("detectedLang")
                            if "coinPairList" in detail:
                                rec["coin_pair_list"] = detail.get("coinPairList")
                            if "tradeWidgets" in detail:
                                rec["trade_widget_count"] = len(detail.get("tradeWidgets") or [])
                            rec["live_state"] = live_state
                            rec["metrics_synced_at"] = cur_ts
                            _, delta_v = _append_metrics_timeline_point(
                                rec,
                                cur_ts=cur_ts,
                                age_sec=age_sec,
                                views=views,
                                likes=likes,
                                comments=comments,
                                shares=shares,
                                recommend=recommend,
                                status_code=content_status,
                                live_state=live_state,
                            )
                            pub_records[platform] = rec
                            emit_telemetry_event(
                                "metrics_point",
                                {
                                    "platform": platform,
                                    "account": rec.get("published_by"),
                                    "article_type": d.get("type"),
                                    "article_id": d.get("article_id"),
                                    "post_id": str(pid),
                                    "age_m": round(age_sec / 60.0, 1),
                                    "views": views,
                                    "delta_v": delta_v,
                                    "likes": likes,
                                    "comments": comments,
                                    "shares": shares,
                                    "recommend": recommend,
                                    "status_code": content_status,
                                    "live_state": live_state,
                                },
                                now_ts=cur_ts,
                            )
                            need_upsert.append(d)
                    elif detail is None and age_sec >= METRICS_FIRST_SYNC_MIN_AGE_SECONDS:
                        _handle_missing_post_detail(
                            rec,
                            pub_records,
                            d,
                            need_upsert,
                            platform=platform,
                            pid=str(pid),
                            cur_ts=cur_ts,
                            age_sec=age_sec,
                            views=views,
                            missing_state="deleted",
                        )
                elif platform == PLATFORM_OKX:
                    detail = _call_post_detail_fetcher(fetch_okx_post_detail, str(pid))
                    if isinstance(detail, dict):
                        rec["missing_confirm_count"] = 0
                        review_state = detail.get("reviewState")
                        rec["review_state"] = review_state
                        rec["metrics_synced_at"] = cur_ts
                        if review_state == 2:
                            views = rec["view_count"] = detail.get("viewCount", 0)
                            likes = rec["like_count"] = detail.get("likeCount", 0)
                            comments = rec["comment_count"] = detail.get("commentNum", 0)
                            live_state = "ok"
                        else:
                            live_state = f"review_{review_state}"
                        rec["live_state"] = live_state
                        _, delta_v = _append_metrics_timeline_point(
                            rec,
                            cur_ts=cur_ts,
                            age_sec=age_sec,
                            views=views,
                            likes=likes,
                            comments=comments,
                            shares=None,
                            recommend=None,
                            status_code=review_state,
                            live_state=live_state,
                        )
                        pub_records[platform] = rec
                        emit_telemetry_event(
                            "metrics_point",
                            {
                                "platform": platform,
                                "account": rec.get("published_by"),
                                "article_type": d.get("type"),
                                "article_id": d.get("article_id"),
                                "post_id": str(pid),
                                "age_m": round(age_sec / 60.0, 1),
                                "views": views,
                                "delta_v": delta_v,
                                "likes": likes,
                                "comments": comments,
                                "status_code": review_state,
                                "live_state": live_state,
                            },
                            now_ts=cur_ts,
                        )
                        need_upsert.append(d)
                    elif detail is None and age_sec >= METRICS_FIRST_SYNC_MIN_AGE_SECONDS:
                        _handle_missing_post_detail(
                            rec,
                            pub_records,
                            d,
                            need_upsert,
                            platform=platform,
                            pid=str(pid),
                            cur_ts=cur_ts,
                            age_sec=age_sec,
                            views=views,
                            missing_state="missing",
                        )
            except Exception as exc:
                logger.warning(f"[发布/指标同步] 拉取 {platform} 帖子 {pid} 详情异常(已保留原状态跳过): {exc}")

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
