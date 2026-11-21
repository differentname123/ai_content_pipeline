# -*- coding: utf-8 -*-
"""
Reddit 平台专用采集与差异抹平适配器 (RedditHarvester)

================================================================================
【运维速查手册 (Ops Quick Guide) —— 日常巡检与修改必读】
================================================================================
1. 专用爬虫小号复用与账号隔离：
   - 默认复用 `config/config.json` 中的 `x_xiaohao_browser_session_dir` (`W:\\temp\\x_xiaohao`) 中已登录的 Reddit 小号 Cookie
     (`reddit_session` / `token_v2`)，与 X 共享同一个只读爬虫浏览器目录，**无需新建目录，且严禁触碰主号 `x_myself`**。
   - Cookie 提取走 `extract_domain_cookies_from_user_data_dir` 持久化文件缓存（`<1ms` 毫秒级读取，不拉起浏览器进程，与 X 零锁冲突）。
   - 若未来需重新登录 Reddit 小号，直接调用本模块 `open_xiaohao_for_reddit_login()` 人工扫码/输入一次即可。

2. 两大核心采集能力（经实测全 `HTTP 200` 接口矩阵，避开所有 `403` 死路）：
   - **能力 A：热门榜单与趋势流 (`fetch_hot_or_recommended` / `harvest`)**
     * 主通道（已登录小号 Cookie + `curl_cffi` Chrome TLS）：
       ① 全站实时热门榜 `/r/popular/{sort}.json`（捕捉全网突发大瓜，配额 ~100 次/分钟）；
       ② 15 大黄金垂直板块聚合流 `/r/{sub1+sub2+...}/{sort}.json`（原生携带真实点赞 `score`、评论数、多图相册与外链）。
     * 兜底通道（未登录或 Cookie 失效时记录告警后降级）：15 板块单请求 `.rss`（带 120s 内存防限流缓存）+ `ArcticShift /api/posts/ids` 分批补齐。
   - **能力 B：指定关键词全站精准搜索 (`search_or_topic_posts`)**
     * 避坑铁律：Reddit 对 `/search.json`、`/r/all/search.json`、`/by_id/` 即使带登录 Cookie 仍返 `HTTP 403`，严禁使用！
     * 主通道（实测 `HTTP 200`，配额 `600 次/10分钟`）：调用官方新版搜索流 `/svc/shreddit/search/?q={kw}&type=posts&t={t}&sort={top|relevance|new}`
       扫描全站帖子 `(subreddit, post_id)` -> 先走 `ArcticShift /api/posts/ids` 批量秒级还原 -> 对刚发布尚未入镜像库的实时热帖，
       直接走 `/r/{sub}/comments/{id}.json` 一次请求同时拿齐最新主帖正文与前排神评！

3. 异常感知与数据质量自检：
   - 实时监测 Cookie 掉线 (`401/403`)、频率限制 (`429`)、`shreddit` DOM 结构漂移、评论接口失灵及残缺帖，统一输出至 `get_health_summary()`。
================================================================================
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
import html
from html.parser import HTMLParser
import json
import logging
import os
from pathlib import Path
import re
import threading
import time
from urllib.parse import quote, unquote
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    cffi_requests = None

from common.common_utils import (
    DEFAULT_RUNTIME_DIR,
    PLATFORM_REDDIT,
    clean_post_text,
    normalize_publish_timestamp,
    resolve_session_dir,
    safe_int,
)
from platforms.external_link_extractor import (
    enrich_posts_with_external_articles,
    is_fetchable_external_article_url,
)
from platforms.harvesters.base_harvester import BasePlatformHarvester
from platforms.x_api import (
    extract_domain_cookies_from_user_data_dir,
    invalidate_burner_cookie_cache,
)

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 15
FIREFOX_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0"
CHROME_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"

REDDIT_BURNER_COOLDOWN_SECONDS = 6 * 3600
_REDDIT_BURNER_HEALTH_STATE_PATH: Path = DEFAULT_RUNTIME_DIR / "reddit_burner_health_state.json"
_REDDIT_BURNER_HEALTH_LOCK = threading.Lock()

# 覆盖全球突发新闻、科技/AI巨头、商业吃瓜、美股期权与加密圈的 15 大黄金板块
DEFAULT_GOLDMINE_SUBREDDITS = [
    "wallstreetbets",       # 期权豪赌、暴富与破产交割单圣地
    "stocks",               # 美股科技巨头与财报大瓜
    "investing",            # 深度投资逻辑与宏观市场讨论
    "technology",           # 科技圈丑闻与商业政策巨震
    "news",                 # 美国与全球突发社会新闻
    "worldnews",            # 国际地缘政经突发大事件
    "nvidia",               # AI 算力龙头、黄仁勋与芯片生态动态
    "openai",               # 大模型前沿风波与高管内幕
    "singularity",          # AI 突破与硅谷内幕
    "CryptoCurrency",       # 加密圈最大英文主战场
    "Bitcoin",              # 比特币宏观叙事与巨鲸动向
    "FluentInFinance",      # 极易引发转发与争论的财富认知图
    "Fauxmoi",              # 欧美名人、亿万富豪与科技高管八卦爆料主阵地
    "OutOfTheLoop",         # 全网突发热门事件来龙去脉高赞科普阵地
    "business",             # 商业巨头兴衰与公司治理内幕
]

_CJK_RE = re.compile(r"[\u4e00-\u9fff]")
_REDDIT_CN_KEYWORD_MAP: Dict[str, str] = {
    "比特币": "Bitcoin",
    "大饼": "Bitcoin",
    "以太坊": "Ethereum",
    "英伟达": "Nvidia",
    "黄仁勋": "Jensen Huang",
    "马斯克": "Elon Musk",
    "特斯拉": "Tesla",
    "特朗普": "Trump",
    "川普": "Trump",
    "美联储": "Federal Reserve",
    "鲍威尔": "Jerome Powell",
    "苹果": "Apple",
    "微软": "Microsoft",
    "谷歌": "Google",
    "字节跳动": "ByteDance",
    "币安": "Binance",
    "赵长鹏": "Changpeng Zhao",
}


def _prepare_reddit_search_keywords(raw_keywords: Sequence[str]) -> List[str]:
    """
    针对英文主阵地 Reddit 优化检索词顺序：
    1) 纯英文/ASCII 关键词排在最前优先检索；
    2) 中英混合词（如 `'Jensen Huang 出轨'`）提取英文核心短语并保留映射；
    3) 纯中文常见实体（如 `'黄仁勋'`、`'比特币'`）自动补充英文标准词，绝不因纯中文词在英文社区漏搜。
    """
    ascii_first: List[str] = []
    cjk_fallback: List[str] = []
    seen = set()

    def _add(target: List[str], term: str) -> None:
        s = " ".join(str(term or "").split()).strip()
        if not s or len(s) < 2:
            return
        k = s.lower()
        if k not in seen:
            seen.add(k)
            target.append(s)

    for raw in raw_keywords or []:
        kw = str(raw or "").strip()
        if not kw:
            continue
        if not _CJK_RE.search(kw):
            _add(ascii_first, kw)
            continue

        # 提取中英混合词中的英文部分
        ascii_only = " ".join(re.findall(r"[A-Za-z0-9$#@._-]+", kw)).strip()
        if len(ascii_only) >= 2:
            _add(ascii_first, ascii_only)

        # 检查是否命中常见中文实体映射（支持短语包含替换，如 "黄仁勋 出轨" -> "Jensen Huang"）
        for cn_key, en_val in _REDDIT_CN_KEYWORD_MAP.items():
            if cn_key in kw:
                _add(ascii_first, en_val)

        _add(cjk_fallback, kw)

    return ascii_first + cjk_fallback


def open_xiaohao_for_reddit_login() -> None:
    """人工运维辅助入口：打开共用的 `x_xiaohao` 持久化浏览器直达 Reddit 登录页。"""
    from platforms.playwright_base import open_browser_for_manual_use

    xiaohao_dir = resolve_session_dir("x_xiaohao_browser_session_dir", "x_xiaohao")
    invalidate_burner_cookie_cache(xiaohao_dir)
    open_browser_for_manual_use(user_data_dir=xiaohao_dir, start_url="https://www.reddit.com/login/")


class _RedditHTMLParser(HTMLParser):
    """使用 Python 标准库 HTMLParser 解析 Reddit Atom `<content>`，提取正文、配图、视频与外部新闻链接。"""

    def __init__(self) -> None:
        super().__init__()
        self.text_parts: List[str] = []
        self.image_urls: List[str] = []
        self.video_urls: List[str] = []
        self.external_urls: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        attr_dict = dict(attrs)
        candidate_url = ""
        if tag == "a":
            candidate_url = attr_dict.get("href") or ""
        elif tag == "img":
            candidate_url = attr_dict.get("src") or ""
        if candidate_url.startswith(("http://", "https://")):
            clean_u = html.unescape(candidate_url)
            u_low = clean_u.lower()
            if "v.redd.it" in u_low and clean_u not in self.video_urls:
                self.video_urls.append(clean_u)
            elif ("i.redd.it" in u_low or "preview.redd.it" in u_low or "imgur.com" in u_low) and clean_u not in self.image_urls:
                self.image_urls.append(clean_u)
            elif tag == "a" and is_fetchable_external_article_url(clean_u) and clean_u not in self.external_urls:
                self.external_urls.append(clean_u)
        if tag in ("p", "br", "div", "li", "tr"):
            self.text_parts.append("\n")

    def handle_data(self, data: str) -> None:
        if data:
            self.text_parts.append(data)


def _strip_reddit_prefix(raw: str) -> str:
    s = str(raw or "").strip()
    for prefix in ("/u/", "u/", "/r/", "r/"):
        if s.lower().startswith(prefix):
            return s[len(prefix):].strip()
    return s


def _extract_reddit_base36_id(raw_target: str) -> str:
    """从完整帖子 URL 或 `reddit_xxx` / `t3_xxx` 中提取纯净 base36 ID。"""
    s = str(raw_target or "").strip()
    if "/comments/" in s:
        after = s.split("/comments/", 1)[1].strip("/")
        return after.split("/", 1)[0].strip()
    if "/gallery/" in s:
        after = s.split("/gallery/", 1)[1].strip("/")
        return after.split("/", 1)[0].strip()
    return s.replace("reddit_", "").replace("t3_", "").strip("/ ")


def _deduplicate_reddit_image_urls(urls: List[str]) -> List[str]:
    """
    1) 对 `preview.redd.it/<asset>.(png|jpg|jpeg|webp|gif)` 顶层用户原图，直接无损升级为 `https://i.redd.it/<asset>` 原始高清图；
    2) 按底层资源文件名去重，优先保留 `i.redd.it` 原图或非 `crop=1:1` 的高清大图，彻底杜绝 140px 邮票缩略图入库。
    """
    from urllib.parse import parse_qs, urlsplit

    def _upgrade_preview_url(u: str) -> str:
        sp = urlsplit(u)
        if (
            sp.netloc.lower() == "preview.redd.it"
            and sp.path.count("/") == 1
            and sp.path.lower().endswith((".png", ".jpg", ".jpeg", ".webp", ".gif"))
        ):
            return f"https://i.redd.it{sp.path}"
        return u

    def _score_url(u: str) -> int:
        sp = urlsplit(u)
        qs = parse_qs(sp.query)
        if "i.redd.it" in sp.netloc:
            return 100_000
        if "crop" in qs:
            return 10
        widths = qs.get("width") or ["0"]
        try:
            return max(100, int(widths[0]))
        except ValueError:
            return 500

    best_by_asset: Dict[str, str] = {}
    asset_order: List[str] = []
    for raw_u in urls:
        u = _upgrade_preview_url(raw_u)
        sp = urlsplit(u)
        netloc_low = sp.netloc.lower()
        leaf_name = sp.path.rsplit("/", 1)[-1].lower()
        if netloc_low == "i.redd.it" and "." not in leaf_name:
            continue
        asset_name = leaf_name.rsplit(".", 1)[0] if ("redd.it" in netloc_low and "." in leaf_name) else (leaf_name if "redd.it" in netloc_low else u)
        if not asset_name:
            asset_name = u
        if asset_name not in best_by_asset:
            asset_order.append(asset_name)
            best_by_asset[asset_name] = u
        elif _score_url(u) > _score_url(best_by_asset[asset_name]):
            best_by_asset[asset_name] = u

    return [best_by_asset[k] for k in asset_order]


def _parse_reddit_html(raw_html: str) -> Tuple[str, List[str], List[str]]:
    """从 Reddit Atom HTML 内容中提取纯文本、去重高清图片列表与视频链接列表，剔除固定页脚。"""
    txt, imgs, vids, _ = _parse_reddit_html_full(raw_html)
    return txt, imgs, vids


def _parse_reddit_html_full(raw_html: str) -> Tuple[str, List[str], List[str], List[str]]:
    """从 Reddit Atom HTML 内容中同时提取纯文本、高清图片、视频链接与外部新闻链接。"""
    if not raw_html:
        return "", [], [], []
    parser = _RedditHTMLParser()
    parser.feed(raw_html)
    parser.close()
    raw_text = "".join(parser.text_parts)
    footer_idx = raw_text.lower().rfind("submitted by")
    if footer_idx != -1 and ("[link]" in raw_text[footer_idx:].lower() or "[comments]" in raw_text[footer_idx:].lower()):
        raw_text = raw_text[:footer_idx]
    return (
        clean_post_text(raw_text),
        _deduplicate_reddit_image_urls(parser.image_urls),
        parser.video_urls,
        parser.external_urls,
    )


def _extract_urls_from_plain_text(text: str) -> List[str]:
    """从评论纯文本中提取直接附带的 Reddit / Imgur 图片链接。"""
    found: List[str] = []
    for tok in str(text or "").split():
        clean_tok = tok.strip("()[]<>\"',.;")
        low = clean_tok.lower()
        if low.startswith(("http://", "https://")) and any(d in low for d in ("i.redd.it/", "preview.redd.it/", "i.imgur.com/")):
            if clean_tok not in found:
                found.append(clean_tok)
    return _deduplicate_reddit_image_urls(found)


class RedditHarvester(BasePlatformHarvester):
    """
    Reddit 平台采集与归一化适配器。
    优先复用 `x_xiaohao` 中的 Reddit 登录态走官方 `.json` 热榜与 `/svc/shreddit/search/` 全站实时检索，
    并内置登录态掉线、403/429 拦截、DOM 变更及免登 RSS 降级的全链路异常感知。
    """

    SOURCE_NAME = PLATFORM_REDDIT
    ARCTIC_SHIFT_BASE = "https://arctic-shift.photon-reddit.com/api"

    def __init__(
        self,
        burner_cookies: str = "",
        burner_user_data_dir: Optional[str] = None,
        proxies: Optional[Dict[str, str]] = None,
    ):
        super().__init__(proxies=proxies)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": FIREFOX_UA})
        self._last_rss_rl_remaining: float = 1.0
        self._rss_feed_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}

        default_xiaohao_dir = resolve_session_dir("x_xiaohao_browser_session_dir", "x_xiaohao")
        effective_dir = default_xiaohao_dir if burner_user_data_dir is None else str(burner_user_data_dir).strip()
        if effective_dir and "x_myself" in os.path.normcase(os.path.abspath(effective_dir)):
            self.record_health_issue(
                code="REDDIT_MYSELF_ISOLATION_GUARD",
                message=f"拦截到 Reddit 爬虫传入主号目录 `{effective_dir}`，已强制重定向至专用小号目录 `{default_xiaohao_dir}`",
                level="warning",
                channel="init",
                persistent=True,
            )
            effective_dir = default_xiaohao_dir

        self.burner_user_data_dir = effective_dir
        self._explicit_zero_auth = bool(burner_user_data_dir is not None and not str(burner_user_data_dir).strip() and not burner_cookies)
        if not burner_cookies and self.burner_user_data_dir and os.path.exists(self.burner_user_data_dir):
            burner_cookies = extract_domain_cookies_from_user_data_dir(
                user_data_dir=self.burner_user_data_dir,
                urls=["https://www.reddit.com", "https://old.reddit.com"],
                required_any_names=("reddit_session", "token_v2"),
            )
        self.burner_cookies = burner_cookies or ""

        if not self.has_burner_account and not self._explicit_zero_auth:
            self.record_health_issue(
                code="REDDIT_BURNER_AUTH_MISSING",
                message=(
                    f"专用小号目录 `{self.burner_user_data_dir or '未配置'}` 未检测到 Reddit 登录 Cookie (reddit_session)，"
                    f"已自动切换免登双源热榜+镜像搜索模式；建议有空运行 `open_xiaohao_for_reddit_login()` 登录小号。"
                ),
                level="warning",
                channel="auth",
                persistent=True,
            )

    def _burner_state_fingerprint(self) -> str:
        cookie_tail = self.burner_cookies[-24:] if self.burner_cookies else ""
        mtime_part = "0"
        if self.burner_user_data_dir:
            for cand in (
                os.path.join(self.burner_user_data_dir, "Default", "Network", "Cookies"),
                os.path.join(self.burner_user_data_dir, "Default", "Cookies"),
            ):
                if os.path.exists(cand):
                    try:
                        mtime_part = str(int(os.path.getmtime(cand)))
                    except OSError:
                        pass
                    break
        return f"{cookie_tail}:{mtime_part}"

    def _is_burner_cooling_down(self) -> Tuple[bool, int, str]:
        if not self.has_burner_account:
            return False, 0, ""
        try:
            with _REDDIT_BURNER_HEALTH_LOCK:
                if not _REDDIT_BURNER_HEALTH_STATE_PATH.exists():
                    return False, 0, ""
                state = json.loads(_REDDIT_BURNER_HEALTH_STATE_PATH.read_text(encoding="utf-8") or "{}")
            until_ts = safe_int(state.get("cooldown_until_ts"), 0)
            now_ts = int(time.time())
            if until_ts <= now_ts:
                return False, 0, ""
            if state.get("fingerprint") and state.get("fingerprint") != self._burner_state_fingerprint():
                return False, 0, ""
            return True, until_ts - now_ts, str(state.get("reason") or "401/403")
        except Exception:
            return False, 0, ""

    def _mark_burner_cooldown(self, reason: str, channel: str = "reddit_auth") -> None:
        now_ts = int(time.time())
        until_ts = now_ts + REDDIT_BURNER_COOLDOWN_SECONDS
        payload = {
            "updated_at_ts": now_ts,
            "cooldown_until_ts": until_ts,
            "reason": str(reason or "Reddit auth blocked"),
            "fingerprint": self._burner_state_fingerprint(),
        }
        try:
            with _REDDIT_BURNER_HEALTH_LOCK:
                _REDDIT_BURNER_HEALTH_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
                _REDDIT_BURNER_HEALTH_STATE_PATH.write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
        except Exception as e:
            logger.debug(f"[Reddit适配器] 写入冷却状态失败: {e}")
        invalidate_burner_cookie_cache(self.burner_user_data_dir)
        self.record_health_issue(
            code="REDDIT_BURNER_COOLDOWN_ENTERED",
            message=f"Reddit 小号鉴权受限 ({reason})，已进入 6h 静默保护并无缝切换至 100% 免登双源热榜+镜像检索通道",
            level="warning",
            channel=channel,
        )

    def _mark_burner_healthy(self) -> None:
        try:
            with _REDDIT_BURNER_HEALTH_LOCK:
                if _REDDIT_BURNER_HEALTH_STATE_PATH.exists():
                    state = json.loads(_REDDIT_BURNER_HEALTH_STATE_PATH.read_text(encoding="utf-8") or "{}")
                    if safe_int(state.get("cooldown_until_ts"), 0) > 0:
                        _REDDIT_BURNER_HEALTH_STATE_PATH.unlink(missing_ok=True)
        except Exception:
            pass

    @property
    def has_burner_account(self) -> bool:
        return bool("reddit_session=" in self.burner_cookies or "token_v2=" in self.burner_cookies)

    @property
    def is_burner_usable(self) -> bool:
        if not self.has_burner_account:
            return False
        cooling, _, _ = self._is_burner_cooling_down()
        return not cooling

    @property
    def burner_account_summary(self) -> str:
        if not self.has_burner_account:
            return "免登独立运行 (r/popular+15板块双RSS + ArcticShift镜像检索与评论)"
        cooling, rem_s, reason = self._is_burner_cooling_down()
        session_dir_name = os.path.basename(os.path.normpath(self.burner_user_data_dir)) if self.burner_user_data_dir else "直接Cookie注入"
        if cooling:
            return f"免登主干运行中 | 小号静默冷却剩余 {round(rem_s / 60)}min ({reason}, Session={session_dir_name})"
        return f"专用小号在线 (Session={session_dir_name}, 原生JSON全量热榜+shreddit全站检索)"

    def _parse_atom_entries(self, xml_text: str) -> List[Dict[str, Any]]:
        """内部核心：解析 Reddit Atom XML 中的 `<entry>` 列表并转换为统一标准 Schema"""
        root = ET.fromstring(xml_text)
        ns = {"atom": "http://www.w3.org/2005/Atom", "media": "http://search.yahoo.com/mrss/"}
        entries = root.findall("atom:entry", ns)
        results = []

        for idx, ent in enumerate(entries):
            raw_id = (ent.find("atom:id", ns).text or "") if ent.find("atom:id", ns) is not None else ""
            if "t1_" in raw_id:
                continue
            post_id = raw_id.replace("t3_", "").strip()
            if not post_id:
                continue

            title = html.unescape(ent.find("atom:title", ns).text or "") if ent.find("atom:title", ns) is not None else ""
            link_el = ent.find("atom:link", ns)
            post_url = link_el.attrib.get("href", "") if link_el is not None else ""
            cat_el = ent.find("atom:category", ns)
            sub_name = _strip_reddit_prefix(cat_el.attrib.get("term", "reddit")) if cat_el is not None else "reddit"
            author_el = ent.find("atom:author/atom:name", ns)
            author_name = _strip_reddit_prefix(author_el.text or "") if author_el is not None else "redditor"
            updated_str = ent.find("atom:updated", ns).text if ent.find("atom:updated", ns) is not None else ""

            content_el = ent.find("atom:content", ns)
            raw_html = html.unescape(content_el.text or "") if content_el is not None else ""
            body_txt, imgs, vids, ext_urls = _parse_reddit_html_full(raw_html)

            video_url = vids[0] if vids else (post_url if "v.redd.it" in post_url.lower() else None)
            full_text = f"【Reddit r/{sub_name}】{title}\n{body_txt}".strip() if body_txt else f"【Reddit r/{sub_name}】{title}"

            card_type = "BUZZ_VIDEO" if video_url else ("BUZZ_LONG" if len(body_txt) > 500 else "BUZZ_SHORT")
            item = self.create_empty_post(card_type=card_type)
            item["post_id"] = f"reddit_{post_id}"
            item["publish_time"] = normalize_publish_timestamp(updated_str)
            item["author_id"] = author_name
            item["author"].update({
                "username": author_name,
                "author_name": author_name,
                "is_verified": False,
            })
            item["content"].update({
                "title": title,
                "text_content": full_text,
                "hashtags": [sub_name] if sub_name else [],
                "mentioned_coins": [],
            })
            item["media"].update({
                "cover_image": imgs[0] if imgs else None,
                "images": imgs,
                "video_url": video_url,
                "video_duration": 1 if video_url else 0,
            })
            item["engagement"].update({
                "view_count": 0,
                "like_count": 0,
                "comment_count": 0,
            })
            meta_dict: Dict[str, Any] = {
                "url": post_url,
                "subreddit": f"r/{sub_name}",
                "rank_in_feed": idx + 1,
            }
            if ext_urls:
                meta_dict["external_url"] = ext_urls[0]
            item["metadata"].update(meta_dict)
            results.append(self.normalize_post(item, download_media=False))

        return results

    def _apply_arctic_shift_meta_to_item(self, item: Dict[str, Any], meta: Dict[str, Any]) -> None:
        """将 Arctic Shift 返回的完整节点数据合并进单条 Reddit 帖子结构。"""
        mime_to_ext = {
            "image/png": "png",
            "image/jpg": "jpg",
            "image/jpeg": "jpg",
            "image/webp": "webp",
            "image/gif": "gif",
        }
        pid = str(meta.get("id") or "").strip()
        eng = item.setdefault("engagement", {})
        eng["like_count"] = max(safe_int(eng.get("like_count"), 0), safe_int(meta.get("score"), 0))
        eng["comment_count"] = max(safe_int(eng.get("comment_count"), 0), safe_int(meta.get("num_comments"), 0))

        created_utc = safe_int(meta.get("created_utc"), 0)
        if created_utc > 0:
            item["publish_time"] = created_utc

        rv = ((meta.get("media") or {}).get("reddit_video")) or ((meta.get("secure_media") or {}).get("reddit_video")) or {}
        ext_url = html.unescape(str(meta.get("url") or "")).strip()
        is_vid = bool(
            meta.get("is_video")
            or rv
            or str(meta.get("domain") or "").lower() == "v.redd.it"
            or "v.redd.it" in ext_url.lower()
        )
        if is_vid:
            v_url = rv.get("fallback_url") or (ext_url if "v.redd.it" in ext_url.lower() else f"https://v.redd.it/{pid}")
            item["card_type"] = "BUZZ_VIDEO"
            item["media"]["video_url"] = v_url
            item["media"]["video_duration"] = max(1, safe_int(rv.get("duration"), 1))
        elif ext_url:
            low_ext = ext_url.lower()
            if any(d in low_ext for d in ("i.redd.it/", "preview.redd.it/", "i.imgur.com/")):
                from urllib.parse import urlsplit as _urlsplit

                sp_ext = _urlsplit(ext_url)
                has_img_ext = sp_ext.path.lower().endswith((".png", ".jpg", ".jpeg", ".webp", ".gif"))
                candidate_single_urls: List[str] = []
                if sp_ext.netloc.lower() == "i.redd.it" and not has_img_ext:
                    preview_src = html.unescape(
                        str((((meta.get("preview") or {}).get("images") or [{}])[0].get("source") or {}).get("url") or "")
                    ).strip()
                    if preview_src:
                        candidate_single_urls.append(preview_src)
                else:
                    candidate_single_urls.append(ext_url)
                merged_single = _deduplicate_reddit_image_urls(
                    candidate_single_urls + list(item["media"].get("images") or [])
                )
                item["media"]["images"] = merged_single
                item["media"]["cover_image"] = merged_single[0] if merged_single else None
            elif is_fetchable_external_article_url(ext_url):
                item.setdefault("metadata", {})["external_url"] = ext_url
                item["metadata"].setdefault("platform_extra", {})["external_url"] = ext_url

        mm = meta.get("media_metadata") or {}
        if isinstance(mm, dict) and mm:
            ordered_mids: List[str] = []
            g_items = ((meta.get("gallery_data") or {}).get("items")) or []
            for gi in g_items:
                if isinstance(gi, dict) and not gi.get("is_deleted") and gi.get("media_id"):
                    ordered_mids.append(str(gi["media_id"]))
            for k in mm.keys():
                if k not in ordered_mids:
                    ordered_mids.append(str(k))

            gallery_urls: List[str] = []
            for mid in ordered_mids:
                m_obj = mm.get(mid) or {}
                if m_obj.get("status") and str(m_obj.get("status")).lower() != "valid":
                    continue
                if m_obj.get("e") == "RedditVideo":
                    item["card_type"] = "BUZZ_VIDEO"
                    item["media"]["video_url"] = item["media"].get("video_url") or f"https://v.redd.it/{mid}"
                    continue
                s_obj = m_obj.get("s") if isinstance(m_obj.get("s"), dict) else {}
                s_url = html.unescape(str(s_obj.get("u") or s_obj.get("gif") or "")).strip()
                if s_url:
                    gallery_urls.append(s_url)
                else:
                    ext = mime_to_ext.get(str(m_obj.get("m") or "").lower(), "")
                    if ext:
                        gallery_urls.append(f"https://i.redd.it/{mid}.{ext}")

            if gallery_urls:
                merged_imgs = _deduplicate_reddit_image_urls(gallery_urls + list(item["media"].get("images") or []))
                item["media"]["images"] = merged_imgs
                item["media"]["cover_image"] = merged_imgs[0] if merged_imgs else None

    def _reddit_cffi_get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        accept_html: bool = False,
        timeout: int = REQUEST_TIMEOUT,
    ):
        """使用 Chrome TLS 指纹 (`curl_cffi`) + 专用小号 Reddit Cookie 发起请求。"""
        headers: Dict[str, str] = {
            "User-Agent": CHROME_UA,
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
                if accept_html
                else "application/json, text/plain, */*"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.reddit.com/",
        }
        if self.is_burner_usable and self.burner_cookies:
            headers["Cookie"] = self.burner_cookies

        if cffi_requests is not None:
            return cffi_requests.get(
                url,
                params=params,
                headers=headers,
                impersonate="chrome",
                proxies=self.proxies,
                timeout=timeout,
            )
        return requests.get(
            url,
            params=params,
            headers=headers,
            proxies=self.proxies,
            timeout=timeout,
        )

    def _build_post_from_reddit_dict(
        self,
        meta: Dict[str, Any],
        min_publish_ts: int = 0,
    ) -> Optional[Dict[str, Any]]:
        """将官方 `.json` 或 ArcticShift 返回的 `t3` 帖子字典统一转换为标准 Schema。"""
        if not isinstance(meta, dict) or not meta.get("id"):
            return None
        pid = str(meta["id"]).strip()
        created_utc = safe_int(meta.get("created_utc"), 0)
        if min_publish_ts > 0 and created_utc > 0 and created_utc < min_publish_ts:
            return None

        title = clean_post_text(html.unescape(str(meta.get("title") or "")))
        raw_selftext = clean_post_text(html.unescape(str(meta.get("selftext") or "")))
        is_deleted_body = raw_selftext.lower() in ("[deleted]", "[removed]")
        # 【保留过滤原因说明】剔除已被版主/反垃圾系统删空的纯文字废帖 (`removed_by_category` 或 `is_self` 且正文为 `[removed]`/`[deleted]`)：
        # Reddit 删帖后会保留光秃秃的标题但清空正文；若为无外链无媒体的纯文字帖 (`is_self=True`)，继续拉评论和送入下游纯属浪费时间。
        if meta.get("removed_by_category") and bool(meta.get("is_self")):
            return None
        if is_deleted_body and bool(meta.get("is_self")):
            return None

        selftext = "" if is_deleted_body else raw_selftext
        if not title and not selftext:
            return None

        sub_name = _strip_reddit_prefix(str(meta.get("subreddit") or "reddit"))
        author_name = _strip_reddit_prefix(str(meta.get("author") or "redditor"))
        permalink = str(meta.get("permalink") or f"/r/{sub_name}/comments/{pid}/")
        post_url = f"https://www.reddit.com{permalink}" if permalink.startswith("/") else permalink
        full_text = f"【Reddit r/{sub_name}】{title}\n{selftext}".strip() if selftext else f"【Reddit r/{sub_name}】{title}"

        item = self.create_empty_post(card_type="BUZZ_LONG" if len(selftext) > 500 else "BUZZ_SHORT")
        item["post_id"] = f"reddit_{pid}"
        item["publish_time"] = created_utc or int(time.time())
        item["author_id"] = author_name
        item["author"].update({
            "username": author_name,
            "author_name": author_name,
            "is_verified": False,
        })
        item["content"].update({
            "title": title,
            "text_content": full_text,
            "hashtags": [sub_name] if sub_name else [],
            "mentioned_coins": [],
        })
        item["metadata"].update({
            "url": post_url,
            "subreddit": f"r/{sub_name}",
        })
        self._apply_arctic_shift_meta_to_item(item, meta)
        return self.normalize_post(item, download_media=False)

    _ARCTIC_GATE_LOCK = threading.Lock()
    _ARCTIC_WAIT_UNTIL_MONO: float = 0.0

    def _arctic_shift_get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: int = 12,
    ) -> Optional[requests.Response]:
        """
        统一调用 `ArcticShift` 公开镜像并精确遵循 `x-ratelimit-reset` 令牌桶窗口：
        一旦检测到瞬时 `HTTP 429`，全线程共享等待 `x-ratelimit-reset + 1s` 后自动重试 1 次，杜绝并发雪崩丢评论/丢帖。
        """
        url = f"{self.ARCTIC_SHIFT_BASE}/{endpoint.lstrip('/')}"
        headers = {"User-Agent": FIREFOX_UA}
        for attempt in range(2):
            with self._ARCTIC_GATE_LOCK:
                wait_s = self.__class__._ARCTIC_WAIT_UNTIL_MONO - time.monotonic()
            if wait_s > 0:
                time.sleep(min(wait_s, 30.0))

            try:
                resp = requests.get(
                    url,
                    params=params,
                    headers=headers,
                    proxies=self.proxies,
                    timeout=timeout,
                )
            except Exception as e:
                logger.debug(f"[Reddit适配器] ArcticShift {endpoint} 请求异常: {e}")
                return None

            if resp.status_code != 429:
                return resp

            if attempt == 0:
                try:
                    reset_s = float(resp.headers.get("x-ratelimit-reset", "10"))
                except ValueError:
                    reset_s = 10.0
                cool_s = min(28.0, max(2.0, reset_s)) + 1.2
                with self._ARCTIC_GATE_LOCK:
                    target_mono = time.monotonic() + cool_s
                    if target_mono > self.__class__._ARCTIC_WAIT_UNTIL_MONO:
                        self.__class__._ARCTIC_WAIT_UNTIL_MONO = target_mono
                        logger.debug(f"[Reddit适配器] ArcticShift 触发每分钟配额窗口，平滑等待 {cool_s:.1f}s 后继续")
        return None

    def _enrich_posts_via_arctic_shift(self, posts: List[Dict[str, Any]]) -> None:
        """分批（每批最多 50 个 ID）请求公开镜像 `posts/ids`，补齐赞评数、外链、多图相册与视频标记。"""
        if not posts:
            return
        id_map: Dict[str, Dict[str, Any]] = {}
        for p in posts:
            b36 = _extract_reddit_base36_id(p.get("post_id", ""))
            if b36:
                id_map[b36] = p
        if not id_map:
            return

        all_ids = list(id_map.keys())
        for start_idx in range(0, len(all_ids), 50):
            batch_ids = all_ids[start_idx : start_idx + 50]
            try:
                resp = self._arctic_shift_get(
                    "posts/ids",
                    params={"ids": ",".join(batch_ids)},
                    timeout=12,
                )
                if resp is None or resp.status_code != 200:
                    continue
                for meta in (resp.json().get("data") or []):
                    if not isinstance(meta, dict):
                        continue
                    pid = str(meta.get("id") or "").strip()
                    item = id_map.get(pid)
                    if item:
                        self._apply_arctic_shift_meta_to_item(item, meta)
            except Exception as e:
                logger.debug(f"[Reddit适配器] 批量元数据富化跳过: {e}")

    def _build_posts_from_arctic_shift_ids(
        self,
        base36_ids: Sequence[str],
        min_publish_ts: int = 0,
    ) -> Tuple[List[Dict[str, Any]], set]:
        """根据帖子 ID 列表分批（每批 50 个，无总上限截断）从 Arctic Shift 构建标准帖子，返回 `(已构建列表, 镜像库已命中的ID集合)`。"""
        clean_ids = list(dict.fromkeys(str(i).strip() for i in base36_ids if i and str(i).strip()))
        if not clean_ids:
            return [], set()

        built: List[Dict[str, Any]] = []
        found_raw_ids: set = set()
        for start_idx in range(0, len(clean_ids), 50):
            batch_ids = clean_ids[start_idx : start_idx + 50]
            try:
                resp = self._arctic_shift_get(
                    "posts/ids",
                    params={"ids": ",".join(batch_ids)},
                    timeout=12,
                )
                if resp is None or resp.status_code != 200:
                    continue
                for meta in (resp.json().get("data") or []):
                    if not isinstance(meta, dict) or not meta.get("id"):
                        continue
                    pid = str(meta["id"]).strip()
                    found_raw_ids.add(pid)
                    post_obj = self._build_post_from_reddit_dict(meta, min_publish_ts=min_publish_ts)
                    if post_obj:
                        built.append(post_obj)
            except Exception as e:
                logger.debug(f"[Reddit适配器] 按ID分批构建帖子跳过: {e}")
        return built, found_raw_ids

    def _fetch_reddit_post_and_comments_json(
        self,
        subreddit: str,
        base36_id: str,
        max_comments: int = 10,
        min_publish_ts: int = 0,
    ) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        调用已验证 `HTTP 200` 的单帖详情接口 `/r/{sub}/comments/{id}.json`：
        一次请求同时拿回「实时主帖完整 `t3` 数据（解决突发新帖尚未入镜像库的问题）+ 实时前排高赞 `t1` 神评」。
        """
        clean_id = _extract_reddit_base36_id(base36_id)
        if not clean_id or not self.is_burner_usable:
            return None, []
        clean_sub = _strip_reddit_prefix(subreddit)
        url = (
            f"https://www.reddit.com/r/{clean_sub}/comments/{clean_id}.json"
            if clean_sub and clean_sub.lower() != "reddit"
            else f"https://www.reddit.com/comments/{clean_id}.json"
        )
        parent_pid = f"reddit_{clean_id}"
        try:
            resp = self._reddit_cffi_get(
                url,
                params={"sort": "top", "limit": max(15, max_comments * 2), "raw_json": 1},
                timeout=12,
            )
            if resp.status_code in (401, 403):
                self._mark_burner_cooldown(f"HTTP {resp.status_code} on post_detail_json", channel="post_detail_json")
                return None, []
            if resp.status_code != 200:
                return None, []

            self._mark_burner_healthy()
            payload = resp.json()
            if not isinstance(payload, list) or not payload:
                return None, []

            post_item: Optional[Dict[str, Any]] = None
            first_listing = (payload[0].get("data") or {}).get("children") or []
            for child in first_listing:
                if isinstance(child, dict) and child.get("kind") == "t3" and isinstance(child.get("data"), dict):
                    post_item = self._build_post_from_reddit_dict(child["data"], min_publish_ts=min_publish_ts)
                    break

            parsed_comments: List[Dict[str, Any]] = []
            if len(payload) >= 2 and max_comments > 0:
                cmt_children = (payload[1].get("data") or {}).get("children") or []
                for c_node in cmt_children:
                    if not isinstance(c_node, dict) or c_node.get("kind") != "t1":
                        continue
                    d = c_node.get("data") or {}
                    author = _strip_reddit_prefix(str(d.get("author") or ""))
                    # 【保留过滤原因说明】跳过版块管理机器人 (`AutoModerator` / `VisualMod`) 自动置顶声明与已删空评论，
                    # 防止千篇一律的版规模板话术霸占前排高赞评论名额。
                    if not author or author.lower() in ("automoderator", "visualmod", "[deleted]", "none"):
                        continue
                    body = clean_post_text(html.unescape(str(d.get("body") or "")))
                    if not body or body.lower() in ("[deleted]", "[removed]"):
                        continue
                    c_imgs = _extract_urls_from_plain_text(body)
                    parsed_comments.append({
                        "reply_id": str(d.get("id") or ""),
                        "parent_id": parent_pid,
                        "create_time": safe_int(d.get("created_utc"), int(time.time())),
                        "author_uid": author,
                        "author_name": author,
                        "content": body,
                        "likes": safe_int(d.get("score"), 0),
                        "images": c_imgs,
                    })
                parsed_comments.sort(key=lambda x: x["likes"], reverse=True)

            norm_cmts = (
                self.normalize_comments(parsed_comments[:max_comments], parent_post_id=parent_pid)
                if parsed_comments
                else []
            )
            if post_item is not None and norm_cmts:
                post_item["comments"] = norm_cmts
                post_item["engagement"]["comment_count"] = max(
                    safe_int(post_item["engagement"].get("comment_count"), 0), len(norm_cmts)
                )
            return post_item, norm_cmts
        except Exception as e:
            logger.debug(f"[Reddit适配器] 单帖 JSON {clean_id} 异常: {e}")
            return None, []

    def _search_reddit_via_shreddit(
        self,
        keywords: Sequence[str],
        time_filter: str = "week",
        per_kw_target: int = 15,
    ) -> List[Tuple[str, str]]:
        """
        调用已验证 `HTTP 200`（配额 600 次/10分钟）的 Reddit 官方全站搜索组件接口：
        `GET https://www.reddit.com/svc/shreddit/search/?q={kw}&type=posts&t={time_filter}&sort={top|relevance|new}`
        返回去重后的 `[(subreddit, base36_post_id), ...]`。
        """
        if not self.is_burner_usable:
            return []

        discovered: List[Tuple[str, str]] = []
        seen_ids = set()
        blocked = False

        for kw in list(keywords or []):
            if blocked:
                break
            kw_str = str(kw or "").strip()
            if not kw_str:
                continue
            kw_hits = 0
            for sort_mode in ("top", "relevance", "new"):
                if sort_mode == "new" and kw_hits >= max(6, per_kw_target // 2):
                    continue
                try:
                    resp = self._reddit_cffi_get(
                        "https://www.reddit.com/svc/shreddit/search/",
                        params={
                            "q": kw_str,
                            "type": "posts",
                            "t": time_filter,
                            "sort": sort_mode,
                        },
                        accept_html=True,
                        timeout=12,
                    )
                    if resp.status_code in (401, 403):
                        blocked = True
                        self._mark_burner_cooldown(
                            f"HTTP {resp.status_code} on /svc/shreddit/search/",
                            channel="shreddit_search",
                        )
                        break
                    if resp.status_code == 429:
                        self.record_health_issue(
                            code="REDDIT_SHREDDIT_RATE_LIMITED",
                            message="Reddit `/svc/shreddit/search/` 触发 HTTP 429 频率限制",
                            level="warning",
                            channel="shreddit_search",
                        )
                        break
                    if resp.status_code != 200:
                        self.record_health_issue(
                            code="REDDIT_SHREDDIT_HTTP_ERROR",
                            message=f"Reddit `/svc/shreddit/search/` 返回 HTTP {resp.status_code}",
                            level="warning",
                            channel="shreddit_search",
                        )
                        continue

                    self._mark_burner_healthy()
                    html_body = resp.text or ""
                    for sub, pid in re.findall(r"/r/([A-Za-z0-9_]+)/comments/([a-z0-9]{5,9})/", html_body):
                        if pid not in seen_ids:
                            seen_ids.add(pid)
                            discovered.append((sub, pid))
                            kw_hits += 1
                    if kw_hits >= per_kw_target:
                        break
                except Exception as e:
                    self.record_health_issue(
                        code="REDDIT_SHREDDIT_EXCEPTION",
                        message=f"Reddit shreddit 搜索关键词 {kw_str!r} 异常: {e}",
                        level="warning",
                        channel="shreddit_search",
                    )
                    break

        return discovered

    def _discover_reddit_ids_via_yahoo(
        self,
        keywords: Sequence[str],
        hours_back: int = 0,
    ) -> List[Tuple[str, str]]:
        """
        100% 免登全站搜索主干 A（无关键词个数截断）：
        并发检索 `Yahoo Search (site:reddit.com)` 的 24h (`btf=d`) 与 7d (`btf=w` 前两页 `b=1, 8`)，
        若冷门词无近期结果则自动兜底不限时间页与 Bing RSS，保障新帖优先的同时绝不漏抓。
        """
        search_terms: List[str] = []
        seen_terms = set()
        raw_kw_list = list(keywords or [])
        for raw_kw in raw_kw_list:
            kw = str(raw_kw or "").strip()
            if not kw:
                continue
            if kw.lower() not in seen_terms:
                seen_terms.add(kw.lower())
                search_terms.append(kw)
            parts = [p for p in kw.split() if p]
            if len(parts) >= 3 and len(raw_kw_list) <= 6:
                for short_kw in (" ".join(parts[:2]), " ".join(parts[-2:])):
                    if short_kw.lower() not in seen_terms:
                        seen_terms.add(short_kw.lower())
                        search_terms.append(short_kw)

        if not search_terms:
            return []

        def _fetch_text(target_url: str) -> str:
            if cffi_requests is not None:
                resp = cffi_requests.get(
                    target_url,
                    headers={"User-Agent": CHROME_UA, "Accept-Language": "en-US,en;q=0.9"},
                    impersonate="chrome",
                    proxies=self.proxies,
                    timeout=10,
                )
            else:
                resp = requests.get(
                    target_url,
                    headers={"User-Agent": CHROME_UA, "Accept-Language": "en-US,en;q=0.9"},
                    proxies=self.proxies,
                    timeout=10,
                )
            return unquote(resp.text or "") if resp.status_code == 200 else ""

        def _search_one_term(kw: str) -> List[Tuple[str, str]]:
            local_pairs: List[Tuple[str, str]] = []
            local_seen = set()
            q_enc = quote(f"site:reddit.com {kw}")
            yahoo_pages = [
                f"https://search.yahoo.com/search?p={q_enc}&btf=d",
                f"https://search.yahoo.com/search?p={q_enc}&btf=w",
                f"https://search.yahoo.com/search?p={q_enc}&btf=w&b=8",
                f"https://search.yahoo.com/search?p={q_enc}",
            ]

            for p_idx, u in enumerate(yahoo_pages):
                if p_idx >= 2 and len(local_pairs) >= 12:
                    break
                if p_idx == 3 and (len(local_pairs) > 0 or (hours_back and 0 < hours_back <= 168)):
                    break
                try:
                    decoded = _fetch_text(u)
                    if not decoded:
                        continue
                    for sub, rid in re.findall(r"reddit\.com/r/([A-Za-z0-9_]+)/comments/([a-z0-9]{5,9})", decoded):
                        if rid not in local_seen:
                            local_seen.add(rid)
                            local_pairs.append((sub, rid))
                except Exception:
                    continue

            if len(local_pairs) < 8:
                bing_url = f"https://www.bing.com/search?q={q_enc}&format=rss&count=30"
                try:
                    decoded_bing = _fetch_text(bing_url)
                    for sub, rid in re.findall(r"reddit\.com/r/([A-Za-z0-9_]+)/comments/([a-z0-9]{5,9})", decoded_bing):
                        if rid not in local_seen:
                            local_seen.add(rid)
                            local_pairs.append((sub, rid))
                except Exception:
                    pass
            return local_pairs

        discovered: List[Tuple[str, str]] = []
        seen_global = set()
        with ThreadPoolExecutor(max_workers=min(3, len(search_terms))) as pool:
            for pairs in pool.map(_search_one_term, search_terms):
                for sub, rid in pairs:
                    if rid not in seen_global:
                        seen_global.add(rid)
                        discovered.append((sub, rid))
        return discovered

    def _search_reddit_via_arctic_shift(
        self,
        keywords: Sequence[str],
        min_publish_ts: int = 0,
        subreddits: Optional[Sequence[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        100% 免登全站搜索主干 B（零账号）：
        受控并发调用公开镜像 `ArcticShift /api/posts/search?subreddit={sub}&title={kw}&sort=desc`，
        直接拿回带完整 `score`、`num_comments` 与原图相册的最新匹配帖子（`min_publish_ts<=0` 时不加时间下限，按时间倒序自然取最新）。
        """
        ascii_kws = [
            str(k).strip()
            for k in (keywords or [])
            if str(k).strip() and not _CJK_RE.search(str(k))
        ]
        if not ascii_kws:
            return []

        title_terms: List[str] = []
        for ak in ascii_kws:
            if ak.lower() not in {x.lower() for x in title_terms}:
                title_terms.append(ak)
            parts = [p for p in ak.split() if len(p) >= 2]
            if len(parts) >= 3 and len(ascii_kws) <= 4:
                short_entity = " ".join(parts[:2])
                if short_entity.lower() not in {x.lower() for x in title_terms}:
                    title_terms.append(short_entity)

        if subreddits:
            target_subs = [_strip_reddit_prefix(s) for s in subreddits if _strip_reddit_prefix(s)]
        else:
            joined_low = " ".join(ascii_kws).lower()
            if any(k in joined_low for k in ("btc", "bitcoin", "eth", "ethereum", "crypto", "sol", "binance", "coin")):
                target_subs = ["CryptoCurrency", "Bitcoin", "wallstreetbets", "FluentInFinance", "news"]
            elif any(k in joined_low for k in ("ai", "openai", "nvidia", "jensen", "gpt", "agent", "tech", "microsoft", "apple", "google")):
                target_subs = ["technology", "nvidia", "openai", "wallstreetbets", "Fauxmoi"]
            else:
                target_subs = ["news", "worldnews", "technology", "wallstreetbets", "Fauxmoi"]

        # 【保留配额保护说明】ArcticShift 公开镜像对单 IP 设有约 45 次/分钟的窗口限流；
        # 笛卡尔积任务按「每个关键词优先轮询各核心板块」排序并控制在 15 次请求以内，为后续 ID 水合与评论树拉取预留安全配额，防止触发 429 等待。
        tasks: List[Tuple[str, str]] = [
            (sub, kw) for kw in title_terms for sub in target_subs
        ][:15]
        found_by_id: Dict[str, Dict[str, Any]] = {}

        def _query_sub_title(pair: Tuple[str, str]) -> List[Dict[str, Any]]:
            sub_name, t_kw = pair
            out_items: List[Dict[str, Any]] = []
            req_params: Dict[str, Any] = {
                "subreddit": sub_name,
                "title": t_kw,
                "limit": 15,
                "sort": "desc",
            }
            if min_publish_ts > 0:
                req_params["after"] = min_publish_ts
            try:
                resp = self._arctic_shift_get(
                    "posts/search",
                    params=req_params,
                    timeout=10,
                )
                if resp is not None and resp.status_code == 200:
                    for meta in (resp.json().get("data") or []):
                        post_obj = self._build_post_from_reddit_dict(meta, min_publish_ts=min_publish_ts)
                        if post_obj and self.is_valuable_post(post_obj):
                            out_items.append(post_obj)
            except Exception:
                pass
            return out_items

        with ThreadPoolExecutor(max_workers=min(2, max(1, len(tasks)))) as pool:
            for item_list in pool.map(_query_sub_title, tasks):
                for p_obj in item_list:
                    found_by_id[p_obj["post_id"]] = p_obj

        return list(found_by_id.values())

    def _fetch_single_post_rss_fallback(
        self,
        subreddit: str,
        base36_id: str,
        min_publish_ts: int = 0,
    ) -> Optional[Dict[str, Any]]:
        """零账号模式下，对极少数刚发出尚未进入 ArcticShift 的突发新帖，在 RSS 令牌桶充裕时单条安全兜底还原。"""
        clean_id = _extract_reddit_base36_id(base36_id)
        if not clean_id or self._last_rss_rl_remaining < 1.0:
            return None
        clean_sub = _strip_reddit_prefix(subreddit)
        rss_url = (
            f"https://www.reddit.com/r/{clean_sub}/comments/{clean_id}/.rss?limit=12"
            if clean_sub and clean_sub.lower() != "reddit"
            else f"https://www.reddit.com/comments/{clean_id}/.rss?limit=12"
        )
        try:
            resp = self.session.get(rss_url, proxies=self.proxies, timeout=10)
            try:
                self._last_rss_rl_remaining = float(resp.headers.get("x-ratelimit-remaining", "0.0"))
            except ValueError:
                self._last_rss_rl_remaining = 0.0
            if resp.status_code != 200:
                return None
            entries = self._parse_atom_entries(resp.text)
            if not entries:
                return None
            post_item = entries[0]
            if min_publish_ts > 0 and safe_int(post_item.get("publish_time"), 0) < min_publish_ts:
                return None
            return post_item
        except Exception:
            return None

    def _attach_comments_concurrent(
        self,
        posts: List[Dict[str, Any]],
        top_n_comments: int = 25,
        max_comments_per_post: int = 10,
    ) -> None:
        if top_n_comments <= 0 or not posts:
            return
        # 【保留过滤与配额保护说明】
        # 1) 跳过 `comment_count == 0` 的无评论帖，避免空跑网络请求浪费耗时；
        # 2) 零账号模式下评论源 `ArcticShift` 有单 IP 约 45 次/分钟的窗口配额限制，因此免登态单轮最多为前 22 条热帖补评论，避免触发 429 罚站。
        eff_cap = top_n_comments if self.is_burner_usable else min(top_n_comments, 22)
        candidates = [
            p for p in posts
            if not (p.get("comments") or [])
            and safe_int((p.get("engagement") or {}).get("comment_count"), 0) > 0
        ]
        targets = candidates[:eff_cap]
        if not targets:
            return

        def _sub_hint_for(item: Dict[str, Any]) -> str:
            meta = item.get("metadata") or {}
            pextra = meta.get("platform_extra") or {}
            sub = str(meta.get("subreddit") or pextra.get("subreddit") or "")
            return _strip_reddit_prefix(sub)

        workers = min(5 if self.is_burner_usable else 3, len(targets))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            fut_map = {
                pool.submit(
                    self.fetch_post_comments,
                    item["post_id"],
                    max_comments=max_comments_per_post,
                    subreddit=_sub_hint_for(item),
                    allow_rss_fallback=False,
                ): item
                for item in targets
            }
            for fut in as_completed(fut_map):
                item = fut_map[fut]
                try:
                    comments = fut.result()
                    if comments:
                        item["comments"] = comments
                        item["engagement"]["comment_count"] = max(
                            safe_int(item["engagement"].get("comment_count"), 0), len(comments)
                        )
                except Exception:
                    pass

    # ==========================================================================
    # 四大通用原语实现
    # ==========================================================================
    def fetch_hot_or_recommended(
        self,
        count: int = 80,
        subreddits: Optional[List[str]] = None,
        sort: str = "top",
        time_filter: str = "day",
        include_popular: Optional[bool] = None,
        popular_limit: int = 50,
        subreddits_limit: int = 50,
        preserve_all_hot: bool = False,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        原语 1：全量拉取 Reddit 全站热门榜与垂直高信噪比板块爆款（零主观过滤，不混排截断）：
        - 主通道（小号健康可用）：分别拉满全站热榜 `/r/popular/{sort}.json` (`popular_limit`) + 15 大板块聚合 `/r/{subs}/{sort}.json` (`subreddits_limit`)；
        - 免登主干 / 自动降级通道（零账号或小号冷却）：分别拉取 `/r/popular/{sort}/.rss` + `/r/{subs}/{sort}/.rss`（带 180s 缓存严控 2次/60s 令牌桶）+ `ArcticShift` 批量富化赞评数与原图相册。
        """
        subs = subreddits or DEFAULT_GOLDMINE_SUBREDDITS
        combined = "+".join(_strip_reddit_prefix(s) for s in subs if _strip_reddit_prefix(s))
        want_popular = (subreddits is None) if include_popular is None else bool(include_popular)
        eff_pop_lim = max(1, min(100, safe_int(popular_limit, max(count, 50))))
        eff_sub_lim = max(1, min(100, safe_int(subreddits_limit, max(count, 50))))

        def _rank_posts(items_iter) -> List[Dict[str, Any]]:
            return sorted(
                items_iter,
                key=lambda p: (
                    safe_int((p.get("engagement") or {}).get("like_count"), 0)
                    + safe_int((p.get("engagement") or {}).get("comment_count"), 0) * 2,
                    safe_int(p.get("publish_time"), 0),
                ),
                reverse=True,
            )

        if self.is_burner_usable:
            by_id: Dict[str, Dict[str, Any]] = {}
            json_endpoints: List[Tuple[str, str, int]] = []
            if want_popular:
                json_endpoints.append(("r/popular全站热榜", f"https://www.reddit.com/r/popular/{sort}.json", eff_pop_lim))
            if combined:
                json_endpoints.append(("黄金垂直板块聚合", f"https://www.reddit.com/r/{combined}/{sort}.json", eff_sub_lim))

            auth_blocked = False
            for ch_label, j_url, lim in json_endpoints:
                try:
                    resp = self._reddit_cffi_get(
                        j_url,
                        params={"t": time_filter, "limit": lim, "raw_json": 1},
                        timeout=REQUEST_TIMEOUT,
                    )
                    if resp.status_code in (401, 403):
                        auth_blocked = True
                        self._mark_burner_cooldown(
                            f"HTTP {resp.status_code} on {ch_label}",
                            channel=ch_label,
                        )
                        break
                    if resp.status_code == 429:
                        self.record_health_issue(
                            code="REDDIT_JSON_RATE_LIMITED",
                            message=f"Reddit {ch_label} 触发 HTTP 429 频率限制",
                            level="warning",
                            channel=ch_label,
                        )
                        continue
                    if resp.status_code != 200:
                        self.record_health_issue(
                            code="REDDIT_JSON_HTTP_ERROR",
                            message=f"Reddit {ch_label} 返回 HTTP {resp.status_code}",
                            level="warning",
                            channel=ch_label,
                        )
                        continue

                    children = ((resp.json() or {}).get("data") or {}).get("children") or []
                    parsed_cnt = 0
                    for child in children:
                        if not isinstance(child, dict) or child.get("kind") != "t3":
                            continue
                        d = child.get("data") or {}
                        # 【保留过滤原因说明】跳过版块置顶公告帖 (`stickied=True`)：
                        # 版块顶部常年挂着固定不变的版规说明或每日水楼导航帖，非真实热点，跳过以节省评论与外链请求耗时。
                        if sort != "new" and d.get("stickied"):
                            continue
                        p_obj = self._build_post_from_reddit_dict(d)
                        if p_obj and self.is_valuable_post(p_obj):
                            by_id[p_obj["post_id"]] = p_obj
                            parsed_cnt += 1
                    if parsed_cnt == 0:
                        self.record_health_issue(
                            code="REDDIT_JSON_EMPTY_PARSE",
                            message=f"Reddit {ch_label} 返回 HTTP 200 但解析出 0 条有效帖子，疑似 JSON 结构变化",
                            level="warning",
                            channel=ch_label,
                        )
                except Exception as e:
                    self.record_health_issue(
                        code="REDDIT_JSON_EXCEPTION",
                        message=f"Reddit {ch_label} 请求异常: {e}",
                        level="warning",
                        channel=ch_label,
                    )

            if by_id and not auth_blocked:
                self._mark_burner_healthy()
                ranked = _rank_posts(by_id.values())
                return ranked if preserve_all_hot else ranked[:count]

        # 100% 免登热榜通道（严格遵循 Reddit 免登 RSS 每分钟 1 次令牌桶：单次 limit=100 拉满全站热榜，视 x-ratelimit-reset 智能拉取第二源或由 ArcticShift 补齐垂直板块）
        rss_pop_lim = 100 if preserve_all_hot else max(eff_pop_lim, 75)
        rss_sub_lim = 100 if preserve_all_hot else max(eff_sub_lim, 75)

        merged_rss_by_id: Dict[str, Dict[str, Any]] = {}
        fresh_un_enriched: List[Dict[str, Any]] = []
        fresh_cache_buckets: List[Tuple[str, List[Dict[str, Any]]]] = []
        rss_errors: List[str] = []

        def _consume_rss_feed(ch_label: str, cache_key: str, rss_url: str) -> Tuple[bool, float]:
            now_mono = time.monotonic()
            cached = self._rss_feed_cache.get(cache_key)
            if cached and (now_mono - cached[0]) < 180.0:
                for cp in cached[1]:
                    if cp.get("post_id") and cp["post_id"] not in merged_rss_by_id:
                        merged_rss_by_id[cp["post_id"]] = cp
                return True, 0.0

            try:
                resp = self.session.get(rss_url, proxies=self.proxies, timeout=REQUEST_TIMEOUT)
                try:
                    self._last_rss_rl_remaining = float(resp.headers.get("x-ratelimit-remaining", "0.0"))
                except ValueError:
                    self._last_rss_rl_remaining = 0.0
                try:
                    rl_reset_s = float(resp.headers.get("x-ratelimit-reset", "60.0"))
                except ValueError:
                    rl_reset_s = 60.0

                if resp.status_code != 200:
                    rss_errors.append(f"{ch_label} HTTP {resp.status_code}")
                    return False, rl_reset_s

                parsed_entries = [p for p in self._parse_atom_entries(resp.text) if self.is_valuable_post(p)]
                fresh_cache_buckets.append((cache_key, parsed_entries))
                for p_obj in parsed_entries:
                    pid = p_obj["post_id"]
                    if pid not in merged_rss_by_id:
                        merged_rss_by_id[pid] = p_obj
                        fresh_un_enriched.append(p_obj)
                return True, rl_reset_s
            except Exception as e:
                rss_errors.append(f"{ch_label} 异常: {e}")
                return False, 60.0

        pop_reset_s = 0.0
        if want_popular:
            _, pop_reset_s = _consume_rss_feed(
                "r/popular全站热榜RSS",
                f"popular|{sort}|{time_filter}|{rss_pop_lim}",
                f"https://www.reddit.com/r/popular/{sort}/.rss?t={time_filter}&limit={rss_pop_lim}",
            )

        if combined:
            sub_cache_key = f"{combined}|{sort}|{time_filter}|{rss_sub_lim}"
            sub_cached = self._rss_feed_cache.get(sub_cache_key)
            if not want_popular or (sub_cached and (time.monotonic() - sub_cached[0]) < 180.0):
                _consume_rss_feed(
                    "黄金垂直板块聚合RSS",
                    sub_cache_key,
                    f"https://www.reddit.com/r/{combined}/{sort}/.rss?t={time_filter}&limit={rss_sub_lim}",
                )
            elif self._last_rss_rl_remaining >= 1.0 or (0.0 < pop_reset_s <= 18.0):
                if self._last_rss_rl_remaining < 1.0:
                    time.sleep(pop_reset_s + 1.2)
                _consume_rss_feed(
                    "黄金垂直板块聚合RSS",
                    sub_cache_key,
                    f"https://www.reddit.com/r/{combined}/{sort}/.rss?t={time_filter}&limit={rss_sub_lim}",
                )
            else:
                # 当前分钟 RSS 令牌已用于拉满 100 条 r/popular，直接用 ArcticShift 零消耗按时间倒序补齐核心垂直板块最新帖（不做硬性时间丢弃）
                supplement_subs = [s for s in ("nvidia", "openai", "CryptoCurrency", "Bitcoin", "technology") if s.lower() in combined.lower()]
                for v_sub in supplement_subs[:4]:
                    resp_v = self._arctic_shift_get(
                        "posts/search",
                        params={"subreddit": v_sub, "limit": 12, "sort": "desc"},
                        timeout=10,
                    )
                    if resp_v is not None and resp_v.status_code == 200:
                        for meta in (resp_v.json().get("data") or []):
                            vp = self._build_post_from_reddit_dict(meta)
                            if vp and self.is_valuable_post(vp) and vp["post_id"] not in merged_rss_by_id:
                                merged_rss_by_id[vp["post_id"]] = vp

        if fresh_un_enriched:
            self._enrich_posts_via_arctic_shift(fresh_un_enriched)
            save_mono = time.monotonic()
            for cache_key, bucket_entries in fresh_cache_buckets:
                enriched_bucket = [merged_rss_by_id.get(p["post_id"], p) for p in bucket_entries]
                self._rss_feed_cache[cache_key] = (save_mono, enriched_bucket)

        if merged_rss_by_id:
            if rss_errors:
                self.record_health_issue(
                    code="REDDIT_RSS_PARTIAL_DEGRADED",
                    message=f"Reddit 部分免登 RSS 子通道波动 ({'; '.join(rss_errors)})，其余热榜通道正常返回 {len(merged_rss_by_id)} 条",
                    level="warning",
                    channel="rss_fallback",
                )
            ranked_rss = _rank_posts(merged_rss_by_id.values())
            return ranked_rss if preserve_all_hot else ranked_rss[:count]

        self.record_health_issue(
            code="REDDIT_RSS_HTTP_ERROR",
            message=f"Reddit 免登热榜 RSS 失败: {'; '.join(rss_errors) or '返回 0 条'}",
            level="critical",
            channel="rss_fallback",
        )
        return []

    def fetch_user_posts(self, user_id: str, count: int = 15, **kwargs) -> List[Dict[str, Any]]:
        """原语 2：拉取指定 Reddit 用户 (`/u/username`) 发布的帖子"""
        clean_user = _strip_reddit_prefix(user_id)
        if not clean_user:
            return []
        if self.is_burner_usable:
            try:
                resp = self._reddit_cffi_get(
                    f"https://www.reddit.com/user/{clean_user}/submitted.json",
                    params={"limit": max(count, 20), "raw_json": 1},
                )
                if resp.status_code in (401, 403):
                    self._mark_burner_cooldown(f"HTTP {resp.status_code} on user_submitted_json", channel="user_json")
                elif resp.status_code == 200:
                    self._mark_burner_healthy()
                    children = ((resp.json() or {}).get("data") or {}).get("children") or []
                    out = []
                    for ch in children:
                        if isinstance(ch, dict) and ch.get("kind") == "t3":
                            p_obj = self._build_post_from_reddit_dict(ch.get("data") or {})
                            if p_obj:
                                out.append(p_obj)
                    if out:
                        return out[:count]
            except Exception:
                pass

        url = f"https://www.reddit.com/user/{clean_user}/submitted/.rss?limit={count}"
        try:
            resp = self.session.get(url, proxies=self.proxies, timeout=REQUEST_TIMEOUT)
            if resp.status_code != 200:
                return []
            posts = self._parse_atom_entries(resp.text)[:count]
            self._enrich_posts_via_arctic_shift(posts)
            return posts
        except Exception as e:
            logger.warning(f"[Reddit适配器] 拉取用户 u/{clean_user} 异常: {e}")
            return []

    def search_or_topic_posts(
        self,
        query: str,
        count: int = 20,
        subreddits: Optional[List[str]] = None,
        time_filter: str = "",
        hours_back: int = 0,
        fetch_comments: bool = False,
        fetch_external_links: bool = False,
        download_media: bool = False,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        原语 3：按关键词全站精准检索 Reddit 高价值讨论帖（100% 零账号高召回主干 + 小号可选加速）：
        1) 自动将中英混合词/中文常见实体映射为英文优先查询词（无关键词个数截断，默认 `hours_back=0` 不做时间硬删）；
        2) 若小号健康可用：调用 `/svc/shreddit/search/` 全站实时检索；
        3) 100% 免登双引擎主干（零账号或结果不足时自动全开）：
           - 引擎 A：并发检索 `Yahoo Search (site:reddit.com)` 24h (`btf=d`) + 7d (`btf=w` 翻页) + Bing RSS；
           - 引擎 B：并发检索公开镜像 `ArcticShift /api/posts/search?subreddit={sub}&title={kw}&sort=desc`；
        4) 批量经 `ArcticShift /api/posts/ids` 还原完整互动量与原图相册；若小号在线则补齐刚发出的突发新帖。
        """
        reset_health = bool(kwargs.pop("_reset_health", True))
        if reset_health:
            self.reset_health_state()
            self.last_search_note = ""

        raw_q = str(query or "").replace(",", "|")
        raw_keywords = [w.strip() for w in raw_q.split("|") if w.strip()]
        prepared_keywords = _prepare_reddit_search_keywords(raw_keywords)
        now_ts = int(time.time())
        min_ts = int(now_ts - max(1, hours_back) * 3600) if (hours_back and hours_back > 0) else 0
        recent_priority_ts = int(now_ts - 7 * 86400)

        if not time_filter:
            if hours_back and 0 < hours_back <= 24:
                eff_t = "day"
            elif not hours_back or hours_back <= 168:
                eff_t = "week"
            elif hours_back <= 744:
                eff_t = "month"
            else:
                eff_t = "year"
        else:
            eff_t = time_filter

        if not prepared_keywords:
            return self.fetch_hot_or_recommended(
                count=count,
                subreddits=subreddits,
                sort="top",
                time_filter=eff_t,
            )

        sub_filter_set = (
            {_strip_reddit_prefix(s).lower() for s in subreddits if _strip_reddit_prefix(s)}
            if subreddits
            else set()
        )

        pair_map: Dict[str, str] = {}  # pid -> sub
        by_id: Dict[str, Dict[str, Any]] = {}
        shreddit_pair_count = 0
        tried_shreddit = False

        # 通道 1（可选小号加速）：Reddit 官方 /svc/shreddit/search/ 全站搜索
        if self.is_burner_usable:
            tried_shreddit = True
            sh_pairs = self._search_reddit_via_shreddit(
                keywords=prepared_keywords,
                time_filter=eff_t,
                per_kw_target=max(12, count),
            )
            for sub, pid in sh_pairs:
                if sub_filter_set and sub.lower() not in sub_filter_set:
                    continue
                if pid not in pair_map:
                    pair_map[pid] = sub
            shreddit_pair_count = len(pair_map)

        # 通道 2（100% 免登双引擎主干）：Yahoo 24h+7d 索引发现 + ArcticShift 直搜
        yahoo_pair_count = 0
        arctic_direct_count = 0
        if len(pair_map) < max(12, count):
            yh_pairs = self._discover_reddit_ids_via_yahoo(prepared_keywords, hours_back=hours_back)
            for sub, pid in yh_pairs:
                if sub_filter_set and sub.lower() not in sub_filter_set:
                    continue
                if pid not in pair_map:
                    pair_map[pid] = sub
                    yahoo_pair_count += 1

            arctic_direct_posts = self._search_reddit_via_arctic_shift(
                prepared_keywords,
                min_publish_ts=min_ts,
                subreddits=subreddits,
            )
            for ap in arctic_direct_posts:
                if self.is_valuable_post(ap) and ap["post_id"] not in by_id:
                    by_id[ap["post_id"]] = ap
                    arctic_direct_count += 1

        realtime_hydrated = 0
        if pair_map:
            pending_pids = [pid for pid in pair_map.keys() if f"reddit_{pid}" not in by_id]
            hit_raw_ids: set = set()
            if pending_pids:
                arctic_posts, hit_raw_ids = self._build_posts_from_arctic_shift_ids(
                    pending_pids,
                    min_publish_ts=min_ts,
                )
                for ap in arctic_posts:
                    if self.is_valuable_post(ap):
                        by_id[ap["post_id"]] = ap

            # 对刚发布尚未进入 ArcticShift 镜像库的实时新帖：小号在线走 .json，免登且结果偏少时安全兜底 Top 1
            missing_pairs = [(pair_map[pid], pid) for pid in pending_pids if pid not in hit_raw_ids][:12]
            if missing_pairs and self.is_burner_usable:
                max_cmts_inline = 10 if fetch_comments else 5
                with ThreadPoolExecutor(max_workers=min(4, len(missing_pairs))) as pool:
                    futs = [
                        pool.submit(
                            self._fetch_reddit_post_and_comments_json,
                            sub,
                            pid,
                            max_cmts_inline,
                            min_ts,
                        )
                        for sub, pid in missing_pairs
                    ]
                    for fut in as_completed(futs):
                        try:
                            rt_post, _ = fut.result()
                            if rt_post and self.is_valuable_post(rt_post):
                                by_id[rt_post["post_id"]] = rt_post
                                realtime_hydrated += 1
                        except Exception:
                            pass
            elif (
                missing_pairs
                and not self.is_burner_usable
                and len(by_id) < max(5, count // 2)
                and self._last_rss_rl_remaining >= 1.0
            ):
                rss_salvaged = self._fetch_single_post_rss_fallback(
                    subreddit=missing_pairs[0][0],
                    base36_id=missing_pairs[0][1],
                    min_publish_ts=min_ts,
                )
                if rss_salvaged and self.is_valuable_post(rss_salvaged):
                    by_id[rss_salvaged["post_id"]] = rss_salvaged
                    realtime_hydrated += 1

        if tried_shreddit and self.is_burner_usable and shreddit_pair_count == 0 and len(by_id) > 0:
            self.record_health_issue(
                code="REDDIT_SHREDDIT_DOM_OR_EMPTY",
                message=(
                    f"Reddit `/svc/shreddit/search/` 对 {prepared_keywords[:3]} 返回 0 条，但免登双引擎发现了 {len(by_id)} 条近期帖！"
                    f"请留意 shreddit 搜索 HTML 结构是否改版"
                ),
                level="warning",
                channel="shreddit_search",
            )

        results = sorted(
            by_id.values(),
            key=lambda p: (
                1 if safe_int(p.get("publish_time"), 0) >= recent_priority_ts else 0,
                safe_int((p.get("engagement") or {}).get("like_count"), 0)
                + safe_int((p.get("engagement") or {}).get("comment_count"), 0) * 3,
                safe_int(p.get("publish_time"), 0),
            ),
            reverse=True,
        )[:count]

        self.last_search_note = (
            f"检索词={prepared_keywords} | shreddit发现={shreddit_pair_count}, "
            f"Yahoo索引={yahoo_pair_count}, ArcticShift直搜={arctic_direct_count}, "
            f"实时补齐={realtime_hydrated} -> 保留={len(results)}条"
        )

        if fetch_comments and results:
            self._attach_comments_concurrent(results, top_n_comments=len(results), max_comments_per_post=10)
        if fetch_external_links and results:
            enrich_posts_with_external_articles(results, proxies=self.proxies)
        if download_media or fetch_external_links or fetch_comments:
            results = self.normalize_batch(results, download_media=download_media)

        if reset_health:
            self.audit_posts_quality(
                results,
                scene="search",
                expect_comments=fetch_comments,
                expect_media_download=download_media,
                max_fresh_age_hours=max(168, (hours_back or 168) * 2),
            )
            results = [p for p in results if self.is_valuable_post(p)]
        return results

    def fetch_post_comments(
        self,
        post_id: str,
        max_comments: int = 10,
        allow_rss_fallback: bool = True,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        原语 4：提取指定 Reddit 帖子的前排高赞神评：
        1) 通道 A（零账号零限流主力）：公开镜像 `ArcticShift /comments/tree`；
        2) 通道 B：若为刚发布的突发新帖（镜像尚未收录）且小号健康可用，直连 `/r/{sub}/comments/{id}.json` 拿实时高赞评论；
        3) 通道 C：仅在单帖独立调用 (`allow_rss_fallback=True`) 且免登 RSS 令牌桶充裕时回退 `.rss`，绝不在并发批处理中触碰令牌桶。
        """
        raw_target = str(post_id or "").strip()
        clean_id = _extract_reddit_base36_id(raw_target)
        if not clean_id:
            return []

        parent_pid = f"reddit_{clean_id}"
        sub_hint = _strip_reddit_prefix(str(kwargs.get("subreddit") or ""))
        if not sub_hint and "/r/" in raw_target:
            m_sub = re.search(r"/r/([A-Za-z0-9_]+)/", raw_target)
            if m_sub:
                sub_hint = m_sub.group(1)

        # 通道 A：公开镜像评论树（100% 免账号，自带配额平滑重试）
        try:
            resp = self._arctic_shift_get(
                "comments/tree",
                params={"link_id": clean_id, "limit": max(25, max_comments * 3)},
                timeout=10,
            )
            if resp is not None and resp.status_code == 200:
                nodes = resp.json().get("data") or []
                parsed_comments: List[Dict[str, Any]] = []
                for node in nodes:
                    if not isinstance(node, dict):
                        continue
                    d = node.get("data") if isinstance(node.get("data"), dict) else node
                    author = _strip_reddit_prefix(str(d.get("author") or ""))
                    # 【保留过滤原因说明】跳过版块管理机器人 (`AutoModerator` / `VisualMod`) 自动声明及已被删除 (`[deleted]`/`[removed]`) 的残缺空评论，
                    # 防止版规套话和无效占位符占据每帖有限的高赞评论名额。
                    if not author or author.lower() in ("automoderator", "visualmod", "[deleted]", "none"):
                        continue
                    body = clean_post_text(html.unescape(str(d.get("body") or "")))
                    if not body or body.lower() in ("[deleted]", "[removed]"):
                        continue
                    c_imgs = _extract_urls_from_plain_text(body)
                    parsed_comments.append({
                        "reply_id": str(d.get("id") or ""),
                        "parent_id": parent_pid,
                        "create_time": safe_int(d.get("created_utc"), int(time.time())),
                        "author_uid": author,
                        "author_name": author,
                        "content": body,
                        "likes": safe_int(d.get("score"), 0),
                        "images": c_imgs,
                    })
                if parsed_comments:
                    parsed_comments.sort(key=lambda x: x["likes"], reverse=True)
                    return self.normalize_comments(parsed_comments[:max_comments], parent_post_id=parent_pid)
        except Exception as e:
            logger.debug(f"[Reddit适配器] Arctic Shift 评论树 {clean_id} 回退: {e}")

        # 通道 B：小号在线时直接请求单帖详情 `.json` 拿实时热评
        if self.is_burner_usable:
            _, live_cmts = self._fetch_reddit_post_and_comments_json(
                subreddit=sub_hint,
                base36_id=clean_id,
                max_comments=max_comments,
            )
            if live_cmts:
                return live_cmts

        # 通道 C：仅允许单条显式调用且免登 RSS 令牌桶充裕时回退 `.rss`
        if not allow_rss_fallback or self._last_rss_rl_remaining < 1.0:
            return []
        rss_url = (
            raw_target.rstrip("/") + f"/.rss?limit={max_comments + 4}"
            if raw_target.startswith("http")
            else f"https://www.reddit.com/comments/{clean_id}/.rss?limit={max_comments + 4}"
        )
        try:
            resp = self.session.get(rss_url, proxies=self.proxies, timeout=12)
            try:
                self._last_rss_rl_remaining = float(resp.headers.get("x-ratelimit-remaining", "0.0"))
            except ValueError:
                self._last_rss_rl_remaining = 0.0
            if resp.status_code != 200:
                return []
            root = ET.fromstring(resp.text)
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            entries = root.findall("atom:entry", ns)
            raw_comments = []
            for ent in entries[1:]:
                c_id = (ent.find("atom:id", ns).text or "").replace("t1_", "") if ent.find("atom:id", ns) is not None else ""
                author_el = ent.find("atom:author/atom:name", ns)
                author = _strip_reddit_prefix(author_el.text or "") if author_el is not None else "redditor"
                # 【保留过滤原因说明】RSS 兜底评论流同样剔除版块管理机器人固定套话。
                if author.lower() in ("automoderator", "visualmod"):
                    continue
                updated_str = ent.find("atom:updated", ns).text if ent.find("atom:updated", ns) is not None else ""
                content_el = ent.find("atom:content", ns)
                raw_html = html.unescape(content_el.text or "") if content_el is not None else ""
                txt, c_imgs, _ = _parse_reddit_html(raw_html)
                if not txt and not c_imgs:
                    continue
                raw_comments.append({
                    "reply_id": c_id,
                    "parent_id": parent_pid,
                    "create_time": normalize_publish_timestamp(updated_str),
                    "author_uid": author,
                    "author_name": author,
                    "content": txt,
                    "likes": 0,
                    "images": c_imgs,
                })
                if len(raw_comments) >= max_comments:
                    break
            return self.normalize_comments(raw_comments, parent_post_id=parent_pid)
        except Exception as e:
            logger.debug(f"[Reddit适配器] 拉取评论 {rss_url} 异常: {e}")
            return []

    # ==========================================================================
    # 标准调度入口：供上层定时任务调用（全站热榜 r/popular + 15 大黄金板块全量拉取 + 数据体检）
    # ==========================================================================
    def harvest(self, config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        self.reset_health_state()
        cfg = config or {}
        subreddits = cfg.get("subreddits") or DEFAULT_GOLDMINE_SUBREDDITS
        sort = cfg.get("sort", "top")
        time_filter = cfg.get("time_filter", "day")
        limit = safe_int(cfg.get("limit"), 80)
        popular_limit = safe_int(cfg.get("popular_limit"), 50)
        subreddits_limit = safe_int(cfg.get("subreddits_limit"), 50)
        preserve_all_hot = bool(cfg.get("preserve_all_hot", True))
        include_popular = bool(cfg.get("include_popular", True))
        top_n_comments = safe_int(cfg.get("fetch_comments_for_top_n"), 40)
        fetch_ext_links = bool(cfg.get("fetch_external_links", True))
        keywords = cfg.get("keywords")
        download_media = bool(cfg.get("download_media", True))

        self.last_channel_stats = {}
        if keywords:
            query_str = "|".join(keywords) if isinstance(keywords, list) else str(keywords)
            search_subs = cfg.get("subreddits") if cfg.get("restrict_search_to_subreddits") else None
            posts = self.search_or_topic_posts(
                query=query_str,
                count=limit,
                subreddits=search_subs,
                time_filter=time_filter,
                _reset_health=False,
            )
        else:
            posts = self.fetch_hot_or_recommended(
                count=limit,
                subreddits=subreddits,
                sort=sort,
                time_filter=time_filter,
                include_popular=include_popular,
                popular_limit=popular_limit,
                subreddits_limit=subreddits_limit,
                preserve_all_hot=preserve_all_hot,
            )

        valuable_posts = [p for p in posts if self.is_valuable_post(p, cfg)]
        for p in valuable_posts:
            meta = p.get("metadata") or {}
            pextra = meta.get("platform_extra") or {}
            sub = str(meta.get("subreddit") or pextra.get("subreddit") or "r/reddit")
            self.last_channel_stats[sub] = self.last_channel_stats.get(sub, 0) + 1

        self._attach_comments_concurrent(valuable_posts, top_n_comments=top_n_comments, max_comments_per_post=10)
        if fetch_ext_links and valuable_posts:
            ext_cnt = enrich_posts_with_external_articles(valuable_posts, proxies=self.proxies)
            if ext_cnt > 0:
                self.last_channel_stats["外链正文富化"] = ext_cnt

        normalized_posts = self.normalize_batch(valuable_posts, download_media=download_media)
        self.audit_posts_quality(
            normalized_posts,
            scene="harvest",
            expect_comments=(top_n_comments > 0),
            expect_media_download=download_media,
            max_fresh_age_hours=168,
        )
        return [p for p in normalized_posts if self.is_valuable_post(p, cfg)]
