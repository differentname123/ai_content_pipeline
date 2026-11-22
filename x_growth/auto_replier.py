# -*- coding: utf-8 -*-
"""
X (Twitter) 帖子评论全自动回复系统 (`x_growth/auto_replier.py`)

极简、高内聚、低耦合的三模块独立并行架构（零命令行参数，顶部直接配置，默认使用 `x_myself`）：
- 【模块 1 · 收集评论 (`collect_replies_for_account`)】：
  使用指定浏览器会话（含登录态）巡检通知提及页 + 本人主页最近主帖 + 有变化/新发现的帖子详情页，
  将他人对我方帖子/回复的评论连同上下文（原帖内容、上一层回复、评论正文）幂等存入 `x_reply_inbox`，
  同时识别线上已回复记录，绝不重复、绝不漏抓。
- 【模块 2 · AI 生成回复 (`generate_replies_from_db`)】：
  纯查 `x_reply_inbox` 中 `status="pending_generate"` 的待处理评论（零浏览器耦合、纯文本不传图），
  通过 `common/llm_gateway.py` 调用普通模型组生成克制、自然、安全的回复并更新为 `status="ready_to_reply"`。
- 【模块 3 · 精准执行回复 (`publish_replies_for_account`)】：
  扫描 `x_reply_inbox` 中 `status="ready_to_reply"` 的已生成回复，打开真实浏览器导航至对应评论页，
  二次实时查重确认线上未回复后精准点击回复并落库标记 `status="replied"`，做到不重复、不遗漏。
- 【并行调度 (`run_parallel_daemon`)】：
  三个模块各自在独立线程中并行定时运行（默认每 1 小时一次），只通过 MongoDB 状态流转协同；
  模块 1 与模块 3 共用同一浏览器目录时自动由底层 `persistent_browser_session` 跨进程/跨线程锁安全排队。
"""
from collections import Counter
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import os
from pathlib import Path
import random
import re
import sys
import threading
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import unquote

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.common_utils import (
    clean_post_text,
    get_config,
    get_proxy_url,
    get_requests_proxies,
    resolve_session_dir,
    safe_int,
    setup_logger,
)
from common.daemon import compute_jittered_interval, interruptible_sleep, run_daemon_loop
from common.llm_gateway import call_llm_json, render_prompt_template
from common.mongo_db.mongo_base import gen_db_object
from common.x_text import calc_x_weighted_length
from platforms.playwright_base import (
    BusinessErrorException,
    UserDataDirLockTimeoutError,
    _dismiss_overlays,
    _robust_click,
    check_for_crash,
    persistent_browser_session,
)
from platforms.x_api import fetch_x_tweet_zero_auth
from platforms.x_playwright import (
    _locate_focal_tweet,
    _locate_x_reply_box,
    _submit_x_post,
    _wait_x_home_ready,
    _wait_x_post_detail_ready,
    open_browser_for_manual_use,
)

# ==============================================================================
# 一、运维配置区（直接修改下方变量并重启即可生效，无需任何命令行参数）
# ==============================================================================

# 0. 运行模式开关（按需把 RUN_MODE 改成下面任一字符串即可）：
#   "daemon"        -> 【默认挂机】并行启动 3 个独立工人线程（采集 / 生成 / 回复，各自按周期独立运行）
#   "once_all"      -> 【全流程跑一轮】顺序执行 1 次「采集 -> 生成 -> 回复」后退出（适合手动测试完整链路）
#   "collect_once"  -> 【仅采一轮】只运行模块 1 收集评论入库后退出
#   "generate_once" -> 【仅生一轮】只运行模块 2 查库生成回复后退出（不开浏览器）
#   "reply_once"    -> 【仅回一轮】只运行模块 3 精准执行回复后退出
#   "preview"       -> 【只读预检】不开浏览器、不写库、不发回复，打印待生成/待回复列表与看板
#   "status"        -> 【只看报表】打印各状态存量与最近回复明细
#   "login"         -> 【人工登录】打开可见 Chrome 供手动登录并固化浏览器目录会话
RUN_MODE: str = "daemon"

# 1. 需要自动监控并回复评论的账号列表（核心必填项：user_data_dir 浏览器会话目录；默认使用 x_myself）
ACCOUNTS: List[Dict[str, Any]] = [
    {
        "name": "x_myself",               # 数据库分账号隔离标识（建议保持唯一英文短名）
        "user_data_dir": resolve_session_dir("x_myself_browser_session_dir", "x_myself"),  # 已登录该 X 账号的浏览器数据目录路径
        "screen_name": str(get_config("x_myself_screen_name", "peterzhu1003") or "peterzhu1003").strip().lstrip("@"),  # 账号 @用户名（不带 @；可留空，打开浏览器后会自动识别）
        "enabled": True,                  # True=启用该账号；False=临时停用（不删配置）
    },
    # 如需追加其他已登录账号，按下方模板复制一段即可（screen_name 可留空自动识别）：
    # {
    #     "name": "x_account_2",
    #     "user_data_dir": r"W:\temp\x_account_2",
    #     "screen_name": "",
    #     "enabled": True,
    # },
]

# 2. 三个独立工人的并行调度开关与运行周期（单位：秒；默认 3600 秒 = 每 1 小时各独立运行一轮）
ENABLE_COLLECTOR: bool = True             # 是否启动【工人 1 · 收集评论】（False=不采集）
COLLECT_INTERVAL_SECONDS: int = 600      # 工人 1 基础执行间隔（秒；3600=1 小时，改小如 1800 即半小时一轮）
COLLECT_JITTER_SECONDS: Tuple[int, int] = (60, 240)  # 每轮额外附加的随机延迟范围 (最小秒, 最大秒)，防止整点机械规律
COLLECT_INITIAL_DELAY_SECONDS: int = 0    # 启动后工人 1 首次等待多少秒再开跑（0=启动立即运行）

ENABLE_GENERATOR: bool = True             # 是否启动【工人 2 · AI 生成回复】（纯查库不开浏览器）
GENERATE_INTERVAL_SECONDS: int = 600     # 工人 2 基础执行间隔（秒；3600=1 小时）
GENERATE_JITTER_SECONDS: Tuple[int, int] = (15, 60)  # 工人 2 轮次随机抖动范围（秒）
GENERATE_INITIAL_DELAY_SECONDS: int = 30  # 启动后工人 2 延迟几秒运行（默认错开 30 秒，刚好接住工人 1 刚采到的数据）

ENABLE_REPLIER: bool = True               # 是否启动【工人 3 · 精准执行回复】（开浏览器发回复）
REPLY_INTERVAL_SECONDS: int = 600        # 工人 3 基础执行间隔（秒；3600=1 小时）
REPLY_JITTER_SECONDS: Tuple[int, int] = (60, 240)    # 工人 3 轮次随机抖动范围（秒）
REPLY_INITIAL_DELAY_SECONDS: int = 60     # 启动后工人 3 延迟几秒运行（默认错开 60 秒，避免与工人 1 同时抢占同一浏览器目录）

# 3. 【模块 1 · 收集评论】监控范围、扫描深度与防重复过滤参数
# 互动监控范围开关（最常用）：
#   True  -> 只收集【你自己发出去的原创/引用主帖】下面别人的评论（以及你在自己帖下继续的追评）
#   False -> 除了自己主帖，也同时收集【你在别人帖子下发表评论后，别人回复你评论的消息】
ONLY_MY_ROOT_POSTS: bool = True
# 【保留过滤原因说明】单轮仅扫描本人主页最新 N 条原创/引用主帖，避免翻阅陈年旧帖引发无意义的浏览器滚屏风控
MAX_OWN_POSTS_TO_SCAN: int = 20           # 每次访问自己主页最多检查最新多少条主帖
# 【保留过滤原因说明】单轮最多打开 N 个帖子详情页做深度抓评，配合持锁上限控制单次浏览耗时
MAX_POST_DETAILS_PER_RUN: int = 8         # 单轮最多点开几条有评论/新提及的帖子详情页抓取完整评论树
PROFILE_SCROLL_ROUNDS: int = 2            # 浏览自己主页时向下滚屏次数（想扫更早的主帖可适当调大如 3~4）
MENTIONS_SCROLL_ROUNDS: int = 2           # 浏览通知页（全部通知 + 提及）时向下滚屏次数
DETAIL_SCROLL_ROUNDS: int = 2             # 点开单个帖子详情页后向下滚屏加载评论的次数
# 【保留过滤原因说明】对于 48h 内已产生评论的活跃主帖，即便回复总数未变（防删一条增一条），满 4 小时仍低频复查一次
POST_RECHECK_SECONDS: int = 14400         # 活跃主帖强制复查周期（秒；14400=4 小时）
# 【保留过滤原因说明】防止首次上线翻出数月前陈年旧评论批量回复引发扰民与风控；设为 0 表示不限制时间窗
MAX_REPLY_AGE_HOURS: int = 72             # 只收集最近多少小时内发表的评论（72=只看近 3 天；0=历史全收）
# 【保留过滤原因说明】同一主帖下对同一用户最多回复次数（默认 1 次，彻底杜绝遇到其他自动回复机器人时无限对刷死循环）
MAX_REPLIES_PER_USER_PER_POST: int = 1    # 同一帖内对同一个用户最多自动回复几次（想支持来回追评可改为 2，0=不限）

# 4. 【模块 2 · 生成回复】AI 模型组与回复安全长度参数
PROMPT_TEMPLATE_FILENAME: str = "auto_reply_comment.md"  # 使用的提示词文件名（位于 prompt/x_growth/ 目录）
GENERATOR_MODEL_GROUP: str = "low"        # AI 模型组路由（"low"=普通低成本模型 / "medium"=中等模型；严禁硬编码具体模型名）
MAX_GENERATE_PER_RUN: int = 30            # 单轮最多批量生成多少条待回复评论
MAX_REPLY_WEIGHTED_CHARS: int = 240       # 生成回复的最大 X 加权字符长度（中文约 120 字、英文约 240 字母，硬上限 <= 280）
# 【保留过滤原因说明】短文本回复生成场景，120s 超时可快速触发网关容灾切换模型，避免单次卡死拖垮整轮队列
LLM_TIMEOUT_SECONDS: int = 120            # 单次 AI 生成超时时间（秒）
MAX_GENERATE_RETRIES: int = 3             # 单条评论 AI 生成失败最多重试几次（超限标为 failed）

# 5. 【模块 3 · 执行回复】浏览器可见性、频率控制与风控冷却参数
DRY_RUN: bool = False                     # True=仅预览待发内容，绝不打开浏览器或真发；False=真实发送回复
BROWSER_VISIBLE: bool = False             # False=后台静默运行不弹窗（推荐挂机）；True=弹出 Chrome 窗口方便观察过程
LIKE_COMMENT_BEFORE_REPLY: bool = True    # 回复对方评论前是否顺手给对方那条评论点赞
MAX_REPLIES_PER_RUN: int = 5              # 【保留过滤原因说明】单轮最多真实发出 5 条回复，分散行为节奏防高频刷评风控
MAX_REPLIES_PER_DAY: int = 30             # 【保留过滤原因说明】单账号 24 小时滚动自动回复上限（设为 0 表示不限制）
ACTION_DELAY_RANGE: Tuple[int, int] = (12, 25)  # 同一轮发多条回复时，每两条之间随机停顿多少秒 (最小秒, 最大秒)
# 【保留过滤原因说明】`platforms/playwright_base.py` 跨进程目录锁超时为 300s；单次持锁满 150s 主动收手放锁，确保与采集/发帖/互关共存不超时
SESSION_MAX_HOLD_SECONDS: int = 150       # 单次打开浏览器的最长持锁秒数（到点自动收手放锁，下轮再继续）
MAX_REPLY_RETRIES: int = 3                # 单条评论发送失败最多重试几次（超限标为 failed）
RETRY_COOLDOWN_SECONDS: int = 1800        # 单条生成或发送临时失败后的重试退避秒数（1800=30 分钟后再试）
RISK_COOLDOWN_SECONDS: int = 21600        # 遇安全验证、登录失效或业务风控拦截时，该账号自动静默冷却秒数（21600=6 小时）

# 6. 独立 MongoDB 集合名（物理隔离历史旧表与采集评论池，一般无需修改）
REPLY_INBOX_COLLECTION: str = "x_reply_inbox"              # 评论收件箱与生成/发送状态表
POST_CHECKPOINTS_COLLECTION: str = "x_reply_post_checkpoints"  # 主帖检查点表（记录回复数变化与根帖归属）

# ==============================================================================
# 二、基础工具、数据库索引与 X 接口旁路嗅探器
# ==============================================================================

TZ_CN = timezone(timedelta(hours=8))
X_HOME_URL = "https://x.com/home"
X_NOTIFICATIONS_URL = "https://x.com/notifications"
X_MENTIONS_URL = "https://x.com/notifications/mentions"
AUTO_REPLIER_PROMPT_DIR: Path = _PROJECT_ROOT / "prompt" / "x_growth"

_NOTIF_OPS = ("NotificationsTimeline", "NotificationsMentions", "RestNotifications")
_PROFILE_OPS = ("UserTweets", "UserOriginalsTimeline", "UserTweetsAndReplies")

_RE_INBOX_GQL = re.compile(
    r"/graphql/[^/]+/(NotificationsTimeline|NotificationsMentions|UserTweets|UserOriginalsTimeline|UserTweetsAndReplies|TweetDetail|UserByScreenName)(?:\?|$)",
    re.IGNORECASE,
)
_RE_INBOX_REST_NOTIF = re.compile(
    r"/i/api/2/notifications/(?:all|mentions|verified)\.json(?:\?|$)",
    re.IGNORECASE,
)
_RE_FOCAL_ID = re.compile(r'"focalTweetId"\s*:\s*"(\d+)"')
_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
_HASHTAG_RE = re.compile(r"(?:^|\s)#[\w\u4e00-\u9fff]+", re.UNICODE)
_LEADING_MENTIONS_RE = re.compile(r"^(?:@[A-Za-z0-9_]+\s*)+")
_LANG_CODE_RE = re.compile(r"^[a-z]{2,3}$")
_UNDETERMINED_LANGS = {"", "unknown", "und", "qme", "qam", "zxx", "art"}
_RISK_HALT_STATUSES = {"locked_challenge", "not_logged_in", "suspended", "cooldown_required"}

_INDEXED_DBS: Set[int] = set()
_LOGGER: Optional[Any] = None


def _get_logger() -> Any:
    global _LOGGER
    if _LOGGER is None:
        _LOGGER = setup_logger(app_name="x_growth_replier")
    return _LOGGER


def _now_iso(now_ts: Optional[int] = None) -> str:
    dt = datetime.fromtimestamp(int(now_ts), tz=TZ_CN) if now_ts is not None else datetime.now(TZ_CN)
    return dt.isoformat(timespec="seconds")


def _clean_handle(handle: Any) -> str:
    return str(handle or "").strip().lstrip("@").split("/")[0].split("?")[0].strip()


def _norm_lang(lang: Any) -> str:
    raw = str(lang or "").strip().lower().replace("_", "-")
    if not raw:
        return "unknown"
    return raw.split("-", 1)[0].strip() or "unknown"


def _parse_x_ts(raw: Any) -> int:
    if isinstance(raw, (int, float)):
        v = int(raw)
        return (v // 1000 if v > 10_000_000_000 else v) if v > 0 else 0
    s = str(raw or "").strip()
    if not s:
        return 0
    if s.isdigit():
        v = int(s)
        return v // 1000 if v > 10_000_000_000 else v
    try:
        dt = parsedate_to_datetime(s)
        return int((dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp())
    except Exception:
        return 0


def _sleep_jitter(delay_range: Sequence[float]) -> None:
    try:
        lo, hi = float(delay_range[0]), float(delay_range[1])
    except Exception:
        lo, hi = 12.0, 25.0
    if hi > 0:
        time.sleep(random.uniform(max(1.0, lo), max(lo + 1.0, hi)))


def _parse_jitter_pair(raw_jitter: Any) -> Tuple[int, int]:
    if isinstance(raw_jitter, (list, tuple)) and len(raw_jitter) == 2:
        lo, hi = max(0, int(raw_jitter[0])), max(0, int(raw_jitter[1]))
        return (hi, lo) if lo > hi else (lo, hi)
    return (0, 0)


def _default_cfg() -> Dict[str, Any]:
    return {
        "enable_collector": ENABLE_COLLECTOR,
        "collect_interval_seconds": COLLECT_INTERVAL_SECONDS,
        "collect_jitter_seconds": COLLECT_JITTER_SECONDS,
        "collect_initial_delay_seconds": COLLECT_INITIAL_DELAY_SECONDS,
        "enable_generator": ENABLE_GENERATOR,
        "generate_interval_seconds": GENERATE_INTERVAL_SECONDS,
        "generate_jitter_seconds": GENERATE_JITTER_SECONDS,
        "generate_initial_delay_seconds": GENERATE_INITIAL_DELAY_SECONDS,
        "enable_replier": ENABLE_REPLIER,
        "reply_interval_seconds": REPLY_INTERVAL_SECONDS,
        "reply_jitter_seconds": REPLY_JITTER_SECONDS,
        "reply_initial_delay_seconds": REPLY_INITIAL_DELAY_SECONDS,
        "only_my_root_posts": ONLY_MY_ROOT_POSTS,
        "max_own_posts_to_scan": MAX_OWN_POSTS_TO_SCAN,
        "max_post_details_per_run": MAX_POST_DETAILS_PER_RUN,
        "profile_scroll_rounds": PROFILE_SCROLL_ROUNDS,
        "mentions_scroll_rounds": MENTIONS_SCROLL_ROUNDS,
        "detail_scroll_rounds": DETAIL_SCROLL_ROUNDS,
        "post_recheck_seconds": POST_RECHECK_SECONDS,
        "max_reply_age_hours": MAX_REPLY_AGE_HOURS,
        "max_replies_per_user_per_post": MAX_REPLIES_PER_USER_PER_POST,
        "generator_model_group": GENERATOR_MODEL_GROUP,
        "max_generate_per_run": MAX_GENERATE_PER_RUN,
        "max_reply_weighted_chars": MAX_REPLY_WEIGHTED_CHARS,
        "llm_timeout_seconds": LLM_TIMEOUT_SECONDS,
        "max_generate_retries": MAX_GENERATE_RETRIES,
        "dry_run": DRY_RUN,
        "browser_visible": BROWSER_VISIBLE,
        "like_comment_before_reply": LIKE_COMMENT_BEFORE_REPLY,
        "max_replies_per_run": MAX_REPLIES_PER_RUN,
        "max_replies_per_day": MAX_REPLIES_PER_DAY,
        "action_delay_range": ACTION_DELAY_RANGE,
        "session_max_hold_seconds": SESSION_MAX_HOLD_SECONDS,
        "max_reply_retries": MAX_REPLY_RETRIES,
        "retry_cooldown_seconds": RETRY_COOLDOWN_SECONDS,
        "risk_cooldown_seconds": RISK_COOLDOWN_SECONDS,
    }


def _ensure_indexes(db: Any) -> None:
    if db is None:
        return
    key = id(getattr(db, "db", db))
    if key in _INDEXED_DBS:
        return
    ci = getattr(db, "create_index", None)
    if callable(ci):
        ci(REPLY_INBOX_COLLECTION, [("account_name", 1), ("reply_id", 1)], unique=True)
        ci(REPLY_INBOX_COLLECTION, [("account_name", 1), ("status", 1), ("reply_publish_ts", 1)], unique=False)
        ci(
            REPLY_INBOX_COLLECTION,
            [("account_name", 1), ("post_id", 1), ("reply_author_lower", 1), ("status", 1)],
            unique=False,
        )
        ci(REPLY_INBOX_COLLECTION, [("account_name", 1), ("status", 1), ("replied_at_ts", -1)], unique=False)
        ci(POST_CHECKPOINTS_COLLECTION, [("account_name", 1), ("post_id", 1)], unique=True)
    _INDEXED_DBS.add(key)


def resolve_accounts(accounts: Optional[Sequence[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    """归一化已启用的账号配置（按 user_data_dir 去重）。"""
    resolved: List[Dict[str, Any]] = []
    seen_dirs: Set[str] = set()
    for item in (accounts if accounts is not None else ACCOUNTS):
        if not isinstance(item, dict) or item.get("enabled") is False:
            continue
        raw_dir = str(item.get("user_data_dir") or "").strip()
        if not raw_dir:
            continue
        session_path = str(Path(raw_dir).expanduser().resolve())
        norm_key = os.path.normcase(session_path)
        if norm_key in seen_dirs:
            continue
        seen_dirs.add(norm_key)
        resolved.append(
            {
                **item,
                "name": str(item.get("name") or Path(session_path).name).strip(),
                "screen_name": _clean_handle(item.get("screen_name")),
                "user_data_dir": session_path,
            }
        )
    return resolved


def inspect_x_account_status(page: Any) -> Tuple[str, Optional[str]]:
    """检查当前真实 Chrome 页面是否处于正常登录态，若遇验证/掉线立即阻断。"""
    cur_url = str(page.url or "").lower()
    if "/account/access" in cur_url or "arkoselabs" in cur_url:
        return "locked_challenge", f"当前账号触发 X 安全验证解锁页 ({page.url})，等待冷却后重试或手动解锁。"
    if "/i/flow/login" in cur_url or cur_url.rstrip("/").endswith("/login"):
        return "not_logged_in", f"当前浏览器会话未登录 X ({page.url})，请先执行登录。"

    for sel, st, msg in (
        ('iframe[src*="arkoselabs"]', "locked_challenge", "页面检测到 Arkose 人机验证框"),
        ('iframe[id*="arkose"]', "locked_challenge", "页面检测到 Arkose 验证组件"),
        ('form[action*="/account/access"]', "locked_challenge", "页面检测到 X 安全验证表单"),
        ('input[autocomplete="username"]', "not_logged_in", "页面检测到未登录用户名输入框"),
        ('a[data-testid="loginButton"]', "not_logged_in", "页面检测到未登录按钮"),
    ):
        try:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible():
                return st, msg
        except Exception:
            continue
    return "ok", None


def extract_logged_in_identity(context: Any, page: Any) -> Tuple[str, str]:
    """从已登录浏览器上下文中自动提取当前账号 UID 与小写 @screen_name。"""
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
            if href and "/" not in href and href.lower() not in ("i", "home", "explore", "notifications", "messages", "search"):
                self_handle = href.lower()
    except Exception:
        pass

    return self_uid, self_handle


def _resolve_session_identity(context: Any, page: Any, fallback_handle: str) -> Tuple[str, str]:
    self_uid, detected_handle = extract_logged_in_identity(context, page)
    if not detected_handle and not fallback_handle:
        try:
            page.locator('a[data-testid="AppTabBar_Profile_Link"]').first.wait_for(
                state="attached", timeout=5000
            )
            self_uid, detected_handle = extract_logged_in_identity(context, page)
        except Exception:
            pass
    return self_uid, (detected_handle or fallback_handle)


def _clean_x_text_with_entities(raw_text: str, entities: Dict[str, Any], ext_entities: Dict[str, Any]) -> str:
    txt = str(raw_text or "")
    for u_obj in (entities.get("urls") or []):
        if isinstance(u_obj, dict) and u_obj.get("url"):
            txt = txt.replace(u_obj["url"], u_obj.get("expanded_url") or "")
    for m_obj in (ext_entities.get("media") or []) + (entities.get("media") or []):
        if isinstance(m_obj, dict) and m_obj.get("url"):
            txt = txt.replace(m_obj["url"], "")
    return clean_post_text(txt)


def _extract_reply_body_text(
    raw_text: str,
    entities: Dict[str, Any],
    ext_entities: Dict[str, Any],
    *,
    is_reply: bool,
    display_text_range: Any = None,
    used_note_tweet: bool = False,
) -> str:
    """剥离 X 自动前置的 `@handle` 回复前缀及图片短链，保留对方真正说的话。"""
    sliced = str(raw_text or "")
    stripped_by_range = False
    if (
        is_reply
        and (not used_note_tweet)
        and isinstance(display_text_range, (list, tuple))
        and len(display_text_range) >= 1
    ):
        start_idx = safe_int(display_text_range[0], 0)
        if 0 < start_idx <= len(sliced):
            prefix = sliced[:start_idx]
            if re.fullmatch(r"(?:\s*@[A-Za-z0-9_]+)+\s*", prefix):
                sliced = sliced[start_idx:]
                stripped_by_range = True

    cleaned = _clean_x_text_with_entities(sliced, entities, ext_entities)
    if is_reply and not stripped_by_range:
        cleaned = _LEADING_MENTIONS_RE.sub("", cleaned).strip()
    return cleaned


def _unwrap_user_result(raw_user: Any) -> Dict[str, Any]:
    if not isinstance(raw_user, dict):
        return {}
    if raw_user.get("__typename") == "UserWithVisibilityResults" and isinstance(raw_user.get("user"), dict):
        return raw_user["user"]
    return raw_user


def _extract_quoted_summary(tw: Dict[str, Any]) -> Tuple[str, bool]:
    quoted_res = ((tw.get("quoted_status_result") or {}).get("result")) or {}
    if not isinstance(quoted_res, dict):
        return "", False
    if quoted_res.get("__typename") == "TweetWithVisibilityResults" and isinstance(quoted_res.get("tweet"), dict):
        quoted_res = quoted_res["tweet"]
    q_leg = quoted_res.get("legacy") or {}
    if not isinstance(q_leg, dict) or not q_leg:
        return "", False

    q_user = _unwrap_user_result(((quoted_res.get("core") or {}).get("user_results") or {}).get("result"))
    q_uname = _clean_handle((q_user.get("core") or {}).get("screen_name") or (q_user.get("legacy") or {}).get("screen_name"))
    q_note = (((quoted_res.get("note_tweet") or {}).get("note_tweet_results") or {}).get("result") or {}).get("text")
    q_raw = q_note or q_leg.get("full_text") or ""
    q_txt = _clean_x_text_with_entities(q_raw, q_leg.get("entities") or {}, q_leg.get("extended_entities") or {})
    q_media = (q_leg.get("extended_entities") or {}).get("media") or (q_leg.get("entities") or {}).get("media") or []
    prefix = f"[引用 @{q_uname}]: " if q_uname else "[引用]: "
    return (f"{prefix}{q_txt}".strip() if q_txt else ""), bool(q_media)


def parse_gql_tweet_node(tw: Any) -> Optional[Dict[str, Any]]:
    """将 X GraphQL Tweet 节点规范化为统一轻量字典。"""
    if not isinstance(tw, dict):
        return None
    if tw.get("__typename") == "TweetWithVisibilityResults" and isinstance(tw.get("tweet"), dict):
        tw = tw["tweet"]
    legacy = tw.get("legacy") or {}
    if not isinstance(legacy, dict) or not legacy:
        return None
    tweet_id = str(tw.get("rest_id") or legacy.get("id_str") or "").strip()
    if not tweet_id:
        return None

    user_node = _unwrap_user_result(((tw.get("core") or {}).get("user_results") or {}).get("result"))
    u_core = user_node.get("core") or {}
    u_leg = user_node.get("legacy") or {}
    author_id = str(user_node.get("rest_id") or legacy.get("user_id_str") or "").strip()
    author_handle = _clean_handle(u_core.get("screen_name") or u_leg.get("screen_name"))
    author_name = str(u_core.get("name") or u_leg.get("name") or author_handle).strip()

    note_text = (((tw.get("note_tweet") or {}).get("note_tweet_results") or {}).get("result") or {}).get("text")
    raw_text = str(note_text or legacy.get("full_text") or legacy.get("text") or "")
    entities = legacy.get("entities") or {}
    ext_entities = legacy.get("extended_entities") or {}
    in_reply_to_status_id = str(legacy.get("in_reply_to_status_id_str") or "").strip()
    is_reply = bool(in_reply_to_status_id)

    text = _clean_x_text_with_entities(raw_text, entities, ext_entities)
    reply_body_text = _extract_reply_body_text(
        raw_text,
        entities,
        ext_entities,
        is_reply=is_reply,
        display_text_range=legacy.get("display_text_range"),
        used_note_tweet=bool(note_text),
    )

    quoted_summary, quoted_has_media = _extract_quoted_summary(tw)
    if quoted_summary and quoted_summary not in text:
        text = f"{text}\n{quoted_summary}".strip() if text else quoted_summary
    if quoted_summary and quoted_summary not in reply_body_text:
        reply_body_text = f"{reply_body_text}\n{quoted_summary}".strip() if reply_body_text else quoted_summary

    media_list = ext_entities.get("media") or entities.get("media") or []
    has_media = bool(media_list) or quoted_has_media

    return {
        "tweet_id": tweet_id,
        "author_id": author_id,
        "author_handle": author_handle,
        "author_handle_lower": author_handle.lower(),
        "author_name": author_name,
        "text": text,
        "reply_body_text": reply_body_text,
        "lang": _norm_lang(legacy.get("lang")),
        "publish_ts": _parse_x_ts(legacy.get("created_at")),
        "reply_count": safe_int(legacy.get("reply_count"), 0),
        "in_reply_to_status_id": in_reply_to_status_id,
        "in_reply_to_user_id": str(legacy.get("in_reply_to_user_id_str") or "").strip(),
        "in_reply_to_screen_name": _clean_handle(legacy.get("in_reply_to_screen_name")).lower(),
        "conversation_id": str(legacy.get("conversation_id_str") or "").strip() or ("" if is_reply else tweet_id),
        "is_retweet": bool(
            legacy.get("retweeted_status_result")
            or legacy.get("retweeted_status_id_str")
            or legacy.get("retweeted_status")
        ),
        "has_media": has_media,
        "url": f"https://x.com/{author_handle}/status/{tweet_id}" if author_handle else f"https://x.com/i/status/{tweet_id}",
    }


def _walk_gql_tweet_nodes(payload: Any) -> List[Dict[str, Any]]:
    """递归提取 GraphQL JSON 中的全部 Tweet 节点（跳过推广广告与引用嵌套内部重复抓取）。"""
    found: List[Dict[str, Any]] = []

    def _walk(obj: Any) -> None:
        if isinstance(obj, dict):
            if "promotedMetadata" in obj:
                return
            tn = obj.get("__typename")
            if tn == "TweetWithVisibilityResults" and isinstance(obj.get("tweet"), dict):
                _walk(obj["tweet"])
                return
            if isinstance(obj.get("legacy"), dict) and (obj.get("rest_id") or obj["legacy"].get("id_str")) and (
                tn == "Tweet" or isinstance(obj.get("core"), dict)
            ):
                found.append(obj)
                return
            for v in obj.values():
                _walk(v)
        elif isinstance(obj, list):
            for v in obj:
                _walk(v)

    _walk(payload)
    return found


def _parse_rest_notifications_payload(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """兼容解析 X 旧版 REST v2 通知接口 (`globalObjects.tweets` + `globalObjects.users`)。"""
    gobj = payload.get("globalObjects") or {}
    tweets_map = gobj.get("tweets") or {}
    users_map = gobj.get("users") or {}
    if not isinstance(tweets_map, dict):
        return []

    parsed: List[Dict[str, Any]] = []
    for raw_tid, leg in tweets_map.items():
        if not isinstance(leg, dict):
            continue
        tid = str(leg.get("id_str") or raw_tid or "").strip()
        uid = str(leg.get("user_id_str") or "").strip()
        u_obj = users_map.get(uid) if isinstance(users_map, dict) else {}
        u_obj = u_obj if isinstance(u_obj, dict) else {}
        fake_gql = {
            "__typename": "Tweet",
            "rest_id": tid,
            "core": {
                "user_results": {
                    "result": {
                        "__typename": "User",
                        "rest_id": uid,
                        "legacy": {
                            "screen_name": u_obj.get("screen_name") or "",
                            "name": u_obj.get("name") or "",
                        },
                    }
                }
            },
            "legacy": leg,
        }
        item = parse_gql_tweet_node(fake_gql)
        if item:
            parsed.append(item)
    return parsed


def _is_same_account(
    tweet: Dict[str, Any],
    *,
    self_handle_lower: str,
    self_uid: str = "",
) -> bool:
    th = str(tweet.get("author_handle_lower") or "").strip().lower()
    tuid = str(tweet.get("author_id") or "").strip()
    if self_handle_lower and th and th == self_handle_lower:
        return True
    if self_uid and tuid and tuid == self_uid:
        return True
    return False


def _is_reply_targeting_me(
    tweet: Dict[str, Any],
    *,
    my_tweet_ids: Set[str],
    self_handle_lower: str,
    self_uid: str = "",
) -> bool:
    """判断推文是否为他人发来的、明确回复我方推文/评论的目标回复。"""
    if tweet.get("is_retweet"):
        return False
    if _is_same_account(tweet, self_handle_lower=self_handle_lower, self_uid=self_uid):
        return False
    parent_id = str(tweet.get("in_reply_to_status_id") or "").strip()
    if not parent_id:
        return False
    return (
        (parent_id in my_tweet_ids)
        or (bool(self_handle_lower) and str(tweet.get("in_reply_to_screen_name") or "").lower() == self_handle_lower)
        or (bool(self_uid) and str(tweet.get("in_reply_to_user_id") or "") == self_uid)
    )


class XReplyInboxSniffer:
    """只读旁路嗅探器：从浏览器正常加载的 GraphQL / 通知接口提取推文、主帖与对话回复树。"""

    def __init__(self) -> None:
        self.op_counts: Dict[str, int] = {}
        self.tweets_by_id: Dict[str, Dict[str, Any]] = {}
        self.mention_tweets_by_id: Dict[str, Dict[str, Any]] = {}
        self.profile_own_posts_by_id: Dict[str, Dict[str, Any]] = {}
        self.detail_threads_by_focal: Dict[str, Dict[str, Dict[str, Any]]] = {}

    def _register_tweet(self, tw: Dict[str, Any]) -> None:
        tid = tw["tweet_id"]
        prev = self.tweets_by_id.get(tid)
        if prev is None or (len(tw.get("text") or "") >= len(prev.get("text") or "")):
            self.tweets_by_id[tid] = tw

    def ingest_payload(self, op: str, payload: Any, *, resp_url: str = "") -> None:
        if not isinstance(payload, dict):
            return
        if op == "RestNotifications":
            parsed_list = _parse_rest_notifications_payload(payload)
        else:
            parsed_list = [p for n in _walk_gql_tweet_nodes(payload) if (p := parse_gql_tweet_node(n)) is not None]

        if not parsed_list:
            return

        for p in parsed_list:
            self._register_tweet(p)

        if op in ("NotificationsTimeline", "NotificationsMentions", "RestNotifications"):
            for p in parsed_list:
                if (not p["is_retweet"]) and p["in_reply_to_status_id"]:
                    self.mention_tweets_by_id[p["tweet_id"]] = p

        elif op in ("UserTweets", "UserOriginalsTimeline", "UserTweetsAndReplies"):
            for p in parsed_list:
                if (not p["is_retweet"]) and (not p["in_reply_to_status_id"]):
                    self.profile_own_posts_by_id[p["tweet_id"]] = p

        elif op == "TweetDetail":
            focal_id = ""
            if resp_url:
                m_fid = _RE_FOCAL_ID.search(unquote(resp_url))
                if m_fid:
                    focal_id = m_fid.group(1)
            if not focal_id:
                focal_id = next(
                    (p["tweet_id"] for p in parsed_list if not p["in_reply_to_status_id"]),
                    parsed_list[0]["tweet_id"],
                )
            bucket = self.detail_threads_by_focal.setdefault(focal_id, {})
            for p in parsed_list:
                if not p["is_retweet"]:
                    bucket[p["tweet_id"]] = p

    def on_response(self, resp: Any) -> None:
        try:
            url = resp.url or ""
            if resp.status != 200:
                return
            m_gql = _RE_INBOX_GQL.search(url)
            if m_gql:
                op = m_gql.group(1)
                self.ingest_payload(op, resp.json(), resp_url=url)
                self.op_counts[op] = self.op_counts.get(op, 0) + 1
                return
            if _RE_INBOX_REST_NOTIF.search(url):
                self.ingest_payload("RestNotifications", resp.json(), resp_url=url)
                self.op_counts["RestNotifications"] = self.op_counts.get("RestNotifications", 0) + 1
        except Exception:
            pass

    def wait_for_ops(self, page: Any, ops: Sequence[str], prev_total: int, timeout_ms: int = 12000) -> bool:
        deadline = time.time() + timeout_ms / 1000.0
        while time.time() < deadline:
            if sum(self.op_counts.get(op, 0) for op in ops) > prev_total:
                return True
            page.wait_for_timeout(250)
        return False

    def find_my_existing_reply(
        self,
        target_reply_id: str,
        *,
        self_handle_lower: str,
        self_uid: str = "",
    ) -> Optional[Dict[str, Any]]:
        """实时检查本次页面响应中是否已经存在我方对目标评论的回复。"""
        tid = str(target_reply_id or "").strip()
        if not tid:
            return None
        for tw in self.tweets_by_id.values():
            if tw.get("is_retweet"):
                continue
            if str(tw.get("in_reply_to_status_id") or "") == tid and _is_same_account(
                tw, self_handle_lower=self_handle_lower, self_uid=self_uid
            ):
                return tw
        return None


# ==============================================================================
# 三、模块 1：回复收集与入库去重 (`collect_replies_for_account` + `upsert_collected_replies`)
# ==============================================================================

def _classify_unreplyable_text(reply_body_text: str, has_media: bool) -> Optional[str]:
    """仅剔除 100% 确定性无文字可回的空壳评论；其余全部交给 AI 模型判断。"""
    txt = str(reply_body_text or "").strip()
    if not txt:
        return "media_only_no_text" if has_media else "empty_reply_text"
    without_urls = _URL_RE.sub("", txt).strip()
    if not without_urls:
        return "url_only_no_text"
    return None


def _resolve_root_post_ownership_zero_auth(
    post_id: str,
    *,
    self_handle_lower: str,
    self_uid: str,
    tweets_by_id: Dict[str, Dict[str, Any]],
) -> Optional[Tuple[bool, str, str]]:
    """
    先查本地嗅探缓存，再使用 100% 免登公开接口确认根帖是否属于当前账号，
    避免为自己留在别人帖下的评论浪费浏览器资源。
    """
    cached = tweets_by_id.get(post_id)
    if cached:
        is_own = _is_same_account(cached, self_handle_lower=self_handle_lower, self_uid=self_uid)
        return is_own, cached.get("text") or "", cached.get("author_handle") or ""

    try:
        proxies = get_requests_proxies()
        std = fetch_x_tweet_zero_auth(post_id, proxies=proxies) if proxies is not None else fetch_x_tweet_zero_auth(post_id)
    except Exception:
        std = None
    if not isinstance(std, dict):
        return None

    author_obj = std.get("author") or {}
    author_handle = _clean_handle(author_obj.get("username"))
    author_uid = str(std.get("author_id") or "").strip()
    post_text = str((std.get("content") or {}).get("text_content") or "").strip()
    is_own = (bool(self_handle_lower) and author_handle.lower() == self_handle_lower) or (
        bool(self_uid) and author_uid == self_uid
    )
    return is_own, post_text, author_handle


def upsert_collected_replies(
    db: Any,
    *,
    account_name: str,
    self_handle: str,
    self_uid: str = "",
    tweets_by_id: Dict[str, Dict[str, Any]],
    candidate_replies: Iterable[Dict[str, Any]],
    only_my_root_posts: bool = ONLY_MY_ROOT_POSTS,
    max_reply_age_hours: int = MAX_REPLY_AGE_HOURS,
    max_replies_per_user_per_post: int = MAX_REPLIES_PER_USER_PER_POST,
    now_ts: Optional[int] = None,
) -> Dict[str, int]:
    """
    幂等落库捕获到的他人评论（仅写入 `x_reply_inbox`）：
    - 绝不重置已生成 / 已回复 / 已跳过记录；
    - 若线上已由本人或本工具回复过，自动标记或升级为 `status="replied" (detected_on_x)`；
    - 同一主帖下对同一用户超出 `max_replies_per_user_per_post` 自动跳过防死循环。
    """
    _ensure_indexes(db)
    col = db.get_collection(REPLY_INBOX_COLLECTION)
    cp_col = db.get_collection(POST_CHECKPOINTS_COLLECTION)
    now = int(now_ts if now_ts is not None else time.time())
    now_s = _now_iso(now)
    self_handle_low = _clean_handle(self_handle).lower()
    age_cutoff_ts = (now - int(max_reply_age_hours) * 3600) if int(max_reply_age_hours) > 0 else 0

    all_tweets = dict(tweets_by_id or {})
    for item in candidate_replies or []:
        if isinstance(item, dict) and item.get("tweet_id"):
            all_tweets.setdefault(item["tweet_id"], item)

    my_tweet_ids: Set[str] = set()
    my_reply_by_parent: Dict[str, Dict[str, Any]] = {}
    for tw in all_tweets.values():
        if tw.get("is_retweet"):
            continue
        if _is_same_account(tw, self_handle_lower=self_handle_low, self_uid=self_uid):
            my_tweet_ids.add(tw["tweet_id"])
            parent_id = str(tw.get("in_reply_to_status_id") or "").strip()
            if parent_id and parent_id not in my_reply_by_parent:
                my_reply_by_parent[parent_id] = tw

    # 先按时间从早到晚排序，确保同帖同一用户多条评论时优先保留最先发的评论
    sorted_replies = sorted(
        (r for r in (candidate_replies or []) if isinstance(r, dict) and r.get("tweet_id")),
        key=lambda x: (safe_int(x.get("publish_ts"), 0), str(x.get("tweet_id"))),
    )

    stats = Counter()
    seen_rids: Set[str] = set()
    for rep in sorted_replies:
        rid = str(rep["tweet_id"]).strip()
        if not rid or rid in seen_rids or rep.get("is_retweet"):
            continue
        seen_rids.add(rid)

        if not _is_reply_targeting_me(
            rep,
            my_tweet_ids=my_tweet_ids,
            self_handle_lower=self_handle_low,
            self_uid=self_uid,
        ):
            continue

        parent_id = str(rep.get("in_reply_to_status_id") or "").strip()
        parent_tweet = all_tweets.get(parent_id)

        post_id = str(rep.get("conversation_id") or "").strip() or parent_id
        root_tweet = all_tweets.get(post_id)
        cp_doc = cp_col.find_one({"account_name": account_name, "post_id": post_id}) or {}

        is_own_root: Optional[bool] = None
        if root_tweet:
            is_own_root = _is_same_account(root_tweet, self_handle_lower=self_handle_low, self_uid=self_uid)
        elif "is_own_post" in cp_doc and cp_doc.get("is_own_post") is not None:
            is_own_root = bool(cp_doc.get("is_own_post"))
        elif parent_id == post_id and parent_id in my_tweet_ids:
            is_own_root = True

        if only_my_root_posts and is_own_root is not True:
            stats["skipped_not_own_root"] += 1
            continue

        pub_ts = safe_int(rep.get("publish_ts"), 0)
        if age_cutoff_ts > 0 and pub_ts > 0 and pub_ts < age_cutoff_ts:
            stats["skipped_too_old"] += 1
            continue

        post_text = str((root_tweet or {}).get("text") or cp_doc.get("post_text") or "").strip()
        post_has_media = bool((root_tweet or {}).get("has_media"))
        post_url = str(
            (root_tweet or {}).get("url")
            or cp_doc.get("post_url")
            or (f"https://x.com/{self_handle_low}/status/{post_id}" if (is_own_root and self_handle_low) else f"https://x.com/i/status/{post_id}")
        )
        parent_text = str((parent_tweet or {}).get("text") or post_text).strip()
        my_existing = my_reply_by_parent.get(rid)

        existing_doc = col.find_one({"account_name": account_name, "reply_id": rid})
        if existing_doc is not None:
            updates: Dict[str, Any] = {}
            if post_text and not str(existing_doc.get("post_text") or "").strip():
                updates["post_text"] = post_text
            if parent_text and not str(existing_doc.get("parent_tweet_text") or "").strip():
                updates["parent_tweet_text"] = parent_text
            if my_existing and existing_doc.get("status") != "replied":
                updates.update(
                    {
                        "status": "replied",
                        "replied_tweet_id": my_existing["tweet_id"],
                        "replied_url": my_existing["url"],
                        "replied_text": my_existing.get("reply_body_text") or my_existing.get("text") or "",
                        "replied_source": "detected_on_x",
                        "replied_at": _now_iso(my_existing["publish_ts"] or now),
                        "replied_at_ts": my_existing["publish_ts"] or now,
                        "updated_at": now_s,
                        "updated_at_ts": now,
                    }
                )
                stats["upgraded_replied_on_x"] += 1
            elif updates:
                updates["updated_at"] = now_s
                updates["updated_at_ts"] = now
            if updates:
                col.update_one({"account_name": account_name, "reply_id": rid}, {"$set": updates})
            stats["existing_unchanged"] += 1
            continue

        reply_author = str(rep.get("author_handle") or "").strip()
        reply_author_low = str(rep.get("author_handle_lower") or reply_author.lower()).strip()
        reply_body = str(rep.get("reply_body_text") or "").strip()
        unreplyable_reason = _classify_unreplyable_text(reply_body, bool(rep.get("has_media")))

        doc: Dict[str, Any] = {
            "account_name": account_name,
            "account_handle": self_handle_low,
            "reply_id": rid,
            "reply_url": rep.get("url") or f"https://x.com/i/status/{rid}",
            "reply_author": reply_author,
            "reply_author_lower": reply_author_low,
            "reply_author_name": str(rep.get("author_name") or reply_author).strip(),
            "reply_text": reply_body,
            "reply_raw_text": str(rep.get("text") or "").strip(),
            "reply_lang": str(rep.get("lang") or "unknown"),
            "reply_has_media": bool(rep.get("has_media")),
            "reply_publish_ts": pub_ts,
            "reply_publish_at": _now_iso(pub_ts) if pub_ts > 0 else None,
            "post_id": post_id,
            "post_url": post_url,
            "post_text": post_text,
            "post_has_media": post_has_media,
            "is_own_root_post": bool(is_own_root) if is_own_root is not None else False,
            "parent_tweet_id": parent_id,
            "parent_tweet_text": parent_text,
            "status": "pending_generate",
            "skip_reason": None,
            "generated_reply": None,
            "generated_lang": None,
            "generated_weighted_len": None,
            "generator_model": None,
            "generated_at": None,
            "generated_at_ts": 0,
            "generate_attempts": 0,
            "generate_last_error": None,
            "generate_next_retry_ts": 0,
            "replied_tweet_id": None,
            "replied_url": None,
            "replied_text": None,
            "replied_source": None,
            "replied_at": None,
            "replied_at_ts": 0,
            "reply_attempts": 0,
            "reply_last_error": None,
            "reply_next_retry_ts": 0,
            "collected_at": now_s,
            "collected_at_ts": now,
            "updated_at": now_s,
            "updated_at_ts": now,
        }

        if my_existing:
            doc.update(
                {
                    "status": "replied",
                    "replied_tweet_id": my_existing["tweet_id"],
                    "replied_url": my_existing["url"],
                    "replied_text": my_existing.get("reply_body_text") or my_existing.get("text") or "",
                    "replied_source": "detected_on_x",
                    "replied_at": _now_iso(my_existing["publish_ts"] or now),
                    "replied_at_ts": my_existing["publish_ts"] or now,
                }
            )
            stats["new_already_replied"] += 1
        elif unreplyable_reason:
            doc.update({"status": "skipped", "skip_reason": unreplyable_reason})
            stats["new_skipped"] += 1
        elif max_replies_per_user_per_post > 0 and reply_author_low:
            prior_cnt = col.count_documents(
                {
                    "account_name": account_name,
                    "post_id": post_id,
                    "reply_author_lower": reply_author_low,
                    "status": {"$in": ["pending_generate", "ready_to_reply", "replied"]},
                }
            )
            if prior_cnt >= int(max_replies_per_user_per_post):
                doc.update({"status": "skipped", "skip_reason": "max_replies_per_user_per_post_reached"})
                stats["new_skipped"] += 1
            else:
                stats["new_pending"] += 1
        else:
            stats["new_pending"] += 1

        col.update_one({"account_name": account_name, "reply_id": rid}, {"$set": doc}, upsert=True)
        stats["total_inserted"] += 1

    return dict(stats)


def collect_replies_for_account(
    account: Dict[str, Any],
    *,
    db: Any = None,
    config: Optional[Dict[str, Any]] = None,
    logger: Any = None,
) -> Dict[str, Any]:
    """【模块 1 入口】打开指定账号的已登录浏览器，抓取提及与本人主帖下的新评论并落库。"""
    log = logger or _get_logger()
    cfg = {**_default_cfg(), **(config or {})}
    db_obj = db if db is not None else gen_db_object()
    _ensure_indexes(db_obj)

    acc_name = str(account["name"])
    acc_handle = _clean_handle(account.get("screen_name")).lower()
    session_path = str(Path(str(account["user_data_dir"])).expanduser().resolve())

    if not os.path.isdir(session_path):
        warn = f"浏览器会话目录不存在: {session_path}"
        log.error(f"🚨 [模块1/目录缺失 | {acc_name}] {warn}")
        return {"account_name": acc_name, "status": "session_missing", "warning": warn, "browser_opened": False}

    inbox_col = db_obj.get_collection(REPLY_INBOX_COLLECTION)
    cp_col = db_obj.get_collection(POST_CHECKPOINTS_COLLECTION)
    proxy_server = get_proxy_url()
    sniffer = XReplyInboxSniffer()

    only_own_roots = bool(cfg.get("only_my_root_posts", ONLY_MY_ROOT_POSTS))
    max_own_posts = max(1, safe_int(cfg.get("max_own_posts_to_scan"), MAX_OWN_POSTS_TO_SCAN))
    max_details = max(1, safe_int(cfg.get("max_post_details_per_run"), MAX_POST_DETAILS_PER_RUN))
    recheck_sec = max(60, safe_int(cfg.get("post_recheck_seconds"), POST_RECHECK_SECONDS))
    max_age_h = max(0, safe_int(cfg.get("max_reply_age_hours"), MAX_REPLY_AGE_HOURS))
    max_per_user = max(0, safe_int(cfg.get("max_replies_per_user_per_post"), MAX_REPLIES_PER_USER_PER_POST))
    hold_limit = max(30, safe_int(cfg.get("session_max_hold_seconds"), SESSION_MAX_HOLD_SECONDS))

    try:
        with persistent_browser_session(
            session_path,
            debug=bool(cfg.get("browser_visible", BROWSER_VISIBLE)),
            proxy=proxy_server,
            use_stealth_chrome=True,
            reuse_existing_page=True,
            default_timeout_ms=45000,
            navigation_timeout_ms=45000,
            install_guard=True,
        ) as (context, page):
            lock_t0 = time.monotonic()
            page.on("response", sniffer.on_response)
            try:
                # 第 1a 站：优先浏览「全部通知」页（绕开 /mentions 对新号/小号的静默折叠过滤）
                mentions_scrolls = max(0, safe_int(cfg.get("mentions_scroll_rounds"), MENTIONS_SCROLL_ROUNDS))
                prev_all = sum(sniffer.op_counts.get(op_k, 0) for op_k in _NOTIF_OPS)
                page.goto(X_NOTIFICATIONS_URL, timeout=45000, wait_until="domcontentloaded")
                sniffer.wait_for_ops(page, _NOTIF_OPS, prev_all, timeout_ms=10000)
                page.wait_for_timeout(random.randint(1500, 2600))
                _dismiss_overlays(page, aggressive=False, desc="x-reply-notifications")
                check_for_crash(page)

                acc_st, acc_msg = inspect_x_account_status(page)
                if acc_st != "ok":
                    log.error(f"🚨 [模块1/账号阻断 | {acc_name}] status={acc_st} | {acc_msg}")
                    return {"account_name": acc_name, "status": acc_st, "warning": acc_msg, "browser_opened": True}

                self_uid, eff_handle = _resolve_session_identity(context, page, acc_handle)
                if not eff_handle:
                    msg = f"未能识别浏览器登录账号用户名 ({session_path})，请确认已登录。"
                    log.error(f"🚨 [模块1/身份缺失 | {acc_name}] {msg}")
                    return {"account_name": acc_name, "status": "not_logged_in", "warning": msg, "browser_opened": True}

                for _ in range(mentions_scrolls):
                    if time.monotonic() - lock_t0 >= hold_limit:
                        break
                    page.mouse.wheel(0, random.randint(1400, 2200))
                    page.wait_for_timeout(random.randint(1200, 2000))

                # 第 1b 站：补充浏览「提及」页（全部通知刷屏极多时兜底早先的 @ 提及）
                if time.monotonic() - lock_t0 < hold_limit:
                    prev_m = sum(sniffer.op_counts.get(op_k, 0) for op_k in _NOTIF_OPS)
                    page.goto(X_MENTIONS_URL, timeout=45000, wait_until="domcontentloaded")
                    sniffer.wait_for_ops(page, _NOTIF_OPS, prev_m, timeout_ms=10000)
                    page.wait_for_timeout(random.randint(1000, 1800))
                    _dismiss_overlays(page, aggressive=False, desc="x-reply-mentions")
                    check_for_crash(page)
                    acc_st_m, acc_msg_m = inspect_x_account_status(page)
                    if acc_st_m != "ok":
                        log.error(f"🚨 [模块1/提及阻断 | {acc_name}] status={acc_st_m} | {acc_msg_m}")
                        return {"account_name": acc_name, "status": acc_st_m, "warning": acc_msg_m, "browser_opened": True}
                    for _ in range(min(1, mentions_scrolls)):
                        if time.monotonic() - lock_t0 >= hold_limit:
                            break
                        page.mouse.wheel(0, random.randint(1400, 2200))
                        page.wait_for_timeout(random.randint(1000, 1600))

                # 第 2 站：浏览己方个人主页，发现最近原创/引用主帖及其 reply_count
                prev_u = sum(sniffer.op_counts.get(op_k, 0) for op_k in _PROFILE_OPS)
                page.goto(f"https://x.com/{eff_handle}", timeout=45000, wait_until="domcontentloaded")
                sniffer.wait_for_ops(page, _PROFILE_OPS, prev_u, timeout_ms=12000)
                page.wait_for_timeout(random.randint(1500, 2600))
                _dismiss_overlays(page, aggressive=False, desc="x-reply-profile")
                check_for_crash(page)

                acc_st2, acc_msg2 = inspect_x_account_status(page)
                if acc_st2 != "ok":
                    log.error(f"🚨 [模块1/主页阻断 | {acc_name}] status={acc_st2} | {acc_msg2}")
                    return {"account_name": acc_name, "status": acc_st2, "warning": acc_msg2, "browser_opened": True}

                for _ in range(max(0, safe_int(cfg.get("profile_scroll_rounds"), PROFILE_SCROLL_ROUNDS))):
                    if time.monotonic() - lock_t0 >= hold_limit:
                        break
                    page.mouse.wheel(0, random.randint(1800, 2600))
                    page.wait_for_timeout(random.randint(1200, 2000))

                # 显式空采防护：重载主页一次，仍无任何接口响应绝不伪装正常 0 条
                if sum(sniffer.op_counts.values()) == 0:
                    page.reload(timeout=45000, wait_until="domcontentloaded")
                    sniffer.wait_for_ops(page, _PROFILE_OPS, 0, timeout_ms=10000)
                    page.wait_for_timeout(2000)
                if sum(sniffer.op_counts.values()) == 0:
                    msg = f"账号 @{eff_handle} 未捕获到任何 X 接口响应 (op_counts={{}})，疑似网络或页面异常。"
                    log.warning(f"⚠️ [模块1/空采告警 | {acc_name}] {msg}")
                    return {"account_name": acc_name, "status": "empty_capture", "warning": msg, "browser_opened": True}

                now_ts = int(time.time())
                age_cutoff_ts = (now_ts - max_age_h * 3600) if max_age_h > 0 else 0

                own_posts = [
                    p for p in sniffer.profile_own_posts_by_id.values()
                    if _is_same_account(p, self_handle_lower=eff_handle, self_uid=self_uid)
                ]
                own_posts.sort(key=lambda x: safe_int(x.get("publish_ts"), 0), reverse=True)
                own_posts = own_posts[:max_own_posts]

                own_posts_map = {p["tweet_id"]: p for p in own_posts}
                my_tweet_ids: Set[str] = {
                    tw["tweet_id"]
                    for tw in sniffer.tweets_by_id.values()
                    if (not tw.get("is_retweet")) and _is_same_account(tw, self_handle_lower=eff_handle, self_uid=self_uid)
                }

                # 从全部通知 + 提及通知中精准筛选出指向我方推文的他人未入库回复（剔除己方被赞回复及无关第三方回复）
                mentions_captured = 0
                mentions_skip_other_root = 0
                mentions_skip_old = 0
                mentions_already_in_db = 0
                fresh_mention_replies: List[Dict[str, Any]] = []
                fresh_mention_conv_ids: Set[str] = set()

                for rep in sorted(
                    sniffer.mention_tweets_by_id.values(),
                    key=lambda x: safe_int(x.get("publish_ts"), 0),
                    reverse=True,
                ):
                    if not _is_reply_targeting_me(
                        rep,
                        my_tweet_ids=my_tweet_ids,
                        self_handle_lower=eff_handle,
                        self_uid=self_uid,
                    ):
                        continue
                    mentions_captured += 1
                    rid = rep["tweet_id"]
                    pub_ts = safe_int(rep.get("publish_ts"), 0)
                    if age_cutoff_ts > 0 and pub_ts > 0 and pub_ts < age_cutoff_ts:
                        mentions_skip_old += 1
                        continue
                    if inbox_col.find_one({"account_name": acc_name, "reply_id": rid}):
                        mentions_already_in_db += 1
                        continue
                    conv_id = str(rep.get("conversation_id") or rep.get("in_reply_to_status_id") or "").strip()
                    if not conv_id:
                        continue
                    fresh_mention_replies.append(rep)
                    fresh_mention_conv_ids.add(conv_id)

                # 选出需要打开详情页的帖子/评论链接
                targets_to_open: List[Tuple[str, str, str]] = []  # (target_id, target_url, kind)
                queued_ids: Set[str] = set()

                for post in own_posts:
                    pid = post["tweet_id"]
                    rcount = safe_int(post.get("reply_count"), 0)
                    cp = cp_col.find_one({"account_name": acc_name, "post_id": pid})
                    cp_col.update_one(
                        {"account_name": acc_name, "post_id": pid},
                        {
                            "$set": {
                                "account_name": acc_name,
                                "post_id": pid,
                                "post_url": post["url"],
                                "post_text": post["text"],
                                "is_own_post": True,
                                "seen_reply_count": rcount,
                                "updated_at": _now_iso(now_ts),
                                "updated_at_ts": now_ts,
                            }
                        },
                        upsert=True,
                    )
                    if rcount <= 0 and pid not in fresh_mention_conv_ids:
                        continue
                    last_rc = safe_int((cp or {}).get("last_reply_count"), -1)
                    last_chk = safe_int((cp or {}).get("last_checked_ts"), 0)
                    last_captured = safe_int((cp or {}).get("last_captured_replies"), -1)
                    pub_ts = safe_int(post.get("publish_ts"), 0)
                    is_recent_48h = (pub_ts <= 0) or ((now_ts - pub_ts) <= 48 * 3600)
                    missing_replies_retry = (
                        rcount > 0
                        and last_captured == 0
                        and is_recent_48h
                        and (now_ts - last_chk) >= min(recheck_sec, 1800)
                    )
                    need_open = (
                        cp is None
                        or last_chk <= 0
                        or rcount != last_rc
                        or (pid in fresh_mention_conv_ids)
                        or missing_replies_retry
                        or (is_recent_48h and (now_ts - last_chk) >= recheck_sec)
                    )
                    if need_open and pid not in queued_ids:
                        targets_to_open.append((pid, post["url"], "own_post"))
                        queued_ids.add(pid)

                # 处理通知中发现的未入库回复（尤其是早于主页前 20 条的帖子或嵌套追评）
                own_posts_with_replies = sum(1 for p in own_posts if safe_int(p.get("reply_count"), 0) > 0)

                for rep in fresh_mention_replies:
                    rid = rep["tweet_id"]
                    conv_id = str(rep.get("conversation_id") or rep.get("in_reply_to_status_id") or "").strip()

                    if conv_id in own_posts_map:
                        # 如果是追评或主帖需要打开，确保主帖或该评论在待查队列中
                        if conv_id not in queued_ids:
                            targets_to_open.append((conv_id, own_posts_map[conv_id]["url"], "own_post"))
                            queued_ids.add(conv_id)
                        elif rep.get("in_reply_to_status_id") != conv_id and rid not in queued_ids:
                            targets_to_open.append((rid, rep["url"], "mention_reply"))
                            queued_ids.add(rid)
                        continue

                    cp = cp_col.find_one({"account_name": acc_name, "post_id": conv_id}) or {}
                    if only_own_roots and cp.get("is_own_post") is False:
                        mentions_skip_other_root += 1
                        continue
                    if only_own_roots and cp.get("is_own_post") is None:
                        owned_info = _resolve_root_post_ownership_zero_auth(
                            conv_id,
                            self_handle_lower=eff_handle,
                            self_uid=self_uid,
                            tweets_by_id=sniffer.tweets_by_id,
                        )
                        if owned_info is not None:
                            is_own, r_txt, r_author = owned_info
                            cp_col.update_one(
                                {"account_name": acc_name, "post_id": conv_id},
                                {
                                    "$set": {
                                        "account_name": acc_name,
                                        "post_id": conv_id,
                                        "is_own_post": is_own,
                                        "post_text": r_txt,
                                        "post_url": f"https://x.com/{r_author or 'i'}/status/{conv_id}",
                                        "updated_at": _now_iso(now_ts),
                                        "updated_at_ts": now_ts,
                                    }
                                },
                                upsert=True,
                            )
                            if not is_own:
                                mentions_skip_other_root += 1
                                continue

                    open_id = rid if rep.get("in_reply_to_status_id") != conv_id else conv_id
                    open_url = rep["url"] if open_id == rid else f"https://x.com/{eff_handle}/status/{conv_id}"
                    if open_id not in queued_ids:
                        targets_to_open.append((open_id, open_url, "mention_detail"))
                        queued_ids.add(open_id)

                details_opened = 0
                halt_status: Optional[str] = None
                halt_warning: Optional[str] = None
                for target_id, target_url, _kind in targets_to_open[:max_details]:
                    if time.monotonic() - lock_t0 >= hold_limit:
                        log.info(f"⏹️ [模块1/持锁止损 | {acc_name}] 已达持锁上限 {hold_limit}s，安全提前放锁。")
                        break
                    prev_det = sniffer.op_counts.get("TweetDetail", 0)
                    try:
                        page.goto(target_url, timeout=45000, wait_until="domcontentloaded")
                        sniffer.wait_for_ops(page, ("TweetDetail",), prev_det, timeout_ms=12000)
                        _wait_x_post_detail_ready(page, timeout_ms=12000)
                        page.wait_for_timeout(random.randint(1200, 2200))
                        _dismiss_overlays(page, aggressive=False, desc="x-reply-detail")
                        det_st, det_msg = inspect_x_account_status(page)
                        if det_st != "ok":
                            halt_status, halt_warning = det_st, det_msg
                            break
                        for _ in range(max(0, safe_int(cfg.get("detail_scroll_rounds"), DETAIL_SCROLL_ROUNDS))):
                            if time.monotonic() - lock_t0 >= hold_limit:
                                break
                            page.mouse.wheel(0, random.randint(1200, 2000))
                            page.wait_for_timeout(random.randint(900, 1500))
                        details_opened += 1
                    except Exception as nav_e:
                        log.warning(f"[模块1/详情跳过 | {acc_name}] {target_url}: {str(nav_e)[:120]}")

                # 汇总所有捕获到的候选回复并幂等入库
                all_candidate_replies: Dict[str, Dict[str, Any]] = {}
                for bucket in sniffer.detail_threads_by_focal.values():
                    for tid, tw in bucket.items():
                        if tw.get("in_reply_to_status_id"):
                            all_candidate_replies[tid] = tw
                for tid, tw in sniffer.mention_tweets_by_id.items():
                    all_candidate_replies.setdefault(tid, tw)

                upsert_stats = upsert_collected_replies(
                    db_obj,
                    account_name=acc_name,
                    self_handle=eff_handle,
                    self_uid=self_uid,
                    tweets_by_id=sniffer.tweets_by_id,
                    candidate_replies=all_candidate_replies.values(),
                    only_my_root_posts=only_own_roots,
                    max_reply_age_hours=max_age_h,
                    max_replies_per_user_per_post=max_per_user,
                    now_ts=int(time.time()),
                )

                # 更新已打开详情的根帖检查点（仅当确实捕获到该根帖本体时才写入根帖信息，避免子回复误标根帖归属）
                chk_now = int(time.time())
                all_my_tweet_ids: Set[str] = {
                    tw["tweet_id"]
                    for tw in sniffer.tweets_by_id.values()
                    if (not tw.get("is_retweet")) and _is_same_account(tw, self_handle_lower=eff_handle, self_uid=self_uid)
                }
                for focal_id, bucket in sniffer.detail_threads_by_focal.items():
                    focal_tw = sniffer.tweets_by_id.get(focal_id) or bucket.get(focal_id)
                    if not focal_tw:
                        continue
                    conv_id = focal_tw.get("conversation_id") or focal_id
                    root_tw = sniffer.tweets_by_id.get(conv_id)
                    if not root_tw:
                        if conv_id == focal_id:
                            root_tw = focal_tw
                        else:
                            continue
                    is_own_root = _is_same_account(root_tw, self_handle_lower=eff_handle, self_uid=self_uid)
                    detail_reply_cnt = sum(
                        1
                        for tw in bucket.values()
                        if _is_reply_targeting_me(
                            tw,
                            my_tweet_ids=all_my_tweet_ids,
                            self_handle_lower=eff_handle,
                            self_uid=self_uid,
                        )
                    )
                    inbox_cnt = inbox_col.count_documents({"account_name": acc_name, "post_id": conv_id})
                    cp_col.update_one(
                        {"account_name": acc_name, "post_id": conv_id},
                        {
                            "$set": {
                                "account_name": acc_name,
                                "post_id": conv_id,
                                "post_url": root_tw["url"],
                                "post_text": root_tw["text"],
                                "is_own_post": is_own_root,
                                "last_reply_count": safe_int(root_tw.get("reply_count"), 0),
                                "last_captured_replies": max(detail_reply_cnt, inbox_cnt),
                                "last_checked_ts": chk_now,
                                "last_checked_at": _now_iso(chk_now),
                                "updated_at": _now_iso(chk_now),
                                "updated_at_ts": chk_now,
                            }
                        },
                        upsert=True,
                    )

                hold_sec = round(time.monotonic() - lock_t0, 1)
                m_parts = []
                if mentions_skip_other_root:
                    m_parts.append(f"非己帖跳过={mentions_skip_other_root}")
                if mentions_skip_old:
                    m_parts.append(f"过旧跳过={mentions_skip_old}")
                if mentions_already_in_db:
                    m_parts.append(f"已在库={mentions_already_in_db}")
                m_suffix = f"({','.join(m_parts)})" if m_parts else ""
                replied_on_x = upsert_stats.get("new_already_replied", 0) + upsert_stats.get("upgraded_replied_on_x", 0)
                skip_suffix = f" | 过滤跳过={upsert_stats['new_skipped']}" if upsert_stats.get("new_skipped") else ""
                log.info(
                    f"✅ [模块1-采集 | @{eff_handle}] 主帖={len(own_posts)}(有评={own_posts_with_replies}) | "
                    f"通知回复={mentions_captured}{m_suffix} | 展详情={details_opened} | "
                    f"新增待生成={upsert_stats.get('new_pending', 0)} | 线上已回={replied_on_x}{skip_suffix} | 持锁={hold_sec}s"
                )
                return {
                    "account_name": acc_name,
                    "screen_name": eff_handle,
                    "status": halt_status or "ok",
                    "warning": halt_warning,
                    "browser_opened": True,
                    "own_posts_scanned": len(own_posts),
                    "own_posts_with_replies": own_posts_with_replies,
                    "mentions_captured": mentions_captured,
                    "mentions_skip_other_root": mentions_skip_other_root,
                    "mentions_skip_old": mentions_skip_old,
                    "mentions_already_in_db": mentions_already_in_db,
                    "details_opened": details_opened,
                    "hold_seconds": hold_sec,
                    **upsert_stats,
                }
            finally:
                try:
                    page.remove_listener("response", sniffer.on_response)
                except Exception:
                    pass
    except UserDataDirLockTimeoutError as lock_e:
        log.warning(f"⏳ [模块1/目录锁等待超时 | {acc_name}] 下轮再试: {lock_e}")
        return {"account_name": acc_name, "status": "lock_timeout", "warning": str(lock_e), "browser_opened": False}


# ==============================================================================
# 四、模块 2：纯查库 AI 生成安全回复 (`generate_replies_from_db`)
# ==============================================================================

def _clean_generated_reply_text(raw_text: Any, reply_author: str = "") -> str:
    txt = str(raw_text or "").strip().strip('"').strip("“”")
    clean_author = _clean_handle(reply_author)
    if clean_author:
        txt = re.sub(rf"^(?:@{re.escape(clean_author)}\s*)+", "", txt, flags=re.IGNORECASE).strip()
    return txt


def _build_parent_context_block(doc: Dict[str, Any]) -> str:
    post_id = str(doc.get("post_id") or "").strip()
    parent_id = str(doc.get("parent_tweet_id") or "").strip()
    parent_txt = str(doc.get("parent_tweet_text") or "").strip() or "（上一层回复正文未捕获）"
    if doc.get("is_own_root_post") is False:
        return f"上述原帖由其他博主发布，你此前在该帖下发表了评论，对方正在回复你的评论：\n{parent_txt}"
    if not parent_id or parent_id == post_id:
        return "该评论直接回复你的上述原帖。"
    return f"该评论是对你此前一条回复的追评，你上一条回复内容为：\n{parent_txt}"


def generate_replies_from_db(
    account: Dict[str, Any],
    *,
    db: Any = None,
    config: Optional[Dict[str, Any]] = None,
    logger: Any = None,
    now_ts: Optional[int] = None,
) -> Dict[str, Any]:
    """【模块 2 入口】纯查库为 `status="pending_generate"` 的评论生成安全自然回复（零浏览器依赖）。"""
    log = logger or _get_logger()
    cfg = {**_default_cfg(), **(config or {})}
    db_obj = db if db is not None else gen_db_object()
    _ensure_indexes(db_obj)

    col = db_obj.get_collection(REPLY_INBOX_COLLECTION)
    acc_name = str(account["name"])
    eff_now = int(now_ts if now_ts is not None else time.time())
    limit = max(1, safe_int(cfg.get("max_generate_per_run"), MAX_GENERATE_PER_RUN))
    max_wchars = min(280, max(20, safe_int(cfg.get("max_reply_weighted_chars"), MAX_REPLY_WEIGHTED_CHARS)))
    model_group = str(cfg.get("generator_model_group") or GENERATOR_MODEL_GROUP).strip() or "low"
    llm_timeout = max(30, safe_int(cfg.get("llm_timeout_seconds"), LLM_TIMEOUT_SECONDS))
    max_retries = max(1, safe_int(cfg.get("max_generate_retries"), MAX_GENERATE_RETRIES))
    retry_cd = max(60, safe_int(cfg.get("retry_cooldown_seconds"), RETRY_COOLDOWN_SECONDS))

    pending_docs = list(
        col.find(
            {
                "account_name": acc_name,
                "status": "pending_generate",
                "generate_next_retry_ts": {"$lte": eff_now},
            }
        )
        .sort("reply_publish_ts", 1)
        .limit(limit)
    )

    if not pending_docs:
        ready_in_db = int(col.count_documents({"account_name": acc_name, "status": "ready_to_reply"}))
        replied_in_db = int(col.count_documents({"account_name": acc_name, "status": "replied"}))
        log.info(f"ℹ️ [模块2-生成 | {acc_name}] 待生成=0（空闲 | 库内待发={ready_in_db}, 已回={replied_in_db}）")
        return {"account_name": acc_name, "status": "ok", "pending_found": 0, "ready_count": 0, "skipped_count": 0, "failed_count": 0}

    ready_cnt = 0
    skipped_cnt = 0
    failed_cnt = 0

    for doc in pending_docs:
        rid = str(doc.get("reply_id") or "")
        acc_handle = str(doc.get("account_handle") or account.get("screen_name") or "me").lstrip("@")
        reply_author = str(doc.get("reply_author") or "").lstrip("@")
        reply_text = str(doc.get("reply_text") or "").strip()
        post_text = str(doc.get("post_text") or "").strip() or "（原帖正文暂无文本）"

        prompt_text, prompt_meta = render_prompt_template(
            PROMPT_TEMPLATE_FILENAME,
            {
                "ACCOUNT_HANDLE": acc_handle,
                "MY_POST_TEXT": post_text,
                "MY_POST_HAS_MEDIA": "是" if doc.get("post_has_media") else "否",
                "PARENT_CONTEXT_BLOCK": _build_parent_context_block(doc),
                "REPLY_AUTHOR": f"@{reply_author}" if reply_author else "读者",
                "REPLY_TEXT": reply_text,
                "REPLY_HAS_MEDIA": "是" if doc.get("reply_has_media") else "否",
                "MAX_WEIGHTED_CHARS": str(max_wchars),
            },
            prompt_dir=AUTO_REPLIER_PROMPT_DIR,
        )

        def _validate_reply_json(data: Dict[str, Any]) -> Optional[str]:
            skip_val = data.get("skip")
            if not isinstance(skip_val, bool):
                return "`skip` 必须为布尔值 true 或 false"
            if skip_val:
                return None
            lang_norm = _norm_lang(data.get("language"))
            if lang_norm == "unknown" or not _LANG_CODE_RE.match(lang_norm):
                return f"`language` ({data.get('language')}) 必须是双字母小写语言代码"
            txt = _clean_generated_reply_text(data.get("reply_text"), reply_author)
            if not txt:
                return "未标记 skip 时 `reply_text` 不能为空"
            if _URL_RE.search(txt):
                return "`reply_text` 严禁包含外部 URL"
            if _HASHTAG_RE.search(txt):
                return "`reply_text` 严禁包含 #Hashtag"
            wlen = calc_x_weighted_length(txt)
            if wlen > max_wchars:
                return f"`reply_text` 加权长度 {wlen} 超过上限 {max_wchars}"
            return None

        parsed, trace = call_llm_json(
            prompt=prompt_text,
            prompt_meta=prompt_meta,
            model=None,
            file_paths=None,
            timeout=llm_timeout,
            preset_model_group=model_group,
            validator=_validate_reply_json,
            task_label=f"auto_replier:{rid}",
            logger_obj=log,
        )

        step_ts = int(time.time())
        step_iso = _now_iso(step_ts)
        if not parsed:
            attempts = safe_int(doc.get("generate_attempts"), 0) + 1
            err_msg = str(trace.get("parse_error") or trace.get("error") or "llm_call_failed")[:240]
            new_status = "failed" if attempts >= max_retries else "pending_generate"
            col.update_one(
                {"account_name": acc_name, "reply_id": rid},
                {
                    "$set": {
                        "status": new_status,
                        "generate_attempts": attempts,
                        "generate_last_error": err_msg,
                        "generate_next_retry_ts": step_ts + retry_cd,
                        "updated_at": step_iso,
                        "updated_at_ts": step_ts,
                    }
                },
            )
            failed_cnt += 1
            log_fn = log.error if new_status == "failed" else log.warning
            log_fn(f"⚠️ [模块2/生成失败 | {acc_name}] reply_id={rid} attempts={attempts}/{max_retries} | {err_msg}")
            continue

        model_used = str(trace.get("model_used") or f"group:{model_group}")
        if parsed.get("skip"):
            reason = str(parsed.get("skip_reason") or "").strip() or "model_skipped"
            col.update_one(
                {"account_name": acc_name, "reply_id": rid},
                {
                    "$set": {
                        "status": "skipped",
                        "skip_reason": f"ai_safety:{reason}",
                        "generator_model": model_used,
                        "generated_at": step_iso,
                        "generated_at_ts": step_ts,
                        "generate_last_error": None,
                        "updated_at": step_iso,
                        "updated_at_ts": step_ts,
                    }
                },
            )
            skipped_cnt += 1
            log.info(f"⏭️ [模块2/安全跳过 | {acc_name}] @{reply_author} reply_id={rid} | reason={reason}")
            continue

        reply_out = _clean_generated_reply_text(parsed.get("reply_text"), reply_author)
        w_len = calc_x_weighted_length(reply_out)
        gen_lang = _norm_lang(parsed.get("language"))

        col.update_one(
            {"account_name": acc_name, "reply_id": rid},
            {
                "$set": {
                    "status": "ready_to_reply",
                    "generated_reply": reply_out,
                    "generated_lang": gen_lang,
                    "generated_weighted_len": w_len,
                    "generator_model": model_used,
                    "generated_at": step_iso,
                    "generated_at_ts": step_ts,
                    "generate_last_error": None,
                    "updated_at": step_iso,
                    "updated_at_ts": step_ts,
                }
            },
        )
        ready_cnt += 1
        log.info(f"✍️ [模块2/生成就绪 | {acc_name}] @{reply_author} reply_id={rid} ({gen_lang}|{w_len}w) -> {reply_out}")

    log.info(
        f"✅ [模块2-生成 | {acc_name}] 待生成={len(pending_docs)} -> 就绪={ready_cnt} | 跳过={skipped_cnt} | 失败={failed_cnt}"
    )
    return {
        "account_name": acc_name,
        "status": "ok",
        "pending_found": len(pending_docs),
        "ready_count": ready_cnt,
        "skipped_count": skipped_cnt,
        "failed_count": failed_cnt,
    }


# ==============================================================================
# 五、模块 3：精准防重执行回复 (`publish_replies_for_account`)
# ==============================================================================

def _like_focal_comment_on_page(page: Any, reply_id: str, logger: Any) -> None:
    try:
        focal = _locate_focal_tweet(page, post_id=reply_id)
        unlike_btn = focal.locator('button[data-testid="unlike"]').first
        if unlike_btn.count() > 0 and unlike_btn.is_visible():
            return
        like_btn = focal.locator('button[data-testid="like"]').first
        if like_btn.count() > 0 and like_btn.is_visible():
            _robust_click(like_btn)
            page.wait_for_timeout(random.randint(400, 900))
    except Exception as exc:
        logger.debug(f"[模块3/点赞略过] reply_id={reply_id}: {exc}")


def publish_replies_for_account(
    account: Dict[str, Any],
    *,
    db: Any = None,
    dry_run: Optional[bool] = None,
    config: Optional[Dict[str, Any]] = None,
    logger: Any = None,
    now_ts: Optional[int] = None,
) -> Dict[str, Any]:
    """【模块 3 入口】扫描 `status="ready_to_reply"` 记录，打开目标评论页二次核验未回复后精准发送。"""
    log = logger or _get_logger()
    cfg = {**_default_cfg(), **(config or {})}
    eff_dry = bool(cfg.get("dry_run", DRY_RUN) if dry_run is None else dry_run)
    db_obj = db if db is not None else gen_db_object()
    _ensure_indexes(db_obj)

    col = db_obj.get_collection(REPLY_INBOX_COLLECTION)
    acc_name = str(account["name"])
    acc_handle = _clean_handle(account.get("screen_name")).lower()
    session_path = str(Path(str(account["user_data_dir"])).expanduser().resolve())

    eff_now = int(now_ts if now_ts is not None else time.time())
    daily_cap = max(0, safe_int(cfg.get("max_replies_per_day"), MAX_REPLIES_PER_DAY))
    per_run_cap = max(1, safe_int(cfg.get("max_replies_per_run"), MAX_REPLIES_PER_RUN))
    max_per_user = max(0, safe_int(cfg.get("max_replies_per_user_per_post"), MAX_REPLIES_PER_USER_PER_POST))
    max_retries = max(1, safe_int(cfg.get("max_reply_retries"), MAX_REPLY_RETRIES))
    retry_cd = max(60, safe_int(cfg.get("retry_cooldown_seconds"), RETRY_COOLDOWN_SECONDS))
    hold_limit = max(30, safe_int(cfg.get("session_max_hold_seconds"), SESSION_MAX_HOLD_SECONDS))
    like_before = bool(cfg.get("like_comment_before_reply", LIKE_COMMENT_BEFORE_REPLY))
    delay_range = cfg.get("action_delay_range", ACTION_DELAY_RANGE)

    today_sent = col.count_documents(
        {
            "account_name": acc_name,
            "status": "replied",
            "replied_source": "auto_replier",
            "replied_at_ts": {"$gte": eff_now - 86400},
        }
    )
    if (not eff_dry) and daily_cap > 0 and today_sent >= daily_cap:
        log.info(f"⏸️ [模块3/每日上限 | {acc_name}] 24h 已回复 {today_sent}/{daily_cap} 条，本轮跳过。")
        return {
            "account_name": acc_name,
            "status": "daily_limit_reached",
            "dry_run": False,
            "browser_opened": False,
            "daily_sent_count": today_sent,
            "replied_count": 0,
            "items": [],
        }

    eff_limit = per_run_cap if (eff_dry or daily_cap <= 0) else min(per_run_cap, max(1, daily_cap - today_sent))
    candidates = list(
        col.find(
            {
                "account_name": acc_name,
                "status": "ready_to_reply",
                "generated_reply": {"$exists": True, "$ne": ""},
                "reply_next_retry_ts": {"$lte": eff_now},
            }
        )
        .sort("reply_publish_ts", 1)
        .limit(max(eff_limit * 3, eff_limit))
    )

    if eff_dry or not candidates:
        preview = [
            {
                "reply_id": d["reply_id"],
                "reply_author": f"@{d.get('reply_author') or ''}",
                "reply_text": d.get("reply_text") or "",
                "generated_reply": d.get("generated_reply") or "",
            }
            for d in candidates[:eff_limit]
        ]
        if eff_dry:
            log.info(f"[模块3/DRY_RUN | {acc_name}] 待发队列={len(candidates)}条 (本轮预览 {len(preview)} 条)")
        else:
            cap_txt = str(daily_cap) if daily_cap > 0 else "∞"
            log.info(f"ℹ️ [模块3-回复 | {acc_name}] 待发送=0（空闲未开浏览器 | 24h已回={today_sent}/{cap_txt}）")
        return {
            "account_name": acc_name,
            "status": "ok",
            "dry_run": eff_dry,
            "browser_opened": False,
            "daily_sent_count": today_sent,
            "candidates_found": len(candidates),
            "replied_count": 0,
            "items": preview,
        }

    if not os.path.isdir(session_path):
        warn = f"浏览器会话目录不存在: {session_path}"
        log.error(f"🚨 [模块3/目录缺失 | {acc_name}] {warn}")
        return {"account_name": acc_name, "status": "session_missing", "warning": warn, "browser_opened": False}

    proxy_server = get_proxy_url()
    sniffer = XReplyInboxSniffer()
    sent_items: List[Dict[str, Any]] = []
    skipped_cnt = 0
    failed_cnt = 0
    halt_status: Optional[str] = None
    halt_warning: Optional[str] = None

    try:
        with persistent_browser_session(
            session_path,
            debug=bool(cfg.get("browser_visible", BROWSER_VISIBLE)),
            proxy=proxy_server,
            use_stealth_chrome=True,
            reuse_existing_page=True,
            default_timeout_ms=45000,
            navigation_timeout_ms=45000,
            install_guard=True,
        ) as (context, page):
            lock_t0 = time.monotonic()
            page.on("response", sniffer.on_response)
            try:
                page.goto(X_HOME_URL, timeout=45000, wait_until="domcontentloaded")
                _wait_x_home_ready(page, timeout_ms=18000)
                page.wait_for_timeout(random.randint(1200, 2200))
                _dismiss_overlays(page, aggressive=False, desc="x-replier-home")
                check_for_crash(page)

                home_st, home_msg = inspect_x_account_status(page)
                if home_st != "ok":
                    log.error(f"🚨 [模块3/账号阻断 | {acc_name}] status={home_st} | {home_msg}")
                    return {"account_name": acc_name, "status": home_st, "warning": home_msg, "browser_opened": True}

                self_uid, eff_handle = _resolve_session_identity(context, page, acc_handle)

                live_actions = 0
                for doc in candidates:
                    if len(sent_items) >= eff_limit:
                        break
                    if time.monotonic() - lock_t0 >= hold_limit:
                        log.info(f"⏹️ [模块3/持锁止损 | {acc_name}] 已达持锁上限 {hold_limit}s，安全提前放锁。")
                        break

                    rid = str(doc.get("reply_id") or "").strip()
                    post_id = str(doc.get("post_id") or "").strip()
                    reply_author_low = str(doc.get("reply_author_lower") or "").strip()

                    # 防重层 1：发送前重新读库核验状态与同帖单用户回复上限
                    fresh = col.find_one({"account_name": acc_name, "reply_id": rid})
                    if not fresh or fresh.get("status") != "ready_to_reply":
                        continue
                    reply_text = str(fresh.get("generated_reply") or "").strip()
                    if not reply_text:
                        continue
                    if max_per_user > 0 and reply_author_low:
                        already_user_cnt = col.count_documents(
                            {
                                "account_name": acc_name,
                                "post_id": post_id,
                                "reply_author_lower": reply_author_low,
                                "status": "replied",
                            }
                        )
                        if already_user_cnt >= max_per_user:
                            col.update_one(
                                {"account_name": acc_name, "reply_id": rid},
                                {
                                    "$set": {
                                        "status": "skipped",
                                        "skip_reason": "max_replies_per_user_per_post_reached",
                                        "updated_at": _now_iso(),
                                        "updated_at_ts": int(time.time()),
                                    }
                                },
                            )
                            skipped_cnt += 1
                            continue

                    if live_actions > 0:
                        _sleep_jitter(delay_range)

                    reply_url = str(fresh.get("reply_url") or f"https://x.com/i/status/{rid}")
                    prev_det = sniffer.op_counts.get("TweetDetail", 0)
                    resp = page.goto(reply_url, timeout=45000, wait_until="domcontentloaded")
                    sniffer.wait_for_ops(page, ("TweetDetail",), prev_det, timeout_ms=12000)
                    _wait_x_post_detail_ready(page, timeout_ms=15000)
                    page.wait_for_timeout(random.randint(1500, 2600))
                    _dismiss_overlays(page, aggressive=False, desc="x-replier-detail")

                    step_st, step_msg = inspect_x_account_status(page)
                    if step_st != "ok":
                        halt_status, halt_warning = step_st, step_msg
                        log.error(f"🚨 [模块3/发送中触发验证 | {acc_name}] {step_msg}")
                        break

                    if resp and resp.status in (404, 410):
                        col.update_one(
                            {"account_name": acc_name, "reply_id": rid},
                            {"$set": {"status": "unavailable", "skip_reason": f"http_{resp.status}", "updated_at": _now_iso(), "updated_at_ts": int(time.time())}},
                        )
                        skipped_cnt += 1
                        continue

                    post_gone = False
                    for err_sel in ('div[data-testid="error-detail"]', 'div[data-testid="empty_state_header_text"]'):
                        try:
                            loc = page.locator(err_sel).first
                            if loc.count() > 0 and loc.is_visible():
                                post_gone = True
                                break
                        except Exception:
                            pass
                    if post_gone:
                        col.update_one(
                            {"account_name": acc_name, "reply_id": rid},
                            {"$set": {"status": "unavailable", "skip_reason": "reply_deleted_or_unavailable", "updated_at": _now_iso(), "updated_at_ts": int(time.time())}},
                        )
                        skipped_cnt += 1
                        continue

                    # 防重层 2：检查实时抓到的 TweetDetail 中是否已有己方回复（防止人工已回复或历史重复）
                    live_dup = sniffer.find_my_existing_reply(rid, self_handle_lower=eff_handle, self_uid=self_uid)
                    if live_dup:
                        dup_ts = live_dup.get("publish_ts") or int(time.time())
                        col.update_one(
                            {"account_name": acc_name, "reply_id": rid},
                            {
                                "$set": {
                                    "status": "replied",
                                    "replied_tweet_id": live_dup["tweet_id"],
                                    "replied_url": live_dup["url"],
                                    "replied_text": live_dup.get("reply_body_text") or live_dup.get("text") or "",
                                    "replied_source": "detected_on_x",
                                    "replied_at": _now_iso(dup_ts),
                                    "replied_at_ts": dup_ts,
                                    "updated_at": _now_iso(),
                                    "updated_at_ts": int(time.time()),
                                }
                            },
                        )
                        skipped_cnt += 1
                        log.info(f"🛡️ [模块3/线上防重命中 | {acc_name}] reply_id={rid} 已有己方回复 {live_dup['tweet_id']}，跳过发送。")
                        continue

                    inline_box = page.locator('div[data-testid="tweetTextarea_0"], div[data-testid="tweetTextarea_0_label"]').first
                    if inline_box.count() == 0 or not inline_box.is_visible():
                        focal_tweet = _locate_focal_tweet(page, post_id=rid)
                        reply_btn = focal_tweet.locator('button[data-testid="reply"]').first
                        if reply_btn.count() > 0 and (
                            reply_btn.is_disabled() or str(reply_btn.get_attribute("aria-disabled") or "").lower() == "true"
                        ):
                            col.update_one(
                                {"account_name": acc_name, "reply_id": rid},
                                {"$set": {"status": "unavailable", "skip_reason": "reply_restricted", "updated_at": _now_iso(), "updated_at_ts": int(time.time())}},
                            )
                            skipped_cnt += 1
                            continue

                    try:
                        if like_before:
                            _like_focal_comment_on_page(page, rid, log)
                        reply_container = _locate_x_reply_box(page, post_id=rid)
                        new_reply_id = _submit_x_post(
                            page,
                            reply_container,
                            content=reply_text,
                            image_path_list=None,
                            text_list=None,
                            prepared_images=[],
                        )
                        live_actions += 1
                    except BusinessErrorException as biz_e:
                        done_ts = int(time.time())
                        err_s = f"business_blocked:{biz_e}"[:240]
                        attempts = safe_int(fresh.get("reply_attempts"), 0) + 1
                        col.update_one(
                            {"account_name": acc_name, "reply_id": rid},
                            {
                                "$set": {
                                    "status": "failed" if attempts >= max_retries else "ready_to_reply",
                                    "reply_attempts": attempts,
                                    "reply_last_error": err_s,
                                    "reply_next_retry_ts": done_ts + retry_cd,
                                    "updated_at": _now_iso(done_ts),
                                    "updated_at_ts": done_ts,
                                }
                            },
                        )
                        halt_status, halt_warning = "cooldown_required", err_s
                        log.error(f"🚨 [模块3/业务风控阻断 | {acc_name}] reply_id={rid} | {err_s}")
                        break
                    except Exception as send_e:
                        done_ts = int(time.time())
                        err_s = str(send_e)[:240]
                        attempts = safe_int(fresh.get("reply_attempts"), 0) + 1
                        new_st = "failed" if attempts >= max_retries else "ready_to_reply"
                        col.update_one(
                            {"account_name": acc_name, "reply_id": rid},
                            {
                                "$set": {
                                    "status": new_st,
                                    "reply_attempts": attempts,
                                    "reply_last_error": err_s,
                                    "reply_next_retry_ts": done_ts + retry_cd,
                                    "updated_at": _now_iso(done_ts),
                                    "updated_at_ts": done_ts,
                                }
                            },
                        )
                        failed_cnt += 1
                        log.warning(f"⚠️ [模块3/发送失败 | {acc_name}] reply_id={rid} ({attempts}/{max_retries}) | {err_s}")
                        continue

                    done_ts = int(time.time())
                    done_iso = _now_iso(done_ts)
                    replied_url = f"https://x.com/{eff_handle or 'i'}/status/{new_reply_id}" if new_reply_id else None
                    col.update_one(
                        {"account_name": acc_name, "reply_id": rid},
                        {
                            "$set": {
                                "status": "replied",
                                "replied_tweet_id": new_reply_id,
                                "replied_url": replied_url,
                                "replied_text": reply_text,
                                "replied_source": "auto_replier",
                                "replied_at": done_iso,
                                "replied_at_ts": done_ts,
                                "reply_last_error": None,
                                "updated_at": done_iso,
                                "updated_at_ts": done_ts,
                            }
                        },
                    )
                    sent_items.append(
                        {
                            "reply_id": rid,
                            "reply_author": f"@{fresh.get('reply_author') or ''}",
                            "replied_tweet_id": new_reply_id,
                            "replied_text": reply_text,
                            "replied_url": replied_url,
                        }
                    )
                    log.info(
                        f"💬 [模块3/精准回复成功 | @{eff_handle or acc_name}] 对 @{fresh.get('reply_author')}({rid}) -> "
                        f"new_id={new_reply_id} | {reply_text}"
                    )

                hold_sec = round(time.monotonic() - lock_t0, 1)
                cap_txt = str(daily_cap) if daily_cap > 0 else "∞"
                log.info(
                    f"✅ [模块3-回复 | @{eff_handle or acc_name}] 候选={len(candidates)} -> 已回={len(sent_items)} | "
                    f"跳过={skipped_cnt} | 失败={failed_cnt} | 24h累计={today_sent + len(sent_items)}/{cap_txt} | 持锁={hold_sec}s"
                )
                return {
                    "account_name": acc_name,
                    "screen_name": eff_handle,
                    "status": halt_status or "ok",
                    "warning": halt_warning,
                    "dry_run": False,
                    "browser_opened": True,
                    "daily_sent_count": today_sent + len(sent_items),
                    "candidates_found": len(candidates),
                    "replied_count": len(sent_items),
                    "skipped_count": skipped_cnt,
                    "failed_count": failed_cnt,
                    "hold_seconds": hold_sec,
                    "items": sent_items,
                }
            finally:
                try:
                    page.remove_listener("response", sniffer.on_response)
                except Exception:
                    pass
    except UserDataDirLockTimeoutError as lock_e:
        log.warning(f"⏳ [模块3/目录锁等待超时 | {acc_name}] 下轮再试: {lock_e}")
        return {"account_name": acc_name, "status": "lock_timeout", "warning": str(lock_e), "browser_opened": False}


# ==============================================================================
# 六、状态看板与三工人独立并行常驻调度 (`run_parallel_daemon`)
# ==============================================================================

def build_status_report(
    db: Any,
    accounts: Sequence[Dict[str, Any]],
    *,
    now_ts: Optional[int] = None,
) -> Dict[str, Any]:
    _ensure_indexes(db)
    col = db.get_collection(REPLY_INBOX_COLLECTION)
    cp_col = db.get_collection(POST_CHECKPOINTS_COLLECTION)
    eff_now = int(now_ts if now_ts is not None else time.time())

    acc_reports: List[Dict[str, Any]] = []
    for acc in accounts:
        name = str(acc["name"])
        counts = {
            st: int(col.count_documents({"account_name": name, "status": st}))
            for st in ("pending_generate", "ready_to_reply", "replied", "skipped", "unavailable", "failed")
        }
        sent_24h = int(
            col.count_documents(
                {
                    "account_name": name,
                    "status": "replied",
                    "replied_source": "auto_replier",
                    "replied_at_ts": {"$gte": eff_now - 86400},
                }
            )
        )
        checkpoints = int(cp_col.count_documents({"account_name": name, "is_own_post": True}))
        recent_docs = sorted(
            list(col.find({"account_name": name, "status": {"$in": ["ready_to_reply", "replied"]}}, {"_id": 0})),
            key=lambda d: max(safe_int(d.get("replied_at_ts"), 0), safe_int(d.get("updated_at_ts"), 0)),
            reverse=True,
        )[:5]
        acc_reports.append(
            {
                "account_name": name,
                "screen_name": _clean_handle(acc.get("screen_name")),
                "checkpoints": checkpoints,
                "sent_24h": sent_24h,
                "counts": counts,
                "recent": [
                    {
                        "status": d.get("status"),
                        "reply_author": f"@{d.get('reply_author') or ''}",
                        "reply_text": (d.get("reply_text") or "")[:60],
                        "generated_reply": (d.get("generated_reply") or d.get("replied_text") or "")[:80],
                    }
                    for d in recent_docs
                ],
            }
        )
    return {"generated_at": _now_iso(eff_now), "accounts": acc_reports}


def format_status_text(report: Dict[str, Any]) -> str:
    lines = ["=" * 76, f"X 自动评论回复看板 | {report['generated_at']}", "-" * 76]
    for a in report.get("accounts") or []:
        c = a["counts"]
        lines.append(
            f"[{a['account_name']} @{a['screen_name'] or '自动识别'}] 已盯主帖={a['checkpoints']} | 24h已自动回={a['sent_24h']} | "
            f"待生成={c['pending_generate']} | 待发送={c['ready_to_reply']} | 已回复={c['replied']} | "
            f"跳过={c['skipped']} | 失效={c['unavailable']} | 失败={c['failed']}"
        )
        for item in a["recent"]:
            lines.append(
                f"   - [{item['status']}] {item['reply_author']}: {item['reply_text']} -> {item['generated_reply']}"
            )
    lines.append("=" * 76)
    return "\n".join(lines)


def _run_worker_with_initial_delay(
    *,
    worker_name: str,
    initial_delay: int,
    interval_seconds: int,
    jitter_range: Tuple[int, int],
    step_fn: Any,
    stop_event: threading.Event,
    max_rounds: int,
    logger: Any,
) -> None:
    if initial_delay > 0:
        if not interruptible_sleep(initial_delay, stop_event=stop_event):
            return
    run_daemon_loop(
        step_fn,
        interval_seconds=max(60, int(interval_seconds)),
        additive_jitter_range=jitter_range,
        name=worker_name,
        logger_obj=logger,
        max_rounds=max_rounds,
        stop_event=stop_event,
    )


def run_parallel_daemon(
    accounts: Optional[Sequence[Dict[str, Any]]] = None,
    *,
    stop_event: Optional[threading.Event] = None,
    max_rounds: int = 0,
    config: Optional[Dict[str, Any]] = None,
) -> None:
    """【默认常驻入口】并行启动 3 个独立工人线程，按各自固定间隔独立循环执行。"""
    log = _get_logger()
    cfg = {**_default_cfg(), **(config or {})}
    pool = resolve_accounts(accounts)
    if not pool:
        log.error("🚨 [AutoReplier] ACCOUNTS 中没有启用的有效账号，退出。")
        return

    ev = stop_event if stop_event is not None else threading.Event()
    risk_cd_sec = max(300, safe_int(cfg.get("risk_cooldown_seconds"), RISK_COOLDOWN_SECONDS))
    collect_cooldown_until: Dict[str, float] = {}
    reply_cooldown_until: Dict[str, float] = {}

    collect_interval = max(60, safe_int(cfg.get("collect_interval_seconds"), 3600))
    collect_jitter = _parse_jitter_pair(cfg.get("collect_jitter_seconds"))
    generate_interval = max(60, safe_int(cfg.get("generate_interval_seconds"), 3600))
    generate_jitter = _parse_jitter_pair(cfg.get("generate_jitter_seconds"))
    reply_interval = max(60, safe_int(cfg.get("reply_interval_seconds"), 3600))
    reply_jitter = _parse_jitter_pair(cfg.get("reply_jitter_seconds"))

    def _schedule_next_sleep(
        worker_label: str,
        round_no: int,
        base_sec: int,
        jitter_pair: Tuple[int, int],
        *,
        override_base: Optional[int] = None,
        reason: str = "",
    ) -> Dict[str, Any]:
        eff_base = override_base if override_base is not None else base_sec
        sleep_sec = int(round(compute_jittered_interval(eff_base, additive_jitter_range=jitter_pair)))
        if max_rounds == 0 or round_no < max_rounds:
            next_at = (datetime.now(TZ_CN) + timedelta(seconds=sleep_sec)).strftime("%H:%M:%S")
            mins_txt = f"{round(sleep_sec / 60, 1):g}分钟" if sleep_sec >= 60 else f"{sleep_sec}秒"
            reason_txt = f" | {reason}" if reason else ""
            log.info(f"⏰ [{worker_label} | #{round_no}] 下次执行 {next_at} (约{mins_txt}后{reason_txt})")
        return {"next_sleep_seconds": sleep_sec}

    def _collector_step(round_no: int) -> Dict[str, Any]:
        db = gen_db_object()
        any_empty = False
        for acc in pool:
            acc_dir, acc_name = acc["user_data_dir"], acc["name"]
            rem = collect_cooldown_until.get(acc_dir, 0.0) - time.time()
            if rem > 0:
                log.info(f"⏸️ [工人1-采集 | #{round_no} | {acc_name}] 独立风控冷却中（剩余 {int(rem)}s），跳过。")
                continue
            res = collect_replies_for_account(acc, db=db, config=cfg, logger=log)
            st = str(res.get("status") or "ok")
            if st in _RISK_HALT_STATUSES:
                collect_cooldown_until[acc_dir] = time.time() + risk_cd_sec
                log.error(f"⏳ [工人1-采集 | {acc_name}] 触发独立风控冷却 {risk_cd_sec}s | status={st}")
            elif st == "empty_capture":
                any_empty = True
            elif st == "ok":
                collect_cooldown_until.pop(acc_dir, None)
        if any_empty:
            return _schedule_next_sleep(
                "工人1-采集",
                round_no,
                collect_interval,
                collect_jitter,
                override_base=min(collect_interval, 600),
                reason="空采重试",
            )
        active_rems = [max(0.0, collect_cooldown_until.get(a["user_data_dir"], 0.0) - time.time()) for a in pool]
        if pool and all(r > 0 for r in active_rems):
            return _schedule_next_sleep(
                "工人1-采集",
                round_no,
                collect_interval,
                collect_jitter,
                override_base=max(collect_interval, int(min(active_rems))),
                reason="风控冷却",
            )
        return _schedule_next_sleep("工人1-采集", round_no, collect_interval, collect_jitter)

    def _generator_step(round_no: int) -> Dict[str, Any]:
        db = gen_db_object()
        for acc in pool:
            generate_replies_from_db(acc, db=db, config=cfg, logger=log)
        return _schedule_next_sleep("工人2-生成", round_no, generate_interval, generate_jitter)

    def _replier_step(round_no: int) -> Dict[str, Any]:
        db = gen_db_object()
        for acc in pool:
            acc_dir, acc_name = acc["user_data_dir"], acc["name"]
            rem = reply_cooldown_until.get(acc_dir, 0.0) - time.time()
            if rem > 0:
                log.info(f"⏸️ [工人3-回复 | #{round_no} | {acc_name}] 独立风控冷却中（剩余 {int(rem)}s），跳过。")
                continue
            res = publish_replies_for_account(acc, db=db, config=cfg, logger=log)
            st = str(res.get("status") or "ok")
            if st in _RISK_HALT_STATUSES:
                reply_cooldown_until[acc_dir] = time.time() + risk_cd_sec
                log.error(f"⏳ [工人3-回复 | {acc_name}] 触发独立风控冷却 {risk_cd_sec}s | status={st}")
            elif st == "ok":
                reply_cooldown_until.pop(acc_dir, None)
        active_rems = [max(0.0, reply_cooldown_until.get(a["user_data_dir"], 0.0) - time.time()) for a in pool]
        if pool and all(r > 0 for r in active_rems):
            return _schedule_next_sleep(
                "工人3-回复",
                round_no,
                reply_interval,
                reply_jitter,
                override_base=max(reply_interval, int(min(active_rems))),
                reason="风控冷却",
            )
        return _schedule_next_sleep("工人3-回复", round_no, reply_interval, reply_jitter)

    worker_specs = []
    if bool(cfg.get("enable_collector", True)):
        worker_specs.append(
            (
                "ReplyCollector",
                max(0, safe_int(cfg.get("collect_initial_delay_seconds"), 0)),
                collect_interval,
                collect_jitter,
                _collector_step,
            )
        )
    if bool(cfg.get("enable_generator", True)):
        worker_specs.append(
            (
                "ReplyGenerator",
                max(0, safe_int(cfg.get("generate_initial_delay_seconds"), 30)),
                generate_interval,
                generate_jitter,
                _generator_step,
            )
        )
    if bool(cfg.get("enable_replier", True)):
        worker_specs.append(
            (
                "ReplyPublisher",
                max(0, safe_int(cfg.get("reply_initial_delay_seconds"), 60)),
                reply_interval,
                reply_jitter,
                _replier_step,
            )
        )

    scope_desc = "仅本人主帖" if bool(cfg.get("only_my_root_posts", ONLY_MY_ROOT_POSTS)) else "本人主帖+他帖追评"
    log.info(
        f"🚀 [AutoReplier/三工人并行启动] 账号={[a['name'] + '(@' + (a['screen_name'] or '自动') + ')' for a in pool]} | "
        f"范围={scope_desc} | 启动工人={[w[0] for w in worker_specs]} | 各自独立并行周期="
        f"[{', '.join(f'{w[0]}:{w[2]}s(延迟{w[1]}s)' for w in worker_specs)}]"
    )

    threads: List[threading.Thread] = []
    for w_name, init_delay, base_sec, jitter_pair, fn in worker_specs:
        t = threading.Thread(
            target=_run_worker_with_initial_delay,
            kwargs={
                "worker_name": w_name,
                "initial_delay": init_delay,
                "interval_seconds": base_sec,
                "jitter_range": jitter_pair,
                "step_fn": fn,
                "stop_event": ev,
                "max_rounds": max_rounds,
                "logger": log,
            },
            name=w_name,
            daemon=True,
        )
        t.start()
        threads.append(t)

    try:
        while any(t.is_alive() for t in threads):
            if ev.wait(timeout=1.0):
                break
    except KeyboardInterrupt:
        log.info("🛑 [AutoReplier] 收到手动中断信号，正在安全停止全部工人线程...")
        ev.set()
    finally:
        for t in threads:
            t.join(timeout=10.0)


def main(mode: Optional[str] = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    eff_mode = (mode or RUN_MODE).strip().lower()
    log = _get_logger()
    pool = resolve_accounts()

    if eff_mode == "login":
        for acc in pool:
            Path(acc["user_data_dir"]).mkdir(parents=True, exist_ok=True)
            open_browser_for_manual_use(user_data_dir=acc["user_data_dir"], home_url=X_HOME_URL)
        return 0

    db = gen_db_object()
    if eff_mode == "status":
        print(format_status_text(build_status_report(db, pool)))
        return 0

    if eff_mode == "preview":
        for acc in pool:
            res = publish_replies_for_account(acc, db=db, dry_run=True)
            print(f"[预览 | {acc['name']}] 待发送={res.get('candidates_found', 0)} | 明细={res.get('items', [])}")
        print(format_status_text(build_status_report(db, pool)))
        return 0

    if eff_mode == "collect_once":
        for acc in pool:
            collect_replies_for_account(acc, db=db, logger=log)
        print(format_status_text(build_status_report(db, pool)))
        return 0

    if eff_mode == "generate_once":
        for acc in pool:
            generate_replies_from_db(acc, db=db, logger=log)
        print(format_status_text(build_status_report(db, pool)))
        return 0

    if eff_mode == "reply_once":
        for acc in pool:
            publish_replies_for_account(acc, db=db, logger=log)
        print(format_status_text(build_status_report(db, pool)))
        return 0

    if eff_mode == "once_all":
        for acc in pool:
            collect_replies_for_account(acc, db=db, logger=log)
            generate_replies_from_db(acc, db=db, logger=log)
            publish_replies_for_account(acc, db=db, logger=log)
        print(format_status_text(build_status_report(db, pool)))
        return 0

    run_parallel_daemon(accounts=pool)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(RUN_MODE))
