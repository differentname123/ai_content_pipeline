# -*- coding: utf-8 -*-
"""
轻量级独立 X 账号状态无损探测器 (`platforms/x_account_probe.py`)

定位：
- 仅使用只读观察者小号会话（固定读取 `x_xiaohao_browser_session_dir`）+ 免登公开资料接口，
  在不登陆、不打开、不写入 `x_dahao`（`@2434954452qqc`）与 `x_myself`（`@peterzhu1003`）的前提下，
  感知两个被观察账号是否仍在全站搜索（`from:<handle>&f=live`）中恢复可见、以及主资料是否存活。
- 默认以前端零弹窗无头模式（`HEADLESS=True`）+ 低占用常驻守护模式（`RUN_MODE="daemon"`，默认每 6 小时 1 轮）运行：
  每轮约耗时十几秒，查完立刻关闭 Chrome 进程（闲置内存仅 ~25 MB），全部结论滚动记录到 `logs/x_account_probe.log`。
- 自带 Fail-Closed 空白对照校准：当被测号显示无搜索结果时，自动用 `NASA` 验证观察者搜索功能本身正常，
  绝不把小号掉登录 / 代理超时 / 接口限流误判为被测账号仍处于 `SEARCH_BAN`。
"""
from datetime import datetime, timedelta, timezone
import os
import re
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
from urllib.parse import quote

import requests

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.common_utils import (
    DEFAULT_SESSION_ROOT,
    get_config,
    get_proxy_url,
    resolve_cross_platform_path,
    resolve_session_dir,
    setup_logger,
)
from common.daemon import run_daemon_loop
from common.post_utils import AccountIsolationError, assert_not_x_myself_dir
from platforms.x_browser import (
    UserDataDirLockTimeoutError,
    XBrowserSession,
    _user_data_dir_lock,
    human_pause,
    inspect_x_page_health,
    sync_playwright,
)

logger = setup_logger(app_name="x_account_probe")
TZ_CN = timezone(timedelta(hours=8))

# ==============================================================================
# 一、常用配置区（改完保存后直接运行本脚本即可，无需命令行参数）
# ==============================================================================

# 运行模式：
#   - "daemon" : 常驻后台定时巡检（启动先立刻跑 1 轮，之后按周期静默循环，推荐）
#   - "once"   : 仅运行 1 轮后立刻退出
RUN_MODE: str = "daemon"

# True = 后台静默无窗口运行（绝不弹出桌面 Chrome 干扰正常办公）；False = 显示浏览器窗口调试
HEADLESS: bool = True

# 常驻巡检周期（小时）：默认每 6 小时 1 轮（如需每小时检查一次，可改为 1.0）
PROBE_INTERVAL_HOURS: float = 6.0
PROBE_JITTER_RATIO: float = 0.08

# 若观察者小号本身触发登录失效或验证拦截，自动加长退避时长（小时），避免反复撞墙
OBSERVER_ERROR_BACKOFF_HOURS: float = 12.0

# 只读观察者小号目录配置键（严禁指向 x_myself 或 x_dahao）
OBSERVER_SESSION_CONFIG_KEY: str = "x_xiaohao_browser_session_dir"
OBSERVER_SESSION_FALLBACK_NAME: str = "x_xiaohao"

# 是否走全局代理、是否先做免登公开主页检查
USE_PROXY: bool = True
CHECK_PUBLIC_PROFILE: bool = True

# 空白对照账号（仅当任一被测号无搜索结果时才访问 1 次，用于确认观察者搜索本身可用）
CONTROL_HANDLE: str = "NASA"

# 轻量超时与查询间距参数
SEARCH_WAIT_SECONDS: float = 12.0
BETWEEN_QUERY_PAUSE_MS: Tuple[int, int] = (2500, 4500)
PROFILE_HTTP_TIMEOUT_SEC: int = 10
BROWSER_LOCK_TIMEOUT_SEC: int = 5


def _clean_handle(raw: Any) -> str:
    return str(raw or "").strip().lstrip("@").strip()


def get_default_targets() -> List[Tuple[str, str]]:
    """从项目配置解析被观察账号列表 `(别名, handle)`。"""
    dahao = _clean_handle(get_config("x_dahao_screen_name", "2434954452qqc")) or "2434954452qqc"
    myself = _clean_handle(get_config("x_myself_screen_name", "peterzhu1003")) or "peterzhu1003"
    return [
        ("x_dahao", dahao),
        ("x_myself", myself),
    ]


TARGET_ACCOUNTS: List[Tuple[str, str]] = get_default_targets()


# ==============================================================================
# 二、物理隔离守卫（严禁触碰被观察账号目录）
# ==============================================================================

def assert_safe_probe_session_dir(user_data_dir: Optional[str]) -> str:
    """
    强制校验探测脚本使用的浏览器目录绝不是被观察账号目录（`x_myself` / `x_dahao`）。
    """
    # 【保留过滤原因说明】严禁只读状态探测器触碰被限流/静养中的 x_myself 或 x_dahao 目录，避免并发目录锁或触发写前风控升级。
    raw = str(user_data_dir or "").strip()
    if not raw:
        raise AccountIsolationError(
            "安全隔离红线拦截 [platforms.x_account_probe]：观察者会话目录不能为空，必须使用独立小号目录（如 x_xiaohao）！"
        )

    assert_not_x_myself_dir(raw, context="platforms.x_account_probe")

    norm_raw = raw.replace("\\", "/").lower()
    norm_abs = str(Path(raw).resolve()).replace("\\", "/").lower()
    parts_raw = {p.lower() for p in Path(norm_raw).parts if p}
    parts_abs = {p.lower() for p in Path(norm_abs).parts if p}
    if (
        "x_dahao" in parts_raw
        or "x_dahao" in parts_abs
        or "/x_dahao" in norm_raw
        or "/x_dahao" in norm_abs
        or "x_dahao" in norm_raw
    ):
        raise AccountIsolationError(
            f"安全隔离红线拦截 [platforms.x_account_probe]：禁止状态探测器触碰大号目录 `{raw}` (x_dahao)！"
        )
    return raw


# ==============================================================================
# 三、免登公开资料检查 + 搜索响应解析
# ==============================================================================

def fetch_public_profile_status(
    handle: str,
    *,
    proxy_url: Optional[str] = None,
    timeout: int = PROFILE_HTTP_TIMEOUT_SEC,
) -> Dict[str, Any]:
    """
    零浏览器检查目标账号的公开主页是否仍存在/活跃（基于 fxtwitter 免登公开端点）。
    返回 `profile_state`: `"ALIVE"` | `"SUSPECT_UNAVAILABLE"` | `"ERROR"`。
    注：第三方返回 404/unavailable 仅记为 `SUSPECT_UNAVAILABLE`，需由一等 X 页面复核后才允许判定冻结。
    """
    clean = _clean_handle(handle)
    if not clean:
        return {
            "handle": "",
            "profile_state": "ERROR",
            "followers": None,
            "following": None,
            "tweets": None,
            "detail": "empty handle",
        }

    resolved_proxy = get_proxy_url() if proxy_url is None else str(proxy_url).strip()
    proxies = {"http": resolved_proxy, "https": resolved_proxy} if resolved_proxy else None

    try:
        resp = requests.get(
            f"https://api.fxtwitter.com/{clean}",
            proxies=proxies,
            timeout=int(timeout),
            headers={"User-Agent": "Mozilla/5.0"},
        )
        data: Dict[str, Any] = {}
        try:
            parsed = resp.json()
            if isinstance(parsed, dict):
                data = parsed
        except Exception:
            data = {}

        code = data.get("code")
        msg = str(data.get("message") or "").strip()
        user = data.get("user") if isinstance(data.get("user"), dict) else None

        if resp.status_code == 200 and code == 200 and user is not None:
            return {
                "handle": clean,
                "profile_state": "ALIVE",
                "followers": user.get("followers"),
                "following": user.get("following"),
                "tweets": user.get("tweets"),
                "detail": "HTTP 200",
            }

        low_msg = msg.lower()
        if (
            resp.status_code == 404
            or code == 404
            or any(tok in low_msg for tok in ("not found", "suspended", "does not exist"))
        ):
            return {
                "handle": clean,
                "profile_state": "SUSPECT_UNAVAILABLE",
                "followers": None,
                "following": None,
                "tweets": None,
                "detail": msg or f"HTTP {resp.status_code}",
            }

        return {
            "handle": clean,
            "profile_state": "ERROR",
            "followers": None,
            "following": None,
            "tweets": None,
            "detail": msg or f"HTTP {resp.status_code}",
        }
    except Exception as e:
        return {
            "handle": clean,
            "profile_state": "ERROR",
            "followers": None,
            "following": None,
            "tweets": None,
            "detail": str(e),
        }


def _extract_tweet_author_handle(tweet_obj: Dict[str, Any]) -> str:
    core = tweet_obj.get("core")
    if not isinstance(core, dict):
        return ""
    user_results = core.get("user_results")
    if not isinstance(user_results, dict):
        return ""
    res = user_results.get("result")
    if not isinstance(res, dict):
        return ""

    # X GraphQL 兼容：`screen_name` 可能在 `legacy` 下，也可能直接出现在新版 `core` 下
    for key in ("legacy", "core"):
        sub = res.get(key)
        if isinstance(sub, dict) and sub.get("screen_name"):
            return _clean_handle(sub.get("screen_name"))
    return ""


def _unwrap_tweet_result(node: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(node, dict):
        return None
    if node.get("__typename") == "TweetWithVisibilityResults" and isinstance(node.get("tweet"), dict):
        node = node["tweet"]
    if isinstance(node, dict) and (node.get("rest_id") or isinstance(node.get("legacy"), dict)):
        return node
    return None


def extract_search_timeline_tweets(payload: Any, target_handle: str) -> List[Dict[str, str]]:
    """
    从 X `SearchTimeline` GraphQL 响应中递归提取目标作者的推文条目（自动兼容包装层与去重）。
    """
    target_lower = _clean_handle(target_handle).lower()
    if not target_lower or payload is None:
        return []

    found: List[Dict[str, str]] = []
    seen_ids = set()
    stack: List[Any] = [payload]

    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            tweet_results = cur.get("tweet_results")
            candidate = (
                _unwrap_tweet_result(tweet_results.get("result"))
                if isinstance(tweet_results, dict)
                else None
            )
            if candidate is None and "core" in cur and "legacy" in cur:
                candidate = _unwrap_tweet_result(cur)

            if candidate is not None:
                author = _extract_tweet_author_handle(candidate).lower()
                if author == target_lower:
                    legacy = candidate.get("legacy") if isinstance(candidate.get("legacy"), dict) else {}
                    tweet_id = str(candidate.get("rest_id") or legacy.get("id_str") or "").strip()
                    if tweet_id and tweet_id not in seen_ids:
                        seen_ids.add(tweet_id)
                        found.append(
                            {
                                "tweet_id": tweet_id,
                                "created_at": str(legacy.get("created_at") or ""),
                                "text": str(legacy.get("full_text") or "")[:80],
                            }
                        )

            for v in cur.values():
                if isinstance(v, (dict, list)):
                    stack.append(v)
        elif isinstance(cur, list):
            for item in cur:
                if isinstance(item, (dict, list)):
                    stack.append(item)

    return found


def _extract_dom_tweets_for_handle(page: Any, handle: str) -> List[Dict[str, str]]:
    """只读检查渲染后的推文卡片链接，兜底识别目标账号的 `/<handle>/status/<id>`。"""
    clean = _clean_handle(handle).lower()
    if not clean:
        return []

    pat = re.compile(rf"^(?:https?://(?:x|twitter)\.com)?/{re.escape(clean)}/status/(\d+)")
    results: List[Dict[str, str]] = []
    seen = set()

    try:
        articles = page.locator('article[data-testid="tweet"]')
        art_count = min(int(articles.count() or 0), 10)
    except Exception:
        return []

    for idx in range(art_count):
        try:
            art = articles.nth(idx)
            links = art.locator('a[href*="/status/"]')
            link_count = min(int(links.count() or 0), 5)
            for j in range(link_count):
                href = str(links.nth(j).get_attribute("href") or "").strip()
                m = pat.match(href.lower())
                if not m:
                    continue
                tid = m.group(1)
                if tid not in seen:
                    seen.add(tid)
                    txt = ""
                    try:
                        txt = str(art.inner_text() or "").replace("\n", " ")[:80]
                    except Exception:
                        pass
                    results.append({"tweet_id": tid, "created_at": "", "text": txt})
                break
        except Exception:
            continue
    return results


def _read_dom_empty_or_retry_state(page: Any) -> Tuple[bool, str]:
    """区分 DOM 明确 `No results` 空态 与 页面加载报错态，绝不把报错当成被屏蔽。"""
    try:
        empty_loc = page.locator('[data-testid="emptyState"]').first
        if empty_loc.count() > 0 and empty_loc.is_visible():
            return True, "empty"
    except Exception:
        pass

    try:
        main_loc = page.locator('main[role="main"]').first
        if main_loc.count() > 0 and main_loc.is_visible():
            low_txt = str(main_loc.inner_text() or "").lower()
            if "something went wrong" in low_txt or "try reloading" in low_txt or "出错了" in low_txt:
                return False, "retry_error"
            if "no results for" in low_txt or "没有找到" in low_txt:
                return True, "empty"
    except Exception:
        pass

    return False, ""


# ==============================================================================
# 四、单目标只读探测与总编排
# ==============================================================================

_UNAVAILABLE_PROFILE_HINTS = (
    "account suspended",
    "账号已被冻结",
    "您的账号已被冻结",
    "this account doesn't exist",
    "this account doesn’t exist",
    "此账号不存在",
)


def _is_observer_blocked_on_profile_page(page: Any) -> Tuple[bool, str]:
    """区分“观察者自身被封/掉登录”与“目标他人主页展示 Account suspended”。"""
    cur_url = str(getattr(page, "url", "") or "").lower()
    if "/account/suspended" in cur_url:
        return True, f"检测到观察者账号封禁页 ({cur_url})"
    health_status, health_msg = inspect_x_page_health(page)
    if health_status in ("locked_challenge", "not_logged_in"):
        return True, health_msg
    if health_status == "suspended":
        try:
            alert_loc = page.locator('[role="alert"]').first
            if alert_loc.count() > 0 and alert_loc.is_visible():
                low = str(alert_loc.inner_text() or "").lower()
                if "your account" in low or "您的账号" in low:
                    return True, health_msg
        except Exception:
            pass
    return False, ""


def verify_profile_on_page(
    page: Any,
    handle: str,
    *,
    wait_seconds: float = 10.0,
) -> Dict[str, Any]:
    """
    通过已登录的观察者 X 页面（一手信源）复核目标主页是否真正存活。
    仅在第三方免登接口疑似失真（`SUSPECT_UNAVAILABLE`）时按需触发，消除第三方缓存误报。
    """
    clean = _clean_handle(handle)
    if not clean:
        return {
            "handle": "",
            "profile_state": "ERROR",
            "followers": None,
            "following": None,
            "tweets": None,
            "detail": "empty handle",
        }

    url = f"https://x.com/{quote(clean, safe='')}"
    state: Dict[str, Any] = {"gql_res": None}

    def _on_resp(resp: Any) -> None:
        resp_url = str(getattr(resp, "url", "") or "")
        if "UserByScreenName" not in resp_url:
            return
        try:
            status = int(getattr(resp, "status", 0) or 0)
        except Exception:
            status = 0
        if status != 200:
            return
        try:
            payload = resp.json()
        except Exception:
            return
        if not isinstance(payload, dict):
            return
        res = ((payload.get("data") or {}).get("user") or {}).get("result")
        if not isinstance(res, dict):
            return
        typename = str(res.get("__typename") or "")
        if typename == "UserUnavailable":
            reason = str(res.get("reason") or res.get("message") or "unavailable").strip()
            state["gql_res"] = {
                "handle": clean,
                "profile_state": "UNAVAILABLE",
                "followers": None,
                "following": None,
                "tweets": None,
                "detail": f"X UserUnavailable: {reason}",
            }
            return
        legacy = res.get("legacy") if isinstance(res.get("legacy"), dict) else None
        if typename == "User" or legacy is not None:
            leg = legacy or {}
            state["gql_res"] = {
                "handle": clean,
                "profile_state": "ALIVE",
                "followers": leg.get("followers_count"),
                "following": leg.get("friends_count"),
                "tweets": leg.get("statuses_count"),
                "detail": "X UserByScreenName HTTP 200",
            }

    has_on = hasattr(page, "on")
    if has_on:
        page.on("response", _on_resp)

    try:
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=25000)
        except Exception as e:
            obs_blocked, obs_msg = _is_observer_blocked_on_profile_page(page)
            if obs_blocked:
                return {
                    "handle": clean,
                    "profile_state": "OBSERVER_BLOCKED",
                    "followers": None,
                    "following": None,
                    "tweets": None,
                    "detail": obs_msg,
                }
            return {
                "handle": clean,
                "profile_state": "ERROR",
                "followers": None,
                "following": None,
                "tweets": None,
                "detail": f"profile goto failed: {e}",
            }

        deadline = time.monotonic() + max(1.0, float(wait_seconds))
        while True:
            obs_blocked, obs_msg = _is_observer_blocked_on_profile_page(page)
            if obs_blocked:
                return {
                    "handle": clean,
                    "profile_state": "OBSERVER_BLOCKED",
                    "followers": None,
                    "following": None,
                    "tweets": None,
                    "detail": obs_msg,
                }

            if state["gql_res"] is not None:
                return state["gql_res"]

            for sel in ('[data-testid="emptyState"]', 'div[data-testid="empty_state_header_text"]', 'main[role="main"]'):
                try:
                    loc = page.locator(sel).first
                    if loc.count() > 0 and loc.is_visible():
                        low_txt = str(loc.inner_text() or "").lower()
                        for hint in _UNAVAILABLE_PROFILE_HINTS:
                            if hint in low_txt:
                                return {
                                    "handle": clean,
                                    "profile_state": "UNAVAILABLE",
                                    "followers": None,
                                    "following": None,
                                    "tweets": None,
                                    "detail": f"X DOM unavailable ({hint})",
                                }
                except Exception:
                    continue

            for sel in ('[data-testid="UserName"]', '[data-testid="UserProfileHeader_Items"]'):
                try:
                    loc = page.locator(sel).first
                    if loc.count() > 0 and loc.is_visible():
                        return {
                            "handle": clean,
                            "profile_state": "ALIVE",
                            "followers": None,
                            "following": None,
                            "tweets": None,
                            "detail": "X profile DOM visible",
                        }
                except Exception:
                    continue

            if time.monotonic() >= deadline:
                return {
                    "handle": clean,
                    "profile_state": "ERROR",
                    "followers": None,
                    "following": None,
                    "tweets": None,
                    "detail": "timeout verifying X profile",
                }

            if hasattr(page, "wait_for_timeout"):
                page.wait_for_timeout(500)
            else:
                time.sleep(0.5)
    finally:
        if has_on and hasattr(page, "remove_listener"):
            try:
                page.remove_listener("response", _on_resp)
            except Exception:
                pass

def probe_search_on_page(
    page: Any,
    handle: str,
    *,
    wait_seconds: float = SEARCH_WAIT_SECONDS,
) -> Dict[str, Any]:
    """
    在已打开的观察者页面中执行一次只读 `from:<handle>&f=live` 搜索检测。
    返回 `search_state`: `"VISIBLE"` | `"HIDDEN"` | `"OBSERVER_BLOCKED"` | `"ERROR"`。
    """
    clean = _clean_handle(handle)
    if not clean:
        return {
            "handle": "",
            "search_state": "ERROR",
            "observer_status": "ok",
            "tweet_count": 0,
            "tweets": [],
            "detail": "empty handle",
        }

    url = f"https://x.com/search?q=from%3A{quote(clean, safe='')}&src=typed_query&f=live"
    found_tweets: List[Dict[str, str]] = []
    seen_ids = set()
    state: Dict[str, Any] = {
        "api_ok": False,
        "api_ok_at": None,
        "api_error": "",
    }

    def _on_response(resp: Any) -> None:
        resp_url = str(getattr(resp, "url", "") or "")
        if "SearchTimeline" not in resp_url:
            return
        try:
            status = int(getattr(resp, "status", 0) or 0)
        except Exception:
            status = 0

        if status in (401, 403, 429):
            state["api_error"] = f"SearchTimeline HTTP {status}"
            return
        if status == 200:
            try:
                payload = resp.json()
            except Exception:
                return
            for tw in extract_search_timeline_tweets(payload, clean):
                tid = tw.get("tweet_id")
                if tid and tid not in seen_ids:
                    seen_ids.add(tid)
                    found_tweets.append(tw)
            state["api_ok"] = True
            if state["api_ok_at"] is None:
                state["api_ok_at"] = time.monotonic()

    has_on = hasattr(page, "on")
    if has_on:
        page.on("response", _on_response)

    try:
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=25000)
        except Exception as e:
            health_status, health_msg = inspect_x_page_health(page)
            if health_status != "ok":
                return {
                    "handle": clean,
                    "search_state": "OBSERVER_BLOCKED",
                    "observer_status": health_status,
                    "tweet_count": 0,
                    "tweets": [],
                    "detail": health_msg,
                }
            return {
                "handle": clean,
                "search_state": "ERROR",
                "observer_status": "ok",
                "tweet_count": 0,
                "tweets": [],
                "detail": f"goto failed: {e}",
            }

        health_status, health_msg = inspect_x_page_health(page)
        if health_status != "ok":
            return {
                "handle": clean,
                "search_state": "OBSERVER_BLOCKED",
                "observer_status": health_status,
                "tweet_count": 0,
                "tweets": [],
                "detail": health_msg,
            }

        deadline = time.monotonic() + max(1.0, float(wait_seconds))
        while True:
            health_status, health_msg = inspect_x_page_health(page)
            if health_status != "ok":
                return {
                    "handle": clean,
                    "search_state": "OBSERVER_BLOCKED",
                    "observer_status": health_status,
                    "tweet_count": 0,
                    "tweets": [],
                    "detail": health_msg,
                }

            if state["api_error"]:
                return {
                    "handle": clean,
                    "search_state": "ERROR",
                    "observer_status": "ok",
                    "tweet_count": 0,
                    "tweets": [],
                    "detail": state["api_error"],
                }

            if found_tweets:
                return {
                    "handle": clean,
                    "search_state": "VISIBLE",
                    "observer_status": "ok",
                    "tweet_count": len(found_tweets),
                    "tweets": list(found_tweets),
                    "detail": f"SearchTimeline HTTP 200 returned {len(found_tweets)} tweet(s)",
                }

            dom_tweets = _extract_dom_tweets_for_handle(page, clean)
            if dom_tweets:
                return {
                    "handle": clean,
                    "search_state": "VISIBLE",
                    "observer_status": "ok",
                    "tweet_count": len(dom_tweets),
                    "tweets": dom_tweets,
                    "detail": f"DOM matched {len(dom_tweets)} tweet(s)",
                }

            is_empty, empty_kind = _read_dom_empty_or_retry_state(page)
            if empty_kind == "retry_error":
                return {
                    "handle": clean,
                    "search_state": "ERROR",
                    "observer_status": "ok",
                    "tweet_count": 0,
                    "tweets": [],
                    "detail": "DOM rendered retry/error state",
                }
            if is_empty:
                return {
                    "handle": clean,
                    "search_state": "HIDDEN",
                    "observer_status": "ok",
                    "tweet_count": 0,
                    "tweets": [],
                    "detail": "DOM emptyState: No results",
                }

            if (
                state["api_ok"]
                and not found_tweets
                and state["api_ok_at"] is not None
                and (time.monotonic() - float(state["api_ok_at"])) >= 1.5
            ):
                return {
                    "handle": clean,
                    "search_state": "HIDDEN",
                    "observer_status": "ok",
                    "tweet_count": 0,
                    "tweets": [],
                    "detail": "SearchTimeline HTTP 200 returned 0 tweets",
                }

            if time.monotonic() >= deadline:
                return {
                    "handle": clean,
                    "search_state": "ERROR",
                    "observer_status": "ok",
                    "tweet_count": 0,
                    "tweets": [],
                    "detail": "timeout waiting for SearchTimeline or DOM result",
                }

            if hasattr(page, "wait_for_timeout"):
                page.wait_for_timeout(500)
            else:
                time.sleep(0.5)
    finally:
        if has_on and hasattr(page, "remove_listener"):
            try:
                page.remove_listener("response", _on_response)
            except Exception:
                pass


def _normalize_targets(targets: Optional[Sequence[Any]]) -> List[Tuple[str, str]]:
    if targets is None:
        return get_default_targets()
    normalized: List[Tuple[str, str]] = []
    for item in targets:
        if isinstance(item, (tuple, list)) and len(item) >= 2:
            alias = str(item[0] or "").strip() or _clean_handle(item[1])
            handle = _clean_handle(item[1])
        else:
            handle = _clean_handle(item)
            alias = handle
        if handle:
            normalized.append((alias, handle))
    return normalized


def _synthesize_verdict(
    *,
    profile_state: str,
    search_state: str,
    observer_error: str,
    calibration_passed: bool,
) -> str:
    if search_state in ("OBSERVER_BLOCKED", "SKIPPED_OBSERVER_BLOCKED"):
        return "OBSERVER_ERROR"
    if observer_error and search_state != "VISIBLE":
        return "OBSERVER_ERROR"
    if profile_state == "UNAVAILABLE":
        return "PROFILE_UNAVAILABLE"
    if search_state == "VISIBLE":
        return "VISIBLE_OK"
    if search_state == "HIDDEN":
        return "SEARCH_BANNED" if calibration_passed else "UNCERTAIN"
    return "UNCERTAIN"


class _HeadlessBurnerSession:
    """
    仅限一次性只读观察者小号（`x_xiaohao`）使用的无桌面窗口会话。
    不弹出桌面浏览器，不影响用户正常用电脑；每轮探测结束立即关闭 Chrome 释放全部内存。
    """

    def __init__(
        self,
        user_data_dir: str,
        *,
        use_proxy: bool = True,
        proxy: Optional[str] = None,
        lock_timeout: int = BROWSER_LOCK_TIMEOUT_SEC,
        default_timeout_ms: int = 30000,
        navigation_timeout_ms: int = 30000,
    ) -> None:
        # 【保留过滤原因说明】无头静默模式严格锁定只读观察者小号目录，绝不允许用于受限账号或写号（x_myself / x_dahao）。
        safe_raw = assert_safe_probe_session_dir(user_data_dir)
        fallback_name = Path(safe_raw.replace("\\", "/")).name or OBSERVER_SESSION_FALLBACK_NAME
        self.user_data_dir = resolve_cross_platform_path(
            safe_raw,
            DEFAULT_SESSION_ROOT / fallback_name,
        )
        assert_safe_probe_session_dir(self.user_data_dir)
        self.proxy = proxy if proxy is not None else (get_proxy_url() if use_proxy else None)
        self.lock_timeout = int(lock_timeout)
        self.default_timeout_ms = int(default_timeout_ms)
        self.navigation_timeout_ms = int(navigation_timeout_ms)

        self._lock_cm: Any = None
        self._pw_cm: Any = None
        self.context: Any = None
        self.page: Any = None

    def start(self) -> "_HeadlessBurnerSession":
        if self.is_alive():
            return self
        self.close()

        os.makedirs(self.user_data_dir, exist_ok=True)
        self._lock_cm = _user_data_dir_lock(self.user_data_dir, timeout=self.lock_timeout)
        self._lock_cm.__enter__()
        try:
            self._pw_cm = sync_playwright()
            p = self._pw_cm.__enter__()
            launch_kwargs: Dict[str, Any] = {
                "user_data_dir": self.user_data_dir,
                "channel": "chrome",
                "headless": True,
                "viewport": {"width": 1366, "height": 900},
                "args": [
                    "--disable-restore-session-state",
                    "--no-default-browser-check",
                    "--mute-audio",
                ],
            }
            if isinstance(self.proxy, str) and self.proxy.strip():
                launch_kwargs["proxy"] = {"server": self.proxy.strip()}
            elif isinstance(self.proxy, dict) and self.proxy.get("server"):
                launch_kwargs["proxy"] = self.proxy

            self.context = p.chromium.launch_persistent_context(**launch_kwargs)
            if self.default_timeout_ms:
                self.context.set_default_timeout(self.default_timeout_ms)
            if self.navigation_timeout_ms:
                self.context.set_default_navigation_timeout(self.navigation_timeout_ms)

            if self.context.pages:
                self.page = self.context.pages[0]
            else:
                self.page = self.context.new_page()

            for old_page in list(self.context.pages):
                if old_page != self.page:
                    try:
                        old_page.close()
                    except Exception:
                        pass
            return self
        except BaseException:
            self.close()
            raise

    def is_alive(self) -> bool:
        if self.context is None or self.page is None:
            return False
        try:
            return not self.page.is_closed()
        except Exception:
            return False

    def ensure_alive(self) -> Any:
        if not self.is_alive():
            self.start()
        return self.page

    def close(self) -> None:
        try:
            if self.context is not None:
                try:
                    self.context.close()
                except Exception:
                    pass
                self.context = None
                self.page = None
        finally:
            if self._pw_cm is not None:
                try:
                    self._pw_cm.__exit__(None, None, None)
                except Exception:
                    pass
                self._pw_cm = None
            if self._lock_cm is not None:
                try:
                    self._lock_cm.__exit__(None, None, None)
                except Exception:
                    pass
                self._lock_cm = None

    def __enter__(self) -> "_HeadlessBurnerSession":
        return self.start()

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        self.close()
        return False


def probe_x_accounts(
    targets: Optional[Sequence[Any]] = None,
    *,
    observer_session_dir: Optional[str] = None,
    control_handle: str = CONTROL_HANDLE,
    check_public_profile: bool = CHECK_PUBLIC_PROFILE,
    use_proxy: bool = USE_PROXY,
    headless: bool = HEADLESS,
    wait_seconds: float = SEARCH_WAIT_SECONDS,
    session_factory: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    """
    执行一次轻量化无损状态探测：
    1) 先校验观察者目录绝非被观察账号目录；
    2) 免登查询公开主页是否存活；
    3) 懒打开只读观察者 Chrome（默认无头静默，零桌面弹窗）逐个检索 `from:<handle>&f=live`，需要时做 1 次 `NASA` 空白对照；
    4) 完成后立即关闭 Chrome 释放全部资源，并返回结构化诊断结果。
    """
    resolved_targets = _normalize_targets(targets)
    raw_dir = (
        observer_session_dir
        if observer_session_dir is not None
        else resolve_session_dir(OBSERVER_SESSION_CONFIG_KEY, OBSERVER_SESSION_FALLBACK_NAME)
    )
    safe_dir = assert_safe_probe_session_dir(raw_dir)
    checked_at = datetime.now(TZ_CN).strftime("%Y-%m-%d %H:%M:%S")

    profile_map: Dict[str, Dict[str, Any]] = {}
    for _alias, handle in resolved_targets:
        if check_public_profile:
            profile_map[handle] = fetch_public_profile_status(handle)
        else:
            profile_map[handle] = {
                "handle": handle,
                "profile_state": "SKIPPED",
                "followers": None,
                "following": None,
                "tweets": None,
                "detail": "skipped",
            }

    search_results: Dict[str, Dict[str, Any]] = {}
    observer_error = ""
    clean_ctrl = _clean_handle(control_handle)
    calibration: Dict[str, Any] = {
        "performed": False,
        "control_handle": clean_ctrl,
        "passed": True,
        "result": None,
    }

    default_factory = _HeadlessBurnerSession if headless else XBrowserSession
    factory = session_factory or default_factory
    session = factory(
        user_data_dir=safe_dir,
        use_proxy=use_proxy,
        lock_timeout=BROWSER_LOCK_TIMEOUT_SEC,
    )

    try:
        try:
            page = session.ensure_alive()
        except UserDataDirLockTimeoutError as e:
            observer_error = f"观察者目录被占用（请先关闭手动打开的 x_xiaohao Chrome 窗口后重试）: {e}"
            page = None
        except Exception as e:
            observer_error = f"观察者 Chrome 启动失败（若桌面已手动打开 x_xiaohao 窗口请先关闭后重试）: {e}"
            page = None

        if page is not None:
            for idx, (_alias, handle) in enumerate(resolved_targets):
                if observer_error:
                    search_results[handle] = {
                        "handle": handle,
                        "search_state": "SKIPPED_OBSERVER_BLOCKED",
                        "observer_status": "blocked",
                        "tweet_count": 0,
                        "tweets": [],
                        "detail": observer_error,
                    }
                    continue

                if idx > 0:
                    human_pause(BETWEEN_QUERY_PAUSE_MS[0], BETWEEN_QUERY_PAUSE_MS[1], page=page)

                res = probe_search_on_page(page, handle, wait_seconds=wait_seconds)
                search_results[handle] = res
                if res.get("search_state") == "OBSERVER_BLOCKED":
                    observer_error = str(res.get("detail") or res.get("observer_status") or "observer_blocked")

            has_hidden = any(r.get("search_state") == "HIDDEN" for r in search_results.values())
            if not observer_error and has_hidden and clean_ctrl:
                human_pause(BETWEEN_QUERY_PAUSE_MS[0], BETWEEN_QUERY_PAUSE_MS[1], page=page)
                ctrl_res = probe_search_on_page(page, clean_ctrl, wait_seconds=wait_seconds)
                if ctrl_res.get("search_state") == "OBSERVER_BLOCKED":
                    observer_error = str(
                        ctrl_res.get("detail") or ctrl_res.get("observer_status") or "observer_blocked"
                    )
                calibration = {
                    "performed": True,
                    "control_handle": clean_ctrl,
                    "passed": ctrl_res.get("search_state") == "VISIBLE",
                    "result": ctrl_res,
                }

            for _alias, handle in resolved_targets:
                if observer_error:
                    break
                if profile_map.get(handle, {}).get("profile_state") == "SUSPECT_UNAVAILABLE":
                    human_pause(BETWEEN_QUERY_PAUSE_MS[0], BETWEEN_QUERY_PAUSE_MS[1], page=page)
                    confirmed = verify_profile_on_page(
                        page,
                        handle,
                        wait_seconds=min(float(wait_seconds), 10.0),
                    )
                    if confirmed.get("profile_state") == "OBSERVER_BLOCKED":
                        observer_error = str(confirmed.get("detail") or "observer_blocked")
                        profile_map[handle] = confirmed
                        break
                    profile_map[handle] = confirmed
        else:
            for _alias, handle in resolved_targets:
                search_results[handle] = {
                    "handle": handle,
                    "search_state": "SKIPPED_OBSERVER_BLOCKED",
                    "observer_status": "launch_failed",
                    "tweet_count": 0,
                    "tweets": [],
                    "detail": observer_error,
                }
    finally:
        try:
            session.close()
        except Exception:
            pass

    target_reports: List[Dict[str, Any]] = []
    for alias, handle in resolved_targets:
        prof = profile_map.get(handle, {})
        srch = search_results.get(handle, {})
        verdict = _synthesize_verdict(
            profile_state=str(prof.get("profile_state") or "ERROR"),
            search_state=str(srch.get("search_state") or "ERROR"),
            observer_error=observer_error,
            calibration_passed=bool(calibration.get("passed", False)),
        )
        target_reports.append(
            {
                "alias": alias,
                "handle": handle,
                "verdict": verdict,
                "profile": prof,
                "search": srch,
            }
        )

    return {
        "checked_at": checked_at,
        "observer_session_dir": safe_dir,
        "headless": bool(headless),
        "observer_ok": not bool(observer_error),
        "observer_error": observer_error,
        "calibration": calibration,
        "targets": target_reports,
    }


# ==============================================================================
# 五、人工可读报告、常驻守护与入口
# ==============================================================================

_VERDICT_LABELS: Dict[str, Tuple[str, str]] = {
    "VISIBLE_OK": ("🟢 ✅ 【已解封】搜索可见已恢复", "Search Ban 已解除，可按计划开始低频温和观察/复出。"),
    "SEARCH_BANNED": ("🔴 【未解封】仍处于 Search Ban（搜索屏蔽）", "继续保持零写入静默等待，切勿发帖/发评试探。"),
    "PROFILE_UNAVAILABLE": ("🔴 【异常】主页异常/疑似冻结", "公开主页不可访问，建议登录确认账号安全状态后再决策。"),
    "OBSERVER_ERROR": ("❌ 【作废】观察者会话异常（本次结论作废）", "观察者账号本身未就绪或浏览器目录被占用，未产生有效判定。"),
    "UNCERTAIN": ("🔴 【存疑/未解封】空白对照未通过或超时", "本次未判定解封，建议稍后重试；不要据此结论贸然用被测号操作。"),
}


def format_probe_report(report: Dict[str, Any]) -> str:
    """格式化清晰简明的中文状态报告（绿色图标仅留给账号真正恢复 `VISIBLE_OK`）。"""
    mode_label = "后台无窗口静默" if report.get("headless", HEADLESS) else "有头窗口"
    browser_label = "已关闭(0残余Chrome占用)" if report.get("observer_ok") else "异常中断"
    lines = [
        "=" * 72,
        f"X 账号无损状态探测报告 | 时间: {report.get('checked_at', '')} | 模式: {mode_label}",
        f"观察者目录: {report.get('observer_session_dir', '')} | 浏览器状态: {browser_label}",
    ]
    if report.get("observer_error"):
        lines.append(f"❌ 观察者异常: {report['observer_error']}")

    cal = report.get("calibration") or {}
    if cal.get("performed"):
        cal_text = "[有效]（观察者搜索功能正常，非误报）" if cal.get("passed") else "❌ 未通过（已自动降级为存疑，防误报）"
        lines.append(f"空白对照校准 (@{cal.get('control_handle', '')}): {cal_text}")
    else:
        lines.append("空白对照校准: 无需执行（所有已出结果账号均直接搜出帖子或观察者未就绪）")

    lines.append("-" * 72)
    for item in report.get("targets") or []:
        verdict = str(item.get("verdict") or "UNCERTAIN")
        label, advice = _VERDICT_LABELS.get(verdict, _VERDICT_LABELS["UNCERTAIN"])
        prof = item.get("profile") or {}
        srch = item.get("search") or {}

        stats = "主页跳过"
        p_state = prof.get("profile_state")
        if p_state == "ALIVE":
            stats = f"粉丝={prof.get('followers')} | 关注={prof.get('following')} | 总帖={prof.get('tweets')}"
        elif p_state and p_state != "SKIPPED":
            stats = f"主页状态={p_state} ({prof.get('detail', '')})"

        tweets = srch.get("tweets") or []
        latest_id = tweets[0].get("tweet_id", "") if tweets else ""
        search_desc = f"{srch.get('search_state', 'UNKNOWN')} ({srch.get('detail', '')})"
        if latest_id:
            search_desc += f" | 最新命中ID: {latest_id}"

        lines.append(f"[{item.get('alias', '')}] @{item.get('handle', '')} -> {label}")
        lines.append(f"  公开主页: {stats}")
        lines.append(f"  全站搜索: {search_desc}")
        lines.append(f"  建议动作: {advice}")

    lines.append("=" * 72)
    return "\n".join(lines)


def run_probe_daemon(
    *,
    targets: Optional[Sequence[Any]] = None,
    observer_session_dir: Optional[str] = None,
    control_handle: str = CONTROL_HANDLE,
    check_public_profile: bool = CHECK_PUBLIC_PROFILE,
    use_proxy: bool = USE_PROXY,
    headless: bool = HEADLESS,
    interval_hours: float = PROBE_INTERVAL_HOURS,
    jitter_ratio: float = PROBE_JITTER_RATIO,
    observer_error_backoff_hours: float = OBSERVER_ERROR_BACKOFF_HOURS,
    max_rounds: int = 0,
    stop_event: Optional[threading.Event] = None,
    session_factory: Optional[Callable[..., Any]] = None,
) -> int:
    """
    低资源常驻巡检守护循环：
    - 每轮按需无头启动观察者 Chrome 查完即关，休眠期间 0 Chrome 进程常驻；
    - 账号状态发生切换时打印醒目提示；全部结果写入 `logs/x_account_probe.log`。
    """
    raw_dir = (
        observer_session_dir
        if observer_session_dir is not None
        else resolve_session_dir(OBSERVER_SESSION_CONFIG_KEY, OBSERVER_SESSION_FALLBACK_NAME)
    )
    safe_dir = assert_safe_probe_session_dir(raw_dir)
    base_interval_sec = max(60.0, float(interval_hours) * 3600.0)
    backoff_sec = max(base_interval_sec, float(observer_error_backoff_hours) * 3600.0)
    last_verdicts: Dict[str, str] = {}

    def _step(round_idx: int) -> Dict[str, Any]:
        res = probe_x_accounts(
            targets=targets,
            observer_session_dir=safe_dir,
            control_handle=control_handle,
            check_public_profile=check_public_profile,
            use_proxy=use_proxy,
            headless=headless,
            session_factory=session_factory,
        )

        for item in res.get("targets") or []:
            handle = str(item.get("handle") or "")
            new_verdict = str(item.get("verdict") or "UNCERTAIN")
            prev_verdict = last_verdicts.get(handle)
            if prev_verdict is not None and prev_verdict != new_verdict:
                if new_verdict == "VISIBLE_OK":
                    logger.warning(f"🎉 [状态变化] @{handle}: {prev_verdict} -> {new_verdict}（全站搜索已恢复可见！）")
                elif new_verdict == "PROFILE_UNAVAILABLE":
                    logger.error(f"⛔ [状态变化] @{handle}: {prev_verdict} -> {new_verdict}（公开主页异常！）")
                else:
                    logger.info(f"🔔 [状态变化] @{handle}: {prev_verdict} -> {new_verdict}")
            last_verdicts[handle] = new_verdict

        logger.info("\n" + format_probe_report(res))

        if not res.get("observer_ok"):
            logger.warning(
                f"⏳ [观察者异常退避] 观察者会话暂不可用，暂停常规巡检并安全退避 {backoff_sec / 3600:.1f} 小时后重试。"
            )
            return {"next_sleep_seconds": backoff_sec}

        logger.info(
            f"⏳ [常驻休眠] 第 {round_idx} 轮巡检结束（Chrome 已关闭释放），预计约 {base_interval_sec / 3600:.1f} 小时后执行下一轮 | 日志: logs/x_account_probe.log"
        )
        return {}

    return run_daemon_loop(
        _step,
        interval_seconds=base_interval_sec,
        jitter_ratio=jitter_ratio,
        name="x_account_probe",
        logger_obj=logger,
        max_rounds=max_rounds,
        stop_event=stop_event,
        error_backoff_seconds=max(1800.0, base_interval_sec),
    )


if __name__ == "__main__":
    mode = str(RUN_MODE or "daemon").strip().lower()
    if mode == "once":
        result = probe_x_accounts(headless=HEADLESS)
        logger.info("\n" + format_probe_report(result))
    else:
        logger.info(
            "\n" + "=" * 72 + "\n"
            f"🚀 [x_account_probe] 启动无损常驻状态巡检 | 周期: 每 {PROBE_INTERVAL_HOURS}h | "
            f"无窗口静默(headless)={HEADLESS} | 观察者={OBSERVER_SESSION_FALLBACK_NAME}\n"
            "📄 查看最新报告日志: logs/x_account_probe.log\n"
            + "=" * 72
        )
        run_probe_daemon()
