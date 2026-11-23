# -*- coding: utf-8 -*-
"""
X (Twitter) Playwright 浏览器自动化独立模块：
- 自动发帖 (create_x_post)：支持纯文本、分段键入 (text_list)、多图上传（自动兼容本地路径与网络图片URL）
- 自动评论/互动 (comment_on_x_post)：支持对指定 X 帖子链接或帖子 ID 进行回复评论（支持带图评论、分段键入）与点赞
- 手动登录固化 (login_and_save_session) 与人工接管浏览器窗口 (open_browser_for_manual_use)
- 底层锁、缓存清理、浮层免疫、光标控制与故障取证统一复用 platforms.playwright_base
"""
import concurrent.futures
from datetime import timezone
from email.utils import parsedate_to_datetime
import os
import random
import re
import sys
import time
import traceback
from pathlib import Path
from urllib.parse import unquote

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.common_utils import (
    clean_post_text,
    download_web_media,
    get_media_save_dir,
    get_proxy_url,
    resolve_session_dir,
    safe_int,
    setup_logger,
)
from platforms.playwright_base import _fmt_hit
from platforms.x_browser import (
    USER_DATA_LOCK_TIMEOUT_SEC,
    BusinessErrorException,
    ChallengeRequiredError,
    PlaywrightTimeoutError,
    UserDataDirLockTimeoutError,
    XBrowserSession,
    _BaseApiWatcher,
    _alert_failure_scene,
    _dismiss_overlays,
    _force_caret_to_tail,
    _forensics,
    _hit_test,
    _interact_fallback_locators,
    _normalize_text_list,
    _parse_api_json,
    _pick_trusted_send_button,
    assert_x_page_healthy,
    check_for_crash,
    expect,
    human_click,
    human_pause,
    human_scroll,
    human_type,
    inspect_x_page_health,
)

logger = setup_logger(app_name="x_playwright")

# ==============================================================================
#                                   运行配置
# ==============================================================================
USER_DATA_DIR = resolve_session_dir("x_myself_browser_session_dir", "x_myself")
X_HOME_URL = "https://x.com/home"
X_COMPOSE_URL = "https://x.com/compose/post"
X_LOGIN_URL = "https://x.com/i/flow/login"

API_WAIT_TIMEOUT_MS = 15000
MAX_X_IMAGES = 4

RE_SUBMIT_API_PRIMARY = re.compile(
    r"graphql/[^/]+/(CreateTweet|CreateNoteTweet)",
    re.IGNORECASE,
)
RE_SUBMIT_API_SECONDARY = re.compile(
    r"(/i/api/graphql/[^/]+/.*Tweet|/1\.1/statuses/update\.json)",
    re.IGNORECASE,
)

RE_X_SEND_EXACT = re.compile(r"^\s*(发帖|发布|回复|Post|Reply|Tweet)\s*$", re.IGNORECASE)
RE_X_STATUS_ID = re.compile(r"/status/(\d{10,25})")
RE_PIN_MENU_TEXT = re.compile(r"(置顶到你的个人资料|置顶|Pin to your profile|Pin to profile)", re.IGNORECASE)
RE_UNPIN_MENU_TEXT = re.compile(r"(从个人资料取消置顶|取消置顶|Unpin from profile|Unpin)", re.IGNORECASE)
RE_PIN_API = re.compile(
    r"(/graphql/[^/]+/(PinTweet|CreatePin)|/i/api/1\.1/account/pin_tweet\.json)",
    re.IGNORECASE,
)
RE_NOTIF_API = re.compile(
    r"(/graphql/[^/]+/(NotificationsTimeline|NotificationsMentions)|/i/api/2/notifications/(?:all|mentions|verified)\.json)",
    re.IGNORECASE,
)
_LEADING_MENTIONS_RE = re.compile(r"^(?:@[A-Za-z0-9_]+\s*)+")
_TCO_URL_RE = re.compile(r"https?://t\.co/\S+")


def extract_x_post_id(post_url_or_id):
    """从 X/Twitter 帖子链接或纯数字字符串中提取帖子 ID (rest_id)。"""
    raw = str(post_url_or_id or "").strip()
    if not raw:
        return "unknown"
    if raw.isdigit():
        return raw
    m = RE_X_STATUS_ID.search(raw)
    if m:
        return m.group(1)
    return raw.rstrip("/").split("/")[-1].split("?")[0] or "unknown"


def normalize_x_post_url(target_post_url):
    """将传入的 X 帖子 ID 或链接规范化为可访问的 https://x.com/... 完整 URL。"""
    raw_target = str(target_post_url or "").strip()
    if not raw_target:
        return ""
    if raw_target.isdigit():
        return f"https://x.com/i/status/{raw_target}"
    if not raw_target.startswith(("http://", "https://")):
        return "https://" + raw_target.lstrip("/")
    return raw_target


# ==============================================================================
#                      图片地址解析（兼容本地路径、相对路径与网络URL）
# ==============================================================================
def _safe_download_web_media(url):
    """
    安全下载网络图片：
    若当前线程已处于 Playwright 的 asyncio 事件循环中，自动切入独立工作线程执行下载，
    避免 asyncio.run() 抛出 RuntimeError。
    """
    try:
        return download_web_media(url)
    except RuntimeError:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(download_web_media, url).result(timeout=45)
    except Exception as e:
        logger.warning(f"[图片/下载] 下载网络图片异常 | URL: <{url}> | 原因: {e}")
        return None


def _prepare_image_files(image_path_list):
    """
    将传入的 image_path_list 规范化为本地存在的有效图片绝对路径列表：
    1. 支持传入单个字符串/Path，或列表/元组/集合；支持用 '|' 分隔的多图字符串；
    2. 支持字典项（如 {'local_path': '...', 'original_url': '...'}）；
    3. 若为 http:// 或 https:// 开头的网络图片地址，自动下载至 media_downloads 目录；
    4. 若为文件名或相对路径，自动在项目根目录及 media_downloads 目录下探测；
    5. 自动限制单帖最多上传 MAX_X_IMAGES (4) 张图片。
    """
    if not image_path_list:
        return []

    raw_items = []
    if isinstance(image_path_list, (str, Path)):
        s = str(image_path_list).strip()
        if "|" in s:
            raw_items.extend(part.strip() for part in s.split("|") if part.strip())
        elif s:
            raw_items.append(s)
    elif isinstance(image_path_list, dict):
        raw_items.append(image_path_list)
    elif isinstance(image_path_list, (list, tuple, set)):
        for item in image_path_list:
            if isinstance(item, (str, Path)):
                s = str(item).strip()
                if "|" in s:
                    raw_items.extend(part.strip() for part in s.split("|") if part.strip())
                elif s:
                    raw_items.append(s)
            elif isinstance(item, dict):
                raw_items.append(item)

    media_dir = Path(get_media_save_dir())
    valid_paths = []

    for idx, item in enumerate(raw_items):
        candidate_str = ""
        fallback_url = ""
        if isinstance(item, dict):
            candidate_str = str(item.get("local_path") or item.get("path") or "").strip()
            fallback_url = str(item.get("original_url") or item.get("url") or "").strip()
            if not candidate_str and fallback_url:
                candidate_str = fallback_url
        else:
            candidate_str = str(item).strip()

        if not candidate_str:
            continue

        # 情况 1：网络图片 URL -> 自动下载到本地
        if candidate_str.lower().startswith(("http://", "https://")):
            logger.info(f"[图片/准备] 检测到网络图片 URL，正在下载至本地 | 第【{idx + 1}】张: <{candidate_str}>")
            local_file = _safe_download_web_media(candidate_str)
            if local_file and os.path.isfile(local_file):
                valid_paths.append(os.path.abspath(local_file))
                logger.info(f"[图片/准备] 网络图片下载完成 | 本地路径: <{local_file}>")
            else:
                logger.warning(f"[图片/准备] 网络图片下载失败，已跳过 | URL: <{candidate_str}>")
            continue

        # 情况 2：本地路径探测（直接路径 -> media_downloads 目录 -> 项目根目录）
        probe_candidates = [
            Path(candidate_str),
            media_dir / candidate_str,
            media_dir / Path(candidate_str).name,
            _PROJECT_ROOT / candidate_str,
        ]
        resolved_local = None
        for p in probe_candidates:
            try:
                if p.is_file() and p.stat().st_size > 0:
                    resolved_local = str(p.resolve())
                    break
            except OSError:
                continue

        if resolved_local:
            valid_paths.append(resolved_local)
        elif fallback_url and fallback_url.lower().startswith(("http://", "https://")):
            logger.info(
                f"[图片/准备] 本地路径不存在，回退下载 original_url | 原路径: <{candidate_str}> | URL: <{fallback_url}>"
            )
            local_file = _safe_download_web_media(fallback_url)
            if local_file and os.path.isfile(local_file):
                valid_paths.append(os.path.abspath(local_file))
            else:
                logger.warning(f"[图片/准备] 回退 URL 下载失败，已跳过: <{fallback_url}>")
        else:
            logger.warning(f"[图片/准备] 图片路径不存在或文件为空，已跳过: <{candidate_str}>")

    # 去重但保持顺序
    deduped_paths = list(dict.fromkeys(valid_paths))
    if len(deduped_paths) > MAX_X_IMAGES:
        logger.warning(
            f"[图片/准备] X 平台单帖最多支持 {MAX_X_IMAGES} 张图片，当前传入 {len(deduped_paths)} 张，自动截取前 {MAX_X_IMAGES} 张"
        )
        deduped_paths = deduped_paths[:MAX_X_IMAGES]

    return deduped_paths


# ==============================================================================
#                    X 编辑器：登录态检查 / 页面就绪等待 / 定位 / 唤醒 / 键入 / 图片上传
# ==============================================================================
def _wait_x_home_ready(page, timeout_ms=25000):
    """等待 X 主页 SPA 核心组件（发帖框、左侧发帖按钮或登录表单）渲染就绪。"""
    ready_selector = (
        'div[data-testid="tweetTextarea_0"], '
        'a[data-testid="SideNav_NewTweet_Button"], '
        'button[data-testid="SideNav_NewTweet_Button"], '
        'input[autocomplete="username"], '
        'a[data-testid="loginButton"]'
    )
    try:
        page.locator(ready_selector).first.wait_for(state="visible", timeout=timeout_ms)
    except Exception:
        pass


def _wait_x_post_detail_ready(page, timeout_ms=25000):
    """等待 X 帖子详情页 SPA 核心组件（主帖 article、回复框、错误提示或登录表单）渲染就绪。"""
    ready_selector = (
        'article[data-testid="tweet"], '
        'div[data-testid="tweetTextarea_0"], '
        'div[data-testid="error-detail"], '
        'div[data-testid="empty_state_header_text"], '
        'input[autocomplete="username"], '
        'a[data-testid="loginButton"]'
    )
    try:
        page.locator(ready_selector).first.wait_for(state="visible", timeout=timeout_ms)
    except Exception:
        pass


def _check_x_login_state(page):
    """
    检查当前页面是否处于已登录状态。
    若被重定向到登录页或检测到登录表单，返回错误提示字符串；已登录则返回 None。
    """
    current_url = (page.url or "").lower()
    if "/i/flow/login" in current_url or current_url.rstrip("/").endswith("/login"):
        return f"检测到页面跳转至 X 登录页 ({page.url})，请先执行 open_browser_for_manual_use 登录账号。"

    login_indicators = (
        'input[autocomplete="username"]',
        'input[name="text"][type="text"]',
        'a[data-testid="loginButton"]',
    )
    for sel in login_indicators:
        try:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible():
                return f"页面检测到未登录组件 ({sel})，本地会话可能未登录或已失效，请先登录 X 账号。"
        except Exception:
            continue
    return None


def _snapshot_x_editor(editor_container, real_editor):
    """读取 X 发帖编辑器状态快照，返回 (文本字符数, 媒体缩略图数)。"""
    try:
        text_len = len((real_editor.inner_text() or "").strip()) if real_editor.is_visible() else 0
    except Exception:
        text_len = 0
    try:
        media_cnt = editor_container.locator(
            '[data-testid="attachments"] [data-testid="tweetPhoto"], '
            '[data-testid="attachments"] img, '
            '[data-testid="attachments"] video, '
            'div[data-testid="tweetPhoto"] img'
        ).count()
    except Exception:
        media_cnt = 0
    return text_len, media_cnt


def _resolve_composer_container_from_textarea(textarea_locator):
    """
    从 tweetTextarea_0 向上回溯最近的完整发帖/回复容器（同时包含输入框、工具栏 toolBar 与发送按钮）。
    """
    return textarea_locator.locator(
        'xpath=ancestor::div['
        './/button[@data-testid="tweetButtonInline" or @data-testid="tweetButton"] '
        'or .//div[@data-testid="toolBar"]'
        '][1]'
    )


def _locate_x_post_creator(page):
    """
    精准锁定 X 主页顶部内联发帖容器或弹窗发帖容器。
    若主页内联发帖框未渲染，则自动点击左侧导航栏「发帖(Post)」按钮唤起发帖弹窗。
    """
    # 1. 若页面已打开 Modal 发帖框，优先使用 Modal 容器
    modal_loc = page.locator('div[role="dialog"]:has(div[data-testid="tweetTextarea_0"])').first
    try:
        if modal_loc.count() > 0 and modal_loc.is_visible():
            logger.info("[定位/DOM] 已锁定 X 弹窗发帖区容器 (dialog)")
            return modal_loc
    except Exception:
        pass

    # 2. 尝试锁定主页顶部内联发帖框
    inline_textarea = page.locator('div[data-testid="tweetTextarea_0"]').first
    try:
        inline_textarea.wait_for(state="visible", timeout=6000)
        composer = _resolve_composer_container_from_textarea(inline_textarea)
        if composer.count() > 0 and composer.first.is_visible():
            logger.info("[定位/DOM] 已锁定 X 主页内联发帖区容器 (XPath 最近公共祖先)")
            return composer.first
    except Exception:
        pass

    # 3. 若主页内联编辑器未就绪，点击左侧导航栏发帖按钮唤起发帖 Modal
    try:
        side_post_btn = page.locator(
            'a[data-testid="SideNav_NewTweet_Button"], button[data-testid="SideNav_NewTweet_Button"]'
        ).first
        if side_post_btn.count() > 0 and side_post_btn.is_visible():
            logger.info("[定位/DOM] 主页内联编辑器未就绪，点击左侧导航栏「发帖」按钮唤起弹窗...")
            human_click(page, side_post_btn)
            modal_loc = page.locator('div[role="dialog"]:has(div[data-testid="tweetTextarea_0"])').first
            modal_loc.wait_for(state="visible", timeout=8000)
            logger.info("[定位/DOM] 已锁定 X 弹窗发帖区容器")
            return modal_loc
    except Exception as e:
        logger.warning(f"[定位/DOM] 点击侧栏发帖按钮异常: {str(e)[:120]}")

    # 4. 兜底回退
    inline_textarea.wait_for(state="visible", timeout=8000)
    composer = _resolve_composer_container_from_textarea(inline_textarea)
    if composer.count() > 0:
        return composer.first
    return page.locator('main[role="main"]').first


def _locate_focal_tweet(page, post_id=None):
    """
    在 X 帖子详情页中定位目标主帖 article 元素：
    1. 优先匹配带 tabindex="-1" 的详情页焦点推文；
    2. 其次匹配包含 /status/{post_id} 链接的推文；
    3. 兜底取页面首个 article[data-testid="tweet"]。
    """
    cands = [
        page.locator('article[data-testid="tweet"][tabindex="-1"]').first,
    ]
    if post_id and post_id != "unknown":
        cands.append(page.locator(f'article[data-testid="tweet"]:has(a[href*="/status/{post_id}"])').first)
    cands.append(page.locator('article[data-testid="tweet"]').first)

    for c in cands:
        try:
            if c.count() > 0 and c.is_visible():
                return c
        except Exception:
            continue
    return page.locator('article[data-testid="tweet"]').first


def _locate_x_reply_box(page, post_id=None):
    """
    锁定 X 帖子详情页的回复输入容器：
    1. 若回复弹窗 (div[role="dialog"]) 已打开，直接返回弹窗容器；
    2. 若详情页内联回复框存在（含折叠态），先点击展开工具栏与回复按钮，再返回其完整容器；
    3. 若内联回复框不可见，则自动点击目标主帖下的「回复(reply)」图标唤起回复弹窗。
    """
    # 策略 1：已打开的回复弹窗
    modal_loc = page.locator('div[role="dialog"]:has(div[data-testid="tweetTextarea_0"])').first
    try:
        if modal_loc.count() > 0 and modal_loc.is_visible():
            logger.info("[定位/DOM] 已锁定 X 回复弹窗容器 (dialog)")
            return modal_loc
    except Exception:
        pass

    # 策略 2：详情页内联回复框（支持折叠态点击激活）
    placeholder = page.locator(
        'div[data-testid="tweetTextarea_0"], div[data-testid="tweetTextarea_0_label"]'
    ).first
    try:
        if placeholder.count() == 0 or not placeholder.is_visible():
            page.mouse.wheel(0, 300)
            page.wait_for_timeout(500)

        if placeholder.count() > 0 and placeholder.is_visible():
            try:
                placeholder.scroll_into_view_if_needed(timeout=3000)
                human_click(page, placeholder, timeout_ms=5000)
                page.wait_for_timeout(500)
            except Exception:
                pass

            inline_textarea = page.locator('div[data-testid="tweetTextarea_0"]').first
            composer = _resolve_composer_container_from_textarea(inline_textarea)
            if composer.count() > 0 and composer.first.is_visible():
                logger.info("[定位/DOM] 已锁定 X 详情页内联回复区容器")
                return composer.first
    except Exception as e:
        logger.debug(f"[定位/DOM] 内联回复框探测未命中: {e}")

    # 策略 3：点击主帖的 reply 按钮唤起回复弹窗
    try:
        focal_tweet = _locate_focal_tweet(page, post_id=post_id)
        reply_icon_btn = focal_tweet.locator('button[data-testid="reply"]').first
        if reply_icon_btn.count() == 0:
            reply_icon_btn = page.locator('button[data-testid="reply"]').first
        if reply_icon_btn.count() > 0 and reply_icon_btn.is_visible():
            logger.info("[定位/DOM] 内联回复框未展开，点击帖子「回复(reply)」按钮唤起回复弹窗...")
            human_click(page, reply_icon_btn)
            modal_loc = page.locator('div[role="dialog"]:has(div[data-testid="tweetTextarea_0"])').first
            modal_loc.wait_for(state="visible", timeout=8000)
            logger.info("[定位/DOM] 已锁定 X 回复弹窗容器")
            return modal_loc
    except Exception as e:
        logger.warning(f"[定位/DOM] 点击回复图标唤起弹窗异常: {str(e)[:120]}")

    # 策略 4：最终等待兜底
    inline_textarea = page.locator('div[data-testid="tweetTextarea_0"]').first
    inline_textarea.wait_for(state="visible", timeout=8000)
    composer = _resolve_composer_container_from_textarea(inline_textarea)
    if composer.count() > 0:
        return composer.first
    return page.locator('main[role="main"]').first


def _wake_x_editor(page, editor_container, max_round=3):
    """唤醒 X Draft.js 富文本编辑器并确保焦点落入其中（纯物理点击，严禁 synthetic JS 事件）。返回 real_editor(Locator)。"""
    target_cands = (
        editor_container.locator('div[data-testid="tweetTextarea_0"][contenteditable="true"]').first,
        editor_container.locator('div[role="textbox"][data-testid="tweetTextarea_0"]').first,
        editor_container.locator('div.public-DraftEditor-content[contenteditable="true"]').first,
        editor_container.locator('div[data-testid="tweetTextarea_0_label"]').first,
        editor_container.locator('div[contenteditable="true"]').first,
    )
    target = target_cands[0]
    for c in target_cands:
        try:
            if c.count() > 0:
                target = c
                break
        except Exception:
            continue

    real_editor = editor_container.locator(
        'div[data-testid="tweetTextarea_0"][contenteditable="true"], '
        'div[role="textbox"][data-testid="tweetTextarea_0"], '
        'div.public-DraftEditor-content[contenteditable="true"], '
        'div[contenteditable="true"]'
    ).first
    last_hit = None

    for rnd in range(1, max_round + 1):
        _dismiss_overlays(page, aggressive=(rnd >= 2), desc=f"x-wake-r{rnd}")
        try:
            target.scroll_into_view_if_needed(timeout=3000)
        except Exception:
            pass
        page.wait_for_timeout(200)

        last_hit = _hit_test(page, target)
        if not last_hit.get("ok"):
            logger.warning(f"[编辑器/唤醒] 第{rnd}轮输入区不可命中，先清障再点击 | 详情: 【{_fmt_hit(last_hit)}】")

        try:
            if rnd == 1:
                human_click(page, target, timeout_ms=6000)
            elif rnd == 2:
                target.click(timeout=5000, force=True)
            else:
                box = target.bounding_box()
                if isinstance(box, dict):
                    page.mouse.click(box["x"] + box["width"] / 2, box["y"] + min(box["height"] / 2, 20))
        except Exception as e:
            logger.warning(f"[编辑器/唤醒] 第{rnd}轮点击动作抛错，升级策略重试 | 详情: 【{str(e)[:150]}】")

        try:
            expect(real_editor).to_be_editable(timeout=6000 if rnd == 1 else 4000)
            focused = page.evaluate(
                """() => {
                    const a = document.activeElement;
                    return !!(a && (a.isContentEditable ||
                                    (a.getAttribute && a.getAttribute('data-testid') === 'tweetTextarea_0') ||
                                    (a.closest && a.closest('[data-testid="tweetTextarea_0"]'))));
                }"""
            )
            logger.info(f"[编辑器/唤醒] X 编辑器唤醒成功 | 轮次: 【{rnd}】 | 状态: [可编辑] | 焦点在编辑器: 【{focused}】")
            return real_editor
        except Exception:
            logger.warning(f"[编辑器/唤醒] 第{rnd}轮唤醒未生效，升级策略重试")

    _forensics(page, "x_wake_fail", {"last_hit_test": last_hit})
    raise Exception(f"X 编辑器唤醒失败（{max_round}级降级全部失效）。最后命中测试: {last_hit}")


def _dismiss_x_typeahead_if_open(page):
    """
    若 X 编辑器的 $Cashtag / #Hashtag / @Mention 自动补全下拉框处于弹出状态，
    按 Escape 将其关闭，防止后续按 Enter 换行时误选中下拉框第一项。
    """
    try:
        dropdown = page.locator('div[data-testid="typeaheadDropdown"]').first
        if dropdown.count() > 0 and dropdown.is_visible():
            page.keyboard.press("Escape")
            page.wait_for_timeout(120)
    except Exception:
        pass


def _type_x_body(page, real_editor, text):
    """
    针对 X Draft.js 富文本编辑器的自然变速键入实现：
    - 换行前自动关闭 typeaheadDropdown 候选弹窗，杜绝 $SYMBOL / #Tag 回车被吞；
    - 复用 x_browser.human_type 进行变速小块打字与标点停顿（严禁恒定 60ms 或 insert_text）。
    """
    if not text:
        return
    human_type(page, real_editor, text, dismiss_typeahead_fn=_dismiss_x_typeahead_if_open)


def _type_x_text_list(page, real_editor, valid_text_list):
    """按 text_list 顺序逐项键入 X 编辑器。"""
    total = len(valid_text_list)
    for idx, item in enumerate(valid_text_list):
        seg_text = item.get("text", "")
        delay_sec = item.get("delay", 0.0)
        logger.info(
            f"[编辑器/正文] 正在键入 text_list 第 【{idx + 1}/{total}】 项 "
            f"| 字符数: 【{len(seg_text)}】 | 输入后等待: 【{delay_sec}s】"
        )
        if seg_text:
            _type_x_body(page, real_editor, seg_text)
        if delay_sec > 0:
            page.wait_for_timeout(int(delay_sec * 1000))


def _upload_x_images(page, editor_container, valid_images):
    """
    X 平台图片上传：
    1. 挂载 `upload.x.com` / `media/upload` 请求与完成计数监听，确认图片上传链路真实完成；
    2. 优先通过 input[data-testid='fileInput'] 直接批量注入文件；
    3. 后备通过点击工具栏媒体按钮触发 FileChooser 上传；
    4. 等待全部缩略图渲染及上传进度条与网络请求结束。
    """
    if not valid_images:
        return

    media_state = {"inflight": set(), "finalize_ok": 0}

    def _is_media_upload_url(url_str):
        u = (url_str or "").lower()
        return "upload.x.com" in u or "media/upload" in u

    def _on_media_req(req):
        try:
            if _is_media_upload_url(req.url) and req.method in ("POST", "PUT"):
                media_state["inflight"].add(id(req))
        except Exception:
            pass

    def _on_media_done(req):
        try:
            media_state["inflight"].discard(id(req))
        except Exception:
            pass

    def _on_media_resp(resp):
        try:
            if _is_media_upload_url(resp.url):
                media_state["inflight"].discard(id(resp.request))
                u = (resp.url or "").lower()
                if resp.status in (200, 201, 202) and ("command=finalize" in u or "media/upload" in u):
                    media_state["finalize_ok"] += 1
        except Exception:
            pass

    page.on("request", _on_media_req)
    page.on("requestfinished", _on_media_done)
    page.on("requestfailed", _on_media_done)
    page.on("response", _on_media_resp)

    uploaded = False
    try:
        # 通道 1：直接定位 input[data-testid="fileInput"] 注入
        file_input = editor_container.locator('input[data-testid="fileInput"], input[type="file"]').first
        if file_input.count() == 0:
            file_input = page.locator('input[data-testid="fileInput"], input[type="file"]').first

        if file_input.count() > 0:
            try:
                file_input.set_input_files(valid_images, timeout=15000)
                uploaded = True
                logger.info(f"[编辑器/图片] 通过【input[data-testid=fileInput]】批量挂载完毕 | 数量: 【{len(valid_images)}】张")
            except Exception as e:
                logger.warning(f"[编辑器/图片] 批量直注失败，尝试逐张注入 | 原因: 【{str(e)[:140]}】")
                try:
                    for img_path in valid_images:
                        file_input.set_input_files(img_path, timeout=10000)
                        page.wait_for_timeout(1200)
                    uploaded = True
                    logger.info(f"[编辑器/图片] 通过【input[data-testid=fileInput]】逐张挂载完毕 | 数量: 【{len(valid_images)}】张")
                except Exception as sub_e:
                    logger.warning(f"[编辑器/图片] input 直注通道未完成，转入 FileChooser 后备通道 | 原因: 【{str(sub_e)[:140]}】")

        # 通道 2：点击图片图标通过 FileChooser 注入
        if not uploaded:
            img_btn_cands = [
                editor_container.locator(
                    'button[aria-label*="添加照片或视频"], button[aria-label*="Add photos or video" i], '
                    'button[aria-label*="媒体"], button[aria-label*="Media" i]'
                ).first,
                page.locator(
                    'button[aria-label*="添加照片或视频"], button[aria-label*="Add photos or video" i]'
                ).first,
            ]
            try:
                img_btn = _interact_fallback_locators(img_btn_cands, action="wait", timeout=5000, desc="X 上传图片按钮")
                with page.expect_file_chooser(timeout=5000) as fc_info:
                    human_click(page, img_btn)
                fc = fc_info.value
                if len(valid_images) > 1 and not fc.is_multiple():
                    fc.set_files(valid_images[0])
                    page.wait_for_timeout(1200)
                    for img_path in valid_images[1:]:
                        with page.expect_file_chooser(timeout=5000) as next_fc_info:
                            human_click(page, img_btn)
                        next_fc_info.value.set_files(img_path)
                        page.wait_for_timeout(1200)
                else:
                    fc.set_files(valid_images)
                uploaded = True
                logger.info(f"[编辑器/图片] 通过【FileChooser】成功提交图片 | 数量: 【{len(valid_images)}】张")
            except Exception as fc_err:
                logger.warning(f"[编辑器/图片] 图片上传通道全部失败，降级为纯文本发送 | 原因: 【{str(fc_err)[:150]}】")
                return

        thumb_loc = editor_container.locator(
            '[data-testid="attachments"] img, '
            '[data-testid="tweetPhoto"] img, '
            'img[src^="blob:"]'
        )
        # 等待全部目标图片预览节点渲染完成
        try:
            target_idx = max(0, min(len(valid_images), MAX_X_IMAGES) - 1)
            expect(thumb_loc.nth(target_idx)).to_be_visible(timeout=15000)
        except Exception:
            try:
                expect(thumb_loc.first).to_be_visible(timeout=5000)
            except Exception:
                pass

        # 等待进度条消失且媒体上传网络请求落定（至多 18s）
        end_wait = time.time() + 18.0
        while time.time() < end_wait:
            progress_busy = False
            try:
                progress = editor_container.locator('[data-testid="attachments"] [role="progressbar"]')
                if progress.count() > 0 and progress.first.is_visible():
                    progress_busy = True
            except Exception:
                pass
            if not progress_busy and not media_state["inflight"]:
                break
            page.wait_for_timeout(350)

        page.wait_for_timeout(1200)
    finally:
        for ev_name, handler in (
            ("request", _on_media_req),
            ("requestfinished", _on_media_done),
            ("requestfailed", _on_media_done),
            ("response", _on_media_resp),
        ):
            try:
                page.remove_listener(ev_name, handler)
            except Exception:
                pass


# ==============================================================================
#                        发送：GraphQL 接口监听与结果判定
# ==============================================================================
class _XApiWatcher(_BaseApiWatcher):
    """X GraphQL CreateTweet / CreateNoteTweet 接口监听器。"""

    def __init__(self, page):
        super().__init__(page, RE_SUBMIT_API_PRIMARY, RE_SUBMIT_API_SECONDARY)


def _extract_tweet_id_from_graphql_body(body):
    """从 X CreateTweet / CreateNoteTweet GraphQL 响应 JSON 中提取新帖 rest_id。"""
    if not isinstance(body, dict):
        return None
    data = body.get("data")
    if isinstance(data, dict):
        for key in ("create_tweet", "notetweet_create"):
            node = data.get(key)
            if isinstance(node, dict):
                res = (node.get("tweet_results") or {}).get("result")
                if isinstance(res, dict):
                    rid = res.get("rest_id") or (res.get("tweet") or {}).get("rest_id")
                    if rid:
                        return str(rid)
    # 正则兜底提取
    raw_str = str(body)
    m = re.search(r"['\"]rest_id['\"]\s*:\s*['\"](\d{15,22})['\"]", raw_str)
    if m:
        return m.group(1)
    return None


def _read_x_api_verdict(page, watcher, pre_captured_resp=None, wait_timeout_ms=API_WAIT_TIMEOUT_MS):
    """
    解读 X 发帖/回复接口响应结果：
    - 主判据：POST /i/api/graphql/.../CreateTweet (或 CreateNoteTweet) 返回 rest_id；
    - 若包含 errors 且无 rest_id，则抛出 BusinessErrorException。
    """
    resp = pre_captured_resp if pre_captured_resp is not None else watcher.wait_primary(wait_timeout_ms)
    if resp is not None:
        body = _parse_api_json(resp)
        tweet_id = _extract_tweet_id_from_graphql_body(body)
        if tweet_id:
            logger.info(
                f"[发送/校验] X GraphQL 主接口确认发帖成功 | HTTP: 【{resp.status}】 "
                f"| 帖子ID(rest_id): 【{tweet_id}】 | 结果: [Success]"
            )
            return True, tweet_id

        errors = body.get("errors") if isinstance(body, dict) else None
        if errors and isinstance(errors, list):
            first_err = errors[0] if isinstance(errors[0], dict) else {"message": str(errors[0])}
            err_msg = first_err.get("message") or str(errors)
            err_code = first_err.get("code", "unknown")
            logger.error(
                f"[发送/校验] X 服务端业务规则拒收本条帖子 | HTTP: 【{resp.status}】 "
                f"| code: 【{err_code}】 | 原因: 【{err_msg}】"
            )
            _forensics(page, "x_biz_reject", {"url": resp.url, "status": resp.status, "body": body})
            low_err = str(err_msg).lower()
            is_risk_block = str(err_code) in ("226", "326") or any(
                k in low_err for k in ("automated", "challenge", "suspended", "locked", "bounce")
            )
            err_cls = ChallengeRequiredError if is_risk_block else BusinessErrorException
            raise err_cls(f"X 业务发送被服务端拦截 (code={err_code})，原因: {err_msg}")

        logger.warning(
            f"[发送/校验] X 主接口返回 HTTP {resp.status} 但未包含 rest_id 与 errors，转入辅助/DOM校验 | url={resp.url}"
        )

    sec = watcher.latest_secondary()
    if sec is not None:
        body = _parse_api_json(sec)
        tweet_id = _extract_tweet_id_from_graphql_body(body)
        if tweet_id:
            logger.info(f"[发送/校验] 辅助接口确认 X 发帖成功 | 帖子ID: 【{tweet_id}】 | 结果: [Success]")
            return True, tweet_id

    if resp is None:
        logger.warning(f"[发送/校验] 【{wait_timeout_ms}ms】 内未捕获到主接口响应，转入 DOM 兜底校验")
    return False, None


# ==============================================================================
#                              核心：执行发帖与回复
# ==============================================================================
def _submit_x_post(
    page,
    editor_container,
    content="",
    image_path_list=None,
    text_list=None,
    prepared_images=None,
):
    """在 X 发帖/回复容器作用域内执行：唤醒 → 键入文字 → 上传图片 → 点击发帖 → 校验响应。"""
    content = str(content) if content else ""
    valid_text_list = _normalize_text_list(text_list)
    valid_images = prepared_images if prepared_images is not None else _prepare_image_files(image_path_list)

    if not content.strip() and not valid_text_list and not valid_images:
        raise Exception("发帖/回复正文与图片不能同时为空")

    _dismiss_overlays(page, aggressive=False, desc="x-pre-submit")

    real_editor = _wake_x_editor(page, editor_container)

    if valid_text_list or content.strip():
        _force_caret_to_tail(page, real_editor, tag="x-before-body")
        if valid_text_list:
            total_chars = sum(len(x.get("text", "")) for x in valid_text_list)
            logger.info(
                f"[编辑器/正文] 使用 text_list 键入 X 正文 | 项数: 【{len(valid_text_list)}】 | 总字数: 【{total_chars}】"
            )
            page.wait_for_timeout(300)
            _type_x_text_list(page, real_editor, valid_text_list)
            page.wait_for_timeout(300)
        else:
            logger.info(f"[编辑器/正文] 开始键入 X 正文 | 字符数: 【{len(content)}】")
            page.wait_for_timeout(300)
            _type_x_body(page, real_editor, content)
            page.wait_for_timeout(300)

        try:
            current_text = (real_editor.inner_text() or "").strip()
        except Exception:
            current_text = ""

        expect_non_empty = (
            any(x.get("text", "").strip() for x in valid_text_list) if valid_text_list else bool(content.strip())
        )
        if not current_text and expect_non_empty:
            raise Exception("X 编辑器键入后内容为空，已终止发送以避免使用不安全输入兜底。")

        logger.info("[编辑器/正文] X 正文输入完成 | 结果: [Success]")

    if valid_images:
        _dismiss_x_typeahead_if_open(page)
        _upload_x_images(page, editor_container, valid_images)

    _dismiss_x_typeahead_if_open(page)
    send_btn_cands = [
        editor_container.locator('button[data-testid="tweetButtonInline"]').first,
        editor_container.locator('button[data-testid="tweetButton"]').first,
        page.locator('div[role="dialog"] button[data-testid="tweetButton"]').first,
        page.locator('button[data-testid="tweetButtonInline"]').first,
        page.locator('button[data-testid="tweetButton"]').first,
        editor_container.locator("button").filter(has_text=RE_X_SEND_EXACT).first,
    ]

    send_button = None
    try:
        send_button = _pick_trusted_send_button(send_btn_cands) or _interact_fallback_locators(
            send_btn_cands, action="wait", timeout=6000, desc="X 发帖按钮(tweetButton)"
        )
        expect(send_button).to_be_enabled(timeout=12000)
        # X 的按钮禁用有时使用 aria-disabled="true"
        end_wait = time.time() + 10.0
        while time.time() < end_wait:
            aria_disabled = send_button.get_attribute("aria-disabled")
            if aria_disabled != "true":
                break
            page.wait_for_timeout(300)
        if send_button.get_attribute("aria-disabled") == "true":
            raise PlaywrightTimeoutError("tweetButton aria-disabled 持续为 true")
    except PlaywrightTimeoutError:
        _forensics(
            page,
            "x_btn_disabled_fatal",
            {
                "btn_html": send_button.evaluate("el => el.outerHTML") if send_button else "none",
                "editor_text": real_editor.inner_text() if real_editor else "none",
            },
        )
        raise Exception(
            "X「发帖/回复」按钮持续处于禁用状态（可能正文为空、普通账号字数超出 280 字符限制，或媒体仍在处理中）。"
        )

    text_before, media_before = _snapshot_x_editor(editor_container, real_editor)
    api_success, post_id = False, None

    logger.info(
        f"[发送/提交] 触发 X「发帖/回复」并挂载 GraphQL 监听 | 主判据: <CreateTweet/CreateNoteTweet> "
        f"| 发送前编辑器: 【文本{text_before}字 / 媒体{media_before}个】"
    )
    watcher = _XApiWatcher(page)
    try:
        clicked = False
        for attempt in range(2):
            try:
                if attempt == 0:
                    human_click(page, send_button)
                else:
                    send_button.click(timeout=5000, force=True)
                clicked = True
                break
            except Exception as ce:
                logger.warning(f"[发送/提交] 第【{attempt + 1}】次点击发帖按钮失败，尝试重定位点击 | 详情: 【{str(ce)[:140]}】")

        if not clicked:
            _forensics(page, "x_send_click_fail", {"hit": _hit_test(page, send_button)})
            raise Exception("X 发帖按钮物理点击失败。")

        first_resp = watcher.wait_primary(6000)
        if first_resp is None and watcher.primary_request_count == 0:
            cur_text, cur_media = _snapshot_x_editor(editor_container, real_editor)
            content_intact = (text_before > 0 and cur_text > 0) or (media_before > 0 and cur_media > 0)
            if content_intact:
                try:
                    btn_ready = (
                        send_button.is_visible()
                        and send_button.is_enabled()
                        and send_button.get_attribute("aria-disabled") != "true"
                    )
                except Exception:
                    btn_ready = False
                if btn_ready:
                    logger.warning(
                        f"[发送/重试] 首次点击后未触发 CreateTweet 请求且编辑器内容完整"
                        f"（文本={cur_text}, 媒体={cur_media}），安全补点一次发帖按钮"
                    )
                    _dismiss_x_typeahead_if_open(page)
                    try:
                        human_click(page, send_button, timeout_ms=5000)
                    except Exception:
                        pass
                    first_resp = watcher.wait_primary(max(6000, API_WAIT_TIMEOUT_MS - 6000))
        elif first_resp is None:
            first_resp = watcher.wait_primary(max(6000, API_WAIT_TIMEOUT_MS - 6000))

        api_success, post_id = _read_x_api_verdict(
            page,
            watcher,
            pre_captured_resp=first_resp,
            wait_timeout_ms=2000 if first_resp is None else 0,
        )
    except PlaywrightTimeoutError as e:
        logger.warning(f"[发送/校验] 等待 X 接口响应超时，转入 DOM 兜底校验 | 详情: 【{str(e)[:120]}】")
    finally:
        watcher.close()

    if api_success:
        page.wait_for_timeout(1500)
        return post_id

    # DOM 兜底校验：先拦截跳转安全验证/登录页与 HTTP>=400 失败响应，严防编辑器被重定向清空导致误判成功
    page.wait_for_timeout(3000)
    health_st, health_msg = inspect_x_page_health(page)
    if health_st in ("locked_challenge", "suspended"):
        _forensics(page, "x_send_challenge_redirect", {"url": page.url, "status": health_st})
        raise ChallengeRequiredError(health_msg or f"X 发送后页面触发安全验证挑战: {page.url}")
    if health_st == "not_logged_in":
        _forensics(page, "x_send_challenge_redirect", {"url": page.url, "status": health_st})
        raise BusinessErrorException(health_msg or f"X 发送后页面跳转登录页: {page.url}")

    def _extract_toast_post_id():
        try:
            toast_link = page.locator('[data-testid="toast"] a[href*="/status/"]').first
            if toast_link.count() > 0:
                href = toast_link.get_attribute("href") or ""
                m = re.search(r"/status/(\d{15,22})", href)
                if m:
                    return m.group(1)
        except Exception:
            pass
        return None

    post_id = _extract_toast_post_id()
    if post_id:
        logger.info(f"[发送/校验] 从底部 Toast 提示捕获到新推文 ID: 【{post_id}】 | 结果: [Success]")
        return post_id

    bad_http = next((getattr(r, "status", 0) for r in reversed(watcher.primary) if getattr(r, "status", 0) >= 400), None)
    if bad_http:
        _forensics(page, "x_send_http_error", {"status": bad_http, "url": page.url})
        if bad_http in (403, 429):
            raise ChallengeRequiredError(f"X 发送主接口返回风控限流状态 HTTP {bad_http}")
        if bad_http == 401:
            raise BusinessErrorException(f"X 发送主接口鉴权失效 HTTP {bad_http}")
        raise Exception(f"X 发送主接口返回错误状态 HTTP {bad_http} 且未捕获到新帖 ID")

    text_after, media_after = _snapshot_x_editor(editor_container, real_editor)
    if (text_before > 0 and text_after == 0) or (media_before > 0 and media_after == 0):
        # 编辑器已清空时短轮询 Toast 链接复核，未拿到真实 post_id 绝不假成功放行
        for _ in range(3):
            page.wait_for_timeout(1000)
            post_id = _extract_toast_post_id()
            if post_id:
                logger.info(f"[发送/校验] 延迟 Toast 复核捕获到新推文 ID: 【{post_id}】 | 结果: [Success]")
                return post_id

        toast_txt = ""
        try:
            t_loc = page.locator('[data-testid="toast"], [role="alert"]').first
            if t_loc.count() > 0:
                toast_txt = (t_loc.inner_text() or "").strip()[:160]
        except Exception:
            pass
        _forensics(
            page,
            "x_send_unconfirmed",
            {
                "text_before": text_before,
                "text_after": text_after,
                "media_before": media_before,
                "media_after": media_after,
                "toast_text": toast_txt,
                "url": page.url,
            },
        )
        raise Exception(
            f"X 编辑器虽已清空（文本 {text_before}→{text_after}，媒体 {media_before}→{media_after}），"
            f"但未捕获 CreateTweet rest_id 或成功 Toast 状态链接（toast={toast_txt or '无'}），拒绝假成功放行。"
        )

    _forensics(
        page,
        "x_send_no_effect",
        {
            "text_before": text_before,
            "text_after": text_after,
            "media_before": media_before,
            "media_after": media_after,
        },
    )
    raise Exception(
        f"X 发帖按钮已点击但编辑器未清空且未捕获到 CreateTweet 成功响应（文本 {text_before}→{text_after}，"
        f"媒体 {media_before}→{media_after}）。"
    )


# ==============================================================================
#                     页面级可复用原语 (长驻单会话可直接调用)
# ==============================================================================
def _clean_handle(handle):
    return str(handle or "").strip().lstrip("@").split("/")[0].split("?")[0].strip()


def _parse_x_created_ts(raw_val):
    if isinstance(raw_val, (int, float)):
        v = int(raw_val)
        return v // 1000 if v > 10_000_000_000 else v
    s = str(raw_val or "").strip()
    if not s:
        return 0
    if s.isdigit():
        v = int(s)
        return v // 1000 if v > 10_000_000_000 else v
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return 0


def _unwrap_gql_user(raw_user):
    if not isinstance(raw_user, dict):
        return {}
    if raw_user.get("__typename") == "UserWithVisibilityResults" and isinstance(raw_user.get("user"), dict):
        return raw_user["user"]
    return raw_user


def _clean_tweet_text(raw_text, entities, ext_entities):
    txt = str(raw_text or "")
    for u_obj in (entities.get("urls") or []):
        if isinstance(u_obj, dict) and u_obj.get("url"):
            txt = txt.replace(u_obj["url"], u_obj.get("expanded_url") or "")
    for m_obj in (ext_entities.get("media") or []) + (entities.get("media") or []):
        if isinstance(m_obj, dict) and m_obj.get("url"):
            txt = txt.replace(m_obj["url"], "")
    return clean_post_text(_TCO_URL_RE.sub("", txt))


def _parse_gql_tweet_node(tw):
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

    user_node = _unwrap_gql_user(((tw.get("core") or {}).get("user_results") or {}).get("result"))
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

    text = _clean_tweet_text(raw_text, entities, ext_entities)
    reply_body = str(raw_text or "")
    display_range = legacy.get("display_text_range")
    stripped_by_range = False
    if is_reply and (not note_text) and isinstance(display_range, (list, tuple)) and len(display_range) >= 1:
        start_idx = safe_int(display_range[0], 0)
        if 0 < start_idx <= len(reply_body):
            prefix = reply_body[:start_idx]
            if re.fullmatch(r"(?:\s*@[A-Za-z0-9_]+)+\s*", prefix):
                reply_body = reply_body[start_idx:]
                stripped_by_range = True
    reply_body = _clean_tweet_text(reply_body, entities, ext_entities)
    if is_reply and not stripped_by_range:
        reply_body = _LEADING_MENTIONS_RE.sub("", reply_body).strip()

    media_list = ext_entities.get("media") or entities.get("media") or []
    return {
        "tweet_id": tweet_id,
        "author_id": author_id,
        "author_handle": author_handle,
        "author_handle_lower": author_handle.lower(),
        "author_name": author_name,
        "text": text,
        "reply_body_text": reply_body,
        "publish_ts": _parse_x_created_ts(legacy.get("created_at")),
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
        "has_media": bool(media_list),
        "url": f"https://x.com/{author_handle}/status/{tweet_id}" if author_handle else f"https://x.com/i/status/{tweet_id}",
    }


def _walk_gql_tweet_nodes(payload):
    found = []

    def _walk(obj):
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


def _parse_rest_notifications(payload):
    gobj = payload.get("globalObjects") or {}
    tweets_map = gobj.get("tweets") or {}
    users_map = gobj.get("users") or {}
    if not isinstance(tweets_map, dict):
        return []
    out = []
    for raw_tid, leg in tweets_map.items():
        if not isinstance(leg, dict):
            continue
        tid = str(leg.get("id_str") or raw_tid or "").strip()
        uid = str(leg.get("user_id_str") or "").strip()
        u_obj = users_map.get(uid) if isinstance(users_map, dict) else {}
        u_obj = u_obj if isinstance(u_obj, dict) else {}
        item = _parse_gql_tweet_node(
            {
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
        )
        if item:
            out.append(item)
    return out


def _detect_logged_in_identity(page, fallback_handle=""):
    uid = ""
    handle = _clean_handle(fallback_handle).lower()
    try:
        ctx = getattr(page, "context", None)
        if ctx is not None:
            for ck in ctx.cookies(["https://x.com"]):
                if ck.get("name") == "twid":
                    raw = unquote(str(ck.get("value") or "")).strip().strip('"')
                    uid = raw[2:] if raw.startswith("u=") else raw
                    break
    except Exception:
        pass
    try:
        prof_link = page.locator('a[data-testid="AppTabBar_Profile_Link"]').first
        if prof_link.count() > 0:
            href = str(prof_link.get_attribute("href") or "").strip().strip("/")
            if href and "/" not in href and href.lower() not in ("i", "home", "explore", "notifications", "messages", "search"):
                handle = href.lower()
    except Exception:
        pass
    return uid, handle


def _navigate_spa_or_goto(page, nav_selector, target_url, timeout_ms=60000):
    cur = str(getattr(page, "url", "") or "").lower()
    if "x.com/" in cur and nav_selector:
        try:
            nav_link = page.locator(nav_selector).first
            if nav_link.count() > 0 and nav_link.is_visible():
                human_click(page, nav_link, timeout_ms=5000)
                return None
        except Exception:
            pass
    return page.goto(target_url, timeout=timeout_ms, wait_until="domcontentloaded")


def post_on_x_page(
    page,
    content="",
    image_path_list=None,
    text_list=None,
    prepared_images=None,
    *,
    account_name=None,
    breaker=None,
    scroll_before=True,
):
    """在已打开并登录的 X 页面上发布主帖，返回新帖 rest_id。"""
    content = str(content or "")
    valid_text_list = _normalize_text_list(text_list)
    valid_images = prepared_images if prepared_images is not None else _prepare_image_files(image_path_list)
    if not content.strip() and not valid_text_list and not valid_images:
        raise Exception("发帖正文与有效图片不能同时为空")

    cur = str(getattr(page, "url", "") or "").lower().rstrip("/")
    response = None
    if cur != "https://x.com/home":
        response = _navigate_spa_or_goto(page, 'a[data-testid="AppTabBar_Home_Link"]', X_HOME_URL, timeout_ms=60000)
    _wait_x_home_ready(page)
    _dismiss_overlays(page, aggressive=False, desc="x-home-nav")

    if response and response.status >= 400:
        if response.status in (403, 429):
            if breaker is not None and account_name:
                breaker.trip(account_name, reason=f"home_http_{response.status}", url=page.url)
            raise ChallengeRequiredError(f"X 主页加载返回风控 HTTP {response.status}")
        raise Exception(f"X 主页加载异常 (HTTP {response.status})")

    assert_x_page_healthy(page, account_name=account_name, breaker=breaker)
    check_for_crash(page)

    if scroll_before:
        human_scroll(page, rounds=random.randint(1, 2), dy_range=(320, 680), pause_range_ms=(500, 1100))
        page.mouse.wheel(0, -1600)
        human_pause(600, 1200, page=page)

    editor_container = _locate_x_post_creator(page)
    return _submit_x_post(
        page,
        editor_container,
        content=content,
        image_path_list=image_path_list,
        text_list=text_list,
        prepared_images=valid_images,
    )


def reply_on_x_page(
    page,
    target_post_url,
    content="",
    image_path_list=None,
    like=False,
    text_list=None,
    prepared_images=None,
    *,
    account_name=None,
    breaker=None,
):
    """在已打开的 X 页面上进入目标帖子详情页并发送回复（默认不顺手点赞），返回评论 rest_id。"""
    post_url = normalize_x_post_url(target_post_url)
    if not post_url:
        raise Exception("缺少目标帖子链接或 ID: target_post_url 不能为空")
    target_post_id = extract_x_post_id(post_url)
    effective_content = str(content or "").strip()
    valid_text_list = _normalize_text_list(text_list)
    valid_images = prepared_images if prepared_images is not None else _prepare_image_files(image_path_list)
    if not effective_content and not valid_text_list and not valid_images and not like:
        raise Exception("回复内容/图片为空且未开启点赞，无需执行互动")

    response = page.goto(post_url, timeout=60000, wait_until="domcontentloaded")
    _wait_x_post_detail_ready(page)
    _dismiss_overlays(page, aggressive=False, desc="x-comment-nav")

    if response and response.status >= 400:
        if response.status in (403, 429):
            if breaker is not None and account_name:
                breaker.trip(account_name, reason=f"detail_http_{response.status}", url=page.url)
            raise ChallengeRequiredError(f"X 帖子详情页返回风控 HTTP {response.status}")
        raise Exception(f"X 帖子详情页加载异常 (HTTP {response.status})")

    assert_x_page_healthy(page, account_name=account_name, breaker=breaker)
    check_for_crash(page)

    for err_sel in ('div[data-testid="error-detail"]', 'div[data-testid="empty_state_header_text"]'):
        try:
            err_loc = page.locator(err_sel).first
            if err_loc.count() > 0 and err_loc.is_visible():
                err_txt = (err_loc.inner_text() or "").strip()[:120]
                raise Exception(f"目标 X 帖子不可用或已删除: {err_txt}")
        except Exception as exc:
            if "不可用或已删除" in str(exc):
                raise

    if like:
        try:
            focal_tweet = _locate_focal_tweet(page, post_id=target_post_id)
            already_liked = focal_tweet.locator('button[data-testid="unlike"]').first
            if already_liked.count() > 0 and already_liked.is_visible():
                logger.info("[点赞/Like] 目标 X 帖子已处于点赞状态，无需重复点击")
            else:
                like_btn = focal_tweet.locator('button[data-testid="like"]').first
                if like_btn.count() == 0:
                    like_btn = page.locator('button[data-testid="like"]').first
                if like_btn.count() > 0 and like_btn.is_visible():
                    human_click(page, like_btn)
                    human_pause(400, 900, page=page)
                    logger.info("[点赞/Like] X 帖子点赞操作执行完成 | 结果: [Success]")
        except Exception as like_e:
            logger.warning(f"[点赞/Like] 点赞操作异常（不阻断评论主流程）: {str(like_e)[:120]}")

    if not effective_content and not valid_text_list and not valid_images:
        return "LIKE_ONLY"

    reply_container = _locate_x_reply_box(page, post_id=target_post_id)
    return _submit_x_post(
        page,
        reply_container,
        content=effective_content,
        image_path_list=image_path_list,
        text_list=text_list,
        prepared_images=valid_images,
    )


def follow_on_x_page(
    page,
    target_screen_name,
    browse_seconds=(6, 12),
    *,
    account_name=None,
    breaker=None,
):
    """在已打开的 X 页面上访问目标主页、短暂自然浏览并点击关注，返回 'FOLLOWED' 或 'ALREADY_FOLLOWING'。"""
    clean_handle = _clean_handle(target_screen_name)
    if not clean_handle:
        raise Exception("缺少目标用户名: target_screen_name 不能为空")

    profile_url = f"https://x.com/{clean_handle}"
    response = page.goto(profile_url, timeout=60000, wait_until="domcontentloaded")
    try:
        page.locator(
            'div[data-testid="UserName"], '
            'div[data-testid="empty_state_header_text"], '
            'input[autocomplete="username"], '
            'a[data-testid="loginButton"]'
        ).first.wait_for(state="visible", timeout=25000)
    except Exception:
        pass

    _dismiss_overlays(page, aggressive=False, desc="x-follow-nav")
    if response and response.status >= 400:
        if response.status in (403, 429):
            if breaker is not None and account_name:
                breaker.trip(account_name, reason=f"follow_http_{response.status}", url=page.url)
            raise ChallengeRequiredError(f"X 用户主页返回风控 HTTP {response.status}")
        raise Exception(f"X 用户主页加载异常 (HTTP {response.status})")

    assert_x_page_healthy(page, account_name=account_name, breaker=breaker)
    check_for_crash(page)

    empty_hdr = page.locator('div[data-testid="empty_state_header_text"]').first
    if empty_hdr.count() > 0 and empty_hdr.is_visible():
        err_txt = (empty_hdr.inner_text() or "").strip()[:120]
        raise Exception(f"目标 X 账号不可用或不存在 (@{clean_handle}): {err_txt}")

    if browse_seconds:
        lo_s, hi_s = float(browse_seconds[0]), float(browse_seconds[1])
        human_scroll(page, rounds=1, dy_range=(280, 560), pause_range_ms=(500, 900))
        human_pause(int(lo_s * 1000), int(hi_s * 1000), page=page)
        page.mouse.wheel(0, -900)
        human_pause(400, 800, page=page)

    primary_col = page.locator('[data-testid="primaryColumn"]').first
    header_placement = primary_col.locator('div[data-testid="placementTracking"]')

    already_btn = header_placement.locator('button[data-testid$="-unfollow"], button[data-testid$="-cancel"]').first
    if already_btn.count() > 0 and already_btn.is_visible():
        return "ALREADY_FOLLOWING"

    follow_btn = header_placement.locator('button[data-testid$="-follow"]').first
    if follow_btn.count() == 0:
        follow_btn = primary_col.locator('button[data-testid$="-follow"]').first
    if follow_btn.count() == 0 or not follow_btn.is_visible():
        raise Exception(f"未能在 @{clean_handle} 主页定位到可点击的关注按钮")

    if (follow_btn.get_attribute("data-testid") or "").endswith("-unfollow"):
        return "ALREADY_FOLLOWING"

    friendship_ok = {"hit": False}

    def _on_follow_resp(resp):
        u = (resp.url or "").lower()
        if ("friendships/create" in u or "createfriendship" in u) and resp.status == 200:
            friendship_ok["hit"] = True

    page.on("response", _on_follow_resp)
    try:
        human_click(page, follow_btn)
        deadline = time.time() + 10.0
        while time.time() < deadline:
            if friendship_ok["hit"]:
                break
            unfollow_now = header_placement.locator('button[data-testid$="-unfollow"], button[data-testid$="-cancel"]').first
            if unfollow_now.count() > 0 and unfollow_now.is_visible():
                friendship_ok["hit"] = True
                break
            page.wait_for_timeout(300)
    finally:
        try:
            page.remove_listener("response", _on_follow_resp)
        except Exception:
            pass

    if friendship_ok["hit"]:
        human_pause(800, 1400, page=page)
        return "FOLLOWED"

    _forensics(page, f"x_follow_unconfirmed_{clean_handle}", {"url": profile_url})
    raise Exception(f"点击关注按钮后未检测到关注状态切换 (@{clean_handle})")


def pin_on_x_page(
    page,
    target_post_id,
    eff_handle="",
    *,
    account_name=None,
    breaker=None,
):
    """在已打开的 X 页面上置顶本人指定推文，返回 'PINNED' 或 'ALREADY_PINNED'。"""
    tid = extract_x_post_id(target_post_id)
    if not tid or tid == "unknown":
        raise Exception(f"无效的置顶推文 ID: {target_post_id}")
    clean_h = _clean_handle(eff_handle)
    target_url = f"https://x.com/{clean_h}/status/{tid}" if clean_h else f"https://x.com/i/status/{tid}"

    response = page.goto(target_url, timeout=60000, wait_until="domcontentloaded")
    _wait_x_post_detail_ready(page)
    _dismiss_overlays(page, aggressive=False, desc="x-pin-detail")
    if response and response.status >= 400:
        if response.status in (403, 429):
            if breaker is not None and account_name:
                breaker.trip(account_name, reason=f"pin_http_{response.status}", url=page.url)
            raise ChallengeRequiredError(f"X 置顶页返回风控 HTTP {response.status}")
        raise Exception(f"X 帖子详情页加载异常 (HTTP {response.status})")

    assert_x_page_healthy(page, account_name=account_name, breaker=breaker)
    check_for_crash(page)

    focal = _locate_focal_tweet(page, post_id=tid)
    caret_btn = focal.locator('button[data-testid="caret"]').first
    if caret_btn.count() == 0:
        raise Exception(f"未能定位推文 {tid} 右上角菜单按钮 (caret)")
    human_click(page, caret_btn)
    human_pause(500, 1000, page=page)

    unpin_item = page.locator('div[role="menuitem"][data-testid="unpin"]').first
    if unpin_item.count() == 0:
        unpin_item = page.locator('div[role="menuitem"]').filter(has_text=RE_UNPIN_MENU_TEXT).first
    if unpin_item.count() > 0 and unpin_item.is_visible():
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass
        return "ALREADY_PINNED"

    pin_item = page.locator('div[role="menuitem"][data-testid="pin"]').first
    if pin_item.count() == 0:
        pin_item = page.locator('div[role="menuitem"]').filter(has_text=RE_PIN_MENU_TEXT).first
    if pin_item.count() == 0 or not pin_item.is_visible():
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass
        raise Exception(f"推文 {tid} 菜单中未找到置顶选项")

    human_click(page, pin_item)
    confirm_btn = page.locator('button[data-testid="confirmationSheetConfirm"]').first
    confirm_btn.wait_for(state="visible", timeout=6000)

    pin_ok = {"hit": False}

    def _on_pin_resp(resp):
        if resp.status == 200 and RE_PIN_API.search(resp.url or ""):
            pin_ok["hit"] = True

    page.on("response", _on_pin_resp)
    try:
        human_click(page, confirm_btn)
        deadline = time.time() + 8.0
        while time.time() < deadline:
            if pin_ok["hit"]:
                break
            page.wait_for_timeout(250)
    finally:
        try:
            page.remove_listener("response", _on_pin_resp)
        except Exception:
            pass

    human_pause(800, 1500, page=page)
    return "PINNED"


def sniff_notifications_on_page(
    page,
    own_handle="",
    scroll_rounds=2,
    *,
    account_name=None,
    breaker=None,
):
    """
    通过站内点击 Notifications 入口自然跳转并旁路嗅探收到的新回复，零高风险详情页连扫。
    返回: `{"own_handle": ..., "own_uid": ..., "notifications_count": ..., "items": [...], "tweets_by_id": {...}}`
    """
    tweets_by_id = {}
    mention_tweets = {}
    notif_hits = {"count": 0}

    def _on_notif_resp(resp):
        try:
            url = resp.url or ""
            if resp.status != 200 or not RE_NOTIF_API.search(url):
                return
            payload = resp.json()
            if not isinstance(payload, dict):
                return
            if "notifications/" in url.lower() and ".json" in url.lower():
                parsed = _parse_rest_notifications(payload)
            else:
                parsed = [p for n in _walk_gql_tweet_nodes(payload) if (p := _parse_gql_tweet_node(n)) is not None]
            for p in parsed:
                tid = p["tweet_id"]
                tweets_by_id[tid] = p
                if (not p["is_retweet"]) and p["in_reply_to_status_id"]:
                    mention_tweets[tid] = p
            notif_hits["count"] += 1
        except Exception:
            pass

    page.on("response", _on_notif_resp)
    try:
        cur = str(getattr(page, "url", "") or "").lower()
        if "x.com/" not in cur:
            page.goto(X_HOME_URL, timeout=60000, wait_until="domcontentloaded")
            _wait_x_home_ready(page)
            assert_x_page_healthy(page, account_name=account_name, breaker=breaker)

        notif_url = "https://x.com/notifications"
        if cur.rstrip("/") == notif_url:
            page.reload(timeout=45000, wait_until="domcontentloaded")
        else:
            _navigate_spa_or_goto(page, 'a[data-testid="AppTabBar_Notifications_Link"]', notif_url, timeout_ms=45000)
            nav_deadline = time.time() + 2.5
            while time.time() < nav_deadline:
                if "/notifications" in str(getattr(page, "url", "") or "").lower():
                    break
                page.wait_for_timeout(200)
            if "/notifications" not in str(getattr(page, "url", "") or "").lower():
                page.goto(notif_url, timeout=45000, wait_until="domcontentloaded")

        deadline = time.time() + 10.0
        while time.time() < deadline:
            if notif_hits["count"] > 0:
                break
            page.wait_for_timeout(250)

        human_pause(900, 1600, page=page)
        _dismiss_overlays(page, aggressive=False, desc="x-notif-sniff")
        assert_x_page_healthy(page, account_name=account_name, breaker=breaker)
        check_for_crash(page)

        if scroll_rounds > 0:
            human_scroll(page, rounds=scroll_rounds, dy_range=(900, 1600), pause_range_ms=(900, 1600))
    finally:
        try:
            page.remove_listener("response", _on_notif_resp)
        except Exception:
            pass

    own_uid, eff_handle = _detect_logged_in_identity(page, fallback_handle=own_handle)
    my_tweet_ids = {
        tw["tweet_id"]
        for tw in tweets_by_id.values()
        if (not tw.get("is_retweet"))
        and (
            (eff_handle and tw.get("author_handle_lower") == eff_handle)
            or (own_uid and tw.get("author_id") == own_uid)
        )
    }

    items = []
    for rep in sorted(mention_tweets.values(), key=lambda x: safe_int(x.get("publish_ts"), 0), reverse=True):
        is_self = (eff_handle and rep.get("author_handle_lower") == eff_handle) or (
            own_uid and rep.get("author_id") == own_uid
        )
        if is_self:
            continue
        parent_id = str(rep.get("in_reply_to_status_id") or "").strip()
        conv_id = str(rep.get("conversation_id") or parent_id).strip()
        targets_me = (
            (parent_id in my_tweet_ids)
            or (bool(eff_handle) and rep.get("in_reply_to_screen_name") == eff_handle)
            or (bool(own_uid) and rep.get("in_reply_to_user_id") == own_uid)
        )
        if not targets_me:
            continue
        parent_tw = tweets_by_id.get(parent_id) or tweets_by_id.get(conv_id) or {}
        items.append(
            {
                "reply_id": rep["tweet_id"],
                "post_id": conv_id or parent_id,
                "in_reply_to_status_id": parent_id,
                "conversation_id": conv_id,
                "reply_author": rep.get("author_handle") or "",
                "reply_author_lower": rep.get("author_handle_lower") or "",
                "reply_text": rep.get("reply_body_text") or rep.get("text") or "",
                "reply_raw_text": rep.get("text") or "",
                "reply_created_ts": safe_int(rep.get("publish_ts"), 0),
                "reply_has_media": bool(rep.get("has_media")),
                "reply_url": rep.get("url") or f"https://x.com/i/status/{rep['tweet_id']}",
                "parent_text": parent_tw.get("text") or "",
                "parent_has_media": bool(parent_tw.get("has_media")),
                "parent_publish_ts": safe_int(parent_tw.get("publish_ts"), 0),
            }
        )

    return {
        "own_handle": eff_handle,
        "own_uid": own_uid,
        "notifications_count": notif_hits["count"],
        "items": items,
        "tweets_by_id": tweets_by_id,
    }


# ==============================================================================
#                     主控入口函数 (发帖 / 评论 / 登录与人工接管)
# ==============================================================================
def _run_x_browser_action(
    user_data_dir,
    *,
    debug=True,
    use_proxy=True,
    task_tag="[任务/Main]",
    forensic_tag="x_action_error",
    forensic_url=X_HOME_URL,
    fn=None,
):
    """统一 X 持久化浏览器动作封装（Patchright + 本机有头 Chrome，零 headless 伪装与看门狗注入）。"""
    if not debug:
        logger.info(f"{task_tag} 已强制启用本机正版有头 Chrome 真实指纹模式（已禁用 headless=new / 假 UA 伪装）")
    try:
        with XBrowserSession(
            user_data_dir,
            use_proxy=use_proxy,
            args=["--disable-restore-session-state", "--no-default-browser-check", "--mute-audio"],
        ) as session:
            page = session.page
            try:
                return fn(session.context, page)
            except BusinessErrorException as biz_e:
                error_info = f"[业务拦截] {biz_e}"
                logger.error(f"{task_tag} X 操作被服务端业务规则阻断 | 原因: 【{error_info}】")
                return error_info, False, None
            except Exception as e:
                error_info = f"[{type(e).__name__}] {e}\n[Traceback]:\n{traceback.format_exc()}"
                try:
                    _forensics(page, forensic_tag, {"url": forensic_url, "error": error_info[:2000]})
                except Exception:
                    pass
                _alert_failure_scene(error_info)
                return error_info, False, None
    except UserDataDirLockTimeoutError as lock_e:
        error_info = f"[UserDataDirLockTimeout] {lock_e}"
        logger.error(f"{task_tag} {error_info}")
        return error_info, False, None
    except Exception as core_e:
        error_info = f"[CoreEngineCrash] Patchright 底层崩溃:\n{core_e}"
        logger.error(error_info)
        return error_info, False, None


def create_x_post(
    content="",
    image_path_list=None,
    user_data_dir=USER_DATA_DIR,
    debug=True,
    text_list=None,
    use_proxy=True,
):
    """
    主控入口 1：调度浏览器打开 X (https://x.com/home) 并执行【图文/纯文本自动发帖】全流程。

    返回:
        Tuple(错误信息(str|None), 是否成功(bool), 帖子ID(str|None))
    """
    if not os.path.isdir(user_data_dir):
        return (
            f"缺少 X 浏览器用户环境: {user_data_dir}，请先运行 open_browser_for_manual_use(USER_DATA_DIR) 登录 X 账号",
            False,
            None,
        )

    content = str(content or "")
    valid_text_list = _normalize_text_list(text_list)
    valid_images = _prepare_image_files(image_path_list)

    if not content.strip() and not valid_text_list and not valid_images:
        return "发帖正文与有效图片不能同时为空", False, None

    text_summary = (
        f"text_list({len(valid_text_list)}项/{sum(len(x['text']) for x in valid_text_list)}字)"
        if valid_text_list
        else f"{len(content)}字"
    )
    proxy_server = get_proxy_url() if use_proxy else None

    logger.info(
        f"\n{'=' * 70}\n[任务/Main] 启动 X (Twitter) 自动化发帖 | URL: <{X_HOME_URL}> "
        f"| 正文: 【{text_summary}】 | 有效图片: 【{len(valid_images)}】张 "
        f"| 代理: 【{proxy_server or '直连'}】 | 模式: 【Patchright有头真Chrome】\n{'=' * 70}"
    )

    def _do_post(_context, page):
        post_id = post_on_x_page(
            page,
            content=content,
            image_path_list=image_path_list,
            text_list=text_list,
            prepared_images=valid_images,
        )
        post_url = f"https://x.com/i/status/{post_id}" if post_id else "未知"
        logger.info(
            f"[任务/Main] X 帖子发布成功 | 帖子ID(rest_id): 【{post_id}】 | 链接: <{post_url}> | 结果: [Success]"
        )
        return None, True, post_id

    return _run_x_browser_action(
        user_data_dir,
        debug=debug,
        use_proxy=use_proxy,
        task_tag="[任务/Main]",
        forensic_tag="x_post_error",
        forensic_url=X_HOME_URL,
        fn=_do_post,
    )


def comment_on_x_post(
    target_post_url,
    content="",
    image_path_list=None,
    like=False,
    user_data_dir=USER_DATA_DIR,
    debug=True,
    text_list=None,
    use_proxy=True,
    comment="",
):
    """
    主控入口 2：打开指定 X 帖子详情页（支持完整 URL 或纯数字 tweet_id）并执行【点赞(可选) + 回复评论(支持文字与配图)】全流程。

    返回:
        Tuple(错误信息(str|None), 是否成功(bool), 评论ID(str|None))
    """
    post_url = normalize_x_post_url(target_post_url)
    if not post_url:
        return "缺少目标帖子链接或ID: target_post_url 不能为空", False, None

    target_post_id = extract_x_post_id(post_url)
    effective_content = str(content if content else (comment or "")).strip()
    valid_text_list = _normalize_text_list(text_list)
    valid_images = _prepare_image_files(image_path_list)

    if not effective_content and not valid_text_list and not valid_images and not like:
        return "回复内容/图片为空且未开启点赞，无需执行互动", False, None

    if not os.path.isdir(user_data_dir):
        return (
            f"缺少 X 浏览器用户环境: {user_data_dir}，请先运行 open_browser_for_manual_use(USER_DATA_DIR) 登录 X 账号",
            False,
            None,
        )

    text_summary = (
        f"text_list({len(valid_text_list)}项/{sum(len(x['text']) for x in valid_text_list)}字)"
        if valid_text_list
        else f"{len(effective_content)}字"
    )
    proxy_server = get_proxy_url() if use_proxy else None

    logger.info(
        f"\n{'=' * 70}\n[任务/Comment] 启动 X 自动化评论/互动 | 目标帖子ID: 【{target_post_id}】 | URL: <{post_url}> "
        f"| 评论正文: 【{text_summary}】 | 评论配图: 【{len(valid_images)}】张 | 是否点赞: 【{like}】 "
        f"| 代理: 【{proxy_server or '直连'}】 | 模式: 【Patchright有头真Chrome】\n{'=' * 70}"
    )

    def _do_comment(_context, page):
        comment_id = reply_on_x_page(
            page,
            target_post_url=post_url,
            content=effective_content,
            image_path_list=image_path_list,
            like=like,
            text_list=text_list,
            prepared_images=valid_images,
        )
        if comment_id == "LIKE_ONLY":
            logger.info("[任务/Comment] 仅点赞模式执行完毕 | 结果: [Success]")
            return None, True, "LIKE_ONLY"
        comment_url = f"https://x.com/i/status/{comment_id}" if comment_id else "未知"
        logger.info(
            f"[任务/Comment] X 评论回复发布成功 | 原帖ID: 【{target_post_id}】 "
            f"| 评论ID(rest_id): 【{comment_id}】 | 链接: <{comment_url}> | 结果: [Success]"
        )
        return None, True, comment_id

    return _run_x_browser_action(
        user_data_dir,
        debug=debug,
        use_proxy=use_proxy,
        task_tag="[任务/Comment]",
        forensic_tag=f"x_comment_error_{target_post_id}",
        forensic_url=post_url,
        fn=_do_comment,
    )


def follow_x_user(
    target_screen_name,
    user_data_dir=USER_DATA_DIR,
    debug=True,
    use_proxy=True,
):
    """
    主控入口 2.5：打开指定 X 用户个人主页（`https://x.com/{screen_name}`）并执行安全幂等关注操作。

    返回:
        Tuple(错误信息(str|None), 是否成功(bool), 状态标识(str|None: "FOLLOWED" | "ALREADY_FOLLOWING"))
    """
    clean_handle = _clean_handle(target_screen_name)
    if not clean_handle:
        return "缺少目标用户名: target_screen_name 不能为空", False, None

    if not os.path.isdir(user_data_dir):
        return (
            f"缺少 X 浏览器用户环境: {user_data_dir}，请先登录 X 账号保存会话",
            False,
            None,
        )

    profile_url = f"https://x.com/{clean_handle}"
    proxy_server = get_proxy_url() if use_proxy else None

    logger.info(
        f"\n{'=' * 70}\n[任务/Follow] 启动 X 自动化关注 | 目标账号: 【@{clean_handle}】 | URL: <{profile_url}> "
        f"| 代理: 【{proxy_server or '直连'}】 | 模式: 【Patchright有头真Chrome】\n{'=' * 70}"
    )

    def _do_follow(_context, page):
        status = follow_on_x_page(page, clean_handle)
        logger.info(f"[任务/Follow] X 关注完成 | 账号: 【@{clean_handle}】 | 状态: [{status}]")
        return None, True, status

    return _run_x_browser_action(
        user_data_dir,
        debug=debug,
        use_proxy=use_proxy,
        task_tag="[任务/Follow]",
        forensic_tag=f"x_follow_error_{clean_handle}",
        forensic_url=profile_url,
        fn=_do_follow,
    )


def login_and_save_session(user_data_dir=USER_DATA_DIR, use_proxy=True):
    """打开真实有头 Chrome 供人工登录 X 账号，按 Enter 后关闭并保留完整缓存与登录态。"""
    logger.info(f"[登录/Auth] 准备手动登录 X 账号 | 存储路径: <{user_data_dir}>")
    with XBrowserSession(user_data_dir, use_proxy=use_proxy) as session:
        session.page.goto(X_HOME_URL, timeout=60000, wait_until="domcontentloaded")
        input("\n[登录/Auth] 等待操作 | 动作: 【在浏览器中完成 X 登录并进入主页后，请按 Enter 键保存会话并退出】")
        logger.info("[登录/Auth] X 登录会话已固化到本地 | 结果: [Success]")


def open_browser_for_manual_use(user_data_dir=USER_DATA_DIR, home_url=X_HOME_URL, use_proxy=True):
    """主控入口 3：启动真实有头 Chrome 交由人工自由操作（直接关闭窗口即自动保存退出，不清理缓存）。"""
    logger.info(f"\n{'=' * 60}\n[人工/Manual] 启动本地正版 Chrome 交接控制权 | 目录: <{user_data_dir}>\n{'=' * 60}")
    with XBrowserSession(user_data_dir, use_proxy=use_proxy) as session:
        try:
            session.page.goto(home_url, wait_until="commit", timeout=60000)
        except Exception as nav_err:
            logger.warning(f"[人工/Manual] 初始页面导航偏慢或遇网络波动 ({nav_err})，浏览器仍保持开启。")
        logger.info("[人工/Manual] ✅ 浏览器已就绪，控制权已交接 | 🛑 退出方式: 【直接关闭浏览器窗口，程序自动结束】")
        while session.context and session.context.pages:
            try:
                session.context.pages[-1].wait_for_event("close", timeout=1000)
            except PlaywrightTimeoutError:
                continue
            except Exception:
                break
        logger.info("[人工/Manual] 👋 窗口已关闭，控制权收回，系统资源已释放。\n")


# ==============================================================================
#                                   使用示例（默认安全不执行）
# 如需单步直调底层动作，请取消对应示例注释；日常队列调度与长驻守护请优先直接运行 platforms/x_ops_daemon.py
# ==============================================================================
if __name__ == "__main__":
    pass

    # ==========================================================================
    # 示例 1：首次使用 —— 手动登录 X 账号并持久化保存浏览器 Session
    # （登录成功后直接关闭弹出的浏览器窗口，登录态会自动保存在 USER_DATA_DIR 中）
    # ==========================================================================
    # open_browser_for_manual_use(USER_DATA_DIR)

    # ==========================================================================
    # 示例 2：自动发布 X 帖子（传入文字 + 图片地址列表）
    # image_path_list 支持本地绝对路径、media_downloads 下文件名、或网络图片 URL（最多 4 张图）
    # ==========================================================================
    # post_text = (
    #     "$BTC 链上筹码结构正在发生微妙变化。\n\n"
    #     "短期投机资金在震荡中不断洗出，而长线地址的持仓水位依然稳如泰山。"
    # )
    # err, success, new_tweet_id = create_x_post(
    #     content=post_text,
    #     image_path_list=[],
    #     user_data_dir=USER_DATA_DIR,
    #     debug=True,
    # )
    # if success:
    #     logger.info(f"[结果/Final] 🎉 发帖成功 | 帖子ID: 【{new_tweet_id}】")
    # else:
    #     logger.error(f"[结果/Final] ❌ 发帖失败 | 错误原因:\n{err}")

    # ==========================================================================
    # 示例 3：对指定的 X 帖子进行自动回复（支持直接传推文链接或纯数字 ID）
    # ==========================================================================
    # c_err, c_success, comment_id = comment_on_x_post(
    #     target_post_url="https://x.com/i/status/请替换为目标帖子ID",
    #     content="补充一点观察：目前永续合约资金费率已回到中性区间。",
    #     image_path_list=[],
    #     like=False,  # False 表示仅回复不点赞，降低双写风控特征
    #     user_data_dir=USER_DATA_DIR,
    #     debug=True,
    # )
    # if c_success:
    #     logger.info(f"[结果/Final] 🎉 评论成功 | 评论ID: 【{comment_id}】")
    # else:
    #     logger.error(f"[结果/Final] ❌ 评论失败 | 错误原因:\n{c_err}")
