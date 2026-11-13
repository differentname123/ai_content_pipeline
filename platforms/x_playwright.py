# -*- coding: utf-8 -*-
"""
X (Twitter) Playwright 浏览器自动化独立模块：
- 自动发帖 (create_x_post)：支持纯文本、分段键入 (text_list)、多图上传（自动兼容本地路径与网络图片URL）
- 自动评论/互动 (comment_on_x_post)：支持对指定 X 帖子链接或帖子 ID 进行回复评论（支持带图评论、分段键入）与点赞
- 手动登录固化 (login_and_save_session) 与人工接管浏览器窗口 (open_browser_for_manual_use)
- 底层锁、缓存清理、浮层免疫、光标控制与故障取证统一复用 platforms.playwright_base
"""
import concurrent.futures
import os
import re
import sys
import time
import traceback
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import expect, sync_playwright

from common.common_utils import (
    download_web_media,
    get_media_save_dir,
    get_proxy_url,
    resolve_session_dir,
    setup_logger,
)
from platforms.playwright_base import (
    TYPE_CHUNK_SIZE,
    USER_DATA_LOCK_TIMEOUT_SEC,
    BusinessErrorException,
    PageCrashedException,
    UserDataDirLockTimeoutError,
    _alert_failure_scene,
    _BaseApiWatcher,
    _dismiss_overlays,
    _fmt_hit,
    _force_caret_to_tail,
    _forensics,
    _hit_test,
    _interact_fallback_locators,
    _launch_persistent,
    _normalize_text_list,
    _parse_api_json,
    _pick_trusted_send_button,
    _robust_click,
    _type_body,
    _user_data_dir_lock,
    check_for_crash,
    clean_browser_cache,
    install_overlay_guard,
    open_browser_for_manual_use as _base_open_browser_for_manual_use,
    report_guard_hits,
)

logger = setup_logger(app_name="x_playwright")

# ==============================================================================
#                                   运行配置
# ==============================================================================
USER_DATA_DIR = resolve_session_dir("x_xiaohao_browser_session_dir", "x_myself")
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
            _robust_click(side_post_btn)
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
                _robust_click(placeholder)
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
            _robust_click(reply_icon_btn)
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


def _wake_x_editor(page, editor_container, max_round=4):
    """唤醒 X Draft.js 富文本编辑器并确保焦点落入其中。返回 real_editor(Locator)。"""
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
            logger.warning(f"[编辑器/唤醒] 第{rnd}轮输入区不可命中，先清障再强点 | 详情: 【{_fmt_hit(last_hit)}】")

        try:
            if rnd == 1:
                target.click(timeout=6000)
            elif rnd == 2:
                target.click(timeout=5000, force=True)
            elif rnd == 3:
                target.evaluate(
                    """(el) => {
                        el.scrollIntoView({block:'center'});
                        const o = {bubbles:true, cancelable:true, view:window};
                        try { el.dispatchEvent(new PointerEvent('pointerdown', o)); } catch(e) {}
                        try { el.dispatchEvent(new MouseEvent('mousedown', o)); } catch(e) {}
                        if (el.focus) el.focus();
                        try { el.dispatchEvent(new PointerEvent('pointerup', o)); } catch(e) {}
                        try { el.dispatchEvent(new MouseEvent('mouseup', o)); } catch(e) {}
                        try { el.dispatchEvent(new MouseEvent('click', o)); } catch(e) {}
                    }"""
                )
            else:
                box = target.bounding_box()
                if box:
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
            if not focused:
                try:
                    real_editor.evaluate("el => el.focus()")
                except Exception:
                    pass
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
    针对 X Draft.js 富文本编辑器的安全键入实现：
    - 按行拆分键入，换行前自动检测并关闭 typeaheadDropdown 候选弹窗，杜绝 $SYMBOL / #Tag 回车被吞；
    - 每行内部复用 _type_body 分块键入与落字校验。
    """
    if not text:
        return
    normalized = str(text).replace("\r\n", "\n").replace("\r", "\n")
    lines = normalized.split("\n")
    for idx, line in enumerate(lines):
        if line:
            _type_body(page, real_editor, line)
            _dismiss_x_typeahead_if_open(page)
        if idx < len(lines) - 1:
            _dismiss_x_typeahead_if_open(page)
            try:
                real_editor.press("Enter", timeout=3000)
            except Exception:
                page.keyboard.press("Enter")
            page.wait_for_timeout(80)
    _dismiss_x_typeahead_if_open(page)


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
    1. 优先通过 input[data-testid='fileInput'] 直接批量注入文件；
    2. 后备通过点击工具栏媒体按钮触发 FileChooser 上传；
    3. 若多图一次性注入异常，自动降级为逐张上传；
    4. 等待缩略图 [data-testid='attachments'] 渲染并确保上传进度条结束。
    """
    if not valid_images:
        return

    uploaded = False

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
                _robust_click(img_btn)
            fc = fc_info.value
            if len(valid_images) > 1 and not fc.is_multiple():
                fc.set_files(valid_images[0])
                page.wait_for_timeout(1200)
                for img_path in valid_images[1:]:
                    with page.expect_file_chooser(timeout=5000) as next_fc_info:
                        _robust_click(img_btn)
                    next_fc_info.value.set_files(img_path)
                    page.wait_for_timeout(1200)
            else:
                fc.set_files(valid_images)
            uploaded = True
            logger.info(f"[编辑器/图片] 通过【FileChooser】成功提交图片 | 数量: 【{len(valid_images)}】张")
        except Exception as fc_err:
            logger.warning(f"[编辑器/图片] 图片上传通道全部失败，降级为纯文本发送 | 原因: 【{str(fc_err)[:150]}】")
            return

    # 等待附件预览渲染完成
    try:
        expect(
            editor_container.locator(
                '[data-testid="attachments"] img, '
                '[data-testid="tweetPhoto"] img, '
                'img[src^="blob:"]'
            ).first
        ).to_be_visible(timeout=15000)
    except Exception:
        pass

    # 等待图片上传进度条消失（若有）
    end_wait = time.time() + 20.0
    while time.time() < end_wait:
        try:
            progress = editor_container.locator('[data-testid="attachments"] [role="progressbar"]')
            if progress.count() == 0 or not progress.first.is_visible():
                break
        except Exception:
            break
        page.wait_for_timeout(400)

    page.wait_for_timeout(1500)


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


def _read_x_api_verdict(page, watcher):
    """
    解读 X 发帖/回复接口响应结果：
    - 主判据：POST /i/api/graphql/.../CreateTweet (或 CreateNoteTweet) 返回 rest_id；
    - 若包含 errors 且无 rest_id，则抛出 BusinessErrorException。
    """
    resp = watcher.wait_primary(API_WAIT_TIMEOUT_MS)
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
            raise BusinessErrorException(f"X 业务发送被服务端拦截 (code={err_code})，原因: {err_msg}")

        if resp.status >= 400:
            logger.warning(f"[发送/校验] X 主接口返回 HTTP {resp.status}，转入 DOM 兜底校验")
            return False, None

    sec = watcher.latest_secondary()
    if sec is not None:
        body = _parse_api_json(sec)
        tweet_id = _extract_tweet_id_from_graphql_body(body)
        if tweet_id:
            logger.info(f"[发送/校验] 辅助接口确认 X 发帖成功 | 帖子ID: 【{tweet_id}】 | 结果: [Success]")
            return True, tweet_id

    logger.warning(f"[发送/校验] 【{API_WAIT_TIMEOUT_MS}ms】 内未从接口提取到 rest_id，转入 DOM 兜底校验")
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
    report_guard_hits(page, "x-pre-submit")

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
            logger.info(f"[编辑器/正文] 开始键入 X 正文 | 字符数: 【{len(content)}】 | 分块: 【{TYPE_CHUNK_SIZE}】")
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
            logger.warning("[编辑器/正文] 首次键入后编辑器为空，触发 insert_text 兜底补录")
            _wake_x_editor(page, editor_container, max_round=2)
            full_fallback_text = (
                "".join(x.get("text", "") for x in valid_text_list) if valid_text_list else content
            )
            page.keyboard.insert_text(full_fallback_text)
            page.wait_for_timeout(400)
            _dismiss_x_typeahead_if_open(page)

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
        for attempt in range(3):
            try:
                if attempt == 0:
                    _robust_click(send_button)
                elif attempt == 1:
                    send_button.click(timeout=5000, force=True)
                else:
                    send_button.evaluate("el => el.click()")
                clicked = True
                break
            except Exception as ce:
                logger.warning(f"[发送/提交] 第【{attempt + 1}】次点击发帖按钮失败，升级重试 | 详情: 【{str(ce)[:140]}】")

        if not clicked:
            _forensics(page, "x_send_click_fail", {"hit": _hit_test(page, send_button)})
            raise Exception("X 发帖按钮点击 3 级降级全部失败。")

        api_success, post_id = _read_x_api_verdict(page, watcher)
    except PlaywrightTimeoutError as e:
        logger.warning(f"[发送/校验] 等待 X 接口响应超时，转入 DOM 兜底校验 | 详情: 【{str(e)[:120]}】")
    finally:
        watcher.close()

    if api_success:
        page.wait_for_timeout(1500)
        return post_id

    # DOM 兜底校验：检查 Toast 提示中的新推文链接或编辑器是否已清空
    page.wait_for_timeout(3000)
    try:
        toast_link = page.locator('[data-testid="toast"] a[href*="/status/"]').first
        if toast_link.count() > 0:
            href = toast_link.get_attribute("href") or ""
            m = re.search(r"/status/(\d{15,22})", href)
            if m:
                post_id = m.group(1)
                logger.info(f"[发送/校验] 从底部 Toast 提示捕获到新推文 ID: 【{post_id}】 | 结果: [Success]")
                return post_id
    except Exception:
        pass

    text_after, media_after = _snapshot_x_editor(editor_container, real_editor)
    if (text_before > 0 and text_after == 0) or (media_before > 0 and media_after == 0):
        logger.info(
            f"[发送/校验] DOM 兜底比对通过（X 编辑器已清空） | 文本: 【{text_before}→{text_after}】 "
            f"| 媒体: 【{media_before}→{media_after}】 | 结果: [Success]"
        )
        return post_id

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
#                     主控入口函数 (发帖 / 评论 / 登录与人工接管)
# ==============================================================================
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

    参数:
        content (str): 要发布的推文正文文本。
        image_path_list (str | list[str] | None): 图片地址（支持本地绝对路径、media_downloads 文件名、或 http/https 网络图片链接，最多 4 张）。
        user_data_dir (str): 浏览器持久化用户目录，默认读取配置中的 x_myself_browser_session_dir。
        debug (bool): True 为可见窗口模式，False 为离屏后台模式。
        text_list (list[dict] | None): 可选分段键入列表，形如 [{"text": "...", "delay": 1.0}]。
        use_proxy (bool): 是否挂载 config.json 中配置的代理（默认 True）。

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
        f"| 代理: 【{proxy_server or '直连'}】 | 模式: 【{'debug可见' if debug else '离屏后台'}】\n{'=' * 70}"
    )

    anti_freeze_args = ["--disable-restore-session-state", "--no-default-browser-check"]
    offscreen_args = [
        "--disable-blink-features=AutomationControlled",
        "--disable-gpu",
        "--window-position=-10000,-10000",
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-renderer-backgrounding",
        "--disable-background-timer-throttling",
        "--disable-backgrounding-occluded-windows",
        "--disable-features=CalculateNativeWinOcclusion",
        "--disable-breakpad",
        "--force-device-scale-factor=1",
        "--hide-scrollbars",
    ] + anti_freeze_args

    debug_args = [
        "--disable-blink-features=AutomationControlled",
        "--start-maximized",
        "--window-position=0,0",
    ] + anti_freeze_args

    try:
        with _user_data_dir_lock(user_data_dir, timeout=USER_DATA_LOCK_TIMEOUT_SEC):
            with sync_playwright() as p:
                context = None
                try:
                    context = _launch_persistent(
                        p,
                        user_data_dir,
                        args=debug_args if debug else offscreen_args,
                        viewport=None if debug else {"width": 1920, "height": 1080},
                        debug=debug,
                        proxy=proxy_server,
                    )
                    context.set_default_timeout(60000)
                    context.set_default_navigation_timeout(60000)

                    page = context.new_page()
                    for old_page in context.pages:
                        if old_page != page:
                            try:
                                old_page.close()
                            except Exception:
                                pass
                    page.bring_to_front()

                    install_overlay_guard(page)

                    response = page.goto(X_HOME_URL, timeout=60000, wait_until="domcontentloaded")
                    _wait_x_home_ready(page)
                    _dismiss_overlays(page, aggressive=False, desc="x-home-nav")
                    report_guard_hits(page, "x-home-nav")

                    if response and response.status >= 400:
                        return f"X 主页加载异常 (HTTP {response.status})", False, None

                    login_err = _check_x_login_state(page)
                    if login_err:
                        return login_err, False, None

                    check_for_crash(page)

                    try:
                        editor_container = _locate_x_post_creator(page)
                    except Exception as e:
                        login_err = _check_x_login_state(page)
                        if login_err:
                            return login_err, False, None
                        return f"无法定位 X 发帖编辑器: {e}", False, None

                    post_id = _submit_x_post(
                        page,
                        editor_container,
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

                except BusinessErrorException as biz_e:
                    error_info = f"[业务拦截] {biz_e}"
                    logger.error(f"[任务/Main] X 发帖被服务端业务规则阻断 | 原因: 【{error_info}】")
                    return error_info, False, None

                except Exception as e:
                    error_info = f"[{type(e).__name__}] {e}\n[Traceback]:\n{traceback.format_exc()}"
                    if context and context.pages:
                        try:
                            _forensics(
                                context.pages[0], "x_post_error", {"url": X_HOME_URL, "error": error_info[:2000]}
                            )
                        except Exception:
                            pass
                    _alert_failure_scene(error_info)
                    return error_info, False, None

                finally:
                    if context:
                        try:
                            context.close()
                        except Exception:
                            pass

    except UserDataDirLockTimeoutError as lock_e:
        error_info = f"[UserDataDirLockTimeout] {lock_e}"
        logger.error(f"[任务/Main] {error_info}")
        return error_info, False, None

    except Exception as core_e:
        error_info = f"[CoreEngineCrash] Playwright底层崩溃:\n{core_e}"
        logger.error(error_info)
        return error_info, False, None


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

    参数:
        target_post_url (str | int): 目标 X 帖子链接（如 "https://x.com/xxx/status/123456"）或纯数字帖子 ID。
        content (str): 评论/回复正文文本（与 comment 参数等价，二者传其一即可）。
        image_path_list (str | list[str] | None): 评论附带的图片路径或网络图片 URL（可选，最多 4 张）。
        like (bool): 是否在评论前顺带对该帖子点赞（默认 False）。
        user_data_dir (str): 浏览器持久化用户目录，默认 USER_DATA_DIR。
        debug (bool): True 为可见窗口模式，False 为离屏后台模式。
        text_list (list[dict] | None): 可选分段键入列表，形如 [{"text": "...", "delay": 1.0}]。
        use_proxy (bool): 是否挂载 config.json 中配置的代理（默认 True）。
        comment (str): content 的兼容别名参数。

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
        f"| 代理: 【{proxy_server or '直连'}】 | 模式: 【{'debug可见' if debug else '离屏后台'}】\n{'=' * 70}"
    )

    anti_freeze_args = ["--disable-restore-session-state", "--no-default-browser-check"]
    offscreen_args = [
        "--disable-blink-features=AutomationControlled",
        "--disable-gpu",
        "--window-position=-10000,-10000",
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-renderer-backgrounding",
        "--disable-background-timer-throttling",
        "--disable-backgrounding-occluded-windows",
        "--disable-features=CalculateNativeWinOcclusion",
        "--disable-breakpad",
        "--force-device-scale-factor=1",
        "--hide-scrollbars",
    ] + anti_freeze_args

    debug_args = [
        "--disable-blink-features=AutomationControlled",
        "--start-maximized",
        "--window-position=0,0",
    ] + anti_freeze_args

    try:
        with _user_data_dir_lock(user_data_dir, timeout=USER_DATA_LOCK_TIMEOUT_SEC):
            with sync_playwright() as p:
                context = None
                try:
                    context = _launch_persistent(
                        p,
                        user_data_dir,
                        args=debug_args if debug else offscreen_args,
                        viewport=None if debug else {"width": 1920, "height": 1080},
                        debug=debug,
                        proxy=proxy_server,
                    )
                    context.set_default_timeout(60000)
                    context.set_default_navigation_timeout(60000)

                    page = context.new_page()
                    for old_page in context.pages:
                        if old_page != page:
                            try:
                                old_page.close()
                            except Exception:
                                pass
                    page.bring_to_front()

                    install_overlay_guard(page)

                    response = page.goto(post_url, timeout=60000, wait_until="domcontentloaded")
                    _wait_x_post_detail_ready(page)
                    _dismiss_overlays(page, aggressive=False, desc="x-comment-nav")
                    report_guard_hits(page, "x-comment-nav")

                    if response and response.status >= 400:
                        return f"X 帖子详情页加载异常 (HTTP {response.status})", False, None

                    login_err = _check_x_login_state(page)
                    if login_err:
                        return login_err, False, None

                    check_for_crash(page)

                    # 检查目标帖子是否已被删除或不存在
                    for err_sel in ('div[data-testid="error-detail"]', 'div[data-testid="empty_state_header_text"]'):
                        try:
                            err_loc = page.locator(err_sel).first
                            if err_loc.count() > 0 and err_loc.is_visible():
                                err_txt = (err_loc.inner_text() or "").strip()[:120]
                                return f"目标 X 帖子不可用或已删除: {err_txt}", False, None
                        except Exception:
                            pass

                    if like:
                        try:
                            focal_tweet = _locate_focal_tweet(page, post_id=target_post_id)
                            already_liked = focal_tweet.locator('button[data-testid="unlike"]').first
                            if already_liked.count() > 0 and already_liked.is_visible():
                                logger.info("[点赞/Like] 目标 X 帖子已处于点赞状态(检测到 unlike 按钮)，无需重复点击")
                            else:
                                like_btn = focal_tweet.locator('button[data-testid="like"]').first
                                if like_btn.count() == 0:
                                    like_btn = page.locator('button[data-testid="like"]').first
                                if like_btn.count() > 0 and like_btn.is_visible():
                                    _robust_click(like_btn)
                                    page.wait_for_timeout(500)
                                    logger.info("[点赞/Like] X 帖子点赞操作执行完成 | 结果: [Success]")
                                else:
                                    logger.warning("[点赞/Like] 未找到 X 帖子点赞按钮，跳过点赞")
                        except Exception as like_e:
                            logger.warning(f"[点赞/Like] 点赞操作异常（不阻断评论主流程）: {str(like_e)[:120]}")

                    if not effective_content and not valid_text_list and not valid_images:
                        logger.info("[任务/Comment] 仅点赞模式执行完毕 | 结果: [Success]")
                        return None, True, "LIKE_ONLY"

                    try:
                        reply_container = _locate_x_reply_box(page, post_id=target_post_id)
                    except Exception as e:
                        return f"无法定位 X 回复输入区: {e}", False, None

                    comment_id = _submit_x_post(
                        page,
                        reply_container,
                        content=effective_content,
                        image_path_list=image_path_list,
                        text_list=text_list,
                        prepared_images=valid_images,
                    )

                    comment_url = f"https://x.com/i/status/{comment_id}" if comment_id else "未知"
                    logger.info(
                        f"[任务/Comment] X 评论回复发布成功 | 原帖ID: 【{target_post_id}】 "
                        f"| 评论ID(rest_id): 【{comment_id}】 | 链接: <{comment_url}> | 结果: [Success]"
                    )
                    return None, True, comment_id

                except BusinessErrorException as biz_e:
                    error_info = f"[业务拦截] {biz_e}"
                    logger.error(f"[任务/Comment] X 评论被服务端业务规则阻断 | 原因: 【{error_info}】")
                    return error_info, False, None

                except Exception as e:
                    error_info = f"[{type(e).__name__}] {e}\n[Traceback]:\n{traceback.format_exc()}"
                    if context and context.pages:
                        try:
                            _forensics(
                                context.pages[0],
                                f"x_comment_error_{target_post_id}",
                                {"url": post_url, "error": error_info[:2000]},
                            )
                        except Exception:
                            pass
                    _alert_failure_scene(error_info)
                    return error_info, False, None

                finally:
                    if context:
                        try:
                            context.close()
                        except Exception:
                            pass

    except UserDataDirLockTimeoutError as lock_e:
        error_info = f"[UserDataDirLockTimeout] {lock_e}"
        logger.error(f"[任务/Comment] {error_info}")
        return error_info, False, None

    except Exception as core_e:
        error_info = f"[CoreEngineCrash] Playwright底层崩溃:\n{core_e}"
        logger.error(error_info)
        return error_info, False, None


def follow_x_user(
    target_screen_name,
    user_data_dir=USER_DATA_DIR,
    debug=True,
    use_proxy=True,
):
    """
    主控入口 2.5：打开指定 X 用户个人主页（`https://x.com/{screen_name}`）并执行安全幂等关注操作。
    - 严格限定在 `[data-testid="primaryColumn"]` 主资料卡区域内定位按钮，杜绝误点右侧推荐栏；
    - 若检测到已处于关注状态（`data-testid$="-unfollow"`），直接幂等返回 `("ALREADY_FOLLOWING")`，绝不误触发取关。

    返回:
        Tuple(错误信息(str|None), 是否成功(bool), 状态标识(str|None: "FOLLOWED" | "ALREADY_FOLLOWING"))
    """
    clean_handle = str(target_screen_name or "").strip().lstrip("@").split("/")[0].split("?")[0].strip()
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
        f"| 代理: 【{proxy_server or '直连'}】 | 模式: 【{'debug可见' if debug else '离屏后台'}】\n{'=' * 70}"
    )

    anti_freeze_args = ["--disable-restore-session-state", "--no-default-browser-check"]
    offscreen_args = [
        "--disable-blink-features=AutomationControlled",
        "--disable-gpu",
        "--window-position=-10000,-10000",
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-renderer-backgrounding",
        "--disable-background-timer-throttling",
        "--disable-backgrounding-occluded-windows",
        "--disable-features=CalculateNativeWinOcclusion",
        "--disable-breakpad",
        "--force-device-scale-factor=1",
        "--hide-scrollbars",
    ] + anti_freeze_args

    debug_args = [
        "--disable-blink-features=AutomationControlled",
        "--start-maximized",
        "--window-position=0,0",
    ] + anti_freeze_args

    try:
        with _user_data_dir_lock(user_data_dir, timeout=USER_DATA_LOCK_TIMEOUT_SEC):
            with sync_playwright() as p:
                context = None
                try:
                    context = _launch_persistent(
                        p,
                        user_data_dir,
                        args=debug_args if debug else offscreen_args,
                        viewport=None if debug else {"width": 1920, "height": 1080},
                        debug=debug,
                        proxy=proxy_server,
                    )
                    context.set_default_timeout(60000)
                    context.set_default_navigation_timeout(60000)

                    page = context.new_page()
                    for old_page in context.pages:
                        if old_page != page:
                            try:
                                old_page.close()
                            except Exception:
                                pass
                    page.bring_to_front()
                    install_overlay_guard(page)

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
                    report_guard_hits(page, "x-follow-nav")

                    if response and response.status >= 400:
                        return f"X 用户主页加载异常 (HTTP {response.status})", False, None

                    login_err = _check_x_login_state(page)
                    if login_err:
                        return login_err, False, None

                    check_for_crash(page)

                    empty_hdr = page.locator('div[data-testid="empty_state_header_text"]').first
                    if empty_hdr.count() > 0 and empty_hdr.is_visible():
                        err_txt = (empty_hdr.inner_text() or "").strip()[:120]
                        return f"目标 X 账号不可用或不存在 (@{clean_handle}): {err_txt}", False, None

                    primary_col = page.locator('[data-testid="primaryColumn"]').first
                    header_Placement = primary_col.locator('div[data-testid="placementTracking"]')

                    # 幂等保护：若已处于关注或待批准状态，直接返回成功，绝不点击取关按钮
                    already_btn = header_Placement.locator(
                        'button[data-testid$="-unfollow"], button[data-testid$="-cancel"]'
                    ).first
                    if already_btn.count() > 0 and already_btn.is_visible():
                        logger.info(f"[关注/Follow] 账号 @{clean_handle} 已处于关注状态，幂等跳过点击")
                        return None, True, "ALREADY_FOLLOWING"

                    follow_btn = header_Placement.locator('button[data-testid$="-follow"]').first
                    if follow_btn.count() == 0:
                        follow_btn = primary_col.locator('button[data-testid$="-follow"]').first
                    if follow_btn.count() == 0 or not follow_btn.is_visible():
                        return f"未能在 @{clean_handle} 主页定位到可点击的关注按钮", False, None

                    testid_val = follow_btn.get_attribute("data-testid") or ""
                    if testid_val.endswith("-unfollow"):
                        return None, True, "ALREADY_FOLLOWING"

                    friendship_ok = {"hit": False}

                    def _on_follow_resp(resp):
                        u = (resp.url or "").lower()
                        if ("friendships/create" in u or "createfriendship" in u) and resp.status == 200:
                            friendship_ok["hit"] = True

                    page.on("response", _on_follow_resp)
                    try:
                        _robust_click(follow_btn)
                        deadline = time.time() + 10.0
                        while time.time() < deadline:
                            if friendship_ok["hit"]:
                                break
                            unfollow_now = header_Placement.locator(
                                'button[data-testid$="-unfollow"], button[data-testid$="-cancel"]'
                            ).first
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
                        page.wait_for_timeout(1200)
                        logger.info(f"[任务/Follow] X 关注成功 | 账号: 【@{clean_handle}】 | 结果: [Success]")
                        return None, True, "FOLLOWED"

                    _forensics(page, f"x_follow_unconfirmed_{clean_handle}", {"url": profile_url})
                    return f"点击关注按钮后未检测到关注状态切换 (@{clean_handle})", False, None

                except BusinessErrorException as biz_e:
                    error_info = f"[业务拦截] {biz_e}"
                    logger.error(f"[任务/Follow] X 关注被阻断 | 原因: 【{error_info}】")
                    return error_info, False, None

                except Exception as e:
                    error_info = f"[{type(e).__name__}] {e}\n[Traceback]:\n{traceback.format_exc()}"
                    if context and context.pages:
                        try:
                            _forensics(
                                context.pages[0],
                                f"x_follow_error_{clean_handle}",
                                {"url": profile_url, "error": error_info[:2000]},
                            )
                        except Exception:
                            pass
                    _alert_failure_scene(error_info)
                    return error_info, False, None

                finally:
                    if context:
                        try:
                            context.close()
                        except Exception:
                            pass

    except UserDataDirLockTimeoutError as lock_e:
        error_info = f"[UserDataDirLockTimeout] {lock_e}"
        logger.error(f"[任务/Follow] {error_info}")
        return error_info, False, None

    except Exception as core_e:
        error_info = f"[CoreEngineCrash] Playwright底层崩溃:\n{core_e}"
        logger.error(error_info)
        return error_info, False, None


def login_and_save_session(user_data_dir=USER_DATA_DIR, use_proxy=True):
    """打开可见浏览器供人工登录 X 账号，按 Enter 后关闭并固化会话至 user_data_dir。"""
    proxy_server = get_proxy_url() if use_proxy else None
    logger.info(f"[登录/Auth] 准备手动登录 X 账号 | 存储路径: <{user_data_dir}> | 代理: <{proxy_server or '直连'}>")
    with _user_data_dir_lock(user_data_dir, timeout=USER_DATA_LOCK_TIMEOUT_SEC):
        clean_browser_cache(user_data_dir)
        with sync_playwright() as p:
            context = None
            try:
                context = _launch_persistent(
                    p,
                    user_data_dir,
                    args=["--disable-blink-features=AutomationControlled", "--start-maximized"],
                    hide_automation=False,
                    debug=True,
                    proxy=proxy_server,
                )
                page = context.pages[0] if context.pages else context.new_page()
                page.bring_to_front()
                page.goto(X_HOME_URL)
                input("\n[登录/Auth] 等待操作 | 动作: 【在浏览器中完成 X 登录并进入主页后，请按 Enter 键保存会话并退出】")
                logger.info("[登录/Auth] X 登录会话已固化到本地 | 结果: [Success]")
            finally:
                if context:
                    try:
                        context.close()
                    except Exception:
                        pass


def open_browser_for_manual_use(user_data_dir=USER_DATA_DIR, home_url=X_HOME_URL, use_proxy=True):
    """主控入口 3：启动可见浏览器交由人工自由操作（用于首次登录 X 账号或日常手动浏览，直接关闭窗口即自动保存退出）。"""
    proxy_server = get_proxy_url() if use_proxy else None
    return _base_open_browser_for_manual_use(user_data_dir=user_data_dir, home_url=home_url, proxy=proxy_server)


# ==============================================================================
#                                   启动入口与使用示例
# ==============================================================================
if __name__ == "__main__":
    # ==========================================================================
    # 示例 1：首次使用 —— 手动登录 X 账号并持久化保存浏览器 Session
    # （登录成功后直接关闭弹出的浏览器窗口，登录态会自动保存在 USER_DATA_DIR 中）
    # ==========================================================================
    open_browser_for_manual_use(USER_DATA_DIR)

    # ==========================================================================
    # 示例 2：自动发布 X 帖子（传入文字 + 图片地址列表）
    # image_path_list 支持：
    #   1) 本地图片绝对路径，如 r"W:\project\python_project\ai_content_pipeline\media_downloads\xxx.jpg"
    #   2) media_downloads 目录下的文件名，如 "xxx.jpg"
    #   3) 网络图片 URL，如 "https://pbs.twimg.com/media/xxx.jpg"（会自动下载到本地后上传）
    #   4) 传 None 或 [] 则发送纯文字推文（最多支持 4 张图）
    # ==========================================================================
    post_text = (
        "$BTC 链上筹码结构正在发生微妙变化。\n\n"
        "短期投机资金在震荡中不断洗出，而长线地址的持仓水位依然稳如泰山。"
        "真正的行情往往在大多数人失去耐心时悄然启动。"
    )
    post_images = [
        # r"W:\project\python_project\ai_content_pipeline\media_downloads\example.jpg",
        # "https://pbs.twimg.com/media/example.jpg",
    ]

    err, success, new_tweet_id = create_x_post(
        content=post_text,
        image_path_list=post_images,
        user_data_dir=USER_DATA_DIR,
        debug=True,
    )

    if success:
        logger.info(f"\n[结果/Final] 🎉 ======== X 自动发帖成功 ======== | 帖子ID: 【{new_tweet_id}】")
    else:
        logger.error(f"\n[结果/Final] ❌ ======== X 自动发帖失败 ======== | 错误原因:\n{err}")

    # ==========================================================================
    # 示例 3：对指定的 X 帖子进行自动评论/回复（可选同时点赞、可选带图评论）
    # target_post_url 支持传完整推文链接（https://x.com/xxx/status/123456）或纯数字推文 ID
    # ==========================================================================
    if success and new_tweet_id:
        reply_text = "补充一点：目前永续合约资金费率已回到中性区间，杠杆泡沫清理得相当干净。"
        reply_images = [
            # r"W:\project\python_project\ai_content_pipeline\media_downloads\chart.png",
        ]

        c_err, c_success, comment_id = comment_on_x_post(
            target_post_url=f"https://x.com/i/status/{new_tweet_id}",
            content=reply_text,
            image_path_list=reply_images,
            like=True,  # True 表示评论前顺带给原帖点赞
            user_data_dir=USER_DATA_DIR,
            debug=True,
        )

        if c_success:
            logger.info(f"\n[结果/Final] 🎉 ======== X 自动评论成功 ======== | 评论ID: 【{comment_id}】")
        else:
            logger.error(f"\n[结果/Final] ❌ ======== X 自动评论失败 ======== | 错误原因:\n{c_err}")
