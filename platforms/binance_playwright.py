# -*- coding: utf-8 -*-
"""
币安广场 (Binance Square) Playwright 浏览器自动化模块：
- 自动发帖 (create_binance_post) 与自动评论 (comment_on_binance_post)
- 支持多图上传、超链接注入、K线图表卡片挂载、看多/看空情绪标签、分段键入 (text_list)
- 登录态凭证提取 (get_auth_tokens_robust)、手动登录固化 (login_and_save_session)、人工接管 (open_browser_for_manual_use)
- 底层锁、缓存清理、浮层免疫、光标控制与故障取证统一复用 platforms.playwright_base
"""
import os
import re
import sys
import time
import traceback
from pathlib import Path
from urllib.parse import unquote, urlparse

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import expect, sync_playwright

from common.common_utils import resolve_session_dir, setup_logger
from platforms.playwright_base import (
    API_WAIT_TIMEOUT_MS,
    RE_ADD_LINK,
    RE_CONFIRM,
    RE_MORE,
    RE_SEND,
    RE_SEND_EXACT,
    TYPE_CHUNK_SIZE,
    USER_DATA_LOCK_TIMEOUT_SEC,
    BusinessErrorException,
    PageCrashedException,
    UserDataDirLockTimeoutError,
    _alert_failure_scene,
    _BaseApiWatcher,
    _dismiss_overlays,
    _fmt_hit,
    _focus_editor_end,
    _force_caret_to_head,
    _force_caret_to_tail,
    _forensics,
    _hit_test,
    _interact_fallback_locators,
    _launch_persistent,
    _normalize_text_list,
    _parse_api_json,
    _pick_trusted_send_button,
    _robust_click,
    _snapshot_editor,
    _type_body,
    _type_text_list,
    _user_data_dir_lock,
    check_for_crash,
    clean_browser_cache,
    install_overlay_guard,
    open_browser_for_manual_use as _base_open_browser_for_manual_use,
    report_guard_hits,
)

logger = setup_logger(app_name="binance_playwright")

# ==============================================================================
#                                   运行配置
# ==============================================================================
USER_DATA_DIR = resolve_session_dir("ruru_browser_session_dir", "biance_ruru")
LOGIN_URL = "https://www.binance.com/zh-CN/login"

RE_SUBMIT_API_PRIMARY = re.compile(r"pgc/content/add|pgc/comment/add", re.IGNORECASE)
RE_SUBMIT_API_SECONDARY = re.compile(
    r"pgc/.*/(add|create|reply|comment|publish)|comment/.*/(add|create)",
    re.IGNORECASE,
)


# ==============================================================================
#                            编辑器：定位 / 唤醒 / 注入
# ==============================================================================
def _smart_scroll_to_editor(page, max_scrolls=20):
    """步进式 PageDown 探底，锁定评论区富文本容器并滚入可视范围。"""
    editor_container = page.locator("div.feed-post-editor").first
    for i in range(max_scrolls):
        if editor_container.is_visible():
            editor_container.scroll_into_view_if_needed()
            logger.info(f"[定位/DOM] 已锁定评论区局部作用域 | 滚动次数: 【{i}】 | 选择器: <div.feed-post-editor>")
            return editor_container
        page.keyboard.press("PageDown")
        time.sleep(0.5)
    raise Exception(f"向下滚动 {max_scrolls} 次仍未找到评论输入区，疑似死链、风控滑块拦截或 body 被 scroll-lock 锁死。")


def _resolve_wake_target(editor_container):
    """解析点击唤醒编辑器的候选元素（兼容评论区和广场主页发帖区）。"""
    cands = (
        editor_container.locator(
            'input[type="text"]:not([type="search"]):not([placeholder*="搜索"])'
            ':not([placeholder*="Search"]):not([aria-label*="搜索"]):not([aria-label*="Search"]),'
            'input[placeholder]:not([type="search"]):not([placeholder*="搜索"]):not([placeholder*="Search"])'
        ).first,
        editor_container.get_by_placeholder(
            re.compile(
                r"(分享您的洞见|Share your insights|分享你的|发布您的回复|发布你的回复|写下你的|说点什么|发表评论|回复|评论|Reply|Comment|Write|Post)",
                re.IGNORECASE,
            )
        ).first,
        editor_container.locator('div[contenteditable="true"].ProseMirror').first,
        editor_container.locator('div[contenteditable="true"]').first,
        editor_container.locator('[class*="placeholder"]').first,
    )
    for c in cands:
        try:
            if c.count() > 0:
                return c
        except Exception:
            continue
    return editor_container.locator('input, div[contenteditable="true"]').first


def _wake_editor(page, editor_container, max_round=4):
    """唤醒富文本编辑器（每轮先清障，点击方式逐级升级）。返回 real_editor(Locator)。"""
    target = _resolve_wake_target(editor_container)
    real_editor = editor_container.locator('div[contenteditable="true"].ProseMirror').first
    last_hit = None

    for rnd in range(1, max_round + 1):
        _dismiss_overlays(page, aggressive=(rnd >= 2), desc=f"wake-r{rnd}")
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
                target.click(timeout=8000)
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
                page.keyboard.press("Tab")
        except Exception as e:
            logger.warning(f"[编辑器/唤醒] 第{rnd}轮点击动作抛错，升级策略重试 | 详情: 【{str(e)[:150]}】")

        try:
            expect(real_editor).to_be_editable(timeout=8000 if rnd == 1 else 4000)
            focused = page.evaluate(
                """() => {
                    const a = document.activeElement;
                    return !!(a && (a.isContentEditable ||
                                    (a.classList && a.classList.contains('ProseMirror')) ||
                                    (a.closest && a.closest('.ProseMirror'))));
                }"""
            )
            if not focused:
                try:
                    real_editor.evaluate("el => el.focus()")
                except Exception:
                    pass
            logger.info(f"[编辑器/唤醒] 唤醒成功 | 轮次: 【{rnd}】 | 状态: [可编辑] | 焦点在编辑器: 【{focused}】")
            return real_editor
        except Exception:
            logger.warning(f"[编辑器/唤醒] 第{rnd}轮唤醒未生效（ProseMirror 仍不可编辑），升级策略重试")

    _forensics(page, "wake_fail", {"last_hit_test": last_hit})
    raise Exception(f"编辑器唤醒失败（{max_round}级降级全部失效），疑似浮层持续拦截或前端结构变更。最后命中测试: {last_hit}")


def _inject_single_link(page, editor_container, real_editor, link_text, link_url, idx):
    """在富文本末尾唤起「更多 → 添加链接」弹窗，注入单条超链接并校验上屏。"""
    logger.info(f"[编辑器/链接] 开始注入第 【{idx + 1}】 条 | 锚文本: 【{link_text}】 | URL: <{link_url}>")
    try:
        _focus_editor_end(page, real_editor)
        page.keyboard.press("Space")
        page.wait_for_timeout(150)

        _interact_fallback_locators(
            [
                editor_container.locator("#post-editor-more-icon").first,
                editor_container.locator("svg").filter(has=page.locator('path[d^="M12 16.5"]')).first,
                editor_container.locator("div.icon-box").filter(has=page.locator("svg")).last,
                editor_container.get_by_role("button", name=RE_MORE).first,
                editor_container.locator('button[aria-label*="更多"], button[aria-label*="More" i]').first,
            ],
            action="click",
            timeout=4000,
            desc="更多按钮",
        )
        page.wait_for_timeout(350)

        _interact_fallback_locators(
            [
                page.locator(".menu-item").filter(has_text=RE_ADD_LINK).first,
                page.get_by_role("menuitem", name=RE_ADD_LINK).first,
                page.locator('[role="menuitem"], [class*="menu-item"]').filter(has_text=RE_ADD_LINK).first,
            ],
            action="click",
            timeout=4000,
            desc="添加链接选项",
        )

        dialog = page
        try:
            dlg = page.get_by_role("dialog").last
            dlg.wait_for(state="visible", timeout=2000)
            dialog = dlg
        except Exception:
            pass

        name_input = _interact_fallback_locators(
            [
                dialog.locator('input[name="name"][data-bn-type="input"]').first,
                dialog.locator('input[name="name"]').first,
                dialog.get_by_placeholder(re.compile(r"正文|名称|标题|text|name|title", re.IGNORECASE)).first,
            ],
            action="wait",
            timeout=6000,
            desc="链接正文输入框",
        )

        link_input = _interact_fallback_locators(
            [
                dialog.locator('input[name="link"][data-bn-type="input"]').first,
                dialog.locator('input[name="link"]').first,
                dialog.get_by_placeholder(re.compile(r"链接|地址|link|url|address", re.IGNORECASE)).first,
            ],
            action="wait",
            timeout=6000,
            desc="链接地址输入框",
        )

        confirm_btn = _interact_fallback_locators(
            [
                dialog.locator('button[type="submit"][data-bn-type="button"]').filter(has_text=RE_CONFIRM).first,
                dialog.locator('button[type="submit"]').filter(has_text=RE_CONFIRM).first,
                dialog.get_by_role("button", name=RE_CONFIRM).first,
            ],
            action="wait",
            timeout=6000,
            desc="链接确认按钮",
        )

        name_input.fill(link_text)
        page.wait_for_timeout(200)
        link_input.fill(link_url)
        page.wait_for_timeout(200)

        expect(confirm_btn).to_be_enabled(timeout=6000)
        confirm_btn.click(timeout=6000)
        expect(name_input).to_be_hidden(timeout=6000)

        expect(
            real_editor.locator("a").filter(has_text=re.compile(re.escape(link_text), re.IGNORECASE)).first
        ).to_be_visible(timeout=5000)

        _focus_editor_end(page, real_editor)
        page.keyboard.press("Space")
        page.wait_for_timeout(200)
        logger.info(f"[编辑器/链接] 第 【{idx + 1}】 条注入成功并已上屏 | 结果: [Success]")
        return True

    except Exception as e:
        logger.warning(
            f"[编辑器/链接] 第 【{idx + 1}】 条注入失败，按设计跳过继续下一条 | 锚文本: 【{link_text}】 "
            f"| 可能原因: 【{str(e)[:150]}】 | 结果: [Skipped]"
        )
        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(250)
        except Exception:
            pass
        return False


def _inject_chart(page, editor_container, chart_info):
    """注入 K 线图表卡片逻辑。"""
    if not chart_info or not isinstance(chart_info, dict):
        return False

    coin = str(chart_info.get("coin", "")).strip()
    bridge = str(chart_info.get("bridge", "")).strip()
    c_type = str(chart_info.get("type", "")).strip()

    if not coin:
        logger.warning("[编辑器/图表] 未提供有效的 coin，跳过图表注入")
        return False

    logger.info(f"[编辑器/图表] 开始注入图表 | 目标: {coin}-{bridge} ({c_type})")

    try:
        _interact_fallback_locators(
            [
                editor_container.locator('svg path[d^="M17.123 1.803"]').locator(".."),
                editor_container.locator('.trade-widget-icon, [class*="widget-icon"]').first,
            ],
            action="click",
            timeout=5000,
            desc="添加图表图标",
        )
        page.wait_for_timeout(500)

        search_input = _interact_fallback_locators(
            [
                page.locator('input[aria-label*="搜索币种"], input[placeholder*="搜索币种"]').first,
                page.locator(".bn-textField-input").first,
            ],
            action="wait",
            timeout=5000,
            desc="图表搜索框",
        )

        def is_target_request(response):
            if "pgc/trade/widget/list" in response.url and response.request.method == "POST":
                try:
                    post_data = response.request.post_data_json
                    if post_data and post_data.get("keyword", "").upper() == coin.upper():
                        return True
                except Exception:
                    pass
            return False

        with page.expect_response(is_target_request, timeout=10000) as response_info:
            search_input.fill(coin)
            page.wait_for_timeout(800)

        resp = response_info.value
        body = resp.json()
        items = body.get("data", [])

        if not items:
            raise Exception(f"接口未返回任何关于 {coin} 的数据")

        target_index = -1
        for i, item in enumerate(items):
            if (
                str(item.get("coin", "")).upper() == coin.upper()
                and str(item.get("bridge", "")).upper() == bridge.upper()
                and str(item.get("type", "")).lower() == c_type.lower()
            ):
                target_index = i
                break

        if target_index == -1:
            raise Exception(f"接口返回的数据中未找到匹配 {coin}-{bridge}-{c_type} 的项")

        logger.info(f"[编辑器/图表] 匹配成功 | 目标在列表中的索引为: 【{target_index}】")

        list_items = page.locator("div.overflow-y-auto > div.cursor-pointer")
        list_items.nth(target_index).wait_for(state="visible", timeout=5000)
        list_items.nth(target_index).click(timeout=3000)
        page.wait_for_timeout(800)

        logger.info("[编辑器/图表] 图表注入成功 | 结果: [Success]")
        return True

    except Exception as e:
        logger.warning(f"[编辑器/图表] 图表注入失败，已跳过 | 原因: 【{str(e)[:150]}】 | 结果: [Skipped]")
        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(250)
        except Exception:
            pass
        return False


def _inject_sentiment(page, editor_container, sentiment):
    """注入情绪（看多/看空）图标逻辑。"""
    if not sentiment or sentiment not in ["看多", "看空"]:
        return

    logger.info(f"[编辑器/情绪] 准备点击情绪标签 | 目标: 【{sentiment}】")
    try:
        svg_locators = editor_container.locator("svg").filter(has=page.locator('path[d^="M20.91 6.861"]'))
        count = svg_locators.count()
        if count == 0:
            logger.warning(f"[编辑器/情绪] 未找到对应的情绪图标，已跳过 | 目标: 【{sentiment}】")
            return

        target_svg = None
        for i in range(count):
            svg_node = svg_locators.nth(i)
            style_str = svg_node.get_attribute("style") or ""
            is_bear = "rotateX(180deg)" in style_str

            if sentiment == "看空" and is_bear:
                target_svg = svg_node
                break
            elif sentiment == "看多" and not is_bear:
                target_svg = svg_node
                break

        if target_svg:
            target_svg.locator("..").click(timeout=3000)
            page.wait_for_timeout(500)
            logger.info("[编辑器/情绪] 情绪标签点击成功 | 结果: [Success]")
        else:
            logger.warning(f"[编辑器/情绪] 页面存在情绪图标，但未找到匹配 【{sentiment}】 状态的图标元素 | 结果: [Skipped]")

    except Exception as e:
        logger.warning(f"[编辑器/情绪] 情绪标签点击异常，已跳过 | 原因: 【{str(e)[:150]}】 | 结果: [Skipped]")


# ==============================================================================
#                        发送：接口监听 / 结果判定 / 提交主干
# ==============================================================================
class _ApiWatcher(_BaseApiWatcher):
    """币安广场发帖/评论提交接口监听器。"""

    def __init__(self, page):
        super().__init__(page, RE_SUBMIT_API_PRIMARY, RE_SUBMIT_API_SECONDARY)


def _read_api_verdict(page, watcher):
    """解读发送结果。主判据：POST pgc/content/add 且 code=000000 或 success=True。"""
    resp = watcher.wait_primary(API_WAIT_TIMEOUT_MS)
    if resp is not None:
        body = _parse_api_json(resp)
        if str(body.get("code", "")) == "000000" or body.get("success") is True:
            data = body.get("data")
            cid = data.get("id") if isinstance(data, dict) else None
            logger.info(f"[发送/校验] 主接口确认发送成功 | 响应码: 【000000】 | 评论ID: 【{cid}】 | 结果: [Success]")
            return True, cid
        if body:
            err = body.get("message", "未知业务拦截")
            logger.error(
                f"[发送/校验] 服务端业务规则拒收本条评论 | HTTP: 【{resp.status}】 | 原因: 【{err}】 "
                f"| 排查方向: 【内容违规 / 发送频率限制 / 该帖已关闭评论 / 账号权限不足】"
            )
            _forensics(page, "biz_reject", {"url": resp.url, "status": resp.status, "body": body})
            raise BusinessErrorException(f"业务发送被服务器拦截，原因: {err}")
        logger.warning(f"[发送/校验] 主接口响应非 JSON(HTTP 【{resp.status}】)，转入 DOM 兜底校验")
        return False, None

    sec = watcher.latest_secondary()
    if sec is None:
        logger.warning(f"[发送/校验] 【{API_WAIT_TIMEOUT_MS}ms】 内未捕获到提交接口，转入 DOM 兜底校验")
        return False, None

    body = _parse_api_json(sec)
    logger.info(
        f"[发送/校验] 观测到疑似评论接口（非主判据） | URL: <{sec.url}> | HTTP: 【{sec.status}】 | body: 【{str(body)[:200]}】"
    )
    if str(body.get("code", "")) == "000000" or body.get("success") is True:
        data = body.get("data")
        cid = (data.get("id") or data.get("commentId") or data.get("contentId")) if isinstance(data, dict) else None
        logger.info(f"[发送/校验] 辅助接口判定发送成功 | 评论ID: 【{cid}】 | 结果: [Success]")
        return True, cid
    return False, None


def _submit_comment(
    page,
    editor_container,
    comment,
    image_path_list=None,
    url_info_list=None,
    chart_info=None,
    sentiment=None,
    text_list=None,
):
    """在隔离作用域内跑完发帖/评论全链路。"""
    comment = str(comment) if comment else ""
    valid_text_list = _normalize_text_list(text_list)

    _dismiss_overlays(page, aggressive=False, desc="pre-submit")
    report_guard_hits(page, "pre-submit")

    real_editor = _wake_editor(page, editor_container)

    valid_images = []
    if image_path_list:
        if isinstance(image_path_list, str):
            image_path_list = [image_path_list]
        for p in image_path_list:
            if os.path.exists(p):
                valid_images.append(p)
            else:
                logger.warning(f"[编辑器/图片] 图片路径不存在，已跳过: <{p}>")

    if valid_images:
        file_input = editor_container.locator('input[type="file"]').first
        try:
            file_input.set_input_files(valid_images, timeout=15000)
            mounted = False
            try:
                expect(
                    editor_container.locator(
                        "img[src^='blob'],img[src^='http'],img[src^='data:'],[class*='thumb'],[class*='preview'] img"
                    ).nth(len(valid_images) - 1)
                ).to_be_visible(timeout=15000)
                mounted = True
            except Exception:
                page.wait_for_timeout(3500)
            logger.info(
                f"[编辑器/图片] 图片批量挂载完毕 | 数量: 【{len(valid_images)}】张 | 缩略图全可见: 【{mounted}】 | 结果: [Success]"
            )
        except Exception as e:
            logger.warning(
                f"[编辑器/图片] 批量注入失败，可能前端缺失 multiple 属性。进入逐张降级保序上传模式 | 错误: 【{str(e)[:150]}】"
            )
            try:
                for img_path in valid_images:
                    file_input.set_input_files(img_path, timeout=5000)
                    page.wait_for_timeout(1000)
                logger.info(f"[编辑器/图片] 逐张降级上传完毕 | 数量: 【{len(valid_images)}】张 | 结果: [Success]")
            except Exception as sub_e:
                logger.warning(f"[编辑器/图片] 逐张降级上传依然失败，本条自动降级为纯文本 | 致命错误: 【{str(sub_e)[:150]}】")

    links = [u for u in (url_info_list or []) if isinstance(u, dict)] if isinstance(url_info_list, list) else []
    if links:
        logger.info(f"[编辑器/链接] 检测到超链接任务 | 数量: 【{len(links)}】 | 结果: [启动注入流]")
    for idx, url_info in enumerate(links):
        link_text = str(url_info.get("text", "")).strip()
        link_url = str(url_info.get("url", "")).strip()
        if not link_text or not link_url:
            continue
        if not re.match(r"^https?://", link_url, re.IGNORECASE):
            link_url = "https://" + link_url
        _dismiss_overlays(page, aggressive=False, desc=f"pre-link-{idx}")
        _inject_single_link(page, editor_container, real_editor, link_text, link_url, idx)

    _force_caret_to_head(page, real_editor, tag="before-body")

    if valid_text_list or comment.strip():
        if valid_text_list:
            total_chars = sum(len(x.get("text", "")) for x in valid_text_list)
            logger.info(
                f"[编辑器/正文] 优先使用 text_list 键入正文 | 列表项数: 【{len(valid_text_list)}】 "
                f"| 总字符数: 【{total_chars}】 | 分块: 【{TYPE_CHUNK_SIZE}】"
            )
            page.wait_for_timeout(800)
            _type_text_list(page, real_editor, valid_text_list)
            page.wait_for_timeout(500)
        else:
            logger.info(f"[编辑器/正文] 开始键入正文 | 字符数: 【{len(comment)}】 | 分块: 【{TYPE_CHUNK_SIZE}】")
            page.wait_for_timeout(800)
            _type_body(page, real_editor, comment)
            page.wait_for_timeout(500)

        try:
            current_text = (real_editor.inner_text() or "").strip()
        except Exception:
            current_text = ""

        expect_non_empty = (
            any(x.get("text", "").strip() for x in valid_text_list) if valid_text_list else bool(comment.strip())
        )
        if not current_text and expect_non_empty:
            logger.warning("[编辑器/正文] 正文被静默清空，触发一次全量补录")
            _dismiss_overlays(page, aggressive=True, desc="text-retry")
            try:
                expect(real_editor).to_be_editable(timeout=4000)
            except Exception:
                real_editor = _wake_editor(page, editor_container, max_round=2)
            _force_caret_to_head(page, real_editor, tag="text-retry")
            if valid_text_list:
                _type_text_list(page, real_editor, valid_text_list)
            else:
                _type_body(page, real_editor, comment)
            page.wait_for_timeout(500)

        logger.info("[编辑器/同步] 执行强制状态唤醒 (State Resync)...")
        try:
            _force_caret_to_tail(page, real_editor, tag="resync")
            real_editor.press("Space")
            page.wait_for_timeout(100)
            real_editor.press("Backspace")
            page.wait_for_timeout(300)
        except Exception as sync_e:
            logger.warning(f"[编辑器/同步] 状态唤醒动作异常，但继续主流程: {str(sync_e)[:100]}")

        logger.info("[编辑器/正文] 正文输入与状态同步完成 | 结果: [Success]")

    if chart_info:
        logger.info("[编辑器/图表] 准备在正文末尾注入图表...")
        _dismiss_overlays(page, aggressive=False, desc="pre-chart")
        _force_caret_to_tail(page, real_editor, tag="before-chart")

        try:
            real_editor.press("Shift+Enter")
            page.wait_for_timeout(200)
        except Exception:
            page.keyboard.press("Shift+Enter")
            page.wait_for_timeout(200)

        _inject_chart(page, editor_container, chart_info)

    if sentiment:
        _inject_sentiment(page, editor_container, sentiment)

    _dismiss_overlays(page, aggressive=False, desc="pre-send")
    send_btn_cands = [
        editor_container.locator("button").filter(has_text=RE_SEND_EXACT).first,
        editor_container.get_by_role("button", name=RE_SEND).first,
    ]

    send_button = None
    try:
        send_button = _pick_trusted_send_button(send_btn_cands) or _interact_fallback_locators(
            send_btn_cands, action="wait", timeout=5000, desc="发送按钮"
        )
        expect(send_button).to_be_enabled(timeout=8000)
    except PlaywrightTimeoutError:
        _forensics(
            page,
            "btn_disabled_fatal",
            {
                "btn_html": send_button.evaluate("el => el.outerHTML") if send_button else "none",
                "editor_text": real_editor.inner_text() if real_editor else "none",
            },
        )
        raise Exception("发送按钮被找到，但持续处于禁用(Disabled)状态。前端组件未能识别输入内容。已阻断强制点击，防止死等接口。")
    except Exception as e:
        logger.warning(f"[发送/按钮] 发生意外的定位错误: {str(e)[:150]}")
        send_button = send_btn_cands[0]

    text_before, media_before = _snapshot_editor(real_editor)
    api_success, comment_id = False, None

    logger.info(
        f"[发送/提交] 触发发送并挂载接口监听 | 主判据: <pgc/content/add> "
        f"| 发送前编辑器: 【文本{text_before}字 / 媒体{media_before}个】"
    )
    watcher = _ApiWatcher(page)
    try:
        clicked = False
        for attempt in range(3):
            hit = _hit_test(page, send_button)
            if not hit.get("ok"):
                logger.warning(
                    f"[发送/提交] 发送按钮不可命中，先清障再重试 | 第【{attempt + 1}】次 | 详情: 【{_fmt_hit(hit)}】"
                )
                _dismiss_overlays(page, aggressive=(attempt >= 1), desc=f"send-blocked-{attempt}")
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
                logger.warning(f"[发送/提交] 第【{attempt + 1}】次点击失败，升级点击方式重试 | 详情: 【{str(ce)[:150]}】")

        if not clicked:
            _forensics(page, "send_click_fail", {"hit": _hit_test(page, send_button)})
            raise Exception("发送按钮点击 3 级降级全部失败（疑似被浮层持续拦截或按钮已失效）。")

        try:
            re_follow_and_reply = re.compile(r"关注并回复|Follow and [R|r]eply", re.IGNORECASE)
            follow_reply_btn = (
                page.locator("div[role='dialog'], [class*='modal']")
                .get_by_role("button", name=re_follow_and_reply)
                .first
            )

            if follow_reply_btn.is_visible(timeout=1500):
                logger.info("[发送/权限] 触发了「仅限关注者评论」限制，正在自动点击【关注并回复】")
                watcher.primary.clear()
                watcher.secondary.clear()
                follow_reply_btn.click(timeout=3000)
                page.wait_for_timeout(500)
        except Exception:
            pass

        api_success, comment_id = _read_api_verdict(page, watcher)
    except PlaywrightTimeoutError as e:
        logger.warning(f"[发送/校验] 等待接口响应超时，转入 DOM 兜底校验 | 详情: 【{str(e)[:120]}】")
    finally:
        watcher.close()

    if api_success:
        return comment_id

    page.wait_for_timeout(3000)
    text_after, media_after = _snapshot_editor(real_editor)
    if (text_before > 0 and text_after < text_before / 3) or (media_before > 0 and media_after < media_before):
        logger.info(
            f"[发送/校验] DOM 兜底比对通过（输入框已被大幅清空） | 文本: 【{text_before}→{text_after}】 "
            f"| 媒体: 【{media_before}→{media_after}】 | 结果: [Success]"
        )
        return comment_id

    report_guard_hits(page, "send-fail")
    _forensics(
        page,
        "send_no_effect",
        {
            "text_before": text_before,
            "text_after": text_after,
            "media_before": media_before,
            "media_after": media_after,
            "send_btn_hit_test": _hit_test(page, send_button),
        },
    )
    raise Exception(
        f"发送已点击但输入框未清空且接口无成功响应（文本 {text_before}→{text_after}，"
        f"媒体 {media_before}→{media_after}），疑似发送按钮失效、内容被前端校验拦下或网络堵塞。"
    )


# ==============================================================================
#                              URL / 帖子ID 解析与导航体检
# ==============================================================================
def extract_binance_post_id(post_url):
    """从帖子 URL 提取纯数字 post_id。"""
    raw = str(post_url or "").strip()
    if not raw:
        return None
    try:
        url = raw if raw.startswith(("http://", "https://")) else "https://" + raw
        parsed = urlparse(url)
        if re.search(r"(^|\.)binance\.(com|info|me)$", (parsed.hostname or "").lower()):
            m = re.search(r"/square/post/(?:[a-zA-Z\-]+/)?(\d+)", parsed.path) or re.search(r"/post/(\d+)", parsed.path)
            if m:
                return m.group(1)

        m = re.search(r"/post/(?:[a-zA-Z\-]+/)?(\d+)", raw)
        if m:
            return m.group(1)
        tail = raw.split("?")[0].strip("/").split("/")[-1]
        return tail if tail.isdigit() else None
    except Exception:
        return None


def _verify_page_ready(page, response, post_id):
    """落地页四重体检，返回错误描述(str) 或 None(通过)。"""
    if response and response.status >= 400:
        return f"页面加载异常或帖子已被删除 (HTTP 状态码: {response.status})"

    if post_id:
        landed = unquote(page.url)
        if post_id not in landed:
            page.wait_for_timeout(2000)
            landed = unquote(page.url)
        if post_id not in landed:
            editor_alive = True
            try:
                page.locator(
                    "div[contenteditable='true'].ProseMirror,[placeholder*='回复'],[placeholder*='评论']"
                ).first.wait_for(state="attached", timeout=5000)
            except Exception:
                editor_alive = False
            if not editor_alive:
                return f"页面发生重定向，目标帖子已被删除或失效 | 原帖子ID: {post_id} | 现落地URL: {landed}"
            logger.info(f"[导航/Nav] URL 漂移但评论编辑器存在，判定为前端路由行为，放行 | 落地URL: <{landed}>")

    try:
        page.locator("a[href*='login']").first.wait_for(state="visible", timeout=3000)
        return "页面探测到 Login 按钮，本地 Cookie 可能已过期失效。"
    except PlaywrightTimeoutError:
        pass

    try:
        page.locator("text=您无法查看此内容").wait_for(state="visible", timeout=2500)
        return "触发平台软拦截（账号已被该创作者拉黑或设置了权限），安全跳过当前任务。"
    except PlaywrightTimeoutError:
        pass

    return None


def _locate_post_creator(page):
    """精准锁定广场主页最顶部的发帖区容器。"""
    precise_locator = page.locator("div.short-editor-inner").first
    try:
        if precise_locator.is_visible(timeout=3000):
            logger.info("[定位/DOM] 已精确锁定主页发帖区作用域 (基于 short-editor-inner)")
            return precise_locator
    except Exception:
        pass

    try:
        fallback_input = page.get_by_placeholder(re.compile(r"分享您的洞见|Share your insights", re.IGNORECASE)).first
        if fallback_input.is_visible(timeout=3000):
            container = fallback_input.locator(
                "xpath=ancestor::div[has(div[@contenteditable='true'] or input[@type='file'])][1]"
            )
            if container.count() > 0:
                logger.info("[定位/DOM] 已锁定主页发帖区作用域 (基于占位符溯源)")
                return container.first
    except Exception:
        pass

    logger.warning("[定位/DOM] 精确定位与溯源均未命中，降级为锁定页面首个富文本容器")
    return page.locator('div:has(div[contenteditable="true"].ProseMirror), div:has(input[type="file"])').first


# ==============================================================================
#                          会话管理 / 凭证提取 / 主控入口
# ==============================================================================
def login_and_save_session():
    """打开可见浏览器供人工手动登录，回车后关闭并把会话固化到本地 User Data 目录。"""
    logger.info(f"[登录/Auth] 准备手动登录 | 存储路径: <{USER_DATA_DIR}>")
    with _user_data_dir_lock(USER_DATA_DIR, timeout=USER_DATA_LOCK_TIMEOUT_SEC):
        clean_browser_cache(USER_DATA_DIR)

        with sync_playwright() as p:
            context = None
            try:
                context = _launch_persistent(
                    p,
                    USER_DATA_DIR,
                    args=["--disable-blink-features=AutomationControlled", "--start-maximized"],
                    hide_automation=False,
                    debug=True,
                )
                page = context.new_page()
                page.goto(LOGIN_URL)
                input("\n[登录/Auth] 等待操作 | 动作: 【登录成功后，请按 Enter 键关闭并保存会话】")
                logger.info("[登录/Auth] 会话已固化到本地 | 结果: [Success]")
            finally:
                if context:
                    try:
                        context.close()
                    except Exception:
                        pass


def open_browser_for_manual_use(user_data_dir=USER_DATA_DIR, home_url="https://www.binance.com/zh-CN"):
    """启动可见浏览器交由人工自由操作。"""
    return _base_open_browser_for_manual_use(user_data_dir=user_data_dir, home_url=home_url)


def create_binance_post(
    content="",
    image_path_list=None,
    user_data_dir=USER_DATA_DIR,
    url_info_list=None,
    chart_info=None,
    sentiment=None,
    debug=True,
    text_list=None,
):
    """
    主控入口：调度浏览器打开广场主页并执行【独立发帖】全流程。
    返回 Tuple(错误信息(str|None), 是否成功(bool), 帖子ID(str|None))。
    """
    if not os.path.isdir(user_data_dir):
        return f"缺少用户环境: {user_data_dir}，请先执行登录", False, None

    valid_text_list = _normalize_text_list(text_list)
    text_summary = (
        f"text_list({len(valid_text_list)}项/{sum(len(x['text']) for x in valid_text_list)}字)"
        if valid_text_list
        else f"{len(str(content or ''))}字"
    )
    img_count = len(image_path_list) if isinstance(image_path_list, list) else (1 if image_path_list else 0)
    has_chart = "是" if chart_info else "否"
    square_url = "https://www.binance.com/zh-CN/square"
    logger.info(
        f"\n{'=' * 70}\n[任务/Main] 启动自动化发帖 | URL: <{square_url}> "
        f"| 正文: 【{text_summary}】 | 图片数量: 【{img_count}】张 "
        f"| 链接数: 【{len(url_info_list or [])}】 | 包含图表: 【{has_chart}】 "
        f"| 情绪标签: 【{sentiment or '无'}】 "
        f"| 模式: 【{'debug可见' if debug else '离屏后台'}】\n{'=' * 70}"
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

                    response = page.goto(square_url, timeout=60000, wait_until="domcontentloaded")
                    _dismiss_overlays(page, aggressive=False, desc="square-nav")
                    report_guard_hits(page, "square-nav")

                    if response and response.status >= 400:
                        return f"广场主页加载异常 (HTTP {response.status})", False, None

                    try:
                        page.locator("a[href*='login']").first.wait_for(state="visible", timeout=3000)
                        return "页面探测到 Login 按钮，本地 Cookie 可能已过期失效。", False, None
                    except PlaywrightTimeoutError:
                        pass

                    check_for_crash(page)

                    try:
                        editor_container = _locate_post_creator(page)
                    except Exception as e:
                        return f"无法定位发帖区: {e}", False, None

                    post_id = _submit_comment(
                        page,
                        editor_container,
                        content,
                        image_path_list,
                        url_info_list,
                        chart_info,
                        sentiment,
                        text_list=text_list,
                    )

                    logger.info(f"[任务/Main] 帖子发布成功 | 帖子关联ID: 【{post_id}】 | 结果: [Success]")
                    return None, True, post_id

                except BusinessErrorException as biz_e:
                    error_info = f"[业务拦截] {biz_e}"
                    logger.error(f"[任务/Main] 发帖被服务端业务规则阻断 | 原因: 【{error_info}】")
                    return error_info, False, None

                except Exception as e:
                    error_info = f"[{type(e).__name__}] {e}"
                    if context and context.pages:
                        try:
                            _forensics(context.pages[0], "post_error", {"url": square_url, "error": error_info[:2000]})
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


def comment_on_binance_post(
    post_url,
    comment="",
    image_path_list=None,
    user_data_dir=USER_DATA_DIR,
    url_info_list=None,
    chart_info=None,
    sentiment=None,
    debug=False,
    text_list=None,
):
    """
    主控入口：调度浏览器打开帖子并执行评论全流程。
    返回 Tuple(错误信息(str|None), 是否成功(bool), 评论ID(str|None))。
    """
    if not os.path.isdir(user_data_dir):
        return f"缺少用户环境: {user_data_dir}，请先执行登录", False, None

    post_id = extract_binance_post_id(post_url)
    valid_text_list = _normalize_text_list(text_list)
    text_summary = (
        f"text_list({len(valid_text_list)}项/{sum(len(x['text']) for x in valid_text_list)}字)"
        if valid_text_list
        else f"{len(str(comment or ''))}字"
    )
    img_count = len(image_path_list) if isinstance(image_path_list, list) else (1 if image_path_list else 0)
    has_chart = "是" if chart_info else "否"
    logger.info(
        f"\n{'=' * 70}\n[任务/Main] 启动自动化评论 | 帖子ID: 【{post_id}】 | URL: <{post_url}> "
        f"| 正文: 【{text_summary}】 | 图片数量: 【{img_count}】张 "
        f"| 链接数: 【{len(url_info_list or [])}】 | 包含图表: 【{has_chart}】 "
        f"| 情绪标签: 【{sentiment or '无'}】 "
        f"| 模式: 【{'debug可见' if debug else '离屏后台'}】\n{'=' * 70}"
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
                    _dismiss_overlays(page, aggressive=False, desc="post-nav")
                    report_guard_hits(page, "post-nav")

                    page_err = _verify_page_ready(page, response, post_id)
                    if page_err:
                        logger.warning(
                            f"[导航/Nav] 落地页体检未通过，安全跳过本任务 | 帖子ID: 【{post_id}】 | 原因: 【{page_err}】"
                        )
                        return page_err, False, None

                    check_for_crash(page)

                    _dismiss_overlays(page, aggressive=False, desc="pre-scroll")
                    try:
                        editor_container = _smart_scroll_to_editor(page)
                    except PlaywrightTimeoutError:
                        logger.warning("[导航/Nav] 编辑器定位超时，疑似 scroll-lock 未解除，强制清障后重试一次")
                        _dismiss_overlays(page, aggressive=True, desc="scroll-lock-retry")
                        page.wait_for_timeout(500)
                        editor_container = _smart_scroll_to_editor(page)

                    check_for_crash(page)

                    comment_id = _submit_comment(
                        page,
                        editor_container,
                        comment,
                        image_path_list,
                        url_info_list,
                        chart_info,
                        sentiment,
                        text_list=text_list,
                    )
                    logger.info(
                        f"[任务/Main] 评论发送成功 | 帖子ID: 【{post_id}】 | 评论ID: 【{comment_id}】 | 结果: [Success]"
                    )
                    return None, True, comment_id

                except BusinessErrorException as biz_e:
                    error_info = f"[业务拦截] {biz_e}"
                    logger.error(
                        f"[任务/Main] 发帖被服务端业务规则阻断，无需重试 | 帖子ID: 【{post_id}】 | 原因: 【{error_info}】"
                    )
                    return error_info, False, None

                except Exception as e:
                    is_timeout = isinstance(e, PlaywrightTimeoutError)
                    if is_timeout:
                        error_info = f"[元素/网络超时] {e}"
                        logger.error(f"[任务/Main] 元素等待或网络请求超时 | 帖子ID: 【{post_id}】 | 详情: 【{error_info[:200]}】")
                    else:
                        error_info = f"[{type(e).__name__}] {e}\n[Traceback]:\n{traceback.format_exc()}"
                        logger.error(f"[任务/Main] 执行中发生未预期异常 | 帖子ID: 【{post_id}】 | 摘要: 【{str(e)[:200]}】")

                    if context and context.pages:
                        try:
                            pg = context.pages[0]
                            report_guard_hits(pg, "timeout" if is_timeout else "error")
                            _forensics(
                                pg,
                                ("timeout" if is_timeout else "error") + f"_{post_id}",
                                {"post_url": post_url, "error": error_info[:2000]},
                            )
                        except Exception as s_e:
                            logger.warning(f"[任务/Debug] 故障现场保存失败: {s_e}")

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
        error_info = (
            f"[CoreEngineCrash] Playwright 底层启动/运行发生系统级崩溃:\n{core_e}\n\n"
            f"[Traceback]:\n{traceback.format_exc()}"
        )
        logger.error(f"[任务/Main] Playwright 核心框架崩溃 | 详情: 【{str(core_e)[:200]}】")
        return error_info, False, None


def get_auth_tokens_robust(user_data_dir, debug=False):
    """以真实浏览器请求为样本提取脱机 API 凭证，同时顺带提取 User Data。"""
    if not os.path.exists(user_data_dir):
        logger.warning(f"[凭证/Auth] 环境目录不存在，无法提取 | 目录: <{user_data_dir}>")
        return None, None, None

    visit_url = "https://www.binance.com/zh-CN/square/profile/insights_anchor"
    api_keyword = "pgc/user/client"
    user_api_keyword = "pgc/user?getFollowCount"

    logger.info(
        f"[凭证/Auth] 启动浏览器提取凭证 | 目录: <{user_data_dir}> | 主拦截: <{api_keyword}> | 辅拦截: <{user_api_keyword}>"
    )

    try:
        with _user_data_dir_lock(user_data_dir, timeout=USER_DATA_LOCK_TIMEOUT_SEC):
            with sync_playwright() as p:
                context = None
                try:
                    context = _launch_persistent(
                        p,
                        user_data_dir,
                        args=["--disable-blink-features=AutomationControlled"],
                        viewport={"width": 1280, "height": 720},
                        hide_automation=False,
                        debug=debug,
                    )
                    page = context.pages[0] if context.pages else context.new_page()

                    user_api_responses = []

                    def on_response(res):
                        if user_api_keyword in res.url and res.request.method != "OPTIONS":
                            user_api_responses.append(res)

                    page.on("response", on_response)

                    with page.expect_request(
                        lambda req: api_keyword in req.url and req.method != "OPTIONS", timeout=20000
                    ) as req_info:
                        page.goto(visit_url, wait_until="networkidle")

                    req = req_info.value
                    headers = req.headers
                    csrf = (headers.get("csrftoken") or "").strip() or None
                    cookie = (headers.get("cookie") or "").strip()
                    source = "request-header"

                    if not cookie:
                        raw_cookies = context.cookies("https://www.binance.com")
                        cookie = "; ".join(f"{c['name']}={c['value']}" for c in raw_cookies).strip()
                        source = "context-cookies(binance域)"

                    if not cookie:
                        logger.warning(
                            f"[凭证/Auth] 提取失败：捕获到请求但无任何合法凭据 | 目录: <{user_data_dir}> | 请求: 【{req.method} {req.url}】"
                        )
                        return None, None, None

                    has_p20t = "p20t=" in cookie
                    level = logger.info if (has_p20t and csrf) else logger.warning
                    level(
                        f"[凭证/Auth] 凭证提取完成 | 目录: <{user_data_dir}> | 来源: <{source}> | CSRF: 【{str(csrf)[:8]}...】 "
                        f"| Cookie长度: 【{len(cookie)}】 | 含核心 p20t: 【{has_p20t}】"
                    )

                    user_data = None
                    for res in user_api_responses:
                        try:
                            if res.ok:
                                json_body = res.json()
                                if json_body and json_body.get("success"):
                                    user_data = json_body.get("data")
                                    logger.info(
                                        f"[凭证/Auth] 成功捕获用户信息 | 目录: <{user_data_dir}> | 昵称: 【{user_data.get('displayName')}】 | UID: 【{user_data.get('squareUid')}】"
                                    )
                                    break
                        except Exception as e:
                            logger.debug(f"[凭证/Auth] 尝试解析用户信息响应时出现异常(已忽略): {e}")

                    return cookie, csrf, user_data

                except PlaywrightTimeoutError:
                    logger.warning(
                        f"[凭证/Auth] 提取失败：20s 内未捕获到目标接口 <{api_keyword}> | 目录: <{user_data_dir}>"
                    )
                    return None, None, None
                except Exception as e:
                    logger.error(f"[凭证/Auth] 提取过程发生未预期异常 | 目录: <{user_data_dir}> | 详情: 【{e}】")
                    return None, None, None
                finally:
                    if context:
                        try:
                            context.close()
                        except Exception:
                            pass
    except UserDataDirLockTimeoutError as lock_e:
        logger.error(f"[凭证/Auth] 等待 user_data_dir 锁超时，放弃提取 | 目录: <{user_data_dir}> | 详情: 【{lock_e}】")
        return None, None, None


# ==============================================================================
#                                   启动入口
# ==============================================================================
if __name__ == "__main__":
    # 其他可选入口（按需取消注释）：
    # login_and_save_session()
    open_browser_for_manual_use(USER_DATA_DIR)
    # cookies, csrf, udata = get_auth_tokens_robust(USER_DATA_DIR)

    test_content = (
        "$BTC \n\n交易所的估值天花板，正在被真实的线下消费捅破。\n\n"
        "何一确认：币安支付现已接入日本全国 PayPay 商户。这不是小圈子里的链上自嗨，而是把加密支付直接接进了主流线下商业网络。\n\n"
        "多数人看 BNB，眼光始终停留在手续费折扣和打新挖矿。但当它跳出交易所围墙、切入真实世界的支付场景时，流动性就不再只是投机筹码，而是有了实体刚需做托底。\n\n"
        "靠发预告撑不起长线，能在街头实体里跑通闭环的生态，基本盘只会越来越硬。"
    )

    target_chart = {"coin": "BTC", "bridge": "USDT", "type": "future"}
    test_links = [{"text": "更多行情", "url": "https://www.binance.com"}]
    test_images = [r"E:\chrome\1759239193.png"]

    err, success, p_id = create_binance_post(
        content=test_content,
        image_path_list=test_images,
        url_info_list=test_links,
        chart_info=target_chart,
        user_data_dir=USER_DATA_DIR,
        debug=True,
    )

    if success:
        logger.info(f"\n[结果/Final] 🎉 ======== 带图表发帖任务成功 ======== | 帖子ID: 【{p_id}】")
    else:
        logger.error(f"\n[结果/Final] ❌ ======== 带图表发帖任务失败 ======== | 错误原因:\n{err}")
