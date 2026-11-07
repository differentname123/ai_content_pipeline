# -*- coding: utf-8 -*-
"""
Playwright 跨平台共享自动化底座 (Binance Square & OKX Orbit 共用)：
- user_data_dir 跨进程/跨线程可重入独占锁 (_user_data_dir_lock) 与残留锁自愈
- 持久化浏览器上下文启动 (_launch_persistent) 与缓存瘦身 (clean_browser_cache)
- 故障现场取证 (_forensics，统一输出至 logs/forensics/) 与遮挡命中测试 (_hit_test)
- 三级浮层清障 (_dismiss_overlays) 与常驻弹窗守卫 (install_overlay_guard)
- 富文本编辑器光标置顶/置底 (_force_caret_to_head / _force_caret_to_tail) 与分块键入 (_type_body / _type_text_list)
- 多重选择器回退定位 (_interact_fallback_locators)、三段式点击 (_robust_click)、接口响应监听 (_BaseApiWatcher)
- 人工接管浏览器窗口 (open_browser_for_manual_use)
"""
import atexit
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
from pathlib import Path

if os.name == "nt":
    import msvcrt
else:
    import fcntl

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import expect, sync_playwright

from common.common_utils import (
    DEFAULT_FORENSICS_DIR,
    DEFAULT_SESSION_ROOT,
    get_config,
    resolve_cross_platform_path,
    setup_logger,
)

logger = setup_logger(app_name="playwright_base")

# ==============================================================================
#                                 通用运行配置与常量
# ==============================================================================
TYPE_CHUNK_SIZE = 80
TYPE_DELAY_MS = 60
API_WAIT_TIMEOUT_MS = 10000
GUARD_INTERVAL_MS = 700
USER_DATA_LOCK_TIMEOUT_SEC = 300
NEWLINE_SAFE_MODE = False

RE_MORE = re.compile(r"更多|More|Options|Expand", re.IGNORECASE)
RE_ADD_LINK = re.compile(r"添加链接|Add link|Insert link", re.IGNORECASE)
RE_CONFIRM = re.compile(r"确认|Confirm|OK|Save|Add", re.IGNORECASE)

RE_SEND = re.compile(r"回复|发送|评论|发文|发布|Reply|Comment|Send|Post|Publish", re.IGNORECASE)
RE_SEND_EXACT = re.compile(r"^(回复|发送|评论|发文|发布|Reply|Comment|Send|Post|Publish)$", re.IGNORECASE)

RE_COMMENT_BTN = re.compile(r"评论|回复|发送|Comment|Reply|Send", re.IGNORECASE)
RE_COMMENT_BTN_EXACT = re.compile(r"^(评论|回复|发送|Comment|Reply|Send)$", re.IGNORECASE)

RE_DISMISS = re.compile(
    r"^\s*(好的|好|知道了|我知道了|明白了|明白|了解|开始使用|立即体验|马上体验|下一步|"
    r"完成|跳过|不再提示|不再显示|以后再说|稍后|关闭|"
    r"OK|Okay|Got it|Got It|I see|Understood|Skip|Next|Done|Continue|Close|Dismiss|Later|Maybe later)\s*$",
    re.IGNORECASE,
)

RE_SEND_BLACKLIST = re.compile(
    r"(文章|立即回复|去回复|查看|更多|展开|取消|Article|Cancel|View|More)",
    re.IGNORECASE,
)

COOKIE_SELECTORS = (
    "#onetrust-accept-btn-handler",
    "#onetrust-close-btn-container button",
    "button:has-text('接受所有 Cookie')",
    "button:has-text('全部允许')",
    "button:has-text('确认我的选择')",
    "button:has-text('Accept All')",
    "button:has-text('Allow All')",
)

EDITOR_GUARD_SELECTOR = '.ProseMirror,[contenteditable="true"],input[type="file"],textarea'


class PageCrashedException(Exception):
    """页面崩溃/死机（如内存溢出触发的重新加载）"""


class BusinessErrorException(Exception):
    """发送请求被服务端业务规则拦截"""


class UserDataDirLockTimeoutError(TimeoutError):
    """等待 user_data_dir 独占锁超时"""


# ==============================================================================
#            user_data_dir 跨进程 / 跨线程 可重入防死锁安全锁
# ==============================================================================
_PROCESS_LOCK_REGISTRY_GUARD = threading.Lock()
_PROCESS_LOCKS_BY_KEY = {}


def _normalize_user_data_dir_key(user_data_dir):
    raw = str(user_data_dir or "").strip()
    norm_path = os.path.normcase(os.path.abspath(raw))
    digest = hashlib.sha256(norm_path.encode("utf-8", errors="ignore")).hexdigest()[:24]
    return norm_path, digest


def _try_os_lock_fd(fd):
    if os.name == "nt":
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
    else:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock_os_lock_fd(fd):
    if os.name == "nt":
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
    else:
        fcntl.flock(fd, fcntl.LOCK_UN)


def _clean_stale_chromium_singleton(user_data_dir):
    """清理上次异常强退可能遗留的 SingletonLock / SingletonCookie / SingletonSocket 标记。"""
    if not user_data_dir or not os.path.isdir(user_data_dir):
        return
    for name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        p = os.path.join(user_data_dir, name)
        if os.path.exists(p) or os.path.islink(p):
            try:
                os.remove(p)
            except Exception:
                pass


class _ReentrantUserDataLock:
    """
    单目录锁管理器：
    1. 进程内通过 RLock + 线程递归计数支持多线程排队与同线程重入；
    2. 跨进程通过 OS 内核级文件描述符锁 (msvcrt.locking / fcntl.flock) + 禁止句柄继承，
       保证任何异常退出内核都会立即释放锁，绝不死锁。
    """

    def __init__(self, norm_path, digest):
        self.norm_path = norm_path
        self.digest = digest
        self.rlock = threading.RLock()
        self.owner_thread_id = None
        self.depth = 0
        self.fd = None
        lock_dir = os.path.join(tempfile.gettempdir(), "playwright_userdata_locks")
        os.makedirs(lock_dir, exist_ok=True)
        self.lock_file_path = os.path.join(lock_dir, f"ud_{digest}.lock")
        self.meta_file_path = os.path.join(lock_dir, f"ud_{digest}.meta.json")

    def _read_holder_meta(self):
        try:
            if os.path.exists(self.meta_file_path):
                with open(self.meta_file_path, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception:
            pass
        return {}

    def _write_holder_meta(self):
        try:
            payload = {
                "pid": os.getpid(),
                "thread_id": threading.get_ident(),
                "user_data_dir": self.norm_path,
                "acquired_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "timestamp": time.time(),
            }
            with open(self.meta_file_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
        except Exception:
            pass

    def acquire(self, timeout=USER_DATA_LOCK_TIMEOUT_SEC):
        start_ts = time.monotonic()
        deadline = start_ts + max(0.0, float(timeout))
        tid = threading.get_ident()

        remaining = max(0.0, deadline - time.monotonic())
        if not self.rlock.acquire(timeout=remaining if timeout > 0 else -1):
            meta = self._read_holder_meta()
            raise UserDataDirLockTimeoutError(
                f"等待同进程线程释放 user_data_dir 超时({timeout}s) | 目录: <{self.norm_path}> | 占用信息: {meta}"
            )

        try:
            if self.depth > 0 and self.owner_thread_id == tid:
                self.depth += 1
                return

            last_log_ts = 0.0
            while True:
                fd = None
                try:
                    flags = os.O_CREAT | os.O_RDWR
                    if hasattr(os, "O_NOINHERIT"):
                        flags |= os.O_NOINHERIT
                    fd = os.open(self.lock_file_path, flags, 0o666)
                    try:
                        os.set_inheritable(fd, False)
                    except Exception:
                        pass

                    if os.fstat(fd).st_size == 0:
                        os.write(fd, b"L")

                    _try_os_lock_fd(fd)

                    self.fd = fd
                    self.owner_thread_id = tid
                    self.depth = 1
                    self._write_holder_meta()
                    _clean_stale_chromium_singleton(self.norm_path)
                    waited = time.monotonic() - start_ts
                    if waited >= 1.0:
                        logger.info(
                            f"[并发锁/Lock] 已成功获取 user_data_dir 独占锁 (等待了 {waited:.1f}s) "
                            f"| 目录: <{self.norm_path}> | PID: 【{os.getpid()}】"
                        )
                    return
                except (OSError, IOError):
                    if fd is not None:
                        try:
                            os.close(fd)
                        except Exception:
                            pass

                    now = time.monotonic()
                    if now >= deadline:
                        meta = self._read_holder_meta()
                        raise UserDataDirLockTimeoutError(
                            f"等待 user_data_dir 跨进程锁超时({timeout}s)，其它程序仍在占用该目录 "
                            f"| 目录: <{self.norm_path}> | 占用方详情: {meta}"
                        )

                    if now - last_log_ts >= 10.0:
                        last_log_ts = now
                        meta = self._read_holder_meta()
                        left_sec = int(max(0.0, deadline - now))
                        logger.warning(
                            f"[并发锁/Wait] user_data_dir 正在被其它程序使用，排队等待中... "
                            f"| 目录: <{self.norm_path}> | 占用PID: 【{meta.get('pid', '未知')}】 "
                            f"| 剩余最长等待: 【{left_sec}s】"
                        )
                    time.sleep(min(0.5, max(0.05, deadline - now)))
        except BaseException:
            self.rlock.release()
            raise

    def release(self):
        tid = threading.get_ident()
        if self.depth <= 0 or self.owner_thread_id != tid:
            return
        try:
            self.depth -= 1
            if self.depth == 0:
                self.owner_thread_id = None
                fd = self.fd
                self.fd = None
                if fd is not None:
                    try:
                        _unlock_os_lock_fd(fd)
                    except Exception:
                        pass
                    try:
                        os.close(fd)
                    except Exception:
                        pass
                try:
                    if os.path.exists(self.meta_file_path):
                        os.remove(self.meta_file_path)
                except Exception:
                    pass
        finally:
            try:
                self.rlock.release()
            except Exception:
                pass

    def force_release_on_exit(self):
        fd = self.fd
        self.fd = None
        self.depth = 0
        self.owner_thread_id = None
        if fd is not None:
            try:
                _unlock_os_lock_fd(fd)
            except Exception:
                pass
            try:
                os.close(fd)
            except Exception:
                pass


def _get_user_data_lock_obj(user_data_dir):
    norm_path, digest = _normalize_user_data_dir_key(user_data_dir)
    with _PROCESS_LOCK_REGISTRY_GUARD:
        lock_obj = _PROCESS_LOCKS_BY_KEY.get(norm_path)
        if lock_obj is None:
            lock_obj = _ReentrantUserDataLock(norm_path, digest)
            _PROCESS_LOCKS_BY_KEY[norm_path] = lock_obj
        return lock_obj


@contextmanager
def _user_data_dir_lock(user_data_dir, timeout=USER_DATA_LOCK_TIMEOUT_SEC):
    """可重入的 user_data_dir 独占锁上下文管理器。"""
    lock_obj = _get_user_data_lock_obj(user_data_dir)
    lock_obj.acquire(timeout=timeout)
    try:
        yield lock_obj
    finally:
        lock_obj.release()


def _cleanup_all_locks_at_exit():
    with _PROCESS_LOCK_REGISTRY_GUARD:
        locks = list(_PROCESS_LOCKS_BY_KEY.values())
    for lk in locks:
        try:
            lk.force_release_on_exit()
        except Exception:
            pass


atexit.register(_cleanup_all_locks_at_exit)


# ==============================================================================
#                            浏览器 / 通用底层工具
# ==============================================================================
def clean_browser_cache(user_data_dir):
    """清理浏览器冗余缓存目录、保留登录凭证。"""
    if not os.path.exists(user_data_dir):
        return

    with _user_data_dir_lock(user_data_dir, timeout=USER_DATA_LOCK_TIMEOUT_SEC):
        garbage = (
            "Cache",
            "Code Cache",
            "GPUCache",
            "ShaderCache",
            "GrShaderCache",
            "Service Worker",
            "CacheStorage",
        )
        deleted = 0
        for base in (user_data_dir, os.path.join(user_data_dir, "Default")):
            for name in garbage:
                path = os.path.join(base, name)
                if not os.path.exists(path):
                    continue
                try:
                    if os.path.isdir(path):
                        shutil.rmtree(path, ignore_errors=True)
                    else:
                        os.remove(path)
                    deleted += 1
                except Exception:
                    pass
        logger.info(f"[缓存/Clean] 浏览器数据瘦身完成 | 目录: <{user_data_dir}> | 清理冗余项: 【{deleted}】")


def check_for_crash(page):
    """探测页面渲染崩溃：500ms 窗口内出现【重新加载】按钮即判定崩溃。"""
    try:
        page.get_by_role("button", name="重新加载").first.wait_for(state="visible", timeout=500)
    except PlaywrightTimeoutError:
        return
    raise PageCrashedException("页面 DOM 渲染崩溃，检测到【重新加载】按钮")


def _interact_fallback_locators(locators, action="wait", timeout=5000, desc="目标元素"):
    """轮询后备选择器清单，把长阻塞打散为 200ms 时间片。"""
    end_time = time.time() + timeout / 1000.0
    last_err = None
    while time.time() < end_time:
        for loc in locators:
            try:
                loc.wait_for(state="visible", timeout=200)
                if action == "click":
                    loc.click(timeout=1500)
                return loc
            except Exception as e:
                last_err = e
    raise Exception(f"在 {timeout}ms 内未能 {action} 【{desc}】 | 底层最后错误: {str(last_err)[:100]}")


def _robust_click(locator):
    """三段降级点击：常规 → 强制穿透遮挡 → JS 原生。"""
    for attempt in ("normal", "force"):
        try:
            locator.click(timeout=1500, force=(attempt == "force"))
            return
        except Exception:
            continue
    locator.evaluate("node => node.click()")


def _focus_editor_end(page, editor_node):
    """把光标聚焦到富文本末尾，为后续键入 / 唤醒菜单做准备。"""
    try:
        editor_node.click(timeout=2000)
    except Exception:
        pass
    page.keyboard.press("End")
    page.wait_for_timeout(120)


def _snapshot_editor(editor):
    """读取编辑器状态快照，返回 (文本字符数, 媒体元素数[img/a])。"""
    try:
        if not editor.is_visible():
            return 0, 0
        return len(editor.inner_text().strip()), editor.locator("img, a").count()
    except Exception:
        return 0, 0


# ==============================================================================
#                            存证 / 遮挡命中测试
# ==============================================================================
_PAGE_DIAG_JS = r"""
() => {
    const cx = innerWidth / 2, cy = innerHeight / 2;
    const top = document.elementFromPoint(cx, cy);
    return {
        url: location.href,
        bodyOverflow: getComputedStyle(document.body).overflow,
        centerTag: top ? top.tagName : null,
        centerClass: top ? (top.className || '').toString().slice(0, 200) : null,
        centerText: top ? (top.innerText || '').replace(/\s+/g, ' ').slice(0, 200) : null,
        guardHits: window.__bnGuardHits || 0,
        guardLast: window.__bnGuardLast || ''
    };
}
"""


def _forensics(page, tag, extra=None):
    """统一存证：截图 + HTML + JSON（统一保存至 logs/forensics/ 目录）。"""
    save_dir = str(DEFAULT_FORENSICS_DIR)
    try:
        os.makedirs(save_dir, exist_ok=True)
    except Exception:
        pass

    base_name = f"forensic_{tag}_{int(time.time() * 1000)}"
    base_path = os.path.join(save_dir, base_name)

    payload = dict(extra or {})
    try:
        payload["page_diag"] = page.evaluate(_PAGE_DIAG_JS)
    except Exception as e:
        payload["page_diag"] = f"unavailable:{str(e)[:80]}"

    try:
        page.screenshot(path=f"{base_path}.png", full_page=False)
    except Exception:
        pass
    try:
        with open(f"{base_path}.html", "w", encoding="utf-8") as f:
            f.write(page.content())
    except Exception:
        pass
    try:
        with open(f"{base_path}.json", "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
    except Exception:
        pass

    logger.warning(f"[存证/Forensic] 故障现场已落盘 | 文件前缀: <{base_path}> | 页面诊断: 【{payload.get('page_diag')}】")
    return base_path


_HIT_TEST_JS = r"""
(el) => {
    const r = el.getBoundingClientRect();
    if (r.width === 0 || r.height === 0) return {ok:false, reason:'zero-size'};
    const cx = r.left + r.width / 2;
    const cy = r.top + r.height / 2;
    if (cx < 0 || cy < 0 || cx > window.innerWidth || cy > window.innerHeight) {
        return {ok:false, reason:'outside-viewport',
                rect:{x:Math.round(r.left), y:Math.round(r.top),
                      w:Math.round(r.width), h:Math.round(r.height)}};
    }
    const top = document.elementFromPoint(cx, cy);
    if (!top) return {ok:false, reason:'no-element-at-point'};
    if (top === el || el.contains(top) || top.contains(el)) return {ok:true};

    let blocker = top, hop = 0;
    while (blocker.parentElement && hop < 12) {
        const st = window.getComputedStyle(blocker);
        if (st.position === 'fixed' && (parseInt(st.zIndex || '0', 10) > 0)) break;
        blocker = blocker.parentElement;
        hop++;
    }
    const br = blocker.getBoundingClientRect();
    return {
        ok: false,
        reason: 'intercepted',
        blockerTag: blocker.tagName,
        blockerClass: (blocker.className || '').toString().slice(0, 200),
        blockerId: blocker.id || '',
        blockerText: (blocker.innerText || '').replace(/\s+/g, ' ').slice(0, 200),
        coverRatio: +(((br.width * br.height) /
                      (window.innerWidth * window.innerHeight)) || 0).toFixed(3),
        bodyOverflow: window.getComputedStyle(document.body).overflow
    };
}
"""


def _hit_test(page, locator):
    try:
        return locator.evaluate(_HIT_TEST_JS)
    except Exception as e:
        return {"ok": False, "reason": f"eval-error:{str(e)[:120]}"}


def _fmt_hit(hit):
    if hit.get("ok"):
        return "可点击"
    return (
        f"{hit.get('reason')} | 遮挡者: <{hit.get('blockerTag')}> "
        f"class={hit.get('blockerClass')} text='{hit.get('blockerText')}' "
        f"覆盖率={hit.get('coverRatio')} bodyOverflow={hit.get('bodyOverflow')}"
    )


# ==============================================================================
#                    浮层清障（分级歼灭 + 编辑器白名单保护）
# ==============================================================================
_NUKE_OVERLAY_JS = r"""
(guardSelector) => {
    const killed = [];
    const isProtected = (n) => {
        try { return n.querySelector(guardSelector) !== null; } catch (e) { return false; }
    };

    for (const el of [document.body, document.documentElement]) {
        try {
            const st = window.getComputedStyle(el);
            if (st.overflow === 'hidden' || st.overflowY === 'hidden') {
                el.style.setProperty('overflow', 'auto', 'important');
                el.style.setProperty('overflow-y', 'auto', 'important');
            }
            if (st.position === 'fixed') el.style.removeProperty('position');
            Array.from(el.classList).forEach(c => {
                if (/modal|dialog|lock|no-?scroll|overflow-hidden|popup-open/i.test(c)) {
                    el.classList.remove(c);
                }
            });
        } catch (e) {}
    }

    const cx = window.innerWidth / 2, cy = window.innerHeight / 2;
    const total = window.innerWidth * window.innerHeight;
    document.querySelectorAll('body *').forEach(n => {
        try {
            const st = window.getComputedStyle(n);
            if (st.position !== 'fixed' && st.position !== 'absolute') return;
            if (st.display === 'none' || st.visibility === 'hidden' || st.opacity === '0') return;
            const r = n.getBoundingClientRect();
            if (r.width < 40 || r.height < 40) return;
            const ratio = (r.width * r.height) / total;
            const coversCenter = (r.left <= cx && r.right >= cx && r.top <= cy && r.bottom >= cy);
            const z = parseInt(st.zIndex || '0', 10);
            if ((ratio >= 0.55 || (coversCenter && z >= 100)) && !isProtected(n)) {
                killed.push({
                    tag: n.tagName,
                    cls: (n.className || '').toString().slice(0, 80),
                    text: (n.innerText || '').replace(/\s+/g, ' ').slice(0, 60),
                    ratio: +ratio.toFixed(2), z: z
                });
                n.style.setProperty('pointer-events', 'none', 'important');
                n.style.setProperty('display', 'none', 'important');
            }
        } catch (e) {}
    });
    return killed;
}
"""

_INSIDE_EDITOR_JS = """
(el, gs) => {
    const box = el.closest("div[role='dialog'],[class*='modal'],[class*='mask'],[class*='guide'],[class*='popup']")
                || el.parentElement;
    return !!(box && box.querySelector(gs));
}
"""


def _dismiss_overlays(page, aggressive=False, desc=""):
    """分级清障（绝不触碰任何含编辑器的容器）。"""
    acted = False

    for sel in COOKIE_SELECTORS:
        try:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible(timeout=400):
                loc.click(timeout=2000, no_wait_after=True, force=True)
                acted = True
                logger.info(f"[清障/L1] 已关闭 Cookie 横幅 | 选择器: <{sel}> | 场景: <{desc}>")
                page.wait_for_timeout(250)
        except Exception:
            pass

    makers = (
        lambda: page.get_by_role("button", name=RE_DISMISS),
        lambda: page.locator(
            "div[role='dialog'],[class*='modal'],[class*='mask'],[class*='guide'],"
            "[class*='onboard'],[class*='popup'],[class*='tooltip'],[class*='tour']"
        )
        .locator("button,[role='button'],div[class*='btn'],span[class*='btn'],a[class*='btn']")
        .filter(has_text=RE_DISMISS),
        lambda: page.locator("button,[role='button'],div[class*='btn'],span[class*='btn']").filter(has_text=RE_DISMISS),
    )
    for maker in makers:
        try:
            loc = maker()
            for i in range(min(loc.count(), 3)):
                item = loc.nth(i)
                try:
                    if not item.is_visible(timeout=300):
                        continue
                    if item.evaluate(_INSIDE_EDITOR_JS, EDITOR_GUARD_SELECTOR):
                        continue
                    txt = (item.inner_text() or "").strip()[:20]
                    item.click(timeout=2500, no_wait_after=True, force=True)
                    acted = True
                    logger.info(f"[清障/L1] 已点掉引导浮层 | 文案: <{txt}> | 场景: <{desc}>")
                    page.wait_for_timeout(350)
                except Exception:
                    continue
        except Exception:
            pass

    if not acted:
        try:
            close_ic = page.locator(
                "div[role='dialog'] [aria-label*='lose'],div[role='dialog'] [class*='close'],"
                "div[role='dialog'] svg[class*='close'],[class*='modal'] [class*='close']"
            ).first
            if close_ic.count() > 0 and close_ic.is_visible(timeout=300):
                close_ic.click(timeout=2000, force=True, no_wait_after=True)
                acted = True
                logger.info(f"[清障/L2] 已点击弹窗关闭图标 | 场景: <{desc}>")
                page.wait_for_timeout(250)
        except Exception:
            pass
        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(150)
        except Exception:
            pass

    if aggressive:
        try:
            killed = page.evaluate(_NUKE_OVERLAY_JS, EDITOR_GUARD_SELECTOR)
            acted = acted or bool(killed)
            logger.info(f"[清障/L3] 强制歼灭遮罩层 | 场景: <{desc}> | 数量: 【{len(killed)}】 | 明细: 【{killed}】")
        except Exception as e:
            logger.warning(f"[清障/L3] 强制歼灭失败 | 场景: <{desc}> | 原因: 【{str(e)[:120]}】")

    return acted


_GUARD_INIT_JS = r"""
(() => {
  try {
    const HINT = /guide|tip|tour|onboard|popup|first|newbie|intro|welcome|banner/i;
    const SCOPE = /square|thread|post|short|feed|bibi|social|orbit/i;
    for (const k of Object.keys(localStorage)) {
      try {
        if (HINT.test(k) && (SCOPE.test(k) || HINT.test(k))) localStorage.setItem(k, 'true');
      } catch (e) {}
    }
    ['square_thread_guide_shown','square_guide_shown','bn_square_guide','square_guide_v2',
     'shortpost_guide','square_short_post_guide','thread_scroll_guide','square_onboarding']
      .forEach(k => { try { localStorage.setItem(k, 'true'); } catch (e) {} });
  } catch (e) {}

  const RE = /^\s*(好的|好|知道了|我知道了|明白了|明白|了解|开始使用|立即体验|下一步|完成|跳过|不再提示|不再显示|以后再说|稍后|OK|Okay|Got it|I see|Understood|Skip|Next|Continue|Dismiss|Later)\s*$/i;
  const GUARD_SEL = '.ProseMirror,[contenteditable="true"],input[type="file"],textarea';

  const tick = () => {
    try {
      for (const el of [document.body, document.documentElement]) {
        const st = getComputedStyle(el);
        if (st.overflow === 'hidden' || st.overflowY === 'hidden') {
          el.style.setProperty('overflow', 'auto', 'important');
          el.style.setProperty('overflow-y', 'auto', 'important');
        }
      }
      const nodes = document.querySelectorAll(
        "button,[role='button'],div[class*='btn'],span[class*='btn'],a[class*='btn']");
      for (const n of nodes) {
        const t = (n.innerText || '').trim();
        if (!t || t.length > 12 || !RE.test(t)) continue;
        const box = n.closest("div[role='dialog'],[class*='modal'],[class*='mask'],"
                              + "[class*='guide'],[class*='popup'],[class*='tour']") || n.parentElement;
        if (box && box.querySelector(GUARD_SEL)) continue;
        const r = n.getBoundingClientRect();
        if (r.width > 0 && r.height > 0) {
          n.click();
          window.__bnGuardHits = (window.__bnGuardHits || 0) + 1;
          window.__bnGuardLast = t;
        }
      }
    } catch (e) {}
  };

  if (!window.__bnGuardTimer) {
    window.__bnGuardTimer = setInterval(tick, __GUARD_INTERVAL__);
    tick();
  }
})();
"""


def install_overlay_guard(page):
    """武装浮层免疫（在 page.goto() 之前调用）。"""
    try:
        page.add_init_script(_GUARD_INIT_JS.replace("__GUARD_INTERVAL__", str(GUARD_INTERVAL_MS)))
    except Exception as e:
        logger.warning(f"[守卫/Guard] init_script 注入失败 | 原因: 【{str(e)[:120]}】")

    trigger = page.locator(
        "div[role='dialog'],[class*='mask'],[class*='overlay'],[class*='backdrop'],[class*='guide']"
    ).filter(has_text=RE_DISMISS).first

    def _on_overlay():
        acted = _dismiss_overlays(page, aggressive=False, desc="locator_handler")
        if not acted:
            try:
                trigger.evaluate("el => el.style.display = 'none'")
            except Exception:
                pass

    try:
        page.add_locator_handler(trigger, _on_overlay, no_wait_after=True)
        logger.info("[守卫/Guard] 浮层免疫已武装 | 机制: 【预置flag + JS看门狗 + locator_handler自愈】")
        return
    except TypeError:
        pass
    except Exception as e:
        logger.warning(f"[守卫/Guard] locator_handler 不可用，退化为手动清障 | 原因: 【{str(e)[:120]}】")
        return

    try:
        page.add_locator_handler(trigger, _on_overlay)
    except Exception:
        pass


def report_guard_hits(page, stage=""):
    """读取 JS 看门狗战果。"""
    try:
        info = page.evaluate("() => ({hits: window.__bnGuardHits || 0, last: window.__bnGuardLast || ''})")
        if info and info.get("hits"):
            logger.info(
                f"[守卫/Guard] 看门狗累计自动关闭浮层 | 阶段: <{stage}> | 次数: 【{info['hits']}】 "
                f"| 最后文案: <{info['last']}>"
            )
    except Exception:
        pass


# ==============================================================================
#                        编辑器：光标控制与正文分块键入
# ==============================================================================
_CARET_TO_TAIL_JS = r"""
(element) => {
    element.focus();
    if (typeof window.getSelection !== "undefined" && typeof document.createRange !== "undefined") {
        const range = document.createRange();
        range.selectNodeContents(element);
        range.collapse(false);
        const sel = window.getSelection();
        sel.removeAllRanges();
        sel.addRange(range);
    }
    return true;
}
"""

_CARET_PROBE_JS = r"""
(element) => {
    try {
        const sel = window.getSelection();
        if (!sel || sel.rangeCount === 0) return {atHead:false, reason:'no-range'};
        const probe = document.createRange();
        probe.selectNodeContents(element);
        probe.setEnd(sel.getRangeAt(0).startContainer, sel.getRangeAt(0).startOffset);
        return {atHead: probe.toString().length === 0, offsetChars: probe.toString().length};
    } catch (e) {
        return {atHead:null, reason:String(e)};
    }
}
"""

_CARET_TO_HEAD_JS = r"""
(element) => {
    element.focus();
    if (typeof window.getSelection !== "undefined" && typeof document.createRange !== "undefined") {
        const range = document.createRange();
        range.selectNodeContents(element);
        range.collapse(true);
        const sel = window.getSelection();
        sel.removeAllRanges();
        sel.addRange(range);
    }
    return true;
}
"""


def _force_caret_to_tail(page, real_editor, tag=""):
    """把光标绝对锁定到编辑器文本的尾部。"""
    try:
        real_editor.evaluate(_CARET_TO_TAIL_JS)
    except Exception as e:
        logger.warning(f"[编辑器/光标] Range 置底执行异常，降级为键盘方案 | 场景: <{tag}> | 详情: 【{str(e)[:120]}】")

    for combo in ("Control+End", "Meta+End", "End"):
        try:
            real_editor.press(combo, timeout=2000)
            break
        except Exception:
            continue
    page.wait_for_timeout(300)


def _force_caret_to_head(page, real_editor, tag=""):
    """把光标锁定到编辑器文本的绝对头部。"""
    try:
        real_editor.evaluate(_CARET_TO_HEAD_JS)
    except Exception as e:
        logger.warning(f"[编辑器/光标] Range 置顶执行异常，降级为键盘方案 | 场景: <{tag}> | 详情: 【{str(e)[:120]}】")

    for combo in ("Control+Home", "Home"):
        try:
            real_editor.press(combo, timeout=3000)
            break
        except Exception:
            continue

    page.wait_for_timeout(300)
    try:
        verify = real_editor.evaluate(_CARET_PROBE_JS)
    except Exception:
        verify = {"atHead": "unknown"}
    logger.info(
        f"[编辑器/光标] 已锁定光标到文本头部 | 场景: <{tag}> | 校验: 【在头部={verify.get('atHead')}, "
        f"距头部字符数={verify.get('offsetChars')}】"
    )


def _type_body(page, real_editor, text):
    """键入正文：按 TYPE_CHUNK_SIZE 切分调用避免超长文本撞总超时；每块后校验是否真落字。"""
    if NEWLINE_SAFE_MODE and "\n" in text:
        lines = text.split("\n")
        for li, line in enumerate(lines):
            _type_body(page, real_editor, line)
            if li < len(lines) - 1:
                try:
                    real_editor.press("Shift+Enter", timeout=3000)
                except Exception:
                    page.keyboard.press("Shift+Enter")
        return

    for start in range(0, len(text), TYPE_CHUNK_SIZE):
        seg = text[start : start + TYPE_CHUNK_SIZE]
        try:
            before = len(real_editor.inner_text() or "")
        except Exception:
            before = -1

        try:
            real_editor.press_sequentially(seg, delay=TYPE_DELAY_MS, timeout=60000)
        except AttributeError:
            real_editor.type(seg, delay=TYPE_DELAY_MS, timeout=60000)
        except Exception as e:
            logger.warning(
                f"[编辑器/正文] 第 【{start // TYPE_CHUNK_SIZE}】 块键入抛错，转入落字校验补录 | 详情: 【{str(e)[:120]}】"
            )

        if before < 0:
            continue
        try:
            after = len(real_editor.inner_text() or "")
        except Exception:
            after = before
        if after <= before:
            logger.warning(
                f"[编辑器/正文] 第 【{start // TYPE_CHUNK_SIZE}】 块未落字，降级 insert_text 补录 | 片段长度: 【{len(seg)}】"
            )
            try:
                page.keyboard.insert_text(seg)
            except Exception:
                pass


def _normalize_text_list(text_list):
    """解析并规范化 text_list 参数，元素支持 dict 或 JSON 字符串，形如 {"text": "大家好", "delay": 5}。"""
    if not text_list or not isinstance(text_list, (list, tuple)):
        return []
    normalized = []
    for idx, item in enumerate(text_list):
        if isinstance(item, str):
            raw_s = item.strip()
            if raw_s.startswith("{") and raw_s.endswith("}"):
                try:
                    item = json.loads(raw_s)
                except Exception:
                    item = {"text": item, "delay": 0}
            else:
                item = {"text": item, "delay": 0}

        if not isinstance(item, dict):
            logger.warning(f"[编辑器/正文] text_list 第 【{idx + 1}】 项非 dict/JSON，已跳过: {item}")
            continue

        raw_text = item.get("text", "")
        text_val = "" if raw_text is None else str(raw_text)
        raw_delay = item.get("delay", 0)
        try:
            delay_val = max(0.0, float(raw_delay or 0))
        except Exception:
            delay_val = 0.0

        if text_val != "" or delay_val > 0:
            normalized.append({"text": text_val, "delay": delay_val})
    return normalized


def _type_text_list(page, real_editor, valid_text_list):
    """按 text_list 顺序逐元素键入文字，每个元素输入完成后等待其 delay 秒数。"""
    total = len(valid_text_list)
    for idx, item in enumerate(valid_text_list):
        seg_text = item.get("text", "")
        delay_sec = item.get("delay", 0.0)
        logger.info(
            f"[编辑器/正文] 正在键入 text_list 第 【{idx + 1}/{total}】 项 "
            f"| 字符数: 【{len(seg_text)}】 | 输入后等待: 【{delay_sec}s】"
        )
        if seg_text:
            _type_body(page, real_editor, seg_text)
        if delay_sec > 0:
            page.wait_for_timeout(int(delay_sec * 1000))


# ==============================================================================
#                        发送按钮解析 / 接口监听 / 持久化浏览器启动
# ==============================================================================
def _pick_trusted_send_button(cands, blacklist_re=RE_SEND_BLACKLIST):
    """按候选顺序挑选发送按钮，过滤黑名单按钮。"""
    for c in cands:
        try:
            if c.count() == 0:
                continue
            try:
                txt = (c.inner_text(timeout=1500) or "").strip()
            except Exception:
                txt = ""
            if txt and blacklist_re.search(txt):
                logger.info(f"[发送/按钮] 候选命中黑名单已跳过 | 文案: <{txt[:20]}>")
                continue
            return c
        except Exception:
            continue
    return None


class _BaseApiWatcher:
    """通用 POST 接口响应监听器，支持主接口与辅助接口正则匹配。"""

    def __init__(self, page, primary_re, secondary_re):
        self.page = page
        self.primary_re = primary_re
        self.secondary_re = secondary_re
        self.primary = []
        self.secondary = []
        self._closed = False
        page.on("response", self._on_response)

    def _on_response(self, resp):
        try:
            if resp.request.method != "POST":
                return
            url = resp.url or ""
            if self.primary_re and self.primary_re.search(url):
                self.primary.append(resp)
            elif self.secondary_re and self.secondary_re.search(url):
                self.secondary.append(resp)
        except Exception:
            pass

    def wait_primary(self, timeout_ms):
        end = time.time() + timeout_ms / 1000.0
        while time.time() < end:
            if self.primary:
                return self.primary[-1]
            self.page.wait_for_timeout(200)
        return None

    def latest_secondary(self):
        return self.secondary[-1] if self.secondary else None

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            self.page.remove_listener("response", self._on_response)
        except Exception:
            pass


def _parse_api_json(resp):
    """兼容网关错误返回 HTML 而非 JSON 的场景，恒返回 dict。"""
    try:
        raw = resp.json()
        return raw if isinstance(raw, dict) else {"raw": str(raw)}
    except Exception:
        return {}


def _alert_failure_scene(error_msg):
    """失败终局提示：响铃 + 聚合打印失败原因。"""
    sys.stdout.write("\a")
    sys.stdout.flush()
    logger.error(
        f"\n{'=' * 60}\n[中断/Halt] 🚨 任务异常终止（已保存故障现场，不阻塞等待人工）"
        f"\n[中断/Halt] 失败原因: 【{error_msg}】\n{'=' * 60}"
    )


def _launch_persistent(p, user_data_dir, args, viewport=None, hide_automation=True, debug=True, proxy=None):
    """
    统一的持久化浏览器上下文启动口：
    - 自动绑定可重入目录锁并在 context.close() 时精确释放
    - 自动兼容系统 Chrome 与内置 Chromium 回退
    - 自动授予剪贴板读写权限并恢复原生快捷键
    - 支持可选代理配置 proxy (str 或 dict)
    """
    fallback_name = Path(str(user_data_dir).replace("\\", "/")).name or "browser_default"
    user_data_dir = resolve_cross_platform_path(user_data_dir, DEFAULT_SESSION_ROOT / fallback_name)
    os.makedirs(user_data_dir, exist_ok=True)

    lock_obj = _get_user_data_lock_obj(user_data_dir)
    lock_obj.acquire(timeout=USER_DATA_LOCK_TIMEOUT_SEC)

    try:
        browser_channel = get_config("browser_channel", default="chrome")
        kwargs = {
            "user_data_dir": user_data_dir,
            "headless": not debug,
            "args": args,
        }
        if browser_channel and str(browser_channel).strip().lower() not in ("", "chromium", "none"):
            kwargs["channel"] = str(browser_channel).strip()

        if proxy:
            if isinstance(proxy, str) and proxy.strip():
                kwargs["proxy"] = {"server": proxy.strip()}
            elif isinstance(proxy, dict) and proxy.get("server"):
                kwargs["proxy"] = proxy

        if viewport:
            kwargs["viewport"] = viewport
        else:
            kwargs["no_viewport"] = True

        ignored_args = []
        if hide_automation:
            ignored_args.append("--enable-automation")

        ignored_args.extend(
            [
                "--disable-extensions",
                "--disable-default-apps",
                "--disable-component-extensions-with-background-pages",
            ]
        )
        kwargs["ignore_default_args"] = ignored_args

        try:
            context = p.chromium.launch_persistent_context(**kwargs)
        except Exception as launch_err:
            if "channel" in kwargs and (
                "distribution 'chrome' is not found" in str(launch_err).lower()
                or "executable doesn't exist" in str(launch_err).lower()
                or "chrome" in str(launch_err).lower()
            ):
                logger.warning(
                    f"[浏览器启动] 未检测到系统级 {kwargs.get('channel')}，自动回退至 Playwright 内置 Chromium: {launch_err}"
                )
                kwargs.pop("channel", None)
                context = p.chromium.launch_persistent_context(**kwargs)
            else:
                raise
    except BaseException:
        lock_obj.release()
        raise

    orig_close = context.close
    _closed_flag = {"done": False}

    def _safe_close(*c_args, **c_kwargs):
        try:
            return orig_close(*c_args, **c_kwargs)
        finally:
            if not _closed_flag["done"]:
                _closed_flag["done"] = True
                lock_obj.release()

    try:
        context.close = _safe_close
    except Exception:
        pass

    try:
        context.grant_permissions(["clipboard-read", "clipboard-write"])
    except Exception as e:
        logger.warning(f"[环境] 剪贴板权限授予失败: {e}")

    return context


def open_browser_for_manual_use(user_data_dir, home_url="https://www.binance.com/zh-CN", proxy=None):
    """启动可见浏览器交由人工自由操作（关闭浏览器窗口后自动保存会话并退出）。"""
    logger.info(f"\n{'=' * 60}\n[人工/Manual] 启动本地浏览器交接控制权 | 目录: <{user_data_dir}>\n{'=' * 60}")
    with _user_data_dir_lock(user_data_dir, timeout=USER_DATA_LOCK_TIMEOUT_SEC):
        with sync_playwright() as p:
            context = None
            try:
                context = _launch_persistent(
                    p,
                    user_data_dir,
                    args=["--disable-blink-features=AutomationControlled", "--start-maximized", "--window-position=0,0"],
                    debug=True,
                    proxy=proxy,
                )
                page = context.pages[0] if context.pages else context.new_page()
                page.bring_to_front()
                page.goto(home_url)
                logger.info("[人工/Manual] ✅ 浏览器已就绪，控制权已交接 | 🛑 退出方式: 【直接关闭浏览器窗口，程序自动结束】")
                page.wait_for_event("close", timeout=0)
            except Exception as e:
                logger.warning(f"[人工/Manual] 浏览器运行异常 | 可能原因: 【环境损坏或窗口被手动强杀: {e}】")
            finally:
                if context:
                    try:
                        context.close()
                    except Exception:
                        pass
                logger.info("[人工/Manual] 👋 窗口已关闭，控制权收回，系统资源已释放。\n")
