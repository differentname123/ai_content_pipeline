# -*- coding: utf-8 -*-
import atexit
from contextlib import contextmanager
import hashlib
import json
import os
import re
import sys
import tempfile
import threading
import time
import traceback

if os.name == "nt":
    import msvcrt
else:
    import fcntl

from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from playwright.sync_api import sync_playwright, expect, TimeoutError as PlaywrightTimeoutError

from common.common_utils import (
    setup_logger, get_config, resolve_session_dir,
    resolve_cross_platform_path, DEFAULT_SESSION_ROOT, DEFAULT_FORENSICS_DIR
)

# ==============================================================================
#                                   运行配置
# ==============================================================================
USER_DATA_DIR = resolve_session_dir("okx_ruru_browser_session_dir", "okx_ruru")
OKX_ORBIT_URL = "https://www.okx.com/zh-hans/orbit"

TYPE_CHUNK_SIZE = 80  # 正文分块长度：仅切分 press_sequentially 调用，键序与延迟不变
TYPE_DELAY_MS = 60  # 逐字键入延迟(ms)，模拟真人输入
API_WAIT_TIMEOUT_MS = 12000  # 发送后等待提交接口响应的上限
GUARD_INTERVAL_MS = 700  # 页面内 JS 看门狗轮询间隔
USER_DATA_LOCK_TIMEOUT_SEC = 300  # user_data_dir 跨进程/跨线程最大等待时间(5分钟)

# ⚠️ 换行安全模式：False(默认) —— 正文中的 \n 直接作 Enter 送出；
#    True(可选加固) —— 改用 Shift+Enter，防止"Enter=发送"导致内容截断提前发出。
NEWLINE_SAFE_MODE = False

# ==============================================================================
#                             文案正则 / 选择器常量
# ==============================================================================
RE_SEND = re.compile(r"发文|发布|发送|Post|Publish|Send", re.IGNORECASE)
RE_SEND_EXACT = re.compile(r"^(发文|发布|发送|Post|Publish|Send)$", re.IGNORECASE)

# 评论按钮文案匹配
RE_COMMENT_BTN = re.compile(r"评论|回复|发送|Comment|Reply|Send", re.IGNORECASE)
RE_COMMENT_BTN_EXACT = re.compile(r"^(评论|回复|发送|Comment|Reply|Send)$", re.IGNORECASE)

# 引导浮层"确认关闭"类按钮：刻意不含「取消/Cancel」，避免误取消业务弹窗
RE_DISMISS = re.compile(
    r"^\s*(好的|好|知道了|我知道了|明白了|明白|了解|开始使用|立即体验|马上体验|下一步|"
    r"完成|跳过|不再提示|不再显示|以后再说|稍后|关闭|"
    r"OK|Okay|Got it|Got It|I see|Understood|Skip|Next|Done|Continue|Close|Dismiss|Later|Maybe later)\s*$",
    re.IGNORECASE,
)

# 发送按钮黑名单：排除工具栏上的「文章」「更多」等非提交按钮
RE_SEND_BLACKLIST = re.compile(r"(文章|立即回复|去回复|查看|更多|展开|取消|Article|Cancel|View|More)", re.IGNORECASE)

# Cookie 同意横幅选择器
COOKIE_SELECTORS = (
    "#onetrust-accept-btn-handler",
    "#onetrust-close-btn-container button",
    "button:has-text('接受所有 Cookie')",
    "button:has-text('全部允许')",
    "button:has-text('确认我的选择')",
    "button:has-text('Accept All')",
    "button:has-text('Allow All')",
)

# 编辑器保护白名单：任何清障动作都不许碰到含这些元素的容器
EDITOR_GUARD_SELECTOR = '.ProseMirror,[contenteditable="true"],input[type="file"],textarea'

# OKX 星球发帖提交接口主判据与辅助判据
RE_SUBMIT_API_PRIMARY = re.compile(r"priapi/v5/content/ugc/publish|content/ugc/publish", re.IGNORECASE)
RE_SUBMIT_API_SECONDARY = re.compile(r"content/.*/publish|ugc/publish|orbit/.*/publish", re.IGNORECASE)

# OKX 星球评论提交接口主判据与辅助判据
RE_COMMENT_API_PRIMARY = re.compile(r"priapi/v5/content/user/comment|content/user/comment", re.IGNORECASE)
RE_COMMENT_API_SECONDARY = re.compile(r"content/.*/comment|ugc/comment|orbit/.*/comment", re.IGNORECASE)

logger = setup_logger(app_name="okx_playwright")


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
    raw = str(user_data_dir or USER_DATA_DIR).strip()
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
    """
    当本进程已合法取得 user_data_dir 独占锁后，清理上次异常强退可能遗留的
    SingletonLock / SingletonCookie / SingletonSocket 标记，防止 Chromium 假死锁。
    """
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
1.
进程内通过 RLock + 线程递归计数支持多线程排队与同线程重入；
    2. 跨进程通过 OS 内核级文件描述符锁 (msvcrt.locking / fcntl.flock) + 禁止句柄继承 (O_NOINHERIT)，
       保证无论 Ctrl+C、IDE 强行 Stop、kill -9 还是进程崩溃，内核都会立即释放锁，绝不死锁。
    """

    def __init__(self, norm_path, digest):
        self.norm_path = norm_path
        self.digest = digest
        self.rlock = threading.RLock()
        self.owner_thread_id = None
        self.depth = 0
        self.fd = None
        lock_dir = os.path.join(tempfile.gettempdir(), "okx_playwright_userdata_locks")
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

_CARET_TO_TAIL_JS = r"""
(element) => {
    element.focus();
    if (typeof window.getSelection !== "undefined" && typeof document.createRange !== "undefined") {
        const range = document.createRange();
        range.selectNodeContents(element);
        range.collapse(false); // false = 折叠到绝对尾部
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


def _snapshot_editor(editor_container, real_editor):
    """
    读取编辑器状态快照，用于发送前后比对是否清空。
    返回 (文本字符数, 媒体缩略图数)。
    """
    try:
        text_len = len(real_editor.inner_text().strip()) if real_editor.is_visible() else 0
    except Exception:
        text_len = 0
    try:
        media_cnt = editor_container.locator(
            "img[src^='blob:'],img[src^='http'],img[src^='data:'],[class*='preview'] img,[class*='thumb'] img"
        ).count()
    except Exception:
        media_cnt = 0
    return text_len, media_cnt


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
        guardHits: window.__okxGuardHits || 0,
        guardLast: window.__okxGuardLast || ''
    };
}
"""


def _forensics(page, tag, extra=None):
    """统一存证：截图 + HTML + JSON。"""
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
    return (f"{hit.get('reason')} | 遮挡者: <{hit.get('blockerTag')}> "
            f"class={hit.get('blockerClass')} text='{hit.get('blockerText')}' "
            f"覆盖率={hit.get('coverRatio')} bodyOverflow={hit.get('bodyOverflow')}")


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
        ).locator(
            "button,[role='button'],div[class*='btn'],span[class*='btn'],a[class*='btn']"
        ).filter(has_text=RE_DISMISS),
        lambda: page.locator(
            "button,[role='button'],div[class*='btn'],span[class*='btn']"
        ).filter(has_text=RE_DISMISS),
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
            logger.warning(f"[清障/L3] 强制歼灭失败，页面可能仍被浮层锁死 | 场景: <{desc}> | 原因: 【{str(e)[:120]}】")

    return acted


# ==============================================================================
#          常驻守卫：init_script 预置 flag + JS 看门狗 + locator_handler 自愈
# ==============================================================================

_GUARD_INIT_JS = r"""
(() => {
  try {
    const HINT = /guide|tip|tour|onboard|popup|first|newbie|intro|welcome|banner/i;
    const SCOPE = /orbit|okx|ugc|square|thread|post|short|feed|social/i;
    for (const k of Object.keys(localStorage)) {
      try {
        if (HINT.test(k) && (SCOPE.test(k) || HINT.test(k))) localStorage.setItem(k, 'true');
      } catch (e) {}
    }
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
          window.__okxGuardHits = (window.__okxGuardHits || 0) + 1;
          window.__okxGuardLast = t;
        }
      }
    } catch (e) {}
  };

  if (!window.__okxGuardTimer) {
    window.__okxGuardTimer = setInterval(tick, __GUARD_INTERVAL__);
    tick();
  }
})();
"""


def install_overlay_guard(page):
    """武装浮层免疫（必须在 page.goto() 之前调用）。"""
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
        logger.info("[守卫/Guard] 浮层免疫已武装（兼容模式：无 no_wait_after）")
    except Exception as e:
        logger.warning(f"[守卫/Guard] locator_handler 挂载失败，退化为手动清障 | 原因: 【{str(e)[:120]}】")


def report_guard_hits(page, stage=""):
    try:
        info = page.evaluate("() => ({hits: window.__okxGuardHits || 0, last: window.__okxGuardLast || ''})")
        if info and info.get("hits"):
            logger.info(f"[守卫/Guard] 看门狗累计自动关闭浮层 | 阶段: <{stage}> | 次数: 【{info['hits']}】 "
                        f"| 最后文案: <{info['last']}>")
    except Exception:
        pass


# ==============================================================================
#                    OKX 星球编辑器：定位 / 唤醒 / 键入 / 图片上传
# ==============================================================================

def _locate_post_creator(page):
    """
    精准锁定 OKX 星球发帖区容器（兼容 CSS Modules 动态哈希后缀）。
    目标结构：<div class="column-0BGXd"> 内含 inputArea (tiptap ProseMirror) 与 toolbar (publishBtn)
    """
    candidates = [
        page.locator('div[class*="column-"]:has(div.tiptap.ProseMirror):has(button[class*="publishBtn"])').first,
        page.locator('div:has(> div[class*="inputArea-"]):has(> div[class*="toolbar-"])').first,
        page.locator('div.tiptap.ProseMirror').first.locator(
            'xpath=ancestor::div[.//button[contains(@class, "publishBtn") or .//span[text()="发文"]]][1]'
        ),
    ]

    for idx, loc in enumerate(candidates):
        try:
            if loc.is_visible(timeout=4000):
                logger.info(f"[定位/DOM] 已锁定 OKX 星球发帖区容器 (策略 #{idx + 1})")
                return loc
        except Exception:
            continue

    logger.warning("[定位/DOM] 精确组合策略未命中，降级锁定页面首个包含 ProseMirror 的上层容器")
    fallback = page.locator('div:has(div.tiptap.ProseMirror[contenteditable="true"])').first
    fallback.wait_for(state="visible", timeout=10000)
    return fallback


def _locate_comment_box(page):
    """
    精准锁定 OKX 星球帖子详情页的评论输入区容器（不绑定 cmIdleReplyBtn 等随焦点变化的动态类名）。
    目标结构：<div class="cmInputRow-fObz9"> 内含 cmEditorWrap (tiptap ProseMirror)
    """
    candidates = [
        page.locator('div[class*="cmEditorWrap-"]').first,
        page.locator('div[class*="cmInputRow-"]').first,
        page.locator('div:has(> div[class*="cmEditorWrap-"])').first,
    ]

    for idx, loc in enumerate(candidates):
        try:
            if loc.is_visible(timeout=4000):
                logger.info(f"[定位/DOM] 已锁定 OKX 星球评论区容器 (策略 #{idx + 1})")
                return loc
        except Exception:
            continue

    try:
        page.mouse.wheel(0, 350)
        page.wait_for_timeout(600)
    except Exception:
        pass

    for idx, loc in enumerate(candidates):
        try:
            if loc.is_visible(timeout=3000):
                logger.info(f"[定位/DOM] 滚动后已锁定 OKX 星球评论区容器 (策略 #{idx + 1})")
                return loc
        except Exception:
            continue

    logger.warning("[定位/DOM] 精确评论容器策略未命中，降级锁定包含 ProseMirror 的容器")
    fallback = page.locator('div:has(div.tiptap.ProseMirror[contenteditable="true"])').first
    fallback.wait_for(state="visible", timeout=10000)
    return fallback


def _resolve_wake_target(editor_container):
    """解析点击唤醒 OKX Tiptap 编辑器的目标元素。"""
    cands = (
        editor_container.locator('div.tiptap.ProseMirror[contenteditable="true"]').first,
        editor_container.locator('p[data-placeholder*="在此处输入"], p[data-placeholder]').first,
        editor_container.locator('div[class*="editorContent-"], div[class*="inputArea-"]').first,
        editor_container.locator('div[contenteditable="true"]').first,
    )
    for c in cands:
        try:
            if c.count() > 0:
                return c
        except Exception:
            continue
    return editor_container.locator('div[contenteditable="true"]').first


def _wake_editor(page, editor_container, max_round=4):
    """
    唤醒 OKX 星球 Tiptap 富文本编辑器并确保焦点落入其中。
    返回 real_editor(Locator)。
    """
    target = _resolve_wake_target(editor_container)
    real_editor = editor_container.locator(
        'div.tiptap.ProseMirror[contenteditable="true"], div[contenteditable="true"].ProseMirror, div[contenteditable="true"]'
    ).first
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
                target.click(timeout=6000)
            elif rnd == 2:
                target.click(timeout=5000, force=True)
            elif rnd == 3:
                target.evaluate("""(el) => {
                    el.scrollIntoView({block:'center'});
                    const o = {bubbles:true, cancelable:true, view:window};
                    try { el.dispatchEvent(new PointerEvent('pointerdown', o)); } catch(e) {}
                    try { el.dispatchEvent(new MouseEvent('mousedown', o)); } catch(e) {}
                    if (el.focus) el.focus();
                    try { el.dispatchEvent(new PointerEvent('pointerup', o)); } catch(e) {}
                    try { el.dispatchEvent(new MouseEvent('mouseup', o)); } catch(e) {}
                    try { el.dispatchEvent(new MouseEvent('click', o)); } catch(e) {}
                }""")
            else:
                box = target.bounding_box()
                if box:
                    page.mouse.click(box["x"] + box["width"] / 2, box["y"] + min(box["height"] / 2, 20))
        except Exception as e:
            logger.warning(f"[编辑器/唤醒] 第{rnd}轮点击动作抛错，升级策略重试 | 详情: 【{str(e)[:150]}】")

        try:
            expect(real_editor).to_be_editable(timeout=6000 if rnd == 1 else 4000)
            focused = page.evaluate("""() => {
                const a = document.activeElement;
                return !!(a && (a.isContentEditable ||
                                (a.classList && a.classList.contains('ProseMirror')) ||
                                (a.closest && a.closest('.ProseMirror'))));
            }""")
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
    raise Exception(
        f"编辑器唤醒失败（{max_round}级降级全部失效），疑似浮层持续拦截或前端结构变更。最后命中测试: {last_hit}")


def _upload_images(page, editor_container, valid_images):
    """
    OKX 星球图片上传：
    优先点击工具栏「图片」按钮 (<button aria-label="图片">) 并通过 expect_file_chooser 注入文件；
    若页面存在直接可操作的 input[type="file"] 则作为后备通道；支持多图一次性注入与单图循环降级。
    """
    if not valid_images:
        return

    img_btn_cands = [
        editor_container.locator('button[aria-label="图片"], button[aria-label*="图片"], button[aria-label*="Image" i]').first,
        editor_container.locator('button:has(i.okx-trading-upload-general-outlined)').first,
        editor_container.locator('button[class*="toolBtn-"]:has(i[class*="upload"])').first,
    ]

    uploaded = False

    # 通道 1：点击图片按钮拦截 FileChooser
    try:
        img_btn = _interact_fallback_locators(img_btn_cands, action="wait", timeout=5000, desc="上传图片图标按钮")
        with page.expect_file_chooser(timeout=5000) as fc_info:
            _robust_click(img_btn)
        fc = fc_info.value

        if len(valid_images) > 1 and not fc.is_multiple():
            logger.info("[编辑器/图片] 检测到文件选择器不支持多选，自动切换为逐张保序上传模式")
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
        logger.info(f"[编辑器/图片] 通过【图片按钮 FileChooser】成功提交图片 | 数量: 【{len(valid_images)}】张")
    except Exception as fc_err:
        logger.warning(f"[编辑器/图片] FileChooser 通道未完成，尝试 input[type='file'] 直注兜底 | 原因: 【{str(fc_err)[:150]}】")

    # 通道 2：直接查找 DOM 内的 input[type="file"]
    if not uploaded:
        file_input = editor_container.locator('input[type="file"]').first
        if file_input.count() == 0:
            file_input = page.locator('input[type="file"]').first

        try:
            file_input.set_input_files(valid_images, timeout=10000)
            uploaded = True
            logger.info(f"[编辑器/图片] 通过【input[type=file]】批量挂载完毕 | 数量: 【{len(valid_images)}】张")
        except Exception as e:
            logger.warning(f"[编辑器/图片] 批量直注失败，进入逐张降级上传模式 | 错误: 【{str(e)[:120]}】")
            try:
                for img_path in valid_images:
                    file_input.set_input_files(img_path, timeout=5000)
                    page.wait_for_timeout(1200)
                uploaded = True
                logger.info(f"[编辑器/图片] 逐张降级上传完毕 | 数量: 【{len(valid_images)}】张")
            except Exception as sub_e:
                logger.warning(f"[编辑器/图片] 图片上传通道全部失败，自动降级为纯文本 | 错误: 【{str(sub_e)[:150]}】")
                return

    # 等待缩略图渲染及上传状态稳定
    try:
        expect(editor_container.locator(
            "img[src^='blob:'],img[src^='http'],img[src^='data:'],[class*='thumb'] img,[class*='preview'] img"
        ).first).to_be_visible(timeout=12000)
    except Exception:
        pass
    page.wait_for_timeout(2000)


def _type_body(page, real_editor, text):
    """
    键入正文：按 TYPE_CHUNK_SIZE 切分调用避免超长文本撞总超时；
    每块后校验是否真落字，未落字用 keyboard.insert_text 补录。
    """
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
        seg = text[start:start + TYPE_CHUNK_SIZE]
        try:
            before = len(real_editor.inner_text() or "")
        except Exception:
            before = -1

        try:
            real_editor.press_sequentially(seg, delay=TYPE_DELAY_MS, timeout=60000)
        except AttributeError:
            real_editor.type(seg, delay=TYPE_DELAY_MS, timeout=60000)
        except Exception as e:
            logger.warning(f"[编辑器/正文] 第 【{start // TYPE_CHUNK_SIZE}】 块键入抛错，转入落字校验补录 "
                           f"| 详情: 【{str(e)[:120]}】")

        if before < 0:
            continue
        try:
            after = len(real_editor.inner_text() or "")
        except Exception:
            after = before
        if after <= before:
            logger.warning(f"[编辑器/正文] 第 【{start // TYPE_CHUNK_SIZE}】 块未落字，降级 insert_text 补录 "
                           f"| 片段长度: 【{len(seg)}】")
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
#                        发送：按钮解析 / 接口监听 / 结果判定
# ==============================================================================

def _pick_trusted_send_button(cands):
    """按候选顺序挑选「发文」按钮，过滤掉「文章」「更多」等黑名单按钮。"""
    for c in cands:
        try:
            if c.count() == 0:
                continue
            try:
                txt = (c.inner_text(timeout=1500) or "").strip()
            except Exception:
                txt = ""
            if txt and RE_SEND_BLACKLIST.search(txt):
                logger.info(f"[发送/按钮] 候选命中黑名单已跳过 | 文案: <{txt[:20]}>")
                continue
            return c
        except Exception:
            continue
    return None


class _ApiWatcher:
    """点击前挂 response 监听、点击后轮询取 OKX 发帖接口结果。"""

    def __init__(self, page):
        self.page = page
        self.primary = []
        self.secondary = []
        self._closed = False
        page.on("response", self._on_response)

    def _on_response(self, resp):
        try:
            if resp.request.method != "POST":
                return
            url = resp.url or ""
            if RE_SUBMIT_API_PRIMARY.search(url):
                self.primary.append(resp)
            elif RE_SUBMIT_API_SECONDARY.search(url):
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


def _read_api_verdict(page, watcher):
    """
    解读 OKX 星球发帖结果。
    主判据：POST /priapi/v5/content/ugc/publish 且 code == "0"，提取 data.contentId。
    """
    resp = watcher.wait_primary(API_WAIT_TIMEOUT_MS)
    if resp is not None:
        body = _parse_api_json(resp)
        code_str = str(body.get("code", ""))
        if code_str in ("0", "000000") or body.get("success") is True:
            data = body.get("data")
            cid = (data.get("contentId") or data.get("id")) if isinstance(data, dict) else None
            logger.info(f"[发送/校验] OKX 主接口确认发文成功 | 响应码: 【{code_str}】 | 帖子ID(contentId): 【{cid}】 | 结果: [Success]")
            return True, cid
        if body:
            err = body.get("msg") or body.get("message") or body.get("error_message") or "未知业务拦截"
            logger.error(f"[发送/校验] 服务端业务规则拒收本条帖子 | HTTP: 【{resp.status}】 | code: 【{code_str}】 | 原因: 【{err}】")
            _forensics(page, "biz_reject", {"url": resp.url, "status": resp.status, "body": body})
            raise BusinessErrorException(f"业务发送被服务器拦截 (code={code_str})，原因: {err}")
        logger.warning(f"[发送/校验] 主接口响应非 JSON(HTTP 【{resp.status}】)，转入 DOM 兜底校验")
        return False, None

    sec = watcher.latest_secondary()
    if sec is None:
        logger.warning(f"[发送/校验] 【{API_WAIT_TIMEOUT_MS}ms】 内未捕获到提交接口，转入 DOM 兜底校验")
        return False, None

    body = _parse_api_json(sec)
    code_str = str(body.get("code", ""))
    logger.info(f"[发送/校验] 观测到疑似发文接口（辅助判据） | URL: <{sec.url}> | HTTP: 【{sec.status}】 | body: 【{str(body)[:200]}】")
    if code_str in ("0", "000000") or body.get("success") is True:
        data = body.get("data")
        cid = (data.get("contentId") or data.get("id")) if isinstance(data, dict) else None
        logger.info(f"[发送/校验] 辅助接口判定发文成功 | 帖子ID: 【{cid}】 | 结果: [Success]")
        return True, cid
    return False, None


class _CommentApiWatcher:
    """点击前挂 response 监听、点击后轮询取 OKX 评论接口结果。"""

    def __init__(self, page):
        self.page = page
        self.primary = []
        self.secondary = []
        self._closed = False
        page.on("response", self._on_response)

    def _on_response(self, resp):
        try:
            if resp.request.method != "POST":
                return
            url = resp.url or ""
            if RE_COMMENT_API_PRIMARY.search(url):
                self.primary.append(resp)
            elif RE_COMMENT_API_SECONDARY.search(url):
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


def _read_comment_api_verdict(page, watcher):
    """
    解读 OKX 星球评论结果。
    主判据：POST /priapi/v5/content/user/comment 且 code == "0"，提取 data.commentId（兜底 id / contentId）。
    """
    resp = watcher.wait_primary(API_WAIT_TIMEOUT_MS)
    if resp is not None:
        body = _parse_api_json(resp)
        code_str = str(body.get("code", ""))
        if code_str in ("0", "000000") or body.get("success") is True:
            data = body.get("data")
            comment_id = (
                (data.get("commentId") or data.get("id") or data.get("contentId"))
                if isinstance(data, dict) else None
            )
            logger.info(
                f"[评论/校验] OKX 主接口确认评论成功 | 响应码: 【{code_str}】 "
                f"| 评论ID(commentId): 【{comment_id}】 | 结果: [Success]"
            )
            return True, comment_id
        if body:
            err = body.get("msg") or body.get("message") or body.get("error_message") or "未知业务拦截"
            logger.error(f"[评论/校验] 服务端业务规则拒收本条评论 | HTTP: 【{resp.status}】 | code: 【{code_str}】 | 原因: 【{err}】")
            _forensics(page, "comment_biz_reject", {"url": resp.url, "status": resp.status, "body": body})
            raise BusinessErrorException(f"业务评论被服务器拦截 (code={code_str})，原因: {err}")
        logger.warning(f"[评论/校验] 主接口响应非 JSON(HTTP 【{resp.status}】)，转入 DOM 兜底校验")
        return False, None

    sec = watcher.latest_secondary()
    if sec is None:
        logger.warning(f"[评论/校验] 【{API_WAIT_TIMEOUT_MS}ms】 内未捕获到评论提交接口，转入 DOM 兜底校验")
        return False, None

    body = _parse_api_json(sec)
    code_str = str(body.get("code", ""))
    logger.info(f"[评论/校验] 观测到疑似评论接口（辅助判据） | URL: <{sec.url}> | HTTP: 【{sec.status}】 | body: 【{str(body)[:200]}】")
    if code_str in ("0", "000000") or body.get("success") is True:
        data = body.get("data")
        comment_id = (
            (data.get("commentId") or data.get("id") or data.get("contentId"))
            if isinstance(data, dict) else None
        )
        logger.info(f"[评论/校验] 辅助接口判定评论成功 | 评论ID: 【{comment_id}】 | 结果: [Success]")
        return True, comment_id
    return False, None


# ==============================================================================
#                              核心：提交一条 OKX 星球帖子
# ==============================================================================


# ==============================================================================
#                              核心：提交一条 OKX 星球评论
# ==============================================================================

def _submit_comment(page, comment_container, content=""):
    """执行：点击选中评论框 → 直接键入评论文本 → 定位激活态「评论」按钮点击并校验响应。"""
    content = str(content) if content else ""
    if not content.strip():
        raise Exception("评论内容不能为空")

    _dismiss_overlays(page, aggressive=False, desc="pre-comment-submit")
    report_guard_hits(page, "pre-comment-submit")

    # 直接定位评论框内的 ProseMirror 编辑器
    real_editor = page.locator(
        'div[class*="cmEditorWrap-"] div.tiptap.ProseMirror, '
        'div[class*="editorContent-"] div.tiptap.ProseMirror, '
        'div.tiptap.ProseMirror[contenteditable="true"]'
    ).first

    try:
        real_editor.scroll_into_view_if_needed(timeout=3000)
    except Exception:
        pass
    page.wait_for_timeout(200)

    # 1. 点击选中评论输入框，立即直接打字
    _robust_click(real_editor)
    page.wait_for_timeout(300)

    logger.info(f"[评论/正文] 已选中评论输入框，直接开始键入评论 | 字符数: 【{len(content)}】")
    _type_body(page, real_editor, content)
    page.wait_for_timeout(300)

    try:
        current_text = (real_editor.inner_text() or "").strip()
    except Exception:
        current_text = ""

    if not current_text:
        logger.warning("[评论/正文] 首次键入未检测到文本，直接使用 keyboard.insert_text 补录")
        try:
            _robust_click(real_editor)
            page.wait_for_timeout(150)
            page.keyboard.insert_text(content)
            page.wait_for_timeout(300)
        except Exception as ins_e:
            logger.warning(f"[评论/正文] insert_text 补录异常: {str(ins_e)[:100]}")

    logger.info("[评论/正文] 评论输入完成 | 结果: [Success]")

    # 2. 定位激活态「评论」按钮：
    # <button type="button" data-testid="okd-button" data-e2e-okd-button-loading="false"
    #         class="okui-primarybutton-var okui-primarybutton okui-primarybutton-sm">评论</button>
    # 注意：输入框激活后外层 cmInputRow- 与按钮 cmIdleReplyBtn- 均已移除，因此直接在 page 上定位
    comment_btn_cands = [
        page.locator('button[data-testid="okd-button"].okui-primarybutton').filter(has_text=RE_COMMENT_BTN_EXACT).first,
        page.locator('button[data-testid="okd-button"]').filter(has_text=RE_COMMENT_BTN_EXACT).first,
        page.get_by_role("button", name="评论", exact=True).first,
        page.locator('button.okui-primarybutton').filter(has_text=RE_COMMENT_BTN_EXACT).first,
        page.locator("button").filter(has_text=RE_COMMENT_BTN_EXACT).first,
    ]

    comment_button = None
    try:
        comment_button = _interact_fallback_locators(
            comment_btn_cands, action="wait", timeout=5000, desc="评论按钮"
        )
        expect(comment_button).to_be_enabled(timeout=5000)
    except PlaywrightTimeoutError:
        logger.warning("[评论/按钮] 评论按钮在 5s 内未变为 enabled，尝试直接强制点击")
    except Exception as e:
        logger.warning(f"[评论/按钮] 发生意外的定位错误: {str(e)[:150]}")
        comment_button = _pick_trusted_send_button(comment_btn_cands) or comment_btn_cands[0]

    try:
        text_before = len((real_editor.inner_text() or "").strip())
    except Exception:
        text_before = len(content)
    api_success, comment_id = False, None

    logger.info(f"[评论/提交] 触发「评论」并挂载接口监听 | 主判据: <priapi/v5/content/user/comment> "
                f"| 发送前评论框: 【文本{text_before}字】")
    watcher = _CommentApiWatcher(page)
    try:
        clicked = False
        for attempt in range(3):
            try:
                if attempt == 0:
                    _robust_click(comment_button)
                elif attempt == 1:
                    comment_button.click(timeout=3000, force=True)
                else:
                    comment_button.evaluate("el => el.click()")
                clicked = True
                break
            except Exception as ce:
                logger.warning(f"[评论/提交] 第【{attempt + 1}】次点击失败，升级点击方式重试 | 详情: 【{str(ce)[:150]}】")

        if not clicked:
            _forensics(page, "comment_click_fail", {"hit": _hit_test(page, comment_button)})
            raise Exception("评论按钮点击 3 级降级全部失败（疑似被浮层持续拦截或按钮已失效）。")

        api_success, comment_id = _read_comment_api_verdict(page, watcher)
    except PlaywrightTimeoutError as e:
        logger.warning(f"[评论/校验] 等待接口响应超时，转入 DOM 兜底校验 | 详情: 【{str(e)[:120]}】")
    finally:
        watcher.close()

    if api_success:
        return comment_id

    page.wait_for_timeout(3000)
    try:
        text_after = len((real_editor.inner_text() or "").strip())
    except Exception:
        text_after = 0
    if text_before > 0 and text_after < text_before / 3:
        logger.info(f"[评论/校验] DOM 兜底比对通过（评论框已被清空） | 文本: 【{text_before}→{text_after}】 | 结果: [Success]")
        return comment_id

    report_guard_hits(page, "comment-send-fail")
    _forensics(page, "comment_send_no_effect", {
        "text_before": text_before,
        "text_after": text_after,
        "comment_btn_hit_test": _hit_test(page, comment_button),
    })
    raise Exception(f"评论已点击但评论框未清空且接口无成功响应（文本 {text_before}→{text_after}），"
                    f"疑似按钮失效、内容被前端校验拦下或网络堵塞。")


# ==============================================================================
#                              主控辅助与入口函数
# ==============================================================================

def _alert_failure_scene(error_msg):
    """失败终局提示：响铃 + 聚合打印失败原因。"""
    sys.stdout.write("\a")
    sys.stdout.flush()
    logger.error(f"\n{'=' * 60}\n[中断/Halt] 🚨 任务异常终止（已保存故障现场，不阻塞等待人工）"
                 f"\n[中断/Halt] 失败原因: 【{error_msg}】\n{'=' * 60}")


def _launch_persistent(p, user_data_dir, args, viewport=None, hide_automation=True, debug=True):
    """统一的持久化上下文启动口，底层绑定可重入目录锁。"""
    fallback_name = Path(str(user_data_dir).replace("\\", "/")).name or "okx_default"
    user_data_dir = resolve_cross_platform_path(user_data_dir, DEFAULT_SESSION_ROOT / fallback_name)
    os.makedirs(user_data_dir, exist_ok=True)

    lock_obj = _get_user_data_lock_obj(user_data_dir)
    lock_obj.acquire(timeout=USER_DATA_LOCK_TIMEOUT_SEC)

    try:
        browser_channel = get_config("browser_channel", default="chrome")
        kwargs = {
            "user_data_dir": user_data_dir,
            "headless": not debug,
            "args": args
        }
        if browser_channel and str(browser_channel).strip().lower() not in ("", "chromium", "none"):
            kwargs["channel"] = str(browser_channel).strip()

        if viewport:
            kwargs["viewport"] = viewport
        else:
            kwargs["no_viewport"] = True

        ignored_args = []
        if hide_automation:
            ignored_args.append("--enable-automation")

        ignored_args.extend([
            "--disable-extensions",
            "--disable-default-apps",
            "--disable-component-extensions-with-background-pages"
        ])
        kwargs["ignore_default_args"] = ignored_args

        try:
            context = p.chromium.launch_persistent_context(**kwargs)
        except Exception as launch_err:
            if "channel" in kwargs and ("distribution 'chrome' is not found" in str(launch_err).lower() or "executable doesn't exist" in str(launch_err).lower() or "chrome" in str(launch_err).lower()):
                logger.warning(f"[浏览器启动] 未检测到系统级 {kwargs.get('channel')}，自动回退至 Playwright 内置 Chromium: {launch_err}")
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
        context.grant_permissions(['clipboard-read', 'clipboard-write'])
    except Exception as e:
        logger.warning(f"[环境] 剪贴板权限授予失败: {e}")

    return context


def _add_hot_topic_tag(page, editor_container, real_editor):
    """
    在正文输入完成后追加默认热门标签：
    将光标锁定至编辑器末尾 → 输入 "\n\n#" 触发标签联想弹窗 → 等待 0.5 秒 → 按下 Enter 键选中首个热门标签。
    """
    logger.info("[编辑器/标签] 准备通过输入 '#' + Enter 添加热门话题标签...")
    try:
        _robust_click(real_editor)
        _force_caret_to_tail(page, real_editor, tag="before-hot-tag")
        try:
            real_editor.press_sequentially("\n\n#", delay=TYPE_DELAY_MS, timeout=5000)
        except AttributeError:
            real_editor.type("\n\n#", delay=TYPE_DELAY_MS, timeout=5000)
        except Exception:
            page.keyboard.type("\n\n#", delay=TYPE_DELAY_MS)

        # 等待 0.5 秒让热门标签联想列表弹出并默认选中第一项
        page.wait_for_timeout(500)

        # 按下 Enter 键确认选中第一个热门标签
        try:
            real_editor.press("Enter", timeout=3000)
        except Exception:
            page.keyboard.press("Enter")

        page.wait_for_timeout(350)
        logger.info("[编辑器/标签] 默认热门标签('# + Enter')添加完成 | 结果: [Success]")
    except Exception as e:
        logger.warning(f"[编辑器/标签] 添加热门标签失败，忽略并继续主流程 | 原因: 【{str(e)[:150]}】")


def _is_futures_panel_open(page):
    """检测仓位分享弹窗 (<section class="productPanel-..." aria-label="futures">) 是否已真正打开可见。"""
    try:
        return bool(
            page.evaluate(
                """() => {
                    const el = document.querySelector('section[aria-label="futures"], section[class*="productPanel-"], div[class*="positionList-"]');
                    if (!el) return false;
                    const r = el.getBoundingClientRect();
                    const st = window.getComputedStyle(el);
                    return r.width > 10 && r.height > 10 &&
                           st.display !== 'none' && st.visibility !== 'hidden' &&
                           parseFloat(st.opacity || '1') > 0.1;
                }"""
            )
        )
    except Exception:
        return False


def _add_position_share(page, editor_container):
    """
    点击仓位分享按钮 (<i class="icon iconfont okx-trading-line-chart">)，
    连续点击确保弹窗 (<section class="productPanel-..." aria-label="futures">) 打开，
    随后选择第一个仓位 (<button class="selectableRow-...">)，最后点击「确认」按钮 (<button class="... orbitConfirmBtn-...">)。
    """
    logger.info("[编辑器/仓位] 准备添加仓位分享...")
    pos_btn_cands = [
        editor_container.locator('button:has(i.okx-trading-line-chart)').first,
        editor_container.locator('i.okx-trading-line-chart').first,
        page.locator('button:has(i.okx-trading-line-chart)').first,
        page.locator('i.okx-trading-line-chart').first,
    ]

    try:
        pos_btn = _interact_fallback_locators(
            pos_btn_cands, action="wait", timeout=5000, desc="仓位分享按钮(okx-trading-line-chart)"
        )
        try:
            pos_btn.scroll_into_view_if_needed(timeout=2000)
            page.wait_for_timeout(150)
        except Exception:
            pass

        box = pos_btn.bounding_box()

        # 第 1 次点击仓位分享按钮
        if box and box["width"] > 0 and box["height"] > 0:
            px = box["x"] + box["width"] / 2
            py = box["y"] + box["height"] / 2
            page.mouse.move(px, py)
            page.mouse.click(px, py, delay=50)
        else:
            _robust_click(pos_btn)
        logger.info("[编辑器/仓位] 已执行仓位分享按钮第 1 次点击")

        page.wait_for_timeout(500)

        # 第 2 次点击仓位分享按钮（若第 1 次未弹出面板，立即补点第 2 次）
        if not _is_futures_panel_open(page):
            logger.info("[编辑器/仓位] 仓位弹窗尚未打开，执行第 2 次连续点击...")
            box2 = pos_btn.bounding_box() or box
            if box2 and box2["width"] > 0 and box2["height"] > 0:
                px2 = box2["x"] + box2["width"] / 2
                py2 = box2["y"] + box2["height"] / 2
                page.mouse.move(px2, py2)
                page.mouse.click(px2, py2, delay=60)
            else:
                _robust_click(pos_btn)
            page.wait_for_timeout(500)

        # 若仍未打开，使用 JS 原生穿透点击兜底
        if not _is_futures_panel_open(page):
            logger.info("[编辑器/仓位] 触发 JS 原生穿透点击仓位按钮兜底...")
            pos_btn.evaluate("el => (el.closest('button,[role=\"button\"]') || el).click()")
            page.wait_for_timeout(500)

        # 1. 定位并点击弹窗中的第一个可选仓位按钮 (<button type="button" class="selectableRow-QNxjW">)
        first_position_cands = [
            page.locator('section[aria-label="futures"] div[class*="positionList-"] button[class*="selectableRow-"]').first,
            page.locator('section[class*="productPanel-"] div[class*="positionList-"] button[class*="selectableRow-"]').first,
            page.locator('div[class*="positionList-"] button[class*="selectableRow-"]').first,
            page.locator('section[aria-label="futures"] button[class*="selectableRow-"]').first,
        ]

        first_pos_btn = _interact_fallback_locators(
            first_position_cands, action="wait", timeout=6000, desc="弹窗内第一个持仓项(selectableRow)"
        )
        _robust_click(first_pos_btn)
        logger.info("[编辑器/仓位] 已选中第一个仓位项，准备点击「确认」按钮...")
        page.wait_for_timeout(300)

        # 2. 定位并点击弹窗底部的「确认」按钮 (<button data-testid="okd-button" ... class="... orbitConfirmBtn-qDfs2"><span class="btn-content">确认</span></button>)
        re_confirm_text = re.compile(r"^\s*(确认|确定|Confirm|OK)\s*$", re.IGNORECASE)
        confirm_btn_cands = [
            page.locator('button[class*="orbitConfirmBtn-"]').first,
            page.locator('button[data-testid="okd-button"][class*="orbitConfirmBtn"]').first,
            page.locator('button[data-testid="okd-button"].btn-fill-primary').filter(has_text=re_confirm_text).first,
            page.locator('button.okui-btn.btn-fill-primary').filter(has_text=re_confirm_text).first,
            page.get_by_role("button", name=re_confirm_text).first,
        ]

        confirm_btn = _interact_fallback_locators(
            confirm_btn_cands, action="wait", timeout=5000, desc="仓位分享「确认」按钮(orbitConfirmBtn)"
        )
        try:
            expect(confirm_btn).to_be_enabled(timeout=3000)
        except Exception:
            pass

        _robust_click(confirm_btn)
        page.wait_for_timeout(500)
        logger.info("[编辑器/仓位] 已点击「确认」按钮，仓位分享挂载完成 | 结果: [Success]")
    except Exception as e:
        logger.warning(f"[编辑器/仓位] 添加仓位分享失败（可能当前无持仓或弹窗超时），忽略并继续 | 原因: 【{str(e)[:150]}】")


def _submit_post(
    page,
    editor_container,
    content="",
    image_path_list=None,
    text_list=None,
    add_hot_tag=True,
    add_position=True,
):
    """
    在发帖容器作用域内执行：
    唤醒编辑器 → 键入文本 → (可选)增加热门标签 → (可选)增加仓位分享 → (可选)上传图片 → 点击「发文」并校验响应。
    """
    content = str(content) if content else ""
    valid_text_list = _normalize_text_list(text_list)

    _dismiss_overlays(page, aggressive=False, desc="pre-submit")
    report_guard_hits(page, "pre-submit")

    real_editor = _wake_editor(page, editor_container)

    # 1. 输入正文内容
    if valid_text_list or content.strip():
        _force_caret_to_tail(page, real_editor, tag="before-body")
        if valid_text_list:
            total_chars = sum(len(x.get("text", "")) for x in valid_text_list)
            logger.info(
                f"[编辑器/正文] 使用 text_list 键入正文 | 列表项数: 【{len(valid_text_list)}】 "
                f"| 总字符数: 【{total_chars}】 | 分块: 【{TYPE_CHUNK_SIZE}】"
            )
            page.wait_for_timeout(500)
            _type_text_list(page, real_editor, valid_text_list)
            page.wait_for_timeout(400)
        else:
            logger.info(f"[编辑器/正文] 开始键入正文 | 字符数: 【{len(content)}】 | 分块: 【{TYPE_CHUNK_SIZE}】")
            page.wait_for_timeout(500)
            _type_body(page, real_editor, content)
            page.wait_for_timeout(400)

        try:
            current_text = (real_editor.inner_text() or "").strip()
        except Exception:
            current_text = ""

        expect_non_empty = (
            any(x.get("text", "").strip() for x in valid_text_list)
            if valid_text_list else bool(content.strip())
        )
        if not current_text and expect_non_empty:
            logger.warning("[编辑器/正文] 正文被静默清空，触发一次全量补录")
            _dismiss_overlays(page, aggressive=True, desc="text-retry")
            try:
                expect(real_editor).to_be_editable(timeout=4000)
            except Exception:
                real_editor = _wake_editor(page, editor_container, max_round=2)
            _force_caret_to_tail(page, real_editor, tag="text-retry")
            if valid_text_list:
                _type_text_list(page, real_editor, valid_text_list)
            else:
                _type_body(page, real_editor, content)
            page.wait_for_timeout(400)

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

    # 2. 打完文字后，在末尾输入 "\n\n#" 并在 0.5s 后按 Enter 增加热门标签（若开启）
    if add_hot_tag:
        _add_hot_topic_tag(page, editor_container, real_editor)

    # 3. 增加仓位分享，选择第一个仓位并点击确认（若开启）
    if add_position:
        _add_position_share(page, editor_container)

    # 4. 挂载图片（如果提供）
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
        _dismiss_overlays(page, aggressive=False, desc="pre-image")
        _upload_images(page, editor_container, valid_images)

    # 5. 定位并点击「发文」按钮 (<button type="button" class="publishBtn-qrlWy">)
    _dismiss_overlays(page, aggressive=False, desc="pre-send")
    send_btn_cands = [
        editor_container.locator('button[class*="publishBtn"]').first,
        editor_container.locator('button:has(span[class*="publishBtnText"])').first,
        editor_container.locator("button").filter(has_text=RE_SEND_EXACT).first,
        editor_container.get_by_role("button", name=RE_SEND).first,
    ]

    send_button = None
    try:
        send_button = _pick_trusted_send_button(send_btn_cands) or _interact_fallback_locators(
            send_btn_cands, action="wait", timeout=5000, desc="发文按钮"
        )
        expect(send_button).to_be_enabled(timeout=10000)
    except PlaywrightTimeoutError:
        _forensics(page, "btn_disabled_fatal", {
            "btn_html": send_button.evaluate("el => el.outerHTML") if send_button else "none",
            "editor_text": real_editor.inner_text() if real_editor else "none"
        })
        raise Exception(
            "「发文」按钮被找到，但持续处于禁用(Disabled)状态（可能正文为空、字数超500上限或图片仍在上传中）。"
        )
    except Exception as e:
        logger.warning(f"[发送/按钮] 发生意外的定位错误: {str(e)[:150]}")
        send_button = send_btn_cands[0]

    text_before, media_before = _snapshot_editor(editor_container, real_editor)
    api_success, content_id = False, None

    logger.info(f"[发送/提交] 触发「发文」并挂载接口监听 | 主判据: <priapi/v5/content/ugc/publish> "
                f"| 发送前编辑器: 【文本{text_before}字 / 媒体{media_before}个】")
    watcher = _ApiWatcher(page)
    try:
        clicked = False
        for attempt in range(3):
            hit = _hit_test(page, send_button)
            if not hit.get("ok"):
                logger.warning(
                    f"[发送/提交] 发文按钮不可命中，先清障再重试 | 第【{attempt + 1}】次 | 详情: 【{_fmt_hit(hit)}】")
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
            raise Exception("发文按钮点击 3 级降级全部失败（疑似被浮层持续拦截或按钮已失效）。")

        api_success, content_id = _read_api_verdict(page, watcher)
    except PlaywrightTimeoutError as e:
        logger.warning(f"[发送/校验] 等待接口响应超时，转入 DOM 兜底校验 | 详情: 【{str(e)[:120]}】")
    finally:
        watcher.close()

    if api_success:
        return content_id

    page.wait_for_timeout(3000)
    text_after, media_after = _snapshot_editor(editor_container, real_editor)
    if (text_before > 0 and text_after < text_before / 3) or (media_before > 0 and media_after < media_before):
        logger.info(f"[发送/校验] DOM 兜底比对通过（输入框已被清空） | 文本: 【{text_before}→{text_after}】 "
                    f"| 媒体: 【{media_before}→{media_after}】 | 结果: [Success]")
        return content_id

    report_guard_hits(page, "send-fail")
    _forensics(page, "send_no_effect", {
        "text_before": text_before, "text_after": text_after,
        "media_before": media_before, "media_after": media_after,
        "send_btn_hit_test": _hit_test(page, send_button),
    })
    raise Exception(f"发文已点击但输入框未清空且接口无成功响应（文本 {text_before}→{text_after}，"
                    f"媒体 {media_before}→{media_after}），疑似按钮失效、内容被前端校验拦下或网络堵塞。")


def create_okx_post(
    content="",
    image_path_list=None,
    user_data_dir=USER_DATA_DIR,
    debug=True,
    text_list=None,
    add_hot_tag=False,
    add_position=False,
):
    """
    主控入口 1：调度浏览器打开 OKX 星球主页并执行【纯文本 / 热门标签 / 仓位分享 / 带图片发帖】全流程。
    :param add_hot_tag: 是否在文字末尾增加默认热门标签，默认 True
    :param add_position: 是否增加仓位分享（默认选第一个持仓），默认 True
    返回 Tuple(错误信息(str|None), 是否成功(bool), 帖子ID(str|None))。
    """
    if not os.path.isdir(user_data_dir):
        return f"缺少用户环境: {user_data_dir}，请先执行登录", False, None

    valid_text_list = _normalize_text_list(text_list)
    text_summary = (
        f"text_list({len(valid_text_list)}项/{sum(len(x['text']) for x in valid_text_list)}字)"
        if valid_text_list else f"{len(str(content or ''))}字"
    )
    img_count = len(image_path_list) if isinstance(image_path_list, list) else (1 if image_path_list else 0)

    logger.info(f"\n{'=' * 70}\n[任务/Main] 启动 OKX 星球自动化发帖 | URL: <{OKX_ORBIT_URL}> "
                f"| 正文: 【{text_summary}】 | 图片数量: 【{img_count}】张 "
                f"| 热门标签: 【{add_hot_tag}】 | 仓位分享: 【{add_position}】 "
                f"| 模式: 【{'debug可见' if debug else '离屏后台'}】"
                f"\n{'=' * 70}")

    anti_freeze_args = ['--disable-restore-session-state', '--no-default-browser-check']
    offscreen_args = [
        '--disable-blink-features=AutomationControlled', '--disable-gpu',
        '--window-position=-10000,-10000', '--no-sandbox', '--disable-dev-shm-usage',
        '--disable-renderer-backgrounding', '--disable-background-timer-throttling',
        '--disable-backgrounding-occluded-windows', '--disable-features=CalculateNativeWinOcclusion',
        '--disable-breakpad', '--force-device-scale-factor=1', '--hide-scrollbars',
    ] + anti_freeze_args

    debug_args = [
        '--disable-blink-features=AutomationControlled', '--start-maximized', '--window-position=0,0'
    ] + anti_freeze_args

    try:
        with _user_data_dir_lock(user_data_dir, timeout=USER_DATA_LOCK_TIMEOUT_SEC):
            with sync_playwright() as p:
                context = None
                try:
                    context = _launch_persistent(
                        p, user_data_dir,
                        args=debug_args if debug else offscreen_args,
                        viewport=None if debug else {'width': 1920, 'height': 1080},
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

                    response = page.goto(OKX_ORBIT_URL, timeout=60000, wait_until="domcontentloaded")
                    _dismiss_overlays(page, aggressive=False, desc="orbit-nav")
                    report_guard_hits(page, "orbit-nav")

                    if response and response.status >= 400:
                        return f"OKX 星球主页加载异常 (HTTP {response.status})", False, None

                    try:
                        page.locator("a[href*='/account/login']").first.wait_for(state="visible", timeout=3000)
                        return "页面探测到登录入口，本地 Cookie 可能已过期失效，请先重新登录。", False, None
                    except PlaywrightTimeoutError:
                        pass

                    check_for_crash(page)

                    try:
                        editor_container = _locate_post_creator(page)
                    except Exception as e:
                        return f"无法定位 OKX 星球发帖区: {e}", False, None

                    post_id = _submit_post(
                        page, editor_container,
                        content=content,
                        image_path_list=image_path_list,
                        text_list=text_list,
                        add_hot_tag=add_hot_tag,
                        add_position=add_position,
                    )

                    logger.info(f"[任务/Main] OKX 星球帖子发布成功 | 帖子ID(contentId): 【{post_id}】 | 结果: [Success]")
                    return None, True, post_id

                except BusinessErrorException as biz_e:
                    error_info = f"[业务拦截] {biz_e}"
                    logger.error(f"[任务/Main] 发帖被服务端业务规则阻断 | 原因: 【{error_info}】")
                    return error_info, False, None

                except Exception as e:
                    error_info = f"[{type(e).__name__}] {e}\n[Traceback]:\n{traceback.format_exc()}"
                    if context and context.pages:
                        try:
                            _forensics(context.pages[0], "post_error", {"url": OKX_ORBIT_URL, "error": error_info[:2000]})
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

def comment_on_okx_post(target_post_url, content="", like=False, user_data_dir=USER_DATA_DIR, debug=True):
    """
    主控入口 3：调度浏览器打开指定 OKX 星球帖子详情页并执行【点赞(可选) + 纯文本评论(可选)】全流程。
    :param target_post_url: 目标帖子详情页 URL
    :param content: 评论文本内容（若为空且 like=True，则仅执行点赞）
    :param like: 是否需要点赞，默认 False
    :param user_data_dir: 浏览器用户数据目录
    :param debug: 是否开启调试模式（True 显示界面，False 离屏后台）
    返回 Tuple(错误信息(str|None), 是否成功(bool), 评论ID(str|None))，格式与 create_okx_post 保持一致。
    """
    target_post_url = str(target_post_url or "").strip()
    if not target_post_url:
        return "缺少目标帖子链接: target_post_url 不能为空", False, None

    content = str(content or "").strip()
    if not content and not like:
        return "评论内容为空且未开启点赞，无需执行互动", False, None

    if not os.path.isdir(user_data_dir):
        return f"缺少用户环境: {user_data_dir}，请先执行登录", False, None

    logger.info(f"\n{'=' * 70}\n[任务/Comment] 启动 OKX 星球自动化互动 | 目标帖子: <{target_post_url}> "
                f"| 评论字数: 【{len(content)}字】 | 是否点赞: 【{like}】 | 模式: 【{'debug可见' if debug else '离屏后台'}】"
                f"\n{'=' * 70}")

    anti_freeze_args = ['--disable-restore-session-state', '--no-default-browser-check']
    offscreen_args = [
        '--disable-blink-features=AutomationControlled', '--disable-gpu',
        '--window-position=-10000,-10000', '--no-sandbox', '--disable-dev-shm-usage',
        '--disable-renderer-backgrounding', '--disable-background-timer-throttling',
        '--disable-backgrounding-occluded-windows', '--disable-features=CalculateNativeWinOcclusion',
        '--disable-breakpad', '--force-device-scale-factor=1', '--hide-scrollbars',
    ] + anti_freeze_args

    debug_args = [
        '--disable-blink-features=AutomationControlled', '--start-maximized', '--window-position=0,0'
    ] + anti_freeze_args

    try:
        with _user_data_dir_lock(user_data_dir, timeout=USER_DATA_LOCK_TIMEOUT_SEC):
            with sync_playwright() as p:
                context = None
                try:
                    context = _launch_persistent(
                        p, user_data_dir,
                        args=debug_args if debug else offscreen_args,
                        viewport=None if debug else {'width': 1920, 'height': 1080},
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

                    response = page.goto(target_post_url, timeout=60000, wait_until="domcontentloaded")
                    _dismiss_overlays(page, aggressive=False, desc="comment-nav")
                    report_guard_hits(page, "comment-nav")

                    if response and response.status >= 400:
                        return f"OKX 帖子详情页加载异常 (HTTP {response.status})", False, None

                    try:
                        page.locator("a[href*='/account/login']").first.wait_for(state="visible", timeout=3000)
                        return "页面探测到登录入口，本地 Cookie 可能已过期失效，请先重新登录。", False, None
                    except PlaywrightTimeoutError:
                        pass

                    check_for_crash(page)

                    # ---------- 点赞逻辑处理 ----------
                    if like:
                        try:
                            # 1. 检测是否已点赞（已点赞图标具有 aria-label="取消点赞" 或包含 okx-trading-like-filled / liked 类）
                            already_liked = page.locator(
                                'i[aria-label="取消点赞"], i.okx-trading-like-filled, i[class*="liked-"]'
                            ).first
                            if already_liked.count() > 0 and already_liked.is_visible(timeout=1500):
                                logger.info("[点赞/Like] 目标帖子已处于点赞状态(检测到「取消点赞」图标)，无需重复点击")
                            else:
                                # 2. 定位未点赞图标并执行点击
                                like_icon_cands = [
                                    page.locator('i[aria-label="点赞"]').first,
                                    page.locator('i.okx-trading-like:not(.okx-trading-like-filled)').first,
                                    page.locator('button:has(i[aria-label="点赞"])').first,
                                    page.locator('button:has(i.okx-trading-like)').first,
                                ]
                                like_target = None
                                for c in like_icon_cands:
                                    try:
                                        if c.count() > 0 and c.is_visible(timeout=1000):
                                            like_target = c
                                            break
                                    except Exception:
                                        continue

                                if like_target:
                                    try:
                                        like_target.scroll_into_view_if_needed(timeout=2000)
                                    except Exception:
                                        pass
                                    page.wait_for_timeout(200)
                                    _robust_click(like_target)
                                    page.wait_for_timeout(500)
                                    logger.info("[点赞/Like] 帖子点赞操作执行完成 | 结果: [Success]")
                                else:
                                    logger.warning("[点赞/Like] 未能匹配到帖子点赞按钮，跳过点赞")
                        except Exception as like_e:
                            logger.warning(f"[点赞/Like] 点赞操作异常（不阻断评论主流程）: {str(like_e)[:120]}")

                    if not content:
                        logger.info("[任务/Comment] 仅点赞模式执行完毕 | 结果: [Success]")
                        return None, True, "LIKE_ONLY"

                    # ---------- 评论流程 ----------
                    try:
                        comment_container = _locate_comment_box(page)
                    except Exception as e:
                        return f"无法定位 OKX 星球评论区: {e}", False, None

                    comment_id = _submit_comment(
                        page, comment_container,
                        content=content
                    )

                    logger.info(f"[任务/Comment] OKX 星球评论发布成功 | 评论ID(commentId): 【{comment_id}】 | 结果: [Success]")
                    return None, True, comment_id

                except BusinessErrorException as biz_e:
                    error_info = f"[业务拦截] {biz_e}"
                    logger.error(f"[任务/Comment] 评论被服务端业务规则阻断 | 原因: 【{error_info}】")
                    return error_info, False, None

                except Exception as e:
                    error_info = f"[{type(e).__name__}] {e}\n[Traceback]:\n{traceback.format_exc()}"
                    if context and context.pages:
                        try:
                            _forensics(context.pages[0], "comment_error", {"url": target_post_url, "error": error_info[:2000]})
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

def open_browser_for_manual_use(user_data_dir=USER_DATA_DIR, home_url=OKX_ORBIT_URL):
    """
    主控入口 2：启动可见浏览器交由人工自由操作（用于首次扫码/账号登录）。
    """
    logger.info(f"\n{'=' * 60}\n[人工/Manual] 启动本地浏览器交接控制权 | 目录: <{user_data_dir}>\n{'=' * 60}")
    with _user_data_dir_lock(user_data_dir, timeout=USER_DATA_LOCK_TIMEOUT_SEC):
        with sync_playwright() as p:
            context = None
            try:
                context = _launch_persistent(
                    p, user_data_dir,
                    args=['--disable-blink-features=AutomationControlled', '--start-maximized', '--window-position=0,0'],
                    debug=True,
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
# ==============================================================================
#                    核心：通过浏览器原生请求批量删除 OKX 星球帖子
# ==============================================================================

_DELETE_SIGN_HOOK_INIT_JS = r"""
(() => {
    if (window.__okxDeleteHookInstalled) return;
    window.__okxDeleteHookInstalled = true;
    window.__okx_sig_map = {};
    window.__okx_matched_sample = null;

    const extractCryptoKey = (val, depth = 0) => {
        if (!val || depth > 4) return;
        try {
            if (typeof CryptoKey !== 'undefined' && val instanceof CryptoKey) {
                if (val.type === 'private') {
                    window.__okx_priv_key = val;
                }
                return;
            }
            if (typeof val === 'object') {
                for (const k of Object.keys(val)) {
                    extractCryptoKey(val[k], depth + 1);
                }
            }
        } catch (e) {}
    };

    // 1. 监听 IndexedDB 读取，自动捕获持久化的不可导出私钥对象
    try {
        const origGet = IDBObjectStore.prototype.get;
        IDBObjectStore.prototype.get = function(...args) {
            const req = origGet.apply(this, args);
            req.addEventListener('success', function() {
                extractCryptoKey(this.result);
            });
            return req;
        };
        const origGetAll = IDBObjectStore.prototype.getAll;
        if (origGetAll) {
            IDBObjectStore.prototype.getAll = function(...args) {
                const req = origGetAll.apply(this, args);
                req.addEventListener('success', function() {
                    extractCryptoKey(this.result);
                });
                return req;
            };
        }
    } catch (e) {}

    // 2. 劫持 WebCrypto sign，捕获内存私钥与签名原文
    try {
        const origSign = window.crypto.subtle.sign.bind(window.crypto.subtle);
        window.crypto.subtle.sign = async function(algorithm, key, data) {
            try {
                if (key && key.type === 'private') {
                    window.__okx_priv_key = key;
                    window.__okx_sign_algo = algorithm;
                }
                let rawText = '';
                try {
                    const u8 = data instanceof ArrayBuffer
                        ? new Uint8Array(data)
                        : new Uint8Array(data.buffer || data);
                    rawText = new TextDecoder().decode(u8);
                } catch (e) {}

                const sigBuf = await origSign(algorithm, key, data);
                try {
                    const b64 = btoa(String.fromCharCode(...new Uint8Array(sigBuf)));
                    const fullSig = `{P1363}${b64}`;
                    window.__okx_last_sign_raw = rawText;
                    window.__okx_sig_map[fullSig] = rawText;
                } catch (e) {}
                return sigBuf;
            } catch (err) {
                return origSign(algorithm, key, data);
            }
        };
    } catch (e) {}

    // 3. 关联 XHR / fetch 请求头中的 x-client-signature，自动学习 1.3 签名拼接模板
    const recordSample = (method, url, headers, bodyStr) => {
        try {
            const sig = headers['x-client-signature'] || headers['X-Client-Signature'];
            const ts = headers['x-request-timestamp'] || headers['X-Request-Timestamp'];
            if (!sig || !window.__okx_sig_map[sig]) return;
            const rawText = window.__okx_sig_map[sig];
            const u = new URL(url, location.origin);
            window.__okx_matched_sample = {
                rawText,
                method: (method || 'GET').toUpperCase(),
                fullPath: u.pathname + u.search,
                pathname: u.pathname,
                ts: String(ts || ''),
                body: typeof bodyStr === 'string' ? bodyStr : ''
            };
        } catch (e) {}
    };

    try {
        const origOpen = XMLHttpRequest.prototype.open;
        const origSetHeader = XMLHttpRequest.prototype.setRequestHeader;
        const origSend = XMLHttpRequest.prototype.send;
        XMLHttpRequest.prototype.open = function(method, url, ...rest) {
            this.__okx_method = method;
            this.__okx_url = url;
            this.__okx_headers = {};
            return origOpen.call(this, method, url, ...rest);
        };
        XMLHttpRequest.prototype.setRequestHeader = function(k, v) {
            if (this.__okx_headers) this.__okx_headers[k.toLowerCase()] = v;
            return origSetHeader.call(this, k, v);
        };
        XMLHttpRequest.prototype.send = function(body) {
            recordSample(this.__okx_method, this.__okx_url, this.__okx_headers || {}, body);
            return origSend.call(this, body);
        };
    } catch (e) {}

    try {
        const origFetch = window.fetch.bind(window);
        window.fetch = async function(input, init = {}) {
            try {
                const url = typeof input === 'string' ? input : (input && input.url) || '';
                const method = init.method || (input && input.method) || 'GET';
                const hdrs = {};
                if (init.headers) {
                    if (init.headers instanceof Headers) {
                        init.headers.forEach((v, k) => { hdrs[k.toLowerCase()] = v; });
                    } else if (typeof init.headers === 'object') {
                        for (const [k, v] of Object.entries(init.headers)) {
                            hdrs[k.toLowerCase()] = v;
                        }
                    }
                }
                recordSample(method, url, hdrs, init.body);
            } catch (e) {}
            return origFetch(input, init);
        };
    } catch (e) {}
})();
"""

_TRIGGER_DELETE_API_JS = r"""
async ({ contentId, seq }) => {
    const getCookie = (name) => {
        const match = document.cookie.match(new RegExp('(^|;\\s*)' + name + '=([^;]*)'));
        return match ? decodeURIComponent(match[2]) : '';
    };

    const ts = Date.now().toString();
    const path = '/priapi/v5/content/ugc/delete';
    const fullPath = `${path}?t=${ts}`;
    const bodyStr = JSON.stringify({ contentId: String(contentId) });

    const rawToken = getCookie('token');
    const authHeader = rawToken
        ? (rawToken.startsWith('Bearer ') ? rawToken : rawToken)
        : '';
    const devId = getCookie('devId') || getCookie('fingerprint_id') || '';
    const siteInfo = getCookie('ok_site_info') || '';
    const traceId = getCookie('traceId') || `${Date.now()}000000`;
    const locale = getCookie('locale') || getCookie('preferLocale') || 'zh_CN';

    let clientSignature = '';
    let usedRawToSign = '';

    // 若尚未通过被动 Hook 捕获到私钥，主动遍历 IndexedDB 查找不可导出私钥对象
    if (!window.__okx_priv_key && typeof indexedDB !== 'undefined' && indexedDB.databases) {
        try {
            const dbs = await indexedDB.databases();
            for (const dbInfo of (dbs || [])) {
                if (!dbInfo || !dbInfo.name || window.__okx_priv_key) continue;
                await new Promise((resolve) => {
                    try {
                        const openReq = indexedDB.open(dbInfo.name);
                        openReq.onsuccess = () => {
                            const db = openReq.result;
                            const storeNames = Array.from(db.objectStoreNames || []);
                            if (!storeNames.length) { db.close(); return resolve(); }
                            let pending = storeNames.length;
                            for (const sName of storeNames) {
                                try {
                                    const tx = db.transaction(sName, 'readonly');
                                    const store = tx.objectStore(sName);
                                    const getAllReq = store.getAll();
                                    getAllReq.onsuccess = () => {
                                        const items = getAllReq.result || [];
                                        const findKey = (v, d = 0) => {
                                            if (!v || d > 4) return;
                                            if (typeof CryptoKey !== 'undefined' && v instanceof CryptoKey && v.type === 'private') {
                                                window.__okx_priv_key = v;
                                                return;
                                            }
                                            if (typeof v === 'object') {
                                                for (const k of Object.keys(v)) findKey(v[k], d + 1);
                                            }
                                        };
                                        findKey(items);
                                        if (--pending <= 0) { db.close(); resolve(); }
                                    };
                                    getAllReq.onerror = () => { if (--pending <= 0) { db.close(); resolve(); } };
                                } catch (e) {
                                    if (--pending <= 0) { db.close(); resolve(); }
                                }
                            }
                        };
                        openReq.onerror = () => resolve();
                    } catch (e) { resolve(); }
                });
            }
        } catch (e) {}
    }

    // 如果已截获到内存私钥，则动态生成 {P1363} 签名
    if (window.__okx_priv_key) {
        try {
            const sample = window.__okx_matched_sample;
            if (sample && sample.rawText) {
                let text = sample.rawText;
                if (sample.fullPath && text.includes(sample.fullPath)) {
                    text = text.replace(sample.fullPath, fullPath);
                } else if (sample.pathname && text.includes(sample.pathname)) {
                    text = text.replace(sample.pathname, path);
                }
                if (sample.ts && text.includes(sample.ts)) {
                    text = text.split(sample.ts).join(ts);
                }
                if (sample.method && text.includes(sample.method)) {
                    text = text.replace(sample.method, 'POST');
                }
                if (sample.body && text.includes(sample.body)) {
                    text = text.replace(sample.body, bodyStr);
                } else if (sample.method === 'GET' && !text.includes(bodyStr)) {
                    text = text + bodyStr;
                }
                usedRawToSign = text;
            } else {
                usedRawToSign = `${ts}POST${fullPath}${bodyStr}`;
            }

            const sigBuf = await window.crypto.subtle.sign(
                window.__okx_sign_algo || { name: 'ECDSA', hash: { name: 'SHA-256' } },
                window.__okx_priv_key,
                new TextEncoder().encode(usedRawToSign)
            );
            const b64Sig = btoa(String.fromCharCode(...new Uint8Array(sigBuf)));
            clientSignature = `{P1363}${b64Sig}`;
        } catch (signErr) {
            usedRawToSign = `sign_error:${signErr.message}`;
        }
    }

    const headers = {
        'accept': 'application/json',
        'content-type': 'application/json',
        'authorization': authHeader,
        'devid': devId,
        'platform': 'web',
        'x-cdn': 'https://www.okx.com',
        'x-id-group': `${traceId}-c-${seq}`,
        'x-locale': locale,
        'x-request-timestamp': ts,
        'x-simulated-trading': 'undefined',
        'x-site-info': siteInfo,
        'x-utc': '8',
        'x-zkdex-env': '0'
    };

    if (clientSignature) {
        headers['x-client-signature'] = clientSignature;
        headers['x-client-signature-version'] = '1.3';
    }

    try {
        const resp = await fetch(`https://www.okx.com${fullPath}`, {
            method: 'POST',
            headers,
            body: bodyStr,
            credentials: 'include'
        });
        const text = await resp.text();
        let json = {};
        try {
            json = JSON.parse(text);
        } catch (e) {
            json = { raw: text.slice(0, 300) };
        }
        return {
            httpStatus: resp.status,
            body: json,
            signed: Boolean(clientSignature),
            hasPrivKey: Boolean(window.__okx_priv_key),
            sampleRaw: window.__okx_last_sign_raw || ''
        };
    } catch (fetchErr) {
        return {
            httpStatus: 0,
            body: { msg: fetchErr.message || String(fetchErr) },
            signed: Boolean(clientSignature),
            hasPrivKey: Boolean(window.__okx_priv_key),
            sampleRaw: window.__okx_last_sign_raw || ''
        };
    }
}
"""


def _delete_single_post_by_api(page, content_id, seq=1):
    """在已登录页面上下文内触发单条删帖 API 请求并校验返回结果。"""
    cid_str = str(content_id or "").strip()
    if not cid_str:
        return False, "contentId 为空"

    res = page.evaluate(_TRIGGER_DELETE_API_JS, {"contentId": cid_str, "seq": int(seq)})
    http_status = res.get("httpStatus", 0)
    body = res.get("body") if isinstance(res.get("body"), dict) else {}
    code_str = str(body.get("code", ""))
    signed = res.get("signed", False)

    if http_status == 200 and (code_str in ("0", "000000") or body.get("success") is True):
        logger.info(
            f"[删除/校验] 帖子删除成功 | contentId: 【{cid_str}】 | 响应码: 【{code_str}】 "
            f"| 携带签名: 【{signed}】 | 结果: [Success]"
        )
        return True, None

    err_msg = (
        body.get("msg")
        or body.get("message")
        or body.get("error_message")
        or body.get("raw")
        or f"HTTP_{http_status}"
    )
    logger.error(
        f"[删除/校验] 帖子删除失败 | contentId: 【{cid_str}】 | HTTP: 【{http_status}】 "
        f"| code: 【{code_str}】 | 携带签名: 【{signed}】 | 原因: 【{err_msg}】"
    )
    return False, f"HTTP={http_status}, code={code_str}, msg={err_msg}"


def delete_okx_posts(post_id_list, user_data_dir=USER_DATA_DIR, debug=False, interval_sec=0.8):
    """
    主控入口 4：打开 OKX 星球页面后，通过浏览器上下文逐个触发 `/priapi/v5/content/ugc/delete` 接口批量删除帖子。
    :param post_id_list: 待删除的帖子 ID 列表（支持 list/tuple/set 或单个 str/int，自动保序去重）
    :param user_data_dir: 浏览器用户数据目录
    :param debug: 是否开启调试模式（True 显示界面，False 离屏后台，默认 False）
    :param interval_sec: 每次删除请求之间的间隔秒数，默认 0.8 秒
    返回 Tuple(错误信息(str|None), 是否全部成功(bool), 结果明细(dict: {"success_ids": [...], "failed_ids": {...}}))
    """
    if isinstance(post_id_list, (str, int)):
        raw_ids = [str(post_id_list).strip()] if str(post_id_list).strip() else []
    elif isinstance(post_id_list, (list, tuple, set)):
        raw_ids = [str(x).strip() for x in post_id_list if str(x or "").strip()]
    else:
        raw_ids = []

    clean_id_list = list(dict.fromkeys(raw_ids))

    if not clean_id_list:
        return "待删除的 post_id_list 不能为空", False, {"success_ids": [], "failed_ids": {}}

    if not os.path.isdir(user_data_dir):
        return f"缺少用户环境: {user_data_dir}，请先执行登录", False, {"success_ids": [], "failed_ids": {}}

    logger.info(
        f"\n{'=' * 70}\n[任务/Delete] 启动 OKX 星球批量接口删帖 | URL: <{OKX_ORBIT_URL}> "
        f"| 去重后待删数量: 【{len(clean_id_list)}】条 | 间隔: 【{interval_sec}s】 "
        f"| 模式: 【{'debug可见' if debug else '离屏后台'}】\n{'=' * 70}"
    )

    anti_freeze_args = ['--disable-restore-session-state', '--no-default-browser-check']
    offscreen_args = [
        '--disable-blink-features=AutomationControlled', '--disable-gpu',
        '--window-position=-10000,-10000', '--no-sandbox', '--disable-dev-shm-usage',
        '--disable-renderer-backgrounding', '--disable-background-timer-throttling',
        '--disable-backgrounding-occluded-windows', '--disable-features=CalculateNativeWinOcclusion',
        '--disable-breakpad', '--force-device-scale-factor=1', '--hide-scrollbars',
    ] + anti_freeze_args

    debug_args = [
        '--disable-blink-features=AutomationControlled', '--start-maximized', '--window-position=0,0'
    ] + anti_freeze_args

    success_ids = []
    failed_ids = {}

    try:
        with _user_data_dir_lock(user_data_dir, timeout=USER_DATA_LOCK_TIMEOUT_SEC):
            with sync_playwright() as p:
                context = None
                try:
                    context = _launch_persistent(
                        p, user_data_dir,
                        args=debug_args if debug else offscreen_args,
                        viewport=None if debug else {'width': 1920, 'height': 1080},
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

                    # 注入浮层免疫与 WebCrypto 签名截获脚本
                    install_overlay_guard(page)
                    page.add_init_script(_DELETE_SIGN_HOOK_INIT_JS)

                    response = page.goto(OKX_ORBIT_URL, timeout=60000, wait_until="domcontentloaded")
                    _dismiss_overlays(page, aggressive=False, desc="delete-nav")
                    report_guard_hits(page, "delete-nav")

                    if response and response.status >= 400:
                        err_msg = f"OKX 星球主页加载异常 (HTTP {response.status})"
                        return err_msg, False, {"success_ids": success_ids, "failed_ids": failed_ids}

                    try:
                        page.locator("a[href*='/account/login']").first.wait_for(state="visible", timeout=3000)
                        err_msg = "页面探测到登录入口，本地 Cookie 可能已过期失效，请先重新登录。"
                        return err_msg, False, {"success_ids": success_ids, "failed_ids": failed_ids}
                    except PlaywrightTimeoutError:
                        pass

                    check_for_crash(page)

                    # 轮询等待页面初始化请求与 WebCrypto 签名私钥就绪（最多等待 6 秒）
                    for wait_round in range(12):
                        page.wait_for_timeout(500)
                        try:
                            ready = page.evaluate("() => Boolean(window.__okx_priv_key)")
                            if ready:
                                break
                            if wait_round == 3:
                                page.mouse.wheel(0, 300)
                        except Exception:
                            pass

                    total = len(clean_id_list)
                    for idx, post_id in enumerate(clean_id_list, start=1):
                        logger.info(f"[删除/Delete] 正在触发第 【{idx}/{total}】 条删帖请求 | contentId: 【{post_id}】")
                        ok, err_detail = _delete_single_post_by_api(page, post_id, seq=30 + idx)
                        if ok:
                            success_ids.append(post_id)
                        else:
                            failed_ids[post_id] = err_detail

                        if idx < total and interval_sec > 0:
                            page.wait_for_timeout(int(interval_sec * 1000))

                    all_success = (len(failed_ids) == 0)
                    summary = {"success_ids": success_ids, "failed_ids": failed_ids}

                    if all_success:
                        logger.info(
                            f"[任务/Delete] 批量删帖全部完成 | 成功: 【{len(success_ids)}/{total}】 | 结果: [Success]"
                        )
                        return None, True, summary
                    else:
                        err_summary = f"部分或全部帖子删除失败（成功 {len(success_ids)} 条，失败 {len(failed_ids)} 条）: {failed_ids}"
                        logger.warning(f"[任务/Delete] {err_summary}")
                        return err_summary, False, summary

                except Exception as e:
                    error_info = f"[{type(e).__name__}] {e}\n[Traceback]:\n{traceback.format_exc()}"
                    if context and context.pages:
                        try:
                            _forensics(
                                context.pages[0],
                                "delete_error",
                                {"url": OKX_ORBIT_URL, "error": error_info[:2000], "failed_ids": failed_ids}
                            )
                        except Exception:
                            pass
                    _alert_failure_scene(error_info)
                    return error_info, False, {"success_ids": success_ids, "failed_ids": failed_ids}

                finally:
                    if context:
                        try:
                            context.close()
                        except Exception:
                            pass

    except UserDataDirLockTimeoutError as lock_e:
        error_info = f"[UserDataDirLockTimeout] {lock_e}"
        logger.error(f"[任务/Delete] {error_info}")
        return error_info, False, {"success_ids": success_ids, "failed_ids": failed_ids}

    except Exception as core_e:
        error_info = f"[CoreEngineCrash] Playwright底层崩溃:\n{core_e}"
        logger.error(error_info)
        return error_info, False, {"success_ids": success_ids, "failed_ids": failed_ids}


# ==============================================================================
#                                   启动入口
# ==============================================================================
if __name__ == "__main__":
    # 如需人工登录，取消下方注释：
    open_browser_for_manual_use(USER_DATA_DIR)