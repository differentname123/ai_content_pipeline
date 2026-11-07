# -*- coding: utf-8 -*-
"""
=========================================================================================
[功能摘要]
    币安广场「全自动评论」RPA：复用本地 Chrome 登录态打开帖子，在评论区一次性注入
    【图片 + 超链接 + 正文】并发送，最终以「提交接口响应」为主判据、「编辑器被清空」为兜底
    判据来确认成败，失败自动落地故障现场。

[输入数据]
- post_url

: 目标帖子 URL(str)，用于解析 post_id 做删帖/重定向校验
-     comment

: 评论正文(str)，允许多行
-     image_path

: 本地图片物理路径(str|None)，文件缺失或上传失败自动降级为纯文本
-     url_info_list

: 超链接清单，Shape: [{"text": <锚文本>, "url": <地址>}, ...]
- user_data_dir

: Chrome 持久化目录，承载 Cookie / CSRF 登录态

[数据流转/交互]
1.
凭证挂载：launch_persistent_context 复用 User Data 目录 → 免登录浏览器上下文
    2. 浮层免疫：goto 之前武装守卫(init_script 预置引导flag + JS看门狗 + locator_handler)
    3. 落地体检：HTTP 状态 → post_id 是否漂移(重定向/删帖) → 登录态 → 平台软拦截 → 崩溃探测
    4. 作用域隔离：PageDown 步进探底锁定 div.feed-post-editor，之后所有操作只在该容器内进行
    5. 数据注入：唤醒 ProseMirror →[图片]→[逐条超链接(弹窗填写)]→ 光标置顶 →[正文分块键入]
    6. 结果判定：点击发送 → _ApiWatcher 抓 POST `pgc/content/add`；超时则比对编辑器清空程度
    7. 失败留证：_forensics 落地 png / html / json(含视口中心遮挡诊断)，供人工复盘

[输出数据]
-
返回 Tuple(错误信息|None, 是否成功|bool, 评论ID|None)
-
副作用：结构化终端日志；失败现场文件 forensic_*.png/.html/.json；浏览器上下文关闭
=========================================================================================
"""
import atexit
from contextlib import contextmanager
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import threading
import time
import traceback
from urllib.parse import urlparse, unquote

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
USER_DATA_DIR = resolve_session_dir("ruru_browser_session_dir", "biance_ruru")
LOGIN_URL = "https://www.binance.com/zh-CN/login"

TYPE_CHUNK_SIZE = 80  # 正文分块长度：仅切分 press_sequentially 调用，键序与延迟不变
TYPE_DELAY_MS = 60  # 逐字键入延迟(ms)，模拟真人输入
API_WAIT_TIMEOUT_MS = 10000  # 发送后等待提交接口响应的上限
GUARD_INTERVAL_MS = 700  # 页面内 JS 看门狗轮询间隔
USER_DATA_LOCK_TIMEOUT_SEC = 300  # user_data_dir 跨进程/跨线程最大等待时间(5分钟)

# ⚠️ 换行安全模式：False(默认，线上行为) —— 正文中的 \n 直接作 Enter 送出；
#    True(可选加固) —— 改用 Shift+Enter，防止"Enter=发送"导致内容截断提前发出。
#    这会改变键序，仅在确认遇到"多行正文被截断发出"时才开启。
NEWLINE_SAFE_MODE = False

# ==============================================================================
#                             文案正则 / 选择器常量
# ==============================================================================
RE_MORE = re.compile(r"更多|More|Options|Expand", re.IGNORECASE)
RE_ADD_LINK = re.compile(r"添加链接|Add link|Insert link", re.IGNORECASE)
RE_CONFIRM = re.compile(r"确认|Confirm|OK|Save|Add", re.IGNORECASE)

RE_SEND = re.compile(r"回复|发送|评论|发文|发布|Reply|Comment|Send|Post|Publish", re.IGNORECASE)
RE_SEND_EXACT = re.compile(r"^(回复|发送|评论|发文|发布|Reply|Comment|Send|Post|Publish)$", re.IGNORECASE)

# 引导浮层"确认关闭"类按钮：刻意不含「取消/Cancel」，避免误取消业务弹窗
RE_DISMISS = re.compile(
    r"^\s*(好的|好|知道了|我知道了|明白了|明白|了解|开始使用|立即体验|马上体验|下一步|"
    r"完成|跳过|不再提示|不再显示|以后再说|稍后|关闭|"
    r"OK|Okay|Got it|Got It|I see|Understood|Skip|Next|Done|Continue|Close|Dismiss|Later|Maybe later)\s*$",
    re.IGNORECASE,
)

# 发送按钮黑名单：页面底部存在「立即回复」这类"打开编辑器"的跳转按钮，误点会导致正文根本没提交
RE_SEND_BLACKLIST = re.compile(r"(立即回复|去回复|查看|更多|展开|取消|Cancel|View|More)", re.IGNORECASE)

# Cookie 同意（页面内确实存在 OneTrust 隐私偏好中心）
COOKIE_SELECTORS = (
    "#onetrust-accept-btn-handler",
    "#onetrust-close-btn-container button",
    "button:has-text('全部允许')",
    "button:has-text('确认我的选择')",
    "button:has-text('Accept All')",
    "button:has-text('Allow All')",
)
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
    """
    把光标绝对锁定到编辑器文本的尾部。
    对抗 ProseMirror 中普通 `End` 键只到行尾导致的光标乱窜问题。
    """
    try:
        real_editor.evaluate(_CARET_TO_TAIL_JS)
    except Exception as e:
        logger.warning(f"[编辑器/光标] Range 置底执行异常，降级为键盘方案 | 场景: <{tag}> | 详情: 【{str(e)[:120]}】")

    # 兜底：使用 Control+End (Win) 或 Meta+End (Mac) 跳到整个文档末尾
    for combo in ("Control+End", "Meta+End", "End"):
        try:
            real_editor.press(combo, timeout=2000)
            break
        except Exception:
            continue
    page.wait_for_timeout(300)


# 编辑器保护白名单：任何清障动作都不许碰到含这些元素的容器
EDITOR_GUARD_SELECTOR = '.ProseMirror,[contenteditable="true"],input[type="file"],textarea'

# 提交接口主判据（与线上一致，只认这一个）；辅助判据仅用于观测与"主接口没抓到"时的成功识别
RE_SUBMIT_API_PRIMARY = re.compile(r"pgc/content/add", re.IGNORECASE)
RE_SUBMIT_API_SECONDARY = re.compile(
    r"(content/comment|comment/add|comment/create|/reply|square/.*(publish|post/add))", re.IGNORECASE
)
logger = setup_logger(app_name="biance_playwright")


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
    1. 进程内通过 RLock + 线程递归计数支持多线程排队与同线程重入；
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
        lock_dir = os.path.join(tempfile.gettempdir(), "biance_playwright_userdata_locks")
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

        # 第一关：进程内线程锁（支持同线程无阻塞重入）
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

            # 第二关：跨进程 OS 内核文件锁
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
    """
    可重入的 user_data_dir 独占锁上下文管理器。
    保证同一机器跨进程、同进程多线程绝对不会同时访问同一个 user_data_dir，最多等待 5 分钟。
    """
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
    """清理浏览器冗余缓存目录、保留登录凭证。单项删除失败按原设计静默忽略（尽力而为，不阻断主流程）。"""
    if not os.path.exists(user_data_dir):
        return

    with _user_data_dir_lock(user_data_dir, timeout=USER_DATA_LOCK_TIMEOUT_SEC):
        garbage = ("Cache", "Code Cache", "GPUCache", "ShaderCache", "GrShaderCache", "Service Worker", "CacheStorage")
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
    """
    对抗前端结构多变的核心健壮性机制：轮询后备选择器清单，把长阻塞打散为 200ms 时间片，
    避免单一选择器失效造成整体长时间卡顿。action="click" 命中即点击返回，否则命中即返回 locator。
    """
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
    """三段降级点击：常规 → 强制穿透遮挡 → JS 原生。前两段失败静默降级，末段失败如实抛出。"""
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
    """
    读取编辑器状态快照，用于发送前后比对是否清空。
    返回 (文本字符数, 媒体元素数[img/a])；元素不可见或异常时返回 (0, 0)。
    """
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
    """
    统一存证：截图 + HTML + JSON（自动附带"视口中心是谁挡住的"诊断与看门狗战果）。
    所有降级 / 失败路径都应调用，杜绝"死无对证"。extra 形貌: 任意可 JSON 序列化的 dict。
    """
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
    """返回 {ok: bool, reason, blockerTag/blockerText/coverRatio...}：既是判断依据，也是最有价值的排查日志。"""
    try:
        return locator.evaluate(_HIT_TEST_JS)
    except Exception as e:
        return {"ok": False, "reason": f"eval-error:{str(e)[:120]}"}


def _fmt_hit(hit):
    """把命中测试结果压成一行人话，供日志直接引用。"""
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
    """
    分级清障（绝不触碰任何含编辑器的容器）：
      L1 —— 按语义点掉 Cookie 横幅 / 引导浮层
      L2 —— 点弹窗关闭图标 / 按 Escape
      L3 —— aggressive=True 时物理移除遮罩 + 解 scroll-lock（兜底）
    返回：本次是否执行过任何清障动作(bool)。
    """
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
    const SCOPE = /square|thread|post|short|feed|bibi|social/i;
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
    """
    武装浮层免疫（⚠️ 必须在 page.goto() 之前调用，init_script 只对之后的导航生效）。
    三重保险：init 预置引导已读 flag + 页面内 JS 看门狗常驻点击 + locator_handler 被挡自愈。
    """
    try:
        page.add_init_script(_GUARD_INIT_JS.replace("__GUARD_INTERVAL__", str(GUARD_INTERVAL_MS)))
    except Exception as e:
        logger.warning(f"[守卫/Guard] init_script 注入失败，引导浮层可能反复弹出并拦截点击 | 原因: 【{str(e)[:120]}】")

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
        logger.warning(
            f"[守卫/Guard] locator_handler 不可用(需 Playwright>=1.42)，退化为手动清障 | 原因: 【{str(e)[:120]}】")
        return

    try:
        page.add_locator_handler(trigger, _on_overlay)
        logger.info("[守卫/Guard] 浮层免疫已武装（兼容模式：无 no_wait_after）")
    except Exception as e:
        logger.warning(f"[守卫/Guard] locator_handler 挂载失败，退化为手动清障 | 原因: 【{str(e)[:120]}】")


def report_guard_hits(page, stage=""):
    """读取 JS 看门狗战果，便于事后定位"到底自动关掉了什么浮层"。"""
    try:
        info = page.evaluate("() => ({hits: window.__bnGuardHits || 0, last: window.__bnGuardLast || ''})")
        if info and info.get("hits"):
            logger.info(f"[守卫/Guard] 看门狗累计自动关闭浮层 | 阶段: <{stage}> | 次数: 【{info['hits']}】 "
                        f"| 最后文案: <{info['last']}>")
    except Exception:
        pass


# ==============================================================================
#                            编辑器：定位 / 唤醒 / 光标
# ==============================================================================

def _smart_scroll_to_editor(page, max_scrolls=20):
    """步进式 PageDown 探底，锁定评论区富文本容器并滚入可视范围（后续所有操作的作用域根）。"""
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
    """
    解析"点哪里能唤醒编辑器"的候选（兼容评论区和广场主页发帖区）。
    """
    cands = (
        editor_container.locator(
            'input[type="text"]:not([type="search"]):not([placeholder*="搜索"])'
            ':not([placeholder*="Search"]):not([aria-label*="搜索"]):not([aria-label*="Search"]),'
            'input[placeholder]:not([type="search"]):not([placeholder*="搜索"]):not([placeholder*="Search"])'
        ).first,
        editor_container.get_by_placeholder(
            re.compile(
                r"(分享您的洞见|Share your insights|分享你的|发布您的回复|发布你的回复|写下你的|说点什么|发表评论|回复|评论|Reply|Comment|Write|Post)",
                re.IGNORECASE)
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
    """
    唤醒富文本编辑器（业务动作不变：点击输入区让 ProseMirror 变为可编辑）。
    每轮先清障，点击方式逐级升级：常规 → force → JS 事件序列 → 鼠标坐标+Tab。返回 real_editor(Locator)。
    """
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
                page.keyboard.press("Tab")
        except Exception as e:
            logger.warning(f"[编辑器/唤醒] 第{rnd}轮点击动作抛错，升级策略重试 | 详情: 【{str(e)[:150]}】")

        try:
            expect(real_editor).to_be_editable(timeout=8000 if rnd == 1 else 4000)
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


def _force_caret_to_head(page, real_editor, tag=""):
    """
    把光标锁定到编辑器文本的绝对头部——这是保证「正文顶在已注入超链接之前」的关键潜规则。
    """
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
    logger.info(f"[编辑器/光标] 已锁定光标到文本头部 | 场景: <{tag}> | 校验: 【在头部={verify.get('atHead')}, "
                f"距头部字符数={verify.get('offsetChars')}】")


# ==============================================================================
#                          编辑器：超链接注入 / 正文键入
# ==============================================================================

def _inject_single_link(page, editor_container, real_editor, link_text, link_url, idx):
    """
    在富文本末尾唤起「更多 → 添加链接」弹窗，注入单条超链接并校验上屏。
    """
    logger.info(f"[编辑器/链接] 开始注入第 【{idx + 1}】 条 | 锚文本: 【{link_text}】 | URL: <{link_url}>")
    try:
        _focus_editor_end(page, real_editor)
        page.keyboard.press("Space")
        page.wait_for_timeout(150)

        _interact_fallback_locators([
            editor_container.locator('#post-editor-more-icon').first,
            editor_container.locator("svg").filter(has=page.locator('path[d^="M12 16.5"]')).first,
            editor_container.locator("div.icon-box").filter(has=page.locator('svg')).last,
            editor_container.get_by_role("button", name=RE_MORE).first,
            editor_container.locator('button[aria-label*="更多"], button[aria-label*="More" i]').first,
        ], action="click", timeout=4000, desc="更多按钮")
        page.wait_for_timeout(350)

        _interact_fallback_locators([
            page.locator('.menu-item').filter(has_text=RE_ADD_LINK).first,
            page.get_by_role("menuitem", name=RE_ADD_LINK).first,
            page.locator('[role="menuitem"], [class*="menu-item"]').filter(has_text=RE_ADD_LINK).first,
        ], action="click", timeout=4000, desc="添加链接选项")

        dialog = page
        try:
            dlg = page.get_by_role("dialog").last
            dlg.wait_for(state="visible", timeout=2000)
            dialog = dlg
        except Exception:
            pass

        name_input = _interact_fallback_locators([
            dialog.locator('input[name="name"][data-bn-type="input"]').first,
            dialog.locator('input[name="name"]').first,
            dialog.get_by_placeholder(re.compile(r"正文|名称|标题|text|name|title", re.IGNORECASE)).first,
        ], action="wait", timeout=6000, desc="链接正文输入框")

        link_input = _interact_fallback_locators([
            dialog.locator('input[name="link"][data-bn-type="input"]').first,
            dialog.locator('input[name="link"]').first,
            dialog.get_by_placeholder(re.compile(r"链接|地址|link|url|address", re.IGNORECASE)).first,
        ], action="wait", timeout=6000, desc="链接地址输入框")

        confirm_btn = _interact_fallback_locators([
            dialog.locator('button[type="submit"][data-bn-type="button"]').filter(has_text=RE_CONFIRM).first,
            dialog.locator('button[type="submit"]').filter(has_text=RE_CONFIRM).first,
            dialog.get_by_role("button", name=RE_CONFIRM).first,
        ], action="wait", timeout=6000, desc="链接确认按钮")

        name_input.fill(link_text)
        page.wait_for_timeout(200)
        link_input.fill(link_url)
        page.wait_for_timeout(200)

        expect(confirm_btn).to_be_enabled(timeout=6000)
        confirm_btn.click(timeout=6000)
        expect(name_input).to_be_hidden(timeout=6000)

        expect(real_editor.locator("a").filter(
            has_text=re.compile(re.escape(link_text), re.IGNORECASE)).first).to_be_visible(timeout=5000)

        _focus_editor_end(page, real_editor)
        page.keyboard.press("Space")
        page.wait_for_timeout(200)
        logger.info(f"[编辑器/链接] 第 【{idx + 1}】 条注入成功并已上屏 | 结果: [Success]")
        return True

    except Exception as e:
        logger.warning(f"[编辑器/链接] 第 【{idx + 1}】 条注入失败，按设计跳过继续下一条 | 锚文本: 【{link_text}】 "
                       f"| 可能原因: 【更多菜单未唤醒 / 弹窗结构变动 / 上屏校验超时: {str(e)[:150]}】 | 结果: [Skipped]")
        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(250)
        except Exception:
            pass
        return False


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
                           f"| 片段长度: 【{len(seg)}】 | 可能原因: 【前端框架吞键 / 焦点被抢】")
            try:
                page.keyboard.insert_text(seg)
            except Exception:
                pass


def _normalize_text_list(text_list):
    """
    解析并规范化 text_list 参数，元素支持 dict 或 JSON 字符串，形如 {"text": "大家好", "delay": 5}。
    若无有效元素则返回空列表 []。
    """
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
    """
    按 text_list 顺序逐元素键入文字，每个元素输入完成后等待其 delay 秒数，再继续下一个元素的输入。
    """
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
    """按原候选顺序挑选发送按钮，仅追加黑名单过滤。"""
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
    """点击前挂 response 监听、点击后轮询取结果。"""

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
    """兼容 502/风控返回 HTML 而非 JSON 的场景，恒返回 dict。"""
    try:
        raw = resp.json()
        return raw if isinstance(raw, dict) else {"raw": str(raw)}
    except Exception:
        return {}


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
            logger.error(f"[发送/校验] 服务端业务规则拒收本条评论 | HTTP: 【{resp.status}】 | 原因: 【{err}】 "
                         f"| 排查方向: 【内容违规 / 发送频率限制 / 该帖已关闭评论 / 账号权限不足】")
            _forensics(page, "biz_reject", {"url": resp.url, "status": resp.status, "body": body})
            raise BusinessErrorException(f"业务发送被服务器拦截，原因: {err}")
        logger.warning(f"[发送/校验] 主接口响应非 JSON(HTTP 【{resp.status}】)，转入 DOM 兜底校验 "
                       f"| 可能原因: 【网关 502 / 风控返回 HTML 页面】")
        return False, None

    sec = watcher.latest_secondary()
    if sec is None:
        logger.warning(f"[发送/校验] 【{API_WAIT_TIMEOUT_MS}ms】 内未捕获到提交接口，转入 DOM 兜底校验 "
                       f"| 可能原因: 【点击未真正生效 / 请求被前端校验拦下 / 网络极慢】")
        return False, None

    body = _parse_api_json(sec)
    logger.info(f"[发送/校验] 观测到疑似评论接口（非主判据） | URL: <{sec.url}> | HTTP: 【{sec.status}】 "
                f"| body: 【{str(body)[:200]}】")
    if str(body.get("code", "")) == "000000" or body.get("success") is True:
        data = body.get("data")
        cid = (data.get("id") or data.get("commentId") or data.get("contentId")) if isinstance(data, dict) else None
        logger.info(f"[发送/校验] 辅助接口判定发送成功 | 评论ID: 【{cid}】 | 结果: [Success]")
        return True, cid
    return False, None


# ==============================================================================
#                              核心：提交一条评论
# ==============================================================================

def _submit_comment(page, editor_container, comment, image_path_list=None, url_info_list=None, chart_info=None,
                    sentiment=None, text_list=None):
    """在隔离作用域内跑完发帖全链路。"""
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
                expect(editor_container.locator(
                    "img[src^='blob'],img[src^='http'],img[src^='data:'],[class*='thumb'],[class*='preview'] img"
                ).nth(len(valid_images) - 1)).to_be_visible(timeout=15000)
                mounted = True
            except Exception:
                page.wait_for_timeout(3500)
            logger.info(f"[编辑器/图片] 图片批量挂载完毕 | 数量: 【{len(valid_images)}】张 | "
                        f"缩略图全可见: 【{mounted}】 | 结果: [Success]")
        except Exception as e:
            logger.warning(f"[编辑器/图片] 批量注入失败，可能前端缺失 multiple 属性。进入逐张降级保序上传模式 | "
                           f"错误: 【{str(e)[:150]}】")
            try:
                for idx, img_path in enumerate(valid_images):
                    file_input.set_input_files(img_path, timeout=5000)
                    page.wait_for_timeout(1000)
                logger.info(f"[编辑器/图片] 逐张降级上传完毕 | 数量: 【{len(valid_images)}】张 | 结果: [Success]")
            except Exception as sub_e:
                logger.warning(f"[编辑器/图片] 逐张降级上传依然失败，本条自动降级为纯文本 | "
                               f"致命错误: 【{str(sub_e)[:150]}】")

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
            any(x.get("text", "").strip() for x in valid_text_list)
            if valid_text_list else bool(comment.strip())
        )
        if not current_text and expect_non_empty:
            logger.warning("[编辑器/正文] 正文被静默清空，触发一次全量补录 | 可能原因: 【前端框架重渲染 / 浮层抢焦点】")
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

    # 开始点击发送
    _dismiss_overlays(page, aggressive=False, desc="pre-send")
    send_btn_cands = [
        editor_container.locator("button").filter(has_text=RE_SEND_EXACT).first,
        editor_container.get_by_role("button", name=RE_SEND).first,
    ]

    try:
        send_button = _pick_trusted_send_button(send_btn_cands) or _interact_fallback_locators(
            send_btn_cands, action="wait", timeout=5000, desc="发送按钮")
        expect(send_button).to_be_enabled(timeout=8000)
    except PlaywrightTimeoutError:
        _forensics(page, "btn_disabled_fatal", {
            "btn_html": send_button.evaluate("el => el.outerHTML") if send_button else "none",
            "editor_text": real_editor.inner_text() if real_editor else "none"
        })
        raise Exception(
            "发送按钮被找到，但持续处于禁用(Disabled)状态。前端组件未能识别输入内容。已阻断强制点击，防止死等接口。")
    except Exception as e:
        logger.warning(f"[发送/按钮] 发生意外的定位错误: {str(e)[:150]}")
        send_button = send_btn_cands[0]

    text_before, media_before = _snapshot_editor(real_editor)
    api_success, comment_id = False, None

    logger.info(f"[发送/提交] 触发发送并挂载接口监听 | 主判据: <pgc/content/add> "
                f"| 发送前编辑器: 【文本{text_before}字 / 媒体{media_before}个】")
    watcher = _ApiWatcher(page)
    try:
        clicked = False
        for attempt in range(3):
            hit = _hit_test(page, send_button)
            if not hit.get("ok"):
                logger.warning(
                    f"[发送/提交] 发送按钮不可命中，先清障再重试 | 第【{attempt + 1}】次 | 详情: 【{_fmt_hit(hit)}】")
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
            RE_FOLLOW_AND_REPLY = re.compile(r"关注并回复|Follow and [R|r]eply", re.IGNORECASE)
            follow_reply_btn = page.locator("div[role='dialog'], [class*='modal']").get_by_role(
                "button", name=RE_FOLLOW_AND_REPLY
            ).first

            if follow_reply_btn.is_visible(timeout=1500):
                logger.info("[发送/权限] 触发了「仅限关注者评论」限制，正在自动点击【关注并回复】")
                watcher.primary.clear()
                watcher.secondary.clear()
                follow_reply_btn.click(timeout=3000)
                page.wait_for_timeout(500)
        except Exception as bypass_e:
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
        logger.info(f"[发送/校验] DOM 兜底比对通过（输入框已被大幅清空） | 文本: 【{text_before}→{text_after}】 "
                    f"| 媒体: 【{media_before}→{media_after}】 | 结果: [Success]")
        return comment_id

    report_guard_hits(page, "send-fail")
    _forensics(page, "send_no_effect", {
        "text_before": text_before, "text_after": text_after,
        "media_before": media_before, "media_after": media_after,
        "send_btn_hit_test": _hit_test(page, send_button),
    })
    raise Exception(f"发送已点击但输入框未清空且接口无成功响应（文本 {text_before}→{text_after}，"
                    f"媒体 {media_before}→{media_after}），疑似发送按钮失效、内容被前端校验拦下或网络堵塞。")


# ==============================================================================
#                              URL / 帖子ID 解析
# ==============================================================================

def extract_binance_post_id(post_url):
    """
    从帖子 URL 提取纯数字 post_id（严格路径优先，失败退化为宽松匹配再退化为数字尾段）。
    """
    raw = str(post_url or "").strip()
    if not raw:
        return None
    try:
        url = raw if raw.startswith(("http://", "https://")) else "https://" + raw
        parsed = urlparse(url)
        if re.search(r"(^|\.)binance\.(com|info|me)$", (parsed.hostname or "").lower()):
            m = (re.search(r"/square/post/(?:[a-zA-Z\-]+/)?(\d+)", parsed.path)
                 or re.search(r"/post/(\d+)", parsed.path))
            if m:
                return m.group(1)

        m = re.search(r"/post/(?:[a-zA-Z\-]+/)?(\d+)", raw)
        if m:
            return m.group(1)
        tail = raw.split("?")[0].strip("/").split("/")[-1]
        return tail if tail.isdigit() else None
    except Exception:
        return None


# ==============================================================================
#                                  主控入口辅助
# ==============================================================================

def _alert_failure_scene(error_msg):
    """失败终局提示：响铃 + 聚合打印失败原因。"""
    sys.stdout.write("\a")
    sys.stdout.flush()
    logger.error(f"\n{'=' * 60}\n[中断/Halt] 🚨 任务异常终止（已保存故障现场，不阻塞等待人工）"
                 f"\n[中断/Halt] 失败原因: 【{error_msg}】\n{'=' * 60}")


def _verify_page_ready(page, response, post_id):
    """
    落地页四重体检，返回错误描述(str) 或 None(通过)。
    """
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
                return (f"页面发生重定向，目标帖子已被删除或失效 | 原帖子ID: {post_id} | 现落地URL: {landed}")
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
    """
    精准锁定广场主页最顶部的发帖区容器。
    """
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
                "xpath=ancestor::div[has(div[@contenteditable='true'] or input[@type='file'])][1]")
            if container.count() > 0:
                logger.info("[定位/DOM] 已锁定主页发帖区作用域 (基于占位符溯源)")
                return container.first
    except Exception:
        pass

    logger.warning("[定位/DOM] 精确定位与溯源均未命中，降级为锁定页面首个富文本容器")
    return page.locator('div:has(div[contenteditable="true"].ProseMirror), div:has(input[type="file"])').first


# ==============================================================================
#                          会话管理 / 凭证提取 / 人工接管
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
                    p, USER_DATA_DIR,
                    args=['--disable-blink-features=AutomationControlled', '--start-maximized'],
                    hide_automation=False,
                    debug=True,  # 🟢 修复：显式传参以启用有头模式
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


def _launch_persistent(p, user_data_dir, args, viewport=None, hide_automation=True, debug=True):
    """
    统一的持久化上下文启动口，已修复 DevTools 快捷键失效问题
    🟢 修复：将 debug 默认值改为 True，并确保 headless 参数正确响应 debug
    🟢 并发加固：在底层启动口绑定可重入目录锁，确保任何直接或间接调用都受到独占保护并在 close 时释放。
    """
    fallback_name = Path(str(user_data_dir).replace("\\", "/")).name or "biance_default"
    user_data_dir = resolve_cross_platform_path(user_data_dir, DEFAULT_SESSION_ROOT / fallback_name)
    os.makedirs(user_data_dir, exist_ok=True)

    lock_obj = _get_user_data_lock_obj(user_data_dir)
    lock_obj.acquire(timeout=USER_DATA_LOCK_TIMEOUT_SEC)

    try:
        browser_channel = get_config("browser_channel", default="chrome")
        kwargs = {
            "user_data_dir": user_data_dir,
            "headless": not debug,  # 当 debug=True 时 headless=False（有头模式），debug=False 时 headless=True（无头模式）
            "args": args
        }
        if browser_channel and str(browser_channel).strip().lower() not in ("", "chromium", "none"):
            kwargs["channel"] = str(browser_channel).strip()

        # 🚀 核心修复 1：彻底关闭 Playwright 的内部视口模拟机制
        if viewport:
            kwargs["viewport"] = viewport
        else:
            # 必须显式传入 no_viewport=True，否则 Playwright 会默认 800x600 模拟并劫持输入
            kwargs["no_viewport"] = True

        # 🚀 核心修复 2：剔除那些会破坏 Chrome 原生快捷键的默认参数
        ignored_args = []
        if hide_automation:
            ignored_args.append("--enable-automation")

        # 恢复 Chrome 底层负责路由部分快捷键的组件
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

    # 包装 context.close，确保即使外部直接调用 _launch_persistent 也能在关闭时精确释放锁引用
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

    # 🚀 核心修复 3：主动赋予环境剪贴板读写权限，防止 Ctrl+C / Ctrl+V 被沙箱静默拦截
    try:
        context.grant_permissions(['clipboard-read', 'clipboard-write'])
    except Exception as e:
        logger.warning(f"[环境] 剪贴板权限授予失败，可能影响 F12 粘贴: {e}")

    return context


# ==============================================================================
#                                  主控入口 (发帖)
# ==============================================================================
def create_binance_post(content="", image_path_list=None, user_data_dir=USER_DATA_DIR,
                        url_info_list=None, chart_info=None, sentiment=None, debug=True, text_list=None):
    """
    主控入口：调度浏览器打开广场主页并执行【独立发帖】全流程。
    复用底层高度解耦的 _submit_comment 动作链路。
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
    has_chart = "是" if chart_info else "否"
    square_url = "https://www.binance.com/zh-CN/square"
    logger.info(f"\n{'=' * 70}\n[任务/Main] 启动自动化发帖 | URL: <{square_url}> "
                f"| 正文: 【{text_summary}】 | 图片数量: 【{img_count}】张 "
                f"| 链接数: 【{len(url_info_list or [])}】 | 包含图表: 【{has_chart}】 "
                f"| 情绪标签: 【{sentiment or '无'}】 "
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
                        debug=debug,  # 🟢 修复：将外层的 debug 透传进底层
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
                        page, editor_container, content, image_path_list, url_info_list, chart_info,
                        sentiment, text_list=text_list
                    )

                    logger.info(f"[任务/Main] 帖子发布成功 | 帖子关联ID: 【{post_id}】 | 结果: [Success]")
                    return None, True, post_id

                except BusinessErrorException as biz_e:
                    error_info = f"[业务拦截] {biz_e}"
                    logger.error(f"[任务/Main] 发帖被服务端业务规则阻断 | 原因: 【{error_info}】")
                    return error_info, False, None

                except Exception as e:
                    is_timeout = isinstance(e, PlaywrightTimeoutError)
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


# ==============================================================================
#                                  主控入口 (评论)
# ==============================================================================
def comment_on_binance_post(post_url, comment="", image_path_list=None, user_data_dir=USER_DATA_DIR,
                            url_info_list=None, chart_info=None, sentiment=None, debug=False, text_list=None):
    """
    主控入口：调度浏览器打开帖子并执行评论全流程。
    入参形貌: url_info_list = [{"text": str, "url": str}, ...]
              chart_info = {"coin": "BTC", "bridge": "USDT", "type": "future"}
              text_list = [{"text": "大家好", "delay": 5}, ...]
    返回 Tuple(错误信息(str|None), 是否成功(bool), 评论ID(str|None))。
    """
    if not os.path.isdir(user_data_dir):
        return f"缺少用户环境: {user_data_dir}，请先执行登录", False, None

    post_id = extract_binance_post_id(post_url)
    valid_text_list = _normalize_text_list(text_list)
    text_summary = (
        f"text_list({len(valid_text_list)}项/{sum(len(x['text']) for x in valid_text_list)}字)"
        if valid_text_list else f"{len(str(comment or ''))}字"
    )
    img_count = len(image_path_list) if isinstance(image_path_list, list) else (1 if image_path_list else 0)
    has_chart = "是" if chart_info else "否"
    logger.info(f"\n{'=' * 70}\n[任务/Main] 启动自动化评论 | 帖子ID: 【{post_id}】 | URL: <{post_url}> "
                f"| 正文: 【{text_summary}】 | 图片数量: 【{img_count}】张 "
                f"| 链接数: 【{len(url_info_list or [])}】 | 包含图表: 【{has_chart}】 "
                f"| 情绪标签: 【{sentiment or '无'}】 "
                f"| 模式: 【{'debug可见' if debug else '离屏后台'}】"
                f"\n{'=' * 70}")

    anti_freeze_args = ['--disable-restore-session-state', '--no-default-browser-check']
    offscreen_args = [
                         '--disable-blink-features=AutomationControlled', '--disable-gpu',
                         '--window-position=-10000,-10000', '--no-sandbox', '--disable-dev-shm-usage',
                         '--disable-renderer-backgrounding', '--disable-background-timer-throttling',
                         '--disable-backgrounding-occluded-windows', '--disable-features=CalculateNativeWinOcclusion',
                         '--disable-breakpad',
                         '--force-device-scale-factor=1',
                         '--hide-scrollbars',
                     ] + anti_freeze_args

    debug_args = [
                     '--disable-blink-features=AutomationControlled', '--start-maximized',
                     '--window-position=0,0'
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
                        debug=debug,  # 🟢 修复：将外层的 debug 透传进底层
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
                        logger.warning(f"[导航/Nav] 落地页体检未通过，安全跳过本任务 | 帖子ID: 【{post_id}】 "
                                       f"| 原因: 【{page_err}】 | 结果: [Failed]")
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
                        page, editor_container, comment, image_path_list, url_info_list,
                        chart_info, sentiment, text_list=text_list
                    )
                    logger.info(
                        f"[任务/Main] 评论发送成功 | 帖子ID: 【{post_id}】 | 评论ID: 【{comment_id}】 | 结果: [Success]")
                    return None, True, comment_id

                except BusinessErrorException as biz_e:
                    error_info = f"[业务拦截] {biz_e}"
                    logger.error(f"[任务/Main] 发帖被服务端业务规则阻断，无需重试 | 帖子ID: 【{post_id}】 "
                                 f"| 原因: 【{error_info}】 | 结果: [Failed]")
                    return error_info, False, None

                except Exception as e:
                    is_timeout = isinstance(e, PlaywrightTimeoutError)
                    if is_timeout:
                        error_info = f"[元素/网络超时] {e}"
                        logger.error(f"[任务/Main] 元素等待或网络请求超时 | 帖子ID: 【{post_id}】 "
                                     f"| 可能原因: 【引导浮层拦截 / 帖子软删除 / 页面卡顿 / 网络抖动】 "
                                     f"| 详情: 【{error_info[:200]}】 | 结果: [Failed]")
                    else:
                        error_info = f"[{type(e).__name__}] {e}\n[Traceback]:\n{traceback.format_exc()}"
                        logger.error(f"[任务/Main] 执行中发生未预期异常 | 帖子ID: 【{post_id}】 "
                                     f"| 摘要: 【{str(e)[:200]}】 | 结果: [Failed]")

                    if context and context.pages:
                        try:
                            pg = context.pages[0]
                            report_guard_hits(pg, "timeout" if is_timeout else "error")
                            _forensics(pg, ("timeout" if is_timeout else "error") + f"_{post_id}",
                                       {"post_url": post_url, "error": error_info[:2000]})
                        except Exception as s_e:
                            logger.warning(f"[任务/Debug] 故障现场保存失败 | 可能原因: 【磁盘不可写或页面已销毁: {s_e}】")

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
        error_info = (f"[CoreEngineCrash] Playwright 底层启动/运行发生系统级崩溃:\n{core_e}\n\n"
                      f"[Traceback]:\n{traceback.format_exc()}")
        logger.error(f"[任务/Main] Playwright 核心框架崩溃，浏览器引擎未能启动 "
                     f"| 可能原因: 【Chrome 版本不匹配 / User Data 目录被其它进程占用 / 磁盘权限不足】 "
                     f"| 详情: 【{str(core_e)[:200]}】")
        return error_info, False, None


# ==============================================================================
#                                  人工接管函数
# ==============================================================================
def open_browser_for_manual_use(user_data_dir, home_url="https://www.binance.com/zh-CN"):
    """启动可见浏览器交由人工自由操作（含 window-position 归零 + 置顶，防历史屏幕外坐标缓存）。"""
    logger.info(f"\n{'=' * 60}\n[人工/Manual] 启动本地浏览器交接控制权 | 目录: <{user_data_dir}>\n{'=' * 60}")
    with _user_data_dir_lock(user_data_dir, timeout=USER_DATA_LOCK_TIMEOUT_SEC):
        with sync_playwright() as p:
            context = None
            try:
                context = _launch_persistent(
                    p, user_data_dir,
                    args=['--disable-blink-features=AutomationControlled', '--start-maximized', '--window-position=0,0'],
                    debug=True,  # 🟢 修复：显式传参以启用有头模式
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


def get_auth_tokens_robust(user_data_dir, debug=False):
    """
    以真实浏览器请求为样本提取脱机 API 凭证，同时顺带提取 User Data。
    """
    if not os.path.exists(user_data_dir):
        logger.warning(f"[凭证/Auth] 环境目录不存在，无法提取 | 目录: <{user_data_dir}>")
        return None, None, None

    visit_url = "https://www.binance.com/zh-CN/square/profile/insights_anchor"
    api_keyword = "pgc/user/client"
    user_api_keyword = "pgc/user?getFollowCount"

    logger.info(
        f"[凭证/Auth] 启动浏览器提取凭证(Headed 必须可见) | 目录: <{user_data_dir}> | 主拦截: <{api_keyword}> | 辅拦截: <{user_api_keyword}>")

    try:
        with _user_data_dir_lock(user_data_dir, timeout=USER_DATA_LOCK_TIMEOUT_SEC):
            with sync_playwright() as p:
                context = None
                try:
                    context = _launch_persistent(
                        p, user_data_dir,
                        args=['--disable-blink-features=AutomationControlled'],
                        viewport={'width': 1280, 'height': 720},
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
                            f"[凭证/Auth] 提取失败：捕获到请求但无任何合法凭据 | 目录: <{user_data_dir}> | 请求: 【{req.method} {req.url}】 "
                            f"| 排查方向: 【浏览器当前是否处于登录态】")
                        return None, None, None

                    has_p20t = "p20t=" in cookie
                    level = logger.info if (has_p20t and csrf) else logger.warning
                    level(f"[凭证/Auth] 凭证提取完成 | 目录: <{user_data_dir}> | 来源: <{source}> | CSRF: 【{str(csrf)[:8]}...】 "
                          f"| Cookie长度: 【{len(cookie)}】 | 含核心 p20t: 【{has_p20t}】"
                          f"{'' if (has_p20t and csrf) else ' | 提醒: 缺失 p20t 或 CSRF，后续 API 很可能 401/400'}")

                    user_data = None
                    for res in user_api_responses:
                        try:
                            if res.ok:
                                json_body = res.json()
                                if json_body and json_body.get("success"):
                                    user_data = json_body.get("data")
                                    logger.info(
                                        f"[凭证/Auth] 成功捕获用户信息 | 目录: <{user_data_dir}> | 昵称: 【{user_data.get('displayName')}】 | UID: 【{user_data.get('squareUid')}】")
                                    break
                        except Exception as e:
                            logger.debug(
                                f"[凭证/Auth] 尝试解析用户信息响应时出现异常(已忽略) | 目录: <{user_data_dir}> | 详情: {e}")

                    return cookie, csrf, user_data

                except PlaywrightTimeoutError:
                    logger.warning(f"[凭证/Auth] 提取失败：20s 内未捕获到目标接口 <{api_keyword}> "
                                   f"| 目录: <{user_data_dir}> | 排查方向: 【浏览器打开时是否已登录 / 页面是否被风控拦截】")
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


def _inject_chart(page, editor_container, chart_info):
    """
    注入图表卡片逻辑。
    """
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
        chart_icon = _interact_fallback_locators([
            editor_container.locator('svg path[d^="M17.123 1.803"]').locator(".."),
            editor_container.locator('.trade-widget-icon, [class*="widget-icon"]').first
        ], action="click", timeout=5000, desc="添加图表图标")
        page.wait_for_timeout(500)

        search_input = _interact_fallback_locators([
            page.locator('input[aria-label*="搜索币种"], input[placeholder*="搜索币种"]').first,
            page.locator('.bn-textField-input').first
        ], action="wait", timeout=5000, desc="图表搜索框")

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
            if (str(item.get("coin", "")).upper() == coin.upper() and
                    str(item.get("bridge", "")).upper() == bridge.upper() and
                    str(item.get("type", "")).lower() == c_type.lower()):
                target_index = i
                break

        if target_index == -1:
            raise Exception(f"接口返回的数据中未找到匹配 {coin}-{bridge}-{c_type} 的项")

        logger.info(f"[编辑器/图表] 匹配成功 | 目标在列表中的索引为: 【{target_index}】")

        list_items = page.locator('div.overflow-y-auto > div.cursor-pointer')
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
    """
    注入情绪（看多/看空）图标逻辑。
    """
    if not sentiment or sentiment not in ["看多", "看空"]:
        return

    logger.info(f"[编辑器/情绪] 准备点击情绪标签 | 目标: 【{sentiment}】")
    try:
        svg_locators = editor_container.locator('svg').filter(has=page.locator('path[d^="M20.91 6.861"]'))
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
            logger.info(f"[编辑器/情绪] 情绪标签点击成功 | 结果: [Success]")
        else:
            logger.warning(
                f"[编辑器/情绪] 页面存在情绪图标，但未找到匹配 【{sentiment}】 状态的图标元素 | 结果: [Skipped]")

    except Exception as e:
        logger.warning(f"[编辑器/情绪] 情绪标签点击异常，已跳过 | 原因: 【{str(e)[:150]}】 | 结果: [Skipped]")


# ==============================================================================
#                                   启动入口
# ==============================================================================
if __name__ == "__main__":
    # 其他可选入口（按需取消注释）：
    # login_and_save_session()
    open_browser_for_manual_use(USER_DATA_DIR)
    # cookies, csrf, udata = get_auth_tokens_robust(USER_DATA_DIR)

    # test_url = "https://www.binance.com/zh-CN/square/post/309692475255842"
    # test_msg = "少即是多，慢即是快。同频共振！🚀"
    # test_img = r"E:\chrome\1759239193.png"
    # test_links = [{"text": "带单", "url": "https://www.binance.com/zh-CN/square/post/309692475255842"}]
    #
    # err, success, c_id = comment_on_binance_post(
    #     post_url=test_url, comment=test_msg, image_path_list=test_img, url_info_list=test_links, debug=True
    # )
    #
    # if success:
    #     logger.info(f"\n[结果/Final] 🎉 ======== 自动评论任务圆满成功 ======== | 评论ID: 【{c_id}】")
    # else:
    #     logger.error(f"\n[结果/Final] ❌ ======== 任务失败 ======== | 最终追溯:\n{err}")
    #
    # err, success, c_id = create_binance_post(
    #     content=test_msg, image_path_list=test_img, url_info_list=test_links, debug=True
    # )
    #
    # if success:
    #     logger.info(f"\n[结果/Final] 🎉 ======== 自动发帖任务圆满成功 ======== | 发帖ID: 【{c_id}】")
    # else:
    #     logger.error(f"\n[结果/Final] ❌ ======== 任务失败 ======== | 最终追溯:\n{err}")

    test_content = "$BTC \n\n交易所的估值天花板，正在被真实的线下消费捅破。\n\n何一确认：币安支付现已接入日本全国 PayPay 商户。这不是小圈子里的链上自嗨，而是把加密支付直接接进了主流线下商业网络。\n\n多数人看 BNB，眼光始终停留在手续费折扣和打新挖矿。但当它跳出交易所围墙、切入真实世界的支付场景时，流动性就不再只是投机筹码，而是有了实体刚需做托底。\n\n靠发预告撑不起长线，能在街头实体里跑通闭环的生态，基本盘只会越来越硬。"

    target_chart = {
        "coin": "BTC",
        "bridge": "USDT",
        "type": "future"
    }

    test_links = [
        {"text": "更多行情", "url": "https://www.binance.com"}
    ]
    test_images = [r"E:\chrome\1759239193.png"]

    text_list = [
        {"text": "$BTC", "delay": 2},

        {"text": "大家好，欢迎来到币安广场！", "delay": 2},

    ]

    err, success, p_id = create_binance_post(
        content=test_content,
        image_path_list=test_images,
        url_info_list=test_links,
        chart_info=target_chart,
        user_data_dir=USER_DATA_DIR,
        debug=True
    )

    if success:
        logger.info(f"\n[结果/Final] 🎉 ======== 带图表发帖任务成功 ======== | 帖子ID: 【{p_id}】")
    else:
        logger.error(f"\n[结果/Final] ❌ ======== 带图表发帖任务失败 ======== | 错误原因:\n{err}")