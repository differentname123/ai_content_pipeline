# -*- coding: utf-8 -*-
"""
双平台（Binance / OKX）内容采集、AI 生成与自动发布主入口 (`parser/article_format_logical.py`)

日常使用：
- 启停子任务：直接修改下方 `TASK_SWITCHES` 后右键运行本文件；
- 调整冷却时长、浏览量门槛、字数上限、互动开关等参数：统一在 `parser/config.py` 修改。
"""
import argparse
import signal
import sys
import threading
from pathlib import Path
from typing import Dict, Optional, Sequence

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# ==============================================================================
# 一、任务启停控制面板（直接改 True / False，右键运行即可）
#     更多阈值、冷却与互动参数统一见 parser/config.py
# ==============================================================================
TASK_SWITCHES: Dict[str, bool] = {
    "fetch_posts": True,           # 双平台热榜/推荐流原帖采集
    "fetch_and_screen": True,      # 分析帖纯文本初筛 + 多模态论据精提
    "viral_screen": True,          # 娱乐高流量一阶段纯文本粗筛
    "single_analysis_gen": True,   # 单币分析文章生成
    "single_analysis_pub": True,   # 单币分析文章发布（总编擂台）
    "viral_gen": True,             # 娱乐高流量二阶段洗稿生成
    "viral_pub": True,             # 娱乐高流量文章发布（总编擂台）
    "hudong": False,               # 评论区点赞/互评（默认关闭）
    "delete_old_post": False,      # 逆向行情/超期历史清理（默认关闭）
}

from common.common_utils import PLATFORM_ACCOUNTS, PLATFORM_BINANCE, PLATFORM_OKX
from parser.article_generator import (
    generate_analysis_articles,
    generate_analysis_articles_once,
    generate_viral_articles,
    generate_viral_articles_once,
    screen_viral_posts,
    screen_viral_posts_once,
)
from parser.maintenance_tasks import _delete_old_posts_once, _hudong_once, delete_old_post, hudong
from parser.mlu_pipeline import format_image_article, format_image_article_once
from parser.platform_adapters import logger
from parser.post_ingestion import fetch_all_hot_posts, fetch_all_hot_posts_once
from parser.publish_state import default_state_store
from parser.publisher_arena import (
    auto_publish_articles,
    auto_publish_viral_articles,
    publish_analysis_once,
    publish_viral_once,
)

# 各任务对应的 (常驻循环函数, 单轮执行函数)
TASK_REGISTRY = {
    "fetch_posts": (fetch_all_hot_posts, fetch_all_hot_posts_once),
    "fetch_and_screen": (format_image_article, format_image_article_once),
    "viral_screen": (screen_viral_posts, screen_viral_posts_once),
    "single_analysis_gen": (generate_analysis_articles, generate_analysis_articles_once),
    "single_analysis_pub": (auto_publish_articles, publish_analysis_once),
    "viral_gen": (generate_viral_articles, generate_viral_articles_once),
    "viral_pub": (auto_publish_viral_articles, publish_viral_once),
    "hudong": (hudong, _hudong_once),
    "delete_old_post": (delete_old_post, _delete_old_posts_once),
}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Binance / OKX 内容采编与发布主控流水线")
    parser.add_argument("--tasks", nargs="*", help="临时覆盖启用任务名（默认读取 TASK_SWITCHES）")
    parser.add_argument("--once", action="store_true", help="对选定任务顺序跑一轮后退出")
    parser.add_argument("--dry-run", action="store_true", help="只读查看启用任务与各账号发帖冷却状态")
    args = parser.parse_args(argv)

    if args.tasks:
        task_keys = [name.strip() for item in args.tasks for name in str(item).split(",") if name.strip()]
        unknown = [k for k in task_keys if k not in TASK_REGISTRY]
        if unknown:
            raise ValueError(f"未知任务名: {unknown}，可选值: {list(TASK_REGISTRY)}")
    else:
        task_keys = [key for key, enabled in TASK_SWITCHES.items() if enabled]

    if args.dry_run:
        logger.info(f"[主控/DryRun] 选定任务清单: {task_keys}")
        for plat in (PLATFORM_BINANCE, PLATFORM_OKX):
            accs = PLATFORM_ACCOUNTS.get(plat) or []
            ok_an, blk_an = default_state_store.get_eligible_accounts(plat, accs, is_viral=False)
            ok_vi, blk_vi = default_state_store.get_eligible_accounts(plat, accs, is_viral=True)
            logger.info(f"[主控/DryRun] {plat} | 分析可用={ok_an} 阻塞={blk_an} | 流量可用={ok_vi} 阻塞={blk_vi}")
        return 0

    if args.once:
        logger.info(f"[主控/单轮模式] 顺序执行任务: {task_keys}")
        for key in task_keys:
            TASK_REGISTRY[key][1]()
        return 0

    stop_event = threading.Event()
    for sig_name in ("SIGINT", "SIGTERM"):
        if hasattr(signal, sig_name):
            try:
                signal.signal(getattr(signal, sig_name), lambda *_: stop_event.set())
            except Exception:
                pass

    def _run(key: str) -> None:
        try:
            TASK_REGISTRY[key][0](stop_event=stop_event)
        except Exception:
            logger.exception(f"❌ [任务/退出] 后台任务异常结束 | 任务: [{key}]")
            raise

    logger.info(f"[主控/启动] 激活任务数: [{len(task_keys)}] | 任务清单: {task_keys}")
    threads = [threading.Thread(target=_run, args=(k,), name=k) for k in task_keys]
    for t in threads:
        t.start()
    while any(t.is_alive() for t in threads):
        for t in threads:
            t.join(timeout=1.0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
