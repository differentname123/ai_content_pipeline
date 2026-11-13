# -*- coding: utf-8 -*-
"""
一键串联编排入口 (`x_growth/comment_pipeline.py`)

高内聚 · 低耦合分工：
- 拉取帖子归 `x_browser_collector.py`（纯浏览器被动捕获并 100% 存入 `harvested_social_posts`，可单独运行）；
- 生成评论并盲测打分归 `comment_generator.py`（纯查库按 **15 条/波** 循环处理直至库内待评合格帖 100% 清零，可单独运行）；
- 本文件仅作为薄编排层，顺序调用「1A 拉取落库 -> 1B 循环清零生成」，方便日常直接右键一键常驻挂机。
"""
import json
import random
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Tuple

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from x_growth.comment_generator import generate_all_pending_comments
from x_growth.growth_config import (
    COLLECTOR_DEFAULTS,
    GENERATOR_DEFAULTS,
    PIPELINE_SCHEDULE_CONFIG,
    SUPPORTED_COMMENT_LANGUAGES,
    TZ_CN,
    get_growth_logger,
)
from x_growth.x_browser_collector import collect_and_save_posts


def run_comment_pipeline(
    user_data_dir: str = PIPELINE_SCHEDULE_CONFIG["user_data_dir"],
    *,
    min_followers: int = COLLECTOR_DEFAULTS["min_followers"],
    max_followers: int = COLLECTOR_DEFAULTS["max_followers"],
    browser_visible: bool = COLLECTOR_DEFAULTS["browser_visible"],
    home_scroll_rounds: int = COLLECTOR_DEFAULTS["home_scroll_rounds"],
    thread_clicks: int = COLLECTOR_DEFAULTS["thread_clicks_per_run"],
    profile_visits: int = COLLECTOR_DEFAULTS["profile_visits_per_run"],
    wave_size: int = GENERATOR_DEFAULTS["wave_size"],
    comment_batch_size: int = GENERATOR_DEFAULTS["comment_batch_size"],
    allowed_languages: Sequence[str] = GENERATOR_DEFAULTS["allowed_languages"],
    force_rescore: bool = False,
) -> Dict[str, Any]:
    """
    轻量串联执行一轮：
    1. 调 `collect_and_save_posts` 拉取帖子全量入库；
    2. 调 `generate_all_pending_comments` 按 `wave_size`（默认 15 条/波）循环生成并盲评，直到库内 `<24h` 合格待评帖 100% 清零。
    """
    t0 = time.perf_counter()
    session_path = str(Path(user_data_dir).expanduser().resolve())

    collect_res = collect_and_save_posts(
        user_data_dir=session_path,
        min_followers=min_followers,
        max_followers=max_followers,
        browser_visible=browser_visible,
        home_scroll_rounds=home_scroll_rounds,
        thread_clicks=thread_clicks,
        profile_visits=profile_visits,
        allowed_languages=allowed_languages,
    )
    if collect_res.get("status") != "ok":
        return {
            "status": collect_res.get("status"),
            "user_data_dir": session_path,
            "account_warning": collect_res.get("account_warning"),
            "browsed_posts_saved": 0,
            "browsed_new_in_db": 0,
            "waves_executed": 0,
            "target_posts_selected": 0,
            "newly_scored_posts": 0,
            "skipped_posts": 0,
            "elapsed_seconds": round(time.perf_counter() - t0, 2),
            "items": [],
        }

    extra_handles = [collect_res["self_handle"]] if collect_res.get("self_handle") else None
    gen_res = generate_all_pending_comments(
        min_followers=min_followers,
        max_followers=max_followers,
        allowed_languages=allowed_languages,
        wave_size=wave_size,
        comment_batch_size=comment_batch_size,
        extra_self_handles=extra_handles,
        force_rescore=force_rescore,
    )

    return {
        "status": "ok",
        "user_data_dir": session_path,
        "account_warning": None,
        "browsed_posts_saved": int(collect_res.get("browsed_posts_saved", 0)),
        "browsed_new_in_db": int(collect_res.get("browsed_new_in_db", 0)),
        "waves_executed": int(gen_res.get("waves_executed", 0)),
        "target_posts_selected": int(gen_res.get("target_posts_selected", 0)),
        "newly_scored_posts": int(gen_res.get("newly_scored_posts", 0)),
        "skipped_posts": int(gen_res.get("skipped_posts", 0)),
        "elapsed_seconds": round(time.perf_counter() - t0, 2),
        "items": gen_res.get("items") or [],
    }


def run_comment_pipeline_daemon(
    user_data_dir: str = PIPELINE_SCHEDULE_CONFIG["user_data_dir"],
    interval_seconds: Optional[int] = None,
    jitter_seconds: Optional[Tuple[int, int]] = None,
) -> None:
    """常驻定时串联守护：每轮先拉帖入库，再按 15 条/波把库内合格待评帖 100% 清零生成完。"""
    logger = get_growth_logger()
    base_interval = max(60, int(interval_seconds or PIPELINE_SCHEDULE_CONFIG.get("interval_seconds", 3600)))
    raw_jitter = jitter_seconds if jitter_seconds is not None else PIPELINE_SCHEDULE_CONFIG.get("jitter_seconds", (120, 600))
    if isinstance(raw_jitter, (list, tuple)) and len(raw_jitter) == 2:
        j_low, j_high = max(0, int(raw_jitter[0])), max(0, int(raw_jitter[1]))
        if j_low > j_high:
            j_low, j_high = j_high, j_low
    else:
        j_low, j_high = 0, 0

    session_path = str(Path(user_data_dir).expanduser().resolve())
    logger.info(
        f"⏰ [CommentPipeline/常驻串联任务启动] 采集目录={session_path} | "
        f"周期={base_interval}s + 抖动({j_low}~{j_high}s) | 每波={GENERATOR_DEFAULTS['wave_size']}条直至清零 | "
        f"语种={list(SUPPORTED_COMMENT_LANGUAGES)}"
    )

    round_no = 0
    while True:
        round_no += 1
        logger.info(f"========== [CommentPipeline/轮次 #{round_no}] 开始执行 ==========")
        try:
            res = run_comment_pipeline(session_path, force_rescore=False)
            if res.get("status") != "ok":
                logger.error(f"⚠️ [CommentPipeline/轮次 #{round_no} 待处理] {res.get('account_warning')}")
            else:
                top_preview = [
                    f"{it['author']}({it['language']}|{it['best_score']}分): {it['best_comment']}"
                    for it in (res.get("items") or [])[:3]
                ]
                logger.info(
                    f"✅ [CommentPipeline/轮次 #{round_no} 完成] 顺路入库={res.get('browsed_posts_saved', 0)}条"
                    f"(新{res.get('browsed_new_in_db', 0)}) | 生成波次={res.get('waves_executed', 0)}波 | "
                    f"清零合格帖={res.get('target_posts_selected', 0)}条 -> 新增可发评论={res.get('newly_scored_posts', 0)}条 | "
                    f"耗时={res.get('elapsed_seconds', 0)}s"
                    + (f" | 预览={top_preview}" if top_preview else "")
                )
        except KeyboardInterrupt:
            logger.info("[CommentPipeline/常驻任务退出] 收到手动中断信号，安全停止。")
            break
        except Exception as exc:
            logger.exception(f"❌ [CommentPipeline/轮次 #{round_no} 异常] 将在下轮重试: {exc}")

        sleep_sec = base_interval + (random.randint(j_low, j_high) if j_high > 0 else 0)
        next_run_time = (datetime.now(TZ_CN) + timedelta(seconds=sleep_sec)).strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"💤 [CommentPipeline/常驻休眠] 休眠 {sleep_sec} 秒，下一轮将于 [{next_run_time}] 执行...")
        try:
            time.sleep(sleep_sec)
        except KeyboardInterrupt:
            logger.info("[CommentPipeline/常驻任务退出] 休眠期间收到中断信号，安全停止。")
            break


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    target_dir = str(PIPELINE_SCHEDULE_CONFIG["user_data_dir"])
    if PIPELINE_SCHEDULE_CONFIG.get("continuous_loop", True):
        run_comment_pipeline_daemon(
            user_data_dir=target_dir,
            interval_seconds=PIPELINE_SCHEDULE_CONFIG.get("interval_seconds", 3600),
            jitter_seconds=PIPELINE_SCHEDULE_CONFIG.get("jitter_seconds", (120, 600)),
        )
    else:
        res = run_comment_pipeline(
            user_data_dir=target_dir,
            force_rescore=bool(PIPELINE_SCHEDULE_CONFIG.get("force_rescore", False)),
        )
        print(json.dumps(res, ensure_ascii=False, indent=2))
