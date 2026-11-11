# -*- coding: utf-8 -*-
"""
多模态原创内容生产流水线总控入口 (`pipeline_runner.py`)

================================================================================
【使用说明（直接在 IDE 点击运行即可，无需命令行参数）】
1. 本总控将 Step 1 入库 (`harvested_social_posts`) 之后的三大核心环节串联执行：
   - Step 2 (`step2_comprehension.py`)：单帖「主帖图文+热评」忠实语义还原与自洽性体检
   - Step 3 (`step3_topic_grouping.py`)：跨平台公平单窗口按短编号归堆与选题
   - Step 4 (`step4_creation.py` + `visual_studio.py`)：X 原生推文创作 + 真实原图复用/裁剪/拼接/去边角水印
2. 调整风格或参数：直接修改 `content_pipeline/pipeline_config.py` 中的 `ACTIVE_PROFILE`、
   `STAGE_SWITCHES`、`preset_model_group` 或 `prompt/pipeline_v2/*.md` 提示词文件。
3. 可观测性与调优闭环：
   - 精炼高密度执行报告：输出至控制台与 `logs/content_pipeline.log`
   - 100% 完整全链路追踪档案：自动写入 `logs/runtime/pipeline_traces/` 与 `pipeline_run_traces` 集合
================================================================================
"""
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from content_pipeline.pipeline_config import (
    ACTIVE_PROFILE,
    SCHEDULE_CONFIG,
    STAGE_SWITCHES,
)
from content_pipeline.pipeline_db import PipelineDB
from content_pipeline.pipeline_logger import (
    TZ_CN,
    format_pipeline_execution_report,
    get_pipeline_logger,
    save_run_audit_bundle,
)
from content_pipeline.step2_comprehension import run_step2_comprehension
from content_pipeline.step3_topic_grouping import run_step3_topic_selection
from content_pipeline.step4_creation import run_step4_creation


def execute_pipeline_round(
    profile_name: str = ACTIVE_PROFILE,
    switches: Optional[Dict[str, bool]] = None,
    step2_override: Optional[Dict[str, Any]] = None,
    step3_override: Optional[Dict[str, Any]] = None,
    step4_override: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """执行完整一轮（或按开关指定子阶段）的原创内容生产流水线，并持久化全链路追踪报告。"""
    logger = get_pipeline_logger()
    db = PipelineDB()
    active_switches = dict(STAGE_SWITCHES)
    if switches:
        active_switches.update(switches)

    started_ts = int(time.time())
    t0 = time.perf_counter()
    run_id = f"run_{datetime.now(TZ_CN).strftime('%H%M%S')}_{uuid4().hex[:6]}"

    logger.info(
        f"▶ [流水线启动] RunID={run_id} | Profile={profile_name} | 阶段开关={active_switches}"
    )

    step2_summary = None
    newly_digested: List[Dict[str, Any]] = []
    if active_switches.get("step2_comprehension", True):
        step2_summary, newly_digested = run_step2_comprehension(
            db=db,
            profile_name=profile_name,
            step2_cfg=step2_override,
            run_id=run_id,
        )

    step3_summary = None
    selected_topics: List[Dict[str, Any]] = []
    if active_switches.get("step3_topic_selection", True):
        step3_summary, selected_topics = run_step3_topic_selection(
            db=db,
            profile_name=profile_name,
            step3_cfg=step3_override,
            run_id=run_id,
        )

    step4_summary = None
    created_works: List[Dict[str, Any]] = []
    if active_switches.get("step4_creation", True):
        topics_input = selected_topics if active_switches.get("step3_topic_selection", True) else None
        step4_summary, created_works = run_step4_creation(
            db=db,
            topics=topics_input,
            profile_name=profile_name,
            step4_cfg=step4_override,
            run_id=run_id,
        )

    total_elapsed = round(time.perf_counter() - t0, 2)
    finished_bj = datetime.now(TZ_CN).strftime("%Y-%m-%d %H:%M:%S")

    run_record: Dict[str, Any] = {
        "run_id": run_id,
        "profile_name": profile_name,
        "switches": active_switches,
        "started_at_ts": started_ts,
        "finished_at_bj": finished_bj,
        "total_elapsed_seconds": total_elapsed,
        "step2_summary": step2_summary,
        "step3_summary": step3_summary,
        "step4_summary": step4_summary,
        "audit_details": {
            "digests": [
                {
                    "post_key": d.get("post_key"),
                    "source": d.get("source"),
                    "is_usable": d.get("is_usable"),
                    "reject_reason": d.get("reject_reason"),
                    "summary": d.get("summary"),
                    "valuable_comments": d.get("valuable_comments"),
                    "images": d.get("images"),
                    "model_used": (d.get("llm_trace") or {}).get("model_used"),
                    "prompt_sha1": (d.get("llm_trace") or {}).get("prompt_sha1"),
                }
                for d in newly_digested
            ],
            "selected_topics": [
                {
                    "topic_id": t.get("topic_id"),
                    "rank": t.get("rank"),
                    "topic_summary": t.get("topic_summary"),
                    "mode": t.get("mode"),
                    "post_keys": t.get("post_keys"),
                    "why_selected": t.get("why_selected"),
                }
                for t in selected_topics
            ],
            "creations": [
                {
                    "creation_id": c.get("creation_id"),
                    "topic_id": c.get("topic_id"),
                    "topic_summary": c.get("topic_summary"),
                    "status": c.get("status"),
                    "post_text": c.get("post_text"),
                    "char_count": c.get("char_count"),
                    "source_post_keys": c.get("source_post_keys"),
                    "visual_plan": c.get("visual_plan"),
                    "visual_trace": c.get("visual_trace"),
                    "final_image_paths": c.get("final_image_paths"),
                    "final_image_path": c.get("final_image_path"),
                    "text_model": (c.get("llm_trace") or {}).get("model_used"),
                    "prompt_sha1": (c.get("llm_trace") or {}).get("prompt_sha1"),
                }
                for c in created_works
            ],
        },
    }

    trace_path = save_run_audit_bundle(run_record)
    db.save_run_trace(run_record)

    report_text = format_pipeline_execution_report(run_record, trace_path)
    logger.info(report_text)
    return run_record


def main() -> None:
    logger = get_pipeline_logger()
    continuous = bool(SCHEDULE_CONFIG.get("continuous_loop", False))
    interval_sec = max(60, int(SCHEDULE_CONFIG.get("interval_seconds", 3600)))

    round_idx = 0
    while True:
        round_idx += 1
        try:
            execute_pipeline_round()
        except KeyboardInterrupt:
            logger.info("[流水线退出] 收到手动中断信号，安全退出。")
            break
        except Exception as exc:
            logger.exception(f"❌ [流水线异常] 第 {round_idx} 轮运行出现未捕获异常: {exc}")

        if not continuous:
            break

        logger.info(f"[常驻调度] 第 {round_idx} 轮已完成，休眠 {interval_sec} 秒后开启下一轮...")
        try:
            time.sleep(interval_sec)
        except KeyboardInterrupt:
            logger.info("[流水线退出] 休眠期间收到手动中断信号，安全退出。")
            break


if __name__ == "__main__":
    main()
