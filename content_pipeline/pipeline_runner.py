# -*- coding: utf-8 -*-
"""
多模态原创内容生产流水线总控入口 (`pipeline_runner.py`)

================================================================================
【使用说明（直接在 IDE 点击运行即可，无需命令行参数）】
1. 本总控将 Step 1 入库 (`harvested_social_posts`) 之后的三大核心环节串联执行：
   - Step 2 (`step2_comprehension.py`)：单帖「图文+热评」多模态完整语义还原
   - Step 3 (`step3_topic_grouping.py`)：全局单窗口按短编号归堆与爆款选题（零自由标签）
   - Step 4 (`step4_creation.py` + `visual_studio.py`)：高网感原创正文 + 智能选图/改图/生图
2. 调整风格或参数：直接修改 `content_pipeline/pipeline_config.py` 中的 `ACTIVE_PROFILE`、
   `STAGE_SWITCHES` 或 `prompt/pipeline_v2/*.md` 提示词文件，无需改动引擎逻辑。
3. 可观测性与调优闭环：
   - 精炼高密度执行报告：输出至控制台与 `logs/content_pipeline.log`
   - 100% 完整全链路追踪档案：自动写入 `logs/runtime/pipeline_traces/` 与 `pipeline_run_traces` 集合
================================================================================
"""
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional
from uuid import uuid4

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from content_pipeline.pipeline_config import (
    ACTIVE_PROFILE,
    CONTENT_PROFILES,
    SCHEDULE_CONFIG,
    STAGE_SWITCHES,
    STEP2_CONFIG,
    STEP3_CONFIG,
    STEP4_CONFIG,
)
from content_pipeline.pipeline_db import PipelineDB
from content_pipeline.pipeline_logger import (
    format_pipeline_execution_report,
    get_pipeline_logger,
    now_bj_str,
    save_run_audit_bundle,
)
from content_pipeline.step2_comprehension import run_step2_comprehension
from content_pipeline.step3_topic_grouping import run_step3_topic_selection
from content_pipeline.step4_creation import run_step4_creation


def execute_pipeline_round(
    profile_name: str = ACTIVE_PROFILE,
    stage_switches: Optional[Dict[str, bool]] = None,
    step2_overrides: Optional[Dict[str, Any]] = None,
    step3_overrides: Optional[Dict[str, Any]] = None,
    step4_overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """执行完整一轮（或按开关指定阶段）的内容理解、选题归堆与图文创作流水线。"""
    logger = get_pipeline_logger()
    db = PipelineDB()

    switches = dict(STAGE_SWITCHES)
    if stage_switches:
        switches.update(stage_switches)

    profile_cfg = CONTENT_PROFILES.get(profile_name) or CONTENT_PROFILES[ACTIVE_PROFILE]
    run_id = uuid4().hex
    started_ts = int(time.time())
    started_perf = time.perf_counter()

    logger.info(
        f"==================== [流水线启动] RunID={run_id[:12]} | Profile={profile_name} | 开关={switches} ===================="
    )

    step2_summary = None
    step2_digests = []
    if switches.get("step2_comprehension", True):
        step2_summary, step2_digests = run_step2_comprehension(
            db=db,
            profile_name=profile_name,
            step2_cfg=step2_overrides,
            run_id=run_id,
        )

    step3_summary = None
    selected_topics = []
    if switches.get("step3_topic_selection", True):
        step3_summary, selected_topics = run_step3_topic_selection(
            db=db,
            profile_name=profile_name,
            step3_cfg=step3_overrides,
            run_id=run_id,
        )

    step4_summary = None
    created_works = []
    if switches.get("step4_creation", True):
        topics_arg = selected_topics if switches.get("step3_topic_selection", True) else None
        step4_summary, created_works = run_step4_creation(
            db=db,
            topics=topics_arg,
            profile_name=profile_name,
            step4_cfg=step4_overrides,
            run_id=run_id,
        )

    total_elapsed = round(time.perf_counter() - started_perf, 2)

    # 构建完整无损的可回溯运行档案（用于后续深度效果分析与提示词优化）
    run_record: Dict[str, Any] = {
        "run_id": run_id,
        "started_at_ts": started_ts,
        "finished_at_bj": now_bj_str(),
        "total_elapsed_seconds": total_elapsed,
        "profile_name": profile_name,
        "profile_display_name": profile_cfg.get("display_name"),
        "stage_switches": switches,
        "config_snapshot": {
            "step2": {**STEP2_CONFIG, **(step2_overrides or {})},
            "step3": {**STEP3_CONFIG, **(step3_overrides or {})},
            "step4": {**STEP4_CONFIG, **(step4_overrides or {})},
        },
        "step2_summary": step2_summary,
        "step3_summary": step3_summary,
        "step4_summary": step4_summary,
        # 完整阶段明细：方便直接在一个归档 JSON 中查看每条帖子的理解结果、选题决策和生成全文
        "details": {
            "step2_digests": [
                {
                    "post_key": d.get("post_key"),
                    "status": d.get("status"),
                    "is_usable": d.get("is_usable"),
                    "filter_reason": d.get("filter_reason"),
                    "domain": d.get("domain"),
                    "core_hook": d.get("core_hook"),
                    "complete_summary": d.get("complete_summary"),
                    "image_descriptions": d.get("image_descriptions"),
                    "comment_essence": d.get("comment_essence"),
                    "model_used": (d.get("llm_trace") or {}).get("model_used"),
                    "prompt_sha1": (d.get("llm_trace") or {}).get("prompt_sha1"),
                    "duration_seconds": (d.get("llm_trace") or {}).get("duration_seconds"),
                }
                for d in step2_digests
            ],
            "step3_topics": [
                {
                    "topic_id": t.get("topic_id"),
                    "rank": t.get("rank"),
                    "topic_title": t.get("topic_title"),
                    "domain": t.get("domain"),
                    "mode": t.get("mode"),
                    "post_keys": t.get("post_keys"),
                    "selection_reason": t.get("selection_reason"),
                    "creative_angle": t.get("creative_angle"),
                }
                for t in selected_topics
            ],
            "step4_creations": [
                {
                    "creation_id": c.get("creation_id"),
                    "topic_id": c.get("topic_id"),
                    "status": c.get("status"),
                    "domain": c.get("domain"),
                    "title": c.get("title"),
                    "opening_hook": c.get("opening_hook"),
                    "body": c.get("body"),
                    "closing_question": c.get("closing_question"),
                    "source_post_keys": c.get("source_post_keys"),
                    "visual_plan": c.get("visual_plan"),
                    "visual_trace": c.get("visual_trace"),
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
