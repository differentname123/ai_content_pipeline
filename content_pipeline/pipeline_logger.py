# -*- coding: utf-8 -*-
"""
流水线双层可观测性与日志系统 (`pipeline_logger.py`)

设计目标：
1. 控制台与 `logs/content_pipeline.log`：仅打印高信息密度的阶段健康度、漏斗转化率与核心产出摘要（零噪音、零刷屏）。
2. 结构化全量追踪档案 `logs/runtime/pipeline_traces/run_<ts>_<id>.json` 与 `latest_pipeline_run_report.json`：
   100% 完整保存每一步的提示词 SHA1 指纹、实际命中模型、单帖理解与拦截原因、选题决策以及最终推文与配图操作轨迹。
"""
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

from common.common_utils import save_json, setup_logger
from content_pipeline.pipeline_config import (
    PIPELINE_LATEST_REPORT_PATH,
    PIPELINE_LOG_APP_NAME,
    PIPELINE_TRACE_DIR,
)

TZ_CN = timezone(timedelta(hours=8))
_CONFIGURED = False


def get_pipeline_logger() -> logging.Logger:
    """获取独立且干净的 `content_pipeline` 日志记录器。"""
    global _CONFIGURED
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass
    if hasattr(sys.stderr, "reconfigure"):
        try:
            sys.stderr.reconfigure(encoding="utf-8")
        except Exception:
            pass

    logger = setup_logger(app_name=PIPELINE_LOG_APP_NAME)
    if not _CONFIGURED:
        for noisy in (
            "httpx",
            "httpcore",
            "openai",
            "urllib3",
            "common.mongo_db.mongo_base",
            "common.mongo_db.mongo_manager",
        ):
            logging.getLogger(noisy).setLevel(logging.WARNING)
        _CONFIGURED = True
    return logger


def save_run_audit_bundle(run_record: Dict[str, Any]) -> str:
    """将单轮全链路审计档案同时写入历史归档与 `latest_pipeline_run_report.json` 快照。"""
    run_id = str(run_record.get("run_id") or "unknown")
    ts_tag = datetime.now(TZ_CN).strftime("%Y%m%d_%H%M%S")
    archive_path: Path = PIPELINE_TRACE_DIR / f"run_{ts_tag}_{run_id}.json"

    save_json(archive_path, run_record)
    save_json(PIPELINE_LATEST_REPORT_PATH, run_record)
    return str(archive_path)


def format_pipeline_execution_report(run_record: Dict[str, Any], trace_file: str) -> str:
    """生成高信息密度、一目了然的单轮执行汇总报告。"""
    sep = "=" * 92
    sub_sep = "-" * 92
    lines: List[str] = [
        "",
        sep,
        f"🚀 [原创内容流水线执行报告] RunID: {run_record.get('run_id')} | Profile: {run_record.get('profile_name')} "
        f"| 总耗时: {run_record.get('total_elapsed_seconds', 0):.2f}s | 时间: {run_record.get('finished_at_bj')}",
        sub_sep,
    ]

    # --- Step 2 摘要 ---
    s2 = run_record.get("step2_summary")
    if s2:
        src_str = ", ".join(f"{k}:{v}" for k, v in (s2.get("usable_by_source") or {}).items()) or "无新增"
        models_str = ", ".join(f"{k}:{v}" for k, v in (s2.get("models_used") or {}).items()) or "无调用"
        lines.append(
            f"【Step 2 | 单帖图文理解】状态: {s2.get('status')} | 耗时: {s2.get('elapsed_seconds', 0):.2f}s | "
            f"窗口原帖: {s2.get('window_raw_count', 0)} (缓存跳过: {s2.get('already_digested_count', 0)}, "
            f"新处理: {s2.get('processed_count', 0)})"
        )
        lines.append(
            f"   ├─ 理解结果: 成功={s2.get('success_count', 0)} (带主帖图={s2.get('with_images_count', 0)}, "
            f"共读图={s2.get('total_images_fed', 0)}张) | 失败={s2.get('failed_count', 0)} | "
            f"合格可用={s2.get('usable_count', 0)} | 自洽性拦截={s2.get('filtered_count', 0)}"
        )
        lines.append(f"   ├─ 可用来源: {src_str}")
        lines.append(f"   └─ 命中模型: {models_str}")
        if s2.get("filtered_samples"):
            lines.append(f"      (拦截样本: {'; '.join((s2.get('filtered_samples') or [])[:4])})")

    # --- Step 3 摘要 ---
    s3 = run_record.get("step3_summary")
    if s3:
        lines.append(sub_sep)
        lines.append(
            f"【Step 3 | 全局归堆选题】状态: {s3.get('status')} | 耗时: {s3.get('elapsed_seconds', 0):.2f}s | "
            f"跨平台候选池: {s3.get('candidate_count', 0)} (防重剔除已用帖: {s3.get('excluded_used_posts', 0)}) -> "
            f"精选话题: {s3.get('selected_count', 0)} 个 | 模型: {s3.get('model_used')}"
        )
        for idx, t in enumerate(s3.get("selected_topics_preview") or [], 1):
            lines.append(
                f"   ├─ 选题#{idx} ({t.get('mode')}, 关联 {len(t.get('post_keys', []))} 帖: "
                f"{', '.join(t.get('post_keys', []))}): {t.get('topic_summary')}"
            )
            lines.append(f"   │    └─ 入选依据: {t.get('why_selected')}")
        if s3.get("runner_up_notes"):
            ru_preview = " | ".join(
                f"{r.get('topic_brief')} (未选: {r.get('skip_reason')})"
                for r in (s3.get("runner_up_notes") or [])[:2]
            )
            lines.append(f"   └─ 备选复盘: {ru_preview}")

    # --- Step 4 摘要 ---
    s4 = run_record.get("step4_summary")
    if s4:
        lines.append(sub_sep)
        lines.append(
            f"【Step 4 | 原生推文与真实配图】状态: {s4.get('status')} | 耗时: {s4.get('elapsed_seconds', 0):.2f}s | "
            f"目标选题: {s4.get('target_count', 0)} -> 产出成品: {s4.get('created_count', 0)} 篇 "
            f"(失败: {s4.get('failed_count', 0)})"
        )
        for idx, c in enumerate(s4.get("creations_preview") or [], 1):
            lines.append(
                f"   ├─ 成品#{idx} ({c.get('char_count')}字 | 模型:{c.get('text_model')}) 话题: {c.get('topic_summary')}"
            )
            lines.append(f"   │    ├─ 首行预览: {c.get('first_line')}")
            img_list = c.get("final_image_paths") or []
            img_str = ", ".join(img_list) if img_list else "纯文本推文（无配图）"
            lines.append(
                f"   │    └─ 真实视觉: 动作=[{c.get('visual_action')}] 轨迹={c.get('visual_notes') or []} -> {img_str}"
            )

    lines.append(sub_sep)
    lines.append(f"📁 完整可回溯分析档案 (含Prompt指纹/原始I-O/全量拦截与裁剪明细): {trace_file}")
    lines.append(sep)
    return "\n".join(lines)
