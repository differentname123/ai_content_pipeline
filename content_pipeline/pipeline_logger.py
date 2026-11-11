# -*- coding: utf-8 -*-
"""
流水线双层可观测性与日志系统 (`pipeline_logger.py`)

设计目标：
1. 控制台与 `logs/content_pipeline.log`：仅打印高信息密度的关键进度、阶段漏斗统计与核心决策结果（零噪音、零冗余）。
2. 结构化全量追踪文件 `logs/runtime/pipeline_traces/run_<ts>_<id>.json` 与 `latest_pipeline_run_report.json`：
   100% 完整保存每一步的输入摘要、提示词版本指纹、模型耗时、入选/淘汰理由及最终图文路径，彻底支撑后续效果复盘与提示词调优。
3. 与既有系统互不干扰：不篡改其他模块的日志文件，仅在当前进程抑制底层 HTTP/网关的控制台刷屏。
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

    pipe_logger = setup_logger(app_name=PIPELINE_LOG_APP_NAME)
    if not _CONFIGURED:
        logging.getLogger().setLevel(logging.WARNING)
        # 抑制底层模块在控制台的常规过程日志（保留其自身文件日志与 WARNING+ 告警）
        for mod_name in (
            "common.mongo_db.mongo_manager",
            "common.mongo_db.mongo_base",
            "httpx",
            "httpcore",
            "openai",
            "urllib3",
        ):
            logging.getLogger(mod_name).setLevel(logging.WARNING)

        # 对 model_api 保留其文件日志 Handler，但将控制台 StreamHandler 调至 WARNING，避免并发时重复刷屏
        model_logger = logging.getLogger("model_api")
        for handler in model_logger.handlers:
            if type(handler) is logging.StreamHandler:
                handler.setLevel(logging.WARNING)

        _CONFIGURED = True
    return pipe_logger


def now_bj_str() -> str:
    return datetime.now(TZ_CN).strftime("%Y-%m-%d %H:%M:%S")


def save_run_audit_bundle(run_record: Dict[str, Any]) -> Path:
    """
    将单轮完整执行轨迹（含 Step 2/3/4 全量细节与调优诊断信息）原子落盘：
    1. 历史归档：`logs/runtime/pipeline_traces/run_<YYYYMMDD_HHMMSS>_<run_id[:8]>.json`
    2. 最新快照：`logs/runtime/latest_pipeline_run_report.json`
    """
    ts_tag = datetime.now(TZ_CN).strftime("%Y%m%d_%H%M%S")
    short_id = str(run_record.get("run_id") or "run")[:8]
    trace_path = PIPELINE_TRACE_DIR / f"run_{ts_tag}_{short_id}.json"
    save_json(trace_path, run_record)
    save_json(PIPELINE_LATEST_REPORT_PATH, run_record)
    return trace_path


def format_pipeline_execution_report(run_record: Dict[str, Any], trace_file: Path) -> str:
    """
    生成高信息密度、结构清晰的流水线执行与质量诊断总览报告。
    兼顾运维健康度（耗时/成功率/模型分布）与业务可分析性（过滤原因/选题依据/视觉产出）。
    """
    lines: List[str] = []
    sep = "=" * 96
    sub_sep = "-" * 96

    lines.append("")
    lines.append(sep)
    lines.append(
        f"🚀 [内容流水线执行报告] RunID: {run_record.get('run_id', '')[:12]} | "
        f"Profile: {run_record.get('profile_name')} ({run_record.get('profile_display_name')}) | "
        f"总耗时: {run_record.get('total_elapsed_seconds', 0):.2f}s"
    )
    lines.append(sub_sep)

    # --- Step 2 摘要 ---
    s2 = run_record.get("step2_summary")
    if s2:
        lines.append(
            f"【Step 2 | 多模态单帖理解】状态: {s2.get('status')} | 耗时: {s2.get('elapsed_seconds', 0):.2f}s | "
            f"窗口原帖: {s2.get('window_raw_count', 0)} (已有缓存: {s2.get('already_digested_count', 0)}) -> "
            f"本轮新理解: {s2.get('processed_count', 0)} (成功:{s2.get('success_count', 0)} / 失败:{s2.get('failed_count', 0)})"
        )
        lines.append(
            f"   ├─ 图文覆盖: 带本地图理解 {s2.get('with_images_count', 0)} 条 (共读图 {s2.get('total_images_fed', 0)} 张) | "
            f"有效素材(usable): {s2.get('usable_count', 0)} 条 | 剔除废帖: {s2.get('filtered_count', 0)} 条"
        )
        if s2.get("domain_distribution"):
            dom_str = ", ".join(f"{k}:{v}" for k, v in s2["domain_distribution"].items())
            lines.append(f"   ├─ 领域分布: {dom_str}")
        if s2.get("filtered_samples"):
            filt_str = " ; ".join(s2["filtered_samples"][:4])
            lines.append(f"   └─ 剔除样本原因: {filt_str}")

    # --- Step 3 摘要 ---
    s3 = run_record.get("step3_summary")
    if s3:
        lines.append(sub_sep)
        lines.append(
            f"【Step 3 | 全局单窗口选题归堆】状态: {s3.get('status')} | 耗时: {s3.get('elapsed_seconds', 0):.2f}s | "
            f"模型: {s3.get('model_used') or 'N/A'} | "
            f"候选情报池: {s3.get('candidate_count', 0)} 条 (防重排除旧帖:{s3.get('excluded_used_posts', 0)}) -> "
            f"精选话题: {s3.get('selected_count', 0)} 个"
        )
        for idx, t in enumerate(s3.get("selected_topics_preview") or [], 1):
            lines.append(
                f"   ├─ 选题#{idx} [{t.get('domain')}] ({t.get('mode')}, 关联 {len(t.get('post_keys', []))} 帖: "
                f"{', '.join(t.get('post_keys', []))}): {t.get('topic_title')}"
            )
            lines.append(f"   │    └─ 入选理由: {t.get('selection_reason')}")
        if s3.get("runner_up_notes"):
            ru_preview = " | ".join(
                f"{r.get('topic_brief')} (未选原因: {r.get('skip_reason')})"
                for r in (s3.get("runner_up_notes") or [])[:2]
            )
            lines.append(f"   └─ 备选落选复盘: {ru_preview}")

    # --- Step 4 摘要 ---
    s4 = run_record.get("step4_summary")
    if s4:
        lines.append(sub_sep)
        lines.append(
            f"【Step 4 | 原创图文生产】状态: {s4.get('status')} | 耗时: {s4.get('elapsed_seconds', 0):.2f}s | "
            f"目标选题: {s4.get('target_count', 0)} -> 产出成品: {s4.get('created_count', 0)} 篇 "
            f"(失败: {s4.get('failed_count', 0)})"
        )
        for idx, c in enumerate(s4.get("creations_preview") or [], 1):
            lines.append(
                f"   ├─ 成品#{idx} [{c.get('domain')}] 《{c.get('title')}》 "
                f"(正文:{c.get('body_length')}字 | 文案模型:{c.get('text_model')})"
            )
            lines.append(f"   │    ├─ 开头钩子: {c.get('opening_hook')}")
            lines.append(
                f"   │    └─ 视觉配图: 动作=[{c.get('visual_action')}] (模型:{c.get('image_model') or '本地原图'}) "
                f"-> {c.get('final_image_path') or '无图'}"
            )

    lines.append(sub_sep)
    lines.append(f"📁 完整可回溯分析档案 (含Prompt指纹/原始I-O/全量淘汰明细): {trace_file}")
    lines.append(sep)
    return "\n".join(lines)
