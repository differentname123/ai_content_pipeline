# -*- coding: utf-8 -*-
"""
模块化流水线双层可观测性与日志系统 (`pipeline_logger.py`)

设计目标：
1. 控制台与 `logs/content_pipeline.log`：仅打印高信息密度的各独立模块健康度、双环漏斗转化率与盲测终审结果（零刷屏）。
2. 结构化全量追踪档案 `logs/runtime/pipeline_traces/run_<ts>_<id>.json` 与 `latest_pipeline_run_report.json`：
   100% 完整保存每一步的提示词指纹、命中模型、单帖理解明细、方向发现、指哪打哪深挖、素材精选、初稿快照与盲测返修对比。
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
    """生成高信息密度、一目了然的模块化双环执行汇总报告。"""
    sep = "=" * 104
    sub_sep = "-" * 104
    lines: List[str] = [
        "",
        sep,
        f"🚀 [模块化多模态内容生产报告] RunID: {run_record.get('run_id')} | Profile: {run_record.get('profile_name')} "
        f"| 总耗时: {run_record.get('total_elapsed_seconds', 0):.2f}s | 时间: {run_record.get('finished_at_bj')}",
        sub_sep,
    ]

    # --- 模块 1A：可选广谱采集 ---
    h_sum = run_record.get("harvest_summary")
    if h_sum:
        p_cnts = {k: (v or {}).get("harvested_count", 0) for k, v in (h_sum.get("platform_stats") or {}).items()}
        h_alerts = h_sum.get("ops_alerts") or []
        lines.append(
            f"【模块 1A | 全平台广谱采集】耗时: {h_sum.get('elapsed_seconds', 0):.2f}s | 入库: {h_sum.get('total_harvested', 0)}条 "
            f"(+{h_sum.get('total_new_inserted', 0)}/~{h_sum.get('total_repolled_updated', 0)}) | 分布: {p_cnts} | "
            f"配图: {h_sum.get('total_local_images', 0)}张 | 评论: {h_sum.get('total_comments', 0)}条 | "
            f"外链正文: {h_sum.get('total_external_articles', 0)}篇 | 告警: {len(h_alerts)}项"
        )
        for al in h_alerts:
            lines.append(f"   ├─ {al}")
        lines.append(sub_sep)

    # --- 模块 2：单帖理解 ---
    s2 = run_record.get("step2_summary")
    if s2:
        src_str = ", ".join(f"{k}:{v}" for k, v in (s2.get("usable_by_source") or {}).items()) or "无新增"
        models_str = ", ".join(f"{k}:{v}" for k, v in (s2.get("models_used") or {}).items()) or "无调用"
        lines.append(
            f"【模块 2 | 单帖多模态理解】状态: {s2.get('status')} | 耗时: {s2.get('elapsed_seconds', 0):.2f}s | "
            f"时间窗原帖: {s2.get('in_time_window_total', 0)} (排除视频: {s2.get('excluded_video_count', 0)} -> 候选: {s2.get('window_raw_count', 0)})"
        )
        lines.append(
            f"   ├─ 理解进度: 缓存跳过={s2.get('already_digested_count', 0)} | 新处理={s2.get('processed_count', 0)} "
            f"(带图={s2.get('with_images_count', 0)}帖/{s2.get('total_images_fed', 0)}张, 外链文={s2.get('with_external_article_count', 0)}篇) | "
            f"可用={s2.get('usable_count', 0)} | 空壳拦截={s2.get('filtered_count', 0)} | 失败={s2.get('failed_count', 0)}"
        )
        lines.append(f"   ├─ 可用来源: {src_str}")
        lines.append(f"   └─ 命中模型: {models_str}")

    # --- 模块 3：热点方向发现 ---
    r3 = run_record.get("radar_summary")
    if r3:
        lines.append(sub_sep)
        lines.append(
            f"【模块 3 | 热点方向发现雷达】状态: {r3.get('status')} | 耗时: {r3.get('elapsed_seconds', 0):.2f}s | "
            f"全量候选: {r3.get('candidate_count', 0)}条 ({r3.get('batch_count', 1)}批) -> 发现方向: {r3.get('selected_count', 0)}个 | 模型: {r3.get('models_used')}"
        )
        for idx, d in enumerate(r3.get("directions_preview") or [], 1):
            lines.append(
                f"   ├─ 方向#{idx}: {d.get('direction_summary')} | 搜索词={d.get('search_keywords')} | 种子帖={d.get('seed_post_keys')}"
            )
            lines.append(f"   │    └─ 价值依据: {d.get('why_worth_creating')}")

    # --- 模块 4：按方向关键词指哪打哪深挖 ---
    e4 = run_record.get("expansion_summary")
    if e4:
        lines.append(sub_sep)
        lines.append(
            f"【模块 4 | 按方向跨平台『指哪打哪』深挖】状态: {e4.get('status')} | 耗时: {e4.get('elapsed_seconds', 0):.2f}s | "
            f"搜索召回: {e4.get('total_searched_posts', 0)}条 | 即时新理解: {e4.get('total_newly_comprehended', 0)}条"
        )
        for idx, ex in enumerate(e4.get("directions_expansion_preview") or [], 1):
            lines.append(
                f"   ├─ 方向#{idx} ({ex.get('direction_summary', '')[:40]}): "
                f"种子={ex.get('seed_count')} + 新搜={ex.get('searched_count')} {ex.get('by_platform_counts')} -> 候选可用池={ex.get('usable_candidates_total')}条"
            )
            if ex.get("platform_diagnosis"):
                lines.append(f"   │    ├─ 平台诊断: {ex.get('platform_diagnosis')}")
            for al in ex.get("ops_alerts") or []:
                lines.append(f"   │    └─ 异常告警: {al}")

    # --- 模块 5：方向素材精选 ---
    c5 = run_record.get("curation_summary")
    if c5:
        lines.append(sub_sep)
        lines.append(
            f"【模块 5 | 方向素材精选去噪】状态: {c5.get('status')} | 耗时: {c5.get('elapsed_seconds', 0):.2f}s | "
            f"输入方向: {c5.get('input_directions', 0)} -> 合格进入创作: {c5.get('viable_directions', 0)}"
        )
        for idx, cp in enumerate(c5.get("curation_preview") or [], 1):
            lines.append(
                f"   ├─ 方向#{idx} [{cp.get('status')}]: 候选 {cp.get('candidate_count')} 帖 -> 精选保留 {cp.get('kept_count')} 帖 "
                f"(剔除噪音 {cp.get('rejected_count')} 帖) | {cp.get('curated_post_keys')}"
            )
            if cp.get("rejected_summary"):
                lines.append(f"   │    ├─ 排噪边界: {cp.get('rejected_summary')}")
            if cp.get("curation_note"):
                lines.append(f"   │    ├─ 精选说明: {cp.get('curation_note')}")
            details = cp.get("curated_items_detail") or []
            if details:
                sample_str = " ; ".join(
                    f"{it.get('id')}({it.get('post_key')}): {it.get('reason')}" for it in details[:5]
                )
                more_tag = f" ...等共{len(details)}条" if len(details) > 5 else ""
                lines.append(f"   │    └─ 入选锚点示例: {sample_str}{more_tag}")

    # --- 模块 6：解耦原创图文创作 ---
    cr6 = run_record.get("creation_summary")
    if cr6:
        lines.append(sub_sep)
        lines.append(
            f"【模块 6 | 解耦原创图文创作 + 像素编辑】状态: {cr6.get('status')} | 耗时: {cr6.get('elapsed_seconds', 0):.2f}s | "
            f"目标任务: {cr6.get('target_count', 0)} -> 产出初稿: {cr6.get('created_count', 0)}篇 (失败: {cr6.get('failed_count', 0)})"
        )
        for idx, c in enumerate(cr6.get("creations_preview") or [], 1):
            lines.append(
                f"   ├─ 初稿#{idx} ({c.get('char_count')}字 | 候选真图:{c.get('candidate_images_count', 0)}张 | 模型:{c.get('text_model')}) "
                f"焦点: {c.get('direction_summary')}"
            )
            lines.append(f"   │    ├─ 首行预览: {c.get('first_line')}")
            img_list = c.get("final_image_paths") or []
            img_str = ", ".join(img_list) if img_list else "纯文本（无配图）"
            lines.append(
                f"   │    └─ 视觉动作: [{c.get('visual_action')}] 轨迹={c.get('visual_notes') or []} -> {img_str}"
            )

    # --- 模块 7：零背景网友盲测 ---
    rv7 = run_record.get("review_summary")
    if rv7:
        lines.append(sub_sep)
        lines.append(
            f"【模块 7 | 零背景普通网友盲测终审】状态: {rv7.get('status')} | 耗时: {rv7.get('elapsed_seconds', 0):.2f}s | "
            f"盲测: {rv7.get('reviewed_count', 0)}篇 -> 准予发布(ready): {rv7.get('ready_count', 0)}篇 "
            f"(一次直通: {rv7.get('passed_direct_count', 0)}, 返修通过: {rv7.get('passed_after_revision_count', 0)}) | "
            f"质量淘汰(dropped): {rv7.get('dropped_count', 0)}篇 | 异常(review_failed): {rv7.get('error_count', 0)}篇"
        )
        for idx, rv in enumerate(rv7.get("reviews_preview") or [], 1):
            lines.append(
                f"   ├─ 盲测#{idx} [{rv.get('publish_status')} | {rv.get('final_outcome')}] "
                f"终稿={rv.get('final_char_count')}字 | 首轮模型={rv.get('round1_model')}"
            )
            lines.append(f"   │    ├─ 首轮盲测({rv.get('round1_verdict')}): {rv.get('round1_takeaway')}")
            if rv.get("round1_feedback"):
                lines.append(f"   │    ├─ 网友挑刺: {'；'.join(rv.get('round1_feedback') or [])}")
            if rv.get("round2_verdict"):
                lines.append(
                    f"   │    └─ 返修再测({rv.get('round2_verdict')} | 模型={rv.get('round2_model')}): {rv.get('round2_takeaway')}"
                )

    lines.append(sub_sep)
    lines.append(f"📁 完整可回溯分析档案: {trace_file}")
    lines.append(sep)
    return "\n".join(lines)
