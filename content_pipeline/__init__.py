# -*- coding: utf-8 -*-
"""
独立、通用、提示词驱动的多模态内容理解与原创图文生产流水线 (`content_pipeline`)

核心设计原则：
1. 物理隔离零冲突：仅只读消费 `harvested_social_posts`，产物独立存入 `pipeline_*` 系列集合与 `logs/content_pipeline.log`，绝不干扰旧版代码。
2. 回归第一性原理的三步后处理架构（衔接 Step 1 拉取）：
   - Step 2 (`step2_comprehension.py`)：单帖「文+图+热评」多模态完整理解，消除不看图盲筛，降维为自洽情报卡。
   - Step 3 (`step3_topic_grouping.py`)：单窗口全局视野按编号归堆与爆款选题，彻底告别脆弱的自由标签匹配。
   - Step 4 (`step4_creation.py` + `visual_studio.py`)：X 原生推文创作（无标题/无固定字数框框） + 确定性真实像素配图工坊（无损复用 / 坐标裁剪去外壳 / 原图拼接 / 小角标修复 / 纯文字，禁用 AI 生图与重绘）。
3. 模型池轮询与提示词解耦：统一接入 `model_api.py` 的 `preset_model_group` 模型组自动容灾，新增或调整账号风格仅需修改 `pipeline_config.py` 或 `prompt/pipeline_v2/*.md`。
4. 双层可观测性：控制台与主日志紧凑无冗余；数据库与 `logs/runtime/pipeline_traces/` 保留 100% 完整决策链路供复盘调优。
"""

from content_pipeline.pipeline_config import (
    ACTIVE_PROFILE,
    CONTENT_PROFILES,
    PIPELINE_COLLECTIONS,
    SCHEDULE_CONFIG,
    STAGE_SWITCHES,
    STEP2_CONFIG,
    STEP3_CONFIG,
    STEP4_CONFIG,
)

__all__ = [
    "ACTIVE_PROFILE",
    "CONTENT_PROFILES",
    "PIPELINE_COLLECTIONS",
    "SCHEDULE_CONFIG",
    "STAGE_SWITCHES",
    "STEP2_CONFIG",
    "STEP3_CONFIG",
    "STEP4_CONFIG",
]
