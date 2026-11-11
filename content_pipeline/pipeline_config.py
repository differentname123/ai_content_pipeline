# -*- coding: utf-8 -*-
"""
流水线全局配置中心 (`pipeline_config.py`)

================================================================================
【日常扩展与调优指南（无需改动引擎代码）】
1. 切换生成风格/语种/账号人设：修改 `ACTIVE_PROFILE`（或往 `CONTENT_PROFILES` 新增一项并指向新的 `.md` 提示词文件）。
2. 单独调试某一步：修改 `STAGE_SWITCHES` 中的布尔开关，或直接运行对应步骤脚本。
3. 切换单次运行 / 常驻定时循环：修改 `SCHEDULE_CONFIG["continuous_loop"]`。
================================================================================
"""
from pathlib import Path
from typing import Any, Dict

from common.common_utils import DEFAULT_LOG_DIR, DEFAULT_MEDIA_DIR, DEFAULT_RUNTIME_DIR, PROMPT_DIR

# ==============================================================================
# 一、物理隔离的目录与 MongoDB 集合配置（与旧版 `social_media_posts` / `generated_articles` 零冲突）
# ==============================================================================
PIPELINE_PROMPT_DIR: Path = PROMPT_DIR / "pipeline_v2"
PIPELINE_VISUAL_DIR: Path = DEFAULT_MEDIA_DIR / "pipeline_visuals"
PIPELINE_TRACE_DIR: Path = DEFAULT_RUNTIME_DIR / "pipeline_traces"
PIPELINE_LATEST_REPORT_PATH: Path = DEFAULT_RUNTIME_DIR / "latest_pipeline_run_report.json"
PIPELINE_LOG_APP_NAME: str = "content_pipeline"  # 对应 logs/content_pipeline.log

for _dir in (PIPELINE_PROMPT_DIR, PIPELINE_VISUAL_DIR, PIPELINE_TRACE_DIR, DEFAULT_LOG_DIR):
    _dir.mkdir(parents=True, exist_ok=True)

PIPELINE_COLLECTIONS: Dict[str, str] = {
    # Step 1 产出的标准化原始帖子集合（本流水线仅【只读】查询，绝不修改原集合文档）
    "raw_posts": "harvested_social_posts",
    # Step 2 单帖多模态完整情报卡集合（按 source + post_id 唯一幂等存储）
    "post_digests": "pipeline_post_digests",
    # Step 3 全局单窗口归堆与精选话题集合
    "topic_clusters": "pipeline_topic_clusters",
    # Step 4 最终原创图文成品集合
    "creations": "pipeline_creations",
    # 全链路运行审计与调优追踪集合
    "run_traces": "pipeline_run_traces",
}


# ==============================================================================
# 二、可插拔内容风格 Profile 配置（新增赛道/平台风格只需加一套提示词与配置）
# ==============================================================================
CONTENT_PROFILES: Dict[str, Dict[str, Any]] = {
    # 预设 1：全领域高网感中文爆款（科技/AI、宏观政治、商业财富、社会奇闻、加密热点通吃）
    "zh_viral_general": {
        "display_name": "中文全领域高网感爆款号",
        "language": "zh",
        "step2_prompt_file": "step2_post_comprehension.md",
        "step3_prompt_file": "step3_topic_selection_default.md",
        "step4_prompt_file": "step4_creation_zh_viral.md",
        "topic_preference": (
            "面向泛科技、财经商业、全球大事件与加密市场高认知读者。优先挑选：\n"
            "1) 跨平台多源热议的重磅事件（如科技/AI巨头突发动态、政治宏观震荡、商业巨额赔偿/并购）；\n"
            "2) 极具戏剧反差的暴富/暴亏/套利奇闻（如普通人靠新工具套利、离奇商业操作）；\n"
            "3) 互动率极高、评论区吵翻天或极具幽默传播力的独立神帖。\n"
            "不要局限于单一币圈行情喊单，全网有流量的硬核好故事均可入选。"
        ),
        "style_guidelines": (
            "输出语言：简体中文。\n"
            "篇幅与节奏：220~550字左右，短段落排版（每段不超过3行），适合社交媒体信息流快速阅读与展开停留。\n"
            "语气基调：犀利、通透、有信息增量、带一点克制的幽默或冷峻洞察，像一位见多识广的业内老炮在跟朋友聊大瓜。"
        ),
    },
    # 预设 2：英文 X (Twitter) 蓝 V 高停留深度叙事号（直接切换 ACTIVE_PROFILE 即可生效）
    "en_x_creator": {
        "display_name": "English X High-Retention Creator",
        "language": "en",
        "step2_prompt_file": "step2_post_comprehension.md",
        "step3_prompt_file": "step3_topic_selection_default.md",
        "step4_prompt_file": "step4_creation_en_x.md",
        "topic_preference": (
            "Target global English X Premium audience across Tech/AI, Macro/Geopolitics, Markets/Business, and Crypto.\n"
            "Prioritize multi-source breaking stories, asymmetric money/tech plays, and high-controversy debates with strong reply potential."
        ),
        "style_guidelines": (
            "Output Language: Native, punchy English.\n"
            "Length & Formatting: 180-380 words. Hook in first 2 lines before 'Show more', clean bullet points for key mechanics, sharp closing question."
        ),
    },
}

# 当前激活的创作 Profile（切换风格只需改这一行）
ACTIVE_PROFILE: str = "zh_viral_general"


# ==============================================================================
# 三、总控阶段开关与调度配置（直接在 IDE 运行 `pipeline_runner.py` 即可生效）
# ==============================================================================
STAGE_SWITCHES: Dict[str, bool] = {
    "step2_comprehension": True,    # 是否执行 Step 2：对新拉取帖子做多模态图文+神评完整理解
    "step3_topic_selection": True,  # 是否执行 Step 3：单窗口全局视野按编号归堆与爆款选题
    "step4_creation": True,         # 是否执行 Step 4：原创正文撰写 + 多模态视觉选图/改图/生图
}

SCHEDULE_CONFIG: Dict[str, Any] = {
    "continuous_loop": False,       # False = 单次跑完完整流水线即退出（方便调试看效果）；True = 常驻定时循环
    "interval_seconds": 3600,       # 常驻模式下每轮间隔秒数（3600 = 1小时）
}


# ==============================================================================
# 四、各模块精细化参数与模型链配置
# ==============================================================================
STEP2_CONFIG: Dict[str, Any] = {
    # 扫描最近多少小时内入库/发布的原始帖子
    "lookback_hours": 48,
    # 单轮最多新理解多少条未处理帖子（按平台均衡+热力分优先；设为 0 表示处理窗口内全部未处理帖）
    "max_new_posts_per_run": 25,
    # 并发工作线程数（多模态 Flash 模型轻量快速，5 并发稳妥高效）
    "concurrency": 5,
    # 单条帖子最多挂载几张本地图片传给多模态模型（兼顾完整理解与延迟）
    "max_images_per_post": 3,
    # 若帖子正文无图但前排神评带图，是否允许挂载最高赞的 1 张评论图辅助理解
    "include_top_comment_image_if_post_no_image": True,
    # 单条帖子最多提取前多少条评论喂给 Step 2
    "max_comments_per_post": 8,
    # 是否强制重算已有情报卡（日常增量运行保持 False，调试新提示词时可临时改 True）
    "force_recompute": False,
    # 模型路由配置（优先高成功率、低延迟多模态模型，自动降级兜底）
    "model": "gemini-aistudio-3.6-flash",
    "fallback_models": [
        "gemini-aistudio-3.8-flash",
        "gemini-aistudio-3.1-pro-preview",
        "gpt-5.6-max",
    ],
    "timeout": 120,
}

STEP3_CONFIG: Dict[str, Any] = {
    # 纳入全局选题视野的情报卡时间窗口（小时）
    "lookback_hours": 36,
    # 单窗口最多放入多少条可用情报卡（120 条约 1.8 万 Tokens，完全处于单上下文黄金理解区）
    "max_candidates_in_window": 120,
    # 每轮精选出几个最佳爆款选题进入 Step 4 生产
    "target_topic_count": 3,
    # 防重复机制：过去多少小时内已被用于生成成品的原帖不再重复入选，同时将已生成标题注入防重记忆
    "dedup_used_posts_hours": 48,
    # 主编决策模型（全局推理与多源事件连线能力强）
    "model": "gemini-aistudio-3.1-pro-preview",
    "fallback_models": [
        "gpt-6-codex-luna-max",
        "gemini-aistudio-3.8-flash",
    ],
    "timeout": 180,
}

STEP4_CONFIG: Dict[str, Any] = {
    # 创作并发数
    "concurrency": 2,
    # 单次运行最多创作几篇待产选题
    "max_creations_per_run": 5,
    # 主笔文案与视觉导演模型
    "model": "gemini-aistudio-3.1-pro-preview",
    "fallback_models": [
        "gpt-6-codex-luna-max",
        "gemini-aistudio-3.8-flash",
    ],
    "timeout": 210,
    # 多模态视觉工作室配置（支持 reuse 复用 / edit 多模态改图 / generate 多模态生图）
    "enable_visual_studio": True,
    "image_models": [
        "gemini-3.1-flash-image",
        "gemini-3-pro-image",
        "gemini-2.5-flash-image",
        "gpt-image-2.5",
    ],
    "image_timeout": 90,
}
