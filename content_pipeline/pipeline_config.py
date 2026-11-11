# -*- coding: utf-8 -*-
"""
流水线全局配置中心 (`pipeline_config.py`)

================================================================================
【日常扩展与调优指南（无需改动引擎代码）】
1. 切换生成风格/语种/账号人设：修改 `ACTIVE_PROFILE`（或往 `CONTENT_PROFILES` 新增一项并指向新的 `.md` 提示词文件）。
2. 单独调试某一步：修改 `STAGE_SWITCHES` 中的布尔开关，或直接运行对应步骤脚本。
3. 切换单次运行 / 常驻定时循环：修改 `SCHEDULE_CONFIG["continuous_loop"]`。
4. 模型路由规范：默认 `model=None`，统一通过 `preset_model_group` ("low" / "medium" / "high")
   复用 `app/ai_api/model_api.py` 的加权模型池与自动降级机制，绝不在业务配置中硬编码模型名。
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
# 二、可插拔内容风格 Profile 配置（不设任何固定字数区间牢笼，篇幅完全由信息密度决定）
# ==============================================================================
CONTENT_PROFILES: Dict[str, Dict[str, Any]] = {
    # 预设 1：X 平台中文全领域高网感原生推文（科技/AI、宏观政治、商业财富、交易实录、高共鸣奇闻/神梗）
    "zh_viral_general": {
        "display_name": "X平台中文全领域原生爆款号",
        "language": "zh",
        "step2_prompt_file": "step2_post_comprehension.md",
        "step3_prompt_file": "step3_topic_selection_default.md",
        "step4_prompt_file": "step4_creation_zh_viral.md",
        "topic_preference": (
            "面向 X (Twitter) 简中泛科技、商业财经、全球宏观大事件与加密交易高认知受众。优先挑选：\n"
            "1) 跨平台多源共振的重磅突发事件（科技/AI巨头核心动态、宏观地缘震荡、商业巨额诉讼/人事地震）；\n"
            "2) 有真实数据或实锤截图支撑的暴富/暴亏/套利奇闻（或评论区扒出惊人反转的案例）；\n"
            "3) 冲突张力极强、观点犀利或极具冷幽默传播力的独立好帖。\n"
            "全领域开放视野，不局限于单一币圈行情喊单，宁缺毋滥。"
        ),
        "style_guidelines": (
            "输出语言：自然地道的简体中文。\n"
            "原生推文语感：像一位见多识广、说话一针见血的业内真人在发 X 推文。第一句直切核心事实、反差数字或槽点；"
            "有话则长、无话则短，把话说透立刻收尾，绝不铺垫寒暄、绝不凑字数、绝不强行升华说教。"
        ),
    },
    # 预设 2：英文 X (Twitter) 原生高留存推文号（直接切换 ACTIVE_PROFILE 即可生效）
    "en_x_creator": {
        "display_name": "English X Native Creator",
        "language": "en",
        "step2_prompt_file": "step2_post_comprehension.md",
        "step3_prompt_file": "step3_topic_selection_default.md",
        "step4_prompt_file": "step4_creation_en_x.md",
        "topic_preference": (
            "Target global English X audience across Tech/AI, Macro/Geopolitics, Markets/Business, and Crypto.\n"
            "Prioritize multi-source breaking stories, hard-number asymmetric trades/debunkings, and sharp real-world ironies."
        ),
        "style_guidelines": (
            "Output Language: Crisp, idiomatic English native to X.\n"
            "Voice & Pacing: Lead immediately with the hardest number, contrast, or punchline. "
            "Let actual information density dictate length—never pad filler, never repeat text already obvious inside the attached screenshot, and stop right when the point lands."
        ),
    },
}

# 当前激活的创作 Profile（切换风格只需改这一行）
ACTIVE_PROFILE: str = "zh_viral_general"


# ==============================================================================
# 三、总控阶段开关与调度配置（直接在 IDE 运行 `pipeline_runner.py` 即可生效）
# ==============================================================================
STAGE_SWITCHES: Dict[str, bool] = {
    "step2_comprehension": True,    # Step 2：对新拉取帖子做多模态图文+热评忠实还原与自洽性体检
    "step3_topic_selection": True,  # Step 3：单窗口跨平台均衡审阅、按编号合并事件与选题
    "step4_creation": True,         # Step 4：X 原生文案撰写 + 真实原图复用/裁剪/拼接/去边角水印
}

SCHEDULE_CONFIG: Dict[str, Any] = {
    "continuous_loop": False,       # False = 单次跑完完整流水线即退出（方便调试看效果）；True = 常驻定时循环
    "interval_seconds": 3600,       # 常驻模式下每轮间隔秒数（3600 = 1小时）
}


# ==============================================================================
# 四、各阶段工程参数（统一走 `preset_model_group` 模型组，所有上限均对齐平台客观物理边界）
# ==============================================================================
STEP2_CONFIG: Dict[str, Any] = {
    # 扫描最近多少小时内入库/发布的原始帖子
    "lookback_hours": 48,
    # 单轮最多新理解多少条未处理帖子（按 5 大平台轮询均衡抽取，冷启动各取 Top 8 共 40 条，日常每小时增量约 20~35 条可一次消化）
    "max_new_posts_per_run": 40,
    # 并发工作线程数
    "concurrency": 5,
    # 单帖最多挂载几张主帖原图（对齐 X 平台单帖原生 4 图物理上限，仅取主帖图，绝不混入评论区无关表情包）
    "max_images_per_post": 4,
    # 单帖最多提取前多少条评论（对齐 Step 1 单帖拉取的前 10 条热评）
    "max_comments_per_post": 10,
    # 是否强制重算已有情报卡（日常增量运行保持 False，调试新提示词时可临时改 True）
    "force_recompute": False,
    # 模型组调度：model=None 时由 model_api.py 按权重调度组内模型并自动跨渠道降级兜底
    "model": None,
    "preset_model_group": "medium",
    "timeout": 600,
}

STEP3_CONFIG: Dict[str, Any] = {
    # 纳入全局选题视野的情报卡时间窗口（小时）
    "lookback_hours": 36,
    # 单窗口最多放入多少条可用情报卡（按各平台均衡轮询填入，约 1.5 万 Tokens 处于单窗口高召回区）
    "max_candidates_in_window": 120,
    # 每轮最多精选几个话题进入 Step 4 生产（宁缺毋滥，允许少于该上限）
    "target_topic_count": 3,
    # 防重复记忆窗口：过去多少小时内已被消费过的原帖不再重复入选，并将已产话题注入防重列表
    "dedup_used_posts_hours": 72,
    "model": None,
    "preset_model_group": "medium",
    "timeout": 600,
}

STEP4_CONFIG: Dict[str, Any] = {
    # 创作并发数
    "concurrency": 2,
    # 单次运行最多生产几篇待产选题
    "max_creations_per_run": 5,
    # 单个选题最多挂载多少张真实可用候选原图供主笔模型直接看图裁切/拼接（1:1 物理附件对齐）
    "max_candidate_images_per_topic": 4,
    # 是否启用本地真实像素视觉工作室（仅执行 reuse / crop / stitch / 边角小水印消除 / none，严禁 AI 生图）
    "enable_visual_studio": True,
    "model": None,
    "preset_model_group": "medium",
    "timeout": 600,
}
