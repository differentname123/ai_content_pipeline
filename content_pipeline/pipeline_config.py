# -*- coding: utf-8 -*-
"""
流水线全局配置中心 (`pipeline_config.py`)

================================================================================
【日常扩展与调优指南（无需改动引擎代码）】
1. 切换生成风格/语种/账号定位：修改 `ACTIVE_PROFILE`（或往 `CONTENT_PROFILES` 新增一项并指向对应 `.md` 提示词文件）。
2. 单独调试某一步：修改 `STAGE_SWITCHES` 中的布尔开关，或直接运行对应步骤脚本。
3. 切换单次运行 / 常驻定时循环：修改 `SCHEDULE_CONFIG["continuous_loop"]`。
4. 模型路由规范：统一保持 `model=None` + `preset_model_group="medium"` + `timeout=600`，
   自动走 `app/ai_api/model_api.py` 的加权模型池负载均衡与跨渠道容灾兜底，绝不硬编码模型名。
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
    # Step 2 单帖多模态完整情报档案集合（按 source + post_id 唯一幂等存储）
    "post_digests": "pipeline_post_digests",
    # Step 3 全局单窗口归堆与选题集合
    "topic_clusters": "pipeline_topic_clusters",
    # Step 4 & Step 5 原创图文成品与读者盲测终审集合
    "creations": "pipeline_creations",
    # 全链路运行审计与调优追踪集合
    "run_traces": "pipeline_run_traces",
}


# ==============================================================================
# 二、可插拔内容风格 Profile 配置（纯抽象原则，不设任何固定字数区间或具体案例框框）
# ==============================================================================
CONTENT_PROFILES: Dict[str, Dict[str, Any]] = {
    # 预设 1：中文社交平台全领域原生创作者号
    "zh_viral_general": {
        "display_name": "中文全领域原生内容号",
        "language": "zh",
        "step2_prompt_file": "step2_post_comprehension.md",
        "step3_prompt_file": "step3_topic_selection_default.md",
        "step4_prompt_file": "step4_creation_zh_viral.md",
        "step5_prompt_file": "step5_cold_reader_review.md",
        "topic_preference": (
            "面向广泛关注科技商业、宏观政经、金融交易与社会真实故事的公众读者。优先挑选：\n"
            "1) 多源讨论共振、信息密度高的重要突发事件或前沿动态；\n"
            "2) 具备扎实事实数据、鲜明因果反转或真实个体经历的深度与反差议题；\n"
            "3) 冲突张力鲜明、洞察犀利或天然具备幽默共鸣感的高质量独立内容。\n"
            "全领域开放视野，不局限于单一垂直行业，宁缺毋滥。"
        ),
        "style_guidelines": (
            "输出语言：自然地道的简体中文。\n"
            "表达基调：以洞察敏锐、表达利落的独立创作者口吻直接面向公众发声；"
            "对陌生背景自然融入简明交代确保零门槛顺畅阅读，直切核心看点，篇幅随内容实质自然伸缩，讲透即止。"
        ),
    },
    # 预设 2：英文社交平台全领域原生创作者号
    "en_x_creator": {
        "display_name": "English Native Social Creator",
        "language": "en",
        "step2_prompt_file": "step2_post_comprehension.md",
        "step3_prompt_file": "step3_topic_selection_default.md",
        "step4_prompt_file": "step4_creation_en_x.md",
        "step5_prompt_file": "step5_cold_reader_review.md",
        "topic_preference": (
            "Target a broad, sharp English-speaking audience across technology, business, macro events, markets, and compelling human stories.\n"
            "Prioritize multi-source breaking developments, verifiable asymmetric outcomes or reversals, and high-resonance standalone insights."
        ),
        "style_guidelines": (
            "Output Language: Crisp, idiomatic English.\n"
            "Voice & Pacing: Write directly as an authentic public creator; weave brief context naturally for unfamiliar entities so any reader grasps the full picture effortlessly, lead with the sharpest point, and let real substance dictate length."
        ),
    },
}

# 当前激活的创作 Profile（切换风格只需改这一行）
ACTIVE_PROFILE: str = "zh_viral_general"


# ==============================================================================
# 三、总控阶段开关与调度配置（直接在 IDE 运行 `pipeline_runner.py` 即可生效）
# ==============================================================================
STAGE_SWITCHES: Dict[str, bool] = {
    "step2_comprehension": True,    # Step 2：单帖「文字+全部主帖原图+前排评论」多模态无损还原
    "step3_topic_selection": True,  # Step 3：全局单窗口同事件归并及高潜选题
    "step4_creation": True,         # Step 4：原生初稿创作 + 真实原图确定性编辑
    "step5_blind_review": True,     # Step 5：零背景普通网友视角盲测终审（最多 1 次定点打磨）
}

SCHEDULE_CONFIG: Dict[str, Any] = {
    "continuous_loop": False,       # False = 单次跑完完整流水线即退出（方便调试看效果）；True = 常驻定时循环
    "interval_seconds": 3600,       # 常驻模式下每轮间隔秒数（3600 = 1小时）
}


# ==============================================================================
# 四、各阶段工程参数（零脆弱硬编码门槛：不截断图片张数、不设像素门槛、不剔除历史已用帖）
# ==============================================================================
STEP2_CONFIG: Dict[str, Any] = {
    # 严格按真实发布时间 `publish_time` 扫描最近多少小时内的帖子
    "lookback_hours": 48,
    # 入口确定性排除所有含视频帖子（免除一切未下载视频猜测与视频适配复杂度）
    "exclude_video_posts": True,
    # 单轮最多新理解多少条未处理帖子（按各平台公平轮询抽取，已理解过的直接命中缓存）
    "max_new_posts_per_run": 50,
    # 并发工作线程数
    "concurrency": 5,
    # 单帖最多附带前多少条高赞评论
    "max_comments_per_post": 10,
    # 是否强制重算已有单帖档案（日常增量运行保持 False）
    "force_recompute": False,
    # 统一走模型组负载均衡
    "model": None,
    "preset_model_group": "medium",
    "timeout": 600,
}

STEP3_CONFIG: Dict[str, Any] = {
    # 纳入全局选题视野的单帖真实发布时间窗口（小时）
    "lookback_hours": 36,
    # 单窗口最多放入多少条可用单帖档案（按跨平台公平轮询填入）
    "max_candidates_in_window": 120,
    # 每轮最多挑选几个话题进入生产（宁缺毋滥，允许少于该上限）
    "target_topic_count": 3,
    "model": None,
    "preset_model_group": "medium",
    "timeout": 600,
}

STEP4_CONFIG: Dict[str, Any] = {
    # 创作并发数
    "concurrency": 2,
    # 单次运行最多生产几篇选题
    "max_creations_per_run": 5,
    # 是否启用本地真实像素视觉工作室（仅执行 reuse / crop / stitch / 小角标消除 / none，严禁 AI 生图）
    "enable_visual_studio": True,
    "model": None,
    "preset_model_group": "medium",
    "timeout": 600,
}

STEP5_CONFIG: Dict[str, Any] = {
    # 读者盲测并发数
    "concurrency": 3,
    # 当盲测返回 `revise` 时最多交回 Step 4 定点打磨几次（严格 <= 1，绝不死循环）
    "max_revisions": 1,
    "model": None,
    "preset_model_group": "medium",
    "timeout": 600,
}
