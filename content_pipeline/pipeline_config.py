# -*- coding: utf-8 -*-
"""
模块化多模态内容生产平台全局配置中心 (`pipeline_config.py`)

================================================================================
【架构准则与日常调优指南】
1. 高内聚 · 低耦合 · 独立可拼装：
   - 模块 1 (`harvester_runner.py`)   : `harvest_broad_feeds()` / `harvest_by_keywords(keywords)`
   - 模块 2 (`step2_comprehension.py`): `run_step2_comprehension()` / `comprehend_posts_by_keys(post_keys)`
   - 模块 3 (`topic_radar.py`)        : `discover_hot_directions()`（支持单窗口全量 或 超量自动分批归并）
   - 模块 4 (`direction_expander.py`) : `expand_direction_across_platforms(direction)`（按方向关键词指哪打哪深挖）
   - 模块 5 (`material_curator.py`)   : `curate_materials_for_direction(direction, digests)`（剔除跑题搜索噪音）
   - 模块 6 (`content_creator.py`)    : `create_from_materials(materials_bundle)`（与上游发现/检索 100% 解耦）
   - 模块 7 (`blind_reviewer.py`)     : `review_and_polish_creation(work_doc, materials_bundle)`（可插拔零背景网友盲测）
   - 模块 8 (`comment_creator.py`)    : `generate_comments_from_post(post_doc)`（全语种全领域独立帖子神评生成，100% 解耦）
2. 物理隔离：与历史旧集合（`social_media_posts` / `generated_articles`）100% 隔离，绝不触碰旧业务数据。
3. 模型路由规范：统一保持 `model=None` + `preset_model_group="medium"` + 快速故障切换超时 (`150s~180s`)，
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
    # 模块 1 产出的标准化原始帖子集合（下游模块仅读取最新原帖与评论，写入统一经由 HarvestedPostManager）
    "raw_posts": "harvested_social_posts",
    # 模块 2 单帖多模态完整情报档案集合（按 source + post_id 唯一幂等存储）
    "post_digests": "pipeline_post_digests",
    # 模块 3~5 热点方向发现、跨平台关键词深挖与精选素材集合
    "directions": "pipeline_directions",
    # 兼容保留的历史归堆集合名（供一键清理维护使用）
    "topic_clusters": "pipeline_topic_clusters",
    # 模块 6~7 原创图文成品与网友盲测终审集合
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
        "radar_prompt_file": "step3_topic_radar.md",
        "curation_prompt_file": "step4_material_curation.md",
        "creation_prompt_file": "step4_creation_zh_viral.md",
        "review_prompt_file": "step5_cold_reader_review.md",
        "topic_preference": (
            "面向广泛关注科技商业、宏观政经、金融交易与社会真实故事的公众读者。优先挑选：\n"
            "1) 多源讨论共振、信息密度高的重要突发事件或前沿动态；\n"
            "2) 具备扎实事实数据、鲜明因果反转或真实个体经历的深度与反差议题；\n"
            "3) 冲突张力鲜明、洞察犀利或天然具备幽默共鸣感的高质量独立内容。\n"
            "全领域开放视野，不局限于单一垂直行业，宁缺毋滥。"
        ),
        "style_guidelines": (
            "输出语言：自然地道的简体中文。\n"
            "发布场景与表达基调：面向社交平台信息流（如 X / 社区动态流）读者，以洞察敏锐、表达利落的独立创作者口吻直接面向公众发声；"
            "对陌生背景自然融入简明交代确保零门槛顺畅阅读，开篇直切核心看点，行文紧凑干练、不铺陈水词，篇幅完全随内容实质自然伸缩，讲透即止。"
        ),
    },
    # 预设 2：英文社交平台全领域原生创作者号
    "en_x_creator": {
        "display_name": "English Native Social Creator",
        "language": "en",
        "step2_prompt_file": "step2_post_comprehension.md",
        "radar_prompt_file": "step3_topic_radar.md",
        "curation_prompt_file": "step4_material_curation.md",
        "creation_prompt_file": "step4_creation_en_x.md",
        "review_prompt_file": "step5_cold_reader_review.md",
        "topic_preference": (
            "Target a broad, sharp English-speaking audience across technology, business, macro events, markets, and compelling human stories.\n"
            "Prioritize multi-source breaking developments, verifiable asymmetric outcomes or reversals, and high-resonance standalone insights."
        ),
        "style_guidelines": (
            "Output Language: Crisp, idiomatic English.\n"
            "Voice & Pacing: Write directly for a fast-moving social timeline (e.g., X) as an authentic public creator; "
            "weave brief context naturally for unfamiliar entities so any reader grasps the full picture effortlessly, lead with the sharpest point, cut filler, and let real substance dictate length."
        ),
    },
}

# 当前激活的创作 Profile（切换风格只需改这一行）
ACTIVE_PROFILE: str = "zh_viral_general"


# ==============================================================================
# 三、总控阶段开关与调度配置（每个模块亦可独立 import 调用）
# ==============================================================================
STAGE_SWITCHES: Dict[str, bool] = {
    "harvest_broad": False,            # 可选：是否在跑流水线前先触发一轮全平台广谱采集
    "step2_comprehension": True,       # 模块 2：单帖「正文+外链原文+全量配图+前排评论」高并发理解
    "step3_discover_directions": True, # 模块 3：热点方向与搜索词发现（支持单窗口全量或自动分批归并）
    "step4_targeted_search": True,     # 模块 4：按方向关键词跨五平台「指哪打哪」深挖 + 新帖即时理解
    "step5_curate_materials": True,    # 模块 5：方向素材精选（剔除关键词碰撞与跑题噪音，仅留同向高价值帖）
    "step6_create_content": True,      # 模块 6：高内聚低耦合原创图文创作 + 真实原图确定性编辑
    "step7_blind_review": True,        # 模块 7：可插拔 100% 物理上下文隔离普通网友盲测（最多 1 次打磨）
}

SCHEDULE_CONFIG: Dict[str, Any] = {
    "continuous_loop": False,       # False = 单次跑完完整流水线即退出；True = 常驻定时循环
    "interval_seconds": 3600,       # 常驻模式下每轮间隔秒数（3600 = 1小时）
}


# ==============================================================================
# 四、各独立模块工程参数（统一走模型组负载均衡，零脆弱硬编码门槛）
# ==============================================================================
STEP2_CONFIG: Dict[str, Any] = {
    # 严格按真实发布时间 `publish_time` 扫描最近多少小时内的帖子
    "lookback_hours": 48,
    # 入口确定性排除所有含视频帖子
    "exclude_video_posts": True,
    # 单轮广谱理解最多新处理多少条未理解帖子（提升至 250 以当轮完整吃透五大平台全量热榜，已有缓存帖 0 开销跳过）
    "max_new_posts_per_run": 250,
    # 高并发工作线程数（避免个别慢速通道阻塞整体进度）
    "concurrency": 12,
    # 单帖理解时附带前多少条高赞评论（注：已理解过的帖子绝不因后续新增评论重跑理解，最新评论在创作阶段直读原表补齐）
    "max_comments_per_post": 10,
    # 单帖理解跨轮次最大尝试次数：单帖若累计失败达到 3 次则标记为 abandoned，后续轮次不再重复捞取理解
    "max_comprehension_attempts": 3,
    # 是否强制重算已有单帖档案（日常运行保持 False，严格幂等复用缓存）
    "force_recompute": False,
    "model": None,
    "preset_model_group": "medium",
    "timeout": 150,
}

RADAR_CONFIG: Dict[str, Any] = {
    # 纳入热点方向发现的真实发布时间窗口（小时），与 Step 2 保持统一
    "lookback_hours": 48,
    # 单窗口最大消化条数：候选数 <= batch_size 时单窗口一次通览；超过时自动分批并发提取再二次归并，绝不丢弃候选
    "batch_size": 80,
    "batch_concurrency": 4,
    # 每轮最多发现几个值得深挖创作的方向（宁缺毋滥，允许少于该上限）
    "max_directions": 3,
    # 回看最近多少小时内已产出的选题摘要，防止连续轮次重复炒同一旧闻（除非有重大新进展）
    "recent_created_dedup_hours": 72,
    "model": None,
    "preset_model_group": "medium",
    "timeout": 180,
}

EXPANDER_CONFIG: Dict[str, Any] = {
    # 每个方向在每个开启平台上最多按关键词检索多少条近期高相关帖子
    "per_platform_limit": 25,
    # 关键词定向检索的时间窗口（小时）
    "hours_back": 72,
    # 检索命中时同步拉取前排评论、外链全文并下载真实图片
    "fetch_comments": True,
    "fetch_external_links": True,
    "download_media": True,
}

CURATOR_CONFIG: Dict[str, Any] = {
    "concurrency": 3,
    "model": None,
    "preset_model_group": "medium",
    "timeout": 150,
}

CREATOR_CONFIG: Dict[str, Any] = {
    "concurrency": 3,
    "max_creations_per_run": 5,
    # 直接从 `harvested_social_posts` 读取每条关联原帖最新前多少条高赞原始评论注入创作上下文
    "max_raw_comments_per_post": 15,
    # 配图处理模式：默认 `"select_only"` = 只从真实原图中直接挑选（0张/1张/多张原封不动无损复用），不做任何裁剪、拼接或去水印
    "image_mode": "select_only",
    "enable_visual_studio": True,
    "model": None,
    "preset_model_group": "medium",
    "timeout": 180,
}

REVIEWER_CONFIG: Dict[str, Any] = {
    "concurrency": 3,
    # 当盲测返回 `revise` 时最多交回创作模块定点打磨几次（严格 <= 1，绝不死循环）
    "max_revisions": 1,
    "model": None,
    "preset_model_group": "medium",
    "timeout": 150,
}

COMMENT_CREATOR_CONFIG: Dict[str, Any] = {
    "prompt_file": "post_comment_creation.md",
    # 单次默认生成多少条切入维度彼此独立的候选评论（首条即为最优直发推荐）
    "candidate_count": 3,
    # 注入提示词的「原帖作者本人补充/自评」与「社区前排热评」最大条数
    "max_author_comments": 10,
    "max_community_comments": 15,
    # 社交平台非认证账号单条回复物理加权字符红线（X 硬上限 280，预留 20 字符安全余量）
    "max_weighted_chars": 260,
    # 当输入的帖子含尚未落盘的远程配图时，自动下载到本地供多模态模型直接看图理解
    "download_missing_media": True,
    # X 平台默认使用的会话目录配置键（对应 config/config.json 中的 x_xiaohao_browser_session_dir）
    "x_session_config_key": "x_xiaohao_browser_session_dir",
    "model": None,
    "preset_model_group": "medium",
    "timeout": 180,
}

# 向后兼容别名
STEP3_CONFIG = RADAR_CONFIG
STEP4_CONFIG = CREATOR_CONFIG
STEP5_CONFIG = REVIEWER_CONFIG
