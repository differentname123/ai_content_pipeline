# -*- coding: utf-8 -*-
"""
双平台（Binance / OKX）流水线统一配置中心 (`parser/config.py`)

所有面向人的可调业务参数集中在此（任务启停开关直接置顶在 `parser/article_format_logical.py`）：
1. 提示词版本与模板文件名；
2. 账号发帖冷却与同币冷却时长；
3. 原帖采集、初筛、高流量筛选、文章生成与互动参数。
"""
import re
from pathlib import Path
from typing import Any, Dict

from common.common_utils import (
    PLATFORM_BINANCE,
    PLATFORM_OKX,
    PROMPT_DIR,
    get_runtime_file_path,
)

# ==============================================================================
# 1. 提示词版本与模板配置
# ==============================================================================
prompt_version = "1125-2"
SELECTION_PROMPT_VERSION = "20261125-v9_platform_scorer_decoupled"

PRE_SCREEN_PROMPT_NAME = "内容生成方案_分析类MLU初筛.txt"
MLU_EXTRACT_PROMPT_NAME = "内容生成方案_分析类MLU提取.txt"
ARTICLE_PROMPT_NAME = "内容生成方案_分析类文章生成.txt"
ANALYSIS_EDITOR_PROMPT_NAME = "内容生成方案_分析类_发布前总编精选.txt"
VIRAL_STAGE1_PROMPT_NAME = "内容生成方案_娱乐类_单帖可用性筛选.txt"
VIRAL_STAGE2_PROMPT_NAME = "内容生成方案_娱乐类_单帖洗稿和神评生成.txt"
VIRAL_EDITOR_PROMPT_NAME = "内容生成方案_娱乐类_发布前总编精选.txt"

PRE_SCREEN_PROMPT_FILE_PATH = str(PROMPT_DIR / PRE_SCREEN_PROMPT_NAME)
PROMPT_FILE_PATH = str(PROMPT_DIR / MLU_EXTRACT_PROMPT_NAME)
ARTICLE_PROMPT_FILE_PATH = str(PROMPT_DIR / ARTICLE_PROMPT_NAME)
ANALYSIS_EDITOR_PROMPT_FILE_PATH = str(PROMPT_DIR / ANALYSIS_EDITOR_PROMPT_NAME)
VIRAL_STAGE1_PROMPT_FILE_PATH = str(PROMPT_DIR / VIRAL_STAGE1_PROMPT_NAME)
VIRAL_STAGE2_PROMPT_FILE_PATH = str(PROMPT_DIR / VIRAL_STAGE2_PROMPT_NAME)
VIRAL_EDITOR_PROMPT_FILE_PATH = str(PROMPT_DIR / VIRAL_EDITOR_PROMPT_NAME)

# ==============================================================================
# 2. 账号发布冷却与互动开关
# ==============================================================================
STATE_FILE = str(
    get_runtime_file_path(
        "account_publish_state.json",
        legacy_path=Path(__file__).resolve().with_name("account_publish_state.json"),
    )
)

ACCOUNT_COOLDOWN_SECONDS = 3 * 3600       # Binance 单账号全局发帖间隔（合并分析帖与流量帖，至少 2h；默认 3h 对应单号 <=8 篇/天健康节奏）
OKX_ACCOUNT_COOLDOWN_SECONDS = 3 * 3600   # OKX 单账号全局发帖间隔（合并分析帖与流量帖，至少 2h；默认 3h 对应单号 <=8 篇/天健康节奏）
ACCOUNT_TOPIC_COOLDOWN_SECONDS = 2 * 3600 # 同账号同币种冷却（2h）
WAF_BACKOFF_SCHEDULE_SECONDS = (1800, 3600, 7200)  # HTTP 405 / WAF 人机验证单账号阶梯退避（30m -> 60m -> 120m 封顶）
AUTH_EXPIRED_BACKOFF_SECONDS = 3600                # Cookie / 登录态失效单账号自动退避（60m，支持人工补登后自动恢复）

INTERACTION_CONFIG: Dict[str, Dict[str, Any]] = {
    PLATFORM_OKX: {
        "enable_like": False,
        "enable_comment": False,
        "max_comment_accounts": 1,
    },
    PLATFORM_BINANCE: {
        "enable_like": True,
        "enable_comment": True,
        "max_comment_accounts": 1,
    },
}

# ==============================================================================
# 3. 采集、筛选、生成与发布核心业务阈值
# ==============================================================================
BINANCE_SOURCE = PLATFORM_BINANCE
OKX_SOURCE = PLATFORM_OKX

POST_QUERY_LIMIT = 50000
POST_MAX_AGE_HOURS = 48
MAX_MEDIA_COUNT = 10
MAX_CONCURRENCY = 20
LLM_MAX_RETRIES = 3
PRE_SCREEN_BATCH_SIZE = 50
PRE_SCREEN_CONCURRENCY = 5
# 单轮交替流控上限：防止版本切换初期全量历史积压在初筛阶段阻塞下游最新原帖的论据精提与分析生成
PRE_SCREEN_MAX_PER_ROUND = 1000
MLU_FORMAT_MAX_PER_ROUND = 100

VIRAL_STAGE1_BATCH_SIZE = 50
VIRAL_POST_MAX_AGE_HOURS = 24
VIRAL_PUBLISH_MAX_AGE_HOURS = 24
VIRAL_MIN_VIEW_COUNT_BINANCE = 1000
VIRAL_MIN_VIEW_COUNT_OKX = 10000
VIRAL_MIN_VIEW_COUNTS: Dict[str, int] = {
    PLATFORM_BINANCE: VIRAL_MIN_VIEW_COUNT_BINANCE,
    PLATFORM_OKX: VIRAL_MIN_VIEW_COUNT_OKX,
}

ARTICLE_TYPE_ANALYSIS = "analysis"
ARTICLE_TYPE_VIRAL = "viral"
ARTICLE_MATERIAL_LIMIT = 10
ARTICLE_MIN_MATERIALS = 5
ARTICLE_POST_USAGE_LIMIT = 2
ARTICLE_MAX_CHARS = 800  # 生成层物理防失控上限；具体平台甜区字数偏置由发布层平台评分模型独立决定
ARTICLE_VARIANTS = 1
ARTICLE_CONCURRENCY = 5
ARTICLE_GENERATION_INTERVAL_SECONDS = 1200
ANALYSIS_GENERATION_INTERVAL_SECONDS = 3 * 3600  # 单币分析文章生成间隔（3 小时；匹配 6h 发布窗口与 6h 账号冷却，大幅减少无效重复生成）

PUBLISH_MAX_ATTEMPTS = 3
PUBLISH_POLL_SECONDS = 600
METRICS_SYNC_INTERVAL_SECONDS = 600
INTERACTION_INTERVAL_SECONDS = 1200
MAX_REPLAY_DAYS = 7
DELETE_INTERVAL_SEC = 3600
IMAGE_DEDUP_LOOKBACK_HOURS = 168  # 7 天内跨文章类型视觉内容哈希去重

ALLOWED_IMAGE_FORMATS = {"JPEG", "PNG", "WEBP", "GIF", "BMP", "MPO"}
MEDIA_PATTERN = re.compile(r"\[(插图|长文封面|视频封面|视频):\s*(https?://[^\]]+)\]")

SHELF_LIFE_SECONDS: Dict[str, int] = {
    "hours": 2 * 3600,
    "days": 24 * 3600,
    "weeks": 7 * 24 * 3600,
    "long": 15 * 24 * 3600,
    "unknown": 24 * 3600,
}

PUBLISH_RULES: Dict[str, Dict[str, Any]] = {
    ARTICLE_TYPE_ANALYSIS: {
        "action": "分析发布",
        "hours": 6,
        "text": "text",
        "images": "image_placeholders",
        "score": "score",
        "state_prefix": "",
    },
    ARTICLE_TYPE_VIRAL: {
        "action": "流量发布",
        "hours": VIRAL_PUBLISH_MAX_AGE_HOURS,
        "text": "post_text",
        "images": "selected_images",
        "score": "viral_score",
        "state_prefix": "viral_",
    },
}
