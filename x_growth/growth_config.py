# -*- coding: utf-8 -*-
"""
X 平台潜力用户评论生产与独立发布配置 (`x_growth/growth_config.py`)

极简双模块架构：
1. `comment_pipeline.py` : 唯一核心入参 `user_data_dir` -> 拉推荐流找未建立关注关系的同体量潜力蓝 V ->
                           拉每人 24h 内最新 1 帖入 `harvested_social_posts` ->
                           纯文本 5 帖/批用 `low` 模型组生成口语评论并标记分类与语种 (`language`) ->
                           独立 `medium` 模型组按“去人机味真人盲测”严苛打分写入嵌套字段 `auto_comment`。
2. `comment_publisher.py`: 纯查数据库 `harvested_social_posts` -> 按账号定位分类 (`categories`)、语种 (`languages`) 与高分排序筛选 ->
                           核验认证/24h内/未关注关系 -> 发送评论并回写 `auto_comment.published`。
"""
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from common.common_utils import PROMPT_DIR, get_config, setup_logger
from platforms.harvesters import NEW_HARVESTED_COLLECTION_NAME

TZ_CN = timezone(timedelta(hours=8))

X_GROWTH_PROMPT_DIR: Path = PROMPT_DIR / "x_growth"
X_GROWTH_PROMPT_DIR.mkdir(parents=True, exist_ok=True)

# 复用统一的帖子数据表，所有生成/分类/语种/评分/发布状态统一收纳在该单一嵌套字段下，绝不污染顶层结构
POSTS_COLLECTION_NAME: str = NEW_HARVESTED_COLLECTION_NAME  # "harvested_social_posts"
AUTO_COMMENT_FIELD: str = "auto_comment"

# 仅针对高收益验证语种区（英语 en、日语 ja）生成与发布同语种评论
SUPPORTED_COMMENT_LANGUAGES: Tuple[str, ...] = ("en", "ja")

# 预定义通用一级帖子分类体系（生成评论时强制从中单选其一，发布时可按当前运营账号定位灵活筛选）
POST_CATEGORIES: Dict[str, str] = {
    "tech_ai": "人工智能、编程开发、科技数码、互联网产品",
    "crypto_web3": "加密资产、区块链、链上交易、Web3",
    "finance_macro": "股票市场、宏观经济、商业投资、创业经营",
    "news_politics": "国际时事、社会热点、政策法规、公共议题",
    "gaming_anime": "电子游戏、动漫二次元、电竞赛事",
    "entertainment_pop": "影视音乐、明星网红、流行文化、幽默热梗",
    "sports_fitness": "体育赛事、竞技运动、健身健康",
    "lifestyle_emotion": "日常生活、情感共鸣、职场成长、旅行美食",
    "science_culture": "自然科学、硬核科普、历史人文、阅读哲思",
    "other": "不属于以上领域的其他综合内容",
}


def format_category_list_for_prompt() -> str:
    """将预定义分类体系渲染为提示词枚举列表。"""
    return "\n".join(f"- `{key}`：{desc}" for key, desc in POST_CATEGORIES.items())


def normalize_language_code(lang: str) -> str:
    """归一化语言标识为简洁小写 ISO-639-1 主码（如 zh-CN -> zh, en-US -> en）。"""
    raw = str(lang or "").strip().lower().replace("_", "-")
    if not raw:
        return "unknown"
    base = raw.split("-", 1)[0].strip()
    return base or "unknown"


def get_configured_self_handles() -> List[str]:
    """收集 config.json 中记录的自有账号名（小写），避免把自己的号筛进候选池。"""
    handles: List[str] = []
    for key in ("x_xiaohao_screen_name", "x_myself_screen_name", "x_dahao_screen_name"):
        val = str(get_config(key, "") or "").strip().lstrip("@").lower()
        if val and val not in handles:
            handles.append(val)
    return handles


def now_iso_cn() -> str:
    return datetime.now(TZ_CN).isoformat(timespec="seconds")


_LOGGER: Optional[logging.Logger] = None


def get_growth_logger() -> logging.Logger:
    global _LOGGER
    if _LOGGER is None:
        _LOGGER = setup_logger(app_name="x_growth")
    return _LOGGER


# 模块一默认参数（均可在调用函数时覆盖，日常仅需传 user_data_dir）
PIPELINE_DEFAULTS: Dict[str, Any] = {
    "feed_fetch_count": 30,             # 推荐流 (For You) 单次拉取条数
    "feed_detail_concurrency": 2,       # 温和拉取推荐帖评论区以提取评论用户的线程数（防并发突发风控）
    "min_followers": 100,               # 潜力认证用户最小粉丝数（过滤纯空壳号）
    "max_followers": 3000,              # 潜力认证用户最大粉丝数（聚焦会认真看每条评论且高意愿回关的同体量个体号）
    "max_target_users_per_run": 15,     # 单轮最多拉取多少位潜力用户的最新帖（温和节奏防 code=326 风控）
    "post_max_age_seconds": 86400,      # 只保留最近一天（24h）内发布的帖子
    "allowed_languages": SUPPORTED_COMMENT_LANGUAGES,  # 仅拉取并生成英语(en)与日语(ja)帖子的评论
    "comment_batch_size": 5,            # 每次批量生成 5 条帖子的评论、分类与语种
    "score_batch_size": 6,              # 独立评分模型单批横向打分条数上限
    "generator_model_group": "low",     # 评论、分类与语种生成模型组（轻量高性价比）
    "scorer_model_group": "medium",     # 独立第三方打分模型组（高于生成层一级，物理隔离严禁自评）
    "max_weighted_chars": 260,          # 单条回复加权字符物理上限
    "llm_timeout": 300,
}

# 模块二默认参数（独立发评论模块）
PUBLISHER_DEFAULTS: Dict[str, Any] = {
    "allowed_categories": None,         # None 或空列表表示不限分类；传如 ["tech_ai", "crypto_web3"] 则只发对应赛道
    "allowed_languages": SUPPORTED_COMMENT_LANGUAGES,  # 默认仅允许发布 en / ja 语种评论（单号运营可进一步传 ["en"] 或 ["ja"]）
    "min_score": 70,                    # 仅发布独立盲测评分 >= 该分值的自然可用评论
    "max_publish_per_run": 3,           # 单次调用最多发送几条高分评论
    "post_max_age_seconds": 86400,      # 仅发布 24 小时以内的新鲜帖子
    "author_dedup_days": 7,             # 同一作者多少天内不重复发评打扰
    "like_when_commenting": True,       # 发评论时顺手给原帖点赞
    "also_follow": False,               # 发评论时是否顺手关注该作者
    "playwright_debug": False,          # 是否弹出有头浏览器调试窗口
    "action_delay_range": (10, 22),     # 多条真实发送之间的随机拟人间隔秒数
}
