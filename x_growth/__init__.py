# -*- coding: utf-8 -*-
"""
X 平台极简双模块涨粉与共鸣评论引擎 (`x_growth`)

1. `run_comment_pipeline(user_data_dir)`   : 推荐流挖掘同体量潜力蓝V -> 拉每人24h最新1帖入库 -> 5帖纯文本用 `low` 组生成口语评论+分类+语种 -> 独立 `medium` 组盲测去人机味打分存入嵌套字段 `auto_comment`；
2. `publish_comments_from_db(user_data_dir)`: 纯查数据库按账号分类属性、评论语种与分数排序发送评论。
"""
from x_growth.comment_pipeline import run_comment_pipeline
from x_growth.comment_publisher import publish_comments_from_db
from x_growth.growth_config import (
    AUTO_COMMENT_FIELD,
    PIPELINE_DEFAULTS,
    POST_CATEGORIES,
    PUBLISHER_DEFAULTS,
    SUPPORTED_COMMENT_LANGUAGES,
    normalize_language_code,
)

__all__ = [
    "AUTO_COMMENT_FIELD",
    "POST_CATEGORIES",
    "SUPPORTED_COMMENT_LANGUAGES",
    "PIPELINE_DEFAULTS",
    "PUBLISHER_DEFAULTS",
    "normalize_language_code",
    "run_comment_pipeline",
    "publish_comments_from_db",
]
