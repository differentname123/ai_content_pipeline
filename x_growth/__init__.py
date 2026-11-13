# -*- coding: utf-8 -*-
"""
X 平台极简低耦合评论系统 (`x_growth`)

职责边界（各模块均可独立直接运行）：
1. `x_browser_collector.py` (`collect_and_save_posts`)       : 纯真实桌面 Chrome 被动捕获并 100% 落库，零大模型依赖；
2. `comment_generator.py` (`generate_all_pending_comments`)  : 纯查库按 15 条/波循环生成同语种评论并盲测打分，直至库内合格待评帖 100% 清零，零浏览器依赖；
3. `comment_pipeline.py` (`run_comment_pipeline`)            : 薄编排入口，顺序执行「拉取落库 -> 循环清零生成」；
4. `comment_publisher.py` (`publish_comments_from_db`)       : 多账号按自身语种、分类、分数消费公共池并回写 `consumed=True`、`consumed_by`、`consumed_at` 锁帖。
"""
from typing import Any

from x_growth.growth_config import (
    AUTO_COMMENT_FIELD,
    COLLECTOR_DEFAULTS,
    GENERATOR_DEFAULTS,
    PIPELINE_DEFAULTS,
    PIPELINE_SCHEDULE_CONFIG,
    POST_CATEGORIES,
    PUBLISHER_DEFAULTS,
    SUPPORTED_COMMENT_LANGUAGES,
    normalize_language_code,
)

__all__ = [
    "AUTO_COMMENT_FIELD",
    "POST_CATEGORIES",
    "SUPPORTED_COMMENT_LANGUAGES",
    "COLLECTOR_DEFAULTS",
    "GENERATOR_DEFAULTS",
    "PIPELINE_SCHEDULE_CONFIG",
    "PIPELINE_DEFAULTS",
    "PUBLISHER_DEFAULTS",
    "normalize_language_code",
    "collect_posts_via_browser",
    "collect_and_save_posts",
    "run_collector_daemon",
    "generate_all_pending_comments",
    "run_generator_daemon",
    "run_comment_pipeline",
    "run_comment_pipeline_daemon",
    "publish_comments_from_db",
    "run_comment_publisher_daemon",
]


def __getattr__(name: str) -> Any:
    if name in ("collect_posts_via_browser", "collect_and_save_posts", "run_collector_daemon"):
        from x_growth import x_browser_collector

        return getattr(x_browser_collector, name)
    if name in ("generate_all_pending_comments", "run_generator_daemon"):
        from x_growth import comment_generator

        return getattr(comment_generator, name)
    if name in ("run_comment_pipeline", "run_comment_pipeline_daemon"):
        from x_growth import comment_pipeline

        return getattr(comment_pipeline, name)
    if name in ("publish_comments_from_db", "run_comment_publisher_daemon"):
        from x_growth import comment_publisher

        return getattr(comment_publisher, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
