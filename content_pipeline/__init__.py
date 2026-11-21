# -*- coding: utf-8 -*-
"""
模块化多模态内容生产平台 (`content_pipeline`)

高内聚 · 低耦合独立组件矩阵：
- 模块 1 (`platforms.harvester_runner`)        : `harvest_broad_feeds()` / `harvest_by_keywords(keywords)`
- 模块 2 (`content_pipeline.step2_comprehension`): `run_step2_comprehension()` / `comprehend_posts_by_keys(post_keys)`
- 模块 3 (`content_pipeline.topic_radar`)        : `discover_hot_directions()` / `run_topic_radar()`
- 模块 4 (`content_pipeline.direction_expander`) : `expand_direction_across_platforms(direction)`
- 模块 5 (`content_pipeline.material_curator`)   : `curate_materials_for_direction(direction, digests)`
- 模块 6 (`content_pipeline.content_creator`)    : `assemble_creation_materials(post_keys)` / `create_from_materials(bundle)`
- 模块 7 (`content_pipeline.blind_reviewer`)     : `review_single_creation_blind(...)` / `review_and_polish_creation(...)`
- 模块 8 (`content_pipeline.comment_creator`)    : `generate_comments_from_post(...)` / `generate_comments_for_x_post(...)`
"""

from content_pipeline.blind_reviewer import (
    review_and_polish_creation,
    review_single_creation_blind,
    run_blind_review_batch,
)
from content_pipeline.comment_creator import (
    assemble_comment_bundle,
    calc_x_weighted_length,
    fetch_complete_x_post,
    generate_comments_for_post_key,
    generate_comments_for_x_post,
    generate_comments_from_post,
    get_default_x_session_dir,
    publish_x_comment,
)
from content_pipeline.content_creator import (
    assemble_creation_materials,
    create_from_materials,
    revise_creation_with_feedback,
    run_content_creation_batch,
)
from content_pipeline.direction_expander import (
    expand_direction_across_platforms,
    run_direction_expansion_batch,
)
from content_pipeline.material_curator import (
    curate_materials_for_direction,
    run_material_curation_batch,
)
from content_pipeline.pipeline_config import (
    ACTIVE_PROFILE,
    COMMENT_CREATOR_CONFIG,
    CONTENT_PROFILES,
    CREATOR_CONFIG,
    CURATOR_CONFIG,
    EXPANDER_CONFIG,
    PIPELINE_COLLECTIONS,
    RADAR_CONFIG,
    REVIEWER_CONFIG,
    SCHEDULE_CONFIG,
    STAGE_SWITCHES,
    STEP2_CONFIG,
)
from content_pipeline.step2_comprehension import (
    comprehend_posts_by_keys,
    run_step2_comprehension,
)
from content_pipeline.topic_radar import (
    discover_hot_directions,
    run_topic_radar,
)

__all__ = [
    "ACTIVE_PROFILE",
    "COMMENT_CREATOR_CONFIG",
    "CONTENT_PROFILES",
    "PIPELINE_COLLECTIONS",
    "SCHEDULE_CONFIG",
    "STAGE_SWITCHES",
    "STEP2_CONFIG",
    "RADAR_CONFIG",
    "EXPANDER_CONFIG",
    "CURATOR_CONFIG",
    "CREATOR_CONFIG",
    "REVIEWER_CONFIG",
    "run_step2_comprehension",
    "comprehend_posts_by_keys",
    "discover_hot_directions",
    "run_topic_radar",
    "expand_direction_across_platforms",
    "run_direction_expansion_batch",
    "curate_materials_for_direction",
    "run_material_curation_batch",
    "assemble_creation_materials",
    "create_from_materials",
    "revise_creation_with_feedback",
    "run_content_creation_batch",
    "review_single_creation_blind",
    "review_and_polish_creation",
    "run_blind_review_batch",
    "assemble_comment_bundle",
    "calc_x_weighted_length",
    "fetch_complete_x_post",
    "generate_comments_from_post",
    "generate_comments_for_x_post",
    "generate_comments_for_post_key",
    "get_default_x_session_dir",
    "publish_x_comment",
]
