# -*- coding: utf-8 -*-
"""
模块二：纯依赖数据库的独立评论发布器 (`x_growth/comment_publisher.py`)

完全独立于拉取与生成流程，仅依赖 MongoDB `harvested_social_posts` 中的 `auto_comment` 嵌套字段：
1. 从数据库查询已有评论且未发布过的帖子；
2. 按当前运营账号属性筛选分类 (`categories`)，并按强模型评分 (`auto_comment.best_score`) 从高到低排序；
3. 发布前针对当前登录目录 `user_data_dir` 严格核验三项硬条件：
   - 目标作者必须是认证用户 (`is_blue_verified == True`)；
   - 帖子必须是 24 小时以内发布的 (`now_ts - publish_time <= 86400`)；
   - 我尚未关注该作者 (`is_following == False`) 且该作者也尚未关注我 (`is_followed_by == False`)；
4. 执行评论发送（可选点赞 / 顺带关注），并将发布结果回写至 `auto_comment` 嵌套字段下。
"""
import json
import random
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.common_utils import safe_int
from common.mongo_db.mongo_base import gen_db_object
from platforms.harvesters.x_harvester import XHarvester
from platforms.x_playwright import comment_on_x_post, follow_x_user
from x_growth.growth_config import (
    AUTO_COMMENT_FIELD,
    POST_CATEGORIES,
    POSTS_COLLECTION_NAME,
    PUBLISHER_DEFAULTS,
    get_configured_self_handles,
    get_growth_logger,
    normalize_language_code,
    now_iso_cn,
)


def _sleep_jitter(delay_range: Sequence[float]) -> None:
    try:
        low, high = float(delay_range[0]), float(delay_range[1])
    except Exception:
        low, high = 10.0, 22.0
    if high > 0:
        time.sleep(random.uniform(max(1.0, low), max(low + 1.0, high)))


def publish_comments_from_db(
    user_data_dir: str = PUBLISHER_DEFAULTS["user_data_dir"],
    *,
    categories: Optional[Sequence[str]] = PUBLISHER_DEFAULTS["allowed_categories"],
    languages: Optional[Sequence[str]] = PUBLISHER_DEFAULTS["allowed_languages"],
    min_score: float = PUBLISHER_DEFAULTS["min_score"],
    max_publish: int = PUBLISHER_DEFAULTS["max_publish_per_run"],
    also_follow: bool = PUBLISHER_DEFAULTS["also_follow"],
    like_when_commenting: bool = PUBLISHER_DEFAULTS["like_when_commenting"],
    playwright_debug: bool = PUBLISHER_DEFAULTS["playwright_debug"],
    dry_run: bool = PUBLISHER_DEFAULTS["dry_run"],
) -> Dict[str, Any]:
    """
    从 `harvested_social_posts` 中按分类、语种与评分从高到低筛选待评帖子并发送评论。
    - `user_data_dir` : 执行发评论的 X 账号登录目录路径；
    - `categories`    : 当前账号定位允许评论的分类列表（如 `["tech_ai", "crypto_web3"]`；传 `None` 则全分类通用）；
    - `languages`     : 当前账号允许评论的语种列表（如 `["zh", "en"]`；传 `None` 则全语种通用）；
    - `dry_run`       : `True` 时仅查库并核验关系输出预览，不实际启动浏览器发送。
    """
    logger = get_growth_logger()
    session_path = str(Path(user_data_dir).expanduser().resolve())
    harvester = XHarvester(burner_user_data_dir=session_path)
    if not harvester.has_burner_account:
        raise RuntimeError(f"指定的登录目录未检测到有效 X 登录态 Cookie (auth_token/ct0): {session_path}")

    self_uid = str(harvester.burner_account_uid or "").strip()
    self_handles = set(get_configured_self_handles())

    db = gen_db_object()
    col = db.get_collection(POSTS_COLLECTION_NAME)

    now_ts = int(time.time())
    max_age_sec = int(PUBLISHER_DEFAULTS["post_max_age_seconds"])
    cutoff_ts = now_ts - max_age_sec

    # 查询近期已由当前会话评论过的作者，避免短时间内对同一人重复刷评
    dedup_days = max(1, int(PUBLISHER_DEFAULTS["author_dedup_days"]))
    recent_commented_docs = list(
        col.find(
            {
                "source": "x",
                f"{AUTO_COMMENT_FIELD}.published": True,
                f"{AUTO_COMMENT_FIELD}.published_by_dir": session_path,
                f"{AUTO_COMMENT_FIELD}.published_at_ts": {"$gte": now_ts - dedup_days * 86400},
            },
            {"author.username": 1, "_id": 0},
        )
    )
    commented_authors: Set[str] = {
        str((d.get("author") or {}).get("username") or "").strip().lstrip("@").lower()
        for d in recent_commented_docs
        if (d.get("author") or {}).get("username")
    }

    query: Dict[str, Any] = {
        "source": "x",
        "publish_time": {"$gte": cutoff_ts},
        "author.is_verified": True,
        f"{AUTO_COMMENT_FIELD}.skip": {"$ne": True},
        f"{AUTO_COMMENT_FIELD}.published": {"$ne": True},
        f"{AUTO_COMMENT_FIELD}.best_comment": {"$exists": True, "$ne": ""},
        f"{AUTO_COMMENT_FIELD}.best_score": {"$gte": float(min_score)},
    }

    clean_cats = [str(c).strip() for c in (categories or []) if str(c).strip() and str(c).strip().lower() != "all"]
    if clean_cats:
        invalid_cats = [c for c in clean_cats if c not in POST_CATEGORIES]
        if invalid_cats:
            raise ValueError(f"包含未知帖子分类 {invalid_cats}，合法分类为: {list(POST_CATEGORIES.keys())}")
        query[f"{AUTO_COMMENT_FIELD}.category"] = {"$in": clean_cats}

    eff_languages = PUBLISHER_DEFAULTS["allowed_languages"] if languages is None else languages
    clean_langs = [
        normalize_language_code(l)
        for l in (eff_languages or [])
        if str(l).strip() and str(l).strip().lower() != "all"
    ]
    clean_langs = [l for l in clean_langs if l and l != "unknown"]
    if clean_langs:
        query[f"{AUTO_COMMENT_FIELD}.language"] = {"$in": clean_langs}

    candidates = list(
        col.find(query)
        .sort([(f"{AUTO_COMMENT_FIELD}.best_score", -1), ("publish_time", -1)])
        .limit(max(20, int(max_publish) * 8))
    )

    logger.info(
        f"[Module2/启动] 模式={'DRY_RUN(预览)' if dry_run else 'LIVE(真实发送)'} | 会话={harvester.burner_account_summary} | "
        f"筛选分类={clean_cats or 'ALL'} | 筛选语种={clean_langs or 'ALL'} | 最低分={min_score} | 数据库初筛命中={len(candidates)}条"
    )

    published_items: List[Dict[str, Any]] = []
    skipped_items: List[Dict[str, Any]] = []
    live_sent_count = 0

    for doc in candidates:
        if len(published_items) >= max(1, int(max_publish)):
            break

        pid = str(doc.get("post_id") or "").strip()
        pub_ts = safe_int(doc.get("publish_time"), 0)
        current_ts = int(time.time())
        if not pid or pub_ts <= 0 or (current_ts - pub_ts) > max_age_sec:
            skipped_items.append({"post_id": pid, "reason": "expired_over_24h"})
            continue

        author_obj = doc.get("author") or {}
        handle = str(author_obj.get("username") or "").strip().lstrip("@")
        h_low = handle.lower()
        author_uid = str(doc.get("author_id") or "").strip()

        if not handle or h_low in self_handles or (self_uid and author_uid == self_uid):
            continue
        if h_low in commented_authors:
            skipped_items.append({"post_id": pid, "author": f"@{handle}", "reason": "recently_commented_same_author"})
            continue

        # 针对当前实际执行账号实时核验：1) 认证用户  2) 我未关注他  3) 他也未关注我
        live_prof = harvester.fetch_user_profile_detail(handle)
        if live_prof is not None:
            is_blue = bool(live_prof.get("is_blue_verified") or live_prof.get("is_verified"))
            is_following = bool(live_prof.get("is_following"))
            is_followed_by = bool(live_prof.get("is_followed_by"))
        else:
            rel_snap = ((doc.get(AUTO_COMMENT_FIELD) or {}).get("author_relationship")) or {}
            is_blue = bool(author_obj.get("is_verified"))
            is_following = bool(rel_snap.get("is_following"))
            is_followed_by = bool(rel_snap.get("is_followed_by"))

        if not is_blue:
            skipped_items.append({"post_id": pid, "author": f"@{handle}", "reason": "not_verified"})
            continue
        if is_following:
            skipped_items.append({"post_id": pid, "author": f"@{handle}", "reason": "already_following_author"})
            continue
        if is_followed_by:
            skipped_items.append({"post_id": pid, "author": f"@{handle}", "reason": "author_already_followed_me"})
            continue

        auto_meta = doc.get(AUTO_COMMENT_FIELD) or {}
        comment_text = str(auto_meta.get("best_comment") or "").strip()
        score = auto_meta.get("best_score")
        category = auto_meta.get("category")
        language = auto_meta.get("language") or "unknown"
        post_url = str((doc.get("metadata") or {}).get("url") or f"https://x.com/{handle}/status/{pid}")

        if dry_run:
            commented_authors.add(h_low)
            item_res = {
                "post_id": pid,
                "post_url": post_url,
                "author": f"@{handle}",
                "category": category,
                "language": language,
                "score": score,
                "comment_text": comment_text,
                "status": "DRY_RUN",
                "comment_id": None,
            }
            published_items.append(item_res)
            logger.info(
                f"[Module2/预览待发] [{category} | {language} | {score}分] @{handle} <{post_url}>\n"
                f"  -> 评论内容: {comment_text}"
            )
            continue

        if live_sent_count > 0:
            _sleep_jitter(PUBLISHER_DEFAULTS["action_delay_range"])

        err, ok, comment_id = comment_on_x_post(
            target_post_url=post_url,
            content=comment_text,
            like=like_when_commenting,
            user_data_dir=session_path,
            debug=playwright_debug,
        )
        live_sent_count += 1

        if not ok:
            logger.warning(f"[Module2/发送失败] @{handle} <{post_url}> | error={err}")
            col.update_one(
                {"source": "x", "post_id": pid},
                {
                    "$set": {
                        f"{AUTO_COMMENT_FIELD}.last_publish_error": str(err or "unknown_error"),
                        f"{AUTO_COMMENT_FIELD}.updated_at": now_iso_cn(),
                    }
                },
            )
            skipped_items.append({"post_id": pid, "author": f"@{handle}", "reason": f"send_failed:{err}"})
            continue

        follow_state = None
        if also_follow:
            _sleep_jitter((4, 9))
            f_err, f_ok, f_code = follow_x_user(
                target_screen_name=handle,
                user_data_dir=session_path,
                debug=playwright_debug,
            )
            follow_state = f_code if f_ok else f"FAILED:{f_err}"

        done_ts = int(time.time())
        done_iso = now_iso_cn()
        col.update_one(
            {"source": "x", "post_id": pid},
            {
                "$set": {
                    f"{AUTO_COMMENT_FIELD}.published": True,
                    f"{AUTO_COMMENT_FIELD}.published_at_ts": done_ts,
                    f"{AUTO_COMMENT_FIELD}.published_at": done_iso,
                    f"{AUTO_COMMENT_FIELD}.published_by_dir": session_path,
                    f"{AUTO_COMMENT_FIELD}.published_comment_id": comment_id,
                    f"{AUTO_COMMENT_FIELD}.follow_state": follow_state,
                    f"{AUTO_COMMENT_FIELD}.last_publish_error": None,
                    f"{AUTO_COMMENT_FIELD}.updated_at": done_iso,
                }
            },
        )
        commented_authors.add(h_low)
        item_res = {
            "post_id": pid,
            "post_url": post_url,
            "author": f"@{handle}",
            "category": category,
            "language": language,
            "score": score,
            "comment_text": comment_text,
            "status": "PUBLISHED",
            "comment_id": comment_id,
            "follow_state": follow_state,
        }
        published_items.append(item_res)
        logger.info(
            f"[Module2/发送成功] [{category} | {language} | {score}分] @{handle} <{post_url}> | comment_id={comment_id}"
        )

    return {
        "status": "ok",
        "dry_run": dry_run,
        "user_data_dir": session_path,
        "categories_filter": clean_cats or "ALL",
        "languages_filter": clean_langs or "ALL",
        "min_score": min_score,
        "db_candidates_found": len(candidates),
        "published_count": len(published_items),
        "published_items": published_items,
        "skipped_items": skipped_items,
    }


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    # 直接在 IDE 中点击运行即可，所有配置固定在 x_growth/growth_config.py 的 PUBLISHER_DEFAULTS 中
    res = publish_comments_from_db(
        user_data_dir=str(PUBLISHER_DEFAULTS["user_data_dir"]),
        categories=PUBLISHER_DEFAULTS.get("allowed_categories"),
        languages=PUBLISHER_DEFAULTS.get("allowed_languages"),
        dry_run=bool(PUBLISHER_DEFAULTS.get("dry_run", False)),
    )
    print(json.dumps(res, ensure_ascii=False, indent=2))
