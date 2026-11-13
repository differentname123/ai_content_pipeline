# -*- coding: utf-8 -*-
"""
模块二：多账号按自身定位消费公共评论池 & 单次留痕锁帖 (`x_growth/comment_publisher.py`)

低耦合高内聚设计：
1. 任意运营账号（英文号、日文号、垂类号）根据自身 `allowed_languages`、`allowed_categories`、`min_followers~max_followers`、`min_score` 查询尚未被消费（`auto_comment.consumed != True`）的帖子；
2. 同一作者防打扰仅按当前发评账号自己的消费记录 (`consumed_dir`) 独立统计，A 号行为不干扰 B 号；
3. 打开目标推文用当前发评账号自己的真实登录态核验：
   - 若帖子已删除/超24h/掉蓝V（对所有号均无效），标记 `auto_comment.skip = True`；
   - 若仅当前发评账号自己已与该作者互关/关注（当 `avoid_connected_authors=True`），仅本号当次跳过，绝不锁死该帖，其他未关注账号依然可以消费；
4. 发送评论成功后，立即回写 `consumed=True`、`consumed_by`（发评账号）、`consumed_at`（发评时间）及 `published_comment_id`，确保其他账号后续不再重复消费同一帖子。
"""
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from playwright.sync_api import sync_playwright

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.common_utils import get_proxy_url, safe_int
from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import HarvestedPostManager
from content_pipeline.pipeline_db import is_video_post
from platforms.playwright_base import (
    USER_DATA_LOCK_TIMEOUT_SEC,
    BusinessErrorException,
    _dismiss_overlays,
    _launch_persistent,
    _robust_click,
    _user_data_dir_lock,
    check_for_crash,
    install_overlay_guard,
)
from platforms.x_playwright import (
    _locate_focal_tweet,
    _locate_x_reply_box,
    _submit_x_post,
    _wait_x_home_ready,
    _wait_x_post_detail_ready,
)
from x_growth.growth_config import (
    AUTO_COMMENT_FIELD,
    POST_CATEGORIES,
    POSTS_COLLECTION_NAME,
    PUBLISHER_DEFAULTS,
    TZ_CN,
    get_configured_self_handles,
    get_growth_logger,
    normalize_language_code,
    now_iso_cn,
)
from x_growth.x_browser_collector import (
    X_HOME_URL,
    XPassiveGraphQLSniffer,
    build_real_chrome_launch_options,
    extract_logged_in_identity,
    inspect_x_page_account_status,
    persist_browsed_posts_to_db,
)


def _mark_objective_post_invalid(col: Any, post_id: str, reason: str) -> None:
    """仅当帖子客观失效（已删帖/超24h/作者取消蓝V）时标记 skip=True，防止所有账号重复打开废帖。"""
    if not post_id:
        return
    col.update_one(
        {"source": "x", "post_id": post_id},
        {
            "$set": {
                f"{AUTO_COMMENT_FIELD}.skip": True,
                f"{AUTO_COMMENT_FIELD}.skip_reason": f"live_check:{reason}",
                f"{AUTO_COMMENT_FIELD}.updated_at": now_iso_cn(),
            }
        },
    )


def _sleep_jitter(delay_range: Sequence[float]) -> None:
    try:
        low, high = float(delay_range[0]), float(delay_range[1])
    except Exception:
        low, high = 14.0, 28.0
    if high > 0:
        time.sleep(random.uniform(max(1.0, low), max(low + 1.0, high)))


def _like_focal_tweet_on_page(page: Any, post_id: str, logger: Any) -> None:
    try:
        focal_tweet = _locate_focal_tweet(page, post_id=post_id)
        already_liked = focal_tweet.locator('button[data-testid="unlike"]').first
        if already_liked.count() > 0 and already_liked.is_visible():
            return
        like_btn = focal_tweet.locator('button[data-testid="like"]').first
        if like_btn.count() == 0:
            like_btn = page.locator('button[data-testid="like"]').first
        if like_btn.count() > 0 and like_btn.is_visible():
            _robust_click(like_btn)
            page.wait_for_timeout(random.randint(500, 1000))
    except Exception as like_e:
        logger.debug(f"[Module2/点赞略过] post_id={post_id}: {like_e}")


def _follow_author_in_same_session(page: Any, handle: str, logger: Any) -> str:
    clean_handle = str(handle or "").strip().lstrip("@")
    if not clean_handle:
        return "SKIPPED_EMPTY"
    try:
        page.goto(f"https://x.com/{clean_handle}", timeout=35000, wait_until="domcontentloaded")
        page.wait_for_timeout(random.randint(2000, 3500))
        _dismiss_overlays(page, aggressive=False, desc="x-growth-follow")
        primary_col = page.locator('[data-testid="primaryColumn"]').first
        header_pl = primary_col.locator('div[data-testid="placementTracking"]')
        already_btn = header_pl.locator('button[data-testid$="-unfollow"], button[data-testid$="-cancel"]').first
        if already_btn.count() > 0 and already_btn.is_visible():
            return "ALREADY_FOLLOWING"

        follow_btn = header_pl.locator('button[data-testid$="-follow"]').first
        if follow_btn.count() == 0:
            follow_btn = primary_col.locator('button[data-testid$="-follow"]').first
        if follow_btn.count() == 0 or not follow_btn.is_visible():
            return "FOLLOW_BTN_NOT_FOUND"

        _robust_click(follow_btn)
        page.wait_for_timeout(random.randint(1500, 2500))
        return "FOLLOWED"
    except Exception as f_exc:
        logger.warning(f"[Module2/顺带关注异常] @{clean_handle}: {f_exc}")
        return f"FAILED:{str(f_exc)[:80]}"


def publish_comments_from_db(
    user_data_dir: str = PUBLISHER_DEFAULTS["user_data_dir"],
    *,
    categories: Optional[Sequence[str]] = PUBLISHER_DEFAULTS["allowed_categories"],
    languages: Optional[Sequence[str]] = PUBLISHER_DEFAULTS["allowed_languages"],
    min_followers: int = PUBLISHER_DEFAULTS["min_followers"],
    max_followers: int = PUBLISHER_DEFAULTS["max_followers"],
    min_score: float = PUBLISHER_DEFAULTS["min_score"],
    max_publish: int = PUBLISHER_DEFAULTS["max_publish_per_run"],
    avoid_connected_authors: bool = PUBLISHER_DEFAULTS["avoid_connected_authors"],
    also_follow: bool = PUBLISHER_DEFAULTS["also_follow"],
    like_when_commenting: bool = PUBLISHER_DEFAULTS["like_when_commenting"],
    browser_visible: bool = PUBLISHER_DEFAULTS["browser_visible"],
    dry_run: bool = PUBLISHER_DEFAULTS["dry_run"],
) -> Dict[str, Any]:
    """
    按当前运营账号自身筛选条件消费公共评论池：
    一旦发送成功即写入 `consumed=True`、`consumed_by` 与 `consumed_at`，锁定该帖不再被其他账号重复消费。
    """
    logger = get_growth_logger()
    session_path = str(Path(user_data_dir).expanduser().resolve())
    self_handles: Set[str] = set(get_configured_self_handles())

    db = gen_db_object()
    col = db.get_collection(POSTS_COLLECTION_NAME)

    now_ts = int(time.time())
    max_age_sec = int(PUBLISHER_DEFAULTS["post_max_age_seconds"])
    cutoff_ts = now_ts - max_age_sec

    # 仅统计【当前发评账号自己】近 N 天内评论过的作者，互不干扰其他账号
    dedup_days = max(1, int(PUBLISHER_DEFAULTS["author_dedup_days"]))
    recent_own_docs = list(
        col.find(
            {
                "source": "x",
                f"{AUTO_COMMENT_FIELD}.consumed": True,
                f"{AUTO_COMMENT_FIELD}.consumed_dir": session_path,
                f"{AUTO_COMMENT_FIELD}.consumed_at_ts": {"$gte": now_ts - dedup_days * 86400},
            },
            {"author.username": 1, "_id": 0},
        )
    )
    commented_authors: Set[str] = {
        str((d.get("author") or {}).get("username") or "").strip().lstrip("@").lower()
        for d in recent_own_docs
        if (d.get("author") or {}).get("username")
    }

    query: Dict[str, Any] = {
        "source": "x",
        "publish_time": {"$gte": cutoff_ts},
        "author.is_verified": True,
        "author.followers_count": {"$gte": int(min_followers), "$lte": int(max_followers)},
        f"{AUTO_COMMENT_FIELD}.skip": {"$ne": True},
        f"{AUTO_COMMENT_FIELD}.consumed": {"$ne": True},
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

    raw_candidates = list(
        col.find(query)
        .sort([(f"{AUTO_COMMENT_FIELD}.best_score", -1), ("publish_time", -1)])
        .limit(max(20, int(max_publish) * 8))
    )
    candidates = [d for d in raw_candidates if not is_video_post(d)]

    logger.info(
        f"[Module2/启动] 模式={'DRY_RUN(纯库预览)' if dry_run else 'LIVE(真实Chrome消费发评)'} | 账号目录={session_path} | "
        f"语种={clean_langs or 'ALL'} | 赛道={clean_cats or 'ALL'} | 粉丝={min_followers}~{max_followers} | "
        f"最低分={min_score} | 未消费候选={len(candidates)}条"
    )

    published_items: List[Dict[str, Any]] = []
    skipped_items: List[Dict[str, Any]] = []
    target_limit = max(1, int(max_publish))

    # ── 模式 A：纯数据库预览（零浏览器启动） ──
    if dry_run or not candidates:
        for doc in candidates:
            if len(published_items) >= target_limit:
                break
            pid = str(doc.get("post_id") or "").strip()
            pub_ts = safe_int(doc.get("publish_time"), 0)
            if not pid or pub_ts <= 0 or (int(time.time()) - pub_ts) > max_age_sec:
                skipped_items.append({"post_id": pid, "reason": "expired_over_24h"})
                continue

            author_obj = doc.get("author") or {}
            handle = str(author_obj.get("username") or "").strip().lstrip("@")
            h_low = handle.lower()
            if not handle or h_low in self_handles:
                continue
            if h_low in commented_authors:
                skipped_items.append({"post_id": pid, "author": f"@{handle}", "reason": "recently_commented_by_this_account"})
                continue

            auto_meta = doc.get(AUTO_COMMENT_FIELD) or {}
            comment_text = str(auto_meta.get("best_comment") or "").strip()
            score = auto_meta.get("best_score")
            category = auto_meta.get("category")
            language = auto_meta.get("language") or "unknown"
            post_url = str((doc.get("metadata") or {}).get("url") or f"https://x.com/{handle}/status/{pid}")

            commented_authors.add(h_low)
            published_items.append(
                {
                    "post_id": pid,
                    "post_url": post_url,
                    "author": f"@{handle}",
                    "followers": int(author_obj.get("followers_count") or 0),
                    "category": category,
                    "language": language,
                    "score": score,
                    "comment_text": comment_text,
                    "status": "DRY_RUN",
                    "comment_id": None,
                }
            )
            logger.info(
                f"[Module2/预览待消费] [{category} | {language} | {score}分] @{handle} ({author_obj.get('followers_count')}粉) <{post_url}>\n"
                f"  -> 评论内容: {comment_text}"
            )

        return {
            "status": "ok",
            "dry_run": dry_run,
            "user_data_dir": session_path,
            "account_warning": None,
            "categories_filter": clean_cats or "ALL",
            "languages_filter": clean_langs or "ALL",
            "min_score": min_score,
            "db_candidates_found": len(candidates),
            "published_count": len(published_items),
            "published_items": published_items,
            "skipped_items": skipped_items,
        }

    # ── 模式 B：真实桌面 Chrome 原地核验 + 发评 + 消费留痕锁帖 ──
    if not os.path.isdir(session_path):
        msg = f"浏览器会话目录不存在: {session_path}"
        logger.error(f"🚨 [Module2/目录缺失] {msg}")
        return {
            "status": "not_logged_in",
            "dry_run": False,
            "user_data_dir": session_path,
            "account_warning": msg,
            "categories_filter": clean_cats or "ALL",
            "languages_filter": clean_langs or "ALL",
            "min_score": min_score,
            "db_candidates_found": len(candidates),
            "published_count": 0,
            "published_items": [],
            "skipped_items": [],
        }

    proxy_server = get_proxy_url()
    chrome_args, viewport_cfg = build_real_chrome_launch_options(browser_visible=browser_visible)
    sniffer = XPassiveGraphQLSniffer()

    with _user_data_dir_lock(session_path, timeout=USER_DATA_LOCK_TIMEOUT_SEC):
        with sync_playwright() as p:
            context = None
            page = None
            try:
                context = _launch_persistent(
                    p,
                    session_path,
                    args=chrome_args,
                    viewport=viewport_cfg,
                    hide_automation=True,
                    debug=True,
                    proxy=proxy_server,
                )
                context.set_default_timeout(45000)
                context.set_default_navigation_timeout(45000)

                page = context.pages[0] if context.pages else context.new_page()
                for extra_p in list(context.pages):
                    if extra_p != page:
                        try:
                            extra_p.close()
                        except Exception:
                            pass

                page.on("response", sniffer.on_response)
                install_overlay_guard(page)

                page.goto(X_HOME_URL, timeout=45000, wait_until="domcontentloaded")
                _wait_x_home_ready(page, timeout_ms=18000)
                page.wait_for_timeout(random.randint(1500, 2800))
                _dismiss_overlays(page, aggressive=False, desc="x-pub-home")
                check_for_crash(page)

                acc_status, acc_msg = inspect_x_page_account_status(page)
                if acc_status != "ok":
                    logger.error(f"🚨 [Module2/账号状态阻断] status={acc_status} | {acc_msg}")
                    return {
                        "status": acc_status,
                        "dry_run": False,
                        "user_data_dir": session_path,
                        "account_warning": acc_msg,
                        "categories_filter": clean_cats or "ALL",
                        "languages_filter": clean_langs or "ALL",
                        "min_score": min_score,
                        "db_candidates_found": len(candidates),
                        "published_count": 0,
                        "published_items": [],
                        "skipped_items": [],
                    }

                self_uid, self_handle = extract_logged_in_identity(context, page)
                if self_handle:
                    self_handles.add(self_handle.lower())
                consumer_label = f"@{self_handle}" if self_handle else (f"uid:{self_uid}" if self_uid else Path(session_path).name)

                live_sent_count = 0
                for doc in candidates:
                    if len(published_items) >= target_limit:
                        break

                    pid = str(doc.get("post_id") or "").strip()
                    pub_ts = safe_int(doc.get("publish_time"), 0)
                    if not pid or pub_ts <= 0 or (int(time.time()) - pub_ts) > max_age_sec:
                        _mark_objective_post_invalid(col, pid, "expired_over_24h")
                        skipped_items.append({"post_id": pid, "reason": "expired_over_24h"})
                        continue

                    # 并发安全二次校验：防止多账号几乎同时运行时消费到同一条刚被锁定的帖子
                    fresh_state = col.find_one(
                        {"source": "x", "post_id": pid},
                        {f"{AUTO_COMMENT_FIELD}.consumed": 1, f"{AUTO_COMMENT_FIELD}.skip": 1, "_id": 0},
                    )
                    fresh_auto = (fresh_state or {}).get(AUTO_COMMENT_FIELD) or {}
                    if fresh_auto.get("consumed") or fresh_auto.get("skip"):
                        skipped_items.append({"post_id": pid, "reason": "already_consumed_by_other_account"})
                        continue

                    author_obj = doc.get("author") or {}
                    handle = str(author_obj.get("username") or "").strip().lstrip("@")
                    h_low = handle.lower()
                    author_uid = str(doc.get("author_id") or "").strip()

                    if not handle or h_low in self_handles or (self_uid and author_uid == self_uid):
                        continue
                    if h_low in commented_authors:
                        skipped_items.append(
                            {"post_id": pid, "author": f"@{handle}", "reason": "recently_commented_by_this_account"}
                        )
                        continue

                    auto_meta = doc.get(AUTO_COMMENT_FIELD) or {}
                    comment_text = str(auto_meta.get("best_comment") or "").strip()
                    score = auto_meta.get("best_score")
                    category = auto_meta.get("category")
                    language = auto_meta.get("language") or "unknown"
                    post_url = str((doc.get("metadata") or {}).get("url") or f"https://x.com/{handle}/status/{pid}")

                    if live_sent_count > 0:
                        _sleep_jitter(PUBLISHER_DEFAULTS["action_delay_range"])

                    prev_det = sniffer.op_counts.get("TweetDetail", 0)
                    resp = page.goto(post_url, timeout=45000, wait_until="domcontentloaded")
                    sniffer.wait_for_op_increment(page, ("TweetDetail",), prev_det, timeout_ms=12000)
                    _wait_x_post_detail_ready(page, timeout_ms=15000)
                    page.wait_for_timeout(random.randint(1800, 3400))
                    _dismiss_overlays(page, aggressive=False, desc="x-pub-detail")

                    step_status, step_msg = inspect_x_page_account_status(page)
                    if step_status != "ok":
                        logger.error(f"🚨 [Module2/发评途中触发安全验证] {step_msg}")
                        break

                    if resp and resp.status >= 400:
                        skipped_items.append({"post_id": pid, "author": f"@{handle}", "reason": f"http_{resp.status}"})
                        continue

                    post_unavailable = False
                    for err_sel in ('div[data-testid="error-detail"]', 'div[data-testid="empty_state_header_text"]'):
                        try:
                            err_loc = page.locator(err_sel).first
                            if err_loc.count() > 0 and err_loc.is_visible():
                                post_unavailable = True
                                break
                        except Exception:
                            pass
                    if post_unavailable:
                        _mark_objective_post_invalid(col, pid, "post_deleted_or_unavailable")
                        skipped_items.append({"post_id": pid, "author": f"@{handle}", "reason": "post_deleted_or_unavailable"})
                        continue

                    live_post = sniffer.detailed_posts_by_id.get(pid)
                    live_prof = (
                        (((live_post or {}).get("metadata") or {}).get("platform_extra") or {}).get("author_profile")
                        or sniffer.users_by_handle.get(h_low)
                    )

                    live_pub_ts = safe_int((live_post or {}).get("publish_time"), pub_ts)
                    if live_pub_ts <= 0 or (int(time.time()) - live_pub_ts) > max_age_sec:
                        _mark_objective_post_invalid(col, pid, "expired_over_24h_live")
                        skipped_items.append({"post_id": pid, "author": f"@{handle}", "reason": "expired_over_24h_live"})
                        continue

                    if isinstance(live_prof, dict):
                        is_blue = bool(live_prof.get("is_blue_verified") or live_prof.get("is_verified"))
                        if not is_blue:
                            _mark_objective_post_invalid(col, pid, "not_verified")
                            skipped_items.append({"post_id": pid, "author": f"@{handle}", "reason": "not_verified"})
                            continue
                        # 仅针对当前发评账号自己的关注关系判断：跳过但不污染公共帖子，其他未关注账号依然可用！
                        if avoid_connected_authors and (
                            bool(live_prof.get("is_following")) or bool(live_prof.get("is_followed_by"))
                        ):
                            skipped_items.append(
                                {"post_id": pid, "author": f"@{handle}", "reason": "connected_with_current_account_only"}
                            )
                            continue

                    try:
                        if like_when_commenting:
                            _like_focal_tweet_on_page(page, pid, logger)

                        reply_container = _locate_x_reply_box(page, post_id=pid)
                        comment_id = _submit_x_post(
                            page,
                            reply_container,
                            content=comment_text,
                            image_path_list=None,
                            text_list=None,
                            prepared_images=[],
                        )
                        live_sent_count += 1
                    except BusinessErrorException as biz_e:
                        err_str = f"business_blocked:{biz_e}"
                        logger.error(f"[Module2/业务阻断] @{handle} <{post_url}> | {err_str}")
                        col.update_one(
                            {"source": "x", "post_id": pid},
                            {"$set": {f"{AUTO_COMMENT_FIELD}.last_publish_error": err_str, f"{AUTO_COMMENT_FIELD}.updated_at": now_iso_cn()}},
                        )
                        skipped_items.append({"post_id": pid, "author": f"@{handle}", "reason": err_str})
                        break
                    except Exception as send_e:
                        err_str = str(send_e)[:240]
                        logger.warning(f"[Module2/发送失败] @{handle} <{post_url}> | error={err_str}")
                        col.update_one(
                            {"source": "x", "post_id": pid},
                            {"$set": {f"{AUTO_COMMENT_FIELD}.last_publish_error": err_str, f"{AUTO_COMMENT_FIELD}.updated_at": now_iso_cn()}},
                        )
                        skipped_items.append({"post_id": pid, "author": f"@{handle}", "reason": f"send_failed:{err_str}"})
                        continue

                    follow_state = None
                    if also_follow:
                        page.wait_for_timeout(random.randint(2500, 4500))
                        follow_state = _follow_author_in_same_session(page, handle, logger)

                    done_ts = int(time.time())
                    done_iso = now_iso_cn()
                    col.update_one(
                        {"source": "x", "post_id": pid},
                        {
                            "$set": {
                                f"{AUTO_COMMENT_FIELD}.consumed": True,
                                f"{AUTO_COMMENT_FIELD}.consumed_by": consumer_label,
                                f"{AUTO_COMMENT_FIELD}.consumed_dir": session_path,
                                f"{AUTO_COMMENT_FIELD}.consumed_at": done_iso,
                                f"{AUTO_COMMENT_FIELD}.consumed_at_ts": done_ts,
                                f"{AUTO_COMMENT_FIELD}.published_comment_id": comment_id,
                                f"{AUTO_COMMENT_FIELD}.published_comment_text": comment_text,
                                f"{AUTO_COMMENT_FIELD}.follow_state": follow_state,
                                f"{AUTO_COMMENT_FIELD}.last_publish_error": None,
                                f"{AUTO_COMMENT_FIELD}.updated_at": done_iso,
                            }
                        },
                    )
                    commented_authors.add(h_low)
                    published_items.append(
                        {
                            "post_id": pid,
                            "post_url": post_url,
                            "author": f"@{handle}",
                            "category": category,
                            "language": language,
                            "score": score,
                            "comment_text": comment_text,
                            "consumed_by": consumer_label,
                            "consumed_at": done_iso,
                            "status": "CONSUMED_AND_PUBLISHED",
                            "comment_id": comment_id,
                            "follow_state": follow_state,
                        }
                    )
                    logger.info(
                        f"[Module2/消费成功] 账号={consumer_label} | [{category} | {language} | {score}分] @{handle} <{post_url}> | comment_id={comment_id}"
                    )

                pub_captured = sniffer.export_all_captured_root_posts()
                if pub_captured:
                    try:
                        post_manager = HarvestedPostManager(db, collection_name=POSTS_COLLECTION_NAME)
                        persist_browsed_posts_to_db(post_manager, col, pub_captured)
                    except Exception as persist_e:
                        logger.debug(f"[Module2/顺路入库略过] {persist_e}")
            finally:
                if page is not None:
                    try:
                        page.remove_listener("response", sniffer.on_response)
                    except Exception:
                        pass
                if context is not None:
                    try:
                        context.close()
                    except Exception:
                        pass

    return {
        "status": "ok",
        "dry_run": False,
        "user_data_dir": session_path,
        "account_warning": None,
        "categories_filter": clean_cats or "ALL",
        "languages_filter": clean_langs or "ALL",
        "min_score": min_score,
        "db_candidates_found": len(candidates),
        "published_count": len(published_items),
        "published_items": published_items,
        "skipped_items": skipped_items,
    }


def run_comment_publisher_daemon(
    user_data_dir: str = PUBLISHER_DEFAULTS["user_data_dir"],
    interval_seconds: Optional[int] = None,
    jitter_seconds: Optional[Tuple[int, int]] = None,
) -> None:
    """可选常驻定时消费发评循环。"""
    logger = get_growth_logger()
    base_interval = max(120, int(interval_seconds or PUBLISHER_DEFAULTS.get("publish_interval_seconds", 3600)))
    raw_jitter = (
        jitter_seconds
        if jitter_seconds is not None
        else PUBLISHER_DEFAULTS.get("publish_jitter_seconds", (180, 600))
    )
    if isinstance(raw_jitter, (list, tuple)) and len(raw_jitter) == 2:
        j_low, j_high = max(0, int(raw_jitter[0])), max(0, int(raw_jitter[1]))
        if j_low > j_high:
            j_low, j_high = j_high, j_low
    else:
        j_low, j_high = 0, 0

    session_path = str(Path(user_data_dir).expanduser().resolve())
    logger.info(
        f"⏰ [Module2/常驻发评任务启动] 账号目录={session_path} | 周期={base_interval}s + 抖动({j_low}~{j_high}s)"
    )
    round_no = 0
    while True:
        round_no += 1
        logger.info(f"========== [Module2/发评轮次 #{round_no}] 开始执行 ==========")
        try:
            res = publish_comments_from_db(
                user_data_dir=session_path,
                categories=PUBLISHER_DEFAULTS.get("allowed_categories"),
                languages=PUBLISHER_DEFAULTS.get("allowed_languages"),
                dry_run=bool(PUBLISHER_DEFAULTS.get("dry_run", False)),
            )
            if res.get("status") != "ok":
                logger.error(f"⚠️ [Module2/发评轮次 #{round_no} 待处理] {res.get('account_warning')}")
            else:
                logger.info(
                    f"✅ [Module2/发评轮次 #{round_no} 完成] 未消费候选={res.get('db_candidates_found', 0)}条 | "
                    f"本次成功消费={res.get('published_count', 0)}条"
                )
        except KeyboardInterrupt:
            logger.info("[Module2/常驻发评退出] 收到手动中断信号，安全停止。")
            break
        except Exception as exc:
            logger.exception(f"❌ [Module2/发评轮次 #{round_no} 异常] {exc}")

        sleep_sec = base_interval + (random.randint(j_low, j_high) if j_high > 0 else 0)
        next_run_time = (datetime.now(TZ_CN) + timedelta(seconds=sleep_sec)).strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"💤 [Module2/常驻休眠] 休眠 {sleep_sec} 秒，下一轮将于 [{next_run_time}] 执行...")
        try:
            time.sleep(sleep_sec)
        except KeyboardInterrupt:
            logger.info("[Module2/常驻发评退出] 休眠期间收到中断信号，安全停止。")
            break


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    target_dir = str(PUBLISHER_DEFAULTS["user_data_dir"])
    if PUBLISHER_DEFAULTS.get("continuous_loop", False):
        run_comment_publisher_daemon(
            user_data_dir=target_dir,
            interval_seconds=PUBLISHER_DEFAULTS.get("publish_interval_seconds", 3600),
            jitter_seconds=PUBLISHER_DEFAULTS.get("publish_jitter_seconds", (180, 600)),
        )
    else:
        res = publish_comments_from_db(
            user_data_dir=target_dir,
            categories=PUBLISHER_DEFAULTS.get("allowed_categories"),
            languages=PUBLISHER_DEFAULTS.get("allowed_languages"),
            dry_run=bool(PUBLISHER_DEFAULTS.get("dry_run", False)),
        )
        print(json.dumps(res, ensure_ascii=False, indent=2))
