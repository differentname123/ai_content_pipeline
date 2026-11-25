# -*- coding: utf-8 -*-
"""
维护任务与人工复盘入口 (`parser/maintenance_tasks.py`)

统一收口：
1. 评论区可配置互动 (`hudong`, `interact_comment`, `INTERACTION_CONFIG`)；
2. 按逆向行情/超时自动删帖 (`delete_old_post`, `_delete_old_posts_once`) 与按时间段精准批量删帖 (`delete_posts_by_account_and_time`)；
3. 手动批量重洗与人工质量抽样复盘工具 (`clear_all_media_format_batch`, `get_need_check_*`, `query_hot_posts_by_keyword`)。
"""
import argparse
import random
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from common.common_utils import (
    BINANCE_ACCOUNTS as ACCOUNTS,
    BINANCE_ACCOUNT_USERNAME_MAP,
    OKX_ACCOUNT_UID_MAP,
    PLATFORM_ACCOUNTS,
    PLATFORM_BINANCE,
    PLATFORM_OKX,
    POST_SOURCES,
    save_json,
)
from common.daemon import run_daemon_loop
from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import GeneratedArticleManager, UniversalPostManager
from parser.config import INTERACTION_CONFIG, METRICS_SYNC_INTERVAL_SECONDS
from parser.mlu_pipeline import build_pre_screen_item
from parser.platform_adapters import BEIJING_TZ, PLATFORMS, _get_account_session_dir, _log, logger, sync_published_metrics
from parser.post_ingestion import (
    ARTICLE_TYPE_VIRAL,
    DELETE_INTERVAL_SEC,
    INTERACTION_INTERVAL_SECONDS,
    MAX_REPLAY_DAYS,
    POST_QUERY_LIMIT,
    _find_posts_by_sources,
    _publish_time_seconds,
    normalize_post_media,
    prompt_version,
)
from platforms.binance_api import (
    delete_binance_square_content,
    fetch_binance_square_replies,
    get_latest_crypto_price,
    like_and_bookmark,
)
from platforms.binance_playwright import get_auth_tokens_robust
from platforms.okx_api import fetch_okx_user_timeline
from platforms.okx_playwright import delete_okx_posts


def _load_account_credentials(action: str, with_user_info: bool = False) -> Tuple[Dict[str, Dict[str, Any]], int]:
    credentials: Dict[str, Dict[str, Any]] = {}
    errors = 0
    for account in ACCOUNTS:
        try:
            session_dir = _get_account_session_dir(PLATFORM_BINANCE, account)
            cookies, token, user_info = get_auth_tokens_robust(session_dir, debug=False)
            if not cookies or not token:
                logger.warning(
                    f"[{action}/账号] 凭证不完整 | 账号: [{account}] | 后续: [跳过当前账号] "
                    "| 排查: [检查登录状态与浏览器会话]"
                )
                continue
            credentials[account] = {"cookies": cookies, "csrf_token": token}
            if with_user_info:
                credentials[account]["user_info"] = user_info or {}
        except Exception:
            errors += 1
            logger.exception(
                f"❌ [{action}/账号] 获取凭证失败 | 账号: [{account}] | 后续: [继续其他账号] "
                "| 排查: [检查会话目录、浏览器启动与登录状态]"
            )
    return credentials, errors


def interact_comment(
    god_comments: Sequence[str],
    other_account_list: Sequence[str],
    post_id: Any,
    stance: Optional[str],
    platform: str = PLATFORM_BINANCE,
    max_count: int = 1,
    like: bool = False,
) -> Dict[str, Dict[str, Any]]:
    """随机抽取 `max_count` 个其他账号对指定帖子发表评论（可选顺带点赞）。"""
    results: Dict[str, Dict[str, Any]] = {}
    unconfirmed = raised = 0
    if not other_account_list or not god_comments or max_count <= 0:
        return results
    accounts = random.sample(list(other_account_list), len(other_account_list))[:max_count]

    for account, text in zip(accounts, god_comments):
        session_dir = _get_account_session_dir(platform, account)
        try:
            if platform not in PLATFORMS:
                raise ValueError(f"不支持的评论互动平台: {platform}")
            error, success, comment_id = PLATFORMS[platform].comment(
                session_dir,
                post_id,
                text,
                stance,
                like=like,
            )
            results[f"{text}"] = {
                "account": account,
                "comment_id": str(comment_id) if comment_id else "",
                "success": success,
                "error_info": error,
            }
            unconfirmed += int(not success)
        except Exception:
            raised += 1
            _log(
                "互动/评论",
                "发送异常，远端结果未知，继续其他评论",
                level="exception",
                平台=platform,
                帖子=post_id,
                账号=account,
                排查="检查登录、网络及远端评论状态",
            )
    if accounts and god_comments:
        _log(
            "互动/评论汇总",
            "评论尝试结束",
            level="warning" if unconfirmed or raised else "info",
            平台=platform,
            帖子=post_id,
            配对=min(len(accounts), len(god_comments)),
            未确认=unconfirmed,
            调用异常=raised,
            结果条目=len(results),
            排查="未确认时检查结果字典中的error_info及远端评论",
        )
    return results


def _interact_for_platform(article_manager: Any, platform: str, cutoff_ts: float) -> None:
    started = time.monotonic()
    cfg = INTERACTION_CONFIG.get(platform) or {}
    enable_like = bool(cfg.get("enable_like", False))
    enable_comment = bool(cfg.get("enable_comment", False))
    max_comment_accounts = int(cfg.get("max_comment_accounts", 1))

    if not enable_like and not enable_comment:
        _log("互动/跳过", "当前平台已在 INTERACTION_CONFIG 中关闭点赞与互评", 平台=platform)
        return

    binance = platform == PLATFORM_BINANCE
    credentials: Dict[str, Dict[str, Any]] = {}
    accounts = ACCOUNTS if binance else list(PLATFORM_ACCOUNTS.get(platform) or [])
    if binance and enable_like:
        credentials, _ = _load_account_credentials("互动")
        if not credentials:
            _log(
                "互动/跳过",
                "无可用凭证，暂不处理该平台",
                level="warning",
                平台=platform,
                账号数=len(accounts),
                排查="恢复账号登录与浏览器会话",
            )
            return
    if not accounts:
        _log("互动/跳过", "未配置互动账号", level="warning", 平台=platform)
        return

    articles = (
        article_manager.find_articles(
            query={
                "status": "ok",
                f"publish_records.{platform}.status": "success",
                f"publish_records.{platform}.publish_time": {"$gte": cutoff_ts},
            }
        )
        or []
    )
    pending, post_ids = [], []
    for article in articles:
        record = (article.get("publish_records") or {}).get(platform) or {}
        post_id = record.get("post_id")
        if not record.get("interacted") and post_id and str(post_id) != "None":
            pending.append(article)
            post_ids.append(str(post_id))
    if not pending:
        _log("互动/完成", "没有可提交帖子", 平台=platform, 待处理=0, 耗时=f"{time.monotonic() - started:.2f}秒")
        return

    if binance and enable_like:
        try:
            like_and_bookmark(post_ids, credentials)
        except Exception:
            _log(
                "互动/执行",
                "点赞收藏接口异常，保留未互动状态等待下轮",
                level="exception",
                平台=platform,
                帖数=len(post_ids),
                排查="检查凭证、网络与限流；核对远端已完成操作",
            )
            return

    interacted_at = time.time()
    for article in pending:
        record = article["publish_records"][platform]
        other_accounts = [account for account in accounts if account != record.get("published_by")]
        info = article.get("article_info") or {}
        comments = info.get("bait_comments") if article.get("type") == ARTICLE_TYPE_VIRAL else info.get("god_comments")
        raw_stance = article.get("stance")
        sentiment = raw_stance if binance and raw_stance in ("看多", "看空") else None

        if enable_comment and max_comment_accounts > 0:
            if other_accounts:
                record["comment_interact_info"] = interact_comment(
                    comments or [],
                    other_accounts,
                    record.get("post_id"),
                    sentiment,
                    platform=platform,
                    max_count=max_comment_accounts,
                    like=(enable_like and not binance),
                )
            else:
                _log(
                    "互动/跳过",
                    "排除发帖账号后没有其他互动账号",
                    level="warning",
                    平台=platform,
                    帖子=record.get("post_id"),
                    发布者=record.get("published_by"),
                )
                record["comment_interact_info"] = {}
        elif not binance and enable_like and other_accounts:
            like_accounts = random.sample(other_accounts, len(other_accounts))[: max(1, max_comment_accounts)]
            like_results = {}
            for acc in like_accounts:
                session_dir = _get_account_session_dir(platform, acc)
                err, ok, _ = PLATFORMS[platform].comment(session_dir, record.get("post_id"), "", sentiment, like=True)
                like_results[f"like_by_{acc}"] = {"account": acc, "success": ok, "error_info": err}
            record["comment_interact_info"] = like_results
        else:
            record["comment_interact_info"] = {}

        record["interacted"], record["interaction_time"] = True, interacted_at

    try:
        article_manager.upsert_articles(pending)
    except Exception:
        _log(
            "互动/回写",
            "远端互动已尝试，状态保存失败",
            level="exception",
            平台=platform,
            记录=len(pending),
            排查="检查数据库；重试前核对已完成的远端互动",
        )
        return

    _log(
        "互动/完成",
        "接口已返回且状态已保存",
        平台=platform,
        帖数=len(pending),
        点赞=enable_like,
        互评=enable_comment,
        互评账号上限=max_comment_accounts,
        耗时=f"{time.monotonic() - started:.2f}秒",
    )


def _hudong_once(article_manager: Optional[Any] = None) -> None:
    mgr = article_manager or GeneratedArticleManager(gen_db_object())
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).timestamp()
    _interact_for_platform(mgr, PLATFORM_BINANCE, cutoff)
    _interact_for_platform(mgr, PLATFORM_OKX, cutoff)


def hudong(stop_event: Optional[threading.Event] = None, max_rounds: int = 0) -> None:
    """评论区互动守护循环。"""
    mgr = GeneratedArticleManager(gen_db_object())
    run_daemon_loop(
        lambda _round_idx: _hudong_once(mgr),
        interval_seconds=INTERACTION_INTERVAL_SECONDS,
        error_backoff_seconds=INTERACTION_INTERVAL_SECONDS,
        name="互动",
        logger_obj=logger,
        max_rounds=max_rounds,
        stop_event=stop_event,
    )


def _parse_time_bound(value: Any, default_ts: float, is_end: bool = False) -> float:
    if value is None:
        return default_ts
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return _publish_time_seconds(value)
    if isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=BEIJING_TZ)
        return dt.timestamp()
    if isinstance(value, str):
        text = value.strip()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(text, fmt).replace(tzinfo=BEIJING_TZ)
                if fmt == "%Y-%m-%d" and is_end:
                    dt = dt + timedelta(days=1) - timedelta(seconds=1)
                return dt.timestamp()
            except ValueError:
                continue
    raise ValueError(f"无法解析时间参数: {value!r}")


def delete_posts_by_account_and_time(
    platform: str = PLATFORM_OKX,
    accounts: Any = None,
    start_time: Any = None,
    end_time: Any = None,
    days: float = 3,
    extra_post_ids: Any = None,
    sync_db: bool = True,
    debug: bool = False,
) -> Dict[str, Dict[str, Any]]:
    """删除指定平台、账号在 `[start_time, end_time]` 时间窗口内的全部帖子（默认近 3 天）。"""
    started = time.monotonic()
    now = time.time()
    end_ts = _parse_time_bound(end_time, now, is_end=True)
    start_ts = _parse_time_bound(start_time, end_ts - float(days) * 86400.0, is_end=False)
    start_str = datetime.fromtimestamp(start_ts, BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
    end_str = datetime.fromtimestamp(end_ts, BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(accounts, str):
        target_accounts = [accounts.strip()] if accounts.strip() else []
    elif isinstance(accounts, (list, tuple, set)):
        target_accounts = [str(a).strip() for a in accounts if str(a).strip()]
    else:
        target_accounts = list(PLATFORM_ACCOUNTS.get(platform) or [])

    if not target_accounts:
        _log("按时间删帖/跳过", "未找到有效目标账号", level="warning", 平台=platform)
        return {}

    article_manager = GeneratedArticleManager(gen_db_object())
    post_manager = UniversalPostManager(gen_db_object())

    account_post_ids: Dict[str, List[str]] = {acc: [] for acc in target_accounts}
    articles_by_post_id: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    all_published_articles = (
        article_manager.find_articles(
            query={f"publish_records.{platform}.post_id": {"$exists": True, "$ne": None}},
            limit=0,
        )
        or []
    )
    for art in all_published_articles:
        rec = ((art.get("publish_records") or {}).get(platform)) or {}
        pid = str(rec.get("post_id") or "").strip()
        if not pid or pid == "None":
            continue
        articles_by_post_id[pid].append(art)
        if rec.get("delete_time"):
            continue
        pub_by = str(rec.get("published_by") or "").strip()
        if pub_by not in account_post_ids:
            continue
        raw_pub_time = rec.get("publish_time")
        if raw_pub_time is not None:
            try:
                pub_ts = _publish_time_seconds(raw_pub_time)
            except ValueError:
                pub_ts = None
        else:
            created_at = art.get("created_at")
            pub_ts = created_at.timestamp() if isinstance(created_at, datetime) else None
        if pub_ts is not None and start_ts <= pub_ts <= end_ts:
            account_post_ids[pub_by].append(pid)

    uid_to_account = {}
    if platform == PLATFORM_OKX:
        uid_to_account = {uid: acc for acc, uid in OKX_ACCOUNT_UID_MAP.items() if acc in account_post_ids}
    elif platform == PLATFORM_BINANCE:
        uid_to_account = {uname: acc for acc, uname in BINANCE_ACCOUNT_USERNAME_MAP.items() if acc in account_post_ids}

    if uid_to_account:
        raw_posts = _find_posts_by_sources(
            post_manager,
            sources=(platform,),
            limit=POST_QUERY_LIMIT,
            extra_query={"publish_time": {"$gte": start_ts - 3600}},
        )
        for p in raw_posts:
            pid = str(p.get("post_id") or "").strip()
            if not pid:
                continue
            author = p.get("author") or {}
            aid = str(
                p.get("author_id")
                or author.get("author_id")
                or author.get("id")
                or author.get("uid")
                or author.get("username")
                or ""
            ).strip()
            acc = uid_to_account.get(aid)
            if not acc:
                continue
            try:
                p_ts = _publish_time_seconds(p.get("publish_time", 0))
            except ValueError:
                continue
            if start_ts <= p_ts <= end_ts:
                account_post_ids[acc].append(pid)

    if isinstance(extra_post_ids, dict):
        for acc, pids in extra_post_ids.items():
            if acc in account_post_ids and pids:
                account_post_ids[acc].extend(str(x).strip() for x in pids if str(x).strip())
    elif isinstance(extra_post_ids, (list, tuple, set)) and target_accounts:
        for pid in extra_post_ids:
            if str(pid).strip():
                account_post_ids[target_accounts[0]].append(str(pid).strip())

    for acc in target_accounts:
        account_post_ids[acc] = list(dict.fromkeys(account_post_ids[acc]))

    _log(
        "按时间删帖/计划",
        "已汇总待删帖清单",
        平台=platform,
        时间段=f"{start_str} ~ {end_str}",
        账号清单={acc: len(pids) for acc, pids in account_post_ids.items()},
    )

    summary: Dict[str, Dict[str, Any]] = {}
    credentials: Dict[str, Dict[str, Any]] = {}
    if platform == PLATFORM_BINANCE:
        credentials, _ = _load_account_credentials("按时间删帖", with_user_info=True)

    for acc in target_accounts:
        pids = account_post_ids.get(acc) or []
        deleted_list, failed_list = [], []

        if platform == PLATFORM_OKX:
            okx_uid = OKX_ACCOUNT_UID_MAP.get(acc)
            if okx_uid:
                try:
                    remote_okx_items = (
                        fetch_okx_user_timeline(
                            author_id=okx_uid,
                            max_pages=60,
                            start_ts=start_ts,
                            end_ts=end_ts,
                        )
                        or []
                    )
                    for item in remote_okx_items:
                        rid = str(item.get("post_id") or "").strip()
                        if rid and rid not in pids:
                            pids.append(rid)
                except Exception as exc:
                    _log("按时间删帖/远端扫描", "拉取 OKX 远端主页帖子列表异常，继续按已知ID删除", level="warning", 账号=acc, 原因=exc)
            if not pids:
                summary[acc] = {"target_count": 0, "deleted": [], "failed": []}
                continue
            session_dir = _get_account_session_dir(PLATFORM_OKX, acc)
            _, _, detail = delete_okx_posts(post_id_list=pids, user_data_dir=session_dir, debug=debug)
            detail = detail or {}
            deleted_list.extend(str(x) for x in (detail.get("success_ids") or []))
            failed_list.extend(str(x) for x in ((detail.get("failed_ids") or {}).keys()))
        elif platform == PLATFORM_BINANCE:
            cred = credentials.get(acc)
            if not cred:
                _log("按时间删帖/跳过", "Binance 账号凭证不可用", level="warning", 账号=acc)
                summary[acc] = {"target_count": len(pids), "deleted": [], "failed": pids}
                continue
            square_uid = (cred.get("user_info") or {}).get("squareUid")
            if square_uid:
                try:
                    remote_items = (
                        fetch_binance_square_replies(
                            target_square_uid=square_uid,
                            cookies=cred["cookies"],
                            csrf_token=cred["csrf_token"],
                            limit=1000,
                            time_offset=int(end_ts * 1000),
                        )
                        or []
                    )
                    for item in remote_items:
                        rid = str(item.get("reply_id") or "").strip()
                        r_ts = item.get("create_time") or item.get("publish_time")
                        if rid:
                            if r_ts is not None:
                                try:
                                    if not (start_ts <= _publish_time_seconds(r_ts) <= end_ts):
                                        continue
                                except ValueError:
                                    pass
                            if rid not in pids:
                                pids.append(rid)
                except Exception as exc:
                    _log("按时间删帖/远端扫描", "拉取 Binance 远端帖子列表异常，继续按已知ID删除", level="warning", 账号=acc, 原因=exc)

            for pid in pids:
                try:
                    ok = delete_binance_square_content(
                        content_id=pid,
                        cookies=cred["cookies"],
                        csrf_token=cred["csrf_token"],
                    )
                    (deleted_list if ok else failed_list).append(pid)
                except Exception:
                    failed_list.append(pid)

        summary[acc] = {"target_count": len(pids), "deleted": deleted_list, "failed": failed_list}

        if sync_db and deleted_list:
            deleted_at = time.time()
            checked_at = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
            reason_str = f"按时间段批量清理 ({start_str} ~ {end_str})"
            updated_articles = []
            for pid in deleted_list:
                for art in articles_by_post_id.get(pid, []):
                    p_rec = art.setdefault("publish_records", {}).setdefault(platform, {})
                    p_rec.update(delete_time=deleted_at, delete_reason=reason_str, check_time=checked_at)
                    updated_articles.append(art)
            if updated_articles:
                try:
                    article_manager.upsert_articles(updated_articles)
                except Exception as exc:
                    _log("按时间删帖/回写", "回写 generated_articles 删除状态异常", level="warning", 账号=acc, 原因=exc)
            try:
                q_ids = list(deleted_list) + [int(x) for x in deleted_list if x.isdigit()]
                post_manager.delete_posts({"post_id": {"$in": q_ids}})
            except Exception as exc:
                _log("按时间删帖/清理原帖", "清理 social_media_posts 自爬记录异常", level="warning", 账号=acc, 原因=exc)

        _log(
            "按时间删帖/账号完成",
            "账号时间段帖子清理完毕",
            平台=platform,
            账号=acc,
            目标数=len(pids),
            成功删除=len(deleted_list),
            失败数=len(failed_list),
        )

    _log(
        "按时间删帖/汇总",
        "全部指定账号删帖任务结束",
        平台=platform,
        时间段=f"{start_str} ~ {end_str}",
        总成功=sum(len(v["deleted"]) for v in summary.values()),
        总失败=sum(len(v["failed"]) for v in summary.values()),
        耗时=f"{time.monotonic() - started:.2f}秒",
    )
    return summary


def _article_delete_reason(article: Dict[str, Any], now: float, prices: Optional[Tuple[float, float]]) -> Tuple[Optional[str], bool]:
    b_rec = ((article.get("publish_records") or {}).get(PLATFORM_BINANCE)) or {}
    price_updated = prices is not None
    if article.get("type") == ARTICLE_TYPE_VIRAL:
        prices = None
        price_updated = False
    elif price_updated and prices is not None:
        base_price, current_price = prices
        difference = (current_price - base_price) / base_price
        profit = difference if article.get("stance") == "看多" else -difference
        b_rec["profit_pct"] = round(profit, 4)
        threshold = 0.2 if article.get("stance") == "看多" else 0.5
        if profit <= -threshold:
            return f"逆向 {abs(profit) * 100:.2f}% (基准: {threshold * 100:.0f}%)", True
    publish_time = b_rec.get("publish_time")
    if publish_time is None:
        return None, price_updated
    try:
        elapsed = now - _publish_time_seconds(publish_time)
        if elapsed >= MAX_REPLAY_DAYS * 24 * 3600:
            return f"超时 (已发布 {elapsed / (24 * 3600):.2f} 天, 基准: {MAX_REPLAY_DAYS} 天)", price_updated
    except Exception:
        logger.warning(
            f"[历史清理/时间] 无法判断帖子是否超时 | 帖子: [{b_rec.get('post_id')}] | 发布时间: [{publish_time}] "
            "| 后续: [保留当前时效状态] | 排查: [核对秒/毫秒时间戳]",
            exc_info=True,
        )
    return None, price_updated


def _delete_old_posts_once(article_manager: Optional[Any] = None) -> None:
    mgr = article_manager or GeneratedArticleManager(gen_db_object())
    started, now = time.monotonic(), time.time()
    reverse_count = timeout_count = failed = 0
    deleted_ids = set()
    credentials, account_errors = _load_account_credentials("历史清理", with_user_info=True)
    try:
        articles = (
            mgr.find_articles(
                query={
                    f"publish_records.{PLATFORM_BINANCE}.status": "success",
                    f"publish_records.{PLATFORM_BINANCE}.post_id": {"$exists": True, "$ne": None},
                    f"publish_records.{PLATFORM_BINANCE}.delete_time": {"$in": [None, False]},
                }
            )
            or []
        )
        logger.info(f"[历史清理/巡检] 已查询待检测文章 | 文章: [{len(articles)}] | 可用账号: [{len(credentials)}]")
        for article in articles:
            b_rec = ((article.get("publish_records") or {}).get(PLATFORM_BINANCE)) or {}
            if b_rec.get("delete_time"):
                continue
            post_id = str(b_rec.get("post_id") or "").strip()
            if not post_id:
                continue
            account, topic, stance = b_rec.get("published_by"), article.get("topic"), article.get("stance")
            prices = None
            try:
                if topic and stance in ("看多", "看空") and b_rec.get("publish_price") is not None:
                    base_price = float(b_rec["publish_price"])
                    if base_price > 0:
                        latest = get_latest_crypto_price(topic)
                        if latest is not None:
                            prices = base_price, float(latest)
            except Exception:
                logger.warning(
                    f"[历史清理/行情] 读取或转换价格失败 | 帖子: [{post_id}] | 币种: [{topic}] "
                    "| 后续: [继续时效检查] | 排查: [检查价格字段、行情接口与网络]",
                    exc_info=True,
                )
            reason, price_updated = _article_delete_reason(article, now, prices)
            deleted = False
            credential = credentials.get(account)
            if reason and not credential:
                logger.warning(
                    f"[历史清理/无法删除] 已触发删除条件，但账号凭证不可用 | 账号: [{account}] | 帖子: [{post_id}] "
                    f"| 原因: [{reason}] | 排查: [恢复对应账号登录与会话]"
                )
            if reason and credential:
                stage = "请求远端删除"
                try:
                    success = delete_binance_square_content(
                        content_id=post_id,
                        cookies=credential["cookies"],
                        csrf_token=credential["csrf_token"],
                    )
                    if success:
                        deleted = True
                        deleted_ids.add(post_id)
                        b_rec.update(
                            delete_time=time.time(),
                            delete_reason=reason,
                            check_time=datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S"),
                        )
                        stage = "回写已删除状态"
                        mgr.upsert_articles([article])
                        reverse_count += int(reason.startswith("逆向"))
                        timeout_count += int(not reason.startswith("逆向"))
                        logger.info(
                            f"[历史清理/删除] 远端删除并回写成功 | 帖子: [{post_id}] | 账号: [{account}] | 原因: [{reason}]"
                        )
                    else:
                        failed += 1
                        logger.warning(
                            f"[历史清理/删除] 接口未确认成功 | 帖子: [{post_id}] | 账号: [{account}] "
                            "| 后续: [下轮再检查] | 排查: [检查凭证、限流与远端帖子是否存在]"
                        )
                except Exception:
                    failed += 1
                    logger.exception(
                        f"❌ [历史清理/删除] 帖子处理失败 | 阶段: [{stage}] | 帖子: [{post_id}] | 账号: [{account}] "
                        f"| 远端已确认删除: [{deleted}] | 后续: [继续其他帖子] "
                        "| 排查: [检查删除接口或数据库，先核对远端结果]"
                    )
            if not deleted and price_updated:
                try:
                    b_rec["check_time"] = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
                    mgr.upsert_articles([article])
                except Exception:
                    logger.warning(
                        f"[历史清理/行情回写] 涨跌幅保存失败 | 帖子: [{post_id}] | 后续: [继续其他帖子] "
                        "| 排查: [检查数据库连接与写入权限]",
                        exc_info=True,
                    )
    except Exception:
        logger.exception(
            "❌ [历史清理/巡检] 数据库扫描或处理异常 | 后续: [继续远端兜底清理] "
            "| 排查: [检查数据库连接、文章结构与删除结果]"
        )

    try:
        cutoff_ms = int((now - MAX_REPLAY_DAYS * 24 * 3600) * 1000)
        for account in ACCOUNTS:
            credential = credentials.get(account)
            if not credential:
                continue
            square_uid = (credential.get("user_info") or {}).get("squareUid")
            if not square_uid:
                continue
            stage = "拉取历史帖子与回复"
            try:
                items = (
                    fetch_binance_square_replies(
                        target_square_uid=square_uid,
                        cookies=credential["cookies"],
                        csrf_token=credential["csrf_token"],
                        limit=1000,
                        time_offset=cutoff_ms,
                        filter_type="REPLY",
                    )
                    or []
                )
                items.extend(
                    fetch_binance_square_replies(
                        target_square_uid=square_uid,
                        cookies=credential["cookies"],
                        csrf_token=credential["csrf_token"],
                        limit=1000,
                        time_offset=cutoff_ms,
                    )
                    or []
                )
                for item in items:
                    raw_id = item.get("reply_id")
                    if not raw_id or str(raw_id) in deleted_ids:
                        continue
                    post_id = str(raw_id)
                    try:
                        success = delete_binance_square_content(
                            content_id=post_id,
                            cookies=credential["cookies"],
                            csrf_token=credential["csrf_token"],
                        )
                    except Exception:
                        failed += 1
                        logger.exception(
                            f"❌ [历史清理/远端删除] 删除请求异常 | 账号: [{account}] | 帖子: [{post_id}] "
                            "| 后续: [继续其他记录] | 排查: [检查凭证、网络与限流，核对远端结果]"
                        )
                        continue
                    if not success:
                        failed += 1
                        logger.warning(
                            f"[历史清理/远端删除] 接口未确认成功 | 账号: [{account}] | 帖子: [{post_id}] "
                            "| 排查: [检查帖子是否存在、登录与接口响应]"
                        )
                        continue
                    timeout_count += 1
                    deleted_ids.add(post_id)
                    stage = "回写远端已删除记录"
                    matched = mgr.find_articles(query={f"publish_records.{PLATFORM_BINANCE}.post_id": post_id})
                    if not matched:
                        continue
                    deleted_at = time.time()
                    checked_at = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
                    for article in matched:
                        b_rec = article.setdefault("publish_records", {}).setdefault(PLATFORM_BINANCE, {})
                        b_rec.update(
                            delete_time=deleted_at,
                            delete_reason=f"超时 (基准: {MAX_REPLAY_DAYS} 天)",
                            check_time=checked_at,
                        )
                    mgr.upsert_articles(matched)
                    logger.info(f"[历史清理/远端回写] 已保存删除状态 | 帖子: [{post_id}] | 记录: [{len(matched)}]")
            except Exception:
                account_errors += 1
                logger.exception(
                    f"❌ [历史清理/远端账号] 当前账号处理失败 | 账号: [{account}] | 阶段: [{stage}] "
                    "| 后续: [继续其他账号] | 排查: [检查历史接口、用户信息及数据库；核对已完成的删除]"
                )
    except Exception:
        logger.exception(
            "❌ [历史清理/兜底] 远端清理本轮中断 | 后续: [下轮重试] "
            "| 排查: [检查账号用户信息、历史列表与数据库]"
        )

    logger.info(
        f"[历史清理/汇总] 本轮结束 | 逆向删除: [{reverse_count}] | 超时删除: [{timeout_count}] | 失败: [{failed}] "
        f"| 账号异常: [{account_errors}] | 耗时: [{time.monotonic() - started:.2f} 秒] | 下次检查: [{DELETE_INTERVAL_SEC} 秒后]"
    )


def delete_old_post(stop_event: Optional[threading.Event] = None, max_rounds: int = 0) -> None:
    """历史逆向与超期帖定期自动清理守护循环。"""
    mgr = GeneratedArticleManager(gen_db_object())
    run_daemon_loop(
        lambda _round_idx: _delete_old_posts_once(mgr),
        interval_seconds=DELETE_INTERVAL_SEC,
        error_backoff_seconds=DELETE_INTERVAL_SEC,
        name="历史清理",
        logger_obj=logger,
        max_rounds=max_rounds,
        stop_event=stop_event,
    )


def clear_all_media_format_batch() -> None:
    """手动批量清空所有已抽取的 `logic_mul` 与 `pre_screen` 字段（谨慎使用）。"""
    manager = UniversalPostManager(gen_db_object())
    posts = _find_posts_by_sources(manager, limit=POST_QUERY_LIMIT)
    updates = [post for post in posts if post.get("logic_mul") is not None or post.get("pre_screen") is not None]
    for post in updates:
        post["logic_mul"] = post["pre_screen"] = None
    if updates:
        manager.upsert_posts(updates)
    logger.info(f"[数据/清理] 提取与初筛字段已清空 | 扫描: [{len(posts)}] | 更新: [{len(updates)}]")


# ==============================================================================
# 人工查询与复盘工具
# ==============================================================================
def clean_article_records(raw_records: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """精简待复盘文章字段。"""
    results = []
    for item in raw_records:
        if not isinstance(item, dict):
            continue
        info, brief = item.get("article_info") or {}, item.get("creation_brief") or {}
        results.append(
            {
                "article_info": {
                    key: info.get(key, "" if key == "text" else [])
                    for key in ("text", "god_comments", "image_placeholders", "used_material_ids")
                },
                "creation_brief": {"task": brief.get("task", {}), "materials": brief.get("materials", [])},
            }
        )
    return results


def get_need_check_article_info() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """随机抽取近 6 小时待复盘未发布分析文章（最多 10 篇）及其关联原帖信息。"""
    manager = GeneratedArticleManager(gen_db_object())
    candidates = (
        manager.find_articles(
            query={
                "created_at": {"$gte": datetime.now(timezone.utc) - timedelta(hours=6)},
                "status": "ok",
                f"publish_records.{PLATFORM_BINANCE}.status": {"$ne": "success"},
                f"publish_records.{PLATFORM_OKX}.status": {"$ne": "success"},
            }
        )
        or []
    )
    candidates = [article for article in candidates if article.get("prompt_version") == prompt_version]
    random.shuffle(candidates)
    selected = candidates[:10]
    data = clean_article_records(selected)
    raw_ids = set()
    for article in selected:
        mapping = article.get("material_post_mapping") or {}
        if isinstance(mapping, dict):
            raw_ids.update(str(post_id).strip() for post_id in mapping.values() if post_id)
    source_posts = []
    if raw_ids:
        post_manager = UniversalPostManager(gen_db_object())
        ids = list(raw_ids)
        query_ids = ids + [int(post_id) for post_id in ids if post_id.isdigit()]
        raw_posts = post_manager.find_posts(query={"post_id": {"$in": query_ids}}, limit=0) or []
        source_posts = [
            {"origin_content": (post.get("content") or {}).get("text_content", ""), "logic_mul": post.get("logic_mul")}
            for post in raw_posts
            if isinstance(post, dict)
        ]
    logger.info(
        f"[文章/待审查] 审查数据已组装 | 候选: [{len(candidates)}] | 随机抽取: [{len(data)}] | 关联原帖: [{len(source_posts)}]"
    )
    return data, source_posts


def query_hot_posts_by_keyword(target_key: str, hours: int = 24, min_views: int = 1000) -> List[Dict[str, Any]]:
    """按关键字检索近 `hours` 小时高阅读热点原帖。"""
    manager = UniversalPostManager(gen_db_object())
    start_time = time.time() - hours * 3600
    query = {
        "source": {"$in": list(POST_SOURCES)},
        "publish_time": {"$gte": start_time},
        "engagement.view_count": {"$gte": min_views},
        "content.text_content": {"$regex": target_key, "$options": "i"},
    }
    posts = manager.find_posts(query=query, sort=[("engagement.view_count", -1)], limit=0) or []
    results = []
    for post in posts:
        engagement = post.get("engagement") or {}
        results.append(
            {
                "text_content": (post.get("content") or {}).get("text_content", ""),
                "images": (post.get("media") or {}).get("images", []),
                "engagement": {key: engagement.get(key, 0) for key in ("view_count", "like_count", "comment_count")},
            }
        )
    logger.info(f"[数据/查询] 热门帖查询完成 | 关键字: [{target_key}] | 时长: [{hours} 小时] | 匹配: [{len(results)}]")
    return results


def get_need_check_pre_screen_info(
    limit: int = 100,
    no_evidence_filter: Optional[bool] = None,
    only_current_version: bool = True,
    random_sample: bool = True,
    save_path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """抽样导出分析类初筛结果供提示词质量复盘。"""
    post_manager = UniversalPostManager(gen_db_object())
    query: Dict[str, Any] = {
        "source": {"$in": list(POST_SOURCES)},
        "pre_screen": {"$exists": True, "$ne": None},
    }
    if only_current_version:
        query["pre_screen.prompt_version"] = prompt_version
    if isinstance(no_evidence_filter, bool):
        query["pre_screen.no_evidence"] = no_evidence_filter

    raw_posts = post_manager.find_posts(query=query, limit=0) or []
    total_matched = len(raw_posts)
    total_no_ev = sum(1 for p in raw_posts if (p.get("pre_screen") or {}).get("no_evidence") is True)
    total_has_ev = total_matched - total_no_ev

    if random_sample:
        random.shuffle(raw_posts)
    selected_posts = raw_posts[:limit] if (limit and limit > 0) else raw_posts

    review_list = []
    for post in selected_posts:
        if not isinstance(post, dict):
            continue
        base_item = build_pre_screen_item(post)
        pre_screen = post.get("pre_screen") or {}
        review_list.append(
            {
                "post_id": base_item["post_id"],
                "text_content": base_item["text_content"],
                "images": base_item["images"],
                "no_evidence": pre_screen.get("no_evidence"),
                "reason": pre_screen.get("reason", ""),
            }
        )

    if save_path:
        save_json(save_path, review_list)

    logger.info(
        f"[初筛/审查] 已导出初筛复盘数据 | 匹配总数: [{total_matched} (淘汰: {total_no_ev} / 保留: {total_has_ev})] "
        f"| 本次抽取: [{len(review_list)}] | 过滤条件(no_evidence): [{no_evidence_filter}] | 导出文件: [{save_path or '未落盘'}]"
    )
    return review_list


def get_need_check_viral_article_info(
    limit: int = 100,
    hours: int = 12,
    only_current_version: bool = True,
    random_sample: bool = True,
    save_path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """抽样组装高流量二阶段重塑复盘数据集。"""
    article_manager = GeneratedArticleManager(gen_db_object())
    query: Dict[str, Any] = {
        "type": ARTICLE_TYPE_VIRAL,
        f"publish_records.{PLATFORM_BINANCE}.status": {"$ne": "success"},
        f"publish_records.{PLATFORM_OKX}.status": {"$ne": "success"},
    }
    if hours and hours > 0:
        query["created_at"] = {"$gte": datetime.now(timezone.utc) - timedelta(hours=hours)}
    if only_current_version:
        query["prompt_version"] = prompt_version

    candidates = article_manager.find_articles(query=query, sort=[("created_at", -1)]) or []
    if not candidates:
        logger.info(f"[高流量/审查] 未找到待审查文章 | 筛选时间: [近 {hours} 小时]")
        return []

    if random_sample:
        random.shuffle(candidates)
    selected_articles = candidates[:limit] if (limit and limit > 0) else candidates

    post_ids = set()
    for art in selected_articles:
        for pid in art.get("post_id_list") or []:
            if pid is not None and str(pid).strip():
                post_ids.add(str(pid).strip())
                if str(pid).strip().isdigit():
                    post_ids.add(int(pid))

    post_map: Dict[str, Dict[str, Any]] = {}
    if post_ids:
        post_manager = UniversalPostManager(gen_db_object())
        raw_posts = post_manager.find_posts(query={"post_id": {"$in": list(post_ids)}}, limit=0) or []
        for p in raw_posts:
            if isinstance(p, dict) and p.get("post_id") is not None:
                post_map[str(p["post_id"]).strip()] = p

    review_list = []
    for article in selected_articles:
        p_list = article.get("post_id_list") or []
        target_post_id = str(p_list[0]).strip() if p_list else ""
        raw_p = post_map.get(target_post_id, {})
        info = article.get("article_info") or {}
        engagement = raw_p.get("engagement") or {}

        clean_text, _, mapping = normalize_post_media(raw_p) if raw_p else ("", [], {})
        image_placeholders = list(mapping.keys())

        source_post_data = {
            "post_id": str(raw_p.get("post_id") or target_post_id).strip(),
            "text": clean_text.strip(),
            "images": image_placeholders,
            "engagement": {
                "view_count": engagement.get("view_count", 0),
                "like_count": engagement.get("like_count", 0),
                "comment_count": engagement.get("comment_count", 0),
            },
        }

        usable = info.get("usable", False)
        if not usable:
            remodeled_article_data: Dict[str, Any] = {
                "usable": False,
                "image_semantics": info.get("image_semantics", []),
                "viral_analysis": info.get("viral_analysis", ""),
                "decision_reason": info.get("decision_reason", ""),
            }
        else:
            remodeled_article_data = {
                "usable": True,
                "topic_tag": info.get("topic_tag", ""),
                "viral_score": info.get("viral_score", 0.0),
                "image_semantics": info.get("image_semantics", []),
                "selected_images": info.get("selected_images", []),
                "viral_analysis": info.get("viral_analysis", ""),
                "decision_reason": info.get("decision_reason", ""),
                "image_role": info.get("image_role", ""),
                "post_text": info.get("post_text", ""),
                "bait_comments": info.get("bait_comments", []),
            }

        remodeled_article_data = {
            k: v for k, v in remodeled_article_data.items() if k == "usable" or v not in (None, "", [], {})
        }
        review_list.append({"source_post": source_post_data, "remodeled_article": remodeled_article_data})

    if save_path:
        save_json(save_path, review_list)

    logger.info(
        f"[高流量/审查] 精简复盘数据组装完成 | 候选: [{len(candidates)}] | 抽样返回: [{len(review_list)}] "
        f"| 导出文件: [{save_path or '未落盘'}]"
    )
    return review_list


def sync_recent_metrics_once(
    article_manager: Optional[Any] = None,
    *,
    lookback_hours: int = 72,
    limit_per_platform: int = 60,
    now_ts: Optional[float] = None,
) -> Dict[str, Dict[str, int]]:
    """独立后台流量回访：免登只读拉取近期已发帖曲线并写回 `publish_records.<platform>.metrics_timeline`。"""
    mgr = article_manager or GeneratedArticleManager(gen_db_object())
    cur_ts = time.time() if now_ts is None else float(now_ts)
    cutoff = cur_ts - max(1, int(lookback_hours)) * 3600
    summary: Dict[str, Dict[str, int]] = {}

    for platform in (PLATFORM_BINANCE, PLATFORM_OKX):
        docs = (
            mgr.find_articles(
                query={
                    "status": "ok",
                    f"publish_records.{platform}.status": "success",
                    f"publish_records.{platform}.publish_time": {"$gte": cutoff},
                },
                sort=[(f"publish_records.{platform}.publish_time", -1)],
                limit=limit_per_platform,
            )
            or []
        )
        prev_synced = {
            idx: ((doc.get("publish_records") or {}).get(platform) or {}).get("metrics_synced_at")
            for idx, doc in enumerate(docs)
            if isinstance(doc, dict)
        }
        if docs:
            sync_published_metrics(mgr, platform, docs, now_ts=cur_ts)

        updated = 0
        missing_or_hidden = 0
        for idx, doc in enumerate(docs):
            if not isinstance(doc, dict):
                continue
            rec = (doc.get("publish_records") or {}).get(platform) or {}
            if rec.get("metrics_synced_at") != prev_synced.get(idx):
                updated += 1
            if rec.get("live_state") in ("deleted", "missing", "hidden"):
                missing_or_hidden += 1

        summary[platform] = {
            "scanned": len(docs),
            "updated": updated,
            "missing_or_hidden": missing_or_hidden,
        }
        _log(
            "指标追踪/汇总",
            "本轮已发帖流量曲线回访完成",
            平台=platform,
            扫描=len(docs),
            更新=updated,
            异常或被删=missing_or_hidden,
        )
    return summary


def sync_recent_metrics(stop_event: Optional[threading.Event] = None, max_rounds: int = 0) -> None:
    """定时后台回访已发帖阅读与互动增长曲线。"""
    mgr = GeneratedArticleManager(gen_db_object())
    run_daemon_loop(
        lambda _round_idx: sync_recent_metrics_once(mgr),
        interval_seconds=METRICS_SYNC_INTERVAL_SECONDS,
        jitter_ratio=0.1,
        error_backoff_seconds=120,
        name="指标追踪",
        logger_obj=logger,
        max_rounds=max_rounds,
        stop_event=stop_event,
    )


def _format_timeline_curve(timeline: Any) -> str:
    if not isinstance(timeline, list) or not timeline:
        return "暂无曲线"
    parts: List[str] = []
    for item in timeline:
        if not isinstance(item, dict):
            continue
        age_m = item.get("age_m")
        age_label = f"{int(round(float(age_m)))}m" if isinstance(age_m, (int, float)) else "?"
        val = item.get("v")
        if val is None:
            val_label = str(item.get("state") or "?")
        else:
            val_label = str(int(val))
        parts.append(f"{age_label}:{val_label}")
    return " -> ".join(parts) if parts else "暂无曲线"


def build_publish_telemetry_report(
    article_manager: Optional[Any] = None,
    *,
    hours: int = 24,
    sync_first: bool = True,
    now_ts: Optional[float] = None,
) -> str:
    """生成一行一帖 + 分组汇总的简洁复盘战报（含生命周期增长曲线与异常状态标记）。"""
    mgr = article_manager or GeneratedArticleManager(gen_db_object())
    cur_ts = time.time() if now_ts is None else float(now_ts)
    if sync_first:
        sync_recent_metrics_once(mgr, lookback_hours=max(int(hours), 48), now_ts=cur_ts)

    cutoff = cur_ts - max(1, int(hours)) * 3600
    rows: List[Tuple[float, str, Dict[str, Any], Dict[str, Any]]] = []
    for platform in (PLATFORM_BINANCE, PLATFORM_OKX):
        docs = (
            mgr.find_articles(
                query={
                    "status": "ok",
                    f"publish_records.{platform}.status": "success",
                    f"publish_records.{platform}.publish_time": {"$gte": cutoff},
                },
                sort=[(f"publish_records.{platform}.publish_time", -1)],
                limit=200,
            )
            or []
        )
        for doc in docs:
            if not isinstance(doc, dict):
                continue
            rec = (doc.get("publish_records") or {}).get(platform) or {}
            raw_pt = rec.get("publish_time") or 0
            pt = (raw_pt / 1000.0) if float(raw_pt) > 1e11 else float(raw_pt)
            if pt >= cutoff:
                rows.append((pt, platform, rec, doc))
    rows.sort(key=lambda x: x[0])

    header_time = datetime.fromtimestamp(cur_ts, tz=BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
    lines: List[str] = [f"=== 近 {hours}h 发布效果与流量曲线复盘 ({header_time} CST, 共 {len(rows)} 篇) ==="]
    grouped: Dict[Tuple[str, str, str], List[Tuple[Optional[int], int, int, bool]]] = defaultdict(list)

    for pt, platform, rec, doc in rows:
        age_h = max(0.0, (cur_ts - pt) / 3600.0)
        acc = str(rec.get("published_by") or "?")
        atype = str(doc.get("type") or "?")
        src = str(doc.get("source") or "?")
        pid = str(rec.get("post_id") or "?")
        v = rec.get("view_count")
        l = int(rec.get("like_count") or 0)
        c = int(rec.get("comment_count") or 0)
        s = int(rec.get("share_count") or 0)
        state = str(rec.get("live_state") or ("ok" if v is not None else "pending"))
        is_bad = state in ("deleted", "missing", "hidden") or (rec.get("recommend") is False)
        snap = rec.get("publish_snapshot") or {}
        src_v = snap.get("source_views")
        fit = rec.get("platform_fit_score")
        pairs = rec.get("coin_pair_list")
        extra = []
        if platform == PLATFORM_BINANCE:
            extra.append(f"rec={rec.get('recommend')}/cs={rec.get('content_status')}")
            extra.append(f"pairs={pairs}")
        else:
            extra.append(f"rev={rec.get('review_state')}")
        if fit is not None:
            extra.append(f"fit={fit}")
        if src_v is not None:
            extra.append(f"src_v={src_v}")

        pt_str = datetime.fromtimestamp(pt, tz=BEIJING_TZ).strftime("%m-%d %H:%M")
        curve_str = _format_timeline_curve(rec.get("metrics_timeline"))
        flag = ""
        if state in ("deleted", "missing", "hidden"):
            flag = f" [!{state}]"
        elif age_h >= 2.0 and isinstance(v, (int, float)) and v < 50:
            flag = " [!低流停滞]"
        elif isinstance(v, (int, float)) and v >= 500:
            flag = " [★高流]"

        lines.append(
            f"[{pt_str}] ({age_h:4.1f}h) {platform.upper():7s} {acc:6s} {atype:8s} src={src:7s} "
            f"pid={pid} v={v} l={l} c={c} s={s} state={state} {' '.join(extra)}{flag}\n"
            f"    curve: {curve_str}"
        )
        numeric_v = int(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None
        grouped[(platform, acc, atype)].append((numeric_v, l, c, is_bad))

    lines.append("\n=== 分组汇总 (platform / account / type) ===")
    for (platform, acc, atype), entries in sorted(grouped.items()):
        views_list = sorted(x[0] for x in entries if x[0] is not None)
        med = views_list[len(views_list) // 2] if views_list else None
        avg = round(sum(views_list) / len(views_list), 1) if views_list else None
        total_l = sum(x[1] for x in entries)
        total_c = sum(x[2] for x in entries)
        bad_cnt = sum(1 for x in entries if x[3])
        lines.append(
            f"  {platform:7s} / {acc:6s} / {atype:8s}: n={len(entries)} live={len(views_list)} "
            f"bad/del={bad_cnt} med_v={med} avg_v={avg} likes={total_l} comments={total_c}"
        )
    return "\n".join(lines)


def _main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="已发帖流量回访与复盘战报工具")
    parser.add_argument("--report-hours", type=int, default=24, help="输出最近 N 小时的发帖复盘战报（默认 24）")
    parser.add_argument("--no-sync", action="store_true", help="跳过远端同步，仅读取本地库内已落盘轨迹")
    args = parser.parse_args(argv)
    print(build_publish_telemetry_report(hours=args.report_hours, sync_first=not args.no_sync))
    return 0


__all__ = [
    "INTERACTION_CONFIG",
    "_article_delete_reason",
    "_delete_old_posts_once",
    "_hudong_once",
    "build_publish_telemetry_report",
    "clean_article_records",
    "clear_all_media_format_batch",
    "delete_old_post",
    "delete_posts_by_account_and_time",
    "get_need_check_article_info",
    "get_need_check_pre_screen_info",
    "get_need_check_viral_article_info",
    "hudong",
    "interact_comment",
    "query_hot_posts_by_keyword",
    "sync_recent_metrics",
    "sync_recent_metrics_once",
]


if __name__ == "__main__":
    raise SystemExit(_main())
