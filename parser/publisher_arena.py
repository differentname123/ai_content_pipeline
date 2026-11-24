# -*- coding: utf-8 -*-
"""
跨平台去重、总编 Top-50 擂台与自动发布调度引擎 (`parser/publisher_arena.py`)

核心职责与进化点：
1. 发布前零成本账号可用性预检与并发互斥占用：通过 `AccountPublishStateStore` 统一校验
   账号级全局间隔冷却（合并分析与流量帖）并通过 `try_claim_account` 防止双线程并发连发撞车；
   全平台无可用账号时立即跳过，**零 DB 查询、零指标同步、零总编大模型调用**；
2. 7 天跨文章类型视觉内容去重 (`content_id` dHash + URL + 本地路径) 与 2 小时同账号同币冷却；
3. 跨平台竞品内容与截图界面分流规范：校验文本竞品词及选中配图 `exchange_ui`，严禁竞品交易所界面发往对方平台，并保留 OKX 排除 OKX 原创帖规范；
4. 总编横向擂台：拉取近 50 条历史战报与互动数据，剥离内部评分后经由 `common.llm_gateway.call_llm_json`
   (`model=None, preset_model_group="medium"`) 选稿，记录真实 `selection_model_used` 并回写数据库。
"""
import json
import math
import re
import threading
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

from common.common_utils import (
    PLATFORM_ACCOUNTS,
    PLATFORM_BINANCE,
    PLATFORM_OKX,
    get_image_content_id,
)
from common.daemon import run_daemon_loop
from common.llm_gateway import call_llm_json, render_prompt_template
from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import GeneratedArticleManager, UniversalPostManager
from parser.platform_adapters import (
    BEIJING_TZ,
    COIN_TAG_PATTERN,
    PLATFORMS,
    _execute_platform_post,
    _format_analysis_post_content,
    _format_viral_post_content,
    _get_account_session_dir,
    _log,
    _normalize_hot_coins,
    _safe_get_latest_price,
    has_rival_exchange_ui,
    has_rival_keyword,
    logger,
    normalize_exchange_ui,
    score_platform_candidate,
    score_platform_candidate_form,
    sync_published_metrics,
)
from parser.post_ingestion import (
    ANALYSIS_EDITOR_PROMPT_NAME,
    ARTICLE_MAX_CHARS,
    ARTICLE_TYPE_ANALYSIS,
    ARTICLE_TYPE_VIRAL,
    IMAGE_DEDUP_LOOKBACK_HOURS,
    OKX_SOURCE,
    PUBLISH_MAX_ATTEMPTS,
    PUBLISH_POLL_SECONDS,
    PUBLISH_RULES,
    SELECTION_PROMPT_VERSION,
    VIRAL_EDITOR_PROMPT_NAME,
    _is_valid_image_file,
    prompt_version,
)
from parser.publish_state import (
    AccountPublishStateStore,
    classify_infra_backoff_error,
    default_state_store,
)
from platforms.playwright_base import ChallengeRequiredError


# ==============================================================================
# 图片去重与候选筛选
# ==============================================================================
_VIRAL_CORE_COINS = ("BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "FIL", "DOT")


def _extract_selected_image_exchange_uis(article: Dict[str, Any]) -> List[str]:
    """提取当前文章实际选中配图的交易平台界面标识列表（老数据缺失该字段时默认空串放行）。"""
    info = article.get("article_info")
    if not isinstance(info, dict):
        return []
    mapping = info.get("image_mapping")
    if not isinstance(mapping, dict):
        mapping = {}
    placeholders = (
        info.get("selected_images")
        if article.get("type") == ARTICLE_TYPE_VIRAL
        else info.get("image_placeholders")
    )
    if not isinstance(placeholders, list) or not placeholders:
        placeholders = list(mapping.keys())
    sem_by_id = {
        item.get("id"): item.get("exchange_ui")
        for item in (info.get("image_semantics") or [])
        if isinstance(item, dict) and item.get("id")
    }
    uis: List[str] = []
    for key in placeholders:
        meta = mapping.get(key)
        raw_ui = meta.get("exchange_ui") if isinstance(meta, dict) else None
        if raw_ui is None:
            raw_ui = sem_by_id.get(key)
        uis.append(normalize_exchange_ui(raw_ui))
    return uis


def _extract_article_image_addresses(article: Dict[str, Any]) -> Tuple[Set[str], List[str]]:
    """提取文章采用图片的全部去重标识（dHash / 本地路径 / 原始 URL）与统计主标识列表。"""
    info = article.get("article_info")
    if not isinstance(info, dict):
        return set(), []
    mapping = info.get("image_mapping")
    if not isinstance(mapping, dict) or not mapping:
        return set(), []
    placeholders = (
        info.get("selected_images")
        if article.get("type") == ARTICLE_TYPE_VIRAL
        else info.get("image_placeholders")
    )
    selected = (
        [mapping[key] for key in placeholders if key in mapping]
        if isinstance(placeholders, list) and placeholders
        else mapping.values()
    )
    addresses: Set[str] = set()
    primary: List[str] = []
    for item in selected:
        if not isinstance(item, dict):
            continue
        url = (item.get("original_url") or "").strip()
        path = (item.get("local_path") or "").strip()
        content_id = (item.get("content_id") or "").strip() or get_image_content_id(path)
        addresses.update(value for value in (url, path, content_id) if value)
        primary_key = content_id or path or url
        if primary_key:
            primary.append(primary_key)
    return addresses, primary


def _get_recent_published_image_usage(
    article_manager: Any,
    platform: str,
    hours: int = IMAGE_DEDUP_LOOKBACK_HOURS,
    now: Optional[float] = None,
) -> Tuple[Set[str], List[str], int]:
    """查询指定平台近 `hours` 小时内跨类型已发布占用的全部图片标识集合。"""
    cur_ts = time.time() if now is None else float(now)
    recent = (
        article_manager.find_articles(
            query={
                "status": "ok",
                f"publish_records.{platform}.status": "success",
                f"publish_records.{platform}.publish_time": {"$gte": cur_ts - hours * 3600},
            }
        )
        or []
    )
    addresses: Set[str] = set()
    primary: List[str] = []
    for article in recent:
        used, first = _extract_article_image_addresses(article)
        addresses.update(used)
        primary.extend(first)
    return addresses, primary, len(recent)


def _get_valid_publish_candidate_indices(
    account_state: Dict[str, Any],
    candidates: Sequence[Tuple[Dict[str, Any], Set[str], str]],
    now: float,
    used_addresses: Set[str],
    article_type: str,
    *,
    limit: int = 50,
    state_store: Optional[AccountPublishStateStore] = None,
    platform: str = "",
    account: str = "",
) -> List[int]:
    """筛出当前账号满足 2 小时币种冷却与视觉图片去重约束的前 `limit` 个候选索引。"""
    store = state_store or default_state_store
    valid_indices: List[int] = []
    for index, (article, addresses, _) in enumerate(candidates):
        if article_type == ARTICLE_TYPE_ANALYSIS:
            coins_to_check = article.get("coins") or ([article.get("topic")] if article.get("topic") else [])
            if store.is_topic_in_cooldown(
                platform,
                account,
                coins_to_check,
                now=now,
                account_state=account_state,
            ):
                continue
        if addresses and addresses & used_addresses:
            continue
        valid_indices.append(index)
        if len(valid_indices) >= limit:
            break
    return valid_indices


def _select_publish_candidate(
    account_state: Dict[str, Any],
    candidates: Sequence[Tuple[Dict[str, Any], Set[str], str]],
    now: float,
    used_addresses: Set[str],
    article_type: str,
) -> Optional[int]:
    valid = _get_valid_publish_candidate_indices(
        account_state,
        candidates,
        now,
        used_addresses,
        article_type,
        limit=1,
    )
    return valid[0] if valid else None


def _collect_publish_candidates(
    article_manager: Any,
    platform: str,
    hot_coins: Optional[Sequence[Any]],
    used_addresses: Set[str],
    article_type: str,
) -> Tuple[List[Tuple[Dict[str, Any], Set[str], str]], int, Counter]:
    """查询、校验并排序可发布候选文章，返回 `(candidates, raw_count, filter_stats)`。"""
    rule = PUBLISH_RULES[article_type]
    viral = article_type == ARTICLE_TYPE_VIRAL
    type_query = ARTICLE_TYPE_VIRAL if viral else ARTICLE_TYPE_ANALYSIS

    raw = (
        article_manager.find_articles(
            query={
                "type": type_query,
                "created_at": {"$gte": datetime.now(timezone.utc) - timedelta(hours=rule["hours"])},
                "status": "ok",
                f"publish_records.{platform}.status": {"$ne": "success"},
            }
        )
        or []
    )
    coins = _normalize_hot_coins(hot_coins)
    hot_set = {coin.lower() for coin in coins}
    viral_coins = list(dict.fromkeys([*coins, *_VIRAL_CORE_COINS]))
    patterns = (
        [
            (coin.upper(), re.compile(rf"(?<![A-Za-z0-9]){re.escape(coin)}(?:USDT|USDC)?(?![A-Za-z0-9])", re.IGNORECASE))
            for coin in viral_coins
        ]
        if viral
        else []
    )

    candidates: List[Tuple[Dict[str, Any], Set[str], str]] = []
    stats: Counter = Counter()

    for article in raw:
        info = article.get("article_info")
        if article.get("prompt_version") != prompt_version:
            stats["version"] += 1
            continue

        text_content = info.get(rule["text"]) if isinstance(info, dict) else None
        if (
            not isinstance(info, dict)
            or info.get("status") != "ok"
            or (viral and info.get("usable") is not True)
            or not isinstance(text_content, str)
            or not text_content.strip()
            or not isinstance(info.get(rule["images"], []), list)
            or isinstance(info.get(rule["score"]), bool)
            or not isinstance(info.get(rule["score"]), (int, float))
        ):
            stats["structure"] += 1
            continue

        comments_list = info.get("bait_comments", []) if viral else info.get("god_comments", [])
        comments_text = " ".join(str(c) for c in (comments_list or []) if c)
        if has_rival_keyword(platform, text_content + " " + comments_text):
            stats["rival_keyword"] += 1
            continue
        if has_rival_exchange_ui(platform, _extract_selected_image_exchange_uis(article)):
            stats["rival_image_ui"] += 1
            continue

        record = (article.get("publish_records") or {}).get(platform) or {}
        if record.get("attempts", 0) >= PUBLISH_MAX_ATTEMPTS:
            stats["attempts"] += 1
            continue

        # 跨平台流量分发规范：OKX 仅搬运 Binance 高流量爆款，避免发 OKX 原创同源帖
        if viral and platform == PLATFORM_OKX:
            article_source = str(article.get("source") or "").strip().lower()
            if article_source == OKX_SOURCE.lower():
                stats["source_conflict"] += 1
                continue

        if viral:
            tag_match = COIN_TAG_PATTERN.search(text_content)
            if tag_match:
                matched_coin = tag_match.group(0).lstrip("$").upper()
            else:
                matched_coin = next((coin for coin, pattern in patterns if pattern.search(text_content)), "")
        else:
            topic = (article.get("topic") or "").strip()
            matched_coin = topic if topic and topic.lower() in hot_set else None
            if not matched_coin:
                stats["no_hot_coin"] += 1
                continue

        image_keys = info.get(rule["images"]) or []
        mapping = info.get("image_mapping") or {}
        if not image_keys or any(
            not isinstance(mapping.get(key), dict) or not _is_valid_image_file(mapping[key].get("local_path"))
            for key in image_keys
        ):
            stats["invalid_image"] += 1
            continue

        addresses, _ = _extract_article_image_addresses(article)
        if addresses and addresses & used_addresses:
            stats["image_dup"] += 1
            continue

        candidates.append((article, addresses, matched_coin))

    if viral and candidates:
        eng_map: Dict[str, Dict[str, Any]] = {}
        src_pub_time_map: Dict[str, Any] = {}
        try:
            db_obj = article_manager.db if hasattr(article_manager, "db") else gen_db_object()
            post_mgr = UniversalPostManager(db_obj)
            raw_pids = []
            for art, _, _ in candidates:
                for pid in art.get("post_id_list") or []:
                    if pid is not None:
                        raw_pids.append(pid)
                        if str(pid).isdigit():
                            raw_pids.append(int(pid))
                            raw_pids.append(str(pid))
            if raw_pids:
                src_posts = post_mgr.find_posts(query={"post_id": {"$in": list(dict.fromkeys(raw_pids))}}, limit=0) or []
                for sp in src_posts:
                    spid = str(sp.get("post_id"))
                    eng_map[spid] = sp.get("engagement") or {}
                    src_pub_time_map[spid] = sp.get("publish_time")
        except Exception as exc:
            logger.warning(f"[高流量/选材] 批量查询原帖互动数据异常，降级按 viral_score 排序: {exc}")

        now_ts = time.time()

        def _viral_rank_key(item: Tuple[Dict[str, Any], Set[str], str]) -> float:
            art = item[0]
            info = art["article_info"]
            v_score = float(info.get(rule["score"], 0.0))
            pid_str = str((art.get("post_id_list") or [""])[0])
            eng = eng_map.get(pid_str) or {}
            art["_source_engagement"] = eng
            views = int(eng.get("view_count", 0) or 0)
            likes = int(eng.get("like_count", 0) or 0)
            comments = int(eng.get("comment_count", 0) or 0)
            hot_energy = views + likes * 30 + comments * 80
            # 【保留过滤/加权原因说明】发布层按目标平台分布规律与版式可读性打分（生成层保持平台中立）
            img_cnt = len(info.get(rule["images"]) or [])
            raw_text = str(info.get(rule["text"]) or "")
            p_score, p_breakdown = score_platform_candidate(platform, ARTICLE_TYPE_VIRAL, raw_text, img_cnt)
            art["_platform_score_meta"] = {"score": p_score, "breakdown": p_breakdown}
            return v_score + math.log10(max(hot_energy, 1000)) * 1.2 + p_score

        candidates.sort(key=_viral_rank_key, reverse=True)
    else:
        def _analysis_rank_key(item: Tuple[Dict[str, Any], Set[str], str]) -> float:
            art = item[0]
            info = art["article_info"]
            score = float(info.get(rule["score"], 0.0))
            img_cnt = len(info.get(rule["images"]) or [])
            raw_text = str(info.get(rule["text"]) or "")
            # 【保留过滤/加权原因说明】由平台适配器按目标平台决定形态、版式与篇幅偏置
            p_score, p_breakdown = score_platform_candidate(platform, ARTICLE_TYPE_ANALYSIS, raw_text, img_cnt)
            art["_platform_score_meta"] = {"score": p_score, "breakdown": p_breakdown}
            return score + p_score

        candidates.sort(key=_analysis_rank_key, reverse=True)
    return candidates, len(raw), stats


# ==============================================================================
# 总编横向擂台与发布回写
# ==============================================================================
def _check_account_health_guard(
    article_manager: Any,
    platform: str,
    account: str,
    *,
    lookback: int = 3,
) -> Tuple[bool, str]:
    """
    基于已同步落库的风控与语言指标检查账号健康度（零网络请求）：
    - Binance 中文发布号最新已同步帖子 `lan != "cn"` -> 立即拦截（语言池错配）；
    - Binance 最近 `lookback` 篇已同步帖中有 >=2 篇 `recommend is False` 或 `content_status == 5` -> 熔断暂停。
    """
    if platform != PLATFORM_BINANCE or not hasattr(article_manager, "find_articles"):
        return True, ""
    try:
        docs = (
            article_manager.find_articles(
                query={
                    "status": "ok",
                    f"publish_records.{platform}.status": "success",
                    f"publish_records.{platform}.published_by": account,
                    f"publish_records.{platform}.metrics_synced_at": {"$exists": True, "$ne": None},
                },
                sort=[(f"publish_records.{platform}.publish_time", -1)],
                limit=lookback,
            )
            or []
        )
    except Exception:
        return True, ""
    if not docs:
        return True, ""

    latest_rec = (docs[0].get("publish_records") or {}).get(platform) or {}
    latest_lan = latest_rec.get("lan")
    if isinstance(latest_lan, str) and latest_lan.strip() and latest_lan.strip().lower() != "cn":
        return False, f"账号最新帖子语言池异常(lan={latest_lan}!=cn)，请先核查账号语区设置"

    penalized = 0
    for doc in docs:
        rec = (doc.get("publish_records") or {}).get(platform) or {}
        if rec.get("recommend") is False or rec.get("content_status") == 5:
            penalized += 1
    if penalized >= 2:
        return False, f"账号近{len(docs)}篇已同步帖子中有{penalized}篇受限(recommend=False或content_status=5)，触发健康熔断"
    return True, ""


def _get_recent_published_history_with_metrics(
    article_manager: Any,
    platform: str,
    target_accounts: Optional[Sequence[str]] = None,
    limit: int = 50,
    article_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """拉取目标账号近期已发战报（支持按文章类型隔离），并按需增量同步免登阅读/互动数。"""
    query: Dict[str, Any] = {
        "status": "ok",
        f"publish_records.{platform}.status": "success",
    }
    if article_type:
        query["type"] = article_type
    if target_accounts:
        query[f"publish_records.{platform}.published_by"] = {"$in": list(target_accounts)}

    docs = (
        article_manager.find_articles(
            query=query,
            sort=[(f"publish_records.{platform}.publish_time", -1)],
            limit=limit,
        )
        or []
    )
    return sync_published_metrics(article_manager, platform, docs)


def _select_and_polish_best_candidate(
    article_manager: Any,
    platform: str,
    account: str,
    account_state: Dict[str, Any],
    candidates: Sequence[Tuple[Dict[str, Any], Set[str], str]],
    now: float,
    used_addresses: Set[str],
    article_type: str,
    *,
    state_store: Optional[AccountPublishStateStore] = None,
) -> Optional[int]:
    """总编 Top-50 横向擂台与终审润色（异常或返回无效时自动回退榜首候选）。"""
    valid_indices = _get_valid_publish_candidate_indices(
        account_state,
        candidates,
        now,
        used_addresses,
        article_type,
        limit=50,
        state_store=state_store,
        platform=platform,
        account=account,
    )
    if not valid_indices:
        return None

    rule = PUBLISH_RULES[article_type]
    viral = article_type == ARTICLE_TYPE_VIRAL

    target_accs = PLATFORM_ACCOUNTS.get(platform)
    historical_performance = _get_recent_published_history_with_metrics(
        article_manager,
        platform,
        target_accounts=target_accs,
        limit=25,
        article_type=article_type,
    )
    # 仅保留极简一行防重摘要（过滤浏览量/账号均值噪声，防止小号或未成熟数据误导选品）
    recent_topics = [
        f"{item.get('topic') or '热帖'} | {str(item.get('content') or '').strip()[:45]}"
        for item in historical_performance
        if str(item.get("content") or "").strip()
    ][:25]

    arena_items = []
    for rank_no, idx in enumerate(valid_indices, 1):
        art, _, m_coin = candidates[idx]
        info = art["article_info"]
        if viral:
            img_roles = [
                img.get("semantic_role", "")
                for img in (info.get("image_semantics") or [])
                if isinstance(img, dict) and img.get("keep")
            ]
            post_txt = info.get("post_text", "") or ""
            sel_imgs = info.get("selected_images") or []
            p_meta = art.get("_platform_score_meta")
            if not isinstance(p_meta, dict):
                ps, pbd = score_platform_candidate(platform, ARTICLE_TYPE_VIRAL, post_txt, len(sel_imgs))
                p_meta = {"score": ps, "breakdown": pbd}
                art["_platform_score_meta"] = p_meta
            # 严格剔除内部打分，仅附带目标平台客观适配度供总编统筹内容质量与渠道契合度
            arena_items.append(
                {
                    "candidate_no": rank_no,
                    "topic": art.get("topic") or m_coin,
                    "source_platform": art.get("source") or "",
                    "image_count": len(sel_imgs),
                    "char_count": len("".join(post_txt.split())),
                    "platform_fit_score": p_meta.get("score", 0.0),
                    "platform_fit_notes": (p_meta.get("breakdown") or {}).get("notes") or [],
                    "kept_images_desc": img_roles,
                    "draft_text": post_txt,
                    "draft_comments": info.get("bait_comments", []),
                }
            )
        else:
            brief_mats = (art.get("creation_brief") or {}).get("materials") or []
            selected_phs = list(info.get("image_placeholders") or [])
            selected_phs_set = set(selected_phs)
            img_descs = []
            for m in brief_mats:
                if not isinstance(m, dict):
                    continue
                vis = m.get("visual_evidence")
                vis_list = vis if isinstance(vis, list) else ([vis] if isinstance(vis, dict) else [])
                for v in vis_list:
                    if isinstance(v, dict) and v.get("placeholder") in selected_phs_set and v.get("what_it_shows"):
                        img_descs.append(v["what_it_shows"])
            draft_txt = info.get("text", "") or ""
            p_meta = art.get("_platform_score_meta")
            if not isinstance(p_meta, dict):
                ps, pbd = score_platform_candidate(platform, ARTICLE_TYPE_ANALYSIS, draft_txt, len(selected_phs))
                p_meta = {"score": ps, "breakdown": pbd}
                art["_platform_score_meta"] = p_meta
            # 严格剔除内部打分，仅保留客观盘面初稿、配图描述与目标平台适配分
            arena_items.append(
                {
                    "candidate_no": rank_no,
                    "topic": art.get("topic"),
                    "stance": art.get("stance"),
                    "image_count": len(selected_phs),
                    "char_count": len("".join(draft_txt.split())),
                    "platform_fit_score": p_meta.get("score", 0.0),
                    "platform_fit_notes": (p_meta.get("breakdown") or {}).get("notes") or [],
                    "kept_images_desc": img_descs,
                    "draft_text": draft_txt,
                    "draft_comments": info.get("god_comments", []),
                }
            )

    prompt_name = VIRAL_EDITOR_PROMPT_NAME if viral else ANALYSIS_EDITOR_PROMPT_NAME
    payload = {
        "platform": platform.upper(),
        "recent_topics": recent_topics,
        "candidates": arena_items,
    }

    try:
        base_prompt, prompt_meta = render_prompt_template(prompt_name)
        if not base_prompt.strip():
            raise ValueError(f"总编精选提示词为空或读取失败: {prompt_name}")
        t0 = time.monotonic()
        parsed, trace = call_llm_json(
            prompt=f"{base_prompt}\n{json.dumps(payload, ensure_ascii=False, indent=2)}",
            prompt_meta=prompt_meta,
            model=None,
            preset_model_group="medium",
            expected_type=dict,
            max_attempts=2,
            task_label=f"{rule['action']}/总编擂台",
            logger_obj=logger,
        )
        if not isinstance(parsed, dict):
            raise ValueError(trace.get("error") or "总编返回结果非字典")

        cand_no = int(parsed.get("selected_candidate_no", 1))
        if not (1 <= cand_no <= len(valid_indices)):
            cand_no = 1
        chosen_idx = valid_indices[cand_no - 1]
        chosen_art, _, chosen_coin = candidates[chosen_idx]

        # 经对照实验证实：盲眼总编只选不改可避免无图改写失真与幻觉，原汁原味保留上游看图初稿与神评
        chosen_p_meta = chosen_art.get("_platform_score_meta") or {}
        chosen_art["_selection_meta"] = {
            "prompt_version": SELECTION_PROMPT_VERSION,
            "reason": str(parsed.get("reason") or ""),
            "model_used": str(trace.get("model_used") or ""),
            "selected_candidate_no": cand_no,
            "candidate_pool_size": len(valid_indices),
            "platform_fit_score": chosen_p_meta.get("score"),
            "platform_fit_breakdown": chosen_p_meta.get("breakdown"),
            "selected_at": time.time(),
        }
        _log(
            f"{rule['action']}/总编擂台",
            "极简纯选优挑稿完成（只选不改）",
            平台=platform,
            账号=account,
            模型=trace.get("model_used") or "未返回",
            参选数=len(valid_indices),
            胜出序号=f"#{cand_no}",
            胜出主题=chosen_art.get("topic") or chosen_coin,
            理由=str(parsed.get("reason") or "")[:80],
            耗时=f"{time.monotonic() - t0:.2f}s",
        )
        return chosen_idx
    except Exception as exc:
        _log(
            f"{rule['action']}/总编擂台",
            "总编擂台调用异常，安全回退至榜首候选",
            level="warning",
            平台=platform,
            账号=account,
            原因=exc,
        )
        return valid_indices[0]


def _persist_publish_result(
    article_manager: Any,
    platform: str,
    account: str,
    account_state: Dict[str, Any],
    article: Dict[str, Any],
    plat_record: Dict[str, Any],
    success: bool,
    post_id: Optional[str],
    attempts: int,
    now: float,
    topic: str,
    latest_price: Optional[float],
    err_msg: Optional[str],
    article_type: str,
    matched_coin: Optional[str] = None,
    *,
    state_store: Optional[AccountPublishStateStore] = None,
) -> str:
    """持久化账号状态与数据库 `publish_records`。"""
    store = state_store or default_state_store
    viral = article_type == ARTICLE_TYPE_VIRAL
    error_detail = ""
    problems, saved = [], []

    try:
        if success:
            all_topics = article.get("coins") or ([topic] if topic else [])
            updated_acc_state = store.record_success(
                platform,
                account,
                is_viral=viral,
                topic=topic,
                topics=all_topics,
                now=now,
            )
            account_state.update(updated_acc_state)
            plat_record.update(
                status="success",
                published_by=account,
                publish_time=now,
                post_id=post_id,
                attempts=attempts,
                last_error=None,
                last_attempt_time=now,
                publish_price=latest_price,
            )
            plat_record.setdefault("interacted", False)
            plat_record.setdefault("interaction_time", None)
            plat_record.setdefault("comment_interact_info", {})
            sel_meta = article.get("_selection_meta") or {}
            if sel_meta.get("prompt_version"):
                plat_record["selection_prompt_version"] = sel_meta["prompt_version"]
            if sel_meta.get("reason"):
                plat_record["selection_reason"] = sel_meta["reason"]
            if sel_meta.get("model_used"):
                plat_record["selection_model_used"] = sel_meta["model_used"]
            if sel_meta.get("platform_fit_score") is not None:
                plat_record["platform_fit_score"] = sel_meta["platform_fit_score"]
            if sel_meta.get("platform_fit_breakdown"):
                plat_record["platform_fit_breakdown"] = sel_meta["platform_fit_breakdown"]
        else:
            infra_kind = classify_infra_backoff_error(err_msg)
            error_detail = f"发布接口未确认成功；可能是会话失效、网络异常、限流或内容校验失败；详情: {err_msg}"
            store.record_failure(
                platform,
                account,
                str(err_msg or ""),
                is_viral=viral,
                now=now,
                wrapped_detail=error_detail,
            )
            account_state.update(store.get_account_state(platform, account))
            if infra_kind:
                # 账号级 WAF / 登录失效不消耗目标文章的尝试次数，也不将文章误标为 failed
                return error_detail
            plat_record.update(
                status="failed",
                last_error=error_detail,
                attempts=attempts,
                last_attempt_time=now,
                publish_price=latest_price,
            )
        saved.append("账号状态")
    except Exception as exc:
        problems.append(("写入账号状态", exc))

    if viral:
        plat_record["matched_coin"] = matched_coin

    try:
        if not viral and article.get("_id") is not None:
            latest = article_manager.find_articles(query={"_id": article["_id"]}, limit=1) or []
            if latest and isinstance(latest[0].get("publish_records"), dict):
                article["publish_records"] = dict(latest[0]["publish_records"])
        if not isinstance(article.get("publish_records"), dict):
            article["publish_records"] = {}
        article["publish_records"][platform] = plat_record

        article_id = article.get("article_id")
        if article_id and hasattr(article_manager, "db") and hasattr(article_manager.db, "get_collection"):
            col = article_manager.db.get_collection(article_manager.collection_name)
            col.update_one(
                {"article_id": article_id},
                {"$set": {f"publish_records.{platform}": plat_record, "updated_at": datetime.now(timezone.utc)}},
            )
        else:
            article_manager.upsert_articles([article])
        saved.append("数据库状态")
    except Exception as exc:
        problems.append(("回写数据库", exc))

    if problems:
        details = "; ".join(f"{label}: {type(exc).__name__}: {exc}" for label, exc in problems)
        raise RuntimeError(f"{details} | 已保存: [{', '.join(saved) or '无'}]") from problems[0][1]
    return error_detail


def _publish_articles_for_platform(
    article_manager: Any,
    platform: str,
    hot_coins: Optional[Sequence[Any]],
    article_type: str,
    *,
    state_store: Optional[AccountPublishStateStore] = None,
) -> None:
    """单平台发布调度：优先零成本预检可用账号，无可用账号时立即跳过一切 DB/LLM 开销。"""
    accounts = PLATFORM_ACCOUNTS.get(platform) or []
    if not accounts:
        return

    store = state_store or default_state_store
    rule, now = PUBLISH_RULES[article_type], time.time()
    viral = article_type == ARTICLE_TYPE_VIRAL

    # 第一道成本防线：全部账号均在正常发帖间隔冷却窗口时，零 DB 查询、零总编 LLM 调用直接跳过
    eligible_accounts, blocked_reasons = store.get_eligible_accounts(
        platform,
        accounts,
        is_viral=viral,
        now=now,
    )
    for blocked_acc, reason in blocked_reasons.items():
        _log(f"{rule['action']}/账号", "账号暂不可发布", 平台=platform, 账号=blocked_acc, 状态=reason)

    if not eligible_accounts:
        _log(
            f"{rule['action']}/跳过",
            "当前平台无可用发布账号，跳过本轮查库、指标同步与总编选稿",
            平台=platform,
            账号状态=blocked_reasons,
        )
        return

    resolved_hot_coins = (
        list(hot_coins)
        if hot_coins is not None
        else (PLATFORMS[platform].hot_coins(for_publish=True) or ["BTC", "ETH", "BNB", "SOL"])
    )

    dedup_hours = max(rule["hours"], IMAGE_DEDUP_LOOKBACK_HOURS)
    used_addresses, used_primary, recent_count = _get_recent_published_image_usage(
        article_manager,
        platform,
        hours=dedup_hours,
        now=now,
    )
    candidates, raw_count, stats = _collect_publish_candidates(
        article_manager,
        platform,
        resolved_hot_coins,
        used_addresses,
        article_type,
    )

    filter_labels = (
        ("版本不符", stats["version"]),
        ("结构无效", stats["structure"]),
        ("尝试超限", stats["attempts"]),
        ("同源冲突", stats["source_conflict"]),
        ("含竞品词", stats["rival_keyword"]),
        ("竞品图界面", stats["rival_image_ui"]),
        ("非热币", stats["no_hot_coin"]),
        ("图片缺失", stats["invalid_image"]),
        ("图片重复", stats["image_dup"]),
    )
    filter_summary = "/".join(f"{name}:{cnt}" for name, cnt in filter_labels if cnt > 0) or "无"
    _log(
        f"{rule['action']}/计划",
        "候选已按评分降序排列",
        平台=platform,
        窗口=f"{rule['hours']}h",
        图片去重窗口=f"{dedup_hours}h",
        查询=raw_count,
        候选=len(candidates),
        过滤原因=filter_summary,
        近7d已发=recent_count,
        占用图片=len(set(used_primary)),
        可用账号=len(eligible_accounts),
        总账号=len(accounts),
    )

    platform_state = store.load_state().get(platform, {})
    for account in eligible_accounts:
        if not candidates:
            break
        healthy, health_reason = _check_account_health_guard(article_manager, platform, account)
        if not healthy:
            _log(
                f"{rule['action']}/账号熔断",
                "账号健康检查未通过，自动暂停本轮发布",
                level="error",
                平台=platform,
                账号=account,
                原因=health_reason,
                排查="核对账号主页语言设置、最近帖子推荐状态与简介合规性",
            )
            continue
        claimed, claim_reason = store.try_claim_account(
            platform,
            account,
            is_viral=viral,
            now=time.time(),
        )
        if not claimed:
            _log(f"{rule['action']}/账号", "账号并发或全局冷却限制，已跳过", 平台=platform, 账号=account, 状态=claim_reason)
            continue

        try:
            account_state = platform_state.get(account) or store.get_account_state(platform, account)
            cur_now = time.time()
            index = _select_and_polish_best_candidate(
                article_manager,
                platform,
                account,
                account_state,
                candidates,
                cur_now,
                used_addresses,
                article_type,
                state_store=store,
            )
            if index is None:
                _log(f"{rule['action']}/账号", "候选受主题冷却或已发图片冲突限制", 平台=platform, 账号=account)
                continue

            article, addresses, matched_coin = candidates[index]
            info = article["article_info"]
            topic = (article.get("topic") or info.get("topic_tag") or "") if viral else (article.get("topic") or "").strip()
            stance = article.get("stance", "")
            record = dict((article.get("publish_records") or {}).get(platform) or {})
            started = time.monotonic()
            stage, api_result = "准备正文、媒体及配置", "尚未调用"
            try:
                if viral:
                    text, paths = _format_viral_post_content(info, matched_coin)
                else:
                    text, paths = _format_analysis_post_content(article, platform=platform)

                session_dir = _get_account_session_dir(platform, account)
                attempts = record.get("attempts", 0) + 1
                stage, api_result = "调用发布接口", "未知"
                chart_coin = None if viral else (article.get("primary_coin") or topic)
                sentiment_tag = None if viral else stance
                try:
                    err, success, post_id = _execute_platform_post(
                        platform,
                        session_dir,
                        text,
                        paths,
                        topic=chart_coin,
                        stance=sentiment_tag,
                    )
                except ChallengeRequiredError as waf_exc:
                    err, success, post_id = f"[WAF/人机验证] {waf_exc}", False, None

                post_id_str = str(post_id)
                api_result = "成功" if success else "失败"
                latest_price = None if viral else _safe_get_latest_price(platform, topic)
                if success:
                    used_addresses.update(addresses)
                stage = "保存已确认的发布结果"
                pub_now = time.time()
                error = _persist_publish_result(
                    article_manager,
                    platform,
                    account,
                    account_state,
                    article,
                    record,
                    success,
                    post_id_str,
                    attempts,
                    pub_now,
                    topic,
                    latest_price,
                    err,
                    article_type,
                    matched_coin,
                    state_store=store,
                )
            except Exception as exc:
                raise RuntimeError(
                    f"{rule['action']}阶段[{stage}]失败 | 平台: [{platform}] | 账号: [{account}] "
                    f"| 文章: [{article.get('_id')}] | 主题: [{topic}] | 接口结果: [{api_result}] "
                    f"| 请核对远端结果与本地状态 | 原因: [{exc}]"
                ) from exc
            if success:
                candidates.pop(index)
            infra_backoff_kind = None if success else classify_infra_backoff_error(err)
            logged_attempts = record.get("attempts", 0) if infra_backoff_kind else attempts
            _log(
                f"{rule['action']}/文章",
                "本次处理结束",
                level="info" if success else "error",
                平台=platform,
                账号=account,
                文章=article.get("_id"),
                主题=topic,
                热币=matched_coin,
                评分=info.get(rule["score"]),
                尝试=f"{logged_attempts}/{PUBLISH_MAX_ATTEMPTS}",
                耗时=f"{time.monotonic() - started:.2f}秒",
                结果=api_result,
                说明="远端发布与本地回写完成" if success else error,
            )
        finally:
            store.release_account(platform, account)


def _publish_analysis_articles_for_platform(article_manager: Any, platform: str, hot_coins: Optional[Sequence[Any]]) -> None:
    _publish_articles_for_platform(article_manager, platform, hot_coins, ARTICLE_TYPE_ANALYSIS)


def _publish_viral_articles_for_platform(article_manager: Any, platform: str, hot_coins: Optional[Sequence[Any]]) -> None:
    _publish_articles_for_platform(article_manager, platform, hot_coins, ARTICLE_TYPE_VIRAL)


def _publish_on_platforms(
    article_manager: Any,
    publish_one_platform: Callable[[Any, str, Optional[Sequence[Any]]], None],
    action: str,
) -> None:
    for platform in PLATFORMS:
        if not (PLATFORM_ACCOUNTS.get(platform) or []):
            continue
        try:
            publish_one_platform(article_manager, platform, None)
        except Exception:
            _log(
                f"{action}/平台隔离",
                "当前平台本轮发布异常，继续其他平台",
                level="exception",
                平台=platform,
                排查="检查会话、网络及数据库；先核对远端是否已经发布",
            )


def publish_analysis_once(article_manager: Optional[Any] = None) -> None:
    mgr = article_manager or GeneratedArticleManager(gen_db_object())
    _publish_on_platforms(mgr, _publish_analysis_articles_for_platform, "单币分析发布")


def publish_viral_once(article_manager: Optional[Any] = None) -> None:
    mgr = article_manager or GeneratedArticleManager(gen_db_object())
    _publish_on_platforms(mgr, _publish_viral_articles_for_platform, "流量发布")


def auto_publish_articles(stop_event: Optional[threading.Event] = None, max_rounds: int = 0) -> None:
    """单币分析文章自动发布守护循环。"""
    mgr = GeneratedArticleManager(gen_db_object())
    run_daemon_loop(
        lambda _round_idx: publish_analysis_once(mgr),
        interval_seconds=PUBLISH_POLL_SECONDS,
        jitter_ratio=0.1,
        error_backoff_seconds=PUBLISH_POLL_SECONDS,
        name="单币分析发布",
        logger_obj=logger,
        max_rounds=max_rounds,
        stop_event=stop_event,
    )


def auto_publish_viral_articles(stop_event: Optional[threading.Event] = None, max_rounds: int = 0) -> None:
    """娱乐高流量文章自动发布守护循环。"""
    mgr = GeneratedArticleManager(gen_db_object())
    run_daemon_loop(
        lambda _round_idx: publish_viral_once(mgr),
        interval_seconds=PUBLISH_POLL_SECONDS,
        jitter_ratio=0.1,
        error_backoff_seconds=PUBLISH_POLL_SECONDS,
        name="流量发布",
        logger_obj=logger,
        max_rounds=max_rounds,
        stop_event=stop_event,
    )


__all__ = [
    "_check_account_health_guard",
    "_collect_publish_candidates",
    "_extract_article_image_addresses",
    "_get_recent_published_history_with_metrics",
    "_get_recent_published_image_usage",
    "_get_valid_publish_candidate_indices",
    "_persist_publish_result",
    "_publish_analysis_articles_for_platform",
    "_publish_articles_for_platform",
    "_publish_on_platforms",
    "_publish_viral_articles_for_platform",
    "_select_and_polish_best_candidate",
    "_select_publish_candidate",
    "auto_publish_articles",
    "auto_publish_viral_articles",
    "publish_analysis_once",
    "publish_viral_once",
]
