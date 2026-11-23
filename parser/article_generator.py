# -*- coding: utf-8 -*-
"""
内容筛选与文章生成引擎 (`parser/article_generator.py`)

统一收口三条核心生产链路：
1. 娱乐高流量一阶段纯文本批量粗筛 (`screen_viral_posts`, `batch_screen_viral_posts`, `low` 模型组)；
2. 论据选材与单币分析文章生成 (`generate_analysis_articles`, `build_article_task`, `execute_article_task`, `medium` 模型组)；
3. 娱乐高流量二阶段多模态洗稿与神评生成 (`generate_viral_articles`, `build_viral_article_task`, `execute_viral_article_task`, `medium` 模型组)。

所有模型调用严格经由 `common.llm_gateway.call_llm_json(..., model=None, preset_model_group=...)` 发起，
并将真实返回的 `trace["model_used"]` 落库至 `record["model_name"]`（零硬编码模型名）。
"""
import json
import random
import threading
import time
from collections import Counter, defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from common.common_utils import (
    PLATFORM_BINANCE,
    PLATFORM_OKX,
    POST_SOURCES,
    get_image_content_id,
)
from common.daemon import run_daemon_loop
from common.llm_gateway import call_llm_json, render_prompt_template
from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import GeneratedArticleManager, UniversalPostManager
from parser.config import ANALYSIS_GENERATION_INTERVAL_SECONDS
from parser.mlu_pipeline import _resolve_prompt_pair, run_concurrent_screening_batches
from parser.platform_adapters import BEIJING_TZ, PLATFORMS, _log, get_hot_coin, logger, normalize_exchange_ui
from parser.post_ingestion import (
    ARTICLE_CONCURRENCY,
    ARTICLE_GENERATION_INTERVAL_SECONDS,
    ARTICLE_MATERIAL_LIMIT,
    ARTICLE_MAX_CHARS,
    ARTICLE_MIN_MATERIALS,
    ARTICLE_POST_USAGE_LIMIT,
    ARTICLE_PROMPT_FILE_PATH,
    ARTICLE_PROMPT_NAME,
    ARTICLE_TYPE_ANALYSIS,
    ARTICLE_TYPE_VIRAL,
    ARTICLE_VARIANTS,
    BINANCE_SOURCE,
    LLM_MAX_RETRIES,
    MEDIA_PATTERN,
    POST_QUERY_LIMIT,
    SHELF_LIFE_SECONDS,
    VIRAL_MIN_VIEW_COUNTS,
    VIRAL_POST_MAX_AGE_HOURS,
    VIRAL_STAGE1_BATCH_SIZE,
    VIRAL_STAGE1_PROMPT_NAME,
    VIRAL_STAGE2_PROMPT_FILE_PATH,
    VIRAL_STAGE2_PROMPT_NAME,
    _find_posts_by_sources,
    _is_self_post,
    _is_valid_image_file,
    _is_valid_raw_post,
    _key_error,
    _publish_time_seconds,
    build_source_image_mapping,
    ensure_post_media_downloaded,
    normalize_post_media,
    prompt_version,
)


# ==============================================================================
# 一、娱乐高流量第一阶段：纯文本批量粗筛
# ==============================================================================
def is_need_viral_screening(post: Dict[str, Any]) -> bool:
    """判定原帖是否符合 24h 内高流量图文帖一阶段粗筛条件。"""
    screen = post.get("viral_screen")
    if (
        isinstance(screen, dict)
        and screen.get("prompt_version") == prompt_version
        and isinstance(screen.get("keep"), bool)
    ):
        return False
    engagement = post.get("engagement") or {}
    view_count = engagement.get("view_count", 0)
    min_view_count = VIRAL_MIN_VIEW_COUNTS.get(post.get("source"), 1000)
    if not isinstance(view_count, (int, float)) or isinstance(view_count, bool) or view_count <= min_view_count:
        return False
    try:
        publish_time = _publish_time_seconds(post.get("publish_time", 0))
    except ValueError:
        return False
    age_hours = (time.time() - publish_time) / 3600
    if age_hours > VIRAL_POST_MAX_AGE_HOURS or age_hours < -1:
        return False
    text = (post.get("content") or {}).get("text_content") or ""
    if any(match.group(1) == "视频" for match in MEDIA_PATTERN.finditer(text)):
        return False
    return _is_valid_raw_post(post, require_local_media=False)


def check_viral_stage1_info(json_data: Any, expected_post_ids: Iterable[Any]) -> Tuple[bool, str]:
    """校验一阶段粗筛输出 `[{post_id, keep, event_key}]`。"""
    if not isinstance(json_data, list):
        return False, "高流量一阶段粗筛返回最外层必须是列表(JSON Array)"
    expected = {str(pid).strip() for pid in expected_post_ids}
    seen = set()
    for index, item in enumerate(json_data, 1):
        label = f"viral_stage1[{index}]"
        error = _key_error(item, {"post_id", "keep", "event_key"}, label)
        if error:
            return False, error
        pid = str(item.get("post_id", "")).strip()
        if not pid:
            return False, f"{label}.post_id 不能为空"
        if not isinstance(item["keep"], bool):
            return False, f"{label}.keep 必须是布尔值"
        if not isinstance(item["event_key"], str):
            return False, f"{label}.event_key 必须是字符串"
        seen.add(pid)
    missing = expected - seen
    if missing:
        return False, f"高流量一阶段粗筛返回缺少 post_id: {', '.join(sorted(missing)[:10])}"
    return True, ""


def gen_viral_stage1_info(
    batch_posts: Sequence[Dict[str, Any]],
    prompt_template: Optional[str] = None,
    prompt_meta: Optional[Dict[str, str]] = None,
) -> Dict[str, Dict[str, Any]]:
    """调用 `low` 组大模型执行高流量一阶段纯文本粗筛（不传图）。"""
    base_prompt, p_meta = _resolve_prompt_pair(prompt_template, VIRAL_STAGE1_PROMPT_NAME, prompt_meta)
    items = []
    for post in batch_posts:
        normalized_text, _, placeholders = normalize_post_media(post)
        items.append(
            {
                "post_id": str(post.get("post_id", "")).strip(),
                "image_count": len(placeholders),
                "text": normalized_text.strip(),
            }
        )
    ids = [item["post_id"] for item in items]
    data, trace = call_llm_json(
        prompt=f"{base_prompt}\n{json.dumps(items, ensure_ascii=False)}",
        prompt_meta=p_meta,
        model=None,
        preset_model_group="low",
        expected_type=list,
        validator=lambda response: check_viral_stage1_info(response, ids),
        max_attempts=LLM_MAX_RETRIES,
        task_label="高流量/批量粗筛",
        logger_obj=logger,
    )
    if not isinstance(data, list):
        logger.warning(
            f"[高流量/批量粗筛] 模型返回无效或校验失败 | 首帖: [{ids[0] if ids else 'NONE'}] "
            f"| 原因: [{trace.get('error') or trace.get('call_status')}]"
        )
        return {}
    return {
        str(item["post_id"]).strip(): {
            "keep": item["keep"],
            "event_key": item["event_key"].strip(),
        }
        for item in data
        if isinstance(item, dict) and "post_id" in item
    }


def process_and_save_single_viral_post(
    batch_posts: Sequence[Dict[str, Any]],
    post_manager: Any,
    prompt_template: Optional[str] = None,
    prompt_meta: Optional[Dict[str, str]] = None,
) -> Tuple[int, int]:
    """执行单批高流量一阶段粗筛并回写 `viral_screen`，返回 `(保留数, 淘汰数)`。"""
    started = time.monotonic()
    screen_map = gen_viral_stage1_info(batch_posts, prompt_template, prompt_meta=prompt_meta)
    if not screen_map:
        return 0, 0
    updates, kept_count = [], 0
    now = time.time()
    for post in batch_posts:
        pid = str(post.get("post_id", "")).strip()
        result = screen_map.get(pid)
        if not result:
            continue
        post["viral_screen"] = {
            "keep": result["keep"],
            "event_key": result["event_key"],
            "prompt_version": prompt_version,
            "screen_time": now,
        }
        updates.append(post)
        kept_count += int(bool(result["keep"]))
    if updates:
        post_manager.upsert_posts(updates)
    rejected_count = len(updates) - kept_count
    logger.info(
        f"[高流量/粗筛回写] 批次已保存 | 处理: [{len(updates)}] | 放行(keep=True): [{kept_count}] "
        f"| 淘汰: [{rejected_count}] | 耗时: [{time.monotonic() - started:.2f} 秒]"
    )
    return kept_count, rejected_count


def batch_screen_viral_posts(posts: Sequence[Dict[str, Any]], post_manager: Any) -> Tuple[int, int, int, int]:
    """每批 50 帖并发执行高流量一阶段纯文本粗筛。"""
    return run_concurrent_screening_batches(
        posts,
        post_manager,
        is_need_viral_screening,
        process_and_save_single_viral_post,
        VIRAL_STAGE1_PROMPT_NAME,
        VIRAL_STAGE1_BATCH_SIZE,
        "高流量粗筛",
    )


def screen_viral_posts_once(post_manager: Optional[Any] = None) -> Tuple[int, int, int, int]:
    """单轮娱乐帖高流量一阶段粗筛任务，返回 `(待筛数, 通过数, 淘汰数, 建议休眠秒)`。"""
    started = time.monotonic()
    if post_manager is None:
        post_manager = UniversalPostManager(gen_db_object())
    lookback_hours = VIRAL_POST_MAX_AGE_HOURS + 2
    min_publish_ts = time.time() - lookback_hours * 3600
    source_batches = [
        post_manager.find_posts_by_source(
            source,
            limit=POST_QUERY_LIMIT,
            extra_query={"publish_time": {"$gte": min_publish_ts}},
        )
        or []
        for source in POST_SOURCES
    ]
    posts = [post for batch in source_batches for post in batch]
    if not posts:
        logger.info(f"[高流量粗筛/轮询] 库内暂无近{lookback_hours}h原帖 | 下次扫描: [60s后]")
        return 0, 0, 0, 60
    viral_pending, viral_pass, viral_rej, _ = batch_screen_viral_posts(posts, post_manager) or (0, 0, 0, 0)
    delay = 5 if any(len(batch) >= POST_QUERY_LIMIT for batch in source_batches) else 60
    if viral_pending == 0:
        logger.info(
            f"[高流量粗筛/轮询] 本轮无新增待粗筛娱乐原帖 | 库内近{lookback_hours}h原帖: [{len(posts)}] "
            f"| 耗时: [{time.monotonic() - started:.2f}s] | 下次扫描: [{delay}s后]"
        )
    else:
        logger.info(
            f"[高流量粗筛/汇总] 本轮娱乐帖粗筛完成 | 扫描原帖: [{len(posts)}] | 待筛: [{viral_pending}] "
            f"| 放行(keep=True): [{viral_pass}] | 淘汰: [{viral_rej}] | 耗时: [{time.monotonic() - started:.2f}s] "
            f"| 下次扫描: [{delay}s后]"
        )
    return viral_pending, viral_pass, viral_rej, delay


def screen_viral_posts(stop_event: Optional[threading.Event] = None, max_rounds: int = 0) -> None:
    """独立娱乐帖高流量一阶段粗筛守护循环。"""

    def _step(_round_idx: int) -> Dict[str, Any]:
        _, _, _, delay = screen_viral_posts_once()
        return {"next_sleep_seconds": delay}

    run_daemon_loop(
        _step,
        interval_seconds=60,
        error_backoff_seconds=60,
        name="高流量粗筛/轮询",
        logger_obj=logger,
        max_rounds=max_rounds,
        stop_event=stop_event,
    )


# ==============================================================================
# 二、公共选材与落库记录整理
# ==============================================================================
def _get_all_self_published_post_ids(article_manager: Optional[Any] = None) -> set:
    """查询库中全部自营矩阵已成功发布的 `post_id` 集合，防止回流自引。"""
    if article_manager is None:
        article_manager = GeneratedArticleManager(gen_db_object())
    articles = (
        article_manager.find_articles(
            query={
                "$or": [
                    {f"publish_records.{PLATFORM_BINANCE}.post_id": {"$exists": True, "$ne": None}},
                    {f"publish_records.{PLATFORM_OKX}.post_id": {"$exists": True, "$ne": None}},
                ]
            },
            limit=0,
        )
        or []
    )
    self_ids = set()
    for art in articles:
        pub = art.get("publish_records") or {}
        for plat in (PLATFORM_BINANCE, PLATFORM_OKX):
            pid = (pub.get(plat) or {}).get("post_id")
            if pid and str(pid).strip() and str(pid).strip() != "None":
                self_ids.add(str(pid).strip())
    return self_ids


def transform_mlus(mlu_list: Sequence[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """将论据列表转为带 dHash 去重占位符 `[ASSET_IMG_n]` 的创作素材列表与图片溯源映射。"""
    materials, image_mapping, asset_by_source = [], {}, {}
    for index, evidence in enumerate(mlu_list, 1):
        coin = str(evidence.get("target_coin") or "").strip().upper()
        material = {
            "id": f"M{index}",
            "coin": coin,
            "target_coin": coin,
            "fact": evidence.get("core_fact", ""),
            "underlying_logic": evidence.get("logic_link", ""),
            "raw_hook": evidence.get("raw_hook", ""),
            "visual_evidence": None,
        }
        materials.append(material)
        images = [image for image in evidence.get("images", []) if image.get("usable") is True]
        if not images:
            continue
        source_mapping = evidence.get("source_image_mapping")
        if source_mapping is None:
            source_mapping = build_source_image_mapping(
                {
                    "content": {"text_content": evidence.get("source_text_content", "")},
                    "media": {"local_mapping": evidence.get("source_local_mapping") or {}},
                }
            )
        if not all(
            image.get("image_id") in source_mapping
            and isinstance(image.get("context"), str)
            and image["context"].strip()
            for image in images
        ):
            logger.warning(
                f"[文章/配图] 当前论据保留文字、取消整组配图 | 帖子: [{evidence.get('source_post_id')}] "
                "| 原因: [至少一图缺少来源或描述] | 排查: [核对原文编号、映射与context]"
            )
            continue
        visual_items, seen_ids = [], set()
        ev_source = evidence.get("source") or BINANCE_SOURCE
        for image in images:
            image_id = image["image_id"]
            if image_id in seen_ids:
                continue
            seen_ids.add(image_id)
            meta = source_mapping[image_id]
            local_path = meta.get("local_path") or ""
            content_id = get_image_content_id(local_path)
            source_key = ("content_id", content_id) if content_id else (ev_source, evidence.get("source_post_id"), image_id)
            ex_ui = normalize_exchange_ui(image.get("exchange_ui"))
            placeholder = asset_by_source.get(source_key)
            if placeholder is None:
                placeholder = f"[ASSET_IMG_{len(asset_by_source) + 1}]"
                asset_by_source[source_key] = placeholder
                image_mapping[placeholder] = {
                    "source": ev_source,
                    "source_post_id": evidence.get("source_post_id"),
                    "original_image_id": image_id,
                    "original_image_id_list": [image_id],
                    "content_id": content_id,
                    "exchange_ui": ex_ui,
                    **meta,
                }
            elif ex_ui and not image_mapping[placeholder].get("exchange_ui"):
                image_mapping[placeholder]["exchange_ui"] = ex_ui
            visual_items.append({"placeholder": placeholder, "what_it_shows": image["context"]})
        material["visual_evidence"] = visual_items[0] if len(visual_items) == 1 else visual_items
    return materials, image_mapping


def extract_and_group_valid_evidences(article_manager: Optional[Any] = None) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """提取近 15 天有效 `logic_mul.evidences` 并按热币/立场分组、按 `impact_weight` 降序排序。"""
    binance_hot_coins = {coin.upper() for coin in get_hot_coin()}
    okx_hot_coins = {coin.upper() for coin in (PLATFORMS[PLATFORM_OKX].hot_coins() or [])}
    hot_coins = binance_hot_coins.union(okx_hot_coins)
    started = time.monotonic()
    manager = UniversalPostManager(gen_db_object())
    now = time.time()
    posts = _find_posts_by_sources(
        manager,
        limit=POST_QUERY_LIMIT,
        extra_query={"publish_time": {"$gte": now - 16 * 24 * 3600}},
    )
    self_post_ids = _get_all_self_published_post_ids(article_manager)
    grouped: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    invalid_times, self_skipped = 0, 0
    for post in posts:
        if _is_self_post(post, self_post_ids):
            self_skipped += 1
            continue
        logic, raw_time = post.get("logic_mul"), post.get("publish_time")
        if not isinstance(logic, dict) or logic.get("prompt_version") != prompt_version or not logic.get("evidences") or not raw_time:
            continue
        try:
            publish_time = _publish_time_seconds(raw_time)
        except ValueError:
            invalid_times += 1
            continue
        age = now - publish_time
        if age > 15 * 24 * 3600 or age < -3600:
            continue
        source_mapping = build_source_image_mapping(post)
        post_source = post.get("source") or BINANCE_SOURCE
        for evidence in logic["evidences"]:
            if any(image.get("usable") is not True for image in evidence.get("images", [])):
                continue
            duration = SHELF_LIFE_SECONDS.get(evidence.get("shelf_life", "unknown"), 24 * 3600)
            target_coin = str(evidence.get("target_coin") or "").strip().upper()
            if not target_coin and isinstance(evidence.get("coins"), list):
                candidate_coins = [str(coin).strip().upper() for coin in evidence["coins"] if str(coin).strip()]
            else:
                candidate_coins = [target_coin] if target_coin else []
            stance = evidence.get("stance")
            if age > duration or not candidate_coins or stance not in ("看多", "看空"):
                continue
            enriched = {
                **evidence,
                "source": post_source,
                "source_post_id": post.get("post_id", "UNKNOWN"),
                "publish_time": publish_time,
                "source_text_content": (post.get("content") or {}).get("text_content", ""),
                "source_image_mapping": source_mapping,
            }
            for coin in candidate_coins:
                if coin and coin in hot_coins:
                    grouped[coin][stance].append(enriched)
    for stances in grouped.values():
        for evidences in stances.values():
            evidences.sort(key=lambda item: item.get("impact_weight", 0), reverse=True)
    result = json.loads(json.dumps(grouped))
    log = logger.warning if invalid_times else logger.info
    total_evidences = sum(len(items) for stances in result.values() for items in stances.values())
    log(
        f"[论据/筛选] 已分组并按权重排序 | 扫描: [{len(posts)}] | 币种: [{len(result)}] | 论据: [{total_evidences}，含跨组重复] "
        f"| 自家帖拦截: [{self_skipped}] | 无效时间跳过: [{invalid_times}] | 耗时: [{time.monotonic() - started:.2f} 秒]"
    )
    return result


def get_post_usage_counts(article_manager: Any, source: Any, post_ids: Sequence[Any]) -> Dict[Any, int]:
    """统计原帖被成功发布文章引用的次数（同文章同帖只计一次）。"""
    unique_ids = list(dict.fromkeys(post_ids))
    counts = dict.fromkeys(unique_ids, 0)
    if not unique_ids:
        return counts
    source_query = {"$in": list(source)} if isinstance(source, (list, tuple, set)) else source
    articles = article_manager.find_articles(
        query={
            "source": source_query,
            "status": "ok",
            "post_id_list": {"$in": unique_ids},
            "$or": [
                {f"publish_records.{PLATFORM_BINANCE}.status": "success"},
                {f"publish_records.{PLATFORM_OKX}.status": "success"},
            ],
        },
        limit=0,
    )
    for article in articles or []:
        for post_id in set(article.get("post_id_list", [])):
            if post_id in counts:
                counts[post_id] += 1
    return counts


def get_recent_openings(article_manager: Any, source: Any, topic: str, stance: str, limit: int = 5) -> List[str]:
    """获取近阶段同币同立场成功文章的首行文本（供去重参考）。"""
    if limit <= 0:
        return []
    articles = article_manager.find_articles(
        query={"source": source, "topic": topic, "stance": stance, "status": "ok"},
        sort=[("created_at", -1)],
        limit=limit,
    )
    openings = []
    for article in articles or []:
        text = (article.get("article_info") or {}).get("text", "")
        if isinstance(text, str) and text.strip():
            opening = text.strip().splitlines()[0][:80]
            if opening not in openings:
                openings.append(opening)
    return openings


def prepare_article_record_for_db(article_data: Dict[str, Any]) -> Dict[str, Any]:
    """校验并补全即将入库 `generated_articles` 的文档默认字段。"""
    record = dict(article_data)
    record.setdefault("type", ARTICLE_TYPE_ANALYSIS)
    for key in ("source", "topic", "stance"):
        if not isinstance(record.get(key), str) or not record[key].strip():
            raise ValueError(f"文章缺失有效字段: {key}")
    status = record.get("status")
    if status not in ("processing", "ok", "skip", "error"):
        raise ValueError(f"不支持的文章状态: {status}")
    post_ids = []
    if status == "ok":
        info, mapping = record.get("article_info"), record.get("material_post_mapping")
        if not isinstance(info, dict) or info.get("status") != "ok":
            raise ValueError("成功记录必须包含 status=ok 的 article_info")
        if not isinstance(mapping, dict):
            raise ValueError("成功记录必须包含 material_post_mapping")
        used_ids = info.get("used_material_ids")
        if not isinstance(used_ids, list) or not used_ids:
            raise ValueError("成功文章的 used_material_ids 不能为空")
        for material_id in used_ids:
            if not isinstance(material_id, str) or material_id not in mapping:
                raise ValueError(f"实际采用素材无法反查原帖: {material_id!r}")
            post_id = mapping[material_id]
            if not isinstance(post_id, (str, int)) or isinstance(post_id, bool) or post_id in (None, "", "UNKNOWN"):
                raise ValueError(f"素材对应的原帖 ID 无效: {material_id}")
            if post_id not in post_ids:
                post_ids.append(post_id)
    elif status == "skip" and record.get("type") == ARTICLE_TYPE_VIRAL:
        post_ids = [pid for pid in (record.get("post_id_list") or []) if pid not in (None, "", "UNKNOWN")]
    record["post_id_list"] = post_ids
    for key, default in {
        "article_info": None,
        "error_message": None,
        "error_history": [],
        "raw_response": None,
        "attempt_count": 0,
        "publish_records": {},
    }.items():
        record.setdefault(key, default)
    return record


# ==============================================================================
# 三、单币分析文章生成
# ==============================================================================
def check_article_info(
    article_info: Any,
    materials: Sequence[Dict[str, Any]],
    image_mapping: Dict[str, Any],
    max_chars: int,
) -> Tuple[bool, str]:
    """校验单币分析文章输出 `{status,text,god_comments,image_placeholders,used_material_ids,score,reason}`。"""
    import re

    keys = {"status", "text", "god_comments", "image_placeholders", "used_material_ids", "score", "reason"}
    error = _key_error(article_info, keys, "文章")
    if error:
        return False, error
    if article_info["status"] not in ("ok", "skip"):
        return False, "status 必须为 ok 或 skip"
    if not isinstance(article_info["text"], str):
        return False, "text 必须是字符串"
    for key in ("god_comments", "image_placeholders", "used_material_ids"):
        values = article_info[key]
        if not isinstance(values, list) or any(not isinstance(value, str) or not value for value in values):
            return False, f"{key} 必须是非空字符串组成的列表，列表本身可为空"
        if key != "god_comments" and len(values) != len(set(values)):
            return False, f"{key} 不允许重复"
    if article_info["status"] == "skip":
        if (
            article_info["text"] != ""
            or article_info["image_placeholders"]
            or article_info["used_material_ids"]
            or article_info["god_comments"]
            or article_info["score"] is not None
        ):
            return False, "skip 必须清空正文、图片、神评、采用素材，score 必须为 null"
        if not isinstance(article_info["reason"], str) or not article_info["reason"].strip():
            return False, "skip 必须说明 reason"
        return True, ""
    text = article_info["text"]
    if not text.strip():
        return False, "ok 正文不能为空"
    if len(re.sub(r"\s", "", text)) > max_chars:
        return False, f"正文非空白字符数超过 {max_chars}"
    if re.search(r"\[(?:ASSET_IMG|IMAGE|VIDEO)_\d+\]", text):
        return False, "正文不能包含图片或视频占位符"
    if type(article_info["score"]) not in (int, float) or not 0 <= article_info["score"] <= 10:
        return False, "score 必须是 0—10 的数值，不能是布尔值"
    if article_info["reason"] is not None:
        return False, "ok 的 reason 必须为 null"
    if not article_info["god_comments"]:
        return False, "ok 状态下 god_comments 必须至少包含 1 条神评"
    material_by_id = {item["id"]: item for item in materials}
    used_ids, selected_images = article_info["used_material_ids"], article_info["image_placeholders"]
    if not used_ids or any(item not in material_by_id for item in used_ids):
        return False, "used_material_ids 必须引用实际存在的素材且不能为空"
    if image_mapping and not selected_images:
        return False, "ok 状态下必须至少选择 1 张配图（image_placeholders 不能为空）"
    if len(selected_images) > 3:
        return False, "最多使用 3 张图片"
    allowed_images = set()
    for material_id in used_ids:
        visual = material_by_id[material_id]["visual_evidence"]
        if not visual:
            continue
        chain = visual if isinstance(visual, list) else [visual]
        chain_ids = [item["placeholder"] for item in chain]
        allowed_images.update(chain_ids)
        chosen = [item for item in selected_images if item in chain_ids]
        if chosen and chosen != chain_ids:
            return False, f"素材 {material_id} 的图片证据链必须整组使用并保留输入顺序"
    if any(item not in allowed_images or item not in image_mapping for item in selected_images):
        return False, "图片必须来自已采用素材，且存在真实来源映射"
    return True, ""


def build_article_task(
    coin: str,
    stance: str,
    ev_list: Sequence[Dict[str, Any]],
    article_manager: Any,
) -> Dict[str, Any]:
    """均衡选材（最多 10 条、最少 5 条）并构建分析文章生成任务（不预设硬编码模型名）。"""
    brief: Dict[str, Any] = {"task": {"topic": coin, "stance": stance, "max_chars": ARTICLE_MAX_CHARS}, "materials": []}
    record: Dict[str, Any] = {
        "source": BINANCE_SOURCE,
        "type": ARTICLE_TYPE_ANALYSIS,
        "topic": coin,
        "stance": stance,
        "status": "processing",
        "creation_brief": brief,
        "material_post_mapping": {},
        "post_id_list": [],
        "prompt_file_path": ARTICLE_PROMPT_FILE_PATH,
        "model_name": None,
        "prompt_version": prompt_version,
        "article_info": None,
        "error_message": None,
        "error_history": [],
        "raw_response": None,
        "attempt_count": 0,
    }
    candidates = [
        item
        for item in ev_list
        if isinstance(item, dict)
        and isinstance(item.get("source_post_id"), (str, int))
        and not isinstance(item.get("source_post_id"), bool)
        and item["source_post_id"] not in ("", "UNKNOWN")
        and isinstance(item.get("core_fact"), str)
        and item["core_fact"].strip()
    ]
    usage = get_post_usage_counts(
        article_manager,
        POST_SOURCES,
        list(dict.fromkeys(item["source_post_id"] for item in candidates)),
    )
    groups = defaultdict(list)
    for item in candidates:
        if usage.get(item["source_post_id"], 0) < ARTICLE_POST_USAGE_LIMIT:
            groups[(item.get("dimension"), item.get("shelf_life"))].append(item)

    def _has_usable_img(ev: Dict[str, Any]) -> bool:
        return any(isinstance(img, dict) and img.get("usable") for img in (ev.get("images") or []))

    grouped_queues = {
        key: deque(
            sorted(
                items,
                key=lambda item: (1 if _has_usable_img(item) else 0, item.get("impact_weight", 0), random.random()),
                reverse=True,
            )
        )
        for key, items in groups.items()
    }
    selected: List[Dict[str, Any]] = []
    while grouped_queues and len(selected) < ARTICLE_MATERIAL_LIMIT:
        keys = sorted(
            grouped_queues,
            key=lambda key: (
                1 if _has_usable_img(grouped_queues[key][0]) else 0,
                grouped_queues[key][0].get("impact_weight", 0),
                random.random(),
            ),
            reverse=True,
        )
        for key in keys:
            if len(selected) >= ARTICLE_MATERIAL_LIMIT:
                break
            selected.append(grouped_queues[key].popleft())
            if not grouped_queues[key]:
                del grouped_queues[key]

    if len(selected) < ARTICLE_MIN_MATERIALS:
        reason = (
            f"可用论据不足 {ARTICLE_MIN_MATERIALS} 条（当前 {len(selected)} 条）；"
            f"需检查论据有效性及原帖已发布引用次数是否小于 {ARTICLE_POST_USAGE_LIMIT}。"
        )
        record["status"] = "skip"
        record["article_info"] = {
            "status": "skip",
            "text": "",
            "god_comments": [],
            "image_placeholders": [],
            "used_material_ids": [],
            "score": None,
            "reason": reason,
            "image_mapping": {},
        }
        return {
            "is_skip": True,
            "record": record,
            "coin": coin,
            "stance": stance,
            "reason": reason,
            "selected_count": len(selected),
        }

    materials, image_mapping = transform_mlus(selected)
    file_paths, uploaded_images_desc = [], []
    for ph, meta in image_mapping.items():
        lp = meta.get("local_path") if isinstance(meta, dict) else None
        if lp and _is_valid_image_file(lp):
            file_paths.append(lp)
            uploaded_images_desc.append(f"上传的第 {len(file_paths)} 张图片对应占位符 {ph}")
            if len(file_paths) >= 4:
                break

    if not file_paths:
        reason = "选中论据中无有效本地实锤配图，跳过无图纯文字生成"
        record["status"] = "skip"
        record["article_info"] = {
            "status": "skip",
            "text": "",
            "god_comments": [],
            "image_placeholders": [],
            "used_material_ids": [],
            "score": None,
            "reason": reason,
            "image_mapping": {},
        }
        return {
            "is_skip": True,
            "record": record,
            "coin": coin,
            "stance": stance,
            "reason": reason,
            "selected_count": len(selected),
        }

    brief["task"]["uploaded_images"] = uploaded_images_desc
    brief["materials"] = materials
    record["material_post_mapping"] = {
        material["id"]: evidence["source_post_id"] for material, evidence in zip(materials, selected)
    }
    prompt, prompt_meta = render_prompt_template(ARTICLE_PROMPT_NAME)
    if not prompt.strip():
        raise ValueError(f"文章提示词为空或读取失败: {ARTICLE_PROMPT_FILE_PATH}")
    return {
        "is_skip": False,
        "record": record,
        "full_prompt": f"{prompt}\n{json.dumps(brief, ensure_ascii=False)}",
        "prompt_meta": prompt_meta,
        "materials": materials,
        "image_mapping": image_mapping,
        "file_paths": file_paths,
        "coin": coin,
        "stance": stance,
    }


# ==============================================================================
# 四、娱乐高流量第二阶段：原图验货与重塑
# ==============================================================================
def get_remodeled_viral_post_ids(article_manager: Any, source: Any, post_ids: Sequence[Any]) -> set:
    """获取已执行过高流量二阶段 (status=ok/skip) 的原帖 ID 集合。"""
    raw_ids = list(dict.fromkeys(post_ids))
    if not raw_ids:
        return set()
    str_ids = [str(pid).strip() for pid in raw_ids if str(pid).strip()]
    int_ids = [int(pid) for pid in str_ids if pid.isdigit()]
    query_ids = list(dict.fromkeys([*raw_ids, *str_ids, *int_ids]))
    source_query = {"$in": list(source)} if isinstance(source, (list, tuple, set)) else source
    articles = (
        article_manager.find_articles(
            query={
                "source": source_query,
                "type": ARTICLE_TYPE_VIRAL,
                "status": {"$in": ["ok", "skip"]},
                "post_id_list": {"$in": query_ids},
            },
            limit=0,
        )
        or []
    )
    used = set()
    for article in articles:
        for pid in article.get("post_id_list", []):
            if pid is not None:
                used.add(str(pid).strip())
    return used


def extract_valid_viral_posts(article_manager: Any) -> List[Dict[str, Any]]:
    """筛出通过一阶段粗筛的近 24h 高互动原帖，按热度加权排序并实施同源事件限额冷却（<=2）。"""
    started = time.monotonic()
    post_manager = UniversalPostManager(gen_db_object())
    now = time.time()
    posts = _find_posts_by_sources(
        post_manager,
        limit=POST_QUERY_LIMIT,
        extra_query={"publish_time": {"$gte": now - (VIRAL_POST_MAX_AGE_HOURS + 2) * 3600}},
    )
    self_post_ids = _get_all_self_published_post_ids(article_manager)

    recent_viral_articles = (
        article_manager.find_articles(
            query={
                "source": {"$in": list(POST_SOURCES)},
                "type": ARTICLE_TYPE_VIRAL,
                "status": "ok",
                "created_at": {"$gte": datetime.now(timezone.utc) - timedelta(hours=48)},
            },
            limit=0,
        )
        or []
    )
    used_event_keys: Counter = Counter()
    for art in recent_viral_articles:
        ek = (((art.get("creation_brief") or {}).get("stage1_screen") or {}).get("event_key") or "").strip()
        if ek:
            used_event_keys[ek] += 1

    candidates = []
    for post in posts:
        if _is_self_post(post, self_post_ids):
            continue
        screen = post.get("viral_screen")
        if not isinstance(screen, dict) or screen.get("prompt_version") != prompt_version or screen.get("keep") is not True:
            continue
        engagement = post.get("engagement") or {}
        view_count = engagement.get("view_count", 0)
        like_count = engagement.get("like_count", 0) or 0
        comment_count = engagement.get("comment_count", 0) or 0
        min_view_count = VIRAL_MIN_VIEW_COUNTS.get(post.get("source"), 1000)
        if not isinstance(view_count, (int, float)) or isinstance(view_count, bool) or view_count <= min_view_count:
            continue
        try:
            publish_time = _publish_time_seconds(post.get("publish_time", 0))
        except ValueError:
            continue
        age_hours = (now - publish_time) / 3600
        if age_hours > VIRAL_POST_MAX_AGE_HOURS or age_hours < -1:
            continue
        text = (post.get("content") or {}).get("text_content") or ""
        if any(match.group(1) == "视频" for match in MEDIA_PATTERN.finditer(text)):
            continue
        if not (post.get("media") or {}).get("images"):
            continue
        if not _is_valid_raw_post(post, require_local_media=False):
            continue
        hot_weight = view_count + like_count * 20 + comment_count * 50
        candidates.append((post, hot_weight, publish_time))

    candidate_ids = [item[0].get("post_id") for item in candidates if item[0].get("post_id") is not None]
    processed_ids = get_remodeled_viral_post_ids(article_manager, POST_SOURCES, candidate_ids)
    unprocessed = [item for item in candidates if str(item[0].get("post_id", "")).strip() not in processed_ids]
    unprocessed.sort(key=lambda item: (item[1], item[2]), reverse=True)

    result, event_skipped = [], 0
    for post, _, _ in unprocessed:
        event_key = ((post.get("viral_screen") or {}).get("event_key") or "").strip()
        if event_key and used_event_keys[event_key] >= 2:
            event_skipped += 1
            continue
        if event_key:
            used_event_keys[event_key] += 1
        result.append(post)

    logger.info(
        f"[高流量/选材] 已筛选待重塑候选帖 | 扫描: [{len(posts)}] | 粗筛达标: [{len(candidates)}] "
        f"| 已处理跳过: [{len(candidates) - len(unprocessed)}] | 同源热点冷却跳过: [{event_skipped}] "
        f"| 本轮待精改: [{len(result)}] | 耗时: [{time.monotonic() - started:.2f} 秒]"
    )
    return result


def check_viral_article_info(raw_info: Any, placeholders: Sequence[str], max_chars: int) -> Tuple[bool, str]:
    """校验高流量二阶段重塑输出的全部字段、覆盖性与枚举约束。"""
    import re

    expected_keys = {
        "image_semantics",
        "selected_images",
        "global_truth",
        "feasibility_audit",
        "usable",
        "topic_tag",
        "expression_strategy",
        "post_text",
        "bait_comments",
        "viral_score",
    }
    error = _key_error(raw_info, expected_keys, "高流量二阶段重塑")
    if error:
        return False, error

    image_semantics = raw_info["image_semantics"]
    if not isinstance(image_semantics, list):
        return False, "image_semantics 必须是列表"

    parsed_ids, kept_ids_in_semantics = [], set()
    for item in image_semantics:
        if not isinstance(item, dict):
            return False, "image_semantics 中的元素必须是字典对象"
        if "exchange_ui" not in item:
            return False, f"图片 {item.get('id')!r} 缺少 exchange_ui 字段"
        img_id, role, keep, ex_ui = (
            item.get("id"),
            item.get("semantic_role"),
            item.get("keep"),
            item.get("exchange_ui"),
        )
        if not isinstance(img_id, str) or img_id not in placeholders:
            return False, f"image_semantics.id ({img_id!r}) 不在原帖真实图片占位符 {sorted(placeholders)} 中"
        if not isinstance(role, str) or not role.strip():
            return False, f"图片 {img_id} 的 semantic_role 必须是非空字符串"
        if ex_ui is not None and not isinstance(ex_ui, str):
            return False, f"图片 {img_id} 的 exchange_ui 必须是字符串或 null"
        if not isinstance(keep, bool):
            return False, f"图片 {img_id} 的 keep 必须是布尔值"
        parsed_ids.append(img_id)
        if keep:
            kept_ids_in_semantics.add(img_id)

    if len(parsed_ids) != len(set(parsed_ids)) or set(parsed_ids) != set(placeholders):
        return False, f"image_semantics 必须不重不漏地覆盖全部原帖图片: 期望 {sorted(placeholders)}, 实际 {sorted(parsed_ids)}"

    if not isinstance(raw_info["global_truth"], str) or not raw_info["global_truth"].strip():
        return False, "global_truth 必须是非空字符串"
    if not isinstance(raw_info["feasibility_audit"], str) or not raw_info["feasibility_audit"].strip():
        return False, "feasibility_audit 必须是非空字符串（放行或淘汰均需说明依据）"

    usable = raw_info["usable"]
    if not isinstance(usable, bool):
        return False, "usable 必须是布尔值"
    if not usable:
        return True, ""

    allowed_tags = (
        "搞钱与盈亏",
        "情感与两性",
        "奇葩与吃瓜",
        "人性与潜规则",
        "职场与生存",
        "搞笑与整活",
    )
    if raw_info["topic_tag"] not in allowed_tags:
        return False, f"topic_tag 取值不合法: {raw_info['topic_tag']!r}"

    selected_images = raw_info["selected_images"]
    if not isinstance(selected_images, list) or any(not isinstance(item, str) or not item for item in selected_images):
        return False, "selected_images 必须是非空字符串组成的列表"
    if len(selected_images) != len(set(selected_images)):
        return False, "selected_images 不允许重复"
    if len(selected_images) > 5:
        return False, "selected_images 最多保留 5 张图片"
    if any(item not in placeholders for item in selected_images):
        return False, f"selected_images 必须来自原帖真实图片占位符 {sorted(placeholders)}"
    if any(item not in kept_ids_in_semantics for item in selected_images):
        return False, "selected_images 中包含了在 image_semantics 里被标记为 keep=false 的废图"

    if not isinstance(raw_info["expression_strategy"], str) or not raw_info["expression_strategy"].strip():
        return False, "usable 为 true 时 expression_strategy 必须是非空字符串"

    text = raw_info["post_text"]
    if not isinstance(text, str) or not text.strip():
        return False, "post_text 必须是非空字符串"
    if len(re.sub(r"\s", "", text)) > max_chars:
        return False, f"post_text 非空白字符数超过 {max_chars}"
    if re.search(r"\[(?:ASSET_IMG|IMAGE|VIDEO)_\d+\]", text):
        return False, "post_text 不能包含图片或视频占位符"

    comments = raw_info["bait_comments"]
    if (
        not isinstance(comments, list)
        or not 1 <= len(comments) <= 3
        or any(not isinstance(item, str) or not item.strip() for item in comments)
    ):
        return False, "bait_comments 必须是包含 1—3 条非空字符串的列表"
    if any(len(re.sub(r"\s", "", item)) > 24 for item in comments):
        return False, "bait_comments 单条评论字数过长（需控制在 24 字以内）"

    viral_score = raw_info["viral_score"]
    if isinstance(viral_score, bool) or not isinstance(viral_score, (int, float)):
        return False, "viral_score 必须是数字"
    if not (0.0 < float(viral_score) <= 10.0):
        return False, "usable 为 true 时 viral_score 必须在 (0, 10] 区间内"
    return True, ""


def build_viral_article_task(post: Dict[str, Any]) -> Dict[str, Any]:
    """构建高流量二阶段多模态精改任务（不预设硬编码模型名）。"""
    post_id = post.get("post_id")
    post_id_str = str(post_id or "").strip()
    post_source = post.get("source") or BINANCE_SOURCE
    screen = post.get("viral_screen") or {}
    normalized_text, file_paths, placeholders = normalize_post_media(post)
    source_mapping = build_source_image_mapping(post)
    image_mapping = {
        img_id: {
            "source": post_source,
            "source_post_id": post_id,
            "original_image_id": img_id,
            "original_image_id_list": [img_id],
            "content_id": get_image_content_id(meta.get("local_path") or ""),
            **meta,
        }
        for img_id, meta in source_mapping.items()
    }

    comments = [comment_info.get("content", "") for comment_info in (post.get("comments") or []) if isinstance(comment_info, dict)][:20]
    brief = {
        "current_year": datetime.now(BEIJING_TZ).year,
        "original_post": {
            "post_id": post_id_str,
            "text_content": normalized_text.strip(),
            "comments": comments,
            "available_images": list(placeholders.keys()),
        },
        "stage1_screen": {
            "event_key": screen.get("event_key", ""),
        },
    }
    record: Dict[str, Any] = {
        "source": post_source,
        "type": ARTICLE_TYPE_VIRAL,
        "topic": "奇葩与吃瓜",
        "stance": "无主语直抛",
        "status": "processing",
        "creation_brief": brief,
        "material_post_mapping": {"M1": post_id},
        "post_id_list": [post_id],
        "prompt_file_path": VIRAL_STAGE2_PROMPT_FILE_PATH,
        "model_name": None,
        "prompt_version": prompt_version,
        "article_info": None,
        "error_message": None,
        "error_history": [],
        "raw_response": None,
        "attempt_count": 0,
    }
    prompt, prompt_meta = render_prompt_template(VIRAL_STAGE2_PROMPT_NAME)
    if not prompt.strip():
        raise ValueError(f"高流量文章提示词为空或读取失败: {VIRAL_STAGE2_PROMPT_FILE_PATH}")
    return {
        "record": record,
        "full_prompt": f"{prompt}\n{json.dumps(brief['original_post'], ensure_ascii=False)}",
        "prompt_meta": prompt_meta,
        "file_paths": file_paths,
        "placeholders": set(source_mapping.keys()),
        "image_mapping": image_mapping,
        "post_id": post_id_str,
        "topic": "奇葩与吃瓜",
    }


def _generate_article_record(
    task: Dict[str, Any],
    validator: Callable[[Any], Tuple[bool, str]],
    convert: Callable[[Dict[str, Any]], Dict[str, Any]],
    action: str,
    context: str,
    *,
    preset_model_group: str = "medium",
    file_paths: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """统一通过 `common.llm_gateway.call_llm_json` 调用文章模型并忠实记录真实 `model_used`。"""
    record = task["record"]
    raw_info, trace = call_llm_json(
        prompt=task["full_prompt"],
        prompt_meta=task.get("prompt_meta"),
        model=None,
        preset_model_group=preset_model_group,
        file_paths=file_paths or [],
        expected_type=dict,
        validator=validator,
        max_attempts=LLM_MAX_RETRIES,
        task_label=action,
        logger_obj=logger,
    )
    record["attempt_count"] = int(trace.get("schema_attempt") or trace.get("attempts") or 1)
    record["raw_response"] = trace.get("raw_response") or ""
    record["error_history"] = [{"message": msg} if isinstance(msg, str) else msg for msg in (trace.get("error_history") or [])]
    if trace.get("model_used"):
        record["model_name"] = trace["model_used"]

    if not isinstance(raw_info, dict):
        error_message = str(trace.get("error") or trace.get("parse_error") or trace.get("call_status") or "LLM 返回空或校验未通过")
        record.update(status="error", article_info=None, error_message=error_message)
        _log(
            action,
            "生成文章时模型调用或结果校验失败",
            level="error",
            上下文=context,
            尝试=f"{record['attempt_count']}/{LLM_MAX_RETRIES}",
            原因=error_message,
            后续="返回error记录，不入库",
            排查="检查模型服务、字段契约和素材/图片引用",
        )
        return record

    info = convert(raw_info)
    record.update(article_info=info, status=info["status"], error_message=None)
    return record


def execute_article_task(task_data: Dict[str, Any], article_manager: Any) -> Dict[str, Any]:
    """执行单个分析文章生成任务，校验通过且 `status=ok` 时落库。"""
    coin, stance = task_data["coin"], task_data["stance"]
    if task_data.get("is_skip"):
        logger.debug(f"[单币分析/跳过] 选材不足 | 币种: [{coin}] | 立场: [{stance}] | 原因: [{task_data['reason']}]")
        return task_data["record"]
    started = time.monotonic()
    mapping = task_data["image_mapping"]

    def convert(info: Dict[str, Any]) -> Dict[str, Any]:
        info["image_mapping"] = {key: dict(mapping[key]) for key in info["image_placeholders"]}
        return info

    record = _generate_article_record(
        task_data,
        lambda info: check_article_info(info, task_data["materials"], mapping, ARTICLE_MAX_CHARS),
        convert,
        "单币分析/生成",
        f"币种={coin},立场={stance}",
        preset_model_group="medium",
        file_paths=task_data.get("file_paths") or [],
    )
    if record["status"] != "ok":
        if record["status"] == "skip":
            _log(
                "单币分析/跳过",
                "模型决定不生成文章",
                币种=coin,
                立场=stance,
                原因=(record.get("article_info") or {}).get("reason", "未说明"),
            )
        return record
    saved = prepare_article_record_for_db(record)
    article_manager.upsert_articles([saved])
    _log(
        "单币分析/入库",
        "文章已校验并入库",
        币种=coin,
        立场=stance,
        模型=saved.get("model_name") or "未返回",
        评分=(saved.get("article_info") or {}).get("score"),
        尝试=record["attempt_count"],
        耗时=f"{time.monotonic() - started:.2f}s",
    )
    return saved


def _convert_viral_article_info(raw: Dict[str, Any], task: Dict[str, Any]) -> Dict[str, Any]:
    """将二阶段模型原始字典转为标准化落库 `article_info` 结构。"""
    record = task["record"]
    usable = raw["usable"]
    record["topic"] = raw["topic_tag"] if usable else "无效噪音"
    normalized_semantics: List[Dict[str, Any]] = []
    exchange_ui_by_id: Dict[str, str] = {}
    for item in raw["image_semantics"]:
        clean_item = dict(item)
        ex_ui = normalize_exchange_ui(clean_item.get("exchange_ui"))
        clean_item["exchange_ui"] = ex_ui
        normalized_semantics.append(clean_item)
        if clean_item.get("id"):
            exchange_ui_by_id[clean_item["id"]] = ex_ui
    info: Dict[str, Any] = {
        "status": "ok" if usable else "skip",
        "usable": usable,
        "decision_reason": raw["feasibility_audit"].strip(),
        "viral_analysis": raw["global_truth"].strip(),
        "image_semantics": normalized_semantics,
        "topic_tag": record["topic"],
        "selected_images": [],
        "image_role": "",
        "post_text": "",
        "bait_comments": [],
        "viral_score": 0.0,
        "image_mapping": {},
    }
    if not usable:
        return info
    selected = raw["selected_images"]
    info.update(
        selected_images=selected,
        image_role=raw["expression_strategy"].strip(),
        post_text=raw["post_text"].strip(),
        used_material_ids=["M1"],
        bait_comments=[comment.strip() for comment in raw["bait_comments"]],
        viral_score=round(float(raw["viral_score"]), 1),
        image_mapping={
            key: {**dict(task["image_mapping"][key]), "exchange_ui": exchange_ui_by_id.get(key, "")}
            for key in selected
        },
    )
    return info


def execute_viral_article_task(task_data: Dict[str, Any], article_manager: Any) -> Dict[str, Any]:
    """执行单个高流量二阶段洗稿任务，保存 `ok` 或 `skip`，`error` 不入库。"""
    started = time.monotonic()
    post_id = task_data["post_id"]
    record = _generate_article_record(
        task_data,
        lambda info: check_viral_article_info(info, task_data["placeholders"], ARTICLE_MAX_CHARS),
        lambda info: _convert_viral_article_info(info, task_data),
        "高流量/精改",
        f"原帖={post_id}",
        preset_model_group="medium",
        file_paths=task_data["file_paths"],
    )
    if record["status"] == "error":
        return record
    saved = prepare_article_record_for_db(record)
    article_manager.upsert_articles([saved])
    info = saved["article_info"]
    _log(
        "高流量/完成",
        "原图验货与重塑结果已入库",
        原帖=post_id,
        状态=saved["status"],
        赛道=saved["topic"],
        模型=saved.get("model_name") or "未返回",
        评分=info["viral_score"],
        选图=len(info["selected_images"]),
        原因=info["decision_reason"],
        尝试=record["attempt_count"],
        耗时=f"{time.monotonic() - started:.2f}s",
    )
    return saved


def _execute_article_tasks(
    tasks: Sequence[Dict[str, Any]],
    worker: Callable[[Dict[str, Any], Any], Dict[str, Any]],
    manager: Any,
    action: str,
) -> Tuple[List[Dict[str, Any]], int, int]:
    results, raised, model_skips = [], 0, 0
    if not tasks:
        return results, raised, model_skips
    with ThreadPoolExecutor(max_workers=ARTICLE_CONCURRENCY) as executor:
        futures = {executor.submit(worker, task, manager): task for task in tasks}
        for future in as_completed(futures):
            task = futures[future]
            try:
                result = future.result()
                results.append(result)
                model_skips += int(result.get("status") == "skip" and not task.get("is_skip"))
            except Exception:
                raised += 1
                _log(
                    f"{action}/执行",
                    "任务异常结束，继续其他任务",
                    level="exception",
                    上下文=task.get("post_id") or f"{task.get('coin')}/{task.get('stance')}",
                    排查="检查任务字段与数据库；保存结果未知时先核对记录",
                )
    return results, raised, model_skips


def generate_analysis_articles_once() -> List[Dict[str, Any]]:
    """单轮并发生成单币分析文章，返回含 `ok/skip/error` 的记录列表。"""
    started = time.monotonic()
    manager = GeneratedArticleManager(gen_db_object())
    grouped = extract_and_group_valid_evidences(manager)
    groups = [(coin, stance, items) for coin, stances in grouped.items() for stance, items in stances.items()]
    tasks = [
        build_article_task(coin, stance, items, manager)
        for coin, stance, items in groups
        for _ in range(ARTICLE_VARIANTS)
    ]
    local_skips = sum(bool(task.get("is_skip")) for task in tasks)
    skipped_groups: Dict[str, int] = {}
    for task in tasks:
        if task.get("is_skip"):
            key = f"{task['coin']}/{task['stance']}"
            if key not in skipped_groups:
                skipped_groups[key] = task.get("selected_count", 0)
    skip_summary_str = ", ".join(f"{k}({v}条)" for k, v in skipped_groups.items()) or "无"

    if len(tasks) == local_skips:
        _log(
            "单币分析/选材",
            f"本轮各币种有效论据均不足(<{ARTICLE_MIN_MATERIALS}条)，跳过生成",
            扫描币种=len(grouped),
            方向分组=len(groups),
            各组有效论据=skip_summary_str,
            耗时=f"{time.monotonic() - started:.2f}s",
        )
        return [task["record"] for task in tasks]

    _log(
        "单币分析/计划",
        "选材完成，启动单币分析文章生成",
        扫描币种=len(grouped),
        方向分组=len(groups),
        待生成任务=len(tasks) - local_skips,
        跳过分组=skip_summary_str,
    )
    results, raised, model_skips = _execute_article_tasks(tasks, execute_article_task, manager, "单币分析")
    counts = Counter(item.get("status") for item in results)
    _log(
        "单币分析/汇总",
        "本轮单币分析生成结束",
        可执行任务=len(tasks) - local_skips,
        成功入库=counts["ok"],
        模型跳过=model_skips,
        论据不足跳过=local_skips,
        失败=counts["error"] + raised,
        耗时=f"{time.monotonic() - started:.2f}s",
    )
    return results


def generate_viral_articles_once() -> List[Dict[str, Any]]:
    """单轮并发执行高流量二阶段多模态重塑任务。"""
    started = time.monotonic()
    manager = GeneratedArticleManager(gen_db_object())
    tasks = []
    for post in extract_valid_viral_posts(manager):
        if not ensure_post_media_downloaded(post):
            logger.debug(f"[高流量/媒体] 按需下载或图片校验未通过，跳过原帖 | 原帖: [{post.get('post_id')}]")
            continue
        tasks.append(build_viral_article_task(post))
    if not tasks:
        return []
    _log(
        "高流量/计划",
        "第二阶段多模态精改就绪",
        待精改任务=len(tasks),
        并发=ARTICLE_CONCURRENCY,
        模型调用上限=len(tasks) * LLM_MAX_RETRIES,
    )
    results, raised, _ = _execute_article_tasks(tasks, execute_viral_article_task, manager, "高流量")
    counts = Counter(item.get("status") for item in results)
    _log(
        "高流量/汇总",
        "本轮精改结束",
        任务=len(tasks),
        成功=counts["ok"],
        二阶段淘汰=counts["skip"],
        失败=counts["error"] + raised,
        耗时=f"{time.monotonic() - started:.2f}s",
    )
    return results


def _poll_generation(
    task: Callable[[], List[Dict[str, Any]]],
    action: str,
    *,
    interval_seconds: int = ARTICLE_GENERATION_INTERVAL_SECONDS,
    stop_event: Optional[threading.Event] = None,
    max_rounds: int = 0,
) -> None:
    def _step(_round_idx: int) -> Dict[str, Any]:
        results = task()
        has_ok = any(isinstance(item, dict) and item.get("status") == "ok" for item in (results or []))
        return {"next_sleep_seconds": interval_seconds if has_ok else 60}

    run_daemon_loop(
        _step,
        interval_seconds=60,
        error_backoff_seconds=60,
        name=f"{action}/轮询",
        logger_obj=logger,
        max_rounds=max_rounds,
        stop_event=stop_event,
    )


def generate_analysis_articles(stop_event: Optional[threading.Event] = None, max_rounds: int = 0) -> None:
    _poll_generation(
        generate_analysis_articles_once,
        "单币文章",
        interval_seconds=ANALYSIS_GENERATION_INTERVAL_SECONDS,
        stop_event=stop_event,
        max_rounds=max_rounds,
    )


def generate_viral_articles(stop_event: Optional[threading.Event] = None, max_rounds: int = 0) -> None:
    _poll_generation(
        generate_viral_articles_once,
        "高流量",
        interval_seconds=ARTICLE_GENERATION_INTERVAL_SECONDS,
        stop_event=stop_event,
        max_rounds=max_rounds,
    )


__all__ = [
    "batch_screen_viral_posts",
    "build_article_task",
    "build_viral_article_task",
    "check_article_info",
    "check_viral_article_info",
    "check_viral_stage1_info",
    "execute_article_task",
    "execute_viral_article_task",
    "extract_and_group_valid_evidences",
    "extract_valid_viral_posts",
    "gen_viral_stage1_info",
    "generate_analysis_articles",
    "generate_analysis_articles_once",
    "generate_viral_articles",
    "generate_viral_articles_once",
    "get_post_usage_counts",
    "get_recent_openings",
    "get_remodeled_viral_post_ids",
    "is_need_viral_screening",
    "prepare_article_record_for_db",
    "process_and_save_single_viral_post",
    "screen_viral_posts",
    "screen_viral_posts_once",
    "transform_mlus",
]
