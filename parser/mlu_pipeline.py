# -*- coding: utf-8 -*-
"""
分析类原帖初筛与多模态论据 (MLU) 提取流水线 (`parser/mlu_pipeline.py`)

核心职责：
1. 批量纯文本初筛 (`batch_pre_screen_posts`)：每批 50 帖调用 `low` 模型组判定 `no_evidence`，原地回写 `pre_screen`；
2. 并发多模态论据精提 (`process_and_save_single_post`)：传入原帖图片至 `low` 视觉模型组提取结构化 `evidences`，回写 `logic_mul`；
3. 共享并发批次筛查调度 (`run_concurrent_screening_batches`)，供高流量一阶段粗筛复用；
4. 独立守护循环 (`format_image_article_once`, `format_image_article`)。
"""
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from common.common_utils import POST_SOURCES
from common.daemon import run_daemon_loop
from common.llm_gateway import call_llm_json, render_prompt_template
from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import UniversalPostManager
from parser.platform_adapters import _log, logger, normalize_exchange_ui
from parser.post_ingestion import (
    LLM_MAX_RETRIES,
    MAX_CONCURRENCY,
    MEDIA_PATTERN,
    MLU_EXTRACT_PROMPT_NAME,
    POST_MAX_AGE_HOURS,
    POST_QUERY_LIMIT,
    PRE_SCREEN_BATCH_SIZE,
    PRE_SCREEN_CONCURRENCY,
    PRE_SCREEN_PROMPT_NAME,
    _is_valid_raw_post,
    _key_error,
    ensure_post_media_downloaded,
    normalize_post_media,
    prompt_version,
)


def is_need_pre_screening(post: Dict[str, Any]) -> bool:
    """判定原帖是否需要进行分析类纯文本初筛。"""
    logic = post.get("logic_mul")
    if isinstance(logic, dict) and logic.get("prompt_version") == prompt_version:
        return False
    screen = post.get("pre_screen")
    if (
        isinstance(screen, dict)
        and screen.get("prompt_version") == prompt_version
        and isinstance(screen.get("no_evidence"), bool)
    ):
        return False
    return _is_valid_raw_post(post, require_local_media=False)


def is_need_formatting(post: Dict[str, Any]) -> bool:
    """判定原帖是否已通过当前版本初筛且尚无当前版本 `logic_mul`，需要多模态精提。"""
    screen = post.get("pre_screen")
    logic = post.get("logic_mul")
    if (
        (isinstance(logic, dict) and logic.get("prompt_version") == prompt_version)
        or not isinstance(screen, dict)
        or screen.get("prompt_version") != prompt_version
        or screen.get("no_evidence") is not False
    ):
        return False
    return _is_valid_raw_post(post, require_local_media=False)


def build_pre_screen_item(post: Dict[str, Any]) -> Dict[str, Any]:
    """原帖 -> `{post_id, text_content, images:[image1,...]}`（初筛仅传编号不上传媒体）。"""
    text = (post.get("content") or {}).get("text_content") or ""
    image_count = sum(match.group(1) != "视频" for match in MEDIA_PATTERN.finditer(text))
    return {
        "post_id": str(post.get("post_id", "")).strip(),
        "text_content": MEDIA_PATTERN.sub("", text).strip(),
        "images": [f"image{i}" for i in range(1, image_count + 1)],
    }


def check_pre_screen_info(json_data: Any, expected_post_ids: Iterable[Any]) -> Tuple[bool, str]:
    """校验批量初筛输出 `{post_id: {no_evidence: bool, reason: str}}`。"""
    if not isinstance(json_data, dict):
        return False, "初筛返回最外层必须是字典"
    expected = {str(pid) for pid in expected_post_ids}
    missing = expected - json_data.keys()
    if missing:
        return False, f"初筛返回缺少 post_id: {', '.join(sorted(missing)[:10])}"
    for pid in expected:
        item, label = json_data[pid], f"post[{pid}]"
        error = _key_error(item, {"no_evidence", "reason"}, label)
        if error:
            return False, error
        if not isinstance(item["no_evidence"], bool):
            return False, f"{label}.no_evidence 必须是布尔值"
        if not isinstance(item["reason"], str) or not item["reason"].strip():
            return False, f"{label}.reason 必须是非空字符串"
    return True, ""


def _resolve_prompt_pair(
    prompt_template: Any,
    default_prompt_name: str,
    prompt_meta: Optional[Dict[str, str]] = None,
) -> Tuple[str, Dict[str, str]]:
    if isinstance(prompt_template, str) and prompt_template.strip():
        return prompt_template, prompt_meta or {"prompt_file": default_prompt_name}
    rendered, meta = render_prompt_template(default_prompt_name)
    if not rendered.strip():
        raise ValueError(f"提示词为空或读取失败: {default_prompt_name}")
    return rendered, meta


def gen_pre_screen_batch_info(
    batch_posts: Sequence[Dict[str, Any]],
    prompt_template: Optional[str] = None,
    prompt_meta: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """调用 `low` 组大模型批量判定初筛批次，返回 `{post_id: {no_evidence, reason}}`。"""
    base_prompt, p_meta = _resolve_prompt_pair(prompt_template, PRE_SCREEN_PROMPT_NAME, prompt_meta)
    items = [build_pre_screen_item(post) for post in batch_posts]
    ids = [item["post_id"] for item in items]
    data, trace = call_llm_json(
        prompt=f"{base_prompt}\n{json.dumps(items, ensure_ascii=False)}",
        prompt_meta=p_meta,
        model=None,
        preset_model_group="low",
        expected_type=dict,
        validator=lambda response: check_pre_screen_info(response, ids),
        max_attempts=LLM_MAX_RETRIES,
        task_label="初筛/批次",
        logger_obj=logger,
    )
    if not isinstance(data, dict):
        logger.warning(
            f"[初筛/批次] 模型返回无效或校验失败 | 首帖: [{ids[0] if ids else 'NONE'}] "
            f"| 原因: [{trace.get('error') or trace.get('call_status')}]"
        )
        return {}
    return {str(key): value for key, value in data.items()}


def process_and_save_pre_screen_batch(
    batch_posts: Sequence[Dict[str, Any]],
    post_manager: Any,
    prompt_template: Optional[str] = None,
    prompt_meta: Optional[Dict[str, str]] = None,
) -> Tuple[int, int]:
    """执行单批初筛并将 `pre_screen` 回写至数据库，返回 `(通过数, 淘汰数)`。"""
    started = time.monotonic()
    screen_map = gen_pre_screen_batch_info(batch_posts, prompt_template, prompt_meta=prompt_meta)
    if not screen_map:
        return 0, 0
    updates, filtered = [], 0
    for post in batch_posts:
        result = screen_map.get(str(post.get("post_id", "")).strip())
        if not result:
            continue
        post["pre_screen"] = {
            "no_evidence": result["no_evidence"],
            "reason": result["reason"].strip(),
            "prompt_version": prompt_version,
            "screen_time": time.time(),
        }
        updates.append(post)
        filtered += int(bool(result["no_evidence"]))
    if updates:
        post_manager.upsert_posts(updates)
    passed = len(updates) - filtered
    logger.info(
        f"[初筛/回写] 批次已落库 | 处理: [{len(updates)}] | 通过: [{passed}] | 淘汰: [{filtered}] "
        f"| 耗时: [{time.monotonic() - started:.2f} 秒]"
    )
    return passed, filtered


def check_format_info(json_data: Any, placeholders: Sequence[str]) -> Tuple[bool, str]:
    """校验多模态论据提取输出 `{evidences: [...]}` 的严格字段与枚举契约。"""
    if not isinstance(json_data, dict):
        return False, "最外层必须是字典"
    if "evidences" not in json_data:
        return False, "最外层缺少 evidences"
    if not isinstance(json_data["evidences"], list):
        return False, "evidences 必须是列表"

    evidence_keys = {
        "target_coin",
        "stance",
        "dimension",
        "core_fact",
        "logic_link",
        "raw_hook",
        "impact_weight",
        "shelf_life",
        "images",
        "impact_reason",
    }
    image_keys = {"image_id", "image_type", "exchange_ui", "context", "usable", "unusable_reason"}
    enums = {
        "dimension": ("K线形态", "技术指标", "链上数据", "资金流", "消息面", "基本面", "情绪判断"),
        "stance": ("看多", "看空", "震荡"),
        "shelf_life": ("hours", "days", "weeks", "long", "unknown"),
    }
    image_types = ("盘面截图", "数据图表", "新闻截图", "社交截图", "收益截图", "梗图表情", "实拍照片", "其他")

    for index, evidence in enumerate(json_data["evidences"], 1):
        label = f"evidences[{index}]"
        error = _key_error(evidence, evidence_keys, label)
        if error:
            return False, error
        coin = evidence["target_coin"]
        if not isinstance(coin, str) or not coin.strip():
            return False, f"{label}.target_coin 必须是非空字符串"
        if coin != coin.upper():
            return False, f"{label}.target_coin 必须为大写英文字符串: {coin!r}"
        if not isinstance(evidence["core_fact"], str) or not evidence["core_fact"].strip():
            return False, f"{label}.core_fact 必须是非空字符串"
        for field in ("logic_link", "raw_hook"):
            if not isinstance(evidence[field], str):
                return False, f"{label}.{field} 必须是字符串（允许为空字符串）"
        weight = evidence["impact_weight"]
        if not isinstance(weight, int) or isinstance(weight, bool) or not 1 <= weight <= 5:
            return False, f"{label}.impact_weight 必须是 1—5 的整数，不能是布尔值"
        for field, allowed in enums.items():
            if evidence[field] not in allowed:
                return False, f"{label}.{field} 取值不合法: {evidence[field]!r}"
        if not isinstance(evidence["images"], list):
            return False, f"{label}.images 必须是列表"
        for image_index, image in enumerate(evidence["images"], 1):
            image_label = f"{label}.images[{image_index}]"
            error = _key_error(image, image_keys, image_label)
            if error:
                return False, error
            if image["image_id"] not in placeholders:
                return False, f"{image_label}.image_id 未出现在原文: {image['image_id']!r}"
            if image["image_type"] not in image_types:
                return False, f"{image_label}.image_type 不在允许枚举内"
            exchange_ui = image["exchange_ui"]
            if exchange_ui is not None and not isinstance(exchange_ui, str):
                return False, f"{image_label}.exchange_ui 必须是字符串或 null"
            usable, reason = image["usable"], image["unusable_reason"]
            if not isinstance(usable, bool) or not isinstance(reason, str):
                return False, f"{image_label} 的 usable 必须是布尔值，unusable_reason 必须是字符串"
            if usable and reason.strip():
                return False, f"{image_label} 可用时 unusable_reason 必须为空"
            if not usable and not reason.strip():
                return False, f"{image_label} 不可用时必须说明 unusable_reason"
    return True, ""


def gen_media_format_info(post: Dict[str, Any]) -> Dict[str, Any]:
    """原帖 -> 调用 `low` 视觉模型组提取 `{evidences: [...]}`（失败返回空字典）。"""
    text, paths, mapping = normalize_post_media(post)
    base_prompt, p_meta = render_prompt_template(MLU_EXTRACT_PROMPT_NAME)
    if not base_prompt.strip():
        raise ValueError(f"论据提取提示词为空或读取失败: {MLU_EXTRACT_PROMPT_NAME}")
    data, trace = call_llm_json(
        prompt=f"{base_prompt}\n{text}",
        prompt_meta=p_meta,
        model=None,
        preset_model_group="low",
        file_paths=paths,
        expected_type=dict,
        validator=lambda response: check_format_info(response, list(mapping)),
        max_attempts=LLM_MAX_RETRIES,
        task_label=f"论据/提取[{post.get('post_id', 'UNKNOWN_ID')}]",
        logger_obj=logger,
    )
    if not isinstance(data, dict):
        logger.warning(
            f"[论据/提取] 模型返回无效或校验失败 | 帖子ID: [{post.get('post_id', 'UNKNOWN_ID')}] "
            f"| 原因: [{trace.get('error') or trace.get('call_status')}]"
        )
        return {}
    for evidence in data.get("evidences") or []:
        if isinstance(evidence, dict):
            for image in evidence.get("images") or []:
                if isinstance(image, dict):
                    image["exchange_ui"] = normalize_exchange_ui(image.get("exchange_ui"))
    return data


def process_and_save_single_post(post: Dict[str, Any], post_manager: Any) -> None:
    """对单帖提取多模态论据并回写 `logic_mul`，隔离单帖异常。"""
    started = time.monotonic()
    post_id = post.get("post_id", "UNKNOWN_ID") if isinstance(post, dict) else "UNKNOWN_ID"
    try:
        if not ensure_post_media_downloaded(post):
            logger.debug(f"[帖子/媒体] 按需下载或图片校验未通过，跳过本帖 | 帖子: [{post_id}]")
            return
        info = gen_media_format_info(post)
        if not info:
            return
        info["prompt_version"] = prompt_version
        post["logic_mul"] = info
        post_manager.upsert_posts([post])
        logger.info(
            f"[帖子/回写] 论据已保存 | 帖子: [{post_id}] | 论据: [{len(info['evidences'])} 条] "
            f"| 耗时: [{time.monotonic() - started:.2f} 秒]"
        )
    except Exception:
        logger.exception(
            f"❌ [帖子/处理] 当前帖子失败 | 帖子: [{post_id}] | 后续: [继续其他任务] "
            "| 排查: [检查帖子字段、提示词、媒体路径及数据库]"
        )


def run_concurrent_screening_batches(
    posts: Sequence[Dict[str, Any]],
    post_manager: Any,
    predicate: Callable[[Dict[str, Any]], bool],
    processor: Callable[[Sequence[Dict[str, Any]], Any, str], Tuple[int, int]],
    prompt_name: str,
    batch_size: int,
    action: str,
) -> Tuple[int, int, int, int]:
    """公共并发批量纯文本筛查调度器，返回 `(待筛数, 通过数, 淘汰数, 失败批次数)`。"""
    pending = [post for post in posts if predicate(post)]
    if not pending:
        logger.debug(f"[{action}/计划] 当前无待筛新帖 | 扫描: [{len(posts)}]")
        return 0, 0, 0, 0

    prompt, _meta = render_prompt_template(prompt_name)
    if not isinstance(prompt, str) or not prompt.strip():
        raise ValueError(f"{action}提示词为空或读取失败: {prompt_name}")

    batches = [pending[index : index + batch_size] for index in range(0, len(pending), batch_size)]
    _log(
        f"{action}/计划",
        "批量文本筛查就绪",
        扫描=len(posts),
        待筛=len(pending),
        跳过=len(posts) - len(pending),
        批次=len(batches),
        单批上限=batch_size,
        并发=PRE_SCREEN_CONCURRENCY,
    )
    started = time.monotonic()
    passed = rejected = failed = 0
    with ThreadPoolExecutor(max_workers=PRE_SCREEN_CONCURRENCY) as executor:
        futures = {executor.submit(processor, batch, post_manager, prompt): batch for batch in batches}
        for future in as_completed(futures):
            batch = futures[future]
            try:
                kept, dropped = future.result()
                passed += kept
                rejected += dropped
                failed += int(kept + dropped == 0)
            except Exception:
                failed += 1
                _log(
                    f"{action}/执行",
                    "当前批次处理或回写失败，继续其他批次",
                    level="exception",
                    首帖=batch[0].get("post_id"),
                    帖数=len(batch),
                    排查="检查模型、帖子字段与数据库；核对是否已部分写入",
                )
    _log(
        f"{action}/汇总",
        "本轮筛查结束",
        待筛=len(pending),
        通过=passed,
        淘汰=rejected,
        失败批次=failed,
        耗时=f"{time.monotonic() - started:.2f}秒",
    )
    return len(pending), passed, rejected, failed


def batch_pre_screen_posts(posts: Sequence[Dict[str, Any]], post_manager: Any) -> Tuple[int, int, int, int]:
    """每批 50 帖并发初筛分析原帖并原地更新 `pre_screen`。"""
    return run_concurrent_screening_batches(
        posts,
        post_manager,
        is_need_pre_screening,
        process_and_save_pre_screen_batch,
        PRE_SCREEN_PROMPT_NAME,
        PRE_SCREEN_BATCH_SIZE,
        "初筛",
    )


def format_image_article_once(post_manager: Optional[Any] = None) -> Tuple[int, int, int, int, int]:
    """单轮执行分析原帖初筛与并发多模态论据提取，返回 `(待初筛数, 通过数, 淘汰数, 精提数, 建议休眠秒)`。"""
    started = time.monotonic()
    if post_manager is None:
        post_manager = UniversalPostManager(gen_db_object())
    min_publish_ts = time.time() - (POST_MAX_AGE_HOURS + 2) * 3600
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
        logger.info("[分析筛查/轮询] 库内暂无近50h原帖 | 下次扫描: [60s后]")
        return 0, 0, 0, 0, 60

    pre_pending, pre_pass, pre_rej, _ = batch_pre_screen_posts(posts, post_manager) or (0, 0, 0, 0)
    valid_posts = [post for post in posts if is_need_formatting(post)]
    if valid_posts:
        logger.info(
            f"[论据提取/计划] 启动多模态论据精提 | 扫描原帖: [{len(posts)}] | 待精提: [{len(valid_posts)}] "
            f"| 并发上限: [{MAX_CONCURRENCY}]"
        )
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as executor:
            futures = [executor.submit(process_and_save_single_post, post, post_manager) for post in valid_posts]
            for future in as_completed(futures):
                future.result()

    delay = 5 if any(len(batch) >= POST_QUERY_LIMIT for batch in source_batches) else 60
    if pre_pending == 0 and not valid_posts:
        logger.info(
            f"[分析筛查/轮询] 本轮无新增待处理分析原帖 | 库内近50h原帖: [{len(posts)}] "
            f"| 耗时: [{time.monotonic() - started:.2f}s] | 下次扫描: [{delay}s后]"
        )
    else:
        logger.info(
            f"[分析筛查/汇总] 本轮分析初筛与精提完成 | 扫描原帖: [{len(posts)}] | 初筛(通过/淘汰): [{pre_pass}/{pre_rej}] "
            f"| 论据精提: [{len(valid_posts)}帖] | 耗时: [{time.monotonic() - started:.2f}s] | 下次扫描: [{delay}s后]"
        )
    return pre_pending, pre_pass, pre_rej, len(valid_posts), delay


def format_image_article(
    stop_event: Optional[threading.Event] = None,
    max_rounds: int = 0,
) -> None:
    """独立分析帖初筛与论据精提守护循环。"""

    def _step(_round_idx: int) -> Dict[str, Any]:
        post_manager = UniversalPostManager(gen_db_object())
        _, _, _, _, delay = format_image_article_once(post_manager)
        return {"next_sleep_seconds": delay}

    run_daemon_loop(
        _step,
        interval_seconds=60,
        error_backoff_seconds=60,
        name="分析筛查/轮询",
        logger_obj=logger,
        max_rounds=max_rounds,
        stop_event=stop_event,
    )


__all__ = [
    "batch_pre_screen_posts",
    "build_pre_screen_item",
    "check_format_info",
    "check_pre_screen_info",
    "format_image_article",
    "format_image_article_once",
    "gen_media_format_info",
    "gen_pre_screen_batch_info",
    "is_need_formatting",
    "is_need_pre_screening",
    "process_and_save_pre_screen_batch",
    "process_and_save_single_post",
    "run_concurrent_screening_batches",
]
