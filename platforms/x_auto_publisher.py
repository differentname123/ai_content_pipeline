# -*- coding: utf-8 -*-
"""
X (Twitter) 自动化应用层调度脚本 (x_auto_publisher.py)

【核心设计理念】
- 简洁、稳定优先（80/20 准则，零过度设计）：一切以无人值守稳定跑通为第一准则；
- 双重严格门禁：
  1. 等级门禁（默认仅发 S 级强烈必发爆款；支持配置化调整）；
  2. 字数门禁（严格校验纯汉字数 <= 140 以及 X平台标准计算单位 <= 280，拒绝超长发推）；
- 账号物理隔离与冷却：每个账号独立判定发帖间隔（至少半小时），安全防风控；
- 显式故障报警：发帖或模型调用异常时，在控制台打印醒目的报错 Banner；
- 状态持久化：发布成功/失败结果幂等回写至 MongoDB publish_records.x 与本地运行时状态文件。
"""
import json
import logging
import math
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.common_utils import (
    PLATFORM_X,
    PROMPT_DIR,
    get_config,
    get_runtime_file_path,
    read_json,
    resolve_session_dir,
    save_json,
    setup_logger,
)
from common.daemon import run_daemon_loop
from common.llm_gateway import call_llm_json, render_prompt_template
from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import GeneratedArticleManager, UniversalPostManager
from common.x_text import calc_x_weighted_length
from platforms.x_api import (
    fetch_multi_x_user_posts,
    format_historical_x_posts_for_prompt,
    load_cached_x_posts,
    merge_x_account_posts,
    resolve_x_accounts,
)
from platforms.x_playwright import create_x_post

# 初始化标准日志
logger = setup_logger(app_name="x_auto_publisher")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


# ==============================================================================
# 一、核心调度与账号配置（写在最前，方便按需自由调整）
# ==============================================================================

# 1. 目标发布账号列表（每个账号包含：别名 alias、浏览器会话目录 session_dir、主页用户名 screen_name）
TARGET_ACCOUNTS = [
    {
        "alias": "myself",
        "session_dir": resolve_session_dir("x_myself_browser_session_dir", "x_myself"),
        "screen_name": str(get_config("x_myself_screen_name", "peterzhu1003")).lstrip("@").strip(),
    },
    # 如需多账号轮询发帖，在此直接追加已登录好的浏览器目录即可：
    # {
    #     "alias": "dahao",
    #     "session_dir": resolve_session_dir("x_dahao_browser_session_dir", "x_dahao"),
    #     "screen_name": str(get_config("x_dahao_screen_name", "2434954452qqc")).lstrip("@").strip(),
    # },
]

# 2. 轮询巡检与发帖冷却间隔（秒）
CHECK_INTERVAL_SECONDS: int = 600           # 循环休眠轮询间隔（默认 10 分钟 = 600 秒）
# 【保留冷却原因说明】避开 X AuthorDiversityScorer 短时间同号连续发帖相互降权，并避免高频空转消耗模型额度；
# 每篇发布后冷却 3 小时（10800 秒），兼顾自然发布节奏与额度消耗控制。
ACCOUNT_POST_COOLDOWN_SECONDS: int = 10800

# 3. 严格门禁配置（严格精品制）
# 【保留过滤原因说明】严格执行精品门禁，仅允许发布 S 级顶级爆款；宁缺毋滥，非 S 级绝不推送。
ALLOWED_GRADES: List[str] = ["S"]           # 允许发布的评级（严格仅限 "S" 顶级爆款；A 级及以下严禁发送）
MAX_X_RAW_CHARS: int = 140                  # 纯汉字数上限（X 平台单条推文标准建议上限）
MAX_X_UNITS: int = 280                      # X 平台标准计数单位上限（半角字符记 1，汉字/Emoji/全角记 2，URL 记 23）
STRICT_X_LENGTH_GATE: bool = True           # 是否启用字符数量代码级二次强制校验

# 4. 候选素材抓取与精简排序配置
HISTORICAL_PROMPT_ACCOUNTS: List[str] = get_config("x_prompt_accounts", ["dahao", "myself"])  # 对比账号矩阵（包含大号与发帖号）
CANDIDATE_LOOKBACK_HOURS: int = 6           # 抓取近 6 小时内的全部候选帖
CANDIDATE_QUERY_LIMIT: int = 0              # 0 表示从数据库全量拉取 6h 候选，再按热度与新鲜度预排精选
HISTORICAL_REF_COUNT: int = 50              # 单账号实时抓取参考历史上限
HISTORICAL_SELF_ANTIDUP_COUNT: int = 30     # 仅当前待发账号近期已发条数，构成严格撞题禁止区
HISTORICAL_BENCHMARK_COUNT: int = 8         # 跨账号高浏览代表作条数，仅作风格与写法参考（绝不作为本号防重门槛）
# 【保留过滤原因说明】实测 6h 全量 198 篇候选拼接超 17 万字符，导致中档模型注意力分散且耗时翻倍；
# 经 6 窗口历史回放校准，前 35 篇会截断少数头部历史爆款，调整为前 50 篇既完整覆盖爆款池，又较原全量压缩 ~70% 上下文。
MAX_LLM_CANDIDATES: int = 50
_MEDIA_TAG_RE = re.compile(r"\[(?:插图|长文封面|视频封面|视频|ASSET_IMG|IMAGE|VIDEO)[^\]]*\]")

# 5. 运行模式与模型调用
BROWSER_VISIBLE: bool = True                # Playwright 发帖是否显示浏览器可见窗口（方便实时观测发帖状态）
MODEL_PRESET_GROUP: str = "medium"          # 按用户要求切换至中等模型组（避免消耗 high 组额度）
AI_TIMEOUT_SECONDS: int = 1200              # 模型调用超时阈值（秒），确保长候选列表推理不提前超时中断
AI_MAX_RETRIES: int = 3                     # 模型调用失败时的最大重试次数（默认 3 次）
AI_RETRY_DELAY_SECONDS: int = 15            # 重试退避等待基础间隔（秒）
PROMPT_TEMPLATE_PATH: str = str(PROMPT_DIR / "X平台_结构化自动选帖与改写提示词.txt")
STATE_FILE_PATH: str = str(get_runtime_file_path("x_publisher_state.json"))


# ==============================================================================
# 二、字符计算与工具函数
# ==============================================================================

calculate_x_character_units = calc_x_weighted_length


def load_publisher_state() -> Dict[str, Any]:
    """读取本地账号发帖状态记录"""
    if os.path.exists(STATE_FILE_PATH):
        try:
            data = read_json(STATE_FILE_PATH)
            if isinstance(data, dict):
                return data
        except Exception as e:
            logger.warning(f"[状态/读取] 读取本地状态文件异常: {e}，使用空状态初始化")
    return {}


def save_publisher_state(state: Dict[str, Any]) -> None:
    """持久化本地账号发帖状态记录"""
    try:
        save_json(STATE_FILE_PATH, state)
    except Exception as e:
        logger.warning(f"[状态/保存] 持久化本地状态文件异常: {e}")


def is_account_in_cooldown(
    account: Dict[str, Any],
    state: Dict[str, Any],
    cooldown_seconds: float = ACCOUNT_POST_COOLDOWN_SECONDS,
) -> Tuple[bool, float]:
    """
    检查账号当前是否处于发帖冷却中。
    返回: (是否在冷却中, 剩余冷却秒数)
    """
    alias = account["alias"]
    acc_state = state.get(alias) or {}
    last_pub_time = float(acc_state.get("last_publish_time") or 0.0)
    elapsed = time.time() - last_pub_time
    if elapsed < float(cooldown_seconds):
        remaining = float(cooldown_seconds) - elapsed
        return True, remaining
    return False, 0.0


# ==============================================================================
# 三、候选数据与提示词组装
# ==============================================================================

def fetch_unposted_viral_candidates(hours: int = CANDIDATE_LOOKBACK_HOURS, limit: int = CANDIDATE_QUERY_LIMIT) -> List[Dict[str, Any]]:
    """
    从数据库查询近 hours 小时内 status='ok' 且尚未成功发布到 X 的候选重塑帖。
    """
    try:
        article_manager = GeneratedArticleManager(gen_db_object())
        # 兼容 MongoDB 子字段查询：查询未发布或发布状态不为 success 的记录
        query = {
            "type": "viral",
            "status": "ok",
            "$or": [
                {f"publish_records.{PLATFORM_X}": None},
                {f"publish_records.{PLATFORM_X}.status": {"$ne": "success"}},
            ],
        }
        if hours and hours > 0:
            query["created_at"] = {"$gte": datetime.now(timezone.utc) - timedelta(hours=hours)}

        candidates = article_manager.find_articles(
            query=query,
            sort=[("created_at", -1)],
            limit=limit,
        ) or []
    except Exception as e:
        logger.error(f"❌ [候选/查询] 数据库查询流量候选帖失败: {e}")
        return []

    valid_candidates = []
    for art in candidates:
        if not isinstance(art, dict):
            continue
        info = art.get("article_info") or {}
        if not info.get("usable"):
            continue
        # Python 层二次防御：确保从未在 X 平台成功发布
        x_rec = (art.get("publish_records") or {}).get(PLATFORM_X) or {}
        if x_rec.get("status") == "success":
            continue
        valid_candidates.append(art)

    return valid_candidates


def _strip_media_markers(text: str) -> str:
    cleaned = _MEDIA_TAG_RE.sub(" ", str(text or ""))
    return re.sub(r"\s+", " ", cleaned).strip()


def rank_and_trim_x_candidates(
    candidates: List[Dict[str, Any]],
    max_candidates: int = MAX_LLM_CANDIDATES,
    now_ts: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """
    批量关联原帖热度与发布时间，按确定性综合分排序并截取前 `max_candidates` 篇送审，
    避免百篇长上下文稀释模型注意力。
    """
    if not candidates:
        return []

    cur_ts = time.time() if now_ts is None else float(now_ts)
    src_map: Dict[str, Dict[str, Any]] = {}
    try:
        raw_pids: List[Any] = []
        for art in candidates:
            for pid in art.get("post_id_list") or []:
                if pid is not None:
                    raw_pids.append(pid)
                    if str(pid).isdigit():
                        raw_pids.append(int(pid))
                        raw_pids.append(str(pid))
        if raw_pids:
            post_mgr = UniversalPostManager(gen_db_object())
            src_docs = post_mgr.find_posts(query={"post_id": {"$in": list(dict.fromkeys(raw_pids))}}, limit=0) or []
            for sp in src_docs:
                src_map[str(sp.get("post_id"))] = sp
    except Exception as exc:
        logger.debug(f"[候选/预排] 查询原帖热度异常，降级仅按 viral_score 排序: {exc}")

    scored: List[Tuple[float, Dict[str, Any]]] = []
    for art in candidates:
        info = art.get("article_info") or {}
        v_score = float(info.get("viral_score") or 0.0)
        pid_str = str((art.get("post_id_list") or [""])[0]).strip()
        sp = src_map.get(pid_str) or {}
        eng = sp.get("engagement") or art.get("_src_engagement") or {}
        views = int(eng.get("view_count") or 0)
        likes = int(eng.get("like_count") or 0)
        comments = int(eng.get("comment_count") or 0)

        age_h = 0.0
        raw_spt = sp.get("publish_time") or art.get("_src_publish_time") or 0
        if raw_spt:
            try:
                spt_val = float(raw_spt)
                spt_s = spt_val / 1000.0 if spt_val > 1e11 else spt_val
                age_h = round(max(0.0, (cur_ts - spt_s) / 3600.0), 1)
            except (TypeError, ValueError):
                age_h = 0.0

        hot_energy = views + likes * 30 + comments * 80
        rank_score = v_score + math.log10(max(hot_energy, 1000)) * 1.0 - (0.5 if age_h > 24.0 else 0.0)

        enriched = dict(art)
        if views > 0:
            enriched["_src_views"] = views
        if raw_spt:
            enriched["_src_age_hours"] = age_h
        scored.append((rank_score, enriched))

    scored.sort(key=lambda item: item[0], reverse=True)
    cap = int(max_candidates) if max_candidates and int(max_candidates) > 0 else len(scored)
    return [item[1] for item in scored[:cap]]


def build_candidate_prompt_data(candidates: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """
    将候选文章列表转换为高信息密度的精简结构（含原帖人话 `original_text` 与保留图精简描述），
    并建立快速查找字典。
    返回: (candidate_prompt_list, candidates_map)
    """
    prompt_list = []
    candidates_map = {}

    for art in candidates:
        info = art.get("article_info") or {}
        art_id = str(art.get("article_id") or "").strip()
        p_list = art.get("post_id_list") or []
        src_pid = str(p_list[0]).strip() if p_list else ""

        primary_id = art_id or src_pid
        if not primary_id:
            continue

        candidates_map[primary_id] = art
        if art_id:
            candidates_map[art_id] = art
        if src_pid:
            candidates_map[src_pid] = art

        orig_raw = ((art.get("creation_brief") or {}).get("original_post") or {}).get("text_content") or ""
        original_text = _strip_media_markers(orig_raw)[:220]
        kept_images = [
            {"id": img.get("id"), "desc": str(img.get("semantic_role") or "")[:90]}
            for img in (info.get("image_semantics") or [])
            if isinstance(img, dict) and img.get("keep")
        ]

        # 【保留过滤原因说明】：纯字段级精简，仅传 X 选帖与改写高价值信息：
        # 1. 剔除冗余废图语义（keep=False）与思考脚手架（decision_reason/image_role/viral_score 避免偏见与冗余）；
        # 2. 补充 original_text 让选帖模型能直接回看活人原话，避免被上游过分改写带偏。
        cleaned_item = {
            "id": primary_id,
            "topic_tag": info.get("topic_tag", ""),
            "source_views": art.get("_src_views"),
            "source_age_hours": art.get("_src_age_hours"),
            "original_text": original_text,
            "draft_text": info.get("post_text", ""),
            "kept_images": kept_images,
            "viral_analysis": str(info.get("viral_analysis") or "")[:120],
        }
        cleaned_item = {k: v for k, v in cleaned_item.items() if v not in (None, "", [], {})}
        prompt_list.append(cleaned_item)

    return prompt_list, candidates_map


def build_account_historical_posts_block(account: Dict[str, Any], count: int = HISTORICAL_REF_COUNT) -> str:
    """
    物理隔离「本号防重撞题名单」与「跨号高流爆款标杆」：
    - 区块 A：仅当前待发账号自己最近 30 条推文（严禁与本区块事件撞题）；
    - 区块 B：参考账号矩阵（排除本号）高浏览 Top 8 代表作（仅供写法与钩子标尺参考，严禁作本号撞题淘汰依据）。
    """
    cur_sn = str(account.get("screen_name") or "").lstrip("@").strip()
    cur_alias = str(account.get("alias") or "").strip()
    raw_accounts = list(HISTORICAL_PROMPT_ACCOUNTS or [])
    if cur_sn:
        raw_accounts.append(f"@{cur_sn}")
    target_specs = resolve_x_accounts(raw_accounts, default_accounts=["dahao", "myself"])

    posts_by_acc = None
    try:
        posts_by_acc = fetch_multi_x_user_posts(accounts=target_specs, count=max(count, 50), sort_by="time")
    except Exception as e:
        logger.warning(f"[历史/同步] 实时获取账号矩阵最新推文受阻: {e}，降级使用本地历史缓存")

    if not posts_by_acc:
        posts_by_acc = load_cached_x_posts(accounts=target_specs, sort_by="time")

    self_specs = resolve_x_accounts([f"@{cur_sn}"] if cur_sn else [cur_alias])
    other_specs = [s for s in target_specs if s["screen_name"].lower() != cur_sn.lower()]

    self_posts = merge_x_account_posts(
        posts_by_acc,
        accounts=self_specs,
        total_count=HISTORICAL_SELF_ANTIDUP_COUNT,
        sort_by="time",
    )
    bench_posts = (
        merge_x_account_posts(
            posts_by_acc,
            accounts=other_specs,
            total_count=HISTORICAL_BENCHMARK_COUNT,
            sort_by="views",
        )
        if other_specs
        else []
    )

    self_block = format_historical_x_posts_for_prompt(
        self_posts,
        recent_count=HISTORICAL_SELF_ANTIDUP_COUNT,
        include_account_summary=True,
    )
    bench_block = format_historical_x_posts_for_prompt(
        bench_posts,
        recent_count=HISTORICAL_BENCHMARK_COUNT,
        include_account_summary=True,
    )
    return (
        "<target_account_posted_history>\n"
        f"【严格防重区 — 仅当前发帖账号 @{cur_sn or cur_alias} 近期已发推文；凡与本区内同事件/同钱包/同波操作重合的候选必须淘汰】\n"
        + self_block.strip()
        + "\n</target_account_posted_history>\n\n"
        + "<cross_account_viral_benchmarks>\n"
        "【爆款风格与写法标杆 — 来自对照账号高浏览代表作；仅供提炼标题钩子、口吻与长度标尺，严禁将其作为本号撞题淘汰依据】\n"
        + bench_block.strip()
        + "\n</cross_account_viral_benchmarks>"
    )


# ==============================================================================
# 四、强模型决策与双重门禁校验
# ==============================================================================

def _validate_x_selection_payload(data: Dict[str, Any]) -> Optional[str]:
    if not isinstance(data, dict):
        return "返回内容不是合法 JSON 对象"
    if "selected_posts" not in data and "rejected_posts" not in data:
        return "缺少必填字段 selected_posts / rejected_posts"
    selected = data.get("selected_posts")
    if selected is not None and not isinstance(selected, (list, dict)):
        return "selected_posts 必须是数组或对象"
    return None


def select_x_posts_via_llm(
    account: Dict[str, Any],
    historical_block: str,
    candidate_prompt_list: List[Dict[str, Any]],
    max_retries: int = AI_MAX_RETRIES,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    通过统一 LLM 网关 (`model=None` + `preset_model_group="medium"`) 执行结构化选帖与改写，
    返回 `(parsed_obj, llm_meta)`，忠实保留 `model_used`。
    """
    prompt_path = Path(PROMPT_TEMPLATE_PATH)
    if not prompt_path.is_file():
        raise FileNotFoundError(f"未找到提示词模板文件: {PROMPT_TEMPLATE_PATH}")

    candidate_block = (
        f"以下是我待筛选的候选帖子列表（共 {len(candidate_prompt_list)} 篇，已按原帖热度与新鲜度预排）：\n"
        + json.dumps(candidate_prompt_list, ensure_ascii=False)
    )
    full_prompt, prompt_meta = render_prompt_template(
        prompt_path,
        {
            "HISTORICAL_X_POSTS": historical_block.strip(),
            "CANDIDATE_POSTS": candidate_block.strip(),
        },
    )

    logger.info(
        f"🤖 [AI决策/启动] 正在调度模型组 [{MODEL_PRESET_GROUP}] 审查候选池 | "
        f"发帖账号: [@{account['screen_name']}] | 候选帖数: [{len(candidate_prompt_list)}] 篇"
    )

    parsed_obj, llm_meta = call_llm_json(
        prompt=full_prompt,
        prompt_meta=prompt_meta,
        model=None,
        preset_model_group=MODEL_PRESET_GROUP,
        timeout=AI_TIMEOUT_SECONDS,
        validator=_validate_x_selection_payload,
        schema_retries=max(1, int(max_retries)),
        retry_times=2,
        task_label="X选帖",
        logger_obj=logger,
    )
    if parsed_obj:
        logger.info(
            f"✅ [AI决策/完成] 结构化决策成功 | 模型: [{llm_meta.get('model_used')}] "
            f"| 耗时: {llm_meta.get('duration_seconds', 0):.2f}s"
        )
        return parsed_obj, llm_meta

    logger.error(
        f"❌ [AI决策/失败] 模型组 [{MODEL_PRESET_GROUP}] 未返回有效决策: {llm_meta.get('error') or llm_meta.get('error_history')}"
    )
    return {}, llm_meta


def query_ai_for_structured_selection(
    account: Dict[str, Any],
    historical_block: str,
    candidate_prompt_list: List[Dict[str, Any]],
    max_retries: int = AI_MAX_RETRIES,
) -> Dict[str, Any]:
    """兼容封装：仅返回决策 JSON 字典。"""
    parsed_obj, _ = select_x_posts_via_llm(
        account=account,
        historical_block=historical_block,
        candidate_prompt_list=candidate_prompt_list,
        max_retries=max_retries,
    )
    return parsed_obj


def validate_double_gates(selected_item: Dict[str, Any]) -> Tuple[bool, str, int, int]:
    """
    双重门禁校验：
    1. 等级门禁（Grade Gate）：必须在 ALLOWED_GRADES（默认仅 S 级）内；
    2. 字数门禁（Length Gate）：必须满足纯汉字 <= MAX_X_RAW_CHARS (140) 且 X计数单位 <= MAX_X_UNITS (280)。
    返回: (通过与否, 拦截/放行原因说明, 纯字符数, X计数单位)
    """
    cand_id = str(selected_item.get("id") or "").strip()
    grade = str(selected_item.get("grade") or "").strip().upper()
    final_text = str(selected_item.get("final_x_text") or "").strip()

    # 门禁 1：等级校验
    normalized_allowed = [g.upper() for g in ALLOWED_GRADES]
    if grade not in normalized_allowed:
        reason = f"等级未达标：评级为 [{grade}]，当前门禁仅允许 {normalized_allowed}"
        return False, reason, len(final_text), 0

    if not final_text:
        return False, "改写文案为空", 0, 0

    # 门禁 2：字数与字符计数单位校验
    raw_chars = len(final_text)
    x_units = calculate_x_character_units(final_text)

    if STRICT_X_LENGTH_GATE:
        if raw_chars > MAX_X_RAW_CHARS:
            reason = f"字数超限：纯中文字数 {raw_chars} 字超过上限 {MAX_X_RAW_CHARS} 字"
            return False, reason, raw_chars, x_units
        if x_units > MAX_X_UNITS:
            reason = f"X字符单位超限：推文折算单位 {x_units} 超过上限 {MAX_X_UNITS} 单位"
            return False, reason, raw_chars, x_units

    return True, "通过双重门禁检验", raw_chars, x_units


def resolve_candidate_images(candidate_doc: Dict[str, Any], ai_selected_keys: List[str]) -> List[str]:
    """
    从候选文章中根据图片编号解析出本地存在的真实高清图片路径（或备用网络URL）。
    """
    info = candidate_doc.get("article_info") or {}
    img_mapping = info.get("image_mapping") or {}
    keys = ai_selected_keys or info.get("selected_images") or []

    resolved_paths = []
    for k in keys:
        k_str = str(k).strip()
        if k_str.isdigit():
            k_str = f"[IMAGE_{k_str}]"
        entry = img_mapping.get(k_str) or img_mapping.get(k)
        if isinstance(entry, dict):
            local_p = entry.get("local_path")
            orig_u = entry.get("original_url")
            if local_p and os.path.isfile(local_p):
                resolved_paths.append(local_p)
            elif orig_u:
                resolved_paths.append(orig_u)

    # 兜底：若指定的编号未能解析到，按顺序取 mapping 中的前 4 张有效图片
    if not resolved_paths and img_mapping:
        for entry in img_mapping.values():
            if isinstance(entry, dict):
                local_p = entry.get("local_path")
                orig_u = entry.get("original_url")
                if local_p and os.path.isfile(local_p):
                    resolved_paths.append(local_p)
                elif orig_u:
                    resolved_paths.append(orig_u)
                if len(resolved_paths) >= 4:
                    break

    return resolved_paths[:4]


# ==============================================================================
# 五、执行发帖、状态回写与显式告警
# ==============================================================================

def execute_publish_and_update_state(
    account: Dict[str, Any],
    candidate_doc: Dict[str, Any],
    selected_item: Dict[str, Any],
    image_paths: List[str],
    raw_chars: int,
    x_units: int,
    state: Dict[str, Any],
    dry_run: bool = False,
    model_used: str = "",
    browser_visible: bool = BROWSER_VISIBLE,
) -> bool:
    """
    调用 Playwright 发帖，检查返回状态，显式打印 Banner 日志，并幂等回写至数据库与本地状态。
    """
    alias = account["alias"]
    screen_name = account["screen_name"]
    session_dir = account["session_dir"]
    cand_id = str(selected_item.get("id") or "").strip()
    grade = str(selected_item.get("grade") or "").strip().upper()
    reason = str(selected_item.get("decision_reason") or "").strip()
    final_text = str(selected_item.get("final_x_text") or "").strip()
    bait_comments = [str(c).strip() for c in (selected_item.get("bait_comments") or []) if str(c).strip()]

    if dry_run:
        logger.info(
            f"\n{'*' * 85}\n"
            f"🔍 [DRY-RUN 模拟模式] 账号 [@{screen_name}] 拟发推文（已通过双重门禁，未实际调用浏览器）：\n"
            f"候选ID: [{cand_id}] | 评级: [{grade}] | 字数: [{raw_chars}字 / {x_units} X单位]\n"
            f"有效配图: [{len(image_paths)}张] -> {image_paths}\n"
            f"入选理由: {reason}\n"
            f"一楼神评备选: {bait_comments}\n"
            f"最终正文:\n{final_text}\n"
            f"{'*' * 85}"
        )
        return True

    if not os.path.isdir(session_dir):
        logger.error(
            "\n" + "!" * 90 + "\n"
            f"❌❌❌ [环境缺失告警] 账号 [@{screen_name}] 浏览器目录不存在: {session_dir}\n"
            f"请先通过登录工具或手动登录生成此会话目录后方可发帖！\n"
            + "!" * 90 + "\n"
        )
        return False

    logger.info(
        f"\n{'=' * 85}\n"
        f"🚀 [发帖启动] 准备向 X (Twitter) 发布优质候选帖\n"
        f"账号: [@{screen_name}] | 候选ID: [{cand_id}] | 评级: [{grade}]\n"
        f"字数: [{raw_chars} 字 / {x_units} X单位] | 配图: [{len(image_paths)} 张]\n"
        f"决策理由: {reason}\n"
        f"发布正文:\n{final_text}\n"
        f"{'=' * 85}"
    )

    # 调用底层自动化发帖入口
    err_msg, success, tweet_id = create_x_post(
        content=final_text,
        image_path_list=image_paths,
        user_data_dir=session_dir,
        debug=browser_visible,
    )

    now_ts = time.time()
    now_str = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")

    # 1. 显式日志反馈
    if success:
        logger.info(
            "\n" + "=" * 90 + "\n"
            f"🎉🎉🎉 [X发帖成功] 账号 [@{screen_name}] 发推成功！\n"
            f"Tweet ID: {tweet_id or '已发出'}\n"
            f"候选ID: {cand_id} | 评级: {grade}\n"
            f"一楼神评备选: {bait_comments}\n"
            f"发布时间: {now_str}\n"
            + "=" * 90 + "\n"
        )
    else:
        logger.error(
            "\n" + "!" * 90 + "\n"
            f"❌❌❌ [X发帖失败告警] 账号 [@{screen_name}] 发布推文未确认成功！\n"
            f"候选ID: [{cand_id}] | 评级: [{grade}]\n"
            f"失败原因: {err_msg}\n"
            f"排查建议: 检查浏览器窗口是否有风控挑战、Cookie 是否过期，或网络代理是否正常。\n"
            + "!" * 90 + "\n"
        )

    # 2. 数据库状态回写（保证幂等，不重复发送）
    try:
        article_manager = GeneratedArticleManager(gen_db_object())
        article_id = candidate_doc.get("article_id")
        plat_record = {
            "status": "success" if success else "failed",
            "published_by": screen_name,
            "account_alias": alias,
            "publish_time": now_ts,
            "publish_time_str": now_str,
            "post_id": str(tweet_id) if tweet_id else "",
            "publish_text": final_text,
            "grade": grade,
            "selection_reason": reason,
            "model_used": str(model_used or ""),
            "raw_chars": raw_chars,
            "x_units": x_units,
            "bait_comments": bait_comments,
            "last_error": err_msg if not success else None,
        }
        if article_id and hasattr(article_manager, "db") and hasattr(article_manager.db, "get_collection"):
            col = article_manager.db.get_collection(article_manager.collection_name)
            col.update_one(
                {"article_id": article_id},
                {
                    "$set": {
                        f"publish_records.{PLATFORM_X}": plat_record,
                        "updated_at": datetime.now(timezone.utc),
                    }
                },
            )
            logger.info(f"[数据回写] 文章 [{article_id}] 的 X 平台发布状态已同步入库")
    except Exception as e:
        logger.error(f"❌ [数据回写/异常] 同步发布状态至 MongoDB 失败: {e}")

    # 3. 本地账号状态文件更新
    acc_state = state.setdefault(alias, {})
    if model_used:
        acc_state["last_model_used"] = str(model_used)
    if success:
        acc_state["last_publish_time"] = now_ts
        acc_state["last_publish_time_str"] = now_str
        acc_state["last_tweet_id"] = str(tweet_id)
        acc_state["last_candidate_id"] = cand_id
        acc_state["publish_count"] = int(acc_state.get("publish_count") or 0) + 1
        acc_state["last_error"] = None
    else:
        acc_state["last_attempt_time"] = now_ts
        acc_state["last_error"] = str(err_msg)
    save_publisher_state(state)

    return success


# ==============================================================================
# 六、单轮巡检与主循环总控
# ==============================================================================

def run_single_check_cycle(
    state: Dict[str, Any],
    dry_run: bool = False,
    *,
    force: bool = False,
    cooldown_seconds: float = ACCOUNT_POST_COOLDOWN_SECONDS,
    candidate_hours: int = CANDIDATE_LOOKBACK_HOURS,
    browser_visible: bool = BROWSER_VISIBLE,
) -> Dict[str, Any]:
    """
    单轮巡检：依次遍历配置的目标账号，判定冷却与候选发布。
    若全部账号均在冷却中，返回精确 `next_sleep_seconds` 避免轮询空转刷屏日志。
    """
    cooldown_waits: List[Tuple[str, float]] = []
    ready_count = 0
    published_any = False

    for account in TARGET_ACCOUNTS:
        screen_name = account["screen_name"]

        # 1. 冷却门禁校验（强制执行账号发帖冷却，规避同号短时密集连发触发 AuthorDiversityScorer 降权）
        in_cooldown, remaining_sec = is_account_in_cooldown(account, state, cooldown_seconds=cooldown_seconds)
        if in_cooldown and not dry_run and not force:
            cooldown_waits.append((screen_name, remaining_sec))
            continue

        ready_count += 1
        logger.info(f"🎯 [账号就绪] 账号 [@{screen_name}] 冷却完毕，开始执行选帖与发布检查")

        # 2. 拉取未发布的精品候选帖并按热度与新鲜度确定性截取送审 Top-N
        candidates = fetch_unposted_viral_candidates(hours=candidate_hours, limit=CANDIDATE_QUERY_LIMIT)
        if not candidates:
            logger.info(f"ℹ️ [候选池为空] 近 {candidate_hours}h 暂无待发布的可用候选文章，跳过本轮")
            continue

        ranked_candidates = rank_and_trim_x_candidates(candidates, max_candidates=MAX_LLM_CANDIDATES)
        prompt_candidates, candidates_map = build_candidate_prompt_data(ranked_candidates)
        if not prompt_candidates:
            logger.info("ℹ️ [候选池为空] 有效清洗后候选数量为 0，跳过本轮")
            continue

        # 3. 获取客观历史战报
        historical_block = build_account_historical_posts_block(account, count=HISTORICAL_REF_COUNT)

        # 4. 结构化选帖决策
        ai_res, llm_meta = select_x_posts_via_llm(account, historical_block, prompt_candidates)
        if not ai_res:
            logger.warning(f"⚠️ [决策跳过] 模型组未返回有效决策，账号 [@{screen_name}] 等待下轮巡检")
            continue

        model_used = str(llm_meta.get("model_used") or "")
        selected_posts = ai_res.get("selected_posts") or []
        if isinstance(selected_posts, dict):
            selected_posts = [selected_posts]

        if not selected_posts:
            rejected = ai_res.get("rejected_posts") or []
            logger.info(
                f"ℹ️ [宁缺毋滥] AI 判定本批 {len(prompt_candidates)} 篇候选均未达到高潜发布标准 | "
                f"入选: 0 篇 | 淘汰: {len(rejected)} 篇 | 账号 [@{screen_name}] 本轮不发推"
            )
            continue

        # 5. 逐一应用双重门禁（仅限 ALLOW_GRADES 且 140 原字 / 280 X单位）
        selected_posts = sorted(
            selected_posts,
            key=lambda item: 0 if isinstance(item, dict) and str(item.get("grade") or "").strip().upper() == "S" else 1,
        )
        published_this_round = False
        for sel_item in selected_posts:
            cand_id = str(sel_item.get("id") or "").strip()
            grade = str(sel_item.get("grade") or "").strip().upper()

            is_valid, gate_reason, raw_chars, x_units = validate_double_gates(sel_item)
            if not is_valid:
                logger.info(
                    f"⏭️ [门禁拦截] 候选 [{cand_id}] 未通过门禁 | 评级: [{grade}] | "
                    f"原因: {gate_reason} | 放弃发送该篇"
                )
                continue

            matched_doc = candidates_map.get(cand_id)
            if not matched_doc:
                logger.warning(f"⚠️ [素材匹配] 未能在候选池映射中找到 ID [{cand_id}] 的原始文档，尝试下一篇")
                continue

            image_paths = resolve_candidate_images(matched_doc, sel_item.get("selected_images") or [])

            pub_ok = execute_publish_and_update_state(
                account=account,
                candidate_doc=matched_doc,
                selected_item=sel_item,
                image_paths=image_paths,
                raw_chars=raw_chars,
                x_units=x_units,
                state=state,
                dry_run=dry_run,
                model_used=model_used,
                browser_visible=browser_visible,
            )
            if pub_ok:
                published_this_round = True
                published_any = True
                break

        if not published_this_round:
            logger.info(f"ℹ️ [本轮结束] 账号 [@{screen_name}] 本轮候选均未通过最终发帖门禁，静默等待下轮")

    if ready_count == 0 and cooldown_waits:
        min_sn, min_rem = min(cooldown_waits, key=lambda item: item[1])
        next_bj = (datetime.now(timezone(timedelta(hours=8))) + timedelta(seconds=min_rem + 2.0)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        rem_min = max(1, int((min_rem + 59) // 60))
        logger.info(
            f"⏳ [冷却休眠] 全部账号处于发帖冷却中（最早就绪 @{min_sn}，约 {rem_min} 分钟后 [{next_bj}] 唤醒）"
        )
        return {"status": "cooldown", "next_sleep_seconds": max(1.0, float(min_rem) + 2.0)}

    if cooldown_waits:
        skipped_desc = ", ".join(f"@{sn}({int(sec // 60)}m)" for sn, sec in cooldown_waits)
        logger.info(f"⏳ [冷却跳过] 以下账号仍在冷却期已跳过: {skipped_desc}")

    return {"status": "published" if published_any else "idle"}


def main() -> None:
    """应用层总控主入口（直接运行进入常驻守护循环；支持命令行参数配置）。"""
    import argparse

    parser = argparse.ArgumentParser(description="X (Twitter) 自动化精选图文发帖总控")
    parser.add_argument("--dry-run", action="store_true", help="模拟测试：执行完整选帖与双重门禁校验，但不实际调用浏览器发帖")
    parser.add_argument("--once", action="store_true", help="单次执行：仅执行单轮巡检后立即退出")
    parser.add_argument("--force", action="store_true", help="忽略账号发布冷却检查强制执行一轮")
    parser.add_argument("--poll-seconds", type=int, default=CHECK_INTERVAL_SECONDS, help="候选为空/未发帖时的巡检休眠间隔(秒)")
    parser.add_argument("--cooldown-hours", type=float, default=ACCOUNT_POST_COOLDOWN_SECONDS / 3600.0, help="同账号成功发帖后的冷却小时数")
    parser.add_argument("--candidate-hours", type=int, default=CANDIDATE_LOOKBACK_HOURS, help="候选文章回溯小时数")
    parser.add_argument("--debug", action="store_true", help="强制开启可见浏览器窗口")
    args, _ = parser.parse_known_args()

    poll_seconds = max(30, int(args.poll_seconds))
    cooldown_seconds = max(0.0, float(args.cooldown_hours) * 3600.0)
    browser_visible = True if args.debug else BROWSER_VISIBLE

    print("\n" + "=" * 90)
    print("🚀 X (Twitter) 全自动精选图文发帖总控启动")
    print(f"- 目标账号清单: {len(TARGET_ACCOUNTS)} 个 {[a['alias'] + '(@' + a['screen_name'] + ')' for a in TARGET_ACCOUNTS]}")
    print(f"- 巡检休眠周期: {poll_seconds} 秒 | 账号冷却: {cooldown_seconds / 3600.0:.2f} 小时 | 回溯窗口: {args.candidate_hours}h")
    print(f"- 精品等级门禁: 仅发送 {ALLOWED_GRADES} 级 | 字数门禁: <={MAX_X_RAW_CHARS} 字 / <={MAX_X_UNITS} X单位")
    print(f"- 运行模式: {'【模拟测试 DRY-RUN】' if args.dry_run else '【正式实盘发帖】'} | 浏览器模式: {'可见窗口' if browser_visible else '后台无头'}")
    print("=" * 90 + "\n")

    state = load_publisher_state()

    def _step(round_idx: int) -> Dict[str, Any]:
        logger.info(f"========== [x_auto_publisher] 开始第 #{round_idx} 轮巡检 ==========")
        return run_single_check_cycle(
            state,
            dry_run=args.dry_run,
            force=args.force,
            cooldown_seconds=cooldown_seconds,
            candidate_hours=args.candidate_hours,
            browser_visible=browser_visible,
        )

    try:
        run_daemon_loop(
            _step,
            interval_seconds=poll_seconds,
            name="x_auto_publisher",
            logger_obj=logger,
            max_rounds=1 if args.once else 0,
        )
    finally:
        save_publisher_state(state)


if __name__ == "__main__":
    main()
