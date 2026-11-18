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

from app.ai_api.model_api import generate_content
from common.common_utils import (
    PLATFORM_X,
    PROMPT_DIR,
    clean_post_text,
    get_config,
    get_runtime_file_path,
    read_file_to_str,
    read_json,
    resolve_session_dir,
    save_json,
    setup_logger,
    string_to_object,
)
from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import GeneratedArticleManager
from platforms.x_api import (
    _format_candidate_posts_literal,
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
ACCOUNT_POST_COOLDOWN_SECONDS: int = 1800   # 每个账号连续发帖的最小冷却间隔（默认至少半小时 = 1800 秒）

# 3. 严格门禁配置（严格精品制）
ALLOWED_GRADES: List[str] = ["S"]           # 允许发布的最高评级（默认仅限 "S" 强烈必发；后续需求大时可改为 ["S", "A"]）
MAX_X_RAW_CHARS: int = 140                  # 纯汉字数上限（X 平台单条推文标准建议上限）
MAX_X_UNITS: int = 280                      # X 平台标准计数单位上限（半角字符记 1，汉字/Emoji/全角记 2，URL 记 23）
STRICT_X_LENGTH_GATE: bool = True           # 是否启用字符数量代码级二次强制校验

# 4. 候选素材抓取配置（与 x_api 默认保持 100% 一致：近 6h，limit=0 全量拉取，零人为截断）
HISTORICAL_PROMPT_ACCOUNTS: List[str] = get_config("x_prompt_accounts", ["dahao", "myself"])  # 对比账号矩阵（包含大号与发帖号）
CANDIDATE_LOOKBACK_HOURS: int = 6           # 抓取近 6 小时内的全部候选帖（与 x_api 一致）
CANDIDATE_QUERY_LIMIT: int = 0              # 0 表示全量拉取，绝不截断（获取全量 100+ 篇候选）
HISTORICAL_REF_COUNT: int = 50              # 参考历史已发推文数量（跨账号矩阵共 50 条）

# 5. 运行模式与模型调用
BROWSER_VISIBLE: bool = True                # Playwright 发帖是否显示浏览器可见窗口（方便实时观测发帖状态）
MODEL_PRESET_GROUP: str = "high"            # 调用的最强模型组（100% 走 high，使用 gpt-6.1-sol-max 等最强模型）
AI_TIMEOUT_SECONDS: int = 1200              # 模型调用超时阈值（秒），设为 20 分钟 = 1200 秒，确保最强模型（如 gpt-6.1-sol-max 深度推理）充足运算不中断
AI_MAX_RETRIES: int = 3                     # 模型调用失败时的最大重试次数（默认 3 次）
AI_RETRY_DELAY_SECONDS: int = 15            # 重试退避等待基础间隔（秒）
PROMPT_TEMPLATE_PATH: str = str(PROMPT_DIR / "X平台_结构化自动选帖与改写提示词.txt")
STATE_FILE_PATH: str = str(get_runtime_file_path("x_publisher_state.json"))


# ==============================================================================
# 二、字符计算与工具函数
# ==============================================================================

def calculate_x_character_units(text: str) -> int:
    """
    计算推文在 X (Twitter) 规则下的长度计数单位：
    - ASCII 半角字符（英文、数字、空格、英文字符）：计 1
    - CJK 汉字、全角标点、Emoji 表情：计 2
    - URL 链接（http:// 或 https:// 开头）：统一按 23 单位折算
    """
    if not text:
        return 0
    url_pattern = re.compile(r"https?://\S+")
    urls = url_pattern.findall(text)
    clean_text = url_pattern.sub("", text)

    units = len(urls) * 23
    for ch in clean_text:
        units += 1 if ord(ch) <= 127 else 2
    return units


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


def is_account_in_cooldown(account: Dict[str, Any], state: Dict[str, Any]) -> Tuple[bool, float]:
    """
    检查账号当前是否处于发帖冷却中。
    返回: (是否在冷却中, 剩余冷却秒数)
    """
    alias = account["alias"]
    acc_state = state.get(alias) or {}
    last_pub_time = float(acc_state.get("last_publish_time") or 0.0)
    now = time.time()
    elapsed = now - last_pub_time
    if elapsed < ACCOUNT_POST_COOLDOWN_SECONDS:
        remaining = ACCOUNT_POST_COOLDOWN_SECONDS - elapsed
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


def build_candidate_prompt_data(candidates: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """
    将候选文章列表转换为 Prompt 所需的精简数据结构，并建立快速查找字典。
    返回: (candidate_prompt_list, candidates_map)
    """
    prompt_list = []
    candidates_map = {}

    for art in candidates:
        info = art.get("article_info") or {}
        art_id = str(art.get("article_id") or "").strip()
        p_list = art.get("post_id_list") or []
        src_pid = str(p_list[0]).strip() if p_list else ""

        # 优先使用 article_id 作为标识，同时 post_id_list 亦做索引映射
        primary_id = art_id or src_pid
        if not primary_id:
            continue

        candidates_map[primary_id] = art
        if art_id:
            candidates_map[art_id] = art
        if src_pid:
            candidates_map[src_pid] = art

        # 【保留过滤原因说明】：纯字段级保留与剔除（保持极简、稳定，绝不搞任何脆弱的字符串切分或二次逻辑）：
        # 1. 彻底剔除上游 Stage 2 专属思考脚手架（decision_reason 和 image_role，合计 4.5 万字符死重，当前 X 选帖提示词零引用、零依赖）；
        # 2. 其余业务字段（包含 image_semantics 配图语义列表）100% 完整原样保留。
        cleaned_item = {
            "id": primary_id,
            "topic_tag": info.get("topic_tag", ""),
            "viral_score": info.get("viral_score", 0.0),
            "image_semantics": info.get("image_semantics", []),
            "selected_images": info.get("selected_images", []),
            "viral_analysis": info.get("viral_analysis", ""),
            "post_text": info.get("post_text", ""),
            "bait_comments": info.get("bait_comments", []),
        }
        cleaned_item = {k: v for k, v in cleaned_item.items() if v not in (None, "", [], {})}
        prompt_list.append({"remodeled_article": cleaned_item})

    return prompt_list, candidates_map


def build_account_historical_posts_block(account: Dict[str, Any], count: int = HISTORICAL_REF_COUNT) -> str:
    """
    获取对比账号矩阵（默认含 dahao 与 myself）最近 count 条客观已发推文并格式化为 Prompt 区块。
    与 x_api 保持 100% 一致，跨账号合并提取爆款规律。
    """
    target_specs = resolve_x_accounts(HISTORICAL_PROMPT_ACCOUNTS, default_accounts=["dahao", "myself"])

    # 尝试无登录联网刷新最新推文表现；若受限或异常自动回退至本地缓存
    posts_by_acc = None
    try:
        posts_by_acc = fetch_multi_x_user_posts(accounts=target_specs, count=max(count, 50), sort_by="time")
    except Exception as e:
        logger.warning(f"[历史/同步] 实时获取账号矩阵最新推文受阻: {e}，降级使用本地历史缓存")

    if not posts_by_acc:
        posts_by_acc = load_cached_x_posts(accounts=target_specs, sort_by="time")

    selected_posts = merge_x_account_posts(
        posts_by_acc,
        accounts=target_specs,
        total_count=count,
        sort_by="time",
    )
    return format_historical_x_posts_for_prompt(selected_posts, recent_count=count, include_account_summary=True)


# ==============================================================================
# 四、强模型决策与双重门禁校验
# ==============================================================================

def safe_parse_llm_json(raw_content: str) -> Dict[str, Any]:
    """
    鲁棒解析 LLM 返回的 JSON 字典：
    1. 剥离 Markdown 代码块（```json ... ```）；
    2. 剥离变量赋值前缀（如 `texts = ` 或 `result = `）；
    3. 提取最外层匹配的 { ... }；
    4. 采用 strict=False 允许字符串内存在未转义换行/制表符；
    5. 兜底回退 string_to_object 双引擎解析。
    """
    if not isinstance(raw_content, str) or not raw_content.strip():
        return {}

    cleaned = raw_content.strip()
    if "```" in cleaned:
        cleaned = re.sub(r"^```(?:json|python)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)
        cleaned = cleaned.strip()

    assign_match = re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*\s*=\s*", cleaned)
    if assign_match:
        cleaned = cleaned[assign_match.end():].strip()

    start_idx = cleaned.find("{")
    end_idx = cleaned.rfind("}")
    if start_idx != -1 and end_idx > start_idx:
        json_candidate = cleaned[start_idx : end_idx + 1]
    else:
        json_candidate = cleaned

    try:
        obj = json.loads(json_candidate, strict=False)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    try:
        obj = string_to_object(json_candidate)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    try:
        repaired = re.sub(r",\s*([}\]])", r"\1", json_candidate)
        obj = json.loads(repaired, strict=False)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    return {}


def query_ai_for_structured_selection(
    account: Dict[str, Any],
    historical_block: str,
    candidate_prompt_list: List[Dict[str, Any]],
    max_retries: int = AI_MAX_RETRIES,
) -> Dict[str, Any]:
    """
    将客观历史数据与候选帖子装填入结构化提示词，请求强模型输出决策 JSON（失败自动重试机制）。
    """
    if not os.path.exists(PROMPT_TEMPLATE_PATH):
        raise FileNotFoundError(f"未找到提示词模板文件: {PROMPT_TEMPLATE_PATH}")

    template_str = read_file_to_str(PROMPT_TEMPLATE_PATH)
    candidate_block = "以下是我待筛选的候选帖子列表：\n" + _format_candidate_posts_literal(candidate_prompt_list)

    full_prompt = template_str.replace("{HISTORICAL_X_POSTS}", historical_block.strip()).replace(
        "{CANDIDATE_POSTS}", candidate_block.strip()
    )

    logger.info(
        f"🤖 [AI决策/启动] 正在调度强模型组 [{MODEL_PRESET_GROUP}] 审查候选池 | "
        f"发帖账号: [@{account['screen_name']}] | 候选帖数: [{len(candidate_prompt_list)}] 篇 | 最大重试: [{max_retries}] 次"
    )

    for attempt in range(1, max_retries + 1):
        t0 = time.time()
        res = generate_content(
            prompt=full_prompt,
            preset_model_group=MODEL_PRESET_GROUP,
            timeout=AI_TIMEOUT_SECONDS,
        )
        elapsed = time.time() - t0

        # 极简结果导向：完全不依赖外部状态字段，直接拿结果说话——只要能解析出需要的选帖 JSON，即判定成功
        raw_content = res.get("content", "") if isinstance(res, dict) else str(res or "")
        parsed_obj = safe_parse_llm_json(raw_content)

        if isinstance(parsed_obj, dict) and ("selected_posts" in parsed_obj or "rejected_posts" in parsed_obj):
            logger.info(f"✅ [AI决策/完成] 结构化决策返回成功 | 尝试: [第{attempt}次] | 耗时: {elapsed:.2f}s")
            return parsed_obj

        err_detail = res.get("error_history") if isinstance(res, dict) and res.get("error_history") else "未能解析出符合规范的选帖JSON"
        logger.warning(f"⚠️ [AI决策/重试] 强模型组 [{MODEL_PRESET_GROUP}] 第 {attempt}/{max_retries} 次未获得有效决策: {err_detail}")

        if attempt < max_retries:
            wait_sec = AI_RETRY_DELAY_SECONDS * attempt
            logger.info(f"⏳ [AI重试等待] 将在 {wait_sec} 秒后自动发起第 {attempt + 1} 次重试...")
            time.sleep(wait_sec)

    logger.error(f"❌ [AI决策/耗尽] 连续 {max_retries} 次调用强模型组 [{MODEL_PRESET_GROUP}] 均未确认成功，本轮放弃")
    return {}


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

    if dry_run:
        logger.info(
            f"\n{'*' * 85}\n"
            f"🔍 [DRY-RUN 模拟模式] 账号 [@{screen_name}] 拟发推文（已通过双重门禁，未实际调用浏览器）：\n"
            f"候选ID: [{cand_id}] | 评级: [{grade}] | 字数: [{raw_chars}字 / {x_units} X单位]\n"
            f"有效配图: [{len(image_paths)}张] -> {image_paths}\n"
            f"入选理由: {reason}\n"
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
        debug=BROWSER_VISIBLE,
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
            "raw_chars": raw_chars,
            "x_units": x_units,
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

def run_single_check_cycle(state: Dict[str, Any], dry_run: bool = False) -> None:
    """
    单轮巡检：依次遍历配置的目标账号，判定冷却与候选发布。
    """
    for account in TARGET_ACCOUNTS:
        alias = account["alias"]
        screen_name = account["screen_name"]

        # 1. 冷却门禁校验
        in_cooldown, remaining_sec = is_account_in_cooldown(account, state)
        # if in_cooldown and not dry_run:
        #     rem_min = max(1, int(remaining_sec // 60))
        #     logger.info(f"⏳ [冷却跳过] 账号 [@{screen_name}] 处于发帖冷却中（还需等待约 {rem_min} 分钟）")
        #     continue

        logger.info(f"🎯 [账号就绪] 账号 [@{screen_name}] 冷却完毕，开始执行选帖与发布检查")

        # 2. 拉取未发布的精品候选帖
        candidates = fetch_unposted_viral_candidates(hours=CANDIDATE_LOOKBACK_HOURS, limit=CANDIDATE_QUERY_LIMIT)
        if not candidates:
            logger.info(f"ℹ️ [候选池为空] 近 {CANDIDATE_LOOKBACK_HOURS}h 暂无待发布的可用候选文章，跳过本轮")
            continue

        prompt_candidates, candidates_map = build_candidate_prompt_data(candidates)
        if not prompt_candidates:
            logger.info("ℹ️ [候选池为空] 有效清洗后候选数量为 0，跳过本轮")
            continue

        # 3. 获取客观历史战报
        historical_block = build_account_historical_posts_block(account, count=HISTORICAL_REF_COUNT)

        # 4. 强模型结构化决策
        ai_res = query_ai_for_structured_selection(account, historical_block, prompt_candidates)
        if not ai_res:
            logger.warning(f"⚠️ [决策跳过] 强模型未返回有效决策，账号 [@{screen_name}] 等待下轮巡检")
            continue

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

        # 5. 逐一应用双重门禁（优先审核排在最前的高潜帖）
        published_this_round = False
        for rank_idx, sel_item in enumerate(selected_posts, 1):
            cand_id = str(sel_item.get("id") or "").strip()
            grade = str(sel_item.get("grade") or "").strip().upper()

            # 双重门禁校验
            is_valid, gate_reason, raw_chars, x_units = validate_double_gates(sel_item)
            if not is_valid:
                logger.info(
                    f"⏭️ [门禁拦截] 候选 [{cand_id}] 未通过门禁 | 评级: [{grade}] | "
                    f"原因: {gate_reason} | 放弃发送该篇"
                )
                continue

            # 匹配真实数据库文档
            matched_doc = candidates_map.get(cand_id)
            if not matched_doc:
                logger.warning(f"⚠️ [素材匹配] 未能在候选池映射中找到 ID [{cand_id}] 的原始文档，尝试下一篇")
                continue

            # 解析本地真实配图
            image_paths = resolve_candidate_images(matched_doc, sel_item.get("selected_images") or [])

            # 6. 执行正式发布
            pub_ok = execute_publish_and_update_state(
                account=account,
                candidate_doc=matched_doc,
                selected_item=sel_item,
                image_paths=image_paths,
                raw_chars=raw_chars,
                x_units=x_units,
                state=state,
                dry_run=dry_run,
            )
            if pub_ok:
                published_this_round = True
                break  # 账号单轮仅发布 1 篇最高评级精品帖，随后进入半小时冷却

        if not published_this_round:
            logger.info(f"ℹ️ [本轮结束] 账号 [@{screen_name}] 本轮候选均未通过最终发帖门禁，静默等待下轮")


def main() -> None:
    """应用层总控主入口（在 IDE 中直接点击绿色三角运行即可常驻无尽循环；亦支持命令行参数）"""
    import argparse
    parser = argparse.ArgumentParser(description="X (Twitter) 自动化精选图文发帖总控")
    parser.add_argument("--dry-run", action="store_true", help="模拟测试：执行完整选帖与双重门禁校验，但不实际调用浏览器发帖")
    parser.add_argument("--once", action="store_true", help="单次执行：仅执行单轮巡检后立即退出（不进入循环休眠）")
    args, _ = parser.parse_known_args()

    print("\n" + "=" * 90)
    print("🚀 X (Twitter) 全自动精选图文发帖总控启动")
    print(f"- 目标账号清单: {len(TARGET_ACCOUNTS)} 个 {[a['alias'] + '(@' + a['screen_name'] + ')' for a in TARGET_ACCOUNTS]}")
    print(f"- 巡检休眠周期: 每 {CHECK_INTERVAL_SECONDS} 秒 (约 {int(CHECK_INTERVAL_SECONDS // 60)} 分钟) 检查一轮")
    print(f"- 账号发帖冷却: 每篇发布后强制冷却 {ACCOUNT_POST_COOLDOWN_SECONDS} 秒 (约 {int(ACCOUNT_POST_COOLDOWN_SECONDS // 60)} 分钟)")
    print(f"- 精品等级门禁: 仅发送评级为 {ALLOWED_GRADES} 的内容（非精品坚决不发）")
    print(f"- 字符限制门禁: 纯汉字上限 {MAX_X_RAW_CHARS} 字，标准计算单位上限 {MAX_X_UNITS} 单位")
    print(f"- 运行模式: {'【模拟测试 DRY-RUN】' if args.dry_run else '【正式实盘发帖】'} | 浏览器模式: {'可见窗口 (Debug)' if BROWSER_VISIBLE else '后台无头 (Headless)'}")
    print("=" * 90 + "\n")

    state = load_publisher_state()

    try:
        cycle_count = 1
        while True:
            logger.info(f"\n{'#' * 40} 开始第 【{cycle_count}】 轮巡检 {'#' * 40}")
            try:
                run_single_check_cycle(state, dry_run=args.dry_run)
            except Exception as e:
                logger.exception(f"❌ [主循环/异常] 本轮巡检发生未捕获异常: {e}，将在下次循环中自动自愈重试")

            if args.once:
                logger.info("🏁 已开启 --once 参数，单轮巡检执行完毕，正常退出。")
                break

            logger.info(f"💤 第 【{cycle_count}】 轮巡检结束，进入休眠，将在 {CHECK_INTERVAL_SECONDS} 秒后启动下一轮...")
            cycle_count += 1
            time.sleep(CHECK_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("\n🛑 收到人工中断信号 (Ctrl+C)，X 发帖总控已安全退出。")
        save_publisher_state(state)


if __name__ == "__main__":
    main()
