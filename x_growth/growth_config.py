# -*- coding: utf-8 -*-
"""
X 平台极简低耦合评论系统配置与纯文本公共工具 (`x_growth/growth_config.py`)

职责物理隔离（高内聚 · 低耦合）：
1. 模块 1A `x_browser_collector.py`（纯拉取落库，零大模型依赖）：
   启动真实桌面 Chrome 刷推荐流 + 点 3 条热帖 + 逛最多 6 个 100~3000 粉蓝 V 主页，顺路所有主帖与热评 100% 存入 `harvested_social_posts`。
2. 模块 1B `comment_generator.py`（纯查库生成打分，零浏览器依赖）：
   只查 `harvested_social_posts` 中 `<24h + 蓝V + 100~3000粉 + 英/日 + 非视频 + 未打分` 的合格主帖，
   按 **15 条/波**（内部 5 帖/批 `low` 生成 + 独立 `medium` 盲测打分 + 当波即刻落库）循环执行，直至库内待评合格帖 **100% 清零**。
3. 串联入口 `comment_pipeline.py`（薄编排层）：
   常驻定时顺序调用「1A 拉取落库 -> 1B 循环清零生成」，一键无人值守。
4. 模块 2 `comment_publisher.py`（多账号消费发评 & 单次留痕锁帖）：
   各运营账号按自身语种、赛道、最低分查询 `consumed != True` 的公共帖子，发评成功后回写 `consumed=True`、`consumed_by` 与 `consumed_at`。
"""
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from common.common_utils import PROMPT_DIR, get_config, resolve_session_dir, setup_logger
from platforms.harvesters import NEW_HARVESTED_COLLECTION_NAME

TZ_CN = timezone(timedelta(hours=8))

X_GROWTH_PROMPT_DIR: Path = PROMPT_DIR / "x_growth"
X_GROWTH_PROMPT_DIR.mkdir(parents=True, exist_ok=True)

# 唯一复用的帖子集合名，绝不新建额外数据表；所有评论生产与消费状态统一收纳在文档内 `auto_comment` 字段下
POSTS_COLLECTION_NAME: str = NEW_HARVESTED_COLLECTION_NAME  # "harvested_social_posts"
AUTO_COMMENT_FIELD: str = "auto_comment"

# 仅针对经过验证的高收益语种区（英语 en、日语 ja）生成与发布同语种评论
SUPPORTED_COMMENT_LANGUAGES: Tuple[str, ...] = ("en", "ja")

# 预定义通用一级帖子分类体系（生成时单选其一，发布端各账号可按自身定位自由筛选）
POST_CATEGORIES: Dict[str, str] = {
    "tech_ai": "人工智能、编程开发、科技数码、互联网产品",
    "crypto_web3": "加密资产、区块链、链上交易、Web3",
    "finance_macro": "股票市场、宏观经济、商业投资、创业经营",
    "news_politics": "国际时事、社会热点、政策法规、公共议题",
    "gaming_anime": "电子游戏、动漫二次元、电竞赛事",
    "entertainment_pop": "影视音乐、明星网红、流行文化、幽默热梗",
    "sports_fitness": "体育赛事、竞技运动、健身健康",
    "lifestyle_emotion": "日常生活、情感共鸣、职场成长、旅行美食",
    "science_culture": "自然科学、硬核科普、历史人文、阅读哲思",
    "other": "不属于以上领域的其他综合内容",
}


def format_category_list_for_prompt() -> str:
    """将预定义分类体系渲染为提示词枚举列表。"""
    return "\n".join(f"- `{key}`：{desc}" for key, desc in POST_CATEGORIES.items())


def normalize_language_code(lang: str) -> str:
    """归一化语言标识为简洁小写 ISO-639-1 主码（如 zh-CN -> zh, en-US -> en）。"""
    raw = str(lang or "").strip().lower().replace("_", "-")
    if not raw:
        return "unknown"
    base = raw.split("-", 1)[0].strip()
    return base or "unknown"


def get_configured_self_handles() -> List[str]:
    """收集 config.json 中记录的自有账号名（小写），避免给自己的账号生成或发送评论。"""
    handles: List[str] = []
    for key in ("x_xiaohao_screen_name", "x_myself_screen_name", "x_dahao_screen_name"):
        val = str(get_config(key, "") or "").strip().lstrip("@").lower()
        if val and val not in handles:
            handles.append(val)
    return handles


def now_iso_cn() -> str:
    return datetime.now(TZ_CN).isoformat(timespec="seconds")


_LOGGERS: Dict[str, logging.Logger] = {}


def get_growth_logger(app_name: str = "x_growth") -> logging.Logger:
    logger_obj = _LOGGERS.get(app_name)
    if logger_obj is None:
        logger_obj = setup_logger(app_name=app_name)
        _LOGGERS[app_name] = logger_obj
    return logger_obj


# ==============================================================================
# 一、模块 1A：纯拉取落库配置（直接右键运行 x_browser_collector.py 即生效）
# 注意：绑定的登录目录请勿再让裸 HTTP 爬虫提取 Cookie 共用！
# ==============================================================================
COLLECTOR_DEFAULTS: Dict[str, Any] = {
    "user_data_dir": resolve_session_dir("x_xiaohao_browser_session_dir", "x_xiaohao"),  # 用于浏览采集的已登录目录
    "continuous_loop": False,           # 单独运行 x_browser_collector.py 时：False=拉取一轮即退出；True=常驻定时拉取
    "interval_seconds": 2400,           # 单独开启常驻拉取时的基础间隔秒数（40 分钟）
    "jitter_seconds": (120, 360),       # 随机正抖动秒数（2~6 分钟）
    "browser_visible": False,           # False=启用Chrome新版--headless=new真无头(保留显卡指纹+零桌面窗口+零抢焦点)；True=显示窗口
    "home_scroll_rounds": 6,            # 首页 For You 拟人慢速滚屏次数（顺路收集更多大V与热帖）
    "thread_clicks_per_run": 4,         # 顺路点开几条英/日热帖看评论区（顺便收热评 + 发现更多评论区小蓝V）
    "profile_visits_per_run": 9,        # 顺路点进几个 100~3000 粉蓝 V 主页收集原创主帖
    "min_followers": 100,               # 主页访问目标最小粉丝数
    "max_followers": 3000,              # 主页访问目标最大粉丝数（严守同体量高回关率蓝V）
    "post_max_age_seconds": 86400,      # 近期主帖参考时间窗（24h）
    "author_avoid_seconds": 43200,      # 12h 内已拉取过帖子或访问过主页的目标作者避让（兼顾作者分布均匀与日更博主新帖捕捉）
    "max_posts_per_author_per_fetch": 3,# 单次拉取同一目标作者最多保留最新几条原创主帖（不增加额外请求提高单主页产出）
    "avoid_connected_authors": True,    # 选人进主页时直接避让已关注/互关作者，把主页访问名额留给未触达新作者
    "allowed_languages": SUPPORTED_COMMENT_LANGUAGES,
}

# ==============================================================================
# 二、模块 1B：纯查库循环生成与盲测打分配置（直接右键运行 comment_generator.py 即生效）
# 零浏览器依赖：每次取 15 条生成并落库，自动循环多波直至库内合格待评帖 100% 清零！
# ==============================================================================
GENERATOR_DEFAULTS: Dict[str, Any] = {
    "continuous_loop": False,           # 单独运行 comment_generator.py 时：False=把当前库存一次性清零后退出；True=常驻轮询清零
    "interval_seconds": 900,            # 单独开启常驻生成时的轮询间隔秒数（15 分钟）
    "jitter_seconds": (30, 120),
    "min_followers": 100,               # 目标发帖作者最小粉丝数
    "max_followers": 3000,              # 目标发帖作者最大粉丝数
    "post_max_age_seconds": 86400,      # 仅对最近 24h 内发布的新鲜主帖生产评论
    "allowed_languages": SUPPORTED_COMMENT_LANGUAGES,  # 仅针对英语(en)与日语(ja)帖子生成同语种评论
    "wave_size": 20,                    # 每波取 20 条合格未评帖处理并落库，若库内还有剩余则当场自动继续下一波直至清零
    "max_waves_per_run": 20,            # 单次运行安全波次熔断上限（20 波 × 20 条 = 400 条，正常 1~2 波即清零）
    "comment_batch_size": 5,            # 每波内部按 5 条/批调用 low 模型生成评论与分类
    "score_batch_size": 6,              # 每波内部按 6 条/批调用独立 medium 模型横向盲测打分
    "generator_model_group": "low",     # 评论生成模型组
    "scorer_model_group": "medium",     # 独立第三方盲测打分模型组（物理隔离严禁自评）
    "max_weighted_chars": 260,          # 单条回复加权字符上限
    # 【保留限流原因说明】评论生成与打分均为短文本批处理，实测网关偶发挂死 300s 会拖垮整轮新鲜度，收紧为 120s 触发快切容灾
    "llm_timeout": 120,
    "force_rescore": False,             # 是否强制重评已有分数的帖子（日常保持 False）
}

# ==============================================================================
# 三、一键串联常驻定时配置（直接右键运行 comment_pipeline.py 即生效：先拉取 -> 再循环清零生成）
# ==============================================================================
PIPELINE_SCHEDULE_CONFIG: Dict[str, Any] = {
    "user_data_dir": COLLECTOR_DEFAULTS["user_data_dir"],
    "continuous_loop": True,            # True=常驻定时循环（每轮先拉帖入库，再把库内合格待评帖100%生成完）；False=跑一轮退出
    "interval_seconds": 2400,           # 基础轮询间隔秒数（2400 秒 = 40 分钟）
    "jitter_seconds": (120, 360),       # 每轮附加随机正抖动（2~6 分钟）
    "force_rescore": False,
    "risk_cooldown_seconds": 21600,     # 遇安全验证或登录失效时自动静默冷却 6 小时，严禁死磕硬撞
}

# 兼容聚合视图
PIPELINE_DEFAULTS: Dict[str, Any] = {**COLLECTOR_DEFAULTS, **GENERATOR_DEFAULTS}

# ==============================================================================
# 四、模块 2：多账号消费发评配置（直接右键运行 comment_publisher.py 即生效）
# 不同运营账号只需按自身定位调整 user_data_dir / allowed_languages / allowed_categories
# ==============================================================================
PUBLISHER_DEFAULTS: Dict[str, Any] = {
    "user_data_dir": resolve_session_dir("x_myself_browser_session_dir", "x_myself"),  # 默认单账号兼容项（@peterzhu1003）
    "account_pool": [                   # 公共评论池消费账号池：严格隔离主号中文币圈人设与英/日泛娱乐养号号
        # 【保留过滤原因说明】线上实测（2026-11-16~19）表明：`x_myself`（@peterzhu1003）同时承担简中加密主帖发布，
        # 挂载 ALL 赛道英/日评论 3 天共发 67 条仅获中位 7 浏览、0 增粉，反而撕裂了 X SimClusters 语种与垂直赛道画像，
        # 导致主号后续 12 条简中加密主帖全被锁死在 24~62 浏览上限。因此默认暂停主号消费英/日非垂直评论池；
        # 仅当接入纯英文/日文专职养号浏览器目录时，将其加入下方并设置 `"enabled": True`。
        {
            "name": "x_myself",
            "user_data_dir": resolve_session_dir("x_myself_browser_session_dir", "x_myself"),
            "enabled": False,
            "allowed_categories": ["crypto_web3"],
            "allowed_languages": ("en",),
        },
    ],
    "continuous_loop": True,            # True=直接运行 comment_publisher.py 即按周期常驻自动发评
    "publish_interval_seconds": 2400,   # 开启常驻发评时的基础间隔秒数（40 分钟，配合每轮 1 条更均匀覆盖全天）
    "publish_jitter_seconds": (120, 420),
    "dry_run": False,                   # True=仅查库预览待发评论；False=启动真实 Chrome 发评并回写消费记录
    "browser_visible": False,           # False=--headless=new 静默运行不抢焦点；True=显示窗口观察发评过程
    "allowed_categories": None,         # None=不限分类；如填 ["tech_ai", "crypto_web3"] 则只消费对应赛道帖子
    "allowed_languages": SUPPORTED_COMMENT_LANGUAGES,  # 英文专号填 ("en",)，日文专号填 ("ja",)，综合号保留 ("en", "ja")
    "min_followers": 100,               # 当前账号只消费粉丝数 >= 该值的作者帖子
    "max_followers": 3000,              # 当前账号只消费粉丝数 <= 该值的作者帖子
    "min_score": 75,                    # 仅消费独立盲测评分 >= 该分值（75+ 自然活人感优质档）的评论
    "max_publish_per_run": 1,           # 单轮仅精挑并发送 1 条最优评论（动作最分散、不扎堆）
    "max_publish_per_day": 24,          # 当前账号 24 小时滚动发评上限（配合每~45分钟发1条，均匀覆盖全天）
    "post_max_age_seconds": 86400,      # 仅消费 24 小时以内的新鲜帖子
    # 【保留过滤原因说明】经 C1~C3 (N=52, 互动率 69.2%) 与 N=68 细分复查：EN 4~9 评仍达 66.7%~80% 互动、JA <=14 评仍有高阅读，
    # 此前拍脑袋砍到 5 属小样本过拟合且误伤正常供给；恢复已验证的 15 上限作防爆楼安全网，低竞争区由 replies 升序自然优先占领
    "max_post_comments": 15,
    # 【保留过滤原因说明】仅在发评前打开详情页、用线上实时数据复核：发布超 2 小时且仍 0 赞的帖子视为停流冷帖，当场跳过（库内旧快照不据此硬删）
    "stale_zero_engagement_seconds": 7200,
    "stale_min_likes": 1,               # 超过上述时长后要求原帖至少获得的点赞数，过滤长期无人问津的停流帖
    "author_dedup_days": 7,             # 当前账号多少天内不对同一作者重复发评
    "avoid_connected_authors": True,    # 当前账号打开推文时若已与该作者关注/互关则本号跳过（不锁帖，其他号仍可用）
    "like_when_commenting": True,       # 发评论时顺手给原帖点赞
    "also_follow": False,               # 发评论时是否顺手关注该作者
    "action_delay_range": (14, 28),     # 多条真实发送之间的随机拟人间隔秒数
    "risk_cooldown_seconds": 21600,     # 遇安全验证或服务端限流拒收时自动静默冷却秒数（6 小时）
    "sync_metrics_after_publish": True, # 每轮发评完成后通过免登接口同步近期已发评论的阅读/点赞等公开指标
    "metrics_lookback_hours": 72,       # 每次自动同步回溯最近多少小时内已消费发布的评论（默认 72 小时）
}
