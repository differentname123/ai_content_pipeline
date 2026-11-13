# -*- coding: utf-8 -*-
"""
多平台内容拉取总控与每小时常驻入库任务 (`harvester_runner.py`)

================================================================================
【使用说明（直接在 IDE 中点击运行即可）】
1. 开关平台：修改下方 `PLATFORM_SWITCHES` 字典（`True` 开启 / `False` 关闭）。
2. 循环模式：默认直接在 IDE 运行本文件即进入「每 1 小时（3600 秒）自动拉取一轮」的常驻循环；
   若某次调试只想单次跑一轮就退出，将 `GLOBAL_SCHEDULE_CONFIG["continuous_loop"]` 改为 `False` 即可。
3. 职责边界：本模块仅负责纯粹的跨平台拉取、基础结构清洗、图片按视觉哈希下载与幂等入库 (`harvested_social_posts`)，
   不做任何主观流量/字数/赞数阈值过滤。
4. 运维日志：统一输出至控制台与 `logs/harvester_runner.log`，仅打印高信息密度的拉取执行状态、
   子通道命中明细、图片/评论拉取完成度与异常告警。
================================================================================
"""
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.common_utils import (
    get_runtime_file_path,
    resolve_session_dir,
    safe_int,
    save_json,
    setup_logger,
)
from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import HarvestedPostManager
from platforms.harvesters import (
    HARVESTER_REGISTRY,
    NEW_HARVESTED_COLLECTION_NAME,
)

TZ_CN = timezone(timedelta(hours=8))


def _configure_clean_harvester_logging() -> logging.Logger:
    """
    统一日志出口至 `logs/harvester_runner.log` 与控制台，
    并抑制底层子模块冗余的过程 INFO 流水日志（仅保留 WARNING/ERROR 异常告警），确保运维日志高密度无噪音。
    """
    runner_logger = setup_logger(app_name="harvester_runner")
    root_logger = logging.getLogger()
    root_logger.handlers = list(runner_logger.handlers)
    root_logger.setLevel(logging.WARNING)

    for sub_mod in (
        "platforms.binance_api",
        "platforms.okx_api",
        "common.mongo_db.mongo_manager",
        "common.mongo_db.mongo_base",
    ):
        logging.getLogger(sub_mod).setLevel(logging.WARNING)

    return runner_logger


logger = _configure_clean_harvester_logging()


# ==============================================================================
# 一、【最上层总控开关】开启 (True) 或 关闭 (False) 对应平台的拉取
# ==============================================================================
PLATFORM_SWITCHES: Dict[str, bool] = {
    "x": True,              # 英文 X (Twitter)：监控垂直大V时间线 + 小号推荐主页流与全量评论
    "reddit": True,         # 英文 Reddit：10大板块单请求聚合 RSS + 外链正文
    "hackernews": True,     # 英文 HackerNews：首页 front_page + 关键词故事 + 外链正文与评论
    "binance": True,        # 中文 币安广场：首页推荐流 + 币种讨论流与评论
    "okx": True,            # 中文 欧易 Orbit：广场推荐流 + 币种讨论流与评论
    "zhihu": False,         # 预留扩展：知乎
    "xiaohongshu": False,   # 预留扩展：小红书
}


# ==============================================================================
# 二、【全局入库与常驻定时调度配置（直接在代码中修改，无需命令行参数）】
# ==============================================================================
GLOBAL_SCHEDULE_CONFIG: Dict[str, Any] = {
    # 是否开启常驻循环拉取（True = 直接运行本文件即每 1 小时循环拉取；False = 仅拉取 1 轮后退出）
    "continuous_loop": True,
    # 每轮拉取间隔（秒）：3600 秒 = 每 1 小时拉取一轮
    "interval_seconds": 3600,
    # 独立的 MongoDB 集合名称
    "mongo_collection_name": NEW_HARVESTED_COLLECTION_NAME,  # "harvested_social_posts"
    # 是否启用 MongoDB 自动入库
    "enable_mongo_upsert": True,
    # 是否并发下载帖子与评论配图到本地 `media_downloads/`
    "download_media": True,
    # 每次运行后在 logs/runtime/ 下保存最新拉取报告与样本快照
    "snapshot_filename": "latest_harvested_posts_snapshot.json",
}


# ==============================================================================
# 三、【各平台拉取范围配置（纯粹拉取与结构归一化，不设任何流量/字数过滤门槛）】
# ==============================================================================
PLATFORM_HARVEST_CONFIGS: Dict[str, Dict[str, Any]] = {
    "x": {
        "burner_user_data_dir": resolve_session_dir("x_xiaohao_browser_session_dir", "x_xiaohao"),
        "burner_cookies": "",
        "include_home_feed": True,        # 小号健康可用时每轮仅拉 1 次 HomeTimeline；冷却中自动静默跳过
        "home_feed_count": 30,
        "include_trending": True,         # 100% 免登全球/美国实时热搜榜 (getdaytrends) -> 免登穿透热搜爆款推文
        "trending_topic_count": 5,
        "tweets_per_trend": 5,
        "kol_handles": [
            "Polymarket",
            "Kalshi",
            "KobeissiLetter",
            "WatcherGuru",
            "Dexerto",
            "wallstreetbets",
            "MarioNawfal",
            "MorningBrew",
            "StockMKTNewz",
            "OpenAI",
            "CoinGlass_com",
            "whale_alert",
            "NoContextHumans",
            "WuBlockchain",
            "arkham",
        ],
        "tweets_per_handle": 5,
        "fetch_comments_for_top_n": 8,    # 最小特权：仅在小号健康时为互动最高的 Top 8 重点帖单线程慢速补评论
        "fetch_external_links": True,
    },
    "reddit": {
        "burner_user_data_dir": resolve_session_dir("x_xiaohao_browser_session_dir", "x_xiaohao"),
        "burner_cookies": "",
        "include_popular": True,          # 同时拉取 Reddit 全站实时热门榜 /r/popular
        "subreddits": [
            "wallstreetbets",
            "stocks",
            "investing",
            "technology",
            "news",
            "worldnews",
            "nvidia",
            "openai",
            "singularity",
            "CryptoCurrency",
            "Bitcoin",
            "FluentInFinance",
            "Fauxmoi",
            "OutOfTheLoop",
            "business",
        ],
        "sort": "top",
        "time_filter": "day",
        "limit": 30,
        "fetch_comments_for_top_n": 30,
        "fetch_external_links": True,
    },
    "hackernews": {
        "front_page_count": 15,
        "top_24h_count": 10,              # 补充过去 24h 高分热榜 (points>=50)
        "keywords": ["AI", "OpenAI", "Nvidia", "crypto", "billion"],
        "per_keyword_count": 3,
        "fetch_comments_for_top_n": 50,
        "fetch_external_links": True,
    },
    "binance": {
        "recommend_count": 15,
        "include_hot_hashtags": True,     # 币安广场官方热门话题榜 (hashtag/hot-list)
        "hot_hashtag_count": 4,
        "per_hashtag_count": 5,
        "include_hot_coins": True,        # 币安合约热搜币种榜 (topSearchList)
        "hot_coin_count": 3,
        "keywords": [],
        "coins": ["BTC", "ETH", "SOL"],
        "per_coin_count": 5,
    },
    "okx": {
        "recommend_count": 12,
        "include_hot_coins": True,        # 欧易官方热门币种排行 (rubik/web/public/hot-rank)
        "hot_coin_count": 3,
        "coins": ["BTC", "ETH", "SOL"],
        "per_coin_count": 5,
        "author_ids": [],
    },
    "zhihu": {},
    "xiaohongshu": {},
}


# ==============================================================================
# 四、运维拉取状态诊断与精简报告生成（全链路平台异常感知 + 数据质量体检）
# ==============================================================================
def create_harvester_instance(platform_name: str, platform_cfg: Dict[str, Any]):
    """按平台标识实例化对应的平台适配器（X 与 Reddit 共用专用爬虫小号目录 `x_xiaohao`）"""
    cls = HARVESTER_REGISTRY.get(platform_name)
    if not cls:
        raise ValueError(f"未注册的平台适配器: {platform_name}")
    if platform_name in ("x", "reddit"):
        return cls(
            burner_cookies=platform_cfg.get("burner_cookies", ""),
            burner_user_data_dir=platform_cfg.get("burner_user_data_dir"),
        )
    return cls()


def _resolve_auth_mode_desc(plat_name: str, harvester: Any) -> str:
    """提取各平台当前账号与通道状态"""
    if plat_name in ("x", "reddit"):
        return str(getattr(harvester, "burner_account_summary", "免登录模式"))
    if plat_name == "hackernews":
        return "免登录 (Algolia首页+24h高分榜+外链全文)"
    if plat_name == "binance":
        return "免登录 (广场推荐+热门话题榜+热搜币种榜)"
    if plat_name == "okx":
        return "免登录 (Orbit推荐+热门币种榜+自营过滤)"
    return "免登录模式"


def _has_external_article(post: Dict[str, Any]) -> bool:
    txt = str(((post.get("content") or {}).get("text_content")) or "")
    if "【原帖关联外链全文" in txt:
        return True
    meta = post.get("metadata") or {}
    pextra = meta.get("platform_extra") or {}
    if meta.get("external_link_enriched") or pextra.get("external_link_enriched"):
        return True
    ext = meta.get("external_article") or pextra.get("external_article")
    return isinstance(ext, dict) and bool(str(ext.get("text") or "").strip())


def _summarize_platform_execution(
    plat_name: str,
    p_cfg: Dict[str, Any],
    harvester: Any,
    posts: List[Dict[str, Any]],
    upsert_stats: Dict[str, Any],
    cost_s: float,
) -> Dict[str, Any]:
    """统计单平台拉取执行效果与异常感知报告：杜绝接口失效、登录掉线或残缺空帖伪装成正常"""
    auth_mode = _resolve_auth_mode_desc(plat_name, harvester)
    total_posts = len(posts)
    new_count = safe_int(upsert_stats.get("new_count"), 0)
    updated_count = safe_int(upsert_stats.get("updated_count"), 0)

    posts_with_images = 0
    remote_image_count = 0
    local_image_count = 0
    posts_with_comments = 0
    total_comments = 0
    comment_images_count = 0
    external_articles_count = 0

    for p in posts:
        media = p.get("media") or {}
        imgs = media.get("images") or []
        lmap = media.get("local_mapping") or {}
        if imgs:
            posts_with_images += 1
            remote_image_count += len(imgs)
            local_image_count += sum(1 for u in imgs if u in lmap)

        cmts = p.get("comments") or []
        if cmts:
            posts_with_comments += 1
            total_comments += len(cmts)
            for c in cmts:
                comment_images_count += len((c.get("local_mapping") or {}))

        if _has_external_article(p):
            external_articles_count += 1

    channel_stats: Dict[str, int] = getattr(harvester, "last_channel_stats", {}) or {}
    health_summary: Dict[str, Any] = (
        harvester.get_health_summary() if hasattr(harvester, "get_health_summary") else {}
    )
    health_status = str(health_summary.get("health_status") or "healthy")
    health_issues: List[Dict[str, Any]] = list(health_summary.get("issues") or [])
    quality_report: Dict[str, Any] = dict(health_summary.get("quality_report") or {})

    alerts: List[str] = []
    seen_alert_msgs = set()

    def _push_alert(msg: str) -> None:
        if msg and msg not in seen_alert_msgs:
            seen_alert_msgs.add(msg)
            alerts.append(msg)

    for issue in health_issues:
        lvl = str(issue.get("level") or "warning")
        if lvl not in ("critical", "warning"):
            continue
        prefix = "🚨" if lvl == "critical" else "⚠️"
        code = str(issue.get("code") or "HEALTH")
        ch = str(issue.get("channel") or plat_name)
        _push_alert(f"{prefix} [{plat_name}/{code}] ({ch}) {issue.get('message')}")

    if total_posts == 0:
        _push_alert(
            f"❌ [{plat_name}] 本轮拉取有效条数为 0！请检查代理网络、登录态或 `platforms/harvesters/{plat_name}_harvester.py` 接口连通性。"
        )

    if remote_image_count > 0 and local_image_count == 0 and p_cfg.get("download_media", True):
        _push_alert(
            f"⚠️ [{plat_name}] 发现 {remote_image_count} 张配图但本地下载成功 0 张！请检查图片 CDN 网络连通性。"
        )

    # 构造子通道拉取明细与关键主干子通道 0 条告警
    if plat_name == "x":
        total_ch = len(channel_stats)
        zero_ch = [k for k, v in channel_stats.items() if v == 0]
        zero_core_ch = [k for k in zero_ch if not k.startswith("@")]
        zero_kol_ch = [k for k in zero_ch if k.startswith("@")]
        ok_ch = total_ch - len(zero_ch)
        detail_str = ", ".join(f"{k}:{v}" for k, v in channel_stats.items())
        channel_summary = f"子通道({ok_ch}/{total_ch}命中): {detail_str}"
        if zero_core_ch:
            _push_alert(f"⚠️ [x] 核心主干流返回 0 条: {', '.join(zero_core_ch)} (请检查代理网络或推荐流/热榜源状态)")
        elif len(zero_kol_ch) >= 4:
            _push_alert(f"⚠️ [x] 多个监控博主本轮返回 0 条: {', '.join(zero_kol_ch)}")
    elif plat_name == "reddit":
        sub_cnt = len(p_cfg.get("subreddits") or [])
        detail_str = ", ".join(f"{k}:{v}" for k, v in channel_stats.items())
        channel_summary = f"热榜聚合(popular+{sub_cnt}板块 -> 抓回{total_posts}条) | 分布: {detail_str}"
    else:
        total_ch = len(channel_stats)
        zero_ch = [k for k, v in channel_stats.items() if v == 0]
        ok_ch = total_ch - len(zero_ch)
        raw_sum = sum(channel_stats.values())
        detail_str = ", ".join(f"{k}:{v}" for k, v in channel_stats.items())
        channel_summary = f"子通道({ok_ch}/{total_ch}命中, 原始{raw_sum}条->去重后{total_posts}条): {detail_str}"

    if total_posts == 0:
        status_label = "❌空数据"
        norm_status = "empty"
    elif health_status == "critical":
        status_label = "🚨严重降级"
        norm_status = "critical"
    elif health_status == "degraded" or alerts:
        status_label = "⚠️降级预警"
        norm_status = "degraded"
    else:
        status_label = "✅正常"
        norm_status = "ok"

    return {
        "status": norm_status,
        "status_label": status_label,
        "health_status": health_status,
        "health_issues": health_issues,
        "quality_report": quality_report,
        "auth_mode": auth_mode,
        "harvested_count": total_posts,
        "new_count": new_count,
        "updated_count": updated_count,
        "posts_with_images": posts_with_images,
        "remote_images_count": remote_image_count,
        "local_images_mapped": local_image_count,
        "posts_with_comments": posts_with_comments,
        "total_comments_attached": total_comments,
        "comment_images_mapped": comment_images_count,
        "external_articles_count": external_articles_count,
        "channel_stats": channel_stats,
        "channel_summary": channel_summary,
        "alerts": alerts,
        "elapsed_seconds": cost_s,
    }


def format_ops_harvest_report(report: Dict[str, Any]) -> str:
    """渲染高信息密度运维拉取报告（清晰呈现采集健康度、降级告警、子通道明细与数据完整度）"""
    lines = [
        "",
        "=" * 120,
        f"📋 【多平台拉取与健康体检报告】 轮次: #{report.get('round_no', 1)} | 时间: {report['run_time']} "
        f"| 总耗时: {report['elapsed_seconds']}s | 库内累计: {report.get('collection_total_docs', 0)}条 ({report['target_collection']})",
        f"📊 本轮汇总: 入库 {report['total_harvested']} 条 (新增: {report['total_new_inserted']} / 复抓更新: {report['total_repolled_updated']}) "
        f"| 配图下载: {report['total_local_images']}/{report['total_remote_images']} 张 "
        f"| 评论拉取: {report['total_comments']} 条 (含评论配图 {report['total_comment_images']} 张) "
        f"| 外链正文: {report.get('total_external_articles', 0)} 篇",
        "-" * 120,
        f"{'平台':<10} | {'健康状态':<8} | {'耗时':<6} | {'入库数':<6} | {'新增/复抓':<10} | {'配图(已下/发现)':<14} | {'评论(带评帖/总条数)':<16} | {'外链文':<6} | 账号与采集通道状态",
        "-" * 120,
    ]

    for plat in report["enabled_platforms"]:
        st = report["platform_stats"].get(plat) or {}
        status_lbl = st.get("status_label", "❌报错")
        cost_str = f"{st.get('elapsed_seconds', 0):.1f}s"
        tot_str = f"{st.get('harvested_count', 0)}条"
        new_upd_str = f"+{st.get('new_count', 0)} / ~{st.get('updated_count', 0)}"
        img_str = f"{st.get('local_images_mapped', 0)}/{st.get('remote_images_count', 0)}张 ({st.get('posts_with_images', 0)}帖)"
        cmt_str = f"{st.get('posts_with_comments', 0)}帖 / {st.get('total_comments_attached', 0)}条"
        ext_str = f"{st.get('external_articles_count', 0)}篇"
        auth_str = st.get("auth_mode", "-")
        lines.append(
            f"{plat:<10} | {status_lbl:<8} | {cost_str:<6} | {tot_str:<6} | {new_upd_str:<10} | {img_str:<14} | {cmt_str:<16} | {ext_str:<6} | {auth_str}"
        )

    lines.append("-" * 120)
    lines.append("🔎 【各平台子通道拉取命中明细】")
    for plat in report["enabled_platforms"]:
        st = report["platform_stats"].get(plat) or {}
        if st.get("status") == "error":
            lines.append(f"  • [{plat:<10}] ❌ 采集报错中断: {st.get('error_message')}")
        else:
            lines.append(f"  • [{plat:<10}] {st.get('channel_summary', '-')}")

    lines.append("-" * 120)
    ops_alerts = report.get("ops_alerts") or []
    if ops_alerts:
        lines.append(f"🚨 【平台异常感知与数据质量告警 ({len(ops_alerts)} 项)】")
        for idx, alert_msg in enumerate(ops_alerts, 1):
            lines.append(f"  {idx}. {alert_msg}")
    else:
        lines.append("🟢 【巡检结论】全部开启平台登录态、热榜通道、评论富化、外链提取与图片下载均健康正常，0 降级、0 残缺帖。")
    lines.append("=" * 120)
    return "\n".join(lines)


# ==============================================================================
# 五、两大标准模块化编程入口（广谱巡检采集 & 按关键词跨平台指哪打哪深挖）
# ==============================================================================
def run_harvest_once(
    switches: Optional[Dict[str, bool]] = None,
    platform_configs: Optional[Dict[str, Dict[str, Any]]] = None,
    global_config: Optional[Dict[str, Any]] = None,
    round_no: int = 1,
) -> Dict[str, Any]:
    """执行单轮多平台广谱拉取、结构化清洗、入库与运维巡检报告输出"""
    started = time.monotonic()
    effective_switches = switches if switches is not None else PLATFORM_SWITCHES
    effective_p_cfgs = platform_configs if platform_configs is not None else PLATFORM_HARVEST_CONFIGS
    effective_g_cfg = global_config if global_config is not None else GLOBAL_SCHEDULE_CONFIG

    collection_name = effective_g_cfg.get("mongo_collection_name", NEW_HARVESTED_COLLECTION_NAME)
    enable_mongo = bool(effective_g_cfg.get("enable_mongo_upsert", True))
    download_media = bool(effective_g_cfg.get("download_media", True))

    enabled_platforms = [p for p, on in effective_switches.items() if on]
    disabled_platforms = [p for p, on in effective_switches.items() if not on]

    logger.info(
        f"🚀 [拉取总控] 开始第 #{round_no} 轮广谱采集 | 开启平台: {enabled_platforms} | 目标集合: [{collection_name}]"
    )

    post_manager: Optional[HarvestedPostManager] = None
    if enable_mongo:
        post_manager = HarvestedPostManager(gen_db_object(), collection_name=collection_name)

    report: Dict[str, Any] = {
        "round_no": round_no,
        "run_time": datetime.now(TZ_CN).strftime("%Y-%m-%d %H:%M:%S"),
        "target_collection": collection_name,
        "collection_total_docs": 0,
        "enabled_platforms": enabled_platforms,
        "disabled_platforms": disabled_platforms,
        "platform_stats": {},
        "total_harvested": 0,
        "total_upserted_to_mongo": 0,
        "total_new_inserted": 0,
        "total_repolled_updated": 0,
        "total_remote_images": 0,
        "total_local_images": 0,
        "total_comments": 0,
        "total_comment_images": 0,
        "total_external_articles": 0,
        "post_keys": [],
        "ops_alerts": [],
        "elapsed_seconds": 0.0,
    }

    all_harvested_posts: List[Dict[str, Any]] = []

    for plat_name in enabled_platforms:
        plat_start = time.monotonic()
        p_cfg = dict(effective_p_cfgs.get(plat_name) or {})
        p_cfg.setdefault("download_media", download_media)

        try:
            harvester = create_harvester_instance(plat_name, p_cfg)
            posts = harvester.harvest(p_cfg) or []
            upsert_stats = {"total": 0, "new_count": 0, "updated_count": 0, "cost_ms": 0.0}

            if post_manager and posts:
                upsert_stats = post_manager.upsert_posts(posts) or upsert_stats
                for p in posts:
                    if isinstance(p.get("first_seen_at"), datetime):
                        p["first_seen_at"] = p["first_seen_at"].isoformat()
                    if isinstance(p.get("db_update_time"), datetime):
                        p["db_update_time"] = p["db_update_time"].isoformat()

            cost_s = round(time.monotonic() - plat_start, 2)
            plat_summary = _summarize_platform_execution(
                plat_name=plat_name,
                p_cfg=p_cfg,
                harvester=harvester,
                posts=posts,
                upsert_stats=upsert_stats,
                cost_s=cost_s,
            )

            report["platform_stats"][plat_name] = plat_summary
            report["total_harvested"] += plat_summary["harvested_count"]
            report["total_upserted_to_mongo"] += upsert_stats["total"]
            report["total_new_inserted"] += plat_summary["new_count"]
            report["total_repolled_updated"] += plat_summary["updated_count"]
            report["total_remote_images"] += plat_summary["remote_images_count"]
            report["total_local_images"] += plat_summary["local_images_mapped"]
            report["total_comments"] += plat_summary["total_comments_attached"]
            report["total_comment_images"] += plat_summary["comment_images_mapped"]
            report["total_external_articles"] += plat_summary["external_articles_count"]
            report["ops_alerts"].extend(plat_summary["alerts"])
            all_harvested_posts.extend(posts)

            logger.info(
                f"{plat_summary['status_label']} [{plat_name}] 完成 ({cost_s}s) "
                f"| 入库: {plat_summary['harvested_count']}条 (+{plat_summary['new_count']}/~{plat_summary['updated_count']}) "
                f"| 配图: {plat_summary['local_images_mapped']}/{plat_summary['remote_images_count']}张 "
                f"| 评论: {plat_summary['posts_with_comments']}帖/{plat_summary['total_comments_attached']}条 "
                f"| 外链文: {plat_summary['external_articles_count']}篇"
            )

        except Exception as e:
            cost_s = round(time.monotonic() - plat_start, 2)
            err_alert = (
                f"❌ [{plat_name}] 平台采集抛出异常: {e} "
                f"(排查文件: `platforms/harvesters/{plat_name}_harvester.py`)"
            )
            report["platform_stats"][plat_name] = {
                "status": "error",
                "status_label": "❌报错",
                "auth_mode": "异常中断",
                "error_message": str(e),
                "harvested_count": 0,
                "new_count": 0,
                "updated_count": 0,
                "posts_with_images": 0,
                "remote_images_count": 0,
                "local_images_mapped": 0,
                "posts_with_comments": 0,
                "total_comments_attached": 0,
                "comment_images_mapped": 0,
                "external_articles_count": 0,
                "channel_stats": {},
                "channel_summary": f"异常中断: {e}",
                "alerts": [err_alert],
                "elapsed_seconds": cost_s,
            }
            report["ops_alerts"].append(err_alert)
            logger.error(f"❌ [{plat_name}] 采集异常（已隔离，不影响其他平台）: {e}", exc_info=True)

    if post_manager:
        try:
            report["collection_total_docs"] = post_manager.collection.count_documents({})
        except Exception:
            pass

    report["post_keys"] = [
        f"{p.get('source')}:{p.get('post_id')}"
        for p in all_harvested_posts
        if p.get("source") and p.get("post_id")
    ]
    report["elapsed_seconds"] = round(time.monotonic() - started, 2)

    snapshot_file = effective_g_cfg.get("snapshot_filename")
    if snapshot_file:
        snap_path = get_runtime_file_path(snapshot_file)
        save_json(
            snap_path,
            {
                "report": report,
                "sample_posts_by_platform": {
                    plat: [p for p in all_harvested_posts if p.get("source") == plat][:5]
                    for plat in enabled_platforms
                },
            },
        )

    logger.info(format_ops_harvest_report(report))
    return report


def harvest_broad_feeds(
    switches: Optional[Dict[str, bool]] = None,
    platform_configs: Optional[Dict[str, Dict[str, Any]]] = None,
    global_config: Optional[Dict[str, Any]] = None,
    round_no: int = 1,
) -> Dict[str, Any]:
    """模块 1A 编程接口：执行全平台广谱巡检采集并幂等写入 `harvested_social_posts`。"""
    return run_harvest_once(
        switches=switches,
        platform_configs=platform_configs,
        global_config=global_config,
        round_no=round_no,
    )


def harvest_by_keywords(
    keywords: Sequence[str],
    platforms: Optional[Sequence[str]] = None,
    per_platform_limit: int = 25,
    hours_back: int = 72,
    fetch_comments: bool = True,
    fetch_external_links: bool = True,
    download_media: bool = True,
    save_to_mongo: bool = True,
    collection_name: str = NEW_HARVESTED_COLLECTION_NAME,
    platform_configs: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    模块 1B 编程接口：按指定关键词组跨平台并发深挖采集（「指哪打哪」检索），
    自动拉取外链正文、前排高赞评论、下载主帖与评论区配图，幂等写入 `harvested_social_posts`，
    并输出分清「垂直社区客观少数据」与「综合大平台爬虫故障/漏搜」的异常感知诊断报告。

    可完全独立于下游或上游任何模块单独调用。
    """
    started = time.monotonic()
    clean_keywords: List[str] = []
    seen_kw = set()
    for kw in keywords or []:
        s = str(kw or "").strip()
        if s and s.lower() not in seen_kw:
            seen_kw.add(s.lower())
            clean_keywords.append(s)

    if not clean_keywords:
        return {
            "keywords": [],
            "platforms": [],
            "by_platform_counts": {},
            "platform_diagnosis": {},
            "platform_health": {},
            "platform_errors": {},
            "ops_alerts": [],
            "total_posts": 0,
            "new_count": 0,
            "updated_count": 0,
            "post_keys": [],
            "posts": [],
            "elapsed_seconds": 0.0,
        }

    effective_p_cfgs = platform_configs if platform_configs is not None else PLATFORM_HARVEST_CONFIGS
    if platforms is not None:
        target_platforms = [p for p in platforms if p in HARVESTER_REGISTRY]
    else:
        target_platforms = [p for p, on in PLATFORM_SWITCHES.items() if on and p in HARVESTER_REGISTRY]

    query_str = "|".join(clean_keywords)
    by_platform_counts: Dict[str, int] = {p: 0 for p in target_platforms}
    platform_health: Dict[str, Dict[str, Any]] = {}
    platform_errors: Dict[str, str] = {}
    ops_alerts: List[str] = []
    by_key: Dict[str, Dict[str, Any]] = {}

    def _search_one_platform(plat_name: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        p_cfg = dict(effective_p_cfgs.get(plat_name) or {})
        harvester = create_harvester_instance(plat_name, p_cfg)
        raw_hits = harvester.search_or_topic_posts(
            query=query_str,
            count=per_platform_limit,
            hours_back=hours_back,
            fetch_comments=fetch_comments,
            fetch_external_links=fetch_external_links,
            download_media=download_media,
        ) or []
        valid_hits = [p for p in raw_hits if harvester.is_valuable_post(p, p_cfg)]
        h_summary = harvester.get_health_summary() if hasattr(harvester, "get_health_summary") else {}
        return valid_hits, h_summary

    if target_platforms:
        with ThreadPoolExecutor(max_workers=min(5, len(target_platforms))) as pool:
            fut_map = {pool.submit(_search_one_platform, plat): plat for plat in target_platforms}
            for fut in as_completed(fut_map):
                plat = fut_map[fut]
                try:
                    plat_posts, h_sum = fut.result()
                    plat_posts = plat_posts or []
                    by_platform_counts[plat] = len(plat_posts)
                    platform_health[plat] = h_sum or {}
                    for issue in (h_sum or {}).get("issues") or []:
                        lvl = str(issue.get("level") or "")
                        if lvl in ("critical", "warning"):
                            pfx = "🚨" if lvl == "critical" else "⚠️"
                            ops_alerts.append(
                                f"{pfx} [{plat}/{issue.get('code', 'SEARCH')}] {issue.get('message')}"
                            )
                    for p in plat_posts:
                        pk = f"{p.get('source')}:{p.get('post_id')}"
                        if p.get("source") and p.get("post_id"):
                            by_key[pk] = p
                except Exception as e:
                    platform_errors[plat] = str(e)
                    ops_alerts.append(f"❌ [{plat}/EXCEPTION] 关键词检索抛出未捕获异常: {e}")
                    logger.warning(f"⚠️ [定向关键词深挖] 平台 [{plat}] 检索异常 ({query_str}): {e}")

    # 区分「垂直社区属性客观少数据」vs「全球综合大平台应当有数据却漏搜/通道降级」
    mega_platforms = {"x", "reddit", "binance"}
    max_mega_hits = max((by_platform_counts.get(mp, 0) for mp in mega_platforms), default=0)
    platform_diagnosis: Dict[str, str] = {}
    for plat in target_platforms:
        cnt = by_platform_counts.get(plat, 0)
        h_stat = str((platform_health.get(plat) or {}).get("health_status") or "healthy")
        note = str((platform_health.get(plat) or {}).get("search_note") or "").strip()
        if plat in platform_errors:
            platform_diagnosis[plat] = f"❌报错中断 ({platform_errors[plat][:80]})"
        elif h_stat == "critical":
            platform_diagnosis[plat] = f"🚨通道严重异常 (命中{cnt}条{', ' + note if note else ''})"
        elif h_stat == "degraded":
            platform_diagnosis[plat] = f"⚠️通道降级预警 (命中{cnt}条{', ' + note if note else ''})"
        elif cnt > 0:
            platform_diagnosis[plat] = f"✅正常命中 ({cnt}条{', ' + note if note else ''})"
        elif plat in ("okx", "hackernews"):
            reason = "加密垂直社区非本领域话题" if plat == "okx" else "英文极客社区无匹配讨论"
            platform_diagnosis[plat] = f"ℹ️平台属性正常0条 ({reason})"
        elif plat in mega_platforms and max_mega_hits >= 5:
            msg = (
                f"⚠️ [{plat}/SUSPECTED_SEARCH_MISS] 同批其他大平台已搜出 {max_mega_hits} 条热帖，"
                f"但综合平台 [{plat}] 返回 0 条！请排查关键词中英文覆盖或 [{plat}] 搜索接口状态。"
            )
            ops_alerts.append(msg)
            platform_diagnosis[plat] = "⚠️疑似漏搜0条 (同批大平台已有热帖)"
        else:
            platform_diagnosis[plat] = "ℹ️未命中近期帖 (0条)"

    all_posts = list(by_key.values())
    upsert_stats = {"total": 0, "new_count": 0, "updated_count": 0}
    if save_to_mongo and all_posts:
        post_manager = HarvestedPostManager(gen_db_object(), collection_name=collection_name)
        upsert_stats = post_manager.upsert_posts(all_posts) or upsert_stats
        for p in all_posts:
            if isinstance(p.get("first_seen_at"), datetime):
                p["first_seen_at"] = p["first_seen_at"].isoformat()
            if isinstance(p.get("db_update_time"), datetime):
                p["db_update_time"] = p["db_update_time"].isoformat()

    elapsed = round(time.monotonic() - started, 2)
    post_keys = list(by_key.keys())
    logger.info(
        f"🔎 [定向关键词深挖] 关键词={clean_keywords} | 平台命中={by_platform_counts} "
        f"| 诊断={platform_diagnosis} "
        f"| 合计={len(all_posts)}条 (+{safe_int(upsert_stats.get('new_count'), 0)}/~{safe_int(upsert_stats.get('updated_count'), 0)}) "
        f"| 告警={len(ops_alerts)}项 | 耗时={elapsed}s"
    )
    return {
        "keywords": clean_keywords,
        "platforms": target_platforms,
        "by_platform_counts": by_platform_counts,
        "platform_diagnosis": platform_diagnosis,
        "platform_health": platform_health,
        "platform_errors": platform_errors,
        "ops_alerts": ops_alerts,
        "total_posts": len(all_posts),
        "new_count": safe_int(upsert_stats.get("new_count"), 0),
        "updated_count": safe_int(upsert_stats.get("updated_count"), 0),
        "post_keys": post_keys,
        "posts": all_posts,
        "elapsed_seconds": elapsed,
    }


def run_harvest_daemon(interval_seconds: Optional[int] = None):
    """常驻定时拉取循环：默认每 3600 秒（1 小时）执行一轮"""
    interval = interval_seconds or GLOBAL_SCHEDULE_CONFIG.get("interval_seconds", 3600)
    logger.info(
        f"⏰ [常驻定时拉取已启动] 轮询周期: 每 {interval} 秒 ({interval / 3600:.1f} 小时/轮) "
        f"| 开启平台: {[k for k, v in PLATFORM_SWITCHES.items() if v]}"
    )
    round_no = 1
    while True:
        try:
            run_harvest_once(round_no=round_no)
        except Exception as e:
            logger.error(f"❌ [常驻定时拉取] 第 #{round_no} 轮外层异常: {e}", exc_info=True)
        round_no += 1
        next_run_time = (datetime.now(TZ_CN) + timedelta(seconds=interval)).strftime("%Y-%m-%d %H:%M:%S")
        logger.info(
            f"💤 [常驻定时拉取] 第 #{round_no - 1} 轮已完成，休眠 {interval} 秒，下一轮将于 [{next_run_time}] 自动执行..."
        )
        time.sleep(interval)


if __name__ == "__main__":
    # 直接在 IDE 中点击运行即可：默认读取 GLOBAL_SCHEDULE_CONFIG["continuous_loop"] (True) 进入每小时常驻循环
    if GLOBAL_SCHEDULE_CONFIG.get("continuous_loop", True):
        run_harvest_daemon(interval_seconds=GLOBAL_SCHEDULE_CONFIG.get("interval_seconds", 3600))
    else:
        run_harvest_once(round_no=1)
