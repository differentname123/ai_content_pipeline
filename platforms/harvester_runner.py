# -*- coding: utf-8 -*-
"""
多平台内容拉取总控面板与每小时定时入库任务 (`harvester_runner.py`)

================================================================================
【使用说明】
1. 使用者只需修改最上方的 `PLATFORM_SWITCHES` 字典（`True` 开启 / `False` 关闭），
   即可一键控制单次或定时任务要拉取哪些平台的数据。
2. 所有拉取到的帖子均由各平台专属适配器（`platforms/harvesters/*_harvester.py`）
   自动抹平字段差异、注入 `[插图: url]`、按视觉哈希下载配图至 `media_downloads/`，
   并统一写入独立的 MongoDB 集合 `harvested_social_posts`。
3. 每一轮运行结束后，自动生成高信息密度的【多平台拉取运维巡检报告】，
   直观展示各平台账号状态、子通道命中明细、新增/复抓数量、配图/神评覆盖率与异常告警。
4. 运行方式：
   - 单次立即拉取并入库：`python platforms/harvester_runner.py`
   - 启动每小时定时任务：`python platforms/harvester_runner.py --daemon`（默认每 3600 秒即 1 小时拉取一轮）
================================================================================
"""
import argparse
from collections import Counter
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

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

logger = setup_logger(app_name="harvester_runner")
TZ_CN = timezone(timedelta(hours=8))


# ==============================================================================
# 一、【最上层总控开关】开启 (True) 或 关闭 (False) 对应平台的拉取
# ==============================================================================
PLATFORM_SWITCHES: Dict[str, bool] = {
    "x": True,              # 英文 X (Twitter)：顶流大V、真实豪赌赔率(@Polymarket/@Kalshi)、内幕吃瓜与神评
    "reddit": True,         # 英文 Reddit：10大黄金板块 24h 暴富暴负交割单、科技大瓜
    "hackernews": True,     # 英文 HackerNews：硅谷科技/AI/商业首页高赞热帖与极客神评
    "binance": True,        # 中文 币安广场：高互动实盘故事、极端行情吐槽与评论区神评
    "okx": True,            # 中文 欧易 Orbit：合约实盘交割单、热门币种讨论与神评
    "zhihu": False,         # 预留扩展：知乎 (待启用时设为 True)
    "xiaohongshu": False,   # 预留扩展：小红书 (待启用时设为 True)
}


# ==============================================================================
# 二、【全局入库与定时调度配置】
# ==============================================================================
GLOBAL_SCHEDULE_CONFIG: Dict[str, Any] = {
    # 独立的 MongoDB 集合名称
    "mongo_collection_name": NEW_HARVESTED_COLLECTION_NAME,  # "harvested_social_posts"
    # 是否启用 MongoDB 自动入库
    "enable_mongo_upsert": True,
    # 是否并发下载帖子与评论配图到本地 `media_downloads/` 并建立 `local_mapping`
    "download_media": True,
    # 定时任务默认轮询间隔（秒）：3600 秒 = 每 1 小时自动跑一轮
    "interval_seconds": 3600,
    # 每次运行后在 logs/runtime/ 下保存最新拉取巡检报告与样本快照
    "snapshot_filename": "latest_harvested_posts_snapshot.json",
}


# ==============================================================================
# 三、【各平台拉取范围配置（第一步仅负责忠实记录与结构归一化，不设脆弱过滤门槛）】
# ==============================================================================
PLATFORM_HARVEST_CONFIGS: Dict[str, Dict[str, Any]] = {
    "x": {
        # 只读小号浏览器缓存目录或 Cookie 字符串（若为空则自动只跑 0 账号免登通道）
        "burner_user_data_dir": r"W:\temp\x_myself",
        "burner_cookies": "",
        # 重点监控的英文高价值大 V 列表（含 @Polymarket / @Kalshi 原生赌局热帖）
        "kol_handles": [
            "Polymarket",
            "Kalshi",
            "KobeissiLetter",
            "WatcherGuru",
            "Dexerto",
            "wallstreetbets",
            "MarioNawfal",
            "MorningBrew",
            "CoinGlass_com",
            "whale_alert",
            "NoContextHumans",
            "unusual_whales",
            "AutismCapital",
        ],
        "tweets_per_handle": 5,
        # 默认关闭小号 For You 泛娱乐推荐流，专注拉取上述 13 个垂直大 V
        "include_home_feed": False,
        "home_feed_count": 20,
        "fetch_comments_for_top_n": 5,
    },
    "reddit": {
        "subreddits": [
            "wallstreetbets",
            "CryptoCurrency",
            "stocks",
            "options",
            "FluentInFinance",
            "technology",
            "singularity",
            "algotrading",
            "PersonalFinance",
            "antiwork",
        ],
        "sort": "top",
        "time_filter": "day",
        "limit": 20,
        "fetch_comments_for_top_n": 0,  # Reddit免登RSS限流为1次/60s，默认单次请求拿齐10板块图文主帖
    },
    "hackernews": {
        "limit": 12,
        "min_points": 50,
        "keywords": ["AI", "OpenAI", "Nvidia", "crypto", "billion"],
        "fetch_comments_for_top_n": 5,
    },
    "binance": {
        "recommend_count": 15,
        "keywords": [],
        "coins": ["BTC", "ETH", "SOL"],
        "per_coin_count": 6,
    },
    "okx": {
        "recommend_count": 12,
        "coins": ["BTC", "ETH", "SOL"],
        "per_coin_count": 5,
        "author_ids": [],
    },
    "zhihu": {},
    "xiaohongshu": {},
}


# ==============================================================================
# 四、运维巡检诊断分析与高密度报告生成器
# ==============================================================================
def create_harvester_instance(platform_name: str, platform_cfg: Dict[str, Any]):
    """按平台标识实例化对应的平台适配器"""
    cls = HARVESTER_REGISTRY.get(platform_name)
    if not cls:
        raise ValueError(f"未注册的平台适配器: {platform_name}")
    if platform_name == "x":
        return cls(
            burner_cookies=platform_cfg.get("burner_cookies", ""),
            burner_user_data_dir=platform_cfg.get("burner_user_data_dir"),
        )
    return cls()


def _resolve_auth_mode_desc(plat_name: str, harvester: Any) -> str:
    """提取运维可读的各平台当前账号与通道状态"""
    if plat_name == "x":
        if getattr(harvester, "has_burner_account", False):
            return str(getattr(harvester, "account_identity_desc", "已登录只读小号"))
        return "免登录GuestToken模式(无小号Cookie)"
    if plat_name == "reddit":
        return "免登录(0账号10板块+聚合RSS)"
    if plat_name == "hackernews":
        return "免登录(0账号Algolia官方API)"
    if plat_name == "binance":
        return "免登录(0账号公开流+自营号过滤)"
    if plat_name == "okx":
        return "免登录(0账号Orbit流+自营号过滤)"
    return "免登录模式"


def _analyze_platform_posts(
    plat_name: str,
    p_cfg: Dict[str, Any],
    harvester: Any,
    posts: List[Dict[str, Any]],
    upsert_stats: Dict[str, Any],
    cost_s: float,
) -> Dict[str, Any]:
    """针对单平台拉取结果提取高密度运维诊断指标、子通道分布、Top样本与告警"""
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
    rising_posts_count = 0

    for p in posts:
        media = p.get("media") or {}
        imgs = media.get("images") or []
        lmap = media.get("local_mapping") or {}
        if imgs:
            posts_with_images += 1
            remote_image_count += len(imgs)
        local_image_count += len(lmap)

        cmts = p.get("comments") or []
        if cmts:
            posts_with_comments += 1
            total_comments += len(cmts)
            for c in cmts:
                comment_images_count += len((c.get("local_mapping") or {}))

        eng = p.get("engagement") or {}
        if safe_int(eng.get("delta_likes"), 0) > 0 or safe_int(eng.get("delta_views"), 0) > 0:
            rising_posts_count += 1

    # 子通道明细与平台专属巡检告警
    alerts: List[str] = []
    channel_detail = ""

    if total_posts == 0:
        alerts.append(
            f"❌ [{plat_name}] 本轮拉取结果为 0 条！排查方向: 检查网络代理连通性或 `platforms/harvesters/{plat_name}_harvester.py` 接口响应状态。"
        )

    if remote_image_count > 0 and local_image_count == 0 and p_cfg.get("download_media", True):
        alerts.append(
            f"⚠️ [{plat_name}] 检测到 {remote_image_count} 张远程配图但本地下载成功数为 0！排查方向: 检查代理连通性或图片 CDN 域名访问。"
        )

    if plat_name == "x":
        configured_handles = p_cfg.get("kol_handles") or []
        author_counter = Counter(str((p.get("author") or {}).get("username") or "").lower() for p in posts)
        hit_parts = []
        missing_handles = []
        for h in configured_handles:
            clean_h = str(h).lstrip("@").strip()
            cnt = author_counter.get(clean_h.lower(), 0)
            if cnt > 0:
                hit_parts.append(f"@{clean_h}:{cnt}")
            else:
                missing_handles.append(f"@{clean_h}")
        hit_kol_count = len(configured_handles) - len(missing_handles)
        channel_detail = f"监控大V命中 {hit_kol_count}/{len(configured_handles)} ({', '.join(hit_parts[:8])}{'...' if len(hit_parts) > 8 else ''})"
        if missing_handles:
            alerts.append(
                f"⚠️ [x] 以下监控大V本轮返回 0 条: {', '.join(missing_handles)} "
                f"(排查: 检查大V是否改名/停更，或小号 Session `{p_cfg.get('burner_user_data_dir')}` 是否失效)"
            )

    elif plat_name == "reddit":
        sub_counter = Counter(
            str(((p.get("metadata") or {}).get("platform_extra") or {}).get("subreddit") or "unknown")
            for p in posts
        )
        top_subs = ", ".join(f"{k}:{v}" for k, v in sub_counter.most_common(6))
        channel_detail = f"聚合板块命中 {len(sub_counter)}个 ({top_subs})"

    elif plat_name == "hackernews":
        likes_list = [safe_int((p.get("engagement") or {}).get("like_count"), 0) for p in posts]
        p_min = min(likes_list) if likes_list else 0
        p_max = max(likes_list) if likes_list else 0
        kws = ", ".join(p_cfg.get("keywords") or [])
        channel_detail = f"热榜+关键词[{kws}] | 赞数区间: {p_min}~{p_max} points"

    elif plat_name in ("binance", "okx"):
        card_counter = Counter(str(p.get("card_type") or "UNKNOWN") for p in posts)
        coin_counter: Counter = Counter()
        for p in posts:
            for c in (p.get("content") or {}).get("mentioned_coins") or []:
                coin_counter[str(c).upper()] += 1
        card_str = "/".join(f"{k}:{v}" for k, v in card_counter.most_common())
        coin_str = ", ".join(f"{k}({v})" for k, v in coin_counter.most_common(5)) or "无"
        channel_detail = f"题型[{card_str}] | 热门提及币种: {coin_str}"

    # 选出本平台互动热度最高的一条样本用于一眼验真
    top_sample_str = "无"
    if posts:
        top_post = max(
            posts,
            key=lambda x: (
                safe_int((x.get("engagement") or {}).get("like_count"), 0),
                safe_int((x.get("engagement") or {}).get("view_count"), 0),
                safe_int((x.get("engagement") or {}).get("comment_count"), 0),
            ),
        )
        t_author = (top_post.get("author") or {}).get("author_name") or (top_post.get("author") or {}).get("username") or "anon"
        t_eng = top_post.get("engagement") or {}
        t_likes = safe_int(t_eng.get("like_count"), 0)
        t_views = safe_int(t_eng.get("view_count"), 0)
        t_cmts = safe_int(t_eng.get("comment_count"), 0)
        t_dlikes = safe_int(t_eng.get("delta_likes"), 0)
        raw_txt = ((top_post.get("content") or {}).get("title") or (top_post.get("content") or {}).get("text_content") or "")
        clean_preview = " ".join(raw_txt.replace("[插图:", " ").split())[:55]
        delta_tag = f" 📈Δ赞+{t_dlikes}" if t_dlikes > 0 else ""
        top_sample_str = f"@{t_author} (❤️{t_likes} 👀{t_views} 💬{t_cmts}{delta_tag}) 「{clean_preview}...」"

    status_label = "✅正常" if (total_posts > 0 and not alerts) else ("⚠️预警" if total_posts > 0 else "❌空数据")

    return {
        "status": "ok" if total_posts > 0 else "empty",
        "status_label": status_label,
        "auth_mode": auth_mode,
        "harvested_count": total_posts,
        "new_count": new_count,
        "updated_count": updated_count,
        "rising_posts_count": rising_posts_count,
        "posts_with_images": posts_with_images,
        "remote_images_count": remote_image_count,
        "local_images_mapped": local_image_count,
        "posts_with_comments": posts_with_comments,
        "total_comments_attached": total_comments,
        "comment_images_mapped": comment_images_count,
        "channel_detail": channel_detail,
        "top_post_preview": top_sample_str,
        "alerts": alerts,
        "elapsed_seconds": cost_s,
    }


def format_ops_harvest_report(report: Dict[str, Any]) -> str:
    """将结构化巡检报告渲染为运维人员一眼可读的高信息密度面板"""
    lines = [
        "",
        "=" * 112,
        f"📋 【多平台内容拉取运维巡检报告 (Ops Harvest Report)】 轮次: #{report.get('round_no', 1)} | 时间: {report['run_time']}",
        "=" * 112,
        f"🗄️ 数据库集合 : {report['target_collection']} (库内当前累计总帖数: {report.get('collection_total_docs', 0)} 条)",
        f"⏱️ 本轮总耗时 : {report['elapsed_seconds']}s | 开启平台: {report['enabled_platforms']} | 关闭平台: {report['disabled_platforms']}",
        f"📊 本轮总产出 : 共抓回 {report['total_harvested']} 条 (🆕新帖入库: {report['total_new_inserted']} 条 | 🔄老帖复抓更新: {report['total_repolled_updated']} 条 | 📈热度蹿升: {report['total_rising_posts']} 条)",
        f"🖼️ 多模态富化 : 主帖本地配图 {report['total_local_images']} 张 | 附带热评 {report['total_comments']} 条 (含评论区梗图 {report['total_comment_images']} 张)",
        "-" * 112,
        f"{'平台':<11} | {'状态':<6} | {'耗时':<7} | {'总抓取':<6} | {'新增/复抓':<10} | {'带图帖/本地图':<13} | {'带评帖/神评数':<13} | 账号与采集通道状态",
        "-" * 112,
    ]

    for plat in report["enabled_platforms"]:
        st = report["platform_stats"].get(plat) or {}
        status_lbl = st.get("status_label", "❌异常")
        cost_str = f"{st.get('elapsed_seconds', 0):.1f}s"
        tot_str = f"{st.get('harvested_count', 0)}条"
        new_upd_str = f"+{st.get('new_count', 0)} / ~{st.get('updated_count', 0)}"
        img_str = f"{st.get('posts_with_images', 0)}帖/{st.get('local_images_mapped', 0)}张"
        cmt_str = f"{st.get('posts_with_comments', 0)}帖/{st.get('total_comments_attached', 0)}条"
        auth_str = st.get("auth_mode", "-")
        lines.append(
            f"{plat:<11} | {status_lbl:<6} | {cost_str:<7} | {tot_str:<6} | {new_upd_str:<10} | {img_str:<13} | {cmt_str:<13} | {auth_str}"
        )

    lines.append("-" * 112)
    lines.append("🔍 【各平台子通道命中明细 & 本轮最高热度样本】")
    for plat in report["enabled_platforms"]:
        st = report["platform_stats"].get(plat) or {}
        if st.get("status") == "error":
            lines.append(f"  • [{plat:<10}] ❌ 运行异常: {st.get('error_message')}")
        else:
            lines.append(f"  • [{plat:<10}] {st.get('channel_detail', '-')}")
            lines.append(f"               🔥 Top样本: {st.get('top_post_preview', '-')}")

    lines.append("-" * 112)
    ops_alerts = report.get("ops_alerts") or []
    if ops_alerts:
        lines.append(f"🚨 【运维巡检告警与排查建议 ({len(ops_alerts)} 项需关注)】")
        for idx, alert_msg in enumerate(ops_alerts, 1):
            lines.append(f"  {idx}. {alert_msg}")
    else:
        lines.append("🟢 【运维巡检结论】全部开启平台及子通道运行健康，0 报错、0 空抓，图片与评论富化均正常！")
    lines.append("=" * 112)
    return "\n".join(lines)


# ==============================================================================
# 五、核心执行引擎（支持单轮运行 & 每小时常驻定时守护）
# ==============================================================================
def run_harvest_once(
    switches: Optional[Dict[str, bool]] = None,
    platform_configs: Optional[Dict[str, Dict[str, Any]]] = None,
    global_config: Optional[Dict[str, Any]] = None,
    round_no: int = 1,
) -> Dict[str, Any]:
    """
    执行单轮多平台内容拉取、差异抹平、入库与高密度运维巡检报告输出。
    具备平台级异常隔离：任一平台网络波动或报错，绝不阻塞其余平台正常采集入库。
    """
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
        f"🚀 [多源拉取总控] 开始执行第 #{round_no} 轮采集 | 开启平台: {enabled_platforms} "
        f"| 关闭平台: {disabled_platforms} | 目标集合: [{collection_name}]"
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
        "total_rising_posts": 0,
        "total_local_images": 0,
        "total_comments": 0,
        "total_comment_images": 0,
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
            plat_summary = _analyze_platform_posts(
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
            report["total_rising_posts"] += plat_summary["rising_posts_count"]
            report["total_local_images"] += plat_summary["local_images_mapped"]
            report["total_comments"] += plat_summary["total_comments_attached"]
            report["total_comment_images"] += plat_summary["comment_images_mapped"]
            report["ops_alerts"].extend(plat_summary["alerts"])
            all_harvested_posts.extend(posts)

        except Exception as e:
            cost_s = round(time.monotonic() - plat_start, 2)
            err_alert = (
                f"❌ [{plat_name}] 平台采集抛出未捕获异常: {e} "
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
                "rising_posts_count": 0,
                "posts_with_images": 0,
                "local_images_mapped": 0,
                "posts_with_comments": 0,
                "total_comments_attached": 0,
                "comment_images_mapped": 0,
                "channel_detail": f"异常中断: {e}",
                "top_post_preview": "无",
                "alerts": [err_alert],
                "elapsed_seconds": cost_s,
            }
            report["ops_alerts"].append(err_alert)
            logger.error(f"❌ [{plat_name}] 平台拉取异常（已隔离，不影响其他平台）: {e}", exc_info=True)

    if post_manager:
        try:
            report["collection_total_docs"] = post_manager.collection.count_documents({})
        except Exception:
            pass

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

    ops_banner = format_ops_harvest_report(report)
    logger.info(ops_banner)
    return report


def run_harvest_daemon(interval_seconds: Optional[int] = None):
    """
    常驻定时拉取任务守护进程：
    默认每 3600 秒（1 小时）按 `PLATFORM_SWITCHES` 自动拉取一轮并输出运维巡检报告。
    """
    interval = interval_seconds or GLOBAL_SCHEDULE_CONFIG.get("interval_seconds", 3600)
    logger.info(
        f"⏰ [定时拉取守护进程启动] 轮询周期: [每 {interval} 秒 ({interval / 3600:.1f} 小时)] "
        f"| 目标集合: [{GLOBAL_SCHEDULE_CONFIG['mongo_collection_name']}] "
        f"| 当前开启平台: {[k for k, v in PLATFORM_SWITCHES.items() if v]}"
    )
    round_no = 1
    while True:
        try:
            run_harvest_once(round_no=round_no)
        except Exception as e:
            logger.error(f"❌ [定时拉取守护] 第 #{round_no} 轮外层异常: {e}", exc_info=True)
        round_no += 1
        next_run_time = (datetime.now(TZ_CN) + timedelta(seconds=interval)).strftime("%Y-%m-%d %H:%M:%S")
        logger.info(
            f"💤 [定时拉取守护] 第 #{round_no - 1} 轮结束，休眠 {interval} 秒（下一轮预计执行时间: {next_run_time}）..."
        )
        time.sleep(interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="多平台内容拉取总控与每小时定时入库任务")
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="以常驻定时任务模式循环运行（默认每 3600 秒即 1 小时拉取一轮）",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=GLOBAL_SCHEDULE_CONFIG["interval_seconds"],
        help="定时任务轮询间隔秒数（默认 3600 秒 = 1 小时）",
    )
    parser.add_argument(
        "--only",
        type=str,
        default="",
        help="可选：临时只运行指定平台（用逗号分隔，如 --only x,reddit,hackernews）",
    )
    args = parser.parse_args()

    if args.only:
        selected = {s.strip().lower() for s in args.only.split(",") if s.strip()}
        for k in list(PLATFORM_SWITCHES.keys()):
            PLATFORM_SWITCHES[k] = k in selected

    if args.daemon:
        run_harvest_daemon(interval_seconds=args.interval)
    else:
        run_harvest_once(round_no=1)
