# -*- coding: utf-8 -*-
"""
多平台高价值帖子拉取总控面板与定时入库任务 (`harvester_runner.py`)

================================================================================
【使用说明】
1. 使用者只需修改最上方的 `PLATFORM_SWITCHES` 字典（`True` 开启 / `False` 关闭），
   即可一键控制本轮或定时任务要拉取哪些平台的数据。
2. 所有拉取到的帖子均由各平台专属适配器（`platforms/harvesters/*_harvester.py`）
   自动抹平字段差异、注入 `[插图: url]`、并发下载配图至 `media_downloads/`，
   并统一写入全新独立的 MongoDB 集合 `harvested_social_posts`（与历史旧集合 `social_media_posts` 100% 物理隔离）。
3. 运行方式：
   - 单次立即拉取并入库：`python platforms/harvester_runner.py`
   - 启动常驻定时任务：  `python platforms/harvester_runner.py --daemon --interval 1800`
================================================================================
"""
import argparse
import json
import logging
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
    save_json,
    setup_logger,
)
from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import HarvestedPostManager
from platforms.harvesters import (
    HARVESTER_REGISTRY,
    NEW_HARVESTED_COLLECTION_NAME,
)

setup_logger(app_name="harvester_runner")
logger = logging.getLogger(__name__)
TZ_CN = timezone(timedelta(hours=8))


# ==============================================================================
# 一、【最上层总控开关】开启 (True) 或 关闭 (False) 对应平台的拉取
# ==============================================================================
PLATFORM_SWITCHES: Dict[str, bool] = {
    "x": True,              # 英文 X (Twitter)：顶流大V、真实豪赌赔率(@Polymarket/@Kalshi)、内幕吃瓜与神评
    "reddit": True,         # 英文 Reddit：10大黄金板块 24h 暴富暴负交割单、科技大瓜与高赞神评
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
    # 全新独立的 MongoDB 集合名称（绝不与历史 `social_media_posts` 产生交集）
    "mongo_collection_name": NEW_HARVESTED_COLLECTION_NAME,  # 默认: "harvested_social_posts"
    # 是否启用 MongoDB 自动入库
    "enable_mongo_upsert": True,
    # 是否并发下载帖子配图到本地 `media_downloads/` 并建立 `media.local_mapping`
    "download_media": True,
    # 定时任务默认轮询间隔（秒），1800 秒 = 每 30 分钟自动跑一轮
    "interval_seconds": 1800,
    # 每次运行后在 logs/runtime/ 下保存一份最新拉取快照便于直观检查
    "snapshot_filename": "latest_harvested_posts_snapshot.json",
}


# ==============================================================================
# 三、【各平台拉取范围与价值过滤细则配置】
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
        "tweets_per_handle": 6,
        "include_home_feed": True,
        "home_feed_count": 25,
        "fetch_comments_for_top_n": 4,
        # 价值过滤门槛：剔除低于 20 赞且低于 1000 浏览的无意义水帖
        "min_likes": 20,
        "min_views": 1000,
        "min_text_length": 15,
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
        "min_text_length": 15,
    },
    "hackernews": {
        "limit": 12,
        "min_points": 50,
        "keywords": ["AI", "OpenAI", "Nvidia", "crypto", "billion"],
        "fetch_comments_for_top_n": 5,
        "min_text_length": 15,
    },
    "binance": {
        "recommend_count": 15,
        "keywords": [],
        "coins": ["BTC", "ETH", "SOL"],
        "per_coin_count": 6,
        "min_views": 300,
        "min_likes": 3,
        "min_text_length": 15,
    },
    "okx": {
        "recommend_count": 12,
        "coins": ["BTC", "ETH", "SOL"],
        "per_coin_count": 5,
        "author_ids": [],
        "min_views": 300,
        "min_likes": 2,
        "min_text_length": 15,
    },
    "zhihu": {},
    "xiaohongshu": {},
}


# ==============================================================================
# 四、核心执行引擎（支持单轮运行 & 常驻定时任务守护）
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


def run_harvest_once(
    switches: Optional[Dict[str, bool]] = None,
    platform_configs: Optional[Dict[str, Dict[str, Any]]] = None,
    global_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    执行单轮多平台高价值内容拉取、差异抹平与新集合 (`harvested_social_posts`) 幂等入库。
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
        f"🚀 [多源拉取总控] 开始执行本轮采集 | 开启平台: {enabled_platforms} | 关闭平台: {disabled_platforms} "
        f"| 目标独立集合: [{collection_name}]"
    )

    post_manager: Optional[HarvestedPostManager] = None
    if enable_mongo:
        post_manager = HarvestedPostManager(gen_db_object(), collection_name=collection_name)

    report: Dict[str, Any] = {
        "run_time": datetime.now(TZ_CN).strftime("%Y-%m-%d %H:%M:%S"),
        "target_collection": collection_name,
        "enabled_platforms": enabled_platforms,
        "disabled_platforms": disabled_platforms,
        "platform_stats": {},
        "total_harvested": 0,
        "total_upserted_to_mongo": 0,
        "total_local_images": 0,
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
            upserted_count = 0

            if post_manager and posts:
                post_manager.upsert_posts(posts)
                upserted_count = len(posts)
                for p in posts:
                    if isinstance(p.get("db_update_time"), datetime):
                        p["db_update_time"] = p["db_update_time"].isoformat()

            img_count = sum(len((p.get("media") or {}).get("local_mapping") or {}) for p in posts)
            comment_count = sum(len(p.get("comments") or []) for p in posts)
            cost_s = round(time.monotonic() - plat_start, 2)

            report["platform_stats"][plat_name] = {
                "status": "ok",
                "harvested_count": len(posts),
                "mongo_upserted": upserted_count,
                "local_images_mapped": img_count,
                "total_comments_attached": comment_count,
                "elapsed_seconds": cost_s,
            }
            report["total_harvested"] += len(posts)
            report["total_upserted_to_mongo"] += upserted_count
            report["total_local_images"] += img_count
            all_harvested_posts.extend(posts)

            logger.info(
                f"✅ [{plat_name}] 平台拉取与入库成功 | 有效标准帖: [{len(posts)}条] | 入库集合[{collection_name}]: [{upserted_count}条] "
                f"| 本地配图: [{img_count}张] | 附带神评: [{comment_count}条] | 耗时: [{cost_s}s]"
            )
        except Exception as e:
            cost_s = round(time.monotonic() - plat_start, 2)
            report["platform_stats"][plat_name] = {
                "status": "error",
                "error_message": str(e),
                "harvested_count": 0,
                "mongo_upserted": 0,
                "elapsed_seconds": cost_s,
            }
            logger.error(f"❌ [{plat_name}] 平台拉取异常（已隔离，不影响其他平台）: {e}", exc_info=True)

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

    logger.info(
        f"🏁 [多源拉取总控] 本轮执行完毕 | 开启平台: [{len(enabled_platforms)}个] | 总计入库 [{collection_name}]: "
        f"[{report['total_upserted_to_mongo']}条] | 本地配图: [{report['total_local_images']}张] | 总耗时: [{report['elapsed_seconds']}s]"
    )
    return report


def run_harvest_daemon(interval_seconds: Optional[int] = None):
    """
    常驻定时拉取任务守护进程：
    按照 `PLATFORM_SWITCHES` 开启的平台周期性自动拉取并写入独立集合 `harvested_social_posts`。
    """
    interval = interval_seconds or GLOBAL_SCHEDULE_CONFIG.get("interval_seconds", 1800)
    logger.info(
        f"⏰ [定时拉取守护进程启动] 轮询周期: [每 {interval} 秒] | 目标独立集合: [{GLOBAL_SCHEDULE_CONFIG['mongo_collection_name']}] "
        f"| 当前开启平台: {[k for k, v in PLATFORM_SWITCHES.items() if v]}"
    )
    round_no = 1
    while True:
        try:
            logger.info(f"========== 开始第 #{round_no} 轮定时多平台拉取 ==========")
            run_harvest_once()
        except Exception as e:
            logger.error(f"❌ [定时拉取守护] 第 #{round_no} 轮外层异常: {e}", exc_info=True)
        round_no += 1
        logger.info(f"💤 [定时拉取守护] 本轮结束，休眠 {interval} 秒后进入下一轮...")
        time.sleep(interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="多平台高价值内容拉取总控与定时入库任务")
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="以常驻定时任务模式循环运行（默认单次立即运行一轮）",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=GLOBAL_SCHEDULE_CONFIG["interval_seconds"],
        help="定时任务轮询间隔秒数（默认 1800 秒）",
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
        summary_report = run_harvest_once()
        print("\n" + "=" * 95)
        print("📊 多平台高价值内容拉取 & 独立集合 (harvested_social_posts) 入库汇总报告")
        print("=" * 95)
        print(json.dumps(summary_report, indent=2, ensure_ascii=False))
