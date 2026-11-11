# -*- coding: utf-8 -*-
"""
向后兼容入口 (Compatibility Wrapper)

注意：
全平台采集与差异抹平架构已升级为模块化包：
- 顶层平台开关与定时调度入口：`platforms/harvester_runner.py`
- 各平台独立适配器与踩坑档案：`platforms/harvesters/{x,reddit,hackernews,binance,okx}_harvester.py`

请直接运行或引用 `platforms/harvester_runner.py`。本文件保留别名导出以兼容旧脚本调用。
"""
from platforms.harvester_runner import (
    GLOBAL_SCHEDULE_CONFIG,
    HARVESTER_CONFIG,
    PLATFORM_HARVEST_CONFIGS,
    PLATFORM_SWITCHES,
    create_harvester_instance,
    run_continuous_daemon,
    run_harvest_daemon,
    run_harvest_once,
)
from platforms.harvesters import (
    HARVESTER_REGISTRY,
    NEW_HARVESTED_COLLECTION_NAME,
    BasePlatformHarvester,
    BinanceHarvester,
    HackerNewsHarvester,
    OkxHarvester,
    RedditHarvester,
    XiaohongshuHarvester,
    XHarvester,
    ZhihuHarvester,
)

__all__ = [
    "PLATFORM_SWITCHES",
    "GLOBAL_SCHEDULE_CONFIG",
    "PLATFORM_HARVEST_CONFIGS",
    "HARVESTER_CONFIG",
    "HARVESTER_REGISTRY",
    "NEW_HARVESTED_COLLECTION_NAME",
    "BasePlatformHarvester",
    "XHarvester",
    "RedditHarvester",
    "HackerNewsHarvester",
    "BinanceHarvester",
    "OkxHarvester",
    "ZhihuHarvester",
    "XiaohongshuHarvester",
    "create_harvester_instance",
    "run_harvest_once",
    "run_harvest_daemon",
    "run_continuous_daemon",
]

if __name__ == "__main__":
    run_harvest_once()
