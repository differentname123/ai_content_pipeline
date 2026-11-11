# -*- coding: utf-8 -*-
"""
多平台高价值帖子拉取适配器注册中心 (`platforms.harvesters`)
"""
from platforms.harvesters.base_harvester import (
    NEW_HARVESTED_COLLECTION_NAME,
    BasePlatformHarvester,
)
from platforms.harvesters.binance_harvester import BinanceHarvester
from platforms.harvesters.extensible_template import (
    XiaohongshuHarvester,
    ZhihuHarvester,
)
from platforms.harvesters.hackernews_harvester import HackerNewsHarvester
from platforms.harvesters.okx_harvester import OkxHarvester
from platforms.harvesters.reddit_harvester import RedditHarvester
from platforms.harvesters.x_harvester import XHarvester

# 平台唯一标识 -> 适配器类 映射注册表
HARVESTER_REGISTRY = {
    "x": XHarvester,
    "reddit": RedditHarvester,
    "hackernews": HackerNewsHarvester,
    "binance": BinanceHarvester,
    "okx": OkxHarvester,
    "zhihu": ZhihuHarvester,
    "xiaohongshu": XiaohongshuHarvester,
}

__all__ = [
    "NEW_HARVESTED_COLLECTION_NAME",
    "BasePlatformHarvester",
    "XHarvester",
    "RedditHarvester",
    "HackerNewsHarvester",
    "BinanceHarvester",
    "OkxHarvester",
    "ZhihuHarvester",
    "XiaohongshuHarvester",
    "HARVESTER_REGISTRY",
]
