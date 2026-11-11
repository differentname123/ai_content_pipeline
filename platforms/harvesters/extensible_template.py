# -*- coding: utf-8 -*-
"""
未来新增平台（如：知乎 `zhihu`、小红书 `xiaohongshu`）的标准扩展模板

设计说明：
当后续需要接入知乎、小红书或任何新社区平台时，不需要改动任何上游调度器或下游入库/洗稿逻辑，
只需像下面这样继承 `BasePlatformHarvester`，实现本平台的拉取与字段映射即可。
"""
import logging
from typing import Any, Dict, List, Optional

from platforms.harvesters.base_harvester import BasePlatformHarvester

logger = logging.getLogger(__name__)


class ZhihuHarvester(BasePlatformHarvester):
    """
    知乎 (`zhihu`) 平台适配器预留扩展槽位。
    后续实现时：在顶部填写【知乎接口探索与踩坑档案】，并实现以下 4 个方法即可无缝并入总控开关。
    """

    SOURCE_NAME = "zhihu"

    def fetch_hot_or_recommended(self, count: int = 20, **kwargs) -> List[Dict[str, Any]]:
        logger.info("[知乎适配器] 暂未配置知乎采集源，返回空列表")
        return []

    def harvest(self, config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        return self.fetch_hot_or_recommended()


class XiaohongshuHarvester(BasePlatformHarvester):
    """
    小红书 (`xiaohongshu`) 平台适配器预留扩展槽位。
    后续实现时：在顶部填写【小红书接口探索与踩坑档案】，并实现以下 4 个方法即可无缝并入总控开关。
    """

    SOURCE_NAME = "xiaohongshu"

    def fetch_hot_or_recommended(self, count: int = 20, **kwargs) -> List[Dict[str, Any]]:
        logger.info("[小红书适配器] 暂未配置小红书采集源，返回空列表")
        return []

    def harvest(self, config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        return self.fetch_hot_or_recommended()
