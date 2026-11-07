# mongo_manager.py
# -- coding: utf-8 --

import logging
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.common_utils import setup_logger
from common.mongo_db.mongo_base import gen_db_object

setup_logger()
logger = logging.getLogger(__name__)

_INDEXED_COLLECTIONS = set()
_INDEX_LOCK = threading.Lock()


class UniversalPostManager:
    """
    通用社交媒体帖子数据管理器。
    兼容 Binance, OKX, X(Twitter), Zhihu, Xiaohongshu, Bilibili 等全平台通用 Schema。
    """

    COLLECTION_NAME = "social_media_posts"
    UNIQUE_KEYS = ["source", "post_id"]

    def __init__(self, db_instance):
        if not db_instance:
            raise ValueError("必须提供一个有效的 MongoBase 实例")
        self.db = db_instance
        self.collection_name = self.COLLECTION_NAME
        self._ensure_indexes()

    @property
    def collection(self):
        """暴露底层 PyMongo Collection，支持直接调用 delete_many / aggregate 等原生方法。"""
        return self.db.get_collection(self.collection_name)

    def _ensure_indexes(self):
        """
        初始化核心索引（进程周期内每个集合仅执行并输出一次日志）：
        - (source, post_id)  : 联合唯一，防止跨平台 ID 冲突与重复写入
        - (source, publish_time) : 支撑按平台 + 时间窗口的下推过滤查询，避免全表扫描
        - publish_time       : 全局时间线拉取
        - (source, card_type): 平台 / 帖子类型维度统计
        - post_id            : 纯按 ID 列表检索
        """
        if self.collection_name in _INDEXED_COLLECTIONS:
            return
        with _INDEX_LOCK:
            if self.collection_name in _INDEXED_COLLECTIONS:
                return
            self.db.create_index(self.collection_name, [("source", 1), ("post_id", 1)], unique=True)
            self.db.create_index(self.collection_name, [("source", 1), ("publish_time", -1)], unique=False)
            self.db.create_index(self.collection_name, [("publish_time", -1)], unique=False)
            self.db.create_index(self.collection_name, [("source", 1), ("card_type", 1)], unique=False)
            self.db.create_index(self.collection_name, [("post_id", 1)], unique=False)
            _INDEXED_COLLECTIONS.add(self.collection_name)
            logger.info(
                f"[数据库/索引] 原帖集合索引就绪 | 集合: [{self.collection_name}] | 唯一键: [{self.UNIQUE_KEYS}]"
            )

    def upsert_posts(self, data_list):
        """
        将清洗后的通用 Schema 数据批量安全入库。
        - 命中 (source + post_id) -> 更新最新数据 (如点赞、评论数)
        - 未命中               -> 插入新帖
        """
        if not data_list:
            logger.debug("[数据库/入库] upsert_posts 收到空列表，已跳过")
            return

        source_counter = {}
        for i, item in enumerate(data_list):
            post_id = item.get("post_id")
            source = item.get("source")
            if not post_id or not source:
                logger.error(
                    f"❌ [数据库/校验] 帖子入库缺少必填唯一键 | 集合: [{self.collection_name}] | 索引位置: [{i}] "
                    f"| post_id: [{post_id!r}] | source: [{source!r}] | 排查方向: [检查上游采集清洗逻辑是否漏填主键]"
                )
                raise ValueError(f"索引 {i} 数据错误: 必须包含完整的 'post_id' 和 'source'")
            source_counter[source] = source_counter.get(source, 0) + 1

        update_time = datetime.now(timezone.utc)
        for item in data_list:
            item["db_update_time"] = update_time

        start = datetime.now(timezone.utc)
        self.db.bulk_upsert(self.collection_name, data_list, self.UNIQUE_KEYS)
        cost_ms = (datetime.now(timezone.utc) - start).total_seconds() * 1000
        logger.debug(
            f"[数据库/入库] 原帖批量写入完成 | 数量: [{len(data_list)}] | 平台分布: [{source_counter}] | 耗时: [{cost_ms:.1f}ms]"
        )

    def find_posts(self, query=None, sort=None, limit=0, projection=None):
        """通用帖子查询入口（支持任意复合过滤条件、排序与字段投影）。"""
        effective_sort = sort if sort is not None else [("publish_time", -1)]
        return self.db.find_many(
            self.collection_name,
            query=query or {},
            projection=projection,
            sort=effective_sort,
            limit=limit,
        )

    def find_posts_by_source(self, source, limit=100, extra_query=None, projection=None):
        """按平台来源拉取数据，按发布时间最新排序，支持下推附加过滤条件。"""
        query = {"source": source}
        if extra_query and isinstance(extra_query, dict):
            query.update(extra_query)

        posts = self.db.find_many(
            self.collection_name,
            query=query,
            projection=projection,
            sort=[("publish_time", -1)],
            limit=limit,
        )

        logger.debug(
            f"[数据库/查询] 按平台查询完成 | 平台: [{source}] | 上限: [{limit}] | 命中: [{len(posts) if posts else 0}]"
        )
        return posts

    def find_posts_by_ids(self, post_ids, source=None):
        """根据 post_id 列表批量拉取帖子数据。"""
        if not post_ids:
            return []

        query = {"post_id": {"$in": post_ids}}
        if source:
            query["source"] = source

        start = datetime.now(timezone.utc)
        posts = self.db.find_many(self.collection_name, query=query)
        cost_ms = (datetime.now(timezone.utc) - start).total_seconds() * 1000

        logger.debug(
            f"[数据库/查询] 按ID列表查询完成 | 平台: [{source or 'ALL'}] | 目标ID数: [{len(post_ids)}] "
            f"| 命中: [{len(posts) if posts else 0}] | 耗时: [{cost_ms:.1f}ms]"
        )
        return posts

    def delete_posts(self, query: dict) -> int:
        """根据查询条件批量删除帖子记录。"""
        return self.db.delete_many(self.collection_name, query)


class GeneratedArticleManager:
    """
    生成文章管理器，独立使用 generated_articles 集合。
    核心原则：本类只做底层纯粹的增改与查询封装，不做任何业务数据校验或拼接。
    """

    COLLECTION_NAME = "generated_articles"
    UNIQUE_KEYS = ["article_id"]

    def __init__(self, db_instance):
        if not db_instance:
            raise ValueError("必须提供一个有效的 MongoBase 实例")
        self.db = db_instance
        self.collection_name = self.COLLECTION_NAME
        self._ensure_indexes()

    @property
    def collection(self):
        return self.db.get_collection(self.collection_name)

    def _ensure_indexes(self):
        if self.collection_name in _INDEXED_COLLECTIONS:
            return
        with _INDEX_LOCK:
            if self.collection_name in _INDEXED_COLLECTIONS:
                return
            self.db.create_index(self.collection_name, [("article_id", 1)], unique=True)
            self.db.create_index(
                self.collection_name,
                [("type", 1), ("created_at", -1)],
                unique=False,
            )
            self.db.create_index(
                self.collection_name,
                [("source", 1), ("status", 1), ("post_id_list", 1)],
                unique=False,
            )
            self.db.create_index(
                self.collection_name,
                [("source", 1), ("topic", 1), ("stance", 1), ("status", 1), ("created_at", -1)],
                unique=False,
            )
            self.db.create_index(self.collection_name, [("updated_at", -1)], unique=False)
            _INDEXED_COLLECTIONS.add(self.collection_name)
            logger.info(
                f"[数据库/索引] 生成文章集合索引就绪 | 集合: [{self.collection_name}] | 唯一键: [{self.UNIQUE_KEYS}]"
            )

    def upsert_articles(self, data_list):
        """
        通用批量更新/插入文章数据的底层方法。
        自动补齐 updated_at，若为新数据则自动生成 article_id 和 created_at。
        """
        if not data_list:
            logger.debug("[数据库/入库] upsert_articles 收到空列表，已跳过")
            return

        now = datetime.now(timezone.utc)
        for record in data_list:
            if not record.get("article_id"):
                record["article_id"] = uuid4().hex
                record.setdefault("created_at", now)
            record["updated_at"] = now

        self.db.bulk_upsert(self.collection_name, data_list, self.UNIQUE_KEYS)

    def find_articles(self, query=None, sort=None, limit=0):
        """通用查询入口"""
        query = query or {}
        return self.db.find_many(self.collection_name, query=query, sort=sort, limit=limit)

    def find_articles_by_ids(self, article_ids, source=None):
        """根据 article_id 批量精确查询"""
        if not article_ids:
            return []
        query = {"article_id": {"$in": article_ids}}
        if source:
            query["source"] = source
        return self.db.find_many(self.collection_name, query=query)


# ==========================================
# 接入清洗流程的使用示例
# ==========================================
if __name__ == "__main__":
    # 1. 建立数据库连接
    db_instance = gen_db_object()
    post_manager = UniversalPostManager(db_instance)

    # 2. 模拟经 clean_universal_posts / update_posts_in_place 清洗完成后的数据
    cleaned_data = [
        {
            "post_id": "binance_1001",
            "publish_time": 1700000000,
            "author_id": "author_001",
            "card_type": "BUZZ_LONG",
            "source": "binance",
            "metadata": {"url": "...", "is_ai_generated": False},
            "author": {"username": "CryptoKing"},
            "content": {"title": "BTC 分析", "text_content": "今天拉盘..."},
            "engagement": {"view_count": 100, "like_count": 10},
        },
        {
            "post_id": "xhs_6688",
            "publish_time": 1700000500,
            "author_id": "user_xhs1",
            "card_type": "NOTE_IMAGE",
            "source": "xiaohongshu",
            "metadata": {"url": "...", "is_ai_generated": False},
            "author": {"username": "小红薯"},
            "content": {"title": "OOTD", "text_content": "今天穿搭..."},
            "engagement": {"view_count": 500, "like_count": 200},
        },
    ]

    # 3. 一键入库
    post_manager.upsert_posts(cleaned_data)

    # 4. 验证查询
    binance_posts = post_manager.find_posts_by_source("binance", limit=5)
    logger.info(f"样例验证 | binance 帖子数={len(binance_posts) if binance_posts else 0}")
