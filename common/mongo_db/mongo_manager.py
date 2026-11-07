# mongo_manager.py
# -- coding: utf-8 --

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import logging
from uuid import uuid4
from datetime import datetime, timezone # 替换原有的 from datetime import datetime
from common.common_utils import setup_logger
from common.mongo_db.mongo_base import gen_db_object

setup_logger()

# 拿到属于当前文件的专属 logger
logger = logging.getLogger(__name__)

class UniversalPostManager:
    """
    通用社交媒体帖子数据管理器。
    兼容 Binance, Zhihu, Xiaohongshu, Bilibili 等全平台通用 Schema。
    """

    COLLECTION_NAME = "social_media_posts"
    UNIQUE_KEYS = ["source", "post_id"]

    def __init__(self, db_instance):
        if not db_instance:
            raise ValueError("必须提供一个有效的 MongoBase 实例")
        self.db = db_instance
        self.collection_name = self.COLLECTION_NAME
        self._ensure_indexes()

    def _ensure_indexes(self):
        """
        初始化核心索引，保障查询速度与数据隔离。
        - source + post_id : 联合唯一，防止跨平台 ID 冲突与重复写入 (遵循最左前缀)
        - publish_time     : 时间线拉取
        - source + card_type : 平台 / 帖子类型维度统计
        - post_id          : 新增普通索引，用于脱离 source 纯按 ID 检索的场景
        """
        self.db.create_index(self.collection_name, [('source', 1), ('post_id', 1)], unique=True)
        self.db.create_index(self.collection_name, [('publish_time', -1)], unique=False)
        self.db.create_index(self.collection_name, [('source', 1), ('card_type', 1)], unique=False)

        # 【新增索引】：为了支持单纯按 post_id 列表查询而不引起全表扫描
        self.db.create_index(self.collection_name, [('post_id', 1)], unique=False)

        logger.info(
            "索引就绪 | collection=%s | indexes=[uniq(source,post_id), publish_time(-1), (source,card_type), post_id]",
            self.collection_name
        )

    def upsert_posts(self, data_list):
        """
        将清洗后的通用 Schema 数据批量安全入库。
        - 命中 (source + post_id) -> 更新最新数据 (如点赞、评论数)
        - 未命中               -> 插入新帖
        """
        if not data_list:
            logger.warning("upsert_posts 收到空数据集，已跳过入库")
            return

        # 先做全量前置校验，再统一打标，避免校验失败时残留脏副作用
        source_counter = {}
        for i, item in enumerate(data_list):
            post_id = item.get("post_id")
            source = item.get("source")
            if not post_id or not source:
                logger.error(
                    "入库校验失败 | index=%s | post_id=%r | source=%r | reason=缺失联合唯一键字段",
                    i, post_id, source
                )
                raise ValueError(f"索引 {i} 数据错误: 必须包含完整的 'post_id' 和 'source'")
            source_counter[source] = source_counter.get(source, 0) + 1

        # 校验全部通过后，统一追加最后更新时间（UTC，避免跨时区歧义）
        update_time = datetime.now(timezone.utc)
        for item in data_list:
            item['db_update_time'] = update_time

        start = datetime.now(timezone.utc)
        self.db.bulk_upsert(self.collection_name, data_list, self.UNIQUE_KEYS)
        cost_ms = (datetime.now(timezone.utc) - start).total_seconds() * 1000
        logger.info(
            "批量入库完成 | total=%s | dist=%s | cost=%.1fms | keys=%s",
            len(data_list), source_counter, cost_ms, self.UNIQUE_KEYS
        )

    def find_posts_by_source(self, source, limit=100):
        """按平台来源拉取数据，按发布时间最新排序"""
        posts = self.db.find_many(
            self.collection_name,
            query={"source": source},
            sort=[("publish_time", -1)],
            limit=limit
        )

        logger.info(
            "查询完成 | source=%s | limit=%s | matched=%s",
            source, limit, len(posts) if posts else 0
        )
        return posts

    def find_posts_by_ids(self, post_ids, source=None):
        """
        根据 post_id 列表批量拉取帖子数据。

        :param post_ids: list[str], 帖子 ID 列表 (例如: ["binance_1001", "xhs_6688"])
        :param source: str (可选), 指定平台来源。
                       强烈建议传入此参数！不仅能防止不同平台间偶然的 ID 冲突，
                       还能直接命中 (source, post_id) 的联合唯一索引，查询最快。
        :return: list[dict], 匹配的帖子列表
        """
        if not post_ids:
            return []

        # 核心语法：使用 MongoDB 的 $in 操作符
        query = {"post_id": {"$in": post_ids}}

        # 如果提供了 source，追加到查询条件中
        if source:
            query["source"] = source

        start = datetime.now(timezone.utc)
        posts = self.db.find_many(
            self.collection_name,
            query=query
        )
        cost_ms = (datetime.now(timezone.utc) - start).total_seconds() * 1000

        logger.info(
            "按ID列表查询完成 | source=%s | id_count=%s | matched=%s | cost=%.1fms",
            source or "ALL_PLATFORMS", len(post_ids), len(posts) if posts else 0, cost_ms
        )
        return posts


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

    def _ensure_indexes(self):
        self.db.create_index(self.collection_name, [('article_id', 1)], unique=True)
        self.db.create_index(
            self.collection_name,
            [('source', 1), ('status', 1), ('post_id_list', 1)],
            unique=False
        )
        self.db.create_index(
            self.collection_name,
            [('source', 1), ('topic', 1), ('stance', 1), ('status', 1), ('created_at', -1)],
            unique=False
        )
        self.db.create_index(self.collection_name, [('updated_at', -1)], unique=False)

    def upsert_articles(self, data_list):
        """
        通用批量更新/插入文章数据的底层方法。
        自动补齐 updated_at，若为新数据则自动生成 article_id 和 created_at。
        """
        if not data_list:
            logger.warning("upsert_articles 收到空数据集，已跳过入库")
            return

        now = datetime.now(timezone.utc)
        for record in data_list:
            # 若没有 article_id，视为新数据并补齐主键与创建时间
            if not record.get('article_id'):
                record['article_id'] = uuid4().hex
                record.setdefault('created_at', now)
            # 无论新增还是更新，永远刷新 updated_at
            record['updated_at'] = now

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
            "engagement": {"view_count": 100, "like_count": 10}
        },
        {
            "post_id": "xhs_6688",  # 不同平台的 ID 逻辑完全不同
            "publish_time": 1700000500,
            "author_id": "user_xhs1",
            "card_type": "NOTE_IMAGE",
            "source": "xiaohongshu",
            "metadata": {"url": "...", "is_ai_generated": False},
            "author": {"username": "小红薯"},
            "content": {"title": "OOTD", "text_content": "今天穿搭..."},
            "engagement": {"view_count": 500, "like_count": 200}
        }
    ]

    # 3. 一键入库
    post_manager.upsert_posts(cleaned_data)

    # 4. 验证查询
    binance_posts = post_manager.find_posts_by_source("binance", limit=5)
    logger.info("样例验证 | binance 帖子数=%s", len(binance_posts) if binance_posts else 0)
