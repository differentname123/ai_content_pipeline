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

    def __init__(self, db_instance, collection_name: str = None):
        if not db_instance:
            raise ValueError("必须提供一个有效的 MongoBase 实例")
        self.db = db_instance
        self.collection_name = (collection_name or self.COLLECTION_NAME).strip()
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


class HarvestedPostManager(UniversalPostManager):
    """
    全新独立的多平台高价值帖子入库管理器（使用独立集合 `harvested_social_posts`）。
    与历史旧集合 `social_media_posts` 100% 物理隔离，互不干扰。
    支持 X、Reddit、HackerNews、Binance、OKX 以及后续扩展的知乎 (`zhihu`)、小红书 (`xiaohongshu`) 等全部社区帖子。

    核心增强特性：
    1. 历史数据表现追踪 (`engagement_history`)：每次复抓同一帖子时，自动追加带时间戳的互动快照（最多保留最近 48 个快照点），绝不丢失历史轨迹。
    2. 蹿升速度计算 (`delta_likes`, `delta_views`, `likes_per_hour`, `views_per_hour`, `poll_count`)：直接赋能下游「潜力黑马帖 / 爆发斜率」识别。
    3. 非破坏性合并：复抓时保留首次发现时间 `first_seen_at`，并智能合并已下载的本地图片映射 (`media.local_mapping`) 与热评列表 (`comments`)。
    """

    COLLECTION_NAME = "harvested_social_posts"
    UNIQUE_KEYS = ["source", "post_id"]
    MAX_HISTORY_SNAPSHOTS = 48

    def _ensure_indexes(self):
        super()._ensure_indexes()
        index_tag = f"{self.collection_name}::surge_idx"
        if index_tag in _INDEXED_COLLECTIONS:
            return
        with _INDEX_LOCK:
            if index_tag in _INDEXED_COLLECTIONS:
                return
            self.db.create_index(self.collection_name, [("engagement.viral_score", -1)], unique=False)
            self.db.create_index(self.collection_name, [("engagement.likes_per_hour", -1)], unique=False)
            self.db.create_index(self.collection_name, [("engagement.poll_count", -1)], unique=False)
            _INDEXED_COLLECTIONS.add(index_tag)

    def upsert_posts(self, data_list):
        """
        将清洗后的通用 Schema 数据批量安全入库，并自动计算互动历史快照 (`engagement_history`) 与蹿升斜率指标。
        """
        if not data_list:
            logger.debug("[数据库/入库] HarvestedPostManager.upsert_posts 收到空列表，已跳过")
            return

        source_counter = {}
        ids_by_source = {}
        for i, item in enumerate(data_list):
            post_id = str(item.get("post_id") or "").strip()
            source = str(item.get("source") or "").strip()
            if not post_id or not source:
                raise ValueError(f"索引 {i} 数据错误: 必须包含完整的 'post_id' 和 'source'")
            item["post_id"] = post_id
            item["source"] = source
            source_counter[source] = source_counter.get(source, 0) + 1
            ids_by_source.setdefault(source, []).append(post_id)

        # 1. 批量查询库中已存在的同源帖子，用于构建历史快照与计算增量
        existing_map = {}
        projection = {
            "source": 1,
            "post_id": 1,
            "publish_time": 1,
            "first_seen_at": 1,
            "db_update_time": 1,
            "engagement": 1,
            "engagement_history": 1,
            "media.local_mapping": 1,
            "comments": 1,
        }
        for src, pids in ids_by_source.items():
            existing_docs = self.db.find_many(
                self.collection_name,
                query={"source": src, "post_id": {"$in": list(set(pids))}},
                projection=projection,
                limit=len(pids) + 10,
            )
            for doc in existing_docs or []:
                existing_map[(doc.get("source"), str(doc.get("post_id")))] = doc

        now_dt = datetime.now(timezone.utc)
        now_ts = int(now_dt.timestamp())
        now_str = now_dt.strftime("%Y-%m-%d %H:%M:%S UTC")

        new_count = 0
        updated_count = 0

        for item in data_list:
            key = (item["source"], item["post_id"])
            old_doc = existing_map.get(key)

            eng = item.setdefault("engagement", {})
            cur_views = int(eng.get("view_count") or 0)
            cur_likes = int(eng.get("like_count") or 0)
            cur_comments = int(eng.get("comment_count") or 0)
            cur_shares = int(eng.get("share_count") or 0)
            cur_bookmarks = int(eng.get("bookmark_count") or 0)
            cur_viral = float(eng.get("viral_score") or 0.0)

            cur_snap = {
                "ts": now_ts,
                "time_str": now_str,
                "view_count": cur_views,
                "like_count": cur_likes,
                "comment_count": cur_comments,
                "share_count": cur_shares,
                "bookmark_count": cur_bookmarks,
                "viral_score": cur_viral,
            }

            pub_ts = int(item.get("publish_time") or 0)
            age_hours = max((now_ts - pub_ts) / 3600.0, 0.1) if (pub_ts > 0 and now_ts > pub_ts) else 1.0

            if old_doc:
                updated_count += 1
                item["first_seen_at"] = old_doc.get("first_seen_at") or old_doc.get("db_update_time") or now_dt

                # 恢复并追加 engagement_history
                history = list(old_doc.get("engagement_history") or [])
                old_eng = old_doc.get("engagement") or {}
                if not history and old_eng:
                    old_dt = old_doc.get("db_update_time") or item["first_seen_at"]
                    old_ts = int(old_dt.timestamp()) if hasattr(old_dt, "timestamp") else (now_ts - 60)
                    old_str = (
                        old_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
                        if hasattr(old_dt, "strftime")
                        else now_str
                    )
                    history.append({
                        "ts": old_ts,
                        "time_str": old_str,
                        "view_count": int(old_eng.get("view_count") or 0),
                        "like_count": int(old_eng.get("like_count") or 0),
                        "comment_count": int(old_eng.get("comment_count") or 0),
                        "share_count": int(old_eng.get("share_count") or 0),
                        "bookmark_count": int(old_eng.get("bookmark_count") or 0),
                        "viral_score": float(old_eng.get("viral_score") or 0.0),
                    })

                prev_snap = history[-1] if history else cur_snap
                delta_likes = cur_likes - int(prev_snap.get("like_count") or 0)
                delta_views = cur_views - int(prev_snap.get("view_count") or 0)

                elapsed_sec = max(now_ts - int(prev_snap.get("ts") or now_ts), 0)
                if elapsed_sec >= 60 and (delta_likes > 0 or delta_views > 0):
                    window_hours = elapsed_sec / 3600.0
                    likes_per_hour = round(delta_likes / window_hours, 2)
                    views_per_hour = round(delta_views / window_hours, 2)
                elif delta_likes > 0 or delta_views > 0:
                    window_hours = max(elapsed_sec / 3600.0, 1.0 / 60.0)
                    likes_per_hour = round(delta_likes / window_hours, 2)
                    views_per_hour = round(delta_views / window_hours, 2)
                else:
                    likes_per_hour = round(cur_likes / age_hours, 2)
                    views_per_hour = round(cur_views / age_hours, 2)

                history.append(cur_snap)
                if len(history) > self.MAX_HISTORY_SNAPSHOTS:
                    history = history[-self.MAX_HISTORY_SNAPSHOTS :]

                item["engagement_history"] = history
                eng["poll_count"] = len(history)
                eng["delta_likes"] = delta_likes
                eng["delta_views"] = delta_views
                eng["likes_per_hour"] = likes_per_hour
                eng["views_per_hour"] = views_per_hour

                # 非破坏性合并本地图片映射与已拉取的热评
                old_mapping = (old_doc.get("media") or {}).get("local_mapping") or {}
                if old_mapping:
                    cur_media = item.setdefault("media", {})
                    cur_mapping = cur_media.setdefault("local_mapping", {})
                    for k_url, v_path in old_mapping.items():
                        cur_mapping.setdefault(k_url, v_path)

                old_comments = old_doc.get("comments") or []
                cur_comments_list = item.get("comments") or []
                if not cur_comments_list and old_comments:
                    item["comments"] = old_comments
                elif cur_comments_list and old_comments:
                    merged_by_id = {str(c.get("reply_id") or ""): c for c in old_comments if isinstance(c, dict)}
                    for c in cur_comments_list:
                        if isinstance(c, dict):
                            rid = str(c.get("reply_id") or "")
                            if rid in merged_by_id and merged_by_id[rid].get("local_mapping") and not c.get("local_mapping"):
                                c["local_mapping"] = merged_by_id[rid]["local_mapping"]
                            merged_by_id[rid] = c
                    merged_list = list(merged_by_id.values())
                    merged_list.sort(key=lambda x: int(x.get("likes") or 0), reverse=True)
                    item["comments"] = merged_list[:30]
            else:
                new_count += 1
                item["first_seen_at"] = now_dt
                item["engagement_history"] = [cur_snap]
                eng["poll_count"] = 1
                eng["delta_likes"] = 0
                eng["delta_views"] = 0
                eng["likes_per_hour"] = round(cur_likes / age_hours, 2)
                eng["views_per_hour"] = round(cur_views / age_hours, 2)

            item["db_update_time"] = now_dt

        start = datetime.now(timezone.utc)
        self.db.bulk_upsert(self.collection_name, data_list, self.UNIQUE_KEYS)
        cost_ms = (datetime.now(timezone.utc) - start).total_seconds() * 1000
        logger.info(
            f"[数据库/入库] harvested_social_posts 批量写入完成 | 总数: [{len(data_list)}] "
            f"(新增: [{new_count}], 复抓更新快照: [{updated_count}]) | 平台分布: [{source_counter}] | 耗时: [{cost_ms:.1f}ms]"
        )



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
