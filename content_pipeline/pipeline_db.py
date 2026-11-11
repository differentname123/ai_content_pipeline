# -*- coding: utf-8 -*-
"""
流水线专用数据库访问层 (`pipeline_db.py`)

物理隔离承诺：
1. 对 `harvested_social_posts` 严格只读（Read-Only），绝不写入或覆盖采集器字段。
2. Step 2 / Step 3 / Step 4 与运行轨迹分别写入独立的 `pipeline_*` 集合，与历史旧集合完全解耦。
"""
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from common.mongo_db.mongo_base import MongoBase, gen_db_object
from content_pipeline.pipeline_config import PIPELINE_COLLECTIONS

_INDEX_READY: Set[str] = set()
_INDEX_LOCK = threading.Lock()


def make_post_key(source: str, post_id: str) -> str:
    return f"{str(source).strip()}:{str(post_id).strip()}"


def split_post_key(post_key: str) -> Tuple[str, str]:
    raw = str(post_key or "").strip()
    if ":" not in raw:
        return "", raw
    src, pid = raw.split(":", 1)
    return src.strip(), pid.strip()


class PipelineDB:
    """统一管理新流水线的只读源查询与三阶段产物持久化。"""

    def __init__(self, db_instance: Optional[MongoBase] = None):
        self.db: MongoBase = db_instance or gen_db_object()
        self.raw_col = PIPELINE_COLLECTIONS["raw_posts"]
        self.digest_col = PIPELINE_COLLECTIONS["post_digests"]
        self.topic_col = PIPELINE_COLLECTIONS["topic_clusters"]
        self.creation_col = PIPELINE_COLLECTIONS["creations"]
        self.trace_col = PIPELINE_COLLECTIONS["run_traces"]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        tag = f"{self.digest_col}|{self.topic_col}|{self.creation_col}|{self.trace_col}"
        if tag in _INDEX_READY:
            return
        with _INDEX_LOCK:
            if tag in _INDEX_READY:
                return
            # Step 2 情报卡集合索引
            self.db.create_index(self.digest_col, [("source", 1), ("post_id", 1)], unique=True)
            self.db.create_index(self.digest_col, [("post_key", 1)], unique=True)
            self.db.create_index(self.digest_col, [("is_usable", 1), ("publish_time", -1)], unique=False)
            self.db.create_index(self.digest_col, [("digested_at_ts", -1)], unique=False)

            # Step 3 选题集合索引
            self.db.create_index(self.topic_col, [("topic_id", 1)], unique=True)
            self.db.create_index(self.topic_col, [("run_id", 1)], unique=False)
            self.db.create_index(self.topic_col, [("status", 1), ("created_at_ts", -1)], unique=False)

            # Step 4 成品集合索引
            self.db.create_index(self.creation_col, [("creation_id", 1)], unique=True)
            self.db.create_index(self.creation_col, [("topic_id", 1)], unique=False)
            self.db.create_index(self.creation_col, [("profile_name", 1), ("created_at_ts", -1)], unique=False)
            self.db.create_index(self.creation_col, [("source_post_keys", 1)], unique=False)

            # 全链路运行轨迹索引
            self.db.create_index(self.trace_col, [("run_id", 1)], unique=True)
            self.db.create_index(self.trace_col, [("started_at_ts", -1)], unique=False)
            _INDEX_READY.add(tag)

    # --------------------------------------------------------------------------
    # Step 1 -> Step 2 只读原帖查询
    # --------------------------------------------------------------------------
    def fetch_recent_raw_posts(self, lookback_hours: float = 48.0, limit: int = 500) -> List[Dict[str, Any]]:
        """获取最近窗口内的原始标准化帖子，按热力分与发布时间优先排序。"""
        cutoff_ts = int(time.time() - max(lookback_hours, 1.0) * 3600)
        # 同时兼容 publish_time 在窗口内，或近期新入库的帖子
        cutoff_dt = datetime.fromtimestamp(cutoff_ts, tz=timezone.utc)
        query = {
            "$or": [
                {"publish_time": {"$gte": cutoff_ts}},
                {"db_update_time": {"$gte": cutoff_dt}},
            ]
        }
        docs = self.db.find_many(
            self.raw_col,
            query=query,
            sort=[("engagement.viral_score", -1), ("publish_time", -1)],
            limit=limit,
        )
        return docs or []

    def fetch_raw_posts_by_keys(self, post_keys: Sequence[str]) -> Dict[str, Dict[str, Any]]:
        """按 `source:post_id` 列表精确批量取回最新原帖（含最新实时互动快照与本地图片映射）。"""
        grouped_ids: Dict[str, List[str]] = {}
        for pk in post_keys or []:
            src, pid = split_post_key(pk)
            if src and pid:
                grouped_ids.setdefault(src, []).append(pid)

        out: Dict[str, Dict[str, Any]] = {}
        for src, pids in grouped_ids.items():
            docs = self.db.find_many(
                self.raw_col,
                query={"source": src, "post_id": {"$in": list(set(pids))}},
                limit=len(pids) + 5,
            )
            for d in docs or []:
                out[make_post_key(d.get("source", ""), d.get("post_id", ""))] = d
        return out

    # --------------------------------------------------------------------------
    # Step 2 情报卡读写 (`pipeline_post_digests`)
    # --------------------------------------------------------------------------
    def get_existing_digest_keys(self, post_keys: Sequence[str]) -> Set[str]:
        """查询哪些 `post_key` 已经成功完成 Step 2 理解，支持幂等增量跳过。"""
        if not post_keys:
            return set()
        docs = self.db.find_many(
            self.digest_col,
            query={"post_key": {"$in": list(set(post_keys))}, "status": "ok"},
            projection={"post_key": 1},
            limit=len(post_keys) + 5,
        )
        return {str(d.get("post_key")) for d in (docs or []) if d.get("post_key")}

    def upsert_digests(self, digests: List[Dict[str, Any]]) -> None:
        if not digests:
            return
        now_dt = datetime.now(timezone.utc)
        for item in digests:
            item["updated_at"] = now_dt
        self.db.bulk_upsert(self.digest_col, digests, ["source", "post_id"])

    def fetch_usable_digests(self, lookback_hours: float = 36.0, limit: int = 300) -> List[Dict[str, Any]]:
        """获取时间窗口内所有 `is_usable=True` 的完整帖子情报卡。"""
        cutoff_ts = int(time.time() - max(lookback_hours, 1.0) * 3600)
        query = {
            "status": "ok",
            "is_usable": True,
            "$or": [
                {"publish_time": {"$gte": cutoff_ts}},
                {"digested_at_ts": {"$gte": cutoff_ts}},
            ],
        }
        docs = self.db.find_many(
            self.digest_col,
            query=query,
            sort=[("digested_at_ts", -1)],
            limit=limit,
        )
        return docs or []

    def fetch_digests_by_keys(self, post_keys: Sequence[str]) -> Dict[str, Dict[str, Any]]:
        if not post_keys:
            return {}
        docs = self.db.find_many(
            self.digest_col,
            query={"post_key": {"$in": list(set(post_keys))}},
            limit=len(post_keys) + 5,
        )
        return {str(d.get("post_key")): d for d in (docs or []) if d.get("post_key")}

    # --------------------------------------------------------------------------
    # Step 3 话题归堆读写 (`pipeline_topic_clusters`)
    # --------------------------------------------------------------------------
    def upsert_topics(self, topics: List[Dict[str, Any]]) -> None:
        if not topics:
            return
        now_dt = datetime.now(timezone.utc)
        for t in topics:
            t["updated_at"] = now_dt
        self.db.bulk_upsert(self.topic_col, topics, ["topic_id"])

    def fetch_pending_topics(self, run_id: Optional[str] = None, limit: int = 10) -> List[Dict[str, Any]]:
        query: Dict[str, Any] = {"status": "pending_creation"}
        if run_id:
            query["run_id"] = run_id
        return self.db.find_many(
            self.topic_col,
            query=query,
            sort=[("rank", 1), ("created_at_ts", -1)],
            limit=limit,
        ) or []

    # --------------------------------------------------------------------------
    # Step 4 原创成品读写与防重复查询 (`pipeline_creations`)
    # --------------------------------------------------------------------------
    def fetch_recent_creation_memory(
        self,
        profile_name: str,
        lookback_hours: float = 48.0,
    ) -> Tuple[Set[str], List[str]]:
        """
        返回 `(近期已被消费过的原帖 post_key 集合, 近期已生成的选题标题列表)`，
        用于 Step 3 硬过滤已用原帖 + 软记忆防旧闻复读。
        """
        cutoff_ts = int(time.time() - max(lookback_hours, 1.0) * 3600)
        docs = self.db.find_many(
            self.creation_col,
            query={
                "profile_name": profile_name,
                "status": "ok",
                "created_at_ts": {"$gte": cutoff_ts},
            },
            projection={"source_post_keys": 1, "topic_title": 1, "title": 1},
            sort=[("created_at_ts", -1)],
            limit=100,
        )
        used_keys: Set[str] = set()
        recent_titles: List[str] = []
        for d in docs or []:
            for pk in d.get("source_post_keys") or []:
                used_keys.add(str(pk))
            title_label = (d.get("topic_title") or d.get("title") or "").strip()
            if title_label and title_label not in recent_titles:
                recent_titles.append(title_label)
        return used_keys, recent_titles

    def upsert_creations(self, creations: List[Dict[str, Any]]) -> None:
        if not creations:
            return
        now_dt = datetime.now(timezone.utc)
        for c in creations:
            c["updated_at"] = now_dt
        self.db.bulk_upsert(self.creation_col, creations, ["creation_id"])

    # --------------------------------------------------------------------------
    # 全链路运行轨迹审计 (`pipeline_run_traces`)
    # --------------------------------------------------------------------------
    def save_run_trace(self, run_record: Dict[str, Any]) -> None:
        if not run_record or not run_record.get("run_id"):
            return
        record_copy = dict(run_record)
        record_copy["updated_at"] = datetime.now(timezone.utc)
        self.db.bulk_upsert(self.trace_col, [record_copy], ["run_id"])
