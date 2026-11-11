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


def sort_posts_within_platform(source: str, posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    按各平台原生有效热度指标在平台内部排序（避免跨平台量纲差异导致像 Reddit RSS 这种无公开赞数的渠道被饿死）：
    - Reddit：优先按多板块 24h Top 热榜名次 `metadata.platform_extra.rank_in_feed` 升序（第 1 名最前），其次按发布时间倒序；
    - 其他平台（X / HackerNews / Binance / OKX）：按 `engagement.viral_score` 倒序，其次按发布时间倒序。
    """
    if str(source).lower() == "reddit":
        return sorted(
            posts,
            key=lambda p: (
                int((((p.get("metadata") or {}).get("platform_extra") or {}).get("rank_in_feed")) or 9999),
                -int(p.get("publish_time") or 0),
            ),
        )
    return sorted(
        posts,
        key=lambda p: (
            -float((p.get("engagement") or {}).get("viral_score") or 0.0),
            -int(p.get("publish_time") or 0),
        ),
    )


def round_robin_interleave_by_source(
    items: List[Dict[str, Any]],
    max_total: int,
    raw_lookup: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """
    将候选帖子或情报卡按 `source` 分桶、桶内按平台自身热度排序后，执行多平台公平轮询抽取（Round-Robin）。
    保证 X、Reddit、HackerNews、Binance、OKX 在下游理解与全局选题窗口中永远享有均衡席位。
    """
    if not items:
        return []
    by_src: Dict[str, List[Dict[str, Any]]] = {}
    for it in items:
        src = str(it.get("source") or "other").strip().lower()
        by_src.setdefault(src, []).append(it)

    sorted_buckets: Dict[str, List[Dict[str, Any]]] = {}
    for src, bucket in by_src.items():
        if raw_lookup is not None:
            # 若传入的是 Step 2 情报卡，结合 raw_lookup 里的实时互动/热榜排名在平台桶内排序
            if src == "reddit":
                sorted_buckets[src] = sorted(
                    bucket,
                    key=lambda d: (
                        int(
                            (
                                ((raw_lookup.get(d.get("post_key", "")) or {}).get("metadata") or {})
                                .get("platform_extra")
                                or {}
                            ).get("rank_in_feed")
                            or 9999
                        ),
                        -int(d.get("publish_time") or 0),
                    ),
                )
            else:
                sorted_buckets[src] = sorted(
                    bucket,
                    key=lambda d: (
                        -float(
                            ((raw_lookup.get(d.get("post_key", "")) or {}).get("engagement") or {}).get("viral_score")
                            or 0.0
                        ),
                        -int(d.get("publish_time") or 0),
                    ),
                )
        else:
            sorted_buckets[src] = sort_posts_within_platform(src, bucket)

    cap = max_total if max_total > 0 else len(items)
    preferred_order = ["x", "reddit", "hackernews", "binance", "okx"]
    ordered_sources = [s for s in preferred_order if s in sorted_buckets] + sorted(
        s for s in sorted_buckets.keys() if s not in preferred_order
    )

    interleaved: List[Dict[str, Any]] = []
    idx = 0
    while len(interleaved) < cap:
        added_in_round = False
        for src in ordered_sources:
            b = sorted_buckets[src]
            if idx < len(b):
                interleaved.append(b[idx])
                added_in_round = True
                if len(interleaved) >= cap:
                    break
        if not added_in_round:
            break
        idx += 1
    return interleaved


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
            self.db.create_index(self.digest_col, [("source", 1), ("post_id", 1)], unique=True)
            self.db.create_index(self.digest_col, [("post_key", 1)], unique=True)
            self.db.create_index(self.digest_col, [("is_usable", 1), ("publish_time", -1)], unique=False)
            self.db.create_index(self.digest_col, [("digested_at_ts", -1)], unique=False)

            self.db.create_index(self.topic_col, [("topic_id", 1)], unique=True)
            self.db.create_index(self.topic_col, [("run_id", 1)], unique=False)
            self.db.create_index(self.topic_col, [("status", 1), ("created_at_ts", -1)], unique=False)

            self.db.create_index(self.creation_col, [("creation_id", 1)], unique=True)
            self.db.create_index(self.creation_col, [("topic_id", 1)], unique=False)
            self.db.create_index(self.creation_col, [("profile_name", 1), ("created_at_ts", -1)], unique=False)
            self.db.create_index(self.creation_col, [("source_post_keys", 1)], unique=False)

            self.db.create_index(self.trace_col, [("run_id", 1)], unique=True)
            self.db.create_index(self.trace_col, [("started_at_ts", -1)], unique=False)
            _INDEX_READY.add(tag)

    # --------------------------------------------------------------------------
    # Step 1 -> Step 2 只读原帖查询（分平台公平查询，彻底消除 viral_score=0 渠道饥饿）
    # --------------------------------------------------------------------------
    def fetch_recent_raw_posts(
        self,
        lookback_hours: float = 48.0,
        per_source_limit: int = 200,
    ) -> List[Dict[str, Any]]:
        """
        按平台分别获取最近窗口内的原始标准化帖子并公平交错返回，
        彻底防止单一高产或高浏览量纲平台挤占总查询上限、饿死 Reddit 等渠道。
        """
        cutoff_ts = int(time.time() - max(lookback_hours, 1.0) * 3600)
        cutoff_dt = datetime.fromtimestamp(cutoff_ts, tz=timezone.utc)
        raw_collection = self.db.get_collection(self.raw_col)
        sources = [s for s in (raw_collection.distinct("source") or []) if isinstance(s, str) and s.strip()]

        all_docs: List[Dict[str, Any]] = []
        for src in sources:
            query = {
                "source": src,
                "$or": [
                    {"publish_time": {"$gte": cutoff_ts}},
                    {"db_update_time": {"$gte": cutoff_dt}},
                ],
            }
            sort_spec = (
                [("publish_time", -1)]
                if src.lower() == "reddit"
                else [("engagement.viral_score", -1), ("publish_time", -1)]
            )
            docs = self.db.find_many(
                self.raw_col,
                query=query,
                sort=sort_spec,
                limit=per_source_limit,
            )
            if docs:
                all_docs.extend(sort_posts_within_platform(src, docs))

        return round_robin_interleave_by_source(all_docs, max_total=len(all_docs))

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

    def fetch_usable_digests(self, lookback_hours: float = 36.0, limit: int = 500) -> List[Dict[str, Any]]:
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
        lookback_hours: float = 72.0,
    ) -> Tuple[Set[str], List[str]]:
        """
        返回 `(近期已被消费过的原帖 post_key 集合, 近期已生成的话题概述列表)`，
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
            projection={"source_post_keys": 1, "topic_summary": 1, "topic_title": 1},
            sort=[("created_at_ts", -1)],
            limit=200,
        )
        used_keys: Set[str] = set()
        recent_topics: List[str] = []
        for d in docs or []:
            for pk in d.get("source_post_keys") or []:
                used_keys.add(str(pk))
            label = str(d.get("topic_summary") or d.get("topic_title") or "").strip()
            if label and label not in recent_topics:
                recent_topics.append(label)
        return used_keys, recent_topics

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
