# -*- coding: utf-8 -*-
"""
流水线专用数据库访问层与确定性入口过滤 (`pipeline_db.py`)

物理隔离与极简确定性入口准则：
1. 对 `harvested_social_posts` 严格只读（Read-Only），绝不写入或覆盖采集器字段。
2. 入口仅执行两条 100% 确定性的物理过滤：
   - 严格按真实发布时间 `publish_time >= cutoff_ts` 入窗（彻底杜绝旧帖因复抓刷新 `db_update_time` 混入当期窗口）；
   - 凡带视频标记（`video_url` 非空、`video_duration > 0`、`card_type == "BUZZ_VIDEO"` 等）的帖子在入口直接排除，不纳入后续参考。
3. Step 2 / Step 3 / Step 4 / Step 5 与运行轨迹分别写入独立的 `pipeline_*` 集合，与历史旧集合完全解耦。
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


def is_video_post(raw_post: Dict[str, Any]) -> bool:
    """
    100% 确定性判断帖子是否为含视频帖：
    - `media.video_url` 非空 或 `media.video_duration > 0`
    - `card_type == "BUZZ_VIDEO"`
    - 被引用推文标记包含视频 (`metadata.quoted_post.has_video` / `metadata.platform_extra.quoted_post.has_video`)
    - 媒体或原链接为平台视频资源 (`amplify_video_thumb` / `ext_tw_video_thumb` / `tweet_video_thumb` / `v.redd.it`)
    """
    if not isinstance(raw_post, dict):
        return False
    media = raw_post.get("media") or {}
    if media.get("video_url") or int(media.get("video_duration") or 0) > 0:
        return True
    if str(raw_post.get("card_type") or "").upper() == "BUZZ_VIDEO":
        return True

    meta = raw_post.get("metadata") or {}
    extra = meta.get("platform_extra") or {}
    qp = meta.get("quoted_post") or extra.get("quoted_post") or {}
    if isinstance(qp, dict) and qp.get("has_video"):
        return True

    url_candidates = list(media.get("images") or []) + [
        media.get("cover_image"),
        meta.get("url"),
        meta.get("external_url"),
    ]
    for u in url_candidates:
        u_low = str(u or "").lower()
        if any(tok in u_low for tok in ("amplify_video_thumb", "ext_tw_video_thumb", "tweet_video_thumb", "v.redd.it")):
            return True
    return False


def sort_posts_within_platform(source: str, posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    按各平台原生有效热度指标在平台内部排序：
    - Reddit：若具备真实赞数 (`viral_score > 0` 或 `like_count > 0`) 优先结合热榜排位与赞数，否则按 24h 热榜名次 `rank_in_feed` 升序；
    - 其他平台（X / HackerNews / Binance / OKX）：按 `engagement.viral_score` 倒序，其次按发布时间倒序。
    """
    if str(source).lower() == "reddit":
        return sorted(
            posts,
            key=lambda p: (
                int((((p.get("metadata") or {}).get("platform_extra") or {}).get("rank_in_feed")) or 9999),
                -int((p.get("engagement") or {}).get("like_count") or 0),
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
    将候选帖子或单帖档案按 `source` 分桶、桶内按平台自身热度排序后，执行多平台公平轮询抽取（Round-Robin）。
    保证 X、Reddit、HackerNews、Binance、OKX 在理解与全局选题窗口中始终享有均衡席位。
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
                        -int(
                            ((raw_lookup.get(d.get("post_key", "")) or {}).get("engagement") or {}).get("like_count")
                            or 0
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
    """统一管理新流水线的只读源查询、确定性入口过滤与各阶段产物持久化。"""

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
            self.db.create_index(self.creation_col, [("publish_status", 1), ("created_at_ts", -1)], unique=False)
            self.db.create_index(self.creation_col, [("source_post_keys", 1)], unique=False)

            self.db.create_index(self.trace_col, [("run_id", 1)], unique=True)
            self.db.create_index(self.trace_col, [("started_at_ts", -1)], unique=False)
            _INDEX_READY.add(tag)

    # --------------------------------------------------------------------------
    # Step 1 -> Step 2 确定性入口过滤查询（严格 publish_time 窗口 + 一刀切排除视频帖）
    # --------------------------------------------------------------------------
    def fetch_recent_raw_posts_with_stats(
        self,
        lookback_hours: float = 48.0,
        per_source_limit: int = 200,
        exclude_video_posts: bool = True,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
        """
        按平台分别查询严格位于 `publish_time >= cutoff_ts` 真实发布时间窗口内的原帖，
        并在入口确定性排除所有含视频帖子，最后跨平台公平轮询返回。
        """
        cutoff_ts = int(time.time() - max(lookback_hours, 1.0) * 3600)
        raw_collection = self.db.get_collection(self.raw_col)
        sources = [s for s in (raw_collection.distinct("source") or []) if isinstance(s, str) and s.strip()]

        all_docs: List[Dict[str, Any]] = []
        total_in_time_window = 0
        excluded_video_count = 0

        for src in sources:
            query = {
                "source": src,
                "publish_time": {"$gte": cutoff_ts},
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
            ) or []
            total_in_time_window += len(docs)

            if exclude_video_posts:
                clean_docs: List[Dict[str, Any]] = []
                for d in docs:
                    if is_video_post(d):
                        excluded_video_count += 1
                    else:
                        clean_docs.append(d)
                docs = clean_docs

            if docs:
                all_docs.extend(sort_posts_within_platform(src, docs))

        interleaved = round_robin_interleave_by_source(all_docs, max_total=len(all_docs))
        stats = {
            "in_time_window_total": total_in_time_window,
            "excluded_video_count": excluded_video_count,
            "eligible_non_video_count": len(interleaved),
        }
        return interleaved, stats

    def fetch_recent_raw_posts(
        self,
        lookback_hours: float = 48.0,
        per_source_limit: int = 200,
        exclude_video_posts: bool = True,
    ) -> List[Dict[str, Any]]:
        posts, _ = self.fetch_recent_raw_posts_with_stats(
            lookback_hours=lookback_hours,
            per_source_limit=per_source_limit,
            exclude_video_posts=exclude_video_posts,
        )
        return posts

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
    # Step 2 单帖档案读写 (`pipeline_post_digests`)
    # --------------------------------------------------------------------------
    def get_existing_digest_keys(self, post_keys: Sequence[str]) -> Set[str]:
        """查询哪些 `post_key` 已经成功完成 Step 2 理解，支持幂等增量复用缓存。"""
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
        """严格按真实发布时间 `publish_time >= cutoff_ts` 获取窗口内全部 `is_usable=True` 的单帖档案。"""
        cutoff_ts = int(time.time() - max(lookback_hours, 1.0) * 3600)
        query = {
            "status": "ok",
            "is_usable": True,
            "publish_time": {"$gte": cutoff_ts},
        }
        docs = self.db.find_many(
            self.digest_col,
            query=query,
            sort=[("publish_time", -1)],
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
    # Step 4 & Step 5 成品读写 (`pipeline_creations`)
    # --------------------------------------------------------------------------
    def fetch_creations_pending_review(self, run_id: Optional[str] = None, limit: int = 10) -> List[Dict[str, Any]]:
        query: Dict[str, Any] = {"status": "ok", "publish_status": "pending_review"}
        if run_id:
            query["run_id"] = run_id
        return self.db.find_many(
            self.creation_col,
            query=query,
            sort=[("created_at_ts", 1)],
            limit=limit,
        ) or []

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
