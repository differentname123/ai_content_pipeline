# -*- coding: utf-8 -*-
"""
多平台高价值内容拉取架构 —— 抽象基类与统一契约 (BasePlatformHarvester)

设计哲学：
1. 极简可靠：不引入复杂脆弱的框架，所有平台适配器只继承一个基类 `BasePlatformHarvester`。
2. 职责内聚：每个平台适配器内部负责「拉取本平台数据 + 抹平本平台字段差异」，对外统一输出标准帖子结构。
3. 四大通用原语：任何社区/内容平台（X、Reddit、HackerNews、币安、OKX、知乎、小红书）在业务上最常用的无非是 4 个动作：
   - `fetch_hot_or_recommended`: 拉取平台首页推荐流 / 热榜爆款流
   - `fetch_user_posts`:         拉取指定大 V / 博主的最新发帖
   - `search_or_topic_posts`:    按关键词、话题标签、垂直板块或币种搜索帖子
   - `fetch_post_comments`:      拉取单条爆款帖底下的前排高赞神评（用于借梗与反转提炼）
4. 质量优先：内置通用价值门槛过滤 (`filter_high_value_posts`)，剔除无意义水帖，绝不为了拉取而拉取垃圾数据。
"""
import logging
import re
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from common.common_utils import (
    clean_post_text,
    create_standard_post_schema,
    get_requests_proxies,
    normalize_and_enrich_post_schema,
    normalize_and_enrich_posts_batch,
    normalize_comment_item,
    safe_int,
)

logger = logging.getLogger(__name__)

# 全新独立的 MongoDB 集合名称，与历史 `social_media_posts` 完全物理隔离，互不产生交集
NEW_HARVESTED_COLLECTION_NAME = "harvested_social_posts"

_RE_INLINE_MEDIA_MARK = re.compile(r"\[(?:插图|长文封面|视频封面|视频)\s*:[^\]]*\]")


class BasePlatformHarvester(ABC):
    """
    所有平台拉取适配器的统一基类。
    内置「采集通道健康度追踪 + 返回数据完整性与新鲜度体检」，杜绝平台接口失效或登录掉线时静默伪装正常。
    """

    SOURCE_NAME: str = "unknown"

    def __init__(self, proxies: Optional[Dict[str, str]] = None):
        self.proxies = proxies if proxies is not None else (get_requests_proxies() or {})
        self.health_issues: List[Dict[str, Any]] = []
        self.last_channel_stats: Dict[str, int] = {}
        self.last_search_note: str = ""
        self.last_quality_report: Dict[str, Any] = {}

    # ==========================================================================
    # 平台异常感知与健康度追踪（严禁子通道失败静默吞没）
    # ==========================================================================
    def reset_health_state(self) -> None:
        """每次执行新一轮 `harvest` 或 `search_or_topic_posts` 前清空单轮异常记录（保留初始化期鉴权告警）。"""
        self.health_issues = [x for x in self.health_issues if x.get("persistent")]
        self.last_quality_report = {}

    def record_health_issue(
        self,
        code: str,
        message: str,
        level: str = "warning",
        channel: str = "",
        persistent: bool = False,
    ) -> None:
        """
        记录平台采集过程中的真实异常（登录失效、HTTP 401/403/429、接口改版解析 0 条、主通道降级等）。
        level: "critical"（严重故障/不可用） | "warning"（部分子通道异常或触发降级） | "info"（平台属性客观说明）
        """
        key = f"{level}:{code}:{channel}:{message}"
        for existing in self.health_issues:
            if existing.get("_dedup_key") == key:
                return
        issue = {
            "platform": self.SOURCE_NAME,
            "level": level,
            "code": code,
            "channel": channel or self.SOURCE_NAME,
            "message": message,
            "persistent": persistent,
            "ts": int(time.time()),
            "_dedup_key": key,
        }
        self.health_issues.append(issue)
        if level == "critical":
            logger.error(f"🚨 [{self.SOURCE_NAME}健康告警/{code}] ({channel or '主通道'}) {message}")
        elif level == "warning":
            logger.warning(f"⚠️ [{self.SOURCE_NAME}健康预警/{code}] ({channel or '子通道'}) {message}")

    def audit_posts_quality(
        self,
        posts: List[Dict[str, Any]],
        scene: str = "harvest",
        expect_comments: bool = True,
        expect_media_download: bool = False,
        max_fresh_age_hours: int = 168,
    ) -> Dict[str, Any]:
        """
        对拉取并归一化后的帖子批次做确定性数据质量自检，防止残缺帖、陈旧缓存帖、评论接口失灵或图片下载全挂悄悄流入下游。
        """
        now_ts = int(time.time())
        total = len(posts)
        valid_posts: List[Dict[str, Any]] = []
        corrupt_dropped = 0
        empty_text_no_media = 0
        very_short_text_no_media = 0
        stale_posts = 0
        invalid_ts_posts = 0
        expected_cmt_posts = 0
        actual_cmt_posts = 0
        remote_img_total = 0
        local_img_total = 0

        stale_cutoff = now_ts - max(24, max_fresh_age_hours) * 3600

        for p in posts or []:
            if not isinstance(p, dict) or not str(p.get("post_id") or "").strip():
                corrupt_dropped += 1
                continue

            cnt = p.get("content") or {}
            media = p.get("media") or {}
            eng = p.get("engagement") or {}

            raw_txt = str(cnt.get("text_content") or "").strip()
            pure_txt = _RE_INLINE_MEDIA_MARK.sub("", raw_txt).strip()
            title_txt = str(cnt.get("title") or "").strip()
            imgs = [u for u in (media.get("images") or []) if u]
            lmap = media.get("local_mapping") or {}
            has_any_media = bool(imgs or media.get("cover_image") or media.get("video_url"))

            # 【保留过滤原因说明】残缺空壳帖剔除：
            # 若单条帖子既无任何正文/标题，又无任何图片或视频链接，属于平台接口字段残缺或已被作者/管理员清空的死帖；
            # 继续保留会白白浪费后续评论拉取、外链解析与下游大模型 Token，故在此直接剔除。
            if not pure_txt and not title_txt and not has_any_media:
                empty_text_no_media += 1
                corrupt_dropped += 1
                continue

            if len(pure_txt) + len(title_txt) < 6 and not has_any_media:
                very_short_text_no_media += 1

            pub_ts = safe_int(p.get("publish_time"), 0)
            if pub_ts <= 946684800 or pub_ts > now_ts + 86400:
                invalid_ts_posts += 1
            elif pub_ts < stale_cutoff:
                stale_posts += 1

            if safe_int(eng.get("comment_count"), 0) >= 5:
                expected_cmt_posts += 1
                if p.get("comments"):
                    actual_cmt_posts += 1

            remote_img_total += len(imgs)
            local_img_total += sum(1 for u in imgs if u in lmap)
            valid_posts.append(p)

        if corrupt_dropped > 0:
            self.record_health_issue(
                code="CORRUPT_POSTS_DROPPED",
                message=f"[{scene}] 拦截剔除了 {corrupt_dropped}/{total} 条无正文且无媒体的残缺空壳帖（疑似平台字段解析退化）",
                level="critical" if corrupt_dropped >= max(2, total // 2) else "warning",
                channel=scene,
            )

        kept = len(valid_posts)
        if kept >= 4 and invalid_ts_posts > 0:
            self.record_health_issue(
                code="INVALID_PUBLISH_TIME",
                message=f"[{scene}] 有 {invalid_ts_posts}/{kept} 条帖子时间戳异常，请检查时间字段解析",
                level="warning",
                channel=scene,
            )

        if kept >= 5 and stale_posts >= max(4, int(kept * 0.6)):
            self.record_health_issue(
                code="STALE_FEED_DETECTED",
                message=(
                    f"[{scene}] 高达 {stale_posts}/{kept} 条帖子早于 {max_fresh_age_hours}h 前！"
                    f"疑似退化到免登历史缓存流或置顶旧帖，请检查登录态或排序参数"
                ),
                level="warning",
                channel=scene,
            )

        if expect_comments and expected_cmt_posts >= 3 and actual_cmt_posts == 0:
            self.record_health_issue(
                code="COMMENTS_FETCH_BROKEN",
                message=(
                    f"[{scene}] 发现 {expected_cmt_posts} 条热帖显示有 5+ 评论，但实际拉回评论数为 0！"
                    f"疑似本平台评论接口失效或鉴权受限"
                ),
                level="warning",
                channel=f"{scene}:comments",
            )

        if expect_media_download and remote_img_total >= 3:
            if local_img_total == 0:
                self.record_health_issue(
                    code="MEDIA_DOWNLOAD_ALL_FAILED",
                    message=f"[{scene}] 发现 {remote_img_total} 张远程图片但本地下载成功 0 张！图片 CDN 或代理异常",
                    level="critical",
                    channel=f"{scene}:media",
                )
            elif local_img_total < remote_img_total * 0.5:
                self.record_health_issue(
                    code="MEDIA_DOWNLOAD_PARTIAL_FAIL",
                    message=f"[{scene}] 图片下载成功率偏低 ({local_img_total}/{remote_img_total} 张)，部分配图无法供下游多模态使用",
                    level="warning",
                    channel=f"{scene}:media",
                )

        critical_cnt = sum(1 for x in self.health_issues if x.get("level") == "critical")
        warning_cnt = sum(1 for x in self.health_issues if x.get("level") == "warning")
        health_status = "critical" if critical_cnt > 0 else ("degraded" if warning_cnt > 0 else "healthy")

        report = {
            "platform": self.SOURCE_NAME,
            "scene": scene,
            "health_status": health_status,
            "raw_count": total,
            "valid_count": kept,
            "corrupt_dropped": corrupt_dropped,
            "stale_posts": stale_posts,
            "invalid_ts_posts": invalid_ts_posts,
            "expected_cmt_posts": expected_cmt_posts,
            "actual_cmt_posts": actual_cmt_posts,
            "remote_images": remote_img_total,
            "local_images": local_img_total,
            "issues": [
                {k: v for k, v in item.items() if k != "_dedup_key"}
                for item in self.health_issues
            ],
        }
        self.last_quality_report = report
        return report

    def get_health_summary(self) -> Dict[str, Any]:
        """获取当前采集器实例最新的健康状态与告警列表。"""
        critical_cnt = sum(1 for x in self.health_issues if x.get("level") == "critical")
        warning_cnt = sum(1 for x in self.health_issues if x.get("level") == "warning")
        status = "critical" if critical_cnt > 0 else ("degraded" if warning_cnt > 0 else "healthy")
        return {
            "platform": self.SOURCE_NAME,
            "health_status": status,
            "critical_count": critical_cnt,
            "warning_count": warning_cnt,
            "search_note": self.last_search_note,
            "issues": [
                {k: v for k, v in item.items() if k != "_dedup_key"}
                for item in self.health_issues
            ],
            "quality_report": self.last_quality_report,
        }

    # ==========================================================================
    # 四大通用采集原语（各平台按自身能力实现，不支持的原语可安全返回空列表）
    # ==========================================================================
    @abstractmethod
    def fetch_hot_or_recommended(self, count: int = 20, **kwargs) -> List[Dict[str, Any]]:
        """原语 1：拉取平台热榜 / 首页算法推荐流"""
        raise NotImplementedError

    def fetch_user_posts(self, user_id: str, count: int = 15, **kwargs) -> List[Dict[str, Any]]:
        """原语 2：拉取指定博主 / 大 V 主页的最新帖子"""
        logger.debug(f"[{self.SOURCE_NAME}] 当前适配器未启用 fetch_user_posts (user_id={user_id})")
        return []

    def search_or_topic_posts(self, query: str, count: int = 20, **kwargs) -> List[Dict[str, Any]]:
        """原语 3：按关键词、话题标签、垂直板块或标的搜索高价值帖子"""
        logger.debug(f"[{self.SOURCE_NAME}] 当前适配器未启用 search_or_topic_posts (query={query})")
        return []

    def fetch_post_comments(self, post_id: str, max_comments: int = 10, **kwargs) -> List[Dict[str, Any]]:
        """原语 4：拉取指定帖子底下的前排高赞评论（神评树）"""
        logger.debug(f"[{self.SOURCE_NAME}] 当前适配器未启用 fetch_post_comments (post_id={post_id})")
        return []

    # ==========================================================================
    # 本平台差异抹平与价值过滤通用工具
    # ==========================================================================
    def create_empty_post(self, card_type: str = "BUZZ_SHORT") -> Dict[str, Any]:
        """创建带有当前平台 `source` 标识的标准空白帖子骨架"""
        return create_standard_post_schema(source=self.SOURCE_NAME, card_type=card_type)

    def normalize_post(self, post: Dict[str, Any], download_media: bool = False) -> Dict[str, Any]:
        """将单条帖子抹平平台差异（统一时间戳、内嵌 `[插图: url]`、统一 `comments`、对齐互动量纲）"""
        post["source"] = self.SOURCE_NAME
        return normalize_and_enrich_post_schema(
            post,
            download_media=download_media,
            proxies=self.proxies,
        )

    def normalize_batch(self, posts: List[Dict[str, Any]], download_media: bool = True) -> List[Dict[str, Any]]:
        """批量并发抹平差异并下载配图至本地 `media_downloads/`"""
        for p in posts:
            if isinstance(p, dict):
                p["source"] = self.SOURCE_NAME
        return normalize_and_enrich_posts_batch(
            posts,
            download_media=download_media,
            proxies=self.proxies,
        )

    def normalize_comments(self, raw_comments: List[Any], parent_post_id: str = "", default_ts: int = 0) -> List[Dict[str, Any]]:
        """将本平台原始评论列表统一转化为标准评论结构（含 `content`, `likes`, `author_name`）"""
        out = []
        seen = set()
        for c in raw_comments or []:
            nc = normalize_comment_item(c, parent_post_id=parent_post_id, default_ts=default_ts)
            if not nc:
                continue
            key = nc["reply_id"] or nc["content"][:80]
            if key in seen:
                continue
            seen.add(key)
            out.append(nc)
        return out

    def is_valuable_post(self, post: Dict[str, Any], config: Optional[Dict[str, Any]] = None) -> bool:
        """
        基础数据有效性校验（仅剔除无内容残缺空壳帖）：
        【保留过滤原因说明】
        - 本函数不做任何发布时间、点赞量或字数长短的主观过滤（时间与选题筛选统一交由下游流水线处理）；
        - 仅剔除缺少 `post_id` 或「既无标题/正文、又无任何图片/视频」的损坏空壳死帖，避免空数据浪费采集富化耗时与下游算力。
        """
        if not isinstance(post, dict) or not str(post.get("post_id") or "").strip():
            return False

        content_obj = post.get("content") or {}
        media_obj = post.get("media") or {}
        raw_txt = _RE_INLINE_MEDIA_MARK.sub("", str(content_obj.get("text_content") or "")).strip()
        title_txt = str(content_obj.get("title") or "").strip()
        has_text = bool(raw_txt or title_txt)
        has_media = bool(media_obj.get("images") or media_obj.get("inline_images") or media_obj.get("cover_image") or media_obj.get("video_url"))
        return has_text or has_media

    # ==========================================================================
    # 标准调度入口：供上层定时任务统一调用
    # ==========================================================================
    @abstractmethod
    def harvest(self, config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        按平台配置执行完整的一轮高价值采集（拉取 -> 质量过滤 -> 补充神评 -> 差异抹平与配图下载），
        返回可直接入库的标准格式帖子列表。
        """
        raise NotImplementedError
