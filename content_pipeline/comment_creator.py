# -*- coding: utf-8 -*-
"""
模块 8：独立全语种全领域帖子评论生成模块 (`comment_creator.py`)

核心定位与正交职责（100% 解耦于上游“挑哪些人/哪些帖去评”的筛选逻辑）：
1. 输入一条完整的原始帖子（完整正文 + 引用原帖 + 关联外链原文 + 全部真实原图多模态附件 + 原帖作者本人的补充/自评 + 评论区前排其他网友评论），
   生成任意语种（默认自动对齐原帖原生语种与社区语境，或按指定目标语种输出）的走心、机锋、去 AI 味高质量评论；
2. 同时兼容三种调用姿势：
   - `generate_comments_from_post(post_doc, ...)`      : 纯内存解耦核心入口（支持标准 `harvested_social_posts` 结构、轻量自定义字典或纯文本）；
   - `generate_comments_for_x_post(post_url_or_id, ...)`: 传入任意 X 帖子链接或推文 ID，默认通过 `x_xiaohao_browser_session_dir` 实时拉齐完整图文与评论后生成评论；
   - `generate_comments_for_post_key(post_key, ...)`   : 传入库内 `source:post_id`（跨 X / Reddit / HN / Binance / OKX 全平台通用）直接生成评论；
3. 模型路由严格遵循 `PROJECT_PRINCIPLES.md`：统一走 `model=None` + `preset_model_group="medium"` 模型组负载均衡，绝不硬编码模型名。
"""
import copy
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from PIL import Image

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.common_utils import (
    clean_post_text,
    create_standard_post_schema,
    get_requests_proxies,
    normalize_and_enrich_post_schema,
    normalize_comment_item,
    resolve_session_dir,
    safe_int,
)
from content_pipeline.llm_gateway import call_llm_json, render_prompt_template
from content_pipeline.pipeline_config import COMMENT_CREATOR_CONFIG
from content_pipeline.pipeline_db import PipelineDB, is_video_post, make_post_key
from content_pipeline.pipeline_logger import get_pipeline_logger
from content_pipeline.step2_comprehension import (
    ORIGIN_DISPLAY_NAMES,
    SUPPORTED_IMAGE_SUFFIXES,
    extract_ordered_local_images,
    get_unified_post_text,
    strip_inline_media_placeholders,
)

_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
_HASHTAG_RE = re.compile(r"(?:^|\s)#[A-Za-z0-9_\u4e00-\u9fff]+")
_X_STATUS_URL_RE = re.compile(
    r"(?:x\.com|twitter\.com)/(?:(?P<handle>[A-Za-z0-9_]{1,30})/status|i/(?:web/)?status)/(?P<tid>\d+)",
    re.IGNORECASE,
)

_GENERIC_AUTHOR_PLACEHOLDERS = {
    "",
    "anonymous",
    "unknown",
    "user",
    "匿名",
    "网友",
    "未知",
}

_LANGUAGE_ALIASES: Dict[str, str] = {
    "en": "地道自然的社交英语 (Idiomatic English)",
    "english": "地道自然的社交英语 (Idiomatic English)",
    "zh": "地道自然的简体中文",
    "zh-cn": "地道自然的简体中文",
    "zh_cn": "地道自然的简体中文",
    "cn": "地道自然的简体中文",
    "中文": "地道自然的简体中文",
    "简体中文": "地道自然的简体中文",
    "zh-tw": "地道自然的繁體中文",
    "zh_tw": "地道自然的繁體中文",
    "zh-hk": "地道自然的繁體中文",
    "繁体中文": "地道自然的繁體中文",
    "繁體中文": "地道自然的繁體中文",
    "ja": "地道自然的日语 (自然な日本語)",
    "jp": "地道自然的日语 (自然な日本語)",
    "japanese": "地道自然的日语 (自然な日本語)",
    "日语": "地道自然的日语 (自然な日本語)",
    "日文": "地道自然的日语 (自然な日本語)",
    "ko": "地道自然的韩语 (자연스러운 한국어)",
    "korean": "地道自然的韩语 (자연스러운 한국어)",
    "韩语": "地道自然的韩语 (자연스러운 한국어)",
    "es": "地道自然的西班牙语 (Español natural)",
    "spanish": "地道自然的西班牙语 (Español natural)",
    "fr": "地道自然的法语 (Français naturel)",
    "de": "地道自然的德语 (Natürliches Deutsch)",
    "pt": "地道自然的葡萄牙语 (Português natural)",
    "ru": "地道自然的俄语 (Естественный русский)",
    "ar": "地道自然的阿拉伯语",
}


def get_default_x_session_dir(comment_cfg: Optional[Dict[str, Any]] = None) -> str:
    """获取默认 X 账号浏览器会话目录（固定优先读取 `x_xiaohao_browser_session_dir`）。"""
    cfg = comment_cfg or COMMENT_CREATOR_CONFIG
    cfg_key = str(cfg.get("x_session_config_key") or "x_xiaohao_browser_session_dir").strip()
    return resolve_session_dir(cfg_key, "x_xiaohao")


def calc_x_weighted_length(text: str) -> int:
    """
    按 X (Twitter) 官方 `twitter-text` v3 加权规则计算单条评论的加权字符长度：
    - 任何 URL 统一计 23 字符；
    - 基础拉丁 / 半角 ASCII 及常见西文区段 (`0x0000` ~ `0x10FF`，代理对与特殊宽字符除外) 计 1；
    - 中文 / 日韩文 / 全角符号 / Emoji 等其余字符统一计 2。
    未认证账号单条评论硬上限为 280。
    """
    if not text:
        return 0
    s = str(text).strip()
    if not s:
        return 0

    total = 0

    def _replace_url(match: re.Match) -> str:
        nonlocal total
        total += 23
        return ""

    rem = _URL_RE.sub(_replace_url, s)
    for ch in rem:
        cp = ord(ch)
        if 0x0000 <= cp <= 0x10FF or 0x2000 <= cp <= 0x200D or 0x2010 <= cp <= 0x201F or 0x2032 <= cp <= 0x2037:
            total += 1
        else:
            total += 2
    return total


def parse_x_post_url_or_id(post_url_or_id: str) -> Tuple[str, str]:
    """
    从任意 X 推文链接或纯数字 ID 中提取 `(screen_name, tweet_id)`：
    支持 `https://x.com/user/status/123456`、`https://twitter.com/i/web/status/123456` 或 `"123456"`。
    """
    raw = str(post_url_or_id or "").strip()
    if not raw:
        return "", ""
    if raw.isdigit():
        return "", raw
    m = _X_STATUS_URL_RE.search(raw)
    if m:
        handle = (m.group("handle") or "").strip().lstrip("@")
        tid = (m.group("tid") or "").strip()
        return handle, tid
    digits_match = re.search(r"(\d{10,25})", raw)
    if digits_match:
        return "", digits_match.group(1)
    return "", ""


def _build_language_directive(target_language: Optional[str]) -> str:
    raw = str(target_language or "auto").strip()
    low = raw.lower()
    if not raw or low in {"auto", "same", "match", "native", "default"}:
        return (
            "100% 自动跟随原帖主体语种、文字制式与社区原生表达习惯，严格使用与原帖完全一致的母语者自然口吻撰写评论；"
            "若原帖为纯图片无正文，则依据画面内主要文字语种对齐（画面无文字时默认使用自然英语）。"
        )
    mapped = _LANGUAGE_ALIASES.get(low, raw)
    return (
        f"无论原帖使用何种语言，本次生成的所有评论必须统一使用「{mapped}」撰写，"
        "保持该语种母语社区真实用户的地道口吻与自然语感，绝不出现生硬翻译腔。"
    )


def _is_valid_local_image(path_str: str) -> bool:
    """校验本地图片文件非空且可被 PIL 正常解码，杜绝损坏文件触发多模态网关报错。"""
    if not path_str:
        return False
    p = Path(path_str)
    if not p.is_file() or p.suffix.lower() not in SUPPORTED_IMAGE_SUFFIXES:
        return False
    try:
        if p.stat().st_size <= 0:
            return False
        with Image.open(p) as im:
            im.verify()
        return True
    except Exception:
        return False


def _coerce_to_standard_post_dict(post_input: Union[Dict[str, Any], str]) -> Tuple[Dict[str, Any], List[str]]:
    """
    将任意输入形态（标准 `harvested_social_posts` 文档、轻量自定义字典或纯字符串）
    无损对齐为内部统一帖子字典，并返回调用方直接传入的本地图片路径列表。
    """
    direct_local_images: List[str] = []

    if isinstance(post_input, str):
        std = create_standard_post_schema(source="custom", card_type="BUZZ_SHORT")
        std["post_id"] = f"inline_{int(time.time() * 1000)}"
        std["publish_time"] = int(time.time())
        std["content"]["text_content"] = post_input.strip()
        return std, direct_local_images

    if not isinstance(post_input, dict):
        raise TypeError(f"不支持的帖子输入类型: {type(post_input)}")

    doc = copy.deepcopy(post_input)

    # 若已经是标准 Schema（具备 content 字典与 media 字典），仅补充可能散落在顶层的便捷字段
    if isinstance(doc.get("content"), dict) and isinstance(doc.get("media"), dict):
        raw_imgs = list((doc.get("media") or {}).get("images") or []) + list(doc.get("images") or [])
        remote_imgs: List[str] = []
        for u in raw_imgs:
            s = str(u or "").strip()
            if not s:
                continue
            if s.startswith(("http://", "https://")):
                if s not in remote_imgs:
                    remote_imgs.append(s)
            elif Path(s).is_file():
                resolved = str(Path(s).resolve())
                if resolved not in direct_local_images:
                    direct_local_images.append(resolved)
        if remote_imgs:
            doc["media"]["images"] = remote_imgs
        return doc, direct_local_images

    # 兼容轻量自定义字典输入：{"text": ..., "title": ..., "author": ..., "images": [...], "comments": [...]}
    source = str(doc.get("source") or doc.get("platform") or "custom").strip().lower()
    std = create_standard_post_schema(source=source, card_type="BUZZ_SHORT")
    std["post_id"] = str(doc.get("post_id") or doc.get("id") or f"inline_{int(time.time() * 1000)}").strip()
    std["publish_time"] = safe_int(doc.get("publish_time") or doc.get("created_at"), int(time.time()))

    raw_author = doc.get("author")
    if isinstance(raw_author, dict):
        std["author"].update(raw_author)
    elif isinstance(raw_author, str) and raw_author.strip():
        clean_handle = raw_author.strip().lstrip("@")
        std["author"]["username"] = clean_handle
        std["author"]["author_name"] = raw_author.strip()
    elif doc.get("username") or doc.get("author_name"):
        std["author"]["username"] = str(doc.get("username") or "").strip().lstrip("@") or None
        std["author"]["author_name"] = str(doc.get("author_name") or doc.get("username") or "anonymous").strip()

    text_val = str(
        doc.get("text_content")
        or doc.get("text")
        or doc.get("body")
        or doc.get("content")
        or ""
    ).strip()
    quoted_text = str(doc.get("quoted_text") or "").strip()
    if quoted_text and quoted_text not in text_val:
        text_val = f"{text_val}\n[引用原帖]: {quoted_text}".strip()

    std["content"]["title"] = str(doc.get("title") or "").strip() or None
    std["content"]["text_content"] = text_val

    remote_imgs = []
    for img in (doc.get("images") or doc.get("image_urls") or []):
        s = str(img or "").strip()
        if not s:
            continue
        if s.startswith(("http://", "https://")):
            if s not in remote_imgs:
                remote_imgs.append(s)
        elif Path(s).is_file():
            resolved = str(Path(s).resolve())
            if resolved not in direct_local_images:
                direct_local_images.append(resolved)

    std["media"]["images"] = remote_imgs
    std["media"]["cover_image"] = remote_imgs[0] if remote_imgs else None
    std["comments"] = list(doc.get("comments") or [])
    if doc.get("url"):
        std["metadata"]["url"] = str(doc["url"]).strip()
    if doc.get("author_comments"):
        std["metadata"]["explicit_author_comments"] = list(doc["author_comments"])
    return std, direct_local_images


def _extract_post_author_identifiers(post_doc: Dict[str, Any]) -> set:
    """提取原帖作者的归一化身份标识集合（排除通用匿名占位词，防止误把路人当成原帖作者）。"""
    author_obj = post_doc.get("author") or {}
    candidates = [
        author_obj.get("username"),
        post_doc.get("author_id"),
        author_obj.get("author_name"),
    ]
    ident_set = set()
    for val in candidates:
        s = str(val or "").strip().lstrip("@").lower()
        if s and s not in _GENERIC_AUTHOR_PLACEHOLDERS:
            ident_set.add(s)
    return ident_set


def _is_comment_from_post_author(comment_obj: Dict[str, Any], author_identifiers: set) -> bool:
    if comment_obj.get("is_post_author") is True:
        return True
    if not author_identifiers:
        return False
    for key in ("author_uid", "author_name"):
        val = str(comment_obj.get(key) or "").strip().lstrip("@").lower()
        if val and val in author_identifiers:
            return True
    return False


def _dedupe_normalized_comments(comments: Sequence[Any], parent_post_id: str = "") -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen = set()
    for raw_c in comments or []:
        is_author_flag = isinstance(raw_c, dict) and bool(raw_c.get("is_post_author"))
        nc = normalize_comment_item(raw_c, parent_post_id=parent_post_id)
        if not nc:
            continue
        if is_author_flag:
            nc["is_post_author"] = True
        dedupe_key = nc.get("reply_id") or f"{nc.get('author_uid', '')}:{nc.get('content', '')[:100]}"
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        out.append(nc)
    return out


def assemble_comment_bundle(
    post_input: Union[Dict[str, Any], str],
    extra_comments: Optional[Sequence[Union[str, Dict[str, Any]]]] = None,
    author_comments: Optional[Sequence[Union[str, Dict[str, Any]]]] = None,
    download_missing_media: bool = True,
    max_author_comments: int = 10,
    max_community_comments: int = 15,
) -> Dict[str, Any]:
    """
    将任意完整原帖及其补充评论组装为多模态评论生成所需的标准上下文包：
    1. 自动合并原帖正文、标题、被引用原帖正文与关联外链正文；
    2. 自动区分「原帖作者本人的自评/补充回复（按时间正序）」与「其他网友前排热评（按赞数倒序）」；
    3. 自动补齐下载尚未落盘的远程配图（含主帖配图、引用原帖配图与评论区配图），并通过 PIL 校验图像完整性。
    """
    post_doc, direct_local_images = _coerce_to_standard_post_dict(post_input)
    post_id = str(post_doc.get("post_id") or "")

    # 合并原帖自带评论 + 调用方额外传入的社区评论
    combined_raw_comments: List[Any] = list(post_doc.get("comments") or [])
    if extra_comments:
        combined_raw_comments.extend(extra_comments)

    explicit_author_raw: List[Any] = list((post_doc.get("metadata") or {}).get("explicit_author_comments") or [])
    if author_comments:
        explicit_author_raw.extend(author_comments)
    for item in explicit_author_raw:
        if isinstance(item, str):
            combined_raw_comments.append({"content": item, "is_post_author": True})
        elif isinstance(item, dict):
            tagged = dict(item)
            tagged["is_post_author"] = True
            combined_raw_comments.append(tagged)

    post_doc["comments"] = _dedupe_normalized_comments(combined_raw_comments, parent_post_id=post_id)

    # 检查是否存在尚未落盘的远程图片 URL，按需自动下载至本地 media_downloads/
    media_obj = post_doc.get("media") or {}
    local_map = media_obj.get("local_mapping") or {}
    has_missing_post_img = any(
        u and (u not in local_map or not Path(str(local_map.get(u) or "")).is_file())
        for u in (media_obj.get("images") or [])
    )
    has_missing_cmt_img = any(
        any(
            cu and (cu not in (c.get("local_mapping") or {}) or not Path(str((c.get("local_mapping") or {}).get(cu) or "")).is_file())
            for cu in (c.get("images") or [])
        )
        for c in (post_doc.get("comments") or [])
        if isinstance(c, dict)
    )
    if download_missing_media and (has_missing_post_img or has_missing_cmt_img):
        # 保留 is_post_author 标记（normalize_and_enrich_post_schema 会重建评论字典）
        author_flag_keys = {
            c.get("reply_id") or c.get("content", "")[:100]
            for c in (post_doc.get("comments") or [])
            if isinstance(c, dict) and c.get("is_post_author")
        }
        post_doc = normalize_and_enrich_post_schema(
            post_doc,
            download_media=True,
            proxies=get_requests_proxies() or {},
        )
        for c in post_doc.get("comments") or []:
            ckey = c.get("reply_id") or c.get("content", "")[:100]
            if ckey in author_flag_keys:
                c["is_post_author"] = True

    # 切分「原帖作者自评/补充」与「其他网友评论」
    author_idents = _extract_post_author_identifiers(post_doc)
    author_cmt_list: List[Dict[str, Any]] = []
    community_cmt_list: List[Dict[str, Any]] = []

    for c in post_doc.get("comments") or []:
        if not isinstance(c, dict):
            continue
        if _is_comment_from_post_author(c, author_idents):
            author_cmt_list.append(c)
        else:
            community_cmt_list.append(c)

    # 作者自评按时间正序排列（还原串推/补充说明的自然语序），社区评论按赞数倒序排列
    author_cmt_list.sort(key=lambda x: safe_int(x.get("create_time"), 0))
    community_cmt_list.sort(key=lambda x: safe_int(x.get("likes"), 0), reverse=True)

    selected_author_cmts = author_cmt_list[: max(0, int(max_author_comments))]
    selected_community_cmts = community_cmt_list[: max(0, int(max_community_comments))]

    # 为了让 extract_ordered_local_images 同时提取选中作者自评与选中社区热评中的配图，先放入有序评论列表
    ordered_selected_comments = selected_author_cmts + selected_community_cmts
    post_for_img = copy.copy(post_doc)
    post_for_img["comments"] = ordered_selected_comments

    extracted_img_items = extract_ordered_local_images(
        post_for_img,
        max_comments=len(ordered_selected_comments),
        include_comment_images=True,
    )

    valid_img_items: List[Dict[str, Any]] = []
    seen_local_paths = set()

    for local_p in direct_local_images:
        if local_p not in seen_local_paths and _is_valid_local_image(local_p):
            seen_local_paths.add(local_p)
            valid_img_items.append(
                {
                    "origin": "post",
                    "origin_label": ORIGIN_DISPLAY_NAMES["post"],
                    "remote_url": "",
                    "local_path": local_p,
                }
            )

    for item in extracted_img_items:
        lp = item.get("local_path") or ""
        if lp not in seen_local_paths and _is_valid_local_image(lp):
            seen_local_paths.add(lp)
            valid_img_items.append(item)

    # 构建评论图片 -> IMG_k 索引映射，消除 `[图片评论]` 盲区
    path_to_img_tag: Dict[str, str] = {}
    manifest_lines: List[str] = []
    attachment_paths: List[str] = []
    for idx, img_item in enumerate(valid_img_items, 1):
        tag = f"IMG_{idx}"
        lp = img_item["local_path"]
        attachment_paths.append(lp)
        path_to_img_tag[lp] = tag
        manifest_lines.append(f"- `{tag}`（{img_item['origin_label']}）")

    manifest_text = "\n".join(manifest_lines) if manifest_lines else "（本帖无配图附件，纯文字帖）"

    def _format_comment_lines(cmts: List[Dict[str, Any]], is_author_section: bool) -> str:
        if not cmts:
            return "（无）"
        lines: List[str] = []
        for idx, c in enumerate(cmts, 1):
            c_body = strip_inline_media_placeholders(str(c.get("content") or ""))
            c_tags: List[str] = []
            c_lmap = c.get("local_mapping") or {}
            for _, lp in c_lmap.items():
                resolved = str(Path(lp).resolve()) if lp and Path(lp).is_file() else ""
                if resolved and resolved in path_to_img_tag:
                    c_tags.append(path_to_img_tag[resolved])
            img_note = f" [评论配图见上传附件 {', '.join(c_tags)}]" if c_tags else ""
            if not c_body and not img_note:
                continue
            if is_author_section:
                lines.append(f"[作者补充 #{idx}] {c_body}{img_note}".strip())
            else:
                c_user = c.get("author_name") or c.get("author_uid") or "网友"
                likes = safe_int(c.get("likes"), 0)
                like_tag = f" (👍{likes})" if likes > 0 else ""
                lines.append(f"{idx}. @{c_user}{like_tag}: {c_body}{img_note}".strip())
        return "\n".join(lines) if lines else "（无）"

    unified_text = get_unified_post_text(post_doc)
    # 兜底检查 metadata.quoted_post 是否存在尚未拼入正文的引用文字
    qp = (post_doc.get("metadata") or {}).get("quoted_post") or {}
    if isinstance(qp, dict) and str(qp.get("text") or "").strip():
        q_txt = clean_post_text(str(qp.get("text") or ""))
        if q_txt and q_txt not in unified_text:
            q_user = str(qp.get("username") or "").strip()
            q_prefix = f"[引用 @{q_user}]" if q_user else "[引用原帖]"
            unified_text = f"{unified_text}\n{q_prefix}: {q_txt}".strip()

    source = str(post_doc.get("source") or "unknown")
    author_obj = post_doc.get("author") or {}
    author_display = (
        author_obj.get("author_name")
        or author_obj.get("username")
        or post_doc.get("author_id")
        or "匿名作者"
    )
    author_handle = str(author_obj.get("username") or "").strip().lstrip("@")
    author_header = (
        f"{author_display} (@{author_handle})"
        if author_handle and author_handle.lower() != str(author_display).lower().lstrip("@")
        else str(author_display)
    )

    pub_ts = safe_int(post_doc.get("publish_time"), 0)
    pub_str = (
        datetime.fromtimestamp(pub_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        if pub_ts > 0
        else "未知时间"
    )

    post_meta_and_text = (
        f"- 来源平台: {source} | 原帖作者: {author_header} | 发布时间: {pub_str}\n"
        f"- 已上传真实图片附件数: {len(attachment_paths)} 张\n\n"
        f"{unified_text or '（原帖无正文文字，请直接依据上传图片理解内容）'}"
    )

    return {
        "post_doc": post_doc,
        "post_key": make_post_key(source, str(post_doc.get("post_id") or "")),
        "source": source,
        "post_id": str(post_doc.get("post_id") or ""),
        "url": str((post_doc.get("metadata") or {}).get("url") or ""),
        "author_header": author_header,
        "unified_text": unified_text,
        "is_video": is_video_post(post_doc),
        "attachment_paths": attachment_paths,
        "image_items": valid_img_items,
        "author_comments": selected_author_cmts,
        "community_comments": selected_community_cmts,
        "prompt_slots": {
            "POST_META_AND_TEXT": post_meta_and_text,
            "UPLOADED_IMAGES_MANIFEST": manifest_text,
            "AUTHOR_COMMENTS": _format_comment_lines(selected_author_cmts, is_author_section=True),
            "COMMUNITY_COMMENTS": _format_comment_lines(selected_community_cmts, is_author_section=False),
        },
    }


def _make_comment_json_validator(max_weighted_chars: int = 260):
    """构建确定性输出契约校验器（确保结构完整、无垃圾标签/外链、且满足平台物理字符上限）。"""

    def _validate(data: Dict[str, Any]) -> Optional[str]:
        comments = data.get("comments")
        if not isinstance(comments, list) or not comments:
            return "缺少非空数组字段 `comments`"

        for idx, item in enumerate(comments, 1):
            if not isinstance(item, dict):
                return f"`comments[{idx}]` 必须是对象"
            txt = str(item.get("reply_text") or "").strip()
            if not txt:
                return f"`comments[{idx}].reply_text` 不能为空"
            if _URL_RE.search(txt):
                return f"`comments[{idx}].reply_text` 严禁包含外部 URL 链接"
            if _HASHTAG_RE.search(txt):
                return f"`comments[{idx}].reply_text` 严禁包含 #Hashtag 标签"
            w_len = calc_x_weighted_length(txt)
            if max_weighted_chars > 0 and w_len > max_weighted_chars:
                return (
                    f"`comments[{idx}].reply_text` 加权长度 {w_len} 超过单条回复上限 {max_weighted_chars}，请精炼表达"
                )
        return None

    return _validate


def generate_comments_from_post(
    post_input: Union[Dict[str, Any], str],
    extra_comments: Optional[Sequence[Union[str, Dict[str, Any]]]] = None,
    author_comments: Optional[Sequence[Union[str, Dict[str, Any]]]] = None,
    target_language: str = "auto",
    style_hint: str = "",
    candidate_count: Optional[int] = None,
    allow_video_posts: bool = False,
    comment_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    【核心 100% 解耦入口】输入一条完整的原始帖子（及可选的作者自评/其他评论），生成高质量候选神评。

    参数：
    - `post_input`       : 标准 `harvested_social_posts` 字典、轻量自定义字典（含 `text`/`images`/`comments`）或纯文本字符串；
    - `extra_comments`   : 可选追加的评论区其他网友评论列表（支持字符串或评论字典）；
    - `author_comments`  : 可选追加的原帖作者本人补充说明 / 串推自评列表（支持字符串或评论字典）；
    - `target_language`  : `"auto"`（默认自动跟随原帖语种）或任意目标语种（如 `"en"`, `"zh-CN"`, `"zh-TW"`, `"ja"`, `"es"` 等）；
    - `style_hint`       : 可选的附加语调/人设微调提示（默认留空，完全按原帖语境自然生发）；
    - `candidate_count`  : 生成几条不同切入角度的候选评论（默认读取 `COMMENT_CREATOR_CONFIG["candidate_count"]` = 3）；
    - `allow_video_posts`: 默认 `False`（遵循 `PROJECT_PRINCIPLES.md` 源头排除含视频帖，防止对未见画面瞎编）。
    """
    logger = get_pipeline_logger()
    cfg = dict(COMMENT_CREATOR_CONFIG)
    if comment_cfg:
        cfg.update(comment_cfg)

    n_candidates = max(1, int(candidate_count or cfg.get("candidate_count", 3)))
    max_weighted = int(cfg.get("max_weighted_chars", 260))

    bundle = assemble_comment_bundle(
        post_input=post_input,
        extra_comments=extra_comments,
        author_comments=author_comments,
        download_missing_media=bool(cfg.get("download_missing_media", True)),
        max_author_comments=int(cfg.get("max_author_comments", 10)),
        max_community_comments=int(cfg.get("max_community_comments", 15)),
    )

    post_summary = {
        "post_key": bundle["post_key"],
        "source": bundle["source"],
        "post_id": bundle["post_id"],
        "url": bundle["url"],
        "author": bundle["author_header"],
        "text_preview": (bundle["unified_text"] or "")[:160],
        "uploaded_images_count": len(bundle["attachment_paths"]),
        "author_comments_count": len(bundle["author_comments"]),
        "community_comments_count": len(bundle["community_comments"]),
    }

    if bundle["is_video"] and not allow_video_posts:
        logger.info(f"[CommentCreator/跳过视频帖] post_key={bundle['post_key']} 含视频，按准则跳过盲评")
        return {
            "status": "skipped_video",
            "error": "原帖包含视频媒体，默认排除以避免对未下载视频画面产生幻觉（如需仅基于文字强行生成可传 allow_video_posts=True）",
            "best_comment": "",
            "comments": [],
            "detected_language": "",
            "post_key_anchor": "",
            "post_summary": post_summary,
            "llm_trace": None,
        }

    if not bundle["unified_text"].strip() and not bundle["attachment_paths"]:
        return {
            "status": "skipped_empty",
            "error": "原帖既无有效正文也无可用配图，无法生成评论",
            "best_comment": "",
            "comments": [],
            "detected_language": "",
            "post_key_anchor": "",
            "post_summary": post_summary,
            "llm_trace": None,
        }

    prompt_vars = dict(bundle["prompt_slots"])
    prompt_vars.update(
        {
            "LANGUAGE_DIRECTIVE": _build_language_directive(target_language),
            "STYLE_HINT": style_hint.strip() or "（无特殊附加限制，完全贴合原帖具体语境自然发挥）",
            "CANDIDATE_COUNT": str(n_candidates),
        }
    )

    prompt_file = str(cfg.get("prompt_file") or "post_comment_creation.md")
    prompt_text, prompt_meta = render_prompt_template(prompt_file, prompt_vars)

    parsed, trace = call_llm_json(
        prompt=prompt_text,
        prompt_meta=prompt_meta,
        model=cfg.get("model"),
        file_paths=bundle["attachment_paths"] or None,
        timeout=int(cfg.get("timeout", 180)),
        preset_model_group=str(cfg.get("preset_model_group", "medium")),
        validator=_make_comment_json_validator(max_weighted_chars=max_weighted),
    )

    if not parsed:
        err_msg = trace.get("error") or "评论生成多模态模型调用失败"
        logger.warning(f"[CommentCreator/生成失败] post_key={bundle['post_key']} | error={err_msg}")
        return {
            "status": "failed",
            "error": err_msg,
            "best_comment": "",
            "comments": [],
            "detected_language": "",
            "post_key_anchor": "",
            "post_summary": post_summary,
            "llm_trace": trace,
        }

    formatted_comments: List[Dict[str, Any]] = []
    for rank_idx, item in enumerate(parsed.get("comments") or [], 1):
        reply_txt = str(item.get("reply_text") or "").strip().strip('"').strip("“”")
        if not reply_txt:
            continue
        formatted_comments.append(
            {
                "rank": rank_idx,
                "reply_text": reply_txt,
                "angle": str(item.get("angle") or "").strip(),
                "weighted_length": calc_x_weighted_length(reply_txt),
            }
        )

    best_comment = formatted_comments[0]["reply_text"] if formatted_comments else ""
    detected_lang = str(parsed.get("detected_language") or "").strip()
    anchor = str(parsed.get("post_key_anchor") or "").strip()

    logger.info(
        f"[CommentCreator/生成成功] post_key={bundle['post_key']} | lang={detected_lang} | "
        f"附图={len(bundle['attachment_paths'])}张 | 作者自评={len(bundle['author_comments'])}条 | "
        f"网友热评={len(bundle['community_comments'])}条 | 首选评论({calc_x_weighted_length(best_comment)}w): "
        f"{best_comment[:80]}"
    )

    return {
        "status": "ok",
        "best_comment": best_comment,
        "comments": formatted_comments,
        "detected_language": detected_lang,
        "post_key_anchor": anchor,
        "post_summary": post_summary,
        "llm_trace": trace,
    }


def fetch_complete_x_post(
    post_url_or_id: str,
    max_comments: int = 20,
    fetch_external_links: bool = True,
    download_media: bool = True,
    user_data_dir: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    使用 `x_xiaohao_browser_session_dir` 会话（只读安全拉取，零浏览器弹窗，必要时自动回退免登镜像）
    一次性拉齐指定 X 帖子的「完整正文 + 引用原帖 + 外链正文 + 作者自评 + 前排热评 + 本地原图」。
    """
    from platforms.external_link_extractor import enrich_posts_with_external_articles
    from platforms.harvesters.x_harvester import XHarvester

    screen_name, tweet_id = parse_x_post_url_or_id(post_url_or_id)
    if not tweet_id:
        raise ValueError(f"无法从输入 `{post_url_or_id}` 中解析合法的 X 推文 ID")

    session_dir = user_data_dir or get_default_x_session_dir()
    harvester = XHarvester(burner_user_data_dir=session_dir)
    post_doc = harvester.fetch_tweet_with_comments(
        tweet_id=tweet_id,
        screen_name=screen_name,
        max_comments=max_comments,
    )
    if not post_doc:
        return None

    if fetch_external_links:
        enrich_posts_with_external_articles([post_doc], proxies=harvester.proxies)
    post_doc = harvester.normalize_post(post_doc, download_media=download_media)
    return post_doc


def generate_comments_for_x_post(
    post_url_or_id: str,
    extra_comments: Optional[Sequence[Union[str, Dict[str, Any]]]] = None,
    author_comments: Optional[Sequence[Union[str, Dict[str, Any]]]] = None,
    target_language: str = "auto",
    style_hint: str = "",
    candidate_count: Optional[int] = None,
    allow_video_posts: bool = False,
    user_data_dir: Optional[str] = None,
    comment_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    便捷一站式入口：传入任意 X 推文 URL 或 ID -> 使用 `x_xiaohao_browser_session_dir` 拉取完整图文与评论 -> 生成候选神评。
    """
    cfg = dict(COMMENT_CREATOR_CONFIG)
    if comment_cfg:
        cfg.update(comment_cfg)

    session_dir = user_data_dir or get_default_x_session_dir(cfg)
    post_doc = fetch_complete_x_post(
        post_url_or_id=post_url_or_id,
        max_comments=max(20, int(cfg.get("max_community_comments", 15)) + int(cfg.get("max_author_comments", 10))),
        fetch_external_links=True,
        download_media=bool(cfg.get("download_missing_media", True)),
        user_data_dir=session_dir,
    )
    if not post_doc:
        return {
            "status": "failed",
            "error": f"未能拉取到 X 帖子详情: {post_url_or_id}",
            "best_comment": "",
            "comments": [],
            "detected_language": "",
            "post_key_anchor": "",
            "post_summary": {"url": str(post_url_or_id)},
            "llm_trace": None,
        }

    result = generate_comments_from_post(
        post_input=post_doc,
        extra_comments=extra_comments,
        author_comments=author_comments,
        target_language=target_language,
        style_hint=style_hint,
        candidate_count=candidate_count,
        allow_video_posts=allow_video_posts,
        comment_cfg=cfg,
    )
    result["fetched_post"] = post_doc
    return result


def generate_comments_for_post_key(
    post_key: str,
    db: Optional[PipelineDB] = None,
    extra_comments: Optional[Sequence[Union[str, Dict[str, Any]]]] = None,
    author_comments: Optional[Sequence[Union[str, Dict[str, Any]]]] = None,
    target_language: str = "auto",
    style_hint: str = "",
    candidate_count: Optional[int] = None,
    allow_video_posts: bool = False,
    comment_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    便捷数据库入口：按 `source:post_id`（如 `"x:190123..."`, `"reddit:..."`, `"binance:..."`）
    从 MongoDB `harvested_social_posts` 读取完整原帖并生成评论。
    """
    db = db or PipelineDB()
    raw_map = db.fetch_raw_posts_by_keys([post_key])
    post_doc = raw_map.get(post_key)
    if not post_doc:
        return {
            "status": "failed",
            "error": f"在 harvested_social_posts 中未找到 post_key={post_key}",
            "best_comment": "",
            "comments": [],
            "detected_language": "",
            "post_key_anchor": "",
            "post_summary": {"post_key": post_key},
            "llm_trace": None,
        }
    return generate_comments_from_post(
        post_input=post_doc,
        extra_comments=extra_comments,
        author_comments=author_comments,
        target_language=target_language,
        style_hint=style_hint,
        candidate_count=candidate_count,
        allow_video_posts=allow_video_posts,
        comment_cfg=comment_cfg,
    )


def publish_x_comment(
    post_url_or_id: str,
    comment_text: str,
    like: bool = True,
    user_data_dir: Optional[str] = None,
    debug: bool = False,
) -> Tuple[Optional[str], bool, Optional[str]]:
    """
    可选下游执行器封装：使用指定 X 会话（默认 `x_xiaohao_browser_session_dir`）在目标推文下点赞并发送生成的评论。
    返回 `(error_msg, success, comment_id)`。
    """
    from platforms.x_playwright import comment_on_x_post

    screen_name, tweet_id = parse_x_post_url_or_id(post_url_or_id)
    if not tweet_id:
        return f"无效的 X 帖子链接或 ID: {post_url_or_id}", False, None
    post_url = (
        post_url_or_id
        if str(post_url_or_id).startswith(("http://", "https://"))
        else f"https://x.com/{screen_name or 'i'}/status/{tweet_id}"
    )
    session_dir = user_data_dir or get_default_x_session_dir()
    return comment_on_x_post(
        target_post_url=post_url,
        content=comment_text,
        like=like,
        user_data_dir=session_dir,
        debug=debug,
    )


if __name__ == "__main__":
    import json

    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    print(f"[配置检查] 默认 X 会话目录 (x_xiaohao_browser_session_dir): {get_default_x_session_dir()}")

    # 示例 1：直接传入完整帖子结构 + 原帖作者自己的补充评论 -> 自动对齐原帖语言生成神评
    demo_post = {
        "source": "x",
        "author": {"username": "indie_builder", "author_name": "Alex Rivera"},
        "text": (
            "Spent 4 months building a custom billing engine from scratch so we wouldn't pay Stripe 2.9%. "
            "Total Stripe fees we would have paid on last month's revenue: $41.80."
        ),
        "author_comments": [
            "Before anyone asks: yes, tax compliance alone broke me twice last week."
        ],
        "comments": [
            {"author_name": "dev_dan", "content": "Classic premature optimization lol", "likes": 85},
            {"author_name": "sarah_ships", "content": "Every engineer's rite of passage.", "likes": 34},
        ],
    }
    demo_res = generate_comments_from_post(demo_post, target_language="auto")
    print("\n=== 示例 1：直接输入完整原帖 + 作者自评（自动英文对齐）===")
    print(json.dumps({k: v for k, v in demo_res.items() if k != "llm_trace"}, ensure_ascii=False, indent=2))

    # 示例 2：同一条帖子，指定输出任意其他语言（例如简体中文）
    demo_zh_res = generate_comments_from_post(demo_post, target_language="zh-CN")
    print("\n=== 示例 2：指定生成简体中文评论 ===")
    print(json.dumps({k: v for k, v in demo_zh_res.items() if k != "llm_trace"}, ensure_ascii=False, indent=2))
