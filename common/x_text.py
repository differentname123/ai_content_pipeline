# -*- coding: utf-8 -*-
"""
X (Twitter) 加权字符与长度校验单一事实来源 (`common/x_text.py`)

统一 X 平台推文与回复的长度核算口径（供 `platforms/` 与 `x_growth/` 共享）：
- URL (`http://...` / `https://...`) 按 Twitter `t.co` 规则统一计 `23` 单位；
- 基础拉丁 / ASCII 及西文常用标点区段计 `1` 单位；
- 中日韩文字、全角字符、Emoji（星平面字符 > 0xFFFF）等统一计 `2` 单位。
"""
import re
from typing import Optional

X_URL_WEIGHTED_LENGTH = 23
X_STANDARD_MAX_WEIGHTED_LENGTH = 280

_URL_RE = re.compile(r"https?://[^\s<>\"')\]]+", re.IGNORECASE)
_TRAILING_PUNCT_RE = re.compile(r"[.,!?;:]+$")


def calc_x_weighted_length(text: str) -> int:
    """按 X (Twitter) 加权规则计算文本计数单位。"""
    if not text:
        return 0
    s = str(text).strip()
    if not s:
        return 0

    total = 0

    def _replace_url(match: re.Match) -> str:
        nonlocal total
        raw_url = match.group(0)
        trimmed = _TRAILING_PUNCT_RE.sub("", raw_url)
        trailing_len = len(raw_url) - len(trimmed)
        total += X_URL_WEIGHTED_LENGTH + trailing_len
        return ""

    rem = _URL_RE.sub(_replace_url, s)
    for ch in rem:
        cp = ord(ch)
        if 0x0000 <= cp <= 0x10FF or 0x2000 <= cp <= 0x200D or 0x2010 <= cp <= 0x201F or 0x2032 <= cp <= 0x2037:
            total += 1
        else:
            total += 2
    return total


def calc_raw_char_length(text: str) -> int:
    """计算去除首尾空白后的原始字符数。"""
    if not text:
        return 0
    return len(str(text).strip())


def is_within_x_limit(
    text: str,
    max_units: int = X_STANDARD_MAX_WEIGHTED_LENGTH,
    max_raw_chars: Optional[int] = None,
) -> bool:
    """判断文本是否同时满足 X 加权单位上限及可选的原始字符数上限。"""
    if not text or not str(text).strip():
        return False
    if max_units > 0 and calc_x_weighted_length(text) > int(max_units):
        return False
    if max_raw_chars is not None and max_raw_chars > 0 and calc_raw_char_length(text) > int(max_raw_chars):
        return False
    return True
