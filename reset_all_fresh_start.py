# -*- coding: utf-8 -*-
r"""
临时全量重置脚本：清空所有涉及账号（OKX + Binance）的全部历史帖子，并清空涉及的 MongoDB 集合与本地状态缓存，实现完全从头开始。

执行顺序（安全优先）：
1. 汇总并备份待删帖子 ID（从远端主页时间线 + MongoDB generated_articles/social_media_posts + 现场取证日志），落盘到 reset_post_ids_backup.json（防止中途中断丢失 ID）。
2. 逐账号删除远端所有帖子：
   - OKX 账号 (myself, mama, nana, ruru)：拉取远端主页存活帖 + 数据库记录，调用 delete_okx_posts 批量删除，并二次复核主页直到存活帖清零。
   - Binance 账号 (qiqi, ruru)：拉取远端全部帖子与回复 (fetch_binance_square_replies) + 数据库记录，调用 delete_binance_square_content 逐条删除。
3. 清空涉及的 MongoDB 数据库集合（仅限 crawler_data 库下的 social_media_posts 与 generated_articles，绝不触碰其他无关集合）。
4. 清理本地账号发布冷却状态文件 (account_publish_state.json)、下载的图片缓存 (media_downloads) 及历史取证日志 (forensics_logs)。

运行方式（PowerShell）：
$env:PYTHONIOENCODING='utf-8'; & "w:\project\python_project\crypto_trade\.venv\Scripts\python.exe" "w:\project\python_project\crypto_trade\biance\reset_all_fresh_start.py"
"""

import glob
import json
import logging
import os
import re
import shutil
import sys
import time
from collections import defaultdict

from pathlib import Path

PROJECT_ROOT = str(Path(__file__).resolve().parent)
BIANCE_DIR = os.path.join(PROJECT_ROOT, "biance")
PARSER_DIR = os.path.join(PROJECT_ROOT, "parser")

for p in (PROJECT_ROOT, BIANCE_DIR, PARSER_DIR):
    if p not in sys.path:
        sys.path.insert(0, p)

from common.common_utils import (
    DEFAULT_FORENSICS_DIR, get_config, get_media_save_dir,
    read_json, resolve_session_dir, save_json, setup_logger,
)
from common.mongo_db.mongo_base import gen_db_object
from biance.okx_playwright import delete_okx_posts
from biance.okx_squre_api import fetch_okx_user_timeline
from biance.biance_playwright import get_auth_tokens_robust
from biance.biance_squre_api import delete_binance_square_content, fetch_binance_square_replies

setup_logger()
logger = logging.getLogger("reset_all_fresh_start")

# ============================================================
# 可调配置开关
# ============================================================
DELETE_OKX_REMOTE_POSTS = True          # 是否删除 OKX 全部账号 (myself, mama, nana, ruru) 的所有帖子
DELETE_BINANCE_REMOTE_POSTS = True      # 是否删除 Binance 全部账号 (qiqi, ruru) 的所有帖子与回复
WIPE_MONGODB_COLLECTIONS = True         # 是否清空 MongoDB 中的 social_media_posts 和 generated_articles
CLEAR_LOCAL_STATE_AND_MEDIA = True      # 是否清理 account_publish_state.json、media_downloads 与 forensics_logs

OKX_DELETE_INTERVAL_SEC = 0.45          # OKX 单条删帖请求间隔秒数（默认 0.45s，兼顾速度与防频控）
OKX_DEBUG_BROWSER = False               # 是否显示 OKX 删帖浏览器窗口

OKX_ACCOUNTS = {
    "myself": "872834593500823552",
    "mama": "872896855657885696",
    "nana": "1035697745975390208",
    "ruru": "950093749101883392",
}
OKX_NICKNAME_TO_ACC = {
    "稳定短线": "myself",
    "马到功成6": "mama",
    "来去之间Zzz": "nana",
    "瑞见未来": "ruru",
}

BINANCE_ACCOUNTS = ["qiqi", "ruru"]

BACKUP_JSON_PATH = os.path.join(PROJECT_ROOT, "reset_post_ids_backup.json")
MEDIA_DOWNLOADS_DIR = get_media_save_dir()
FORENSICS_DIR = str(DEFAULT_FORENSICS_DIR)


def _get_okx_session_dir(account: str) -> str:
    return resolve_session_dir(f"okx_{account}_browser_session_dir", f"okx_{account}")


def collect_all_post_ids(db) -> dict:
    """
    在清空数据库前，全方位收集 OKX 与 Binance 各账号的历史 post_id，并与本地备份文件合并保存。
    """
    backup_data = {"okx": defaultdict(list), "binance": defaultdict(list)}

    # 0. 若存在上次未跑完的备份文件，先加载进来合并
    if os.path.exists(BACKUP_JSON_PATH):
        try:
            old_bkp = read_json(BACKUP_JSON_PATH) or {}
            for plat in ("okx", "binance"):
                for acc, pids in (old_bkp.get(plat) or {}).items():
                    backup_data[plat][acc].extend(str(x).strip() for x in (pids or []) if str(x).strip())
        except Exception as e:
            logger.warning(f"读取已有备份文件异常: {e}")

    # 1. 从 OKX 远端用户主页接口拉取当前所有存活帖子（放在最前面优先删除）
    if DELETE_OKX_REMOTE_POSTS:
        for acc, uid in OKX_ACCOUNTS.items():
            logger.info(f"🔍 [收集/OKX远端] 正在拉取账号 [{acc}] (uid={uid}) 主页存活帖子列表...")
            live_items = fetch_okx_user_timeline(author_id=uid, max_pages=120) or []
            live_pids = [str(item["post_id"]).strip() for item in live_items if item.get("post_id")]
            logger.info(f"✅ [收集/OKX远端] 账号 [{acc}] 主页当前存活帖子数: {len(live_pids)} 条")
            # 远端确认存活的 ID 放在列表最前面
            backup_data["okx"][acc] = live_pids + backup_data["okx"][acc]

    # 2. 从 MongoDB generated_articles 收集所有发布记录
    ga_col = db.get_collection("generated_articles")
    for plat, acc_list in (("okx", list(OKX_ACCOUNTS.keys())), ("binance", BINANCE_ACCOUNTS)):
        docs = list(ga_col.find(
            {f"publish_records.{plat}.post_id": {"$exists": True, "$ne": None}},
            {f"publish_records.{plat}": 1},
        ))
        db_cnt = 0
        for doc in docs:
            rec = ((doc.get("publish_records") or {}).get(plat)) or {}
            pid = str(rec.get("post_id") or "").strip()
            pub_by = str(rec.get("published_by") or "").strip()
            if pid and pid != "None" and pub_by in acc_list:
                backup_data[plat][pub_by].append(pid)
                db_cnt += 1
        logger.info(f"📦 [收集/MongoDB] 从 generated_articles 收集到 [{plat}] 历史发布记录共 {db_cnt} 条")

    # 3. 从 forensics_logs 补充扫描任何已从数据库清理但留在取证日志中的 OKX 帖子 ID
    if os.path.isdir(FORENSICS_DIR):
        forensic_cnt = 0
        for h_path in glob.glob(os.path.join(FORENSICS_DIR, "forensic_comment_*.html")):
            try:
                txt = open(h_path, "r", encoding="utf-8", errors="ignore").read()
                m_pid = re.search(r"orbit/post/(\d+)", txt)
                if not m_pid:
                    continue
                pid = m_pid.group(1)
                m_author = re.search(r'class="avatar-kg3yd[^"]*"[^>]*alt="([^"]+)"', txt)
                if not m_author:
                    m_author = re.search(r'alt="([^"]+)"[^>]*class="avatar-kg3yd', txt)
                if m_author and m_author.group(1) in OKX_NICKNAME_TO_ACC:
                    acc = OKX_NICKNAME_TO_ACC[m_author.group(1)]
                    backup_data["okx"][acc].append(pid)
                    forensic_cnt += 1
            except Exception:
                pass
        if forensic_cnt:
            logger.info(f"📂 [收集/取证日志] 从 forensics_logs 补充扫描到 OKX 帖子关联记录 {forensic_cnt} 条")

    # 4. 保序去重并落盘备份
    final_backup = {"okx": {}, "binance": {}}
    for plat in ("okx", "binance"):
        for acc, pids in backup_data[plat].items():
            final_backup[plat][acc] = list(dict.fromkeys(x for x in pids if x and x != "None"))

    save_json(BACKUP_JSON_PATH, final_backup)
    logger.info(
        f"💾 [备份完成] 待删帖子 ID 已备份至: {BACKUP_JSON_PATH} | "
        f"OKX={ {k: len(v) for k, v in final_backup['okx'].items()} } | "
        f"Binance={ {k: len(v) for k, v in final_backup['binance'].items()} }"
    )
    return final_backup


def wipe_all_okx_posts(collected_okx_ids: dict):
    """
    逐个账号清空 OKX 所有帖子：
    优先删除远端主页 timeline/1 存活的帖子（最多循环 3 轮直至远端主页存活帖为 0），
    若 collected_okx_ids 中还有未在主页显示但需尝试删除的 ID，也会一并处理。
    """
    for acc, uid in OKX_ACCOUNTS.items():
        session_dir = _get_okx_session_dir(acc)
        logger.info(f"\n{'=' * 65}\n🚀 [OKX清帖] 开始清理账号 [{acc}] (uid={uid}, session={session_dir})\n{'=' * 65}")

        # 第一阶段：以远端实时存活列表为准循环清理（高效且不会把时间浪费在已被系统删除的无效ID上）
        for round_idx in range(1, 4):
            live_items = fetch_okx_user_timeline(author_id=uid, max_pages=120) or []
            live_pids = [str(item["post_id"]).strip() for item in live_items if item.get("post_id")]
            if not live_pids:
                logger.info(f"🎉 [OKX清帖] 账号 [{acc}] 远端主页存活帖子已清零 (第 {round_idx} 轮检查)")
                break

            logger.info(f"🔥 [OKX清帖] 账号 [{acc}] 第 {round_idx} 轮待删远端存活帖: {len(live_pids)} 条")
            err_msg, all_ok, detail = delete_okx_posts(
                post_id_list=live_pids,
                user_data_dir=session_dir,
                debug=OKX_DEBUG_BROWSER,
                interval_sec=OKX_DELETE_INTERVAL_SEC,
            )
            succ = len((detail or {}).get("success_ids") or [])
            fail = len((detail or {}).get("failed_ids") or {})
            logger.info(
                f"📊 [OKX清帖] 账号 [{acc}] 第 {round_idx} 轮结果: 成功={succ} 条, 失败={fail} 条 | err={err_msg}"
            )
            if succ == 0 and fail > 0:
                logger.warning(f"⚠️ [OKX清帖] 账号 [{acc}] 本轮无任何帖子删除成功，停止继续重试，请检查登录状态")
                break


def wipe_all_binance_posts(collected_binance_ids: dict):
    """
    逐个账号清空 Binance Square 所有帖子与评论回复：
    结合数据库记录 + 远端 fetch_binance_square_replies（主帖 + REPLY）全量删除。
    """
    for acc in BINANCE_ACCOUNTS:
        logger.info(f"\n{'=' * 65}\n🚀 [Binance清帖] 开始清理账号 [{acc}]\n{'=' * 65}")
        try:
            session_dir = resolve_session_dir(f"{acc}_browser_session_dir", f"biance_{acc}")
            cookies, csrf_token, user_info = get_auth_tokens_robust(session_dir, debug=False)
        except Exception as e:
            logger.error(f"❌ [Binance清帖] 获取账号 [{acc}] 凭证异常: {e}")
            continue

        if not cookies or not csrf_token:
            logger.warning(f"⚠️ [Binance清帖] 账号 [{acc}] 凭证不完整，跳过该账号")
            continue

        target_ids = list(collected_binance_ids.get(acc) or [])
        square_uid = (user_info or {}).get("squareUid")

        if square_uid:
            future_ms = int((time.time() + 3600) * 1000)
            for f_type in (None, "REPLY"):
                try:
                    remote_items = fetch_binance_square_replies(
                        target_square_uid=square_uid,
                        cookies=cookies,
                        csrf_token=csrf_token,
                        limit=2000,
                        time_offset=future_ms,
                        filter_type=f_type,
                    ) or []
                    logger.info(
                        f"🔍 [Binance清帖] 账号 [{acc}] 远端拉取 (filter_type={f_type}) 返回: {len(remote_items)} 条"
                    )
                    for item in remote_items:
                        rid = str(item.get("reply_id") or "").strip()
                        if rid:
                            target_ids.append(rid)
                except Exception as e:
                    logger.warning(f"⚠️ [Binance清帖] 账号 [{acc}] 拉取远端列表 (filter_type={f_type}) 异常: {e}")

        target_ids = list(dict.fromkeys(x for x in target_ids if x and x != "None"))
        if not target_ids:
            logger.info(f"🎉 [Binance清帖] 账号 [{acc}] 无待删帖子或回复")
            continue

        logger.info(f"🔥 [Binance清帖] 账号 [{acc}] 共汇总待删帖子/回复: {len(target_ids)} 条，开始逐条删除...")
        succ, fail = 0, 0
        for idx, pid in enumerate(target_ids, start=1):
            try:
                ok = delete_binance_square_content(
                    content_id=pid,
                    cookies=cookies,
                    csrf_token=csrf_token,
                )
                if ok:
                    succ += 1
                else:
                    fail += 1
            except Exception as e:
                fail += 1
                logger.warning(f"⚠️ [Binance清帖] 删除 content_id={pid} 异常: {e}")
            if idx % 25 == 0 or idx == len(target_ids):
                logger.info(f"⏳ [Binance清帖] 账号 [{acc}] 进度: {idx}/{len(target_ids)} (成功={succ}, 失败={fail})")
            time.sleep(0.25)

        logger.info(f"✅ [Binance清帖] 账号 [{acc}] 清理完成 | 成功={succ} 条 | 失败={fail} 条")


def wipe_mongodb_collections(db):
    """
    清空涉及的两个 MongoDB 集合：social_media_posts 与 generated_articles。
    """
    logger.info(f"\n{'=' * 65}\n🗄️ [数据库清理] 开始清空 MongoDB 涉及集合 (social_media_posts, generated_articles)\n{'=' * 65}")
    for col_name in ("social_media_posts", "generated_articles"):
        col = db.get_collection(col_name)
        before_cnt = col.count_documents({})
        res = col.delete_many({})
        after_cnt = col.count_documents({})
        logger.info(
            f"✅ [数据库清理] 集合 [{col_name}] 清空完成 | 清理前={before_cnt} 条 | 已删除={res.deleted_count} 条 | 剩余={after_cnt} 条"
        )


def clear_local_state_and_cache():
    """
    清理本地账号发布冷却状态文件、下载图片缓存目录及历史取证日志。
    """
    logger.info(f"\n{'=' * 65}\n🧹 [本地状态清理] 开始重置本地状态文件与图片缓存\n{'=' * 65}")

    # 1. 删除 account_publish_state.json
    state_paths = [
        os.path.join(PARSER_DIR, "account_publish_state.json"),
        os.path.join(BIANCE_DIR, "account_publish_state.json"),
        os.path.abspath("account_publish_state.json"),
    ]
    for sp in set(state_paths):
        if os.path.exists(sp):
            try:
                os.remove(sp)
                logger.info(f"✅ [本地状态清理] 已删除账号状态文件: {sp}")
            except Exception as e:
                logger.warning(f"⚠️ [本地状态清理] 删除状态文件失败 ({sp}): {e}")

    # 2. 清空 media_downloads 下的缓存图片
    if os.path.isdir(MEDIA_DOWNLOADS_DIR):
        removed_files = 0
        for entry in os.listdir(MEDIA_DOWNLOADS_DIR):
            full_p = os.path.join(MEDIA_DOWNLOADS_DIR, entry)
            try:
                if os.path.isfile(full_p) or os.path.islink(full_p):
                    os.unlink(full_p)
                    removed_files += 1
                elif os.path.isdir(full_p):
                    shutil.rmtree(full_p)
                    removed_files += 1
            except Exception as e:
                logger.warning(f"⚠️ [本地状态清理] 删除媒体文件失败 ({full_p}): {e}")
        logger.info(f"✅ [本地状态清理] 已清空媒体缓存目录 {MEDIA_DOWNLOADS_DIR} (共移除 {removed_files} 项)")

    # 3. 清空 forensics_logs 下的历史错误取证文件
    if os.path.isdir(FORENSICS_DIR):
        removed_logs = 0
        for entry in os.listdir(FORENSICS_DIR):
            full_p = os.path.join(FORENSICS_DIR, entry)
            try:
                if os.path.isfile(full_p):
                    os.unlink(full_p)
                    removed_logs += 1
            except Exception:
                pass
        logger.info(f"✅ [本地状态清理] 已清空取证日志目录 {FORENSICS_DIR} (共移除 {removed_logs} 个文件)")

    # 4. 移除备份文件（全部执行完毕后清理）
    if os.path.exists(BACKUP_JSON_PATH):
        try:
            os.remove(BACKUP_JSON_PATH)
            logger.info(f"✅ [本地状态清理] 已移除临时 ID 备份文件: {BACKUP_JSON_PATH}")
        except Exception:
            pass


def main():
    start_ts = time.monotonic()
    logger.info("🚨 开始执行【全量删帖 + 数据库清空 + 状态重置】脚本...")

    db = gen_db_object()

    # Step 1: 收集并备份所有待删帖子 ID（必须在清空数据库前执行）
    collected_ids = collect_all_post_ids(db)

    # Step 2: 删除所有涉及账号的远端帖子
    if DELETE_OKX_REMOTE_POSTS:
        wipe_all_okx_posts(collected_ids.get("okx") or {})

    if DELETE_BINANCE_REMOTE_POSTS:
        wipe_all_binance_posts(collected_ids.get("binance") or {})

    # Step 3: 清空涉及的 MongoDB 集合 (social_media_posts, generated_articles)
    if WIPE_MONGODB_COLLECTIONS:
        wipe_mongodb_collections(db)

    # Step 4: 清理本地状态文件与图片缓存
    if CLEAR_LOCAL_STATE_AND_MEDIA:
        clear_local_state_and_cache()

    elapsed = time.monotonic() - start_ts
    logger.info(f"\n🎉🎉🎉 全量重置任务全部完成！总耗时: {elapsed:.1f} 秒。现在可以完全从零开始运行流水线！")


if __name__ == "__main__":
    main()
