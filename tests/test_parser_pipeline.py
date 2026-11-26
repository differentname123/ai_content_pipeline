# -*- coding: utf-8 -*-
"""Parser pipeline unit tests: publish_state, platform_adapters, article_generator, publisher_arena."""
import json
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from PIL import Image

from parser.article_generator import (
    _convert_viral_article_info,
    build_article_task,
    build_viral_article_task,
    check_article_info,
    check_viral_article_info,
    execute_article_task,
    execute_viral_article_task,
    extract_and_group_valid_evidences,
    extract_valid_viral_posts,
    generate_analysis_articles,
    generate_viral_articles,
    get_remodeled_viral_post_ids,
    transform_mlus,
)
from parser.mlu_pipeline import check_format_info, is_need_formatting, is_need_pre_screening
from parser.platform_adapters import (
    BinancePlatform,
    OkxPlatform,
    _build_publish_text_list,
    _format_analysis_post_content,
    _format_viral_post_content,
    has_rival_exchange_ui,
    has_rival_keyword,
    infer_exchange_ui_from_description,
    normalize_exchange_ui,
    score_platform_candidate,
    score_platform_candidate_form,
    sync_published_metrics,
)
from parser.post_ingestion import (
    ARTICLE_MAX_CHARS,
    ARTICLE_TYPE_ANALYSIS,
    ARTICLE_TYPE_VIRAL,
    _fetch_hot_posts,
    _is_valid_raw_post,
    ensure_post_media_downloaded,
    prompt_version,
)
from parser.publish_state import AccountPublishStateStore, classify_infra_backoff_error
from parser.publisher_arena import (
    _check_account_health_guard,
    _collect_publish_candidates,
    _publish_articles_for_platform,
    _select_and_polish_best_candidate,
)


def _create_tiny_png(directory: Path, name: str = "sample.png", color=(12, 120, 210)) -> str:
    p = directory / name
    img = Image.new("RGB", (16, 16), color=color)
    if color == (12, 120, 210):
        # Generate distinct spatial luminance gradients per filename so 9x8 dHash differs reliably
        hv = sum((idx + 1) * ord(ch) for idx, ch in enumerate(name))
        mode = hv % 4
        for y in range(16):
            for x in range(16):
                if mode == 0:
                    v = (x * 16 + y * 5 + hv) % 256
                elif mode == 1:
                    v = ((15 - x) * 16 + y * 7 + hv) % 256
                elif mode == 2:
                    v = (y * 16 + x * 3 + hv) % 256
                else:
                    v = ((x ^ y) * 17 + hv) % 256
                img.putpixel((x, y), (v, (v + 60) % 256, (255 - v) % 256))
    img.save(p, format="PNG")
    return str(p)


class TestPublishStateStore(unittest.TestCase):
    def test_parser_config_is_single_source_of_truth(self):
        import parser.config as pcfg
        import parser.article_format_logical as afl
        import parser.maintenance_tasks as mt
        import parser.publish_state as ps

        self.assertIs(mt.INTERACTION_CONFIG, pcfg.INTERACTION_CONFIG)
        self.assertEqual(ps.ACCOUNT_COOLDOWN_SECONDS, pcfg.ACCOUNT_COOLDOWN_SECONDS)
        self.assertEqual(ps.OKX_ACCOUNT_COOLDOWN_SECONDS, pcfg.OKX_ACCOUNT_COOLDOWN_SECONDS)
        self.assertEqual(set(afl.TASK_SWITCHES.keys()), set(afl.TASK_REGISTRY.keys()))

    def test_failure_does_not_block_and_cooldown_topic_reset(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "state.json"
            store = AccountPublishStateStore(str(state_path))
            t0 = 1_700_000_000.0

            # Initially eligible for both analysis and viral
            self.assertEqual(store.can_publish("binance", "ruru", is_viral=False, now=t0), (True, ""))
            self.assertEqual(store.can_publish("binance", "ruru", is_viral=True, now=t0), (True, ""))

            # Transient timeout/button errors record diagnostics but do NOT block publishing
            detail = store.record_failure("binance", "ruru", "timeout waiting for button", is_viral=False, now=t0)
            self.assertEqual(detail, "timeout waiting for button")
            self.assertEqual(store.can_publish("binance", "ruru", is_viral=False, now=t0 + 1), (True, ""))
            self.assertEqual(store.can_publish("binance", "ruru", is_viral=True, now=t0 + 1), (True, ""))

            # Legacy future backoff_until / viral_backoff_until timestamps in state JSON are ignored and cleaned on write
            raw_state = json.loads(state_path.read_text(encoding="utf-8"))
            raw_state["binance"]["ruru"]["backoff_until"] = t0 + 99_999
            raw_state["binance"]["ruru"]["backoff_reason"] = "auth_expired"
            raw_state["binance"]["ruru"]["viral_backoff_until"] = t0 + 99_999
            raw_state["binance"]["ruru"]["viral_backoff_reason"] = "waf_challenge"
            state_path.write_text(json.dumps(raw_state), encoding="utf-8")

            self.assertEqual(store.can_publish("binance", "ruru", is_viral=False, now=t0 + 2), (True, ""))
            self.assertEqual(store.can_publish("binance", "ruru", is_viral=True, now=t0 + 2), (True, ""))

            # Ordinary non-WAF viral failure records count and cleans legacy viral_backoff_* without blocking
            store.record_failure("binance", "ruru", "[业务拦截] 频率限制", is_viral=True, now=t0 + 5)
            snap_after_biz_fail = store.get_account_state("binance", "ruru")
            self.assertNotIn("viral_backoff_until", snap_after_biz_fail)
            self.assertNotIn("viral_backoff_reason", snap_after_biz_fail)
            self.assertEqual(snap_after_biz_fail["viral_consecutive_failures"], 1)
            self.assertEqual(store.can_publish("binance", "ruru", is_viral=True, now=t0 + 6), (True, ""))

            # HTTP 405 / WAF triggers stepped account-level backoff (30m -> 60m -> 120m cap) blocking both types on THAT account ONLY
            store.record_failure("binance", "myself", "[WAF/人机验证] 广场页面返回 HTTP 405", is_viral=False, now=t0 + 10)
            ok_ana, reason_waf = store.can_publish("binance", "myself", is_viral=False, now=t0 + 20)
            ok_vir, _ = store.can_publish("binance", "myself", is_viral=True, now=t0 + 20)
            self.assertFalse(ok_ana)
            self.assertFalse(ok_vir)
            self.assertIn("waf_challenge", reason_waf)
            # Healthy peer account remains completely unaffected (fault-domain isolation)
            self.assertEqual(store.can_publish("binance", "ruru", is_viral=False, now=t0 + 20), (True, ""))

            # Auto-recovers after 1800s window expires; 2nd WAF backs off 3600s, 3rd+ caps at 7200s
            self.assertEqual(store.can_publish("binance", "myself", is_viral=False, now=t0 + 10 + 1801), (True, ""))
            store.record_failure("binance", "myself", "HTTP 405", is_viral=True, now=t0 + 2000)
            self.assertFalse(store.can_publish("binance", "myself", is_viral=False, now=t0 + 2000 + 3500)[0])
            self.assertTrue(store.can_publish("binance", "myself", is_viral=False, now=t0 + 2000 + 3601)[0])
            store.record_failure("binance", "myself", "ChallengeRequiredError", is_viral=False, now=t0 + 6000)
            self.assertFalse(store.can_publish("binance", "myself", is_viral=False, now=t0 + 6000 + 7100)[0])
            self.assertTrue(store.can_publish("binance", "myself", is_viral=False, now=t0 + 6000 + 7201)[0])

            # Auth expired triggers 60m (3600s) account backoff and auto-recovers
            store.record_failure(
                "binance",
                "ruru",
                "页面探测到登录入口或会话已超时，本地 Cookie 可能已过期失效，请先重新登录。",
                is_viral=False,
                now=t0 + 7000,
            )
            ok_auth, reason_auth = store.can_publish("binance", "ruru", is_viral=True, now=t0 + 7000 + 1800)
            self.assertFalse(ok_auth)
            self.assertIn("auth_expired", reason_auth)
            self.assertTrue(store.can_publish("binance", "ruru", is_viral=True, now=t0 + 7000 + 3601)[0])

            # Success resets backoff + consecutive_failures, cleans legacy keys, and enforces 3h account cooldown + 2h topic cooldown
            t1 = t0 + 20_000
            store.record_success("binance", "myself", is_viral=False, topic="BTC", now=t1)
            snap = store.get_account_state("binance", "myself")
            self.assertNotIn("backoff_until", snap)
            self.assertNotIn("backoff_reason", snap)
            self.assertEqual(snap["account_backoff_until"], 0)
            self.assertEqual(snap["account_backoff_reason"], "")
            self.assertEqual(snap["account_infra_failures"], 0)
            self.assertEqual(snap["consecutive_failures"], 0)
            self.assertEqual(snap["account_last_publish_time"], t1)
            self.assertEqual(snap["last_publish_type"], "analysis")
            self.assertTrue(store.is_topic_in_cooldown("binance", "myself", "BTC", now=t1 + 3600))
            self.assertFalse(store.is_topic_in_cooldown("binance", "myself", "BTC", now=t1 + 7201))

            # Analysis & viral share the same account-level global cooldown (3h)
            ok_before_3h, _ = store.can_publish("binance", "myself", is_viral=False, now=t1 + 2 * 3600)
            ok_viral_before_3h, _ = store.can_publish("binance", "myself", is_viral=True, now=t1 + 2 * 3600)
            ok_after_3h, _ = store.can_publish("binance", "myself", is_viral=False, now=t1 + 3 * 3600 + 1)
            ok_viral_after_3h, _ = store.can_publish("binance", "myself", is_viral=True, now=t1 + 3 * 3600 + 1)
            self.assertFalse(ok_before_3h)
            self.assertFalse(ok_viral_before_3h)
            self.assertTrue(ok_after_3h)
            self.assertTrue(ok_viral_after_3h)

    def test_account_global_cooldown_legacy_compat_and_try_claim_concurrency(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "legacy_state.json"
            t0 = 1_700_000_000.0
            # Legacy JSON with only viral_last_publish_time (no account_last_publish_time)
            state_path.write_text(
                json.dumps({"binance": {"myself": {"viral_last_publish_time": t0}}}),
                encoding="utf-8",
            )
            store = AccountPublishStateStore(str(state_path))
            ok_an, reason_an = store.can_publish("binance", "myself", is_viral=False, now=t0 + 1800)
            self.assertFalse(ok_an)
            self.assertIn("账号全局发布冷却中", reason_an)

            # After cooldown passes, try_claim_account prevents concurrent claim across types
            t_ready = t0 + 3 * 3600 + 5
            claimed1, _ = store.try_claim_account("binance", "myself", is_viral=False, now=t_ready)
            self.assertTrue(claimed1)
            claimed2, reason2 = store.try_claim_account("binance", "myself", is_viral=True, now=t_ready + 1)
            self.assertFalse(claimed2)
            self.assertIn("正在执行中", reason2)
            ok_while_claimed, _ = store.can_publish("binance", "myself", is_viral=True, now=t_ready + 1)
            self.assertFalse(ok_while_claimed)

            store.release_account("binance", "myself")
            ok_after_release, _ = store.can_publish("binance", "myself", is_viral=True, now=t_ready + 1)
            self.assertTrue(ok_after_release)

    def test_thread_safety_smoke(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = AccountPublishStateStore(str(Path(tmp) / "concurrent_state.json"))
            errors = []

            def _worker(idx: int):
                try:
                    for j in range(5):
                        store.record_success("binance", f"acc_{idx}", topic=f"COIN{j}", now=1000.0 + idx * 10 + j)
                        store.can_publish("binance", f"acc_{idx}", now=2000.0)
                except Exception as exc:
                    errors.append(exc)

            threads = [threading.Thread(target=_worker, args=(i,)) for i in range(4)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            self.assertEqual(errors, [])


class TestPlatformAdapters(unittest.TestCase):
    def test_has_rival_keyword(self):
        self.assertTrue(has_rival_keyword("binance", "我在 OKX 上交易"))
        self.assertTrue(has_rival_keyword("binance", "我在 OKEX 交易"))
        self.assertTrue(has_rival_keyword("binance", "欧易最新公告"))
        self.assertTrue(has_rival_keyword("binance", "欧意最新公告"))
        self.assertTrue(has_rival_keyword("binance", "Bybit 跟单爆了"))
        self.assertTrue(has_rival_keyword("binance", "在 Bitget 开多"))
        self.assertTrue(has_rival_keyword("binance", "HTX 转出"))
        self.assertTrue(has_rival_keyword("binance", "Gate.io 首发"))
        self.assertTrue(has_rival_keyword("binance", "火币历史公告"))
        self.assertTrue(has_rival_keyword("binance", "$OKB 日线突破前高"))
        self.assertFalse(has_rival_keyword("binance", "lookbook 观察笔记"))
        self.assertFalse(has_rival_keyword("binance", "Binance BTC setup"))
        self.assertFalse(has_rival_keyword("binance", "navigate this high volatility"))
        self.assertTrue(has_rival_keyword("okx", "在币安看到一个信号"))
        self.assertTrue(has_rival_keyword("okx", "Binance 合约截图"))
        self.assertTrue(has_rival_keyword("okx", "Bybit 跟单翻车"))
        self.assertTrue(has_rival_keyword("okx", "Bitget 账户爆仓"))
        self.assertTrue(has_rival_keyword("okx", "火币老图"))
        self.assertFalse(has_rival_keyword("okx", "OKX ETH setup"))
        self.assertFalse(has_rival_keyword("okx", "$BNB 突破前高"))

    def test_normalize_and_has_rival_exchange_ui(self):
        self.assertEqual(normalize_exchange_ui(None), "")
        self.assertEqual(normalize_exchange_ui("  "), "")
        self.assertEqual(normalize_exchange_ui("none"), "")
        self.assertEqual(normalize_exchange_ui("无"), "")
        self.assertEqual(normalize_exchange_ui("Binance"), "binance")
        self.assertEqual(normalize_exchange_ui("币安"), "binance")
        self.assertEqual(normalize_exchange_ui("OKEX"), "okx")
        self.assertEqual(normalize_exchange_ui("欧易"), "okx")
        self.assertEqual(normalize_exchange_ui("Bybit"), "bybit")
        self.assertEqual(normalize_exchange_ui("某不知名App 界面"), "other")

        self.assertEqual(infer_exchange_ui_from_description("展示币安广场创作者卡片与收益"), "binance")
        self.assertEqual(infer_exchange_ui_from_description("欧易 App 永续持仓页截图"), "okx")
        self.assertEqual(infer_exchange_ui_from_description("Bybit contract page"), "bybit")
        self.assertEqual(infer_exchange_ui_from_description("TradingView K线图，右下角有 @张三 水印"), "")

        self.assertFalse(has_rival_exchange_ui("binance", ["", "binance"]))
        self.assertTrue(has_rival_exchange_ui("binance", ["binance", "okx"]))
        self.assertTrue(has_rival_exchange_ui("binance", ["bybit"]))
        self.assertFalse(has_rival_exchange_ui("okx", ["", "okx"]))
        self.assertTrue(has_rival_exchange_ui("okx", ["binance"]))
        # X 等非交易所平台不受竞品交易所 UI 限制
        self.assertFalse(has_rival_exchange_ui("x", ["binance", "okx", "bybit"]))

    def test_build_publish_text_list_splits_coin_tags_with_newline(self):
        segments = _build_publish_text_list("$BTC 看多结构明确\n$ETH 同步放量")
        coin_segs = [s for s in segments if s["text"].startswith("$")]
        self.assertEqual([s["text"] for s in coin_segs], ["$BTC", "$ETH"])
        self.assertTrue(all(s["delay"] == 2 for s in coin_segs))
        non_coin = [s for s in segments if not s["text"].startswith("$")]
        self.assertTrue(all(s["text"].startswith("\n") for s in non_coin))

    def test_binance_decorate_analysis(self):
        adapter = BinancePlatform()
        decorated = adapter.decorate_analysis("BTC 突破关键阻力", "BTC")
        self.assertEqual(decorated, "BTC 突破关键阻力\n\n#BTC\n$BTC ")
        self.assertEqual(adapter.decorate_analysis("宏观流动性承压", "MARKET"), "宏观流动性承压")
        self.assertEqual(adapter.decorate_analysis("无明确币种正文", ""), "无明确币种正文")
        self.assertEqual(OkxPlatform().decorate_analysis("宏观流动性承压", "MARKET"), "宏观流动性承压")

    def test_format_analysis_and_viral_post_content_fixes(self):
        an_art = {
            "topic": "ZEC",
            "article_info": {
                "text": "ZEC破800后持续走弱。",
                "image_placeholders": [],
                "image_mapping": {},
            },
        }
        bn_text, _ = _format_analysis_post_content(an_art, platform="binance")
        self.assertTrue(bn_text.startswith("$ZEC 破800"))
        self.assertNotIn("$ZEC ZEC", bn_text)
        self.assertIn("#ZEC\n$ZEC ", bn_text)
        self.assertNotIn("\n\n\n\n\n\n👇", bn_text)
        self.assertIn("\n\n👇", bn_text)

        market_art = {
            "topic": "MARKET",
            "article_info": {
                "text": "宏观流动性收缩压制风险资产。",
                "image_placeholders": [],
                "image_mapping": {},
            },
        }
        market_text, _ = _format_analysis_post_content(market_art, platform="binance")
        self.assertTrue(market_text.startswith("宏观流动性收缩压制风险资产。"))
        self.assertNotIn("$MARKET", market_text)
        self.assertNotIn("#MARKET", market_text)

        okx_text, _ = _format_analysis_post_content(an_art, platform="okx")
        self.assertTrue(okx_text.startswith("$ZEC 破800"))
        self.assertNotIn("$ZEC ZEC", okx_text)

        # Viral: existing $SYMBOL preserves opening hook; missing $SYMBOL appends cleanly; empty coin keeps text clean
        v_with_tag, _ = _format_viral_post_content({"post_text": "凌晨暴跌，我的 $DOGE 多单扛不住了", "selected_images": [], "image_mapping": {}}, "BTC")
        self.assertTrue(v_with_tag.startswith("凌晨暴跌"))
        self.assertNotIn("$BTC", v_with_tag)

        v_no_tag, _ = _format_viral_post_content({"post_text": "三十天账面蒸发四千万", "selected_images": [], "image_mapping": {}}, "BTC")
        self.assertEqual(v_no_tag, "三十天账面蒸发四千万\n\n$BTC")

        v_no_coin, _ = _format_viral_post_content({"post_text": "三十天账面蒸发四千万", "selected_images": [], "image_mapping": {}}, "")
        self.assertEqual(v_no_coin, "三十天账面蒸发四千万")

        # 1-char coin (e.g. $H) must NOT auto-append $H to avoid prefix autocomplete misfire ($HYPE)
        v_one_char, _ = _format_viral_post_content({"post_text": "这个行情真离谱", "selected_images": [], "image_mapping": {}}, "H")
        self.assertEqual(v_one_char, "这个行情真离谱")
        self.assertEqual(BinancePlatform().decorate_analysis("单字母走势", "H"), "单字母走势\n\n#H")
        self.assertIn("BTC", BinancePlatform().hot_coins(for_publish=True))

    def test_stage1_upstream_guardrails_and_health_circuit_breaker(self):
        with tempfile.TemporaryDirectory() as tmp:
            img_bn = _create_tiny_png(Path(tmp), "bn.png")
            img_okx = _create_tiny_png(Path(tmp), "okx.png")
            img_clean = _create_tiny_png(Path(tmp), "clean.png")

            now = time.time()
            evidences = [
                {
                    "source": "binance",
                    "source_post_id": "p_low_weight",
                    "core_fact": "Low weight noise",
                    "logic_link": "Weak",
                    "raw_hook": "",
                    "dimension": "K线形态",
                    "shelf_life": "days",
                    "impact_weight": 1,
                    "target_coin": "BTC",
                    "publish_time": now - 3600,
                    "images": [],
                },
                *[
                    {
                        "source": "binance" if i != 1 else "okx",
                        "source_post_id": f"p_hi_{i}",
                        "core_fact": f"High catalyst {i}",
                        "logic_link": f"Strong logic {i}",
                        "raw_hook": "",
                        "dimension": f"Dim{i}",
                        "shelf_life": "days",
                        "impact_weight": 5,
                        "target_coin": "BTC",
                        "publish_time": now - (i + 1) * 3600,
                        "images": (
                            [{"image_id": "[IMAGE_1]", "usable": True, "exchange_ui": "binance" if i == 0 else "okx", "context": "图"}]
                            if i in (0, 1)
                            else ([{"image_id": "[IMAGE_1]", "usable": True, "exchange_ui": "", "context": "普通图"}] if i == 2 else [])
                        ),
                        "source_image_mapping": {
                            "[IMAGE_1]": {
                                "original_placeholder": f"[插图: https://x/{i}.png]",
                                "original_url": f"https://x/{i}.png",
                                "local_path": img_bn if i == 0 else (img_okx if i == 1 else img_clean),
                            }
                        } if i <= 2 else {},
                    }
                    for i in range(5)
                ],
            ]
            mock_mgr = MagicMock()
            mock_mgr.find_articles.return_value = []
            task = build_article_task("BTC", "看多", evidences, mock_mgr)
            self.assertFalse(task["is_skip"])
            # Low weight=1 noise excluded when >=5 high impact materials exist; metadata preserved
            self.assertEqual(len(task["materials"]), 5)
            self.assertTrue(all(m["impact_weight"] == 5 for m in task["materials"]))
            self.assertTrue(all("age_hours" in m and "dimension" in m for m in task["materials"]))
            # Rival UI mix prevention: cannot upload both img_bn and img_okx together
            self.assertFalse(img_bn in task["file_paths"] and img_okx in task["file_paths"])
            self.assertIn(img_clean, task["file_paths"])

            # Empty comments stripped from viral task brief
            v_post = {
                "source": "binance",
                "post_id": "v_nocmt",
                "content": {"text_content": "爆仓图\n[插图: https://x/clean.png]"},
                "media": {"local_mapping": {"https://x/clean.png": img_clean}},
                "comments": [],
                "viral_screen": {"event_key": "k"},
            }
            v_task = build_viral_article_task(v_post)
            self.assertNotIn("comments", v_task["record"]["creation_brief"]["original_post"])

            # Account health guard blocks lan != cn or >=2/3 penalized
            bad_lan_mgr = MagicMock()
            bad_lan_mgr.find_articles.return_value = [
                {"publish_records": {"binance": {"lan": "en", "recommend": True, "content_status": 2}}}
            ]
            ok_lan, _ = _check_account_health_guard(bad_lan_mgr, "binance", "qiqi")
            self.assertFalse(ok_lan)

            bad_rec_mgr = MagicMock()
            bad_rec_mgr.find_articles.return_value = [
                {"publish_records": {"binance": {"lan": "cn", "recommend": False, "content_status": 2}}},
                {"publish_records": {"binance": {"lan": "cn", "recommend": True, "content_status": 5}}},
                {"publish_records": {"binance": {"lan": "cn", "recommend": True, "content_status": 2}}},
            ]
            ok_rec, _ = _check_account_health_guard(bad_rec_mgr, "binance", "myself")
            self.assertFalse(ok_rec)

            good_mgr = MagicMock()
            good_mgr.find_articles.return_value = [
                {"publish_records": {"binance": {"lan": "cn", "recommend": True, "content_status": 2}}},
                {"publish_records": {"binance": {"lan": "cn", "recommend": True, "content_status": 2}}},
            ]
            ok_good, _ = _check_account_health_guard(good_mgr, "binance", "myself")
            self.assertTrue(ok_good)

    def test_sync_published_metrics_refreshes_until_mature_and_marks_ramping(self):
        now = 1_800_000_000.0
        docs = [
            {
                "article_id": "a_ramp",
                "topic": "BTC",
                "stance": "看多",
                "article_info": {"text": "刚发布不久的新帖"},
                "publish_records": {"binance": {"post_id": "101", "published_by": "myself", "publish_time": now - 1800, "view_count": None}},
            },
            {
                "article_id": "a_refresh",
                "topic": "ETH",
                "stance": "看空",
                "article_info": {"text": "12小时前发的老帖，此前停留在15"},
                "publish_records": {
                    "binance": {
                        "post_id": "102",
                        "published_by": "myself",
                        "publish_time": now - 12 * 3600,
                        "view_count": 15,
                        "metrics_synced_at": now - 3 * 3600,
                    }
                },
            },
            {
                "article_id": "a_settled",
                "topic": "SOL",
                "stance": "看多",
                "article_info": {"text": "72小时前已完成成熟定型同步的帖子"},
                "publish_records": {
                    "binance": {
                        "post_id": "103",
                        "published_by": "myself",
                        "publish_time": now - 72 * 3600,
                        "view_count": 500,
                        "metrics_synced_at": now - 40 * 3600,
                    }
                },
            },
            {
                "article_id": "a_blocked",
                "topic": "BTC",
                "stance": "看空",
                "article_info": {"text": "被风控隐藏 status=5 的帖子"},
                "publish_records": {"binance": {"post_id": "104", "published_by": "myself", "publish_time": now - 3600, "view_count": None}},
            },
        ]
        mock_mgr = MagicMock(spec=["upsert_articles"])

        def _fake_detail(pid):
            if pid == "101":
                return {"contentStatus": 2, "viewCount": 18, "likeCount": 0, "commentCount": 0, "recommend": True, "lan": "cn", "detectedLang": "zh-CN"}
            if pid == "102":
                return {"contentStatus": 2, "viewCount": 680, "likeCount": 4, "commentCount": 2, "recommend": True, "lan": "en", "detectedLang": "zh-CN"}
            if pid == "104":
                return {"contentStatus": 5, "viewCount": None, "recommend": False, "lan": None, "detectedLang": None}
            raise AssertionError(f"settled post {pid} should not be re-fetched")

        with patch("parser.platform_adapters.fetch_binance_post_detail", side_effect=_fake_detail) as mock_fetch:
            hist = sync_published_metrics(mock_mgr, "binance", docs, now_ts=now)

        self.assertEqual(mock_fetch.call_count, 3)
        mock_mgr.upsert_articles.assert_called_once()
        self.assertEqual(hist[0]["views"], "爬坡中(发布<2h)")
        self.assertFalse(hist[0]["is_mature"])
        self.assertEqual(hist[1]["views"], 680)
        self.assertTrue(hist[1]["is_mature"])
        self.assertEqual(hist[2]["views"], 500)
        self.assertEqual(docs[1]["publish_records"]["binance"]["lan"], "en")
        self.assertEqual(docs[1]["publish_records"]["binance"]["recommend"], True)
        self.assertEqual(docs[3]["publish_records"]["binance"]["content_status"], 5)
        self.assertIs(docs[3]["publish_records"]["binance"]["recommend"], False)
        self.assertIsNone(docs[3]["publish_records"]["binance"]["view_count"])


class TestArticleGenerator(unittest.TestCase):
    def test_analysis_and_viral_record_real_model_used(self):
        with tempfile.TemporaryDirectory() as tmp:
            img_path = _create_tiny_png(Path(tmp), "chart.png")

            # Build analysis task with 5 valid evidences (using distinct post IDs + usable image)
            evidences = []
            for i in range(5):
                evidences.append(
                    {
                        "source": "binance",
                        "source_post_id": f"src_post_{i}",
                        "core_fact": f"Fact {i}",
                        "logic_link": f"Logic {i}",
                        "raw_hook": f"Hook {i}",
                        "dimension": f"Dim{i}",
                        "shelf_life": "days",
                        "impact_weight": 5,
                        "target_coin": "BTC",
                        "images": [{"image_id": "[IMAGE_1]", "usable": True, "context": "K线突破图"}] if i == 0 else [],
                        "source_image_mapping": {
                            "[IMAGE_1]": {"original_placeholder": "[插图: https://x/a.png]", "original_url": "https://x/a.png", "local_path": img_path}
                        } if i == 0 else {},
                    }
                )

            mock_mgr = MagicMock()
            mock_mgr.find_articles.return_value = []

            task = build_article_task("BTC", "看多", evidences, mock_mgr)
            self.assertFalse(task["is_skip"])
            self.assertIsNone(task["record"]["model_name"])  # Zero hardcoded model name!

            analysis_resp = {
                "status": "ok",
                "text": "BTC 放量突破前高。",
                "god_comments": ["多头节奏稳了"],
                "image_placeholders": ["[ASSET_IMG_1]"],
                "used_material_ids": ["M1"],
                "score": 8.5,
                "reason": None,
            }
            with patch(
                "parser.article_generator.call_llm_json",
                return_value=(analysis_resp, {"ok": True, "model_used": "real-analysis-model-v1", "schema_attempt": 1, "raw_response": "{}"}),
            ) as mock_llm:
                saved = execute_article_task(task, mock_mgr)

            self.assertEqual(saved["status"], "ok")
            self.assertEqual(saved["model_name"], "real-analysis-model-v1")
            self.assertIsNone(mock_llm.call_args.kwargs["model"])
            self.assertEqual(mock_llm.call_args.kwargs["preset_model_group"], "medium")

            # Viral stage 2 task
            viral_post = {
                "source": "binance",
                "post_id": "vpost_99",
                "content": {"text_content": "爆仓名场面\n[插图: https://x/v.png]"},
                "media": {"local_mapping": {"https://x/v.png": img_path}},
                "comments": [{"content": "太真实了"}],
                "viral_screen": {"event_key": "liquidation_meme"},
            }
            v_task = build_viral_article_task(viral_post)
            self.assertIsNone(v_task["record"]["model_name"])
            viral_resp = {
                "image_semantics": [{"id": "[IMAGE_1]", "semantic_role": "交易爆仓图", "exchange_ui": "", "keep": True}],
                "selected_images": ["[IMAGE_1]"],
                "global_truth": "人性总在极限杠杆后后悔",
                "feasibility_audit": "画面直观，可独立传播",
                "usable": True,
                "topic_tag": "搞钱与盈亏",
                "expression_strategy": "短句反讽",
                "post_text": "开单前觉得自己是天才，强平短信到了才想起敬畏市场。",
                "bait_comments": ["我也是这样亏的"],
                "viral_score": 8.8,
            }
            with patch(
                "parser.article_generator.call_llm_json",
                return_value=(viral_resp, {"ok": True, "model_used": "real-viral-model-v2", "schema_attempt": 1, "raw_response": "{}"}),
            ) as mock_v_llm:
                v_saved = execute_viral_article_task(v_task, mock_mgr)

            self.assertEqual(v_saved["status"], "ok")
            self.assertEqual(v_saved["model_name"], "real-viral-model-v2")
            self.assertEqual(v_saved["article_info"]["image_mapping"]["[IMAGE_1]"]["exchange_ui"], "")
            self.assertIsNone(mock_v_llm.call_args.kwargs["model"])
            self.assertEqual(mock_v_llm.call_args.kwargs["preset_model_group"], "medium")

    def test_exchange_ui_validation_and_propagation(self):
        mlu_bad = {
            "evidences": [
                {
                    "target_coin": "BTC",
                    "stance": "看多",
                    "dimension": "资金流",
                    "core_fact": "鲸鱼提币",
                    "logic_link": "抛压减少",
                    "raw_hook": "",
                    "impact_reason": "直观",
                    "impact_weight": 4,
                    "shelf_life": "days",
                    "images": [
                        {
                            "image_id": "[IMAGE_1]",
                            "image_type": "收益截图",
                            "context": "实盘",
                            "usable": True,
                            "unusable_reason": "",
                        }
                    ],
                }
            ]
        }
        ok_bad, _ = check_format_info(mlu_bad, ["[IMAGE_1]"])
        self.assertFalse(ok_bad)

        mlu_ok = json.loads(json.dumps(mlu_bad))
        mlu_ok["evidences"][0]["images"][0]["exchange_ui"] = "欧易"
        ok_good, err_good = check_format_info(mlu_ok, ["[IMAGE_1]"])
        self.assertTrue(ok_good, err_good)

        with tempfile.TemporaryDirectory() as tmp:
            img_path = _create_tiny_png(Path(tmp), "okx_ui.png")
            ev_list = [
                {
                    **mlu_ok["evidences"][0],
                    "source": "okx",
                    "source_post_id": "p_okx_1",
                    "source_image_mapping": {
                        "[IMAGE_1]": {"original_placeholder": "[插图: https://x/okx.png]", "original_url": "https://x/okx.png", "local_path": img_path}
                    },
                }
            ]
            mats, mapping = transform_mlus(ev_list)
            self.assertEqual(len(mats), 1)
            self.assertEqual(mapping["[ASSET_IMG_1]"]["exchange_ui"], "okx")

            # Viral schema validation & normalization
            v_post = {
                "source": "binance",
                "post_id": "v_ex_1",
                "content": {"text_content": "爆仓图\n[插图: https://x/okx.png]"},
                "media": {"local_mapping": {"https://x/okx.png": img_path}},
                "comments": [],
                "viral_screen": {"event_key": "test_ui"},
            }
            v_task = build_viral_article_task(v_post)
            v_bad = {
                "image_semantics": [{"id": "[IMAGE_1]", "semantic_role": "持仓图", "keep": True}],
                "selected_images": ["[IMAGE_1]"],
                "global_truth": "真实爆仓",
                "feasibility_audit": "ok",
                "usable": True,
                "topic_tag": "搞钱与盈亏",
                "expression_strategy": "保留",
                "post_text": "直接爆了",
                "bait_comments": ["笑死"],
                "viral_score": 8.0,
            }
            v_ok_flag, _ = check_viral_article_info(v_bad, ["[IMAGE_1]"], 260)
            self.assertFalse(v_ok_flag)

            v_bad["image_semantics"][0]["exchange_ui"] = "币安"
            v_ok_flag2, v_err2 = check_viral_article_info(v_bad, ["[IMAGE_1]"], 260)
            self.assertTrue(v_ok_flag2, v_err2)
            converted = _convert_viral_article_info(v_bad, v_task)
            self.assertEqual(converted["image_semantics"][0]["exchange_ui"], "binance")
            self.assertEqual(converted["image_mapping"]["[IMAGE_1]"]["exchange_ui"], "binance")

    def test_generation_polling_intervals_split_analysis_and_viral(self):
        import parser.config as pcfg

        self.assertEqual(pcfg.ANALYSIS_GENERATION_INTERVAL_SECONDS, 3 * 3600)
        self.assertEqual(pcfg.ARTICLE_GENERATION_INTERVAL_SECONDS, 1200)

        captured_sleeps = []

        def _run_once(step_fn, **_kwargs):
            captured_sleeps.append(step_fn(1)["next_sleep_seconds"])

        with patch("parser.article_generator.run_daemon_loop", side_effect=_run_once):
            with patch("parser.article_generator.generate_analysis_articles_once", return_value=[{"status": "ok"}]):
                generate_analysis_articles(max_rounds=1)
            with patch("parser.article_generator.generate_analysis_articles_once", return_value=[{"status": "skip"}]):
                generate_analysis_articles(max_rounds=1)
            with patch("parser.article_generator.generate_analysis_articles_once", return_value=[]):
                generate_analysis_articles(max_rounds=1)
            with patch("parser.article_generator.generate_viral_articles_once", return_value=[{"status": "ok"}]):
                generate_viral_articles(max_rounds=1)

        self.assertEqual(captured_sleeps, [3 * 3600, 60, 60, 1200])


class TestPublisherArena(unittest.TestCase):
    def test_early_exit_when_zero_accounts_eligible_skips_db_and_llm(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = AccountPublishStateStore(str(Path(tmp) / "state.json"))
            now = time.time()
            # Force all configured binance accounts into normal 3h account-level cooldown
            with patch("parser.publisher_arena.PLATFORM_ACCOUNTS", {"binance": ["myself", "ruru"]}):
                store.record_success("binance", "myself", is_viral=False, now=now)
                store.record_success("binance", "ruru", is_viral=False, now=now)
                mock_mgr = MagicMock()
                with patch("parser.publisher_arena.call_llm_json") as mock_llm:
                    _publish_articles_for_platform(mock_mgr, "binance", ["BTC"], ARTICLE_TYPE_ANALYSIS, state_store=store)
                    # Analysis cooldown also blocks viral on the same accounts
                    _publish_articles_for_platform(mock_mgr, "binance", ["BTC"], ARTICLE_TYPE_VIRAL, state_store=store)

                mock_mgr.find_articles.assert_not_called()
                mock_llm.assert_not_called()

    def test_rival_image_ui_cross_platform_routing_and_accurate_viral_tags(self):
        with tempfile.TemporaryDirectory() as tmp:
            img_okx = _create_tiny_png(Path(tmp), "ui_okx.png")
            img_bn = _create_tiny_png(Path(tmp), "ui_bn.png")
            img_clean = _create_tiny_png(Path(tmp), "ui_clean.png")
            img_legacy = _create_tiny_png(Path(tmp), "ui_legacy.png")

            def _make_viral(
                aid,
                img_path,
                ui,
                post_text,
                orig_text="原帖提到 $JCT",
                source="okx",
                include_ui_field=True,
                semantic_role="截图",
            ):
                meta = {"local_path": img_path, "original_url": f"https://img/{aid}.png", "content_id": f"cid_{aid}"}
                sem = {"id": "[IMAGE_1]", "semantic_role": semantic_role, "keep": True}
                if include_ui_field:
                    meta["exchange_ui"] = ui
                    sem["exchange_ui"] = ui
                return {
                    "article_id": aid,
                    "source": source,
                    "type": ARTICLE_TYPE_VIRAL,
                    "prompt_version": prompt_version,
                    "status": "ok",
                    "creation_brief": {"original_post": {"text_content": orig_text}},
                    "post_id_list": [aid],
                    "article_info": {
                        "status": "ok",
                        "usable": True,
                        "post_text": post_text,
                        "selected_images": ["[IMAGE_1]"],
                        "image_semantics": [sem],
                        "viral_score": 8.5,
                        "bait_comments": ["笑死"],
                        "image_mapping": {"[IMAGE_1]": meta},
                    },
                    "publish_records": {},
                }

            bn_pool = [
                _make_viral("v_okx_ui", img_okx, "okx", "麻吉大哥昨晚又加仓爆掉 2000 万刀", source="x"),
                _make_viral(
                    "v_role_okx",
                    img_okx,
                    "",
                    "山顶套牢图",
                    source="okx",
                    semantic_role="欧易 App 永续持仓页截图",
                ),
                _make_viral("v_same_bn", img_clean, "", "币安站内原帖", source="binance"),
                _make_viral("v_bn_ui", img_bn, "binance", "SOL 跌破前低引发连环止损", source="okx"),
                _make_viral("v_clean", img_clean, "", "昨晚全网吃瓜现场", source="okx"),
                _make_viral("v_legacy", img_legacy, "", "$ETH 资金费率转负了", source="okx", include_ui_field=False),
            ]
            mock_mgr = MagicMock()
            mock_mgr.find_articles.return_value = bn_pool
            mock_mgr.db = MagicMock()

            bn_cands, _, bn_stats = _collect_publish_candidates(mock_mgr, "binance", ["JCT"], set(), ARTICLE_TYPE_VIRAL)
            self.assertEqual(bn_stats["rival_image_ui"], 2)
            self.assertEqual(bn_stats["source_conflict"], 1)
            bn_by_id = {c[0]["article_id"]: c[2] for c in bn_cands}
            self.assertEqual(set(bn_by_id.keys()), {"v_bn_ui", "v_clean", "v_legacy"})
            # Original noisy JCT does NOT pollute clean post_text; SOL & ETH extracted cleanly; pure story keeps ""
            self.assertEqual(bn_by_id["v_bn_ui"], "SOL")
            self.assertEqual(bn_by_id["v_clean"], "")
            self.assertEqual(bn_by_id["v_legacy"], "ETH")

            okx_pool = [
                _make_viral("v_okx_ui", img_okx, "okx", "麻吉大哥昨晚又加仓爆掉 2000 万刀", source="binance"),
                _make_viral("v_bn_ui", img_bn, "binance", "SOL 跌破前低引发连环止损", source="binance"),
                _make_viral(
                    "v_role_bn",
                    img_bn,
                    "",
                    "行情异动图",
                    source="binance",
                    semantic_role="币安广场创作者带单主页截图",
                ),
                _make_viral("v_same_okx", img_clean, "", "欧易站内原帖", source="okx"),
                _make_viral("v_clean", img_clean, "", "昨晚全网吃瓜现场", source="binance"),
                _make_viral("v_legacy", img_legacy, "", "$ETH 资金费率转负了", source="binance", include_ui_field=False),
            ]
            mock_mgr.find_articles.return_value = okx_pool
            okx_cands, _, okx_stats = _collect_publish_candidates(mock_mgr, "okx", ["JCT"], set(), ARTICLE_TYPE_VIRAL)
            self.assertEqual(okx_stats["rival_image_ui"], 2)
            self.assertEqual(okx_stats["source_conflict"], 1)
            okx_ids = {c[0]["article_id"] for c in okx_cands}
            self.assertEqual(okx_ids, {"v_okx_ui", "v_clean", "v_legacy"})

    def test_publish_articles_for_platform_skips_claimed_and_releases_on_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = AccountPublishStateStore(str(Path(tmp) / "claim_state.json"))
            img_p = _create_tiny_png(Path(tmp), "claim.png")
            art = {
                "_id": "mongo_1",
                "article_id": "art_claim",
                "source": "okx",
                "type": ARTICLE_TYPE_VIRAL,
                "prompt_version": prompt_version,
                "status": "ok",
                "creation_brief": {"original_post": {"text_content": "BTC 测试"}},
                "post_id_list": ["p_claim"],
                "article_info": {
                    "status": "ok",
                    "usable": True,
                    "post_text": "$BTC 波动剧烈",
                    "selected_images": ["[IMAGE_1]"],
                    "image_semantics": [{"id": "[IMAGE_1]", "semantic_role": "K线", "exchange_ui": "", "keep": True}],
                    "viral_score": 8.5,
                    "bait_comments": ["稳"],
                    "image_mapping": {"[IMAGE_1]": {"local_path": img_p, "original_url": "https://x/c.png", "content_id": "cid_c", "exchange_ui": ""}},
                },
                "publish_records": {},
            }
            mock_mgr = MagicMock()
            # Calls: 1) recent published image usage; 2) candidates; 3) health guard (ruru); 4) chief editor recent history
            mock_mgr.find_articles.side_effect = [[], [art], [], []]
            mock_mgr.collection_name = "generated_articles"

            # Pre-claim "myself" as if another thread is publishing it concurrently
            self.assertTrue(store.try_claim_account("binance", "myself", is_viral=False)[0])
            try:
                with patch("parser.publisher_arena.PLATFORM_ACCOUNTS", {"binance": ["myself", "ruru"]}), \
                     patch("parser.publisher_arena.call_llm_json", return_value=({"selected_candidate_no": 1, "reason": "ok"}, {"ok": True, "model_used": "m1"})), \
                     patch("parser.publisher_arena._execute_platform_post", side_effect=RuntimeError("boom")) as mock_exec:
                    with self.assertRaises(RuntimeError):
                        _publish_articles_for_platform(mock_mgr, "binance", ["BTC"], ARTICLE_TYPE_VIRAL, state_store=store)
                    # Only "ruru" should have been attempted because "myself" was claimed
                    self.assertEqual(mock_exec.call_count, 1)
                # "ruru" must be released even after exception in finally block
                self.assertTrue(store.can_publish("binance", "ruru", is_viral=True)[0])
            finally:
                store.release_account("binance", "myself")

    def test_okx_viral_skips_okx_sourced_articles(self):
        with tempfile.TemporaryDirectory() as tmp:
            img_b = _create_tiny_png(Path(tmp), "bin.png")
            img_o = _create_tiny_png(Path(tmp), "okx.png")
            img_u = _create_tiny_png(Path(tmp), "unk.png")
            art_binance = {
                "source": "binance",
                "type": ARTICLE_TYPE_VIRAL,
                "prompt_version": prompt_version,
                "status": "ok",
                "creation_brief": {"original_post": {"text_content": "BTC 暴走"}},
                "post_id_list": ["b1"],
                "article_info": {
                    "status": "ok",
                    "usable": True,
                    "post_text": "BTC 今日看点",
                    "selected_images": ["[IMAGE_1]"],
                    "viral_score": 8.2,
                    "bait_comments": ["笑死"],
                    "image_mapping": {"[IMAGE_1]": {"local_path": img_b, "original_url": "https://b/1.png", "content_id": "cid_b"}},
                },
                "publish_records": {},
            }
            art_okx = {
                "source": "okx",
                "type": ARTICLE_TYPE_VIRAL,
                "prompt_version": prompt_version,
                "status": "ok",
                "creation_brief": {"original_post": {"text_content": "BTC 测试"}},
                "post_id_list": ["o1"],
                "article_info": {
                    "status": "ok",
                    "usable": True,
                    "post_text": "OKX 原创帖",
                    "selected_images": ["[IMAGE_1]"],
                    "viral_score": 9.5,
                    "bait_comments": ["冲"],
                    "image_mapping": {"[IMAGE_1]": {"local_path": img_o, "original_url": "https://o/1.png", "content_id": "cid_o"}},
                },
                "publish_records": {},
            }
            art_unknown_src = {
                "source": "",
                "type": ARTICLE_TYPE_VIRAL,
                "prompt_version": prompt_version,
                "status": "ok",
                "creation_brief": {"original_post": {"text_content": "未知来源"}},
                "post_id_list": ["u1"],
                "article_info": {
                    "status": "ok",
                    "usable": True,
                    "post_text": "来源缺失测试帖",
                    "selected_images": ["[IMAGE_1]"],
                    "viral_score": 9.0,
                    "bait_comments": ["啊"],
                    "image_mapping": {"[IMAGE_1]": {"local_path": img_u, "original_url": "https://u/1.png", "content_id": "cid_u"}},
                },
                "publish_records": {},
            }
            mock_mgr = MagicMock()
            mock_mgr.find_articles.return_value = [art_okx, art_binance, art_unknown_src]
            mock_mgr.db = MagicMock()

            okx_cands, okx_raw, okx_stats = _collect_publish_candidates(
                mock_mgr,
                "okx",
                ["BTC"],
                set(),
                ARTICLE_TYPE_VIRAL,
            )
            self.assertEqual(okx_raw, 3)
            self.assertEqual(okx_stats["source_conflict"], 2)
            self.assertEqual(len(okx_cands), 1)
            self.assertEqual(okx_cands[0][0]["source"], "binance")

            # Binance 仅发异源流量帖（跳过 binance 与空来源），保留 okx 来源
            art_okx_clean = {
                **art_okx,
                "article_info": {**art_okx["article_info"], "post_text": "大盘突现异常波动"},
            }
            mock_mgr.find_articles.return_value = [art_okx_clean, art_binance, art_unknown_src]
            bn_cands, bn_raw, bn_stats = _collect_publish_candidates(
                mock_mgr,
                "binance",
                ["BTC"],
                set(),
                ARTICLE_TYPE_VIRAL,
            )
            self.assertEqual(bn_raw, 3)
            self.assertEqual(bn_stats["source_conflict"], 2)
            self.assertEqual(len(bn_cands), 1)
            self.assertEqual(bn_cands[0][0]["source"], "okx")

            # 多源合成分析帖不受 source_conflict 拦截
            art_analysis = {
                "source": "binance",
                "type": ARTICLE_TYPE_ANALYSIS,
                "topic": "BTC",
                "prompt_version": prompt_version,
                "status": "ok",
                "article_info": {
                    "status": "ok",
                    "text": "$BTC 多维分析正文",
                    "image_placeholders": ["[ASSET_IMG_1]"],
                    "score": 8.8,
                    "god_comments": ["稳健"],
                    "image_mapping": {
                        "[ASSET_IMG_1]": {
                            "local_path": img_b,
                            "original_url": "https://b/a1.png",
                            "content_id": "cid_a1",
                            "exchange_ui": "binance",
                        }
                    },
                },
                "publish_records": {},
            }
            mock_mgr.find_articles.return_value = [art_analysis]
            an_cands, _, an_stats = _collect_publish_candidates(
                mock_mgr,
                "binance",
                ["BTC"],
                set(),
                ARTICLE_TYPE_ANALYSIS,
            )
            self.assertEqual(an_stats["source_conflict"], 0)
            self.assertEqual(len(an_cands), 1)

    def test_ranking_favors_multi_img_and_no_18h_penalty(self):
        with tempfile.TemporaryDirectory() as tmp:
            img1 = _create_tiny_png(Path(tmp), "r1.png")
            img2 = _create_tiny_png(Path(tmp), "r2.png")
            now = time.time()
            # Short 1-img viral (high base score 9.2, young post) vs 2-img narrative viral (lower base score 8.5, >20h old)
            art_short_1img = {
                "article_id": "v_short1",
                "source": "binance",
                "type": ARTICLE_TYPE_VIRAL,
                "prompt_version": prompt_version,
                "status": "ok",
                "post_id_list": ["p_short"],
                "article_info": {
                    "status": "ok",
                    "usable": True,
                    "post_text": "就离谱，刚买就跌了三十个点。",
                    "selected_images": ["[IMAGE_1]"],
                    "viral_score": 9.2,
                    "bait_comments": ["笑死"],
                    "image_mapping": {"[IMAGE_1]": {"local_path": img1, "original_url": "https://r/1.png", "content_id": "c1", "exchange_ui": ""}},
                },
                "publish_records": {},
            }
            art_multi_2img = {
                "article_id": "v_multi2",
                "source": "binance",
                "type": ARTICLE_TYPE_VIRAL,
                "prompt_version": prompt_version,
                "status": "ok",
                "post_id_list": ["p_multi"],
                "article_info": {
                    "status": "ok",
                    "usable": True,
                    "post_text": "某项目创始人嘴上喊着生态建设，链上大额地址却连夜解锁转入交易所，前后对比实在太讽刺了，两张实锤图自己看细节。",
                    "selected_images": ["[IMAGE_1]", "[IMAGE_2]"],
                    "viral_score": 8.5,
                    "bait_comments": ["太真实"],
                    "image_mapping": {
                        "[IMAGE_1]": {"local_path": img1, "original_url": "https://r/1.png", "content_id": "c1", "exchange_ui": ""},
                        "[IMAGE_2]": {"local_path": img2, "original_url": "https://r/2.png", "content_id": "c2", "exchange_ui": ""},
                    },
                },
                "publish_records": {},
            }
            mock_mgr = MagicMock()
            mock_mgr.db = MagicMock()
            mock_post_col = MagicMock()
            mock_post_col.find.return_value = [
                {"post_id": "p_short", "publish_time": now - 3600, "engagement": {"view_count": 5000}},
                {"post_id": "p_multi", "publish_time": now - 22 * 3600, "engagement": {"view_count": 5000}},
            ]
            mock_mgr.db.get_collection.return_value = mock_post_col

            with patch("parser.publisher_arena.UniversalPostManager") as mock_upm:
                mock_upm.return_value.find_posts.return_value = [
                    {"post_id": "p_short", "publish_time": now - 3600, "engagement": {"view_count": 5000}},
                    {"post_id": "p_multi", "publish_time": now - 22 * 3600, "engagement": {"view_count": 5000}},
                ]
                mock_mgr.find_articles.return_value = [
                    {**art_short_1img, "source": "okx"},
                    {**art_multi_2img, "source": "okx"},
                ]
                cands_bn, _, _ = _collect_publish_candidates(mock_mgr, "binance", ["BTC"], set(), ARTICLE_TYPE_VIRAL)
                mock_mgr.find_articles.return_value = [art_short_1img, art_multi_2img]
                cands_okx, _, _ = _collect_publish_candidates(mock_mgr, "okx", ["BTC"], set(), ARTICLE_TYPE_VIRAL)
            self.assertEqual([c[0]["article_id"] for c in cands_bn], ["v_multi2", "v_short1"])
            # OKX keeps neutral 0.0 platform bias -> raw viral_score 9.2 stays ahead of 8.5 (decoupled from Binance)
            self.assertEqual([c[0]["article_id"] for c in cands_okx], ["v_short1", "v_multi2"])

            # Verify exact Binance vs OKX form scoring curve across lengths & image counts
            self.assertAlmostEqual(score_platform_candidate_form("binance", ARTICLE_TYPE_VIRAL, 2, 40), 1.0)
            self.assertAlmostEqual(score_platform_candidate_form("binance", ARTICLE_TYPE_VIRAL, 2, 200), 1.0)
            self.assertAlmostEqual(score_platform_candidate_form("binance", ARTICLE_TYPE_VIRAL, 1, 35), 0.0)
            self.assertAlmostEqual(score_platform_candidate_form("binance", ARTICLE_TYPE_VIRAL, 1, 75), -0.5)
            self.assertAlmostEqual(score_platform_candidate_form("binance", ARTICLE_TYPE_VIRAL, 1, 160), 0.0)
            self.assertAlmostEqual(score_platform_candidate_form("binance", ARTICLE_TYPE_VIRAL, 1, 280), -0.5)
            self.assertAlmostEqual(score_platform_candidate_form("binance", ARTICLE_TYPE_VIRAL, 2, 280), 0.5)
            self.assertAlmostEqual(score_platform_candidate_form("binance", ARTICLE_TYPE_ANALYSIS, 2, 180), 1.1)
            self.assertAlmostEqual(score_platform_candidate_form("binance", ARTICLE_TYPE_ANALYSIS, 2, 100), 0.8)
            self.assertAlmostEqual(score_platform_candidate_form("binance", ARTICLE_TYPE_ANALYSIS, 1, 180), 0.3)
            self.assertAlmostEqual(score_platform_candidate_form("binance", ARTICLE_TYPE_ANALYSIS, 1, 280), -0.5)
            self.assertAlmostEqual(score_platform_candidate_form("okx", ARTICLE_TYPE_VIRAL, 2, 40), 0.0)
            self.assertAlmostEqual(score_platform_candidate_form("okx", ARTICLE_TYPE_VIRAL, 1, 75), 0.0)
            self.assertAlmostEqual(score_platform_candidate_form("okx", ARTICLE_TYPE_ANALYSIS, 2, 180), 0.0)

    def test_generation_viral_selection_is_platform_agnostic_and_preserves_short_gems(self):
        with tempfile.TemporaryDirectory() as tmp:
            img1 = _create_tiny_png(Path(tmp), "g1.png")
            img2 = _create_tiny_png(Path(tmp), "g2.png")
            now = time.time()
            # Higher-engagement 1-image short gem MUST beat lower-engagement 2-image post in generation selection (no 1.35x Binance img_boost)
            p_short_1img = {
                "source": "binance",
                "post_id": "gen_short_top",
                "publish_time": now - 600,
                "author": {"username": "trader_a"},
                "viral_screen": {"prompt_version": prompt_version, "keep": True, "event_key": ""},
                "engagement": {"view_count": 20000, "like_count": 50, "comment_count": 20},
                "content": {"text_content": "一句封神短梗\n[插图: https://x/1.png]"},
                "media": {"images": ["https://x/1.png"], "local_mapping": {"https://x/1.png": img1}},
            }
            p_multi_2img = {
                "source": "binance",
                "post_id": "gen_multi_mid",
                "publish_time": now - 300,
                "author": {"username": "trader_b"},
                "viral_screen": {"prompt_version": prompt_version, "keep": True, "event_key": ""},
                "engagement": {"view_count": 17000, "like_count": 40, "comment_count": 15},
                "content": {"text_content": "两图故事\n[插图: https://x/1.png][插图: https://x/2.png]"},
                "media": {
                    "images": ["https://x/1.png", "https://x/2.png"],
                    "local_mapping": {"https://x/1.png": img1, "https://x/2.png": img2},
                },
            }
            mock_art_mgr = MagicMock()
            mock_art_mgr.find_articles.return_value = []
            with patch("parser.article_generator.UniversalPostManager") as mock_upm, \
                 patch("parser.article_generator.gen_db_object"), \
                 patch("parser.article_generator._get_all_self_published_post_ids", return_value=set()):
                mock_upm.return_value.find_posts_by_source.side_effect = (
                    lambda src, **kw: [p_multi_2img, p_short_1img] if src == "binance" else []
                )
                selected = extract_valid_viral_posts(mock_art_mgr)
            self.assertEqual([p["post_id"] for p in selected], ["gen_short_top", "gen_multi_mid"])

    def test_chief_editor_strips_scores_and_never_rewrites_draft(self):
        with tempfile.TemporaryDirectory() as tmp:
            img_p = _create_tiny_png(Path(tmp), "editor.png")
            art = {
                "article_id": "art_1",
                "type": ARTICLE_TYPE_ANALYSIS,
                "topic": "BTC",
                "stance": "看多",
                "creation_brief": {"materials": [{"id": "M1", "fact": "链上净流出", "visual_evidence": {"placeholder": "[ASSET_IMG_1]", "what_it_shows": "流出柱状图"}}]},
                "article_info": {
                    "status": "ok",
                    "text": "原始干净正文",
                    "god_comments": ["稳健观点"],
                    "image_placeholders": ["[ASSET_IMG_1]"],
                    "used_material_ids": ["M1"],
                    "score": 9.9,
                    "image_mapping": {"[ASSET_IMG_1]": {"local_path": img_p}},
                },
            }
            mock_mgr = MagicMock()
            mock_mgr.find_articles.return_value = []
            mock_mgr.collection_name = "generated_articles"
            mock_col = MagicMock()
            mock_mgr.db.get_collection.return_value = mock_col

            captured_prompt = {}

            def _fake_editor(**kwargs):
                captured_prompt["text"] = kwargs["prompt"]
                # Even if model adds extra fields, pick-only editor MUST NOT overwrite draft_text or call DB update_one
                return (
                    {
                        "selected_candidate_no": 1,
                        "final_text": "擅自篡改正文",
                        "final_comments": ["篡改神评"],
                        "reason": "test",
                    },
                    {"ok": True, "model_used": "editor-model-v1"},
                )

            with patch("parser.publisher_arena.call_llm_json", side_effect=_fake_editor):
                idx = _select_and_polish_best_candidate(
                    mock_mgr,
                    "binance",
                    "myself",
                    {"topic_publish_history": {}},
                    [(art, {"cid_1"}, "BTC")],
                    time.time(),
                    set(),
                    ARTICLE_TYPE_ANALYSIS,
                )

            self.assertEqual(idx, 0)
            self.assertNotIn("9.9", captured_prompt["text"])
            self.assertIn("platform_fit_score", captured_prompt["text"])
            self.assertEqual(art["article_info"]["text"], "原始干净正文")
            self.assertEqual(art["article_info"]["god_comments"], ["稳健观点"])
            mock_col.update_one.assert_not_called()
            self.assertEqual(art["_selection_meta"]["model_used"], "editor-model-v1")

    def test_chief_editor_isolates_article_type_and_uses_minimal_recent_topics(self):
        with tempfile.TemporaryDirectory() as tmp:
            img_p = _create_tiny_png(Path(tmp), "editor2.png")
            art = {
                "article_id": "art_2",
                "type": ARTICLE_TYPE_VIRAL,
                "topic": "搞钱与盈亏",
                "creation_brief": {},
                "article_info": {
                    "status": "ok",
                    "usable": True,
                    "post_text": "原始干净吃瓜正文",
                    "bait_comments": ["笑死"],
                    "selected_images": ["[IMAGE_1]"],
                    "image_semantics": [{"id": "[IMAGE_1]", "semantic_role": "收益截图", "keep": True}],
                    "viral_score": 8.5,
                    "image_mapping": {"[IMAGE_1]": {"local_path": img_p}},
                },
            }
            now = time.time()
            hist_docs = [
                {
                    "article_id": "r1",
                    "type": ARTICLE_TYPE_VIRAL,
                    "topic": "ETH",
                    "article_info": {"post_text": "最近发过ETH巨鲸爆仓吃瓜帖"},
                    "publish_records": {"binance": {"published_by": "ruru", "publish_time": now - 10 * 3600, "view_count": 1200, "metrics_synced_at": now - 100}},
                }
            ]
            mock_mgr = MagicMock()
            mock_mgr.find_articles.return_value = hist_docs
            mock_mgr.collection_name = "generated_articles"
            captured_payload = {}

            def _fake_editor(**kwargs):
                captured_payload["prompt"] = kwargs["prompt"]
                return (
                    {"selected_candidate_no": 1, "reason": "ok"},
                    {"ok": True, "model_used": "editor-model-v2"},
                )

            with patch("parser.publisher_arena.call_llm_json", side_effect=_fake_editor):
                _select_and_polish_best_candidate(
                    mock_mgr,
                    "binance",
                    "ruru",
                    {"topic_publish_history": {}},
                    [(art, {"cid_2"}, "BTC")],
                    now,
                    set(),
                    ARTICLE_TYPE_VIRAL,
                )

            q_used = mock_mgr.find_articles.call_args.kwargs["query"]
            self.assertEqual(q_used.get("type"), ARTICLE_TYPE_VIRAL)
            self.assertIn("recent_topics", captured_payload["prompt"])
            self.assertNotIn("account_view_baselines", captured_payload["prompt"])
            self.assertIn("最近发过ETH巨鲸爆仓吃瓜帖", captured_payload["prompt"])

    def test_waf_challenge_backs_off_account_without_consuming_article_attempts(self):
        from platforms.playwright_base import ChallengeRequiredError

        with tempfile.TemporaryDirectory() as tmp:
            store = AccountPublishStateStore(str(Path(tmp) / "waf_state.json"))
            img_p = _create_tiny_png(Path(tmp), "waf.png")
            art = {
                "_id": "mongo_waf",
                "article_id": "art_waf",
                "source": "okx",
                "type": ARTICLE_TYPE_VIRAL,
                "prompt_version": prompt_version,
                "status": "ok",
                "creation_brief": {"original_post": {"text_content": "BTC 测试"}},
                "post_id_list": ["p_waf"],
                "article_info": {
                    "status": "ok",
                    "usable": True,
                    "post_text": "$BTC 高杠杆爆仓反差吃瓜",
                    "selected_images": ["[IMAGE_1]"],
                    "image_semantics": [{"id": "[IMAGE_1]", "semantic_role": "截图", "exchange_ui": "", "keep": True}],
                    "viral_score": 8.6,
                    "bait_comments": ["笑死"],
                    "image_mapping": {"[IMAGE_1]": {"local_path": img_p, "original_url": "https://x/w.png", "content_id": "cid_waf", "exchange_ui": ""}},
                },
                "publish_records": {},
            }
            mock_mgr = MagicMock()
            mock_mgr.find_articles.side_effect = [[], [art], [], []]
            mock_mgr.collection_name = "generated_articles"
            mock_col = MagicMock()
            mock_mgr.db.get_collection.return_value = mock_col

            self.assertEqual(classify_infra_backoff_error("[WAF/人机验证] 广场页面返回 HTTP 405"), "waf_challenge")
            self.assertIsNone(classify_infra_backoff_error("timeout waiting for editor"))

            with patch("parser.publisher_arena.PLATFORM_ACCOUNTS", {"binance": ["myself"]}), \
                 patch("parser.publisher_arena.call_llm_json", return_value=({"selected_candidate_no": 1, "reason": "ok"}, {"ok": True, "model_used": "m1"})), \
                 patch("parser.publisher_arena._execute_platform_post", side_effect=ChallengeRequiredError("HTTP 405")):
                _publish_articles_for_platform(mock_mgr, "binance", ["BTC"], ARTICLE_TYPE_VIRAL, state_store=store)

            # Account is now in WAF backoff; DB publish_records must NOT be marked failed or increment attempts
            ok_next, reason_next = store.can_publish("binance", "myself", is_viral=False)
            self.assertFalse(ok_next)
            self.assertIn("waf_challenge", reason_next)
            mock_col.update_one.assert_not_called()
            self.assertEqual(art["publish_records"], {})


class TestLazyMediaDownload(unittest.TestCase):
    def test_is_valid_raw_post_require_local_media_modes(self):
        now = time.time()
        post = {
            "source": "binance",
            "post_id": "lazy_1",
            "publish_time": now - 60,
            "author": {"username": "someone_else"},
            "content": {"text_content": "BTC 分析\n[插图: https://example.com/a.png]"},
            "media": {"images": ["https://example.com/a.png"], "local_mapping": {}},
        }
        self.assertTrue(_is_valid_raw_post(post, require_local_media=False))
        self.assertFalse(_is_valid_raw_post(post))

        video_post = {
            **post,
            "content": {"text_content": "BTC 视频\n[视频: https://example.com/v.mp4]"},
        }
        self.assertFalse(_is_valid_raw_post(video_post, require_local_media=False))

    def test_ensure_post_media_downloaded_downloads_once_and_caches_failures(self):
        now = time.time()
        with tempfile.TemporaryDirectory() as tmp:
            img_path = _create_tiny_png(Path(tmp), "dl.png")
            post = {
                "source": "binance",
                "post_id": "lazy_dl_1",
                "publish_time": now - 60,
                "author": {"username": "someone_else"},
                "content": {"text_content": "图表信号\n[插图: https://example.com/ok_unique_1.png]"},
                "media": {"images": ["https://example.com/ok_unique_1.png"], "local_mapping": {}},
            }
            with patch("parser.post_ingestion.download_web_media", return_value=img_path) as mock_dl:
                self.assertTrue(ensure_post_media_downloaded(post))
                self.assertEqual(mock_dl.call_count, 1)
                self.assertEqual(post["media"]["local_mapping"]["https://example.com/ok_unique_1.png"], img_path)
                # Second call reuses local verified mapping with zero downloads
                self.assertTrue(ensure_post_media_downloaded(post))
                self.assertEqual(mock_dl.call_count, 1)

            dead_url = "https://example.com/dead_unique_test_404.png"
            bad_post = {
                "source": "okx",
                "post_id": "lazy_dl_bad",
                "publish_time": now - 60,
                "author": {"author_id": "99999"},
                "content": {"text_content": f"缺图帖\n[插图: {dead_url}]"},
                "media": {"images": [dead_url], "local_mapping": {}},
            }
            with patch("parser.post_ingestion.download_web_media", return_value=None) as mock_fail:
                self.assertFalse(ensure_post_media_downloaded(bad_post))
                self.assertFalse(ensure_post_media_downloaded(bad_post))
                self.assertEqual(mock_fail.call_count, 1)

    def test_fetch_hot_posts_disables_eager_media_download(self):
        calls = []

        def _fake_fetch_feed(**kwargs):
            calls.append(dict(kwargs))
            return []

        mock_mgr = MagicMock()
        _fetch_hot_posts(mock_mgr, ["BTC"], _fake_fetch_feed, "binance")
        self.assertEqual(len(calls), 3)
        self.assertTrue(all(c.get("download_media") is False for c in calls))

    def test_single_prompt_version_invalidates_all_stages_cleanly(self):
        with tempfile.TemporaryDirectory() as tmp:
            img1 = _create_tiny_png(Path(tmp), "single_ver.png")
            now = time.time()
            old_ver = f"{prompt_version}-legacy"
            # 1. Old-version logic_mul/pre_screen MUST trigger re-screening and re-formatting (no deadlock)
            post_old_all = {
                "source": "binance",
                "post_id": "single_old_1",
                "publish_time": now - 3600,
                "pre_screen": {"prompt_version": old_ver, "no_evidence": False},
                "logic_mul": {
                    "prompt_version": old_ver,
                    "evidences": [
                        {
                            "target_coin": "BTC",
                            "stance": "看多",
                            "dimension": "资金流",
                            "core_fact": "大户净流入",
                            "logic_link": "买盘托底",
                            "raw_hook": "",
                            "impact_weight": 5,
                            "shelf_life": "days",
                            "images": [{"image_id": "[IMAGE_1]", "usable": True, "context": "资金图"}],
                        }
                    ],
                },
                "content": {"text_content": "BTC资金流强势\n[插图: https://x/mlu.png]"},
                "media": {"images": ["https://x/mlu.png"], "local_mapping": {"https://x/mlu.png": img1}},
            }
            self.assertTrue(is_need_pre_screening(post_old_all))
            self.assertFalse(is_need_formatting(post_old_all))

            # Once pre_screen updates to prompt_version, old logic_mul STILL gets re-extracted cleanly
            post_old_mlu_only = dict(post_old_all, pre_screen={"prompt_version": prompt_version, "no_evidence": False})
            self.assertFalse(is_need_pre_screening(post_old_mlu_only))
            self.assertTrue(is_need_formatting(post_old_mlu_only))

            post_cur = dict(
                post_old_mlu_only,
                post_id="single_cur_1",
                logic_mul={**post_old_all["logic_mul"], "prompt_version": prompt_version},
            )
            self.assertFalse(is_need_pre_screening(post_cur))
            self.assertFalse(is_need_formatting(post_cur))

            # 2. Upstream extractor filter rejects old-ver evidences and admits prompt_version evidences
            mock_art_mgr = MagicMock()
            mock_art_mgr.find_articles.return_value = []
            with patch("parser.article_generator.get_hot_coin", return_value=["BTC"]), \
                 patch("parser.article_generator.PLATFORMS", {"okx": MagicMock(hot_coins=MagicMock(return_value=[]))}), \
                 patch("parser.article_generator.UniversalPostManager") as mock_upm, \
                 patch("parser.article_generator.gen_db_object"), \
                 patch("parser.article_generator._get_all_self_published_post_ids", return_value=set()):
                mock_upm.return_value.find_posts_by_source.side_effect = (
                    lambda src, **kw: [post_old_all, post_cur] if src == "binance" else []
                )
                grouped = extract_and_group_valid_evidences(mock_art_mgr)
            self.assertEqual([e["source_post_id"] for e in grouped["BTC"]["看多"]], ["single_cur_1"])

            # 3. get_remodeled_viral_post_ids filters by current prompt_version so old generations do not block regeneration
            get_remodeled_viral_post_ids(mock_art_mgr, "binance", ["v1"])
            _, kwargs = mock_art_mgr.find_articles.call_args
            self.assertEqual(kwargs["query"]["prompt_version"], prompt_version)

    def test_platform_candidate_scoring_ssot_separates_binance_and_okx(self):
        clean_2p_ana = "第一段交待核心矛盾，资金已经连买三天，现价压在关键支撑与筹码密集区附近，短期抛压正在出清。\n\n" + ("补充链上实锤与筹码异动细节。" * 8)
        wall_ana = "一整坨没有空行换气的长文字墙" * 11
        short_split_viral = "今日两百万\n\n直接沉默"
        trailing_q_text = "今天盘面真走弱了吗？"

        s_clean, bd_clean = score_platform_candidate("binance", ARTICLE_TYPE_ANALYSIS, clean_2p_ana, 2)
        s_wall, bd_wall = score_platform_candidate("binance", ARTICLE_TYPE_ANALYSIS, wall_ana, 2)
        self.assertGreater(s_clean, s_wall)
        self.assertAlmostEqual(bd_clean["readability"], 0.2)
        self.assertAlmostEqual(bd_wall["readability"], -0.4)

        s_q, bd_q = score_platform_candidate("binance", ARTICLE_TYPE_VIRAL, trailing_q_text, 1)
        self.assertAlmostEqual(bd_q["question"], -0.4)

        s_split, bd_split = score_platform_candidate("binance", ARTICLE_TYPE_VIRAL, short_split_viral, 1)
        self.assertAlmostEqual(bd_split["readability"], -0.3)

        s_over400, _ = score_platform_candidate("binance", ARTICLE_TYPE_ANALYSIS, "字" * 420, 1)
        self.assertLess(s_over400, -0.5)

        # OKX stays completely neutral on Binance form/question rules
        s_okx_140, bd_okx_140 = score_platform_candidate("okx", ARTICLE_TYPE_ANALYSIS, wall_ana + "？", 1)
        self.assertAlmostEqual(bd_okx_140["form"], 0.0)
        self.assertAlmostEqual(bd_okx_140["question"], 0.0)
        self.assertAlmostEqual(s_okx_140, 0.0)

        s_okx_240, _ = score_platform_candidate("okx", ARTICLE_TYPE_ANALYSIS, "长段" * 120, 1)
        self.assertAlmostEqual(s_okx_240, -0.3)

    def test_generation_accepts_natural_long_article_above_260(self):
        self.assertGreaterEqual(ARTICLE_MAX_CHARS, 800)
        long_320 = "深度复盘正文" + ("，核心指标与盘面支撑交错演变" * 24)
        self.assertGreater(len(long_320), 260)
        with tempfile.TemporaryDirectory() as tmp:
            img_p = _create_tiny_png(Path(tmp), "long_ok.png")
            mats = [{"id": "M1", "fact": "链上", "visual_evidence": {"placeholder": "[ASSET_IMG_1]", "what_it_shows": "链上"}}]
            imap = {"[ASSET_IMG_1]": {"local_path": img_p}}
            ana_info = {
                "status": "ok",
                "text": long_320,
                "god_comments": ["写得透"],
                "image_placeholders": ["[ASSET_IMG_1]"],
                "used_material_ids": ["M1"],
                "score": 8.5,
                "reason": None,
            }
            ok_ana, err_ana = check_article_info(ana_info, mats, imap, ARTICLE_MAX_CHARS)
            self.assertTrue(ok_ana, err_ana)

            vir_info = {
                "image_semantics": [{"id": "[IMAGE_1]", "semantic_role": "图", "exchange_ui": "", "keep": True}],
                "selected_images": ["[IMAGE_1]"],
                "global_truth": "真实长故事",
                "feasibility_audit": "ok",
                "usable": True,
                "topic_tag": "搞钱与盈亏",
                "expression_strategy": "保留完整故事",
                "post_text": long_320,
                "bait_comments": ["精彩"],
                "viral_score": 8.5,
            }
            ok_vir, err_vir = check_viral_article_info(vir_info, ["[IMAGE_1]"], ARTICLE_MAX_CHARS)
            self.assertTrue(ok_vir, err_vir)

            ana_info["text"] = "字" * (ARTICLE_MAX_CHARS + 10)
            ok_over, _ = check_article_info(ana_info, mats, imap, ARTICLE_MAX_CHARS)
            self.assertFalse(ok_over)

    def test_format_image_article_once_interleaves_and_prioritizes_newest(self):
        from parser.mlu_pipeline import format_image_article_once

        now = time.time()
        posts = [
            {
                "source": "binance",
                "post_id": "old_pre_1",
                "publish_time": now - 36000,
                "content": {"text_content": "旧待初筛帖[插图: https://x/1.png]"},
                "media": {"images": ["https://x/1.png"]},
            },
            {
                "source": "binance",
                "post_id": "new_pre_1",
                "publish_time": now - 60,
                "content": {"text_content": "最新待初筛帖[插图: https://x/2.png]"},
                "media": {"images": ["https://x/2.png"]},
            },
            {
                "source": "okx",
                "post_id": "ready_mlu_1",
                "publish_time": now - 120,
                "pre_screen": {"prompt_version": prompt_version, "no_evidence": False, "reason": "ok"},
                "content": {"text_content": "已过初筛待精提帖[插图: https://x/3.png]"},
                "media": {"images": ["https://x/3.png"]},
            },
        ]
        mock_pm = MagicMock()
        mock_pm.find_posts_by_source.side_effect = (
            lambda src, **kw: [p for p in posts if p["source"] == src]
        )
        screened_order = []
        formatted_ids = []

        def _fake_batch(batch_posts, pm, max_pending=0):
            from parser.mlu_pipeline import _sort_posts_newest_first, is_need_pre_screening
            pending = _sort_posts_newest_first([p for p in batch_posts if is_need_pre_screening(p)])
            screened_order.extend([p["post_id"] for p in pending[:max_pending]])
            return len(pending), len(pending[:max_pending]), 0, 0

        with patch("parser.mlu_pipeline.PRE_SCREEN_MAX_PER_ROUND", 1), \
             patch("parser.mlu_pipeline.batch_pre_screen_posts", side_effect=_fake_batch), \
             patch("parser.mlu_pipeline.process_and_save_single_post", side_effect=lambda p, pm: formatted_ids.append(p["post_id"])):
            pre_pending, pre_pass, _, fmt_cnt, delay = format_image_article_once(post_manager=mock_pm)

        # Newest pending post processed first even under pre_screen bound, AND downstream logic_mul extraction is NOT starved!
        self.assertEqual(screened_order, ["new_pre_1"])
        self.assertEqual(formatted_ids, ["ready_mlu_1"])
        self.assertEqual((pre_pending, pre_pass, fmt_cnt), (2, 1, 1))
        self.assertEqual(delay, 5)

    def test_viral_zero_images_and_citation_leaks_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            img1 = _create_tiny_png(Path(tmp), "v_leak.png")
            base_viral = {
                "image_semantics": [{"id": "[IMAGE_1]", "semantic_role": "图", "exchange_ui": "", "keep": False}],
                "selected_images": [],
                "global_truth": "全废图",
                "feasibility_audit": "no",
                "usable": True,
                "topic_tag": "搞钱与盈亏",
                "expression_strategy": "无",
                "post_text": "缺少配图却放行",
                "bait_comments": ["笑死"],
                "viral_score": 7.5,
            }
            ok_zero, err_zero = check_viral_article_info(base_viral, ["[IMAGE_1]"], ARTICLE_MAX_CHARS)
            self.assertFalse(ok_zero)
            self.assertIn("至少需要保留 1 张有效图片", err_zero)

            cite_viral = {
                **base_viral,
                "image_semantics": [{"id": "[IMAGE_1]", "semantic_role": "新闻截图", "exchange_ui": "", "keep": True}],
                "selected_images": ["[IMAGE_1]"],
                "post_text": "美国司法部申请没收超 11 亿美元 USDC[cite: 1]。",
            }
            ok_cite, err_cite = check_viral_article_info(cite_viral, ["[IMAGE_1]"], ARTICLE_MAX_CHARS)
            self.assertFalse(ok_cite)
            self.assertIn("引用标注", err_cite)

            mats = [{"id": "M1", "fact": "数据", "visual_evidence": {"placeholder": "[ASSET_IMG_1]", "what_it_shows": "K线"}}]
            imap = {"[ASSET_IMG_1]": {"local_path": img1}}
            ana_cite = {
                "status": "ok",
                "text": "ETF 资金继续流出[cite: 2]，守住前低观察。",
                "god_comments": ["继续观察"],
                "image_placeholders": ["[ASSET_IMG_1]"],
                "used_material_ids": ["M1"],
                "score": 8.0,
                "reason": None,
            }
            ok_ana_cite, _ = check_article_info(ana_cite, mats, imap, ARTICLE_MAX_CHARS)
            self.assertFalse(ok_ana_cite)

    def test_analysis_format_avoids_duplicate_headline_coin_prefix(self):
        # Inline coin occurrence later in sentence must NOT prepend a second "$NEAR " at start
        near_art = {
            "topic": "NEAR",
            "article_info": {
                "text": "大盘弱势里 NEAR 还在往 3.26 美元附近爬。",
                "image_placeholders": [],
                "image_mapping": {},
            },
        }
        okx_near, _ = _format_analysis_post_content(near_art, platform="okx")
        self.assertTrue(okx_near.startswith("大盘弱势里 $NEAR "), okx_near)
        self.assertNotIn("$NEAR 大盘弱势里 $NEAR", okx_near)

        # Article that omits coin symbol completely STILL gets "$SOL " prefix cleanly
        no_coin_art = {
            "topic": "SOL",
            "article_info": {
                "text": "现货主动买盘转强，守住 67.3 美元台阶。",
                "image_placeholders": [],
                "image_mapping": {},
            },
        }
        bn_sol, _ = _format_analysis_post_content(no_coin_art, platform="binance")
        self.assertTrue(bn_sol.startswith("$SOL 现货主动买盘转强"), bn_sol)

    def test_extract_valid_viral_posts_caps_repeated_same_author(self):
        with tempfile.TemporaryDirectory() as tmp:
            img1 = _create_tiny_png(Path(tmp), "auth1.png")
            now = time.time()
            spam_posts = [
                {
                    "source": "okx",
                    "post_id": f"spam_{i}",
                    "publish_time": now - i * 60,
                    "author": {"author_id": "spammer_1"},
                    "viral_screen": {"prompt_version": prompt_version, "keep": True, "event_key": f"ek_{i}"},
                    "engagement": {"view_count": 20000 - i * 100, "like_count": 10, "comment_count": 5},
                    "content": {"text_content": f"同作者连发第{i}条[插图: https://x/a{i}.png]"},
                    "media": {"images": [f"https://x/a{i}.png"], "local_mapping": {f"https://x/a{i}.png": img1}},
                }
                for i in range(4)
            ]
            mock_art_mgr = MagicMock()
            mock_art_mgr.find_articles.return_value = []
            with patch("parser.article_generator.UniversalPostManager") as mock_upm, \
                 patch("parser.article_generator.gen_db_object"), \
                 patch("parser.article_generator._get_all_self_published_post_ids", return_value=set()):
                mock_upm.return_value.find_posts_by_source.side_effect = (
                    lambda src, **kw: spam_posts if src == "okx" else []
                )
                selected = extract_valid_viral_posts(mock_art_mgr)
            self.assertEqual([p["post_id"] for p in selected], ["spam_0", "spam_1"])


class TestPromptAbstractionHygiene(unittest.TestCase):
    def test_prompts_are_abstract_and_free_of_legacy_case_anchors(self):
        from common.llm_gateway import render_prompt_template
        from parser.config import (
            ANALYSIS_EDITOR_PROMPT_NAME,
            ARTICLE_PROMPT_NAME,
            MLU_EXTRACT_PROMPT_NAME,
            PRE_SCREEN_PROMPT_NAME,
            VIRAL_EDITOR_PROMPT_NAME,
            VIRAL_STAGE1_PROMPT_NAME,
            VIRAL_STAGE2_PROMPT_NAME,
        )

        prompt_names = (
            PRE_SCREEN_PROMPT_NAME,
            MLU_EXTRACT_PROMPT_NAME,
            ARTICLE_PROMPT_NAME,
            ANALYSIS_EDITOR_PROMPT_NAME,
            VIRAL_STAGE1_PROMPT_NAME,
            VIRAL_STAGE2_PROMPT_NAME,
            VIRAL_EDITOR_PROMPT_NAME,
        )
        forbidden_case_anchors = (
            "K-DOT Plot",
            "主播假酒",
            "大厂员工篡改代码",
            "半场开香槟",
            "某某预警",
            "白捡钱感",
            "强平劫后余生",
            "大饼→",
            "二饼→",
            "破位就空",
            "500 元挑战",
            "再不上车就彻底晚了",
            "卖盘比买盘多几倍",
            "止损不丢人",
            "散户看戏",
            "巨鲸抢筹",
            "带血筹码",
        )
        for pname in prompt_names:
            text, _ = render_prompt_template(pname)
            self.assertTrue(text.strip(), f"Empty prompt: {pname}")
            for anchor in forbidden_case_anchors:
                self.assertNotIn(anchor, text, f"Found legacy concrete anchor {anchor!r} in {pname}")

    def test_generation_prompts_are_free_of_platform_layout_quotas(self):
        from common.llm_gateway import render_prompt_template
        from parser.config import ARTICLE_PROMPT_NAME, VIRAL_STAGE2_PROMPT_NAME

        forbidden_platform_quotas = (
            "150~240",
            "<=260",
            "2~3 个短段",
            "优先挑 2~3 张",
            "约 110 字",
            "超 120 字",
        )
        for pname in (ARTICLE_PROMPT_NAME, VIRAL_STAGE2_PROMPT_NAME):
            text, _ = render_prompt_template(pname)
            for quota in forbidden_platform_quotas:
                self.assertNotIn(quota, text, f"Found platform-coupled quota {quota!r} in generation prompt {pname}")


class TestTelemetryAndMetricsTimeline(unittest.TestCase):
    def test_emit_telemetry_event_writes_jsonl_and_never_raises(self):
        from parser.telemetry import TELEMETRY_LOG_FILE, emit_telemetry_event, reset_telemetry_handler_for_test

        orig_path = TELEMETRY_LOG_FILE
        with tempfile.TemporaryDirectory() as tmp:
            tmp_log = str(Path(tmp) / "events.jsonl")
            reset_telemetry_handler_for_test(tmp_log)
            try:
                ok = emit_telemetry_event("publish_result", {"platform": "binance", "post_id": "123", "unused": None}, now_ts=1_800_000_000.0)
                self.assertTrue(ok)
                reset_telemetry_handler_for_test(orig_path)
                lines = Path(tmp_log).read_text(encoding="utf-8").strip().splitlines()
                self.assertEqual(len(lines), 1)
                obj = json.loads(lines[0])
                self.assertEqual(obj["event"], "publish_result")
                self.assertEqual(obj["platform"], "binance")
                self.assertEqual(obj["post_id"], "123")
                self.assertNotIn("unused", obj)
                # Invalid path should swallow I/O errors cleanly and return False
                bad_ok = emit_telemetry_event("test_err", {"a": 1}, log_path=str(Path(tmp)))
                self.assertFalse(bad_ok)
            finally:
                reset_telemetry_handler_for_test(orig_path)

    def test_metrics_timeline_records_growth_curve_dedups_and_caps(self):
        now = 1_800_000_000.0
        pub_t = now - 1800  # 30m old -> 15m interval
        docs = [
            {
                "article_id": "a_curve",
                "type": ARTICLE_TYPE_VIRAL,
                "article_info": {"post_text": "冷启动曲线样本"},
                "publish_records": {"binance": {"post_id": "201", "published_by": "myself", "publish_time": pub_t, "view_count": None}},
            },
            {
                "article_id": "a_del",
                "type": ARTICLE_TYPE_VIRAL,
                "article_info": {"post_text": "被删样本"},
                "publish_records": {"binance": {"post_id": "202", "published_by": "ruru", "publish_time": pub_t, "view_count": None}},
            },
            {
                "article_id": "a_okx_miss",
                "type": ARTICLE_TYPE_VIRAL,
                "article_info": {"post_text": "OKX 审核未过样本"},
                "publish_records": {"okx": {"post_id": "301", "published_by": "nana", "publish_time": pub_t, "view_count": None}},
            },
        ]
        mock_mgr = MagicMock(spec=["upsert_articles"])

        def _fake_bn(pid):
            if pid == "201":
                return {
                    "contentStatus": 2,
                    "viewCount": 120,
                    "likeCount": 1,
                    "commentCount": 0,
                    "shareCount": 2,
                    "recommend": True,
                    "lan": "cn",
                    "detectedLang": "zh-CN",
                    "coinPairList": ["$BTC "],
                    "tradeWidgets": [{"coin": "BTC"}],
                }
            return None

        with patch("parser.platform_adapters.fetch_binance_post_detail", side_effect=_fake_bn), \
             patch("parser.platform_adapters.emit_telemetry_event") as mock_emit:
            sync_published_metrics(mock_mgr, "binance", docs[:2], now_ts=now)
            # Within 5 minutes (less than 8m min gap), same state -> no duplicate timeline point even if forced
            sync_published_metrics(mock_mgr, "binance", docs[:1], now_ts=now + 300)
            # 16 minutes later -> passes 15m early refresh interval and appends point 2
            with patch("parser.platform_adapters.fetch_binance_post_detail", return_value={"contentStatus": 2, "viewCount": 195, "likeCount": 2, "commentCount": 1, "shareCount": 2, "recommend": True}):
                sync_published_metrics(mock_mgr, "binance", docs[:1], now_ts=now + 960)

        rec = docs[0]["publish_records"]["binance"]
        self.assertEqual(rec["live_state"], "ok")
        self.assertEqual(rec["share_count"], 2)
        self.assertEqual(rec["coin_pair_list"], ["$BTC "])
        self.assertEqual(rec["trade_widget_count"], 1)
        self.assertEqual(len(rec["metrics_timeline"]), 2)
        self.assertEqual([pt["v"] for pt in rec["metrics_timeline"]], [120, 195])
        # Second emit on post 201 should compute delta_v = 75
        deltas = [call.args[1].get("delta_v") for call in mock_emit.call_args_list if call.args[1].get("post_id") == "201"]
        self.assertIn(75, deltas)
        self.assertEqual(docs[1]["publish_records"]["binance"]["live_state"], "deleted")

        with patch("parser.platform_adapters.fetch_okx_post_detail", return_value=None), \
             patch("parser.platform_adapters.emit_telemetry_event"):
            sync_published_metrics(mock_mgr, "okx", docs[2:], now_ts=now)
        self.assertEqual(docs[2]["publish_records"]["okx"]["live_state"], "missing")

        # Cap check: pre-populate 24 points and append 25th -> capped at 24
        rec["metrics_timeline"] = [{"ts": int(now + i * 600), "age_m": float(i * 10), "v": i, "state": "ok", "status": 2, "rec": True} for i in range(24)]
        with patch("parser.platform_adapters.fetch_binance_post_detail", return_value={"contentStatus": 2, "viewCount": 999, "likeCount": 3, "commentCount": 1, "shareCount": 2, "recommend": True}), \
             patch("parser.platform_adapters.emit_telemetry_event"):
            sync_published_metrics(mock_mgr, "binance", docs[:1], now_ts=now + 24 * 600 + 7500)
        self.assertEqual(len(rec["metrics_timeline"]), 24)
        self.assertEqual(rec["metrics_timeline"][-1]["v"], 999)

    def test_post_detail_fetchers_raise_on_error_flag(self):
        from platforms import binance_api, okx_api

        fake_sess = MagicMock()
        fake_sess.get.side_effect = ConnectionError("proxy timeout")
        with patch.object(binance_api, "MAX_RETRIES", 2), \
             patch.object(binance_api.time, "sleep"):
            self.assertIsNone(binance_api.fetch_binance_post_detail("123", session=fake_sess))
            with self.assertRaises(ConnectionError):
                binance_api.fetch_binance_post_detail("123", session=fake_sess, raise_on_error=True)

        err_payload_resp = MagicMock()
        err_payload_resp.status_code = 200
        err_payload_resp.raise_for_status.return_value = None
        err_payload_resp.json.return_value = {"code": "999999", "success": False, "message": "rate limit"}
        fake_sess.get.side_effect = None
        fake_sess.get.return_value = err_payload_resp
        with patch.object(binance_api, "MAX_RETRIES", 2), \
             patch.object(binance_api.time, "sleep"):
            with self.assertRaises(RuntimeError):
                binance_api.fetch_binance_post_detail("123", session=fake_sess, raise_on_error=True)

        empty_bn_resp = MagicMock()
        empty_bn_resp.status_code = 200
        empty_bn_resp.raise_for_status.return_value = None
        empty_bn_resp.json.return_value = {"code": "000000", "success": True, "data": None}
        fake_sess.get.return_value = empty_bn_resp
        self.assertIsNone(binance_api.fetch_binance_post_detail("123", session=fake_sess, raise_on_error=True))

        okx_err_sess = MagicMock()
        okx_err_sess.get.side_effect = TimeoutError("read timeout")
        with patch.object(okx_api, "MAX_RETRIES", 2), \
             patch.object(okx_api.time, "sleep"):
            self.assertIsNone(okx_api.fetch_okx_post_detail("456", session=okx_err_sess))
            with self.assertRaises(TimeoutError):
                okx_api.fetch_okx_post_detail("456", session=okx_err_sess, raise_on_error=True)

        empty_okx_resp = MagicMock()
        empty_okx_resp.raise_for_status.return_value = None
        empty_okx_resp.json.return_value = {"code": "52001", "data": None}
        okx_err_sess.get.side_effect = None
        okx_err_sess.get.return_value = empty_okx_resp
        self.assertIsNone(okx_api.fetch_okx_post_detail("456", session=okx_err_sess, raise_on_error=True))

    def test_sync_published_metrics_transient_error_and_two_step_missing(self):
        from parser.post_ingestion import ARTICLE_TYPE_VIRAL

        pub_t = 1700000000.0
        now = pub_t + 8 * 3600  # 8h old -> normal refresh interval is 2h
        mock_mgr = MagicMock(spec=["upsert_articles"])

        doc = {
            "article_id": "a_live_blip",
            "type": ARTICLE_TYPE_VIRAL,
            "article_info": {"post_text": "瞬态抖动防误锁测试"},
            "publish_records": {
                "binance": {
                    "post_id": "888001",
                    "published_by": "myself",
                    "publish_time": pub_t,
                    "view_count": 320,
                    "live_state": "ok",
                    "metrics_synced_at": now - 3 * 3600,
                    "metrics_timeline": [
                        {"ts": int(now - 3 * 3600), "age_m": 300.0, "v": 320, "state": "ok", "status": 2, "rec": True}
                    ],
                }
            },
        }
        rec = doc["publish_records"]["binance"]

        # 1. Transient network exception -> keeps original live_state="ok" & metrics_synced_at
        with patch("parser.platform_adapters.fetch_binance_post_detail", side_effect=ConnectionError("blip")) as mock_fetch, \
             patch("parser.platform_adapters.emit_telemetry_event"):
            sync_published_metrics(mock_mgr, "binance", [doc], now_ts=now)
        self.assertEqual(mock_fetch.call_args.kwargs.get("raise_on_error"), True)
        self.assertEqual(rec["live_state"], "ok")
        self.assertEqual(rec["view_count"], 320)
        self.assertEqual(rec.get("missing_confirm_count", 0), 0)
        self.assertEqual(rec["metrics_synced_at"], now - 3 * 3600)

        # 2. 1st empty return on live post -> suspect only, stays "ok", no deleted point appended
        with patch("parser.platform_adapters.fetch_binance_post_detail", return_value=None), \
             patch("parser.platform_adapters.emit_telemetry_event"):
            sync_published_metrics(mock_mgr, "binance", [doc], now_ts=now)
        self.assertEqual(rec["live_state"], "ok")
        self.assertEqual(rec["missing_confirm_count"], 1)
        self.assertEqual(len(rec["metrics_timeline"]), 1)

        # 3. Recheck 10m later (faster than 2h normal interval) recovers cleanly and resets counter
        with patch(
            "parser.platform_adapters.fetch_binance_post_detail",
            return_value={"contentStatus": 2, "viewCount": 380, "likeCount": 4, "commentCount": 1, "shareCount": 0, "recommend": True},
        ), patch("parser.platform_adapters.emit_telemetry_event"):
            sync_published_metrics(mock_mgr, "binance", [doc], now_ts=now + 605)
        self.assertEqual(rec["live_state"], "ok")
        self.assertEqual(rec["view_count"], 380)
        self.assertEqual(rec["missing_confirm_count"], 0)

        # 4. Two consecutive empty returns confirm deletion and then stop polling
        with patch("parser.platform_adapters.fetch_binance_post_detail", return_value=None), \
             patch("parser.platform_adapters.emit_telemetry_event"):
            sync_published_metrics(mock_mgr, "binance", [doc], now_ts=now + 605 + 7300)
            self.assertEqual(rec["live_state"], "ok")
            self.assertEqual(rec["missing_confirm_count"], 1)
            sync_published_metrics(mock_mgr, "binance", [doc], now_ts=now + 605 + 7300 + 610)
            self.assertEqual(rec["live_state"], "deleted")
            self.assertEqual(rec["missing_confirm_count"], 2)

        with patch("parser.platform_adapters.fetch_binance_post_detail") as mock_settled:
            sync_published_metrics(mock_mgr, "binance", [doc], now_ts=now + 605 + 7300 + 7200)
        mock_settled.assert_not_called()

        # 5. Legacy single-blip "deleted" record (no missing_confirm_count) self-heals on next run
        legacy_doc = {
            "article_id": "a_legacy_blip",
            "type": ARTICLE_TYPE_VIRAL,
            "article_info": {"post_text": "历史单次误判自愈"},
            "publish_records": {
                "binance": {
                    "post_id": "888002",
                    "published_by": "ruru",
                    "publish_time": pub_t,
                    "view_count": 210,
                    "live_state": "deleted",
                    "metrics_synced_at": now - 1800,
                }
            },
        }
        with patch(
            "parser.platform_adapters.fetch_binance_post_detail",
            return_value={"contentStatus": 2, "viewCount": 330, "likeCount": 2, "commentCount": 0, "shareCount": 0, "recommend": True},
        ), patch("parser.platform_adapters.emit_telemetry_event"):
            sync_published_metrics(mock_mgr, "binance", [legacy_doc], now_ts=now)
        self.assertEqual(legacy_doc["publish_records"]["binance"]["live_state"], "ok")
        self.assertEqual(legacy_doc["publish_records"]["binance"]["view_count"], 330)
        self.assertEqual(legacy_doc["publish_records"]["binance"]["missing_confirm_count"], 0)

    def test_persist_publish_result_writes_publish_snapshot_and_telemetry(self):
        from parser.publisher_arena import _persist_publish_result

        with tempfile.TemporaryDirectory() as tmp:
            store = AccountPublishStateStore(str(Path(tmp) / "pub_snap.json"))
            now = 1_800_000_000.0
            art = {
                "article_id": "art_snap_1",
                "source": "okx",
                "model_name": "gpt-6-luna",
                "prompt_version": prompt_version,
                "post_id_list": ["src_okx_1"],
                "article_info": {
                    "selected_images": ["[IMAGE_1]"],
                    "image_mapping": {"[IMAGE_1]": {"exchange_ui": ""}},
                },
                "_selection_meta": {
                    "prompt_version": "sel-v1",
                    "reason": "anti-clutter",
                    "model_used": "editor-m1",
                    "candidate_pool_size": 18,
                    "selected_candidate_no": 3,
                    "platform_fit_score": 1.2,
                },
                "_source_engagement": {"view_count": 12000, "like_count": 88, "comment_count": 19},
                "_source_publish_time": now - 5 * 3600,
            }
            mock_mgr = MagicMock(spec=["upsert_articles"])
            acc_state: Dict[str, Any] = {}
            rec: Dict[str, Any] = {}
            with patch("parser.publisher_arena.emit_telemetry_event") as mock_emit:
                _persist_publish_result(
                    mock_mgr,
                    "binance",
                    "myself",
                    acc_state,
                    art,
                    rec,
                    True,
                    "3990001",
                    1,
                    now,
                    "搞钱与盈亏",
                    None,
                    None,
                    ARTICLE_TYPE_VIRAL,
                    "BTC",
                    state_store=store,
                    final_char_len=145,
                    final_img_cnt=2,
                )
            snap = rec["publish_snapshot"]
            self.assertEqual(snap["source_platform"], "okx")
            self.assertEqual(snap["source_views"], 12000)
            self.assertEqual(snap["source_delay_hours"], 5.0)
            self.assertEqual(snap["arena_pool_size"], 18)
            self.assertEqual(snap["arena_rank"], 3)
            self.assertEqual(snap["final_char_len"], 145)
            self.assertEqual(snap["final_img_cnt"], 2)
            mock_emit.assert_called_once()
            self.assertEqual(mock_emit.call_args.args[0], "publish_result")

    def test_sync_recent_metrics_once_and_report_builder(self):
        from parser.maintenance_tasks import build_publish_telemetry_report, sync_recent_metrics_once

        now = 1_800_000_000.0
        bn_doc = {
            "article_id": "a_rep_1",
            "type": ARTICLE_TYPE_VIRAL,
            "source": "okx",
            "status": "ok",
            "article_info": {"post_text": "测试报告帖"},
            "publish_records": {
                "binance": {
                    "status": "success",
                    "published_by": "myself",
                    "publish_time": now - 3600,
                    "post_id": "9001",
                    "view_count": 320,
                    "like_count": 2,
                    "comment_count": 1,
                    "share_count": 0,
                    "recommend": True,
                    "content_status": 2,
                    "live_state": "ok",
                    "platform_fit_score": 1.2,
                    "metrics_synced_at": now - 100,
                    "metrics_timeline": [
                        {"ts": int(now - 2400), "age_m": 20.0, "v": 150, "state": "ok"},
                        {"ts": int(now - 100), "age_m": 58.3, "v": 320, "state": "ok"},
                    ],
                    "publish_snapshot": {"source_views": 25000},
                }
            },
        }
        mock_mgr = MagicMock(spec=["find_articles", "upsert_articles"])
        mock_mgr.find_articles.side_effect = [[bn_doc], [], [bn_doc], []]
        with patch("parser.platform_adapters.fetch_binance_post_detail", return_value=None):
            summary = sync_recent_metrics_once(mock_mgr, lookback_hours=24, now_ts=now)
        self.assertEqual(summary["binance"]["scanned"], 1)
        self.assertEqual(summary["okx"]["scanned"], 0)
        report = build_publish_telemetry_report(mock_mgr, hours=24, sync_first=False, now_ts=now)
        self.assertIn("pid=9001", report)
        self.assertIn("20m:150 -> 58m:320", report)
        self.assertIn("分组汇总", report)


if __name__ == "__main__":
    unittest.main()
