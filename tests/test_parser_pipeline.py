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
    check_viral_article_info,
    execute_article_task,
    execute_viral_article_task,
    generate_analysis_articles,
    generate_viral_articles,
    transform_mlus,
)
from parser.mlu_pipeline import check_format_info
from parser.platform_adapters import (
    BinancePlatform,
    OkxPlatform,
    _build_publish_text_list,
    _format_analysis_post_content,
    _format_viral_post_content,
    has_rival_exchange_ui,
    has_rival_keyword,
    normalize_exchange_ui,
    sync_published_metrics,
)
from parser.post_ingestion import (
    ARTICLE_TYPE_ANALYSIS,
    ARTICLE_TYPE_VIRAL,
    _fetch_hot_posts,
    _is_valid_raw_post,
    ensure_post_media_downloaded,
    prompt_version,
)
from parser.publish_state import AccountPublishStateStore
from parser.publisher_arena import (
    _collect_publish_candidates,
    _publish_articles_for_platform,
    _select_and_polish_best_candidate,
)


def _create_tiny_png(directory: Path, name: str = "sample.png") -> str:
    p = directory / name
    Image.new("RGB", (8, 8), color=(12, 120, 210)).save(p, format="PNG")
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

            # Auth-expired, HTTP 405/WAF, and transient failures record diagnostics but do NOT block publishing
            store.record_failure(
                "binance",
                "ruru",
                "页面探测到登录入口或会话已超时，本地 Cookie 可能已过期失效，请先重新登录。",
                is_viral=False,
                now=t0,
            )
            store.record_failure("binance", "ruru", "HTTP 405", is_viral=False, now=t0 + 1)
            detail = store.record_failure("binance", "ruru", "timeout waiting for button", is_viral=False, now=t0 + 2)
            self.assertEqual(detail, "timeout waiting for button")
            self.assertEqual(store.can_publish("binance", "ruru", is_viral=False, now=t0 + 3), (True, ""))
            self.assertEqual(store.can_publish("binance", "ruru", is_viral=True, now=t0 + 3), (True, ""))

            snap_fail = store.get_account_state("binance", "ruru")
            self.assertEqual(snap_fail["consecutive_failures"], 3)
            self.assertEqual(snap_fail["last_error_msg"], "timeout waiting for button")
            self.assertEqual(snap_fail["last_error_time"], t0 + 2)

            # Legacy future backoff_until / viral_backoff_until timestamps in state JSON are ignored and cleaned on write
            raw_state = json.loads(state_path.read_text(encoding="utf-8"))
            raw_state["binance"]["ruru"]["backoff_until"] = t0 + 99_999
            raw_state["binance"]["ruru"]["backoff_reason"] = "auth_expired"
            raw_state["binance"]["ruru"]["viral_backoff_until"] = t0 + 99_999
            raw_state["binance"]["ruru"]["viral_backoff_reason"] = "waf_challenge"
            state_path.write_text(json.dumps(raw_state), encoding="utf-8")

            self.assertEqual(store.can_publish("binance", "ruru", is_viral=False, now=t0 + 10), (True, ""))
            self.assertEqual(store.can_publish("binance", "ruru", is_viral=True, now=t0 + 10), (True, ""))

            store.record_failure("binance", "ruru", "HTTP 405", is_viral=True, now=t0 + 11)
            snap_after_viral_fail = store.get_account_state("binance", "ruru")
            self.assertNotIn("viral_backoff_until", snap_after_viral_fail)
            self.assertNotIn("viral_backoff_reason", snap_after_viral_fail)
            self.assertEqual(snap_after_viral_fail["viral_consecutive_failures"], 1)

            # Success resets consecutive_failures, cleans legacy backoff keys, and enforces 3h account-level cooldown + 2h topic cooldown
            t1 = t0 + 100
            store.record_success("binance", "ruru", is_viral=False, topic="BTC", now=t1)
            snap = store.get_account_state("binance", "ruru")
            self.assertNotIn("backoff_until", snap)
            self.assertNotIn("backoff_reason", snap)
            self.assertEqual(snap["consecutive_failures"], 0)
            self.assertEqual(snap["account_last_publish_time"], t1)
            self.assertEqual(snap["last_publish_type"], "analysis")
            self.assertTrue(store.is_topic_in_cooldown("binance", "ruru", "BTC", now=t1 + 3600))
            self.assertFalse(store.is_topic_in_cooldown("binance", "ruru", "BTC", now=t1 + 7201))

            # Analysis & viral share the same account-level global cooldown (3h)
            ok_before_3h, _ = store.can_publish("binance", "ruru", is_viral=False, now=t1 + 2 * 3600)
            ok_viral_before_3h, _ = store.can_publish("binance", "ruru", is_viral=True, now=t1 + 2 * 3600)
            ok_after_3h, _ = store.can_publish("binance", "ruru", is_viral=False, now=t1 + 3 * 3600 + 1)
            ok_viral_after_3h, _ = store.can_publish("binance", "ruru", is_viral=True, now=t1 + 3 * 3600 + 1)
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
        self.assertTrue(has_rival_keyword("binance", "欧易最新公告"))
        self.assertTrue(has_rival_keyword("binance", "$OKB 日线突破前高"))
        self.assertFalse(has_rival_keyword("binance", "lookbook 观察笔记"))
        self.assertFalse(has_rival_keyword("binance", "Binance BTC setup"))
        self.assertTrue(has_rival_keyword("okx", "在币安看到一个信号"))
        self.assertFalse(has_rival_keyword("okx", "OKX ETH setup"))

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

            def _make_viral(aid, img_path, ui, post_text, orig_text="原帖提到 $JCT", source="binance", include_ui_field=True):
                meta = {"local_path": img_path, "original_url": f"https://img/{aid}.png", "content_id": f"cid_{aid}"}
                sem = {"id": "[IMAGE_1]", "semantic_role": "截图", "keep": True}
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

            arts = [
                _make_viral("v_okx_ui", img_okx, "okx", "麻吉大哥昨晚又加仓爆掉 2000 万刀"),
                _make_viral("v_bn_ui", img_bn, "binance", "SOL 跌破前低引发连环止损"),
                _make_viral("v_clean", img_clean, "", "昨晚全网吃瓜现场"),
                _make_viral("v_legacy", img_legacy, "", "$ETH 资金费率转负了", include_ui_field=False),
            ]
            mock_mgr = MagicMock()
            mock_mgr.find_articles.return_value = arts
            mock_mgr.db = MagicMock()

            bn_cands, _, bn_stats = _collect_publish_candidates(mock_mgr, "binance", ["JCT"], set(), ARTICLE_TYPE_VIRAL)
            self.assertEqual(bn_stats["rival_image_ui"], 1)
            bn_by_id = {c[0]["article_id"]: c[2] for c in bn_cands}
            self.assertEqual(set(bn_by_id.keys()), {"v_bn_ui", "v_clean", "v_legacy"})
            # Original noisy JCT does NOT pollute clean post_text; SOL & ETH extracted cleanly; pure story keeps ""
            self.assertEqual(bn_by_id["v_bn_ui"], "SOL")
            self.assertEqual(bn_by_id["v_clean"], "")
            self.assertEqual(bn_by_id["v_legacy"], "ETH")

            okx_cands, _, okx_stats = _collect_publish_candidates(mock_mgr, "okx", ["JCT"], set(), ARTICLE_TYPE_VIRAL)
            self.assertEqual(okx_stats["rival_image_ui"], 1)
            okx_ids = {c[0]["article_id"] for c in okx_cands}
            self.assertEqual(okx_ids, {"v_okx_ui", "v_clean", "v_legacy"})

    def test_publish_articles_for_platform_skips_claimed_and_releases_on_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = AccountPublishStateStore(str(Path(tmp) / "claim_state.json"))
            img_p = _create_tiny_png(Path(tmp), "claim.png")
            art = {
                "_id": "mongo_1",
                "article_id": "art_claim",
                "source": "binance",
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
            mock_mgr.find_articles.side_effect = [[], [art], []]
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
            mock_mgr = MagicMock()
            mock_mgr.find_articles.return_value = [art_okx, art_binance]
            mock_mgr.db = MagicMock()

            cands, raw_cnt, stats = _collect_publish_candidates(
                mock_mgr,
                "okx",
                ["BTC"],
                set(),
                ARTICLE_TYPE_VIRAL,
            )
            self.assertEqual(raw_cnt, 2)
            self.assertEqual(stats["source_conflict"], 1)
            self.assertEqual(len(cands), 1)
            self.assertEqual(cands[0][0]["source"], "binance")

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


if __name__ == "__main__":
    unittest.main()
