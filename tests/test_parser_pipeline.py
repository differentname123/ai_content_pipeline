# -*- coding: utf-8 -*-
"""Parser pipeline unit tests: publish_state, platform_adapters, article_generator, publisher_arena."""
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from PIL import Image

from parser.article_generator import (
    build_article_task,
    build_viral_article_task,
    execute_article_task,
    execute_viral_article_task,
)
from parser.platform_adapters import (
    BinancePlatform,
    _build_publish_text_list,
    has_rival_keyword,
)
from parser.post_ingestion import (
    ARTICLE_TYPE_ANALYSIS,
    ARTICLE_TYPE_VIRAL,
    prompt_version,
)
from parser.publish_state import (
    AUTH_BACKOFF_SECONDS,
    TRANSIENT_BACKOFF_SECONDS,
    WAF_BACKOFF_SECONDS,
    AccountPublishStateStore,
    classify_publish_error,
)
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
    def test_classify_publish_error(self):
        self.assertEqual(classify_publish_error("广场主页加载异常 (HTTP 405)"), ("waf_challenge", WAF_BACKOFF_SECONDS))
        self.assertEqual(classify_publish_error("[WAF/人机验证] 我们需要确认您是人类"), ("waf_challenge", WAF_BACKOFF_SECONDS))
        self.assertEqual(classify_publish_error("页面探测到登录入口，Cookie失效"), ("auth_expired", AUTH_BACKOFF_SECONDS))
        self.assertEqual(
            classify_publish_error("页面探测到登录入口或会话已超时，本地 Cookie 可能已过期失效，请先重新登录。"),
            ("auth_expired", AUTH_BACKOFF_SECONDS),
        )
        self.assertEqual(classify_publish_error("需要验证：登录会话已超时，执行安全验证"), ("auth_expired", AUTH_BACKOFF_SECONDS))
        self.assertEqual(classify_publish_error("timeout waiting for button"), ("transient", TRANSIENT_BACKOFF_SECONDS))

    def test_parser_config_is_single_source_of_truth(self):
        import parser.config as pcfg
        import parser.article_format_logical as afl
        import parser.maintenance_tasks as mt
        import parser.publish_state as ps

        self.assertIs(mt.INTERACTION_CONFIG, pcfg.INTERACTION_CONFIG)
        self.assertEqual(ps.WAF_BACKOFF_SECONDS, pcfg.WAF_BACKOFF_SECONDS)
        self.assertEqual(ps.AUTH_BACKOFF_SECONDS, pcfg.AUTH_BACKOFF_SECONDS)
        self.assertEqual(set(afl.TASK_SWITCHES.keys()), set(afl.TASK_REGISTRY.keys()))

    def test_cooldown_backoff_topic_and_success_reset(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "state.json"
            store = AccountPublishStateStore(str(state_path))
            t0 = 1_700_000_000.0

            # Initially eligible for both analysis and viral
            ok, reason = store.can_publish("binance", "ruru", is_viral=False, now=t0)
            self.assertTrue(ok, reason)

            # Failure with HTTP 405 triggers 60m backoff for analysis ONLY, independent from viral
            cat, sec, _ = store.record_failure("binance", "ruru", "HTTP 405", is_viral=False, now=t0)
            self.assertEqual((cat, sec), ("waf_challenge", 3600))
            ok_an, _ = store.can_publish("binance", "ruru", is_viral=False, now=t0 + 1800)
            ok_vi, _ = store.can_publish("binance", "ruru", is_viral=True, now=t0 + 1800)
            self.assertFalse(ok_an)
            self.assertTrue(ok_vi)

            # After 60m backoff expires, success clears backoff and sets 6h cooldown + 2h topic cooldown
            t1 = t0 + 3601
            store.record_success("binance", "ruru", is_viral=False, topic="BTC", now=t1)
            snap = store.get_account_state("binance", "ruru")
            self.assertEqual(snap["backoff_until"], 0.0)
            self.assertEqual(snap["consecutive_failures"], 0)
            self.assertTrue(store.is_topic_in_cooldown("binance", "ruru", "BTC", now=t1 + 3600))
            self.assertFalse(store.is_topic_in_cooldown("binance", "ruru", "BTC", now=t1 + 7201))

            # Analysis cooldown is 6h
            ok_before_6h, _ = store.can_publish("binance", "ruru", is_viral=False, now=t1 + 5 * 3600)
            ok_after_6h, _ = store.can_publish("binance", "ruru", is_viral=False, now=t1 + 6 * 3600 + 1)
            self.assertFalse(ok_before_6h)
            self.assertTrue(ok_after_6h)

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
        self.assertFalse(has_rival_keyword("binance", "Binance BTC setup"))
        self.assertTrue(has_rival_keyword("okx", "在币安看到一个信号"))
        self.assertFalse(has_rival_keyword("okx", "OKX ETH setup"))

    def test_build_publish_text_list_splits_coin_tags_with_newline(self):
        segments = _build_publish_text_list("$BTC 看多结构明确\n$ETH 同步放量")
        coin_segs = [s for s in segments if s["text"].startswith("$")]
        self.assertEqual([s["text"] for s in coin_segs], ["$BTC", "$ETH"])
        self.assertTrue(all(s["delay"] == 2 for s in coin_segs))
        non_coin = [s for s in segments if not s["text"].startswith("$")]
        self.assertTrue(all(s["text"].startswith("\n") for s in non_coin))

    def test_binance_decorate_analysis(self):
        adapter = BinancePlatform()
        with patch("parser.platform_adapters.fetch_binance_hot_hashtags", return_value=[{"hashtag": "#HotCrypto"}]), \
             patch("parser.platform_adapters.fetch_binance_future_hot_coins", return_value=[{"symbol": "ETHUSDT"}]):
            decorated = adapter.decorate_analysis("BTC 突破关键阻力", "BTC")
        self.assertIn("#HotCrypto", decorated)
        self.assertIn("#BTC", decorated)
        self.assertIn("$BTC ", decorated)
        self.assertIn("#ETH", decorated)
        self.assertIn("$ETH ", decorated)


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
                "image_semantics": [{"id": "[IMAGE_1]", "semantic_role": "交易爆仓图", "keep": True}],
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
            self.assertIsNone(mock_v_llm.call_args.kwargs["model"])
            self.assertEqual(mock_v_llm.call_args.kwargs["preset_model_group"], "medium")


class TestPublisherArena(unittest.TestCase):
    def test_early_exit_when_zero_accounts_eligible_skips_db_and_llm(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = AccountPublishStateStore(str(Path(tmp) / "state.json"))
            now = time.time()
            # Force all configured binance accounts into WAF backoff
            with patch("parser.publisher_arena.PLATFORM_ACCOUNTS", {"binance": ["myself", "ruru"]}):
                store.record_failure("binance", "myself", "HTTP 405", is_viral=False, now=now)
                store.record_failure("binance", "ruru", "HTTP 405", is_viral=False, now=now)
                mock_mgr = MagicMock()
                with patch("parser.publisher_arena.call_llm_json") as mock_llm:
                    _publish_articles_for_platform(mock_mgr, "binance", ["BTC"], ARTICLE_TYPE_ANALYSIS, state_store=store)

                mock_mgr.find_articles.assert_not_called()
                mock_llm.assert_not_called()

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

    def test_chief_editor_strips_scores_and_rejects_rival_keywords(self):
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

            captured_prompt = {}

            def _fake_editor(**kwargs):
                captured_prompt["text"] = kwargs["prompt"]
                # Deliberately return rival keyword "OKX" on Binance so arena MUST reject overwriting draft_text!
                return (
                    {
                        "selected_candidate_no": 1,
                        "final_text": "欢迎去 OKX 交易 BTC",
                        "final_comments": ["走起"],
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
            # Original clean text preserved because polished output mentioned rival OKX
            self.assertEqual(art["article_info"]["text"], "原始干净正文")
            self.assertEqual(art["_selection_meta"]["model_used"], "editor-model-v1")


if __name__ == "__main__":
    unittest.main()
