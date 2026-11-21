# -*- coding: utf-8 -*-
"""Core primitives unit tests: x_text, post_utils, llm_gateway, daemon, self-post guard."""
import logging
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from common.common_utils import is_self_post
from common.daemon import compute_jittered_interval, run_daemon_loop
from common.llm_gateway import call_llm_json, parse_llm_json, render_prompt_template
from common.post_utils import (
    AccountIsolationError,
    assert_not_x_myself_dir,
    get_unified_post_text,
    is_fetchable_external_article_url,
    is_video_post,
)
from common.x_text import calc_raw_char_length, calc_x_weighted_length, is_within_x_limit


class TestXText(unittest.TestCase):
    def test_weighted_length_ascii_cjk_emoji_and_url(self):
        self.assertEqual(calc_x_weighted_length("hello!"), 6)
        self.assertEqual(calc_x_weighted_length("比特币"), 6)
        self.assertEqual(calc_x_weighted_length("🚀"), 2)
        # URL normalized to 23 weighted units regardless of raw URL length
        url_text = "Check https://example.com/very/long/path?foo=bar&baz=qux now"
        self.assertEqual(calc_x_weighted_length(url_text), len("Check  now") + 23)
        self.assertEqual(calc_raw_char_length("  abc中文  "), 5)

    def test_is_within_x_limit_double_gate(self):
        self.assertTrue(is_within_x_limit("简短测试", max_units=280, max_raw_chars=140))
        self.assertFalse(is_within_x_limit("", max_units=280, max_raw_chars=140))
        # 141 CJK chars => > 140 raw chars AND 282 > 280 weighted units
        long_cjk = "币" * 141
        self.assertFalse(is_within_x_limit(long_cjk, max_units=280, max_raw_chars=140))
        # 145 ASCII chars => > 140 raw chars even though weighted units = 145 <= 280
        ascii_145 = "a" * 145
        self.assertTrue(is_within_x_limit(ascii_145, max_units=280))
        self.assertFalse(is_within_x_limit(ascii_145, max_units=280, max_raw_chars=140))


class TestPostUtils(unittest.TestCase):
    def test_assert_not_x_myself_dir(self):
        for bad in (
            "x_myself",
            r"W:\project\python_project\ai_content_pipeline\browser_sessions\x_myself",
            "/opt/sessions/X_MYSELF/Default",
        ):
            with self.assertRaises(AccountIsolationError):
                assert_not_x_myself_dir(bad, context="unit_test")

        # Empty (zero-auth) and burner dirs must be allowed
        assert_not_x_myself_dir("", context="unit_test")
        assert_not_x_myself_dir(None, context="unit_test")
        assert_not_x_myself_dir(r"W:\project\sessions\x_xiaohao", context="unit_test")

    def test_is_fetchable_external_article_url(self):
        self.assertTrue(is_fetchable_external_article_url("https://coindesk.com/markets/btc-update"))
        self.assertFalse(is_fetchable_external_article_url("https://x.com/someone/status/123"))
        self.assertFalse(is_fetchable_external_article_url("https://www.binance.com/zh-CN/square/post/1"))
        self.assertFalse(is_fetchable_external_article_url("https://cdn.example.com/report.pdf"))
        self.assertFalse(is_fetchable_external_article_url("not-a-url"))

    def test_is_video_post(self):
        self.assertFalse(is_video_post({"card_type": "BUZZ_SHORT", "media": {"images": ["a.jpg"]}}))
        self.assertTrue(is_video_post({"media": {"video_url": "https://v.mp4"}}))
        self.assertTrue(is_video_post({"media": {"video_duration": 15}}))
        self.assertTrue(is_video_post({"card_type": "BUZZ_VIDEO"}))
        self.assertTrue(is_video_post({"metadata": {"platform_extra": {"quoted_post": {"has_video": True}}}}))

    def test_get_unified_post_text(self):
        post = {
            "content": {
                "title": "Market Pulse",
                "text_content": "BTC broke resistance\n\n[插图: https://img.example/1.png]",
            },
            "metadata": {
                "external_article": {
                    "title": "Deep Dive",
                    "site_domain": "example.org",
                    "text": "Full body content.",
                }
            },
            "comments": [{"content": "Great point!"}],
        }
        unified = get_unified_post_text(post, include_comments=True)
        self.assertIn("【标题】Market Pulse", unified)
        self.assertIn("BTC broke resistance", unified)
        self.assertNotIn("[插图:", unified)
        self.assertIn("Full body content.", unified)
        self.assertIn("Great point!", unified)


class TestLLMGateway(unittest.TestCase):
    def test_render_prompt_template_supports_double_and_single_braces(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "tpl.md"
            p.write_text('Hello {{USER}}, focus on {TOPIC}. JSON sample: {"ok": true}', encoding="utf-8")
            rendered, meta = render_prompt_template("tpl.md", {"USER": "Alice", "TOPIC": "BTC"}, prompt_dir=tmp)
            self.assertIn("Hello Alice, focus on BTC.", rendered)
            self.assertIn('{"ok": true}', rendered)
            self.assertEqual(meta["prompt_file"], "tpl.md")
            self.assertTrue(meta["prompt_sha1"])

    def test_parse_llm_json(self):
        self.assertEqual(parse_llm_json('```json\n{"a": 1, "b": [2, 3],}\n```'), {"a": 1, "b": [2, 3]})
        self.assertIsNone(parse_llm_json("not json at all"))

    def test_call_llm_json_retries_schema_and_forwards_model_used(self):
        responses = [
            {"status": "✅ 成功", "content": '{"wrong": 1}', "metrics": {"model_used": "model-a"}, "trace_id": "t1"},
            {"status": "✅ 成功", "content": '```json\n{"ok": true, "val": 42}\n```', "metrics": {"model_used": "model-b"}, "trace_id": "t2"},
        ]
        captured_kwargs = []

        def _fake_generate_content(**kwargs):
            captured_kwargs.append(kwargs)
            return responses.pop(0)

        with patch("common.llm_gateway.generate_content", side_effect=_fake_generate_content):
            parsed, trace = call_llm_json(
                prompt="return json",
                model=None,
                preset_model_group="medium",
                validator=lambda d: None if "ok" in d else "missing ok key",
                max_attempts=3,
                task_label="test",
            )

        self.assertEqual(parsed, {"ok": True, "val": 42})
        self.assertTrue(trace["ok"])
        self.assertEqual(trace["model_used"], "model-b")
        self.assertEqual(trace["schema_attempt"], 2)
        self.assertEqual(len(captured_kwargs), 2)
        for kw in captured_kwargs:
            self.assertIsNone(kw["model"])
            self.assertEqual(kw["preset_model_group"], "medium")


class TestDaemonAndSelfPost(unittest.TestCase):
    def test_run_daemon_loop_max_rounds_exception_and_dynamic_sleep(self):
        calls = []
        sleeps = []

        def _step(round_idx: int):
            calls.append(round_idx)
            if round_idx == 1:
                raise RuntimeError("transient boom")
            if round_idx == 2:
                return {"next_sleep_seconds": 12.5}
            return None

        test_logger = logging.getLogger("test_daemon_loop")
        with patch("common.daemon.interruptible_sleep", side_effect=lambda sec, **_: sleeps.append(sec) or True):
            with self.assertLogs(test_logger, level="ERROR"):
                ran = run_daemon_loop(
                    _step,
                    interval_seconds=60,
                    error_backoff_seconds=5,
                    max_rounds=3,
                    logger_obj=test_logger,
                )

        self.assertEqual(ran, 3)
        self.assertEqual(calls, [1, 2, 3])
        # Round 3 hits max_rounds so sleep only runs after rounds 1 and 2
        self.assertEqual(sleeps, [5.0, 12.5])

    def test_run_daemon_loop_stop_event(self):
        stop = threading.Event()
        calls = []

        def _step(round_idx: int):
            calls.append(round_idx)
            stop.set()
            return {"next_sleep_seconds": 300}

        ran = run_daemon_loop(_step, interval_seconds=100, stop_event=stop)
        self.assertEqual(ran, 1)
        self.assertEqual(calls, [1])

    def test_compute_jittered_interval(self):
        val = compute_jittered_interval(100, additive_jitter_range=(10, 20))
        self.assertGreaterEqual(val, 110.0)
        self.assertLessEqual(val, 120.0)

    def test_is_self_post_across_platforms(self):
        self.assertTrue(is_self_post({"source": "binance", "author": {"username": "ruivision"}}))
        self.assertTrue(is_self_post({"source": "okx", "author": {"authorId": "872834593500823552"}}))
        self.assertTrue(is_self_post({"source": "x", "author": {"username": "@TestSelfHandle"}}, extra_x_handles={"testselfhandle"}))
        self.assertFalse(is_self_post({"source": "okx", "author": {"authorId": "999999999999", "nickName": "someone_else"}}))


if __name__ == "__main__":
    unittest.main()
