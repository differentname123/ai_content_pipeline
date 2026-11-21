# -*- coding: utf-8 -*-
"""X & platform automation unit tests: X retry safety, Binance WAF, x_api isolation/fallback, x_auto_publisher, x_growth."""
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from common.post_utils import AccountIsolationError
from platforms.binance_playwright import (
    _detect_binance_logged_out,
    _detect_binance_waf_or_challenge,
    _run_binance_browser_action,
)
from platforms.playwright_base import ChallengeRequiredError
from platforms.x_api import (
    build_x_selection_prompt,
    extract_domain_cookies_from_user_data_dir,
    fetch_x_tweet_zero_auth,
)
from platforms.x_auto_publisher import run_single_check_cycle, validate_double_gates
from platforms.x_playwright import _submit_x_post
from x_growth.comment_publisher import _filter_and_rank_candidates, sync_recent_comment_metrics
from x_growth.x_browser_collector import collect_and_save_posts


class _FakeLocator:
    def __init__(self, *, text="hello x", visible=True, enabled=True, count=1, aria_disabled=None, media_count=0):
        self._text = text
        self._visible = visible
        self._enabled = enabled
        self._count = count
        self._aria_disabled = aria_disabled
        self._media_count = media_count
        self.click_calls = 0
        self.first = self

    def count(self):
        return self._count

    def is_visible(self, **_kwargs):
        return self._visible

    def is_enabled(self):
        return self._enabled

    def get_attribute(self, attr):
        if attr == "aria-disabled":
            return self._aria_disabled
        if attr == "href":
            return None
        return None

    def inner_text(self):
        return self._text

    def click(self, **_kwargs):
        self.click_calls += 1

    def evaluate(self, _js):
        self.click_calls += 1
        return True

    def locator(self, selector):
        if "img" in selector or "attachments" in selector:
            return _FakeLocator(count=self._media_count, visible=False)
        return self

    def filter(self, **_kwargs):
        return self


class TestXPlaywrightSubmitRetry(unittest.TestCase):
    def _run_submit_with_watcher_factory(self, watcher_factory):
        send_btn = _FakeLocator(text="Post", visible=True, enabled=True)
        editor_box = _FakeLocator(text="hello x", visible=True, enabled=True)

        class _FakePage:
            url = "https://x.com/home"
            keyboard = MagicMock()

            def wait_for_timeout(self, _ms):
                pass

            def locator(self, _sel):
                return _FakeLocator(count=0, visible=False)

        page = _FakePage()
        container = MagicMock()
        container.locator.return_value = send_btn

        with patch("platforms.x_playwright._dismiss_overlays"), \
             patch("platforms.x_playwright.report_guard_hits"), \
             patch("platforms.x_playwright._wake_x_editor", return_value=editor_box), \
             patch("platforms.x_playwright._force_caret_to_tail"), \
             patch("platforms.x_playwright._type_x_body"), \
             patch("platforms.x_playwright._dismiss_x_typeahead_if_open"), \
             patch("platforms.x_playwright._pick_trusted_send_button", return_value=send_btn), \
             patch("platforms.x_playwright.expect"), \
             patch("platforms.x_playwright._robust_click", side_effect=lambda btn: btn.click()), \
             patch("platforms.x_playwright._XApiWatcher", side_effect=watcher_factory):
            post_id = _submit_x_post(page, container, content="hello x", prepared_images=[])
        return post_id, send_btn

    def test_safe_retry_triggers_only_when_zero_requests_fired(self):
        created_watchers = []

        def _factory(_page):
            w = MagicMock()
            w.primary = []
            w.primary_request_count = 0
            # First wait_primary(6000) misses response; after retry, watcher sees CreateTweet
            def _wait_primary(_ms):
                if w.wait_primary.call_count == 1:
                    return None
                w.primary_request_count = 1
                return SimpleNamespace(status=200, url="https://x.com/i/api/graphql/abc/CreateTweet")

            w.wait_primary.side_effect = _wait_primary
            created_watchers.append(w)
            return w

        with patch("platforms.x_playwright._read_x_api_verdict", return_value=(True, "999888777666555")):
            pid, btn = self._run_submit_with_watcher_factory(_factory)

        self.assertEqual(pid, "999888777666555")
        self.assertEqual(btn.click_calls, 2)  # 1 initial + 1 safe retry

    def test_no_retry_when_primary_request_already_in_flight(self):
        def _factory(_page):
            w = MagicMock()
            w.primary = []
            w.primary_request_count = 1  # Already sent over network! Must NEVER double-click!
            w.wait_primary.return_value = None
            return w

        with patch("platforms.x_playwright._read_x_api_verdict", return_value=(True, "111222333444555")):
            pid, btn = self._run_submit_with_watcher_factory(_factory)

        self.assertEqual(pid, "111222333444555")
        self.assertEqual(btn.click_calls, 1)  # Strictly single click!


class TestBinanceWafDetection(unittest.TestCase):
    def test_detects_http_405_and_human_challenge_text(self):
        fake_page = MagicMock()
        waf_405 = _detect_binance_waf_or_challenge(fake_page, response=SimpleNamespace(status=405))
        self.assertIsNotNone(waf_405)
        self.assertIn("405", waf_405)

        fake_page.evaluate.return_value = "安全验证：我们需要确认您是人类，请完成检查"
        waf_text = _detect_binance_waf_or_challenge(fake_page, response=SimpleNamespace(status=200))
        self.assertIsNotNone(waf_text)
        self.assertIn("WAF", waf_text)

        fake_page.evaluate.return_value = "币安广场正常内容"
        self.assertIsNone(_detect_binance_waf_or_challenge(fake_page, response=SimpleNamespace(status=200)))

    def test_detects_binance_logged_out_modal_and_signup_widget(self):
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

        # Case 1: Session expired modal text
        page_modal = MagicMock()
        page_modal.evaluate.return_value = "需要验证 登录会话已超时，请完成安全验证以保持登录状态 执行安全验证"
        err_modal = _detect_binance_logged_out(page_modal, wait_ms=10)
        self.assertIsNotNone(err_modal)
        self.assertIn("会话已超时", err_modal)

        # Case 2: Signup widget visible in DOM
        page_widget = MagicMock()
        page_widget.evaluate.return_value = "币安广场"
        page_widget.locator.return_value.first.wait_for.return_value = None
        err_widget = _detect_binance_logged_out(page_widget, wait_ms=10)
        self.assertIsNotNone(err_widget)
        self.assertIn("登录", err_widget)

        # Case 3: Logged-in healthy page (no modal text, signup widget times out)
        page_ok = MagicMock()
        page_ok.evaluate.return_value = "币安广场 写点什么吧"
        page_ok.locator.return_value.first.wait_for.side_effect = PlaywrightTimeoutError("timeout")
        self.assertIsNone(_detect_binance_logged_out(page_ok, wait_ms=10))

    def test_waf_error_prefix_is_not_duplicated(self):
        class _DummySession:
            def __enter__(self):
                return MagicMock(), MagicMock()

            def __exit__(self, *args):
                return False

        def _raise_prefixed(_ctx, _page):
            raise ChallengeRequiredError("[WAF/人机验证] 广场页面返回 HTTP 405")

        with patch("platforms.binance_playwright.persistent_browser_session", return_value=_DummySession()):
            err, ok, pid = _run_binance_browser_action(
                "dummy_dir",
                debug=False,
                forensic_tag="tag",
                forensic_url="https://example",
                fn=_raise_prefixed,
            )

        self.assertFalse(ok)
        self.assertIsNone(pid)
        self.assertEqual(err.count("[WAF/人机验证]"), 1)


class TestXApiAndAutoPublisher(unittest.TestCase):
    def test_x_api_blocks_x_myself_cookie_extraction(self):
        with self.assertRaises(AccountIsolationError):
            extract_domain_cookies_from_user_data_dir(r"W:\sessions\x_myself", ["https://x.com"])

    def test_build_x_selection_prompt_renders_and_saves(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "prompt_out.txt"
            sample_post = {
                "post_id": "1900000000000000001",
                "publish_time": 1_700_000_000,
                "publish_time_str": "2026-11-20 12:00:00",
                "author": {"username": "peterzhu1003", "followers_count": 1200},
                "content": {"text_content": "$BTC 突破回踩确认"},
                "engagement": {"view_count": 5000, "like_count": 20, "comment_count": 3, "share_count": 2, "bookmark_count": 4, "engagement_rate_pct": 0.58},
                "media": {"images": []},
                "account_alias": "myself",
                "account_screen_name": "peterzhu1003",
            }
            prompt_text = build_x_selection_prompt(
                accounts=[{"alias": "myself", "screen_name": "peterzhu1003"}],
                recent_count=10,
                posts_data=[sample_post],
                candidate_posts=[{"remodeled_article": {"usable": True, "id": "cand_1", "post_text": "候选爆款测试"}}],
                save_prompt_path=str(out_path),
            )
            self.assertNotIn("{HISTORICAL_X_POSTS}", prompt_text)
            self.assertNotIn("{CANDIDATE_POSTS}", prompt_text)
            self.assertIn("$BTC 突破回踩确认", prompt_text)
            self.assertIn("候选爆款测试", prompt_text)
            self.assertTrue(out_path.exists())
            self.assertEqual(out_path.read_text(encoding="utf-8"), prompt_text)

    def test_fetch_x_tweet_zero_auth_syndication_fallback(self):
        syn_payload = {
            "id_str": "1888888888888888888",
            "text": "Bitcoin ETF inflows accelerating today",
            "created_at": "Thu Nov 20 12:00:00 +0000 2026",
            "user": {"id_str": "42", "screen_name": "satoshi_test", "name": "Satoshi", "followers_count": 500},
            "favorite_count": 12,
            "conversation_count": 3,
        }
        mock_resp = SimpleNamespace(status_code=200, json=lambda: syn_payload)
        with patch("platforms.x_api.fetch_x_single_post_raw", return_value=None), \
             patch("platforms.x_api.requests.get", return_value=mock_resp):
            parsed = fetch_x_tweet_zero_auth("1888888888888888888", screen_name="satoshi_test", proxies={})
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["post_id"], "1888888888888888888")
        self.assertEqual(parsed["engagement"]["like_count"], 12)
        self.assertEqual(parsed["engagement"]["comment_count"], 3)

    def test_x_auto_publisher_double_gate_and_cooldown_sleep(self):
        ok, _, raw_len, units = validate_double_gates(
            {"id": "1", "grade": "S", "final_x_text": "$BTC 链上筹码稳定，短期洗盘接近尾声。"}
        )
        self.assertTrue(ok)
        self.assertLessEqual(raw_len, 140)
        self.assertLessEqual(units, 280)

        bad_grade = validate_double_gates({"id": "2", "grade": "A", "final_x_text": "$BTC short"})
        self.assertFalse(bad_grade[0])

        oversized = validate_double_gates({"id": "3", "grade": "S", "final_x_text": "币" * 141})
        self.assertFalse(oversized[0])

        with patch("platforms.x_auto_publisher.TARGET_ACCOUNTS", [{"alias": "myself", "screen_name": "peter", "session_dir": "x"}]), \
             patch("platforms.x_auto_publisher.fetch_unposted_viral_candidates") as mock_fetch:
            res = run_single_check_cycle(
                {"myself": {"last_publish_time": time.time() - 100}},
                dry_run=False,
                cooldown_seconds=10800,
            )
        mock_fetch.assert_not_called()
        self.assertEqual(res["status"], "cooldown")
        self.assertGreater(res["next_sleep_seconds"], 10600)


class TestXGrowth(unittest.TestCase):
    def test_collector_blocks_x_myself_before_browser(self):
        with tempfile.TemporaryDirectory() as tmp:
            myself_dir = Path(tmp) / "x_myself"
            myself_dir.mkdir()
            with patch("x_growth.x_browser_collector.persistent_browser_session") as mock_browser:
                with self.assertRaises(AccountIsolationError):
                    collect_and_save_posts(user_data_dir=str(myself_dir))
                mock_browser.assert_not_called()

    def test_sync_recent_comment_metrics_uses_zero_auth(self):
        fake_doc = {
            "post_id": "1001",
            "author": {"username": "target_user"},
            "auto_comment": {
                "consumed": True,
                "consumed_by": "@peterzhu1003",
                "published_comment_id": "2002",
                "comment_engagement": {},
            },
        }
        mock_col = MagicMock()
        cursor = MagicMock()
        cursor.sort.return_value.limit.return_value = [fake_doc]
        mock_col.find.return_value = cursor
        mock_db = MagicMock()
        mock_db.get_collection.return_value = mock_col

        fake_parsed = {"engagement": {"view_count": 320, "like_count": 5, "comment_count": 1}}
        with patch("x_growth.comment_publisher.gen_db_object", return_value=mock_db), \
             patch("x_growth.comment_publisher.fetch_x_tweet_zero_auth", return_value=fake_parsed) as mock_zero:
            res = sync_recent_comment_metrics(lookback_hours=72, min_refresh_interval_sec=0)

        mock_zero.assert_called_once()
        self.assertEqual(mock_zero.call_args.args[0], "2002")
        self.assertEqual(res["synced_count"], 1)
        mock_col.update_one.assert_called_once()

    def test_candidate_filter_and_ranking_respects_replies_15_and_priority(self):
        now = 1_700_000_000
        docs = [
            {
                "post_id": "crowded",
                "publish_time": now - 600,
                "engagement": {"like_count": 20, "comment_count": 16},
                "auto_comment": {"best_score": 95},
            },
            {
                "post_id": "stale",
                "publish_time": now - 8000,
                "engagement": {"like_count": 0, "comment_count": 0},
                "auto_comment": {"best_score": 90},
            },
            {
                "post_id": "good_1",
                "publish_time": now - 1200,
                "engagement": {"like_count": 4, "comment_count": 2},
                "auto_comment": {"best_score": 82},
            },
            {
                "post_id": "good_0_replies",
                "publish_time": now - 1000,
                "engagement": {"like_count": 3, "comment_count": 0},
                "auto_comment": {"best_score": 80},
            },
        ]
        picked, stats = _filter_and_rank_candidates(
            docs,
            now_ts=now,
            max_post_comments=15,
            stale_zero_engagement_seconds=7200,
            stale_min_likes=1,
        )
        self.assertEqual(stats["filtered_crowded"], 1)
        self.assertEqual(stats["filtered_stale"], 1)
        self.assertEqual([d["post_id"] for d in picked], ["good_0_replies", "good_1"])


if __name__ == "__main__":
    unittest.main()
