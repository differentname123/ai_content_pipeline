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
from platforms.x_auto_publisher import (
    build_account_historical_posts_block,
    build_candidate_prompt_data,
    rank_and_trim_x_candidates,
    run_single_check_cycle,
    validate_double_gates,
)
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

        def _make_auth_page(*, body_text, visible_ok, hidden_ok, editor_visible):
            login_probe = MagicMock()

            def _wait_for(*, state="visible", timeout=None):
                if state == "visible" and not visible_ok:
                    raise PlaywrightTimeoutError("timeout")
                if state == "hidden" and not hidden_ok:
                    raise PlaywrightTimeoutError("timeout")

            login_probe.wait_for.side_effect = _wait_for
            login_loc = MagicMock()
            login_loc.first = login_probe

            editor_probe = MagicMock()
            editor_probe.is_visible.return_value = editor_visible
            editor_loc = MagicMock()
            editor_loc.first = editor_probe

            page = MagicMock()
            page.evaluate.return_value = body_text
            page.locator.side_effect = lambda sel: editor_loc if "short-editor-inner" in sel else login_loc
            return page

        # Case 1: Session expired modal text
        page_modal = _make_auth_page(
            body_text="需要验证 登录会话已超时，请完成安全验证以保持登录状态 执行安全验证",
            visible_ok=False,
            hidden_ok=True,
            editor_visible=False,
        )
        err_modal = _detect_binance_logged_out(page_modal, wait_ms=10, settle_ms=10)
        self.assertIsNotNone(err_modal)
        self.assertIn("会话已超时", err_modal)

        # Case 2: Truly logged-out persistent signup widget (stays visible and no post editor)
        page_widget = _make_auth_page(
            body_text="币安广场",
            visible_ok=True,
            hidden_ok=False,
            editor_visible=False,
        )
        err_widget = _detect_binance_logged_out(page_widget, wait_ms=10, settle_ms=10)
        self.assertIsNotNone(err_widget)
        self.assertIn("登录", err_widget)

        # Case 3: SSR hydration flash (signup widget disappears within settle_ms)
        page_ssr_flash = _make_auth_page(
            body_text="币安广场",
            visible_ok=True,
            hidden_ok=True,
            editor_visible=False,
        )
        self.assertIsNone(_detect_binance_logged_out(page_ssr_flash, wait_ms=10, settle_ms=10))

        # Case 4: Slow hydration where signup widget lingers, but post editor is already visible
        page_slow_hydration = _make_auth_page(
            body_text="币安广场 写点什么吧",
            visible_ok=True,
            hidden_ok=False,
            editor_visible=True,
        )
        self.assertIsNone(_detect_binance_logged_out(page_slow_hydration, wait_ms=10, settle_ms=10))

        # Case 5: Healthy logged-in page (no modal text, signup widget times out)
        page_ok = _make_auth_page(
            body_text="币安广场 写点什么吧",
            visible_ok=False,
            hidden_ok=True,
            editor_visible=True,
        )
        self.assertIsNone(_detect_binance_logged_out(page_ok, wait_ms=10, settle_ms=10))

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

    def test_x_auto_publisher_rank_trim_split_history_and_compact_candidates(self):
        # 1. rank_and_trim_x_candidates trims to top N using viral_score + source engagement
        cands = [
            {
                "article_id": f"art_{i}",
                "post_id_list": [f"p_{i}"],
                "creation_brief": {"original_post": {"text_content": f"原帖人话 {i} [插图: https://a.com/{i}.jpg]"}},
                "article_info": {
                    "viral_score": 6.0 + (i % 4),
                    "topic_tag": "搞钱与盈亏",
                    "post_text": f"改写稿 {i}",
                    "image_semantics": [
                        {"id": "[IMAGE_1]", "semantic_role": "核心实盘图", "keep": True},
                        {"id": "[IMAGE_2]", "semantic_role": "废图", "keep": False},
                    ],
                },
                "_src_engagement": {"view_count": 1000 * (i + 1), "like_count": i, "comment_count": 0},
            }
            for i in range(10)
        ]
        from platforms.x_auto_publisher import MAX_LLM_CANDIDATES

        self.assertEqual(MAX_LLM_CANDIDATES, 50)
        with patch("platforms.x_auto_publisher.UniversalPostManager") as mock_pm:
            mock_pm.return_value.find_posts.return_value = []
            trimmed = rank_and_trim_x_candidates(cands, max_candidates=4, now_ts=1_800_000_000.0)
        self.assertEqual(len(trimmed), 4)
        self.assertGreaterEqual(trimmed[0]["_src_views"], trimmed[-1]["_src_views"])

        # Stale penalty applies past 24h, not at 20h
        age_cands = [
            {"article_id": "a_20h", "post_id_list": ["p20"], "article_info": {"viral_score": 8.0}, "_src_engagement": {"view_count": 5000}, "_src_publish_time": 1_800_000_000 - 20 * 3600},
            {"article_id": "a_30h", "post_id_list": ["p30"], "article_info": {"viral_score": 8.0}, "_src_engagement": {"view_count": 5000}, "_src_publish_time": 1_800_000_000 - 30 * 3600},
        ]
        with patch("platforms.x_auto_publisher.UniversalPostManager") as mock_pm:
            mock_pm.return_value.find_posts.return_value = []
            age_ranked = rank_and_trim_x_candidates(age_cands, max_candidates=2, now_ts=1_800_000_000.0)
        self.assertEqual([c["article_id"] for c in age_ranked], ["a_20h", "a_30h"])

        plist, cmap = build_candidate_prompt_data(trimmed)
        self.assertEqual(len(plist), 4)
        self.assertIn(plist[0]["id"], cmap)
        self.assertIn("original_text", plist[0])
        self.assertNotIn("[插图:", plist[0]["original_text"])
        self.assertEqual(len(plist[0]["kept_images"]), 1)
        self.assertEqual(plist[0]["kept_images"][0]["id"], "[IMAGE_1]")

        # 2. build_account_historical_posts_block physically splits anti-dup vs cross-account benchmarks
        fake_posts = {
            "peterzhu1003": [
                {
                    "post_id": "111",
                    "publish_time": 1_800_000_000,
                    "publish_time_str": "2026-11-21 10:00:00",
                    "author": {"username": "peterzhu1003", "followers_count": 17},
                    "content": {"text_content": "本号自己发过的旧闻"},
                    "engagement": {"view_count": 40, "like_count": 1, "comment_count": 0, "share_count": 0, "bookmark_count": 0, "engagement_rate_pct": 2.5},
                    "media": {"images": []},
                }
            ],
            "2434954452qqc": [
                {
                    "post_id": "222",
                    "publish_time": 1_799_990_000,
                    "publish_time_str": "2026-11-21 08:00:00",
                    "author": {"username": "2434954452qqc", "followers_count": 665},
                    "content": {"text_content": "对照号跑出的大爆款"},
                    "engagement": {"view_count": 80000, "like_count": 120, "comment_count": 50, "share_count": 10, "bookmark_count": 30, "engagement_rate_pct": 0.26},
                    "media": {"images": []},
                }
            ],
        }
        with patch("platforms.x_auto_publisher.fetch_multi_x_user_posts", return_value=fake_posts):
            hist_block = build_account_historical_posts_block({"alias": "myself", "screen_name": "peterzhu1003"})
        self.assertIn("<target_account_posted_history>", hist_block)
        self.assertIn("<cross_account_viral_benchmarks>", hist_block)
        self.assertIn("本号自己发过的旧闻", hist_block.split("</target_account_posted_history>")[0])
        self.assertNotIn("对照号跑出的大爆款", hist_block.split("</target_account_posted_history>")[0])
        self.assertIn("对照号跑出的大爆款", hist_block.split("<cross_account_viral_benchmarks>")[1])


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
