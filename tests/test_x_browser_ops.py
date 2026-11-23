# -*- coding: utf-8 -*-
"""Comprehensive unit tests for clean X browser engine & unified XOpsDaemon."""
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
import io
import pathlib
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

import platforms.x_ops_daemon as x_ops_daemon_mod

from platforms.x_browser import (
    BusinessErrorException,
    ChallengeRequiredError,
    XAccountCircuitBreaker,
    assert_x_page_healthy,
    human_click,
    human_type,
    inspect_x_page_health,
)
from platforms.x_playwright import (
    _detect_x_response_risk,
    _dismiss_x_overlays,
    follow_on_x_page,
    pin_on_x_page,
)
from platforms.x_ops_daemon import (
    DEFAULT_ACCOUNT,
    XAccountStateStore,
    XOpsDaemon,
    enqueue_x_action,
    get_x_action,
    register_action_handler,
    wait_x_action,
)

TZ_CN = timezone(timedelta(hours=8))


def _ts_cn(year: int, month: int, day: int, hour: int, minute: int = 0) -> int:
    return int(datetime(year, month, day, hour, minute, 0, tzinfo=TZ_CN).timestamp())


def _encode_snowflake(ts_sec: int, seq: int = 1) -> str:
    ms_since_epoch = int(ts_sec) * 1000 - 1288834974657
    return str((ms_since_epoch << 22) | (seq & 0x3FFFFF))


class _FakeCursor:
    def __init__(self, docs):
        self._docs = list(docs)

    def sort(self, key_list):
        if not key_list:
            return self
        items = list(self._docs)
        for field, direction in reversed(key_list):
            items.sort(key=lambda d: d.get(field) or 0, reverse=(direction < 0))
        return _FakeCursor(items)

    def __iter__(self):
        return iter(self._docs)


def _match_query(doc, query):
    for k, v in (query or {}).items():
        val = doc.get(k)
        if isinstance(v, dict) and "$lte" in v:
            if val is None or val > v["$lte"]:
                return False
        elif val != v:
            return False
    return True


class _InMemoryCollection:
    def __init__(self):
        self.docs = []

    def create_index(self, *_args, **_kwargs):
        return None

    def find_one(self, query=None):
        for d in self.docs:
            if _match_query(d, query):
                return dict(d)
        return None

    def find(self, query=None):
        return _FakeCursor([dict(d) for d in self.docs if _match_query(d, query)])

    def insert_one(self, doc):
        self.docs.append(dict(doc))

    def update_one(self, query, update, upsert=False):
        target = None
        for d in self.docs:
            if _match_query(d, query):
                target = d
                break
        if target is None:
            if not upsert:
                return
            target = dict(query or {})
            self.docs.append(target)
        for k, v in (update.get("$set") or {}).items():
            target[k] = v

    def count_documents(self, query=None):
        return sum(1 for d in self.docs if _match_query(d, query))


class _FakeDB:
    def __init__(self):
        self.cols = {}

    def get_collection(self, name):
        if name not in self.cols:
            self.cols[name] = _InMemoryCollection()
        return self.cols[name]


class _FakeMouse:
    def __init__(self):
        self.events = []

    def move(self, x, y, steps=1):
        self.events.append(("move", x, y, steps))

    def down(self):
        self.events.append(("down",))

    def up(self):
        self.events.append(("up",))

    def wheel(self, dx, dy):
        self.events.append(("wheel", dx, dy))


class _FakeKeyboard:
    def __init__(self):
        self.pressed = []
        self.insert_called = False

    def press(self, key):
        self.pressed.append(key)

    def insert_text(self, _text):
        self.insert_called = True
        raise AssertionError("insert_text must NEVER be called by human_type")


class _FakeSession:
    def __init__(self, page=None):
        self.page = page or MagicMock()
        self.alive = False
        self.started_count = 0
        self.closed_count = 0
        self.goto_home_count = 0

    def is_alive(self):
        return self.alive

    def ensure_alive(self):
        if not self.alive:
            self.alive = True
            self.started_count += 1
        return self.page

    def goto_home(self, wait_ms=0):
        self.goto_home_count += 1
        return self.page

    def close(self):
        if self.alive:
            self.alive = False
            self.closed_count += 1


class TestXBrowserPrimitives(unittest.TestCase):
    def test_circuit_breaker_trip_blocks_expiry_and_clear(self):
        db = _FakeDB()
        breaker = XAccountCircuitBreaker(db, default_freeze_seconds=48 * 3600)

        frozen, rem, _ = breaker.is_frozen("x_myself", now_ts=1_000_000)
        self.assertFalse(frozen)
        self.assertEqual(rem, 0)

        until_ts = breaker.trip("x_myself", reason="locked_challenge", url="https://x.com/account/access", now_ts=1_000_000)
        self.assertEqual(until_ts, 1_000_000 + 48 * 3600)

        frozen2, rem2, reason2 = breaker.is_frozen("x_myself", now_ts=1_001_000)
        self.assertTrue(frozen2)
        self.assertEqual(rem2, 48 * 3600 - 1000)
        self.assertIn("locked_challenge", reason2)

        # Expires automatically after deadline
        frozen3, _, _ = breaker.is_frozen("x_myself", now_ts=until_ts + 1)
        self.assertFalse(frozen3)

        # Explicit clear clears immediately
        breaker.trip("x_myself", reason="test", now_ts=2_000_000)
        breaker.clear("x_myself", now_ts=2_000_010)
        frozen4, _, _ = breaker.is_frozen("x_myself", now_ts=2_000_020)
        self.assertFalse(frozen4)

    def test_inspect_x_page_health_and_assert_healthy(self):
        def _make_page(url, visible_map=None, text_map=None):
            vmap = visible_map or {}
            tmap = text_map or {}
            page = MagicMock()
            page.url = url

            def _locator(sel):
                loc = MagicMock()
                loc.first = loc
                is_vis = bool(vmap.get(sel, False))
                loc.count.return_value = 1 if (sel in vmap or sel in tmap) and (is_vis or sel in tmap) else 0
                loc.is_visible.return_value = is_vis or (sel in tmap)
                loc.inner_text.return_value = tmap.get(sel, "")
                return loc

            page.locator.side_effect = _locator
            return page

        # Healthy page
        ok_page = _make_page("https://x.com/home")
        self.assertEqual(inspect_x_page_health(ok_page), ("ok", ""))

        # /account/access redirect
        ch_page = _make_page("https://x.com/account/access")
        st, _ = inspect_x_page_health(ch_page)
        self.assertEqual(st, "locked_challenge")

        # Arkose & Cloudflare DOM iframes
        ark_page = _make_page("https://x.com/home", visible_map={'iframe[src*="arkoselabs"]': True})
        self.assertEqual(inspect_x_page_health(ark_page)[0], "locked_challenge")
        cf_page = _make_page("https://x.com/home", visible_map={'iframe[src*="challenges.cloudflare.com"]': True})
        self.assertEqual(inspect_x_page_health(cf_page)[0], "locked_challenge")

        # Logged out flow
        login_page = _make_page("https://x.com/i/flow/login")
        self.assertEqual(inspect_x_page_health(login_page)[0], "not_logged_in")

        # Suspended text
        susp_page = _make_page("https://x.com/home", text_map={'main[role="main"]': "Your account is suspended."})
        self.assertEqual(inspect_x_page_health(susp_page)[0], "suspended")

        # assert_x_page_healthy trips breaker on challenge
        db = _FakeDB()
        breaker = XAccountCircuitBreaker(db)
        with self.assertRaises(ChallengeRequiredError):
            assert_x_page_healthy(ch_page, account_name="x_myself", breaker=breaker)
        frozen, _, _ = breaker.is_frozen("x_myself")
        self.assertTrue(frozen)

    def test_human_click_uses_physical_mouse_and_never_calls_evaluate(self):
        page = MagicMock()
        page.mouse = _FakeMouse()
        page.wait_for_timeout = MagicMock()

        locator = MagicMock()
        locator.bounding_box.return_value = {"x": 100.0, "y": 200.0, "width": 80.0, "height": 40.0}
        locator.evaluate.side_effect = AssertionError("JS evaluate() click must NEVER be called!")

        human_click(page, locator)

        kinds = [ev[0] for ev in page.mouse.events]
        self.assertEqual(kinds, ["move", "down", "up"])
        _, mx, my, steps = page.mouse.events[0]
        self.assertGreaterEqual(mx, 100.0 + 80.0 * 0.25)
        self.assertLessEqual(mx, 100.0 + 80.0 * 0.75)
        self.assertGreaterEqual(my, 200.0 + 40.0 * 0.25)
        self.assertLessEqual(my, 200.0 + 40.0 * 0.75)
        self.assertGreaterEqual(steps, 8)
        locator.evaluate.assert_not_called()

    def test_human_type_chunks_variable_delays_and_never_calls_insert_text(self):
        page = MagicMock()
        page.keyboard = _FakeKeyboard()
        page.wait_for_timeout = MagicMock()

        typed_chunks = []
        delays = []
        editor = MagicMock()

        def _seq(chunk, delay=0, timeout=0):
            typed_chunks.append(chunk)
            delays.append(delay)

        editor.press_sequentially.side_effect = _seq
        dismiss_calls = []

        human_type(
            page,
            editor,
            "Hello, $BTC world!\nLine two。",
            dismiss_typeahead_fn=lambda _p: dismiss_calls.append(1),
        )

        self.assertFalse(page.keyboard.insert_called)
        self.assertEqual("".join(typed_chunks), "Hello, $BTC world!Line two。")
        self.assertTrue(all(1 <= len(c) <= 3 for c in typed_chunks))
        self.assertTrue(all(45 <= d <= 135 for d in delays))
        editor.press.assert_called_once_with("Enter", timeout=3000)
        self.assertGreaterEqual(len(dismiss_calls), 3)


class TestXOpsDaemon(unittest.TestCase):
    def test_default_account_is_strictly_x_myself(self):
        self.assertEqual(DEFAULT_ACCOUNT["name"], "x_myself")
        self.assertNotIn("dahao", DEFAULT_ACCOUNT["user_data_dir"].lower())

    def test_custom_plugin_and_post_quotas_and_result_lookup(self):
        db = _FakeDB()
        base_ts = _ts_cn(2026, 11, 24, 10, 0)
        page = MagicMock()
        page.url = "https://x.com/home"
        page.locator.return_value.first.count.return_value = 0
        sess = _FakeSession(page=page)

        custom_hits = []
        sleeps = []
        register_action_handler("custom_ping", lambda ctx, item: custom_hits.append(item["payload"]) or {"status": "done", "result": {"pong": True}})
        daemon = XOpsDaemon(db=db, session=sess, sleep_fn=sleeps.append)

        # Enqueue 1 custom action + post + read-only sniff + 2 cooldown-blocked posts
        enqueue_x_action("x_myself", "custom_ping", {"v": 42}, db=db, now_ts=base_ts)
        id_p1 = enqueue_x_action("x_myself", "post", {"content": "First post"}, db=db, now_ts=base_ts + 1)
        id_sniff = enqueue_x_action("x_myself", "sniff_notifications", {"scroll_rounds": 1}, db=db, now_ts=base_ts + 2)
        id_p2 = enqueue_x_action("x_myself", "post", {"content": "Too soon deferred"}, db=db, now_ts=base_ts + 3)
        id_p2_nodelay = enqueue_x_action(
            "x_myself",
            "post",
            {"content": "Too soon skip", "defer_on_cooldown": False},
            db=db,
            now_ts=base_ts + 4,
        )

        with patch("platforms.x_ops_daemon.post_on_x_page", return_value="900000000000000001"), \
             patch("platforms.x_ops_daemon.sniff_notifications_on_page", return_value={"items": []}), \
             patch("platforms.x_ops_daemon._wait_x_home_ready"), \
             patch("platforms.x_ops_daemon.assert_x_page_healthy"):
            res = daemon.run_once(now_ts=base_ts + 10)

        self.assertEqual(res["executed"], 3)  # custom_ping + p1 + sniff_notifications
        self.assertEqual(res["deferred"], 1)  # p2 deferred (stays pending)
        self.assertEqual(res["skipped"], 1)   # p2_nodelay explicitly skipped
        self.assertEqual(custom_hits, [{"v": 42}])

        q_col = db.get_collection("x_ops_queue")
        self.assertEqual(q_col.find_one({"_id": id_p1})["status"], "done")
        self.assertEqual(q_col.find_one({"_id": id_sniff})["status"], "done")
        p2_doc = q_col.find_one({"_id": id_p2})
        self.assertEqual(p2_doc["status"], "pending")
        self.assertGreater(p2_doc["scheduled_at_ts"], base_ts + 7200)
        self.assertIn("post_interval_cooldown", p2_doc["error"])
        self.assertEqual(q_col.find_one({"_id": id_p2_nodelay})["status"], "skipped")
        self.assertEqual(get_x_action(id_p1, db=db)["result"]["post_id"], "900000000000000001")

        # Cancel pending deferred p2 before testing daily cap = 5 posts
        daemon._update_queue_item(id_p2, status="cancelled", now_ts=base_ts + 20)
        for i in range(4):
            daemon.state_store.record_action("x_myself", "post", now_ts=base_ts + (i + 1) * 8000)
        id_p_cap = enqueue_x_action("x_myself", "post", {"content": "6th post"}, db=db, now_ts=base_ts + 5 * 8000)
        with patch("platforms.x_ops_daemon.post_on_x_page", return_value="900000000000000009"):
            res_cap = daemon.run_once(now_ts=base_ts + 5 * 8000 + 5)
        self.assertEqual(res_cap["skipped"], 1)
        self.assertIn("post_daily_cap_reached", q_col.find_one({"_id": id_p_cap})["error"])

    def test_reply_quotas_age_gate_snowflake_and_like_false(self):
        db = _FakeDB()
        now_ts = _ts_cn(2026, 11, 24, 14, 0)
        page = MagicMock()
        page.url = "https://x.com/home"
        sess = _FakeSession(page=page)
        sleeps = []
        daemon = XOpsDaemon(db=db, session=sess, sleep_fn=sleeps.append)

        # 1. Old post decoded purely via Snowflake ID (>24h old) must be skipped (first item -> 0 sleep)
        old_root_id = _encode_snowflake(now_ts - 30 * 3600)
        id_old = enqueue_x_action(
            "x_myself",
            "reply",
            {"target_post_url": "111", "root_post_id": old_root_id, "reply_id": "111", "content": "late"},
            db=db,
            now_ts=now_ts,
        )

        # 2. Fresh post (<24h): allow up to 3 replies, force like=False, block 4th per post
        fresh_root_id = _encode_snowflake(now_ts - 2 * 3600)
        r_ids = [
            enqueue_x_action(
                "x_myself",
                "reply",
                {"target_post_url": f"20{i}", "root_post_id": fresh_root_id, "reply_id": f"20{i}", "content": f"r{i}"},
                db=db,
                now_ts=now_ts + 1 + i,
            )
            for i in range(4)
        ]

        captured_like_flags = []

        def _fake_reply(_page, **kwargs):
            captured_like_flags.append(kwargs.get("like"))
            return "999"

        with patch("platforms.x_ops_daemon.reply_on_x_page", side_effect=_fake_reply), \
             patch("platforms.x_ops_daemon._wait_x_home_ready"), \
             patch("platforms.x_ops_daemon.assert_x_page_healthy"):
            res = daemon.run_once(now_ts=now_ts + 10)

        self.assertEqual(res["executed"], 3)
        self.assertEqual(res["skipped"], 2)  # 1 old post + 1 over per-post cap (3)
        self.assertEqual(captured_like_flags, [False, False, False])
        q_col = db.get_collection("x_ops_queue")
        self.assertIn("parent_post_too_old", q_col.find_one({"_id": id_old})["error"])
        self.assertIn("per_post_reply_cap", q_col.find_one({"_id": r_ids[3]})["error"])

        # 3. 1h cap = 4 -> 5th deferred by default, skipped when defer_on_cooldown=False
        second_root = _encode_snowflake(now_ts - 1800)
        ok_4th = enqueue_x_action("x_myself", "reply", {"target_post_url": "301", "root_post_id": second_root, "reply_id": "301", "content": "ok"}, db=db, now_ts=now_ts + 20)
        blocked_5th = enqueue_x_action("x_myself", "reply", {"target_post_url": "302", "root_post_id": second_root, "reply_id": "302", "content": "defer"}, db=db, now_ts=now_ts + 21)
        blocked_5th_skip = enqueue_x_action(
            "x_myself",
            "reply",
            {"target_post_url": "303", "root_post_id": second_root, "reply_id": "303", "content": "skip", "defer_on_cooldown": False},
            db=db,
            now_ts=now_ts + 22,
        )
        with patch("platforms.x_ops_daemon.reply_on_x_page", side_effect=_fake_reply):
            res_hr = daemon.run_once(now_ts=now_ts + 30)
        self.assertEqual(res_hr["deferred"], 1)
        self.assertEqual(res_hr["skipped"], 1)
        self.assertEqual(q_col.find_one({"_id": ok_4th})["status"], "done")
        deferred_doc = q_col.find_one({"_id": blocked_5th})
        self.assertEqual(deferred_doc["status"], "pending")
        self.assertEqual(deferred_doc["scheduled_at_ts"], (now_ts + 10) + 3615)
        self.assertIn("reply_hourly_cap", deferred_doc["error"])
        self.assertEqual(q_col.find_one({"_id": blocked_5th_skip})["status"], "skipped")

    def test_reply_dedup_blocks_duplicate_reply_id(self):
        db = _FakeDB()
        now_ts = _ts_cn(2026, 11, 24, 15, 0)
        page = MagicMock()
        page.url = "https://x.com/home"
        sess = _FakeSession(page=page)
        daemon = XOpsDaemon(db=db, session=sess, sleep_fn=lambda _s: None)

        fresh_root = _encode_snowflake(now_ts - 600)
        r1 = enqueue_x_action(
            "x_myself",
            "reply",
            {"target_post_url": "https://x.com/u/status/777111", "root_post_id": fresh_root, "reply_id": "777111", "content": "first"},
            db=db,
            now_ts=now_ts,
        )
        r2 = enqueue_x_action(
            "x_myself",
            "reply",
            {"target_post_url": "https://x.com/u/status/777111", "root_post_id": fresh_root, "reply_id": "777111", "content": "dup"},
            db=db,
            now_ts=now_ts + 1,
        )
        with patch("platforms.x_ops_daemon.reply_on_x_page", return_value="888222") as mock_reply, \
             patch("platforms.x_ops_daemon._wait_x_home_ready"), \
             patch("platforms.x_ops_daemon.assert_x_page_healthy"):
            res = daemon.run_once(now_ts=now_ts + 5)

        self.assertEqual(res["executed"], 1)
        self.assertEqual(res["skipped"], 1)
        mock_reply.assert_called_once()
        q_col = db.get_collection("x_ops_queue")
        self.assertEqual(q_col.find_one({"_id": r1})["status"], "done")
        self.assertEqual(q_col.find_one({"_id": r2})["status"], "skipped")
        self.assertIn("already_replied", q_col.find_one({"_id": r2})["error"])

    def test_inter_write_pause_and_follow_interval_deferral(self):
        db = _FakeDB()
        t0 = _ts_cn(2026, 11, 24, 15, 30)
        page = MagicMock()
        page.url = "https://x.com/home"
        sess = _FakeSession(page=page)
        sleeps = []
        daemon = XOpsDaemon(db=db, session=sess, sleep_fn=sleeps.append)

        # Batch 1: 2 follows -> executes #1, waits random(35, 90)s, defers #2 by min_follow_interval_seconds
        f1 = enqueue_x_action("x_myself", "follow", {"screen_name": "alice"}, db=db, now_ts=t0)
        f2 = enqueue_x_action("x_myself", "follow", {"screen_name": "bob"}, db=db, now_ts=t0 + 1)
        with patch("platforms.x_ops_daemon.follow_on_x_page", return_value="FOLLOWED"), \
             patch("platforms.x_ops_daemon._wait_x_home_ready"), \
             patch("platforms.x_ops_daemon.assert_x_page_healthy"):
            res = daemon.run_once(now_ts=t0 + 5)

        self.assertEqual(res["executed"], 1)
        self.assertEqual(res["deferred"], 1)
        self.assertEqual(len(sleeps), 1)
        self.assertGreaterEqual(sleeps[0], 35.0)
        self.assertLessEqual(sleeps[0], 90.0)
        q_col = db.get_collection("x_ops_queue")
        self.assertEqual(q_col.find_one({"_id": f1})["status"], "done")
        f2_doc = q_col.find_one({"_id": f2})
        self.assertEqual(f2_doc["status"], "pending")
        self.assertEqual(f2_doc["scheduled_at_ts"], (t0 + 5) + 905)
        self.assertIn("follow_interval_cooldown", f2_doc["error"])

        # Batch 2: read-only sniff after a write does NOT sleep
        sleeps.clear()
        daemon._update_queue_item(f2, status="cancelled", now_ts=t0 + 6)
        enqueue_x_action("x_myself", "post", {"content": "post before sniff"}, db=db, now_ts=t0 + 10)
        enqueue_x_action("x_myself", "sniff_notifications", {"scroll_rounds": 1}, db=db, now_ts=t0 + 11)
        with patch("platforms.x_ops_daemon.post_on_x_page", return_value="999000"), \
             patch("platforms.x_ops_daemon.sniff_notifications_on_page", return_value={"items": []}):
            res2 = daemon.run_once(now_ts=t0 + 15)
        self.assertEqual(res2["executed"], 2)
        self.assertEqual(sleeps, [])

    def test_idle_queue_starts_zero_browsers_and_performs_zero_default_actions(self):
        db = _FakeDB()
        sess = _FakeSession()
        daemon = XOpsDaemon(db=db, session=sess)

        res = daemon.run_once(now_ts=_ts_cn(2026, 11, 24, 15, 0))
        self.assertEqual(res["status"], "idle")
        self.assertEqual(res["due"], 0)
        self.assertEqual(res["executed"], 0)
        self.assertEqual(sess.started_count, 0)
        self.assertFalse(sess.is_alive())

    def test_sniff_notifications_is_read_only_and_wait_x_action_returns_result(self):
        db = _FakeDB()
        now_ts = _ts_cn(2026, 11, 24, 16, 0)
        page = MagicMock()
        page.url = "https://x.com/home"
        sess = _FakeSession(page=page)
        daemon = XOpsDaemon(db=db, session=sess)

        pending_id = enqueue_x_action("x_myself", "sniff_notifications", {"scroll_rounds": 1}, db=db, now_ts=now_ts)
        with self.assertRaises(TimeoutError):
            wait_x_action(pending_id, timeout_seconds=0.05, poll_interval=0.01, db=db)

        fake_sniff = {
            "own_handle": "peterzhu1003",
            "own_uid": "12345",
            "notifications_count": 1,
            "items": [{"reply_id": "888", "reply_text": "nice post"}],
        }
        with patch("platforms.x_ops_daemon.sniff_notifications_on_page", return_value=fake_sniff), \
             patch("platforms.x_ops_daemon.reply_on_x_page", side_effect=AssertionError("must never auto-reply")), \
             patch("platforms.x_ops_daemon._wait_x_home_ready"), \
             patch("platforms.x_ops_daemon.assert_x_page_healthy"):
            res = daemon.run_once(now_ts=now_ts + 5)

        self.assertEqual(res["status"], "ok")
        self.assertEqual(res["executed"], 1)
        doc = wait_x_action(pending_id, timeout_seconds=1.0, poll_interval=0.01, db=db)
        self.assertEqual(doc["status"], "done")
        self.assertEqual(doc["result"]["items"], [{"reply_id": "888", "reply_text": "nice post"}])

    def test_runs_anytime_without_quiet_hours_and_isolates_other_accounts(self):
        db = _FakeDB()
        night_ts = _ts_cn(2026, 11, 23, 23, 30)
        page = MagicMock()
        page.url = "https://x.com/home"
        sess = _FakeSession(page=page)
        daemon = XOpsDaemon(db=db, session=sess)

        id_myself = enqueue_x_action("x_myself", "post", {"content": "Night post for x_myself"}, db=db, now_ts=night_ts)
        id_dahao = enqueue_x_action("x_dahao", "post", {"content": "Task for x_dahao"}, db=db, now_ts=night_ts + 1)

        with patch("platforms.x_ops_daemon.post_on_x_page", return_value="900000000000000088"), \
             patch("platforms.x_ops_daemon._wait_x_home_ready"), \
             patch("platforms.x_ops_daemon.assert_x_page_healthy"):
            res = daemon.run_once(now_ts=night_ts + 5)

        self.assertEqual(res["status"], "ok")
        self.assertEqual(res["executed"], 1)
        q_col = db.get_collection("x_ops_queue")
        self.assertEqual(q_col.find_one({"_id": id_myself})["status"], "done")
        self.assertEqual(q_col.find_one({"_id": id_dahao})["status"], "pending")

    def test_challenge_triggers_zero_second_freeze_closes_browser_and_stops_queue(self):
        db = _FakeDB()
        now_ts = _ts_cn(2026, 11, 24, 11, 0)
        page = MagicMock()
        page.url = "https://x.com/account/access"
        sess = _FakeSession(page=page)
        daemon = XOpsDaemon(db=db, session=sess)

        id1 = enqueue_x_action("x_myself", "post", {"content": "will hit challenge"}, db=db, now_ts=now_ts)
        id2 = enqueue_x_action("x_myself", "post", {"content": "must stay pending"}, db=db, now_ts=now_ts + 1)

        with patch("platforms.x_ops_daemon.post_on_x_page", side_effect=ChallengeRequiredError("locked /account/access")), \
             patch("platforms.x_ops_daemon._wait_x_home_ready"), \
             patch("platforms.x_ops_daemon.assert_x_page_healthy"):
            res = daemon.run_once(now_ts=now_ts + 5)

        self.assertEqual(res["status"], "frozen_challenge")
        self.assertFalse(sess.is_alive())
        self.assertEqual(sess.closed_count, 1)

        frozen, _, reason = daemon.breaker.is_frozen("x_myself", now_ts=now_ts + 10)
        self.assertTrue(frozen)
        self.assertIn("locked", reason)

        q_col = db.get_collection("x_ops_queue")
        self.assertEqual(q_col.find_one({"_id": id1})["status"], "failed")
        self.assertEqual(q_col.find_one({"_id": id2})["status"], "pending")

        # Subsequent run_once must refuse to open Chrome while frozen
        res_next = daemon.run_once(now_ts=now_ts + 60)
        self.assertEqual(res_next["status"], "frozen")
        self.assertEqual(sess.started_count, 1)

    def test_mid_batch_breaker_trip_via_business_error_halts_remaining_items(self):
        db = _FakeDB()
        now_ts = _ts_cn(2026, 11, 24, 12, 0)
        page = MagicMock()
        page.url = "https://x.com/i/flow/login"
        sess = _FakeSession(page=page)
        daemon = XOpsDaemon(db=db, session=sess, sleep_fn=lambda _s: None)

        id1 = enqueue_x_action("x_myself", "post", {"content": "hits logged out"}, db=db, now_ts=now_ts)
        id2 = enqueue_x_action("x_myself", "post", {"content": "must not run after logout"}, db=db, now_ts=now_ts + 1)

        def _trip_and_raise(*_args, **kwargs):
            daemon.breaker.trip("x_myself", reason="not_logged_in", url=page.url, now_ts=now_ts + 5)
            raise BusinessErrorException("not_logged_in")

        with patch("platforms.x_ops_daemon.post_on_x_page", side_effect=_trip_and_raise), \
             patch("platforms.x_ops_daemon._wait_x_home_ready"), \
             patch("platforms.x_ops_daemon.assert_x_page_healthy"):
            res = daemon.run_once(now_ts=now_ts + 5)

        self.assertEqual(res["status"], "frozen_challenge")
        self.assertFalse(sess.is_alive())
        self.assertEqual(sess.closed_count, 1)
        q_col = db.get_collection("x_ops_queue")
        self.assertEqual(q_col.find_one({"_id": id1})["status"], "failed")
        self.assertEqual(q_col.find_one({"_id": id2})["status"], "pending")

    def test_follow_and_pin_risk_capture_and_pin_unconfirmed_raises(self):
        # 1. Shared response risk detector checks HTTP 403/429 & risk codes
        self.assertEqual(_detect_x_response_risk(SimpleNamespace(status=403)), "HTTP 403")
        self.assertEqual(_detect_x_response_risk(SimpleNamespace(status=429)), "HTTP 429")
        resp_226 = SimpleNamespace(status=200, json=lambda: {"errors": [{"code": 226, "message": "automated"}]})
        self.assertIn("code=226", _detect_x_response_risk(resp_226))
        resp_161 = SimpleNamespace(status=200, json=lambda: {"errors": [{"code": 161, "message": "Unable to follow more people"}]})
        self.assertIn("code=161", _detect_x_response_risk(resp_161))
        self.assertIsNone(_detect_x_response_risk(SimpleNamespace(status=200, json=lambda: {"data": {}})))

        # 2. _dismiss_x_overlays skips _dismiss_overlays when compose/reply dialog is visible
        open_dialog_loc = MagicMock()
        open_dialog_loc.first = open_dialog_loc
        open_dialog_loc.count.return_value = 1
        open_dialog_loc.is_visible.return_value = True
        dlg_page = MagicMock()
        dlg_page.locator.return_value = open_dialog_loc
        with patch("platforms.x_playwright._dismiss_overlays") as mock_dismiss:
            self.assertFalse(_dismiss_x_overlays(dlg_page, desc="reply-open"))
            mock_dismiss.assert_not_called()

        no_dialog_loc = MagicMock()
        no_dialog_loc.first = no_dialog_loc
        no_dialog_loc.count.return_value = 0
        dlg_page.locator.return_value = no_dialog_loc
        with patch("platforms.x_playwright._dismiss_overlays", return_value=True) as mock_dismiss:
            self.assertTrue(_dismiss_x_overlays(dlg_page, desc="clean"))
            mock_dismiss.assert_called_once()

        # 3. follow_on_x_page trips breaker & raises ChallengeRequiredError on 403 response
        def _make_action_page(url_str, emit_resp=None, *, on_wait=None):
            listeners = {}
            page = MagicMock()
            page.url = url_str
            page.goto.return_value = SimpleNamespace(status=200)
            page.on.side_effect = lambda ev, fn: listeners.setdefault(ev, []).append(fn)
            page.remove_listener.side_effect = lambda ev, fn: listeners.get(ev, []).remove(fn) if fn in listeners.get(ev, []) else None
            if on_wait is not None:
                page.wait_for_timeout.side_effect = on_wait

            def _click(_p, _loc):
                if emit_resp is not None:
                    for fn in list(listeners.get("response", [])):
                        fn(emit_resp)

            def _locator(sel):
                loc = MagicMock()
                loc.first = loc
                if "empty_state" in sel or "-unfollow" in sel or "-cancel" in sel or "unpin" in str(sel).lower():
                    loc.count.return_value = 0
                    loc.is_visible.return_value = False
                else:
                    loc.count.return_value = 1
                    loc.is_visible.return_value = True
                    loc.get_attribute.return_value = "follow"
                loc.filter.side_effect = lambda **kw: (
                    _locator("unpin")
                    if "Unpin" in str(getattr(kw.get("has_text"), "pattern", ""))
                    else loc
                )
                loc.locator.side_effect = _locator
                return loc

            page.locator.side_effect = _locator
            return page, _click

        db = _FakeDB()
        breaker = XAccountCircuitBreaker(db)
        follow_403 = SimpleNamespace(status=403, url="https://x.com/i/api/1.1/friendships/create.json")
        fpage, fclick = _make_action_page("https://x.com/target_u", emit_resp=follow_403)
        with patch("platforms.x_playwright._dismiss_overlays"), \
             patch("platforms.x_playwright.assert_x_page_healthy"), \
             patch("platforms.x_playwright.check_for_crash"), \
             patch("platforms.x_playwright._forensics"), \
             patch("platforms.x_playwright.human_scroll"), \
             patch("platforms.x_playwright.human_pause"), \
             patch("platforms.x_playwright.human_click", side_effect=fclick):
            with self.assertRaises(ChallengeRequiredError):
                follow_on_x_page(fpage, "target_u", browse_seconds=None, account_name="x_myself", breaker=breaker)
        self.assertTrue(breaker.is_frozen("x_myself")[0])

        # 4. pin_on_x_page trips breaker on risk code 226 & raises when unconfirmed (no API hit)
        breaker.clear("x_myself")
        pin_226 = SimpleNamespace(
            status=200,
            url="https://x.com/i/api/1.1/account/pin_tweet.json",
            json=lambda: {"errors": [{"code": 226, "message": "automated"}]},
        )
        p_risk_page, p_risk_click = _make_action_page("https://x.com/peter/status/123456789012345678", emit_resp=pin_226)
        with patch("platforms.x_playwright._wait_x_post_detail_ready"), \
             patch("platforms.x_playwright._dismiss_overlays"), \
             patch("platforms.x_playwright.assert_x_page_healthy"), \
             patch("platforms.x_playwright.check_for_crash"), \
             patch("platforms.x_playwright._forensics"), \
             patch("platforms.x_playwright.human_pause"), \
             patch("platforms.x_playwright.human_click", side_effect=p_risk_click):
            with self.assertRaises(ChallengeRequiredError):
                pin_on_x_page(p_risk_page, "123456789012345678", "peter", account_name="x_myself", breaker=breaker)
        self.assertTrue(breaker.is_frozen("x_myself")[0])

        t_state = {"now": 1000.0}
        ppage, pclick = _make_action_page(
            "https://x.com/peter/status/123456789012345678",
            emit_resp=None,
            on_wait=lambda _ms: t_state.__setitem__("now", t_state["now"] + 9.0),
        )
        with patch("platforms.x_playwright._wait_x_post_detail_ready"), \
             patch("platforms.x_playwright._dismiss_overlays"), \
             patch("platforms.x_playwright.assert_x_page_healthy"), \
             patch("platforms.x_playwright.check_for_crash"), \
             patch("platforms.x_playwright._forensics"), \
             patch("platforms.x_playwright.human_pause"), \
             patch("platforms.x_playwright.human_click", side_effect=pclick), \
             patch("platforms.x_playwright.time.time", side_effect=lambda: t_state["now"]):
            with self.assertRaises(Exception) as ctx_err:
                pin_on_x_page(ppage, "123456789012345678", "peter", account_name="x_myself", breaker=breaker)
        self.assertIn("未检测到置顶成功接口响应", str(ctx_err.exception))

    def test_no_cli_argparse_and_config_driven_main(self):
        daemon_src = pathlib.Path(x_ops_daemon_mod.__file__).read_text(encoding="utf-8")
        self.assertNotIn("argparse", daemon_src)
        self.assertNotIn("QUIET_HOURS_CN", daemon_src)
        self.assertNotIn("check_comment_sentinel", daemon_src)
        self.assertNotIn("call_llm_json", daemon_src)
        self.assertNotIn("sync_and_reply", daemon_src)
        self.assertIn(
            x_ops_daemon_mod.RUN_MODE,
            ("status", "daemon", "once", "smoke_test", "login", "clear_freeze"),
        )
        self.assertEqual(DEFAULT_ACCOUNT["name"], "x_myself")

        fake_db = _FakeDB()
        fake_daemon = XOpsDaemon(db=fake_db, session=_FakeSession())
        buf = io.StringIO()
        with patch.object(x_ops_daemon_mod, "XOpsDaemon", return_value=fake_daemon):
            with redirect_stdout(buf):
                x_ops_daemon_mod.main("status")
        out = buf.getvalue()
        self.assertIn("XOpsDaemon Status (x_myself", out)
        with self.assertRaises(ValueError):
            with patch.object(x_ops_daemon_mod, "XOpsDaemon", return_value=fake_daemon):
                x_ops_daemon_mod.main("invalid_mode")


if __name__ == "__main__":
    unittest.main()
