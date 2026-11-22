# -*- coding: utf-8 -*-
"""Offline unit tests for 72h highest-conversion X auto-pinner (`x_growth/auto_pinner.py`)."""
import ast
import base64
import contextlib
import logging
import pathlib
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from platforms.playwright_base import UserDataDirLockTimeoutError
from x_growth.auto_pinner import (
    ACCOUNTS,
    CONVERSION_METRIC_MODE,
    INTERVAL_SECONDS,
    PIN_HISTORY_COLLECTION,
    PIN_SNAPSHOTS_COLLECTION,
    SESSION_MAX_HOLD_SECONDS,
    WINDOW_HOURS,
    XPinAnalyticsSniffer,
    build_status_report,
    compute_post_conversion_metrics,
    detect_pinned_tweet_id_from_dom,
    format_status_text,
    resolve_accounts,
    run_pinner_daemon,
    select_best_conversion_post,
    sync_best_pinned_post_for_account,
)


def _get_dotted(doc, key):
    cur = doc
    for part in key.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def _set_dotted(doc, key, val):
    parts = key.split(".")
    cur = doc
    for p in parts[:-1]:
        if p not in cur or not isinstance(cur[p], dict):
            cur[p] = {}
        cur = cur[p]
    cur[parts[-1]] = val


def _match_query(doc, query):
    if not query:
        return True
    for k, cond in query.items():
        if k == "$or":
            if not any(_match_query(doc, sub) for sub in cond):
                return False
            continue
        val = _get_dotted(doc, k)
        if isinstance(cond, dict):
            for op, target in cond.items():
                if op == "$gte" and (val is None or val < target):
                    return False
                if op == "$gt" and (val is None or val <= target):
                    return False
                if op == "$lte" and (val is None or val > target):
                    return False
                if op == "$lt" and (val is None or val >= target):
                    return False
                if op == "$in" and val not in target:
                    return False
                if op == "$ne" and val == target:
                    return False
                if op == "$exists" and (bool(target) != (val is not None)):
                    return False
        elif val != cond:
            return False
    return True


class _FakeCursor(list):
    def sort(self, key_or_list, direction=1):
        items = list(self)
        pairs = list(key_or_list) if isinstance(key_or_list, list) else [(key_or_list, direction)]
        for key, d in reversed(pairs):
            items.sort(key=lambda doc: _get_dotted(doc, key) or 0, reverse=(int(d) < 0))
        return _FakeCursor(items)

    def limit(self, n):
        return _FakeCursor(self[:n])


class _FakeCollection:
    def __init__(self, docs=None):
        self.docs = list(docs or [])

    def find(self, query=None, _projection=None):
        return _FakeCursor([dict(d) for d in self.docs if _match_query(d, query)])

    def find_one(self, query=None, _projection=None, sort=None):
        matched = [d for d in self.docs if _match_query(d, query)]
        if sort and matched:
            key, direction = sort[0]
            matched.sort(key=lambda d: _get_dotted(d, key) or 0, reverse=(direction < 0))
        return dict(matched[0]) if matched else None

    def count_documents(self, query=None):
        return sum(1 for d in self.docs if _match_query(d, query))

    def insert_one(self, doc):
        self.docs.append(dict(doc))

    def update_one(self, query, update, upsert=False):
        target = next((d for d in self.docs if _match_query(d, query)), None)
        if target is None:
            if not upsert:
                return
            target = dict(query)
            self.docs.append(target)
        for k, v in (update.get("$set") or {}).items():
            _set_dotted(target, k, v)

    def update_many(self, query, update):
        for d in self.docs:
            if _match_query(d, query):
                for k, v in (update.get("$set") or {}).items():
                    _set_dotted(d, k, v)


class _FakeDB:
    def __init__(self):
        self.cols = {}

    def get_collection(self, name):
        if name not in self.cols:
            self.cols[name] = _FakeCollection()
        return self.cols[name]

    def create_index(self, *_args, **_kwargs):
        pass


def _gql_tweet(
    rest_id,
    username="peterzhu1003",
    *,
    uid="u_me",
    full_text="Post content",
    views=100,
    likes=2,
    retweets=0,
    replies=0,
    quotes=0,
    bookmarks=0,
    in_reply_to_status_id=None,
    retweeted=None,
    created_at_ts=None,
):
    ts = int(created_at_ts if created_at_ts is not None else time.time() - 3600)
    legacy = {
        "id_str": str(rest_id),
        "user_id_str": str(uid),
        "created_at": time.strftime("%a %b %d %H:%M:%S +0000 %Y", time.gmtime(ts)),
        "full_text": full_text,
        "favorite_count": likes,
        "retweet_count": retweets,
        "reply_count": replies,
        "quote_count": quotes,
        "bookmark_count": bookmarks,
        "entities": {"urls": [], "media": []},
    }
    if in_reply_to_status_id:
        legacy["in_reply_to_status_id_str"] = str(in_reply_to_status_id)
    if retweeted:
        legacy["retweeted_status_result"] = {"result": retweeted}
    return {
        "__typename": "Tweet",
        "rest_id": str(rest_id),
        "views": {"count": str(views), "state": "EnabledWithCount"},
        "core": {
            "user_results": {
                "result": {
                    "__typename": "User",
                    "rest_id": str(uid),
                    "core": {"screen_name": username},
                    "legacy": {"screen_name": username},
                }
            }
        },
        "legacy": legacy,
    }


def _activity_payload(tweet_id, *, impressions=100, engagements=10, detail_expands=4, profile_visits=1, follows=0, link_clicks=0):
    b64_res = base64.b64encode(f"TweetResults:{tweet_id}".encode("utf-8")).decode("ascii")
    b64_tw = base64.b64encode(f"Tweet:{tweet_id}".encode("utf-8")).decode("ascii")
    grid = [
        {"metric_type": "DetailExpands", **({"metric_value": detail_expands} if detail_expands else {})},
        {"metric_type": "Engagements", **({"metric_value": engagements} if engagements else {})},
        {"metric_type": "Follows", **({"metric_value": follows} if follows else {})},
        {"metric_type": "Impressions", **({"metric_value": impressions} if impressions else {})},
        {"metric_type": "LinkClicks", **({"metric_value": link_clicks} if link_clicks else {})},
        {"metric_type": "ProfileVisits", **({"metric_value": profile_visits} if profile_visits else {})},
    ]
    return {
        "data": {
            "tweet_result_by_rest_id": {
                "id": b64_res,
                "result": {"__typename": "Tweet", "id": b64_tw, "datapoints_grid": grid},
            }
        }
    }


def _make_fake_page(*, url="https://x.com/peterzhu1003", visible_selectors=("AppTabBar_Profile_Link",)):
    page = MagicMock()
    page.url = url

    def _locator(sel):
        loc = MagicMock()
        loc.first = loc
        is_vis = any(tok in sel for tok in visible_selectors)
        loc.count.return_value = 1 if is_vis else 0
        loc.is_visible.return_value = is_vis
        loc.get_attribute.side_effect = lambda attr: ("/peterzhu1003" if attr == "href" else "")
        loc.locator.side_effect = _locator
        loc.filter.side_effect = lambda **_kw: loc
        return loc

    page.locator.side_effect = _locator
    resp = MagicMock()
    resp.status = 200
    page.goto.return_value = resp
    return page


class TestAutoPinner(unittest.TestCase):
    def setUp(self):
        self.silent_logger = logging.getLogger("test_x_auto_pinner_silent")
        self.silent_logger.handlers = [logging.NullHandler()]
        self.silent_logger.propagate = False
        self.silent_logger.setLevel(logging.INFO)
        self._logger_patcher = patch("x_growth.auto_pinner._get_logger", return_value=self.silent_logger)
        self._logger_patcher.start()

    def tearDown(self):
        self._logger_patcher.stop()

    def test_architecture_isolation_and_config(self):
        mod_path = pathlib.Path(__file__).resolve().parent.parent / "x_growth" / "auto_pinner.py"
        src = mod_path.read_text(encoding="utf-8")
        tree = ast.parse(src, filename=str(mod_path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    self.assertFalse(alias.name.startswith("x_growth"))
                    self.assertNotEqual(alias.name, "argparse")
            elif isinstance(node, ast.ImportFrom):
                self.assertFalse((node.module or "").startswith("x_growth"))

        self.assertNotIn("argparse", src)
        for legacy_tbl in ("social_media_posts", "generated_articles", "harvested_social_posts"):
            self.assertNotIn(legacy_tbl, src)

        self.assertEqual(INTERVAL_SECONDS, 21600)
        self.assertEqual(WINDOW_HOURS, 72)
        self.assertEqual(CONVERSION_METRIC_MODE, "weighted_conversion")
        self.assertLessEqual(SESSION_MAX_HOLD_SECONDS, 180)
        self.assertTrue(bool(ACCOUNTS[0]["name"]))
        self.assertTrue(bool(ACCOUNTS[0]["user_data_dir"]))

        with tempfile.TemporaryDirectory() as tmp:
            p_only = resolve_accounts([{"user_data_dir": tmp}])
            self.assertEqual(len(p_only), 1)
            self.assertEqual(p_only[0]["screen_name"], "")
            self.assertEqual(p_only[0]["name"], Path(tmp).name)

    def test_sniffer_parses_pinned_activity_and_monotonic_counters(self):
        sniffer = XPinAnalyticsSniffer()
        # 1. UserByScreenName with legacy pinned_tweet_ids_str
        sniffer.ingest_payload(
            "UserByScreenName",
            {"data": {"user": {"result": {"__typename": "User", "legacy": {"pinned_tweet_ids_str": ["111"]}}}}},
        )
        self.assertEqual(sniffer.pinned_tweet_id_from_api, "111")

        # 2. TimelinePinEntry overrides with latest timeline pinned tweet + skips RT/replies + monotonic merge
        p_pin = _gql_tweet("222", views=120, likes=5)
        p_rt = _gql_tweet("333", retweeted=_gql_tweet("999", "other"))
        p_reply = _gql_tweet("444", in_reply_to_status_id="100")
        sniffer.ingest_payload(
            "UserOriginalsTimeline",
            {
                "instructions": [
                    {"type": "TimelinePinEntry", "entry": {"entryId": "pinned-222", "itemContent": {"tweet_results": {"result": p_pin}}}},
                    {"type": "TimelineAddEntries", "entries": [p_rt, p_reply]},
                ]
            },
        )
        self.assertEqual(sniffer.pinned_tweet_id_from_api, "222")
        self.assertIn("222", sniffer.own_posts_by_id)
        self.assertNotIn("333", sniffer.own_posts_by_id)
        self.assertNotIn("444", sniffer.own_posts_by_id)

        # Stale lower views does not downgrade 120 -> 40
        sniffer.ingest_payload("UserOriginalsTimeline", {"items": [_gql_tweet("222", views=40, likes=1)]})
        self.assertEqual(sniffer.own_posts_by_id["222"]["views"], 120)
        self.assertEqual(sniffer.own_posts_by_id["222"]["likes"], 5)

        # 3. TweetActivityQuery decodes base64 id & preserves zeroed omitted fields
        sniffer.ingest_payload(
            "TweetActivityQuery",
            _activity_payload("222", impressions=125, engagements=24, detail_expands=9, profile_visits=3, follows=1, link_clicks=0),
        )
        self.assertEqual(sniffer.analytics_by_id["222"]["follows"], 1)
        self.assertEqual(sniffer.analytics_by_id["222"]["profile_visits"], 3)
        self.assertEqual(sniffer.analytics_by_id["222"]["link_clicks"], 0)
        self.assertEqual(sniffer.analytics_by_id["222"]["impressions"], 125)

        # 4. DOM fallback detection
        art = MagicMock()
        ctx = MagicMock()
        ctx.count.return_value = 1
        ctx.inner_text.return_value = "已置顶"
        link = MagicMock()
        link.get_attribute.return_value = "/peterzhu1003/status/222"
        art.locator.side_effect = lambda s: (
            MagicMock(first=ctx) if "socialContext" in s else MagicMock(all=MagicMock(return_value=[link]))
        )
        dom_page = MagicMock()
        dom_page.locator.return_value = MagicMock(first=art)
        art.count.return_value = 1
        self.assertEqual(detect_pinned_tweet_id_from_dom(dom_page, self_handle_lower="peterzhu1003"), "222")

    def test_conversion_scoring_smoothing_modes_and_fallbacks(self):
        now = 1_800_000_000
        # Real-shape posts: fluke 1/14 vs strong 21/96 + 1 ProfileVisit vs high-view low conversion 12/143 + Follow winner
        p_fluke = {"tweet_id": "t_fluke", "publish_ts": now - 10 * 3600, "views": 14, "likes": 1, "retweets": 0, "replies": 0, "quotes": 0, "bookmarks": 0}
        p_strong = {"tweet_id": "t_strong", "publish_ts": now - 30 * 3600, "views": 96, "likes": 1, "retweets": 0, "replies": 0, "quotes": 0, "bookmarks": 0}
        p_high_view = {"tweet_id": "t_views", "publish_ts": now - 50 * 3600, "views": 143, "likes": 0, "retweets": 0, "replies": 0, "quotes": 0, "bookmarks": 0}
        p_follow = {"tweet_id": "t_follow", "publish_ts": now - 12 * 3600, "views": 80, "likes": 3, "retweets": 1, "replies": 1, "quotes": 0, "bookmarks": 1}
        p_old = {"tweet_id": "t_old_80h", "publish_ts": now - 80 * 3600, "views": 500, "likes": 50, "retweets": 10, "replies": 5, "quotes": 2, "bookmarks": 5}

        a_map = {
            "t_strong": {"impressions": 96, "engagements": 21, "profile_visits": 1, "follows": 0, "detail_expands": 0, "link_clicks": 0},
            "t_views": {"impressions": 143, "engagements": 12, "profile_visits": 2, "follows": 0, "detail_expands": 0, "link_clicks": 0},
            "t_follow": {"impressions": 80, "engagements": 18, "profile_visits": 4, "follows": 2, "detail_expands": 5, "link_clicks": 1},
        }

        # Excluding old >72h post; t_follow clearly wins on Follows + ProfileVisits
        sel1 = select_best_conversion_post([p_fluke, p_strong, p_high_view, p_follow, p_old], analytics_by_id=a_map, now_ts=now)
        self.assertEqual(sel1["winner"]["tweet_id"], "t_follow")
        self.assertEqual(sel1["window_count"], 4)

        # Without t_follow, t_strong beats both p_high_view and below-min-impressions p_fluke
        sel2 = select_best_conversion_post([p_fluke, p_strong, p_high_view], analytics_by_id=a_map, now_ts=now)
        self.assertEqual(sel2["winner"]["tweet_id"], "t_strong")
        self.assertGreater(sel2["winner"]["conversion_score"], compute_post_conversion_metrics(p_fluke)["conversion_score"])

        # Test all 4 conversion modes explicitly
        for mode_name in ("weighted_conversion", "profile_follow", "engagement_rate", "public_interaction"):
            m_res = compute_post_conversion_metrics(p_follow, a_map["t_follow"], metric_mode=mode_name)
            self.assertEqual(m_res["conversion_mode"], mode_name)
            self.assertGreater(m_res["conversion_score"], 0.0)

        # Test fallback when no posts meet MIN_IMPRESSIONS or MIN_POST_AGE_MINUTES
        tiny_recent = {"tweet_id": "t_new", "publish_ts": now - 60, "views": 3, "likes": 1, "retweets": 0, "replies": 0, "quotes": 0, "bookmarks": 0}
        sel_tiny = select_best_conversion_post([tiny_recent], now_ts=now, min_impressions=50, min_post_age_minutes=30)
        self.assertEqual(sel_tiny["winner"]["tweet_id"], "t_new")
        self.assertTrue(sel_tiny["fallback_reason"])

        # Empty 72h window: keep existing pin if present, else fall back to all recent posts so 1 post stays pinned
        sel_keep = select_best_conversion_post([p_old], current_pinned_id="t_old_80h", now_ts=now)
        self.assertEqual(sel_keep["decision"], "keep_existing_no_window_posts")
        self.assertIsNone(sel_keep["winner"])

        sel_unpinned = select_best_conversion_post([p_old], current_pinned_id=None, now_ts=now)
        self.assertEqual(sel_unpinned["winner"]["tweet_id"], "t_old_80h")
        self.assertEqual(sel_unpinned["fallback_reason"], "no_72h_posts_fallback_all")

        # Hysteresis switch threshold
        sel_hyst = select_best_conversion_post(
            [p_strong, p_high_view],
            analytics_by_id=a_map,
            current_pinned_id="t_views",
            now_ts=now,
            min_switch_improvement_ratio=0.80,
        )
        self.assertEqual(sel_hyst["winner"]["tweet_id"], "t_views")
        self.assertEqual(sel_hyst["decision"], "already_pinned")

    def test_sync_best_pinned_post_full_flows(self):
        db = _FakeDB()
        now = int(time.time())

        def _make_flow_session(state):
            @contextlib.contextmanager
            def _ctx(*_a, **_kw):
                page = _make_fake_page()
                listeners = {}
                state.setdefault("pin_clicks", 0)
                state.setdefault("confirm_clicks", 0)

                def _on(ev, fn):
                    listeners[ev] = fn

                def _emit(op, payload, url):
                    resp = MagicMock()
                    resp.status = 200
                    resp.url = url
                    resp.json.return_value = payload
                    if "response" in listeners:
                        listeners["response"](resp)

                def _locator(sel):
                    loc = MagicMock()
                    loc.first = loc
                    loc.get_attribute.side_effect = lambda attr: ("/peterzhu1003" if attr == "href" else "")
                    loc.locator.side_effect = _locator
                    loc.filter.side_effect = lambda **_kw: loc

                    if "confirmationSheetConfirm" in sel:
                        loc.count.return_value = 1 if state.get("menu_pin_clicked") else 0
                        loc.is_visible.return_value = bool(state.get("menu_pin_clicked"))
                        loc.wait_for.side_effect = lambda **_kw: None
                        def _click_confirm(**_kw):
                            state["confirm_clicks"] += 1
                            state["current_pinned"] = state.get("target_pin")
                            _emit("CreatePin", {}, "https://x.com/i/api/graphql/x/PinTweet")
                        loc.click.side_effect = _click_confirm
                        return loc

                    if 'data-testid="unpin"' in sel or "取消置顶" in sel:
                        is_unpin = bool(state.get("menu_shows_unpin"))
                        loc.count.return_value = 1 if is_unpin else 0
                        loc.is_visible.return_value = is_unpin
                        return loc

                    if 'data-testid="pin"' in sel or "置顶" in sel:
                        is_pin = not bool(state.get("menu_shows_unpin"))
                        loc.count.return_value = 1 if is_pin else 0
                        loc.is_visible.return_value = is_pin
                        def _click_pin(**_kw):
                            state["pin_clicks"] += 1
                            state["menu_pin_clicked"] = True
                        loc.click.side_effect = _click_pin
                        return loc

                    vis = any(k in sel for k in ("AppTabBar_Profile_Link", "caret", 'article[data-testid="tweet"]'))
                    loc.count.return_value = 1 if vis else 0
                    loc.is_visible.return_value = vis
                    return loc

                page.locator.side_effect = _locator

                def _goto(u, **_kw2):
                    page.url = u
                    if u.endswith("/analytics"):
                        tid = u.split("/status/")[1].split("/")[0]
                        if tid in state.get("analytics", {}):
                            _emit("TweetActivityQuery", _activity_payload(tid, **state["analytics"][tid]), f"https://x.com/i/api/graphql/x/TweetActivityQuery?variables=%7B%22rest_id%22%3A%22{tid}%22%7D")
                    elif u.rstrip("/").endswith("/peterzhu1003"):
                        cur_pin = state.get("current_pinned")
                        _emit(
                            "UserByScreenName",
                            {"data": {"user": {"result": {"__typename": "User", "legacy": {"pinned_tweet_ids_str": [cur_pin] if cur_pin else []}}}}},
                            "https://x.com/i/api/graphql/x/UserByScreenName",
                        )
                        instrs = []
                        if cur_pin:
                            pin_node = next((p for p in state["posts"] if p["rest_id"] == cur_pin), _gql_tweet(cur_pin, created_at_ts=now - 100 * 3600))
                            instrs.append({"type": "TimelinePinEntry", "entry": {"entryId": f"pinned-{cur_pin}", "itemContent": {"tweet_results": {"result": pin_node}}}})
                        instrs.append({"type": "TimelineAddEntries", "entries": state["posts"]})
                        _emit("UserOriginalsTimeline", {"instructions": instrs}, "https://x.com/i/api/graphql/x/UserOriginalsTimeline")
                    elif "/status/" in u:
                        tid = u.split("/status/")[1].split("/")[0]
                        state["target_pin"] = tid
                        state["menu_pin_clicked"] = False
                        _emit("TweetDetail", {"items": [_gql_tweet(tid, created_at_ts=now - 3600)]}, "https://x.com/i/api/graphql/x/TweetDetail")
                    r = MagicMock()
                    r.status = 200
                    return r

                page.on.side_effect = _on
                page.goto.side_effect = _goto
                yield (MagicMock(), page)
            return _ctx

        p1 = _gql_tweet("9001", views=90, likes=1, created_at_ts=now - 5 * 3600)
        p2 = _gql_tweet("9002", views=110, likes=3, bookmarks=2, created_at_ts=now - 4 * 3600)
        old_pinned = _gql_tweet("8001", views=500, likes=20, created_at_ts=now - 100 * 3600)
        state = {
            "current_pinned": None,
            "posts": [p1, p2],
            "analytics": {
                "9001": {"impressions": 90, "engagements": 8, "profile_visits": 0, "follows": 0},
                "9002": {"impressions": 110, "engagements": 24, "profile_visits": 3, "follows": 1},
            },
        }

        with tempfile.TemporaryDirectory() as tmp:
            acc = {"name": "x_myself", "screen_name": "peterzhu1003", "user_data_dir": tmp}

            # 1. dry_run=True evaluates without pinning
            with patch("x_growth.auto_pinner.persistent_browser_session", side_effect=_make_flow_session(state)), \
                 patch("x_growth.auto_pinner._dismiss_overlays"), \
                 patch("x_growth.auto_pinner.check_for_crash"), \
                 patch("x_growth.auto_pinner._wait_x_post_detail_ready"), \
                 patch("x_growth.auto_pinner._resolve_session_identity", return_value=("u_me", "peterzhu1003")), \
                 patch.object(XPinAnalyticsSniffer, "wait_for_ops", return_value=True), \
                 patch.object(XPinAnalyticsSniffer, "wait_for_analytics", return_value=True):
                res_dry = sync_best_pinned_post_for_account(acc, db=db, dry_run=True)
            self.assertEqual(res_dry["status"], "preview_would_pin")
            self.assertEqual(res_dry["winner"]["tweet_id"], "9002")
            self.assertEqual(state["confirm_clicks"], 0)

            # 2. Live run when old post (8001, >72h) was pinned -> switches to 9002 & keeps strictly 1 is_currently_pinned
            snap_col = db.get_collection(PIN_SNAPSHOTS_COLLECTION)
            snap_col.update_one({"account_name": "x_myself", "tweet_id": "8001"}, {"$set": {"is_currently_pinned": True}}, upsert=True)
            state["current_pinned"] = "8001"
            state["posts"] = [p1, p2, old_pinned]
            with patch("x_growth.auto_pinner.persistent_browser_session", side_effect=_make_flow_session(state)), \
                 patch("x_growth.auto_pinner._dismiss_overlays"), \
                 patch("x_growth.auto_pinner.check_for_crash"), \
                 patch("x_growth.auto_pinner._wait_x_post_detail_ready"), \
                 patch("x_growth.auto_pinner._resolve_session_identity", return_value=("u_me", "peterzhu1003")), \
                 patch.object(XPinAnalyticsSniffer, "wait_for_ops", return_value=True), \
                 patch.object(XPinAnalyticsSniffer, "wait_for_analytics", return_value=True):
                res_pin = sync_best_pinned_post_for_account(acc, db=db, dry_run=False)
            self.assertEqual(res_pin["status"], "pinned")
            self.assertEqual(res_pin["verified_pinned_id"], "9002")
            self.assertEqual(state["confirm_clicks"], 1)
            self.assertEqual(snap_col.count_documents({"account_name": "x_myself", "is_currently_pinned": True}), 1)
            self.assertIs(snap_col.find_one({"tweet_id": "8001"})["is_currently_pinned"], False)
            self.assertIs(snap_col.find_one({"tweet_id": "9002"})["is_currently_pinned"], True)

            # 3. Second run immediately after -> already_pinned with zero pin clicks
            prev_clicks = state["confirm_clicks"]
            with patch("x_growth.auto_pinner.persistent_browser_session", side_effect=_make_flow_session(state)), \
                 patch("x_growth.auto_pinner._dismiss_overlays"), \
                 patch("x_growth.auto_pinner.check_for_crash"), \
                 patch("x_growth.auto_pinner._wait_x_post_detail_ready"), \
                 patch("x_growth.auto_pinner._resolve_session_identity", return_value=("u_me", "peterzhu1003")), \
                 patch.object(XPinAnalyticsSniffer, "wait_for_ops", return_value=True), \
                 patch.object(XPinAnalyticsSniffer, "wait_for_analytics", return_value=True):
                res_idem = sync_best_pinned_post_for_account(acc, db=db, dry_run=False)
            self.assertEqual(res_idem["status"], "already_pinned")
            self.assertEqual(state["confirm_clicks"], prev_clicks)

            # 4. If profile API didn't show pin but caret menu shows 'unpin', safely returns already_pinned
            state["current_pinned"] = None
            state["menu_shows_unpin"] = True
            with patch("x_growth.auto_pinner.persistent_browser_session", side_effect=_make_flow_session(state)), \
                 patch("x_growth.auto_pinner._dismiss_overlays"), \
                 patch("x_growth.auto_pinner.check_for_crash"), \
                 patch("x_growth.auto_pinner._wait_x_post_detail_ready"), \
                 patch("x_growth.auto_pinner._resolve_session_identity", return_value=("u_me", "peterzhu1003")), \
                 patch.object(XPinAnalyticsSniffer, "wait_for_ops", return_value=True), \
                 patch.object(XPinAnalyticsSniffer, "wait_for_analytics", return_value=True):
                res_unpin_menu = sync_best_pinned_post_for_account(acc, db=db, dry_run=False)
            self.assertEqual(res_unpin_menu["status"], "already_pinned")
            self.assertEqual(state["confirm_clicks"], prev_clicks)

            # 5. Error branches: empty_capture, locked_challenge, UserDataDirLockTimeoutError
            @contextlib.contextmanager
            def _empty_sess(*_a, **_kw):
                yield (MagicMock(), _make_fake_page(url="https://x.com/peterzhu1003"))

            with patch("x_growth.auto_pinner.persistent_browser_session", side_effect=_empty_sess), \
                 patch("x_growth.auto_pinner._dismiss_overlays"), \
                 patch("x_growth.auto_pinner.check_for_crash"), \
                 patch.object(XPinAnalyticsSniffer, "wait_for_ops", return_value=False):
                self.assertEqual(sync_best_pinned_post_for_account(acc, db=db)["status"], "empty_capture")

            @contextlib.contextmanager
            def _chal_sess(*_a, **_kw):
                yield (MagicMock(), _make_fake_page(url="https://x.com/account/access", visible_selectors=()))

            with patch("x_growth.auto_pinner.persistent_browser_session", side_effect=_chal_sess):
                self.assertEqual(sync_best_pinned_post_for_account(acc, db=db)["status"], "locked_challenge")

            with patch("x_growth.auto_pinner.persistent_browser_session", side_effect=UserDataDirLockTimeoutError("busy")):
                self.assertEqual(sync_best_pinned_post_for_account(acc, db=db)["status"], "lock_timeout")

            # 6. Status dashboard formatting & daemon 1-round run
            rep = build_status_report(db, [acc])
            txt = format_status_text(rep)
            self.assertIn("9002", txt)
            self.assertIn("当前置顶", txt)
            self.assertGreaterEqual(db.get_collection(PIN_HISTORY_COLLECTION).count_documents({}), 2)

            with patch("x_growth.auto_pinner.gen_db_object", return_value=db), \
                 patch("x_growth.auto_pinner.sync_best_pinned_post_for_account", return_value={"status": "already_pinned"}) as mock_sync:
                rounds = run_pinner_daemon(accounts=[acc], max_rounds=1)
            self.assertEqual(rounds, 1)
            self.assertEqual(mock_sync.call_count, 1)


if __name__ == "__main__":
    unittest.main()
