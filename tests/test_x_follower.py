# -*- coding: utf-8 -*-
"""Offline unit tests for self-contained X auto-follower (`x_growth/auto_follower.py`)."""
import ast
import contextlib
import pathlib
import re
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from common.post_utils import AccountIsolationError
from x_growth.auto_follower import (
    ACTION_DELAY_RANGE,
    FOLLOW_RECORDS_COLLECTION,
    LIMIT_COOLDOWN_SECONDS,
    RISK_COOLDOWN_SECONDS,
    SESSION_MAX_HOLD_SECONDS,
    STRATEGY_VERSION,
    TARGET_ACCOUNTS,
    UNFOLLOW_AFTER_HOURS,
    UNFOLLOW_ENABLED,
    USER_PROFILES_COLLECTION,
    XUserSniffer,
    assert_safe_collector_dir,
    build_status_report,
    extract_user_from_gql_node,
    follow_for_account,
    format_status_text,
    mark_follow_backs,
    pick_verified_follower_seeds,
    record_follow_outcome,
    resolve_target_accounts,
    run_one_cycle,
    score_user_potential,
    select_candidates_for_account,
    select_unfollow_candidates,
    unfollow_non_followers_for_account,
    upsert_and_score_users,
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
                if op == "$in":
                    if isinstance(val, list):
                        if not any(v in target for v in val):
                            return False
                    elif val not in target:
                        return False
                if op == "$nin":
                    if isinstance(val, list):
                        if any(v in target for v in val):
                            return False
                    elif val in target:
                        return False
                if op == "$ne" and val == target:
                    return False
                if op == "$exists" and (bool(target) != (val is not None)):
                    return False
                if op == "$regex":
                    flags = re.IGNORECASE if "i" in str(cond.get("$options") or "") else 0
                    if not re.search(target, str(val or ""), flags):
                        return False
        elif isinstance(val, list):
            if cond not in val:
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

    def update_one(self, query, update, upsert=False):
        target = next((d for d in self.docs if _match_query(d, query)), None)
        if target is None:
            if not upsert:
                return
            target = dict(query)
            for k, v in (update.get("$setOnInsert") or {}).items():
                _set_dotted(target, k, v)
            self.docs.append(target)
        for k, v in (update.get("$set") or {}).items():
            _set_dotted(target, k, v)


class _FakeDB:
    def __init__(self):
        self.cols = {}

    def get_collection(self, name):
        if name not in self.cols:
            self.cols[name] = _FakeCollection()
        return self.cols[name]

    def create_index(self, *_args, **_kwargs):
        pass


def _make_user(username="good_peer", *, now=1_800_000_000, **overrides):
    obj = {
        "screen_name": username,
        "screen_name_lower": username.lower(),
        "user_id": "101",
        "name": f"Name {username}",
        "description": "Building open AI tools and connecting with builders.",
        "location": "Tokyo",
        "is_blue_verified": True,
        "is_verified": True,
        "verified_type": None,
        "is_protected": False,
        "default_profile_image": False,
        "followers_count": 700,
        "following_count": 850,
        "statuses_count": 420,
        "favourites_count": 300,
        "listed_count": 5,
        "source_roles": ["post_author"],
        "languages": ["en"],
        "last_active_ts": now - 3600,
        "last_seen_ts": now,
    }
    obj.update(overrides)
    return obj


def _gql_user(username, *, followers=600, following=700, desc="f4f builder", is_blue=True, verified_type=None):
    return {
        "__typename": "User",
        "rest_id": f"uid_{username}",
        "is_blue_verified": is_blue,
        "core": {"screen_name": username, "name": username, "created_at": "Thu Aug 31 14:21:07 +0000 2017"},
        "relationship_counts": {"followers": followers, "following": following},
        "tweet_counts": {"tweets": 180},
        "profile_bio": {"description": desc},
        "verification": {"verified": is_blue, "verified_type": verified_type},
        "legacy": {"screen_name": username, "followers_count": followers, "friends_count": following, "statuses_count": 180, "favourites_count": 90, "description": desc},
    }


def _gql_tweet(rest_id, username, *, reply_count=12, in_reply_to=None, retweeted=None, quoted=None, lang="en", **u_kwargs):
    node = {
        "__typename": "Tweet",
        "rest_id": str(rest_id),
        "core": {"user_results": {"result": _gql_user(username, **u_kwargs)}},
        "legacy": {"id_str": str(rest_id), "created_at": "Sat Nov 21 10:00:00 +0000 2026", "lang": lang, "reply_count": reply_count, "favorite_count": 20, "full_text": "hello"},
    }
    if in_reply_to:
        node["legacy"]["in_reply_to_status_id_str"] = str(in_reply_to)
    if retweeted:
        node["legacy"]["retweeted_status_result"] = {"result": retweeted}
    if quoted:
        node["quoted_status_result"] = {"result": quoted}
    return node


class TestRefactoredAutoFollower(unittest.TestCase):
    def test_zero_coupling_no_argparse_and_physical_isolation(self):
        mod_path = pathlib.Path(__file__).resolve().parent.parent / "x_growth" / "auto_follower.py"
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
        self.assertNotIn("harvested_social_posts", src)

        pool = resolve_target_accounts()
        self.assertEqual([a["name"] for a in pool], ["x_myself"])
        with self.assertRaises(AccountIsolationError):
            assert_safe_collector_dir("W:/temp/x_myself")
        assert_safe_collector_dir("W:/temp/x_xiaohao")

    def test_sniffer_captures_home_comments_and_verified_followers_circle(self):
        sniffer = XUserSniffer()
        orig_tw = _gql_tweet("100", "orig_author", followers=600, following=700)
        qt_tw = _gql_tweet("101", "quoted_author", followers=1200, following=1300)
        home_payload = {
            "home_tweets": [
                _gql_tweet("200", "feed_poster", followers=900, following=1000, reply_count=25, quoted=qt_tw),
                _gql_tweet("201", "retweeter_u", followers=400, following=500, retweeted=orig_tw),
            ],
            "who_to_follow": _gql_user("sidebar_peer", followers=550, following=650),
        }
        sniffer.ingest_payload("HomeTimeline", home_payload)
        self.assertIn("200", sniffer.discovered_threads)
        self.assertNotIn("201", sniffer.discovered_threads)

        detail_payload = {
            "thread": [
                _gql_tweet("200", "feed_poster"),
                _gql_tweet("301", "commenter_1", in_reply_to="200", lang="ja", desc="follow back always"),
            ]
        }
        sniffer.ingest_payload("TweetDetail", detail_payload, resp_url="https://x.com/i/api/graphql/x/TweetDetail?variables=%7B%22focalTweetId%22%3A%22200%22%7D")

        vf_payload = {"users": [_gql_user("circle_peer_1", followers=820, following=910, desc="互关"), _gql_user("circle_peer_2", followers=1400, following=1500)]}
        sniffer.ingest_payload("BlueVerifiedFollowers", vf_payload)

        for name in ("feed_poster", "quoted_author", "retweeter_u", "orig_author", "sidebar_peer", "commenter_1", "circle_peer_1", "circle_peer_2"):
            self.assertIn(name, sniffer.users_by_handle)
        self.assertIn("comment_author", sniffer.users_by_handle["commenter_1"]["source_roles"])
        self.assertIn("verified_follower_of_peer", sniffer.users_by_handle["circle_peer_1"]["source_roles"])

    def test_extract_visibility_wrapper_and_scoring_hard_gates_and_ordering(self):
        wrapped = {"__typename": "UserWithVisibilityResults", "user": _gql_user("BrandCorp", verified_type="Business")}
        parsed = extract_user_from_gql_node(wrapped)
        ok, reason, *_ = score_user_potential(parsed)
        self.assertFalse(ok)
        self.assertEqual(reason, "official_or_brand")

        now = 1_800_000_000
        bad_cases = [
            (_make_user("", now=now), "invalid_handle"),
            (_make_user("dead_u", now=now, account_unavailable=True), "unavailable"),
            (_make_user("locked_u", now=now, is_protected=True), "protected"),
            (_make_user("corp_u", now=now, verified_type="Government"), "official_or_brand"),
            (_make_user("unver_u", now=now, is_blue_verified=False, is_verified=False), "not_verified"),
            (_make_user("nof4f_u", now=now, description="Builder | no follow back"), "anti_mutual_bio"),
            (_make_user("cap5k_u", now=now, followers_count=2000, following_count=4850), "near_5k_follow_cap"),
            (_make_user("tiny_u", now=now, followers_count=15, following_count=20), "followers_out_of_range"),
            (_make_user("mega_u", now=now, followers_count=25000, following_count=25000), "followers_out_of_range"),
            (_make_user("shell_u", now=now, statuses_count=2), "low_activity_shell"),
            (_make_user("snob_u", now=now, followers_count=1000, following_count=100), "ratio_out_of_range"),
            (_make_user("bot_u", now=now, followers_count=100, following_count=600), "ratio_out_of_range"),
            (_make_user("stale_u", now=now, last_active_ts=now - 20 * 86400), "inactive_over_window"),
        ]
        for obj, expected in bad_cases:
            ok, r, sc, _, _ = score_user_potential(obj, now_ts=now)
            self.assertFalse(ok, f"Expected reject for {obj['screen_name']}")
            self.assertEqual(r, expected)
            self.assertEqual(sc, 0.0)

        # Chinese mutual keyword "互fo互赞" matches mutual_bio_pts
        hufo = _make_user("hahajing", now=now, description="热衷AI，互fo互赞", source_roles=["comment_author"], last_active_ts=now - 600)
        ok_hf, _, sc_hf, bd_hf, meta_hf = score_user_potential(hufo, now_ts=now)
        self.assertTrue(ok_hf)
        self.assertTrue(meta_hf["has_mutual_bio"])
        self.assertEqual(bd_hf["mutual_bio_pts"], 15.0)
        self.assertGreaterEqual(sc_hf, 95.0)

        # Freshly harvested verified_follower_of_peer (even with tweet_ts=0) scores strong & beats weak peer
        vf_peer = _make_user("vf_peer", now=now, last_active_ts=0, last_seen_ts=now - 3600, source_roles=["verified_follower_of_peer"])
        weak_peer = _make_user("weak_p", now=now, followers_count=6000, following_count=3300, default_profile_image=True, description="x", favourites_count=5, last_active_ts=now - 5 * 86400)
        ok_vf, _, sc_vf, _, _ = score_user_potential(vf_peer, now_ts=now)
        ok_wk, _, sc_wk, _, _ = score_user_potential(weak_peer, now_ts=now)
        self.assertTrue(ok_vf)
        self.assertTrue(ok_wk)
        self.assertGreater(sc_hf, sc_vf)
        self.assertGreater(sc_vf, sc_wk)

    def test_pick_seeds_and_upsert_rescore(self):
        db = _FakeDB()
        now = 1_800_000_000
        s_old_expanded = _make_user("seed_done", now=now, followers_count=800, following_count=900, description="F4F", source_roles=["comment_author"], seed_expanded_at_ts=now - 2 * 86400)
        s_ready_mb = _make_user("seed_ready", now=now, followers_count=800, following_count=900, description="F4F", source_roles=["comment_author"])
        s_ready_norm = _make_user("seed_norm", now=now, followers_count=900, following_count=1000, description="Builder", source_roles=["comment_author"])
        upsert_and_score_users(db, [s_old_expanded, s_ready_mb, s_ready_norm], now_ts=now)
        db.get_collection(USER_PROFILES_COLLECTION).update_one({"screen_name_lower": "seed_done"}, {"$set": {"seed_expanded_at_ts": now - 2 * 86400}})

        seeds = pick_verified_follower_seeds(db, max_seeds=2, now_ts=now)
        self.assertEqual(seeds, ["seed_ready", "seed_norm"])

        # Re-upserting a different batch 6 days later also decays scores of untouched recent docs in DB
        before = db.get_collection(USER_PROFILES_COLLECTION).find_one({"screen_name_lower": "seed_ready"})["follow_back_score"]
        upsert_and_score_users(db, [_make_user("brand_new", now=now + 6 * 86400)], now_ts=now + 6 * 86400)
        after = db.get_collection(USER_PROFILES_COLLECTION).find_one({"screen_name_lower": "seed_ready"})["follow_back_score"]
        self.assertLess(after, before)

    def test_select_candidates_isolation_and_unlimited_daily_mode(self):
        db = _FakeDB()
        now = int(time.time())
        u_mid = _make_user("mid_u", now=now, followers_count=2000, following_count=1700, last_active_ts=now - 7200)
        u_top = _make_user("top_u", now=now, followers_count=500, following_count=650, description="follow back", source_roles=["comment_author"], last_active_ts=now - 1800)
        u_retry = _make_user("retry_u", now=now, followers_count=600, following_count=700, last_active_ts=now - 3600)
        upsert_and_score_users(db, [u_mid, u_top, u_retry], now_ts=now)

        record_follow_outcome(db, account_name="x_myself", account_handle="peterzhu1003", account_dir="/tmp/m", target_profile=u_mid, status="followed", now_ts=now - 500)
        record_follow_outcome(db, account_name="x_myself", account_handle="peterzhu1003", account_dir="/tmp/m", target_profile=u_retry, status="failed", error="timeout", now_ts=now - 30 * 3600)

        cands1, t1, _ = select_candidates_for_account(db, {"name": "x_myself", "screen_name": "peterzhu1003"}, limit=10, now_ts=now)
        self.assertEqual([c["screen_name"] for c in cands1], ["top_u", "retry_u"])
        self.assertEqual(t1, 2)

        # Account 2 sees all 3 independently
        cands2, t2, _ = select_candidates_for_account(db, {"name": "x_alt", "screen_name": "alt"}, limit=10, now_ts=now)
        self.assertEqual(t2, 3)

        # max_follow_per_day=0 means no local daily ceiling in dry-run or daily quota check
        with tempfile.TemporaryDirectory() as tmp:
            unlimited_acc = {"name": "x_myself", "screen_name": "peterzhu1003", "user_data_dir": tmp, "max_follow_per_run": 10, "max_follow_per_day": 0}
            res = follow_for_account(unlimited_acc, db=db, dry_run=True)
            self.assertEqual(res["status"], "ok")
            self.assertEqual(res["followed_count"], 2)

    def test_record_outcome_follow_backs_analytics_and_cycle_cooldowns(self):
        db = _FakeDB()
        now = 1_800_000_000
        p = _make_user("drift_peer", now=now, followers_count=3100, following_count=3643, follow_back_score=84.5, source_roles=["verified_follower_of_peer"])
        record_follow_outcome(db, account_name="x_myself", account_handle="peterzhu1003", account_dir="/tmp/m", target_profile=p, status="followed", candidate_rank=1, eligible_pool_size=20, now_ts=now - 7200)
        stored = db.get_collection(FOLLOW_RECORDS_COLLECTION).find_one({"account_name": "x_myself", "target_handle_lower": "drift_peer"})
        self.assertEqual(stored["ff_ratio"], round(3643 / 3100, 3))
        self.assertEqual(stored["strategy_version"], STRATEGY_VERSION)

        fb = mark_follow_backs(db, account_name="x_myself", follower_handles=["drift_peer"], now_ts=now)
        self.assertEqual(fb["newly_followed_back"], 1)

        rep = build_status_report(db, [{"name": "x_myself", "screen_name": "peterzhu1003", "max_follow_per_day": 0}], now_ts=now)
        self.assertEqual(rep["analytics"]["by_followers"][">3000"]["followed_back"], 1)
        self.assertEqual(rep["analytics"]["by_source"]["verified_circle"]["followed_back"], 1)
        self.assertIn("无上限", format_status_text(rep))

        # Test cycle isolation: collector locked -> 6h; acc_a rate_limited -> 2h; acc_b -> continues normally
        def _fake_follow(acc, *, db, dry_run=False, **_kwargs):
            return {"account_name": acc["name"], "screen_name": acc["screen_name"], "status": "rate_limited" if acc["name"] == "acc_a" else "ok", "followed_count": 0 if acc["name"] == "acc_a" else 5}

        with tempfile.TemporaryDirectory() as tmp:
            p1, p2 = str(Path(tmp) / "a"), str(Path(tmp) / "b")
            accs = [{"name": "acc_a", "screen_name": "ua", "user_data_dir": p1, "enabled": True}, {"name": "acc_b", "screen_name": "ub", "user_data_dir": p2, "enabled": True}]
            cd, col_st = {}, {"cooldown_until": None}
            with patch("x_growth.auto_follower.gen_db_object", return_value=db), \
                 patch("x_growth.auto_follower.harvest_potential_users", return_value={"status": "locked_challenge", "captured_total": 0}) as m_h, \
                 patch("x_growth.auto_follower.follow_for_account", side_effect=_fake_follow):
                t_start = time.time()
                r1 = run_one_cycle(accounts=accs, cooldown_until=cd, collector_state=col_st)
                self.assertEqual(r1["harvest"]["status"], "locked_challenge")
                self.assertAlmostEqual(col_st["cooldown_until"] - t_start, RISK_COOLDOWN_SECONDS, delta=5.0)
                self.assertAlmostEqual(cd[p1] - t_start, LIMIT_COOLDOWN_SECONDS, delta=5.0)
                self.assertNotIn(p2, cd)

                r2 = run_one_cycle(accounts=accs, cooldown_until=cd, collector_state=col_st)
                self.assertEqual(m_h.call_count, 1)
                self.assertEqual(r2["harvest"]["status"], "cooldown")
                self.assertEqual([a["status"] for a in r2["accounts"]], ["cooldown", "ok"])

        self.assertEqual(TARGET_ACCOUNTS[0]["max_follow_per_run"], 20)
        self.assertEqual(TARGET_ACCOUNTS[0]["max_follow_per_day"], 400)
        self.assertEqual(ACTION_DELAY_RANGE, (3, 5))
        self.assertEqual(SESSION_MAX_HOLD_SECONDS, 180)
        self.assertEqual(UNFOLLOW_AFTER_HOURS, 48)
        self.assertTrue(UNFOLLOW_ENABLED)

    def test_follow_180s_lock_hold_stop_loss(self):
        db = _FakeDB()
        now = int(time.time())
        cands = [_make_user(f"cand_{i}", now=now, followers_count=500 + i, following_count=650 + i) for i in range(5)]
        upsert_and_score_users(db, cands, now_ts=now)

        @contextlib.contextmanager
        def _fake_session(*_a, **_kw):
            yield (None, MagicMock())

        clock = {"t": 1000.0}

        def _fake_follow_one(_page, _sniffer, cand, **_kw):
            clock["t"] += 100.0
            return {"outcome": "followed", "live_profile": cand, "error": None}

        with tempfile.TemporaryDirectory() as tmp:
            acc = {"name": "x_myself", "screen_name": "peterzhu1003", "user_data_dir": tmp, "max_follow_per_run": 20, "max_follow_per_day": 400}
            with patch("x_growth.auto_follower.persistent_browser_session", side_effect=_fake_session), \
                 patch("x_growth.auto_follower._open_and_check_account_home", return_value=("ok", None)), \
                 patch("x_growth.auto_follower.extract_logged_in_handle", return_value="peterzhu1003"), \
                 patch("x_growth.auto_follower._sync_follow_backs", return_value={"observed_followers": 0, "newly_followed_back": 0}), \
                 patch("x_growth.auto_follower._sleep_jitter"), \
                 patch("x_growth.auto_follower._follow_one", side_effect=_fake_follow_one), \
                 patch("x_growth.auto_follower.time.monotonic", side_effect=lambda: clock["t"]):
                res = follow_for_account(acc, db=db, config={"session_max_hold_seconds": 180})

        self.assertEqual(res["status"], "ok")
        self.assertTrue(res["stopped_by_hold_limit"])
        self.assertTrue(res["browser_opened"])
        self.assertEqual(res["followed_count"], 2)
        self.assertGreaterEqual(res["hold_seconds"], 180.0)

    def test_select_unfollow_candidates_48h_and_permanent_exclusion(self):
        db = _FakeDB()
        now = int(time.time())
        users = [
            _make_user("due_oldest", now=now),
            _make_user("due_newer", now=now),
            _make_user("not_due_30h", now=now),
            _make_user("already_fb", now=now),
            _make_user("already_unf", now=now),
            _make_user("failed_recent", now=now),
        ]
        upsert_and_score_users(db, users, now_ts=now)
        rec_col = db.get_collection(FOLLOW_RECORDS_COLLECTION)
        acc_cfg = {"name": "x_myself", "screen_name": "peterzhu1003"}

        record_follow_outcome(db, account_name="x_myself", account_handle="peterzhu1003", account_dir="/tmp/m", target_profile=users[0], status="followed", now_ts=now - 60 * 3600)
        record_follow_outcome(db, account_name="x_myself", account_handle="peterzhu1003", account_dir="/tmp/m", target_profile=users[1], status="followed", now_ts=now - 49 * 3600)
        record_follow_outcome(db, account_name="x_myself", account_handle="peterzhu1003", account_dir="/tmp/m", target_profile=users[2], status="followed", now_ts=now - 30 * 3600)
        record_follow_outcome(db, account_name="x_myself", account_handle="peterzhu1003", account_dir="/tmp/m", target_profile=users[3], status="followed", followed_back=True, now_ts=now - 72 * 3600)
        record_follow_outcome(db, account_name="x_myself", account_handle="peterzhu1003", account_dir="/tmp/m", target_profile=users[4], status="unfollowed", now_ts=now - 72 * 3600)
        record_follow_outcome(db, account_name="x_myself", account_handle="peterzhu1003", account_dir="/tmp/m", target_profile=users[5], status="followed", now_ts=now - 72 * 3600)
        rec_col.update_one({"account_name": "x_myself", "target_handle_lower": "failed_recent"}, {"$set": {"unfollow_last_error_ts": now - 3600}})

        due, total_due = select_unfollow_candidates(db, acc_cfg, limit=10, now_ts=now)
        self.assertEqual(total_due, 2)
        self.assertEqual([d["target_handle"] for d in due], ["due_oldest", "due_newer"])

        # "unfollowed" is permanently excluded from future follow selections
        cands, _, _ = select_candidates_for_account(db, acc_cfg, limit=10, now_ts=now)
        self.assertEqual(cands, [])

    def test_unfollow_non_followers_rescues_failed_stop_loss_and_honest_report(self):
        db = _FakeDB()
        now = int(time.time())
        acc_names = ["u_unfol", "u_rescued", "u_fail", "u_recent"]
        profiles = [_make_user(h, now=now) for h in acc_names]
        upsert_and_score_users(db, profiles, now_ts=now)
        for p, hrs in zip(profiles, [55, 52, 50, 10]):
            record_follow_outcome(db, account_name="x_myself", account_handle="peterzhu1003", account_dir="/tmp/m", target_profile=p, status="followed", now_ts=now - hrs * 3600)

        @contextlib.contextmanager
        def _fake_session(*_a, **_kw):
            yield (None, MagicMock())

        outcomes = {
            "u_unfol": {"outcome": "unfollowed", "reason": "no_follow_back_48h", "live_profile": None, "error": None},
            "u_rescued": {"outcome": "kept_followed_back", "reason": "followed_back_live", "live_profile": None, "error": None},
            "u_fail": {"outcome": "failed", "reason": None, "live_profile": None, "error": "unfollow_btn_not_found"},
        }

        with tempfile.TemporaryDirectory() as tmp:
            acc = {"name": "x_myself", "screen_name": "peterzhu1003", "user_data_dir": tmp, "max_follow_per_run": 20, "max_follow_per_day": 400}
            with patch("x_growth.auto_follower.persistent_browser_session", side_effect=_fake_session), \
                 patch("x_growth.auto_follower._open_and_check_account_home", return_value=("ok", None)), \
                 patch("x_growth.auto_follower.extract_logged_in_handle", return_value="peterzhu1003"), \
                 patch("x_growth.auto_follower._sleep_jitter"), \
                 patch("x_growth.auto_follower._unfollow_one", side_effect=lambda _p, _s, rec, **_kw: outcomes[rec["target_handle"]]):
                res = unfollow_non_followers_for_account(acc, db=db)

        self.assertEqual(res["status"], "ok")
        self.assertEqual(res["due_count"], 3)
        self.assertEqual(res["unfollowed_count"], 1)
        self.assertEqual(res["rescued_count"], 1)
        self.assertEqual(res["failed_count"], 1)
        self.assertTrue(res["browser_opened"])

        rcol = db.get_collection(FOLLOW_RECORDS_COLLECTION)
        r_unf = rcol.find_one({"account_name": "x_myself", "target_handle_lower": "u_unfol"})
        r_res = rcol.find_one({"account_name": "x_myself", "target_handle_lower": "u_rescued"})
        r_fail = rcol.find_one({"account_name": "x_myself", "target_handle_lower": "u_fail"})
        self.assertEqual(r_unf["status"], "unfollowed")
        self.assertEqual(r_unf["unfollow_reason"], "no_follow_back_48h")
        self.assertEqual(r_res["status"], "followed")
        self.assertTrue(r_res["followed_back"])
        self.assertGreater(r_res["follow_back_latency_hours"], 48.0)
        self.assertEqual(r_fail["status"], "followed")
        self.assertEqual(r_fail["unfollow_last_error"], "unfollow_btn_not_found")
        self.assertGreater(r_fail["unfollow_last_error_ts"], 0)

        rep = build_status_report(db, [acc], now_ts=now)
        self.assertEqual(rep["accounts"][0]["followed"], 4)
        self.assertEqual(rep["accounts"][0]["followed_back"], 1)
        self.assertEqual(rep["accounts"][0]["unfollowed"], 1)
        self.assertEqual(rep["accounts"][0]["unfollow_due"], 0)
        txt = format_status_text(rep)
        self.assertIn("已取关=1", txt)
        self.assertIn("满48h待取关=0", txt)

        # Also verify 180s stop-loss works inside unfollow_non_followers_for_account
        db2 = _FakeDB()
        for idx in range(3):
            p = _make_user(f"sl_{idx}", now=now)
            record_follow_outcome(db2, account_name="x_myself", account_handle="peterzhu1003", account_dir="/tmp/m", target_profile=p, status="followed", now_ts=now - (70 - idx) * 3600)
        clock = {"t": 2000.0}

        def _slow_unfollow(_p, _s, _rec, **_kw):
            clock["t"] += 100.0
            return {"outcome": "unfollowed", "reason": "no_follow_back_48h", "live_profile": None, "error": None}

        with tempfile.TemporaryDirectory() as tmp2:
            acc2 = {"name": "x_myself", "screen_name": "peterzhu1003", "user_data_dir": tmp2}
            with patch("x_growth.auto_follower.persistent_browser_session", side_effect=_fake_session), \
                 patch("x_growth.auto_follower._open_and_check_account_home", return_value=("ok", None)), \
                 patch("x_growth.auto_follower.extract_logged_in_handle", return_value="peterzhu1003"), \
                 patch("x_growth.auto_follower._sleep_jitter"), \
                 patch("x_growth.auto_follower._unfollow_one", side_effect=_slow_unfollow), \
                 patch("x_growth.auto_follower.time.monotonic", side_effect=lambda: clock["t"]):
                sl_res = unfollow_non_followers_for_account(acc2, db=db2, config={"session_max_hold_seconds": 180})
        self.assertTrue(sl_res["stopped_by_hold_limit"])
        self.assertEqual(sl_res["unfollowed_count"], 2)

    def test_run_one_cycle_unfollow_vs_daily_limit_and_halts(self):
        db = _FakeDB()
        with tempfile.TemporaryDirectory() as tmp:
            accs = [
                {"name": "acc_daily", "screen_name": "u1", "user_data_dir": str(Path(tmp) / "d"), "enabled": True},
                {"name": "acc_halt", "screen_name": "u2", "user_data_dir": str(Path(tmp) / "h"), "enabled": True},
            ]
            unf_calls = []

            def _fake_follow(acc, **_kw):
                if acc["name"] == "acc_daily":
                    return {"account_name": acc["name"], "screen_name": acc["screen_name"], "status": "daily_limit_reached", "followed_count": 0, "browser_opened": False}
                return {"account_name": acc["name"], "screen_name": acc["screen_name"], "status": "rate_limited", "followed_count": 0, "browser_opened": True}

            def _fake_unfollow(acc, **_kw):
                unf_calls.append(acc["name"])
                return {"account_name": acc["name"], "screen_name": acc["screen_name"], "status": "ok", "unfollowed_count": 0, "rescued_count": 0}

            with patch("x_growth.auto_follower.gen_db_object", return_value=db), \
                 patch("x_growth.auto_follower.follow_for_account", side_effect=_fake_follow), \
                 patch("x_growth.auto_follower.unfollow_non_followers_for_account", side_effect=_fake_unfollow):
                r = run_one_cycle(run_harvest=False, run_follow=True, run_unfollow=True, accounts=accs)

        self.assertEqual(unf_calls, ["acc_daily"])
        self.assertIn("unfollow", r["accounts"][0])
        self.assertNotIn("unfollow", r["accounts"][1])


if __name__ == "__main__":
    unittest.main()
