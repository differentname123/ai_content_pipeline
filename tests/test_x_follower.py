# -*- coding: utf-8 -*-
"""Offline unit tests for simplified X auto-follower (`x_growth/auto_follower.py`)."""
import re
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from common.mongo_db.mongo_manager import HARVESTED_POSTS_COLLECTION
from common.post_utils import AccountIsolationError
from x_growth.auto_follower import (
    FOLLOW_RECORDS_COLLECTION,
    STRATEGY_VERSION,
    TARGET_ACCOUNTS,
    assert_safe_collector_dir,
    build_status_report,
    find_follow_candidates,
    format_status_text,
    is_worth_following,
    mark_follow_backs,
    record_follow_outcome,
    resolve_target_accounts,
    run_follow_round,
)
from x_growth.x_browser_collector import extract_user_profile_from_gql_result


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
                if op == "$in" and val not in target:
                    return False
                if op == "$ne" and val == target:
                    return False
                if op == "$exists" and (bool(target) != (val is not None)):
                    return False
                if op == "$regex":
                    flags = re.IGNORECASE if "i" in str(cond.get("$options") or "") else 0
                    if not re.search(target, str(val or ""), flags):
                        return False
        elif val != cond:
            return False
    return True


class _FakeCursor(list):
    def sort(self, *_args, **_kwargs):
        return self

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

    def aggregate(self, pipeline):
        docs = list(self.docs)
        for stage in pipeline:
            if "$match" in stage:
                docs = [d for d in docs if _match_query(d, stage["$match"])]
            elif "$sort" in stage:
                for key, direction in reversed(list(stage["$sort"].items())):
                    docs.sort(key=lambda d: _get_dotted(d, key) or 0, reverse=(direction < 0))
            elif "$group" in stage:
                grouped = {}
                for d in docs:
                    uname = str(_get_dotted(d, "author.username") or "").lower()
                    if not uname:
                        continue
                    prof = _get_dotted(d, "metadata.platform_extra.author_profile") or {}
                    pub = int(d.get("publish_time") or 0)
                    lang = _get_dotted(d, "metadata.lang")
                    if uname not in grouped:
                        grouped[uname] = {
                            "_id": uname,
                            "screen_name": _get_dotted(d, "author.username"),
                            "user_id": d.get("author_id"),
                            "name": prof.get("name"),
                            "followers_count": prof.get("followers_count", 0),
                            "following_count": prof.get("following_count", 0),
                            "is_blue_verified": prof.get("is_blue_verified", False),
                            "is_verified": prof.get("is_verified", False),
                            "is_protected": prof.get("is_protected", False),
                            "description": prof.get("description", ""),
                            "langs": set(),
                            "last_active_ts": pub,
                        }
                    if lang:
                        grouped[uname]["langs"].add(lang)
                    if pub > grouped[uname]["last_active_ts"]:
                        grouped[uname]["last_active_ts"] = pub
                docs = [{**v, "langs": list(v["langs"])} for v in grouped.values()]
        return docs


class _FakeDB:
    def __init__(self):
        self.cols = {}

    def get_collection(self, name):
        if name not in self.cols:
            self.cols[name] = _FakeCollection()
        return self.cols[name]

    def create_index(self, *_args, **_kwargs):
        pass


def _make_author(**overrides):
    now = 1_800_000_000
    obj = {
        "screen_name": "good_peer",
        "user_id": "101",
        "name": "Good Peer",
        "is_blue_verified": True,
        "is_verified": True,
        "is_protected": False,
        "followers_count": 800,
        "following_count": 900,
        "description": "Shipping open tools.",
        "last_active_ts": now - 3600,
        "languages": ["en"],
    }
    obj.update(overrides)
    return obj


def _make_post(username, *, pub_ts, **prof_overrides):
    prof = _make_author(screen_name=username, **prof_overrides)
    return {
        "source": "x",
        "post_id": f"{username}_{pub_ts}",
        "publish_time": pub_ts,
        "author_id": prof["user_id"],
        "author": {"username": username},
        "metadata": {
            "lang": (prof.get("languages") or ["en"])[0],
            "platform_extra": {"author_profile": prof},
        },
    }


class TestSimpleAutoFollower(unittest.TestCase):
    NOW = 1_800_000_000

    def test_four_filter_rules_and_anti_mutual_precedence(self):
        # 1. Self / protected / not verified
        for self_h in ("", "unknown", "peterzhu1003", "hsiaohuchu"):
            ok, reason, _, kw = is_worth_following(_make_author(screen_name=self_h), now_ts=self.NOW)
            self.assertFalse(ok)
            self.assertEqual(reason, "self_or_invalid")
            self.assertIsNone(kw)

        self.assertEqual(is_worth_following(_make_author(is_protected=True), now_ts=self.NOW)[1], "protected")
        self.assertEqual(
            is_worth_following(_make_author(is_blue_verified=False, is_verified=False), now_ts=self.NOW)[1],
            "not_verified",
        )

        # 2. Followers 100..3000
        self.assertEqual(
            is_worth_following(_make_author(followers_count=99, following_count=150), now_ts=self.NOW)[1],
            "followers_out_of_range",
        )
        self.assertEqual(
            is_worth_following(_make_author(followers_count=3001, following_count=3200), now_ts=self.NOW)[1],
            "followers_out_of_range",
        )

        # 3. Ratio 0.80..3.00 and <=4500 cap
        self.assertEqual(
            is_worth_following(_make_author(followers_count=1000, following_count=790), now_ts=self.NOW)[1],
            "ratio_out_of_range",
        )
        self.assertEqual(
            is_worth_following(_make_author(followers_count=500, following_count=1550), now_ts=self.NOW)[1],
            "ratio_out_of_range",
        )
        self.assertEqual(
            is_worth_following(_make_author(followers_count=2000, following_count=4600), now_ts=self.NOW)[1],
            "near_5k_follow_cap",
        )

        # 4. Active within 7 days & anti-mutual beats mutual bio
        self.assertEqual(
            is_worth_following(_make_author(last_active_ts=self.NOW - 8 * 86400), now_ts=self.NOW)[1],
            "inactive_over_window",
        )
        for bio in ("No follow back", "不互关，勿扰", "フォロバしません", "Follow back? No F4F!"):
            ok, reason, has_mb, kw = is_worth_following(_make_author(description=bio), now_ts=self.NOW)
            self.assertFalse(ok)
            self.assertEqual(reason, "anti_mutual_bio")
            self.assertFalse(has_mb)
            self.assertIsNone(kw)

        ok_mut, reason_mut, has_mb_true, kw_mut = is_worth_following(
            _make_author(description="Indie dev #F4F 互关必回"), now_ts=self.NOW
        )
        self.assertTrue(ok_mut)
        self.assertIsNone(reason_mut)
        self.assertTrue(has_mb_true)
        self.assertEqual(kw_mut, "F4F")

    def test_ranking_and_funnel_breakdown(self):
        db = _FakeDB()
        posts_col = db.get_collection(HARVESTED_POSTS_COLLECTION)
        posts_col.docs.extend([
            _make_post("regular_older", pub_ts=self.NOW - 7200, description="Coffee lover", languages=["ja"]),
            _make_post("regular_newest", pub_ts=self.NOW - 600, description="AI builder", languages=["en"]),
            _make_post("mutual_older", pub_ts=self.NOW - 86400, description="Always follow back #F4F", languages=["en"]),
            _make_post("mutual_newer", pub_ts=self.NOW - 1800, description="有关必回，一起成长", languages=["zh"]),
            _make_post("already_done", pub_ts=self.NOW - 300, description="#F4F"),
            _make_post("unverified_user", pub_ts=self.NOW - 500, is_blue_verified=False, is_verified=False),
            _make_post("anti_user", pub_ts=self.NOW - 500, description="No follow back"),
        ])
        db.get_collection(FOLLOW_RECORDS_COLLECTION).docs.append({
            "account_name": "x_myself",
            "target_handle_lower": "already_done",
            "status": "followed",
        })

        cands, total, funnel = find_follow_candidates(
            db, {"name": "x_myself", "screen_name": "peterzhu1003"}, limit=10, now_ts=self.NOW
        )
        self.assertEqual(total, 4)
        self.assertEqual(
            [c["screen_name"] for c in cands],
            ["mutual_newer", "mutual_older", "regular_newest", "regular_older"],
        )
        self.assertEqual(funnel["scanned_7d_authors"], 7)
        self.assertEqual(funnel["excluded_already_recorded"], 1)
        self.assertEqual(funnel["reject_counts"]["not_verified"], 1)
        self.assertEqual(funnel["reject_counts"]["anti_mutual_bio"], 1)
        self.assertEqual(funnel["eligible_mutual_bio"], 2)
        self.assertEqual(funnel["eligible_top_langs"]["en"], 2)
        self.assertEqual(cands[0]["mutual_bio_keyword"], "有关必回")
        self.assertEqual(cands[0]["primary_lang"], "zh")
        self.assertEqual(cands[0]["post_age_hours"], 0.5)

    def test_default_pool_only_myself_with_higher_limits_and_collector_guard(self):
        pool = resolve_target_accounts()
        self.assertEqual(len(pool), 1)
        self.assertEqual(pool[0]["name"], "x_myself")
        self.assertEqual(pool[0]["screen_name"], "peterzhu1003")
        self.assertEqual(pool[0]["max_follow_per_run"], TARGET_ACCOUNTS[0]["max_follow_per_run"])
        self.assertEqual(pool[0]["max_follow_per_day"], TARGET_ACCOUNTS[0]["max_follow_per_day"])
        self.assertGreaterEqual(pool[0]["max_follow_per_run"], 8)
        self.assertGreaterEqual(pool[0]["max_follow_per_day"], 80)

        for bad_dir in (r"W:\temp\x_myself", "./browser_sessions/x_myself"):
            with self.assertRaises(AccountIsolationError):
                assert_safe_collector_dir(bad_dir)
        assert_safe_collector_dir(r"W:\temp\x_xiaohao")

    def test_run_follow_round_preserves_per_account_limits_in_dry_run(self):
        db = _FakeDB()
        now = int(time.time())
        posts_col = db.get_collection(HARVESTED_POSTS_COLLECTION)
        for i in range(30):
            posts_col.docs.append(_make_post(f"cand_{i}", pub_ts=now - 600, last_active_ts=now - 600, description="#F4F"))
        custom_acc = {
            "name": "x_myself",
            "screen_name": "peterzhu1003",
            "user_data_dir": "W:/temp/x_myself",
            "max_follow_per_run": 25,
            "max_follow_per_day": 600,
        }
        with patch("x_growth.auto_follower.gen_db_object", return_value=db):
            res = run_follow_round(round_no=1, accounts=[custom_acc], dry_run=True)
        self.assertEqual(res["accounts"][0]["max_follow_per_day"], 600)
        self.assertEqual(res["accounts"][0]["followed_count"], 25)

    def test_telemetry_follow_backs_and_analytics_report(self):
        db = _FakeDB()
        t_mature = self.NOW - 30 * 3600
        t_recent = self.NOW - 4 * 3600

        record_follow_outcome(
            db,
            account_name="x_myself",
            account_handle="peterzhu1003",
            account_dir="W:/temp/x_myself",
            target_profile=_make_author(
                screen_name="Alice_F4F",
                followers_count=400,
                following_count=520,
                description="Follow back #F4F",
                languages=["en"],
                last_active_ts=t_mature - 1800,
            ),
            status="followed",
            candidate_rank=1,
            eligible_pool_size=250,
            now_ts=t_mature,
        )
        record_follow_outcome(
            db,
            account_name="x_myself",
            account_handle="peterzhu1003",
            account_dir="W:/temp/x_myself",
            target_profile=_make_author(
                screen_name="bob_peer",
                followers_count=1800,
                following_count=1600,
                description="Normal bio",
                languages=["ja"],
                last_active_ts=t_mature - 40 * 3600,
            ),
            status="followed",
            candidate_rank=2,
            eligible_pool_size=250,
            now_ts=t_mature,
        )
        record_follow_outcome(
            db,
            account_name="x_myself",
            account_handle="peterzhu1003",
            account_dir="W:/temp/x_myself",
            target_profile=_make_author(
                screen_name="carol_fresh",
                followers_count=900,
                following_count=1600,
                description="Normal bio",
                languages=["zh"],
                last_active_ts=t_recent - 80 * 3600,
            ),
            status="followed",
            candidate_rank=3,
            eligible_pool_size=250,
            now_ts=t_recent,
        )

        alice_rec = db.get_collection(FOLLOW_RECORDS_COLLECTION).find_one(
            {"account_name": "x_myself", "target_handle_lower": "alice_f4f"}
        )
        self.assertEqual(alice_rec["strategy_version"], STRATEGY_VERSION)
        self.assertEqual(alice_rec["primary_lang"], "en")
        self.assertEqual(alice_rec["post_age_hours_at_follow"], 0.5)
        self.assertEqual(alice_rec["candidate_rank"], 1)
        self.assertEqual(alice_rec["eligible_pool_size"], 250)
        self.assertIn("followed_hour_cn", alice_rec)
        self.assertTrue(alice_rec["has_mutual_bio"])
        self.assertEqual(alice_rec["mutual_bio_keyword"], "Follow back")

        fb = mark_follow_backs(db, account_name="x_myself", follower_handles=["alice_f4f"], now_ts=self.NOW)
        self.assertEqual(fb["newly_followed_back"], 1)
        self.assertEqual(fb["new_handles"], ["Alice_F4F"])
        self.assertEqual(len(fb["matched_details"]), 1)
        self.assertEqual(fb["matched_details"][0]["latency_hours"], 30.0)

        alice_after = db.get_collection(FOLLOW_RECORDS_COLLECTION).find_one(
            {"account_name": "x_myself", "target_handle_lower": "alice_f4f"}
        )
        bob_after = db.get_collection(FOLLOW_RECORDS_COLLECTION).find_one(
            {"account_name": "x_myself", "target_handle_lower": "bob_peer"}
        )
        self.assertEqual(alice_after["follow_back_check_count"], 1)
        self.assertEqual(bob_after["follow_back_check_count"], 1)
        self.assertEqual(alice_after["last_follow_back_check_ts"], self.NOW)

        report = build_status_report(
            db,
            [{"name": "x_myself", "screen_name": "peterzhu1003", "max_follow_per_day": 80}],
            now_ts=self.NOW,
        )
        acc = report["accounts"][0]
        self.assertEqual(acc["follows_24h"], 1)
        self.assertEqual(acc["total_followed"], 3)
        self.assertEqual(acc["followed_back"], 1)
        self.assertEqual(acc["follow_back_rate_pct"], 33.3)

        an = report["analytics"]
        self.assertEqual(an["mature_24h_followed"], 2)
        self.assertEqual(an["mature_24h_followed_back"], 1)
        self.assertEqual(an["mature_24h_rate_pct"], 50.0)
        self.assertEqual(an["median_follow_back_latency_hours"], 30.0)
        self.assertEqual(an["by_mutual_bio"]["mutual_bio"]["rate_pct"], 100.0)
        self.assertEqual(an["by_mutual_bio"]["normal"]["rate_pct"], 0.0)
        self.assertEqual(an["by_lang"]["en"]["rate_pct"], 100.0)
        self.assertEqual(an["by_lang"]["ja"]["followed"], 1)
        self.assertEqual(an["by_followers"]["100~499"]["rate_pct"], 100.0)
        self.assertEqual(an["by_ff_ratio"]["1.0~1.5"]["rate_pct"], 100.0)
        self.assertEqual(an["by_post_age"]["<=24h"]["rate_pct"], 100.0)

        rendered = format_status_text(report)
        self.assertIn("策略效果分析", rendered)
        self.assertIn("满24h成熟样本=1/2 (50.0%)", rendered)
        self.assertIn("中位回关耗时=30.0h", rendered)

    def test_round_cooldown_and_protected_sniffer(self):
        db = _FakeDB()
        now = int(time.time())
        posts_col = db.get_collection(HARVESTED_POSTS_COLLECTION)
        posts_col.docs.append({
            "source": "x",
            "post_id": "recent_browser",
            "publish_time": now - 600,
            "db_update_time": now - 60,
            "author": {"username": "peer_0"},
            "metadata": {
                "lang": "en",
                "platform_extra": {
                    "category": "x_browser_home",
                    "author_profile": _make_author(screen_name="peer_0", last_active_ts=now - 600),
                },
            },
        })
        for i in range(1, 32):
            posts_col.docs.append(_make_post(f"peer_{i}", pub_ts=now - 600, last_active_ts=now - 600))

        with tempfile.TemporaryDirectory() as tmp:
            acc_ok_dir = Path(tmp) / "ok_acc"
            acc_ok_dir.mkdir()
            acc_miss_dir = Path(tmp) / "missing_acc"
            pool = [
                {"name": "acc_ok", "screen_name": "u1", "user_data_dir": str(acc_ok_dir)},
                {"name": "acc_miss", "screen_name": "u2", "user_data_dir": str(acc_miss_dir)},
            ]
            cd_map = {}

            def _fake_run(acc_cfg, **_kwargs):
                if acc_cfg["name"] == "acc_ok":
                    return {"account_name": "acc_ok", "status": "rate_limited", "follows_24h": 0, "max_follow_per_day": 80, "followed_count": 0, "followed_items": []}
                return {"account_name": "acc_miss", "status": "session_missing", "follows_24h": 0, "max_follow_per_day": 80, "followed_count": 0, "followed_items": []}

            with patch("x_growth.auto_follower.gen_db_object", return_value=db), \
                 patch("x_growth.auto_follower.run_account_follow", side_effect=_fake_run), \
                 patch("x_growth.auto_follower._save_status_heartbeat"):
                res = run_follow_round(round_no=1, accounts=pool, cooldown_until=cd_map)

            self.assertEqual(res["refresh"]["fetch_mode"], "reused_recent_harvest")
            self.assertIn(str(acc_ok_dir), cd_map)
            self.assertNotIn(str(acc_miss_dir), cd_map)

        prof = extract_user_profile_from_gql_result({
            "__typename": "User",
            "rest_id": "777",
            "is_blue_verified": True,
            "privacy": {"protected": True},
            "core": {"screen_name": "LockedBlue", "name": "Locked"},
            "relationship_counts": {"followers": 500, "following": 600},
        })
        self.assertTrue(prof["is_protected"])


if __name__ == "__main__":
    unittest.main()
