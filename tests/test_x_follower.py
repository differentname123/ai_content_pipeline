# -*- coding: utf-8 -*-
"""Offline unit tests for X mutual-follow selection, safety isolation, follow-back tracking, and daemon behavior."""
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from common.post_utils import AccountIsolationError
from x_growth.auto_follower import (
    _assert_safe_collector_dir,
    _resolve_follower_pool,
    run_follow_round,
)
from x_growth.follow_selector import (
    FOLLOW_CANDIDATES_COLLECTION_NAME,
    FOLLOW_RECORDS_COLLECTION_NAME,
    build_follow_ops_summary,
    evaluate_follow_candidate,
    mark_follow_backs,
    record_follow_outcome,
    select_follow_candidates_for_account,
    upsert_discovered_users,
)
from x_growth.growth_config import AUTO_COMMENT_FIELD, POSTS_COLLECTION_NAME
from x_growth.x_browser_collector import (
    XPassiveGraphQLSniffer,
    extract_user_profile_from_gql_result,
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
        if k == "$and":
            if not all(_match_query(doc, sub) for sub in cond):
                return False
            continue
        val = _get_dotted(doc, k)
        if isinstance(cond, dict):
            for op, target in cond.items():
                if op == "$gte" and (val is None or val < target):
                    return False
                if op == "$gt" and (val is None or val <= target):
                    return False
                if op == "$in" and val not in target:
                    return False
                if op == "$ne" and val == target:
                    return False
                if op == "$exists":
                    exists = val is not None
                    if bool(target) != exists:
                        return False
                if op == "$regex":
                    import re

                    flags = re.IGNORECASE if "i" in str(cond.get("$options") or "") else 0
                    if not re.search(target, str(val or ""), flags):
                        return False
        else:
            if val != cond:
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
        target = None
        for d in self.docs:
            if _match_query(d, query):
                target = d
                break
        if target is None:
            if not upsert:
                return
            target = dict(query)
            for k, v in (update.get("$setOnInsert") or {}).items():
                _set_dotted(target, k, v)
            self.docs.append(target)
        for k, v in (update.get("$set") or {}).items():
            _set_dotted(target, k, v)

    def bulk_write(self, ops, ordered=False):
        for op in ops:
            self.update_one(op._filter, op._doc, upsert=bool(op._upsert))

    def aggregate(self, _pipeline):
        return []


class _FakeDB:
    def __init__(self):
        self.cols = {}
        self.index_calls = []

    def get_collection(self, name):
        if name not in self.cols:
            self.cols[name] = _FakeCollection()
        return self.cols[name]

    def create_index(self, col_name, keys, unique=False):
        self.index_calls.append((col_name, keys, unique))


def _base_profile(**overrides):
    now = 1_800_000_000
    prof = {
        "user_id": "1001",
        "screen_name": "valid_peer",
        "name": "Valid Peer",
        "is_blue_verified": True,
        "is_verified": True,
        "is_protected": False,
        "followers_count": 800,
        "following_count": 900,
        "statuses_count": 120,
        "description": "Building open tools and writing notes.",
        "last_active_ts": now - 12 * 3600,
        "languages": ["en"],
    }
    prof.update(overrides)
    return prof


class TestFollowSelector(unittest.TestCase):
    NOW = 1_800_000_000

    def test_six_hard_gates_rejections(self):
        # 1. Self & invalid handles
        for self_h in ("", "unknown", "peterzhu1003", "2434954452qqc", "hsiaohuchu"):
            res = evaluate_follow_candidate(_base_profile(screen_name=self_h), now_ts=self.NOW)
            self.assertFalse(res["worth_following"])
            self.assertEqual(res["reject_reason"], "self_or_invalid_handle")

        # 2. Protected / private account
        res_prot = evaluate_follow_candidate(_base_profile(is_protected=True), now_ts=self.NOW)
        self.assertFalse(res_prot["worth_following"])
        self.assertEqual(res_prot["reject_reason"], "protected")

        # 3. Not verified
        res_unv = evaluate_follow_candidate(
            _base_profile(is_blue_verified=False, is_verified=False), now_ts=self.NOW
        )
        self.assertFalse(res_unv["worth_following"])
        self.assertEqual(res_unv["reject_reason"], "not_verified")

        # 4. Followers out of range (<100 or >3000)
        for fc in (99, 3001):
            res_fc = evaluate_follow_candidate(
                _base_profile(followers_count=fc, following_count=fc + 100), now_ts=self.NOW
            )
            self.assertFalse(res_fc["worth_following"])
            self.assertEqual(res_fc["reject_reason"], "followers_out_of_range")

        # 5. Following too low / near 5k cap / ratio out of range
        res_fg_low = evaluate_follow_candidate(
            _base_profile(followers_count=100, following_count=49), now_ts=self.NOW
        )
        self.assertEqual(res_fg_low["reject_reason"], "following_too_low")

        res_cap = evaluate_follow_candidate(
            _base_profile(followers_count=2000, following_count=4600), now_ts=self.NOW
        )
        self.assertEqual(res_cap["reject_reason"], "near_5k_follow_cap")

        # Exception allowed when followers >= 4500 and ratio <= 1.05 (with custom max_followers)
        res_cap_ok = evaluate_follow_candidate(
            _base_profile(followers_count=4600, following_count=4600),
            now_ts=self.NOW,
            config={"max_followers": 5000},
        )
        self.assertTrue(res_cap_ok["worth_following"])

        res_low_r = evaluate_follow_candidate(
            _base_profile(followers_count=1000, following_count=790), now_ts=self.NOW
        )
        self.assertEqual(res_low_r["reject_reason"], "ratio_out_of_range")

        res_high_r = evaluate_follow_candidate(
            _base_profile(followers_count=500, following_count=1600), now_ts=self.NOW
        )
        self.assertEqual(res_high_r["reject_reason"], "ratio_out_of_range")

        # 6. Statuses low / inactive / anti-mutual bio
        res_st = evaluate_follow_candidate(_base_profile(statuses_count=19), now_ts=self.NOW)
        self.assertEqual(res_st["reject_reason"], "statuses_too_low")

        res_old = evaluate_follow_candidate(
            _base_profile(last_active_ts=self.NOW - 8 * 86400), now_ts=self.NOW
        )
        self.assertEqual(res_old["reject_reason"], "inactive_over_window")

    def test_anti_mutual_bio_precedes_mutual_bio(self):
        for bio in (
            "I don't follow back, no f4f please",
            "No follow back / one way account",
            "专业吐槽，不互关，勿扰",
            "フォロバしません。相互フォロー不可",
            "Follow back? No F4F!",
        ):
            res = evaluate_follow_candidate(_base_profile(description=bio), now_ts=self.NOW)
            self.assertFalse(res["worth_following"], f"Bio should be rejected: {bio}")
            self.assertEqual(res["reject_reason"], "anti_mutual_bio")

    def test_mutual_bio_signals_and_tier_s(self):
        for bio in (
            "Indie hacker #F4F building in public",
            "长期主义，有关必回，一起成长",
            "AI 交流互关中",
            "Always follow you back!",
            "フォロバ100 相互フォロー希望",
        ):
            res = evaluate_follow_candidate(
                _base_profile(
                    followers_count=800,
                    following_count=900,  # prime ratio 1.125 -> +25
                    last_active_ts=self.NOW - 6 * 3600,  # <=24h -> +15
                    description=bio,  # mutual bio -> +20
                ),
                now_ts=self.NOW,
            )
            self.assertTrue(res["worth_following"], f"Bio should qualify: {bio}")
            self.assertEqual(res["tier"], "S")
            self.assertTrue(res["has_mutual_bio"])
            self.assertIn("mutual_bio", res["signals"])
            self.assertEqual(res["follow_score"], 100)  # 40 + 25 + 15 + 20

    def test_warm_interaction_is_account_scoped_and_relaxes_ratio(self):
        db = _FakeDB()
        # Author has ff_ratio = 0.60 (rejected without warm interaction, accepted WITH warm interaction)
        upsert_discovered_users(
            db,
            [
                _base_profile(
                    screen_name="warm_author",
                    followers_count=1000,
                    following_count=600,
                    last_active_ts=self.NOW - 10 * 3600,
                )
            ],
            now_ts=self.NOW,
        )
        # Only x_myself (@peterzhu1003) received a reply/like from warm_author in harvested_social_posts
        posts_col = db.get_collection(POSTS_COLLECTION_NAME)
        posts_col.docs.append(
            {
                "source": "x",
                "post_id": "p_1",
                "author": {"username": "warm_author"},
                AUTO_COMMENT_FIELD: {
                    "consumed": True,
                    "consumed_by": "@peterzhu1003",
                    "consumed_dir": str(Path("W:/temp/x_myself").expanduser().resolve()),
                    "consumed_at_ts": self.NOW - 3600,
                    "comment_engagement": {"like_count": 1, "comment_count": 0},
                },
            }
        )

        # x_myself sees warm_author as Tier S warm lead
        myself_cands, myself_stats = select_follow_candidates_for_account(
            db,
            account_name="x_myself",
            account_dir="W:/temp/x_myself",
            account_handle="peterzhu1003",
            limit=10,
            now_ts=self.NOW,
        )
        self.assertEqual(len(myself_cands), 1)
        self.assertEqual(myself_cands[0]["screen_name"], "warm_author")
        self.assertEqual(myself_cands[0]["tier"], "S")
        self.assertTrue(myself_cands[0]["has_prior_interaction"])
        self.assertIn("warm_interaction", myself_cands[0]["signals"])
        self.assertEqual(myself_stats["warm_leads_available"], 1)

        # x_dahao must NOT inherit x_myself's warm relaxation (ratio=0.60 stays rejected for x_dahao)
        dahao_cands, dahao_stats = select_follow_candidates_for_account(
            db,
            account_name="x_dahao",
            account_dir="W:/temp/x_dahao",
            account_handle="2434954452qqc",
            limit=10,
            now_ts=self.NOW,
        )
        self.assertEqual(len(dahao_cands), 0)
        self.assertEqual(dahao_stats["warm_leads_available"], 0)

    def test_tier_a_b_boundaries_and_ranking_order(self):
        db = _FakeDB()
        upsert_discovered_users(
            db,
            [
                # Tier B: active 5 days ago (>72h)
                _base_profile(
                    screen_name="tier_b_old",
                    followers_count=800,
                    following_count=900,
                    last_active_ts=self.NOW - 5 * 86400,
                ),
                # Tier B: followers=2800 (>2500)
                _base_profile(
                    screen_name="tier_b_big",
                    followers_count=2800,
                    following_count=2900,
                    last_active_ts=self.NOW - 6 * 3600,
                ),
                # Tier A: prime ratio 1.1, <=72h, 150..2500
                _base_profile(
                    screen_name="tier_a_user",
                    followers_count=500,
                    following_count=550,
                    last_active_ts=self.NOW - 30 * 3600,
                ),
                # Tier S: mutual bio
                _base_profile(
                    screen_name="tier_s_user",
                    followers_count=400,
                    following_count=360,  # 0.90 good_ratio
                    last_active_ts=self.NOW - 48 * 3600,
                    description="Let's grow together! #F4F",
                ),
            ],
            now_ts=self.NOW,
        )

        ranked, stats = select_follow_candidates_for_account(
            db, account_name="x_myself", account_handle="peterzhu1003", limit=10, now_ts=self.NOW
        )
        self.assertEqual(
            [c["screen_name"] for c in ranked],
            ["tier_s_user", "tier_a_user", "tier_b_big", "tier_b_old"],
        )
        self.assertEqual(stats["tiers"], {"S": 1, "A": 1, "B": 2})

    def test_follow_back_tracking_and_ops_summary(self):
        db = _FakeDB()
        t0 = self.NOW - 10 * 3600
        ev_s = evaluate_follow_candidate(
            _base_profile(screen_name="alice_f4f", description="互关必回"), now_ts=t0
        )
        ev_a = evaluate_follow_candidate(
            _base_profile(screen_name="bob_peer", followers_count=500, following_count=600), now_ts=t0
        )

        record_follow_outcome(
            db,
            account_name="x_myself",
            account_handle="peterzhu1003",
            account_dir="W:/temp/x_myself",
            target_profile=_base_profile(screen_name="alice_f4f", followers_count=800, following_count=900),
            status="followed",
            evaluation=ev_s,
            now_ts=t0,
        )
        record_follow_outcome(
            db,
            account_name="x_myself",
            account_handle="peterzhu1003",
            account_dir="W:/temp/x_myself",
            target_profile=_base_profile(screen_name="bob_peer", followers_count=500, following_count=600),
            status="followed",
            evaluation=ev_a,
            now_ts=t0,
        )

        fb_res = mark_follow_backs(
            db,
            account_name="x_myself",
            follower_handles=["Alice_F4F", "random_fan"],
            now_ts=self.NOW,
        )
        self.assertEqual(fb_res["newly_followed_back"], 1)
        self.assertEqual(fb_res["new_handles"], ["alice_f4f"])

        rec = db.get_collection(FOLLOW_RECORDS_COLLECTION_NAME).find_one(
            {"account_name": "x_myself", "target_handle_lower": "alice_f4f"}
        )
        self.assertTrue(rec["followed_back"])
        self.assertEqual(rec["follow_back_latency_hours"], 10.0)

        summary = build_follow_ops_summary(
            db,
            [{"name": "x_myself", "screen_name": "peterzhu1003", "user_data_dir": "W:/temp/x_myself"}],
            now_ts=self.NOW,
        )
        acc_sum = summary["accounts"][0]
        self.assertEqual(acc_sum["total_followed"], 2)
        self.assertEqual(acc_sum["followed_back"], 1)
        self.assertEqual(acc_sum["follow_back_rate_pct"], 50.0)
        self.assertEqual(summary["by_tier"]["S"]["rate_pct"], 100.0)
        self.assertEqual(summary["by_tier"]["A"]["rate_pct"], 0.0)
        self.assertEqual(summary["by_signal"]["mutual_bio"]["followed_back"], 1)


class TestAutoFollowerSafetyAndPool(unittest.TestCase):
    def test_default_pool_has_myself_and_dahao_without_xiaohao(self):
        pool = _resolve_follower_pool()
        names = [a["name"] for a in pool]
        self.assertEqual(names, ["x_myself", "x_dahao"])
        self.assertNotIn("x_xiaohao", names)

    def test_collector_isolation_blocks_myself_and_dahao(self):
        for bad_dir in (r"W:\temp\x_myself", r"W:\temp\x_dahao", "./browser_sessions/x_myself"):
            with self.assertRaises(AccountIsolationError):
                _assert_safe_collector_dir(bad_dir)
        # x_xiaohao is allowed as collector
        _assert_safe_collector_dir(r"W:\temp\x_xiaohao")

    def test_session_missing_never_triggers_cooldown_while_rate_limit_does(self):
        db = _FakeDB()
        now = int(time.time())
        db.get_collection(POSTS_COLLECTION_NAME).docs.append(
            {
                "source": "x",
                "post_id": "recent_1",
                "db_update_time": now - 60,
                "metadata": {"platform_extra": {"category": "x_browser_home"}},
            }
        )
        # Populate enough qualified candidates so refresh reuses DB without launching collector
        upsert_discovered_users(
            db,
            [
                _base_profile(
                    screen_name=f"peer_{i}",
                    followers_count=500 + i,
                    following_count=600 + i,
                    last_active_ts=now - 3600,
                )
                for i in range(32)
            ],
            now_ts=now,
        )

        with tempfile.TemporaryDirectory() as tmp:
            existing_myself = Path(tmp) / "acc_myself"
            existing_myself.mkdir()
            missing_dahao = Path(tmp) / "acc_dahao_missing"

            test_pool = [
                {
                    "name": "x_myself",
                    "screen_name": "peterzhu1003",
                    "user_data_dir": str(existing_myself),
                    "overrides": {"max_follow_per_run": 1, "max_follow_per_day": 15},
                },
                {
                    "name": "x_dahao",
                    "screen_name": "2434954452qqc",
                    "user_data_dir": str(missing_dahao),
                    "overrides": {"max_follow_per_run": 1, "max_follow_per_day": 25},
                },
            ]

            cooldown_map = {}

            def _fake_exec(acc_cfg, **_kwargs):
                if acc_cfg["name"] == "x_myself":
                    return {
                        "account_name": "x_myself",
                        "screen_name": "peterzhu1003",
                        "user_data_dir": acc_cfg["user_data_dir"],
                        "status": "rate_limited",
                        "dry_run": False,
                        "warning": "code=161",
                        "follows_24h": 0,
                        "max_follow_per_day": 15,
                        "followed_count": 0,
                        "followed_items": [],
                        "skipped_items": [],
                    }
                return {
                    "account_name": "x_dahao",
                    "screen_name": "2434954452qqc",
                    "user_data_dir": acc_cfg["user_data_dir"],
                    "status": "session_missing",
                    "dry_run": False,
                    "warning": "missing dir",
                    "follows_24h": 0,
                    "max_follow_per_day": 25,
                    "followed_count": 0,
                    "followed_items": [],
                    "skipped_items": [],
                }

            with patch("x_growth.auto_follower.gen_db_object", return_value=db), \
                 patch("x_growth.auto_follower.execute_follows_for_account", side_effect=_fake_exec), \
                 patch("x_growth.auto_follower._save_runtime_status_heartbeat"), \
                 patch("x_growth.auto_follower._sleep_jitter"):
                res = run_follow_round(round_no=1, accounts=test_pool, cooldown_until=cooldown_map)

            self.assertEqual(res["refresh"]["fetch_mode"], "reused_recent_harvest")
            self.assertIn(str(existing_myself), cooldown_map)
            self.assertNotIn(str(missing_dahao), cooldown_map)

    def test_sniffer_captures_protected_and_exports_users(self):
        gql_user = {
            "__typename": "User",
            "rest_id": "777",
            "is_blue_verified": True,
            "privacy": {"protected": True},
            "core": {"screen_name": "LockedBlue", "name": "Locked"},
            "relationship_counts": {"followers": 500, "following": 600},
            "tweet_counts": {"tweets": 120},
            "profile_bio": {"description": "Locked account"},
        }
        prof = extract_user_profile_from_gql_result(gql_user)
        self.assertIsNotNone(prof)
        self.assertTrue(prof["is_protected"])
        self.assertTrue(prof["is_blue_verified"])

        sniffer = XPassiveGraphQLSniffer()
        sniffer.register_user(prof, source_role="comment_author", post_lang="en-US")
        sniffer.register_user(prof, source_role="comment_author", post_lang="ja")
        exported = sniffer.export_discovered_users()
        self.assertEqual(len(exported), 1)
        self.assertEqual(exported[0]["observed_langs"], ["en", "ja"])


if __name__ == "__main__":
    unittest.main()
