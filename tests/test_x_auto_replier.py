# -*- coding: utf-8 -*-
"""Offline unit tests for parallel X comment auto-replier (`x_growth/auto_replier.py`)."""
import ast
import contextlib
import pathlib
import re
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from common.llm_gateway import render_prompt_template
from platforms.playwright_base import BusinessErrorException, UserDataDirLockTimeoutError
from x_growth.auto_replier import (
    ACCOUNTS,
    ACTION_DELAY_RANGE,
    AUTO_REPLIER_PROMPT_DIR,
    COLLECT_INTERVAL_SECONDS,
    GENERATE_INTERVAL_SECONDS,
    GENERATOR_MODEL_GROUP,
    MAX_REPLIES_PER_USER_PER_POST,
    POST_CHECKPOINTS_COLLECTION,
    PROMPT_TEMPLATE_FILENAME,
    REPLY_INBOX_COLLECTION,
    REPLY_INTERVAL_SECONDS,
    RISK_COOLDOWN_SECONDS,
    RUN_MODE,
    SESSION_MAX_HOLD_SECONDS,
    XReplyInboxSniffer,
    build_status_report,
    collect_replies_for_account,
    format_status_text,
    generate_replies_from_db,
    publish_replies_for_account,
    resolve_accounts,
    run_parallel_daemon,
    upsert_collected_replies,
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


def _gql_tweet(
    rest_id,
    username,
    *,
    uid="uid_1",
    full_text="hello world",
    in_reply_to_status_id=None,
    in_reply_to_user_id=None,
    in_reply_to_screen_name=None,
    conversation_id=None,
    reply_count=0,
    lang="en",
    created_at="Sat Nov 21 10:00:00 +0000 2026",
    display_text_range=None,
    media=None,
    urls=None,
    retweeted=None,
    quoted=None,
):
    legacy = {
        "id_str": str(rest_id),
        "user_id_str": str(uid),
        "created_at": created_at,
        "lang": lang,
        "reply_count": reply_count,
        "full_text": full_text,
        "entities": {"urls": urls or [], "media": media or []},
    }
    if display_text_range is not None:
        legacy["display_text_range"] = display_text_range
    if media:
        legacy["extended_entities"] = {"media": media}
    if in_reply_to_status_id:
        legacy["in_reply_to_status_id_str"] = str(in_reply_to_status_id)
    if in_reply_to_user_id:
        legacy["in_reply_to_user_id_str"] = str(in_reply_to_user_id)
    if in_reply_to_screen_name:
        legacy["in_reply_to_screen_name"] = str(in_reply_to_screen_name)
    if conversation_id:
        legacy["conversation_id_str"] = str(conversation_id)
    if retweeted:
        legacy["retweeted_status_result"] = {"result": retweeted}

    node = {
        "__typename": "Tweet",
        "rest_id": str(rest_id),
        "core": {
            "user_results": {
                "result": {
                    "__typename": "User",
                    "rest_id": str(uid),
                    "core": {"screen_name": username, "name": f"Name {username}"},
                    "legacy": {"screen_name": username, "name": f"Name {username}"},
                }
            }
        },
        "legacy": legacy,
    }
    if quoted:
        node["quoted_status_result"] = {"result": quoted}
    return node


def _make_fake_page(*, url="https://x.com/home", status_ok=True, visible_selectors=(), disabled_selectors=()):
    page = MagicMock()
    page.url = url

    def _locator(sel):
        loc = MagicMock()
        loc.first = loc
        is_vis = any(tok in sel for tok in visible_selectors)
        loc.count.return_value = 1 if is_vis else 0
        loc.is_visible.return_value = is_vis
        is_dis = any(tok in sel for tok in disabled_selectors)
        loc.is_disabled.return_value = is_dis
        loc.get_attribute.side_effect = lambda attr: ("true" if (attr == "aria-disabled" and is_dis) else ("/peterzhu1003" if attr == "href" else ""))
        loc.locator.side_effect = _locator
        return loc

    page.locator.side_effect = _locator
    resp = MagicMock()
    resp.status = 200 if status_ok else 404
    page.goto.return_value = resp
    return page


class TestAutoReplier(unittest.TestCase):
    def test_architecture_isolation_config_and_prompt_rendering(self):
        mod_path = pathlib.Path(__file__).resolve().parent.parent / "x_growth" / "auto_replier.py"
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

        self.assertEqual(RUN_MODE, "daemon")
        self.assertEqual(COLLECT_INTERVAL_SECONDS, 3600)
        self.assertEqual(GENERATE_INTERVAL_SECONDS, 3600)
        self.assertEqual(REPLY_INTERVAL_SECONDS, 3600)
        self.assertEqual(GENERATOR_MODEL_GROUP, "low")
        self.assertEqual(MAX_REPLIES_PER_USER_PER_POST, 1)
        self.assertLessEqual(SESSION_MAX_HOLD_SECONDS, 180)
        self.assertEqual(ACCOUNTS[0]["name"], "x_myself")

        pool = resolve_accounts()
        self.assertEqual([a["name"] for a in pool], ["x_myself"])

        # Supports user passing ONLY user_data_dir without screen_name
        with tempfile.TemporaryDirectory() as tmp:
            p_only = resolve_accounts([{"user_data_dir": tmp}])
            self.assertEqual(len(p_only), 1)
            self.assertEqual(p_only[0]["screen_name"], "")
            self.assertEqual(p_only[0]["name"], Path(tmp).name)

        rendered, meta = render_prompt_template(
            PROMPT_TEMPLATE_FILENAME,
            {
                "ACCOUNT_HANDLE": "peterzhu1003",
                "MY_POST_TEXT": "Shipping small tools every day.",
                "MY_POST_HAS_MEDIA": "否",
                "PARENT_CONTEXT_BLOCK": "该评论直接回复你的上述原帖。",
                "REPLY_AUTHOR": "@alice",
                "REPLY_TEXT": "Totally agree, consistency wins.",
                "REPLY_HAS_MEDIA": "否",
                "MAX_WEIGHTED_CHARS": "240",
            },
            prompt_dir=AUTO_REPLIER_PROMPT_DIR,
        )
        self.assertEqual(meta["prompt_file"], PROMPT_TEMPLATE_FILENAME)
        self.assertNotIn("{{", rendered)
        self.assertNotIn("}}", rendered)
        self.assertIn("Shipping small tools every day.", rendered)

    def test_sniffer_parses_graphql_and_rest_notifications_and_strips_mentions(self):
        sniffer = XReplyInboxSniffer()
        # 1. Mentions GraphQL with display_text_range + quoted + retweet skipped
        r1 = _gql_tweet(
            "2001",
            "alice",
            uid="u_alice",
            full_text="@peterzhu1003 Great insight! https://t.co/img1",
            display_text_range=[14, 28],
            media=[{"url": "https://t.co/img1", "media_url_https": "https://pbs.twimg.com/media/x.jpg"}],
            in_reply_to_status_id="1001",
            in_reply_to_user_id="u_me",
            in_reply_to_screen_name="peterzhu1003",
            conversation_id="1001",
        )
        rt = _gql_tweet("2002", "rt_user", retweeted=_gql_tweet("999", "other"))
        sniffer.ingest_payload("NotificationsMentions", {"items": [r1, rt]})
        self.assertIn("2001", sniffer.mention_tweets_by_id)
        self.assertNotIn("2002", sniffer.mention_tweets_by_id)
        self.assertEqual(sniffer.mention_tweets_by_id["2001"]["reply_body_text"], "Great insight!")
        self.assertTrue(sniffer.mention_tweets_by_id["2001"]["has_media"])

        # 2. UserTweets skips own retweets & replies, keeps own original root posts
        my_post = _gql_tweet("1001", "peterzhu1003", uid="u_me", full_text="My root post", reply_count=2)
        my_reply = _gql_tweet("1002", "peterzhu1003", uid="u_me", full_text="@bob thx", in_reply_to_status_id="888", conversation_id="888")
        sniffer.ingest_payload("UserTweets", {"items": [my_post, my_reply, rt]})
        self.assertEqual(list(sniffer.profile_own_posts_by_id.keys()), ["1001"])

        # 3. REST v2 notifications payload without display_text_range falls back cleanly
        rest_payload = {
            "globalObjects": {
                "tweets": {
                    "2003": {
                        "id_str": "2003",
                        "user_id_str": "u_bob",
                        "full_text": "@peterzhu1003 How did you measure latency?",
                        "created_at": "Sat Nov 21 11:00:00 +0000 2026",
                        "lang": "en",
                        "in_reply_to_status_id_str": "1001",
                        "in_reply_to_user_id_str": "u_me",
                        "in_reply_to_screen_name": "peterzhu1003",
                        "conversation_id_str": "1001",
                        "entities": {},
                    }
                },
                "users": {"u_bob": {"id_str": "u_bob", "screen_name": "bob", "name": "Bob"}},
            }
        }
        sniffer.ingest_payload("RestNotifications", rest_payload)
        self.assertEqual(sniffer.mention_tweets_by_id["2003"]["reply_body_text"], "How did you measure latency?")

        # 4. TweetDetail detects existing self-reply
        my_existing_reply = _gql_tweet(
            "3001",
            "peterzhu1003",
            uid="u_me",
            full_text="@alice Thank you!",
            display_text_range=[7, 17],
            in_reply_to_status_id="2001",
            conversation_id="1001",
        )
        sniffer.ingest_payload(
            "TweetDetail",
            {"nodes": [my_post, r1, my_existing_reply]},
            resp_url="https://x.com/i/api/graphql/x/TweetDetail?variables=%7B%22focalTweetId%22%3A%221001%22%7D",
        )
        found = sniffer.find_my_existing_reply("2001", self_handle_lower="peterzhu1003", self_uid="u_me")
        self.assertIsNotNone(found)
        self.assertEqual(found["tweet_id"], "3001")

    def test_upsert_collected_replies_dedup_upgrades_and_limits(self):
        db = _FakeDB()
        now = 1_800_000_000
        sniffer = XReplyInboxSniffer()
        my_post = _gql_tweet("1001", "peterzhu1003", uid="u_me", full_text="My post content", reply_count=5)
        other_root = _gql_tweet("9001", "other_kol", uid="u_kol", full_text="Someone else post")
        my_comment_on_other = _gql_tweet(
            "9002", "peterzhu1003", uid="u_me", full_text="Nice thread",
            in_reply_to_status_id="9001", conversation_id="9001",
        )
        # Replies to test all cases:
        rep_pending = _gql_tweet(
            "2001", "alice", uid="u_a", full_text="@peterzhu1003 Interesting perspective!",
            display_text_range=[14, 38], in_reply_to_status_id="1001", in_reply_to_user_id="u_me",
            in_reply_to_screen_name="peterzhu1003", conversation_id="1001",
            created_at=time.strftime("%a %b %d %H:%M:%S +0000 %Y", time.gmtime(now - 600)),
        )
        rep_second_same_user = _gql_tweet(
            "2002", "alice", uid="u_a", full_text="@peterzhu1003 Second comment same user",
            display_text_range=[14, 38], in_reply_to_status_id="1001", in_reply_to_user_id="u_me",
            in_reply_to_screen_name="peterzhu1003", conversation_id="1001",
            created_at=time.strftime("%a %b %d %H:%M:%S +0000 %Y", time.gmtime(now - 300)),
        )
        rep_already_replied = _gql_tweet(
            "2003", "carol", uid="u_c", full_text="@peterzhu1003 Already answered",
            display_text_range=[14, 30], in_reply_to_status_id="1001", conversation_id="1001",
            created_at=time.strftime("%a %b %d %H:%M:%S +0000 %Y", time.gmtime(now - 500)),
        )
        my_ans_2003 = _gql_tweet(
            "3003", "peterzhu1003", uid="u_me", full_text="@carol Yes indeed!",
            display_text_range=[7, 18], in_reply_to_status_id="2003", conversation_id="1001",
            created_at=time.strftime("%a %b %d %H:%M:%S +0000 %Y", time.gmtime(now - 200)),
        )
        rep_media_only = _gql_tweet(
            "2004", "dave", uid="u_d", full_text="@peterzhu1003 https://t.co/img",
            display_text_range=[14, 14], media=[{"url": "https://t.co/img"}],
            in_reply_to_status_id="1001", conversation_id="1001",
            created_at=time.strftime("%a %b %d %H:%M:%S +0000 %Y", time.gmtime(now - 400)),
        )
        rep_too_old = _gql_tweet(
            "2005", "eve", uid="u_e", full_text="@peterzhu1003 Old reply",
            display_text_range=[14, 23], in_reply_to_status_id="1001", conversation_id="1001",
            created_at=time.strftime("%a %b %d %H:%M:%S +0000 %Y", time.gmtime(now - 100 * 3600)),
        )
        rep_on_other_root = _gql_tweet(
            "9003", "frank", uid="u_f", full_text="@peterzhu1003 Hey under KOL post",
            display_text_range=[14, 32], in_reply_to_status_id="9002", conversation_id="9001",
            created_at=time.strftime("%a %b %d %H:%M:%S +0000 %Y", time.gmtime(now - 300)),
        )
        rep_peer_to_peer = _gql_tweet(
            "2006", "grace", uid="u_g", full_text="@alice Agreeing with alice",
            display_text_range=[7, 26], in_reply_to_status_id="2001", in_reply_to_user_id="u_a",
            in_reply_to_screen_name="alice", conversation_id="1001",
            created_at=time.strftime("%a %b %d %H:%M:%S +0000 %Y", time.gmtime(now - 200)),
        )

        sniffer.ingest_payload(
            "TweetDetail",
            {"nodes": [my_post, other_root, my_comment_on_other, rep_pending, rep_second_same_user, rep_already_replied, my_ans_2003, rep_media_only, rep_too_old, rep_on_other_root, rep_peer_to_peer]},
        )

        res = upsert_collected_replies(
            db,
            account_name="x_myself",
            self_handle="peterzhu1003",
            self_uid="u_me",
            tweets_by_id=sniffer.tweets_by_id,
            candidate_replies=sniffer.tweets_by_id.values(),
            only_my_root_posts=True,
            max_reply_age_hours=72,
            max_replies_per_user_per_post=1,
            now_ts=now,
        )
        self.assertEqual(res["new_pending"], 1)
        self.assertEqual(res["new_already_replied"], 1)
        self.assertEqual(res["new_skipped"], 2)  # media_only_no_text + max_replies_per_user_per_post_reached
        self.assertEqual(res["skipped_too_old"], 1)
        self.assertEqual(res["skipped_not_own_root"], 1)

        inbox = db.get_collection(REPLY_INBOX_COLLECTION)
        d1 = inbox.find_one({"account_name": "x_myself", "reply_id": "2001"})
        self.assertEqual(d1["status"], "pending_generate")
        self.assertEqual(d1["post_text"], "My post content")
        self.assertEqual(d1["reply_text"], "Interesting perspective!")
        self.assertEqual(inbox.find_one({"reply_id": "2002"})["skip_reason"], "max_replies_per_user_per_post_reached")
        self.assertEqual(inbox.find_one({"reply_id": "2003"})["replied_source"], "detected_on_x")
        self.assertEqual(inbox.find_one({"reply_id": "2004"})["skip_reason"], "media_only_no_text")
        self.assertIsNone(inbox.find_one({"reply_id": "2006"}))

        # Simulate manual reply detected on next run for an existing ready_to_reply item -> upgrades to replied!
        inbox.update_one({"reply_id": "2001"}, {"$set": {"status": "ready_to_reply", "generated_reply": "Thanks!"}})
        my_manual_reply = _gql_tweet(
            "3001", "peterzhu1003", uid="u_me", full_text="@alice Thanks from phone!",
            display_text_range=[7, 24], in_reply_to_status_id="2001", conversation_id="1001",
        )
        sniffer.ingest_payload("TweetDetail", {"nodes": [my_manual_reply]})
        res2 = upsert_collected_replies(
            db,
            account_name="x_myself",
            self_handle="peterzhu1003",
            self_uid="u_me",
            tweets_by_id=sniffer.tweets_by_id,
            candidate_replies=sniffer.tweets_by_id.values(),
            now_ts=now + 60,
        )
        self.assertEqual(res2["upgraded_replied_on_x"], 1)
        self.assertEqual(inbox.find_one({"reply_id": "2001"})["status"], "replied")
        self.assertEqual(inbox.find_one({"reply_id": "2001"})["replied_tweet_id"], "3001")

    def test_collect_replies_empty_capture_challenge_and_lock_timeout(self):
        db = _FakeDB()

        @contextlib.contextmanager
        def _fake_session(*_a, **_kw):
            yield (MagicMock(), _make_fake_page(url="https://x.com/notifications/mentions", visible_selectors=("AppTabBar_Profile_Link",)))

        with tempfile.TemporaryDirectory() as tmp:
            acc = {"name": "x_myself", "screen_name": "peterzhu1003", "user_data_dir": tmp}
            with patch("x_growth.auto_replier.persistent_browser_session", side_effect=_fake_session), \
                 patch("x_growth.auto_replier._dismiss_overlays"), \
                 patch("x_growth.auto_replier.check_for_crash"), \
                 patch.object(XReplyInboxSniffer, "wait_for_ops", return_value=False):
                res_empty = collect_replies_for_account(acc, db=db)
            self.assertEqual(res_empty["status"], "empty_capture")

            @contextlib.contextmanager
            def _challenge_session(*_a, **_kw):
                yield (MagicMock(), _make_fake_page(url="https://x.com/account/access"))

            with patch("x_growth.auto_replier.persistent_browser_session", side_effect=_challenge_session), \
                 patch("x_growth.auto_replier._dismiss_overlays"), \
                 patch("x_growth.auto_replier.check_for_crash"), \
                 patch.object(XReplyInboxSniffer, "wait_for_ops", return_value=False):
                res_chal = collect_replies_for_account(acc, db=db)
            self.assertEqual(res_chal["status"], "locked_challenge")

            with patch("x_growth.auto_replier.persistent_browser_session", side_effect=UserDataDirLockTimeoutError("busy")):
                res_lock = collect_replies_for_account(acc, db=db)
            self.assertEqual(res_lock["status"], "lock_timeout")

    def test_generate_replies_uses_low_group_validates_and_transitions_states(self):
        db = _FakeDB()
        now = 1_800_000_000
        col = db.get_collection(REPLY_INBOX_COLLECTION)
        base_doc = {
            "account_name": "x_myself",
            "account_handle": "peterzhu1003",
            "post_id": "1001",
            "post_text": "We released our lightweight daemon today.",
            "post_has_media": False,
            "parent_tweet_id": "1001",
            "parent_tweet_text": "We released our lightweight daemon today.",
            "reply_has_media": False,
            "status": "pending_generate",
            "generate_attempts": 0,
            "generate_next_retry_ts": 0,
        }
        col.docs.extend([
            {**base_doc, "reply_id": "r_ok", "reply_author": "alice", "reply_text": "How does it avoid duplicates?", "reply_publish_ts": now - 300},
            {**base_doc, "reply_id": "r_skip", "reply_author": "spammer", "reply_text": "DM me for guaranteed 100x crypto pump", "reply_publish_ts": now - 200},
            {**base_doc, "reply_id": "r_fail", "reply_author": "bob", "reply_text": "Nice update", "reply_publish_ts": now - 100, "generate_attempts": 2},
        ])

        captured_calls = []

        def _fake_call_llm_json(**kwargs):
            captured_calls.append(kwargs)
            validator = kwargs["validator"]
            rid = kwargs["task_label"].split(":")[-1]
            if rid == "r_ok":
                # Verify validator rejects bad outputs first
                self.assertIsNotNone(validator({"skip": False, "language": "en", "reply_text": "See https://evil.com"}))
                self.assertIsNotNone(validator({"skip": False, "language": "en", "reply_text": "Great #tag"}))
                self.assertIsNotNone(validator({"skip": False, "language": "en", "reply_text": "x" * 245}))
                self.assertIsNotNone(validator({"skip": False, "language": "en", "reply_text": "@alice"}))
                payload = {"skip": False, "skip_reason": "", "language": "en", "reply_text": "@alice It stores unique reply IDs locally before sending."}
                self.assertIsNone(validator(payload))
                return payload, {"model_used": "mock-low"}
            if rid == "r_skip":
                payload = {"skip": True, "skip_reason": "spam_solicitation", "language": "en", "reply_text": ""}
                self.assertIsNone(validator(payload))
                return payload, {"model_used": "mock-low"}
            return None, {"parse_error": "gateway_timeout"}

        with patch("x_growth.auto_replier.call_llm_json", side_effect=_fake_call_llm_json):
            res = generate_replies_from_db(
                {"name": "x_myself", "screen_name": "peterzhu1003"},
                db=db,
                config={"max_generate_retries": 3},
                now_ts=now,
            )

        self.assertEqual(res["ready_count"], 1)
        self.assertEqual(res["skipped_count"], 1)
        self.assertEqual(res["failed_count"], 1)
        for kw in captured_calls:
            self.assertIsNone(kw["model"])
            self.assertIsNone(kw["file_paths"])
            self.assertEqual(kw["preset_model_group"], "low")

        d_ok = col.find_one({"reply_id": "r_ok"})
        self.assertEqual(d_ok["status"], "ready_to_reply")
        self.assertEqual(d_ok["generated_reply"], "It stores unique reply IDs locally before sending.")
        self.assertEqual(d_ok["generated_lang"], "en")

        d_skip = col.find_one({"reply_id": "r_skip"})
        self.assertEqual(d_skip["status"], "skipped")
        self.assertEqual(d_skip["skip_reason"], "ai_safety:spam_solicitation")

        d_fail = col.find_one({"reply_id": "r_fail"})
        self.assertEqual(d_fail["status"], "failed")
        self.assertEqual(d_fail["generate_attempts"], 3)

    def test_publish_replies_dry_run_daily_cap_live_dedup_and_hold_limit(self):
        db = _FakeDB()
        now = int(time.time())
        col = db.get_collection(REPLY_INBOX_COLLECTION)

        with tempfile.TemporaryDirectory() as tmp:
            acc = {"name": "x_myself", "screen_name": "peterzhu1003", "user_data_dir": tmp}

            # 1. Empty queue opens zero browsers
            res_empty = publish_replies_for_account(acc, db=db, now_ts=now)
            self.assertFalse(res_empty["browser_opened"])
            self.assertEqual(res_empty["candidates_found"], 0)

            col.docs.extend([
                {"account_name": "x_myself", "reply_id": "r_send", "reply_url": "https://x.com/alice/status/r_send", "reply_author": "alice", "reply_author_lower": "alice", "post_id": "1001", "status": "ready_to_reply", "generated_reply": "Thanks Alice!", "reply_publish_ts": now - 400, "reply_next_retry_ts": 0, "reply_attempts": 0},
                {"account_name": "x_myself", "reply_id": "r_dup", "reply_url": "https://x.com/bob/status/r_dup", "reply_author": "bob", "reply_author_lower": "bob", "post_id": "1001", "status": "ready_to_reply", "generated_reply": "Thanks Bob!", "reply_publish_ts": now - 300, "reply_next_retry_ts": 0, "reply_attempts": 0},
                {"account_name": "x_myself", "reply_id": "r_gone", "reply_url": "https://x.com/carol/status/r_gone", "reply_author": "carol", "reply_author_lower": "carol", "post_id": "1001", "status": "ready_to_reply", "generated_reply": "Thanks Carol!", "reply_publish_ts": now - 200, "reply_next_retry_ts": 0, "reply_attempts": 0},
                {"account_name": "x_myself", "reply_id": "r_biz", "reply_url": "https://x.com/dave/status/r_biz", "reply_author": "dave", "reply_author_lower": "dave", "post_id": "1001", "status": "ready_to_reply", "generated_reply": "Thanks Dave!", "reply_publish_ts": now - 100, "reply_next_retry_ts": 0, "reply_attempts": 0},
            ])

            # 2. dry_run=True never opens browser
            res_dry = publish_replies_for_account(acc, db=db, dry_run=True, now_ts=now)
            self.assertFalse(res_dry["browser_opened"])
            self.assertEqual(res_dry["candidates_found"], 4)

            # 3. Live run handles: send success -> live_dup skip -> gone unavailable -> BusinessErrorException cooldown
            visited_urls = []

            @contextlib.contextmanager
            def _fake_session(*_a, **_kw):
                page = _make_fake_page(url="https://x.com/home", visible_selectors=("tweetTextarea_0",))

                def _goto(u, **_kw2):
                    visited_urls.append(u)
                    page.url = u
                    resp = MagicMock()
                    resp.status = 404 if "r_gone" in u else 200
                    return resp

                page.goto.side_effect = _goto
                yield (MagicMock(), page)

            def _fake_find_dup(rid, **_kw):
                if rid == "r_dup":
                    return {"tweet_id": "9999", "url": "https://x.com/peterzhu1003/status/9999", "text": "Already replied", "publish_ts": now - 50}
                return None

            def _fake_submit(_page, _box, content="", **_kw):
                if content == "Thanks Dave!":
                    raise BusinessErrorException("spam throttle")
                return "7777"

            with patch("x_growth.auto_replier.persistent_browser_session", side_effect=_fake_session), \
                 patch("x_growth.auto_replier._wait_x_home_ready"), \
                 patch("x_growth.auto_replier._wait_x_post_detail_ready"), \
                 patch("x_growth.auto_replier._dismiss_overlays"), \
                 patch("x_growth.auto_replier.check_for_crash"), \
                 patch("x_growth.auto_replier._resolve_session_identity", return_value=("u_me", "peterzhu1003")), \
                 patch("x_growth.auto_replier._locate_x_reply_box", return_value=MagicMock()), \
                 patch("x_growth.auto_replier._like_focal_comment_on_page"), \
                 patch("x_growth.auto_replier._sleep_jitter"), \
                 patch("x_growth.auto_replier._submit_x_post", side_effect=_fake_submit), \
                 patch.object(XReplyInboxSniffer, "wait_for_ops", return_value=False), \
                 patch.object(XReplyInboxSniffer, "find_my_existing_reply", side_effect=_fake_find_dup):
                res_live = publish_replies_for_account(acc, db=db, config={"max_replies_per_run": 5}, now_ts=now)

            self.assertEqual(res_live["status"], "cooldown_required")
            self.assertEqual(res_live["replied_count"], 1)
            self.assertEqual(res_live["skipped_count"], 2)
            self.assertIn("https://x.com/alice/status/r_send", visited_urls)
            self.assertEqual(col.find_one({"reply_id": "r_send"})["status"], "replied")
            self.assertEqual(col.find_one({"reply_id": "r_send"})["replied_tweet_id"], "7777")
            self.assertEqual(col.find_one({"reply_id": "r_dup"})["replied_source"], "detected_on_x")
            self.assertEqual(col.find_one({"reply_id": "r_gone"})["status"], "unavailable")
            self.assertEqual(col.find_one({"reply_id": "r_biz"})["reply_attempts"], 1)

            # 4. Daily cap reached blocks opening browser
            res_cap = publish_replies_for_account(acc, db=db, config={"max_replies_per_day": 1}, now_ts=now + 10)
            self.assertEqual(res_cap["status"], "daily_limit_reached")
            self.assertFalse(res_cap["browser_opened"])

            # 5. Session max hold stop-loss exits cleanly without exceeding hold budget
            db_hold = _FakeDB()
            h_col = db_hold.get_collection(REPLY_INBOX_COLLECTION)
            for idx in range(3):
                h_col.docs.append(
                    {
                        "account_name": "x_myself",
                        "reply_id": f"rh_{idx}",
                        "reply_url": f"https://x.com/u{idx}/status/rh_{idx}",
                        "reply_author": f"u{idx}",
                        "reply_author_lower": f"u{idx}",
                        "post_id": "1001",
                        "status": "ready_to_reply",
                        "generated_reply": f"Reply {idx}",
                        "reply_publish_ts": now + idx,
                        "reply_next_retry_ts": 0,
                        "reply_attempts": 0,
                    }
                )
            clock = {"t": 1000.0}

            def _slow_submit(_p, _b, content="", **_kw):
                clock["t"] += 80.0
                return f"sent_{content}"

            with patch("x_growth.auto_replier.persistent_browser_session", side_effect=_fake_session), \
                 patch("x_growth.auto_replier._wait_x_home_ready"), \
                 patch("x_growth.auto_replier._wait_x_post_detail_ready"), \
                 patch("x_growth.auto_replier._dismiss_overlays"), \
                 patch("x_growth.auto_replier.check_for_crash"), \
                 patch("x_growth.auto_replier._resolve_session_identity", return_value=("u_me", "peterzhu1003")), \
                 patch("x_growth.auto_replier._locate_x_reply_box", return_value=MagicMock()), \
                 patch("x_growth.auto_replier._like_focal_comment_on_page"), \
                 patch("x_growth.auto_replier._sleep_jitter"), \
                 patch("x_growth.auto_replier._submit_x_post", side_effect=_slow_submit), \
                 patch.object(XReplyInboxSniffer, "wait_for_ops", return_value=False), \
                 patch.object(XReplyInboxSniffer, "find_my_existing_reply", return_value=None), \
                 patch("x_growth.auto_replier.time.monotonic", side_effect=lambda: clock["t"]):
                res_hold = publish_replies_for_account(
                    acc, db=db_hold, config={"max_replies_per_run": 5, "session_max_hold_seconds": 150}, now_ts=now
                )
            self.assertEqual(res_hold["status"], "ok")
            self.assertEqual(res_hold["replied_count"], 2)
            self.assertGreaterEqual(res_hold["hold_seconds"], 150.0)

    def test_collect_replies_full_flow_zero_auth_filter_and_checkpoints(self):
        db = _FakeDB()
        now = int(time.time())

        @contextlib.contextmanager
        def _flow_session(*_a, **_kw):
            page = _make_fake_page(url="https://x.com/notifications/mentions", visible_selectors=("AppTabBar_Profile_Link",))
            listeners = {}

            def _on(ev, fn):
                listeners[ev] = fn

            def _emit(op, payload, url):
                resp = MagicMock()
                resp.status = 200
                resp.url = url
                resp.json.return_value = payload
                if "response" in listeners:
                    listeners["response"](resp)

            def _goto(u, **_kw2):
                page.url = u
                if "mentions" in u:
                    m_own = _gql_tweet(
                        "2101", "alice", uid="u_a", full_text="@peterzhu1003 Great post!",
                        display_text_range=[14, 25], in_reply_to_status_id="1101", in_reply_to_user_id="u_me",
                        in_reply_to_screen_name="peterzhu1003", conversation_id="1101",
                        created_at=time.strftime("%a %b %d %H:%M:%S +0000 %Y", time.gmtime(now - 300)),
                    )
                    m_other = _gql_tweet(
                        "2201", "bob", uid="u_b", full_text="@peterzhu1003 Reply on KOL post",
                        display_text_range=[14, 31], in_reply_to_status_id="9902", in_reply_to_user_id="u_me",
                        in_reply_to_screen_name="peterzhu1003", conversation_id="9900",
                        created_at=time.strftime("%a %b %d %H:%M:%S +0000 %Y", time.gmtime(now - 200)),
                    )
                    _emit("NotificationsMentions", {"items": [m_own, m_other]}, "https://x.com/i/api/graphql/x/NotificationsMentions")
                elif u.endswith("/peterzhu1003"):
                    p_own = _gql_tweet("1101", "peterzhu1003", uid="u_me", full_text="Own post text", reply_count=1)
                    _emit("UserTweets", {"items": [p_own]}, "https://x.com/i/api/graphql/x/UserTweets")
                elif "/status/1101" in u:
                    p_own = _gql_tweet("1101", "peterzhu1003", uid="u_me", full_text="Own post text", reply_count=1)
                    r_own = _gql_tweet(
                        "2101", "alice", uid="u_a", full_text="@peterzhu1003 Great post!",
                        display_text_range=[14, 25], in_reply_to_status_id="1101", in_reply_to_user_id="u_me",
                        in_reply_to_screen_name="peterzhu1003", conversation_id="1101",
                        created_at=time.strftime("%a %b %d %H:%M:%S +0000 %Y", time.gmtime(now - 300)),
                    )
                    _emit("TweetDetail", {"items": [p_own, r_own]}, "https://x.com/i/api/graphql/x/TweetDetail?variables=%7B%22focalTweetId%22%3A%221101%22%7D")
                resp = MagicMock()
                resp.status = 200
                return resp

            page.on.side_effect = _on
            page.goto.side_effect = _goto
            yield (MagicMock(), page)

        def _fake_zero_auth(pid, **_kw):
            if pid == "9900":
                return {"author": {"username": "other_kol"}, "author_id": "u_kol", "content": {"text_content": "Other root"}}
            return None

        with tempfile.TemporaryDirectory() as tmp:
            acc = {"name": "x_myself", "screen_name": "peterzhu1003", "user_data_dir": tmp}
            with patch("x_growth.auto_replier.persistent_browser_session", side_effect=_flow_session), \
                 patch("x_growth.auto_replier._dismiss_overlays"), \
                 patch("x_growth.auto_replier.check_for_crash"), \
                 patch("x_growth.auto_replier._wait_x_post_detail_ready"), \
                 patch("x_growth.auto_replier._resolve_session_identity", return_value=("u_me", "peterzhu1003")), \
                 patch("x_growth.auto_replier.fetch_x_tweet_zero_auth", side_effect=_fake_zero_auth), \
                 patch.object(XReplyInboxSniffer, "wait_for_ops", return_value=True):
                res = collect_replies_for_account(acc, db=db)

        self.assertEqual(res["status"], "ok")
        self.assertEqual(res["details_opened"], 1)
        self.assertEqual(res["new_pending"], 1)
        inbox = db.get_collection(REPLY_INBOX_COLLECTION)
        cp = db.get_collection(POST_CHECKPOINTS_COLLECTION)
        self.assertEqual(inbox.find_one({"reply_id": "2101"})["status"], "pending_generate")
        self.assertIsNone(inbox.find_one({"reply_id": "2201"}))
        self.assertTrue(cp.find_one({"post_id": "1101"})["is_own_post"])
        self.assertFalse(cp.find_one({"post_id": "9900"})["is_own_post"])

    def test_parallel_daemon_runs_three_workers_and_status_dashboard(self):
        db = _FakeDB()
        with tempfile.TemporaryDirectory() as tmp:
            acc = {"name": "x_myself", "screen_name": "peterzhu1003", "user_data_dir": tmp, "enabled": True}
            seen = []
            seen_lock = threading.Lock()

            def _rec(name):
                def _fn(*_a, **_kw):
                    with seen_lock:
                        seen.append((name, threading.current_thread().name))
                    return {"account_name": "x_myself", "status": "ok", "pending_found": 0, "replied_count": 0}
                return _fn

            with patch("x_growth.auto_replier.gen_db_object", return_value=db), \
                 patch("x_growth.auto_replier.collect_replies_for_account", side_effect=_rec("collect")), \
                 patch("x_growth.auto_replier.generate_replies_from_db", side_effect=_rec("generate")), \
                 patch("x_growth.auto_replier.publish_replies_for_account", side_effect=_rec("publish")):
                run_parallel_daemon(
                    accounts=[acc],
                    max_rounds=1,
                    config={
                        "collect_initial_delay_seconds": 0,
                        "generate_initial_delay_seconds": 0,
                        "reply_initial_delay_seconds": 0,
                    },
                )

            roles = {role for role, _tname in seen}
            tnames = {tname for _role, tname in seen}
            self.assertEqual(roles, {"collect", "generate", "publish"})
            self.assertEqual(tnames, {"ReplyCollector", "ReplyGenerator", "ReplyPublisher"})

            rep = build_status_report(db, [acc])
            txt = format_status_text(rep)
            self.assertIn("x_myself @peterzhu1003", txt)
            self.assertIn("待生成=0", txt)


if __name__ == "__main__":
    unittest.main()
