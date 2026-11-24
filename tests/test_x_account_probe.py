# -*- coding: utf-8 -*-
"""Unit tests for lightweight X account status probe (`platforms/x_account_probe.py`)."""
import logging
from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, patch

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from common.post_utils import AccountIsolationError
from platforms import x_account_probe as probe
from platforms.x_browser import UserDataDirLockTimeoutError


class _FakeLocatorNode:
    def __init__(self, *, visible: bool = False, text: str = "", attrs=None, children=None):
        self._visible = visible
        self._text = text
        self._attrs = dict(attrs or {})
        self._children = list(children or [])

    @property
    def first(self):
        return self

    def count(self) -> int:
        if self._children:
            return len(self._children)
        return 1 if (self._visible or self._text or self._attrs) else 0

    def is_visible(self) -> bool:
        return self._visible

    def inner_text(self) -> str:
        return self._text

    def get_attribute(self, name: str):
        return self._attrs.get(name)

    def nth(self, idx: int):
        return self._children[idx]

    def locator(self, selector: str):
        if selector == 'a[href*="/status/"]' and self._attrs.get("_status_links"):
            links = [
                _FakeLocatorNode(visible=True, attrs={"href": href})
                for href in self._attrs["_status_links"]
            ]
            return _FakeLocatorNode(visible=True, children=links)
        return _FakeLocatorNode()


class _FakeResponse:
    def __init__(self, url: str, status: int, payload=None):
        self.url = url
        self.status = status
        self._payload = payload

    def json(self):
        return self._payload


class _FakeSearchPage:
    """Deterministic fake Playwright page for search probe scenarios."""

    def __init__(self, scenarios: dict):
        self.scenarios = scenarios
        self.url = "about:blank"
        self._listeners = {}
        self._active_dom = {}
        self.visited_queries = []
        self._closed = False

    def on(self, event: str, fn):
        self._listeners.setdefault(event, []).append(fn)

    def remove_listener(self, event: str, fn):
        lst = self._listeners.get(event, [])
        if fn in lst:
            lst.remove(fn)

    def goto(self, url: str, **_kwargs):
        self.url = url
        low_url = url.lower()
        matched_key = None
        if "/search" in low_url:
            for key in self.scenarios:
                if key.startswith("profile:"):
                    continue
                k = key.lower()
                if f"from%3a{k}" in low_url or f"from:{k}" in low_url:
                    matched_key = key
                    break
        else:
            ordered_keys = [key for key in self.scenarios if key.startswith("profile:")] + [
                key for key in self.scenarios if not key.startswith("profile:")
            ]
            for key in ordered_keys:
                k = key[len("profile:"):].lower() if key.startswith("profile:") else key.lower()
                if low_url.rstrip("/").endswith(f"/{k}"):
                    matched_key = key
                    break
        sc = self.scenarios.get(matched_key, {})
        self.visited_queries.append(matched_key)
        self.url = sc.get("final_url", url)
        self._active_dom = sc.get("dom", {})

        for resp in sc.get("responses", []):
            for fn in list(self._listeners.get("response", [])):
                fn(resp)

    def wait_for_timeout(self, _ms: int):
        return None

    def is_closed(self) -> bool:
        return self._closed

    def locator(self, selector: str):
        dom = self._active_dom
        if selector in dom:
            return dom[selector]
        if selector == 'article[data-testid="tweet"]' and dom.get("tweet_links"):
            articles = [
                _FakeLocatorNode(
                    visible=True,
                    text=tw_text,
                    attrs={"_status_links": [href]},
                )
                for href, tw_text in dom["tweet_links"]
            ]
            return _FakeLocatorNode(visible=True, children=articles)
        return _FakeLocatorNode()


class _FakeSession:
    def __init__(self, page=None, *, lock_error: Exception = None):
        self.page = page
        self.lock_error = lock_error
        self.closed = False

    def ensure_alive(self):
        if self.lock_error is not None:
            raise self.lock_error
        return self.page

    def close(self):
        self.closed = True


class TestXAccountProbe(unittest.TestCase):
    def test_assert_safe_probe_session_dir_blocks_monitored_accounts(self):
        self.assertEqual(
            probe.assert_safe_probe_session_dir(r"W:\temp\x_xiaohao"),
            r"W:\temp\x_xiaohao",
        )
        for bad in (
            "",
            "   ",
            r"W:\temp\x_myself",
            "w:/temp/x_myself/",
            r"W:\TEMP\X_MYSELF",
            r"W:\temp\x_dahao",
            "w:/temp/X_DAHAO/",
        ):
            with self.assertRaises(AccountIsolationError, msg=f"Should reject {bad!r}"):
                probe.assert_safe_probe_session_dir(bad)

    def test_extract_search_timeline_tweets_handles_both_schemas_and_dedups(self):
        payload = {
            "data": {
                "search_by_raw_query": {
                    "search_timeline": {
                        "timeline": {
                            "instructions": [
                                {
                                    "entries": [
                                        # 1) Legacy user schema
                                        {
                                            "content": {
                                                "itemContent": {
                                                    "tweet_results": {
                                                        "result": {
                                                            "rest_id": "111",
                                                            "core": {
                                                                "user_results": {
                                                                    "result": {
                                                                        "legacy": {"screen_name": "2434954452qqc"}
                                                                    }
                                                                }
                                                            },
                                                            "legacy": {
                                                                "created_at": "Tue Nov 24 00:00:00 +0000 2026",
                                                                "full_text": "legacy schema match",
                                                            },
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        # 2) New core user schema wrapped in TweetWithVisibilityResults
                                        {
                                            "content": {
                                                "itemContent": {
                                                    "tweet_results": {
                                                        "result": {
                                                            "__typename": "TweetWithVisibilityResults",
                                                            "tweet": {
                                                                "rest_id": "222",
                                                                "core": {
                                                                    "user_results": {
                                                                        "result": {
                                                                            "core": {"screen_name": "2434954452QQC"}
                                                                        }
                                                                    }
                                                                },
                                                                "legacy": {
                                                                    "created_at": "Tue Nov 24 01:00:00 +0000 2026",
                                                                    "full_text": "core schema wrapped match",
                                                                },
                                                            },
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        # 3) Duplicate rest_id should be ignored
                                        {
                                            "content": {
                                                "itemContent": {
                                                    "tweet_results": {
                                                        "result": {
                                                            "rest_id": "111",
                                                            "core": {
                                                                "user_results": {
                                                                    "result": {
                                                                        "legacy": {"screen_name": "2434954452qqc"}
                                                                    }
                                                                }
                                                            },
                                                            "legacy": {"full_text": "dup"},
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        # 4) Impersonator handle must NOT match target
                                        {
                                            "content": {
                                                "itemContent": {
                                                    "tweet_results": {
                                                        "result": {
                                                            "rest_id": "999",
                                                            "core": {
                                                                "user_results": {
                                                                    "result": {
                                                                        "legacy": {"screen_name": "2434594452qqc"}
                                                                    }
                                                                }
                                                            },
                                                            "legacy": {"full_text": "impersonator"},
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                    ]
                                }
                            ]
                        }
                    }
                }
            }
        }
        tweets = probe.extract_search_timeline_tweets(payload, "@2434954452qqc")
        self.assertEqual([t["tweet_id"] for t in tweets], ["111", "222"])

    def test_fetch_public_profile_status_alive_unavailable_and_error(self):
        alive_resp = MagicMock(status_code=200)
        alive_resp.json.return_value = {
            "code": 200,
            "user": {"followers": 682, "following": 0, "tweets": 315},
        }
        with patch("platforms.x_account_probe.requests.get", return_value=alive_resp):
            r1 = probe.fetch_public_profile_status("@2434954452qqc", proxy_url="")
        self.assertEqual(r1["profile_state"], "ALIVE")
        self.assertEqual(r1["followers"], 682)
        self.assertEqual(r1["tweets"], 315)

        dead_resp = MagicMock(status_code=404)
        dead_resp.json.return_value = {"code": 404, "message": "User not found"}
        with patch("platforms.x_account_probe.requests.get", return_value=dead_resp):
            r2 = probe.fetch_public_profile_status("missing_user", proxy_url="")
        self.assertEqual(r2["profile_state"], "SUSPECT_UNAVAILABLE")

        with patch("platforms.x_account_probe.requests.get", side_effect=RuntimeError("conn reset")):
            r3 = probe.fetch_public_profile_status("any_user", proxy_url="")
        self.assertEqual(r3["profile_state"], "ERROR")

    def _run_probe_with_scenarios(self, scenarios, *, profile_states=None, targets=None, lock_error=None):
        fake_page = _FakeSearchPage(scenarios)
        created_sessions = []

        def _factory(**_kwargs):
            s = _FakeSession(fake_page, lock_error=lock_error)
            created_sessions.append(s)
            return s

        default_profiles = profile_states or {}

        def _fake_profile(handle, **_kwargs):
            return default_profiles.get(
                handle,
                {
                    "handle": handle,
                    "profile_state": "ALIVE",
                    "followers": 100,
                    "following": 10,
                    "tweets": 50,
                    "detail": "HTTP 200",
                },
            )

        with (
            patch("platforms.x_account_probe.fetch_public_profile_status", side_effect=_fake_profile),
            patch("platforms.x_account_probe.human_pause"),
        ):
            report = probe.probe_x_accounts(
                targets=targets or [("x_dahao", "2434954452qqc"), ("x_myself", "peterzhu1003")],
                observer_session_dir=r"W:\temp\x_xiaohao",
                session_factory=_factory,
                wait_seconds=1.5,
            )
        return report, fake_page, created_sessions[0]

    def test_case_a_both_hidden_and_nasa_control_passes(self):
        scenarios = {
            "2434954452qqc": {
                "responses": [_FakeResponse("https://x.com/i/api/graphql/abc/SearchTimeline", 200, {"data": {}})],
                "dom": {'[data-testid="emptyState"]': _FakeLocatorNode(visible=True, text="No results")},
            },
            "peterzhu1003": {
                "responses": [_FakeResponse("https://x.com/i/api/graphql/abc/SearchTimeline", 200, {"data": {}})],
                "dom": {'main[role="main"]': _FakeLocatorNode(visible=True, text='No results for "from:peterzhu1003"')},
            },
            "nasa": {
                "dom": {"tweet_links": [("/NASA/status/200001", "Artemis update")]},
            },
        }
        report, fake_page, sess = self._run_probe_with_scenarios(scenarios)
        self.assertTrue(sess.closed)
        self.assertTrue(report["observer_ok"])
        self.assertTrue(report["calibration"]["performed"])
        self.assertTrue(report["calibration"]["passed"])
        self.assertEqual([t["verdict"] for t in report["targets"]], ["SEARCH_BANNED", "SEARCH_BANNED"])
        self.assertEqual(fake_page.visited_queries, ["2434954452qqc", "peterzhu1003", "nasa"])

    def test_case_b_hidden_but_control_also_hidden_downgrades_to_uncertain(self):
        scenarios = {
            "2434954452qqc": {
                "dom": {'[data-testid="emptyState"]': _FakeLocatorNode(visible=True, text="No results")},
            },
            "peterzhu1003": {
                "dom": {'[data-testid="emptyState"]': _FakeLocatorNode(visible=True, text="No results")},
            },
            "nasa": {
                "dom": {'[data-testid="emptyState"]': _FakeLocatorNode(visible=True, text="No results")},
            },
        }
        report, _page, sess = self._run_probe_with_scenarios(scenarios)
        self.assertTrue(sess.closed)
        self.assertTrue(report["calibration"]["performed"])
        self.assertFalse(report["calibration"]["passed"])
        self.assertEqual([t["verdict"] for t in report["targets"]], ["UNCERTAIN", "UNCERTAIN"])

    def test_case_c_both_visible_skips_control_page(self):
        gql_visible = {
            "data": {
                "tweet_results": {
                    "result": {
                        "rest_id": "55555",
                        "core": {"user_results": {"result": {"core": {"screen_name": "2434954452qqc"}}}},
                        "legacy": {"full_text": "recovered"},
                    }
                }
            }
        }
        scenarios = {
            "2434954452qqc": {
                "responses": [_FakeResponse("https://x.com/i/api/graphql/abc/SearchTimeline", 200, gql_visible)],
            },
            "peterzhu1003": {
                "dom": {"tweet_links": [("/peterzhu1003/status/66666", "back online")]},
            },
        }
        report, fake_page, sess = self._run_probe_with_scenarios(scenarios)
        self.assertTrue(sess.closed)
        self.assertFalse(report["calibration"]["performed"])
        self.assertEqual([t["verdict"] for t in report["targets"]], ["VISIBLE_OK", "VISIBLE_OK"])
        self.assertEqual(fake_page.visited_queries, ["2434954452qqc", "peterzhu1003"])
        self.assertEqual(report["targets"][0]["search"]["tweets"][0]["tweet_id"], "55555")

    def test_case_d_observer_blocked_stops_immediately_and_closes(self):
        scenarios = {
            "2434954452qqc": {
                "final_url": "https://x.com/i/flow/login",
            },
            "peterzhu1003": {
                "dom": {"tweet_links": [("/peterzhu1003/status/66666", "should not run")]},
            },
        }
        report, fake_page, sess = self._run_probe_with_scenarios(scenarios)
        self.assertTrue(sess.closed)
        self.assertFalse(report["observer_ok"])
        self.assertEqual(fake_page.visited_queries, ["2434954452qqc"])
        self.assertEqual(report["targets"][0]["search"]["search_state"], "OBSERVER_BLOCKED")
        self.assertEqual(report["targets"][1]["search"]["search_state"], "SKIPPED_OBSERVER_BLOCKED")
        self.assertEqual([t["verdict"] for t in report["targets"]], ["OBSERVER_ERROR", "OBSERVER_ERROR"])

    def test_case_e_user_data_dir_lock_timeout_handled_cleanly(self):
        report, _page, sess = self._run_probe_with_scenarios(
            {},
            lock_error=UserDataDirLockTimeoutError("locked by another chrome"),
        )
        self.assertTrue(sess.closed)
        self.assertFalse(report["observer_ok"])
        self.assertIn("x_xiaohao", report["observer_error"])
        self.assertEqual([t["verdict"] for t in report["targets"]], ["OBSERVER_ERROR", "OBSERVER_ERROR"])

    def test_retry_error_and_rate_limit_never_misdiagnosed_as_search_ban(self):
        scenarios = {
            "2434954452qqc": {
                "responses": [_FakeResponse("https://x.com/i/api/graphql/abc/SearchTimeline", 429, {})],
            },
            "peterzhu1003": {
                "dom": {'main[role="main"]': _FakeLocatorNode(visible=True, text="Something went wrong. Try reloading.")},
            },
        }
        report, _page, sess = self._run_probe_with_scenarios(scenarios)
        self.assertTrue(sess.closed)
        self.assertFalse(report["calibration"]["performed"])
        self.assertEqual(report["targets"][0]["search"]["search_state"], "ERROR")
        self.assertEqual(report["targets"][1]["search"]["search_state"], "ERROR")
        self.assertEqual([t["verdict"] for t in report["targets"]], ["UNCERTAIN", "UNCERTAIN"])

    def test_suspect_unavailable_false_positive_overridden_by_live_x_profile(self):
        scenarios = {
            "peterzhu1003": {
                "dom": {'[data-testid="emptyState"]': _FakeLocatorNode(visible=True, text="No results")},
            },
            "nasa": {
                "dom": {"tweet_links": [("/NASA/status/200001", "Artemis update")]},
            },
            "profile:peterzhu1003": {
                "responses": [
                    _FakeResponse(
                        "https://x.com/i/api/graphql/xyz/UserByScreenName",
                        200,
                        {
                            "data": {
                                "user": {
                                    "result": {
                                        "__typename": "User",
                                        "legacy": {
                                            "followers_count": 34,
                                            "friends_count": 202,
                                            "statuses_count": 192,
                                        },
                                    }
                                }
                            }
                        },
                    )
                ],
                "dom": {'[data-testid="UserName"]': _FakeLocatorNode(visible=True, text="@peterzhu1003")},
            },
        }
        profiles = {
            "peterzhu1003": {
                "handle": "peterzhu1003",
                "profile_state": "SUSPECT_UNAVAILABLE",
                "followers": None,
                "following": None,
                "tweets": None,
                "detail": "User not found",
            }
        }
        report, fake_page, sess = self._run_probe_with_scenarios(
            scenarios,
            profile_states=profiles,
            targets=[("x_myself", "peterzhu1003")],
        )
        self.assertTrue(sess.closed)
        self.assertEqual(report["targets"][0]["profile"]["profile_state"], "ALIVE")
        self.assertEqual(report["targets"][0]["profile"]["followers"], 34)
        self.assertEqual(report["targets"][0]["verdict"], "SEARCH_BANNED")
        self.assertIn("profile:peterzhu1003", fake_page.visited_queries)

    def test_suspect_unavailable_confirmed_by_first_party_x_profile_becomes_unavailable(self):
        scenarios = {
            "2434954452qqc": {
                "dom": {'[data-testid="emptyState"]': _FakeLocatorNode(visible=True, text="No results")},
            },
            "nasa": {
                "dom": {"tweet_links": [("/NASA/status/200001", "Artemis update")]},
            },
            "profile:2434954452qqc": {
                "dom": {
                    'div[data-testid="empty_state_header_text"]': _FakeLocatorNode(
                        visible=True,
                        text="Account suspended",
                    )
                },
            },
        }
        profiles = {
            "2434954452qqc": {
                "handle": "2434954452qqc",
                "profile_state": "SUSPECT_UNAVAILABLE",
                "followers": None,
                "following": None,
                "tweets": None,
                "detail": "User not found",
            }
        }
        report, _page, sess = self._run_probe_with_scenarios(
            scenarios,
            profile_states=profiles,
            targets=[("x_dahao", "2434954452qqc")],
        )
        self.assertTrue(sess.closed)
        self.assertEqual(report["targets"][0]["profile"]["profile_state"], "UNAVAILABLE")
        self.assertEqual(report["targets"][0]["verdict"], "PROFILE_UNAVAILABLE")

    def test_verify_profile_on_page_gql_unavailable_and_dom_visible(self):
        gql_unavail = _FakeSearchPage({
            "profile:gone": {
                "responses": [
                    _FakeResponse(
                        "https://x.com/i/api/graphql/xyz/UserByScreenName",
                        200,
                        {"data": {"user": {"result": {"__typename": "UserUnavailable", "reason": "Suspended"}}}},
                    )
                ]
            }
        })
        r1 = probe.verify_profile_on_page(gql_unavail, "gone", wait_seconds=1.0)
        self.assertEqual(r1["profile_state"], "UNAVAILABLE")
        self.assertIn("Suspended", r1["detail"])

        dom_alive = _FakeSearchPage({
            "profile:okuser": {
                "dom": {'[data-testid="UserName"]': _FakeLocatorNode(visible=True, text="okuser")},
            }
        })
        r2 = probe.verify_profile_on_page(dom_alive, "okuser", wait_seconds=1.0)
        self.assertEqual(r2["profile_state"], "ALIVE")

        obs_susp = _FakeSearchPage({
            "profile:okuser": {
                "final_url": "https://x.com/account/suspended",
            }
        })
        r3 = probe.verify_profile_on_page(obs_susp, "okuser", wait_seconds=1.0)
        self.assertEqual(r3["profile_state"], "OBSERVER_BLOCKED")

    def test_format_probe_report_renders_chinese_verdicts(self):
        report = {
            "checked_at": "2026-11-24 19:15:00",
            "observer_session_dir": r"W:\temp\x_xiaohao",
            "observer_ok": True,
            "observer_error": "",
            "calibration": {"performed": True, "control_handle": "NASA", "passed": True},
            "targets": [
                {
                    "alias": "x_dahao",
                    "handle": "2434954452qqc",
                    "verdict": "SEARCH_BANNED",
                    "profile": {"profile_state": "ALIVE", "followers": 682, "following": 0, "tweets": 315},
                    "search": {"search_state": "HIDDEN", "detail": "DOM emptyState: No results", "tweets": []},
                },
                {
                    "alias": "x_myself",
                    "handle": "peterzhu1003",
                    "verdict": "VISIBLE_OK",
                    "profile": {"profile_state": "ALIVE", "followers": 34, "following": 202, "tweets": 192},
                    "search": {
                        "search_state": "VISIBLE",
                        "detail": "SearchTimeline HTTP 200 returned 1 tweet(s)",
                        "tweets": [{"tweet_id": "987654321"}],
                    },
                },
            ],
        }
        rendered = probe.format_probe_report(report)
        self.assertIn("搜索可见已恢复", rendered)
        self.assertIn("仍处于 Search Ban", rendered)
        self.assertIn("987654321", rendered)

    def test_headless_burner_session_rejects_monitored_dirs(self):
        for bad in (r"W:\temp\x_myself", r"W:\temp\x_dahao", ""):
            with self.assertRaises(AccountIsolationError):
                probe._HeadlessBurnerSession(bad)

        sess = probe._HeadlessBurnerSession(r"W:\temp\x_xiaohao")
        self.assertIn("x_xiaohao", sess.user_data_dir.lower())

    def test_probe_x_accounts_selects_headless_vs_headed_session_factory(self):
        fake_headless = _FakeSession(_FakeSearchPage({
            "2434954452qqc": {"dom": {"tweet_links": [("/2434954452qqc/status/1", "ok")]}},
        }))
        fake_headed = _FakeSession(_FakeSearchPage({
            "2434954452qqc": {"dom": {"tweet_links": [("/2434954452qqc/status/2", "ok")]}},
        }))
        with (
            patch("platforms.x_account_probe.fetch_public_profile_status", return_value={"profile_state": "ALIVE"}),
            patch("platforms.x_account_probe._HeadlessBurnerSession", return_value=fake_headless) as m_hl,
            patch("platforms.x_account_probe.XBrowserSession", return_value=fake_headed) as m_hd,
        ):
            r_hl = probe.probe_x_accounts(
                targets=[("x_dahao", "2434954452qqc")],
                observer_session_dir=r"W:\temp\x_xiaohao",
                headless=True,
                wait_seconds=1.0,
            )
            r_hd = probe.probe_x_accounts(
                targets=[("x_dahao", "2434954452qqc")],
                observer_session_dir=r"W:\temp\x_xiaohao",
                headless=False,
                wait_seconds=1.0,
            )

        m_hl.assert_called_once()
        m_hd.assert_called_once()
        self.assertTrue(fake_headless.closed)
        self.assertTrue(fake_headed.closed)
        self.assertTrue(r_hl["headless"])
        self.assertFalse(r_hd["headless"])
        self.assertEqual(r_hl["targets"][0]["verdict"], "VISIBLE_OK")
        self.assertEqual(r_hd["targets"][0]["verdict"], "VISIBLE_OK")

    def test_run_probe_daemon_logs_transition_and_observer_backoff(self):
        round_1 = {
            "checked_at": "2026-11-24 19:30:00",
            "observer_session_dir": r"W:\temp\x_xiaohao",
            "headless": True,
            "observer_ok": True,
            "observer_error": "",
            "calibration": {"performed": True, "control_handle": "NASA", "passed": True},
            "targets": [
                {
                    "alias": "x_dahao",
                    "handle": "2434954452qqc",
                    "verdict": "SEARCH_BANNED",
                    "profile": {"profile_state": "ALIVE", "followers": 682, "following": 0, "tweets": 315},
                    "search": {"search_state": "HIDDEN", "detail": "DOM emptyState: No results", "tweets": []},
                }
            ],
        }
        round_2 = {
            "checked_at": "2026-11-24 20:30:00",
            "observer_session_dir": r"W:\temp\x_xiaohao",
            "headless": True,
            "observer_ok": True,
            "observer_error": "",
            "calibration": {"performed": False, "control_handle": "NASA", "passed": True},
            "targets": [
                {
                    "alias": "x_dahao",
                    "handle": "2434954452qqc",
                    "verdict": "VISIBLE_OK",
                    "profile": {"profile_state": "ALIVE", "followers": 685, "following": 0, "tweets": 315},
                    "search": {"search_state": "VISIBLE", "detail": "ok", "tweets": [{"tweet_id": "777"}]},
                }
            ],
        }
        round_err = {
            "checked_at": "2026-11-24 21:30:00",
            "observer_session_dir": r"W:\temp\x_xiaohao",
            "headless": True,
            "observer_ok": False,
            "observer_error": "not_logged_in",
            "calibration": {"performed": False, "control_handle": "NASA", "passed": True},
            "targets": [
                {
                    "alias": "x_dahao",
                    "handle": "2434954452qqc",
                    "verdict": "OBSERVER_ERROR",
                    "profile": {"profile_state": "ALIVE", "followers": 685, "following": 0, "tweets": 315},
                    "search": {"search_state": "OBSERVER_BLOCKED", "detail": "not_logged_in", "tweets": []},
                }
            ],
        }

        mem_logger = logging.getLogger("test_x_account_probe_mem")
        mem_logger.setLevel(logging.INFO)
        mem_logger.propagate = False

        with (
            patch("platforms.x_account_probe.logger", mem_logger),
            patch("platforms.x_account_probe.probe_x_accounts", side_effect=[round_1, round_2]),
            patch("common.daemon.interruptible_sleep", return_value=True),
            self.assertLogs(mem_logger, level="INFO") as cm,
        ):
            rounds = probe.run_probe_daemon(
                targets=[("x_dahao", "2434954452qqc")],
                observer_session_dir=r"W:\temp\x_xiaohao",
                interval_hours=6.0,
                max_rounds=2,
            )
        self.assertEqual(rounds, 2)
        joined = "\n".join(cm.output)
        self.assertIn("状态变化", joined)
        self.assertIn("SEARCH_BANNED -> VISIBLE_OK", joined)

        # Verify observer error applies long backoff override via run_daemon_loop
        sleep_calls = []

        def _fake_sleep(sec, *_args, **_kwargs):
            sleep_calls.append(sec)
            return True

        with (
            patch("platforms.x_account_probe.logger", mem_logger),
            patch("platforms.x_account_probe.probe_x_accounts", side_effect=[round_err, round_2]),
            patch("common.daemon.interruptible_sleep", side_effect=_fake_sleep),
            self.assertLogs(mem_logger, level="INFO"),
        ):
            rounds2 = probe.run_probe_daemon(
                targets=[("x_dahao", "2434954452qqc")],
                observer_session_dir=r"W:\temp\x_xiaohao",
                interval_hours=6.0,
                observer_error_backoff_hours=12.0,
                max_rounds=2,
            )
        self.assertEqual(rounds2, 2)
        self.assertEqual(sleep_calls, [12.0 * 3600.0])


if __name__ == "__main__":
    unittest.main()
