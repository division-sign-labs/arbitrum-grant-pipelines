"""lib.hyperliquid — two /info calls per wallet, one CSV row out.

Two things bite here and both are covered: `cumVlm` comes back as a *string*
(and `float("0.0") > 0` is False, which is what "checked, never traded" hangs
on), and the ledger endpoint returns a bare JSON list rather than an object, so
an error payload is a dict and must not be treated as zero events.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from config.settings import HYPERLIQUID_API_URL
from lib.hyperliquid import HyperliquidClient

UTC = timezone.utc
ADDRESS = "0xAbCdEf0123456789AbCdEf0123456789AbCdEf01"


def client() -> HyperliquidClient:
    # The real client paces to 0.8 rps; tests do not need the budget.
    return HyperliquidClient(rps=0)


@pytest.fixture
def info(requests_mock):
    """Dispatch POST /info on the payload's `type`, the way the real API does."""
    routes: dict[str, object] = {}

    def respond(request, context):
        payload = request.json()
        try:
            return routes[payload["type"]]
        except KeyError:
            context.status_code = 422
            return {"error": f"unhandled type {payload.get('type')!r}"}

    requests_mock.post(HYPERLIQUID_API_URL, json=respond)
    return routes


# --- lifetime volume -----------------------------------------------------


def test_lifetime_volume_parses_the_string_cum_vlm(info, fixture_json):
    info["userRateLimit"] = fixture_json("hyperliquid_info")["userRateLimit"]

    assert client().lifetime_volume(ADDRESS) == pytest.approx(1234567.89)


def test_lifetime_volume_lowercases_the_address_it_asks_about(info, requests_mock):
    info["userRateLimit"] = {"cumVlm": "0"}

    client().lifetime_volume(ADDRESS)

    assert requests_mock.request_history[-1].json() == {
        "type": "userRateLimit",
        "user": ADDRESS.lower(),
    }


@pytest.mark.parametrize(
    "payload",
    [
        {"cumVlm": "0.0"},
        {"cumVlm": None},
        {"cumVlm": ""},
        {},
        {"cumVlm": "not-a-number"},
        {"cumVlm": ["nope"]},
    ],
)
def test_lifetime_volume_degrades_to_zero_rather_than_raising(info, payload):
    info["userRateLimit"] = payload

    assert client().lifetime_volume(ADDRESS) == 0.0


# --- first activity ------------------------------------------------------


def test_first_activity_is_the_minimum_ledger_time(info, fixture_json):
    info["userNonFundingLedgerUpdates"] = fixture_json("hyperliquid_info")[
        "userNonFundingLedgerUpdates"
    ]

    first_at, count = client().first_activity(ADDRESS)

    # The list is not sorted; the earliest entry is the second one.
    assert first_at == datetime(2024, 1, 1, tzinfo=UTC)
    assert count == 3


def test_first_activity_requests_the_whole_history(info, requests_mock):
    info["userNonFundingLedgerUpdates"] = []

    client().first_activity(ADDRESS)

    assert requests_mock.request_history[-1].json() == {
        "type": "userNonFundingLedgerUpdates",
        "user": ADDRESS.lower(),
        "startTime": 0,
    }


def test_first_activity_of_an_empty_ledger(info):
    info["userNonFundingLedgerUpdates"] = []

    assert client().first_activity(ADDRESS) == (None, 0)


def test_first_activity_when_the_response_is_not_a_list(info):
    # An error payload is an object, not a list; treating it as "no events" is
    # right, but it must not blow up on .get() over a dict's keys.
    info["userNonFundingLedgerUpdates"] = {"error": "rate limited"}

    assert client().first_activity(ADDRESS) == (None, 0)


def test_first_activity_counts_events_even_when_none_carry_a_time(info):
    info["userNonFundingLedgerUpdates"] = [{"delta": {"type": "deposit"}}, {"time": 0}]

    first_at, count = client().first_activity(ADDRESS)

    # time=0 is falsy and is skipped along with the missing one, but both are
    # still ledger events and the count says so.
    assert first_at is None
    assert count == 2


def test_first_activity_skips_malformed_entries(info):
    info["userNonFundingLedgerUpdates"] = ["junk", {"time": 1735689600000}, None]

    first_at, count = client().first_activity(ADDRESS)

    assert first_at == datetime(2025, 1, 1, tzinfo=UTC)
    assert count == 3


# --- wallet_summary ------------------------------------------------------


def test_wallet_summary_is_one_row_from_two_calls(info, fixture_json, requests_mock):
    recorded = fixture_json("hyperliquid_info")
    info["userRateLimit"] = recorded["userRateLimit"]
    info["userNonFundingLedgerUpdates"] = recorded["userNonFundingLedgerUpdates"]

    summary = client().wallet_summary(ADDRESS)

    assert set(summary) == {
        "address",
        "has_hl_activity",
        "cum_volume_usd",
        "first_activity_at",
        "ledger_event_count",
        "checked_at",
    }
    assert summary["address"] == ADDRESS.lower()
    assert summary["has_hl_activity"] is True
    assert summary["cum_volume_usd"] == pytest.approx(1234567.89)
    assert summary["first_activity_at"] == "2024-01-01T00:00:00+00:00"
    assert summary["ledger_event_count"] == 3
    assert requests_mock.call_count == 2


def test_a_wallet_that_only_ever_deposited_still_counts_as_active(info, fixture_json):
    info["userRateLimit"] = fixture_json("hyperliquid_info")["userRateLimit_never_traded"]
    info["userNonFundingLedgerUpdates"] = [{"time": 1767225600000, "delta": {"type": "deposit"}}]

    summary = client().wallet_summary(ADDRESS)

    assert summary["cum_volume_usd"] == 0.0
    assert summary["has_hl_activity"] is True


def test_a_wallet_that_never_touched_hyperliquid_is_recorded_as_checked(info, fixture_json):
    info["userRateLimit"] = fixture_json("hyperliquid_info")["userRateLimit_never_traded"]
    info["userNonFundingLedgerUpdates"] = []

    summary = client().wallet_summary(ADDRESS)

    # "checked, no activity" is a different fact from "never checked", and the
    # ingest depends on this row existing with has_hl_activity False.
    assert summary["has_hl_activity"] is False
    assert summary["cum_volume_usd"] == 0.0
    assert summary["first_activity_at"] is None
    assert summary["ledger_event_count"] == 0
    assert summary["checked_at"].endswith("+00:00")
