"""lib.neynar and the cast-search paging in pipelines.token_evangelists.

Cast search is the one Neynar endpoint that nests its payload under `result`
instead of returning `casts`/`next` at the top level, which is why
`search_response_to_casts` exists and why token_evangelists reads the cursor
itself rather than going through the generic paginator. Both halves are pinned
here against the recorded response shape.
"""

from __future__ import annotations

import pandas as pd
import pytest

from config.settings import NEYNAR_API_BASE
from lib.neynar import MAX_FIDS_PER_CALL, NeynarClient, search_response_to_casts
from pipelines.token_evangelists import collect_token_casts, iter_search_casts

FEED_URL = f"{NEYNAR_API_BASE}/v2/farcaster/feed/channels"
SEARCH_URL = f"{NEYNAR_API_BASE}/v2/farcaster/cast/search"
BULK_URL = f"{NEYNAR_API_BASE}/v2/farcaster/user/bulk"
TOKEN = "0x7c9f4c87d911613fe9ca58b579f737911aad2d43"


def client() -> NeynarClient:
    return NeynarClient(api_key="test-key", rps=0)


def test_a_missing_api_key_fails_loudly(monkeypatch):
    import lib.neynar as neynar

    monkeypatch.setattr(neynar, "NEYNAR_API_KEY", None)

    with pytest.raises(RuntimeError, match="NEYNAR_API_KEY is not set"):
        NeynarClient()


def test_the_api_key_and_accept_headers_are_sent(requests_mock):
    requests_mock.get(FEED_URL, json={"casts": []})

    list(client().channel_feed("arbitrum"))

    headers = requests_mock.request_history[0].headers
    assert headers["x-api-key"] == "test-key"
    assert headers["accept"] == "application/json"


def test_paginate_follows_the_top_level_cursor(requests_mock):
    requests_mock.get(
        FEED_URL,
        [
            {"json": {"casts": [{"hash": "0x1"}], "next": {"cursor": "c2"}}},
            {"json": {"casts": [{"hash": "0x2"}], "next": {"cursor": None}}},
        ],
    )

    casts = list(client().channel_feed("arbitrum"))

    assert [c["hash"] for c in casts] == ["0x1", "0x2"]
    assert requests_mock.request_history[1].qs["cursor"] == ["c2"]
    assert requests_mock.request_history[0].qs["channel_ids"] == ["arbitrum"]


def test_paginate_stops_at_max_pages(requests_mock):
    requests_mock.get(FEED_URL, json={"casts": [{"hash": "0x1"}], "next": {"cursor": "more"}})

    assert len(list(client().channel_feed("arbitrum", max_pages=3))) == 3
    assert requests_mock.call_count == 3


def test_paginate_stops_on_a_payload_with_no_cursor_key(requests_mock):
    requests_mock.get(FEED_URL, json={"casts": [{"hash": "0x1"}]})

    assert len(list(client().channel_feed("arbitrum"))) == 1
    assert requests_mock.call_count == 1


def test_bulk_users_batches_at_a_hundred_fids(requests_mock):
    def respond(request, context):
        fids = request.qs["fids"][0].split(",")
        assert len(fids) <= MAX_FIDS_PER_CALL
        return {"users": [{"fid": int(f)} for f in fids]}

    requests_mock.get(BULK_URL, json=respond)

    users = client().bulk_users(range(250))

    assert len(users) == 250
    assert requests_mock.call_count == 3
    assert [len(r.qs["fids"][0].split(",")) for r in requests_mock.request_history] == [100, 100, 50]


def test_bulk_users_of_nothing_makes_no_calls(requests_mock):
    assert client().bulk_users([]) == []
    assert requests_mock.call_count == 0


def test_cast_reactions_sends_the_hash_and_types(requests_mock):
    requests_mock.get(
        f"{NEYNAR_API_BASE}/v2/farcaster/reactions/cast",
        json={"reactions": [{"user": {"fid": 1}}], "next": {"cursor": None}},
    )

    reactions = list(client().cast_reactions("0xABC", types="likes", max_pages=1))

    assert reactions == [{"user": {"fid": 1}}]
    request = requests_mock.request_history[0]
    assert request.qs["hash"] == ["0xabc"]
    assert request.qs["types"] == ["likes"]


def test_channel_returns_the_channel_object(requests_mock):
    requests_mock.get(
        f"{NEYNAR_API_BASE}/v2/farcaster/channel",
        json={"channel": {"id": "arbitrum", "parent_url": "https://warpcast.com/~/channel/arbitrum"}},
    )

    assert client().channel("arbitrum")["id"] == "arbitrum"


# --- the cast-search nesting --------------------------------------------


def test_search_response_to_casts_unwraps_the_result_envelope(fixture_json):
    payload = fixture_json("neynar_cast_search")["page1"]

    casts = search_response_to_casts(payload)

    assert [c["hash"] for c in casts] == [
        "0xa1b2c3d4e5f60718293a4b5c6d7e8f9012345678",
        "0xb2c3d4e5f60718293a4b5c6d7e8f901234567890",
    ]


def test_search_response_to_casts_also_accepts_a_flat_payload():
    assert search_response_to_casts({"casts": [{"hash": "0x1"}]}) == [{"hash": "0x1"}]
    assert search_response_to_casts({}) == []


def test_the_generic_paginator_finds_nothing_in_a_search_payload(requests_mock, fixture_json):
    """Why token_evangelists does not use search_casts: the generic paginator
    reads `casts` from the top level, which cast search does not have."""
    requests_mock.get(SEARCH_URL, json=fixture_json("neynar_cast_search")["page1"])

    assert list(client().search_casts("$ARBSUMMER")) == []


def test_iter_search_casts_reads_both_the_casts_and_the_cursor_from_result(
    requests_mock, fixture_json
):
    recorded = fixture_json("neynar_cast_search")
    requests_mock.get(SEARCH_URL, [{"json": recorded["page1"]}, {"json": recorded["page2"]}])

    casts = list(iter_search_casts(client(), "$ARBSUMMER", max_pages=None))

    assert len(casts) == 4
    first = requests_mock.request_history[0]
    assert first.qs["mode"] == ["literal"]  # a substring match, like the reference LIKE
    assert first.qs["sort_type"] == ["desc_chron"]  # the early-stop depends on the order
    assert requests_mock.request_history[1].qs["cursor"] == ["cursor-page-2"]


def test_iter_search_casts_stops_at_max_pages(requests_mock, fixture_json):
    recorded = fixture_json("neynar_cast_search")
    requests_mock.get(SEARCH_URL, [{"json": recorded["page1"]}, {"json": recorded["page2"]}])

    assert len(list(iter_search_casts(client(), "$ARBSUMMER", max_pages=1))) == 2


def test_iter_search_casts_stops_on_an_empty_page(requests_mock):
    requests_mock.get(SEARCH_URL, json={"result": {"casts": [], "next": {"cursor": "more"}}})

    assert list(iter_search_casts(client(), "$X", max_pages=None)) == []
    assert requests_mock.call_count == 1


# --- collect_token_casts -------------------------------------------------


def test_collect_token_casts_keeps_only_matching_casts_and_records_how(
    requests_mock, fixture_json
):
    recorded = fixture_json("neynar_cast_search")
    requests_mock.get(SEARCH_URL, [{"json": recorded["page1"]}, {"json": recorded["page2"]}] * 2)
    address_fids: dict[str, int] = {}

    casts = collect_token_casts(
        client(),
        {"token_address": TOKEN, "symbol": "ARBSUMMER", "chain_id": 42161},
        since=pd.Timestamp("2026-08-01T00:00:00Z"),
        max_pages=2,
        address_fids=address_fids,
    )

    by_hash = {c["cast_hash"]: c for c in casts}
    assert set(by_hash) == {
        "0xa1b2c3d4e5f60718293a4b5c6d7e8f9012345678",  # $ARBSUMMER in the text
        "0xb2c3d4e5f60718293a4b5c6d7e8f901234567890",  # posted on the token frame
        "0xc3d4e5f60718293a4b5c6d7e8f90123456789012",  # names the contract
    }
    assert by_hash["0xa1b2c3d4e5f60718293a4b5c6d7e8f9012345678"]["matched_on"] == "ticker"
    assert by_hash["0xb2c3d4e5f60718293a4b5c6d7e8f901234567890"]["matched_on"] == "parent_url"
    assert by_hash["0xc3d4e5f60718293a4b5c6d7e8f90123456789012"]["matched_on"] == "address"
    assert by_hash["0xa1b2c3d4e5f60718293a4b5c6d7e8f9012345678"]["likes_count"] == 12

    # Wallets ride along free with casts we already paid for.
    assert address_fids["0xbbbb000000000000000000000000000000000002"] == 3621
    assert address_fids["0xaaaa000000000000000000000000000000000001"] == 3621


def test_collect_token_casts_stops_at_the_window_edge(requests_mock, fixture_json):
    recorded = fixture_json("neynar_cast_search")
    requests_mock.get(SEARCH_URL, [{"json": recorded["page1"]}, {"json": recorded["page2"]}] * 2)

    casts = collect_token_casts(
        client(),
        {"token_address": TOKEN, "symbol": "ARBSUMMER", "chain_id": 42161},
        since=pd.Timestamp("2026-08-05T00:00:00Z"),
        max_pages=2,
        address_fids={},
    )

    # The desc_chron order means the 2026-08-04 cast ends the walk.
    assert {c["cast_hash"] for c in casts} == {
        "0xa1b2c3d4e5f60718293a4b5c6d7e8f9012345678",
        "0xb2c3d4e5f60718293a4b5c6d7e8f901234567890",
    }


def test_collect_token_casts_skips_the_ticker_search_for_a_short_symbol(
    requests_mock, fixture_json
):
    requests_mock.get(SEARCH_URL, json=fixture_json("neynar_cast_search")["page2"])

    collect_token_casts(
        client(),
        {"token_address": TOKEN, "symbol": "AB", "chain_id": 42161},
        since=pd.Timestamp("2026-08-01T00:00:00Z"),
        max_pages=1,
        address_fids={},
    )

    # One query only — the address. A two-character ticker matches half of
    # Farcaster and would cost a page walk for nothing.
    queries = [r.qs["q"][0] for r in requests_mock.request_history]
    assert queries == [TOKEN]
