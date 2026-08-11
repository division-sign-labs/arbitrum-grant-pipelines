"""lib.clanker — the token -> Farcaster-fid edge that exists nowhere on-chain.

The pagination loop is driven against recorded page payloads
(tests/fixtures/clanker_tokens_page*.json) reproducing the live response shape:
`{"data": [...], "cursor": ..., "total": ...}` with the deployer's Farcaster
identity nested at `related.user`. Page 2 deliberately repeats page 1's last
token, which is the real straddle the cursor produces when several tokens share
a deploy second.
"""

from __future__ import annotations

import json

import pytest

from config.settings import CHAIN_ARBITRUM, CLANKER_API_BASE
from lib.clanker import PAGE_SIZE, ClankerClient, normalise_token

TOKENS_URL = f"{CLANKER_API_BASE}/tokens"


@pytest.fixture
def pages(fixture_json):
    return [
        fixture_json("clanker_tokens_page1"),
        fixture_json("clanker_tokens_page2"),
    ]


@pytest.fixture
def clanker_api(requests_mock, pages):
    """Serve the recorded pages, switching on the cursor the client sends back.

    requests_mock lowercases the query string it parses, so the cursor keys are
    lowercased to match.
    """
    by_cursor = {None: pages[0], pages[0]["cursor"].lower(): pages[1]}

    def respond(request, context):
        cursor = request.qs.get("cursor", [None])[0]
        payload = by_cursor.get(cursor)
        if payload is None:
            context.status_code = 404
            return json.dumps({"error": f"unexpected cursor {cursor!r}"})
        return json.dumps(payload)

    requests_mock.get(TOKENS_URL, text=respond)
    return requests_mock


def client() -> ClankerClient:
    return ClankerClient(rps=0)


# --- normalise_token -----------------------------------------------------


def test_normalise_token_flattens_a_full_record(pages):
    row = pages[0]["data"][0]

    assert normalise_token(row, CHAIN_ARBITRUM) == {
        "token_address": "0x3192583d06e805d966737797d72f6a7b50fcf069",
        "chain_id": 42161,
        "platform": "clanker",
        "deployer_address": "0x1b85596a595d330ae7b0d837e77bc5101ca8a32a",
        "admin_address": "0x1b85596a595d330ae7b0d837e77bc5101ca8a32a",
        "fid": 194,
        "username": "rish",
        "name": "test",
        "symbol": "TEST",
        "deployed_at": "2026-08-05T05:19:40.000Z",
        "tx_hash": "0x29b8ed1081e47562cdd7f5d910beb3a27296207409368c9d60f1dff99167f3ee",
        "pool_address": "0x5b01c88050cfe90fc9023f862fbd661878689e35bf42478499d9cacfd345303b",
        "paired_token": "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
        "token_type": "clanker_v4",
        "starting_market_cap_usd": 9.870869513221747,
        "price_usd": 0.0,
        "market_cap_usd": 0.0,
        "volume_24h_usd": 0,
    }


def test_normalise_token_leaves_fid_null_when_there_was_no_farcaster_requestor(pages):
    # ~39% of Arbitrum Clanker tokens are deployed from a contract with no
    # requesting account; those rely on the linked_wallets join instead.
    flat = normalise_token(pages[0]["data"][1], CHAIN_ARBITRUM)

    assert flat["fid"] is None
    assert flat["username"] is None
    assert flat["deployer_address"] == "0x3b5aedd9ab921e592d2fccaa6e33cf893e7eeb70"


def test_normalise_token_falls_back_to_created_at_and_nulls_empty_strings(pages):
    flat = normalise_token(pages[1]["data"][1], CHAIN_ARBITRUM)

    assert flat["deployed_at"] == "2026-08-04T22:13:02.881Z"  # deployed_at was null
    assert flat["pool_address"] is None  # null
    assert flat["admin_address"] is None  # "" must not become the empty address
    assert flat["paired_token"] is None  # no pool_config at all
    assert flat["starting_market_cap_usd"] is None
    assert (flat["market_cap_usd"], flat["volume_24h_usd"]) == (31000.5, 812.25)


def test_normalise_token_uses_the_requested_chain_when_the_row_omits_it():
    flat = normalise_token({"contract_address": "0xAB", "name": "x"}, CHAIN_ARBITRUM)

    assert flat["chain_id"] == CHAIN_ARBITRUM
    assert flat["token_address"] == "0xab"
    assert flat["platform"] == "clanker"


def test_normalise_token_survives_a_row_with_nothing_in_it():
    flat = normalise_token({}, 4663)

    assert flat["token_address"] == ""
    assert flat["chain_id"] == 4663
    assert all(flat[key] is None for key in ("deployer_address", "fid", "tx_hash"))


# --- pagination ----------------------------------------------------------


def test_iter_tokens_follows_the_cursor_and_dedupes_the_straddle(clanker_api):
    rows = list(client().iter_tokens(CHAIN_ARBITRUM))

    addresses = [row["contract_address"] for row in rows]
    assert len(addresses) == len(set(addresses)) == 3
    # Page 2 repeats page 1's second token; it must be yielded once, from page 1.
    assert addresses[1].lower() == "0xb52a72a30e6da051a00f77015df73c940c2a43c2"
    assert addresses[2] == "0x7c9f4c87d911613fe9ca58b579f737911aad2d43"


def test_iter_tokens_sends_the_documented_query_parameters(clanker_api, pages):
    list(client().iter_tokens(CHAIN_ARBITRUM))

    first = clanker_api.request_history[0]
    assert first.qs["chainid"] == [str(CHAIN_ARBITRUM)]
    assert first.qs["limit"] == [str(PAGE_SIZE)]
    assert first.qs["sort"] == ["desc"]
    assert first.qs["includeuser"] == ["true"]
    assert "cursor" not in first.qs
    # The second call is the same query plus the cursor page 1 handed back.
    # (requests_mock lowercases the query string it parses.)
    assert clanker_api.request_history[1].qs["cursor"] == [pages[0]["cursor"].lower()]


def test_include_user_can_be_turned_off(clanker_api):
    list(client().iter_tokens(CHAIN_ARBITRUM, max_pages=1, include_user=False))

    assert "includeuser" not in clanker_api.request_history[0].qs


def test_iter_tokens_stops_at_max_pages(clanker_api):
    rows = list(client().iter_tokens(CHAIN_ARBITRUM, max_pages=1))

    assert len(rows) == 2
    assert clanker_api.call_count == 1


def test_iter_tokens_stops_when_the_cursor_runs_out(clanker_api):
    list(client().iter_tokens(CHAIN_ARBITRUM))

    # Page 2 has cursor=null, so exactly two requests — not a third that 404s.
    assert clanker_api.call_count == 2


def test_iter_tokens_stops_on_an_empty_page(requests_mock):
    requests_mock.get(TOKENS_URL, json={"data": [], "cursor": "more", "total": 0})

    assert list(client().iter_tokens(CHAIN_ARBITRUM)) == []
    assert requests_mock.call_count == 1


def test_iter_tokens_dedupes_on_id_when_a_row_has_no_contract_address(requests_mock):
    requests_mock.get(
        TOKENS_URL,
        json={"data": [{"id": 1, "name": "a"}, {"id": 1, "name": "a"}], "cursor": None},
    )

    assert len(list(client().iter_tokens(CHAIN_ARBITRUM))) == 1


def test_total_tokens_reads_the_envelope_count(requests_mock, pages):
    requests_mock.get(TOKENS_URL, json=pages[0])

    assert client().total_tokens(CHAIN_ARBITRUM) == 565
    assert requests_mock.request_history[0].qs["limit"] == ["1"]


def test_total_tokens_of_a_chain_clanker_does_not_serve_is_zero(requests_mock):
    requests_mock.get(TOKENS_URL, json={"data": []})

    assert client().total_tokens(999) == 0
