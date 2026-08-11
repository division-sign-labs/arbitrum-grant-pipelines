"""lib.bankr — the freshness/identity top-up on top of Dune's robinhood.* tables.

The endpoint returns exactly 50 launches with no pagination, so there is nothing
to page-test; what matters is `normalise_launch`, which is where Bankr's
millisecond epoch becomes an ISO timestamp and its chain *name* becomes the
numeric chain id the whole CSV contract and graph key on.
"""

from __future__ import annotations

from config.settings import BANKR_API_BASE, CHAIN_ROBINHOOD
from lib.bankr import CHAIN_IDS, BankrClient, normalise_launch

LAUNCHES_URL = f"{BANKR_API_BASE}/token-launches"


def client() -> BankrClient:
    return BankrClient(rps=0)


def test_normalise_launch_converts_ms_epoch_and_chain_name(fixture_json):
    row = fixture_json("bankr_token_launches")["launches"][0]

    assert normalise_launch(row) == {
        "token_address": "0x4a1f9c2e0b7d6853a9e1c0f2b3d45566778899aa",
        "chain_id": CHAIN_ROBINHOOD,
        "chain": "robinhood",
        "platform": "bankr",
        "deployer_address": "0x9f8e7d6c5b4a39281706f5e4d3c2b1a099887766",
        "fee_recipient_address": "0x1122334455667788990011223344556677889900",
        "x_username": "bankrbot",
        "name": "Bankr Bot",
        "symbol": "BNKR",
        # 1770000000000 ms -> 2026-02-02T02:40:00+00:00 UTC, not local time.
        "deployed_at": "2026-02-02T02:40:00+00:00",
        "tx_hash": "0xab12cd34ef567890abcdef1234567890abcdef1234567890abcdef1234567890",
        "pool_address": "0xfedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
        "launch_type": "doppler",
        "status": "graduated",
        "source": "bankr_api",
    }


def test_normalise_launch_maps_base_and_nulls_empty_strings(fixture_json):
    flat = normalise_launch(fixture_json("bankr_token_launches")["launches"][1])

    assert flat["chain_id"] == CHAIN_IDS["base"] == 8453
    # An empty walletAddress must be None, never the zero-length address.
    assert flat["deployer_address"] is None
    assert flat["fee_recipient_address"] is None
    assert flat["pool_address"] is None
    assert flat["x_username"] is None


def test_normalise_launch_leaves_an_unknown_chain_unresolved(fixture_json):
    flat = normalise_launch(fixture_json("bankr_token_launches")["launches"][2])

    # A new Bankr deploy target must surface as a null chain_id rather than
    # being silently filed under Robinhood.
    assert flat["chain"] == "somenewchain"
    assert flat["chain_id"] is None
    assert flat["deployed_at"] is None  # no timestamp at all
    assert flat["tx_hash"] is None
    assert flat["deployer_address"] is None  # deployer was null


def test_normalise_launch_of_an_empty_record():
    flat = normalise_launch({})

    assert flat["token_address"] == ""
    assert flat["chain"] == ""
    assert flat["chain_id"] is None
    assert flat["platform"] == "bankr"
    assert flat["source"] == "bankr_api"


def test_recent_launches_reads_the_launches_key(requests_mock, fixture_json):
    payload = fixture_json("bankr_token_launches")
    requests_mock.get(LAUNCHES_URL, json=payload)

    launches = client().recent_launches()

    assert len(launches) == 3
    assert launches[0]["tokenSymbol"] == "BNKR"


def test_recent_launches_of_an_unexpected_payload_is_empty(requests_mock):
    requests_mock.get(LAUNCHES_URL, json={"data": []})

    assert client().recent_launches() == []


def test_creator_fees_lowercases_the_wallet_in_the_path(requests_mock):
    address = "0x9F8E7D6C5B4A39281706F5E4D3C2B1A099887766"
    requests_mock.get(
        f"{BANKR_API_BASE}/public/doppler/creator-fees/{address.lower()}",
        json={"totalFeesUsd": 12.5},
    )

    assert client().creator_fees(address) == {"totalFeesUsd": 12.5}


def test_creator_fees_degrades_to_none_rather_than_failing_a_run(requests_mock, recorded_sleep):
    address = "0x1111111111111111111111111111111111111111"
    requests_mock.get(
        f"{BANKR_API_BASE}/public/doppler/creator-fees/{address}",
        status_code=404,
        text="not found",
    )

    # This is a cheap "did this wallet launch anything" probe; a miss is the
    # common case and must not abort the pipeline around it.
    assert client().creator_fees(address) is None
