"""pipelines.token_evangelists — the attribution maths.

This is the one number in the repo that makes a causal-ish claim, so it gets the
most exact tests. Two properties are asserted over and over:

  * the window is half-open — engaged_at < block_time <= engaged_at + 5 days.
    A buy at the exact instant of the engagement earns nothing (the buyer cannot
    have been moved by a cast they engaged with simultaneously), and a buy one
    second past the fifth day earns nothing either.
  * credit is conserved. A purchase's `amount_usd` is split equally among the
    distinct authors who qualify, so summing `attributed_usd` across all authors
    of one purchase returns exactly the purchase.

The frames are built by hand rather than by running the pipeline: `attribute`
takes a normalised engagements frame and a normalised buys frame, and those two
shapes are the contract worth pinning.
"""

from __future__ import annotations

import pandas as pd
import pytest

from config.settings import ATTRIBUTION_WINDOW_DAYS
from lib.dune import DuneError
from pipelines.token_evangelists import (
    ATTRIBUTION_COLUMNS,
    SUMMARY_COLUMNS,
    TOKEN_CAST_COLUMNS,
    _to_utc,
    attribute,
    classify_cast,
    fetch_buys,
    harvest_addresses,
    load_token_registry,
    resolve_buyer_fids,
    select_tokens,
    summarise,
    token_volumes,
)

TOKEN = "0x7c9f4c87d911613fe9ca58b579f737911aad2d43"
OTHER_TOKEN = "0x912ce59144191c1204e64559fe8253a0e49e6548"
CHAIN = 42161
T0 = pd.Timestamp("2026-06-01T00:00:00Z")
WINDOW = pd.Timedelta(days=ATTRIBUTION_WINDOW_DAYS)


ENGAGEMENT_COLUMNS = [
    "token_address",
    "chain_id",
    "cast_hash",
    "author_fid",
    "engager_fid",
    "engaged_at",
    "engagement",
]
BUY_COLUMNS = [
    "token_address",
    "chain_id",
    "buyer_address",
    "tx_from",
    "tx_hash",
    "block_time",
    "amount_usd",
    "token_amount",
    "buyer_fid",
]


def engagements(*rows) -> pd.DataFrame:
    """rows: (author_fid, engager_fid, offset_from_T0[, token]).

    The shape `collect_engagements` produces once `run()` has normalised
    `engaged_at` to aware UTC.
    """
    return pd.DataFrame(
        [
            {
                "token_address": row[3] if len(row) > 3 else TOKEN,
                "chain_id": CHAIN,
                "cast_hash": f"0xcast{row[0]}",
                "author_fid": row[0],
                "engager_fid": row[1],
                "engaged_at": T0 + row[2],
                "engagement": "like",
            }
            for row in rows
        ],
        columns=ENGAGEMENT_COLUMNS,
    )


def buys(*rows) -> pd.DataFrame:
    """rows: (tx_hash, buyer_fid, offset_from_T0, amount_usd[, token]).

    The shape `fetch_buys` + `resolve_buyer_fids` produce, including the
    nullable Int64 buyer_fid that `attribute` has to bridge to the plain int64
    on the engagement side.
    """
    frame = pd.DataFrame(
        [
            {
                "token_address": row[4] if len(row) > 4 else TOKEN,
                "chain_id": CHAIN,
                "buyer_address": f"0xbuyer{row[1]}",
                "tx_from": f"0xbuyer{row[1]}",
                "tx_hash": row[0],
                "block_time": T0 + row[2],
                "amount_usd": row[3],
                "token_amount": 1.0,
                "buyer_fid": row[1],
            }
            for row in rows
        ],
        columns=BUY_COLUMNS,
    )
    frame["buyer_fid"] = frame["buyer_fid"].astype("Int64")
    return frame


# --- window boundaries ---------------------------------------------------


@pytest.mark.parametrize(
    "offset, credited",
    [
        (pd.Timedelta(0), False),  # simultaneous: the buyer was not moved by it
        (pd.Timedelta(seconds=1), True),  # one second after: inside
        (WINDOW - pd.Timedelta(seconds=1), True),  # just inside the far edge
        (WINDOW, True),  # exactly five days: inclusive
        (WINDOW + pd.Timedelta(seconds=1), False),  # one second late: outside
        (-pd.Timedelta(seconds=1), False),  # the buy came first
        (-WINDOW, False),
    ],
)
def test_the_attribution_window_is_half_open(offset, credited):
    result = attribute(
        engagements((7, 99, pd.Timedelta(0))),
        buys(("0xtx", 99, offset, 100.0)),
        ATTRIBUTION_WINDOW_DAYS,
    )

    assert (len(result) == 1) is credited
    if credited:
        assert result.iloc[0]["attributed_usd"] == 100.0
        assert result.iloc[0]["n_influencers"] == 1


def test_a_buy_just_outside_the_window_earns_nobody_anything():
    eng = engagements((1, 99, pd.Timedelta(0)), (2, 99, pd.Timedelta(days=4)))
    # 5 days + 1s after author 1's cast, but only 1 day + 1s after author 2's.
    purchases = buys(("0xtx", 99, WINDOW + pd.Timedelta(seconds=1), 90.0))

    result = attribute(eng, purchases, ATTRIBUTION_WINDOW_DAYS)

    assert list(result["author_fid"]) == [2]
    # Author 1 falling out of the window must not leave author 2 with a third of
    # the buy — the split is over the authors who *qualify*.
    assert result.iloc[0]["attributed_usd"] == 90.0
    assert result.iloc[0]["n_influencers"] == 1


# --- the equal split -----------------------------------------------------


def test_three_influencers_split_one_purchase_equally():
    eng = engagements(
        (1, 99, pd.Timedelta(days=1)),
        (2, 99, pd.Timedelta(days=2)),
        (3, 99, pd.Timedelta(days=3)),
    )
    purchases = buys(("0xtx", 99, pd.Timedelta(days=4), 300.0))

    result = attribute(eng, purchases, ATTRIBUTION_WINDOW_DAYS)

    assert sorted(result["author_fid"]) == [1, 2, 3]
    assert set(result["n_influencers"]) == {3}
    assert list(result["attributed_usd"]) == [100.0, 100.0, 100.0]
    # Credit is conserved: the split never invents or destroys volume.
    assert result["attributed_usd"].sum() == pytest.approx(300.0)


def test_one_author_engaged_twice_is_still_one_influencer():
    eng = engagements((1, 99, pd.Timedelta(days=1)), (1, 99, pd.Timedelta(days=2)))
    eng.loc[1, "cast_hash"] = "0xcast1b"  # a second cast by the same author
    purchases = buys(("0xtx", 99, pd.Timedelta(days=3), 250.0))

    result = attribute(eng, purchases, ATTRIBUTION_WINDOW_DAYS)

    assert len(result) == 1
    assert result.iloc[0]["n_influencers"] == 1
    assert result.iloc[0]["attributed_usd"] == 250.0


def test_the_split_is_per_purchase_not_per_buyer():
    eng = engagements((1, 99, pd.Timedelta(0)), (2, 99, pd.Timedelta(days=3)))
    purchases = buys(
        ("0xtx1", 99, pd.Timedelta(days=1), 100.0),  # only author 1 qualifies yet
        ("0xtx2", 99, pd.Timedelta(days=4), 200.0),  # both authors qualify
    )

    result = attribute(eng, purchases, ATTRIBUTION_WINDOW_DAYS)

    by_tx = result.set_index(["tx_hash", "author_fid"])["attributed_usd"].to_dict()
    assert by_tx == {("0xtx1", 1): 100.0, ("0xtx2", 1): 100.0, ("0xtx2", 2): 100.0}
    assert result.groupby("tx_hash")["attributed_usd"].sum().to_dict() == {
        "0xtx1": 100.0,
        "0xtx2": 200.0,
    }


def test_two_buyers_of_the_same_token_are_attributed_independently():
    eng = engagements((1, 99, pd.Timedelta(0)), (1, 55, pd.Timedelta(0)), (2, 55, pd.Timedelta(0)))
    purchases = buys(
        ("0xtx1", 99, pd.Timedelta(days=1), 100.0),
        ("0xtx2", 55, pd.Timedelta(days=1), 100.0),
    )

    result = attribute(eng, purchases, ATTRIBUTION_WINDOW_DAYS)

    assert result.set_index(["tx_hash", "author_fid"])["n_influencers"].to_dict() == {
        ("0xtx1", 1): 1,
        ("0xtx2", 1): 2,
        ("0xtx2", 2): 2,
    }


# --- what must not be attributed ----------------------------------------


def test_engagement_with_one_token_never_credits_a_buy_of_another():
    eng = engagements((1, 99, pd.Timedelta(0), OTHER_TOKEN))
    purchases = buys(("0xtx", 99, pd.Timedelta(days=1), 100.0, TOKEN))

    assert attribute(eng, purchases, ATTRIBUTION_WINDOW_DAYS).empty


def test_a_different_buyer_earns_the_author_nothing():
    eng = engagements((1, 42, pd.Timedelta(0)))
    purchases = buys(("0xtx", 99, pd.Timedelta(days=1), 100.0))

    assert attribute(eng, purchases, ATTRIBUTION_WINDOW_DAYS).empty


def test_buys_with_no_resolvable_fid_are_dropped():
    eng = engagements((1, 99, pd.Timedelta(0)))
    purchases = buys(("0xtx1", 99, pd.Timedelta(days=1), 100.0))
    anonymous = purchases.copy()
    anonymous["tx_hash"] = "0xtx2"
    anonymous["buyer_fid"] = pd.array([None], dtype="Int64")

    result = attribute(eng, pd.concat([purchases, anonymous], ignore_index=True), ATTRIBUTION_WINDOW_DAYS)

    assert list(result["tx_hash"]) == ["0xtx1"]


def test_empty_inputs_return_the_exact_output_contract():
    for eng, purchases in (
        (engagements(), buys(("0xtx", 99, pd.Timedelta(days=1), 100.0))),
        (engagements((1, 99, pd.Timedelta(0))), buys()),
        (engagements(), buys()),
    ):
        result = attribute(eng, purchases, ATTRIBUTION_WINDOW_DAYS)
        assert result.empty
        assert list(result.columns) == ATTRIBUTION_COLUMNS


def test_every_attribution_row_carries_the_full_output_contract():
    result = attribute(
        engagements((1, 99, pd.Timedelta(0))),
        buys(("0xtx", 99, pd.Timedelta(days=1), 100.0)),
        ATTRIBUTION_WINDOW_DAYS,
    )

    assert list(result.columns) == ATTRIBUTION_COLUMNS
    row = result.iloc[0]
    assert row["token_address"] == TOKEN
    assert row["chain_id"] == CHAIN
    assert row["buyer_address"] == "0xbuyer99"
    assert row["amount_usd"] == 100.0
    assert row["block_time"] == T0 + pd.Timedelta(days=1)


def test_the_window_length_is_a_parameter_not_a_constant():
    eng = engagements((1, 99, pd.Timedelta(0)))
    purchases = buys(("0xtx", 99, pd.Timedelta(days=2), 100.0))

    assert len(attribute(eng, purchases, 3)) == 1
    assert attribute(eng, purchases, 1).empty


# --- summarise -----------------------------------------------------------


def casts(*rows) -> pd.DataFrame:
    """rows: (author_fid, cast_hash[, token])."""
    return pd.DataFrame(
        [
            {
                "token_address": row[2] if len(row) > 2 else TOKEN,
                "chain_id": CHAIN,
                "cast_hash": row[1],
                "author_fid": row[0],
                "timestamp": T0.isoformat(),
                "matched_on": "ticker",
                "likes_count": 0,
                "recasts_count": 0,
            }
            for row in rows
        ],
        columns=TOKEN_CAST_COLUMNS,
    )


def test_summarise_rolls_attribution_up_per_author():
    eng = engagements(
        (1, 99, pd.Timedelta(0)),
        (2, 99, pd.Timedelta(0)),
        (1, 55, pd.Timedelta(0)),
    )
    purchases = buys(
        ("0xtx1", 99, pd.Timedelta(days=1), 300.0),
        ("0xtx2", 55, pd.Timedelta(days=1), 80.0),
    )
    attributions = attribute(eng, purchases, ATTRIBUTION_WINDOW_DAYS)

    summary = summarise(casts((1, "0xc1"), (1, "0xc2"), (2, "0xc3")), attributions)

    assert list(summary.columns) == SUMMARY_COLUMNS
    by_author = summary.set_index("author_fid")
    assert by_author.loc[1, "cast_count"] == 2
    assert by_author.loc[1, "unique_buyers_influenced"] == 2
    assert by_author.loc[1, "total_purchases"] == 2
    # totalPurchaseVolumeUsd deliberately double-counts a shared purchase...
    assert by_author.loc[1, "total_purchase_volume_usd"] == pytest.approx(380.0)
    # ...while attributedUsd is the conserved half share of tx1 plus all of tx2.
    assert by_author.loc[1, "attributed_usd"] == pytest.approx(230.0)
    assert by_author.loc[2, "attributed_usd"] == pytest.approx(150.0)
    assert summary["attributed_usd"].sum() == pytest.approx(380.0)


def test_summarise_keeps_authors_who_posted_and_moved_nobody():
    summary = summarise(casts((1, "0xc1"), (9, "0xc9")), pd.DataFrame(columns=ATTRIBUTION_COLUMNS))

    assert set(summary["author_fid"]) == {1, 9}
    assert set(summary["cast_count"]) == {1}
    assert list(summary["attributed_usd"]) == [0.0, 0.0]
    assert list(summary["unique_buyers_influenced"]) == [0, 0]
    assert summary["total_purchases"].dtype.kind == "i"


def test_summarise_of_no_casts_is_empty_with_the_right_columns():
    summary = summarise(casts(), pd.DataFrame(columns=ATTRIBUTION_COLUMNS))

    assert summary.empty
    assert list(summary.columns) == SUMMARY_COLUMNS


def test_summarise_is_sorted_by_attributed_usd_descending():
    eng = engagements((1, 99, pd.Timedelta(0)), (2, 55, pd.Timedelta(0)))
    purchases = buys(
        ("0xtx1", 99, pd.Timedelta(days=1), 10.0),
        ("0xtx2", 55, pd.Timedelta(days=1), 900.0),
    )
    attributions = attribute(eng, purchases, ATTRIBUTION_WINDOW_DAYS)

    summary = summarise(casts((1, "0xc1"), (2, "0xc2")), attributions)

    assert list(summary["author_fid"]) == [2, 1]


# --- buyer -> fid resolution --------------------------------------------


def test_resolve_buyer_fids_prefers_the_taker_over_tx_from():
    purchases = pd.DataFrame(
        [{"buyer_address": "0xtaker", "tx_from": "0xsender", "amount_usd": 1.0}]
    )

    resolved = resolve_buyer_fids(purchases, {"0xtaker": 11, "0xsender": 22})

    assert resolved.loc[0, "buyer_fid"] == 11
    assert resolved.loc[0, "buyer_address"] == "0xtaker"


def test_resolve_buyer_fids_falls_back_to_tx_from_and_rewrites_the_wallet():
    # On Clanker's v4 hook pools the taker is usually the router, not a human;
    # when only tx_from resolves, that EOA is the wallet the graph must key on.
    purchases = pd.DataFrame(
        [{"buyer_address": "0xrouter", "tx_from": "0xhuman", "amount_usd": 1.0}]
    )

    resolved = resolve_buyer_fids(purchases, {"0xhuman": 7})

    assert resolved.loc[0, "buyer_fid"] == 7
    assert resolved.loc[0, "buyer_address"] == "0xhuman"


def test_resolve_buyer_fids_with_an_empty_map_leaves_every_buy_anonymous():
    purchases = pd.DataFrame(
        [{"buyer_address": "0xa", "tx_from": "0xb", "amount_usd": 1.0}]
    )

    resolved = resolve_buyer_fids(purchases, {})

    assert resolved["buyer_fid"].isna().all()
    assert resolved.loc[0, "buyer_address"] == "0xa"


def test_resolve_buyer_fids_of_an_empty_frame_still_adds_the_column():
    resolved = resolve_buyer_fids(pd.DataFrame(columns=["buyer_address", "tx_from"]), {"0xa": 1})

    assert "buyer_fid" in resolved.columns
    assert resolved.empty


# --- cast classification -------------------------------------------------


def test_classify_cast_prefers_the_contract_address_over_the_ticker():
    cast = {"text": f"buying {TOKEN.upper()} aka $ARBSUMMER"}

    assert classify_cast(cast, TOKEN, "ARBSUMMER", CHAIN, allow_ticker=True) == "address"


def test_classify_cast_recognises_the_token_frame_url():
    for key in ("parent_url", "root_parent_url"):
        cast = {"text": "gm", key: f"eip155:{CHAIN}/erc20:{TOKEN}".upper()}
        assert classify_cast(cast, TOKEN, "ARBSUMMER", CHAIN, allow_ticker=True) == "parent_url"


def test_classify_cast_matches_a_ticker_only_when_tickers_are_allowed():
    cast = {"text": "long $arbsummer forever"}

    assert classify_cast(cast, TOKEN, "ARBSUMMER", CHAIN, allow_ticker=True) == "ticker"
    # A one- or two-character symbol matches half of Farcaster, so the caller
    # switches ticker matching off and only the address counts.
    assert classify_cast(cast, TOKEN, "ARBSUMMER", CHAIN, allow_ticker=False) is None


def test_classify_cast_needs_the_dollar_sign():
    cast = {"text": "arbsummer is a season, not a token"}

    assert classify_cast(cast, TOKEN, "ARBSUMMER", CHAIN, allow_ticker=True) is None


def test_classify_cast_of_a_frame_url_for_a_different_chain():
    cast = {"text": "gm", "parent_url": f"eip155:8453/erc20:{TOKEN}"}

    assert classify_cast(cast, TOKEN, "", CHAIN, allow_ticker=False) is None


def test_classify_cast_tolerates_a_cast_with_no_text():
    assert classify_cast({}, TOKEN, "ARBSUMMER", CHAIN, allow_ticker=True) is None


# --- wallet harvesting ---------------------------------------------------


def test_harvest_addresses_takes_every_eth_wallet_a_user_object_exposes(fixture_json):
    author = fixture_json("neynar_cast_search")["page1"]["result"]["casts"][0]["author"]

    harvested = harvest_addresses(author)

    assert harvested == [
        "0xbbbb000000000000000000000000000000000002",  # verified
        "0xbbbb000000000000000000000000000000000002",  # primary (same wallet)
        "0xaaaa000000000000000000000000000000000001",  # custody
    ]


def test_harvest_addresses_skips_solana_and_missing_wallets():
    user = {
        "verified_addresses": {
            "eth_addresses": [],
            "sol_addresses": ["9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin"],
            "primary": {"eth_address": None},
        }
    }

    # Solana addresses are base58 and case-sensitive; lowercasing them into the
    # 0x-keyed map would key two different wallets to one node.
    assert harvest_addresses(user) == []
    assert harvest_addresses({}) == []


# --- timestamp normalisation --------------------------------------------


def test_to_utc_parses_both_the_dune_and_neynar_spellings():
    parsed = _to_utc(pd.Series(["2026-08-09 19:56:12.000 UTC", "2026-08-09T19:56:12.000Z"]))

    assert str(parsed.dt.tz) == "UTC"
    assert parsed.iloc[0] == parsed.iloc[1] == pd.Timestamp("2026-08-09T19:56:12Z")


def test_to_utc_turns_unparseable_values_into_nat_rather_than_raising():
    parsed = _to_utc(pd.Series(["not a timestamp", "2026-01-01T00:00:00Z"]))

    assert parsed.isna().tolist() == [True, False]


def test_to_utc_of_an_empty_series_keeps_the_utc_dtype():
    parsed = _to_utc(pd.Series([], dtype="object"))

    assert parsed.empty
    assert str(parsed.dt.tz) == "UTC"


# --- Dune-facing paths (driven by the FakeDuneRunner in conftest) --------


def token_frame(*rows) -> pd.DataFrame:
    """rows: (token_address, chain_id[, symbol])."""
    return pd.DataFrame(
        [
            {
                "token_address": row[0],
                "chain_id": row[1],
                "platform": "clanker",
                "symbol": row[2] if len(row) > 2 else "SYM",
                "fid": None,
            }
            for row in rows
        ]
    )


DUNE_BUY_ROW = {
    "token_address": "0x7C9F4C87D911613FE9CA58B579F737911AAD2D43",
    "buyer_address": "0xAAAA000000000000000000000000000000000002",
    "tx_from": "0xBBBB000000000000000000000000000000000003",
    "tx_hash": "0xCC" + "0" * 62,
    "block_time": "2026-06-02 12:00:00.000 UTC",
    "amount_usd": "125.5",
    "token_amount": "1000",
}


def test_fetch_buys_normalises_what_dune_returns(fake_dune):
    runner = fake_dune(responses={"evangelism buys": pd.DataFrame([DUNE_BUY_ROW])})
    notes: list[str] = []

    result = fetch_buys(runner, token_frame((TOKEN, CHAIN)), "2026-06-01", None, notes)

    row = result.iloc[0]
    assert row["token_address"] == TOKEN  # lowercased for the graph key
    assert row["buyer_address"] == "0xaaaa000000000000000000000000000000000002"
    assert row["tx_from"] == "0xbbbb000000000000000000000000000000000003"
    assert row["amount_usd"] == 125.5  # coerced out of Dune's CSV strings
    assert row["block_time"] == pd.Timestamp("2026-06-02T12:00:00Z")
    assert row["chain_id"] == CHAIN
    assert notes == []


def test_fetch_buys_drops_rows_dune_could_not_timestamp(fake_dune):
    bad = dict(DUNE_BUY_ROW, block_time="", tx_hash="0xdd" + "0" * 62)
    runner = fake_dune(responses={"evangelism buys": pd.DataFrame([DUNE_BUY_ROW, bad])})

    result = fetch_buys(runner, token_frame((TOKEN, CHAIN)), "2026-06-01", None, [])

    # A buy with no time cannot be placed in an attribution window, so it is not
    # evidence of anything and must not reach the join.
    assert list(result["tx_hash"]) == [DUNE_BUY_ROW["tx_hash"].lower()]


def test_fetch_buys_degrades_to_an_empty_frame_when_dune_fails(fake_dune):
    runner = fake_dune(responses={"evangelism buys": DuneError("dex.trades: no such table")})
    notes: list[str] = []

    result = fetch_buys(runner, token_frame((TOKEN, CHAIN)), "2026-06-01", None, notes)

    assert result.empty
    assert list(result.columns) == [
        "token_address", "chain_id", "buyer_address", "tx_from", "tx_hash",
        "block_time", "amount_usd", "token_amount",
    ]
    assert notes == ["buys query failed for chain arbitrum: dex.trades: no such table"]


def test_fetch_buys_skips_a_chain_dune_has_no_name_for(fake_dune):
    runner = fake_dune()
    notes: list[str] = []

    result = fetch_buys(runner, token_frame((TOKEN, 999)), "2026-06-01", None, notes)

    assert result.empty
    assert runner.calls == []  # nothing was spent on a chain we cannot query
    assert notes == ["no Dune blockchain name for chain 999; buys not fetched"]


def test_fetch_buys_queries_each_chain_separately(fake_dune):
    runner = fake_dune(responses={"evangelism buys": pd.DataFrame([DUNE_BUY_ROW])})

    result = fetch_buys(
        runner, token_frame((TOKEN, CHAIN), (OTHER_TOKEN, 4663)), "2026-06-01", None, []
    )

    # One query per chain, in chain-id order (groupby sorts).
    assert runner.labels() == ["evangelism buys robinhood", "evangelism buys arbitrum"]
    assert sorted(result["chain_id"].unique()) == [4663, 42161]


def test_fetch_buys_passes_limit_through_as_a_row_cap(fake_dune):
    runner = fake_dune(responses={"evangelism buys": pd.DataFrame([DUNE_BUY_ROW])})

    fetch_buys(runner, token_frame((TOKEN, CHAIN)), "2026-06-01", 10, [])

    assert runner.calls[0]["limit"] == 10


def test_token_volumes_qualifies_from_lifetime_dex_volume(fake_dune):
    runner = fake_dune(
        responses={
            "evangelism token volume": pd.DataFrame(
                [{"token_address": TOKEN.upper(), "volume_usd": "75000"}]
            )
        }
    )

    volumes = token_volumes(runner, token_frame((TOKEN, CHAIN)), [])

    assert volumes.iloc[0]["token_address"] == TOKEN
    assert volumes.iloc[0]["volume_usd"] == 75000.0


def test_token_volumes_degrades_with_a_note_when_the_query_fails(fake_dune):
    runner = fake_dune(responses={"evangelism token volume": DuneError("boom")})
    notes: list[str] = []

    volumes = token_volumes(runner, token_frame((TOKEN, CHAIN)), notes)

    assert volumes.empty
    assert list(volumes.columns) == ["token_address", "chain_id", "volume_usd"]
    assert notes == ["volume query failed for chain arbitrum: boom"]


def args_for(**overrides):
    from lib.cli import base_parser

    args = base_parser("token_evangelists").parse_args([])
    defaults = {
        "tokens": None,
        "max_tokens": 25,
        "min_volume": 50_000.0,
        "chain_id": CHAIN,
    }
    for key, value in {**defaults, **overrides}.items():
        setattr(args, key, value)
    return args


def test_select_tokens_applies_the_volume_gate(fake_dune):
    registry = token_frame((TOKEN, CHAIN, "ARBSUMMER"), (OTHER_TOKEN, CHAIN, "ARB"))
    runner = fake_dune(
        responses={
            "evangelism token volume": pd.DataFrame(
                [
                    {"token_address": TOKEN, "volume_usd": 90_000},
                    {"token_address": OTHER_TOKEN, "volume_usd": 10_000},
                ]
            )
        }
    )

    selected = select_tokens(runner, registry, args_for(), [])

    assert list(selected["token_address"]) == [TOKEN]


def test_select_tokens_caps_at_max_tokens_and_says_so(fake_dune):
    registry = token_frame((TOKEN, CHAIN), (OTHER_TOKEN, CHAIN))
    runner = fake_dune(
        responses={
            "evangelism token volume": pd.DataFrame(
                [
                    {"token_address": TOKEN, "volume_usd": 90_000},
                    {"token_address": OTHER_TOKEN, "volume_usd": 900_000},
                ]
            )
        }
    )
    notes: list[str] = []

    selected = select_tokens(runner, registry, args_for(max_tokens=1), notes)

    # Highest volume first, so --max-tokens keeps the ones worth paying for.
    assert list(selected["token_address"]) == [OTHER_TOKEN]
    assert notes == ["1 qualifying tokens dropped by --max-tokens"]


def test_explicit_tokens_bypass_the_gate_and_spend_nothing_on_dune(fake_dune):
    runner = fake_dune()
    notes: list[str] = []

    selected = select_tokens(
        runner, token_frame((TOKEN, CHAIN, "ARBSUMMER")), args_for(tokens=f" {TOKEN.upper()} "), notes
    )

    assert list(selected["token_address"]) == [TOKEN]
    assert selected.iloc[0]["symbol"] == "ARBSUMMER"  # enriched from the registry
    assert runner.calls == []
    assert notes == ["token set supplied via --tokens; the volume gate was not applied"]


def test_an_explicit_token_absent_from_every_registry_is_still_trusted(fake_dune):
    selected = select_tokens(
        fake_dune(), token_frame((OTHER_TOKEN, CHAIN)), args_for(tokens=TOKEN), []
    )

    assert list(selected["token_address"]) == [TOKEN]
    assert selected.iloc[0]["symbol"] == ""  # no symbol, so no ticker search


def test_a_malformed_explicit_token_fails_at_render_time(fake_dune):
    from lib.sqlfmt import SqlLiteralError

    with pytest.raises(SqlLiteralError):
        select_tokens(fake_dune(), token_frame((TOKEN, CHAIN)), args_for(tokens="0xnope"), [])


def test_dry_run_plans_against_the_registry_head_without_executing(fake_dune):
    registry = token_frame((TOKEN, CHAIN), (OTHER_TOKEN, CHAIN))
    runner = fake_dune()
    args = args_for(max_tokens=1)
    args.dry_run = True
    notes: list[str] = []

    selected = select_tokens(runner, registry, args, notes)

    assert len(selected) == 1
    assert runner.calls == []
    assert notes == ["dry-run: token set is the registry head, not the volume-qualified set"]


def test_an_empty_registry_selects_nothing_and_costs_nothing(fake_dune):
    runner = fake_dune()

    selected = select_tokens(runner, token_frame(), args_for(), [])

    assert selected.empty
    assert runner.calls == []


def test_load_token_registry_merges_both_launchpads(layout):
    from lib.runs import RunWriter

    for data_type, address, symbol in (
        ("clanker_tokens", TOKEN, "ARBSUMMER"),
        ("bankr_tokens", OTHER_TOKEN, "ARB"),
    ):
        writer = RunWriter(data_type)
        writer.write(
            "tokens",
            pd.DataFrame(
                [{"token_address": address.upper(), "chain_id": CHAIN, "platform": "x",
                  "symbol": symbol, "fid": 1}]
            ),
        )
        writer.finish()

    registry, notes = load_token_registry()

    assert sorted(registry["token_address"]) == sorted([TOKEN, OTHER_TOKEN])
    assert set(registry["source_registry"]) == {"clanker_tokens", "bankr_tokens"}
    assert notes == []


def test_load_token_registry_survives_a_launchpad_that_never_ran(layout):
    from lib.runs import RunWriter

    writer = RunWriter("clanker_tokens")
    writer.write(
        "tokens",
        pd.DataFrame([{"token_address": TOKEN, "chain_id": CHAIN, "platform": "clanker",
                       "symbol": "ARBSUMMER", "fid": 1}]),
    )
    writer.finish()

    registry, notes = load_token_registry()

    assert list(registry["token_address"]) == [TOKEN]
    assert notes == ["no completed bankr_tokens run; contributed no candidates"]


def test_load_token_registry_with_no_runs_at_all(layout):
    registry, notes = load_token_registry()

    assert registry.empty
    assert list(registry.columns) == [
        "token_address", "chain_id", "platform", "symbol", "fid", "source_registry"
    ]
    assert len(notes) == 2
