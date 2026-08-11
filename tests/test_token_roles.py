"""Fee recipients as first-class launch wallets: the decode, the fids, the edges.

The premise the whole file defends is that a launch has two wallets and the
deploying one is often the less interesting of the two — a bot, a factory, or an
ERC-4337 smart account. So:

  * `sql/robinhood.py` must actually ask the chain who is paid, not guess;
  * both pipelines must resolve both wallets to fids without ever letting an
    identity lookup take the run down;
  * `cypher/tokens.py` must give the fee recipient the same edges as the
    deployer while keeping one address that holds both roles down to one edge.

The Cypher assertions are structural because the suite is offline. What they
pin down is the part that a live run cannot show you at a glance: that the
MERGE key is the node pair, and that the guards are what stop a second edge
from appearing beside the first.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

import lib.fid_resolver
import lib.neynar
import lib.wallet_fids
from cypher.tokens import BANKR_CYPHER, CLANKER_CYPHER, deploy_and_create
from pipelines import bankr_tokens, clanker_tokens
from sql import robinhood as rh

DEPLOYER = "0x1111111111111111111111111111111111111111"
FEE = "0x2222222222222222222222222222222222222222"
SINCE = datetime(2026, 7, 1, tzinfo=timezone.utc)


# --- the Dune decode -----------------------------------------------------


def test_the_registry_query_reads_both_beneficiary_emitters():
    sql = rh.tokens_by_factory_sql(None, SINCE)

    # Either emitter alone leaves a tenth of launches unattributed.
    assert rh.DOPPLER_V4_INITIALIZER in sql
    assert rh.STREAMABLE_FEES_LOCKER in sql
    assert rh.BENEFICIARIES_TOPIC0 in sql
    assert rh.LOCK_TOPIC0 in sql
    assert "fee_recipient_address" in sql
    assert "fee_recipient_source" in sql


def test_the_decode_only_looks_past_each_emitters_protocol_baseline():
    """An array no longer than the baseline is the protocol paying itself."""
    sql = rh.tokens_by_factory_sql(None, SINCE)

    assert f"{rh.INITIALIZER_PROTOCOL_ENTRIES} AS protocol_entries" in sql
    assert f"{rh.LOCKER_PROTOCOL_ENTRIES} AS protocol_entries" in sql
    assert "length(e.data) >= 64 + (e.protocol_entries + 1) * 64" in sql
    # Largest share wins, and the tie-break keeps the run repeatable.
    assert "ORDER BY priority, shares DESC, beneficiary" in sql


def test_the_decode_refuses_an_array_whose_layout_moved():
    sql = rh.tokens_by_factory_sql(None, SINCE)

    # The offset word pins the "one dynamic array, nothing else" encoding the
    # byte positions below it assume.
    assert rh._ARRAY_HEAD_OFFSET in sql
    assert "(length(e.data) - 64) % 64 = 0" in sql


def test_the_cheap_path_still_declares_the_column_it_cannot_fill():
    """resolve_launcher=False has no log ordering, so it has no fee recipient."""
    sql = rh.tokens_by_factory_sql(None, SINCE, resolve_launcher=False)

    assert "cast(NULL AS varchar)             AS fee_recipient_address" in sql
    assert rh.STREAMABLE_FEES_LOCKER not in sql


# --- the CSV contract carries it ----------------------------------------


def token_frame(columns: list[str], *rows: dict) -> pd.DataFrame:
    """A CSV-shaped frame: every contracted column, null unless the row names it."""
    return pd.DataFrame(
        [{column: row.get(column) for column in columns} for row in rows],
        columns=columns,
    )


def test_the_fee_recipient_survives_the_merge_of_the_two_sources():
    columns = bankr_tokens.TOKEN_COLUMNS
    dune = token_frame(
        columns,
        {"token_address": "0xaaa", "deployer_address": DEPLOYER, "fee_recipient_address": FEE},
    )
    api = token_frame(
        columns,
        {"token_address": "0xaaa", "name": "Token", "fee_recipient_address": FEE},
        {"token_address": "0xbbb", "fee_recipient_address": DEPLOYER},
    )

    merged = bankr_tokens.merge_registries(dune, api).set_index("token_address")

    # Dune wins on the chain facts, the API fills what Dune has not indexed.
    assert merged.loc["0xaaa", "fee_recipient_address"] == FEE
    assert merged.loc["0xaaa", "name"] == "Token"
    assert merged.loc["0xbbb", "fee_recipient_address"] == DEPLOYER


# --- fid resolution ------------------------------------------------------


@pytest.fixture
def stub_fids(monkeypatch):
    """Answer the local map with `local` and Neynar with `remote`."""

    def install(local: dict, remote: dict | None = None, local_error: Exception | None = None):
        asked: list[list[str]] = []

        def wallet_to_fid(run_id=None):
            if local_error is not None:
                raise local_error
            return dict(local)

        def addresses_to_fids(client, addresses, batch=100):
            addresses = list(addresses)
            asked.append(addresses)
            return {a: (remote or {})[a] for a in addresses if a in (remote or {})}

        monkeypatch.setattr(lib.fid_resolver, "wallet_to_fid", wallet_to_fid)
        monkeypatch.setattr(lib.fid_resolver, "addresses_to_fids", addresses_to_fids)
        monkeypatch.setattr(lib.neynar, "NeynarClient", lambda *a, **k: object())
        return asked

    return install


def bankr_frame(*extra: dict) -> pd.DataFrame:
    return token_frame(
        bankr_tokens.TOKEN_COLUMNS,
        {"token_address": "0xaaa", "deployer_address": DEPLOYER, "fee_recipient_address": FEE},
        *extra,
    )


def test_both_launch_wallets_are_resolved_and_land_in_their_own_columns(stub_fids):
    stub_fids({DEPLOYER: 11}, {FEE: 22})
    notes: list[str] = []

    out = bankr_tokens.attach_fids(bankr_frame(), notes, dry_run=False)

    assert out.loc[0, "fid"] == 11
    assert out.loc[0, "fee_recipient_fid"] == 22


def test_only_the_distinct_union_of_the_two_columns_reaches_neynar(stub_fids):
    """67k tokens must not become 134k lookups, nor even 2 for one address."""
    asked = stub_fids({}, {})
    frame = bankr_frame(
        {
            "token_address": "0xbbb",
            "deployer_address": DEPLOYER,
            "fee_recipient_address": DEPLOYER.upper(),
        }
    )

    bankr_tokens.attach_fids(frame, [], dry_run=False)

    assert asked == [[DEPLOYER, FEE]]


def test_a_missing_linked_wallets_run_costs_the_local_pass_not_the_run(stub_fids):
    """The backfill may not have produced a run yet; that is a normal state."""
    stub_fids({}, {FEE: 22}, local_error=FileNotFoundError("no completed runs"))
    notes: list[str] = []

    out = bankr_tokens.attach_fids(bankr_frame(), notes, dry_run=False)

    assert out.loc[0, "fee_recipient_fid"] == 22
    assert out.loc[0, "fid"] is None


def test_a_neynar_outage_leaves_the_columns_null_and_says_so(monkeypatch):
    def boom(*args, **kwargs):
        raise RuntimeError("neynar is down")

    monkeypatch.setattr(lib.fid_resolver, "wallet_to_fid", lambda run_id=None: {})
    monkeypatch.setattr(lib.neynar, "NeynarClient", boom)
    notes: list[str] = []

    out = bankr_tokens.attach_fids(bankr_frame(), notes, dry_run=False)

    assert out.loc[0, "fid"] is None
    assert out.loc[0, "fee_recipient_fid"] is None
    assert any("neynar" in note for note in notes)


def test_the_neynar_top_up_is_bounded_and_the_skip_is_recorded(stub_fids):
    asked = stub_fids({}, {})
    notes: list[str] = []

    lib.wallet_fids.resolve_wallet_fids(
        [DEPLOYER, FEE], notes, what="test wallets", max_neynar=1
    )

    assert asked == [[DEPLOYER]]
    assert any("capped" in note for note in notes)


def test_clanker_resolves_the_admin_without_touching_the_recorded_fid(stub_fids):
    """Clanker names the requesting account itself; that beats a wallet lookup."""
    stub_fids({}, {FEE: 22})
    frame = token_frame(
        clanker_tokens.TOKEN_COLUMNS,
        {
            "token_address": "0xaaa",
            "deployer_address": DEPLOYER,
            "admin_address": FEE,
            "fid": 7,
        },
    )

    out = clanker_tokens.attach_admin_fids(frame, [], dry_run=False)

    assert out.loc[0, "fid"] == 7
    assert out.loc[0, "fee_recipient_fid"] == 22


def test_distinct_addresses_folds_case_and_drops_non_addresses():
    column = pd.Series([DEPLOYER.upper(), DEPLOYER, "<nil>", None, ""])

    assert lib.wallet_fids.distinct_addresses(column, pd.Series([FEE])) == [DEPLOYER, FEE]


# --- the graph edges -----------------------------------------------------


@pytest.mark.parametrize(
    "cypher, fee_column, role",
    [
        (BANKR_CYPHER, "fee_recipient_address", "fee_recipient"),
        (CLANKER_CYPHER, "admin_address", "admin"),
    ],
)
def test_the_fee_recipient_gets_the_same_edges_as_the_deployer(cypher, fee_column, role):
    assert f"MERGE (w:Wallet {{address: toLower(row.{fee_column})}})" in cypher
    assert cypher.count("MERGE (w)-[d:DEPLOYED]->(t)") == 2
    assert cypher.count("MERGE (a)-[c:CREATED]->(t)") == 2
    assert "SET d.role = 'deployer'" in cypher
    assert f"SET d.role = '{role}'" in cypher
    assert f"SET c.role = '{role}'" in cypher


@pytest.mark.parametrize("cypher", [BANKR_CYPHER, CLANKER_CYPHER])
def test_the_merge_key_stays_the_node_pair_so_a_re_run_adopts_its_own_edges(cypher):
    """`role` in the MERGE key would duplicate every edge written before it."""
    assert "[d:DEPLOYED {" not in cypher
    assert "[c:CREATED {" not in cypher


def test_one_address_holding_both_roles_writes_a_single_deployer_edge():
    """The guard, not the MERGE key, is what keeps the second edge from existing."""
    tail = deploy_and_create("fee_recipient_address", "fee_recipient_fid", "fee_recipient")

    assert (
        "WHEN row.fee_recipient_address IS NULL\n"
        "             OR toLower(row.fee_recipient_address)\n"
        "                = toLower(coalesce(row.deployer_address, '')) THEN []" in tail
    )
    # Same idea one level up: one person behind both wallets is still one edge.
    assert "WHEN row.fee_recipient_fid IS NULL\n             OR row.fee_recipient_fid = row.fid" in tail


def test_a_null_deployer_still_lets_the_fee_recipient_through():
    """coalesce, not a bare comparison: `x = null` would swallow the edge."""
    tail = deploy_and_create("fee_recipient_address", "fee_recipient_fid", "fee_recipient")

    assert "toLower(coalesce(row.deployer_address, ''))" in tail
