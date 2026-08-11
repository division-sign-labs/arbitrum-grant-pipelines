"""The CSV contract, written down once and checked from both sides.

`data/<data_type>/<run_ts>/<name>.csv` is the seam between the pipelines and
ingestion, and the two halves are written by different code that never imports
each other. So the contract is transcribed here verbatim, and then:

  * every pipeline's column constant must equal its contract row, in order —
    ingestion reads these by name and the graph keys on them;
  * every ingestion step's required columns must be a subset of the contract row
    for the CSV it reads, so an ingest can never demand a column no pipeline
    writes.

A rename on either side fails here, naming the file and the column, instead of
producing a run that loads cleanly and writes nulls into the graph.
"""

from __future__ import annotations

import importlib

import pytest

from tests.test_ingestion import INGEST_MODULES, steps_for

CSV_CONTRACT: dict[str, list[str]] = {
    "linked_wallets/wallets": ["fid", "address", "protocol", "is_primary", "source"],
    "linked_wallets/accounts": [
        "fid", "username", "display_name", "neynar_score", "follower_count",
        "following_count", "custody_address", "registered_at",
    ],
    "contract_deployers/deployments": [
        "fid", "deployer_address", "contract_address", "chain_id", "deployed_at",
        "tx_hash", "deploy_method",
    ],
    "contract_deployers/deployer_activity": [
        "fid", "address", "chain_id", "tx_count", "first_tx_at", "last_tx_at",
    ],
    "miniapp_builders_activity/builder_wallets": ["fid", "address"],
    "miniapp_builders_activity/builder_activity": [
        "fid", "address", "chain_id", "tx_count", "first_tx_at", "last_tx_at",
    ],
    "brand_engagement/brand_engagements": [
        "engager_fid", "brand_fid", "engagement_type", "cast_hash",
        "target_cast_hash", "timestamp",
    ],
    "brand_engagement/brand_engagement_summary": [
        "engager_fid", "brand_fid", "replies", "likes", "recasts", "mentions",
        "weighted_score", "window_start", "window_end",
    ],
    "brand_engagement/channel_casts": [
        "cast_hash", "author_fid", "channel_id", "timestamp", "parent_hash",
        "likes_count", "recasts_count", "replies_count", "text_length",
    ],
    "brand_engagement/channel_engagement_summary": [
        "fid", "channel_id", "casts_posted", "reactions_received",
        "replies_received", "reactions_given", "first_cast_at", "last_cast_at",
    ],
    "clanker_tokens/tokens": [
        "token_address", "chain_id", "platform", "deployer_address",
        "admin_address", "fid", "fee_recipient_fid", "username", "name", "symbol",
        "deployed_at", "tx_hash", "pool_address", "paired_token", "token_type",
        "starting_market_cap_usd", "price_usd", "market_cap_usd", "volume_24h_usd",
    ],
    "bankr_tokens/tokens": [
        "token_address", "chain_id", "platform", "deployer_address",
        "fee_recipient_address", "fid", "fee_recipient_fid", "name", "symbol",
        "deployed_at", "tx_hash", "pool_address", "launch_type", "source",
    ],
    "bankr_tokens/token_volume": [
        "token_address", "chain_id", "day", "swap_count", "volume_native", "volume_usd",
    ],
    "token_buyers/buys": [
        "fid", "buyer_address", "token_address", "chain_id", "platform",
        "amount_usd", "token_amount", "block_time", "tx_hash",
    ],
    "token_evangelists/token_casts": [
        "token_address", "chain_id", "cast_hash", "author_fid", "timestamp",
        "matched_on", "likes_count", "recasts_count",
    ],
    "token_evangelists/attributions": [
        "token_address", "chain_id", "author_fid", "buyer_fid", "buyer_address",
        "tx_hash", "amount_usd", "block_time", "attributed_usd", "n_influencers",
    ],
    "token_evangelists/evangelist_summary": [
        "token_address", "chain_id", "author_fid", "cast_count",
        "unique_buyers_influenced", "total_purchases",
        "total_purchase_volume_usd", "attributed_usd",
    ],
    "popular_tokens/trades": [
        "fid", "address", "token_address", "chain_id", "side", "amount_usd",
        "token_amount", "block_time", "tx_hash",
    ],
    "popular_tokens/holdings": [
        "fid", "address", "token_address", "chain_id", "balance", "balance_raw",
        "last_activity_at",
    ],
    "popular_tokens/vault_deposits": [
        "fid", "address", "vault_address", "chain_id", "assets", "assets_raw",
        "shares_raw", "block_time", "tx_hash",
    ],
    "popular_tokens/lp_events": [
        "fid", "address", "pool_address", "token_address", "chain_id", "event",
        "amount0", "amount1", "block_time", "tx_hash",
    ],
    "arb_cohort/cohort": ["address", "fid", "sources", "priority", "neynar_score"],
    "hyperliquid_activity/hl_activity": [
        "address", "fid", "has_hl_activity", "cum_volume_usd", "first_activity_at",
        "ledger_event_count", "checked_at",
    ],
}

# The module constant each pipeline writes its CSV from. linked_wallets builds
# both of its frames in lib.fid_resolver, which is where its columns live.
PIPELINE_COLUMN_CONSTANTS: dict[str, tuple[str, str]] = {
    "linked_wallets/wallets": ("lib.fid_resolver", "WALLET_COLUMNS"),
    "linked_wallets/accounts": ("lib.fid_resolver", "ACCOUNT_COLUMNS"),
    "contract_deployers/deployments": ("pipelines.contract_deployers", "DEPLOYMENTS_CSV_COLUMNS"),
    "contract_deployers/deployer_activity": ("pipelines.contract_deployers", "ACTIVITY_CSV_COLUMNS"),
    "miniapp_builders_activity/builder_wallets": ("pipelines.miniapp_builders", "WALLETS_CSV_COLUMNS"),
    "miniapp_builders_activity/builder_activity": ("pipelines.miniapp_builders", "ACTIVITY_CSV_COLUMNS"),
    "brand_engagement/brand_engagements": ("pipelines.brand_engagement", "ENGAGEMENT_COLUMNS"),
    "brand_engagement/brand_engagement_summary": ("pipelines.brand_engagement", "ENGAGEMENT_SUMMARY_COLUMNS"),
    "brand_engagement/channel_casts": ("pipelines.brand_engagement", "CHANNEL_CAST_COLUMNS"),
    "brand_engagement/channel_engagement_summary": ("pipelines.brand_engagement", "CHANNEL_SUMMARY_COLUMNS"),
    "clanker_tokens/tokens": ("pipelines.clanker_tokens", "TOKEN_COLUMNS"),
    "bankr_tokens/tokens": ("pipelines.bankr_tokens", "TOKEN_COLUMNS"),
    "bankr_tokens/token_volume": ("pipelines.bankr_tokens", "VOLUME_COLUMNS"),
    "token_buyers/buys": ("pipelines.token_buyers", "BUYS_COLUMNS"),
    "token_evangelists/token_casts": ("pipelines.token_evangelists", "TOKEN_CAST_COLUMNS"),
    "token_evangelists/attributions": ("pipelines.token_evangelists", "ATTRIBUTION_COLUMNS"),
    "token_evangelists/evangelist_summary": ("pipelines.token_evangelists", "SUMMARY_COLUMNS"),
    "popular_tokens/trades": ("pipelines.popular_tokens", "TRADES_COLUMNS"),
    "popular_tokens/holdings": ("pipelines.popular_tokens", "HOLDINGS_COLUMNS"),
    "popular_tokens/vault_deposits": ("pipelines.popular_tokens", "VAULT_COLUMNS"),
    "popular_tokens/lp_events": ("pipelines.popular_tokens", "LP_COLUMNS"),
    "arb_cohort/cohort": ("pipelines.arb_cohort", "COLUMNS"),
    "hyperliquid_activity/hl_activity": ("pipelines.hyperliquid_activity", "COLUMNS"),
}


def test_every_contract_entry_has_a_pipeline_constant():
    assert set(PIPELINE_COLUMN_CONSTANTS) == set(CSV_CONTRACT)


@pytest.mark.parametrize("key", sorted(CSV_CONTRACT))
def test_the_pipeline_writes_exactly_the_contracted_columns_in_order(key):
    module_name, attribute = PIPELINE_COLUMN_CONSTANTS[key]
    module = importlib.import_module(module_name)

    assert list(getattr(module, attribute)) == CSV_CONTRACT[key], (
        f"{module_name}.{attribute} no longer matches the CSV contract for {key}.csv"
    )


@pytest.mark.parametrize("module_name, data_type, source", INGEST_MODULES)
def test_ingestion_only_requires_columns_the_contract_promises(module_name, data_type, source):
    for step in steps_for(module_name, source):
        key = f"{data_type}/{step.csv}"
        assert key in CSV_CONTRACT, f"{module_name} reads {key}.csv, which is not in the contract"

        surplus = set(step.columns) - set(CSV_CONTRACT[key])
        assert not surplus, (
            f"{module_name}/{step.label} requires column(s) {sorted(surplus)} that no "
            f"pipeline writes to {key}.csv"
        )


def test_every_contracted_csv_has_an_ingestion_step_or_a_documented_reason():
    consumed = {
        f"{data_type}/{step.csv}"
        for module_name, data_type, source in INGEST_MODULES
        for step in steps_for(module_name, source)
    }

    # arb_cohort is a driver for the Hyperliquid crawl, not a graph input: its
    # wallets and fids are already in the graph from the runs it aggregates, so
    # run_all marks its ingestion optional.
    assert set(CSV_CONTRACT) - consumed == {"arb_cohort/cohort"}


def test_the_graph_merge_keys_are_all_contracted_columns():
    """Every MERGE key the constraints enforce has to arrive from a CSV column."""
    from ingestion.constraints import CONSTRAINTS

    csv_columns = {column for columns in CSV_CONTRACT.values() for column in columns}
    # The Cypher-side spelling of each constrained property, and the CSV column
    # it is written from.
    sources = {
        "fid": "fid",
        "address": "address",
        "chainId": "chain_id",
        "channelId": "channel_id",
        "name": "name",  # Platform{name} is a literal, but `name` is contracted too
    }

    for spec in CONSTRAINTS:
        for prop in spec.properties:
            assert prop in sources, f"unmapped constraint property {spec.label}.{prop}"
            assert sources[prop] in csv_columns or prop == "address"
