"""Dune SQL for the popular-Arbitrum-token index (config.tokens.INDEX_TOKENS).

Four independent legs, one builder each, because they answer four different
questions about the same seven addresses and each has a different cost shape:

  index_trades_sql    dex.trades          — who swapped in or out of the index
  index_holdings_sql  erc20 transfers     — who still holds it (net balance)
  vault_deposits_sql  arbitrum.logs       — who deposited into the Gauntlet vaults
  lp_events_sql       uniswap v3 decoded  — who provided liquidity against it

Source choices, all verified live against Dune (2026-08):

* dex.trades is preferred over the per-DEX decoded tables because it already
  normalises every aggregator/router on Arbitrum into one row shape with a USD
  price attached. Its `block_time` is `timestamp with time zone` while the raw
  `arbitrum.*` tables are plain timestamps, so every literal compared against it
  is cast explicitly. `block_date` is carried alongside `block_time` purely for
  partition pruning.

* Balances are reconstructed from erc20_arbitrum.evt_transfer rather than read
  from a balances table, because Dune's balance tables are not available with
  this key. `value` is a uint256; summing it signed requires an explicit
  int256 cast, which DuneSQL supports (verified).

* The Gauntlet vaults are MetaMorpho ERC-4626s with no decoded schema on Dune,
  so the Deposit event is decoded by hand out of arbitrum.logs. Verified layout:
  topic0 = ERC4626_DEPOSIT_TOPIC0, topic1 = sender, topic2 = owner, and
  data is exactly 64 bytes = abi.encode(assets, shares).

* For liquidity the decoded `uniswap_v3_arbitrum` schema is used. IMPORTANT
  (verified the hard way): in `uniswap_v3_arbitrum.pools` the `contract_address`
  column is the *factory* (0x1f98431c…f984 for every row) and `id` is the pool
  address. Joining pool events on `contract_address` silently returns zero rows.
  `base_liquidity_events` was rejected as the source because it carries no pool
  address column at all, and the CSV contract requires one.

Every interpolated value goes through lib.sqlfmt so a malformed seed fails at
render time instead of producing a query that quietly matches nothing.
"""

from __future__ import annotations

from datetime import datetime
from typing import Iterable, Sequence

from config.settings import CHAIN_ARBITRUM, MIN_BUY_USD
from config.tokens import ERC4626_DEPOSIT_TOPIC0, by_address
from lib.sqlfmt import (
    SqlLiteralError,
    address,
    address_list,
    hash_literal,
    timestamp,
)

# Dune's `blockchain` value for Arbitrum One in the cross-chain tables.
ARBITRUM = "arbitrum"

# Fallback when a token is neither in config.tokens nor in tokens.erc20.
DEFAULT_DECIMALS = 18

__all__ = [
    "index_trades_sql",
    "index_holdings_sql",
    "vault_deposits_sql",
    "lp_events_sql",
    "TRADES_RAW_COLUMNS",
    "HOLDINGS_RAW_COLUMNS",
    "VAULT_RAW_COLUMNS",
    "LP_RAW_COLUMNS",
    "ARBITRUM",
]

# The column sets each builder promises. The pipeline uses these to write a
# correctly-shaped empty CSV when a leg degrades.
TRADES_RAW_COLUMNS = [
    "taker_address",
    "tx_from_address",
    "token_address",
    "side",
    "amount_usd",
    "token_amount",
    "block_time",
    "tx_hash",
]
HOLDINGS_RAW_COLUMNS = [
    "address",
    "token_address",
    "balance",
    "balance_raw",
    "last_activity_at",
]
VAULT_RAW_COLUMNS = [
    "sender_address",
    "owner_address",
    "vault_address",
    "assets_raw",
    "shares_raw",
    "block_time",
    "tx_hash",
]
LP_RAW_COLUMNS = [
    "lp_address",
    "pool_address",
    "token0_address",
    "token1_address",
    "event",
    "amount0",
    "amount1",
    "block_time",
    "tx_hash",
]


# -- literal helpers -------------------------------------------------------


def _tz_timestamp(value) -> str:
    """A timestamp literal comparable against dex.trades' tz-aware block_time."""
    return f"CAST({timestamp(value)} AS timestamp with time zone)"


def _date_literal(value) -> str:
    """`DATE '2025-01-01'` for the block_date partition column."""
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        raise SqlLiteralError(f"not a datetime: {value!r}")
    return f"DATE '{value.strftime('%Y-%m-%d')}'"


def _topic_address_list(values: Iterable[str]) -> str:
    """Addresses as 32-byte left-padded topic literals.

    Comparing a padded literal against the whole `topicN` column is a plain
    varbinary equality the engine can use directly, which is materially cheaper
    than calling bytearray_substring on every row before comparing.
    """
    rendered = [hash_literal("0x" + "00" * 12 + address(v)[2:]) for v in values]
    if not rendered:
        raise SqlLiteralError("empty address list")
    return ", ".join(rendered)


def _decimals_divisor(column: str, addresses: Sequence[str]) -> str:
    """A CASE that maps a token address column to its 10^decimals divisor.

    Decimals come from config.tokens, not from a tokens.erc20 join, because the
    index is fixed and hand-verified — one fewer join on the most expensive leg.
    """
    branches = []
    for raw in addresses:
        token = by_address(raw)
        decimals = int(token["decimals"]) if token else DEFAULT_DECIMALS
        branches.append(f"        WHEN {column} = {address(raw)} THEN 1e{decimals}")
    if not branches:
        raise SqlLiteralError("empty address list")
    body = "\n".join(branches)
    return f"CASE\n{body}\n        ELSE 1e{DEFAULT_DECIMALS}\n    END"


def _hex(column: str) -> str:
    """to_hex() drops the 0x prefix, so every address/hash output re-adds it."""
    return f"'0x' || lower(to_hex({column}))"


# -- 1. trades -------------------------------------------------------------


def index_trades_sql(
    token_addresses: Sequence[str],
    since,
    until=None,
    min_amount_usd: float = MIN_BUY_USD,
    trader_addresses: Sequence[str] | None = None,
) -> str:
    """Swaps into or out of the index on Arbitrum, one row per index-token side.

    A swap of an index token for a non-index token yields one row; an index-to-
    index swap (ARB -> PENDLE) yields two, one 'buy' and one 'sell'. That is what
    the UNNEST does — it fans each trade out over its two sides and keeps only the
    sides that are in the index, in a single pass over dex.trades. The obvious
    alternative (two SELECTs UNION ALL'd) scans the table twice.

    `taker` on Arbitrum is very often an aggregator router rather than a person,
    so `tx_from` is emitted alongside it and the caller decides which one matches
    a Farcaster wallet.

    `min_amount_usd` is the cost lever: ARB alone is ~1M dex.trades rows a month,
    and the $50 floor (settings.MIN_BUY_USD) halves that without touching any
    trade a human would call meaningful.
    """
    tokens = address_list(token_addresses)
    filters = [
        f"blockchain = '{ARBITRUM}'",
        f"block_date >= {_date_literal(since)}",
        f"block_time >= {_tz_timestamp(since)}",
        f"(token_bought_address IN ({tokens}) OR token_sold_address IN ({tokens}))",
    ]
    if until is not None:
        filters.append(f"block_date <= {_date_literal(until)}")
        filters.append(f"block_time < {_tz_timestamp(until)}")
    if min_amount_usd:
        filters.append(f"amount_usd >= {float(min_amount_usd)}")
    if trader_addresses:
        traders = address_list(trader_addresses)
        filters.append(f"(taker IN ({traders}) OR tx_from IN ({traders}))")
    where = "\n      AND ".join(filters)

    return f"""
WITH src AS (
    SELECT
        taker,
        tx_from,
        token_bought_address,
        token_sold_address,
        token_bought_amount,
        token_sold_amount,
        amount_usd,
        block_time,
        tx_hash
    FROM dex.trades
    WHERE {where}
)
SELECT
    {_hex('s.taker')} AS taker_address,
    {_hex('s.tx_from')} AS tx_from_address,
    {_hex('x.token_address')} AS token_address,
    x.side,
    s.amount_usd,
    x.token_amount,
    s.block_time,
    {_hex('s.tx_hash')} AS tx_hash
FROM src s
CROSS JOIN UNNEST(ARRAY[
    ROW('buy', s.token_bought_address, s.token_bought_amount),
    ROW('sell', s.token_sold_address, s.token_sold_amount)
]) AS x(side, token_address, token_amount)
WHERE x.token_address IN ({tokens})
"""


# -- 2. holdings -----------------------------------------------------------


def index_holdings_sql(
    token_addresses: Sequence[str],
    holder_addresses: Sequence[str] | None,
    since,
    min_balance_raw: int = 0,
) -> str:
    """Net balance per (holder, index token), from the full transfer history.

    A balance is cumulative, so the scan deliberately has no lower bound on
    block time — netting only the last N months would report a flow, not a
    balance. `since` instead becomes a HAVING on the *last* movement: a pair
    whose balance cannot have changed since the previous run is dropped, which
    is exactly right for the HOLDS edge (a singleton the ingestion overwrites).

    `holder_addresses` is what makes this affordable. Unrestricted, the group-by
    emits one row per distinct ARB holder ever — millions. Callers chunk the
    Farcaster wallet set with lib.sqlfmt.chunked and run this once per chunk.
    Passing None runs it unrestricted and is only sensible when the wallet set is
    so large that the chunk count would exceed the number of distinct holders.

    `min_balance_raw` defaults to "any positive balance", which is the honest
    reading of the data but does surface wei-sized residuals left behind by
    routers. Raising it is the caller's policy call, not this module's.
    """
    tokens = address_list(token_addresses)
    divisor = _decimals_divisor("contract_address", token_addresses)

    if holder_addresses:
        holders = address_list(holder_addresses)
        in_filter = f'\n      AND "to" IN ({holders})'
        out_filter = f'\n      AND "from" IN ({holders})'
    else:
        in_filter = ""
        out_filter = ""

    return f"""
WITH moves AS (
    SELECT
        contract_address,
        "to" AS holder,
        CAST(value AS int256) AS delta,
        evt_block_time
    FROM erc20_arbitrum.evt_transfer
    WHERE contract_address IN ({tokens}){in_filter}
    UNION ALL
    SELECT
        contract_address,
        "from" AS holder,
        -CAST(value AS int256) AS delta,
        evt_block_time
    FROM erc20_arbitrum.evt_transfer
    WHERE contract_address IN ({tokens}){out_filter}
)
SELECT
    {_hex('holder')} AS address,
    {_hex('contract_address')} AS token_address,
    CAST(SUM(delta) AS double) / ({divisor}) AS balance,
    CAST(SUM(delta) AS varchar) AS balance_raw,
    max(evt_block_time) AS last_activity_at
FROM moves
GROUP BY holder, contract_address
HAVING SUM(delta) > int256 '{int(min_balance_raw)}'
   AND max(evt_block_time) >= {timestamp(since)}
"""


# -- 3. ERC-4626 vault deposits -------------------------------------------


def vault_deposits_sql(
    vault_addresses: Sequence[str],
    depositor_addresses: Sequence[str] | None = None,
    since=None,
) -> str:
    """Deposit(sender, owner, assets, shares) out of raw arbitrum.logs.

    The Gauntlet MetaMorpho vaults have no decoded schema on Dune, so the event
    is unpacked by hand. Verified against real rows: data is exactly 64 bytes,
    assets in bytes 1-32 and shares in bytes 33-64, and both indexed addresses
    sit in the low 20 bytes of their topic.

    Assets are left raw here rather than scaled, because the divisor is the
    *underlying* asset's decimals (USDC 6, WETH 18), not the share token's — the
    caller scales using config.tokens' asset_decimals.

    `depositor_addresses` is optional and usually unnecessary: four contract
    addresses plus one topic0 is already selective enough that the whole history
    is ~39k rows (verified), which is cheaper to intersect locally than to chunk.
    """
    vaults = address_list(vault_addresses)
    filters = [
        f"contract_address IN ({vaults})",
        f"topic0 = {hash_literal(ERC4626_DEPOSIT_TOPIC0)}",
        # Guards the substring decode against a same-topic0 event with a
        # different payload shape sneaking in from an unrelated contract.
        "length(data) >= 64",
    ]
    if since is not None:
        filters.append(f"block_time >= {timestamp(since)}")
    if depositor_addresses:
        topics = _topic_address_list(depositor_addresses)
        filters.append(f"(topic1 IN ({topics}) OR topic2 IN ({topics}))")
    where = "\n      AND ".join(filters)

    sender = "bytearray_substring(topic1, 13, 20)"
    owner = "bytearray_substring(topic2, 13, 20)"

    return f"""
SELECT
    {_hex(sender)} AS sender_address,
    {_hex(owner)} AS owner_address,
    {_hex('contract_address')} AS vault_address,
    CAST(bytearray_to_uint256(bytearray_substring(data, 1, 32)) AS varchar) AS assets_raw,
    CAST(bytearray_to_uint256(bytearray_substring(data, 33, 32)) AS varchar) AS shares_raw,
    block_time,
    {_hex('tx_hash')} AS tx_hash
FROM arbitrum.logs
WHERE {where}
"""


# -- 4. Uniswap v3 liquidity ----------------------------------------------


def lp_events_sql(
    token_addresses: Sequence[str],
    lp_addresses: Sequence[str] | None = None,
    since=None,
) -> str:
    """Uniswap v3 Mint/Burn on pools that hold an index token.

    The LP wallet is `evt_tx_from`, not the event's `owner`: on v3 the owner is
    almost always the NonfungiblePositionManager, so keying on it would attribute
    every position on Arbitrum to one contract.

    Pool token decimals come from tokens.erc20 (aggregated, since that table can
    carry more than one row per contract) so the counterparty side of an
    ARB/USDC pool is scaled correctly and not assumed to be 18.
    """
    tokens = address_list(token_addresses)

    event_filters = []
    if since is not None:
        event_filters.append(f"evt_block_time >= {timestamp(since)}")
    if lp_addresses:
        event_filters.append(f"evt_tx_from IN ({address_list(lp_addresses)})")
    event_where = (
        "\n    WHERE " + "\n      AND ".join(event_filters) if event_filters else ""
    )

    return f"""
WITH idx_pools AS (
    SELECT
        p.id AS pool,
        p.token0,
        p.token1,
        COALESCE(max(t0.decimals), {DEFAULT_DECIMALS}) AS decimals0,
        COALESCE(max(t1.decimals), {DEFAULT_DECIMALS}) AS decimals1
    FROM uniswap_v3_arbitrum.pools p
    LEFT JOIN tokens.erc20 t0
           ON t0.blockchain = '{ARBITRUM}' AND t0.contract_address = p.token0
    LEFT JOIN tokens.erc20 t1
           ON t1.blockchain = '{ARBITRUM}' AND t1.contract_address = p.token1
    WHERE p.token0 IN ({tokens}) OR p.token1 IN ({tokens})
    GROUP BY p.id, p.token0, p.token1
),
events AS (
    SELECT contract_address, evt_tx_from, evt_tx_hash, evt_block_time,
           amount0, amount1, 'mint' AS event
    FROM uniswap_v3_arbitrum.uniswapv3pool_evt_mint{event_where}
    UNION ALL
    SELECT contract_address, evt_tx_from, evt_tx_hash, evt_block_time,
           amount0, amount1, 'burn' AS event
    FROM uniswap_v3_arbitrum.uniswapv3pool_evt_burn{event_where}
)
SELECT
    {_hex('e.evt_tx_from')} AS lp_address,
    {_hex('e.contract_address')} AS pool_address,
    {_hex('p.token0')} AS token0_address,
    {_hex('p.token1')} AS token1_address,
    e.event,
    CAST(e.amount0 AS double) / power(10, p.decimals0) AS amount0,
    CAST(e.amount1 AS double) / power(10, p.decimals1) AS amount1,
    e.evt_block_time AS block_time,
    {_hex('e.evt_tx_hash')} AS tx_hash
FROM events e
JOIN idx_pools p ON p.pool = e.contract_address
"""


def chain_id() -> int:
    """Every table in this module is Arbitrum One only."""
    return CHAIN_ARBITRUM
