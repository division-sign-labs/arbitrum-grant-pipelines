"""Dune SQL builders for DEX trade activity, keyed by token address.

WHAT these produce: per-trade buy rows and per-token volume rollups from
`dex.trades`, the only cross-DEX decoded trade table on Dune that carries a USD
value on every row. Building the same thing from `erc20_arbitrum.evt_transfer`
plus `prices.usd` would mean pricing every launch token ourselves, and freshly
launched Clanker/Bankr tokens are exactly the ones `prices.usd` does not carry.

WHY both `taker` and `tx_from` are returned: on router and aggregator trades the
`taker` recorded by the decoder is frequently the router contract, not the human
who signed. Neither column is reliably the Farcaster wallet on its own, so both
are emitted and the caller picks whichever resolves against the linked-wallet
map. Doing that client-side is cheaper than a self-join on Dune and keeps the
"which address did we actually match" decision in one place.

Every builder returns a LIST of SQL strings — one per chunk of token addresses —
because a `WHERE token IN (...)` list of thousands of varbinary literals blows
past Trino's query-text limits. Callers run each and concatenate. An empty token
list yields an empty list, so callers never issue a query that matches nothing.

Cost notes:
  * `dex.trades` is partitioned on `block_month` / `block_date`; both are
    constrained alongside `block_time` so Trino prunes partitions instead of
    scanning the full history of every chain.
  * `block_time` is `timestamp(3) with time zone` here (the `arbitrum.*` tables
    use a plain timestamp), so the bound is rendered as
    `from_iso8601_timestamp(...)` rather than a bare `timestamp '...'` literal.
"""

from __future__ import annotations

from datetime import datetime
from typing import Iterable, Sequence

from config.settings import BACKFILL_START, CHAIN_ARBITRUM, DUNE_CHAIN_NAMES, MIN_BUY_USD
from lib import sqlfmt
from lib.sqlfmt import SqlLiteralError

# Trino tolerates far larger IN-lists than this, but 500 varbinary literals is
# roughly 11KB of query text and keeps each execution's plan small enough that a
# failure costs one cheap retry rather than a full re-scan.
TOKEN_CHUNK_SIZE = 500

TRADES_TABLE = "dex.trades"


def _chain_name(chain: int | str) -> str:
    """Accept a chain id or a Dune `blockchain` value; return the Dune name."""
    if isinstance(chain, str):
        name = chain.strip().lower()
    else:
        try:
            name = DUNE_CHAIN_NAMES.get(int(chain), "")
        except (TypeError, ValueError) as exc:
            raise SqlLiteralError(f"not a chain id or name: {chain!r}") from exc
    if not name or not name.replace("_", "").isalnum():
        raise SqlLiteralError(f"unknown/unsafe chain: {chain!r}")
    return name


def _tz_timestamp(value: str | datetime) -> str:
    """A tz-aware bound for `dex.trades.block_time`.

    `sqlfmt.timestamp` does the validation and UTC normalisation; we only
    re-shape its output, because a plain `timestamp '...'` literal compared
    against a `timestamp with time zone` column leans on Trino's session zone.
    """
    naive = sqlfmt.timestamp(value)[len("timestamp '") : -1]
    return f"from_iso8601_timestamp('{naive.replace(' ', 'T')}Z')"


def _date_literal(value: str | datetime) -> str:
    naive = sqlfmt.timestamp(value)[len("timestamp '") : -1]
    return f"date '{naive[:10]}'"


def _month_literal(value: str | datetime) -> str:
    """The partition month containing `value`, for the coarse `block_month` prune."""
    naive = sqlfmt.timestamp(value)[len("timestamp '") : -1]
    return f"date '{naive[:7]}-01'"


def _number(value: float) -> str:
    try:
        rendered = float(value)
    except (TypeError, ValueError) as exc:
        raise SqlLiteralError(f"not a number: {value!r}") from exc
    if rendered != rendered or rendered in (float("inf"), float("-inf")):
        raise SqlLiteralError(f"not a finite number: {value!r}")
    return repr(rendered)


def _time_filters(since: str | datetime) -> str:
    return (
        f"AND t.block_month >= {_month_literal(since)}\n"
        f"      AND t.block_date >= {_date_literal(since)}\n"
        f"      AND t.block_time >= {_tz_timestamp(since)}"
    )


def token_buys_sql(
    token_addresses: Sequence[str],
    chain: int | str = CHAIN_ARBITRUM,
    since: str | datetime = BACKFILL_START,
    min_usd: float = MIN_BUY_USD,
    chunk_size: int = TOKEN_CHUNK_SIZE,
) -> list[str]:
    """Per-trade rows where one of `token_addresses` was the token *bought*.

    Returns one query per chunk of tokens. Result columns:
        buyer_address, tx_from, token_address, blockchain, project, version,
        amount_usd, token_amount, block_time, tx_hash, evt_index

    `amount_usd >= min_usd` also drops the NULL-priced rows, which is the
    intended behaviour: an unpriced swap cannot clear a dollar threshold.
    """
    name = _chain_name(chain)
    where_time = _time_filters(since)
    threshold = _number(min_usd)

    queries: list[str] = []
    for chunk in sqlfmt.chunked(list(token_addresses), chunk_size):
        if not chunk:
            continue
        queries.append(
            f"""
SELECT
      '0x' || lower(to_hex(t.taker))                 AS buyer_address,
      '0x' || lower(to_hex(t.tx_from))               AS tx_from,
      '0x' || lower(to_hex(t.token_bought_address))  AS token_address,
      t.blockchain                                   AS blockchain,
      t.project                                      AS project,
      t.version                                      AS version,
      t.amount_usd                                   AS amount_usd,
      t.token_bought_amount                          AS token_amount,
      t.block_time                                   AS block_time,
      '0x' || lower(to_hex(t.tx_hash))               AS tx_hash,
      t.evt_index                                    AS evt_index
FROM {TRADES_TABLE} t
WHERE t.blockchain = {sqlfmt.text(name)}
      AND t.token_bought_address IN ({sqlfmt.address_list(chunk)})
      AND t.amount_usd >= {threshold}
      {where_time}
""".strip()
        )
    return queries


def token_volume_totals_sql(
    token_addresses: Sequence[str],
    chain: int | str = CHAIN_ARBITRUM,
    since: str | datetime = BACKFILL_START,
    chunk_size: int = TOKEN_CHUNK_SIZE,
) -> list[str]:
    """Both-sides traded volume per token, for the >$50k evangelist cut.

    Returns one query per chunk. Result columns:
        token_address, blockchain, trade_count, volume_usd, buy_volume_usd,
        sell_volume_usd, unique_takers, first_trade_at, last_trade_at

    Buys and sells are counted in a single scan via a CASE on which side of the
    trade matched, rather than a UNION ALL of two filtered scans. A trade
    between two tracked tokens is attributed to the bought side; that is rare
    enough (both legs would have to be launch tokens) to be worth the
    simplicity.
    """
    name = _chain_name(chain)
    where_time = _time_filters(since)

    queries: list[str] = []
    for chunk in sqlfmt.chunked(list(token_addresses), chunk_size):
        if not chunk:
            continue
        in_list = sqlfmt.address_list(chunk)
        queries.append(
            f"""
SELECT
      '0x' || lower(to_hex(
          CASE WHEN t.token_bought_address IN ({in_list})
               THEN t.token_bought_address
               ELSE t.token_sold_address END))                              AS token_address,
      t.blockchain                                                          AS blockchain,
      count(*)                                                              AS trade_count,
      coalesce(sum(t.amount_usd), 0)                                        AS volume_usd,
      coalesce(sum(CASE WHEN t.token_bought_address IN ({in_list})
                        THEN t.amount_usd END), 0)                          AS buy_volume_usd,
      coalesce(sum(CASE WHEN t.token_bought_address NOT IN ({in_list})
                        THEN t.amount_usd END), 0)                          AS sell_volume_usd,
      count(DISTINCT t.taker)                                               AS unique_takers,
      min(t.block_time)                                                     AS first_trade_at,
      max(t.block_time)                                                     AS last_trade_at
FROM {TRADES_TABLE} t
WHERE t.blockchain = {sqlfmt.text(name)}
      AND (t.token_bought_address IN ({in_list})
           OR t.token_sold_address IN ({in_list}))
      {where_time}
GROUP BY 1, 2
""".strip()
        )
    return queries


def blockchain_available_sql(chain: int | str) -> str:
    """One row iff `dex.trades` carries any trade for this chain.

    `blockchain` is the table's top-level partition key, so this is a metadata
    touch rather than a scan. Used to find out whether Robinhood Chain (a young
    Orbit L2) has been onboarded to the DEX decoders yet, without letting a
    "no" fail the run.
    """
    name = _chain_name(chain)
    return (
        f"SELECT DISTINCT t.blockchain AS blockchain\n"
        f"FROM {TRADES_TABLE} t\n"
        f"WHERE t.blockchain = {sqlfmt.text(name)}\n"
        f"LIMIT 1"
    )


def normalise_addresses(values: Iterable[str]) -> list[str]:
    """Lowercase, de-duplicate and drop anything that is not a 20-byte address.

    Registry CSVs arrive from third-party APIs, so they contain blanks, NaN and
    the occasional non-EVM identifier. Dropping them here means a single bad row
    cannot fail the whole chunk inside `sqlfmt.address_list`.
    """
    seen: dict[str, None] = {}
    for value in values:
        if not isinstance(value, str):
            continue
        candidate = value.strip().lower()
        try:
            seen.setdefault(sqlfmt.address(candidate), None)
        except SqlLiteralError:
            continue
    return list(seen)
