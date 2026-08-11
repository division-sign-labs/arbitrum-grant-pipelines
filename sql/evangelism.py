"""DuneSQL builders for token evangelism: which tokens are worth analysing, and
every on-chain buy of them that attribution can hang off.

The operator's reference query did the whole attribution in SQL, joining
`dune.neynar.dataset_farcaster_casts` and `_reactions` against `dex.trades`.
Those neynar datasets do not exist on our key (verified: all six fail), so the
social half now comes from the Neynar REST API and only the trade half stays on
Dune. That split is why these builders return raw buys rather than a finished
attribution: the 5-day window join happens locally in pandas, against casts Dune
cannot see.

`dex.trades` is the right source for the trade half because it is already
decoded and USD-priced across every DEX on the chain, so a token that trades on
three different routers still yields one comparable `amount_usd` per fill.
"""

from __future__ import annotations

from typing import Iterable, Sequence

from config.settings import BACKFILL_START, CHAIN_ARBITRUM, DUNE_CHAIN_NAMES
from lib import sqlfmt
from lib.state import parse_ts

# Farcaster embeds a token as a CAIP-19-ish frame/parent url. Casts launched
# from a token page carry it verbatim on parent_url / root_parent_url, which is
# the third way the reference query recognised a token cast.
TOKEN_PARENT_URL_TEMPLATE = "eip155:{chain_id}/erc20:{address}"


def token_parent_url(address: str, chain_id: int) -> str:
    """The parent_url form a Farcaster client stamps on a token's casts."""
    return TOKEN_PARENT_URL_TEMPLATE.format(
        chain_id=int(chain_id), address=sqlfmt.address(address)
    )


def chain_name(chain) -> str:
    """Accept a numeric chain id or a Dune `blockchain` value; return the value.

    Callers hold chain ids (the CSV contract and the graph key on chainId) but
    `dex.trades` partitions on the name, so every builder funnels through here
    rather than each one re-deriving the mapping.
    """
    if isinstance(chain, str) and not chain.isdigit():
        return chain.strip().lower()
    chain_id = int(chain)
    name = DUNE_CHAIN_NAMES.get(chain_id)
    if not name:
        raise sqlfmt.SqlLiteralError(
            f"no Dune blockchain name known for chain id {chain_id}; "
            f"add it to config.settings.DUNE_CHAIN_NAMES"
        )
    return name


def _tstz(value) -> str:
    """A `timestamp with time zone` literal.

    `dex.trades.block_time` is timestamptz while `arbitrum.*` block_time is a
    plain timestamp. Casting a bare `timestamp '...'` would resolve against the
    session zone; `from_iso8601_timestamp` pins it to UTC explicitly, so the
    window means the same thing no matter how Dune is configured.
    """
    parsed = parse_ts(value)
    if parsed is None:
        raise sqlfmt.SqlLiteralError(f"not a timestamp: {value!r}")
    return f"from_iso8601_timestamp('{parsed.strftime('%Y-%m-%dT%H:%M:%SZ')}')"


def _date(value) -> str:
    """A `date` literal for the partition column, so Trino prunes before scanning."""
    parsed = parse_ts(value)
    if parsed is None:
        raise sqlfmt.SqlLiteralError(f"not a timestamp: {value!r}")
    return f"date '{parsed.strftime('%Y-%m-%d')}'"


def token_buys_for_attribution_multi_sql(
    token_addresses: Sequence[str],
    chain=CHAIN_ARBITRUM,
    since=BACKFILL_START,
) -> str:
    """Every buy of any of `token_addresses`, one row per (tx, buyer, token).

    No USD floor: attribution divides each purchase among the influencers who
    earned it, and a $5 buy is still evidence a cast landed. `MIN_BUY_USD` gates
    the token_buyers pipeline, not this one.

    Multi-leg routes (a single tx that fills the same token across two pools)
    collapse into one row, because the buyer made one decision and the graph
    MERGEs the edge on txHash — leaving the legs split would double-count the
    purchase against every influencer.

    `taker` is the trading account; on router-mediated swaps it can be a
    contract, so `tx_from` comes back too and the local join tries both against
    the Farcaster wallet map.
    """
    addresses = [sqlfmt.address(a) for a in token_addresses]
    if not addresses:
        raise sqlfmt.SqlLiteralError("no token addresses given")
    return f"""
SELECT
    '0x' || lower(to_hex(t.token_bought_address))               AS token_address,
    '0x' || lower(to_hex(coalesce(t.taker, t.tx_from)))          AS buyer_address,
    '0x' || lower(to_hex(t.tx_from))                             AS tx_from,
    '0x' || lower(to_hex(t.tx_hash))                             AS tx_hash,
    min(t.block_time)                                            AS block_time,
    sum(t.amount_usd)                                            AS amount_usd,
    sum(t.token_bought_amount)                                   AS token_amount,
    count(*)                                                     AS legs
FROM dex.trades t
WHERE t.blockchain = {sqlfmt.text(chain_name(chain))}
  AND t.token_bought_address IN ({sqlfmt.address_list(addresses)})
  AND t.block_date >= {_date(since)}
  AND t.block_time >= {_tstz(since)}
GROUP BY 1, 2, 3, 4
""".strip()


def token_buys_for_attribution_sql(
    token_address: str,
    chain=CHAIN_ARBITRUM,
    since=BACKFILL_START,
) -> str:
    """Every buy of one token — the single-token form of the builder above."""
    return token_buys_for_attribution_multi_sql([token_address], chain, since)


def token_volume_totals_sql(
    token_addresses: Iterable[str],
    chain=CHAIN_ARBITRUM,
    since=BACKFILL_START,
) -> str:
    """Lifetime traded volume per token, both sides of the book.

    This is the qualification gate: only tokens above
    EVANGELIST_MIN_VOLUME_USD are worth spending Neynar search and reaction
    calls on. Volume counts buys *and* sells because a token nobody can exit is
    not a token anyone evangelised into.

    Deliberately reads from BACKFILL_START rather than the incremental window:
    a token's lifetime volume must not shrink just because today's run only
    looked at yesterday, or the qualifying set would flap between runs.
    """
    addresses = [sqlfmt.address(a) for a in token_addresses]
    if not addresses:
        raise sqlfmt.SqlLiteralError("no token addresses given")
    in_list = sqlfmt.address_list(addresses)
    return f"""
WITH scoped AS (
    SELECT
        token_bought_address,
        token_sold_address,
        token_bought_symbol,
        token_sold_symbol,
        amount_usd,
        block_time
    FROM dex.trades
    WHERE blockchain = {sqlfmt.text(chain_name(chain))}
      AND block_date >= {_date(since)}
      AND block_time >= {_tstz(since)}
      AND (
            token_bought_address IN ({in_list})
         OR token_sold_address IN ({in_list})
      )
),
sided AS (
    SELECT
        token_bought_address AS token,
        token_bought_symbol  AS symbol,
        amount_usd,
        block_time
    FROM scoped
    WHERE token_bought_address IN ({in_list})
    UNION ALL
    SELECT
        token_sold_address AS token,
        token_sold_symbol  AS symbol,
        amount_usd,
        block_time
    FROM scoped
    WHERE token_sold_address IN ({in_list})
)
SELECT
    '0x' || lower(to_hex(token)) AS token_address,
    max(symbol)                  AS dex_symbol,
    sum(amount_usd)              AS volume_usd,
    count(*)                     AS trade_count,
    min(block_time)              AS first_trade_at,
    max(block_time)              AS last_trade_at
FROM sided
GROUP BY 1
ORDER BY volume_usd DESC
""".strip()
