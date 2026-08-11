"""Dune SQL for Arbitrum contract deployment and per-wallet chain activity.

WHAT these builders produce:
  - the full distinct-deployer roll-up on Arbitrum (373k addresses since
    2025-01-01), used to find which deployers are Farcaster accounts;
  - per-contract deployment detail for a known set of addresses;
  - per-address transaction counts and first/last activity.

WHY the join to Farcaster happens elsewhere: Dune carries no Farcaster tables
with this key — `dune.neynar.dataset_farcaster_*` does not exist — and this
account may only create PUBLIC uploads, so a wallet table cannot be shipped to
Dune to join against. The deployer roll-up is small enough (a few hundred
thousand rows) to pull whole and intersect with the linked-wallet set locally
in pandas, which is both cheaper and less leaky than any Dune-side alternative.

Column notes that bit during development and are easy to get wrong again:
  - `from` and `to` are reserved words in Trino: always double-quote them.
  - address columns are varbinary; `to_hex()` drops the `0x`, so every rendered
    address is `'0x' || lower(to_hex(col))`.
  - `arbitrum.creation_traces` is partitioned by `block_month` (a date) and has
    no `block_date`; `arbitrum.transactions` has `block_date` but no
    `block_month`. Both carry a plain `timestamp(3)` `block_time`, so
    `sqlfmt.timestamp()` literals compare directly without a cast.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable, Sequence

import pandas as pd

from lib.sqlfmt import SqlLiteralError, address_list, chunked, timestamp

logger = logging.getLogger(__name__)

# Dune's IN-list planning stays comfortable at a thousand literals; beyond that
# the query text itself starts to dominate the request.
DEFAULT_ADDRESS_CHUNK = 1000

DEPLOYER_SUMMARY_COLUMNS = [
    "deployer_address",
    "contract_count",
    "first_deploy_at",
    "last_deploy_at",
]
DEPLOYMENT_COLUMNS = [
    "deployer_address",
    "contract_address",
    "deployed_at",
    "tx_hash",
    "deploy_method",
]
WALLET_ACTIVITY_COLUMNS = ["address", "tx_count", "first_tx_at", "last_tx_at"]


def _as_datetime(value) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        raise SqlLiteralError(f"not a datetime: {value!r}")
    return value


def _month_literal(value) -> str:
    """`date '2025-01-01'` for `creation_traces.block_month` partition pruning.

    Floored to the first of the month: the partition key is the month itself, so
    anything later would drop the very creations we asked for.
    """
    return f"date '{_as_datetime(value).strftime('%Y-%m-01')}'"


def _date_literal(value) -> str:
    """`date '2025-01-14'` for `transactions.block_date` partition pruning."""
    return f"date '{_as_datetime(value).strftime('%Y-%m-%d')}'"


def arbitrum_deployers_sql(since) -> str:
    """Every distinct Arbitrum contract creator since `since`, with its counts.

    Deliberately unfiltered by who the deployer is: the whole point is to pull
    the full set once and intersect it with Farcaster wallets client-side.
    """
    return f"""
SELECT
    '0x' || lower(to_hex("from")) AS deployer_address,
    count(*)                      AS contract_count,
    min(block_time)               AS first_deploy_at,
    max(block_time)               AS last_deploy_at
FROM arbitrum.creation_traces
WHERE block_month >= {_month_literal(since)}
  AND block_time >= {timestamp(since)}
  AND "from" IS NOT NULL
GROUP BY 1
"""


def arbitrum_deployments_sql(
    deployer_addresses: Sequence[str],
    since,
    chunk_size: int = DEFAULT_ADDRESS_CHUNK,
) -> list[str]:
    """Per-contract deployment rows for a known set of addresses, one SQL per chunk.

    A contract can be created two ways, and both must land on the human:
      - the cohort wallet is the trace creator (a plain EOA deploy) — 'direct';
      - the cohort wallet only *sent* the transaction and a factory contract
        executed the CREATE — 'via_factory'. The trace creator is then the
        factory, which is useless for attribution, so the tx sender is used.

    The `senders` CTE is filtered by `"from" IN (...)` before anything is
    joined. That keeps the transactions read a selective column scan and leaves
    the join against creation_traces a small broadcast, rather than joining two
    very large tables and filtering afterwards.
    """
    ts = timestamp(since)
    month = _month_literal(since)
    day = _date_literal(since)

    statements: list[str] = []
    for chunk in chunked(list(deployer_addresses), chunk_size):
        addrs = address_list(chunk)
        statements.append(
            f"""
WITH creations AS (
    SELECT block_time, tx_hash, address, "from" AS creator
    FROM arbitrum.creation_traces
    WHERE block_month >= {month}
      AND block_time >= {ts}
      AND address IS NOT NULL
),
senders AS (
    SELECT hash, "from" AS tx_sender
    FROM arbitrum.transactions
    WHERE block_date >= {day}
      AND block_time >= {ts}
      AND "from" IN ({addrs})
)
SELECT
    CASE
        WHEN c.creator IN ({addrs}) THEN '0x' || lower(to_hex(c.creator))
        ELSE '0x' || lower(to_hex(s.tx_sender))
    END                              AS deployer_address,
    '0x' || lower(to_hex(c.address)) AS contract_address,
    c.block_time                     AS deployed_at,
    '0x' || lower(to_hex(c.tx_hash)) AS tx_hash,
    CASE
        -- A null sender means the creator matched but its transaction is not in
        -- the sender set, which for an EOA creator can only be a direct deploy.
        WHEN s.tx_sender IS NULL THEN 'direct'
        WHEN c.creator = s.tx_sender THEN 'direct'
        ELSE 'via_factory'
    END                              AS deploy_method
FROM creations c
LEFT JOIN senders s ON s.hash = c.tx_hash
WHERE c.creator IN ({addrs})
   OR s.tx_sender IS NOT NULL
"""
        )
    return statements


def arbitrum_wallet_activity_sql(
    addresses: Sequence[str],
    since,
    chunk_size: int = DEFAULT_ADDRESS_CHUNK,
) -> list[str]:
    """Transaction count and first/last activity per address, one SQL per chunk.

    Failed transactions are counted: the edge this feeds means "this wallet was
    active on this chain", and a reverted transaction is still activity.
    """
    ts = timestamp(since)
    day = _date_literal(since)

    statements: list[str] = []
    for chunk in chunked(list(addresses), chunk_size):
        addrs = address_list(chunk)
        statements.append(
            f"""
SELECT
    '0x' || lower(to_hex("from")) AS address,
    count(*)                      AS tx_count,
    min(block_time)               AS first_tx_at,
    max(block_time)               AS last_tx_at
FROM arbitrum.transactions
WHERE block_date >= {day}
  AND block_time >= {ts}
  AND "from" IN ({addrs})
GROUP BY 1
"""
        )
    return statements


def run_chunked(
    dune,
    statements: Iterable[str],
    label: str,
    columns: Sequence[str],
    limit: int | None = None,
    use_cache: bool = True,
) -> pd.DataFrame:
    """Execute chunked SQL and concatenate the results into one frame.

    Always returns a frame carrying `columns`, so a run that matched nothing
    still writes a CSV with the header the ingestion contract expects instead of
    an empty file.
    """
    statements = list(statements)
    frames: list[pd.DataFrame] = []
    for i, sql in enumerate(statements, start=1):
        chunk_label = f"{label} [{i}/{len(statements)}]"
        frame = dune.run_sql(sql, label=chunk_label, limit=limit, use_cache=use_cache)
        if not frame.empty:
            frames.append(frame)
    if not frames:
        logger.info("%s: no rows", label)
        return pd.DataFrame(columns=list(columns))
    combined = pd.concat(frames, ignore_index=True)
    missing = [c for c in columns if c not in combined.columns]
    if missing:
        # Defensive: an upstream schema change would otherwise surface as a
        # KeyError deep in the pipeline instead of a named, fixable warning.
        logger.warning("%s: result is missing column(s) %s", label, missing)
        for column in missing:
            combined[column] = None
    return combined
