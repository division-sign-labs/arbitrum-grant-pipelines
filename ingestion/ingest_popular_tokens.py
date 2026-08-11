"""popular_tokens -> TRADED, HOLDS, DEPOSITED_IN, PROVIDED_LIQUIDITY.

Four independent legs of the same run, and any of them can be absent: the
pipeline writes a CSV only for the legs it was asked to run and degrades a
failed leg to an empty file. The manifest is the authority on which files exist,
so a missing leg is skipped with a log line rather than failing the ingest.

Merge keys, and why they are not just the tx hash:

  TRADED              (txHash, side). One transaction that both sells ARB and
                      buys PENDLE produces two rows; keying on the hash alone
                      would let the second overwrite the first.
  PROVIDED_LIQUIDITY  (txHash, event, poolAddress). A single tx can add
                      liquidity to two pools of the same token.
  DEPOSITED_IN        (txHash). One ERC-4626 deposit per tx per vault.
  HOLDS               a singleton per (wallet, token): a balance is a current
                      fact, so it is overwritten, and `asOf` dates it.

Raw uint256 amounts stay strings. Neo4j integers are 64-bit signed and a token
balance in wei routinely exceeds that; the human-scale decimal value sits
alongside in `balance` / `assets`.
"""

from __future__ import annotations

from cypher.popular_tokens import (
    HOLDINGS_CYPHER,
    LP_CYPHER,
    TRADES_CYPHER,
    VAULT_CYPHER,
)
from ingestion.base import Step, ingest_main, unique_rows

DATA_TYPE = "popular_tokens"

STEPS = [
    Step(
        label="trades -> TRADED",
        csv="trades",
        columns=[
            "fid", "address", "token_address", "chain_id", "side", "amount_usd",
            "token_amount", "block_time", "tx_hash",
        ],
        cypher=TRADES_CYPHER,
        required=False,
        transform=lambda rows: unique_rows(
            rows, ["address", "token_address", "chain_id", "tx_hash", "side"]
        ),
    ),
    Step(
        label="holdings -> HOLDS",
        csv="holdings",
        columns=[
            "fid", "address", "token_address", "chain_id", "balance", "balance_raw",
            "last_activity_at",
        ],
        cypher=HOLDINGS_CYPHER,
        required=False,
        transform=lambda rows: unique_rows(
            rows, ["address", "token_address", "chain_id"]
        ),
    ),
    Step(
        label="vault_deposits -> DEPOSITED_IN",
        csv="vault_deposits",
        columns=[
            "fid", "address", "vault_address", "chain_id", "assets", "assets_raw",
            "shares_raw", "block_time", "tx_hash",
        ],
        cypher=VAULT_CYPHER,
        required=False,
        transform=lambda rows: unique_rows(
            rows, ["address", "vault_address", "chain_id", "tx_hash"]
        ),
    ),
    Step(
        label="lp_events -> PROVIDED_LIQUIDITY",
        csv="lp_events",
        columns=[
            "fid", "address", "pool_address", "token_address", "chain_id", "event",
            "amount0", "amount1", "block_time", "tx_hash",
        ],
        cypher=LP_CYPHER,
        required=False,
        transform=lambda rows: unique_rows(
            rows, ["address", "token_address", "chain_id", "tx_hash", "event", "pool_address"]
        ),
    ),
]


def main(argv=None) -> int:
    return ingest_main(
        "ingest_popular_tokens", __doc__.splitlines()[0], DATA_TYPE, STEPS, argv
    )


if __name__ == "__main__":
    raise SystemExit(main())
