"""linked_wallets -> (WarpcastAccount)-[:ACCOUNT]->(Wallet).

This is the join table the rest of the graph hangs off: every other ingest can
attach a wallet to an fid only because this one recorded the link. It is also
the only module that owns WarpcastAccount profile properties (username, score,
follower counts) — everywhere else an account node is a stub created so an edge
has somewhere to land.

Wallet keys are lowercased, but only when they look like 0x-hex: `wallets.csv`
carries Solana addresses too (protocol='sol'), and base58 is case-sensitive, so
blanket toLower() would key two different Solana wallets to the same node.

The `source` column distinguishes verified addresses from the custody address
Farcaster assigns at registration; both are real wallets, so both become nodes,
and the distinction lives on the ACCOUNT edge.
"""

from __future__ import annotations

from cypher.linked_wallets import ACCOUNTS_CYPHER, WALLETS_CYPHER
from ingestion.base import Step, ingest_main, unique_rows

DATA_TYPE = "linked_wallets"

STEPS = [
    Step(
        label="accounts -> WarpcastAccount",
        csv="accounts",
        columns=[
            "fid", "username", "display_name", "neynar_score", "follower_count",
            "following_count", "custody_address", "registered_at",
        ],
        cypher=ACCOUNTS_CYPHER,
        transform=lambda rows: unique_rows(rows, ["fid"]),
    ),
    Step(
        label="wallets -> ACCOUNT",
        csv="wallets",
        columns=["fid", "address", "protocol", "is_primary", "source"],
        cypher=WALLETS_CYPHER,
        transform=lambda rows: unique_rows(rows, ["fid", "address"]),
    ),
]


def main(argv=None) -> int:
    return ingest_main(
        "ingest_linked_wallets", __doc__.splitlines()[0], DATA_TYPE, STEPS, argv
    )


if __name__ == "__main__":
    raise SystemExit(main())
