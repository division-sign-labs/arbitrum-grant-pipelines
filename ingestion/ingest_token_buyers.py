"""token_buyers -> (Wallet)-[:BOUGHT]->(Token).

The edge is keyed on the swap's transaction hash, which makes re-ingesting an
overlapping window free. One caveat worth stating: a single transaction can
contain two buys of the same token by the same wallet (a router splitting a
fill across pools). The CSV contract carries no event index, so those collapse
into one edge and the last row in the file wins. The alternative — keying on
(txHash, amount) — would turn a rounding difference into a duplicate purchase,
which is the worse failure for the volume numbers this feeds.

The buyer's fid links wallet to account only when it is not already linked;
linked_wallets owns that edge's properties.
"""

from __future__ import annotations

from cypher.token_buyers import BUYS_CYPHER
from ingestion.base import Step, ingest_main, unique_rows

DATA_TYPE = "token_buyers"

STEPS = [
    Step(
        label="buys -> BOUGHT",
        csv="buys",
        columns=[
            "fid", "buyer_address", "token_address", "chain_id", "platform",
            "amount_usd", "token_amount", "block_time", "tx_hash",
        ],
        cypher=BUYS_CYPHER,
        transform=lambda rows: unique_rows(
            rows, ["buyer_address", "token_address", "chain_id", "tx_hash"]
        ),
    )
]


def main(argv=None) -> int:
    return ingest_main(
        "ingest_token_buyers", __doc__.splitlines()[0], DATA_TYPE, STEPS, argv
    )


if __name__ == "__main__":
    raise SystemExit(main())
