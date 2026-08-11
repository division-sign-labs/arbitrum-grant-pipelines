"""contract_deployers -> (Wallet)-[:DEPLOYED]->(Contract) and -[:ACTIVE_ON]->(Chain).

DEPLOYED is an event edge keyed on the deploy tx hash, so re-ingesting an
overlapping window collapses instead of double-counting. The key is (wallet,
contract, txHash) rather than txHash alone, which matters for factory deploys:
one transaction can create several contracts, and each gets its own edge.

ACTIVE_ON is the aggregate counterpart — one edge per (wallet, chain),
recomputed and overwritten on every run. Note that miniapp_builders writes the
same edge type for its own wallets: whichever pipeline ran most recently wins,
and `source` on the edge records which one that was. The two agree by
construction (both count the same wallet's transactions on the same chain), so
this is a last-writer-wins refresh, not a conflict.
"""

from __future__ import annotations

from cypher.contract_deployers import ACTIVITY_CYPHER, DEPLOYMENTS_CYPHER
from ingestion.base import Step, ingest_main, unique_rows

DATA_TYPE = "contract_deployers"

STEPS = [
    Step(
        label="deployments -> DEPLOYED",
        csv="deployments",
        columns=[
            "fid", "deployer_address", "contract_address", "chain_id",
            "deployed_at", "tx_hash", "deploy_method",
        ],
        cypher=DEPLOYMENTS_CYPHER,
        transform=lambda rows: unique_rows(
            rows, ["deployer_address", "contract_address", "chain_id", "tx_hash"]
        ),
    ),
    Step(
        label="deployer_activity -> ACTIVE_ON",
        csv="deployer_activity",
        columns=["fid", "address", "chain_id", "tx_count", "first_tx_at", "last_tx_at"],
        cypher=ACTIVITY_CYPHER,
        required=False,
        transform=lambda rows: unique_rows(rows, ["address", "chain_id"]),
    ),
]


def main(argv=None) -> int:
    return ingest_main(
        "ingest_contract_deployers", __doc__.splitlines()[0], DATA_TYPE, STEPS, argv
    )


if __name__ == "__main__":
    raise SystemExit(main())
