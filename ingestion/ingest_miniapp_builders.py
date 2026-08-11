"""miniapp_builders_activity -> builder wallets and their on-chain footprint.

The seed list is a set of fids, so this module's job is to make sure each
builder's account and wallets exist and are linked, then hang the per-chain
activity aggregate off the wallet. It writes the ACCOUNT edge but does not own
it: linked_wallets knows the protocol and which address is primary, so those
properties are only filled in on creation and never overwritten here.

`miniappBuilder` on the account is the one property this module does own — it is
the seed-list membership flag, and it is what a "builders who also deploy
contracts" query filters on.

ACTIVE_ON is shared with contract_deployers; see that module for why
last-writer-wins is the right behaviour there.
"""

from __future__ import annotations

from cypher.miniapp_builders import ACTIVITY_CYPHER, WALLETS_CYPHER
from ingestion.base import Step, ingest_main, unique_rows

DATA_TYPE = "miniapp_builders_activity"

STEPS = [
    Step(
        label="builder_wallets -> ACCOUNT",
        csv="builder_wallets",
        columns=["fid", "address"],
        cypher=WALLETS_CYPHER,
        transform=lambda rows: unique_rows(rows, ["fid", "address"]),
    ),
    Step(
        label="builder_activity -> ACTIVE_ON",
        csv="builder_activity",
        columns=["fid", "address", "chain_id", "tx_count", "first_tx_at", "last_tx_at"],
        cypher=ACTIVITY_CYPHER,
        required=False,
        transform=lambda rows: unique_rows(rows, ["address", "chain_id"]),
    ),
]


def main(argv=None) -> int:
    return ingest_main(
        "ingest_miniapp_builders", __doc__.splitlines()[0], DATA_TYPE, STEPS, argv
    )


if __name__ == "__main__":
    raise SystemExit(main())
