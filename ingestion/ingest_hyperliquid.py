"""hyperliquid_activity -> (Wallet)-[:USED]->(Platform{name:'hyperliquid'}).

The crawl checks every cohort wallet and records the negatives too, because
"checked, no activity" is a different and useful fact from "never checked". The
graph only needs the positives as edges, so rows with has_hl_activity=false are
dropped here and the wallet is left without a USED edge — but the wallet node
still gets `hlCheckedAt`, so a later run can tell the two apart without
re-reading the CSV.

USED is a singleton per wallet: lifetime volume and first-activity time are
current facts, recomputed and overwritten on each run.
"""

from __future__ import annotations

from cypher.hyperliquid import ACTIVITY_CYPHER, CHECKED_CYPHER
from ingestion.base import Step, ingest_main, unique_rows

DATA_TYPE = "hyperliquid_activity"
PLATFORM = "hyperliquid"

COLUMNS = [
    "address", "fid", "has_hl_activity", "cum_volume_usd", "first_activity_at",
    "ledger_event_count", "checked_at",
]


def _active(rows: list[dict]) -> list[dict]:
    return unique_rows([row for row in rows if row.get("has_hl_activity")], ["address"])


def _inactive(rows: list[dict]) -> list[dict]:
    return unique_rows(
        [
            {"address": row.get("address"), "checked_at": row.get("checked_at")}
            for row in rows
            if not row.get("has_hl_activity")
        ],
        ["address"],
    )


STEPS = [
    Step(
        label="hl_activity -> USED",
        csv="hl_activity",
        columns=COLUMNS,
        cypher=ACTIVITY_CYPHER,
        transform=_active,
        params={"platform": PLATFORM},
    ),
    Step(
        label="hl_activity -> checked, no activity",
        csv="hl_activity",
        columns=COLUMNS,
        cypher=CHECKED_CYPHER,
        transform=_inactive,
    ),
]


def main(argv=None) -> int:
    return ingest_main(
        "ingest_hyperliquid", __doc__.splitlines()[0], DATA_TYPE, STEPS, argv
    )


if __name__ == "__main__":
    raise SystemExit(main())
