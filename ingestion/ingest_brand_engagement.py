"""brand_engagement -> ENGAGED_WITH, POSTED_IN, REACTED_IN.

The graph keeps social activity as aggregates, not as one edge per like. A
popular brand account collects millions of reactions; an edge each would swamp
every other signal in the graph and answer no question that
`weightedScore` does not answer better. So `brand_engagement_summary.csv` and
`channel_engagement_summary.csv` become the three singleton edges above,
recomputed and overwritten on each run.

The two event-level CSVs are still read — they are the evidence behind the
aggregates and they must satisfy the column contract — but they are used only to
materialise the accounts and channels the aggregates point at. That guarantees
every participant exists as a node even when a summary row was filtered out by a
score threshold upstream. The per-event rows themselves stay in the CSV run,
which is where an auditor should look for them.
"""

from __future__ import annotations

from cypher.brand_engagement import (
    CHANNEL_NODES_CYPHER,
    CHANNEL_SUMMARY_CYPHER,
    ENGAGEMENT_SUMMARY_CYPHER,
    PARTICIPANTS_CYPHER,
)
from ingestion.base import Step, ingest_main, unique_rows

DATA_TYPE = "brand_engagement"


def _participants(rows: list[dict]) -> list[dict]:
    """Both ends of every engagement, as account stubs."""
    fids = {row.get("engager_fid") for row in rows} | {row.get("brand_fid") for row in rows}
    return [{"fid": fid} for fid in sorted(fid for fid in fids if fid is not None)]


def _channel_nodes(rows: list[dict]) -> list[dict]:
    """One row per (author, channel) seen in the cast feed."""
    pairs = {
        (row.get("author_fid"), row.get("channel_id"))
        for row in rows
        if row.get("channel_id")
    }
    return [
        {"fid": fid, "channel_id": channel_id}
        for fid, channel_id in sorted(pairs, key=lambda pair: (pair[1], pair[0] or 0))
    ]


STEPS = [
    Step(
        label="brand_engagements -> accounts",
        csv="brand_engagements",
        columns=[
            "engager_fid", "brand_fid", "engagement_type", "cast_hash",
            "target_cast_hash", "timestamp",
        ],
        cypher=PARTICIPANTS_CYPHER,
        transform=_participants,
    ),
    Step(
        label="brand_engagement_summary -> ENGAGED_WITH",
        csv="brand_engagement_summary",
        columns=[
            "engager_fid", "brand_fid", "replies", "likes", "recasts", "mentions",
            "weighted_score", "window_start", "window_end",
        ],
        cypher=ENGAGEMENT_SUMMARY_CYPHER,
        transform=lambda rows: unique_rows(rows, ["engager_fid", "brand_fid"]),
    ),
    Step(
        label="channel_casts -> Channel",
        csv="channel_casts",
        columns=[
            "cast_hash", "author_fid", "channel_id", "timestamp", "parent_hash",
            "likes_count", "recasts_count", "replies_count", "text_length",
        ],
        cypher=CHANNEL_NODES_CYPHER,
        required=False,
        transform=_channel_nodes,
    ),
    Step(
        label="channel_engagement_summary -> POSTED_IN/REACTED_IN",
        csv="channel_engagement_summary",
        columns=[
            "fid", "channel_id", "casts_posted", "reactions_received",
            "replies_received", "reactions_given", "first_cast_at", "last_cast_at",
        ],
        cypher=CHANNEL_SUMMARY_CYPHER,
        required=False,
        transform=lambda rows: unique_rows(rows, ["fid", "channel_id"]),
    ),
]


def main(argv=None) -> int:
    return ingest_main(
        "ingest_brand_engagement", __doc__.splitlines()[0], DATA_TYPE, STEPS, argv
    )


if __name__ == "__main__":
    raise SystemExit(main())
