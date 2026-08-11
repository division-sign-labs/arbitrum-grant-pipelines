"""token_evangelists -> POSTED_ABOUT and EVANGELIZED.

Three CSVs, two edge types:

  token_casts.csv        every cast that mentioned a token -> POSTED_ABOUT,
                         keyed on the cast hash.
  attributions.csv       the buy-side evidence. It does not get an edge of its
                         own: the schema has no per-attribution relationship,
                         and the pairwise credit it carries is exactly what
                         evangelist_summary aggregates. What it does do is MERGE
                         the BOUGHT edges it references, so the evangelist
                         subgraph is complete even if token_buyers has not been
                         ingested for that window. Value properties are set
                         ON CREATE only, so token_buyers (the authority on a
                         purchase) is never overwritten by the attribution copy.
  evangelist_summary.csv the per (author, token) rollup -> EVANGELIZED, a
                         singleton recomputed and overwritten each run.

Attribution is fractional — a buyer exposed to three influencers splits credit
three ways — so `attributedUsd` summed across authors is comparable to purchase
volume, while `totalPurchaseVolumeUsd` deliberately double-counts. Both are kept
because they answer different questions.
"""

from __future__ import annotations

from cypher.token_evangelists import (
    ATTRIBUTIONS_CYPHER,
    SUMMARY_CYPHER,
    TOKEN_CASTS_CYPHER,
)
from ingestion.base import Step, ingest_main, unique_rows

DATA_TYPE = "token_evangelists"

STEPS = [
    Step(
        label="token_casts -> POSTED_ABOUT",
        csv="token_casts",
        columns=[
            "token_address", "chain_id", "cast_hash", "author_fid", "timestamp",
            "matched_on", "likes_count", "recasts_count",
        ],
        cypher=TOKEN_CASTS_CYPHER,
        transform=lambda rows: unique_rows(
            rows, ["author_fid", "token_address", "chain_id", "cast_hash"]
        ),
    ),
    Step(
        label="attributions -> BOUGHT",
        csv="attributions",
        columns=[
            "token_address", "chain_id", "author_fid", "buyer_fid", "buyer_address",
            "tx_hash", "amount_usd", "block_time", "attributed_usd", "n_influencers",
        ],
        cypher=ATTRIBUTIONS_CYPHER,
        required=False,
        transform=lambda rows: unique_rows(
            rows, ["buyer_address", "token_address", "chain_id", "tx_hash", "author_fid"]
        ),
    ),
    Step(
        label="evangelist_summary -> EVANGELIZED",
        csv="evangelist_summary",
        columns=[
            "token_address", "chain_id", "author_fid", "cast_count",
            "unique_buyers_influenced", "total_purchases",
            "total_purchase_volume_usd", "attributed_usd",
        ],
        cypher=SUMMARY_CYPHER,
        required=False,
        transform=lambda rows: unique_rows(
            rows, ["author_fid", "token_address", "chain_id"]
        ),
    ),
]


def main(argv=None) -> int:
    return ingest_main(
        "ingest_token_evangelists", __doc__.splitlines()[0], DATA_TYPE, STEPS, argv
    )


if __name__ == "__main__":
    raise SystemExit(main())
