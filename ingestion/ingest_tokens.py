"""clanker_tokens / bankr_tokens -> Token nodes, DEPLOYED and CREATED edges.

One module, two data types, because the graph shape is identical: a token, the
wallets behind it, and — when the launch platform knows it — the Farcaster
account that ordered the launch. Only the columns differ, so `--source` selects
which run directory to read and which property set to write.

  --source clanker   data/clanker_tokens/   (Clanker API, 42161)
  --source bankr     data/bankr_tokens/     (Bankr API + Dune robinhood.*, 4663 and 42161)

Two wallets per token, not one: whoever deployed it and whoever is paid its fees
(`admin_address` on Clanker, `fee_recipient_address` on Bankr). They get the same
edges under different `role` values — see `cypher/tokens.py` for why, and for why
one address holding both roles still yields exactly one edge.

Both DEPLOYED and CREATED are singletons rather than tx-keyed events: a token is
deployed exactly once, so the pair (wallet, token) already is the natural key and
the tx hash rides along as a property.

Market data (price, market cap, 24h volume) is a snapshot, not history — each
run overwrites it, and `asOf` says when it was true. Bankr's per-day swap volume
is folded into totals on the Token node for the same reason: the schema has no
time-series edge, and the daily detail stays in the CSV run.
"""

from __future__ import annotations

from cypher.tokens import BANKR_CYPHER, CLANKER_CYPHER, VOLUME_CYPHER
from ingestion.base import Step, ingest_main

SOURCES = ("clanker", "bankr")

CLANKER_COLUMNS = [
    "token_address", "chain_id", "platform", "deployer_address", "admin_address",
    "fid", "fee_recipient_fid", "username", "name", "symbol", "deployed_at",
    "tx_hash", "pool_address", "paired_token", "token_type",
    "starting_market_cap_usd", "price_usd", "market_cap_usd", "volume_24h_usd",
]

BANKR_COLUMNS = [
    "token_address", "chain_id", "platform", "deployer_address",
    "fee_recipient_address", "fid", "fee_recipient_fid", "name", "symbol",
    "deployed_at", "tx_hash", "pool_address", "launch_type", "source",
]

VOLUME_COLUMNS = [
    "token_address", "chain_id", "day", "swap_count", "volume_native", "volume_usd",
]


def _latest_per_token(rows: list[dict]) -> list[dict]:
    """One row per (address, chain), keeping the newest deploy record.

    A token can appear twice in a run when the API paginates across a write, and
    the CSV is ordered newest-first, so the first sighting wins.
    """
    seen: dict[tuple, dict] = {}
    for row in rows:
        key = (row.get("token_address"), row.get("chain_id"))
        if key not in seen:
            seen[key] = row
    return list(seen.values())


def _totals_per_token(rows: list[dict]) -> list[dict]:
    """Collapse the daily volume series to per-token totals and a date range."""
    totals: dict[tuple, dict] = {}
    for row in rows:
        key = (row.get("token_address"), row.get("chain_id"))
        entry = totals.get(key)
        if entry is None:
            entry = {
                "token_address": key[0],
                "chain_id": key[1],
                "swap_count": 0,
                "volume_usd": 0.0,
                "volume_native": 0.0,
                "first_day": None,
                "last_day": None,
            }
            totals[key] = entry
        entry["swap_count"] += row.get("swap_count") or 0
        entry["volume_usd"] += row.get("volume_usd") or 0.0
        entry["volume_native"] += row.get("volume_native") or 0.0
        day = row.get("day")
        if day is not None:
            if entry["first_day"] is None or day < entry["first_day"]:
                entry["first_day"] = day
            if entry["last_day"] is None or day > entry["last_day"]:
                entry["last_day"] = day
    return list(totals.values())


STEPS_BY_SOURCE = {
    "clanker": [
        Step(
            label="tokens -> Token/DEPLOYED/CREATED",
            csv="tokens",
            columns=CLANKER_COLUMNS,
            cypher=CLANKER_CYPHER,
            transform=_latest_per_token,
            params={"platform": "clanker"},
        )
    ],
    "bankr": [
        Step(
            label="tokens -> Token/DEPLOYED/CREATED",
            csv="tokens",
            columns=BANKR_COLUMNS,
            cypher=BANKR_CYPHER,
            transform=_latest_per_token,
            params={"platform": "bankr"},
        ),
        Step(
            label="token_volume -> Token totals",
            csv="token_volume",
            columns=VOLUME_COLUMNS,
            cypher=VOLUME_CYPHER,
            required=False,
            transform=_totals_per_token,
            params={"platform": "bankr"},
        ),
    ],
}


def _add_args(parser) -> None:
    parser.add_argument(
        "--source",
        choices=SOURCES,
        required=True,
        help="Which launchpad run to ingest: clanker_tokens or bankr_tokens.",
    )


def main(argv=None) -> int:
    return ingest_main(
        "ingest_tokens",
        __doc__.splitlines()[0],
        lambda args: f"{args.source}_tokens",
        lambda args: STEPS_BY_SOURCE[args.source],
        argv,
        add_args=_add_args,
    )


if __name__ == "__main__":
    raise SystemExit(main())
