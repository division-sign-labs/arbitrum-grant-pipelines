"""Arbitrum on-chain activity for the seeded Farcaster miniapp builders.

WHAT this produces, under data/miniapp_builders_activity/<run_ts>/:
  builder_wallets.csv   every EVM wallet linked to a seeded builder fid;
  builder_activity.csv  each of those wallets' Arbitrum transaction footprint.

WHY it is seed-driven: there is no on-chain or API signal that says "this
Farcaster account shipped a miniapp". The set has to be curated, so it comes
from seeds/miniapp_builders.csv and the pipeline's job is only to resolve those
fids to wallets and measure them.

WHY it always recomputes the full window: this cohort is hundreds of accounts,
not millions, so the Dune query is selective on `"from" IN (...)` and costs
roughly the same whatever the time range. Meanwhile `tx_count` /
`first_tx_at` / `last_tx_at` feed the singleton (Wallet)-[:ACTIVE_ON]->(Chain)
edge, which ingestion overwrites rather than accumulates — an incremental
window would silently replace a lifetime count with a partial one. So the
activity query always starts at BACKFILL_START unless the operator passes
--since explicitly. The watermark is still maintained so later runs need no
flags.

Wallet resolution prefers the local linked_wallets map (free, already paid
for). Only fids missing from it go to Neynar's /v2/farcaster/user/bulk. A dry
run skips the Neynar calls entirely so it spends nothing.
"""

from __future__ import annotations

import logging
from typing import Iterable

import pandas as pd

from config.settings import BACKFILL_START, CHAIN_ARBITRUM, NEYNAR_FID_BATCH
from lib.cli import base_parser, resolve_window
from lib.dune import DuneRunner
from lib.logging_utils import setup_logging
from lib.runs import RunWriter
from lib.seeds import SeedMissingError, load_miniapp_builders
from lib.state import max_timestamp, parse_ts, set_watermark
from pipelines.contract_deployers import (
    is_evm_address,
    iso_timestamps,
    load_farcaster_wallets,
)
from sql.deployers import (
    WALLET_ACTIVITY_COLUMNS,
    arbitrum_wallet_activity_sql,
    run_chunked,
)

logger = logging.getLogger(__name__)

PIPELINE = "miniapp_builders"
DATA_TYPE = "miniapp_builders_activity"

WALLETS_CSV_COLUMNS = ["fid", "address"]
ACTIVITY_CSV_COLUMNS = [
    "fid",
    "address",
    "chain_id",
    "tx_count",
    "first_tx_at",
    "last_tx_at",
]


def _local_builder_wallets(fids: set[int]) -> pd.DataFrame:
    """Wallets for the seeded fids taken from the linked_wallets run, if there is one.

    A missing linked_wallets run is not fatal here the way it is for
    contract_deployers: this cohort is small enough that Neynar can resolve all
    of it directly.
    """
    try:
        wallets = load_farcaster_wallets()
    except SystemExit as exc:
        logger.warning("no local wallet map (%s); resolving every fid via Neynar", exc)
        return pd.DataFrame(columns=WALLETS_CSV_COLUMNS)
    return wallets[wallets["fid"].isin(fids)][WALLETS_CSV_COLUMNS].copy()


def _neynar_builder_wallets(fids: Iterable[int]) -> pd.DataFrame:
    """Verified EVM addresses plus custody address for fids Neynar knows about.

    Solana verifications are dropped: this pipeline only measures Arbitrum. The
    custody address is kept because it is a real wallet the account controls and
    Farcaster-native builders do sometimes deploy from it.
    """
    fids = sorted({int(f) for f in fids})
    if not fids:
        return pd.DataFrame(columns=WALLETS_CSV_COLUMNS)

    from lib.neynar import NeynarClient  # imported late: a dry run needs no key

    client = NeynarClient()
    rows: list[dict] = []
    resolved: set[int] = set()
    for start in range(0, len(fids), NEYNAR_FID_BATCH):
        batch = fids[start : start + NEYNAR_FID_BATCH]
        for user in client.bulk_users(batch):
            fid = user.get("fid")
            if fid is None:
                continue
            resolved.add(int(fid))
            verified = user.get("verified_addresses") or {}
            primary = verified.get("primary") or {}
            candidates = list(verified.get("eth_addresses") or [])
            candidates.append(primary.get("eth_address"))
            candidates.append(user.get("custody_address"))
            for address in candidates:
                if is_evm_address(address):
                    rows.append({"fid": int(fid), "address": address.strip().lower()})

    unknown = set(fids) - resolved
    if unknown:
        logger.warning(
            "Neynar returned no profile for %d seeded fid(s): %s",
            len(unknown),
            sorted(unknown)[:20],
        )
    frame = pd.DataFrame(rows, columns=WALLETS_CSV_COLUMNS)
    return frame.drop_duplicates()


def resolve_builder_wallets(fids: set[int], allow_neynar: bool = True) -> pd.DataFrame:
    """fid + address for the seeded builders, local map first, Neynar for the rest."""
    local = _local_builder_wallets(fids)
    logger.info(
        "local wallet map covers %d/%d seeded fids (%d wallets)",
        local["fid"].nunique() if not local.empty else 0,
        len(fids),
        len(local),
    )

    missing = fids - set(local["fid"].tolist())
    if missing and not allow_neynar:
        logger.info("[dry-run] would resolve %d fid(s) via Neynar bulk_users", len(missing))
        remote = pd.DataFrame(columns=WALLETS_CSV_COLUMNS)
    elif missing:
        logger.info("resolving %d fid(s) via Neynar", len(missing))
        remote = _neynar_builder_wallets(missing)
    else:
        remote = pd.DataFrame(columns=WALLETS_CSV_COLUMNS)

    combined = pd.concat([local, remote], ignore_index=True)
    if combined.empty:
        return pd.DataFrame(columns=WALLETS_CSV_COLUMNS)
    combined["fid"] = combined["fid"].astype("int64")
    combined["address"] = combined["address"].astype(str).str.strip().str.lower()
    combined = combined[combined["address"].map(is_evm_address)]
    return combined.drop_duplicates(subset=WALLETS_CSV_COLUMNS).reset_index(drop=True)


def run(window, args) -> dict:
    notes: list[str] = []
    dune = DuneRunner(dry_run=args.dry_run)
    writer = RunWriter(DATA_TYPE, dry_run=args.dry_run)

    seed = load_miniapp_builders()
    fids = {int(f) for f in seed["fid"].tolist()}
    if args.limit and len(fids) > args.limit:
        fids = set(sorted(fids)[: args.limit])
        notes.append(f"--limit truncated the builder cohort to {args.limit} fids")
    logger.info("%d seeded miniapp builder fids", len(fids))

    wallets = resolve_builder_wallets(fids, allow_neynar=not args.dry_run)
    unresolved = fids - set(wallets["fid"].tolist())
    if unresolved:
        notes.append(f"{len(unresolved)} seeded fid(s) resolved to no EVM wallet")
    logger.info("%d wallets for %d fids", len(wallets), wallets["fid"].nunique() if not wallets.empty else 0)

    # Full recompute by default — the activity aggregates back a singleton edge.
    activity_since = window.since if args.since else parse_ts(BACKFILL_START)
    addresses = sorted(wallets["address"].unique()) if not wallets.empty else []
    activity = run_chunked(
        dune,
        arbitrum_wallet_activity_sql(addresses, activity_since) if addresses else [],
        label="arbitrum builder activity",
        columns=WALLET_ACTIVITY_COLUMNS,
        limit=args.limit,
    )
    if not addresses:
        logger.warning("no builder wallets to measure; writing empty activity")
        notes.append("no EVM wallets resolved for the seeded builders")

    builder_activity = _build_activity(activity, wallets)
    watermark = max_timestamp(builder_activity["last_tx_at"])

    writer.write("builder_wallets", wallets[WALLETS_CSV_COLUMNS])
    writer.write("builder_activity", builder_activity)
    writer.finish(
        params={
            "chain_id": CHAIN_ARBITRUM,
            "activity_since": activity_since.isoformat() if activity_since else None,
            "seeded_fids": len(fids),
            "wallets": int(len(wallets)),
            "full_recompute": args.since is None,
            "limit": args.limit,
            "dune": dune.summary(),
        },
        since=window.since,
        new_watermark=watermark,
        notes=notes,
    )
    if not args.dry_run:
        set_watermark(PIPELINE, watermark, run_ts=writer.run_ts)

    return {
        "run_ts": writer.run_ts,
        "seeded_fids": len(fids),
        "wallets": int(len(wallets)),
        "active_wallets": int(len(builder_activity)),
        "watermark": watermark.isoformat() if watermark else None,
    }


def _build_activity(activity: pd.DataFrame, wallets: pd.DataFrame) -> pd.DataFrame:
    """Attach fids to the Dune activity rows, one row per (fid, address)."""
    if activity.empty or wallets.empty:
        return pd.DataFrame(columns=ACTIVITY_CSV_COLUMNS)
    frame = activity.copy()
    frame["address"] = frame["address"].astype(str).str.strip().str.lower()
    # An inner merge is right here even though it can fan out: a shared address
    # genuinely belongs to each fid that verified it, and the graph wants both.
    frame = frame.merge(wallets[WALLETS_CSV_COLUMNS], on="address", how="inner")
    if frame.empty:
        return pd.DataFrame(columns=ACTIVITY_CSV_COLUMNS)
    frame["chain_id"] = CHAIN_ARBITRUM
    frame["tx_count"] = pd.to_numeric(frame["tx_count"], errors="coerce").fillna(0).astype("int64")
    frame["first_tx_at"] = iso_timestamps(frame["first_tx_at"])
    frame["last_tx_at"] = iso_timestamps(frame["last_tx_at"])
    frame = frame.drop_duplicates(subset=["fid", "address"])
    return frame[ACTIVITY_CSV_COLUMNS].reset_index(drop=True)


def main(argv=None) -> int:
    parser = base_parser(PIPELINE, "Arbitrum activity for the seeded Farcaster miniapp builders.")
    args = parser.parse_args(argv)
    setup_logging(args.log_level)
    window = resolve_window(args, PIPELINE)
    try:
        result = run(window, args)
    except (SeedMissingError, ValueError) as exc:
        # The seed file is an operator input, so a missing or malformed one is a
        # configuration problem to be told about, not a stack trace.
        logger.error("%s: %s", PIPELINE, exc)
        return 2
    logger.info("%s done: %s", PIPELINE, result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
