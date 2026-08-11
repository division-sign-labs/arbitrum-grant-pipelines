"""Arbitrum contract deployers who are also Farcaster accounts.

WHAT this produces, under data/contract_deployers/<run_ts>/:
  deployments.csv        one row per contract created by a Farcaster-linked
                         wallet, with how it was created (direct vs via a
                         factory the wallet's transaction invoked);
  deployer_activity.csv  that wallet's overall Arbitrum transaction footprint.

WHY these sources: `arbitrum.creation_traces` on Dune is the only complete
record of contract creations on Arbitrum, including the ones a factory performs
internally that never appear as a `to = null` transaction. Dune has no
Farcaster tables at all with this key, and this account may only publish PUBLIC
uploads, so the wallet set cannot be shipped there to join against. Instead the
whole distinct-deployer roll-up (~373k addresses since 2025-01-01) is pulled
once and intersected with the linked-wallet set locally in pandas; only the
matched addresses — a far smaller set — are queried for detail.

Two different windows are in play on purpose:
  - deployment rows are windowed by the run's watermark, because they are
    append-only events that ingestion MERGEs on tx hash;
  - `deployer_activity` is recomputed from BACKFILL_START every run, because it
    feeds the singleton (Wallet)-[:ACTIVE_ON]->(Chain) edge that ingestion
    overwrites. A window-scoped tx_count would replace a lifetime figure with a
    partial one. Pass --since explicitly to override that and narrow it.

`--limit N` caps rows on every Dune query and the number of matched addresses
carried forward. It makes for a cheap smoke test, but a truncated deployer
roll-up will usually intersect no Farcaster wallets at all, so expect empty
outputs from a limited run.
"""

from __future__ import annotations

import logging
import re
from typing import Any

import pandas as pd

from config.settings import BACKFILL_START, CHAIN_ARBITRUM
from lib.cli import base_parser, resolve_window
from lib.dune import DuneRunner
from lib.logging_utils import setup_logging
from lib.runs import RunWriter, read_csv
from lib.state import max_timestamp, parse_ts, set_watermark
from sql.deployers import (
    DEPLOYER_SUMMARY_COLUMNS,
    DEPLOYMENT_COLUMNS,
    WALLET_ACTIVITY_COLUMNS,
    arbitrum_deployers_sql,
    arbitrum_deployments_sql,
    arbitrum_wallet_activity_sql,
    run_chunked,
)

logger = logging.getLogger(__name__)

PIPELINE = "contract_deployers"
DATA_TYPE = "contract_deployers"

DEPLOYMENTS_CSV_COLUMNS = [
    "fid",
    "deployer_address",
    "contract_address",
    "chain_id",
    "deployed_at",
    "tx_hash",
    "deploy_method",
]
ACTIVITY_CSV_COLUMNS = [
    "fid",
    "address",
    "chain_id",
    "tx_count",
    "first_tx_at",
    "last_tx_at",
]

_EVM_ADDRESS = re.compile(r"^0x[0-9a-fA-F]{40}$")
# Candidate address column names, in preference order, for whatever shape the
# wallet map arrives in.
_ADDRESS_COLUMNS = ("address", "wallet", "wallet_address", "eth_address", "deployer_address")


# --- shared helpers (miniapp_builders imports these) -----------------------


def is_evm_address(value: Any) -> bool:
    """True for a 20-byte hex address. Also the filter that drops Solana rows."""
    return isinstance(value, str) and bool(_EVM_ADDRESS.match(value.strip()))


def iso_timestamps(series: pd.Series) -> pd.Series:
    """Normalise Dune's `2025-01-01 00:00:00.000 UTC` strings to ISO-8601 UTC.

    Dune's CSV encoding varies by column type and result size; ingestion should
    not have to guess, so every timestamp leaves this repo in one format.
    """
    if series.empty:
        return series.astype(object)

    def _one(value):
        parsed = parse_ts(value)
        return parsed.isoformat() if parsed else None

    return series.map(_one)


def _frame_from_mapping(mapping: dict) -> pd.DataFrame | None:
    """Coerce {address: fid} or {fid: address(es)} into a fid/address frame."""
    rows: list[dict] = []
    for key, value in mapping.items():
        if is_evm_address(key):
            rows.append({"fid": value, "address": key})
            continue
        values = value if isinstance(value, (list, tuple, set)) else [value]
        for item in values:
            if isinstance(item, dict):
                item = next(
                    (item[c] for c in _ADDRESS_COLUMNS if c in item), None
                )
            rows.append({"fid": key, "address": item})
    return pd.DataFrame(rows) if rows else None


def _normalise_wallet_frame(obj: Any) -> pd.DataFrame | None:
    """Reduce any plausible wallet-map shape to columns fid + address."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        obj = _frame_from_mapping(obj)
        if obj is None:
            return None
    if isinstance(obj, pd.Series):
        obj = obj.rename("fid").rename_axis("address").reset_index()
    if isinstance(obj, (list, tuple)):
        obj = pd.DataFrame(list(obj))
    if not isinstance(obj, pd.DataFrame) or obj.empty:
        return None

    frame = obj.copy()
    frame.columns = [str(c).strip().lower() for c in frame.columns]
    if "fid" not in frame.columns:
        return None
    address_column = next((c for c in _ADDRESS_COLUMNS if c in frame.columns), None)
    if address_column is None:
        return None

    frame = frame[["fid", address_column]].rename(columns={address_column: "address"})
    frame = frame[frame["address"].map(is_evm_address)]
    frame = frame[pd.to_numeric(frame["fid"], errors="coerce").notna()]
    if frame.empty:
        return None
    frame["fid"] = frame["fid"].astype("int64")
    frame["address"] = frame["address"].str.strip().str.lower()
    return frame.drop_duplicates(subset=["fid", "address"]).reset_index(drop=True)


def _wallets_from_resolver() -> pd.DataFrame | None:
    """Try lib.fid_resolver, which the linked_wallets pipeline owns.

    Imported defensively: that module may not exist yet, and its exact return
    shape is not this pipeline's to dictate, so anything it hands back goes
    through the same normaliser as the CSV fallback.
    """
    try:
        from lib import fid_resolver  # noqa: PLC0415 — optional dependency
    except ImportError:
        logger.debug("lib.fid_resolver not available; falling back to run CSVs")
        return None

    for name in ("load_wallet_map", "wallet_to_fid"):
        function = getattr(fid_resolver, name, None)
        if function is None:
            continue
        try:
            frame = _normalise_wallet_frame(function())
        except Exception as exc:  # a broken resolver must not block the run
            logger.warning("lib.fid_resolver.%s() failed (%s); ignoring", name, exc)
            continue
        if frame is not None:
            logger.info("wallet map from lib.fid_resolver.%s(): %d rows", name, len(frame))
            return frame
    return None


def _wallets_from_run_csv() -> pd.DataFrame:
    try:
        raw = read_csv("linked_wallets", "wallets")
    except FileNotFoundError as exc:
        raise SystemExit(
            f"{PIPELINE} needs the Farcaster wallet map and could not find one.\n"
            "Neither lib.fid_resolver nor a completed linked_wallets run is available.\n"
            "Run `python -m pipelines.linked_wallets --backfill` first.\n"
            f"({exc})"
        ) from exc
    frame = _normalise_wallet_frame(raw)
    if frame is None or frame.empty:
        raise SystemExit(
            f"{PIPELINE}: the latest linked_wallets run holds no usable EVM wallets.\n"
            "Expected wallets.csv with columns fid,address,protocol,is_primary,source.\n"
            "Re-run `python -m pipelines.linked_wallets --backfill`."
        )
    return frame


def load_farcaster_wallets(required: bool = True) -> pd.DataFrame:
    """fid + lowercase EVM address for every wallet linked to a Farcaster account.

    `required=False` is for --dry-run on a fresh checkout, where stage A has not
    run yet. Reporting the missing dependency and continuing is more useful there
    than aborting, because the rest of the plan still renders and validates.
    """
    frame = _wallets_from_resolver()
    if frame is None:
        if not required:
            try:
                frame = _wallets_from_run_csv()
            except SystemExit:
                logger.warning(
                    "no linked_wallets run yet — planning without the wallet map"
                )
                return pd.DataFrame(columns=["fid", "address"])
        else:
            frame = _wallets_from_run_csv()
    logger.info(
        "Farcaster wallet map: %d wallets across %d fids",
        len(frame),
        frame["fid"].nunique(),
    )
    return frame


def address_fid_map(wallets: pd.DataFrame) -> pd.Series:
    """address -> fid, keeping the lowest fid when an address is claimed twice.

    Farcaster enforces one verification per address, so collisions are rare and
    come from custody addresses; picking deterministically keeps one row per
    contract rather than duplicating the whole deployment history.
    """
    return wallets.groupby("address")["fid"].min()


# --- pipeline --------------------------------------------------------------


def run(window, args) -> dict:
    notes: list[str] = []
    dune = DuneRunner(dry_run=args.dry_run)
    writer = RunWriter(DATA_TYPE, dry_run=args.dry_run)

    wallets = load_farcaster_wallets(required=not args.dry_run)
    if wallets.empty and args.dry_run:
        notes.append("no linked_wallets run available; this plan is unvalidated against it")
    known_addresses = set(wallets["address"])
    to_fid = address_fid_map(wallets)

    summary = dune.run_sql(
        arbitrum_deployers_sql(window.query_since),
        label="arbitrum distinct deployers",
        limit=args.limit,
    )
    if summary.empty:
        matched = pd.DataFrame(columns=DEPLOYER_SUMMARY_COLUMNS)
    else:
        summary["deployer_address"] = summary["deployer_address"].str.strip().str.lower()
        matched = summary[summary["deployer_address"].isin(known_addresses)].copy()
    logger.info(
        "%d Arbitrum deployers in window, %d are Farcaster accounts",
        len(summary),
        len(matched),
    )

    addresses = sorted(matched["deployer_address"].unique()) if not matched.empty else []
    if args.limit and len(addresses) > args.limit:
        addresses = addresses[: args.limit]
        notes.append(f"--limit truncated the matched deployer set to {args.limit}")
    if summary.empty and not addresses:
        if args.dry_run:
            # No Dune results in a dry run, so borrow real wallets purely so the
            # chunked SQL is rendered and reviewable.
            addresses = sorted(known_addresses)[:5]
            notes.append("dry-run: chunked SQL rendered against sample linked wallets")
        elif args.limit:
            notes.append(
                "--limit truncated the deployer roll-up, so no Farcaster matches were possible"
            )

    if addresses:
        detail = run_chunked(
            dune,
            arbitrum_deployments_sql(addresses, window.query_since),
            label="arbitrum deployments",
            columns=DEPLOYMENT_COLUMNS,
            limit=args.limit,
        )
        # ACTIVE_ON is a singleton edge ingestion overwrites; see module docstring.
        activity_since = window.since if args.since else parse_ts(BACKFILL_START)
        activity = run_chunked(
            dune,
            arbitrum_wallet_activity_sql(addresses, activity_since),
            label="arbitrum deployer activity",
            columns=WALLET_ACTIVITY_COLUMNS,
            limit=args.limit,
        )
    else:
        logger.warning("no Farcaster-linked deployers matched; writing empty outputs")
        notes.append("no Farcaster-linked deployers matched in this window")
        activity_since = window.since if args.since else parse_ts(BACKFILL_START)
        detail = pd.DataFrame(columns=DEPLOYMENT_COLUMNS)
        activity = pd.DataFrame(columns=WALLET_ACTIVITY_COLUMNS)

    deployments = _build_deployments(detail, known_addresses, to_fid)
    deployer_activity = _build_activity(activity, known_addresses, to_fid)

    watermark = max_timestamp(deployments["deployed_at"])
    if watermark is None and not matched.empty:
        # Nothing new was written, but the roll-up still proves how far the
        # matched cohort's history runs; do not rescan it next time.
        watermark = max_timestamp(matched["last_deploy_at"])

    writer.write("deployments", deployments)
    writer.write("deployer_activity", deployer_activity)
    writer.finish(
        params={
            "chain_id": CHAIN_ARBITRUM,
            "deployments_since": window.query_since.isoformat(),
            "activity_since": activity_since.isoformat() if activity_since else None,
            "deployers_scanned": int(len(summary)),
            "deployers_matched": int(len(matched)),
            "addresses_queried": len(addresses),
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
        "deployers_scanned": int(len(summary)),
        "deployers_matched": int(len(matched)),
        "deployments": int(len(deployments)),
        "deployer_activity": int(len(deployer_activity)),
        "watermark": watermark.isoformat() if watermark else None,
    }


def _build_deployments(
    detail: pd.DataFrame, known_addresses: set[str], to_fid: pd.Series
) -> pd.DataFrame:
    if detail.empty:
        return pd.DataFrame(columns=DEPLOYMENTS_CSV_COLUMNS)
    frame = detail.copy()
    for column in ("deployer_address", "contract_address", "tx_hash"):
        frame[column] = frame[column].astype(str).str.strip().str.lower()
    # The SQL already restricts to the queried addresses; this only guards
    # against a null tx_sender leaking a '0xnone' through the CASE.
    frame = frame[frame["deployer_address"].isin(known_addresses)]
    frame["fid"] = frame["deployer_address"].map(to_fid)
    frame = frame[frame["fid"].notna()]
    frame["fid"] = frame["fid"].astype("int64")
    frame["chain_id"] = CHAIN_ARBITRUM
    frame["deployed_at"] = iso_timestamps(frame["deployed_at"])
    frame["deploy_method"] = frame["deploy_method"].fillna("direct")
    frame = frame.drop_duplicates(subset=["contract_address", "tx_hash"])
    return frame[DEPLOYMENTS_CSV_COLUMNS].reset_index(drop=True)


def _build_activity(
    activity: pd.DataFrame, known_addresses: set[str], to_fid: pd.Series
) -> pd.DataFrame:
    if activity.empty:
        return pd.DataFrame(columns=ACTIVITY_CSV_COLUMNS)
    frame = activity.copy()
    frame["address"] = frame["address"].astype(str).str.strip().str.lower()
    frame = frame[frame["address"].isin(known_addresses)]
    frame["fid"] = frame["address"].map(to_fid)
    frame = frame[frame["fid"].notna()]
    frame["fid"] = frame["fid"].astype("int64")
    frame["chain_id"] = CHAIN_ARBITRUM
    frame["tx_count"] = pd.to_numeric(frame["tx_count"], errors="coerce").fillna(0).astype("int64")
    frame["first_tx_at"] = iso_timestamps(frame["first_tx_at"])
    frame["last_tx_at"] = iso_timestamps(frame["last_tx_at"])
    frame = frame.drop_duplicates(subset=["fid", "address"])
    return frame[ACTIVITY_CSV_COLUMNS].reset_index(drop=True)


def main(argv=None) -> int:
    parser = base_parser(PIPELINE, "Arbitrum contract deployers linked to Farcaster accounts.")
    args = parser.parse_args(argv)
    setup_logging(args.log_level)
    window = resolve_window(args, PIPELINE)
    result = run(window, args)
    logger.info("%s done: %s", PIPELINE, result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
