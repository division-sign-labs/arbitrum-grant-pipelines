"""Every Farcaster account and the wallets it has verified — the project's base table.

Produces:
  wallets.csv   fid,address,protocol,is_primary,source
  accounts.csv  fid,username,display_name,neynar_score,follower_count,
                following_count,custody_address,registered_at

Why Neynar and nothing else: the fid -> address mapping is the join key for every
other pipeline here, and Dune does not carry it. The `dune.neynar.dataset_farcaster_*`
tables do not exist with our key (all six were probed and fail), and there is no
other Farcaster casts/verifications table on Dune at all. So the only way to get
this is to enumerate the fid space through the Neynar API.

Why enumeration rather than a query: /user/bulk takes 100 fids per call and there
is no "list all users" endpoint. fids are allocated monotonically from 1, so
walking 1..tip in batches of 100 visits every account exactly once. The tip sits
around 3.35M (measured 2026-08-09), so a full backfill is ~33.5k calls; at the
300 rpm rate limit that is a little over two hours.

Because it is a two-hour crawl, it is built to survive being killed:
  * rows are appended and flushed per batch, not buffered to the end
  * a checkpoint ({'scan_cursor', 'run_ts', 'max_fid'}) is written to
    state/linked_wallets.json after every batch
  * --resume picks the cursor back up and keeps writing into the *same*
    incomplete run directory, which has no manifest yet and is therefore
    invisible to downstream readers until it is finished
A kill between flushing a batch and checkpointing it re-fetches that batch on
resume, duplicating at most 100 accounts' rows. Ingestion MERGEs on (fid, address),
so the duplicates collapse rather than double-count.

Stopping: past the tip, /user/bulk returns HTTP 404 rather than an empty list
(verified). lib.fid_resolver.fetch_users turns that into an empty batch, and the
scan stops after FID_SCAN_EMPTY_BATCH_STOP consecutive empty batches so a small
gap in the fid space cannot end the run early.

Incremental mode scans from the highest fid seen last time upward, which is sound
only because fid allocation is monotonic — a new account always gets a new, higher
fid. OUT OF SCOPE: re-resolving accounts already captured. If an existing account
verifies a new wallet, changes username, or its Neynar score moves, this pipeline
will not notice; only a fresh --backfill re-reads them. That is a deliberate cost
trade (two hours per full refresh), not an oversight.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Sequence

from config.settings import FID_SCAN_CEILING, FID_SCAN_EMPTY_BATCH_STOP, NEYNAR_FID_BATCH
from lib.cli import base_parser, resolve_window
from lib.fid_resolver import ACCOUNT_COLUMNS, WALLET_COLUMNS, resolve_fid_wave
from lib.logging_utils import setup_logging
from lib.neynar import NeynarClient
from lib.runs import RunWriter, incomplete_runs
from lib.state import parse_ts, read_state, set_watermark, write_state

logger = logging.getLogger(__name__)

PIPELINE = "linked_wallets"
PROGRESS_EVERY_BATCHES = 25


def _scan_bounds(args, state: dict, window) -> tuple[int, int, str]:
    """First and last fid to scan (inclusive), and where the start came from."""
    ceiling = int(args.max_fid) if args.max_fid else FID_SCAN_CEILING

    if args.start_fid is not None:
        start, source = int(args.start_fid), "flag"
    elif args.resume and state.get("scan_cursor"):
        start, source = int(state["scan_cursor"]), "resume"
    elif window.is_backfill:
        start, source = 1, "backfill"
    else:
        # Monotonic allocation means everything at or below max_fid is already
        # captured; only the gap between it and the tip is new.
        previous = int(state.get("max_fid") or 0)
        start, source = previous + 1, "incremental"
        if previous == 0:
            logger.warning(
                "no max_fid in state — incremental run will rescan from fid 1. "
                "Run --backfill instead if this is a first run."
            )

    end = int(args.end_fid) if args.end_fid is not None else ceiling
    if args.limit:
        end = min(end, start + int(args.limit) - 1)
    return start, end, source


def _open_writer(args, state: dict) -> RunWriter:
    """A fresh run, or the in-flight one --resume is continuing."""
    if not args.resume:
        return RunWriter(PIPELINE, dry_run=args.dry_run)

    run_ts = state.get("run_ts")
    if not run_ts:
        raise SystemExit(
            "--resume: no in-flight scan recorded in state/linked_wallets.json.\n"
            "Start one with `python -m pipelines.linked_wallets --backfill`."
        )
    if run_ts not in {path.name for path in incomplete_runs(PIPELINE)}:
        raise SystemExit(
            f"--resume: run {run_ts} has no resumable directory (it may have "
            f"finished already, in which case just run without --resume)."
        )
    logger.info("resuming run %s at fid %s", run_ts, state.get("scan_cursor"))
    return RunWriter(PIPELINE, dry_run=args.dry_run, run_ts=run_ts)


def _merge_state(updates: dict, drop: Sequence[str] = ()) -> dict:
    """Apply updates to the state file without clobbering keys we did not set.

    The scan holds its state for hours. Writing an in-memory copy back wholesale
    would erase anything written meanwhile — most importantly the watermark, which
    a previous run may have set after this one read the file.
    """
    state = read_state(PIPELINE)
    state.update(updates)
    for key in drop:
        state.pop(key, None)
    write_state(PIPELINE, state)
    return state


def _checkpoint(cursor: int, run_ts: str, max_fid: int) -> None:
    """Persist scan position after every batch so a kill costs one batch at most."""
    _merge_state({"scan_cursor": cursor, "run_ts": run_ts, "max_fid": max_fid})


def _open_outputs(writer: RunWriter) -> None:
    """Open both CSVs for streaming appends and register them in the manifest.

    RunWriter only records a file once a row is appended to it, so a scan that
    finds nothing would seal a manifest reporting zero files while header-only
    CSVs sit on disk. Ingestion reads manifest['files'], so claim both up front
    and let append_row count up from there.
    """
    for name, columns in (("wallets", WALLET_COLUMNS), ("accounts", ACCOUNT_COLUMNS)):
        writer.open_append(name, columns)
        if not writer.dry_run:
            writer.files.setdefault(name, 0)


def run(window, args) -> dict:
    state = read_state(PIPELINE)
    start, end, start_source = _scan_bounds(args, state, window)
    writer = _open_writer(args, state)

    total_fids = max(end - start + 1, 0)
    batches = -(-total_fids // NEYNAR_FID_BATCH)
    params = {
        "start_fid": start,
        "end_fid": end,
        "start_source": start_source,
        "batch_size": NEYNAR_FID_BATCH,
        "empty_batch_stop": FID_SCAN_EMPTY_BATCH_STOP,
        "limit": args.limit,
        "resumed": bool(args.resume),
    }

    if total_fids <= 0:
        logger.info("nothing to scan: start fid %d is past end fid %d", start, end)
        writer.finish(
            params=params,
            since=window.since,
            notes=["no fids in range; the scan is already at or past the tip"],
        )
        return {"fids_scanned": 0, "accounts": 0, "wallets": 0, "run_ts": writer.run_ts}

    if args.dry_run:
        # Plan only. Spending 33k API calls to describe a plan defeats the point.
        logger.info(
            "[dry-run] would scan fids %d..%d (%d fids, %d calls, ~%.1f min at %d rpm)",
            start,
            end,
            total_fids,
            batches,
            batches / 300.0,
            300,
        )
        logger.info("[dry-run] wallets.csv columns: %s", WALLET_COLUMNS)
        logger.info("[dry-run] accounts.csv columns: %s", ACCOUNT_COLUMNS)
        writer.finish(params=params, since=window.since, notes=["dry run: no API calls made"])
        return {
            "fids_scanned": 0,
            "accounts": 0,
            "wallets": 0,
            "planned_calls": batches,
            "run_ts": writer.run_ts,
        }

    client = NeynarClient()
    _open_outputs(writer)

    max_fid_seen = int(state.get("max_fid") or 0)
    newest_registration: datetime | None = None
    consecutive_empty = 0
    fids_scanned = 0
    accounts_written = 0
    wallets_written = 0
    stopped_at_tip = False
    started = time.monotonic()

    cursor = start
    batch_index = 0
    workers = max(1, int(args.workers))
    try:
        while cursor <= end:
            # A wave of batches is issued together and consumed in order, so the
            # scan cursor still advances monotonically and a kill costs at most
            # one wave (workers x 100 fids) of re-fetching on resume.
            wave: list[list[int]] = []
            probe = cursor
            while probe <= end and len(wave) < workers:
                wave.append(list(range(probe, min(probe + NEYNAR_FID_BATCH, end + 1))))
                probe = wave[-1][-1] + 1

            for chunk, wallet_rows, account_rows in resolve_fid_wave(client, wave, workers):
                for row in wallet_rows:
                    writer.append_row("wallets", row)
                wallets_written += len(wallet_rows)

                for account in account_rows:
                    writer.append_row("accounts", account)
                    max_fid_seen = max(max_fid_seen, int(account["fid"]))
                    registered = parse_ts(account.get("registered_at"))
                    if registered and (
                        newest_registration is None or registered > newest_registration
                    ):
                        newest_registration = registered
                accounts_written += len(account_rows)

                consecutive_empty = 0 if account_rows else consecutive_empty + 1
                fids_scanned += len(chunk)
                cursor = chunk[-1] + 1
                batch_index += 1

                if consecutive_empty >= FID_SCAN_EMPTY_BATCH_STOP:
                    stopped_at_tip = True
                    break

            _checkpoint(cursor, writer.run_ts, max_fid_seen)

            if batch_index % PROGRESS_EVERY_BATCHES < workers:
                elapsed = max(time.monotonic() - started, 1e-6)
                rate = fids_scanned / elapsed
                remaining = max(end - cursor + 1, 0)
                logger.info(
                    "fid %d/%d | %d accounts, %d wallets | %.0f fids/s | ~%.0f min left",
                    cursor - 1,
                    end,
                    accounts_written,
                    wallets_written,
                    rate,
                    (remaining / rate) / 60 if rate else 0,
                )

            if stopped_at_tip:
                logger.info(
                    "stopping at fid %d: %d consecutive empty batches (past the tip)",
                    cursor - 1,
                    consecutive_empty,
                )
                break
    except KeyboardInterrupt:
        writer.close_appends()
        logger.warning(
            "interrupted at fid %d. Resume with:\n"
            "  python -m pipelines.linked_wallets --resume",
            cursor,
        )
        raise SystemExit(130)

    notes = []
    if stopped_at_tip:
        notes.append(f"reached the fid tip near {max_fid_seen}")
    if args.limit:
        notes.append(f"--limit capped the scan at {args.limit} fids")
    if not stopped_at_tip and cursor > end and end < FID_SCAN_CEILING:
        notes.append(f"stopped at the requested end fid {end}, not the tip")

    params["max_fid"] = max_fid_seen
    params["fids_scanned"] = fids_scanned
    writer.finish(
        params=params,
        since=window.since,
        new_watermark=newest_registration,
        notes=notes,
    )

    # The scan is done, so drop the resume pointer — a later --resume must not
    # reattach to a run that already has a manifest.
    _merge_state(
        {"max_fid": max_fid_seen, "last_scan_end": cursor - 1},
        drop=("scan_cursor", "run_ts"),
    )
    set_watermark(PIPELINE, newest_registration, run_ts=writer.run_ts)

    logger.info(
        "%s: %d fids scanned, %d accounts, %d wallets, max_fid=%d",
        PIPELINE,
        fids_scanned,
        accounts_written,
        wallets_written,
        max_fid_seen,
    )
    return {
        "fids_scanned": fids_scanned,
        "accounts": accounts_written,
        "wallets": wallets_written,
        "max_fid": max_fid_seen,
        "run_ts": writer.run_ts,
    }


def main(argv=None) -> int:
    parser = base_parser(PIPELINE, __doc__.splitlines()[0])
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Continue the last interrupted scan into its existing run directory.",
    )
    parser.add_argument(
        "--start-fid",
        type=int,
        default=None,
        help="First fid to scan; overrides the resume cursor and the watermark.",
    )
    parser.add_argument(
        "--end-fid",
        type=int,
        default=None,
        help="Last fid to scan, inclusive. Stops there regardless of the tip.",
    )
    parser.add_argument(
        "--max-fid",
        type=int,
        default=None,
        help=f"Override the scan ceiling (default FID_SCAN_CEILING={FID_SCAN_CEILING}).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=6,
        help=(
            "Batches in flight at once. The shared rate limiter still caps the "
            "request rate; this only stops round-trip latency from wasting it. "
            "1 makes the scan strictly sequential."
        ),
    )
    args = parser.parse_args(argv)
    setup_logging(args.log_level)

    # --resume continues a scan that already knows its own start, so it does not
    # need a watermark to exist; neither does an explicit --start-fid.
    if (args.resume or args.start_fid is not None) and not (args.backfill or args.since):
        args.backfill = True

    window = resolve_window(args, PIPELINE)
    result = run(window, args)
    logger.info("done: %s", result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
