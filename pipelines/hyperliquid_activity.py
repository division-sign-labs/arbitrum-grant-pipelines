"""Hyperliquid usage for every wallet in the Arbitrum cohort.

WHAT it produces
    hyperliquid_activity/hl_activity.csv — one row per cohort wallet:
    address, fid, has_hl_activity, cum_volume_usd, first_activity_at,
    ledger_event_count, checked_at. It answers "which Arbitrum-native Farcaster
    builders and traders also use Hyperliquid, how much have they traded, and
    since when".

WHY it is built this way
    Hyperliquid has no bulk endpoint. Everything is per-user /info POSTs, and
    lib.hyperliquid needs two of them per wallet (userRateLimit for lifetime
    volume, userNonFundingLedgerUpdates for first touch). The documented budget is
    1200 weight/min at 20 weight per call, and we pace to 80% of it: ~48 calls/min
    = ~24 wallets/min. A 10k-wallet cohort is therefore ~7 hours of wall clock.

    At that duration a crash, a laptop lid, or a Ctrl-C is not an edge case, it is
    the expected case. So this pipeline is built around resumption rather than
    retrofitted with it:
      * every wallet is flushed to the CSV the instant its two calls return
        (RunWriter.open_append / append_row), so nothing is ever lost;
      * the manifest is written only at the end, which is what makes an
        interrupted directory discoverable via lib.runs.incomplete_runs and
        invisible to downstream readers;
      * state/hyperliquid_activity.json tracks the in-flight run so the operator
        can see where it got to without parsing CSVs;
      * --resume re-opens the newest incomplete run and skips the addresses its
        CSV already holds.

    --recheck-days makes repeat runs cheap: a wallet checked inside the window is
    copied forward from the previous completed run with its original checked_at
    instead of being re-fetched, so each completed run is still a full snapshot of
    the cohort but only the stale part costs API calls.

There is no watermark: this is a per-wallet crawl over a derived cohort, not a
time-windowed extract. `checked_at` in the previous run is the freshness signal,
and --recheck-days is the knob.
"""

from __future__ import annotations

import csv
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

from config.settings import (
    HYPERLIQUID_CALL_WEIGHT,
    HYPERLIQUID_SAFETY_FACTOR,
    HYPERLIQUID_WEIGHT_PER_MINUTE,
)
from lib import runs
from lib.cli import base_parser
from lib.hyperliquid import HyperliquidClient
from lib.logging_utils import setup_logging
from lib.runs import RunWriter, utc_now
from lib.state import parse_ts, read_state, write_state

logger = logging.getLogger(__name__)

PIPELINE = "hyperliquid_activity"
COHORT_PIPELINE = "arb_cohort"
CSV_NAME = "hl_activity"

# Ingestion reads these exact names in this exact order.
COLUMNS = [
    "address",
    "fid",
    "has_hl_activity",
    "cum_volume_usd",
    "first_activity_at",
    "ledger_event_count",
    "checked_at",
]

CALLS_PER_WALLET = 2
PROGRESS_EVERY = 50


def wallets_per_minute() -> float:
    """Sustained throughput implied by the rate limit lib.hyperliquid paces to."""
    calls_per_minute = (
        HYPERLIQUID_WEIGHT_PER_MINUTE / HYPERLIQUID_CALL_WEIGHT
    ) * HYPERLIQUID_SAFETY_FACTOR
    return calls_per_minute / CALLS_PER_WALLET


@dataclass(frozen=True)
class Target:
    address: str
    fid: int | None
    priority: int


# --- cohort input --------------------------------------------------------


def load_cohort(args) -> tuple[list[Target], str]:
    """The cohort in priority order: most specific wallets get crawled first."""
    try:
        run_dir = runs.resolve_run(COHORT_PIPELINE, args.cohort_run_id)
    except FileNotFoundError as exc:
        raise SystemExit(
            f"{exc}\nThe Hyperliquid crawl is driven off arb_cohort/cohort.csv."
        ) from exc

    cohort = runs.read_csv(COHORT_PIPELINE, "cohort", run_id=run_dir.name, required=False)
    if cohort.empty:
        logger.warning("cohort run %s has no wallets", run_dir.name)
        return [], run_dir.name

    cohort["priority"] = pd.to_numeric(cohort["priority"], errors="coerce").fillna(99).astype(int)
    if args.max_priority is not None:
        before = len(cohort)
        cohort = cohort[cohort["priority"] <= args.max_priority]
        logger.info(
            "--max-priority %d kept %d/%d cohort wallets", args.max_priority, len(cohort), before
        )
    if args.min_score is not None:
        before = len(cohort)
        scored = (
            pd.to_numeric(cohort["neynar_score"], errors="coerce")
            if "neynar_score" in cohort.columns
            else pd.Series(float("nan"), index=cohort.index)
        )
        cohort = cohort[scored >= args.min_score]
        logger.info(
            "--min-score %s kept %d/%d cohort wallets", args.min_score, len(cohort), before
        )

    cohort = cohort.sort_values(["priority"], kind="stable")
    fids = pd.to_numeric(cohort["fid"], errors="coerce")
    targets: list[Target] = []
    seen: set[str] = set()
    for address, fid, priority in zip(cohort["address"], fids, cohort["priority"]):
        key = str(address).strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        targets.append(
            Target(key, None if pd.isna(fid) else int(fid), int(priority))
        )
    return targets, run_dir.name


# --- previous results ----------------------------------------------------


def read_rows(path: Path) -> pd.DataFrame:
    """Read an hl_activity.csv, discarding anything a hard kill left half-written."""
    if not path.exists():
        return pd.DataFrame(columns=COLUMNS)
    try:
        frame = pd.read_csv(path, on_bad_lines="skip")
    except pd.errors.EmptyDataError:
        return pd.DataFrame(columns=COLUMNS)
    return drop_torn_rows(frame, path)


def drop_torn_rows(frame: pd.DataFrame, path: Path) -> pd.DataFrame:
    """Keep only rows that were written whole.

    on_bad_lines="skip" is not enough: a row cut short has *fewer* fields than the
    header, which the parser happily pads with NaN instead of rejecting. A row is
    only real if it still has a well-formed address and the checked_at that is
    written last, so those two together identify a torn tail whether the cut
    landed mid-address or mid-row.
    """
    if frame.empty or "address" not in frame.columns or "checked_at" not in frame.columns:
        return pd.DataFrame(columns=COLUMNS)
    address_ok = (
        frame["address"]
        .astype("string")
        .str.strip()
        .str.lower()
        .str.fullmatch(r"0x[0-9a-f]{40}")
        .fillna(False)
    )
    checked_ok = frame["checked_at"].astype("string").str.strip().fillna("").ne("")
    good = frame[address_ok & checked_ok]
    dropped = len(frame) - len(good)
    if dropped:
        logger.warning("discarded %d half-written row(s) from %s", dropped, path)
    return good


def previous_results() -> tuple[dict[str, dict], str | None]:
    """address -> row from the most recent completed run, for the recheck window."""
    run_dir = runs.latest_run(PIPELINE)
    if run_dir is None:
        return {}, None
    frame = read_rows(run_dir / f"{CSV_NAME}.csv")
    if frame.empty:
        return {}, run_dir.name
    frame = frame.drop_duplicates(subset=["address"], keep="last")
    rows = {
        str(row["address"]).strip().lower(): row
        for row in frame.to_dict("records")
        if str(row.get("address", "")).strip()
    }
    logger.info("previous completed run %s holds %d wallets", run_dir.name, len(rows))
    return rows, run_dir.name


def resumable_run() -> Path | None:
    """Newest directory with rows but no manifest — an interrupted crawl."""
    candidates = runs.incomplete_runs(PIPELINE)
    return candidates[-1] if candidates else None


def fresh_run_ts() -> str:
    """A run_ts no directory has claimed yet.

    run_ts has one-second resolution and this pipeline appends rather than
    truncates, so two runs started inside the same second would stack their rows
    on top of an already-sealed run's CSV. Stepping forward a second is enough:
    only --resume is allowed to reuse an existing directory.
    """
    ts = runs.new_run_ts()
    root = Path(runs.DATA_DIR) / PIPELINE
    while (root / ts).exists():
        moment = datetime.strptime(ts, runs.RUN_TS_FORMAT).replace(tzinfo=timezone.utc)
        ts = (moment + timedelta(seconds=1)).strftime(runs.RUN_TS_FORMAT)
    return ts


def rewrite_clean(path: Path, frame: pd.DataFrame) -> int:
    """Rewrite a resumed CSV from its parseable rows so appends stay well-formed.

    A process killed between writerow and flush can leave a half line. Reading
    with on_bad_lines="skip" drops it, and rewriting removes it from the file, so
    the appender never builds on top of a torn row.
    """
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        count = 0
        for row in frame.to_dict("records"):
            writer.writerow({col: _csv_value(col, row.get(col)) for col in COLUMNS})
            count += 1
    return count


# Columns that must round-trip as integers: pandas reads a column holding blanks
# as float64, which would otherwise turn fid 123 into "123.0" and break the
# WarpcastAccount MERGE key. cum_volume_usd is deliberately not in here — it is a
# real float and must stay one.
INT_COLUMNS = frozenset({"fid", "ledger_event_count"})


def _csv_value(column: str, value):
    if value is None or value is pd.NA or (isinstance(value, float) and pd.isna(value)):
        return ""
    if column in INT_COLUMNS and isinstance(value, float):
        return int(value)
    return value


# --- planning ------------------------------------------------------------


@dataclass
class Plan:
    fetch: list[Target]
    carry_forward: list[tuple[Target, dict]]
    already_done: int
    cohort_size: int
    truncated_by_limit: int


def build_plan(targets: list[Target], done: set[str], prior: dict[str, dict],
               recheck_days: int, limit: int | None) -> Plan:
    cutoff = utc_now() - timedelta(days=max(recheck_days, 0))
    fetch: list[Target] = []
    carry: list[tuple[Target, dict]] = []
    already = 0
    for target in targets:
        if target.address in done:
            already += 1
            continue
        row = prior.get(target.address)
        checked_at = parse_ts(row.get("checked_at")) if row is not None else None
        if checked_at is not None and checked_at >= cutoff:
            carry.append((target, row))
        else:
            fetch.append(target)
    truncated = 0
    if limit is not None and limit < len(fetch):
        truncated = len(fetch) - limit
        fetch = fetch[:limit]
    return Plan(fetch, carry, already, len(targets), truncated)


# --- crawling ------------------------------------------------------------


def format_duration(seconds: float) -> str:
    seconds = max(int(seconds), 0)
    hours, rest = divmod(seconds, 3600)
    minutes, secs = divmod(rest, 60)
    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def to_row(target: Target, summary: dict) -> dict:
    return {
        "address": summary.get("address", target.address),
        "fid": "" if target.fid is None else target.fid,
        "has_hl_activity": bool(summary.get("has_hl_activity")),
        "cum_volume_usd": summary.get("cum_volume_usd", 0.0),
        "first_activity_at": summary.get("first_activity_at") or "",
        "ledger_event_count": summary.get("ledger_event_count", 0),
        "checked_at": summary.get("checked_at"),
    }


def carried_row(target: Target, row: dict) -> dict:
    """Copy a still-fresh result forward, keeping its original checked_at."""
    out = {col: _csv_value(col, row.get(col)) for col in COLUMNS}
    out["address"] = target.address
    # The cohort is the authority on identity; a fid can appear after the wallet
    # was first crawled, so take it from this run rather than the old row.
    out["fid"] = "" if target.fid is None else target.fid
    return out


def checkpoint(run_ts: str, cohort_run: str, completed: int, total: int, active: bool) -> None:
    state = read_state(PIPELINE)
    if active:
        state["in_progress"] = {
            "run_ts": run_ts,
            "cohort_run": cohort_run,
            "completed": completed,
            "total": total,
            "updated_at": utc_now().isoformat(),
        }
    else:
        state.pop("in_progress", None)
        state["last_run_ts"] = run_ts
        state["last_completed_at"] = utc_now().isoformat()
        state["wallets_in_last_run"] = completed
    write_state(PIPELINE, state)


def crawl(writer: RunWriter, plan: Plan, cohort_run: str) -> dict:
    """Fetch every stale wallet, flushing each result before the next call."""
    client = HyperliquidClient()
    total = len(plan.fetch)
    done = 0
    active = 0
    failures: list[str] = []
    started = time.monotonic()
    interrupted = False

    # The interrupt guard wraps the whole loop, not just the HTTP call: a Ctrl-C
    # landing between the append and the next request must still exit through the
    # resumable path rather than as a traceback.
    try:
        for target in plan.fetch:
            try:
                summary = client.wallet_summary(target.address)
            except (RuntimeError, requests.RequestException, ValueError, TypeError) as exc:
                # No error column in the CSV contract, and a missing row is the
                # honest encoding of "not checked": the wallet stays stale and the
                # next run picks it up again.
                failures.append(target.address)
                logger.warning("hyperliquid lookup failed for %s: %s", target.address, exc)
                continue
            writer.append_row(CSV_NAME, to_row(target, summary))
            done += 1
            if summary.get("has_hl_activity"):
                active += 1
            if done % PROGRESS_EVERY == 0:
                elapsed = time.monotonic() - started
                rate = done / elapsed if elapsed > 0 else 0.0
                remaining = total - done - len(failures)
                eta = remaining / rate if rate > 0 else 0.0
                logger.info(
                    "%d/%d fetched (%.1f wallets/min, %d with HL activity, %d failed), eta %s",
                    done,
                    total,
                    rate * 60,
                    active,
                    len(failures),
                    format_duration(eta),
                )
                checkpoint(
                    writer.run_ts,
                    cohort_run,
                    done + len(plan.carry_forward) + plan.already_done,
                    plan.cohort_size,
                    active=True,
                )
    except KeyboardInterrupt:
        interrupted = True

    elapsed = time.monotonic() - started
    return {
        "fetched": done,
        "with_activity": active,
        "failures": failures,
        "elapsed_seconds": round(elapsed, 1),
        "interrupted": interrupted,
        "http_requests": client.http.request_count,
    }


# --- run -----------------------------------------------------------------


def run(args) -> dict:
    targets, cohort_run = load_cohort(args)
    prior, prior_run = previous_results()

    resume_dir = None
    done: set[str] = set()
    if args.resume:
        resume_dir = resumable_run()
        if resume_dir is None:
            logger.warning("--resume: no incomplete run found, starting a fresh one")
        else:
            existing = read_rows(resume_dir / f"{CSV_NAME}.csv")
            existing = existing.drop_duplicates(subset=["address"], keep="last") if not existing.empty else existing
            if not existing.empty and not args.dry_run:
                kept = rewrite_clean(resume_dir / f"{CSV_NAME}.csv", existing)
                logger.info("resuming run %s with %d rows already written", resume_dir.name, kept)
            else:
                logger.info("resuming run %s (%d rows already written)", resume_dir.name, len(existing))
            done = {
                str(a).strip().lower()
                for a in existing.get("address", pd.Series(dtype="object"))
                if str(a).strip()
            }
            recorded = read_state(PIPELINE).get("in_progress", {}).get("cohort_run")
            if recorded and recorded != cohort_run:
                logger.warning(
                    "resumed run was started against cohort %s but the current cohort is %s; "
                    "new wallets will be appended and dropped ones simply left in place",
                    recorded,
                    cohort_run,
                )

    plan = build_plan(targets, done, prior, args.recheck_days, args.limit)
    projected = len(plan.fetch) / wallets_per_minute() * 60 if plan.fetch else 0.0
    logger.info(
        "plan: %d cohort wallets | %d to fetch | %d fresh within %dd (carried forward) | "
        "%d already in this run | projected %s at %.0f wallets/min",
        plan.cohort_size,
        len(plan.fetch),
        len(plan.carry_forward),
        args.recheck_days,
        plan.already_done,
        format_duration(projected),
        wallets_per_minute(),
    )
    if plan.truncated_by_limit:
        logger.info(
            "--limit %d leaves %d stale wallets for a later run", args.limit, plan.truncated_by_limit
        )

    params = {
        "cohort_run": cohort_run,
        "previous_run": prior_run,
        "resumed_run": resume_dir.name if resume_dir else None,
        "max_priority": args.max_priority,
        "min_score": args.min_score,
        "recheck_days": args.recheck_days,
        "limit": args.limit,
        "cohort_wallets": plan.cohort_size,
        "to_fetch": len(plan.fetch),
        "carried_forward": len(plan.carry_forward),
    }

    if args.dry_run:
        logger.info(
            "[dry-run] would spend %d Hyperliquid calls (%d wallets x %d) over ~%s; nothing written",
            len(plan.fetch) * CALLS_PER_WALLET,
            len(plan.fetch),
            CALLS_PER_WALLET,
            format_duration(projected),
        )
        RunWriter(PIPELINE, dry_run=True).finish(params=params, since=None, new_watermark=None)
        return {
            "dry_run": True,
            "cohort_wallets": plan.cohort_size,
            "to_fetch": len(plan.fetch),
            "carried_forward": len(plan.carry_forward),
            "projected_seconds": round(projected),
        }

    writer = RunWriter(PIPELINE, run_ts=resume_dir.name if resume_dir else fresh_run_ts())
    writer.open_append(CSV_NAME, COLUMNS)
    # Register the file even if the run turns out to have nothing to write, so the
    # manifest always names hl_activity.csv for ingestion to find.
    writer.files.setdefault(CSV_NAME, 0)
    for target, row in plan.carry_forward:
        writer.append_row(CSV_NAME, carried_row(target, row))
    if plan.carry_forward:
        logger.info(
            "carried %d results forward from run %s (checked within %d days)",
            len(plan.carry_forward),
            prior_run,
            args.recheck_days,
        )
    checkpoint(writer.run_ts, cohort_run, len(plan.carry_forward) + plan.already_done,
               plan.cohort_size, active=True)

    result = crawl(writer, plan, cohort_run)

    if result["interrupted"]:
        # No manifest: the directory stays invisible to readers and is exactly what
        # --resume will pick up next time.
        writer.close_appends()
        checkpoint(writer.run_ts, cohort_run,
                   result["fetched"] + len(plan.carry_forward) + plan.already_done,
                   plan.cohort_size, active=True)
        logger.warning(
            "interrupted after %d wallets; run %s left unsealed — resume with "
            "`python -m pipelines.%s --resume`",
            result["fetched"],
            writer.run_ts,
            PIPELINE,
        )
        return {"run_ts": writer.run_ts, "interrupted": True, **{k: result[k] for k in ("fetched", "with_activity")}}

    notes = []
    if result["failures"]:
        notes.append(
            f"{len(result['failures'])} wallets failed after retries and were left "
            f"unchecked for the next run: {', '.join(result['failures'][:10])}"
            + (" ..." if len(result["failures"]) > 10 else "")
        )
    if plan.truncated_by_limit:
        notes.append(f"--limit left {plan.truncated_by_limit} stale wallets unchecked")
    if plan.carry_forward:
        notes.append(
            f"{len(plan.carry_forward)} rows carried forward unchanged from run {prior_run}"
        )

    params.update(
        {
            "fetched": result["fetched"],
            "with_activity": result["with_activity"],
            "failed": len(result["failures"]),
            "elapsed_seconds": result["elapsed_seconds"],
            "http_requests": result["http_requests"],
        }
    )
    writer.finish(params=params, since=None, new_watermark=None, notes=notes)
    checkpoint(writer.run_ts, cohort_run, writer.files.get(CSV_NAME, 0), plan.cohort_size, active=False)
    logger.info(
        "hyperliquid: %d fetched (%d active), %d carried forward, %d failed in %s",
        result["fetched"],
        result["with_activity"],
        len(plan.carry_forward),
        len(result["failures"]),
        format_duration(result["elapsed_seconds"]),
    )
    return {
        "run_ts": writer.run_ts,
        "rows": writer.files.get(CSV_NAME, 0),
        "fetched": result["fetched"],
        "with_activity": result["with_activity"],
        "carried_forward": len(plan.carry_forward),
        "failed": len(result["failures"]),
    }


def main(argv=None) -> int:
    parser = base_parser(
        PIPELINE,
        "Crawl Hyperliquid /info for every wallet in the Arbitrum cohort "
        f"(~{wallets_per_minute():.0f} wallets/min, 2 calls each). Resumable.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Continue the newest unsealed run instead of starting a new one, "
        "skipping every address already in its CSV.",
    )
    parser.add_argument(
        "--max-priority",
        type=int,
        default=None,
        help="Only crawl cohort wallets at least this specific (1 deployers … 6 popular-token "
        "traders). The cheapest way to keep a run short and still cover the builders.",
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=None,
        help="Only crawl wallets whose Neynar 0-1 account score is at least this.",
    )
    parser.add_argument(
        "--recheck-days",
        type=int,
        default=30,
        help="Re-check a wallet only if the previous completed run checked it more than "
        "this many days ago; fresher results are copied forward (default: 30, 0 rechecks all).",
    )
    parser.add_argument(
        "--cohort-run-id",
        default=None,
        help="arb_cohort run to drive from (default: the latest completed one).",
    )
    args = parser.parse_args(argv)
    setup_logging(args.log_level)
    if args.since or args.backfill:
        logger.info(
            "--since/--backfill are ignored: this crawls a wallet list, not a time window. "
            "Use --recheck-days to control how much gets re-fetched."
        )
    summary = run(args)
    logger.info("%s done: %s", PIPELINE, summary)
    return 130 if summary.get("interrupted") else 0


if __name__ == "__main__":
    raise SystemExit(main())
