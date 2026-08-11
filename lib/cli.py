"""Shared argparse surface and time-window resolution for every pipeline.

Window rules:
  --since T   -> start at T
  otherwise   -> start at the stored watermark
  --backfill  -> start at BACKFILL_START, ignoring the watermark
  neither     -> error, telling the operator to backfill first

An incremental window is widened by INCREMENTAL_OVERLAP_DAYS on the read side
because Dune's neynar datasets sync daily and chain tables trail the tip. The
extra day costs a little scan and buys correctness; MERGE absorbs the duplicates.
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

from config.settings import BACKFILL_START, INCREMENTAL_OVERLAP_DAYS
from lib.state import get_watermark, parse_ts

logger = logging.getLogger(__name__)


@dataclass
class Window:
    """The time window a run covers."""

    since: datetime  # logical start (what the operator asked for)
    query_since: datetime  # what we actually query (since - overlap)
    is_backfill: bool
    source: str  # "flag" | "watermark" | "backfill"

    def describe(self) -> str:
        return (
            f"window since={self.since.isoformat()} "
            f"query_since={self.query_since.isoformat()} "
            f"({'backfill' if self.is_backfill else 'incremental'}, from {self.source})"
        )


def base_parser(name: str, description: str = "") -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=f"python -m pipelines.{name}", description=description)
    parser.add_argument(
        "--backfill",
        action="store_true",
        help=f"Ignore the watermark and start from BACKFILL_START ({BACKFILL_START}).",
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="ISO timestamp to start from; overrides the watermark.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Render queries and report the plan without writing CSVs or spending credits.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap rows/pages fetched. Use for cheap smoke tests.",
    )
    parser.add_argument("--log-level", default=None, help="DEBUG/INFO/WARNING")
    return parser


def ingestion_parser(name: str, description: str = "") -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=f"python -m ingestion.{name}", description=description)
    parser.add_argument(
        "--run-id",
        default=None,
        help="Run directory to ingest (default: the latest completed run).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate CSVs and print the Cypher without touching Neo4j.",
    )
    parser.add_argument(
        "--batch-size", type=int, default=None, help="Rows per UNWIND batch."
    )
    parser.add_argument("--log-level", default=None, help="DEBUG/INFO/WARNING")
    return parser


def resolve_window(args, pipeline: str, state_dir=None) -> Window:
    if args.since:
        since = parse_ts(args.since)
        if since is None:
            raise SystemExit(f"--since {args.since!r} is not a parseable timestamp")
        source = "flag"
        is_backfill = False
    elif args.backfill:
        since = parse_ts(BACKFILL_START)
        source = "backfill"
        is_backfill = True
    else:
        since = get_watermark(pipeline, state_dir)
        if since is None:
            raise SystemExit(
                f"No watermark for '{pipeline}' and no --since given.\n"
                f"Run `python -m pipelines.{pipeline} --backfill` first."
            )
        source = "watermark"
        is_backfill = False

    query_since = since if is_backfill else since - timedelta(days=INCREMENTAL_OVERLAP_DAYS)
    window = Window(
        since=since, query_since=query_since, is_backfill=is_backfill, source=source
    )
    logger.info("%s: %s", pipeline, window.describe())
    return window
