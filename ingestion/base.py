"""Shared machinery for every `ingestion.ingest_*` module.

The contract each ingest module fills in is small on purpose: name the data
type, then declare one `Step` per CSV — which columns it must have and the
Cypher that writes it, which it imports from `cypher.<data_type>`. Everything
else (finding the run, validating columns, coercing types, batching, provenance
stamping, dry-run rendering, exit codes) happens here so the modules stay
readable and behave identically.

Three conventions the Cypher in those modules relies on:

  * `$rows` is the batch, `$asOf` is one timestamp shared by the whole ingest,
    `$ingestedBy` is the repo's provenance string, and `$source` is the data
    type — so any edge can say which pipeline produced it.
  * A node the step *owns* gets `SET n.asOf = $asOf, n.ingestedBy = $ingestedBy`
    unconditionally. A node the step merely *references* (a stub, so an edge has
    somewhere to land) gets `ON CREATE SET` only, so a later authoritative write
    is never downgraded and `asOf` keeps meaning "when this fact was refreshed".
  * Addresses are lowercased in pandas AND with `toLower()` in Cypher. Either
    alone would do; both together mean a hand-run query cannot introduce a
    duplicate Wallet that differs only by case.

Type coercion is not optional plumbing. pandas turns an empty CSV cell into
float('nan'), and the neo4j driver happily stores that as a Float NaN which then
poisons every comparison against it; and an integer column with one empty cell
comes back as 1.0, 2.0, ... which would write Float fids. So every column is
read as text and coerced by name, NaN becomes None, and `*_raw` columns stay
strings because uint256 balances overflow Neo4j's 64-bit integers.
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

import pandas as pd

from config.settings import PROVENANCE
# The fragment moved to `cypher.common` with the rest of the statements;
# `ingestion.base` stays its import site for callers already reaching for it here.
from cypher.common import optional_account_link
from ingestion.constraints import ensure_constraints
from lib.cli import ingestion_parser
from lib.logging_utils import setup_logging
from lib.neo4j_utils import Neo4jUtils
from lib.runs import MANIFEST_NAME, read_manifest, resolve_run, utc_now
from lib.state import parse_ts

logger = logging.getLogger(__name__)

# Reserved for "the operator asked for something that is not there" — a missing
# run, an unfinished run, a CSV that does not match the contract. Anything a
# retry cannot fix, so the caller (run_all) can tell it apart from a crash.
EXIT_BAD_RUN = 2


class IngestError(RuntimeError):
    """A problem with the requested run, phrased for whoever has to fix it."""


# --- column typing -------------------------------------------------------
# Keyed by the exact column names in the CSV contract. A column absent from all
# of these is written as a trimmed string.

INT_COLUMNS = frozenset(
    {
        "fid", "author_fid", "buyer_fid", "engager_fid", "brand_fid",
        "fee_recipient_fid",
        "chain_id", "follower_count", "following_count", "tx_count",
        "likes_count", "recasts_count", "replies_count", "text_length",
        "swap_count", "cast_count", "unique_buyers_influenced",
        "total_purchases", "n_influencers", "casts_posted",
        "reactions_received", "replies_received", "reactions_given",
        "ledger_event_count", "replies", "likes", "recasts", "mentions",
        "priority",
    }
)

FLOAT_COLUMNS = frozenset(
    {
        "neynar_score", "amount_usd", "token_amount", "attributed_usd",
        "total_purchase_volume_usd", "weighted_score", "starting_market_cap_usd",
        "price_usd", "market_cap_usd", "volume_24h_usd", "volume_native",
        "volume_usd", "assets", "balance", "amount0", "amount1",
        "cum_volume_usd", "weight",
    }
)

BOOL_COLUMNS = frozenset({"is_primary", "has_hl_activity"})

TIMESTAMP_COLUMNS = frozenset(
    {
        "deployed_at", "block_time", "timestamp", "registered_at", "first_tx_at",
        "last_tx_at", "first_cast_at", "last_cast_at", "window_start",
        "window_end", "first_activity_at", "last_activity_at", "checked_at",
        "day",
    }
)

# Lowercased in pandas. Solana addresses are base58 and case-sensitive, so the
# rule is "lowercase it only if it looks like 0x-hex" — `wallets.csv` carries
# both protocols in one column.
ADDRESS_COLUMNS = frozenset(
    {
        "address", "deployer_address", "admin_address", "fee_recipient_address",
        "contract_address",
        "token_address", "buyer_address", "pool_address", "paired_token",
        "vault_address", "custody_address",
    }
)

HASH_COLUMNS = frozenset({"tx_hash", "cast_hash", "target_cast_hash", "parent_hash"})

# Words that mean "no value" when they turn up in a typed column — a pandas
# round-trip through float, or an upstream API's idea of null. They are NOT
# treated as missing in a text column: "NULL" is a perfectly plausible memecoin
# ticker and nulling it would be silent data loss.
_SENTINELS = {"nan", "none", "null", "nat", "<na>"}
_TEXT_KINDS = {"str", "address", "hash"}


def _column_kind(column: str, overrides: dict[str, str] | None) -> str:
    if overrides and column in overrides:
        return overrides[column]
    if column in INT_COLUMNS:
        return "int"
    if column in FLOAT_COLUMNS:
        return "float"
    if column in BOOL_COLUMNS:
        return "bool"
    if column in TIMESTAMP_COLUMNS:
        return "timestamp"
    if column in ADDRESS_COLUMNS:
        return "address"
    if column in HASH_COLUMNS:
        return "hash"
    return "str"


_TRUE = {"true", "t", "1", "yes", "y"}
_FALSE = {"false", "f", "0", "no", "n"}


def _coerce(value: Any, kind: str) -> Any:
    """Text -> the python type the neo4j driver should see. None when missing."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text:
        return None
    if kind not in _TEXT_KINDS and text.lower() in _SENTINELS:
        return None
    if kind == "int":
        # "1.0" happens whenever pandas has floated an int column somewhere
        # upstream; int(float(...)) absorbs it.
        return int(float(text))
    if kind == "float":
        return float(text)
    if kind == "bool":
        lowered = text.lower()
        if lowered in _TRUE:
            return True
        if lowered in _FALSE:
            return False
        raise ValueError(f"not a boolean: {text!r}")
    if kind == "timestamp":
        return parse_ts(text)
    if kind == "address":
        return text.lower() if text.startswith("0x") else text
    if kind == "hash":
        return text.lower()
    return text


# --- run loading ---------------------------------------------------------


def load_run(data_type: str, run_id: str | None = None) -> tuple[Path, dict]:
    """Resolve a run directory and its manifest, or explain what is missing."""
    try:
        run_dir = resolve_run(data_type, run_id)
    except FileNotFoundError as exc:
        raise IngestError(str(exc)) from exc
    if not (run_dir / MANIFEST_NAME).exists():
        raise IngestError(
            f"run {run_dir} has no {MANIFEST_NAME}: the pipeline that wrote it did "
            f"not finish, so the CSVs may be half-written. Re-run the pipeline, or "
            f"pass --run-id for a completed run."
        )
    return run_dir, read_manifest(run_dir)


def read_rows(
    run_dir: Path,
    name: str,
    required_columns: Sequence[str],
    required: bool = True,
    types: dict[str, str] | None = None,
) -> list[dict]:
    """Load one CSV as neo4j-ready dicts, failing loudly on a contract breach."""
    path = run_dir / f"{name}.csv"
    if not path.exists():
        if required:
            raise IngestError(
                f"{path} is missing, but the run's manifest declares it. The run "
                f"directory has been modified since it was written; re-run the "
                f"{run_dir.parent.name} pipeline."
            )
        logger.warning("%s not present in this run; skipping that step", path.name)
        return []

    # Everything as text: pandas' own inference is what would corrupt ints and
    # uint256 strings, and empty-only NA keeps a username of "NA" intact.
    frame = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])

    missing = [column for column in required_columns if column not in frame.columns]
    if missing:
        raise IngestError(
            f"{path} is missing required column(s): {', '.join(missing)}. "
            f"Found: {', '.join(frame.columns) or '(none)'}. The CSV contract for "
            f"{run_dir.parent.name}/{name}.csv is fixed — ingestion and the graph "
            f"schema both depend on those names."
        )

    columns = list(frame.columns)
    coerced: dict[str, list] = {}
    failures: dict[str, int] = {}
    for column in columns:
        kind = _column_kind(column, types)
        values: list[Any] = []
        for raw in frame[column].tolist():
            try:
                values.append(_coerce(raw, kind))
            except (TypeError, ValueError):
                failures[column] = failures.get(column, 0) + 1
                values.append(None)
        coerced[column] = values

    for column, count in failures.items():
        logger.warning(
            "%s.%s: %d value(s) could not be read as %s and were written as null",
            name,
            column,
            count,
            _column_kind(column, types),
        )

    ordered = [coerced[column] for column in columns]
    rows = [dict(zip(columns, values)) for values in zip(*ordered)] if ordered else []
    logger.info("read %s (%d rows)", path.name, len(rows))
    return rows


def unique_rows(rows: Iterable[dict], keys: Sequence[str]) -> list[dict]:
    """Collapse rows to one per natural key, last write winning.

    Two rows with the same key inside one UNWIND batch would MERGE the same node
    twice in a single transaction — correct, but pointless work, and for
    singleton edges the second SET silently overwrites the first anyway.
    """
    deduped: dict[tuple, dict] = {}
    for row in rows:
        deduped[tuple(row.get(key) for key in keys)] = row
    return list(deduped.values())


# --- writing -------------------------------------------------------------


def run_unwind(
    neo4j: Neo4jUtils,
    query: str,
    rows: Sequence[dict],
    label: str,
    as_of: datetime | None = None,
    source: str | None = None,
    batch_size: int | None = None,
    params: dict | None = None,
) -> dict:
    """Neo4jUtils.run_unwind with the provenance parameters every write needs."""
    merged: dict[str, Any] = {
        "asOf": as_of or utc_now(),
        "ingestedBy": PROVENANCE,
        "source": source,
    }
    merged.update(params or {})
    return neo4j.run_unwind(
        query, rows, batch_size=batch_size, params=merged, label=label
    )


@dataclass(frozen=True)
class Step:
    """One CSV and the write it drives."""

    label: str
    csv: str
    columns: Sequence[str]
    cypher: str
    transform: Callable[[list[dict]], list[dict]] | None = None
    required: bool = True
    types: dict[str, str] | None = None
    params: dict = field(default_factory=dict)


def _prepare(run_dir: Path, manifest: dict, step: Step) -> list[dict]:
    declared = set(manifest.get("files", {}))
    # A file the manifest never declared means the pipeline degraded (it logged a
    # warning and wrote nothing). That is a supported outcome, not a failure —
    # only a *declared* file going missing is corruption.
    if step.required and step.csv not in declared:
        logger.warning(
            "run %s does not declare %s.csv; the pipeline degraded, skipping %s",
            run_dir.name,
            step.csv,
            step.label,
        )
        return []
    rows = read_rows(
        run_dir,
        step.csv,
        step.columns,
        required=step.required and step.csv in declared,
        types=step.types,
    )
    if step.transform is not None:
        rows = step.transform(rows)
        logger.info("%s: %d rows after transform", step.label, len(rows))
    return rows


def _print_plan(data_type: str, run_dir: Path, manifest: dict, prepared: list[tuple[Step, list[dict]]]) -> None:
    print(f"\n=== {data_type} :: {run_dir} ===")
    print(f"generated_at={manifest.get('generated_at')} row_total={manifest.get('row_total')}")
    for step, rows in prepared:
        print(f"\n--- {step.label}  ({step.csv}.csv, {len(rows)} rows) ---")
        if step.params:
            print(f"extra params: {json.dumps(step.params, default=str)}")
        print(step.cypher.strip())
        if rows:
            print(f"first row: {json.dumps(rows[0], default=str)}")
    total = sum(len(rows) for _, rows in prepared)
    print(f"\n[dry-run] {total} rows across {len(prepared)} steps; nothing was written.\n")


def ingest_run(data_type: str, steps: Sequence[Step], args: argparse.Namespace) -> int:
    """Validate a run and write it, or render the plan under --dry-run."""
    run_dir, manifest = load_run(data_type, args.run_id)
    logger.info(
        "%s: run %s (%d rows across %d files, generated %s)",
        data_type,
        run_dir.name,
        manifest.get("row_total", 0),
        len(manifest.get("files", {})),
        manifest.get("generated_at"),
    )
    for note in manifest.get("notes", []):
        logger.info("run note: %s", note)

    # Read and validate everything before opening a connection: a CSV that
    # breaches the contract should cost nothing and leave the graph untouched.
    prepared = [(step, _prepare(run_dir, manifest, step)) for step in steps]

    if args.dry_run:
        _print_plan(data_type, run_dir, manifest, prepared)
        return 0

    as_of = utc_now()
    totals: dict[str, dict] = {}
    with Neo4jUtils() as neo4j:
        if not getattr(args, "no_constraints", False):
            ensure_constraints(neo4j)
        for step, rows in prepared:
            if not rows:
                logger.info("%s: no rows, nothing to write", step.label)
                continue
            totals[step.label] = run_unwind(
                neo4j,
                step.cypher,
                rows,
                label=step.label,
                as_of=as_of,
                source=data_type,
                batch_size=args.batch_size,
                params=step.params,
            )

    nodes = sum(t["nodes_created"] for t in totals.values())
    rels = sum(t["relationships_created"] for t in totals.values())
    props = sum(t["properties_set"] for t in totals.values())
    for label, counters in totals.items():
        logger.info(
            "%-28s +%d nodes  +%d rels  %d props",
            label,
            counters["nodes_created"],
            counters["relationships_created"],
            counters["properties_set"],
        )
    logger.info(
        "%s run %s ingested: +%d nodes, +%d relationships, %d properties set",
        data_type,
        run_dir.name,
        nodes,
        rels,
        props,
    )
    return 0


def _resolve(value, args: argparse.Namespace):
    return value(args) if callable(value) else value


def ingest_main(
    name: str,
    description: str,
    data_type,
    steps,
    argv: Sequence[str] | None = None,
    add_args: Callable[[argparse.ArgumentParser], None] | None = None,
) -> int:
    """The whole of an ingest module's `main()`.

    `data_type` and `steps` may each be a value or a callable taking the parsed
    args, which is what lets ingest_tokens serve two data types from one module.
    """
    parser = ingestion_parser(name, description)
    parser.add_argument(
        "--no-constraints",
        action="store_true",
        help="Skip the CREATE CONSTRAINT IF NOT EXISTS pass (run_all does it once).",
    )
    if add_args is not None:
        add_args(parser)
    args = parser.parse_args(argv)
    setup_logging(args.log_level)
    try:
        return ingest_run(_resolve(data_type, args), _resolve(steps, args), args)
    except IngestError as exc:
        logger.error("%s", exc)
        return EXIT_BAD_RUN
