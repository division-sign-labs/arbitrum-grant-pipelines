"""Confirm the Dune tables this repo depends on exist and have the columns we assume.

Run this before trusting any SQL in `sql/`, and again whenever a query starts
returning zero rows for no obvious reason — upstream datasets do drift. Uses
SHOW COLUMNS / SHOW TABLES, which are metadata-only and effectively free.

    python -m scripts.probe_schemas
    python -m scripts.probe_schemas --tables robinhood.logs dex.trades
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib.dune import DuneError, DuneRunner  # noqa: E402
from lib.logging_utils import setup_logging  # noqa: E402

# Everything the pipelines read. Grouped by which pipeline would break.
TABLES = [
    # linked_wallets / brand_engagement / evangelism
    "dune.neynar.dataset_farcaster_verifications",
    "dune.neynar.dataset_farcaster_casts",
    "dune.neynar.dataset_farcaster_reactions",
    "dune.neynar.dataset_farcaster_fids",
    "dune.neynar.dataset_farcaster_user_data",
    "dune.neynar.dataset_farcaster_profile_with_addresses",
    # contract_deployers / miniapp_builders
    "arbitrum.creation_traces",
    "arbitrum.transactions",
    "arbitrum.logs",
    # token_buyers / popular_tokens
    "dex.trades",
    "erc20_arbitrum.evt_transfer",
    "tokens.erc20",
    "prices.usd",
    # bankr / robinhood chain
    "robinhood.transactions",
    "robinhood.logs",
    "robinhood.creation_traces",
    "robinhood.traces",
]

SCHEMAS = ["robinhood", "uniswap_v3_arbitrum", "uniswap_v4_arbitrum", "dex"]


def probe_tables(dune: DuneRunner, tables: list[str]) -> dict:
    results = {}
    for table in tables:
        try:
            df = dune.run_sql(f"SHOW COLUMNS FROM {table}", label=f"cols {table}")
            column_field = "Column" if "Column" in df.columns else df.columns[0]
            type_field = "Type" if "Type" in df.columns else df.columns[1]
            results[table] = {
                "exists": True,
                "columns": dict(zip(df[column_field], df[type_field])),
            }
            print(f"\n=== {table} — {len(df)} columns ===")
            for name, dtype in zip(df[column_field], df[type_field]):
                print(f"    {name:<34} {dtype}")
        except DuneError as exc:
            results[table] = {"exists": False, "error": str(exc)[:300]}
            print(f"\n=== {table} — MISSING ===\n    {str(exc)[:300]}")
    return results


def probe_schemas(dune: DuneRunner, schemas: list[str]) -> dict:
    results = {}
    for schema in schemas:
        try:
            df = dune.run_sql(f"SHOW TABLES FROM {schema}", label=f"tables {schema}")
            names = df[df.columns[0]].tolist()
            results[schema] = {"exists": True, "tables": names}
            print(f"\n=== schema {schema} — {len(names)} tables ===")
            for name in names[:80]:
                print(f"    {name}")
            if len(names) > 80:
                print(f"    ... and {len(names) - 80} more")
        except DuneError as exc:
            results[schema] = {"exists": False, "error": str(exc)[:300]}
            print(f"\n=== schema {schema} — MISSING ===\n    {str(exc)[:300]}")
    return results


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tables", nargs="*", default=None)
    parser.add_argument("--schemas", nargs="*", default=None)
    parser.add_argument("--out", default=None, help="Write the result map to this JSON file.")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    setup_logging(args.log_level)
    # Metadata queries are cheap but identical across runs; caching them keeps
    # repeat probes instant.
    dune = DuneRunner(cache_ttl_hours=1.0)

    report = {
        "schemas": probe_schemas(dune, args.schemas if args.schemas is not None else SCHEMAS),
        "tables": probe_tables(dune, args.tables if args.tables is not None else TABLES),
    }

    missing = [t for t, r in report["tables"].items() if not r["exists"]]
    print("\n" + "=" * 70)
    print(f"{len(report['tables']) - len(missing)}/{len(report['tables'])} tables present")
    if missing:
        print("MISSING: " + ", ".join(missing))

    if args.out:
        Path(args.out).write_text(json.dumps(report, indent=2))
        print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
