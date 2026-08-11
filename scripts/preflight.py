"""Check that everything an operator must supply is actually present.

Run this before a backfill. It answers one question — "will this get four hours
in and then die because a key or a seed file was missing?" — without spending a
Dune credit or an API call.

    .venv/bin/python -m scripts.preflight
    .venv/bin/python -m scripts.preflight --check-connections   # also dial out
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import settings  # noqa: E402
from lib.logging_utils import setup_logging  # noqa: E402
from lib.seeds import SeedMissingError, load_brand_accounts, load_miniapp_builders  # noqa: E402

OK = "  ok   "
WARN = "  warn "
FAIL = "  FAIL "

CREDENTIALS = [
    ("DUNE_API_KEY", settings.DUNE_API_KEY, "on-chain pipelines (Dune)"),
    ("NEYNAR_API_KEY", settings.NEYNAR_API_KEY, "all social pipelines (Neynar)"),
    ("NEO4J_URI", settings.NEO4J_URI, "ingestion target"),
    ("NEO4J_PASSWORD", settings.NEO4J_PASSWORD, "ingestion target"),
]


def check_credentials() -> list[str]:
    print("credentials (.env)")
    problems = []
    for name, value, why in CREDENTIALS:
        if value:
            print(f"{OK}{name:<16} set  ({why})")
        else:
            print(f"{FAIL}{name:<16} MISSING — needed for {why}")
            problems.append(f"{name} is not set in .env")
    return problems


def check_seeds() -> tuple[list[str], list[str]]:
    """Seeds are warnings, not failures: they gate two pipelines, not the repo."""
    print("\nseed files")
    problems: list[str] = []
    warnings: list[str] = []
    for label, loader, pipeline in [
        ("miniapp_builders", load_miniapp_builders, "pipelines.miniapp_builders"),
        ("brand_accounts", load_brand_accounts, "pipelines.brand_engagement"),
    ]:
        try:
            df = loader()
        except SeedMissingError:
            print(f"{WARN}{label:<18} not found — {pipeline} cannot run")
            warnings.append(f"seeds/{label}.csv is missing; {pipeline} cannot run")
            continue
        except ValueError as exc:
            print(f"{FAIL}{label:<18} malformed: {exc}")
            problems.append(f"seeds/{label}.csv is malformed: {exc}")
            continue
        if df.empty:
            print(f"{WARN}{label:<18} header-only template — {pipeline} will produce nothing")
            warnings.append(
                f"seeds/{label}.csv is still an empty template; {pipeline} will produce nothing"
            )
        else:
            print(f"{OK}{label:<18} {len(df)} row(s)")
    return problems, warnings


def check_layout() -> list[str]:
    print("\nlayout")
    problems = []
    for path, required in [
        (settings.REPO_ROOT / "requirements.txt", True),
        (settings.REPO_ROOT / ".env", True),
        (settings.SEEDS_DIR, False),
    ]:
        if path.exists():
            print(f"{OK}{path.name} present")
        elif required:
            print(f"{FAIL}{path} missing")
            problems.append(f"{path} is missing")
    for directory in (settings.DATA_DIR, settings.STATE_DIR):
        status = "exists" if directory.exists() else "will be created on first run"
        print(f"{OK}{directory.name}/ {status}")
    return problems


def check_connections() -> list[str]:
    print("\nconnections")
    problems = []
    try:
        from lib.neo4j_utils import Neo4jUtils

        with Neo4jUtils() as neo4j:
            nodes = neo4j.node_count()
        print(f"{OK}neo4j reachable ({nodes} nodes)")
    except Exception as exc:
        print(f"{FAIL}neo4j unreachable: {str(exc)[:160]}")
        problems.append(f"Neo4j is unreachable: {str(exc)[:160]}")

    try:
        from lib.dune import DuneRunner

        DuneRunner(cache_ttl_hours=0)
        print(f"{OK}dune key accepted")
    except Exception as exc:
        print(f"{FAIL}dune client init failed: {str(exc)[:160]}")
        problems.append(f"Dune client failed to initialise: {str(exc)[:160]}")

    try:
        from lib.neynar import NeynarClient

        users = NeynarClient().bulk_users([3])
        print(f"{OK}neynar key accepted (resolved fid 3 -> {users[0].get('username')})")
    except Exception as exc:
        print(f"{FAIL}neynar call failed: {str(exc)[:160]}")
        problems.append(f"Neynar call failed: {str(exc)[:160]}")
    return problems


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check-connections",
        action="store_true",
        help="Also dial Neo4j and make one Neynar call (a few seconds, no Dune credits).",
    )
    parser.add_argument("--log-level", default="WARNING")
    args = parser.parse_args(argv)
    setup_logging(args.log_level)

    problems = check_credentials()
    seed_problems, warnings = check_seeds()
    problems += seed_problems
    problems += check_layout()
    if args.check_connections:
        problems += check_connections()

    print("\n" + "=" * 62)
    if problems:
        print(f"NOT READY — {len(problems)} problem(s):")
        for problem in problems:
            print(f"  - {problem}")
    else:
        print("READY — credentials and layout are in place.")
    for warning in warnings:
        print(f"  note: {warning}")
    if warnings and not problems:
        print("\nEverything runs except the seed-gated pipelines listed above.")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
