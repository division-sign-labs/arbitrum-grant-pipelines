"""Applies the graph's schema DDL to a Neo4j target.

The DDL itself lives in `cypher.schema`; this module is the part that talks to a
server. Each spec offers a ladder of candidate statements and the first one the
server accepts wins — a Community/Aura-Free target rejects the Enterprise-only
NODE KEY rung. If every rung fails we log a warning and carry on: a missing
constraint makes ingestion slower, not wrong.

Run standalone before the first ingest:
    python -m ingestion.constraints
"""

from __future__ import annotations

import argparse
import logging

from neo4j.exceptions import Neo4jError

from cypher.schema import CONSTRAINTS, INDEXES, ConstraintSpec, index_statement
from lib.logging_utils import setup_logging
from lib.neo4j_utils import Neo4jUtils

logger = logging.getLogger(__name__)

__all__ = [
    "CONSTRAINTS",
    "INDEXES",
    "ConstraintSpec",
    "ensure_constraints",
    "existing_schema_names",
    "index_statement",
    "main",
]


def existing_schema_names(neo4j: Neo4jUtils) -> set[str]:
    """Names of constraints and indexes already present on the target."""
    names: set[str] = set()
    for statement in ("SHOW CONSTRAINTS", "SHOW INDEXES"):
        for row in neo4j.execute_query(statement):
            name = row.get("name")
            if name:
                names.add(name)
    return names


def ensure_constraints(neo4j: Neo4jUtils, dry_run: bool = False) -> dict:
    """Create every missing constraint and index. Safe to call on every ingest.

    Returns a report of what was created, what was already there, and what the
    server refused, so callers can log it rather than guess.
    """
    report: dict[str, list[str]] = {"created": [], "existing": [], "failed": []}

    if dry_run:
        for spec in CONSTRAINTS:
            print(spec.statements()[0])
        for index in INDEXES:
            print(index_statement(*index))
        return report

    present = existing_schema_names(neo4j)

    for spec in CONSTRAINTS:
        if spec.name in present:
            report["existing"].append(spec.name)
            continue
        for statement in spec.statements():
            try:
                neo4j.execute_write(statement)
            except Neo4jError as exc:
                # Enterprise-only syntax on a Community target lands here; try
                # the next rung before giving up on this key.
                logger.warning("constraint %s rejected (%s): %s", spec.name, exc.code, exc.message)
                continue
            report["created"].append(spec.name)
            break
        else:
            logger.warning(
                "could not create any constraint for %s%s — ingestion will still "
                "MERGE correctly but more slowly",
                spec.label,
                list(spec.properties),
            )
            report["failed"].append(spec.name)

    for name, label, properties in INDEXES:
        if name in present:
            report["existing"].append(name)
            continue
        try:
            neo4j.execute_write(index_statement(name, label, properties))
        except Neo4jError as exc:
            logger.warning("index %s rejected (%s): %s", name, exc.code, exc.message)
            report["failed"].append(name)
        else:
            report["created"].append(name)

    logger.info(
        "schema: %d created, %d already present, %d failed",
        len(report["created"]),
        len(report["existing"]),
        len(report["failed"]),
    )
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m ingestion.constraints",
        description="Create the graph's uniqueness constraints and indexes.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the DDL without touching Neo4j.",
    )
    parser.add_argument("--log-level", default=None, help="DEBUG/INFO/WARNING")
    args = parser.parse_args(argv)
    setup_logging(args.log_level)

    if args.dry_run:
        ensure_constraints(None, dry_run=True)  # type: ignore[arg-type]
        return 0

    with Neo4jUtils() as neo4j:
        report = ensure_constraints(neo4j)
        for row in neo4j.execute_query(
            "SHOW CONSTRAINTS YIELD name, type, labelsOrTypes, properties "
            "RETURN name, type, labelsOrTypes, properties ORDER BY name"
        ):
            print(
                f"{row['name']:<28} {row['type']:<26} "
                f"{row['labelsOrTypes']} {row['properties']}"
            )
    print(
        f"created={len(report['created'])} existing={len(report['existing'])} "
        f"failed={len(report['failed'])}"
    )
    return 1 if report["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
