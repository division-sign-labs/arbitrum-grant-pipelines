"""Cypher statements, one module per data type.

The counterpart to `sql/`: `sql` builds what goes to Dune, `cypher` holds what
goes to Neo4j, and `ingestion.ingest_<name>` is the module that binds a CSV to a
statement. Nothing here reads a file, opens a driver or spends anything — a
module in this package is text plus the constants interpolated into it, which is
what lets the tests assert on a query without a database.

`cypher.schema` owns the constraint and index DDL; `cypher.common` owns the
fragments more than one data type shares.
"""
