"""Neo4j client.

Adapted from quotient-analytics-pipelines `include/helpers/neo4j.py` — the
connection handling there is battle-tested against Aura (idle-dropped sockets,
stale routing tables, auth rate limits) and there is no reason to relearn it.

Deliberate differences: no `clear_database` (this writes into the shared
production graph), and a `run_unwind` batching helper since every ingestion
module in this repo does the same chunked UNWIND-MERGE.
"""

from __future__ import annotations

import logging
import random
import time
from typing import Any, Callable, Optional, Sequence, TypeVar

import pandas as pd
from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import ServiceUnavailable, SessionExpired

from config.settings import (
    NEO4J_BATCH_SIZE,
    NEO4J_DATABASE,
    NEO4J_PASSWORD,
    NEO4J_URI,
    NEO4J_USERNAME,
)

logger = logging.getLogger(__name__)

_AUTH_RATE_LIMIT_CODE = "Neo.ClientError.Security.AuthenticationRateLimit"

# A driver whose routing table or pooled sockets have gone bad stays bad: every
# retry against it re-reads the same stale routing table. Connection errors
# therefore rebuild the driver before the next attempt.
_CONNECTION_ERRORS = (ServiceUnavailable, SessionExpired)
_MAX_BACKOFF_SECONDS = 60.0

# Long crawls hand back pooled connections that Aura closed while idle. Retire
# them by age and ping anything idle rather than hitting EOF mid-query.
_MAX_CONNECTION_LIFETIME_SECONDS = 300
_LIVENESS_CHECK_TIMEOUT_SECONDS = 0

T = TypeVar("T")


class Neo4jUtils:
    def __init__(
        self,
        uri: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        max_retries: int = 5,
    ):
        self.uri = uri or NEO4J_URI
        self.username = username or NEO4J_USERNAME
        self.password = password or NEO4J_PASSWORD
        self.database = database or NEO4J_DATABASE
        self.max_retries = max_retries

        if not self.uri:
            raise RuntimeError("NEO4J_URI is not set. Copy .env.example to .env.")
        if not self.password:
            raise RuntimeError("NEO4J_PASSWORD is not set. Copy .env.example to .env.")

        self.driver = self._build_driver()
        self._verify_connectivity()

    # -- connection management --------------------------------------------

    def _build_driver(self):
        return GraphDatabase.driver(
            self.uri,
            auth=basic_auth(self.username, self.password),
            max_connection_pool_size=50,
            connection_acquisition_timeout=30,
            max_connection_lifetime=_MAX_CONNECTION_LIFETIME_SECONDS,
            liveness_check_timeout=_LIVENESS_CHECK_TIMEOUT_SECONDS,
            keep_alive=True,
        )

    def _reset_driver(self):
        old = self.driver
        self.driver = self._build_driver()
        if old is not None:
            try:
                old.close()
            except Exception as exc:
                logger.warning("failed to close previous Neo4j driver: %s", exc)
        logger.info("rebuilt the Neo4j driver after a connection error")

    def _retry_delay(self, retry_number: int) -> float:
        base = min(2.0**retry_number, _MAX_BACKOFF_SECONDS)
        return base + random.uniform(0, min(base / 2, 5.0))

    def _run_with_retry(self, operation: Callable[[Any], T], *, label: str) -> T:
        last_error: Optional[BaseException] = None
        for attempt in range(self.max_retries + 1):
            if attempt:
                time.sleep(self._retry_delay(attempt))
            try:
                with self.driver.session(database=self.database) as session:
                    return operation(session)
            except _CONNECTION_ERRORS as exc:
                last_error = exc
                logger.error("Neo4j connection error: %s", exc)
                self._reset_driver()
            except Exception as exc:
                if getattr(exc, "code", None) != _AUTH_RATE_LIMIT_CODE:
                    logger.error("%s failed: %s", label, exc)
                    raise
                last_error = exc
                logger.warning("Neo4j auth rate limit; backing off")
            if attempt < self.max_retries:
                logger.info("retrying %s (%d/%d)", label, attempt + 1, self.max_retries)
        raise RuntimeError(
            f"{label} failed after {self.max_retries} attempts"
        ) from last_error

    def _verify_connectivity(self, counter: int = 0):
        try:
            with self.driver.session(database=self.database) as session:
                session.run("RETURN 1").consume()
            logger.info("connected to Neo4j at %s", self.uri)
        except Exception as exc:
            if getattr(exc, "code", None) == _AUTH_RATE_LIMIT_CODE and counter < self.max_retries:
                delay = 2 * (counter + 1)
                logger.warning("Neo4j auth rate limit on connect; retrying in %ds", delay)
                time.sleep(delay)
                return self._verify_connectivity(counter + 1)
            logger.error("failed to connect to Neo4j: %s", exc)
            raise

    def close(self):
        if self.driver:
            self.driver.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # -- queries -----------------------------------------------------------

    def execute_query(self, query: str, parameters: dict | None = None) -> list[dict]:
        def _run(session):
            return [record.data() for record in session.run(query, parameters or {})]

        return self._run_with_retry(_run, label="query")

    def execute_query_df(self, query: str, parameters: dict | None = None) -> pd.DataFrame:
        return pd.DataFrame(self.execute_query(query, parameters))

    def execute_write(self, query: str, parameters: dict | None = None) -> dict:
        """Run a write and return its counters, so callers can report what changed."""

        def _run(session):
            result = session.run(query, parameters or {})
            summary = result.consume()
            counters = summary.counters
            return {
                "nodes_created": counters.nodes_created,
                "relationships_created": counters.relationships_created,
                "properties_set": counters.properties_set,
            }

        return self._run_with_retry(_run, label="write")

    def run_unwind(
        self,
        query: str,
        rows: Sequence[dict],
        batch_size: int | None = None,
        params: dict | None = None,
        label: str = "unwind",
    ) -> dict:
        """Apply a `UNWIND $rows AS row ...` write in batches.

        Every ingestion module funnels through here so batching, counter
        aggregation and progress logging are identical across the repo.
        """
        batch_size = batch_size or NEO4J_BATCH_SIZE
        rows = list(rows)
        totals = {"nodes_created": 0, "relationships_created": 0, "properties_set": 0}
        if not rows:
            logger.info("%s: nothing to write", label)
            return totals

        for start in range(0, len(rows), batch_size):
            batch = rows[start : start + batch_size]
            counters = self.execute_write(query, {**(params or {}), "rows": batch})
            for key in totals:
                totals[key] += counters[key]
            logger.info(
                "%s: %d/%d rows (+%d nodes, +%d rels)",
                label,
                min(start + batch_size, len(rows)),
                len(rows),
                counters["nodes_created"],
                counters["relationships_created"],
            )
        logger.info(
            "%s complete: %d rows, +%d nodes, +%d rels, %d props set",
            label,
            len(rows),
            totals["nodes_created"],
            totals["relationships_created"],
            totals["properties_set"],
        )
        return totals

    # -- introspection -----------------------------------------------------

    def node_count(self, label: str | None = None) -> int:
        query = f"MATCH (n:{label}) RETURN count(n) AS c" if label else "MATCH (n) RETURN count(n) AS c"
        result = self.execute_query(query)
        return result[0]["c"] if result else 0

    def relationship_count(self, rel_type: str | None = None) -> int:
        query = (
            f"MATCH ()-[r:{rel_type}]->() RETURN count(r) AS c"
            if rel_type
            else "MATCH ()-[r]->() RETURN count(r) AS c"
        )
        result = self.execute_query(query)
        return result[0]["c"] if result else 0
