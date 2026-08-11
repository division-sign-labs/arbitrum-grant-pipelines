"""Dune Analytics runner: inline SQL -> execute -> poll -> paginated results.

Uses the raw REST API rather than dune-client so we control pagination (the
verifications backfill is millions of rows), retries, and result caching. Every
execution is logged with its id and row count so credit spend is auditable
after the fact.

Results come back through the CSV endpoint: for wide result sets it is
materially cheaper to parse than the JSON one, and pandas can consume it
directly.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from config.settings import (
    DATA_DIR,
    DUNE_API_BASE,
    DUNE_API_KEY,
    DUNE_MAX_WAIT_SECONDS,
    DUNE_PERFORMANCE,
    DUNE_POLL_SECONDS,
    DUNE_RESULT_PAGE_SIZE,
)

logger = logging.getLogger(__name__)

# Dune's CSV result endpoint renders SQL NULL as the literal string "<nil>".
# Left alone it is poison: notna() reports the value as present, an address
# column ships "<nil>" into the graph as if it were an address, and one NULL in
# a BIGINT column downgrades the whole column to str. Declaring it as a NA token
# at parse time fixes both the emptiness check and the dtype inference.
DUNE_NULL_TOKENS = ["<nil>"]

_TERMINAL_OK = {"QUERY_STATE_COMPLETED"}
_TERMINAL_BAD = {"QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED", "QUERY_STATE_EXPIRED"}
_RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class DuneError(RuntimeError):
    pass


class DuneRunner:
    """Executes ad-hoc SQL on Dune and returns a DataFrame."""

    def __init__(
        self,
        api_key: str | None = None,
        performance: str | None = None,
        cache_ttl_hours: float = 24.0,
        cache_dir: Path | None = None,
        dry_run: bool = False,
    ):
        self.api_key = api_key or DUNE_API_KEY
        self.performance = performance or DUNE_PERFORMANCE
        self.cache_ttl_seconds = cache_ttl_hours * 3600
        self.cache_dir = Path(cache_dir or (DATA_DIR / ".dune_cache"))
        self.dry_run = dry_run
        # Public by default: this account's plan caps private queries, and the
        # scratch query only ever holds SQL we would be happy to publish (token
        # addresses and public Farcaster verification addresses).
        self.private_queries = (
            os.environ.get("DUNE_PRIVATE_QUERIES", "false").lower() == "true"
        )
        self._query_id: int | None = None
        self.executions: list[dict] = []
        self.session = requests.Session()
        if self.api_key:
            self.session.headers.update({"X-DUNE-API-KEY": self.api_key})
        elif not dry_run:
            raise RuntimeError(
                "DUNE_API_KEY is not set. Copy .env.example to .env and fill it in."
            )

    # -- HTTP plumbing -----------------------------------------------------

    def _request(self, method: str, path: str, *, max_retries: int = 6, **kwargs):
        url = f"{DUNE_API_BASE}{path}"
        delay = 2.0
        last_error = None
        for attempt in range(max_retries + 1):
            try:
                response = self.session.request(method, url, timeout=120, **kwargs)
            except requests.RequestException as exc:
                last_error = exc
                logger.warning("dune %s %s failed (%s); retrying", method, path, exc)
            else:
                if response.status_code < 400:
                    return response
                if response.status_code not in _RETRYABLE_STATUS:
                    raise DuneError(
                        f"dune {method} {path} -> {response.status_code}: {response.text[:500]}"
                    )
                last_error = DuneError(
                    f"{response.status_code}: {response.text[:200]}"
                )
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        delay = max(delay, float(retry_after))
                    except ValueError:
                        pass
                logger.warning(
                    "dune %s %s -> %s; retry %d/%d in %.0fs",
                    method,
                    path,
                    response.status_code,
                    attempt + 1,
                    max_retries,
                    delay,
                )
            if attempt < max_retries:
                time.sleep(delay)
                delay = min(delay * 2, 120)
        raise DuneError(f"dune {method} {path} failed after {max_retries} retries: {last_error}")

    # -- caching -----------------------------------------------------------

    def _cache_path(self, sql: str) -> Path:
        digest = hashlib.sha256(f"{self.performance}\n{sql}".encode()).hexdigest()[:24]
        return self.cache_dir / f"{digest}.csv"

    def _read_cache(self, sql: str) -> pd.DataFrame | None:
        if self.cache_ttl_seconds <= 0:
            return None
        path = self._cache_path(sql)
        if not path.exists():
            return None
        age = time.time() - path.stat().st_mtime
        if age > self.cache_ttl_seconds:
            return None
        logger.info("dune cache hit (%s, %.1fh old)", path.name, age / 3600)
        return pd.read_csv(path)

    def _write_cache(self, sql: str, df: pd.DataFrame) -> None:
        if self.cache_ttl_seconds <= 0:
            return
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        df.to_csv(self._cache_path(sql), index=False)

    # -- execution ---------------------------------------------------------

    def run_sql(
        self,
        sql: str,
        *,
        label: str = "query",
        limit: int | None = None,
        use_cache: bool = True,
        performance: str | None = None,
    ) -> pd.DataFrame:
        """Run inline SQL and return every result row as a DataFrame."""
        sql = prepare_sql(sql, limit=limit)

        if self.dry_run:
            logger.info("[dry-run] would execute Dune query %r:\n%s", label, sql)
            return pd.DataFrame()

        if use_cache:
            cached = self._read_cache(sql)
            if cached is not None:
                return cached

        started = time.time()
        query_id = self._scratch_query(label, sql)
        execution_id = self._execute(query_id, performance or self.performance)
        self._wait(execution_id, label)
        df = self._fetch_results(execution_id)

        elapsed = time.time() - started
        self.executions.append(
            {
                "label": label,
                "query_id": query_id,
                "rows": len(df),
                "seconds": round(elapsed, 1),
            }
        )
        logger.info(
            "dune %r: %d rows in %.0fs (query_id=%s)", label, len(df), elapsed, query_id
        )
        if use_cache:
            self._write_cache(sql, df)
        return df

    def _scratch_query(self, label: str, sql: str) -> int:
        """One reusable query per runner, its SQL rewritten before each execution.

        Creating a query per execution burns Dune's saved-query allowance — a
        long backfill is thousands of executions, and accounts cap the number of
        private queries ("Max number of private queries reached"), which stops
        the run dead no matter how much compute budget is left. Archiving after
        each run does not give the slot back.

        So the runner creates exactly one query and PATCHes its SQL thereafter.
        """
        if self._query_id is None:
            response = self._request(
                "POST",
                "/query",
                json={
                    "name": "[arbitrum-grant-pipelines] scratch",
                    "query_sql": sql,
                    "is_private": self.private_queries,
                },
            )
            self._query_id = response.json()["query_id"]
            logger.info("dune: created scratch query %s", self._query_id)
            return self._query_id

        self._request(
            "PATCH",
            f"/query/{self._query_id}",
            json={"query_sql": sql, "name": f"[arbitrum-grant-pipelines] {label}"},
        )
        return self._query_id

    def close(self) -> None:
        """Archive the scratch query so it does not linger in the query list."""
        if self._query_id is not None:
            self._archive_query(self._query_id)
            self._query_id = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _execute(self, query_id: int, performance: str) -> str:
        response = self._request(
            "POST", f"/query/{query_id}/execute", json={"performance": performance}
        )
        return response.json()["execution_id"]

    def _wait(self, execution_id: str, label: str) -> None:
        deadline = time.time() + DUNE_MAX_WAIT_SECONDS
        while time.time() < deadline:
            state = self._request("GET", f"/execution/{execution_id}/status").json()
            status = state.get("state")
            if status in _TERMINAL_OK:
                return
            if status in _TERMINAL_BAD:
                raise DuneError(
                    f"dune execution {execution_id} ({label}) ended in {status}: "
                    f"{json.dumps(state)[:500]}"
                )
            logger.debug("dune %r: %s", label, status)
            time.sleep(DUNE_POLL_SECONDS)
        raise DuneError(
            f"dune execution {execution_id} ({label}) exceeded "
            f"{DUNE_MAX_WAIT_SECONDS}s"
        )

    def _fetch_results(self, execution_id: str) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        offset = 0
        while True:
            response = self._request(
                "GET",
                f"/execution/{execution_id}/results/csv",
                params={"limit": DUNE_RESULT_PAGE_SIZE, "offset": offset},
            )
            text = response.text
            if not text.strip():
                break
            page = pd.read_csv(io.StringIO(text), na_values=DUNE_NULL_TOKENS)
            if page.empty:
                break
            frames.append(page)
            if len(page) < DUNE_RESULT_PAGE_SIZE:
                break
            offset += len(page)
            logger.debug("dune results: %d rows so far", offset)
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def _archive_query(self, query_id: int) -> None:
        """Ad-hoc queries are disposable; leaving them piles up the query list."""
        try:
            self._request("POST", f"/query/{query_id}/archive", max_retries=1)
        except Exception as exc:  # never let cleanup mask a real failure
            logger.debug("could not archive dune query %s: %s", query_id, exc)

    # -- diagnostics -------------------------------------------------------

    def probe(self, table: str, limit: int = 5) -> pd.DataFrame:
        """Peek at a table's columns. Used to confirm schemas before writing SQL."""
        df = self.run_sql(f"SELECT * FROM {table} LIMIT {int(limit)}", label=f"probe {table}")
        logger.info("probe %s columns: %s", table, list(df.columns))
        return df

    def summary(self) -> dict[str, Any]:
        return {
            "executions": len(self.executions),
            "rows": sum(e["rows"] for e in self.executions),
            "seconds": round(sum(e["seconds"] for e in self.executions), 1),
            "detail": self.executions,
        }


def prepare_sql(sql: str, limit: int | None = None) -> str:
    """Normalise whitespace/semicolons and optionally wrap in a row cap.

    The wrap is what `--limit` means for a Dune pipeline: a real but tiny
    execution, which is how we smoke-test SQL without paying for a full scan.
    """
    cleaned = sql.strip().rstrip(";").strip()
    if limit:
        cleaned = f"SELECT * FROM (\n{cleaned}\n) LIMIT {int(limit)}"
    return cleaned
