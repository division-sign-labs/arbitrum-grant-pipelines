"""lib.dune — the only module in the repo that can spend money.

Everything here runs against `requests_mock` bound to the real DUNE_API_BASE, so
the create/execute/poll/paginate/archive choreography and the retry policy are
exercised for real without an API key. The cache tests matter because a cache
miss is a re-execution: a broken TTL is a silent credit leak.
"""

from __future__ import annotations

import os
import time

import pytest
import requests

from config.settings import DUNE_API_BASE
from lib import dune
from lib.dune import DuneError, DuneRunner, prepare_sql

QUERY_ID = 42
EXEC_ID = "01HZTESTEXECUTION"

CREATE_URL = f"{DUNE_API_BASE}/query"
EXECUTE_URL = f"{DUNE_API_BASE}/query/{QUERY_ID}/execute"
STATUS_URL = f"{DUNE_API_BASE}/execution/{EXEC_ID}/status"
RESULTS_URL = f"{DUNE_API_BASE}/execution/{EXEC_ID}/results/csv"
ARCHIVE_URL = f"{DUNE_API_BASE}/query/{QUERY_ID}/archive"
PATCH_URL = f"{DUNE_API_BASE}/query/{QUERY_ID}"


# --- prepare_sql ---------------------------------------------------------


def test_prepare_sql_strips_whitespace_and_trailing_semicolons():
    assert prepare_sql("  SELECT 1 ;  ") == "SELECT 1"
    assert prepare_sql("SELECT 1;") == "SELECT 1"
    # Only the trailing terminator goes; an internal one would be a real
    # multi-statement query and must not be silently mangled.
    assert prepare_sql("SELECT ';' AS x;") == "SELECT ';' AS x"


def test_prepare_sql_wraps_in_a_row_cap_when_limited():
    wrapped = prepare_sql("SELECT * FROM arbitrum.transactions", limit=5)

    assert wrapped == "SELECT * FROM (\nSELECT * FROM arbitrum.transactions\n) LIMIT 5"
    assert prepare_sql("SELECT 1", limit="7").endswith("LIMIT 7")


@pytest.mark.parametrize("limit", [None, 0])
def test_prepare_sql_does_not_wrap_without_a_limit(limit):
    assert prepare_sql("SELECT 1", limit=limit) == "SELECT 1"


# --- helpers -------------------------------------------------------------


def _csv(rows: int, offset: int = 0) -> str:
    body = "\n".join(f"0xaddr{offset + i},{offset + i}" for i in range(rows))
    return "address,n\n" + body + ("\n" if body else "")


def _row_count(page: str) -> int:
    lines = [line for line in page.splitlines() if line.strip()]
    return max(len(lines) - 1, 0)


def register_happy_path(m, statuses=None, results=None):
    """Wire up one complete execution. `results` is a list of CSV page bodies."""
    m.post(CREATE_URL, json={"query_id": QUERY_ID})
    m.post(EXECUTE_URL, json={"execution_id": EXEC_ID})
    m.get(
        STATUS_URL,
        [{"json": {"state": s}} for s in (statuses or ["QUERY_STATE_COMPLETED"])],
    )

    # The client pages by row offset, so serve pages the way Dune does: keyed on
    # the offset the previous page ended at.
    pages = list(results if results is not None else [_csv(2)])
    by_offset: dict[int, str] = {}
    cursor = 0
    for page in pages:
        by_offset[cursor] = page
        cursor += _row_count(page)

    def paginate(request, context):
        offset = int(request.qs.get("offset", ["0"])[0])
        return by_offset.get(offset, "address,n\n")

    m.get(RESULTS_URL, text=paginate)
    m.post(ARCHIVE_URL, json={})
    # Subsequent run_sql calls rewrite the scratch query rather than create one.
    m.patch(PATCH_URL, json={})


def runner(tmp_path, **kwargs) -> DuneRunner:
    kwargs.setdefault("cache_dir", tmp_path / "dune-cache")
    kwargs.setdefault("cache_ttl_hours", 0)
    return DuneRunner(api_key="test-key", **kwargs)


# --- construction --------------------------------------------------------


def test_a_missing_api_key_fails_before_any_request(monkeypatch):
    monkeypatch.setattr(dune, "DUNE_API_KEY", None)

    with pytest.raises(RuntimeError, match="DUNE_API_KEY is not set"):
        DuneRunner()


def test_dry_run_needs_no_key_and_makes_no_requests(monkeypatch, requests_mock):
    monkeypatch.setattr(dune, "DUNE_API_KEY", None)

    frame = DuneRunner(dry_run=True).run_sql("SELECT 1;", label="probe")

    assert frame.empty
    assert requests_mock.call_count == 0


def test_the_api_key_is_sent_as_a_header(tmp_path, requests_mock, recorded_sleep):
    register_happy_path(requests_mock)

    runner(tmp_path).run_sql("SELECT 1")

    assert requests_mock.request_history[0].headers["X-DUNE-API-KEY"] == "test-key"


# --- the execution choreography -----------------------------------------


def test_run_sql_creates_executes_polls_and_fetches(tmp_path, requests_mock, recorded_sleep):
    register_happy_path(
        requests_mock,
        statuses=["QUERY_STATE_PENDING", "QUERY_STATE_EXECUTING", "QUERY_STATE_COMPLETED"],
        results=[_csv(2)],
    )

    frame = runner(tmp_path).run_sql("SELECT address, n FROM t;", label="deployers")

    assert list(frame.columns) == ["address", "n"]
    assert len(frame) == 2

    # requests_mock lowercases the path it records.
    calls = [(r.method, r.path) for r in requests_mock.request_history]
    assert calls[0] == ("POST", "/api/v1/query")
    assert calls[1] == ("POST", f"/api/v1/query/{QUERY_ID}/execute")
    assert [c for c in calls if c[1].endswith("/status")] == [
        ("GET", f"/api/v1/execution/{EXEC_ID.lower()}/status")
    ] * 3
    # Archiving is deferred to close(): the scratch query is reused across calls.
    assert not any(c[1].endswith("/archive") for c in calls)

    # Two non-terminal polls -> two waits of DUNE_POLL_SECONDS.
    assert recorded_sleep == [dune.DUNE_POLL_SECONDS, dune.DUNE_POLL_SECONDS]

    created = requests_mock.request_history[0].json()
    assert created["query_sql"] == "SELECT address, n FROM t"


def test_a_second_query_patches_the_scratch_query_instead_of_creating_one(
    tmp_path, requests_mock, recorded_sleep
):
    # Dune caps how many saved queries an account may hold ("Max number of
    # private queries reached"), and a backfill is thousands of executions, so
    # creating one per execution stops the run regardless of compute budget.
    register_happy_path(requests_mock, results=[_csv(1)])
    runner_ = runner(tmp_path)

    runner_.run_sql("SELECT 1 AS a", label="first")
    runner_.run_sql("SELECT 2 AS b", label="second")

    creates = [r for r in requests_mock.request_history if r.method == "POST" and r.path == "/api/v1/query"]
    patches = [r for r in requests_mock.request_history if r.method == "PATCH"]
    assert len(creates) == 1
    assert len(patches) == 1
    assert patches[0].json()["query_sql"] == "SELECT 2 AS b"
    assert runner_._query_id == QUERY_ID


def test_close_archives_the_scratch_query_once(tmp_path, requests_mock, recorded_sleep):
    register_happy_path(requests_mock, results=[_csv(1)])
    runner_ = runner(tmp_path)
    runner_.run_sql("SELECT 1")

    runner_.close()
    runner_.close()  # idempotent: nothing left to archive

    archives = [r for r in requests_mock.request_history if r.path.endswith("/archive")]
    assert len(archives) == 1
    assert runner_._query_id is None


def test_run_sql_applies_limit_as_a_real_but_tiny_execution(tmp_path, requests_mock, recorded_sleep):
    register_happy_path(requests_mock, results=[_csv(1)])

    runner(tmp_path).run_sql("SELECT 1", limit=3)

    assert requests_mock.request_history[0].json()["query_sql"].endswith("LIMIT 3")


def test_a_failed_execution_raises_and_leaves_the_scratch_query_reusable(
    tmp_path, requests_mock, recorded_sleep
):
    requests_mock.post(CREATE_URL, json={"query_id": QUERY_ID})
    requests_mock.post(EXECUTE_URL, json={"execution_id": EXEC_ID})
    requests_mock.get(STATUS_URL, json={"state": "QUERY_STATE_FAILED", "error": {"type": "SYNTAX"}})
    requests_mock.post(ARCHIVE_URL, json={})
    requests_mock.patch(PATCH_URL, json={})
    runner_ = runner(tmp_path)

    with pytest.raises(DuneError, match="QUERY_STATE_FAILED"):
        runner_.run_sql("SELECT bad")

    # One leg failing must not discard the scratch query: popular_tokens runs
    # four independent legs through one runner and the later ones still need it.
    assert runner_._query_id == QUERY_ID


def test_polling_gives_up_at_the_deadline(tmp_path, requests_mock, monkeypatch, recorded_sleep):
    monkeypatch.setattr(dune, "DUNE_MAX_WAIT_SECONDS", 0)
    requests_mock.post(CREATE_URL, json={"query_id": QUERY_ID})
    requests_mock.post(EXECUTE_URL, json={"execution_id": EXEC_ID})
    requests_mock.get(STATUS_URL, json={"state": "QUERY_STATE_EXECUTING"})
    requests_mock.post(ARCHIVE_URL, json={})

    with pytest.raises(DuneError, match="exceeded"):
        runner(tmp_path).run_sql("SELECT 1")


# --- result pagination ---------------------------------------------------


def test_results_are_paginated_until_a_short_page(tmp_path, requests_mock, monkeypatch, recorded_sleep):
    monkeypatch.setattr(dune, "DUNE_RESULT_PAGE_SIZE", 2)
    register_happy_path(requests_mock, results=[_csv(2, 0), _csv(2, 2), _csv(1, 4)])

    frame = runner(tmp_path).run_sql("SELECT 1")

    assert len(frame) == 5
    assert list(frame["n"]) == [0, 1, 2, 3, 4]
    offsets = [
        int(r.qs["offset"][0]) for r in requests_mock.request_history if r.path.endswith("/csv")
    ]
    assert offsets == [0, 2, 4]


def test_pagination_stops_on_an_empty_page_after_a_full_one(
    tmp_path, requests_mock, monkeypatch, recorded_sleep
):
    monkeypatch.setattr(dune, "DUNE_RESULT_PAGE_SIZE", 2)
    # A result whose row count is an exact multiple of the page size can only be
    # detected as finished by fetching the empty page after it.
    register_happy_path(requests_mock, results=[_csv(2, 0), _csv(2, 2), "address,n\n"])

    frame = runner(tmp_path).run_sql("SELECT 1")

    assert len(frame) == 4
    assert list(frame["n"]) == [0, 1, 2, 3]


def test_an_empty_result_set_yields_an_empty_frame(tmp_path, requests_mock, recorded_sleep):
    register_happy_path(requests_mock, results=[""])

    assert runner(tmp_path).run_sql("SELECT 1").empty


# --- retries -------------------------------------------------------------


def test_a_429_is_retried_and_retry_after_is_honoured(tmp_path, requests_mock, recorded_sleep):
    requests_mock.post(
        CREATE_URL,
        [
            {"status_code": 429, "headers": {"Retry-After": "7"}, "text": "slow down"},
            {"status_code": 200, "json": {"query_id": QUERY_ID}},
        ],
    )
    requests_mock.post(EXECUTE_URL, json={"execution_id": EXEC_ID})
    requests_mock.get(STATUS_URL, json={"state": "QUERY_STATE_COMPLETED"})
    requests_mock.get(RESULTS_URL, text=_csv(1))
    requests_mock.post(ARCHIVE_URL, json={})

    frame = runner(tmp_path).run_sql("SELECT 1")

    assert len(frame) == 1
    # Retry-After beats the default 2s backoff rather than being ignored.
    assert recorded_sleep == [7.0]


def test_backoff_doubles_when_there_is_no_retry_after(tmp_path, requests_mock, recorded_sleep):
    requests_mock.post(
        CREATE_URL,
        [
            {"status_code": 503, "text": "unavailable"},
            {"status_code": 502, "text": "bad gateway"},
            {"status_code": 200, "json": {"query_id": QUERY_ID}},
        ],
    )
    requests_mock.post(EXECUTE_URL, json={"execution_id": EXEC_ID})
    requests_mock.get(STATUS_URL, json={"state": "QUERY_STATE_COMPLETED"})
    requests_mock.get(RESULTS_URL, text=_csv(1))
    requests_mock.post(ARCHIVE_URL, json={})

    runner(tmp_path).run_sql("SELECT 1")

    assert recorded_sleep == [2.0, 4.0]


def test_a_non_retryable_status_fails_immediately(tmp_path, requests_mock, recorded_sleep):
    requests_mock.post(CREATE_URL, status_code=400, text="bad sql: no such table")

    with pytest.raises(DuneError, match="no such table"):
        runner(tmp_path).run_sql("SELECT * FROM nope")

    # One attempt only: a syntax error is not going to fix itself.
    assert requests_mock.call_count == 1
    assert recorded_sleep == []


def test_retries_are_bounded_and_then_raise(tmp_path, requests_mock, recorded_sleep):
    requests_mock.post(CREATE_URL, status_code=500, text="boom")

    with pytest.raises(DuneError, match="failed after 6 retries"):
        runner(tmp_path).run_sql("SELECT 1")

    assert requests_mock.call_count == 7  # the first attempt plus six retries
    assert len(recorded_sleep) == 6


def test_a_connection_error_is_retried(tmp_path, requests_mock, recorded_sleep):
    requests_mock.post(
        CREATE_URL,
        [
            {"exc": requests.exceptions.ConnectTimeout},
            {"status_code": 200, "json": {"query_id": QUERY_ID}},
        ],
    )
    requests_mock.post(EXECUTE_URL, json={"execution_id": EXEC_ID})
    requests_mock.get(STATUS_URL, json={"state": "QUERY_STATE_COMPLETED"})
    requests_mock.get(RESULTS_URL, text=_csv(1))
    requests_mock.post(ARCHIVE_URL, json={})

    assert len(runner(tmp_path).run_sql("SELECT 1")) == 1


def test_a_failing_archive_never_masks_a_successful_run(tmp_path, requests_mock, recorded_sleep):
    register_happy_path(requests_mock, results=[_csv(1)])
    requests_mock.post(ARCHIVE_URL, status_code=500, text="nope")

    assert len(runner(tmp_path).run_sql("SELECT 1")) == 1


# --- caching -------------------------------------------------------------


def test_a_cache_hit_skips_the_whole_execution(tmp_path, requests_mock, recorded_sleep):
    register_happy_path(requests_mock, results=[_csv(2)])
    cached = runner(tmp_path, cache_ttl_hours=24)

    first = cached.run_sql("SELECT address, n FROM t")
    calls_after_first = requests_mock.call_count
    second = cached.run_sql("SELECT address, n FROM t")

    assert requests_mock.call_count == calls_after_first
    assert second.to_dict("records") == first.to_dict("records")
    assert len(cached.executions) == 1


def test_a_different_query_misses_the_cache(tmp_path, requests_mock, recorded_sleep):
    register_happy_path(requests_mock, results=[_csv(1)])
    cached = runner(tmp_path, cache_ttl_hours=24)

    cached.run_sql("SELECT 1")
    calls_after_first = requests_mock.call_count
    cached.run_sql("SELECT 2")

    assert requests_mock.call_count > calls_after_first
    assert len(cached.executions) == 2


def test_an_expired_cache_entry_re_executes(tmp_path, requests_mock, recorded_sleep):
    register_happy_path(requests_mock, results=[_csv(1)])
    cached = runner(tmp_path, cache_ttl_hours=1)
    sql = "SELECT 1"

    cached.run_sql(sql)
    path = cached._cache_path(prepare_sql(sql))
    assert path.exists()
    stale = time.time() - 2 * 3600
    os.utime(path, (stale, stale))
    calls_after_first = requests_mock.call_count

    cached.run_sql(sql)

    assert requests_mock.call_count > calls_after_first


def test_caching_is_off_when_the_ttl_is_zero(tmp_path, requests_mock, recorded_sleep):
    register_happy_path(requests_mock, results=[_csv(1)])
    uncached = runner(tmp_path, cache_ttl_hours=0)

    uncached.run_sql("SELECT 1")
    uncached.run_sql("SELECT 1")

    assert len(uncached.executions) == 2
    assert not (tmp_path / "dune-cache").exists()


def test_use_cache_false_bypasses_a_warm_cache(tmp_path, requests_mock, recorded_sleep):
    register_happy_path(requests_mock, results=[_csv(1)])
    cached = runner(tmp_path, cache_ttl_hours=24)

    cached.run_sql("SELECT 1")
    cached.run_sql("SELECT 1", use_cache=False)

    assert len(cached.executions) == 2


def test_the_cache_key_separates_performance_tiers(tmp_path):
    small = runner(tmp_path, performance="medium")
    large = runner(tmp_path, performance="large")

    assert small._cache_path("SELECT 1") != large._cache_path("SELECT 1")


# --- diagnostics ---------------------------------------------------------


def test_summary_accumulates_every_execution(tmp_path, requests_mock, recorded_sleep):
    register_happy_path(requests_mock, results=[_csv(2)])
    r = runner(tmp_path)

    r.run_sql("SELECT 1", label="a")
    r.run_sql("SELECT 2", label="b")
    summary = r.summary()

    assert summary["executions"] == 2
    assert summary["rows"] == 4
    assert [d["label"] for d in summary["detail"]] == ["a", "b"]


def test_probe_selects_from_the_named_table(tmp_path, requests_mock, recorded_sleep):
    register_happy_path(requests_mock, results=[_csv(1)])

    runner(tmp_path).probe("arbitrum.creation_traces", limit=3)

    assert (
        requests_mock.request_history[0].json()["query_sql"]
        == "SELECT * FROM arbitrum.creation_traces LIMIT 3"
    )


def test_the_default_cache_dir_lives_under_data_dir(layout):
    assert DuneRunner(api_key="k").cache_dir == layout.data / ".dune_cache"
