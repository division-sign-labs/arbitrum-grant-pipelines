"""lib.runs — the run-directory contract every pipeline and ingest depends on.

The invariant under test throughout: a run is invisible to readers until
`finish()` writes the manifest. Everything else here (append durability, resume
counting, read_csv's missing-file behaviour) exists to make that invariant hold
across a crash.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pandas as pd
import pytest

from lib import runs
from lib.runs import MANIFEST_NAME, RunWriter


def test_write_then_finish_round_trips_through_the_manifest(layout):
    writer = RunWriter("linked_wallets")
    frame = pd.DataFrame(
        [{"fid": 1, "address": "0xabc"}, {"fid": 2, "address": "0xdef"}]
    )

    target = writer.write("wallets", frame)
    manifest = writer.finish(
        params={"chain_id": 42161},
        since=datetime(2026, 1, 1, tzinfo=timezone.utc),
        new_watermark=datetime(2026, 2, 3, 4, 5, 6, tzinfo=timezone.utc),
        notes=["one note"],
    )

    assert target == layout.data / "linked_wallets" / writer.run_ts / "wallets.csv"
    assert manifest["files"] == {"wallets": 2}
    assert manifest["row_total"] == 2
    assert manifest["params"] == {"chain_id": 42161}
    assert manifest["since"] == "2026-01-01T00:00:00+00:00"
    assert manifest["new_watermark"] == "2026-02-03T04:05:06+00:00"
    assert manifest["notes"] == ["one note"]
    assert manifest["data_type"] == "linked_wallets"

    on_disk = json.loads((writer.path / MANIFEST_NAME).read_text())
    assert on_disk == manifest
    assert runs.read_manifest(writer.path) == manifest
    assert pd.read_csv(target).to_dict("records") == frame.to_dict("records")


def test_finish_normalises_naive_timestamps_to_utc(layout):
    writer = RunWriter("token_buyers")
    writer.write("buys", pd.DataFrame(columns=["fid"]))

    manifest = writer.finish(new_watermark=datetime(2026, 5, 5, 6, 7, 8))

    # A naive timestamp is assumed UTC rather than local: state.parse_ts reads
    # this value back and a local-time round trip would move the watermark.
    assert manifest["new_watermark"] == "2026-05-05T06:07:08+00:00"


def test_dry_run_writes_nothing_but_still_reports_the_shape(layout):
    writer = RunWriter("clanker_tokens", dry_run=True)

    assert writer.write("tokens", pd.DataFrame([{"a": 1}, {"a": 2}])) is None
    manifest = writer.finish(params={"x": 1})

    assert manifest["files"] == {"tokens": 2}
    assert not writer.path.exists()
    assert runs.latest_run("clanker_tokens") is None


def test_latest_run_ignores_directories_without_a_manifest(layout):
    finished = RunWriter("bankr_tokens", run_ts="20260101T000000Z")
    finished.write("tokens", pd.DataFrame([{"token_address": "0xa"}]))
    finished.finish()

    crashed = RunWriter("bankr_tokens", run_ts="20260202T000000Z")
    crashed.write("tokens", pd.DataFrame([{"token_address": "0xb"}]))
    # no finish(): the process died before sealing the run

    assert runs.run_dirs("bankr_tokens") == [finished.path]
    # The crashed run sorts *after* the finished one, so this also proves the
    # filter runs before the "newest wins" pick.
    assert runs.latest_run("bankr_tokens") == finished.path
    assert runs.incomplete_runs("bankr_tokens") == [crashed.path]


def test_latest_run_picks_the_newest_completed_run(layout):
    paths = []
    for run_ts in ("20260101T000000Z", "20260301T000000Z", "20260201T000000Z"):
        writer = RunWriter("popular_tokens", run_ts=run_ts)
        writer.write("trades", pd.DataFrame([{"fid": 1}]))
        writer.finish()
        paths.append(writer.path)

    assert runs.latest_run("popular_tokens") == paths[1]
    assert runs.run_dirs("popular_tokens") == [paths[0], paths[2], paths[1]]


def test_run_dirs_of_an_unknown_data_type_is_empty(layout):
    assert runs.run_dirs("never_ran") == []
    assert runs.latest_run("never_ran") is None
    assert runs.incomplete_runs("never_ran") == []


def test_resolve_run_by_id_and_the_error_when_there_is_none(layout):
    writer = RunWriter("arb_cohort", run_ts="20260401T000000Z")
    writer.write("cohort", pd.DataFrame([{"address": "0xa"}]))
    writer.finish()

    assert runs.resolve_run("arb_cohort", "20260401T000000Z") == writer.path

    with pytest.raises(FileNotFoundError, match="no such run"):
        runs.resolve_run("arb_cohort", "20991231T000000Z")

    with pytest.raises(FileNotFoundError, match=r"pipelines\.token_buyers --backfill"):
        runs.resolve_run("token_buyers")


def test_append_rows_are_flushed_immediately_and_survive_a_kill(layout):
    """The Hyperliquid crawl runs for hours; a kill must not lose finished rows."""
    writer = RunWriter("hyperliquid_activity", run_ts="20260501T000000Z")
    writer.open_append("hl_activity", ["address", "cum_volume_usd"])
    writer.append_row("hl_activity", {"address": "0xa", "cum_volume_usd": 1.0})
    writer.append_row("hl_activity", {"address": "0xb", "cum_volume_usd": 2.0})

    # Still open, never closed, no manifest — exactly the state a SIGKILL leaves.
    partial = pd.read_csv(writer.path / "hl_activity.csv")
    assert list(partial["address"]) == ["0xa", "0xb"]
    assert runs.latest_run("hyperliquid_activity") is None


def test_open_append_counts_existing_rows_when_resuming(layout):
    first = RunWriter("hyperliquid_activity", run_ts="20260501T000000Z")
    first.open_append("hl_activity", ["address", "cum_volume_usd"])
    first.append_row("hl_activity", {"address": "0xa", "cum_volume_usd": 1.0})
    first.append_row("hl_activity", {"address": "0xb", "cum_volume_usd": 2.0})
    first.close_appends()

    resumed = RunWriter("hyperliquid_activity", run_ts="20260501T000000Z")
    resumed.open_append("hl_activity", ["address", "cum_volume_usd"])
    resumed.append_row("hl_activity", {"address": "0xc", "cum_volume_usd": 3.0})
    manifest = resumed.finish()

    # The manifest must count the pre-existing rows, not just this process's.
    assert manifest["files"] == {"hl_activity": 3}
    frame = pd.read_csv(resumed.path / "hl_activity.csv")
    assert list(frame["address"]) == ["0xa", "0xb", "0xc"]
    # One header row only — the resume must not re-write it.
    assert (resumed.path / "hl_activity.csv").read_text().count("address,") == 1


def test_open_append_is_idempotent_within_one_writer(layout):
    writer = RunWriter("hyperliquid_activity")
    first = writer.open_append("hl_activity", ["address"])
    second = writer.open_append("hl_activity", ["address"])

    assert first is second
    writer.append_row("hl_activity", {"address": "0xa"})
    assert writer.finish()["files"] == {"hl_activity": 1}


def test_finish_closes_open_append_handles(layout):
    writer = RunWriter("hyperliquid_activity")
    writer.open_append("hl_activity", ["address"])
    writer.append_row("hl_activity", {"address": "0xa"})
    writer.finish()

    assert writer._append_handles == {}


def test_append_in_dry_run_touches_no_files(layout):
    writer = RunWriter("hyperliquid_activity", dry_run=True)

    assert writer.open_append("hl_activity", ["address"]) is None
    writer.append_row("hl_activity", {"address": "0xa"})  # must not raise

    assert not writer.path.exists()


def test_read_csv_missing_file_raises_or_returns_empty(layout):
    writer = RunWriter("linked_wallets")
    writer.write("wallets", pd.DataFrame([{"fid": 1, "address": "0xa"}]))
    writer.finish()

    loaded = runs.read_csv("linked_wallets", "wallets")
    assert loaded.to_dict("records") == [{"fid": 1, "address": "0xa"}]

    with pytest.raises(FileNotFoundError, match="accounts.csv missing from run"):
        runs.read_csv("linked_wallets", "accounts")

    optional = runs.read_csv("linked_wallets", "accounts", required=False)
    assert optional.empty
    assert list(optional.columns) == []


def test_read_csv_still_raises_when_the_run_itself_is_absent(layout):
    # required=False forgives a missing *file*, not a missing *run* — a caller
    # that has not backfilled at all needs to hear about it.
    with pytest.raises(FileNotFoundError, match="no completed runs"):
        runs.read_csv("linked_wallets", "wallets", required=False)


def test_new_run_ts_is_sortable_utc(layout):
    stamp = runs.new_run_ts()

    assert len(stamp) == 16 and stamp.endswith("Z")
    parsed = datetime.strptime(stamp, runs.RUN_TS_FORMAT).replace(tzinfo=timezone.utc)
    assert abs((runs.utc_now() - parsed).total_seconds()) < 60
