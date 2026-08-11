"""lib.cli — window resolution, the thing that decides what every run scans.

Four inputs, four outcomes, and one asymmetry worth pinning down: the overlap
day is added to `query_since` for incremental runs only. Applying it to a
backfill would shift BACKFILL_START a day earlier every time someone re-ran it.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from config.settings import BACKFILL_START, INCREMENTAL_OVERLAP_DAYS
from lib import state
from lib.cli import base_parser, ingestion_parser, resolve_window

UTC = timezone.utc
PIPELINE = "token_buyers"


def parse(*argv):
    return base_parser(PIPELINE).parse_args(list(argv))


def test_since_flag_wins_and_carries_the_overlap(layout):
    window = resolve_window(parse("--since", "2026-06-10T00:00:00Z"), PIPELINE)

    assert window.source == "flag"
    assert window.is_backfill is False
    assert window.since == datetime(2026, 6, 10, tzinfo=UTC)
    assert window.query_since == window.since - timedelta(days=INCREMENTAL_OVERLAP_DAYS)


def test_since_flag_beats_a_stored_watermark(layout):
    state.set_watermark(PIPELINE, datetime(2026, 1, 1, tzinfo=UTC))

    window = resolve_window(parse("--since", "2026-06-10"), PIPELINE)

    assert window.source == "flag"
    assert window.since == datetime(2026, 6, 10, tzinfo=UTC)


def test_watermark_is_used_when_no_flag_is_given(layout):
    state.set_watermark(PIPELINE, datetime(2026, 6, 1, 8, 30, tzinfo=UTC))

    window = resolve_window(parse(), PIPELINE)

    assert window.source == "watermark"
    assert window.is_backfill is False
    assert window.since == datetime(2026, 6, 1, 8, 30, tzinfo=UTC)
    assert window.query_since == window.since - timedelta(days=INCREMENTAL_OVERLAP_DAYS)


def test_backfill_starts_at_backfill_start_with_no_overlap(layout):
    state.set_watermark(PIPELINE, datetime(2026, 6, 1, tzinfo=UTC))

    window = resolve_window(parse("--backfill"), PIPELINE)

    assert window.source == "backfill"
    assert window.is_backfill is True
    assert window.since == state.parse_ts(BACKFILL_START)
    # No overlap: a backfill already starts at the beginning of the record.
    assert window.query_since == window.since


def test_no_flag_and_no_watermark_exits_with_the_backfill_instruction(layout):
    with pytest.raises(SystemExit) as excinfo:
        resolve_window(parse(), PIPELINE)

    message = str(excinfo.value)
    assert "No watermark for 'token_buyers'" in message
    assert "--backfill" in message


def test_unparseable_since_exits_rather_than_silently_scanning_everything(layout):
    with pytest.raises(SystemExit) as excinfo:
        resolve_window(parse("--since", "last tuesday"), PIPELINE)

    assert "not a parseable timestamp" in str(excinfo.value)


def test_window_uses_the_state_dir_it_is_handed(layout, tmp_path):
    elsewhere = tmp_path / "other-state"
    state.set_watermark(PIPELINE, datetime(2026, 9, 9, tzinfo=UTC), base_dir=elsewhere)

    window = resolve_window(parse(), PIPELINE, state_dir=elsewhere)

    assert window.since == datetime(2026, 9, 9, tzinfo=UTC)


def test_window_describe_names_the_mode_and_source(layout):
    window = resolve_window(parse("--backfill"), PIPELINE)

    described = window.describe()
    assert "backfill" in described
    assert window.since.isoformat() in described
    assert window.query_since.isoformat() in described


def test_base_parser_defaults_and_flags():
    defaults = parse()
    assert (defaults.backfill, defaults.since, defaults.dry_run, defaults.limit) == (
        False,
        None,
        False,
        None,
    )

    args = parse("--dry-run", "--limit", "5", "--log-level", "DEBUG")
    assert args.dry_run is True
    assert args.limit == 5
    assert args.log_level == "DEBUG"


def test_ingestion_parser_surface():
    args = ingestion_parser("ingest_token_buyers").parse_args(
        ["--run-id", "20260101T000000Z", "--dry-run", "--batch-size", "250"]
    )

    assert args.run_id == "20260101T000000Z"
    assert args.dry_run is True
    assert args.batch_size == 250
    # The ingestion surface deliberately has no --since/--backfill: an ingest is
    # a pure function of a run directory.
    assert not hasattr(args, "since")
