"""lib.state — watermarks and the timestamp parsing every pipeline funnels through.

parse_ts is the single point where four different upstream timestamp spellings
(Dune's "... UTC", Neynar's "...Z", plain ISO, bare dates) become one aware UTC
datetime, so it gets a table of every shape observed in the wild plus the
garbage cases that must return None rather than raise.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from lib import state

UTC = timezone.utc


def test_watermark_round_trip(layout):
    assert state.get_watermark("linked_wallets") is None

    state.set_watermark("linked_wallets", datetime(2026, 3, 1, 12, 0, tzinfo=UTC), run_ts="20260301T120000Z")

    assert state.get_watermark("linked_wallets") == datetime(2026, 3, 1, 12, 0, tzinfo=UTC)
    stored = json.loads((layout.state / "linked_wallets.json").read_text())
    assert stored["watermark"] == "2026-03-01T12:00:00+00:00"
    assert stored["last_run_ts"] == "20260301T120000Z"
    assert "updated_at" in stored


def test_set_watermark_never_moves_backwards(layout):
    state.set_watermark("token_buyers", datetime(2026, 6, 1, tzinfo=UTC), run_ts="a")
    state.set_watermark("token_buyers", datetime(2026, 5, 1, tzinfo=UTC), run_ts="b")

    assert state.get_watermark("token_buyers") == datetime(2026, 6, 1, tzinfo=UTC)
    # The rejected write must not touch the bookkeeping either.
    assert json.loads((layout.state / "token_buyers.json").read_text())["last_run_ts"] == "a"


def test_set_watermark_rejects_an_equal_timestamp(layout):
    state.set_watermark("token_buyers", datetime(2026, 6, 1, tzinfo=UTC), run_ts="a")
    state.set_watermark("token_buyers", datetime(2026, 6, 1, tzinfo=UTC), run_ts="b")

    assert json.loads((layout.state / "token_buyers.json").read_text())["last_run_ts"] == "a"


def test_set_watermark_advances_and_preserves_unrelated_keys(layout):
    state.write_state("popular_tokens", {"watermark": "2026-01-01T00:00:00+00:00", "custom": 7})
    state.set_watermark("popular_tokens", datetime(2026, 4, 1, tzinfo=UTC), run_ts="c")

    stored = json.loads((layout.state / "popular_tokens.json").read_text())
    assert stored["watermark"] == "2026-04-01T00:00:00+00:00"
    assert stored["custom"] == 7


def test_set_watermark_of_none_is_a_no_op(layout):
    state.set_watermark("brand_engagement", None, run_ts="a")

    assert not (layout.state / "brand_engagement.json").exists()
    assert state.get_watermark("brand_engagement") is None


def test_set_watermark_normalises_a_naive_timestamp_to_utc(layout):
    state.set_watermark("arb_cohort", datetime(2026, 7, 1, 9, 0), run_ts="a")

    assert state.get_watermark("arb_cohort") == datetime(2026, 7, 1, 9, 0, tzinfo=UTC)


def test_set_watermark_compares_across_offsets(layout):
    """A +02:00 timestamp that is *earlier* in UTC must still be rejected."""
    state.set_watermark("arb_cohort", datetime(2026, 7, 1, 12, 0, tzinfo=UTC), run_ts="a")
    state.set_watermark(
        "arb_cohort",
        datetime(2026, 7, 1, 13, 0, tzinfo=timezone(timedelta(hours=2))),  # 11:00Z
        run_ts="b",
    )

    assert state.get_watermark("arb_cohort") == datetime(2026, 7, 1, 12, 0, tzinfo=UTC)


def test_set_watermark_accepts_an_iso_string_as_well_as_a_datetime(layout):
    # Pipelines pass the datetime max_timestamp returns, but manifests and CLI
    # flags carry ISO strings; the read side goes through parse_ts, so the write
    # side has to as well or the two are asymmetric.
    state.set_watermark("linked_wallets", "2026-01-01T00:00:00Z")

    assert state.get_watermark("linked_wallets") == datetime(2026, 1, 1, tzinfo=UTC)


def test_set_watermark_ignores_an_unparseable_value_rather_than_crashing(layout):
    state.set_watermark("linked_wallets", datetime(2026, 1, 1, tzinfo=UTC), run_ts="a")
    state.set_watermark("linked_wallets", "not a timestamp", run_ts="b")

    # A garbage watermark must not clear or corrupt a good one.
    assert state.get_watermark("linked_wallets") == datetime(2026, 1, 1, tzinfo=UTC)
    assert json.loads((layout.state / "linked_wallets.json").read_text())["last_run_ts"] == "a"


def test_set_watermark_of_a_pandas_timestamp(layout):
    state.set_watermark("linked_wallets", pd.Timestamp("2026-02-02T10:00:00Z"))

    assert state.get_watermark("linked_wallets") == datetime(2026, 2, 2, 10, 0, tzinfo=UTC)


def test_read_state_treats_corrupt_json_as_empty(layout):
    (layout.state / "broken.json").write_text("{not json")

    assert state.read_state("broken") == {}
    assert state.get_watermark("broken") is None


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("2026-01-02T03:04:05+00:00", datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)),
        ("2026-01-02T03:04:05Z", datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)),
        ("2026-01-02T03:04:05.123Z", datetime(2026, 1, 2, 3, 4, 5, 123000, tzinfo=UTC)),
        # Dune's CSV encoding of a timestamp column.
        ("2026-01-02 03:04:05.000 UTC", datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)),
        ("2026-01-02 03:04:05 UTC", datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)),
        # Naive: assumed UTC, never local.
        ("2026-01-02 03:04:05", datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)),
        ("2026-01-02 03:04:05.500", datetime(2026, 1, 2, 3, 4, 5, 500000, tzinfo=UTC)),
        ("2026-01-02", datetime(2026, 1, 2, tzinfo=UTC)),
        # An offset is respected and converted, not discarded.
        ("2026-01-02T05:04:05+02:00", datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)),
        ("  2026-01-02T03:04:05Z  ", datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)),
    ],
)
def test_parse_ts_accepts_every_upstream_spelling(raw, expected):
    assert state.parse_ts(raw) == expected


@pytest.mark.parametrize("raw", [None, "", "   ", "nan", "NaN", "NaT", "none", "not a date", "2026-13-45",
                                 float("nan")])
def test_parse_ts_returns_none_for_missing_or_garbage(raw):
    assert state.parse_ts(raw) is None


def test_parse_ts_passes_through_datetimes_and_pandas_timestamps():
    assert state.parse_ts(datetime(2026, 1, 1)) == datetime(2026, 1, 1, tzinfo=UTC)
    assert state.parse_ts(datetime(2026, 1, 1, tzinfo=UTC)) == datetime(2026, 1, 1, tzinfo=UTC)
    assert state.parse_ts(pd.Timestamp("2026-01-01T00:00:00Z")) == datetime(2026, 1, 1, tzinfo=UTC)


def test_to_utc_localises_naive_and_converts_aware():
    assert state.to_utc(datetime(2026, 1, 1, 5)) == datetime(2026, 1, 1, 5, tzinfo=UTC)
    aware = datetime(2026, 1, 1, 5, tzinfo=timezone(timedelta(hours=-5)))
    assert state.to_utc(aware) == datetime(2026, 1, 1, 10, tzinfo=UTC)


def test_max_timestamp_across_mixed_series_and_junk():
    dune_style = pd.Series(["2026-01-02 03:04:05.000 UTC", "bogus", None])
    neynar_style = pd.Series(["2026-01-03T00:00:00Z", ""])

    assert state.max_timestamp(dune_style, neynar_style) == datetime(2026, 1, 3, tzinfo=UTC)


def test_max_timestamp_of_nothing_is_none():
    assert state.max_timestamp() is None
    assert state.max_timestamp(None, pd.Series([], dtype="object")) is None
    assert state.max_timestamp(pd.Series(["nope", "also nope"])) is None
