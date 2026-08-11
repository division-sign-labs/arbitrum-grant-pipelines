"""Shared fixtures.

Three jobs:

1. Make the default run provably offline. `_no_network` breaks `socket` so a
   forgotten mock fails loudly instead of quietly hitting a live API (and
   burning a Dune credit) on someone's laptop.
2. Redirect the filesystem layout. `config.settings` resolves DATA_DIR /
   STATE_DIR / SEEDS_DIR at import time and five modules bind their own
   references to those objects, so patching `config.settings` alone is not
   enough — every binding is rewritten, and `_LAYOUT_BINDINGS` is asserted
   against the real imports so a new binding cannot silently escape.
3. Supply recorded payloads and a DuneRunner stand-in, so pipeline code can be
   driven end to end without an API key.
"""

from __future__ import annotations

import json
import socket
import time
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import pytest

import config.settings as settings
import lib.dune
import lib.runs
import lib.seeds
import lib.state
import scripts.run_all

FIXTURE_DIR = Path(__file__).parent / "fixtures"

# (module, attribute, which tmp directory) for every module-level binding of a
# path constant. Checked against the modules' real imports by
# tests/test_layout_fixture.py so this list cannot rot.
_LAYOUT_BINDINGS = (
    (settings, "DATA_DIR", "data"),
    (settings, "STATE_DIR", "state"),
    (settings, "SEEDS_DIR", "seeds"),
    (lib.runs, "DATA_DIR", "data"),
    (lib.state, "STATE_DIR", "state"),
    (lib.seeds, "DATA_DIR", "data"),
    (lib.seeds, "SEEDS_DIR", "seeds"),
    (lib.dune, "DATA_DIR", "data"),
    (scripts.run_all, "SEEDS_DIR", "seeds"),
)


@dataclass(frozen=True)
class Layout:
    """Where the redirected DATA_DIR / STATE_DIR / SEEDS_DIR live for one test."""

    root: Path
    data: Path
    state: Path
    seeds: Path


@pytest.fixture(autouse=True)
def _no_network(request, monkeypatch):
    """Fail any test that opens a real socket, unless it is marked `live`."""
    if request.node.get_closest_marker("live"):
        return

    def blocked(*args, **kwargs):
        raise AssertionError(
            "the default test run is offline — this test opened a real socket. "
            "Mock it with the `requests_mock` fixture, or mark it @pytest.mark.live."
        )

    monkeypatch.setattr(socket.socket, "connect", blocked)
    monkeypatch.setattr(socket.socket, "connect_ex", blocked)
    monkeypatch.setattr(socket, "create_connection", blocked)


@pytest.fixture(autouse=True)
def layout(tmp_path, monkeypatch) -> Layout:
    """Point every DATA_DIR / STATE_DIR / SEEDS_DIR binding at a fresh tmp_path."""
    dirs = {name: tmp_path / name for name in ("data", "state", "seeds")}
    for path in dirs.values():
        path.mkdir()
    for module, attribute, key in _LAYOUT_BINDINGS:
        monkeypatch.setattr(module, attribute, dirs[key])
    return Layout(root=tmp_path, data=dirs["data"], state=dirs["state"], seeds=dirs["seeds"])


@pytest.fixture
def recorded_sleep(monkeypatch) -> list[float]:
    """Replace `time.sleep` with a recorder.

    Retry and polling paths sleep for seconds to minutes by design; the tests
    assert on *what* they would have slept, which is the interesting part, and
    the suite stays instant. `lib.http` and `lib.dune` both `import time`, so
    patching the module object covers both.
    """
    slept: list[float] = []

    def fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr(time, "sleep", fake_sleep)
    return slept


def load_fixture(name: str):
    """Read a recorded API payload from tests/fixtures/<name>.json."""
    return json.loads((FIXTURE_DIR / f"{name}.json").read_text())


@pytest.fixture
def fixture_json():
    return load_fixture


@dataclass
class FakeDuneRunner:
    """Stand-in for `lib.dune.DuneRunner` that never leaves the process.

    `responses` maps a substring of the query label to either a DataFrame to
    return or an exception to raise, which is how the degrade-gracefully paths
    (a chain `dex.trades` does not cover, a table that has moved) get exercised
    without a Dune key. Anything unmatched returns an empty frame, mirroring a
    query that ran fine and found nothing.
    """

    responses: dict = field(default_factory=dict)
    dry_run: bool = False
    calls: list = field(default_factory=list)
    executions: list = field(default_factory=list)

    def run_sql(
        self,
        sql: str,
        *,
        label: str = "query",
        limit=None,
        use_cache: bool = True,
        performance=None,
    ) -> pd.DataFrame:
        self.calls.append({"sql": sql, "label": label, "limit": limit})
        if self.dry_run:
            return pd.DataFrame()
        for key, value in self.responses.items():
            if key in label:
                if isinstance(value, Exception):
                    raise value
                frame = value.copy()
                self.executions.append({"label": label, "query_id": 0, "rows": len(frame), "seconds": 0.0})
                return frame
        return pd.DataFrame()

    def summary(self) -> dict:
        return {
            "executions": len(self.executions),
            "rows": sum(e["rows"] for e in self.executions),
            "seconds": 0.0,
            "detail": self.executions,
        }

    def labels(self) -> list[str]:
        return [call["label"] for call in self.calls]


@pytest.fixture
def fake_dune():
    return FakeDuneRunner
