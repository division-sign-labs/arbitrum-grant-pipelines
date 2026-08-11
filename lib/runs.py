"""Timestamped CSV run directories: data/<data_type>/<run_ts>/*.csv + manifest.json.

A run is only "real" once its manifest exists. A crashed pipeline leaves a
directory behind, but `latest_run()` ignores it, so downstream pipelines and
ingestion never read half-written data.
"""

from __future__ import annotations

import csv
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd

from config.settings import DATA_DIR, PROVENANCE

logger = logging.getLogger(__name__)

RUN_TS_FORMAT = "%Y%m%dT%H%M%SZ"
MANIFEST_NAME = "manifest.json"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def new_run_ts() -> str:
    return utc_now().strftime(RUN_TS_FORMAT)


def _iso(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    return str(value)


class RunWriter:
    """Collects a pipeline's CSVs under one timestamped directory."""

    def __init__(
        self,
        data_type: str,
        dry_run: bool = False,
        run_ts: str | None = None,
        base_dir: Path | None = None,
    ):
        self.data_type = data_type
        self.dry_run = dry_run
        self.run_ts = run_ts or new_run_ts()
        self.base_dir = Path(base_dir or DATA_DIR)
        self.path = self.base_dir / data_type / self.run_ts
        self.files: dict[str, int] = {}
        self._append_handles: dict[str, tuple] = {}
        if not dry_run:
            self.path.mkdir(parents=True, exist_ok=True)

    # -- whole-frame writes ------------------------------------------------

    def write(self, name: str, df: pd.DataFrame) -> Path | None:
        """Write one CSV. `name` is a bare stem, e.g. "wallets"."""
        target = self.path / f"{name}.csv"
        rows = int(len(df))
        if self.dry_run:
            logger.info(
                "[dry-run] would write %s (%d rows, cols=%s)",
                target,
                rows,
                list(df.columns),
            )
            self.files[name] = rows
            return None
        df.to_csv(target, index=False)
        self.files[name] = rows
        logger.info("wrote %s (%d rows)", target, rows)
        return target

    # -- streaming writes (long crawls that must survive a kill) -----------

    def open_append(self, name: str, columns: Sequence[str]):
        """Open a CSV for row-at-a-time appends, creating the header if new.

        Used by the Hyperliquid crawl, where a multi-hour run has to be
        resumable: every row is flushed as soon as it is known.
        """
        if self.dry_run:
            return None
        if name in self._append_handles:
            return self._append_handles[name][1]
        target = self.path / f"{name}.csv"
        existed = target.exists()
        handle = target.open("a", newline="")
        writer = csv.DictWriter(handle, fieldnames=list(columns))
        if not existed:
            writer.writeheader()
            handle.flush()
        else:
            # Resuming: count what is already there so the manifest is right.
            with target.open() as f:
                self.files[name] = max(sum(1 for _ in f) - 1, 0)
        self._append_handles[name] = (handle, writer)
        return writer

    def append_row(self, name: str, row: dict) -> None:
        if self.dry_run:
            return
        _, writer = self._append_handles[name]
        writer.writerow(row)
        self._append_handles[name][0].flush()
        self.files[name] = self.files.get(name, 0) + 1

    def close_appends(self) -> None:
        for handle, _ in self._append_handles.values():
            handle.close()
        self._append_handles.clear()

    # -- completion --------------------------------------------------------

    def finish(
        self,
        params: dict | None = None,
        since=None,
        new_watermark=None,
        notes: Iterable[str] | None = None,
    ) -> dict:
        """Seal the run with a manifest. Only now is it visible to readers."""
        self.close_appends()
        manifest = {
            "data_type": self.data_type,
            "run_ts": self.run_ts,
            "generated_at": utc_now().isoformat(),
            "generated_by": PROVENANCE,
            "files": self.files,
            "row_total": sum(self.files.values()),
            "params": params or {},
            "since": _iso(since),
            "new_watermark": _iso(new_watermark),
            "notes": list(notes or []),
        }
        if self.dry_run:
            logger.info("[dry-run] manifest would be: %s", json.dumps(manifest, indent=2))
            return manifest
        (self.path / MANIFEST_NAME).write_text(json.dumps(manifest, indent=2))
        logger.info(
            "run complete: %s (%d rows across %d files)",
            self.path,
            manifest["row_total"],
            len(self.files),
        )
        return manifest


def run_dirs(data_type: str, base_dir: Path | None = None) -> list[Path]:
    """Completed runs for a data type, oldest first."""
    root = Path(base_dir or DATA_DIR) / data_type
    if not root.is_dir():
        return []
    return sorted(d for d in root.iterdir() if d.is_dir() and (d / MANIFEST_NAME).exists())


def latest_run(data_type: str, base_dir: Path | None = None) -> Path | None:
    dirs = run_dirs(data_type, base_dir)
    return dirs[-1] if dirs else None


def incomplete_runs(data_type: str, base_dir: Path | None = None) -> list[Path]:
    """Directories with data but no manifest — resumable or abandoned."""
    root = Path(base_dir or DATA_DIR) / data_type
    if not root.is_dir():
        return []
    return sorted(
        d for d in root.iterdir() if d.is_dir() and not (d / MANIFEST_NAME).exists()
    )


def resolve_run(
    data_type: str, run_id: str | None = None, base_dir: Path | None = None
) -> Path:
    """A specific run directory by id, or the latest completed one."""
    if run_id:
        path = Path(base_dir or DATA_DIR) / data_type / run_id
        if not path.is_dir():
            raise FileNotFoundError(f"no such run: {path}")
        return path
    path = latest_run(data_type, base_dir)
    if path is None:
        raise FileNotFoundError(
            f"no completed runs for '{data_type}'. Run "
            f"`python -m pipelines.{data_type} --backfill` first."
        )
    return path


def read_manifest(run_dir: Path) -> dict:
    return json.loads((run_dir / MANIFEST_NAME).read_text())


def read_csv(
    data_type: str,
    name: str,
    run_id: str | None = None,
    base_dir: Path | None = None,
    required: bool = True,
) -> pd.DataFrame:
    """Load one CSV from a completed run."""
    run_dir = resolve_run(data_type, run_id, base_dir)
    target = run_dir / f"{name}.csv"
    if not target.exists():
        if required:
            raise FileNotFoundError(f"{target} missing from run {run_dir.name}")
        return pd.DataFrame()
    return pd.read_csv(target)
