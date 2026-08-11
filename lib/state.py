"""Per-pipeline watermarks in state/<pipeline>.json.

The watermark is the newest *event* timestamp a run actually saw, not the wall
clock at run time — so a pipeline that lags behind the chain tip resumes from
where the data ended, not from where the clock was.

It is written only after a run's manifest is sealed. A crash mid-run therefore
re-reads the same window next time, which is safe because ingestion MERGEs.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from config.settings import STATE_DIR

logger = logging.getLogger(__name__)


def _state_path(pipeline: str, base_dir: Path | None = None) -> Path:
    return Path(base_dir or STATE_DIR) / f"{pipeline}.json"


def read_state(pipeline: str, base_dir: Path | None = None) -> dict:
    path = _state_path(pipeline, base_dir)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        logger.warning("state file %s is corrupt; treating as empty", path)
        return {}


def write_state(pipeline: str, state: dict, base_dir: Path | None = None) -> Path:
    path = _state_path(pipeline, base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, default=str))
    return path


def get_watermark(pipeline: str, base_dir: Path | None = None) -> datetime | None:
    raw = read_state(pipeline, base_dir).get("watermark")
    if not raw:
        return None
    return parse_ts(raw)


def set_watermark(
    pipeline: str,
    watermark,
    run_ts: str | None = None,
    base_dir: Path | None = None,
) -> None:
    if watermark is None:
        logger.info("%s: no rows in window, watermark left unchanged", pipeline)
        return
    state = read_state(pipeline, base_dir)
    previous = state.get("watermark")
    # Callers pass a datetime from a DataFrame, but manifests and CLI flags carry
    # ISO strings; accept either rather than making every caller convert.
    new = parse_ts(watermark)
    if new is None:
        logger.warning(
            "%s: watermark %r is not a parseable timestamp, leaving it unchanged",
            pipeline,
            watermark,
        )
        return
    # A short window can legitimately return older max-timestamps than a prior
    # wide run; never let the watermark walk backwards and re-scan history.
    if previous:
        prior = parse_ts(previous)
        if prior and new <= prior:
            logger.info(
                "%s: watermark %s not newer than %s, keeping existing",
                pipeline,
                new.isoformat(),
                prior.isoformat(),
            )
            return
    state["watermark"] = new.isoformat()
    state["last_run_ts"] = run_ts
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    write_state(pipeline, state, base_dir)
    logger.info("%s: watermark -> %s", pipeline, state["watermark"])


def parse_ts(value) -> datetime | None:
    """Parse an ISO-8601 / Dune timestamp string into an aware UTC datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return to_utc(value)
    text = str(value).strip()
    if not text or text.lower() in {"nan", "nat", "none"}:
        return None
    text = text.replace("Z", "+00:00")
    # Dune returns "2025-01-01 00:00:00.000 UTC" in some result encodings.
    text = text.replace(" UTC", "+00:00")
    try:
        return to_utc(datetime.fromisoformat(text))
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return to_utc(datetime.strptime(text, fmt))
            except ValueError:
                continue
    logger.warning("could not parse timestamp %r", value)
    return None


def to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def max_timestamp(*series) -> datetime | None:
    """Largest parseable timestamp across any number of pandas Series/iterables."""
    best: datetime | None = None
    for s in series:
        if s is None:
            continue
        for value in s:
            parsed = parse_ts(value)
            if parsed and (best is None or parsed > best):
                best = parsed
    return best
