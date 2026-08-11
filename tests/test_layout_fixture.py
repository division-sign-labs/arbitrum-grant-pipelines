"""Guards on the test harness itself.

`config.settings` resolves DATA_DIR / STATE_DIR / SEEDS_DIR once, at import
time, and every module that imports one of them binds its own name to the same
Path object. Patching `config.settings` alone therefore redirects nothing. The
autouse `layout` fixture patches each binding by hand, and this module walks the
source tree to prove the list is complete — otherwise a new `from
config.settings import DATA_DIR` would silently start writing into the
developer's real data/ directory during a test run.
"""

from __future__ import annotations

import ast
import importlib
from datetime import datetime, timezone
from pathlib import Path

import pytest

from config import settings
from lib import dune, runs, seeds, state
from tests.conftest import _LAYOUT_BINDINGS

REPO_ROOT = Path(__file__).resolve().parent.parent
PATH_CONSTANTS = {"DATA_DIR", "STATE_DIR", "SEEDS_DIR"}
PACKAGES = ("lib", "pipelines", "ingestion", "scripts", "sql")


def _module_name(path: Path) -> str:
    return ".".join(path.relative_to(REPO_ROOT).with_suffix("").parts)


def _imports_of(path: Path) -> set[str]:
    tree = ast.parse(path.read_text())
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "config.settings":
            found |= {alias.name for alias in node.names} & PATH_CONSTANTS
    return found


def _source_bindings() -> set[tuple[str, str]]:
    bindings = set()
    for package in PACKAGES:
        for path in sorted((REPO_ROOT / package).rglob("*.py")):
            for constant in _imports_of(path):
                bindings.add((_module_name(path), constant))
    return bindings


def test_every_module_that_binds_a_path_constant_is_redirected():
    covered = {(module.__name__, attribute) for module, attribute, _ in _LAYOUT_BINDINGS}

    missing = _source_bindings() - covered
    assert not missing, (
        "these modules import a path constant but the `layout` fixture does not "
        f"patch them, so tests would touch the real filesystem: {sorted(missing)}"
    )


def test_the_binding_list_names_real_attributes():
    for module, attribute, _ in _LAYOUT_BINDINGS:
        assert hasattr(module, attribute), f"{module.__name__}.{attribute} no longer exists"


@pytest.mark.parametrize(
    "module, attribute, key",
    [(m, a, k) for m, a, k in _LAYOUT_BINDINGS],
    ids=[f"{m.__name__}.{a}" for m, a, _ in _LAYOUT_BINDINGS],
)
def test_the_layout_fixture_actually_redirects_each_binding(module, attribute, key, layout):
    assert getattr(module, attribute) == getattr(layout, key)
    assert layout.root in getattr(module, attribute).parents


def test_writes_land_in_the_tmp_layout_not_the_repo(layout):
    runs.RunWriter("smoke_test").finish()
    state.set_watermark("smoke_test", datetime(2026, 1, 1, tzinfo=timezone.utc))

    assert (layout.data / "smoke_test").is_dir()
    assert (layout.state / "smoke_test.json").is_file()
    assert not (REPO_ROOT / "data" / "smoke_test").exists()
    assert not (REPO_ROOT / "state" / "smoke_test.json").exists()


def test_the_settings_module_still_exposes_what_the_pipelines_import():
    # A rename here breaks every pipeline at import time; the suite should say
    # so in one line rather than in thirty collection errors.
    for name in (
        "DATA_DIR", "STATE_DIR", "SEEDS_DIR", "CHAIN_ARBITRUM", "CHAIN_ROBINHOOD",
        "BACKFILL_START", "INCREMENTAL_OVERLAP_DAYS", "MIN_BUY_USD",
        "EVANGELIST_MIN_VOLUME_USD", "ATTRIBUTION_WINDOW_DAYS",
        "DEFAULT_MIN_USER_SCORE", "USER_SCORE_PROPERTY", "ENGAGEMENT_WEIGHTS",
        "ARBITRUM_CHANNEL_ID", "NEYNAR_FID_BATCH", "NEYNAR_ADDRESS_BATCH",
        "FID_SCAN_CEILING", "FID_SCAN_EMPTY_BATCH_STOP", "NEO4J_BATCH_SIZE",
        "PROVENANCE", "DUNE_UPLOAD_ENABLED",
    ):
        assert hasattr(settings, name), f"config.settings.{name} is gone"


def test_the_dune_cache_lives_under_the_redirected_data_dir(layout):
    assert dune.DATA_DIR == layout.data
    assert seeds.SEEDS_DIR == layout.seeds


def test_every_pipeline_and_ingest_module_imports_cleanly():
    """Collection-time smoke test: nothing in the repo raises at import."""
    for package in ("pipelines", "ingestion", "sql"):
        for path in sorted((REPO_ROOT / package).glob("*.py")):
            if path.name == "__init__.py":
                continue
            importlib.import_module(_module_name(path))
