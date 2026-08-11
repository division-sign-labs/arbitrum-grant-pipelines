"""ingestion — the CSV contract, the type coercion, and the Cypher/row handshake.

The headline test is `test_every_row_key_the_cypher_reads_is_produced`: every
`row.<key>` and `$<param>` a module's Cypher mentions is checked against what
the loader (read_rows + the step's transform) actually hands the driver. A
column renamed in a pipeline would otherwise write nulls into the graph and no
one would notice until a query came back empty.

The coercion tests exist because the failure mode there is silent too: pandas
turns an empty cell into float('nan'), the neo4j driver stores it as a Float
NaN, and every later comparison against that property is false.
"""

from __future__ import annotations

import dataclasses
import importlib
import json
import re
from datetime import datetime, timezone

import pytest

from ingestion import base
from ingestion.base import IngestError, Step, ingest_run, load_run, read_rows, unique_rows
from lib.cli import ingestion_parser

UTC = timezone.utc

# Every ingest module and the data type it reads. ingest_tokens serves two, and
# picks between them with --source, so it is listed once per source.
INGEST_MODULES = [
    ("ingestion.ingest_linked_wallets", "linked_wallets", None),
    ("ingestion.ingest_contract_deployers", "contract_deployers", None),
    ("ingestion.ingest_miniapp_builders", "miniapp_builders_activity", None),
    ("ingestion.ingest_brand_engagement", "brand_engagement", None),
    ("ingestion.ingest_tokens", "clanker_tokens", "clanker"),
    ("ingestion.ingest_tokens", "bankr_tokens", "bankr"),
    ("ingestion.ingest_token_buyers", "token_buyers", None),
    ("ingestion.ingest_popular_tokens", "popular_tokens", None),
    ("ingestion.ingest_token_evangelists", "token_evangelists", None),
    ("ingestion.ingest_hyperliquid", "hyperliquid_activity", None),
]

# Parameters ingestion.base supplies to every write; anything else must come
# from the step's own `params`.
BASE_PARAMS = {"rows", "asOf", "ingestedBy", "source"}

# `row.<key>` lookups that are deliberately absent from the CSV for some
# sources. Cypher reads a missing map key as null, which is what makes one
# shared query tail serve both launchpads. Anything NOT listed here that the
# loader fails to produce is a bug, not a design choice.
KNOWN_OPTIONAL_ROW_KEYS = {
    ("ingestion.ingest_tokens", "bankr", "tokens -> Token/DEPLOYED/CREATED"): {"username"},
}

ROW_KEY = re.compile(r"\brow\.([A-Za-z_][A-Za-z0-9_]*)")
PARAM = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")


def steps_for(module_name: str, source: str | None):
    module = importlib.import_module(module_name)
    if source is not None:
        return module.STEPS_BY_SOURCE[source]
    return module.STEPS


def sample_value(column: str, index: int):
    """A plausible CSV cell for a column, typed the way ingestion.base reads it."""
    kind = base._column_kind(column, None)
    if kind == "int":
        return {"chain_id": 42161, "fid": 1000 + index}.get(column, index + 1)
    if kind == "float":
        return round(1.5 + index, 4)
    if kind == "bool":
        # Alternate so the true/false split in ingest_hyperliquid gets both.
        return "true" if index % 2 == 0 else "false"
    if kind == "timestamp":
        return f"2026-0{index + 1}-01T00:00:00Z"
    if kind == "address":
        return "0x" + f"{index + 1:040x}"
    if kind == "hash":
        return "0x" + f"{index + 1:064x}"
    if column.endswith("_raw"):
        return str(10**20 + index)
    return f"{column}-{index}"


def write_csv(run_dir, name: str, columns, rows: int = 2):
    lines = [",".join(columns)]
    for index in range(rows):
        lines.append(",".join(str(sample_value(column, index)) for column in columns))
    (run_dir / f"{name}.csv").write_text("\n".join(lines) + "\n")


def build_run(layout, data_type: str, steps, run_ts: str = "20260101T000000Z"):
    """A complete run directory holding every CSV the given steps declare."""
    run_dir = layout.data / data_type / run_ts
    run_dir.mkdir(parents=True)
    files = {}
    for step in steps:
        if step.csv in files:
            continue
        write_csv(run_dir, step.csv, step.columns)
        files[step.csv] = 2
    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "data_type": data_type,
                "run_ts": run_ts,
                "generated_at": "2026-01-01T00:00:00+00:00",
                "generated_by": "arbitrum-grant-pipelines",
                "files": files,
                "row_total": sum(files.values()),
                "params": {},
                "since": None,
                "new_watermark": None,
                "notes": [],
            }
        )
    )
    return run_dir


# --- the Cypher / row handshake -----------------------------------------


@pytest.mark.parametrize("module_name, data_type, source", INGEST_MODULES)
def test_every_row_key_the_cypher_reads_is_produced(module_name, data_type, source, layout):
    steps = steps_for(module_name, source)
    run_dir = build_run(layout, data_type, steps)

    for step in steps:
        rows = read_rows(run_dir, step.csv, step.columns, types=step.types)
        if step.transform is not None:
            rows = step.transform(rows)
        assert rows, f"{module_name}/{step.label} produced no rows from a full CSV"

        produced = set().union(*(set(row) for row in rows))
        referenced = set(ROW_KEY.findall(step.cypher))
        expected_gap = KNOWN_OPTIONAL_ROW_KEYS.get((module_name, source, step.label), set())

        assert referenced - produced == expected_gap, (
            f"{module_name}/{step.label}: Cypher reads row keys the loader does not "
            f"produce: {sorted(referenced - produced - expected_gap)}"
        )


@pytest.mark.parametrize("module_name, data_type, source", INGEST_MODULES)
def test_every_cypher_parameter_is_supplied(module_name, data_type, source):
    for step in steps_for(module_name, source):
        referenced = set(PARAM.findall(step.cypher))
        available = BASE_PARAMS | set(step.params)

        assert referenced <= available, (
            f"{module_name}/{step.label}: Cypher uses undeclared parameter(s) "
            f"{sorted(referenced - available)}"
        )
        assert "$rows" in step.cypher and "UNWIND $rows AS row" in step.cypher


@pytest.mark.parametrize("module_name, data_type, source", INGEST_MODULES)
def test_every_step_declares_the_columns_its_transform_keys_on(module_name, data_type, source, layout):
    """A transform that dedupes on a column the CSV lacks would silently collapse
    every row into one, so run the real transform over a real CSV and check the
    row count survives."""
    steps = steps_for(module_name, source)
    run_dir = build_run(layout, data_type, steps)

    for step in steps:
        rows = read_rows(run_dir, step.csv, step.columns, types=step.types)
        assert len(rows) == 2
        if step.transform is not None:
            transformed = step.transform(rows)
            assert 1 <= len(transformed) <= 2, f"{module_name}/{step.label} collapsed its rows"


@pytest.mark.parametrize("module_name, data_type, source", INGEST_MODULES)
def test_dry_run_ingest_reads_a_real_run_and_touches_no_database(
    module_name, data_type, source, layout, capsys
):
    steps = steps_for(module_name, source)
    build_run(layout, data_type, steps)
    args = ingestion_parser("x").parse_args(["--dry-run"])
    args.no_constraints = True

    # Neo4jUtils would raise on a missing NEO4J_URI; reaching 0 proves the whole
    # validate-then-write path ran without ever constructing one.
    assert ingest_run(data_type, steps, args) == 0

    printed = capsys.readouterr().out
    assert "nothing was written" in printed
    for step in steps:
        assert step.label in printed


def test_the_ingest_module_list_matches_the_orchestrator_plan():
    """run_all names an ingestion module per pipeline; they must all exist."""
    from scripts.run_all import PLAN, resolve_ingest_module

    for step in PLAN:
        resolved = resolve_ingest_module(step.ingest)
        if step.ingest_required:
            assert resolved is not None, f"no ingestion module for {step.pipeline}"


# --- type coercion -------------------------------------------------------


@pytest.mark.parametrize(
    "value, kind, expected",
    [
        ("42", "int", 42),
        ("42.0", "int", 42),  # pandas floated the column somewhere upstream
        ("1.5", "float", 1.5),
        ("true", "bool", True),
        ("FALSE", "bool", False),
        ("1", "bool", True),
        ("no", "bool", False),
        ("0xABCDEF", "address", "0xabcdef"),
        ("9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin", "address", "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin"),
        ("0xAB", "hash", "0xab"),
        ("  spaced  ", "str", "spaced"),
        ("2026-01-02 03:04:05.000 UTC", "timestamp", datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)),
    ],
)
def test_coerce_types_by_column_kind(value, kind, expected):
    assert base._coerce(value, kind) == expected


@pytest.mark.parametrize("kind", ["int", "float", "bool", "timestamp", "address", "hash", "str"])
@pytest.mark.parametrize("value", [None, float("nan"), "", "   "])
def test_missing_values_become_none_for_every_kind(kind, value):
    assert base._coerce(value, kind) is None


@pytest.mark.parametrize("sentinel", ["nan", "NaN", "None", "null", "NaT", "<NA>"])
def test_sentinels_are_null_in_typed_columns_but_literal_in_text(sentinel):
    assert base._coerce(sentinel, "int") is None
    assert base._coerce(sentinel, "float") is None
    assert base._coerce(sentinel, "timestamp") is None
    # "NULL" is a plausible memecoin ticker; nulling it would be data loss.
    assert base._coerce(sentinel, "str") == sentinel


def test_coerce_raises_on_a_non_boolean_so_the_caller_can_count_it():
    with pytest.raises(ValueError, match="not a boolean"):
        base._coerce("maybe", "bool")


def test_column_kinds_follow_the_csv_contract():
    assert base._column_kind("fid", None) == "int"
    assert base._column_kind("amount_usd", None) == "float"
    assert base._column_kind("is_primary", None) == "bool"
    assert base._column_kind("block_time", None) == "timestamp"
    assert base._column_kind("buyer_address", None) == "address"
    assert base._column_kind("cast_hash", None) == "hash"
    assert base._column_kind("username", None) == "str"
    # *_raw columns stay text: a uint256 balance overflows Neo4j's 64-bit int.
    assert base._column_kind("balance_raw", None) == "str"
    assert base._column_kind("fid", {"fid": "str"}) == "str"


# --- read_rows -----------------------------------------------------------


def test_read_rows_turns_empty_cells_into_none_not_nan(layout):
    run_dir = layout.data / "linked_wallets" / "20260101T000000Z"
    run_dir.mkdir(parents=True)
    (run_dir / "accounts.csv").write_text(
        "fid,username,neynar_score,follower_count,registered_at\n"
        "194,rish,0.93,291045,2021-05-01T00:00:00Z\n"
        "195,,,,\n"
    )

    rows = read_rows(run_dir, "accounts", ["fid", "username", "neynar_score"])

    assert rows[0] == {
        "fid": 194,
        "username": "rish",
        "neynar_score": 0.93,
        "follower_count": 291045,
        "registered_at": datetime(2021, 5, 1, tzinfo=UTC),
    }
    assert rows[1] == {
        "fid": 195,
        "username": None,
        "neynar_score": None,
        "follower_count": None,
        "registered_at": None,
    }
    # Not NaN — a Float NaN in the graph poisons every comparison against it.
    assert all(value is None or value == value for row in rows for value in row.values())


def test_read_rows_keeps_int_columns_integral_even_when_one_cell_is_blank(layout):
    run_dir = layout.data / "contract_deployers" / "20260101T000000Z"
    run_dir.mkdir(parents=True)
    (run_dir / "deployer_activity.csv").write_text("fid,tx_count\n1,5\n,7\n")

    rows = read_rows(run_dir, "deployer_activity", ["fid", "tx_count"])

    assert [type(row["tx_count"]) for row in rows] == [int, int]
    assert rows[1]["fid"] is None


def test_read_rows_preserves_a_username_of_na(layout):
    run_dir = layout.data / "linked_wallets" / "20260101T000000Z"
    run_dir.mkdir(parents=True)
    (run_dir / "accounts.csv").write_text("fid,username\n1,NA\n2,N/A\n")

    rows = read_rows(run_dir, "accounts", ["fid", "username"])

    assert [row["username"] for row in rows] == ["NA", "N/A"]


def test_read_rows_lowercases_hex_addresses_but_not_solana(layout):
    run_dir = layout.data / "linked_wallets" / "20260101T000000Z"
    run_dir.mkdir(parents=True)
    solana = "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin"
    (run_dir / "wallets.csv").write_text(
        f"fid,address,protocol\n1,0xAABBCCDDEEFF00112233445566778899AABBCCDD,eth\n2,{solana},sol\n"
    )

    rows = read_rows(run_dir, "wallets", ["fid", "address"])

    assert rows[0]["address"] == "0xaabbccddeeff00112233445566778899aabbccdd"
    assert rows[1]["address"] == solana


def test_read_rows_keeps_uint256_raw_values_as_strings(layout):
    run_dir = layout.data / "popular_tokens" / "20260101T000000Z"
    run_dir.mkdir(parents=True)
    huge = "115792089237316195423570985008687907853269984665640564039457584007913129639935"
    (run_dir / "holdings.csv").write_text(f"address,balance,balance_raw\n0xa,1.5,{huge}\n")

    rows = read_rows(run_dir, "holdings", ["address", "balance_raw"])

    assert rows[0]["balance_raw"] == huge
    assert rows[0]["balance"] == 1.5


def test_read_rows_nulls_an_unreadable_value_instead_of_failing_the_run(layout, caplog):
    run_dir = layout.data / "token_buyers" / "20260101T000000Z"
    run_dir.mkdir(parents=True)
    (run_dir / "buys.csv").write_text("fid,amount_usd\n1,12.5\n2,not-a-number\n")

    rows = read_rows(run_dir, "buys", ["fid", "amount_usd"])

    assert [row["amount_usd"] for row in rows] == [12.5, None]


def test_read_rows_rejects_a_csv_that_breaches_the_column_contract(layout):
    run_dir = layout.data / "token_buyers" / "20260101T000000Z"
    run_dir.mkdir(parents=True)
    (run_dir / "buys.csv").write_text("fid,amount\n1,12.5\n")

    with pytest.raises(IngestError, match="missing required column\\(s\\): amount_usd"):
        read_rows(run_dir, "buys", ["fid", "amount_usd"])


def test_read_rows_missing_file_is_fatal_or_skippable(layout):
    run_dir = layout.data / "token_buyers" / "20260101T000000Z"
    run_dir.mkdir(parents=True)

    with pytest.raises(IngestError, match="is missing, but the run's manifest declares it"):
        read_rows(run_dir, "buys", ["fid"])

    assert read_rows(run_dir, "buys", ["fid"], required=False) == []


def test_read_rows_of_a_header_only_csv_is_empty(layout):
    run_dir = layout.data / "token_buyers" / "20260101T000000Z"
    run_dir.mkdir(parents=True)
    (run_dir / "buys.csv").write_text("fid,amount_usd\n")

    assert read_rows(run_dir, "buys", ["fid", "amount_usd"]) == []


# --- unique_rows ---------------------------------------------------------


def test_unique_rows_keeps_the_last_write_per_key():
    rows = [
        {"fid": 1, "address": "0xa", "n": 1},
        {"fid": 1, "address": "0xa", "n": 2},
        {"fid": 1, "address": "0xb", "n": 3},
    ]

    deduped = unique_rows(rows, ["fid", "address"])

    assert deduped == [{"fid": 1, "address": "0xa", "n": 2}, {"fid": 1, "address": "0xb", "n": 3}]


def test_unique_rows_treats_none_as_its_own_key():
    rows = [{"fid": None, "address": "0xa"}, {"fid": 1, "address": "0xa"}]

    assert len(unique_rows(rows, ["fid", "address"])) == 2


# --- run resolution ------------------------------------------------------


def test_load_run_explains_a_missing_run(layout):
    with pytest.raises(IngestError, match="no completed runs for 'token_buyers'"):
        load_run("token_buyers")


def test_load_run_explains_a_missing_run_id(layout):
    with pytest.raises(IngestError, match="no such run"):
        load_run("token_buyers", "20991231T000000Z")


def test_load_run_returns_the_manifest(layout):
    steps = steps_for("ingestion.ingest_token_buyers", None)
    run_dir = build_run(layout, "token_buyers", steps)

    resolved, manifest = load_run("token_buyers")

    assert resolved == run_dir
    assert manifest["files"] == {"buys": 2}


def test_a_csv_the_manifest_never_declared_is_skipped_not_failed(layout, capsys):
    """A degraded pipeline writes no file and says so in the manifest; ingestion
    must treat that as a supported outcome, and only a *declared* file going
    missing as corruption."""
    steps = steps_for("ingestion.ingest_linked_wallets", None)
    run_dir = build_run(layout, "linked_wallets", steps)
    manifest = json.loads((run_dir / "manifest.json").read_text())
    del manifest["files"]["wallets"]
    (run_dir / "manifest.json").write_text(json.dumps(manifest))
    (run_dir / "wallets.csv").unlink()

    args = ingestion_parser("x").parse_args(["--dry-run"])
    args.no_constraints = True
    assert ingest_run("linked_wallets", steps, args) == 0

    printed = capsys.readouterr().out
    assert "wallets -> ACCOUNT  (wallets.csv, 0 rows)" in printed


def test_a_declared_csv_that_vanished_is_an_error(layout):
    steps = steps_for("ingestion.ingest_linked_wallets", None)
    run_dir = build_run(layout, "linked_wallets", steps)
    (run_dir / "wallets.csv").unlink()  # manifest still declares it

    args = ingestion_parser("x").parse_args(["--dry-run"])
    args.no_constraints = True
    with pytest.raises(IngestError, match="wallets.csv is missing"):
        ingest_run("linked_wallets", steps, args)


def test_ingest_main_reports_a_bad_run_with_the_reserved_exit_code(layout):
    from ingestion.ingest_token_buyers import main

    # No run at all: a retry cannot fix it, so run_all needs it distinguishable
    # from a crash.
    assert main(["--dry-run"]) == base.EXIT_BAD_RUN


# --- provenance ----------------------------------------------------------


def test_run_unwind_stamps_provenance_on_every_write():
    class Recorder:
        def __init__(self):
            self.params = None

        def run_unwind(self, query, rows, batch_size=None, params=None, label=""):
            self.params = params
            return {"nodes_created": 0, "relationships_created": 0, "properties_set": 0}

    recorder = Recorder()
    base.run_unwind(
        recorder,
        "UNWIND $rows AS row RETURN row",
        [{"fid": 1}],
        label="x",
        as_of=datetime(2026, 1, 1, tzinfo=UTC),
        source="linked_wallets",
        params={"platform": "clanker"},
    )

    assert recorder.params == {
        "asOf": datetime(2026, 1, 1, tzinfo=UTC),
        "ingestedBy": "arbitrum-grant-pipelines",
        "source": "linked_wallets",
        "platform": "clanker",
    }


def test_optional_account_link_is_guarded_on_a_null_fid():
    cypher = base.optional_account_link("buyer_fid")

    assert "row.buyer_fid IS NULL" in cypher
    assert "MERGE (linked:WarpcastAccount {fid: row.buyer_fid})" in cypher
    # linked_wallets owns the ACCOUNT edge's properties; every other module may
    # only fill them in on creation.
    assert "ON CREATE SET link.protocol" in cypher


def test_step_is_frozen_so_a_module_cannot_mutate_the_shared_plan():
    step = Step(label="l", csv="c", columns=["a"], cypher="UNWIND $rows AS row RETURN row")

    # STEPS is module-level state shared across every ingest in a process.
    with pytest.raises(dataclasses.FrozenInstanceError):
        step.csv = "other"
