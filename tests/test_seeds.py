"""lib.seeds — the two inputs the pipelines cannot derive for themselves.

A missing or malformed seed must fail with instructions, never with an empty
result that looks like a real (and very disappointing) answer.
"""

from __future__ import annotations

import pytest

from lib import seeds
from lib.seeds import SeedMissingError, load_brand_accounts, load_miniapp_builders


def write_seed(layout, name: str, text: str):
    (layout.seeds / f"{name}.csv").write_text(text)


# --- missing / malformed -------------------------------------------------


@pytest.mark.parametrize(
    "loader, name",
    [(load_miniapp_builders, "miniapp_builders"), (load_brand_accounts, "brand_accounts")],
)
def test_a_missing_seed_names_every_path_it_searched(loader, name, layout):
    with pytest.raises(SeedMissingError) as excinfo:
        loader()

    message = str(excinfo.value)
    assert f"Seed file '{name}.csv' not found" in message
    for candidate in seeds._candidate_paths(name):
        assert str(candidate) in message
    assert "Expected schema:" in message


def test_seed_missing_error_is_a_file_not_found_error(layout):
    with pytest.raises(FileNotFoundError):
        load_miniapp_builders()


def test_a_seed_without_the_fid_column_names_the_column_and_the_schema(layout):
    write_seed(layout, "brand_accounts", "handle,name\narbitrum,Arbitrum\n")

    with pytest.raises(ValueError, match=r"missing column\(s\) \['fid'\]") as excinfo:
        load_brand_accounts()

    assert "fid,name[,weight]" in str(excinfo.value)


# --- brand accounts ------------------------------------------------------


def test_brand_accounts_defaults_the_weight_to_one(layout):
    write_seed(layout, "brand_accounts", "fid,name\n1,Arbitrum\n2,Offchain Labs\n")

    frame = load_brand_accounts()

    assert list(frame["weight"]) == [1.0, 1.0]
    assert list(frame["name"]) == ["Arbitrum", "Offchain Labs"]


def test_brand_accounts_keeps_an_explicit_weight_and_fills_a_blank_one(layout):
    write_seed(layout, "brand_accounts", "fid,name,weight\n1,Arbitrum,3\n2,Alt,\n3,Bad,junk\n")

    frame = load_brand_accounts().set_index("fid")

    assert frame.loc[1, "weight"] == 3.0
    assert frame.loc[2, "weight"] == 1.0
    assert frame.loc[3, "weight"] == 1.0


def test_brand_accounts_supplies_a_name_column_when_the_seed_omits_it(layout):
    write_seed(layout, "brand_accounts", "fid\n1\n")

    frame = load_brand_accounts()

    assert "name" in frame.columns
    assert frame.loc[0, "name"] is None


# --- shared loader behaviour --------------------------------------------


def test_column_names_are_stripped_and_lowercased(layout):
    write_seed(layout, "miniapp_builders", " FID , Username \n7,alice\n")

    frame = load_miniapp_builders()

    assert list(frame.columns) == ["fid", "username"]
    assert frame.loc[0, "fid"] == 7


def test_rows_without_a_numeric_fid_are_dropped_and_the_column_is_int(layout):
    write_seed(layout, "miniapp_builders", "fid,username\n7,alice\n,bob\nnotanumber,carol\n8,dave\n")

    frame = load_miniapp_builders()

    assert list(frame["fid"]) == [7, 8]
    assert frame["fid"].dtype.kind == "i"


def test_duplicate_fids_collapse_to_the_first_sighting(layout):
    write_seed(layout, "miniapp_builders", "fid,username\n7,alice\n7,alice-again\n")

    frame = load_miniapp_builders()

    assert len(frame) == 1
    assert frame.iloc[0]["username"] == "alice"


def test_a_header_only_seed_loads_as_empty_rather_than_raising(layout):
    # The seeds ship as header-only templates so the repo is runnable; run_all's
    # preflight is what warns about them, not this loader.
    write_seed(layout, "miniapp_builders", "fid,username\n")

    assert load_miniapp_builders().empty


def test_the_data_dir_fallback_is_searched_when_seeds_dir_is_empty(layout):
    fallback = layout.data / "brand_accounts"
    fallback.mkdir()
    (fallback / "brand_accounts.csv").write_text("fid,name\n99,Arbitrum\n")

    frame = load_brand_accounts()

    assert list(frame["fid"]) == [99]


def test_seeds_dir_wins_over_the_data_dir_fallback(layout):
    fallback = layout.data / "brand_accounts"
    fallback.mkdir()
    (fallback / "brand_accounts.csv").write_text("fid,name\n99,Fallback\n")
    write_seed(layout, "brand_accounts", "fid,name\n1,Primary\n")

    assert list(load_brand_accounts()["name"]) == ["Primary"]
