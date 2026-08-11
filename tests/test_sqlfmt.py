"""lib.sqlfmt — every literal that reaches Dune goes through these.

Each builder gets both halves: the rendering it must produce (Dune's varbinary
columns take *bare* 0x literals, its text columns take quoted ones — mixing them
up produces a query that runs, costs money and matches nothing) and the input it
must refuse.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from lib import sqlfmt
from lib.sqlfmt import SqlLiteralError

ARB = "0x912CE59144191C1204E64559FE8253a0e49E6548"
WETH = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"


def test_address_lowercases_and_trims():
    assert sqlfmt.address(ARB) == ARB.lower()
    assert sqlfmt.address(f"  {WETH}  ") == WETH


def test_address_accepts_an_uppercase_prefix_from_a_block_explorer():
    # 0X arrives from explorers and checksummed sources; rejecting it would be a
    # gratuitous failure on a value we can read perfectly well.
    assert sqlfmt.address(ARB.replace("0x", "0X")) == ARB.lower()


@pytest.mark.parametrize(
    "bad",
    [
        "912ce59144191c1204e64559fe8253a0e49e6548",  # no 0x
        "0x912ce59144191c1204e64559fe8253a0e49e65",  # 19 bytes
        "0x912ce59144191c1204e64559fe8253a0e49e6548aa",  # 21 bytes
        "0xzzce59144191c1204e64559fe8253a0e49e6548",  # not hex
        "0x912ce59144191c1204e64559fe8253a0e49e6548'; DROP--",
        None,
        42,
        "",
    ],
)
def test_address_rejects_anything_that_is_not_a_20_byte_hex_address(bad):
    with pytest.raises(SqlLiteralError):
        sqlfmt.address(bad)


def test_address_list_renders_bare_literals_for_varbinary_in_clauses():
    assert sqlfmt.address_list([ARB, WETH]) == f"{ARB.lower()}, {WETH}"


def test_address_str_list_renders_quoted_literals_for_text_columns():
    assert sqlfmt.address_str_list([ARB]) == f"'{ARB.lower()}'"


@pytest.mark.parametrize("builder", [sqlfmt.address_list, sqlfmt.address_str_list])
def test_address_lists_reject_empty_and_propagate_a_bad_member(builder):
    with pytest.raises(SqlLiteralError, match="empty address list"):
        builder([])
    with pytest.raises(SqlLiteralError, match="not a 20-byte hex address"):
        builder([ARB, "nope"])


def test_hash_literal_accepts_short_and_full_length_hex():
    tx = "0x" + "ab" * 32
    assert sqlfmt.hash_literal(tx.upper()) == tx  # 0X prefix and all
    assert sqlfmt.hash_literal("0xABCD") == "0xabcd"
    assert sqlfmt.hash_literal(f"  {tx}  ") == tx


@pytest.mark.parametrize(
    "bad",
    [
        "0x",
        "0x1",  # a single nibble is not a hash
        "0xg1",
        "abcd",
        None,
        "0x" + "a" * 129,
    ],
)
def test_hash_literal_rejects_non_hashes(bad):
    with pytest.raises(SqlLiteralError):
        sqlfmt.hash_literal(bad)


def test_hash_str_list_quotes_and_rejects_empty():
    assert sqlfmt.hash_str_list(["0xAB", "0xcd"]) == "'0xab', '0xcd'"
    with pytest.raises(SqlLiteralError, match="empty hash list"):
        sqlfmt.hash_str_list([])


def test_int_list_coerces_numeric_shapes():
    assert sqlfmt.int_list([1, "2", 3.0, True]) == "1, 2, 3, 1"


@pytest.mark.parametrize("bad", [["1", "two"], [None], [{}]])
def test_int_list_rejects_non_integers(bad):
    with pytest.raises(SqlLiteralError, match="not an integer"):
        sqlfmt.int_list(bad)


def test_int_list_rejects_empty():
    with pytest.raises(SqlLiteralError, match="empty integer list"):
        sqlfmt.int_list([])


def test_timestamp_renders_naive_utc_from_a_string_or_datetime():
    expected = "timestamp '2026-01-02 03:04:05'"
    assert sqlfmt.timestamp("2026-01-02T03:04:05Z") == expected
    assert sqlfmt.timestamp(datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)) == expected
    # Naive input is taken as already-UTC and passed through unshifted.
    assert sqlfmt.timestamp(datetime(2026, 1, 2, 3, 4, 5)) == expected


def test_timestamp_converts_a_non_utc_offset_before_dropping_the_zone():
    aware = datetime(2026, 1, 2, 5, 4, 5, tzinfo=timezone(timedelta(hours=2)))
    assert sqlfmt.timestamp(aware) == "timestamp '2026-01-02 03:04:05'"


@pytest.mark.parametrize("bad", [None, 1735689600, ["2026-01-01"]])
def test_timestamp_rejects_non_datetimes(bad):
    with pytest.raises(SqlLiteralError, match="not a datetime"):
        sqlfmt.timestamp(bad)


def test_timestamp_propagates_an_unparseable_string():
    with pytest.raises(ValueError):
        sqlfmt.timestamp("last tuesday")


def test_text_doubles_embedded_quotes():
    assert sqlfmt.text("plain") == "'plain'"
    assert sqlfmt.text("O'Brien") == "'O''Brien'"
    assert sqlfmt.text("'; DROP TABLE t --") == "'''; DROP TABLE t --'"


@pytest.mark.parametrize("bad", [None, 7, b"bytes"])
def test_text_rejects_non_strings(bad):
    with pytest.raises(SqlLiteralError, match="not a string"):
        sqlfmt.text(bad)


def test_like_pattern_neutralises_sql_wildcards():
    # A ticker containing % would otherwise turn `%$x%` into a full-table scan.
    assert sqlfmt.like_pattern("100%") == r"100\%"
    assert sqlfmt.like_pattern("a_b") == r"a\_b"
    assert sqlfmt.like_pattern("%_%") == r"\%\_\%"
    assert sqlfmt.like_pattern("ARB") == "ARB"


def test_like_pattern_escapes_the_escape_character_first():
    # Backslash must be doubled *before* % and _ gain their own backslashes,
    # otherwise the escape of an escape swallows the next wildcard.
    assert sqlfmt.like_pattern("a\\b") == "a\\\\b"
    assert sqlfmt.like_pattern("\\%") == r"\\\%"


@pytest.mark.parametrize("bad", [None, 5])
def test_like_pattern_rejects_non_strings(bad):
    with pytest.raises(SqlLiteralError, match="not a string"):
        sqlfmt.like_pattern(bad)


def test_chunked_boundaries():
    assert list(sqlfmt.chunked([], 3)) == []
    assert list(sqlfmt.chunked([1, 2, 3], 3)) == [[1, 2, 3]]
    assert list(sqlfmt.chunked([1, 2, 3, 4], 3)) == [[1, 2, 3], [4]]
    assert list(sqlfmt.chunked(range(5), 2)) == [[0, 1], [2, 3], [4]]
    assert list(sqlfmt.chunked([1], 10)) == [[1]]


def test_chunked_consumes_any_iterable_and_never_drops_a_member():
    values = (str(i) for i in range(7))
    chunks = list(sqlfmt.chunked(list(values), 3))

    assert [len(c) for c in chunks] == [3, 3, 1]
    assert [v for chunk in chunks for v in chunk] == [str(i) for i in range(7)]
