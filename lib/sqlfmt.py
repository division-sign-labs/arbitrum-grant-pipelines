"""Validated SQL literal builders.

Every value spliced into a Dune query goes through one of these. They are not a
defence against a hostile caller so much as a guarantee that a malformed seed
file or a stray None fails loudly at render time instead of producing a query
that silently matches nothing.
"""

import re
from datetime import datetime, timezone
from typing import Iterable, Sequence

# The 0x prefix is matched case-insensitively: hex from block explorers and
# checksummed sources sometimes arrives as 0X, and rejecting it would be a
# gratuitous failure on a value we can read perfectly well.
_HEX_ADDRESS = re.compile(r"^0[xX][0-9a-fA-F]{40}$")
_HEX_HASH = re.compile(r"^0[xX][0-9a-fA-F]{2,128}$")


class SqlLiteralError(ValueError):
    """Raised when a value cannot be rendered as a safe SQL literal."""


def address(value: str) -> str:
    """A checksum-insensitive 20-byte address as a DuneSQL varbinary literal."""
    if not isinstance(value, str) or not _HEX_ADDRESS.match(value.strip()):
        raise SqlLiteralError(f"not a 20-byte hex address: {value!r}")
    return value.strip().lower()


def address_list(values: Iterable[str]) -> str:
    """`0xaaa..., 0xbbb...` for `WHERE col IN (...)` against varbinary columns."""
    rendered = [address(v) for v in values]
    if not rendered:
        raise SqlLiteralError("empty address list")
    return ", ".join(rendered)


def address_str_list(values: Iterable[str]) -> str:
    """`'0xaaa...', '0xbbb...'` for the text address columns in neynar datasets."""
    rendered = [f"'{address(v)}'" for v in values]
    if not rendered:
        raise SqlLiteralError("empty address list")
    return ", ".join(rendered)


def hash_literal(value: str) -> str:
    if not isinstance(value, str) or not _HEX_HASH.match(value.strip()):
        raise SqlLiteralError(f"not a hex hash: {value!r}")
    return value.strip().lower()


def hash_str_list(values: Iterable[str]) -> str:
    rendered = [f"'{hash_literal(v)}'" for v in values]
    if not rendered:
        raise SqlLiteralError("empty hash list")
    return ", ".join(rendered)


def int_list(values: Iterable) -> str:
    rendered = []
    for v in values:
        try:
            rendered.append(str(int(v)))
        except (TypeError, ValueError) as exc:
            raise SqlLiteralError(f"not an integer: {v!r}") from exc
    if not rendered:
        raise SqlLiteralError("empty integer list")
    return ", ".join(rendered)


def timestamp(value) -> str:
    """`timestamp '2025-01-01 00:00:00'` from a datetime or ISO string."""
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        raise SqlLiteralError(f"not a datetime: {value!r}")
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return f"timestamp '{value.strftime('%Y-%m-%d %H:%M:%S')}'"


def text(value: str) -> str:
    """A single-quoted string literal with quotes doubled."""
    if not isinstance(value, str):
        raise SqlLiteralError(f"not a string: {value!r}")
    return "'" + value.replace("'", "''") + "'"


def like_pattern(value: str) -> str:
    """A LIKE pattern literal with SQL wildcards in the input neutralised.

    Ticker matching interpolates user-controlled symbols; a `%` inside one would
    otherwise turn `%$abc%` into a match-everything scan.
    """
    if not isinstance(value, str):
        raise SqlLiteralError(f"not a string: {value!r}")
    return value.replace("\\", "\\\\").replace("%", r"\%").replace("_", r"\_")


def chunked(values: Sequence, size: int = 1000):
    """Yield fixed-size slices so huge IN-lists become several queries."""
    values = list(values)
    for i in range(0, len(values), size):
        yield values[i : i + size]
