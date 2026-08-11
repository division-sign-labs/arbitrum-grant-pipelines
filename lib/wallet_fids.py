"""Local-first wallet -> fid lookup for pipelines that start from chain data.

`lib.fid_resolver` owns the Neynar contract and the `linked_wallets` run; this
is the consumer side of it, and it exists because two launchpad pipelines need
the same three behaviours and would otherwise drift apart:

  * the local `linked_wallets` map is consulted first, because it costs nothing
    and already covers every wallet this repo has crawled;
  * Neynar's reverse lookup is asked only about what is left, 100 addresses per
    call, and only up to `MAX_NEYNAR_ADDRESSES` of them;
  * every failure mode — no `linked_wallets` run yet, `lib.fid_resolver` absent,
    Neynar down — degrades to a smaller mapping and a note, never to an
    exception. A registry pipeline that dies because an identity lookup failed
    has thrown away the chain data it just paid for.

The cap matters at Bankr's scale: 67k tokens carry ~15k distinct deployers and a
similar number of fee recipients, and resolving every one of them on every run
would spend hundreds of Neynar calls re-asking about addresses that have already
been answered "no". The uncapped path is `linked_wallets`, whose full crawl
lands in the local map; this is only the top-up for what that crawl has not
reached yet.
"""

from __future__ import annotations

import logging
from typing import Iterable

import pandas as pd

logger = logging.getLogger(__name__)

# ~200 calls at NEYNAR_ADDRESS_BATCH. Chosen to keep a single pipeline run's
# identity top-up inside a couple of minutes at NEYNAR_REQUESTS_PER_SECOND.
MAX_NEYNAR_ADDRESSES = 20_000


def distinct_addresses(*columns: pd.Series | None) -> list[str]:
    """The sorted, lowercased union of several address columns.

    Only 0x-prefixed values survive: these columns also carry pandas NaN and, on
    a Dune round-trip that missed the NULL sentinel, the string "<nil>".
    """
    seen: set[str] = set()
    for column in columns:
        if column is None:
            continue
        for value in column.dropna().astype(str):
            text = value.strip().lower()
            if text.startswith("0x"):
                seen.add(text)
    return sorted(seen)


def resolve_wallet_fids(
    addresses: Iterable[str],
    notes: list[str],
    *,
    what: str = "wallets",
    max_neynar: int | None = None,
) -> dict[str, int]:
    """{lowercased address: fid} for as many of `addresses` as can be resolved.

    `notes` is appended to in place with anything the run's manifest should
    record — a skipped top-up, a missing local map, a failed lookup.
    """
    addresses = [a for a in addresses if a]
    if not addresses:
        return {}

    try:
        from lib.fid_resolver import addresses_to_fids, wallet_to_fid
    except ImportError as exc:
        logger.warning(
            "lib.fid_resolver not available (%s); leaving fid null for %d %s",
            exc,
            len(addresses),
            what,
        )
        notes.append(f"lib.fid_resolver missing at run time: {what} fids were not resolved")
        return {}

    wanted = set(addresses)
    mapping: dict[str, int] = {}
    try:
        local = wallet_to_fid()
        mapping.update({k: v for k, v in local.items() if k in wanted})
        logger.info("local linked_wallets map covered %d %s", len(mapping), what)
    except Exception as exc:  # noqa: BLE001 - no linked_wallets run yet is fine
        logger.info("no local wallet map available (%s); falling back to Neynar", exc)

    missing = [a for a in addresses if a not in mapping]
    cap = MAX_NEYNAR_ADDRESSES if max_neynar is None else max(0, int(max_neynar))
    asked, skipped = missing[:cap], len(missing) - min(len(missing), cap)
    if skipped:
        logger.warning(
            "capping the Neynar top-up at %d of %d unresolved %s", cap, len(missing), what
        )
        notes.append(
            f"Neynar top-up capped at {cap} addresses: {skipped} unresolved {what} "
            f"were left null this run and will resolve once linked_wallets covers them"
        )

    if asked:
        try:
            from lib.neynar import NeynarClient

            mapping.update(addresses_to_fids(NeynarClient(), asked))
        except Exception as exc:  # noqa: BLE001 - a lookup failure must not kill the run
            logger.warning(
                "Neynar address lookup failed (%s); keeping the local matches only", exc
            )
            notes.append(f"neynar fid lookup failed: {str(exc)[:200]}")

    logger.info("resolved %d/%d %s to fids", len(mapping), len(addresses), what)
    return mapping
