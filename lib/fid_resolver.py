"""fid <-> wallet resolution against Neynar, plus readers for the resulting run.

This is the join key for the whole project. Every other pipeline starts from an
on-chain address and needs the Farcaster account behind it, or starts from an
fid and needs the addresses to look for on Arbitrum. Dune carries none of this
(the `dune.neynar.dataset_farcaster_*` tables do not exist with our key), so the
Neynar API is the only source and this module is the only place that shape is
parsed.

Two directions:
  resolve_fids      fid   -> user objects, via GET /v2/farcaster/user/bulk
  resolve_addresses address -> user objects, via GET /v2/farcaster/user/bulk-by-address

Both endpoints cap at 100 identifiers per call. The address endpoint is a GET
and 400 addresses overflows the URI length limit (verified: HTTP 414), which is
why the batch size is a hard ceiling rather than a tuning knob.

Verified Neynar behaviours this module exists to absorb:
  * /user/bulk returns HTTP 404 — not an empty list — when *no* fid in the batch
    exists. A batch with even one live fid returns 200 and silently drops the
    misses. Past the fid tip every batch 404s, so a caller that does not treat
    404 as "empty" dies the moment the scan walks off the end.
  * /user/bulk-by-address returns a dict keyed by the *lowercased* address, with
    a list of users per address; addresses with no match are absent entirely.
    One address can map to several fids (an address shared across accounts).

Address casing: eth addresses are lowercased everywhere, because that is the
Wallet node's MERGE key. Solana addresses are base58 and case-sensitive, so they
are passed through untouched — lowercasing one would corrupt it. Dedupe still
compares case-insensitively so a single wallet cannot appear twice.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Iterable, Sequence

import pandas as pd
import requests

from config.settings import NEYNAR_ADDRESS_BATCH, NEYNAR_FID_BATCH
from lib.runs import read_csv
from lib.sqlfmt import chunked

logger = logging.getLogger(__name__)

DATA_TYPE = "linked_wallets"

WALLET_COLUMNS = ["fid", "address", "protocol", "is_primary", "source"]
ACCOUNT_COLUMNS = [
    "fid",
    "username",
    "display_name",
    "neynar_score",
    "follower_count",
    "following_count",
    "custody_address",
    "registered_at",
]

# The fid path lives in NeynarClient.bulk_users; only the address endpoint is
# called directly from here.
USER_BY_ADDRESS_PATH = "/v2/farcaster/user/bulk-by-address"


def _norm_address(value, protocol: str = "eth") -> str | None:
    """Canonical form for storage: eth lowercased, sol left alone (base58)."""
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none"}:
        return None
    return text.lower() if protocol == "eth" else text


def _status_of(exc: requests.HTTPError) -> int | None:
    response = getattr(exc, "response", None)
    return None if response is None else response.status_code


# -- fid -> user ----------------------------------------------------------


def fetch_users(client, fids: Sequence[int]) -> list[dict]:
    """One /user/bulk call for <=100 fids, treating a whole-batch miss as empty.

    Neynar 404s when not one fid in the batch resolves. That is the normal state
    past the fid tip, so it is data, not an error.
    """
    fids = [int(f) for f in fids]
    if not fids:
        return []
    try:
        return client.bulk_users(fids)
    except requests.HTTPError as exc:
        if _status_of(exc) == 404:
            logger.debug("fids %d..%d: none exist (404)", fids[0], fids[-1])
            return []
        raise


def flatten_user(user: dict) -> tuple[list[dict], dict]:
    """One Neynar user object -> its wallets.csv rows and its accounts.csv row.

    Emits every verified eth/sol address plus the custody address, which is a
    real wallet the account controls and is often the one that actually deployed
    a contract. Verified rows are emitted first so that when the custody address
    is also verified, the dedupe keeps the richer 'verified' row.
    """
    if not isinstance(user, dict) or user.get("fid") is None:
        return [], {}
    fid = int(user["fid"])

    verified = user.get("verified_addresses") or {}
    primary = verified.get("primary") or {}
    primary_eth = _norm_address(primary.get("eth_address"), "eth")
    primary_sol = _norm_address(primary.get("sol_address"), "sol")

    rows: list[dict] = []
    seen: set[tuple[str, str]] = set()

    def add(raw, protocol: str, source: str, is_primary: bool) -> None:
        address = _norm_address(raw, protocol)
        if not address:
            return
        key = (address.lower(), protocol)
        if key in seen:
            return
        seen.add(key)
        rows.append(
            {
                "fid": fid,
                "address": address,
                "protocol": protocol,
                "is_primary": bool(is_primary),
                "source": source,
            }
        )

    for raw in verified.get("eth_addresses") or []:
        address = _norm_address(raw, "eth")
        add(raw, "eth", "verified", address is not None and address == primary_eth)
    for raw in verified.get("sol_addresses") or []:
        address = _norm_address(raw, "sol")
        add(raw, "sol", "verified", address is not None and address == primary_sol)
    add(user.get("custody_address"), "eth", "custody", False)

    experimental = user.get("experimental") or {}
    score = user.get("score")
    if score is None:
        score = experimental.get("neynar_user_score")

    account = {
        "fid": fid,
        "username": user.get("username"),
        "display_name": user.get("display_name"),
        "neynar_score": score,
        "follower_count": user.get("follower_count"),
        "following_count": user.get("following_count"),
        "custody_address": _norm_address(user.get("custody_address"), "eth"),
        "registered_at": user.get("registered_at"),
    }
    return rows, account


def resolve_fids(
    client,
    fids: Iterable[int],
    batch: int = NEYNAR_FID_BATCH,
    on_batch: Callable[[list[int], list[dict], list[dict], list[dict]], None] | None = None,
    collect: bool = True,
) -> tuple[list[dict], list[dict]]:
    """Resolve fids to (wallet_rows, account_rows).

    `on_batch(fids, users, wallet_rows, account_rows)` fires once per API call so
    a long scan can flush to disk as it goes. Pass `collect=False` alongside it to
    stop accumulating in memory — the full-tip scan is ~3.3M accounts, which is
    far too many dicts to hold.
    """
    size = max(1, min(int(batch), NEYNAR_FID_BATCH))
    all_wallets: list[dict] = []
    all_accounts: list[dict] = []

    for chunk in chunked([int(f) for f in fids], size):
        users = fetch_users(client, chunk)
        wallet_rows: list[dict] = []
        account_rows: list[dict] = []
        for user in users:
            rows, account = flatten_user(user)
            wallet_rows.extend(rows)
            if account:
                account_rows.append(account)
        if on_batch is not None:
            on_batch(chunk, users, wallet_rows, account_rows)
        if collect:
            all_wallets.extend(wallet_rows)
            all_accounts.extend(account_rows)

    return all_wallets, all_accounts


def rows_from_users(users: Iterable[dict]) -> tuple[list[dict], list[dict]]:
    """Split a batch of Neynar user objects into wallet rows and account rows."""
    wallet_rows: list[dict] = []
    account_rows: list[dict] = []
    for user in users:
        rows, account = flatten_user(user)
        wallet_rows.extend(rows)
        if account:
            account_rows.append(account)
    return wallet_rows, account_rows


def resolve_fid_wave(
    client,
    chunks: Sequence[Sequence[int]],
    workers: int = 6,
) -> list[tuple[list[int], list[dict], list[dict]]]:
    """Resolve several fid batches concurrently, returned in submission order.

    A full-tip scan is ~33.5k sequential calls, and the wall clock is dominated
    by round-trip latency rather than the rate limit: issued one at a time, a
    ~700ms round trip yields ~1.4 calls/s against a budget of 5. Overlapping a
    handful of requests saturates the budget instead of the socket and turns a
    six-hour crawl into roughly two.

    The rate limiter in `HttpClient` is lock-guarded and enforces spacing across
    all threads, so concurrency raises throughput without raising the request
    rate past what Neynar allows. Order is preserved so the caller can still
    checkpoint a monotonic scan cursor.
    """
    chunks = [list(chunk) for chunk in chunks]
    if not chunks:
        return []
    if len(chunks) == 1 or workers <= 1:
        chunk = chunks[0]
        wallet_rows, account_rows = rows_from_users(fetch_users(client, chunk))
        return [(chunk, wallet_rows, account_rows)]

    with ThreadPoolExecutor(max_workers=min(workers, len(chunks))) as pool:
        futures = [(chunk, pool.submit(fetch_users, client, chunk)) for chunk in chunks]
        results = []
        for chunk, future in futures:
            wallet_rows, account_rows = rows_from_users(future.result())
            results.append((chunk, wallet_rows, account_rows))
    return results


# -- address -> user ------------------------------------------------------


def resolve_addresses(
    client,
    addresses: Iterable[str],
    batch: int = NEYNAR_ADDRESS_BATCH,
) -> dict[str, list[dict]]:
    """Reverse lookup: {lowercased address: [user, ...]} for addresses that match.

    Unmatched addresses are simply absent from the result — callers should use
    `.get(addr, [])`. The batch size is clamped to NEYNAR_ADDRESS_BATCH because
    this endpoint is a GET and larger batches return HTTP 414.
    """
    size = max(1, min(int(batch), NEYNAR_ADDRESS_BATCH))

    unique: list[str] = []
    seen: set[str] = set()
    for raw in addresses:
        address = _norm_address(raw, "eth")
        if not address or address in seen:
            continue
        seen.add(address)
        unique.append(address)

    found: dict[str, list[dict]] = {}
    for chunk in chunked(unique, size):
        try:
            payload = client.http.get_json(
                USER_BY_ADDRESS_PATH, params={"addresses": ",".join(chunk)}
            )
        except requests.HTTPError as exc:
            # Same contract as /user/bulk: no match anywhere in the batch is a 404.
            if _status_of(exc) == 404:
                continue
            raise
        if not isinstance(payload, dict):
            logger.warning("unexpected bulk-by-address payload type: %s", type(payload))
            continue
        for address, users in payload.items():
            if isinstance(users, list) and users:
                found.setdefault(str(address).lower(), []).extend(users)

    logger.info(
        "resolved %d/%d addresses to Farcaster accounts", len(found), len(unique)
    )
    return found


def addresses_to_fids(
    client, addresses: Iterable[str], batch: int = NEYNAR_ADDRESS_BATCH
) -> dict[str, int]:
    """resolve_addresses collapsed to one fid per address (lowest fid wins)."""
    resolved = resolve_addresses(client, addresses, batch=batch)
    out: dict[str, int] = {}
    for address, users in resolved.items():
        fids = [int(u["fid"]) for u in users if isinstance(u, dict) and u.get("fid")]
        if fids:
            out[address] = min(fids)
    return out


# -- readers for the completed run ---------------------------------------


def load_wallet_map(run_id: str | None = None, protocol: str | None = None) -> pd.DataFrame:
    """The latest (or named) linked_wallets run's wallets.csv, typed and cleaned.

    This is what other pipelines join their on-chain addresses against locally —
    Dune only allows PUBLIC uploads on this account, so the wallet set never
    leaves the machine.
    """
    df = read_csv(DATA_TYPE, "wallets", run_id=run_id)
    if df.empty:
        return pd.DataFrame(columns=WALLET_COLUMNS)
    df = df[pd.to_numeric(df["fid"], errors="coerce").notna()].copy()
    df["fid"] = df["fid"].astype(int)
    df["address"] = df["address"].astype(str).str.strip()
    df["protocol"] = df["protocol"].astype(str).str.lower()
    # Only eth addresses are case-normalised; base58 sol addresses must not be.
    eth = df["protocol"] == "eth"
    df.loc[eth, "address"] = df.loc[eth, "address"].str.lower()
    df["is_primary"] = df["is_primary"].astype(str).str.lower().isin({"true", "1"})
    if protocol:
        df = df[df["protocol"] == protocol.lower()]
    return df.reset_index(drop=True)


def wallet_to_fid(run_id: str | None = None) -> dict[str, int]:
    """{lowercased eth address: fid} for local joins against chain data.

    An address can carry more than one fid (shared or transferred wallets), and a
    dict cannot. Ties break deterministically: a verified link beats a custody
    link, a primary address beats a secondary, and the lowest fid beats the rest,
    so the same run always produces the same map.
    """
    df = load_wallet_map(run_id=run_id, protocol="eth")
    if df.empty:
        return {}
    ranked = df.assign(
        _custody=(df["source"].astype(str).str.lower() != "verified").astype(int),
        _secondary=(~df["is_primary"]).astype(int),
    ).sort_values(["address", "_custody", "_secondary", "fid"])
    deduped = ranked.drop_duplicates(subset=["address"], keep="first")
    return dict(zip(deduped["address"], deduped["fid"].astype(int)))
