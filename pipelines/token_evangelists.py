"""Token evangelism: which Farcaster accounts actually moved money into a token.

Produces, per qualifying token:
  token_casts.csv        casts that talk about the token
  attributions.csv       one row per (buy, influencing author)
  evangelist_summary.csv one row per (token, author) with the rolled-up credit

The claim being made is causal-ish and deliberately conservative: a buy counts
for an author only if the buyer demonstrably saw that author's cast about the
token — they reacted to it, or wrote it — within ATTRIBUTION_WINDOW_DAYS before
the purchase. Each purchase's USD is split equally among the distinct authors
who qualify, so ten people shilling the same token cannot each claim the whole
buy. Split-equally is a choice, not a measurement: we have no way to rank whose
cast mattered more, and equal shares at least conserve the total.

Why the sources are what they are
---------------------------------
The reference implementation did all of this in one Dune query against
`dune.neynar.dataset_farcaster_casts` and `_reactions`. Those tables do not
exist on our key — verified, all six neynar datasets fail — and there is no
Farcaster social data on Dune at all. So the pipeline is split:

  casts + reactions  -> Neynar REST (the only source that has them)
  buys               -> Dune `dex.trades` (decoded and USD-priced across DEXes)
  the join           -> pandas, locally

The semantics of the reference query are preserved exactly; only the execution
engine moved. In particular a cast counts if its text contains `$SYMBOL` or the
contract address, or if its parent_url/root_parent_url is the token's
`eip155:<chainId>/erc20:<address>` frame url.

Cost
----
Neynar search and reaction calls are the expensive part and they scale with the
number of tokens, so qualification is strict (lifetime DEX volume >=
EVANGELIST_MIN_VOLUME_USD) and `--max-tokens` defaults low. `--dry-run` prints
the full plan — tokens, search calls, reaction calls — and spends nothing.
"""

from __future__ import annotations

import json
import logging
from typing import Iterable, Iterator, Sequence

import pandas as pd

from config.settings import (
    ATTRIBUTION_WINDOW_DAYS,
    BACKFILL_START,
    CHAIN_ARBITRUM,
    EVANGELIST_MIN_VOLUME_USD,
)
from lib import sqlfmt
from lib.cli import base_parser, resolve_window
from lib.dune import DuneError, DuneRunner
from lib.logging_utils import setup_logging
from lib.neynar import NeynarClient, search_response_to_casts
from lib.runs import RunWriter, read_csv
from lib.state import set_watermark
from sql.evangelism import (
    chain_name,
    token_buys_for_attribution_multi_sql,
    token_buys_for_attribution_sql,
    token_parent_url,
)
from sql.evangelism import token_volume_totals_sql as local_volume_totals_sql

logger = logging.getLogger(__name__)

PIPELINE = "token_evangelists"

TOKEN_CAST_COLUMNS = [
    "token_address",
    "chain_id",
    "cast_hash",
    "author_fid",
    "timestamp",
    "matched_on",
    "likes_count",
    "recasts_count",
]
ATTRIBUTION_COLUMNS = [
    "token_address",
    "chain_id",
    "author_fid",
    "buyer_fid",
    "buyer_address",
    "tx_hash",
    "amount_usd",
    "block_time",
    "attributed_usd",
    "n_influencers",
]
SUMMARY_COLUMNS = [
    "token_address",
    "chain_id",
    "author_fid",
    "cast_count",
    "unique_buyers_influenced",
    "total_purchases",
    "total_purchase_volume_usd",
    "attributed_usd",
]

# Registries this pipeline draws its candidate tokens from.
TOKEN_REGISTRIES = ("clanker_tokens", "bankr_tokens")

DEFAULT_MAX_TOKENS = 25
DEFAULT_SEARCH_PAGES = 5  # 100 casts/page, newest first
DEFAULT_REACTION_CASTS = 200  # per token, highest-engagement casts first
DEFAULT_REACTION_PAGES = 2  # 100 reactors/page

# A one- or two-character ticker matches half of Farcaster. Below this length a
# cast only counts if it names the contract address outright.
MIN_TICKER_LENGTH = 3

SEARCH_PATH = "/v2/farcaster/cast/search"
SEARCH_PAGE_SIZE = 100  # verified: limit=150 -> 400 ExceededMaxLimit


# --- timestamps ----------------------------------------------------------


def _to_utc(series: pd.Series) -> pd.Series:
    """Parse a column of mixed Dune/Neynar timestamps into aware UTC.

    Dune renders `2026-08-09 19:56:12.000 UTC`; Neynar renders
    `2026-08-09T19:56:12.000Z`. Normalising the Dune suffix first lets one
    ISO8601 parse handle both instead of two passes with different formats.
    """
    if series.empty:
        return pd.to_datetime(series, utc=True, errors="coerce")
    normalised = series.astype(str).str.replace(" UTC", "+00:00", regex=False)
    return pd.to_datetime(normalised, utc=True, errors="coerce", format="ISO8601")


# --- token registry ------------------------------------------------------


def load_token_registry() -> tuple[pd.DataFrame, list[str]]:
    """Candidate tokens from the clanker and bankr registry runs.

    Both registries publish token_address/chain_id/platform/symbol/fid, which is
    everything the search needs. A missing registry is not fatal — it just means
    that launchpad contributes no candidates this run.
    """
    notes: list[str] = []
    frames: list[pd.DataFrame] = []
    for data_type in TOKEN_REGISTRIES:
        try:
            df = read_csv(data_type, "tokens", required=False)
        except FileNotFoundError:
            notes.append(f"no completed {data_type} run; contributed no candidates")
            logger.warning("no completed %s run — skipping its tokens", data_type)
            continue
        if df.empty:
            notes.append(f"{data_type} registry is empty")
            continue
        keep = df.reindex(
            columns=["token_address", "chain_id", "platform", "symbol", "fid"]
        ).copy()
        keep["source_registry"] = data_type
        frames.append(keep)
        logger.info("%s: %d candidate tokens", data_type, len(keep))

    if not frames:
        return pd.DataFrame(
            columns=["token_address", "chain_id", "platform", "symbol", "fid", "source_registry"]
        ), notes

    registry = pd.concat(frames, ignore_index=True)
    registry = registry[registry["token_address"].notna()].copy()
    registry["token_address"] = registry["token_address"].astype(str).str.strip().str.lower()
    registry["chain_id"] = (
        pd.to_numeric(registry["chain_id"], errors="coerce")
        .fillna(CHAIN_ARBITRUM)
        .astype(int)
    )
    registry["symbol"] = registry["symbol"].fillna("").astype(str).str.strip()
    registry = registry.drop_duplicates(subset=["token_address", "chain_id"], keep="first")
    return registry.reset_index(drop=True), notes


# --- volume qualification ------------------------------------------------


def _local_volume_builder():
    """The in-repo volume builder, chunked so a large registry stays one query per 500 tokens."""
    return lambda addrs, chain, since: [
        local_volume_totals_sql(chunk, chain, since)
        for chunk in sqlfmt.chunked(list(addrs), 500)
    ]


def _as_query_list(rendered) -> list[str] | None:
    """Normalise a builder's output to a list of SQL strings, or None if it is not SQL.

    sql.trades returns one query per address chunk; the local builder returns a
    single string. Both are valid, so callers work in lists either way.
    """
    queries = [rendered] if isinstance(rendered, str) else list(rendered or [])
    if not queries or not all(isinstance(q, str) for q in queries):
        return None
    return queries


def resolve_volume_sql_builder():
    """Prefer sql.trades' volume builder, fall back to the local equivalent.

    sql/trades.py belongs to the trades pipeline: it may not exist yet, and its
    signature and return type are not ours to pin down (it currently returns a
    *list* of chunked queries, not one string). Rather than couple the two
    modules, try a short ladder of call shapes and check that what comes back is
    SQL that actually filters on the address we asked about. Anything
    unrecognised degrades to the copy in sql/evangelism.py, which produces the
    same two columns this pipeline reads.
    """
    try:
        from sql.trades import token_volume_totals_sql as shared  # type: ignore
    except Exception as exc:  # ImportError, or the module raising at import time
        logger.info("sql.trades unavailable (%s); using the local volume query", exc)
        return _local_volume_builder(), "sql.evangelism"

    shapes = (
        lambda addrs, chain, since: shared(addrs, chain, since),
        lambda addrs, chain, since: shared(addrs, chain=chain, since=since),
        lambda addrs, chain, since: shared(addrs, chain_id=chain, since=since),
        lambda addrs, chain, since: shared(token_addresses=addrs, chain=chain, since=since),
    )
    probe_addr = "0x912ce59144191c1204e64559fe8253a0e49e6548"
    for shape in shapes:
        try:
            queries = _as_query_list(shape([probe_addr], CHAIN_ARBITRUM, BACKFILL_START))
        except TypeError:
            continue
        except Exception as exc:
            logger.warning("sql.trades volume builder raised (%s); using local copy", exc)
            break
        if queries and all(
            "select" in q.lower() and probe_addr[2:] in q.lower() for q in queries
        ):
            logger.info("using sql.trades.token_volume_totals_sql for qualification")
            return (lambda addrs, chain, since: _as_query_list(shape(addrs, chain, since)) or []), "sql.trades"
    logger.info(
        "sql.trades.token_volume_totals_sql did not match a known call shape; using local copy"
    )
    return _local_volume_builder(), "sql.evangelism"


def token_volumes(
    runner: DuneRunner, registry: pd.DataFrame, notes: list[str]
) -> pd.DataFrame:
    """Lifetime DEX volume for every candidate token, one query per chain.

    Deliberately ignores --limit. Wrapping this in a row cap would not shrink
    the scan (the cost is the same) and would silently qualify an arbitrary
    handful of tokens, which is worse than useless. --max-tokens is the knob
    that bounds the expensive half.
    """
    builder, origin = resolve_volume_sql_builder()
    frames: list[pd.DataFrame] = []
    for chain_id, group in registry.groupby("chain_id"):
        addresses = sorted(set(group["token_address"]))
        try:
            name = chain_name(chain_id)
        except sqlfmt.SqlLiteralError as exc:
            logger.warning("skipping chain %s: %s", chain_id, exc)
            notes.append(f"chain {chain_id} has no Dune blockchain name; volume not computed")
            continue
        try:
            queries = builder(addresses, chain_id, BACKFILL_START)
        except sqlfmt.SqlLiteralError as exc:
            logger.warning("could not render volume SQL for chain %s: %s", chain_id, exc)
            notes.append(f"volume SQL unrenderable for chain {chain_id}: {exc}")
            continue
        for sql in queries:
            try:
                df = runner.run_sql(sql, label=f"evangelism token volume {name}")
            except DuneError as exc:
                # dex.trades may simply not cover a chain (robinhood is new).
                logger.warning("volume query failed for %s: %s", name, exc)
                notes.append(f"volume query failed for chain {name}: {exc}")
                continue
            if df.empty:
                continue
            df = df.copy()
            df["chain_id"] = int(chain_id)
            frames.append(df)
    logger.info("volume SQL sourced from %s", origin)
    if not frames:
        return pd.DataFrame(columns=["token_address", "chain_id", "volume_usd"])
    volumes = pd.concat(frames, ignore_index=True)
    volumes["token_address"] = volumes["token_address"].astype(str).str.lower()
    volumes["volume_usd"] = pd.to_numeric(volumes["volume_usd"], errors="coerce").fillna(0.0)
    return volumes


def explicit_token_frame(addresses: Iterable[str], registry: pd.DataFrame, chain_id: int) -> pd.DataFrame:
    """Build the token frame for `--tokens`, enriching from the registry if possible."""
    by_key: dict[tuple[str, int], dict] = {}
    by_address: dict[str, dict] = {}
    if not registry.empty:
        for record in registry.to_dict("records"):
            key = (record["token_address"], int(record["chain_id"]))
            by_key[key] = record
            by_address.setdefault(record["token_address"], record)

    rows = []
    for raw in addresses:
        address = sqlfmt.address(raw)
        # An address the operator names is trusted even if no registry has it;
        # the registry only supplies the symbol, which enables ticker search.
        match = by_key.get((address, chain_id)) or by_address.get(address) or {}
        rows.append(
            {
                "token_address": address,
                "chain_id": int(match.get("chain_id") or chain_id),
                "platform": match.get("platform"),
                "symbol": str(match.get("symbol") or ""),
                "fid": match.get("fid"),
                "volume_usd": float("nan"),
            }
        )
    return pd.DataFrame(rows)


def select_tokens(
    runner: DuneRunner, registry: pd.DataFrame, args, notes: list[str]
) -> pd.DataFrame:
    """The tokens this run will actually spend Neynar calls on."""
    if args.tokens:
        addresses = [a.strip() for a in args.tokens.split(",") if a.strip()]
        tokens = explicit_token_frame(addresses, registry, args.chain_id)
        logger.info("--tokens given: %d explicit tokens, volume gate skipped", len(tokens))
        notes.append("token set supplied via --tokens; the volume gate was not applied")
        return tokens.head(args.max_tokens)

    if registry.empty:
        return pd.DataFrame(columns=["token_address", "chain_id", "platform", "symbol", "fid", "volume_usd"])

    if args.dry_run:
        # Qualification needs a real Dune execution, and --dry-run must not
        # spend. Fall back to the registry order so the plan still shows a
        # concrete, correctly-sized token set to reason about.
        logger.info(
            "[dry-run] volume gate not executed; planning against the first %d registry tokens",
            args.max_tokens,
        )
        notes.append("dry-run: token set is the registry head, not the volume-qualified set")
        planned = registry.head(args.max_tokens).copy()
        planned["volume_usd"] = float("nan")
        return planned

    volumes = token_volumes(runner, registry, notes)
    if volumes.empty:
        logger.warning("no DEX volume found for any candidate token")
        return pd.DataFrame(columns=["token_address", "chain_id", "platform", "symbol", "fid", "volume_usd"])

    merged = registry.merge(
        volumes[["token_address", "chain_id", "volume_usd"]],
        on=["token_address", "chain_id"],
        how="inner",
    )
    qualifying = merged[merged["volume_usd"] >= args.min_volume].copy()
    qualifying = qualifying.sort_values("volume_usd", ascending=False)
    logger.info(
        "%d/%d candidate tokens cleared $%.0f lifetime volume",
        len(qualifying),
        len(registry),
        args.min_volume,
    )
    if len(qualifying) > args.max_tokens:
        logger.info(
            "capping at --max-tokens=%d (dropping %d lower-volume tokens)",
            args.max_tokens,
            len(qualifying) - args.max_tokens,
        )
        notes.append(
            f"{len(qualifying) - args.max_tokens} qualifying tokens dropped by --max-tokens"
        )
    return qualifying.head(args.max_tokens).reset_index(drop=True)


# --- Neynar: casts -------------------------------------------------------


def iter_search_casts(
    client: NeynarClient, query: str, max_pages: int | None
) -> Iterator[dict]:
    """Paginate cast search, newest first.

    NeynarClient.search_casts routes through the generic paginator, which reads
    `casts` and `next` from the top level of the payload. Cast search — unlike
    every feed endpoint — nests both under `result` (verified live: the only
    top-level key is `result`), so the generic paginator finds no casts and no
    cursor and stops after one page. lib.neynar ships
    `search_response_to_casts` for exactly this shape; this reads the cursor
    from the same place.

    `sort_type=desc_chron` is explicit because the caller stops paginating at
    the first cast older than the window, which is only sound if the order is
    guaranteed. `mode=literal` keeps the match a substring match rather than a
    semantic one, which is what the reference LIKE did.
    """
    params = {
        "q": query,
        "limit": SEARCH_PAGE_SIZE,
        "mode": "literal",
        "sort_type": "desc_chron",
    }
    cursor: str | None = None
    pages = 0
    while True:
        call = dict(params)
        if cursor:
            call["cursor"] = cursor
        payload = client.http.get_json(SEARCH_PATH, params=call)
        casts = search_response_to_casts(payload)
        for cast in casts:
            yield cast
        pages += 1
        if not casts:
            return
        if max_pages is not None and pages >= max_pages:
            return
        result = payload.get("result") or payload
        cursor = (result.get("next") or {}).get("cursor")
        if not cursor:
            return


def classify_cast(cast: dict, address: str, symbol: str, chain_id: int, allow_ticker: bool) -> str | None:
    """How this cast refers to the token, or None if it does not.

    Ordered most-specific-first: naming the contract or being posted onto the
    token's own frame is unambiguous, a ticker is not.
    """
    text = (cast.get("text") or "").lower()
    if address in text:
        return "address"
    frame_url = token_parent_url(address, chain_id)
    for key in ("parent_url", "root_parent_url"):
        value = cast.get(key)
        if value and str(value).lower() == frame_url:
            return "parent_url"
    if allow_ticker and symbol and f"${symbol}".lower() in text:
        return "ticker"
    return None


def harvest_addresses(user: dict) -> list[str]:
    """Every wallet a Neynar user object exposes, lowercase.

    These come free with the casts and reactions we already paid for, so the
    buyer->fid map gets them whether or not a linked_wallets run exists.
    """
    out: list[str] = []
    verified = user.get("verified_addresses") or {}
    out.extend(verified.get("eth_addresses") or [])
    primary = verified.get("primary") or {}
    if primary.get("eth_address"):
        out.append(primary["eth_address"])
    if user.get("custody_address"):
        out.append(user["custody_address"])
    return [a.lower() for a in out if isinstance(a, str) and a.startswith("0x")]


def collect_token_casts(
    client: NeynarClient,
    token: dict,
    since,
    max_pages: int,
    address_fids: dict[str, int],
) -> list[dict]:
    """Casts about one token, deduped across the ticker and address searches."""
    address = token["token_address"]
    symbol = str(token.get("symbol") or "").strip()
    chain_id = int(token["chain_id"])
    allow_ticker = len(symbol) >= MIN_TICKER_LENGTH
    if symbol and not allow_ticker:
        logger.info(
            "token %s symbol %r is under %d chars; address matches only",
            address,
            symbol,
            MIN_TICKER_LENGTH,
        )

    queries = [address]
    if allow_ticker:
        queries.append(f"${symbol}")

    found: dict[str, dict] = {}
    for query in queries:
        seen_page_rows = 0
        for cast in iter_search_casts(client, query, max_pages):
            seen_page_rows += 1
            timestamp = cast.get("timestamp")
            parsed = pd.Timestamp(timestamp) if timestamp else None
            if parsed is None or pd.isna(parsed):
                continue
            parsed = parsed.tz_convert("UTC") if parsed.tzinfo else parsed.tz_localize("UTC")
            if parsed < since:
                # desc_chron: everything past here is older than the window.
                break
            matched = classify_cast(cast, address, symbol, chain_id, allow_ticker)
            if matched is None:
                continue
            cast_hash = cast.get("hash")
            author = cast.get("author") or {}
            author_fid = author.get("fid")
            if not cast_hash or author_fid is None:
                continue
            if cast_hash in found:
                continue
            reactions = cast.get("reactions") or {}
            found[cast_hash] = {
                "token_address": address,
                "chain_id": chain_id,
                "cast_hash": cast_hash,
                "author_fid": int(author_fid),
                "timestamp": parsed.isoformat(),
                "matched_on": matched,
                "likes_count": int(reactions.get("likes_count") or 0),
                "recasts_count": int(reactions.get("recasts_count") or 0),
            }
            for wallet in harvest_addresses(author):
                address_fids.setdefault(wallet, int(author_fid))
        logger.debug("search %r on %s: %d rows scanned", query, address, seen_page_rows)

    logger.info(
        "token %s (%s): %d matching casts", address, symbol or "?", len(found)
    )
    return list(found.values())


# --- Neynar: reactions ---------------------------------------------------


def collect_engagements(
    client: NeynarClient,
    casts: Sequence[dict],
    max_casts: int,
    max_pages: int,
    reaction_types: str,
    address_fids: dict[str, int],
) -> list[dict]:
    """Who engaged with these casts, and when.

    Two kinds of engagement, matching the reference: reacting to a cast, and
    having written it. Authoring counts because an author who then buys their
    own call is exactly the self-attribution case the reference kept.

    Casts with zero likes and zero recasts are skipped outright — the reactions
    endpoint would return an empty page, and at one call per cast that is the
    single biggest saving available here. The rest are visited
    highest-engagement first so a truncating `--max-reaction-casts` keeps the
    casts that mattered.
    """
    rows: list[dict] = []
    for cast in casts:
        rows.append(
            {
                "token_address": cast["token_address"],
                "chain_id": cast["chain_id"],
                "cast_hash": cast["cast_hash"],
                "author_fid": cast["author_fid"],
                "engager_fid": cast["author_fid"],
                "engaged_at": cast["timestamp"],
                "engagement": "authored",
            }
        )

    with_reactions = [c for c in casts if (c["likes_count"] + c["recasts_count"]) > 0]
    with_reactions.sort(key=lambda c: c["likes_count"] + c["recasts_count"], reverse=True)
    skipped = len(casts) - len(with_reactions)
    targets = with_reactions[:max_casts]
    if len(with_reactions) > max_casts:
        logger.info(
            "capping reaction fetches at %d of %d reacted-to casts", max_casts, len(with_reactions)
        )
    logger.info(
        "fetching reactions for %d casts (%d had none), up to %d pages each",
        len(targets),
        skipped,
        max_pages,
    )

    for cast in targets:
        for reaction in client.cast_reactions(
            cast["cast_hash"], types=reaction_types, max_pages=max_pages
        ):
            user = reaction.get("user") or {}
            fid = user.get("fid")
            if fid is None:
                continue
            fid = int(fid)
            if fid == cast["author_fid"]:
                # Reference semantics: a reactor is only an engager if they are
                # not the author. Self-reactions carry no information.
                continue
            engaged_at = reaction.get("reaction_timestamp") or cast["timestamp"]
            rows.append(
                {
                    "token_address": cast["token_address"],
                    "chain_id": cast["chain_id"],
                    "cast_hash": cast["cast_hash"],
                    "author_fid": cast["author_fid"],
                    "engager_fid": fid,
                    "engaged_at": engaged_at,
                    "engagement": reaction.get("reaction_type") or "reaction",
                }
            )
            for wallet in harvest_addresses(user):
                address_fids.setdefault(wallet, fid)
    return rows


# --- Dune: buys ----------------------------------------------------------


def fetch_buys(
    runner: DuneRunner, tokens: pd.DataFrame, since, limit: int | None, notes: list[str]
) -> pd.DataFrame:
    """Every buy of every qualifying token, batched one query per chain."""
    frames: list[pd.DataFrame] = []
    for chain_id, group in tokens.groupby("chain_id"):
        addresses = sorted(set(group["token_address"]))
        try:
            name = chain_name(chain_id)
        except sqlfmt.SqlLiteralError as exc:
            logger.warning("skipping buys for chain %s: %s", chain_id, exc)
            notes.append(f"no Dune blockchain name for chain {chain_id}; buys not fetched")
            continue
        for chunk in sqlfmt.chunked(addresses, 200):
            sql = (
                token_buys_for_attribution_sql(chunk[0], chain_id, since)
                if len(chunk) == 1
                else token_buys_for_attribution_multi_sql(chunk, chain_id, since)
            )
            try:
                df = runner.run_sql(
                    sql, label=f"evangelism buys {name}", limit=limit
                )
            except DuneError as exc:
                # dex.trades does not necessarily cover every Orbit chain.
                logger.warning("buys query failed for %s: %s", name, exc)
                notes.append(f"buys query failed for chain {name}: {exc}")
                continue
            if df.empty:
                continue
            df = df.copy()
            df["chain_id"] = int(chain_id)
            frames.append(df)
    if not frames:
        return pd.DataFrame(
            columns=[
                "token_address",
                "chain_id",
                "buyer_address",
                "tx_from",
                "tx_hash",
                "block_time",
                "amount_usd",
                "token_amount",
            ]
        )
    buys = pd.concat(frames, ignore_index=True)
    for column in ("token_address", "buyer_address", "tx_from", "tx_hash"):
        buys[column] = buys[column].astype(str).str.lower()
    buys["amount_usd"] = pd.to_numeric(buys["amount_usd"], errors="coerce").fillna(0.0)
    buys["block_time"] = _to_utc(buys["block_time"])
    return buys[buys["block_time"].notna()].reset_index(drop=True)


# --- wallet -> fid -------------------------------------------------------


def load_linked_wallets(address_fids: dict[str, int], notes: list[str]) -> int:
    """Seed the address->fid map from the linked_wallets run, if there is one.

    That pipeline enumerates every fid's verified addresses, so it is far wider
    than what falls out of the casts we happened to search. It is optional:
    without it we still resolve every buyer who appears as an author or reactor
    in this run's own payloads.
    """
    try:
        wallets = read_csv("linked_wallets", "wallets", required=False)
    except FileNotFoundError:
        logger.warning(
            "no completed linked_wallets run; buyer fids come only from this run's "
            "cast and reaction payloads"
        )
        notes.append("linked_wallets run absent; buyer->fid map limited to harvested addresses")
        return 0
    if wallets.empty or "address" not in wallets.columns or "fid" not in wallets.columns:
        notes.append("linked_wallets run had no usable wallets.csv")
        return 0
    added = 0
    for address, fid in zip(wallets["address"], wallets["fid"]):
        if not isinstance(address, str):
            continue
        key = address.strip().lower()
        if not key.startswith("0x") or pd.isna(fid):
            continue
        # linked_wallets is authoritative, so it wins any conflict.
        address_fids[key] = int(fid)
        added += 1
    logger.info("linked_wallets: %d address->fid mappings", added)
    return added


def resolve_buyer_fids(buys: pd.DataFrame, address_fids: dict[str, int]) -> pd.DataFrame:
    """Attach a Farcaster fid to each buy, preferring the taker over tx_from."""
    if buys.empty:
        return buys.assign(buyer_fid=pd.Series(dtype="Int64"))
    resolved = buys.copy()
    taker_fid = resolved["buyer_address"].map(address_fids)
    sender_fid = resolved["tx_from"].map(address_fids)
    resolved["buyer_fid"] = taker_fid.fillna(sender_fid).astype("Int64")
    # When only tx_from resolved, that EOA is the wallet the graph should key on.
    fallback = taker_fid.isna() & sender_fid.notna()
    resolved.loc[fallback, "buyer_address"] = resolved.loc[fallback, "tx_from"]
    return resolved


# --- attribution ---------------------------------------------------------


def attribute(
    engagements: pd.DataFrame, buys: pd.DataFrame, window_days: int
) -> pd.DataFrame:
    """Split each purchase equally among the authors who earned it.

    A buy at time T is credited to every author whose cast the buyer engaged
    with in [T - window, T). The bound is strict on the left and inclusive on
    the right — engaged_at < block_time <= engaged_at + window — so an
    engagement that happens *after* the purchase never earns credit for it.
    """
    empty = pd.DataFrame(columns=ATTRIBUTION_COLUMNS)
    if engagements.empty or buys.empty:
        return empty
    attributable = buys[buys["buyer_fid"].notna()].copy()
    if attributable.empty:
        return empty
    # buyer_fid arrives as nullable Int64; the engagement side is plain int64.
    # Merging across the two dtypes matches nothing, so pin both to int64 now
    # that the nulls are gone.
    attributable["buyer_fid"] = attributable["buyer_fid"].astype("int64")

    keys = ["token_address", "chain_id", "author_fid", "engager_fid", "engaged_at"]
    eng = engagements.drop_duplicates(subset=keys).copy()
    eng["engager_fid"] = eng["engager_fid"].astype("int64")
    eng["author_fid"] = eng["author_fid"].astype("int64")

    merged = attributable.merge(
        eng[["token_address", "chain_id", "author_fid", "engager_fid", "engaged_at"]],
        left_on=["token_address", "chain_id", "buyer_fid"],
        right_on=["token_address", "chain_id", "engager_fid"],
        how="inner",
    )
    logger.info("attribution candidate pairs before the window filter: %d", len(merged))
    if merged.empty:
        return empty

    window = pd.Timedelta(days=window_days)
    in_window = (merged["engaged_at"] < merged["block_time"]) & (
        merged["block_time"] <= merged["engaged_at"] + window
    )
    merged = merged[in_window]
    if merged.empty:
        return empty

    # One row per (purchase, author): engaging with three casts by the same
    # author is still one author's worth of influence.
    pairs = merged.drop_duplicates(
        subset=["token_address", "chain_id", "tx_hash", "buyer_fid", "author_fid"]
    ).copy()
    group = ["token_address", "chain_id", "tx_hash", "buyer_fid"]
    pairs["n_influencers"] = pairs.groupby(group)["author_fid"].transform("nunique")
    pairs["attributed_usd"] = pairs["amount_usd"] / pairs["n_influencers"]
    return pairs[ATTRIBUTION_COLUMNS].reset_index(drop=True)


def summarise(casts: pd.DataFrame, attributions: pd.DataFrame) -> pd.DataFrame:
    """Roll attribution up to one row per (token, author).

    Authors who cast about a token but influenced nobody still appear, with
    zeroes: "posted and nothing happened" is a real, useful answer.
    """
    if casts.empty:
        return pd.DataFrame(columns=SUMMARY_COLUMNS)

    cast_counts = (
        casts.groupby(["token_address", "chain_id", "author_fid"], as_index=False)
        .agg(cast_count=("cast_hash", "nunique"))
    )

    if attributions.empty:
        summary = cast_counts.assign(
            unique_buyers_influenced=0,
            total_purchases=0,
            total_purchase_volume_usd=0.0,
            attributed_usd=0.0,
        )
        return summary[SUMMARY_COLUMNS].sort_values(
            "attributed_usd", ascending=False
        ).reset_index(drop=True)

    rolled = (
        attributions.groupby(["token_address", "chain_id", "author_fid"], as_index=False)
        .agg(
            unique_buyers_influenced=("buyer_fid", "nunique"),
            total_purchases=("tx_hash", "nunique"),
            total_purchase_volume_usd=("amount_usd", "sum"),
            attributed_usd=("attributed_usd", "sum"),
        )
    )
    summary = cast_counts.merge(
        rolled, on=["token_address", "chain_id", "author_fid"], how="outer"
    )
    summary["cast_count"] = summary["cast_count"].fillna(0).astype(int)
    for column in ("unique_buyers_influenced", "total_purchases"):
        summary[column] = summary[column].fillna(0).astype(int)
    for column in ("total_purchase_volume_usd", "attributed_usd"):
        summary[column] = summary[column].fillna(0.0)
    return (
        summary[SUMMARY_COLUMNS]
        .sort_values("attributed_usd", ascending=False)
        .reset_index(drop=True)
    )


# --- plan / cost ---------------------------------------------------------


def log_plan(tokens: pd.DataFrame, search_pages: int, reaction_casts: int, reaction_pages: int) -> dict:
    """Print what the run will cost before it starts spending."""
    queries = 0
    for token in tokens.to_dict("records"):
        symbol = str(token.get("symbol") or "").strip()
        queries += 2 if len(symbol) >= MIN_TICKER_LENGTH else 1
    max_search_calls = queries * search_pages
    max_reaction_calls = len(tokens) * reaction_casts * reaction_pages
    plan = {
        "tokens": len(tokens),
        "search_queries": queries,
        "max_search_calls": max_search_calls,
        "max_reaction_calls": max_reaction_calls,
        "max_neynar_calls": max_search_calls + max_reaction_calls,
    }
    logger.info(
        "plan: %d tokens -> %d search queries x %d pages = up to %d search calls; "
        "up to %d reaction calls (%d casts x %d pages per token); "
        "worst case %d Neynar requests",
        plan["tokens"],
        queries,
        search_pages,
        max_search_calls,
        max_reaction_calls,
        reaction_casts,
        reaction_pages,
        plan["max_neynar_calls"],
    )
    for token in tokens.to_dict("records"):
        logger.info(
            "  qualified: %s chain=%s symbol=%s volume_usd=%s platform=%s",
            token["token_address"],
            token["chain_id"],
            token.get("symbol") or "?",
            (
                f"{token['volume_usd']:,.0f}"
                if pd.notna(token.get("volume_usd"))
                else "n/a"
            ),
            token.get("platform") or "?",
        )
    return plan


# --- run -----------------------------------------------------------------


def run(window, args) -> dict:
    notes: list[str] = []
    writer = RunWriter(PIPELINE, dry_run=args.dry_run)
    runner = DuneRunner(dry_run=args.dry_run)

    search_pages = args.max_search_pages or args.limit or DEFAULT_SEARCH_PAGES
    reaction_casts = args.max_reaction_casts or args.limit or DEFAULT_REACTION_CASTS
    reaction_pages = args.max_reaction_pages

    registry, registry_notes = load_token_registry()
    notes.extend(registry_notes)
    tokens = select_tokens(runner, registry, args, notes)

    params = {
        "min_volume_usd": args.min_volume,
        "max_tokens": args.max_tokens,
        "explicit_tokens": args.tokens,
        "attribution_window_days": ATTRIBUTION_WINDOW_DAYS,
        "max_search_pages": search_pages,
        "max_reaction_casts": reaction_casts,
        "max_reaction_pages": reaction_pages,
        "reaction_types": args.reaction_types,
        "limit": args.limit,
    }

    if tokens.empty:
        logger.warning(
            "no qualifying tokens. Backfill clanker_tokens/bankr_tokens first, "
            "or pass --tokens / lower --min-volume."
        )
        notes.append("no qualifying tokens; all outputs are empty")
        writer.write("token_casts", pd.DataFrame(columns=TOKEN_CAST_COLUMNS))
        writer.write("attributions", pd.DataFrame(columns=ATTRIBUTION_COLUMNS))
        writer.write("evangelist_summary", pd.DataFrame(columns=SUMMARY_COLUMNS))
        writer.finish(params=params, since=window.since, new_watermark=None, notes=notes)
        return {"tokens": 0, "casts": 0, "attributions": 0, "evangelists": 0}

    plan = log_plan(tokens, search_pages, reaction_casts, reaction_pages)
    params["plan"] = plan

    if args.dry_run:
        for chain_id, group in tokens.groupby("chain_id"):
            addresses = sorted(set(group["token_address"]))
            try:
                sql = (
                    token_buys_for_attribution_sql(addresses[0], chain_id, window.query_since)
                    if len(addresses) == 1
                    else token_buys_for_attribution_multi_sql(
                        addresses, chain_id, window.query_since
                    )
                )
            except sqlfmt.SqlLiteralError as exc:
                logger.warning("could not render buys SQL for chain %s: %s", chain_id, exc)
                continue
            logger.info("[dry-run] buys query for chain %s:\n%s", chain_id, sql)
        writer.write("token_casts", pd.DataFrame(columns=TOKEN_CAST_COLUMNS))
        writer.write("attributions", pd.DataFrame(columns=ATTRIBUTION_COLUMNS))
        writer.write("evangelist_summary", pd.DataFrame(columns=SUMMARY_COLUMNS))
        writer.finish(params=params, since=window.since, new_watermark=None, notes=notes)
        return {"dry_run": True, **plan}

    # Casts must reach back one attribution window further than the buys: a buy
    # on the first day of the window can be earned by a cast five days before it.
    cast_since = pd.Timestamp(window.query_since).tz_convert("UTC") - pd.Timedelta(
        days=ATTRIBUTION_WINDOW_DAYS
    )
    logger.info("casts from %s, buys from %s", cast_since.isoformat(), window.query_since.isoformat())

    client = NeynarClient()
    address_fids: dict[str, int] = {}
    load_linked_wallets(address_fids, notes)

    all_casts: list[dict] = []
    all_engagements: list[dict] = []
    for token in tokens.to_dict("records"):
        casts = collect_token_casts(client, token, cast_since, search_pages, address_fids)
        if not casts:
            continue
        all_casts.extend(casts)
        all_engagements.extend(
            collect_engagements(
                client,
                casts,
                reaction_casts,
                reaction_pages,
                args.reaction_types,
                address_fids,
            )
        )
    logger.info(
        "neynar: %d requests for %d casts and %d engagements",
        client.http.request_count,
        len(all_casts),
        len(all_engagements),
    )

    casts_df = pd.DataFrame(all_casts, columns=TOKEN_CAST_COLUMNS)
    engagements_df = pd.DataFrame(
        all_engagements,
        columns=[
            "token_address",
            "chain_id",
            "cast_hash",
            "author_fid",
            "engager_fid",
            "engaged_at",
            "engagement",
        ],
    )
    if not engagements_df.empty:
        engagements_df["engaged_at"] = _to_utc(engagements_df["engaged_at"])
        engagements_df = engagements_df[engagements_df["engaged_at"].notna()]

    buys = fetch_buys(runner, tokens, window.query_since, args.limit, notes)
    logger.info("dune: %d buys across %d tokens", len(buys), buys["token_address"].nunique() if not buys.empty else 0)
    buys = resolve_buyer_fids(buys, address_fids)
    known = int(buys["buyer_fid"].notna().sum()) if not buys.empty else 0
    if not buys.empty:
        logger.info(
            "%d/%d buys map to a Farcaster fid (%.1f%%)",
            known,
            len(buys),
            100.0 * known / len(buys),
        )
        if known == 0:
            notes.append(
                "no buyer wallet resolved to a fid; attribution is empty. "
                "Backfill linked_wallets to widen the map."
            )

    attributions = attribute(engagements_df, buys, ATTRIBUTION_WINDOW_DAYS)
    summary = summarise(casts_df, attributions)

    out_attributions = attributions.copy()
    if not out_attributions.empty:
        out_attributions["block_time"] = out_attributions["block_time"].map(
            lambda t: t.isoformat() if pd.notna(t) else ""
        )

    writer.write("token_casts", casts_df[TOKEN_CAST_COLUMNS])
    writer.write("attributions", out_attributions[ATTRIBUTION_COLUMNS])
    writer.write("evangelist_summary", summary[SUMMARY_COLUMNS])

    new_watermark = None
    if not buys.empty:
        latest = buys["block_time"].max()
        if pd.notna(latest):
            new_watermark = latest.to_pydatetime()

    writer.finish(
        params=params, since=window.since, new_watermark=new_watermark, notes=notes
    )
    set_watermark(PIPELINE, new_watermark, run_ts=writer.run_ts)

    return {
        "tokens": int(len(tokens)),
        "casts": int(len(casts_df)),
        "engagements": int(len(engagements_df)),
        "buys": int(len(buys)),
        "buys_with_fid": known,
        "attributions": int(len(attributions)),
        "evangelists": int(len(summary)),
        "attributed_usd": float(summary["attributed_usd"].sum()) if not summary.empty else 0.0,
        "neynar_requests": client.http.request_count,
        "dune": runner.summary(),
    }


def main(argv=None) -> int:
    parser = base_parser(
        PIPELINE,
        "Attribute token purchases to the Farcaster accounts that talked them up.",
    )
    parser.add_argument(
        "--min-volume",
        type=float,
        default=EVANGELIST_MIN_VOLUME_USD,
        help=f"Lifetime DEX volume a token needs to qualify (default {EVANGELIST_MIN_VOLUME_USD:,.0f}).",
    )
    parser.add_argument(
        "--tokens",
        default=None,
        help="Comma-separated token addresses to analyse, bypassing the volume gate.",
    )
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=DEFAULT_MAX_TOKENS,
        help=f"Hard cap on tokens per run — the main cost control (default {DEFAULT_MAX_TOKENS}).",
    )
    parser.add_argument(
        "--chain-id",
        type=int,
        default=CHAIN_ARBITRUM,
        help="Chain assumed for --tokens addresses absent from the registries.",
    )
    parser.add_argument(
        "--max-search-pages",
        type=int,
        default=None,
        help=f"Cast-search pages per query, 100 casts each (default {DEFAULT_SEARCH_PAGES}, or --limit).",
    )
    parser.add_argument(
        "--max-reaction-casts",
        type=int,
        default=None,
        help=f"Casts per token to fetch reactions for (default {DEFAULT_REACTION_CASTS}, or --limit).",
    )
    parser.add_argument(
        "--max-reaction-pages",
        type=int,
        default=DEFAULT_REACTION_PAGES,
        help=f"Reaction pages per cast, 100 reactors each (default {DEFAULT_REACTION_PAGES}).",
    )
    parser.add_argument(
        "--reaction-types",
        default="likes,recasts",
        help="Neynar reaction types counted as engagement (default likes,recasts).",
    )
    args = parser.parse_args(argv)
    setup_logging(args.log_level)
    window = resolve_window(args, PIPELINE)
    result = run(window, args)
    logger.info("%s done: %s", PIPELINE, json.dumps(result, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
