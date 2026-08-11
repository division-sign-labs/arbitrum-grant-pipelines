"""Farcaster users who bought at least $50 of a Clanker or Bankr launch token.

PRODUCES  token_buyers/<run_ts>/buys.csv
          fid,buyer_address,token_address,chain_id,platform,amount_usd,
          token_amount,block_time,tx_hash
          One row per (buyer, token, transaction), feeding
          (Wallet)-[:BOUGHT]->(Token) and the attribution stage of
          token_evangelists.

SOURCE    `dex.trades` on Dune. It is the only cross-DEX decoded table that
          carries a USD value on every row, and — verified live — it does cover
          the Uniswap v4 hook pools that Clanker deploys on Arbitrum
          (project='uniswap', version='4'), which was the main open question.
          Reconstructing buys from `erc20_arbitrum.evt_transfer` would mean
          pricing brand-new launch tokens ourselves, and `prices.usd` does not
          carry them.

WHY THE FID JOIN IS LOCAL
          Dune only permits public table uploads on this account, so the
          Farcaster wallet set never leaves the machine. The buyer set coming
          back from `dex.trades` is small (a few tokens' worth of trades), so a
          pandas join against the linked-wallet map is both cheaper and more
          private than an upload.

WHY TWO CANDIDATE ADDRESSES PER TRADE
          Verified on real Clanker v4 trades: a single `taker` address appears
          across dozens of distinct `tx_from` values — the decoder records the
          router/hook, not the human who signed. `taker` alone would therefore
          resolve almost no Farcaster users on v4 pools. The SQL emits both
          `taker` and `tx_from`; we prefer `taker` when it maps to an fid (it is
          the address that actually received the tokens) and fall back to
          `tx_from` otherwise, and the address that matched is what lands in
          `buyer_address` so the graph edge hangs off a wallet we know.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable

import pandas as pd

from config.settings import (
    CHAIN_ARBITRUM,
    CHAIN_ROBINHOOD,
    DUNE_CHAIN_NAMES,
    MIN_BUY_USD,
)
from lib import runs, state
from lib.cli import base_parser, resolve_window
from lib.dune import DuneError, DuneRunner
from lib.logging_utils import setup_logging
from sql import trades as trades_sql

logger = logging.getLogger(__name__)

PIPELINE = "token_buyers"
DATA_TYPE = "token_buyers"

BUYS_COLUMNS = [
    "fid",
    "buyer_address",
    "token_address",
    "chain_id",
    "platform",
    "amount_usd",
    "token_amount",
    "block_time",
    "tx_hash",
]

# Registry run -> platform label, in precedence order. A token address that
# somehow appears in both registries keeps the first platform listed here.
REGISTRIES: tuple[tuple[str, str], ...] = (
    ("clanker_tokens", "clanker"),
    ("bankr_tokens", "bankr"),
)

# Chains a launch token can plausibly live on for this grant. Bankr can deploy
# elsewhere; anything outside this set is reported rather than silently queried.
SUPPORTED_CHAINS = (CHAIN_ARBITRUM, CHAIN_ROBINHOOD)


# --- inputs ---------------------------------------------------------------


def _load_registry(data_type: str, platform: str, notes: list[str]) -> pd.DataFrame:
    """Token registry from the latest completed run, or empty if there is none.

    `runs.read_csv(required=False)` only covers a missing file inside an
    existing run; with no completed run at all it raises, so both cases are
    handled here. A missing registry degrades the run rather than failing it.
    """
    try:
        df = runs.read_csv(data_type, "tokens", required=False)
    except FileNotFoundError:
        logger.warning(
            "no completed %s run; %s tokens will be skipped "
            "(run `python -m pipelines.%s --backfill` first)",
            data_type,
            platform,
            data_type,
        )
        notes.append(f"no {data_type} run available; {platform} tokens skipped")
        return pd.DataFrame(columns=["token_address", "chain_id", "platform"])

    if df.empty or "token_address" not in df.columns:
        logger.warning("%s run has no usable tokens.csv", data_type)
        notes.append(f"{data_type} run contained no tokens")
        return pd.DataFrame(columns=["token_address", "chain_id", "platform"])

    out = df[["token_address", "chain_id"]].copy()
    out["platform"] = platform
    logger.info("%s: %d token rows", data_type, len(out))
    return out


def _token_universe(notes: list[str]) -> pd.DataFrame:
    """All launch tokens to watch, as token_address / chain_id / platform."""
    frames = [_load_registry(dt, platform, notes) for dt, platform in REGISTRIES]
    universe = pd.concat(frames, ignore_index=True)
    if universe.empty:
        return universe

    universe["token_address"] = (
        universe["token_address"].astype("string").str.strip().str.lower()
    )
    universe["chain_id"] = pd.to_numeric(universe["chain_id"], errors="coerce")

    bad_chain = int(universe["chain_id"].isna().sum())
    if bad_chain:
        logger.warning("dropping %d registry rows with no chain_id", bad_chain)
        notes.append(f"dropped {bad_chain} registry rows with an unparseable chain_id")
    universe = universe.dropna(subset=["token_address", "chain_id"])
    universe["chain_id"] = universe["chain_id"].astype(int)

    # Registry order is the precedence order, so the first row for an address wins.
    universe = universe.drop_duplicates(subset=["token_address", "chain_id"], keep="first")

    unsupported = universe[~universe["chain_id"].isin(SUPPORTED_CHAINS)]
    if not unsupported.empty:
        counts = unsupported["chain_id"].value_counts().to_dict()
        logger.warning("skipping tokens on unsupported chains: %s", counts)
        notes.append(f"skipped tokens on chains outside this grant's scope: {counts}")
        universe = universe[universe["chain_id"].isin(SUPPORTED_CHAINS)]

    return universe.reset_index(drop=True)


def _coerce_fid_map(mapping) -> dict[str, int]:
    """Normalise whatever the resolver hands back into address(lower) -> fid."""
    if isinstance(mapping, pd.DataFrame):
        if mapping.empty or not {"fid", "address"} <= set(mapping.columns):
            return {}
        pairs = zip(mapping["address"], mapping["fid"])
    elif isinstance(mapping, dict):
        pairs = mapping.items()
    else:
        pairs = ((row.get("address"), row.get("fid")) for row in mapping)

    out: dict[str, int] = {}
    for address, fid in pairs:
        if not isinstance(address, str):
            continue
        try:
            fid_int = int(fid)
        except (TypeError, ValueError):
            continue
        key = address.strip().lower()
        # One address can be verified by more than one fid; keep the lowest so
        # repeated runs attribute the buy to the same account every time.
        if key not in out or fid_int < out[key]:
            out[key] = fid_int
    return out


def _load_fid_map(notes: list[str]) -> dict[str, int]:
    """address -> fid, from lib.fid_resolver if it exists, else linked_wallets."""
    try:
        from lib import fid_resolver  # noqa: PLC0415 — optional sibling module
    except ImportError:
        fid_resolver = None

    resolver = getattr(fid_resolver, "wallet_to_fid", None)
    if resolver is not None:
        try:
            mapping = _coerce_fid_map(resolver())
        except (TypeError, ValueError, FileNotFoundError) as exc:
            logger.warning("lib.fid_resolver.wallet_to_fid() unusable (%s); "
                           "falling back to linked_wallets", exc)
        else:
            if mapping:
                logger.info("fid map: %d addresses (lib.fid_resolver)", len(mapping))
                return mapping
            logger.warning("lib.fid_resolver returned nothing; trying linked_wallets")

    try:
        wallets = runs.read_csv("linked_wallets", "wallets", required=False)
    except FileNotFoundError:
        wallets = pd.DataFrame()

    if wallets.empty or not {"fid", "address"} <= set(wallets.columns):
        notes.append("no linked_wallets run: no buys could be attributed to an fid")
        return {}

    if "protocol" in wallets.columns:
        # Solana addresses cannot collide with hex, but excluding them keeps the
        # map small and makes the intent explicit.
        wallets = wallets[wallets["protocol"].astype("string").str.lower() != "solana"]

    mapping = _coerce_fid_map(wallets)
    logger.info("fid map: %d addresses (linked_wallets)", len(mapping))
    return mapping


# --- extraction -----------------------------------------------------------


def _chain_supported_on_dune(
    dune: DuneRunner, chain_id: int, notes: list[str]
) -> bool:
    """Does `dex.trades` carry this chain at all?

    Robinhood Chain is young enough that DEX decoding could lag; probing once is
    a metadata-cheap way to find out. A "no" downgrades that chain to zero rows
    instead of failing the run.
    """
    name = DUNE_CHAIN_NAMES.get(chain_id, str(chain_id))
    if dune.dry_run:
        # A dry run gets an empty frame back from every query, which would look
        # exactly like "chain absent". Assume coverage so the plan still shows
        # the SQL this chain would run.
        logger.info("[dry-run] assuming dex.trades covers %s", name)
        return True
    try:
        found = dune.run_sql(
            trades_sql.blockchain_available_sql(chain_id),
            label=f"dex.trades coverage probe {name}",
        )
    except DuneError as exc:
        logger.warning("coverage probe for %s failed (%s); skipping chain", name, exc)
        notes.append(f"dex.trades coverage probe failed for {name}: {str(exc)[:200]}")
        return False

    if found.empty:
        logger.warning("dex.trades has no rows for blockchain=%r; skipping chain", name)
        notes.append(f"dex.trades does not cover blockchain={name!r}; no buys collected there")
        return False
    return True


def chains_enabled(chain_id, extra_chains: set[str]) -> bool:
    """Whether a non-Arbitrum chain was explicitly opted into via --chains."""
    if "all" in extra_chains:
        return True
    name = DUNE_CHAIN_NAMES.get(int(chain_id), str(chain_id))
    return name in extra_chains or str(chain_id) in extra_chains


def _fetch_buys(
    dune: DuneRunner,
    universe: pd.DataFrame,
    since: datetime,
    min_usd: float,
    limit: int | None,
    notes: list[str],
    extra_chains: set[str] | None = None,
) -> tuple[pd.DataFrame, dict]:
    """Run the buy query per chain, chunked over the token list."""
    extra_chains = extra_chains or set()
    frames: list[pd.DataFrame] = []
    stats: dict[str, dict] = {}

    for chain_id, group in universe.groupby("chain_id", sort=True):
        name = DUNE_CHAIN_NAMES.get(int(chain_id), str(chain_id))
        tokens = trades_sql.normalise_addresses(group["token_address"].tolist())
        if not tokens:
            logger.warning("chain %s: no valid token addresses", name)
            continue

        # Robinhood carries ~67k Bankr tokens, so it costs ~135 executions and a
        # large share of a billing cycle's datapoints — and it has already been
        # measured as a near-dead end for this grant: only 1.1% of Bankr launch
        # wallets belong to a Farcaster account. Arbitrum is the deliverable, so
        # the expensive chain is opt-in rather than a default the operator
        # discovers by running out of Dune credits mid-backfill.
        if int(chain_id) != CHAIN_ARBITRUM and not chains_enabled(chain_id, extra_chains):
            logger.info(
                "chain %s: skipped (%d tokens). Enable with --chains all or --chains %s",
                name,
                len(tokens),
                name,
            )
            notes.append(
                f"chain {name} skipped ({len(tokens)} tokens); re-run with --chains {name} to include it"
            )
            stats[name] = {"tokens": len(tokens), "queries": 0, "rows": 0, "skipped": True}
            continue

        # Arbitrum is the grant's home chain and is certain to be decoded;
        # anything else gets probed before we spend a scan on it.
        if int(chain_id) != CHAIN_ARBITRUM and not _chain_supported_on_dune(
            dune, int(chain_id), notes
        ):
            stats[name] = {"tokens": len(tokens), "queries": 0, "rows": 0, "covered": False}
            continue

        queries = trades_sql.token_buys_sql(tokens, chain_id, since, min_usd)
        if limit:
            # A smoke test should cost one execution, not one per token chunk.
            queries = queries[:1]
        logger.info(
            "chain %s: %d tokens -> %d quer%s", name, len(tokens), len(queries),
            "y" if len(queries) == 1 else "ies",
        )

        rows = 0
        for index, sql in enumerate(queries, start=1):
            df = dune.run_sql(
                sql, label=f"{PIPELINE} buys {name} {index}/{len(queries)}", limit=limit
            )
            if df.empty:
                continue
            df["chain_id"] = int(chain_id)
            frames.append(df)
            rows += len(df)
        stats[name] = {
            "tokens": len(tokens),
            "queries": len(queries),
            "rows": rows,
            "covered": True,
        }

    if not frames:
        return pd.DataFrame(), stats
    return pd.concat(frames, ignore_index=True), stats


# --- shaping --------------------------------------------------------------


def _parse_block_time(values: pd.Series) -> pd.Series:
    """Dune's CSV encoding is '2025-10-24 23:26:03.000 UTC'; make it tz-aware."""
    cleaned = values.astype("string").str.replace(" UTC", "+00:00", regex=False)
    return pd.to_datetime(cleaned, utc=True, errors="coerce", format="mixed")


def _resolve_buyers(raw: pd.DataFrame, fid_map: dict[str, int]) -> pd.DataFrame:
    """Attach an fid to every trade we can, dropping the ones we cannot."""
    df = raw.copy()
    for column in ("buyer_address", "tx_from", "token_address", "tx_hash"):
        df[column] = df[column].astype("string").str.strip().str.lower()

    mapping = pd.Series(fid_map, dtype="Int64") if fid_map else pd.Series(dtype="Int64")
    taker_fid = df["buyer_address"].map(mapping).astype("Int64")
    sender_fid = df["tx_from"].map(mapping).astype("Int64")

    use_taker = taker_fid.notna()
    df["fid"] = taker_fid.where(use_taker, sender_fid)
    df["buyer_address"] = df["buyer_address"].where(use_taker, df["tx_from"])

    resolved = df[df["fid"].notna()].copy()
    logger.info(
        "resolved %d/%d trades to an fid (%d via taker, %d via tx_from)",
        len(resolved),
        len(df),
        int(use_taker.sum()),
        int((~use_taker & sender_fid.notna()).sum()),
    )
    return resolved


def _shape_buys(resolved: pd.DataFrame, universe: pd.DataFrame) -> pd.DataFrame:
    """Collapse to one row per (buyer, token, tx) and apply the CSV contract."""
    if resolved.empty:
        return pd.DataFrame(columns=BUYS_COLUMNS)

    df = resolved.merge(
        universe[["token_address", "chain_id", "platform"]],
        on=["token_address", "chain_id"],
        how="left",
    )
    df["platform"] = df["platform"].fillna("unknown")
    df["amount_usd"] = pd.to_numeric(df["amount_usd"], errors="coerce").fillna(0.0)
    df["token_amount"] = pd.to_numeric(df["token_amount"], errors="coerce").fillna(0.0)
    df["block_time"] = _parse_block_time(df["block_time"])
    df = df.dropna(subset=["block_time"])

    # A router can split one buy across several hops, emitting several trade
    # rows in one transaction. The graph MERGEs :BOUGHT on txHash, so fold them
    # here instead of letting ingestion silently keep whichever arrived last.
    grouped = df.groupby(
        ["fid", "buyer_address", "token_address", "chain_id", "platform", "tx_hash"],
        as_index=False,
        dropna=False,
    ).agg(
        amount_usd=("amount_usd", "sum"),
        token_amount=("token_amount", "sum"),
        block_time=("block_time", "min"),
    )

    grouped["fid"] = grouped["fid"].astype("Int64")
    grouped["chain_id"] = grouped["chain_id"].astype(int)
    grouped = grouped.sort_values(["block_time", "tx_hash"]).reset_index(drop=True)
    out = grouped[BUYS_COLUMNS].copy()
    out["block_time"] = grouped["block_time"].map(
        lambda ts: ts.isoformat() if pd.notna(ts) else None
    )
    return out


# --- orchestration --------------------------------------------------------


def run(window, args) -> dict:
    notes: list[str] = []
    writer = runs.RunWriter(DATA_TYPE, dry_run=args.dry_run)
    dune = DuneRunner(dry_run=args.dry_run)

    universe = _token_universe(notes)
    logger.info("watching %d launch tokens", len(universe))

    fid_map = _load_fid_map(notes)
    if not fid_map and not args.dry_run:
        raise SystemExit(
            "No wallet->fid map available. Run "
            "`python -m pipelines.linked_wallets --backfill` first."
        )

    if universe.empty:
        logger.warning("no launch tokens to watch; writing an empty buys.csv")
        notes.append("no tokens in either registry; nothing to query")
        raw, stats = pd.DataFrame(), {}
    else:
        raw, stats = _fetch_buys(
            dune,
            universe,
            window.query_since,
            MIN_BUY_USD,
            args.limit,
            notes,
            extra_chains={c.strip().lower() for c in (args.chains or "").split(",") if c.strip()},
        )

    if raw.empty:
        buys = pd.DataFrame(columns=BUYS_COLUMNS)
        new_watermark = None
    else:
        buys = _shape_buys(_resolve_buyers(raw, fid_map), universe)
        new_watermark = state.max_timestamp(buys["block_time"])

    writer.write("buys", buys)
    writer.finish(
        params={
            "min_usd": MIN_BUY_USD,
            "tokens_watched": int(len(universe)),
            "chains": stats,
            "limit": args.limit,
            "dune": dune.summary(),
        },
        since=window.since,
        new_watermark=new_watermark,
        notes=notes,
    )
    if not args.dry_run:
        state.set_watermark(PIPELINE, new_watermark, run_ts=writer.run_ts)

    return {
        "run_ts": writer.run_ts,
        "tokens_watched": int(len(universe)),
        "trades_seen": int(len(raw)),
        "buys": int(len(buys)),
        "unique_fids": int(buys["fid"].nunique()) if not buys.empty else 0,
        "watermark": new_watermark.isoformat() if new_watermark else None,
        "notes": notes,
    }


def main(argv: Iterable[str] | None = None) -> int:
    parser = base_parser(PIPELINE, __doc__.splitlines()[0])
    parser.add_argument(
        "--chains",
        default="",
        help=(
            "Comma-separated non-Arbitrum chains to include, or 'all'. Arbitrum "
            "always runs. Robinhood is off by default: it is ~135 Dune executions "
            "over 67k Bankr tokens whose wallets are only ~1%% Farcaster."
        ),
    )
    args = parser.parse_args(argv)
    setup_logging(args.log_level)

    window = resolve_window(args, PIPELINE)
    summary = run(window, args)
    logger.info("%s done: %s", PIPELINE, summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
