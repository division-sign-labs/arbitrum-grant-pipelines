"""Which Farcaster users are actually positioned in blue-chip Arbitrum assets.

Produces four CSVs under data/popular_tokens/<run_ts>/ describing the index in
config.tokens (ARB, PENDLE, L3 plus the four Gauntlet MetaMorpho vaults):

  trades.csv          every swap into or out of an index token, per wallet
  holdings.csv        current net balance per (wallet, index token)
  vault_deposits.csv  ERC-4626 deposits into the Gauntlet vaults
  lp_events.csv       Uniswap v3 Mint/Burn on pools holding an index token

Why these four and not one: "holds ARB" and "trades ARB" and "LPs ARB" are
different claims about a person, and the graph models them as different edges
(HOLDS / TRADED / PROVIDED_LIQUIDITY / DEPOSITED_IN). Collapsing them would
throw away the distinction the grant is actually asking about.

The Farcaster join is done locally, in pandas. Dune only permits PUBLIC uploads
on this account, so the wallet set never leaves the machine — every leg pulls
chain-side rows and intersects them against lib.fid_resolver's wallet map here.

Cost control, which is the whole difficulty of this pipeline:

* trades — ARB alone is ~1M dex.trades rows a month, so an 18-month backfill is
  ~19M source rows. The leg is therefore run in fixed-length time slices
  (--trade-chunk-days) so each Dune execution is bounded and the run is
  restartable, and each slice is intersected against the wallet set and thrown
  away before the next one is fetched. It is NOT chunked by wallet set: the
  wallet set is far larger than the number of time slices, and chunking by it
  would multiply full-table scans instead of dividing them. A $50 floor
  (--min-trade-usd, default settings.MIN_BUY_USD) removes dust.

* holdings — a net balance needs the *whole* transfer history, so this is the
  one leg that must be restricted to the wallet set on the Dune side; otherwise
  the group-by emits a row per distinct ARB holder ever. Chunked at 1000
  addresses per query. If the wallet set is big enough that the chunk count
  would exceed --max-holdings-chunks, the run refuses rather than quietly
  spending a fortune, and tells the operator to narrow the set or opt in to the
  unrestricted aggregate.

* vaults and LP — small enough to pull whole (4 vault contracts + one topic0 is
  ~39k rows of all time; index pools see ~9k mints a month), so they run
  unrestricted by default and intersect locally. --restrict always forces the
  wallet-set filter for a small cohort.

Each leg is independent: if one fails — a table moves, a decode drifts — it logs
a warning, writes a correctly-shaped empty CSV, records a note on the manifest,
and the other three still finish. The watermark only advances when every leg ran
and every leg succeeded, so a partial run can never fool the next incremental
into skipping a window it has not actually read.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta

import pandas as pd

from config.settings import CHAIN_ARBITRUM, MIN_BUY_USD
from config.tokens import GAUNTLET_VAULTS, token_addresses
from lib.cli import base_parser, resolve_window
from lib.dune import DuneError, DuneRunner
from lib.logging_utils import setup_logging
from lib.runs import RunWriter
from lib.sqlfmt import SqlLiteralError, chunked
from lib.state import parse_ts, set_watermark
from sql.popular import (
    index_holdings_sql,
    index_trades_sql,
    lp_events_sql,
    vault_deposits_sql,
)

logger = logging.getLogger(__name__)

PIPELINE = "popular_tokens"
DATA_TYPE = "popular_tokens"

LEGS = ("trades", "holdings", "vaults", "lp")

# The ingestion contract. These headers are written even when a leg degrades.
TRADES_COLUMNS = [
    "fid",
    "address",
    "token_address",
    "chain_id",
    "side",
    "amount_usd",
    "token_amount",
    "block_time",
    "tx_hash",
]
HOLDINGS_COLUMNS = [
    "fid",
    "address",
    "token_address",
    "chain_id",
    "balance",
    "balance_raw",
    "last_activity_at",
]
VAULT_COLUMNS = [
    "fid",
    "address",
    "vault_address",
    "chain_id",
    "assets",
    "assets_raw",
    "shares_raw",
    "block_time",
    "tx_hash",
]
LP_COLUMNS = [
    "fid",
    "address",
    "pool_address",
    "token_address",
    "chain_id",
    "event",
    "amount0",
    "amount1",
    "block_time",
    "tx_hash",
]

OUTPUTS = {
    "trades": ("trades", TRADES_COLUMNS),
    "holdings": ("holdings", HOLDINGS_COLUMNS),
    "vaults": ("vault_deposits", VAULT_COLUMNS),
    "lp": ("lp_events", LP_COLUMNS),
}

# 1000 addresses is ~43KB of SQL text per IN-list; comfortably under any
# statement-size limit while keeping the chunk count low.
HOLDINGS_CHUNK = 1000
# Above this many chunks the restricted holdings leg costs more than it saves.
MAX_HOLDINGS_CHUNKS = 25
# Slices for the trades leg. A month of index trades is ~500k rows at the $50
# floor, which paginates in ~17 result pages — a comfortable single execution.
TRADE_CHUNK_DAYS = 30


# -- shared helpers --------------------------------------------------------


def _to_iso(series: pd.Series) -> pd.Series:
    """Dune's "2026-08-10 05:08:29.000 UTC" -> ISO-8601 UTC, vectorised."""
    if series.empty:
        return series.astype("object")
    cleaned = series.astype(str).str.replace(" UTC", "+00:00", regex=False)
    parsed = pd.to_datetime(cleaned, utc=True, errors="coerce", format="mixed")
    return parsed.dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")


def _empty(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def _load_wallet_map(run_id: str | None, allow_missing: bool) -> dict[str, int]:
    """{lowercased eth address: fid} from the latest linked_wallets run.

    Imported lazily and defensively: this pipeline is useless without the wallet
    map, but a --dry-run should still be able to render its plan on a machine
    where linked_wallets has never been run.
    """
    try:
        from lib.fid_resolver import wallet_to_fid

        mapping = wallet_to_fid(run_id=run_id)
    except (ImportError, AttributeError) as exc:
        logger.warning("lib.fid_resolver unavailable (%s); falling back to read_csv", exc)
        mapping = _wallet_map_from_csv(run_id)
    except FileNotFoundError as exc:
        if not allow_missing:
            raise SystemExit(
                f"{exc}\nRun `python -m pipelines.linked_wallets --backfill` first: "
                f"{PIPELINE} joins every on-chain row against that wallet set locally."
            ) from exc
        logger.warning("no linked_wallets run (%s); continuing with an empty map", exc)
        mapping = {}

    logger.info("wallet map: %d eth addresses", len(mapping))
    if not mapping and not allow_missing:
        raise SystemExit(
            "linked_wallets produced no eth addresses; nothing to join against."
        )
    return mapping


def _wallet_map_from_csv(run_id: str | None) -> dict[str, int]:
    """Fallback path if lib.fid_resolver ever loses wallet_to_fid."""
    from lib.runs import read_csv

    df = read_csv("linked_wallets", "wallets", run_id=run_id)
    if df.empty:
        return {}
    eth = df[df["protocol"].astype(str).str.lower() == "eth"].copy()
    eth["address"] = eth["address"].astype(str).str.strip().str.lower()
    eth = eth[eth["fid"].notna()]
    eth = eth.sort_values("fid").drop_duplicates(subset=["address"], keep="first")
    return dict(zip(eth["address"], eth["fid"].astype(int)))


def _attach_fid(
    df: pd.DataFrame, wallets: dict[str, int], *candidates: str
) -> pd.DataFrame:
    """Keep rows where any candidate address column is a known Farcaster wallet.

    Candidates are tried in order, so `taker` (the semantic trader) wins over
    `tx_from` (the EOA that signed) when both are Farcaster wallets — a single
    trade never produces two rows.
    """
    if df.empty:
        return df.assign(fid=pd.Series(dtype="int64"), address=pd.Series(dtype="object"))

    fid = pd.Series(pd.NA, index=df.index, dtype="object")
    addr = pd.Series(pd.NA, index=df.index, dtype="object")
    for column in candidates:
        if column not in df.columns:
            continue
        normalised = df[column].astype(str).str.strip().str.lower()
        matched = normalised.map(wallets)
        take = fid.isna() & matched.notna()
        fid = fid.mask(take, matched)
        addr = addr.mask(take, normalised)

    out = df.assign(fid=fid, address=addr)
    out = out[out["fid"].notna()].copy()
    out["fid"] = out["fid"].astype("int64")
    return out


def _collapse(
    df: pd.DataFrame,
    keys: list[str],
    sums: list[str],
    big_int_sums: list[str] | None = None,
    first: list[str] | None = None,
) -> pd.DataFrame:
    """Fold rows that ingestion would MERGE onto one edge, summing their amounts.

    One swap routed through three pools is three dex.trades rows for the same
    (wallet, token, side, txHash); one multi-range LP add is several Mint events
    in one tx. Ingestion keys those edges on txHash, so simply dropping the
    duplicates would quietly throw the extra volume away. Summing keeps the
    edge's usd/amount equal to what the wallet actually moved.

    `big_int_sums` are exact uint256 strings (assets/shares) and are summed as
    Python ints, because they routinely exceed float64's 53 bits of mantissa.
    """
    if df.empty:
        return df
    big_int_sums = big_int_sums or []
    first = first or []

    work = df.copy()
    for column in big_int_sums:
        work[column] = work[column].map(lambda v: int(str(v)))

    agg = {c: "sum" for c in sums + big_int_sums}
    agg.update({c: "first" for c in first})
    out = work.groupby(keys, as_index=False, sort=False).agg(agg)

    for column in big_int_sums:
        out[column] = out[column].map(lambda v: str(int(v)))
    return out


def _run_chunks(
    dune: DuneRunner,
    builder,
    chunks: list[list[str]],
    label: str,
    limit: int | None,
) -> pd.DataFrame:
    """Execute one query per address chunk and concatenate the results."""
    frames: list[pd.DataFrame] = []
    for i, chunk in enumerate(chunks, start=1):
        df = dune.run_sql(builder(chunk), label=f"{label} {i}/{len(chunks)}", limit=limit)
        if not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# -- leg 1: trades ---------------------------------------------------------


def leg_trades(
    dune: DuneRunner, wallets: dict[str, int], window, args, index_addresses: list[str]
) -> pd.DataFrame:
    """dex.trades sliced by time, intersected with the wallet set per slice."""
    since = window.query_since
    end = datetime.now(tz=since.tzinfo)
    step = timedelta(days=max(1, int(args.trade_chunk_days)))

    restrict = sorted(wallets) if args.restrict == "always" and wallets else None
    if restrict:
        logger.info("trades: restricting to %d wallets on the Dune side", len(restrict))

    frames: list[pd.DataFrame] = []
    slice_start = since
    slices = 0
    while slice_start < end:
        slice_end = min(slice_start + step, end)
        sql = index_trades_sql(
            index_addresses,
            slice_start,
            until=slice_end,
            min_amount_usd=args.min_trade_usd,
            trader_addresses=restrict,
        )
        raw = dune.run_sql(
            sql,
            label=f"index trades {slice_start:%Y-%m-%d}",
            limit=args.limit,
        )
        slices += 1
        if not raw.empty:
            matched = _attach_fid(raw, wallets, "taker_address", "tx_from_address")
            logger.info(
                "trades %s: %d source rows -> %d farcaster rows",
                f"{slice_start:%Y-%m-%d}",
                len(raw),
                len(matched),
            )
            if not matched.empty:
                frames.append(matched)
        # --limit means "one cheap slice", not "one cheap slice per month".
        if args.limit:
            break
        slice_start = slice_end

    logger.info("trades: %d slices executed", slices)
    if not frames:
        return _empty(TRADES_COLUMNS)

    df = pd.concat(frames, ignore_index=True)
    df["chain_id"] = CHAIN_ARBITRUM
    df["block_time"] = _to_iso(df["block_time"])
    df = _collapse(
        df,
        keys=["fid", "address", "token_address", "chain_id", "side", "tx_hash"],
        sums=["amount_usd", "token_amount"],
        first=["block_time"],
    )
    return df[TRADES_COLUMNS]


# -- leg 2: holdings -------------------------------------------------------


def leg_holdings(
    dune: DuneRunner, wallets: dict[str, int], window, args, index_addresses: list[str]
) -> pd.DataFrame:
    """Net balances, restricted to the wallet set because the scan is full-history."""
    addresses = sorted(wallets)
    chunks = list(chunked(addresses, max(1, int(args.holdings_chunk))))

    if args.holdings_unrestricted:
        logger.warning(
            "holdings: running unrestricted — the group-by emits a row per "
            "distinct index-token holder ever, not just Farcaster wallets"
        )
        raw = dune.run_sql(
            index_holdings_sql(
                index_addresses, None, window.query_since, args.min_balance_raw
            ),
            label="index holdings (unrestricted)",
            limit=args.limit,
        )
    else:
        if not chunks:
            raise ValueError("no wallet addresses to restrict the holdings scan to")
        if args.limit:
            chunks = chunks[:1]
        elif len(chunks) > int(args.max_holdings_chunks):
            raise ValueError(
                f"holdings would need {len(chunks)} Dune executions "
                f"({len(addresses)} wallets / {args.holdings_chunk} per query), over the "
                f"--max-holdings-chunks ceiling of {args.max_holdings_chunks}. Narrow the "
                f"wallet set (point --wallets-run-id at a smaller linked_wallets run), "
                f"raise --holdings-chunk, or pass --holdings-unrestricted to pull the "
                f"whole holder aggregate once and intersect locally."
            )
        raw = _run_chunks(
            dune,
            lambda chunk: index_holdings_sql(
                index_addresses, chunk, window.query_since, args.min_balance_raw
            ),
            chunks,
            "index holdings",
            args.limit,
        )

    if raw.empty:
        return _empty(HOLDINGS_COLUMNS)

    df = _attach_fid(raw, wallets, "address")
    if df.empty:
        return _empty(HOLDINGS_COLUMNS)

    df["chain_id"] = CHAIN_ARBITRUM
    df["balance_raw"] = df["balance_raw"].astype(str)
    df["last_activity_at"] = _to_iso(df["last_activity_at"])
    df = df.drop_duplicates(subset=["address", "token_address"], keep="last")
    return df[HOLDINGS_COLUMNS]


# -- leg 3: ERC-4626 vault deposits ---------------------------------------


def leg_vaults(
    dune: DuneRunner, wallets: dict[str, int], window, args
) -> pd.DataFrame:
    """Gauntlet vault deposits, scaled by the *underlying asset's* decimals."""
    vaults = [v["address"] for v in GAUNTLET_VAULTS]
    restrict = _restriction(wallets, args, "vaults")

    if restrict is None:
        raw = dune.run_sql(
            vault_deposits_sql(vaults, None, window.query_since),
            label="gauntlet vault deposits",
            limit=args.limit,
        )
    else:
        raw = _run_chunks(
            dune,
            lambda chunk: vault_deposits_sql(vaults, chunk, window.query_since),
            restrict,
            "gauntlet vault deposits",
            args.limit,
        )

    if raw.empty:
        return _empty(VAULT_COLUMNS)

    # The owner is the position holder; the sender may be a router or a relayer,
    # so it is only used when the owner is not a wallet we know.
    df = _attach_fid(raw, wallets, "owner_address", "sender_address")
    if df.empty:
        return _empty(VAULT_COLUMNS)

    df["chain_id"] = CHAIN_ARBITRUM
    df["block_time"] = _to_iso(df["block_time"])
    df = _collapse(
        df,
        keys=["fid", "address", "vault_address", "chain_id", "tx_hash"],
        sums=[],
        big_int_sums=["assets_raw", "shares_raw"],
        first=["block_time"],
    )

    # Scaled by the *underlying asset's* decimals (USDC 6, WETH 18), not the
    # share token's 18 — assets is denominated in what was deposited.
    asset_decimals = {
        v["address"]: int(v.get("asset_decimals", v["decimals"])) for v in GAUNTLET_VAULTS
    }
    divisor = df["vault_address"].map(asset_decimals).fillna(18).rpow(10.0)
    df["assets"] = df["assets_raw"].map(int) / divisor
    return df[VAULT_COLUMNS]


# -- leg 4: Uniswap v3 liquidity ------------------------------------------


def leg_lp(
    dune: DuneRunner, wallets: dict[str, int], window, args, index_addresses: list[str]
) -> pd.DataFrame:
    """Mint/Burn on index pools, fanned out to one row per matched index token."""
    restrict = _restriction(wallets, args, "lp")

    if restrict is None:
        raw = dune.run_sql(
            lp_events_sql(index_addresses, None, window.query_since),
            label="uniswap v3 index lp events",
            limit=args.limit,
        )
    else:
        raw = _run_chunks(
            dune,
            lambda chunk: lp_events_sql(index_addresses, chunk, window.query_since),
            restrict,
            "uniswap v3 index lp events",
            args.limit,
        )

    if raw.empty:
        return _empty(LP_COLUMNS)

    df = _attach_fid(raw, wallets, "lp_address")
    if df.empty:
        return _empty(LP_COLUMNS)

    # An index/index pool (none today, but the config can grow) is genuinely two
    # facts about the same event, so it becomes two rows rather than one guess.
    index_set = set(index_addresses)
    sides = []
    for column in ("token0_address", "token1_address"):
        side = df[df[column].isin(index_set)].copy()
        side["token_address"] = side[column]
        sides.append(side)
    df = pd.concat(sides, ignore_index=True) if sides else _empty(LP_COLUMNS)
    if df.empty:
        return _empty(LP_COLUMNS)

    df["chain_id"] = CHAIN_ARBITRUM
    df["block_time"] = _to_iso(df["block_time"])
    df = _collapse(
        df,
        keys=[
            "fid",
            "address",
            "pool_address",
            "token_address",
            "chain_id",
            "event",
            "tx_hash",
        ],
        sums=["amount0", "amount1"],
        first=["block_time"],
    )
    return df[LP_COLUMNS]


def _restriction(
    wallets: dict[str, int], args, leg: str
) -> list[list[str]] | None:
    """Address chunks to filter a cheap leg by, or None to pull it whole.

    'auto' restricts only when the wallet set is small enough that the extra
    executions are cheaper than downloading the unrestricted result — which is
    the cohort case, not the whole-Farcaster case.
    """
    if args.restrict == "never" or not wallets:
        return None
    chunks = list(chunked(sorted(wallets), max(1, int(args.holdings_chunk))))
    if args.restrict == "always":
        return chunks
    if len(chunks) <= int(args.max_holdings_chunks):
        return chunks
    logger.info(
        "%s: %d wallets would need %d executions; pulling the leg whole and "
        "intersecting locally instead",
        leg,
        len(wallets),
        len(chunks),
    )
    return None


# -- orchestration ---------------------------------------------------------


def _selected_legs(args) -> list[str]:
    parts = [p.strip().lower() for p in (args.parts or "").split(",") if p.strip()]
    skip = {p.strip().lower() for p in (args.skip or "").split(",") if p.strip()}
    unknown = (set(parts) | skip) - set(LEGS)
    if unknown:
        raise SystemExit(
            f"unknown leg(s) {sorted(unknown)}; choose from {', '.join(LEGS)}"
        )
    chosen = parts or list(LEGS)
    return [leg for leg in LEGS if leg in chosen and leg not in skip]


def run(window, args) -> dict:
    index_addresses = token_addresses()
    selected = _selected_legs(args)
    logger.info(
        "%s: %d index tokens, legs=%s", PIPELINE, len(index_addresses), ",".join(selected)
    )

    wallets = _load_wallet_map(args.wallets_run_id, allow_missing=args.dry_run)
    dune = DuneRunner(dry_run=args.dry_run)
    writer = RunWriter(DATA_TYPE, dry_run=args.dry_run)

    handlers = {
        "trades": lambda: leg_trades(dune, wallets, window, args, index_addresses),
        "holdings": lambda: leg_holdings(dune, wallets, window, args, index_addresses),
        "vaults": lambda: leg_vaults(dune, wallets, window, args),
        "lp": lambda: leg_lp(dune, wallets, window, args, index_addresses),
    }

    notes: list[str] = []
    degraded: list[str] = []
    results: dict[str, pd.DataFrame] = {}

    for leg in LEGS:
        name, columns = OUTPUTS[leg]
        if leg not in selected:
            logger.info("%s: leg %r not selected, no CSV written", PIPELINE, leg)
            notes.append(f"leg '{leg}' not run in this pass; {name}.csv absent")
            continue
        try:
            df = handlers[leg]()
        except (DuneError, SqlLiteralError, ValueError, KeyError) as exc:
            logger.warning("%s leg failed: %s", leg, exc, exc_info=True)
            notes.append(f"leg '{leg}' degraded to an empty {name}.csv: {exc}")
            degraded.append(leg)
            df = _empty(columns)
        results[leg] = df
        writer.write(name, df.reindex(columns=columns))

    watermark = _watermark(results)
    complete = set(selected) == set(LEGS) and not degraded

    params = {
        "legs": selected,
        "degraded": degraded,
        "index_tokens": index_addresses,
        "wallet_addresses": len(wallets),
        "min_trade_usd": args.min_trade_usd,
        "trade_chunk_days": args.trade_chunk_days,
        "holdings_chunk": args.holdings_chunk,
        "holdings_unrestricted": bool(args.holdings_unrestricted),
        "min_balance_raw": args.min_balance_raw,
        "restrict": args.restrict,
        "limit": args.limit,
        "dune": dune.summary(),
    }
    if not complete:
        notes.append(
            "watermark not advanced: a partial or degraded run must not let the "
            "next incremental skip a window it never read"
        )

    writer.finish(
        params=params,
        since=window.since,
        new_watermark=watermark if complete else None,
        notes=notes,
    )
    if complete and not args.dry_run:
        set_watermark(PIPELINE, watermark, run_ts=writer.run_ts)

    summary = {
        "run_ts": writer.run_ts,
        "rows": {OUTPUTS[k][0]: len(v) for k, v in results.items()},
        "degraded": degraded,
        # What was actually persisted, not what was computed — a degraded run
        # computes a watermark and then deliberately declines to store it.
        "watermark": watermark.isoformat() if (watermark and complete) else None,
    }
    logger.info("%s: %s", PIPELINE, summary)
    return summary


def _watermark(results: dict[str, pd.DataFrame]) -> datetime | None:
    """Newest event time across the event legs.

    holdings.csv is deliberately excluded: it is a state snapshot keyed on a
    balance, and its last_activity_at can sit far ahead of the newest trade in
    the window (a transfer is not a trade). Letting it drive the watermark would
    skip trades the next run has not seen.
    """
    best: datetime | None = None
    for leg, column in (("trades", "block_time"), ("vaults", "block_time"), ("lp", "block_time")):
        df = results.get(leg)
        if df is None or df.empty or column not in df.columns:
            continue
        for value in df[column]:
            parsed = parse_ts(value)
            if parsed and (best is None or parsed > best):
                best = parsed
    return best


def main(argv=None):
    parser = base_parser(PIPELINE, __doc__ or "")
    parser.formatter_class = argparse.RawDescriptionHelpFormatter
    parser.add_argument(
        "--parts",
        default=None,
        help=f"Comma-separated legs to run (default all): {', '.join(LEGS)}.",
    )
    parser.add_argument(
        "--skip",
        default=None,
        help="Comma-separated legs to skip, applied after --parts.",
    )
    parser.add_argument(
        "--min-trade-usd",
        type=float,
        default=MIN_BUY_USD,
        help=f"Drop dex.trades rows below this USD size (default {MIN_BUY_USD}). "
        "The main cost lever on the trades leg.",
    )
    parser.add_argument(
        "--trade-chunk-days",
        type=int,
        default=TRADE_CHUNK_DAYS,
        help=f"Days of dex.trades per Dune execution (default {TRADE_CHUNK_DAYS}).",
    )
    parser.add_argument(
        "--holdings-chunk",
        type=int,
        default=HOLDINGS_CHUNK,
        help=f"Wallet addresses per restricted query (default {HOLDINGS_CHUNK}).",
    )
    parser.add_argument(
        "--max-holdings-chunks",
        type=int,
        default=MAX_HOLDINGS_CHUNKS,
        help=f"Refuse the restricted holdings leg above this many executions "
        f"(default {MAX_HOLDINGS_CHUNKS}).",
    )
    parser.add_argument(
        "--min-balance-raw",
        type=int,
        default=0,
        help="Drop holdings below this raw (undivided) balance. 0 keeps every "
        "positive balance, including the wei-sized residue routers leave behind.",
    )
    parser.add_argument(
        "--holdings-unrestricted",
        action="store_true",
        help="Pull the whole holder aggregate in one query and intersect locally "
        "instead of chunking by wallet. Use when the wallet set is very large.",
    )
    parser.add_argument(
        "--restrict",
        choices=("auto", "always", "never"),
        default="auto",
        help="Wallet-set filtering for the cheap legs (vaults, lp). auto restricts "
        "only a small wallet set; always forces it; never pulls them whole.",
    )
    parser.add_argument(
        "--wallets-run-id",
        default=None,
        help="linked_wallets run to join against (default: the latest completed run).",
    )
    args = parser.parse_args(argv)

    setup_logging(args.log_level)
    window = resolve_window(args, PIPELINE)
    run(window, args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
