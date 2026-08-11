"""Bankr token registry on Robinhood Chain, plus per-token Uniswap v4 volume.

Produces:
  data/bankr_tokens/<run_ts>/tokens.csv        — the launch registry
  data/bankr_tokens/<run_ts>/token_volume.csv  — daily swaps/volume per token

Why two sources for one registry. The Bankr API returns exactly the 50 most
recent launches and honours no pagination parameter (verified — limit, offset,
page and cursor are all ignored). At the observed launch rate those 50 records
cover well under an hour; a token seen in one call has usually fallen out of the
next. So the API cannot be the registry — but it is the only place the
launcher's off-chain identity (`xUsername`) and the token's human name appear
before Dune indexes the chain, and Dune's indexing lag is real: at probe time
only 39 of the 47 most recent Robinhood launches had landed in
`robinhood.creation_traces`.

The pipeline therefore reads both and merges on `token_address`: Dune's
`robinhood.*` tables for history (source `dune_robinhood`), the API for the
freshest tail (source `bankr_api`). Dune wins on conflict because it is the
chain itself; the API fills in what Dune has not indexed yet.

The one thing worth knowing before reading the SQL: launches arrive as ERC-4337
user operations, so the transaction sender is a bundler, not the launcher. See
`sql/robinhood.py` for the evidence and the attribution rule. Getting this wrong
would collapse thousands of launchers onto a dozen bundler EOAs.

Two wallets per launch, not one. `deployer_address` is that UserOperation
sender — a smart account, which almost never carries a Farcaster verification.
`fee_recipient_address` is who the launch pays, decoded from the Doppler
beneficiary arrays on the chain side and read straight off `feeRecipient` on the
API side, and it is usually an EOA the launcher owns. Both are written, both are
resolved to fids, and the graph gives both the same edges under a `role`.
"""

from __future__ import annotations

import logging

import pandas as pd

from config.settings import CHAIN_ROBINHOOD
from lib.bankr import BankrClient, normalise_launch
from lib.cli import base_parser, resolve_window
from lib.dune import DuneError, DuneRunner
from lib.logging_utils import setup_logging
from lib.runs import RunWriter
from lib.state import max_timestamp, set_watermark
from lib.wallet_fids import distinct_addresses, resolve_wallet_fids
from sql import robinhood as rh_sql

logger = logging.getLogger(__name__)

PIPELINE = "bankr_tokens"
DATA_TYPE = "bankr_tokens"

TOKEN_COLUMNS = [
    "token_address",
    "chain_id",
    "platform",
    "deployer_address",
    "fee_recipient_address",
    "fid",
    "fee_recipient_fid",
    "name",
    "symbol",
    "deployed_at",
    "tx_hash",
    "pool_address",
    "launch_type",
    "source",
]

VOLUME_COLUMNS = [
    "token_address",
    "chain_id",
    "day",
    "swap_count",
    "volume_native",
    "volume_usd",
]


# Dune's CSV result endpoint renders SQL NULL as the literal string "<nil>",
# which pandas happily reads as data. Left alone it turns every nullable column
# into an object column of sentinel strings — notna() lies, numeric columns stop
# being numeric, and "<nil>" lands in the graph as an address.
DUNE_NULL = "<nil>"


def _empty(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def clean_dune_frame(df: pd.DataFrame, numeric: tuple[str, ...] = ()) -> pd.DataFrame:
    """Coerce the numeric columns, and belt-and-braces the "<nil>" sentinel.

    `lib.dune` now parses Dune's NULL sentinel out at the CSV boundary, so the
    replace below is normally a no-op; it stays because this function is also
    handed frames from the result cache written before that fix.
    """
    if df.empty:
        return df
    df = df.replace(DUNE_NULL, None)
    for column in numeric:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def fetch_api_launches() -> pd.DataFrame:
    """The 50 most recent launches, filtered to Robinhood Chain.

    Never allowed to fail the run: this is a freshness top-up on top of a
    chain-derived registry, so an API outage costs us the newest hour, not the
    pipeline.
    """
    try:
        raw = BankrClient().recent_launches()
    except Exception as exc:  # noqa: BLE001 - degrade, never crash the run
        logger.warning("bankr API unavailable (%s); continuing with Dune only", exc)
        return _empty(TOKEN_COLUMNS)

    rows = []
    for record in raw:
        launch = normalise_launch(record)
        if launch["chain_id"] != CHAIN_ROBINHOOD or not launch["token_address"]:
            continue
        rows.append(
            {
                "token_address": launch["token_address"],
                "chain_id": CHAIN_ROBINHOOD,
                "platform": "bankr",
                "deployer_address": launch["deployer_address"],
                "fee_recipient_address": launch["fee_recipient_address"],
                "fid": None,
                "fee_recipient_fid": None,
                "name": launch["name"],
                "symbol": launch["symbol"],
                "deployed_at": launch["deployed_at"],
                "tx_hash": launch["tx_hash"],
                "pool_address": launch["pool_address"],
                "launch_type": launch["launch_type"],
                "source": "bankr_api",
            }
        )
    logger.info("bankr API: %d Robinhood launches of %d returned", len(rows), len(raw))
    return pd.DataFrame(rows, columns=TOKEN_COLUMNS)


def fetch_dune_registry(
    dune: DuneRunner, window, limit: int | None, notes: list[str]
) -> pd.DataFrame:
    """The historical registry from robinhood.creation_traces."""
    sql = rh_sql.tokens_by_factory_sql(rh_sql.DEFAULT_FACTORIES, window.query_since)
    try:
        df = dune.run_sql(sql, label="bankr registry", limit=limit)
    except DuneError as exc:
        logger.warning(
            "bankr registry query failed (%s); falling back to the API's 50 launches",
            exc,
        )
        notes.append(f"dune registry query failed, API-only run: {str(exc)[:200]}")
        return _empty(TOKEN_COLUMNS)

    if df.empty:
        logger.warning("bankr registry query returned no rows")
        return _empty(TOKEN_COLUMNS)

    df = clean_dune_frame(df)
    by_userop = 0
    if "deployer_source" in df.columns:
        by_userop = int((df["deployer_source"] == "erc4337_userop").sum())
        notes.append(
            f"{by_userop}/{len(df)} deployers resolved from the ERC-4337 "
            f"UserOperation sender; the rest fell back to the transaction sender"
        )
    if "fee_recipient_source" in df.columns:
        by_source = df["fee_recipient_source"].value_counts().to_dict()
        notes.append(
            f"{int(df['fee_recipient_address'].notna().sum())}/{len(df)} tokens "
            f"carry a fee recipient decoded from the Doppler beneficiary arrays "
            f"({by_source}); see sql/robinhood.py for the decode and its validation"
        )

    out = pd.DataFrame(
        {
            "token_address": df["token_address"],
            "chain_id": CHAIN_ROBINHOOD,
            "platform": "bankr",
            "deployer_address": df["deployer_address"],
            "fee_recipient_address": df.get("fee_recipient_address"),
            "fid": None,
            "fee_recipient_fid": None,
            "name": None,
            "symbol": None,
            "deployed_at": df["deployed_at"],
            "tx_hash": df["tx_hash"],
            "pool_address": None,
            "launch_type": "doppler",
            "source": "dune_robinhood",
        },
        columns=TOKEN_COLUMNS,
    )
    logger.info("dune registry: %d tokens (%d via userop)", len(out), by_userop)
    return out


def merge_registries(dune_df: pd.DataFrame, api_df: pd.DataFrame) -> pd.DataFrame:
    """Union the two sources, preferring the chain but keeping API metadata.

    Dune knows the deployer correctly; the API knows the token's name, symbol
    and pool id. For a token both sources saw, we want the union of those facts,
    not whichever row happened to sort first.

    `fee_recipient_address` is filled the same way. Both sources have it, and
    they agree — the Dune decode was validated against the API's own labels —
    so the API only ever fills a token the chain query has not indexed yet.
    """
    # Both sides index by token_address below, and a duplicate on either side
    # would make the aligned .loc assignment blow up rather than merge.
    dune_df = dune_df.drop_duplicates(subset=["token_address"], keep="first")
    api_df = api_df.drop_duplicates(subset=["token_address"], keep="first")

    if dune_df.empty and api_df.empty:
        return _empty(TOKEN_COLUMNS)
    if dune_df.empty:
        return api_df.reset_index(drop=True)
    if api_df.empty:
        return dune_df.reset_index(drop=True)

    api_indexed = api_df.set_index("token_address")
    merged = dune_df.set_index("token_address").copy()

    overlap = merged.index.intersection(api_indexed.index)
    for column in ("name", "symbol", "pool_address", "launch_type", "fee_recipient_address"):
        if len(overlap):
            merged.loc[overlap, column] = merged.loc[overlap, column].fillna(
                api_indexed.loc[overlap, column]
            )

    api_only = api_indexed.loc[api_indexed.index.difference(merged.index)]
    out = pd.concat([merged, api_only])
    out = out.reset_index().rename(columns={"index": "token_address"})
    return out[TOKEN_COLUMNS]


def attach_fids(
    tokens: pd.DataFrame, notes: list[str], dry_run: bool, limit: int | None = None
) -> pd.DataFrame:
    """Map both launch wallets — deployer and fee recipient — to fids.

    Resolution is local-first and shared with `clanker_tokens`; see
    `lib.wallet_fids`. Only the distinct union of the two columns is looked up,
    which at 67k tokens is roughly 15k deployers plus a comparable number of fee
    recipients rather than 134k lookups.

    The two columns have very different prospects, which is the point of
    carrying both. A deployer here is an ERC-4337 smart account, and a Farcaster
    verification points at a user's own EOA far more often than at a contract
    wallet — hardly any of them resolve. The fee recipient is usually that EOA.

    `limit` doubles as the ceiling on the Neynar top-up, so `--limit 50` is a
    smoke test that costs one API call rather than a few hundred.
    """
    if tokens.empty:
        return tokens
    addresses = distinct_addresses(
        tokens.get("deployer_address"), tokens.get("fee_recipient_address")
    )
    if not addresses:
        return tokens
    if dry_run:
        logger.info("[dry-run] would resolve %d launch wallet addresses", len(addresses))
        return tokens

    mapping = resolve_wallet_fids(
        addresses, notes, what="bankr launch wallets", max_neynar=limit
    )

    tokens = tokens.copy()
    for column, target in (
        ("deployer_address", "fid"),
        ("fee_recipient_address", "fee_recipient_fid"),
    ):
        tokens[target] = tokens[column].map(
            lambda a: mapping.get(str(a).lower()) if isinstance(a, str) else None
        )
    notes.append(
        f"{len(mapping)} of {len(addresses)} distinct launch wallets mapped to a fid: "
        f"{int(tokens['fid'].notna().sum())} token rows via the deployer, "
        f"{int(tokens['fee_recipient_fid'].notna().sum())} via the fee recipient. "
        f"Deployers are ERC-4337 smart accounts, which are rarely the address a "
        f"Farcaster user verifies; fee recipients usually are"
    )
    return tokens


def fetch_volume(
    dune: DuneRunner, window, limit: int | None, notes: list[str]
) -> pd.DataFrame:
    """Daily swap counts and volume per token, via the v4 Initialize->Swap map."""
    sql = rh_sql.token_volume_sql(None, window.query_since)
    try:
        df = dune.run_sql(sql, label="bankr token volume", limit=limit)
    except DuneError as exc:
        logger.warning("token volume query failed (%s); writing an empty file", exc)
        notes.append(f"token_volume unavailable, wrote empty file: {str(exc)[:200]}")
        return _empty(VOLUME_COLUMNS)

    if df.empty:
        notes.append("token_volume query returned no rows for this window")
        return _empty(VOLUME_COLUMNS)

    df = clean_dune_frame(df, numeric=("swap_count", "volume_native", "volume_usd"))
    df["chain_id"] = CHAIN_ROBINHOOD
    for column in VOLUME_COLUMNS:
        if column not in df.columns:
            df[column] = None
    priced = int(df["volume_usd"].notna().sum())
    notes.append(
        f"{priced}/{len(df)} token-days carry a USD volume; the rest traded only "
        f"against numeraires prices.usd does not cover on chain {CHAIN_ROBINHOOD}. "
        f"swap_count covers every swap; both volume columns cover priced pools only"
    )
    return df[VOLUME_COLUMNS]


def run(window, args) -> dict:
    writer = RunWriter(DATA_TYPE, dry_run=args.dry_run)
    dune = DuneRunner(dry_run=args.dry_run)
    notes: list[str] = [
        "registry factory is the Doppler token factory on Robinhood Chain "
        f"({rh_sql.BANKR_TOKEN_FACTORY}); Bankr is its dominant consumer but "
        "rows sourced from Dune cannot be proven Bankr-exclusive. Rows with "
        "source='bankr_api' are Bankr-attributed with certainty.",
        "deployer_address is the ERC-4337 UserOperation sender (a smart "
        "account), not the transaction sender, which on this chain is a bundler.",
        "fee_recipient_address is the launcher's entry in the Doppler fee split, "
        "decoded from the initializer's and the fee locker's beneficiary arrays "
        "and validated against the Bankr API's own labels. It is the wallet most "
        "likely to be the launcher's own EOA, so it carries the attribution the "
        "smart-account deployer cannot.",
    ]

    api_df = fetch_api_launches()
    dune_df = fetch_dune_registry(dune, window, args.limit, notes)

    if args.dry_run:
        # DuneRunner returns an empty frame in dry-run, so the merge below would
        # be exercised against nothing useful; report the plan instead.
        logger.info(
            "[dry-run] would merge %d API launches with the Dune registry", len(api_df)
        )

    tokens = merge_registries(dune_df, api_df)
    tokens = attach_fids(tokens, notes, args.dry_run, limit=args.limit)
    if not tokens.empty:
        tokens = tokens.sort_values("deployed_at", ascending=False).drop_duplicates(
            subset=["token_address"], keep="first"
        )
    tokens = tokens.reset_index(drop=True)

    volume = fetch_volume(dune, window, args.limit, notes)

    writer.write("tokens", tokens)
    writer.write("token_volume", volume)

    watermark = max_timestamp(tokens["deployed_at"]) if not tokens.empty else None
    source_mix = (
        tokens["source"].value_counts().to_dict() if not tokens.empty else {}
    )

    writer.finish(
        params={
            "chain_id": CHAIN_ROBINHOOD,
            "factories": list(rh_sql.DEFAULT_FACTORIES),
            "pool_manager": rh_sql.UNISWAP_V4_POOL_MANAGER,
            "limit": args.limit,
            "is_backfill": window.is_backfill,
        },
        since=window.since,
        new_watermark=watermark,
        notes=notes,
    )
    if not args.dry_run:
        set_watermark(PIPELINE, watermark, run_ts=writer.run_ts)

    return {
        "tokens": len(tokens),
        "sources": source_mix,
        "volume_rows": len(volume),
        "run_ts": writer.run_ts,
    }


def main(argv=None) -> int:
    parser = base_parser(PIPELINE, __doc__.split("\n")[0])
    args = parser.parse_args(argv)
    setup_logging(args.log_level)
    window = resolve_window(args, PIPELINE)
    result = run(window, args)
    logger.info("%s: %s", PIPELINE, result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
