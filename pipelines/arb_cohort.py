"""The deduplicated Arbitrum wallet cohort, assembled from every upstream run.

WHAT it produces
    arb_cohort/cohort.csv — one row per EVM wallet that any Arbitrum-facing
    pipeline has touched: address, the Farcaster identity behind it (fid,
    neynar_score), every pipeline that saw it (`sources`, pipe-delimited) and the
    `priority` of the most specific of those pipelines.

WHY this shape
    The downstream crawls are priced per wallet, not per query: Hyperliquid costs
    two /info calls each and paces at ~24 wallets/min, so a 10k cohort is a
    seven-hour job. A crawl driven off an unordered wallet dump wastes its first
    hours on the least interesting addresses. Ordering the cohort
    smallest-source-first means the operator can stop the crawl at any point — or
    cap it with --max-priority — and still have covered the wallets the grant is
    actually about.

    "Smallest source first" is literal. contract deployers are a few thousand
    wallets and every one of them is a builder; popular-token traders are hundreds
    of thousands and most are noise. A wallet's priority is therefore the LOWEST
    (most specific) source it appears in, while `sources` keeps the full list so
    the graph can still answer "which pipelines saw this wallet".

WHY it costs nothing
    Pure local aggregation over CSVs already on disk. No Dune credits, no Neynar
    calls. It is fully derived from upstream runs, so it keeps no watermark and is
    simply regenerated whenever an upstream pipeline produces a newer run.

Missing upstream runs are a warning, not an error: the cohort is useful with a
subset of its sources, and the manifest records exactly which runs fed it.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from config.settings import DEFAULT_MIN_USER_SCORE
from lib import runs
from lib.cli import base_parser
from lib.logging_utils import setup_logging
from lib.runs import RunWriter

logger = logging.getLogger(__name__)

PIPELINE = "arb_cohort"

# Ingestion reads these exact names in this exact order.
COLUMNS = ["address", "fid", "sources", "priority", "neynar_score"]

# Hyperliquid, Dune and the Neo4j Wallet key are all EVM-only. Neynar hands back
# Solana verifications in the same wallet list, so everything is filtered through
# this before it can enter the cohort — otherwise the HL crawl burns two calls per
# base58 address that can never match.
EVM_ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")


@dataclass(frozen=True)
class SourceSpec:
    """One (csv, column) pair that contributes wallets to the cohort."""

    name: str  # what lands in the `sources` column
    priority: int
    data_type: str
    csv_name: str
    address_col: str
    fid_col: str | None


# Ordered by priority: 1 is the smallest, most specific wallet set.
# Several pipelines expose the same wallets through two files (the event file and
# its activity rollup); both are read under one source name so a run that produced
# only one of them still contributes.
SOURCES: tuple[SourceSpec, ...] = (
    SourceSpec("contract_deployers", 1, "contract_deployers", "deployments", "deployer_address", "fid"),
    SourceSpec("contract_deployers", 1, "contract_deployers", "deployer_activity", "address", "fid"),
    SourceSpec("miniapp_builders_activity", 2, "miniapp_builders_activity", "builder_wallets", "address", "fid"),
    SourceSpec("miniapp_builders_activity", 2, "miniapp_builders_activity", "builder_activity", "address", "fid"),
    SourceSpec("clanker_tokens", 3, "clanker_tokens", "tokens", "deployer_address", "fid"),
    SourceSpec("clanker_tokens", 3, "clanker_tokens", "tokens", "admin_address", "fee_recipient_fid"),
    SourceSpec("bankr_tokens", 3, "bankr_tokens", "tokens", "deployer_address", "fid"),
    # The fee recipient is a token's beneficiary and is treated as a creator
    # everywhere else in the graph, so it belongs in the cohort on the same
    # footing. It matters most on Bankr, where the deployer is an ERC-4337 smart
    # account and the fee recipient is the wallet a human actually controls.
    SourceSpec("bankr_tokens", 3, "bankr_tokens", "tokens", "fee_recipient_address", "fee_recipient_fid"),
    SourceSpec("token_evangelists", 4, "token_evangelists", "attributions", "buyer_address", "buyer_fid"),
    SourceSpec("token_buyers", 5, "token_buyers", "buys", "buyer_address", "fid"),
    SourceSpec("popular_tokens", 6, "popular_tokens", "trades", "address", "fid"),
    SourceSpec("popular_tokens", 6, "popular_tokens", "holdings", "address", "fid"),
    SourceSpec("popular_tokens", 6, "popular_tokens", "vault_deposits", "address", "fid"),
    SourceSpec("popular_tokens", 6, "popular_tokens", "lp_events", "address", "fid"),
)

# Evangelist *authors* only ever appear as a fid (attributions.csv carries the
# buyer's address but not the author's), so they are expanded to wallets through
# the linked_wallets map instead of being read as an address column.
EVANGELIST_AUTHOR_PRIORITY = 4
EVANGELIST_SOURCE = "token_evangelists"

EMPTY_RECORDS = pd.DataFrame(
    {
        "address": pd.Series(dtype="string"),
        "fid": pd.Series(dtype="Int64"),
        "source": pd.Series(dtype="string"),
        "priority": pd.Series(dtype="int64"),
    }
)


# --- loading -------------------------------------------------------------


class UpstreamLoader:
    """Reads the latest completed run of each upstream data type, once."""

    def __init__(self, base_dir: Path | None = None):
        self.base_dir = base_dir
        self.run_ids: dict[str, str | None] = {}
        self.notes: list[str] = []

    def run_id(self, data_type: str) -> str | None:
        if data_type not in self.run_ids:
            run_dir = runs.latest_run(data_type, self.base_dir)
            if run_dir is None:
                self.run_ids[data_type] = None
                message = f"no completed run for '{data_type}' — skipped"
                logger.warning("%s", message)
                self.notes.append(message)
            else:
                self.run_ids[data_type] = run_dir.name
                logger.info("using %s run %s", data_type, run_dir.name)
        return self.run_ids[data_type]

    def read(self, data_type: str, csv_name: str) -> pd.DataFrame:
        run_id = self.run_id(data_type)
        if run_id is None:
            return pd.DataFrame()
        try:
            df = runs.read_csv(
                data_type, csv_name, run_id=run_id, base_dir=self.base_dir, required=False
            )
        except pd.errors.EmptyDataError:
            # A zero-byte CSV (killed mid-write before the header) is not fatal.
            logger.warning("%s/%s/%s.csv is empty", data_type, run_id, csv_name)
            return pd.DataFrame()
        if df.empty:
            logger.warning(
                "%s/%s.csv absent or empty in run %s — that source contributes nothing",
                data_type,
                csv_name,
                run_id,
            )
        return df


# --- normalisation -------------------------------------------------------


def normalise_address(value) -> str | None:
    """Lowercase an EVM address, or None if it is not one."""
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text or text in {"nan", "none", "<na>"}:
        return None
    if not text.startswith("0x"):
        text = f"0x{text}"
    return text if EVM_ADDRESS_RE.match(text) else None


def to_fid(series: pd.Series) -> pd.Series:
    """Coerce a fid column to nullable Int64 so CSVs never carry '123.0'."""
    return pd.to_numeric(series, errors="coerce").astype("Float64").round().astype("Int64")


def extract(df: pd.DataFrame, spec: SourceSpec) -> pd.DataFrame:
    """Pull (address, fid) pairs out of one upstream CSV."""
    if df.empty or spec.address_col not in df.columns:
        return EMPTY_RECORDS
    addresses = df[spec.address_col].map(normalise_address).astype("string")
    if spec.fid_col and spec.fid_col in df.columns:
        fids = to_fid(df[spec.fid_col])
    else:
        fids = pd.Series(pd.NA, index=df.index, dtype="Int64")
    out = pd.DataFrame({"address": addresses, "fid": fids})
    out = out[out["address"].notna()]
    if out.empty:
        return EMPTY_RECORDS
    # One row per (address, fid) is all the aggregation needs; trade tables can
    # otherwise carry millions of duplicates of the same wallet.
    out = out.drop_duplicates(subset=["address", "fid"])
    out["source"] = pd.Series(spec.name, index=out.index, dtype="string")
    out["priority"] = spec.priority
    return out


# --- linked_wallets join -------------------------------------------------


def load_identity(loader: UpstreamLoader) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(address -> fid) and (fid -> neynar_score) from the linked_wallets run.

    linked_wallets is deliberately NOT a cohort source: every Farcaster user with
    a verified wallet is in it, which is millions of addresses and none of the
    Arbitrum specificity the cohort exists to provide. It is only the join table.
    """
    accounts = loader.read("linked_wallets", "accounts")
    if not accounts.empty and "fid" in accounts.columns:
        profiles = pd.DataFrame({"fid": to_fid(accounts["fid"])})
        profiles["neynar_score"] = (
            pd.to_numeric(accounts["neynar_score"], errors="coerce")
            if "neynar_score" in accounts.columns
            else pd.Series(float("nan"), index=accounts.index, dtype="float64")
        )
        profiles = profiles[profiles["fid"].notna()]
        profiles = profiles.sort_values("neynar_score", ascending=False, na_position="last")
        profiles = profiles.drop_duplicates(subset=["fid"], keep="first")
    else:
        profiles = pd.DataFrame({"fid": pd.Series(dtype="Int64"), "neynar_score": pd.Series(dtype="float64")})

    wallets = loader.read("linked_wallets", "wallets")
    if wallets.empty or "address" not in wallets.columns or "fid" not in wallets.columns:
        wallet_map = pd.DataFrame(
            {"address": pd.Series(dtype="string"), "fid": pd.Series(dtype="Int64")}
        )
        return wallet_map, profiles

    wallet_map = pd.DataFrame(
        {
            "address": wallets["address"].map(normalise_address).astype("string"),
            "fid": to_fid(wallets["fid"]),
        }
    )
    if "is_primary" in wallets.columns:
        wallet_map["is_primary"] = (
            wallets["is_primary"].astype("string").str.lower().isin(["true", "1", "1.0", "yes"])
        )
    else:
        wallet_map["is_primary"] = False
    wallet_map = wallet_map[wallet_map["address"].notna() & wallet_map["fid"].notna()]
    # An address can legitimately map to more than one fid (custody wallets get
    # reused). Resolve it deterministically: primary verification first, then the
    # higher-reputation account, then the older fid.
    wallet_map = wallet_map.merge(profiles, on="fid", how="left")
    wallet_map = wallet_map.sort_values(
        ["is_primary", "neynar_score", "fid"],
        ascending=[False, False, True],
        na_position="last",
    )
    return wallet_map, profiles


def evangelist_author_wallets(
    loader: UpstreamLoader, wallet_map: pd.DataFrame
) -> pd.DataFrame:
    """Wallets of the accounts credited with driving token buys."""
    attributions = loader.read("token_evangelists", "attributions")
    if attributions.empty or "author_fid" not in attributions.columns:
        return EMPTY_RECORDS
    author_fids = to_fid(attributions["author_fid"]).dropna().unique()
    if len(author_fids) == 0 or wallet_map.empty:
        return EMPTY_RECORDS
    matched = wallet_map[wallet_map["fid"].isin(list(author_fids))]
    if matched.empty:
        return EMPTY_RECORDS
    out = matched[["address", "fid"]].drop_duplicates()
    out["source"] = pd.Series(EVANGELIST_SOURCE, index=out.index, dtype="string")
    out["priority"] = EVANGELIST_AUTHOR_PRIORITY
    logger.info(
        "token_evangelists: %d author wallets resolved from %d author fids",
        len(out),
        len(author_fids),
    )
    return out


# --- aggregation ---------------------------------------------------------


def aggregate(records: pd.DataFrame, wallet_map: pd.DataFrame, profiles: pd.DataFrame) -> pd.DataFrame:
    """Collapse per-source rows into one row per wallet."""
    if records.empty:
        return pd.DataFrame(columns=COLUMNS)

    # Sorting by priority before the groupby makes both aggregates deterministic:
    # `sources` reads most-specific-first, and the fid we keep is the one the most
    # specific pipeline reported.
    records = records.sort_values(["priority", "source"], kind="stable")
    grouped = records.groupby("address", sort=False).agg(
        priority=("priority", "min"),
        sources=("source", lambda s: "|".join(dict.fromkeys(s))),
        source_fid=("fid", lambda s: next((v for v in s if pd.notna(v)), pd.NA)),
    )
    cohort = grouped.reset_index()
    cohort["source_fid"] = cohort["source_fid"].astype("Int64")

    if not wallet_map.empty:
        lookup = wallet_map.drop_duplicates(subset=["address"], keep="first")[["address", "fid"]]
        lookup = lookup.rename(columns={"fid": "linked_fid"})
        cohort = cohort.merge(lookup, on="address", how="left")
    else:
        cohort["linked_fid"] = pd.Series(pd.NA, index=cohort.index, dtype="Int64")

    # Upstream pipelines derive their fids from the same linked_wallets snapshot,
    # so these agree in practice; the source fid wins because it is the identity
    # that actually produced the edge, and the map only fills the gaps.
    cohort["fid"] = cohort["source_fid"].fillna(to_fid(cohort["linked_fid"]))

    if not profiles.empty:
        cohort = cohort.merge(profiles, on="fid", how="left")
    if "neynar_score" not in cohort.columns:
        # Nullable Float64, not float64: pd.NA cannot fill a numpy float array,
        # and the sort below relies on na_position to park unscored wallets last.
        cohort["neynar_score"] = pd.Series(pd.NA, index=cohort.index, dtype="Float64")

    cohort = cohort.sort_values(
        ["priority", "neynar_score", "address"],
        ascending=[True, False, True],
        na_position="last",
    )
    return cohort[COLUMNS].reset_index(drop=True)


def apply_filters(cohort: pd.DataFrame, min_score: float | None, max_priority: int | None,
                  limit: int | None) -> tuple[pd.DataFrame, list[str]]:
    notes: list[str] = []
    if max_priority is not None:
        before = len(cohort)
        cohort = cohort[cohort["priority"] <= max_priority]
        notes.append(f"--max-priority {max_priority} kept {len(cohort)}/{before} wallets")
    if min_score is not None:
        before = len(cohort)
        scored = pd.to_numeric(cohort["neynar_score"], errors="coerce")
        # An unscored wallet has no Farcaster account behind it (or the account
        # was missing from the linked_wallets run). A reputation gate cannot be
        # satisfied by an unknown, so those drop out rather than pass silently.
        cohort = cohort[scored >= min_score]
        notes.append(f"--min-score {min_score} kept {len(cohort)}/{before} wallets")
    if limit is not None and limit < len(cohort):
        notes.append(f"--limit {limit} truncated the cohort from {len(cohort)} wallets")
        cohort = cohort.head(limit)
    for note in notes:
        logger.info("%s", note)
    return cohort.reset_index(drop=True), notes


# --- run -----------------------------------------------------------------


def run(args) -> dict:
    """Build the cohort. No time window: it is fully derived from upstream runs."""
    loader = UpstreamLoader()
    wallet_map, profiles = load_identity(loader)
    if wallet_map.empty:
        logger.warning(
            "linked_wallets has no usable wallets.csv — cohort fids come only from "
            "the upstream CSVs and neynar_score will be mostly empty"
        )

    frames: list[pd.DataFrame] = []
    per_source: dict[str, int] = {}
    for spec in SOURCES:
        extracted = extract(loader.read(spec.data_type, spec.csv_name), spec)
        if extracted.empty:
            continue
        frames.append(extracted)
        per_source[spec.name] = per_source.get(spec.name, 0) + len(extracted)
        logger.info(
            "%s/%s.csv[%s] -> %d wallets (priority %d)",
            spec.data_type,
            spec.csv_name,
            spec.address_col,
            len(extracted),
            spec.priority,
        )

    authors = evangelist_author_wallets(loader, wallet_map)
    if not authors.empty:
        frames.append(authors)
        per_source[EVANGELIST_SOURCE] = per_source.get(EVANGELIST_SOURCE, 0) + len(authors)

    records = pd.concat(frames, ignore_index=True) if frames else EMPTY_RECORDS
    cohort = aggregate(records, wallet_map, profiles)
    if cohort.empty:
        loader.notes.append("no upstream wallets found — cohort.csv is header-only")
        logger.warning("cohort is empty; downstream crawls have nothing to do")

    cohort, filter_notes = apply_filters(
        cohort, args.min_score, args.max_priority, args.limit
    )

    by_priority = (
        cohort["priority"].value_counts().sort_index().to_dict() if not cohort.empty else {}
    )
    with_fid = int(cohort["fid"].notna().sum()) if not cohort.empty else 0
    logger.info(
        "cohort: %d wallets, %d with a fid, by priority %s",
        len(cohort),
        with_fid,
        {int(k): int(v) for k, v in by_priority.items()},
    )

    writer = RunWriter(PIPELINE, dry_run=args.dry_run)
    writer.write("cohort", cohort)
    writer.finish(
        params={
            "min_score": args.min_score,
            "max_priority": args.max_priority,
            "limit": args.limit,
            "upstream_runs": loader.run_ids,
            "wallets_per_source": per_source,
            "wallets_by_priority": {int(k): int(v) for k, v in by_priority.items()},
            "wallets_with_fid": with_fid,
        },
        since=None,  # fully derived: no time window and no watermark to advance
        new_watermark=None,
        notes=loader.notes + filter_notes,
    )
    return {
        "run_ts": writer.run_ts,
        "wallets": len(cohort),
        "with_fid": with_fid,
        "by_priority": {int(k): int(v) for k, v in by_priority.items()},
    }


def main(argv=None) -> int:
    parser = base_parser(
        PIPELINE,
        "Assemble the deduplicated Arbitrum wallet cohort from upstream pipeline runs. "
        "Pure local aggregation: no Dune credits, no API calls.",
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=None,
        help=(
            "Drop wallets whose Farcaster account scores below this Neynar 0-1 score "
            f"(wallets with no score are dropped too). The project-wide reputation "
            f"gate is DEFAULT_MIN_USER_SCORE={DEFAULT_MIN_USER_SCORE}; default here is "
            "no filter, so the full cohort is written and consumers gate as they like."
        ),
    )
    parser.add_argument(
        "--max-priority",
        type=int,
        default=None,
        help=(
            "Keep only wallets found by a source at least this specific: "
            "1 contract_deployers, 2 miniapp_builders_activity, 3 clanker/bankr token "
            "creators, 4 token_evangelists, 5 token_buyers, 6 popular_tokens."
        ),
    )
    args = parser.parse_args(argv)
    setup_logging(args.log_level)
    if args.since or args.backfill:
        logger.info(
            "--since/--backfill are ignored: the cohort is derived from the latest "
            "completed upstream runs, not from a time window"
        )
    summary = run(args)
    logger.info("%s done: %s", PIPELINE, summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
