"""Clanker token registry for Arbitrum.

Produces `data/clanker_tokens/<run_ts>/tokens.csv`: every Clanker-deployed token
on Arbitrum (chain 42161) with its deployer wallet, its admin, and — where
Clanker recorded one — the Farcaster fid of the account that requested the
launch.

The admin is not decoration. On Clanker v4 it owns the token's fees and rewards,
which makes it the real beneficiary when the deploying address is a bot or a
smart account: 140 of the 565 Arbitrum tokens have an admin that is not their
deployer, and 70 of those admin addresses are no token's deployer at all. So the
admin gets its own fid lookup (`fee_recipient_fid`) and, in the graph, the same
edges the deployer gets under `role: 'admin'`.

Why the Clanker API and not Dune: the token->fid edge only exists off-chain.
On-chain we can see the factory and the `msg.sender`, but the Farcaster identity
of the person who typed "@clanker deploy <ticker>" is metadata Clanker alone
holds, and Dune carries no Farcaster data whatsoever (verified — the
`dune.neynar.dataset_farcaster_*` tables do not exist for this account). Roughly
61% of Arbitrum tokens carry a fid; the rest were deployed straight from a
contract or a wallet with no Farcaster requestor, so `fid` is nullable and the
`linked_wallets` wallet->fid join is what covers those.

Cost: 565 tokens at 20 per page is ~29 unauthenticated requests for a full
backfill, so the incremental path is an optimisation rather than a necessity.
The feed is sorted newest-first, so an incremental run walks pages until it is
comfortably past the window start and then stops.
"""

from __future__ import annotations

import logging

import pandas as pd

from config.settings import CHAIN_ARBITRUM
from lib.clanker import PAGE_SIZE, ClankerClient, normalise_token
from lib.cli import base_parser, resolve_window
from lib.logging_utils import setup_logging
from lib.runs import RunWriter
from lib.state import max_timestamp, parse_ts, set_watermark
from lib.wallet_fids import distinct_addresses, resolve_wallet_fids

logger = logging.getLogger(__name__)

PIPELINE = "clanker_tokens"
DATA_TYPE = "clanker_tokens"

TOKEN_COLUMNS = [
    "token_address",
    "chain_id",
    "platform",
    "deployer_address",
    "admin_address",
    "fid",
    "fee_recipient_fid",
    "username",
    "name",
    "symbol",
    "deployed_at",
    "tx_hash",
    "pool_address",
    "paired_token",
    "token_type",
    "starting_market_cap_usd",
    "price_usd",
    "market_cap_usd",
    "volume_24h_usd",
]


def collect_tokens(
    client: ClankerClient,
    query_since,
    max_pages: int | None = None,
    stop_early: bool = True,
) -> tuple[list[dict], int]:
    """Walk the newest-first feed, stopping once it is clearly past `query_since`.

    The cursor is timestamp-based, so tokens deployed in the same second can
    straddle a page boundary and arrive slightly out of order. Stopping on the
    first row older than the window would risk truncating one of those; instead
    we require a full page's worth of consecutive stale rows before breaking.
    Returns the kept rows and the number of rows examined.
    """
    kept: list[dict] = []
    examined = 0
    stale_streak = 0
    for raw in client.iter_tokens(CHAIN_ARBITRUM, max_pages=max_pages):
        token = normalise_token(raw, CHAIN_ARBITRUM)
        examined += 1
        if not token["token_address"]:
            logger.debug("clanker row with no contract_address, skipping")
            continue
        deployed_at = parse_ts(token["deployed_at"])
        if stop_early and deployed_at is not None and deployed_at < query_since:
            stale_streak += 1
            if stale_streak >= PAGE_SIZE:
                logger.info(
                    "clanker: %d consecutive tokens older than %s, stopping walk",
                    stale_streak,
                    query_since.isoformat(),
                )
                break
            continue
        # An unparseable deploy time cannot be judged against the window, so
        # keep it rather than silently dropping a token from the registry.
        stale_streak = 0
        kept.append(token)
    return kept, examined


def attach_admin_fids(
    tokens: pd.DataFrame, notes: list[str], dry_run: bool, limit: int | None = None
) -> pd.DataFrame:
    """Resolve `admin_address` to `fee_recipient_fid`, local map first.

    `fid` is left alone: Clanker records the Farcaster account that ordered the
    launch, and that is a stronger fact than anything a wallet lookup could
    infer. This adds the other end — on Clanker v4 the admin is the token's fee
    and reward owner, and 70 of the Arbitrum admins are addresses that appear as
    no token's deployer, so without this column the graph cannot see them at all.

    Same degradation contract as everywhere else: a missing `linked_wallets` run
    or a Neynar outage costs the column, never the run.
    """
    if tokens.empty:
        return tokens
    addresses = distinct_addresses(tokens.get("admin_address"))
    if not addresses:
        return tokens
    if dry_run:
        logger.info("[dry-run] would resolve %d admin addresses", len(addresses))
        return tokens

    mapping = resolve_wallet_fids(
        addresses, notes, what="clanker admins", max_neynar=limit
    )
    tokens = tokens.copy()
    tokens["fee_recipient_fid"] = tokens["admin_address"].map(
        lambda a: mapping.get(str(a).lower()) if isinstance(a, str) else None
    )
    notes.append(
        f"{len(mapping)} of {len(addresses)} admin (fee owner) wallets mapped to a "
        f"fid, covering {int(tokens['fee_recipient_fid'].notna().sum())} token rows"
    )
    return tokens


def run(window, args) -> dict:
    writer = RunWriter(DATA_TYPE, dry_run=args.dry_run)
    client = ClankerClient()

    max_pages = args.limit
    if args.dry_run and max_pages is None:
        # Validate the live API shape end-to-end without crawling all 29 pages.
        max_pages = 1
        logger.info("[dry-run] capping the walk at 1 page")

    total = client.total_tokens(CHAIN_ARBITRUM)
    logger.info("clanker reports %d tokens on chain %d", total, CHAIN_ARBITRUM)

    tokens, examined = collect_tokens(
        client,
        window.query_since,
        max_pages=max_pages,
        stop_early=not window.is_backfill,
    )

    df = pd.DataFrame(tokens, columns=TOKEN_COLUMNS)
    if not df.empty:
        # iter_tokens dedupes within a walk, but a token can legitimately be
        # re-emitted across cursor boundaries; keep the newest record per token.
        df = df.sort_values("deployed_at", ascending=False).drop_duplicates(
            subset=["token_address"], keep="first"
        )
    df = df.reset_index(drop=True)

    with_fid = int(df["fid"].notna().sum()) if not df.empty else 0
    notes = [
        f"clanker reports {total} tokens on chain {CHAIN_ARBITRUM}; "
        f"examined {examined}, kept {len(df)}",
        f"{with_fid}/{len(df)} tokens carry a Farcaster fid; the remainder were "
        f"deployed with no Farcaster requestor and rely on the linked_wallets join",
    ]
    df = attach_admin_fids(df, notes, args.dry_run, limit=args.limit)

    writer.write("tokens", df)

    watermark = max_timestamp(df["deployed_at"]) if not df.empty else None
    if args.dry_run:
        notes.append("dry run: page walk was capped, counts are not a full registry")

    writer.finish(
        params={
            "chain_id": CHAIN_ARBITRUM,
            "max_pages": max_pages,
            "stop_early": not window.is_backfill,
            "is_backfill": window.is_backfill,
        },
        since=window.since,
        new_watermark=watermark,
        notes=notes,
    )
    if not args.dry_run:
        set_watermark(PIPELINE, watermark, run_ts=writer.run_ts)

    return {
        "tokens": len(df),
        "with_fid": with_fid,
        "with_admin_fid": int(df["fee_recipient_fid"].notna().sum()) if not df.empty else 0,
        "examined": examined,
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
