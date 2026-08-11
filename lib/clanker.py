"""Clanker token registry client.

`GET https://www.clanker.world/api/tokens` is public and cursor-paginated, and
with `includeUser=true` it hands back the deployer's Farcaster identity at
`related.user.fid` — which is exactly the token->creator->fid edge we need, with
no wallet join required. Not every token has one (contract-deployed tokens have
no Farcaster requestor), so `fid` is nullable and the wallet join in
`linked_wallets` covers the rest.

Observed: ~565 tokens on Arbitrum (chain 42161), page size caps at 20.
"""

from __future__ import annotations

import logging
from typing import Iterator

from config.settings import CLANKER_API_BASE
from lib.http import HttpClient

logger = logging.getLogger(__name__)

PAGE_SIZE = 20


class ClankerClient:
    def __init__(self, rps: float = 4.0):
        self.http = HttpClient(CLANKER_API_BASE, rps=rps, name="clanker")

    def iter_tokens(
        self,
        chain_id: int,
        max_pages: int | None = None,
        include_user: bool = True,
    ) -> Iterator[dict]:
        """Yield token records newest-first, following the cursor."""
        cursor: str | None = None
        pages = 0
        seen: set[str] = set()
        while True:
            params = {
                "chainId": chain_id,
                "limit": PAGE_SIZE,
                "sort": "desc",
            }
            if include_user:
                params["includeUser"] = "true"
            if cursor:
                params["cursor"] = cursor
            payload = self.http.get_json("/tokens", params=params)
            rows = payload.get("data") or []
            if not rows:
                return
            for row in rows:
                key = str(row.get("contract_address") or row.get("id"))
                # The cursor is timestamp-based; tokens sharing a deploy second
                # can straddle a page boundary and repeat.
                if key in seen:
                    continue
                seen.add(key)
                yield row
            pages += 1
            if max_pages is not None and pages >= max_pages:
                logger.info("clanker: stopping after %d pages (--limit)", pages)
                return
            cursor = payload.get("cursor")
            if not cursor:
                return

    def total_tokens(self, chain_id: int) -> int:
        payload = self.http.get_json(
            "/tokens", params={"chainId": chain_id, "limit": 1, "sort": "desc"}
        )
        return int(payload.get("total") or 0)


def normalise_token(row: dict, chain_id: int) -> dict:
    """Flatten a Clanker API record into our token CSV schema."""
    related = row.get("related") or {}
    user = related.get("user") or {}
    market = related.get("market") or {}
    pool_config = row.get("pool_config") or {}
    return {
        "token_address": (row.get("contract_address") or "").lower(),
        "chain_id": int(row.get("chain_id") or chain_id),
        "platform": "clanker",
        "deployer_address": (row.get("msg_sender") or "").lower() or None,
        "admin_address": (row.get("admin") or "").lower() or None,
        "fid": user.get("fid"),
        "username": user.get("username"),
        "name": row.get("name"),
        "symbol": row.get("symbol"),
        "deployed_at": row.get("deployed_at") or row.get("created_at"),
        "tx_hash": (row.get("tx_hash") or "").lower() or None,
        "pool_address": (row.get("pool_address") or "").lower() or None,
        "paired_token": (pool_config.get("pairedToken") or "").lower() or None,
        "token_type": row.get("type"),
        "starting_market_cap_usd": row.get("starting_market_cap"),
        "price_usd": row.get("priceUsd"),
        "market_cap_usd": market.get("marketCap"),
        "volume_24h_usd": market.get("volume24h"),
    }
