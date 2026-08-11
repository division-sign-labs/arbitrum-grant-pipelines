"""Bankr token-launch client.

Deliberately thin, because the API is: `GET /token-launches` returns exactly the
50 most recent launches and accepts no pagination parameter (verified — limit,
offset, page and cursor are all ignored). At the observed launch rate those 50
records cover under an hour of history, so this endpoint is a freshness/metadata
top-up, not a registry.

The historical registry comes from Dune's `robinhood` schema (see
`sql/robinhood.py`). What this client uniquely provides is the deployer's
off-chain identity (`deployer.walletAddress`, `xUsername`), which the chain
alone does not give us.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from config.settings import BANKR_API_BASE, CHAIN_ROBINHOOD
from lib.http import HttpClient

logger = logging.getLogger(__name__)

# Bankr's `chain` string -> our numeric chain id.
CHAIN_IDS = {"robinhood": CHAIN_ROBINHOOD, "base": 8453}


class BankrClient:
    def __init__(self, rps: float = 2.0):
        self.http = HttpClient(BANKR_API_BASE, rps=rps, name="bankr")

    def recent_launches(self) -> list[dict]:
        """The 50 most recent launches across all chains Bankr deploys to."""
        payload = self.http.get_json("/token-launches")
        launches = payload.get("launches") or []
        logger.info("bankr: %d recent launches", len(launches))
        return launches

    def creator_fees(self, wallet_address: str) -> dict | None:
        """Doppler creator fees for a wallet — a cheap 'did this wallet launch' probe."""
        try:
            return self.http.get_json(
                f"/public/doppler/creator-fees/{wallet_address.lower()}"
            )
        except Exception as exc:
            logger.debug("bankr creator-fees(%s) failed: %s", wallet_address, exc)
            return None


def normalise_launch(row: dict) -> dict:
    """Flatten a Bankr launch record into our token CSV schema."""
    deployer = row.get("deployer") or {}
    fee_recipient = row.get("feeRecipient") or {}
    chain = (row.get("chain") or "").lower()
    ts = row.get("timestamp")
    deployed_at = (
        datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat() if ts else None
    )
    return {
        "token_address": (row.get("tokenAddress") or "").lower(),
        "chain_id": CHAIN_IDS.get(chain),
        "chain": chain,
        "platform": "bankr",
        "deployer_address": (deployer.get("walletAddress") or "").lower() or None,
        "fee_recipient_address": (fee_recipient.get("walletAddress") or "").lower()
        or None,
        "x_username": deployer.get("xUsername"),
        "name": row.get("tokenName"),
        "symbol": row.get("tokenSymbol"),
        "deployed_at": deployed_at,
        "tx_hash": (row.get("txHash") or "").lower() or None,
        "pool_address": (row.get("poolId") or "").lower() or None,
        "launch_type": row.get("launchType"),
        "status": row.get("status"),
        "source": "bankr_api",
    }
