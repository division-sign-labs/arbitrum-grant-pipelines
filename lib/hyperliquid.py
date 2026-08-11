"""Hyperliquid /info client.

Two calls answer everything the grant asks for per wallet:
  userRateLimit              -> cumVlm, lifetime trading volume (a string)
  userNonFundingLedgerUpdates -> deposits/transfers; the earliest is first touch

Budget: 1200 weight/min/IP and these payloads cost ~20 each, so the ceiling is
60 calls/min. We pace to 80% of that. This is why the crawl runs over the
Arbitrum cohort rather than every Farcaster wallet — the cohort is thousands,
the full verification set is millions.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from config.settings import (
    HYPERLIQUID_API_URL,
    HYPERLIQUID_CALL_WEIGHT,
    HYPERLIQUID_SAFETY_FACTOR,
    HYPERLIQUID_WEIGHT_PER_MINUTE,
)
from lib.http import HttpClient

logger = logging.getLogger(__name__)


def _default_rps() -> float:
    calls_per_minute = HYPERLIQUID_WEIGHT_PER_MINUTE / HYPERLIQUID_CALL_WEIGHT
    return (calls_per_minute * HYPERLIQUID_SAFETY_FACTOR) / 60.0


class HyperliquidClient:
    def __init__(self, rps: float | None = None):
        self.http = HttpClient(
            HYPERLIQUID_API_URL,
            headers={"Content-Type": "application/json"},
            rps=rps if rps is not None else _default_rps(),
            name="hyperliquid",
        )

    def _info(self, payload: dict):
        # The base_url IS the endpoint here; pass it through unchanged.
        return self.http.request("POST", HYPERLIQUID_API_URL, json=payload).json()

    def lifetime_volume(self, address: str) -> float:
        """Cumulative lifetime trading volume in USD. 0.0 means never traded."""
        payload = self._info({"type": "userRateLimit", "user": address.lower()})
        try:
            return float((payload or {}).get("cumVlm") or 0.0)
        except (TypeError, ValueError):
            logger.warning("unparseable cumVlm for %s: %r", address, payload)
            return 0.0

    def first_activity(self, address: str) -> tuple[datetime | None, int]:
        """Earliest ledger event (deposit/transfer) and how many were returned.

        Time-ranged responses cap at 500 entries, but they come back oldest-first
        from startTime=0, so the earliest is always in the first page — which is
        all we need for a first-touch date.
        """
        updates = self._info(
            {
                "type": "userNonFundingLedgerUpdates",
                "user": address.lower(),
                "startTime": 0,
            }
        )
        if not isinstance(updates, list) or not updates:
            return None, 0
        times = [u.get("time") for u in updates if isinstance(u, dict) and u.get("time")]
        if not times:
            return None, len(updates)
        return datetime.fromtimestamp(min(times) / 1000, tz=timezone.utc), len(updates)

    def wallet_summary(self, address: str) -> dict:
        """The full per-wallet record: two calls, one row."""
        volume = self.lifetime_volume(address)
        first_at, event_count = self.first_activity(address)
        return {
            "address": address.lower(),
            "has_hl_activity": bool(volume > 0 or first_at is not None),
            "cum_volume_usd": volume,
            "first_activity_at": first_at.isoformat() if first_at else None,
            "ledger_event_count": event_count,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
