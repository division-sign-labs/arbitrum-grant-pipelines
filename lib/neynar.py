"""Neynar API client.

Dune carries the bulk history but syncs roughly daily and has no full-text cast
search. Neynar covers the two gaps: the /arbitrum channel feed (fresh, and Dune
has no clean channel-cast view), and cast search for evangelism when a
full-table LIKE scan on Dune would be too expensive.
"""

from __future__ import annotations

import logging
from typing import Iterator

from config.settings import NEYNAR_API_BASE, NEYNAR_API_KEY, NEYNAR_REQUESTS_PER_SECOND
from lib.http import HttpClient

logger = logging.getLogger(__name__)

MAX_PAGE = 100
MAX_FIDS_PER_CALL = 100


class NeynarClient:
    def __init__(self, api_key: str | None = None, rps: float | None = None):
        key = api_key or NEYNAR_API_KEY
        if not key:
            raise RuntimeError(
                "NEYNAR_API_KEY is not set. Copy .env.example to .env and fill it in."
            )
        self.http = HttpClient(
            NEYNAR_API_BASE,
            headers={"x-api-key": key, "accept": "application/json"},
            rps=rps if rps is not None else NEYNAR_REQUESTS_PER_SECOND,
            name="neynar",
        )

    # -- pagination --------------------------------------------------------

    def paginate(
        self,
        path: str,
        params: dict,
        items_key: str,
        max_pages: int | None = None,
    ) -> Iterator[dict]:
        cursor = None
        pages = 0
        while True:
            call_params = dict(params)
            if cursor:
                call_params["cursor"] = cursor
            payload = self.http.get_json(path, params=call_params)
            for item in payload.get(items_key) or []:
                yield item
            pages += 1
            if max_pages is not None and pages >= max_pages:
                return
            cursor = (payload.get("next") or {}).get("cursor")
            if not cursor:
                return

    # -- endpoints ---------------------------------------------------------

    def channel_feed(
        self,
        channel_id: str,
        max_pages: int | None = None,
        with_replies: bool = True,
    ) -> Iterator[dict]:
        """Casts in a channel, newest first."""
        return self.paginate(
            "/v2/farcaster/feed/channels",
            {
                "channel_ids": channel_id,
                "limit": MAX_PAGE,
                "with_recasts": "false",
                "with_replies": str(with_replies).lower(),
            },
            items_key="casts",
            max_pages=max_pages,
        )

    def search_casts(
        self, query: str, max_pages: int | None = None, **extra
    ) -> Iterator[dict]:
        """Full-text cast search — the evangelism fallback when Dune is too dear."""
        params = {"q": query, "limit": MAX_PAGE, **extra}
        return self.paginate(
            "/v2/farcaster/cast/search", params, items_key="casts", max_pages=max_pages
        )

    def cast_reactions(
        self, cast_hash: str, types: str = "likes,recasts", max_pages: int | None = None
    ) -> Iterator[dict]:
        return self.paginate(
            "/v2/farcaster/reactions/cast",
            {"hash": cast_hash, "types": types, "limit": MAX_PAGE},
            items_key="reactions",
            max_pages=max_pages,
        )

    def bulk_users(self, fids) -> list[dict]:
        """User records (incl. verified_addresses) for up to any number of fids."""
        fids = [int(f) for f in fids]
        users: list[dict] = []
        for i in range(0, len(fids), MAX_FIDS_PER_CALL):
            chunk = fids[i : i + MAX_FIDS_PER_CALL]
            payload = self.http.get_json(
                "/v2/farcaster/user/bulk",
                params={"fids": ",".join(str(f) for f in chunk)},
            )
            users.extend(payload.get("users") or [])
        return users

    def channel(self, channel_id: str) -> dict:
        payload = self.http.get_json(
            "/v2/farcaster/channel", params={"id": channel_id, "type": "id"}
        )
        return payload.get("channel") or {}


def search_response_to_casts(payload: dict) -> list[dict]:
    """Cast-search nests its results one level deeper than the feed endpoints."""
    result = payload.get("result") or {}
    return result.get("casts") or payload.get("casts") or []
