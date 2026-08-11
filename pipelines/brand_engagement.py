"""Farcaster engagement with the Arbitrum brand accounts and the /arbitrum channel.

WHAT THIS PRODUCES
  brand_engagements.csv          one row per engagement event (reply / like / recast / mention)
                                 aimed at a seeded brand account
  brand_engagement_summary.csv   per (engager, brand) counts and a weighted score
  channel_casts.csv              one row per cast in /arbitrum
  channel_engagement_summary.csv per fid: what they posted in the channel and what it earned

WHY NEYNAR AND NOTHING ELSE
  There is no Farcaster social data on Dune with this key — the
  `dune.neynar.dataset_farcaster_*` tables do not exist (verified: all six fail).
  Casts, reactions and channel membership therefore have exactly one source, the
  Neynar REST API, and this pipeline is 100% HTTP. That makes it the one pipeline
  whose cost is measured in API calls rather than Dune credits, so every phase
  logs the running request count.

ENDPOINT SHAPES (verified live against fid 3 and channel /arbitrum, 2026-08)
  GET /v2/farcaster/feed/user/casts  -> {"casts": [...], "next": {"cursor": ...}}
      accepts limit=150; newest-first; each cast carries reactions.likes_count /
      reactions.recasts_count / replies.count, but reactions.likes and
      reactions.recasts come back as EMPTY lists, so the reactor fids can only be
      had from /reactions/cast.
  GET /v2/farcaster/cast/conversation -> {"conversation": {"cast": {..., "direct_replies": [...]}},
                                          "next": {"cursor": ...}}
      limit caps at 50 and it pages via the top-level cursor; with reply_depth=1
      the nested direct_replies of each reply are empty, which is what we want.
  GET /v2/farcaster/reactions/cast   -> {"reactions": [{"reaction_type": "like"|"recast",
                                          "reaction_timestamp": ..., "user": {"fid": ...}}], ...}
      limit 100, top-level cursor.
  GET /v2/farcaster/feed/channels    -> same envelope as the user feed; /arbitrum casts
      carry parent_url == root_parent_url == "https://warpcast.com/~/channel/arbitrum".
  GET /v2/farcaster/cast/search      -> {"result": {"casts": [...], "next": {"cursor": ...}}}
      NOTE the extra nesting: the cursor is at result.next.cursor, NOT at the top
      level, so lib.neynar.NeynarClient.paginate() (and therefore .search_casts())
      yields nothing for this endpoint. Mentions page through _iter_search() below,
      which reads the envelope the endpoint actually returns.

ENGAGEMENT ROW CONVENTIONS (brand_engagements.csv)
  reply    cast_hash = the reply's own hash,     target_cast_hash = the brand cast replied to
  like     cast_hash = ""                        target_cast_hash = the brand cast liked
  recast   cast_hash = ""                        target_cast_hash = the brand cast recast
  mention  cast_hash = the mentioning cast hash, target_cast_hash = "" (no brand cast involved)
  A reaction has no cast of its own in the protocol, hence the blank cast_hash;
  a mention targets the account, not a particular brand cast, hence the blank target.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime
from typing import Iterable, Iterator, Sequence

import pandas as pd
import requests

from config.settings import ARBITRUM_CHANNEL_ID, ENGAGEMENT_WEIGHTS
from lib.cli import base_parser, resolve_window
from lib.logging_utils import setup_logging
from lib.neynar import NeynarClient, search_response_to_casts
from lib.runs import RunWriter, utc_now
from lib.seeds import SeedMissingError, load_brand_accounts
from lib.state import parse_ts, set_watermark

logger = logging.getLogger(__name__)

PIPELINE = "brand_engagement"
DATA_TYPE = "brand_engagement"

# Page sizes each endpoint actually honours (see the docstring — all verified).
BRAND_FEED_PAGE = 150
CHANNEL_FEED_PAGE = 100
CONVERSATION_PAGE = 50
REACTIONS_PAGE = 100
SEARCH_PAGE = 100

# Stops a cursor loop that never terminates because timestamps came back unsorted
# or the cursor stopped advancing. The window bound is the real brake; this is
# only a seatbelt, and hitting it is logged as a warning.
MAX_PAGES_HARD_CAP = 500
# Cast search has no server-side date filter, so a backfill would otherwise walk
# the whole index. Mentions are best-effort by design; this bounds the effort.
DEFAULT_SEARCH_PAGES = 25
# Replies and reactions on one brand cast are rarely more than a few hundred.
MAX_DETAIL_PAGES = 20

ENGAGEMENT_COLUMNS = [
    "engager_fid",
    "brand_fid",
    "engagement_type",
    "cast_hash",
    "target_cast_hash",
    "timestamp",
]
ENGAGEMENT_SUMMARY_COLUMNS = [
    "engager_fid",
    "brand_fid",
    "replies",
    "likes",
    "recasts",
    "mentions",
    "weighted_score",
    "window_start",
    "window_end",
]
CHANNEL_CAST_COLUMNS = [
    "cast_hash",
    "author_fid",
    "channel_id",
    "timestamp",
    "parent_hash",
    "likes_count",
    "recasts_count",
    "replies_count",
    "text_length",
]
CHANNEL_SUMMARY_COLUMNS = [
    "fid",
    "channel_id",
    "casts_posted",
    "reactions_received",
    "replies_received",
    "reactions_given",
    "first_cast_at",
    "last_cast_at",
]

# Everything a degraded HTTP call can raise. requests.HTTPError and the JSON
# decode error are both RequestException; lib.http raises RuntimeError once it
# has exhausted its retries.
TRANSPORT_ERRORS = (requests.RequestException, RuntimeError, ValueError, TypeError, KeyError)


class CallBudget:
    """A cap on one class of expensive per-cast detail call.

    --limit has to be a real brake, not a suggestion: the reply and reaction
    expansions are O(brand casts) API calls and dominate the run's cost. When the
    budget runs out we count the skips so the operator learns the resulting
    numbers are a floor, never a silently truncated total.
    """

    def __init__(self, name: str, limit: int | None):
        self.name = name
        self.limit = limit
        self.used = 0
        self.skipped = 0

    def take(self) -> bool:
        if self.limit is not None and self.used >= self.limit:
            self.skipped += 1
            return False
        self.used += 1
        return True

    @property
    def exhausted(self) -> bool:
        return self.skipped > 0

    def note(self) -> str | None:
        if not self.exhausted:
            return None
        return (
            f"{self.name}: --limit capped detail fetches at {self.limit}; "
            f"{self.skipped} cast(s) were not expanded, so those counts are a floor"
        )


def _iso(value: datetime | None) -> str:
    return value.isoformat() if value is not None else ""


def _fid(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _iter_feed(
    client: NeynarClient,
    path: str,
    params: dict,
    query_since: datetime,
    page_cap: int | None,
    label: str,
    truncations: list[str],
    items_key: str = "casts",
) -> Iterator[tuple[dict, datetime]]:
    """Page a newest-first Neynar feed, stopping once it drops below the window.

    Both feed endpoints page on a top-level next.cursor and return strictly
    descending timestamps, so the first cast older than query_since means the
    remainder of the crawl is out of window too.

    Any stop for a reason OTHER than reaching the window edge is appended to
    `truncations`, because it means the feed was cut short and every count
    derived from it is a floor rather than a total.
    """
    cursor: str | None = None
    pages = 0
    while True:
        call_params = dict(params)
        if cursor:
            call_params["cursor"] = cursor
        payload = client.http.get_json(path, params=call_params)
        items = payload.get(items_key) or []
        pages += 1
        below_window = False
        for item in items:
            timestamp = parse_ts(item.get("timestamp"))
            if timestamp is None:
                continue
            if timestamp < query_since:
                below_window = True
                continue
            yield item, timestamp
        if below_window or not items:
            return
        if page_cap is not None and pages >= page_cap:
            message = (
                f"{label}: --limit stopped the feed at page {page_cap} before reaching "
                f"{query_since.date()}; this run covers only the newest "
                f"{pages * int(params.get('limit') or 0)} cast(s)"
            )
            logger.warning(message)
            truncations.append(message)
            return
        if pages >= MAX_PAGES_HARD_CAP:
            message = (
                f"{label}: hit the {MAX_PAGES_HARD_CAP}-page seatbelt before reaching "
                f"{query_since.date()}; the window was not fully covered"
            )
            logger.warning(message)
            truncations.append(message)
            return
        cursor = (payload.get("next") or {}).get("cursor")
        if not cursor:
            return


def _iter_search(
    client: NeynarClient,
    query: str,
    query_since: datetime,
    page_cap: int | None,
) -> Iterator[tuple[dict, datetime]]:
    """Page /cast/search, which nests its results and its cursor under `result`.

    NeynarClient.paginate() looks for payload["casts"] and payload["next"], both
    of which are absent here, so it yields nothing — this endpoint needs its own
    loop. Verified live: the envelope is {"result": {"casts": [...], "next": {...}}}.
    """
    cursor: str | None = None
    pages = 0
    cap = DEFAULT_SEARCH_PAGES if page_cap is None else min(page_cap, DEFAULT_SEARCH_PAGES)
    while True:
        params = {"q": query, "limit": SEARCH_PAGE, "mode": "literal"}
        if cursor:
            params["cursor"] = cursor
        payload = client.http.get_json("/v2/farcaster/cast/search", params=params)
        casts = search_response_to_casts(payload)
        pages += 1
        below_window = False
        for cast in casts:
            timestamp = parse_ts(cast.get("timestamp"))
            if timestamp is None:
                continue
            if timestamp < query_since:
                below_window = True
                continue
            yield cast, timestamp
        if below_window or not casts or pages >= cap:
            return
        result = payload.get("result") or {}
        nxt = result.get("next") or payload.get("next") or {}
        cursor = nxt.get("cursor")
        if not cursor:
            return


def _fetch_direct_replies(client: NeynarClient, cast_hash: str) -> list[dict]:
    """Direct repliers to one cast. Returns [] rather than dying on shape drift."""
    replies: list[dict] = []
    cursor: str | None = None
    pages = 0
    while True:
        params = {
            "identifier": cast_hash,
            "type": "hash",
            "reply_depth": 1,
            "limit": CONVERSATION_PAGE,
        }
        if cursor:
            params["cursor"] = cursor
        try:
            payload = client.http.get_json("/v2/farcaster/cast/conversation", params=params)
            conversation = payload.get("conversation")
            if not isinstance(conversation, dict):
                logger.warning("conversation %s: no 'conversation' object, skipping replies", cast_hash)
                return replies
            cast = conversation.get("cast")
            if not isinstance(cast, dict):
                logger.warning("conversation %s: no 'conversation.cast', skipping replies", cast_hash)
                return replies
            page = cast.get("direct_replies")
            if not isinstance(page, list):
                logger.warning(
                    "conversation %s: direct_replies is %s, not a list; skipping replies",
                    cast_hash,
                    type(page).__name__,
                )
                return replies
            replies.extend(r for r in page if isinstance(r, dict))
            cursor = (payload.get("next") or {}).get("cursor")
        except TRANSPORT_ERRORS as exc:
            logger.warning("conversation %s failed (%s); replies skipped", cast_hash, exc)
            return replies
        pages += 1
        if not cursor or pages >= MAX_DETAIL_PAGES:
            return replies


def _fetch_reactions(client: NeynarClient, cast_hash: str) -> list[dict]:
    """Likes and recasts on one cast, as {reaction_type, user.fid, reaction_timestamp}."""
    try:
        return [
            r
            for r in client.cast_reactions(
                cast_hash, types="likes,recasts", max_pages=MAX_DETAIL_PAGES
            )
            if isinstance(r, dict)
        ]
    except TRANSPORT_ERRORS as exc:
        logger.warning("reactions %s failed (%s); reactions skipped", cast_hash, exc)
        return []


def _mentioned_fids(cast: dict) -> set[int]:
    profiles = cast.get("mentioned_profiles")
    if not isinstance(profiles, list):
        return set()
    out = set()
    for profile in profiles:
        if isinstance(profile, dict):
            fid = _fid(profile.get("fid"))
            if fid is not None:
                out.add(fid)
    return out


def _resolve_brand_usernames(client: NeynarClient, fids: Sequence[int]) -> dict[int, str]:
    """Usernames for the seeded brand fids — the seed only guarantees `fid`."""
    try:
        users = client.bulk_users(list(fids))
    except TRANSPORT_ERRORS as exc:
        logger.warning("brand username lookup failed (%s); mention search degraded", exc)
        return {}
    out: dict[int, str] = {}
    for user in users:
        fid = _fid(user.get("fid"))
        username = user.get("username")
        if fid is not None and username:
            out[fid] = str(username)
    return out


def _collect_brand_engagement(
    client: NeynarClient,
    brands: pd.DataFrame,
    query_since: datetime,
    page_cap: int | None,
    detail_cap: int | None,
    notes: list[str],
) -> tuple[list[dict], list[datetime]]:
    """Half A: replies, likes, recasts and mentions aimed at the brand accounts."""
    brand_fids = [int(f) for f in brands["fid"]]
    usernames = _resolve_brand_usernames(client, brand_fids)
    seeded_names = {
        int(row.fid): (str(row.name_) if row.name_ and str(row.name_) != "nan" else "")
        for row in brands.itertuples()
    }

    reply_budget = CallBudget("brand replies", detail_cap)
    reaction_budget = CallBudget("brand reactions", detail_cap)

    rows: list[dict] = []
    timestamps: list[datetime] = []
    # Casts already in hand are scanned for mentions for free, before any search.
    mention_sources: list[dict] = []

    for fid in brand_fids:
        label = usernames.get(fid) or seeded_names.get(fid) or str(fid)
        brand_casts = 0
        before = client.http.request_count
        for cast, cast_ts in _iter_feed(
            client,
            "/v2/farcaster/feed/user/casts",
            {"fid": fid, "limit": BRAND_FEED_PAGE},
            query_since,
            page_cap,
            label=f"brand feed @{label} (fid {fid})",
            truncations=notes,
        ):
            cast_hash = cast.get("hash")
            if not cast_hash:
                continue
            brand_casts += 1
            timestamps.append(cast_ts)
            mention_sources.append(cast)

            reactions = cast.get("reactions") or {}
            likes_count = int(reactions.get("likes_count") or 0)
            recasts_count = int(reactions.get("recasts_count") or 0)
            replies_count = int((cast.get("replies") or {}).get("count") or 0)

            if replies_count > 0 and reply_budget.take():
                for reply in _fetch_direct_replies(client, cast_hash):
                    engager = _fid((reply.get("author") or {}).get("fid"))
                    if engager is None or engager == fid:
                        continue
                    reply_ts = parse_ts(reply.get("timestamp")) or cast_ts
                    timestamps.append(reply_ts)
                    mention_sources.append(reply)
                    rows.append(
                        {
                            "engager_fid": engager,
                            "brand_fid": fid,
                            "engagement_type": "reply",
                            "cast_hash": reply.get("hash") or "",
                            "target_cast_hash": cast_hash,
                            "timestamp": _iso(reply_ts),
                        }
                    )

            if (likes_count + recasts_count) > 0 and reaction_budget.take():
                for reaction in _fetch_reactions(client, cast_hash):
                    engager = _fid((reaction.get("user") or {}).get("fid"))
                    if engager is None or engager == fid:
                        continue
                    kind = str(reaction.get("reaction_type") or "").lower()
                    if kind not in ("like", "recast"):
                        continue
                    reaction_ts = parse_ts(reaction.get("reaction_timestamp")) or cast_ts
                    timestamps.append(reaction_ts)
                    rows.append(
                        {
                            "engager_fid": engager,
                            "brand_fid": fid,
                            "engagement_type": kind,
                            "cast_hash": "",
                            "target_cast_hash": cast_hash,
                            "timestamp": _iso(reaction_ts),
                        }
                    )

        logger.info(
            "brand %s (fid %d): %d casts in window, %d API calls so far (+%d)",
            label,
            fid,
            brand_casts,
            client.http.request_count,
            client.http.request_count - before,
        )

    # Mentions, part 1: free — the casts already fetched carry mentioned_profiles.
    brand_fid_set = set(brand_fids)
    mention_rows = _mentions_from_casts(mention_sources, brand_fid_set, timestamps)

    # Mentions, part 2: cast search per brand username. Search has no date filter
    # and no completeness guarantee, so this is explicitly best-effort.
    searched = 0
    unresolved = [f for f in brand_fids if f not in usernames]
    for fid in brand_fids:
        username = usernames.get(fid)
        if not username:
            continue
        try:
            hits = list(_iter_search(client, f"@{username}", query_since, page_cap))
        except TRANSPORT_ERRORS as exc:
            logger.warning("mention search for @%s failed (%s); skipping", username, exc)
            continue
        searched += 1
        mention_rows.extend(
            _mentions_from_casts(
                [c for c, _ in hits], {fid}, timestamps, fallback_handle=username
            )
        )

    rows.extend(mention_rows)

    if unresolved:
        notes.append(
            f"mentions: no username resolved for brand fid(s) {sorted(unresolved)}; "
            "their mentions come only from mentioned_profiles on casts already fetched"
        )
    notes.append(
        "mentions are best-effort: /cast/search has no server-side date filter and no "
        f"completeness guarantee, so the crawl is capped at {DEFAULT_SEARCH_PAGES} pages "
        f"per brand ({searched} brand handle(s) searched) and supplemented by "
        "mentioned_profiles on casts already in hand"
    )
    for budget in (reply_budget, reaction_budget):
        note = budget.note()
        if note:
            logger.warning(note)
            notes.append(note)

    return rows, timestamps


def _mentions_from_casts(
    casts: Iterable[dict],
    brand_fids: set[int],
    timestamps: list[datetime],
    fallback_handle: str | None = None,
) -> list[dict]:
    """Mention events: a cast by someone else that names a brand account.

    mentioned_profiles is the authoritative signal — it is the resolved fid, so
    it cannot confuse @arbitrum with @arbitrumfoundation. Only when a search hit
    comes back with no resolved profiles do we fall back to matching the literal
    handle in the text, which is why the fallback is per-brand and opt-in.
    """
    rows: list[dict] = []
    needle = f"@{fallback_handle.lower()}" if fallback_handle else None
    for cast in casts:
        author = _fid((cast.get("author") or {}).get("fid"))
        cast_hash = cast.get("hash")
        if author is None or not cast_hash:
            continue
        mentioned = _mentioned_fids(cast)
        matched = mentioned & brand_fids
        if not matched and needle:
            text = str(cast.get("text") or "").lower()
            if not mentioned and needle in text:
                matched = set(brand_fids)
        for brand_fid in matched:
            if author == brand_fid:
                continue
            timestamp = parse_ts(cast.get("timestamp"))
            if timestamp is not None:
                timestamps.append(timestamp)
            rows.append(
                {
                    "engager_fid": author,
                    "brand_fid": brand_fid,
                    "engagement_type": "mention",
                    "cast_hash": cast_hash,
                    "target_cast_hash": "",
                    "timestamp": _iso(timestamp),
                }
            )
    return rows


def _summarise_brand_engagement(
    events: pd.DataFrame,
    brands: pd.DataFrame,
    window_start: datetime,
    window_end: datetime,
) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame(columns=ENGAGEMENT_SUMMARY_COLUMNS)
    counts = (
        events.groupby(["engager_fid", "brand_fid", "engagement_type"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )
    for kind in ("reply", "like", "recast", "mention"):
        if kind not in counts.columns:
            counts[kind] = 0
    weights = {int(row.fid): float(row.weight) for row in brands.itertuples()}
    counts["brand_weight"] = counts["brand_fid"].map(weights).fillna(1.0)
    counts["weighted_score"] = counts["brand_weight"] * (
        counts["reply"] * ENGAGEMENT_WEIGHTS["reply"]
        + counts["recast"] * ENGAGEMENT_WEIGHTS["recast"]
        + counts["mention"] * ENGAGEMENT_WEIGHTS["mention"]
        + counts["like"] * ENGAGEMENT_WEIGHTS["like"]
    )
    counts = counts.rename(
        columns={
            "reply": "replies",
            "like": "likes",
            "recast": "recasts",
            "mention": "mentions",
        }
    )
    counts["window_start"] = _iso(window_start)
    counts["window_end"] = _iso(window_end)
    counts["weighted_score"] = counts["weighted_score"].round(4)
    return counts[ENGAGEMENT_SUMMARY_COLUMNS].sort_values(
        ["weighted_score", "engager_fid"], ascending=[False, True]
    )


def _collect_channel(
    client: NeynarClient,
    query_since: datetime,
    page_cap: int | None,
    detail_cap: int | None,
    notes: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, list[datetime], list[dict]]:
    """Half B: the /arbitrum channel feed plus who reacted inside it."""
    cast_rows: list[dict] = []
    timestamps: list[datetime] = []
    reaction_targets: list[tuple[str, int]] = []
    channel_casts: list[dict] = []

    for cast, cast_ts in _iter_feed(
        client,
        "/v2/farcaster/feed/channels",
        {
            "channel_ids": ARBITRUM_CHANNEL_ID,
            "limit": CHANNEL_FEED_PAGE,
            "with_recasts": "false",
            "with_replies": "true",
        },
        query_since,
        page_cap,
        label=f"channel feed /{ARBITRUM_CHANNEL_ID}",
        truncations=notes,
    ):
        cast_hash = cast.get("hash")
        author = _fid((cast.get("author") or {}).get("fid"))
        if not cast_hash or author is None:
            continue
        reactions = cast.get("reactions") or {}
        likes_count = int(reactions.get("likes_count") or 0)
        recasts_count = int(reactions.get("recasts_count") or 0)
        replies_count = int((cast.get("replies") or {}).get("count") or 0)
        channel_id = (cast.get("channel") or {}).get("id") or ARBITRUM_CHANNEL_ID
        timestamps.append(cast_ts)
        channel_casts.append(cast)
        cast_rows.append(
            {
                "cast_hash": cast_hash,
                "author_fid": author,
                "channel_id": channel_id,
                "timestamp": _iso(cast_ts),
                "parent_hash": cast.get("parent_hash") or "",
                "likes_count": likes_count,
                "recasts_count": recasts_count,
                "replies_count": replies_count,
                "text_length": len(str(cast.get("text") or "")),
            }
        )
        if likes_count + recasts_count > 0:
            reaction_targets.append((cast_hash, likes_count + recasts_count))

    casts_df = pd.DataFrame(cast_rows, columns=CHANNEL_CAST_COLUMNS)
    if not casts_df.empty:
        casts_df = casts_df.drop_duplicates(subset=["cast_hash"])
    logger.info(
        "channel %s: %d casts in window, %d with reactions to expand, %d API calls so far",
        ARBITRUM_CHANNEL_ID,
        len(casts_df),
        len(reaction_targets),
        client.http.request_count,
    )

    # Busiest casts first, so a truncated budget still buys the most signal.
    reaction_targets.sort(key=lambda pair: pair[1], reverse=True)
    budget = CallBudget("channel reactions", detail_cap)
    given: dict[int, int] = defaultdict(int)
    for cast_hash, _count in reaction_targets:
        if not budget.take():
            continue
        for reaction in _fetch_reactions(client, cast_hash):
            reactor = _fid((reaction.get("user") or {}).get("fid"))
            if reactor is None:
                continue
            if str(reaction.get("reaction_type") or "").lower() not in ("like", "recast"):
                continue
            given[reactor] += 1
            reaction_ts = parse_ts(reaction.get("reaction_timestamp"))
            if reaction_ts is not None:
                timestamps.append(reaction_ts)
    note = budget.note()
    if note:
        logger.warning(note)
        notes.append(note)

    summary = _summarise_channel(casts_df, given)
    return casts_df, summary, timestamps, channel_casts


def _summarise_channel(casts_df: pd.DataFrame, given: dict[int, int]) -> pd.DataFrame:
    """Per-fid channel activity. Reactors who never posted still get a row."""
    if casts_df.empty and not given:
        return pd.DataFrame(columns=CHANNEL_SUMMARY_COLUMNS)
    channel_id = (
        casts_df["channel_id"].iloc[0] if not casts_df.empty else ARBITRUM_CHANNEL_ID
    )
    if casts_df.empty:
        posted = pd.DataFrame(
            columns=[
                "fid",
                "casts_posted",
                "reactions_received",
                "replies_received",
                "first_cast_at",
                "last_cast_at",
            ]
        )
    else:
        posted = (
            casts_df.assign(
                reactions_received=casts_df["likes_count"] + casts_df["recasts_count"]
            )
            .groupby("author_fid")
            .agg(
                casts_posted=("cast_hash", "count"),
                reactions_received=("reactions_received", "sum"),
                replies_received=("replies_count", "sum"),
                first_cast_at=("timestamp", "min"),
                last_cast_at=("timestamp", "max"),
            )
            .reset_index()
            .rename(columns={"author_fid": "fid"})
        )
    given_df = pd.DataFrame(
        sorted(given.items()), columns=["fid", "reactions_given"]
    )
    merged = posted.merge(given_df, on="fid", how="outer")
    merged["channel_id"] = channel_id
    for column in ("casts_posted", "reactions_received", "replies_received", "reactions_given"):
        merged[column] = pd.to_numeric(merged[column], errors="coerce").fillna(0).astype(int)
    for column in ("first_cast_at", "last_cast_at"):
        merged[column] = merged[column].fillna("")
    merged["fid"] = merged["fid"].astype(int)
    return merged[CHANNEL_SUMMARY_COLUMNS].sort_values(
        ["casts_posted", "reactions_received"], ascending=False
    )


def _plan(brands: pd.DataFrame, window, page_cap: int | None, detail_cap: int | None) -> dict:
    return {
        "brand_accounts": int(len(brands)),
        "brand_fids": [int(f) for f in brands["fid"]],
        "channel_id": ARBITRUM_CHANNEL_ID,
        "query_since": window.query_since.isoformat(),
        "page_cap": page_cap,
        "detail_cap": detail_cap,
        "endpoints": [
            "/v2/farcaster/feed/user/casts",
            "/v2/farcaster/cast/conversation",
            "/v2/farcaster/reactions/cast",
            "/v2/farcaster/cast/search",
            "/v2/farcaster/feed/channels",
        ],
    }


def run(window, args) -> dict:
    """Crawl brand + channel engagement for the window and write one run."""
    brands = load_brand_accounts()
    # `name` collides with the pandas itertuples attribute, so alias it once here.
    brands = brands.rename(columns={"name": "name_"})
    if "name_" not in brands.columns:
        brands["name_"] = ""
    page_cap = args.limit
    detail_cap = args.limit
    plan = _plan(brands, window, page_cap, detail_cap)
    notes: list[str] = []
    writer = RunWriter(DATA_TYPE, dry_run=args.dry_run)

    if args.dry_run:
        logger.info(
            "[dry-run] plan: %d brand account(s) %s, channel /%s, since %s, "
            "page cap %s, detail cap %s",
            plan["brand_accounts"],
            plan["brand_fids"],
            ARBITRUM_CHANNEL_ID,
            window.query_since.isoformat(),
            page_cap,
            detail_cap,
        )
        logger.info(
            "[dry-run] cost is O(brand casts): one /cast/conversation call per brand "
            "cast with replies and one /reactions/cast call per brand cast with "
            "reactions, plus the same for channel casts. No calls made."
        )
        writer.write("brand_engagements", pd.DataFrame(columns=ENGAGEMENT_COLUMNS))
        writer.write("brand_engagement_summary", pd.DataFrame(columns=ENGAGEMENT_SUMMARY_COLUMNS))
        writer.write("channel_casts", pd.DataFrame(columns=CHANNEL_CAST_COLUMNS))
        writer.write("channel_engagement_summary", pd.DataFrame(columns=CHANNEL_SUMMARY_COLUMNS))
        writer.finish(
            params=plan,
            since=window.since,
            new_watermark=None,
            notes=["dry run: nothing fetched, nothing written"],
        )
        return {"dry_run": True, "plan": plan}

    client = NeynarClient()
    timestamps: list[datetime] = []

    if brands.empty:
        notes.append("seeds/brand_accounts.csv is empty; only the channel half ran")
        logger.warning("brand_accounts.csv has no usable rows; skipping the brand half")
        engagement_rows: list[dict] = []
    else:
        engagement_rows, brand_timestamps = _collect_brand_engagement(
            client, brands, window.query_since, page_cap, detail_cap, notes
        )
        timestamps.extend(brand_timestamps)

    events = pd.DataFrame(engagement_rows, columns=ENGAGEMENT_COLUMNS)
    if not events.empty:
        events = events.drop_duplicates(
            subset=["engager_fid", "brand_fid", "engagement_type", "cast_hash", "target_cast_hash"]
        ).sort_values(["brand_fid", "engager_fid", "timestamp"])
    logger.info(
        "brand half: %d engagement events, %d API calls so far",
        len(events),
        client.http.request_count,
    )

    channel_casts_df, channel_summary, channel_timestamps, channel_cast_objects = _collect_channel(
        client, window.query_since, page_cap, detail_cap, notes
    )
    timestamps.extend(channel_timestamps)

    # Channel casts are a free extra source of brand mentions.
    if not brands.empty and channel_cast_objects:
        extra = _mentions_from_casts(
            channel_cast_objects, {int(f) for f in brands["fid"]}, timestamps
        )
        if extra:
            events = pd.concat(
                [events, pd.DataFrame(extra, columns=ENGAGEMENT_COLUMNS)], ignore_index=True
            ).drop_duplicates(
                subset=[
                    "engager_fid",
                    "brand_fid",
                    "engagement_type",
                    "cast_hash",
                    "target_cast_hash",
                ]
            )
            logger.info("channel casts contributed %d additional mention event(s)", len(extra))

    watermark = max(timestamps) if timestamps else None
    window_end = watermark or utc_now()
    summary = _summarise_brand_engagement(events, brands, window.since, window_end)

    writer.write("brand_engagements", events[ENGAGEMENT_COLUMNS])
    writer.write("brand_engagement_summary", summary)
    writer.write("channel_casts", channel_casts_df)
    writer.write("channel_engagement_summary", channel_summary)

    # The graph's ENGAGED_WITH / POSTED_IN / REACTED_IN edges are singletons that
    # ingestion SET-overwrites, so an incremental run's narrow counts would replace
    # a wider run's. Say so here rather than letting the totals quietly shrink.
    notes.append(
        "summaries count only events inside this run's window; the ENGAGED_WITH, "
        "POSTED_IN and REACTED_IN edges are singletons that ingestion overwrites, "
        "so use --backfill when the graph should hold cumulative totals"
    )
    # Likes and replies keep arriving on casts long after they are posted, and a
    # window keyed on cast time stops re-reading them after INCREMENTAL_OVERLAP_DAYS.
    notes.append(
        "late engagement on brand casts older than the window is not re-counted; "
        "a periodic --backfill is what refreshes it"
    )
    plan["neynar_requests"] = client.http.request_count
    notes.append(f"neynar API calls this run: {client.http.request_count}")
    writer.finish(
        params=plan, since=window.since, new_watermark=watermark, notes=notes
    )
    set_watermark(PIPELINE, watermark, run_ts=writer.run_ts)

    logger.info(
        "done: %d engagement events, %d engager-brand pairs, %d channel casts, "
        "%d channel participants, %d neynar calls",
        len(events),
        len(summary),
        len(channel_casts_df),
        len(channel_summary),
        client.http.request_count,
    )
    return {
        "run_ts": writer.run_ts,
        "engagements": int(len(events)),
        "engagement_pairs": int(len(summary)),
        "channel_casts": int(len(channel_casts_df)),
        "channel_participants": int(len(channel_summary)),
        "neynar_requests": client.http.request_count,
        "watermark": _iso(watermark),
    }


def main(argv=None) -> int:
    parser = base_parser(
        PIPELINE,
        "Engagement with the Arbitrum brand accounts and the /arbitrum channel (Neynar only).",
    )
    args = parser.parse_args(argv)
    setup_logging(args.log_level)
    window = resolve_window(args, PIPELINE)
    try:
        run(window, args)
    except SeedMissingError as exc:
        logger.error(
            "%s\n\nThis pipeline cannot guess which accounts count as 'Arbitrum brand'.\n"
            "Write seeds/brand_accounts.csv with a header row 'fid,name,weight', e.g.\n"
            "  fid,name,weight\n"
            "  536359,arbitrum,1.0\n"
            "(fid 536359 is @arbitrum, confirmed live via mentioned_profiles.)",
            exc,
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
