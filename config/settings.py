"""Central configuration. Every tunable the pipelines share lives here."""

import os
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent

load_dotenv(REPO_ROOT / ".env")

# --- filesystem layout ---------------------------------------------------
DATA_DIR = Path(os.environ.get("DATA_DIR", REPO_ROOT / "data"))
STATE_DIR = Path(os.environ.get("STATE_DIR", REPO_ROOT / "state"))
SEEDS_DIR = Path(os.environ.get("SEEDS_DIR", REPO_ROOT / "seeds"))

# --- credentials ---------------------------------------------------------
DUNE_API_KEY = os.environ.get("DUNE_API_KEY")
NEYNAR_API_KEY = os.environ.get("NEYNAR_API_KEY")
NEO4J_URI = os.environ.get("NEO4J_URI")
# Sibling repos use NEO4J_USERNAME; this repo's .env uses NEO4J_USER. Accept both.
NEO4J_USERNAME = os.environ.get("NEO4J_USERNAME") or os.environ.get("NEO4J_USER") or "neo4j"
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD")
NEO4J_DATABASE = os.environ.get("NEO4J_DATABASE", "neo4j")

# --- chains --------------------------------------------------------------
CHAIN_ARBITRUM = 42161
# Robinhood Chain: Arbitrum Orbit L2, mainnet 2026-07-01. Bankr's default deploy target.
CHAIN_ROBINHOOD = 4663

# Dune's `blockchain` column values for the chains we touch.
DUNE_CHAIN_NAMES = {CHAIN_ARBITRUM: "arbitrum", CHAIN_ROBINHOOD: "robinhood"}

# --- time windows --------------------------------------------------------
# 18 months of history is the grant's stated analysis window; round down to a
# clean date so backfills are reproducible rather than relative-to-today.
BACKFILL_START = os.environ.get("BACKFILL_START", "2025-01-01")

# Dune's neynar datasets sync roughly daily and chain tables lag the tip, so an
# incremental run re-reads one extra day. Ingestion MERGEs are idempotent, so
# the overlap collapses instead of double-counting.
INCREMENTAL_OVERLAP_DAYS = 1

# --- thresholds (from the grant spec) ------------------------------------
MIN_BUY_USD = 50.0
EVANGELIST_MIN_VOLUME_USD = 50_000.0
ATTRIBUTION_WINDOW_DAYS = 5

# The reputation gate. The plan called for the quotient score (earlySummerNorm),
# but the target Neo4j is empty and Dune no longer carries the Farcaster social
# graph, so there is nothing to compute it from here. Neynar returns its own
# 0-1 user score on every profile, which is the same shape and same intent, so
# that is what the cohort filter uses. Swap the property name here if the
# quotient score later lands in the graph.
DEFAULT_MIN_USER_SCORE = 0.6
USER_SCORE_PROPERTY = "neynarScore"

# --- engagement weighting ------------------------------------------------
# A reply costs more than a like, so it counts for more. Multiplied by the
# per-account weight in seeds/brand_accounts.csv.
ENGAGEMENT_WEIGHTS = {
    "reply": 3.0,
    "recast": 2.0,
    "mention": 2.0,
    "like": 1.0,
}

ARBITRUM_CHANNEL_ID = "arbitrum"
# Verified against the Neynar channel endpoint: both `parent_url` and
# `root_parent_url` on /arbitrum casts are this exact string.
ARBITRUM_CHANNEL_URLS = ["https://warpcast.com/~/channel/arbitrum"]

# --- API endpoints -------------------------------------------------------
DUNE_API_BASE = "https://api.dune.com/api/v1"
NEYNAR_API_BASE = "https://api.neynar.com"
CLANKER_API_BASE = "https://www.clanker.world/api"
BANKR_API_BASE = "https://api.bankr.bot"
HYPERLIQUID_API_URL = "https://api.hyperliquid.xyz/info"

# --- rate / cost knobs ---------------------------------------------------
DUNE_PERFORMANCE = os.environ.get("DUNE_PERFORMANCE", "medium")
DUNE_POLL_SECONDS = 10
DUNE_MAX_WAIT_SECONDS = 3600
DUNE_RESULT_PAGE_SIZE = 30_000

NEYNAR_REQUESTS_PER_SECOND = 4.0  # Starter is 300 rpm; leave headroom.
NEYNAR_FID_BATCH = 100  # /user/bulk caps at 100 fids per call.
# /user/bulk-by-address is a GET: ~350 addresses blows past the URI length limit
# (verified — 400 returns 414), so keep the batch small enough to fit the URL.
NEYNAR_ADDRESS_BATCH = 100
# Highest fid observed: between 3.0M and 3.5M as of 2026-08. The enumeration
# walks past the tip and stops after a run of empty batches, so this is only a
# starting hint, not a hard ceiling.
FID_SCAN_CEILING = 3_600_000
FID_SCAN_EMPTY_BATCH_STOP = 25

# Dune only permits PUBLIC uploads on this account, so joins that would need a
# private wallet table stay client-side unless explicitly opted in.
DUNE_UPLOAD_ENABLED = os.environ.get("DUNE_UPLOAD_ENABLED", "false").lower() == "true"
DUNE_UPLOAD_NAMESPACE = os.environ.get("DUNE_UPLOAD_NAMESPACE", "amphiboly")

# Hyperliquid allows 1200 weight/min/IP and most /info payloads cost 20, so the
# ceiling is 60 calls/min. We use two calls per wallet and stay under the line.
HYPERLIQUID_WEIGHT_PER_MINUTE = 1200
HYPERLIQUID_CALL_WEIGHT = 20
HYPERLIQUID_SAFETY_FACTOR = 0.8

NEO4J_BATCH_SIZE = 1000

PROVENANCE = "arbitrum-grant-pipelines"


def require(name: str) -> str:
    """Fetch a required credential, failing with an actionable message."""
    value = globals().get(name)
    if not value:
        raise RuntimeError(
            f"{name} is not set. Copy .env.example to .env and fill it in."
        )
    return value
