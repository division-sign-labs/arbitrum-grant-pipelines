# arbitrum-grant-pipelines

Eleven pipelines that measure Arbitrum ecosystem activity on Farcaster and load
it into a Neo4j graph. Each pipeline writes timestamped CSVs to `data/`; a
separate ingestion step MERGEs those into the graph. Extract and load are split
so a run is inspectable and re-ingestable without re-fetching.

## Results

| | |
|---|---|
| Farcaster accounts / wallets indexed | 3,345,915 / 5,623,513 |
| **Arbitrum contract deployers** | **2,076 accounts**, 26,050 deployments (333 with score > 0.6) |
| Miniapp builders | 68 seeded → 257 wallets, 95 Arbitrum-active |
| Brand engagement (arbitrum, offchainlabs) | 23,375 events, 9,489 engager–brand pairs |
| /arbitrum channel | 17,916 casts, 15,433 participants |
| Tokens launched (Clanker + Bankr) | 67,670 |
| Token buyers (≥$50) | 189 |
| Evangelists | 573 casts, 29 attributed buys, 273 evangelists |
| Blue-chip positions (ARB/PENDLE/L3/Gauntlet/Uni LP) | 1,683 trades, 1,344 holdings, 84 vault deposits, 268 LP events |
| **Hyperliquid** | **1,088 of 2,323 wallets active (47%), $1.02B lifetime volume** |

Graph totals: 9.06M nodes, 5.79M relationships.

## Setup

Python 3.13. Needs a Dune key, a Neynar key, and a Neo4j instance.

```bash
make install          # venv + deps + .env from .env.example
$EDITOR .env          # DUNE_API_KEY, NEYNAR_API_KEY, NEO4J_URI/USER/PASSWORD
make preflight        # tells you what's missing
```

Two seed files you must supply (header-only templates ship in `seeds/`):

- `seeds/miniapp_builders.csv` — `fid[,username,app_name,app_url]`. Nothing
  on-chain says "shipped a miniapp", so this list has to come from you.
- `seeds/brand_accounts.csv` — `fid,name[,weight]`. The accounts engagement is
  measured against. `536359` is `@arbitrum`, `279472` is `@offchainlabs`.

## Running

```bash
make smoke            # dry-run everything, spends nothing
make constraints      # create Neo4j constraints (idempotent)
make backfill         # full history, pipelines + ingestion
make incremental      # thereafter, watermark-driven

python scripts/run_all.py --list              # the stage plan
python scripts/run_all.py --only clanker_tokens
python -m pipelines.clanker_tokens --backfill # one pipeline
python -m ingestion.ingest_tokens --source clanker
```

Every pipeline takes `--backfill`, `--since ISO`, `--dry-run`, `--limit N`.

`make backfill` is multi-hour: `linked_wallets` is ~33.5k Neynar calls (~2h) and
the Hyperliquid crawl runs at ~24 wallets/min. Both are checkpointed and resume
with `--resume`.

## Pipelines

Run in five stages, because each reads the completed runs of the one before.

| stage | pipeline | source → output |
|---|---|---|
| A | `linked_wallets` | Neynar → `wallets.csv`, `accounts.csv` (the fid ↔ address map everything joins on) |
| B | `contract_deployers` | Dune `arbitrum.creation_traces` → `deployments.csv`, `deployer_activity.csv` |
| B | `miniapp_builders` | seed + Dune → `builder_wallets.csv`, `builder_activity.csv` |
| B | `brand_engagement` | Neynar → `brand_engagements.csv`, `channel_casts.csv`, + summaries |
| B | `clanker_tokens` | Clanker API → `tokens.csv` |
| B | `bankr_tokens` | Dune `robinhood.*` + Bankr API → `tokens.csv`, `token_volume.csv` |
| C | `token_buyers` | Dune `dex.trades` → `buys.csv` |
| C | `popular_tokens` | Dune → `trades.csv`, `holdings.csv`, `vault_deposits.csv`, `lp_events.csv` |
| D | `token_evangelists` | Neynar + Dune → `token_casts.csv`, `attributions.csv`, `evangelist_summary.csv` |
| E | `arb_cohort` | local → `cohort.csv` |
| E | `hyperliquid_activity` | Hyperliquid `/info` → `hl_activity.csv` |

## Data layout

```
data/<data_type>/<run_ts>/*.csv + manifest.json
state/<pipeline>.json          { watermark }
```

A run only counts once `manifest.json` exists, so a crashed run is invisible to
readers. The watermark advances only after that, so a failed run re-reads its
window rather than skipping it. The manifest records row counts, params, and any
leg that degraded.

## Graph schema

Nodes: `WarpcastAccount {fid}`, `Wallet {address}`, `Token {address, chainId}`,
`Contract {address, chainId}`, `Channel {channelId}`, `Chain {chainId}`,
`Platform {name}`.

| relationship | meaning |
|---|---|
| `(WarpcastAccount)-[:ACCOUNT]->(Wallet)` | verified or custody address |
| `(Wallet)-[:DEPLOYED {role}]->(Contract\|Token)` | `role` = `deployer` / `fee_recipient` / `admin` |
| `(WarpcastAccount)-[:CREATED {role}]->(Token)` | launched it, or receives its fees |
| `(Wallet)-[:BOUGHT\|TRADED\|HOLDS\|PROVIDED_LIQUIDITY\|DEPOSITED_IN]->(Token)` | positions |
| `(WarpcastAccount)-[:EVANGELIZED\|POSTED_ABOUT]->(Token)` | shilled it, with attributed USD |
| `(WarpcastAccount)-[:ENGAGED_WITH]->(WarpcastAccount)` | brand engagement, weighted |
| `(WarpcastAccount)-[:POSTED_IN\|REACTED_IN]->(Channel)` | /arbitrum activity |
| `(Wallet)-[:ACTIVE_ON]->(Chain)` / `-[:USED]->(Platform)` | chain activity, Hyperliquid |

Event edges MERGE on a natural key (txHash, castHash); aggregate edges are one
per node pair and get overwritten. Re-ingesting a run is a no-op.

**One gotcha worth knowing: 25,394 wallets are claimed by more than one
account** (one by 98). Per-account aggregates that traverse wallets need
`count(DISTINCT ...)` or they overstate:

```cypher
MATCH (a:WarpcastAccount)-[:ACCOUNT]->(:Wallet)-[:DEPLOYED]->(c:Contract)
RETURN a.username, count(DISTINCT c)   // not count(c)
```

## Notes

- **All social data comes from Neynar**, not Dune — the
  `dune.neynar.dataset_farcaster_*` tables no longer exist. That's why
  `linked_wallets` enumerates fids instead of running one query.
- **Reputation gate is Neynar's 0–1 `score`** (`DEFAULT_MIN_USER_SCORE = 0.6`),
  stored as `neynarScore`.
- **Attribution is a heuristic, not a measurement.** A buy is credited to an
  author only if the buyer engaged with that author's cast about the token
  within 5 days before buying, and each buy is split equally among qualifying
  authors.
- **`popular_tokens` currently covers the ~30k-wallet Arbitrum cohort**, not all
  4.7M Farcaster wallets. `--wallet-source linked_wallets` widens it at
  considerably more Dune spend. Recorded on the run manifest.
- **Robinhood Chain is opt-in** (`--chains robinhood`). It carries 67k Bankr
  tokens but only ~1% of those wallets are Farcaster accounts, so it costs a lot
  and returns little.

Deeper rationale — the Bankr fee-recipient decode, Dune's caps, Neynar's
batching limits — lives in the docstrings of the modules that deal with it.

## Testing

```bash
make test     # 486 offline unit tests
make smoke    # every pipeline dry-run, spends nothing
```
