# arbitrum-grant-pipelines

Data pipelines that measure Arbitrum ecosystem activity on Farcaster, and load the
result into a Neo4j property graph. Built as an Arbitrum grant deliverable.

The question the graph is built to answer is "who, on Farcaster, is actually doing
something on Arbitrum" — deploying contracts, shipping miniapps, launching tokens,
buying them, holding blue chips, providing liquidity, talking about the ecosystem,
and (as a cross-chain control) trading on Hyperliquid. Eleven pipelines each pull
one slice of that from the source that genuinely has it — Dune for chain data, the
Neynar API for everything social, Clanker/Bankr/Hyperliquid for their own
registries — write timestamped CSV runs to disk, and a separate ingestion layer
MERGEs those runs into the graph. Extraction and loading are deliberately split:
a CSV run is inspectable, diffable, cheap to re-ingest, and survives a Neo4j that
is down, misconfigured, or being rebuilt.

---

## What you need to provide

Three things, and nothing else. Everything below is checked by `make preflight`,
which names whatever is missing and exits non-zero.

**1. Credentials** — copy `.env.example` to `.env` and fill in:

| variable | why |
|---|---|
| `DUNE_API_KEY` | every on-chain pipeline (Arbitrum, Robinhood, dex trades) |
| `NEYNAR_API_KEY` | every social pipeline — Dune has no Farcaster data (see [Limitations](#data-source-notes--limitations)) |
| `NEO4J_URI`, `NEO4J_USERNAME`/`NEO4J_USER`, `NEO4J_PASSWORD` | the ingestion target |

**2. `seeds/miniapp_builders.csv`** — the Farcaster accounts that shipped a
miniapp integrating Arbitrum. Nothing on-chain or in any API says "this account
shipped a miniapp", so this list has to come from you. Only `fid` is required:

```csv
fid,username,app_name,app_url
12345,somebuilder,Some Arbitrum Miniapp,https://example.xyz
```

**3. `seeds/brand_accounts.csv`** — the accounts that count as "Arbitrum brand",
whose engagement is measured (Arbitrum, Offchain Labs, and whoever else you
consider in scope). `weight` scales that account's contribution and defaults to
`1.0`:

```csv
fid,name,weight
536359,arbitrum,1.0
```

`536359` is the real `@arbitrum` account (36k followers, verified via the lookup
below on 2026-08-09) — a starting point, not a complete list. Add Offchain Labs
and whichever ecosystem accounts you consider in scope.

To look up an fid from a handle:

```bash
curl -s "https://api.neynar.com/v2/farcaster/user/search?q=arbitrum&limit=5" \
  -H "x-api-key: $NEYNAR_API_KEY" | python3 -m json.tool
```

Both seed files ship as **header-only templates**. The repo runs without them —
`miniapp_builders` and `brand_engagement` exit with an actionable message rather
than silently writing empty output — but those two pipelines cannot produce
anything until you fill them in. Every other pipeline is unaffected.

---

## Architecture

```
  sources                     pipelines                  runs on disk              graph
  ----------------------      --------------------       ---------------------     -------------

  Dune (Trino SQL)      ─┐                          ┌─ data/<data_type>/
    arbitrum.*           │                          │     <run_ts>/
    dex.trades           ├──▶  pipelines/*.py  ──▶  │       *.csv          ──▶  ingestion/*.py ──▶ Neo4j
    erc20_arbitrum.*     │     (extract +           │       manifest.json          (MERGE, batched
    robinhood.*          │      normalise +         │                                UNWIND writes)
    uniswap_v*_arbitrum  │      local pandas        └─ state/<pipeline>.json
                         │      joins)                    { watermark }
  Neynar REST API       ─┤                                      ▲
    users / casts        │                                      │
    reactions / channels │                                      └── advanced only after
                         │                                          a run is sealed
  Clanker API           ─┤
  Bankr API             ─┤
  Hyperliquid /info     ─┘

  seeds/*.csv  ──▶ (miniapp_builders, brand_engagement)
```

Two rules hold the whole thing together:

1. **A run is only real once its `manifest.json` exists.** A pipeline that dies
   half-way leaves a directory behind, but `lib.runs.latest_run()` ignores it, so
   downstream pipelines and ingestion never read partial data.
2. **The watermark advances only after the manifest is written.** A crashed run
   therefore re-reads the same window next time, which is safe because every
   ingestion write is a MERGE on a natural key.

---

## Setup

Requires **Python 3.13**, a Dune API key, a Neynar API key (Starter, 300 rpm is
enough), and a Neo4j instance you can write to.

```bash
git clone <this repo> && cd arbitrum-grant-pipelines
make install            # creates .venv, installs requirements.txt, copies .env.example -> .env
$EDITOR .env            # DUNE_API_KEY, NEYNAR_API_KEY, NEO4J_URI/USERNAME/PASSWORD/DATABASE
```

Or by hand:

```bash
python3.13 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
```

Every command below assumes the repo root as the working directory and
`.venv/bin/python` as the interpreter — the pipelines are run as modules
(`python -m pipelines.<name>`) and rely on the repo root being on `sys.path`.

### Seed files

See [What you need to provide](#what-you-need-to-provide) above for the schemas
and worked examples, `seeds/README.md` for the full column semantics, and
`make preflight` to check them. `scripts/run_all.py` also warns at startup for
any seed still in template form.

---

## Quickstart

```bash
make smoke                  # every pipeline (bar the HL crawl), --dry-run --limit 50: spends nothing
make constraints            # create the Neo4j constraints and indexes (idempotent)
make backfill               # full history from BACKFILL_START (2025-01-01), pipelines + ingestion
make incremental            # thereafter: watermark-driven update, pipelines + ingestion
```

`make smoke` is the first thing to run against a fresh checkout: it proves every
module imports, every CLI parses, every Dune query renders and every
seed/credential is where it should be. No Dune execution is submitted and no CSV
is written; the only network traffic is a handful of pages from the free,
unauthenticated Clanker and Bankr endpoints.

`make backfill` is a multi-hour job — `linked_wallets` alone is ~33.5k Neynar calls
(~2h) and the `popular_tokens` trade leg scans ~19M `dex.trades` rows. Read
[Operations](#operations) before starting one.

Everything is also runnable one piece at a time:

```bash
.venv/bin/python scripts/run_all.py --list                     # the plan, and the ingestion module each step resolves to
.venv/bin/python scripts/run_all.py --only clanker_tokens      # one pipeline + its ingestion
.venv/bin/python scripts/run_all.py --from-stage C --backfill  # resume a failed schedule
.venv/bin/python -m pipelines.clanker_tokens --backfill        # the pipeline alone
.venv/bin/python -m ingestion.ingest_tokens --source clanker   # load its latest completed run
```

---

## Pipeline reference

The orchestrator runs these in five stages, because each stage reads the completed
runs of the one before it:

| stage | pipelines | why here |
|---|---|---|
| A | `linked_wallets` | the fid → wallet map every other join keys off |
| B | `contract_deployers`, `miniapp_builders`, `brand_engagement`, `clanker_tokens`, `bankr_tokens` | need A (or a seed) and nothing else |
| C | `token_buyers`, `popular_tokens` | trade against the token registries B builds |
| D | `token_evangelists` | attributes C's buys to the accounts that shilled them |
| E | `arb_cohort`, `hyperliquid_activity` | the cohort aggregates every run above; the crawl walks that cohort |

| pipeline | source | output under `data/` | notes |
|---|---|---|---|
| `linked_wallets` | Neynar `/v2/farcaster/user/bulk`, fids enumerated 1 → tip | `linked_wallets/wallets.csv`, `accounts.csv` | The base table. ~3.35M fids at 100/call ≈ 33.5k calls ≈ 2h. Resumable (`--resume`); incremental mode only scans newly allocated fids, so a profile that changes is refreshed only by a fresh `--backfill`. |
| `contract_deployers` | Dune `arbitrum.creation_traces` + `arbitrum.transactions`, intersected with A locally | `contract_deployers/deployments.csv`, `deployer_activity.csv` | Creation traces catch factory-created contracts that never appear as a `to = null` transaction. Deployments are windowed; `deployer_activity` is recomputed from `BACKFILL_START` because it feeds a singleton `ACTIVE_ON` edge. |
| `miniapp_builders` | `seeds/miniapp_builders.csv` → Neynar → Dune `arbitrum.transactions` | `miniapp_builders_activity/builder_wallets.csv`, `builder_activity.csv` | Seed-driven: nothing on-chain says "shipped a miniapp". Wallets come from the local A map first; only unknown fids cost a Neynar call. |
| `brand_engagement` | Neynar feeds, `/cast/conversation`, `/reactions/cast`, `/cast/search`, `/feed/channels` + `seeds/brand_accounts.csv` | `brand_engagement/brand_engagements.csv`, `brand_engagement_summary.csv`, `channel_casts.csv`, `channel_engagement_summary.csv` | 100% HTTP — the one pipeline whose cost is API calls, not Dune credits. Weights: reply 3, recast 2, mention 2, like 1, multiplied by the per-account seed weight. |
| `clanker_tokens` | clanker.world API, `chainId=42161` | `clanker_tokens/tokens.csv` | 565 Arbitrum tokens over ~29 pages. ~61% carry the requesting fid — the token → Farcaster edge exists only in Clanker's own metadata. The admin (the v4 fee and reward owner) differs from the deployer on 140 of them and is resolved to `fee_recipient_fid` separately; it covers 425 tokens where the recorded fid covers 335. |
| `bankr_tokens` | Dune `robinhood.creation_traces` / `robinhood.logs` for history, Bankr API for the fresh tail | `bankr_tokens/tokens.csv`, `token_volume.csv` | Launches arrive as ERC-4337 user operations, so the tx sender is a bundler, not the launcher; attribution comes from the creation trace (see `sql/robinhood.py`). Dune wins on conflict, the API fills in what Dune has not indexed. `fee_recipient_address` is decoded from the Doppler beneficiary arrays and covers ~99% of launches; it is a different wallet from the deployer about two thirds of the time. |
| `token_buyers` | Dune `dex.trades`, filtered to the stage-B registries | `token_buyers/buys.csv` | ≥ `MIN_BUY_USD` ($50). Emits both `taker` and `tx_from` and keeps whichever resolves to an fid — on Clanker's Uniswap v4 hook pools `taker` is usually the router, not the human. |
| `popular_tokens` | Dune `dex.trades`, `erc20_arbitrum.evt_transfer`, `arbitrum.logs`, `uniswap_v3_arbitrum.*` | `popular_tokens/trades.csv`, `holdings.csv`, `vault_deposits.csv`, `lp_events.csv` | Four independent legs over the `config/tokens.py` index (ARB, PENDLE, L3 + four Gauntlet MetaMorpho vaults). A failed leg writes a correctly-shaped empty CSV, notes it on the manifest, and blocks the watermark rather than the run. |
| `token_evangelists` | Neynar `/cast/search` + `/reactions/cast`, Dune `dex.trades` | `token_evangelists/token_casts.csv`, `attributions.csv`, `evangelist_summary.csv` | A buy counts for an author only if the buyer engaged with (or wrote) that author's cast about the token within `ATTRIBUTION_WINDOW_DAYS` (5). Each buy's USD is split equally across qualifying authors, so ten shills cannot each claim the whole trade. Token gate: ≥ `EVANGELIST_MIN_VOLUME_USD` ($50k) lifetime DEX volume. |
| `arb_cohort` | every completed run above, read locally | `arb_cohort/cohort.csv` | No credits, no API calls, no watermark. One row per wallet with `sources` (all pipelines that saw it) and `priority` (the most specific one: 1 deployers … 6 popular-token traders). |
| `hyperliquid_activity` | Hyperliquid `/info` (`userRateLimit`, `userNonFundingLedgerUpdates`) | `hyperliquid_activity/hl_activity.csv` | Two calls per wallet, paced to ~24 wallets/min, so a 10k cohort is ~7h. Rows are flushed as they arrive; `--resume` re-opens the unsealed run, `--recheck-days` copies fresh wallets forward instead of re-fetching them. |

Every pipeline accepts `--backfill`, `--since`, `--dry-run`, `--limit` and
`--log-level`; several add their own cost controls (`--max-tokens`,
`--trade-chunk-days`, `--max-priority`, `--recheck-days`, …). Run
`python -m pipelines.<name> --help` for the full surface.

---

## Data layout

```
data/<data_type>/<run_ts>/            run_ts is UTC, e.g. 20260810T055405Z
    *.csv                             the contract below
    manifest.json                     written last; a run without it is invisible
data/.dune_cache/                     24h SQL-keyed result cache (safe to delete)
state/<pipeline>.json                 { watermark, last_run_ts, updated_at }
```

`manifest.json` records `files` (name → row count), `row_total`, the run `params`,
the `since` the run covered, the `new_watermark` it produced, and any `notes` a
degraded leg left behind. It is the audit trail for the run — read it before
trusting a CSV that looks smaller than expected.

CSV columns are a contract between the pipelines and ingestion, so they are fixed:

| file | columns |
|---|---|
| `linked_wallets/wallets.csv` | `fid,address,protocol,is_primary,source` |
| `linked_wallets/accounts.csv` | `fid,username,display_name,neynar_score,follower_count,following_count,custody_address,registered_at` |
| `contract_deployers/deployments.csv` | `fid,deployer_address,contract_address,chain_id,deployed_at,tx_hash,deploy_method` |
| `contract_deployers/deployer_activity.csv` | `fid,address,chain_id,tx_count,first_tx_at,last_tx_at` |
| `miniapp_builders_activity/builder_wallets.csv` | `fid,address` |
| `miniapp_builders_activity/builder_activity.csv` | `fid,address,chain_id,tx_count,first_tx_at,last_tx_at` |
| `brand_engagement/brand_engagements.csv` | `engager_fid,brand_fid,engagement_type,cast_hash,target_cast_hash,timestamp` |
| `brand_engagement/brand_engagement_summary.csv` | `engager_fid,brand_fid,replies,likes,recasts,mentions,weighted_score,window_start,window_end` |
| `brand_engagement/channel_casts.csv` | `cast_hash,author_fid,channel_id,timestamp,parent_hash,likes_count,recasts_count,replies_count,text_length` |
| `brand_engagement/channel_engagement_summary.csv` | `fid,channel_id,casts_posted,reactions_received,replies_received,reactions_given,first_cast_at,last_cast_at` |
| `clanker_tokens/tokens.csv` | `token_address,chain_id,platform,deployer_address,admin_address,fid,fee_recipient_fid,username,name,symbol,deployed_at,tx_hash,pool_address,paired_token,token_type,starting_market_cap_usd,price_usd,market_cap_usd,volume_24h_usd` |
| `bankr_tokens/tokens.csv` | `token_address,chain_id,platform,deployer_address,fee_recipient_address,fid,fee_recipient_fid,name,symbol,deployed_at,tx_hash,pool_address,launch_type,source` |
| `bankr_tokens/token_volume.csv` | `token_address,chain_id,day,swap_count,volume_native,volume_usd` |
| `token_buyers/buys.csv` | `fid,buyer_address,token_address,chain_id,platform,amount_usd,token_amount,block_time,tx_hash` |
| `token_evangelists/token_casts.csv` | `token_address,chain_id,cast_hash,author_fid,timestamp,matched_on,likes_count,recasts_count` |
| `token_evangelists/attributions.csv` | `token_address,chain_id,author_fid,buyer_fid,buyer_address,tx_hash,amount_usd,block_time,attributed_usd,n_influencers` |
| `token_evangelists/evangelist_summary.csv` | `token_address,chain_id,author_fid,cast_count,unique_buyers_influenced,total_purchases,total_purchase_volume_usd,attributed_usd` |
| `popular_tokens/trades.csv` | `fid,address,token_address,chain_id,side,amount_usd,token_amount,block_time,tx_hash` |
| `popular_tokens/holdings.csv` | `fid,address,token_address,chain_id,balance,balance_raw,last_activity_at` |
| `popular_tokens/vault_deposits.csv` | `fid,address,vault_address,chain_id,assets,assets_raw,shares_raw,block_time,tx_hash` |
| `popular_tokens/lp_events.csv` | `fid,address,pool_address,token_address,chain_id,event,amount0,amount1,block_time,tx_hash` |
| `arb_cohort/cohort.csv` | `address,fid,sources,priority,neynar_score` |
| `hyperliquid_activity/hl_activity.csv` | `address,fid,has_hl_activity,cum_volume_usd,first_activity_at,ledger_event_count,checked_at` |

`fee_recipient_fid` means the same thing on both launchpads — the Farcaster
account behind the wallet that is paid the token's fees — but it is resolved
from a different column on each, because the platforms name that wallet
differently: `admin_address` on Clanker, `fee_recipient_address` on Bankr.
Clanker's `fid` stays what Clanker recorded (the account that ordered the
launch) and is never overwritten by a wallet lookup.

`data/` and `state/` are git-ignored: they are local artifacts, reproducible from
the sources.

---

## Graph schema

Ingestion writes into Neo4j with batched `UNWIND … MERGE`. Every node has a MERGE
key, so re-ingesting the same run is a no-op rather than a duplicate.

### Nodes

| label | MERGE key | notes |
|---|---|---|
| `WarpcastAccount` | `fid` | plus `username`, `displayName`, `neynarScore`, `followerCount`, `custodyAddress`, `registeredAt` |
| `Wallet` | `address` | always lowercase hex |
| `Token` | `(address, chainId)` | Clanker/Bankr launches, index tokens, and vaults (`kind:'vault'`) |
| `Contract` | `(address, chainId)` | deployed contracts |
| `Channel` | `channelId` | `arbitrum` |
| `Chain` | `chainId` | 42161 Arbitrum, 4663 Robinhood Chain |
| `Platform` | `name` | `hyperliquid` |

`cypher/schema.py` backs every one of those keys with a uniqueness
constraint — without it a MERGE inside a large `UNWIND` degrades to a label scan
per row — and tries `IS UNIQUE` before `IS NODE KEY` so the schema installs on
Community and Aura Free as well as Enterprise. It also adds non-key lookup
indexes on `Token(address)`, `Contract(address)` and `WarpcastAccount(username)`.
`ingestion/constraints.py` is what applies that DDL to a target.

### Relationships

Event edges MERGE on a natural key (`txHash` / `castHash`) and accumulate.
Aggregate edges are **singletons** per node pair: they are recomputed from a full
window each run and SET-overwritten, which is why the pipelines that feed them
recompute from `BACKFILL_START` rather than from the watermark.

| relationship | properties | kind |
|---|---|---|
| `(WarpcastAccount)-[:ACCOUNT]->(Wallet)` | `isPrimary`, `protocol` | edge per verified address |
| `(Wallet)-[:DEPLOYED]->(Contract)` | `txHash`, `deployedAt`, `method` | event (`txHash`) |
| `(Wallet)-[:DEPLOYED]->(Token)` | `role`, `txHash`, `deployedAt`, `platform`, `asOf` | singleton |
| `(WarpcastAccount)-[:CREATED]->(Token)` | `role`, `platform`, `deployedAt`, `asOf` | singleton |
| `(Wallet)-[:BOUGHT]->(Token)` | `txHash`, `usd`, `timestamp` | event (`txHash`) |
| `(Wallet)-[:TRADED]->(Token)` | `txHash`, `side`, `usd`, `timestamp` | event (`txHash`) |
| `(Wallet)-[:HOLDS]->(Token)` | `balance`, `lastActivityAt`, `asOf` | singleton |
| `(Wallet)-[:DEPOSITED_IN]->(Token {kind:'vault'})` | `txHash`, `assets`, `timestamp` | event (`txHash`) |
| `(Wallet)-[:PROVIDED_LIQUIDITY]->(Token)` | `txHash`, `event`, `timestamp` | event (`txHash`) |
| `(WarpcastAccount)-[:POSTED_ABOUT]->(Token)` | `castHash`, `timestamp`, `matchedOn` | event (`castHash`) |
| `(WarpcastAccount)-[:EVANGELIZED]->(Token)` | `castCount`, `attributedUsd`, `uniqueBuyers`, `asOf` | singleton |
| `(WarpcastAccount)-[:ENGAGED_WITH]->(WarpcastAccount)` | `replies`, `likes`, `recasts`, `mentions`, `weightedScore`, `windowStart`, `asOf` | singleton |
| `(WarpcastAccount)-[:POSTED_IN]->(Channel)` | `castCount`, `firstAt`, `lastAt`, `asOf` | singleton |
| `(WarpcastAccount)-[:REACTED_IN]->(Channel)` | `given`, `received`, `asOf` | singleton |
| `(Wallet)-[:ACTIVE_ON]->(Chain)` | `txCount`, `firstTxAt`, `lastTxAt`, `asOf` | singleton |
| `(Wallet)-[:USED]->(Platform {name:'hyperliquid'})` | `volumeUsd`, `firstActivityAt`, `checkedAt` | singleton |

#### `role` on a token launch

A launch has two wallets worth knowing about, and they are frequently different
people. The deployer can be a bot, a factory or an ERC-4337 smart account; the
address that receives the token's fees is usually an EOA the human actually
controls, and therefore the one a Farcaster verification is likely to point at.
So both get the same two edges, told apart by `role`:

| `role` | who | source column |
|---|---|---|
| `deployer` | the deploying address (and, on `CREATED`, the fid the launchpad recorded) | `deployer_address` / `fid` |
| `fee_recipient` | Bankr: the launcher's entry in the Doppler fee split | `fee_recipient_address` / `fee_recipient_fid` |
| `admin` | Clanker: the token's fee and reward owner | `admin_address` / `fee_recipient_fid` |

`role` is a property, **not** part of the MERGE key — the key stays the node
pair, as for every other singleton. That works because the fee-recipient write
is skipped whenever it would target the same wallet (or the same fid) as the
deployer write, so each pair is touched by exactly one branch of exactly one
row. One address holding both roles therefore yields exactly one edge, labelled
`deployer`, and a re-run adopts the edge it wrote last time instead of
duplicating it. Current shape of the graph: 565 `deployer` + 140 `admin`
DEPLOYED edges for Clanker, 337 `deployer` + 223 `fee_recipient` for a Bankr
window of 337 tokens.

---

## Operations

### The orchestrator

`scripts/run_all.py` runs each pipeline as a subprocess, then its ingestion module,
in stage order. Subprocesses mean one failure cannot poison the rest of the
schedule, and child output is inherited rather than captured so a long crawl still
narrates itself.

| flag | effect |
|---|---|
| `--backfill` / `--since T` / `--dry-run` / `--limit N` | fanned out to every pipeline |
| `--batch-size N` | fanned out to every ingestion module |
| `--only a,b` | run just these pipelines (names or data-type directory names) |
| `--from-stage C` | skip every stage before C — how you resume a failed schedule |
| `--skip-ingest` | pipelines only; no constraints, no Neo4j writes |
| `--continue-on-error` | keep going after a failure (the exit code is still non-zero) |
| `--list` | print the plan, and which ingestion module each step resolves to |

It prints a stage/step/status/duration table at the end and exits non-zero if
anything failed, with the exact command to retry the failing step and the
`--from-stage` invocation to resume the rest. `ingestion.constraints` runs once
at the top of the schedule (unless `--skip-ingest`), and each ingestion step is
then invoked with `--no-constraints` so the DDL pass is not repeated per data
type.

### Cadence

* **Backfill once**, then run `make incremental` on a schedule. Daily is a
  reasonable default: `INCREMENTAL_OVERLAP_DAYS = 1` means every incremental run
  re-reads one extra day to absorb Dune's indexing lag, and MERGE collapses the
  duplicates.
* `linked_wallets` incremental only scans newly allocated fids. Existing profiles
  (new verified wallets, changed username, moved score) are refreshed only by a
  fresh `--backfill` — roughly a two-hour job, worth running monthly.
* `arb_cohort` has no watermark; regenerate it whenever an upstream pipeline has
  produced a newer run, and always before a Hyperliquid crawl.

### Resuming the Hyperliquid crawl

The crawl is ~24 wallets/min and flushes every wallet to CSV as it lands, so an
interrupted run is a resumable one, not a lost one:

```bash
.venv/bin/python -m pipelines.hyperliquid_activity --resume          # continue the newest unsealed run
.venv/bin/python -m pipelines.hyperliquid_activity --max-priority 2  # builders only: a short run that still covers the point
.venv/bin/python -m pipelines.hyperliquid_activity --recheck-days 30 # copy wallets checked in the last 30d forward instead of re-fetching
```

An unsealed run directory has no `manifest.json`, so it stays invisible to
ingestion until it finishes. `state/hyperliquid_activity.json` records where the
in-flight run got to.

`linked_wallets --resume` behaves the same way for the fid scan.

### Re-running one stage or one pipeline

```bash
.venv/bin/python scripts/run_all.py --from-stage C --backfill      # stage C onward
.venv/bin/python scripts/run_all.py --only popular_tokens          # one pipeline + its ingestion
.venv/bin/python -m ingestion.ingest_popular_tokens --run-id 20260810T055405Z   # re-ingest a specific run
```

Re-ingesting an old run is always safe — every write is a MERGE — but note that
singleton aggregate edges are overwritten by whatever run you ingest last, so
re-ingest the *newest* run afterwards if you replay history.

Ingestion modules do not map one-to-one onto pipeline names; `make plan` prints
the current resolution, and it is this:

| pipeline | ingestion |
|---|---|
| `linked_wallets` | `ingestion.ingest_linked_wallets` |
| `contract_deployers` | `ingestion.ingest_contract_deployers` |
| `miniapp_builders` | `ingestion.ingest_miniapp_builders` |
| `brand_engagement` | `ingestion.ingest_brand_engagement` |
| `clanker_tokens` | `ingestion.ingest_tokens --source clanker` |
| `bankr_tokens` | `ingestion.ingest_tokens --source bankr` |
| `token_buyers` | `ingestion.ingest_token_buyers` |
| `popular_tokens` | `ingestion.ingest_popular_tokens` |
| `token_evangelists` | `ingestion.ingest_token_evangelists` |
| `arb_cohort` | none — it is a driver for the crawl, and its wallets are already in the graph from the runs it aggregates |
| `hyperliquid_activity` | `ingestion.ingest_hyperliquid` |

### When a Dune query starts returning nothing

Upstream datasets drift. `scripts/probe_schemas.py` checks that the tables and
columns the SQL assumes still exist, using metadata-only queries that cost
nothing:

```bash
.venv/bin/python -m scripts.probe_schemas
.venv/bin/python -m scripts.probe_schemas --tables robinhood.logs dex.trades
```

Dune results are cached under `data/.dune_cache` for 24h keyed by SQL text, so an
edit-and-rerun loop is free until the SQL actually changes. `make clean` drops it.

### Cost control

* `--dry-run` renders every query and plan and spends nothing — always the first
  thing to run against a changed pipeline.
* `--limit N` caps rows and pages everywhere. Useful for smoke tests; note that a
  truncated deployer roll-up usually intersects zero Farcaster wallets, so limited
  runs legitimately produce empty output.
* `popular_tokens` refuses to run the holdings leg if it would need more than
  `--max-holdings-chunks` Dune executions, instead of quietly spending a fortune.
* `token_evangelists` gates on token volume and caps `--max-tokens` (default 25),
  because its cost scales with Neynar calls per token.

---

## Testing

```bash
make test                    # pytest over tests/
make smoke                   # every pipeline bar the HL crawl, --dry-run --limit 50
make plan                    # the stage plan, and which ingestion module each step resolves to
.venv/bin/python -m scripts.probe_schemas   # confirm the Dune tables/columns still exist
```

Unit tests belong in `tests/` and target the pure functions — SQL literal
building (`lib/sqlfmt.py`), window resolution, watermark monotonicity,
run/manifest handling, and each pipeline's normalisation of a recorded API
payload. Anything that would hit the network should be mocked with
`requests-mock` (already in `requirements.txt`) rather than skipped. `make test`
reports "no tests collected" rather than failing when `tests/` is empty.

`make smoke` is the integration check that costs nothing: it runs each pipeline
with `--dry-run --limit 50`, which exercises imports, CLI parsing, SQL rendering,
seed loading and credential presence end to end. `hyperliquid_activity` is the
one pipeline it cannot cover — it reads a *sealed* `arb_cohort` run and a dry run
seals nothing, so smoke it separately once a real cohort exists:
`make smoke ARGS="--only hyperliquid_activity"`.

---

## Data source notes & limitations

These are verified constraints discovered while building this, not assumptions.
They explain several design choices that would otherwise look arbitrary.

* **Dune has no Farcaster data.** The `dune.neynar.dataset_farcaster_*` tables do
  not exist with this API key — all six were probed and all six fail — and there is
  no other Farcaster casts / reactions / verifications table on Dune. **Every piece
  of social data in this repo therefore comes from the Neynar REST API**, which is
  why `linked_wallets` is a two-hour enumeration instead of one SQL query, and why
  `brand_engagement` and `token_evangelists` are HTTP-bound rather than
  credit-bound. Any reference implementation that joins casts to trades in a single
  Dune query cannot be reproduced here.
* **The Neo4j graph is built from scratch.** The target database was empty (0 nodes,
  no constraints) when this was written. Nothing here assumes pre-existing nodes;
  ingestion creates its own constraints and MERGEs every node it needs.
* **Neynar's score substitutes for the quotient score.** The original spec gated on
  a quotient reputation score, but there is nothing in the empty graph to compute it
  from and Dune no longer carries the Farcaster social graph. Neynar returns a 0–1
  `score` on every profile — same shape, same intent — so that is the gate:
  `DEFAULT_MIN_USER_SCORE = 0.6`, stored on the graph as `neynarScore`
  (`USER_SCORE_PROPERTY`). Change those two constants if a real quotient score later
  lands in the graph.
* **Bankr's API only exposes the 50 most recent launches.** It honours no
  pagination parameter — `limit`, `offset`, `page` and `cursor` are all ignored
  (verified). Those 50 records cover well under an hour of launches, so the API
  cannot be the registry. Historical Bankr data comes from Dune's `robinhood.*`
  schema (Robinhood Chain is an Arbitrum Orbit L2, chainId 4663, mainnet since
  2026-07-01); the API is used only for the freshest tail, where Dune's indexing
  lag is real — at probe time only 39 of the 47 most recent launches had landed on
  Dune.
* **Dune uploads on this account are public-only**, so the Farcaster wallet set is
  never shipped to Dune. Every fid ↔ address join happens locally in pandas: the
  pipeline pulls the chain-side aggregate (e.g. all 373k distinct Arbitrum deployers
  since 2025-01-01), intersects it here, and only then queries detail for the
  matched addresses. `lib/dune_upload.py` exists but is opt-in behind
  `DUNE_UPLOAD_ENABLED` (default false) and should stay off unless the account gains
  private tables.
* **Neynar batching limits are hard, not tunable.** `/user/bulk` takes 100 fids per
  call. `/user/bulk-by-address` is a GET and 400 addresses overflows the URI length
  limit (HTTP 414), so 100 is the ceiling there too. Past the fid tip `/user/bulk`
  returns 404 rather than an empty list, which the scan treats as "empty" and stops
  after 25 consecutive empty batches.
* **Cast search paginates differently from every other Neynar endpoint** — its
  cursor is at `result.next.cursor`, not at the top level — so the generic paginator
  silently yields nothing for it. `brand_engagement` reads that envelope directly.
* **Clanker's fid is metadata, not chain state.** Roughly 61% of the 565 Arbitrum
  Clanker tokens carry the requesting fid; the rest were deployed from a contract or
  a wallet with no Farcaster requester, and are only linked to a person through the
  `linked_wallets` wallet → fid map. The admin's own fid closes some of that gap —
  425 of 565 tokens carry one — but the two are different facts and are stored as
  different columns rather than coalesced.
* **The Bankr fee recipient is decoded, not read off a label.** Nothing on
  Robinhood Chain emits "fee recipient". What it emits is a Doppler beneficiary
  array — `(address, uint256)[]` — from two contracts: the v4 initializer, keyed
  by the token, and the StreamableFeesLocker, keyed by pool id. The protocol
  occupies a fixed number of entries in each (one, and two), so an array longer
  than that baseline carries a launcher, and the launcher is its largest share.
  Validated against 134 tokens labelled by Bankr's own
  `/public/doppler/creator-fees/<wallet>` endpoint: 130 matched immediately, and
  the four misses turned out to be the same 5/95 shape with an older protocol
  address — which is why the rule orders by share rather than filtering a list of
  known protocol addresses. A second pass over a fresh window matched 62 of 62.
  Coverage is ~99% of launches; the remainder emit neither event and keep a null
  `fee_recipient_address`. The locker's rows are attributed by log position
  within the bundled transaction (a launch owns the logs from its token's first
  one up to the next launch's), so `resolve_launcher=False` — which skips the log
  ordering entirely — cannot fill the column at all.
* **A pipeline run tops up at most `MAX_NEYNAR_ADDRESSES` (20k) wallets.** Bankr's
  67k tokens carry ~15k distinct deployers and a comparable number of distinct fee
  recipients, and re-asking Neynar about all of them every run would spend hundreds
  of calls re-confirming misses. `linked_wallets` is the uncapped path; this is only
  the top-up for what its crawl has not reached yet, and anything skipped is
  recorded on the run's manifest. `--limit N` lowers the ceiling to N, which is what
  makes a smoke test cost one call instead of hundreds.
* **`dex.trades` records the router, not the human, on Uniswap v4 hook pools.** A
  single `taker` appears across dozens of distinct `tx_from` values, so
  `token_buyers` emits both candidates per trade and keeps whichever resolves to an
  fid, preferring `taker`.
* **Hyperliquid has no bulk endpoint.** Everything is per-user `/info` POSTs at
  ~24 wallets/min, which is why the crawl runs over the derived `arb_cohort`
  (thousands of wallets, ordered most-specific-first) rather than over the full
  verification set (millions).
* **Attribution is a conservative heuristic, not a measurement.** A buy is credited
  to an author only on demonstrated exposure — the buyer reacted to, or wrote, that
  author's cast about the token within 5 days before the purchase — and each buy's
  USD is split equally among qualifying authors. Equal shares conserve the total;
  we have no basis for ranking whose cast mattered more.

---

## Repo layout

```
config/       settings.py (every tunable), tokens.py (the blue-chip index + vaults)
lib/          shared clients and plumbing: dune, neynar, clanker, bankr, hyperliquid,
              http, neo4j_utils, runs, state, cli, seeds, sqlfmt, fid_resolver,
              wallet_fids (the local-first address -> fid lookup pipelines use)
sql/          Trino SQL builders, one module per subject area (deployers, trades,
              popular, robinhood, evangelism) — all literals go through lib/sqlfmt.py
cypher/       the Neo4j counterpart to sql/: one module per data type holding the
              statements ingestion writes, plus schema.py (constraint/index DDL)
              and common.py (fragments more than one data type shares)
pipelines/    one module per data type; `python -m pipelines.<name>`
ingestion/    ingest_*.py (one per data type, except ingest_tokens.py which covers
              both launchpads), base.py (shared UNWIND/MERGE machinery) and
              constraints.py; `python -m ingestion.ingest_<name>`
scripts/      run_all.py (the orchestrator), probe_schemas.py (Dune schema checks)
seeds/        operator-supplied CSVs + their schema documentation
data/, state/ run artifacts and watermarks (git-ignored)
```
