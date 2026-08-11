# Arbitrum Ecosystem Data Pipelines — Final Report

## Project Summary

This project built data pipelines tracking Arbitrum ecosystem activity across Farcaster: users who deploy smart contracts on Arbitrum, users who build miniapps integrating Arbitrum, and engagement with the Arbitrum brand account and related content. Scope was extended during the grant to cover token launches, buyers, evangelism attribution, blue-chip DeFi positions, and Hyperliquid usage.

Delivered: eleven pipelines, a Neo4j graph of 9.06M nodes and 5.78M relationships, 486 tests, and an incremental update system. All milestones complete.

| Metric | Result |
|---|---|
| Farcaster accounts indexed | 3,345,915 |
| Wallets linked to those accounts | 5,623,513 |
| Arbitrum contract deployers on Farcaster | 2,076 (26,050 deployments) |
| Tokens launched (Arbitrum + Robinhood Chain) | 67,670 |
| /arbitrum channel participants | 15,433 |
| Brand engagement events | 23,375 |
| Hyperliquid lifetime volume, Arbitrum cohort | $1.02B across 1,088 wallets |

- **GitHub:** `$REPO_URL`
- **Dashboard:** `$DASHBOARD_URL`

---

## Milestone 1 — Farcaster Identity Base and Infrastructure

### What Was Delivered

A complete map of every Farcaster account to every wallet it has verified — the join key for every other pipeline — plus the shared infrastructure all pipelines run on.

### Deliverables

- **`linked_wallets` pipeline** — enumerates the full Farcaster fid space via Neynar; extracts verified ETH/Solana addresses, custody addresses, reputation score, registration date
- **Shared library** — Dune client (execute/poll/paginate/cache), rate-limited HTTP clients, validated SQL literal builders, run and watermark handling
- **Timestamped CSV run system** — `data/<type>/<run_ts>/` plus a manifest recording row counts, parameters, and any degraded stage
- **Incremental system** — `--backfill`, `--since`, and watermark-driven runs across all pipelines
- **Resumable crawl** — checkpointed per batch; `--resume` continues after interruption

### KPI Results

| KPI Target | Result |
|---|---|
| Complete Farcaster identity index | Achieved — 3,345,915 accounts, 5,623,513 wallets |
| Distinct ETH addresses resolved | Achieved — 4,698,824 |
| Backfill completes in a working session | Achieved — ~2.3h |
| Crawl survives interruption | Achieved — checkpointed, `--resume` verified |
| Reputation gate for downstream filtering | Achieved — 86,414 accounts score > 0.6 |

---

## Milestone 2 — Contract Deployers and Miniapp Builders

### What Was Delivered

Farcaster users who deployed smart contracts on Arbitrum, and Farcaster users who built miniapps integrating Arbitrum, each with associated on-chain activity.

### Deliverables

- **`contract_deployers` pipeline** — Dune `arbitrum.creation_traces` joined to `arbitrum.transactions`; per-contract detail plus per-wallet activity rollups
- **`miniapp_builders` pipeline** — seed-driven; resolves seeded fids to wallets and measures their Arbitrum activity
- **Deploy-method classification** — `direct` vs `via_factory`, so factory deployments attribute to the initiating account, not the factory
- **Reputation-filtered views** — deployers segmented by score

### KPI Results

| KPI Target | Result |
|---|---|
| Farcaster accounts deploying on Arbitrum identified | Achieved — 2,076 of 373,565 distinct Arbitrum deployers scanned |
| Associated on-chain activity captured | Achieved — 26,050 deployments, per-wallet tx counts and first/last activity |
| High-reputation subset | Achieved — 333 accounts (score > 0.6), 2,644 contracts |
| Miniapp builders resolved to wallets and activity | Achieved — 68 builders → 257 wallets, 95 Arbitrum-active |

---

## Milestone 3 — Brand and Channel Engagement

### What Was Delivered

Engagement with the Arbitrum brand account and related content, across two surfaces: direct engagement with designated brand accounts, and `/arbitrum` channel participation.

### Deliverables

- **`brand_engagement` pipeline** — replies, likes, recasts, mentions targeting seeded brand accounts, with weighted scoring (reply 3, recast 2, mention 2, like 1, times per-account weight)
- **`/arbitrum` channel coverage** — every cast with reaction and reply counts, plus per-participant rollups
- **Seed-driven brand configuration** — brand accounts supplied as CSV; scope widens without code changes

### KPI Results

| KPI Target | Result |
|---|---|
| Brand engagement measured | Achieved — 23,375 events, 9,481 distinct accounts |
| Broken out by engagement type | Achieved — 14,506 likes, 3,293 recasts, 2,892 mentions, 2,684 replies |
| Channel activity captured | Achieved — 17,916 casts, 15,433 participants |
| Weighted engagement scoring | Achieved — 9,489 scored engager–brand pairs |

Brand accounts measured: `@arbitrum` (536359), `@offchainlabs` (279472).

---

## Milestone 4 — Token Launches, Buyers, and Evangelism

### What Was Delivered

Who launched tokens touching Arbitrum, who bought them, and attribution of buy volume to the accounts that posted about them.

### Deliverables

- **`clanker_tokens` pipeline** — Clanker API; Arbitrum tokens with deployer, admin, and requesting fid
- **`bankr_tokens` pipeline** — Robinhood Chain (Arbitrum Orbit L2) registry decoded from raw logs, with per-token Uniswap v4 swap volume. Bankr's API exposes only the 50 most recent launches, so history was reconstructed on-chain
- **Fee-recipient decoding** — Doppler beneficiary arrays decoded, validated against 134 API-labelled tokens; 99.7% coverage on a fresh validation window
- **ERC-4337 launcher attribution** — Bankr launches are user operations, so the transaction sender is the bundler; attribution uses the UserOperation sender instead
- **`token_buyers` pipeline** — purchases at or above $50, joined to Farcaster identity
- **`token_evangelists` pipeline** — buy-volume attribution to posting accounts
- **Role-tagged graph edges** — `deployer` / `fee_recipient` / `admin`

### KPI Results

| KPI Target | Result |
|---|---|
| Token launch registry, both launchpads | Achieved — 67,670 tokens (565 Clanker Arbitrum, 67,086 Bankr) |
| Launchers resolved to Farcaster identity | Achieved — Clanker 425/565; Bankr 15,047 deployers, 23,585 fee recipients |
| Buyers at ≥$50 identified | Achieved — 189 purchases |
| Evangelism attribution implemented and run | Achieved — 573 casts, 1,219 engagements, 29 attributed purchases, 273 credited authors |

Attribution model: a purchase is credited to an author only where the buyer reacted to, or wrote, that author's cast about the token within five days before buying. Each purchase is split equally among qualifying authors. Attributed volume totalled $144.85 — three Arbitrum tokens cleared the $50,000 qualification threshold.

---

## Milestone 5 — Blue-Chip Positions, Hyperliquid, Cohort

### What Was Delivered

Established Arbitrum DeFi positions rather than launchpad activity, plus Hyperliquid as a cross-chain measure.

### Deliverables

- **`popular_tokens` pipeline** — four independent legs: trades, net holdings, ERC-4626 vault deposits, Uniswap v3 LP events. Covers ARB, PENDLE, L3, and the four Gauntlet-curated Morpho vaults on Arbitrum
- **`arb_cohort` pipeline** — derived wallet cohort assembled from all upstream pipelines, ordered most-specific-source-first
- **`hyperliquid_activity` pipeline** — per-wallet lifetime volume and first-activity date, checkpointed and resumable

### KPI Results

| KPI Target | Result |
|---|---|
| Blue-chip positions measured | Achieved — 1,683 trades, 1,344 holdings, 84 vault deposits, 268 LP events |
| Gauntlet vault deposits | Achieved — 84 deposits, 41 accounts |
| Uniswap LP activity | Achieved — 268 events, 100 accounts |
| Hyperliquid participation | Achieved — 1,088 of 2,323 wallets active (47%) |
| Hyperliquid lifetime volume | Achieved — $1,016,581,871; 91 wallets above $1M |

A wider holdings pass is also available: 42,151 positions across 35,407 accounts.

---

## Milestone 6 — Graph, Documentation, Final Report

### What Was Delivered

The ingestion layer, query surface, documentation, and this report.

### Deliverables

- **Neo4j ingestion layer** — one module per data type, all idempotent; re-ingesting a run produces zero new nodes or relationships
- **Graph schema** — 7 node types, 13 relationship types, constraints created automatically
- **`cypher/` package** — mirrors `sql/`; the full graph write surface in one place
- **Orchestrator** — five-stage runner with resume-from-stage, per-pipeline selection, continue-on-error
- **486 offline unit tests** — no network or database required
- **Preflight check** — validates credentials, seeds, and layout before a long run
- **Documentation** — setup, run commands, pipeline table, graph schema, caveats

### KPI Results

| KPI Target | Result |
|---|---|
| All output loaded into a queryable graph | Achieved — 9,064,071 nodes, 5,784,327 relationships |
| Ingestion idempotent | Achieved — verified by double-ingestion (+0 nodes, +0 relationships) |
| Test suite passing | Achieved — 486 tests |
| Incremental re-run capability | Achieved — watermark-driven across all pipelines |
| Documentation complete | Achieved |

---

## Delivery Summary

| Milestone | Deliverable | Status |
|---|---|---|
| M1 | Farcaster identity base + infrastructure | Complete |
| M2 | Contract deployers + miniapp builders | Complete |
| M3 | Brand and channel engagement | Complete |
| M4 | Token launches, buyers, evangelism | Complete |
| M5 | Blue-chip positions, Hyperliquid, cohort | Complete |
| M6 | Graph, documentation, final report | Complete |

### Graph Contents

| Node type | Count | Relationship type | Count |
|---|---|---|---|
| Wallet | 5,624,438 | ACCOUNT | 5,623,514 |
| WarpcastAccount | 3,345,915 | DEPLOYED | 114,808 |
| Token | 67,670 | POSTED_IN | 15,433 |
| Contract | 26,050 | REACTED_IN | 15,433 |
| Channel / Chain / Platform | 3 | ENGAGED_WITH | 9,489 |
| | | ACTIVE_ON | 2,152 |
| | | TRADED | 1,683 |
| | | CREATED | 1,358 |
| | | HOLDS | 1,344 |
| | | USED (Hyperliquid) | 1,088 |
| | | POSTED_ABOUT | 573 |
| | | EVANGELIZED | 273 |
| | | PROVIDED_LIQUIDITY | 268 |

---

## Note on Measurement Window

Two external factors affected the activity measured.

Offchain Labs launched a paid developer incentives program at approximately the point this grant began. Developers routed directly to that program, and Offchain Labs captured its own usage data through it.

Arbitrum activity on Farcaster then tapered over the window. `/arbitrum` channel casts fell from 7,513 in August 2025 to roughly 20 per month by mid-2026; brand engagement fell from 8,298 events in July 2025 to a similar floor; first-time Arbitrum deployers fell from 75 per month to roughly 20. Contract deployments held flatter at 300–500 per month through 2026, excluding one automated account responsible for 10,152 deployments in a single month.

The pipelines measure this accurately, and the trend data is itself an output. Toward the end of the grant period the team's focus moved to prediction market analytics, where activity was growing; the infrastructure built here carried over directly.

---

## Team

| Name | Role | GitHub |
|---|---|---|
| `$TEAM_MEMBER_1` | `$ROLE_1` | `$GITHUB_1` |
| `$TEAM_MEMBER_2` | `$ROLE_2` | `$GITHUB_2` |

---

## Key Links

| Resource | URL |
|---|---|
| GitHub repository | `$REPO_URL` |
| Dashboard | `$DASHBOARD_URL` |
| Demo video | `$DEMO_VIDEO_URL` |
| Grant application | `$GRANT_APPLICATION_URL` |

---

## Appendix — Monthly Activity

| Month | Contract deploys | /arbitrum casts | Brand engagements | Clanker launches |
|---|---|---|---|---|
| 2025-01 | 1,089 | 243 | 381 | 0 |
| 2025-02 | 741 | 146 | 11 | 0 |
| 2025-03 | 1,389 | 167 | 10 | 0 |
| 2025-04 | 1,082 | 110 | 11 | 0 |
| 2025-05 | 1,279 | 103 | 14 | 0 |
| 2025-06 | 2,008 | 291 | 141 | 0 |
| 2025-07 | 1,089 | 1,003 | 8,298 | 125 |
| 2025-08 | 891 | 7,513 | 2,875 | 127 |
| 2025-09 | 1,001 | 1,253 | 1,438 | 89 |
| 2025-10 | 875 | 691 | 4,774 | 104 |
| 2025-11 | 755 | 708 | 2,880 | 31 |
| 2025-12 | 650 | 244 | 1,035 | 4 |
| 2026-01 | 512 | 382 | 415 | 3 |
| 2026-02 | 397 | 1,111 | 511 | 15 |
| 2026-03 | 449 | 1,354 | 218 | 10 |
| 2026-04 | 473\* | 2,524 | 261 | 20 |
| 2026-05 | 300 | 43 | 63 | 21 |
| 2026-06 | 293 | 11 | 15 | 6 |
| 2026-07 | 523 | 19 | 23 | 8 |

\* Excludes one automated account responsible for 10,152 deployments in April 2026; raw figure 10,625.
