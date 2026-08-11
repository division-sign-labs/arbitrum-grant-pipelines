# Arbitrum Ecosystem Data Pipelines — Final Report

## Project Summary

We built data pipelines that measure Arbitrum activity across Farcaster. They answer who deploys contracts on Arbitrum, who builds miniapps, who launches and buys tokens, who holds major Arbitrum assets, and who engages with Arbitrum's accounts and channel.

Everything lands in a graph database you can query. Eleven pipelines, 9.06M nodes, 5.78M relationships. All milestones complete.

| | |
|---|---|
| Farcaster accounts indexed | 3,345,915 |
| Wallets belonging to those accounts | 5,623,513 |
| Arbitrum contract deployers on Farcaster | 2,076 |
| Contracts they deployed | 26,050 |
| Tokens launched | 67,670 |
| /arbitrum channel participants | 15,433 |
| Brand engagement events | 23,375 |
| Hyperliquid volume from Arbitrum builders | $1.02B |

- **GitHub:** `$REPO_URL`
- **Dashboard:** `$DASHBOARD_URL`

---

## Milestone 1 — Farcaster Accounts and Their Wallets

Every Farcaster account, and the wallets each one owns. This is what lets us say a wallet on Arbitrum belongs to a specific person.

**Deliverables**

- `linked_wallets` pipeline — every Farcaster account, its verified wallets, its reputation score
- Shared pipeline library used by everything downstream
- Timestamped CSV output for every run
- Incremental updates, so a rerun only fetches what is new

**Results**

| | |
|---|---|
| Accounts indexed | 3,345,915 |
| Wallets linked | 5,623,513 |
| Distinct Ethereum addresses | 4,698,824 |
| Accounts scoring above 0.6 | 86,414 |
| Full rebuild time | ~2.3 hours |

---

## Milestone 2 — Contract Deployers and Miniapp Builders

Who deploys smart contracts on Arbitrum, and who builds miniapps. Plus what those people do on-chain.

**Deliverables**

- `contract_deployers` pipeline — every contract deployment on Arbitrum traced to a Farcaster account
- `miniapp_builders` pipeline — miniapp builders and their Arbitrum activity
- Deployments made through a factory contract are credited to the person, not the factory

**Results**

| | |
|---|---|
| Arbitrum deployers scanned | 373,565 |
| Of those, Farcaster accounts | 2,076 |
| Contracts they deployed | 26,050 |
| Deployers scoring above 0.6 | 333 (2,644 contracts) |
| Miniapp builders tracked | 68 |
| Their wallets | 257 |
| Wallets active on Arbitrum | 95 |

---

## Milestone 3 — Engagement With Arbitrum

Who engages with Arbitrum's accounts on Farcaster, and who posts in the /arbitrum channel.

**Deliverables**

- `brand_engagement` pipeline — replies, likes, recasts and mentions aimed at Arbitrum's accounts
- /arbitrum channel coverage — every post, with reactions and replies
- Weighted scoring, so a reply counts for more than a like

**Results**

| | |
|---|---|
| Engagement events | 23,375 |
| Distinct accounts engaging | 9,481 |
| Likes | 14,506 |
| Recasts | 3,293 |
| Mentions | 2,892 |
| Replies | 2,684 |
| /arbitrum channel posts | 17,916 |
| Channel participants | 15,433 |

Accounts measured: `@arbitrum`, `@offchainlabs`.

---

## Milestone 4 — Token Launches and Buyers

Who launches tokens on Arbitrum and on Robinhood Chain, who buys them, and which posts led to purchases.

Robinhood Chain is an Arbitrum Orbit chain that launched in July 2026. Most token activity we found sits there rather than on Arbitrum itself.

**Deliverables**

- Token registry for Arbitrum and Robinhood Chain, with the account behind each launch and per-token trading volume
- `token_buyers` pipeline — purchases of $50 or more, matched to Farcaster accounts
- `token_evangelists` pipeline — purchases traced back to the posts that influenced them

**Results**

| | |
|---|---|
| Tokens tracked | 67,670 |
| On Robinhood Chain | 67,086 |
| On Arbitrum | 565 |
| Distinct launchers identified | 15,047 |
| Fee recipients identified | 23,585 |
| Purchases of $50 or more | 189 |
| Posts about tracked tokens | 573 |
| Accounts credited with influencing a purchase | 273 |
| Purchase volume traced to posts | $144.85 |

A purchase counts as influenced when the buyer engaged with a post about that token within five days of buying.

---

## Milestone 5 — Major Arbitrum Tokens and Hyperliquid

Who holds and trades the big Arbitrum tokens — ARB, PENDLE and Layer3. Who deposits into Gauntlet's lending vaults. Who provides liquidity on Uniswap. And which of these wallets trade on Hyperliquid.

**Deliverables**

- `popular_tokens` pipeline — trades, holdings, vault deposits and liquidity positions
- `arb_cohort` pipeline — the combined list of wallets active on Arbitrum
- `hyperliquid_activity` pipeline — lifetime trading volume and first trade date per wallet

**Results**

| | |
|---|---|
| Trades | 1,683 |
| Holdings | 1,344 |
| Gauntlet vault deposits | 84 (41 accounts) |
| Uniswap liquidity events | 268 (100 accounts) |
| Wallets checked on Hyperliquid | 2,323 |
| Wallets that trade there | 1,088 (47%) |
| Their lifetime volume | $1,016,581,871 |
| Wallets above $1M volume | 91 |

A wider pass covering 35,407 accounts and 42,151 holdings is also available.

---

## Milestone 6 — Graph Database and Documentation

Everything loaded into a graph you can query, with documentation and tests.

**Deliverables**

- Neo4j graph — 7 node types, 13 relationship types
- Loading tools for every dataset, safe to rerun
- Orchestrator that runs all eleven pipelines in order
- 486 tests
- Documentation covering setup, commands and the graph structure

**Results**

| | |
|---|---|
| Nodes | 9,064,071 |
| Relationships | 5,784,327 |
| Tests passing | 486 |

---

## Delivery Summary

| Milestone | Deliverable | Status |
|---|---|---|
| M1 | Farcaster accounts and their wallets | Complete |
| M2 | Contract deployers and miniapp builders | Complete |
| M3 | Engagement with Arbitrum | Complete |
| M4 | Token launches and buyers | Complete |
| M5 | Major Arbitrum tokens and Hyperliquid | Complete |
| M6 | Graph database and documentation | Complete |

### What Is in the Graph

| Node type | Count | Relationship | Count |
|---|---|---|---|
| Wallet | 5,624,438 | Owns a wallet | 5,623,514 |
| Farcaster account | 3,345,915 | Deployed | 114,808 |
| Token | 67,670 | Posted in channel | 15,433 |
| Contract | 26,050 | Reacted in channel | 15,433 |
| Channel, Chain, Platform | 3 | Engaged with Arbitrum | 9,489 |
| | | Active on chain | 2,152 |
| | | Traded | 1,683 |
| | | Created a token | 1,358 |
| | | Holds a token | 1,344 |
| | | Traded on Hyperliquid | 1,088 |
| | | Posted about a token | 573 |
| | | Influenced a purchase | 273 |
| | | Provided liquidity | 268 |

---

## Note on the Measurement Window

Around the time this grant started, Offchain Labs launched a paid incentives program for developers. Developers went there directly, and Offchain Labs collected its own usage data through it.

Arbitrum activity on Farcaster then declined over the period. /arbitrum channel posts fell from 7,513 in August 2025 to about 20 a month by mid-2026. Brand engagement fell from 8,298 events in July 2025 to a similar level. First-time Arbitrum deployers fell from 75 a month to about 20. Contract deployments held steadier at 300–500 a month.

Near the end of the grant we shifted focus to prediction market analytics, where activity was growing. The infrastructure built here carried over.

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

## Appendix — Activity by Month

| Month | Contract deploys | /arbitrum posts | Brand engagements | Arbitrum token launches |
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
| 2026-04 | 473 | 2,524 | 261 | 20 |
| 2026-05 | 300 | 43 | 63 | 21 |
| 2026-06 | 293 | 11 | 15 | 6 |
| 2026-07 | 523 | 19 | 23 | 8 |
