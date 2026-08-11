# Quotient — Final Report

## Project Summary

Quotient builds data analytics tools to help brands grow on Farcaster. This grant funded three things: content teaching developers how and why to build Arbitrum-integrated miniapps on Farcaster, data pipelines tracking Arbitrum ecosystem development on Farcaster, and a dashboard reporting on the drivers of that growth.

All three milestones are complete.

**Funding ask:** 20,000 USD · **Milestones:** 3 · **Category:** Analytics

| | |
|---|---|
| Farcaster accounts indexed | 3,345,915 |
| Wallets belonging to those accounts | 5,623,513 |
| Arbitrum contract deployers on Farcaster | 2,076 |
| Contracts they deployed | 26,050 |
| Tokens launched | 67,670 |
| /arbitrum channel participants | 15,433 |
| Brand engagement events | 23,375 |
| Lifetime Hyperliquid volume, miniapp builders | $183,281,830 |
| Lifetime Hyperliquid volume, Arbitrum ecosystem posters | $202,172,797 |

Public links:

- **Pipelines:** https://github.com/division-sign-labs/arbitrum-grant-pipelines
- **Dashboard:** https://arb.quotient.social

---

## Milestone 1 — Content Describing How and Why to Build on Arbitrum

**3,000 USD**

### What Was Delivered

Four published articles and a working miniapp hosting resources on building Arbitrum-integrated miniapps on Farcaster. The content covers the build process, the audiences reachable through Arbitrum, and the grant and incentive programs available to miniapp developers.

Start of this milestone was delayed to deconflict with Offchain Labs, which launched a separate initiative promoting Arbitrum miniapps on Farcaster during the same period.

### Deliverables

- **ArbSwap** — a social trading miniapp recommending tokens based on the trading activity of a user's Farcaster mutual followers, focused on Clanker tokens to support Clanker's launch on Arbitrum. https://arbswap.trading
- **Building Farcaster Mini Apps on Arbitrum** — https://paragraph.com/@quotient/building-farcaster-mini-apps-on-arbitrum
- **How to Analyze Your Arbitrum Miniapp User Data with Quotient** — https://paragraph.com/@quotient/how-to-analyze-your-arbitrum-miniapp-user-data-with-quotient
- **Arbitrum Incentives for Miniapp Builders** — https://paragraph.com/@quotient/arbitrum-incentives-for-miniapp-builders
- **Where Liquidity Meets Virality: Building Games with Arbitrum + Farcaster** — https://paragraph.com/@quotient/where-liquidity-meets-virality-building-games-with-arbitrum-farcaster
- **Launch announcement** — https://farcaster.xyz/quotient/0x93c5474e

### KPI Results

| KPI Target | Result |
|---|---|
| Users who added the miniapp (low 250 / medium 1,000 / high 5,000) | 257 unique users — low target met |
| Users completing an in-app swap from Base to Arbitrum | 17 |
| Farcaster trending apps leaderboard | Peaked at #32 |
| Articles published | 4 |

ArbSwap was recognised by the Arbitrum Foundation and Offchain Labs in the inaugural week of Arbitrum Miniapp Developer rewards: https://x.com/BFreshHB/status/1951302727742660890

---

## Milestone 2 — Data Pipelines Tracking Arbitrum Ecosystem Development

**8,250 USD**

### What Was Delivered

Eleven data pipelines measuring Arbitrum activity across Farcaster, loading into a Neo4j graph. They identify who deploys contracts on Arbitrum, who builds miniapps, who launches and buys tokens, who holds major Arbitrum assets, and who engages with Arbitrum's accounts and channel.

Every pipeline can rebuild from scratch or update incrementally, writes CSV output for inspection, and resumes after interruption.

### Deliverables

- **`linked_wallets`** — every Farcaster account, its verified wallets, its reputation score
- **`contract_deployers`** — every Arbitrum contract deployment traced to a Farcaster account, with on-chain activity. Deployments made through a factory contract are credited to the person, not the factory
- **`miniapp_builders`** — miniapp builders and their Arbitrum activity
- **`brand_engagement`** — replies, likes, recasts and mentions directed at Arbitrum's accounts, plus full /arbitrum channel coverage, with weighted scoring
- **`clanker_tokens` and `bankr_tokens`** — token launches on Arbitrum and Robinhood Chain, with the account behind each launch and per-token trading volume
- **`token_buyers`** — purchases of $50 or more, matched to Farcaster accounts
- **`token_evangelists`** — purchases traced to the posts that influenced them
- **`popular_tokens`** — trades, holdings, Gauntlet vault deposits and Uniswap liquidity for ARB, PENDLE and Layer3
- **`arb_cohort`** — the combined list of wallets active on Arbitrum
- **`hyperliquid_activity`** — lifetime trading volume and first trade date per wallet
- **Neo4j graph, loaders, orchestrator, 486 tests, and documentation**

### KPI Results

| KPI Target | Result |
|---|---|
| Users who deployed smart contracts on Arbitrum, with on-chain activity | 2,076 Farcaster accounts, 26,050 contracts, from 373,565 Arbitrum deployers scanned |
| Users who created miniapps integrating Arbitrum, with on-chain activity | 68 builders, 257 wallets, 95 active on Arbitrum |
| Engagement with the Arbitrum brand account and related content | 23,375 events from 9,481 accounts; 17,916 /arbitrum posts from 15,433 participants |

Detail by category:

| | |
|---|---|
| Farcaster accounts indexed | 3,345,915 |
| Wallets linked | 5,623,513 |
| Accounts scoring above 0.6 | 86,414 |
| Deployers scoring above 0.6 | 333 (2,644 contracts) |
| Likes / recasts / mentions / replies | 14,506 / 3,293 / 2,892 / 2,684 |
| Tokens tracked | 67,670 |
| Distinct token launchers identified | 15,047 |
| Purchases of $50 or more | 189 |
| Wallets checked on Hyperliquid | 2,323 |
| Wallets trading on Hyperliquid | 1,088 (47%) |
| Their lifetime Hyperliquid volume | $1,016,581,871 |
| Graph nodes / relationships | 9,064,071 / 5,784,327 |
| Tests passing | 486 |

### Links

- **Repository:** https://github.com/division-sign-labs/arbitrum-grant-pipelines

---

## Milestone 3 — Dashboard for Monitoring Arbitrum Ecosystem Growth on Farcaster

**8,750 USD**

### What Was Delivered

A dashboard reporting on the drivers of Arbitrum ecosystem growth on Farcaster, built on the graph from Milestone 2. It presents the data across five views covering accounts, engagement, growth over time, protocols, and tokens.

### Deliverables

- **Accounts** — Farcaster accounts active on Arbitrum, ranked by activity and reputation
- **Engagement** — engagement with Arbitrum's accounts and the /arbitrum channel
- **Growth** — activity over time, including monthly casts, engagements and distinct authors
- **Protocols** — activity across Arbitrum ecosystem protocols including Hyperliquid
- **Tokens** — token launches, purchases, and the posts associated with them
- **Snapshot mode** — the dashboard runs against a committed data snapshot without graph credentials

### KPI Results

| KPI Target | Result |
|---|---|
| Weekly active users (low 50 / medium 250 / high 1,000) | `$WAU_RESULT` |

### Links

- **Dashboard:** https://arb.quotient.social
- **Repository:** https://github.com/division-sign-labs/arbitrum-grant-dashboard

---

## Delivery Summary

| Milestone | Deliverable | Amount | Status |
|---|---|---|---|
| M1 | Content describing how and why to build on Arbitrum | 3,000 USD | Complete |
| M2 | Data pipelines tracking Arbitrum ecosystem development | 8,250 USD | Complete |
| M3 | Dashboard for monitoring Arbitrum ecosystem growth | 8,750 USD | Complete |
| | **Total** | **20,000 USD** | |

---

## Findings

**Miniapp builders trade significant volume on Arbitrum ecosystem protocols.** The 68 miniapp builders tracked hold wallets with $183,281,830 in lifetime Hyperliquid volume. Arbitrum ecosystem posters and brand engagers account for $202,172,797. These groups overlap, so the figures are not additive.

**Farcaster accounts deploying contracts on Arbitrum are a small but identifiable group.** Of 373,565 addresses that deployed a contract on Arbitrum, 2,076 belong to Farcaster accounts. 333 of those score above 0.6 on reputation.

**Most token launch activity sits on Robinhood Chain rather than Arbitrum One.** Robinhood Chain, an Arbitrum Orbit chain launched in July 2026, carries 67,086 of the 67,670 tokens tracked.

**Posting drives little measurable purchase volume.** 573 posts about tracked tokens produced 29 purchases traceable to a post, totalling $144.85. A purchase counts as influenced when the buyer engaged with a post about that token within five days of buying.

---

## Note on the Measurement Window

Around the time this grant started, Offchain Labs launched a paid incentives program for developers. Developers went there directly, and Offchain Labs collected its own usage data through it. This delayed the start of Milestone 1 while we deconflicted with that initiative.

Arbitrum activity on Farcaster declined over the period. /arbitrum channel posts fell from 7,513 in August 2025 to about 20 a month by mid-2026. Brand engagement fell from 8,298 events in July 2025 to a similar level. First-time Arbitrum deployers fell from 75 a month to about 20. Contract deployments stayed between 300 and 500 a month.

Near the end of the grant we shifted focus to prediction market analytics, where activity was growing. We reused this infrastructure for that work.

---

## Team

| Name | Role | Links |
|---|---|---|
| Jordan Olmstead | Product / Data | [LinkedIn](https://www.linkedin.com/in/jordan-o-5b5845128/) · [Farcaster](https://warpcast.com/ruminations) · [GitHub](https://github.com/jchanolm) |
| Steve Simkins | Content / DevRel | [LinkedIn](https://www.linkedin.com/in/steve-simkins/) · [Farcaster](https://warpcast.com/stevedylandev.eth) · [GitHub](https://github.com/stevedylandev) · [X](https://x.com/stevedylandev) |
| Francisco Pablo Marengo | Data Analytics / Research | [LinkedIn](https://www.linkedin.com/in/francisco-pablo-marengo-103165239) |

---

## Key Links

| Resource | URL |
|---|---|
| Website | https://usequotient.xyz |
| Dashboard | https://arb.quotient.social |
| Pipelines repository | https://github.com/division-sign-labs/arbitrum-grant-pipelines |
| Dashboard repository | https://github.com/division-sign-labs/arbitrum-grant-dashboard |
| ArbSwap miniapp | https://arbswap.trading |
| Building Farcaster Mini Apps on Arbitrum | https://paragraph.com/@quotient/building-farcaster-mini-apps-on-arbitrum |
| How to Analyze Your Arbitrum Miniapp User Data | https://paragraph.com/@quotient/how-to-analyze-your-arbitrum-miniapp-user-data-with-quotient |
| Arbitrum Incentives for Miniapp Builders | https://paragraph.com/@quotient/arbitrum-incentives-for-miniapp-builders |
| Building Games with Arbitrum + Farcaster | https://paragraph.com/@quotient/where-liquidity-meets-virality-building-games-with-arbitrum-farcaster |
| Budget breakdown | https://docs.google.com/spreadsheets/d/1ZCnZs5iSpWN2mzMVrcXFefNTN-SDF9gjPM6gS_PfIiw/edit |
| GitHub organisation | https://github.com/jchanolm |

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
