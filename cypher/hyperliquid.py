"""Writes (Wallet)-[:USED]->(Platform {name: 'hyperliquid'}).

USED is a singleton per wallet: lifetime volume and first-activity time are
current facts, recomputed and overwritten on each run.

`CHECKED_CYPHER` is the negative half of the same crawl. It writes no edge — it
only stamps `hlCheckedAt`/`hlActive` on the wallet, which is what lets a later
run tell "checked, no activity" apart from "never checked".
"""

from __future__ import annotations

from cypher.common import optional_account_link

ACTIVITY_CYPHER = f"""
UNWIND $rows AS row
MERGE (w:Wallet {{address: toLower(row.address)}})
SET w.hlCheckedAt = row.checked_at,
    w.hlActive = true,
    w.asOf = $asOf,
    w.ingestedBy = $ingestedBy
MERGE (p:Platform {{name: $platform}})
  ON CREATE SET p.asOf = $asOf, p.ingestedBy = $ingestedBy
MERGE (w)-[u:USED]->(p)
SET u.volumeUsd = row.cum_volume_usd,
    u.firstActivityAt = row.first_activity_at,
    u.ledgerEventCount = row.ledger_event_count,
    u.checkedAt = row.checked_at,
    u.asOf = $asOf,
    u.ingestedBy = $ingestedBy
{optional_account_link()}
"""

CHECKED_CYPHER = """
UNWIND $rows AS row
MERGE (w:Wallet {address: toLower(row.address)})
SET w.hlCheckedAt = row.checked_at,
    w.hlActive = false,
    w.asOf = $asOf,
    w.ingestedBy = $ingestedBy
"""
