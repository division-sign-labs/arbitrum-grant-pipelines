"""Writes builder (WarpcastAccount)-[:ACCOUNT]->(Wallet) and -[:ACTIVE_ON]->(Chain).

`miniappBuilder` is the seed-list membership flag and the one account property
this data type owns. The ACCOUNT edge is written but not owned: linked_wallets
knows the protocol and which address is primary, so those properties are only
set ON CREATE here and never overwritten.
"""

from __future__ import annotations

from cypher.common import optional_account_link

WALLETS_CYPHER = """
UNWIND $rows AS row
MERGE (a:WarpcastAccount {fid: row.fid})
SET a.miniappBuilder = true,
    a.asOf = $asOf,
    a.ingestedBy = $ingestedBy
MERGE (w:Wallet {address: toLower(row.address)})
  ON CREATE SET w.asOf = $asOf, w.ingestedBy = $ingestedBy
MERGE (a)-[r:ACCOUNT]->(w)
  ON CREATE SET r.protocol = 'eth', r.isPrimary = false, r.source = $source
SET r.asOf = $asOf,
    r.ingestedBy = $ingestedBy
"""

ACTIVITY_CYPHER = f"""
UNWIND $rows AS row
MERGE (w:Wallet {{address: toLower(row.address)}})
  ON CREATE SET w.asOf = $asOf, w.ingestedBy = $ingestedBy
MERGE (chain:Chain {{chainId: row.chain_id}})
  ON CREATE SET chain.asOf = $asOf, chain.ingestedBy = $ingestedBy
MERGE (w)-[r:ACTIVE_ON]->(chain)
SET r.txCount = row.tx_count,
    r.firstTxAt = row.first_tx_at,
    r.lastTxAt = row.last_tx_at,
    r.source = $source,
    r.asOf = $asOf,
    r.ingestedBy = $ingestedBy
{optional_account_link()}
"""
