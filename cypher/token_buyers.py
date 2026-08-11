"""Writes (Wallet)-[:BOUGHT]->(Token).

Keyed on the swap's transaction hash, which makes re-ingesting an overlapping
window free. Two buys of the same token by the same wallet in one transaction
therefore collapse into a single edge: the CSV contract carries no event index,
and keying on (txHash, amount) instead would turn a rounding difference into a
phantom second purchase.
"""

from __future__ import annotations

from cypher.common import optional_account_link

BUYS_CYPHER = f"""
UNWIND $rows AS row
MERGE (t:Token {{address: toLower(row.token_address), chainId: row.chain_id}})
  ON CREATE SET t.platform = row.platform, t.asOf = $asOf, t.ingestedBy = $ingestedBy
MERGE (w:Wallet {{address: toLower(row.buyer_address)}})
  ON CREATE SET w.asOf = $asOf, w.ingestedBy = $ingestedBy
MERGE (w)-[b:BOUGHT {{txHash: row.tx_hash}}]->(t)
SET b.usd = row.amount_usd,
    b.tokenAmount = row.token_amount,
    b.timestamp = row.block_time,
    b.platform = row.platform,
    b.chainId = row.chain_id,
    b.asOf = $asOf,
    b.ingestedBy = $ingestedBy
{optional_account_link()}
"""
