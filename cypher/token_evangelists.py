"""Writes (WarpcastAccount)-[:POSTED_ABOUT]->(Token) and -[:EVANGELIZED]->(Token).

POSTED_ABOUT is keyed on the cast hash; EVANGELIZED is a singleton per (author,
token), recomputed and overwritten each run.

`ATTRIBUTIONS_CYPHER` writes no edge of its own — it MERGEs the BOUGHT edges its
rows reference so the evangelist subgraph is complete even when token_buyers has
not been ingested for that window. Those value properties are set ON CREATE only,
because token_buyers is the authority on a purchase and must never be overwritten
by the attribution copy.
"""

from __future__ import annotations

from cypher.common import optional_account_link

TOKEN_CASTS_CYPHER = """
UNWIND $rows AS row
MERGE (t:Token {address: toLower(row.token_address), chainId: row.chain_id})
  ON CREATE SET t.asOf = $asOf, t.ingestedBy = $ingestedBy
MERGE (a:WarpcastAccount {fid: row.author_fid})
  ON CREATE SET a.asOf = $asOf, a.ingestedBy = $ingestedBy
MERGE (a)-[p:POSTED_ABOUT {castHash: row.cast_hash}]->(t)
SET p.timestamp = row.timestamp,
    p.matchedOn = row.matched_on,
    p.likesCount = row.likes_count,
    p.recastsCount = row.recasts_count,
    p.asOf = $asOf,
    p.ingestedBy = $ingestedBy
"""

ATTRIBUTIONS_CYPHER = f"""
UNWIND $rows AS row
MERGE (t:Token {{address: toLower(row.token_address), chainId: row.chain_id}})
  ON CREATE SET t.asOf = $asOf, t.ingestedBy = $ingestedBy
MERGE (a:WarpcastAccount {{fid: row.author_fid}})
  ON CREATE SET a.asOf = $asOf, a.ingestedBy = $ingestedBy
MERGE (w:Wallet {{address: toLower(row.buyer_address)}})
  ON CREATE SET w.asOf = $asOf, w.ingestedBy = $ingestedBy
MERGE (w)-[b:BOUGHT {{txHash: row.tx_hash}}]->(t)
  ON CREATE SET b.usd = row.amount_usd,
                b.timestamp = row.block_time,
                b.chainId = row.chain_id
SET b.asOf = $asOf,
    b.ingestedBy = $ingestedBy
{optional_account_link("buyer_fid")}
"""

SUMMARY_CYPHER = """
UNWIND $rows AS row
MERGE (a:WarpcastAccount {fid: row.author_fid})
  ON CREATE SET a.asOf = $asOf, a.ingestedBy = $ingestedBy
MERGE (t:Token {address: toLower(row.token_address), chainId: row.chain_id})
  ON CREATE SET t.asOf = $asOf, t.ingestedBy = $ingestedBy
MERGE (a)-[e:EVANGELIZED]->(t)
SET e.castCount = row.cast_count,
    e.uniqueBuyers = row.unique_buyers_influenced,
    e.totalPurchases = row.total_purchases,
    e.totalPurchaseVolumeUsd = row.total_purchase_volume_usd,
    e.attributedUsd = row.attributed_usd,
    e.asOf = $asOf,
    e.ingestedBy = $ingestedBy
"""
