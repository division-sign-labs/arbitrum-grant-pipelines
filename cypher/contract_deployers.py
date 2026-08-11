"""Writes (Wallet)-[:DEPLOYED]->(Contract) and (Wallet)-[:ACTIVE_ON]->(Chain).

DEPLOYED is keyed on (wallet, contract, txHash) rather than txHash alone: a
factory deploy creates several contracts in one transaction and each needs its
own edge. ACTIVE_ON is the aggregate counterpart — one edge per (wallet, chain),
overwritten on every run, and shared with miniapp_builders.
"""

from __future__ import annotations

from cypher.common import optional_account_link

DEPLOYMENTS_CYPHER = f"""
UNWIND $rows AS row
MERGE (w:Wallet {{address: toLower(row.deployer_address)}})
  ON CREATE SET w.asOf = $asOf, w.ingestedBy = $ingestedBy
MERGE (c:Contract {{address: toLower(row.contract_address), chainId: row.chain_id}})
SET c.asOf = $asOf, c.ingestedBy = $ingestedBy
MERGE (w)-[d:DEPLOYED {{txHash: row.tx_hash}}]->(c)
SET d.deployedAt = row.deployed_at,
    d.method = row.deploy_method,
    d.chainId = row.chain_id,
    d.asOf = $asOf,
    d.ingestedBy = $ingestedBy
{optional_account_link()}
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
