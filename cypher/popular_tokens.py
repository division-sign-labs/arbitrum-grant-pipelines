"""Writes TRADED, HOLDS, DEPOSITED_IN and PROVIDED_LIQUIDITY from (Wallet) to (Token).

Merge keys, and why they are not just the tx hash:

  TRADED              (txHash, side). One transaction that both sells ARB and
                      buys PENDLE produces two rows; keying on the hash alone
                      would let the second overwrite the first.
  PROVIDED_LIQUIDITY  (txHash, event, poolAddress). A single tx can add
                      liquidity to two pools of the same token.
  DEPOSITED_IN        (txHash). One ERC-4626 deposit per tx per vault.
  HOLDS               a singleton per (wallet, token): a balance is a current
                      fact, so it is overwritten, and `asOf` dates it.

A vault is a Token node carrying `kind = 'vault'`, so a deposit target and a
traded token share one label and one constraint.
"""

from __future__ import annotations

from cypher.common import optional_account_link

TRADES_CYPHER = f"""
UNWIND $rows AS row
MERGE (t:Token {{address: toLower(row.token_address), chainId: row.chain_id}})
  ON CREATE SET t.asOf = $asOf, t.ingestedBy = $ingestedBy
MERGE (w:Wallet {{address: toLower(row.address)}})
  ON CREATE SET w.asOf = $asOf, w.ingestedBy = $ingestedBy
MERGE (w)-[tr:TRADED {{txHash: row.tx_hash, side: row.side}}]->(t)
SET tr.usd = row.amount_usd,
    tr.tokenAmount = row.token_amount,
    tr.timestamp = row.block_time,
    tr.chainId = row.chain_id,
    tr.asOf = $asOf,
    tr.ingestedBy = $ingestedBy
{optional_account_link()}
"""

HOLDINGS_CYPHER = f"""
UNWIND $rows AS row
MERGE (t:Token {{address: toLower(row.token_address), chainId: row.chain_id}})
  ON CREATE SET t.asOf = $asOf, t.ingestedBy = $ingestedBy
MERGE (w:Wallet {{address: toLower(row.address)}})
  ON CREATE SET w.asOf = $asOf, w.ingestedBy = $ingestedBy
MERGE (w)-[h:HOLDS]->(t)
SET h.balance = row.balance,
    h.balanceRaw = row.balance_raw,
    h.lastActivityAt = row.last_activity_at,
    h.asOf = $asOf,
    h.ingestedBy = $ingestedBy
{optional_account_link()}
"""

VAULT_CYPHER = f"""
UNWIND $rows AS row
MERGE (v:Token {{address: toLower(row.vault_address), chainId: row.chain_id}})
SET v.kind = 'vault',
    v.asOf = $asOf,
    v.ingestedBy = $ingestedBy
MERGE (w:Wallet {{address: toLower(row.address)}})
  ON CREATE SET w.asOf = $asOf, w.ingestedBy = $ingestedBy
MERGE (w)-[d:DEPOSITED_IN {{txHash: row.tx_hash}}]->(v)
SET d.assets = row.assets,
    d.assetsRaw = row.assets_raw,
    d.sharesRaw = row.shares_raw,
    d.timestamp = row.block_time,
    d.chainId = row.chain_id,
    d.asOf = $asOf,
    d.ingestedBy = $ingestedBy
{optional_account_link()}
"""

LP_CYPHER = f"""
UNWIND $rows AS row
MERGE (t:Token {{address: toLower(row.token_address), chainId: row.chain_id}})
  ON CREATE SET t.asOf = $asOf, t.ingestedBy = $ingestedBy
MERGE (w:Wallet {{address: toLower(row.address)}})
  ON CREATE SET w.asOf = $asOf, w.ingestedBy = $ingestedBy
MERGE (w)-[lp:PROVIDED_LIQUIDITY {{
    txHash: row.tx_hash, event: row.event, poolAddress: toLower(row.pool_address)
}}]->(t)
SET lp.amount0 = row.amount0,
    lp.amount1 = row.amount1,
    lp.timestamp = row.block_time,
    lp.chainId = row.chain_id,
    lp.asOf = $asOf,
    lp.ingestedBy = $ingestedBy
{optional_account_link()}
"""
