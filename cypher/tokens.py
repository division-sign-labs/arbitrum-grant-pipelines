"""Writes (Token), (Wallet)-[:DEPLOYED]->(Token) and (WarpcastAccount)-[:CREATED]->(Token).

Two statements for two launchpads over one graph shape — only the property set
differs, so `deploy_and_create()` builds the shared tail both end with.

A launch has two wallets worth knowing about, and they are frequently different
people: the address that deployed the token, and the address that receives its
fees. The deployer can be a bot, a factory or an ERC-4337 smart account; the fee
recipient is usually an EOA the human actually controls, so it is the one a
Farcaster verification is likely to point at. Both therefore get the same edges,
distinguished by `role`:

    (Wallet)-[:DEPLOYED {role}]->(Token)
    (WarpcastAccount)-[:CREATED {role}]->(Token)

    role = 'deployer'        the deploying address, on both platforms
         | 'fee_recipient'   Bankr: the launcher's entry in the Doppler fee split
         | 'admin'           Clanker: the token's fee and reward owner

DEPLOYED and CREATED stay singletons keyed on the node pair, with `role` as a
property rather than part of the MERGE key. That is safe because the fee-recipient
branch is skipped whenever it would target the same wallet (or the same fid) as
the deployer branch, so each pair is written by exactly one branch of exactly one
row — the input is already one row per token. Nothing can create a second edge
beside the first, and nothing can flip a role on a re-run. Putting `role` in the
MERGE key instead would have done neither: it would have left every edge written
before roles existed orphaned, and duplicated it.

Market data is a snapshot overwritten each run, dated by `asOf`.
"""

from __future__ import annotations

# `row.<key>` on a map without that key is null in Cypher, so one tail serves
# both sources even though only Clanker carries `username`.
_DEPLOYER = """
FOREACH (_ IN CASE WHEN row.deployer_address IS NULL THEN [] ELSE [1] END |
  MERGE (w:Wallet {address: toLower(row.deployer_address)})
    ON CREATE SET w.asOf = $asOf, w.ingestedBy = $ingestedBy
  MERGE (w)-[d:DEPLOYED]->(t)
  SET d.role = 'deployer',
      d.txHash = row.tx_hash,
      d.deployedAt = row.deployed_at,
      d.platform = row.platform,
      d.asOf = $asOf,
      d.ingestedBy = $ingestedBy
)
FOREACH (_ IN CASE WHEN row.fid IS NULL THEN [] ELSE [1] END |
  MERGE (a:WarpcastAccount {fid: row.fid})
    ON CREATE SET a.asOf = $asOf, a.ingestedBy = $ingestedBy
  SET a.username = coalesce(row.username, a.username)
  MERGE (a)-[c:CREATED]->(t)
  SET c.role = 'deployer',
      c.platform = row.platform,
      c.deployedAt = row.deployed_at,
      c.asOf = $asOf,
      c.ingestedBy = $ingestedBy
)
"""


def deploy_and_create(fee_address_key: str, fee_fid_key: str, fee_role: str) -> str:
    """The shared tail: deployer edges, then fee-recipient edges under `fee_role`.

    The two guards are what keep a wallet (or an account) that is both deployer
    and fee recipient down to a single edge labelled 'deployer' — the more
    specific fact of the two, and the one that was true first.
    """
    return f"""{_DEPLOYER.strip()}
FOREACH (_ IN CASE
           WHEN row.{fee_address_key} IS NULL
             OR toLower(row.{fee_address_key})
                = toLower(coalesce(row.deployer_address, '')) THEN []
           ELSE [1] END |
  MERGE (w:Wallet {{address: toLower(row.{fee_address_key})}})
    ON CREATE SET w.asOf = $asOf, w.ingestedBy = $ingestedBy
  MERGE (w)-[d:DEPLOYED]->(t)
  SET d.role = '{fee_role}',
      d.txHash = row.tx_hash,
      d.deployedAt = row.deployed_at,
      d.platform = row.platform,
      d.asOf = $asOf,
      d.ingestedBy = $ingestedBy
)
FOREACH (_ IN CASE
           WHEN row.{fee_fid_key} IS NULL
             OR row.{fee_fid_key} = row.fid THEN []
           ELSE [1] END |
  MERGE (a:WarpcastAccount {{fid: row.{fee_fid_key}}})
    ON CREATE SET a.asOf = $asOf, a.ingestedBy = $ingestedBy
  // No username here: row.username is the launchpad's record of who ordered the
  // launch, which belongs to the deployer's account rather than to this one.
  MERGE (a)-[c:CREATED]->(t)
  SET c.role = '{fee_role}',
      c.platform = row.platform,
      c.deployedAt = row.deployed_at,
      c.asOf = $asOf,
      c.ingestedBy = $ingestedBy
)
"""


# Clanker's admin is the token's fee and reward owner, and Clanker's own
# vocabulary calls it the admin, so that is the role it carries here.
CLANKER_CYPHER = f"""
UNWIND $rows AS row
MERGE (t:Token {{address: toLower(row.token_address), chainId: row.chain_id}})
SET t.name = coalesce(row.name, t.name),
    t.symbol = coalesce(row.symbol, t.symbol),
    t.platform = coalesce(row.platform, t.platform, $platform),
    t.deployedAt = coalesce(row.deployed_at, t.deployedAt),
    t.deployTxHash = coalesce(row.tx_hash, t.deployTxHash),
    t.poolAddress = coalesce(row.pool_address, t.poolAddress),
    t.pairedToken = coalesce(row.paired_token, t.pairedToken),
    t.tokenType = coalesce(row.token_type, t.tokenType),
    t.adminAddress = coalesce(row.admin_address, t.adminAddress),
    t.creatorUsername = coalesce(row.username, t.creatorUsername),
    t.startingMarketCapUsd = coalesce(row.starting_market_cap_usd, t.startingMarketCapUsd),
    t.priceUsd = coalesce(row.price_usd, t.priceUsd),
    t.marketCapUsd = coalesce(row.market_cap_usd, t.marketCapUsd),
    t.volume24hUsd = coalesce(row.volume_24h_usd, t.volume24hUsd),
    t.asOf = $asOf,
    t.ingestedBy = $ingestedBy
{deploy_and_create("admin_address", "fee_recipient_fid", "admin")}
"""

BANKR_CYPHER = f"""
UNWIND $rows AS row
MERGE (t:Token {{address: toLower(row.token_address), chainId: row.chain_id}})
SET t.name = coalesce(row.name, t.name),
    t.symbol = coalesce(row.symbol, t.symbol),
    t.platform = coalesce(row.platform, t.platform, $platform),
    t.deployedAt = coalesce(row.deployed_at, t.deployedAt),
    t.deployTxHash = coalesce(row.tx_hash, t.deployTxHash),
    t.poolAddress = coalesce(row.pool_address, t.poolAddress),
    t.launchType = coalesce(row.launch_type, t.launchType),
    t.discoverySource = coalesce(row.source, t.discoverySource),
    t.feeRecipientAddress = coalesce(row.fee_recipient_address, t.feeRecipientAddress),
    t.asOf = $asOf,
    t.ingestedBy = $ingestedBy
{deploy_and_create("fee_recipient_address", "fee_recipient_fid", "fee_recipient")}
"""

VOLUME_CYPHER = """
UNWIND $rows AS row
MERGE (t:Token {address: toLower(row.token_address), chainId: row.chain_id})
  ON CREATE SET t.platform = $platform
SET t.dexSwapCount = row.swap_count,
    t.dexVolumeUsd = row.volume_usd,
    t.dexVolumeNative = row.volume_native,
    t.dexVolumeFrom = row.first_day,
    t.dexVolumeTo = row.last_day,
    t.asOf = $asOf,
    t.ingestedBy = $ingestedBy
"""
