"""Writes (WarpcastAccount) and (WarpcastAccount)-[:ACCOUNT]->(Wallet).

The only place WarpcastAccount profile properties are set from an authoritative
source; everywhere else an account node is a stub created so an edge has
somewhere to land.

Wallet keys are lowercased, but only when they look like 0x-hex: `wallets.csv`
carries Solana addresses too, and base58 is case-sensitive, so a blanket
toLower() would key two different Solana wallets to the same node.
"""

from __future__ import annotations

from config.settings import USER_SCORE_PROPERTY

# The score property is configurable — the plan called for quotient's score and
# Neynar's is what exists today — so it is interpolated into Cypher, which means
# it has to be validated as an identifier first.
if not USER_SCORE_PROPERTY.isidentifier():
    raise RuntimeError(
        f"USER_SCORE_PROPERTY={USER_SCORE_PROPERTY!r} is not a valid Cypher property name"
    )

_WALLET_KEY = (
    "CASE WHEN row.address STARTS WITH '0x' THEN toLower(row.address) ELSE row.address END"
)

ACCOUNTS_CYPHER = f"""
UNWIND $rows AS row
MERGE (a:WarpcastAccount {{fid: row.fid}})
SET a.username = coalesce(row.username, a.username),
    a.displayName = coalesce(row.display_name, a.displayName),
    a.{USER_SCORE_PROPERTY} = coalesce(row.neynar_score, a.{USER_SCORE_PROPERTY}),
    a.followerCount = coalesce(row.follower_count, a.followerCount),
    a.followingCount = coalesce(row.following_count, a.followingCount),
    a.custodyAddress = coalesce(toLower(row.custody_address), a.custodyAddress),
    a.registeredAt = coalesce(row.registered_at, a.registeredAt),
    a.asOf = $asOf,
    a.ingestedBy = $ingestedBy
"""

WALLETS_CYPHER = f"""
UNWIND $rows AS row
MERGE (a:WarpcastAccount {{fid: row.fid}})
  ON CREATE SET a.asOf = $asOf, a.ingestedBy = $ingestedBy
MERGE (w:Wallet {{address: {_WALLET_KEY}}})
SET w.protocol = coalesce(row.protocol, w.protocol, 'eth'),
    w.asOf = $asOf,
    w.ingestedBy = $ingestedBy
MERGE (a)-[r:ACCOUNT]->(w)
SET r.isPrimary = coalesce(row.is_primary, false),
    r.protocol = coalesce(row.protocol, 'eth'),
    r.source = coalesce(row.source, 'verified'),
    r.asOf = $asOf,
    r.ingestedBy = $ingestedBy
"""
