"""Cypher fragments shared by more than one data type.

A fragment here is spliced into a larger statement rather than executed, so it
carries its own scope requirements — see each one's comment for what must
already be bound.
"""

from __future__ import annotations


# Attaches a wallet to its Farcaster account when the CSV carries an fid. The
# ACCOUNT edge's own properties are owned by `linked_wallets`, so this only ever
# fills them in on creation — a run of contract_deployers must not decide that
# someone's primary wallet is no longer primary. Requires `w` (the Wallet) to be
# in scope and `$source` in the parameters.
def optional_account_link(fid_column: str = "fid") -> str:
    return f"""
FOREACH (_ IN CASE WHEN row.{fid_column} IS NULL THEN [] ELSE [1] END |
  MERGE (linked:WarpcastAccount {{fid: row.{fid_column}}})
    ON CREATE SET linked.asOf = $asOf, linked.ingestedBy = $ingestedBy
  MERGE (linked)-[link:ACCOUNT]->(w)
    ON CREATE SET link.protocol = 'eth', link.isPrimary = false, link.source = $source
  SET link.asOf = $asOf, link.ingestedBy = $ingestedBy
)"""
