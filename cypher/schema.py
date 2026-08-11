"""The graph's uniqueness constraints and lookup indexes.

Every MERGE key used anywhere in this package is backed by a uniqueness
constraint here. That is not decoration: an unconstrained MERGE on a 1M-row
UNWIND does a full label scan per row, and two concurrent ingests can create
duplicate "unique" nodes. With the constraint in place MERGE becomes an index
lookup and the duplicate is impossible.

Constraint *type* is negotiated, not assumed. NODE KEY is an Enterprise feature;
a Community/Aura-Free target rejects it. Each spec therefore carries a ladder of
candidate statements — plain uniqueness first because it is the portable one,
NODE KEY second for the extra existence guarantee where it is available — and
`ingestion.constraints` takes the first statement the server accepts.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ConstraintSpec:
    """One MERGE key and the statements that could enforce it."""

    name: str
    label: str
    properties: tuple[str, ...]

    def statements(self) -> list[str]:
        target = ", ".join(f"n.{prop}" for prop in self.properties)
        return [
            f"CREATE CONSTRAINT {self.name} IF NOT EXISTS "
            f"FOR (n:{self.label}) REQUIRE ({target}) IS UNIQUE",
            f"CREATE CONSTRAINT {self.name} IF NOT EXISTS "
            f"FOR (n:{self.label}) REQUIRE ({target}) IS NODE KEY",
        ]


# The MERGE keys, verbatim from the graph schema. Token and Contract are keyed on
# (address, chainId) because the same address is a different thing on a different
# chain — Bankr in particular deploys to Robinhood Chain (4663) and Arbitrum
# (42161) from the same deployer.
CONSTRAINTS: tuple[ConstraintSpec, ...] = (
    ConstraintSpec("warpcast_account_fid", "WarpcastAccount", ("fid",)),
    ConstraintSpec("wallet_address", "Wallet", ("address",)),
    ConstraintSpec("token_address_chain", "Token", ("address", "chainId")),
    ConstraintSpec("contract_address_chain", "Contract", ("address", "chainId")),
    ConstraintSpec("channel_id", "Channel", ("channelId",)),
    ConstraintSpec("chain_id", "Chain", ("chainId",)),
    ConstraintSpec("platform_name", "Platform", ("name",)),
)

# Lookups that are not MERGE keys but that analysts and the QA queries lean on:
# "which chain is this token on", "who is @username". Cheap to maintain, and
# without them every such query is a label scan.
INDEXES: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("token_address", "Token", ("address",)),
    ("contract_address", "Contract", ("address",)),
    ("warpcast_account_username", "WarpcastAccount", ("username",)),
)


def index_statement(name: str, label: str, properties: tuple[str, ...]) -> str:
    target = ", ".join(f"n.{prop}" for prop in properties)
    return f"CREATE INDEX {name} IF NOT EXISTS FOR (n:{label}) ON ({target})"
