"""The hand-curated index of Arbitrum tokens and vaults we track.

Clanker/Bankr tokens are discovered dynamically; these are the blue-chip
positions that say "this Farcaster user is actually in the Arbitrum economy".
All addresses lowercase — every join in this repo keys on lowercase hex.
"""

from config.settings import CHAIN_ARBITRUM

# kind: "token" for plain ERC-20s, "vault" for ERC-4626 share tokens.
POPULAR_TOKENS = [
    {
        "address": "0x912ce59144191c1204e64559fe8253a0e49e6548",
        "symbol": "ARB",
        "name": "Arbitrum",
        "chain_id": CHAIN_ARBITRUM,
        "decimals": 18,
        "kind": "token",
    },
    {
        "address": "0x0c880f6761f1af8d9aa9c466984b80dab9a8c9e8",
        "symbol": "PENDLE",
        "name": "Pendle",
        "chain_id": CHAIN_ARBITRUM,
        "decimals": 18,
        "kind": "token",
    },
    {
        "address": "0x46777c76dbbe40fabb2aab99e33ce20058e76c59",
        "symbol": "L3",
        "name": "Layer3",
        "chain_id": CHAIN_ARBITRUM,
        "decimals": 18,
        "kind": "token",
    },
]

# Gauntlet-curated MetaMorpho (ERC-4626) vaults on Arbitrum One. Depositing here
# is the "gauntlet stables" leg of the grant spec.
GAUNTLET_VAULTS = [
    {
        "address": "0x7e97fa6893871a2751b5fe961978dccb2c201e65",
        "symbol": "gtUSDCcore",
        "name": "Gauntlet USDC Core",
        "chain_id": CHAIN_ARBITRUM,
        "decimals": 18,
        "kind": "vault",
        "asset_symbol": "USDC",
        "asset_decimals": 6,
    },
    {
        "address": "0x7c574174da4b2be3f705c6244b4bfa0815a8b3ed",
        "symbol": "gtUSDCprime",
        "name": "Gauntlet USDC Prime",
        "chain_id": CHAIN_ARBITRUM,
        "decimals": 18,
        "kind": "vault",
        "asset_symbol": "USDC",
        "asset_decimals": 6,
    },
    {
        "address": "0xbd14bea2ecececd5f32149b0f84be7f7f446b964",
        "symbol": "gtWETHcore",
        "name": "Gauntlet WETH Core",
        "chain_id": CHAIN_ARBITRUM,
        "decimals": 18,
        "kind": "vault",
        "asset_symbol": "WETH",
        "asset_decimals": 18,
    },
    {
        "address": "0x139250cdb310d657eac506c7c7fc6acde34af1ec",
        "symbol": "gtUSDT0core",
        "name": "Gauntlet USDT0 Core",
        "chain_id": CHAIN_ARBITRUM,
        "decimals": 18,
        "kind": "vault",
        "asset_symbol": "USDT0",
        "asset_decimals": 6,
    },
]

INDEX_TOKENS = POPULAR_TOKENS + GAUNTLET_VAULTS

# ERC-4626 Deposit(address indexed sender, address indexed owner, uint256 assets, uint256 shares)
ERC4626_DEPOSIT_TOPIC0 = (
    "0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7"
)


def token_addresses(kind: str | None = None) -> list[str]:
    """Lowercase addresses from the index, optionally filtered by kind."""
    return [t["address"] for t in INDEX_TOKENS if kind is None or t["kind"] == kind]


def by_address(address: str) -> dict | None:
    target = address.lower()
    return next((t for t in INDEX_TOKENS if t["address"] == target), None)
