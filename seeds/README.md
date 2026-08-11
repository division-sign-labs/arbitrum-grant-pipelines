# Seed files

Two inputs the pipelines cannot derive for themselves. Drop them here as CSVs;
`lib/seeds.py` also looks in `data/<name>/` if you keep them elsewhere.

## `miniapp_builders.csv`

Farcaster accounts that shipped a miniapp integrating Arbitrum. Drives
`pipelines/miniapp_builders.py`, which resolves each fid to its verified wallets
and measures their Arbitrum activity.

| column     | required | notes                          |
|------------|----------|--------------------------------|
| `fid`      | yes      | integer Farcaster ID           |
| `username` | no       | for readability only           |
| `app_name` | no       | recorded on the graph node     |
| `app_url`  | no       | recorded on the graph node     |

```csv
fid,username,app_name,app_url
5650,vitalik.eth,Example Miniapp,https://example.xyz
```

## `brand_accounts.csv`

The Arbitrum-brand accounts that engagement is measured against — Arbitrum,
Offchain Labs, and whoever else counts. Drives `pipelines/brand_engagement.py`.

| column   | required | notes                                                        |
|----------|----------|--------------------------------------------------------------|
| `fid`    | yes      | integer Farcaster ID                                          |
| `name`   | no       | label for the node                                            |
| `weight` | no       | multiplier on this account's engagement score; defaults to 1.0 |

```csv
fid,name,weight
1234,arbitrum,1.0
5678,offchainlabs,0.8
```

Per-action weights (reply 3, recast 2, mention 2, like 1) live in
`config/settings.py: ENGAGEMENT_WEIGHTS` and multiply the per-account weight.

## Missing seeds

Pipelines that need a seed raise `SeedMissingError` naming the paths searched
and the expected schema. They do not silently produce an empty result.
