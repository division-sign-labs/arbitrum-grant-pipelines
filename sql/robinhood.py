"""DuneSQL for the Bankr token registry on Robinhood Chain (Arbitrum Orbit, 4663).

Why this file exists: the Bankr API returns only the 50 most recent launches and
accepts no pagination (verified). At the observed launch rate those 50 records
cover well under an hour, so any historical registry has to be reconstructed
from Dune's raw `robinhood.*` tables. Nothing here is a decoded Dune spellbook
table — Robinhood Chain is new enough that only the raw
transactions/logs/traces/creation_traces tables exist — so every event is
matched by topic0 and sliced out of `data` by hand.

Everything below was verified live against Dune on 2026-08-09. The three
non-obvious things it establishes:

1.  Bankr tokens are created by a single factory contract,
    `BANKR_TOKEN_FACTORY`. Checked twice against the API's most-recent launches,
    minutes apart: 39 of 47 tokens indexed the first time, 30 of 47 the second,
    and in both passes every indexed token traced back to that one creator and
    to nothing else. The shortfall each time is Dune's indexing lag against a
    launch feed under an hour old, not a second factory. The factory is itself a
    contract, deployed 2026-06-30 — the day before Robinhood Chain's mainnet
    launch. Re-run `factory_discovery_sql` to re-confirm; a second
    `creator_address` in its output is the signal to update `DEFAULT_FACTORIES`.

2.  **Launches are ERC-4337 user operations, so `robinhood.transactions."from"`
    is the bundler, not the launcher.** Over one six-hour window, 140 factory
    creations carried only 12 distinct `transactions."from"` values but 77
    distinct ERC-4337 `UserOperationEvent` senders. Using the transaction sender
    as `deployer_address` would collapse thousands of distinct launchers onto a
    dozen bundler EOAs and poison every wallet->fid join downstream. The real
    launcher is the UserOperation `sender` — a smart account, which is also what
    the Bankr API reports as `deployer.walletAddress`. `tokens_by_factory_sql`
    attributes it by log ordering: a UserOperation's logs precede its
    `UserOperationEvent`, so the launcher is the sender of the first such event
    emitted after the new token's own first log in the same transaction. The
    bundler is still returned, as `bundler_address`, because it is the honest
    answer to "who paid for this transaction".

3.  Trading happens on Uniswap v4, whose `Swap` event is keyed by a 32-byte
    `PoolId`, not by token address. The token->pool edge therefore has to come
    from the pool's `Initialize` event, which carries `(id, currency0,
    currency1)` in its topics. `token_volume_sql` builds that map first and
    aggregates swaps through it.

4.  **The fee recipient is recoverable from the launch's Doppler beneficiary
    arrays**, which matters because it is far likelier than the deployer to be a
    human's own EOA. Two contracts emit one, both as a bare
    `(address beneficiary, uint256 shares)[]`, and every launch uses exactly one
    of them:

      * `DOPPLER_V4_INITIALIZER` emits `BENEFICIARIES_TOPIC0` keyed by the token
        address in topic1. The protocol holds one entry (5%); a second entry, at
        95%, is the launcher's fee recipient.
      * `STREAMABLE_FEES_LOCKER` emits `LOCK_TOPIC0` keyed by pool id. The
        protocol holds two entries there; a third, the largest of the three, is
        the fee recipient.

    So the rule is structural rather than an address allow-list: an array longer
    than that contract's protocol baseline carries a launcher, and the launcher
    is its largest share. Verified on 2026-08-09 against 134 tokens labelled by
    the Bankr API (`GET /public/doppler/creator-fees/<wallet>` enumerates the
    tokens a wallet receives fees for): 130 matched on the first attempt, and
    the four misses were the same 5/95 shape with an older protocol address —
    which is exactly why an allow-list was rejected and the share ordering used
    instead. Over a three-day window the two events cover ~99% of launches
    (4442 via the initializer, 464 via the locker, of 4925). `fee_recipient_source`
    records which one answered.

A note on scale: the factory has produced ~67k contracts since the chain opened,
and the sampled distribution is overwhelmingly one shape (a 53-byte proxy whose
address ends in the `ba3` vanity suffix Doppler mines for). The suffix filter is
exposed as an option rather than forced on, because a launcher changing its salt
should degrade the registry's precision, not silently empty it.

Caveat worth carrying into the manifest: this factory is the Doppler token
factory as deployed on Robinhood Chain. Bankr is by far its dominant consumer,
but any other Doppler frontend pointing at the same factory would land in this
registry too. The Bankr API rows (source `bankr_api`) are the only ones we can
call Bankr-attributed with certainty.
"""

from __future__ import annotations

from typing import Iterable, Sequence

from lib import sqlfmt

# --- verified addresses and topics ---------------------------------------

# Creates every Bankr/Doppler token on Robinhood Chain. Deployed 2026-06-30 by
# 0x4482f353a46a4d4088f9550eb2c9cc92d0d5f768.
BANKR_TOKEN_FACTORY = "0x1b37d3a72082029c44b35b604ea473617580b69a"
DEFAULT_FACTORIES: tuple[str, ...] = (BANKR_TOKEN_FACTORY,)

# Uniswap v4 PoolManager on Robinhood Chain: the chain's busiest Swap emitter.
UNISWAP_V4_POOL_MANAGER = "0x8366a39cc670b4001a1121b8f6a443a643e40951"

# Canonical ERC-4337 v0.7 EntryPoint (same address on every chain).
ERC4337_ENTRYPOINT = "0x0000000071727de22e5e9d8baf0edac6f37da032"

# Wrapped native on Robinhood Chain — the numeraire Bankr pools are quoted in,
# and the chain's single busiest Transfer emitter.
WRAPPED_NATIVE = "0x0bd7d308f8e1639fab988df18a8011f41eacad73"

# UserOperationEvent(bytes32 indexed userOpHash, address indexed sender,
#                    address indexed paymaster, uint256 nonce, bool success,
#                    uint256 actualGasCost, uint256 actualGasUsed)
USER_OPERATION_EVENT_TOPIC0 = (
    "0x49628fd1471006c1482da88028e9ce4dbb080b815c9b0344d39e5a8e6ec1419f"
)

# Uniswap v4 Initialize(PoolId indexed id, Currency indexed currency0,
#   Currency indexed currency1, uint24 fee, int24 tickSpacing, IHooks hooks,
#   uint160 sqrtPriceX96, int24 tick) — 3 indexed topics, 160 bytes of data.
V4_INITIALIZE_TOPIC0 = (
    "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438"
)

# Uniswap v4 Swap(PoolId indexed id, address indexed sender, int128 amount0,
#   int128 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick,
#   uint24 fee) — 2 indexed topics, 192 bytes of data. amount0/amount1 are
# signed pool-side deltas, so volume is their absolute value.
V4_SWAP_TOPIC0 = (
    "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f"
)

# Doppler mines a salt so the token address sorts above its numeraire; on this
# chain that shows up as a constant 3-hex-digit tail.
TOKEN_ADDRESS_SUFFIX = "ba3"

# --- fee recipients -------------------------------------------------------

# The Doppler v4 pool initializer, reported by the Bankr API as each launch's
# `initializer`. Emits the launch's own fee split, keyed by the token.
DOPPLER_V4_INITIALIZER = "0x4e3468951d49f2eea976ed0d6e75ffcb44a9a544"

# Doppler's StreamableFeesLocker, reported by the Bankr API as `feesContract`.
# Emits the locked position's fee split, keyed by pool id.
STREAMABLE_FEES_LOCKER = "0x9982538f41f2ae29ddb9d3d9307010052984fdbb"

# <initializer>(address indexed asset, (address,uint256)[] beneficiaries)
BENEFICIARIES_TOPIC0 = (
    "0x5be4f748347693e0500df872d81f7d96bce1b98e6f5adff0cfddfe3e9e415f20"
)

# <locker>(bytes32 indexed poolId, (address,uint256)[] beneficiaries)
LOCK_TOPIC0 = "0x0c90f8fcadd900399eb6c30bc91ec4531380b92bc2c4c364675528b1d30601e2"

# How many entries each emitter's array holds before any launcher is added: the
# initializer keeps one protocol entry, the locker two. An array longer than
# this is the only thing that says "a launcher was given a cut".
INITIALIZER_PROTOCOL_ENTRIES = 1
LOCKER_PROTOCOL_ENTRIES = 2

# A single dynamic array as an event's only non-indexed argument is ABI-encoded
# as [offset=0x20][length][pair, pair, ...], two words per pair. Checking the
# offset word is what tells us the layout has not changed under us.
_ARRAY_HEAD_OFFSET = (
    "0x0000000000000000000000000000000000000000000000000000000000000020"
)


def _factory_list(factories: Sequence[str] | None) -> str:
    return sqlfmt.address_list(factories if factories else DEFAULT_FACTORIES)


def factory_discovery_sql(token_addresses: Iterable[str]) -> str:
    """Which contract created these tokens? The answer is the factory.

    Cross-references the token addresses the Bankr API hands us against
    `robinhood.creation_traces`; the `from` column on a creation trace is the
    immediate creator, so grouping by it names the factory. Run this whenever
    Bankr might have redeployed — a sudden second `creator_address` in the
    output is the signal that `DEFAULT_FACTORIES` needs updating.
    """
    return f"""
SELECT
    '0x' || lower(to_hex(ct."from"))  AS creator_address,
    count(*)                          AS tokens_created,
    count(DISTINCT ct.address)        AS distinct_tokens,
    min(ct.block_time)                AS first_seen_at,
    max(ct.block_time)                AS last_seen_at,
    min(length(ct.code))              AS min_code_len,
    max(length(ct.code))              AS max_code_len
FROM robinhood.creation_traces ct
WHERE ct.address IN ({sqlfmt.address_list(token_addresses)})
GROUP BY 1
ORDER BY tokens_created DESC
"""


def _fee_recipient_ctes(ts: str) -> str:
    """CTEs resolving one fee recipient per token from the two beneficiary events.

    Both emitters encode the same `(address, uint256)[]`, so one decode serves
    both; they differ only in how a row is keyed back to a token and in how many
    entries the protocol occupies. The number of entries is derived from the
    data length rather than read out of the length word, so a malformed length
    can never drive the UNNEST, and the offset word is checked so a change to
    the event's argument list shows up as missing rows instead of as garbage
    addresses.
    """
    return f"""
-- The locker keys its event by pool id, not by token, so it is attributed by
-- log position: a launch owns the logs from where its token first speaks up to
-- where the next launch in the same bundled transaction starts.
launch_windows AS (
    SELECT
        tl.tx_hash,
        tl.token,
        tl.first_index,
        lead(tl.first_index) OVER (
            PARTITION BY tl.tx_hash ORDER BY tl.first_index
        ) AS next_index
    FROM token_logs tl
),
fee_events AS (
    SELECT
        c.token,
        1 AS priority,
        {INITIALIZER_PROTOCOL_ENTRIES} AS protocol_entries,
        l.data
    FROM robinhood.logs l
    JOIN creations c ON c.token = bytearray_substring(l.topic1, 13, 20)
    WHERE l.contract_address = {sqlfmt.address(DOPPLER_V4_INITIALIZER)}
      AND l.topic0 = {sqlfmt.hash_literal(BENEFICIARIES_TOPIC0)}
      AND l.block_time >= {ts}
    UNION ALL
    SELECT
        w.token,
        2 AS priority,
        {LOCKER_PROTOCOL_ENTRIES} AS protocol_entries,
        l.data
    FROM robinhood.logs l
    JOIN launch_windows w
      ON w.tx_hash = l.tx_hash
     AND l.index > w.first_index
     AND (w.next_index IS NULL OR l.index < w.next_index)
    WHERE l.contract_address = {sqlfmt.address(STREAMABLE_FEES_LOCKER)}
      AND l.topic0 = {sqlfmt.hash_literal(LOCK_TOPIC0)}
      AND l.block_time >= {ts}
),
fee_arrays AS (
    SELECT e.token, e.priority, e.data,
           cast((length(e.data) - 64) / 64 AS integer) AS entries
    FROM fee_events e
    WHERE bytearray_substring(e.data, 1, 32) = {sqlfmt.hash_literal(_ARRAY_HEAD_OFFSET)}
      AND (length(e.data) - 64) % 64 = 0
      AND length(e.data) >= 64 + (e.protocol_entries + 1) * 64
),
fee_shares AS (
    SELECT
        a.token,
        a.priority,
        bytearray_substring(a.data, 77 + (i - 1) * 64, 20)              AS beneficiary,
        bytearray_to_uint256(bytearray_substring(a.data, 97 + (i - 1) * 64, 32)) AS shares
    FROM fee_arrays a
    CROSS JOIN UNNEST(sequence(1, a.entries)) AS s(i)
),
-- Largest share wins, and the initializer wins over the locker when a token
-- somehow has both. `beneficiary` breaks a share tie so the run is repeatable.
fee_recipients AS (
    SELECT
        token,
        priority,
        beneficiary,
        row_number() OVER (
            PARTITION BY token ORDER BY priority, shares DESC, beneficiary
        ) AS rn
    FROM fee_shares
)"""


def tokens_by_factory_sql(
    factory_addresses: Sequence[str] | None,
    since,
    require_suffix: bool = False,
    resolve_launcher: bool = True,
) -> str:
    """The historical Bankr token registry: every token a factory has created.

    `deployer_address` is the ERC-4337 UserOperation sender (the launcher's
    smart account), falling back to the transaction sender when no
    `UserOperationEvent` can be matched — a launch submitted as a plain
    transaction rather than a user operation would take that path. See this
    module's docstring for why the transaction sender alone is not usable.

    `fee_recipient_address` is the launcher's cut of the Doppler fee split, and
    `fee_recipient_source` says which of the two emitters supplied it. It is the
    more useful of the two wallets for attribution: the deployer is a smart
    account, whereas the fee recipient is usually an EOA the launcher controls
    and therefore one a Farcaster account may have verified.

    Set `resolve_launcher=False` to skip the extra scans of `robinhood.logs` and
    return only what `creation_traces` knows; the query gets much cheaper,
    `deployer_address` falls back to the transaction sender, and the fee
    recipient — which needs the log ordering too — comes back null.
    """
    factories = _factory_list(factory_addresses)
    ts = sqlfmt.timestamp(since)

    suffix_filter = ""
    if require_suffix:
        # to_hex() yields 40 uppercase hex chars with no 0x prefix, so the last
        # three characters start at position 38.
        suffix_filter = (
            f"\n      AND lower(substr(to_hex(ct.address), 38, 3)) = "
            f"{sqlfmt.text(TOKEN_ADDRESS_SUFFIX)}"
        )

    if not resolve_launcher:
        return f"""
WITH creations AS (
    SELECT ct.address AS token, ct.tx_hash, ct.block_time
    FROM robinhood.creation_traces ct
    WHERE ct."from" IN ({factories})
      AND ct.block_time >= {ts}{suffix_filter}
)
SELECT
    '0x' || lower(to_hex(c.token))    AS token_address,
    '0x' || lower(to_hex(t."from"))   AS deployer_address,
    '0x' || lower(to_hex(t."from"))   AS bundler_address,
    cast(NULL AS varchar)             AS fee_recipient_address,
    cast(NULL AS varchar)             AS fee_recipient_source,
    '0x' || lower(to_hex(c.tx_hash))  AS tx_hash,
    c.block_time                      AS deployed_at
FROM creations c
LEFT JOIN robinhood.transactions t
       ON t.hash = c.tx_hash
      AND t.block_time >= {ts}
ORDER BY deployed_at DESC
"""

    return f"""
WITH creations AS (
    SELECT ct.address AS token, ct.tx_hash, ct.block_time
    FROM robinhood.creation_traces ct
    WHERE ct."from" IN ({factories})
      AND ct.block_time >= {ts}{suffix_filter}
),
-- Where the new token first speaks in its own transaction. Creation traces
-- carry no log index, so the token's own first log is what locates it against
-- the UserOperationEvent boundaries.
token_logs AS (
    SELECT l.tx_hash, l.contract_address AS token, min(l.index) AS first_index
    FROM robinhood.logs l
    JOIN creations c
      ON c.tx_hash = l.tx_hash
     AND c.token = l.contract_address
    WHERE l.block_time >= {ts}
    GROUP BY 1, 2
),
user_ops AS (
    SELECT
        l.tx_hash,
        l.index AS op_index,
        bytearray_substring(l.topic2, 13, 20) AS sender
    FROM robinhood.logs l
    WHERE l.contract_address = {sqlfmt.address(ERC4337_ENTRYPOINT)}
      AND l.topic0 = {sqlfmt.hash_literal(USER_OPERATION_EVENT_TOPIC0)}
      AND l.block_time >= {ts}
),
-- A bundler batches many user operations into one transaction and each one's
-- logs precede its own UserOperationEvent, so the launcher is the sender of the
-- nearest such event following the token's first log.
matched AS (
    SELECT
        tl.tx_hash,
        tl.token,
        u.sender,
        row_number() OVER (
            PARTITION BY tl.tx_hash, tl.token ORDER BY u.op_index
        ) AS rn
    FROM token_logs tl
    JOIN user_ops u
      ON u.tx_hash = tl.tx_hash
     AND u.op_index > tl.first_index
),{_fee_recipient_ctes(ts)}
SELECT
    '0x' || lower(to_hex(c.token))                       AS token_address,
    '0x' || lower(to_hex(coalesce(m.sender, t."from")))  AS deployer_address,
    '0x' || lower(to_hex(t."from"))                      AS bundler_address,
    CASE WHEN m.sender IS NOT NULL THEN 'erc4337_userop'
         ELSE 'tx_sender' END                            AS deployer_source,
    '0x' || lower(to_hex(f.beneficiary))                 AS fee_recipient_address,
    CASE WHEN f.priority = 1 THEN 'doppler_initializer'
         WHEN f.priority = 2 THEN 'streamable_fees_locker' END
                                                         AS fee_recipient_source,
    '0x' || lower(to_hex(c.tx_hash))                     AS tx_hash,
    c.block_time                                         AS deployed_at
FROM creations c
LEFT JOIN matched m
       ON m.tx_hash = c.tx_hash
      AND m.token = c.token
      AND m.rn = 1
LEFT JOIN fee_recipients f
       ON f.token = c.token
      AND f.rn = 1
LEFT JOIN robinhood.transactions t
       ON t.hash = c.tx_hash
      AND t.block_time >= {ts}
ORDER BY deployed_at DESC
"""


def token_volume_sql(
    token_addresses: Iterable[str] | None,
    since,
    factory_addresses: Sequence[str] | None = None,
    registry_since=None,
) -> str:
    """Daily Uniswap v4 swap counts and volume per token.

    `since` bounds the swaps being aggregated. `registry_since` optionally
    bounds the token set and the pool map; leave it None (the default) so both
    are read over all history, otherwise an incremental run drops the volume of
    every token launched before its window.

    Pass `token_addresses=None` — the preferred path — to price every token the
    factories have created. The token set is then resolved inside the query as a
    semi-join against `creation_traces` instead of being shipped as a literal
    IN-list, which is what makes a ~67k-token backfill a single execution rather
    than 34 chunked ones.

    Volume is measured on the numeraire side of the pair (the side that is not
    the launched token), because that is the leg with a price.

    `prices.usd` does cover chain 4663, but only two assets: WETH
    (`WRAPPED_NATIVE`, 18 decimals) and USDG (6 decimals). WETH is the numeraire
    for the large majority of Bankr pools — 102 of the 119 pooled tokens in a
    sampled window — so most rows do get a USD figure.

    Three columns, three different guarantees:

    * `swap_count` is exact. Every leg is counted whether or not it can be
      priced, so this is the honest activity measure.
    * `volume_usd` is complete for pools quoted in WETH or USDG and null when a
      token traded only against something `prices.usd` does not carry. It is a
      floor on days when a token traded in both kinds of pool.
    * `volume_native` covers the same priced legs as `volume_usd`. It is the
      weakest of the three: a token that traded against both WETH and USDG in
      one day has their units added together, so prefer `volume_usd` for
      anything comparative.

    Unpriced legs are deliberately excluded from both volume columns rather than
    folded in at an assumed 18 decimals — Doppler also builds token/token pools,
    and adding raw units of an unknown token to WETH yields a number denominated
    in nothing.
    """
    ts = sqlfmt.timestamp(since)
    # `since` bounds the swaps we are aggregating, but it must NOT bound the
    # token set or the pool map: a token launched last week and traded today
    # would lose its Initialize event and its volume would silently vanish from
    # every incremental run. Those two CTEs are unbounded unless the caller
    # explicitly narrows them.
    registry_bound = (
        f"\n      AND ct.block_time >= {sqlfmt.timestamp(registry_since)}"
        if registry_since is not None
        else ""
    )
    pool_bound = (
        f"\n      AND l.block_time >= {sqlfmt.timestamp(registry_since)}"
        if registry_since is not None
        else ""
    )

    if token_addresses is None:
        token_source = f"""
    SELECT DISTINCT ct.address AS token
    FROM robinhood.creation_traces ct
    WHERE ct."from" IN ({_factory_list(factory_addresses)}){registry_bound}"""
    else:
        token_source = f"""
    SELECT t.token
    FROM (VALUES {_token_values(token_addresses)}) AS t(token)"""

    return f"""
WITH toks AS ({token_source}
),
-- Uniswap v4 keys everything by PoolId, so the token->pool edge only exists in
-- the pool's Initialize event. Scanned over all history on purpose — see above.
pools AS (
    SELECT
        l.topic1                                AS pool_id,
        bytearray_substring(l.topic2, 13, 20)   AS currency0,
        bytearray_substring(l.topic3, 13, 20)   AS currency1
    FROM robinhood.logs l
    WHERE l.contract_address = {sqlfmt.address(UNISWAP_V4_POOL_MANAGER)}
      AND l.topic0 = {sqlfmt.hash_literal(V4_INITIALIZE_TOPIC0)}{pool_bound}
),
-- Doppler sorts the launched token above its numeraire, but do not rely on it:
-- match the token on either side and label the other side the numeraire.
token_pools AS (
    SELECT p.pool_id, p.currency1 AS token, p.currency0 AS numeraire, true AS token_is_1
    FROM pools p JOIN toks t ON t.token = p.currency1
    UNION ALL
    SELECT p.pool_id, p.currency0 AS token, p.currency1 AS numeraire, false AS token_is_1
    FROM pools p JOIN toks t ON t.token = p.currency0
),
swaps AS (
    SELECT
        l.topic1                                                     AS pool_id,
        cast(l.block_time AS date)                                   AS day,
        bytearray_to_int256(bytearray_substring(l.data,  1, 32))     AS amount0,
        bytearray_to_int256(bytearray_substring(l.data, 33, 32))     AS amount1
    FROM robinhood.logs l
    WHERE l.contract_address = {sqlfmt.address(UNISWAP_V4_POOL_MANAGER)}
      AND l.topic0 = {sqlfmt.hash_literal(V4_SWAP_TOPIC0)}
      AND l.block_time >= {ts}
),
legs AS (
    SELECT
        tp.token,
        s.day,
        -- Uniswap v4 denotes native ETH as the zero address, but prices.usd
        -- only carries the wrapped token. Quote the two as one asset or every
        -- native-paired pool silently loses its USD figure.
        CASE WHEN tp.numeraire = 0x0000000000000000000000000000000000000000
             THEN {sqlfmt.address(WRAPPED_NATIVE)}
             ELSE tp.numeraire END AS numeraire,
        -- Cast to double before abs(): the amounts are DuneSQL int256, and
        -- double is the type the division below needs anyway.
        abs(cast(
            CASE WHEN tp.token_is_1 THEN s.amount0 ELSE s.amount1 END AS double
        )) AS numeraire_raw
    FROM swaps s
    JOIN token_pools tp ON tp.pool_id = s.pool_id
),
agg AS (
    SELECT token, day, numeraire,
           count(*)          AS swap_count,
           sum(numeraire_raw) AS numeraire_raw
    FROM legs
    GROUP BY 1, 2, 3
),
-- A daily average rather than a per-minute lookup: prices.usd covers only
-- about half the minutes on this chain, and joining minute-to-minute priced a
-- handful of swaps per day and silently dropped the rest, making volume_usd
-- read orders of magnitude low. Day is also the grain we report at.
px AS (
    SELECT
        pr.contract_address,
        cast(pr.minute AS date) AS day,
        avg(pr.price)           AS price,
        max(pr.decimals)        AS decimals
    FROM prices.usd pr
    WHERE pr.blockchain = 'robinhood'
      AND pr.minute >= date_trunc('day', {ts})
    GROUP BY 1, 2
)
SELECT
    '0x' || lower(to_hex(a.token))                          AS token_address,
    a.day                                                   AS day,
    -- Every leg counts here, priced or not: a swap happened.
    sum(a.swap_count)                                       AS swap_count,
    -- Both volume columns cover priced legs only. A token can trade against
    -- several numeraires in a day (Doppler also builds token/token pools), and
    -- summing raw WETH units together with units of some other token would
    -- produce a number denominated in nothing at all.
    sum(CASE WHEN px.price IS NULL THEN NULL
             ELSE a.numeraire_raw
                  / power(10, px.decimals) END)             AS volume_native,
    sum(CASE WHEN px.price IS NULL THEN NULL
             ELSE a.numeraire_raw
                  / power(10, px.decimals) * px.price END)  AS volume_usd
FROM agg a
LEFT JOIN px
       ON px.contract_address = a.numeraire
      AND px.day = a.day
GROUP BY 1, 2
ORDER BY a.day DESC, swap_count DESC
"""


def _token_values(token_addresses: Iterable[str]) -> str:
    """`(0xaaa...), (0xbbb...)` — a VALUES list of varbinary address literals."""
    rendered = [f"({sqlfmt.address(a)})" for a in token_addresses]
    if not rendered:
        raise sqlfmt.SqlLiteralError("empty token address list")
    return ", ".join(rendered)
