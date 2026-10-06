# perp-maker: design

A market-making bot for the Sigma perpetuals engine, and, for the shadow run only, a flow
simulator that trades against it so that the market has fills, candles and funding that mean
something. One binary, two roles, each behind its own flag and its own key.

This document is the design; the crate (`crates/maker`) follows it. It sits in this workspace
next to the liquidator and the cranker and shares `perp-bot-common` (the full node, keys, the
price service, alerts, the deployment file).

## 1. What a maker on this engine has to live with

- **Prices arrive every three seconds, signed, and anyone may relay them.** The maker's
  reference price is the price service's latest signed update, which it also relays in front
  of its own transaction, so it never quotes on a price older than it has to. There is no
  faster feed: the engine's mark price follows the feed TWAP, and the book is matched against
  the index at most 5% away.
- **Every quote change is a transaction** (about 1 s to finality, 0.001 HANEUL of gas). The
  bot cannot stream quotes; it re-quotes in rounds, and a round is one atomic PTB: relay the
  prices, open a session, cancel what is resting, post the new ladder, close the session.
  Either the whole new book lands or nothing changes.
- **The book is public and slow, so the maker will be picked off** when the exchanges move
  faster than one oracle round. The defences are the spread (never below what one round of
  BTC volatility can move), re-quoting as soon as the reference moves, smaller size further
  out, and an inventory limit that stops the bleeding. In the shadow run there is nobody to
  pick the maker off; the parameters still have to be the ones that survive later.
- **A new position starts at a margin ratio of 1.0** (no leverage) unless the account sets
  its own; the maker sets it to the market's initial margin ratio once, so that a ladder of
  $20k needs $2k of margin and not $20k. The engine allocates the margin a session's resting
  orders need from the account's unallocated balance when the session ends with
  `allocate_missing_margin = true`.
- **Orders can carry an expiry.** Every quote expires `--expire-secs` after it was posted, so
  that if the process dies the book is empty within a minute instead of resting on a stale
  price forever. The engine cancels expired makers as takers meet them.
- **Post-only exists** (order type 2) and aborts the whole session when a level would cross.
  The simulation that precedes every send reports that, and the round is rebuilt with that
  level moved out one tick; a second failure skips the level.

## 2. The quoting model

All prices are in USD, sizes in base units, inventory `q` is the signed position size.

### Reference price

```
p   = latest signed price of the market's base feed (the service's /v1/updates)
```

The feed TWAP is not used for quoting: it lags the spot by design (a minute), and quoting on
it would give the first taker the difference. It is used by the engine for the mark, so the
maker's margin is judged on it; that only matters at the inventory limit (§ inventory).

### Inventory skew

```
skew = -k_skew * clamp(q / q_max, -1, 1)          (in bps; k_skew default = 2 × half-spread)
mid  = p × (1 + skew)
```

Long inventory lowers the whole ladder so that asks are hit first and bids less; short raises
it. At the limit the skew equals the full spread: the reducing side sits at the reference price
and the growing side one full spread away.

### Half-spread

```
s = max(s_min, s_base + k_vol × σ_3s + k_conf × confidence_bps)
```

- `s_base` (default 8 bp) is what the maker wants to earn per round trip above the 2 bp maker
  fee it is charged (fee tiers may turn that into a rebate later).
- `σ_3s` is the EWMA of absolute log returns of the signed price between rounds, in bps: what
  one oracle round can move. `k_vol` default 2: the spread covers two rounds of typical motion.
- `confidence_bps` is the signed update's own confidence interval (the venues' disagreement).
- `s_min` default 5 bp keeps the ladder honest when everything is calm.

### Levels

`n` levels per side (default 5). Level `i` (1-based) sits at

```
bid_i = mid × (1 - s - (i-1) × step)      ask_i = mid × (1 + s + (i-1) × step)
size_i = size_base × (1 + growth)^(i-1)
```

rounded to the tick (bids down, asks up) and the lot (down). `step` default 6 bp, `size_base`
default 0.01 BTC, `growth` default 0.5. Outer levels are bigger because they are hit less often
and each fill there is worth more spread. A level whose value is under the market's minimum
order value is dropped.

### Inventory limits

| Condition | Action |
|---|---|
| `abs(q) < q_max` | Both sides quoted, skewed |
| `abs(q) >= q_max` (default 0.25 BTC) | The growing side is not quoted; the reducing side is quoted reduce-only, skewed to the reference price |
| `abs(q) >= q_hard` (default 2 × q_max) | As above, and an immediate-or-cancel reduce-only order for `abs(q) - q_max` at up to `--flatten-slippage-bps` (default 20) through the book, each round until back under `q_max` |
| margin health (margin / maintenance) below `--min-health` (default 3.0) | Growing side off until it recovers; alert |

The maker's own margin and maintenance come from the position object and `perp-engine`'s
formulas, the same the liquidator judges by, so the maker sees the liquidation coming the same
way its own liquidator would.

### When to re-quote

A round is sent when any of these holds, at most once per `--min-interval-secs` (default 3):

1. the reference moved more than `--requote-bps` (default 3) from the reference of the last
   round;
2. `--refresh-secs` (default 20) passed since the last round (keeps the expiry alive);
3. the position's resting sizes or `q` changed since the last round (a fill happened);
4. the spread or the inventory state changed the ladder shape.

When none holds, nothing is sent and no gas is spent. A quiet market costs about 3 rounds a
minute; a busy one, one per oracle round.

### The round, as one PTB

```
update_price_feed(base)                       when the service has a newer update
update_price_feed(collateral)                 only when the stored one is older than 15 s
cancel_orders(ch, cap, account, [order ids])  the previous round's orders, on the shared object
start_session(ch, cap, account, feeds, none, clock) -> hp
place_limit_order(hp, side, size, price, 2 /* post-only */, client id, reduce_only, expiry) × levels
end_session(hp, …, allocate_missing_margin = true, deallocate_free_collateral = false)
```

`cancel_orders` takes the clearing house as a shared object and `start_session` then takes it
by value into the hot potato, so the cancel goes first in the same PTB; the order IDs are the
ones the previous round posted, kept in memory and reconciled against the position's pending
sizes (an ID the engine no longer knows, because the order filled or expired, is dropped from
the list before the round is built, since a cancel of an unknown order aborts). The session
ends through `fees::end_session(hp, cap, account, registry, schedule, tiers, true, false, clock)`
when the deployment carries the fee objects (volume and maker rebates are only credited
through it) and through the core `end_session` otherwise.

Simulated first; signed only when the simulation succeeds; the result waited for the
checkpoint. The order IDs come back in the simulation's return values and the execution's
`PostedOrder` events.

### Kill switches

- Oracle update older than `--max-price-age-secs` (default 8, under the market's 10 s
  tolerance): cancel everything, quote nothing, alert. Resume when fresh.
- Confidence wider than `--max-confidence-bps` (default 50): as above.
- Market paused or closed: quote nothing.
- `--max-failures` PTB failures in a row (default 3): stop, alert critical, try again every
  `--retry-secs`.
- Gas balance under `--min-gas-balance`: alert; under a tenth of it: stop.
- Shutdown (SIGTERM, ctrl-c): one last round that only cancels.

## 3. The flow simulator (shadow run only)

`--flow` turns on a second role with its own key and engine account: a taker that trades
against the maker so that fills, candles, open interest and funding exist.

- Every `Exp(mean = --flow-mean-secs)` seconds (default 45) it sends one immediate-or-cancel
  limit order (type 3) at the reference price ± `--flow-slippage-bps` (default 15), of size
  lognormal around `--flow-size` (default 0.01 BTC, σ = 0.6, capped at 5×), side random with a
  pull toward its own flat inventory (P(buy) = 0.5 − 0.4 × clamp(q_flow / q_flow_max, −1, 1)).
- Beyond `--flow-max-position` (default 0.2 BTC) only reducing orders.
- It is refused on a deployment whose collateral type is not a `tusd` module, and it logs
  loudly that it is on; it is a shadow-run tool and never a product feature.

The simulator reads the same reference price and the same position object; its PTB is relay +
session + one order + end session. It gives the maker what the maker needs to be tested:
adverse fills, inventory, skew, the reduce-only side, and a chart.

## 4. What the bot reads and writes

| Reads | From |
|---|---|
| Signed updates (price, confidence, timestamp) | the price service, each round |
| Clearing house: paused, lot, tick, min order value, max pending orders, margin ratios | the full node, at start and every `--params-refresh-secs` |
| The maker's position: `base`, `pending_bids`, `pending_asks`, `collateral`, funding fields | the full node: dynamic field `keys::PositionKey { account_id }` of the clearing house (`derive_dynamic_child_id`), every round |
| The account's unallocated balance, the key's gas | the full node |
| Reference gas price | the full node |

No indexer: the maker must keep quoting while the indexer is behind, and everything it needs is
on chain in two objects. (The liquidator needs the indexer because it watches everybody's
positions; the maker watches only its own.)

Writes: one PTB per round from the maker key; one per order from the flow key.

## 5. Setup checks (refuse to start on any mismatch)

Chain ID; key against `--address`; the account exists, is the engine's, and the cap is over it
and held by the key; the clearing house and feeds are what the deployment says; the price
service answers and signs for the market's source; the position object exists (or
`--create-position` opens it, one transaction) and the account's margin ratio on the market is
set (or `--set-leverage` sets it); the maker's collateral covers the full ladder at that margin
ratio with the configured limits; the flow key, if on, is a different key with its own account.

## 6. Observability

`/health` (503 while enabled and no round landed for 2 × `--refresh-secs`, or a kill switch is
on), `/status` (reference, mid, spread, the ladder as posted, inventory, margin health, last 50
rounds with digests and outcomes, open kill switches), `/metrics` (`perp_maker_` prefix:
quotes per side, half-spread bps, inventory, margin health, fills since start, rounds and
failures, gas, price age). Alerts through the common webhook: started, kill switch on/off,
inventory limit reached, failures, low gas.

## 7. Parameters (shadow run defaults)

| Flag | Default | Meaning |
|---|---|---|
| `--levels` | 5 | levels per side |
| `--half-spread-bps` | 8 | `s_base` |
| `--min-half-spread-bps` | 5 | `s_min` |
| `--vol-multiplier` | 2.0 | `k_vol` |
| `--conf-multiplier` | 0.5 | `k_conf` |
| `--level-step-bps` | 6 | spacing |
| `--size` | 0.01 | base size (BTC) |
| `--size-growth` | 0.5 | per level |
| `--max-position` | 0.25 | `q_max` (BTC) |
| `--hard-position` | 0.5 | `q_hard` |
| `--skew-bps` | 2 × half-spread | `k_skew` at the limit |
| `--requote-bps` | 3 | move that triggers a round |
| `--refresh-secs` | 20 | heartbeat round |
| `--expire-secs` | 60 | quote expiry |
| `--min-interval-secs` | 3 | rate limit |
| `--max-price-age-secs` | 8 | kill switch |
| `--leverage` | market max (10) | position initial margin ratio = 1 / leverage |
| `--min-health` | 3.0 | stop growing below |

With these, the resting ladder is 0.01 + 0.015 + 0.0225 + 0.034 + 0.051 ≈ 0.13 BTC a side
(about $11k) and needs about $1.1k of margin at 10x; the maker's 400,000 TUSD allocation is
far above that, and the inventory limit of 0.25 BTC (about $21k) is a fraction of it.

## 8. Layout

```
crates/maker/src
  config.rs     flags (above), validated
  setup.rs      startup checks, create position, set leverage
  model.rs      pure: reference → (mid, spread, ladder) given inventory, params, rounding; unit-tested
  position.rs   the position object: derive the field ID, decode, margin through perp-engine
  ptb.rs        the round builder (relay, session, cancel, post, end), the flow order builder; unit-tested call shapes
  maker.rs      the loop: observe, decide, build, simulate, send, record
  flow.rs       the simulator loop
  status.rs, metrics.rs, main.rs   as the cranker's
scripts/localnet/maker_check.py   the localnet pass (below)
```

`perp-bot-common` gains: the position dynamic field read and the fee-object entries of the
deployment file (`fees.schedule`, `fees.tierRegistry`, `registry`), which the web's
`perp.<network>.json` already carries.

## 9. Verification

Unit tests on `model.rs`: skew direction and magnitude, spread floor and volatility term,
rounding to tick and lot on each side, level sizes and the minimum order value, the three
inventory states, the re-quote triggers. Unit tests on `ptb.rs`: the calls of a round in order,
post-only type and expiry on every level, the fee extension's `end_session` when configured.

`scripts/localnet/maker_check.py` on the held localnet stack, in phases: a startup refusal (wrong
cap); the first round posts `2 × levels` orders around the signed price; a 10 bp move of the
signed price re-quotes within one interval and the old orders are gone; a taker fill (the flow
role, one order) moves inventory and the next round is skewed; inventory pushed past `q_max`
leaves only the reduce-only side; past `q_hard` the flatten order appears; a stale price pulls
every quote; stopping the bot leaves the book empty after the expiry; the flow simulator alone
produces candles the indexer serves.

## 10. Rollout on the shadow run

1. Maker on with the defaults (0.13 BTC a side), flow off; watch a day of re-quotes and gas.
2. Flow on at one order a minute; the chart fills; watch inventory, skew and the reduce-only
   transitions through real volatility.
3. Widen sizes if the margin health stays above 5.

The maker later becomes the first operator of the market-making vault: the vault owner's
session is the same session, so the loop does not change, only whose account it trades.
