# perp-liquidator

A liquidation bot for the Haneul perpetuals engine.

It watches every position through the indexer's database, measures each one against its
maintenance requirement with the same formulas the indexer uses (`perp-engine`), and
liquidates the ones below it, largest first. Each liquidation is one transaction that:

1. relays the oracle service's latest signed prices for the market (when configured), so it
   never waits for the relayer and is priced at the newest price;
2. opens a session of the liquidator's account and liquidates, force-canceling the account's
   resting orders;
3. sells (or buys back) what was taken over with an immediate-or-cancel reduce-only order at
   most `--unwind-slippage-bps` from the mark price;
4. closes the session, allocating the margin the remainder needs and returning the rest.

Everything happens or nothing does. Whatever the book did not take is unwound again every
`--unwind-interval-ms`.

Nothing is signed that the full node has not simulated, and every engine abort is read for
what it means: wait for the next checkpoint (the position recovered, the indexer was behind),
back off, or call an operator (the account is underfunded, the package was upgraded, the bad
debt is beyond what the insurance fund and socialization may take).

A position whose bad debt only auto-deleveraging can close is closed against the most
profitable and most leveraged positions on the other side when the liquidator holds the ADL
capability (`--adl-cap`), and reported with the plan otherwise.

## Layout

A Cargo workspace for the engine's operational bots. Each runs as its own process with its own
key; what they have in common lives in one crate so that a fix or a version move happens once.

| Crate | What it is |
|---|---|
| `crates/common` (`perp-bot-common`) | The full node (reading objects, resolving and simulating transactions, executing and waiting for the checkpoint), keys, the deployment file, the indexer's database, the oracle service's signed prices and alerts. |
| `crates/liquidator` (`perp-liquidator`) | The liquidation bot described here. |
| `crates/cranker` (`perp-cranker`) | The funding cranker, below. |

The indexer and SDK pins are workspace dependencies in the root `Cargo.toml`, shared by every
crate.

## Running

```bash
cargo build --release
target/release/perp-liquidator \
    --deployment perp.mainnet.json \
    --database-url postgres://reader@db/perp_indexer \
    --rpc-url https://fullnode.example:443 \
    --chain-id <chain id> \
    --key-file /etc/perp-liquidator/key \
    --account <Account object> \
    --account-cap <AuthorityCap object over it> \
    --oracle-updates-url https://oracle.example/v1/updates \
    --alert-webhook-url https://hooks.slack.com/...
```

Start with `--check-only` to run the startup checks and exit, and with `--dry-run` to simulate
everything and send nothing. Every flag has an environment variable form; `--help` lists them.

### The deployment file

The same file the API and the front end read, with what transacting needs:

```json
{
  "packages": {"perpetuals": "0x...", "perpetualsOriginal": "0x... (after an upgrade)"},
  "registry": "0x... (for ADL)",
  "collateral": {"coinType": "0x...::ryusd::RYUSD", "decimals": 6, "priceFeedStorage": "0x..."},
  "markets": {
    "BTC-USD": {"clearingHouse": "0x...", "basePriceFeedStorage": "0x..."}
  }
}
```

### The account and its key

The liquidator trades from its own engine `Account`, which takes over liquidated positions and
pays their margin from its unallocated collateral. Fund it with enough collateral for the
largest position it may have to take over at the initial margin ratio; with unwinding, most of
that comes straight back.

Give the bot an **assistant** capability over the account (`account::new_assistant_account_cap`)
and keep the admin capability offline: an assistant can liquidate and trade but cannot withdraw.
The key file may be the Haneul CLI keystore (with `--address`), a `haneulprivkey` string or a
base64 entry; keep it `chmod 600`.

The address needs HANEUL for gas. Transactions are sent at the reference gas price on purpose:
paying more makes the engine charge its priority taker fee on the unwinding order.

The first start in a market needs `--create-positions` to open the account's position object.

### Startup checks

Before the first round the bot checks, and refuses to start on any mismatch: the full node's
chain ID against `--chain-id`; the key against `--address`; that the capability is over the
account and held by the key's address; that the account, every clearing house and every price
feed is what the deployment file says it is; that the indexer has the account and the account
has a position in every market; that the oracle service answers and which source it signs
for; and that the ADL capability is the engine's and held by the key's address.

## Watching it

`--listen-address` (default `127.0.0.1:9188`) serves:

- `/health`: 200 while rounds keep finishing, the indexer keeps up and nothing critical is open;
  503 with the reasons otherwise.
- `/status`: the mode, the markets with their prices, the least healthy positions, the last
  100 actions with their outcomes and digests, and the open problems.
- `/metrics`: Prometheus metrics, prefixed `perp_liquidator_`. `ops/prometheus-alerts.yml` has
  alert rules for them.

Alerts go to the log and, with `--alert-webhook-url`, to a Slack, Discord or plain JSON
webhook. A condition that lasts is posted once per `--alert-repeat-secs`.

## Limits

- One transaction at a time: every liquidation spends from the same account and the same gas
  coins. A burst of liquidations is worked through largest first, at a few per second.
- The bot finds positions through the indexer, so it is as current as the indexer. Prices from
  the oracle service, when configured, are ahead of the indexer and are used to find positions
  that a fresh price makes liquidatable.
- Stop orders, TWAP orders and funding updates are not this bot's job.

## Testing

```bash
cargo test
```

`scripts/localnet/check.py` runs the bot against a local network with the engine published and
the indexer following it (the stack the indexer's `scripts/localnet/run_all.sh` holds up with
`HOLD`) and checks, in eight phases: a startup refusal, opening its position, a dry run,
liquidations with unwinding and a force-canceled resting order, inventory the book could not
take (alerted on and sold once bids return), a bad-debt report and an ADL, and liquidation on a
signed price source with no relayer: the market's price goes stale after ten seconds, a stand-in
for the oracle service serves a newer signed price, and the bot finds the position on that price
and relays it in front of the liquidation. It publishes `oracle_haneul` from an engine checkout
given with `--engine-head` (read only).

The margin formulas come from the indexer repository's `perp-engine` crate and the event
layouts from its `perp-types` crate, pinned to one commit of it, so that both judge positions
the same way. Move the pin when the indexer moves.

## Funding cranker

Every session updates a market's funding and TWAPs when they are due, so a market that trades
needs no crank. A quiet one does: its premium TWAP is sampled only when something touches the
market, so funding drifts from the book while nobody trades, and the engine catches up at most
three missed funding intervals in one update. Funding a market goes longer than that without is
lost.

`perp-cranker` reads each market's clearing house from the full node every
`--poll-interval-ms` and calls `clearing_house::update_funding`, which samples the due TWAPs and
settles the due funding as a session start does, when:

- funding is due on the market's own schedule (the next multiple of its funding frequency), or
- a TWAP has gone `--twap-min-interval-ms` (60 s by default) without a sample, and never more
  often than the market allows.

When `--oracle-updates-url` is set the market's latest signed base price goes in front of the
crank, which then goes through while the relayer is behind; without it a stale price makes the
crank abort, which is reported. Paused and closed markets are left alone. Every crank is built
and simulated by the full node first and signed only then. A market whose cranks keep failing
the same way (`--skip-after` in a row) is left alone for `--skip-secs` instead of being paid for
every round. A market that has missed two funding intervals raises a warning, three a critical
alert and an unhealthy `/health`. When the deployment file changes the cranker exits with code 3,
for its supervisor to start it again on the new markets.

```bash
target/release/perp-cranker \
    --deployment perp.mainnet.json \
    --rpc-url https://fullnode.example:443 \
    --chain-id <chain id> \
    --key-file /etc/perp-cranker/key \
    --oracle-updates-url https://oracle.example/v1/updates
```

The key needs gas and no capability; give the cranker a key of its own. `/health`, `/status` and
`/metrics` are served on `--listen-address` (`127.0.0.1:9189`); `ops/prometheus-alerts.yml` has
rules for both bots.

`scripts/localnet/cranker_check.py` runs it on the same held stack as `check.py` (a fresh one:
both switch the market to a signed source) and checks, in eight phases: startup checks, TWAP
samples and funding at each minute boundary on a market nobody trades, a dry run, a paused
market, the critical alert after more than three missed intervals and the catch-up, stale prices
with nothing relaying (refused and backed off), the signed base price relayed in the crank, and
the exit on a changed deployment file.
