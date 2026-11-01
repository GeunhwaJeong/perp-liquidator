// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! What the keeper is told.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::ensure;
use clap::Parser;
use haneul_sdk_types::Address;
use perp_bot_common::alerts::AlertFormat;
use url::Url;

#[derive(Parser, Debug)]
#[clap(rename_all = "kebab-case", author, version)]
pub struct Args {
    /// The keeper's own database, for the tickets' details. Keep it apart from the indexer's,
    /// which can be dropped and rebuilt from the chain: the details exist nowhere else.
    #[clap(env = "PERP_KEEPER_DATABASE_URL", long)]
    pub details_database_url: Url,
    /// The indexer's database, read to tell which details a ticket on chain has taken up and,
    /// when executing, which tickets are open and what their markets and positions are. A
    /// read-only role is enough.
    #[clap(env = "PERP_DATABASE_URL", long)]
    pub database_url: Url,
    /// Connections to each database.
    #[clap(long, default_value_t = 4)]
    pub pool_size: u32,

    /// The largest request body taken, in bytes. A ticket's details are a few hundred.
    #[clap(long, default_value_t = 4_096)]
    pub max_body_bytes: usize,
    /// At most this many details wait for their tickets: new details push the oldest waiting
    /// ones out, so that a flood of made-up details cannot shut the door on real ones.
    #[clap(long, default_value_t = 10_000)]
    pub max_unmatched: i64,
    /// At most this many details from one client wait for their tickets; its oldest are pushed
    /// out first. The client is the peer's address, or the last `X-Forwarded-For` address when
    /// the peer is the site's reverse proxy on this host.
    #[clap(long, default_value_t = 20)]
    pub max_unmatched_per_source: i64,
    /// How long details wait for a ticket with their commitment before they are dropped, in
    /// seconds. The front end hands them in right before the ticket's transaction is signed,
    /// so the ones still waiting after this belong to a transaction that never went through.
    /// It hands them in again once the ticket is on chain, which keeps them whatever was pushed
    /// out or dropped in between.
    #[clap(long, default_value_t = 600)]
    pub unmatched_ttl_secs: u64,
    /// How often to look up the tickets of waiting details, in seconds.
    #[clap(long, default_value_t = 60)]
    pub match_interval_secs: u64,

    /// The deployment description shared with the API and the front end
    /// (`perp.<network>.json`). With `--rpc-url` and `--key-file` the keeper also executes the
    /// stop tickets that name it; without the three it only takes details in.
    #[clap(env = "PERP_DEPLOYMENT", long)]
    pub deployment: Option<PathBuf>,
    /// A full node's gRPC endpoint.
    #[clap(env = "PERP_RPC_URL", long)]
    pub rpc_url: Option<Url>,
    /// The chain the deployment is on. Startup fails on any other.
    #[clap(env = "PERP_CHAIN_ID", long)]
    pub chain_id: Option<String>,
    /// The signing key: a Haneul CLI keystore, a `haneulprivkey` string or a base64 entry. The
    /// keeper needs no capability, only gas: give it a key of its own, whose address the
    /// tickets name as their executor.
    #[clap(env = "PERP_KEEPER_KEY_FILE", long)]
    pub key_file: Option<PathBuf>,
    /// Which key of a keystore to use, and the address the key must have.
    #[clap(env = "PERP_KEEPER_ADDRESS", long)]
    pub address: Option<Address>,
    /// The oracle service's updates endpoint (`.../v1/updates`). When set, the market's latest
    /// signed prices go in front of every execution, which then triggers on the newest price
    /// and goes through while the relayer is behind.
    #[clap(env = "PERP_ORACLE_UPDATES_URL", long)]
    pub oracle_updates_url: Option<Url>,

    /// Build and simulate, but never sign or send.
    #[clap(long)]
    pub dry_run: bool,
    /// Run the startup checks and exit.
    #[clap(long)]
    pub check_only: bool,
    /// How often to look at the open tickets, in milliseconds.
    #[clap(long, default_value_t = 1_000)]
    pub poll_interval_ms: u64,
    /// How far a market order's average fill may stray from the index price, in basis points.
    /// The engine's market orders take any price, so a book its makers have left can fill a
    /// stop at a price nobody meant; such an execution is held back and alerted on. Limit
    /// orders carry their own price and are not checked.
    #[clap(long, default_value_t = 1_000)]
    pub max_slippage_bps: u64,
    /// How long a ticket that failed to execute waits before it is tried again, in seconds. The
    /// wait doubles with every failure in a row, up to `--max-retry-secs`, and is forgotten once
    /// the ticket's trigger price is no longer crossed.
    #[clap(long, default_value_t = 1)]
    pub retry_secs: u64,
    #[clap(long, default_value_t = 30)]
    pub max_retry_secs: u64,
    /// Failures in a row of one ticket after which an alert is raised.
    #[clap(long, default_value_t = 5)]
    pub alert_after: u32,
    /// How long a stop loss / take profit, a reduce-only stop or a reduce-only TWAP may go
    /// without a position it reduces before the keeper cancels it, in seconds; 0 never cancels.
    /// Left in place, such a ticket would act on the next position the account opens on that
    /// side, which it was never meant for. The wait covers a position the indexer has not seen
    /// yet.
    #[clap(long, default_value_t = 120)]
    pub cancel_orphans_after_secs: u64,

    /// Sign nothing whose gas budget is above this, in the smallest unit of HANEUL.
    #[clap(long, default_value_t = 500_000_000)]
    pub max_gas_budget: u64,
    /// Alert when the key's address holds less HANEUL than this.
    #[clap(long, default_value_t = 1.0)]
    pub min_gas_balance: f64,
    /// How long to wait for a transaction to be executed, in seconds.
    #[clap(long, default_value_t = 30)]
    pub execute_timeout_secs: u64,

    /// Serves `POST /v1/details`, `/health`, `/status` and `/metrics`.
    #[clap(long, default_value = "127.0.0.1:9191")]
    pub listen_address: SocketAddr,
    /// Where alerts are posted.
    #[clap(env = "PERP_ALERT_WEBHOOK_URL", long)]
    pub alert_webhook_url: Option<Url>,
    #[clap(long, value_enum, default_value_t = AlertFormat::Slack)]
    pub alert_format: AlertFormat,
    /// The same alert is posted at most once in this many seconds.
    #[clap(long, default_value_t = 600)]
    pub alert_repeat_secs: u64,
    /// Log as JSON lines.
    #[clap(long)]
    pub log_json: bool,
}

impl Args {
    pub fn match_interval(&self) -> Duration {
        Duration::from_secs(self.match_interval_secs)
    }

    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_ms)
    }

    /// Whether the keeper executes tickets, rather than only taking their details in.
    pub fn executes(&self) -> bool {
        self.deployment.is_some()
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(self.pool_size > 0, "--pool-size is 0");
        ensure!(self.max_body_bytes > 0, "--max-body-bytes is 0");
        ensure!(self.max_unmatched > 0, "--max-unmatched is not positive");
        ensure!(
            self.max_unmatched_per_source > 0,
            "--max-unmatched-per-source is not positive"
        );
        ensure!(self.unmatched_ttl_secs > 0, "--unmatched-ttl-secs is 0");
        ensure!(self.match_interval_secs > 0, "--match-interval-secs is 0");
        let given = [
            self.deployment.is_some(),
            self.rpc_url.is_some(),
            self.key_file.is_some(),
        ];
        ensure!(
            given.iter().all(|g| *g) || given.iter().all(|g| !*g),
            "Executing tickets takes --deployment, --rpc-url and --key-file together"
        );
        ensure!(
            self.executes() || !(self.dry_run || self.check_only),
            "--dry-run and --check-only are about executing tickets, which needs --deployment, --rpc-url and --key-file"
        );
        ensure!(self.poll_interval_ms > 0, "--poll-interval-ms is 0");
        ensure!(self.max_slippage_bps > 0, "--max-slippage-bps is 0");
        ensure!(self.retry_secs > 0, "--retry-secs is 0");
        ensure!(
            self.max_retry_secs >= self.retry_secs,
            "--max-retry-secs is below --retry-secs"
        );
        ensure!(self.alert_after > 0, "--alert-after is 0");
        ensure!(self.max_gas_budget > 0, "--max-gas-budget is 0");
        Ok(())
    }
}
