// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! What the cranker is told.

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
    /// The deployment description shared with the API and the front end
    /// (`perp.<network>.json`). When the file changes the cranker exits, so that its supervisor
    /// starts it again on the new markets.
    #[clap(env = "PERP_DEPLOYMENT", long)]
    pub deployment: PathBuf,
    /// A full node's gRPC endpoint.
    #[clap(env = "PERP_RPC_URL", long)]
    pub rpc_url: Url,
    /// The chain the deployment is on. Startup fails on any other.
    #[clap(env = "PERP_CHAIN_ID", long)]
    pub chain_id: Option<String>,

    /// The signing key: a Haneul CLI keystore, a `haneulprivkey` string or a base64 entry. The
    /// cranker needs no capability, only gas: give it a key of its own.
    #[clap(env = "PERP_CRANKER_KEY_FILE", long)]
    pub key_file: PathBuf,
    /// Which key of a keystore to use, and the address the key must have.
    #[clap(env = "PERP_CRANKER_ADDRESS", long)]
    pub address: Option<Address>,

    /// Markets to crank, by ticker. All markets of the deployment when left out.
    #[clap(long, value_delimiter = ',')]
    pub markets: Vec<String>,

    /// Build and simulate, but never sign or send.
    #[clap(long)]
    pub dry_run: bool,
    /// Run the startup checks and exit.
    #[clap(long)]
    pub check_only: bool,

    /// How often to look at every market, in milliseconds.
    #[clap(long, default_value_t = 1_000)]
    pub poll_interval_ms: u64,
    /// A quiet market's TWAPs are sampled at least this often, in milliseconds, and never more
    /// often than the market's own sampling interval. Funding is cranked on the market's own
    /// schedule regardless.
    #[clap(long, default_value_t = 60_000)]
    pub twap_min_interval_ms: u64,
    /// Wait this long past the moment something falls due before cranking it, in milliseconds,
    /// so that a wall clock slightly ahead of the chain's does not pay for a crank that finds
    /// nothing due yet.
    #[clap(long, default_value_t = 1_000)]
    pub due_margin_ms: u64,

    /// The oracle service's updates endpoint (`.../v1/updates`). When set, the market's latest
    /// signed base price goes in front of every crank, which then goes through while the
    /// relayer is behind.
    #[clap(env = "PERP_ORACLE_UPDATES_URL", long)]
    pub oracle_updates_url: Option<Url>,

    /// Failures in a row after which a market is left alone for `--skip-secs`.
    #[clap(long, default_value_t = 2)]
    pub skip_after: u32,
    /// How long a failing market is left alone, in seconds.
    #[clap(long, default_value_t = 60)]
    pub skip_secs: u64,
    /// How often to check the deployment file for changes, in seconds.
    #[clap(long, default_value_t = 120)]
    pub config_check_secs: u64,

    /// Sign nothing whose gas budget is above this, in the smallest unit of HANEUL.
    #[clap(long, default_value_t = 500_000_000)]
    pub max_gas_budget: u64,
    /// Alert when the key's address holds less HANEUL than this.
    #[clap(long, default_value_t = 1.0)]
    pub min_gas_balance: f64,
    /// How long to wait for a transaction to be executed, in seconds.
    #[clap(long, default_value_t = 30)]
    pub execute_timeout_secs: u64,

    /// Serves `/health`, `/status` and `/metrics`.
    #[clap(long, default_value = "127.0.0.1:9189")]
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
    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_ms)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(self.poll_interval_ms > 0, "--poll-interval-ms is 0");
        ensure!(self.max_gas_budget > 0, "--max-gas-budget is 0");
        ensure!(self.skip_after > 0, "--skip-after is 0");
        ensure!(self.config_check_secs > 0, "--config-check-secs is 0");
        Ok(())
    }
}
