// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! What the liquidator is told: how it works. The deployment file it reads is shared with the
//! other bots (`perp_bot_common::deployment`).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::ensure;
use bigdecimal::BigDecimal;
use clap::Parser;
use haneul_sdk_types::Address;
use url::Url;

pub use perp_bot_common::alerts::AlertFormat;
pub use perp_bot_common::deployment::{Deployment, MarketConfig, db_id};

#[derive(Parser, Debug)]
#[clap(rename_all = "kebab-case", author, version)]
pub struct Args {
    /// The deployment description shared with the API and the front end
    /// (`perp.<network>.json`), with the engine's package, the price feeds and, for ADL, the
    /// registry.
    #[clap(env = "PERP_DEPLOYMENT", long)]
    pub deployment: PathBuf,
    /// The indexer's database. A read-only role is enough.
    #[clap(env = "PERP_DATABASE_URL", long)]
    pub database_url: Url,
    #[clap(long, default_value_t = 4)]
    pub db_pool_size: u32,
    /// A full node's gRPC endpoint.
    #[clap(env = "PERP_RPC_URL", long)]
    pub rpc_url: Url,
    /// The chain the deployment is on. Startup fails on any other: a key and a deployment must
    /// never be pointed at the wrong network by accident.
    #[clap(env = "PERP_CHAIN_ID", long)]
    pub chain_id: Option<String>,

    /// The signing key: a Haneul CLI keystore, a `haneulprivkey` string or a base64 entry.
    #[clap(env = "PERP_LIQUIDATOR_KEY_FILE", long)]
    pub key_file: PathBuf,
    /// Which key of a keystore to use, and the address the key must have.
    #[clap(env = "PERP_LIQUIDATOR_ADDRESS", long)]
    pub address: Option<Address>,
    /// The liquidator's `Account` object, which takes over liquidated positions.
    #[clap(env = "PERP_LIQUIDATOR_ACCOUNT", long)]
    pub account: Address,
    /// A capability over the account held by the key's address. An assistant capability is
    /// enough and is the one to use: it can trade but cannot withdraw.
    #[clap(env = "PERP_LIQUIDATOR_ACCOUNT_CAP", long)]
    pub account_cap: Address,

    /// Markets to work on, by ticker. All markets of the deployment when left out.
    #[clap(long, value_delimiter = ',')]
    pub markets: Vec<String>,
    /// Accounts never to liquidate, by account ID (other accounts of the same operator, say).
    #[clap(long, value_delimiter = ',')]
    pub exclude_accounts: Vec<i64>,

    /// Build and simulate, but never sign or send.
    #[clap(long)]
    pub dry_run: bool,
    /// Run the startup checks and exit.
    #[clap(long)]
    pub check_only: bool,
    /// Open the liquidator's position in a market where it has none, instead of failing.
    #[clap(long)]
    pub create_positions: bool,

    /// How often to look for newly indexed checkpoints, in milliseconds.
    #[clap(long, default_value_t = 250)]
    pub poll_interval_ms: u64,
    /// How often to reload every position instead of only the changed ones, in seconds.
    #[clap(long, default_value_t = 60)]
    pub full_reload_secs: u64,
    /// Positions this close above their maintenance requirement are tried too, in basis points
    /// of the requirement. The engine prices a session slightly differently than the indexer's
    /// copy allows, and simulation sorts out the ones that are not liquidatable.
    #[clap(long, default_value_t = 10)]
    pub attempt_buffer_bps: u32,
    /// The most liquidations sent per round, the largest first.
    #[clap(long, default_value_t = 20)]
    pub max_liquidations_per_round: usize,

    /// Sell (or buy back) a position taken over in the same transaction, and keep unwinding
    /// what is left afterwards.
    #[clap(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub unwind: bool,
    /// The furthest from the mark price an unwinding order may trade, in basis points. Keep it
    /// below the liquidation fee, which is what pays for it.
    #[clap(long, default_value_t = 50)]
    pub unwind_slippage_bps: u32,
    /// How often to try unwinding what is left of taken-over positions, in milliseconds.
    #[clap(long, default_value_t = 2_000)]
    pub unwind_interval_ms: u64,
    /// Above this USD value of inventory left in one market, alert.
    #[clap(long, default_value_t = 10_000.0)]
    pub max_inventory_usd: f64,

    /// Sign nothing whose gas budget is above this, in the smallest unit of HANEUL.
    #[clap(long, default_value_t = 2_000_000_000)]
    pub max_gas_budget: u64,
    /// Alert when the key's address holds less HANEUL than this.
    #[clap(long, default_value_t = 5.0)]
    pub min_gas_balance: f64,
    /// Alert when the account's unallocated collateral is below this, in collateral units.
    #[clap(long, default_value_t = 0.0)]
    pub min_collateral: f64,
    /// How long to wait for a transaction to be executed, in seconds.
    #[clap(long, default_value_t = 30)]
    pub execute_timeout_secs: u64,
    /// `/health` fails when the indexed chain is older than this, in seconds.
    #[clap(long, default_value_t = 30)]
    pub max_indexer_lag_secs: u64,

    /// The oracle service's updates endpoint (`.../v1/updates`). When set, the latest signed
    /// prices go in front of every liquidation, which then never waits for the relayer, and a
    /// signed price newer than the chain's is used to find liquidatable positions early.
    #[clap(env = "PERP_ORACLE_UPDATES_URL", long)]
    pub oracle_updates_url: Option<Url>,
    /// An `AuthorityCap<PACKAGE, ADL>` held by the key's address. Without it, a position only
    /// ADL can close is reported, not closed.
    #[clap(env = "PERP_ADL_CAP", long)]
    pub adl_cap: Option<Address>,

    /// Serves `/health`, `/status` and `/metrics`.
    #[clap(long, default_value = "127.0.0.1:9188")]
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

    pub fn attempt_buffer(&self) -> BigDecimal {
        BigDecimal::from(self.attempt_buffer_bps) / BigDecimal::from(10_000)
    }

    pub fn unwind_slippage(&self) -> BigDecimal {
        BigDecimal::from(self.unwind_slippage_bps) / BigDecimal::from(10_000)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.poll_interval_ms >= 50,
            "--poll-interval-ms is below 50"
        );
        ensure!(
            self.unwind_slippage_bps < 10_000,
            "--unwind-slippage-bps must be below 10,000"
        );
        ensure!(
            self.max_liquidations_per_round > 0,
            "--max-liquidations-per-round is 0"
        );
        ensure!(self.max_gas_budget > 0, "--max-gas-budget is 0");
        Ok(())
    }
}
