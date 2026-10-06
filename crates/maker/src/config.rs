// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! What the maker is told. The quoting parameters are the design's table, with the shadow
//! run's defaults.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::ensure;
use bigdecimal::{BigDecimal, Signed};
use clap::Parser;
use haneul_sdk_types::Address;
use perp_bot_common::alerts::AlertFormat;
use url::Url;

#[derive(Parser, Debug)]
#[clap(rename_all = "kebab-case", author, version)]
pub struct Args {
    /// The deployment description shared with the API and the front end (`perp.<network>.json`).
    #[clap(env = "PERP_DEPLOYMENT", long)]
    pub deployment: PathBuf,
    /// A full node's gRPC endpoint.
    #[clap(env = "PERP_RPC_URL", long)]
    pub rpc_url: Url,
    /// The chain the deployment is on. Startup fails on any other.
    #[clap(env = "PERP_CHAIN_ID", long)]
    pub chain_id: Option<String>,
    /// The maker's signing key: a Haneul CLI keystore, a `haneulprivkey` string or a base64
    /// entry.
    #[clap(env = "PERP_MAKER_KEY_FILE", long)]
    pub key_file: PathBuf,
    /// Which key of a keystore to use, and the address the key must have.
    #[clap(env = "PERP_MAKER_ADDRESS", long)]
    pub address: Option<Address>,
    /// The maker's engine account (an `Account` object).
    #[clap(env = "PERP_MAKER_ACCOUNT", long)]
    pub account: Address,
    /// The capability over the account held by the key: an assistant cap is enough and cannot
    /// withdraw.
    #[clap(env = "PERP_MAKER_ACCOUNT_CAP", long)]
    pub account_cap: Address,
    /// The market to quote, by ticker.
    #[clap(long)]
    pub market: String,
    /// The oracle service's updates endpoint (`.../v1/updates`): the reference price, and what
    /// every round relays in front of itself.
    #[clap(env = "PERP_ORACLE_UPDATES_URL", long)]
    pub oracle_updates_url: Url,
    /// The indexer's database. When given, the account's orders resting at startup (posted by
    /// an earlier run, or by hand) are adopted and cancelled by the first round; without it,
    /// only orders this run posts are known, and earlier ones rest until they expire.
    #[clap(env = "PERP_DATABASE_URL", long)]
    pub database_url: Option<Url>,
    /// The indexer's API (`https://api…`), the same adoption over HTTP when the database is not
    /// reachable: `/v4/orders/parentSubaccountNumber` of the key's address.
    #[clap(env = "PERP_INDEXER_URL", long)]
    pub indexer_url: Option<Url>,
    /// The account's number among the key's accounts, for the indexer's API.
    #[clap(long, default_value_t = 0)]
    pub subaccount_number: u32,
    /// Build and simulate every round, but never sign or send.
    #[clap(long)]
    pub dry_run: bool,
    /// Run the startup checks and exit.
    #[clap(long)]
    pub check_only: bool,
    /// Open the account's position on the market if it has none (one transaction).
    #[clap(long)]
    pub create_position: bool,
    /// Set the account's leverage on the market to this before quoting (one transaction);
    /// a new position starts with no leverage, which makes the ladder need its full value.
    #[clap(long)]
    pub leverage: Option<u32>,

    // ---- quoting
    /// Levels per side.
    #[clap(long, default_value_t = 5)]
    pub levels: u32,
    /// The half-spread a round trip should earn, in basis points.
    #[clap(long, default_value = "8")]
    pub half_spread_bps: BigDecimal,
    #[clap(long, default_value = "5")]
    pub min_half_spread_bps: BigDecimal,
    /// How many rounds of typical motion the spread covers.
    #[clap(long, default_value = "2")]
    pub vol_multiplier: BigDecimal,
    /// How much of the signed price's confidence interval widens the spread.
    #[clap(long, default_value = "0.5")]
    pub conf_multiplier: BigDecimal,
    /// Spacing between levels, in basis points.
    #[clap(long, default_value = "6")]
    pub level_step_bps: BigDecimal,
    /// The innermost level's size, in base units.
    #[clap(long, default_value = "0.01")]
    pub size: BigDecimal,
    /// How much bigger each further level is.
    #[clap(long, default_value = "0.5")]
    pub size_growth: BigDecimal,
    /// Inventory beyond which only the reducing side is quoted, in base units.
    #[clap(long, default_value = "0.25")]
    pub max_position: BigDecimal,
    /// Inventory beyond which the excess is sold through the book.
    #[clap(long, default_value = "0.5")]
    pub hard_position: BigDecimal,
    /// The skew at the inventory limit, in basis points; twice the half-spread when left out.
    #[clap(long)]
    pub skew_bps: Option<BigDecimal>,
    /// How far through the book a flattening order may go, in basis points.
    #[clap(long, default_value = "20")]
    pub flatten_slippage_bps: BigDecimal,
    /// Margin over maintenance requirement under which the growing side is not quoted.
    #[clap(long, default_value = "3")]
    pub min_health: BigDecimal,
    /// A move of the reference that triggers a round, in basis points.
    #[clap(long, default_value = "3")]
    pub requote_bps: BigDecimal,
    /// A round is sent at least this often while quoting, in seconds, to keep the expiry alive.
    #[clap(long, default_value_t = 20)]
    pub refresh_secs: u64,
    /// Every quote expires this long after it is posted, in seconds.
    #[clap(long, default_value_t = 60)]
    pub expire_secs: u64,
    /// At most one round per this many seconds.
    #[clap(long, default_value_t = 3)]
    pub min_interval_secs: u64,
    /// The weight of the latest move in the volatility estimate.
    #[clap(long, default_value = "0.1")]
    pub vol_alpha: BigDecimal,
    /// How often the reference is looked at, in milliseconds.
    #[clap(long, default_value_t = 1_000)]
    pub poll_interval_ms: u64,

    // ---- kill switches
    /// Quote nothing on a signed price older than this, in seconds.
    #[clap(long, default_value_t = 8)]
    pub max_price_age_secs: u64,
    /// Quote nothing on a confidence interval wider than this, in basis points.
    #[clap(long, default_value = "50")]
    pub max_confidence_bps: BigDecimal,
    /// Failures in a row after which quoting stops for `--retry-secs`.
    #[clap(long, default_value_t = 3)]
    pub max_failures: u32,
    #[clap(long, default_value_t = 60)]
    pub retry_secs: u64,
    /// Sign nothing whose gas budget is above this, in the smallest unit of HANEUL.
    #[clap(long, default_value_t = 500_000_000)]
    pub max_gas_budget: u64,
    /// Alert when the key's address holds less HANEUL than this.
    #[clap(long, default_value_t = 5.0)]
    pub min_gas_balance: f64,
    /// How often the market's parameters are read again, in seconds.
    #[clap(long, default_value_t = 300)]
    pub params_refresh_secs: u64,

    // ---- the flow simulator
    /// Run the shadow run's flow simulator: a taker, on its own key and account, that trades
    /// against the maker so that fills and candles exist. Refused on real collateral.
    #[clap(long)]
    pub flow: bool,
    #[clap(env = "PERP_FLOW_KEY_FILE", long)]
    pub flow_key_file: Option<PathBuf>,
    #[clap(env = "PERP_FLOW_ADDRESS", long)]
    pub flow_address: Option<Address>,
    #[clap(env = "PERP_FLOW_ACCOUNT", long)]
    pub flow_account: Option<Address>,
    #[clap(env = "PERP_FLOW_ACCOUNT_CAP", long)]
    pub flow_account_cap: Option<Address>,
    /// Mean seconds between the simulator's trades.
    #[clap(long, default_value_t = 45)]
    pub flow_mean_secs: u64,
    /// The simulator's typical size, in base units (lognormal around it).
    #[clap(long, default_value = "0.01")]
    pub flow_size: BigDecimal,
    /// How far through the book the simulator's orders may go, in basis points.
    #[clap(long, default_value = "15")]
    pub flow_slippage_bps: BigDecimal,
    /// Beyond this inventory the simulator only reduces.
    #[clap(long, default_value = "0.2")]
    pub flow_max_position: BigDecimal,

    // ---- operations
    #[clap(long, default_value_t = 30)]
    pub execute_timeout_secs: u64,
    /// Serves `/health`, `/status` and `/metrics`.
    #[clap(long, default_value = "127.0.0.1:9190")]
    pub listen_address: SocketAddr,
    #[clap(env = "PERP_ALERT_WEBHOOK_URL", long)]
    pub alert_webhook_url: Option<Url>,
    #[clap(long, value_enum, default_value_t = AlertFormat::Slack)]
    pub alert_format: AlertFormat,
    #[clap(long, default_value_t = 600)]
    pub alert_repeat_secs: u64,
    /// Log as JSON lines.
    #[clap(long)]
    pub log_json: bool,
}

impl Args {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.levels >= 1 && self.levels <= 20,
            "--levels must be 1..=20"
        );
        ensure!(self.size.is_positive(), "--size must be positive");
        ensure!(
            self.max_position.is_positive() && self.hard_position >= self.max_position,
            "--hard-position must be at least --max-position, both positive"
        );
        ensure!(
            self.expire_secs > self.refresh_secs,
            "--expire-secs must be longer than --refresh-secs, or quotes lapse between rounds"
        );
        ensure!(
            self.min_interval_secs >= 1,
            "--min-interval-secs must be at least 1"
        );
        ensure!(
            self.leverage.is_none_or(|l| (1..=100).contains(&l)),
            "--leverage must be 1..=100"
        );
        if self.flow {
            ensure!(
                self.flow_key_file.is_some()
                    && self.flow_account.is_some()
                    && self.flow_account_cap.is_some(),
                "--flow needs --flow-key-file, --flow-account and --flow-account-cap"
            );
            ensure!(
                self.flow_mean_secs >= 5,
                "--flow-mean-secs must be at least 5"
            );
        }
        Ok(())
    }

    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_ms.max(200))
    }

    pub fn skew_bps(&self) -> BigDecimal {
        self.skew_bps
            .clone()
            .unwrap_or_else(|| &self.half_spread_bps * BigDecimal::from(2))
    }
}
