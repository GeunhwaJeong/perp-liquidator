// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! What the liquidator is told: the deployment it works on, and how it works.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, bail, ensure};
use bigdecimal::BigDecimal;
use clap::{Parser, ValueEnum};
use haneul_sdk_types::{Address, TypeTag};
use serde::Deserialize;
use url::Url;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum AlertFormat {
    /// `{"text": ...}`, which Slack and most chat webhooks take.
    Slack,
    /// `{"content": ...}`.
    Discord,
    /// `{"level", "key", "message", "service"}`.
    Json,
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

/// The deployment file. Unknown fields are left for the other readers of the same file.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeploymentFile {
    packages: PackagesFile,
    registry: Option<String>,
    collateral: CollateralFile,
    markets: BTreeMap<String, MarketFile>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PackagesFile {
    perpetuals: String,
    /// Where the engine's types were first published. The same as `perpetuals` until the
    /// package is upgraded; after that, calls go to the new version while objects and events
    /// keep the original's types.
    perpetuals_original: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CollateralFile {
    coin_type: String,
    decimals: u32,
    price_feed_storage: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MarketFile {
    clearing_house: String,
    base_price_feed_storage: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Deployment {
    pub perpetuals: Address,
    pub perpetuals_original: Address,
    pub registry: Option<Address>,
    pub collateral_type: TypeTag,
    pub collateral_decimals: u32,
    pub collateral_feed: Address,
    pub markets: Vec<MarketConfig>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MarketConfig {
    pub ticker: String,
    pub clearing_house: Address,
    pub base_feed: Address,
}

impl Deployment {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let json = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read {}", path.display()))?;
        Self::parse(&json).with_context(|| format!("Invalid deployment file {}", path.display()))
    }

    pub fn parse(json: &str) -> anyhow::Result<Self> {
        let file: DeploymentFile = serde_json::from_str(json)?;
        let address = |what: &str, value: &str| -> anyhow::Result<Address> {
            value
                .parse()
                .map_err(|e| anyhow::anyhow!("{what} is not an address ({value}): {e}"))
        };
        let perpetuals = address("packages.perpetuals", &file.packages.perpetuals)?;
        let perpetuals_original = match &file.packages.perpetuals_original {
            Some(original) => address("packages.perpetualsOriginal", original)?,
            None => perpetuals,
        };
        let collateral_type: TypeTag = file
            .collateral
            .coin_type
            .parse()
            .map_err(|e| anyhow::anyhow!("collateral.coinType: {e}"))?;
        let mut markets = Vec::new();
        let mut seen = BTreeSet::new();
        for (ticker, market) in file.markets {
            let clearing_house =
                address(&format!("{ticker} clearingHouse"), &market.clearing_house)?;
            if !seen.insert(clearing_house) {
                bail!("{ticker} names a clearing house another market already does");
            }
            markets.push(MarketConfig {
                base_feed: address(
                    &format!("{ticker} basePriceFeedStorage"),
                    &market.base_price_feed_storage,
                )?,
                ticker,
                clearing_house,
            });
        }
        ensure!(!markets.is_empty(), "the deployment lists no markets");
        Ok(Self {
            perpetuals,
            perpetuals_original,
            registry: file
                .registry
                .as_deref()
                .map(|r| address("registry", r))
                .transpose()?,
            collateral_type,
            collateral_decimals: file.collateral.decimals,
            collateral_feed: address(
                "collateral.priceFeedStorage",
                &file.collateral.price_feed_storage,
            )?,
            markets,
        })
    }

    /// The markets to work on: the ones named, or all.
    pub fn select(&self, tickers: &[String]) -> anyhow::Result<Vec<MarketConfig>> {
        if tickers.is_empty() {
            return Ok(self.markets.clone());
        }
        tickers
            .iter()
            .map(|ticker| {
                self.markets
                    .iter()
                    .find(|m| &m.ticker == ticker)
                    .cloned()
                    .with_context(|| format!("The deployment has no market {ticker}"))
            })
            .collect()
    }
}

/// An address the way the indexer's tables write object IDs: `0x` and 64 lowercase digits.
pub fn db_id(address: &Address) -> String {
    format!("0x{}", hex::encode(address.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const FILE: &str = r#"{
        "network": "localnet",
        "packages": {"perpetuals": "0xc8"},
        "registry": "0x5",
        "collateral": {"coinType": "0x7f::tusd::TUSD", "decimals": 6, "priceFeedStorage": "0x1d"},
        "markets": {
            "BTC-USD": {"marketId": "BTC-USD", "clearingHouse": "0xc0b6", "basePriceFeedStorage": "0xfa"},
            "ETH-USD": {"clearingHouse": "0xe7", "basePriceFeedStorage": "0xfb"}
        }
    }"#;

    #[test]
    fn reads_the_shared_deployment_file() {
        let d = Deployment::parse(FILE).unwrap();
        assert_eq!(d.perpetuals, Address::from_static("0xc8"));
        assert_eq!(d.perpetuals_original, d.perpetuals);
        assert_eq!(d.registry, Some(Address::from_static("0x5")));
        assert_eq!(d.collateral_decimals, 6);
        assert_eq!(d.markets.len(), 2);
        let btc = &d.select(&["BTC-USD".into()]).unwrap()[0];
        assert_eq!(btc.clearing_house, Address::from_static("0xc0b6"));
        assert_eq!(btc.base_feed, Address::from_static("0xfa"));
        assert!(d.select(&["SOL-USD".into()]).is_err());
        assert_eq!(db_id(&btc.clearing_house), format!("0x{:0>64}", "c0b6"),);
    }

    #[test]
    fn refuses_what_it_cannot_transact_with() {
        let no_feed = FILE.replace(r#", "basePriceFeedStorage": "0xfb""#, "");
        assert!(Deployment::parse(&no_feed).is_err());
        let twice = FILE.replace("0xe7", "0xc0b6");
        assert!(Deployment::parse(&twice).is_err());
    }
}
