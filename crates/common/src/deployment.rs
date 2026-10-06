// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The deployment a bot works on: the deployment description the front end and the indexer's
//! API read (`perp.<network>.json`), down to what a transaction needs to name.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use anyhow::{Context, bail, ensure};
use haneul_sdk_types::{Address, TypeTag};
use serde::Deserialize;

/// The deployment file. Unknown fields are left for the other readers of the same file.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeploymentFile {
    packages: PackagesFile,
    registry: Option<String>,
    collateral: CollateralFile,
    /// The fee-tier extension's objects, when the deployment runs it.
    fees: Option<FeesFile>,
    markets: BTreeMap<String, MarketFile>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FeesFile {
    schedule: String,
    tier_registry: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PackagesFile {
    perpetuals: String,
    perpetuals_fees: Option<String>,
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
    /// The fee-tier extension, through which sessions record volume and earn maker rebates.
    pub fees: Option<FeeObjects>,
    pub collateral_type: TypeTag,
    pub collateral_decimals: u32,
    pub collateral_feed: Address,
    pub markets: Vec<MarketConfig>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FeeObjects {
    /// The `perpetuals_fees` package.
    pub package: Address,
    pub schedule: Address,
    pub tier_registry: Address,
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
        let fees = match (&file.fees, &file.packages.perpetuals_fees) {
            (Some(fees), Some(package)) => Some(FeeObjects {
                package: address("packages.perpetualsFees", package)?,
                schedule: address("fees.schedule", &fees.schedule)?,
                tier_registry: address("fees.tierRegistry", &fees.tier_registry)?,
            }),
            (Some(_), None) => bail!("fees are configured but packages.perpetualsFees is missing"),
            _ => None,
        };
        Ok(Self {
            perpetuals,
            perpetuals_original,
            registry: file
                .registry
                .as_deref()
                .map(|r| address("registry", r))
                .transpose()?,
            fees,
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
        "packages": {"perpetuals": "0xc8", "perpetualsFees": "0xfe"},
        "registry": "0x5",
        "fees": {"schedule": "0x5c", "tierRegistry": "0x7e"},
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
        let fees = d.fees.as_ref().unwrap();
        assert_eq!(fees.package, Address::from_static("0xfe"));
        assert_eq!(fees.schedule, Address::from_static("0x5c"));
        assert_eq!(fees.tier_registry, Address::from_static("0x7e"));
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
