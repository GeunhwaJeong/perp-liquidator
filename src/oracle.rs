// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Signed prices from the oracle service.
//!
//! The service signs a price for every feed each round and serves the latest at
//! `/v1/updates`. Anyone may relay one with `price_feed_storage::update_price_feed`, and an
//! update that is not newer than the stored price is skipped on chain without failing the
//! transaction. So every liquidation carries the latest updates of its market's feeds: it
//! never fails for want of a fresh price, and it is priced at the newest one.

use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

use anyhow::Context;
use bigdecimal::BigDecimal;
use haneul_sdk_types::Address;
use num_bigint::{BigInt, BigUint};
use serde::Deserialize;
use url::Url;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedUpdate {
    pub price_feed_storage: Address,
    pub storage_id: u32,
    /// 18 decimals.
    pub price: u128,
    pub confidence: u128,
    pub timestamp_ms: u64,
    pub public_key: Vec<u8>,
    pub signature: Vec<u8>,
}

impl SignedUpdate {
    pub fn price(&self) -> BigDecimal {
        BigDecimal::new(BigInt::from(self.price), 18)
    }
}

/// What a relay of the updates calls into.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Relay {
    /// The `oracle_haneul` package.
    pub package: Address,
    /// Its `Source` object.
    pub source: Address,
    /// The oracle's `Config` object.
    pub config: Address,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Updates {
    pub relay: Relay,
    by_feed: HashMap<Address, SignedUpdate>,
}

impl Updates {
    /// The updates for these feeds that the service has.
    pub fn for_feeds(&self, feeds: &[Address]) -> Vec<SignedUpdate> {
        let mut out: Vec<SignedUpdate> = Vec::new();
        for feed in feeds {
            if let Some(update) = self.by_feed.get(feed)
                && !out.iter().any(|u| u.price_feed_storage == *feed)
            {
                out.push(update.clone());
            }
        }
        out
    }

    pub fn get(&self, feed: &Address) -> Option<&SignedUpdate> {
        self.by_feed.get(feed)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Response {
    package_id: String,
    source_id: String,
    aggregator_config_id: String,
    updates: Vec<UpdateJson>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpdateJson {
    price_feed_storage_id: String,
    storage_id: u32,
    price: String,
    confidence: String,
    timestamp_ms: String,
    public_key: String,
    signature: String,
}

pub fn parse(body: &str) -> anyhow::Result<Updates> {
    let response: Response = serde_json::from_str(body).context("Unexpected response shape")?;
    let address = |what: &str, value: &str| -> anyhow::Result<Address> {
        Address::from_str(value).map_err(|e| anyhow::anyhow!("{what} {value}: {e}"))
    };
    let relay = Relay {
        package: address("packageId", &response.package_id)?,
        source: address("sourceId", &response.source_id)?,
        config: address("aggregatorConfigId", &response.aggregator_config_id)?,
    };
    let mut by_feed = HashMap::new();
    for u in response.updates {
        let update = SignedUpdate {
            price_feed_storage: address("priceFeedStorageId", &u.price_feed_storage_id)?,
            storage_id: u.storage_id,
            price: u.price.parse().context("price")?,
            confidence: u.confidence.parse().context("confidence")?,
            timestamp_ms: u.timestamp_ms.parse().context("timestampMs")?,
            public_key: hex::decode(&u.public_key).context("publicKey")?,
            signature: hex::decode(&u.signature).context("signature")?,
        };
        by_feed.insert(update.price_feed_storage, update);
    }
    Ok(Updates { relay, by_feed })
}

/// What a feed's TWAP becomes when `price_now` lands at `now_ms` on a TWAP last `last_twap` at
/// `last_ms`: `oracle_aggregator::price_feed::update_twap`, in the same integer arithmetic.
pub fn update_twap(
    price_now: u128,
    last_twap: u128,
    now_ms: u64,
    last_ms: u64,
    period_ms: u64,
) -> u128 {
    let price = BigUint::from(price_now);
    let twap = BigUint::from(last_twap);
    let twap = if now_ms == last_ms {
        if period_ms <= 1 {
            (price + twap) / 2u32
        } else {
            (price + twap * (period_ms - 1)) / period_ms
        }
    } else {
        let elapsed = now_ms.saturating_sub(last_ms);
        if period_ms <= elapsed {
            (price * elapsed + twap) / (elapsed + 1)
        } else {
            (price * elapsed + twap * (period_ms - elapsed)) / period_ms
        }
    };
    twap.try_into().unwrap_or(u128::MAX)
}

/// An 18-decimal price as the raw integer the oracle keeps.
pub fn raw(price: &BigDecimal) -> Option<u128> {
    let (digits, _) = (price * BigDecimal::new(BigInt::from(1), -18))
        .with_scale(0)
        .into_bigint_and_exponent();
    digits.try_into().ok()
}

#[derive(Clone)]
pub struct OracleClient {
    http: reqwest::Client,
    url: Url,
}

impl OracleClient {
    pub fn new(url: Url) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .context("Failed to build the HTTP client")?;
        Ok(Self { http, url })
    }

    pub async fn fetch(&self) -> anyhow::Result<Updates> {
        let response = self
            .http
            .get(self.url.clone())
            .send()
            .await?
            .error_for_status()?;
        parse(&response.text().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BODY: &str = r#"{
        "packageId": "0xa1", "sourceId": "0xa2", "aggregatorConfigId": "0xa3",
        "updates": [
            {"symbol": "BTC/USD", "storageId": 4, "priceFeedStorageId": "0xb1",
             "price": "94123450000000000000000", "confidence": "10000000000000000000",
             "timestampMs": "1790000000123", "publicKey": "0a0b", "signature": "0c0d"},
            {"symbol": "TUSD/USD", "storageId": 5, "priceFeedStorageId": "0xb2",
             "price": "1000000000000000000", "confidence": "0",
             "timestampMs": "1790000000124", "publicKey": "0a0b", "signature": "0e0f"}
        ]
    }"#;

    #[test]
    fn reads_the_services_updates() {
        let updates = parse(BODY).unwrap();
        assert_eq!(updates.relay.package, Address::from_static("0xa1"));
        let btc = updates.get(&Address::from_static("0xb1")).unwrap();
        assert_eq!(btc.storage_id, 4);
        assert_eq!(btc.price(), BigDecimal::from_str("94123.45").unwrap());
        assert_eq!(btc.timestamp_ms, 1_790_000_000_123);
        assert_eq!(btc.signature, vec![0x0c, 0x0d]);
    }

    #[test]
    fn picks_the_feeds_asked_for_once_each() {
        let updates = parse(BODY).unwrap();
        let (btc, tusd, other) = (
            Address::from_static("0xb1"),
            Address::from_static("0xb2"),
            Address::from_static("0xb3"),
        );
        let picked = updates.for_feeds(&[btc, other, tusd, btc]);
        let feeds: Vec<Address> = picked.iter().map(|u| u.price_feed_storage).collect();
        assert_eq!(feeds, vec![btc, tusd]);
    }

    #[test]
    fn twap_follows_the_aggregators_formula() {
        // Within the window: the new price weighs the elapsed time, the old TWAP the rest.
        assert_eq!(update_twap(100, 200, 1_250, 1_000, 1_000), 175);
        // A whole window or more elapsed: the old TWAP keeps a weight of one millisecond.
        assert_eq!(
            update_twap(100, 200, 3_000, 1_000, 1_000),
            (100 * 2_000 + 200) / 2_001
        );
        // Feeds that follow the price at once.
        assert_eq!(update_twap(42, 45, 10, 5, 1), (42 * 5 + 45) / 6);
        // An update in the same millisecond counts as one.
        assert_eq!(update_twap(100, 200, 1_000, 1_000, 1), 150);
        assert_eq!(update_twap(100, 200, 1_000, 1_000, 4), (100 + 200 * 3) / 4);
    }

    #[test]
    fn decimals_go_back_to_raw_prices() {
        assert_eq!(
            raw(&BigDecimal::from_str("45000").unwrap()),
            Some(45_000 * 10u128.pow(18))
        );
        assert_eq!(
            raw(&BigDecimal::from_str("0.5").unwrap()),
            Some(5 * 10u128.pow(17))
        );
    }

    #[test]
    fn refuses_malformed_numbers() {
        assert!(parse(&BODY.replace("\"1790000000123\"", "\"soon\"")).is_err());
        assert!(parse(&BODY.replace("0c0d", "zz")).is_err());
    }
}
