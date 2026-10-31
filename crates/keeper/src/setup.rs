// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Startup checks before executing tickets: the right chain, the right key, and a deployment
//! whose objects are what the deployment file says they are.

use std::collections::HashMap;

use anyhow::{Context, ensure};
use haneul_sdk_types::{Address, StructTag};
use perp_bot_common::chain::{Chain, ObjectInfo};
use perp_bot_common::deployment::{Deployment, db_id};
use perp_bot_common::keys::Key;
use perp_bot_common::oracle::OracleClient;
use tracing::{info, warn};

use crate::config::Args;
use crate::ptb::{Engine, Feeds};

#[derive(Clone, Debug)]
pub struct Market {
    pub ticker: String,
    pub clearing_house: Address,
    /// The clearing house as the indexer writes it.
    pub db_id: String,
    pub feeds: Feeds,
}

/// The oracle service whose signed prices go in front of executions.
pub struct Oracle {
    pub client: OracleClient,
    /// The source the service signs for, as the markets name their price sources.
    pub source_id: i32,
    /// The TWAP window of each base feed for that source, in milliseconds.
    pub twap_periods: HashMap<Address, u64>,
}

pub struct Setup {
    pub engine: Engine,
    pub markets: Vec<Market>,
    pub oracle: Option<Oracle>,
}

pub async fn check(
    args: &Args,
    deployment: &Deployment,
    chain: &Chain,
    key: &Key,
) -> anyhow::Result<Setup> {
    let chain_id = chain
        .chain_id()
        .await
        .context("The full node does not answer")?;
    match &args.chain_id {
        Some(expected) => ensure!(
            *expected == chain_id,
            "The full node is on chain {chain_id}, not {expected}"
        ),
        None => warn!(
            chain_id,
            "--chain-id is not set; nothing stops this key from being used on the wrong network"
        ),
    }
    if let Some(address) = args.address {
        ensure!(
            key.address == address,
            "The key is for {}, not {address}",
            key.address
        );
    }
    info!(address = %key.address, chain_id, "Executing as");

    let orders_package = deployment.perpetuals_orders.context(
        "The deployment file names no packages.perpetualsOrders, whose tickets the keeper executes",
    )?;
    let registry = deployment
        .registry
        .context("The deployment file names no registry")?;
    let types = deployment.perpetuals_original;
    let registry_object = chain.object(registry).await.context("The registry")?;
    ensure!(
        struct_tag(&registry_object).is_ok_and(|t| is(&t, Some(types), "registry", "Registry"))
            && registry_object.shared,
        "{registry} is a {}, not the engine's shared Registry",
        registry_object.object_type
    );

    let collateral = deployment.collateral_type.clone();
    check_feed(chain, deployment.collateral_feed, "the collateral").await?;
    let mut markets = Vec::new();
    for config in &deployment.markets {
        let ch = chain
            .object(config.clearing_house)
            .await
            .with_context(|| format!("The {} clearing house", config.ticker))?;
        let ch_type = struct_tag(&ch)?;
        ensure!(
            is(&ch_type, Some(types), "clearing_house", "ClearingHouse")
                && ch_type.type_params() == [collateral.clone()]
                && ch.shared,
            "{} ({}) is a {}, not a shared ClearingHouse<{collateral}>",
            config.ticker,
            config.clearing_house,
            ch.object_type
        );
        check_feed(chain, config.base_feed, &config.ticker).await?;
        info!(market = config.ticker, clearing_house = %config.clearing_house, "Market");
        markets.push(Market {
            ticker: config.ticker.clone(),
            clearing_house: config.clearing_house,
            db_id: db_id(&config.clearing_house),
            feeds: Feeds {
                base: config.base_feed,
                collateral: deployment.collateral_feed,
            },
        });
    }

    let oracle = match &args.oracle_updates_url {
        None => {
            warn!(
                "--oracle-updates-url is not set: tickets trigger on the prices the relayer last put on chain"
            );
            None
        }
        Some(url) => {
            let client = OracleClient::new(url.clone())?;
            let updates = client
                .fetch()
                .await
                .with_context(|| format!("The oracle service at {url} does not answer"))?;
            let source = chain
                .object(updates.relay.source)
                .await
                .context("The oracle source")?;
            let source_id = source
                .json
                .get("source_cap")
                .and_then(|cap| json_u64(cap, "source_id"))
                .with_context(|| format!("{} has no source ID", updates.relay.source))?;
            let mut twap_periods = HashMap::new();
            for market in &markets {
                let feed = chain
                    .object(market.feeds.base)
                    .await
                    .with_context(|| format!("The {} price feed", market.ticker))?;
                let period = feed.json["feeds"]
                    .as_array()
                    .and_then(|feeds| {
                        feeds
                            .iter()
                            .find(|f| json_u64(f, "source_id") == Some(source_id))
                    })
                    .and_then(|f| json_u64(f, "twap_period_ms"));
                match period {
                    Some(period) => {
                        twap_periods.insert(market.feeds.base, period);
                    }
                    None => warn!(
                        market = market.ticker,
                        "The base feed has no feed of the oracle service's source"
                    ),
                }
            }
            info!(source = %updates.relay.source, source_id, "Relaying the oracle service's prices");
            Some(Oracle {
                client,
                source_id: i32::try_from(source_id)?,
                twap_periods,
            })
        }
    };

    Ok(Setup {
        engine: Engine {
            package: deployment.perpetuals,
            types_package: types,
            orders_package,
            registry,
            collateral,
        },
        markets,
        oracle,
    })
}

async fn check_feed(chain: &Chain, feed: Address, of: &str) -> anyhow::Result<()> {
    let object = chain
        .object(feed)
        .await
        .with_context(|| format!("The price feed of {of}"))?;
    ensure!(
        struct_tag(&object).is_ok_and(|t| is(&t, None, "price_feed_storage", "PriceFeedStorage")),
        "{feed} of {of} is not a price feed storage"
    );
    Ok(())
}

fn struct_tag(info: &ObjectInfo) -> anyhow::Result<StructTag> {
    info.object_type
        .parse()
        .map_err(|e| anyhow::anyhow!("Unreadable object type {}: {e}", info.object_type))
}

/// Whether `tag` is `module::name`, published at `address` when one is given.
fn is(tag: &StructTag, address: Option<Address>, module: &str, name: &str) -> bool {
    address.is_none_or(|a| *tag.address() == a)
        && tag.module().as_str() == module
        && tag.name().as_str() == name
}

fn json_u64(value: &serde_json::Value, field: &str) -> Option<u64> {
    match value.get(field)? {
        serde_json::Value::String(s) => s.parse().ok(),
        serde_json::Value::Number(n) => n.as_u64(),
        _ => None,
    }
}
