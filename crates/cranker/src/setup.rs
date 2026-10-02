// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Startup checks: the right chain, the right key, and markets that are what the deployment
//! says they are.

use anyhow::{Context, ensure};
use haneul_sdk_types::{Address, StructTag};
use perp_bot_common::chain::{Chain, ObjectInfo};
use perp_bot_common::deployment::{Deployment, MarketConfig};
use perp_bot_common::keys::Key;
use perp_bot_common::oracle::OracleClient;
use tracing::{info, warn};

use crate::config::Args;
use crate::cranker::Market;
use crate::ptb::Crank;
use crate::schedule::Schedule;

pub async fn check(
    args: &Args,
    deployment: &Deployment,
    selected: &[MarketConfig],
    chain: &Chain,
    key: &Key,
    oracle: Option<&OracleClient>,
) -> anyhow::Result<Vec<Market>> {
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
    info!(address = %key.address, chain_id, "Signing as");

    let types = deployment.perpetuals_original;
    let collateral = deployment.collateral_type.clone();
    let mut markets = Vec::new();
    for config in selected {
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
        let schedule = Schedule::from_clearing_house(&ch.json)
            .with_context(|| format!("The {} clearing house's schedule", config.ticker))?;
        let feed = chain
            .object(config.base_feed)
            .await
            .with_context(|| format!("The {} price feed", config.ticker))?;
        ensure!(
            struct_tag(&feed).is_ok_and(|t| is(&t, None, "price_feed_storage", "PriceFeedStorage")),
            "{} of {} is not a price feed storage",
            config.base_feed,
            config.ticker
        );
        info!(
            market = config.ticker,
            funding_frequency_ms = schedule.funding_frequency_ms,
            premium_twap_frequency_ms = schedule.premium_twap_frequency_ms,
            spread_twap_frequency_ms = schedule.spread_twap_frequency_ms,
            paused = schedule.paused,
            "Market"
        );
        markets.push(Market {
            ticker: config.ticker.clone(),
            crank: Crank {
                package: deployment.perpetuals,
                collateral: collateral.clone(),
                clearing_house: config.clearing_house,
                base_feed: config.base_feed,
            },
        });
    }

    if let Some(oracle) = oracle {
        match oracle.fetch().await {
            Ok(updates) => {
                let feeds: Vec<Address> = markets.iter().map(|m| m.crank.base_feed).collect();
                let served = updates.for_feeds(&feeds).len();
                info!(
                    source = %updates.relay.source,
                    served,
                    of = feeds.len(),
                    "The oracle service signs for the markets' feeds"
                );
                if served < feeds.len() {
                    warn!("The oracle service has no signed price for some markets' base feeds");
                }
            }
            Err(e) => warn!("The oracle service does not answer yet: {e:#}"),
        }
    }
    Ok(markets)
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
