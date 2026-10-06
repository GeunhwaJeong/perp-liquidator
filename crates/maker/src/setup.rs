// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Startup checks: the right chain and key, a capability over the account held by the key, the
//! market and feeds the deployment names, the price service signing for them, and a position
//! object on the market (opened on request) at the leverage asked for.

use std::str::FromStr;

use anyhow::{Context, bail, ensure};
use bigdecimal::{BigDecimal, One};
use haneul_crypto::HaneulSigner;
use haneul_sdk_types::{Address, StructTag, TypeTag};
use haneul_transaction_builder::ObjectInput;
use perp_bot_common::chain::{Chain, ObjectInfo};
use perp_bot_common::deployment::{Deployment, MarketConfig};
use perp_bot_common::keys::Key;
use perp_bot_common::oracle::OracleClient;
use tracing::{info, warn};

use crate::market::{self, MarketView};
use crate::ptb::{Builder, Engine, MarketObjects};

/// Everything a trading role (the maker, or the flow simulator) needs resolved.
#[derive(Clone, Debug)]
pub struct Role {
    pub engine: Engine,
    pub account_id: u64,
    pub market: MarketObjects,
    pub view: MarketView,
}

pub struct Wanted<'a> {
    pub chain_id: Option<&'a str>,
    pub address: Option<Address>,
    pub account: Address,
    pub account_cap: Address,
    pub create_position: bool,
    pub leverage: Option<u32>,
    /// Build and simulate, but never sign or send: the position is not opened.
    pub dry_run: bool,
}

pub async fn check(
    deployment: &Deployment,
    config: &MarketConfig,
    chain: &Chain,
    key: &Key,
    wanted: &Wanted<'_>,
    what: &str,
) -> anyhow::Result<Role> {
    let chain_id = chain
        .chain_id()
        .await
        .context("The full node does not answer")?;
    match wanted.chain_id {
        Some(expected) => ensure!(
            expected == chain_id,
            "The full node is on chain {chain_id}, not {expected}"
        ),
        None => warn!(
            chain_id,
            "--chain-id is not set; nothing stops this key from being used on the wrong network"
        ),
    }
    if let Some(address) = wanted.address {
        ensure!(
            key.address == address,
            "The {what} key is for {}, not {address}",
            key.address
        );
    }
    info!(what, address = %key.address, chain_id, "Signing as");

    let types = deployment.perpetuals_original;
    let collateral = &deployment.collateral_type;

    // The capability: over the account, held by the key's address.
    let cap = chain
        .object(wanted.account_cap)
        .await
        .with_context(|| format!("The {what} account capability"))?;
    let cap_type = struct_tag(&cap)?;
    ensure!(
        is(&cap_type, None, "authority", "AuthorityCap") && cap_type.type_params().len() == 2,
        "{} is a {}, not an account capability",
        wanted.account_cap,
        cap.object_type
    );
    let context = &cap_type.type_params()[0];
    ensure!(
        matches!(context, TypeTag::Struct(s) if is(s, Some(types), "authority", "ACCOUNT")),
        "{} is a capability over {context}, not over an engine account",
        wanted.account_cap
    );
    let role = cap_type.type_params()[1].clone();
    match &role {
        TypeTag::Struct(s) if is(s, None, "authority", "ASSISTANT") => {}
        TypeTag::Struct(s) if is(s, None, "authority", "ADMIN") => warn!(
            what,
            "Running with the admin capability, which can withdraw the account's collateral; an assistant capability can trade as well and cannot"
        ),
        other => bail!(
            "{} has role {other}, neither ADMIN nor ASSISTANT",
            wanted.account_cap
        ),
    }
    ensure!(
        cap.owner
            .as_deref()
            .is_some_and(|o| same_id(o, &key.address)),
        "{} is not held by {} (owner: {:?})",
        wanted.account_cap,
        key.address,
        cap.owner
    );
    let cap_for = cap
        .json
        .get("for")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    ensure!(
        same_id(cap_for, &wanted.account),
        "{} is a capability over {cap_for}, not over {}",
        wanted.account_cap,
        wanted.account
    );

    // The account.
    let account = chain
        .object(wanted.account)
        .await
        .with_context(|| format!("The {what} account"))?;
    let account_type = struct_tag(&account)?;
    ensure!(
        is(&account_type, Some(types), "account", "Account")
            && account_type.type_params() == [collateral.clone()],
        "{} is a {}, not an Account<{collateral}>",
        wanted.account,
        account.object_type
    );
    let account_id = json_u64(&account.json, "account_id")
        .with_context(|| format!("{} has no account ID", wanted.account))?;

    let engine = Engine {
        package: deployment.perpetuals,
        types_package: types,
        collateral: collateral.clone(),
        role,
        account: wanted.account,
        cap: wanted.account_cap,
        fees: match (&deployment.fees, deployment.registry) {
            (Some(fees), Some(registry)) => Some((fees.clone(), registry)),
            (Some(_), None) => bail!("the deployment has fee objects but no registry"),
            _ => None,
        },
    };

    // The market and its feeds.
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
    let view = MarketView::from_json(&ch.json)
        .with_context(|| format!("The {} clearing house's parameters", config.ticker))?;
    let feed = |info: &ObjectInfo| {
        struct_tag(info).is_ok_and(|t| is(&t, None, "price_feed_storage", "PriceFeedStorage"))
    };
    for (id, label) in [
        (config.base_feed, "base"),
        (deployment.collateral_feed, "collateral"),
    ] {
        let object = chain
            .object(id)
            .await
            .with_context(|| format!("The {} {label} price feed", config.ticker))?;
        ensure!(feed(&object), "{id} is not a price feed storage");
    }
    let market = MarketObjects {
        clearing_house: config.clearing_house,
        base_feed: config.base_feed,
        collateral_feed: deployment.collateral_feed,
    };
    info!(
        market = config.ticker,
        account = %wanted.account,
        account_id,
        lot = view.lot_size,
        tick = view.tick_size,
        paused = view.paused,
        fees = engine.fees.is_some(),
        "Market"
    );

    // The position on the market, at the leverage asked for.
    let mut position = market::position(chain, config.clearing_house, types, account_id).await?;
    if position.is_none() {
        if !wanted.create_position {
            bail!(
                "The {what} account has no position in {}; run once with --create-position",
                config.ticker
            );
        }
        ensure!(
            !wanted.dry_run,
            "--create-position sends a transaction, which a dry run does not"
        );
        let mut b = Builder::new(&engine, &ObjectInput::new);
        b.create_position(config.clearing_house);
        send(chain, key, b.finish(), "open the position").await?;
        info!(what, market = config.ticker, "Opened the position");
        position = market::position(chain, config.clearing_house, types, account_id).await?;
    }
    let position = position.context("the position was not found after opening it")?;
    if let Some(leverage) = wanted.leverage {
        let wanted_ratio = BigDecimal::from(1) / BigDecimal::from(leverage);
        let wanted_ratio = wanted_ratio.max(view.margin_ratio_initial.clone());
        if position.initial_margin_ratio != wanted_ratio {
            ensure!(
                !wanted.dry_run,
                "--leverage sends a transaction, which a dry run does not"
            );
            let raw = ifixed_bytes(&wanted_ratio)?;
            let mut b = Builder::new(&engine, &ObjectInput::new);
            b.set_leverage(config.clearing_house, &raw);
            send(chain, key, b.finish(), "set the leverage").await?;
            info!(
                what,
                market = config.ticker,
                initial_margin_ratio = %wanted_ratio,
                "Set the position's margin ratio"
            );
        }
    } else if position.initial_margin_ratio.is_one() {
        warn!(
            what,
            "The position has no leverage (margin ratio 1.0): a ladder needs its full value in margin; --leverage sets it"
        );
    }
    Ok(Role {
        engine,
        account_id,
        market,
        view,
    })
}

/// Checks that the price service signs for the market's feeds.
pub async fn check_oracle(oracle: &OracleClient, market: &MarketObjects) -> anyhow::Result<()> {
    let updates = oracle
        .fetch()
        .await
        .context("The oracle service does not answer")?;
    let served = updates.for_feeds(&[market.base_feed, market.collateral_feed]);
    ensure!(
        served
            .iter()
            .any(|u| u.price_feed_storage == market.base_feed),
        "The oracle service has no signed price for the market's base feed"
    );
    info!(
        source = %updates.relay.source,
        served = served.len(),
        "The oracle service signs for the market's feeds"
    );
    Ok(())
}

async fn send(
    chain: &Chain,
    key: &Key,
    mut tx: haneul_transaction_builder::TransactionBuilder,
    what: &str,
) -> anyhow::Result<()> {
    tx.set_sender(key.address);
    if let Ok(price) = chain.reference_gas_price().await {
        tx.set_gas_price(price);
    }
    let tx = chain
        .build(tx)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to {what}: {e}"))?;
    let signature = key
        .private
        .sign_transaction(&tx)
        .map_err(|e| anyhow::anyhow!("signing failed: {e}"))?;
    let outcome = chain
        .execute(&tx, signature)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to {what}: {e}"))?;
    ensure!(
        outcome.success,
        "Failed to {what}: {} failed: {}",
        outcome.digest,
        outcome.error.unwrap_or_default()
    );
    Ok(())
}

/// An `ifixed` value's 32 little-endian bytes, for a `u256` argument.
pub fn ifixed_bytes(value: &BigDecimal) -> anyhow::Result<[u8; 32]> {
    use num_bigint::BigUint;
    let scaled = (value * BigDecimal::from_str("1000000000000000000").unwrap()).with_scale(0);
    let (digits, _) = scaled.into_bigint_and_exponent();
    let unsigned: BigUint = digits
        .try_into()
        .map_err(|_| anyhow::anyhow!("a negative margin ratio"))?;
    let bytes = unsigned.to_bytes_le();
    ensure!(bytes.len() <= 32, "the value does not fit a u256");
    let mut out = [0u8; 32];
    out[..bytes.len()].copy_from_slice(&bytes);
    Ok(out)
}

pub fn struct_tag(info: &ObjectInfo) -> anyhow::Result<StructTag> {
    info.object_type
        .parse()
        .map_err(|e| anyhow::anyhow!("Unreadable object type {}: {e}", info.object_type))
}

/// Whether `tag` is `module::name`, published at `address` when one is given.
pub fn is(tag: &StructTag, address: Option<Address>, module: &str, name: &str) -> bool {
    address.is_none_or(|a| *tag.address() == a)
        && tag.module().as_str() == module
        && tag.name().as_str() == name
}

fn same_id(text: &str, address: &Address) -> bool {
    Address::from_str(text).is_ok_and(|a| a == *address)
}

pub fn json_u64(json: &serde_json::Value, field: &str) -> Option<u64> {
    match json.get(field)? {
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_margin_ratio_becomes_the_engines_fixed_bytes() {
        let bytes = ifixed_bytes(&BigDecimal::from_str("0.1").unwrap()).unwrap();
        let mut expected = [0u8; 32];
        expected[..8].copy_from_slice(&100_000_000_000_000_000u64.to_le_bytes());
        assert_eq!(bytes, expected);
    }
}
