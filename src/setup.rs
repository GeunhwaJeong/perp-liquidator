// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Startup checks. Everything the liquidator will rely on is looked at once before the first
//! round, so that a wrong ID, key or network fails loudly at once instead of as a stream of
//! aborts later.

use anyhow::{Context, bail, ensure};
use haneul_crypto::HaneulSigner;
use haneul_sdk_types::{Address, StructTag, TypeTag};
use tracing::{info, warn};

use crate::chain::{Chain, ObjectInfo};
use crate::config::{Args, Deployment, MarketConfig, db_id};
use crate::keys::Key;
use crate::liquidator::{AdlSetup, Market, OracleSetup};
use crate::oracle::OracleClient;
use crate::ptb::{Builder, Engine, MarketObjects};
use crate::store::Store;

pub struct Checked {
    pub engine: Engine,
    pub account_id: i64,
    pub markets: Vec<Market>,
    pub oracle: Option<OracleSetup>,
    pub adl: Option<AdlSetup>,
}

pub async fn check(
    args: &Args,
    deployment: &Deployment,
    selected: &[MarketConfig],
    chain: &Chain,
    store: &Store,
    key: &Key,
) -> anyhow::Result<Checked> {
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
    let collateral = &deployment.collateral_type;

    // The capability: over the account, held by the key's address.
    let cap = chain
        .object(args.account_cap)
        .await
        .context("The account capability")?;
    let cap_type = struct_tag(&cap)?;
    ensure!(
        is(&cap_type, None, "authority", "AuthorityCap") && cap_type.type_params().len() == 2,
        "{} is a {}, not an account capability",
        args.account_cap,
        cap.object_type
    );
    let context = &cap_type.type_params()[0];
    ensure!(
        matches!(context, TypeTag::Struct(s) if is(s, Some(types), "authority", "ACCOUNT")),
        "{} is a capability over {context}, not over an engine account",
        args.account_cap
    );
    let role = cap_type.type_params()[1].clone();
    let role_name = match &role {
        TypeTag::Struct(s) if is(s, None, "authority", "ADMIN") => "admin",
        TypeTag::Struct(s) if is(s, None, "authority", "ASSISTANT") => "assistant",
        other => bail!(
            "{} has role {other}, neither ADMIN nor ASSISTANT",
            args.account_cap
        ),
    };
    ensure!(
        owned_by(&cap, &key.address),
        "{} is not held by {} (owner: {:?})",
        args.account_cap,
        key.address,
        cap.owner
    );
    let cap_for = cap
        .json
        .get("for")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    ensure!(
        same_id(cap_for, &args.account),
        "{} is a capability over {cap_for}, not over {}",
        args.account_cap,
        args.account
    );
    if role_name == "admin" {
        warn!(
            "Running with the admin capability, which can withdraw the account's collateral; an assistant capability can liquidate as well and cannot"
        );
    }

    // The account.
    let account = chain
        .object(args.account)
        .await
        .context("The liquidator's account")?;
    let account_type = struct_tag(&account)?;
    ensure!(
        is(&account_type, Some(types), "account", "Account")
            && account_type.type_params() == [collateral.clone()],
        "{} is a {}, not an Account<{collateral}>",
        args.account,
        account.object_type
    );
    let account_id: i64 = json_u64(&account.json, "account_id")
        .with_context(|| format!("{} has no account ID", args.account))?
        .try_into()?;
    let indexed = store
        .account(&db_id(&args.account))
        .await?
        .with_context(|| {
            format!(
                "The indexer has not seen account {}; is it caught up?",
                args.account
            )
        })?;
    ensure!(
        indexed.account_id == account_id,
        "The indexer has account {} as ID {}, the chain as {account_id}",
        args.account,
        indexed.account_id
    );

    let engine = Engine {
        package: deployment.perpetuals,
        types_package: types,
        collateral: collateral.clone(),
        role,
        account: args.account,
        cap: args.account_cap,
    };

    // The markets and their price feeds.
    let feed = |info: &ObjectInfo| {
        struct_tag(info).is_ok_and(|t| is(&t, None, "price_feed_storage", "PriceFeedStorage"))
    };
    let collateral_feed = chain
        .object(deployment.collateral_feed)
        .await
        .context("The collateral price feed")?;
    ensure!(
        feed(&collateral_feed),
        "{} is not a price feed storage",
        deployment.collateral_feed
    );
    let mut markets = Vec::new();
    let mut missing = Vec::new();
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
        let base_feed = chain
            .object(config.base_feed)
            .await
            .with_context(|| format!("The {} price feed", config.ticker))?;
        ensure!(
            feed(&base_feed),
            "{} of {} is not a price feed storage",
            config.base_feed,
            config.ticker
        );
        let market = Market {
            ticker: config.ticker.clone(),
            db_id: db_id(&config.clearing_house),
            objects: MarketObjects {
                clearing_house: config.clearing_house,
                base_feed: config.base_feed,
                collateral_feed: deployment.collateral_feed,
            },
        };
        if !store.has_position(&market.db_id, account_id).await? {
            missing.push(market.clone());
        }
        markets.push(market);
    }

    if !missing.is_empty() {
        let tickers: Vec<&str> = missing.iter().map(|m| m.ticker.as_str()).collect();
        if !args.create_positions {
            bail!(
                "The liquidator has no position in {}; run once with --create-positions",
                tickers.join(", ")
            );
        }
        ensure!(
            !args.dry_run && !args.check_only,
            "--create-positions sends a transaction, which a dry run or a check does not"
        );
        create_positions(chain, key, &engine, &missing).await?;
        info!(
            markets = tickers.join(", "),
            "Opened the liquidator's positions"
        );
    }

    let oracle = match &args.oracle_updates_url {
        None => None,
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
            info!(source = %updates.relay.source, source_id, "Relaying the oracle service's prices");
            Some(OracleSetup {
                client,
                source_id: i32::try_from(source_id)?,
            })
        }
    };

    let adl = match args.adl_cap {
        None => None,
        Some(cap_id) => {
            let registry = deployment
                .registry
                .context("--adl-cap needs the registry in the deployment file")?;
            let cap = chain.object(cap_id).await.context("The ADL capability")?;
            let tag = struct_tag(&cap)?;
            let param = |i: usize, name: &str| matches!(tag.type_params().get(i), Some(TypeTag::Struct(s)) if is(s, Some(types), "authority", name));
            ensure!(
                is(&tag, None, "authority", "AuthorityCap")
                    && param(0, "PACKAGE")
                    && param(1, "ADL"),
                "{cap_id} is a {}, not the engine's ADL capability",
                cap.object_type
            );
            ensure!(
                owned_by(&cap, &key.address),
                "{cap_id} is not held by {}",
                key.address
            );
            Some(AdlSetup {
                cap: cap_id,
                registry,
            })
        }
    };

    Ok(Checked {
        engine,
        account_id,
        markets,
        oracle,
        adl,
    })
}

async fn create_positions(
    chain: &Chain,
    key: &Key,
    engine: &Engine,
    markets: &[Market],
) -> anyhow::Result<()> {
    let unresolved = haneul_transaction_builder::ObjectInput::new;
    let mut b = Builder::new(engine, &unresolved);
    for market in markets {
        b.create_position(market.objects.clearing_house);
    }
    let mut tx = b.finish();
    tx.set_sender(key.address);
    let tx = chain
        .build(tx)
        .await
        .context("Opening positions would fail")?;
    let signature = key.private.sign_transaction(&tx)?;
    let outcome = chain.execute(&tx, signature).await?;
    ensure!(
        outcome.success,
        "Opening positions failed: {}",
        outcome.error.unwrap_or_default()
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

fn owned_by(info: &ObjectInfo, address: &Address) -> bool {
    info.owner
        .as_deref()
        .is_some_and(|owner| same_id(owner, address))
}

fn same_id(text: &str, address: &Address) -> bool {
    text.parse::<Address>().is_ok_and(|a| a == *address)
}

/// A u64 field, which JSON renderings of Move values write as a string.
fn json_u64(value: &serde_json::Value, field: &str) -> Option<u64> {
    match value.get(field)? {
        serde_json::Value::String(s) => s.parse().ok(),
        serde_json::Value::Number(n) => n.as_u64(),
        _ => None,
    }
}
