// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

use std::io::IsTerminal;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, ensure};
use clap::Parser;
use perp_bot_common::alerts::{Alerts, Level};
use perp_bot_common::chain::Chain;
use perp_bot_common::deployment::{Deployment, db_id};
use perp_bot_common::keys;
use perp_bot_common::oracle::OracleClient;
use perp_bot_common::store::Store;
use perp_maker::config::Args;
use perp_maker::metrics::Metrics;
use perp_maker::model::Params;
use perp_maker::status::{Board, Status, now_ms, router};
use perp_maker::{flow, maker, setup};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    if args.log_json {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_ansi(std::io::stdout().is_terminal())
            .with_env_filter(filter)
            .init();
    }
    match run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!("{e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> anyhow::Result<()> {
    args.validate()?;
    let deployment = Deployment::load(&args.deployment)?;
    let config = deployment
        .select(std::slice::from_ref(&args.market))?
        .remove(0);
    let key = keys::load(&args.key_file, args.address)?;
    let chain = Chain::new(
        &args.rpc_url,
        Duration::from_secs(args.execute_timeout_secs),
    )?;
    let oracle = OracleClient::new(args.oracle_updates_url.clone())?;

    let role = setup::check(
        &deployment,
        &config,
        &chain,
        &key,
        &setup::Wanted {
            chain_id: args.chain_id.as_deref(),
            address: args.address,
            account: args.account,
            account_cap: args.account_cap,
            create_position: args.create_position,
            leverage: args.leverage,
            dry_run: args.dry_run,
        },
        "maker",
    )
    .await?;
    setup::check_oracle(&oracle, &role.market).await?;
    let resting = match (&args.database_url, &args.indexer_url) {
        (Some(url), _) => {
            let store = Store::connect(url, 2).await?;
            let ids = store
                .open_orders(&db_id(&role.market.clearing_house), role.account_id as i64)
                .await
                .context("The indexer's open orders")?;
            info!(
                resting = ids.len(),
                "Adopted the account's resting orders from the indexer"
            );
            ids
        }
        (None, Some(url)) => {
            let ids =
                open_orders_over_http(url, &key.address, args.subaccount_number, &config.ticker)
                    .await
                    .context("The indexer API's open orders")?;
            info!(
                resting = ids.len(),
                "Adopted the account's resting orders from the indexer API"
            );
            ids
        }
        (None, None) => {
            warn!(
                "Neither --database-url nor --indexer-url: orders resting from before are not known and rest until they expire"
            );
            Vec::new()
        }
    };

    let flow_role = if args.flow {
        ensure!(
            flow::is_test_collateral(&deployment.collateral_type),
            "--flow is a shadow-run tool and is refused on {} collateral",
            deployment.collateral_type
        );
        let flow_key = keys::load(
            args.flow_key_file.as_ref().expect("validated"),
            args.flow_address,
        )?;
        ensure!(
            flow_key.address != key.address,
            "the flow simulator must not share the maker's key: their transactions would lock each other's gas"
        );
        let role = setup::check(
            &deployment,
            &config,
            &chain,
            &flow_key,
            &setup::Wanted {
                chain_id: args.chain_id.as_deref(),
                address: args.flow_address,
                account: args.flow_account.expect("validated"),
                account_cap: args.flow_account_cap.expect("validated"),
                create_position: args.create_position,
                leverage: args.leverage,
                dry_run: args.dry_run,
            },
            "flow",
        )
        .await?;
        warn!(
            address = %flow_key.address,
            "The flow simulator is on: this account trades at random against the maker"
        );
        Some((flow_key, role))
    } else {
        None
    };
    info!(
        market = config.ticker,
        dry_run = args.dry_run,
        flow = args.flow,
        "Startup checks passed"
    );
    if args.check_only {
        return Ok(());
    }

    let metrics = Arc::new(Metrics::new()?);
    let alerts = Arc::new(Alerts::new(
        args.alert_webhook_url.clone(),
        args.alert_format,
        Duration::from_secs(args.alert_repeat_secs),
        metrics.alerts.clone(),
        format!("perp-maker {}", config.ticker),
    )?);
    let board = Arc::new(Board::new(
        Status {
            mode: if args.dry_run { "dry-run" } else { "live" }.to_owned(),
            address: key.address.to_string(),
            market: config.ticker.clone(),
            started_at_ms: now_ms(),
            ..Status::default()
        },
        Duration::from_secs(args.refresh_secs * 2 + 30),
    ));

    let cancel = CancellationToken::new();
    let listener = tokio::net::TcpListener::bind(args.listen_address)
        .await
        .with_context(|| format!("Failed to listen on {}", args.listen_address))?;
    info!(address = %args.listen_address, "Serving /health, /status and /metrics");
    let server = {
        let cancel = cancel.clone();
        let app = router(board.clone(), metrics.clone());
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move { cancel.cancelled().await })
                .await
        })
    };

    alerts.raise(
        Level::Info,
        "started",
        format!(
            "Started {} on {}{}",
            if args.dry_run {
                "in dry-run mode"
            } else {
                "live"
            },
            config.ticker,
            if args.flow {
                " with the flow simulator"
            } else {
                ""
            }
        ),
    );

    let base_source_id = role.view.base_source_id();
    let collateral_source_id = role.view.collateral_source_id();
    let params = Params {
        levels: args.levels,
        half_spread_bps: args.half_spread_bps.clone(),
        min_half_spread_bps: args.min_half_spread_bps.clone(),
        vol_multiplier: args.vol_multiplier.clone(),
        conf_multiplier: args.conf_multiplier.clone(),
        level_step_bps: args.level_step_bps.clone(),
        size_base: args.size.clone(),
        size_growth: args.size_growth.clone(),
        max_position: args.max_position.clone(),
        hard_position: args.hard_position.clone(),
        skew_bps: args.skew_bps(),
        flatten_slippage_bps: args.flatten_slippage_bps.clone(),
        min_order_usd: role.view.min_order_usd.clone(),
        min_health: args.min_health.clone(),
        tick: role.view.tick_size,
        lot: role.view.lot_size,
    };
    let maker = maker::Maker::new(maker::Parts {
        settings: maker::Settings {
            dry_run: args.dry_run,
            poll_interval: args.poll_interval(),
            params,
            requote_bps: args.requote_bps.clone(),
            refresh: Duration::from_secs(args.refresh_secs),
            expire: Duration::from_secs(args.expire_secs),
            min_interval: Duration::from_secs(args.min_interval_secs),
            vol_alpha: args.vol_alpha.clone(),
            max_price_age: Duration::from_secs(args.max_price_age_secs),
            max_confidence_bps: args.max_confidence_bps.clone(),
            max_failures: args.max_failures,
            retry: Duration::from_secs(args.retry_secs),
            max_gas_budget: args.max_gas_budget,
            min_gas_balance: args.min_gas_balance,
            params_refresh: Duration::from_secs(args.params_refresh_secs),
            base_source_id,
            collateral_source_id,
        },
        chain: chain.clone(),
        key,
        role,
        oracle: oracle.clone(),
        metrics: metrics.clone(),
        alerts: alerts.clone(),
        board: board.clone(),
        resting,
    });

    let flow_task = flow_role.map(|(flow_key, role)| {
        let flow = flow::Flow::new(flow::Parts {
            settings: flow::Settings {
                dry_run: args.dry_run,
                mean: Duration::from_secs(args.flow_mean_secs),
                size: args.flow_size.clone(),
                slippage_bps: args.flow_slippage_bps.clone(),
                max_position: args.flow_max_position.clone(),
                max_gas_budget: args.max_gas_budget,
                base_source_id,
            },
            chain: chain.clone(),
            key: flow_key,
            role,
            oracle: oracle.clone(),
            metrics: metrics.clone(),
            alerts: alerts.clone(),
            board: board.clone(),
        });
        let cancel = cancel.clone();
        tokio::spawn(async move { flow.run(cancel).await })
    });

    {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            info!("Stopping: pulling the quotes");
            cancel.cancel();
        });
    }
    maker.run(cancel.clone()).await;
    cancel.cancel();
    if let Some(task) = flow_task {
        let _ = task.await;
    }
    let _ = server.await;
    Ok(())
}

/// The IDs of the account's open orders, from the indexer API in the dYdX protocol's shape.
async fn open_orders_over_http(
    indexer: &url::Url,
    address: &haneul_sdk_types::Address,
    subaccount_number: u32,
    ticker: &str,
) -> anyhow::Result<Vec<u128>> {
    let mut url = indexer.join("/v4/orders/parentSubaccountNumber")?;
    url.query_pairs_mut()
        .append_pair("address", &address.to_string())
        .append_pair("parentSubaccountNumber", &subaccount_number.to_string())
        .append_pair("status", "OPEN")
        .append_pair("ticker", ticker)
        .append_pair("limit", "100");
    let orders: Vec<serde_json::Value> = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    orders
        .iter()
        .filter_map(|o| o.get("id").and_then(|id| id.as_str()))
        .map(|id| {
            id.parse::<u128>()
                .map_err(|e| anyhow::anyhow!("order id {id}: {e}"))
        })
        .collect()
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("a SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = ctrl_c.await;
}
