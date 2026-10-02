// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

use std::io::IsTerminal;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use perp_liquidator::alerts::{Alerts, Level};
use perp_liquidator::chain::Chain;
use perp_liquidator::config::{Args, Deployment, db_id};
use perp_liquidator::liquidator::{Liquidator, Parts, Settings};
use perp_liquidator::metrics::Metrics;
use perp_liquidator::status::{Board, Status, now_ms, router};
use perp_liquidator::store::Store;
use perp_liquidator::{keys, setup};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
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
        // Colors only on a terminal: in a file or a journal they are noise.
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
    let selected = deployment.select(&args.markets)?;
    let key = keys::load(&args.key_file, args.address)?;
    let chain = Chain::new(
        &args.rpc_url,
        Duration::from_secs(args.execute_timeout_secs),
    )?;
    let store = Store::connect(&args.database_url, args.db_pool_size).await?;

    let checked = setup::check(&args, &deployment, &selected, &chain, &store, &key).await?;
    let tickers: Vec<&str> = checked.markets.iter().map(|m| m.ticker.as_str()).collect();
    info!(
        account = %args.account,
        account_id = checked.account_id,
        markets = tickers.join(", "),
        oracle = checked.oracle.is_some(),
        adl = checked.adl.is_some(),
        dry_run = args.dry_run,
        "Startup checks passed"
    );
    if args.check_only {
        return Ok(());
    }

    let metrics = Arc::new(Metrics::new()?);
    let service = format!("perp-liquidator {}", tickers.join(","));
    let alerts = Arc::new(Alerts::new(
        args.alert_webhook_url.clone(),
        args.alert_format,
        Duration::from_secs(args.alert_repeat_secs),
        metrics.clone(),
        service,
    )?);
    let board = Arc::new(Board::new(
        Status {
            mode: if args.dry_run { "dry-run" } else { "live" }.to_owned(),
            address: key.address.to_string(),
            account_id: checked.account_id,
            started_at_ms: now_ms(),
            ..Status::default()
        },
        // A round normally takes a poll interval; give slow transactions room.
        (args.poll_interval() * 40).max(Duration::from_secs(60)),
        (args.max_indexer_lag_secs * 1000) as i64,
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

    let settings = Settings {
        dry_run: args.dry_run,
        poll_interval: args.poll_interval(),
        full_reload: Duration::from_secs(args.full_reload_secs),
        attempt_buffer: args.attempt_buffer(),
        max_liquidations_per_round: args.max_liquidations_per_round,
        exclude: args.exclude_accounts.iter().copied().collect(),
        unwind: args.unwind,
        unwind_slippage: args.unwind_slippage(),
        unwind_interval: Duration::from_millis(args.unwind_interval_ms),
        max_inventory_usd: args.max_inventory_usd,
        max_gas_budget: args.max_gas_budget,
        min_gas_balance: args.min_gas_balance,
        min_collateral: args.min_collateral,
        max_indexer_lag_ms: (args.max_indexer_lag_secs * 1000) as i64,
    };
    alerts.raise(
        Level::Info,
        "started",
        format!(
            "Started {} as account {} on {}",
            if args.dry_run {
                "in dry-run mode"
            } else {
                "live"
            },
            checked.account_id,
            tickers.join(", ")
        ),
    );
    let liquidator = Liquidator::new(Parts {
        settings,
        store,
        chain,
        key,
        engine: checked.engine,
        account_id: checked.account_id,
        account_db_id: db_id(&args.account),
        collateral_decimals: deployment.collateral_decimals,
        markets: checked.markets,
        oracle: checked.oracle,
        adl: checked.adl,
        metrics,
        alerts,
        board,
    });

    {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            info!("Stopping after the current round");
            cancel.cancel();
        });
    }
    liquidator.run(cancel.clone()).await;
    cancel.cancel();
    let _ = server.await;
    Ok(())
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
