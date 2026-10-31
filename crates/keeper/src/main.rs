// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

use std::io::IsTerminal;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use perp_bot_common::alerts::{Alerts, Level};
use perp_bot_common::chain::Chain;
use perp_bot_common::deployment::Deployment;
use perp_bot_common::keys;
use perp_bot_common::store::Store as Snapshots;
use perp_keeper::config::Args;
use perp_keeper::executor::{Executor, Parts, Settings};
use perp_keeper::matcher;
use perp_keeper::metrics::Metrics;
use perp_keeper::now_ms;
use perp_keeper::server::{App, router};
use perp_keeper::setup;
use perp_keeper::status::{Board, Status};
use perp_keeper::store::{Indexer, Store};
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
    let store = Store::connect(&args.details_database_url, args.pool_size).await?;
    let indexer = Indexer::connect(&args.database_url, args.pool_size).await?;
    let metrics = Arc::new(Metrics::new()?);
    let mode = match (args.executes(), args.dry_run) {
        (false, _) => "details-only",
        (true, false) => "live",
        (true, true) => "dry-run",
    };
    let board = Arc::new(Board::new(
        Status {
            mode: mode.to_owned(),
            started_at_ms: now_ms(),
            ..Status::default()
        },
        // A round normally takes a poll interval and a few executions; give slow ones room.
        args.executes()
            .then(|| (args.poll_interval() * 30).max(Duration::from_secs(60))),
    ));

    let executor = if args.executes() {
        Some(
            executor(
                &args,
                store.clone(),
                indexer.clone(),
                metrics.clone(),
                board.clone(),
            )
            .await?,
        )
    } else {
        info!(
            "Taking details in only: without --deployment, --rpc-url and --key-file no ticket is executed"
        );
        None
    };
    if args.check_only {
        return Ok(());
    }

    let cancel = CancellationToken::new();
    let matcher = tokio::spawn(matcher::run(
        store.clone(),
        indexer,
        Duration::from_secs(args.unmatched_ttl_secs),
        args.match_interval(),
        cancel.clone(),
    ));

    let listener = tokio::net::TcpListener::bind(args.listen_address)
        .await
        .with_context(|| format!("Failed to listen on {}", args.listen_address))?;
    info!(
        address = %args.listen_address,
        "Serving POST /v1/details, /health, /status and /metrics"
    );
    let app = App {
        store,
        max_unmatched: args.max_unmatched,
        board,
        metrics,
    };
    let server = {
        let cancel = cancel.clone();
        let app = router(app, args.max_body_bytes);
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move { cancel.cancelled().await })
                .await
        })
    };
    {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            info!("Stopping");
            cancel.cancel();
        });
    }

    match executor {
        Some(executor) => executor.run(cancel.clone()).await,
        None => cancel.cancelled().await,
    }
    cancel.cancel();
    let _ = matcher.await;
    server
        .await
        .context("The server task failed")?
        .context("The server failed")
}

/// Checks what executing tickets needs and sets the executor up.
async fn executor(
    args: &Args,
    store: Store,
    indexer: Indexer,
    metrics: Arc<Metrics>,
    board: Arc<Board>,
) -> anyhow::Result<Executor> {
    let (Some(path), Some(rpc_url), Some(key_file)) =
        (&args.deployment, &args.rpc_url, &args.key_file)
    else {
        anyhow::bail!("Executing tickets takes --deployment, --rpc-url and --key-file");
    };
    let deployment = Deployment::load(path)?;
    let key = keys::load(key_file, args.address)?;
    let chain = Chain::new(rpc_url, Duration::from_secs(args.execute_timeout_secs))?;
    let setup = setup::check(args, &deployment, &chain, &key).await?;
    let tickers: Vec<&str> = setup.markets.iter().map(|m| m.ticker.as_str()).collect();
    info!(
        markets = tickers.join(", "),
        oracle = setup.oracle.is_some(),
        dry_run = args.dry_run,
        "Startup checks passed"
    );
    let snapshots = Snapshots::connect(&args.database_url, args.pool_size).await?;
    let alerts = Arc::new(Alerts::new(
        args.alert_webhook_url.clone(),
        args.alert_format,
        Duration::from_secs(args.alert_repeat_secs),
        metrics.alerts.clone(),
        format!("perp-keeper {}", tickers.join(",")),
    )?);
    board.update(|s| s.address = Some(key.address.to_string()));
    if !args.check_only {
        alerts.raise(
            Level::Info,
            "started",
            format!(
                "Started {} on {}",
                if args.dry_run {
                    "in dry-run mode"
                } else {
                    "live"
                },
                tickers.join(", ")
            ),
        );
    }
    Ok(Executor::new(Parts {
        settings: Settings {
            dry_run: args.dry_run,
            poll_interval: args.poll_interval(),
            max_slippage_bps: args.max_slippage_bps,
            retry: Duration::from_secs(args.retry_secs),
            max_retry: Duration::from_secs(args.max_retry_secs),
            alert_after: args.alert_after,
            max_gas_budget: args.max_gas_budget,
            min_gas_balance: args.min_gas_balance,
        },
        chain,
        key,
        engine: setup.engine,
        markets: setup.markets,
        oracle: setup.oracle,
        store,
        indexer,
        snapshots,
        metrics,
        alerts,
        board,
    }))
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
