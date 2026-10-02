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
use perp_bot_common::oracle::OracleClient;
use perp_cranker::config::Args;
use perp_cranker::cranker::{Cranker, Parts, Settings, Stopped};
use perp_cranker::metrics::Metrics;
use perp_cranker::setup;
use perp_cranker::status::{Board, Status, now_ms, router};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

/// The exit code that tells a supervisor to start the cranker again on a changed deployment.
const DEPLOYMENT_CHANGED: u8 = 3;

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
        Ok(Stopped::Cancelled) => ExitCode::SUCCESS,
        Ok(Stopped::DeploymentChanged) => ExitCode::from(DEPLOYMENT_CHANGED),
        Err(e) => {
            error!("{e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> anyhow::Result<Stopped> {
    args.validate()?;
    let contents = std::fs::read(&args.deployment)
        .with_context(|| format!("Failed to read {}", args.deployment.display()))?;
    let deployment = Deployment::load(&args.deployment)?;
    let selected = deployment.select(&args.markets)?;
    let key = keys::load(&args.key_file, args.address)?;
    let chain = Chain::new(
        &args.rpc_url,
        Duration::from_secs(args.execute_timeout_secs),
    )?;
    let oracle = args
        .oracle_updates_url
        .clone()
        .map(OracleClient::new)
        .transpose()?;

    let markets =
        setup::check(&args, &deployment, &selected, &chain, &key, oracle.as_ref()).await?;
    let tickers: Vec<&str> = markets.iter().map(|m| m.ticker.as_str()).collect();
    info!(
        markets = tickers.join(", "),
        oracle = oracle.is_some(),
        dry_run = args.dry_run,
        "Startup checks passed"
    );
    if args.check_only {
        return Ok(Stopped::Cancelled);
    }

    let metrics = Arc::new(Metrics::new()?);
    let alerts = Arc::new(Alerts::new(
        args.alert_webhook_url.clone(),
        args.alert_format,
        Duration::from_secs(args.alert_repeat_secs),
        metrics.alerts.clone(),
        format!("perp-cranker {}", tickers.join(",")),
    )?);
    let board = Arc::new(Board::new(
        Status {
            mode: if args.dry_run { "dry-run" } else { "live" }.to_owned(),
            address: key.address.to_string(),
            started_at_ms: now_ms(),
            ..Status::default()
        },
        // A round normally takes a poll interval and a few cranks; give slow ones room.
        (args.poll_interval() * 30).max(Duration::from_secs(60)),
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
            "Started {} on {}",
            if args.dry_run {
                "in dry-run mode"
            } else {
                "live"
            },
            tickers.join(", ")
        ),
    );
    let cranker = Cranker::new(Parts {
        settings: Settings {
            dry_run: args.dry_run,
            poll_interval: args.poll_interval(),
            twap_min_interval_ms: args.twap_min_interval_ms,
            due_margin_ms: args.due_margin_ms,
            max_gas_budget: args.max_gas_budget,
            min_gas_balance: args.min_gas_balance,
            skip_after: args.skip_after,
            skip_for: Duration::from_secs(args.skip_secs),
            config_check: Duration::from_secs(args.config_check_secs),
        },
        chain,
        key,
        markets,
        oracle,
        metrics,
        alerts,
        board,
        deployment: (args.deployment.clone(), contents),
    });

    {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            info!("Stopping after the current round");
            cancel.cancel();
        });
    }
    let stopped = cranker.run(cancel.clone()).await;
    cancel.cancel();
    let _ = server.await;
    Ok(stopped)
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
