// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

use std::io::IsTerminal;
use std::process::ExitCode;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use perp_keeper::config::Args;
use perp_keeper::matcher;
use perp_keeper::server::{App, router};
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
    info!(address = %args.listen_address, "Serving POST /v1/details and /health");
    {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            info!("Stopping");
            cancel.cancel();
        });
    }
    let app = App {
        store,
        max_unmatched: args.max_unmatched,
    };
    let served = axum::serve(listener, router(app, args.max_body_bytes))
        .with_graceful_shutdown(cancel.clone().cancelled_owned())
        .await;
    cancel.cancel();
    let _ = matcher.await;
    served.context("The server failed")
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
