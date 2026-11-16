// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! `POST /v1/details`, `/health`, `/status` and `/metrics`.
//!
//! Handing in details needs no signature: only the true details hash to a ticket's
//! commitment, so anything else is stored under a commitment no ticket holds and is dropped
//! after `--unmatched-ttl-secs`, or pushed out sooner by newer details. The answer carries the
//! commitment, which the front end compares with its own before it asks for the ticket's
//! transaction to be signed. It hands the details in again once the ticket is on chain; details
//! a ticket already holds are kept whatever waits, so nothing a flood pushed out is lost.
//!
//! The intake works on a bounded number of requests at once and takes a bounded number from
//! one client; one over either bound is turned away at once (`intake`). It has database
//! connections of its own, so that nothing handed in holds one that a ticket is executed with.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Json;
use axum::Router;
use axum::extract::rejection::JsonRejection;
use axum::extract::{ConnectInfo, DefaultBodyLimit, State};
use axum::http::header::RETRY_AFTER;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use perp_bot_common::alerts::{Alerts, Level};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::details::Submission;
use crate::intake::{Door, Refused};
use crate::metrics::Metrics;
use crate::now_ms;
use crate::status::Board;
use crate::store::{Evicted, Indexer, Inserted, Limits, Store};

#[derive(Clone)]
pub struct App {
    /// The intake's own connections to the details database.
    pub store: Store,
    /// The intake's own connections to the indexer's database, which tells whether a ticket
    /// on chain already holds handed-in details.
    pub indexer: Indexer,
    pub limits: Limits,
    pub door: Arc<Door>,
    pub board: Arc<Board>,
    pub metrics: Arc<Metrics>,
    pub alerts: Arc<Alerts>,
}

pub fn router(app: App, max_body_bytes: usize) -> Router {
    Router::new()
        .route("/v1/details", post(submit))
        .route("/health", get(health))
        .route("/status", get(status))
        .route("/metrics", get(metrics_text))
        .layer(DefaultBodyLimit::max(max_body_bytes))
        .with_state(app)
}

async fn submit(
    State(app): State<App>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Result<Json<Submission>, JsonRejection>,
) -> Response {
    let source = source(peer, &headers);
    // Held until the answer is ready: the place of this request among the ones worked on.
    let _place = match app.door.enter(&source, Instant::now()) {
        Ok(place) => place,
        Err(refused) => return turn_away(&app, refused),
    };
    let Json(submission) = match body {
        Ok(body) => body,
        Err(rejection) => return refuse(rejection.status(), rejection.body_text()),
    };
    let commitment = match submission.commitment() {
        Ok(commitment) => commitment,
        Err(e) => return refuse(StatusCode::BAD_REQUEST, format!("{e:#}")),
    };
    let details = serde_json::to_string(&submission).expect("a submission serializes");
    let kind = submission.kind();
    let stored = tokio::time::timeout(
        app.door.timeout,
        store(&app, &commitment, kind, &details, &source),
    )
    .await;
    let (inserted, evicted, held) = match stored {
        Ok(Ok(stored)) => stored,
        Ok(Err(e)) => {
            error!("Failed to store ticket details: {e:#}");
            return refuse(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to store the details".to_owned(),
            );
        }
        Err(_) => return turn_away(&app, Refused::Slow),
    };
    let commitment = hex::encode(commitment);
    let known = inserted == Inserted::Known;
    if !known {
        info!(kind, commitment, held, "Stored ticket details");
    }
    if evicted.source + evicted.capacity > 0 {
        let counter = &app.metrics.details_evicted;
        counter
            .with_label_values(&["source"])
            .inc_by(evicted.source as u64);
        counter
            .with_label_values(&["capacity"])
            .inc_by(evicted.capacity as u64);
        warn!(
            source,
            by_source = evicted.source,
            by_capacity = evicted.capacity,
            "Pushed waiting details out to make room"
        );
    }
    Json(json!({"commitment": commitment, "known": known, "held": held})).into_response()
}

/// Looks up whether a ticket holds the commitment, and stores the details.
async fn store(
    app: &App,
    commitment: &[u8; 32],
    kind: &str,
    details: &str,
    source: &str,
) -> anyhow::Result<(Inserted, Evicted, bool)> {
    let held = match app.indexer.held(&[commitment.to_vec()]).await {
        Ok(held) => !held.is_empty(),
        Err(e) => {
            warn!("Failed to look the commitment up in the indexer: {e:#}");
            false
        }
    };
    let (inserted, evicted) = app
        .store
        .insert(
            commitment,
            kind,
            details,
            now_ms(),
            source,
            held,
            app.limits,
        )
        .await?;
    Ok((inserted, evicted, held))
}

/// Turns a request away and counts it. Enough of them within a minute are a flood: a problem
/// on `/status` and one alert. Anyone can make the intake turn requests away, so the alert is
/// one for the flood and is not repeated for as long as it lasts.
fn turn_away(app: &App, refused: Refused) -> Response {
    app.metrics
        .details_refused
        .with_label_values(&[refused.reason()])
        .inc();
    if let Some(count) = app.door.refused(Instant::now()) {
        let message = format!(
            "The intake turned {count} requests away within a minute, the last as `{}`. \
             Open tickets are executed as before; the details of a new one may be refused",
            refused.reason()
        );
        warn!("{message}");
        if app
            .board
            .set_problem("intake", Level::Warning, message.clone())
        {
            app.alerts.raise(Level::Warning, "intake", message);
        }
    }
    let (status, wait, message) = match refused {
        Refused::TooMany(wait) => {
            let secs = wait.as_secs_f64().ceil().max(1.0) as u64;
            (
                StatusCode::TOO_MANY_REQUESTS,
                secs,
                format!("Too many details from this address; try again in {secs} s"),
            )
        }
        Refused::Busy => (
            StatusCode::SERVICE_UNAVAILABLE,
            1,
            "The keeper is taking as many details as it can at once; try again".to_owned(),
        ),
        Refused::Slow => (
            StatusCode::SERVICE_UNAVAILABLE,
            1,
            "Storing the details took too long; try again".to_owned(),
        ),
    };
    (
        status,
        [(RETRY_AFTER, wait.to_string())],
        Json(json!({"error": message})),
    )
        .into_response()
}

/// Takes the flood's problem back once the intake has turned nothing away for a minute.
pub async fn watch(app: App, cancel: CancellationToken) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_secs(5)) => {}
        }
        if app.door.calm(Instant::now()) && app.board.clear_problem("intake") {
            info!("The intake turns no request away any more");
        }
    }
}

/// Healthy while the details database answers and, when executing, rounds complete with no
/// critical problem open.
async fn health(State(app): State<App>) -> Response {
    let (mut healthy, mut reasons) = app.board.health();
    if let Err(e) = app.store.ping().await {
        healthy = false;
        reasons.insert(0, format!("{e:#}"));
    }
    let code = if healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, Json(json!({"healthy": healthy, "reasons": reasons}))).into_response()
}

async fn status(State(app): State<App>) -> Response {
    Json(app.board.snapshot()).into_response()
}

async fn metrics_text(State(app): State<App>) -> Response {
    (
        [("content-type", "text/plain; version=0.0.4")],
        app.metrics.exposition(),
    )
        .into_response()
}

fn refuse(status: StatusCode, message: String) -> Response {
    (status, Json(json!({"error": message}))).into_response()
}

/// The client that handed details in: the peer, or, when the peer is the site's reverse proxy
/// on this host, the last address it added to `X-Forwarded-For` (Caddy replaces what an
/// untrusted client sends there with the client's own address).
fn source(peer: SocketAddr, headers: &HeaderMap) -> String {
    if peer.ip().is_loopback()
        && let Some(forwarded) = headers
            .get("x-forwarded-for")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.rsplit(',').next())
            .map(str::trim)
            .filter(|address| !address.is_empty())
    {
        return forwarded.to_owned();
    }
    peer.ip().to_string()
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    #[test]
    fn the_source_is_the_proxys_last_forwarded_address_or_the_peer() {
        let proxy: SocketAddr = "127.0.0.1:50000".parse().unwrap();
        let remote: SocketAddr = "203.0.113.7:50000".parse().unwrap();
        let mut headers = HeaderMap::new();
        assert_eq!(source(proxy, &headers), "127.0.0.1");
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("198.51.100.1, 192.0.2.9"),
        );
        assert_eq!(source(proxy, &headers), "192.0.2.9");
        // Only the proxy on this host is believed.
        assert_eq!(source(remote, &headers), "203.0.113.7");
        headers.insert("x-forwarded-for", HeaderValue::from_static(" "));
        assert_eq!(source(proxy, &headers), "127.0.0.1");
    }
}
