// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! `POST /v1/details`, `/health`, `/status` and `/metrics`.
//!
//! Handing in details needs no signature: only the true details hash to a ticket's
//! commitment, so anything else is stored under a commitment no ticket holds and is dropped
//! after `--unmatched-ttl-secs`. The answer carries the commitment, which the front end
//! compares with its own before it asks for the ticket's transaction to be signed.

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde_json::json;
use tracing::{error, info};

use crate::details::Submission;
use crate::metrics::Metrics;
use crate::now_ms;
use crate::status::Board;
use crate::store::{Inserted, Store};

#[derive(Clone)]
pub struct App {
    pub store: Store,
    pub max_unmatched: i64,
    pub board: Arc<Board>,
    pub metrics: Arc<Metrics>,
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

async fn submit(State(app): State<App>, body: Result<Json<Submission>, JsonRejection>) -> Response {
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
    match app
        .store
        .insert(&commitment, kind, &details, now_ms(), app.max_unmatched)
        .await
    {
        Ok(Inserted::Full) => refuse(
            StatusCode::SERVICE_UNAVAILABLE,
            "Too many details are waiting for their tickets".to_owned(),
        ),
        Ok(inserted) => {
            let commitment = hex::encode(commitment);
            let known = inserted == Inserted::Known;
            if !known {
                info!(kind, commitment, "Stored ticket details");
            }
            Json(json!({"commitment": commitment, "known": known})).into_response()
        }
        Err(e) => {
            error!("Failed to store ticket details: {e:#}");
            refuse(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to store the details".to_owned(),
            )
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
