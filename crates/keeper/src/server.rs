// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! `POST /v1/details` and `/health`.
//!
//! Handing in details needs no signature: only the true details hash to a ticket's
//! commitment, so anything else is stored under a commitment no ticket holds and is dropped
//! after `--unmatched-ttl-secs`. The answer carries the commitment, which the front end
//! compares with its own before it asks for the ticket's transaction to be signed.

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
use crate::now_ms;
use crate::store::{Inserted, Store};

#[derive(Clone)]
pub struct App {
    pub store: Store,
    pub max_unmatched: i64,
}

pub fn router(app: App, max_body_bytes: usize) -> Router {
    Router::new()
        .route("/v1/details", post(submit))
        .route("/health", get(health))
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

async fn health(State(app): State<App>) -> Response {
    match app.store.ping().await {
        Ok(()) => Json(json!({"healthy": true})).into_response(),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"healthy": false, "reasons": [format!("{e:#}")]})),
        )
            .into_response(),
    }
}

fn refuse(status: StatusCode, message: String) -> Response {
    (status, Json(json!({"error": message}))).into_response()
}
