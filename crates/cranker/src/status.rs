// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! `/health`, `/status` and `/metrics`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use perp_bot_common::alerts::Level;
use serde::Serialize;

use crate::metrics::Metrics;

#[derive(Clone, Debug, Default, Serialize)]
pub struct MarketStatus {
    pub paused: u64,
    pub funding_last_upd_ms: u64,
    pub next_funding_ms: u64,
    pub missed_funding_intervals: u64,
    pub premium_twap_last_upd_ms: u64,
    pub spread_twap_last_upd_ms: u64,
    pub last_crank_ms: Option<i64>,
    pub last_digest: Option<String>,
    /// What was due at the last crank.
    pub last_due: Vec<&'static str>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Status {
    pub mode: String,
    pub address: String,
    pub started_at_ms: i64,
    pub last_round_ms: Option<i64>,
    pub cranks: u64,
    pub markets: BTreeMap<String, MarketStatus>,
    pub problems: BTreeMap<String, Problem>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Problem {
    pub level: Level,
    pub message: String,
}

pub struct Board {
    status: Mutex<Status>,
    last_round: Mutex<Option<Instant>>,
    /// `/health` fails when no round has completed for this long.
    stale_after: Duration,
}

impl Board {
    pub fn new(status: Status, stale_after: Duration) -> Self {
        Self {
            status: Mutex::new(status),
            last_round: Mutex::new(None),
            stale_after,
        }
    }

    pub fn update(&self, f: impl FnOnce(&mut Status)) {
        f(&mut self.status.lock().unwrap());
    }

    pub fn snapshot(&self) -> Status {
        self.status.lock().unwrap().clone()
    }

    pub fn round_done(&self) {
        *self.last_round.lock().unwrap() = Some(Instant::now());
        self.update(|s| s.last_round_ms = Some(now_ms()));
    }

    /// Sets a problem; true when it is new or changed level.
    pub fn set_problem(&self, key: &str, level: Level, message: String) -> bool {
        let mut status = self.status.lock().unwrap();
        let changed = status.problems.get(key).is_none_or(|p| p.level != level);
        status
            .problems
            .insert(key.to_owned(), Problem { level, message });
        changed
    }

    /// Clears a problem; true when there was one.
    pub fn clear_problem(&self, key: &str) -> bool {
        self.status.lock().unwrap().problems.remove(key).is_some()
    }

    /// Healthy while rounds complete and no critical problem is open.
    pub fn health(&self) -> (bool, Vec<String>) {
        let mut reasons = Vec::new();
        match *self.last_round.lock().unwrap() {
            None => reasons.push("no round has completed yet".to_owned()),
            Some(at) if at.elapsed() > self.stale_after => {
                reasons.push(format!("no round for {} s", at.elapsed().as_secs()))
            }
            Some(_) => {}
        }
        for (key, problem) in &self.status.lock().unwrap().problems {
            if problem.level == Level::Critical {
                reasons.push(format!("{key}: {}", problem.message));
            }
        }
        (reasons.is_empty(), reasons)
    }
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

#[derive(Clone)]
struct App {
    board: Arc<Board>,
    metrics: Arc<Metrics>,
}

pub fn router(board: Arc<Board>, metrics: Arc<Metrics>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/status", get(status))
        .route("/metrics", get(metrics_text))
        .with_state(App { board, metrics })
}

async fn health(State(app): State<App>) -> impl IntoResponse {
    let (healthy, reasons) = app.board.health();
    let code = if healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        code,
        Json(serde_json::json!({"healthy": healthy, "reasons": reasons})),
    )
}

async fn status(State(app): State<App>) -> impl IntoResponse {
    Json(app.board.snapshot())
}

async fn metrics_text(State(app): State<App>) -> impl IntoResponse {
    (
        [("content-type", "text/plain; version=0.0.4")],
        app.metrics.exposition(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_needs_a_recent_round_and_no_critical_problem() {
        let board = Board::new(Status::default(), Duration::from_secs(60));
        assert!(!board.health().0);
        board.round_done();
        assert!(board.health().0);
        assert!(board.set_problem("gas", Level::Warning, "low".into()));
        assert!(board.health().0);
        assert!(board.set_problem("gas", Level::Critical, "empty".into()));
        let (healthy, reasons) = board.health();
        assert!(!healthy);
        assert_eq!(reasons, vec!["gas: empty".to_owned()]);
        assert!(!board.set_problem("gas", Level::Critical, "still empty".into()));
        assert!(board.clear_problem("gas"));
        assert!(board.health().0);
    }
}
