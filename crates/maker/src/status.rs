// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! `/health`, `/status` and `/metrics`.

use std::collections::{BTreeMap, VecDeque};
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

const KEPT_ROUNDS: usize = 50;

#[derive(Clone, Debug, Default, Serialize)]
pub struct LevelStatus {
    pub side: &'static str,
    pub price: String,
    pub size: String,
    pub reduce_only: bool,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct RoundRecord {
    pub at_ms: i64,
    pub reason: String,
    pub outcome: String,
    pub digest: Option<String>,
    pub levels: usize,
    pub gas: i64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Status {
    pub mode: String,
    pub address: String,
    pub market: String,
    pub started_at_ms: i64,
    pub last_round_ms: Option<i64>,
    pub rounds: u64,
    pub reference: Option<String>,
    pub mid: Option<String>,
    pub half_spread_bps: Option<String>,
    pub skew_bps: Option<String>,
    pub sigma_bps: Option<String>,
    pub inventory: Option<&'static str>,
    pub position: Option<String>,
    pub health: Option<String>,
    pub quoting: bool,
    pub resting_orders: usize,
    pub ladder: Vec<LevelStatus>,
    pub recent_rounds: VecDeque<RoundRecord>,
    pub flow: Option<FlowStatus>,
    pub problems: BTreeMap<String, Problem>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct FlowStatus {
    pub address: String,
    pub trades: u64,
    pub position: Option<String>,
    pub last_trade_ms: Option<i64>,
    pub last_digest: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Problem {
    pub level: Level,
    pub message: String,
}

pub struct Board {
    status: Mutex<Status>,
    last_round: Mutex<Option<Instant>>,
    /// `/health` fails when no round has completed for this long while quoting.
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

    pub fn round_done(&self, record: RoundRecord) {
        *self.last_round.lock().unwrap() = Some(Instant::now());
        self.update(|s| {
            s.last_round_ms = Some(now_ms());
            s.rounds += 1;
            s.recent_rounds.push_front(record);
            s.recent_rounds.truncate(KEPT_ROUNDS);
        });
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
        let quoting = self.status.lock().unwrap().quoting;
        match *self.last_round.lock().unwrap() {
            None => reasons.push("no round has completed yet".to_owned()),
            Some(at) if quoting && at.elapsed() > self.stale_after => {
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
    fn health_needs_a_recent_round_while_quoting_and_no_critical_problem() {
        let board = Board::new(
            Status {
                quoting: true,
                ..Status::default()
            },
            Duration::from_secs(60),
        );
        assert!(!board.health().0);
        board.round_done(RoundRecord::default());
        assert!(board.health().0);
        assert!(board.set_problem("gas", Level::Critical, "empty".into()));
        assert!(!board.health().0);
        assert!(board.clear_problem("gas"));
        assert!(board.health().0);
        assert_eq!(board.snapshot().rounds, 1);
    }
}
