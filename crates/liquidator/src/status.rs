// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! What the liquidator is doing, for `/status` and `/health`.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;

use crate::alerts::Level;
use crate::metrics::Metrics;

const RECENT_ACTIONS: usize = 100;

#[derive(Clone, Debug, Default, Serialize)]
pub struct Status {
    pub mode: String,
    pub address: String,
    pub account_id: i64,
    pub started_at_ms: i64,
    /// When the last round finished, and whether it did without error.
    pub last_round_ms: Option<i64>,
    pub last_round_ok: bool,
    pub last_error: Option<String>,
    pub indexer_checkpoint: Option<i64>,
    pub indexer_lag_ms: Option<i64>,
    pub gas_balance_haneul: Option<f64>,
    pub account_collateral: Option<f64>,
    pub markets: Vec<MarketStatus>,
    /// The least healthy positions.
    pub at_risk: Vec<PositionStatus>,
    pub recent: VecDeque<Action>,
    /// Conditions that need an operator, by key. Set while they last.
    pub problems: BTreeMap<String, Problem>,
}

#[derive(Clone, Debug, Serialize)]
pub struct MarketStatus {
    pub ticker: String,
    pub market: String,
    pub tradable: bool,
    pub mark_price: String,
    pub index_price: String,
    pub oracle_age_ms: Option<i64>,
    pub positions: usize,
    pub liquidatable: usize,
    pub inventory_base: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct PositionStatus {
    pub ticker: String,
    pub account_id: i64,
    pub base: String,
    pub margin: String,
    pub maintenance: String,
    pub health: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Action {
    pub at_ms: i64,
    pub kind: String,
    pub ticker: String,
    pub account_id: i64,
    pub outcome: String,
    pub digest: Option<String>,
    pub detail: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Problem {
    pub level: Level,
    pub message: String,
    pub since_ms: i64,
}

pub struct Board {
    status: RwLock<Status>,
    /// `/health` fails when no round has finished for this long.
    stale_after: Duration,
    max_lag_ms: i64,
}

impl Board {
    pub fn new(status: Status, stale_after: Duration, max_lag_ms: i64) -> Self {
        Self {
            status: RwLock::new(status),
            stale_after,
            max_lag_ms,
        }
    }

    pub fn update(&self, f: impl FnOnce(&mut Status)) {
        f(&mut self.status.write().unwrap());
    }

    pub fn snapshot(&self) -> Status {
        self.status.read().unwrap().clone()
    }

    pub fn record(&self, action: Action) {
        self.update(|s| {
            s.recent.push_front(action);
            s.recent.truncate(RECENT_ACTIONS);
        });
    }

    /// Notes a condition that needs an operator; returns whether it is new.
    pub fn set_problem(&self, key: &str, level: Level, message: String) -> bool {
        let mut fresh = false;
        self.update(|s| {
            fresh = !s.problems.contains_key(key);
            let since_ms = s.problems.get(key).map_or_else(now_ms, |p| p.since_ms);
            s.problems.insert(
                key.to_owned(),
                Problem {
                    level,
                    message,
                    since_ms,
                },
            );
        });
        fresh
    }

    pub fn clear_problem(&self, key: &str) -> bool {
        let mut cleared = false;
        self.update(|s| cleared = s.problems.remove(key).is_some());
        cleared
    }

    /// Healthy while rounds keep finishing, the indexer keeps up and nothing critical is open.
    pub fn health(&self) -> (bool, Vec<String>) {
        let s = self.status.read().unwrap();
        let mut reasons = Vec::new();
        match s.last_round_ms {
            None => reasons.push("no round has finished yet".to_owned()),
            Some(at) if now_ms() - at > self.stale_after.as_millis() as i64 => {
                reasons.push(format!("no round for {} ms", now_ms() - at))
            }
            _ => {}
        }
        if !s.last_round_ok {
            reasons.push(format!(
                "last round failed: {}",
                s.last_error.as_deref().unwrap_or("?")
            ));
        }
        if let Some(lag) = s.indexer_lag_ms
            && lag > self.max_lag_ms
        {
            reasons.push(format!("indexer {lag} ms behind"));
        }
        for (key, problem) in &s.problems {
            if problem.level == Level::Critical {
                reasons.push(format!("{key}: {}", problem.message));
            }
        }
        (reasons.is_empty(), reasons)
    }
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

#[derive(Clone)]
struct AppState {
    board: Arc<Board>,
    metrics: Arc<Metrics>,
}

pub fn router(board: Arc<Board>, metrics: Arc<Metrics>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/status", get(status))
        .route("/metrics", get(metrics_text))
        .with_state(AppState { board, metrics })
}

async fn health(State(app): State<AppState>) -> impl IntoResponse {
    let (healthy, reasons) = app.board.health();
    let code = if healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        code,
        Json(serde_json::json!({ "healthy": healthy, "reasons": reasons })),
    )
}

async fn status(State(app): State<AppState>) -> impl IntoResponse {
    Json(app.board.snapshot())
}

async fn metrics_text(State(app): State<AppState>) -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        app.metrics.exposition(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn board() -> Board {
        Board::new(Status::default(), Duration::from_secs(10), 30_000)
    }

    #[test]
    fn unhealthy_until_a_round_finishes_and_while_something_critical_is_open() {
        let b = board();
        assert!(!b.health().0);
        b.update(|s| {
            s.last_round_ms = Some(now_ms());
            s.last_round_ok = true;
            s.indexer_lag_ms = Some(1_000);
        });
        assert_eq!(b.health(), (true, vec![]));

        assert!(b.set_problem("gas", Level::Warning, "low".into()));
        assert!(b.health().0, "a warning does not fail health");
        assert!(b.set_problem("underfunded", Level::Critical, "deposit".into()));
        assert!(!b.set_problem("underfunded", Level::Critical, "deposit".into()));
        assert_eq!(b.health().1, vec!["underfunded: deposit".to_owned()]);
        assert!(b.clear_problem("underfunded"));
        assert!(b.health().0);

        b.update(|s| s.indexer_lag_ms = Some(31_000));
        assert!(!b.health().0);
    }

    #[test]
    fn keeps_the_latest_actions() {
        let b = board();
        for i in 0..150 {
            b.record(Action {
                at_ms: i,
                kind: "liquidate".into(),
                ticker: "BTC-USD".into(),
                account_id: i,
                outcome: "executed".into(),
                digest: None,
                detail: None,
            });
        }
        let s = b.snapshot();
        assert_eq!(s.recent.len(), RECENT_ACTIONS);
        assert_eq!(s.recent.front().unwrap().account_id, 149);
    }
}
