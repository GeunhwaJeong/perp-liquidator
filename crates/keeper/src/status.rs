// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! What `/status` shows and `/health` judges.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use perp_bot_common::alerts::Level;
use serde::Serialize;

use crate::now_ms;

#[derive(Clone, Debug, Serialize)]
pub struct LastExecution {
    pub ticket_id: String,
    pub kind: &'static str,
    pub digest: String,
    pub at_ms: i64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Status {
    /// `details-only`, `live` or `dry-run`.
    pub mode: String,
    pub address: Option<String>,
    pub started_at_ms: i64,
    pub last_round_ms: Option<i64>,
    pub executions: u64,
    /// Open tickets that name the keeper, by state, as of the last round.
    pub tickets: BTreeMap<String, u64>,
    pub last_execution: Option<LastExecution>,
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
    /// `/health` fails when no round has completed for this long; None when the keeper
    /// executes nothing and so runs no rounds.
    stale_after: Option<Duration>,
}

impl Board {
    pub fn new(status: Status, stale_after: Option<Duration>) -> Self {
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
        if let Some(stale_after) = self.stale_after {
            match *self.last_round.lock().unwrap() {
                None => reasons.push("no round has completed yet".to_owned()),
                Some(at) if at.elapsed() > stale_after => {
                    reasons.push(format!("no round for {} s", at.elapsed().as_secs()))
                }
                Some(_) => {}
            }
        }
        for (key, problem) in &self.status.lock().unwrap().problems {
            if problem.level == Level::Critical {
                reasons.push(format!("{key}: {}", problem.message));
            }
        }
        (reasons.is_empty(), reasons)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_needs_a_recent_round_and_no_critical_problem() {
        let board = Board::new(Status::default(), Some(Duration::from_secs(60)));
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

    #[test]
    fn taking_details_in_alone_needs_no_rounds() {
        let board = Board::new(Status::default(), None);
        assert!(board.health().0);
    }
}
