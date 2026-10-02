// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! When a market needs a crank, read from its clearing house.
//!
//! Every session updates a market's funding and TWAPs when they are due, so a market that
//! trades needs no crank. A quiet one does: its premium TWAP is only sampled when something
//! touches the market, so funding computed from it drifts from the book while nobody trades,
//! and the engine catches up at most three missed funding intervals at once
//! (`market::funding_period_adjustment`): funding a market goes longer than that without is lost.
//! `clearing_house::update_funding` samples whichever TWAPs are due and settles the funding
//! that is, exactly as a session start does.

use anyhow::{Context, bail};
use serde_json::Value;

/// The engine settles at most this many missed funding intervals in one update.
pub const MAX_CAUGHT_UP_INTERVALS: u64 = 3;

/// `ClearingHouse.paused`: 0 trades, 1 is paused, 2 is closed.
pub const NOT_PAUSED: u64 = 0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Schedule {
    pub paused: u64,
    pub funding_frequency_ms: u64,
    pub funding_last_upd_ms: u64,
    pub premium_twap_frequency_ms: u64,
    pub premium_twap_last_upd_ms: u64,
    pub spread_twap_frequency_ms: u64,
    pub spread_twap_last_upd_ms: u64,
}

/// What a crank at a given time would update.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Due {
    pub funding: bool,
    pub premium_twap: bool,
    pub spread_twap: bool,
}

impl Due {
    pub fn any(&self) -> bool {
        self.funding || self.premium_twap || self.spread_twap
    }

    /// What is due, for logs and metrics: `funding`, `premium_twap`, `spread_twap`.
    pub fn labels(&self) -> Vec<&'static str> {
        [
            (self.funding, "funding"),
            (self.premium_twap, "premium_twap"),
            (self.spread_twap, "spread_twap"),
        ]
        .into_iter()
        .filter_map(|(due, label)| due.then_some(label))
        .collect()
    }
}

impl Schedule {
    /// Reads the clearing house object as the full node renders it in JSON. Integers may come
    /// as strings or as JSON numbers, which some renderings give as floats (`60000.0`).
    pub fn from_clearing_house(json: &Value) -> anyhow::Result<Self> {
        let twap = &json["market_params"]["twap_params"];
        let state = &json["market_state"];
        let schedule = Self {
            paused: integer(&json["paused"]).context("paused")?,
            funding_frequency_ms: integer(&twap["funding_frequency_ms"])
                .context("funding_frequency_ms")?,
            funding_last_upd_ms: integer(&state["funding_last_upd_ms"])
                .context("funding_last_upd_ms")?,
            premium_twap_frequency_ms: integer(&twap["premium_twap_frequency_ms"])
                .context("premium_twap_frequency_ms")?,
            premium_twap_last_upd_ms: integer(&state["premium_twap_last_upd_ms"])
                .context("premium_twap_last_upd_ms")?,
            spread_twap_frequency_ms: integer(&twap["spread_twap_frequency_ms"])
                .context("spread_twap_frequency_ms")?,
            spread_twap_last_upd_ms: integer(&state["spread_twap_last_upd_ms"])
                .context("spread_twap_last_upd_ms")?,
        };
        if schedule.funding_frequency_ms == 0 {
            bail!("the funding frequency is 0");
        }
        Ok(schedule)
    }

    pub fn trades(&self) -> bool {
        self.paused == NOT_PAUSED
    }

    /// Funding updates happen on multiples of the frequency: this is the start of the interval
    /// after the one the last update fell in (`market::next_funding_update_time`).
    pub fn next_funding_ms(&self) -> u64 {
        let last = self.funding_last_upd_ms;
        last - last % self.funding_frequency_ms + self.funding_frequency_ms
    }

    /// Funding intervals that have ended without an update.
    pub fn missed_funding_intervals(&self, now_ms: u64) -> u64 {
        let next = self.next_funding_ms();
        if now_ms < next {
            0
        } else {
            (now_ms - next) / self.funding_frequency_ms + 1
        }
    }

    /// What a crank now would update. Funding is due on the engine's own schedule. A TWAP is
    /// due when its sampling interval has passed and it has gone `twap_min_interval_ms` without
    /// a sample: a quiet market is sampled that often, never more often than it allows.
    pub fn due(&self, now_ms: u64, twap_min_interval_ms: u64) -> Due {
        let sample_after = |last: u64, frequency: u64| last + frequency.max(twap_min_interval_ms);
        Due {
            funding: now_ms >= self.next_funding_ms(),
            premium_twap: now_ms
                >= sample_after(
                    self.premium_twap_last_upd_ms,
                    self.premium_twap_frequency_ms,
                ),
            spread_twap: now_ms
                >= sample_after(self.spread_twap_last_upd_ms, self.spread_twap_frequency_ms),
        }
    }
}

fn integer(value: &Value) -> anyhow::Result<u64> {
    match value {
        Value::String(s) => s.parse().with_context(|| format!("not an integer: {s}")),
        Value::Number(n) => n
            .as_u64()
            .or_else(|| {
                n.as_f64()
                    .filter(|f| f.fract() == 0.0 && *f >= 0.0 && *f <= u64::MAX as f64)
                    .map(|f| f as u64)
            })
            .with_context(|| format!("not an unsigned integer: {n}")),
        Value::Null => bail!("missing"),
        other => bail!("not an integer: {other}"),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn schedule() -> Schedule {
        Schedule {
            paused: 0,
            funding_frequency_ms: 3_600_000,
            funding_last_upd_ms: 7_200_000,
            premium_twap_frequency_ms: 60_000,
            premium_twap_last_upd_ms: 7_200_000,
            spread_twap_frequency_ms: 5_000,
            spread_twap_last_upd_ms: 7_200_000,
        }
    }

    #[test]
    fn reads_the_clearing_house_json() {
        let ch = json!({
            "paused": 0,
            "market_params": {"twap_params": {
                "funding_frequency_ms": "60000",
                "funding_period_ms": "21600000",
                "premium_twap_frequency_ms": 1000.0,
                "premium_twap_period_ms": "60000",
                "spread_twap_frequency_ms": 1000,
                "spread_twap_period_ms": "60000"
            }},
            "market_state": {
                "funding_last_upd_ms": "1790952000000",
                "premium_twap_last_upd_ms": "1790952001000",
                "spread_twap_last_upd_ms": "1790952002000"
            }
        });
        let s = Schedule::from_clearing_house(&ch).unwrap();
        assert_eq!(s.funding_frequency_ms, 60_000);
        assert_eq!(s.premium_twap_frequency_ms, 1_000);
        assert_eq!(s.spread_twap_last_upd_ms, 1_790_952_002_000);
        assert!(s.trades());
    }

    #[test]
    fn refuses_a_clearing_house_it_cannot_read() {
        assert!(Schedule::from_clearing_house(&json!({"paused": 0})).is_err());
        let mut ch = json!({
            "paused": 1.5,
            "market_params": {"twap_params": {"funding_frequency_ms": "1", "premium_twap_frequency_ms": "1", "spread_twap_frequency_ms": "1"}},
            "market_state": {"funding_last_upd_ms": "0", "premium_twap_last_upd_ms": "0", "spread_twap_last_upd_ms": "0"}
        });
        assert!(Schedule::from_clearing_house(&ch).is_err());
        ch["paused"] = json!(0);
        ch["market_params"]["twap_params"]["funding_frequency_ms"] = json!("0");
        assert!(Schedule::from_clearing_house(&ch).is_err());
    }

    #[test]
    fn funding_is_due_on_the_next_multiple_of_the_frequency() {
        let mut s = schedule();
        // Last updated 10 minutes into an hour: due at the next full hour.
        s.funding_last_upd_ms = 7_200_000 + 600_000;
        assert_eq!(s.next_funding_ms(), 10_800_000);
        assert!(!s.due(10_799_999, 60_000).funding);
        assert!(s.due(10_800_000, 60_000).funding);
    }

    #[test]
    fn counts_the_missed_funding_intervals() {
        let s = schedule();
        assert_eq!(s.missed_funding_intervals(10_799_999), 0);
        assert_eq!(s.missed_funding_intervals(10_800_000), 1);
        assert_eq!(s.missed_funding_intervals(14_400_000), 2);
        assert_eq!(s.missed_funding_intervals(18_000_001), 3);
        assert_eq!(s.missed_funding_intervals(21_600_000), 4);
    }

    #[test]
    fn samples_a_quiet_market_at_the_minimum_interval_not_more_often() {
        let s = schedule();
        // The spread TWAP may be sampled every 5 s, but a quiet market only needs one a minute.
        let due = s.due(7_200_000 + 30_000, 60_000);
        assert!(!due.any());
        let due = s.due(7_200_000 + 60_000, 60_000);
        assert!(due.premium_twap && due.spread_twap && !due.funding);
        assert_eq!(due.labels(), vec!["premium_twap", "spread_twap"]);
        // A market that allows fewer samples than the minimum is sampled at its own pace.
        let mut slow = s;
        slow.premium_twap_frequency_ms = 120_000;
        let due = slow.due(7_200_000 + 60_000, 60_000);
        assert!(!due.premium_twap && due.spread_twap);
    }

    #[test]
    fn a_paused_or_closed_market_does_not_trade() {
        let mut s = schedule();
        s.paused = 1;
        assert!(!s.trades());
        s.paused = 2;
        assert!(!s.trades());
    }
}
