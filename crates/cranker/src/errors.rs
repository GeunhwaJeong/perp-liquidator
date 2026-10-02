// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Failure tracking per market and kind of failure. A market that keeps failing the same way is
//! left alone for a while instead of being retried, and paid for, every round; a success clears
//! its record.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

/// The latest distinct messages kept per record.
const KEPT_MESSAGES: usize = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Kind {
    /// The clearing house could not be read.
    Read,
    /// The full node refused to build the crank: it would abort, or failed otherwise.
    Build,
    /// The crank was sent and did not land, or landed and failed.
    Execute,
}

impl Kind {
    pub fn label(&self) -> &'static str {
        match self {
            Kind::Read => "read",
            Kind::Build => "build",
            Kind::Execute => "execute",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Record {
    /// Failures in a row.
    pub count: u32,
    pub last_at: Instant,
    pub messages: VecDeque<String>,
}

pub struct Tracker {
    skip_after: u32,
    skip_for: Duration,
    records: HashMap<(String, Kind), Record>,
}

impl Tracker {
    /// After `skip_after` failures in a row a market is skipped for `skip_for`.
    pub fn new(skip_after: u32, skip_for: Duration) -> Self {
        Self {
            skip_after: skip_after.max(1),
            skip_for,
            records: HashMap::new(),
        }
    }

    /// Records a failure and returns the failures in a row so far.
    pub fn failed(&mut self, market: &str, kind: Kind, message: &str, now: Instant) -> u32 {
        let record = self
            .records
            .entry((market.to_owned(), kind))
            .or_insert_with(|| Record {
                count: 0,
                last_at: now,
                messages: VecDeque::new(),
            });
        record.count += 1;
        record.last_at = now;
        if !record.messages.iter().any(|m| m == message) {
            record.messages.push_back(message.to_owned());
            if record.messages.len() > KEPT_MESSAGES {
                record.messages.pop_front();
            }
        }
        record.count
    }

    /// A crank landed: the market's failures are forgotten.
    pub fn succeeded(&mut self, market: &str) {
        self.records.retain(|(m, _), _| m != market);
    }

    /// Until when the market is skipped, if it is: the kind whose failures put it there.
    pub fn skipped(&self, market: &str, now: Instant) -> Option<(Kind, Duration)> {
        self.records
            .iter()
            .filter(|((m, _), r)| m == market && r.count >= self.skip_after)
            .filter_map(|((_, kind), r)| {
                let until = r.last_at + self.skip_for;
                (until > now).then(|| (*kind, until - now))
            })
            .max_by_key(|(_, left)| *left)
    }

    pub fn record(&self, market: &str, kind: Kind) -> Option<&Record> {
        self.records.get(&(market.to_owned(), kind))
    }

    /// Markets failing right now, with how many times in a row and the latest message.
    pub fn failing(&self) -> Vec<(String, Kind, u32, String)> {
        let mut out: Vec<_> = self
            .records
            .iter()
            .map(|((market, kind), r)| {
                (
                    market.clone(),
                    *kind,
                    r.count,
                    r.messages.back().cloned().unwrap_or_default(),
                )
            })
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.label().cmp(b.1.label())));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skips_a_market_after_repeated_failures_for_a_while() {
        let mut t = Tracker::new(2, Duration::from_secs(60));
        let t0 = Instant::now();
        assert_eq!(t.failed("BTC-USD", Kind::Build, "stale price", t0), 1);
        assert!(t.skipped("BTC-USD", t0).is_none());
        assert_eq!(t.failed("BTC-USD", Kind::Build, "stale price", t0), 2);
        let (kind, left) = t.skipped("BTC-USD", t0 + Duration::from_secs(10)).unwrap();
        assert_eq!(kind, Kind::Build);
        assert_eq!(left, Duration::from_secs(50));
        assert!(t.skipped("BTC-USD", t0 + Duration::from_secs(60)).is_none());
        // Another market is not affected.
        assert!(t.skipped("ETH-USD", t0).is_none());
    }

    #[test]
    fn keeps_the_latest_distinct_messages() {
        let mut t = Tracker::new(2, Duration::from_secs(60));
        let now = Instant::now();
        for m in ["a", "b", "a", "c", "d"] {
            t.failed("BTC-USD", Kind::Execute, m, now);
        }
        let r = t.record("BTC-USD", Kind::Execute).unwrap();
        assert_eq!(r.count, 5);
        assert_eq!(
            r.messages,
            VecDeque::from(vec!["b".into(), "c".into(), "d".into()])
        );
    }

    #[test]
    fn a_success_clears_the_market() {
        let mut t = Tracker::new(1, Duration::from_secs(60));
        let now = Instant::now();
        t.failed("BTC-USD", Kind::Read, "unreachable", now);
        t.failed("ETH-USD", Kind::Read, "unreachable", now);
        assert!(t.skipped("BTC-USD", now).is_some());
        t.succeeded("BTC-USD");
        assert!(t.skipped("BTC-USD", now).is_none());
        assert_eq!(t.failing().len(), 1);
    }
}
