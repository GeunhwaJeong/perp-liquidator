// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! What the keeper says about the tickets it cannot act on, and what it holds back.
//!
//! A ticket the keeper cannot execute stays open, and its stop never fires: nobody learns of
//! it from a gauge. Such tickets are reported once they have stayed that way for a while. The
//! report is one alert for all of them, because anyone can make a ticket that names the keeper
//! and hand in no details for it.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

/// Whether a ticket in `state` (without the `twap_` prefix) stays there until someone other than
/// the keeper acts: its details never arrived or do not read as the ticket's, it names a market
/// the keeper does not run, or the engine takes no order of its details.
pub fn is_stuck(state: &str) -> bool {
    matches!(
        state,
        "no_details" | "unreadable" | "unknown_market" | "invalid"
    )
}

/// The tickets that are stuck, since when, and which of them were reported.
#[derive(Default)]
pub struct Stuck {
    since: HashMap<String, (&'static str, Instant)>,
    reported: HashSet<String>,
}

/// The tickets stuck for long enough: all of them, oldest first, and the ones among them that
/// no earlier report named.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Overdue {
    pub all: Vec<(String, &'static str)>,
    pub new: Vec<(String, &'static str)>,
}

impl Stuck {
    /// Records what a round saw of a ticket. A ticket that leaves the stuck states, or changes
    /// from one to another, starts afresh.
    pub fn observe(&mut self, ticket_id: &str, state: &'static str, now: Instant) {
        if !is_stuck(state) {
            self.since.remove(ticket_id);
            self.reported.remove(ticket_id);
            return;
        }
        match self.since.get(ticket_id) {
            Some((before, _)) if *before == state => {}
            _ => {
                self.since.insert(ticket_id.to_owned(), (state, now));
                self.reported.remove(ticket_id);
            }
        }
    }

    /// Forgets the tickets that are no longer open.
    pub fn retain(&mut self, open: &HashSet<&str>) {
        self.since.retain(|id, _| open.contains(id.as_str()));
        self.reported.retain(|id| open.contains(id.as_str()));
    }

    /// The tickets stuck for at least `after`, marking them reported.
    pub fn overdue(&mut self, after: Duration, now: Instant) -> Overdue {
        let mut all: Vec<(&String, &'static str, Instant)> = self
            .since
            .iter()
            .filter(|(_, (_, since))| now.duration_since(*since) >= after)
            .map(|(id, (state, since))| (id, *state, *since))
            .collect();
        all.sort_by(|a, b| a.2.cmp(&b.2).then_with(|| a.0.cmp(b.0)));
        let mut overdue = Overdue::default();
        for (id, state, _) in all {
            if self.reported.insert(id.clone()) {
                overdue.new.push((id.clone(), state));
            }
            overdue.all.push((id.clone(), state));
        }
        overdue
    }
}

/// How many tickets a report names before it only counts the rest.
const NAMED: usize = 3;

/// The report of the overdue tickets, for the status board and the alert.
pub fn describe(overdue: &[(String, &'static str)], after: Duration) -> String {
    let named: Vec<String> = overdue
        .iter()
        .take(NAMED)
        .map(|(id, state)| format!("{id} ({state})"))
        .collect();
    let more = match overdue.len().saturating_sub(NAMED) {
        0 => String::new(),
        n => format!(" and {n} more"),
    };
    format!(
        "{} open ticket{} could not be executed for over {} s and will not fire as {}: {}{more}",
        overdue.len(),
        if overdue.len() == 1 { "" } else { "s" },
        after.as_secs(),
        if overdue.len() == 1 {
            "it is"
        } else {
            "they are"
        },
        named.join(", "),
    )
}

/// How far the indexer's watermark is behind the keeper's clock, in milliseconds.
pub fn lag_ms(now_ms: i64, watermark_ms: i64) -> i64 {
    (now_ms - watermark_ms).max(0)
}

/// Why the keeper runs no chunk of a TWAP ticket made with `gas` (in the smallest unit of
/// HANEUL) for `chunks` chunks, if it does not. The engine takes a TWAP ticket with no gas at
/// all and with any number of chunks, and the keeper pays each chunk's transaction: a ticket
/// must bring at least `min_gas` and plan at most `max_chunks`. A limit of 0 is no limit.
pub fn twap_refusal(gas: u64, chunks: u64, min_gas: u64, max_chunks: u64) -> Option<&'static str> {
    if gas < min_gas {
        return Some("underfunded");
    }
    if max_chunks > 0 && chunks > max_chunks {
        return Some("too_many_chunks");
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    const AFTER: Duration = Duration::from_secs(120);

    fn names<'a>(list: &'a [(String, &'static str)]) -> Vec<(&'a str, &'static str)> {
        list.iter()
            .map(|(id, state)| (id.as_str(), *state))
            .collect()
    }

    #[test]
    fn only_the_states_a_ticket_does_not_leave_by_itself_are_stuck() {
        for state in ["no_details", "unreadable", "unknown_market", "invalid"] {
            assert!(is_stuck(state), "{state}");
        }
        for state in [
            "waiting",
            "triggered",
            "no_position",
            "no_price",
            "market_paused",
            "backoff",
            "expired",
            "scheduled",
            "underfunded",
        ] {
            assert!(!is_stuck(state), "{state}");
        }
    }

    #[test]
    fn a_ticket_is_overdue_once_it_was_stuck_for_long_enough_and_is_new_only_once() {
        let start = Instant::now();
        let mut stuck = Stuck::default();
        stuck.observe("0xa", "no_details", start);
        assert_eq!(stuck.overdue(AFTER, start + AFTER / 2), Overdue::default());

        let first = stuck.overdue(AFTER, start + AFTER);
        assert_eq!(names(&first.all), vec![("0xa", "no_details")]);
        assert_eq!(names(&first.new), vec![("0xa", "no_details")]);

        stuck.observe("0xa", "no_details", start + AFTER * 2);
        let later = stuck.overdue(AFTER, start + AFTER * 2);
        assert_eq!(names(&later.all), vec![("0xa", "no_details")]);
        assert!(later.new.is_empty());
    }

    #[test]
    fn a_ticket_that_gets_its_details_or_changes_state_starts_afresh() {
        let start = Instant::now();
        let mut stuck = Stuck::default();
        stuck.observe("0xa", "no_details", start);
        stuck.observe("0xb", "no_details", start);
        assert_eq!(stuck.overdue(AFTER, start + AFTER).new.len(), 2);

        // 0xa got its details; 0xb's details arrived and do not read as the ticket's.
        stuck.observe("0xa", "waiting", start + AFTER);
        stuck.observe("0xb", "unreadable", start + AFTER);
        assert_eq!(stuck.overdue(AFTER, start + AFTER), Overdue::default());

        let again = stuck.overdue(AFTER, start + AFTER * 2);
        assert_eq!(names(&again.all), vec![("0xb", "unreadable")]);
        assert_eq!(names(&again.new), vec![("0xb", "unreadable")]);
    }

    #[test]
    fn a_ticket_that_is_no_longer_open_is_forgotten() {
        let start = Instant::now();
        let mut stuck = Stuck::default();
        stuck.observe("0xa", "no_details", start);
        stuck.observe("0xb", "invalid", start);
        stuck.retain(&HashSet::from(["0xb"]));
        assert_eq!(
            names(&stuck.overdue(AFTER, start + AFTER).all),
            vec![("0xb", "invalid")]
        );
    }

    #[test]
    fn the_report_names_the_oldest_three_and_counts_the_rest() {
        let start = Instant::now();
        let mut stuck = Stuck::default();
        for (i, id) in ["0xd", "0xc", "0xb", "0xa", "0xe"].iter().enumerate() {
            stuck.observe(id, "no_details", start + Duration::from_secs(i as u64));
        }
        let overdue = stuck.overdue(AFTER, start + AFTER * 2);
        assert_eq!(
            describe(&overdue.all, AFTER),
            "5 open tickets could not be executed for over 120 s and will not fire as they are: \
             0xd (no_details), 0xc (no_details), 0xb (no_details) and 2 more"
        );
        assert_eq!(
            describe(&overdue.all[..1], AFTER),
            "1 open ticket could not be executed for over 120 s and will not fire as it is: \
             0xd (no_details)"
        );
    }

    #[test]
    fn the_lag_is_never_negative() {
        assert_eq!(lag_ms(10_000, 4_000), 6_000);
        assert_eq!(lag_ms(4_000, 10_000), 0);
    }

    #[test]
    fn a_twap_ticket_brings_its_gas_and_plans_no_more_chunks_than_allowed() {
        assert_eq!(twap_refusal(1_000_000, 120, 1_000_000, 120), None);
        assert_eq!(
            twap_refusal(999_999, 2, 1_000_000, 120),
            Some("underfunded")
        );
        assert_eq!(twap_refusal(0, 2, 1_000_000, 120), Some("underfunded"));
        assert_eq!(
            twap_refusal(1_000_000, 121, 1_000_000, 120),
            Some("too_many_chunks")
        );
        // 0 switches a limit off.
        assert_eq!(twap_refusal(0, 100_000, 0, 0), None);
    }
}
