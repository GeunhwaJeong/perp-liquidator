// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! What the maker does about a round that does not go through.
//!
//! A post-only level that would match a resting order aborts the whole session, and the cancels
//! in front of it with it. Such a level is left out and the round is built again, so that the
//! rest of the ladder lands.
//!
//! A round that fails for any other reason leaves the previous quotes resting at prices the
//! maker no longer stands behind. They are cancelled in a transaction of their own, which needs
//! no session, and the round is tried again at the pace of any other round. A try that the
//! simulation refuses costs nothing, and a maker that waits out its failures is away when the
//! market needs it.
//!
//! A transaction that was sent and got no answer may have run. The maker keeps what it knew to
//! rest, asks the full node for the transaction at every poll, and takes over what it posted
//! once the node has it.

use std::time::{Duration, Instant};

use perp_bot_common::chain::Abort;

/// `clearing_house::EPostOnlyOrderWouldMatch`.
pub const POST_ONLY_WOULD_MATCH: u64 = 47;

/// The level a refused round leaves out: the one whose order the abort names, when the abort is
/// the engine's refusal of a post-only order that would match. `placed` holds the command index
/// of each level's order, in the order of the levels.
pub fn crossing_level(abort: &Abort, placed: &[u64]) -> Option<usize> {
    if abort.module != "clearing_house" || abort.code != POST_ONLY_WOULD_MATCH {
        return None;
    }
    let command = abort.command?;
    placed.iter().position(|placed| *placed == command)
}

/// Whether a round or a pull may be tried now: at most one per `interval`, whether the last one
/// went through or not.
pub fn due(last_attempt: Option<Instant>, interval: Duration, now: Instant) -> bool {
    last_attempt.is_none_or(|at| now.saturating_duration_since(at) >= interval)
}

/// What to do about a transaction that was sent and got no answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// It ran: what it posted rests, and what it cancelled is gone.
    TakeOver,
    /// It ran and failed, so that nothing changed; or the full node has not seen it for as long
    /// as its quotes would have lived, so that nothing of it can be matched any more.
    Forget,
    /// The full node does not have it yet: ask again at the next poll.
    AskAgain,
}

/// `found` is how the transaction ended when the full node has it. `lifetime` is how long a
/// quote lives after it is posted.
pub fn verdict(found: Option<bool>, waited: Duration, lifetime: Duration) -> Verdict {
    match found {
        Some(true) => Verdict::TakeOver,
        Some(false) => Verdict::Forget,
        None if waited < lifetime => Verdict::AskAgain,
        None => Verdict::Forget,
    }
}

/// The failures in a row since the last round that went through.
#[derive(Debug, Default)]
pub struct Failures {
    count: u32,
    since: Option<Instant>,
}

impl Failures {
    /// Counts a failure and returns how long the run has lasted.
    pub fn record(&mut self, now: Instant) -> Duration {
        self.count += 1;
        let since = *self.since.get_or_insert(now);
        now.saturating_duration_since(since)
    }

    pub fn count(&self) -> u32 {
        self.count
    }

    /// Ends the run; true when there was one.
    pub fn clear(&mut self) -> bool {
        let was = self.count > 0;
        self.count = 0;
        self.since = None;
        was
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn abort(module: &str, code: u64, command: Option<u64>) -> Abort {
        Abort {
            package: "0xe2".into(),
            module: module.into(),
            function: "place_limit_order".into(),
            code,
            command,
        }
    }

    #[test]
    fn a_post_only_abort_names_the_level_to_leave_out() {
        // Commands 0..=3 relay, cancel and open the session; 4 is the flatten order.
        let placed = [5, 6, 7, 8];
        assert_eq!(
            crossing_level(&abort("clearing_house", 47, Some(5)), &placed),
            Some(0)
        );
        assert_eq!(
            crossing_level(&abort("clearing_house", 47, Some(8)), &placed),
            Some(3)
        );
    }

    #[test]
    fn other_aborts_and_other_commands_leave_no_level_out() {
        let placed = [5, 6];
        // The flatten order is not a level.
        assert_eq!(
            crossing_level(&abort("clearing_house", 47, Some(4)), &placed),
            None
        );
        // The margin does not cover the ladder.
        assert_eq!(
            crossing_level(&abort("clearing_house", 30, Some(5)), &placed),
            None
        );
        // The same code in another module.
        assert_eq!(crossing_level(&abort("market", 47, Some(5)), &placed), None);
        // The node did not say which command aborted.
        assert_eq!(
            crossing_level(&abort("clearing_house", 47, None), &placed),
            None
        );
        assert_eq!(
            crossing_level(&abort("clearing_house", 47, Some(5)), &[]),
            None
        );
    }

    #[test]
    fn a_failed_attempt_is_spaced_like_one_that_went_through() {
        let start = Instant::now();
        let interval = Duration::from_secs(3);
        assert!(due(None, interval, start));
        assert!(!due(Some(start), interval, start + Duration::from_secs(1)));
        assert!(!due(
            Some(start),
            interval,
            start + Duration::from_millis(2_999)
        ));
        assert!(due(Some(start), interval, start + interval));
    }

    #[test]
    fn a_transaction_without_an_answer_is_asked_for_until_its_quotes_would_have_expired() {
        let lifetime = Duration::from_secs(60);
        let soon = Duration::from_secs(4);
        assert_eq!(verdict(Some(true), soon, lifetime), Verdict::TakeOver);
        // Found late is still found: its quotes are cancelled by ID, expired or not.
        assert_eq!(
            verdict(Some(true), Duration::from_secs(90), lifetime),
            Verdict::TakeOver
        );
        assert_eq!(verdict(Some(false), soon, lifetime), Verdict::Forget);
        assert_eq!(verdict(None, soon, lifetime), Verdict::AskAgain);
        assert_eq!(
            verdict(None, Duration::from_secs(59), lifetime),
            Verdict::AskAgain
        );
        assert_eq!(verdict(None, lifetime, lifetime), Verdict::Forget);
    }

    #[test]
    fn a_run_of_failures_lasts_from_its_first_until_it_is_cleared() {
        let start = Instant::now();
        let mut failures = Failures::default();
        assert!(!failures.clear());
        assert_eq!(failures.record(start), Duration::ZERO);
        assert_eq!(
            failures.record(start + Duration::from_secs(3)),
            Duration::from_secs(3)
        );
        assert_eq!(
            failures.record(start + Duration::from_secs(31)),
            Duration::from_secs(31)
        );
        assert_eq!(failures.count(), 3);
        assert!(failures.clear());
        assert_eq!(failures.count(), 0);
        // The next run starts at its own first failure.
        assert_eq!(
            failures.record(start + Duration::from_secs(60)),
            Duration::ZERO
        );
        assert_eq!(failures.count(), 1);
    }
}
