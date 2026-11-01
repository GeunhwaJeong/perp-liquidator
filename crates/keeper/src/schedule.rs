// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! What a TWAP ticket needs next, decided the way `perpetuals_orders::twap_orders` decides what
//! it accepts, from the ticket's progress as the indexer last saw it.
//!
//! The first chunk runs as soon as the ticket is seen, every later one once the execution gap has
//! passed since the last attempt. The keeper always asks for the target chunk: the engine itself
//! adds what earlier chunks scheduled but did not fill, and merges a small tail into the last
//! chunk. A ticket whose whole size is processed is finalized. A ticket the engine no longer runs
//! is cancelled, which releases the collateral it no longer needs and hands the keeper the gas it
//! still holds: past its expiry, past its first-run deadline without a first chunk, or spoiled,
//! when nothing filled within its retry window. A ticket whose first chunk never ran cannot spoil;
//! it is left to its owner.

use perp_bot_common::tickets::TwapDetails;

/// How long past a deadline the keeper waits before it cancels, so that the chain's clock, which
/// can run behind the keeper's, has passed the deadline as well.
pub const GRACE_MS: u64 = 10_000;

/// A TWAP ticket's progress as `ProcessedTWAPOrderTicket` last reported it; all zero before the
/// first attempt.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Progress {
    pub processed: u64,
    pub scheduled: u64,
    pub last_attempt_ms: u64,
    pub retry_anchor_ms: u64,
}

impl Progress {
    pub fn has_attempts(&self) -> bool {
        self.last_attempt_ms != 0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Plan {
    /// Run a chunk now, asking for `amount`.
    Run { amount: u64 },
    /// The next chunk is due at `at_ms`.
    Scheduled { at_ms: u64 },
    /// The whole size is processed: finalize.
    Complete,
    /// Past a deadline by the keeper's clock but within `GRACE_MS`: neither run nor cancelled.
    Lapsing,
    /// Past `expire_timestamp`: cancel.
    Expired,
    /// No first chunk before `first_run_expire_timestamp`: cancel.
    FirstRunExpired,
    /// Nothing filled within the retry window: cancel.
    Spoiled,
    /// The engine takes no chunk of these details on this market: the size is not in lots or has
    /// fewer lots than chunks, or a chunk is more than one execution may take.
    Invalid,
}

impl Plan {
    pub fn state(&self) -> &'static str {
        match self {
            Self::Run { .. } => "due",
            Self::Scheduled { .. } => "scheduled",
            Self::Complete => "complete",
            Self::Lapsing => "lapsing",
            Self::Expired => "expired",
            Self::FirstRunExpired => "first_run_expired",
            Self::Spoiled => "spoiled",
            Self::Invalid => "invalid",
        }
    }

    /// Whether the plan ends with a cancellation.
    pub fn cancels(&self) -> bool {
        matches!(self, Self::Expired | Self::FirstRunExpired | Self::Spoiled)
    }
}

/// `twap_orders::target_chunk_amount`: the size split into chunks, rounded down to the lot size.
pub fn target_chunk(details: &TwapDetails, lot_size: u64) -> u64 {
    details.size / lot_size / details.chunks_amount * lot_size
}

/// What `twap_orders::execute` puts in the order when asked for `amount`, and the most it
/// accepts (`ETwapOrderAmountUncertaintyViolated` beyond): the newly scheduled amount, or the
/// whole rest when a small tail merges into the last chunk, plus what earlier chunks scheduled
/// but did not fill, as far as the most one execution may take allows.
pub fn execution_amount(
    details: &TwapDetails,
    progress: &Progress,
    amount: u64,
    lot_size: u64,
) -> (u64, u64) {
    let chunk = target_chunk(details, lot_size);
    let unfilled = progress.scheduled.saturating_sub(progress.processed);
    let unscheduled = details.size.saturating_sub(progress.scheduled);
    let threshold = chunk as u128 * details.small_tail_merge_threshold_bps as u128;
    let tail = unscheduled % chunk;
    let merge_tail = unscheduled / chunk == 1 && tail > 0 && (tail as u128) * 10_000 < threshold;
    let amount = if merge_tail {
        unscheduled
    } else {
        amount.min(unscheduled)
    };
    let max_chunk = {
        let bps = details.max_one_execution_amount_bps.min(10_000);
        let max = (details.size as u128 * bps as u128 / 10_000) as u64;
        max - max % lot_size
    };
    let max_execution = if merge_tail {
        max_chunk.max(amount)
    } else {
        max_chunk
    };
    let retry = if unfilled > 0 && amount < max_execution {
        unfilled.min(max_execution - amount)
    } else {
        0
    };
    (amount + retry, max_execution)
}

pub fn plan(details: &TwapDetails, progress: &Progress, lot_size: u64, now_ms: u64) -> Plan {
    // `finalize` checks neither deadline: a complete ticket is finalized even when expired.
    if progress.processed >= details.size {
        return Plan::Complete;
    }
    if lot_size == 0
        || !details.size.is_multiple_of(lot_size)
        || details.size / lot_size < details.chunks_amount
    {
        return Plan::Invalid;
    }
    // Whether `now` is past the last moment the engine accepts a chunk, grace included; within
    // the grace the ticket is lapsing.
    let mut lapsing = false;
    let mut past = |accepted_until: u64| {
        if now_ms > accepted_until.saturating_add(GRACE_MS) {
            return true;
        }
        lapsing |= now_ms > accepted_until;
        false
    };
    // The engine refuses a chunk once `now > expire_timestamp`...
    if let Some(expire) = details.expire_timestamp_ms
        && past(expire)
    {
        return Plan::Expired;
    }
    let mut due = now_ms;
    if !progress.has_attempts() {
        // ... a first chunk once `now >= first_run_expire_timestamp`...
        if let Some(first) = details.first_run_expire_timestamp_ms
            && past(first.saturating_sub(1))
        {
            return Plan::FirstRunExpired;
        }
    } else {
        // ... and any later chunk once `now - retry_anchor >= retry + gap + uncertainty`.
        let window = details
            .time_for_retry_ms
            .saturating_add(details.execution_gap_ms)
            .saturating_add(details.execution_time_uncertainty_ms);
        let spoils_at = progress.retry_anchor_ms.saturating_add(window);
        if past(spoils_at.saturating_sub(1)) {
            return Plan::Spoiled;
        }
        due = progress
            .last_attempt_ms
            .saturating_add(details.execution_gap_ms);
    }
    if lapsing {
        return Plan::Lapsing;
    }
    let chunk = target_chunk(details, lot_size);
    let (execution, max) = execution_amount(details, progress, chunk, lot_size);
    if execution == 0 || execution > max {
        return Plan::Invalid;
    }
    if now_ms < due {
        return Plan::Scheduled { at_ms: due };
    }
    Plan::Run { amount: chunk }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOT: u64 = 1_000_000;
    const T0: u64 = 1_800_000_000_000;

    /// 1 BTC in four chunks a minute apart, as in `perp_bot_common::tickets`.
    fn four_chunks() -> TwapDetails {
        TwapDetails {
            first_run_expire_timestamp_ms: None,
            expire_timestamp_ms: None,
            execution_gap_ms: 60_000,
            execution_time_uncertainty_ms: 10_000,
            chunks_amount: 4,
            small_tail_merge_threshold_bps: 0,
            time_for_retry_ms: 30_000,
            amount_uncertainty_bps: 0,
            max_one_execution_amount_bps: 5_000,
            side: false,
            size: 1_000_000_000,
            max_slippage_bps: 100,
            reduce_only: false,
            integrator_info: None,
            salt: vec![0],
        }
    }

    fn after(chunks: u64, filled: u64, at: u64) -> Progress {
        Progress {
            processed: filled * 250_000_000,
            scheduled: chunks * 250_000_000,
            last_attempt_ms: at,
            retry_anchor_ms: at,
        }
    }

    #[test]
    fn the_first_chunk_runs_at_once_and_the_next_after_the_gap() {
        let d = four_chunks();
        assert_eq!(
            plan(&d, &Progress::default(), LOT, T0),
            Plan::Run {
                amount: 250_000_000
            }
        );
        let one = after(1, 1, T0);
        assert_eq!(
            plan(&d, &one, LOT, T0 + 59_999),
            Plan::Scheduled { at_ms: T0 + 60_000 }
        );
        assert_eq!(
            plan(&d, &one, LOT, T0 + 60_000),
            Plan::Run {
                amount: 250_000_000
            }
        );
        assert_eq!(plan(&d, &after(4, 4, T0), LOT, T0), Plan::Complete);
    }

    #[test]
    fn what_did_not_fill_is_added_to_the_next_chunk_up_to_one_executions_most() {
        let mut d = four_chunks();
        // Two chunks scheduled, one filled: the next asks for a chunk and gets two.
        let progress = after(2, 1, T0);
        assert_eq!(
            execution_amount(&d, &progress, 250_000_000, LOT),
            (500_000_000, 500_000_000)
        );
        d.max_one_execution_amount_bps = 2_500;
        assert_eq!(
            execution_amount(&d, &progress, 250_000_000, LOT),
            (250_000_000, 250_000_000)
        );
        // All four scheduled, three filled: the last run only retries.
        assert_eq!(
            execution_amount(&d, &after(4, 3, T0), 250_000_000, LOT),
            (250_000_000, 250_000_000)
        );
    }

    #[test]
    fn a_small_tail_merges_into_the_last_chunk() {
        // Ten lots in three chunks of three: the last chunk takes the tail of one when it is
        // under half a chunk, else the tail runs on its own.
        let mut d = four_chunks();
        d.size = 10 * LOT;
        d.chunks_amount = 3;
        d.max_one_execution_amount_bps = 3_000;
        let two = Progress {
            processed: 6 * LOT,
            scheduled: 6 * LOT,
            last_attempt_ms: T0,
            retry_anchor_ms: T0,
        };
        assert_eq!(execution_amount(&d, &two, 3 * LOT, LOT), (3 * LOT, 3 * LOT));
        d.small_tail_merge_threshold_bps = 5_000;
        assert_eq!(execution_amount(&d, &two, 3 * LOT, LOT), (4 * LOT, 4 * LOT));
        let three = Progress {
            processed: 9 * LOT,
            scheduled: 9 * LOT,
            ..two
        };
        d.small_tail_merge_threshold_bps = 0;
        assert_eq!(execution_amount(&d, &three, 3 * LOT, LOT), (LOT, 3 * LOT));
        assert_eq!(
            plan(&d, &three, LOT, T0 + 60_000),
            Plan::Run { amount: 3 * LOT }
        );
    }

    #[test]
    fn deadlines_cancel_only_once_the_chain_is_past_them_too() {
        let mut d = four_chunks();
        d.expire_timestamp_ms = Some(T0 + 80_000);
        let one = after(1, 1, T0);
        assert_eq!(
            plan(&d, &one, LOT, T0 + 80_000),
            Plan::Run {
                amount: 250_000_000
            }
        );
        assert_eq!(plan(&d, &one, LOT, T0 + 80_001), Plan::Lapsing);
        assert_eq!(
            plan(&d, &one, LOT, T0 + 80_000 + GRACE_MS + 1),
            Plan::Expired
        );
        // A complete ticket is finalized, expired or not.
        assert_eq!(
            plan(&d, &after(4, 4, T0), LOT, T0 + 900_000),
            Plan::Complete
        );

        let mut d = four_chunks();
        d.first_run_expire_timestamp_ms = Some(T0 + 5_000);
        assert_eq!(
            plan(&d, &Progress::default(), LOT, T0 + 4_999),
            Plan::Run {
                amount: 250_000_000
            }
        );
        assert_eq!(
            plan(&d, &Progress::default(), LOT, T0 + 5_000),
            Plan::Lapsing
        );
        assert_eq!(
            plan(&d, &Progress::default(), LOT, T0 + 5_000 + GRACE_MS),
            Plan::FirstRunExpired
        );
        // The first-run deadline binds the first chunk only.
        assert_eq!(
            plan(&d, &one, LOT, T0 + 60_000),
            Plan::Run {
                amount: 250_000_000
            }
        );
    }

    #[test]
    fn nothing_filled_within_the_retry_window_spoils_the_ticket() {
        let d = four_chunks();
        // Last fill at T0, an unfilled attempt since: the window is retry + gap + uncertainty.
        let progress = Progress {
            last_attempt_ms: T0 + 60_000,
            ..after(2, 1, T0)
        };
        assert_eq!(
            plan(&d, &progress, LOT, T0 + 99_999),
            Plan::Scheduled {
                at_ms: T0 + 120_000
            }
        );
        assert_eq!(plan(&d, &progress, LOT, T0 + 100_000), Plan::Lapsing);
        assert_eq!(
            plan(&d, &progress, LOT, T0 + 100_000 + GRACE_MS),
            Plan::Spoiled
        );
        // Never attempted, never spoiled.
        assert_eq!(
            plan(&d, &Progress::default(), LOT, T0 + 10_000_000),
            Plan::Run {
                amount: 250_000_000
            }
        );
    }

    #[test]
    fn details_the_engine_never_runs_are_invalid() {
        let mut d = four_chunks();
        d.size = 1_000_500_000;
        assert_eq!(plan(&d, &Progress::default(), LOT, T0), Plan::Invalid);
        let mut d = four_chunks();
        d.size = 3 * LOT;
        assert_eq!(plan(&d, &Progress::default(), LOT, T0), Plan::Invalid);
        let mut d = four_chunks();
        d.max_one_execution_amount_bps = 2_000;
        assert_eq!(plan(&d, &Progress::default(), LOT, T0), Plan::Invalid);
        assert!(Plan::Spoiled.cancels() && !Plan::Lapsing.cancels());
    }
}
