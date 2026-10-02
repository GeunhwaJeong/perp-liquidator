// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! What an engine abort means for the liquidator: wait and try again, wait for the indexer,
//! or tell a human.

use std::time::Duration;

use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    /// The engine found the position above its maintenance requirement: the indexer's copy or
    /// the prices here were behind, or someone else liquidated it first.
    AboveMaintenance,
    /// The resting orders passed in were not all of the position's orders: the indexer has not
    /// caught up with its latest ones.
    OrdersOutOfDate,
    /// The liquidator's account cannot cover the margin of the position it would take over.
    LiquidatorUnderfunded,
    /// The liquidator's own position in the market has negative equity.
    LiquidatorInBadDebt,
    /// The bad debt is beyond what the insurance fund and the socialization limits take: only
    /// auto-deleveraging can close the position.
    BadDebtBeyondLimits,
    MarketPaused,
    MarketClosed,
    /// An oracle price is older than the market accepts.
    StaleOracle,
    /// The index price moved too far from its TWAP for the market to price anything.
    OracleDivergence,
    /// A reduce-only order found nothing to reduce: the liquidation took over no size.
    NothingToReduce,
    /// A session that would do nothing: an unwinding order found nothing to trade with.
    NoFill,
    OpenInterestCap,
    /// The package called is not the market's current version: the engine was upgraded.
    PackageVersion,
    /// The ADL plan was refused: the positions it relied on changed.
    AdlRejected,
    Other,
}

impl Reason {
    pub fn label(&self) -> &'static str {
        match self {
            Reason::AboveMaintenance => "above_maintenance",
            Reason::OrdersOutOfDate => "orders_out_of_date",
            Reason::LiquidatorUnderfunded => "liquidator_underfunded",
            Reason::LiquidatorInBadDebt => "liquidator_in_bad_debt",
            Reason::BadDebtBeyondLimits => "bad_debt_beyond_limits",
            Reason::MarketPaused => "market_paused",
            Reason::MarketClosed => "market_closed",
            Reason::StaleOracle => "stale_oracle",
            Reason::OracleDivergence => "oracle_divergence",
            Reason::NothingToReduce => "nothing_to_reduce",
            Reason::NoFill => "no_fill",
            Reason::OpenInterestCap => "open_interest_cap",
            Reason::PackageVersion => "package_version",
            Reason::AdlRejected => "adl_rejected",
            Reason::Other => "other",
        }
    }

    /// How long to leave the position alone after this. Short where the next checkpoint may
    /// change the answer, long where a human has to act first.
    pub fn retry_after(&self) -> Duration {
        Duration::from_secs(match self {
            Reason::AboveMaintenance | Reason::OrdersOutOfDate | Reason::NoFill => 2,
            Reason::NothingToReduce | Reason::StaleOracle => 3,
            Reason::OracleDivergence | Reason::AdlRejected | Reason::Other => 10,
            Reason::OpenInterestCap => 30,
            Reason::LiquidatorUnderfunded
            | Reason::LiquidatorInBadDebt
            | Reason::BadDebtBeyondLimits
            | Reason::MarketPaused
            | Reason::MarketClosed => 60,
            Reason::PackageVersion => 300,
        })
    }

    /// Whether this needs an operator rather than time.
    pub fn needs_operator(&self) -> bool {
        matches!(
            self,
            Reason::LiquidatorUnderfunded
                | Reason::LiquidatorInBadDebt
                | Reason::BadDebtBeyondLimits
                | Reason::PackageVersion
        )
    }
}

/// Reads an abort raised in `module` with `code`.
pub fn classify(module: &str, code: u64) -> Reason {
    match (module, code) {
        ("clearing_house", 38) => Reason::AboveMaintenance,
        ("clearing_house", 4) => Reason::OrdersOutOfDate,
        ("clearing_house", 30) => Reason::LiquidatorUnderfunded,
        ("clearing_house", 39) => Reason::LiquidatorInBadDebt,
        ("clearing_house", 21 | 22 | 24) => Reason::BadDebtBeyondLimits,
        ("clearing_house", 32) => Reason::MarketPaused,
        ("clearing_house", 35) => Reason::MarketClosed,
        ("clearing_house", 9) => Reason::NothingToReduce,
        ("clearing_house", 11 | 45) => Reason::NoFill,
        ("clearing_house", 15 | 16) => Reason::OpenInterestCap,
        ("clearing_house", 10) | ("registry", 5000) => Reason::PackageVersion,
        // Registry 5007: the capability (ADL) is not authorized on the registry.
        ("registry", 5007) => Reason::AdlRejected,
        ("market", 1000) => Reason::StaleOracle,
        ("market", 1003) => Reason::OracleDivergence,
        ("adl", 6000..=6004) => Reason::AdlRejected,
        _ => Reason::Other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_codes_map_to_what_they_mean() {
        assert_eq!(classify("clearing_house", 38), Reason::AboveMaintenance);
        assert_eq!(classify("clearing_house", 4), Reason::OrdersOutOfDate);
        assert_eq!(classify("clearing_house", 22), Reason::BadDebtBeyondLimits);
        assert_eq!(classify("market", 1000), Reason::StaleOracle);
        assert_eq!(classify("adl", 6002), Reason::AdlRejected);
        // The same number means something else in another module.
        assert_eq!(classify("market", 38), Reason::Other);
        assert_eq!(classify("position", 4), Reason::Other);
    }

    #[test]
    fn operator_problems_wait_longer_than_timing_ones() {
        assert!(Reason::LiquidatorUnderfunded.needs_operator());
        assert!(!Reason::AboveMaintenance.needs_operator());
        assert!(
            Reason::LiquidatorUnderfunded.retry_after() > Reason::AboveMaintenance.retry_after()
        );
    }
}
