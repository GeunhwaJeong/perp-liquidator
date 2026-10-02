// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Auto-deleveraging plans.
//!
//! When liquidating a position would leave more bad debt than the insurance fund and the
//! market's socialization limits can take, the engine refuses, and the position can only be
//! closed by `adl::execute_adl`: it is closed at the mark price against chosen counterparties
//! on the other side, who also absorb its negative collateral in the shares given. The engine
//! checks the plan (sizes in whole lots that add up to the position, counterparties on the
//! opposite side holding at least what is taken from them, shares summing to one) but leaves
//! the choice to whoever holds the ADL capability.
//!
//! The choice here follows the usual exchange rule: the most profitable and most leveraged
//! positions go first, ranked by unrealized return times leverage.

use bigdecimal::{BigDecimal, Signed, ToPrimitive};
use serde::Serialize;

/// One in `ifixed`: what the shares of a plan add up to.
pub const ONE: u64 = 1_000_000_000_000_000_000;

const B9: i64 = 1_000_000_000;

/// A position that could take over part of the bad-debt position.
#[derive(Clone, Debug)]
pub struct Counterparty {
    pub account_id: i64,
    pub base: BigDecimal,
    /// What it paid for its size: `|quote_notional|`.
    pub entry_notional: BigDecimal,
    pub unrealized_pnl: BigDecimal,
    pub margin: BigDecimal,
    pub notional: BigDecimal,
}

impl Counterparty {
    /// Unrealized return on the entry, times leverage. None for positions that are not ranked
    /// at all: losing ones, and ones without margin.
    fn score(&self) -> Option<BigDecimal> {
        if !self.unrealized_pnl.is_positive()
            || !self.margin.is_positive()
            || !self.entry_notional.is_positive()
        {
            return None;
        }
        Some(&self.unrealized_pnl / &self.entry_notional * (&self.notional / &self.margin))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Share {
    pub account_id: i64,
    /// Base taken over, in 9-decimal units.
    pub size: u64,
    /// Part of the negative collateral absorbed, in `ifixed` (all shares add up to [`ONE`]).
    pub weight: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Plan {
    /// The bad-debt position's size, in 9-decimal units.
    pub size: u64,
    pub shares: Vec<Share>,
}

/// Plans closing a position of `size` (9-decimal units, positive) that is long when
/// `is_long`. None when the profitable positions on the other side do not add up to it.
pub fn plan(size: u64, is_long: bool, lot_size: u64, candidates: &[Counterparty]) -> Option<Plan> {
    if size == 0 || lot_size == 0 || !size.is_multiple_of(lot_size) {
        return None;
    }
    let mut ranked: Vec<(BigDecimal, &Counterparty)> = candidates
        .iter()
        .filter(|c| {
            if is_long {
                c.base.is_negative()
            } else {
                c.base.is_positive()
            }
        })
        .filter_map(|c| c.score().map(|score| (score, c)))
        .collect();
    ranked.sort_by(|(a, x), (b, y)| b.cmp(a).then_with(|| x.account_id.cmp(&y.account_id)));

    let mut remaining = size;
    let mut sizes = Vec::new();
    for (_, c) in ranked {
        if remaining == 0 {
            break;
        }
        let held = whole_lots(&c.base, lot_size);
        let take = held.min(remaining);
        if take > 0 {
            sizes.push((c.account_id, take));
            remaining -= take;
        }
    }
    if remaining != 0 {
        return None;
    }

    // Shares follow the size taken over. Rounding leaves a remainder, which goes to the first
    // counterparty, as the engine itself does with the collateral.
    let mut shares: Vec<Share> = sizes
        .iter()
        .map(|&(account_id, take)| Share {
            account_id,
            size: take,
            weight: (u128::from(take) * u128::from(ONE) / u128::from(size)) as u64,
        })
        .collect();
    let assigned: u64 = shares.iter().map(|s| s.weight).sum();
    shares[0].weight += ONE - assigned;
    Some(Plan { size, shares })
}

/// `|base|` in whole lots, in 9-decimal units.
fn whole_lots(base: &BigDecimal, lot_size: u64) -> u64 {
    let raw = (base.abs() * BigDecimal::from(B9))
        .with_scale_round(0, bigdecimal::RoundingMode::Floor)
        .to_u64()
        .unwrap_or(u64::MAX);
    raw - raw % lot_size
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    fn dec(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).unwrap()
    }

    /// A position at `entry` now marked at 30,000.
    fn cp(account_id: i64, base: &str, entry: &str, collateral: &str) -> Counterparty {
        let base = dec(base);
        let entry_notional = (&base * dec(entry)).abs();
        let notional = (&base * dec("30000")).abs();
        let unrealized_pnl = &base * dec("30000") - &base * dec(entry);
        let margin = dec(collateral) + &unrealized_pnl;
        Counterparty {
            account_id,
            base,
            entry_notional,
            unrealized_pnl,
            margin,
            notional,
        }
    }

    #[test]
    fn the_most_leveraged_winners_take_it_first() {
        let candidates = [
            // Short 0.2 from 100,000 on 20,000: return 70%, leverage 6,000 / 34,000.
            cp(1, "-0.2", "100000", "20000"),
            // Short 0.2 from 100,000 on 2,000: return 70%, leverage 6,000 / 16,000.
            cp(2, "-0.2", "100000", "2000"),
            // A long and a losing short are never chosen.
            cp(3, "0.5", "20000", "1000"),
            cp(4, "-0.5", "20000", "1000"),
        ];
        let plan = plan(300_000_000, true, 1_000_000, &candidates).unwrap();
        assert_eq!(plan.size, 300_000_000);
        assert_eq!(
            plan.shares,
            vec![
                Share {
                    account_id: 2,
                    size: 200_000_000,
                    weight: 666_666_666_666_666_667
                },
                Share {
                    account_id: 1,
                    size: 100_000_000,
                    weight: 333_333_333_333_333_333
                },
            ]
        );
        assert_eq!(plan.shares.iter().map(|s| s.weight).sum::<u64>(), ONE);
    }

    #[test]
    fn only_whole_lots_are_taken() {
        // 0.0995 holds 99 whole lots of 0.001.
        let candidates = [
            cp(1, "-0.0995", "100000", "5000"),
            cp(2, "-1", "90000", "90000"),
        ];
        let plan = plan(100_000_000, true, 1_000_000, &candidates).unwrap();
        let sizes: Vec<(i64, u64)> = plan.shares.iter().map(|s| (s.account_id, s.size)).collect();
        assert_eq!(sizes, vec![(1, 99_000_000), (2, 1_000_000)]);
    }

    #[test]
    fn a_short_is_taken_over_by_longs() {
        let candidates = [cp(5, "0.4", "20000", "2000")];
        let plan = plan(300_000_000, false, 1_000_000, &candidates).unwrap();
        assert_eq!(
            plan.shares,
            vec![Share {
                account_id: 5,
                size: 300_000_000,
                weight: ONE
            }]
        );
    }

    #[test]
    fn no_plan_without_enough_on_the_other_side() {
        let candidates = [cp(1, "-0.1", "100000", "5000")];
        assert_eq!(plan(300_000_000, true, 1_000_000, &candidates), None);
        assert_eq!(plan(0, true, 1_000_000, &candidates), None);
        // Not a whole number of lots.
        assert_eq!(plan(1_500_000, true, 1_000_000, &candidates), None);
    }
}
