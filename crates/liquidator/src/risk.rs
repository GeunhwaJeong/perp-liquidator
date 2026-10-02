// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Which positions can be liquidated, judged the way `clearing_house::liquidate` judges them.
//!
//! The engine settles the position's funding, then liquidates when its margin (collateral value
//! plus unrealized profit, at the session's mark price) is below its maintenance requirement
//! (its largest size with resting orders, times the mark price, times the maintenance ratio).
//! The same formulas run here over the indexer's copy of the chain. The session also moves the
//! TWAPs and funding before it prices anything, so the mark price here can differ slightly from
//! the one the engine will use; positions just above the line are tried too, and simulation
//! settles every case before anything is signed.

use std::cmp::Ordering;

use bigdecimal::{BigDecimal, One, Signed, ToPrimitive, Zero};
use num_bigint::BigInt;
use perp_engine::{Position, Valuation};
use serde::Serialize;

/// The engine's prices and sizes are integers with 9 decimals.
const B9: i64 = 1_000_000_000;

/// A market as far as liquidating in it goes.
#[derive(Clone, Debug)]
pub struct MarketView {
    /// The clearing house ID.
    pub id: String,
    pub ticker: String,
    /// False while the market is paused, closed or settled: nothing can be liquidated then.
    pub tradable: bool,
    pub valuation: Valuation,
    pub margin_ratio_maintenance: BigDecimal,
    /// In the engine's 9-decimal units.
    pub lot_size: u64,
    pub tick_size: u64,
    pub index_price: BigDecimal,
}

/// One position measured against the liquidation line.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Assessment {
    pub market: String,
    pub account_id: i64,
    pub base: BigDecimal,
    /// `|base|` at the mark price.
    pub notional: BigDecimal,
    pub margin: BigDecimal,
    pub maintenance: BigDecimal,
    pub pending_orders: i64,
}

impl Assessment {
    /// The engine's test: strictly below the maintenance requirement. A position with nothing
    /// to margin is liquidatable only while its collateral is negative, which closes it out as
    /// bad debt.
    pub fn liquidatable(&self) -> bool {
        self.margin < self.maintenance
    }

    /// Negative equity: liquidating it leaves bad debt for the insurance fund.
    pub fn bad_debt(&self) -> bool {
        self.margin.is_negative()
    }

    /// Margin over maintenance requirement: below 1 is liquidatable. None for a position that
    /// needs no margin.
    pub fn health(&self) -> Option<BigDecimal> {
        if self.maintenance.is_zero() {
            None
        } else {
            Some((&self.margin / &self.maintenance).with_scale(6))
        }
    }

    /// Whether to put this position to the engine: liquidatable here, or within `buffer` (a
    /// fraction of the requirement) of it, since the session's own prices may differ a little.
    pub fn worth_trying(&self, buffer: &BigDecimal) -> bool {
        self.liquidatable() || self.margin < &self.maintenance * (BigDecimal::one() + buffer)
    }
}

pub fn assess(market: &MarketView, account_id: i64, position: &Position) -> Assessment {
    let margin = position.margin(&market.valuation).margin;
    let maintenance = position.maintenance_requirement(
        &market.valuation.mark_price,
        &market.margin_ratio_maintenance,
    );
    Assessment {
        market: market.id.clone(),
        account_id,
        notional: (position.base.abs() * &market.valuation.mark_price).with_scale(6),
        base: position.base.clone(),
        margin,
        maintenance,
        pending_orders: position.pending_orders,
    }
}

/// The order positions are liquidated in: the largest requirement first, since the largest
/// positions put the most at risk if prices keep moving; then the furthest below the line.
pub fn rank(candidates: &mut [Assessment]) {
    candidates.sort_by(|a, b| {
        b.maintenance
            .cmp(&a.maintenance)
            .then_with(|| shortfall(b).cmp(&shortfall(a)))
            .then_with(|| a.market.cmp(&b.market))
            .then_with(|| a.account_id.cmp(&b.account_id))
    });
}

fn shortfall(a: &Assessment) -> BigDecimal {
    &a.maintenance - &a.margin
}

/// The smallest multiple of `lot_size` (9-decimal units) that covers `abs_base`, or None when it
/// does not fit an order size.
pub fn lots_covering(abs_base: &BigDecimal, lot_size: u64) -> Option<u64> {
    if lot_size == 0 {
        return None;
    }
    let raw = ceil_integer(&(abs_base.abs() * BigDecimal::from(B9)));
    let lot = BigInt::from(lot_size);
    let lots = (&raw + &lot - BigInt::one()) / &lot;
    (lots * lot).to_u64()
}

/// The limit price of an order that unwinds a position at most `slippage` (a fraction) away
/// from `mark`: below it for a sale, above it for a purchase. It is rounded onto the tick grid
/// towards the mark, so that it never gives away more than the slippage. None when no such
/// price exists on the grid.
pub fn limit_price(
    mark: &BigDecimal,
    is_ask: bool,
    slippage: &BigDecimal,
    tick_size: u64,
) -> Option<u64> {
    if tick_size == 0 || !mark.is_positive() {
        return None;
    }
    let tick = BigInt::from(tick_size);
    let factor = if is_ask {
        BigDecimal::one() - slippage
    } else {
        BigDecimal::one() + slippage
    };
    let raw = mark * factor * BigDecimal::from(B9);
    let ticks = if is_ask {
        let up = ceil_integer(&raw);
        (&up + &tick - BigInt::one()) / &tick
    } else {
        floor_integer(&raw) / &tick
    };
    let price = (ticks * tick).to_u64()?;
    // The engine keeps the price below the top bit of an order ID.
    (price != 0 && price < 1 << 63).then_some(price)
}

fn floor_integer(value: &BigDecimal) -> BigInt {
    let (digits, scale) = value
        .with_scale_round(0, bigdecimal::RoundingMode::Floor)
        .into_bigint_and_exponent();
    debug_assert_eq!(scale, 0);
    digits
}

fn ceil_integer(value: &BigDecimal) -> BigInt {
    let (digits, scale) = value
        .with_scale_round(0, bigdecimal::RoundingMode::Ceiling)
        .into_bigint_and_exponent();
    debug_assert_eq!(scale, 0);
    digits
}

/// Orders assessments for display: the least healthy first.
pub fn by_health(a: &Assessment, b: &Assessment) -> Ordering {
    match (a.health(), b.health()) {
        (Some(x), Some(y)) => x.cmp(&y),
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (None, None) => a.margin.cmp(&b.margin),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    fn dec(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).unwrap()
    }

    fn market(mark: &str) -> MarketView {
        MarketView {
            id: "0xc".into(),
            ticker: "BTC-USD".into(),
            tradable: true,
            valuation: Valuation {
                mark_price: dec(mark),
                collateral_price: dec("1"),
                collateral_haircut: dec("0"),
                cum_funding_rate_long: dec("0"),
                cum_funding_rate_short: dec("0"),
                margin_ratio_initial: dec("0.1"),
            },
            margin_ratio_maintenance: dec("0.05"),
            lot_size: 1_000_000,
            tick_size: 1_000_000_000,
            index_price: dec(mark),
        }
    }

    /// A position entered at `entry` with `collateral` USD of collateral.
    fn position(base: &str, entry: &str, collateral: &str) -> Position {
        Position {
            collateral: dec(collateral),
            base: dec(base),
            quote_notional: dec(base) * dec(entry),
            cum_funding_rate_long: dec("0"),
            cum_funding_rate_short: dec("0"),
            asks_quantity: dec("0"),
            bids_quantity: dec("0"),
            pending_orders: 0,
            initial_margin_ratio: dec("0.1"),
        }
    }

    #[test]
    fn the_line_is_the_maintenance_requirement() {
        // Long 0.3 at 100,000 on 3,000: at 95,000 the margin is 1,500 against a requirement of
        // 1,425; at 94,700 it is 1,410 against 1,420.5.
        let p = position("0.3", "100000", "3000");
        let above = assess(&market("95000"), 7, &p);
        assert_eq!(above.margin, dec("1500"));
        assert_eq!(above.maintenance, dec("1425"));
        assert!(!above.liquidatable());
        assert!(!above.bad_debt());
        assert_eq!(above.health(), Some(dec("1.052631")));

        let below = assess(&market("94700"), 7, &p);
        assert!(below.liquidatable());
        assert!(!below.bad_debt());
        assert_eq!(below.notional, dec("28410"));
    }

    #[test]
    fn exactly_at_the_line_is_not_liquidatable() {
        // 1,500 of collateral is exactly 5% of 0.3 at 100,000.
        let p = position("0.3", "100000", "1500");
        let at = assess(&market("100000"), 7, &p);
        assert_eq!(at.margin, at.maintenance);
        assert!(!at.liquidatable());
        assert!(at.worth_trying(&dec("0.001")));
        assert!(!at.worth_trying(&dec("0")));
    }

    #[test]
    fn shorts_lose_as_the_price_rises() {
        let p = position("-0.3", "100000", "3000");
        assert!(!assess(&market("104000"), 1, &p).liquidatable());
        let up = assess(&market("105000"), 1, &p);
        // 3,000 - 1,500 = 1,500 against 0.05 * 0.3 * 105,000 = 1,575.
        assert!(up.liquidatable());
        let way_up = assess(&market("111000"), 1, &p);
        assert!(way_up.bad_debt());
    }

    #[test]
    fn resting_orders_count_toward_the_requirement() {
        let mut p = position("0.1", "100000", "600");
        assert!(!assess(&market("100000"), 1, &p).liquidatable());
        // Bids that could make it 0.2 double the requirement: 1,000 against 600.
        p.bids_quantity = dec("0.1");
        p.pending_orders = 2;
        let a = assess(&market("100000"), 1, &p);
        assert_eq!(a.maintenance, dec("1000"));
        assert!(a.liquidatable());
        assert_eq!(a.pending_orders, 2);
    }

    #[test]
    fn a_flat_position_with_negative_collateral_is_closed_out() {
        let p = position("0", "0", "-12.5");
        let a = assess(&market("100000"), 3, &p);
        assert_eq!(a.maintenance, dec("0"));
        assert!(a.liquidatable());
        assert!(a.bad_debt());
        assert_eq!(a.health(), None);
        let healthy = assess(&market("100000"), 3, &position("0", "0", "5"));
        assert!(!healthy.liquidatable());
    }

    #[test]
    fn largest_requirement_goes_first() {
        let m = market("94000");
        let mut list = vec![
            assess(&m, 1, &position("0.1", "100000", "500")),
            assess(&m, 2, &position("1", "100000", "6500")),
            assess(&m, 3, &position("0.1", "100000", "100")),
        ];
        rank(&mut list);
        let order: Vec<i64> = list.iter().map(|a| a.account_id).collect();
        // 2 has the largest requirement; 3 and 1 tie on it and 3 is further below.
        assert_eq!(order, vec![2, 3, 1]);
    }

    #[test]
    fn sizes_round_up_to_whole_lots() {
        assert_eq!(lots_covering(&dec("0.3"), 1_000_000), Some(300_000_000));
        assert_eq!(lots_covering(&dec("-0.3004"), 1_000_000), Some(301_000_000));
        assert_eq!(
            lots_covering(&dec("0.0000000001"), 1_000_000),
            Some(1_000_000)
        );
        assert_eq!(lots_covering(&dec("0"), 1_000_000), Some(0));
        assert_eq!(lots_covering(&dec("1"), 0), None);
        assert_eq!(lots_covering(&dec("1e20"), 1), None);
    }

    #[test]
    fn unwind_prices_stay_within_the_slippage_on_the_grid() {
        let s = dec("0.005");
        // Selling at most 0.5% under 94,123.45: 93,652.83275 rounds up to 93,653.
        assert_eq!(
            limit_price(&dec("94123.45"), true, &s, 1_000_000_000),
            Some(93_653_000_000_000)
        );
        // Buying at most 0.5% over it: 94,594.06725 rounds down to 94,594.
        assert_eq!(
            limit_price(&dec("94123.45"), false, &s, 1_000_000_000),
            Some(94_594_000_000_000)
        );
        // A purchase on a grid coarser than the price has nowhere to go but zero.
        assert_eq!(limit_price(&dec("0.5"), false, &s, 1_000_000_000), None);
        assert_eq!(limit_price(&dec("0"), true, &s, 1), None);
    }
}
