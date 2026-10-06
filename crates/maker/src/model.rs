// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The quoting model, pure: given the reference price, what the maker knows about volatility
//! and its own inventory, the ladder it wants resting. Everything on chain happens elsewhere.
//!
//! Prices are USD; the engine takes them in 9-decimal units on the tick, sizes in 9-decimal
//! base units on the lot.

use bigdecimal::{BigDecimal, One, Signed, ToPrimitive, Zero};

/// Nine decimals: the engine's price and size units.
pub const UNIT: u64 = 1_000_000_000;

#[derive(Clone, Debug)]
pub struct Params {
    pub levels: u32,
    /// `s_base`: what a round trip should earn, in basis points of the reference.
    pub half_spread_bps: BigDecimal,
    pub min_half_spread_bps: BigDecimal,
    /// How many rounds of typical motion the spread covers.
    pub vol_multiplier: BigDecimal,
    /// How much of the signed price's confidence interval goes into the spread.
    pub conf_multiplier: BigDecimal,
    pub level_step_bps: BigDecimal,
    /// The innermost level's size, in base units.
    pub size_base: BigDecimal,
    /// Each further level is this much bigger than the one before.
    pub size_growth: BigDecimal,
    /// `q_max`: inventory beyond which only the reducing side is quoted.
    pub max_position: BigDecimal,
    /// `q_hard`: inventory beyond which the excess is sold through the book.
    pub hard_position: BigDecimal,
    /// The skew at the inventory limit, in basis points of the reference.
    pub skew_bps: BigDecimal,
    pub flatten_slippage_bps: BigDecimal,
    /// The market's minimum order value, in USD.
    pub min_order_usd: BigDecimal,
    /// Margin health (margin over the maintenance requirement) under which the growing side is
    /// not quoted.
    pub min_health: BigDecimal,
    /// The market's tick and lot, in the engine's 9-decimal units.
    pub tick: u64,
    pub lot: u64,
}

/// What the model is told each round.
#[derive(Clone, Debug)]
pub struct Observation {
    pub reference: BigDecimal,
    /// The signed update's confidence interval, in basis points of its price.
    pub confidence_bps: BigDecimal,
    /// The estimate of one round's motion, in basis points.
    pub sigma_bps: BigDecimal,
    /// The maker's position, signed base units.
    pub position: BigDecimal,
    /// Margin over maintenance requirement; None when the position needs none.
    pub health: Option<BigDecimal>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Inventory {
    /// Both sides quoted.
    Balanced,
    /// Over `q_max` or under the health floor: only the reducing side, reduce-only.
    Reducing,
    /// Over `q_hard`: reducing side plus an immediate order for the excess.
    Flattening,
}

impl Inventory {
    pub fn label(&self) -> &'static str {
        match self {
            Inventory::Balanced => "balanced",
            Inventory::Reducing => "reducing",
            Inventory::Flattening => "flattening",
        }
    }
}

/// One resting order the maker wants.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Level {
    pub is_bid: bool,
    /// 9-decimal USD, on the tick.
    pub price: u64,
    /// 9-decimal base units, on the lot.
    pub size: u64,
    pub reduce_only: bool,
}

/// An immediate-or-cancel order that brings the inventory back under `q_max`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Flatten {
    pub is_bid: bool,
    pub price: u64,
    pub size: u64,
}

#[derive(Clone, Debug)]
pub struct Quote {
    pub reference: BigDecimal,
    pub mid: BigDecimal,
    pub half_spread_bps: BigDecimal,
    pub skew_bps: BigDecimal,
    pub inventory: Inventory,
    pub levels: Vec<Level>,
    pub flatten: Option<Flatten>,
}

fn bps(value: &BigDecimal) -> BigDecimal {
    value / BigDecimal::from(10_000)
}

/// A USD price as the engine's 9-decimal integer, rounded onto the tick: bids down, asks up,
/// so that a rounding never crosses the mid.
pub fn price_units(price: &BigDecimal, tick: u64, is_bid: bool) -> u64 {
    let raw = (price * BigDecimal::from(UNIT)).with_scale(0);
    let raw = raw.to_u128().unwrap_or(0);
    let tick = tick.max(1) as u128;
    let down = raw / tick * tick;
    let on_tick = if is_bid || down == raw {
        down
    } else {
        down + tick
    };
    on_tick.min(u64::MAX as u128) as u64
}

/// A base size as the engine's 9-decimal integer, rounded down onto the lot.
pub fn size_units(size: &BigDecimal, lot: u64) -> u64 {
    let raw = (size * BigDecimal::from(UNIT)).with_scale(0);
    let raw = raw.to_u128().unwrap_or(0);
    let lot = lot.max(1) as u128;
    ((raw / lot) * lot).min(u64::MAX as u128) as u64
}

pub fn quote(p: &Params, o: &Observation) -> Quote {
    let one = BigDecimal::one();

    // Inventory state.
    let q = &o.position;
    let abs_q = q.abs();
    let unhealthy = o.health.as_ref().is_some_and(|h| h < &p.min_health);
    let inventory = if abs_q >= p.hard_position && p.hard_position.is_positive() {
        Inventory::Flattening
    } else if abs_q >= p.max_position || unhealthy {
        Inventory::Reducing
    } else {
        Inventory::Balanced
    };

    // Skew: long inventory lowers the ladder, short raises it.
    let fraction = if p.max_position.is_positive() {
        (q / &p.max_position).max(-one.clone()).min(one.clone())
    } else {
        BigDecimal::zero()
    };
    let skew_bps = -&p.skew_bps * fraction;
    let mid = &o.reference * (&one + bps(&skew_bps));

    // Spread.
    let half_spread_bps = (&p.half_spread_bps
        + &p.vol_multiplier * &o.sigma_bps
        + &p.conf_multiplier * &o.confidence_bps)
        .max(p.min_half_spread_bps.clone());
    let half = bps(&half_spread_bps);
    let step = bps(&p.level_step_bps);

    let quote_bids = match inventory {
        Inventory::Balanced => true,
        _ => q.is_negative(),
    };
    let quote_asks = match inventory {
        Inventory::Balanced => true,
        _ => q.is_positive(),
    };
    let reduce_only = inventory != Inventory::Balanced;

    let mut levels = Vec::new();
    let mut size = p.size_base.clone();
    // The reducing side never rests more than the position it reduces.
    let mut remaining = abs_q.clone();
    for i in 0..p.levels {
        let offset = &half + &step * BigDecimal::from(i);
        let size_here = if reduce_only {
            let s = size.clone().min(remaining.clone());
            remaining -= &s;
            s
        } else {
            size.clone()
        };
        let bid_price = &mid * (&one - &offset);
        let ask_price = &mid * (&one + &offset);
        for (is_bid, price, on) in [
            (true, bid_price, quote_bids),
            (false, ask_price, quote_asks),
        ] {
            if !on {
                continue;
            }
            let price_u = price_units(&price, p.tick, is_bid);
            let size_u = size_units(&size_here, p.lot);
            if price_u == 0 || size_u == 0 {
                continue;
            }
            let value = BigDecimal::from(size_u) * BigDecimal::from(price_u)
                / BigDecimal::from(UNIT)
                / BigDecimal::from(UNIT);
            if value < p.min_order_usd {
                continue;
            }
            levels.push(Level {
                is_bid,
                price: price_u,
                size: size_u,
                reduce_only,
            });
        }
        size *= &one + &p.size_growth;
    }

    let flatten = (inventory == Inventory::Flattening).then(|| {
        let excess = &abs_q - &p.max_position;
        let is_bid = q.is_negative();
        let slip = bps(&p.flatten_slippage_bps);
        let price = if is_bid {
            &o.reference * (&one + &slip)
        } else {
            &o.reference * (&one - &slip)
        };
        Flatten {
            is_bid,
            price: price_units(&price, p.tick, is_bid),
            size: size_units(&excess, p.lot),
        }
    });

    Quote {
        reference: o.reference.clone(),
        mid,
        half_spread_bps,
        skew_bps,
        inventory,
        levels,
        flatten,
    }
}

/// Whether the reference moved enough since the last round to re-quote.
pub fn moved(
    last_reference: &BigDecimal,
    reference: &BigDecimal,
    requote_bps: &BigDecimal,
) -> bool {
    if last_reference.is_zero() {
        return true;
    }
    let change = (reference - last_reference).abs() / last_reference * BigDecimal::from(10_000);
    change >= *requote_bps
}

/// The exponentially weighted estimate of one round's motion, in basis points: the previous
/// estimate decays by `1 - alpha` and the latest absolute move counts `alpha`.
pub fn update_sigma(
    sigma_bps: &BigDecimal,
    last_reference: &BigDecimal,
    reference: &BigDecimal,
    alpha: &BigDecimal,
) -> BigDecimal {
    if last_reference.is_zero() {
        return sigma_bps.clone();
    }
    let move_bps = (reference - last_reference).abs() / last_reference * BigDecimal::from(10_000);
    (BigDecimal::one() - alpha) * sigma_bps + alpha * move_bps
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    fn dec(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).unwrap()
    }

    fn params() -> Params {
        Params {
            levels: 3,
            half_spread_bps: dec("8"),
            min_half_spread_bps: dec("5"),
            vol_multiplier: dec("2"),
            conf_multiplier: dec("0.5"),
            level_step_bps: dec("6"),
            size_base: dec("0.01"),
            size_growth: dec("0.5"),
            max_position: dec("0.25"),
            hard_position: dec("0.5"),
            skew_bps: dec("16"),
            flatten_slippage_bps: dec("20"),
            min_order_usd: dec("10"),
            min_health: dec("3"),
            tick: UNIT,     // $1
            lot: 1_000_000, // 0.001
        }
    }

    fn observe(position: &str) -> Observation {
        Observation {
            reference: dec("85000"),
            confidence_bps: dec("0"),
            sigma_bps: dec("0"),
            position: dec(position),
            health: None,
        }
    }

    #[test]
    fn a_flat_maker_quotes_both_sides_around_the_reference() {
        let q = quote(&params(), &observe("0"));
        assert_eq!(q.inventory, Inventory::Balanced);
        assert_eq!(q.half_spread_bps, dec("8"));
        assert_eq!(q.levels.len(), 6);
        let bids: Vec<&Level> = q.levels.iter().filter(|l| l.is_bid).collect();
        let asks: Vec<&Level> = q.levels.iter().filter(|l| !l.is_bid).collect();
        // 85,000 × (1 − 0.0008) = 84,932; rounded down to the dollar.
        assert_eq!(bids[0].price, 84_932 * UNIT);
        // 85,000 × (1 + 0.0008) = 85,068.
        assert_eq!(asks[0].price, 85_068 * UNIT);
        // The next level is 6 bp further out: 85,000 × (1 − 0.0014) = 84,881.
        assert_eq!(bids[1].price, 84_881 * UNIT);
        // Sizes grow by half a level: 0.01, 0.015, 0.0225 (on the lot).
        assert_eq!(bids[0].size, 10_000_000);
        assert_eq!(bids[1].size, 15_000_000);
        assert_eq!(bids[2].size, 22_000_000);
        assert!(q.levels.iter().all(|l| !l.reduce_only));
        assert!(q.flatten.is_none());
    }

    #[test]
    fn inventory_skews_the_ladder_toward_the_reducing_side() {
        let long = quote(&params(), &observe("0.125"));
        // Half of q_max long: the mid is 8 bp under the reference.
        assert_eq!(long.skew_bps, dec("-8"));
        assert_eq!(long.mid, dec("85000") * dec("0.9992"));
        let asks: Vec<&Level> = long.levels.iter().filter(|l| !l.is_bid).collect();
        // The best ask sits at mid + 8 bp = the reference, rounded up.
        assert_eq!(asks[0].price, 85_000 * UNIT);
        let short = quote(&params(), &observe("-0.125"));
        assert_eq!(short.skew_bps, dec("8"));
    }

    #[test]
    fn at_the_limit_only_the_reducing_side_rests_reduce_only() {
        let q = quote(&params(), &observe("0.3"));
        assert_eq!(q.inventory, Inventory::Reducing);
        assert!(q.levels.iter().all(|l| !l.is_bid && l.reduce_only));
        // The resting asks never exceed the position: 0.01 + 0.015 + 0.0225 < 0.3, all kept.
        let total: u64 = q.levels.iter().map(|l| l.size).sum();
        assert_eq!(total, 47_000_000);
        assert!(q.flatten.is_none());

        // A small position caps the reducing side at the position.
        let small = quote(
            &params(),
            &Observation {
                health: Some(dec("2")),
                ..observe("0.012")
            },
        );
        assert_eq!(small.inventory, Inventory::Reducing);
        let total: u64 = small.levels.iter().map(|l| l.size).sum();
        assert_eq!(total, 12_000_000);
    }

    #[test]
    fn past_the_hard_limit_the_excess_is_sold_through_the_book() {
        let q = quote(&params(), &observe("-0.6"));
        assert_eq!(q.inventory, Inventory::Flattening);
        let f = q.flatten.unwrap();
        assert!(f.is_bid);
        // Buys back 0.6 − 0.25 = 0.35 at up to 20 bp over the reference.
        assert_eq!(f.size, 350_000_000);
        assert_eq!(f.price, 85_170 * UNIT);
        assert!(q.levels.iter().all(|l| l.is_bid && l.reduce_only));
    }

    #[test]
    fn the_spread_widens_with_volatility_and_confidence_and_has_a_floor() {
        let calm = quote(
            &params(),
            &Observation {
                sigma_bps: dec("0"),
                ..observe("0")
            },
        );
        assert_eq!(calm.half_spread_bps, dec("8"));
        let busy = quote(
            &params(),
            &Observation {
                sigma_bps: dec("3"),
                confidence_bps: dec("4"),
                ..observe("0")
            },
        );
        // 8 + 2 × 3 + 0.5 × 4 = 16.
        assert_eq!(busy.half_spread_bps, dec("16"));
        let floor = quote(
            &Params {
                half_spread_bps: dec("1"),
                ..params()
            },
            &observe("0"),
        );
        assert_eq!(floor.half_spread_bps, dec("5"));
    }

    #[test]
    fn tiny_levels_under_the_minimum_order_value_are_dropped() {
        let q = quote(
            &Params {
                size_base: dec("0.00005"),
                ..params()
            },
            &observe("0"),
        );
        // 0.00005 BTC rounds down to zero lots; the next levels are 0.000075 and 0.0001125,
        // also zero on a 0.001 lot.
        assert!(q.levels.is_empty());
        let q = quote(
            &Params {
                size_base: dec("0.001"),
                min_order_usd: dec("100"),
                ..params()
            },
            &observe("0"),
        );
        // 0.001 × $85k = $85 < $100 is dropped; 0.0015 rounds to 0.001, dropped; 0.00225 → 0.002
        // = $170 is kept.
        assert_eq!(q.levels.len(), 2);
    }

    #[test]
    fn rounding_never_crosses_the_mid() {
        assert_eq!(price_units(&dec("84932.6"), UNIT, true), 84_932 * UNIT);
        assert_eq!(price_units(&dec("85067.2"), UNIT, false), 85_068 * UNIT);
        assert_eq!(price_units(&dec("85068"), UNIT, false), 85_068 * UNIT);
        assert_eq!(size_units(&dec("0.0129"), 1_000_000), 12_000_000);
    }

    #[test]
    fn requotes_on_a_move_and_tracks_volatility() {
        assert!(moved(&dec("85000"), &dec("85030"), &dec("3")));
        assert!(!moved(&dec("85000"), &dec("85020"), &dec("3")));
        assert!(moved(&dec("0"), &dec("85020"), &dec("3")));
        let sigma = update_sigma(&dec("2"), &dec("85000"), &dec("85085"), &dec("0.1"));
        // 0.9 × 2 + 0.1 × 10 = 2.8.
        assert_eq!(sigma.with_scale(6), dec("2.8").with_scale(6));
    }
}
