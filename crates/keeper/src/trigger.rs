// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Whether a stop ticket can be executed now, decided the way `perpetuals_orders::stop_orders`
//! decides it, so that the keeper only simulates the tickets the engine would take.
//!
//! The prices come from the engine's formulas as `perp_engine` restates them, over the indexer's
//! copy of the market and the signed price the execution relays. The mark price can be off by a
//! TWAP sample: a mark price trigger updates the market's TWAPs and funding before it reads the
//! mark price, and the indexer's copy has not seen that update. The simulation has the last word.

use bigdecimal::{BigDecimal, Signed, Zero};
use num_bigint::BigUint;
use perp_bot_common::position::ifixed_to_decimal;
use perp_bot_common::tickets::{StandaloneStop, StopLossTakeProfit};
use perp_engine::Pricing;

/// A market's price for each trigger price type.
#[derive(Clone, Debug, PartialEq)]
pub struct Prices {
    /// 0: the base feed's price.
    pub index: BigDecimal,
    /// 1: the middle of the book, or the index price while a side of the book is empty.
    pub book: BigDecimal,
    /// 2: the mark price, or `None` while the index price strays further from its TWAP than the
    /// market allows, when the engine refuses to compute it (`EIndexTwapDivergence`).
    pub mark: Option<BigDecimal>,
}

impl Prices {
    /// `max_index_twap_divergence` is the market's limit, as a fraction of the index TWAP; when
    /// it is not known the mark price is kept and the simulation decides.
    pub fn new(
        pricing: &Pricing,
        now_ms: i64,
        max_index_twap_divergence: Option<&BigDecimal>,
    ) -> Self {
        let divergence = (&pricing.index_price - &pricing.index_twap_price).abs();
        let within_limit = max_index_twap_divergence
            .is_none_or(|limit| divergence <= &pricing.index_twap_price * limit);
        Self {
            index: pricing.index_price.clone(),
            book: pricing.book_price(),
            mark: within_limit.then(|| pricing.mark_price(now_ms)),
        }
    }

    fn trigger(&self, trigger_price_type: u8) -> Result<&BigDecimal, Verdict> {
        match trigger_price_type {
            0 => Ok(&self.index),
            1 => Ok(&self.book),
            2 => self.mark.as_ref().ok_or(Verdict::MarkUnavailable),
            _ => Err(Verdict::Invalid),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// The trigger price has crossed: the ticket is worth a simulation. `stop_loss` when it
    /// guards a position against a loss; those go first.
    Triggered { stop_loss: bool },
    /// The trigger price has not crossed yet.
    Waiting,
    /// At or past its expiry: the engine will never take it (`EStopOrderTicketExpired`).
    Expired,
    /// The order closes or reduces a position the account does not hold on that side: closed,
    /// flipped, or not opened yet (`EInvalidPositionForSltp`, `EReduceOnlyViolated`).
    NoPosition,
    /// A mark price trigger while there is no mark price.
    MarkUnavailable,
    /// A trigger price type the engine does not have (`EInvalidStopOrderTriggerPriceType`).
    Invalid,
}

/// Whether an expiry has passed: the engine takes a ticket only strictly before it.
pub fn expired(expire_timestamp_ms: Option<u64>, now_ms: u64) -> bool {
    expire_timestamp_ms.is_some_and(|expiry| now_ms >= expiry)
}

/// A stop loss / take profit on the account's position in the details' market, whose base is
/// `position_base` (negative for a short).
pub fn stop_loss_take_profit(
    details: &StopLossTakeProfit,
    prices: &Prices,
    position_base: &BigDecimal,
    now_ms: u64,
) -> Verdict {
    if expired(details.expire_timestamp_ms, now_ms) {
        return Verdict::Expired;
    }
    // A short is an ask position.
    if position_base.is_zero() || details.position_is_ask != position_base.is_negative() {
        return Verdict::NoPosition;
    }
    let price = match prices.trigger(details.trigger_price_type) {
        Ok(price) => price,
        Err(verdict) => return verdict,
    };
    // A short's stop loss is above the price and its take profit below; a long's the other way
    // around.
    let short = details.position_is_ask;
    let stop_loss = details
        .stop_loss_price
        .as_ref()
        .map(decimal)
        .is_some_and(|level| {
            if short {
                price >= &level
            } else {
                price <= &level
            }
        });
    let take_profit = details
        .take_profit_price
        .as_ref()
        .map(decimal)
        .is_some_and(|level| {
            if short {
                price <= &level
            } else {
                price >= &level
            }
        });
    if stop_loss || take_profit {
        Verdict::Triggered { stop_loss }
    } else {
        Verdict::Waiting
    }
}

/// A standalone stop. A reduce-only one needs a position its order reduces: asks reduce longs
/// and bids shorts. It guards that position against a loss when it fires on the adverse move: a
/// sale on the way down, a buy-back on the way up.
pub fn standalone_stop(
    details: &StandaloneStop,
    prices: &Prices,
    position_base: &BigDecimal,
    now_ms: u64,
) -> Verdict {
    if expired(details.expire_timestamp_ms, now_ms) {
        return Verdict::Expired;
    }
    if details.reduce_only
        && (position_base.is_zero() || position_base.is_negative() == details.side)
    {
        return Verdict::NoPosition;
    }
    let price = match prices.trigger(details.trigger_price_type) {
        Ok(price) => price,
        Err(verdict) => return verdict,
    };
    let level = decimal(&details.stop_index_price);
    let crossed = if details.ge_stop_index_price {
        price >= &level
    } else {
        price <= &level
    };
    if crossed {
        Verdict::Triggered {
            stop_loss: details.reduce_only && details.side != details.ge_stop_index_price,
        }
    } else {
        Verdict::Waiting
    }
}

/// An `ifixed` from its 32 little-endian bytes.
fn decimal(ifixed: &[u8; 32]) -> BigDecimal {
    ifixed_to_decimal(BigUint::from_bytes_le(ifixed))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use haneul_sdk_types::Address;

    use super::*;

    fn dec(text: &str) -> BigDecimal {
        BigDecimal::from_str(text).unwrap()
    }

    fn usd(dollars: u128) -> [u8; 32] {
        let mut bytes = [0; 32];
        bytes[..16].copy_from_slice(&(dollars * 10u128.pow(18)).to_le_bytes());
        bytes
    }

    /// The same price for every trigger type.
    fn at(price: &str) -> Prices {
        Prices {
            index: dec(price),
            book: dec(price),
            mark: Some(dec(price)),
        }
    }

    fn long_stop_loss_take_profit() -> StopLossTakeProfit {
        StopLossTakeProfit {
            clearing_house: Address::from_static("0xc0b6"),
            expire_timestamp_ms: None,
            is_limit_order: false,
            trigger_price_type: 0,
            stop_loss_price: Some(usd(95_000)),
            take_profit_price: Some(usd(105_000)),
            position_is_ask: false,
            size: 250_000_000,
            price: 0,
            order_type: 0,
            integrator_info: None,
            salt: vec![0; 32],
        }
    }

    fn standalone_buy_above() -> StandaloneStop {
        StandaloneStop {
            clearing_house: Address::from_static("0xc0b6"),
            expire_timestamp_ms: None,
            is_limit_order: false,
            trigger_price_type: 0,
            stop_index_price: usd(101_000),
            ge_stop_index_price: true,
            side: false,
            size: 100_000_000,
            price: 0,
            order_type: 0,
            reduce_only: false,
            integrator_info: None,
            salt: vec![0; 32],
        }
    }

    #[test]
    fn a_long_stops_at_or_below_its_stop_loss_and_takes_profit_at_or_above() {
        let details = long_stop_loss_take_profit();
        let long = dec("0.25");
        let verdict = |price: &str| stop_loss_take_profit(&details, &at(price), &long, 0);
        assert_eq!(verdict("94000"), Verdict::Triggered { stop_loss: true });
        assert_eq!(verdict("95000"), Verdict::Triggered { stop_loss: true });
        assert_eq!(verdict("95000.000000001"), Verdict::Waiting);
        assert_eq!(verdict("104999.999999999"), Verdict::Waiting);
        assert_eq!(verdict("105000"), Verdict::Triggered { stop_loss: false });
    }

    #[test]
    fn a_shorts_levels_are_the_other_way_around() {
        let mut details = long_stop_loss_take_profit();
        details.position_is_ask = true;
        details.stop_loss_price = Some(usd(105_000));
        details.take_profit_price = Some(usd(95_000));
        let short = dec("-0.25");
        let verdict = |price: &str| stop_loss_take_profit(&details, &at(price), &short, 0);
        assert_eq!(verdict("106000"), Verdict::Triggered { stop_loss: true });
        assert_eq!(verdict("105000"), Verdict::Triggered { stop_loss: true });
        assert_eq!(verdict("100000"), Verdict::Waiting);
        assert_eq!(verdict("95000"), Verdict::Triggered { stop_loss: false });
    }

    #[test]
    fn a_stop_loss_take_profit_needs_a_position_on_its_side() {
        let details = long_stop_loss_take_profit();
        let crashed = at("90000");
        assert_eq!(
            stop_loss_take_profit(&details, &crashed, &dec("0"), 0),
            Verdict::NoPosition
        );
        assert_eq!(
            stop_loss_take_profit(&details, &crashed, &dec("-0.1"), 0),
            Verdict::NoPosition
        );
        // A take profit alone does nothing on the way down.
        let mut take_profit_only = long_stop_loss_take_profit();
        take_profit_only.stop_loss_price = None;
        assert_eq!(
            stop_loss_take_profit(&take_profit_only, &crashed, &dec("0.25"), 0),
            Verdict::Waiting
        );
    }

    #[test]
    fn a_ticket_expires_at_its_expiry() {
        assert!(!expired(None, u64::MAX));
        assert!(!expired(Some(1_000), 999));
        assert!(expired(Some(1_000), 1_000));
        let mut details = long_stop_loss_take_profit();
        details.expire_timestamp_ms = Some(1_000);
        let (crashed, long) = (at("90000"), dec("0.25"));
        assert_eq!(
            stop_loss_take_profit(&details, &crashed, &long, 999),
            Verdict::Triggered { stop_loss: true }
        );
        assert_eq!(
            stop_loss_take_profit(&details, &crashed, &long, 1_000),
            Verdict::Expired
        );
    }

    #[test]
    fn each_trigger_type_reads_its_own_price() {
        let prices = Prices {
            index: dec("90000"),
            book: dec("100000"),
            mark: Some(dec("110000")),
        };
        let long = dec("0.25");
        let verdict = |trigger_price_type: u8, prices: &Prices| {
            let mut details = long_stop_loss_take_profit();
            details.trigger_price_type = trigger_price_type;
            stop_loss_take_profit(&details, prices, &long, 0)
        };
        assert_eq!(verdict(0, &prices), Verdict::Triggered { stop_loss: true });
        assert_eq!(verdict(1, &prices), Verdict::Waiting);
        assert_eq!(verdict(2, &prices), Verdict::Triggered { stop_loss: false });
        assert_eq!(verdict(3, &prices), Verdict::Invalid);
        let no_mark = Prices {
            mark: None,
            ..prices.clone()
        };
        assert_eq!(verdict(2, &no_mark), Verdict::MarkUnavailable);
        assert_eq!(verdict(0, &no_mark), Verdict::Triggered { stop_loss: true });
    }

    #[test]
    fn a_standalone_stop_fires_on_its_side_of_the_level() {
        let flat = dec("0");
        let above = standalone_buy_above();
        assert_eq!(
            standalone_stop(&above, &at("101000"), &flat, 0),
            Verdict::Triggered { stop_loss: false }
        );
        assert_eq!(
            standalone_stop(&above, &at("100999.999999999"), &flat, 0),
            Verdict::Waiting
        );
        let mut below = standalone_buy_above();
        below.ge_stop_index_price = false;
        assert_eq!(
            standalone_stop(&below, &at("101000"), &flat, 0),
            Verdict::Triggered { stop_loss: false }
        );
        assert_eq!(
            standalone_stop(&below, &at("101000.000000001"), &flat, 0),
            Verdict::Waiting
        );
    }

    #[test]
    fn a_reduce_only_standalone_stop_needs_a_position_it_reduces() {
        // A buy-back on the way up reduces a short: its stop loss.
        let mut buy_back = standalone_buy_above();
        buy_back.reduce_only = true;
        let above = at("102000");
        assert_eq!(
            standalone_stop(&buy_back, &above, &dec("-0.1"), 0),
            Verdict::Triggered { stop_loss: true }
        );
        assert_eq!(
            standalone_stop(&buy_back, &above, &dec("0.1"), 0),
            Verdict::NoPosition
        );
        assert_eq!(
            standalone_stop(&buy_back, &above, &dec("0"), 0),
            Verdict::NoPosition
        );
        // A sale reduces a long: on the way down its stop loss, on the way up its take profit.
        let mut sale = buy_back.clone();
        sale.side = true;
        sale.ge_stop_index_price = false;
        assert_eq!(
            standalone_stop(&sale, &at("100000"), &dec("0.1"), 0),
            Verdict::Triggered { stop_loss: true }
        );
        sale.ge_stop_index_price = true;
        assert_eq!(
            standalone_stop(&sale, &above, &dec("0.1"), 0),
            Verdict::Triggered { stop_loss: false }
        );
    }

    #[test]
    fn prices_follow_the_engine_and_lose_the_mark_beyond_the_divergence_limit() {
        let pricing = Pricing {
            index_price: dec("100500"),
            index_twap_price: dec("100000"),
            premium_twap: dec("0"),
            spread_twap: dec("0"),
            best_bid: Some(dec("99990")),
            best_ask: Some(dec("100010")),
            funding_last_upd_ms: 0,
            funding_frequency_ms: 3_600_000,
            funding_period_ms: 86_400_000,
        };
        // The index is 0.5% off its TWAP: exactly at a 0.5% limit is still within it.
        let limit = dec("0.005");
        let prices = Prices::new(&pricing, 1_000, Some(&limit));
        assert_eq!(prices.index, dec("100500"));
        assert_eq!(prices.book, dec("100000"));
        // The median of the funding price, the TWAP plus the spread TWAP and the book price.
        assert_eq!(prices.mark, Some(dec("100000")));
        assert_eq!(prices.mark, Some(pricing.mark_price(1_000)));
        assert_eq!(Prices::new(&pricing, 1_000, Some(&dec("0.004"))).mark, None);
        assert!(Prices::new(&pricing, 1_000, None).mark.is_some());
        let one_sided = Pricing {
            best_ask: None,
            ..pricing.clone()
        };
        assert_eq!(
            Prices::new(&one_sided, 1_000, Some(&limit)).book,
            dec("100500")
        );
    }
}
