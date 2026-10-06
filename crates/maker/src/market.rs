// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! What the maker reads from the chain each round: the clearing house (whether it trades, its
//! lot, tick and limits, its funding state, the top of its book), the base feed's TWAP (what
//! the mark follows) and the maker's own position, valued with `perp-engine`'s formulas.

use anyhow::{Context, bail};
use bigdecimal::{BigDecimal, Zero};
use haneul_sdk_types::Address;
use num_bigint::BigUint;
use perp_bot_common::chain::Chain;
use perp_bot_common::position::{self, ifixed_to_decimal};
use perp_engine::{Position, Valuation};
use serde_json::Value;

/// The clearing house's parameters and state the maker uses.
#[derive(Clone, Debug)]
pub struct MarketView {
    /// 0 trading, 1 paused, 2 closed.
    pub paused: u64,
    pub lot_size: u64,
    pub tick_size: u64,
    pub min_order_usd: BigDecimal,
    pub max_pending_orders: u64,
    pub margin_ratio_initial: BigDecimal,
    pub margin_ratio_maintenance: BigDecimal,
    pub collateral_haircut: BigDecimal,
    pub cum_funding_rate_long: BigDecimal,
    pub cum_funding_rate_short: BigDecimal,
    pub best_bid: Option<u64>,
    pub best_ask: Option<u64>,
    pub base_source_id: u64,
    pub collateral_source_id: u64,
}

impl MarketView {
    pub fn trades(&self) -> bool {
        self.paused == 0
    }

    pub fn base_source_id(&self) -> u64 {
        self.base_source_id
    }

    pub fn collateral_source_id(&self) -> u64 {
        self.collateral_source_id
    }

    pub fn from_json(json: &Value) -> anyhow::Result<Self> {
        let core = &json["market_params"]["core_params"];
        let limits = &json["market_params"]["limits_params"];
        let state = &json["market_state"];
        let book = &json["orderbook"];
        Ok(Self {
            paused: integer(&json["paused"]).context("paused")?,
            lot_size: integer(&core["lot_size"]).context("lot_size")?,
            tick_size: integer(&core["tick_size"]).context("tick_size")?,
            min_order_usd: fixed(&limits["min_order_usd_value"]).context("min_order_usd_value")?,
            max_pending_orders: integer(&limits["max_pending_orders"])
                .context("max_pending_orders")?,
            margin_ratio_initial: fixed(&core["margin_ratio_initial"])
                .context("margin_ratio_initial")?,
            margin_ratio_maintenance: fixed(&core["margin_ratio_maintenance"])
                .context("margin_ratio_maintenance")?,
            collateral_haircut: fixed(&core["collateral_haircut"]).context("collateral_haircut")?,
            cum_funding_rate_long: fixed(&state["cum_funding_rate_long"])
                .context("cum_funding_rate_long")?,
            cum_funding_rate_short: fixed(&state["cum_funding_rate_short"])
                .context("cum_funding_rate_short")?,
            best_bid: integer(&book["best_bid_price"]).ok().filter(|p| *p > 0),
            best_ask: integer(&book["best_ask_price"]).ok().filter(|p| *p > 0),
            base_source_id: integer(&core["base_source_id"]).unwrap_or(0),
            collateral_source_id: integer(&core["collateral_source_id"]).unwrap_or(0),
        })
    }

    /// What the maker's position is valued against, at the mark the feed TWAP gives.
    pub fn valuation(&self, mark_price: BigDecimal, collateral_price: BigDecimal) -> Valuation {
        Valuation {
            mark_price,
            collateral_price,
            collateral_haircut: self.collateral_haircut.clone(),
            cum_funding_rate_long: self.cum_funding_rate_long.clone(),
            cum_funding_rate_short: self.cum_funding_rate_short.clone(),
            margin_ratio_initial: self.margin_ratio_initial.clone(),
        }
    }
}

/// A feed's TWAP, 18 decimals, from its storage object.
pub fn feed_twap(json: &Value, source_id: u64) -> anyhow::Result<BigDecimal> {
    let feeds = json["feeds"]
        .as_array()
        .context("the storage has no feeds")?;
    for feed in feeds {
        let id = integer(&feed["source_id"]).or_else(|_| integer(&feed["id"]));
        if feeds.len() == 1 || id.is_ok_and(|id| id == source_id) {
            return fixed(&feed["twap_price"]).context("twap_price");
        }
    }
    bail!("the storage has no feed of source {source_id}")
}

/// The maker's position on the market, or None while it has no position object there.
pub async fn position(
    chain: &Chain,
    clearing_house: Address,
    types_package: Address,
    account_id: u64,
) -> anyhow::Result<Option<Position>> {
    let id = position::field_id(clearing_house, types_package, account_id);
    match chain.object(id).await {
        Ok(object) => Ok(Some(position::parse(&object.json)?)),
        Err(e) if e.to_string().contains("NotFound") || e.to_string().contains("not found") => {
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

/// Margin over maintenance requirement; None when the position needs none.
pub fn health(position: &Position, valuation: &Valuation, mmr: &BigDecimal) -> Option<BigDecimal> {
    let maintenance = position.maintenance_requirement(&valuation.mark_price, mmr);
    if maintenance.is_zero() {
        return None;
    }
    let margin = position.margin(valuation).margin;
    Some((margin / maintenance).with_scale(4))
}

fn integer(value: &Value) -> anyhow::Result<u64> {
    match value {
        Value::Number(n) => n.as_u64().context("not a u64"),
        Value::String(s) => s.parse().context("not an integer"),
        other => bail!("not an integer: {other}"),
    }
}

/// An `ifixed` field as a decimal.
fn fixed(value: &Value) -> anyhow::Result<BigDecimal> {
    let text = match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        other => bail!("not a number: {other}"),
    };
    let unsigned: BigUint = text.parse().context("not an integer")?;
    Ok(ifixed_to_decimal(unsigned))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn reads_the_clearing_house_as_the_node_renders_it() {
        let json: Value = serde_json::from_str(
            r#"{"paused":0,"market_params":{"core_params":{"lot_size":"1000000","tick_size":"1000000000",
                "margin_ratio_initial":"100000000000000000","margin_ratio_maintenance":"50000000000000000",
                "collateral_haircut":"0"},
                "limits_params":{"min_order_usd_value":"10000000000000000000","max_pending_orders":100}},
                "market_state":{"cum_funding_rate_long":"0","cum_funding_rate_short":"115792089237316195423570985008687907853269984665640564039457484007913129639936"},
                "orderbook":{"best_bid_price":"85225000000000","best_ask_price":"85265000000000"}}"#,
        )
        .unwrap();
        let m = MarketView::from_json(&json).unwrap();
        assert!(m.trades());
        assert_eq!(m.lot_size, 1_000_000);
        assert_eq!(m.tick_size, 1_000_000_000);
        assert_eq!(m.min_order_usd, BigDecimal::from(10));
        assert_eq!(m.max_pending_orders, 100);
        assert_eq!(m.margin_ratio_initial, BigDecimal::from_str("0.1").unwrap());
        // 2^256 − 1e17 is −0.1.
        assert_eq!(
            m.cum_funding_rate_short,
            BigDecimal::from_str("-0.1").unwrap()
        );
        assert_eq!(m.best_bid, Some(85_225_000_000_000));
        assert_eq!(m.best_ask, Some(85_265_000_000_000));
    }

    #[test]
    fn reads_the_feeds_twap() {
        let json: Value = serde_json::from_str(
            r#"{"feeds":[{"source_id":0,"price":"85474490000000000000000","twap_price":"85469952213956273365658"}]}"#,
        )
        .unwrap();
        let twap = feed_twap(&json, 0).unwrap();
        assert_eq!(
            twap.with_scale(2),
            BigDecimal::from_str("85469.95").unwrap()
        );
    }
}
