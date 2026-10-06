// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! An account's position on a market, read from the chain: a dynamic field of the clearing
//! house keyed by `keys::PositionKey { account_id }`, whose value is `position::Position`.
//! The field's object ID is derived, so one object read gives the position without listing
//! the clearing house's fields.

use anyhow::Context;
use bigdecimal::BigDecimal;
use haneul_sdk_types::{Address, Identifier, StructTag, TypeTag};
use num_bigint::{BigInt, BigUint};
use perp_engine::Position;

/// The object ID of the account's position field on the clearing house, as the engine derives
/// it (`dynamic_field::add` with a `PositionKey` of the perpetuals package's original version).
pub fn field_id(clearing_house: Address, types_package: Address, account_id: u64) -> Address {
    let key_type = StructTag::new(
        types_package,
        Identifier::new("keys").expect("an identifier"),
        Identifier::new("PositionKey").expect("an identifier"),
        vec![],
    );
    let key_bytes = bcs::to_bytes(&account_id).expect("a u64 serializes");
    clearing_house.derive_dynamic_child_id(&TypeTag::Struct(Box::new(key_type)), &key_bytes)
}

/// Reads the field object's JSON (`{ id, name: { account_id }, value: { … } }`).
pub fn parse(json: &serde_json::Value) -> anyhow::Result<Position> {
    let value = json.get("value").context("the field has no value")?;
    let ifixed = |name: &str| -> anyhow::Result<BigDecimal> {
        let text = value
            .get(name)
            .and_then(|v| v.as_str())
            .with_context(|| format!("position has no {name}"))?;
        let unsigned: BigUint = text
            .parse()
            .with_context(|| format!("position {name} is not an integer: {text}"))?;
        Ok(ifixed_to_decimal(unsigned))
    };
    let pending_orders = value
        .get("pending_orders")
        .and_then(|v| {
            v.as_str()
                .map(str::to_owned)
                .or_else(|| v.as_u64().map(|n| n.to_string()))
        })
        .context("position has no pending_orders")?
        .parse::<i64>()
        .context("pending_orders")?;
    Ok(Position {
        collateral: ifixed("collateral")?,
        base: ifixed("base_asset_amount")?,
        quote_notional: ifixed("quote_asset_notional_amount")?,
        cum_funding_rate_long: ifixed("cum_funding_rate_long")?,
        cum_funding_rate_short: ifixed("cum_funding_rate_short")?,
        asks_quantity: ifixed("asks_quantity")?,
        bids_quantity: ifixed("bids_quantity")?,
        pending_orders,
        initial_margin_ratio: ifixed("initial_margin_ratio")?,
    })
}

/// The engine's `ifixed`: a two's-complement `u256` scaled by 10^18.
pub fn ifixed_to_decimal(unsigned: BigUint) -> BigDecimal {
    let half = BigUint::from(1u8) << 255;
    let signed = if unsigned >= half {
        BigInt::from(unsigned) - (BigInt::from(1) << 256)
    } else {
        BigInt::from(unsigned)
    };
    BigDecimal::new(signed, 18)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn derives_the_field_the_engine_wrote() {
        // The liquidator's position on the mainnet BTC-USD clearing house (account 1), as the
        // node lists it under the clearing house's dynamic fields.
        let clearing_house = Address::from_static(
            "0xf77dda417d4003c017c96697809d3a25b2767aca586b5b7579e975e47508df77",
        );
        let perpetuals = Address::from_static(
            "0x7be0828aefa3143b632c0267146ce441f7a63f745b02eddcf8f2c28687d21f6a",
        );
        assert_eq!(
            field_id(clearing_house, perpetuals, 1),
            Address::from_static(
                "0xa34022ebd512795d902d67ff98ea4fe1ea03dbcbbe4e9c45179397da8b70da65"
            )
        );
    }

    #[test]
    fn reads_signed_fixed_values() {
        let json: serde_json::Value = serde_json::from_str(
            r#"{"id":"0x1","name":{"account_id":"1"},"value":{
                "asks_quantity":"50000000000000000","base_asset_amount":"115792089237316195423570985008687907853269984665640564039457384007913129639936",
                "bids_quantity":"0","collateral":"400000000000000000000000",
                "cum_funding_rate_long":"0","cum_funding_rate_short":"0",
                "initial_margin_ratio":"100000000000000000","pending_orders":"3",
                "quote_asset_notional_amount":"0"}}"#,
        )
        .unwrap();
        let p = parse(&json).unwrap();
        // 2^256 - 2e17 is -0.2.
        assert_eq!(p.base, BigDecimal::from_str("-0.2").unwrap());
        assert_eq!(p.asks_quantity, BigDecimal::from_str("0.05").unwrap());
        assert_eq!(p.collateral, BigDecimal::from_str("400000").unwrap());
        assert_eq!(p.initial_margin_ratio, BigDecimal::from_str("0.1").unwrap());
        assert_eq!(p.pending_orders, 3);
    }
}
