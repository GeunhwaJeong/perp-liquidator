// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! What a liquidator transaction did, read from its events.

use bigdecimal::BigDecimal;
use haneul_sdk_types::Address;
use perp_types::perpetuals::Event;
use perp_types::types::U256;
use serde::Serialize;

/// Amounts in USD and base sizes are decimals; signs as the engine gives them.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct Report {
    /// Base taken over from liquidated accounts.
    pub base_liquidated: BigDecimal,
    /// What it was taken over at (base times mark price).
    pub quote_liquidated: BigDecimal,
    /// Paid to the liquidator.
    pub liquidation_fees: BigDecimal,
    pub insurance_fund_fees: BigDecimal,
    /// Left by the liquidated account, covered by the insurance fund or socialized.
    pub bad_debt: BigDecimal,
    pub socialized: BigDecimal,
    /// Base the liquidator's own orders traded, both sides together.
    pub base_traded: BigDecimal,
    pub taker_pnl: BigDecimal,
    pub taker_fees: BigDecimal,
    pub adl_counterparties: usize,
}

impl Report {
    /// The liquidator's take: liquidation fees plus what unwinding made, less trading fees.
    pub fn profit(&self) -> BigDecimal {
        &self.liquidation_fees + &self.taker_pnl - &self.taker_fees
    }
}

/// Reads the events of the engine (published at `types_package`) that concern the liquidator's
/// account `me`.
pub fn read(events: &[(String, Vec<u8>)], types_package: &Address, me: u64) -> Report {
    let mut report = Report::default();
    for (event_type, bcs) in events {
        let Some(name) = engine_event(event_type, types_package) else {
            continue;
        };
        let Ok(event) = Event::decode(name, bcs) else {
            continue;
        };
        match event {
            Event::LiquidatedPosition(e) if e.liqor_account_id == me => {
                report.base_liquidated += ifixed(&e.base_liquidated);
                report.quote_liquidated += ifixed(&e.quote_liquidated);
                report.liquidation_fees += ifixed(&e.liquidation_fees);
                report.insurance_fund_fees += ifixed(&e.insurance_fund_fees);
                report.bad_debt += ifixed(&e.bad_debt);
            }
            Event::FilledTakerOrder(e) if e.taker_account_id == me => {
                report.base_traded +=
                    ifixed(&e.base_asset_delta_ask) + ifixed(&e.base_asset_delta_bid);
                report.taker_pnl += ifixed(&e.taker_pnl);
                report.taker_fees += ifixed(&e.taker_fees);
            }
            Event::SocializedBadDebt(e) => report.socialized += ifixed(&e.bad_debt_usd),
            Event::PerformedADL(_) => report.adl_counterparties += 1,
            _ => {}
        }
    }
    report
}

/// The struct name of an event of the engine's `events` module.
fn engine_event<'a>(event_type: &'a str, types_package: &Address) -> Option<&'a str> {
    let mut parts = event_type.splitn(3, "::");
    let (package, module, name) = (parts.next()?, parts.next()?, parts.next()?);
    let package: Address = package.parse().ok()?;
    (package == *types_package && module == "events" && !name.contains('<')).then_some(name)
}

fn ifixed(value: &U256) -> BigDecimal {
    BigDecimal::new(value.to_ifixed(), 18)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use bigdecimal::Zero;
    use num_bigint::BigInt;
    use perp_types::perpetuals::{FilledTakerOrder, LiquidatedPosition};
    use perp_types::types::Address as MoveAddress;

    use super::*;

    fn fx(s: &str) -> U256 {
        let value = (BigDecimal::from_str(s).unwrap() * BigDecimal::from(10u64.pow(18)))
            .with_scale(0)
            .into_bigint_and_exponent()
            .0;
        let twos = if value.sign() == num_bigint::Sign::Minus {
            (BigInt::from(1) << 256) + value
        } else {
            value
        };
        let mut bytes = twos.to_biguint().unwrap().to_bytes_le();
        bytes.resize(32, 0);
        U256(bytes.try_into().unwrap())
    }

    fn id() -> MoveAddress {
        MoveAddress([0xc0; 32])
    }

    #[test]
    fn reads_what_the_liquidator_took_over_and_sold() {
        let package = Address::from_static("0xe1");
        let liquidated = LiquidatedPosition {
            ch_id: id(),
            liqee_account_id: 4,
            liqor_account_id: 9,
            is_liqee_long: true,
            base_liquidated: fx("0.3"),
            quote_liquidated: fx("28200"),
            liqee_pnl: fx("-1800"),
            liquidation_fees: fx("282"),
            insurance_fund_fees: fx("141"),
            bad_debt: fx("0"),
            mark_price: fx("94000"),
        };
        let sold = FilledTakerOrder {
            ch_id: id(),
            taker_account_id: 9,
            taker_pnl: fx("-150"),
            taker_fees: fx("14.1"),
            integrator_id: None,
            integrator_fee_paid_usd: fx("0"),
            base_asset_delta_ask: fx("0.3"),
            quote_asset_delta_ask: fx("28050"),
            base_asset_delta_bid: fx("0"),
            quote_asset_delta_bid: fx("0"),
            mark_price: fx("94000"),
        };
        let someone_else = FilledTakerOrder {
            taker_account_id: 5,
            ..sold.clone()
        };
        let ty = |name: &str| format!("{package}::events::{name}");
        let events = vec![
            (
                ty("LiquidatedPosition"),
                bcs::to_bytes(&liquidated).unwrap(),
            ),
            (ty("FilledTakerOrder"), bcs::to_bytes(&sold).unwrap()),
            (
                ty("FilledTakerOrder"),
                bcs::to_bytes(&someone_else).unwrap(),
            ),
            // Another package's event of the same name is not the engine's.
            (
                format!(
                    "{}::events::FilledTakerOrder",
                    Address::from_static("0xbad")
                ),
                bcs::to_bytes(&sold).unwrap(),
            ),
        ];
        let report = read(&events, &package, 9);
        assert_eq!(report.base_liquidated, BigDecimal::from_str("0.3").unwrap());
        assert_eq!(
            report.liquidation_fees,
            BigDecimal::from_str("282").unwrap()
        );
        assert_eq!(report.base_traded, BigDecimal::from_str("0.3").unwrap());
        assert_eq!(report.profit(), BigDecimal::from_str("117.9").unwrap());
        assert!(report.bad_debt.is_zero());
    }
}
