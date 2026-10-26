// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Ticket details as the front end hands them in: the fields of `tickets.ts`'s detail types in
//! camel case, with integers as decimal strings (a `u64` or an `ifixed` does not survive a JSON
//! number) and the salt as hex. The front end never sets an integrator, so none is taken.

use anyhow::{Context, ensure};
use haneul_sdk_types::Address;
use num_bigint::BigUint;
use perp_bot_common::tickets::{StandaloneStop, StopLossTakeProfit, TwapDetails};
use serde::{Deserialize, Serialize};

/// The front end's salts are 32 bytes; a much longer one is not from it.
const MAX_SALT_BYTES: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "camelCase")]
pub enum Submission {
    StopLossTakeProfit(StopLossTakeProfitJson),
    StandaloneStop(StandaloneStopJson),
    Twap(TwapJson),
}

impl Submission {
    /// The commitment the details hash to, once they check out.
    pub fn commitment(&self) -> anyhow::Result<[u8; 32]> {
        Ok(match self {
            Self::StopLossTakeProfit(d) => d.to_ticket()?.commitment(),
            Self::StandaloneStop(d) => d.to_ticket()?.commitment(),
            Self::Twap(d) => d.to_ticket()?.commitment(),
        })
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::StopLossTakeProfit(_) => "stop_loss_take_profit",
            Self::StandaloneStop(_) => "standalone_stop",
            Self::Twap(_) => "twap",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StopLossTakeProfitJson {
    pub clearing_house: Address,
    pub expire_timestamp_ms: Option<String>,
    pub is_limit_order: bool,
    pub trigger_price_type: u8,
    pub stop_loss_price: Option<String>,
    pub take_profit_price: Option<String>,
    pub position_is_ask: bool,
    pub size: String,
    pub price: String,
    pub order_type: String,
    pub salt: String,
}

impl StopLossTakeProfitJson {
    pub fn to_ticket(&self) -> anyhow::Result<StopLossTakeProfit> {
        check_trigger(self.trigger_price_type)?;
        ensure!(
            self.stop_loss_price.is_some() || self.take_profit_price.is_some(),
            "neither stopLossPrice nor takeProfitPrice is set"
        );
        let size = u64_of("size", &self.size)?;
        ensure!(size > 0, "size is 0");
        Ok(StopLossTakeProfit {
            clearing_house: self.clearing_house,
            expire_timestamp_ms: optional_u64("expireTimestampMs", &self.expire_timestamp_ms)?,
            is_limit_order: self.is_limit_order,
            trigger_price_type: self.trigger_price_type,
            stop_loss_price: self
                .stop_loss_price
                .as_deref()
                .map(|text| ifixed_of("stopLossPrice", text))
                .transpose()?,
            take_profit_price: self
                .take_profit_price
                .as_deref()
                .map(|text| ifixed_of("takeProfitPrice", text))
                .transpose()?,
            position_is_ask: self.position_is_ask,
            size,
            price: u64_of("price", &self.price)?,
            order_type: u64_of("orderType", &self.order_type)?,
            integrator_info: None,
            salt: salt_of(&self.salt)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StandaloneStopJson {
    pub clearing_house: Address,
    pub expire_timestamp_ms: Option<String>,
    pub is_limit_order: bool,
    pub trigger_price_type: u8,
    pub stop_index_price: String,
    pub trigger_at_or_above: bool,
    pub side: bool,
    pub size: String,
    pub price: String,
    pub order_type: String,
    pub reduce_only: bool,
    pub salt: String,
}

impl StandaloneStopJson {
    pub fn to_ticket(&self) -> anyhow::Result<StandaloneStop> {
        check_trigger(self.trigger_price_type)?;
        let size = u64_of("size", &self.size)?;
        ensure!(size > 0, "size is 0");
        Ok(StandaloneStop {
            clearing_house: self.clearing_house,
            expire_timestamp_ms: optional_u64("expireTimestampMs", &self.expire_timestamp_ms)?,
            is_limit_order: self.is_limit_order,
            trigger_price_type: self.trigger_price_type,
            stop_index_price: ifixed_of("stopIndexPrice", &self.stop_index_price)?,
            ge_stop_index_price: self.trigger_at_or_above,
            side: self.side,
            size,
            price: u64_of("price", &self.price)?,
            order_type: u64_of("orderType", &self.order_type)?,
            reduce_only: self.reduce_only,
            integrator_info: None,
            salt: salt_of(&self.salt)?,
        })
    }
}

/// A TWAP's details name no market: its ticket holds the clearing house.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TwapJson {
    pub first_run_expire_timestamp_ms: Option<String>,
    pub expire_timestamp_ms: Option<String>,
    pub execution_gap_ms: String,
    pub execution_time_uncertainty_ms: String,
    pub chunks_amount: String,
    pub small_tail_merge_threshold_bps: String,
    pub time_for_retry_ms: String,
    pub amount_uncertainty_bps: String,
    pub max_one_execution_amount_bps: String,
    pub side: bool,
    pub size: String,
    pub max_slippage_bps: String,
    pub reduce_only: bool,
    pub salt: String,
}

impl TwapJson {
    pub fn to_ticket(&self) -> anyhow::Result<TwapDetails> {
        let size = u64_of("size", &self.size)?;
        let chunks_amount = u64_of("chunksAmount", &self.chunks_amount)?;
        // `twap_orders::new_details` refuses these; the keeper could never execute them.
        ensure!(
            chunks_amount != 0 && size >= chunks_amount,
            "size {size} cannot be split into {chunks_amount} chunks"
        );
        Ok(TwapDetails {
            first_run_expire_timestamp_ms: optional_u64(
                "firstRunExpireTimestampMs",
                &self.first_run_expire_timestamp_ms,
            )?,
            expire_timestamp_ms: optional_u64("expireTimestampMs", &self.expire_timestamp_ms)?,
            execution_gap_ms: u64_of("executionGapMs", &self.execution_gap_ms)?,
            execution_time_uncertainty_ms: u64_of(
                "executionTimeUncertaintyMs",
                &self.execution_time_uncertainty_ms,
            )?,
            chunks_amount,
            small_tail_merge_threshold_bps: u64_of(
                "smallTailMergeThresholdBps",
                &self.small_tail_merge_threshold_bps,
            )?,
            time_for_retry_ms: u64_of("timeForRetryMs", &self.time_for_retry_ms)?,
            amount_uncertainty_bps: u64_of("amountUncertaintyBps", &self.amount_uncertainty_bps)?,
            max_one_execution_amount_bps: u64_of(
                "maxOneExecutionAmountBps",
                &self.max_one_execution_amount_bps,
            )?,
            side: self.side,
            size,
            max_slippage_bps: u64_of("maxSlippageBps", &self.max_slippage_bps)?,
            reduce_only: self.reduce_only,
            integrator_info: None,
            salt: salt_of(&self.salt)?,
        })
    }
}

fn check_trigger(trigger_price_type: u8) -> anyhow::Result<()> {
    ensure!(
        trigger_price_type < 3,
        "triggerPriceType {trigger_price_type} is not 0 (index), 1 (book) or 2 (mark)"
    );
    Ok(())
}

fn u64_of(name: &str, text: &str) -> anyhow::Result<u64> {
    text.parse()
        .with_context(|| format!("{name} is not a u64: {text:?}"))
}

fn optional_u64(name: &str, text: &Option<String>) -> anyhow::Result<Option<u64>> {
    text.as_deref().map(|text| u64_of(name, text)).transpose()
}

/// An `ifixed` as the 32 little-endian bytes of its `u256`. Only non-negative ones: a negative
/// `ifixed` has the top bit set, and no price is negative.
fn ifixed_of(name: &str, text: &str) -> anyhow::Result<[u8; 32]> {
    let value: BigUint = text
        .parse()
        .with_context(|| format!("{name} is not an unsigned integer: {text:?}"))?;
    ensure!(
        value.bits() < 256,
        "{name} does not fit a non-negative ifixed: {text}"
    );
    let mut bytes = [0; 32];
    let little_endian = value.to_bytes_le();
    bytes[..little_endian.len()].copy_from_slice(&little_endian);
    Ok(bytes)
}

fn salt_of(text: &str) -> anyhow::Result<Vec<u8>> {
    let salt = hex::decode(text.strip_prefix("0x").unwrap_or(text)).context("salt is not hex")?;
    ensure!(
        !salt.is_empty() && salt.len() <= MAX_SALT_BYTES,
        "salt is {} bytes, not 1 to {MAX_SALT_BYTES}",
        salt.len()
    );
    Ok(salt)
}

#[cfg(test)]
mod tests {
    use super::*;

    // The same details and hashes as `perp_bot_common::tickets` and perp-web's
    // `tickets.spec.ts`, written as the front end sends them.
    const SALT: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

    fn submission(json: serde_json::Value) -> Submission {
        serde_json::from_value(json).expect("a submission")
    }

    fn stop_loss_take_profit() -> serde_json::Value {
        serde_json::json!({
            "kind": "stopLossTakeProfit",
            "details": {
                "clearingHouse": "0xf77dda417d4003c017c96697809d3a25b2767aca586b5b7579e975e47508df77",
                "isLimitOrder": false,
                "triggerPriceType": 0,
                "stopLossPrice": "95000000000000000000000",
                "takeProfitPrice": "105000000000000000000000",
                "positionIsAsk": false,
                "size": "250000000",
                "price": "0",
                "orderType": "0",
                "salt": SALT,
            },
        })
    }

    fn twap() -> serde_json::Value {
        serde_json::json!({
            "kind": "twap",
            "details": {
                "firstRunExpireTimestampMs": "1800000060000",
                "executionGapMs": "60000",
                "executionTimeUncertaintyMs": "10000",
                "chunksAmount": "4",
                "smallTailMergeThresholdBps": "0",
                "timeForRetryMs": "30000",
                "amountUncertaintyBps": "0",
                "maxOneExecutionAmountBps": "2500",
                "side": false,
                "size": "1000000000",
                "maxSlippageBps": "100",
                "reduceOnly": false,
                "salt": SALT,
            },
        })
    }

    fn commitment(json: serde_json::Value) -> String {
        hex::encode(
            submission(json)
                .commitment()
                .expect("details that check out"),
        )
    }

    fn refusal(json: serde_json::Value) -> String {
        format!("{:#}", submission(json).commitment().unwrap_err())
    }

    #[test]
    fn hashes_what_the_front_end_sends_as_it_commits() {
        assert_eq!(
            commitment(stop_loss_take_profit()),
            "330487179a9f27ca0f9adf7fca5bb677b5484ded8aaecabf9cc5ca5fe1e372b8"
        );
        assert_eq!(
            commitment(serde_json::json!({
                "kind": "stopLossTakeProfit",
                "details": {
                    "clearingHouse": "0xf77dda417d4003c017c96697809d3a25b2767aca586b5b7579e975e47508df77",
                    "expireTimestampMs": "1800000000000",
                    "isLimitOrder": true,
                    "triggerPriceType": 2,
                    "takeProfitPrice": "90000000000000000000000",
                    "positionIsAsk": true,
                    "size": "100000000",
                    "price": "90000000000",
                    "orderType": "1",
                    "salt": SALT,
                },
            })),
            "b3391bdd5582bb2db8e7237f6c0b5805a78399feb0c33d7c1e21842bbd19a689"
        );
        assert_eq!(
            commitment(serde_json::json!({
                "kind": "standaloneStop",
                "details": {
                    "clearingHouse": "0xf77dda417d4003c017c96697809d3a25b2767aca586b5b7579e975e47508df77",
                    "expireTimestampMs": "1800000000000",
                    "isLimitOrder": true,
                    "triggerPriceType": 1,
                    "stopIndexPrice": "101000000000000000000000",
                    "triggerAtOrAbove": true,
                    "side": false,
                    "size": "100000000",
                    "price": "101500000000",
                    "orderType": "0",
                    "reduceOnly": true,
                    "salt": SALT,
                },
            })),
            "17c0fe7d24464e1d3df5dcf1cf54c9534e5044e6284a9ee22272a5e6c3f229ec"
        );
        assert_eq!(
            commitment(twap()),
            "c132d3da096c43c38b2c96ad1f4bc9edac7441c1a272dd3f0d7bb927596539ec"
        );
    }

    #[test]
    fn reads_back_what_it_stores() {
        for json in [stop_loss_take_profit(), twap()] {
            let original = submission(json);
            let stored = serde_json::to_string(&original).unwrap();
            assert_eq!(
                serde_json::from_str::<Submission>(&stored).unwrap(),
                original
            );
        }
    }

    #[test]
    fn refuses_details_no_ticket_could_be_executed_with() {
        let mut json = stop_loss_take_profit();
        json["details"]["triggerPriceType"] = 3.into();
        assert!(refusal(json).contains("triggerPriceType 3"));

        let mut json = stop_loss_take_profit();
        let details = json["details"].as_object_mut().unwrap();
        details.remove("stopLossPrice");
        details.remove("takeProfitPrice");
        assert!(refusal(json).contains("neither"));

        let mut json = stop_loss_take_profit();
        // 2^255: the sign bit of an ifixed.
        json["details"]["stopLossPrice"] =
            "57896044618658097711785492504343953926634992332820282019728792003956564819968".into();
        assert!(refusal(json).contains("non-negative ifixed"));

        let mut json = stop_loss_take_profit();
        json["details"]["size"] = "-1".into();
        assert!(refusal(json).contains("size is not a u64"));

        let mut json = stop_loss_take_profit();
        json["details"]["salt"] = "".into();
        assert!(refusal(json).contains("salt is 0 bytes"));

        let mut json = stop_loss_take_profit();
        json["details"]["salt"] = "zz".into();
        assert!(refusal(json).contains("salt is not hex"));

        let mut json = twap();
        json["details"]["chunksAmount"] = "2000000000".into();
        assert!(refusal(json).contains("cannot be split"));
    }

    #[test]
    fn refuses_fields_it_does_not_know() {
        let mut json = stop_loss_take_profit();
        json["details"]["integratorInfo"] = serde_json::json!({"integratorId": 1});
        assert!(serde_json::from_value::<Submission>(json).is_err());

        let mut json = stop_loss_take_profit();
        json["kind"] = "limit".into();
        assert!(serde_json::from_value::<Submission>(json).is_err());
    }
}
