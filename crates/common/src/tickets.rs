// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The details behind stop and TWAP order tickets. A ticket holds only a blake2b-256 commitment
//! to its details; the executor reveals them when it executes the ticket and the engine hashes
//! them again, so these encodings follow `perpetuals_orders::stop_orders` and
//! `perpetuals_orders::twap_orders` byte for byte. The front end commits with the same encoding
//! (`src/haneul/perp/tickets.ts` in perp-web), and the tests pin both to the same hashes.

use haneul_sdk_types::{Address, hash::Hasher};
use serde::Serialize;

/// `perpetuals::account::IntegratorInfo`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct IntegratorInfo {
    pub integrator_id: u32,
    pub integrator_fee: u32,
}

/// The details of a stop loss / take profit ticket (ticket type 0), checked by
/// `stop_orders::place_stop_order_sltp`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StopLossTakeProfit {
    pub clearing_house: Address,
    pub expire_timestamp_ms: Option<u64>,
    pub is_limit_order: bool,
    /// 0: index price, 1: book price (the index when the book is empty), 2: mark price.
    pub trigger_price_type: u8,
    /// `ifixed` prices as the 32 little-endian bytes of a `u256`.
    pub stop_loss_price: Option<[u8; 32]>,
    pub take_profit_price: Option<[u8; 32]>,
    pub position_is_ask: bool,
    pub size: u64,
    pub price: u64,
    pub order_type: u64,
    pub integrator_info: Option<IntegratorInfo>,
    pub salt: Vec<u8>,
}

impl StopLossTakeProfit {
    pub fn commitment(&self) -> [u8; 32] {
        let fields = (
            &self.clearing_house,
            &self.expire_timestamp_ms,
            &self.is_limit_order,
            &self.trigger_price_type,
            &self.stop_loss_price,
            &self.take_profit_price,
            &self.position_is_ask,
            &self.size,
            &self.price,
            &self.order_type,
            &self.integrator_info,
        );
        commit_with_raw_salt(&fields, &self.salt)
    }
}

/// The details of a standalone stop ticket (ticket type 1), checked by
/// `stop_orders::place_stop_order_standalone`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StandaloneStop {
    pub clearing_house: Address,
    pub expire_timestamp_ms: Option<u64>,
    pub is_limit_order: bool,
    /// 0: index price, 1: book price (the index when the book is empty), 2: mark price.
    pub trigger_price_type: u8,
    /// `ifixed` as the 32 little-endian bytes of a `u256`.
    pub stop_index_price: [u8; 32],
    /// Triggers when the price is at or above `stop_index_price`; otherwise at or below.
    pub ge_stop_index_price: bool,
    /// The engine's side flag: `true` is an ask.
    pub side: bool,
    pub size: u64,
    pub price: u64,
    pub order_type: u64,
    pub reduce_only: bool,
    pub integrator_info: Option<IntegratorInfo>,
    pub salt: Vec<u8>,
}

impl StandaloneStop {
    pub fn commitment(&self) -> [u8; 32] {
        let fields = (
            &self.clearing_house,
            &self.expire_timestamp_ms,
            &self.is_limit_order,
            &self.trigger_price_type,
            &self.stop_index_price,
            &self.ge_stop_index_price,
            &self.side,
            &self.size,
            &self.price,
            &self.order_type,
            &self.reduce_only,
            &self.integrator_info,
        );
        commit_with_raw_salt(&fields, &self.salt)
    }
}

/// `twap_orders::TWAPOrderDetails`. Unlike the stop tickets, the salt is a field of the struct,
/// so it is hashed with its length prefix.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TwapDetails {
    pub first_run_expire_timestamp_ms: Option<u64>,
    pub expire_timestamp_ms: Option<u64>,
    pub execution_gap_ms: u64,
    pub execution_time_uncertainty_ms: u64,
    pub chunks_amount: u64,
    pub small_tail_merge_threshold_bps: u64,
    pub time_for_retry_ms: u64,
    pub amount_uncertainty_bps: u64,
    pub max_one_execution_amount_bps: u64,
    /// The engine's side flag: `true` is an ask.
    pub side: bool,
    pub size: u64,
    pub max_slippage_bps: u64,
    pub reduce_only: bool,
    pub integrator_info: Option<IntegratorInfo>,
    pub salt: Vec<u8>,
}

impl TwapDetails {
    pub fn commitment(&self) -> [u8; 32] {
        Hasher::digest(bcs::to_bytes(self).expect("TWAP details serialize")).into_inner()
    }
}

/// The stop tickets commit to the BCS of each field in order followed by the salt's bare bytes.
fn commit_with_raw_salt(fields: &impl Serialize, salt: &[u8]) -> [u8; 32] {
    let mut bytes = bcs::to_bytes(fields).expect("stop ticket details serialize");
    bytes.extend_from_slice(salt);
    Hasher::digest(bytes).into_inner()
}

#[cfg(test)]
mod tests {
    use super::*;

    // The mainnet BTC-USD clearing house; the hashes below are also pinned by perp-web's
    // `tickets.spec.ts`, so a change on either side breaks one of the two suites.
    const CLEARING_HOUSE: &str =
        "0xf77dda417d4003c017c96697809d3a25b2767aca586b5b7579e975e47508df77";

    fn usd(dollars: u128) -> [u8; 32] {
        let mut bytes = [0; 32];
        bytes[..16].copy_from_slice(&(dollars * 10u128.pow(18)).to_le_bytes());
        bytes
    }

    fn salt() -> Vec<u8> {
        (0..32).collect()
    }

    #[test]
    fn stop_loss_and_take_profit_on_a_long() {
        let details = StopLossTakeProfit {
            clearing_house: Address::from_static(CLEARING_HOUSE),
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
            salt: salt(),
        };
        assert_eq!(
            hex::encode(details.commitment()),
            "330487179a9f27ca0f9adf7fca5bb677b5484ded8aaecabf9cc5ca5fe1e372b8"
        );
    }

    #[test]
    fn take_profit_only_limit_close_of_a_short() {
        let details = StopLossTakeProfit {
            clearing_house: Address::from_static(CLEARING_HOUSE),
            expire_timestamp_ms: Some(1_800_000_000_000),
            is_limit_order: true,
            trigger_price_type: 2,
            stop_loss_price: None,
            take_profit_price: Some(usd(90_000)),
            position_is_ask: true,
            size: 100_000_000,
            price: 90_000_000_000,
            order_type: 1,
            integrator_info: None,
            salt: salt(),
        };
        assert_eq!(
            hex::encode(details.commitment()),
            "b3391bdd5582bb2db8e7237f6c0b5805a78399feb0c33d7c1e21842bbd19a689"
        );
    }

    #[test]
    fn standalone_stop_on_the_book_price() {
        let details = StandaloneStop {
            clearing_house: Address::from_static(CLEARING_HOUSE),
            expire_timestamp_ms: Some(1_800_000_000_000),
            is_limit_order: true,
            trigger_price_type: 1,
            stop_index_price: usd(101_000),
            ge_stop_index_price: true,
            side: false,
            size: 100_000_000,
            price: 101_500_000_000,
            order_type: 0,
            reduce_only: true,
            integrator_info: None,
            salt: salt(),
        };
        assert_eq!(
            hex::encode(details.commitment()),
            "17c0fe7d24464e1d3df5dcf1cf54c9534e5044e6284a9ee22272a5e6c3f229ec"
        );
    }

    #[test]
    fn twap_in_four_chunks() {
        let details = TwapDetails {
            first_run_expire_timestamp_ms: Some(1_800_000_060_000),
            expire_timestamp_ms: None,
            execution_gap_ms: 60_000,
            execution_time_uncertainty_ms: 10_000,
            chunks_amount: 4,
            small_tail_merge_threshold_bps: 0,
            time_for_retry_ms: 30_000,
            amount_uncertainty_bps: 0,
            max_one_execution_amount_bps: 2_500,
            side: false,
            size: 1_000_000_000,
            max_slippage_bps: 100,
            reduce_only: false,
            integrator_info: None,
            salt: salt(),
        };
        assert_eq!(
            hex::encode(details.commitment()),
            "c132d3da096c43c38b2c96ad1f4bc9edac7441c1a272dd3f0d7bb927596539ec"
        );
    }
}
