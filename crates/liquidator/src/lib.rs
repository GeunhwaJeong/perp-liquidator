// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! A liquidation bot for the Haneul perpetuals engine.

// The parts every bot shares, under the paths this crate has always used.
pub use perp_bot_common::{alerts, chain, keys, oracle, store};

pub mod aborts;
pub mod adl;
pub mod config;
pub mod liquidator;
pub mod metrics;
pub mod ptb;
pub mod report;
pub mod risk;
pub mod setup;
pub mod status;
