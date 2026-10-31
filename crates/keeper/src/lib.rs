// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The keeper of the stop and TWAP order tickets. A ticket holds only a commitment to its
//! details, so the front end hands the details to the keeper before it asks for the ticket's
//! transaction to be signed; the keeper checks that they hash to the commitment, keeps them in
//! a database of its own and follows the indexer to see which tickets took them up. Given a
//! deployment, a full node and a key, it also executes the stop tickets that name it once
//! their trigger prices cross.

pub mod config;
pub mod details;
pub mod executor;
pub mod matcher;
pub mod metrics;
pub mod ptb;
pub mod server;
pub mod setup;
pub mod status;
pub mod store;
pub mod trigger;

use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}
