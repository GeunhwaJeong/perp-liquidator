// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The keeper of the stop and TWAP order tickets. A ticket holds only a commitment to its
//! details, so the front end hands the details to the keeper before it asks for the ticket's
//! transaction to be signed; the keeper checks that they hash to the commitment, keeps them in
//! a database of its own and follows the indexer to see which tickets took them up.

pub mod config;
pub mod details;
pub mod matcher;
pub mod server;
pub mod store;

use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}
