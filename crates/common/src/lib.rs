// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! What the perpetuals bots share: the full node (reading, simulating, executing), keys, the
//! deployment file, the indexer's database, signed prices and alerts.

pub mod alerts;
pub mod chain;
pub mod deployment;
pub mod keys;
pub mod oracle;
pub mod position;
pub mod store;
