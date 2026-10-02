// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! A funding cranker for the Haneul perpetuals engine: keeps the funding and the TWAPs of
//! markets nobody trades on schedule.

pub mod config;
pub mod cranker;
pub mod errors;
pub mod metrics;
pub mod ptb;
pub mod schedule;
pub mod setup;
pub mod status;
