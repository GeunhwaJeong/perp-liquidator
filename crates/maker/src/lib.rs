// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! A market maker for the Haneul perpetuals engine, and the flow simulator of the shadow run.
//! `docs/perp-maker-design.md` is the design.

pub mod config;
pub mod flow;
pub mod maker;
pub mod market;
pub mod metrics;
pub mod model;
pub mod ptb;
pub mod setup;
pub mod status;
