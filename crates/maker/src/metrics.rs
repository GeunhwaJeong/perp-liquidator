// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The maker's metrics, served on `/metrics`.

use prometheus::{
    Encoder, Gauge, GaugeVec, IntCounter, IntCounterVec, IntGauge, Opts, Registry, TextEncoder,
};

pub struct Metrics {
    registry: Registry,
    /// Rounds by result: executed, simulated, refused, rejected, unknown, failed, skipped.
    pub rounds: IntCounterVec,
    /// Why rounds were sent: moved, refresh, fills, shape, pull, flatten.
    pub reasons: IntCounterVec,
    pub quotes: GaugeVec,
    pub half_spread_bps: Gauge,
    pub skew_bps: Gauge,
    pub sigma_bps: Gauge,
    pub reference: Gauge,
    pub position: Gauge,
    pub health: Gauge,
    pub quoting: IntGauge,
    pub price_age: Gauge,
    pub fills: IntCounter,
    pub gas_spent: IntCounter,
    pub gas_balance: Gauge,
    pub flow_trades: IntCounterVec,
    pub flow_position: Gauge,
    pub alerts: IntCounterVec,
}

fn opts(name: &str, help: &str) -> Opts {
    Opts::new(name, help).namespace("perp_maker")
}

macro_rules! register {
    ($registry:expr, $metric:expr) => {{
        let metric = $metric?;
        $registry.register(Box::new(metric.clone()))?;
        metric
    }};
}

impl Metrics {
    pub fn new() -> anyhow::Result<Self> {
        let registry = Registry::new();
        Ok(Self {
            rounds: register!(
                registry,
                IntCounterVec::new(opts("rounds_total", "Rounds, by result"), &["result"])
            ),
            reasons: register!(
                registry,
                IntCounterVec::new(
                    opts("round_reasons_total", "Why rounds were sent"),
                    &["reason"]
                )
            ),
            quotes: register!(
                registry,
                GaugeVec::new(opts("quotes", "Levels resting, by side"), &["side"])
            ),
            half_spread_bps: register!(
                registry,
                Gauge::with_opts(opts("half_spread_bps", "The half-spread quoted"))
            ),
            skew_bps: register!(
                registry,
                Gauge::with_opts(opts("skew_bps", "The inventory skew of the mid"))
            ),
            sigma_bps: register!(
                registry,
                Gauge::with_opts(opts(
                    "sigma_bps",
                    "Estimated one-round motion of the reference"
                ))
            ),
            reference: register!(
                registry,
                Gauge::with_opts(opts("reference_price", "The reference price quoted around"))
            ),
            position: register!(
                registry,
                Gauge::with_opts(opts("position", "The maker's position, signed base units"))
            ),
            health: register!(
                registry,
                Gauge::with_opts(opts("margin_health", "Margin over maintenance requirement"))
            ),
            quoting: register!(
                registry,
                IntGauge::with_opts(opts("quoting", "1 while quotes rest"))
            ),
            price_age: register!(
                registry,
                Gauge::with_opts(opts("price_age_seconds", "Age of the signed price used"))
            ),
            fills: register!(
                registry,
                IntCounter::with_opts(opts(
                    "fills_total",
                    "Rounds that found the position changed"
                ))
            ),
            gas_spent: register!(
                registry,
                IntCounter::with_opts(opts("gas_spent_total", "Gas spent, in the smallest unit"))
            ),
            gas_balance: register!(
                registry,
                Gauge::with_opts(opts("gas_balance", "HANEUL held by the maker's address"))
            ),
            flow_trades: register!(
                registry,
                IntCounterVec::new(
                    opts("flow_trades_total", "Simulator trades, by result"),
                    &["result"]
                )
            ),
            flow_position: register!(
                registry,
                Gauge::with_opts(opts("flow_position", "The simulator's position"))
            ),
            alerts: register!(
                registry,
                IntCounterVec::new(opts("alerts_total", "Alerts raised, by level"), &["level"])
            ),
            registry,
        })
    }

    pub fn exposition(&self) -> String {
        let mut buffer = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut buffer)
            .expect("text encoding of metrics");
        String::from_utf8(buffer).expect("metrics are UTF-8")
    }
}
