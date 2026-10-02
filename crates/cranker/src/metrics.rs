// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The cranker's metrics, served on `/metrics`.

use prometheus::{
    Encoder, Gauge, GaugeVec, IntCounter, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder,
};

pub struct Metrics {
    registry: Registry,
    pub rounds: IntCounter,
    /// Cranks by market and result: executed, simulated, refused (would abort or failed to
    /// build), rejected, unknown, failed (landed and failed).
    pub cranks: IntCounterVec,
    /// Markets not cranked, by market and reason: paused, closed, backoff.
    pub skipped: IntCounterVec,
    pub relayed_updates: IntCounter,
    pub gas_spent: IntCounter,
    pub gas_balance: Gauge,
    /// Seconds since the market's last funding update.
    pub funding_age: GaugeVec,
    /// Funding intervals that have ended without an update. The engine catches up three.
    pub missed_funding_intervals: IntGaugeVec,
    pub last_crank: GaugeVec,
    pub alerts: IntCounterVec,
}

fn opts(name: &str, help: &str) -> Opts {
    Opts::new(name, help).namespace("perp_cranker")
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
                IntCounter::with_opts(opts("rounds_total", "Rounds over every market"))
            ),
            cranks: register!(
                registry,
                IntCounterVec::new(
                    opts("cranks_total", "Cranks, by market and result"),
                    &["market", "result"]
                )
            ),
            skipped: register!(
                registry,
                IntCounterVec::new(
                    opts("skipped_total", "Markets not cranked, by market and reason"),
                    &["market", "reason"]
                )
            ),
            relayed_updates: register!(
                registry,
                IntCounter::with_opts(opts(
                    "oracle_updates_included_total",
                    "Signed prices relayed in cranks"
                ))
            ),
            gas_spent: register!(
                registry,
                IntCounter::with_opts(opts(
                    "gas_spent_total",
                    "Gas spent, in the smallest unit of HANEUL"
                ))
            ),
            gas_balance: register!(
                registry,
                Gauge::with_opts(opts("gas_balance", "HANEUL held by the key's address"))
            ),
            funding_age: register!(
                registry,
                GaugeVec::new(
                    opts(
                        "funding_age_seconds",
                        "Seconds since the market's last funding update"
                    ),
                    &["market"]
                )
            ),
            missed_funding_intervals: register!(
                registry,
                IntGaugeVec::new(
                    opts(
                        "missed_funding_intervals",
                        "Funding intervals ended without an update"
                    ),
                    &["market"]
                )
            ),
            last_crank: register!(
                registry,
                GaugeVec::new(
                    opts(
                        "last_crank_timestamp_seconds",
                        "When the market was last cranked"
                    ),
                    &["market"]
                )
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
