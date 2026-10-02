// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Prometheus metrics. What an operator has to be able to see: whether the liquidator keeps
//! up with the chain, how close positions are to the line, what it tried and what came of it,
//! and whether it still has the gas, collateral and inventory room to keep going.

use prometheus::{
    Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry,
};

/// Seconds, from five milliseconds to thirty seconds.
const DURATIONS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

pub struct Metrics {
    pub registry: Registry,
    pub dry_run: IntGauge,
    pub rounds: IntCounter,
    pub round_errors: IntCounter,
    pub round_seconds: Histogram,
    pub indexer_checkpoint: IntGauge,
    pub indexer_lag_ms: IntGauge,
    pub positions_tracked: IntGauge,
    /// Positions below their maintenance requirement, by market.
    pub liquidatable: IntGaugeVec,
    /// Positions within 10% of it.
    pub at_risk: IntGaugeVec,
    pub lowest_health: GaugeVec,
    pub mark_price: GaugeVec,
    /// What was tried (liquidate, unwind, adl) and what came of it.
    pub attempts: IntCounterVec,
    /// Engine aborts by what they mean.
    pub aborts: IntCounterVec,
    pub liquidated_notional_usd: GaugeVec,
    pub liquidation_fees_usd: GaugeVec,
    pub bad_debt_usd: GaugeVec,
    pub inventory_base: GaugeVec,
    pub inventory_usd: GaugeVec,
    pub gas_balance: Gauge,
    pub account_collateral: Gauge,
    pub gas_spent: IntCounter,
    pub tx_seconds: HistogramVec,
    pub build_seconds: Histogram,
    pub oracle_updates_included: IntCounter,
    pub oracle_fetch_errors: IntCounter,
    pub alerts: IntCounterVec,
}

impl Metrics {
    pub fn new() -> anyhow::Result<Self> {
        let registry = Registry::new_custom(Some("perp_liquidator".into()), None)?;
        macro_rules! register {
            ($metric:expr) => {{
                let metric = $metric;
                registry.register(Box::new(metric.clone()))?;
                metric
            }};
        }
        let opts = |name: &str, help: &str| Opts::new(name, help);
        let hist =
            |name: &str, help: &str| HistogramOpts::new(name, help).buckets(DURATIONS.to_vec());
        Ok(Self {
            dry_run: register!(IntGauge::with_opts(opts(
                "dry_run",
                "1 while transactions are only simulated"
            ))?),
            rounds: register!(IntCounter::with_opts(opts("rounds_total", "Rounds run"))?),
            round_errors: register!(IntCounter::with_opts(opts(
                "round_errors_total",
                "Rounds cut short by an error"
            ))?),
            round_seconds: register!(Histogram::with_opts(hist(
                "round_duration_seconds",
                "Time a round took, transactions included"
            ))?),
            indexer_checkpoint: register!(IntGauge::with_opts(opts(
                "indexer_checkpoint",
                "Checkpoint the indexer's state tables are at"
            ))?),
            indexer_lag_ms: register!(IntGauge::with_opts(opts(
                "indexer_lag_ms",
                "How far the indexed chain time is behind the clock"
            ))?),
            positions_tracked: register!(IntGauge::with_opts(opts(
                "positions_tracked",
                "Positions with size, resting orders or debt being watched"
            ))?),
            liquidatable: register!(IntGaugeVec::new(
                opts(
                    "liquidatable_positions",
                    "Positions below their maintenance requirement"
                ),
                &["market"]
            )?),
            at_risk: register!(IntGaugeVec::new(
                opts(
                    "at_risk_positions",
                    "Positions within 10% of their maintenance requirement"
                ),
                &["market"]
            )?),
            lowest_health: register!(GaugeVec::new(
                opts(
                    "lowest_health",
                    "Lowest margin over maintenance requirement in the market"
                ),
                &["market"]
            )?),
            mark_price: register!(GaugeVec::new(
                opts(
                    "mark_price",
                    "Mark price the liquidator values positions at"
                ),
                &["market"]
            )?),
            attempts: register!(IntCounterVec::new(
                opts("attempts_total", "Transactions tried, by kind and outcome"),
                &["market", "kind", "outcome"]
            )?),
            aborts: register!(IntCounterVec::new(
                opts("aborts_total", "Engine aborts, by what they mean"),
                &["market", "kind", "reason"]
            )?),
            liquidated_notional_usd: register!(GaugeVec::new(
                opts(
                    "liquidated_notional_usd_total",
                    "Notional taken over by liquidation since start"
                ),
                &["market"]
            )?),
            liquidation_fees_usd: register!(GaugeVec::new(
                opts(
                    "liquidation_fees_usd_total",
                    "Liquidation fees earned since start"
                ),
                &["market"]
            )?),
            bad_debt_usd: register!(GaugeVec::new(
                opts(
                    "bad_debt_usd_total",
                    "Bad debt left by liquidated accounts since start"
                ),
                &["market"]
            )?),
            inventory_base: register!(GaugeVec::new(
                opts(
                    "inventory_base",
                    "Size the liquidator holds, positive when long"
                ),
                &["market"]
            )?),
            inventory_usd: register!(GaugeVec::new(
                opts("inventory_usd", "Value of the size the liquidator holds"),
                &["market"]
            )?),
            gas_balance: register!(Gauge::with_opts(opts(
                "gas_balance_haneul",
                "HANEUL held by the liquidator's address"
            ))?),
            account_collateral: register!(Gauge::with_opts(opts(
                "account_collateral",
                "Unallocated collateral of the liquidator's account"
            ))?),
            gas_spent: register!(IntCounter::with_opts(opts(
                "gas_spent_total",
                "Gas spent, in the smallest unit of HANEUL"
            ))?),
            tx_seconds: register!(HistogramVec::new(
                hist(
                    "transaction_duration_seconds",
                    "Time from sending a transaction to its effects in a checkpoint"
                ),
                &["kind"]
            )?),
            build_seconds: register!(Histogram::with_opts(hist(
                "build_duration_seconds",
                "Time the full node took to resolve and simulate a transaction"
            ))?),
            oracle_updates_included: register!(IntCounter::with_opts(opts(
                "oracle_updates_included_total",
                "Signed prices relayed in front of transactions"
            ))?),
            oracle_fetch_errors: register!(IntCounter::with_opts(opts(
                "oracle_fetch_errors_total",
                "Failed requests to the oracle service"
            ))?),
            alerts: register!(IntCounterVec::new(
                opts("alerts_total", "Alerts raised, by level"),
                &["level"]
            )?),
            registry,
        })
    }

    pub fn exposition(&self) -> String {
        let encoder = prometheus::TextEncoder::new();
        encoder
            .encode_to_string(&self.registry.gather())
            .unwrap_or_default()
    }
}
