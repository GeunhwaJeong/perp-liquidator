// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The keeper's metrics, served on `/metrics`.

use prometheus::{
    Encoder, Gauge, IntCounter, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder,
};

pub struct Metrics {
    registry: Registry,
    pub rounds: IntCounter,
    /// Open tickets that name the keeper, by state as of the last round: triggered, waiting,
    /// expired, no_position, mark_unavailable, invalid, no_details, unreadable, unknown_market,
    /// market_paused, no_price, backoff, just_executed, orphaned (cancelled for having gone too
    /// long without a position it reduces); for TWAP tickets, prefixed with `twap_`: due,
    /// scheduled, complete, lapsing, expired, first_run_expired, spoiled, invalid and the shared
    /// ones.
    pub tickets: IntGaugeVec,
    /// Transactions by kind (stop_loss_take_profit, standalone_stop, stop_cancel, twap_chunk,
    /// twap_finalize, twap_cancel) and result: executed, simulated (dry run), refused (would abort or failed to
    /// build), guarded (the fill strayed from the index), rejected, unknown, failed (landed and
    /// failed).
    pub executions: IntCounterVec,
    pub relayed_updates: IntCounter,
    /// The net gas of executions, as the effects count it: computation and storage less the
    /// storage rebate. A stop execution, a TWAP finalization or cancellation deletes its ticket,
    /// whose rebate can outweigh the rest; such a transaction adds what it gained to
    /// `gas_rebated` instead. The gas a ticket hands the keeper is not counted here.
    pub gas_spent: IntCounter,
    pub gas_rebated: IntCounter,
    pub gas_balance: Gauge,
    pub alerts: IntCounterVec,
    /// Waiting details pushed out by newer ones, by why: `source` (the client's own oldest,
    /// over `--max-unmatched-per-source`) or `capacity` (the oldest of all, over
    /// `--max-unmatched`). A climb means someone floods the endpoint.
    pub details_evicted: IntCounterVec,
}

fn opts(name: &str, help: &str) -> Opts {
    Opts::new(name, help).namespace("perp_keeper")
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
                IntCounter::with_opts(opts("rounds_total", "Rounds over the open tickets"))
            ),
            tickets: register!(
                registry,
                IntGaugeVec::new(
                    opts("tickets", "Open tickets that name the keeper, by state"),
                    &["state"]
                )
            ),
            executions: register!(
                registry,
                IntCounterVec::new(
                    opts("executions_total", "Ticket executions, by kind and result"),
                    &["kind", "result"]
                )
            ),
            relayed_updates: register!(
                registry,
                IntCounter::with_opts(opts(
                    "oracle_updates_included_total",
                    "Signed prices relayed in executions"
                ))
            ),
            gas_spent: register!(
                registry,
                IntCounter::with_opts(opts(
                    "gas_spent_total",
                    "Net gas of executions that cost more than their storage rebate, in the smallest unit of HANEUL"
                ))
            ),
            gas_rebated: register!(
                registry,
                IntCounter::with_opts(opts(
                    "gas_rebated_total",
                    "Net gas gained by executions whose storage rebate outweighed their cost, in the smallest unit of HANEUL"
                ))
            ),
            gas_balance: register!(
                registry,
                Gauge::with_opts(opts("gas_balance", "HANEUL held by the key's address"))
            ),
            alerts: register!(
                registry,
                IntCounterVec::new(opts("alerts_total", "Alerts raised, by level"), &["level"])
            ),
            details_evicted: register!(
                registry,
                IntCounterVec::new(
                    opts(
                        "details_evicted_total",
                        "Waiting details pushed out by newer ones, by why"
                    ),
                    &["reason"]
                )
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
