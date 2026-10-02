// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The cranking loop: every round, read each market's clearing house, crank the ones that have
//! something due, and keep track of what fails.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use haneul_crypto::HaneulSigner;
use haneul_transaction_builder::{ObjectInput, TransactionBuilder};
use perp_bot_common::alerts::{Alerts, Level};
use perp_bot_common::chain::{Abort, BuildError, Chain, ExecuteError, Outcome};
use perp_bot_common::keys::Key;
use perp_bot_common::oracle::{OracleClient, Updates};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::errors::{Kind, Tracker};
use crate::metrics::Metrics;
use crate::ptb::{self, Crank};
use crate::schedule::{MAX_CAUGHT_UP_INTERVALS, Schedule};
use crate::status::{Board, now_ms};

const HANEUL: &str = "0x2::haneul::HANEUL";
const HANEUL_UNIT: f64 = 1e9;
/// How often the gas balance and the reference gas price are read again.
const REFRESH_EVERY: Duration = Duration::from_secs(60);
/// `market::EBadIndexPrice`: the base price is older than the market's tolerance.
const BAD_INDEX_PRICE: u64 = 1000;
/// `market`'s index-to-TWAP divergence check.
const INDEX_TWAP_DIVERGENCE: u64 = 1003;

pub struct Settings {
    pub dry_run: bool,
    pub poll_interval: Duration,
    pub twap_min_interval_ms: u64,
    pub due_margin_ms: u64,
    pub max_gas_budget: u64,
    pub min_gas_balance: f64,
    pub skip_after: u32,
    pub skip_for: Duration,
    pub config_check: Duration,
}

#[derive(Clone, Debug)]
pub struct Market {
    pub ticker: String,
    pub crank: Crank,
}

pub struct Parts {
    pub settings: Settings,
    pub chain: Chain,
    pub key: Key,
    pub markets: Vec<Market>,
    pub oracle: Option<OracleClient>,
    pub metrics: Arc<Metrics>,
    pub alerts: Arc<Alerts>,
    pub board: Arc<Board>,
    /// The deployment file and its contents at startup, to notice a change.
    pub deployment: (PathBuf, Vec<u8>),
}

/// Why the loop ended.
#[derive(Debug, PartialEq, Eq)]
pub enum Stopped {
    Cancelled,
    /// The deployment file changed: restart on the new markets.
    DeploymentChanged,
}

pub struct Cranker {
    p: Parts,
    tracker: Tracker,
    gas_price: Option<(u64, Instant)>,
    gas_checked: Option<Instant>,
    deployment_checked: Instant,
}

/// What became of one market in a round.
#[derive(Debug, PartialEq, Eq)]
pub enum Result {
    NotDue,
    Skipped(&'static str),
    Cranked,
    Failed,
}

impl Cranker {
    pub fn new(parts: Parts) -> Self {
        let tracker = Tracker::new(parts.settings.skip_after, parts.settings.skip_for);
        Self {
            p: parts,
            tracker,
            gas_price: None,
            gas_checked: None,
            deployment_checked: Instant::now(),
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) -> Stopped {
        loop {
            if cancel.is_cancelled() {
                return Stopped::Cancelled;
            }
            self.round().await;
            if self.deployment_checked.elapsed() >= self.p.settings.config_check {
                self.deployment_checked = Instant::now();
                if self.deployment_changed() {
                    self.p.alerts.raise(
                        Level::Info,
                        "deployment_changed",
                        "The deployment file changed: exiting to start again on it",
                    );
                    return Stopped::DeploymentChanged;
                }
            }
            tokio::select! {
                _ = cancel.cancelled() => return Stopped::Cancelled,
                _ = tokio::time::sleep(self.p.settings.poll_interval) => {}
            }
        }
    }

    fn deployment_changed(&self) -> bool {
        let (path, contents) = &self.p.deployment;
        match std::fs::read(path) {
            Ok(now) => &now != contents,
            Err(e) => {
                warn!("Failed to read the deployment file {}: {e}", path.display());
                false
            }
        }
    }

    /// One look at every market.
    pub async fn round(&mut self) -> Vec<(String, Result)> {
        self.check_gas().await;
        // The oracle service is asked at most once a round, and only when a crank needs it.
        let mut updates: Option<Option<Updates>> = None;
        let markets = self.p.markets.clone();
        let mut results = Vec::with_capacity(markets.len());
        for market in &markets {
            let result = self.market(market, &mut updates).await;
            results.push((market.ticker.clone(), result));
        }
        self.p.metrics.rounds.inc();
        self.p.board.round_done();
        results
    }

    async fn market(&mut self, market: &Market, updates: &mut Option<Option<Updates>>) -> Result {
        let ticker = market.ticker.as_str();
        if let Some((kind, left)) = self.tracker.skipped(ticker, Instant::now()) {
            debug!(
                market = ticker,
                "Skipped for {} s more after {} failures",
                left.as_secs(),
                kind.label()
            );
            self.p
                .metrics
                .skipped
                .with_label_values(&[ticker, "backoff"])
                .inc();
            return Result::Skipped("backoff");
        }

        let schedule = match self.p.chain.object(market.crank.clearing_house).await {
            Ok(object) => Schedule::from_clearing_house(&object.json),
            Err(e) => Err(e),
        };
        let schedule = match schedule {
            Ok(schedule) => schedule,
            Err(e) => {
                self.failed(ticker, Kind::Read, &format!("{e:#}"));
                return Result::Failed;
            }
        };
        let now = now_ms() as u64;
        self.observe(ticker, &schedule, now);

        if !schedule.trades() {
            let reason = if schedule.paused == 2 {
                "closed"
            } else {
                "paused"
            };
            self.p
                .metrics
                .skipped
                .with_label_values(&[ticker, reason])
                .inc();
            return Result::Skipped(reason);
        }

        let due = schedule.due(
            now.saturating_sub(self.p.settings.due_margin_ms),
            self.p.settings.twap_min_interval_ms,
        );
        if !due.any() {
            return Result::NotDue;
        }

        // The market's own base price, signed by the oracle service, in front of the crank.
        let mut relayed = Vec::new();
        let mut relay = None;
        if let Some(client) = &self.p.oracle {
            if updates.is_none() {
                *updates = Some(match client.fetch().await {
                    Ok(u) => Some(u),
                    Err(e) => {
                        warn!("Failed to fetch signed prices: {e:#}");
                        None
                    }
                });
            }
            if let Some(Some(u)) = updates.as_ref() {
                relayed = u.for_feeds(&[market.crank.base_feed]);
                relay = Some(u.relay.clone());
            }
        }
        let tx = ptb::crank(
            &market.crank,
            relay.as_ref().map(|r| (r, relayed.as_slice())),
            &ObjectInput::new,
        );

        let labels = due.labels();
        let tx = match self.prepare(tx).await {
            Ok(tx) => tx,
            Err(e) => {
                let message = match &e {
                    BuildError::Aborted(abort) => describe(abort, self.p.oracle.is_some()),
                    other => other.to_string(),
                };
                self.p
                    .metrics
                    .cranks
                    .with_label_values(&[ticker, "refused"])
                    .inc();
                self.failed(ticker, Kind::Build, &message);
                return Result::Failed;
            }
        };
        let budget = tx.gas_payment.budget;
        if budget > self.p.settings.max_gas_budget {
            self.p
                .metrics
                .cranks
                .with_label_values(&[ticker, "refused"])
                .inc();
            self.failed(
                ticker,
                Kind::Build,
                &format!(
                    "gas budget {budget} is above --max-gas-budget {}",
                    self.p.settings.max_gas_budget
                ),
            );
            return Result::Failed;
        }

        if self.p.settings.dry_run {
            return match self.p.chain.simulate(&tx).await {
                Ok(outcome) if outcome.success => {
                    info!(
                        market = ticker,
                        due = labels.join(","),
                        relayed = relayed.len(),
                        "Would crank"
                    );
                    self.p
                        .metrics
                        .cranks
                        .with_label_values(&[ticker, "simulated"])
                        .inc();
                    Result::Cranked
                }
                Ok(outcome) => {
                    self.failed(ticker, Kind::Execute, &failure(&outcome));
                    Result::Failed
                }
                Err(e) => {
                    self.failed(ticker, Kind::Execute, &format!("simulation failed: {e:#}"));
                    Result::Failed
                }
            };
        }

        let signature = match self.p.key.private.sign_transaction(&tx) {
            Ok(signature) => signature,
            Err(e) => {
                self.failed(ticker, Kind::Execute, &format!("signing failed: {e}"));
                return Result::Failed;
            }
        };
        match self.p.chain.execute(&tx, signature).await {
            Ok(outcome) if outcome.success => {
                self.p
                    .metrics
                    .cranks
                    .with_label_values(&[ticker, "executed"])
                    .inc();
                self.p
                    .metrics
                    .gas_spent
                    .inc_by(outcome.gas_used.max(0) as u64);
                self.p.metrics.relayed_updates.inc_by(relayed.len() as u64);
                self.p
                    .metrics
                    .last_crank
                    .with_label_values(&[ticker])
                    .set(now_ms() as f64 / 1000.0);
                let events: Vec<&str> = outcome
                    .events
                    .iter()
                    .filter_map(|(t, _)| t.split('<').next()?.rsplit("::").next())
                    .collect();
                info!(
                    market = ticker,
                    digest = outcome.digest,
                    due = labels.join(","),
                    relayed = relayed.len(),
                    events = events.join(","),
                    "Cranked"
                );
                self.tracker.succeeded(ticker);
                self.p.board.clear_problem(&format!("failing:{ticker}"));
                self.p.board.update(|s| {
                    s.cranks += 1;
                    let m = s.markets.entry(ticker.to_owned()).or_default();
                    m.last_crank_ms = Some(now_ms());
                    m.last_digest = Some(outcome.digest.clone());
                    m.last_due = labels.clone();
                });
                Result::Cranked
            }
            Ok(outcome) => {
                self.p
                    .metrics
                    .cranks
                    .with_label_values(&[ticker, "failed"])
                    .inc();
                self.failed(ticker, Kind::Execute, &failure(&outcome));
                Result::Failed
            }
            Err(ExecuteError::Rejected(e)) => {
                self.p
                    .metrics
                    .cranks
                    .with_label_values(&[ticker, "rejected"])
                    .inc();
                self.failed(ticker, Kind::Execute, &format!("rejected: {e}"));
                Result::Failed
            }
            Err(ExecuteError::Unknown(e)) => {
                // It may still land; the next round reads the market again either way.
                self.p
                    .metrics
                    .cranks
                    .with_label_values(&[ticker, "unknown"])
                    .inc();
                self.failed(ticker, Kind::Execute, &format!("no answer: {e}"));
                Result::Failed
            }
        }
    }

    /// Publishes a market's schedule and raises the alarm when funding is being lost.
    fn observe(&self, ticker: &str, schedule: &Schedule, now: u64) {
        let missed = schedule.missed_funding_intervals(now);
        self.p
            .metrics
            .funding_age
            .with_label_values(&[ticker])
            .set(now.saturating_sub(schedule.funding_last_upd_ms) as f64 / 1000.0);
        self.p
            .metrics
            .missed_funding_intervals
            .with_label_values(&[ticker])
            .set(missed as i64);
        self.p.board.update(|s| {
            let m = s.markets.entry(ticker.to_owned()).or_default();
            m.paused = schedule.paused;
            m.funding_last_upd_ms = schedule.funding_last_upd_ms;
            m.next_funding_ms = schedule.next_funding_ms();
            m.missed_funding_intervals = missed;
            m.premium_twap_last_upd_ms = schedule.premium_twap_last_upd_ms;
            m.spread_twap_last_upd_ms = schedule.spread_twap_last_upd_ms;
        });
        let key = format!("funding:{ticker}");
        // A paused market accrues no funding, so nothing is being lost there.
        if schedule.trades() && missed >= MAX_CAUGHT_UP_INTERVALS {
            let message = format!(
                "{ticker} has gone {missed} funding intervals without an update: the engine catches up {MAX_CAUGHT_UP_INTERVALS}, so funding is being lost"
            );
            if self
                .p
                .board
                .set_problem(&key, Level::Critical, message.clone())
            {
                self.p.alerts.raise(Level::Critical, &key, message);
            }
        } else if schedule.trades() && missed + 1 >= MAX_CAUGHT_UP_INTERVALS {
            let message = format!(
                "{ticker} has gone {missed} funding intervals without an update; one more and funding is lost"
            );
            if self
                .p
                .board
                .set_problem(&key, Level::Warning, message.clone())
            {
                self.p.alerts.raise(Level::Warning, &key, message);
            }
        } else if self.p.board.clear_problem(&key) {
            self.p.alerts.raise(
                Level::Info,
                &key,
                format!("{ticker} funding is current again"),
            );
        }
    }

    fn failed(&mut self, ticker: &str, kind: Kind, message: &str) {
        let count = self.tracker.failed(ticker, kind, message, Instant::now());
        warn!(
            market = ticker,
            failures = count,
            kind = kind.label(),
            "{message}"
        );
        if count == self.p.settings.skip_after {
            let key = format!("failing:{ticker}");
            let text = format!(
                "{ticker}: {count} {} failures in a row, leaving it for {} s: {message}",
                kind.label(),
                self.p.settings.skip_for.as_secs()
            );
            self.p.board.set_problem(&key, Level::Warning, text.clone());
            self.p.alerts.raise(Level::Warning, &key, text);
        }
    }

    /// Resolves, prices and simulates.
    async fn prepare(
        &mut self,
        mut tx: TransactionBuilder,
    ) -> std::result::Result<haneul_sdk_types::Transaction, BuildError> {
        tx.set_sender(self.p.key.address);
        if let Some(price) = self.gas_price().await {
            tx.set_gas_price(price);
        }
        self.p.chain.build(tx).await
    }

    async fn gas_price(&mut self) -> Option<u64> {
        if let Some((price, at)) = self.gas_price
            && at.elapsed() < REFRESH_EVERY
        {
            return Some(price);
        }
        match self.p.chain.reference_gas_price().await {
            Ok(price) => {
                self.gas_price = Some((price, Instant::now()));
                Some(price)
            }
            Err(e) => {
                debug!("Failed to read the reference gas price: {e:#}");
                self.gas_price.map(|(price, _)| price)
            }
        }
    }

    async fn check_gas(&mut self) {
        if self.p.settings.dry_run
            || self
                .gas_checked
                .is_some_and(|at| at.elapsed() < REFRESH_EVERY)
        {
            return;
        }
        self.gas_checked = Some(Instant::now());
        match self.p.chain.balance(self.p.key.address, HANEUL).await {
            Ok(balance) => {
                let haneul = balance as f64 / HANEUL_UNIT;
                self.p.metrics.gas_balance.set(haneul);
                let key = "gas";
                if haneul < self.p.settings.min_gas_balance {
                    let level = if balance == 0 {
                        Level::Critical
                    } else {
                        Level::Warning
                    };
                    let message = format!(
                        "{} holds {haneul:.3} HANEUL, under --min-gas-balance {}",
                        self.p.key.address, self.p.settings.min_gas_balance
                    );
                    if self.p.board.set_problem(key, level, message.clone()) {
                        self.p.alerts.raise(level, key, message);
                    }
                } else {
                    self.p.board.clear_problem(key);
                }
            }
            Err(e) => warn!("Failed to read the gas balance: {e:#}"),
        }
    }
}

/// What an abort while building a crank means.
pub fn describe(abort: &Abort, has_oracle: bool) -> String {
    match (abort.module.as_str(), abort.code) {
        ("market", BAD_INDEX_PRICE) if has_oracle => {
            "the base price is stale and the oracle service had no newer one".to_owned()
        }
        ("market", BAD_INDEX_PRICE) => {
            "the base price is stale: the relayer is behind; --oracle-updates-url lets cranks relay it".to_owned()
        }
        ("market", INDEX_TWAP_DIVERGENCE) => {
            "the index price is too far from its TWAP for the market to update".to_owned()
        }
        _ => format!(
            "aborts in {}::{} with {}",
            abort.module, abort.function, abort.code
        ),
    }
}

fn failure(outcome: &Outcome) -> String {
    match (&outcome.abort, &outcome.error) {
        (Some(abort), _) => format!(
            "{} failed in {}::{} with {}",
            outcome.digest, abort.module, abort.function, abort.code
        ),
        (None, Some(error)) => format!("{} failed: {error}", outcome.digest),
        (None, None) => format!("{} failed", outcome.digest),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn abort(module: &str, code: u64) -> Abort {
        Abort {
            package: "0xe2".into(),
            module: module.into(),
            function: "f".into(),
            code,
            command: Some(1),
        }
    }

    #[test]
    fn says_what_an_abort_means() {
        assert!(describe(&abort("market", 1000), false).contains("--oracle-updates-url"));
        assert!(describe(&abort("market", 1000), true).contains("no newer one"));
        assert!(describe(&abort("market", 1003), true).contains("TWAP"));
        assert_eq!(
            describe(&abort("clearing_house", 32), true),
            "aborts in clearing_house::f with 32"
        );
    }
}
