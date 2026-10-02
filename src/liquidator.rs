// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The liquidator's rounds.
//!
//! Each round:
//! 1. reads the indexer: the markets, and the positions written since the last round (all of
//!    them now and then, to heal anything missed);
//! 2. fetches the oracle service's signed prices, when configured, and values each market at
//!    the newer of those and the chain's;
//! 3. measures every position against its maintenance requirement;
//! 4. liquidates the ones below it (and just above it), the largest first, each in one
//!    transaction that also sells what is taken over back into the book;
//! 5. keeps unwinding whatever is left of taken-over positions;
//! 6. now and then checks the gas and the collateral.
//!
//! Nothing is signed that the full node has not simulated. What the engine refuses is read for
//! what it means: wait for the next checkpoint, back off, or call an operator.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bigdecimal::{BigDecimal, Signed, ToPrimitive, Zero};
use haneul_crypto::HaneulSigner;
use haneul_sdk_types::{Address, Transaction};
use haneul_transaction_builder::{ObjectInput, TransactionBuilder};
use perp_engine::{MarketState, Position};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::aborts::{Reason, classify};
use crate::adl::{self, Counterparty};
use crate::alerts::{Alerts, Level};
use crate::chain::{Abort, BuildError, Chain, ExecuteError, Outcome};
use crate::keys::Key;
use crate::metrics::Metrics;
use crate::oracle::{self, OracleClient, SignedUpdate, Updates};
use crate::ptb::{Builder, Engine, MarketObjects, Unwind};
use crate::report::{self, Report};
use crate::risk::{self, Assessment, MarketView};
use crate::status::{Action, Board, MarketStatus, PositionStatus, now_ms};
use crate::store::{MarketRow, PositionRow, Snapshot, Store};

const HANEUL: &str = "0x2::haneul::HANEUL";
const GEUNHWA_PER_HANEUL: f64 = 1e9;
const BALANCES_EVERY: Duration = Duration::from_secs(30);
const GAS_PRICE_EVERY: Duration = Duration::from_secs(60);
/// How long a transaction whose outcome is unknown is looked for before it is given up on.
const PENDING_FOR: Duration = Duration::from_secs(120);
/// A position the liquidator just acted on is left alone until the indexer shows the result.
const AFTER_SUCCESS: Duration = Duration::from_secs(5);
/// In a dry run, how long to leave a position after simulating its liquidation.
const DRY_RUN_REPEAT: Duration = Duration::from_secs(30);
/// Health below which a position counts as at risk.
const AT_RISK_HEALTH: f64 = 1.1;

pub struct Settings {
    pub dry_run: bool,
    pub poll_interval: Duration,
    pub full_reload: Duration,
    pub attempt_buffer: BigDecimal,
    pub max_liquidations_per_round: usize,
    pub exclude: HashSet<i64>,
    pub unwind: bool,
    pub unwind_slippage: BigDecimal,
    pub unwind_interval: Duration,
    pub max_inventory_usd: f64,
    pub max_gas_budget: u64,
    pub min_gas_balance: f64,
    pub min_collateral: f64,
    pub max_indexer_lag_ms: i64,
}

#[derive(Clone, Debug)]
pub struct Market {
    pub ticker: String,
    /// The clearing house ID as the indexer writes it.
    pub db_id: String,
    pub objects: MarketObjects,
}

pub struct OracleSetup {
    pub client: OracleClient,
    /// The source the service signs for, as the markets refer to it.
    pub source_id: i32,
}

#[derive(Clone, Copy, Debug)]
pub struct AdlSetup {
    pub cap: Address,
    pub registry: Address,
}

/// Everything the liquidator works with, checked at startup.
pub struct Parts {
    pub settings: Settings,
    pub store: Store,
    pub chain: Chain,
    pub key: Key,
    pub engine: Engine,
    pub account_id: i64,
    pub account_db_id: String,
    pub collateral_decimals: u32,
    pub markets: Vec<Market>,
    pub oracle: Option<OracleSetup>,
    pub adl: Option<AdlSetup>,
    pub metrics: Arc<Metrics>,
    pub alerts: Arc<Alerts>,
    pub board: Arc<Board>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Liquidate,
    Unwind,
    Adl,
}

impl Kind {
    fn label(&self) -> &'static str {
        match self {
            Kind::Liquidate => "liquidate",
            Kind::Unwind => "unwind",
            Kind::Adl => "adl",
        }
    }
}

/// What came of handing a transaction over.
enum Submitted {
    /// Dry run: simulated only.
    Simulated(Outcome),
    Executed(Outcome),
    /// Not sent, for the reason given.
    Refused(String),
    /// Turned down by the validators; it did not run.
    Rejected(String),
    /// Sent, but no answer came: it may still land.
    Unknown {
        digest: String,
        error: String,
    },
}

struct Backoff {
    until: Instant,
    failures: u32,
    /// The position's margin when it was put off. One that has lost more since is looked at
    /// again at once.
    margin: Option<BigDecimal>,
}

struct Pending {
    digest: String,
    kind: Kind,
    ticker: String,
    account_id: i64,
    since: Instant,
}

type PositionKey = (String, i64);

pub struct Liquidator {
    p: Parts,
    positions: HashMap<PositionKey, Position>,
    cursor: Option<i64>,
    last_full: Option<Instant>,
    backoff: HashMap<PositionKey, Backoff>,
    last_unwind: HashMap<String, Instant>,
    last_balances: Option<Instant>,
    gas_price: Option<(u64, Instant)>,
    pending: Vec<Pending>,
    /// Running totals for the gauges, by ticker: notional liquidated, fees, bad debt.
    totals: HashMap<String, (f64, f64, f64)>,
    /// The markets as of the latest round.
    rows: Vec<MarketRow>,
    /// The TWAP window of the oracle service's feed in each price feed storage, read from the
    /// chain, to tell what a relayed price will make of the TWAP.
    twap_periods: HashMap<Address, u64>,
}

/// A market's base price as the next transaction would see it.
struct BasePrice {
    price: Option<BigDecimal>,
    twap: Option<BigDecimal>,
    timestamp_ms: Option<i64>,
}

fn unresolved(id: Address) -> ObjectInput {
    ObjectInput::new(id)
}

impl Liquidator {
    pub fn new(parts: Parts) -> Self {
        parts.metrics.dry_run.set(i64::from(parts.settings.dry_run));
        Self {
            p: parts,
            positions: HashMap::new(),
            cursor: None,
            last_full: None,
            backoff: HashMap::new(),
            last_unwind: HashMap::new(),
            last_balances: None,
            gas_price: None,
            pending: Vec::new(),
            totals: HashMap::new(),
            rows: Vec::new(),
            twap_periods: HashMap::new(),
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) {
        let mut ticker = tokio::time::interval(self.p.settings.poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            // A round in progress is never cut short: a transaction already sent is seen through.
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                _ = ticker.tick() => {}
            }
            let started = Instant::now();
            let result = self.round().await;
            self.p.metrics.rounds.inc();
            self.p
                .metrics
                .round_seconds
                .observe(started.elapsed().as_secs_f64());
            let error = result.as_ref().err().map(|e| format!("{e:#}"));
            self.p.board.update(|s| {
                s.last_round_ms = Some(now_ms());
                s.last_round_ok = error.is_none();
                s.last_error = error.clone();
            });
            match error {
                None => {
                    if self.p.board.clear_problem("round") {
                        info!("Rounds are completing again");
                    }
                }
                Some(error) => {
                    self.p.metrics.round_errors.inc();
                    if self
                        .p
                        .board
                        .set_problem("round", Level::Warning, error.clone())
                    {
                        self.p.alerts.raise(
                            Level::Warning,
                            "round",
                            format!("Rounds are failing: {error}"),
                        );
                    } else {
                        debug!("Round failed: {error}");
                    }
                }
            }
        }
        info!("Stopped");
    }

    async fn round(&mut self) -> anyhow::Result<()> {
        self.resolve_pending().await;

        let full = self.cursor.is_none()
            || self
                .last_full
                .is_none_or(|at| at.elapsed() >= self.p.settings.full_reload);
        let ids: Vec<String> = self.p.markets.iter().map(|m| m.db_id.clone()).collect();
        let Some(snapshot) = self
            .p
            .store
            .snapshot(&ids, if full { None } else { self.cursor })
            .await?
        else {
            anyhow::bail!("the indexer has indexed nothing yet");
        };
        if full {
            self.last_full = Some(Instant::now());
            self.read_twap_periods().await;
        }
        self.observe_indexer(&snapshot);
        self.apply(&snapshot);
        self.rows = snapshot.markets.clone();

        let updates = self.fetch_updates().await;
        let now = now_ms();
        let views = self.views(&snapshot.markets, updates.as_ref(), now);
        let assessments = self.assess(&views);
        self.publish(
            &views,
            &assessments,
            &snapshot.markets,
            updates.as_ref(),
            now,
        );

        let mut candidates: Vec<Assessment> = assessments
            .into_iter()
            .filter(|a| {
                views.get(&a.market).is_some_and(|v| v.tradable)
                    && a.worth_trying(&self.p.settings.attempt_buffer)
                    && !self.p.settings.exclude.contains(&a.account_id)
            })
            .collect();
        risk::rank(&mut candidates);
        let mut attempts = 0;
        for candidate in &candidates {
            if attempts >= self.p.settings.max_liquidations_per_round {
                break;
            }
            if self.held(&(candidate.market.clone(), candidate.account_id), candidate) {
                continue;
            }
            attempts += 1;
            let view = &views[&candidate.market];
            self.liquidate(candidate, view, updates.as_ref()).await;
        }

        if self.p.settings.unwind {
            self.unwind_inventory(&views, updates.as_ref()).await;
        }
        if self
            .last_balances
            .is_none_or(|at| at.elapsed() >= BALANCES_EVERY)
        {
            self.last_balances = Some(Instant::now());
            self.check_balances().await;
        }
        Ok(())
    }

    fn observe_indexer(&mut self, snapshot: &Snapshot) {
        let lag = (now_ms() - snapshot.watermark.timestamp_ms).max(0);
        self.cursor = Some(snapshot.watermark.checkpoint);
        self.p
            .metrics
            .indexer_checkpoint
            .set(snapshot.watermark.checkpoint);
        self.p.metrics.indexer_lag_ms.set(lag);
        self.p.board.update(|s| {
            s.indexer_checkpoint = Some(snapshot.watermark.checkpoint);
            s.indexer_lag_ms = Some(lag);
        });
        if lag > self.p.settings.max_indexer_lag_ms {
            let message = format!(
                "The indexer is {} s behind the chain; liquidations wait for it",
                lag / 1000
            );
            if self
                .p
                .board
                .set_problem("indexer_lag", Level::Warning, message.clone())
            {
                self.p.alerts.raise(Level::Warning, "indexer_lag", message);
            }
        } else if self.p.board.clear_problem("indexer_lag") {
            info!("The indexer has caught up");
        }
    }

    fn apply(&mut self, snapshot: &Snapshot) {
        if snapshot.full {
            self.positions.clear();
        }
        for row in &snapshot.positions {
            let key = (row.market.clone(), row.account_id);
            let position = position(row);
            let matters = !position.base.is_zero()
                || position.pending_orders != 0
                || position.collateral.is_negative();
            if matters {
                self.positions.insert(key.clone(), position);
            } else {
                self.positions.remove(&key);
            }
            // A position that changed is new information: try it again at once.
            if !snapshot.full {
                self.backoff.remove(&key);
            }
        }
        self.p
            .metrics
            .positions_tracked
            .set(self.positions.len() as i64);
    }

    async fn fetch_updates(&mut self) -> Option<Updates> {
        let oracle = self.p.oracle.as_ref()?;
        match oracle.client.fetch().await {
            Ok(updates) => {
                if self.p.board.clear_problem("oracle_service") {
                    info!("The oracle service answers again");
                }
                Some(updates)
            }
            Err(e) => {
                self.p.metrics.oracle_fetch_errors.inc();
                let message = format!(
                    "The oracle service does not answer ({e:#}); liquidations rely on the relayer's prices"
                );
                if self
                    .p
                    .board
                    .set_problem("oracle_service", Level::Warning, message.clone())
                {
                    self.p
                        .alerts
                        .raise(Level::Warning, "oracle_service", message);
                }
                None
            }
        }
    }

    fn market_by_db_id(&self, db_id: &str) -> Option<&Market> {
        self.p.markets.iter().find(|m| m.db_id == db_id)
    }

    /// The signed prices to relay in front of a market's transaction: those of its feeds, for
    /// the source its prices are read from.
    fn refresh_for(
        &self,
        market: &Market,
        updates: Option<&Updates>,
    ) -> Option<(crate::oracle::Relay, Vec<SignedUpdate>)> {
        let (oracle, updates) = (self.p.oracle.as_ref()?, updates?);
        let row = self.rows.iter().find(|r| r.market == market.db_id)?;
        let mut feeds = Vec::new();
        if row.base_source_id == oracle.source_id {
            feeds.push(market.objects.base_feed);
        }
        if row.collateral_source_id == oracle.source_id {
            feeds.push(market.objects.collateral_feed);
        }
        let picked = updates.for_feeds(&feeds);
        (!picked.is_empty()).then(|| (updates.relay.clone(), picked))
    }

    fn views(
        &self,
        rows: &[MarketRow],
        updates: Option<&Updates>,
        now: i64,
    ) -> HashMap<String, MarketView> {
        let mut views = HashMap::new();
        for row in rows {
            let Some(market) = self.market_by_db_id(&row.market) else {
                continue;
            };
            let base = self.base_price(row, market, updates);
            let state = MarketState {
                settlement_enabled: row.settlement_enabled,
                settlement_base_price: row.settlement_base_price.clone(),
                margin_ratio_initial: row.margin_ratio_initial.clone(),
                cum_funding_rate_long: row.cum_funding_rate_long.clone(),
                cum_funding_rate_short: row.cum_funding_rate_short.clone(),
                funding_last_upd_ms: row.funding_last_upd_ms,
                funding_frequency_ms: row.funding_frequency_ms,
                funding_period_ms: row.funding_period_ms,
                premium_twap: row.premium_twap.clone(),
                spread_twap: row.spread_twap.clone(),
                best_bid_price: row.best_bid_price.clone(),
                best_ask_price: row.best_ask_price.clone(),
                collateral_haircut: row.collateral_haircut.clone(),
                oracle_price: base.price,
                oracle_twap_price: base.twap,
                collateral_price: row.collateral_price.clone(),
                event_index_price: row.event_index_price.clone(),
            };
            let Some((pricing, valuation)) = state.price(now) else {
                debug!(market = market.ticker, "No price yet");
                continue;
            };
            views.insert(
                row.market.clone(),
                MarketView {
                    id: row.market.clone(),
                    ticker: market.ticker.clone(),
                    tradable: row.paused == 0 && !row.closed && !row.settlement_enabled,
                    valuation,
                    margin_ratio_maintenance: row.margin_ratio_maintenance.clone(),
                    lot_size: row.lot_size.max(0) as u64,
                    tick_size: row.tick_size.max(0) as u64,
                    index_price: pricing.index_price,
                },
            );
        }
        views
    }

    /// The base asset's price and when it was published: the oracle service's signed one when
    /// it is newer than the chain's and for the market's source.
    /// The base asset's price, TWAP and publication time: the oracle service's signed price,
    /// and the TWAP it will make, when it is newer than the chain's and for the market's source.
    fn base_price(&self, row: &MarketRow, market: &Market, updates: Option<&Updates>) -> BasePrice {
        let on_chain = BasePrice {
            price: row.oracle_price.clone(),
            twap: row.oracle_twap_price.clone(),
            timestamp_ms: row.oracle_timestamp_ms,
        };
        let (Some(setup), Some(updates)) = (self.p.oracle.as_ref(), updates) else {
            return on_chain;
        };
        if row.base_source_id != setup.source_id {
            return on_chain;
        }
        let feed = market.objects.base_feed;
        let Some(update) = updates.get(&feed) else {
            return on_chain;
        };
        if on_chain
            .timestamp_ms
            .is_some_and(|at| update.timestamp_ms as i64 <= at)
        {
            return on_chain;
        }
        // Without the chain's TWAP and window, assume the TWAP catches up at once; simulation
        // settles what that gets wrong.
        let twap = match (
            on_chain.twap.as_ref().and_then(oracle::raw),
            on_chain.timestamp_ms,
            self.twap_periods.get(&feed),
        ) {
            (Some(last_twap), Some(last_ms), Some(period)) => oracle::update_twap(
                update.price,
                last_twap,
                update.timestamp_ms,
                last_ms as u64,
                *period,
            ),
            _ => update.price,
        };
        BasePrice {
            price: Some(update.price()),
            twap: Some(BigDecimal::new(twap.into(), 18)),
            timestamp_ms: Some(update.timestamp_ms as i64),
        }
    }

    /// Reads the TWAP window of the oracle service's feeds from the chain.
    async fn read_twap_periods(&mut self) {
        let Some(setup) = self.p.oracle.as_ref() else {
            return;
        };
        let source_id = i64::from(setup.source_id);
        for market in self.p.markets.clone() {
            let feed = market.objects.base_feed;
            match self.p.chain.object(feed).await {
                Ok(info) => {
                    let period = info.json["feeds"].as_array().and_then(|feeds| {
                        feeds
                            .iter()
                            .find(|f| json_i64(&f["source_id"]) == Some(source_id))
                    });
                    match period.and_then(|f| json_i64(&f["twap_period_ms"])) {
                        Some(period) => {
                            self.twap_periods.insert(feed, period.max(0) as u64);
                        }
                        None => debug!(market = market.ticker, "No feed of the oracle source"),
                    }
                }
                Err(e) => debug!(
                    market = market.ticker,
                    "Failed to read the price feed: {e:#}"
                ),
            }
        }
    }

    fn assess(&self, views: &HashMap<String, MarketView>) -> Vec<Assessment> {
        self.positions
            .iter()
            .filter(|((_, account), _)| *account != self.p.account_id)
            .filter_map(|((market, account), position)| {
                views
                    .get(market)
                    .map(|view| risk::assess(view, *account, position))
            })
            .collect()
    }

    fn inventory(&self, market: &str) -> BigDecimal {
        self.positions
            .get(&(market.to_owned(), self.p.account_id))
            .map(|p| p.base.clone())
            .unwrap_or_default()
    }

    fn publish(
        &self,
        views: &HashMap<String, MarketView>,
        assessments: &[Assessment],
        rows: &[MarketRow],
        updates: Option<&Updates>,
        now: i64,
    ) {
        let m = &self.p.metrics;
        let mut markets = Vec::new();
        for market in &self.p.markets {
            let ticker = market.ticker.as_str();
            let mine: Vec<&Assessment> = assessments
                .iter()
                .filter(|a| a.market == market.db_id)
                .collect();
            let liquidatable = mine.iter().filter(|a| a.liquidatable()).count();
            let healths: Vec<f64> = mine
                .iter()
                .filter_map(|a| a.health())
                .filter_map(|h| h.to_f64())
                .collect();
            let at_risk = healths.iter().filter(|h| **h < AT_RISK_HEALTH).count();
            m.liquidatable
                .with_label_values(&[ticker])
                .set(liquidatable as i64);
            m.at_risk.with_label_values(&[ticker]).set(at_risk as i64);
            if let Some(lowest) = healths.iter().copied().reduce(f64::min) {
                m.lowest_health.with_label_values(&[ticker]).set(lowest);
            }
            let inventory = self.inventory(&market.db_id);
            m.inventory_base
                .with_label_values(&[ticker])
                .set(inventory.to_f64().unwrap_or_default());
            let Some(view) = views.get(&market.db_id) else {
                continue;
            };
            m.mark_price
                .with_label_values(&[ticker])
                .set(view.valuation.mark_price.to_f64().unwrap_or_default());
            let inventory_usd = (inventory.abs() * &view.valuation.mark_price)
                .to_f64()
                .unwrap_or_default();
            m.inventory_usd
                .with_label_values(&[ticker])
                .set(inventory_usd);
            let oracle_age_ms = rows
                .iter()
                .find(|r| r.market == market.db_id)
                .and_then(|row| self.base_price(row, market, updates).timestamp_ms)
                .map(|at| now - at);
            markets.push(MarketStatus {
                ticker: market.ticker.clone(),
                market: market.db_id.clone(),
                tradable: view.tradable,
                mark_price: perp_engine::decimal::plain(&view.valuation.mark_price.with_scale(6)),
                index_price: perp_engine::decimal::plain(&view.index_price.with_scale(6)),
                oracle_age_ms,
                positions: mine.len(),
                liquidatable,
                inventory_base: perp_engine::decimal::plain(&inventory),
            });
        }
        let mut worst: Vec<&Assessment> = assessments
            .iter()
            .filter(|a| a.health().is_some() || a.bad_debt())
            .collect();
        worst.sort_by(|a, b| risk::by_health(a, b));
        let at_risk = worst
            .into_iter()
            .take(20)
            .map(|a| PositionStatus {
                ticker: self
                    .market_by_db_id(&a.market)
                    .map(|m| m.ticker.clone())
                    .unwrap_or_default(),
                account_id: a.account_id,
                base: perp_engine::decimal::plain(&a.base),
                margin: perp_engine::decimal::plain(&a.margin.with_scale(6)),
                maintenance: perp_engine::decimal::plain(&a.maintenance.with_scale(6)),
                health: a.health().map(|h| perp_engine::decimal::plain(&h)),
            })
            .collect();
        self.p.board.update(|s| {
            s.markets = markets;
            s.at_risk = at_risk;
        });
    }

    fn held(&self, key: &PositionKey, candidate: &Assessment) -> bool {
        let Some(backoff) = self.backoff.get(key) else {
            return false;
        };
        if backoff.until <= Instant::now() {
            return false;
        }
        match &backoff.margin {
            // Half a percent of the requirement lost since is enough to look again.
            Some(before) => {
                candidate.margin >= before - &candidate.maintenance / BigDecimal::from(200)
            }
            None => true,
        }
    }

    fn hold(&mut self, key: PositionKey, base: Duration, margin: Option<BigDecimal>) {
        let entry = self.backoff.entry(key).or_insert(Backoff {
            until: Instant::now(),
            failures: 0,
            margin: None,
        });
        entry.failures = entry.failures.saturating_add(1);
        let factor = 1u32 << (entry.failures.min(5) - 1);
        let delay = (base * factor).min(base.max(Duration::from_secs(300)));
        entry.until = Instant::now() + delay;
        entry.margin = margin;
    }

    async fn gas_price(&mut self) -> Option<u64> {
        if let Some((price, at)) = self.gas_price
            && at.elapsed() < GAS_PRICE_EVERY
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

    /// Resolves, prices and simulates. The reference gas price is used on purpose: paying more
    /// makes the engine charge its priority taker fee on the unwinding order.
    async fn prepare(&mut self, mut tx: TransactionBuilder) -> Result<Transaction, BuildError> {
        tx.set_sender(self.p.key.address);
        if let Some(price) = self.gas_price().await {
            tx.set_gas_price(price);
        }
        let started = Instant::now();
        let built = self.p.chain.build(tx).await;
        self.p
            .metrics
            .build_seconds
            .observe(started.elapsed().as_secs_f64());
        debug!(
            "Built and simulated in {} ms",
            started.elapsed().as_millis()
        );
        built
    }

    async fn submit(&mut self, kind: Kind, tx: Transaction, relayed: usize) -> Submitted {
        let budget = tx.gas_payment.budget;
        if budget > self.p.settings.max_gas_budget {
            return Submitted::Refused(format!(
                "gas budget {budget} is above --max-gas-budget {}",
                self.p.settings.max_gas_budget
            ));
        }
        if self.p.settings.dry_run {
            return match self.p.chain.simulate(&tx).await {
                Ok(outcome) => Submitted::Simulated(outcome),
                Err(e) => Submitted::Refused(format!("simulation failed: {e:#}")),
            };
        }
        let signature = match self.p.key.private.sign_transaction(&tx) {
            Ok(signature) => signature,
            Err(e) => return Submitted::Refused(format!("signing failed: {e}")),
        };
        let started = Instant::now();
        let result = self.p.chain.execute(&tx, signature).await;
        debug!(
            digest = %tx.digest(),
            "Executed and checkpointed in {} ms",
            started.elapsed().as_millis()
        );
        self.p
            .metrics
            .tx_seconds
            .with_label_values(&[kind.label()])
            .observe(started.elapsed().as_secs_f64());
        match result {
            Ok(outcome) => {
                self.p
                    .metrics
                    .gas_spent
                    .inc_by(outcome.gas_used.max(0) as u64);
                self.p
                    .metrics
                    .oracle_updates_included
                    .inc_by(relayed as u64);
                Submitted::Executed(outcome)
            }
            Err(ExecuteError::Rejected(error)) => Submitted::Rejected(error),
            Err(ExecuteError::Unknown(error)) => Submitted::Unknown {
                digest: tx.digest().to_string(),
                error,
            },
        }
    }

    fn record(
        &self,
        kind: Kind,
        ticker: &str,
        account_id: i64,
        outcome: &str,
        digest: Option<String>,
        detail: Option<String>,
    ) {
        self.p
            .metrics
            .attempts
            .with_label_values(&[ticker, kind.label(), outcome_label(outcome)])
            .inc();
        self.p.board.record(Action {
            at_ms: now_ms(),
            kind: kind.label().to_owned(),
            ticker: ticker.to_owned(),
            account_id,
            outcome: outcome.to_owned(),
            digest,
            detail,
        });
    }

    fn unwind_for(
        &self,
        view: &MarketView,
        liqee_base: &BigDecimal,
        inventory: &BigDecimal,
    ) -> Option<Unwind> {
        if liqee_base.is_zero() {
            return None;
        }
        // Taking over a long means selling it.
        let is_ask = liqee_base.is_positive();
        let same_side = if !inventory.is_zero() && inventory.is_positive() == is_ask {
            inventory.abs()
        } else {
            BigDecimal::zero()
        };
        let size = risk::lots_covering(&(liqee_base.abs() + same_side), view.lot_size)?;
        let price = risk::limit_price(
            &view.valuation.mark_price,
            is_ask,
            &self.p.settings.unwind_slippage,
            view.tick_size,
        )?;
        Some(Unwind {
            is_ask,
            size,
            price,
        })
    }

    async fn liquidate(
        &mut self,
        candidate: &Assessment,
        view: &MarketView,
        updates: Option<&Updates>,
    ) {
        let key = (candidate.market.clone(), candidate.account_id);
        let Some(market) = self.market_by_db_id(&candidate.market).cloned() else {
            return;
        };
        let ticker = market.ticker.clone();
        let cancel = match self
            .p
            .store
            .open_orders(&candidate.market, candidate.account_id)
            .await
        {
            Ok(ids) => ids,
            Err(e) => {
                warn!(
                    market = ticker,
                    account = candidate.account_id,
                    "Failed to read resting orders: {e:#}"
                );
                self.hold(key, Duration::from_secs(2), None);
                return;
            }
        };
        if cancel.len() as i64 != candidate.pending_orders {
            debug!(
                market = ticker,
                account = candidate.account_id,
                "The indexer lists {} resting orders, the position counts {}",
                cancel.len(),
                candidate.pending_orders
            );
        }
        let refresh = self.refresh_for(&market, updates);
        let relayed = refresh.as_ref().map_or(0, |(_, u)| u.len());
        let inventory = self.inventory(&candidate.market);
        let mut unwind = if self.p.settings.unwind {
            self.unwind_for(view, &candidate.base, &inventory)
        } else {
            None
        };

        let built = loop {
            let (tx, unwind_command) = {
                let mut b = Builder::new(&self.p.engine, &unresolved);
                b.refresh(refresh.as_ref().map(|(relay, u)| (relay, u.as_slice())));
                let command = b.liquidation(
                    &market.objects,
                    candidate.account_id as u64,
                    &cancel,
                    unwind.as_ref(),
                );
                (b.finish(), command)
            };
            match self.prepare(tx).await {
                // The liquidation itself would go through; the order to unwind it would not,
                // most likely because nothing was taken over. Take over what there is as is.
                Err(BuildError::Aborted(abort))
                    if unwind.is_some() && abort.command == unwind_command =>
                {
                    debug!(
                        market = ticker,
                        account = candidate.account_id,
                        "Unwinding would fail ({}::{} {}); liquidating without it",
                        abort.module,
                        abort.function,
                        abort.code
                    );
                    unwind = None;
                }
                other => break other,
            }
        };

        let tx = match built {
            Ok(tx) => tx,
            Err(BuildError::Aborted(abort)) => {
                self.aborted(
                    Kind::Liquidate,
                    &market,
                    candidate.account_id,
                    Some(candidate),
                    abort,
                    view,
                    updates,
                )
                .await;
                return;
            }
            Err(e) => {
                warn!(
                    market = ticker,
                    account = candidate.account_id,
                    "Could not build the liquidation: {e}"
                );
                self.record(
                    Kind::Liquidate,
                    &ticker,
                    candidate.account_id,
                    "failed",
                    None,
                    Some(e.to_string()),
                );
                self.hold(key, Duration::from_secs(5), None);
                return;
            }
        };

        let digest = tx.digest().to_string();
        match self.submit(Kind::Liquidate, tx, relayed).await {
            Submitted::Simulated(outcome) => {
                let detail = if outcome.success {
                    let report = report::read(
                        &outcome.events,
                        &self.p.engine.types_package,
                        self.p.account_id as u64,
                    );
                    info!(
                        market = ticker,
                        account = candidate.account_id,
                        "Dry run: would liquidate {}",
                        describe(&report)
                    );
                    describe(&report)
                } else {
                    format!("would fail: {}", outcome.error.unwrap_or_default())
                };
                self.record(
                    Kind::Liquidate,
                    &ticker,
                    candidate.account_id,
                    "simulated",
                    Some(digest),
                    Some(detail),
                );
                self.hold(key, DRY_RUN_REPEAT, Some(candidate.margin.clone()));
            }
            Submitted::Executed(outcome) if outcome.success => {
                let report = report::read(
                    &outcome.events,
                    &self.p.engine.types_package,
                    self.p.account_id as u64,
                );
                self.count(&ticker, &report, &view.valuation.mark_price);
                info!(
                    market = ticker,
                    account = candidate.account_id,
                    digest = outcome.digest,
                    "Liquidated {}",
                    describe(&report)
                );
                if report.bad_debt.is_positive() {
                    self.p.alerts.raise(
                        Level::Info,
                        &format!("bad_debt:{ticker}:{}", candidate.account_id),
                        format!(
                            "{ticker}: account {} left {} USD of bad debt ({} socialized)",
                            candidate.account_id,
                            usd(&report.bad_debt),
                            usd(&report.socialized)
                        ),
                    );
                }
                for problem in ["liquidator_underfunded", "liquidator_in_bad_debt"] {
                    self.p.board.clear_problem(problem);
                }
                self.record(
                    Kind::Liquidate,
                    &ticker,
                    candidate.account_id,
                    "executed",
                    Some(outcome.digest),
                    Some(describe(&report)),
                );
                self.backoff.remove(&key);
                self.hold(key, AFTER_SUCCESS, None);
            }
            Submitted::Executed(outcome) => match outcome.abort.clone() {
                Some(abort) => {
                    self.aborted(
                        Kind::Liquidate,
                        &market,
                        candidate.account_id,
                        Some(candidate),
                        abort,
                        view,
                        updates,
                    )
                    .await
                }
                None => {
                    let error = outcome.error.unwrap_or_default();
                    warn!(
                        market = ticker,
                        account = candidate.account_id,
                        digest = outcome.digest,
                        "Liquidation failed on chain: {error}"
                    );
                    self.record(
                        Kind::Liquidate,
                        &ticker,
                        candidate.account_id,
                        "failed",
                        Some(outcome.digest),
                        Some(error),
                    );
                    self.hold(key, Duration::from_secs(10), None);
                }
            },
            Submitted::Refused(reason) => {
                self.p.alerts.raise(
                    Level::Warning,
                    &format!("refused:{ticker}"),
                    format!(
                        "{ticker}: liquidation of account {} not sent: {reason}",
                        candidate.account_id
                    ),
                );
                self.record(
                    Kind::Liquidate,
                    &ticker,
                    candidate.account_id,
                    "refused",
                    None,
                    Some(reason),
                );
                self.hold(key, Duration::from_secs(60), None);
            }
            Submitted::Rejected(error) => {
                debug!(
                    market = ticker,
                    account = candidate.account_id,
                    "Liquidation rejected, building it again: {error}"
                );
                self.record(
                    Kind::Liquidate,
                    &ticker,
                    candidate.account_id,
                    "rejected",
                    None,
                    Some(error),
                );
                self.hold(key, Duration::from_secs(1), None);
            }
            Submitted::Unknown { digest, error } => {
                warn!(
                    market = ticker,
                    account = candidate.account_id,
                    digest,
                    "No answer to the liquidation, looking it up: {error}"
                );
                self.record(
                    Kind::Liquidate,
                    &ticker,
                    candidate.account_id,
                    "unknown",
                    Some(digest.clone()),
                    Some(error),
                );
                self.pending.push(Pending {
                    digest,
                    kind: Kind::Liquidate,
                    ticker,
                    account_id: candidate.account_id,
                    since: Instant::now(),
                });
                self.hold(key, Duration::from_secs(10), None);
            }
        }
    }

    fn count(&mut self, ticker: &str, report: &Report, mark: &BigDecimal) {
        let notional = (report.base_liquidated.abs() * mark)
            .to_f64()
            .unwrap_or_default();
        let fees = report.liquidation_fees.to_f64().unwrap_or_default();
        let bad_debt = report.bad_debt.to_f64().unwrap_or_default();
        let totals = self.totals.entry(ticker.to_owned()).or_default();
        totals.0 += notional;
        totals.1 += fees;
        totals.2 += bad_debt;
        let m = &self.p.metrics;
        m.liquidated_notional_usd
            .with_label_values(&[ticker])
            .set(totals.0);
        m.liquidation_fees_usd
            .with_label_values(&[ticker])
            .set(totals.1);
        m.bad_debt_usd.with_label_values(&[ticker]).set(totals.2);
    }

    #[allow(clippy::too_many_arguments)]
    async fn aborted(
        &mut self,
        kind: Kind,
        market: &Market,
        account_id: i64,
        candidate: Option<&Assessment>,
        abort: Abort,
        view: &MarketView,
        updates: Option<&Updates>,
    ) {
        let reason = classify(&abort.module, abort.code);
        let ticker = market.ticker.clone();
        self.p
            .metrics
            .aborts
            .with_label_values(&[ticker.as_str(), kind.label(), reason.label()])
            .inc();
        let detail = format!(
            "{}::{} aborted with {}",
            abort.module, abort.function, abort.code
        );
        self.record(
            kind,
            &ticker,
            account_id,
            &format!("aborted: {}", reason.label()),
            None,
            Some(detail.clone()),
        );
        self.hold(
            (market.db_id.clone(), account_id),
            reason.retry_after(),
            candidate.map(|c| c.margin.clone()),
        );

        let operator = |key: &str, level: Level, message: String| {
            if self.p.board.set_problem(key, level, message.clone()) {
                self.p.alerts.raise(level, key, message);
            }
        };
        match reason {
            Reason::AboveMaintenance
            | Reason::OrdersOutOfDate
            | Reason::NothingToReduce
            | Reason::NoFill => {
                debug!(
                    market = ticker,
                    account = account_id,
                    "{}: {detail}",
                    reason.label()
                );
            }
            Reason::MarketPaused | Reason::MarketClosed => {
                info!(
                    market = ticker,
                    "{}: nothing can be liquidated",
                    reason.label()
                );
            }
            Reason::LiquidatorUnderfunded => operator(
                "liquidator_underfunded",
                Level::Critical,
                format!(
                    "{ticker}: the liquidator's account cannot cover the margin of the position it would take over; deposit collateral"
                ),
            ),
            Reason::LiquidatorInBadDebt => operator(
                "liquidator_in_bad_debt",
                Level::Critical,
                format!("{ticker}: the liquidator's own position has negative equity"),
            ),
            Reason::PackageVersion => operator(
                "package_version",
                Level::Critical,
                "The engine package was upgraded; point the deployment file at the new version"
                    .to_owned(),
            ),
            Reason::StaleOracle => operator(
                &format!("stale_oracle:{ticker}"),
                Level::Warning,
                format!("{ticker}: the oracle price is too old for the market to price anything"),
            ),
            Reason::OracleDivergence => operator(
                &format!("oracle_divergence:{ticker}"),
                Level::Warning,
                format!(
                    "{ticker}: the index price is too far from its TWAP for the market to price anything"
                ),
            ),
            Reason::BadDebtBeyondLimits if kind == Kind::Liquidate => {
                self.deleverage(market, account_id, view, updates).await;
            }
            _ => self.p.alerts.raise(
                Level::Warning,
                &format!("abort:{}:{}", abort.module, abort.code),
                format!(
                    "{ticker}: {} of account {account_id}: {detail}",
                    kind.label()
                ),
            ),
        }
    }

    /// Closes a position whose bad debt liquidation cannot absorb, against the most profitable
    /// positions on the other side, when the liquidator holds the ADL capability; reports what
    /// it would do otherwise.
    async fn deleverage(
        &mut self,
        market: &Market,
        account_id: i64,
        view: &MarketView,
        updates: Option<&Updates>,
    ) {
        let ticker = market.ticker.clone();
        let problem = format!("adl:{ticker}:{account_id}");
        let Some(position) = self
            .positions
            .get(&(market.db_id.clone(), account_id))
            .cloned()
        else {
            return;
        };
        let mark = &view.valuation.mark_price;
        let counterparties: Vec<Counterparty> = self
            .positions
            .iter()
            .filter(|((m, a), _)| *m == market.db_id && *a != account_id)
            .map(|((_, a), p)| {
                let margin = p.margin(&view.valuation);
                Counterparty {
                    account_id: *a,
                    base: p.base.clone(),
                    entry_notional: p.quote_notional.abs(),
                    unrealized_pnl: margin.unrealized_pnl,
                    margin: margin.margin,
                    notional: p.base.abs() * mark,
                }
            })
            .collect();
        let size = risk::lots_covering(&position.base, view.lot_size).unwrap_or(0);
        let plan = adl::plan(
            size,
            position.base.is_positive(),
            view.lot_size,
            &counterparties,
        );
        let Some(plan) = plan else {
            let message = format!(
                "{ticker}: account {account_id} has bad debt beyond the insurance fund and the socialization limits, and the profitable positions on the other side do not cover its size {}; an operator has to decide",
                perp_engine::decimal::plain(&position.base)
            );
            if self
                .p
                .board
                .set_problem(&problem, Level::Critical, message.clone())
            {
                self.p.alerts.raise(Level::Critical, &problem, message);
            }
            return;
        };
        let summary: Vec<String> = plan
            .shares
            .iter()
            .map(|s| format!("{} takes {}", s.account_id, s.size))
            .collect();
        let Some(setup) = self.p.adl else {
            let message = format!(
                "{ticker}: account {account_id} can only be closed by ADL; without --adl-cap the plan is only reported: {}",
                summary.join(", ")
            );
            if self
                .p
                .board
                .set_problem(&problem, Level::Critical, message.clone())
            {
                self.p.alerts.raise(Level::Critical, &problem, message);
            }
            return;
        };

        let cancel = match self.p.store.open_orders(&market.db_id, account_id).await {
            Ok(ids) => ids,
            Err(e) => {
                warn!(
                    market = ticker,
                    account = account_id,
                    "Failed to read resting orders: {e:#}"
                );
                return;
            }
        };
        let refresh = self.refresh_for(market, updates);
        let relayed = refresh.as_ref().map_or(0, |(_, u)| u.len());
        let tx = {
            let mut b = Builder::new(&self.p.engine, &unresolved);
            b.refresh(refresh.as_ref().map(|(relay, u)| (relay, u.as_slice())));
            b.adl(
                &market.objects,
                setup.cap,
                setup.registry,
                account_id as u64,
                &cancel,
                &plan,
            );
            b.finish()
        };
        let tx = match self.prepare(tx).await {
            Ok(tx) => tx,
            Err(e) => {
                let message =
                    format!("{ticker}: ADL of account {account_id} would not go through: {e}");
                if self
                    .p
                    .board
                    .set_problem(&problem, Level::Critical, message.clone())
                {
                    self.p.alerts.raise(Level::Critical, &problem, message);
                }
                self.record(
                    Kind::Adl,
                    &ticker,
                    account_id,
                    "aborted",
                    None,
                    Some(e.to_string()),
                );
                return;
            }
        };
        match self.submit(Kind::Adl, tx, relayed).await {
            Submitted::Simulated(outcome) => {
                self.record(
                    Kind::Adl,
                    &ticker,
                    account_id,
                    "simulated",
                    Some(outcome.digest),
                    Some(summary.join(", ")),
                );
            }
            Submitted::Executed(outcome) if outcome.success => {
                self.p.board.clear_problem(&problem);
                self.p.alerts.raise(
                    Level::Warning,
                    &problem,
                    format!(
                        "{ticker}: account {account_id} was auto-deleveraged: {}",
                        summary.join(", ")
                    ),
                );
                self.record(
                    Kind::Adl,
                    &ticker,
                    account_id,
                    "executed",
                    Some(outcome.digest),
                    Some(summary.join(", ")),
                );
            }
            Submitted::Executed(outcome) => {
                let error = outcome.error.unwrap_or_default();
                self.p.alerts.raise(
                    Level::Critical,
                    &problem,
                    format!("{ticker}: ADL of account {account_id} failed on chain: {error}"),
                );
                self.record(
                    Kind::Adl,
                    &ticker,
                    account_id,
                    "failed",
                    Some(outcome.digest),
                    Some(error),
                );
            }
            Submitted::Refused(reason) => {
                self.record(
                    Kind::Adl,
                    &ticker,
                    account_id,
                    "refused",
                    None,
                    Some(reason),
                );
            }
            Submitted::Rejected(error) => {
                self.record(
                    Kind::Adl,
                    &ticker,
                    account_id,
                    "rejected",
                    None,
                    Some(error),
                );
            }
            Submitted::Unknown { digest, error } => {
                self.record(
                    Kind::Adl,
                    &ticker,
                    account_id,
                    "unknown",
                    Some(digest.clone()),
                    Some(error),
                );
                self.pending.push(Pending {
                    digest,
                    kind: Kind::Adl,
                    ticker,
                    account_id,
                    since: Instant::now(),
                });
            }
        }
    }

    /// Sells (or buys back) what is left of positions taken over, at most the slippage away
    /// from the mark price.
    async fn unwind_inventory(
        &mut self,
        views: &HashMap<String, MarketView>,
        updates: Option<&Updates>,
    ) {
        for market in self.p.markets.clone() {
            let inventory = self.inventory(&market.db_id);
            if inventory.is_zero() {
                self.p
                    .board
                    .clear_problem(&format!("inventory:{}", market.ticker));
                continue;
            }
            let Some(view) = views.get(&market.db_id) else {
                continue;
            };
            let ticker = market.ticker.clone();
            let value = (inventory.abs() * &view.valuation.mark_price)
                .to_f64()
                .unwrap_or_default();
            let problem = format!("inventory:{ticker}");
            if value > self.p.settings.max_inventory_usd {
                let message = format!(
                    "{ticker}: the liquidator holds {} ({value:.0} USD), more than --max-inventory-usd, and the book is not taking it",
                    perp_engine::decimal::plain(&inventory)
                );
                if self
                    .p
                    .board
                    .set_problem(&problem, Level::Warning, message.clone())
                {
                    self.p.alerts.raise(Level::Warning, &problem, message);
                }
            } else {
                self.p.board.clear_problem(&problem);
            }
            if !view.tradable
                || self
                    .last_unwind
                    .get(&market.db_id)
                    .is_some_and(|at| at.elapsed() < self.p.settings.unwind_interval)
            {
                continue;
            }
            self.last_unwind
                .insert(market.db_id.clone(), Instant::now());
            let is_ask = inventory.is_positive();
            let (Some(size), Some(price)) = (
                risk::lots_covering(&inventory, view.lot_size),
                risk::limit_price(
                    &view.valuation.mark_price,
                    is_ask,
                    &self.p.settings.unwind_slippage,
                    view.tick_size,
                ),
            ) else {
                continue;
            };
            let unwind = Unwind {
                is_ask,
                size,
                price,
            };
            let refresh = self.refresh_for(&market, updates);
            let relayed = refresh.as_ref().map_or(0, |(_, u)| u.len());
            let tx = {
                let mut b = Builder::new(&self.p.engine, &unresolved);
                b.refresh(refresh.as_ref().map(|(relay, u)| (relay, u.as_slice())));
                b.unwind(&market.objects, &unwind);
                b.finish()
            };
            let account_id = self.p.account_id;
            let tx = match self.prepare(tx).await {
                Ok(tx) => tx,
                Err(BuildError::Aborted(abort)) => {
                    let reason = classify(&abort.module, abort.code);
                    match reason {
                        // Nothing within the slippage, or the indexer is behind a fill already
                        // made: try again next interval.
                        Reason::NoFill | Reason::NothingToReduce => {
                            debug!(
                                market = ticker,
                                "Nothing to unwind into: {}",
                                reason.label()
                            );
                            self.p
                                .metrics
                                .attempts
                                .with_label_values(&[ticker.as_str(), "unwind", reason.label()])
                                .inc();
                        }
                        _ => {
                            self.aborted(
                                Kind::Unwind,
                                &market,
                                account_id,
                                None,
                                abort,
                                view,
                                updates,
                            )
                            .await
                        }
                    }
                    continue;
                }
                Err(e) => {
                    debug!(market = ticker, "Could not build the unwind: {e}");
                    continue;
                }
            };
            match self.submit(Kind::Unwind, tx, relayed).await {
                Submitted::Simulated(outcome) => {
                    self.record(
                        Kind::Unwind,
                        &ticker,
                        account_id,
                        "simulated",
                        Some(outcome.digest),
                        None,
                    );
                }
                Submitted::Executed(outcome) if outcome.success => {
                    let report = report::read(
                        &outcome.events,
                        &self.p.engine.types_package,
                        account_id as u64,
                    );
                    info!(
                        market = ticker,
                        digest = outcome.digest,
                        "Unwound {} of {}",
                        perp_engine::decimal::plain(&report.base_traded),
                        perp_engine::decimal::plain(&inventory)
                    );
                    self.record(
                        Kind::Unwind,
                        &ticker,
                        account_id,
                        "executed",
                        Some(outcome.digest),
                        Some(describe(&report)),
                    );
                }
                Submitted::Executed(outcome) => {
                    self.record(
                        Kind::Unwind,
                        &ticker,
                        account_id,
                        "failed",
                        Some(outcome.digest),
                        outcome.error,
                    );
                }
                Submitted::Refused(reason) => {
                    self.record(
                        Kind::Unwind,
                        &ticker,
                        account_id,
                        "refused",
                        None,
                        Some(reason),
                    );
                }
                Submitted::Rejected(error) => {
                    self.record(
                        Kind::Unwind,
                        &ticker,
                        account_id,
                        "rejected",
                        None,
                        Some(error),
                    );
                }
                Submitted::Unknown { digest, error } => {
                    self.record(
                        Kind::Unwind,
                        &ticker,
                        account_id,
                        "unknown",
                        Some(digest.clone()),
                        Some(error),
                    );
                    self.pending.push(Pending {
                        digest,
                        kind: Kind::Unwind,
                        ticker,
                        account_id,
                        since: Instant::now(),
                    });
                }
            }
        }
    }

    /// Looks up transactions sent without an answer.
    async fn resolve_pending(&mut self) {
        let pending = std::mem::take(&mut self.pending);
        for p in pending {
            match self.p.chain.transaction(&p.digest).await {
                Ok(Some(outcome)) => {
                    let outcome_label = if outcome.success {
                        "executed"
                    } else {
                        "failed"
                    };
                    info!(
                        digest = p.digest,
                        "Transaction with no answer was found: {outcome_label}"
                    );
                    self.record(
                        p.kind,
                        &p.ticker,
                        p.account_id,
                        outcome_label,
                        Some(p.digest),
                        outcome.error,
                    );
                }
                Ok(None) if p.since.elapsed() < PENDING_FOR => self.pending.push(p),
                Ok(None) => {
                    warn!(digest = p.digest, "Transaction with no answer never landed");
                    self.record(
                        p.kind,
                        &p.ticker,
                        p.account_id,
                        "dropped",
                        Some(p.digest),
                        None,
                    );
                }
                Err(e) => {
                    debug!(
                        digest = p.digest,
                        "Failed to look up the transaction: {e:#}"
                    );
                    if p.since.elapsed() < PENDING_FOR {
                        self.pending.push(p);
                    }
                }
            }
        }
    }

    async fn check_balances(&mut self) {
        match self.p.chain.balance(self.p.key.address, HANEUL).await {
            Ok(raw) => {
                let haneul = raw as f64 / GEUNHWA_PER_HANEUL;
                self.p.metrics.gas_balance.set(haneul);
                self.p.board.update(|s| s.gas_balance_haneul = Some(haneul));
                // Below one transaction's budget nothing more can be sent.
                let level = if raw < self.p.settings.max_gas_budget {
                    Level::Critical
                } else {
                    Level::Warning
                };
                if haneul < self.p.settings.min_gas_balance {
                    let message =
                        format!("The liquidator's address holds {haneul:.3} HANEUL for gas");
                    if self.p.board.set_problem("gas", level, message.clone()) {
                        self.p.alerts.raise(level, "gas", message);
                    }
                } else if self.p.board.clear_problem("gas") {
                    info!("Gas balance is back at {haneul:.3} HANEUL");
                }
            }
            Err(e) => debug!("Failed to read the gas balance: {e:#}"),
        }
        match self.p.store.account(&self.p.account_db_id).await {
            Ok(Some(account)) => {
                let collateral = (account.collateral
                    / perp_engine::decimal::pow10(self.p.collateral_decimals))
                .to_f64()
                .unwrap_or_default();
                self.p.metrics.account_collateral.set(collateral);
                self.p
                    .board
                    .update(|s| s.account_collateral = Some(collateral));
                if collateral < self.p.settings.min_collateral {
                    let message = format!(
                        "The liquidator's account has {collateral:.2} of collateral left to take positions over with"
                    );
                    if self
                        .p
                        .board
                        .set_problem("collateral", Level::Warning, message.clone())
                    {
                        self.p.alerts.raise(Level::Warning, "collateral", message);
                    }
                } else {
                    self.p.board.clear_problem("collateral");
                }
            }
            Ok(None) => debug!("The indexer does not have the liquidator's account"),
            Err(e) => debug!("Failed to read the account: {e:#}"),
        }
    }
}

/// An integer field, which Move JSON writes as a number or, past 32 bits, as a string.
fn json_i64(value: &serde_json::Value) -> Option<i64> {
    match value {
        serde_json::Value::Number(n) => n.as_i64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn position(row: &PositionRow) -> Position {
    Position {
        collateral: row.collateral.clone(),
        base: row.base.clone(),
        quote_notional: row.quote_notional.clone(),
        cum_funding_rate_long: row.cum_funding_rate_long.clone(),
        cum_funding_rate_short: row.cum_funding_rate_short.clone(),
        asks_quantity: row.asks_quantity.clone(),
        bids_quantity: row.bids_quantity.clone(),
        pending_orders: row.pending_orders,
        initial_margin_ratio: row.initial_margin_ratio.clone(),
    }
}

/// Collapses outcomes with details (`aborted: reason`) to their kind, for metric labels.
fn outcome_label(outcome: &str) -> &str {
    outcome.split(':').next().unwrap_or(outcome)
}

fn usd(value: &BigDecimal) -> String {
    perp_engine::decimal::plain(&value.with_scale(2))
}

fn describe(report: &Report) -> String {
    format!(
        "took over {} for {} USD, fees {} USD, unwound {}, bad debt {} USD, profit {} USD",
        perp_engine::decimal::plain(&report.base_liquidated),
        usd(&report.quote_liquidated),
        usd(&report.liquidation_fees),
        perp_engine::decimal::plain(&report.base_traded),
        usd(&report.bad_debt),
        usd(&report.profit()),
    )
}
