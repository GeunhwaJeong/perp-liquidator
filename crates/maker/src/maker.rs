// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The quoting loop: look at the reference and the position every poll, decide whether a round
//! is due, build it as one transaction, simulate, send, and remember what rests.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bigdecimal::{BigDecimal, ToPrimitive, Zero};
use haneul_crypto::HaneulSigner;
use haneul_transaction_builder::{ObjectInput, TransactionBuilder};
use perp_bot_common::alerts::{Alerts, Level};
use perp_bot_common::chain::{Abort, BuildError, Chain, ExecuteError, Outcome};
use perp_bot_common::keys::Key;
use perp_bot_common::oracle::{OracleClient, SignedUpdate, Updates};
use perp_engine::Position;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::market::{self, MarketView};
use crate::metrics::Metrics;
use crate::model::{self, Inventory, Observation, Params, Quote};
use crate::ptb::{Builder, Round};
use crate::setup::Role;
use crate::status::{Board, LevelStatus, RoundRecord, now_ms};

const HANEUL: &str = "0x2::haneul::HANEUL";
const HANEUL_UNIT: f64 = 1e9;
const REFRESH_EVERY: Duration = Duration::from_secs(60);
/// The collateral feed is relayed only when the stored price is older than this: it is shared
/// by every market in the collateral, so writing it from every round would order all of them
/// behind one object.
const COLLATERAL_RELAY_AGE_MS: u64 = 15_000;
/// `clearing_house::EPostOnlyOrderWouldMatch`.
const POST_ONLY_WOULD_MATCH: u64 = 47;
/// `clearing_house::ENotEnoughCollateralToAllocateForSession`.
const NOT_ENOUGH_COLLATERAL: u64 = 30;
/// `market::EBadIndexPrice`.
const BAD_INDEX_PRICE: u64 = 1000;
/// `position::EInitialMarginRequirementViolated`.
const MARGIN_VIOLATED: u64 = 2001;

pub struct Settings {
    pub dry_run: bool,
    pub poll_interval: Duration,
    pub params: Params,
    pub requote_bps: BigDecimal,
    pub refresh: Duration,
    pub expire: Duration,
    pub min_interval: Duration,
    pub vol_alpha: BigDecimal,
    pub max_price_age: Duration,
    pub max_confidence_bps: BigDecimal,
    pub max_failures: u32,
    pub retry: Duration,
    pub max_gas_budget: u64,
    pub min_gas_balance: f64,
    pub params_refresh: Duration,
    pub base_source_id: u64,
    pub collateral_source_id: u64,
}

pub struct Parts {
    pub settings: Settings,
    pub chain: Chain,
    pub key: Key,
    pub role: Role,
    pub oracle: OracleClient,
    pub metrics: Arc<Metrics>,
    pub alerts: Arc<Alerts>,
    pub board: Arc<Board>,
    /// Orders already resting when the maker starts, to cancel in the first round.
    pub resting: Vec<u128>,
}

/// What the maker remembers between polls.
#[derive(Default)]
struct State {
    last_reference: BigDecimal,
    sigma_bps: BigDecimal,
    last_round_at: Option<Instant>,
    last_quote: Option<Quote>,
    /// The order IDs the last round posted, as far as known.
    resting: Vec<u128>,
    /// (base, bids, asks) of the position after the last round.
    last_position: Option<(BigDecimal, BigDecimal, BigDecimal)>,
    failures: u32,
    paused_until: Option<Instant>,
    client_ids: u64,
    view_read_at: Option<Instant>,
}

pub struct Maker {
    p: Parts,
    s: State,
    gas_price: Option<(u64, Instant)>,
    gas_checked: Option<Instant>,
}

/// What a poll decided.
#[derive(Debug, PartialEq, Eq)]
pub enum Decision {
    /// Nothing to do.
    Idle,
    /// A round was sent (or simulated) for this reason.
    Round(&'static str),
    /// Quotes were pulled: a kill switch, or stopping.
    Pulled(&'static str),
    Failed,
}

impl Maker {
    pub fn new(parts: Parts) -> Self {
        let s = State {
            client_ids: (now_ms() as u64) << 8,
            resting: parts.resting.clone(),
            ..State::default()
        };
        Self {
            p: parts,
            s,
            gas_price: None,
            gas_checked: None,
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) {
        loop {
            if cancel.is_cancelled() {
                break;
            }
            self.poll().await;
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(self.p.settings.poll_interval) => {}
            }
        }
        // The last round only cancels.
        if !self.s.resting.is_empty() && !self.p.settings.dry_run {
            info!("Pulling the quotes before stopping");
            self.pull("stopping").await;
        }
    }

    /// One look at the reference and the position.
    pub async fn poll(&mut self) -> Decision {
        self.check_gas().await;
        if let Some(until) = self.s.paused_until {
            if Instant::now() < until {
                return Decision::Idle;
            }
            self.s.paused_until = None;
            self.s.failures = 0;
            self.p.board.clear_problem("failing");
        }

        // The reference: the signed price of the base feed, fresh and confident enough.
        let updates = match self.p.oracle.fetch().await {
            Ok(u) => u,
            Err(e) => {
                warn!("Failed to fetch signed prices: {e:#}");
                return self
                    .kill("oracle", "the oracle service does not answer")
                    .await;
            }
        };
        let Some(update) = updates.get(&self.p.role.market.base_feed).cloned() else {
            return self
                .kill(
                    "oracle",
                    "the oracle service has no signed price for the base feed",
                )
                .await;
        };
        let age_ms = (now_ms() as u64).saturating_sub(update.timestamp_ms);
        self.p.metrics.price_age.set(age_ms as f64 / 1000.0);
        if age_ms > self.p.settings.max_price_age.as_millis() as u64 {
            return self
                .kill(
                    "price_age",
                    &format!("the signed price is {} s old", age_ms / 1000),
                )
                .await;
        }
        let reference = update.price();
        let confidence_bps = if reference.is_zero() {
            BigDecimal::zero()
        } else {
            BigDecimal::new(update.confidence.into(), 18) / &reference * BigDecimal::from(10_000)
        };
        if confidence_bps > self.p.settings.max_confidence_bps {
            return self
                .kill(
                    "confidence",
                    &format!("the signed price's confidence is {confidence_bps:.1} bp"),
                )
                .await;
        }
        for key in ["oracle", "price_age", "confidence"] {
            self.p.board.clear_problem(key);
        }

        // The market and the position.
        let view = match self.view().await {
            Ok(view) => view,
            Err(e) => {
                warn!("Failed to read the market: {e:#}");
                return Decision::Failed;
            }
        };
        if !view.trades() {
            let reason = if view.paused == 2 { "closed" } else { "paused" };
            return self
                .kill("market", &format!("the market is {reason}"))
                .await;
        }
        self.p.board.clear_problem("market");
        let (position, mark, collateral_price) = match self.position_and_marks(&view).await {
            Ok(x) => x,
            Err(e) => {
                warn!("Failed to read the position: {e:#}");
                return Decision::Failed;
            }
        };
        let valuation = view.valuation(mark, collateral_price);
        let health = market::health(&position, &valuation, &view.margin_ratio_maintenance);
        // Every fill moves the position; the resting quantities also move with the maker's
        // own posting and cancelling, so they are not a sign of one.
        let fills = self
            .s
            .last_position
            .as_ref()
            .is_some_and(|(base, _, _)| *base != position.base);
        if fills {
            self.p.metrics.fills.inc();
        }

        // The quote.
        self.s.sigma_bps = model::update_sigma(
            &self.s.sigma_bps,
            &self.s.last_reference,
            &reference,
            &self.p.settings.vol_alpha,
        );
        let mut params = self.p.settings.params.clone();
        params.tick = view.tick_size;
        params.lot = view.lot_size;
        params.min_order_usd = view.min_order_usd.clone();
        let observation = Observation {
            reference: reference.clone(),
            confidence_bps,
            sigma_bps: self.s.sigma_bps.clone(),
            position: position.base.clone(),
            health: health.clone(),
        };
        let quote = model::quote(&params, &observation);
        self.observe(&quote, &position, &health);
        self.s.last_reference = reference.clone();

        // Is a round due?
        let reason = self.reason(&quote, fills);
        let Some(reason) = reason else {
            return Decision::Idle;
        };
        if let Some(at) = self.s.last_round_at
            && at.elapsed() < self.p.settings.min_interval
        {
            debug!(reason, "A round is due but the last one was too recent");
            return Decision::Idle;
        }
        self.p.metrics.reasons.with_label_values(&[reason]).inc();

        // Build it.
        let now = now_ms() as u64;
        let round = Round {
            cancel: self.s.resting.clone(),
            levels: quote.levels.clone(),
            flatten: quote.flatten.clone(),
            expires_at_ms: now + self.p.settings.expire.as_millis() as u64,
            client_id_base: self.next_client_ids(quote.levels.len() as u64),
        };
        let relayed = self.relayed(&updates, &update).await;
        let mut b = Builder::new(&self.p.role.engine, &ObjectInput::new);
        b.refresh(Some((&updates.relay, &relayed)))
            .round(&self.p.role.market, &round);
        let commands = b.commands();
        let tx = b.finish();
        match self.send(tx, commands, reason, round.levels.len()).await {
            Ok(outcome) => {
                self.s.resting = if self.p.settings.dry_run {
                    Vec::new()
                } else {
                    posted_order_ids(&outcome)
                };
                self.s.last_round_at = Some(Instant::now());
                self.s.last_quote = Some(quote.clone());
                self.s.last_position = Some((
                    position.base.clone(),
                    position.bids_quantity.clone(),
                    position.asks_quantity.clone(),
                ));
                self.s.failures = 0;
                self.p.board.clear_problem("failing");
                let quoting = !round.levels.is_empty();
                self.p.metrics.quoting.set(quoting as i64);
                self.p.board.update(|s| s.quoting = quoting);
                info!(
                    reason,
                    digest = outcome.digest,
                    levels = round.levels.len(),
                    resting = self.s.resting.len(),
                    mid = %quote.mid.with_scale(2),
                    half_spread_bps = %quote.half_spread_bps.with_scale(2),
                    inventory = quote.inventory.label(),
                    "Round"
                );
                Decision::Round(reason)
            }
            Err(message) => {
                self.failed(&message);
                Decision::Failed
            }
        }
    }

    /// Why a round is due now, if it is.
    fn reason(&self, quote: &Quote, fills: bool) -> Option<&'static str> {
        let Some(last) = &self.s.last_quote else {
            return Some("start");
        };
        if quote.flatten.is_some() {
            return Some("flatten");
        }
        if fills {
            return Some("fills");
        }
        if model::moved(
            &last.reference,
            &quote.reference,
            &self.p.settings.requote_bps,
        ) {
            return Some("moved");
        }
        let shape = |q: &Quote| {
            (
                q.inventory,
                q.levels.iter().filter(|l| l.is_bid).count(),
                q.levels.iter().filter(|l| !l.is_bid).count(),
            )
        };
        if shape(last) != shape(quote) {
            return Some("shape");
        }
        if !quote.levels.is_empty()
            && self
                .s
                .last_round_at
                .is_none_or(|at| at.elapsed() >= self.p.settings.refresh)
        {
            return Some("refresh");
        }
        None
    }

    /// Cancels everything resting and quotes nothing until the condition clears.
    async fn kill(&mut self, key: &'static str, message: &str) -> Decision {
        let was_new = self
            .p
            .board
            .set_problem(key, Level::Warning, message.to_owned());
        if was_new {
            self.p
                .alerts
                .raise(Level::Warning, key, format!("Quoting stopped: {message}"));
        }
        if self.s.resting.is_empty() && self.s.last_quote.is_none() {
            return Decision::Idle;
        }
        if self.s.resting.is_empty() {
            self.s.last_quote = None;
            return Decision::Idle;
        }
        self.pull(key).await
    }

    async fn pull(&mut self, reason: &'static str) -> Decision {
        if self.p.settings.dry_run {
            self.s.resting.clear();
            self.s.last_quote = None;
            return Decision::Pulled(reason);
        }
        let mut b = Builder::new(&self.p.role.engine, &ObjectInput::new);
        b.pull(&self.p.role.market, &self.s.resting);
        let commands = b.commands();
        match self.send(b.finish(), commands, "pull", 0).await {
            Ok(_) => {
                self.s.resting.clear();
                self.s.last_quote = None;
                self.p.metrics.quoting.set(0);
                self.p.board.update(|s| {
                    s.quoting = false;
                    s.ladder.clear();
                    s.resting_orders = 0;
                });
                info!(reason, "Pulled the quotes");
                Decision::Pulled(reason)
            }
            Err(message) => {
                self.failed(&message);
                Decision::Failed
            }
        }
    }

    /// Builds, prices, simulates and (unless a dry run) sends.
    async fn send(
        &mut self,
        mut tx: TransactionBuilder,
        commands: String,
        reason: &'static str,
        levels: usize,
    ) -> Result<Outcome, String> {
        tx.set_sender(self.p.key.address);
        if let Some(price) = self.gas_price().await {
            tx.set_gas_price(price);
        }
        let tx = match self.p.chain.build(tx).await {
            Ok(tx) => tx,
            Err(e) => {
                let message = match &e {
                    BuildError::Aborted(abort) => format!("{} [{}]", describe(abort), commands),
                    other => other.to_string(),
                };
                self.p.metrics.rounds.with_label_values(&["refused"]).inc();
                return Err(message);
            }
        };
        let budget = tx.gas_payment.budget;
        if budget > self.p.settings.max_gas_budget {
            self.p.metrics.rounds.with_label_values(&["refused"]).inc();
            return Err(format!(
                "gas budget {budget} is above --max-gas-budget {}",
                self.p.settings.max_gas_budget
            ));
        }
        if self.p.settings.dry_run {
            return match self.p.chain.simulate(&tx).await {
                Ok(outcome) if outcome.success => {
                    self.p
                        .metrics
                        .rounds
                        .with_label_values(&["simulated"])
                        .inc();
                    self.p.board.round_done(RoundRecord {
                        at_ms: now_ms(),
                        reason: reason.to_owned(),
                        outcome: "simulated".to_owned(),
                        digest: None,
                        levels,
                        gas: outcome.gas_used,
                    });
                    Ok(outcome)
                }
                Ok(outcome) => Err(failure(&outcome)),
                Err(e) => Err(format!("simulation failed: {e:#}")),
            };
        }
        let signature = self
            .p
            .key
            .private
            .sign_transaction(&tx)
            .map_err(|e| format!("signing failed: {e}"))?;
        match self.p.chain.execute(&tx, signature).await {
            Ok(outcome) if outcome.success => {
                self.p.metrics.rounds.with_label_values(&["executed"]).inc();
                self.p
                    .metrics
                    .gas_spent
                    .inc_by(outcome.gas_used.max(0) as u64);
                self.p.board.round_done(RoundRecord {
                    at_ms: now_ms(),
                    reason: reason.to_owned(),
                    outcome: "executed".to_owned(),
                    digest: Some(outcome.digest.clone()),
                    levels,
                    gas: outcome.gas_used,
                });
                Ok(outcome)
            }
            Ok(outcome) => {
                self.p.metrics.rounds.with_label_values(&["failed"]).inc();
                // It landed and failed: nothing changed on chain, the orders still rest.
                Err(failure(&outcome))
            }
            Err(ExecuteError::Rejected(e)) => {
                self.p.metrics.rounds.with_label_values(&["rejected"]).inc();
                Err(format!("rejected: {e}"))
            }
            Err(ExecuteError::Unknown(e)) => {
                self.p.metrics.rounds.with_label_values(&["unknown"]).inc();
                // It may have landed: the resting IDs are no longer trusted, so the next
                // round cancels nothing it does not find and the expiry clears the rest.
                self.s.resting.clear();
                Err(format!("no answer: {e}"))
            }
        }
    }

    fn failed(&mut self, message: &str) {
        self.s.failures += 1;
        warn!(failures = self.s.failures, "{message}");
        if self.s.failures >= self.p.settings.max_failures {
            let text = format!(
                "{} rounds failed in a row, quoting stops for {} s: {message}",
                self.s.failures,
                self.p.settings.retry.as_secs()
            );
            if self
                .p
                .board
                .set_problem("failing", Level::Critical, text.clone())
            {
                self.p.alerts.raise(Level::Critical, "failing", text);
            }
            self.s.paused_until = Some(Instant::now() + self.p.settings.retry);
            // Whatever rests expires on its own.
            self.s.last_quote = None;
        }
    }

    /// The updates to relay: the base feed's always, the collateral's when stale.
    async fn relayed(&self, updates: &Updates, base: &SignedUpdate) -> Vec<SignedUpdate> {
        let mut out = vec![base.clone()];
        if let Some(collateral) = updates.get(&self.p.role.market.collateral_feed) {
            let stale = match self
                .p
                .chain
                .object(self.p.role.market.collateral_feed)
                .await
            {
                Ok(object) => feed_timestamp(&object.json, self.p.settings.collateral_source_id)
                    .is_none_or(|t| (now_ms() as u64).saturating_sub(t) > COLLATERAL_RELAY_AGE_MS),
                Err(_) => true,
            };
            if stale {
                out.push(collateral.clone());
            }
        }
        out
    }

    async fn view(&mut self) -> anyhow::Result<MarketView> {
        let object = self
            .p
            .chain
            .object(self.p.role.market.clearing_house)
            .await?;
        let view = MarketView::from_json(&object.json)?;
        self.s.view_read_at = Some(Instant::now());
        Ok(view)
    }

    async fn position_and_marks(
        &self,
        _view: &MarketView,
    ) -> anyhow::Result<(Position, BigDecimal, BigDecimal)> {
        let position = market::position(
            &self.p.chain,
            self.p.role.market.clearing_house,
            self.p.role.engine.types_package,
            self.p.role.account_id,
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("the position object is gone"))?;
        let base = self.p.chain.object(self.p.role.market.base_feed).await?;
        let mark = market::feed_twap(&base.json, self.p.settings.base_source_id)?;
        let collateral = self
            .p
            .chain
            .object(self.p.role.market.collateral_feed)
            .await?;
        let collateral_price =
            market::feed_twap(&collateral.json, self.p.settings.collateral_source_id)?;
        Ok((position, mark, collateral_price))
    }

    fn observe(&self, quote: &Quote, position: &Position, health: &Option<BigDecimal>) {
        let m = &self.p.metrics;
        m.reference.set(quote.reference.to_f64().unwrap_or(0.0));
        m.half_spread_bps
            .set(quote.half_spread_bps.to_f64().unwrap_or(0.0));
        m.skew_bps.set(quote.skew_bps.to_f64().unwrap_or(0.0));
        m.sigma_bps.set(self.s.sigma_bps.to_f64().unwrap_or(0.0));
        m.position.set(position.base.to_f64().unwrap_or(0.0));
        m.health.set(
            health
                .as_ref()
                .and_then(|h| h.to_f64())
                .unwrap_or(f64::INFINITY),
        );
        let bids = quote.levels.iter().filter(|l| l.is_bid).count();
        let asks = quote.levels.len() - bids;
        m.quotes.with_label_values(&["bid"]).set(bids as f64);
        m.quotes.with_label_values(&["ask"]).set(asks as f64);
        let ladder: Vec<LevelStatus> = quote
            .levels
            .iter()
            .map(|l| LevelStatus {
                side: if l.is_bid { "bid" } else { "ask" },
                price: units(l.price),
                size: units(l.size),
                reduce_only: l.reduce_only,
            })
            .collect();
        let resting = self.s.resting.len();
        let sigma = self.s.sigma_bps.with_scale(2).to_string();
        self.p.board.update(|s| {
            s.reference = Some(quote.reference.with_scale(2).to_string());
            s.mid = Some(quote.mid.with_scale(2).to_string());
            s.half_spread_bps = Some(quote.half_spread_bps.with_scale(2).to_string());
            s.skew_bps = Some(quote.skew_bps.with_scale(2).to_string());
            s.sigma_bps = Some(sigma);
            s.inventory = Some(quote.inventory.label());
            s.position = Some(position.base.with_scale(6).to_string());
            s.health = health.as_ref().map(|h| h.with_scale(2).to_string());
            s.ladder = ladder;
            s.resting_orders = resting;
        });
        if quote.inventory != Inventory::Balanced {
            let key = "inventory";
            let message = format!(
                "inventory {} ({}): only the reducing side is quoted",
                position.base.with_scale(4),
                quote.inventory.label()
            );
            if self
                .p
                .board
                .set_problem(key, Level::Warning, message.clone())
            {
                self.p.alerts.raise(Level::Warning, key, message);
            }
        } else if self.p.board.clear_problem("inventory") {
            self.p
                .alerts
                .raise(Level::Info, "inventory", "inventory is back in range");
        }
    }

    fn next_client_ids(&mut self, count: u64) -> u64 {
        let base = self.s.client_ids;
        self.s.client_ids += count.max(1);
        base
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
                    let level = if haneul < self.p.settings.min_gas_balance / 10.0 {
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

/// The order IDs of the `PostedOrder` events of an executed round. The event's BCS starts with
/// the clearing house ID (32 bytes), the account ID (u64) and the order ID (u128).
pub fn posted_order_ids(outcome: &Outcome) -> Vec<u128> {
    outcome
        .events
        .iter()
        .filter(|(t, _)| {
            t.split('<')
                .next()
                .unwrap_or_default()
                .ends_with("::events::PostedOrder")
        })
        .filter_map(|(_, bytes)| {
            let slice: [u8; 16] = bytes.get(40..56)?.try_into().ok()?;
            Some(u128::from_le_bytes(slice))
        })
        .collect()
}

/// A feed's latest timestamp, from its storage object.
fn feed_timestamp(json: &serde_json::Value, source_id: u64) -> Option<u64> {
    let feeds = json["feeds"].as_array()?;
    let feed = feeds.iter().find(|f| {
        feeds.len() == 1
            || f["source_id"].as_u64() == Some(source_id)
            || f["source_id"].as_str().and_then(|s| s.parse::<u64>().ok()) == Some(source_id)
    })?;
    match &feed["timestamp_ms"] {
        serde_json::Value::String(s) => s.parse().ok(),
        serde_json::Value::Number(n) => n.as_u64(),
        _ => None,
    }
}

fn units(raw: u64) -> String {
    BigDecimal::new(raw.into(), 9).normalized().to_string()
}

/// What an abort while building a round means.
pub fn describe(abort: &Abort) -> String {
    match (abort.module.as_str(), abort.code) {
        ("clearing_house", POST_ONLY_WOULD_MATCH) => {
            "a post-only level would have matched: the book moved inside the spread".to_owned()
        }
        ("clearing_house", NOT_ENOUGH_COLLATERAL) => {
            "the account's unallocated balance cannot cover the ladder's margin".to_owned()
        }
        ("position", MARGIN_VIOLATED) => {
            "the ladder exceeds the position's initial margin: allocate more or lower --size"
                .to_owned()
        }
        ("market", BAD_INDEX_PRICE) => "the base price is stale on chain".to_owned(),
        _ => format!(
            "aborts in {}::{} with {}{}",
            abort.module,
            abort.function,
            abort.code,
            abort
                .command
                .map(|c| format!(" (command {c})"))
                .unwrap_or_default()
        ),
    }
}

fn failure(outcome: &Outcome) -> String {
    match (&outcome.abort, &outcome.error) {
        (Some(abort), _) => format!("{} failed: {}", outcome.digest, describe(abort)),
        (None, Some(error)) => format!("{} failed: {error}", outcome.digest),
        (None, None) => format!("{} failed", outcome.digest),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_posted_order_ids_from_the_events() {
        let mut bytes = vec![0u8; 40];
        bytes.extend_from_slice(&123_456_789u128.to_le_bytes());
        bytes.extend_from_slice(&[9; 20]);
        let outcome = Outcome {
            digest: "d".into(),
            success: true,
            abort: None,
            error: None,
            events: vec![
                ("0xe1::events::PostedOrder".into(), bytes.clone()),
                ("0xe1::events::CanceledOrder".into(), bytes),
                ("0xe1::events::PostedOrder".into(), vec![0; 10]),
            ],
            gas_used: 0,
            checkpoint: None,
        };
        assert_eq!(posted_order_ids(&outcome), vec![123_456_789]);
    }

    #[test]
    fn says_what_an_abort_means() {
        let abort = |module: &str, code: u64| Abort {
            package: "0xe2".into(),
            module: module.into(),
            function: "f".into(),
            code,
            command: None,
        };
        assert!(describe(&abort("clearing_house", 47)).contains("post-only"));
        assert!(describe(&abort("position", 2001)).contains("margin"));
        assert_eq!(
            describe(&abort("clearing_house", 32)),
            "aborts in clearing_house::f with 32"
        );
    }
}
