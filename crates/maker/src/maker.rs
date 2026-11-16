// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The quoting loop: look at the reference and the position every poll, decide whether a round
//! is due, build it as one transaction, simulate, send, and remember what rests. A level that
//! would match a resting order is left out of the round; a round that fails pulls what rests
//! and is tried again; a transaction that got no answer is asked for until the full node tells
//! what became of it (`failures`).

use std::sync::Arc;
use std::time::{Duration, Instant};

use bigdecimal::{BigDecimal, ToPrimitive, Zero};
use haneul_crypto::HaneulSigner;
use haneul_sdk_types::Transaction;
use haneul_transaction_builder::{ObjectInput, TransactionBuilder};
use perp_bot_common::alerts::{Alerts, Level};
use perp_bot_common::chain::{Abort, BuildError, Chain, ExecuteError, Outcome};
use perp_bot_common::keys::Key;
use perp_bot_common::oracle::{OracleClient, SignedUpdate, Updates};
use perp_engine::Position;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::failures::{self, Failures, POST_ONLY_WOULD_MATCH, Verdict};
use crate::market::{self, MarketView};
use crate::metrics::Metrics;
use crate::model::{self, Inventory, Level as Quoted, Observation, Params, Quote, Sigma};
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
    /// How long rounds may fail in a row before it is a critical problem.
    pub failing_alert_after: Duration,
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
    sigma: Sigma,
    /// When the last round went through: the refresh counts from it.
    last_round_at: Option<Instant>,
    /// When a round, or the pull after a failed read, was last tried, whether it went through
    /// or not: the pace counts from it.
    last_attempt_at: Option<Instant>,
    last_quote: Option<Quote>,
    /// The order IDs the last round posted, as far as known.
    resting: Vec<u128>,
    /// The levels the last round posted, and when they expire. Past that time the engine no
    /// longer matches them, whether or not they were cancelled.
    posted: Vec<Quoted>,
    expires_at_ms: u64,
    /// (base, bids, asks) of the position after the last round.
    last_position: Option<(BigDecimal, BigDecimal, BigDecimal)>,
    /// The position's base at the last poll that read it.
    seen_base: Option<BigDecimal>,
    failures: Failures,
    /// Transactions that were sent and got no answer, until the full node tells what became
    /// of them.
    pending: Vec<Pending>,
    client_ids: u64,
    view_read_at: Option<Instant>,
}

pub struct Maker {
    p: Parts,
    s: State,
    gas_price: Option<(u64, Instant)>,
    gas_checked: Option<Instant>,
}

/// A transaction the full node did not build: its simulation aborted or failed, the node did
/// not answer, or the gas it needs is above the limit. Nothing was sent.
struct Refusal {
    abort: Option<Abort>,
    message: String,
}

/// What a transaction leaves resting when it runs: the levels of a round, until they expire.
#[derive(Clone, Copy)]
struct Posts<'a> {
    levels: &'a [Quoted],
    left_out: usize,
    expires_at_ms: u64,
}

/// A transaction that was sent and got no answer: it may have run.
struct Pending {
    digest: String,
    sent_at: Instant,
    /// The levels it posts when it runs, and when they expire. None for a pull.
    posts: Option<(Vec<Quoted>, u64)>,
    /// The quote and the position a round was built for.
    built_for: Option<(Quote, (BigDecimal, BigDecimal, BigDecimal))>,
}

/// A round that went through.
struct Sent {
    outcome: Outcome,
    /// The levels it posted: the quote's, less the ones that would have matched.
    levels: Vec<Quoted>,
    left_out: usize,
    expires_at_ms: u64,
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
            self.p.board.poll_done();
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
        self.resolve().await;
        self.report_resting();

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
                return self
                    .unread(&format!("the market could not be read: {e:#}"))
                    .await;
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
                return self
                    .unread(&format!("the position could not be read: {e:#}"))
                    .await;
            }
        };
        let valuation = view.valuation(mark, collateral_price);
        let health = market::health(&position, &valuation, &view.margin_ratio_maintenance);
        // Every fill moves the position; the resting quantities also move with the maker's
        // own posting and cancelling, so they are not a sign of one. The round is due for as
        // long as the position differs from the last round's; the counter counts the change
        // once, at the poll that first sees it.
        let fills = self
            .s
            .last_position
            .as_ref()
            .is_some_and(|(base, _, _)| *base != position.base);
        if self
            .s
            .seen_base
            .as_ref()
            .is_some_and(|base| *base != position.base)
        {
            self.p.metrics.fills.inc();
        }
        self.s.seen_base = Some(position.base.clone());

        // The quote.
        self.s
            .sigma
            .observe(&reference, update.timestamp_ms, &self.p.settings.vol_alpha);
        let mut params = self.p.settings.params.clone();
        params.tick = view.tick_size;
        params.lot = view.lot_size;
        params.min_order_usd = view.min_order_usd.clone();
        let observation = Observation {
            reference: reference.clone(),
            confidence_bps,
            sigma_bps: self.s.sigma.bps().clone(),
            position: position.base.clone(),
            health: health.clone(),
        };
        let quote = model::quote(&params, &observation);
        self.observe(&quote, &position, &health);

        // Is a round due?
        let reason = self.reason(&quote, fills);
        let Some(reason) = reason else {
            return Decision::Idle;
        };
        if !failures::due(
            self.s.last_attempt_at,
            self.p.settings.min_interval,
            Instant::now(),
        ) {
            debug!(reason, "A round is due but the last try was too recent");
            return Decision::Idle;
        }
        self.s.last_attempt_at = Some(Instant::now());
        self.p.metrics.reasons.with_label_values(&[reason]).inc();

        let unanswered = self.s.pending.len();
        match self.round(&quote, &updates, &update, reason).await {
            Ok(sent) => {
                self.s.resting = if self.p.settings.dry_run {
                    Vec::new()
                } else {
                    posted_order_ids(&sent.outcome)
                };
                self.s.posted = sent.levels;
                self.s.expires_at_ms = sent.expires_at_ms;
                self.s.last_round_at = Some(Instant::now());
                self.s.last_quote = Some(quote.clone());
                self.s.last_position = Some((
                    position.base.clone(),
                    position.bids_quantity.clone(),
                    position.asks_quantity.clone(),
                ));
                self.went_through();
                self.report_resting();
                info!(
                    reason,
                    digest = sent.outcome.digest,
                    levels = self.s.posted.len(),
                    left_out = sent.left_out,
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
                match self.s.pending.get_mut(unanswered) {
                    // No answer: the round may have run, and a pull on top of it would spend
                    // the same objects. The next poll asks what became of it.
                    Some(pending) => {
                        pending.built_for = Some((
                            quote.clone(),
                            (
                                position.base.clone(),
                                position.bids_quantity.clone(),
                                position.asks_quantity.clone(),
                            ),
                        ));
                    }
                    None => self.retreat().await,
                }
                Decision::Failed
            }
        }
    }

    /// Asks the full node what became of the transactions that got no answer.
    async fn resolve(&mut self) {
        for pending in std::mem::take(&mut self.s.pending) {
            let found = match self.p.chain.transaction(&pending.digest).await {
                Ok(found) => found,
                Err(e) => {
                    debug!(
                        digest = pending.digest,
                        "Failed to look up the transaction: {e:#}"
                    );
                    None
                }
            };
            let verdict = failures::verdict(
                found.as_ref().map(|outcome| outcome.success),
                pending.sent_at.elapsed(),
                self.p.settings.expire,
            );
            match (verdict, found) {
                (Verdict::AskAgain, _) => self.s.pending.push(pending),
                (Verdict::TakeOver, Some(outcome)) => self.take_over(pending, &outcome),
                (Verdict::Forget, Some(outcome)) => warn!(
                    "The transaction that got no answer ran and changed nothing: {}",
                    failure(&outcome)
                ),
                (_, None) => warn!(
                    digest = pending.digest,
                    "The full node has not had the transaction that got no answer for {} s, \
                     and nothing it posted can be matched any more",
                    pending.sent_at.elapsed().as_secs()
                ),
            }
        }
    }

    /// A transaction that got no answer ran: what it posted rests, what it cancelled is gone.
    fn take_over(&mut self, pending: Pending, outcome: &Outcome) {
        info!(
            digest = pending.digest,
            "The transaction that got no answer ran"
        );
        self.p
            .metrics
            .gas_spent
            .inc_by(outcome.gas_used.max(0) as u64);
        let ids = posted_order_ids(outcome);
        // A round that went through after this one was sent says what rests. The orders of
        // this one are then only more to cancel.
        if self.s.last_round_at.is_some_and(|at| at > pending.sent_at) {
            for id in ids {
                if !self.s.resting.contains(&id) {
                    self.s.resting.push(id);
                }
            }
            return;
        }
        let Some((levels, expires_at_ms)) = pending.posts else {
            self.pulled();
            return;
        };
        self.s.resting = ids;
        self.s.posted = levels;
        self.s.expires_at_ms = expires_at_ms;
        self.s.last_round_at = Some(pending.sent_at);
        if let Some((quote, position)) = pending.built_for {
            self.s.last_quote = Some(quote);
            self.s.last_position = Some(position);
        }
        // The missing answer was counted as a failure. When nothing failed after it, the run
        // of failures ends with this round.
        if self.s.failures.count() <= 1 {
            self.went_through();
        }
        self.report_resting();
    }

    /// Builds and sends the round of a quote. A level that would match a resting order is left
    /// out and the round is built again, so that the cancels and the rest of the ladder land:
    /// every try drops one level, and a round without levels has nothing left to match.
    async fn round(
        &mut self,
        quote: &Quote,
        updates: &Updates,
        base: &SignedUpdate,
        reason: &'static str,
    ) -> Result<Sent, String> {
        let expires_at_ms = now_ms() as u64 + self.p.settings.expire.as_millis() as u64;
        let mut round = Round {
            cancel: self.s.resting.clone(),
            levels: quote.levels.clone(),
            flatten: quote.flatten.clone(),
            expires_at_ms,
            client_id_base: self.next_client_ids(quote.levels.len() as u64),
        };
        let relayed = self.relayed(updates, base).await;
        loop {
            let mut b = Builder::new(&self.p.role.engine, &ObjectInput::new);
            b.refresh(Some((&updates.relay, &relayed)))
                .round(&self.p.role.market, &round);
            let commands = b.commands();
            let placed = b.placed().to_vec();
            let tx = match self.build(b.finish(), &commands).await {
                Ok(tx) => tx,
                Err(refusal) => {
                    let crossing = refusal
                        .abort
                        .as_ref()
                        .and_then(|abort| failures::crossing_level(abort, &placed));
                    let Some(index) = crossing else {
                        self.p.metrics.rounds.with_label_values(&["refused"]).inc();
                        return Err(refusal.message);
                    };
                    let level = round.levels.remove(index);
                    self.p.metrics.left_out.inc();
                    debug!(
                        side = if level.is_bid { "bid" } else { "ask" },
                        price = units(level.price),
                        "A level would have matched a resting order and is left out"
                    );
                    continue;
                }
            };
            let left_out = quote.levels.len() - round.levels.len();
            let posts = Posts {
                levels: &round.levels,
                left_out,
                expires_at_ms,
            };
            let outcome = self.submit(tx, reason, Some(posts)).await?;
            return Ok(Sent {
                outcome,
                levels: round.levels,
                left_out,
                expires_at_ms,
            });
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
        if self.s.resting.is_empty() {
            self.s.last_quote = None;
            // No round is tried while the condition lasts, so none fails either.
            self.settle();
            return Decision::Idle;
        }
        self.pull(key).await
    }

    async fn pull(&mut self, reason: &'static str) -> Decision {
        if self.p.settings.dry_run {
            self.pulled();
            return Decision::Pulled(reason);
        }
        let mut b = Builder::new(&self.p.role.engine, &ObjectInput::new);
        b.pull(&self.p.role.market, &self.s.resting);
        let commands = b.commands();
        match self.send(b.finish(), &commands, "pull").await {
            Ok(_) => {
                self.pulled();
                self.settle();
                info!(reason, "Pulled the quotes");
                Decision::Pulled(reason)
            }
            Err(message) => {
                self.failed(&message);
                Decision::Failed
            }
        }
    }

    /// After a round that failed: what rests was posted for a price or a position the maker
    /// could not follow up on, so it is cancelled. A cancel needs no session and no fresh
    /// price, and goes through in most of the cases in which a round does not. When it does
    /// not either, the quotes rest until they expire.
    async fn retreat(&mut self) {
        if self.s.resting.is_empty() || self.p.settings.dry_run {
            return;
        }
        let mut b = Builder::new(&self.p.role.engine, &ObjectInput::new);
        b.pull(&self.p.role.market, &self.s.resting);
        let commands = b.commands();
        match self.send(b.finish(), &commands, "retreat").await {
            Ok(_) => {
                self.pulled();
                info!("Pulled the quotes after the failure");
            }
            Err(message) => {
                warn!("The quotes could not be pulled and rest until they expire: {message}");
            }
        }
    }

    /// A poll that could not read the market or the position. It counts as a failure, and what
    /// rests is pulled at the pace of the rounds: the maker cannot tell what it should quote.
    async fn unread(&mut self, message: &str) -> Decision {
        self.failed(message);
        if failures::due(
            self.s.last_attempt_at,
            self.p.settings.min_interval,
            Instant::now(),
        ) {
            self.s.last_attempt_at = Some(Instant::now());
            self.retreat().await;
        }
        Decision::Failed
    }

    /// Nothing of the maker's rests any more.
    fn pulled(&mut self) {
        self.s.resting.clear();
        self.s.posted.clear();
        self.s.last_quote = None;
        self.report_resting();
    }

    /// A transaction that only cancels: built, simulated and (unless a dry run) sent.
    async fn send(
        &mut self,
        tx: TransactionBuilder,
        commands: &str,
        reason: &'static str,
    ) -> Result<Outcome, String> {
        let tx = match self.build(tx, commands).await {
            Ok(tx) => tx,
            Err(refusal) => {
                self.p.metrics.rounds.with_label_values(&["refused"]).inc();
                return Err(refusal.message);
            }
        };
        self.submit(tx, reason, None).await
    }

    /// Resolves and prices the transaction. The full node simulates it to do so, so that one
    /// that would abort is refused here, with the command that aborted.
    async fn build(
        &mut self,
        mut tx: TransactionBuilder,
        commands: &str,
    ) -> Result<Transaction, Refusal> {
        tx.set_sender(self.p.key.address);
        if let Some(price) = self.gas_price().await {
            tx.set_gas_price(price);
        }
        let tx = match self.p.chain.build(tx).await {
            Ok(tx) => tx,
            Err(BuildError::Aborted(abort)) => {
                return Err(Refusal {
                    message: format!("{} [{}]", describe(&abort), commands),
                    abort: Some(abort),
                });
            }
            Err(other) => {
                return Err(Refusal {
                    abort: None,
                    message: other.to_string(),
                });
            }
        };
        let budget = tx.gas_payment.budget;
        if budget > self.p.settings.max_gas_budget {
            return Err(Refusal {
                abort: None,
                message: format!(
                    "gas budget {budget} is above --max-gas-budget {}",
                    self.p.settings.max_gas_budget
                ),
            });
        }
        Ok(tx)
    }

    /// Simulates a built transaction (a dry run) or signs and sends it. `posts` is what a round
    /// leaves resting; a pull leaves nothing.
    async fn submit(
        &mut self,
        tx: Transaction,
        reason: &'static str,
        posts: Option<Posts<'_>>,
    ) -> Result<Outcome, String> {
        let levels = posts.map_or(0, |p| p.levels.len());
        let left_out = posts.map_or(0, |p| p.left_out);
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
                        left_out,
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
                    left_out,
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
                // A reference gas price that rose with the epoch is one reason for it, and
                // the price is otherwise read once a minute.
                self.gas_price = None;
                Err(format!("rejected: {e}"))
            }
            Err(ExecuteError::Unknown(e)) => {
                self.p.metrics.rounds.with_label_values(&["unknown"]).inc();
                // It may have run. What was known to rest is kept: a cancel skips the orders
                // that are gone. What this one posts is taken over once the full node has it.
                self.s.pending.push(Pending {
                    digest: tx.digest().to_string(),
                    sent_at: Instant::now(),
                    posts: posts.map(|p| (p.levels.to_vec(), p.expires_at_ms)),
                    built_for: None,
                });
                Err(format!("no answer: {e}"))
            }
        }
    }

    /// Counts a failure. Once rounds have failed for `--failing-alert-after-secs` in a row it is
    /// a critical problem: `/health` fails and one alert is raised for the run.
    fn failed(&mut self, message: &str) {
        let lasted = self.s.failures.record(Instant::now());
        let failures = self.s.failures.count();
        warn!(failures, "{message}");
        self.p.board.update(|s| s.failures = failures);
        if lasted >= self.p.settings.failing_alert_after {
            let text = format!(
                "No round has gone through for {} s, {failures} failures in a row: {message}",
                lasted.as_secs()
            );
            if self
                .p
                .board
                .set_problem("failing", Level::Critical, text.clone())
            {
                self.p.alerts.raise(Level::Critical, "failing", text);
            }
        }
    }

    /// Ends a run of failures; true when it had become a problem.
    fn settle(&mut self) -> bool {
        self.s.failures.clear();
        self.p.board.update(|s| s.failures = 0);
        self.p.board.clear_problem("failing")
    }

    /// A round went through: a run of failures is over.
    fn went_through(&mut self) {
        if self.settle() {
            // Under a key of its own: the alert it answers was posted less than a repeat
            // interval ago, and the same key would hold this one back.
            self.p
                .alerts
                .raise(Level::Info, "failing_over", "Rounds go through again");
        }
    }

    /// Tells `/status` and the metrics what rests: the levels of the last round until they
    /// expire, and nothing after that or after a pull. Expired orders stay in the book until
    /// someone cancels them or a taker meets them, but the engine does not match them.
    fn report_resting(&self) {
        let live = (now_ms() as u64) < self.s.expires_at_ms;
        let levels: &[Quoted] = if live { &self.s.posted } else { &[] };
        let bids = levels.iter().filter(|l| l.is_bid).count();
        let asks = levels.len() - bids;
        let m = &self.p.metrics;
        m.quotes.with_label_values(&["bid"]).set(bids as f64);
        m.quotes.with_label_values(&["ask"]).set(asks as f64);
        m.quoting.set(!levels.is_empty() as i64);
        let ladder: Vec<LevelStatus> = levels
            .iter()
            .map(|l| LevelStatus {
                side: if l.is_bid { "bid" } else { "ask" },
                price: units(l.price),
                size: units(l.size),
                reduce_only: l.reduce_only,
            })
            .collect();
        let unanswered = self.s.pending.len();
        self.p.board.update(|s| {
            s.quoting = !ladder.is_empty();
            s.resting_orders = ladder.len();
            s.ladder = ladder;
            s.unanswered = unanswered;
        });
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
        m.sigma_bps.set(self.s.sigma.bps().to_f64().unwrap_or(0.0));
        m.position.set(position.base.to_f64().unwrap_or(0.0));
        m.health.set(
            health
                .as_ref()
                .and_then(|h| h.to_f64())
                .unwrap_or(f64::INFINITY),
        );
        let sigma = self.s.sigma.bps().with_scale(2).to_string();
        self.p.board.update(|s| {
            s.reference = Some(quote.reference.with_scale(2).to_string());
            s.mid = Some(quote.mid.with_scale(2).to_string());
            s.half_spread_bps = Some(quote.half_spread_bps.with_scale(2).to_string());
            s.skew_bps = Some(quote.skew_bps.with_scale(2).to_string());
            s.sigma_bps = Some(sigma);
            s.inventory = Some(quote.inventory.label());
            s.position = Some(position.base.with_scale(6).to_string());
            s.health = health.as_ref().map(|h| h.with_scale(2).to_string());
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
