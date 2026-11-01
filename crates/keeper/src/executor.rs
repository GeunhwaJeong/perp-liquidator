// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The execution loop. Every round reads the open stop and TWAP tickets that name the keeper,
//! their details, and their markets and positions; judges each ticket the way the engine would
//! (`trigger`, `schedule`); and sends one transaction for each ticket that needs one: first the
//! stops whose trigger price has crossed, stop losses first and larger orders before smaller
//! ones, then the TWAP tickets, oldest first: the chunks that are due, the finalizations and the
//! cancellations. The full node simulates every transaction before it is signed, so one the
//! engine would refuse costs nothing; a ticket that keeps failing is tried less and less often,
//! until it no longer needs a transaction.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use bigdecimal::{BigDecimal, Signed, Zero};
use haneul_crypto::HaneulSigner;
use haneul_sdk_types::{Address, Transaction};
use haneul_transaction_builder::{ObjectInput, TransactionBuilder};
use perp_bot_common::alerts::{Alerts, Level};
use perp_bot_common::chain::{Abort, BuildError, Chain, ExecuteError, Outcome};
use perp_bot_common::deployment::db_id;
use perp_bot_common::keys::Key;
use perp_bot_common::oracle::{self, Relay, SignedUpdate, Updates};
use perp_bot_common::store::{MarketRow, Store as Snapshots};
use perp_bot_common::tickets::{StandaloneStop, StopLossTakeProfit, TwapDetails};
use perp_engine::MarketState;
use perp_types::perpetuals::Event;
use perp_types::types::U256;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::details::Submission;
use crate::metrics::Metrics;
use crate::now_ms;
use crate::ptb::{Builder, Details, Engine, Ticket};
use crate::schedule::{self, Plan, Progress};
use crate::setup::{Market, Oracle};
use crate::status::{Board, LastExecution};
use crate::store::{Indexer, OpenTicket, OpenTwap, Store};
use crate::trigger::{self, Prices, Verdict};

const HANEUL: &str = "0x2::haneul::HANEUL";
const HANEUL_UNIT: f64 = 1e9;
/// How often the gas balance and the reference gas price are read again.
const REFRESH_EVERY: Duration = Duration::from_secs(60);
/// How long an executed ticket is left alone while the indexer catches up with its execution.
const JUST_EXECUTED_FOR: Duration = Duration::from_secs(60);
/// `stop_orders::EWrongOrderDetails`.
const WRONG_ORDER_DETAILS: u64 = 6202;
/// `twap_orders::EInvalidOrderDetails`, which also stands for details that do not hash to the
/// ticket's commitment.
const INVALID_TWAP_DETAILS: u64 = 6300;

pub struct Settings {
    pub dry_run: bool,
    pub poll_interval: Duration,
    pub max_slippage_bps: u64,
    pub retry: Duration,
    pub max_retry: Duration,
    pub alert_after: u32,
    pub max_gas_budget: u64,
    pub min_gas_balance: f64,
}

pub struct Parts {
    pub settings: Settings,
    pub chain: Chain,
    pub key: Key,
    pub engine: Engine,
    pub markets: Vec<Market>,
    pub oracle: Option<Oracle>,
    /// The keeper's database, for the details.
    pub store: Store,
    /// The indexer's database: the open tickets and the markets' limits.
    pub indexer: Indexer,
    /// The indexer's database again: the markets and the positions in them.
    pub snapshots: Snapshots,
    pub metrics: Arc<Metrics>,
    pub alerts: Arc<Alerts>,
    pub board: Arc<Board>,
}

/// A ticket's details, read back from the keeper's database.
#[derive(Clone, Debug)]
enum Revealed {
    StopLossTakeProfit(StopLossTakeProfit),
    StandaloneStop(StandaloneStop),
}

impl Revealed {
    /// Stored details were checked when they were handed in; they are checked again against
    /// the ticket before they are used.
    fn read(text: &str) -> anyhow::Result<Self> {
        Ok(match serde_json::from_str::<Submission>(text)? {
            Submission::StopLossTakeProfit(d) => Self::StopLossTakeProfit(d.to_ticket()?),
            Submission::StandaloneStop(d) => Self::StandaloneStop(d.to_ticket()?),
            Submission::Twap(_) => bail!("TWAP details under a stop ticket"),
        })
    }

    fn details(&self) -> Details<'_> {
        match self {
            Self::StopLossTakeProfit(d) => Details::StopLossTakeProfit(d),
            Self::StandaloneStop(d) => Details::StandaloneStop(d),
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::StopLossTakeProfit(_) => "stop_loss_take_profit",
            Self::StandaloneStop(_) => "standalone_stop",
        }
    }

    /// The ticket type the details belong under.
    fn stop_order_type(&self) -> i64 {
        match self {
            Self::StopLossTakeProfit(_) => 0,
            Self::StandaloneStop(_) => 1,
        }
    }

    fn commitment(&self) -> [u8; 32] {
        match self {
            Self::StopLossTakeProfit(d) => d.commitment(),
            Self::StandaloneStop(d) => d.commitment(),
        }
    }

    fn is_limit_order(&self) -> bool {
        match self {
            Self::StopLossTakeProfit(d) => d.is_limit_order,
            Self::StandaloneStop(d) => d.is_limit_order,
        }
    }

    fn size(&self) -> u64 {
        match self {
            Self::StopLossTakeProfit(d) => d.size,
            Self::StandaloneStop(d) => d.size,
        }
    }
}

/// A TWAP ticket's details, read back from the keeper's database.
fn read_twap(text: &str) -> anyhow::Result<TwapDetails> {
    match serde_json::from_str::<Submission>(text)? {
        Submission::Twap(d) => d.to_ticket(),
        _ => bail!("stop details under a TWAP ticket"),
    }
}

/// A ticket that needs a transaction, with what the transaction needs.
struct Candidate {
    ticket_id: String,
    ticket: Ticket,
    account_id: u64,
    /// Index into the markets.
    market: usize,
    relay: Option<(Relay, Vec<SignedUpdate>)>,
    job: Job,
}

enum Job {
    /// A stop ticket whose trigger price has crossed.
    Stop {
        details: Revealed,
        stop_loss: bool,
        /// The order's size at the index price: larger orders go first.
        notional: BigDecimal,
        /// The index price the trigger was judged on.
        index: BigDecimal,
    },
    /// A TWAP ticket's next step: a chunk, its finalization or its cancellation.
    Twap {
        details: TwapDetails,
        plan: Plan,
        /// The last attempt the indexer had seen, to tell when it has seen this one.
        last_attempt_ms: u64,
    },
}

impl Job {
    fn kind(&self) -> &'static str {
        match self {
            Self::Stop { details, .. } => details.kind(),
            Self::Twap {
                plan: Plan::Run { .. },
                ..
            } => "twap_chunk",
            Self::Twap {
                plan: Plan::Complete,
                ..
            } => "twap_finalize",
            Self::Twap { .. } => "twap_cancel",
        }
    }

    /// What the logs say about the transaction besides its kind.
    fn note(&self) -> String {
        match self {
            Self::Stop { stop_loss, .. } => format!("stop_loss={stop_loss}"),
            Self::Twap {
                plan: Plan::Run { amount },
                ..
            } => format!("amount={amount}"),
            Self::Twap { plan, .. } => plan.state().to_owned(),
        }
    }

    /// The index price a market order's fill is held against; `None` for orders with a limit.
    fn market_order_index(&self) -> Option<&BigDecimal> {
        match self {
            Self::Stop { details, index, .. } if !details.is_limit_order() => Some(index),
            _ => None,
        }
    }
}

/// A market as a round sees it.
struct Priced {
    prices: Prices,
    relay: Option<(Relay, Vec<SignedUpdate>)>,
}

struct Failure {
    count: u32,
    until: Instant,
}

pub struct Executor {
    p: Parts,
    /// The keeper's address as the indexer writes the tickets' executors.
    me: String,
    failures: HashMap<String, Failure>,
    just_executed: HashMap<String, Instant>,
    /// TWAP tickets whose chunk was sent, with the last attempt the indexer had seen then: left
    /// alone until the indexer reports another one.
    just_ran: HashMap<String, (Instant, u64)>,
    gas_price: Option<(u64, Instant)>,
    gas_checked: Option<Instant>,
}

impl Executor {
    pub fn new(parts: Parts) -> Self {
        let me = db_id(&parts.key.address);
        Self {
            p: parts,
            me,
            failures: HashMap::new(),
            just_executed: HashMap::new(),
            just_ran: HashMap::new(),
            gas_price: None,
            gas_checked: None,
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) {
        loop {
            if cancel.is_cancelled() {
                return;
            }
            self.round(&cancel).await;
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(self.p.settings.poll_interval) => {}
            }
        }
    }

    /// One look at the open tickets.
    pub async fn round(&mut self, cancel: &CancellationToken) {
        self.check_gas().await;
        let mut states = BTreeMap::new();
        match self.candidates(now_ms(), &mut states).await {
            Ok(candidates) => {
                if self.p.board.clear_problem("reading") {
                    info!("The tickets and their markets can be read again");
                }
                for candidate in &candidates {
                    if cancel.is_cancelled() {
                        break;
                    }
                    self.execute(candidate).await;
                }
            }
            Err(e) => {
                let message = format!("{e:#}");
                warn!("{message}");
                self.p.board.set_problem("reading", Level::Warning, message);
            }
        }
        self.p.metrics.tickets.reset();
        for (state, count) in &states {
            self.p
                .metrics
                .tickets
                .with_label_values(&[state.as_str()])
                .set(*count as i64);
        }
        self.p.board.update(|s| s.tickets = states);
        self.p.metrics.rounds.inc();
        self.p.board.round_done();
    }

    /// The tickets that need a transaction, in the order they are sent, counting every open
    /// ticket by state; a TWAP ticket's state is prefixed with `twap_`.
    async fn candidates(
        &mut self,
        now: i64,
        states: &mut BTreeMap<String, u64>,
    ) -> anyhow::Result<Vec<Candidate>> {
        let tickets = self
            .p
            .indexer
            .open_stop_tickets(&self.me)
            .await
            .context("Failed to read the open tickets")?;
        let twaps = self
            .p
            .indexer
            .open_twap_tickets(&self.me)
            .await
            .context("Failed to read the open TWAP tickets")?;
        let open: HashSet<&str> = tickets
            .iter()
            .map(|t| t.ticket_id.as_str())
            .chain(twaps.iter().map(|t| t.ticket_id.as_str()))
            .collect();
        self.failures.retain(|id, _| open.contains(id.as_str()));
        self.just_executed
            .retain(|id, at| open.contains(id.as_str()) && at.elapsed() < JUST_EXECUTED_FOR);
        self.just_ran
            .retain(|id, (at, _)| open.contains(id.as_str()) && at.elapsed() < JUST_EXECUTED_FOR);
        for twap in &twaps {
            let seen = twap.last_attempt_ms.unwrap_or(0).max(0) as u64;
            if self
                .just_ran
                .get(&twap.ticket_id)
                .is_some_and(|(_, before)| *before != seen)
            {
                self.just_ran.remove(&twap.ticket_id);
            }
        }
        if tickets.is_empty() && twaps.is_empty() {
            return Ok(Vec::new());
        }

        let commitments: Vec<Vec<u8>> = tickets
            .iter()
            .map(|t| t.commitment.clone())
            .chain(twaps.iter().map(|t| t.commitment.clone()))
            .collect();
        let details = self
            .p
            .store
            .details_of(&commitments)
            .await
            .context("Failed to read the tickets' details")?;
        let market_ids: Vec<String> = self.p.markets.iter().map(|m| m.db_id.clone()).collect();
        let Some(snapshot) = self
            .p
            .snapshots
            .snapshot(&market_ids, None)
            .await
            .context("Failed to read the markets")?
        else {
            bail!("The indexer has not indexed anything yet");
        };
        let limits = self
            .p
            .indexer
            .divergence_limits(&market_ids)
            .await
            .context("Failed to read the markets' limits")?;
        let updates = self.fetch_updates().await;

        let mut priced = HashMap::new();
        let mut paused = HashSet::new();
        for market in &self.p.markets {
            let Some(row) = snapshot.markets.iter().find(|r| r.market == market.db_id) else {
                continue;
            };
            if row.paused != 0 || row.closed || row.settlement_enabled {
                paused.insert(market.db_id.clone());
                continue;
            }
            let state = self.market_state(row, market, updates.as_ref());
            let Some((pricing, _)) = state.price(now) else {
                debug!(market = market.ticker, "No price yet");
                continue;
            };
            priced.insert(
                market.db_id.clone(),
                Priced {
                    prices: Prices::new(&pricing, now, limits.get(&market.db_id)),
                    relay: self.relay_for(market, row, updates.as_ref()),
                },
            );
        }
        let positions: HashMap<(String, i64), &BigDecimal> = snapshot
            .positions
            .iter()
            .map(|p| ((p.market.clone(), p.account_id), &p.base))
            .collect();
        let lots: HashMap<&str, u64> = snapshot
            .markets
            .iter()
            .map(|r| (r.market.as_str(), r.lot_size.max(0) as u64))
            .collect();

        let mut candidates = Vec::new();
        for ticket in &tickets {
            let state = match self.assess(ticket, &details, &priced, &paused, &positions, now) {
                Ok(candidate) => {
                    candidates.push(candidate);
                    "triggered"
                }
                Err(state) => state,
            };
            // A ticket that stopped triggering starts afresh when it triggers again: a stop
            // must not wait out the backoff of a failure long past.
            if state != "triggered" && state != "backoff" {
                self.failures.remove(&ticket.ticket_id);
            }
            *states.entry(state.to_owned()).or_default() += 1;
        }
        candidates.sort_by(|a, b| match (&a.job, &b.job) {
            (
                Job::Stop {
                    stop_loss: a_loss,
                    notional: a_notional,
                    ..
                },
                Job::Stop {
                    stop_loss: b_loss,
                    notional: b_notional,
                    ..
                },
            ) => b_loss.cmp(a_loss).then_with(|| b_notional.cmp(a_notional)),
            _ => std::cmp::Ordering::Equal,
        });

        for twap in &twaps {
            let state =
                match self.assess_twap(twap, &details, &priced, &paused, &lots, &positions, now) {
                    Ok(candidate) => {
                        let state = match &candidate.job {
                            Job::Twap { plan, .. } => plan.state(),
                            Job::Stop { .. } => "triggered",
                        };
                        candidates.push(candidate);
                        state
                    }
                    Err(state) => state,
                };
            if !matches!(
                state,
                "due" | "complete" | "expired" | "first_run_expired" | "spoiled" | "backoff"
            ) {
                self.failures.remove(&twap.ticket_id);
            }
            *states.entry(format!("twap_{state}")).or_default() += 1;
        }
        Ok(candidates)
    }

    /// A ticket ready to execute, or the state that keeps it from executing.
    fn assess(
        &self,
        ticket: &OpenTicket,
        details: &HashMap<Vec<u8>, String>,
        priced: &HashMap<String, Priced>,
        paused: &HashSet<String>,
        positions: &HashMap<(String, i64), &BigDecimal>,
        now: i64,
    ) -> Result<Candidate, &'static str> {
        if self.just_executed.contains_key(&ticket.ticket_id) {
            return Err("just_executed");
        }
        let Some(text) = details.get(&ticket.commitment) else {
            return Err("no_details");
        };
        let revealed = match Revealed::read(text) {
            Ok(revealed)
                if revealed.commitment().as_slice() == ticket.commitment.as_slice()
                    && revealed.stop_order_type() == ticket.stop_order_type =>
            {
                revealed
            }
            _ => return Err("unreadable"),
        };
        let clearing_house = revealed.details().clearing_house();
        let Some(index) = self
            .p
            .markets
            .iter()
            .position(|m| m.clearing_house == clearing_house)
        else {
            return Err("unknown_market");
        };
        let market = &self.p.markets[index];
        if paused.contains(&market.db_id) {
            return Err("market_paused");
        }
        let Some(priced) = priced.get(&market.db_id) else {
            return Err("no_price");
        };
        let zero = BigDecimal::zero();
        let base = positions
            .get(&(market.db_id.clone(), ticket.account_id))
            .copied()
            .unwrap_or(&zero);
        let now = now.max(0) as u64;
        let verdict = match &revealed {
            Revealed::StopLossTakeProfit(d) => {
                trigger::stop_loss_take_profit(d, &priced.prices, base, now)
            }
            Revealed::StandaloneStop(d) => trigger::standalone_stop(d, &priced.prices, base, now),
        };
        let Verdict::Triggered { stop_loss } = verdict else {
            return Err(state(verdict));
        };
        if self
            .failures
            .get(&ticket.ticket_id)
            .is_some_and(|f| f.until > Instant::now())
        {
            return Err("backoff");
        }
        let (Ok(id), Ok(account)) = (
            Address::from_str(&ticket.ticket_id),
            Address::from_str(&ticket.account),
        ) else {
            return Err("unreadable");
        };
        let index_price = priced.prices.index.clone();
        Ok(Candidate {
            ticket_id: ticket.ticket_id.clone(),
            ticket: Ticket { id, account },
            account_id: ticket.account_id.max(0) as u64,
            market: index,
            relay: priced.relay.clone(),
            job: Job::Stop {
                notional: BigDecimal::new(revealed.size().into(), 9) * &index_price,
                details: revealed,
                stop_loss,
                index: index_price,
            },
        })
    }

    /// A TWAP ticket's next transaction, or the state that keeps it from one.
    #[allow(clippy::too_many_arguments)]
    fn assess_twap(
        &self,
        twap: &OpenTwap,
        details: &HashMap<Vec<u8>, String>,
        priced: &HashMap<String, Priced>,
        paused: &HashSet<String>,
        lots: &HashMap<&str, u64>,
        positions: &HashMap<(String, i64), &BigDecimal>,
        now: i64,
    ) -> Result<Candidate, &'static str> {
        if self.just_executed.contains_key(&twap.ticket_id)
            || self.just_ran.contains_key(&twap.ticket_id)
        {
            return Err("just_executed");
        }
        let Some(text) = details.get(&twap.commitment) else {
            return Err("no_details");
        };
        let revealed = match read_twap(text) {
            Ok(d) if d.commitment().as_slice() == twap.commitment.as_slice() => d,
            _ => return Err("unreadable"),
        };
        let Some(index) = self.p.markets.iter().position(|m| m.db_id == twap.market) else {
            return Err("unknown_market");
        };
        let market = &self.p.markets[index];
        let Some(&lot_size) = lots.get(market.db_id.as_str()) else {
            return Err("unknown_market");
        };
        let raw = |v: Option<i64>| v.unwrap_or(0).max(0) as u64;
        let progress = Progress {
            processed: raw(twap.processed_amount),
            scheduled: raw(twap.scheduled_amount),
            last_attempt_ms: raw(twap.last_attempt_ms),
            retry_anchor_ms: raw(twap.retry_anchor_ms),
        };
        let plan = schedule::plan(&revealed, &progress, lot_size, now.max(0) as u64);
        let runs = matches!(plan, Plan::Run { .. });
        if !runs && plan != Plan::Complete && !plan.cancels() {
            return Err(plan.state());
        }
        // A chunk needs the market running and a price to relay; finalizing and cancelling
        // release collateral only while the market runs, and need no price while it is paused.
        let is_paused = paused.contains(&market.db_id);
        if runs && is_paused {
            return Err("market_paused");
        }
        let relay = match priced.get(&market.db_id) {
            Some(priced) => priced.relay.clone(),
            None if runs || !is_paused => return Err("no_price"),
            None => None,
        };
        if runs && revealed.reduce_only {
            let zero = BigDecimal::zero();
            let base = positions
                .get(&(market.db_id.clone(), twap.account_id))
                .copied()
                .unwrap_or(&zero);
            // An ask (`side`) reduces a long, a bid a short.
            let reduces = if revealed.side {
                base.is_positive()
            } else {
                base.is_negative()
            };
            if !reduces {
                return Err("no_position");
            }
        }
        if self
            .failures
            .get(&twap.ticket_id)
            .is_some_and(|f| f.until > Instant::now())
        {
            return Err("backoff");
        }
        let (Ok(id), Ok(account)) = (
            Address::from_str(&twap.ticket_id),
            Address::from_str(&twap.account),
        ) else {
            return Err("unreadable");
        };
        Ok(Candidate {
            ticket_id: twap.ticket_id.clone(),
            ticket: Ticket { id, account },
            account_id: twap.account_id.max(0) as u64,
            market: index,
            relay,
            job: Job::Twap {
                details: revealed,
                plan,
                last_attempt_ms: progress.last_attempt_ms,
            },
        })
    }

    async fn execute(&mut self, c: &Candidate) {
        let kind = c.job.kind();
        let market = &self.p.markets[c.market];
        let (feeds, clearing_house) = (market.feeds, market.clearing_house);
        let mut b = Builder::new(&self.p.engine, &ObjectInput::new);
        b.refresh(c.relay.as_ref().map(|(r, u)| (r, u.as_slice())));
        match &c.job {
            Job::Stop { details, .. } => b.execute(&c.ticket, details.details(), &feeds),
            Job::Twap {
                details,
                plan: Plan::Run { amount },
                ..
            } => b.run_chunk(&c.ticket, clearing_house, details, *amount, &feeds),
            Job::Twap {
                details,
                plan: Plan::Complete,
                ..
            } => b.finalize(&c.ticket, clearing_house, details, &feeds),
            Job::Twap { .. } => b.cancel_twap(&c.ticket, clearing_house, &feeds),
        };
        let commands = b.commands();
        let tx = b.finish();

        let tx = match self.prepare(tx).await {
            Ok(tx) => tx,
            Err(e) => {
                self.count(kind, "refused");
                match &e {
                    BuildError::Aborted(abort) => {
                        let level = matches!(
                            (abort.module.as_str(), abort.code),
                            ("stop_orders", WRONG_ORDER_DETAILS)
                                | ("twap_orders", INVALID_TWAP_DETAILS)
                        )
                        .then_some(Level::Critical);
                        self.failed(c, &describe(abort, &commands), level);
                    }
                    other => self.failed(c, &other.to_string(), None),
                }
                return;
            }
        };
        let budget = tx.gas_payment.budget;
        if budget > self.p.settings.max_gas_budget {
            self.count(kind, "refused");
            let message = format!(
                "gas budget {budget} is above --max-gas-budget {}",
                self.p.settings.max_gas_budget
            );
            self.failed(c, &message, None);
            return;
        }

        // The engine fills a market order at any price, so where the simulated one filled is
        // read before it is sent.
        let simulated = if self.p.settings.dry_run || c.job.market_order_index().is_some() {
            match self.p.chain.simulate(&tx).await {
                Ok(outcome) if outcome.success => Some(outcome),
                Ok(outcome) => {
                    self.count(kind, "refused");
                    self.failed(c, &failure(&outcome), None);
                    return;
                }
                Err(e) => {
                    self.failed(c, &format!("simulation failed: {e:#}"), None);
                    return;
                }
            }
        } else {
            None
        };
        if let Some(outcome) = &simulated
            && let Some(index) = c.job.market_order_index()
            && let Some(message) = self.strays(c, index, outcome)
        {
            self.count(kind, "guarded");
            self.failed(c, &message, Some(Level::Warning));
            return;
        }

        let ticker = self.p.markets[c.market].ticker.clone();
        let relayed = c.relay.as_ref().map_or(0, |(_, u)| u.len());
        if self.p.settings.dry_run {
            info!(
                market = ticker,
                ticket = c.ticket_id,
                kind,
                what = c.job.note(),
                relayed,
                "Would execute"
            );
            self.count(kind, "simulated");
            // So that a dry run reports each ticket once a minute rather than every round.
            self.just_executed
                .insert(c.ticket_id.clone(), Instant::now());
            return;
        }

        let signature = match self.p.key.private.sign_transaction(&tx) {
            Ok(signature) => signature,
            Err(e) => {
                self.failed(c, &format!("signing failed: {e}"), None);
                return;
            }
        };
        match self.p.chain.execute(&tx, signature).await {
            Ok(outcome) if outcome.success => {
                self.count(kind, "executed");
                if outcome.gas_used >= 0 {
                    self.p.metrics.gas_spent.inc_by(outcome.gas_used as u64);
                } else {
                    self.p
                        .metrics
                        .gas_rebated
                        .inc_by(outcome.gas_used.unsigned_abs());
                }
                self.p.metrics.relayed_updates.inc_by(relayed as u64);
                info!(
                    market = ticker,
                    ticket = c.ticket_id,
                    kind,
                    what = c.job.note(),
                    digest = outcome.digest,
                    relayed,
                    gas_used = outcome.gas_used,
                    "Executed"
                );
                self.failures.remove(&c.ticket_id);
                self.leave_alone(c);
                self.p.board.update(|s| {
                    s.executions += 1;
                    s.last_execution = Some(LastExecution {
                        ticket_id: c.ticket_id.clone(),
                        kind,
                        digest: outcome.digest.clone(),
                        at_ms: now_ms(),
                    });
                });
            }
            Ok(outcome) => {
                self.count(kind, "failed");
                self.failed(c, &failure(&outcome), None);
            }
            Err(ExecuteError::Rejected(e)) => {
                self.count(kind, "rejected");
                self.failed(c, &format!("rejected: {e}"), None);
            }
            Err(ExecuteError::Unknown(e)) => {
                // It may still land: left alone until the indexer tells.
                self.count(kind, "unknown");
                warn!(ticket = c.ticket_id, kind, "No answer: {e}");
                self.leave_alone(c);
            }
        }
    }

    /// Leaves a ticket alone while the indexer catches up with its transaction: a TWAP ticket
    /// whose chunk was sent until the indexer reports another attempt, any other until it is no
    /// longer open; either for at most `JUST_EXECUTED_FOR`.
    fn leave_alone(&mut self, c: &Candidate) {
        match &c.job {
            Job::Twap {
                plan: Plan::Run { .. },
                last_attempt_ms,
                ..
            } => {
                self.just_ran
                    .insert(c.ticket_id.clone(), (Instant::now(), *last_attempt_ms));
            }
            _ => {
                self.just_executed
                    .insert(c.ticket_id.clone(), Instant::now());
            }
        }
    }

    /// Why a simulated market order's fill strays too far from the index price, if it does.
    fn strays(&self, c: &Candidate, index: &BigDecimal, outcome: &Outcome) -> Option<String> {
        let price = fill_price(&outcome.events, &self.p.engine.types_package, c.account_id)?;
        if index.is_zero() {
            return None;
        }
        let bps = (&price - index).abs() * BigDecimal::from(10_000) / index;
        (bps > self.p.settings.max_slippage_bps).then(|| {
            format!(
                "the market order would fill at {}, {} bps from the index price {}, beyond --max-slippage-bps {}: held back",
                price.with_scale(2),
                bps.with_scale(1),
                index.with_scale(2),
                self.p.settings.max_slippage_bps
            )
        })
    }

    fn failed(&mut self, c: &Candidate, message: &str, alert: Option<Level>) {
        let failure = self.failures.entry(c.ticket_id.clone()).or_insert(Failure {
            count: 0,
            until: Instant::now(),
        });
        failure.count += 1;
        let wait = retry_after(
            self.p.settings.retry,
            self.p.settings.max_retry,
            failure.count,
        );
        failure.until = Instant::now() + wait;
        let count = failure.count;
        let kind = c.job.kind();
        warn!(
            ticket = c.ticket_id,
            kind,
            failures = count,
            retry_in_secs = wait.as_secs(),
            "{message}"
        );
        let level = match alert {
            Some(level) => Some(level),
            None => (count == self.p.settings.alert_after).then_some(Level::Warning),
        };
        if let Some(level) = level {
            let ticker = &self.p.markets[c.market].ticker;
            self.p.alerts.raise(
                level,
                &format!("ticket:{}", c.ticket_id),
                format!(
                    "{ticker} {kind} ticket {}: {count} failures in a row: {message}",
                    c.ticket_id
                ),
            );
        }
    }

    fn count(&self, kind: &str, result: &str) {
        self.p
            .metrics
            .executions
            .with_label_values(&[kind, result])
            .inc();
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
                let message = format!(
                    "The oracle service does not answer ({e:#}); tickets trigger on the relayer's prices"
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

    /// The signed prices to relay in front of a market's execution: those of its feeds, for the
    /// source its prices are read from.
    fn relay_for(
        &self,
        market: &Market,
        row: &MarketRow,
        updates: Option<&Updates>,
    ) -> Option<(Relay, Vec<SignedUpdate>)> {
        let (oracle, updates) = (self.p.oracle.as_ref()?, updates?);
        let mut feeds = Vec::new();
        if row.base_source_id == oracle.source_id {
            feeds.push(market.feeds.base);
        }
        if row.collateral_source_id == oracle.source_id {
            feeds.push(market.feeds.collateral);
        }
        let picked = updates.for_feeds(&feeds);
        (!picked.is_empty()).then(|| (updates.relay.clone(), picked))
    }

    fn market_state(
        &self,
        row: &MarketRow,
        market: &Market,
        updates: Option<&Updates>,
    ) -> MarketState {
        let (oracle_price, oracle_twap_price) = self.base_price(row, market, updates);
        MarketState {
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
            oracle_price,
            oracle_twap_price,
            collateral_price: row.collateral_price.clone(),
            event_index_price: row.event_index_price.clone(),
        }
    }

    /// The base price and TWAP an execution sees: the oracle service's signed price, and the
    /// TWAP its relay makes, when it is newer than the chain's and for the market's source.
    fn base_price(
        &self,
        row: &MarketRow,
        market: &Market,
        updates: Option<&Updates>,
    ) -> (Option<BigDecimal>, Option<BigDecimal>) {
        let on_chain = (row.oracle_price.clone(), row.oracle_twap_price.clone());
        let (Some(oracle), Some(updates)) = (self.p.oracle.as_ref(), updates) else {
            return on_chain;
        };
        if row.base_source_id != oracle.source_id {
            return on_chain;
        }
        let Some(update) = updates.get(&market.feeds.base) else {
            return on_chain;
        };
        if row
            .oracle_timestamp_ms
            .is_some_and(|at| update.timestamp_ms as i64 <= at)
        {
            return on_chain;
        }
        // Without the chain's TWAP and window, assume the TWAP catches up at once; the
        // simulation settles what that gets wrong.
        let twap = match (
            row.oracle_twap_price.as_ref().and_then(oracle::raw),
            row.oracle_timestamp_ms,
            oracle.twap_periods.get(&market.feeds.base),
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
        (Some(update.price()), Some(BigDecimal::new(twap.into(), 18)))
    }

    /// Resolves, prices and simulates. The reference gas price is the only one the engine
    /// executes a ticket at.
    async fn prepare(&mut self, mut tx: TransactionBuilder) -> Result<Transaction, BuildError> {
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

fn state(verdict: Verdict) -> &'static str {
    match verdict {
        Verdict::Triggered { .. } => "triggered",
        Verdict::Waiting => "waiting",
        Verdict::Expired => "expired",
        Verdict::NoPosition => "no_position",
        Verdict::MarkUnavailable => "mark_unavailable",
        Verdict::Invalid => "invalid",
    }
}

/// The wait after `count` failures in a row: `retry`, doubled with each further one, at most
/// `max`.
fn retry_after(retry: Duration, max: Duration, count: u32) -> Duration {
    let factor = 1u32 << (count.clamp(1, 16) - 1);
    (retry * factor).min(max)
}

/// What an abort while building an execution means.
fn describe(abort: &Abort, commands: &str) -> String {
    let meaning = match (abort.module.as_str(), abort.code) {
        ("stop_orders", 6200) => Some("the ticket has expired"),
        ("stop_orders", 6201) => Some("the trigger price has not crossed on chain"),
        ("stop_orders", WRONG_ORDER_DETAILS) => {
            Some("the details do not hash to the ticket's commitment")
        }
        ("stop_orders", 6204) => Some("the keeper is not an executor of the ticket"),
        ("stop_orders", 6206) => Some("the account holds no position on the ticket's side"),
        ("stop_orders", 6208) => Some("the ticket is of the other type"),
        ("stop_orders", 6209) => Some("the order would neither fill nor rest"),
        ("stop_orders", 6210) => Some("the gas price is not the reference gas price"),
        ("twap_orders", INVALID_TWAP_DETAILS) => Some(
            "the details do not hash to the ticket's commitment, or cannot be split into chunks",
        ),
        ("twap_orders", 6301) => Some("the ticket has expired"),
        ("twap_orders", 6302) => {
            Some("the amount is not the details' chunk, or more than one execution may take")
        }
        ("twap_orders", 6303) => Some("the execution gap since the last attempt has not passed"),
        ("twap_orders", 6304) => Some("the ticket's whole size is processed"),
        ("twap_orders", 6305) => Some("nothing filled within the retry window: the ticket spoiled"),
        ("twap_orders", 6306) => Some("the keeper is not an executor of the ticket"),
        ("twap_orders", 6308) => Some("the ticket's size is not processed yet"),
        ("twap_orders", 6309) => Some("the ticket is for another clearing house"),
        ("twap_orders", 6310) => Some("the first chunk's deadline has passed"),
        ("twap_orders", 6311) => Some("the gas price is not the reference gas price"),
        ("twap_orders", 6312) => Some("the size or the chunk is not in lots"),
        ("clearing_house", 1) => Some("the order is empty: a reduce-only order with no position"),
        ("clearing_house", 9) => Some("the reduce-only order has no position to reduce"),
        ("clearing_house", 30) => Some("the account lacks the collateral the filled order needs"),
        ("clearing_house", 32) => Some("the market is paused"),
        ("clearing_house", 40) => Some("the account lacks the free collateral for the order"),
        ("clearing_house", 45) => Some("the book is too thin to fill the market order"),
        ("market", 1000) => Some("the base price is stale"),
        ("market", 1003) => Some("the index price is too far from its TWAP for a mark price"),
        _ => None,
    };
    let at = format!(
        "aborts in {}::{} with {}{}",
        abort.module,
        abort.function,
        abort.code,
        abort
            .command
            .map(|c| format!(" at command {c}"))
            .unwrap_or_default()
    );
    match meaning {
        Some(meaning) => format!("{meaning}: {at}"),
        None => format!("{at} ({commands})"),
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

/// The average price the account's taker orders filled at, from the engine's events.
fn fill_price(
    events: &[(String, Vec<u8>)],
    types_package: &Address,
    account_id: u64,
) -> Option<BigDecimal> {
    let (mut base, mut quote) = (BigDecimal::zero(), BigDecimal::zero());
    for (event_type, bcs) in events {
        let Some(name) = engine_event(event_type, types_package) else {
            continue;
        };
        if let Ok(Event::FilledTakerOrder(e)) = Event::decode(name, bcs)
            && e.taker_account_id == account_id
        {
            base += ifixed(&e.base_asset_delta_ask).abs() + ifixed(&e.base_asset_delta_bid).abs();
            quote +=
                ifixed(&e.quote_asset_delta_ask).abs() + ifixed(&e.quote_asset_delta_bid).abs();
        }
    }
    (!base.is_zero()).then(|| quote / base)
}

/// The struct name of an event of the engine's `events` module.
fn engine_event<'a>(event_type: &'a str, types_package: &Address) -> Option<&'a str> {
    let mut parts = event_type.splitn(3, "::");
    let (package, module, name) = (parts.next()?, parts.next()?, parts.next()?);
    let package: Address = package.parse().ok()?;
    (package == *types_package && module == "events" && !name.contains('<')).then_some(name)
}

fn ifixed(value: &U256) -> BigDecimal {
    BigDecimal::new(value.to_ifixed(), 18)
}

#[cfg(test)]
mod tests {
    use perp_types::perpetuals::FilledTakerOrder;
    use perp_types::types::Address as MoveAddress;

    use super::*;

    fn fx(dollars: u128) -> U256 {
        let mut bytes = [0; 32];
        bytes[..16].copy_from_slice(&(dollars * 10u128.pow(18)).to_le_bytes());
        U256(bytes)
    }

    fn taker_fill(account_id: u64, base: u128, quote: u128) -> FilledTakerOrder {
        FilledTakerOrder {
            ch_id: MoveAddress([0xc0; 32]),
            taker_account_id: account_id,
            taker_pnl: fx(0),
            taker_fees: fx(0),
            integrator_id: None,
            integrator_fee_paid_usd: fx(0),
            base_asset_delta_ask: fx(base),
            quote_asset_delta_ask: fx(quote),
            base_asset_delta_bid: fx(0),
            quote_asset_delta_bid: fx(0),
            mark_price: fx(0),
        }
    }

    #[test]
    fn the_fill_price_is_the_accounts_quote_over_its_base() {
        let package = Address::from_static("0xe1");
        let event_type = format!("{package}::events::FilledTakerOrder");
        let events = vec![
            (
                event_type.clone(),
                bcs::to_bytes(&taker_fill(7, 2, 190_000)).unwrap(),
            ),
            (
                event_type.clone(),
                bcs::to_bytes(&taker_fill(9, 1, 1)).unwrap(),
            ),
            (
                format!("{}::events::FilledTakerOrder", Address::from_static("0xe2")),
                bcs::to_bytes(&taker_fill(7, 1, 1)).unwrap(),
            ),
        ];
        assert_eq!(
            fill_price(&events, &package, 7),
            Some(BigDecimal::from(95_000))
        );
        assert_eq!(fill_price(&events, &package, 8), None);
    }

    #[test]
    fn the_wait_doubles_up_to_its_ceiling() {
        let (retry, max) = (Duration::from_secs(1), Duration::from_secs(30));
        let waits: Vec<u64> = (1..=7)
            .map(|count| retry_after(retry, max, count).as_secs())
            .collect();
        assert_eq!(waits, vec![1, 2, 4, 8, 16, 30, 30]);
        assert_eq!(retry_after(retry, max, 1_000), max);
    }

    #[test]
    fn says_what_an_abort_means() {
        let abort = |module: &str, code: u64| Abort {
            package: "0x0d".into(),
            module: module.into(),
            function: "f".into(),
            code,
            command: Some(4),
        };
        assert_eq!(
            describe(&abort("clearing_house", 45), ""),
            "the book is too thin to fill the market order: aborts in clearing_house::f with 45 at command 4"
        );
        assert!(describe(&abort("stop_orders", 6202), "").starts_with("the details do not hash"));
        assert_eq!(
            describe(&abort("account", 3), "0:a 1:b"),
            "aborts in account::f with 3 at command 4 (0:a 1:b)"
        );
    }

    #[test]
    fn stored_details_read_back_as_the_ticket_they_were_made_for() {
        let text = r#"{"kind": "standaloneStop", "details": {
            "clearingHouse": "0xf77dda417d4003c017c96697809d3a25b2767aca586b5b7579e975e47508df77",
            "expireTimestampMs": "1800000000000", "isLimitOrder": true, "triggerPriceType": 1,
            "stopIndexPrice": "101000000000000000000000", "triggerAtOrAbove": true,
            "side": false, "size": "100000000", "price": "101500000000", "orderType": "0",
            "reduceOnly": true,
            "salt": "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"}}"#;
        let revealed = Revealed::read(text).unwrap();
        assert_eq!(revealed.kind(), "standalone_stop");
        assert_eq!(revealed.stop_order_type(), 1);
        assert!(revealed.is_limit_order());
        assert_eq!(
            hex::encode(revealed.commitment()),
            "17c0fe7d24464e1d3df5dcf1cf54c9534e5044e6284a9ee22272a5e6c3f229ec"
        );
        let twap = r#"{"kind": "twap", "details": {
            "executionGapMs": "60000", "executionTimeUncertaintyMs": "10000", "chunksAmount": "4",
            "smallTailMergeThresholdBps": "0", "timeForRetryMs": "30000",
            "amountUncertaintyBps": "0", "maxOneExecutionAmountBps": "2500", "side": false,
            "size": "1000000000", "maxSlippageBps": "100", "reduceOnly": false, "salt": "00"}}"#;
        assert!(Revealed::read(twap).is_err());
        let details = read_twap(twap).unwrap();
        assert_eq!((details.chunks_amount, details.size), (4, 1_000_000_000));
        assert!(read_twap(text).is_err());
    }
}
