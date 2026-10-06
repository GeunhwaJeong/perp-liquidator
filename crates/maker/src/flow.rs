// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The shadow run's flow simulator: a taker on its own key and account that trades against the
//! maker at random intervals, so that the market has fills, candles, open interest and
//! funding, and the maker is tested against adverse fills. A shadow-run tool: it is refused on
//! any collateral that is not a test coin.

use std::sync::Arc;
use std::time::Duration;

use bigdecimal::{BigDecimal, One, Signed, ToPrimitive};
use haneul_crypto::HaneulSigner;
use haneul_transaction_builder::ObjectInput;
use perp_bot_common::alerts::{Alerts, Level};
use perp_bot_common::chain::{BuildError, Chain};
use perp_bot_common::keys::Key;
use perp_bot_common::oracle::OracleClient;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::maker::describe;
use crate::market;
use crate::metrics::Metrics;
use crate::model::{price_units, size_units};
use crate::ptb::Builder;
use crate::setup::Role;
use crate::status::{Board, FlowStatus, now_ms};

pub struct Settings {
    pub dry_run: bool,
    pub mean: Duration,
    pub size: BigDecimal,
    pub slippage_bps: BigDecimal,
    pub max_position: BigDecimal,
    pub max_gas_budget: u64,
    pub base_source_id: u64,
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
}

/// A small deterministic generator (xorshift64*), enough for sizes and timings.
pub struct Rng(u64);

impl Rng {
    pub fn seeded(seed: u64) -> Self {
        Self(seed.max(1))
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform in (0, 1).
    pub fn uniform(&mut self) -> f64 {
        ((self.next_u64() >> 11) as f64 + 0.5) / (1u64 << 53) as f64
    }

    /// Standard normal, by Box-Muller.
    pub fn normal(&mut self) -> f64 {
        let u = self.uniform();
        let v = self.uniform();
        (-2.0 * u.ln()).sqrt() * (2.0 * std::f64::consts::PI * v).cos()
    }

    /// Exponential with the given mean.
    pub fn exponential(&mut self, mean: f64) -> f64 {
        -mean * self.uniform().ln()
    }
}

/// What the next trade is.
#[derive(Clone, Debug, PartialEq)]
pub struct Trade {
    pub is_bid: bool,
    /// Base units.
    pub size: BigDecimal,
    pub reduce_only: bool,
}

/// Decides a trade: lognormal size around the typical one (σ = 0.6, capped at 5×), a side
/// random with a pull toward flat (P(buy) = 0.5 − 0.4 × q / q_max), and reduce-only beyond the
/// position limit.
pub fn decide(
    rng: &mut Rng,
    typical: &BigDecimal,
    position: &BigDecimal,
    max_position: &BigDecimal,
) -> Trade {
    let factor = (0.6 * rng.normal()).exp().clamp(0.1, 5.0);
    let size = typical * BigDecimal::try_from(factor).unwrap_or_else(|_| BigDecimal::one());
    let fraction = if max_position.is_positive() {
        (position / max_position)
            .max(BigDecimal::from(-1))
            .min(BigDecimal::one())
            .to_f64()
            .unwrap_or(0.0)
    } else {
        0.0
    };
    let p_buy = 0.5 - 0.4 * fraction;
    let beyond = position.abs() >= *max_position;
    let is_bid = if beyond {
        position.is_negative()
    } else {
        rng.uniform() < p_buy
    };
    Trade {
        is_bid,
        size: if beyond {
            size.min(position.abs())
        } else {
            size
        },
        reduce_only: beyond,
    }
}

pub struct Flow {
    p: Parts,
    rng: Rng,
    trades: u64,
}

impl Flow {
    pub fn new(parts: Parts) -> Self {
        Self {
            p: parts,
            rng: Rng::seeded(now_ms() as u64),
            trades: 0,
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) {
        loop {
            let wait = Duration::from_secs_f64(
                self.rng
                    .exponential(self.p.settings.mean.as_secs_f64())
                    .clamp(2.0, self.p.settings.mean.as_secs_f64() * 5.0),
            );
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(wait) => {}
            }
            self.trade().await;
        }
    }

    pub async fn trade(&mut self) {
        let result = self.try_trade().await;
        let label = match &result {
            Ok(true) => "executed",
            Ok(false) => "simulated",
            Err(_) => "failed",
        };
        self.p.metrics.flow_trades.with_label_values(&[label]).inc();
        if let Err(e) = result {
            warn!("The flow simulator's trade failed: {e:#}");
        }
    }

    async fn try_trade(&mut self) -> anyhow::Result<bool> {
        let updates = self.p.oracle.fetch().await?;
        let update = updates
            .get(&self.p.role.market.base_feed)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("no signed price for the base feed"))?;
        let reference = update.price();
        let ch = self
            .p
            .chain
            .object(self.p.role.market.clearing_house)
            .await?;
        let view = market::MarketView::from_json(&ch.json)?;
        if !view.trades() {
            return Ok(false);
        }
        let position = market::position(
            &self.p.chain,
            self.p.role.market.clearing_house,
            self.p.role.engine.types_package,
            self.p.role.account_id,
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("the simulator's position object is gone"))?;
        self.p
            .metrics
            .flow_position
            .set(position.base.to_f64().unwrap_or(0.0));

        let trade = decide(
            &mut self.rng,
            &self.p.settings.size,
            &position.base,
            &self.p.settings.max_position,
        );
        let slip = &self.p.settings.slippage_bps / BigDecimal::from(10_000);
        let price = if trade.is_bid {
            &reference * (BigDecimal::one() + &slip)
        } else {
            &reference * (BigDecimal::one() - &slip)
        };
        let price = price_units(&price, view.tick_size, trade.is_bid);
        let size = size_units(&trade.size, view.lot_size);
        if size == 0 {
            return Ok(false);
        }
        let relayed = updates.for_feeds(&[self.p.role.market.base_feed]);
        let mut b = Builder::new(&self.p.role.engine, &ObjectInput::new);
        b.refresh(Some((&updates.relay, &relayed))).taker(
            &self.p.role.market,
            trade.is_bid,
            size,
            price,
            trade.reduce_only,
        );
        let mut tx = b.finish();
        tx.set_sender(self.p.key.address);
        if let Ok(gas) = self.p.chain.reference_gas_price().await {
            tx.set_gas_price(gas);
        }
        let tx = self.p.chain.build(tx).await.map_err(|e| match e {
            BuildError::Aborted(abort) => anyhow::anyhow!("{}", describe(&abort)),
            other => anyhow::anyhow!("{other}"),
        })?;
        anyhow::ensure!(
            tx.gas_payment.budget <= self.p.settings.max_gas_budget,
            "gas budget {} is above --max-gas-budget",
            tx.gas_payment.budget
        );
        if self.p.settings.dry_run {
            let outcome = self.p.chain.simulate(&tx).await?;
            anyhow::ensure!(outcome.success, "simulation: {:?}", outcome.error);
            info!(
                side = if trade.is_bid { "buy" } else { "sell" },
                size = %trade.size.with_scale(4),
                "Would trade"
            );
            return Ok(false);
        }
        let signature = self
            .p
            .key
            .private
            .sign_transaction(&tx)
            .map_err(|e| anyhow::anyhow!("signing failed: {e}"))?;
        let outcome = self
            .p
            .chain
            .execute(&tx, signature)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        anyhow::ensure!(
            outcome.success,
            "{} failed: {}",
            outcome.digest,
            outcome
                .abort
                .as_ref()
                .map(describe)
                .or(outcome.error.clone())
                .unwrap_or_default()
        );
        self.trades += 1;
        let filled = outcome.events.iter().any(|(t, _)| {
            t.split('<')
                .next()
                .unwrap_or_default()
                .ends_with("::events::FilledTakerOrder")
        });
        info!(
            digest = outcome.digest,
            side = if trade.is_bid { "buy" } else { "sell" },
            size = %trade.size.with_scale(4),
            reduce_only = trade.reduce_only,
            filled,
            "Flow trade"
        );
        let address = self.p.key.address.to_string();
        let trades = self.trades;
        let digest = outcome.digest.clone();
        let base = position.base.with_scale(4).to_string();
        self.p.board.update(|s| {
            s.flow = Some(FlowStatus {
                address,
                trades,
                position: Some(base),
                last_trade_ms: Some(now_ms()),
                last_digest: Some(digest),
            })
        });
        if !filled {
            self.p.alerts.raise(
                Level::Info,
                "flow_unfilled",
                "a simulator trade found nothing inside its slippage",
            );
        }
        Ok(true)
    }
}

/// Whether the collateral is a test coin the simulator may trade.
pub fn is_test_collateral(collateral: &haneul_sdk_types::TypeTag) -> bool {
    match collateral {
        haneul_sdk_types::TypeTag::Struct(s) => s.module().as_str() == "tusd",
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use bigdecimal::Zero;

    use super::*;

    fn dec(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).unwrap()
    }

    #[test]
    fn sizes_vary_around_the_typical_one_and_sides_balance_over_many_trades() {
        let mut rng = Rng::seeded(7);
        let mut buys = 0;
        let mut total = BigDecimal::zero();
        for _ in 0..2_000 {
            let t = decide(&mut rng, &dec("0.01"), &dec("0"), &dec("0.2"));
            assert!(!t.reduce_only);
            assert!(t.size >= dec("0.001") && t.size <= dec("0.05"));
            total += &t.size;
            buys += t.is_bid as u32;
        }
        // The lognormal's mean is e^(0.18) ≈ 1.2 of the typical size.
        let mean = total / BigDecimal::from(2_000);
        assert!(mean > dec("0.009") && mean < dec("0.015"), "{mean}");
        assert!((900..=1_100).contains(&buys), "{buys}");
    }

    #[test]
    fn a_long_simulator_leans_toward_selling_and_only_reduces_past_its_limit() {
        let mut rng = Rng::seeded(11);
        let mut buys = 0;
        for _ in 0..1_000 {
            buys += decide(&mut rng, &dec("0.01"), &dec("0.1"), &dec("0.2")).is_bid as u32;
        }
        // P(buy) = 0.5 − 0.4 × 0.5 = 0.3.
        assert!((230..=370).contains(&buys), "{buys}");
        let t = decide(&mut rng, &dec("0.01"), &dec("0.25"), &dec("0.2"));
        assert!(!t.is_bid && t.reduce_only);
        let t = decide(&mut rng, &dec("0.01"), &dec("-0.25"), &dec("0.2"));
        assert!(t.is_bid && t.reduce_only);
    }

    #[test]
    fn only_a_test_coin_may_be_traded_by_the_simulator() {
        assert!(is_test_collateral(&"0x7f::tusd::TUSD".parse().unwrap()));
        assert!(!is_test_collateral(&"0x7f::ryusd::RYUSD".parse().unwrap()));
    }
}
