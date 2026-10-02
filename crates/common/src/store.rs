// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Reads the indexer's database.
//!
//! Every round reads the watermark, the markets and the positions in one read-only
//! repeatable-read transaction, so that they describe the same checkpoint. Between full
//! reloads only the positions written since the last round are read; the indexer writes a
//! position's row whenever its object changes, so nothing is missed.

use anyhow::Context;
use bigdecimal::BigDecimal;
use diesel::QueryableByName;
use diesel::sql_query;
use diesel::sql_types::{Array, BigInt, Bool, Integer, Nullable, Numeric, SmallInt, Text};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, TransactionManager};
use url::Url;

#[derive(Clone)]
pub struct Store {
    pool: Pool<AsyncPgConnection>,
}

#[derive(Clone, Copy, Debug, QueryableByName)]
pub struct Watermark {
    #[diesel(sql_type = BigInt)]
    pub checkpoint: i64,
    #[diesel(sql_type = BigInt)]
    pub timestamp_ms: i64,
}

#[derive(Clone, Debug, QueryableByName)]
pub struct MarketRow {
    #[diesel(sql_type = Text)]
    pub market: String,
    #[diesel(sql_type = SmallInt)]
    pub paused: i16,
    #[diesel(sql_type = Bool)]
    pub closed: bool,
    #[diesel(sql_type = Bool)]
    pub settlement_enabled: bool,
    #[diesel(sql_type = Nullable<Numeric>)]
    pub settlement_base_price: Option<BigDecimal>,
    #[diesel(sql_type = Numeric)]
    pub margin_ratio_initial: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub margin_ratio_maintenance: BigDecimal,
    #[diesel(sql_type = BigInt)]
    pub lot_size: i64,
    #[diesel(sql_type = BigInt)]
    pub tick_size: i64,
    #[diesel(sql_type = Numeric)]
    pub cum_funding_rate_long: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub cum_funding_rate_short: BigDecimal,
    #[diesel(sql_type = BigInt)]
    pub funding_last_upd_ms: i64,
    #[diesel(sql_type = BigInt)]
    pub funding_frequency_ms: i64,
    #[diesel(sql_type = BigInt)]
    pub funding_period_ms: i64,
    #[diesel(sql_type = Numeric)]
    pub premium_twap: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub spread_twap: BigDecimal,
    #[diesel(sql_type = Nullable<Numeric>)]
    pub best_bid_price: Option<BigDecimal>,
    #[diesel(sql_type = Nullable<Numeric>)]
    pub best_ask_price: Option<BigDecimal>,
    #[diesel(sql_type = Numeric)]
    pub collateral_haircut: BigDecimal,
    #[diesel(sql_type = BigInt)]
    pub base_storage_id: i64,
    #[diesel(sql_type = Integer)]
    pub base_source_id: i32,
    #[diesel(sql_type = Integer)]
    pub collateral_source_id: i32,
    #[diesel(sql_type = Nullable<Numeric>)]
    pub oracle_price: Option<BigDecimal>,
    #[diesel(sql_type = Nullable<Numeric>)]
    pub oracle_twap_price: Option<BigDecimal>,
    #[diesel(sql_type = Nullable<BigInt>)]
    pub oracle_timestamp_ms: Option<i64>,
    #[diesel(sql_type = Nullable<Numeric>)]
    pub collateral_price: Option<BigDecimal>,
    #[diesel(sql_type = Nullable<Numeric>)]
    pub event_index_price: Option<BigDecimal>,
}

#[derive(Clone, Debug, QueryableByName)]
pub struct PositionRow {
    #[diesel(sql_type = Text)]
    pub market: String,
    #[diesel(sql_type = BigInt)]
    pub account_id: i64,
    #[diesel(sql_type = Numeric)]
    pub collateral: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub base: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub quote_notional: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub cum_funding_rate_long: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub cum_funding_rate_short: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub asks_quantity: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub bids_quantity: BigDecimal,
    #[diesel(sql_type = BigInt)]
    pub pending_orders: i64,
    #[diesel(sql_type = Numeric)]
    pub initial_margin_ratio: BigDecimal,
}

#[derive(Clone, Debug, QueryableByName)]
pub struct AccountRow {
    #[diesel(sql_type = BigInt)]
    pub account_id: i64,
    /// Unallocated collateral, in raw coin units.
    #[diesel(sql_type = Numeric)]
    pub collateral: BigDecimal,
}

#[derive(QueryableByName)]
struct OrderIdRow {
    #[diesel(sql_type = Text)]
    order_id: String,
}

pub struct Snapshot {
    pub watermark: Watermark,
    pub markets: Vec<MarketRow>,
    pub positions: Vec<PositionRow>,
    /// Whether `positions` is every relevant position rather than the changed ones.
    pub full: bool,
}

type Transactions = <AsyncPgConnection as AsyncConnection>::TransactionManager;

impl Store {
    pub async fn connect(url: &Url, pool_size: u32) -> anyhow::Result<Self> {
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url.as_str());
        let pool = Pool::builder()
            .max_size(pool_size)
            .connection_timeout(std::time::Duration::from_secs(10))
            .build(manager)
            .await
            .context("Failed to set up the database pool")?;
        let store = Self { pool };
        // Fail at startup rather than on the first round.
        store
            .pool
            .get()
            .await
            .context("Failed to connect to the database")?;
        Ok(store)
    }

    /// The markets and the positions in them as of the indexer's watermark: every relevant
    /// position when `since` is None, else the ones written after checkpoint `since`. None
    /// while the indexer has not indexed anything.
    pub async fn snapshot(
        &self,
        markets: &[String],
        since: Option<i64>,
    ) -> anyhow::Result<Option<Snapshot>> {
        let mut conn = self.pool.get().await.context("No database connection")?;
        Transactions::begin_transaction(&mut *conn).await?;
        let result = read_snapshot(&mut conn, markets, since).await;
        // Read-only: nothing to keep either way.
        Transactions::rollback_transaction(&mut *conn).await?;
        result
    }

    /// The IDs of an account's resting orders in a market.
    pub async fn open_orders(&self, market: &str, account_id: i64) -> anyhow::Result<Vec<u128>> {
        let mut conn = self.pool.get().await.context("No database connection")?;
        let rows: Vec<OrderIdRow> = sql_query(
            "SELECT order_id::TEXT AS order_id FROM orders \
             WHERE market = $1 AND account_id = $2 AND status = 'open' \
             ORDER BY order_id",
        )
        .bind::<Text, _>(market)
        .bind::<BigInt, _>(account_id)
        .load(&mut conn)
        .await?;
        rows.into_iter()
            .map(|row| {
                row.order_id
                    .parse::<u128>()
                    .with_context(|| format!("Order ID {} is not a u128", row.order_id))
            })
            .collect()
    }

    pub async fn account(&self, object_id: &str) -> anyhow::Result<Option<AccountRow>> {
        let mut conn = self.pool.get().await.context("No database connection")?;
        Ok(
            sql_query("SELECT account_id, collateral FROM accounts WHERE object_id = $1")
                .bind::<Text, _>(object_id)
                .get_results(&mut conn)
                .await?
                .pop(),
        )
    }

    /// Whether an account has a position object in a market.
    pub async fn has_position(&self, market: &str, account_id: i64) -> anyhow::Result<bool> {
        #[derive(QueryableByName)]
        struct Exists {
            #[diesel(sql_type = Bool)]
            exists: bool,
        }
        let mut conn = self.pool.get().await.context("No database connection")?;
        let row: Exists = sql_query(
            "SELECT EXISTS (SELECT 1 FROM positions WHERE market = $1 AND account_id = $2) AS exists",
        )
        .bind::<Text, _>(market)
        .bind::<BigInt, _>(account_id)
        .get_result(&mut conn)
        .await?;
        Ok(row.exists)
    }
}

async fn read_snapshot(
    conn: &mut AsyncPgConnection,
    markets: &[String],
    since: Option<i64>,
) -> anyhow::Result<Option<Snapshot>> {
    sql_query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(conn)
        .await?;
    let Some(watermark) = sql_query(
        "SELECT checkpoint_hi_inclusive AS checkpoint, timestamp_ms_hi_inclusive AS timestamp_ms \
         FROM watermarks WHERE pipeline = 'state'",
    )
    .get_results::<Watermark>(conn)
    .await?
    .pop() else {
        return Ok(None);
    };

    let market_rows: Vec<MarketRow> = sql_query(
        "SELECT m.market, m.paused, m.closed, m.settlement_enabled, m.settlement_base_price, \
                m.margin_ratio_initial, m.margin_ratio_maintenance, \
                (m.params->'core_params'->>'lot_size')::BIGINT AS lot_size, \
                (m.params->'core_params'->>'tick_size')::BIGINT AS tick_size, \
                m.cum_funding_rate_long, m.cum_funding_rate_short, m.funding_last_upd_ms, \
                (m.params->'twap_params'->>'funding_frequency_ms')::BIGINT AS funding_frequency_ms, \
                (m.params->'twap_params'->>'funding_period_ms')::BIGINT AS funding_period_ms, \
                m.premium_twap, m.spread_twap, m.best_bid_price, m.best_ask_price, \
                (m.params->'core_params'->>'collateral_haircut')::NUMERIC / 1e18 AS collateral_haircut, \
                m.base_storage_id, m.base_source_id, m.collateral_source_id, \
                b.price AS oracle_price, b.twap_price AS oracle_twap_price, \
                b.timestamp_ms AS oracle_timestamp_ms, \
                c.price AS collateral_price, m.index_price AS event_index_price \
         FROM markets m \
         LEFT JOIN oracle_prices b \
                ON b.storage_id = m.base_storage_id AND b.source_id = m.base_source_id \
         LEFT JOIN oracle_prices c \
                ON c.storage_id = m.collateral_storage_id AND c.source_id = m.collateral_source_id \
         WHERE m.market = ANY($1)",
    )
    .bind::<Array<Text>, _>(markets)
    .load(conn)
    .await?;

    const COLUMNS: &str = "SELECT market, account_id, collateral, base, quote_notional, \
        cum_funding_rate_long, cum_funding_rate_short, asks_quantity, bids_quantity, \
        pending_orders, initial_margin_ratio FROM positions";
    let positions: Vec<PositionRow> = match since {
        // A position matters while it has size, resting orders or debt.
        None => sql_query(format!(
            "{COLUMNS} WHERE market = ANY($1) AND (base <> 0 OR pending_orders <> 0 OR collateral < 0)"
        ))
        .bind::<Array<Text>, _>(markets)
        .load(conn)
        .await?,
        Some(since) => sql_query(format!(
            "{COLUMNS} WHERE market = ANY($1) AND updated_checkpoint > $2"
        ))
        .bind::<Array<Text>, _>(markets)
        .bind::<BigInt, _>(since)
        .load(conn)
        .await?,
    };

    Ok(Some(Snapshot {
        watermark,
        markets: market_rows,
        positions,
        full: since.is_none(),
    }))
}
