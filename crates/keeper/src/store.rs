// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The keeper's two databases. Its own holds the details, keyed by the commitment they hash to;
//! the indexer's is only read, to tell which commitments a ticket on chain holds, which tickets
//! are open and how far the TWAP tickets have come.

use std::collections::HashMap;

use anyhow::Context;
use bigdecimal::BigDecimal;
use diesel::QueryableByName;
use diesel::sql_query;
use diesel::sql_types::{Array, BigInt, Binary, Nullable, Numeric, Text};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use url::Url;

const SCHEMA: [&str; 4] = [
    "CREATE TABLE IF NOT EXISTS ticket_details (
        commitment      BYTEA   PRIMARY KEY,
        -- 'stop_loss_take_profit', 'standalone_stop' or 'twap'.
        kind            TEXT    NOT NULL,
        -- The submission as JSON, which the keeper reads back to execute the ticket.
        details         TEXT    NOT NULL,
        submitted_at_ms BIGINT  NOT NULL,
        -- When a ticket holding the commitment was first seen in the indexer. Until then the
        -- row is dropped once it is old enough, or pushed out by newer details.
        matched_at_ms   BIGINT
    )",
    "CREATE INDEX IF NOT EXISTS ticket_details_unmatched
        ON ticket_details (submitted_at_ms) WHERE matched_at_ms IS NULL",
    // The client that handed the details in, which caps how many of them wait. Added after the
    // table was first deployed.
    "ALTER TABLE ticket_details ADD COLUMN IF NOT EXISTS source TEXT",
    "CREATE INDEX IF NOT EXISTS ticket_details_unmatched_by_source
        ON ticket_details (source, submitted_at_ms) WHERE matched_at_ms IS NULL",
];

#[derive(Debug, PartialEq, Eq)]
pub enum Inserted {
    New,
    /// The same details were handed in before.
    Known,
}

/// How many waiting details may stay.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_unmatched: i64,
    pub max_per_source: i64,
}

/// What an insert pushed out to make room: the source's own oldest waiting details, then the
/// oldest of all.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Evicted {
    pub source: usize,
    pub capacity: usize,
}

#[derive(Clone, Debug, QueryableByName)]
pub struct Stored {
    #[diesel(sql_type = Text)]
    pub kind: String,
    #[diesel(sql_type = Text)]
    pub details: String,
    #[diesel(sql_type = BigInt)]
    pub submitted_at_ms: i64,
    #[diesel(sql_type = Nullable<BigInt>)]
    pub matched_at_ms: Option<i64>,
}

#[derive(QueryableByName)]
struct CommitmentRow {
    #[diesel(sql_type = Binary)]
    commitment: Vec<u8>,
}

#[derive(QueryableByName)]
struct DetailsRow {
    #[diesel(sql_type = Binary)]
    commitment: Vec<u8>,
    #[diesel(sql_type = Text)]
    details: String,
}

/// An open stop ticket the keeper may execute.
#[derive(Clone, Debug, QueryableByName)]
pub struct OpenTicket {
    #[diesel(sql_type = Text)]
    pub ticket_id: String,
    #[diesel(sql_type = BigInt)]
    pub account_id: i64,
    /// The account's shared object, which holds the ticket.
    #[diesel(sql_type = Text)]
    pub account: String,
    /// 0: stop loss / take profit, 1: standalone stop.
    #[diesel(sql_type = BigInt)]
    pub stop_order_type: i64,
    #[diesel(sql_type = Binary)]
    pub commitment: Vec<u8>,
}

/// An open TWAP ticket the keeper may run, with its progress as the indexer last saw it.
#[derive(Clone, Debug, QueryableByName)]
pub struct OpenTwap {
    #[diesel(sql_type = Text)]
    pub ticket_id: String,
    #[diesel(sql_type = BigInt)]
    pub account_id: i64,
    /// The account's shared object, which holds the ticket.
    #[diesel(sql_type = Text)]
    pub account: String,
    /// The clearing house the ticket names, as the indexer writes it.
    #[diesel(sql_type = Text)]
    pub market: String,
    #[diesel(sql_type = Binary)]
    pub commitment: Vec<u8>,
    /// From the last `ProcessedTWAPOrderTicket`; `None` before the first attempt.
    #[diesel(sql_type = Nullable<BigInt>)]
    pub processed_amount: Option<i64>,
    #[diesel(sql_type = Nullable<BigInt>)]
    pub scheduled_amount: Option<i64>,
    #[diesel(sql_type = Nullable<BigInt>)]
    pub last_attempt_ms: Option<i64>,
    #[diesel(sql_type = Nullable<BigInt>)]
    pub retry_anchor_ms: Option<i64>,
}

#[derive(QueryableByName)]
struct LimitRow {
    #[diesel(sql_type = Text)]
    market: String,
    #[diesel(sql_type = Nullable<Numeric>)]
    max_index_twap_divergence: Option<BigDecimal>,
}

async fn pool(url: &Url, pool_size: u32, name: &str) -> anyhow::Result<Pool<AsyncPgConnection>> {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url.as_str());
    let pool = Pool::builder()
        .max_size(pool_size)
        .connection_timeout(std::time::Duration::from_secs(10))
        .build(manager)
        .await
        .with_context(|| format!("Failed to set up the {name} pool"))?;
    // Fail at startup rather than on the first request.
    pool.get()
        .await
        .with_context(|| format!("Failed to connect to the {name}"))?;
    Ok(pool)
}

/// The keeper's own database.
#[derive(Clone)]
pub struct Store {
    pool: Pool<AsyncPgConnection>,
}

impl Store {
    pub async fn connect(url: &Url, pool_size: u32) -> anyhow::Result<Self> {
        let store = Self {
            pool: pool(url, pool_size, "details database").await?,
        };
        let mut conn = store.pool.get().await.context("No database connection")?;
        for statement in SCHEMA {
            sql_query(statement)
                .execute(&mut conn)
                .await
                .context("Failed to create the details table")?;
        }
        drop(conn);
        Ok(store)
    }

    pub async fn ping(&self) -> anyhow::Result<()> {
        let mut conn = self.pool.get().await.context("No database connection")?;
        sql_query("SELECT 1").execute(&mut conn).await?;
        Ok(())
    }

    /// Stores handed-in details. Details a ticket on chain already holds (`held`) are stored as
    /// matched, whatever waits; others make room first, so a flood of made-up details pushes
    /// out the oldest waiting ones, its own first, rather than shutting real ones out.
    #[allow(clippy::too_many_arguments)]
    pub async fn insert(
        &self,
        commitment: &[u8; 32],
        kind: &str,
        details: &str,
        now_ms: i64,
        source: &str,
        held: bool,
        limits: Limits,
    ) -> anyhow::Result<(Inserted, Evicted)> {
        let mut conn = self.pool.get().await.context("No database connection")?;
        let known = sql_query("SELECT commitment FROM ticket_details WHERE commitment = $1")
            .bind::<Binary, _>(commitment.as_slice())
            .get_results::<CommitmentRow>(&mut conn)
            .await?;
        if !known.is_empty() {
            if held {
                sql_query(
                    "UPDATE ticket_details SET matched_at_ms = $2
                     WHERE commitment = $1 AND matched_at_ms IS NULL",
                )
                .bind::<Binary, _>(commitment.as_slice())
                .bind::<BigInt, _>(now_ms)
                .execute(&mut conn)
                .await?;
            }
            return Ok((Inserted::Known, Evicted::default()));
        }
        let mut evicted = Evicted::default();
        if !held {
            evicted.source = sql_query(
                "DELETE FROM ticket_details WHERE commitment IN (
                    SELECT commitment FROM ticket_details
                    WHERE matched_at_ms IS NULL AND source = $1
                    ORDER BY submitted_at_ms DESC, commitment OFFSET $2)",
            )
            .bind::<Text, _>(source)
            .bind::<BigInt, _>(limits.max_per_source - 1)
            .execute(&mut conn)
            .await?;
            evicted.capacity = sql_query(
                "DELETE FROM ticket_details WHERE commitment IN (
                    SELECT commitment FROM ticket_details WHERE matched_at_ms IS NULL
                    ORDER BY submitted_at_ms DESC, commitment OFFSET $1)",
            )
            .bind::<BigInt, _>(limits.max_unmatched - 1)
            .execute(&mut conn)
            .await?;
        }
        let rows = sql_query(
            "INSERT INTO ticket_details (commitment, kind, details, submitted_at_ms, matched_at_ms, source)
             VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (commitment) DO NOTHING",
        )
        .bind::<Binary, _>(commitment.as_slice())
        .bind::<Text, _>(kind)
        .bind::<Text, _>(details)
        .bind::<BigInt, _>(now_ms)
        .bind::<Nullable<BigInt>, _>(held.then_some(now_ms))
        .bind::<Text, _>(source)
        .execute(&mut conn)
        .await?;
        // Zero rows: the same details came in at the same moment.
        let inserted = if rows == 1 {
            Inserted::New
        } else {
            Inserted::Known
        };
        Ok((inserted, evicted))
    }

    pub async fn details(&self, commitment: &[u8]) -> anyhow::Result<Option<Stored>> {
        let mut conn = self.pool.get().await.context("No database connection")?;
        let mut rows = sql_query(
            "SELECT kind, details, submitted_at_ms, matched_at_ms
             FROM ticket_details WHERE commitment = $1",
        )
        .bind::<Binary, _>(commitment)
        .get_results::<Stored>(&mut conn)
        .await?;
        Ok(rows.pop())
    }

    /// The details stored under these commitments, as handed in, by commitment.
    pub async fn details_of(
        &self,
        commitments: &[Vec<u8>],
    ) -> anyhow::Result<HashMap<Vec<u8>, String>> {
        if commitments.is_empty() {
            return Ok(HashMap::new());
        }
        let mut conn = self.pool.get().await.context("No database connection")?;
        let rows =
            sql_query("SELECT commitment, details FROM ticket_details WHERE commitment = ANY($1)")
                .bind::<Array<Binary>, _>(commitments)
                .get_results::<DetailsRow>(&mut conn)
                .await?;
        Ok(rows
            .into_iter()
            .map(|row| (row.commitment, row.details))
            .collect())
    }

    /// The commitments still waiting for a ticket.
    pub async fn unmatched(&self) -> anyhow::Result<Vec<Vec<u8>>> {
        let mut conn = self.pool.get().await.context("No database connection")?;
        let rows = sql_query("SELECT commitment FROM ticket_details WHERE matched_at_ms IS NULL")
            .get_results::<CommitmentRow>(&mut conn)
            .await?;
        Ok(rows.into_iter().map(|row| row.commitment).collect())
    }

    pub async fn mark_matched(
        &self,
        commitments: &[Vec<u8>],
        now_ms: i64,
    ) -> anyhow::Result<usize> {
        let mut conn = self.pool.get().await.context("No database connection")?;
        Ok(sql_query(
            "UPDATE ticket_details SET matched_at_ms = $2
             WHERE commitment = ANY($1) AND matched_at_ms IS NULL",
        )
        .bind::<Array<Binary>, _>(commitments)
        .bind::<BigInt, _>(now_ms)
        .execute(&mut conn)
        .await?)
    }

    /// Drops the details submitted before `before_ms` that no ticket took up.
    pub async fn prune(&self, before_ms: i64) -> anyhow::Result<usize> {
        let mut conn = self.pool.get().await.context("No database connection")?;
        Ok(sql_query(
            "DELETE FROM ticket_details WHERE matched_at_ms IS NULL AND submitted_at_ms < $1",
        )
        .bind::<BigInt, _>(before_ms)
        .execute(&mut conn)
        .await?)
    }
}

/// The indexer's database, read only.
#[derive(Clone)]
pub struct Indexer {
    pool: Pool<AsyncPgConnection>,
}

impl Indexer {
    pub async fn connect(url: &Url, pool_size: u32) -> anyhow::Result<Self> {
        Ok(Self {
            pool: pool(url, pool_size, "indexer's database").await?,
        })
    }

    /// Of `commitments`, the ones a ticket holds. The indexer keeps a ticket's row, with its
    /// latest commitment, after the ticket is executed or deleted.
    pub async fn held(&self, commitments: &[Vec<u8>]) -> anyhow::Result<Vec<Vec<u8>>> {
        let mut conn = self.pool.get().await.context("No database connection")?;
        let rows = sql_query(
            "SELECT DISTINCT encrypted_details AS commitment FROM order_tickets
             WHERE encrypted_details = ANY($1)",
        )
        .bind::<Array<Binary>, _>(commitments)
        .get_results::<CommitmentRow>(&mut conn)
        .await?;
        Ok(rows.into_iter().map(|row| row.commitment).collect())
    }

    /// The open stop tickets that name `executor` (written as the indexer writes addresses)
    /// and no execution domain, oldest first.
    pub async fn open_stop_tickets(&self, executor: &str) -> anyhow::Result<Vec<OpenTicket>> {
        let mut conn = self.pool.get().await.context("No database connection")?;
        Ok(sql_query(
            "SELECT t.ticket_id, t.account_id, a.object_id AS account, \
                    COALESCE(t.stop_order_type, -1)::BIGINT AS stop_order_type, \
                    t.encrypted_details AS commitment \
             FROM order_tickets t JOIN accounts a ON a.account_id = t.account_id \
             WHERE t.kind = 'stop' AND t.status = 'open' AND t.execution_domain IS NULL \
               AND t.executors @> jsonb_build_array($1::TEXT) \
             ORDER BY t.created_checkpoint, t.ticket_id",
        )
        .bind::<Text, _>(executor)
        .get_results(&mut conn)
        .await?)
    }

    /// The open TWAP tickets that name `executor` and no execution domain, oldest first.
    pub async fn open_twap_tickets(&self, executor: &str) -> anyhow::Result<Vec<OpenTwap>> {
        let mut conn = self.pool.get().await.context("No database connection")?;
        Ok(sql_query(
            "SELECT t.ticket_id, t.account_id, a.object_id AS account, t.market, \
                    t.encrypted_details AS commitment, \
                    (t.twap_progress->>'processed_amount')::BIGINT AS processed_amount, \
                    (t.twap_progress->>'scheduled_amount')::BIGINT AS scheduled_amount, \
                    (t.twap_progress->>'last_attempt_timestamp_ms')::BIGINT AS last_attempt_ms, \
                    (t.twap_progress->>'retry_anchor_timestamp_ms')::BIGINT AS retry_anchor_ms \
             FROM order_tickets t JOIN accounts a ON a.account_id = t.account_id \
             WHERE t.kind = 'twap' AND t.status = 'open' AND t.execution_domain IS NULL \
               AND t.market IS NOT NULL AND t.executors @> jsonb_build_array($1::TEXT) \
             ORDER BY t.created_checkpoint, t.ticket_id",
        )
        .bind::<Text, _>(executor)
        .get_results(&mut conn)
        .await?)
    }

    /// Each market's limit on how far its index price may stray from the index TWAP, as a
    /// fraction of the TWAP.
    pub async fn divergence_limits(
        &self,
        markets: &[String],
    ) -> anyhow::Result<HashMap<String, BigDecimal>> {
        let mut conn = self.pool.get().await.context("No database connection")?;
        let rows = sql_query(
            "SELECT market, \
                    (params->'limits_params'->>'max_index_twap_divergence')::NUMERIC / 1e18 \
                        AS max_index_twap_divergence \
             FROM markets WHERE market = ANY($1)",
        )
        .bind::<Array<Text>, _>(markets)
        .get_results::<LimitRow>(&mut conn)
        .await?;
        Ok(rows
            .into_iter()
            .filter_map(|row| Some((row.market, row.max_index_twap_divergence?)))
            .collect())
    }
}
