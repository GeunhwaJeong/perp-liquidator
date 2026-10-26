// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The keeper's two databases. Its own holds the details, keyed by the commitment they hash to;
//! the indexer's is only read, to tell which commitments a ticket on chain holds.

use anyhow::Context;
use diesel::QueryableByName;
use diesel::sql_query;
use diesel::sql_types::{Array, BigInt, Binary, Nullable, Text};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use url::Url;

const SCHEMA: [&str; 2] = [
    "CREATE TABLE IF NOT EXISTS ticket_details (
        commitment      BYTEA   PRIMARY KEY,
        -- 'stop_loss_take_profit', 'standalone_stop' or 'twap'.
        kind            TEXT    NOT NULL,
        -- The submission as JSON, which the keeper reads back to execute the ticket.
        details         TEXT    NOT NULL,
        submitted_at_ms BIGINT  NOT NULL,
        -- When a ticket holding the commitment was first seen in the indexer. Until then the
        -- row is dropped once it is old enough.
        matched_at_ms   BIGINT
    )",
    "CREATE INDEX IF NOT EXISTS ticket_details_unmatched
        ON ticket_details (submitted_at_ms) WHERE matched_at_ms IS NULL",
];

#[derive(Debug, PartialEq, Eq)]
pub enum Inserted {
    New,
    /// The same details were handed in before.
    Known,
    /// Too many details are waiting for their tickets.
    Full,
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
struct CountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
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

    pub async fn insert(
        &self,
        commitment: &[u8; 32],
        kind: &str,
        details: &str,
        now_ms: i64,
        max_unmatched: i64,
    ) -> anyhow::Result<Inserted> {
        let mut conn = self.pool.get().await.context("No database connection")?;
        let known = sql_query("SELECT commitment FROM ticket_details WHERE commitment = $1")
            .bind::<Binary, _>(commitment.as_slice())
            .get_results::<CommitmentRow>(&mut conn)
            .await?;
        if !known.is_empty() {
            return Ok(Inserted::Known);
        }
        let waiting =
            sql_query("SELECT count(*) AS count FROM ticket_details WHERE matched_at_ms IS NULL")
                .get_result::<CountRow>(&mut conn)
                .await?
                .count;
        if waiting >= max_unmatched {
            return Ok(Inserted::Full);
        }
        let rows = sql_query(
            "INSERT INTO ticket_details (commitment, kind, details, submitted_at_ms)
             VALUES ($1, $2, $3, $4) ON CONFLICT (commitment) DO NOTHING",
        )
        .bind::<Binary, _>(commitment.as_slice())
        .bind::<Text, _>(kind)
        .bind::<Text, _>(details)
        .bind::<BigInt, _>(now_ms)
        .execute(&mut conn)
        .await?;
        // Zero rows: the same details came in at the same moment.
        Ok(if rows == 1 {
            Inserted::New
        } else {
            Inserted::Known
        })
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
}
