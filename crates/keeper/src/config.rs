// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! What the keeper is told.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::ensure;
use clap::Parser;
use url::Url;

#[derive(Parser, Debug)]
#[clap(rename_all = "kebab-case", author, version)]
pub struct Args {
    /// The keeper's own database, for the tickets' details. Keep it apart from the indexer's,
    /// which can be dropped and rebuilt from the chain: the details exist nowhere else.
    #[clap(env = "PERP_KEEPER_DATABASE_URL", long)]
    pub details_database_url: Url,
    /// The indexer's database, read to tell which details a ticket on chain has taken up. A
    /// read-only role is enough.
    #[clap(env = "PERP_DATABASE_URL", long)]
    pub database_url: Url,
    /// Connections to each database.
    #[clap(long, default_value_t = 4)]
    pub pool_size: u32,

    /// The largest request body taken, in bytes. A ticket's details are a few hundred.
    #[clap(long, default_value_t = 4_096)]
    pub max_body_bytes: usize,
    /// New details are refused while this many wait for their tickets.
    #[clap(long, default_value_t = 10_000)]
    pub max_unmatched: i64,
    /// How long details wait for a ticket with their commitment before they are dropped, in
    /// seconds. The front end hands them in right before the ticket's transaction is signed,
    /// so the ones still waiting after this belong to a transaction that never went through.
    #[clap(long, default_value_t = 86_400)]
    pub unmatched_ttl_secs: u64,
    /// How often to look up the tickets of waiting details, in seconds.
    #[clap(long, default_value_t = 60)]
    pub match_interval_secs: u64,

    /// Serves `POST /v1/details` and `/health`.
    #[clap(long, default_value = "127.0.0.1:9191")]
    pub listen_address: SocketAddr,
    /// Log as JSON lines.
    #[clap(long)]
    pub log_json: bool,
}

impl Args {
    pub fn match_interval(&self) -> Duration {
        Duration::from_secs(self.match_interval_secs)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(self.pool_size > 0, "--pool-size is 0");
        ensure!(self.max_body_bytes > 0, "--max-body-bytes is 0");
        ensure!(self.max_unmatched > 0, "--max-unmatched is not positive");
        ensure!(self.unmatched_ttl_secs > 0, "--unmatched-ttl-secs is 0");
        ensure!(self.match_interval_secs > 0, "--match-interval-secs is 0");
        Ok(())
    }
}
