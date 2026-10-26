// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Follows the indexer to mark the details a ticket took up, and drops the ones no ticket ever
//! did.

use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::now_ms;
use crate::store::{Indexer, Store};

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Round {
    pub waiting: usize,
    pub matched: usize,
    pub pruned: usize,
}

pub async fn round(
    store: &Store,
    indexer: &Indexer,
    ttl: Duration,
    now_ms: i64,
) -> anyhow::Result<Round> {
    let waiting = store.unmatched().await?;
    let held = if waiting.is_empty() {
        vec![]
    } else {
        indexer.held(&waiting).await?
    };
    // Matching goes first, so that a ticket that arrived late still keeps its details.
    let matched = if held.is_empty() {
        0
    } else {
        store.mark_matched(&held, now_ms).await?
    };
    let pruned = store.prune(now_ms - ttl.as_millis() as i64).await?;
    Ok(Round {
        waiting: waiting.len(),
        matched,
        pruned,
    })
}

pub async fn run(
    store: Store,
    indexer: Indexer,
    ttl: Duration,
    interval: Duration,
    cancel: CancellationToken,
) {
    loop {
        match round(&store, &indexer, ttl, now_ms()).await {
            Ok(round) if round.matched > 0 || round.pruned > 0 => info!(
                waiting = round.waiting,
                matched = round.matched,
                pruned = round.pruned,
                "Matched details to tickets"
            ),
            Ok(_) => {}
            Err(e) => warn!("Failed to match details to tickets: {e:#}"),
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(interval) => {}
        }
    }
}
