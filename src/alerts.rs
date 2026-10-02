// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Alerts: always logged, and posted to a webhook when one is configured. The same alert is
//! posted at most once per repeat interval, so that a condition that lasts does not flood the
//! channel; the log and the counter still see every occurrence.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::json;
use tracing::{error, info, warn};
use url::Url;

use crate::config::AlertFormat;
use crate::metrics::Metrics;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Level {
    Info,
    Warning,
    Critical,
}

impl Level {
    pub fn label(&self) -> &'static str {
        match self {
            Level::Info => "info",
            Level::Warning => "warning",
            Level::Critical => "critical",
        }
    }
}

pub struct Alerts {
    webhook: Option<Url>,
    format: AlertFormat,
    repeat: Duration,
    http: reqwest::Client,
    last: Mutex<HashMap<String, Instant>>,
    metrics: Arc<Metrics>,
    /// Prefixed to every message, to tell deployments apart in a shared channel.
    service: String,
}

impl Alerts {
    pub fn new(
        webhook: Option<Url>,
        format: AlertFormat,
        repeat: Duration,
        metrics: Arc<Metrics>,
        service: String,
    ) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()?;
        Ok(Self {
            webhook,
            format,
            repeat,
            http,
            last: Mutex::default(),
            metrics,
            service,
        })
    }

    /// Raises an alert. `key` identifies the condition for deduplication.
    pub fn raise(&self, level: Level, key: &str, message: impl Into<String>) {
        let message = message.into();
        self.metrics
            .alerts
            .with_label_values(&[level.label()])
            .inc();
        match level {
            Level::Info => info!(alert = key, "{message}"),
            Level::Warning => warn!(alert = key, "{message}"),
            Level::Critical => error!(alert = key, "{message}"),
        }
        let Some(url) = self.webhook.clone() else {
            return;
        };
        {
            let mut last = self.last.lock().unwrap();
            let now = Instant::now();
            if last
                .get(key)
                .is_some_and(|at| now.duration_since(*at) < self.repeat)
            {
                return;
            }
            last.insert(key.to_owned(), now);
        }
        let body = payload(self.format, level, key, &self.service, &message);
        let http = self.http.clone();
        // Posting must never hold up a liquidation.
        tokio::spawn(async move {
            if let Err(e) = http
                .post(url)
                .json(&body)
                .send()
                .await
                .and_then(|r| r.error_for_status())
            {
                warn!("Failed to post an alert: {e}");
            }
        });
    }
}

fn payload(
    format: AlertFormat,
    level: Level,
    key: &str,
    service: &str,
    message: &str,
) -> serde_json::Value {
    let text = format!("[{service}] {}: {message}", level.label().to_uppercase());
    match format {
        AlertFormat::Slack => json!({ "text": text }),
        // Discord refuses messages over 2,000 characters.
        AlertFormat::Discord => json!({ "content": text.chars().take(1_900).collect::<String>() }),
        AlertFormat::Json => {
            json!({ "service": service, "level": level, "key": key, "message": message })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_for_each_kind_of_webhook() {
        let slack = payload(
            AlertFormat::Slack,
            Level::Critical,
            "k",
            "liq-test",
            "underfunded",
        );
        assert_eq!(slack, json!({"text": "[liq-test] CRITICAL: underfunded"}));
        let long = "x".repeat(5_000);
        let discord = payload(AlertFormat::Discord, Level::Warning, "k", "s", &long);
        assert_eq!(discord["content"].as_str().unwrap().chars().count(), 1_900);
        let plain = payload(AlertFormat::Json, Level::Info, "k", "s", "m");
        assert_eq!(
            plain,
            json!({"service": "s", "level": "info", "key": "k", "message": "m"})
        );
    }

    #[tokio::test]
    async fn repeats_are_not_posted_again_within_the_interval() {
        let metrics = Arc::new(Metrics::new().unwrap());
        // Nothing listens on port 9; posting fails quietly in the background.
        let alerts = Alerts::new(
            Some("http://127.0.0.1:9/".parse().unwrap()),
            AlertFormat::Json,
            Duration::from_secs(600),
            metrics.clone(),
            "s".into(),
        )
        .unwrap();
        alerts.raise(Level::Warning, "same", "one");
        alerts.raise(Level::Warning, "same", "two");
        alerts.raise(Level::Warning, "other", "three");
        assert_eq!(alerts.last.lock().unwrap().len(), 2);
        // Every occurrence is still counted.
        assert_eq!(metrics.alerts.with_label_values(&["warning"]).get(), 3);
    }
}
