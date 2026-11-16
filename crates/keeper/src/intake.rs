// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The door of the intake. Anyone can hand details in, and a flood of them must cost the
//! tickets that wait for their price nothing. The intake has database connections of its own,
//! takes a bounded number of requests at once, and a bounded number from one client.
//!
//! A request over either bound is turned away at once and not made to wait. The front end signs
//! nothing without the keeper's answer, so no ticket is made without its details, and the
//! client can try again. Requests that wait hold connections and memory for as long as a flood
//! lasts, and the ones of real clients wait behind them.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// How many clients are remembered at most. Past it, a client that is not known yet is taken
/// without being counted: the bound on requests at once still holds, and a flood from more
/// addresses than this must not grow the map without end.
const MAX_SOURCES: usize = 50_000;
/// Refusals are counted over this long to tell a flood from a client that tried once too often,
/// and a flood is over once this long has passed without one.
const WINDOW: Duration = Duration::from_secs(60);

/// What each client may hand in: `burst` requests at once, and `per_minute` more over time.
/// A burst of zero is no limit.
pub struct Sources {
    burst: f64,
    per_sec: f64,
    /// What a client has left, and when that was so.
    left: HashMap<String, (f64, Instant)>,
}

impl Sources {
    pub fn new(burst: u32, per_minute: u32) -> Self {
        Self {
            burst: burst as f64,
            per_sec: per_minute as f64 / 60.0,
            left: HashMap::new(),
        }
    }

    /// Takes one request of `source`. Refused, it tells how long until the client has one again.
    pub fn take(&mut self, source: &str, now: Instant) -> Result<(), Duration> {
        if self.burst < 1.0 {
            return Ok(());
        }
        if let Some((left, at)) = self.left.get_mut(source) {
            let gained = now.saturating_duration_since(*at).as_secs_f64() * self.per_sec;
            *left = (*left + gained).min(self.burst);
            *at = now;
            if *left < 1.0 {
                let wait = (1.0 - *left) / self.per_sec.max(f64::MIN_POSITIVE);
                return Err(Duration::try_from_secs_f64(wait).unwrap_or(Duration::MAX));
            }
            *left -= 1.0;
            return Ok(());
        }
        if self.left.len() >= MAX_SOURCES {
            self.forget(now);
        }
        if self.left.len() < MAX_SOURCES {
            self.left.insert(source.to_owned(), (self.burst - 1.0, now));
        }
        Ok(())
    }

    /// Forgets the clients whose allowance is whole again: they are as good as new ones.
    fn forget(&mut self, now: Instant) {
        let (burst, per_sec) = (self.burst, self.per_sec);
        self.left.retain(|_, (left, at)| {
            *left + now.saturating_duration_since(*at).as_secs_f64() * per_sec < burst
        });
    }

    pub fn remembered(&self) -> usize {
        self.left.len()
    }
}

/// The requests turned away lately.
pub struct Refusals {
    since: Instant,
    count: u64,
    last: Option<Instant>,
    told: bool,
}

impl Refusals {
    pub fn new(now: Instant) -> Self {
        Self {
            since: now,
            count: 0,
            last: None,
            told: false,
        }
    }

    /// Counts a refusal and returns how many there were in the window when this one makes
    /// them `at_least`: once per flood, and never when `at_least` is zero.
    pub fn record(&mut self, now: Instant, at_least: u64) -> Option<u64> {
        if now.saturating_duration_since(self.since) >= WINDOW {
            self.since = now;
            self.count = 0;
        }
        self.count += 1;
        self.last = Some(now);
        if at_least == 0 || self.count < at_least || self.told {
            return None;
        }
        self.told = true;
        Some(self.count)
    }

    /// True once, when a flood that was told of has been over for a whole window.
    pub fn over(&mut self, now: Instant) -> bool {
        let quiet = self
            .last
            .is_none_or(|last| now.saturating_duration_since(last) >= WINDOW);
        if self.told && quiet {
            self.told = false;
            return true;
        }
        false
    }
}

/// Why a request was turned away.
#[derive(Debug, PartialEq, Eq)]
pub enum Refused {
    /// The client handed in more than it may; it has a request again after this long.
    TooMany(Duration),
    /// As many requests as the keeper takes at once are being worked on.
    Busy,
    /// Storing the details took longer than a request may.
    Slow,
}

impl Refused {
    /// The label of the refusal in the metrics.
    pub fn reason(&self) -> &'static str {
        match self {
            Refused::TooMany(_) => "rate",
            Refused::Busy => "busy",
            Refused::Slow => "slow",
        }
    }
}

pub struct Settings {
    /// Requests worked on at once.
    pub at_once: usize,
    /// How long storing a request's details may take.
    pub timeout: Duration,
    pub burst: u32,
    pub per_minute: u32,
    /// Refusals within a minute that make a flood worth an alert; zero for none.
    pub alert_at: u64,
}

pub struct Door {
    slots: Arc<Semaphore>,
    sources: Mutex<Sources>,
    refusals: Mutex<Refusals>,
    pub timeout: Duration,
    alert_at: u64,
}

impl Door {
    pub fn new(settings: Settings) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(settings.at_once)),
            sources: Mutex::new(Sources::new(settings.burst, settings.per_minute)),
            refusals: Mutex::new(Refusals::new(Instant::now())),
            timeout: settings.timeout,
            alert_at: settings.alert_at,
        }
    }

    /// Lets a request of `source` in, for as long as the permit lives. The client's allowance
    /// is looked at first, so that a client over it takes no place from the others.
    pub fn enter(&self, source: &str, now: Instant) -> Result<OwnedSemaphorePermit, Refused> {
        self.sources
            .lock()
            .unwrap()
            .take(source, now)
            .map_err(Refused::TooMany)?;
        self.slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Refused::Busy)
    }

    /// Counts a request that was turned away. Returns the count of the last minute when the
    /// refusals have become a flood to tell of.
    pub fn refused(&self, now: Instant) -> Option<u64> {
        self.refusals.lock().unwrap().record(now, self.alert_at)
    }

    /// True once, when a flood that was told of is over.
    pub fn calm(&self, now: Instant) -> bool {
        self.refusals.lock().unwrap().over(now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn a_client_has_its_burst_and_then_one_request_at_a_time() {
        let start = Instant::now();
        let mut sources = Sources::new(3, 60);
        for _ in 0..3 {
            assert_eq!(sources.take("a", start), Ok(()));
        }
        // The fourth at once is refused, and told to come back in a second.
        assert_eq!(sources.take("a", start), Err(secs(1)));
        assert_eq!(sources.take("a", start + secs(1)), Ok(()));
        assert!(sources.take("a", start + secs(1)).is_err());
        // Another client is not held to the first one's count.
        assert_eq!(sources.take("b", start + secs(1)), Ok(()));
        // The allowance grows back to the burst and no further.
        let later = start + secs(600);
        for _ in 0..3 {
            assert_eq!(sources.take("a", later), Ok(()));
        }
        assert!(sources.take("a", later).is_err());
    }

    #[test]
    fn a_burst_of_zero_limits_nobody() {
        let start = Instant::now();
        let mut sources = Sources::new(0, 0);
        for _ in 0..1_000 {
            assert_eq!(sources.take("a", start), Ok(()));
        }
        assert_eq!(sources.remembered(), 0);
    }

    #[test]
    fn the_clients_remembered_stay_bounded() {
        let start = Instant::now();
        let mut sources = Sources::new(2, 60);
        for i in 0..MAX_SOURCES {
            assert_eq!(sources.take(&format!("c{i}"), start), Ok(()));
        }
        assert_eq!(sources.remembered(), MAX_SOURCES);
        // At the bound, with every client still short of its burst, a new one is taken and
        // not remembered.
        assert_eq!(sources.take("new", start), Ok(()));
        assert_eq!(sources.remembered(), MAX_SOURCES);
        // Once the others have their burst back they are forgotten, and the new one counts.
        let later = start + secs(120);
        assert_eq!(sources.take("new", later), Ok(()));
        assert_eq!(sources.remembered(), 1);
        assert_eq!(sources.take("new", later), Ok(()));
        assert!(sources.take("new", later).is_err());
    }

    #[test]
    fn refusals_are_a_flood_once_there_are_enough_in_a_minute() {
        let start = Instant::now();
        let mut refusals = Refusals::new(start);
        for i in 0..99 {
            assert_eq!(refusals.record(start + secs(i / 10), 100), None);
        }
        // The hundredth within the minute is told of, and the ones after it are not.
        assert_eq!(refusals.record(start + secs(10), 100), Some(100));
        assert_eq!(refusals.record(start + secs(11), 100), None);
        // It is over a minute after the last one, and that is told once.
        assert!(!refusals.over(start + secs(70)));
        assert!(refusals.over(start + secs(71)));
        assert!(!refusals.over(start + secs(72)));
        // A few refusals spread over minutes never make a flood.
        let mut refusals = Refusals::new(start);
        for i in 0..500 {
            assert_eq!(refusals.record(start + secs(i), 100), None);
        }
        // Nor does any number when no alert is asked for.
        let mut refusals = Refusals::new(start);
        for _ in 0..500 {
            assert_eq!(refusals.record(start, 0), None);
        }
        assert!(!refusals.over(start + secs(120)));
    }

    #[test]
    fn the_door_takes_a_bounded_number_at_once() {
        let door = Door::new(Settings {
            at_once: 2,
            timeout: secs(5),
            burst: 0,
            per_minute: 0,
            alert_at: 0,
        });
        let now = Instant::now();
        let first = door.enter("a", now).unwrap();
        let _second = door.enter("b", now).unwrap();
        assert_eq!(door.enter("c", now).unwrap_err(), Refused::Busy);
        // A request that is done makes room.
        drop(first);
        assert!(door.enter("c", now).is_ok());
    }

    #[test]
    fn a_client_over_its_allowance_takes_no_place() {
        let door = Door::new(Settings {
            at_once: 1,
            timeout: secs(5),
            burst: 1,
            per_minute: 60,
            alert_at: 0,
        });
        let now = Instant::now();
        drop(door.enter("a", now).unwrap());
        assert_eq!(door.enter("a", now).unwrap_err(), Refused::TooMany(secs(1)));
        // The place is free for another client.
        assert!(door.enter("b", now).is_ok());
    }
}
