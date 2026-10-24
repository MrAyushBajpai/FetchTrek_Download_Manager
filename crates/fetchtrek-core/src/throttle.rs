use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug)]
struct LimiterState {
    rate: u64,
    tokens: f64,
    last_update: tokio::time::Instant,
    max_burst: f64,
}

#[derive(Debug)]
pub struct BandwidthLimiter {
    state: Mutex<LimiterState>,
}

pub type TokenBucket = BandwidthLimiter;

impl BandwidthLimiter {
    pub fn new(bytes_per_sec: u64) -> Arc<Self> {
        let max_burst = if bytes_per_sec > 0 {
            (bytes_per_sec as f64).max(64.0 * 1024.0)
        } else {
            0.0
        };

        Arc::new(Self {
            state: Mutex::new(LimiterState {
                rate: bytes_per_sec,
                tokens: 0.0,
                last_update: tokio::time::Instant::now(),
                max_burst,
            }),
        })
    }

    pub fn unlimited() -> Arc<Self> {
        Self::new(0)
    }

    pub fn current_rate_limit(&self) -> u64 {
        self.state.lock().unwrap().rate
    }

    pub fn is_unlimited(&self) -> bool {
        self.current_rate_limit() == 0
    }

    pub fn set_rate_limit(&self, bytes_per_sec: u64) {
        let mut guard = self.state.lock().unwrap();
        let now = tokio::time::Instant::now();
        if guard.rate > 0 {
            let elapsed = now
                .saturating_duration_since(guard.last_update)
                .as_secs_f64();
            guard.tokens = (guard.tokens + elapsed * guard.rate as f64).min(guard.max_burst);
        }
        guard.last_update = now;
        guard.rate = bytes_per_sec;
        guard.max_burst = if bytes_per_sec > 0 {
            (bytes_per_sec as f64).max(64.0 * 1024.0)
        } else {
            0.0
        };
    }

    pub async fn acquire(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }

        let delay = {
            let mut guard = self.state.lock().unwrap();
            if guard.rate == 0 {
                return;
            }

            let now = tokio::time::Instant::now();
            let elapsed = now
                .saturating_duration_since(guard.last_update)
                .as_secs_f64();
            guard.last_update = now;

            // Replenish tokens proportionally to elapsed wall-clock time
            guard.tokens = (guard.tokens + elapsed * guard.rate as f64).min(guard.max_burst);

            let requested = bytes as f64;
            guard.tokens -= requested;

            if guard.tokens < 0.0 {
                let deficit = -guard.tokens;
                let wait_secs = deficit / (guard.rate as f64);
                Duration::from_secs_f64(wait_secs)
            } else {
                Duration::ZERO
            }
        };

        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
    }
}
