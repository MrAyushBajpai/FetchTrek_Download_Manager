use std::sync::Arc;
use std::time::{Duration, Instant};

use fetchtrek_core::BandwidthLimiter;

#[tokio::test]
async fn test_unlimited_rate_limiter_instant_return() {
    let limiter = BandwidthLimiter::unlimited();
    let start = Instant::now();

    // Acquire 10 MiB under unlimited mode
    limiter.acquire(10 * 1024 * 1024).await;
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_millis(5),
        "Unlimited acquire should be virtually instantaneous: {:?}",
        elapsed
    );
    assert_eq!(limiter.current_rate_limit(), 0);
    assert!(limiter.is_unlimited());
}

#[tokio::test]
async fn test_rate_clamping_accuracy() {
    let rate = 1024 * 1024u64; // 1 MiB/s (1,048,576 bytes/sec)
    let limiter = BandwidthLimiter::new(rate);

    let total_bytes = 2 * 1024 * 1024usize; // 2 MiB (2,097,152 bytes)
    let num_tasks = 4;
    let bytes_per_task = total_bytes / num_tasks; // 512 KiB per task
    let chunk_size = 64 * 1024usize; // 64 KiB chunks

    let start = Instant::now();

    let mut handles = Vec::new();
    for _ in 0..num_tasks {
        let lim = Arc::clone(&limiter);
        handles.push(tokio::spawn(async move {
            let mut acquired = 0;
            while acquired < bytes_per_task {
                let to_acquire = (bytes_per_task - acquired).min(chunk_size);
                lim.acquire(to_acquire).await;
                acquired += to_acquire;
            }
        }));
    }

    for h in handles {
        h.await.expect("worker task failed");
    }

    let elapsed = start.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();

    // Invariant: Rate clamping within ±10% tolerance (1.90s to 2.20s for 2 MiB at 1 MiB/s)
    assert!(
        (1.90..=2.20).contains(&elapsed_secs),
        "Total elapsed time for 2 MiB at 1 MiB/s must be within [1.90s, 2.20s], got {:.3}s",
        elapsed_secs
    );
}

#[tokio::test]
async fn test_dynamic_rate_adjustment() {
    let limiter = BandwidthLimiter::new(1024 * 1024); // 1 MiB/s

    let start = Instant::now();
    // Phase 1: Acquire 512 KiB at 1 MiB/s -> takes ~0.50s
    limiter.acquire(512 * 1024).await;
    let phase1_time = start.elapsed().as_secs_f64();
    assert!(
        (0.45..=0.60).contains(&phase1_time),
        "Phase 1 (512 KiB at 1 MiB/s) expected ~0.5s, got {:.3}s",
        phase1_time
    );

    // Switch rate limit to 5 MiB/s mid-transfer
    limiter.set_rate_limit(5 * 1024 * 1024);
    assert_eq!(limiter.current_rate_limit(), 5 * 1024 * 1024);

    let start_phase2 = Instant::now();
    // Phase 2: Acquire 2.5 MiB at 5 MiB/s -> takes ~0.50s
    limiter.acquire(2560 * 1024).await;
    let phase2_time = start_phase2.elapsed().as_secs_f64();
    assert!(
        (0.45..=0.65).contains(&phase2_time),
        "Phase 2 (2.5 MiB at 5 MiB/s) expected ~0.5s, got {:.3}s",
        phase2_time
    );
}
