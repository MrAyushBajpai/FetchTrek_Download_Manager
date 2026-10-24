use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use fetchtrek_storage::{LeasedSlab, SlabPool, SLAB_SIZE, TOTAL_SLABS};

#[test]
fn test_pool_exhaustion_and_reclamation() {
    let pool = SlabPool::new();
    assert_eq!(pool.available_slabs(), TOTAL_SLABS);
    assert!(!pool.is_under_backpressure());

    let mut leases: Vec<LeasedSlab> = Vec::with_capacity(TOTAL_SLABS);
    for _ in 0..TOTAL_SLABS {
        let lease = pool.acquire().expect("acquisition should succeed");
        leases.push(lease);
    }

    // 257th acquisition must return None
    assert_eq!(pool.available_slabs(), 0);
    assert!(pool.acquire().is_none());
    assert!(pool.is_under_backpressure());

    // Verify all 256 leased indices are unique
    let mut indices: Vec<usize> = leases.iter().map(|l| l.index).collect();
    indices.sort_unstable();
    for (expected, actual) in indices.iter().enumerate() {
        assert_eq!(expected, *actual);
    }

    // Drop one lease: available count increments, can reacquire
    let dropped_index = leases.pop().unwrap().index;
    assert_eq!(pool.available_slabs(), 1);
    let reacquired = pool.acquire().expect("should reacquire freed slab");
    assert_eq!(reacquired.index, dropped_index);
    assert_eq!(pool.available_slabs(), 0);
    drop(reacquired);

    // Drop all remaining leases: complete RAII reclamation
    drop(leases);
    assert_eq!(pool.available_slabs(), TOTAL_SLABS);
    assert!(!pool.is_under_backpressure());
}

#[test]
fn test_backpressure_threshold() {
    let pool = SlabPool::new();

    // Acquire down to 52 slabs (256 - 52 = 204 leased)
    let mut leases: Vec<LeasedSlab> = Vec::with_capacity(TOTAL_SLABS);
    for _ in 0..(TOTAL_SLABS - 52) {
        leases.push(pool.acquire().unwrap());
    }
    assert_eq!(pool.available_slabs(), 52);
    assert!(!pool.is_under_backpressure());

    // Acquire 1 more: 51 free slabs remaining -> backpressure triggers
    let threshold_lease = pool.acquire().unwrap();
    assert_eq!(pool.available_slabs(), 51);
    assert!(pool.is_under_backpressure());

    // Acquire another: 50 free slabs -> still backpressure
    let sub_threshold_lease = pool.acquire().unwrap();
    assert_eq!(pool.available_slabs(), 50);
    assert!(pool.is_under_backpressure());

    // Reclaim to 51: still backpressure
    drop(sub_threshold_lease);
    assert_eq!(pool.available_slabs(), 51);
    assert!(pool.is_under_backpressure());

    // Reclaim to 52: backpressure clears
    drop(threshold_lease);
    assert_eq!(pool.available_slabs(), 52);
    assert!(!pool.is_under_backpressure());
}

#[test]
fn test_slab_data_access_and_valid_slice() {
    let pool = SlabPool::new();
    let mut lease = pool.acquire().expect("should acquire");

    assert_eq!(lease.as_slice().len(), SLAB_SIZE);
    assert_eq!(lease.as_mut_slice().len(), SLAB_SIZE);
    assert_eq!(lease.valid_bytes, 0);
    assert_eq!(lease.valid_slice().len(), 0);

    let payload = b"FetchTrek high-performance storage engine";
    lease.as_mut_slice()[..payload.len()].copy_from_slice(payload);
    lease.valid_bytes = payload.len();

    assert_eq!(lease.valid_slice(), payload);
    assert_eq!(&lease.as_slice()[..payload.len()], payload);
}

#[test]
fn test_concurrent_multi_threaded_stress() {
    const NUM_THREADS: usize = 16;
    const OPS_PER_THREAD: usize = 1000; // 16,000 total operations

    static IN_USE: [AtomicBool; TOTAL_SLABS] = [const { AtomicBool::new(false) }; TOTAL_SLABS];

    let pool = SlabPool::new();
    let mut handles = Vec::with_capacity(NUM_THREADS);

    for thread_id in 0..NUM_THREADS {
        let thread_pool = Arc::clone(&pool);
        let handle = thread::spawn(move || {
            for iter in 0..OPS_PER_THREAD {
                // Spin-wait until a slab is acquired
                let mut lease = loop {
                    if let Some(l) = thread_pool.acquire() {
                        break l;
                    }
                    thread::yield_now();
                };

                let idx = lease.index;

                // Detect double-allocation: flag must transition from false to true
                let was_in_use = IN_USE[idx].swap(true, Ordering::SeqCst);
                assert!(
                    !was_in_use,
                    "Double allocation detected: slab {idx} already in use"
                );

                // Write thread-specific verification pattern
                let magic_byte = ((thread_id * 17 + iter) & 0xFF) as u8;
                let slice = lease.as_mut_slice();
                slice[0] = magic_byte;
                slice[SLAB_SIZE - 1] = magic_byte;
                lease.valid_bytes = SLAB_SIZE;

                // Verify written values
                assert_eq!(lease.as_slice()[0], magic_byte);
                assert_eq!(lease.as_slice()[SLAB_SIZE - 1], magic_byte);

                // Clear usage flag before drop
                let was_still_in_use = IN_USE[idx].swap(false, Ordering::SeqCst);
                assert!(was_still_in_use, "Slab {idx} was prematurely released");

                drop(lease);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Worker thread panicked");
    }

    // Verify zero leaks and all slabs restored to pool
    assert_eq!(pool.available_slabs(), TOTAL_SLABS);
    assert!(!pool.is_under_backpressure());

    // Verify all in-use flags are false
    for (idx, flag) in IN_USE.iter().enumerate() {
        assert!(
            !flag.load(Ordering::SeqCst),
            "Slab {idx} leaked in active state"
        );
    }
}
