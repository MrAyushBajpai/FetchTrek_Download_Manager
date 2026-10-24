use core::cell::UnsafeCell;
use core::fmt;
use std::alloc::{alloc_zeroed, handle_alloc_error, Layout};
use std::sync::Arc;

use crossbeam_queue::ArrayQueue;

pub const SLAB_SIZE: usize = 64 * 1024;
pub const TOTAL_SLABS: usize = 256;

#[repr(C, align(4096))]
pub struct SlabBuffer {
    pub data: [u8; SLAB_SIZE],
}

impl fmt::Debug for SlabBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlabBuffer")
            .field("size", &SLAB_SIZE)
            .field("align", &4096)
            .finish()
    }
}

pub struct SlabPool {
    storage: Box<[UnsafeCell<SlabBuffer>; TOTAL_SLABS]>,
    free_indices: ArrayQueue<usize>,
}

// SAFETY: SlabPool manages thread-safe handoff of slab indices via an ArrayQueue.
// Storage memory for each slab is accessed exclusively by the thread holding its LeasedSlab.
unsafe impl Send for SlabPool {}
unsafe impl Sync for SlabPool {}

impl SlabPool {
    pub fn new() -> Arc<Self> {
        let layout = Layout::new::<[UnsafeCell<SlabBuffer>; TOTAL_SLABS]>();
        // SAFETY: Allocates the 16 MiB arena directly on heap memory to avoid stack overflow.
        // Zeroed memory is a valid representation for [u8; SLAB_SIZE].
        let ptr = unsafe { alloc_zeroed(layout) as *mut [UnsafeCell<SlabBuffer>; TOTAL_SLABS] };
        if ptr.is_null() {
            handle_alloc_error(layout);
        }
        let storage = unsafe { Box::from_raw(ptr) };

        let free_indices = ArrayQueue::new(TOTAL_SLABS);
        for i in 0..TOTAL_SLABS {
            let _ = free_indices.push(i);
        }

        Arc::new(Self {
            storage,
            free_indices,
        })
    }

    pub fn acquire(self: &Arc<Self>) -> Option<LeasedSlab> {
        self.free_indices.pop().map(|index| LeasedSlab {
            pool: Arc::clone(self),
            index,
            valid_bytes: 0,
        })
    }

    pub fn available_slabs(&self) -> usize {
        self.free_indices.len()
    }

    pub fn is_under_backpressure(&self) -> bool {
        self.available_slabs() <= 51
    }
}

impl fmt::Debug for SlabPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlabPool")
            .field("available_slabs", &self.available_slabs())
            .field("total_slabs", &TOTAL_SLABS)
            .field("is_under_backpressure", &self.is_under_backpressure())
            .finish()
    }
}

pub struct LeasedSlab {
    pool: Arc<SlabPool>,
    pub index: usize,
    pub valid_bytes: usize,
}

// SAFETY: LeasedSlab has exclusive ownership of the slab at `self.index`.
// It may be transferred across thread boundaries safely.
unsafe impl Send for LeasedSlab {}

impl LeasedSlab {
    pub fn as_slice(&self) -> &[u8] {
        let ptr = self.pool.storage[self.index].get();
        // SAFETY: `self.index` is exclusively held by this lease.
        unsafe { &(*ptr).data }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        let ptr = self.pool.storage[self.index].get();
        // SAFETY: `&mut self` and unique index ownership guarantee no aliasing references.
        unsafe { &mut (*ptr).data }
    }

    pub fn valid_slice(&self) -> &[u8] {
        let len = self.valid_bytes.min(SLAB_SIZE);
        &self.as_slice()[..len]
    }
}

impl Drop for LeasedSlab {
    fn drop(&mut self) {
        let _ = self.pool.free_indices.push(self.index);
    }
}

impl fmt::Debug for LeasedSlab {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LeasedSlab")
            .field("index", &self.index)
            .field("valid_bytes", &self.valid_bytes)
            .finish()
    }
}
