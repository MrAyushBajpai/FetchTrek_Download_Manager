pub mod memory;
pub mod sparse;

pub use memory::{LeasedSlab, SlabBuffer, SlabPool, SLAB_SIZE, TOTAL_SLABS};
pub use sparse::allocate_sparse;
