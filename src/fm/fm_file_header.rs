use serde::{Deserialize, Serialize};

/// File-level metadata persisted in the first page.
#[repr(C)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileHeader {
    /// Total number of blocks currently allocated (including header block 0).
    pub blk_cnt: u32,
    /// Head of the free block list (-1 means empty).
    pub first_free_hole: i32,
    /// Extra fields kept for compatibility with the reference implementation.
    pub pre_f: i32,
    pub next_f: i32,
}

impl Default for FileHeader {
    fn default() -> Self {
        Self {
            blk_cnt: 1,
            first_free_hole: -1,
            pre_f: 0,
            next_f: 0,
        }
    }
}
