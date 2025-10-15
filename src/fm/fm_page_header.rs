use serde::{Deserialize, Serialize};

use super::{bincode_options, BLOCK_SIZE};

/// Per-page metadata stored at the beginning of each on-disk block.
#[repr(C)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct PageHeader {
    pub next_free_page: i32,
    pub prev_free_page: i32,
    pub free_bytes: u32,
}

impl PageHeader {
    pub fn new_free() -> Self {
        Self {
            next_free_page: -1,
            prev_free_page: -1,
            free_bytes: Self::max_free_bytes(),
        }
    }

    pub fn max_free_bytes() -> u32 {
        (BLOCK_SIZE - Self::encoded_len()) as u32
    }

    pub fn encoded_len() -> usize {
        bincode_options()
            .serialized_size(&Self::new_free())
            .expect("page header size") as usize
    }
}

impl Default for PageHeader {
    fn default() -> Self {
        Self::new_free()
    }
}
