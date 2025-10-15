pub mod fm_file_handler;
pub mod fm_file_header;
pub mod fm_manager;
pub mod fm_page_header;

pub use fm_file_handler::FileHandle;
pub use fm_file_header::FileHeader;
pub use fm_manager::FileManager;
pub use fm_page_header::PageHeader;

pub const BLOCK_SIZE: usize = 4096;
pub const PREALLOC_SIZE: usize = BLOCK_SIZE * 256;

use bincode::config::{DefaultOptions, Options};

pub(crate) fn bincode_options() -> impl Options {
    DefaultOptions::new()
        .with_fixint_encoding()
        .with_little_endian()
        .allow_trailing_bytes()
}
