pub mod buffer_manager;
pub mod page_header;
pub mod page;
pub mod page_ops;

/// 导出 BufferManager 和 BlockId
pub use buffer_manager::{BufferManager, BlockId};
