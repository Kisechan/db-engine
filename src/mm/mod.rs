pub mod buffer_manager;
pub mod page;
pub mod page_compact;
pub mod page_guard;
pub mod page_header;
pub mod page_ops;

/// 导出 BufferManager 和 BlockId
pub use buffer_manager::BufferManager;
