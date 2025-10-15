// fm 模块的子模块导出（文件管理相关的子组件）
pub mod fm_bid; // 块标识符
pub mod fm_file_handler; // 文件句柄与块级读写、分配/回收
pub mod fm_file_header; // 文件头结构和序列化
pub mod fm_manager; // 高级文件管理（创建/删除/打开/预分配）
pub mod fm_page_header; // 每页页头

// 便捷重导出，便于外部使用统一类型名
pub use fm_bid::BlockId;
pub use fm_file_handler::FileHandle;
pub use fm_file_header::FileHeader;
pub use fm_manager::{FileManager, FileManagerConfig};
