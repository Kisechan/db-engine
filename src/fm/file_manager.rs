use crate::common::disk_manager::DiskManager;
use super::file_handler::FileHandler;
use super::file_header::FileHeader;

pub struct FileManagerConfig {
    pub page_size: usize,
}

impl Default for FileManagerConfig {
    fn default() -> Self {
        FileManagerConfig { page_size: 4096 }
    }
}

pub struct FileManager;

impl FileManager {
    // 创建一个新数据库文件
    pub fn create_file(path: &str) -> Result<(), String> {
        // 创建文件
        DiskManager::create_file(path)?;

        // 初始化文件头
        let header = FileHeader::default();
        let handler = FileHandler::new(path.to_string(), header);
        handler.flush_header()?;

        Ok(())
    }

    // 打开文件，返回 handler
    pub fn open_file(path: &str) -> Result<FileHandler, String> {
        let header = FileHandler::load_header(path)?;
        Ok(FileHandler::new(path.to_string(), header))
    }

    // 删除文件
    pub fn delete_file(path: &str) -> Result<(), String> {
        DiskManager::delete_file(path)
    }

    // 检查文件是否存在
    pub fn file_exists(path: &str) -> bool {
        DiskManager::file_exists(path)
    }
}