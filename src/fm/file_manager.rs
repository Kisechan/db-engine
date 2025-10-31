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

    // 初始化磁盘映像：在指定目录下创建 data/disk.img 并写入指定字节数的空页
    pub fn init_disk_from_size(size: usize, dir: &str) -> Result<(), String> {
        // 计算页数
        let mut pages = if size == 0 { 1 } else { size / crate::common::disk_manager::PAGE_SIZE };
        if pages == 0 { pages = 1; }

        // 确保目录存在
        std::fs::create_dir_all(dir)
            .map_err(|e| format!("Failed to create dir {}: {}", dir, e))?;

        let file_path = format!("{}/disk.img", dir);

        // 创建或截断文件
        DiskManager::create_file(&file_path)
            .map_err(|e| format!("Failed to create disk image {}: {}", file_path, e))?;

        let zero_page = vec![0u8; crate::common::disk_manager::PAGE_SIZE];

        for i in 0..pages {
            DiskManager::write_page(&file_path, i as u32, &zero_page)
                .map_err(|e| format!("Failed to write zero page {}: {}", i, e))?;
        }

        println!("[FileManager] Initialized disk image '{}' with {} pages ({} bytes)",
            file_path, pages, pages * crate::common::disk_manager::PAGE_SIZE);

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