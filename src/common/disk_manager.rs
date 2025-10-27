use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Read, Write};

pub const PAGE_SIZE: usize = 4096;

// 负责最底层磁盘 I/O，不关注页结构
pub struct DiskManager;

impl DiskManager {
    pub fn create_file(path: &str) -> Result<(), String> {
        // 创建或覆盖文件
        File::create(path)
            .map_err(|e| format!("Failed to create file {}: {}", path, e))?;
        Ok(())
    }

    pub fn write_page(path: &str, page_id: u32, data: &[u8]) -> Result<(), String> {
        if data.len() != PAGE_SIZE {
            return Err(format!("Data size {} != PAGE_SIZE {}", data.len(), PAGE_SIZE));
        }

        let mut file = OpenOptions::new()
            .write(true)
            .open(path)
            .map_err(|e| format!("Failed to open file {}: {}", path, e))?;

        // 计算页的偏移位置
        let offset = (page_id as u64) * (PAGE_SIZE as u64);
        
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| format!("Failed to seek: {}", e))?;

        file.write_all(data)
            .map_err(|e| format!("Failed to write page {}: {}", page_id, e))?;

        // 同步落盘
        file.sync_all()
            .map_err(|e| format!("Failed to sync: {}", e))?;

        Ok(())
    }

    pub fn read_page(path: &str, page_id: u32, buffer: &mut [u8]) -> Result<(), String> {
        if buffer.len() != PAGE_SIZE {
            return Err(format!("Buffer size {} != PAGE_SIZE {}", buffer.len(), PAGE_SIZE));
        }

        let mut file = OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|e| format!("Failed to open file {}: {}", path, e))?;

        // 计算页的偏移位置
        let offset = (page_id as u64) * (PAGE_SIZE as u64);
        
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| format!("Failed to seek: {}", e))?;

        file.read_exact(buffer)
            .map_err(|e| format!("Failed to read page {}: {}", page_id, e))?;

        Ok(())
    }
}
