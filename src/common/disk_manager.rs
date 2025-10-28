use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Read, Write};
use std::path::Path;

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
            .create(true)
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

        // 检查文件是否存在
        if !Path::new(path).exists() {
            // 文件不存在，用零填充缓冲区
            println!("[DiskManager] File {} not found, returning zero-filled page {}", path, page_id);
            buffer.fill(0);
            return Ok(());
        }

        let metadata = std::fs::metadata(path)
            .map_err(|e| format!("Failed to get file metadata: {}", e))?;

        let file_size = metadata.len() as u64;
        let page_offset = (page_id as u64) * (PAGE_SIZE as u64);
        let page_end = page_offset + (PAGE_SIZE as u64);

        // 如果页超过文件范围，用零填充缓冲区
        if page_offset >= file_size {
            println!("[DiskManager] Page {} beyond file size ({}), returning zero-filled page", 
                page_id, file_size);
            buffer.fill(0);
            return Ok(());
        }

        let mut file = OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|e| format!("Failed to open file {}: {}", path, e))?;

        // 计算页的偏移位置
        file.seek(SeekFrom::Start(page_offset))
            .map_err(|e| format!("Failed to seek to page {}: {}", page_id, e))?;

        // 如果页的一部分超过文件范围，先填充零，再读取可用部分
        if page_end > file_size {
            // 先填充缓冲区为零
            buffer.fill(0);
            
            // 计算可读部分的大小
            let readable_size = (file_size - page_offset) as usize;
            
            // 只读取可用部分
            file.read_exact(&mut buffer[..readable_size])
                .map_err(|e| format!("Failed to read partial page {}: {}", page_id, e))?;
            
            println!("[DiskManager] Read partial page {} ({} of {} bytes)", 
                page_id, readable_size, PAGE_SIZE);
        } else {
            // 整页都在文件范围内，完整读取
            file.read_exact(buffer)
                .map_err(|e| format!("Failed to read page {}: {}", page_id, e))?;
        }

        Ok(())
    }

    // 删除文件
    pub fn delete_file(path: &str) -> Result<(), String> {
        // 检查文件是否存在
        if !Path::new(path).exists() {
            return Ok(()); // 文件不存在，视为成功
        }

        std::fs::remove_file(path)
            .map_err(|e| format!("Failed to delete file {}: {}", path, e))?;

        println!("[DiskManager] File deleted: {}", path);
        Ok(())
    }

    // 检查文件是否存在
    pub fn file_exists(path: &str) -> bool {
        Path::new(path).exists()
    }
}
