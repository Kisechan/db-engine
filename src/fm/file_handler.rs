use crate::common::types::{PageId, PAGE_SIZE};
use crate::common::disk_manager::DiskManager;
use super::file_header::FileHeader;
use std::io::{Cursor, Read};

// 已打开的文件句柄
#[derive(Clone)]
pub struct FileHandler {
    pub file_path: String,
    pub header: FileHeader,
}

impl FileHandler {
    pub fn new(file_path: String, header: FileHeader) -> Self {
        FileHandler { file_path, header }
    }

    // 从磁盘加载文件头
    // 格式：[header_size: u32 (4 bytes)][serialized_header_data]
    pub fn load_header(path: &str) -> Result<FileHeader, String> {
        // 检查文件是否存在
        if !std::path::Path::new(path).exists() {
            println!("[FileHandler] File {} not found, using default header", path);
            return Ok(FileHeader::default());
        }

        // 检查文件大小
        let metadata = std::fs::metadata(path)
            .map_err(|e| format!("Failed to get file metadata for {}: {}", path, e))?;

        if metadata.len() < 4 {
            println!("[FileHandler] File {} too small ({} bytes), using default header", 
                path, metadata.len());
            return Ok(FileHeader::default());
        }

        // 读取完整的第0页
        let mut page_buffer = vec![0u8; PAGE_SIZE];
        DiskManager::read_page(path, 0, &mut page_buffer)?;

        // 读取头部的 4 字节大小信息
        let mut size_cursor = Cursor::new(&page_buffer[0..4]);
        let mut size_bytes = [0u8; 4];
        size_cursor.read_exact(&mut size_bytes)
            .map_err(|e| format!("Failed to read header size: {}", e))?;
        let header_size = u32::from_le_bytes(size_bytes) as usize;

        // 验证大小的合理性
        if header_size == 0 || header_size > PAGE_SIZE - 4 {
            println!("[FileHandler] Invalid header size: {}, using default header", header_size);
            return Ok(FileHeader::default());
        }

        // 提取序列化的头部数据
        let header_data = &page_buffer[4..4 + header_size];

        // 反序列化
        let header = FileHeader::deserialize(header_data)?;
        println!("[FileHandler] Loaded header from {}: total_pages={}, free_list_len={}", 
            path, header.total_pages, header.free_list.len());

        Ok(header)
    }

    // 将文件头写入磁盘
    // 格式：[header_size: u32 (4 bytes)][serialized_header_data][padding to page size]
    pub fn flush_header(&self) -> Result<(), String> {
        let data = self.header.serialize()?;
        let header_size = data.len() as u32;

        // 构造页面数据
        let mut page_data = vec![0u8; PAGE_SIZE];

        // 写入大小信息（4 字节）
        page_data[0..4].copy_from_slice(&header_size.to_le_bytes());

        // 写入序列化数据
        page_data[4..4 + data.len()].copy_from_slice(&data);

        // 写入到磁盘第0页
        DiskManager::write_page(&self.file_path, 0, &page_data)?;
        
        println!("[FileHandler] Flushed header to {}: size={} bytes, total_pages={}, free_list_len={}", 
            self.file_path, header_size, self.header.total_pages, self.header.free_list.len());

        Ok(())
    }

    // 分配一个页
    pub fn allocate_page(&mut self) -> Result<PageId, String> {
        let page_id = if let Some(page_id) = self.header.free_list.pop() {
            // 从空闲列表获取
            println!("[FileHandler] Allocated page {} from free list", page_id);
            page_id
        } else {
            // 扩展文件
            let page_id = self.header.total_pages;
            self.header.total_pages += 1;
            println!("[FileHandler] Allocated new page {} (total_pages now: {})", 
                page_id, self.header.total_pages);
            page_id
        };

        self.flush_header()?;
        Ok(page_id)
    }

    // 回收页
    pub fn deallocate_page(&mut self, page_id: PageId) -> Result<(), String> {
        if page_id == 0 {
            return Err("Cannot deallocate page 0 (header page)".to_string());
        }

        if (page_id as u32) >= self.header.total_pages {
            return Err(format!("Invalid page_id: {} (total_pages: {})", page_id, self.header.total_pages));
        }

        self.header.free_list.push(page_id);
        println!("[FileHandler] Deallocated page {}, free_list_len now: {}", 
            page_id, self.header.free_list.len());
        self.flush_header()?;
        Ok(())
    }

    // 写入页数据
    pub fn write_page(&self, page_id: PageId, data: &[u8]) -> Result<(), String> {
        DiskManager::write_page(&self.file_path, page_id as u32, data)
    }

    // 读取页数据
    pub fn read_page(&self, page_id: PageId, buffer: &mut [u8]) -> Result<(), String> {
        DiskManager::read_page(&self.file_path, page_id as u32, buffer)
    }
}