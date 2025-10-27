use crate::common::types::PageId;
use crate::common::disk_manager::DiskManager;
use super::file_header::FileHeader;

const PAGE_SIZE: usize = 4096;

// 已打开的文件句柄
pub struct FileHandler {
    pub file_path: String,
    pub header: FileHeader,
}

impl FileHandler {
    pub fn new(file_path: String, header: FileHeader) -> Self {
        FileHandler { file_path, header }
    }

    // 从磁盘加载文件头
    pub fn load_header(path: &str) -> Result<FileHeader, String> {
        let mut buffer = vec![0u8; PAGE_SIZE];
        DiskManager::read_page(path, 0, &mut buffer)?;
        
        // 找到实际数据的结束位置（去除填充的0字节）
        let end = buffer.iter().rposition(|&b| b != 0).unwrap_or(0) + 1;
        FileHeader::deserialize(&buffer[..end])
    }

    // 将文件头写入磁盘
    pub fn flush_header(&self) -> Result<(), String> {
        let data = self.header.serialize()?;
        let mut page_data = vec![0u8; PAGE_SIZE];
        page_data[..data.len()].copy_from_slice(&data);
        DiskManager::write_page(&self.file_path, 0, &page_data)?;
        Ok(())
    }

    // 分配一个页
    pub fn allocate_page(&mut self) -> Result<PageId, String> {
        let page_id = if let Some(page_id) = self.header.free_list.pop() {
            // 从空闲列表获取
            page_id
        } else {
            // 扩展文件
            let page_id = self.header.total_pages;
            self.header.total_pages += 1;
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
            return Err(format!("Invalid page_id: {}", page_id));
        }

        self.header.free_list.push(page_id);
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