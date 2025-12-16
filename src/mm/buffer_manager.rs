use crate::common::types::{PageId, PAGE_SIZE};
use crate::common::disk_manager::DiskManager;
use super::frame::Frame;
use super::replacer::{Replacer, LRU};
use std::collections::HashMap;

// BufferManager：负责页缓存、替换、脏页管理
#[derive(Clone)]
pub struct BufferManager {
    frames: Vec<Frame>,                   // 缓冲池
    page_table: HashMap<PageId, usize>,   // 页号 -> 帧号的映射
    replacer: LRU,                        // 页替换器
    file_path: String,                    // 数据文件路径
}

impl BufferManager {
    pub fn new(pool_size: usize, file_path: String) -> Self {
        let mut frames = Vec::new();
        for _ in 0..pool_size {
            // pool_size 可指定缓冲池大小
            frames.push(Frame::new(PAGE_SIZE));
        }

        BufferManager {
            frames,
            page_table: HashMap::new(),
            replacer: LRU::new(pool_size),
            file_path,
        }
    }

    pub fn init_memory_from_size(size: usize, file_path: String) -> Self {
        let mut pages = if size == 0 { 1 } else { size / PAGE_SIZE };
        if pages == 0 { pages = 1; }
        println!("[BufferManager] Initializing memory pool from {} bytes -> {} pages (page_size={})",
            size, pages, PAGE_SIZE);
        BufferManager::new(pages, file_path)
    }

    // 获取页数据：查找 -> 加载 -> pin
    pub fn fetch_page(&mut self, page_id: PageId) -> Result<&mut [u8], String> {
        // 查找是否已存在
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            // 页已在缓冲中，增加 pin 计数
            self.frames[frame_id].pin_count += 1;
            self.replacer.pin(frame_id);
            return Ok(&mut self.frames[frame_id].data);
        }

        // 页不存在，选择受害者
        let victim_frame_id = self.replacer.pick_victim()
            .ok_or("No available frame in buffer pool".to_string())?;

        let victim_frame = &mut self.frames[victim_frame_id];

        // 如果受害者页是 dirty，写回磁盘
        if victim_frame.is_dirty && victim_frame.page_id != u32::MAX {
            DiskManager::write_page(&self.file_path, victim_frame.page_id, &victim_frame.data)?;
        }

        // 从页表移除旧页映射
        if victim_frame.page_id != u32::MAX {
            self.page_table.remove(&victim_frame.page_id);
        }

        // 读入新页内容
        DiskManager::read_page(&self.file_path, page_id, &mut victim_frame.data)?;

        // 更新帧信息
        victim_frame.page_id = page_id;
        victim_frame.pin_count = 1;
        victim_frame.is_dirty = false;

        // 添加页表映射
        self.page_table.insert(page_id, victim_frame_id);

        // Pin 该帧
        self.replacer.pin(victim_frame_id);

        Ok(&mut self.frames[victim_frame_id].data)
    }

    // 解 pin 页
    pub fn unpin_page(&mut self, page_id: PageId, is_dirty: bool) -> Result<(), String> {
        let frame_id = self.page_table.get(&page_id)
            .ok_or(format!("Page {} not in buffer pool", page_id))?;

        let frame_id = *frame_id;
        let frame = &mut self.frames[frame_id];

        // pin 计数递减
        if frame.pin_count > 0 {
            frame.pin_count -= 1;
        } else {
            return Err(format!("Pin count underflow for page {}", page_id));
        }

        // 记录 dirty 标记
        if is_dirty {
            frame.is_dirty = true;
        }

        // 当 pin_count 为 0 时，unpin 该帧
        if frame.pin_count == 0 {
            self.replacer.unpin(frame_id);
        }

        Ok(())
    }

    // 清空缓冲池（关闭数据库时调用）
    pub fn flush_all(&mut self) -> Result<(), String> {
        for frame in &self.frames {
            if frame.is_dirty && frame.page_id != u32::MAX {
                DiskManager::write_page(&self.file_path, frame.page_id, &frame.data)?;
            }
        }
        Ok(())
    }

    // 获取缓冲池大小
    pub fn get_pool_size(&self) -> usize {
        self.frames.len()
    }
}