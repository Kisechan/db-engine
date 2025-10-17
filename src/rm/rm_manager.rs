use std::io;
use crate::fm::FileHandle;
use crate::rm::types::Rid;
use crate::mm::{BufferManager, page::Page, page_ops::PageOps};

// 表级管理器：提供插入/读取/删除/扫描函数
pub struct TableManager {
    buf_mgr: BufferManager,
}

impl TableManager {
    // 使用给定的 FileHandle 和缓冲区容量创建表管理器
    pub fn new(handle: FileHandle, capacity: usize) -> Self {
        let buf_mgr = BufferManager::new(handle, capacity);
        TableManager { buf_mgr }
    }

    // 插入一条记录，返回记录标识符 (block, slot)
    pub fn insert(&mut self, data: &[u8]) -> io::Result<Rid> {
        // 分配新数据页（若已有空闲页，可扩展为先查找空闲页）
        let block = self.buf_mgr.allocate_data_page()?;
        // 读取并 pin
        let mut frame = self.buf_mgr.fetch(block)?;
        // 加载页面结构
        let mut page = Page::load(&mut *frame)?;
        // 插入记录到槽目录，获得 slot id
        let slot = page.insert_record(data)?;
        // 写回页面
        page.flush(&mut *frame)?;
        // 解除 pin
        drop(frame);
        self.buf_mgr.mark_dirty(block);
        self.buf_mgr.unpin(block);
        Ok((block, slot))
    }
    
    // 更新指定记录内容：如果新数据长度小于等于旧数据长度，则原位更新；否则，插入新记录并在原位置写入转发指针
    // pub fn update(&mut self, rid: Rid, new_data: &[u8]) -> io::Result<Rid> {
    //     let (block, slot) = rid;
    //     let mut frame = self.buf_mgr.fetch(block)?;
    //     // 加载页面结构
    //     let mut page = Page::load(&mut *frame)?;
    //     // 获取旧记录数据
    //     let old_data = page.get_record(slot)?;
    //     if new_data.len() <= old_data.len() {
    //         // 新数据适合原位更新，直接覆盖记录区域
    //         // 假设 Page 提供 update_record 方法用于原位更新
    //         page.update_record(slot, new_data)?;
    //         page.flush(&mut *frame)?;
    //         self.buf_mgr.mark_dirty(block);
    //         drop(frame);
    //         self.buf_mgr.unpin(block);
    //         Ok(rid)
    //     } else {
    //         // 新数据较长，不适合原位更新
    //         // 插入新记录，获取新记录标识符
    //         let new_rid = self.insert(new_data)?;
            
    //         // 构造转发标记（forwarding pointer）
    //         // 格式：首字节 0xFF 表示转发，后续 4 字节存 block，2 字节存 slot
    //         let fwd_marker: u8 = 0xFF;
    //         let mut fwd_bytes = Vec::new();
    //         fwd_bytes.push(fwd_marker);
    //         fwd_bytes.extend_from_slice(&new_rid.0.to_le_bytes());
    //         fwd_bytes.extend_from_slice(&new_rid.1.to_le_bytes());
    //         // 用 0 填充剩余空间，使总长度与旧记录相同
    //         if old_data.len() > fwd_bytes.len() {
    //             fwd_bytes.extend(std::iter::repeat(0u8).take(old_data.len() - fwd_bytes.len()));
    //         }
    //         // 更新旧记录为转发指针
    //         page.update_record(slot, &fwd_bytes)?;
    //         page.flush(&mut *frame)?;
    //         self.buf_mgr.mark_dirty(block);
    //         drop(frame);
    //         self.buf_mgr.unpin(block);
    //         Ok(new_rid)
    //     }
    // }

    // 根据 Rid 读取记录内容
    pub fn get(&mut self, rid: Rid) -> io::Result<Vec<u8>> {
        let (block, slot) = rid;
        let mut frame = self.buf_mgr.fetch(block)?;
        let page = Page::load(&mut *frame)?;
        let data = page.get_record(slot)?.to_vec();
        drop(frame);
        self.buf_mgr.unpin(block);
        Ok(data)
    }

    // 删除指定 Rid 的记录
    pub fn delete(&mut self, rid: Rid) -> io::Result<()> {
        let (block, slot) = rid;
        let mut frame = self.buf_mgr.fetch(block)?;
        let mut page = Page::load(&mut *frame)?;
        page.delete_record(slot)?;
        page.flush(&mut *frame)?;
        drop(frame);
        self.buf_mgr.mark_dirty(block);
        self.buf_mgr.unpin(block);
        Ok(())
    }

    // 简单扫描给定块列表，返回所有有效 Rid
    pub fn scan(&mut self, blocks: &[u32]) -> io::Result<Vec<Rid>> {
        let mut result = Vec::new();
        for &block in blocks {
            let mut frame = self.buf_mgr.fetch(block)?;
            let page = Page::load(&mut *frame)?;
            for slot in 0..page.header.slot_count {
                if page.get_record(slot).is_ok() {
                    result.push((block, slot));
                }
            }
            drop(frame);
            self.buf_mgr.unpin(block);
        }
        Ok(result)
    }
}
