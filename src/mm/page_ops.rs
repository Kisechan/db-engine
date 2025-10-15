use std::io::{self, ErrorKind};
use crate::mm::page::Page;

/// 在页面上操作记录的接口
pub trait PageOps {
    /// 插入一条记录，返回槽 ID
    fn insert_record(&mut self, data: &[u8]) -> io::Result<u16>;
    /// 根据槽 ID 获取记录数据切片
    fn get_record(&self, slot_id: u16) -> io::Result<&[u8]>;
    /// 删除指定槽 ID 的记录
    fn delete_record(&mut self, slot_id: u16) -> io::Result<()>;
}

impl PageOps for Page {
    fn insert_record(&mut self, data: &[u8]) -> io::Result<u16> {
        let data_len = data.len() as u16;
        // 每个槽目录项占 4 字节
        let slot_entry_size = 4u16;
        // 检查剩余空间
        if self.header.free_bytes < data_len + slot_entry_size {
            return Err(io::Error::new(ErrorKind::Other, "页面空间不足，无法插入记录"));
        }
        // 计算记录写入偏移，相对于页面起始
        let off = self.header.free_offset;
        // 写入 data 到内存 data 区
        self.data.extend_from_slice(data);
        // 增加槽目录
        self.slots.push((off, data_len));
        // 更新页头元数据
        self.header.slot_count += 1;
        self.header.free_offset += data_len;
        self.header.free_bytes = self.header.free_bytes - data_len - slot_entry_size;
        // 返回新插入的槽 ID
        Ok((self.slots.len() - 1) as u16)
    }

    fn get_record(&self, slot_id: u16) -> io::Result<&[u8]> {
        let idx = slot_id as usize;
        if idx >= self.slots.len() {
            return Err(io::Error::new(ErrorKind::InvalidInput, "无效的槽 ID"));
        }
        let (off, len) = self.slots[idx];
        if len == 0 {
            return Err(io::Error::new(ErrorKind::NotFound, "指定槽无记录或已删除"));
        }
        // data Vec 从页头之后开始，因此偏移应减去页头长度
        let start = (off as usize).saturating_sub(crate::mm::page_header::PageHeader::SIZE);
        let end = start + len as usize;
        if end > self.data.len() {
            return Err(io::Error::new(ErrorKind::UnexpectedEof, "记录数据超出范围"));
        }
        Ok(&self.data[start..end])
    }

    fn delete_record(&mut self, slot_id: u16) -> io::Result<()> {
        let idx = slot_id as usize;
        if idx >= self.slots.len() {
            return Err(io::Error::new(ErrorKind::InvalidInput, "无效的槽 ID"));
        }
        let (off, len) = self.slots[idx];
        if len == 0 {
            return Err(io::Error::new(ErrorKind::NotFound, "指定槽无记录或已删除"));
        }
        // 释放空间：增加 free_bytes，简单不做紧缩
        let slot_entry_size = 4u16;
        self.header.free_bytes += len + slot_entry_size;
        // 标记为空槽
        self.slots[idx] = (0, 0);
        self.header.slot_count -= 1;
        Ok(())
    }
}