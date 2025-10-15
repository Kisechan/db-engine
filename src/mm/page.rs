use crate::mm::page_header::PageHeader;
use std::io::{self, ErrorKind};

/// 内存页结构，包含页头、数据区和槽目录
pub struct Page {
    pub header: PageHeader,
    /// 记录数据区（不包含页头）
    pub data: Vec<u8>,
    /// 槽目录：每个槽存 (offset, length)
    pub slots: Vec<(u16, u16)>,
}

impl Page {
    /// 从 frame 读取并解析成 Page
    pub fn load(frame: &mut [u8]) -> io::Result<Page> {
        // 解析页头
        let header = PageHeader::from_bytes(&frame[0..PageHeader::SIZE])?;
        let page_size = frame.len();
        let slot_count = header.slot_count as usize;
        let slot_dir_size = slot_count * 4;
        if page_size < PageHeader::SIZE + slot_dir_size {
            return Err(io::Error::new(ErrorKind::InvalidData, "frame too small for slots"));
        }
        // 解析槽目录（位于页末）
        let mut slots = Vec::with_capacity(slot_count);
        let mut slot_base = page_size - slot_dir_size;
        for _ in 0..slot_count {
            let off = u16::from_le_bytes([frame[slot_base], frame[slot_base + 1]]);
            let len = u16::from_le_bytes([frame[slot_base + 2], frame[slot_base + 3]]);
            slots.push((off, len));
            slot_base += 4;
        }
        // 解析数据区
        let data_end = header.free_offset as usize;
        if data_end < PageHeader::SIZE || data_end > page_size - slot_dir_size {
            return Err(io::Error::new(ErrorKind::InvalidData, "invalid free_offset"));
        }
        let data_len = data_end - PageHeader::SIZE;
        let mut data = vec![0u8; data_len];
        data.copy_from_slice(&frame[PageHeader::SIZE..data_end]);
        Ok(Page { header, data, slots })
    }

    /// 将 Page 序列化并写入 frame
    pub fn flush(&self, frame: &mut [u8]) -> io::Result<()> {
        let page_size = frame.len();
        let slot_count = self.slots.len();
        let slot_dir_size = slot_count * 4;
        // 检查 frame 空间
        if page_size < PageHeader::SIZE + self.data.len() + slot_dir_size {
            return Err(io::Error::new(ErrorKind::UnexpectedEof, "frame too small to flush page"));
        }
        // 更新并写入页头
        let mut hdr = self.header.clone();
        hdr.slot_count = slot_count as u16;
        hdr.free_offset = (PageHeader::SIZE + self.data.len()) as u16;
        // free_bytes 保持在内存结构中管理
        hdr.to_bytes(&mut frame[0..PageHeader::SIZE])?;
        // 写入数据区
        let data_end = PageHeader::SIZE + self.data.len();
        frame[PageHeader::SIZE..data_end].copy_from_slice(&self.data);
        // 写入槽目录
        let mut slot_base = page_size - slot_dir_size;
        for &(off, len) in &self.slots {
            frame[slot_base..slot_base + 2].copy_from_slice(&off.to_le_bytes());
            frame[slot_base + 2..slot_base + 4].copy_from_slice(&len.to_le_bytes());
            slot_base += 4;
        }
        Ok(())
    }
}
