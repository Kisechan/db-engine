use crate::mm::page::Page;
use crate::mm::page_header::PageHeader;
use std::io;

// 页面紧缩，将有效记录移动到数据区前部，重写槽目录，释放连续空间
pub trait PageCompact {
    fn compact(&mut self, page_size: usize) -> io::Result<()>;
}

impl PageCompact for Page {
    fn compact(&mut self, page_size: usize) -> io::Result<()> {
        // 新数据区和槽目录
        let mut new_data = Vec::new();
        let mut new_slots = Vec::new();
        // 遍历旧 slot
        for &(off, len) in &self.slots {
            if len == 0 {
                continue;
            } // 跳过空槽
              // 计算旧数据区相对于 data Vec 的偏移
            let start = (off as usize).saturating_sub(PageHeader::SIZE);
            let end = start + len as usize;
            // 新槽偏移 = header 后 + new_data 长度
            let new_off = PageHeader::SIZE as u16 + new_data.len() as u16;
            new_data.extend_from_slice(&self.data[start..end]);
            new_slots.push((new_off, len));
        }
        // 更新内存结构
        self.data = new_data;
        self.slots = new_slots;
        // 更新页头
        let slot_count = self.slots.len() as u16;
        let free_offset = PageHeader::SIZE as u16 + self.data.len() as u16;
        let slot_dir_size = (self.slots.len() * 4) as u16;
        let free_bytes = page_size as u16 - free_offset - slot_dir_size;
        self.header.slot_count = slot_count;
        self.header.free_offset = free_offset;
        self.header.free_bytes = free_bytes;
        Ok(())
    }
}
