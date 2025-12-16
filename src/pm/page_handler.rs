use crate::common::types::{SlotId, RID, PageId, PAGE_SIZE};
use super::page_header::PageHeader;

// Slot 条目：记录数据的偏移和长度
// 当 offset == -1 时，表示此 slot 已删除
// 此时 length 字段用于存储链表中下一个空闲 slot 的 ID（或 u16::MAX 表示链表末尾）
#[derive(Clone, Copy, Debug)]
pub struct SlotEntry {
    pub offset: i16,  // -1 表示已删除（在空闲链表中），否则为数据在页内的偏移
    pub length: u16,  // 当 offset >= 0 时为数据长度；当 offset == -1 时为下一个空闲 slot ID
}

impl SlotEntry {
    pub const SIZE: usize = 4; // i16 + u16

    pub fn serialize(&self) -> [u8; Self::SIZE] {
        let mut data = [0u8; Self::SIZE];
        data[0..2].copy_from_slice(&self.offset.to_le_bytes());
        data[2..4].copy_from_slice(&self.length.to_le_bytes());
        data
    }

    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        if data.len() < Self::SIZE {
            return Err("Invalid slot entry data".to_string());
        }
        Ok(SlotEntry {
            offset: i16::from_le_bytes([data[0], data[1]]),
            length: u16::from_le_bytes([data[2], data[3]]),
        })
    }

    // 创建已删除的 slot（加入空闲链表）
    pub fn new_free(next_free_slot_id: u16) -> Self {
        SlotEntry {
            offset: -1,
            length: next_free_slot_id,
        }
    }

    // 获取下一个空闲 slot 的 ID（仅当此 slot 已删除时有效）
    pub fn get_next_free(&self) -> u16 {
        if self.offset == -1 {
            self.length
        } else {
            u16::MAX
        }
    }
}

// 页操作封装：插入、删除记录
pub struct PageHandler<'a> {
    pub data: &'a mut [u8], // 页内二进制内容
    pub page_id: PageId,    // 页号
}

impl<'a> PageHandler<'a> {
    pub fn new(data: &'a mut [u8], page_id: PageId) -> Self {
        PageHandler { data, page_id }
    }

    // 读取页头
    pub fn read_header(&self) -> Result<PageHeader, String> {
        PageHeader::deserialize(&self.data[0..PageHeader::SIZE])
    }

    // 写入页头
    pub fn write_header(&mut self, header: PageHeader) -> Result<(), String> {
        let serialized = header.serialize();
        self.data[0..PageHeader::SIZE].copy_from_slice(&serialized);
        Ok(())
    }

    // 计算 slot table 的起始位置（紧跟在页头后）
    pub fn slot_table_start(&self) -> usize {
        PageHeader::SIZE
    }

    // 计算 slot table 的结束位置
    pub fn slot_table_end(&self, slot_count: u16) -> usize {
        self.slot_table_start() + (slot_count as usize) * SlotEntry::SIZE
    }

    // 读取指定 slot
    pub fn read_slot(&self, slot_id: u16) -> Result<SlotEntry, String> {
        let offset = self.slot_table_start() + (slot_id as usize) * SlotEntry::SIZE;
        if offset + SlotEntry::SIZE > PAGE_SIZE {
            return Err("Slot index out of bounds".to_string());
        }
        SlotEntry::deserialize(&self.data[offset..offset + SlotEntry::SIZE])
    }

    // 写入指定 slot
    pub fn write_slot(&mut self, slot_id: u16, slot: SlotEntry) -> Result<(), String> {
        let offset = self.slot_table_start() + (slot_id as usize) * SlotEntry::SIZE;
        if offset + SlotEntry::SIZE > PAGE_SIZE {
            return Err("Slot index out of bounds".to_string());
        }
        let serialized = slot.serialize();
        self.data[offset..offset + SlotEntry::SIZE].copy_from_slice(&serialized);
        Ok(())
    }

    // 插入记录（优先重用空闲 slot）
    pub fn insert_record(&mut self, record: &[u8]) -> Result<RID, String> {
        let record_len = record.len() as u16;
        
        // 读取页头
        let mut header = self.read_header()?;

        // 尝试从空闲链表获取 slot
        let slot_id = if header.has_free_slot() {
            // 从空闲链表中取出第一个空闲 slot
            let free_slot_id = header.free_slot_head;
            let free_slot = self.read_slot(free_slot_id)?;
            
            // 更新链表头指向下一个空闲 slot
            header.free_slot_head = free_slot.get_next_free();
            self.write_header(header)?;
            
            println!("[PageHandler] Page {}: Reusing free slot {} (next_free={}), slot_count={}", 
                self.page_id, free_slot_id, header.free_slot_head, header.slot_count);
            
            free_slot_id
        } else {
            // 无空闲 slot，分配新 slot
            let new_slot_id = header.slot_count;
            header.slot_count += 1;
            
            // println!("[PageHandler] Page {}: Allocating new slot {} (total_slots now: {})", 
            //     self.page_id, new_slot_id, header.slot_count);
            // 一般这里会输出 102，这是个巧合现象
            
            new_slot_id
        };

        // 检查空间
        let slot_table_end = self.slot_table_end(header.slot_count);
        let required_space = record_len as usize;
        let available_space = header.free_space_offset as usize - slot_table_end;

        if available_space < required_space {
            return Err(format!(
                "No enough space in page: need {} bytes, available {} bytes",
                required_space, available_space
            ));
        }

        // 分配数据空间（从高地址向下增长）
        let data_offset = header.free_space_offset - record_len;
        
        // 写入记录数据
        let data_range = (data_offset as usize)..(data_offset as usize + record_len as usize);
        if data_range.end > PAGE_SIZE {
            return Err("Record offset out of bounds".to_string());
        }
        self.data[data_range].copy_from_slice(record);

        // 创建 slot 条目
        let slot = SlotEntry {
            offset: data_offset as i16,
            length: record_len,
        };

        // 写入 slot
        self.write_slot(slot_id, slot)?;

        // 第五步：更新页头
        header.free_space_offset = data_offset;
        self.write_header(header)?;

        Ok(RID {
            page_id: self.page_id,
            slot_id,
        })
    }

    // 删除记录：加入空闲链表（物理删除）
    pub fn delete_record(&mut self, rid: RID) -> Result<(), String> {
        if rid.page_id != self.page_id {
            return Err("RID page_id mismatch".to_string());
        }

        let mut header = self.read_header()?;
        
        if rid.slot_id >= header.slot_count {
            return Err("Slot id out of bounds".to_string());
        }

        // 读取要删除的 slot
        let slot = self.read_slot(rid.slot_id)?;

        // 检查是否已删除
        if slot.offset == -1 {
            return Err(format!("Record at slot {} already deleted", rid.slot_id));
        }

        // 将此 slot 加入空闲链表
        // 新的 slot 会指向原链表头
        let new_free_slot = SlotEntry::new_free(header.free_slot_head);
        self.write_slot(rid.slot_id, new_free_slot)?;

        // 更新链表头
        header.free_slot_head = rid.slot_id;

        // 更新页头
        self.write_header(header)?;

        println!("[PageHandler] Deleted record at RID{{{}, {}}}, new_free_head={}", 
            rid.page_id, rid.slot_id, header.free_slot_head);

        Ok(())
    }

    // 获取记录数据（如果未删除）
    pub fn get_record(&self, rid: RID) -> Result<Vec<u8>, String> {
        if rid.page_id != self.page_id {
            return Err("RID page_id mismatch".to_string());
        }

        let header = self.read_header()?;
        
        if rid.slot_id >= header.slot_count {
            return Err("Slot id out of bounds".to_string());
        }

        let slot = self.read_slot(rid.slot_id)?;

        // 检查是否已删除
        if slot.offset == -1 {
            return Err(format!("Record at RID{{{}, {}}} has been deleted", 
                rid.page_id, rid.slot_id));
        }

        let offset = slot.offset as usize;
        let length = slot.length as usize;

        if offset + length > PAGE_SIZE {
            return Err("Record data out of bounds".to_string());
        }

        Ok(self.data[offset..offset + length].to_vec())
    }

    // 获取页面统计信息（用于诊断）
    pub fn get_stats(&self) -> Result<PageStats, String> {
        let header = self.read_header()?;
        
        // 遍历空闲链表计数
        let mut free_slot_count = 0;
        let mut current_free = header.free_slot_head;
        
        while current_free != u16::MAX {
            free_slot_count += 1;
            let slot = self.read_slot(current_free)?;
            current_free = slot.get_next_free();
            
            // 防止死循环
            if free_slot_count > header.slot_count {
                return Err("Free list corrupted".to_string());
            }
        }

        Ok(PageStats {
            total_slots: header.slot_count,
            free_slots: free_slot_count,
            used_slots: header.slot_count - free_slot_count,
            free_data_space: header.free_space_offset as usize - self.slot_table_end(header.slot_count),
        })
    }
}

// 页面统计信息结构
#[derive(Clone, Debug)]
pub struct PageStats {
    pub total_slots: u16,      // 总 slot 数
    pub free_slots: u16,       // 空闲 slot 数
    pub used_slots: u16,       // 已使用 slot 数
    pub free_data_space: usize, // 空闲数据空间
}

impl PageStats {
    pub fn slot_utilization(&self) -> f32 {
        if self.total_slots == 0 {
            return 0.0;
        }
        self.used_slots as f32 / self.total_slots as f32
    }
}