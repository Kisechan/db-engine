use crate::common::types::{SlotId, RID, PageId};
use super::page_header::PageHeader;

const PAGE_SIZE: usize = 4096;

// Slot 条目：记录数据的偏移和长度
#[derive(Clone, Copy, Debug)]
pub struct SlotEntry {
    pub offset: i16,  // -1 表示已删除，否则为数据在页内的偏移
    pub length: u16,  // 数据长度
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
    fn read_header(&self) -> Result<PageHeader, String> {
        PageHeader::deserialize(&self.data[0..PageHeader::SIZE])
    }

    // 写入页头
    fn write_header(&mut self, header: PageHeader) -> Result<(), String> {
        let serialized = header.serialize();
        self.data[0..PageHeader::SIZE].copy_from_slice(&serialized);
        Ok(())
    }

    // 计算 slot table 的起始位置（紧跟在页头后）
    fn slot_table_start(&self) -> usize {
        PageHeader::SIZE
    }

    // 计算 slot table 的结束位置
    fn slot_table_end(&self, slot_count: u16) -> usize {
        self.slot_table_start() + (slot_count as usize) * SlotEntry::SIZE
    }

    // 读取指定 slot
    fn read_slot(&self, slot_id: u16) -> Result<SlotEntry, String> {
        let offset = self.slot_table_start() + (slot_id as usize) * SlotEntry::SIZE;
        if offset + SlotEntry::SIZE > PAGE_SIZE {
            return Err("Slot index out of bounds".to_string());
        }
        SlotEntry::deserialize(&self.data[offset..offset + SlotEntry::SIZE])
    }

    // 写入指定 slot
    fn write_slot(&mut self, slot_id: u16, slot: SlotEntry) -> Result<(), String> {
        let offset = self.slot_table_start() + (slot_id as usize) * SlotEntry::SIZE;
        if offset + SlotEntry::SIZE > PAGE_SIZE {
            return Err("Slot index out of bounds".to_string());
        }
        let serialized = slot.serialize();
        self.data[offset..offset + SlotEntry::SIZE].copy_from_slice(&serialized);
        Ok(())
    }

    // 插入记录
    pub fn insert_record(&mut self, record: &[u8]) -> Result<RID, String> {
        let record_len = record.len() as u16;
        
        // 读取页头
        let mut header = self.read_header()?;
        let slot_table_end = self.slot_table_end(header.slot_count);

        // 检查是否有足够空间
        // 需要空间：新记录 + 新 slot 条目
        let required_space = (record_len as usize) + SlotEntry::SIZE;
        let available_space = header.free_space_offset as usize - slot_table_end;

        if available_space < required_space {
            return Err("No enough space in page".to_string());
        }

        // 分配 slot
        let slot_id = header.slot_count;
        header.slot_count += 1;

        // 从高地址向下分配空间
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

        // 更新页头
        header.free_space_offset = data_offset;
        self.write_header(header)?;

        Ok(RID {
            page_id: self.page_id,
            slot_id,
        })
    }

    // 删除记录（仅标记为已删除）
    pub fn delete_record(&mut self, rid: RID) -> Result<(), String> {
        if rid.page_id != self.page_id {
            return Err("RID page_id mismatch".to_string());
        }

        let header = self.read_header()?;
        
        if rid.slot_id >= header.slot_count {
            return Err("Slot id out of bounds".to_string());
        }

        // 读取 slot
        let mut slot = self.read_slot(rid.slot_id)?;

        // 标记为已删除
        slot.offset = -1;

        // 写回 slot
        self.write_slot(rid.slot_id, slot)?;

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
            return Err("Record has been deleted".to_string());
        }

        let offset = slot.offset as usize;
        let length = slot.length as usize;

        if offset + length > PAGE_SIZE {
            return Err("Record data out of bounds".to_string());
        }

        Ok(self.data[offset..offset + length].to_vec())
    }
}