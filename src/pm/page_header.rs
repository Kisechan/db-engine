// 页头（存储于每页开头）
// 用于管理 slot table、空间分配等
#[derive(Default, Clone, Copy)]
pub struct PageHeader {
    pub page_type: u16,          // 页面类型魔数：0xDA7A = 数据页
    pub free_space_offset: u16,  // 数据区空闲空间起始位置（从高地址向下增长）
    pub slot_count: u16,         // slot table 总条目数（包括已删除的）
    pub free_slot_head: u16,     // 空闲 slot 链表头 (u16::MAX 表示无)
}

// 页面类型常量
pub const DATA_PAGE_MAGIC: u16 = 0xDA7A;      // 数据页标识

impl PageHeader {
    pub const SIZE: usize = 8; // 4 * u16

    pub fn serialize(&self) -> [u8; Self::SIZE] {
        let mut data = [0u8; Self::SIZE];
        data[0..2].copy_from_slice(&self.page_type.to_le_bytes());
        data[2..4].copy_from_slice(&self.free_space_offset.to_le_bytes());
        data[4..6].copy_from_slice(&self.slot_count.to_le_bytes());
        data[6..8].copy_from_slice(&self.free_slot_head.to_le_bytes());
        data
    }

    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        if data.len() < Self::SIZE {
            return Err("Invalid page header data".to_string());
        }
        Ok(PageHeader {
            page_type: u16::from_le_bytes([data[0], data[1]]),
            free_space_offset: u16::from_le_bytes([data[2], data[3]]),
            slot_count: u16::from_le_bytes([data[4], data[5]]),
            free_slot_head: u16::from_le_bytes([data[6], data[7]]),
        })
    }

    // 检查是否有空闲 slot
    pub fn has_free_slot(&self) -> bool {
        self.free_slot_head != u16::MAX
    }
    
    // 检查是否是有效的数据页
    pub fn is_valid_data_page(&self) -> bool {
        self.page_type == DATA_PAGE_MAGIC
    }
}
