// 页头（存储于每页开头）
// 用于管理 slot table、空间分配等
#[derive(Default, Clone, Copy)]
pub struct PageHeader {
    pub free_space_offset: u16,  // 数据区空闲空间起始位置（从高地址向下增长）
    pub slot_count: u16,         // slot table 条目数
}

impl PageHeader {
    pub const SIZE: usize = 4; // 2 * u16

    pub fn serialize(&self) -> [u8; Self::SIZE] {
        let mut data = [0u8; Self::SIZE];
        data[0..2].copy_from_slice(&self.free_space_offset.to_le_bytes());
        data[2..4].copy_from_slice(&self.slot_count.to_le_bytes());
        data
    }

    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        if data.len() < Self::SIZE {
            return Err("Invalid page header data".to_string());
        }
        Ok(PageHeader {
            free_space_offset: u16::from_le_bytes([data[0], data[1]]),
            slot_count: u16::from_le_bytes([data[2], data[3]]),
        })
    }
}
