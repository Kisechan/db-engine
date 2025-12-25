use crate::common::types::PageId;

// 长数据指针（指向长数据页链的起始页）
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LongDataPtr {
    pub first_page_id: PageId,
    pub total_length: u32, // 总数据长度
}

impl LongDataPtr {
    pub fn new(first_page_id: PageId, total_length: u32) -> Self {
        LongDataPtr {
            first_page_id,
            total_length,
        }
    }

    // 序列化为字节（8 字节：4 字节 page_id + 4 字节 length）
    pub fn serialize(&self) -> [u8; 8] {
        let mut data = [0u8; 8];
        data[0..4].copy_from_slice(&self.first_page_id.to_le_bytes());
        data[4..8].copy_from_slice(&self.total_length.to_le_bytes());
        data
    }

    // 从字节反序列化
    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        if data.len() < 8 {
            return Err("Invalid LongDataPtr data".to_string());
        }
        Ok(LongDataPtr {
            first_page_id: u32::from_le_bytes([data[0], data[1], data[2], data[3]]),
            total_length: u32::from_le_bytes([data[4], data[5], data[6], data[7]]),
        })
    }
}

// LongDataPage 头部（固定 8 字节）
#[derive(Clone, Copy, Debug)]
pub struct LongDataPageHeader {
    pub next_page_id: Option<PageId>, // None 表示链尾
    pub data_length: u16,              // 本页实际数据长度
}

impl LongDataPageHeader {
    pub const SIZE: usize = 6; // 4 字节 next_page_id (0xFFFFFFFF 表示 None) + 2 字节 data_length

    pub fn serialize(&self) -> [u8; Self::SIZE] {
        let mut data = [0u8; Self::SIZE];
        let next_page = self.next_page_id.unwrap_or(0xFFFFFFFF);
        data[0..4].copy_from_slice(&next_page.to_le_bytes());
        data[4..6].copy_from_slice(&self.data_length.to_le_bytes());
        data
    }

    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        if data.len() < Self::SIZE {
            return Err("Invalid LongDataPageHeader data".to_string());
        }
        let next_page_raw = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let next_page_id = if next_page_raw == 0xFFFFFFFF {
            None
        } else {
            Some(next_page_raw)
        };
        Ok(LongDataPageHeader {
            next_page_id,
            data_length: u16::from_le_bytes([data[4], data[5]]),
        })
    }
}

// LongDataPage 管理器（使用引用设计，类似 PageHandler）
pub struct LongDataPage<'a> {
    pub data: &'a mut [u8], // 引用 BufferManager 中的页数据
    pub page_id: PageId,
}

impl<'a> LongDataPage<'a> {
    const PAGE_SIZE: usize = 4096;
    const HEADER_SIZE: usize = LongDataPageHeader::SIZE;
    const DATA_AREA_SIZE: usize = Self::PAGE_SIZE - Self::HEADER_SIZE;

    pub fn new(data: &'a mut [u8], page_id: PageId) -> Self {
        LongDataPage {
            data,
            page_id,
        }
    }

    // 读取页头
    pub fn read_header(&self) -> Result<LongDataPageHeader, String> {
        LongDataPageHeader::deserialize(&self.data[0..Self::HEADER_SIZE])
    }

    // 写入页头
    pub fn write_header(&mut self, header: LongDataPageHeader) -> Result<(), String> {
        let serialized = header.serialize();
        self.data[0..Self::HEADER_SIZE].copy_from_slice(&serialized);
        Ok(())
    }

    // 获取数据区的有效字节数
    pub fn get_data_length(&self) -> Result<usize, String> {
        let header = self.read_header()?;
        Ok(header.data_length as usize)
    }

    // 设置数据区长度
    pub fn set_data_length(&mut self, length: u16) -> Result<(), String> {
        let mut header = self.read_header()?;
        header.data_length = length;
        self.write_header(header)
    }

    // 获取下一页 ID
    pub fn get_next_page(&self) -> Result<Option<PageId>, String> {
        let header = self.read_header()?;
        Ok(header.next_page_id)
    }

    // 设置下一页 ID
    pub fn set_next_page(&mut self, next_page_id: Option<PageId>) -> Result<(), String> {
        let mut header = self.read_header()?;
        header.next_page_id = next_page_id;
        self.write_header(header)
    }

    // 获取可用空间大小
    pub fn get_available_space(&self) -> Result<usize, String> {
        let data_len = self.get_data_length()?;
        Ok(Self::DATA_AREA_SIZE - data_len)
    }

    // 在本页存储数据（返回实际存储的字节数）
    pub fn store_data(&mut self, offset: usize, data: &[u8]) -> Result<usize, String> {
        if offset > Self::DATA_AREA_SIZE {
            return Err("Offset out of bounds".to_string());
        }

        let remaining = Self::DATA_AREA_SIZE - offset;
        let to_write = std::cmp::min(data.len(), remaining);

        let write_range = (Self::HEADER_SIZE + offset)..(Self::HEADER_SIZE + offset + to_write);
        if write_range.end > Self::PAGE_SIZE {
            return Err("Write range out of page bounds".to_string());
        }

        self.data[write_range].copy_from_slice(&data[..to_write]);
        self.set_data_length((offset + to_write) as u16)?;

        Ok(to_write)
    }

    // 从本页读取数据
    pub fn load_data(&self, offset: usize, length: usize) -> Result<Vec<u8>, String> {
        if offset > Self::DATA_AREA_SIZE {
            return Err("Offset out of bounds".to_string());
        }

        let data_len = self.get_data_length()?;
        if offset > data_len {
            return Err("Offset beyond stored data".to_string());
        }

        let available = data_len - offset;
        let to_read = std::cmp::min(length, available);

        let read_range = (Self::HEADER_SIZE + offset)..(Self::HEADER_SIZE + offset + to_read);
        Ok(self.data[read_range].to_vec())
    }

    // 清空页面
    pub fn clear(&mut self) -> Result<(), String> {
        self.data.fill(0);
        let header = LongDataPageHeader {
            next_page_id: None,
            data_length: 0,
        };
        self.write_header(header)
    }
}