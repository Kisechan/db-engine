use crate::common::types::PageId;

// 内存缓冲帧
#[derive(Clone)]
pub struct Frame {
    pub page_id: PageId,
    pub data: Vec<u8>,
    pub pin_count: u32,
    pub is_dirty: bool,
}

impl Frame {
    pub fn new(page_size: usize) -> Self {
        Frame {
            page_id: u32::MAX, // 初始化为无效页号
            data: vec![0u8; page_size],
            pin_count: 0,
            is_dirty: false,
        }
    }
}
