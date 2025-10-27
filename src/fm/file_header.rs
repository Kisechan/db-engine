use serde::{Serialize, Deserialize};
use crate::common::types::PageId;

const FREE_LIST_SIZE: usize = 1024; // 自由列表最大条目数

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FileHeader {
    pub total_pages: u32,        // 文件总页数
    pub free_list: Vec<PageId>,  // 空闲页列表
}

impl Default for FileHeader {
    fn default() -> Self {
        FileHeader {
            total_pages: 1, // 第 0 页是header本身
            free_list: Vec::new(),
        }
    }
}

impl FileHeader {
    pub fn serialize(&self) -> Result<Vec<u8>, String> {
        bincode::serialize(self)
            .map_err(|e| format!("Failed to serialize header: {}", e))
    }

    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        bincode::deserialize(data)
            .map_err(|e| format!("Failed to deserialize header: {}", e))
    }
}