use serde::{Serialize, Deserialize};
use crate::common::types::PageId;
use std::time::{SystemTime, UNIX_EPOCH};

// 数据类型枚举（可扩展）
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DataType {
    Int32,
    // 定长字符串（fixed length）
    Char(usize),
    // 变长字符串（VARCHAR）
    VarChar,
    // 可以继续添加 Date/Float 等类型
}

// 列定义
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

// 表模式（Schema）
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TableSchema {
    pub table_name: String,
    pub table_id: u32,                      // 表的唯一标识 ID
    pub columns: Vec<ColumnDef>,
    pub root_pages: Vec<PageId>,            // 初始/主数据页（可为空），TableHandler 维护更多 data pages 列表
    pub create_time: u64,                   // 创建时间戳（秒）
    pub row_count: u64,                     // 表中的记录数
    pub last_modified: u64,                 // 最后修改时间戳（秒）
}

impl TableSchema {
    // 计算一条记录的固定长度（字节）
    pub fn calculate_fixed_record_size(&self) -> usize {
        self.columns
            .iter()
            .map(|col| match col.data_type {
                DataType::Int32 => 4,
                DataType::Char(len) => len,
                DataType::VarChar => 0, // VARCHAR 使用变长指针
            })
            .sum()
    }

    // 获取当前时间戳
    pub fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }
}
