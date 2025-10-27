use serde::{Serialize, Deserialize};
use crate::common::types::PageId;

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
    pub columns: Vec<ColumnDef>,
    // 初始/主数据页（可为空），TableHandler 维护更多 data pages 列表
    pub root_pages: Vec<PageId>,
}
