use serde::{Deserialize, Serialize};

// 全局基础类型定义
pub type PageId = u32;
pub type SlotId = u16;

// 记录标识符 = 页号 + 槽号
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct RID {
    pub page_id: PageId,
    pub slot_id: SlotId,
}
