use std::hash::{Hash, Hasher};

// 块标识符（简单封装）
// 在此项目中每个块只有一个 u32 编号（相对于文件起始处的块索引）
#[derive(Clone, Copy, Debug, Eq)]
pub struct BlockId {
    // 块号（0 表示文件头块）
    pub number: u32,
}

impl BlockId {
    // 创建新的块 ID
    pub fn new(number: u32) -> Self {
        Self { number }
    }
}

impl PartialEq for BlockId {
    fn eq(&self, other: &Self) -> bool {
        self.number == other.number
    }
}

impl Hash for BlockId {
    // 为 BlockId 提供哈希支持（用于哈希表等）
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u32(self.number);
    }
}
