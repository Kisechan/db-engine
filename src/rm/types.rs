// 记录标识符：指定页号(block)和槽(slot)
pub type Rid = (u32, u16);

// 记录插入时的简单容器（列名-值）
pub struct RecAux {
    pub cols: Vec<(String, Vec<u8>)>,
}

impl RecAux {
    pub fn new() -> Self {
        RecAux { cols: Vec::new() }
    }
    pub fn push(&mut self, col: impl Into<String>, val: Vec<u8>) {
        self.cols.push((col.into(), val));
    }
}