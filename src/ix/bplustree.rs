use crate::ix::node::BPTreeNode;
use crate::ix::errors::{IXResult, IXError};

pub struct BPTree {
    pub root: PageId,
    pub order: usize,  // 每节点最大 key 数
    pfm: PFManager,    // page handler
}

impl BPTree {
    pub fn new(pfm: PFManager, order: usize) -> Self {
        Self { root: 0, order, pfm }
    }

    pub fn insert(&mut self, key: Vec<u8>, rid: (u32, u16)) -> IXResult<()> {
        // TODO: B+Tree 插入逻辑（查找叶子、插入、必要时分裂）
        Ok(())
    }

    pub fn delete(&mut self, key: Vec<u8>, rid: (u32, u16)) -> IXResult<()> {
        // TODO: 删除逻辑（合并/再平衡）
        Ok(())
    }

    pub fn search(&self, key: &[u8]) -> IXResult<Option<(u32, u16)>> {
        // TODO: 查找叶子节点 key
        Ok(None)
    }

    pub fn scan_range(&self, lower: &[u8], upper: &[u8]) -> IXResult<Vec<(u32, u16)>> {
        // TODO: 范围扫描
        Ok(vec![])
    }
}
