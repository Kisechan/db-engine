use crate::ix::bplustree::BPTree;
use crate::ix::errors::IXResult;

pub struct IXHandler {
    pub tree: Option<BPTree>,
}

impl IXHandler {
    pub fn new() -> Self {
        Self { tree: None }
    }

    pub fn insert_entry(&mut self, key: Vec<u8>, rid: (u32, u16)) -> IXResult<()> {
        self.tree.as_mut().unwrap().insert(key, rid)
    }

    pub fn delete_entry(&mut self, key: Vec<u8>, rid: (u32, u16)) -> IXResult<()> {
        self.tree.as_mut().unwrap().delete(key, rid)
    }

    pub fn force_pages(&self) -> IXResult<()> {
        // 调用 PF 层 flush page
        Ok(())
    }
}
