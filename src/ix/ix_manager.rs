use crate::ix::errors::IXResult;

pub struct IXManager {
    // pfm: PFManager,  // TODO: 需要定义 PFManager
}

impl IXManager {
    pub fn new() -> Self {
        Self {}
    }

    pub fn create_index(&self, _table: &str, _index_no: usize, _attr_len: usize) -> IXResult<()> {
        // 1. 创建 index 文件 table.index_no
        // 2. 初始化 root page
        Ok(())
    }

    pub fn destroy_index(&self, _table: &str, _index_no: usize) -> IXResult<()> {
        // 删除 PF index 文件
        Ok(())
    }

    pub fn open_index(&self, _table: &str, _index_no: usize) -> IXResult<()> {
        // 打开 index PF 文件并返回 Handle
        Ok(())
    }

    pub fn close_index(&self) -> IXResult<()> {
        Ok(())
    }
}
