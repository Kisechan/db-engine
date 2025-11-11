use crate::ix::{IXIndexHandle, errors::IXResult};

pub struct IXManager {
    pfm: PFManager,
}

impl IXManager {
    pub fn new(pfm: PFManager) -> Self {
        Self { pfm }
    }

    pub fn create_index(&self, table: &str, index_no: usize, attr_len: usize) -> IXResult<()> {
        // 1. 创建 index 文件 table.index_no
        // 2. 初始化 root page
        Ok(())
    }

    pub fn destroy_index(&self, table: &str, index_no: usize) -> IXResult<()> {
        // 删除 PF index 文件
        Ok(())
    }

    pub fn open_index(&self, table: &str, index_no: usize) -> IXResult<IXIndexHandle> {
        // 打开 index PF 文件并返回 Handle
        Ok(IXIndexHandle::new())
    }

    pub fn close_index(&self, _handle: IXIndexHandle) -> IXResult<()> {
        Ok(())
    }
}
