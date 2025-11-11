use crate::ix::errors::{IXResult, IXError};
use crate::ix::ix_handler::IXHandler;
use std::collections::HashMap;

// 索引管理器
// 管理所有打开的索引，提供索引的生命周期管理
#[allow(dead_code)]
pub struct IXManager {
    // 打开的索引处理器映射：(table_name, index_no) -> IXHandler
    handlers: HashMap<String, IXHandler>,
}

#[allow(dead_code)]
impl IXManager {
    // 创建新的索引管理器
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    // 创建一个新索引
    // 
    // # 参数
    // - `table`: 表名
    // - `index_no`: 索引编号
    // - `attr_len`: 属性长度
    // 
    // # 实现步骤
    // 1. 构造索引文件名
    // 2. 创建 IXHandler 实例
    // 3. 初始化 B+ 树
    // 4. 保存到打开的处理器映射
    pub fn create_index(
        &mut self,
        table: &str,
        index_no: usize,
        _attr_len: usize,
    ) -> IXResult<()> {
        let index_key = Self::make_index_key(table, index_no);

        // 检查索引是否已存在
        if self.handlers.contains_key(&index_key) {
            return Err(IXError::IndexAlreadyExists);
        }

        // 构造索引文件名
        let file_name = format!("{}.idx{}", table, index_no);

        // 创建处理器
        let mut handler = IXHandler::with_config(file_name.clone(), 4);

        // 初始化树
        handler.init_tree()?;

        // 保存处理器
        self.handlers.insert(index_key.clone(), handler);

        println!("[IXManager] Created index: {} ({})", index_key, file_name);

        Ok(())
    }

    // 销毁索引
    // 
    // # 参数
    // - `table`: 表名
    // - `index_no`: 索引编号
    pub fn destroy_index(&mut self, table: &str, index_no: usize) -> IXResult<()> {
        let index_key = Self::make_index_key(table, index_no);

        // 关闭索引
        if let Some(mut handler) = self.handlers.remove(&index_key) {
            handler.close()?;
            println!("[IXManager] Destroyed index: {}", index_key);
            Ok(())
        } else {
            Err(IXError::IndexNotFound)
        }
    }

    // 打开索引
    // 
    // # 参数
    // - `table`: 表名
    // - `index_no`: 索引编号
    pub fn open_index(&mut self, table: &str, index_no: usize) -> IXResult<()> {
        let index_key = Self::make_index_key(table, index_no);

        // 检查是否已打开
        if self.handlers.contains_key(&index_key) {
            return Ok(());
        }

        // 构造索引文件名
        let file_name = format!("{}.idx{}", table, index_no);

        // 创建处理器
        let mut handler = IXHandler::with_config(file_name.clone(), 4);

        // 打开树
        handler.open_tree()?;

        // 保存处理器
        self.handlers.insert(index_key.clone(), handler);

        println!("[IXManager] Opened index: {}", index_key);

        Ok(())
    }

    // 关闭索引
    // 
    // # 参数
    // - `table`: 表名
    // - `index_no`: 索引编号
    pub fn close_index(&mut self, table: &str, index_no: usize) -> IXResult<()> {
        let index_key = Self::make_index_key(table, index_no);

        if let Some(mut handler) = self.handlers.remove(&index_key) {
            handler.close()?;
            println!("[IXManager] Closed index: {}", index_key);
            Ok(())
        } else {
            Err(IXError::IndexNotOpen)
        }
    }

    // 获取索引处理器的可变引用
    pub fn get_handler_mut(
        &mut self,
        table: &str,
        index_no: usize,
    ) -> IXResult<&mut IXHandler> {
        let index_key = Self::make_index_key(table, index_no);

        self.handlers
            .get_mut(&index_key)
            .ok_or(IXError::IndexNotOpen)
    }

    // 获取索引处理器的不可变引用
    pub fn get_handler(&self, table: &str, index_no: usize) -> IXResult<&IXHandler> {
        let index_key = Self::make_index_key(table, index_no);

        self.handlers
            .get(&index_key)
            .ok_or(IXError::IndexNotOpen)
    }

    // 列出所有打开的索引
    pub fn list_indexes(&self) -> Vec<String> {
        self.handlers.keys().cloned().collect()
    }

    // 关闭所有索引
    pub fn close_all(&mut self) -> IXResult<()> {
        for (name, mut handler) in self.handlers.drain() {
            handler.close()?;
            println!("[IXManager] Closed index: {}", name);
        }
        Ok(())
    }

    // 构造索引键
    fn make_index_key(table: &str, index_no: usize) -> String {
        format!("{}_{}", table, index_no)
    }
}

impl Default for IXManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ix_manager_creation() {
        let manager = IXManager::new();
        assert_eq!(manager.list_indexes().len(), 0);
    }

    #[test]
    fn test_create_index() {
        let mut manager = IXManager::new();

        let result = manager.create_index("employee", 0, 4);
        assert!(result.is_ok());

        let indexes = manager.list_indexes();
        assert_eq!(indexes.len(), 1);
        assert!(indexes[0].contains("employee"));

        let _ = manager.close_all();
    }

    #[test]
    fn test_duplicate_index_creation() {
        let mut manager = IXManager::new();

        manager.create_index("employee", 0, 4).ok();

        // 再次创建同一索引应该失败
        let result = manager.create_index("employee", 0, 4);
        assert!(result.is_err());

        let _ = manager.close_all();
    }

    #[test]
    fn test_open_close_index() {
        let mut manager = IXManager::new();

        manager.create_index("employee", 0, 4).ok();
        assert_eq!(manager.list_indexes().len(), 1);

        // 关闭索引
        assert!(manager.close_index("employee", 0).is_ok());
        assert_eq!(manager.list_indexes().len(), 0);
    }

    #[test]
    fn test_get_handler() {
        let mut manager = IXManager::new();

        manager.create_index("employee", 0, 4).ok();

        // 获取处理器
        let handler = manager.get_handler("employee", 0);
        assert!(handler.is_ok());

        // 获取不存在的处理器
        let handler = manager.get_handler("employee", 1);
        assert!(handler.is_err());

        let _ = manager.close_all();
    }

    #[test]
    fn test_multiple_indexes() {
        let mut manager = IXManager::new();

        manager.create_index("employee", 0, 4).ok();
        manager.create_index("employee", 1, 4).ok();
        manager.create_index("department", 0, 4).ok();

        let indexes = manager.list_indexes();
        assert_eq!(indexes.len(), 3);

        let _ = manager.close_all();
    }

    #[test]
    fn test_insert_delete_via_handler() {
        let mut manager = IXManager::new();

        manager.create_index("student", 0, 4).ok();

        // 通过处理器插入
        {
            let handler = manager.get_handler_mut("student", 0).unwrap();
            let key = vec![101];
            let rid = (100u32, 1u16);
            assert!(handler.insert_entry(key, rid).is_ok());
        }

        // 通过处理器删除
        {
            let handler = manager.get_handler_mut("student", 0).unwrap();
            let key = vec![101];
            let rid = (100u32, 1u16);
            assert!(handler.delete_entry(key, rid).is_ok());
        }

        let _ = manager.close_all();
    }
}
