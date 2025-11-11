use crate::ix::bplustree::BPTree;
use crate::ix::errors::{IXResult, IXError};

// B+ 树索引处理器
// 提供索引操作的高层接口，关联 BPTree 实例
#[allow(dead_code)]
pub struct IXHandler {
    pub tree: Option<BPTree>,
    tree_order: usize,  // B+ 树的阶数
    file_name: String,  // 关联的文件名
}

#[allow(dead_code)]
impl IXHandler {
    // 创建新的索引处理器
    pub fn new() -> Self {
        Self {
            tree: None,
            tree_order: 4,  // 默认阶数
            file_name: String::new(),
        }
    }

    // 使用指定的文件名和阶数创建索引处理器
    pub fn with_config(file_name: String, order: usize) -> Self {
        Self {
            tree: None,
            tree_order: order,
            file_name,
        }
    }

    // 初始化 B+ 树
    pub fn init_tree(&mut self) -> IXResult<()> {
        if self.tree.is_some() {
            return Err(IXError::IndexAlreadyExists);
        }

        let btree = BPTree::new(self.tree_order);
        self.tree = Some(btree);

        println!("[IXHandler] Initialized BPTree with order={} for file: {}",
            self.tree_order, self.file_name);

        Ok(())
    }

    // 从磁盘打开索引
    pub fn open_tree(&mut self) -> IXResult<()> {
        // TODO: 从磁盘读取 BPTree 的根节点和结构
        self.init_tree()?;
        println!("[IXHandler] Opened BPTree from file: {}", self.file_name);
        Ok(())
    }

    // 检查树是否已初始化
    fn ensure_tree_initialized(&self) -> IXResult<()> {
        if self.tree.is_none() {
            return Err(IXError::IndexNotOpen);
        }
        Ok(())
    }

    // 向索引中插入条目
    // 
    // # 参数
    // - `key`: 索引键（二进制格式）
    // - `rid`: 记录 ID (page_id, slot_id)
    pub fn insert_entry(&mut self, key: Vec<u8>, rid: (u32, u16)) -> IXResult<()> {
        self.ensure_tree_initialized()?;

        match self.tree.as_mut() {
            Some(tree) => {
                println!("[IXHandler] Inserting key of length {} into index", key.len());
                tree.insert(key, rid)
            }
            None => Err(IXError::IndexNotOpen),
        }
    }

    // 从索引中删除条目
    // 
    // # 参数
    // - `key`: 索引键（二进制格式）
    // - `rid`: 记录 ID (page_id, slot_id)
    pub fn delete_entry(&mut self, key: Vec<u8>, rid: (u32, u16)) -> IXResult<()> {
        self.ensure_tree_initialized()?;

        match self.tree.as_mut() {
            Some(tree) => {
                println!("[IXHandler] Deleting key of length {} from index", key.len());
                tree.delete(key, rid)
            }
            None => Err(IXError::IndexNotOpen),
        }
    }

    // 搜索索引中的键
    // 
    // # 参数
    // - `key`: 索引键（二进制格式）
    // 
    // # 返回
    // 如果找到，返回 Some((page_id, slot_id))，否则返回 None
    pub fn search_entry(&self, key: &[u8]) -> IXResult<Option<(u32, u16)>> {
        self.ensure_tree_initialized()?;

        match &self.tree {
            Some(tree) => {
                println!("[IXHandler] Searching for key of length {}", key.len());
                tree.search(key)
            }
            None => Err(IXError::IndexNotOpen),
        }
    }

    // 范围扫描索引
    // 
    // # 参数
    // - `lower`: 下界键
    // - `upper`: 上界键
    // 
    // # 返回
    // 满足条件的所有记录 ID 列表
    pub fn scan_range(&self, lower: &[u8], upper: &[u8]) -> IXResult<Vec<(u32, u16)>> {
        self.ensure_tree_initialized()?;

        match &self.tree {
            Some(tree) => {
                println!("[IXHandler] Scanning range between keys");
                tree.scan_range(lower, upper)
            }
            None => Err(IXError::IndexNotOpen),
        }
    }

    // 将所有修改的页面写入磁盘
    // 
    // 调用页管理器的 flush 方法确保所有索引数据持久化
    pub fn force_pages(&self) -> IXResult<()> {
        self.ensure_tree_initialized()?;

        println!("[IXHandler] Flushing all index pages to disk for file: {}", 
            self.file_name);

        // TODO: 调用 PF 层的 flush_all_pages 方法
        // pfm.flush_all_pages()?;

        Ok(())
    }

    // 关闭索引，释放资源
    pub fn close(&mut self) -> IXResult<()> {
        self.force_pages()?;
        self.tree = None;

        println!("[IXHandler] Closed index for file: {}", self.file_name);
        Ok(())
    }

    // 获取树的阶数
    pub fn get_order(&self) -> usize {
        self.tree_order
    }

    // 获取文件名
    pub fn get_file_name(&self) -> &str {
        &self.file_name
    }

    // 获取树的可变引用（用于高级操作）
    pub fn get_tree_mut(&mut self) -> IXResult<&mut BPTree> {
        self.ensure_tree_initialized()?;
        Ok(self.tree.as_mut().unwrap())
    }

    // 获取树的不可变引用
    pub fn get_tree(&self) -> IXResult<&BPTree> {
        self.ensure_tree_initialized()?;
        Ok(self.tree.as_ref().unwrap())
    }
}

impl Default for IXHandler {
    fn default() -> Self {
        Self::new()
    }
}

// 别名：IXIndexHandler
#[allow(dead_code)]
pub type IXIndexHandler = IXHandler;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ix_handler_creation() {
        let handler = IXHandler::new();
        assert_eq!(handler.get_order(), 4);
        assert_eq!(handler.get_file_name(), "");
    }

    #[test]
    fn test_ix_handler_with_config() {
        let handler = IXHandler::with_config("test.idx".to_string(), 5);
        assert_eq!(handler.get_order(), 5);
        assert_eq!(handler.get_file_name(), "test.idx");
    }

    #[test]
    fn test_tree_initialization() {
        let mut handler = IXHandler::with_config("test.idx".to_string(), 4);
        
        // 初始时树未初始化
        assert!(handler.get_tree().is_err());
        
        // 初始化树
        assert!(handler.init_tree().is_ok());
        
        // 现在树已初始化
        assert!(handler.get_tree().is_ok());
        
        // 二次初始化应该失败
        assert!(handler.init_tree().is_err());
    }

    #[test]
    fn test_insert_and_delete_operations() {
        let mut handler = IXHandler::with_config("test_ops.idx".to_string(), 3);
        
        // 初始化树
        assert!(handler.init_tree().is_ok());
        
        // 插入条目
        let key1 = vec![1, 2, 3];
        let rid1 = (100u32, 1u16);
        assert!(handler.insert_entry(key1.clone(), rid1).is_ok());
        println!("Inserted entry: key={:?}, rid={:?}", key1, rid1);
        
        // 插入更多条目
        let key2 = vec![5, 6, 7];
        let rid2 = (200u32, 2u16);
        assert!(handler.insert_entry(key2.clone(), rid2).is_ok());
        
        let key3 = vec![3, 4, 5];
        let rid3 = (300u32, 3u16);
        assert!(handler.insert_entry(key3.clone(), rid3).is_ok());
        
        println!("Inserted 3 entries");
        
        // 删除条目
        assert!(handler.delete_entry(key2.clone(), rid2).is_ok());
        println!("Deleted entry: key={:?}", key2);
        
        // 关闭
        assert!(handler.close().is_ok());
    }

    #[test]
    fn test_operations_without_init() {
        let mut handler = IXHandler::new();
        
        // 未初始化时的操作应该失败
        let key = vec![1, 2, 3];
        let rid = (100u32, 1u16);
        
        assert!(handler.insert_entry(key.clone(), rid).is_err());
        assert!(handler.delete_entry(key.clone(), rid).is_err());
        assert!(handler.search_entry(&key).is_err());
        assert!(handler.scan_range(&key, &key).is_err());
        assert!(handler.force_pages().is_err());
    }

    #[test]
    fn test_search_operations() {
        let mut handler = IXHandler::with_config("test_search.idx".to_string(), 4);
        assert!(handler.init_tree().is_ok());
        
        // 插入多个条目
        let test_data = vec![
            (vec![1, 0, 0], (100u32, 0u16)),
            (vec![2, 0, 0], (200u32, 1u16)),
            (vec![3, 0, 0], (300u32, 2u16)),
        ];

        for (key, rid) in &test_data {
            assert!(handler.insert_entry(key.clone(), *rid).is_ok());
        }
        
        // 搜索存在的条目
        let result = handler.search_entry(&vec![1, 0, 0]);
        assert!(result.is_ok());
        let found = result.unwrap();
        assert_eq!(found, Some((100u32, 0u16)));
        println!("✓ Found key [1, 0, 0]: {:?}", found);

        // 搜索不存在的条目
        let result = handler.search_entry(&vec![99, 0, 0]);
        assert!(result.is_ok());
        let found = result.unwrap();
        assert_eq!(found, None);
        println!("✓ Key [99, 0, 0] not found (as expected)");
        
        assert!(handler.close().is_ok());
    }

    #[test]
    fn test_scan_range_operations() {
        let mut handler = IXHandler::with_config("test_scan.idx".to_string(), 4);
        assert!(handler.init_tree().is_ok());
        
        // 插入多个条目
        let test_data = vec![
            (vec![10, 0, 0], (100u32, 0u16)),
            (vec![20, 0, 0], (200u32, 1u16)),
            (vec![30, 0, 0], (300u32, 2u16)),
            (vec![40, 0, 0], (400u32, 3u16)),
            (vec![50, 0, 0], (500u32, 4u16)),
        ];

        for (key, rid) in &test_data {
            assert!(handler.insert_entry(key.clone(), *rid).is_ok());
        }

        println!("Inserted {} entries", test_data.len());
        
        // 范围扫描：[20, 40)
        let results = handler.scan_range(&vec![20, 0, 0], &vec![40, 0, 0]);
        assert!(results.is_ok());
        let results = results.unwrap();
        assert_eq!(results.len(), 2);  // 应该有 key 20, 30
        assert_eq!(results[0], (200u32, 1u16));
        assert_eq!(results[1], (300u32, 2u16));
        println!("✓ Scan range [20, 40) returned {} results", results.len());

        // 范围扫描：[0, 100)（全表）
        let results = handler.scan_range(&vec![0, 0, 0], &vec![255, 255, 255]);
        assert!(results.is_ok());
        let results = results.unwrap();
        assert_eq!(results.len(), 5);
        println!("✓ Full table scan returned {} results", results.len());
        
        assert!(handler.close().is_ok());
    }

    #[test]
    fn test_force_pages() {
        let mut handler = IXHandler::with_config("test_force.idx".to_string(), 4);
        assert!(handler.init_tree().is_ok());
        
        // 插入一些数据
        let key = vec![1, 2, 3];
        let rid = (100u32, 1u16);
        assert!(handler.insert_entry(key, rid).is_ok());
        
        // 调用 force_pages
        assert!(handler.force_pages().is_ok());
        
        assert!(handler.close().is_ok());
    }
}

