// 数据库管理器，负责多个数据库的生命周期管理
//
// # 功能概述
//
// DatabaseManager 提供了完整的多数据库管理功能：
// - 创建和删除数据库
// - 切换当前数据库
// - 列出所有数据库和表
// - 管理数据库上下文（CatalogManager, TableManager, IXManager）
//
// # 目录结构
//
// ```text
// ./data/
//   ├─ db1/
//   │  ├─ catalog.tbl       # 数据字典
//   │  ├─ table1.tbl        # 表数据文件
//   │  └─ table1.idx0       # 索引文件
//   └─ db2/
//      └─ ...
// ```
//
// # 使用示例
//
// ```rust,ignore
// use db_engine::rm::DatabaseManager;
//
// // 创建数据库管理器
// let mut db_mgr = DatabaseManager::new("./data")?;
//
// // 创建数据库
// db_mgr.create_database("KisechansDB", false)?;
//
// // 列出所有数据库
// let databases = db_mgr.list_databases()?;
// println!("Databases: {:?}", databases);
//
// // 切换到数据库（注意：当前版本需要 CatalogManager 支持自定义路径）
// // db_mgr.use_database("KisechansDB")?;
//
// // 删除数据库
// db_mgr.drop_database("KisechansDB", false)?;
//
// // 关闭所有数据库
// db_mgr.close_all()?;
// ```
//
// # 限制和 TODO
//
// - ! CatalogManager 当前使用硬编码路径 "data/catalog.tbl"
// - TODO: 重构 CatalogManager 以支持自定义数据库路径
// - TODO: 实现数据库切换时的路径管理
// - TODO: 添加数据库备份和恢复功能
//
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use crate::rm::catalog_manager::CatalogManager;
use crate::rm::table_manager::TableManager;
use crate::ix::ix_manager::IXManager;

// 数据库上下文，包含单个数据库的所有管理器
pub struct DatabaseContext {
    pub name: String,
    pub path: PathBuf,
    pub catalog: CatalogManager,
    pub table_manager: TableManager,
    pub ix_manager: IXManager,
}

// 数据库管理错误类型
#[derive(Debug)]
pub enum DatabaseError {
    DatabaseNotFound(String),
    DatabaseAlreadyExists(String),
    NoDatabaseSelected,
    IOError(String),
    InitializationError(String),
}

impl std::fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DatabaseError::DatabaseNotFound(name) => 
                write!(f, "Database '{}' does not exist", name),
            DatabaseError::DatabaseAlreadyExists(name) => 
                write!(f, "Database '{}' already exists", name),
            DatabaseError::NoDatabaseSelected => 
                write!(f, "No database selected. Use 'USE database_name' first"),
            DatabaseError::IOError(msg) => 
                write!(f, "I/O error: {}", msg),
            DatabaseError::InitializationError(msg) => 
                write!(f, "Initialization error: {}", msg),
        }
    }
}

impl std::error::Error for DatabaseError {}

impl From<std::io::Error> for DatabaseError {
    fn from(err: std::io::Error) -> Self {
        DatabaseError::IOError(err.to_string())
    }
}

// 数据库管理器，管理多个数据库
pub struct DatabaseManager {
    // 数据库根目录（如 "./data"）
    base_path: PathBuf,
    
    // 所有数据库的上下文（懒加载，只在使用时加载）
    databases: HashMap<String, DatabaseContext>,
    
    // 当前激活的数据库名称
    current_database: Option<String>,
}

impl DatabaseManager {
    // 创建新的数据库管理器
    // 
    // # 参数
    // - `base_path`: 数据库根目录路径
    // 
    // # 返回
    // - `Ok(DatabaseManager)`: 成功创建
    // - `Err(DatabaseError)`: 创建失败
    pub fn new<P: AsRef<Path>>(base_path: P) -> Result<Self, DatabaseError> {
        let base_path = base_path.as_ref().to_path_buf();
        
        // 确保根目录存在
        if !base_path.exists() {
            fs::create_dir_all(&base_path)?;
            println!("[DatabaseManager] Created base directory: {:?}", base_path);
        }
        
        let mut manager = DatabaseManager {
            base_path,
            databases: HashMap::new(),
            current_database: None,
        };
        
        // 扫描已存在的数据库（但不加载它们）
        let db_names = manager.scan_databases()?;
        println!("[DatabaseManager] Found {} existing database(s): {:?}", 
            db_names.len(), db_names);
        
        Ok(manager)
    }
    
    // 扫描 base_path 下的所有数据库目录
    fn scan_databases(&self) -> Result<Vec<String>, DatabaseError> {
        let mut databases = Vec::new();
        
        if let Ok(entries) = fs::read_dir(&self.base_path) {
            for entry in entries.flatten() {
                if let Ok(file_type) = entry.file_type() {
                    if file_type.is_dir() {
                        if let Some(name) = entry.file_name().to_str() {
                            // 检查是否是有效的数据库目录（包含 catalog 文件）
                            let catalog_path = entry.path().join("catalog.tbl");
                            if catalog_path.exists() || name != "." && name != ".." {
                                databases.push(name.to_string());
                            }
                        }
                    }
                }
            }
        }
        
        Ok(databases)
    }
    
    // 创建新数据库
    // 
    // # 参数
    // - `name`: 数据库名称
    // - `if_not_exists`: 如果为 true，数据库已存在时不报错
    // 
    // # 返回
    // - `Ok(())`: 创建成功
    // - `Err(DatabaseError)`: 创建失败
    pub fn create_database(&mut self, name: &str, if_not_exists: bool) -> Result<(), DatabaseError> {
        let db_path = self.base_path.join(name);
        
        // 检查数据库是否已存在
        if db_path.exists() {
            if if_not_exists {
                println!("[DatabaseManager] Database '{}' already exists, skipping", name);
                return Ok(());
            } else {
                return Err(DatabaseError::DatabaseAlreadyExists(name.to_string()));
            }
        }
        
        // 创建数据库目录
        fs::create_dir_all(&db_path)?;
        println!("[DatabaseManager] Created database directory: {:?}", db_path);
        
        // 创建空的 catalog 文件
        let catalog_path = db_path.join("catalog.tbl");
        let empty_data = vec![0u8; 4096]; // 一页空数据
        fs::write(&catalog_path, &empty_data)?;
        println!("[DatabaseManager] Created empty catalog file: {:?}", catalog_path);
        
        println!("[DatabaseManager] Database '{}' created successfully", name);
        Ok(())
    }
    
    // 删除数据库
    // 
    // # 参数
    // - `name`: 数据库名称
    // - `if_exists`: 如果为 true，数据库不存在时不报错
    // 
    // # 返回
    // - `Ok(())`: 删除成功
    // - `Err(DatabaseError)`: 删除失败
    pub fn drop_database(&mut self, name: &str, if_exists: bool) -> Result<(), DatabaseError> {
        let db_path = self.base_path.join(name);
        
        // 检查数据库是否存在
        if !db_path.exists() {
            if if_exists {
                println!("[DatabaseManager] Database '{}' does not exist, skipping", name);
                return Ok(());
            } else {
                return Err(DatabaseError::DatabaseNotFound(name.to_string()));
            }
        }
        
        // 如果是当前数据库，先取消选择
        if self.current_database.as_deref() == Some(name) {
            self.current_database = None;
        }
        
        // 从内存中移除
        self.databases.remove(name);
        
        // 删除数据库目录及其所有内容
        fs::remove_dir_all(&db_path)?;
        println!("[DatabaseManager] Database '{}' dropped successfully", name);
        
        Ok(())
    }
    
    // 切换到指定数据库（懒加载）
    // 
    // # 参数
    // - `name`: 数据库名称
    // 
    // # 返回
    // - `Ok(())`: 切换成功
    // - `Err(DatabaseError)`: 切换失败
    pub fn use_database(&mut self, name: &str) -> Result<(), DatabaseError> {
        let db_path = self.base_path.join(name);
        
        // 检查数据库是否存在
        if !db_path.exists() {
            return Err(DatabaseError::DatabaseNotFound(name.to_string()));
        }
        
        // 如果数据库尚未加载，加载它
        if !self.databases.contains_key(name) {
            let context = self.load_database(name)?;
            self.databases.insert(name.to_string(), context);
        }
        
        // 设置为当前数据库
        self.current_database = Some(name.to_string());
        println!("[DatabaseManager] Switched to database '{}'", name);
        
        Ok(())
    }
    
    // 加载数据库上下文
    fn load_database(&self, name: &str) -> Result<DatabaseContext, DatabaseError> {
        let db_path = self.base_path.join(name);
        
        println!("[DatabaseManager] Loading database '{}' from {:?}", name, db_path);
        
        // 为每个数据库创建独立的 CatalogManager，传入数据库的 catalog 路径
        let catalog_path = db_path.join("catalog.tbl");
        let catalog_for_context = CatalogManager::new(Some(&catalog_path))
            .map_err(|e| DatabaseError::InitializationError(
                format!("Failed to initialize catalog: {}", e)
            ))?;
        
        let catalog_for_table_mgr = CatalogManager::new(Some(&catalog_path))
            .map_err(|e| DatabaseError::InitializationError(
                format!("Failed to initialize catalog: {}", e)
            ))?;
        
        // 创建 TableManager
        let table_manager = TableManager::new(catalog_for_table_mgr)
            .map_err(|e| DatabaseError::InitializationError(
                format!("Failed to initialize table manager: {}", e)
            ))?;
        
        // 创建 IXManager
        let ix_manager = IXManager::new();
        
        Ok(DatabaseContext {
            name: name.to_string(),
            path: db_path,
            catalog: catalog_for_context,
            table_manager,
            ix_manager,
        })
    }
    
    // 列出所有数据库
    // 
    // # 返回
    // - 所有数据库名称的向量
    pub fn list_databases(&self) -> Result<Vec<String>, DatabaseError> {
        self.scan_databases()
    }
    
    // 列出当前数据库的所有表
    // 
    // # 返回
    // - `Ok(Vec<String>)`: 表名列表
    // - `Err(DatabaseError)`: 没有选择数据库
    pub fn list_tables(&self) -> Result<Vec<String>, DatabaseError> {
        let context = self.current_context()?;
        Ok(context.catalog.get_all_tables())
    }
    
    // 获取当前数据库上下文的不可变引用
    // 
    // # 返回
    // - `Ok(&DatabaseContext)`: 当前数据库上下文
    // - `Err(DatabaseError)`: 没有选择数据库
    pub fn current_context(&self) -> Result<&DatabaseContext, DatabaseError> {
        match &self.current_database {
            Some(name) => {
                self.databases.get(name)
                    .ok_or_else(|| DatabaseError::NoDatabaseSelected)
            }
            None => Err(DatabaseError::NoDatabaseSelected),
        }
    }
    
    // 获取当前数据库上下文的可变引用
    // 
    // # 返回
    // - `Ok(&mut DatabaseContext)`: 当前数据库上下文
    // - `Err(DatabaseError)`: 没有选择数据库
    pub fn current_context_mut(&mut self) -> Result<&mut DatabaseContext, DatabaseError> {
        match &self.current_database {
            Some(name) => {
                let name = name.clone(); // 避免借用问题
                self.databases.get_mut(&name)
                    .ok_or_else(|| DatabaseError::NoDatabaseSelected)
            }
            None => Err(DatabaseError::NoDatabaseSelected),
        }
    }
    
    // 获取当前数据库名称
    // 
    // # 返回
    // - `Some(&str)`: 当前数据库名称
    // - `None`: 没有选择数据库
    pub fn current_database_name(&self) -> Option<&str> {
        self.current_database.as_deref()
    }
    
    // 关闭所有数据库，刷新缓冲区
    // 
    // # 返回
    // - `Ok(())`: 关闭成功
    // - `Err(DatabaseError)`: 关闭失败
    pub fn close_all(&mut self) -> Result<(), DatabaseError> {
        println!("[DatabaseManager] Closing all databases...");
        
        // 刷新所有数据库的 catalog
        for (name, context) in &self.databases {
            if let Err(e) = context.catalog.flush_to_disk() {
                eprintln!("[DatabaseManager] Failed to flush catalog for database '{}': {}", name, e);
            }
        }
        
        // 清空所有数据库上下文
        self.databases.clear();
        self.current_database = None;
        
        println!("[DatabaseManager] All databases closed");
        Ok(())
    }
}

impl Drop for DatabaseManager {
    fn drop(&mut self) {
        // 确保在析构时关闭所有数据库
        let _ = self.close_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    
    #[test]
    fn test_create_database_manager() {
        let test_path = "./test_data/db_manager_test";
        
        // 清理测试目录
        let _ = fs::remove_dir_all(test_path);
        
        let manager = DatabaseManager::new(test_path);
        assert!(manager.is_ok());
        
        // 清理
        let _ = fs::remove_dir_all(test_path);
    }
    
    #[test]
    fn test_create_and_drop_database() {
        let test_path = "./test_data/db_create_drop";
        let _ = fs::remove_dir_all(test_path);
        
        let mut manager = DatabaseManager::new(test_path).unwrap();
        
        // 创建数据库
        assert!(manager.create_database("testdb", false).is_ok());
        
        // 验证数据库存在
        let db_path = Path::new(test_path).join("testdb");
        assert!(db_path.exists());
        
        // 删除数据库
        assert!(manager.drop_database("testdb", false).is_ok());
        assert!(!db_path.exists());
        
        // 清理
        let _ = fs::remove_dir_all(test_path);
    }
    
    #[test]
    fn test_create_database_if_not_exists() {
        let test_path = "./test_data/db_if_not_exists";
        let _ = fs::remove_dir_all(test_path);
        
        let mut manager = DatabaseManager::new(test_path).unwrap();
        
        // 第一次创建
        assert!(manager.create_database("testdb", false).is_ok());
        
        // 第二次创建，不使用 if_not_exists，应该失败
        assert!(manager.create_database("testdb", false).is_err());
        
        // 第三次创建，使用 if_not_exists，应该成功
        assert!(manager.create_database("testdb", true).is_ok());
        
        // 清理
        let _ = fs::remove_dir_all(test_path);
    }
    
    #[test]
    fn test_list_databases() {
        let test_path = "./test_data/db_list";
        let _ = fs::remove_dir_all(test_path);
        
        let mut manager = DatabaseManager::new(test_path).unwrap();
        
        // 创建多个数据库
        manager.create_database("db1", false).unwrap();
        manager.create_database("db2", false).unwrap();
        manager.create_database("db3", false).unwrap();
        
        // 列出数据库
        let databases = manager.list_databases().unwrap();
        assert_eq!(databases.len(), 3);
        assert!(databases.contains(&"db1".to_string()));
        assert!(databases.contains(&"db2".to_string()));
        assert!(databases.contains(&"db3".to_string()));
        
        // 清理
        let _ = fs::remove_dir_all(test_path);
    }
    
    #[test]
    fn test_use_database() {
        let test_path = "./test_data/db_use";
        let _ = fs::remove_dir_all(test_path);
        
        let mut manager = DatabaseManager::new(test_path).unwrap();
        
        // 创建数据库
        manager.create_database("KisechansDB", false).unwrap();
        
        // 切换数据库
        match manager.use_database("KisechansDB") {
            Ok(_) => {
                assert_eq!(manager.current_database_name(), Some("KisechansDB"));
            }
            Err(e) => {
                panic!("Failed to use database: {}", e);
            }
        }
        
        // 切换到不存在的数据库
        assert!(manager.use_database("nonexistent").is_err());
        
        // 清理
        let _ = fs::remove_dir_all(test_path);
    }
}
