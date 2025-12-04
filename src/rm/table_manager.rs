use std::collections::HashMap;
use std::path::PathBuf;
use crate::rm::types::TableSchema;
use crate::rm::catalog_manager::CatalogManager;
use crate::rm::table_handler::TableHandler;
use crate::fm::file_manager::FileManager;
use crate::fm::file_handler::FileHandler;
use crate::fm::file_header::FileHeader;
use crate::common::disk_manager::DiskManager;

// 管理所有打开的表
pub struct TableManager {
    // catalog 管理器（持有或引用）
    pub catalog: CatalogManager,

    // 已打开表：表名 -> TableHandler
    pub open_tables: HashMap<String, TableHandler>,
    
    // 数据库路径
    pub db_path: PathBuf,
}

impl TableManager {
    pub fn new(catalog: CatalogManager, db_path: PathBuf) -> Result<Self, String> {
        Ok(TableManager {
            catalog,
            open_tables: HashMap::new(),
            db_path,
        })
    }

    // 创建表：注册 schema 并创建数据文件
    pub fn create_table(&mut self, schema: TableSchema) -> Result<(), String> {
        let table_name = schema.table_name.clone();

        // 检查表是否已存在
        if self.catalog.table_exists(&table_name) {
            return Err(format!("Table '{}' already exists", table_name));
        }

        // 向 catalog 注册 schema
        self.catalog.create_table(schema)?;

        // 创建数据文件（使用数据库路径）
        let file_path = self.db_path.join(format!("{}.tbl", table_name));
        let file_path_str = file_path.to_str()
            .ok_or_else(|| "Invalid file path".to_string())?;
        DiskManager::create_file(file_path_str)
            .map_err(|e| format!("Failed to create data file for table '{}': {}", table_name, e))?;

        // 初始化 FileHeader
        let mut default_header = FileHeader::default();
        
        // 创建文件句柄
        let file_handler = FileHandler::new(file_path_str.to_string(), default_header);
        
        // 将文件头写入第0页（非常重要！）
        file_handler.flush_header()
            .map_err(|e| format!("Failed to flush header for table '{}': {}", table_name, e))?;

        println!("[TableManager] Created table file: {} with initialized header", file_path_str);

        Ok(())
    }

    // 打开表（返回一个可操作的 TableHandler）
    pub fn open_table(&mut self, table_name: &str) -> Result<(), String> {
        // 检查表是否已打开
        if self.open_tables.contains_key(table_name) {
            return Ok(());
        }

        // 检查表是否在 catalog 中存在
        let schema = self.catalog.get_table(table_name)
            .ok_or(format!("Table '{}' not found in catalog", table_name))?
            .clone();

        // 构造数据文件路径（使用数据库路径）
        let file_path = self.db_path.join(format!("{}.tbl", table_name));
        let file_path_str = file_path.to_str()
            .ok_or_else(|| "Invalid file path".to_string())?;

        // 检查文件是否存在
        if !DiskManager::file_exists(file_path_str) {
            return Err(format!("Data file for table '{}' not found at {}", table_name, file_path_str));
        }

        // 加载文件头
        let file_header = FileHandler::load_header(file_path_str)
            .map_err(|e| format!("Failed to load header for table '{}': {}", table_name, e))?;

        // 创建 FileHandler（使用字符串路径）
        let file_handler = FileHandler::new(file_path_str.to_string(), file_header);

        // 创建 TableHandler 并加入 open_tables（传入完整路径）
        let table_handler = TableHandler::new(table_name.to_string(), schema, file_handler, file_path_str.to_string());
        self.open_tables.insert(table_name.to_string(), table_handler);

        println!("[TableManager] Opened table: {}", table_name);

        Ok(())
    }

    // 关闭表（flush + remove）
    pub fn close_table(&mut self, table_name: &str) -> Result<(), String> {
        if let Some(mut th) = self.open_tables.remove(table_name) {
            th.flush()
                .map_err(|e| format!("Failed to flush table '{}': {}", table_name, e))?;
            println!("[TableManager] Closed table: {}", table_name);
        }
        Ok(())
    }

    // 获取可变的 TableHandler 引用（必须先 open_table）
    pub fn get_table_handler_mut(&mut self, table_name: &str) -> Option<&mut TableHandler> {
        self.open_tables.get_mut(table_name)
    }

    // 获取不可变的 TableHandler 引用
    pub fn get_table_handler(&self, table_name: &str) -> Option<&TableHandler> {
        self.open_tables.get(table_name)
    }

    // 获取所有已打开的表名
    pub fn get_open_tables(&self) -> Vec<String> {
        self.open_tables.keys().cloned().collect()
    }

    // 删除表（从 catalog 和磁盘）
    pub fn drop_table(&mut self, table_name: &str) -> Result<(), String> {
        // 检查表是否已打开，如果已打开则先关闭
        if self.open_tables.contains_key(table_name) {
            self.close_table(table_name)?;
        }

        // 从 catalog 移除
        self.catalog.drop_table(table_name)?;

        // 删除数据文件
        let file_path = format!("data/{}.tbl", table_name);
        FileManager::delete_file(&file_path)
            .map_err(|e| format!("Failed to delete data file for table '{}': {}", table_name, e))?;

        println!("[TableManager] Dropped table: {}", table_name);

        Ok(())
    }

    // 检查表是否已打开
    pub fn is_table_open(&self, table_name: &str) -> bool {
        self.open_tables.contains_key(table_name)
    }

    // 检查表是否存在
    pub fn table_exists(&self, table_name: &str) -> bool {
        self.catalog.table_exists(table_name)
    }

    // 刷新并关闭所有打开的表
    pub fn close_all_tables(&mut self) -> Result<(), String> {
        let open_names: Vec<String> = self.get_open_tables();
        for table_name in open_names {
            self.close_table(&table_name)?;
        }
        Ok(())
    }
}
