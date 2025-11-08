// Catalog 管理器，负责数据字典的注册、查询和持久化
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use crate::rm::types::TableSchema;
use crate::common::disk_manager::DiskManager;

// Catalog 持久化文件名
const CATALOG_FILE: &str = "data/catalog.tbl";
const PAGE_SIZE: usize = 4096;

// 内存 + 持久化的 Catalog 管理器
#[derive(Serialize, Deserialize)]
pub struct CatalogManager {
    // 内存缓存：表名 -> schema
    schemas: HashMap<String, TableSchema>,
    
    // 下一个可用的表 ID（自动递增）
    next_table_id: u32,
}

impl CatalogManager {
    // 创建新的 CatalogManager（会尝试从磁盘加载 catalog）
    pub fn new() -> Result<Self, String> {
        let mut mgr = CatalogManager {
            schemas: HashMap::new(),
            next_table_id: 1,  // 从 1 开始分配 table_id（0 保留）
        };
        mgr.load_from_disk()?;
        
        // 计算下一个可用的 table_id
        let max_id = mgr.schemas.values()
            .map(|schema| schema.table_id)
            .max()
            .unwrap_or(0);
        mgr.next_table_id = max_id + 1;
        
        println!("[CatalogManager] Initialized with {} tables, next_table_id={}", 
            mgr.schemas.len(), mgr.next_table_id);
        Ok(mgr)
    }

    // 在内存中注册并持久化表模式
    pub fn create_table(&mut self, mut schema: TableSchema) -> Result<(), String> {
        let name = schema.table_name.clone();
        if self.schemas.contains_key(&name) {
            return Err(format!("Table '{}' already exists", name));
        }
        
        // 自动分配 table_id
        schema.table_id = self.next_table_id;
        self.next_table_id += 1;
        
        // 初始化时间戳
        let now = TableSchema::current_timestamp();
        schema.create_time = now;
        schema.last_modified = now;
        schema.row_count = 0;
        
        self.schemas.insert(name.clone(), schema);
        println!("[CatalogManager] Added table '{}' to memory cache with table_id={}", 
            name, self.schemas[&name].table_id);
        self.flush_to_disk()
    }

    // 删除表 schema（注意：不负责删除数据文件）
    pub fn drop_table(&mut self, table_name: &str) -> Result<(), String> {
        if self.schemas.remove(table_name).is_none() {
            return Err(format!("Table '{}' not found", table_name));
        }
        println!("[CatalogManager] Removed table '{}' from memory cache", table_name);
        self.flush_to_disk()
    }

    // 获取表 schema（内存缓存）返回不可变引用
    pub fn get_table(&self, table_name: &str) -> Option<&TableSchema> {
        self.schemas.get(table_name)
    }

    // 获取表 schema 并 clone 方便外部使用
    pub fn get_table_schema(&self, table_name: &str) -> Result<TableSchema, String> {
        self.schemas
            .get(table_name)
            .cloned()
            .ok_or(format!("Table '{}' not found in catalog", table_name))
    }

    // 将内存中的 catalog 序列化并写盘（覆盖）
    pub fn flush_to_disk(&self) -> Result<(), String> {
        // 序列化 schemas
        let encoded = bincode::serialize(&self.schemas)
            .map_err(|e| format!("Failed to serialize catalog: {}", e))?;

        // 检查大小是否超过页大小
        if encoded.len() > PAGE_SIZE {
            return Err(format!(
                "Catalog size {} exceeds page size {}",
                encoded.len(),
                PAGE_SIZE
            ));
        }

        // 创建或覆盖文件
        DiskManager::create_file(CATALOG_FILE)
            .map_err(|e| format!("Failed to create catalog file: {}", e))?;

        // 将序列化数据填充到一整页
        let mut page_data = vec![0u8; PAGE_SIZE];
        page_data[..encoded.len()].copy_from_slice(&encoded);

        // 写入第0页
        DiskManager::write_page(CATALOG_FILE, 0, &page_data)
            .map_err(|e| format!("Failed to write catalog to disk: {}", e))?;

        println!("[CatalogManager] Flushed {} tables to disk ({} bytes)", 
            self.schemas.len(), encoded.len());
        Ok(())
    }

    // 从磁盘加载 catalog 到内存（如果文件不存在则返回 Ok）
    pub fn load_from_disk(&mut self) -> Result<(), String> {
        // 检查文件是否存在
        if !std::path::Path::new(CATALOG_FILE).exists() {
            println!("[CatalogManager] Catalog file not found, starting with empty schema");
            return Ok(());
        }

        // 检查文件大小
        let metadata = std::fs::metadata(CATALOG_FILE)
            .map_err(|e| format!("Failed to get catalog file metadata: {}", e))?;

        if metadata.len() == 0 {
            println!("[CatalogManager] Catalog file is empty, starting with empty schema");
            return Ok(());
        }

        // 读取第0页
        let mut buffer = vec![0u8; PAGE_SIZE];
        DiskManager::read_page(CATALOG_FILE, 0, &mut buffer)
            .map_err(|e| format!("Failed to read catalog from disk: {}", e))?;

        // 找到真实数据长度（去除尾部 0 字节）
        let end = buffer.iter().rposition(|&b| b != 0).unwrap_or(0) + 1;

        // 如果页面全为 0，表示 catalog 为空
        if end == 0 {
            println!("[CatalogManager] Catalog page is empty, starting with empty schema");
            self.schemas = HashMap::new();
            return Ok(());
        }

        // 反序列化数据
        let bytes = &buffer[..end];
        let schemas: HashMap<String, TableSchema> = bincode::deserialize(bytes)
            .map_err(|e| format!("Failed to deserialize catalog: {}", e))?;

        let table_count = schemas.len();
        self.schemas = schemas;
        println!("[CatalogManager] Loaded {} tables from disk", table_count);
        Ok(())
    }

    // 获取所有表名
    pub fn get_all_tables(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    // 检查表是否存在
    pub fn table_exists(&self, table_name: &str) -> bool {
        self.schemas.contains_key(table_name)
    }
}

impl Default for CatalogManager {
    fn default() -> Self {
        CatalogManager {
            schemas: HashMap::new(),
            next_table_id: 1,
        }
    }
}
