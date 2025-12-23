// Catalog 管理器，负责数据字典的注册、查询和持久化
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use crate::common::types::TableSchema;
use crate::common::disk_manager::DiskManager;
use std::path::{Path, PathBuf};
use crate::common::types::PAGE_SIZE;

// 内存 + 持久化的 Catalog 管理器
#[derive(Serialize, Deserialize)]
pub struct CatalogManager {
    // 内存缓存：表名 -> schema
    schemas: HashMap<String, TableSchema>,
    
    // 下一个可用的表 ID（自动递增）
    next_table_id: u32,
    
    // Catalog 文件路径（不参与序列化）
    #[serde(skip)]
    catalog_file_path: PathBuf,
}

impl CatalogManager {
    // 创建新的 CatalogManager
    // 
    // # 参数
    // - `catalog_path`: catalog 文件路径（如 "./data/db/catalog.tbl"）
    //   如果为 None，使用默认路径 "data/db/catalog.tbl"
    // 
    // # 返回
    // - `Ok(CatalogManager)`: 成功创建
    // - `Err(String)`: 创建失败
    pub fn new<P: AsRef<Path>>(catalog_path: Option<P>) -> Result<Self, String> {
        let catalog_file_path = match catalog_path {
            Some(p) => p.as_ref().to_path_buf(),
            None => PathBuf::from("data/catalog.tbl"), // 默认路径，用于向后兼容
        };
        
        let mut mgr = CatalogManager {
            schemas: HashMap::new(),
            next_table_id: 1,  // 从 1 开始分配 table_id（0 保留）
            catalog_file_path: catalog_file_path.clone(),
        };
        mgr.load_from_disk()?;
        
        // 计算下一个可用的 table_id
        let max_id = mgr.schemas.values()
            .map(|schema| schema.table_id)
            .max()
            .unwrap_or(0);
        mgr.next_table_id = max_id + 1;
        
        println!("[CatalogManager] Initialized with {} tables, next_table_id={} (path: {:?})", 
            mgr.schemas.len(), mgr.next_table_id, mgr.catalog_file_path);
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
        // 确保目录存在
        if let Some(parent) = self.catalog_file_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| format!("Failed to create catalog directory: {}", e))?;
            }
        }
        
        // 序列化 schemas
        let encoded = bincode::serialize(&self.schemas)
            .map_err(|e| format!("Failed to serialize catalog: {}", e))?;

        // 检查大小是否超过页大小（预留8字节存储长度）
        if encoded.len() + 8 > PAGE_SIZE {
            return Err(format!(
                "Catalog size {} exceeds page size {} (with 8-byte header)",
                encoded.len(),
                PAGE_SIZE
            ));
        }

        // 创建或覆盖文件
        let catalog_file = self.catalog_file_path.to_str()
            .ok_or("Invalid catalog file path")?;
        DiskManager::create_file(catalog_file)
            .map_err(|e| format!("Failed to create catalog file: {}", e))?;

        // 创建页数据：前8字节存储数据长度，后面是实际数据
        let mut page_data = vec![0u8; PAGE_SIZE];
        
        // 写入数据长度（u64, little-endian）
        let len_bytes = (encoded.len() as u64).to_le_bytes();
        page_data[..8].copy_from_slice(&len_bytes);
        
        // 写入实际数据
        page_data[8..8+encoded.len()].copy_from_slice(&encoded);

        // 写入第0页
        DiskManager::write_page(catalog_file, 0, &page_data)
            .map_err(|e| format!("Failed to write catalog to disk: {}", e))?;

        println!("[CatalogManager] Flushed {} tables to disk ({} bytes) at {:?}", 
            self.schemas.len(), encoded.len(), self.catalog_file_path);
        Ok(())
    }

    // 从磁盘加载 catalog 到内存（如果文件不存在则返回 Ok）
    pub fn load_from_disk(&mut self) -> Result<(), String> {
        // 检查文件是否存在
        if !self.catalog_file_path.exists() {
            println!("[CatalogManager] Catalog file not found at {:?}, starting with empty schema", 
                self.catalog_file_path);
            return Ok(());
        }

        // 检查文件大小
        let metadata = std::fs::metadata(&self.catalog_file_path)
            .map_err(|e| format!("Failed to get catalog file metadata: {}", e))?;

        // 如果文件为空，说明是新建的空文件
        if metadata.len() == 0 {
            println!("[CatalogManager] Catalog file is empty, starting with empty schema");
            return Ok(());
        }

        // 读取第0页
        let catalog_file = self.catalog_file_path.to_str()
            .ok_or("Invalid catalog file path")?;
        let mut buffer = vec![0u8; PAGE_SIZE];
        DiskManager::read_page(catalog_file, 0, &mut buffer)
            .map_err(|e| format!("Failed to read catalog from disk: {}", e))?;

        // 检查是否全为 0
        let has_nonzero = buffer.iter().any(|&b| b != 0);
        
        // 如果页面全为 0，表示 catalog 为空（新建的数据库）
        if !has_nonzero {
            println!("[CatalogManager] Catalog page is all zeros, starting with empty schema");
            self.schemas = HashMap::new();
            return Ok(());
        }
        
        // 读取数据长度（前8字节）
        let len_bytes: [u8; 8] = buffer[..8].try_into()
            .map_err(|_| "Failed to read length header")?;
        let data_len = u64::from_le_bytes(len_bytes) as usize;
        
        // 检查长度是否合理
        if data_len == 0 || data_len + 8 > PAGE_SIZE {
            return Err(format!("Invalid catalog data length: {}", data_len));
        }

        // 反序列化数据（跳过前8字节的长度头）
        let bytes = &buffer[8..8+data_len];
        let schemas: HashMap<String, TableSchema> = bincode::deserialize(bytes)
            .map_err(|e| format!("Failed to deserialize catalog: {}", e))?;

        let table_count = schemas.len();
        self.schemas = schemas;
        println!("[CatalogManager] Loaded {} tables from disk at {:?}", 
            table_count, self.catalog_file_path);
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
            catalog_file_path: PathBuf::from("data/catalog.tbl"),
        }
    }
}
