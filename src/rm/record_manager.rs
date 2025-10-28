use crate::rm::table_manager::TableManager;
use crate::rm::transaction_logger::TransactionLogger;
use crate::common::types::RID;
use crate::pm::long_data::LongDataPtr;
use std::io::{Read, Write, Cursor};

// 记录格式：
// fixed part: 定长字段数据
// var_ptrs: LongDataPtr 列表（每个 8 字节）
#[derive(Clone, Debug)]
pub struct Record {
    pub fixed_part: Vec<u8>,      // 定长字段数据
    pub var_ptrs: Vec<LongDataPtr>, // 变长字段指针列表
}

impl Record {
    pub fn new(fixed_part: Vec<u8>) -> Self {
        Record {
            fixed_part,
            var_ptrs: Vec::new(),
        }
    }

    pub fn with_var_ptrs(fixed_part: Vec<u8>, var_ptrs: Vec<LongDataPtr>) -> Self {
        Record {
            fixed_part,
            var_ptrs,
        }
    }

    // 序列化为字节流：
    // [fixed_part_size: u32][fixed_part][var_ptr_count: u32][var_ptr1][var_ptr2]...
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        
        // 写入定长部分大小和数据
        let fixed_size = self.fixed_part.len() as u32;
        buf.write_all(&fixed_size.to_le_bytes()).unwrap();
        buf.write_all(&self.fixed_part).unwrap();
        
        // 写入变长指针数量和指针数据
        let ptr_count = self.var_ptrs.len() as u32;
        buf.write_all(&ptr_count.to_le_bytes()).unwrap();
        for ptr in &self.var_ptrs {
            buf.write_all(&ptr.serialize()).unwrap();
        }
        
        buf
    }

    // 从字节流反序列化
    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        let mut cursor = Cursor::new(data);
        
        // 读取定长部分大小和数据
        let mut size_bytes = [0u8; 4];
        cursor.read_exact(&mut size_bytes)
            .map_err(|e| format!("Failed to read fixed part size: {}", e))?;
        let fixed_size = u32::from_le_bytes(size_bytes) as usize;
        
        let mut fixed_part = vec![0u8; fixed_size];
        cursor.read_exact(&mut fixed_part)
            .map_err(|e| format!("Failed to read fixed part: {}", e))?;
        
        // 读取变长指针数量
        let mut count_bytes = [0u8; 4];
        cursor.read_exact(&mut count_bytes)
            .map_err(|e| format!("Failed to read var_ptr count: {}", e))?;
        let ptr_count = u32::from_le_bytes(count_bytes) as usize;
        
        // 读取变长指针
        let mut var_ptrs = Vec::new();
        for _ in 0..ptr_count {
            let mut ptr_bytes = [0u8; 8];
            cursor.read_exact(&mut ptr_bytes)
                .map_err(|e| format!("Failed to read var_ptr: {}", e))?;
            let ptr = LongDataPtr::deserialize(&ptr_bytes)?;
            var_ptrs.push(ptr);
        }
        
        Ok(Record {
            fixed_part,
            var_ptrs,
        })
    }

    // 获取序列化后的大小
    pub fn serialized_size(&self) -> usize {
        4 + self.fixed_part.len() + 4 + (self.var_ptrs.len() * 8)
    }
}

// RecordManager 负责记录级的增删改查和扫描，支持事务和变长数据
pub struct RecordManager {
    // 拥有 TableManager（管理已打开表）
    table_manager: TableManager,
    
    // 事务日志记录器
    logger: TransactionLogger,
    
    // 当前活跃事务 ID（None 表示无事务）
    current_txid: Option<u64>,
}

impl RecordManager {
    pub fn new(table_manager: TableManager, logger: TransactionLogger) -> Self {
        RecordManager {
            table_manager,
            logger,
            current_txid: None,
        }
    }

    // 事务管理

    // 开始一个事务
    pub fn begin_transaction(&mut self) -> Result<u64, String> {
        if self.current_txid.is_some() {
            return Err("Transaction already active".to_string());
        }

        let txid = self.logger.begin_tx()?;
        self.current_txid = Some(txid);
        println!("[TX{}] Transaction started", txid);
        Ok(txid)
    }

    // 提交事务
    pub fn commit_transaction(&mut self) -> Result<(), String> {
        let txid = self.current_txid
            .ok_or("No active transaction".to_string())?;

        // 调用 logger 提交（会自动 flush）
        self.logger.commit(txid)?;
        println!("[TX{}] Transaction committed", txid);
        
        self.current_txid = None;
        Ok(())
    }

    // 回滚事务
    pub fn abort_transaction(&mut self) -> Result<(), String> {
        let txid = self.current_txid
            .ok_or("No active transaction".to_string())?;

        // 调用 logger 回滚（只写 abort 记录，不重放数据操作）
        self.logger.abort(txid)?;
        println!("[TX{}] Transaction aborted", txid);
        
        self.current_txid = None;
        Ok(())
    }

    // 获取当前事务 ID
    pub fn get_current_txid(&self) -> Option<u64> {
        self.current_txid
    }

    // CRUD 操作（支持事务日志）

    // 插入一条记录（指定表）
    // 参数 record 应为 Record 序列化后的字节流
    pub fn insert(&mut self, table: &str, record: &[u8]) -> Result<RID, String> {
        // 自动打开表
        self.table_manager.open_table(table)
            .map_err(|e| format!("Failed to open table '{}': {}", table, e))?;

        // 获取 TableHandler 可变引用
        let th = self.table_manager.get_table_handler_mut(table)
            .ok_or(format!("Failed to get handler for table '{}'", table))?;

        // 调用 TableHandler 的 insert
        let rid = th.insert(record)
            .map_err(|e| format!("Failed to insert record into table '{}': {}", table, e))?;

        // 如果有活跃事务，记录到日志
        if let Some(txid) = self.current_txid {
            self.logger.log_insert(txid, table.to_string(), rid, record.to_vec())?;
            println!("[TX{}] Logged INSERT into table '{}' at RID({}, {})", 
                txid, table, rid.page_id, rid.slot_id);
        }

        Ok(rid)
    }

    // 获取记录
    // 返回 Record 对象，其中变长数据指针已解引用
    pub fn get(&mut self, table: &str, rid: RID) -> Result<Record, String> {
        // 自动打开表
        self.table_manager.open_table(table)
            .map_err(|e| format!("Failed to open table '{}': {}", table, e))?;

        // 获取 TableHandler 可变引用
        let th = self.table_manager.get_table_handler_mut(table)
            .ok_or(format!("Failed to get handler for table '{}'", table))?;

        // 调用 TableHandler 的 get
        let raw_data = th.get(rid)
            .map_err(|e| format!("Failed to get record from table '{}': {}", table, e))?;

        // 反序列化记录
        let record = Record::deserialize(&raw_data)?;

        Ok(record)
    }

    // 获取原始字节数据（不解引用变长数据）
    pub fn get_raw(&mut self, table: &str, rid: RID) -> Result<Vec<u8>, String> {
        // 自动打开表
        self.table_manager.open_table(table)
            .map_err(|e| format!("Failed to open table '{}': {}", table, e))?;

        // 获取 TableHandler 可变引用
        let th = self.table_manager.get_table_handler_mut(table)
            .ok_or(format!("Failed to get handler for table '{}'", table))?;

        // 调用 TableHandler 的 get
        th.get(rid)
            .map_err(|e| format!("Failed to get raw record from table '{}': {}", table, e))
    }

    // 删除记录
    pub fn delete(&mut self, table: &str, rid: RID) -> Result<(), String> {
        // 自动打开表
        self.table_manager.open_table(table)
            .map_err(|e| format!("Failed to open table '{}': {}", table, e))?;

        // 获取 TableHandler 可变引用
        let th = self.table_manager.get_table_handler_mut(table)
            .ok_or(format!("Failed to get handler for table '{}'", table))?;

        // 调用 TableHandler 的 delete
        th.delete(rid)
            .map_err(|e| format!("Failed to delete record from table '{}': {}", table, e))?;

        // 如果有活跃事务，记录到日志
        if let Some(txid) = self.current_txid {
            self.logger.log_delete(txid, table.to_string(), rid)?;
            println!("[TX{}] Logged DELETE from table '{}' at RID({}, {})", 
                txid, table, rid.page_id, rid.slot_id);
        }

        Ok(())
    }

    // 更新记录
    pub fn update(&mut self, table: &str, rid: RID, new_record: &[u8]) -> Result<RID, String> {
        // 自动打开表
        self.table_manager.open_table(table)
            .map_err(|e| format!("Failed to open table '{}': {}", table, e))?;

        // 获取 TableHandler 可变引用
        let th = self.table_manager.get_table_handler_mut(table)
            .ok_or(format!("Failed to get handler for table '{}'", table))?;

        // 如果有活跃事务，先读取旧值用于日志
        let old_record = if self.current_txid.is_some() {
            Some(th.get(rid)
                .map_err(|e| format!("Failed to read old record for logging: {}", e))?)
        } else {
            None
        };

        // 调用 TableHandler 的 update
        let new_rid = th.update(rid, new_record)
            .map_err(|e| format!("Failed to update record in table '{}': {}", table, e))?;

        // 如果有活跃事务，记录 update 到日志（包含新值）
        if let Some(txid) = self.current_txid {
            self.logger.log_update(txid, table.to_string(), rid, new_record.to_vec())?;
            
            if let Some(old) = old_record {
                println!("[TX{}] Logged UPDATE in table '{}' at RID({}, {}): {} bytes -> {} bytes", 
                    txid, table, rid.page_id, rid.slot_id, old.len(), new_record.len());
            }
        }

        Ok(new_rid)
    }

    // 扫描操作

    // 扫描所有记录（简单 Full Table Scan 实现）
    pub fn scan_all<F>(
        &mut self,
        table: &str,
        mut callback: F,
    ) -> Result<(), String>
    where
        F: FnMut(RID, &[u8]) -> bool,
    {
        // 自动打开表
        self.table_manager.open_table(table)
            .map_err(|e| format!("Failed to open table '{}': {}", table, e))?;

        // 获取 TableHandler 可变引用
        let th = self.table_manager.get_table_handler_mut(table)
            .ok_or(format!("Failed to get handler for table '{}'", table))?;

        // 获取所有数据页的克隆副本（避免借用冲突）
        let data_pages = th.get_data_pages().to_vec();

        // 遍历每一页
        for pid in data_pages {
            // 获取该页的所有有效 RID
            let rids = th.list_valid_rids(pid)
                .map_err(|e| format!("Failed to list RIDs for page {}: {}", pid, e))?;

            // 对每个 RID 调用 callback
            for rid in rids {
                // 获取记录数据
                let data = th.get(rid)
                    .map_err(|e| format!("Failed to get record {:?}: {}", rid, e))?;

                // 调用 callback，如果返回 false 则停止扫描
                if !callback(rid, &data) {
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    // 表级操作

    // 创建表
    pub fn create_table(&mut self, schema: crate::rm::types::TableSchema) -> Result<(), String> {
        if self.current_txid.is_some() {
            return Err("Cannot create table inside a transaction".to_string());
        }
        
        self.table_manager.create_table(schema)
            .map_err(|e| format!("Failed to create table: {}", e))
    }

    // 删除表
    pub fn drop_table(&mut self, table: &str) -> Result<(), String> {
        if self.current_txid.is_some() {
            return Err("Cannot drop table inside a transaction".to_string());
        }
        
        self.table_manager.drop_table(table)
            .map_err(|e| format!("Failed to drop table '{}': {}", table, e))
    }

    // 打开表
    pub fn open_table(&mut self, table: &str) -> Result<(), String> {
        self.table_manager.open_table(table)
            .map_err(|e| format!("Failed to open table '{}': {}", table, e))
    }

    // 关闭表
    pub fn close_table(&mut self, table: &str) -> Result<(), String> {
        self.table_manager.close_table(table)
            .map_err(|e| format!("Failed to close table '{}': {}", table, e))
    }

    // 刷新所有表
    pub fn flush_all(&mut self) -> Result<(), String> {
        if self.current_txid.is_some() {
            return Err("Cannot flush inside a transaction".to_string());
        }
        
        self.table_manager.close_all_tables()
            .map_err(|e| format!("Failed to flush all tables: {}", e))
    }

    // 查询方法

    // 检查表是否存在
    pub fn table_exists(&self, table: &str) -> bool {
        self.table_manager.table_exists(table)
    }

    // 获取所有打开的表名
    pub fn get_open_tables(&self) -> Vec<String> {
        self.table_manager.get_open_tables()
    }

    // 检查表是否已打开
    pub fn is_table_open(&self, table: &str) -> bool {
        self.table_manager.is_table_open(table)
    }

    // 获取活跃事务数
    pub fn get_active_txn_count(&self) -> usize {
        self.logger.get_active_txn_count()
    }

    // 变长数据管理

    // 将变长数据写入外部页面链，返回指针

    // 分页存储：每页最多存储 4090 字节数据
    // 超过大小时自动分配新页并链接
    pub fn store_var_data(&mut self, table: &str, data: &[u8]) -> Result<LongDataPtr, String> {
        if data.is_empty() {
            return Err("Cannot store empty data".to_string());
        }

        self.table_manager.open_table(table)
            .map_err(|e| format!("Failed to open table '{}': {}", table, e))?;

        let th = self.table_manager.get_table_handler_mut(table)
            .ok_or(format!("Failed to get handler for table '{}'", table))?;

        let ptr = th.store_var_data(data)
            .map_err(|e| format!("Failed to store variable data: {}", e))?;

        // 如果有活跃事务，记录此操作
        if let Some(txid) = self.current_txid {
            println!("[TX{}] Stored variable data: {} bytes at page {}", 
                txid, ptr.total_length, ptr.first_page_id);
        }

        println!("[VarData] Stored {} bytes, ptr = LongDataPtr(page_id={}, len={})", 
            data.len(), ptr.first_page_id, ptr.total_length);

        Ok(ptr)
    }

    // 从外部页面链读取变长数据
    // 
    // 遍历链表逐页读取数据块，直到读完所有数据
    pub fn load_var_data(&mut self, table: &str, ptr: &LongDataPtr) -> Result<Vec<u8>, String> {
        if ptr.total_length == 0 {
            return Ok(Vec::new());
        }

        self.table_manager.open_table(table)
            .map_err(|e| format!("Failed to open table '{}': {}", table, e))?;

        let th = self.table_manager.get_table_handler_mut(table)
            .ok_or(format!("Failed to get handler for table '{}'", table))?;

        let data = th.load_var_data(ptr)
            .map_err(|e| format!("Failed to load variable data: {}", e))?;

        println!("[VarData] Loaded {} bytes from page {}", 
            data.len(), ptr.first_page_id);

        Ok(data)
    }

    // 删除变长字段时释放页面链

    // 遍历整条链表，将所有页面回收到 free-list
    pub fn release_var_data(&mut self, table: &str, ptr: &LongDataPtr) -> Result<(), String> {
        self.table_manager.open_table(table)
            .map_err(|e| format!("Failed to open table '{}': {}", table, e))?;

        let th = self.table_manager.get_table_handler_mut(table)
            .ok_or(format!("Failed to get handler for table '{}'", table))?;

        th.release_var_data(ptr)
            .map_err(|e| format!("Failed to release variable data: {}", e))?;

        // 如果有活跃事务，记录此操作
        if let Some(txid) = self.current_txid {
            println!("[TX{}] Released variable data: {} bytes from page {}", 
                txid, ptr.total_length, ptr.first_page_id);
        }

        println!("[VarData] Released {} bytes from page {}", 
            ptr.total_length, ptr.first_page_id);

        Ok(())
    }

    // 获取变长数据统计信息
    pub fn get_var_data_stats(&self, table: &str) -> Result<(u32, u32), String> {
        let exists = self.table_manager.table_exists(table);
        if !exists {
            return Err(format!("Table '{}' does not exist", table));
        }
        
        // 这里可以扩展为返回更详细的统计信息
        Ok((0, 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_serialization() {
        // 测试记录序列化和反序列化
        let fixed = vec![1, 2, 3, 4, 5];
        let ptr1 = LongDataPtr::new(100, 1000);
        let ptr2 = LongDataPtr::new(101, 2000);
        
        let record = Record::with_var_ptrs(fixed.clone(), vec![ptr1, ptr2]);
        let serialized = record.serialize();
        
        let deserialized = Record::deserialize(&serialized).unwrap();
        assert_eq!(deserialized.fixed_part, fixed);
        assert_eq!(deserialized.var_ptrs.len(), 2);
        assert_eq!(deserialized.var_ptrs[0], ptr1);
    }

    #[test]
    fn test_var_data_lifecycle() {
        // 测试变长数据的存储、加载、释放流程
        // 这里需要初始化 RecordManager 及其依赖
    }

    #[test]
    fn test_large_var_data() {
        // 测试超过单页大小（4090 字节）的数据分页存储
    }
}
