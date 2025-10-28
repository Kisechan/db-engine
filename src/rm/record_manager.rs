use crate::rm::table_manager::TableManager;
use crate::rm::transaction_logger::TransactionLogger;
use crate::common::types::RID;

// RecordManager 负责记录级的增删改查和扫描，支持事务
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
    pub fn get(&mut self, table: &str, rid: RID) -> Result<Vec<u8>, String> {
        // 自动打开表
        self.table_manager.open_table(table)
            .map_err(|e| format!("Failed to open table '{}': {}", table, e))?;

        // 获取 TableHandler 可变引用
        let th = self.table_manager.get_table_handler_mut(table)
            .ok_or(format!("Failed to get handler for table '{}'", table))?;

        // 调用 TableHandler 的 get
        th.get(rid)
            .map_err(|e| format!("Failed to get record from table '{}': {}", table, e))
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
}
