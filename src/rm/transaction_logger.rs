use serde::{Serialize, Deserialize};
use std::fs::{OpenOptions, File};
use std::io::{Write, Read, BufReader};
use std::collections::HashMap;
use crate::common::types::RID;

// 日志条目类型
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum LogRecord {
    Begin { txid: u64 },
    Insert { txid: u64, table: String, rid: RID, data: Vec<u8> },
    Delete { txid: u64, table: String, rid: RID },
    Update { txid: u64, table: String, rid: RID, new_data: Vec<u8> },
    Commit { txid: u64 },
    Abort { txid: u64 },
}

// 日志文件名固定
const LOG_FILE: &str = "data/db.log";

// 日志缓存管理器
pub struct TransactionLogger {
    // WAL 内存缓存
    pub buffer: Vec<LogRecord>,
    // 回写阈值
    pub max_buffer_size: usize,
    // 事务 ID 自增计数器
    next_txid: u64,
    // 活跃事务集合（用于恢复判断）
    active_txns: HashMap<u64, bool>, // txid -> is_committed
}

impl TransactionLogger {
    pub fn new(max_buffer: usize) -> Self {
        TransactionLogger {
            buffer: Vec::new(),
            max_buffer_size: max_buffer,
            next_txid: 1,
            active_txns: HashMap::new(),
        }
    }

    // 事务管理

    // 开始一个事务，返回 txid
    pub fn begin_tx(&mut self) -> Result<u64, String> {
        let txid = self.next_txid;
        self.next_txid += 1;

        // 记录 Begin 日志
        let record = LogRecord::Begin { txid };
        self.append(record)?;

        // 标记为活跃（未 commit）
        self.active_txns.insert(txid, false);

        Ok(txid)
    }

    // 提交事务
    pub fn commit(&mut self, txid: u64) -> Result<(), String> {
        // 检查事务是否存在
        if !self.active_txns.contains_key(&txid) {
            return Err(format!("Transaction {} not found", txid));
        }

        // 追加 Commit 记录
        let record = LogRecord::Commit { txid };
        self.append(record)?;

        // 标记为已提交
        self.active_txns.insert(txid, true);

        // WAL 原则：commit 必须立即 flush
        self.flush()?;

        Ok(())
    }

    // 回滚事务
    pub fn abort(&mut self, txid: u64) -> Result<(), String> {
        // 检查事务是否存在
        if !self.active_txns.contains_key(&txid) {
            return Err(format!("Transaction {} not found", txid));
        }

        // 追加 Abort 记录
        let record = LogRecord::Abort { txid };
        self.append(record)?;

        // 标记为已回滚
        self.active_txns.remove(&txid);

        // flush（保证 abort 也被记录）
        self.flush()?;

        Ok(())
    }

    // 日志记录

    // 记录插入操作
    pub fn log_insert(&mut self, txid: u64, table: String, rid: RID, data: Vec<u8>) -> Result<(), String> {
        if !self.active_txns.contains_key(&txid) {
            return Err(format!("Transaction {} not active", txid));
        }

        let record = LogRecord::Insert { txid, table, rid, data };
        self.append(record)
    }

    // 记录删除操作
    pub fn log_delete(&mut self, txid: u64, table: String, rid: RID) -> Result<(), String> {
        if !self.active_txns.contains_key(&txid) {
            return Err(format!("Transaction {} not active", txid));
        }

        let record = LogRecord::Delete { txid, table, rid };
        self.append(record)
    }

    // 记录更新操作
    pub fn log_update(&mut self, txid: u64, table: String, rid: RID, new_data: Vec<u8>) -> Result<(), String> {
        if !self.active_txns.contains_key(&txid) {
            return Err(format!("Transaction {} not active", txid));
        }

        let record = LogRecord::Update { txid, table, rid, new_data };
        self.append(record)
    }

    // 缓冲管理

    pub fn append(&mut self, record: LogRecord) -> Result<(), String> {
        self.buffer.push(record);
        if self.buffer.len() >= self.max_buffer_size {
            self.flush()?;
        }
        Ok(())
    }

    // flush：真正写入磁盘（WAL 原则）
    pub fn flush(&mut self) -> Result<(), String> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        for record in &self.buffer {
            let encoded = bincode::serialize(record)
                .map_err(|e| format!("Log serialize error: {}", e))?;

            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(LOG_FILE)
                .map_err(|e| format!("Failed to open log file: {}", e))?;

            // 记录长度前缀（4 字节）+ 数据
            let len = encoded.len() as u32;
            file.write_all(&len.to_le_bytes())
                .map_err(|e| format!("Failed to write log length: {}", e))?;

            file.write_all(&encoded)
                .map_err(|e| format!("Failed to write log record: {}", e))?;
        }

        self.buffer.clear();
        Ok(())
    }

    // 恢复

    // 崩溃恢复：读取日志并恢复已提交的事务
    pub fn crash_recovery(&self) -> Result<Vec<LogRecord>, String> {
        // 检查日志文件是否存在
        if !std::path::Path::new(LOG_FILE).exists() {
            println!("[Recovery] No log file found, skipping recovery");
            return Ok(Vec::new());
        }

        println!("[Recovery] Starting crash recovery from {}", LOG_FILE);

        let file = File::open(LOG_FILE)
            .map_err(|e| format!("Failed to open log file: {}", e))?;

        let mut reader = BufReader::new(file);
        let mut committed_records = Vec::new();
        let mut committed_txns = std::collections::HashSet::new();

        // 第一遍扫描：找出所有已提交的事务
        loop {
            let mut len_bytes = [0u8; 4];
            match reader.read_exact(&mut len_bytes) {
                Ok(_) => {},
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(format!("Failed to read log length: {}", e)),
            }

            let len = u32::from_le_bytes(len_bytes) as usize;
            let mut data = vec![0u8; len];
            reader.read_exact(&mut data)
                .map_err(|e| format!("Failed to read log record: {}", e))?;

            let record: LogRecord = bincode::deserialize(&data)
                .map_err(|e| format!("Failed to deserialize log record: {}", e))?;

            // 记录已提交的事务
            if let LogRecord::Commit { txid } = record {
                committed_txns.insert(txid);
            }
        }

        // 第二遍扫描：收集已提交事务的所有操作
        let file = File::open(LOG_FILE)
            .map_err(|e| format!("Failed to open log file for second pass: {}", e))?;
        let mut reader = BufReader::new(file);

        loop {
            let mut len_bytes = [0u8; 4];
            match reader.read_exact(&mut len_bytes) {
                Ok(_) => {},
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(format!("Failed to read log length: {}", e)),
            }

            let len = u32::from_le_bytes(len_bytes) as usize;
            let mut data = vec![0u8; len];
            reader.read_exact(&mut data)
                .map_err(|e| format!("Failed to read log record: {}", e))?;

            let record: LogRecord = bincode::deserialize(&data)
                .map_err(|e| format!("Failed to deserialize log record: {}", e))?;

            // 只恢复已提交事务的操作
            let txid = match &record {
                LogRecord::Begin { txid } => *txid,
                LogRecord::Insert { txid, .. } => *txid,
                LogRecord::Delete { txid, .. } => *txid,
                LogRecord::Update { txid, .. } => *txid,
                LogRecord::Commit { txid } => *txid,
                LogRecord::Abort { txid } => *txid,
            };

            if committed_txns.contains(&txid) || matches!(record, LogRecord::Begin { .. }) {
                println!("[Recovery] Recovering record: {:?}", record);
                committed_records.push(record);
            }
        }

        println!("[Recovery] Recovery complete, {} records to replay", committed_records.len());
        Ok(committed_records)
    }

    // 获取活跃事务数
    pub fn get_active_txn_count(&self) -> usize {
        self.active_txns.iter().filter(|(_, committed)| !*committed).count()
    }

    // 获取下一个 txid（用于测试）
    pub fn get_next_txid(&self) -> u64 {
        self.next_txid
    }
}
