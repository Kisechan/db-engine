//! 日志文件读取与解析工具
//! 用于调试和分析 data/db.log 中的事务日志记录

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use crate::rm::transaction_logger::LogRecord;

pub struct LogReader {
    log_file: String,
}

impl LogReader {
    pub fn new(log_file: &str) -> Self {
        LogReader {
            log_file: log_file.to_string(),
        }
    }

    // 读取所有日志记录
    pub fn read_all_records(&self) -> Result<Vec<LogRecord>, String> {
        if !Path::new(&self.log_file).exists() {
            return Err(format!("Log file not found: {}", self.log_file));
        }

        let file = File::open(&self.log_file)
            .map_err(|e| format!("Failed to open log file: {}", e))?;

        let mut reader = BufReader::new(file);
        let mut records = Vec::new();

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

            records.push(record);
        }

        Ok(records)
    }

    // 按事务 ID 分组的日志记录
    pub fn read_by_txid(&self) -> Result<std::collections::HashMap<u64, Vec<LogRecord>>, String> {
        let records = self.read_all_records()?;
        let mut txid_map = std::collections::HashMap::new();

        for record in records {
            let txid = match &record {
                LogRecord::Begin { txid } => *txid,
                LogRecord::Insert { txid, .. } => *txid,
                LogRecord::Delete { txid, .. } => *txid,
                LogRecord::Update { txid, .. } => *txid,
                LogRecord::Commit { txid } => *txid,
                LogRecord::Abort { txid } => *txid,
            };

            txid_map.entry(txid).or_insert_with(Vec::new).push(record);
        }

        Ok(txid_map)
    }

    // 打印所有日志记录（简洁格式）
    pub fn print_all_records(&self) -> Result<(), String> {
        let records = self.read_all_records()?;

        println!("\n===== Log File: {} =====", self.log_file);
        println!("Total records: {}\n", records.len());

        for (idx, record) in records.iter().enumerate() {
            match record {
                LogRecord::Begin { txid } => {
                    println!("[{}] BEGIN TX{}", idx, txid);
                }
                LogRecord::Insert { txid, table, rid, data } => {
                    println!("[{}] INSERT TX{}: table='{}', RID({}, {}), data_len={} bytes",
                        idx, txid, table, rid.page_id, rid.slot_id, data.len());
                }
                LogRecord::Delete { txid, table, rid } => {
                    println!("[{}] DELETE TX{}: table='{}', RID({}, {})",
                        idx, txid, table, rid.page_id, rid.slot_id);
                }
                LogRecord::Update { txid, table, rid, new_data } => {
                    println!("[{}] UPDATE TX{}: table='{}', RID({}, {}), data_len={} bytes",
                        idx, txid, table, rid.page_id, rid.slot_id, new_data.len());
                }
                LogRecord::Commit { txid } => {
                    println!("[{}] COMMIT TX{}", idx, txid);
                }
                LogRecord::Abort { txid } => {
                    println!("[{}] ABORT TX{}", idx, txid);
                }
            }
        }

        println!("\n===== End of Log =====\n");

        Ok(())
    }

    // 按事务 ID 打印日志（分组显示）
    pub fn print_by_txid(&self) -> Result<(), String> {
        let txid_map = self.read_by_txid()?;

        println!("\n===== Log File: {} (By Transaction) =====", self.log_file);
        println!("Total transactions: {}\n", txid_map.len());

        let mut txids: Vec<_> = txid_map.keys().copied().collect();
        txids.sort();

        for txid in txids {
            if let Some(records) = txid_map.get(&txid) {
                println!("--- Transaction {} ---", txid);
                for record in records {
                    match record {
                        LogRecord::Begin { .. } => println!("  BEGIN"),
                        LogRecord::Insert { table, rid, data , txid} => {
                            println!("  INSERT: table='{}', RID({}, {}), data_len={}",
                                table, rid.page_id, rid.slot_id, data.len());
                        }
                        LogRecord::Delete { table, rid , txid} => {
                            println!("  DELETE: table='{}', RID({}, {})",
                                table, rid.page_id, rid.slot_id);
                        }
                        LogRecord::Update { table, rid, new_data, txid } => {
                            println!("  UPDATE: table='{}', RID({}, {}), data_len={}",
                                table, rid.page_id, rid.slot_id, new_data.len());
                        }
                        LogRecord::Commit { .. } => println!("  COMMIT"),
                        LogRecord::Abort { .. } => println!("  ABORT"),
                    }
                }
                println!();
            }
        }

        println!("===== End of Log =====\n");

        Ok(())
    }

    // 统计日志信息
    pub fn print_statistics(&self) -> Result<(), String> {
        let records = self.read_all_records()?;

        let mut begin_count = 0;
        let mut insert_count = 0;
        let mut delete_count = 0;
        let mut update_count = 0;
        let mut commit_count = 0;
        let mut abort_count = 0;
        let mut total_data_size = 0usize;

        for record in &records {
            match record {
                LogRecord::Begin { .. } => begin_count += 1,
                LogRecord::Insert { data, .. } => {
                    insert_count += 1;
                    total_data_size += data.len();
                }
                LogRecord::Delete { .. } => delete_count += 1,
                LogRecord::Update { new_data, .. } => {
                    update_count += 1;
                    total_data_size += new_data.len();
                }
                LogRecord::Commit { .. } => commit_count += 1,
                LogRecord::Abort { .. } => abort_count += 1,
            }
        }

        println!("\n===== Log Statistics =====");
        println!("File: {}", self.log_file);
        println!("Total records: {}", records.len());
        println!();
        println!("Record types:");
        println!("  BEGIN:  {} entries", begin_count);
        println!("  INSERT: {} entries", insert_count);
        println!("  DELETE: {} entries", delete_count);
        println!("  UPDATE: {} entries", update_count);
        println!("  COMMIT: {} entries", commit_count);
        println!("  ABORT:  {} entries", abort_count);
        println!();
        println!("Data size:");
        println!("  Total data: {} bytes ({:.2} KB)", total_data_size, total_data_size as f32 / 1024.0);
        println!("  Avg per record: {:.2} bytes", total_data_size as f32 / records.len() as f32);
        println!();
        println!("Transaction info:");
        println!("  Committed: {}", commit_count);
        println!("  Aborted:   {}", abort_count);
        println!("========================\n");

        Ok(())
    }

    // 验证日志完整性
    pub fn verify_integrity(&self) -> Result<bool, String> {
        let txid_map = self.read_by_txid()?;

        println!("\n===== Log Integrity Check =====");

        let mut all_valid = true;

        for (txid, records) in &txid_map {
            // 每个事务应该以 BEGIN 开始
            if records.is_empty() {
                println!("TX{}: Empty transaction", txid);
                all_valid = false;
                continue;
            }

            let first = &records[0];
            if !matches!(first, LogRecord::Begin { .. }) {
                println!("TX{}: Does not start with BEGIN", txid);
                all_valid = false;
            }

            // 每个事务应该以 COMMIT 或 ABORT 结束
            let last = &records[records.len() - 1];
            let is_terminated = matches!(last, LogRecord::Commit { .. } | LogRecord::Abort { .. });
            if !is_terminated {
                println!("TX{}: Does not end with COMMIT or ABORT (incomplete)", txid);
                all_valid = false;
            }

            // 检查 txid 一致性
            for (idx, record) in records.iter().enumerate() {
                let record_txid = match record {
                    LogRecord::Begin { txid } => *txid,
                    LogRecord::Insert { txid, .. } => *txid,
                    LogRecord::Delete { txid, .. } => *txid,
                    LogRecord::Update { txid, .. } => *txid,
                    LogRecord::Commit { txid } => *txid,
                    LogRecord::Abort { txid } => *txid,
                };

                if record_txid != *txid {
                    println!("TX{}: Record {} has mismatched txid {}", txid, idx, record_txid);
                    all_valid = false;
                }
            }

            if is_terminated {
                println!("TX{}: Valid ({} records)", txid, records.len());
            }
        }

        if all_valid {
            println!("\nAll transactions are valid");
        } else {
            println!("\nSome issues found in log");
        }

        println!("==============================\n");

        Ok(all_valid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_reader() {
        let reader = LogReader::new("data/db.log");
        
        if let Ok(records) = reader.read_all_records() {
            println!("Read {} log records", records.len());
        }
    }
}