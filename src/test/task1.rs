use crate::rm::catalog_manager::CatalogManager;
use crate::rm::record_manager::{Record, RecordManager};
use crate::rm::transaction_logger::TransactionLogger;
use crate::rm::table_manager::TableManager;
use crate::rm::types::*;

use rand::Rng;

use crate::common::RID;
use crate::fm::FileManager;

fn random_name() -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let mut s: Vec<u8> = (0..20)
        .map(|_| rng.gen_range(b'a'..=b'z'))
        .collect();
    s
}

// 序列化为 fixed_part
fn make_account_record(id: u32, name: &[u8]) -> Record {
    let mut fixed = Vec::new();
    fixed.extend_from_slice(&id.to_le_bytes());
    fixed.extend_from_slice(name);
    fixed.extend_from_slice(&0u32.to_le_bytes());
    Record::new(fixed)
}

// 清理旧的数据文件
fn cleanup_old_files() -> Result<(), String> {
    println!("\n===== Cleaning up old files =====");
    
    let data_dir = "data";
    
    // 删除整个 data 目录（如果存在）
    if std::path::Path::new(data_dir).exists() {
        std::fs::remove_dir_all(data_dir)
            .map_err(|e| format!("Failed to remove data directory: {}", e))?;
        println!("[Cleanup] Deleted directory: {}", data_dir);
    }

    // 删除 db.log（事务日志）
    let log_path = "db.log";
    if std::path::Path::new(log_path).exists() {
        std::fs::remove_file(log_path)
            .map_err(|e| format!("Failed to delete log file: {}", e))?;
        println!("[Cleanup] Deleted: {}", log_path);
    }

    println!("Cleanup completed.\n");

    std::fs::create_dir(data_dir)
        .map_err(|e| format!("Failed to create data directory: {}", e))?;

    println!("[Init] Created directory: {}", data_dir);

    Ok(())
}

// 确保 data 目录存在
fn ensure_data_dir() -> Result<(), String> {
    let path = "data";
    if !std::path::Path::new(path).exists() {
        std::fs::create_dir(path)
            .map_err(|e| format!("Failed to create data directory: {}", e))?;
        println!("[Init] Created directory: {}", path);
    }
    Ok(())
}

pub fn task1() -> Result<(), String> {
    println!("===== Task1 DB Test Start =====");

    // 确保 data 目录存在
    ensure_data_dir()?;

    // 清理旧文件
    cleanup_old_files()?;

    // 初始化 Catalog & TableManager & Logger & RM
    let catalog = CatalogManager::new()?;
    println!("Catalog initialized.");
    let logger = TransactionLogger::new(1024 * 100 * 4);
    println!("Logger initialized.");
    let table_manager = TableManager::new(catalog)?;
    println!("TableManager initialized.");
    let mut rm = RecordManager::new(table_manager, logger);
    println!("RecordManager initialized.");

    // 定义 account 表结构（定长字段）
    let schema = TableSchema {
        table_name: "account".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Char(20),
                nullable: false,
            },
            ColumnDef {
                name: "balance".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
        ],
        root_pages: vec![],
    };
    
    println!("Creating table: account");

    rm.create_table(schema)?;
    println!("Table 'account' created in catalog.");

    // 注意：这里使用表名 "account"，不是文件路径
    rm.open_table("account")?;
    println!("Table opened: account");

    // 插入 10k 测试数据
    let total = 10_000;
    rm.begin_transaction()?;

    for i in 0..total {
        let name = random_name();
        let rec = make_account_record(i as u32, &name);
        let bytes = rec.serialize();

        let rid = rm.insert("account", &bytes)?;
        if i % 1000 == 0 {
            println!("Inserted {} rows (RID = {:?})", i, rid);
        }
    }

    rm.commit_transaction()?;
    println!("Inserted {} records successfully!", total);

    // 全表扫描验证
    let mut count = 0;
    rm.scan_all("account", |rid: RID, bytes: &[u8]| {
        let rec = Record::deserialize(bytes).unwrap();
        count += 1;
        if count <= 3 {
            println!("[Example] RID {:?}  fixed bytes = {:?}", rid, rec.fixed_part);
        }
        true
    })?;

    println!("Scanned {} records.", count);

    // 输出缓冲页占用 & 文件磁盘页个数
    println!("===== Test Completed Successfully =====");
    Ok(())
}