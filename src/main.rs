mod fm;
mod mm;
mod rm;
mod pm;
mod common;

use rm::catalog_manager::CatalogManager;
use rm::record_manager::{Record, RecordManager};
use rm::transaction_logger::TransactionLogger;
use rm::table_manager::TableManager;
use rm::types::*;

use rand::Rng;

use crate::common::RID;

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

fn main() -> Result<(), String> {
    println!("===== Task1 DB Test Start =====");

    // 初始化 Catalog & TableManager & Logger & RM
    let catalog = CatalogManager::new()?;
    let logger = TransactionLogger::new(1024 * 1024 * 4);
    let table_manager = TableManager::new(catalog)?;
    let mut rm = RecordManager::new(table_manager, logger);

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

    rm.create_table(schema)?;
    rm.open_table("data/account")?;

    println!("Table created: account");

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
