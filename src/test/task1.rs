use crate::rm::{CatalogManager, TableManager, RecordManager};
use crate::rm::types::{DataType, ColumnDef};
use rand::Rng;

pub fn task1() -> Result<()> {
    // 初始化管理器
    let mut catalog = CatalogManager::new();
    let mut table_mgr = TableManager::new();
    let mut record_mgr = RecordManager::new();

    // 定义 account 表 schema
    let account_schema = vec![
        ColumnDef::new("id".into(), DataType::Int),
        ColumnDef::new("name".into(), DataType::VarChar(32)),
        ColumnDef::new("balance".into(), DataType::Int),
    ];

    catalog.create_table("account", account_schema.clone())?;

    // 创建表
    let table_id = table_mgr.create_table("account")?;
    let mut table = table_mgr.open_table(table_id)?;

    // 插入 10000 行记录
    let mut rng = rand::thread_rng();

    for i in 0..10000 {
        let name = format!("User{:04}", i);
        let balance = rng.gen_range(0..100000);

        let record = vec![
            DataType::Int.pack(i as i32),
            DataType::VarChar(32).pack(name.as_bytes()),
            DataType::Int.pack(balance as i32),
        ];

        record_mgr.insert_record(&mut table, &record)?;
    }

    println!("10000 rows inserted into account table.");
    Ok(())
}
