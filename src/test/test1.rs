use std::error::Error;
use std::path::PathBuf;
use crate::fm::{FileManager, FileManagerConfig};
use crate::rm::TableManager;

// 模拟 banking 场景：
// 1. 输入可用的主存空间（以帧数表示），转变为主存中缓冲区块  
// 2. 输入预留的磁盘空间块数，转变为磁盘中的块（预先分配）  
// 3. 向 account 表插入 10000 条记录，从而验证按块管理内存和磁盘
pub fn test1() -> Result<(), Box<dyn Error>> {
    println!("=== 开始 Banking 场景 初始化测试 ===");

    // 模拟主存空间：可用缓冲块数（例如 16 帧）
    let available_memory_frames: usize = 16;
    println!("主存可用块数：{}", available_memory_frames);

    // 模拟磁盘空间：预留磁盘块数（例如 1000 块）
    let available_disk_blocks: u32 = 1000;
    println!("磁盘预留块数：{}", available_disk_blocks);

    // 初始化 FileManager（磁盘空间管理）
    let fm_config = FileManagerConfig::default();
    let file_manager = FileManager::new(fm_config);

    // 数据目录，用于存放表文件
    let data_dir = PathBuf::from("data");
    file_manager.create_dir(&data_dir)?;

    // 创建或打开 account 表文件
    let table_path = data_dir.join("account.tbl");
    if !table_path.exists() {
        file_manager.create_table_file(&table_path)?;
        // 预分配磁盘块（模拟磁盘空间的块划分）
        {
            // 打开 FileHandle 后调用 allocate_block 多次预先分配
            let mut handle = file_manager.open_file(&table_path)?;
            for _ in 0..available_disk_blocks {
                let _ = handle.allocate_block()?;
            }
            println!("预分配 {} 个磁盘块完成", available_disk_blocks);
        }
    }
    println!("初始化 FileManager 成功，文件路径：{:?}", table_path);

    // 打开 FileHandle（将磁盘空间转为块）
    let handle = file_manager.open_file(&table_path)?;

    // 构造 TableManager（内部构造 BufferManager，实现内存块管理）
    let mut table_mgr = TableManager::new(handle, available_memory_frames);

    // 生成数据字典并插入记录
    const NUM_RECORDS: usize = 10_000;
    for i in 0..NUM_RECORDS {
        // 构造一条记录：格式为 "<account_id>,<name>,<balance>"
        let account_id = i + 1;
        let name = format!("User{:05}", i + 1);
        let balance = format!("{:.2}", 1000.0 + (i as f64) * 0.5);
        let record_str = format!("{},{},{}", account_id, name, balance);
        let record_bytes = record_str.as_bytes();

        // 插入记录：TableManager.insert 内部会调用 BufferManager.allocate_data_page
        // 从而按块管理内存和磁盘中的记录存储
        let rid = table_mgr.insert(record_bytes)?;
        // 每插入 1000 条打印一次进度
        if (i + 1) % 1000 == 0 {
            println!("已插入 {} 条记录，最后插入的 rid：{:?}", i + 1, rid);
        }
    }
    println!("共插入 {} 条记录", NUM_RECORDS);
    println!("=== Banking 场景 初始化测试完成 ===");

    Ok(())
}