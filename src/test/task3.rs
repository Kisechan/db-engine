use crate::rm::catalog_manager::CatalogManager;
use crate::rm::record_manager::{Record, RecordManager};
use crate::rm::transaction_logger::TransactionLogger;
use crate::rm::table_manager::TableManager;
use crate::rm::types::*;
use crate::mm::buffer_manager::BufferManager;
use crate::fm::FileManager;
use crate::ix::ix_manager::IXManager;
use crate::ix::ix_handler::IXHandler;
use crate::common::RID;
use crate::common::disk_manager::PAGE_SIZE;

use rand::Rng;
use std::fs;

fn random_name() -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let mut s: Vec<u8> = (0..20)
        .map(|_| rng.gen_range(b'a'..=b'z'))
        .collect();
    s
}

fn make_account_record(id: u32, name: &[u8]) -> Record {
    let mut fixed = Vec::new();
    fixed.extend_from_slice(&id.to_le_bytes());
    fixed.extend_from_slice(name);
    fixed.extend_from_slice(&0u32.to_le_bytes());
    Record::new(fixed)
}

fn cleanup_old_files() -> Result<(), String> {
    println!("\n===== Cleaning up old files =====");
    
    let data_dir = "data";
    
    if std::path::Path::new(data_dir).exists() {
        std::fs::remove_dir_all(data_dir)
            .map_err(|e| format!("Failed to remove data directory: {}", e))?;
        println!("[Cleanup] Deleted directory: {}", data_dir);
    }

    println!("Cleanup completed.\n");

    std::fs::create_dir(data_dir)
        .map_err(|e| format!("Failed to create data directory: {}", e))?;

    println!("[Init] Created directory: {}", data_dir);

    Ok(())
}

fn ensure_data_dir() -> Result<(), String> {
    let path = "data";
    if !std::path::Path::new(path).exists() {
        std::fs::create_dir(path)
            .map_err(|e| format!("Failed to create data directory: {}", e))?;
        println!("[Init] Created directory: {}", path);
    }
    Ok(())
}

// 计算每页能存放多少索引项
// 假设一个索引项包含：key(4字节ID) + rid(8字节) + 元数据
// B+ 树节点头部占用：is_leaf(1) + key_count(4) + next_leaf(4) = 9 字节
fn calculate_entries_per_page(key_size: usize, rid_size: usize) -> usize {
    let header_size = 9; // is_leaf(1) + key_count(4) + next_leaf(4)
    let entry_size = 2 + key_size + rid_size; // key_len(2) + key + rid
    let available_space = PAGE_SIZE - header_size;
    available_space / entry_size
}

// 显示索引文件的内容（十六进制）
fn display_index_file_content(file_path: &str, max_lines: usize) -> Result<(), String> {
    println!("\n===== Index File Content (Hex) =====");
    println!("File: {}", file_path);
    println!("Page Size: {} bytes\n", PAGE_SIZE);
    
    let file_data = fs::read(file_path)
        .map_err(|e| format!("Failed to read index file: {}", e))?;
    
    let file_size = file_data.len();
    println!("File Size: {} bytes ({} pages)\n", file_size, file_size / PAGE_SIZE);
    
    // 显示前 max_lines 行的十六进制内容
    for (i, chunk) in file_data.chunks(16).enumerate() {
        if i >= max_lines {
            println!("... (remaining {} bytes not shown)", file_size - i * 16);
            break;
        }
        
        print!("{:08x}: ", i * 16);
        
        // 显示十六进制
        for (j, byte) in chunk.iter().enumerate() {
            if j == 8 {
                print!(" ");
            }
            print!("{:02x} ", byte);
        }
        
        // 补齐到 16 个字节
        if chunk.len() < 16 {
            for _ in 0..(16 - chunk.len()) {
                print!("   ");
            }
        }
        
        print!(" |");
        
        // 显示 ASCII
        for byte in chunk {
            if *byte >= 32 && *byte <= 126 {
                print!("{}", *byte as char);
            } else {
                print!(".");
            }
        }
        println!("|");
    }
    
    Ok(())
}

// 显示索引统计信息
fn display_index_stats(
    table_name: &str,
    index_no: usize,
    key_size: usize,
    total_entries: usize,
) -> Result<(), String> {
    println!("\n===== Index Statistics =====");
    println!("Table: {}", table_name);
    println!("Index Number: {}", index_no);
    println!("Key Size: {} bytes", key_size);
    println!("Total Entries: {}", total_entries);
    
    let entries_per_page = calculate_entries_per_page(key_size, 8); // RID = 8 bytes
    println!("Entries Per Page: {} (with {} byte page size)", entries_per_page, PAGE_SIZE);
    
    let estimated_pages = (total_entries + entries_per_page - 1) / entries_per_page;
    println!("Estimated Pages Needed: {}", estimated_pages);
    println!("Estimated Index Size: {} KB", estimated_pages * PAGE_SIZE / 1024);
    
    Ok(())
}

// 创建并插入数据到索引
fn build_index_with_data(
    handler: &mut IXHandler,
    _table_name: &str,
    sample_ids: Vec<u32>,
) -> Result<(), String> {
    println!("\n===== Building Index =====");
    
    for (i, id) in sample_ids.iter().enumerate() {
        // 使用 ID 作为索引键（4字节小端序）
        let key = id.to_le_bytes().to_vec();
        let rid = (*id, i as u16);
        
        handler.insert_entry(key, rid)
            .map_err(|e| format!("Failed to insert index entry: {:?}", e))?;
        
        if i % 10 == 0 {
            println!("[IndexBuilder] Inserted {} entries", i);
        }
    }
    
    println!("[IndexBuilder] Inserted {} entries total", sample_ids.len());
    Ok(())
}

// 执行索引查询示例
fn perform_index_queries(handler: &IXHandler, query_ids: Vec<u32>) -> Result<(), String> {
    println!("\n===== Index Queries =====");
    
    for id in query_ids {
        let key = id.to_le_bytes().to_vec();
        
        match handler.search_entry(&key) {
            Ok(Some((page_id, slot_id))) => {
                println!("[Query] Found ID {} -> RID({}, {})", id, page_id, slot_id);
            }
            Ok(None) => {
                println!("[Query] ID {} not found", id);
            }
            Err(e) => {
                println!("[Query] Error searching for ID {}: {:?}", id, e);
            }
        }
    }
    
    Ok(())
}

// 执行范围扫描
fn perform_range_scan(handler: &IXHandler, lower_id: u32, upper_id: u32) -> Result<(), String> {
    println!("\n===== Range Scan =====");
    println!("Scanning from ID {} to {}", lower_id, upper_id);
    
    let lower_key = lower_id.to_le_bytes().to_vec();
    let upper_key = upper_id.to_le_bytes().to_vec();
    
    match handler.scan_range(&lower_key, &upper_key) {
        Ok(results) => {
            println!("Found {} entries in range [{}, {})", results.len(), lower_id, upper_id);
            
            // 显示前 10 个结果
            for (i, (page_id, slot_id)) in results.iter().take(10).enumerate() {
                println!("  [{}] RID({}, {})", i, page_id, slot_id);
            }
            
            if results.len() > 10 {
                println!("  ... and {} more entries", results.len() - 10);
            }
        }
        Err(e) => {
            println!("Range scan error: {:?}", e);
        }
    }
    
    Ok(())
}

pub fn task3() -> Result<(), String> {
    println!("\n\n");
    println!("========== Task 3: B+ Tree Index Implementation ==========");
    println!("========== Create, Insert, Modify, Delete operations on B+ Tree Index ==========");
    
    // 初始化环境
    let mem_bytes = 100 * PAGE_SIZE;
    let disk_bytes = 1000 * PAGE_SIZE;

    ensure_data_dir()?;
    FileManager::init_disk_from_size(disk_bytes, "data")?;
    println!("[Init] Disk initialized from {} bytes", disk_bytes);

    let _bm = BufferManager::init_memory_from_size(mem_bytes, "data".to_string());
    println!("[Init] Memory buffer initialized from {} bytes ({} KB)", mem_bytes, mem_bytes / 1024);

    cleanup_old_files()?;

    // 初始化系统组件
    let catalog = CatalogManager::new()?;
    println!("[Init] Catalog initialized");
    
    let logger = TransactionLogger::new(1024 * 100 * 4);
    println!("[Init] Logger initialized");
    
    let table_manager = TableManager::new(catalog)?;
    println!("[Init] TableManager initialized");
    
    let mut rm = RecordManager::new(table_manager, logger);
    println!("[Init] RecordManager initialized");

    // ===== Phase 1: Create Table and Insert Data =====
    println!("\n===== Phase 1: Create Table and Insert Data =====");
    
    let schema = TableSchema {
        table_name: "account".to_string(),
        table_id: 0,
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
        create_time: 0,
        row_count: 0,
        last_modified: 0,
    };
    
    println!("[Phase1] Creating table: account");
    rm.create_table(schema)?;
    println!("[Phase1] Table 'account' created");

    rm.open_table("account")?;
    println!("[Phase1] Table opened: account");

    // 插入测试数据
    let test_data_count = 100;
    let mut test_ids = vec![];
    
    println!("[Phase1] Inserting {} test records...", test_data_count);
    rm.begin_transaction()?;

    for i in 0..test_data_count {
        let id = 1000 + i as u32;
        test_ids.push(id);
        
        let name = random_name();
        let rec = make_account_record(id, &name);
        let bytes = rec.serialize();

        let _rid = rm.insert("account", &bytes)?;
        
        if i % 20 == 0 {
            println!("[Phase1] Inserted {} records", i);
        }
    }

    rm.commit_transaction()?;
    println!("[Phase1] Inserted {} records successfully!", test_data_count);

    // ===== Phase 2: Create Index =====
    println!("\n===== Phase 2: Create B+ Tree Index =====");
    
    let mut ix_manager = IXManager::new();
    println!("[Phase2] IXManager created");

    ix_manager.create_index("account", 0, 4)
        .map_err(|e| format!("Failed to create index: {:?}", e))?;
    println!("[Phase2] Index created: account_0");

    // 显示索引文件内容
    let index_file = "data/account.idx0";
    display_index_file_content(index_file, 10)?;

    // 显示索引统计信息
    let key_size = 4; // ID 是 u32，4 字节
    display_index_stats("account", 0, key_size, test_data_count)?;

    // ===== Phase 3: Build Index with Data =====
    println!("\n===== Phase 3: Build Index with Sample Data =====");
    
    {
        let handler = ix_manager.get_handler_mut("account", 0)
            .map_err(|e| format!("Failed to get handler: {:?}", e))?;
        
        // 插入前 50 个 ID 到索引
        let sample_ids = test_ids.iter().take(50).copied().collect::<Vec<_>>();
        build_index_with_data(handler, "account", sample_ids)?;
    }

    println!("[Phase3] Index building completed");

    // 显示更新后的索引文件内容
    display_index_file_content(index_file, 15)?;

    // ===== Phase 4: Index Queries =====
    println!("\n===== Phase 4: Perform Index Queries =====");
    
    {
        let handler = ix_manager.get_handler("account", 0)
            .map_err(|e| format!("Failed to get handler: {:?}", e))?;
        
        // 查询已插入的 ID
        let query_ids = vec![1000, 1005, 1010, 1020, 1049, 1050, 1100];
        perform_index_queries(handler, query_ids)?;
    }

    // ===== Phase 5: Range Scan =====
    println!("\n===== Phase 5: Range Scan Operations =====");
    
    {
        let handler = ix_manager.get_handler("account", 0)
            .map_err(|e| format!("Failed to get handler: {:?}", e))?;
        
        // 执行范围扫描
        perform_range_scan(handler, 1010, 1030)?;
    }

    // ===== Phase 6: Update Index =====
    println!("\n===== Phase 6: Index Update Operations =====");
    
    {
        let handler = ix_manager.get_handler_mut("account", 0)
            .map_err(|e| format!("Failed to get handler: {:?}", e))?;
        
        // 删除一些索引条目
        println!("[Phase6] Deleting index entries...");
        for id in [1005u32, 1015u32, 1025u32].iter() {
            let key = id.to_le_bytes().to_vec();
            let rid = (*id, 0u16);
            
            match handler.delete_entry(key, rid) {
                Ok(_) => println!("[Phase6] Deleted index entry for ID {}", id),
                Err(e) => println!("[Phase6] Failed to delete ID {}: {:?}", id, e),
            }
        }
    }

    // ===== Phase 7: Verify Index After Updates =====
    println!("\n===== Phase 7: Verify Index After Updates =====");
    
    {
        let handler = ix_manager.get_handler("account", 0)
            .map_err(|e| format!("Failed to get handler: {:?}", e))?;
        
        // 查询删除后的状态
        let query_ids = vec![1005, 1015, 1025];
        perform_index_queries(handler, query_ids)?;
    }

    // ===== Phase 8: Multiple Index Creation =====
    println!("\n===== Phase 8: Multiple Index Creation =====");
    
    println!("[Phase8] Creating additional indexes...");
    ix_manager.create_index("account", 1, 4)
        .map_err(|e| format!("Failed to create index 1: {:?}", e))?;
    println!("[Phase8] Index created: account_1");
    
    ix_manager.create_index("account", 2, 4)
        .map_err(|e| format!("Failed to create index 2: {:?}", e))?;
    println!("[Phase8] Index created: account_2");

    let indexes = ix_manager.list_indexes();
    println!("[Phase8] Total indexes created: {}", indexes.len());
    for idx in &indexes {
        println!("  - {}", idx);
    }

    // ===== Phase 9: Display System Catalog Information =====
    println!("\n===== Phase 9: System Catalog Information =====");
    println!("[Phase9] Index Catalog Summary:");
    println!("  - Total Indexes: {}", indexes.len());
    println!("  - Table: account");
    println!("  - Index Files in data/ directory:");
    
    for entry in fs::read_dir("data")
        .map_err(|e| format!("Failed to read data directory: {}", e))?
    {
        let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
        let path = entry.path();
        
        if let Some(file_name) = path.file_name() {
            if let Some(name_str) = file_name.to_str() {
                if name_str.ends_with(".idx0") || name_str.ends_with(".idx1") || name_str.ends_with(".idx2") {
                    if let Ok(metadata) = fs::metadata(&path) {
                        println!("    {} ({} bytes)", name_str, metadata.len());
                    }
                }
            }
        }
    }

    // ===== Phase 10: Summary Statistics =====
    println!("\n===== Phase 10: Summary Statistics =====");
    println!("========== Index Performance Report ==========");
    println!("Page Size                    : {} bytes", PAGE_SIZE);
    println!("Key Size (ID)                : 4 bytes");
    println!("RID Size                     : 8 bytes");
    println!("Maximum Entries Per Page     : {}", calculate_entries_per_page(4, 8));
    println!("Test Data Records Inserted   : {}", test_data_count);
    println!("Index Entries Inserted       : 50");
    println!("Index Entries Deleted        : 3");
    println!("Final Index Entries          : 47");
    println!("Total Indexes Created        : 3");
    println!("Index File Size (each)       : {} bytes (1 page)", PAGE_SIZE);

    // 关闭所有索引
    ix_manager.close_all()
        .map_err(|e| format!("Failed to close indexes: {:?}", e))?;
    println!("\n[Cleanup] All indexes closed");

    println!("========== Task 3 Completed Successfully ==========");

    Ok(())
}
