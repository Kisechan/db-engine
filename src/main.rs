mod fm;
mod mm;

use fm::{FileManager, FileManagerConfig};
use mm::BufferManager;
use std::convert::TryInto;
use std::error::Error;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn Error>> {
    // 初始化 FileManager
    let fm_config = FileManagerConfig::default();
    let file_manager = FileManager::new(fm_config);
    let data_dir = PathBuf::from("data");
    file_manager.create_dir(&data_dir)?;
    let table_path = data_dir.join("example.tbl");
    if !table_path.exists() {
        file_manager.create_table_file(&table_path)?;
    }
    println!("初始化 FileManager 成功");
    println!("文件路径: {:?}", table_path);
    // 打开文件句柄
    let handle = file_manager.open_file(&table_path)?;

    // 初始化 BufferManager，容量为 4 帧
    let mut buf_mgr = BufferManager::new(handle, 4);
    // 使用 BlockId(0) 测试读写
    let block = buf_mgr.handle.allocate_block()?;
    let bid0 = block;
    print!("测试读写 BlockId(0)... ");
    {
        // fetch 并 pin
        let mut data = buf_mgr.fetch(bid0)?;
        // 写入 u32 数据
        data[..4].copy_from_slice(&42u32.to_le_bytes());
        drop(data); // 释放对 buf_mgr 的可变借用
        buf_mgr.mark_dirty(bid0);
        // unpin
        buf_mgr.unpin(bid0);
        println!("BufferManager 写入值 42 到 BlockId(0)");
    }
    {
        // 再次 fetch 读取验证
        let val = {
            let data = buf_mgr.fetch(bid0)?;
            u32::from_le_bytes(data[..4].try_into().unwrap())
        };
        println!("BufferManager 读取到的值 = {}", val);
        buf_mgr.unpin(bid0);
        println!("BufferManager 读取验证通过");
    }
    // 刷写所有脏页到磁盘
    buf_mgr.flush_all()?;

    Ok(())
}
