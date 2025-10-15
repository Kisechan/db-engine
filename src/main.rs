mod fm;

use std::convert::TryInto;
use std::error::Error;
use std::path::PathBuf;

// 简单示例：展示如何使用 FileManager / FileHandle
// 这个例子会：
// 1. 创建 data 目录
// 2. 在 data/example.tbl 不存在时创建表文件（写入文件头并预分配空间）
// 3. 打开文件，分配一个块，向块写入 4 字节整数并读回，最后 flush
fn main() -> Result<(), Box<dyn Error>> {
    let manager = fm::FileManager::new(fm::FileManagerConfig::default());

    let data_dir = PathBuf::from("data");
    manager.create_dir(&data_dir)?;

    let table_path = data_dir.join("example.tbl");
    if !table_path.exists() {
        manager.create_table_file(&table_path)?;
    }

    // 打开文件并获取句柄
    let mut handle = manager.open_file(&table_path)?;
    // 分配一个新块
    let block = handle.allocate_block()?;

    // 将一个 u32 值写入块的起始位置
    let mut write_buffer = vec![0u8; handle.block_size()];
    write_buffer[..4].copy_from_slice(&2025u32.to_le_bytes());
    handle.write_block(block, &write_buffer)?;

    // 读回验证
    let mut read_buffer = vec![0u8; handle.block_size()];
    handle.read_block(block, &mut read_buffer)?;
    let value = u32::from_le_bytes(read_buffer[..4].try_into().unwrap());

    println!(
        "读取到的值 {} 来自块 {}，文件 {}",
        value,
        block.number,
        table_path.display()
    );

    // 确保文件头持久化
    handle.flush()?;
    Ok(())
}
