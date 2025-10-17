mod fm;
mod mm;
mod rm;
mod test;

use std::convert::TryInto;
use std::error::Error;
use mm::page::Page;
use mm::page_compact::PageCompact;
use mm::page_header::PageHeader;
use mm::page_ops::PageOps;
use mm::BufferManager;
use test::test1;

// 测试页面级操作：PageHeader、插入/读取/删除、compact、序列化/反序列化
fn test_page_ops(page_size: usize) -> Result<(), Box<dyn Error>> {
    println!("== 开始 Page 层测试 ==");
    // 构造空页面（内存表示）
    let header = PageHeader {
        slot_count: 0,
        free_offset: PageHeader::SIZE as u16,
        free_bytes: (page_size as u16) - (PageHeader::SIZE as u16),
    };
    let mut page = Page {
        header,
        data: Vec::new(),
        slots: Vec::new(),
    };

    // 插入若干记录
    let r1 = b"hello";
    let r2 = b"rustacean";
    let r3 = b"a longer record to test fragmentation and compaction";

    let s1 = page.insert_record(r1)?;
    let s2 = page.insert_record(r2)?;
    let s3 = page.insert_record(r3)?;
    println!("插入记录完成：slot ids = {}, {}, {}", s1, s2, s3);

    // 读取并校验
    let got1 = page.get_record(s1)?;
    assert_eq!(got1, r1);
    let got2 = page.get_record(s2)?;
    assert_eq!(got2, r2);
    println!("读取校验通过");

    // 删除中间一条记录，测试删除逻辑（不紧缩）
    page.delete_record(s2)?;
    println!("删除 slot {} 完成", s2);

    // 删除后读取应报错
    match page.get_record(s2) {
        Ok(_) => panic!("删除后依然能读取到记录，逻辑错误"),
        Err(_) => println!("删除验证通过（无法读取已删除槽）"),
    }

    // 测试紧缩：把剩余记录紧缩到一起并重写槽目录
    let before_free = page.header.free_bytes;
    page.compact(page_size)?;
    let after_free = page.header.free_bytes;
    println!("紧缩完成，free_bytes: {} -> {}", before_free, after_free);

    // 检查紧缩后能插入一个较大的记录（若有足够空间）
    let large = b"this is a newly inserted large record after compaction";
    if page.header.free_bytes as usize >= large.len() + 4 {
        let _ = page.insert_record(large)?;
        println!("紧缩后成功插入大记录");
    } else {
        println!("紧缩后空间仍不足以插入大记录（这是可接受结果）");
    }

    // 序列化到 frame 并从 frame 反序列化，验证 round-trip
    let mut frame = vec![0u8; page_size];
    page.flush(&mut frame)?;
    let page2 = Page::load(&mut frame)?;
    // 校验 slot 数与某些记录
    assert_eq!(page2.header.slot_count, page.header.slot_count);
    if page.header.slot_count > 0 {
        // 只检查第一个有效槽的内容
        let first_slot_idx = 0usize;
        if page2.slots.get(first_slot_idx).is_some() {
            let data = page2.get_record(first_slot_idx as u16)?;
            let data_orig = page.get_record(first_slot_idx as u16)?;
            assert_eq!(data, data_orig);
        }
    }
    println!("Page 序列化/反序列化验证通过");
    println!("== Page 层测试结束 ==\n");
    Ok(())
}

// 测试 BufferManager 的基本读写流程（allocate, fetch, write, read, flush）
// 注意：此函数会消耗 FileHandle（通过 BufferManager::new 接收）
fn test_buffer_manager(handle: fm::FileHandle) -> Result<(), Box<dyn Error>> {
    println!("== 开始 BufferManager 测试 ==");
    // 创建缓冲区管理器，容量 4 帧
    let mut buf_mgr = BufferManager::new(handle, 4);

    // 分配一个数据块（保证不是 block 0）
    let block = buf_mgr.handle.allocate_block()?;
    let bid0 = block;
    print!("测试读写 BlockId({})... ", bid0);

    {
        // fetch 并 pin（返回 PageGuard，Drop 时自动 unpin）
        let mut page = buf_mgr.fetch(bid0)?;
        // 写入 u32 数据到页前 4 字节
        page[..4].copy_from_slice(&42u32.to_le_bytes());
        // 手动 unpin（PageGuard Drop 也会 unpin，但这里演示显式 unpin）
        drop(page);
        // 标记为脏（BufferManager 的 mark_dirty）
        buf_mgr.mark_dirty(bid0);
        buf_mgr.unpin(bid0);
        println!("写入完成");
    }

    {
        // 再次 fetch 读取验证
        let page = buf_mgr.fetch(bid0)?;
        let val = u32::from_le_bytes(page[..4].try_into().unwrap());
        println!("读取到的值 = {}", val);
        // 显示验证
        assert_eq!(val, 42u32);
        drop(page);
        buf_mgr.unpin(bid0);
        println!("读取验证通过");
    }

    // 刷写所有脏页到磁盘
    buf_mgr.flush_all()?;
    println!("flush_all 完成");
    println!("== BufferManager 测试结束 ==\n");
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {

    println!(">>> 开始 Record Manager 初始化测试");
    test1()?;
    Ok(())
}
