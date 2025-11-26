// 全表扫描执行器 (Sequential Scan Executor)
// 
// 实现最基本的表扫描算子，按顺序遍历所有数据页和记录。
// 此执行器是其他算子（过滤、投影等）的数据源。

use crate::exec::iterator::{Executor, ExecutorRecord};
use crate::common::types::RID;
use crate::rm::table_handler::TableHandler;
use crate::pm::page_handler::PageHandler;

// 全表扫描执行器（按顺序扫描所有记录）
// 
// # 内部状态
// - table_name: 要扫描的表名
// - table_handler: 拥有表处理器
// - data_pages: 所有数据页 ID 列表
// - current_page_idx: 当前在扫描第几个页
// - page_slot_idx: 当前页中的 slot 索引
// - page_slot_count: 当前页的总 slot 数
// - is_initialized: 是否已初始化
pub struct SeqScanExecutor {
    table_name: String,
    table_handler: TableHandler,
    
    // 扫描状态
    data_pages: Vec<u32>,           // 所有数据页 ID 列表
    current_page_idx: usize,        // 当前在扫描第几个页
    page_slot_idx: u16,             // 当前页中的 slot 索引
    page_slot_count: u16,           // 当前页的总 slot 数
    is_initialized: bool,
}

impl SeqScanExecutor {
    // 创建新的全表扫描执行器
    // 
    // # Arguments
    // - table_name: 要扫描的表名
    // - table_handler: 已初始化的表处理器
    pub fn new(table_name: String, table_handler: TableHandler) -> Self {
        SeqScanExecutor {
            table_name,
            table_handler,
            data_pages: Vec::new(),
            current_page_idx: 0,
            page_slot_idx: 0,
            page_slot_count: 0,
            is_initialized: false,
        }
    }

    // 将当前的 PageId 和 SlotId 组合为 RID
    fn make_rid(&self) -> RID {
        if self.current_page_idx < self.data_pages.len() {
            RID {
                page_id: self.data_pages[self.current_page_idx],
                slot_id: self.page_slot_idx,
            }
        } else {
            RID {
                page_id: 0,
                slot_id: 0,
            }
        }
    }

    // 获取指定页的 slot 数量
    fn load_page_slot_count(&mut self, page_id: u32) -> Result<u16, String> {
        let page_buf = self.table_handler.buffer_manager.fetch_page(page_id)?;

        let ph = PageHandler::new(page_buf, page_id);
        let header = ph.read_header()?;

        self.table_handler.buffer_manager.unpin_page(page_id, false)?;

        Ok(header.slot_count)
    }

    // 检查指定 RID 对应的 slot 是否有效（未删除）
    fn is_slot_valid(&mut self, rid: RID) -> Result<bool, String> {
        let page_buf = self.table_handler.buffer_manager.fetch_page(rid.page_id)?;

        let ph = PageHandler::new(page_buf, rid.page_id);
        let slot = ph.read_slot(rid.slot_id)?;

        self.table_handler.buffer_manager.unpin_page(rid.page_id, false)?;

        // offset == -1 表示记录已删除
        Ok(slot.offset != -1)
    }

    // 从指定 RID 读取记录数据
    fn read_record_at_rid(&mut self, rid: RID) -> Result<Vec<u8>, String> {
        self.table_handler.get(rid)
    }

    // 跳过当前页中已删除的记录，找到下一个有效记录
    // 
    // 在 rm 模块的设计中，offset == -1 表示该 slot 的记录已被删除。
    // 此方法会跳过这些已删除的位置。
    fn skip_deleted_records(&mut self) -> Result<bool, String> {
        loop {
            // 检查是否超过当前页的范围
            if self.page_slot_idx >= self.page_slot_count {
                // 移到下一页
                self.current_page_idx += 1;
                self.page_slot_idx = 0;

                // 检查是否所有页都已扫描完
                if self.current_page_idx >= self.data_pages.len() {
                    return Ok(false);
                }

                // 加载新页的 slot 数量
                let page_id = self.data_pages[self.current_page_idx];
                self.page_slot_count = self.load_page_slot_count(page_id)?;
                continue;
            }

            // 检查当前 slot 是否有效（未被删除）
            let current_rid = self.make_rid();
            if self.is_slot_valid(current_rid)? {
                return Ok(true);
            }

            // 当前 slot 已删除，继续
            self.page_slot_idx += 1;
        }
    }
}

impl Executor for SeqScanExecutor {
    fn init(&mut self) -> Result<(), String> {
        // 获取所有数据页列表
        self.data_pages = self.table_handler.get_data_pages().to_vec();

        if self.data_pages.is_empty() {
            // 表中无页面，设置为已初始化但无数据
            self.is_initialized = true;
            return Ok(());
        }

        // 初始化为第一页
        self.current_page_idx = 0;
        self.page_slot_idx = 0;
        let page_id = self.data_pages[0];
        self.page_slot_count = self.load_page_slot_count(page_id)?;

        self.is_initialized = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<ExecutorRecord>, String> {
        if !self.is_initialized {
            return Err("Executor not initialized. Call init() first.".to_string());
        }

        // 跳过已删除的记录，找到下一个有效记录
        if !self.skip_deleted_records()? {
            // 所有记录都已遍历完
            return Ok(None);
        }

        // 构建当前记录的 RID
        let current_rid = self.make_rid();

        // 读取当前记录的数据
        let data = self.read_record_at_rid(current_rid)?;

        // 移到下一个 slot，为下一次 next() 调用做准备
        self.page_slot_idx += 1;

        Ok(Some(ExecutorRecord {
            rid: current_rid,
            data,
        }))
    }

    fn close(&mut self) -> Result<(), String> {
        // 关闭表处理器（如果需要）
        self.table_handler.flush()?;
        Ok(())
    }
}