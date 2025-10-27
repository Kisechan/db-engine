use crate::rm::types::TableSchema;
use crate::common::types::{PageId, RID};
use crate::common::disk_manager::PAGE_SIZE;
use crate::fm::file_handler::FileHandler;
use crate::pm::page_handler::{PageHandler, SlotEntry};
use crate::pm::page_header::PageHeader;
use crate::mm::buffer_manager::BufferManager;
use crate::rm::types::ColumnDef;

// 表级操作句柄（绑定一个文件）
pub struct TableHandler {
    pub table_name: String,
    pub schema: TableSchema,
    pub file_handler: FileHandler,
    // 维护该表的数据页列表（可以从 file header/ catalog 中加载）
    pub data_pages: Vec<PageId>,

    // 引用或拥有 BufferManager（在实际项目中用一个全局 BufferManager）
    pub buffer_manager: BufferManager,
}

impl TableHandler {
    pub fn new(table_name: String, schema: TableSchema, file_handler: FileHandler) -> Self {
        // 这里示例构造一个新的 BufferManager，在实际项目会用单例或外部注入
        let bm = BufferManager::new(128, format!("{}.tbl", table_name));
        TableHandler {
            table_name,
            schema,
            file_handler,
            data_pages: Vec::new(),
            buffer_manager: bm,
        }
    }

    // 将 record（字节流）插入表，返回 RID
    pub fn insert(&mut self, record: &[u8]) -> Result<RID, String> {
        // 尝试在现有页上插入
        let data_pages = self.data_pages.clone();
        for pid in data_pages {
            match self.try_insert_on_page(pid, record) {
                Ok(rid) => return Ok(rid),
                Err(_) => {
                    // 该页空间不足，继续尝试下一页
                    continue;
                }
            }
        }

        // 所有现有页都不够 -> 分配新页
        let new_pid = self.file_handler.allocate_page()?;
        
        // 初始化新页
        self.init_page(new_pid)?;
        
        // 在新页上插入记录
        let rid = self.try_insert_on_page(new_pid, record)?;
        
        // 记录新页到页面列表
        self.data_pages.push(new_pid);
        
        Ok(rid)
    }

    // 尝试在指定页上插入记录，失败时返回 Err
    fn try_insert_on_page(&mut self, page_id: PageId, record: &[u8]) -> Result<RID, String> {
        // fetch_page 会自动 pin 该页
        let page_buf = self.buffer_manager.fetch_page(page_id)?;

        // 使用 PageHandler 尝试插入
        let result = {
            let mut ph = PageHandler::new(page_buf, page_id);
            ph.insert_record(record)
        };

        // 根据结果处理 unpin
        match result {
            Ok(rid) => {
                // 插入成功 -> 标记为 dirty 并 unpin
                self.buffer_manager.unpin_page(page_id, true)?;
                Ok(rid)
            }
            Err(e) => {
                // 插入失败（如空间不足）-> unpin 但不标记 dirty
                self.buffer_manager.unpin_page(page_id, false)?;
                Err(e)
            }
        }
    }

    // 初始化一个新页（设置 PageHeader）
    fn init_page(&mut self, page_id: PageId) -> Result<(), String> {
        // fetch_page 获取页缓冲
        let page_buf = self.buffer_manager.fetch_page(page_id)?;

        // 初始化页头
        {
            let mut ph = PageHandler::new(page_buf, page_id);
            
            // 创建初始页头：
            // free_space_offset = PAGE_SIZE（数据区从顶部开始向下增长）
            // slot_count = 0（没有任何记录）
            let init_header = PageHeader {
                free_space_offset: PAGE_SIZE as u16,
                slot_count: 0,
            };
            
            ph.write_header(init_header)?;
        }

        // unpin 页，标记为 dirty
        self.buffer_manager.unpin_page(page_id, true)?;

        Ok(())
    }

    // 获取记录数据
    pub fn get(&mut self, rid: RID) -> Result<Vec<u8>, String> {
        // 检查 RID 是否在有效范围
        if !self.data_pages.contains(&rid.page_id) {
            return Err(format!("Page {} not found in table", rid.page_id));
        }

        // fetch_page 并读取记录
        let page_buf = self.buffer_manager.fetch_page(rid.page_id)?;

        let result = {
            let ph = PageHandler::new(page_buf, rid.page_id);
            ph.get_record(rid)
        };

        // unpin 页（不标记 dirty，因为只是读取）
        if let Err(ref e) = result {
            // 如果读取出错，也要 unpin
            let _ = self.buffer_manager.unpin_page(rid.page_id, false);
            return Err(e.clone());
        }

        self.buffer_manager.unpin_page(rid.page_id, false)?;
        result
    }

    // 删除记录
    pub fn delete(&mut self, rid: RID) -> Result<(), String> {
        // 检查 RID 是否在有效范围
        if !self.data_pages.contains(&rid.page_id) {
            return Err(format!("Page {} not found in table", rid.page_id));
        }

        // fetch_page 并删除记录
        let page_buf = self.buffer_manager.fetch_page(rid.page_id)?;

        let result = {
            let mut ph = PageHandler::new(page_buf, rid.page_id);
            ph.delete_record(rid)
        };

        // 处理结果
        match result {
            Ok(_) => {
                // 删除成功 -> 标记为 dirty 并 unpin
                self.buffer_manager.unpin_page(rid.page_id, true)?;
                
                // 可选：检查页是否为空，如果为空可以回收
                // 这里简化处理，不回收空页
                Ok(())
            }
            Err(e) => {
                // 删除失败 -> unpin 但不标记 dirty
                self.buffer_manager.unpin_page(rid.page_id, false)?;
                Err(e)
            }
        }
    }

    // 更新记录
    pub fn update(&mut self, rid: RID, new_record: &[u8]) -> Result<RID, String> {
        // 检查 RID 是否在有效范围
        if !self.data_pages.contains(&rid.page_id) {
            return Err(format!("Page {} not found in table", rid.page_id));
        }

        // 先读取旧记录获取其长度
        let old_record = self.get(rid)?;
        let old_len = old_record.len();
        let new_len = new_record.len();

        // 如果新长度 <= 旧长度，就原地覆盖
        if new_len <= old_len {
            return self.update_in_place(rid, new_record);
        }

        // 否则删除旧记录并插入新记录
        self.delete(rid)?;
        self.insert(new_record)
    }

    // 原地更新记录（新长度 <= 旧长度）
    fn update_in_place(&mut self, rid: RID, new_record: &[u8]) -> Result<RID, String> {
        // fetch_page
        let page_buf = self.buffer_manager.fetch_page(rid.page_id)?;

        // 读取页头获取 slot 信息
        let result = {
            let mut ph = PageHandler::new(page_buf, rid.page_id);
            
            // 读取 slot 条目
            let slot = ph.read_slot(rid.slot_id)?;
            
            if slot.offset == -1 {
                return Err(format!("Record at RID({}, {}) has been deleted", rid.page_id, rid.slot_id));
            }

            let slot_offset = slot.offset as usize;
            
            // 将新数据写入原位置
            ph.data[slot_offset..slot_offset + new_record.len()].copy_from_slice(new_record);
            
            // 如果新长度 < 旧长度，更新 slot.length
            if new_record.len() < slot.length as usize {
                let updated_slot = SlotEntry {
                    offset: slot.offset,
                    length: new_record.len() as u16,
                };
                ph.write_slot(rid.slot_id, updated_slot)?;
            }

            Ok::<(), String>(())
        };

        // 处理结果并 unpin
        match result {
            Ok(_) => {
                self.buffer_manager.unpin_page(rid.page_id, true)?;
                Ok(rid)
            }
            Err(e) => {
                self.buffer_manager.unpin_page(rid.page_id, false)?;
                Err(e)
            }
        }
    }

    // 刷新表（把所有脏页写回磁盘）
    pub fn flush(&mut self) -> Result<(), String> {
        self.buffer_manager.flush_all()
    }

    // 获取所有数据页
    pub fn get_data_pages(&self) -> &[PageId] {
        &self.data_pages
    }
}
