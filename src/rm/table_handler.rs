use crate::common::types::{PageId, RID, PAGE_SIZE, TableSchema, ColumnDef};
use crate::fm::file_handler::FileHandler;
use crate::pm::page_handler::{PageHandler, SlotEntry};
use crate::pm::page_header::PageHeader;
use crate::mm::buffer_manager::BufferManager;
use crate::pm::long_data::{LongDataPtr, LongDataPage, LongDataPageHeader};

// 表级操作句柄（绑定一个文件）
#[derive(Clone)]
pub struct TableHandler {
    pub table_name: String,
    pub schema: TableSchema,
    pub file_handler: FileHandler,
    // 维护该表的数据页列表（可以从 file header/ catalog 中加载）
    pub data_pages: Vec<PageId>,

    // 引用或拥有 BufferManager
    pub buffer_manager: BufferManager,
}

impl TableHandler {
    pub fn new(table_name: String, schema: TableSchema, file_handler: FileHandler, file_path: String) -> Self {
        let bm = BufferManager::new(128, file_path);
        
        // 从 FileHeader 加载数据页列表
        // 页0是 header，页1+ 是数据页
        let mut data_pages = Vec::new();
        for page_id in 1..file_handler.header.total_pages {
            data_pages.push(page_id);
        }
        
        TableHandler {
            table_name,
            schema,
            file_handler,
            data_pages,
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
            // free_slot_head = u16::MAX（没有空闲 slot）
            let init_header = PageHeader {
                free_space_offset: PAGE_SIZE as u16,
                slot_count: 0,
                free_slot_head: u16::MAX,  // 初始无空闲 slot
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
        self.buffer_manager.flush_all()?;
        Ok(())
    }

    // 获取所有数据页
    pub fn get_data_pages(&self) -> &[PageId] {
        &self.data_pages
    }

    // 列出指定页中所有有效的 RID（未被删除的记录）
    pub fn list_valid_rids(&mut self, page_id: PageId) -> Result<Vec<RID>, String> {
        // 检查页是否存在
        if !self.data_pages.contains(&page_id) {
            return Err(format!("Page {} not found in table", page_id));
        }

        // fetch_page 获取页缓冲
        let page_buf = self.buffer_manager.fetch_page(page_id)?;

        // 读取所有 slot 并收集有效 RID
        let rids = {
            let ph = PageHandler::new(page_buf, page_id);
            
            // 读取页头获取 slot 数量
            let header = ph.read_header()?;
            let slot_count = header.slot_count;
            
            let mut valid_rids = Vec::new();
            
            // 遍历所有 slot
            for slot_id in 0..slot_count {
                let slot = ph.read_slot(slot_id)?;
                
                // offset == -1 表示已删除，skip
                if slot.offset != -1 {
                    valid_rids.push(RID {
                        page_id,
                        slot_id,
                    });
                }
            }
            
            valid_rids
        };

        // unpin 页（不标记 dirty，因为只是读取）
        self.buffer_manager.unpin_page(page_id, false)?;

        Ok(rids)
    }

    // 变长数据管理

    // 将变长数据写入外部页面链，返回指针
    pub fn store_var_data(&mut self, data: &[u8]) -> Result<LongDataPtr, String> {
        const LONG_DATA_PAGE_DATA_SIZE: usize = 4090; // 4096 - 6 字节头部

        if data.is_empty() {
            return Err("Cannot store empty data".to_string());
        }

        let mut current_offset = 0;
        let mut first_page_id: Option<PageId> = None;
        let mut prev_page_id: Option<PageId> = None;

        // 分块写入数据
        while current_offset < data.len() {
            // 计算本页应写入的数据量（提前计算以便在循环尾部使用）
            let remaining = data.len() - current_offset;
            let chunk_size = std::cmp::min(remaining, LONG_DATA_PAGE_DATA_SIZE);

            // 分配新页
            let page_id = self.file_handler.allocate_page()?;

            // fetch 页并初始化
            let page_buf = self.buffer_manager.fetch_page(page_id)?;
            {
                let mut lpage = LongDataPage::new(page_id);
                
                let chunk = &data[current_offset..current_offset + chunk_size];

                // 写入数据
                lpage.store_data(0, chunk)?;

                // 不在此处额外分配 next page，prev->next 链接将在外部处理
                // 复制回缓冲区
                page_buf.copy_from_slice(&lpage.data);
            }

            // unpin 并标记为 dirty
            self.buffer_manager.unpin_page(page_id, true)?;

            // 记录第一页 ID
            if first_page_id.is_none() {
                first_page_id = Some(page_id);
            }

            // 记录前一页的 next_page（如果有）
            if let Some(prev_id) = prev_page_id {
                let prev_buf = self.buffer_manager.fetch_page(prev_id)?;
                {
                    let mut lpage = LongDataPage::new(prev_id);
                    lpage.data.copy_from_slice(prev_buf);
                    lpage.set_next_page(Some(page_id))?;
                    prev_buf.copy_from_slice(&lpage.data);
                }
                self.buffer_manager.unpin_page(prev_id, true)?;
            }

            prev_page_id = Some(page_id);
            current_offset += chunk_size;
        }

        Ok(LongDataPtr::new(
            first_page_id.unwrap(),
            data.len() as u32,
        ))
    }

    // 从外部页面链读取变长数据
    pub fn load_var_data(&mut self, ptr: &LongDataPtr) -> Result<Vec<u8>, String> {
        let mut result = Vec::new();
        let mut current_page_id = Some(ptr.first_page_id);
        let mut remaining = ptr.total_length as usize;

        while let Some(page_id) = current_page_id {
            if remaining == 0 {
                break;
            }

            // fetch 页
            let page_buf = self.buffer_manager.fetch_page(page_id)?;

            {
                let mut lpage = LongDataPage::new(page_id);
                let lpage_data_copy = page_buf.to_vec();
                lpage.data.copy_from_slice(&lpage_data_copy);

                let data_len = lpage.get_data_length()?;
                let to_read = std::cmp::min(data_len, remaining);

                // 读取数据
                let chunk = lpage.load_data(0, to_read)?;
                result.extend_from_slice(&chunk);
                remaining -= to_read;

                // 获取下一页
                current_page_id = lpage.get_next_page()?;
            }

            // unpin 页（不标记 dirty）
            self.buffer_manager.unpin_page(page_id, false)?;
        }

        Ok(result)
    }

    // 删除变长字段时释放页面链
    pub fn release_var_data(&mut self, ptr: &LongDataPtr) -> Result<(), String> {
        let mut current_page_id = Some(ptr.first_page_id);

        while let Some(page_id) = current_page_id {
            // fetch 页获取 next_page
            let page_buf = self.buffer_manager.fetch_page(page_id)?;
            let next_page_id = {
                let mut lpage = LongDataPage::new(page_id);
                let lpage_data_copy = page_buf.to_vec();
                lpage.data.copy_from_slice(&lpage_data_copy);
                lpage.get_next_page()?
            };

            // unpin 页
            self.buffer_manager.unpin_page(page_id, false)?;

            // 释放页（回收到 free-list）
            self.file_handler.deallocate_page(page_id)?;

            current_page_id = next_page_id;
        }

        Ok(())
    }

    // 获取单页的统计信息
    pub fn get_page_stats(&mut self, page_id: PageId) -> Result<crate::pm::page_handler::PageStats, String> {
        if !self.data_pages.contains(&page_id) {
            return Err(format!("Page {} not found in table", page_id));
        }

        let page_buf = self.buffer_manager.fetch_page(page_id)?;

        let stats = {
            let ph = PageHandler::new(page_buf, page_id);
            ph.get_stats()?
        };

        self.buffer_manager.unpin_page(page_id, false)?;

        Ok(stats)
    }

    // 获取整个表的统计信息
    pub fn get_table_stats(&mut self) -> Result<TableStats, String> {
        let mut total_slots = 0u32;
        let mut total_free_slots = 0u32;
        let mut total_used_slots = 0u32;
        let mut total_free_space = 0usize;

        for page_id in self.data_pages.clone() {
            let stats = self.get_page_stats(page_id)?;
            total_slots += stats.total_slots as u32;
            total_free_slots += stats.free_slots as u32;
            total_used_slots += stats.used_slots as u32;
            total_free_space += stats.free_data_space;
        }

        Ok(TableStats {
            num_pages: self.data_pages.len() as u32,
            total_slots,
            total_free_slots,
            total_used_slots,
            total_free_space,
        })
    }
}

#[derive(Clone, Debug)]
pub struct TableStats {
    pub num_pages: u32,
    pub total_slots: u32,
    pub total_free_slots: u32,
    pub total_used_slots: u32,
    pub total_free_space: usize,
}

impl TableStats {
    pub fn print_summary(&self) {
        println!("\n===== Table Statistics =====");
        println!("Pages: {}", self.num_pages);
        println!("Total slots: {}", self.total_slots);
        println!("Used slots: {} ({:.1}%)", 
            self.total_used_slots, 
            self.total_used_slots as f32 / self.total_slots as f32 * 100.0);
        println!("Free slots: {} ({:.1}%)", 
            self.total_free_slots,
            self.total_free_slots as f32 / self.total_slots as f32 * 100.0);
        println!("Free data space: {} bytes", self.total_free_space);
        println!("=============================\n");
    }
}
