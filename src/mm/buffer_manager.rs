use std::collections::{HashMap, VecDeque};
use std::io;

use crate::fm::FileHandle;
use crate::mm::page_guard::PageGuard;
use crate::mm::page_header::PageHeader;

type BlockId = u32;

// 缓冲区管理器：维护固定容量的内存帧，支持加载/缓存/替换/写回等功能
pub struct BufferManager {
    pub handle: FileHandle,       // 与磁盘交互的文件句柄
    capacity: usize,              // 缓冲区容量（帧数）
    block_size: usize,            // 每块大小（字节）
    frames: Vec<Option<Frame>>,   // 每个槽位存放一个 Frame 或空
    lru_list: VecDeque<usize>,    // LRU 队列：存储帧索引，队首为最近最少使用
    free_list: VecDeque<BlockId>, // 空闲数据页列表
    map: HashMap<BlockId, usize>, // BlockId -> frames 索引的快速映射
}

// 缓冲帧：记录块信息、数据、脏标记和 pin 计数
#[derive(Clone)]
struct Frame {
    block_id: BlockId,
    data: Vec<u8>,
    dirty: bool,
    pin_count: usize,
}

impl BufferManager {
    // 创建新的缓冲区管理器，传入已有的 FileHandle 和帧数容量
    pub fn new(handle: FileHandle, capacity: usize) -> Self {
        let block_size = handle.block_size();
        BufferManager {
            handle,
            capacity,
            block_size,
            frames: vec![None; capacity],
            lru_list: VecDeque::new(),
            free_list: VecDeque::new(),
            map: HashMap::new(),
        }
    }

    // 获取指定块的数据引用
    // - 如果已在缓冲区中命中，则直接返回并 pin
    // - 否则加载块到一个空闲帧或替换最久未使用且未被 pin 的帧
    // fetch 返回带自动 unpin 的 PageGuard
    pub fn fetch(&mut self, block_id: BlockId) -> io::Result<PageGuard> {
        // 1. 查找命中
        if let Some(idx) = self.find_frame(block_id) {
            // 增加 pin 计数
            if let Some(frame) = &mut self.frames[idx] {
                frame.pin_count += 1;
            }
            // 更新 LRU：标记为最近使用
            self.touch(idx);
            // 构造 PageGuard 并返回
            let data_slice = &mut self.frames[idx].as_mut().unwrap().data[..];
            let ptr = data_slice.as_mut_ptr();
            let len = data_slice.len();
            let mgr_ptr = self as *mut Self;
            return Ok(PageGuard::new(mgr_ptr, block_id, ptr, len));
        }
        // 1. 查找命中（使用 map 做 O(1) 查找）
        if let Some(&idx) = self.map.get(&block_id) {
            // 增加 pin 计数
            if let Some(frame) = &mut self.frames[idx] {
                frame.pin_count = 1;
            }
            // 更新 LRU：标记为最近使用
            self.touch(idx);
            // 构造 PageGuard 并返回
            let data_slice = &mut self.frames[idx].as_mut().unwrap().data[..];
            let ptr = data_slice.as_mut_ptr();
            let len = data_slice.len();
            let mgr_ptr = self as *mut Self;
            return Ok(PageGuard::new(mgr_ptr, block_id, ptr, len));
        }
        // 2. 未命中：选择空闲帧或替换
        let idx = if let Some(free_idx) = self.frames.iter().position(|f| f.is_none()) {
            // 有空闲帧
            free_idx
        } else {
            // 全部帧已占用，使用 LRU 算法选出候选
            // 队首为最近最少使用
            while let Some(&victim_idx) = self.lru_list.front() {
                if let Some(frame) = &self.frames[victim_idx] {
                    // 只有未被 pin（pin_count==0）的帧才可替换
                    if frame.pin_count == 0 {
                        break;
                    }
                }
                // 否则移动到队尾，继续寻找
                let x = self.lru_list.pop_front().unwrap();
                self.lru_list.push_back(x);
            }
            let victim_idx = *self.lru_list.front().expect("No frame to replace");
            // 如有脏页，写回磁盘，并从 map 中移除旧映射
            if let Some(old_frame) = &mut self.frames[victim_idx] {
                // 写回脏页（若需要）
                if old_frame.dirty {
                    self.handle
                        .write_block(old_frame.block_id, &old_frame.data)?;
                }
                // 从 map 中移除旧的 block_id > idx 映射
                self.map.remove(&old_frame.block_id);
            }
            // 移除旧帧内容
            self.frames[victim_idx] = None;
            victim_idx
        };
        // 3. 加载新块数据到选定帧
        let mut data = vec![0u8; self.block_size];
        // 从磁盘读取块数据到 buffer
        self.handle.read_block(block_id, &mut data)?;
        // 插入新帧并 pin
        let frame = Frame {
            block_id,
            data,
            dirty: false,
            pin_count: 1,
        };
        self.frames[idx] = Some(frame);
        // 在 map 中登记新的映射
        self.map.insert(block_id, idx);
        // 将该帧标记为最近使用
        self.lru_list.push_back(idx);
        // 构造 PageGuard
        let data_slice = &mut self.frames[idx].as_mut().unwrap().data[..];
        let ptr = data_slice.as_mut_ptr();
        let len = data_slice.len();
        let mgr_ptr = self as *mut Self;
        Ok(PageGuard {
            mgr: mgr_ptr,
            block_id,
            data_ptr: ptr,
            len,
            _marker: std::marker::PhantomData,
        })
    }

    // 解除 pin，允许块被替换
    pub fn unpin(&mut self, block_id: BlockId) {
        if let Some(idx) = self.find_frame(block_id) {
            if let Some(frame) = &mut self.frames[idx] {
                if frame.pin_count > 0 {
                    frame.pin_count -= 1;
                }
            }
        }
    }

    // 标记缓冲区内块为脏页，下次替换或 flush 时写回
    pub fn mark_dirty(&mut self, block_id: BlockId) {
        if let Some(idx) = self.find_frame(block_id) {
            if let Some(frame) = &mut self.frames[idx] {
                frame.dirty = true;
            }
        }
    }

    // 刷写所有脏页到磁盘，并调用底层 FileHandle flush
    pub fn flush_all(&mut self) -> io::Result<()> {
        for opt in &mut self.frames {
            if let Some(frame) = opt {
                if frame.dirty {
                    self.handle.write_block(frame.block_id, &frame.data)?;
                    frame.dirty = false;
                }
            }
        }
        // 刷新文件头元数据
        self.handle.flush()?;
        Ok(())
    }
    // 分配新数据页，初始化页头并写入磁盘，返回 BlockId
    pub fn allocate_data_page(&mut self) -> io::Result<BlockId> {
        let fm_bid = self.handle.allocate_block()?;
        let bid = fm_bid;
        // 初始化页面内容：写入空白 header
        let mut buf = vec![0u8; self.block_size];
        let header = PageHeader {
            slot_count: 0,
            free_offset: PageHeader::SIZE as u16,
            free_bytes: (self.block_size - PageHeader::SIZE) as u16,
        };
        header.to_bytes(&mut buf[..PageHeader::SIZE])?;
        self.handle.write_block(bid, &buf)?;
        self.free_list.push_back(bid);
        Ok(bid)
    }
    // 释放数据页，将 BlockId 加入空闲列表
    pub fn free_page(&mut self, block_id: BlockId) -> io::Result<()> {
        // 如果在缓冲区中，移除缓存
        if let Some(idx) = self.find_frame(block_id) {
            self.frames[idx] = None;
            if let Some(pos) = self.lru_list.iter().position(|&x| x == idx) {
                self.lru_list.remove(pos);
            }
        }
        if let Some(&idx) = self.map.get(&block_id) {
            self.frames[idx] = None;
            if let Some(pos) = self.lru_list.iter().position(|&x| x == idx) {
                self.lru_list.remove(pos);
            }
            // 从 map 中移除映射
            self.map.remove(&block_id);
        }
        self.free_list.push_back(block_id);
        Ok(())
    }

    // 内部：查找指定块对应的帧索引
    fn find_frame(&self, block_id: BlockId) -> Option<usize> {
        self.frames.iter().position(|opt| {
            opt.as_ref()
                .map_or(false, |frame| frame.block_id == block_id)
        });
        // 使用 map 做 O(1) 查找
        self.map.get(&block_id).cloned()
    }

    // 内部：在 LRU 队列中更新指定帧为最近使用
    fn touch(&mut self, idx: usize) {
        if let Some(pos) = self.lru_list.iter().position(|&x| x == idx) {
            self.lru_list.remove(pos);
        }
        self.lru_list.push_back(idx);
    }
}


#[derive(Debug, Clone)]
pub enum ReplacementPolicy {
    LRU,
    CLOCK,
}

// 通用缓存条目（用于查询计划、数据字典、日志缓存）
pub struct CacheEntry<T> {
    pub key: String,
    pub value: T,
    // 用于 CLOCK 算法
    pub used: bool,
}

// 通用缓存，支持 LRU 与 CLOCK 替换算法
pub struct Cache<T> {
    capacity: usize,
    policy: ReplacementPolicy,
    map: HashMap<String, CacheEntry<T>>,
    // LRU 队列：队头为最久未使用
    lru: VecDeque<String>,
    // CLOCK 环：维护条目 key 的列表
    clock: Vec<String>,
    clock_hand: usize,
}

impl<T> Cache<T> {
    pub fn new(capacity: usize, policy: ReplacementPolicy) -> Self {
        Cache {
            capacity,
            policy,
            map: HashMap::new(),
            lru: VecDeque::new(),
            clock: Vec::new(),
            clock_hand: 0,
        }
    }

    pub fn insert(&mut self, key: String, value: T) {
        if self.map.contains_key(&key) {
            self.update_usage(&key);
            if let Some(entry) = self.map.get_mut(&key) {
                entry.value = value;
            }
            return;
        }
        if self.map.len() >= self.capacity {
            match self.policy {
                ReplacementPolicy::LRU => self.evict_lru(),
                ReplacementPolicy::CLOCK => self.evict_clock(),
            }
        }
        let entry = CacheEntry {
            key: key.clone(),
            value,
            used: true,
        };
        self.map.insert(key.clone(), entry);
        self.lru.push_back(key.clone());
        self.clock.push(key);
    }

    pub fn get(&mut self, key: &str) -> Option<&T> {
        let found = self.map.contains_key(key);
        if found {
            if let Some(entry) = self.map.get_mut(key) {
                entry.used = true;
            }
            self.update_usage(key);
            return self.map.get(key).map(|entry| &entry.value);
        }
        None
    }

    fn update_usage(&mut self, key: &str) {
        if let Some(pos) = self.lru.iter().position(|k| k == key) {
            self.lru.remove(pos);
            self.lru.push_back(key.to_string());
        }
    }

    fn evict_lru(&mut self) {
        if let Some(evict_key) = self.lru.pop_front() {
            self.map.remove(&evict_key);
            if let Some(pos) = self.clock.iter().position(|k| k == &evict_key) {
                self.clock.remove(pos);
                if self.clock_hand >= self.clock.len() && !self.clock.is_empty() {
                    self.clock_hand = 0;
                }
            }
        }
    }

    fn evict_clock(&mut self) {
        if self.clock.is_empty() {
            return;
        }
        for _ in 0..self.clock.len() * 2 {
            let key = &self.clock[self.clock_hand];
            if let Some(entry) = self.map.get_mut(key) {
                if entry.used {
                    entry.used = false;
                } else {
                    let evict_key = key.clone();
                    self.map.remove(&evict_key);
                    self.clock.remove(self.clock_hand);
                    if self.clock_hand >= self.clock.len() && !self.clock.is_empty() {
                        self.clock_hand = 0;
                    }
                    if let Some(pos) = self.lru.iter().position(|k| k == &evict_key) {
                        self.lru.remove(pos);
                    }
                    return;
                }
            }
            self.clock_hand = (self.clock_hand + 1) % self.clock.len();
        }
    }
}

// 定义专用缓存类型：
// 查询计划缓存，保存 SQL（或计划）字符串
pub type QueryPlanCache = Cache<String>;
// 数据字典缓存，保存字典信息（简单采用字符串表示）
pub type DictCache = Cache<String>;
// 日志缓存，保存日志记录，每条为字符串
pub type LogBuffer = Cache<String>;

/// MemoryManager 综合管理 DBMS 内存空间的划分与页面加载
/// 划分为：
///   1. 查询计划缓存
///   2. 数据字典缓存
///   3. 数据处理缓存（BufferManager）
///   4. 日志缓存
pub struct MemoryManager {
    pub query_cache: QueryPlanCache,
    pub dict_cache: DictCache,
    pub log_buffer: LogBuffer,
    pub data_buffer: BufferManager,
}

impl MemoryManager {
    // 构造 MemoryManager，传入 FileHandle 用于数据处理缓存，同时设置各缓存容量和替换策略
    pub fn new(
        handle: FileHandle,
        buf_capacity: usize,
        query_cap: usize,
        dict_cap: usize,
        log_cap: usize,
        policy: ReplacementPolicy,
    ) -> Self {
        MemoryManager {
            query_cache: Cache::new(query_cap, policy.clone()),
            dict_cache: Cache::new(dict_cap, policy.clone()),
            log_buffer: Cache::new(log_cap, policy.clone()),
            data_buffer: BufferManager::new(handle, buf_capacity),
        }
    }

    // 访问存储在缓冲池中的页面
    pub fn fetch_page(&mut self, block_id: u32) -> io::Result<PageGuard> {
        self.data_buffer.fetch(block_id)
    }

    // 从磁盘加载一个页面到空槽（若存在空槽则自动加载）
    pub fn load_page_to_empty_slot(&mut self, block_id: u32) -> io::Result<PageGuard> {
        // BufferManager.fetch 内部会优先使用空闲帧加载页面
        self.data_buffer.fetch(block_id)
    }

    // 将页面从磁盘加载到牺牲者缓冲池插槽（触发替换算法）
    pub fn load_page_to_victim_slot(&mut self, block_id: u32) -> io::Result<PageGuard> {
        // 当不存在空闲帧时，BufferManager.fetch 会通过 LRU（或 CLOCK）选择牺牲者插槽
        self.data_buffer.fetch(block_id)
    }
}
