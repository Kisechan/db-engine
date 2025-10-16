use crate::fm::FileHandle;
use crate::mm::page_guard::PageGuard;
use crate::mm::page_header::PageHeader;
use std::collections::VecDeque;
use std::io;
type BlockId = u32;
// 缓冲区管理器：维护固定容量的内存帧，支持加载/缓存/替换/写回等功能
pub struct BufferManager {
    pub handle: FileHandle,       // 与磁盘交互的文件句柄
    capacity: usize,              // 缓冲区容量（帧数）
    block_size: usize,            // 每块大小（字节）
    frames: Vec<Option<Frame>>,   // 每个槽位存放一个 Frame 或空
    lru_list: VecDeque<usize>,    // LRU 队列：存储帧索引，队首为最近最少使用
    free_list: VecDeque<BlockId>, // 空闲数据页列表
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
            // 如有脏页，写回磁盘
            if let Some(frame) = &mut self.frames[victim_idx] {
                if frame.dirty {
                    self.handle
                        .write_block(frame.block_id as u32, &frame.data)?;
                }
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
        // 将该帧标记为最近使用
        self.lru_list.push_back(idx);
        // 构造 PageGuard
        let data_slice = &mut self.frames[idx].as_mut().unwrap().data[..];
        let ptr = data_slice.as_mut_ptr();
        let len = data_slice.len();
        let mgr_ptr = self as *mut Self;
        Ok(PageGuard::new(mgr_ptr, block_id, ptr, len))
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
        self.free_list.push_back(block_id);
        Ok(())
    }

    // 内部：查找指定块对应的帧索引
    fn find_frame(&self, block_id: BlockId) -> Option<usize> {
        self.frames.iter().position(|opt| {
            opt.as_ref()
                .map_or(false, |frame| frame.block_id == block_id)
        })
    }

    // 内部：在 LRU 队列中更新指定帧为最近使用
    fn touch(&mut self, idx: usize) {
        if let Some(pos) = self.lru_list.iter().position(|&x| x == idx) {
            self.lru_list.remove(pos);
        }
        self.lru_list.push_back(idx);
    }
}
