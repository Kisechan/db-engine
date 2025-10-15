use std::fs::File;
use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

type BlockId = u32;
use super::fm_file_header::FileHeader;
use super::fm_page_header::PageHeader;

// 文件头块编号常量（块 0）
const HEADER_BLOCK_NUMBER: u32 = 0;

// FileHandle: 对单个表/文件的抽象，封装了对块的读写、分配和释放逻辑
pub struct FileHandle {
    file: File,
    path: PathBuf,
    block_size: usize,
    header: FileHeader,
    header_dirty: bool,
}

impl FileHandle {
    // 内部构造器，FileManager 打开文件后返回 FileHandle
    pub(crate) fn new(file: File, path: PathBuf, block_size: usize, header: FileHeader) -> Self {
        Self {
            file,
            path,
            block_size,
            header,
            header_dirty: false,
        }
    }

    // 返回块大小（字节）
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    // 读取内存中的文件头副本
    pub fn header(&self) -> FileHeader {
        self.header
    }

    // 从指定块读取整个块数据到 buffer
    pub fn read_block(&mut self, block: BlockId, buffer: &mut [u8]) -> io::Result<()> {
        // 校验 buffer 长度是否和块大小一致
        if buffer.len() != self.block_size {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "buffer 长度 {} 与块大小 {} 不匹配",
                    buffer.len(),
                    self.block_size
                ),
            ));
        }

        // 不能将文件头块当成数据块读取
        if block == HEADER_BLOCK_NUMBER {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "不能把文件头块作为数据块读取",
            ));
        }

        self.ensure_valid_block(block)?;
        self.seek_to_block(block)?;
        self.file.read_exact(buffer)
    }

    // 将 buffer 的整块数据写回指定块
    pub fn write_block(&mut self, block: BlockId, buffer: &[u8]) -> io::Result<()> {
        if buffer.len() != self.block_size {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "buffer 长度 {} 与块大小 {} 不匹配",
                    buffer.len(),
                    self.block_size
                ),
            ));
        }

        // 禁止直接覆盖文件头块（文件头由 FileHandle 管理并在需要时写回）
        if block == HEADER_BLOCK_NUMBER {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "不能直接覆盖文件头块",
            ));
        }

        self.ensure_valid_block(block)?;
        self.seek_to_block(block)?;
        self.file.write_all(buffer)
    }

    // 分配一个可用块：优先使用空闲链表，否则扩展文件
    pub fn allocate_block(&mut self) -> io::Result<BlockId> {
        if self.header.first_free_hole >= 0 {
            // 从空闲链表头取出一个块
            let block_num = self.header.first_free_hole as u32;
            let mut page_header = self.read_page_header(block_num)?;

            // 更新文件头指向下一个空闲块
            self.header.first_free_hole = page_header.next_free_page;
            self.header_dirty = true;

            // 如果有下一个空闲块，清除其 prev 指向
            if page_header.next_free_page >= 0 {
                let mut next_header = self.read_page_header(page_header.next_free_page as u32)?;
                next_header.prev_free_page = -1;
                self.write_page_header(page_header.next_free_page as u32, &next_header)?;
            }

            // 清理分配后页头的链表指针，写回磁盘
            page_header.next_free_page = -1;
            page_header.prev_free_page = -1;
            self.write_page_header(block_num, &page_header)?;

            Ok(block_num)
        } else {
            // 否则扩展文件，增加一个新块
            let block_num = self.header.block_count;
            self.ensure_capacity(block_num)?;

            let page_header = PageHeader::clear(self.payload_capacity());
            self.header.block_count += 1;
            self.header_dirty = true;

            // 将新块初始化为零（包含页头），以保证确定性
            self.zero_block(block_num, page_header)?;

            Ok(block_num)
        }
    }

    // 释放一个块并将其插入空闲链表头
    pub fn release_block(&mut self, block: BlockId) -> io::Result<()> {
        if block == HEADER_BLOCK_NUMBER {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "不能释放文件头块",
            ));
        }
        self.ensure_valid_block(block)?;

        // 构造空闲页头并写回磁盘（同时清空页内容）
        let page_header = PageHeader::new_free(self.payload_capacity(), self.header.first_free_hole);
        self.zero_block(block, page_header)?;

        // 如果原先有空闲链表头，需要更新其 prev 指向
        if self.header.first_free_hole >= 0 {
            let mut next_header = self.read_page_header(self.header.first_free_hole as u32)?;
            next_header.prev_free_page = block as i32;
            self.write_page_header(self.header.first_free_hole as u32, &next_header)?;
        }

        // 将该释放块设置为新的空闲链表头
        self.header.first_free_hole = block as i32;
        self.header_dirty = true;
        Ok(())
    }

    // 将内存中脏的文件头写回并 flush 文件
    pub fn flush(&mut self) -> io::Result<()> {
        if self.header_dirty {
            self.write_header()?;
            self.header_dirty = false;
        }
        self.file.flush()
    }

    // 将整个块清零并在块首写入 page header
    fn zero_block(&mut self, block_number: u32, page_header: PageHeader) -> io::Result<()> {
        let mut buffer = vec![0u8; self.block_size];
        buffer[..PageHeader::BYTE_SIZE].copy_from_slice(&page_header.to_bytes());
        self.seek_to_block(block_number)?;
        self.file.write_all(&buffer)?;
        Ok(())
    }

    // 验证块号是否在合理范围内（并排除文件头块）
    fn ensure_valid_block(&self, block_number: u32) -> io::Result<()> {
        if block_number == HEADER_BLOCK_NUMBER {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "文件头块不能作为数据块访问",
            ));
        }
        if block_number >= self.header.block_count {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "块 {} 超出范围（当前块数量 {}）",
                    block_number, self.header.block_count
                ),
            ));
        }
        Ok(())
    }

    // 返回块内可用于存放数据的字节数（不包含页头）
    fn payload_capacity(&self) -> u32 {
        (self.block_size - PageHeader::BYTE_SIZE) as u32
    }

    // 确保文件至少能容纳指定块号（按文件长度扩展）
    fn ensure_capacity(&mut self, block_number: u32) -> io::Result<()> {
        let required_len = (block_number as u64 + 1) * self.block_size as u64;
        let current_len = self.file.metadata()?.len();
        if current_len < required_len {
            self.file.set_len(required_len)?;
        }
        Ok(())
    }

    // 读取指定块的页头（块起始处的 PageHeader）
    fn read_page_header(&mut self, block_number: u32) -> io::Result<PageHeader> {
        self.seek_to_block(block_number)?;
        let mut buf = [0u8; PageHeader::BYTE_SIZE];
        self.file.read_exact(&mut buf)?;
        PageHeader::from_bytes(&buf)
    }

    // 写入指定块的页头（覆盖块起始的字节）
    fn write_page_header(&mut self, block_number: u32, header: &PageHeader) -> io::Result<()> {
        self.seek_to_block(block_number)?;
        self.file.write_all(&header.to_bytes())
    }

    // 将内存中的文件头写回块 0
    fn write_header(&mut self) -> io::Result<()> {
        self.seek_to_block(HEADER_BLOCK_NUMBER)?;
        self.file.write_all(&self.header.to_bytes())
    }

    // 定位到指定块偏移
    fn seek_to_block(&mut self, block_number: u32) -> io::Result<()> {
        let offset = block_number as u64 * self.block_size as u64;
        self.file.seek(SeekFrom::Start(offset)).map(|_| ())
    }
}

// 当 FileHandle 被 Drop 时，如果文件头脏则尝试持久化
impl Drop for FileHandle {
    fn drop(&mut self) {
        if self.header_dirty {
            if let Err(err) = self.write_header() {
                eprintln!(
                    "警告: 无法持久化文件头到 {}: {}",
                    self.path.display(),
                    err
                );
            }
        }
        let _ = self.file.flush();
    }
}
