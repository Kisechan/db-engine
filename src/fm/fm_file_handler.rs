use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use log::error;

use super::fm_file_header::FileHeader;
use super::fm_page_header::PageHeader;
use super::{bincode_options, BLOCK_SIZE};

/// Provides block-level access to a managed file and keeps the on-disk header in sync.
pub struct FileHandle {
    file: File,
    header: FileHeader,
    header_dirty: bool,
    path: PathBuf,
}

impl FileHandle {
    pub(crate) fn new(mut file: File, header: FileHeader, path: PathBuf) -> io::Result<Self> {
        file.seek(SeekFrom::Start(0))?;
        Ok(Self {
            file,
            header,
            header_dirty: false,
            path,
        })
    }

    pub fn header(&self) -> &FileHeader {
        &self.header
    }

    pub fn header_mut(&mut self) -> &mut FileHeader {
        self.header_dirty = true;
        &mut self.header
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn read_block(&mut self, block_num: u32, buf: &mut [u8]) -> io::Result<()> {
        self.ensure_block_range(block_num)?;
        if buf.len() != BLOCK_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "buffer size must match BLOCK_SIZE",
            ));
        }
        self.file.seek(SeekFrom::Start(block_offset(block_num)))?;
        self.file.read_exact(buf)?;
        Ok(())
    }

    pub fn write_block(&mut self, block_num: u32, buf: &[u8]) -> io::Result<()> {
        self.ensure_block_range(block_num)?;
        if buf.len() != BLOCK_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "buffer size must match BLOCK_SIZE",
            ));
        }
        self.file.seek(SeekFrom::Start(block_offset(block_num)))?;
        self.file.write_all(buf)?;
        Ok(())
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.flush_header()?;
        self.file.sync_all()?;
        Ok(())
    }

    pub fn allocate_block(&mut self) -> io::Result<u32> {
        self.allocate_block_with_space(0)
    }

    pub fn allocate_block_with_space(&mut self, min_free_bytes: u32) -> io::Result<u32> {
        if let Some(block) = self.take_free_block(min_free_bytes)? {
            return Ok(block);
        }
        self.append_block()
    }

    pub fn free_block(&mut self, block_num: u32) -> io::Result<()> {
        if block_num == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "block 0 is reserved for the file header",
            ));
        }
        let mut page_hdr = self.read_page_header(block_num)?;
        if page_hdr.prev_free_page != -1
            || page_hdr.next_free_page != -1
            || self.header.first_free_hole == block_num as i32
        {
            return Ok(());
        }
        page_hdr.free_bytes = PageHeader::max_free_bytes();
        page_hdr.prev_free_page = -1;
        page_hdr.next_free_page = self.header.first_free_hole;
        self.write_page_header(block_num, &page_hdr)?;
        if page_hdr.next_free_page >= 0 {
            let mut next = self.read_page_header(page_hdr.next_free_page as u32)?;
            next.prev_free_page = block_num as i32;
            self.write_page_header(page_hdr.next_free_page as u32, &next)?;
        }
        self.header.first_free_hole = block_num as i32;
        self.header_dirty = true;
        Ok(())
    }

    pub fn read_page_header(&mut self, block_num: u32) -> io::Result<PageHeader> {
        self.ensure_block_range(block_num)?;
        let mut buf = vec![0u8; PageHeader::encoded_len()];
        self.file.seek(SeekFrom::Start(block_offset(block_num)))?;
        self.file.read_exact(&mut buf)?;
        let page_hdr: PageHeader = bincode_options()
            .deserialize(&buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(page_hdr)
    }

    pub fn write_page_header(&mut self, block_num: u32, hdr: &PageHeader) -> io::Result<()> {
        self.ensure_block_range(block_num)?;
        let bytes = bincode_options()
            .serialize(hdr)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        if bytes.len() > BLOCK_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "page header larger than block",
            ));
        }
        self.file.seek(SeekFrom::Start(block_offset(block_num)))?;
        self.file.write_all(&bytes)?;
        if bytes.len() < PageHeader::encoded_len() {
            let pad = vec![0u8; PageHeader::encoded_len() - bytes.len()];
            self.file.write_all(&pad)?;
        }
        Ok(())
    }

    pub fn update_free_bytes(&mut self, block_num: u32, free_bytes: u32) -> io::Result<()> {
        if free_bytes as usize > BLOCK_SIZE - PageHeader::encoded_len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "free_bytes exceeds block payload",
            ));
        }
        let mut page_hdr = self.read_page_header(block_num)?;
        let was_in_list = page_hdr.prev_free_page != -1
            || page_hdr.next_free_page != -1
            || self.header.first_free_hole == block_num as i32;
        let should_be_in_list = free_bytes > 0;
        page_hdr.free_bytes = free_bytes;

        match (was_in_list, should_be_in_list) {
            (true, false) => {
                let saved = page_hdr;
                self.detach_from_free_list(block_num, &saved)?;
                page_hdr.prev_free_page = -1;
                page_hdr.next_free_page = -1;
            }
            (false, true) => {
                page_hdr.next_free_page = self.header.first_free_hole;
                page_hdr.prev_free_page = -1;
                if page_hdr.next_free_page >= 0 {
                    let mut next = self.read_page_header(page_hdr.next_free_page as u32)?;
                    next.prev_free_page = block_num as i32;
                    self.write_page_header(page_hdr.next_free_page as u32, &next)?;
                }
                self.header.first_free_hole = block_num as i32;
                self.header_dirty = true;
            }
            _ => {}
        }

        self.write_page_header(block_num, &page_hdr)
    }

    fn ensure_block_range(&self, block_num: u32) -> io::Result<()> {
        if block_num >= self.header.blk_cnt {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "block {} out of range (current block count: {})",
                    block_num, self.header.blk_cnt
                ),
            ));
        }
        Ok(())
    }

    fn take_free_block(&mut self, min_free_bytes: u32) -> io::Result<Option<u32>> {
        let mut current = self.header.first_free_hole;
        while current >= 0 {
            let block = current as u32;
            let mut page_hdr = self.read_page_header(block)?;
            if page_hdr.free_bytes >= min_free_bytes {
                self.detach_from_free_list(block, &page_hdr)?;
                page_hdr.next_free_page = -1;
                page_hdr.prev_free_page = -1;
                self.write_page_header(block, &page_hdr)?;
                return Ok(Some(block));
            }
            current = page_hdr.next_free_page;
        }
        Ok(None)
    }

    fn detach_from_free_list(&mut self, block: u32, page_hdr: &PageHeader) -> io::Result<()> {
        if page_hdr.prev_free_page >= 0 {
            let mut prev = self.read_page_header(page_hdr.prev_free_page as u32)?;
            prev.next_free_page = page_hdr.next_free_page;
            self.write_page_header(page_hdr.prev_free_page as u32, &prev)?;
        } else {
            self.header.first_free_hole = page_hdr.next_free_page;
            self.header_dirty = true;
        }

        if page_hdr.next_free_page >= 0 {
            let mut next = self.read_page_header(page_hdr.next_free_page as u32)?;
            next.prev_free_page = page_hdr.prev_free_page;
            self.write_page_header(page_hdr.next_free_page as u32, &next)?;
        }

        Ok(())
    }

    fn append_block(&mut self) -> io::Result<u32> {
        let new_block = self.header.blk_cnt;
        let offset = block_offset(new_block);
        self.file.seek(SeekFrom::Start(offset))?;
        let page_hdr = PageHeader::new_free();
        let mut block = vec![0u8; BLOCK_SIZE];
        let hdr_bytes = bincode_options()
            .serialize(&page_hdr)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        block[..hdr_bytes.len()].copy_from_slice(&hdr_bytes);
        self.file.write_all(&block)?;
        self.header.blk_cnt += 1;
        self.header_dirty = true;
        Ok(new_block)
    }

    fn flush_header(&mut self) -> io::Result<()> {
        if !self.header_dirty {
            return Ok(());
        }
        let bytes = bincode_options()
            .serialize(&self.header)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        if bytes.len() > BLOCK_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "file header larger than block",
            ));
        }
        self.file.seek(SeekFrom::Start(0))?;
        let mut block = vec![0u8; BLOCK_SIZE];
        block[..bytes.len()].copy_from_slice(&bytes);
        self.file.write_all(&block)?;
        self.file.flush()?;
        self.header_dirty = false;
        Ok(())
    }
}

impl Drop for FileHandle {
    fn drop(&mut self) {
        if let Err(err) = self.flush_header() {
            error!("failed to flush header for {:?}: {}", self.path, err);
        }
    }
}

fn block_offset(block_num: u32) -> u64 {
    block_num as u64 * BLOCK_SIZE as u64
}
