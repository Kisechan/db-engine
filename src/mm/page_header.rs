use std::io::{self, ErrorKind};

/// 页面头元数据，存储槽目录计数、数据区偏移和剩余空闲字节数
#[derive(Debug, Clone)]
pub struct PageHeader {
    /// 当前有效槽数
    pub slot_count: u16,
    /// 下一个可写记录的起始偏移
    pub free_offset: u16,
    /// 页内剩余的空闲字节数
    pub free_bytes: u16,
}

impl PageHeader {
    /// 页头在帧中的字节长度
    pub const SIZE: usize = 6;

    /// 从字节缓冲区解析出 PageHeader，要求 buf.len() >= SIZE
    pub fn from_bytes(buf: &[u8]) -> io::Result<PageHeader> {
        if buf.len() < PageHeader::SIZE {
            return Err(io::Error::new(ErrorKind::UnexpectedEof, "buffer too small for PageHeader"));
        }
        let slot_count = u16::from_le_bytes([buf[0], buf[1]]);
        let free_offset = u16::from_le_bytes([buf[2], buf[3]]);
        let free_bytes = u16::from_le_bytes([buf[4], buf[5]]);
        Ok(PageHeader { slot_count, free_offset, free_bytes })
    }

    /// 将 PageHeader 序列化到字节缓冲区，要求 buf.len() >= SIZE
    pub fn to_bytes(&self, buf: &mut [u8]) -> io::Result<()> {
        if buf.len() < PageHeader::SIZE {
            return Err(io::Error::new(ErrorKind::UnexpectedEof, "buffer too small for PageHeader"));
        }
        buf[0..2].copy_from_slice(&self.slot_count.to_le_bytes());
        buf[2..4].copy_from_slice(&self.free_offset.to_le_bytes());
        buf[4..6].copy_from_slice(&self.free_bytes.to_le_bytes());
        Ok(())
    }
}