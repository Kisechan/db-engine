use std::convert::TryInto;
use std::io::{self, ErrorKind};

// 持久化的文件头，存放在文件的第一个块（块号 0）
// 字段：
// - block_count: 已分配的块数量（下一个可分配块号）
// - first_free_hole: 空闲块链表头（-1 表示无空闲）
// - pre_f / next_f: 预留字段，可用于索引根或双向链表等用途
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileHeader {
    pub block_count: u32,
    pub first_free_hole: i32,
    pub pre_f: i32,
    pub next_f: i32,
}

impl FileHeader {
    // 文件头在磁盘上占用的字节数（固定为 16 字节）
    pub const BYTE_SIZE: usize = 16;

    // 创建一个默认文件头：block_count 从 1 开始（0 用于文件头）
    pub fn new() -> Self {
        Self {
            block_count: 1,
            first_free_hole: -1,
            pre_f: 0,
            next_f: 0,
        }
    }

    // 从小端字节序反序列化
    pub fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() < Self::BYTE_SIZE {
            return Err(io::Error::new(ErrorKind::UnexpectedEof, "文件头缓冲区太小"));
        }

        let block_count = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let first_free_hole = i32::from_le_bytes(bytes[4..8].try_into().unwrap());
        let pre_f = i32::from_le_bytes(bytes[8..12].try_into().unwrap());
        let next_f = i32::from_le_bytes(bytes[12..16].try_into().unwrap());

        Ok(Self {
            block_count,
            first_free_hole,
            pre_f,
            next_f,
        })
    }

    // 序列化为小端字节数组用于写回磁盘
    pub fn to_bytes(self) -> [u8; Self::BYTE_SIZE] {
        let mut buf = [0u8; Self::BYTE_SIZE];
        buf[0..4].copy_from_slice(&self.block_count.to_le_bytes());
        buf[4..8].copy_from_slice(&self.first_free_hole.to_le_bytes());
        buf[8..12].copy_from_slice(&self.pre_f.to_le_bytes());
        buf[12..16].copy_from_slice(&self.next_f.to_le_bytes());
        buf
    }
}

impl Default for FileHeader {
    fn default() -> Self {
        Self::new()
    }
}
