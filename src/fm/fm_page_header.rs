use std::convert::TryInto;
use std::io::{self, ErrorKind};

// 每个数据块开头存储的最小页头，用于链表管理空闲块和记录可用空间
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageHeader {
    // 指向下一个空闲页块（链表），-1 表示末端
    pub next_free_page: i32,
    // 指向前一个空闲页块（双向链表），-1 表示没有
    pub prev_free_page: i32,
    // 页内可用字节数（不包含页头）
    pub free_bytes: u32,
}

impl PageHeader {
    // 页头在块中的字节大小（12 字节）
    pub const BYTE_SIZE: usize = 12;

    // 构造一个新的空闲页头，指向 next
    pub fn new_free(free_bytes: u32, next: i32) -> Self {
        Self {
            next_free_page: next,
            prev_free_page: -1,
            free_bytes,
        }
    }

    // 构造一个清空（非链接）页头
    pub fn clear(free_bytes: u32) -> Self {
        Self {
            next_free_page: -1,
            prev_free_page: -1,
            free_bytes,
        }
    }

    // 从小端字节数组读取页头
    pub fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() < Self::BYTE_SIZE {
            return Err(io::Error::new(
                ErrorKind::UnexpectedEof,
                "页头缓冲区太小",
            ));
        }

        let next_free_page = i32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let prev_free_page = i32::from_le_bytes(bytes[4..8].try_into().unwrap());
        let free_bytes = u32::from_le_bytes(bytes[8..12].try_into().unwrap());

        Ok(Self {
            next_free_page,
            prev_free_page,
            free_bytes,
        })
    }

    // 序列化为字节数组用于写回磁盘
    pub fn to_bytes(self) -> [u8; Self::BYTE_SIZE] {
        let mut buf = [0u8; Self::BYTE_SIZE];
        buf[0..4].copy_from_slice(&self.next_free_page.to_le_bytes());
        buf[4..8].copy_from_slice(&self.prev_free_page.to_le_bytes());
        buf[8..12].copy_from_slice(&self.free_bytes.to_le_bytes());
        buf
    }
}

impl Default for PageHeader {
    fn default() -> Self {
        Self::clear(0)
    }
}
