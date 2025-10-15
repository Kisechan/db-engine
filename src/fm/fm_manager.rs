use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;

use super::fm_file_handler::FileHandle;
use super::fm_file_header::FileHeader;

// FileManager 配置：块大小与预分配字节数
#[derive(Clone, Copy, Debug)]
pub struct FileManagerConfig {
    pub block_size: usize,
    pub preallocate_bytes: u64,
}

impl Default for FileManagerConfig {
    fn default() -> Self {
        const DEFAULT_BLOCK_SIZE: usize = 4096;
        const DEFAULT_PREALLOC_BLOCKS: u64 = 16;
        Self {
            block_size: DEFAULT_BLOCK_SIZE,
            // 默认预分配若干块以减少小文件增长时的开销
            preallocate_bytes: DEFAULT_BLOCK_SIZE as u64 * DEFAULT_PREALLOC_BLOCKS,
        }
    }
}

// FileManager 提供更高层次的文件/目录管理以及打开文件为 FileHandle 的工厂方法
pub struct FileManager {
    config: FileManagerConfig,
}

impl FileManager {
    pub fn new(config: FileManagerConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &FileManagerConfig {
        &self.config
    }

    // 创建目录（递归），如果已存在且为目录则返回 Ok
    pub fn create_dir<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let path = path.as_ref();
        if path.exists() {
            if path.is_dir() {
                return Ok(());
            }
            return Err(io::Error::new(
                ErrorKind::AlreadyExists,
                format!("路径 {} 已存在但不是目录", path.display()),
            ));
        }
        fs::create_dir_all(path)
    }

    // 删除目录及其内容（递归）
    pub fn delete_dir<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(());
        }
        fs::remove_dir_all(path)
    }

    // 删除文件
    pub fn delete_file<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(());
        }
        if path.is_dir() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!("{} 是一个目录", path.display()),
            ));
        }
        fs::remove_file(path)
    }

    // 创建表文件：创建上级目录、按预分配大小扩展文件并写入初始文件头
    pub fn create_table_file<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)?;
            }
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;

        let aligned_size = self.align_prealloc();
        file.set_len(aligned_size)?;
        self.initialize_file(&mut file)
    }

    // 打开已有文件并读取文件头，返回 FileHandle
    pub fn open_file<P: AsRef<Path>>(&self, path: P) -> io::Result<FileHandle> {
        let path = path.as_ref();
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let metadata = file.metadata()?;
        if metadata.len() < self.config.block_size as u64 {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!(
                    "文件 {} 小于一个块（{} 字节）",
                    path.display(),
                    self.config.block_size
                ),
            ));
        }
        let header = self.read_header(&mut file)?;
        if self.config.block_size < FileHeader::BYTE_SIZE {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "块大小小于文件头字节数",
            ));
        }
        Ok(FileHandle::new(
            file,
            path.to_path_buf(),
            self.config.block_size,
            header,
        ))
    }

    // 初始化新文件，写入默认文件头并填充首个块
    fn initialize_file(&self, file: &mut File) -> io::Result<()> {
        let header = FileHeader::new();
        let mut buffer = vec![0u8; self.config.block_size];
        buffer[..FileHeader::BYTE_SIZE].copy_from_slice(&header.to_bytes());
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&buffer)?;
        file.flush()
    }

    // 读取文件头（块 0 的前若干字节）
    fn read_header(&self, file: &mut File) -> io::Result<FileHeader> {
        file.seek(SeekFrom::Start(0))?;
        let mut buf = [0u8; FileHeader::BYTE_SIZE];
        file.read_exact(&mut buf)?;
        FileHeader::from_bytes(&buf)
    }

    // 计算并对齐预分配的字节数到块大小的整数倍
    fn align_prealloc(&self) -> u64 {
        let block_size = self.config.block_size as u64;
        let min_size = block_size;
        let mut prealloc = self.config.preallocate_bytes.max(min_size);
        let remainder = prealloc % block_size;
        if remainder != 0 {
            prealloc += block_size - remainder;
        }
        prealloc
    }
}
