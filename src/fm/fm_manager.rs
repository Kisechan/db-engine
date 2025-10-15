use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use log::info;

use super::fm_file_handler::FileHandle;
use super::fm_file_header::FileHeader;
use super::{bincode_options, BLOCK_SIZE, PREALLOC_SIZE};

pub struct FileManager;

impl FileManager {
    pub fn new() -> Self {
        Self
    }

    pub fn create_dir<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let path = path.as_ref();
        if path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("directory {} already exists", path.display()),
            ));
        }
        fs::create_dir_all(path)
    }

    pub fn delete_dir<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(());
        }
        fs::remove_dir_all(path)
    }

    pub fn delete_file<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(());
        }
        fs::remove_file(path)
    }

    pub fn preallocate_file<P: AsRef<Path>>(&self, path: P, size: usize) -> io::Result<()> {
        let path = path.as_ref();
        if path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("file {} already exists", path.display()),
            ));
        }
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(path)?;
        let target_len = size.max(BLOCK_SIZE) as u64;
        file.set_len(target_len)?;
        file.sync_data()?;
        Ok(())
    }

    pub fn create_empty_file<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let path = path.as_ref();
        if path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("file {} already exists", path.display()),
            ));
        }
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(path)?;
        file.set_len(BLOCK_SIZE as u64)?;
        file.sync_data()?;
        Ok(())
    }

    pub fn create_table_file<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let path = path.as_ref();
        self.preallocate_file(path, PREALLOC_SIZE)?;
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let header = FileHeader::default();
        write_header(&mut file, &header)?;
        file.sync_all()?;
        info!("created table file {}", path.display());
        Ok(())
    }

    pub fn open_file<P: AsRef<Path>>(&self, path: P) -> io::Result<FileHandle> {
        let path = path.as_ref();
        if !path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("file {} not found", path.display()),
            ));
        }
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let header = read_header(&mut file)?;
        info!("opened file {} ({} blocks)", path.display(), header.blk_cnt);
        FileHandle::new(file, header, path.to_path_buf())
    }

    pub fn close_file(&self, mut handle: FileHandle) -> io::Result<()> {
        let path = handle.path().to_path_buf();
        handle.sync()?;
        info!(
            "closed file {} ({} blocks)",
            path.display(),
            handle.header().blk_cnt
        );
        Ok(())
    }
}

fn write_header(file: &mut File, header: &FileHeader) -> io::Result<()> {
    let bytes = bincode_options()
        .serialize(header)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if bytes.len() > BLOCK_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "file header larger than block",
        ));
    }
    file.seek(SeekFrom::Start(0))?;
    let mut block = vec![0u8; BLOCK_SIZE];
    block[..bytes.len()].copy_from_slice(&bytes);
    file.write_all(&block)
}

fn read_header(file: &mut File) -> io::Result<FileHeader> {
    let mut buf = vec![0u8; BLOCK_SIZE];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut buf)?;
    bincode_options()
        .deserialize(&buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}
