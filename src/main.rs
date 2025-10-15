mod fm;

use fm::{FileManager, BLOCK_SIZE};

fn main() {
    env_logger::init();
    let _file_manager = FileManager::new();
    println!("FM subsystem ready (block size: {} bytes)", BLOCK_SIZE);
}

#[allow(dead_code)]
pub fn file_manager() -> FileManager {
    FileManager::new()
}
