pub mod types;
pub mod disk_manager;

pub use types::{PageId, SlotId, RID};
pub use disk_manager::DiskManager;