mod fm;
mod mm;
mod rm;
mod pm;
mod common;
mod test;

use rm::catalog_manager::CatalogManager;
use rm::record_manager::{Record, RecordManager};
use rm::transaction_logger::TransactionLogger;
use rm::table_manager::TableManager;
use rm::types::*;

use rand::Rng;

use crate::common::RID;
use crate::fm::FileManager;

fn main() -> Result<(), String> {
    test::task1::task1()?;
    Ok(())
}