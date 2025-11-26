mod fm;
mod mm;
mod rm;
mod pm;
mod ix;
mod common;
mod test;
mod sql;
mod exec;
mod plan;

use test::task4;

fn main() -> Result<(), String> {
    task4::task4()?;
    
    Ok(())
}