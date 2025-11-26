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

use test::task3;

fn main() -> Result<(), String> {
    task3::task3()?;
    
    Ok(())
}