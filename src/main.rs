mod fm;
mod mm;
mod rm;
mod pm;
mod ix;
mod common;
mod test;

fn main() -> Result<(), String> {
    test::task1::task1()?;
    Ok(())
}