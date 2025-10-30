mod fm;
mod mm;
mod rm;
mod pm;
mod common;
mod test;

fn main() -> Result<(), String> {
    test::task1::task1()?;
    Ok(())
}