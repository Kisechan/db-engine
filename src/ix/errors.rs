pub type IXResult<T> = Result<T, IXError>;

#[derive(Debug)]
pub enum IXError {
    IndexAlreadyExists,
    IndexNotFound,
    IndexNotOpen,
    DuplicateKey,
    KeyNotFound,
    PageOverflow,
    PageUnderflow,
    IOError(String),
    EOF,
    InvalidOperation,
}
