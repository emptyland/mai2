use std::io;

mod lexer;
mod parser;
mod ast;
mod serialize;

#[derive(Debug)]
pub enum ParseError {
    IOError(String),
    SyntaxError(String, SourceLocation),
    TokenError(String, SourcePosition)
}

pub type Result<T> = std::result::Result<T, ParseError>;

#[inline]
fn from_io_result<T>(rs: io::Result<T>) -> Result<T> {
    match rs {
        Ok(v) => Ok(v),
        Err(e) => Err(ParseError::IOError(e.to_string()))
    }
}

#[derive(Debug, Default, Clone)]
pub struct SourceLocation {
    pub start: SourcePosition,
    pub end: SourcePosition,
}

#[derive(Debug, Default, Clone)]
pub struct SourcePosition {
    pub line: u32,
    pub column: u32,
}