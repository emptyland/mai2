use std::io;
use crate::Status;

pub mod lexer;
pub mod parser;
pub mod ast;
pub mod serialize;

use lexer::Token;

#[derive(Debug)]
pub enum ParseError {
    IOError(String),
    SyntaxError(String, SourceLocation),
    TokenError(String, SourcePosition),
}

impl ParseError {
    pub fn to_string(&self) -> String {
        match self {
            Self::IOError(e) => format!("IO error: {}", e),
            Self::SyntaxError(e, location) => {
                format!("Syntax error: [{}:{}-{}:{}] {}",
                        location.start.line,
                        location.start.column,
                        location.end.line,
                        location.end.column,
                        e)
            },
            Self::TokenError(e, pos) => {
                format!("Incorrect token: [{}:{}] {}", pos.line, pos.column, e)
            }
        }
    }
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