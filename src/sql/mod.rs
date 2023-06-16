use std::io;
use crate::{Arena, ArenaBox, ArenaMut, Corrupting, SliceReadWrapper, Status, utils};
use crate::sql::ast::Expression;
use crate::sql::parser::parse_sql_expr;

pub mod lexer;
pub mod parser;
pub mod ast;
pub mod serialize;


pub fn parse_sql_expr_from_content(sql: &str, arena: &ArenaMut<Arena>)
    -> crate::Result<ArenaBox<dyn Expression>> {
    let mut rd = utils::SliceReadWrapper::from(sql);
    parse_sql_expr(&mut rd, arena)
}

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

#[inline]
fn from_parsing_result<T>(rs: Result<T>) -> std::result::Result<T, Status> {
    match rs {
        Ok(v) => Ok(v),
        Err(e) => Err(Status::corrupted(e.to_string()))
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