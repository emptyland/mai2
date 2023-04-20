mod lexer;
mod parser;
mod ast;

pub enum ParseError {
    IOError(String),
    SyntaxError(String, SourceLocation)
}

pub type Result<T> = std::result::Result<T, ParseError>;

#[derive(Debug, Default)]
pub struct SourceLocation {
    pub start: SourcePosition,
    pub end: SourcePosition,
}

#[derive(Debug, Default)]
pub struct SourcePosition {
    pub line: u32,
    pub column: u32,
}