use std::io::Read;
use std::fmt::Display;
use crate::comparator::Comparator;
use crate::mai2;
use crate::mai2::from_io_result;
use crate::mai2::Result;
use crate::sql::SourceLocation;
use crate::status::{Corrupting, Status};

#[derive(Default)]
pub struct TokenPart {
    pub token: Token,
    pub location: SourceLocation,
}

#[derive(Debug, Default, PartialEq)]
pub enum Token {
    #[default]
    Empty,
    Eof,
    Id(String),
    Create,
    Table,

    Plus,
    Comma,
}

pub struct Lexer<'a> {
    lookahead: char,
    reader: &'a mut dyn Read,
    line: u32,
    column: u32,
}

impl <'a> Lexer<'a> {
    pub fn new(reader: &'a mut dyn Read) -> Self {
        let mut this = Self {
            lookahead: '\0',
            reader,
            line: 0,
            column: 0
        };
        this.move_next().unwrap();
        this
    }

    pub fn next(&mut self) -> Result<TokenPart> {
        loop {
            let ch = self.peek();
            match ch {
                '\0' => return self.move_to_single_token(Token::Eof),
                '+' => return self.move_to_single_token(Token::Plus),
                ',' => return self.move_to_single_token(Token::Comma),
                '`' => {
                    let start_pos = self.current_position();
                }
                '_' => {
                    let start_pos = self.current_position();
                    let next_one = self.move_next()?;
                    if Self::is_id_character(next_one) {
                        return self.parse_id(ch, start_pos);
                    }
                    return Ok(self.to_concat_token(Token::Id("_".to_string()), start_pos));
                }
                _ => {
                    if ch.is_ascii_whitespace() {
                        self.move_next()?;
                    } else if ch.is_alphabetic() {
                        return self.parse_id('\0', self.current_position());
                    }
                }
            }
        }
    }

    fn parse_id(&mut self, quote: char, start_pos: SourcePosition) -> Result<TokenPart> {
        let mut id = String::new();
        if quote != '`' && quote != '\0' {
            id.push(quote);
        }
        loop {
            let ch = self.peek();
            if Self::is_id_character(ch) {
                id.push(ch);
                self.move_next()?;
            } else if quote == '`' && ch == quote {
                self.move_next()?;
                break;
            } else {
                break;
            }
        }
        Ok(self.to_concat_token(Token::Id(id), start_pos))
    }

    fn is_id_character(ch: char) -> bool {
        ch.is_alphabetic() || ch.is_alphanumeric() || ch == '_' || ch == '$'
    }

    fn move_to_single_token(&mut self, token: Token) -> Result<TokenPart> {
        self.move_next()?;
        Ok(TokenPart {
            token,
            location: SourceLocation {
                start: self.current_position(),
                end: self.current_position()
            }
        })
    }

    fn to_concat_token(&self, token: Token, start_pos: SourcePosition) -> TokenPart {
        TokenPart {
            token,
            location: SourceLocation {
                start: start_pos,
                end: self.current_position()
            }
        }
    }

    fn current_position(&self) -> SourcePosition {
        SourcePosition {
            line: self.line,
            column: self.column
        }
    }

    fn peek(&self) -> char { self.lookahead }

    fn move_next(&mut self) -> Result<char> {
        let mut buf: [u8;1] = [0;1];
        let read_in_bytes = from_io_result(self.reader.read(&mut buf[..]))?;
        if read_in_bytes < buf.len() {
            self.lookahead = '\0';
            Ok(self.lookahead)
        } else {
            self.lookahead = char::from(buf[0]);
            if self.lookahead == '\n' {
                self.line += 1;
                self.column = 1;
            } else {
                self.column += 1;
            }
            Ok(self.lookahead)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::env::MemorySequentialFile;
    use super::*;

    #[test]
    fn sanity() -> Result<()> {
        let txt = Vec::from("id");
        let mut file = MemorySequentialFile::new(txt);
        let mut lexer = Lexer::new(&mut file);

        let part = lexer.next()?;
        assert_eq!(Token::Id(String::from("id")), part.token);

        Ok(())
    }
}