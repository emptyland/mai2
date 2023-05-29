use std::collections::HashMap;
use std::io::Read;
use std::ops::DerefMut;
use std::str::FromStr;
use crate::base::{Arena, ArenaMut, ArenaStr};
use crate::sql::{from_io_result, ParseError, Result};
use crate::sql::{SourceLocation, SourcePosition};

#[derive(Default, Clone)]
pub struct TokenPart {
    pub token: Token,
    pub location: SourceLocation,
}

#[warn(non_camel_case_types)]
#[derive(Debug, Default, PartialEq, Clone)]
pub enum Token {
    #[default]
    Empty,
    Eof,
    Id(ArenaStr),
    IntLiteral(i64),
    FloatLiteral(f64),
    StringLiteral(ArenaStr),

    // keywords
    Create,
    Drop,
    Table,
    If,
    Not,
    Exists,
    Null,
    Primary,
    Key,
    Default,
    Auto_Increment,
    Insert,
    Into,
    Values,
    Row,
    On,
    And,
    Or,
    Like,
    True,
    False,
    Distinct,
    Unique,
    Index,
    Comment,
    Select,
    From,
    Where,
    Group,
    Order,
    By,
    Having,
    Asc,
    Desc,
    Join,
    Left,
    Right,
    Inner,
    Outer,
    Cross,
    As,
    Union,
    All,
    Limit,
    Offset,

    // types:
    Char,
    Varchar,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Double,

    Plus,
    Minus,
    Star,
    Div,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    LBrace,
    RBrace,
    LBracket,
    // [
    RBracket,
    // ]
    LParent,
    // (
    RParent,
    // )
    Comma,
    // ;
    Semi,
    // .
    Dot,
    // ?
    Question,
}

unsafe impl Sync for Token {}

lazy_static! {
    static ref KEYWORDS: Keywords = Keywords::new();
}

struct Keywords {
    words: HashMap<String, Token>,
}

macro_rules! define_keywords {
    {$map:ident [$($key:ident,)+ $(,)?]} => {
        $($map.insert(stringify!($key).to_uppercase(), Token::$key);)+
    }
}

impl Keywords {
    pub fn new() -> Self {
        let mut words = HashMap::new();
        define_keywords! {
            words
            [
                Create,
                Drop,
                Table,
                If,
                Not,
                Exists,
                Null,
                Primary,
                Key,
                Default,
                Auto_Increment,
                Insert,
                Into,
                Values,
                Row,
                On,
                And,
                Or,
                Like,
                True,
                False,
                Distinct,
                Unique,
                Index,
                Comment,
                Select,
                From,
                Where,
                Group,
                Order,
                By,
                Having,
                Asc,
                Desc,
                Join,
                Left,
                Right,
                Inner,
                Outer,
                Cross,
                As,
                Union,
                All,
                Limit,
                Offset,

                Char,
                Varchar,
                TinyInt,
                SmallInt,
                Int,
                BigInt,
                Float,
                Double,
            ]
        }
        Self {
            words
        }
    }

    pub fn get_keyword(&self, key: &str) -> Option<Token> {
        self.words.get(&key.to_uppercase()).cloned()
    }

    pub fn keyword_or_else<Fn>(&self, key: &str, f: Fn) -> Token
    where Fn: FnOnce() -> Token {
        match self.get_keyword(key) {
            Some(token) => token,
            None => f()
        }
    }
}

unsafe impl Sync for Keywords {}

pub struct Lexer<'a> {
    lookahead: char,
    reader: &'a mut dyn Read,
    arena: ArenaMut<Arena>,
    line: u32,
    column: u32,
}

impl<'a> Lexer<'a> {
    pub fn new(reader: &'a mut dyn Read, arena: ArenaMut<Arena>) -> Self {
        let mut this = Self {
            arena,
            lookahead: '\0',
            reader,
            line: 1,
            column: 1,
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
                '-' => return self.move_to_single_token(Token::Minus),
                '*' => return self.move_to_single_token(Token::Star),
                '/' => return self.move_to_single_token(Token::Div),
                '=' => return self.move_to_single_token(Token::Eq),
                ',' => return self.move_to_single_token(Token::Comma),
                '{' => return self.move_to_single_token(Token::LBrace),
                '}' => return self.move_to_single_token(Token::RBrace),
                '[' => return self.move_to_single_token(Token::LBracket),
                ']' => return self.move_to_single_token(Token::RBracket),
                '(' => return self.move_to_single_token(Token::LParent),
                ')' => return self.move_to_single_token(Token::RParent),
                ';' => return self.move_to_single_token(Token::Semi),
                '.' => return self.move_to_single_token(Token::Dot),
                '?' => return self.move_to_single_token(Token::Question),
                '\'' => return self.parse_str_literal('\''),
                '\"' => return self.parse_str_literal('\"'),
                '`' => {
                    let start_pos = self.current_position();
                    return self.parse_id_or_keyword('`', start_pos);
                }
                '_' => {
                    let start_pos = self.current_position();
                    let next_one = self.move_next()?;
                    if Self::is_id_character(next_one) {
                        return self.parse_id_or_keyword(ch, start_pos);
                    }
                    return Ok(self.to_concat_token(Token::Id(self.str("_")), start_pos));
                }
                _ => {
                    if ch.is_ascii_whitespace() {
                        self.move_next()?;
                    } else if ch.is_alphabetic() {
                        return self.parse_id_or_keyword('\0', self.current_position());
                    } else if ch.is_numeric() {
                        return self.parse_number();
                    } else {
                        let message = format!("Invalid token: `{}'", ch);
                        return Err(ParseError::TokenError(message,
                                                          self.current_position()))
                    }
                }
            }
        }
    }

    fn parse_id_or_keyword(&mut self, quote: char, start_pos: SourcePosition) -> Result<TokenPart> {
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
        let token = KEYWORDS.keyword_or_else(id.as_str(),
                                             ||{Token::Id(self.string(&id)) });
        Ok(self.to_concat_token(token, start_pos))
    }

    fn parse_number(&mut self) -> Result<TokenPart> {
        let mut buf = String::new();
        let mut has_dot = false;
        let start_pos = self.current_position();
        loop {
            let ch = self.peek();
            if ch.is_numeric() {
                self.move_next()?;
                buf.push(ch);
            } else if ch == '.' {
                if has_dot {
                    return Err(ParseError::TokenError("Duplicated dot in number".to_string(),
                                                      self.current_position()))
                } else {
                    self.move_next()?;
                    has_dot = true;
                    buf.push(ch);
                }
            } else {
                break;
            }
        }
        let token = if has_dot {
            Token::FloatLiteral(f64::from_str(buf.as_str()).unwrap())
        } else {
            Token::IntLiteral(i64::from_str(buf.as_str()).unwrap())
        };
        Ok(self.to_concat_token(token, start_pos))
    }

    fn parse_str_literal(&mut self, quote: char) -> Result<TokenPart> {
        let start_pos = self.current_position();
        assert_eq!(quote, self.peek());
        self.move_next()?;
        let mut buf = String::new();
        while self.peek() != quote {
            let ch = self.peek();
            if ch == '\r' || ch == '\n' {
                return Err(ParseError::TokenError("Unexpected `return' character in single-line string".to_string(),
                                                  self.current_position()));
            }
            buf.push(ch);
            self.move_next()?;
        }
        self.move_next()?;
        let str = ArenaStr::new(buf.as_str(), self.arena.deref_mut());
        Ok(self.to_concat_token(Token::StringLiteral(str), start_pos))
    }

    fn is_id_character(ch: char) -> bool {
        ch.is_alphanumeric() || ch == '_' || ch == '$'
    }

    fn move_to_single_token(&mut self, token: Token) -> Result<TokenPart> {
        self.move_next()?;
        Ok(TokenPart {
            token,
            location: SourceLocation {
                start: self.current_position(),
                end: self.current_position(),
            },
        })
    }

    fn to_concat_token(&self, token: Token, start_pos: SourcePosition) -> TokenPart {
        TokenPart {
            token,
            location: SourceLocation {
                start: start_pos,
                end: self.current_position(),
            },
        }
    }

    pub fn current_position(&self) -> SourcePosition {
        SourcePosition {
            line: self.line,
            column: self.column,
        }
    }

    fn peek(&self) -> char { self.lookahead }

    fn move_next(&mut self) -> Result<char> {
        let mut buf: [u8; 1] = [0; 1];
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

    fn str(&self, raw: &str) -> ArenaStr {
        let mut arena = self.arena.clone();
        ArenaStr::new(raw, arena.deref_mut())
    }

    fn string(&self, raw: &String) -> ArenaStr {
        let mut arena = self.arena.clone();
        ArenaStr::from_string(raw, arena.deref_mut())
    }
}


#[cfg(test)]
mod tests {
    use crate::storage::MemorySequentialFile;
    use super::*;

    #[test]
    fn sanity() -> Result<()> {
        let mut arena = Arena::new_ref();
        let txt = Vec::from("id");
        let mut file = MemorySequentialFile::new(txt);
        let mut lexer = Lexer::new(&mut file, arena.get_mut());

        let part = lexer.next()?;
        assert_eq!(Token::Id(ArenaStr::new("id", arena.get_mut().deref_mut())), part.token);

        Ok(())
    }

    #[test]
    fn numbers() -> Result<()> {
        let mut arena = Arena::new_ref();
        let txt = Vec::from("0 1 1000 0.111");
        let mut file = MemorySequentialFile::new(txt);
        let mut lexer = Lexer::new(&mut file, arena.get_mut());

        let mut part = lexer.next()?;
        assert_eq!(Token::IntLiteral(0), part.token);
        part = lexer.next()?;
        assert_eq!(Token::IntLiteral(1), part.token);
        part = lexer.next()?;
        assert_eq!(Token::IntLiteral(1000), part.token);
        part = lexer.next()?;
        assert_eq!(Token::FloatLiteral(0.111), part.token);
        part = lexer.next()?;
        assert_eq!(Token::Eof, part.token);
        Ok(())
    }

    #[test]
    fn strings() -> Result<()> {
        let mut arena = Arena::new_ref();
        let txt = Vec::from("\"\" \'\' \'a\'");
        let mut file = MemorySequentialFile::new(txt);
        let mut lexer = Lexer::new(&mut file, arena.get_mut());

        let mut part = lexer.next()?;
        assert_eq!(Token::StringLiteral(ArenaStr::from_arena("", &mut arena.get_mut())), part.token);
        part = lexer.next()?;
        assert_eq!(Token::StringLiteral(ArenaStr::from_arena("", &mut arena.get_mut())), part.token);
        part = lexer.next()?;
        assert_eq!(Token::StringLiteral(ArenaStr::from_arena("a", &mut arena.get_mut())), part.token);
        Ok(())
    }
}