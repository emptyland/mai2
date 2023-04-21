use std::future::pending;
use std::io::Read;
use crate::arena::{ArenaVec, Handle};
use crate::sql::{ParseError, Result, SourceLocation, SourcePosition};
use crate::sql::ast::{CreateTable, Factory, Statement};
use crate::sql::lexer::{Lexer, Token, TokenPart};
use crate::status::{Corrupting, Status};

pub struct Parser<'a> {
    factory: Factory,
    lexer: Lexer<'a>,
    lookahead: TokenPart,
}

impl <'a> Parser<'a> {
    pub fn new(reader: &'a mut dyn Read, factory: Factory) -> Result<Self> {
        let mut lexer = Lexer::new(reader);
        let lookahead = lexer.next()?;
        Ok(Self {
            factory,
            lexer,
            lookahead,
        })
    }

    pub fn parse(&mut self) -> Result<ArenaVec<Handle<dyn Statement>>> {
        let mut stmts: ArenaVec<Handle<dyn Statement>> = ArenaVec::new(&self.factory.arena);
        loop {
            let part = self.peek();
            if part.token == Token::Eof {
                break;
            }

            match part.token {
                Token::Empty => unreachable!(),
                Token::Eof => break,
                Token::Create => {
                    let start_pos = self.lexer.current_position();
                    self.move_next()?;
                    match self.peek().token {
                        Token::Table => {
                            // let stmt = self.parse_create_table(start_pos)?;
                            // stmts.push(stmt);
                        }
                        _ => self.concat_syntax_error(start_pos, "Unexpected `table'".to_string())?
                    }
                }
                _ => self.current_syntax_error("Unexpected statement".to_string())?
            }
        }
        Ok(stmts)
    }

    fn parse_create_table(&mut self, _start_pos: SourcePosition) -> Result<Handle<CreateTable>> {
        self.match_expected(Token::Table)?;


        todo!()
    }

    fn current_syntax_error(&self, message: String) -> Result<()> {
        self.concat_syntax_error(self.lexer.current_position(), message)
    }

    fn concat_syntax_error(&self, start_pos: SourcePosition, message: String) -> Result<()> {
        let location = SourceLocation {
            start: start_pos,
            end: self.lexer.current_position(),
        };
        Err(ParseError::SyntaxError(message, location))
    }

    fn peek(&self) -> &TokenPart {
        &self.lookahead
    }

    fn match_expected(&mut self, token: Token) -> Result<()> {
        if self.peek().token == token {
            self.move_next()?;
            Ok(())
        } else {
            //let message = format!("Unexpected `{}'", &token, self.peek());
            let location = SourceLocation {
                start: self.lexer.current_position(),
                end: self.lexer.current_position(),
            };
            Err(ParseError::SyntaxError("Unexpected toekn".to_string(), location))
        }
    }

    fn move_next(&mut self) -> Result<()> {
        self.lookahead = self.lexer.next()?;
        Ok(())
    }
}