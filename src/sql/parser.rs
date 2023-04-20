use std::io::Read;
use crate::mai2::Result;
use crate::sql::ast::Statement;
use crate::sql::lexer::{Lexer, Token, TokenPart};
use crate::status::{Corrupting, Status};

pub struct Parser<'a> {
    lexer: Lexer<'a>,
    lookahead: TokenPart,
}

impl <'a> Parser<'a> {
    pub fn new(reader: &'a mut dyn Read) -> Result<Self> {
        let mut lexer = Lexer::new(reader);
        let lookahead = lexer.next()?;
        Ok(Self {
            lexer,
            lookahead,
        })
    }

    pub fn parse(&mut self) -> Result<Vec<Statement>> {

        Ok(Vec::default())
    }

    pub fn peek(&self) -> &TokenPart {
        &self.lookahead
    }

    pub fn match_expected(&mut self, token: Token) -> Result<()> {
        if self.peek().token == token {
            self.move_next()?;
            Ok(())
        } else {
            //let message = format!("Unexpected `{}'", &token, self.peek());
            Err(Status::corrupted("unexpected token"))
        }
    }

    pub fn move_next(&mut self) -> Result<()> {
        self.lookahead = self.lexer.next()?;
        Ok(())
    }
}