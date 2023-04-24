use std::io::Read;
use crate::base::{ArenaVec, ArenaBox, ArenaStr};
use crate::sql::{ParseError, Result, SourceLocation, SourcePosition};
use crate::sql::ast::{ColumnDeclaration, CreateTable, Expression, Factory, Statement, TypeDeclaration};
use crate::sql::lexer::{Lexer, Token, TokenPart};

pub struct Parser<'a> {
    factory: Factory,
    lexer: Lexer<'a>,
    lookahead: TokenPart,
}

impl <'a> Parser<'a> {
    pub fn new(reader: &'a mut dyn Read, factory: Factory) -> Result<Self> {
        let mut lexer = Lexer::new(reader, &factory.arena);
        let lookahead = lexer.next()?;
        Ok(Self {
            factory,
            lexer,
            lookahead,
        })
    }

    pub fn parse(&mut self) -> Result<ArenaVec<ArenaBox<dyn Statement>>> {
        let mut stmts: ArenaVec<ArenaBox<dyn Statement>> = ArenaVec::new(&self.factory.arena);
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
                        Token::Table => stmts.push(self.parse_create_table(start_pos)?.into()),
                        _ => self.concat_syntax_error(start_pos, "Unexpected `table'".to_string())?
                    }
                }
                _ => self.current_syntax_error("Unexpected statement".to_string())?
            }
        }
        Ok(stmts)
    }

    fn parse_create_table(&mut self, start_pos: SourcePosition) -> Result<ArenaBox<CreateTable>> {
        self.match_expected(Token::Table)?;

        let if_not_exists = if self.peek().token == Token::If {
            self.match_expected(Token::If)?;
            self.match_expected(Token::Not)?;
            self.match_expected(Token::Exists)?;
            true
        } else {
            false
        };

        let table_name = self.match_id()?;
        let mut node = self.factory.new_create_table(table_name, if_not_exists);
        self.match_expected(Token::LBrace)?;
        loop {
            node.columns.push(self.parse_column_decl()?);
            if !self.test(Token::Comma)? {
                break;
            }
        }

        // primary key (id, id, ...)
        if self.test(Token::Primary)? {
            self.match_expected(Token::Key)?;
            self.match_expected(Token::LParent)?;
            loop {
                node.primary_keys.push(self.match_id()?);
                if !self.test(Token::Comma)? {
                    break;
                }
            }
            self.match_expected(Token::RParent)?;
        }

        self.match_expected(Token::RBrace)?;
        Ok(node)
    }

    // id type [null|not null] [default expr] [primary key] [auto_increment]
    fn parse_column_decl(&mut self) -> Result<ArenaBox<ColumnDeclaration>> {
        let id = self.match_id()?;
        let type_decl = self.parse_type_decl()?;

        let mut is_not_null = false;
        if self.test(Token::Null)? {
            is_not_null = false;
        } else if self.test(Token::Not)? {
            self.match_expected(Token::Null)?;
            is_not_null = true;
        }

        let default_val = if self.test(Token::Default)? {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let is_primary_key = if self.test(Token::Primary)? {
            self.match_expected(Token::Key)?;
            true
        } else {
            false
        };

        let is_auto_increment = self.test(Token::Auto_Increment)?;
        Ok(self.factory.new_column_decl(id, is_auto_increment, is_not_null, is_primary_key,
                                        type_decl, default_val))
    }

    fn parse_type_decl(&mut self) -> Result<ArenaBox<TypeDeclaration>> {
        let part = self.peek().clone();
        match part.token {
            Token::Char
            | Token::Varchar
            | Token::TinyInt
            | Token::SmallInt
            | Token::Int
            | Token::BigInt => {
                self.move_next()?;
                self.match_expected(Token::LParent)?;
                let pos = self.lexer.current_position();
                let len = self.match_int_literal()?;
                if len <= 0 {
                    self.concat_syntax_error(pos, "Invalid type len".to_string())?;
                }
                self.match_expected(Token::RParent)?;
                Ok(self.factory.new_type_decl(part.token, len as usize, 0))
            }
            _ => Err(self.current_syntax_error("Unexpected type".to_string()).err().unwrap())
        }
    }

    fn parse_expr(&mut self) -> Result<ArenaBox<dyn Expression>> {
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

    fn test(&mut self, token: Token) -> Result<bool> {
        if self.peek().token == token {
            self.move_next()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn match_id(&mut self) -> Result<ArenaStr> {
        let part = self.peek().clone();
        if let Token::Id(id) = part.token {
            self.move_next()?;
            Ok(id.clone())
        } else {
            let location = SourceLocation {
                start: self.lexer.current_position(),
                end: self.lexer.current_position(),
            };
            Err(ParseError::SyntaxError("Unexpected `id'".to_string(), location))
        }
    }

    fn match_int_literal(&mut self) -> Result<i64> {
        let part = self.peek().clone();
        if let Token::IntLiteral(n) = part.token {
            self.move_next()?;
            Ok(n)
        } else {
            let location = SourceLocation {
                start: self.lexer.current_position(),
                end: self.lexer.current_position(),
            };
            Err(ParseError::SyntaxError("Unexpected int literal".to_string(), location))
        }
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
            Err(ParseError::SyntaxError("Unexpected token".to_string(), location))
        }
    }

    fn move_next(&mut self) -> Result<()> {
        self.lookahead = self.lexer.next()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::MemorySequentialFile;
    use super::*;
    use crate::sql::ast::Factory;

    #[test]
    fn sanity() -> Result<()> {
        let factory = Factory::new();
        let txt = Vec::from("create table a { a char(122) }");
        let mut file = MemorySequentialFile::new(txt);
        let mut parser = Parser::new(&mut file, factory)?;
        let mut stmts = parser.parse()?;

        // let yaml = serialize_yaml_to_string(stmts[0].deref_mut());
        // println!("{}", yaml);

        assert_eq!(1, stmts.len());
        let ast = ArenaBox::<CreateTable>::from(stmts[0].clone());
        assert_eq!("a", ast.table_name.as_str());
        assert_eq!(1, ast.columns.len());
        assert!(!ast.if_not_exists);
        Ok(())
    }
}