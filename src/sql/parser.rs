use std::io::Read;
use crate::base::{ArenaVec, ArenaBox, ArenaStr};
use crate::sql::{ParseError, Result, SourceLocation, SourcePosition};
use crate::sql::ast::{ColumnDeclaration, CreateTable, DropTable, Expression, Factory, Identifier, InsertIntoTable, Operator, Placeholder, Statement, TypeDeclaration};
use crate::sql::lexer::{Lexer, Token, TokenPart};

pub struct Parser<'a> {
    factory: Factory,
    lexer: Lexer<'a>,
    lookahead: TokenPart,
    placeholder_order: usize,
}

impl <'a> Parser<'a> {
    pub fn new(reader: &'a mut dyn Read, factory: Factory) -> Result<Self> {
        let mut lexer = Lexer::new(reader, &factory.arena);
        let lookahead = lexer.next()?;
        Ok(Self {
            factory,
            lexer,
            lookahead,
            placeholder_order: 0,
        })
    }

    pub fn parse(&mut self) -> Result<ArenaVec<ArenaBox<dyn Statement>>> {
        self.parse_with_processor(|x,_|{x})
    }

    pub fn parse_with_processor<T, Fn>(&mut self, proc: Fn) -> Result<ArenaVec<T>>
    where Fn: FnOnce(ArenaBox<dyn Statement>, usize)->T + Copy {
        let mut stmts: ArenaVec<T> = ArenaVec::new(&self.factory.arena);
        loop {
            self.placeholder_order = 0;
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
                            let rv = proc(self.parse_create_table(start_pos)?.into(),
                                          self.placeholder_order);
                            stmts.push(rv);
                        }
                        _ => Err(self.concat_syntax_error(start_pos, "Unexpected `table'".to_string()))?
                    }
                }
                Token::Drop => {
                    let start_pos = self.lexer.current_position();
                    self.move_next()?;
                    match self.peek().token {
                        Token::Table => {
                            let rv = proc(self.parse_drop_table(start_pos)?.into(),
                                          self.placeholder_order);
                            stmts.push(rv);
                        }
                        _ => Err(self.concat_syntax_error(start_pos, "Unexpected `table'".to_string()))?
                    }
                }
                Token::Insert => {
                    let rv = proc(self.parse_insert_into_table()?.into(), self.placeholder_order);
                    stmts.push(rv);
                }
                _ => Err(self.current_syntax_error("Unexpected statement".to_string()))?
            }

            if !self.test(Token::Semi)? {
                break;
            }
        }
        Ok(stmts)
    }

    fn parse_create_table(&mut self, _start_pos: SourcePosition) -> Result<ArenaBox<CreateTable>> {
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

    fn parse_drop_table(&mut self, _start_pos: SourcePosition) -> Result<ArenaBox<DropTable>> {
        self.match_expected(Token::Table)?;

        let if_exists = if self.peek().token == Token::If {
            self.match_expected(Token::If)?;
            self.match_expected(Token::Exists)?;
            true
        } else {
            false
        };
        let table_name = self.match_id()?;
        Ok(self.factory.new_drop_table(table_name, if_exists))
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
                    Err(self.concat_syntax_error(pos, "Invalid type len".to_string()))?;
                }
                self.match_expected(Token::RParent)?;
                Ok(self.factory.new_type_decl(part.token, len as usize, 0))
            }
            _ => Err(self.current_syntax_error("Unexpected type".to_string()))
        }
    }

    fn parse_insert_into_table(&mut self) -> Result<ArenaBox<InsertIntoTable>> {
        self.match_expected(Token::Insert)?;
        self.test(Token::Into)?;
        self.match_expected(Token::Table)?;

        let table_name = self.match_id()?;
        let mut node = self.factory.new_insert_into_table(table_name);
        self.match_expected(Token::LParent)?;
        while !self.test(Token::RParent)? {
            node.columns_name.push(self.match_id()?);
            if !self.test(Token::Comma)? {
                self.match_expected(Token::RParent)?;
                break;
            }
        }

        self.match_expected(Token::Values)?;
        loop {
            let mut row = ArenaVec::new(&self.factory.arena);
            self.test(Token::Row)?;
            self.match_expected(Token::LParent)?;
            while !self.test(Token::RParent)? {
                row.push(self.parse_expr()?);
                if !self.test(Token::Comma)? {
                    self.match_expected(Token::RParent)?;
                    break;
                }
            }
            node.values.push(row);
            if !self.test(Token::Comma)? {
                break;
            }
        }

        Ok(node)
    }

    fn parse_expr(&mut self) -> Result<ArenaBox<dyn Expression>> {
        let mut next_op = None;
        let expr = self.parse_expr_with_priority(0, &mut next_op)?;
        Ok(expr)
    }

    fn parse_expr_with_priority(&mut self, limit: i32, receiver: &mut Option<Operator>) -> Result<ArenaBox<dyn Expression>> {
        let _start_pos = self.lexer.current_position();
        if let Some(op) = Operator::from_token(&self.peek().token) {
            self.move_next()?;
            let mut next_op = None;
            let expr = self.parse_expr_with_priority(110, &mut next_op)?;
            return Ok(self.factory.new_unary_expr(op, expr).into());
        }
        let mut expr = self.parse_simple()?;

        let mut may_op = Operator::from_token(&self.peek().token);
        while may_op.is_some() && may_op.as_ref().unwrap().priority() > limit {
            let op = may_op.unwrap();
            self.move_next()?;
            let mut next_op = None;
            let rhs = self.parse_expr_with_priority(op.priority(), &mut next_op)?;
            expr = self.factory.new_binary_expr(op.clone(), expr, rhs).into();
            may_op = next_op;
        }
        *receiver = may_op;
        Ok(expr.into())
    }

    fn parse_simple(&mut self) -> Result<ArenaBox<dyn Expression>> {
        let part = self.peek();
        match part.token {
            Token::Null => {
                self.move_next()?;
                Ok(self.factory.new_literal(()).into())
            }
            Token::True => {
                self.move_next()?;
                Ok(self.factory.new_literal(1i64).into())
            }
            Token::False => {
                self.move_next()?;
                Ok(self.factory.new_literal(0i64).into())
            }
            Token::Question => {
                self.move_next()?;
                let placeholder = self.factory.new_placeholder(self.placeholder_order);
                self.placeholder_order += 1;
                Ok(placeholder.into())
            }
            _ => self.parse_primary()
        }
    }


    fn parse_primary(&mut self) -> Result<ArenaBox<dyn Expression>> {
        let _start_pos = self.lexer.current_position();
        match self.peek().token.clone() {
            Token::LParent => {
                self.move_next()?;
                let expr = self.parse_expr()?;
                self.match_expected(Token::RParent)?;
                Ok(expr)
            }
            Token::IntLiteral(val) => {
                self.move_next()?;
                Ok(self.factory.new_literal(val).into())
            }
            Token::StringLiteral(val) => {
                self.move_next()?;
                Ok(self.factory.new_literal(val).into())
            }
            Token::FloatLiteral(val) => {
                self.move_next()?;
                Ok(self.factory.new_literal(val).into())
            }
            Token::Id(symbol) => {
                self.move_next()?;
                if self.test(Token::Dot)? {
                    let suffix = self.match_id()?;
                    Ok(self.factory.new_fully_qualified_name(symbol, suffix).into())
                } else if self.test(Token::LParent)? {
                    //self.move_next()?;
                    let distinct = self.test(Token::Distinct)?;
                    let mut call = self.factory.new_call_function(symbol, distinct);
                    while !self.test(Token::RParent)? {
                        let arg = self.parse_expr()?;
                        call.args.push(arg);
                        if !self.test(Token::Comma)? {
                            self.match_expected(Token::RParent)?;
                            break;
                        }
                    }
                    Ok(call.into())
                } else {
                    Ok(self.factory.new_identifier(symbol).into())
                }
            },
            _ => unreachable!()
        }

    }

    fn current_syntax_error(&self, message: String) -> ParseError {
        self.concat_syntax_error(self.lexer.current_position(), message)
    }

    fn concat_syntax_error(&self, start_pos: SourcePosition, message: String) -> ParseError {
        let location = SourceLocation {
            start: start_pos,
            end: self.lexer.current_position(),
        };
        ParseError::SyntaxError(message, location)
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
            let message = format!("Unexpected token: `{:?}', expected: `{:?}'", token,
                                  self.peek().token);
            Err(ParseError::SyntaxError(message, location))
        }
    }

    fn move_next(&mut self) -> Result<()> {
        self.lookahead = self.lexer.next()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::ops::DerefMut;
    use std::rc::Rc;
    use crate::base::Arena;
    use crate::storage::MemorySequentialFile;
    use super::*;
    use crate::sql::ast::Factory;
    use crate::sql::serialize::serialize_yaml_to_string;

    #[test]
    fn sanity() -> Result<()> {
        let factory = Factory::new();
        let txt = Vec::from("create table a { a char(122) }");
        let mut file = MemorySequentialFile::new(txt);
        let mut parser = Parser::new(&mut file, factory)?;
        let stmts = parser.parse()?;

        // let yaml = serialize_yaml_to_string(stmts[0].deref_mut());
        // println!("{}", yaml);

        assert_eq!(1, stmts.len());
        let ast = ArenaBox::<CreateTable>::from(stmts[0].clone());
        assert_eq!("a", ast.table_name.as_str());
        assert_eq!(1, ast.columns.len());
        assert!(!ast.if_not_exists);
        Ok(())
    }

    #[test]
    fn insert_into_table() -> Result<()> {
        let arena = Arena::new_rc();
        let yaml = parse_all_to_yaml(&arena, "insert into table t1(a,b,c) values(1,2,3)")?;
        println!("{}", yaml);
        assert_eq!("InsertIntoTable
  table_name: t1
  columns_name:
    - a
    - b
    - c
  values:
    - row:
      - IntLiteral: 1
      - IntLiteral: 2
      - IntLiteral: 3
", yaml);
        Ok(())
    }

    #[test]
    fn simple_expr() -> Result<()> {
        let arena = Arena::new_rc();
        let yaml = parse_expr_to_yaml(&arena, "col + b")?;
        println!("{}", yaml);
        assert_eq!("BinaryExpression:
  op: Add(+)
  lhs:
    Identifier: col
  rhs:
    Identifier: b\n", yaml);
        Ok(())
    }

    #[test]
    fn call_function_expr() -> Result<()> {
        let arena = Arena::new_rc();
        let yaml = parse_expr_to_yaml(&arena, "sum(1)")?;
        println!("{}", yaml);
        assert_eq!("CallFunction:
  name: sum
  distinct: false
  in_args_star: false
  args:
    - IntLiteral: 1
", yaml);
        Ok(())
    }

    fn parse_all_to_yaml(arena: &Rc<RefCell<Arena>>, sql: &str) -> Result<String> {
        parse_to_yaml(arena, sql, false)
    }

    fn parse_expr_to_yaml(arena: &Rc<RefCell<Arena>>, sql: &str) -> Result<String> {
        parse_to_yaml(arena, sql, true)
    }

    fn parse_to_yaml(arena: &Rc<RefCell<Arena>>, sql: &str, parse_expr_only: bool) -> Result<String> {
        let factory = Factory::from(arena);
        let txt = Vec::from(sql);
        let mut file = MemorySequentialFile::new(txt);
        let mut parser = Parser::new(&mut file, factory)?;

        if parse_expr_only {
            let mut expr = parser.parse_expr()?;
            return Ok(serialize_yaml_to_string(expr.deref_mut()));
        }
        let mut stmts = parser.parse()?;

        let mut buf = String::new();
        for i in 0..stmts.len() {
            buf.push_str(serialize_yaml_to_string(stmts[i].deref_mut()).as_str());
        }

        Ok(buf)
    }
}