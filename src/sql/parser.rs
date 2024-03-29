use std::io::Read;

use crate::{Arena, arena_vec, ArenaMut};
use crate::base::{ArenaBox, ArenaStr, ArenaVec};
use crate::sql::{from_parsing_result, ParseError, Result, SourceLocation, SourcePosition};
use crate::sql::ast::*;
use crate::sql::lexer::{Lexer, Token, TokenPart};

pub struct Parser<'a, R: ?Sized> {
    factory: Factory,
    lexer: Lexer<'a, R>,
    lookahead: TokenPart,
    placeholder_order: usize,
}

pub fn parse_sql_expr<R: Read + ?Sized>(reader: &mut R, arena: &ArenaMut<Arena>)
                                        -> crate::Result<ArenaBox<dyn Expression>> {
    let factory = Factory::new(arena);
    let mut parser = from_parsing_result(Parser::new(reader, factory))?;
    from_parsing_result(parser.parse_expr())
}

impl<'a, R: Read + ?Sized> Parser<'a, R> {
    pub fn new(reader: &'a mut R, factory: Factory) -> Result<Self> {
        let mut lexer = Lexer::new(reader, factory.arena.clone());
        let lookahead = lexer.next()?;
        Ok(Self {
            factory,
            lexer,
            lookahead,
            placeholder_order: 0,
        })
    }

    pub fn parse(&mut self) -> Result<ArenaVec<ArenaBox<dyn Statement>>> {
        self.parse_with_processor(|x, _| { x })
    }

    pub fn parse_with_processor<T, Fn>(&mut self, mut proc: Fn) -> Result<ArenaVec<T>>
        where Fn: FnMut(ArenaBox<dyn Statement>, usize) -> T {
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
                        Token::Index => {
                            let rv = proc(self.parse_create_index(start_pos)?.into(),
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
                        Token::Index => {
                            let rv = proc(self.parse_drop_index(start_pos)?.into(),
                                          self.placeholder_order);
                            stmts.push(rv);
                        }
                        _ => Err(self.concat_syntax_error(start_pos, "Unexpected `table'".to_string()))?
                    }
                }
                Token::Alert => {
                    let rv = proc(self.parse_alert_table()?.into(), self.placeholder_order);
                    stmts.push(rv);
                }
                Token::Insert => {
                    let rv = proc(self.parse_insert_into_table()?.into(), self.placeholder_order);
                    stmts.push(rv);
                }
                Token::With => {
                    let rv = proc(self.parse_cte()?, self.placeholder_order);
                    stmts.push(rv);
                }
                Token::Select => {
                    let rv = proc(self.parse_select()?, self.placeholder_order);
                    stmts.push(rv);
                }
                Token::Delete => {
                    let rv = proc(self.parse_delete()?, self.placeholder_order);
                    stmts.push(rv);
                }
                Token::Update => {
                    let rv = proc(self.parse_update()?, self.placeholder_order);
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
        if self.test(Token::Constraint)? {
            self.match_expected(Token::Primary)?;
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

        // index name (xxx,xxx)
        // key name (xxx,xxx)
        // unique name (xxx,xxx)
        loop {
            let (has_index, is_unique) = if self.test(Token::Index)?
                || self.test(Token::Key)? {
                (true, false)
            } else if self.test(Token::Unique)? { // unique [index|key]
                if !self.test(Token::Index)? {
                    self.test(Token::Key)?;
                }
                (true, true)
            } else {
                (false, false)
            };
            if !has_index {
                break;
            }

            let name = self.match_id()?;
            let mut index_decl = self.factory.new_index_decl(name, is_unique);
            self.match_expected(Token::LParent)?;
            loop {
                index_decl.key_parts.push(self.match_id()?);
                if !self.test(Token::Comma)? {
                    break;
                }
            }
            node.secondary_indices.push(index_decl);
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
            // default <expr>
            Some(self.parse_expr()?)
        } else {
            None
        };

        let is_primary_key = if self.test(Token::Primary)? {
            // primary key
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
            | Token::Varchar => {
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
            Token::TinyInt
            | Token::SmallInt
            | Token::Int
            | Token::BigInt => {
                self.move_next()?;
                if self.test(Token::LParent)? {
                    let pos = self.lexer.current_position();
                    let len = self.match_int_literal()?;
                    if len <= 0 {
                        Err(self.concat_syntax_error(pos, "Invalid type len".to_string()))?;
                    }
                    self.match_expected(Token::RParent)?;
                }
                Ok(self.factory.new_type_decl(part.token, 11, 0))
            }
            Token::Float => {
                self.move_next()?;
                Ok(self.factory.new_type_decl(part.token, 0, 0))
            }
            Token::Double => {
                self.move_next()?;
                Ok(self.factory.new_type_decl(part.token, 0, 0))
            }
            _ => {
                let err = format!("Unexpected type, expected: {:?}", part.token);
                Err(self.current_syntax_error(err))
            }
        }
    }

    fn parse_create_index(&mut self, _start_pos: SourcePosition) -> Result<ArenaBox<CreateIndex>> {
        let is_unique = self.test(Token::Unique)?;
        self.match_expected(Token::Index)?;
        let name = self.match_id()?;
        self.match_expected(Token::On)?;
        let table_name = self.match_id()?;
        let mut node = self.factory.new_create_index(name, is_unique, table_name);

        self.match_expected(Token::LParent)?;
        loop {
            let key_part = self.match_id()?;
            node.key_parts.push(key_part);
            if !self.test(Token::Comma)? {
                self.match_expected(Token::RParent)?;
                break;
            }
        }
        Ok(node)
    }

    fn parse_drop_index(&mut self, _start_pos: SourcePosition) -> Result<ArenaBox<DropIndex>> {
        self.match_expected(Token::Index)?;
        let (primary_key, name) = if self.test(Token::Primary)? {
            (true, self.factory.str("PRIMARY"))
        } else {
            (false, self.match_id()?)
        };

        self.match_expected(Token::On)?;
        let table_name = self.match_id()?;

        Ok(self.factory.new_drop_index(name, primary_key, table_name))
    }

    fn parse_alert_table(&mut self) -> Result<ArenaBox<AlertTable>> {
        self.match_expected(Token::Alert)?;
        self.match_expected(Token::Table)?;

        let name = self.match_id()?;
        let action = if self.test(Token::Add)? {
            self.match_expected(Token::Column)?;
            let col_decl = self.parse_column_decl()?;
            AlertTableAction::AddColumn(col_decl, self.parse_column_placement_pos_hint()?)
        } else if self.test(Token::Change)? {
            self.match_expected(Token::Column)?;
            AlertTableAction::ChangeColumn(self.match_id()?, self.parse_column_decl()?,
                                           self.parse_column_placement_pos_hint()?)
        } else if self.test(Token::Alert)? {
            self.match_expected(Token::Column)?;
            let col_name = self.match_id()?;
            let set_or_drop = self.test(Token::Set)?;
            if !set_or_drop {
                self.match_expected(Token::Drop)?;
            }
            self.match_expected(Token::Default)?;
            if set_or_drop {
                AlertTableAction::SetDefault(col_name, self.parse_expr()?)
            } else {
                AlertTableAction::DropDefault(col_name)
            }
        } else if self.test(Token::Modify)? {
            self.match_expected(Token::Column)?;
            AlertTableAction::ModifyColumn(self.parse_column_decl()?,
                                           self.parse_column_placement_pos_hint()?)
        } else if self.test(Token::Drop)? {
            self.match_expected(Token::Column)?;
            AlertTableAction::DropColumn(self.match_id()?)
        } else if self.test(Token::Rename)? {
            self.match_expected(Token::To)?;
            AlertTableAction::DropColumn(self.match_id()?)
        } else {
            Err(self.current_syntax_error("Unexpected alert table sub-clause.".to_string()))?
        };

        Ok(self.factory.new_alert_table(name, action))
    }

    fn parse_column_placement_pos_hint(&mut self) -> Result<ColumnPlacementPosHint> {
        let hint = if self.test(Token::First)? {
            ColumnPlacementPosHint::First
        } else if self.test(Token::After)? {
            ColumnPlacementPosHint::After(self.match_id()?)
        } else {
            ColumnPlacementPosHint::Default
        };
        Ok(hint)
    }

    fn parse_insert_into_table(&mut self) -> Result<ArenaBox<InsertIntoTable>> {
        self.match_expected(Token::Insert)?;
        self.match_expected(Token::Into)?;
        //self.match_expected(Token::Table)?;
        self.test(Token::Table)?;

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

    fn parse_relation(&mut self) -> Result<ArenaBox<dyn Relation>> {
        if self.test(Token::LParent)? {
            let select = self.parse_select()?;
            self.match_expected(Token::RParent)?;
            Ok(select.into())
        } else {
            let name = self.match_id()?;
            let from = self.factory.new_from_clause(name);
            Ok(ArenaBox::<dyn Statement>::from(from).into())
        }
    }

    fn parse_cte(&mut self) -> Result<ArenaBox<dyn Statement>> {
        self.match_expected(Token::With)?;

        let mut items = arena_vec!(&self.factory.arena);
        loop {
            let cte_name = self.match_id()?;
            let mut columns = arena_vec!(&self.factory.arena);
            if self.test(Token::LParent)? {
                loop {
                    let col_name = self.match_id()?;
                    columns.push(col_name);
                    if !self.test(Token::Comma)? {
                        break;
                    }
                }
                self.match_expected(Token::RParent)?;
            }

            self.match_expected(Token::As)?;
            self.match_expected(Token::LParent)?;
            let reference = ArenaBox::<dyn Relation>::from(self.parse_select()?);
            self.match_expected(Token::RParent)?;

            let item = CteWithItem {
                name: cte_name,
                columns,
                reference,
            };
            items.push(item);
            if !self.test(Token::Comma)? {
                break;
            }
        }

        let query = ArenaBox::<dyn Relation>::from(self.parse_select()?);
        Ok(self.factory.new_common_table_expressions(items, query).into())
    }

    // delete from t
    // [where expr]
    // [order by expr...]
    // [limit literal]
    //
    // delete t1,t2... from t1 join ...
    // [where expr]
    //
    // delete from t1, t2 ... using t1 join ...
    // [where expr]
    fn parse_delete(&mut self) -> Result<ArenaBox<dyn Statement>> {
        self.match_expected(Token::Delete)?;
        //let mut multi_delete = false;

        let mut names = arena_vec!(&self.factory.arena);
        if !self.test(Token::From)? {
            loop {
                names.push(self.match_id()?);
                if !self.test(Token::Comma)? {
                    break;
                }
            }
            self.match_expected(Token::From)?;
            //multi_delete = true;
        }

        let mut relation = Option::<ArenaBox<dyn Relation>>::None;
        if names.is_empty() {
            loop {
                names.push(self.match_id()?);
                if !self.test(Token::Comma)? {
                    break;
                }
            }

            if self.test(Token::Using)? {
                relation = Some(self.parse_join_only_use_table_rel()?);
            }
        } else {
            relation = Some(self.parse_join_only_use_table_rel()?);
        }
        let mut node = self.factory.new_delete(names, relation);

        node.where_clause = if self.test(Token::Where)? {
            Some(self.parse_expr()?)
        } else {
            None
        };

        if node.relation.is_none() {
            if self.test(Token::Order)? {
                self.match_expected(Token::By)?;
                self.parse_order_by_clause(&mut node.order_by_clause)?;
            }

            if self.test(Token::Limit)? {
                node.limit_clause = Some(self.parse_expr()?);
            }
        }

        Ok(node.into())
    }

    fn parse_update(&mut self) -> Result<ArenaBox<dyn Statement>> {
        self.match_expected(Token::Update)?;

        let rel = self.parse_join_only_use_table_rel()?;

        let mut node = self.factory.new_update(rel);
        self.match_expected(Token::Set)?;
        self.parse_assignment_at_least_one(&mut node.assignments)?;

        if self.test(Token::Where)? {
            node.where_clause = Some(self.parse_expr()?);
        }

        if !node.relation.as_any().is::<FromClause>() {
            return Ok(node.into());
        }

        if self.test(Token::Order)? {
            self.match_expected(Token::By)?;
            self.parse_order_by_clause(&mut node.order_by_clause)?;
        }

        if self.test(Token::Limit)? {
            node.limit_clause = Some(self.parse_expr()?);
        }
        Ok(node.into())
    }

    fn parse_join_only_use_table_rel(&mut self) -> Result<ArenaBox<dyn Relation>> {
        let mut rel = self.match_table_rel()?;
        loop {
            match self.parse_join_op()? {
                Some(op) => {
                    let rhs = self.match_table_rel()?;
                    self.match_expected(Token::On)?;
                    let on_clause = self.parse_expr()?;
                    let join_node = self.factory.new_join_clause(op, rel.clone(),
                                                                 rhs.clone(), on_clause);
                    rel = ArenaBox::<dyn Statement>::from(join_node).into();
                }
                None => break
            }
        }
        Ok(rel)
    }

    fn match_table_rel(&mut self) -> Result<ArenaBox<dyn Relation>> {
        let name = self.match_id()?;
        let mut rel = self.factory.new_from_clause(name);
        let alias = self.match_alias()?;
        rel.alias_as(alias);
        let stmt: ArenaBox<dyn Statement> = rel.into();
        Ok(stmt.into())
    }

    fn parse_select(&mut self) -> Result<ArenaBox<dyn Statement>> {
        self.match_expected(Token::Select)?;
        let distinct = self.test(Token::Distinct)?;

        let mut node = self.factory.new_select(distinct);
        loop {
            let item = self.parse_select_col_item()?;
            node.columns.push(item);
            if !self.test(Token::Comma)? {
                break;
            }
        }

        if self.test(Token::From)? {
            if self.test(Token::LParent)? {
                node.from_clause = Some(self.parse_relation()?);
                self.match_expected(Token::RParent)?;
            } else {
                node.from_clause = Some(self.parse_relation()?)
            }
            if let Some(clause) = node.from_clause.as_mut() {
                let alias = self.match_alias()?;
                clause.alias_as(alias);

                match self.parse_join_op()? {
                    Some(join_op) => {
                        let mut rhs = self.parse_relation()?;
                        rhs.alias_as(self.match_alias()?);
                        self.match_expected(Token::On)?;
                        let on_clause = self.parse_expr()?;
                        let join_node = self.factory.new_join_clause(join_op, clause.clone(), rhs,
                                                                     on_clause);
                        node.from_clause = Some(ArenaBox::<dyn Statement>::from(join_node).into())
                    }
                    None => {}
                }
            }

            if self.test(Token::Where)? {
                let expr = self.parse_expr()?;
                node.where_clause = Some(expr);
            }

            if self.test(Token::Group)? {
                self.match_expected(Token::By)?;
                self.parse_expr_at_least_one(&mut node.group_by_clause)?;

                if self.test(Token::Having)? {
                    node.having_clause = Some(self.parse_expr()?);
                }
            }

            if self.test(Token::Order)? {
                self.match_expected(Token::By)?;
                self.parse_order_by_clause(&mut node.order_by_clause)?;
            }

            if self.test(Token::Limit)? {
                node.limit_clause = Some(self.parse_expr()?);
                if self.test(Token::Offset)? {
                    node.offset_clause = Some(self.parse_expr()?);
                }
            }
        }

        if let Some(set_op) = self.parse_set_op()? {
            let lhs: ArenaBox<dyn Statement> = node.into();
            let rhs: ArenaBox<dyn Relation> = self.parse_select()?.into();
            let node = self.factory.new_collection(set_op, lhs.into(), rhs);
            Ok(node.into())
        } else {
            Ok(node.into())
        }
    }

    fn parse_set_op(&mut self) -> Result<Option<SetOp>> {
        if self.test(Token::Union)? {
            if self.test(Token::All)? {
                Ok(Some(SetOp::UnionAll))
            } else if self.test(Token::Distinct)? {
                Ok(Some(SetOp::Union))
            } else {
                Ok(Some(SetOp::Union))
            }
        } else {
            Ok(None)
        }
    }

    fn parse_join_op(&mut self) -> Result<Option<JoinOp>> {
        let op = match self.peek().token {
            Token::Left => {
                self.move_next()?;
                self.test(Token::Outer)?;
                self.match_expected(Token::Join)?;
                Some(JoinOp::LeftOuterJoin)
            }
            Token::Right => {
                self.move_next()?;
                self.test(Token::Outer)?;
                self.match_expected(Token::Join)?;
                Some(JoinOp::RightOuterJoin)
            }
            Token::Inner => {
                self.move_next()?;
                self.match_expected(Token::Join)?;
                Some(JoinOp::InnerJoin)
            }
            Token::Cross => {
                self.move_next()?;
                self.match_expected(Token::Join)?;
                Some(JoinOp::CrossJoin)
            }
            Token::Join => {
                self.move_next()?;
                Some(JoinOp::CrossJoin)
            }
            _ => None
        };
        Ok(op)
    }

    fn parse_assignment_at_least_one(&mut self, list: &mut ArenaVec<Assignment>) -> Result<()> {
        loop {
            let id = self.match_id()?;
            let name = if self.test(Token::Dot)? {
                self.match_id()?
            } else {
                ArenaStr::default()
            };
            self.match_expected(Token::Eq)?;
            let expr = self.parse_expr()?;
            let assignment = Assignment {
                lhs: FullyQualifiedName {
                    prefix: if name.is_empty() { ArenaStr::default() } else { id.clone() },
                    suffix: if name.is_empty() { id } else { name },
                },
                rhs: expr,
            };
            list.push(assignment);

            if !self.test(Token::Comma)? {
                break Ok(());
            }
        }
    }

    fn parse_expr_at_least_one(&mut self, list: &mut ArenaVec<ArenaBox<dyn Expression>>) -> Result<()> {
        loop {
            let expr = self.parse_expr()?;
            list.push(expr);
            if !self.test(Token::Comma)? {
                break Ok(());
            }
        }
    }

    fn match_alias(&mut self) -> Result<ArenaStr> {
        if self.test(Token::As)? {
            self.match_id()
        } else if matches!(self.peek().token, Token::Id(_)) {
            self.match_id()
        } else {
            Ok(ArenaStr::default())
        }
    }

    fn parse_select_col_item(&mut self) -> Result<SelectColumnItem> {
        let item = match self.peek().token {
            Token::Star => {
                self.move_next()?;
                SelectColumnItem {
                    expr: SelectColumn::Star,
                    alias: ArenaStr::default(),
                }
            }
            _ => {
                SelectColumnItem {
                    expr: SelectColumn::Expr(self.parse_expr()?),
                    alias: self.match_alias()?,
                }
            }
        };
        Ok(item)
    }

    fn parse_order_by_clause(&mut self, list: &mut ArenaVec<OrderClause>) -> Result<()> {
        loop {
            let key = self.parse_expr()?;
            let mut item = OrderClause {
                key,
                ordering: SqlOrdering::Asc,
            };
            if self.test(Token::Asc)? {
                // ignore...
            } else if self.test(Token::Desc)? {
                item.ordering = SqlOrdering::Desc;
            }
            list.push(item);
            if !self.test(Token::Comma)? {
                break Ok(());
            }
        }
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
        let mut expr = self.parse_suffixed()?;

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
        match part.token.clone() {
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
            Token::Question => {
                self.move_next()?;
                let placeholder = self.factory.new_placeholder(self.placeholder_order);
                self.placeholder_order += 1;
                Ok(placeholder.into())
            }
            _ => self.parse_primary()
        }
    }

    fn parse_suffixed(&mut self) -> Result<ArenaBox<dyn Expression>> {
        let start_pos = self.lexer.current_position();

        let expr = self.parse_simple()?;
        let not = self.test(Token::Not)?;
        match self.peek().token.clone() {
            Token::Between => {
                self.move_next()?;
                let lower = self.parse_simple()?;
                self.match_expected(Token::And)?;
                let upper = self.parse_simple()?;
                Ok(self.factory.new_between_and(expr, lower, upper, not).into())
            }
            Token::In => {
                self.move_next()?;
                self.match_expected(Token::LParent)?;
                if self.peek().token == Token::Select {
                    let rel = ArenaBox::<dyn Relation>::from(self.parse_select()?);
                    let node = self.factory.new_in_relation(expr, rel, not);
                    self.match_expected(Token::RParent)?;
                    Ok(node.into())
                } else {
                    let mut node = self.factory.new_in_literal_set(expr, not);
                    loop {
                        let elem = self.parse_expr()?;
                        node.set.push(elem);
                        if !self.test(Token::Comma)? {
                            break;
                        }
                    }
                    self.match_expected(Token::RParent)?;
                    Ok(node.into())
                }
            }
            Token::Is => {
                if not {
                    return Err(self.concat_syntax_error(start_pos, "Unexpected prefix NOT".to_string()));
                }
                self.move_next()?;
                let node = if self.test(Token::Not)? {
                    self.match_expected(Token::Null)?;
                    self.factory.new_unary_expr(Operator::IsNotNull, expr)
                } else {
                    self.match_expected(Token::Null)?;
                    self.factory.new_unary_expr(Operator::IsNull, expr)
                };
                Ok(node.into())
            }
            _ => Ok(expr)
        }
    }

    fn parse_primary(&mut self) -> Result<ArenaBox<dyn Expression>> {
        let start_pos = self.lexer.current_position();
        match self.peek().token.clone() {
            Token::LParent => {
                self.move_next()?;
                let expr = self.parse_expr()?;
                self.match_expected(Token::RParent)?;
                Ok(expr)
            }

            Token::Id(symbol) => {
                self.move_next()?;
                if self.test(Token::Dot)? {
                    let suffix = self.match_id()?;
                    Ok(self.factory.new_fully_qualified_name(symbol, suffix).into())
                } else if self.test(Token::LParent)? {
                    let distinct = self.test(Token::Distinct)?;
                    let mut call = self.factory.new_call_function(symbol, distinct);
                    if self.test(Token::Star)? {
                        call.in_args_star = true;
                        self.match_expected(Token::RParent)?;
                        return Ok(call.into());
                    }
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
            }
            Token::Case => {
                self.move_next()?;
                let matching = if self.peek().token != Token::When {
                    Some(self.parse_expr()?)
                } else {
                    None
                };
                let mut when_clause = arena_vec!(&self.factory.arena);
                loop {
                    self.match_expected(Token::When)?;
                    let expected = self.parse_expr()?;
                    self.match_expected(Token::Then)?;
                    let then = self.parse_expr()?;

                    when_clause.push(WhenClause {
                        expected,
                        then,
                    });
                    if self.peek().token != Token::When {
                        break;
                    }
                }

                let else_clause = if self.test(Token::Else)? {
                    Some(self.parse_expr()?)
                } else {
                    None
                };
                Ok(self.factory.new_case_when(matching, when_clause, else_clause).into())
            }
            _ => {
                let message = format!("Unexpected primary expression, expected: {:?}",
                                      self.peek().token);
                Err(self.concat_syntax_error(start_pos, message))
            }
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
    use std::ops::DerefMut;

    use crate::base::{Arena, ArenaMut};
    use crate::sql::ast::Factory;
    use crate::sql::serialize::serialize_yaml_to_string;
    use crate::storage::MemorySequentialFile;

    use super::*;

    #[test]
    fn sanity() -> Result<()> {
        let arena = Arena::new_ref();
        let factory = Factory::new(&arena.get_mut());
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
        let arena = Arena::new_ref();
        let yaml = parse_all_to_yaml(arena.get_mut(), "insert into table t1(a,b,c) values(1,2,3)")?;
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
    fn simple_select() -> Result<()> {
        let arena = Arena::new_ref();
        let yaml = parse_all_to_yaml(arena.get_mut(), "select 1 as a")?;
        println!("{}", yaml);
        assert_eq!("Select:
  distinct: false
  columns:
    - expr:
        IntLiteral: 1
      alias: a
", yaml);
        Ok(())
    }

    #[test]
    fn simple_select_with_from_clause() -> Result<()> {
        let arena = Arena::new_ref();
        let yaml = parse_all_to_yaml(arena.get_mut(), "select a as c1,b,c from t")?;
        println!("{}", yaml);
        assert_eq!("Select:
  distinct: false
  columns:
    - expr:
        Identifier: a
      alias: c1
    - expr:
        Identifier: b
    - expr:
        Identifier: c
  from:
    FromClause:
      name: t
", yaml);
        Ok(())
    }

    #[test]
    fn select_with_from_join() -> Result<()> {
        let arena = Arena::new_ref();
        let yaml = parse_all_to_yaml(arena.get_mut(), "select * from t t1 left join tt t2 on t1.id = t2.id")?;
        println!("{}", yaml);
        assert_eq!("Select:
  distinct: false
  columns:
    - expr: *
  from:
    JoinClause:
      op: LEFT OUTER JOIN
      lhs:
        FromClause:
          name: t
          alias: t1
      rhs:
        FromClause:
          name: tt
          alias: t2
      on_clause:
        BinaryExpression:
          op: Eq(=)
          lhs:
            FullQualifiedName: t1.id
          rhs:
            FullQualifiedName: t2.id
", yaml);
        Ok(())
    }

    #[test]
    fn select_with_from_join_subquery() -> Result<()> {
        let arena = Arena::new_ref();
        let sql = "select *
from a t1
left join
(select id from b) t2
on t1.id = t2.id
        ";
        let yaml = parse_all_to_yaml(arena.get_mut(), sql)?;
        println!("{}", yaml);
        assert_eq!("Select:
  distinct: false
  columns:
    - expr: *
  from:
    JoinClause:
      op: LEFT OUTER JOIN
      lhs:
        FromClause:
          name: a
          alias: t1
      rhs:
        Select:
          distinct: false
          columns:
            - expr:
                Identifier: id
          from:
            FromClause:
              name: b
          alias: t2
      on_clause:
        BinaryExpression:
          op: Eq(=)
          lhs:
            FullQualifiedName: t1.id
          rhs:
            FullQualifiedName: t2.id
", yaml);
        Ok(())
    }

    #[test]
    fn simple_expr() -> Result<()> {
        let arena = Arena::new_ref();
        let yaml = parse_expr_to_yaml(arena.get_mut(), "col + b")?;
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
        let arena = Arena::new_ref();
        let yaml = parse_expr_to_yaml(arena.get_mut(), "sum(1)")?;
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

    #[test]
    fn parsing_select_issue001() -> Result<()> {
        let zone = Arena::new_val();
        let arena = zone.get_mut();
        let yaml = parse_to_yaml(arena, "select a+1, b - 20 from t1 where a = 1", false)?;
        assert_eq!("Select:
  distinct: false
  columns:
    - expr:
        BinaryExpression:
          op: Add(+)
          lhs:
            Identifier: a
          rhs:
            IntLiteral: 1
    - expr:
        BinaryExpression:
          op: Sub(-)
          lhs:
            Identifier: b
          rhs:
            IntLiteral: 20
  from:
    FromClause:
      name: t1
  where:
    BinaryExpression:
      op: Eq(=)
      lhs:
        Identifier: a
      rhs:
        IntLiteral: 1
", yaml);
        Ok(())
    }

    #[test]
    fn parsing_cte_sanity() -> Result<()> {
        let zone = Arena::new_val();
        let arena = zone.get_mut();

        const SQL: &str = "with
            cte1 as (select * from a),
            cte2(col1, col2) as (select * from b)
        select * from cte1 inner join cte2 on (cte1.id = cte2.id);
        ";
        let yaml = parse_all_to_yaml(arena, SQL)?;
        assert_eq!("CommonTableExpressions:
  with_clause:
    - name: cte1
      reference:
        Select:
          distinct: false
          columns:
            - expr: *
          from:
            FromClause:
              name: a
    - name: cte2
      columns:
        - col1
        - col2
      reference:
        Select:
          distinct: false
          columns:
            - expr: *
          from:
            FromClause:
              name: b
  query:
    Select:
      distinct: false
      columns:
        - expr: *
      from:
        JoinClause:
          op: INNER JOIN
          lhs:
            FromClause:
              name: cte1
          rhs:
            FromClause:
              name: cte2
          on_clause:
            BinaryExpression:
              op: Eq(=)
              lhs:
                FullQualifiedName: cte1.id
              rhs:
                FullQualifiedName: cte2.id
", yaml);
        Ok(())
    }

    #[test]
    fn parsing_delete_sanity() -> Result<()> {
        let zone = Arena::new_val();
        let arena = zone.get_mut();

        let yaml = parse_all_to_yaml(arena.clone(), "DELETE FROM t1")?;
        assert_eq!("Delete:
  names:
     - t1\n", yaml);

        let yaml = parse_all_to_yaml(arena.clone(), "DELETE FROM t1 WHERE id = 0")?;
        //println!("{yaml}");
        assert_eq!("Delete:
  names:
     - t1
  where:
    BinaryExpression:
      op: Eq(=)
      lhs:
        Identifier: id
      rhs:
        IntLiteral: 0\n", yaml);

        let yaml = parse_all_to_yaml(arena.clone(), "DELETE FROM t1, t2 USING t1 JOIN t2 ON(t1.id = t2.id) WHERE t1.name = \"xxx\"")?;
        // println!("{yaml}");
        assert_eq!("Delete:
  names:
     - t1
     - t2
  relation:
    JoinClause:
      op: CROSS JOIN
      lhs:
        FromClause:
          name: t1
      rhs:
        FromClause:
          name: t2
      on_clause:
        BinaryExpression:
          op: Eq(=)
          lhs:
            FullQualifiedName: t1.id
          rhs:
            FullQualifiedName: t2.id
  where:
    BinaryExpression:
      op: Eq(=)
      lhs:
        FullQualifiedName: t1.name
      rhs:
        StrLiteral: xxx\n", yaml);
        Ok(())
    }

    #[test]
    fn parsing_update_sanity() -> Result<()> {
        let zone = Arena::new_val();
        let arena = zone.get_mut();

        let yaml = parse_all_to_yaml(arena.clone(), "update t1 set a = 1 where a = 1")?;
        //println!("{}", yaml);
        assert_eq!("Update:
  relation:
    FromClause:
      name: t1
  assignments:
    - lhs: a
      rhs:
        IntLiteral: 1
  where:
    BinaryExpression:
      op: Eq(=)
      lhs:
        Identifier: a
      rhs:
        IntLiteral: 1
", yaml);

        let yaml = parse_all_to_yaml(arena.clone(), "update t1 inner join t2 on (t1.id = t2.id) set t1.a = 1, t2.a = 2 where t1.a = 1")?;
        // println!("{}", yaml);
        assert_eq!("Update:
  relation:
    JoinClause:
      op: INNER JOIN
      lhs:
        FromClause:
          name: t1
      rhs:
        FromClause:
          name: t2
      on_clause:
        BinaryExpression:
          op: Eq(=)
          lhs:
            FullQualifiedName: t1.id
          rhs:
            FullQualifiedName: t2.id
  assignments:
    - lhs: t1.a
      rhs:
        IntLiteral: 1
    - lhs: t2.a
      rhs:
        IntLiteral: 2
  where:
    BinaryExpression:
      op: Eq(=)
      lhs:
        FullQualifiedName: t1.a
      rhs:
        IntLiteral: 1
", yaml);

        Ok(())
    }

    fn parse_all_to_yaml(arena: ArenaMut<Arena>, sql: &str) -> Result<String> {
        parse_to_yaml(arena, sql, false)
    }

    fn parse_expr_to_yaml(arena: ArenaMut<Arena>, sql: &str) -> Result<String> {
        parse_to_yaml(arena, sql, true)
    }

    fn parse_to_yaml(arena: ArenaMut<Arena>, sql: &str, parse_expr_only: bool) -> Result<String> {
        let factory = Factory::new(&arena);
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