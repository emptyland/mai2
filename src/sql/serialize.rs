use std::io::Write;
use std::ops::DerefMut;

use crate::base::ArenaStr;
use crate::sql::ast::*;

pub struct YamlWriter<'a> {
    writer: &'a mut dyn Write,
    indent: u32,
}

impl<'a> YamlWriter<'a> {
    pub fn new(writer: &'a mut dyn Write, indent: u32) -> Self {
        Self {
            writer,
            indent,
        }
    }

    fn emit_elem<T: Statement + ?Sized>(&mut self, node: &mut T) {
        self.emit_child("- ", node)
    }

    fn emit_child<T: Statement + ?Sized>(&mut self, prefix: &str, node: &mut T) {
        self.emit_prefix(prefix);
        node.accept(self);
    }

    fn emit_expr<T: Expression + ?Sized>(&mut self, prefix: &str, node: &mut T) {
        self.emit_prefix(prefix);
        node.accept(self);
    }

    fn emit_rel<T: Relation + ?Sized>(&mut self, prefix: &str, node: &mut T) {
        self.emit_prefix(prefix);
        node.accept(self);
    }

    fn emit_prefix(&mut self, prefix: &str) {
        self.emit_indent();
        self.writer.write(prefix.as_bytes()).unwrap();
    }

    fn emit_indent(&mut self) {
        for _ in 0..self.indent {
            self.writer.write("  ".as_bytes()).unwrap();
        }
    }
}

pub fn serialize_yaml_to_string<T: Statement + ?Sized>(ast: &mut T) -> String {
    let mut wr = Vec::<u8>::new();
    let mut encoder = YamlWriter::new(&mut wr, 0);
    ast.accept(&mut encoder);
    String::from_utf8_lossy(&wr).to_string()
}

pub fn serialize_expr_to_string<T: Expression + ?Sized>(ast: &mut T) -> String {
    let mut wr = Vec::<u8>::new();
    let mut encoder = YamlWriter::new(&mut wr, 0);
    ast.accept(&mut encoder);
    String::from_utf8_lossy(&wr).to_string()
}

macro_rules! emit {
    ($self:ident, $($arg:tt)*) => {
        $self.emit_indent();
        writeln!($self.writer, $($arg)+).unwrap();
    }
}

macro_rules! emit_header {
    ($self:ident, $($arg:tt)*) => {
        writeln!($self.writer, $($arg)+).unwrap();
    }
}

macro_rules! indent {
    {$self:ident; $($stmt:stmt);* $(;)?} => {
        $self.indent += 1;
        $($stmt)*
        $self.indent -= 1;
    }
}

impl Visitor for YamlWriter<'_> {
    fn visit_create_table(&mut self, this: &mut CreateTable) {
        emit_header!(self, "CreateTable:");
        self.indent += 1;
        emit!(self, "table_name: {}", this.table_name);
        emit!(self, "if_not_exists: {}", this.if_not_exists);
        if this.columns.len() > 0 {
            emit!(self, "columns:");
        }
        indent! { self;
            for i in 0..this.columns.len() {
                let col = &mut this.columns[i];
                emit!(self, "- name: {}", col.name);
                indent! {self;
                    emit!(self, "type: {}", 1);
                    emit!(self, "not_null: {}", col.not_null);
                    emit!(self, "auto_increment: {}", col.auto_increment);
                    if let Some(expr) = &mut col.default_val {
                        self.emit_child("default_val: ", expr.deref_mut());
                    }
                }
            }
        }
        if this.primary_keys.len() > 0 {
            emit!(self, "primary_key:");
        }
        indent! {self;
            for i in 0..this.primary_keys.len() {
                let key = &this.primary_keys[i];
                emit!(self, "- {}", key);
            }
        }
        self.indent -= 1;
    }

    fn visit_drop_table(&mut self, this: &mut DropTable) {
        emit_header!(self, "DropTable");
        indent! {self;
            emit!(self, "table_name: {}", this.table_name);
            emit!(self, "if_exists: {}", this.if_exists);
        }
    }

    fn visit_create_index(&mut self, this: &mut CreateIndex) {
        emit_header!(self, "CreateIndex");
        indent! {self;
            emit!(self, "name: {}", this.name);
            emit!(self, "table_name: {}", this.table_name);
            emit!(self, "key_parts:");
            indent! {self;
                for name in this.key_parts.iter_mut() {
                    emit!(self, "- {}", name);
                }
            };
        }
    }

    fn visit_drop_index(&mut self, this: &mut DropIndex) {
        emit_header!(self, "DropIndex");
        indent! {self;
            emit!(self, "name: {}", this.name);
            emit!(self, "table_name: {}", this.table_name);
        }
    }

    fn visit_insert_into_table(&mut self, this: &mut InsertIntoTable) {
        emit_header!(self, "InsertIntoTable");
        indent! {self;
            emit!(self, "table_name: {}", this.table_name);
            if this.columns_name.len() > 0 {
                emit!(self, "columns_name:");
                indent! {self;
                    for name in &this.columns_name {
                        emit!(self, "- {}", name);
                    }
                }
            };
            emit!(self, "values:");
            indent! {self;
                for row in this.values.iter_mut() {
                    self.emit_prefix("- row:\n");
                    indent!{ self;
                        for value in row.iter_mut() {
                            self.emit_elem(value.deref_mut());
                        }
                    }
                }
            };
        }
    }

    fn visit_collection(&mut self, _this: &mut Collection) {
        todo!()
    }

    fn visit_common_table_expressions(&mut self, this: &mut CommonTableExpressions) {
        emit_header!(self, "CommonTableExpressions:");
        indent! { self;
            self.emit_prefix("with_clause:\n");
            indent! { self;
                for item in this.with_clause.iter_mut() {
                    emit!(self, "- name: {}", item.name);
                    if !item.columns.is_empty() {
                        self.emit_prefix("  columns:\n");
                        indent! { self;
                            for col in &item.columns {
                                emit!(self, "  - {}", col);
                            }
                        }
                    }
                    self.emit_prefix("  reference:\n");
                    indent! { self;
                        self.indent += 1;
                        self.emit_rel("", item.reference.deref_mut());
                        self.indent -= 1;
                    }
                }
            };
            self.emit_prefix("query:\n");
            indent! { self;
                self.emit_rel("", this.query.deref_mut());
            }
        }
    }

    fn visit_select(&mut self, this: &mut Select) {
        emit_header!(self, "Select:");
        indent! { self;
            emit!(self, "distinct: {}", this.distinct);
            emit!(self, "columns:");
            for col in this.columns.iter_mut() {
                indent!{ self;
                    match &mut col.expr {
                        SelectColumn::Star => { emit!(self, "- expr: *");}
                        SelectColumn::SuffixStar(prefix) => {
                            emit!(self, "- expr: {}.*", prefix);
                        }
                        SelectColumn::Expr(expr) => {
                            emit!(self, "- expr:");
                            self.indent += 2;
                            self.emit_expr("", expr.deref_mut());
                            self.indent -= 2;
                        }
                    };
                    if !col.alias.is_empty() {
                        indent! { self;
                            emit!(self, "alias: {}", col.alias);
                        }
                    }
                }
            };
            if let Some(from) = &mut this.from_clause {
                emit!(self, "from:");
                indent! { self;
                    self.emit_rel("", from.deref_mut());
                }
            };
            if let Some(expr) = &mut this.where_clause {
                emit!(self, "where:");
                indent! { self;
                    self.emit_expr("", expr.deref_mut());
                }
            };
            if !this.group_by_clause.is_empty() {
                emit!(self, "group_by: ");
                for expr in this.group_by_clause.iter_mut() {
                    self.emit_elem(expr.deref_mut())
                }
            };
            if !this.order_by_clause.is_empty() {
                emit!(self, "order_by: ");
                for expr in this.order_by_clause.iter_mut() {
                    self.emit_elem(expr.deref_mut())
                }
            };
            if let Some(limit) = &mut this.limit_clause {
                self.emit_expr("limit: ", limit.deref_mut());
                if let Some(offset) = &mut this.offset_clause {
                    self.emit_expr("offset: ", offset.deref_mut());
                }
            };
            if !this.alias.is_empty() {
                emit!(self, "alias: {}", this.alias);
            }
        }
    }

    fn visit_from_clause(&mut self, this: &mut FromClause) {
        emit_header!(self, "FromClause:");
        indent! { self;
            emit!(self, "name: {}", this.name);
            if !this.alias.is_empty() {
                emit!(self, "alias: {}", this.alias);
            }
        }
    }

    fn visit_join_clause(&mut self, this: &mut JoinClause) {
        emit_header!(self, "JoinClause:");
        indent! { self;
            emit!(self, "op: {}", this.op);
            emit!(self, "lhs:");
            indent! { self;
                self.emit_rel("", this.lhs.deref_mut());
            };
            emit!(self, "rhs:");
            indent! { self;
                self.emit_rel("", this.rhs.deref_mut());
            };
            emit!(self, "on_clause:");
            indent! { self;
                self.emit_expr("", this.on_clause.deref_mut());
            };
        }
    }

    fn visit_identifier(&mut self, this: &mut Identifier) {
        emit_header!(self, "Identifier: {}", this.symbol);
    }

    fn visit_full_qualified_name(&mut self, this: &mut FullyQualifiedName) {
        emit_header!(self, "FullQualifiedName: {}.{}", this.prefix, this.suffix);
    }

    fn visit_unary_expression(&mut self, this: &mut UnaryExpression) {
        emit_header!(self, "UnaryExpression:");
        indent! {self;
            emit!(self, "op: {}", this.op());
            emit!(self, "operand:");
            indent! {self;
                this.operands_mut()[0].accept(self);
            }
        }
    }

    fn visit_binary_expression(&mut self, this: &mut BinaryExpression) {
        emit_header!(self, "BinaryExpression:");
        indent! {self;
            emit!(self, "op: {}", this.op());
            emit!(self, "lhs:");
            indent! {self;
                self.emit_child("", this.lhs_mut().deref_mut());
            };
            emit!(self, "rhs:");
            indent! {self;
                self.emit_child("", this.rhs_mut().deref_mut());
            }
        }
    }

    fn visit_in_literal_set(&mut self, _this: &mut InLiteralSet) {
        todo!()
    }

    fn visit_in_relation(&mut self, _this: &mut InRelation) {
        todo!()
    }

    fn visit_call_function(&mut self, this: &mut CallFunction) {
        emit_header!(self, "CallFunction:");
        indent! {self;
            emit!(self, "name: {}", this.callee_name);
            emit!(self, "distinct: {}", this.distinct);
            emit!(self, "in_args_star: {}", this.in_args_star);
            if this.args.len() > 0 {
                emit!(self, "args:");
                indent! {self;
                    for arg in this.args.iter_mut() {
                        self.emit_elem(arg.deref_mut());
                    }
                }
            }
        }
    }

    fn visit_int_literal(&mut self, this: &mut Literal<i64>) {
        emit_header!(self, "IntLiteral: {}", this.data);
    }

    fn visit_float_literal(&mut self, this: &mut Literal<f64>) {
        emit_header!(self, "FloatLiteral: {}", this.data);
    }

    fn visit_str_literal(&mut self, this: &mut Literal<ArenaStr>) {
        emit_header!(self, "StrLiteral: {}", this.data);
    }

    fn visit_null_literal(&mut self, _this: &mut Literal<()>) {
        emit_header!(self, "NullLiteral: NULL");
    }

    fn visit_placeholder(&mut self, this: &mut Placeholder) {
        emit_header!(self, "Placeholder: {}", this.order);
    }

    fn visit_fast_access_hint(&mut self, this: &mut FastAccessHint) {
        emit_header!(self, "FastAccessHint: {}", this.offset);
    }
}

#[cfg(test)]
mod tests {
    use crate::base::Arena;
    use crate::storage::MemoryWritableFile;

    use super::*;

    #[test]
    fn sanity() {
        let mut wr = MemoryWritableFile::new();
        let mut yaml = YamlWriter::new(&mut wr, 0);

        let arena = Arena::new_val();
        let factory = Factory::new(&arena.get_mut());
        let mut ast = factory.new_create_table(factory.str("aaa"), true);

        ast.accept(&mut yaml);
        let str = String::from_utf8_lossy(wr.buf()).to_string();
        println!("{}", str);
    }
}