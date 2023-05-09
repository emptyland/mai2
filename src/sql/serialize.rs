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

    fn emit_item<T: Statement + ?Sized>(&mut self, node: &mut T) {
        self.emit_child("- ", node)
    }

    fn emit_child<T: Statement + ?Sized>(&mut self, prefix: &str, node: &mut T) {
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
                            self.emit_item(value.deref_mut());
                        }
                    }
                }
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
                        self.emit_item(arg.deref_mut());
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryWritableFile;

    #[test]
    fn sanity() {
        let mut wr = MemoryWritableFile::new();
        let mut yaml = YamlWriter::new(&mut wr, 0);

        let factory = Factory::new();
        let mut ast = factory.new_create_table(factory.str("aaa"), true);

        ast.accept(&mut yaml);
        let str = String::from_utf8_lossy(wr.buf()).to_string();
        println!("{}", str);
    }
}