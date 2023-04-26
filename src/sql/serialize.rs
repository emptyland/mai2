use std::io::Write;
use crate::base::ArenaStr;
use crate::sql::ast::*;

pub struct YamlWriter<'a> {
    writer: &'a mut dyn Write,
    indent: u32,
}

impl <'a> YamlWriter<'a> {
    pub fn new(writer: &'a mut dyn Write, indent: u32) -> Self {
        Self {
            writer,
            indent,
        }
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

pub fn serialize_yaml_to_string(ast: &mut dyn Statement) -> String {
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

macro_rules! indent {
    {$self:ident; $($stmt:stmt);* $(;)?} => {
        $self.indent += 1;
        $($stmt)*
        $self.indent -= 1;
    }
}

impl Visitor for YamlWriter<'_> {
    fn visit_create_table(&mut self, this: &mut CreateTable) {
        emit!(self, "CreateTable:");
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
                        emit!(self, "default_val:");
                        expr.accept(self);
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
        emit!(self, "DropTable");
        indent! {self;
            emit!(self, "table_name: {}", this.table_name);
            emit!(self, "if_exists: {}", this.if_exists);
        }
    }

    fn visit_insert_into_table(&mut self, this: &mut InsertIntoTable) {
        emit!(self, "InsertIntoTable");
        indent! {self;
            emit!(self, "table_name: {}", this.table_name);
            if this.columns_name.len() > 0 {
                emit!(self, "columns_name:");
            };
            indent! {self;
                for i in 0..this.columns_name.len() {
                    emit!(self, "- {}", this.columns_name[i]);
                }
            };
            emit!(self, "values:");
        }
    }

    fn visit_identifier(&mut self, this: &mut Identifier) {
        emit!(self, "Identifier: {}", this.symbol);
    }

    fn visit_full_qualified_name(&mut self, this: &mut FullyQualifiedName) {
        emit!(self, "FullQualifiedName: {}.{}", this.prefix, this.suffix);
    }

    fn visit_unary_expression(&mut self, this: &mut UnaryExpression) {
        emit!(self, "UnaryExpression");
        indent! {self;
            emit!(self, "op: {}", this.op());
            emit!(self, "operand:");
            indent! {self;
                this.operands_mut()[0].accept(self);
            }
        }
    }

    fn visit_binary_expression(&mut self, this: &mut BinaryExpression) {
        this.lhs_mut().accept(self);
        this.rhs_mut().accept(self);

        todo!()
    }

    fn visit_call_function(&mut self, this: &mut CallFunction) {
        emit!(self, "CallFunction:");
        indent! {self;
            emit!(self, "name: {}", this.name);
            emit!(self, "in_args_star: {}", this.in_args_star);
            if this.args.len() > 0 {
                emit!(self, "args:");
                indent! {self;
                    for arg in this.args.as_mut_slice() {
                        self.emit_prefix("- ");
                        arg.accept(self);
                    }
                }
            }
        }
    }

    fn visit_int_literal(&mut self, this: &mut Literal<i64>) {
        emit!(self, "IntLiteral: {}", this.data);
    }

    fn visit_float_literal(&mut self, this: &mut Literal<f64>) {
        emit!(self, "FloatLiteral: {}", this.data);
    }

    fn visit_str_literal(&mut self, this: &mut Literal<ArenaStr>) {
        emit!(self, "StrLiteral: {}", this.data);
    }

    fn visit_null_literal(&mut self, _this: &mut Literal<()>) {
        emit!(self, "NullLiteral: NULL");
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