use std::io;
use std::io::Write;
use std::ops::{Add, Sub, Mul, Deref};
use std::sync::Arc;
use crate::{Arena, arena_vec, ArenaMut, ArenaStr, ArenaVec, corrupted_err};
use crate::{Result};
use crate::exec::evaluator::{Context, Evaluator, Value};
use crate::map::ArenaMap;
use crate::sql::ast;
use crate::sql::ast::{BinaryExpression, CallFunction, CaseWhen, Expression, FastAccessHint, FullyQualifiedName, Identifier, Literal, Operator, Visitor};
use crate::status::Corrupting;

#[derive(Debug, PartialEq)]
pub enum Bytecode {
    // 0x1x
    Ldn(ArenaStr), // load by name
    Ldfn(ArenaStr, ArenaStr), // load by full-name
    Ldfa(usize), // load by fast-access hint
    Ldi(i64), // load const i64
    Ldsz(ArenaStr), // load const str
    Ldsf(f32), // load const f32
    Lddf(f64), // load const f64
    Ldnul, // load NULL

    // 0x2x
    Add,
    Sub,
    Mul,
    Div,
    Mod,

    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Like,

    And,
    Or,
    Not,

    // case
    //     when e1 then v1
    //     when e2 then v2
    //     ...
    // else l1
    Jnz(i32), // jump if true
    Jz(i32), // jump if false
    Jne(i32), //
    Jeq(i32), //
    Jlt(i32), //
    Jle(i32), //
    Jgt(i32), //
    Jge(i32), //
    Jmp(i32), // jump always

    // 0xa0
    Call(ArenaStr, usize), // call(callee-name, n-args)

    // 0xef
    End,
}

pub struct Interpreter {
    stack: ArenaVec<Value>,
    env: ArenaVec<Arc<dyn Context>>,
    arena: ArenaMut<Arena>
}

macro_rules! define_arithmetic_op {
    ($name:ident) => {
        fn $name(&mut self) {
            let rhs = Evaluator::require_number(self.stack.pop().as_ref().unwrap());
            let lhs = Evaluator::require_number(self.stack.pop().as_ref().unwrap());
            if lhs.is_null() || rhs.is_null() {
                self.stack.push(Value::Null);
                return;
            }
            match lhs {
                Value::Int(a) => match rhs {
                    Value::Int(b) => self.ret(Value::Int(a.$name(b))),
                    Value::Float(b) => self.ret(Value::Float((a as f64).$name(b))),
                    _ => self.ret(rhs)
                }
                Value::Float(a) => match rhs {
                    Value::Int(b) => self.ret(Value::Float(a.$name(b as f64))),
                    Value::Float(b) => self.ret(Value::Float(a.$name(b))),
                    _ => self.ret(rhs)
                }
                _ => self.ret(lhs)
            }
        }
    };
}

impl Interpreter {

    fn evaluate(&mut self, bca: &mut BytecodeArray) -> Result<Value> {
        use Bytecode::*;
        bca.reset();
        loop {
            match bca.pick() {
                Ldn(name) => self.stack.push(self.resolve(name.as_str())?),
                Ldfn(prefix, suffix) =>
                    self.ret(self.resolve_fully_qualified(prefix.as_str(), suffix.as_str())?),
                Ldfa(i) => self.ret(self.env().fast_access(i).clone()),
                Ldsz(s) => self.ret(Value::Str(s)),
                Ldsf(f) => self.ret(Value::Float(f as f64)),
                Lddf(f) => self.ret(Value::Float(f)),
                Ldnul => self.ret(Value::Null),

                Add => self.add(),
                Sub => self.sub(),
                Mul => self.mul(),
                Div => self.div(),

                Eq => self.cmp(Operator::Eq),
                Ne => self.cmp(Operator::Ne),
                Lt => self.cmp(Operator::Lt),
                Le => self.cmp(Operator::Le),
                Gt => self.cmp(Operator::Gt),
                Ge => self.cmp(Operator::Ge),

                End => break,
                _ => unreachable!()
            }
        }

        Ok(self.stack.pop().unwrap())
    }

    fn resolve(&self, name: &str) -> Result<Value> {
        let v = self.env().resolve(name);
        if v.is_undefined() {
            corrupted_err!("Unresolved symbol: {name}")
        } else {
            Ok(v)
        }
    }

    fn resolve_fully_qualified(&self, prefix: &str, suffix: &str) -> Result<Value> {
        let v = self.env().resolve_fully_qualified(prefix, suffix);
        if v.is_undefined() {
            corrupted_err!("Unresolved symbol: {prefix}.{suffix}")
        } else {
            Ok(v)
        }
    }

    define_arithmetic_op!(add);
    define_arithmetic_op!(sub);
    define_arithmetic_op!(mul);

    fn div(&mut self) {
        let rhs = Evaluator::require_number(self.stack.pop().as_ref().unwrap());
        let lhs = Evaluator::require_number(self.stack.pop().as_ref().unwrap());
        if lhs.is_null() || rhs.is_null() {
            self.stack.push(Value::Null);
            return;
        }
        match lhs {
            Value::Int(a) => match rhs {
                Value::Int(b) =>
                    self.ret(if b == 0 {Value::Null} else { Value::Int(a / b) }),
                Value::Float(b) => self.ret(Value::Float((a as f64) / b)),
                _ => self.ret(rhs)
            }
            Value::Float(a) => match rhs {
                Value::Int(b) => self.ret(Value::Float(a / (b as f64))),
                Value::Float(b) => self.ret(Value::Float(a / b)),
                _ => self.ret(rhs)
            }
            _ => self.ret(lhs)
        }
    }

    fn cmp(&mut self, op: ast::Operator) {
        let rhs = self.stack.pop().unwrap();
        let lhs = self.stack.pop().unwrap();
        let rv = match &lhs {
            Value::Int(_) =>
                Evaluator::eval_compare_number(&lhs, &Evaluator::require_number(&rhs), &op),
            Value::Float(_) =>
                Evaluator::eval_compare_number(&lhs, &Evaluator::require_number(&rhs), &op),
            Value::Str(s) => match &rhs {
                Value::Str(b) =>
                    Evaluator::eval_compare_op(s, b, &op),
                _ =>
                    Evaluator::eval_compare_number(&Evaluator::require_number(&lhs), &rhs, &op),
            }
            Value::Null => Value::Null,
            _ => unreachable!()
        };
        self.ret(rv);
    }

    fn ret(&mut self, value: Value) { self.stack.push(value); }

    fn env(&self) -> &dyn Context {
        self.env.back().unwrap().deref()
    }
}

pub struct BytecodeBuilder {
    bytes: ArenaVec<u8>,
    constants: ArenaVec<(ArenaStr, ArenaStr)>,
    pool: ArenaMap<(ArenaStr, ArenaStr), usize>,
    arena: ArenaMut<Arena>,
}

struct Label {
    pc: usize,
    bind: bool,
}

impl Label {
    fn new() -> Self {
        Self {
            pc: 0,
            bind: false,
        }
    }
}

impl BytecodeBuilder {
    fn new(arena: &ArenaMut<Arena>) -> Self {
        Self {
            bytes: arena_vec!(arena),
            constants: arena_vec!(arena),
            pool: ArenaMap::new(arena),
            arena: arena.clone()
        }
    }

    fn clear(&mut self) {
        self.bytes.clear();
        self.constants.clear();
        self.pool.clear();
    }

    fn emit_j(&mut self, bc: Bytecode, label: &mut Label) {
        let dist = label.pc;
        label.pc = self.bytes.len();

        use Bytecode::*;
        match bc {
            Jnz(_) => self.emit_byte(0xd0), // jump if true
            Jz(_) => self.emit_byte(0xd1), // jump if false
            Jne(_) => self.emit_byte(0xd2), //
            Jeq(_) => self.emit_byte(0xd3), //
            Jlt(_) => self.emit_byte(0xd4), //
            Jle(_) => self.emit_byte(0xd5), //
            Jgt(_) => self.emit_byte(0xd6), //
            Jge(_) => self.emit_byte(0xd7), //
            Jmp(_) => self.emit_byte(0xd8), // jump always
            _ => unreachable!()
        }
        self.emit_dist(dist as isize);
    }

    fn bind(&mut self, label: Label) {
        debug_assert_ne!(0, label.pc);
        let current_pos = self.bytes.len() as isize;
        let mut pc = label.pc;
        loop {
            let mut buf = &mut self.bytes.as_slice_mut()[pc+1..pc+3];
            let off = i16::from_le_bytes(buf.try_into().unwrap()) as isize;
            // fill the distance!
            let dist = current_pos - pc as isize;
            buf.write(&(dist as i16).to_le_bytes()).unwrap();
            if off == 0 { // end
                break;
            }
            pc = off as usize;
        }
    }

    fn emit(&mut self, bc: Bytecode) {
        use Bytecode::*;

        match bc {
            Ldn(name) => {
                self.emit_byte(0x10);
                self.emit_str(name);
            }
            Ldfn(prefix, suffix) => {
                self.emit_byte(0x11);
                self.emit_full_name(prefix, suffix);
            }
            // 0x12 => Bytecode::Ldfa(self.read_hint()),
            Ldfa(hint) => {
                self.emit_byte(0x12);
                self.emit_hint(hint);
            }
            // 0x13 => Bytecode::Ldi(self.read_i64()),
            Ldi(n) => {
                self.emit_byte(0x13);
                self.emit_i64(n);
            }
            // 0x14 => Bytecode::Ldsz(self.read_pool_str().1),
            Ldsz(s) => {
                self.emit_byte(0x14);
                self.emit_str(s);
            }
            // 0x15 => Bytecode::Ldsf(self.read_f32()),
            Ldsf(f) => {
                self.emit_byte(0x15);
                self.emit_f32(f);
            }
            // 0x16 => Bytecode::Lddf(self.read_f64()),
            Lddf(f) => {
                self.emit_byte(0x16);
                self.emit_f64(f);
            }
            // 0x17 => Bytecode::Ldnul,
            Ldnul => self.emit_byte(0x17),

            // 0x20 => Bytecode::Add,
            Add => self.emit_byte(0x20),
            // 0x21 => Bytecode::Sub,
            Sub => self.emit_byte(0x21),
            // 0x22 => Bytecode::Mul,
            Mul => self.emit_byte(0x22),
            // 0x23 => Bytecode::Div,
            Div => self.emit_byte(0x23),
            // 0x24 => Bytecode::Mod,
            Mod => self.emit_byte(0x24),
            // 0x25 => Bytecode::Eq,
            Eq => self.emit_byte(0x25),
            // 0x26 => Bytecode::Ne,
            Ne => self.emit_byte(0x26),
            // 0x27 => Bytecode::Lt,
            Lt => self.emit_byte(0x27),
            // 0x28 => Bytecode::Le,
            Le => self.emit_byte(0x28),
            // 0x29 => Bytecode::Gt,
            Gt => self.emit_byte(0x29),
            // 0x2a => Bytecode::Ge,
            Ge => self.emit_byte(0x2a),
            // 0x2b => Bytecode::Like,
            Like => self.emit_byte(0x2b),
            // 0x2c => Bytecode::And,
            And => self.emit_byte(0x2c),
            // 0x2d => Bytecode::Or,
            Or => self.emit_byte(0x2d),
            // 0x2e => Bytecode::Not,
            Not => self.emit_byte(0x2e),

            // 0xa0 => Bytecode::Call(self.read_pool_str().1, self.read_hint()),
            Call(name, argc) => {
                self.emit_byte(0xa0);
                self.emit_str(name);
                self.emit_hint(argc);
            }

            End => self.emit_byte(0xef),
            _ => unreachable!()
        }
    }

    fn emit_byte(&mut self, b: u8) {
        self.bytes.push(b);
    }

    fn emit_hint(&mut self, hint: usize) {
        self.bytes.write(&(hint as u16).to_le_bytes()).unwrap();
    }

    fn emit_str(&mut self, s: ArenaStr) {
        let i = self.constant(ArenaStr::default(), s) as u16;
        self.bytes.write(&i.to_le_bytes()).unwrap();
    }

    fn emit_full_name(&mut self, prefix: ArenaStr, suffix: ArenaStr) {
        let i = self.constant(prefix, suffix) as u16;
        self.bytes.write(&i.to_le_bytes()).unwrap();
    }

    fn emit_dist(&mut self, dist: isize) {
        let i = dist as i16;
        self.bytes.write(&i.to_le_bytes()).unwrap();
    }

    fn emit_i64(&mut self, value: i64) {
        self.bytes.write(&value.to_le_bytes()).unwrap();
    }

    fn emit_f32(&mut self, value: f32) {
        self.bytes.write(&value.to_le_bytes()).unwrap();
    }

    fn emit_f64(&mut self, value: f64) {
        self.bytes.write(&value.to_le_bytes()).unwrap();
    }

    fn constant(&mut self, prefix: ArenaStr, suffix: ArenaStr) -> usize {
        let rs = self.pool.get(&(prefix.clone(), suffix.clone())).cloned();
        if let Some(pos) = rs {
            pos
        } else {
            let pos = self.constants.len();
            self.constants.push((prefix, suffix));
            let name = &self.constants[pos];
            self.pool.insert(name.clone(), pos);
            pos
        }
    }

    fn build<W: Write>(&self, w: &mut W) -> io::Result<usize> {
        let offset = (12 + self.constants.len() * 4) as u32;
        let mut written = w.write(&offset.to_le_bytes())?;
        let len = self.bytes.len() as u32;
        written += w.write(&len.to_le_bytes())?;

        let mut offset = offset + len; // pool offset
        let len = self.constants.len() as u32;
        written += w.write(&len.to_le_bytes())?;

        for i in 0..self.constants.len() {
            written += w.write(&offset.to_le_bytes())?;

            let k = &self.constants[i];
            if k.0.is_empty() {
                offset += k.1.len() as u32 + 2;
            } else {
                offset += (k.0.len() + 1 + k.1.len()) as u32 + 2;
            }
        }

        written += w.write(&self.bytes)?;

        for (prefix, suffix) in self.constants.iter() {
            if prefix.is_empty() {
                let len = suffix.len() as u16;
                written += w.write(&len.to_le_bytes())?;
                written += w.write(suffix.as_bytes())?;
            } else {
                let len = (prefix.len() + 1 + suffix.len()) as u16;
                written += w.write(&len.to_le_bytes())?;
                written += w.write(prefix.as_bytes())?;
                written += w.write(&[b'.'])?;
                written += w.write(suffix.as_bytes())?;
            }
        }
        Ok(written)
    }
}



// [bytecode-offset][bytecode-len] | [const-len][const-idx][const-idx].. | [bytecodes....] | [const-pool....] | [eof]
pub struct BytecodeArray<'a> {
    bytes: &'a [u8],
    pc: usize,
    const_pool: ArenaVec<(ArenaStr, ArenaStr)>
}

impl <'a> BytecodeArray<'a> {
    pub fn new(chunk: &'a [u8], arena: &ArenaMut<Arena>) -> Result<Self> {
        if chunk.len() < 12 {
            corrupted_err!("Bytecode chunk is too samll, {}", chunk.len())?;
        }
        let offset = u32::from_le_bytes((&chunk[0..4]).try_into().unwrap()) as usize;
        let len = u32::from_le_bytes((&chunk[4..8]).try_into().unwrap()) as usize;
        let bytes = &chunk[offset..offset+len];

        let mut pool = arena_vec!(arena);
        let len = u32::from_le_bytes((&chunk[8..12]).try_into().unwrap()) as usize;
        for i in 0..len {
            let begin = 12 + i * 4;
            let end = begin + 4;
            let offset = u32::from_le_bytes((&chunk[begin..end]).try_into().unwrap()) as usize;
            let n = u16::from_le_bytes((&chunk[offset..offset + 2]).try_into().unwrap()) as usize;
            let s = std::str::from_utf8(&chunk[offset + 2..offset + 2 + n]).unwrap();
            let full_name = if let Some(dot) = s.find(".") {
                let (p, u) = s.split_at(dot);
                (p, std::str::from_utf8(&u.as_bytes()[1..]).unwrap())
            } else {
                ("", s)
            };
            pool.push((ArenaStr::new(full_name.0, arena.get_mut()),
                       ArenaStr::new(full_name.1, arena.get_mut())));
        }

        Ok(Self {
            bytes,
            pc: 0,
            const_pool: pool
        })
    }

    pub fn reset(&mut self) { self.pc = 0; }

    pub fn pick(&mut self) -> Bytecode {
        if self.pc >= self.bytes.len() {
            return Bytecode::End;
        }

        let opcode = self.bytes[self.pc];
        self.pc += 1;
        match opcode {
            0x10 => Bytecode::Ldn(self.read_pool_str().1),
            0x11 => {
                let name = self.read_pool_str();
                Bytecode::Ldfn(name.0, name.1)
            }
            0x12 => Bytecode::Ldfa(self.read_hint()),
            0x13 => Bytecode::Ldi(self.read_i64()),
            0x14 => Bytecode::Ldsz(self.read_pool_str().1),
            0x15 => Bytecode::Ldsf(self.read_f32()),
            0x16 => Bytecode::Lddf(self.read_f64()),
            0x17 => Bytecode::Ldnul,

            0x20 => Bytecode::Add,
            0x21 => Bytecode::Sub,
            0x22 => Bytecode::Mul,
            0x23 => Bytecode::Div,
            0x24 => Bytecode::Mod,
            0x25 => Bytecode::Eq,
            0x26 => Bytecode::Ne,
            0x27 => Bytecode::Lt,
            0x28 => Bytecode::Le,
            0x29 => Bytecode::Gt,
            0x2a => Bytecode::Ge,
            0x2b => Bytecode::Like,
            0x2c => Bytecode::And,
            0x2d => Bytecode::Or,
            0x2e => Bytecode::Not,

            0xa0 => Bytecode::Call(self.read_pool_str().1, self.read_hint()),

            0xef => Bytecode::End,
            _ => unreachable!()
        }
    }

    fn read_pool_str(&mut self) -> (ArenaStr, ArenaStr) {
        let i = u16::from_le_bytes((&self.bytes[self.pc..self.pc + 2]).try_into().unwrap()) as usize;
        self.pc += 2;
        self.const_pool[i].clone()
    }

    fn read_hint(&mut self) -> usize {
        let i = u16::from_le_bytes((&self.bytes[self.pc..self.pc + 2]).try_into().unwrap()) as usize;
        self.pc += 2;
        i
    }

    fn read_i64(&mut self) -> i64 {
        let n = i64::from_le_bytes((&self.bytes[self.pc..self.pc + 8]).try_into().unwrap());
        self.pc += 8;
        n
    }

    fn read_f32(&mut self) -> f32 {
        let n = f32::from_le_bytes((&self.bytes[self.pc..self.pc + 4]).try_into().unwrap());
        self.pc += 4;
        n
    }

    fn read_f64(&mut self) -> f64 {
        let n = f64::from_le_bytes((&self.bytes[self.pc..self.pc + 8]).try_into().unwrap());
        self.pc += 8;
        n
    }
}

pub struct BytecodeBuildingVisitor {
    builder: BytecodeBuilder,
}

impl BytecodeBuildingVisitor {
    pub fn build<W: Write>(&mut self, expr: &mut dyn Expression, w: &mut W) -> io::Result<usize> {
        self.builder.clear();
        expr.accept(self);
        self.builder.build(w)
    }
}

impl Visitor for BytecodeBuildingVisitor {

    fn visit_identifier(&mut self, this: &mut Identifier) {
        self.builder.emit(Bytecode::Ldn(this.symbol.clone()));
    }

    fn visit_full_qualified_name(&mut self, this: &mut FullyQualifiedName) {
        self.builder.emit(Bytecode::Ldfn(this.prefix.clone(), this.suffix.clone()));
    }

    fn visit_binary_expression(&mut self, this: &mut BinaryExpression) {
        this.lhs_mut().accept(self);
        this.rhs_mut().accept(self);
        match this.op() {
            // Operator::Lit => {}
            // Operator::In => {}
            // Operator::NotIn => {}
            // Operator::Minus => {}
            // Operator::Not => {}
            // Operator::IsNull => {}
            // Operator::IsNotNull => {}
            Operator::Add => self.builder.emit(Bytecode::Add),
            Operator::Sub => self.builder.emit(Bytecode::Sub),
            Operator::Mul => self.builder.emit(Bytecode::Mul),
            Operator::Div => self.builder.emit(Bytecode::Div),
            Operator::Eq => self.builder.emit(Bytecode::Eq),
            Operator::Ne => self.builder.emit(Bytecode::Ne),
            Operator::Lt => self.builder.emit(Bytecode::Lt),
            Operator::Le => self.builder.emit(Bytecode::Le),
            Operator::Gt => self.builder.emit(Bytecode::Gt),
            Operator::Ge => self.builder.emit(Bytecode::Ge),
            Operator::Like => self.builder.emit(Bytecode::Like),
            Operator::And => self.builder.emit(Bytecode::And),
            Operator::Or => self.builder.emit(Bytecode::Or),
            _ => unreachable!()
        }
    }

    fn visit_case_when(&mut self, this: &mut CaseWhen) {
        let mut otherwise = Label::new();
        let mut end = Label::new();
        if let Some(matching) = &mut this.matching {
            for when in this.when_clause.iter_mut() {
                matching.accept(self);
                when.expected.accept(self);
                self.builder.emit_j(Bytecode::Jne(0), &mut otherwise);
                when.then.accept(self);
                self.builder.emit_j(Bytecode::Jmp(0), &mut end);
            }
        } else {
            for when in this.when_clause.iter_mut() {
                when.expected.accept(self);
                self.builder.emit_j(Bytecode::Jz(0), &mut otherwise);
                when.then.accept(self);
                self.builder.emit_j(Bytecode::Jmp(0), &mut end);
            }
        }
        if let Some(else_clause) = &mut this.else_clause {
            self.builder.bind(otherwise);
            else_clause.accept(self);
        }
        self.builder.bind(end);
    }

    fn visit_call_function(&mut self, this: &mut CallFunction) {
        for arg in this.args.iter_mut() {
            arg.accept(self);
        }
        self.builder.emit(Bytecode::Call(this.callee_name.clone(), this.args.len()));
    }

    fn visit_float_literal(&mut self, this: &mut Literal<f64>) {
        self.builder.emit(Bytecode::Lddf(this.data));
    }

    fn visit_str_literal(&mut self, this: &mut Literal<ArenaStr>) {
        self.builder.emit(Bytecode::Ldsz(this.data.clone()));
    }

    fn visit_null_literal(&mut self, _this: &mut Literal<()>) {
        self.builder.emit(Bytecode::Ldnul);
    }

    fn visit_fast_access_hint(&mut self, this: &mut FastAccessHint) {
        self.builder.emit(Bytecode::Ldfa(this.offset));
    }
}

#[cfg(test)]
mod tests {
    use crate::Arena;
    use super::*;
    use Bytecode::*;

    #[test]
    fn bytecode_array_build() {
        let zone = Arena::new_val();
        let arena = zone.get_mut();
        let mut builder = BytecodeBuilder::new(&arena);
        builder.emit(Ldi(100));
        builder.emit(Ldi(200));
        builder.emit(Add);

        let mut chunk = vec![];
        builder.build(&mut chunk).unwrap();

        assert_eq!(31, chunk.len());

        let mut bca = BytecodeArray::new(&chunk, &arena).unwrap();
        assert_eq!(Ldi(100), bca.pick());
        assert_eq!(Ldi(200), bca.pick());
        assert_eq!(Add, bca.pick());
        assert_eq!(End, bca.pick());
    }

    #[test]
    fn bytecode_ldxxx() {
        let zone = Arena::new_val();
        let arena = zone.get_mut();
        let mut builder = BytecodeBuilder::new(&arena);

        let aaa = ArenaStr::new("aaa", arena.get_mut());
        builder.emit(Ldn(aaa.clone()));

        let t1 = ArenaStr::new("t1", arena.get_mut());
        let id = ArenaStr::new("id", arena.get_mut());
        builder.emit(Ldfn(t1.clone(), id.clone()));

        let xxx = ArenaStr::new("xxx", arena.get_mut());
        builder.emit(Ldsz(xxx.clone()));
        builder.emit(Ldsf(0.2));
        builder.emit(Lddf(0.1));

        let mut chunk = vec![];
        builder.build(&mut chunk).unwrap();

        assert_eq!(64, chunk.len());

        let mut bca = BytecodeArray::new(&chunk, &arena).unwrap();
        assert_eq!(Ldn(aaa), bca.pick());
        assert_eq!(Ldfn(t1, id), bca.pick());
        assert_eq!(Ldsz(xxx), bca.pick());
        assert_eq!(Ldsf(0.2), bca.pick());
        assert_eq!(Lddf(0.1), bca.pick());
        assert_eq!(End, bca.pick());
    }
}