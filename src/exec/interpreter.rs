use std::io;
use std::io::Write;
use std::ops::{Add, Sub, Mul, Deref, DerefMut};
use std::sync::Arc;
use crate::{Arena, arena_vec, ArenaBox, ArenaMut, ArenaStr, ArenaVec, corrupted_err};
use crate::{Result};
use crate::exec::evaluator::{Context, Evaluator, Value};
use crate::map::ArenaMap;
use crate::sql::ast;
use crate::sql::ast::*;
use crate::status::Corrupting;

#[derive(Debug, PartialEq)]
pub enum Bytecode {
    // 0x00
    Nop, // do nothing
    // 0x1x
    Ldn(ArenaStr), // load by name
    Ldfn(ArenaStr, ArenaStr), // load by full-name
    Ldfa(usize), // load by fast-access hint
    Ldi(i64), // load const i64
    Ldsz(ArenaStr), // load const str
    Ldsf(f32), // load const f32
    Lddf(f64), // load const f64
    Ldnul, // load NULL
    Ldhd(usize), // load plachholder

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
    Jnz(isize), // jump if true
    Jz(isize), // jump if false
    Jne(isize), //
    Jeq(isize), //
    Jlt(isize), //
    Jle(isize), //
    Jgt(isize), //
    Jge(isize), //
    Jmp(isize), // jump always

    // 0xa0
    Call(ArenaStr, usize), // call(callee-name, n-args)

    // 0xef
    End,
}

pub enum Condition {
    IfTrue = 0,
    IfFalse = 1,
    NotEqual = 2,
    Equal = 3,
    Less = 4,
    LessOrEqual = 5,
    Greater = 6,
    GreaterOrEqual = 7,
    Always = 8,
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
    pub fn new(arena: &ArenaMut<Arena>) -> Self {
        Self {
            stack: arena_vec!(arena),
            env: arena_vec!(arena),
            arena: arena.clone(),
        }
    }

    pub fn evaluate<T: BytecodeChunk>(&mut self, bca: &mut T, env: Arc<dyn Context>) -> Result<Value> {
        self.enter(env);
        let rs = self.evaluate_impl(bca);
        self.exit();
        rs
    }

    pub fn enter(&mut self, env: Arc<dyn Context>) {
        self.env.push(env);
    }

    pub fn exit(&mut self) -> Arc<dyn Context> {
        self.env.pop().unwrap()
    }

    fn evaluate_impl<T: BytecodeChunk>(&mut self, bca: &mut T) -> Result<Value> {
        use Bytecode::*;
        bca.reset();
        loop {
            match bca.pick() {
                Nop => (), // do nothing

                Ldn(name) => self.stack.push(self.resolve(name.as_str())?),
                Ldfn(prefix, suffix) =>
                    self.ret(self.resolve_fully_qualified(prefix.as_str(), suffix.as_str())?),
                Ldfa(i) => self.ret(self.env().fast_access(i).clone()),
                Ldi(n) => self.ret(Value::Int(n)),
                Ldsz(s) => self.ret(Value::Str(s)),
                Ldsf(f) => self.ret(Value::Float(f as f64)),
                Lddf(f) => self.ret(Value::Float(f)),
                Ldnul => self.ret(Value::Null),
                Ldhd(i) => self.ret(self.env().bound_param(i).clone()),

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

                Jnz(dist) => if self.normalize_to_bool() {
                    bca.increase_pc(dist);
                }
                Jz(dist) => if !self.normalize_to_bool() {
                    bca.increase_pc(dist);
                }
                Jne(dist) => if self.test(Operator::Ne) {
                    bca.increase_pc(dist);
                }
                Jeq(dist) => if self.test(Operator::Eq) {
                    bca.increase_pc(dist);
                }
                Jmp(dist) => bca.increase_pc(dist),

                Call(callee, nargs) => self.call(callee, nargs)?,

                End => break,
                _ => unreachable!()
            }
        }

        Ok(self.stack.pop().unwrap())
    }

    fn call(&mut self, callee: ArenaStr, nargs: usize) -> Result<()> {
        let rs = self.env().get_udf(callee.as_str());
        if rs.is_none() {
            corrupted_err!("UDF: {} not found in this env.", callee)?
        }
        let udf = rs.unwrap();
        let args = self.stack.len() - nargs;
        let rv = udf.evaluate(&self.stack.as_slice()[args..], &self.arena)?;
        self.stack.truncate(args);
        self.ret(rv);
        Ok(())
    }

    fn normalize_to_bool(&mut self) -> bool {
        Evaluator::normalize_to_bool(&self.stack.pop().unwrap())
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

    fn test(&mut self, op: ast::Operator) -> bool {
        self.cmp(op);
        self.normalize_to_bool()
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

    fn emit_j(&mut self, cond: Condition, label: &mut Label) {
        let dist = label.pc;
        label.pc = self.bytes.len() + 3 /* len of jxx code */;

        use Condition::*;
        match cond {
            IfTrue => self.emit_byte(0xd0), // jump if true
            IfFalse => self.emit_byte(0xd1), // jump if false
            NotEqual => self.emit_byte(0xd2), //
            Equal => self.emit_byte(0xd3), //
            Less => self.emit_byte(0xd4), //
            LessOrEqual => self.emit_byte(0xd5), //
            Greater => self.emit_byte(0xd6), //
            GreaterOrEqual => self.emit_byte(0xd7), //
            Always => self.emit_byte(0xd8), // jump always
        }
        self.emit_dist(dist as isize);
    }

    fn bind(&mut self, label: &mut Label) {
        debug_assert_ne!(0, label.pc);
        let current_pos = self.bytes.len() as isize;
        let mut pc = label.pc;
        loop {
            let mut buf = &mut self.bytes.as_slice_mut()[pc-2..pc];
            let off = i16::from_le_bytes(buf.try_into().unwrap()) as isize;
            // fill the distance!
            let dist = current_pos - pc as isize;
            buf.write(&(dist as i16).to_le_bytes()).unwrap();
            if off == 0 { // end
                break;
            }
            pc = off as usize;
        }
        label.bind = true;
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
            // 0x18 => Bytecode::Ldhd(self.read_hint()),
            Ldhd(hint) => {
                self.emit_byte(0x18);
                self.emit_hint(hint);
            }

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


pub trait BytecodeChunk {
    fn eof(&self) -> bool;
    fn reset(&mut self);
    fn increase_pc(&mut self, dist: isize);
    fn pc(&self) -> u8 { self.peek(1)[0] }
    fn peek(&self, len: usize) -> &[u8];

    fn constant_at(&self, i: usize) -> &(ArenaStr, ArenaStr);

    fn pick(&mut self) -> Bytecode {
        if self.eof() {
            return Bytecode::End;
        }

        let opcode = self.pc();
        self.increase_pc(1);
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
            0x18 => Bytecode::Ldhd(self.read_hint()),

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

            0xd0 => Bytecode::Jnz(self.read_dist()),
            0xd1 => Bytecode::Jz(self.read_dist()),
            0xd2 => Bytecode::Jne(self.read_dist()),
            0xd3 => Bytecode::Jeq(self.read_dist()),
            0xd4 => Bytecode::Jlt(self.read_dist()),
            0xd5 => Bytecode::Jle(self.read_dist()),
            0xd6 => Bytecode::Jgt(self.read_dist()),
            0xd7 => Bytecode::Jge(self.read_dist()),
            0xd8 => Bytecode::Jmp(self.read_dist()),

            0xa0 => Bytecode::Call(self.read_pool_str().1, self.read_hint()),

            0xef => Bytecode::End,
            _ => unreachable!()
        }
    }

    fn read_hint(&mut self) -> usize {
        let i = u16::from_le_bytes(self.peek(2).try_into().unwrap()) as usize;
        self.increase_pc(2);
        i
    }

    fn read_dist(&mut self) -> isize {
        let i = i16::from_le_bytes(self.peek(2).try_into().unwrap()) as isize;
        self.increase_pc(2);
        i
    }

    fn read_i64(&mut self) -> i64 {
        let n = i64::from_le_bytes(self.peek(8).try_into().unwrap());
        self.increase_pc(8);
        n
    }

    fn read_f32(&mut self) -> f32 {
        let n = f32::from_le_bytes(self.peek(4).try_into().unwrap());
        self.increase_pc(4);
        n
    }

    fn read_f64(&mut self) -> f64 {
        let n = f64::from_le_bytes(self.peek(8).try_into().unwrap());
        self.increase_pc(8);
        n
    }

    fn read_pool_str(&mut self) -> (ArenaStr, ArenaStr) {
        let i = u16::from_le_bytes(self.peek(2).try_into().unwrap()) as usize;
        self.increase_pc(2);
        self.constant_at(i).clone()
    }
}


// [bytecode-offset][bytecode-len] | [const-len][const-idx][const-idx].. | [bytecodes....] | [const-pool....] | [eof]
pub struct BytecodeArray<'a> {
    core: BytecodeSequence<&'a [u8]>
}

impl <'a> BytecodeArray<'a> {
    pub fn new(chunk: &'a [u8], arena: &ArenaMut<Arena>) -> Result<Self> {
        let mut core = BytecodeSequence::<&[u8]> {
            bytes: &[],
            pc: 0,
            const_pool: arena_vec!(arena),
        };
        let (offset, len) = core.parse(chunk, arena)?;
        core.bytes = &chunk[offset..offset + len];
        Ok(Self { core })
    }
}

impl <'a> BytecodeChunk for BytecodeArray<'a> {
    fn eof(&self) -> bool {
        self.core.pc >= self.core.bytes.len()
    }

    fn reset(&mut self) { self.core.reset(); }

    fn increase_pc(&mut self, dist: isize) { self.core.increase_pc(dist); }

    fn peek(&self, len: usize) -> &[u8] {
        &self.core.bytes[self.core.pc..self.core.pc + len]
    }

    fn constant_at(&self, i: usize) -> &(ArenaStr, ArenaStr) {
        &self.core.const_pool[i]
    }
}

pub struct BytecodeVector {
    core: BytecodeSequence<ArenaVec<u8>>,
    code_offset: usize,
    code_len: usize,
}

impl BytecodeVector {
    pub fn new(chunk: ArenaVec<u8>, arena: &ArenaMut<Arena>) -> Self {
        let mut core = BytecodeSequence {
            bytes: arena_vec!(arena),
            pc: 0,
            const_pool: arena_vec!(arena),
        };
        let (code_offset, code_len) = core.parse(&chunk, arena).unwrap();
        core.bytes = chunk;
        Self {
            core,
            code_offset,
            code_len,
        }
    }
}

impl BytecodeChunk for BytecodeVector {
    fn eof(&self) -> bool {
        self.core.pc >= self.code_len
    }

    fn reset(&mut self) {
        self.core.reset();
    }

    fn increase_pc(&mut self, dist: isize) {
        self.core.increase_pc(dist)
    }

    fn peek(&self, len: usize) -> &[u8] {
        let bytes = &self.core.bytes.as_slice()[self.code_offset..self.code_offset + self.code_len];
        &bytes[self.core.pc..self.core.pc + len]
    }

    fn constant_at(&self, i: usize) -> &(ArenaStr, ArenaStr) {
        &self.core.const_pool[i]
    }
}

struct BytecodeSequence<T> {
    bytes: T,
    pc: usize,
    const_pool: ArenaVec<(ArenaStr, ArenaStr)>
}

impl <T> BytecodeSequence<T> {
    pub fn parse(&mut self, chunk: &[u8], arena: &ArenaMut<Arena>) -> Result<(usize, usize)> {
        if chunk.len() < 12 {
            corrupted_err!("Bytecode chunk is too samll, {}", chunk.len())?;
        }
        let offset = u32::from_le_bytes((&chunk[0..4]).try_into().unwrap()) as usize;
        let len = u32::from_le_bytes((&chunk[4..8]).try_into().unwrap()) as usize;
        let bytes = &chunk[offset..offset+len];
        let bytes_range = (offset, len);

        //let mut pool = arena_vec!(arena);
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
            self.const_pool.push((ArenaStr::new(full_name.0, arena.get_mut()),
                       ArenaStr::new(full_name.1, arena.get_mut())));
        }
        self.pc = 0;
        Ok(bytes_range)
    }

    fn reset(&mut self) { self.pc = 0; }

    fn increase_pc(&mut self, dist: isize) {
        // debug_assert!(dist as usize >= self.pc);
        // debug_assert!(self.pc + (dist as usize) < self.bytes.len());
        if dist >= 0 {
            self.pc += dist as usize;
        } else {
            self.pc -= isize::abs(dist) as usize;
        }
    }
}

pub struct BytecodeBuildingVisitor {
    builder: BytecodeBuilder,
}

impl BytecodeBuildingVisitor {
    pub fn new(arena: &ArenaMut<Arena>) -> Self {
        Self {
            builder: BytecodeBuilder::new(arena)
        }
    }

    pub fn build<W: Write>(&mut self, expr: &mut dyn Expression, w: &mut W) -> io::Result<usize> {
        self.builder.clear();
        expr.accept(self);
        self.builder.build(w)
    }

    pub fn build_arena_boxes(boxes: &[ArenaBox<dyn Expression>], arena: &ArenaMut<Arena>) -> ArenaVec<BytecodeVector> {
        let mut this = BytecodeBuildingVisitor::new(arena);
        let mut rv = arena_vec!(arena);
        for it in boxes {
            let mut expr = it.clone();
            let mut chunk = arena_vec!(arena);
            this.build(expr.deref_mut(), &mut chunk).unwrap();
            rv.push(BytecodeVector::new(chunk, arena));
        }
        rv
    }

    pub fn build_arena_box(expr: &ArenaBox<dyn Expression>, arena: &ArenaMut<Arena>) -> BytecodeVector {
        let mut this = BytecodeBuildingVisitor::new(arena);
        let mut copied = expr.clone();
        let mut chunk = arena_vec!(arena);
        this.build(copied.deref_mut(), &mut chunk).unwrap();
        BytecodeVector::new(chunk, arena)
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
                self.builder.emit_j(Condition::NotEqual, &mut otherwise);
                when.then.accept(self);
                self.builder.emit_j(Condition::Always, &mut end);
            }
        } else {
            for when in this.when_clause.iter_mut() {
                when.expected.accept(self);
                self.builder.emit_j(Condition::IfFalse, &mut otherwise);
                when.then.accept(self);
                self.builder.emit_j(Condition::Always, &mut end);
            }
        }
        if let Some(else_clause) = &mut this.else_clause {
            self.builder.bind(&mut otherwise);
            else_clause.accept(self);
        } else {
            self.builder.bind(&mut otherwise);
            self.builder.emit(Bytecode::Ldnul);
        }
        self.builder.bind(&mut end);
        self.builder.emit(Bytecode::Nop);
    }

    fn visit_call_function(&mut self, this: &mut CallFunction) {
        for arg in this.args.iter_mut() {
            arg.accept(self);
        }
        self.builder.emit(Bytecode::Call(this.callee_name.clone(), this.args.len()));
    }

    fn visit_int_literal(&mut self, this: &mut Literal<i64>) {
        self.builder.emit(Bytecode::Ldi(this.data));
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
    use std::ops::DerefMut;
    use crate::{Arena, ArenaRef};
    use super::*;
    use Bytecode::*;
    use crate::exec::executor::MockContent;
    use crate::sql::parse_sql_expr_from_content;

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

    #[test]
    fn bytecode_jmp_and_bind() {
        let zone = Arena::new_val();
        let arena = zone.get_mut();
        let mut builder = BytecodeBuilder::new(&arena);

        builder.emit(Ldi(19));
        builder.emit(Ldi(20));

        let mut label = Label::new();
        builder.emit_j(Condition::Always, &mut label);
        builder.emit(Add);
        builder.bind(&mut label);
        builder.emit(End);

        let mut chunk = vec![];
        builder.build(&mut chunk).unwrap();

        assert_eq!(35, chunk.len());

        let mut bca = BytecodeArray::new(&chunk, &arena).unwrap();
        assert_eq!(Ldi(19), bca.pick());
        assert_eq!(Ldi(20), bca.pick());
        assert_eq!(Jmp(1), bca.pick());
        assert_eq!(Add, bca.pick());
        assert_eq!(End, bca.pick());
    }

    #[test]
    fn bytecode_multi_jmps_bind_once() {
        let zone = Arena::new_val();
        let arena = zone.get_mut();
        let mut builder = BytecodeBuilder::new(&arena);

        builder.emit(Ldi(19));
        builder.emit(Ldi(20));

        let mut label = Label::new();
        builder.emit_j(Condition::Always, &mut label);
        builder.emit(Add);
        builder.emit_j(Condition::Equal, &mut label);
        builder.emit(Sub);
        builder.emit_j(Condition::NotEqual, &mut label);
        builder.emit(Mul);
        builder.bind(&mut label);
        builder.emit(End);

        let mut chunk = vec![];
        builder.build(&mut chunk).unwrap();

        assert_eq!(43, chunk.len());

        let mut bca = BytecodeArray::new(&chunk, &arena).unwrap();
        assert_eq!(Ldi(19), bca.pick());
        assert_eq!(Ldi(20), bca.pick());
        assert_eq!(Jmp(9), bca.pick());
        assert_eq!(Add, bca.pick());
        assert_eq!(Jeq(5), bca.pick());
        assert_eq!(Sub, bca.pick());
        assert_eq!(Jne(1), bca.pick());
        assert_eq!(Mul, bca.pick());
        assert_eq!(End, bca.pick());
    }

    #[test]
    fn bytecode_vector() {
        let zone = Arena::new_val();
        let arena = zone.get_mut();
        let mut builder = BytecodeBuilder::new(&arena);

        builder.emit(Ldi(19));
        builder.emit(Ldi(20));

        let demo = ArenaStr::new("demo", arena.get_mut());
        builder.emit(Ldsz(demo.clone()));

        let mut chunk = arena_vec!(&arena);
        builder.build(&mut chunk).unwrap();

        let mut bca = BytecodeVector::new(chunk, &arena);
        assert_eq!(Ldi(19), bca.pick());
        assert_eq!(Ldi(20), bca.pick());
        assert_eq!(Ldsz(demo), bca.pick());
        assert_eq!(End, bca.pick());
    }

    #[test]
    fn interpreter_sanity() {
        let mut suite = Suite::new();
        let rv = suite.eval(|builder| {
            builder.emit(Ldi(1));
            builder.emit(Ldi(2));
            builder.emit(Add);
        }).unwrap();

        assert_eq!(Value::Int(3), rv);
    }

    #[test]
    fn interpreter_ldsz() {
        let mut suite = Suite::new();
        let a = ArenaStr::new("a", suite.arena.get_mut());
        let b = ArenaStr::new("b", suite.arena.get_mut());
        let c = ArenaStr::new("c", suite.arena.get_mut());

        let rv = suite.eval(|builder| {
            builder.emit(Ldsz(c.clone()));
            builder.emit(Ldsz(b.clone()));
            builder.emit(Ldsz(a.clone()));
        }).unwrap();
        assert_eq!(Value::Str(a.clone()), rv);
    }

    #[test]
    fn interpreter_ldsf() {
        let mut suite = Suite::new();
        let rv = suite.eval(|b| {
            b.emit(Ldsf(0.0));
            b.emit(Lddf(0.1));
        }).unwrap();
        assert_eq!(Value::Float(0.1), rv);

        let mut suite = Suite::new();
        let name = ArenaStr::new("xxx", suite.arena.deref_mut());
        let var = ArenaStr::new("aaa", suite.arena.deref_mut());
        suite.with_env(|env| {
            env.with_env([
                (name.as_str(), Value::Int(99)),
                (var.as_str(), Value::Float(0.1))
            ])
        });
        let rv = suite.eval(|b| {
            b.emit(Ldn(var.clone()));
            b.emit(Ldn(name.clone()));
        }).unwrap();
        assert_eq!(Value::Int(99), rv);
    }

    #[test]
    fn interpreter_add() {
        let mut suite = Suite::new();
        let v100 = ArenaStr::new("100", suite.arena.get_mut());
        assert_eq!(Value::Int(122), suite.eval(|b| {
            b.emit(Ldsz(v100.clone()));
            b.emit(Ldi(22));
            b.emit(Add);
        }).unwrap());

        let mut suite = Suite::new();
        assert_eq!(Value::Float(1.1), suite.eval(|b| {
            b.emit(Lddf(1.0));
            b.emit(Lddf(0.1));
            b.emit(Add);
        }).unwrap())
    }

    #[test]
    fn interpreter_jxx() {
        let mut suite = Suite::new();
        assert_eq!(Value::Int(1), suite.eval(|b| {
            let mut label = Label::new();
            b.emit(Lddf(1.0));
            b.emit(Lddf(0.1));
            b.emit_j(Condition::Always, &mut label);
            b.emit(Add);
            b.bind(&mut label);
            b.emit(Ldi(1));
        }).unwrap())
    }

    #[test]
    fn interpreter_calling() -> Result<()> {
        let mut suite = Suite::new();
        suite.with_env(|x|{x});
        let parts = [
            ArenaStr::new("hello", suite.arena.deref_mut()),
            ArenaStr::new(",", suite.arena.deref_mut()),
            ArenaStr::new("world", suite.arena.deref_mut()),
        ];
        let all_of_parts = ArenaStr::new("hello,world", suite.arena.deref_mut());
        let concat = ArenaStr::new("concat", suite.arena.deref_mut());
        assert_eq!(Value::Str(all_of_parts), suite.eval(|b| {
            b.emit(Ldsz(parts[0].clone()));
            b.emit(Ldsz(parts[1].clone()));
            b.emit(Ldsz(parts[2].clone()));
            b.emit(Call(concat.clone(), 3));
        })?);
        Ok(())
    }

    #[test]
    fn simple_expr() -> Result<()> {
        let mut suite = Suite::new();
        assert_eq!(Value::Int(1), suite.eval_ast("2 - 1")?);
        Ok(())
    }

    struct Suite<'a> {
        zone: ArenaRef<Arena>,
        arena: ArenaMut<Arena>,
        builder: BytecodeBuilder,
        chunk: Vec<u8>,
        bca: Option<BytecodeArray<'a>>,
        env: Option<Arc<dyn Context>>,
        interpreter: Interpreter,
    }

    impl <'a> Suite<'a> {
        fn new() -> Self {
            let zone = Arena::new_ref();
            let arena = zone.get_mut();
            let chunk = vec![];
            Self {
                zone,
                builder: BytecodeBuilder::new(&arena),
                bca: None,
                env: None,
                interpreter: Interpreter::new(&arena),
                chunk,
                arena,
            }
        }

        fn with_env<F>(&mut self, build: F)
            where F: Fn(MockContent) -> MockContent {
            let ctx = build(MockContent::new(&self.arena));
            self.env = Some(Arc::new(ctx));
        }

        fn build<F>(&'a mut self, build: F) -> &mut BytecodeArray where F: Fn(&mut BytecodeBuilder) {
            build(&mut self.builder);
            self.chunk.clear();
            self.builder.build(&mut self.chunk).unwrap();
            self.bca = Some(BytecodeArray::new(&self.chunk, &self.arena).unwrap());
            self.bca.as_mut().unwrap()
        }

        fn eval_ast(&'a mut self, expr: &str) -> Result<Value> {
            let mut ast = parse_sql_expr_from_content(expr, &self.arena)?;
            let mut visitor = BytecodeBuildingVisitor::new(&self.arena);

            visitor.build(ast.deref_mut(), &mut self.chunk).unwrap();
            self.bca = Some(BytecodeArray::new(&self.chunk, &self.arena).unwrap());
            if let Some(env) = self.env.as_ref() {
                self.interpreter.enter(env.clone());
            }
            let rs = self.interpreter.evaluate_impl(self.bca.as_mut().unwrap());
            if self.env.is_some() {
                self.interpreter.exit();
            }
            rs
        }

        fn eval<F>(&'a mut self, build: F) -> Result<Value> where F: Fn(&mut BytecodeBuilder) {
            build(&mut self.builder);
            self.chunk.clear();
            self.builder.build(&mut self.chunk).unwrap();
            self.bca = Some(BytecodeArray::new(&self.chunk, &self.arena).unwrap());
            if let Some(env) = self.env.as_ref() {
                self.interpreter.enter(env.clone());
            }
            let rs = self.interpreter.evaluate_impl(self.bca.as_mut().unwrap());
            if self.env.is_some() {
                self.interpreter.exit();
            }
            rs
        }
    }
}