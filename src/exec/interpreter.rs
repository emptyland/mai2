use std::ops::Deref;
use std::sync::Arc;
use crate::{Arena, arena_vec, ArenaMut, ArenaStr, ArenaVec, corrupted_err};
use crate::{Result};
use crate::exec::evaluator::{Context, Value};
use crate::status::Corrupting;

pub enum Bytecode<'a> {
    // 0x1x
    Ldn(&'a str), // load by name
    Ldfn(&'a str, &'a str), // load by full-name
    Ldfa(usize), // load by fast-access hint
    Ldi(i64), // load const i64
    Ldsz(&'a str), // load const str
    Ldsf(f32), // load const f32
    Lddf(f64), // load const f64
    Ldnul, // load NULL

    // 0x2x
    Add,
    Sub,
    Mul,
    Div,
    Mod,

    // 0x3x
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Like,

    // 0x4x
    And,
    Or,
    Not,

    End,
}

pub struct Interpreter<'a> {
    bytecode_array: BytecodeArray<'a>,
    stack: ArenaVec<Value>,
    env: ArenaVec<Arc<dyn Context>>,
    arena: ArenaMut<Arena>
}

impl <'a> Interpreter<'a> {

    fn evaluate(&mut self) -> Result<Value> {
        use Bytecode::*;
        self.bytecode_array.reset();
        loop {
            match self.bytecode_array.pick() {
                Ldn(name) => self.stack.push(self.resolve(name)?),
                Ldfn(prefix, suffix) =>
                    self.stack.push(self.resolve_fully_qualified(prefix, suffix)?),
                Ldfa(i) =>
                    self.stack.push(self.env().fast_access(i).clone()),
                Ldsz(s) =>
                    self.stack.push(Value::Str(ArenaStr::new(s, self.arena.get_mut()))),
                Ldsf(f) =>
                    self.stack.push(Value::Float(f as f64)),
                Lddf(f) =>
                    self.stack.push(Value::Float(f)),
                Ldnul =>
                    self.stack.push(Value::Null),
                Add =>
                    self.add(),

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

    fn add(&mut self) {
        let rhs = self.stack.pop().unwrap();
        let lhs = self.stack.pop().unwrap();

    }

    fn env(&self) -> &dyn Context {
        self.env.back().unwrap().deref()
    }
}

// [bytecode-offset][bytecode-len] | [const-len][const-idx][const-idx].. | [bytecodes....] | [const-pool....] | [eof]
pub struct BytecodeArray<'a> {
    bytes: &'a [u8],
    pc: usize,
    const_pool: ArenaVec<(&'a str, &'a str)>
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
            if let Some(dot) = s.find(".") {
                pool.push(s.split_at(dot));
            } else {
                pool.push(("", s));
            }
        }

        Ok(Self {
            bytes,
            pc: 0,
            const_pool: pool
        })
    }

    pub fn reset(&mut self) { self.pc = 0; }

    pub fn pick(&mut self) -> Bytecode<'a> {
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
            0x15 => Bytecode::Ldnul,
            _ => unreachable!()
        }
    }

    fn read_pool_str(&mut self) -> (&'a str, &'a str) {
        let i = u16::from_le_bytes((&self.bytes[self.pc..self.pc + 2]).try_into().unwrap()) as usize;
        self.pc += 2;
        self.const_pool[i]
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
}

impl <'a> Iterator for BytecodeArray<'a> {
    type Item = Bytecode<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let bc = self.pick();
        if matches!(bc, Bytecode::End) {
            None
        } else {
            Some(bc)
        }
    }
}