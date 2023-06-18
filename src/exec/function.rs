use std::any::Any;
use std::collections::HashMap;

use crate::{Corrupting, Result, Status, utils};
use crate::base::{Arena, ArenaBox, ArenaMut, ArenaVec};
use crate::exec::db::ColumnType;
use crate::exec::evaluator::Value;

pub struct ExecutionContext {
    arena: ArenaMut<Arena>,
    distinct: bool,
}

impl ExecutionContext {
    pub fn new(distinct: bool, arena: &ArenaMut<Arena>) -> Self {
        Self {
            arena: arena.clone(),
            distinct,
        }
    }
}

pub trait UDF {
    fn setup(&self) -> Result<()> { Ok(()) }
    fn close(&self) {}
    fn signatures(&self) -> &[&str] { &[] }
    fn evaluate(&self, args: &[Value], arena: &ArenaMut<Arena>) -> Result<Value>;
}


pub trait Aggregator {
    fn setup(&self) -> Result<()> { Ok(()) }
    fn close(&self) {}
    fn signature(&self, _params: &[ColumnType]) -> ColumnType { ColumnType::BigInt(11) }
    fn new_buf(&self, arena: &ArenaMut<Arena>) -> ArenaBox<dyn Writable>;
    fn iterate(&self, dest: &mut dyn Writable, args: &[ArenaBox<dyn Writable>]) -> Result<()>;
    fn terminate(&self, dest: &mut dyn Writable) -> Result<Value>;
    fn merge(&self, dest: &mut dyn Writable, input: &dyn Writable) -> Result<()>;
}

pub trait Writable {
    fn any(&self) -> &dyn Any;
    fn any_mut(&mut self) -> &mut dyn Any;
    fn clear(&mut self);
    fn recv(&mut self, _value: &Value) {}
}

pub struct Signature {
    pub ret_val: ColumnType,
    pub params: ArenaVec<ColumnType>,
}

impl Signature {
    pub fn parse(signature: &str, arena: &ArenaMut<Arena>) -> Result<Signature> {
        let mut parts = signature.splitn(2, "->");
        let params_part;
        if let Some(part) = parts.next() {
            params_part = part;
        } else {
            return Err(Status::corrupted("bad format"));
        }

        let mut params = ArenaVec::new(arena);
        if params_part != "()" {
            for part in params_part.split(",") {
                params.push(Self::parse_str_to_ty(part));
            }
        }

        let ret_val_part;
        if let Some(part) = parts.next() {
            ret_val_part = part;
        } else {
            return Err(Status::corrupted("bad format"));
        }
        Ok(Self {
            ret_val: Self::parse_str_to_ty(ret_val_part),
            params,
        })
    }

    fn parse_str_to_ty(input: &str) -> ColumnType {
        match input {
            "str" => ColumnType::Varchar(255),
            "int" => ColumnType::Int(11),
            "bigint" => ColumnType::BigInt(11),
            _ => unreachable!()
        }
    }
}

macro_rules! pure_udf_impl {
    ($name:ident, $proto:expr, $args:ident, $arena:ident, $body:block) => {
        pub struct $name;
        impl $name {
            pub fn new(ctx: &ExecutionContext) -> ArenaBox<dyn UDF> {
                ArenaBox::new(Self{}, ctx.arena.get_mut()).into()
            }
        }
        impl UDF for $name {
            fn signatures(&self) -> &[&str] {
                static DESC:[&str;1] = $proto;
                &DESC
            }
            fn evaluate(&self, $args: &[Value], $arena: &ArenaMut<Arena>) -> Result<Value> $body
        }
    }
}

macro_rules! pure_udaf_def {
    ($name:ident) => {
        pub struct $name;
        impl $name {
            pub fn new(ctx: &ExecutionContext) -> ArenaBox<dyn Aggregator> {
                ArenaBox::new(Self{}, ctx.arena.get_mut()).into()
            }
        }
    }
}

mod udf {
    use crate::base::ArenaStr;

    use super::*;

    pure_udf_impl!(Version, ["()->str"], _args, arena, {
        Ok(Value::Str(ArenaStr::new("mai2-sql:v0.0.1", arena.get_mut())))
    });
    pure_udf_impl!(Length, ["str->int"], args, _arena, {
        if args.is_empty() {
            Ok(Value::Null)
        } else {
            Ok(match &args[0] {
                Value::Str(s) => Value::Int(s.len() as i64),
                _ => Value::Null
            })
        }
    });
    pure_udf_impl!(Concat, ["str->str"], args, arena, {
        if args.is_empty() {
            Ok(Value::Null)
        } else {
            let mut buf = String::new();
            for arg in args {
                buf.push_str(format!("{arg}").as_str());
            }
            Ok(Value::Str(ArenaStr::new(buf.as_str(), arena.get_mut())))
        }
    });
}

mod udaf {
    use std::ops::Deref;

    use crate::corrupted_err;
    use crate::exec::evaluator::Evaluator;

    use super::*;

    pure_udaf_def!(Count);
    impl Aggregator for Count {
        fn new_buf(&self, arena: &ArenaMut<Arena>) -> ArenaBox<dyn Writable> {
            ArenaBox::new(NumberBuf::<i64>::default(), arena.get_mut()).into()
        }

        fn iterate(&self, dest: &mut dyn Writable, _args: &[ArenaBox<dyn Writable>]) -> Result<()> {
            let buf = NumberBuf::<i64>::cast_mut(dest);
            buf.rows += 1;
            buf.value += 1;
            Ok(())
        }

        fn terminate(&self, dest: &mut dyn Writable) -> Result<Value> {
            let buf = NumberBuf::<i64>::cast(dest);
            Ok(Value::Int(buf.value))
        }

        fn merge(&self, dest: &mut dyn Writable, src: &dyn Writable) -> Result<()> {
            let buf = NumberBuf::<i64>::cast_mut(dest);
            let other = NumberBuf::<i64>::cast(src);
            buf.value += other.value;
            buf.rows += other.rows;
            Ok(())
        }
    }

    pure_udaf_def!(Sum);
    impl Sum {
        fn apply(dest: &mut dyn Writable, arg: &dyn Writable) {
            let rv = VariableNumberBuf::cast_mut(dest);
            let input = VariableNumberBuf::cast(arg);
            match &mut rv.number {
                VariableNumber::Unknown => rv.number = input.number.clone(),
                VariableNumber::Integral(buf) => match &input.number {
                    VariableNumber::Integral(n) => {
                        buf.value += n.value;
                        buf.rows += 1;
                    }
                    VariableNumber::Floating(n) => {
                        let new_buf = NumberBuf{
                            value: buf.value as f64 + n.value,
                            rows: buf.rows + 1
                        };
                        rv.number = VariableNumber::Floating(new_buf);
                    }
                    _ => unreachable!()
                }
                VariableNumber::Floating(buf) => match &input.number {
                    VariableNumber::Integral(n) => {
                        buf.value += n.value as f64;
                        buf.rows += 1;
                    }
                    VariableNumber::Floating(n) => {
                        buf.value += n.value;
                        buf.rows += 1;
                    }
                    _ => unreachable!()
                }
            }
        }
    }
    impl Aggregator for Sum {
        fn signature(&self, params: &[ColumnType]) -> ColumnType {
            if params.len() < 1 {
                ColumnType::BigInt(11)
            } else if params[0].is_integral() {
                ColumnType::BigInt(11)
            } else if params[0].is_floating() {
                ColumnType::Double(0, 0)
            } else {
                ColumnType::BigInt(11)
            }
        }

        fn new_buf(&self, arena: &ArenaMut<Arena>) -> ArenaBox<dyn Writable> {
            ArenaBox::new(VariableNumberBuf::default(), arena.get_mut()).into()
        }

        fn iterate(&self, dest: &mut dyn Writable, args: &[ArenaBox<dyn Writable>]) -> Result<()> {
            if args.len() != 1 {
                return corrupted_err!("Invalid number of arguments, need 1, expected: {}", args.len())
            }
            Sum::apply(dest, args[0].deref());
            Ok(())
        }

        fn terminate(&self, dest: &mut dyn Writable) -> Result<Value> {
            let rs = VariableNumberBuf::cast_mut(dest);
            let rv = match &rs.number {
                VariableNumber::Unknown => Value::Null,
                VariableNumber::Integral(buf) => Value::Int(buf.value),
                VariableNumber::Floating(buf) => Value::Float(buf.value),
            };
            Ok(rv)
        }

        fn merge(&self, dest: &mut dyn Writable, input: &dyn Writable) -> Result<()> {
            Sum::apply(dest, input);
            Ok(())
        }
    }

    #[derive(Default)]
    struct VariableNumberBuf {
        number: VariableNumber,
    }

    impl VariableNumberBuf {
        fn cast(input: &dyn Writable) -> &Self {
            input.any().downcast_ref::<Self>().unwrap()
        }

        fn cast_mut(input: &mut dyn Writable) -> &mut Self {
            input.any_mut().downcast_mut::<Self>().unwrap()
        }
    }

    impl Writable for VariableNumberBuf {
        fn any(&self) -> &dyn Any { self }
        fn any_mut(&mut self) -> &mut dyn Any { self }

        fn clear(&mut self) {
            match &mut self.number {
                VariableNumber::Floating(buf) => buf.clear(),
                VariableNumber::Integral(buf) => buf.clear(),
                _ => ()
            }
        }

        fn recv(&mut self, value: &Value) {
            let input = Evaluator::require_number(value);
            match input {
                Value::Int(n) => match &mut self.number {
                    VariableNumber::Unknown =>
                        self.number = VariableNumber::Integral(NumberBuf { value: n, rows: 1 }),
                    VariableNumber::Integral(buf) => {
                        buf.value += n;
                        buf.rows += 1;
                    }
                    VariableNumber::Floating(buf) => {
                        buf.value += n as f64;
                        buf.rows += 1;
                    }
                }
                Value::Float(n) => match &mut self.number {
                    VariableNumber::Unknown =>
                        self.number = VariableNumber::Floating(NumberBuf { value: n, rows: 1 }),
                    VariableNumber::Integral(buf) => {
                        let new_buf = NumberBuf {
                            value: buf.value as f64 + n,
                            rows: buf.rows + 1,
                        };
                        self.number = VariableNumber::Floating(new_buf);
                    }
                    VariableNumber::Floating(buf) => {
                        buf.value += n;
                        buf.rows += 1;
                    }
                }
                Value::Null => (), // Ignore NULL value
                _ => unreachable!()
            }
        }
    }

    #[derive(Default, Clone)]
    enum VariableNumber {
        #[default]
        Unknown,
        Integral(NumberBuf<i64>),
        Floating(NumberBuf<f64>),
        //
    }

    #[derive(Default, Clone)]
    struct NumberBuf<T: Default> {
        value: T,
        rows: usize,
    }

    impl<T: Default + 'static> NumberBuf<T> {
        fn cast(input: &dyn Writable) -> &Self {
            input.any().downcast_ref::<Self>().unwrap()
        }

        fn cast_mut(input: &mut dyn Writable) -> &mut Self {
            input.any_mut().downcast_mut::<Self>().unwrap()
        }
    }

    impl<T: Default + 'static> Writable for NumberBuf<T> {
        fn any(&self) -> &dyn Any {
            self
        }

        fn any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn clear(&mut self) {
            self.value = T::default();
            self.rows = 0;
        }
    }

    impl<T: Writable + 'static> From<ArenaBox<T>> for ArenaBox<dyn Writable> {
        fn from(value: ArenaBox<T>) -> Self {
            Self::from_ptr(value.ptr())
        }
    }
}

impl<T: UDF + 'static> From<ArenaBox<T>> for ArenaBox<dyn UDF> {
    fn from(value: ArenaBox<T>) -> Self {
        Self::from_ptr(value.ptr())
    }
}

impl<T: Aggregator + 'static> From<ArenaBox<T>> for ArenaBox<dyn Aggregator> {
    fn from(value: ArenaBox<T>) -> Self {
        Self::from_ptr(value.ptr())
    }
}

lazy_static! {
    static ref BUILTIN_FNS: BuiltinFns = BuiltinFns::new();
}

struct BuiltinFns {
    udfs: HashMap<&'static str, fn(&ExecutionContext) -> ArenaBox<dyn UDF>>,
    udafs: HashMap<&'static str, fn(&ExecutionContext) -> ArenaBox<dyn Aggregator>>,
}

impl BuiltinFns {
    fn new() -> Self {
        let mut udfs = HashMap::<&'static str, fn(&ExecutionContext) -> ArenaBox<dyn UDF>>::new();
        udfs.insert("version", udf::Version::new);
        udfs.insert("length", udf::Length::new);
        udfs.insert("concat", udf::Concat::new);

        let mut udafs = HashMap::<&'static str, fn(&ExecutionContext) -> ArenaBox<dyn Aggregator>>::new();
        udafs.insert("count", udaf::Count::new);
        udafs.insert("sum", udaf::Sum::new);
        Self {
            udfs,
            udafs,
        }
    }

    fn new_udf(&self, name: &str, ctx: &ExecutionContext) -> Option<ArenaBox<dyn UDF>> {
        let buf: [u8;64] = utils::to_ascii_lowercase(name);
        match self.udfs.get(std::str::from_utf8(&buf[..name.len()]).unwrap()) {
            Some(new) =>
                Some(new(ctx)),
            None => None
        }
    }

    fn new_udaf(&self, name: &str, ctx: &ExecutionContext) -> Option<ArenaBox<dyn Aggregator>> {
        let buf: [u8;64] = utils::to_ascii_lowercase(name);
        match self.udafs.get(std::str::from_utf8(&buf[..name.len()]).unwrap()) {
            Some(new) =>
                Some(new(ctx)),
            None => None
        }
    }
}

pub fn new_udf(name: &str, ctx: &ExecutionContext) -> Option<ArenaBox<dyn UDF>> {
    BUILTIN_FNS.new_udf(name, ctx)
}

pub fn new_udaf(name: &str, ctx: &ExecutionContext) -> Option<ArenaBox<dyn Aggregator>> {
    BUILTIN_FNS.new_udaf(name, ctx)
}

pub fn new_any_fn(name: &str, ctx: &ExecutionContext) -> Option<AnyFn> {
    match new_udf(name, ctx) {
        Some(udf) => Some(AnyFn::Udf(udf)),
        None => match new_udaf(name, ctx) {
            Some(udaf) => Some(AnyFn::Udaf(udaf)),
            None => None
        }
    }
}

pub enum AnyFn {
    Udf(ArenaBox<dyn UDF>),
    Udaf(ArenaBox<dyn Aggregator>),
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_parsing() -> Result<()> {
        let zone = Arena::new_val();
        let arena = zone.get_mut();
        let sig = Signature::parse("()->int", &arena)?;
        assert!(matches!(sig.ret_val, ColumnType::Int(_)));
        assert_eq!(0, sig.params.len());

        let sig = Signature::parse("str,int,int->str", &arena)?;
        assert!(matches!(sig.ret_val, ColumnType::Varchar(_)));
        assert_eq!(3, sig.params.len());
        assert!(matches!(sig.params[0], ColumnType::Varchar(_)));
        assert!(matches!(sig.params[1], ColumnType::Int(_)));
        assert!(matches!(sig.params[2], ColumnType::Int(_)));
        Ok(())
    }
}