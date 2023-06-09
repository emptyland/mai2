use std::any::Any;
use std::collections::HashMap;
use crate::{Corrupting, Result, Status};
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
            distinct
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
    fn signature(&self) -> ColumnType { ColumnType::BigInt(11) }
    fn new_buf(&self, arena: &ArenaMut<Arena>) -> ArenaBox<dyn Writable>;
    fn iterate(&self, dest: &mut dyn Writable, args: &[ArenaBox<dyn Writable>]) -> Result<()>;
    fn terminate(&self, dest: &mut dyn Writable) -> Result<Value>;
    fn merge(&self, dest: &mut dyn Writable, input: &dyn Writable) -> Result<()>;
}

pub trait Writable {
    fn any(&self) -> &dyn Any;
    fn any_mut(&mut self) -> &mut dyn Any;
    fn recv(&mut self, value: &Value) {}
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
            return Err(Status::corrupted("bad format"))
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
            return Err(Status::corrupted("bad format"))
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
        pub struct Count;
        impl Count {
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

    #[derive(Default)]
    struct NumberBuf<T: Default> {
        value: T,
        rows: usize,
    }

    impl <T: Default + 'static> NumberBuf<T> {
        fn cast(input: &dyn Writable) -> &Self {
            input.any().downcast_ref::<Self>().unwrap()
        }

        fn cast_mut(input: &mut dyn Writable) -> &mut Self {
            input.any_mut().downcast_mut::<Self>().unwrap()
        }
    }

    impl <T: Default + 'static> Writable for NumberBuf<T> {
        fn any(&self) -> &dyn Any {
            self
        }

        fn any_mut(&mut self) -> &mut dyn Any {
            self
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
    udfs: HashMap<&'static str, fn (&ExecutionContext)->ArenaBox<dyn UDF>>,
    udafs: HashMap<&'static str, fn (&ExecutionContext)->ArenaBox<dyn Aggregator>>
}

impl BuiltinFns {
    fn new() -> Self {
        let mut udfs = HashMap::<&'static str, fn (&ExecutionContext)->ArenaBox<dyn UDF>>::new();
        udfs.insert("version", udf::Version::new);
        udfs.insert("length", udf::Length::new);
        udfs.insert("concat", udf::Concat::new);

        let mut udafs = HashMap::<&'static str, fn (&ExecutionContext)->ArenaBox<dyn Aggregator>>::new();
        udafs.insert("count", udaf::Count::new);
        Self {
            udfs,
            udafs,
        }
    }

    fn new_udf(&self, name: &str, ctx: &ExecutionContext) -> Option<ArenaBox<dyn UDF>> {
        match self.udfs.get(name) {
            Some(new) =>
                Some(new(ctx)),
            None => None
        }
    }

    fn new_udaf(&self, name: &str, ctx: &ExecutionContext) -> Option<ArenaBox<dyn Aggregator>> {
        match self.udafs.get(name) {
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