#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde;
extern crate serde_yaml;

pub use crate::base::*;
pub use crate::status::*;

pub mod storage;
pub mod exec;
pub mod status;
mod base;
mod sql;
mod suite;

pub type Result<T> = std::result::Result<T, Status>;

#[macro_export]
macro_rules! switch {
    ($cond:expr, $then:expr, $others:expr) => {
        if $cond {$then} else {$others}
    }
}
