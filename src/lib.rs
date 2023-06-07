pub mod storage;
pub mod exec;
pub mod status;
mod base;
mod sql;

pub type Result<T> = std::result::Result<T, Status>;

#[macro_use]
extern crate lazy_static;
extern crate serde_yaml;
#[macro_use]
extern crate serde;

pub use crate::status::*;
pub use crate::base::*;

#[macro_export]
macro_rules! switch {
    ($cond:expr, $then:expr, $others:expr) => {
        if $cond {$then} else {$others}
    }
}
