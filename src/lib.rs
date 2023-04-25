mod storage;
mod sql;
mod exec;
mod base;
pub mod status;

pub type Result<T> = std::result::Result<T, Status>;

#[macro_use]
extern crate lazy_static;
extern crate serde_yaml;
#[macro_use]
extern crate serde;

pub use crate::status::*;

//fn main() {}
