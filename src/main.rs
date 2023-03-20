use std::alloc::Layout;
use std::cell::RefCell;
use std::cmp;
use std::cmp::Ordering;
use std::fmt;
use std::fmt::Formatter;
use std::io;
use std::io::Write;
use std::ops::{Deref, DerefMut};
use std::rc::Rc;
use std::vec::Vec;

use crate::arena::Arena;

mod storage;
mod key;
mod arena;
mod status;
mod skip_list;
mod comparator;
mod mai2;
mod db_impl;
mod column_family;
mod version;
mod env;
mod files;
mod marshal;
mod varint;

fn main() {
    println!("ok")
}
