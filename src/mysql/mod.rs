#![allow(unused_variables)]
#![allow(dead_code)]

use std::io;
use std::io::Write;

mod protocol;

pub trait Marshal {
    fn marshal(&self, writer: &mut dyn Write) -> io::Result<usize>;
}