extern crate core;

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

fn main() {
    issue4();
    issue5();
}

#[warn(dead_code)]
fn issue0() {
    let guess = String::new();
    let r = Rc::new(RefCell::new(guess));
    let c = r.clone();

    io::stdin()
        .read_line(r.deref().borrow_mut().deref_mut())
        .expect("Failed to read line");

    println!("You guessed: {:?}", (*c).borrow().deref());

    (*r).borrow_mut().clear();

    println!("You guessed: {:?}", (*c).borrow().deref());
}

#[warn(dead_code)]
fn issue1() {
    let mut nums = vec![1, 2, 3];
    nums.push(4);
    nums.push(5);
    println!("nums: {:?}", nums);

    let copied = nums.clone();
    assert_eq!(nums, copied);
}


#[warn(dead_code)]
fn issue2() {
    let raw_s: &str = "ok";
    let mut ss = String::from(raw_s);
    println!("rawS:{:?}, ss:{:?}", raw_s, ss);

    ss.push_str(",ok");
    println!("capacity: {:?}", ss.capacity());

    let mut raw_ss = ss.as_str();
    //ss.push_str("1");
    println!("rawS:{:?}, ss:{:?}", raw_ss, ss);

    println!("[0]:{:?}", ss.get(0..1));
}

pub trait Foo {
    fn do_it(&self);
    fn do_that(&self);
}

pub trait Bar {
    fn do_it(&self);
}

pub struct Foo1 {
    pub id: i32,
    pub name: String,
}

impl fmt::Debug for dyn Foo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Foo")
            .field("unknown", &1)
            .finish()
    }
}

impl Foo for Foo1 {
    fn do_it(&self) {
        println!("[1] do_it, id: {:?}, name: {:?}", self.id, self.name)
    }

    fn do_that(&self) {
        println!("[1] do_that, id: {:?}, name: {:?}", self.id, self.name)
    }
}

impl Bar for Foo1 {
    fn do_it(&self) {
        println!("[1] Bar.do_it, id: {:?}, name: {:?}", self.id, self.name)
    }
}

impl Clone for Foo1 {
    fn clone(&self) -> Self {
        Foo1 {
            id: self.id,
            name: self.name.clone(),
        }
    }
}

impl Foo1 {
    fn do_this(&self) {
        println!("[1] do_this, id: {:?}, name: {:?}", self.id, self.name)
    }
}

pub struct Foo2 {
    pub x: f32,
    pub y: f32,
}

impl Foo for Foo2 {
    fn do_it(&self) {
        println!("[2] do_it, x: {:?}, y: {:?}", self.x, self.y);
    }

    fn do_that(&self) {
        println!("[2] do_that, x: {:?}, y: {:?}", self.x, self.y);
    }
}

pub struct Foo3<'a> {
    id: i32,
    nick: &'a str,
    doom: &'a str,
    name: String,
}

impl Foo for Foo3<'_> {
    fn do_it(&self) {
        println!("[3] do_it, id: {}, nick: {}, doom: {}, name: {}", self.id, self.nick, self.doom,
                 self.name);
    }

    fn do_that(&self) {
        println!("[3] do_that, id: {}, nick: {}, doom: {}, name: {}", self.id, self.nick, self.doom,
                 self.name);
    }
}

#[warn(dead_code)]
fn issue3() {
    let foo1 = Foo1 {
        id: 1,
        name: String::from("ok"),
    };
    let foo2 = Foo2 {
        x: 1.0f32,
        y: 2.0f32,
    };

    let mut foo: Box<dyn Foo> = Box::new(foo1.clone());
    foo.do_that();
    foo.do_it();

    println!("foo: {:#?}", foo);

    let mut bar: Box<dyn Bar> = Box::new(foo1);
    bar.do_it();

    foo = Box::new(foo2);
    foo.do_that();
    foo.do_it();

    foo = Box::new(Foo3 {
        id: 0,
        nick: "fo",
        doom: "doom",
        name: String::from("foo3"),
    });

    foo.do_it();
    foo.do_that();
}

#[warn(dead_code)]
fn issue4() {
    let mut arena = Arena::new();
    let kb = key::KeyBundle::for_key_value(&mut arena, 0, "ok".as_bytes(), "111".as_bytes());
    //assert_eq!(key::Tag::KEY, kb.tag());
    assert_eq!(0, kb.sequence_number());
    dbg!(kb);
}

#[warn(dead_code)]
fn issue5() {
    let mut arena = Arena::new();
    let hello = "hello";

    let mut chunk = arena.allocate(Layout::from_size_align(16, 4).unwrap()).unwrap();

    unsafe {
        assert_eq!(16, chunk.as_ref().len());
        chunk.as_mut().write(hello.as_bytes()).expect("TODO: panic message");
        dbg!(chunk.as_mut());
    }
}