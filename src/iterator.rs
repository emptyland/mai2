use std::cell::RefCell;
use std::cmp::Ordering::Equal;
use std::f32::consts::E;
use std::iter::Iterator as StdIterator;
use std::process::id;
use std::rc::Rc;
use crate::comparator::Comparator;
use crate::status::Status;

pub trait Iterator: StdIterator {
    fn valid(&self) -> bool;
    fn seek_to_first(&mut self);
    fn seek_to_last(&mut self);
    fn seek(&mut self, key: &[u8]);

    fn move_next(&mut self);
    fn move_prev(&mut self);

    fn key(&self) -> &[u8];
    fn value(&self) -> &[u8];

    fn status(&self) -> Status;
}

#[derive(Clone, Debug, PartialEq)]
pub enum Direction {
    Forward,
    Reserve,
}

pub struct MergingIterator<'a> {
    cmp: Box<dyn Comparator>,
    children: Vec<IteratorWrapper<'a>>,
    current: Option<usize>,
    direction: Direction,
}

impl<'a> MergingIterator<'a> {
    pub fn new<Cmp: Comparator + 'static>(cmp: Cmp,
                                          children: Vec<Rc<RefCell<dyn Iterator<Item=(Vec<u8>, Vec<u8>)>>>>) -> Self {
        let mut id = 0usize;
        Self {
            cmp: Box::new(cmp),
            children: children.iter().map(|x| {
                let iter = IteratorWrapper::new(id,x);
                id += 1;
                iter
            }).collect(),
            direction: Direction::Forward,
            current: None
        }
    }

    fn find_smallest(&mut self) {
        let mut smallest: Option<usize> = None;
        for i in 0..self.children.len() {
            let iter = &self.children[i];
            if iter.valid {
                match smallest.as_ref() {
                    Some(prev_idx) => {
                        let prev = &self.children[*prev_idx];
                        if self.cmp.lt(iter.key(), prev.key()) {
                            smallest = Some(i);
                        }
                    },
                    None => smallest = Some(i)
                }
            }
        }
        self.current = smallest;
    }

    fn find_largest(&mut self) {
        let mut largest: Option<usize> = None;
        for i in 0..self.children.len() {
            let iter = &self.children[i];
            if iter.valid {
                match largest.as_ref() {
                    Some(prev_idx) => {
                        let prev = &self.children[*prev_idx];
                        if self.cmp.gt(iter.key(), prev.key()) {
                            largest = Some(i);
                        }
                    },
                    None => largest = Some(i)
                }
            }
        }
        self.current = largest;
    }

    fn current(&self) -> &IteratorWrapper {
        &self.children[self.current.unwrap()]
    }

    // fn current_mut(&mut self) -> &mut IteratorWrapper {
    //     &mut self.children[self.current.unwrap()]
    // }
}

impl StdIterator for MergingIterator<'_> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl Iterator for MergingIterator<'_> {
    fn valid(&self) -> bool {
        match self.current.as_ref() {
            Some(idx) => self.children[*idx].valid,
            None => false
        }
    }

    fn seek_to_first(&mut self) {
        self.children.iter_mut().for_each(|x|{x.seek_to_first()});
        self.direction = Direction::Forward;
        self.find_smallest();
    }

    fn seek_to_last(&mut self) {
        self.children.iter_mut().for_each(|x|{x.seek_to_last()});
        self.direction = Direction::Reserve;
        self.find_largest();
    }

    fn seek(&mut self, key: &[u8]) {
        self.children.iter_mut().for_each(|x|{x.seek(key)});
        self.direction = Direction::Forward;
        self.find_smallest();
    }

    fn move_next(&mut self) {
        assert!(self.valid());
        if self.direction != Direction::Forward {
            let key = self.key().to_vec();
            self.children.iter_mut()
                .filter(|x| {x.id != self.current.unwrap()})
                .for_each(|iter| {
                    iter.seek(&key);
                    if iter.valid && self.cmp.compare(&key, iter.key()) == Equal {
                        iter.move_next();
                    }
                });
            self.direction = Direction::Forward;
        }
        self.children[self.current.unwrap()].move_next();
        self.find_smallest();
    }

    fn move_prev(&mut self) {
        assert!(self.valid());
        if self.direction != Direction::Reserve {
            let key = self.key().to_vec();
            self.children.iter_mut()
                .filter(|x| {x.id != self.current.unwrap()})
                .for_each(|iter| {
                    iter.seek(&key);
                    if iter.valid {
                        iter.move_prev();
                    } else {
                        iter.seek_to_last();
                    }
                });
            self.direction = Direction::Reserve;
        }
        self.children[self.current.unwrap()].move_prev();
        self.find_largest();
    }

    fn key(&self) -> &[u8] {
        assert!(self.valid());
        self.current().key()
    }

    fn value(&self) -> &[u8] {
        assert!(self.valid());
        self.current().value()
    }

    fn status(&self) -> Status {
        for iter in &self.children {
            if iter.status() != Status::Ok {
                return iter.status();
            }
        }
        Status::Ok
    }
}

pub struct IteratorWrapper<'a> {
    id: usize,
    valid: bool,
    delegated: Rc<RefCell<dyn Iterator<Item=(Vec<u8>, Vec<u8>)>>>,
    key: Option<&'a [u8]>,
}

impl<'a> IteratorWrapper<'a> {
    pub fn new(id: usize, iter: &Rc<RefCell<dyn Iterator<Item=(Vec<u8>, Vec<u8>)>>>) -> Self {
        Self {
            id,
            valid: false,
            delegated: iter.clone(),
            key: None,
        }
    }

    fn update(&mut self) {
        let iter = unsafe { &*self.delegated.as_ptr() };
        self.valid = iter.valid();
        if self.valid {
            self.key = Some(iter.key());
        } else {
            self.key = None;
        }
    }
}

impl <'a> StdIterator for IteratorWrapper<'a> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl <'a> Iterator for IteratorWrapper<'a> {
    fn valid(&self) -> bool { self.valid }

    fn seek_to_first(&mut self) {
        self.delegated.borrow_mut().seek_to_first();
        self.update()
    }

    fn seek_to_last(&mut self) {
        self.delegated.borrow_mut().seek_to_last();
        self.update()
    }

    fn seek(&mut self, key: &[u8]) {
        self.delegated.borrow_mut().seek(key);
        self.update()
    }

    fn move_next(&mut self) {
        self.delegated.borrow_mut().move_next();
        self.update()
    }

    fn move_prev(&mut self) {
        self.delegated.borrow_mut().move_prev();
        self.update()
    }

    fn key(&self) -> &[u8] {
        self.key.unwrap()
    }

    fn value(&self) -> &[u8] {
        unsafe { &*self.delegated.as_ptr() }.value()
    }

    fn status(&self) -> Status {
        self.delegated.borrow().status()
    }
}

