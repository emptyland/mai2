use std::cell::RefCell;
use std::cmp::Ordering::Equal;
use std::iter::Iterator as StdIterator;
use std::rc::Rc;
use std::sync::Arc;

use crate::storage::Comparator;
use crate::storage::key::{InternalKey, Tag};
use crate::storage::Status;

pub trait Iterator {
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

pub type IteratorRc = Rc<RefCell<dyn Iterator>>;
pub type IteratorArc = Arc<RefCell<dyn Iterator>>;

#[derive(Clone, Debug, PartialEq)]
pub enum Direction {
    Forward,
    Reserve,
}

pub struct DBIterator {
    valid: bool,
    direction: Direction,
    cmp: Rc<dyn Comparator>,
    iter: IteratorRc,
    last_sequence_number: u64,
    saved_key: Vec<u8>,
    saved_value: Vec<u8>,
    status: Status,
}

impl DBIterator {
    pub fn new(cmp: &Rc<dyn Comparator>, iter: IteratorRc, last_sequence_number: u64) -> Self {
        Self {
            valid: false,
            direction: Direction::Forward,
            cmp: cmp.clone(),
            iter,
            last_sequence_number,
            saved_value: Vec::default(),
            saved_key: Vec::default(),
            status: Status::Ok,
        }
    }

    fn clear_saved_value(&mut self) {
        if self.saved_value.capacity() > 1048576 {
            self.saved_value = Vec::default();
        } else {
            self.saved_value.clear();
        }
    }

    fn find_next_user_entry(&mut self, mut skipping: bool) -> Option<Vec<u8>> {
        // Loop until we hit an acceptable entry to yield
        assert!(self.iter.borrow().valid());
        assert_eq!(self.direction, Direction::Forward);
        let mut skip = Option::<Vec<u8>>::None;
        loop {
            let internal_key = InternalKey::parse(self.iter.borrow().key());
            if internal_key.sequence_number <= self.last_sequence_number {
                match internal_key.tag {
                    Tag::Deletion => {
                        // Arrange to skip all upcoming entries for this key since
                        // they are hidden by this deletion.
                        skip = Some(internal_key.user_key.to_vec());
                        skipping = true;
                    }
                    Tag::Key => {
                        if skipping && self.cmp.le(internal_key.user_key, skip.as_ref().unwrap()) {
                            // Hidden it
                        } else {
                            self.valid = true;
                            self.saved_key.clear();
                            return skip;
                        }
                    }
                }
            }
            self.iter.borrow_mut().move_next();
            if !self.iter.borrow().valid() {
                break;
            }
        }
        self.saved_key.clear();
        self.valid = false;
        skip
    }

    fn find_prev_user_entry(&mut self) {
        assert_eq!(Direction::Reserve, self.direction);

        let mut tag = Tag::Deletion;
        if self.iter.borrow().valid() {
            loop {
                let internal_key = InternalKey::parse(self.iter.borrow().key());
                if internal_key.sequence_number <= self.last_sequence_number {
                    if tag != Tag::Deletion && self.cmp.lt(internal_key.user_key, &self.saved_key) {
                        // We encountered a non-deleted value in entries for previous keys,
                        break;
                    }
                    tag = internal_key.tag;
                    if tag == Tag::Deletion {
                        self.saved_key.clear();
                        self.clear_saved_value();
                    } else {
                        self.saved_key = InternalKey::extract_user_key(self.iter.borrow().key())
                            .to_vec();
                        self.saved_value = self.iter.borrow().value().to_vec();
                    }
                }
                self.iter.borrow_mut().move_prev();
                if !self.iter.borrow().valid() {
                    break;
                }
            }
        }

        if tag == Tag::Deletion {
            // End
            self.valid = false;
            self.saved_key.clear();
            self.clear_saved_value();
            self.direction = Direction::Forward;
        } else {
            self.valid = true;
        }
    }

    fn update_valid(&mut self) {
        if self.iter.borrow().valid() {
            if let Some(key) = self.find_next_user_entry(false) {
                self.saved_key = key;
            }
        } else {
            self.valid = false;
        }
    }
}

impl Iterator for DBIterator {
    fn valid(&self) -> bool {
        self.valid
    }

    fn seek_to_first(&mut self) {
        self.direction = Direction::Forward;
        self.clear_saved_value();
        self.iter.borrow_mut().seek_to_first();
        self.update_valid();
    }

    fn seek_to_last(&mut self) {
        self.direction = Direction::Reserve;
        self.clear_saved_value();
        self.iter.borrow_mut().seek_to_last();
        self.find_prev_user_entry();
    }

    fn seek(&mut self, key: &[u8]) {
        self.direction = Direction::Forward;
        self.clear_saved_value();
        self.saved_key.clear();

        let internal_key = InternalKey::from_key(key, self.last_sequence_number,
                                                 Tag::Key);
        self.iter.borrow_mut().seek(&internal_key);
        self.update_valid();
    }

    fn move_next(&mut self) {
        assert!(self.valid);
        if self.direction == Direction::Reserve {
            self.direction = Direction::Forward;
            // iter_ is pointing just before the entries for this->key(),
            // so advance into the range of entries for this->key() and then
            // use the normal skipping code below.
            if !self.iter.borrow().valid() {
                self.iter.borrow_mut().seek_to_first();
            } else {
                self.iter.borrow_mut().move_next();
            }

            if !self.iter.borrow().valid() {
                self.valid = false;
                self.saved_key.clear();
                return;
            }
            // saved_key_ already contains the key to skip past.
        } else {
            // Store in saved_key_ the current key so we skip it below.
            self.saved_key = InternalKey::extract_user_key(self.iter.borrow().key()).to_vec();
        }
        if let Some(key) = self.find_next_user_entry(true) {
            self.saved_key = key;
        }
    }

    fn move_prev(&mut self) {
        todo!()
    }

    fn key(&self) -> &[u8] {
        assert!(self.valid());
        if self.direction == Direction::Forward {
            InternalKey::extract_user_key(unsafe { (*self.iter.as_ptr()).key() })
        } else {
            &self.saved_key
        }
    }

    fn value(&self) -> &[u8] {
        assert!(self.valid());
        if self.direction == Direction::Forward {
            unsafe { (*self.iter.as_ptr()).value() }
        } else {
            &self.saved_value
        }
    }

    fn status(&self) -> Status {
        if self.status == Status::Ok {
            self.iter.borrow().status()
        } else {
            self.status.clone()
        }
    }
}

pub struct MergingIterator<'a> {
    cmp: Box<dyn Comparator>,
    children: Vec<IteratorWrapper<'a>>,
    current: Option<usize>,
    direction: Direction,
}

impl<'a> MergingIterator<'a> {
    pub fn new<Cmp: Comparator + 'static>(cmp: Cmp, children: Vec<IteratorRc>) -> Self {
        let mut id = 0usize;
        Self {
            cmp: Box::new(cmp),
            children: children.iter().map(|x| {
                let iter = IteratorWrapper::new(id, x);
                id += 1;
                iter
            }).collect(),
            direction: Direction::Forward,
            current: None,
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
                    }
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
                    }
                    None => largest = Some(i)
                }
            }
        }
        self.current = largest;
    }

    fn current(&self) -> &IteratorWrapper {
        &self.children[self.current.unwrap()]
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
        self.children.iter_mut().for_each(|x| { x.seek_to_first() });
        self.direction = Direction::Forward;
        self.find_smallest();
    }

    fn seek_to_last(&mut self) {
        self.children.iter_mut().for_each(|x| { x.seek_to_last() });
        self.direction = Direction::Reserve;
        self.find_largest();
    }

    fn seek(&mut self, key: &[u8]) {
        self.children.iter_mut().for_each(|x| { x.seek(key) });
        self.direction = Direction::Forward;
        self.find_smallest();
    }

    fn move_next(&mut self) {
        assert!(self.valid());
        if self.direction != Direction::Forward {
            let key = self.key().to_vec();
            self.children.iter_mut()
                .filter(|x| { x.id != self.current.unwrap() })
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
                .filter(|x| { x.id != self.current.unwrap() })
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
    delegated: IteratorRc,
    key: Option<&'a [u8]>,
}

impl<'a> IteratorWrapper<'a> {
    pub fn new(id: usize, iter: &IteratorRc) -> Self {
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

impl<'a> Iterator for IteratorWrapper<'a> {
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


#[cfg(test)]
mod tests {
    use crate::storage::BitwiseComparator;
    use crate::storage::key::InternalKeyComparator;

    use super::*;

    #[test]
    fn sanity() {
        let mut iters = Vec::<IteratorRc>::default();

        let mut iter1 = MemoryMockIter::default();
        iter1.insert_str("1111", 2, Tag::Key);
        iter1.insert_str("1111", 1, Tag::Key);
        iter1.insert_str("2222", 3, Tag::Key);
        iters.push(Rc::new(RefCell::new(iter1)));

        let mut iter2 = MemoryMockIter::default();
        iter2.insert_str("3333", 5, Tag::Key);
        iter2.insert_str("3333", 4, Tag::Key);
        iter2.insert_str("4444", 6, Tag::Key);
        iters.push(Rc::new(RefCell::new(iter2)));

        let mut iter = MergingIterator::new(default_internal_key_cmp(), iters);
        iter.seek_to_first();
        assert!(iter.valid());
        InternalKey::_test_expect(iter.key(), "1111", 2, Tag::Key);

        iter.move_next();
        assert!(iter.valid());
        InternalKey::_test_expect(iter.key(), "1111", 1, Tag::Key);

        iter.move_next();
        assert!(iter.valid());
        InternalKey::_test_expect(iter.key(), "2222", 3, Tag::Key);

        iter.move_next();
        assert!(iter.valid());
        InternalKey::_test_expect(iter.key(), "3333", 5, Tag::Key);

        iter.move_next();
        assert!(iter.valid());
        InternalKey::_test_expect(iter.key(), "3333", 4, Tag::Key);

        iter.move_next();
        assert!(iter.valid());
        InternalKey::_test_expect(iter.key(), "4444", 6, Tag::Key);

        iter.move_next();
        assert!(!iter.valid());
    }

    #[test]
    fn cross_merging_iter() {
        let mut iters = Vec::<IteratorRc>::default();

        let mut iter1 = MemoryMockIter::default();
        iter1.insert_str("1111", 2, Tag::Key);
        iter1.insert_str("2222", 3, Tag::Key);
        iter1.insert_str("3333", 5, Tag::Key);
        iters.push(Rc::new(RefCell::new(iter1)));

        let mut iter2 = MemoryMockIter::default();
        iter2.insert_str("1111", 1, Tag::Key);
        iter2.insert_str("2222", 4, Tag::Key);
        iter2.insert_str("3333", 6, Tag::Key);
        iters.push(Rc::new(RefCell::new(iter2)));

        let mut iter = MergingIterator::new(default_internal_key_cmp(), iters);
        iter.seek_to_first();
        assert!(iter.valid());
        InternalKey::_test_expect(iter.key(), "1111", 2, Tag::Key);

        iter.move_next();
        assert!(iter.valid());
        InternalKey::_test_expect(iter.key(), "1111", 1, Tag::Key);

        iter.move_next();
        assert!(iter.valid());
        InternalKey::_test_expect(iter.key(), "2222", 4, Tag::Key);

        iter.move_next();
        assert!(iter.valid());
        InternalKey::_test_expect(iter.key(), "2222", 3, Tag::Key);

        iter.move_next();
        assert!(iter.valid());
        InternalKey::_test_expect(iter.key(), "3333", 6, Tag::Key);

        iter.move_next();
        assert!(iter.valid());
        InternalKey::_test_expect(iter.key(), "3333", 5, Tag::Key);

        iter.move_next();
        assert!(!iter.valid());
    }

    fn default_internal_key_cmp() -> InternalKeyComparator {
        let user_cmp = Rc::new(BitwiseComparator {});
        InternalKeyComparator::new(user_cmp)
    }

    struct MemoryMockIter {
        data: Vec<Vec<u8>>,
        iter: usize,
        cmp: InternalKeyComparator,
    }

    impl MemoryMockIter {
        fn new(cmp: &InternalKeyComparator) -> Self {
            Self {
                data: Vec::default(),
                iter: 0,
                cmp: cmp.clone(),
            }
        }

        fn default() -> Self {
            let internal_key_cmp = default_internal_key_cmp();
            Self::new(&internal_key_cmp)
        }

        fn insert(&mut self, key: &[u8], sequence_number: u64, tag: Tag) {
            self.data.push(InternalKey::from_key(key, sequence_number, tag))
        }

        fn insert_str(&mut self, key: &str, sequence_number: u64, tag: Tag) {
            self.insert(key.as_bytes(), sequence_number, tag)
        }
    }

    impl Iterator for MemoryMockIter {
        fn valid(&self) -> bool {
            self.iter < self.data.len()
        }

        fn seek_to_first(&mut self) {
            self.iter = 0
        }

        fn seek_to_last(&mut self) {
            self.iter = self.data.len() - 1;
        }

        fn seek(&mut self, target: &[u8]) {
            self.iter = self.data.len();

            for i in 0..self.data.len() {
                let key = &self.data[i];
                if !self.cmp.lt(key, target) {
                    self.iter = i;
                    break;
                }
            }
        }

        fn move_next(&mut self) {
            self.iter += 1;
        }

        fn move_prev(&mut self) {
            self.iter -= 1;
        }

        fn key(&self) -> &[u8] {
            &self.data[self.iter]
        }

        fn value(&self) -> &[u8] {
            self.key()
        }

        fn status(&self) -> Status {
            Status::Ok
        }
    }
}
