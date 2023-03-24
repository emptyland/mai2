use std::cell::{Ref, RefCell};
use std::cmp::Ordering;
use std::ops::DerefMut;
use std::ptr::addr_of;
use std::rc::Rc;
use std::sync::Arc;

use crate::{iterator, mai2, skip_list};
use crate::arena::{Arena, ScopedMemory};
use crate::comparator::Comparator;
use crate::iterator::Iterator;
use crate::key::{InternalKey, InternalKeyComparator, KeyBundle, Tag};
use crate::skip_list::{Comparing, SkipList};
use crate::status::Status;

pub struct MemoryTable {
    arena: Rc<RefCell<Arena>>,
    table: SkipList<KeyBundle, KeyComparator>,
}

struct KeyComparator {
    internal_key_cmp: InternalKeyComparator,
}

impl Comparing<&KeyBundle> for KeyComparator {
    fn cmp(&self, lhs: &KeyBundle, rhs: &KeyBundle) -> Option<Ordering> {
        Some(self.internal_key_cmp.compare(lhs.key(), rhs.key()))
    }
}

impl MemoryTable {
    pub fn new(internal_key_cmp: InternalKeyComparator) -> Self {
        let arena = Arena::new_rc();
        let key_cmp = KeyComparator { internal_key_cmp };
        let table = SkipList::new(arena.clone(), key_cmp);
        Self {
            arena: arena.clone(),
            table,
        }
    }

    pub fn new_rc(internal_key_cmp: InternalKeyComparator) -> Arc<RefCell<Self>> {
        Arc::new(RefCell::new(Self::new(internal_key_cmp)))
    }

    pub fn iter(this: Arc<RefCell<MemoryTable>>) -> IteratorImpl {
        let table_ptr = addr_of!(this.borrow().table);
        IteratorImpl {
            owns: this.clone(),
            status: Status::Ok,
            iter: skip_list::IteratorImpl::new(table_ptr),
        }
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8], sequence_number: u64, tag: Tag) {
        let internal_key = {
            let mut arena = self.arena.borrow_mut();
            KeyBundle::new(arena.deref_mut(), tag, sequence_number, key, value)
        };

        self.table.insert(&internal_key)
    }

    pub fn get(&self, key: &[u8], sequence_number: u64) -> mai2::Result<(&[u8], Tag)> {
        let mut chunk = ScopedMemory::new();
        let internal_key = KeyBundle::for_key(&mut chunk, sequence_number, key);
        let mut iter = skip_list::IteratorImpl::new(&self.table);
        iter.seek(&internal_key);

        if !iter.valid() {
            //println!("key != valid");
            return Err(Status::NotFound);
        }

        let found_key = iter.key().unwrap();
        if found_key.user_key() != key {
            //println!("key != target");
            return Err(Status::NotFound);
        }

        Ok((found_key.value(), found_key.tag()))
    }

    pub fn approximate_memory_usage() -> usize {
        todo!()
    }
}

pub struct IteratorImpl {
    owns: Arc<RefCell<MemoryTable>>,
    status: Status,
    iter: skip_list::IteratorImpl<KeyBundle, KeyComparator>,
}

impl std::iter::Iterator for IteratorImpl {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.valid() {
            let rv = Some((Vec::from(self.key()), Vec::from(self.value())));
            self.iter.next();
            rv
        } else {
            None
        }
    }
}

impl iterator::Iterator for IteratorImpl {
    fn valid(&self) -> bool {
        self.iter.valid()
    }

    fn seek_to_first(&mut self) {
        self.iter.seek_to_first();
    }

    fn seek_to_last(&mut self) {
        todo!()
    }

    fn seek(&mut self, key: &[u8]) {
        let unpacked_key = InternalKey::parse(key);
        let mut chunk = ScopedMemory::new();
        let internal_key = KeyBundle::for_key(&mut chunk,
                                              unpacked_key.sequence_number,
                                              unpacked_key.user_key);
        self.iter.seek(&internal_key);
    }

    fn next(&mut self) {
        self.iter.next();
    }

    fn prev(&mut self) {
        todo!()
    }

    fn key(&self) -> &[u8] {
        self.iter.key().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.iter.key().unwrap().value()
    }

    fn status(&self) -> Status {
        self.status.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::comparator::BitwiseComparator;

    use super::*;

    #[test]
    fn sanity() {
        let mut table = MemoryTable::new(default_internal_key_cmp());

        table.insert("aaa".as_bytes(), "boo".as_bytes(), 1, Tag::KEY);
        table.insert("aaa".as_bytes(), "obo".as_bytes(), 2, Tag::KEY);
        table.insert("aaa".as_bytes(), "bbb".as_bytes(), 3, Tag::DELETION);

        {
            let (value, tag) = table.get("aaa".as_bytes(), 1).unwrap();
            assert_eq!("boo", String::from_utf8_lossy(value));
            assert_eq!(Tag::KEY, tag);
        }

        {
            let (value, tag) = table.get("aaa".as_bytes(), 2).unwrap();
            assert_eq!("obo", String::from_utf8_lossy(value));
            assert_eq!(Tag::KEY, tag);
        }
    }

    #[test]
    fn insert_keys() {
        let core = MemoryTable::new(default_internal_key_cmp());
        let mut table = Rc::new(core);

        let rf = Rc::get_mut(&mut table).unwrap();
        rf.insert("aaa".as_bytes(), "boo".as_bytes(), 1, Tag::KEY);

        //Rc::make_mut(&mut table);
    }

    fn default_internal_key_cmp() -> InternalKeyComparator {
        let user_cmp = Rc::new(BitwiseComparator {});
        InternalKeyComparator::new(user_cmp)
    }
}