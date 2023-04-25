use std::cell::{Cell, RefCell};
use std::cmp::Ordering;
use std::ptr::addr_of;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{AcqRel, Acquire};

use crate::storage::{inline_skip_list, skip_list};
use crate::base::{Arena, ScopedMemory};
use crate::storage::comparator::Comparator;
use crate::storage::inline_skip_list::InlineSkipList;
use crate::storage::Iterator;
use crate::storage::key::{InternalKey, InternalKeyComparator, KeyBundle, Tag};
use crate::storage::skip_list::Comparing;
use crate::{Status, Result};

pub struct MemoryTable {
    arena: Rc<RefCell<Arena>>,
    table: InlineSkipList<'static, KeyComparator>,
    associated_file_number: Cell<u64>,
    n_entries: AtomicUsize,
}

struct KeyComparator {
    internal_key_cmp: InternalKeyComparator,
}

impl Comparing<&[u8]> for KeyComparator {
    fn cmp(&self, lhs: &[u8], rhs: &[u8]) -> Option<Ordering> {
        Some(self.internal_key_cmp.compare(lhs, rhs))
    }
}

impl MemoryTable {
    pub fn new(internal_key_cmp: InternalKeyComparator) -> Self {
        let arena = Arena::new_rc();
        let key_cmp = KeyComparator { internal_key_cmp };
        let table = InlineSkipList::new(arena.clone(), key_cmp);
        Self {
            arena: arena.clone(),
            table,
            associated_file_number: Cell::new(0),
            n_entries: AtomicUsize::new(0),
        }
    }

    pub fn new_rc(internal_key_cmp: InternalKeyComparator) -> Arc<Self> {
        Arc::new(Self::new(internal_key_cmp))
    }

    pub fn iter(this: Arc<MemoryTable>) -> IteratorImpl {
        let table_ptr = addr_of!(this.table);
        IteratorImpl {
            owns: this.clone(),
            status: Status::Ok,
            iter: this.table.iter(),
        }
    }

    pub fn insert(&self, key: &[u8], value: &[u8], sequence_number: u64, tag: Tag) {
        self.n_entries.fetch_add(1, AcqRel);
        self.table.insert(tag, sequence_number, key, value)
    }

    pub fn get(&self, key: &[u8], sequence_number: u64) -> Result<(&[u8], Tag)> {
        let mut chunk = ScopedMemory::new();
        let internal_key = KeyBundle::from_key(&mut chunk, sequence_number, key);

        let mut iter = self.table.iter();
        iter.seek(&internal_key.key());
        if !iter.valid() {
            return Err(Status::NotFound);
        }

        let found_key = InternalKey::parse(iter.key().unwrap());
        if found_key.user_key != key {
            return Err(Status::NotFound);
        }

        Ok((iter.value().unwrap(), found_key.tag))
    }

    pub fn approximate_memory_usage(&self) -> usize {
        self.arena.borrow().use_in_bytes
    }

    pub fn associate_file_number_to(&self, file_number: u64) {
        self.associated_file_number.set(file_number);
    }

    pub fn associated_file_number(&self) -> u64 {
        self.associated_file_number.get()
    }

    pub fn number_of_entries(&self) -> usize {
        self.n_entries.load(Acquire)
    }
}

pub struct IteratorImpl {
    owns: Arc<MemoryTable>,
    status: Status,
    iter: inline_skip_list::IteratorImpl<'static, KeyComparator>,
}

impl Iterator for IteratorImpl {
    fn valid(&self) -> bool {
        self.iter.valid()
    }

    fn seek_to_first(&mut self) {
        self.iter.seek_to_first();
    }

    fn seek_to_last(&mut self) {
        self.iter.seek_to_last();
    }

    fn seek(&mut self, key: &[u8]) {
        let unpacked_key = InternalKey::parse(key);
        let mut chunk = ScopedMemory::new();
        let internal_key = KeyBundle::from_key(&mut chunk,
                                               unpacked_key.sequence_number,
                                               unpacked_key.user_key);
        self.iter.seek(internal_key.key());
    }

    fn move_next(&mut self) {
        self.iter.next();
    }

    fn move_prev(&mut self) {
        self.iter.prev();
    }

    fn key(&self) -> &[u8] {
        self.iter.key().unwrap()
    }

    fn value(&self) -> &[u8] {
        self.iter.value().unwrap()
    }

    fn status(&self) -> Status {
        self.status.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::BitwiseComparator;

    use super::*;

    #[test]
    fn sanity() {
        let table = MemoryTable::new(default_internal_key_cmp());

        table.insert("aaa".as_bytes(), "boo".as_bytes(), 1, Tag::Key);
        table.insert("aaa".as_bytes(), "obo".as_bytes(), 2, Tag::Key);
        table.insert("aaa".as_bytes(), "bbb".as_bytes(), 3, Tag::Deletion);

        {
            let (value, tag) = table.get("aaa".as_bytes(), 1).unwrap();
            assert_eq!("boo", String::from_utf8_lossy(value));
            assert_eq!(Tag::Key, tag);
        }

        {
            let (value, tag) = table.get("aaa".as_bytes(), 2).unwrap();
            assert_eq!("obo", String::from_utf8_lossy(value));
            assert_eq!(Tag::Key, tag);
        }
    }

    #[test]
    fn insert_keys() {
        let core = MemoryTable::new(default_internal_key_cmp());
        let mut table = Rc::new(core);

        let rf = Rc::get_mut(&mut table).unwrap();
        rf.insert("aaa".as_bytes(), "boo".as_bytes(), 1, Tag::Key);

        //Rc::make_mut(&mut table);
    }

    fn default_internal_key_cmp() -> InternalKeyComparator {
        let user_cmp = Rc::new(BitwiseComparator {});
        InternalKeyComparator::new(user_cmp)
    }
}