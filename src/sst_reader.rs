use std::ptr::NonNull;
use crate::iterator;
use crate::key::InternalKeyComparator;
use crate::status::Status;

pub struct BlockIterator<'a> {
    internal_key_cmp: InternalKeyComparator,
    data: &'a [u8],
    restarts: &'a [u32],
    current_restarts: u64,
    current_local: u64,
    key: Vec<u8>,
    value: Vec<u8>,
}

// impl BlockIterator<'_> {
//     pub fn new(internal_key_cmp: InternalKeyComparator, block: NonNull<[u8]>) -> Self {
//
//     }
// }

impl Iterator for BlockIterator<'_> {
    type Item = ();

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl iterator::Iterator for BlockIterator<'_> {
    fn valid(&self) -> bool {
        todo!()
    }

    fn seek_to_first(&mut self) {
        todo!()
    }

    fn seek_to_last(&mut self) {
        todo!()
    }

    fn seek(&mut self, key: &[u8]) {
        todo!()
    }

    fn next(&mut self) {
        todo!()
    }

    fn prev(&mut self) {
        todo!()
    }

    fn key(&self) -> &[u8] {
        todo!()
    }

    fn value(&self) -> &[u8] {
        todo!()
    }

    fn status(&self) -> Status {
        todo!()
    }
}