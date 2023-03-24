use crate::status::Status;

pub trait Iterator: std::iter::Iterator {
    fn valid(&self) -> bool;
    fn seek_to_first(&mut self);
    fn seek_to_last(&mut self);
    fn seek(&mut self, key: &[u8]);

    fn next(&mut self);
    fn prev(&mut self);

    fn key(&self) -> &[u8];
    fn value(&self) -> &[u8];

    fn status(&self) -> Status;
}