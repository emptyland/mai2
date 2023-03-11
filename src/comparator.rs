use std::cmp::Ordering;

use crate::key::KeyBundle;

pub trait Comparing<T> {
    fn compare(&self, lhs: T, rhs: T) -> Ordering;

    fn lt(&self, lhs: T, rhs: T) -> bool {
        matches!(self.compare(lhs, rhs), Ordering::Less)
    }

    fn le(&self, lhs: T, rhs: T) -> bool {
        !matches!(self.compare(lhs, rhs), Ordering::Greater)
    }

    fn gt(&self, lhs: T, rhs: T) -> bool {
        matches!(self.compare(lhs, rhs), Ordering::Greater)
    }

    fn ge(&self, lhs: T, rhs: T) -> bool {
        !matches!(self.compare(lhs, rhs), Ordering::Less)
    }
}

pub fn new_bitwise_comparator<'a>() -> Box<dyn Comparing<&'a [u8]>> {
    Box::new(BitwiseComparator {})
}

pub type Comparator = Box<dyn for<'a> Comparing<&'a [u8]>>;

struct BitwiseComparator;

impl Comparing<&[u8]> for BitwiseComparator {
    fn compare(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        lhs.cmp(rhs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanity() {
        let cmp = new_bitwise_comparator();
        let ord = cmp.compare("111".as_bytes(), "222".as_bytes());
        assert!(matches!(ord, Ordering::Less));
    }
}