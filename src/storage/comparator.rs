use std::cmp::Ordering;

pub trait Comparator {
    fn compare(&self, lhs: &[u8], rhs: &[u8]) -> Ordering;

    fn lt(&self, lhs: &[u8], rhs: &[u8]) -> bool {
        matches!(self.compare(lhs, rhs), Ordering::Less)
    }

    fn le(&self, lhs: &[u8], rhs: &[u8]) -> bool {
        !matches!(self.compare(lhs, rhs), Ordering::Greater)
    }

    fn gt(&self, lhs: &[u8], rhs: &[u8]) -> bool {
        matches!(self.compare(lhs, rhs), Ordering::Greater)
    }

    fn ge(&self, lhs: &[u8], rhs: &[u8]) -> bool {
        !matches!(self.compare(lhs, rhs), Ordering::Less)
    }

    fn name(&self) -> String;

    fn find_shortest_separator(&self, _limit: &[u8]) -> Vec<u8> {
        todo!()
    }

    fn find_short_successor(&self) -> Vec<u8> {
        todo!()
    }
}

pub struct BitwiseComparator;

impl Comparator for BitwiseComparator {
    fn compare(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        lhs.cmp(rhs)
    }

    fn name(&self) -> String {
        String::from("bitwise-comparator")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanity() {
        let cmp: Box<dyn Comparator> = Box::new(BitwiseComparator {});
        let ord = cmp.compare("111".as_bytes(), "222".as_bytes());
        assert!(matches!(ord, Ordering::Less));
    }
}