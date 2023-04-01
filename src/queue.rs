use std::collections::vec_deque::VecDeque;
use std::sync::Mutex;

pub struct NonBlockingQueue<T> {
    queue: Mutex<VecDeque<T>>,
}

impl<T: Clone> NonBlockingQueue<T> {
    pub fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new())
        }
    }

    pub fn add(&self, item: T) {
        let mut q = self.queue.lock().unwrap();
        q.push_back(item);
    }

    pub fn take(&self) -> Option<T> {
        let mut q = self.queue.lock().unwrap();
        q.pop_front()
    }

    pub fn peek(&self) -> Option<T> {
        let q = self.queue.lock().unwrap();
        if q.is_empty() {
            None
        } else {
            Some(q.front().unwrap().clone())
        }
    }

    pub fn peek_all(&self) -> Vec<T> {
        let q = self.queue.lock().unwrap();
        q.iter().map(|x| { x.clone() }).collect()
    }

    pub fn is_empty(&self) -> bool {
        let q = self.queue.lock().unwrap();
        q.is_empty()
    }

    pub fn is_not_empty(&self) -> bool { !self.is_empty() }
}