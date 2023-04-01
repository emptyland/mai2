use std::any::Any;
use std::cell::Cell;
use std::collections::HashSet;
use std::sync::{Arc, Weak};

use crate::mai2::Snapshot;

pub struct SnapshotSet {
    snapshots: Cell<Vec<Arc<SnapshotImpl>>>,
    sequence_numbers: Cell<HashSet<u64>>,
}

impl SnapshotSet {
    pub fn new() -> Arc<SnapshotSet> {
        Arc::new(Self {
            snapshots: Cell::new(Vec::new()),
            sequence_numbers: Cell::new(HashSet::new()),
        })
    }

    pub fn new_snapshot(this: &Arc<SnapshotSet>, sequence_number: u64, ts: u64) -> Arc<dyn Snapshot> {
        let snapshot = Arc::new(SnapshotImpl {
            sequence_number,
            ts,
            owns: Arc::downgrade(this),
        });
        let mut container = this.snapshots.take();
        let mut numbers = this.sequence_numbers.take();
        container.push(snapshot.clone());
        numbers.insert(snapshot.sequence_number);
        this.snapshots.set(container);
        this.sequence_numbers.set(numbers);
        snapshot
    }

    pub fn remove_snapshot(&self, target: Arc<dyn Snapshot>) {
        let mut numbers = self.sequence_numbers.take();
        let snapshot_impl = SnapshotImpl::from(&target);
        assert!(numbers.contains(&snapshot_impl.sequence_number));

        let container = self.snapshots.take();
        self.snapshots.set(container.iter()
            .filter(|x| {
                (*x).sequence_number != snapshot_impl.sequence_number
            })
            .map(|x| {
                x.clone()
            })
            .collect());

        numbers.remove(&snapshot_impl.sequence_number);
        self.sequence_numbers.set(numbers);
    }

    pub fn is_snapshot_valid(&self, sequence_number: u64) -> bool {
        let numbers = self.sequence_numbers.take();
        let exists = numbers.contains(&sequence_number);
        self.sequence_numbers.set(numbers);
        exists
    }

    pub fn oldest(&self) -> Arc<SnapshotImpl> {
        let container = self.snapshots.take();
        let rv = container.first().unwrap().clone();
        self.snapshots.set(container);
        rv
    }
}

pub struct SnapshotImpl {
    pub sequence_number: u64,
    ts: u64,
    owns: Weak<SnapshotSet>,
}

impl SnapshotImpl {
    pub fn from(handle: &Arc<dyn Snapshot>) -> &Self {
        handle.as_any().downcast_ref::<Self>().unwrap()
    }
}

impl Snapshot for SnapshotImpl {
    fn as_any(&self) -> &dyn Any {
        self
    }
}