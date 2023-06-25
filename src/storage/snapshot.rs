use std::any::Any;
use std::collections::BTreeSet;
use std::sync::{Arc, Weak};
use crate::{log_warn, Logger};
use crate::storage::db::DBImpl;

use crate::storage::Snapshot;

pub struct SnapshotSet {
    logger: Arc<dyn Logger>,
    //snapshots: Vec<Arc<SnapshotImpl>>,
    sequence_numbers: BTreeSet<u64>
}

impl SnapshotSet {
    pub fn new(logger: &Arc<dyn Logger>) -> SnapshotSet {
        Self {
            logger: logger.clone(),
            //snapshots: Vec::default(),
            sequence_numbers: BTreeSet::default(),
        }
    }

    pub fn new_snapshot(&mut self, owns: &Weak<DBImpl>, sequence_number: u64, ts: u64) -> Arc<dyn Snapshot> {
        let snapshot = Arc::new(SnapshotImpl {
            owns: owns.clone(),
            sequence_number,
            ts
        });
        self.sequence_numbers.insert(snapshot.sequence_number);
        snapshot
    }

    pub fn remove_snapshot(&mut self, snapshot_impl: &SnapshotImpl) {
        //assert!(self.sequence_numbers.contains(&snapshot_impl.sequence_number));
        self.sequence_numbers.remove(&snapshot_impl.sequence_number);
    }

    pub fn is_snapshot_valid(&self, sequence_number: u64) -> bool {
        self.sequence_numbers.contains(&sequence_number)
    }

    pub fn oldest(&self) -> u64 {
        self.sequence_numbers.first().cloned().unwrap()
    }

    pub fn is_empty(&self) -> bool {
        self.sequence_numbers.is_empty()
    }
}

impl Drop for SnapshotSet {
    fn drop(&mut self) {
        if !self.sequence_numbers.is_empty() {
            log_warn!(self.logger, "snapshot not released: {}", self.sequence_numbers.len());
        }
    }
}

pub struct SnapshotImpl {
    owns: Weak<DBImpl>,
    pub sequence_number: u64,
    ts: u64
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

impl Drop for SnapshotImpl {
    fn drop(&mut self) {
        if let Some(db) = self.owns.upgrade() {
            //log_debug!(db.logger, "release snapshot: {} at {}", self.sequence_number, self.ts);
            db.snapshots.borrow_mut().remove_snapshot(self);
        }
    }
}