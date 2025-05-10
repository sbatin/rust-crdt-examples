use crate::gcounter::{Convergent, GCounter, ReplicaId};

#[derive(Debug, Clone, Default)]
pub struct PNCounter {
    pos: GCounter,
    neg: GCounter,
}

impl PNCounter {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn inc(&mut self, replica: ReplicaId) {
        self.pos.inc(replica);
    }

    pub fn dec(&mut self, replica: ReplicaId) {
        self.neg.inc(replica);
    }

    pub fn value(&self) -> i64 {
        self.pos.value() as i64 - self.neg.value() as i64
    }
}

impl Convergent for PNCounter {
    type Delta = Self;

    fn merge(&mut self, other: Self) {
        self.pos.merge(other.pos);
        self.neg.merge(other.neg);
    }

    fn merge_delta(&mut self, delta: Self::Delta) {
        self.merge(delta);
    }

    fn take_delta(&mut self) -> Option<Self::Delta> {
        Some(self.clone())
    }
}
