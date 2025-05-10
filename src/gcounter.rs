use std::collections::HashMap;

pub type ReplicaId = u64;

pub trait Convergent {
    type Delta;

    fn merge(&mut self, other: Self);

    fn merge_delta(&mut self, delta: Self::Delta);

    fn take_delta(&mut self) -> Option<Self::Delta>;
}

#[derive(Debug, Clone, Default)]
pub struct GCounter(HashMap<ReplicaId, usize>);

impl GCounter {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn keys(&self) -> impl Iterator<Item = &ReplicaId> {
        self.0.keys()
    }

    pub fn get(&self, replica: &ReplicaId) -> usize {
        self.0.get(replica).map_or(0, |v| *v)
    }

    pub fn value(&self) -> usize {
        self.0.values().sum()
    }

    pub fn inc(&mut self, replica: ReplicaId) {
        self.0.entry(replica).and_modify(|v| *v += 1).or_insert(1);
    }
}

impl Convergent for GCounter {
    type Delta = Self;

    fn merge(&mut self, other: Self) {
        for (k, v) in other.0.into_iter() {
            self.0
                .entry(k)
                .and_modify(|x| *x = (*x).max(v))
                .or_insert(v);
        }
    }

    fn merge_delta(&mut self, delta: Self::Delta) {
        self.merge(delta);
    }

    fn take_delta(&mut self) -> Option<Self::Delta> {
        Some(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REPLICA_1: ReplicaId = 123;
    const REPLICA_2: ReplicaId = 456;

    #[test]
    fn initial_value_is_zero() {
        let counter = GCounter::new();
        assert_eq!(counter.value(), 0);
    }

    #[test]
    fn increment() {
        let mut counter = GCounter::new();
        counter.inc(REPLICA_1);
        assert_eq!(counter.value(), 1);
    }

    #[test]
    fn merge_1() {
        let mut counter1 = GCounter::new();
        let mut counter2 = GCounter::new();

        // counter1 does one increment, counter2 does two increments
        counter1.inc(REPLICA_1);
        counter2.inc(REPLICA_2);
        counter2.inc(REPLICA_2);

        // Now, merge counter2 into counter1
        counter1.merge(counter2);

        // counter1 should now have the value 1 from node1 and 2 from node2
        assert_eq!(counter1.value(), 3);
    }

    #[test]
    fn merge_2() {
        let mut counter1 = GCounter::new();
        let mut counter2 = GCounter::new();

        // both counters increment the same replica
        counter1.inc(REPLICA_1);
        counter2.inc(REPLICA_1);

        // Merge counter2 into counter1
        counter1.merge(counter2);

        assert_eq!(counter1.value(), 1);
    }

    #[test]
    fn merge_3() {
        let mut counter1 = GCounter::new();
        let mut counter2 = GCounter::new();

        // both counters increment the same replica
        counter1.inc(REPLICA_1);
        counter2.inc(REPLICA_1);
        counter2.inc(REPLICA_1);

        // Merge counter2 into counter1
        counter1.merge(counter2);

        assert_eq!(counter1.value(), 2);
    }
}
