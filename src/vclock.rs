use crate::common::ExtendWith;
use crate::common::ReplicaId;
use std::cmp::Ordering;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default, Eq, PartialEq, Hash)]
pub struct VClock(BTreeMap<ReplicaId, usize>);

impl VClock {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn inc(&mut self, replica: ReplicaId) -> usize {
        let v = self.0.entry(replica).or_default();
        *v += 1;
        *v
    }

    pub fn compare(&self, other: &Self) -> Option<Ordering> {
        self.0
            .keys()
            .chain(other.0.keys())
            .try_fold(Ordering::Equal, |prev, k| {
                let va = self.0.get(k);
                let vb = other.0.get(k);
                match prev {
                    Ordering::Equal if va > vb => Some(Ordering::Greater),
                    Ordering::Equal if va < vb => Some(Ordering::Less),
                    Ordering::Less if va > vb => None,
                    Ordering::Greater if va < vb => None,
                    _ => Some(prev),
                }
            })
    }

    pub fn merge(&mut self, other: Self) {
        ExtendWith::extend_with(&mut self.0, other.0, |a, b| *a = (*a).max(b));
    }
}

impl PartialOrd for VClock {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.compare(other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REPLICA_1: ReplicaId = 123;
    const REPLICA_2: ReplicaId = 456;

    #[test]
    fn compare_1() {
        let mut clock1 = VClock::new();
        let mut clock2 = VClock::new();

        assert_eq!(clock1.partial_cmp(&clock2), Some(Ordering::Equal));
        assert_eq!(clock1, clock2);

        assert_eq!(clock1.inc(REPLICA_1), 1);

        assert_eq!(clock1.partial_cmp(&clock2), Some(Ordering::Greater));
        assert_ne!(clock1, clock2);

        assert_eq!(clock2.inc(REPLICA_1), 1);

        assert_eq!(clock1.partial_cmp(&clock2), Some(Ordering::Equal));
        assert_eq!(clock1, clock2);
    }

    #[test]
    fn compare_2() {
        let mut clock1 = VClock::new();
        let mut clock2 = VClock::new();

        assert_eq!(clock1.inc(REPLICA_1), 1);
        assert_eq!(clock1.inc(REPLICA_1), 2);
        assert_eq!(clock1.inc(REPLICA_2), 1);

        clock2.inc(REPLICA_1);
        clock2.inc(REPLICA_2);

        assert_eq!(clock1.partial_cmp(&clock2), Some(Ordering::Greater));
        assert_ne!(clock1, clock2);
    }

    #[test]
    fn compare_3() {
        let mut clock1 = VClock::new();
        let mut clock2 = VClock::new();

        clock1.inc(REPLICA_1);
        clock1.inc(REPLICA_2);

        clock2.inc(REPLICA_1);
        clock2.inc(REPLICA_1);
        clock2.inc(REPLICA_2);

        assert_eq!(clock1.partial_cmp(&clock2), Some(Ordering::Less));
        assert_ne!(clock1, clock2);
    }

    #[test]
    fn compare_4() {
        let mut clock1 = VClock::new();
        let mut clock2 = VClock::new();

        clock1.inc(REPLICA_1);
        clock2.inc(REPLICA_2);

        assert_eq!(clock1.partial_cmp(&clock2), None);
        assert_ne!(clock1, clock2);
    }
}
