use std::cmp::Ordering;
use std::collections::BTreeMap;

pub type ReplicaId = u64;

#[derive(Debug, Clone, Default, Eq, PartialEq, Hash)]
pub struct VClock(BTreeMap<ReplicaId, usize>);

impl VClock {
    pub fn new() -> Self {
        Default::default()
    }

    #[allow(dead_code)]
    pub fn get(&self, replica: &ReplicaId) -> usize {
        self.0.get(replica).map_or(0, |v| *v)
    }

    pub fn inc(&mut self, replica: ReplicaId) -> usize {
        let v = self.0.entry(replica).or_default();
        *v += 1;
        *v
    }

    pub fn merge(&mut self, other: &Self) {
        for (replica, v2) in &other.0 {
            self.0
                .entry(*replica)
                .and_modify(|v1| *v1 = (*v1).max(*v2))
                .or_insert(*v2);
        }
    }

    /// Checks if vector clock is greater or concurrent
    /// with the other vector clock
    pub fn gtc(&self, other: &Self) -> bool {
        self.partial_cmp(other)
            .is_none_or(|x| x == Ordering::Greater)
    }
}

impl PartialOrd for VClock {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    const REPLICA_1: ReplicaId = 123;
    const REPLICA_2: ReplicaId = 456;

    #[test]
    fn merge_1() {
        let mut clock1 = VClock::new();
        let mut clock2 = VClock::new();

        // clock1 does one increment, clock2 does two increments
        clock1.inc(REPLICA_1);
        clock2.inc(REPLICA_2);
        clock2.inc(REPLICA_2);

        // Now, merge clock2 into clock1
        clock1.merge(&clock2);

        // clock1 should now have the value 1 from node1 and 2 from node2
        assert_eq!(clock1.get(&REPLICA_1), 1);
        assert_eq!(clock1.get(&REPLICA_2), 2);
    }

    #[test]
    fn merge_2() {
        let mut clock1 = VClock::new();
        let mut clock2 = VClock::new();

        // both clocks increment the same replica
        clock1.inc(REPLICA_1);
        clock2.inc(REPLICA_1);

        // Merge clock2 into clock1
        clock1.merge(&clock2);

        assert_eq!(clock1.get(&REPLICA_1), 1);
    }

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
