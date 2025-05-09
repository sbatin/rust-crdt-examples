use std::cmp::Ordering;
use crate::gcounter::{GCounter, ReplicaId, Convergent};

#[derive(Debug, Clone)]
pub struct VClock(GCounter);

impl VClock {
    pub fn new() -> Self {
        Self(GCounter::new())
    }

    pub fn inc(&mut self, replica: ReplicaId) {
        self.0.inc(replica);
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
}

impl Convergent for VClock {
    fn merge(&mut self, other: Self) {
        self.0.merge(other.0);
    }
}

impl PartialEq for VClock {
    fn eq(&self, other: &Self) -> bool {
        self.compare(other) == Some(Ordering::Equal)
    }
}

impl Eq for VClock {}

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

        clock1.inc(REPLICA_1);
        clock2.inc(REPLICA_1);

        assert_eq!(clock1.partial_cmp(&clock2), Some(Ordering::Equal));
    }

    #[test]
    fn compare_2() {
        let mut clock1 = VClock::new();
        let mut clock2 = VClock::new();

        clock1.inc(REPLICA_1);
        clock1.inc(REPLICA_1);
        clock1.inc(REPLICA_2);

        clock2.inc(REPLICA_1);
        clock2.inc(REPLICA_2);

        assert_eq!(clock1.partial_cmp(&clock2), Some(Ordering::Greater));
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
    }

    #[test]
    fn compare_4() {
        let mut clock1 = VClock::new();
        let mut clock2 = VClock::new();

        clock1.inc(REPLICA_1);
        clock2.inc(REPLICA_2);

        assert_eq!(clock1.partial_cmp(&clock2), None);
    }
}
