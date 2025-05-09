use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

use crate::gcounter::{ReplicaId, Convergent};
use crate::vclock::VClock;

#[derive(Debug, Clone, PartialEq)]
pub struct ORSet<K: Eq + Hash> {
    replica_id: ReplicaId,
    add: HashMap<K, VClock>,
    rem: HashMap<K, VClock>,
}

fn merge_keys<K: Eq + Hash>(a: &mut HashMap<K, VClock>, b: HashMap<K, VClock>) {
    for (k, vb) in b {
        if let Some(va) = a.get_mut(&k) {
            va.merge(vb);
        } else {
            a.insert(k, vb);
        }
    }
}

impl<K: Eq + Hash> ORSet<K> {
    pub fn new(replica_id: ReplicaId) -> Self {
        Self {
            replica_id,
            add: HashMap::new(),
            rem: HashMap::new(),
        }
    }

    fn pair<Q>(&self, key: &Q) -> (Option<&VClock>, Option<&VClock>)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        (self.add.get(key), self.rem.get(key))
    }

    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self.pair(key) {
            (Some(va), Some(vr)) if va < vr => false,
            (Some(_), _) => true,
            _ => false,
        }
    }

    pub fn add(&mut self, key: K) {
        let mut v = match self.pair(&key) {
            (Some(v), _) => {
                let v = v.clone();
                self.rem.remove(&key);
                v
            }
            (_, Some(v)) => {
                let v = v.clone();
                self.rem.remove(&key);
                v
            }
            _ => VClock::new()
        };
        v.inc(self.replica_id);
        self.add.insert(key, v);
    }

    pub fn remove(&mut self, key: K) {
        let mut v = match self.pair(&key) {
            (Some(v), _) => {
                let v = v.clone();
                self.add.remove(&key);
                v
            }
            (_, Some(v)) => {
                let v = v.clone();
                self.add.remove(&key);
                v
            }
            _ => VClock::new()
        };
        v.inc(self.replica_id);
        self.rem.insert(key, v);
    }
}

impl<K: Eq + Hash> Convergent for ORSet<K> {
    fn merge(&mut self, other: Self) {
        merge_keys(&mut self.add, other.add);
        merge_keys(&mut self.rem, other.rem);

        for (k, vr) in &self.rem {
            match self.add.get(k) {
                Some(va) if va < vr => self.add.remove(k),
                _ => None,
            };
        }

        for (k, va) in &self.add {
            match self.rem.get(k) {
                Some(vr) if va < vr => None,
                _ => self.rem.remove(k),
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REPLICA_1: ReplicaId = 123;
    const REPLICA_2: ReplicaId = 456;
    const REPLICA_3: ReplicaId = 789;

    #[test]
    fn simple() {
        let mut s1 = ORSet::new(REPLICA_1);

        assert!(!s1.contains("foo"));
        s1.add("foo");
        assert!(s1.contains("foo"));
    }

    #[test]
    fn remove_after_add() {
        let mut s1 = ORSet::new(REPLICA_1);

        s1.add("foo");
        assert!(s1.contains("foo"));

        s1.remove("foo");
        assert!(!s1.contains("foo"));
    }

    #[test]
    fn remove_nonexistent() {
        let mut s1 = ORSet::new(REPLICA_1);

        s1.remove("foo");
        assert!(!s1.contains("foo"));
    }

    #[test]
    fn merge_adds_converge() {
        let mut s1 = ORSet::new(REPLICA_1);
        let mut s2 = ORSet::new(REPLICA_2);

        s1.add("foo");
        s2.add("bar");

        s1.merge(s2.clone());
        s2.merge(s1.clone());

        assert!(s1.contains("foo"));
        assert!(s1.contains("bar"));
        assert!(s2.contains("foo"));
        assert!(s2.contains("bar"));
    }

    #[test]
    fn merge_with_removal() {
        let mut s1 = ORSet::new(REPLICA_1);
        let mut s2 = ORSet::new(REPLICA_2);

        s1.add("foo");
        s2.add("foo");

        // remove from one replica
        s1.remove("foo");

        s1.merge(s2.clone());

        // should still exist because set2 added it independently
        assert!(s1.contains("foo"));

        s2.merge(s1.clone());
        assert!(s2.contains("foo"));

        // now remove from set2 as well
        s2.remove("foo");
        s1.merge(s2.clone());

        assert!(!s1.contains("foo"));
        assert!(!s2.contains("foo"));
    }

    #[test]
    fn idempotent_merge() {
        let mut set1 = ORSet::new(REPLICA_1);
        let mut set2 = ORSet::new(REPLICA_2);

        set1.add("grape");
        set2.merge(set1.clone());

        let snapshot = set2.clone();
        set2.merge(set1.clone()); // merge again should not change anything
        assert_eq!(set2, snapshot);
    }

    #[test]
    fn commutativity_and_associativity_of_merge() {
        let mut a = ORSet::new(REPLICA_1);
        let mut b = ORSet::new(REPLICA_2);
        let mut c = ORSet::new(REPLICA_3);

        a.add("kiwi");
        b.add("lemon");
        c.add("mango");

        // (a merge b) merge c
        let mut ab = a.clone();
        ab.merge(b.clone());
        ab.merge(c.clone());

        // a merge (b merge c)
        let mut bc = b.clone();
        bc.merge(c.clone());
        let mut abc = a.clone();
        abc.merge(bc.clone());

        assert_eq!(ab, abc);
    }
}
