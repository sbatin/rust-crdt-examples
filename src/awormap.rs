use crate::aworset::AWORSet;
use crate::gcounter::{Convergent, ReplicaId};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

#[derive(Debug, Clone)]
pub struct AWORMap<K, V> {
    keys: AWORSet<K>,
    vals: HashMap<K, V>,
}

impl<K: Eq + Hash + Clone, V> AWORMap<K, V> {
    pub fn new(replica_id: ReplicaId) -> Self {
        Self {
            keys: AWORSet::new(replica_id),
            vals: HashMap::new(),
        }
    }

    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.vals.get(key)
    }

    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.vals.get_mut(key)
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.keys.add(key.clone());
        self.vals.insert(key, value);
    }

    pub fn remove<Q>(&mut self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.keys.remove(key);
        self.vals.remove(key);
    }
}

impl<K: Eq + Hash + Clone, V: Convergent> Convergent for AWORMap<K, V> {
    fn merge(&mut self, other: Self) {
        let mut other = other;
        self.keys.merge(other.keys);

        let mut self_vals = std::mem::take(&mut self.vals);

        for key in self.keys.keys() {
            let v1 = self_vals.remove(key);
            let v2 = other.vals.remove(key);
            match (v1, v2) {
                (Some(mut v1), Some(v2)) => {
                    v1.merge(v2);
                    self.vals.insert(key.clone(), v1);
                }
                (Some(v1), None) => {
                    self.vals.insert(key.clone(), v1);
                }
                (None, Some(v2)) => {
                    self.vals.insert(key.clone(), v2);
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gcounter::GCounter;

    const REPLICA_1: ReplicaId = 123;
    const REPLICA_2: ReplicaId = 456;

    #[test]
    fn basic_sync() {
        let mut m1 = AWORMap::new(REPLICA_1);
        m1.insert("foo".to_owned(), GCounter::new());
        m1.get_mut("foo").unwrap().inc(REPLICA_1);
        assert_eq!(m1.get("foo").unwrap().value(), 1);

        let mut m2 = AWORMap::new(REPLICA_2);
        m2.insert("foo".to_owned(), GCounter::new());
        m2.get_mut("foo").unwrap().inc(REPLICA_2);
        assert_eq!(m2.get("foo").unwrap().value(), 1);

        m2.merge(m1.clone());
        assert_eq!(m2.get("foo").unwrap().value(), 2);
    }

    #[test]
    fn can_remove_value() {
        let mut m1 = AWORMap::new(REPLICA_1);
        m1.insert("foo".to_owned(), GCounter::new());
        m1.remove("foo");
        assert!(m1.get("foo").is_none());
    }

    #[test]
    fn merge_after_removal() {
        let mut m1 = AWORMap::new(REPLICA_1);
        m1.insert("foo".to_owned(), GCounter::new());
        m1.remove("foo");

        let mut m2 = AWORMap::new(REPLICA_2);
        m2.insert("foo".to_owned(), GCounter::new());
        m2.get_mut("foo").unwrap().inc(REPLICA_2);

        m2.merge(m1.clone());
        assert_eq!(m2.get("foo").unwrap().value(), 1);
    }
}
