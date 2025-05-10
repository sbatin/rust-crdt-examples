use crate::aworset::{AWORSet, DotKernel};
use crate::gcounter::{Convergent, ReplicaId};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

#[derive(Debug, Clone)]
pub struct AWORMap<K, V> {
    keys: AWORSet<K>,
    vals: HashMap<K, V>,
}

#[derive(Debug, Clone)]
pub struct AWORMapDelta<K, V> {
    keys: Option<DotKernel<K>>,
    vals: HashMap<K, V>,
}

impl<K, V> Default for AWORMapDelta<K, V> {
    fn default() -> Self {
        Self {
            keys: None,
            vals: HashMap::new(),
        }
    }
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

impl<K: Eq + Hash + Clone, V: Convergent + Default> Convergent for AWORMap<K, V> {
    type Delta = AWORMapDelta<K, V::Delta>;

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

    fn merge_delta(&mut self, delta: Self::Delta) {
        let mut delta = delta;

        if let Some(delta_keys) = delta.keys {
            self.keys.merge_delta(delta_keys);
        }

        let mut self_vals = std::mem::take(&mut self.vals);

        for key in self.keys.keys() {
            let v = self_vals.remove(key);
            let d = delta.vals.remove(key);
            match (v, d) {
                (Some(mut v), Some(d)) => {
                    v.merge_delta(d);
                    self.vals.insert(key.clone(), v);
                }
                (Some(v), None) => {
                    self.vals.insert(key.clone(), v);
                }
                (None, Some(d)) => {
                    // this is not quite right as some types
                    // needs ReplicaId in order to be constructed
                    let mut v = V::default();
                    v.merge_delta(d);
                    self.vals.insert(key.clone(), v);
                }
                _ => {}
            }
        }
    }

    fn take_delta(&mut self) -> Option<Self::Delta> {
        let keys = self.keys.take_delta();
        let mut vals = HashMap::new();

        for (k, v) in &mut self.vals {
            if let Some(d) = v.take_delta() {
                vals.insert(k.clone(), d);
            }
        }

        if keys.is_none() && vals.is_empty() {
            None
        } else {
            Some(AWORMapDelta { keys, vals })
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

    #[test]
    fn basic_delta_sync() {
        let mut m1 = AWORMap::new(REPLICA_1);
        m1.insert("foo".to_owned(), GCounter::new());
        m1.get_mut("foo").unwrap().inc(REPLICA_1);
        assert_eq!(m1.get("foo").unwrap().value(), 1);

        let mut m2 = AWORMap::new(REPLICA_2);
        m2.insert("bar".to_owned(), GCounter::new());
        m2.get_mut("bar").unwrap().inc(REPLICA_2);
        assert_eq!(m2.get("bar").unwrap().value(), 1);

        m1.merge_delta(m2.take_delta().unwrap());
        assert_eq!(m1.get("foo").unwrap().value(), 1);
        assert_eq!(m1.get("bar").unwrap().value(), 1);
    }
}
