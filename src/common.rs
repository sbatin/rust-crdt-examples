use std::collections::btree_map::Entry as BTreeMapEntry;
use std::collections::hash_map::Entry as HashMapEntry;
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

pub type ReplicaId = u64;

pub trait ExtendWith<V> {
    fn extend_with<F: Fn(&mut V, V)>(a: &mut Self, b: Self, merge: F);
}

impl<K: Eq + Hash, V> ExtendWith<V> for HashMap<K, V> {
    fn extend_with<F: Fn(&mut V, V)>(a: &mut Self, b: Self, merge: F) {
        for (k, v) in b.into_iter() {
            match a.entry(k) {
                HashMapEntry::Vacant(entry) => {
                    entry.insert(v);
                }
                HashMapEntry::Occupied(mut entry) => {
                    merge(entry.get_mut(), v);
                }
            }
        }
    }
}

impl<K: Ord, V> ExtendWith<V> for BTreeMap<K, V> {
    fn extend_with<F: Fn(&mut V, V)>(a: &mut Self, b: Self, merge: F) {
        for (k, v) in b.into_iter() {
            match a.entry(k) {
                BTreeMapEntry::Vacant(entry) => {
                    entry.insert(v);
                }
                BTreeMapEntry::Occupied(mut entry) => {
                    merge(entry.get_mut(), v);
                }
            }
        }
    }
}
