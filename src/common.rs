use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::hash::Hash;

pub type ReplicaId = u64;

pub fn extend_with<K, V, F>(a: &mut HashMap<K, V>, b: HashMap<K, V>, merge: F)
where
    K: Eq + Hash,
    F: Fn(&mut V, V),
{
    for (k, v) in b.into_iter() {
        match a.entry(k) {
            Entry::Vacant(entry) => {
                entry.insert(v);
            }
            Entry::Occupied(mut entry) => {
                merge(entry.get_mut(), v);
            }
        }
    }
}
