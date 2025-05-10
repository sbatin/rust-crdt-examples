use crate::gcounter::{Convergent, ReplicaId};
use std::borrow::Borrow;
use std::collections::{BTreeSet, HashMap};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
struct Dot(ReplicaId, usize);

type VectorClock = HashMap<ReplicaId, usize>;

#[derive(Debug, Clone, Eq, PartialEq)]
struct DotContext {
    clock: VectorClock,
    dots: BTreeSet<Dot>,
}

impl DotContext {
    pub fn new() -> Self {
        Self {
            clock: HashMap::new(),
            dots: BTreeSet::new(),
        }
    }

    pub fn contains(&self, dot: &Dot) -> bool {
        match self.clock.get(&dot.0) {
            Some(n) if n >= &dot.1 => true,
            _ => self.dots.contains(dot),
        }
    }

    pub fn next_dot(&mut self, replica: ReplicaId) -> Dot {
        let v = self
            .clock
            .entry(replica)
            .and_modify(|n| *n += 1)
            .or_insert(1);
        Dot(replica, *v)
    }

    pub fn add(&mut self, dot: Dot) {
        self.dots.insert(dot);
    }

    pub fn merge(&mut self, other: Self) {
        for (k, v) in other.clock.into_iter() {
            self.clock
                .entry(k)
                .and_modify(|x| *x = (*x).max(v))
                .or_insert(v);
        }
        self.dots.extend(other.dots);
        self.compact();
    }

    fn compact(&mut self) {
        let mut dots_to_remove = BTreeSet::new();

        for dot in &self.dots {
            let n = self.clock.get(&dot.0).map_or(0, |v| *v);
            if dot.1 == n + 1 {
                self.clock.insert(dot.0, dot.1 + 1);
                dots_to_remove.insert(dot.clone());
            } else if dot.1 <= n {
                dots_to_remove.insert(dot.clone());
            }
        }

        self.dots = &self.dots - &dots_to_remove;
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DotKernel<K> {
    context: DotContext,
    entries: HashMap<Dot, K>,
}

impl<K> Default for DotKernel<K> {
    fn default() -> Self {
        Self {
            context: DotContext::new(),
            entries: HashMap::new(),
        }
    }
}

impl<K: Clone> DotKernel<K> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        self.entries.values().any(|k| k.borrow() == key)
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.entries.values()
    }

    pub fn add(&mut self, replica: ReplicaId, key: K, delta: &mut Self) {
        let dot = self.context.next_dot(replica);
        self.entries.insert(dot.clone(), key.clone());

        delta.entries.insert(dot.clone(), key);
        delta.context.add(dot);
        delta.context.compact();
    }

    pub fn remove<Q>(&mut self, key: &Q, delta: &mut Self)
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        let mut dots_to_remove = Vec::new();

        for (dot, k) in &self.entries {
            if k.borrow() == key {
                dots_to_remove.push(dot.clone());
            }
        }

        for dot in dots_to_remove {
            self.entries.remove(&dot);
            delta.context.add(dot);
        }

        delta.context.compact();
    }

    pub fn merge(&mut self, other: Self) {
        let mut dots_to_remove = Vec::new();

        for dot in self.entries.keys() {
            // remove elements visible in dot context but not among entries
            if other.context.contains(dot) && !other.entries.contains_key(dot) {
                dots_to_remove.push(dot.clone());
            }
        }

        for (dot, k) in other.entries {
            // add unseen elements
            if !self.entries.contains_key(&dot) && !self.context.contains(&dot) {
                self.entries.insert(dot, k);
            }
        }

        for dot in &dots_to_remove {
            self.entries.remove(dot);
        }

        self.context.merge(other.context);
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AWORSet<K> {
    replica_id: ReplicaId,
    state: DotKernel<K>,
    delta: DotKernel<K>,
}

impl<K: Eq + Clone> AWORSet<K> {
    pub fn new(replica_id: ReplicaId) -> Self {
        Self {
            replica_id,
            state: DotKernel::new(),
            delta: DotKernel::new(),
        }
    }

    pub fn contains<Q>(&self, value: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        self.state.contains(value)
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.state.keys()
    }

    pub fn add(&mut self, value: K) {
        self.state.remove(&value, &mut self.delta);
        self.state.add(self.replica_id, value, &mut self.delta);
    }

    pub fn remove<Q>(&mut self, value: &Q)
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        self.state.remove(value, &mut self.delta);
    }

    pub fn merge_delta(&mut self, delta: DotKernel<K>) {
        self.delta.merge(delta);
        self.state.merge(self.delta.clone());
    }

    pub fn split_delta(&mut self) -> DotKernel<K> {
        std::mem::replace(&mut self.delta, DotKernel::new())
    }
}

impl<K> Convergent for AWORSet<K> {
    fn merge(&mut self, other: Self) {
        self.delta.merge(other.delta);
        self.state.merge(other.state);
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
        let mut s1 = AWORSet::new(REPLICA_1);

        assert!(!s1.contains("foo"));
        s1.add("foo");
        assert!(s1.contains("foo"));
    }

    #[test]
    fn remove_after_add() {
        let mut s1 = AWORSet::new(REPLICA_1);

        s1.add("foo");
        assert!(s1.contains("foo"));

        s1.remove("foo");
        assert!(!s1.contains("foo"));
    }

    #[test]
    fn merge_adds_converge() {
        let mut s1 = AWORSet::new(REPLICA_1);
        let mut s2 = AWORSet::new(REPLICA_2);

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
        let mut s1 = AWORSet::new(REPLICA_1);
        let mut s2 = AWORSet::new(REPLICA_2);

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
        let mut set1 = AWORSet::new(REPLICA_1);
        let mut set2 = AWORSet::new(REPLICA_2);

        set1.add("grape");
        set2.merge(set1.clone());

        let snapshot = set2.clone();
        set2.merge(set1.clone()); // merge again should not change anything

        assert_eq!(set2, snapshot);
    }

    #[test]
    fn commutativity_and_associativity_of_merge() {
        let mut a = AWORSet::new(REPLICA_1);
        let mut b = AWORSet::new(REPLICA_2);
        let mut c = AWORSet::new(REPLICA_3);

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
