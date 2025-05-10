mod common;
mod state_crdt;
mod vclock;

use common::ReplicaId;
use state_crdt::{AWORMap, AWORSet, Convergent, ORSet, PNCounter};
use vclock::VClock;

const CLIENT_1: ReplicaId = 100;
const CLIENT_2: ReplicaId = 200;

fn main() {
    let mut c1 = PNCounter::new();
    let mut c2 = PNCounter::new();

    c1.inc(CLIENT_1);
    c1.inc(CLIENT_2);
    c1.inc(CLIENT_2);

    c2.inc(CLIENT_2);
    c2.dec(CLIENT_1);

    c1.merge(c2);

    println!("value = {}", c1.value());

    let mut c1 = VClock::new();
    let mut c2 = VClock::new();

    c1.inc(CLIENT_1);
    c1.inc(CLIENT_2);
    c1.inc(CLIENT_2);

    c2.inc(CLIENT_2);

    c1.merge(c2);

    println!("value = {:?}", c1);

    let mut s1 = ORSet::new(CLIENT_1);
    s1.add("foo".to_owned());
    s1.remove("foo".to_owned());
    println!("set has foo {}", s1.contains("foo"));

    let mut s2 = ORSet::new(CLIENT_2);
    s2.merge(s1);

    let mut s1 = AWORSet::new(CLIENT_1);
    s1.add("foo".to_owned());
    s1.remove("foo");
    println!("set contains foo {}", s1.contains("foo"));
    println!("keys = {:?}", s1.keys().collect::<Vec<_>>());

    let mut s2 = AWORSet::new(CLIENT_2);
    s2.merge(s1.clone());
    s2.add("banana".to_owned());
    s1.merge_delta(s2.take_delta().unwrap());

    println!("set contains banana {}", s1.contains("banana"));
    println!("keys = {:?}", s1.keys().collect::<Vec<_>>());

    let mut m1 = AWORMap::new(CLIENT_1);
    m1.insert("foo".to_owned(), PNCounter::new());
    m1.get_mut("foo").unwrap().inc(CLIENT_1);
    println!("m1.foo = {}", m1.get("foo").unwrap().value());
    m1.remove("foo");
    println!("m1.foo = {:?}", m1.get("foo"));
}
