mod gcounter;
mod pncounter;
mod vclock;

use pncounter::PNCounter;
use vclock::VClock;
use gcounter::{Convergent, ReplicaId};

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
}
