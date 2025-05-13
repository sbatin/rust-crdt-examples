mod types;

pub use crate::vclock::ReplicaId;
pub use types::*;

pub trait Convergent {
    type Delta;

    fn merge(&mut self, other: Self);

    fn merge_delta(&mut self, delta: Self::Delta);

    fn take_delta(&mut self) -> Option<Self::Delta>;
}
