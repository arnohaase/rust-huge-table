use std::cmp::Ordering;

use crate::attic::primitives::PersistentKey;

#[derive(Eq, PartialEq)]
pub struct KeyWrapper <K>
    where K: PersistentKey
{
    pub key: K
}

impl <K> KeyWrapper<K> where K: PersistentKey
{}

impl <K> Ord for KeyWrapper<K> where K: PersistentKey {
    fn cmp(&self, other: &Self) -> Ordering {
        let self_bytes = self.key.as_raw();
        let other_bytes = other.key.as_raw();
        self_bytes.cmp(&other_bytes)
    }
}
impl <K> PartialOrd for KeyWrapper<K> where K: PersistentKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let self_bytes = self.key.as_raw();
        let other_bytes = other.key.as_raw();
        self_bytes.partial_cmp(&other_bytes)
    }
}
