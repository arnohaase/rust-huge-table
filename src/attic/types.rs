use std::mem::size_of;
use std::convert::TryInto;

use crate::attic::primitives::*;

impl PersistentKey for u64 {
    fn raw_size() -> usize {
        size_of::<u64>()
    }
    fn as_raw(&self) -> Vec<u8> {
        Vec::from(self.to_be_bytes().as_ref())
    }
}
impl PersistentValue for u64 {
    fn from_raw(raw: &[u8]) -> Self {
        let mut offs = 0usize;
        raw.decode_varint_u64(&mut offs)
    }
    fn as_raw(&self) -> Vec<u8> {
        let mut vec = Vec::new();
        vec.encode_varint_u64(*self);
        vec
    }
}