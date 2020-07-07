use std::collections::BTreeMap;

pub struct HugeMap<K,V>
  where K: Eq,
        V: Copy,
        // V: PersistentValue,
{
    memtable: BTreeMap<K,V>,
}

impl <K,V> HugeMap<K,V>
    where K: Ord + Sized,
          V: Copy,
          // V: PersistentValue,
{
    // fn hash(k: &K) -> u64 {
    //     let mut h = new_hasher();
    //     k.hash(&mut h);
    //     h.finish()
    // }

    pub fn put(&mut self, key: K, value: V) {
        self.memtable.insert(key, value);
        // println!("{:?} -> {:?}", &key, &value)
    }

    pub fn get(&self, key: &K) -> Option<V> {
        match self.memtable.get(key) {
            Some(v) => Some(*v),
            None => None
        }
    }
}

// fn new_hasher() -> MetroHasher {
//     Default::default()
// }

