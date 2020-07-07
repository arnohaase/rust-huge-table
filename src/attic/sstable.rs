use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::mem::size_of;
use std::path::PathBuf;

use memmap::{Mmap, MmapOptions};

use crate::attic::primitives::*;
use crate::attic::memtable::KeyWrapper;

pub struct SsTableConfig {
    pub base_path: PathBuf,
}

struct SsTable <K,V>
{
    index_file: File,
    index_mmap: Mmap,
    data_file: File,
    data_mmap: Mmap,

    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

impl <K,V> SsTable <K,V>
  where K: PersistentKey,
        V: PersistentValue,
{
    pub fn from_memtable(config: &SsTableConfig, memtable: &mut BTreeMap<KeyWrapper<K>,V>) -> std::io::Result<SsTable<K,V>> {
        let name_base = uuid::Uuid::new_v4().to_string();

        let mut index_file = new_file(config, &name_base, "index", true)?;
        let mut data_file = new_file(config, &name_base, "data", true)?;

        for (k,v) in memtable.iter() {
            index_file.write_all(k.key.as_raw().as_slice())?;

            //TODO extract 'write_u64'
            let pos_be = u64::to_be(data_file.seek(SeekFrom::Current(0))?);
            //TODO check that this fits into a usize
            let ptr = &pos_be as *const u64 as *const u8;
            index_file.write_all(unsafe { std::slice::from_raw_parts(ptr, size_of::<u64>()) })?;

            let mut v_size_buf = Vec::new();
            let v_data_buf = v.as_raw();
            v_size_buf.encode_varint_usize(v_data_buf.len());
            data_file.write_all(&v_size_buf);
            data_file.write_all(&v_data_buf);
        }

        memtable.clear();

        let mut index_file = new_file(config, &name_base, "index", false)?;
        let mut data_file = new_file(config, &name_base, "data", false)?;
        let index_mmap = unsafe { MmapOptions::new().map(&index_file) }?;
        let data_mmap = unsafe { MmapOptions::new().map(&data_file) }?;

        Ok(SsTable { index_file, index_mmap, data_file, data_mmap, _k: PhantomData, _v: PhantomData })
    }

    pub fn from_name_base(config: &SsTableConfig, name_base: &str) -> std::io::Result<SsTable<K,V>> {
        let index_file = new_file(config, &name_base, "index", false)?;
        let data_file = new_file(config, &name_base, "data", false)?;

        let index_mmap = unsafe { MmapOptions::new().map(&index_file) }?;
        let data_mmap = unsafe { MmapOptions::new().map(&data_file) }?;

        Ok(SsTable { index_file, index_mmap, data_file, data_mmap, _k: PhantomData, _v: PhantomData })
    }

    pub fn get(&self, key: &K) -> Option<V> { //TODO can we avoid copying v?
        // adapted from slice::binary_search_by

        let item_size = K::raw_size() + size_of::<u64>();
        let index_slice = self.index_mmap.as_ref();

        let mut size = self.index_mmap.len() / item_size;
        let mut base = 0usize;

        let key_bytes_vec = key.as_raw();
        let key_bytes = key_bytes_vec.as_slice();

        while size > 1 {
            let half = size / 2;
            let mid = base + half;
            // mid is always in [0, size), that means mid is >= 0 and < size.
            // mid >= 0: by definition
            // mid < size: mid = size / 2 + size / 4 + size / 8 ...

            let to_be_compared = &index_slice [mid*item_size .. mid*item_size+K::raw_size()];
            match key_bytes.cmp(to_be_compared) {
                Ordering::Less => {},
                Ordering::Greater => {
                    base = mid;
                }
                Ordering::Equal => {
                    let offs_value = read_u64(index_slice, mid*item_size+K::raw_size());
                    return Some(self.read_value(&mut usize::try_from(offs_value).unwrap()));
                }
            }

            size -= half;
        }
        // base is always in [0, size) because base <= mid.
        let to_be_compared = &index_slice [base*item_size .. base*item_size+K::raw_size()];
        match key_bytes.cmp(to_be_compared) {
            Ordering::Equal => {
                let offs_value = read_u64(index_slice, base*item_size+K::raw_size());
                Some(self.read_value(&mut usize::try_from(offs_value).unwrap()))
            },
            _ => None,
        }
    }

    fn read_value(&self, offs_value: &mut usize) -> V {
        let data_slice = self.data_mmap.as_ref();

        let v_len = data_slice.decode_varint_usize(offs_value);

        let data_slice = &data_slice[*offs_value..];
        let (v_data_slice, _) = data_slice.split_at(v_len);
        V::from_raw(v_data_slice)
    }
}



fn read_u32(buf: &[u8], offs: usize) -> u32 {
    let data_slice = &buf[offs..];
    let (data_slice,_) = data_slice.split_at(size_of::<u32>());
    u32::from_be_bytes(data_slice.try_into().unwrap())
}

fn read_u64(buf: &[u8], offs: usize) -> u64 {
    let data_slice = &buf[offs..];
    let (data_slice,_) = data_slice.split_at(size_of::<u64>());
    u64::from_be_bytes(data_slice.try_into().unwrap())
}

fn new_file(config: &SsTableConfig, name_base: &str, extension: &str, writeable: bool) -> std::io::Result<File> {
    let mut path = config.base_path.clone();
    path.push(format!("{}.{}", name_base, extension));

    OpenOptions::new()
        .create(writeable)
        .write(writeable)
        .read(true)
        .open(&path)
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;
    use std::collections::BTreeMap;

    use crate::attic::sstable::{SsTableConfig, SsTable};
    use crate::attic::memtable::KeyWrapper;
    use crate::attic::types::*;

    const test_dir: &str = "__test__";

    #[test]
    pub fn test_simple() -> std::io::Result<()> {
        let base_path = PathBuf::from(test_dir);
        std::fs::create_dir(&base_path);
        let config = SsTableConfig { base_path: base_path };

        let mut memtable = BTreeMap::new();
        memtable.insert(KeyWrapper { key: 1u64 }, 101u64);

        let sstable = SsTable::from_memtable(&config, &mut memtable)?;

        assert_eq!(sstable.get(&0u64), None);
        assert_eq!(sstable.get(&1u64), Some(101u64));
        assert_eq!(sstable.get(&2u64), None);

        Ok(())
    }
}
