#[macro_use]
mod prelude;

mod config;
mod memtable;
mod primitives;
mod sstable;
mod table;
mod time;
mod tombstones;

#[cfg(test)]
mod testutils;

use std::collections::HashMap;



fn main() {

    let arr = [1u8, 2u8];
    let r = &arr[0..];

    println!("{}", r[0]);
    println!("{}", r[1]);

    let asdf = std::panic::catch_unwind(|| println!("{}", r[2]));
    println!("yo");
    println!("{:?}", asdf);




    let mut m = HashMap::new();

    m.insert(1, "yo");
    println!("{:?}, {:?}", m.get(&1), m.get(&2));

    m.insert(2, "yeah");
    println!("{:?}, {:?}", m.get(&1), m.get(&2));
}
