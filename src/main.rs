// mod attic;
mod prelude;
mod primitives;
mod sstable;
mod table;


use std::collections::HashMap;

fn main() {

    let mut m = HashMap::new();

    m.insert(1, "yo");
    println!("{:?}, {:?}", m.get(&1), m.get(&2));

    m.insert(2, "yeah");
    println!("{:?}, {:?}", m.get(&1), m.get(&2));
}
