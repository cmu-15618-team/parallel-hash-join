mod hash_bucket;
mod hash_table;
pub mod sequential;

use crate::tuple::{Key, Tuple};

fn time_phase(name: &str, mut f: impl FnMut()) {
    let start = std::time::Instant::now();
    f();
    let elapsed = start.elapsed();
    println!("{}: {:?}", name, elapsed);
}

pub trait HashJoinBenchmark {
    fn partition(&self);
    fn build(&mut self);
    fn probe(&mut self);

    fn run(&mut self) {
        time_phase("partition", || self.partition());
        time_phase("build", || self.build());
        time_phase("probe", || self.probe());
    }

    /// Clone the tuple to simulate outputting it.
    /// See https://doc.rust-lang.org/std/hint/fn.black_box.html
    fn produce_tuple(tuple: &Tuple) {
        std::hint::black_box(tuple.clone());
    }
}

/// Hash a key to bucket index in the hash table.
pub fn bucket_hash(key: Key) -> u64 {
    xxhash_rust::xxh3::xxh3_64_with_seed(&key.to_le_bytes(), 821)
}

pub fn partition_hash(key: Key) -> u64 {
    xxhash_rust::xxh3::xxh3_64_with_seed(&key.to_le_bytes(), 804)
}
