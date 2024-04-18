mod hash_bucket;
mod hash_table;
pub mod sequential;
pub mod shared;

use crate::tuple::{DataChunk, Key, Tuple};

pub type NoOutput = ();

fn time_phase<T>(name: &str, f: impl FnOnce() -> T) -> T {
    let start = std::time::Instant::now();
    let ret = f();
    let elapsed = start.elapsed();
    println!("{}: {:?}", name, elapsed);
    ret
}

pub trait HashJoinBenchmark {
    type PartitionOutput;
    type BuildOutput;

    fn partition(&self, inner: Vec<DataChunk>) -> Self::PartitionOutput;
    fn build(&self, partition_output: Self::PartitionOutput) -> Self::BuildOutput;
    fn probe(&self, build_output: Self::BuildOutput, outer: Vec<DataChunk>);

    fn run(&self, inner: Vec<DataChunk>, outer: Vec<DataChunk>) {
        let partition_output = time_phase("partition", move || self.partition(inner));
        let build_output = time_phase("build", move || self.build(partition_output));
        time_phase("probe", || self.probe(build_output, outer));
    }

    /// Clone the tuple to simulate outputting it.
    /// See https://doc.rust-lang.org/std/hint/fn.black_box.html
    fn produce_tuple(_tuple: &Tuple) {
        use std::hint::black_box;
        fn contains(haystack: &[&str], needle: &str) -> bool {
            haystack.iter().any(|x| x == &needle)
        }

        fn benchmark() {
            let haystack = vec!["abc", "def", "ghi", "jkl", "mno"];
            let needle = "ghi";
            for _ in 0..1 {
                // Adjust our benchmark loop contents
                black_box(contains(black_box(&haystack), black_box(needle)));
            }
        }

        #[allow(clippy::unit_arg)]
        black_box(benchmark())
    }
}

/// Hash a key to bucket index in the hash table.
///
/// While its possible to use the key directly as the hash value, we still hash
/// the key for two reasons:
///
/// 1. The key may not always be u64 in real life.
/// 2. The overhead of hash function is significant.
pub fn bucket_hash(key: Key) -> u64 {
    xxhash_rust::xxh3::xxh3_64_with_seed(&key.to_le_bytes(), 821)
}

/// Hash a key to partition index.
pub fn partition_hash(key: Key) -> u64 {
    xxhash_rust::xxh3::xxh3_64_with_seed(&key.to_le_bytes(), 804)
}
