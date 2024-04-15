use crate::tuple::{DataChunk, Tuple};

use super::{hash_table::sequential::SequentialHashTable, HashJoinBenchmark};

/// Sequential hash join builds a single hash table and probe it sequentially.
/// It's meant to provide a baseline and reference implementation.
pub struct SequentialHashJoin {
    inner: Vec<DataChunk>,
    outer: Vec<DataChunk>,
    hash_table: SequentialHashTable<Vec<Tuple>>,
}

impl SequentialHashJoin {
    pub fn new(bucket_num: usize, inner: Vec<DataChunk>, outer: Vec<DataChunk>) -> Self {
        Self {
            inner,
            outer,
            hash_table: SequentialHashTable::new(bucket_num).unwrap(),
        }
    }
}

impl HashJoinBenchmark for SequentialHashJoin {
    fn partition(&self) {}

    fn build(&mut self) {
        for chunk in std::mem::take(&mut self.inner) {
            for tuple in chunk {
                self.hash_table.insert(tuple);
            }
        }
    }

    fn probe(&mut self) {
        for chunk in std::mem::take(&mut self.outer) {
            for tuple in chunk.iter() {
                self.hash_table
                    .get_matching_tuples(tuple.key())
                    .inspect(Self::produce_tuple);
            }
        }
    }
}
