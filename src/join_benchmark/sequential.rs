use crate::tuple::{DataChunk, Tuple};

use super::{hash_table::sequential::SequentialHashTable, HashJoinBenchmark};

/// Sequential hash join builds a single hash table and probe it sequentially.
/// It's meant to provide a baseline and reference implementation.
pub struct SequentialHashJoin {
    bucket_num: usize,
}

impl SequentialHashJoin {
    pub fn new(bucket_num: usize) -> Self {
        Self { bucket_num }
    }
}

impl HashJoinBenchmark for SequentialHashJoin {
    type PartitionOutput = Vec<DataChunk>;
    type BuildOutput = SequentialHashTable<Vec<Tuple>>;

    fn partition(&self, inner: Vec<DataChunk>) -> Self::PartitionOutput {
        inner
    }

    fn build(&self, inner: Self::PartitionOutput) -> Self::BuildOutput {
        let mut hash_table = SequentialHashTable::new(self.bucket_num).unwrap();
        for chunk in inner {
            for tuple in chunk {
                hash_table.insert(tuple);
            }
        }
        hash_table
    }

    fn probe(&self, hash_table: Self::BuildOutput, outer: Vec<DataChunk>) {
        for chunk in outer {
            for tuple in chunk.iter() {
                hash_table
                    .get_matching_tuples(tuple.key())
                    .inspect(Self::produce_tuple);
            }
        }
    }
}
