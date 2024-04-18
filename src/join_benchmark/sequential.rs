use crate::tuple::{DataChunk, Tuple};

use super::{hash_table::sequential::SequentialHashTable, HashJoinBenchmark, NoOutput};

/// Sequential hash join builds a single hash table and probe it sequentially.
/// It's meant to provide a baseline and reference implementation.
pub struct SequentialHashJoin {
    inner: Option<Vec<DataChunk>>,
    outer: Option<Vec<DataChunk>>,
    bucket_num: usize,
}

impl SequentialHashJoin {
    pub fn new(bucket_num: usize, inner: Vec<DataChunk>, outer: Vec<DataChunk>) -> Self {
        Self {
            inner: Some(inner),
            outer: Some(outer),
            bucket_num,
        }
    }
}

impl HashJoinBenchmark for SequentialHashJoin {
    type PartitionOutput = NoOutput;
    type BuildOutput = SequentialHashTable<Vec<Tuple>>;

    fn partition(&mut self) -> Self::PartitionOutput {}

    fn build(&mut self, _: NoOutput) -> Self::BuildOutput {
        let mut hash_table = SequentialHashTable::new(self.bucket_num).unwrap();
        for chunk in self.inner.take().unwrap() {
            for tuple in chunk {
                hash_table.insert(tuple);
            }
        }
        hash_table
    }

    fn probe(&mut self, hash_table: Self::BuildOutput) {
        for chunk in self.outer.take().unwrap() {
            for tuple in chunk.iter() {
                hash_table
                    .get_matching_tuples(tuple.key())
                    .inspect(Self::produce_tuple);
            }
        }
    }
}
