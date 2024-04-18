use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};

use super::{hash_table::concurrent::ConcurrentHashTable, HashJoinBenchmark};
use crate::tuple::{DataChunk, Tuple};

/// All threads build a single shared hash table and probe it concurrently.
/// Use dynamic scheduling.
pub struct SharedDynamicHashJoin {
    bucket_num: usize,
}

impl SharedDynamicHashJoin {
    pub fn new(bucket_num: usize) -> Self {
        Self { bucket_num }
    }
}

impl HashJoinBenchmark for SharedDynamicHashJoin {
    type PartitionOutput = Vec<DataChunk>;
    type BuildOutput = ConcurrentHashTable<Vec<Tuple>>;

    fn partition(&self, inner: Vec<DataChunk>) -> Self::PartitionOutput {
        inner
    }

    fn build(&self, inner: Self::PartitionOutput) -> Self::BuildOutput {
        let hash_table = ConcurrentHashTable::new(self.bucket_num).unwrap();
        for chunk in inner {
            chunk
                .into_par_iter()
                .for_each(|tuple| hash_table.insert(tuple));
        }
        hash_table
    }

    fn probe(&self, hash_table: Self::BuildOutput, outer: Vec<DataChunk>) {
        for chunk in outer {
            chunk.par_iter().for_each(|tuple| {
                hash_table
                    .get_matching_tuples(tuple.key())
                    .inspect(Self::produce_tuple);
            });
        }
    }
}

/// All threads build a single shared hash table and probe it concurrently.
/// Use static scheduling.
pub struct SharedStaticHashJoin {
    bucket_num: usize,
}

impl SharedStaticHashJoin {
    pub fn new(bucket_num: usize) -> Self {
        Self { bucket_num }
    }
}

impl HashJoinBenchmark for SharedStaticHashJoin {
    type PartitionOutput = Vec<DataChunk>;
    type BuildOutput = ConcurrentHashTable<Vec<Tuple>>;

    fn partition(&self, inner: Vec<DataChunk>) -> Self::PartitionOutput {
        inner
    }

    fn build(&self, inner: Vec<DataChunk>) -> Self::BuildOutput {
        let hash_table = ConcurrentHashTable::new(self.bucket_num).unwrap();
        for chunk in inner {
            // Divide the chunks into equal parts for each thread.
            let num_threads = rayon::current_num_threads();
            let thread_chunk_size = chunk.len() / num_threads;
            let thread_chunks = chunk.chunks(thread_chunk_size).collect::<Vec<_>>();
            rayon::scope(|s| {
                for chunk in thread_chunks {
                    s.spawn(|_| {
                        // Cloning the tuple is the same costly as passing the reference.
                        chunk
                            .iter()
                            .for_each(|tuple| hash_table.insert(tuple.clone()));
                    });
                }
            });
        }
        hash_table
    }

    fn probe(&self, hash_table: Self::BuildOutput, outer: Vec<DataChunk>) {
        for chunk in outer {
            // Divide the chunks into equal parts for each thread.
            let num_threads = rayon::current_num_threads();
            let thread_chunk_size = chunk.len() / num_threads;
            let thread_chunks = chunk.chunks(thread_chunk_size).collect::<Vec<_>>();
            rayon::scope(|s| {
                for chunk in thread_chunks {
                    s.spawn(|_| {
                        chunk.iter().for_each(|tuple| {
                            hash_table
                                .get_matching_tuples(tuple.key())
                                .inspect(Self::produce_tuple);
                        });
                    });
                }
            });
        }
    }
}
