use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};

use super::{
    hash_table::concurrent::ConcurrentHashTable, HashJoinBenchmark, NoOutput, SchedulingType,
    STATIC_SCHEDULING,
};
use crate::tuple::{DataChunk, Tuple};

/// All threads build a single shared hash table and probe it concurrently.
pub struct SharedHashJoin<const S: SchedulingType> {
    inner: Option<Vec<DataChunk>>,
    outer: Option<Vec<DataChunk>>,
    bucket_num: usize,
}

impl<const S: SchedulingType> SharedHashJoin<S> {
    pub fn new(bucket_num: usize, inner: Vec<DataChunk>, outer: Vec<DataChunk>) -> Self {
        Self {
            inner: Some(inner),
            outer: Some(outer),
            bucket_num,
        }
    }
}

impl<const S: SchedulingType> HashJoinBenchmark for SharedHashJoin<S> {
    type PartitionOutput = NoOutput;
    type BuildOutput = ConcurrentHashTable<Vec<Tuple>>;

    fn partition(&mut self) -> Self::PartitionOutput {}

    fn build(&mut self, _: Self::PartitionOutput) -> Self::BuildOutput {
        let hash_table = ConcurrentHashTable::new(self.bucket_num).unwrap();
        for chunk in self.inner.take().unwrap() {
            if S == STATIC_SCHEDULING {
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
            } else {
                chunk
                    .into_par_iter()
                    .for_each(|tuple| hash_table.insert(tuple));
            }
        }
        hash_table
    }

    fn probe(&mut self, hash_table: Self::BuildOutput) {
        for chunk in self.outer.take().unwrap() {
            if S == STATIC_SCHEDULING {
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
            } else {
                chunk.par_iter().for_each(|tuple| {
                    hash_table
                        .get_matching_tuples(tuple.key())
                        .inspect(Self::produce_tuple);
                });
            }
        }
    }
}
