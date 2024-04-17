use std::sync::Arc;
use std::time::Instant;
use parking_lot::Mutex;
use rand::distributions::uniform::SampleBorrow;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};

use super::{hash_table::concurrent::ConcurrentHashTable, HashJoinBenchmark};
use crate::tuple::{DataChunk, Tuple};

/// All threads build a single shared hash table and probe it concurrently.
/// Use dynamic scheduling.
pub struct SharedDynamicHashJoin {
    inner: Vec<DataChunk>,
    outer: Vec<DataChunk>,
    hash_table: ConcurrentHashTable<Vec<Tuple>>,
}

impl SharedDynamicHashJoin {
    pub fn new(bucket_num: usize, inner: Vec<DataChunk>, outer: Vec<DataChunk>) -> Self {
        Self {
            inner,
            outer,
            hash_table: ConcurrentHashTable::new(bucket_num).unwrap(),
        }
    }
}

impl HashJoinBenchmark for SharedDynamicHashJoin {
    fn partition(&self) {}

    fn build(&mut self) {
        for chunk in std::mem::take(&mut self.inner) {
            chunk
                .into_par_iter()
                .for_each(|tuple| self.hash_table.insert(tuple));
        }
    }

    fn probe(&mut self) {
        let vec = Arc::new(boxcar::Vec::new());
        let global_start = Instant::now();
        for chunk in std::mem::take(&mut self.outer) {
            chunk.par_iter().for_each(|tuple| {
                let start = Instant::now();
                self.hash_table
                    .get_matching_tuples(tuple.key())
                    .inspect(Self::produce_tuple);
                let duration = start.elapsed().as_millis();
            });
        }
        let global_duration = global_start.elapsed().as_millis();
        let task_duration_sum = vec.clone().iter().map(|(_, t)| *t).sum::<u128>();
        // print stats
        println!("Global duration: {} ms", global_duration);
        println!("Task duration sum: {} ms", task_duration_sum);
        println!("Scheduling overhead: {} ms", global_duration - task_duration_sum);
    }
}

/// All threads build a single shared hash table and probe it concurrently.
/// Use static scheduling.
pub struct SharedStaticHashJoin {
    inner: Vec<DataChunk>,
    outer: Vec<DataChunk>,
    hash_table: ConcurrentHashTable<Vec<Tuple>>,
}

impl SharedStaticHashJoin {
    pub fn new(bucket_num: usize, inner: Vec<DataChunk>, outer: Vec<DataChunk>) -> Self {
        Self {
            inner,
            outer,
            hash_table: ConcurrentHashTable::new(bucket_num).unwrap(),
        }
    }
}

impl HashJoinBenchmark for SharedStaticHashJoin {
    fn partition(&self) {}

    fn build(&mut self) {
        for chunk in std::mem::take(&mut self.inner) {
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
                            .for_each(|tuple| self.hash_table.insert(tuple.clone()));
                    });
                }
            });
        }
    }

    fn probe(&mut self) {
        for chunk in std::mem::take(&mut self.outer) {
            // Divide the chunks into equal parts for each thread.
            let num_threads = rayon::current_num_threads();
            let thread_chunk_size = chunk.len() / num_threads;
            let thread_chunks = chunk.chunks(thread_chunk_size).collect::<Vec<_>>();
            rayon::scope(|s| {
                for chunk in thread_chunks {
                    s.spawn(|_| {
                        chunk.iter().for_each(|tuple| {
                            self.hash_table
                                .get_matching_tuples(tuple.key())
                                .inspect(Self::produce_tuple);
                        });
                    });
                }
            });
        }
    }
}
