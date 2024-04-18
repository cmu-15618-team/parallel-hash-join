use rayon::iter::{IntoParallelIterator, ParallelIterator};

use crate::tuple::{DataChunk, Tuple};

use super::{
    hash_table::sequential::SequentialHashTable, partition_hash, HashJoinBenchmark, SchedulingType,
    STATIC_SCHEDULING,
};

#[derive(Clone)]
pub struct Partition {
    inner_buffer: boxcar::Vec<Tuple>,
    outer_buffer: boxcar::Vec<Tuple>,
    hash_table: SequentialHashTable<Vec<Tuple>>,
}

impl Partition {
    fn new(bucket_num: usize) -> Self {
        Self {
            inner_buffer: boxcar::Vec::new(),
            outer_buffer: boxcar::Vec::new(),
            hash_table: SequentialHashTable::new(bucket_num).unwrap(),
        }
    }

    /// Move the tuples from the inner relation buffer to the hash table.
    fn populate_hash_table(&mut self) {
        // Insert all tuples in the buffer into the hash table.
        for tuple in std::mem::take(&mut self.inner_buffer).into_iter() {
            self.hash_table.insert(tuple.clone());
        }
    }
}

pub struct ProbePartition {
    outer_buffer: boxcar::Vec<Tuple>,
    hash_table: SequentialHashTable<Vec<Tuple>>,
}

impl From<Partition> for ProbePartition {
    fn from(partition: Partition) -> Self {
        Self {
            outer_buffer: partition.outer_buffer,
            hash_table: partition.hash_table,
        }
    }
}

pub struct PartitionedHashJoin<const S: SchedulingType> {
    inner: Option<Vec<DataChunk>>,
    outer: Option<Vec<DataChunk>>,
    bucket_num: usize,
    partition_num: usize,
}

impl<const S: SchedulingType> PartitionedHashJoin<S> {
    pub fn new(
        bucket_num: usize,
        partition_num: usize,
        inner: Vec<DataChunk>,
        outer: Vec<DataChunk>,
    ) -> Self {
        Self {
            inner: Some(inner),
            outer: Some(outer),
            bucket_num,
            partition_num,
        }
    }
}

impl<const S: SchedulingType> HashJoinBenchmark for PartitionedHashJoin<S> {
    type PartitionOutput = Vec<Partition>;
    type BuildOutput = Vec<ProbePartition>;

    fn partition(&mut self) -> Self::PartitionOutput {
        let partitions =
            vec![Partition::new(self.bucket_num / self.partition_num); self.partition_num];
        // Partition inner table.
        for chunk in self.inner.take().unwrap() {
            if S == STATIC_SCHEDULING {
                let num_threads = rayon::current_num_threads();
                let thread_chunk_size = chunk.len() / num_threads;
                let thread_chunks = chunk.chunks(thread_chunk_size).collect::<Vec<_>>();
                rayon::scope(|s| {
                    for chunk in thread_chunks.into_iter() {
                        s.spawn(|_| {
                            chunk.iter().for_each(|tuple| {
                                let partition_idx =
                                    partition_hash(tuple.key()) as usize & (self.partition_num - 1);
                                partitions[partition_idx].inner_buffer.push(tuple.clone());
                            });
                        });
                    }
                });
            } else {
                chunk.into_par_iter().for_each(|tuple| {
                    let partition_idx =
                        partition_hash(tuple.key()) as usize & (self.partition_num - 1);
                    partitions[partition_idx as usize].inner_buffer.push(tuple);
                });
            }
        }
        // Partition outer table.
        for chunk in self.outer.take().unwrap() {
            if S == STATIC_SCHEDULING {
                let num_threads = rayon::current_num_threads();
                let thread_chunk_size = chunk.len() / num_threads;
                let thread_chunks = chunk.chunks(thread_chunk_size).collect::<Vec<_>>();
                rayon::scope(|s| {
                    for chunk in thread_chunks.into_iter() {
                        s.spawn(|_| {
                            chunk.iter().for_each(|tuple| {
                                let partition_idx =
                                    partition_hash(tuple.key()) as usize & (self.partition_num - 1);
                                partitions[partition_idx].outer_buffer.push(tuple.clone());
                            });
                        });
                    }
                });
            } else {
                chunk.into_par_iter().for_each(|tuple| {
                    let partition_idx =
                        partition_hash(tuple.key()) as usize & (self.partition_num - 1);
                    partitions[partition_idx as usize].outer_buffer.push(tuple);
                });
            }
        }
        partitions
    }

    fn build(&mut self, partitions: Self::PartitionOutput) -> Self::BuildOutput {
        // Static scheduling is hard to implement because Rust does not allow multiple
        // threads to modify disjoint parts of the same vector. But the build phase is
        // very cheap, and inner relation is uniform, so the scheduling method should
        // not matter.
        partitions
            .into_par_iter()
            .map(|mut p| {
                p.populate_hash_table();
                p.into()
            })
            .collect()
    }

    fn probe(&mut self, partitions: Self::BuildOutput) {
        if S == STATIC_SCHEDULING {
            let num_threads = rayon::current_num_threads();
            let thread_partition_size = partitions.len() / num_threads;
            let thread_partitions = partitions.chunks(thread_partition_size).collect::<Vec<_>>();
            rayon::scope(|s| {
                for partitions in thread_partitions.into_iter() {
                    s.spawn(|_| {
                        partitions.iter().for_each(|partition| {
                            for (_, tuple) in partition.outer_buffer.iter() {
                                partition
                                    .hash_table
                                    .get_matching_tuples(tuple.key())
                                    .inspect(Self::produce_tuple);
                            }
                        });
                    });
                }
            });
        } else {
            partitions.into_par_iter().for_each(|partition| {
                for tuple in partition.outer_buffer.into_iter() {
                    partition
                        .hash_table
                        .get_matching_tuples(tuple.key())
                        .inspect(Self::produce_tuple);
                }
            });
        }
    }
}
