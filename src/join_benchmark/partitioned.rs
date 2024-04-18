use rayon::iter::{IntoParallelIterator, ParallelIterator};

use crate::tuple::{DataChunk, Tuple};

use super::{hash_table::sequential::SequentialHashTable, partition_hash, HashJoinBenchmark};

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

    fn consume_input_buffer(self) -> (boxcar::Vec<Tuple>, ProbePartition) {
        (
            self.inner_buffer,
            ProbePartition {
                outer_buffer: self.outer_buffer,
                hash_table: self.hash_table,
            },
        )
    }
}

pub struct ProbePartition {
    outer_buffer: boxcar::Vec<Tuple>,
    hash_table: SequentialHashTable<Vec<Tuple>>,
}

pub struct PartitionedDynamicHashJoin {
    inner: Option<Vec<DataChunk>>,
    outer: Option<Vec<DataChunk>>,
    bucket_num: usize,
    partition_num: usize,
}

impl PartitionedDynamicHashJoin {
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

impl HashJoinBenchmark for PartitionedDynamicHashJoin {
    type PartitionOutput = Vec<Partition>;
    type BuildOutput = Vec<ProbePartition>;

    fn partition(&mut self) -> Self::PartitionOutput {
        let partitions =
            vec![Partition::new(self.bucket_num / self.partition_num); self.partition_num];
        // Partition inner table.
        for chunk in self.inner.take().unwrap() {
            chunk.into_par_iter().for_each(|tuple| {
                let partition_idx = partition_hash(tuple.key()) as usize & (self.partition_num - 1);
                partitions[partition_idx as usize].inner_buffer.push(tuple);
            });
        }
        // Partition outer table.
        for chunk in self.outer.take().unwrap() {
            chunk.into_par_iter().for_each(|tuple| {
                let partition_idx = partition_hash(tuple.key()) as usize & (self.partition_num - 1);
                partitions[partition_idx as usize].outer_buffer.push(tuple);
            });
        }
        partitions
    }

    fn build(&mut self, partitions: Self::PartitionOutput) -> Self::BuildOutput {
        partitions
            .into_par_iter()
            .map(|partition| {
                let (inner_buffer, mut probe_partition) = partition.consume_input_buffer();
                // Insert all tuples in the buffer into the hash table.
                for tuple in inner_buffer.into_iter() {
                    probe_partition.hash_table.insert(tuple);
                }
                probe_partition
            })
            .collect()
    }

    fn probe(&mut self, partitions: Self::BuildOutput) {
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
