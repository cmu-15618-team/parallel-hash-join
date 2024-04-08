use crate::{
    join_benchmark::{bucket_hash, hash_bucket::HashBucket},
    tuple::{Key, Tuple},
};

pub struct SequentialHashTable<B: HashBucket> {
    buckets: Vec<B>,
    bucket_num: usize,
}

impl<B: HashBucket> SequentialHashTable<B>
where
    B: HashBucket,
{
    pub fn new(bucket_num: usize) -> SequentialHashTable<B> {
        let mut buckets = Vec::with_capacity(bucket_num);
        for _ in 0..bucket_num {
            buckets.push(B::default());
        }
        SequentialHashTable {
            buckets,
            bucket_num,
        }
    }

    pub fn insert(&mut self, key: Key, tuple: Tuple) {
        let bucket = &mut self.buckets[bucket_hash(key) as usize % self.bucket_num];
        bucket.push(tuple);
    }

    pub fn get_matching_tuples(&self, key: Key) -> impl Iterator<Item = &Tuple> {
        let bucket = &self.buckets[key as usize % self.buckets.len()];
        bucket.iter().filter(move |t| t.key_match(key))
    }
}
