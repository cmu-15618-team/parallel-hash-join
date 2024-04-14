use crate::{
    join_benchmark::{bucket_hash, hash_bucket::HashBucket},
    tuple::{Key, Tuple},
};

use anyhow::{anyhow, Result};

pub struct ConcurrentHashTable<B: HashBucket> {
    buckets: Vec<parking_lot::Mutex<B>>,
    bucket_num: usize,
}

impl<B: HashBucket> ConcurrentHashTable<B> {
    pub fn new(bucket_num: usize) -> Result<ConcurrentHashTable<B>> {
        // Bucket number must be a power of 2 so modding can be optimized to bitwise AND.
        if !bucket_num.is_power_of_two() {
            return Err(anyhow!("Bucket number must be a power of 2"));
        }
        let mut buckets = Vec::with_capacity(bucket_num);
        for _ in 0..bucket_num {
            buckets.push(parking_lot::Mutex::new(B::default()));
        }
        Ok(ConcurrentHashTable {
            buckets,
            bucket_num,
        })
    }

    pub fn insert(&self, tuple: Tuple) {
        let bucket = &self.buckets[bucket_hash(tuple.key()) as usize & (self.bucket_num - 1)];
        let mut bucket = bucket.lock();
        bucket.push(tuple);
    }

    pub fn get_matching_tuples(&self, key: Key) -> Option<Tuple> {
        let bucket = &self.buckets[bucket_hash(key) as usize & (self.bucket_num - 1)];
        let tuple = unsafe { bucket.make_guard_unchecked() }
            .iter()
            .find(move |t| t.key_match(key))
            .cloned();
        tuple
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_concurrent_hash_table() {
        let hash_table = ConcurrentHashTable::<Vec<Tuple>>::new(16).unwrap();
        let tuple = Tuple::new(1);
        hash_table.insert(tuple.clone());

        assert_eq!(hash_table.get_matching_tuples(1), Some(tuple));

        assert_eq!(hash_table.get_matching_tuples(2), None);

        let tuple = Tuple::new(2);
        hash_table.insert(tuple.clone());

        assert_eq!(hash_table.get_matching_tuples(2), Some(tuple));
    }
}
