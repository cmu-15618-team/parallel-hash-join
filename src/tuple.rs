use rand::{distributions::Distribution, Rng};

const LOW_SKEW_ZIPF_ALPHA: f64 = 2.0;
const HIGH_SKEW_ZIPF_ALPHA: f64 = 1.25;

pub type Key = u64;

#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(align(16))]
pub struct Tuple {
    key: Key,
}

impl Tuple {
    pub fn new(key: Key) -> Tuple {
        Tuple { key }
    }

    pub fn key(&self) -> Key {
        self.key
    }

    pub fn key_match(&self, key: Key) -> bool {
        self.key == key
    }
}

pub type DataChunk = Vec<Tuple>;

pub struct TupleGenerator {
    inner_tuple_num: u64,
    inner_batch_num: u64,
    outer_batch_num: u64,
    batch_size: u64,
}

impl TupleGenerator {
    pub fn new(inner_tuple_num: u64, outer_ratio: u64, batch_size: u64) -> Self {
        if inner_tuple_num % batch_size != 0 {
            panic!("inner_tuple_num must be a multiple of batch_size");
        }
        let inner_batch_num = inner_tuple_num / batch_size;
        let outer_batch_num = inner_batch_num * outer_ratio;
        Self {
            inner_tuple_num,
            inner_batch_num,
            outer_batch_num,
            batch_size,
        }
    }

    /// Each inner tuple matches every outer tuple with equal probability.
    pub fn gen_uniform(&self) -> (Vec<DataChunk>, Vec<DataChunk>) {
        let mut rng = rand::thread_rng();
        let inner = self.gen_inner_table();
        let outer = self.gen_outer_table(|_| Tuple::new(rng.gen_range(0..self.inner_tuple_num)));
        (inner, outer)
    }

    pub fn gen_low_skew(&self) -> (Vec<DataChunk>, Vec<DataChunk>) {
        println!("Generating low skew data, skew alpha: {}", LOW_SKEW_ZIPF_ALPHA);
        let mut rng = rand::thread_rng();
        let zipf = zipf::ZipfDistribution::new(self.inner_tuple_num as usize, LOW_SKEW_ZIPF_ALPHA)
            .unwrap();
        let inner = self.gen_inner_table();
        let outer = self.gen_outer_table(|_| Tuple::new(zipf.sample(&mut rng) as u64 - 1));
        (inner, outer)
    }

    pub fn gen_high_skew(&self) -> (Vec<DataChunk>, Vec<DataChunk>) {
        let mut rng = rand::thread_rng();
        let zipf = zipf::ZipfDistribution::new(self.inner_tuple_num as usize, HIGH_SKEW_ZIPF_ALPHA)
            .unwrap();
        let inner = self.gen_inner_table();
        let outer = self.gen_outer_table(|_| Tuple::new(zipf.sample(&mut rng) as u64 - 1));
        (inner, outer)
    }

    /// The inner table's key is an incremental sequence starting from 0.
    fn gen_inner_table(&self) -> Vec<Vec<Tuple>> {
        self.gen_table(self.inner_batch_num, Tuple::new)
    }

    fn gen_outer_table(&self, gen_fn: impl FnMut(u64) -> Tuple) -> Vec<DataChunk> {
        self.gen_table(self.outer_batch_num, gen_fn)
    }

    /// Generate a table consisting of `batch_num`` batches. `gen_fn` maps index to `Tuple``.
    fn gen_table(&self, batch_num: u64, mut gen_fn: impl FnMut(u64) -> Tuple) -> Vec<DataChunk> {
        (0..batch_num)
            .map(|batch_idx| {
                (0..self.batch_size)
                    .map(|i| gen_fn(batch_idx * self.batch_size + i))
                    .collect::<DataChunk>()
            })
            .collect::<Vec<Vec<Tuple>>>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gen_uniform() {
        let gen = TupleGenerator::new(100, 10, 10);
        let (inner, outer) = gen.gen_uniform();
        verify_gen(&gen, &inner, &outer);
    }

    #[test]
    fn test_gen_low_skew() {
        let gen = TupleGenerator::new(100, 10, 10);
        let (inner, outer) = gen.gen_low_skew();
        verify_gen(&gen, &inner, &outer);
    }

    #[test]
    fn test_gen_high_skew() {
        let gen = TupleGenerator::new(100, 10, 10);
        let (inner, outer) = gen.gen_high_skew();
        verify_gen(&gen, &inner, &outer);
    }

    fn verify_gen(gen: &TupleGenerator, inner: &[DataChunk], outer: &[DataChunk]) {
        let batch_size = gen.batch_size;
        let inner_tuple_num = gen.inner_tuple_num;
        let outer_batch_num = gen.outer_batch_num;

        let key_range = 0..inner_tuple_num;

        // Check batch number and PK sequence.
        assert_eq!(inner.len(), gen.inner_batch_num as usize);
        for (batch_idx, batch) in inner.iter().enumerate() {
            assert_eq!(batch.len(), batch_size as usize);
            for (i, tuple) in batch.iter().enumerate() {
                assert_eq!(tuple.key(), (batch_idx * batch_size as usize + i) as u64);
            }
        }

        // Check batch number and FK constraint.
        assert_eq!(outer.len(), outer_batch_num as usize);
        for batch in outer {
            assert_eq!(batch.len(), batch_size as usize);
            for tuple in batch {
                assert!(key_range.contains(&tuple.key()));
            }
        }
    }
}
