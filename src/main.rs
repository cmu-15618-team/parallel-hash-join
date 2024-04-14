use clap::Parser;
use parallel_hash_join::{
    join_benchmark::{
        sequential::SequentialHashJoin,
        shared::{SharedDynamicHashJoin, SharedStaticHashJoin},
        HashJoinBenchmark,
    },
    tuple::TupleGenerator,
};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Number of tuples in the inner relation
    #[arg(short, long, default_value_t = 16_000_000)]
    inner_tuple_num: usize,

    /// The ratio of the number of tuples of the outer relation to that of the outer relation
    #[arg(short, long, default_value_t = 16)]
    outer_ratio: usize,

    /// Number of tuples in each batch.
    #[arg(short, long, default_value_t = 16_000_000)]
    batch_size: usize,

    /// Number of partitions. Default value calculated from GHC L3 cache size of 10MB.
    #[arg(short, long, default_value_t = 32)]
    partition_num: usize,

    /// Total number of buckets in the hash table(s)
    #[arg(short, long, default_value_t = 1_048_576)]
    bucket_num: usize,
}

fn main() {
    let args = Args::parse();

    let tuple_gen = TupleGenerator::new(
        args.inner_tuple_num as u64,
        args.outer_ratio as u64,
        args.batch_size as u64,
    );

    let parallelism = std::thread::available_parallelism().unwrap();
    println!("Available parallelism: {}", parallelism);

    // Batch size must be a multiple of the number of threads.
    assert_eq!(args.batch_size % parallelism.get(), 0);

    rayon::ThreadPoolBuilder::new()
        .num_threads(parallelism.get())
        .build_global()
        .unwrap();

    {
        let (inner, outer) = tuple_gen.gen_uniform();
        let mut join = SequentialHashJoin::new(args.bucket_num, inner, outer);
        join.run();
    }

    {
        let (inner, outer) = tuple_gen.gen_high_skew();
        let mut join = SharedDynamicHashJoin::new(args.bucket_num, inner, outer);
        join.run();
    }

    {
        let (inner, outer) = tuple_gen.gen_high_skew();
        let mut join = SharedStaticHashJoin::new(args.bucket_num, inner, outer);
        join.run();
    }
}
