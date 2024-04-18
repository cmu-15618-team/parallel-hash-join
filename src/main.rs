use clap::Parser;
use parallel_hash_join::{
    join_benchmark::{
        partitioned::PartitionedDynamicHashJoin,
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
    #[arg(long, default_value_t = 1_00_000)]
    batch_size: usize,

    /// Number of partitions. Default value calculated from GHC L3 cache size of 10MB.
    #[arg(short, long, default_value_t = 32)]
    partition_num: usize,

    /// Total number of buckets in the hash table(s)
    #[arg(short, long, default_value_t = 1_048_576)]
    bucket_num: usize,

    /// Execution mode, value = [all, uq, uhd, uhs, lq, lhd, lhs, hq, hhd, hhs]
    /// [u: uniform, l: low skew, h: high skew], [q: sequential, hd: shared dynamic, hs: shared static]
    #[arg(short, long, default_value = "all")]
    mode: String,
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
    assert!(parallelism.is_power_of_two());
    assert_eq!(args.batch_size % parallelism.get(), 0);
    assert_eq!(args.partition_num % parallelism.get(), 0);
    // We should be able to assign buckets evenly to partitions.
    assert_eq!(args.bucket_num % args.partition_num, 0);

    rayon::ThreadPoolBuilder::new()
        .num_threads(parallelism.get())
        .build_global()
        .unwrap();

    macro_rules! run {
        ($name:expr, $workload_fn:ident, $benchmark:ident $(, $args:expr)*) => {
            let (inner, outer) = tuple_gen.$workload_fn();
            let mut benchmark = $benchmark::new($($args,)* inner, outer);
            println!("Running {}...", $name);
            benchmark.run();
            println!();
        };
    }

    // Execute the selected mode
    match args.mode.as_str() {
        "all" => {
            run!(
                "Uniform + Sequential",
                gen_uniform,
                SequentialHashJoin,
                args.bucket_num
            );
            run!(
                "Uniform + Shared + Dynamic",
                gen_uniform,
                SharedDynamicHashJoin,
                args.bucket_num
            );
            run!(
                "Uniform + Shared + Static",
                gen_uniform,
                SharedStaticHashJoin,
                args.bucket_num
            );
            run!(
                "Uniform + Partitioned + Dynamic",
                gen_low_skew,
                PartitionedDynamicHashJoin,
                args.bucket_num,
                args.partition_num
            );
            run!(
                "Low Skew + Sequential",
                gen_low_skew,
                SequentialHashJoin,
                args.bucket_num
            );
            run!(
                "Low Skew + Shared + Dynamic",
                gen_low_skew,
                SharedDynamicHashJoin,
                args.bucket_num
            );
            run!(
                "Low Skew + Shared + Static",
                gen_low_skew,
                SharedStaticHashJoin,
                args.bucket_num
            );
            run!(
                "Low Skew + Partitioned + Dynamic",
                gen_low_skew,
                PartitionedDynamicHashJoin,
                args.bucket_num,
                args.partition_num
            );
            run!(
                "High Skew + Sequential",
                gen_high_skew,
                SequentialHashJoin,
                args.bucket_num
            );
            run!(
                "High Skew + Shared + Dynamic",
                gen_high_skew,
                SharedDynamicHashJoin,
                args.bucket_num
            );
            run!(
                "High Skew + Shared + Static",
                gen_high_skew,
                SharedStaticHashJoin,
                args.bucket_num
            );
            run!(
                "High Skew + Partitioned + Dynamic",
                gen_low_skew,
                PartitionedDynamicHashJoin,
                args.bucket_num,
                args.partition_num
            );
        }
        "uq" => {
            run!(
                "Uniform + Sequential",
                gen_uniform,
                SequentialHashJoin,
                args.bucket_num
            );
        }
        "uhd" => {
            run!(
                "Uniform + Shared + Dynamic",
                gen_uniform,
                SharedDynamicHashJoin,
                args.bucket_num
            );
        }
        "uhs" => {
            run!(
                "Uniform + Shared + Static",
                gen_uniform,
                SharedStaticHashJoin,
                args.bucket_num
            );
        }
        "upd" => {
            run!(
                "Uniform + Partitioned + Dynamic",
                gen_uniform,
                PartitionedDynamicHashJoin,
                args.bucket_num,
                args.partition_num
            );
        }
        "lq" => {
            run!(
                "Low Skew + Sequential",
                gen_low_skew,
                SequentialHashJoin,
                args.bucket_num
            );
        }
        "lhd" => {
            run!(
                "Low Skew + Shared + Dynamic",
                gen_low_skew,
                SharedDynamicHashJoin,
                args.bucket_num
            );
        }
        "lhs" => {
            run!(
                "Low Skew + Shared + Static",
                gen_low_skew,
                SharedStaticHashJoin,
                args.bucket_num
            );
        }
        "lpd" => {
            run!(
                "Low Skew + Partitioned + Dynamic",
                gen_low_skew,
                PartitionedDynamicHashJoin,
                args.bucket_num,
                args.partition_num
            );
        }
        "hq" => {
            run!(
                "High Skew + Sequential",
                gen_high_skew,
                SequentialHashJoin,
                args.bucket_num
            );
        }
        "hhd" => {
            run!(
                "High Skew + Shared + Dynamic",
                gen_high_skew,
                SharedDynamicHashJoin,
                args.bucket_num
            );
        }
        "hhs" => {
            run!(
                "High Skew + Shared + Static",
                gen_high_skew,
                SharedStaticHashJoin,
                args.bucket_num
            );
        }
        "hpd" => {
            run!(
                "High Skew + Partitioned + Dynamic",
                gen_high_skew,
                PartitionedDynamicHashJoin,
                args.bucket_num,
                args.partition_num
            );
        }
        _ => {}
    }
}
