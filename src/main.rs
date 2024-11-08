use std::num::NonZeroUsize;
use clap::Parser;
use parallel_hash_join::{
    join_benchmark::{
        partitioned::PartitionedHashJoin, sequential::SequentialHashJoin, shared::SharedHashJoin,
        HashJoinBenchmark, DYNAMIC_SCHEDULING, STATIC_SCHEDULING,
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
    /// Best partition number: 4096
    #[arg(short, long, default_value_t = 4096)]
    partition_num: usize,

    /// Total number of buckets in the hash table(s)
    #[arg(short, long, default_value_t = 1_048_576)]
    bucket_num: usize,

    /// Execution mode, value = [all, uq, uhd, uhs, lq, lhd, lhs, hq, hhd, hhs]
    /// [u: uniform, l: low skew, h: high skew], [q: sequential, hd: shared dynamic, hs: shared static]
    #[arg(short, long, default_value = "all")]
    mode: String,

    /// Number of threads to use
    /// Must be a power of 2, and a multiple of batch size and partition number
    /// Default = 8, if set to 0, it will use the number of logical cores
    #[arg(short, long, default_value_t = 8)]
    threads: usize,
}

fn main() {
    let args = Args::parse();

    let tuple_gen = TupleGenerator::new(
        args.inner_tuple_num as u64,
        args.outer_ratio as u64,
        args.batch_size as u64,
    );

    // let parallelism = std::thread::available_parallelism().unwrap();
    let parallelism = if args.threads == 0 {
        std::thread::available_parallelism().unwrap()
    } else {
        NonZeroUsize::try_from(args.threads).unwrap()
    };
    println!(" ==> Thread number: {}", parallelism.get());

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
        ($name:expr, $workload_fn:ident, $benchmark:ident $(::< $scheduling:ident >)? $(, $args:expr)*) => {
            let (inner, outer) = tuple_gen.$workload_fn();
            let mut benchmark = $benchmark$(::< $scheduling >)?::new($($args,)* inner, outer);
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
                SharedHashJoin::<DYNAMIC_SCHEDULING>,
                args.bucket_num
            );
            run!(
                "Uniform + Shared + Static",
                gen_uniform,
                SharedHashJoin::<STATIC_SCHEDULING>,
                args.bucket_num
            );
            run!(
                "Uniform + Partitioned + Dynamic",
                gen_low_skew,
                PartitionedHashJoin::<DYNAMIC_SCHEDULING>,
                args.bucket_num,
                args.partition_num
            );
            run!(
                "Uniform + Partitioned + Static",
                gen_uniform,
                PartitionedHashJoin::<STATIC_SCHEDULING>,
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
                SharedHashJoin::<DYNAMIC_SCHEDULING>,
                args.bucket_num
            );
            run!(
                "Low Skew + Shared + Static",
                gen_low_skew,
                SharedHashJoin::<STATIC_SCHEDULING>,
                args.bucket_num
            );
            run!(
                "Low Skew + Partitioned + Dynamic",
                gen_low_skew,
                PartitionedHashJoin::<DYNAMIC_SCHEDULING>,
                args.bucket_num,
                args.partition_num
            );
            run!(
                "Low Skew + Partitioned + Static",
                gen_low_skew,
                PartitionedHashJoin::<STATIC_SCHEDULING>,
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
                SharedHashJoin::<DYNAMIC_SCHEDULING>,
                args.bucket_num
            );
            run!(
                "High Skew + Shared + Static",
                gen_high_skew,
                SharedHashJoin::<STATIC_SCHEDULING>,
                args.bucket_num
            );
            run!(
                "High Skew + Partitioned + Dynamic",
                gen_low_skew,
                PartitionedHashJoin::<DYNAMIC_SCHEDULING>,
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
                SharedHashJoin::<DYNAMIC_SCHEDULING>,
                args.bucket_num
            );
        }
        "uhs" => {
            run!(
                "Uniform + Shared + Static",
                gen_uniform,
                SharedHashJoin::<STATIC_SCHEDULING>,
                args.bucket_num
            );
        }
        "upd" => {
            run!(
                "Uniform + Partitioned + Dynamic",
                gen_uniform,
                PartitionedHashJoin::<DYNAMIC_SCHEDULING>,
                args.bucket_num,
                args.partition_num
            );
        }
        "ups" => {
            run!(
                "Uniform + Partitioned + Static",
                gen_uniform,
                PartitionedHashJoin::<STATIC_SCHEDULING>,
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
                SharedHashJoin::<DYNAMIC_SCHEDULING>,
                args.bucket_num
            );
        }
        "lhs" => {
            run!(
                "Low Skew + Shared + Static",
                gen_low_skew,
                SharedHashJoin::<STATIC_SCHEDULING>,
                args.bucket_num
            );
        }
        "lpd" => {
            run!(
                "Low Skew + Partitioned + Dynamic",
                gen_low_skew,
                PartitionedHashJoin::<DYNAMIC_SCHEDULING>,
                args.bucket_num,
                args.partition_num
            );
        }
        "lps" => {
            run!(
                "Low Skew + Partitioned + Static",
                gen_low_skew,
                PartitionedHashJoin::<STATIC_SCHEDULING>,
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
                SharedHashJoin::<DYNAMIC_SCHEDULING>,
                args.bucket_num
            );
        }
        "hhs" => {
            run!(
                "High Skew + Shared + Static",
                gen_high_skew,
                SharedHashJoin::<STATIC_SCHEDULING>,
                args.bucket_num
            );
        }
        "hpd" => {
            run!(
                "High Skew + Partitioned + Dynamic",
                gen_high_skew,
                PartitionedHashJoin::<DYNAMIC_SCHEDULING>,
                args.bucket_num,
                args.partition_num
            );
        }
        "hps" => {
            run!(
                "High Skew + Partitioned + Static",
                gen_high_skew,
                PartitionedHashJoin::<STATIC_SCHEDULING>,
                args.bucket_num,
                args.partition_num
            );
        }
        _ => {}
    }
}
