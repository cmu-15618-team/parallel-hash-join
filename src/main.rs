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
    assert_eq!(args.batch_size % parallelism.get(), 0);

    rayon::ThreadPoolBuilder::new()
        .num_threads(parallelism.get())
        .build_global()
        .unwrap();

    macro_rules! run {
        ($name:expr, $workload_fn:ident, $benchmark:ident) => {
            let (inner, outer) = tuple_gen.$workload_fn();
            let mut join = $benchmark::new(args.bucket_num, inner, outer);
            println!("Running {}", $name);
            join.run();
            println!();
        };
    }

    // Execute the selected mode
    match args.mode.as_str() {
        "all" => {
            run!("Uniform + Sequential", gen_uniform, SequentialHashJoin);
            run!(
                "Uniform + Shared + Dynamic",
                gen_uniform,
                SharedDynamicHashJoin
            );
            run!(
                "Uniform + Shared + Static",
                gen_uniform,
                SharedStaticHashJoin
            );
            run!("Low Skew + Sequential", gen_low_skew, SequentialHashJoin);
            run!(
                "Low Skew + Shared + Dynamic",
                gen_low_skew,
                SharedDynamicHashJoin
            );
            run!(
                "Low Skew + Shared + Static",
                gen_low_skew,
                SharedStaticHashJoin
            );
            run!("High Skew + Sequential", gen_high_skew, SequentialHashJoin);
            run!(
                "High Skew + Shared + Dynamic",
                gen_high_skew,
                SharedDynamicHashJoin
            );
            run!(
                "High Skew + Shared + Static",
                gen_high_skew,
                SharedStaticHashJoin
            );
        }
        "uq" => {
            run!("Uniform + Sequential", gen_uniform, SequentialHashJoin);
        }
        "uhd" => {
            run!(
                "Uniform + Shared + Dynamic",
                gen_uniform,
                SharedDynamicHashJoin
            );
        }
        "uhs" => {
            run!(
                "Uniform + Shared + Static",
                gen_uniform,
                SharedStaticHashJoin
            );
        }
        "lq" => {
            run!("Low Skew + Sequential", gen_low_skew, SequentialHashJoin);
        }
        "lhd" => {
            run!(
                "Low Skew + Shared + Dynamic",
                gen_low_skew,
                SharedDynamicHashJoin
            );
        }
        "lhs" => {
            run!(
                "Low Skew + Shared + Static",
                gen_low_skew,
                SharedStaticHashJoin
            );
        }
        "hq" => {
            run!("High Skew + Sequential", gen_high_skew, SequentialHashJoin);
        }
        "hhd" => {
            run!(
                "High Skew + Shared + Dynamic",
                gen_high_skew,
                SharedDynamicHashJoin
            );
        }
        "hhs" => {
            run!(
                "High Skew + Shared + Static",
                gen_high_skew,
                SharedStaticHashJoin
            );
        }
        _ => {}
    }
}
