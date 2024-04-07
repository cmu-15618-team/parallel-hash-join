use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Number of tuples in the inner relation
    #[arg(short, long, default_value_t = 16_000_000)]
    inner_tuple_num: usize,

    /// The ratio of the number of tuples of the outer relation to that of the outer relation
    #[arg(short, long, default_value_t = 16)]
    outer_ratio: usize,

    /// Number of partitions. Default value calculated from GHC L3 cache size of 10MB.
    #[arg(short, long, default_value_t = 32)]
    partition_num: usize,

    /// Total number of buckets in the hash table(s)
    #[arg(short, long, default_value_t = 1_000_000)]
    bucket_num: usize,
}

fn main() {
    let args = Args::parse();
}
