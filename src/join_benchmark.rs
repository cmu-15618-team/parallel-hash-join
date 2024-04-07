use std::hint::black_box;

fn time_phase(name: &str, f: impl Fn()) {
    let start = std::time::Instant::now();
    f();
    let elapsed = start.elapsed();
    println!("{}: {:?}", name, elapsed);
}

pub trait JoinBenchmark {
    fn partition_phase(&self);
    fn build_phase(&self);
    fn probe_phase(&self);

    fn run(&self) {
        time_phase("partition", || self.partition_phase());
        time_phase("build", || self.build_phase());
        time_phase("probe", || self.probe_phase());
    }

    /// Waste some time to simulate outputting a tuple.
    /// See https://doc.rust-lang.org/std/hint/fn.black_box.html
    fn produce_tuple() {
        fn contains(haystack: &[&str], needle: &str) -> bool {
            haystack.iter().any(|x| x == &needle)
        }

        fn waste_time() -> bool {
            let haystack = vec!["candlelight"];
            let needle = "candlelight";
            for _ in 0..10 {
                black_box(contains(black_box(&haystack), black_box(needle)));
            }
            true
        }

        black_box(waste_time());
    }
}
