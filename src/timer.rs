use std::time::Instant;

pub struct ScopedTimer {
    name: &'static str,
    start: Instant,
}

impl ScopedTimer {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            start: Instant::now(),
        }
    }
}

impl Drop for ScopedTimer {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        println!("{} took {:?}", self.name, elapsed);
    }
}
