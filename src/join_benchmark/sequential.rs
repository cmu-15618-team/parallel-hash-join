use crate::tuple::DataChunk;

pub struct SequentialHashJoin {
    inner: Vec<DataChunk>,
    outer: Vec<DataChunk>,
}

impl SequentialHashJoin {}
