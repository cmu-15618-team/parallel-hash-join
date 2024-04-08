use crate::tuple::Tuple;

use super::HashBucket;

impl HashBucket for Vec<Tuple> {
    type TupleIter<'a> = std::slice::Iter<'a, Tuple>;

    fn push(&mut self, tuple: Tuple) {
        self.push(tuple);
    }

    fn iter(&self) -> Self::TupleIter<'_> {
        self.as_slice().iter()
    }
}
