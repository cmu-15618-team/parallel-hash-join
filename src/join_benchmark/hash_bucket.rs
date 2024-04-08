mod vec;

use crate::tuple::Tuple;

pub trait HashBucket: Default {
    type TupleIter<'a>: Iterator<Item = &'a Tuple>
    where
        Self: 'a;

    fn push(&mut self, tuple: Tuple);
    fn iter(&self) -> Self::TupleIter<'_>;
}
