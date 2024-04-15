mod vec;

use crate::tuple::Tuple;

/// A hash bucket supports pushing tuples and iterating over them.
pub trait HashBucket: Default {
    type TupleIter<'a>: Iterator<Item = &'a Tuple>
    where
        Self: 'a;

    fn push(&mut self, tuple: Tuple);
    fn iter(&self) -> Self::TupleIter<'_>;
}

// TODO: implement blocked linked list to see the effect of cache invalidation due to reallocation.
