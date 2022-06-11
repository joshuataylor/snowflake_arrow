use crate::SnowflakeReturnType;
pub use streaming_iterator::StreamingIterator;

/// A [`StreamingIterator`] with an internal buffer of [`Vec<ReturnType>`]
pub struct ReturnTypeStreamingIterator<I, F, T>
where
    I: Iterator<Item = T>,
    F: FnMut(T, &mut Vec<SnowflakeReturnType>),
{
    iterator: I,
    f: F,
    buffer: Vec<SnowflakeReturnType>,
    is_valid: bool,
}

impl<I, F, T> ReturnTypeStreamingIterator<I, F, T>
where
    I: Iterator<Item = T>,
    F: FnMut(T, &mut Vec<SnowflakeReturnType>),
{
    #[inline]
    pub fn new(iterator: I, f: F, buffer: Vec<SnowflakeReturnType>) -> Self {
        Self {
            iterator,
            f,
            buffer,
            is_valid: false,
        }
    }
}

impl<I, F, T> StreamingIterator for ReturnTypeStreamingIterator<I, F, T>
where
    I: Iterator<Item = T>,
    F: FnMut(T, &mut Vec<SnowflakeReturnType>),
{
    type Item = Vec<SnowflakeReturnType>;

    #[inline]
    fn advance(&mut self) {
        let a = self.iterator.next();
        if let Some(a) = a {
            self.is_valid = true;
            self.buffer.clear();
            (self.f)(a, &mut self.buffer);
        } else {
            self.is_valid = false;
        }
    }

    #[inline]
    fn get(&self) -> Option<&Self::Item> {
        if self.is_valid {
            Some(&self.buffer)
        } else {
            None
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iterator.size_hint()
    }
}
