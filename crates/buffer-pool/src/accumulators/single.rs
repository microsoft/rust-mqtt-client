// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::{fs::File, io::IoSlice, marker::PhantomData, sync::Arc};

use bytes_::{Buf as _, BytesMut};

use crate::{BytesAccumulator, Error, Shared, ToIovecs, maybe_uninit_copy_from_file_chunk};

/// This type impls [`BytesAccumulator`] with a target of a single [`BytesMut`].
///
/// `BytesAccumulatorImpl` does not require `reserve` to be called and will grow its inner `BytesMut` anyway.
#[derive(Debug, PartialEq, Eq)]
pub struct BytesAccumulatorImpl<S>(BytesMut, PhantomData<S>);

impl<S> BytesAccumulatorImpl<S> {
    pub fn new() -> Self {
        Default::default()
    }
}

impl<S> BytesAccumulator for BytesAccumulatorImpl<S>
where
    S: Shared,
{
    type Shared = S;

    fn can_accept_more(&self) -> bool {
        true
    }

    fn reserve(&mut self, additional: usize) -> Result<(), Error> {
        self.0.reserve(additional);
        Ok(())
    }

    fn put_shared(&mut self, src: Self::Shared) {
        self.0.extend_from_slice(src.as_ref());
    }

    fn put_file_chunk(&mut self, f: Arc<File>, offset: u64, len: usize) {
        self.0.reserve(len);
        _ = maybe_uninit_copy_from_file_chunk(self.0.spare_capacity_mut(), &f, offset, len);
        unsafe {
            self.0.set_len(self.0.len() + len);
        }
    }

    fn try_put_slice(&mut self, src: &[u8]) -> Option<()> {
        self.0.extend_from_slice(src);
        Some(())
    }

    fn put_done(&mut self) {}

    fn to_iovecs<'a>(&'a self, iovecs: &mut [IoSlice<'a>]) -> ToIovecs {
        if let Some(iovec) = iovecs.first_mut() {
            let chunk = self.0.chunk();
            if !chunk.is_empty() {
                *iovec = IoSlice::new(chunk);
                return ToIovecs::Iovecs {
                    num_iovecs: 1,
                    total_len: chunk.len(),
                };
            }
        }

        ToIovecs::Iovecs {
            num_iovecs: 0,
            total_len: 0,
        }
    }

    fn drain(&mut self, n: usize) {
        self.0.advance(n);
    }

    fn split(&mut self) -> Self {
        BytesAccumulatorImpl(self.0.split(), PhantomData)
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<S> AsRef<[u8]> for BytesAccumulatorImpl<S> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl<S> Default for BytesAccumulatorImpl<S> {
    fn default() -> Self {
        Self(BytesMut::new(), PhantomData)
    }
}
