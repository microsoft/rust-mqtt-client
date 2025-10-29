// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::{
    cmp::Ordering,
    mem::{self, MaybeUninit},
};

use bytes::{Buf as _, BufMut as _, Bytes, BytesMut};

use crate::buffer_pool::{Error, Owned, Shared};

#[derive(Debug)]
pub struct OwnedImpl {
    inner: BytesMut,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SharedImpl(Bytes);

impl OwnedImpl {
    pub(super) fn new(len: usize) -> Self {
        OwnedImpl {
            inner: BytesMut::with_capacity(len),
        }
    }
}

impl Owned for OwnedImpl {
    type Shared = SharedImpl;

    fn filled_len(&self) -> usize {
        self.inner.len()
    }

    fn filled_is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn filled(&self) -> &[u8] {
        self.inner.chunk()
    }

    fn filled_mut(&mut self) -> &mut [u8] {
        self.inner.as_mut()
    }

    fn unfilled_len(&self) -> usize {
        self.inner.capacity() - self.inner.len()
    }

    /// # Safety
    ///
    /// Caller must not read from this slice, and must only write initialized elements to it.
    unsafe fn unfilled_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        unsafe { self.inner.chunk_mut().as_uninit_slice_mut() }
    }

    /// # Safety
    ///
    /// Caller must ensure that `n` bytes have already been initialized.
    unsafe fn fill(&mut self, n: usize) {
        unsafe {
            self.inner.advance_mut(n);
        }
    }

    fn drain(&mut self, n: usize) {
        self.inner.advance(n);
    }

    fn split_to(&mut self, i: usize) -> Self {
        // Use `BytesMut::split_off` instead of `BytesMut::split_to` because
        // the former works if len <= i <= capacity as `Owned::split_to` allows,
        // whereas the latter requires i <= len
        let mut other = self.inner.split_off(i);
        mem::swap(&mut self.inner, &mut other);
        OwnedImpl { inner: other }
    }

    fn put_slice(&mut self, src: &[u8]) {
        self.inner.put_slice(src);
    }

    fn freeze(self) -> Self::Shared {
        SharedImpl(self.inner.freeze())
    }

    fn reserve(&mut self, new_unfilled_len: usize) -> Result<(), Error> {
        self.inner.reserve(new_unfilled_len);
        Ok(())
    }
}

impl Shared for SharedImpl {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn drain(&mut self, i: usize) {
        self.0.advance(i);
    }

    fn split_to(&mut self, i: usize) -> Self {
        SharedImpl(self.0.split_to(i))
    }
}

impl AsRef<[u8]> for SharedImpl {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl PartialEq<[u8]> for SharedImpl {
    fn eq(&self, other: &[u8]) -> bool {
        self.0 == other
    }
}

impl From<Bytes> for SharedImpl {
    fn from(b: Bytes) -> Self {
        Self(b)
    }
}

impl Ord for SharedImpl {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for SharedImpl {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
