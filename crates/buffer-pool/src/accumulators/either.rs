// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::io::IoSlice;

use crate::{BufferPool, BytesAccumulator, Error, Iovecs};

pub enum EitherBytesAccumulator<BP>
where
    BP: BufferPool,
{
    Iovecs(super::iovecs::BytesAccumulatorImpl<BP::Owned>),
    Single(super::single::BytesAccumulatorImpl<BP::Shared>),
}

impl<BP> BytesAccumulator for EitherBytesAccumulator<BP>
where
    BP: BufferPool,
{
    type Shared = BP::Shared;

    fn can_accept_more(&self) -> bool {
        match self {
            Self::Iovecs(ba) => ba.can_accept_more(),
            Self::Single(ba) => ba.can_accept_more(),
        }
    }

    fn reserve(&mut self, additional: usize) -> Result<(), Error> {
        match self {
            Self::Iovecs(ba) => ba.reserve(additional),
            Self::Single(ba) => ba.reserve(additional),
        }
    }

    fn try_put_slice(&mut self, src: &[u8]) -> Option<()> {
        match self {
            Self::Iovecs(ba) => ba.try_put_slice(src),
            Self::Single(ba) => ba.try_put_slice(src),
        }
    }

    fn put_shared(&mut self, src: Self::Shared) {
        match self {
            Self::Iovecs(ba) => ba.put_shared(src),
            Self::Single(ba) => ba.put_shared(src),
        }
    }

    fn put_done(&mut self) {
        match self {
            Self::Iovecs(ba) => ba.put_done(),
            Self::Single(ba) => ba.put_done(),
        }
    }

    fn to_iovecs<'a>(&'a self, iovecs: &mut [IoSlice<'a>]) -> Iovecs {
        match self {
            Self::Iovecs(ba) => ba.to_iovecs(iovecs),
            Self::Single(ba) => ba.to_iovecs(iovecs),
        }
    }

    fn drain(&mut self, n: usize) {
        match self {
            Self::Iovecs(ba) => ba.drain(n),
            Self::Single(ba) => ba.drain(n),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Iovecs(ba) => ba.is_empty(),
            Self::Single(ba) => ba.is_empty(),
        }
    }
}

impl<BP> std::fmt::Debug for EitherBytesAccumulator<BP>
where
    BP: BufferPool,
    super::iovecs::BytesAccumulatorImpl<BP::Owned>: std::fmt::Debug,
    super::single::BytesAccumulatorImpl<BP::Shared>: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Iovecs(ba) => f.debug_tuple("Iovecs").field(ba).finish(),
            Self::Single(ba) => f.debug_tuple("Single").field(ba).finish(),
        }
    }
}
