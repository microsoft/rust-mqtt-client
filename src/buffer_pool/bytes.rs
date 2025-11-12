// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! `bytes` implementation of [`BufferPool`].

use bytes::{Bytes, BytesMut};

use crate::buffer_pool::{BufferPool, Error};

mod buffers;

// This could be Copy too, but that makes it harder to easily swap
// the custom BufferPoolImpl with this one, since it generates warnings
// on code like `pool.clone()` that calling `.clone()` on a Copy type is silly.
#[derive(Clone, Debug, Default)]
pub struct BufferPoolImpl;

impl BufferPoolImpl {
    pub fn new() -> Self {
        Default::default()
    }
}

impl BufferPool for BufferPoolImpl {
    type Shared = Bytes;
    type Owned = BytesMut;

    fn take_owned(&self, len: usize) -> Result<Self::Owned, Error> {
        Ok(BytesMut::with_capacity(len))
    }

    fn take_empty_owned(&self) -> Self::Owned {
        BytesMut::with_capacity(0)
    }
}
