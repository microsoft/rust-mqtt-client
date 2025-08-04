// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! `bytes` implementation of [`BufferSource`] for tests.
//!
//! The types also offer convenience functions to create the buffer types directly.

// This is test code.

use crate::{BufferSource, Error};

mod buffers;
pub use buffers::{OwnedImpl, SharedImpl};

// This could be Copy too, but that makes it harder to easily swap
// the custom BufferPoolImpl with this one, since it generates warnings
// on code like `pool.clone()` that calling `.clone()` on a Copy type is silly.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BufferPoolImpl;

impl BufferPoolImpl {
    pub fn new() -> Self {
        Self
    }
}

impl BufferSource for BufferPoolImpl {
    type Shared = SharedImpl;
    type Owned = OwnedImpl;

    fn take_owned(&self, len: usize) -> Result<Self::Owned, Error> {
        Ok(OwnedImpl::new(len))
    }

    fn take_empty_owned(&self) -> Self::Owned {
        OwnedImpl::new(0)
    }
}
