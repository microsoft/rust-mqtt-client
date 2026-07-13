// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// TODO: Revisit this suppression
#![allow(dead_code)]

use std::{
    io::{self, IoSlice},
    mem::MaybeUninit,
    pin::Pin,
};

mod reader;
pub use reader::Reader;

#[cfg(feature = "__integration")]
pub mod test;

pub mod tokio_tcp;

pub mod tokio_tls;

#[cfg(feature = "websockets")]
pub mod tokio_ws;

mod writer;
pub use writer::Writer;

// TODO(rustup): Remove `Sync` bounds when `std::sync::Exclusive` becomes stable.

pub(crate) trait ReadableStream: Send + Sync {
    fn read<'a>(
        &'a mut self,
        buf: &'a mut [MaybeUninit<u8>],
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>>;
}

pub(crate) trait WritableStream: Send + Sync {
    fn write_vectored<'a, 'buf>(
        &'a mut self,
        bufs: &'a [IoSlice<'buf>],
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>>;

    fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>>;
}
