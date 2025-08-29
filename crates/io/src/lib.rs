// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::{
    io::{self, IoSlice},
    mem::MaybeUninit,
    pin::Pin,
};

mod reader;
pub use reader::Reader;

pub mod tokio_tcp;
pub use tokio_tcp::{TcpStreamRead, TcpStreamWrite};

mod writer;
pub use writer::Writer;

trait ReadableStream {
    fn read<'a, 'buf>(
        &'a mut self,
        buf: &'buf mut [MaybeUninit<u8>],
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + 'a>>
    where
        'buf: 'a;
}

trait WritableStream {
    fn write_vectored<'a, 'buf>(
        &'a mut self,
        bufs: &'buf [IoSlice<'buf>],
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + 'a>>
    where
        'buf: 'a;

    fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + '_>>;
}
