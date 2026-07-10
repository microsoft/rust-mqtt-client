// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::{
    io::{self, IoSlice},
    mem::MaybeUninit,
    pin::Pin,
};

use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadBuf, ReadHalf, WriteHalf};
use tokio_openssl::SslStream;

use crate::buffer_pool::{BufferPool, EitherAccumulator};
use crate::io::stream::TransportStream;
use crate::io::{ReadableStream, Reader, WritableStream, Writer};
use crate::transport::{Proxy, TlsConfig};

/// Connect to the given address and port with a TLS connection, and use the given buffer pools
/// to initialize the buffers for the stream reader and writer.
///
/// The hostname will be matched against the server cert SAN.
pub async fn connect<BP>(
    hostname: &str,
    port: u16,
    config: TlsConfig,
    proxy: Option<Proxy>,
    tcp_nodelay: bool,
    reader_pool: &BP,
    _writer_pool: &BP, // Retained for signature symmetry with the other transports; unused here.
) -> io::Result<(Reader<BP>, Writer<BP>)>
where
    BP: BufferPool,
{
    let ssl_stream = super::stream::connect_tls(hostname, port, config, proxy, tcp_nodelay).await?;

    let (read, write) = tokio::io::split(ssl_stream);
    let read_buf = reader_pool.take_empty_owned();
    let write_buf = EitherAccumulator::Single(Default::default());

    Ok((
        Reader::new(Box::new(OpensslStreamRead { inner: read }), read_buf),
        Writer::new(Box::new(OpensslStreamWrite { inner: write }), write_buf),
    ))
}

struct OpensslStreamRead {
    inner: ReadHalf<SslStream<TransportStream>>,
}

struct OpensslStreamWrite {
    inner: WriteHalf<SslStream<TransportStream>>,
}

impl ReadableStream for OpensslStreamRead {
    fn read<'a>(
        &'a mut self,
        buf: &'a mut [MaybeUninit<u8>],
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
        let mut buf = ReadBuf::uninit(buf);
        Box::pin(async move { self.inner.read_buf(&mut buf).await })
    }
}

impl WritableStream for OpensslStreamWrite {
    fn write_vectored<'a, 'buf>(
        &'a mut self,
        bufs: &'a [IoSlice<'buf>],
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
        Box::pin(self.inner.write_vectored(bufs))
    }

    fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(self.inner.flush())
    }
}
