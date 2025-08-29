// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::io::{self, IoSlice};

use buffer_pool::{BufferPool, BytesAccumulator, EitherBytesAccumulator, Iovecs};
use mqtt_proto::{ByteCounter, Packet, ProtocolVersion};

use crate::WritableStream;

/// This type wraps a writable network stream and provides API to write data to that stream.
pub struct Writer<BP>
where
    BP: BufferPool,
{
    inner: Box<dyn WritableStream>,
    buf: EitherBytesAccumulator<BP>,
}

impl<BP> Writer<BP>
where
    BP: BufferPool,
{
    pub(crate) fn new(inner: Box<dyn WritableStream>, buf: EitherBytesAccumulator<BP>) -> Self {
        Self { inner, buf }
    }

    /// Encodes an MQTT [`Packet`] and enqueues that data to this `Writer`.
    pub async fn write(
        &mut self,
        packet: &Packet<BP::Shared>,
        version: ProtocolVersion,
    ) -> io::Result<()> {
        if !self.buf.can_accept_more() {
            self.flush().await?;
            debug_assert!(
                self.buf.can_accept_more(),
                "Writer cannot accept more even after it was completely drained",
            );
        }

        let num_bytes_needed = {
            let mut counter = ByteCounter::<_, false>::new();
            let () = packet
                .encode(&mut counter, version)
                .map_err(std::io::Error::other)?;
            counter.into_count()
        };

        self.buf
            .reserve(num_bytes_needed)
            .map_err(std::io::Error::other)?;

        let () = packet
            .encode(&mut self.buf, version)
            .map_err(std::io::Error::other)?;

        Ok(())
    }

    /// Write all the data given to the [`BytesAccumulator`] returned by [`bytes_accumulator`] to
    /// the underlying network stream.
    pub async fn flush(&mut self) -> io::Result<()> {
        loop {
            let mut iovecs = [IoSlice::new(&[]); 128];
            let mut written = 0;
            let Iovecs {
                num_iovecs,
                total_len,
            } = self.buf.to_iovecs(&mut iovecs);
            if num_iovecs == 0 {
                break;
            }

            let mut iovecs = &mut iovecs[..num_iovecs];
            while written < total_len {
                match self.inner.write_vectored(iovecs).await {
                    Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                    Ok(written_) => {
                        written += written_;
                        if written < total_len {
                            IoSlice::advance_slices(&mut iovecs, written_);
                        }
                    }
                    Err(err) => return Err(err),
                }
            }

            self.buf.drain(written);
        }

        () = self.inner.flush().await?;

        Ok(())
    }
}

impl<BP> std::fmt::Debug for Writer<BP>
where
    BP: BufferPool,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Writer").finish_non_exhaustive()
    }
}
