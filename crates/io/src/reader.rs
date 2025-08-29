// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::io;

use buffer_pool::{BufferPool, Owned};

use crate::ReadableStream;

/// This type wraps a readable network stream and provides API to read from it
/// into a given [`Owned`](buffer_pool::Owned).
pub struct Reader<BP>
where
    BP: BufferPool,
{
    inner: Box<dyn ReadableStream>,
    buf: BP::Owned,
}

#[derive(Debug)]
pub struct RawPacket<S> {
    pub first_byte: u8,
    pub rest: S,
}

impl<BP> Reader<BP>
where
    BP: BufferPool,
{
    pub(crate) fn new(inner: Box<dyn ReadableStream>, buf: BP::Owned) -> Self {
        Self { inner, buf }
    }

    /// Receives and decodes an MQTT packet from the underlying network stream.
    pub async fn read(&mut self) -> io::Result<RawPacket<BP::Shared>> {
        let (fixed_header_len, first_byte, remaining_length) = loop {
            let mut filled = self.buf.filled();
            let original_filled_len = filled.len();
            if let Some((first_byte, remaining_length)) =
                mqtt_proto::decode_fixed_header(&mut filled).map_err(io::Error::other)?
            {
                let fixed_header_len = filled.len() - original_filled_len;
                break (fixed_header_len, first_byte, remaining_length);
            }

            // Reserve space for the largest fixed header, one byte for packet type and four bytes for remaining length.
            self.buf.reserve(5).map_err(io::Error::other)?;
            // ... and read it.
            //
            // SAFETY: Requirements of `unfilled_mut` and `fill` are upheld.
            unsafe {
                let read = self.inner.read(self.buf.unfilled_mut()).await?;
                self.buf.fill(read);
                if read == 0 {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
            }
        };

        if let Some(remaining) =
            (fixed_header_len + remaining_length).checked_sub(self.buf.filled_len())
        {
            self.buf.reserve(remaining).map_err(io::Error::other)?;
        }

        while self.buf.filled_len() < fixed_header_len + remaining_length {
            // SAFETY: Requirements of `unfilled_mut` and `fill` are upheld.
            unsafe {
                let read = self.inner.read(self.buf.unfilled_mut()).await?;
                self.buf.fill(read);
                if read == 0 {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
            }
        }

        self.buf.drain(fixed_header_len);
        Ok(RawPacket {
            first_byte,
            rest: self.buf.split_to(remaining_length).freeze(),
        })
    }
}

impl<BP> std::fmt::Debug for Reader<BP>
where
    BP: BufferPool,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reader").finish_non_exhaustive()
    }
}
