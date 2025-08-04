// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::{collections::VecDeque, fs::File, io::IoSlice, sync::Arc};

use crate::{BytesAccumulator, Error, Owned, Shared, ToIovecs};

#[derive(Debug)]
pub struct BytesAccumulatorImpl<O>
where
    O: Owned,
{
    owned: O,
    chunks: VecDeque<Chunk<O::Shared>>,
    /// Total number of bytes in `self.chunks`.
    total_size: usize,
}

#[derive(Debug)]
enum Chunk<S> {
    Shared(S),
    File {
        f: Arc<File>,
        offset: u64,
        len: usize,
    },
}

impl<O> BytesAccumulatorImpl<O>
where
    O: Owned,
{
    pub fn new(owned: O) -> Self {
        BytesAccumulatorImpl {
            owned,
            chunks: Default::default(),
            total_size: 0,
        }
    }
}

impl<O> BytesAccumulator for BytesAccumulatorImpl<O>
where
    O: Owned,
{
    type Shared = O::Shared;

    fn can_accept_more(&self) -> bool {
        // TODO: More structured way of calculating this.
        // This depends on the number of iovecs the Writer uses, but knowing that here is a layering violation.
        // Also, this length includes file chunks which the Writer does not write via iovecs.
        // Also, the `- 10` is because we want the last packet that gets encoded to not push `self.chunks.len()`
        // above the number of iovecs the Writer uses, but knowing how many Shared's the packet will need is also
        // a layering violation.
        self.chunks.len() < 128 - 10
    }

    fn reserve(&mut self, additional: usize) -> Result<(), Error> {
        self.owned.reserve(additional)
    }

    fn put_shared(&mut self, src: Self::Shared) {
        // An empty Shared will manifest as an empty iovec in the Writer, which is not only wasteful but also
        // lead to the Writer issuing 0-byte writes and interpreting the resulting 0 from writev as an EOF.
        // So ignore empty Shared's.
        if src.is_empty() {
            return;
        }

        self.put_done();
        self.total_size += src.len();
        self.chunks.push_back(Chunk::Shared(src));
    }

    fn put_file_chunk(&mut self, f: Arc<File>, offset: u64, len: usize) {
        // Writing a chunk requires flushing all previous writes first, which is wasteful if this chunk is empty.
        // So ignore empty chunks.
        if len == 0 {
            return;
        }

        self.put_done();
        self.total_size += len;
        self.chunks.push_back(Chunk::File { f, offset, len });
    }

    fn try_put_slice(&mut self, src: &[u8]) -> Option<()> {
        let dst = self.owned.unfilled_mut();
        if dst.len() >= src.len() {
            let dst = dst.get_mut(..src.len())?;
            crate::maybe_uninit_copy_from_slice(dst, src);
            self.owned.fill(src.len());
            Some(())
        } else {
            None
        }
    }

    fn put_done(&mut self) {
        if !self.owned.filled_is_empty() {
            let shared = self.owned.split_to(self.owned.filled_len()).freeze();
            self.total_size += shared.len();
            self.chunks.push_back(Chunk::Shared(shared));
        }
    }

    fn to_iovecs<'a>(&'a self, iovecs: &mut [IoSlice<'a>]) -> ToIovecs {
        assert!(self.owned.filled_is_empty());

        let mut num_iovecs = 0;
        let mut total_len = 0;
        for (iovec, chunk) in iovecs.iter_mut().zip(&self.chunks) {
            match chunk {
                Chunk::Shared(shared) => {
                    *iovec = IoSlice::new(shared.as_ref());
                    num_iovecs += 1;
                    total_len += shared.len();
                }

                Chunk::File { f, offset, len } => {
                    if num_iovecs == 0 {
                        return ToIovecs::FileChunk {
                            f: f.clone(),
                            offset: *offset,
                            len: *len,
                        };
                    }

                    break;
                }
            }
        }
        ToIovecs::Iovecs {
            num_iovecs,
            total_len,
        }
    }

    fn drain(&mut self, n: usize) {
        // We observed that most of the time the vectored write in `Writer` is able to
        // write all the iovecs we gave it, so we have a fast path for clearing the queue of Shared's
        // for that case. The slow path handles the case of partial writes by dropping
        // only those Shared's that were completely written and partially draining
        // the one that wasn't.

        #[cold]
        fn drain_slow<O>(this: &mut BytesAccumulatorImpl<O>, mut n: usize)
        where
            O: Owned,
        {
            while let Some(chunk) = this.chunks.pop_front() {
                match chunk {
                    Chunk::Shared(mut shared) => {
                        this.total_size -= shared.as_ref().len();

                        if let Some(n_) = n.checked_sub(shared.as_ref().len()) {
                            n = n_;
                        } else {
                            shared.drain(n);
                            n = 0;
                            this.total_size += shared.as_ref().len();
                            this.chunks.push_front(Chunk::Shared(shared));
                            break;
                        }
                    }

                    Chunk::File {
                        f,
                        mut offset,
                        mut len,
                    } => {
                        this.total_size -= len;

                        if let Some(n_) = n.checked_sub(len) {
                            n = n_;
                        } else {
                            offset += u64::try_from(n).expect("usize -> u64");
                            len -= n;
                            n = 0;
                            this.total_size += len;
                            this.chunks.push_front(Chunk::File { f, offset, len });
                            break;
                        }
                    }
                }
            }
            assert_eq!(n, 0);
        }

        if self.total_size == n {
            self.total_size = 0;
            self.chunks.clear();
            return;
        }

        drain_slow(self, n);
    }

    fn split(&mut self) -> Self {
        self.put_done();

        let chunks = std::mem::take(&mut self.chunks);
        let total_size = std::mem::replace(&mut self.total_size, 0);
        BytesAccumulatorImpl {
            total_size,
            owned: self.owned.split_to(0),
            chunks,
        }
    }

    fn is_empty(&self) -> bool {
        self.owned.filled_is_empty() && self.chunks.iter().all(Chunk::is_empty)
    }
}

impl<S> Chunk<S>
where
    S: Shared,
{
    fn is_empty(&self) -> bool {
        match self {
            Self::Shared(shared) => shared.is_empty(),
            Self::File { len, .. } => *len == 0,
        }
    }
}

impl<S> PartialEq for Chunk<S>
where
    S: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Shared(shared1), Self::Shared(shared2)) => shared1 == shared2,

            // We don't want to actually read the file chunk to compare it.
            // Instead we use a softer definition of checking that it's the same Arc<File>
            // with the same chunk range.
            // This means it's possible for two different `File`s to the same file chunk
            // to compare unequal, but this is good enough for our use case.
            (
                Self::File {
                    f: f1,
                    offset: offset1,
                    len: len1,
                },
                Self::File {
                    f: f2,
                    offset: offset2,
                    len: len2,
                },
            ) => Arc::ptr_eq(f1, f2) && offset1 == offset2 && len1 == len2,

            _ => false,
        }
    }
}

impl<S> Eq for Chunk<S> where S: Eq {}
