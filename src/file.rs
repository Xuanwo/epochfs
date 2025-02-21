use anyhow::Result;
use futures::{Stream, StreamExt, TryStreamExt};
use opendal::Buffer;
use std::mem;
use std::pin::pin;

use crate::Fs;

const DEFAULT_CHUNK_SIZE: usize = 8 * 1024 * 1024;

/// File represents a file in the epochfs.
#[derive(Debug, Clone)]
pub struct File {
    fs: Fs,

    path: String,
    chunks: Vec<String>,
}

impl File {
    /// Create a new file.
    pub(crate) fn new(fs: Fs, path: String) -> Self {
        Self::with_chunks(fs, path, Vec::new())
    }

    /// Create a new file.
    pub(crate) fn with_chunks(fs: Fs, path: String, chunks: Vec<String>) -> Self {
        Self { fs, path, chunks }
    }

    /// Get the path and chunks of the file.
    pub(crate) fn into_parts(self) -> (String, Vec<String>) {
        (self.path, self.chunks)
    }

    /// Get the path of the file.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Write given buffer to the file.
    ///
    /// This function will calculate the chunk id from the buffer and write
    /// the buffer to the storage. If the chunk id already exists, we can
    /// reuse the existing chunk instead of creating a new one.
    ///
    /// # TODO
    ///
    /// We can use if-not-exists to save an extra request.
    pub async fn write(&mut self, bs: Buffer) -> Result<()> {
        let chunk_id = self.fs.write_chunk(bs).await?;
        self.chunks.push(chunk_id);
        Ok(())
    }

    /// Write a stream of buffers to the file.
    ///
    /// sink will make sure that all chunks are aligned with 8MiB.
    pub async fn sink(&mut self, mut stream: impl Stream<Item = Result<Buffer>>) -> Result<()> {
        let mut stream = pin!(stream);

        let mut chunks = Vec::new();
        let mut size = 0;
        while let Some(bs) = stream.next().await {
            let bs = bs?;
            if size + bs.len() < DEFAULT_CHUNK_SIZE {
                size += bs.len();
                chunks.push(bs);
                continue;
            }

            let consume_size = DEFAULT_CHUNK_SIZE - size;
            // Push the last chunk.
            chunks.push(bs.slice(0..consume_size));
            self.fs
                .write_chunk(Buffer::from_iter(
                    mem::take(&mut chunks).into_iter().flatten(),
                ))
                .await?;

            if consume_size < bs.len() {
                chunks.push(bs.slice(consume_size..));
                size += bs.len() - consume_size;
            } else {
                size = 0
            }
        }
        if size > 0 {
            self.fs
                .write_chunk(Buffer::from_iter(
                    mem::take(&mut chunks).into_iter().flatten(),
                ))
                .await?;
        }
        Ok(())
    }

    /// Commit the file to the database.
    pub async fn commit(&mut self) -> Result<()> {
        self.fs.commit_file(&self.path, self.chunks.clone()).await?;
        Ok(())
    }

    /// Read given file into buffer.
    pub async fn read(&self) -> Result<Buffer> {
        let buffers: Vec<_> = self.stream().await?.try_collect().await?;
        Ok(Buffer::from_iter(buffers.into_iter().flatten()))
    }

    /// Stream the entire content file in buffers.
    pub async fn stream(&self) -> Result<impl Stream<Item = Result<Buffer>>> {
        let fs = self.fs.clone();
        let stream = futures::stream::iter(self.chunks.clone()).then(move |v| {
            let fs = fs.clone();
            async move { fs.read_chunk(&v).await }
        });
        Ok(stream)
    }
}
