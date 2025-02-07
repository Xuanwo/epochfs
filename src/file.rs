use anyhow::Result;
use opendal::Buffer;

use crate::Fs;

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

    /// Commit the file to the database.
    pub async fn commit(&mut self) -> Result<()> {
        self.fs.commit_file(&self.path, self.chunks.clone()).await?;
        Ok(())
    }
}
