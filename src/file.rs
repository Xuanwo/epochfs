use anyhow::Result;
use opendal::Buffer;

use crate::Fs;

/// File represents a file in the epochfs.
pub struct File {
    fs: Fs,

    path: String,
    chunk_ids: Vec<String>,
}

impl File {
    /// Create a new file.
    pub(crate) fn new(fs: Fs, path: String) -> Self {
        Self {
            fs,
            path,
            chunk_ids: Vec::new(),
        }
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
        let chunk_id = self.fs.upload_chunk(bs).await?;
        self.chunk_ids.push(chunk_id);
        Ok(())
    }

    /// Commit the file to the database.
    pub async fn commit(&mut self) -> Result<()> {
        self.fs
            .commit_file(&self.path, self.chunk_ids.clone())
            .await?;
        Ok(())
    }
}
