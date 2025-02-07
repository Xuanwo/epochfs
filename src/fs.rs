use std::mem;

use anyhow::Result;
use base64::Engine as _;
use chrono::Utc;
use futures::TryStreamExt;
use opendal::{Buffer, Operator};
use prost::Message;
use sqlx::SqlitePool;

use crate::{
    specs::{self, Checkpoint, FileChunks},
    File,
};

/// Fs is the main entry point for the epoch filesystem.
#[derive(Debug, Clone)]
pub struct Fs {
    db: SqlitePool,
    op: Operator,
}

impl Fs {
    /// Create a new filesystem instance.
    pub async fn new(op: Operator) -> Result<Self> {
        let db = SqlitePool::connect("sqlite://:memory:").await?;

        sqlx::query!(
            r#"
             CREATE TABLE IF NOT EXISTS files (
                path TEXT PRIMARY KEY NOT NULL,
                chunks BLOB NOT NULL
            )
            "#,
        )
        .execute(&db)
        .await?;

        Ok(Fs { db, op })
    }

    /// Upload a chunk to the storage and return the chunk id.
    pub(crate) async fn upload_chunk(&self, bs: Buffer) -> Result<String> {
        let chunk_id = chunk_id(bs.clone());

        if !self.op.exists(&chunk_id).await? {
            self.op.write(&chunk_id, bs).await?;
        }

        Ok(chunk_id)
    }

    /// Save the checkpoint to the storage.
    pub(crate) async fn save_checkpoint(&self, chunk_ids: Vec<String>) -> Result<()> {
        let checkpoint = Checkpoint {
            chunks: Some(FileChunks { ids: chunk_ids }),
        };
        let bs = checkpoint.encode_to_vec();
        // FIXME: we should use better checkpoint name.
        self.op
            .write(&Utc::now().timestamp().to_string(), bs)
            .await?;
        Ok(())
    }

    /// Create a new file.
    pub async fn create_file(&self, path: &str) -> Result<File> {
        let file = sqlx::query!(
            r"
                SELECT path FROM files WHERE path = ?
            ",
            path
        )
        .fetch_optional(&self.db)
        .await?;

        if file.is_some() {
            return Err(anyhow::anyhow!("file already exists"));
        }

        let new_file = File::new(self.clone(), path.to_string());

        Ok(new_file)
    }

    /// Commit the file to the database.
    pub async fn commit_file(&self, path: &str, chunk_ids: Vec<String>) -> Result<()> {
        let chunk_ids = super::specs::FileChunks { ids: chunk_ids };
        let chunk_id_content = chunk_ids.encode_to_vec();

        sqlx::query!(
            r#"
                INSERT INTO files (path, chunks) VALUES (?, ?)
            "#,
            path,
            chunk_id_content
        )
        .execute(&self.db)
        .await?;

        Ok(())
    }

    /// Commit the filesystem to storage.
    ///
    /// The commit will be saved as a checkpoint in storage.
    /// The checkpoint consists of multiple files, each containing a
    /// batch of files.
    pub async fn commit(&self) -> Result<()> {
        let mut file_stream = sqlx::query!("SELECT * FROM files").fetch(&self.db);

        let mut chunk_ids = Vec::with_capacity(16);
        let mut size = 0;
        let mut files = Vec::with_capacity(10000);

        while let Some(record) = file_stream.try_next().await? {
            let file = specs::File {
                path: record.path,
                chunks: Some(FileChunks::decode(record.chunks.as_slice())?),
            };
            size += file.encoded_len();
            files.push(file);

            // If the size is less than 8MiB, we can add more files in this chunk.
            if size < 8 * 1024 * 1024 {
                continue;
            }
            let files = specs::Files {
                files: mem::replace(&mut files, Vec::with_capacity(10000)),
            };
            let bs = files.encode_to_vec();
            let chunk_id = self.upload_chunk(Buffer::from(bs)).await?;
            chunk_ids.push(chunk_id);
        }

        if !files.is_empty() {
            let files = specs::Files { files };
            let bs = files.encode_to_vec();
            let chunk_id = self.upload_chunk(Buffer::from(bs)).await?;
            chunk_ids.push(chunk_id);
        }

        self.save_checkpoint(chunk_ids).await?;
        Ok(())
    }
}

/// Calculate the chunk id from the buffer.
///
/// The chunk id is the URL_SAFE_NO_PAD base64 of blake3 hash of the buffer content.
///
/// chund id is used to identify the chunk in the storage. If the chunk
/// id already exists, we can reuse the existing chunk instead of creating
/// a new one.
fn chunk_id(bs: Buffer) -> String {
    let mut hasher = blake3::Hasher::new();
    for b in bs {
        hasher.update(&b);
    }
    let result = hasher.finalize();
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(result.as_bytes())
}

#[cfg(test)]
mod tests {
    use opendal::Buffer;

    use super::*;

    #[test]
    fn test_chunk_id() {
        let bs = Buffer::from("hello world");
        let id = chunk_id(bs);
        assert_eq!(id, "10mB76cKDIgLjYwZhdB128v2ebmaX5kU5ar5a4ManiQ");
    }
}
