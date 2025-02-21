use std::mem;

use anyhow::Result;
use base64::Engine as _;
use futures::{Stream, StreamExt, TryStreamExt};
use opendal::{Buffer, Operator};
use prost::Message;
use sqlx::{QueryBuilder, SqlitePool};
use uuid::{NoContext, Timestamp};

use crate::{
    specs::{self, Checkpoint, FileChunks},
    File,
};

/// Fs is the main entry point for the epoch filesystem.
#[derive(Debug, Clone)]
pub struct Fs {
    db: SqlitePool,
    op: Operator,

    log_path: String,
    data_path: String,
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

        Ok(Fs {
            db,
            op,
            data_path: "data/".to_string(),
            log_path: "logs/".to_string(),
        })
    }

    /// write a chunk to the storage and return the chunk id.
    pub(crate) async fn write_chunk(&self, bs: Buffer) -> Result<String> {
        let chunk_id = chunk_id(bs.clone());

        let chunk_path = format!("{}/{}", &self.data_path, chunk_id);
        if !self.op.exists(&chunk_path).await? {
            self.op.write(&chunk_path, bs).await?;
        }

        Ok(chunk_id)
    }

    /// Read a chunk from the storage.
    pub(crate) async fn read_chunk(&self, chunk_id: &str) -> Result<Buffer> {
        let chunk_path = format!("{}/{}", &self.data_path, chunk_id);
        Ok(self.op.read(&chunk_path).await?)
    }

    /// Save the checkpoint to the storage.
    pub(crate) async fn save_checkpoint(&self, chunk_ids: Vec<String>) -> Result<String> {
        let checkpoint = Checkpoint {
            chunks: Some(FileChunks { ids: chunk_ids }),
        };
        let bs = checkpoint.encode_to_vec();
        let checkpoint_name = uuid::Uuid::new_v7(Timestamp::now(NoContext)).to_string();
        let checkpoint_path = format!("{}/{}.checkpoint", &self.log_path, checkpoint_name);
        self.op.write(&checkpoint_path, bs).await?;
        Ok(checkpoint_name)
    }

    /// Read the checkpoint from the storage.
    pub(crate) async fn read_checkpoint(&self, checkpoint: &str) -> Result<Buffer> {
        let checkpoint_path = format!("{}/{}.checkpoint", &self.log_path, checkpoint);
        let bs = self.op.read(&checkpoint_path).await?;
        Ok(bs)
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
            return Err(anyhow::anyhow!("file {path} already exists"));
        }

        let new_file = File::new(self.clone(), path.to_string());

        Ok(new_file)
    }

    /// Open a file for reading.
    pub async fn open_file(&self, path: &str) -> Result<Option<File>> {
        let Some(record) = sqlx::query!(
            r"
                SELECT * FROM files WHERE path = ?
            ",
            path
        )
        .fetch_optional(&self.db)
        .await?
        else {
            return Ok(None);
        };
        let file = File::with_chunks(
            self.clone(),
            path.to_string(),
            FileChunks::decode(record.chunks.as_slice())?.ids,
        );
        log::debug!(
            "open file {} with {} chunks loaded",
            path,
            file.chunks.len()
        );

        Ok(Some(file))
    }

    /// Check if the file exists in the filesystem.
    pub async fn check_file(&self, path: &str) -> Result<bool> {
        let file = sqlx::query!(
            r"
                SELECT path FROM files WHERE path = ?
            ",
            path
        )
        .fetch_optional(&self.db)
        .await?;

        Ok(file.is_some())
    }

    /// Commit the file to the database.
    pub(crate) async fn commit_file(&self, path: &str, chunk_ids: Vec<String>) -> Result<()> {
        log::debug!("commit file {} with {} chunks saved", path, chunk_ids.len());

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

    /// List all files in the filesystem.
    pub fn list_files(&self) -> impl Stream<Item = Result<File>> + use<'_> {
        let fs = self.clone();

        sqlx::query!("SELECT * FROM files")
            .fetch(&self.db)
            .map(move |record| {
                let record = record?;
                let file = File::with_chunks(
                    fs.clone(),
                    record.path,
                    FileChunks::decode(record.chunks.as_slice())?.ids,
                );
                Ok(file)
            })
    }

    /// Load the filesystem from storage.
    pub async fn load(&self, checkpoint: &str) -> Result<()> {
        let bs = self.read_checkpoint(checkpoint).await?;
        let checkpoint = Checkpoint::decode(bs)?;
        let chunk_ids = checkpoint.chunks.unwrap_or_default().ids;

        for chunk_id in chunk_ids {
            let chunk = self.read_chunk(&chunk_id).await?;
            let files = specs::Files::decode(chunk)?;

            let files = files
                .files
                .into_iter()
                .map(|file| (file.path, file.chunks.unwrap_or_default().encode_to_vec()))
                .collect::<Vec<_>>();

            let mut query_builder = QueryBuilder::new("INSERT INTO files (path, chunks) ");

            query_builder.push_values(files, |mut b, (path, chunks)| {
                b.push_bind(path).push_bind(chunks);
            });

            let query = query_builder.build();

            query.execute(&self.db).await?;
        }

        Ok(())
    }

    /// Commit the filesystem to storage.
    ///
    /// The commit will be saved as a checkpoint in storage.
    /// The checkpoint consists of multiple files, each containing a
    /// batch of files.
    pub async fn commit(&self) -> Result<String> {
        let mut file_stream = self.list_files();

        let mut chunk_ids = Vec::with_capacity(16);
        let mut size = 0;
        let mut files = Vec::with_capacity(10000);

        while let Some(record) = file_stream.try_next().await? {
            let (path, chunks) = record.into_parts();
            let file = specs::File {
                path,
                chunks: Some(FileChunks { ids: chunks }),
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
            let chunk_id = self.write_chunk(Buffer::from(bs)).await?;
            chunk_ids.push(chunk_id);
        }

        if !files.is_empty() {
            let files = specs::Files { files };
            let bs = files.encode_to_vec();
            let chunk_id = self.write_chunk(Buffer::from(bs)).await?;
            chunk_ids.push(chunk_id);
        }

        self.save_checkpoint(chunk_ids).await
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
    use anyhow::Result;
    use futures::stream;
    use opendal::{services::MemoryConfig, Buffer};
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn test_chunk_id() {
        let bs = Buffer::from("hello world");
        let id = chunk_id(bs);
        assert_eq!(id, "10mB76cKDIgLjYwZhdB128v2ebmaX5kU5ar5a4ManiQ");
    }

    #[tokio::test]
    async fn test_upload_chunk() -> Result<()> {
        let op = Operator::from_config(MemoryConfig::default())?.finish();
        let fs = Fs::new(op).await?;

        let source = Buffer::from("hello world");
        let id = fs.write_chunk(source.clone()).await.unwrap();

        let actual = fs.read_chunk(&id).await?;
        assert_eq!(source.to_vec(), actual.to_vec());
        Ok(())
    }

    #[tokio::test]
    async fn test_create_file() -> Result<()> {
        let op = Operator::from_config(MemoryConfig::default())?.finish();
        let fs = Fs::new(op).await?;

        let source = Buffer::from("hello world");
        let id = chunk_id(source.clone());

        let mut file = fs.create_file("hello.txt").await?;
        file.write(source.clone()).await?;
        file.commit().await?;

        let actual = fs.read_chunk(&id).await?;
        assert_eq!(source.to_vec(), actual.to_vec());
        Ok(())
    }

    #[tokio::test]
    async fn test_create_file_with_sink() -> Result<()> {
        let op = Operator::from_config(MemoryConfig::default())?.finish();
        let fs = Fs::new(op).await?;

        let source = Buffer::from("hello world");
        let id = chunk_id(source.clone());

        let mut file = fs.create_file("hello.txt").await?;
        file.sink(stream::iter([Ok(source.clone())])).await?;
        file.commit().await?;

        let actual = fs.read_chunk(&id).await?;
        assert_eq!(source.to_vec(), actual.to_vec());
        Ok(())
    }

    #[tokio::test]
    async fn test_create_file_with_large_sink() -> Result<()> {
        let op = Operator::from_config(MemoryConfig::default())?.finish();
        let fs = Fs::new(op).await?;

        let source = Buffer::from(vec![6u8; 3 * 1024 * 1024]);
        let id_8mib = chunk_id(Buffer::from(vec![6; 8 * 1024 * 1024]));
        let id_1mib = chunk_id(Buffer::from(vec![6; 1024 * 1024]));

        let mut file = fs.create_file("hello.txt").await?;
        // We are writing 3 * 3MiB of data, so we should have 2 chunks:
        // the first is 8MiB, the second is 1MiB.
        file.sink(stream::iter([
            Ok(source.clone()),
            Ok(source.clone()),
            Ok(source.clone()),
        ]))
        .await?;
        file.commit().await?;

        let actual_8mib = fs.read_chunk(&id_8mib).await?;
        assert_eq!(vec![6; 8 * 1024 * 1024].to_vec(), actual_8mib.to_vec());

        let actual_1mib = fs.read_chunk(&id_1mib).await?;
        assert_eq!(vec![6; 1024 * 1024].to_vec(), actual_1mib.to_vec());
        Ok(())
    }

    #[tokio::test]
    async fn test_open_file() -> Result<()> {
        let op = Operator::from_config(MemoryConfig::default())?.finish();
        let fs = Fs::new(op).await?;

        let source = Buffer::from("hello world");

        let mut file = fs.create_file("hello.txt").await?;
        file.write(source.clone()).await?;
        file.commit().await?;

        let file = fs.open_file("hello.txt").await?.expect("file must exist");

        let actual = file.read().await?;
        assert_eq!(source.to_vec(), actual.to_vec());
        Ok(())
    }

    #[tokio::test]
    async fn test_list_files() -> Result<()> {
        let op = Operator::from_config(MemoryConfig::default())?.finish();
        let fs = Fs::new(op).await?;

        let source = Buffer::from("hello world");
        let id = chunk_id(source.clone());

        let mut file = fs.create_file("hello.txt").await?;
        file.write(source.clone()).await?;
        file.commit().await?;

        let files: Vec<File> = fs.list_files().try_collect::<Vec<_>>().await?;
        assert_eq!(files.len(), 1);
        let (path, chunks) = files[0].clone().into_parts();
        assert_eq!(path, "hello.txt");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], id);

        let actual = fs.read_chunk(&id).await?;
        assert_eq!(source.to_vec(), actual.to_vec());

        let actual = files[0].read().await?;
        assert_eq!(source.to_vec(), actual.to_vec());
        Ok(())
    }

    #[tokio::test]
    async fn test_save_checkpoint() -> Result<()> {
        let op = Operator::from_config(MemoryConfig::default())?.finish();
        let fs = Fs::new(op).await?;

        let source = Buffer::from("hello world");

        let mut file = fs.create_file("hello.txt").await?;
        file.write(source.clone()).await?;
        file.commit().await?;

        let checkpoint_name = fs.commit().await?;

        let bs = fs.read_checkpoint(&checkpoint_name).await?;
        let checkpoint = Checkpoint::decode(bs)?;
        assert_eq!(checkpoint.chunks.unwrap().ids.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_load_checkpoint() -> Result<()> {
        let op = Operator::from_config(MemoryConfig::default())?.finish();
        let fs = Fs::new(op.clone()).await?;

        let source = Buffer::from("hello world");

        let mut file = fs.create_file("hello.txt").await?;
        file.write(source.clone()).await?;
        file.commit().await?;

        let checkpoint_name = fs.commit().await?;

        // Create a fs for loading test.
        let fs = Fs::new(op).await?;
        fs.load(&checkpoint_name).await?;

        assert!(fs.check_file("hello.txt").await?);
        Ok(())
    }
}
