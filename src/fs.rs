use anyhow::Result;
use opendal::Operator;
use prost::Message;
use sqlx::SqlitePool;

/// Fs is the main entry point for the epoch filesystem.
#[derive(Debug, Clone)]
pub struct Fs {
    db: SqlitePool,
    _op: Operator,
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

        sqlx::query!(
            r#"
            CREATE TABLE IF NOT EXISTS file_chunks (
                id TEXT PRIMARY KEY NOT NULL,
                size INTEGER NOT NULL
            )
            "#,
        )
        .execute(&db)
        .await?;

        Ok(Fs { db, _op: op })
    }

    /// Add a file to the filesystem.
    ///
    /// This function will add file record in the database. The chunk must
    /// have already been uploaded to the storage.
    pub async fn commit_file_in_db(&self, path: String, chunk_ids: Vec<String>) -> Result<()> {
        let chunk_ids = super::specs::FileChunkIds { ids: chunk_ids };
        let chunk_id_content = chunk_ids.encode_to_vec();

        sqlx::query!(
            r#"INSERT INTO files (path, chunks) VALUES (?, ?)"#,
            path,
            chunk_id_content
        )
        .execute(&self.db)
        .await?;

        Ok(())
    }
}
