use anyhow::Result;
use opendal::Operator;
use sqlx::SqlitePool;

use crate::File;

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

        Ok(Fs { db, op })
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

        let new_file = File::new(self.db.clone(), self.op.clone(), path.to_string());

        Ok(new_file)
    }
}
