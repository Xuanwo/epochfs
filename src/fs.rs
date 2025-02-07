use anyhow::Result;
use opendal::Operator;
use sqlx::SqlitePool;

/// Fs is the main entry point for the epoch filesystem.
#[derive(Debug, Clone)]
pub struct Fs {
    _db: SqlitePool,
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

        Ok(Fs { _db: db, _op: op })
    }
}
