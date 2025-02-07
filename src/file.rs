use anyhow::Result;
use base64::Engine as _;
use opendal::{Buffer, Operator};
use prost::Message;
use sqlx::SqlitePool;

/// File represents a file in the epochfs.
pub struct File {
    db: SqlitePool,
    op: Operator,

    path: String,
    chunk_ids: Vec<String>,
}

impl File {
    /// Create a new file.
    pub(crate) fn new(db: SqlitePool, op: Operator, path: String) -> Self {
        Self {
            db,
            op,
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
    /// It's possible that the file is exists in the storage. We can use if-not-exists to save an extra request.
    pub async fn write(&mut self, bs: Buffer) -> Result<()> {
        let chunk_id = chunk_id(bs.clone());

        let chunk = sqlx::query!(
            r#"
                SELECT id FROM file_chunks WHERE id = ?
            "#,
            chunk_id
        )
        .fetch_optional(&self.db)
        .await?;

        if chunk.is_none() {
            let size = bs.len() as i64;
            self.op.write(&chunk_id, bs).await?;
            sqlx::query!(
                r#"
                    INSERT INTO file_chunks (id, size) VALUES (?, ?)
                "#,
                chunk_id,
                size
            )
            .execute(&self.db)
            .await?;
        }

        self.chunk_ids.push(chunk_id);
        Ok(())
    }

    /// Commit the file to the database.
    pub async fn commit(&mut self) -> Result<()> {
        let chunk_ids = super::specs::FileChunkIds {
            ids: self.chunk_ids.clone(),
        };
        let chunk_id_content = chunk_ids.encode_to_vec();

        sqlx::query!(
            r#"
                INSERT INTO files (path, chunks) VALUES (?, ?)
            "#,
            self.path,
            chunk_id_content
        )
        .execute(&self.db)
        .await?;

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
