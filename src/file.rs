/// File represents a file in the epochfs.
pub struct File {
    db: SqlitePool,
    op: Operator,

    path: String,
    chunk_ids: Vec<String>,
}

impl File {}
