use crate::specs::v1 as specs_v1;
use crate::{file::FileWriter, File};
use anyhow::anyhow;
use anyhow::Result;
use base64::Engine as _;
use chrono::Utc;
use opendal::{Buffer, ErrorKind, Operator};
use std::{collections::BTreeMap, sync::Arc};

pub struct FsContext {
    op: Operator,
    metadata_path: String,
    data_path: String,
    version: usize,

    previous_etag: String,
}

impl FsContext {
    /// Write a chunk to the file system.
    ///
    /// The chunk id is a hash of input data, and is used to identify
    /// the chunk in the storage.
    pub async fn write_chunk(&self, buf: Buffer) -> Result<String> {
        let chunk_id = chunk_id(buf.clone());
        let chunk_path = format!("{}/{}", self.data_path, &chunk_id);
        self.op.write(&chunk_path, buf).await?;
        Ok(chunk_id)
    }
}

pub struct Fs {
    ctx: Arc<FsContext>,
    files: BTreeMap<String, File>,
}

impl Fs {
    pub async fn create(op: Operator) -> Result<Self> {
        let previous_etag = match op.stat("metadata").await {
            Ok(stat) => stat
                .etag()
                .ok_or_else(|| {
                    anyhow!("input storage services doesn't have etag: {:?}", op.info())
                })?
                .to_string(),
            Err(err) if err.kind() == ErrorKind::NotFound => "*".to_string(),
            Err(err) => return Err(err.into()),
        };

        let ctx = Arc::new(FsContext {
            op,
            metadata_path: "metadata".to_string(),
            data_path: "data".to_string(),
            version: 0,
            previous_etag,
        });

        let fs = Self {
            ctx,
            files: BTreeMap::new(),
        };

        Ok(fs)
    }

    /// Create a new file writer
    pub fn new_file_writer(&self, path: &str) -> FileWriter {
        FileWriter::new(self.ctx.clone(), path.to_string())
    }

    pub fn insert_file(&mut self, file: File) {
        self.files.insert(file.path().to_string(), file);
    }

    /// Wirte the manifest to the file system.
    ///
    /// Returning the chunk id of the manifest.
    pub async fn write_manifest(&self) -> Result<String> {
        let manifest = specs_v1::Manifest {
            files: self
                .files
                .clone()
                .into_values()
                .map(File::into_specs_v1)
                .collect(),
        };
        let manifest_content: Buffer =
            bincode::encode_to_vec(manifest, bincode::config::standard())?.into();
        let chunk_id = self.ctx.write_chunk(manifest_content).await?;
        Ok(chunk_id)
    }

    /// TODO: we should support automatically merge.
    pub async fn write_metadata(&self, manifest_path: &str) -> Result<()> {
        let metadata = specs_v1::Metadata {
            version: self.ctx.version,
            manifest: manifest_path.to_string(),
            last_modified: Utc::now().timestamp() as u64,
        };
        let metadata_content: Buffer =
            bincode::encode_to_vec(metadata, bincode::config::standard())?.into();
        self.ctx
            .op
            .write_with(&self.ctx.metadata_path, metadata_content)
            .if_match(&self.ctx.previous_etag)
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
