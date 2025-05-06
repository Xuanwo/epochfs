use std::{mem, sync::Arc};

use crate::{fs::FsContext, specs::v1 as specs_v1};
use anyhow::Result;
use bytes::Buf as _;
use chrono::{DateTime, Utc};
use opendal::Buffer;

/// Use 8MiB as the default chunk size.
const DEFAULT_CHUNK_SIZE: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct File {
    path: String,
    chunks: Vec<String>,

    size: u64,
    last_modified: DateTime<Utc>,
}

impl File {
    pub fn path(&self) -> &str {
        &self.path
    }
}

impl From<specs_v1::File> for File {
    fn from(value: specs_v1::File) -> Self {
        Self {
            path: value.path,
            chunks: value.chunks,
            size: value.size,
            last_modified: DateTime::from_timestamp(value.last_modified as i64, 0).unwrap(),
        }
    }
}

impl From<File> for specs_v1::File {
    fn from(value: File) -> specs_v1::File {
        specs_v1::File {
            path: value.path,
            chunks: value.chunks,
            size: value.size,
            last_modified: value.last_modified.timestamp() as u64,
        }
    }
}

pub struct FileWriter {
    ctx: Arc<FsContext>,
    path: String,

    total_size: u64,
    chunks: Vec<String>,

    buf_size: usize,
    buf: Vec<Buffer>,
}

impl FileWriter {
    pub fn new(ctx: Arc<FsContext>, path: String) -> Self {
        Self {
            ctx,
            path,

            total_size: 0,
            chunks: vec![],
            buf_size: 0,
            buf: vec![],
        }
    }

    pub async fn write(&mut self, buf: Buffer) -> Result<()> {
        self.buf_size += buf.len();
        self.buf.push(buf);

        if self.buf_size >= DEFAULT_CHUNK_SIZE {
            self.flush(false).await?;
        }

        Ok(())
    }

    pub async fn close(&mut self) -> Result<File> {
        self.flush(true).await?;
        Ok(File {
            path: self.path.clone(),
            chunks: mem::take(&mut self.chunks),
            size: self.total_size,
            last_modified: Utc::now(),
        })
    }

    /// Flush the buffer to the file system.
    ///
    /// If `finish` is true, it means that this is the last flush,
    /// it will flush all buffers no matter it's larger than chunk_size or not.
    async fn flush(&mut self, finish: bool) -> Result<()> {
        let mut buf: Buffer = self.buf.drain(..).flatten().collect();

        while self.buf_size >= DEFAULT_CHUNK_SIZE {
            let to_write = buf.slice(..DEFAULT_CHUNK_SIZE);
            let chunk_id = self.ctx.write_chunk(to_write).await?;
            buf.advance(DEFAULT_CHUNK_SIZE);
            self.buf_size -= DEFAULT_CHUNK_SIZE;
            self.total_size += DEFAULT_CHUNK_SIZE as u64;
            self.chunks.push(chunk_id);
        }

        if self.buf_size == 0 {
            return Ok(());
        }

        if finish {
            let chunk_id = self.ctx.write_chunk(buf).await?;
            self.total_size += self.buf_size as u64;
            self.buf_size = 0;
            self.chunks.push(chunk_id);
        } else {
            self.buf.push(buf);
        }

        Ok(())
    }
}
