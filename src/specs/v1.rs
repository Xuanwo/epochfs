use bincode::{Decode, Encode};

/// Metadata for the file system.
#[derive(Encode, Decode, PartialEq, Debug)]
pub struct Metadata {
    /// The version of the given fs.
    pub version: usize,

    /// The path to the latest snapshot.
    pub manifest: String,
    /// The last modified time of the latest snapshot.
    pub last_modified: u64,
}

/// A manifest of the file system.
#[derive(Encode, Decode, PartialEq, Debug)]
pub struct Manifest {
    pub files: Vec<File>,
}

/// A file in the file system.
#[derive(Encode, Decode, PartialEq, Debug)]
pub struct File {
    pub path: String,
    pub chunks: Vec<String>,

    pub size: u64,
    pub last_modified: u64,
}
