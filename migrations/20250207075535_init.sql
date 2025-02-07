-- Add migration script here
CREATE TABLE files (
    path TEXT PRIMARY KEY NOT NULL,
    chunks BLOB NOT NULL
);
 CREATE TABLE file_chunks (
    id TEXT PRIMARY KEY NOT NULL,
    size INTEGER NOT NULL
);
