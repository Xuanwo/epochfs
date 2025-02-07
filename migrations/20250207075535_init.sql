-- Add migration script here
CREATE TABLE files (
    path TEXT PRIMARY KEY NOT NULL,
    chunks BLOB NOT NULL
);
