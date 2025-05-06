# EpochFS

EpochFS is a versioned cloud file system with git-like branching, transaction support.

## Motivation

EpochFS is inspired by [delta Lake](https://github.com/delta-io/delta-rs/) and [lakeFS](https://github.com/treeverse/lakeFS), it is a data lake but for files, offering git-like branching, transaction support, and scaling to handle exabyte-level data effortlessly.

## Features in minds

In my minds, EpochFS will support the following features:

- [ ] A library that allows users to integrate and operate EpochFS directly within their applications.
- [ ] A FUSE implementation enabling users to mount EpochFS as a local file system.
- [ ] Support for both serverless mode and meta-server mode:
    - [ ] Serverless Mode: EpochFS stores all metadata within the storage.
    - [ ] Meta-Server Mode: EpochFS offloads all metadata to a separate server and commits changes to the storage layer, ensuring eventual consistency.
- [ ] Capability to seamlessly convert an existing bucket into EpochFS at no cost.
- [ ] Functionality to export and restore an EpochFS instance.
- [ ] Support for multiple storage backends, including S3, GCS, and Azure Blob Storage.

## Current Plan

The current plan is implement the basic functionalities of EpochFS and provide a library that allows users to integrate and operate EpochFS directly within their applications.

## License

Licensed under <a href="./LICENSE.md">Apache License, Version 2.0</a>.
