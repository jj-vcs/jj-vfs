// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Core virtual filesystem (`jjfsd`) library mounting remote `jj-commit-cloud`
//! servers and local `jj` / Git repositories via Linux FUSE.
//!
//! # Authority and Crash-Safety Ordering
//! Clean repository objects come from `ReadOnlyVfsBackend` sources and populate
//! a content-addressed local disk cache (`cas/`), while workspace mutations
//! write into per-workspace durable overlays (`overlays/<ws_id>/staged/` +
//! `journal.log`) *before* returning from FUSE `write()` or `fsync()`.
//! Background or CLI-triggered snapshots upload dirty blobs before publishing
//! updated `Commit` and `Operation` metadata.
//!
//! # Concurrency and Merge Invariants
//! Multiple active FUSE mounts (Root Namespace Mount and Scoped Direct Mounts)
//! share one sharded `InodeMap`, one local blob CAS, and one `NamespaceRouter`
//! inside a single per-user `jjfsd` Tokio runtime.
//!
//! # Cross-Process and RPC Boundaries
//! Control plane commands (`source add`, `mount`, `SnapshotWorkspace`,
//! `CheckoutTree`) arrive over `$XDG_RUNTIME_DIR/jj-vfs/control.sock`, while
//! remote repository reads and snapshots multiplex over `cc-lib` HTTP/2 gRPC
//! channels (or `jj-cc-daemon` Unix domain sockets).

pub mod commit_tree_file;
pub mod commits_directory;
pub mod fuse;
pub mod inode_map;
pub mod jj_error;
pub mod path_mapper;
pub mod path_mapper_all_commits;
pub mod static_directory;
#[cfg(any(test, feature = "test-helpers"))]
pub mod test_helpers;
pub mod vfs;
pub mod virtual_file;
pub mod workspaces_directory;
