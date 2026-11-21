// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Pluggable `ReadOnlyVfsBackend` abstraction decoupling `NamespaceRouter` and
//! `WorkspaceOverlay` from Commit Cloud gRPC and local `jj` / Git object
//! stores.
//!
//! # Authority and Crash-Safety Ordering
//! `ReadOnlyVfsBackend` inspects only committed, content-addressed repository
//! state (`Commit`, `Tree`, `File`, `Symlink`, `View`, and `WorkspaceState`)
//! and populates `BlobCas` with clean blobs. All workspace mutations stay in
//! `WorkspaceOverlay` until snapshot publication.
//!
//! # Concurrency and Merge Invariants
//! Directory listings and `lookup_in_commit` return exact byte sizes from
//! `TreeEntry` metadata (Commit Cloud) or Git object headers (`gix`), avoiding
//! full-blob reads during `stat` or `ls -l`. Ranged file reads
//! (`read_file_range`) serve `[offset, offset+size)` directly from `BlobCas`
//! (`pread`) or ranged `ReadFile` gRPC streams.

pub mod commit_cloud;
pub mod local;

use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use futures::io::Cursor;

use crate::jj_error::JjError;
use crate::virtual_file::DirectoryEntry;
use crate::virtual_file::DirectoryStream;
use crate::virtual_file::FileAttributes;
use crate::virtual_file::FileType;
use crate::virtual_file::VirtualFile;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VfsWorkspaceRef {
    pub user: String,
    pub workspace_name: String,
    pub commit_id_hex: String,
    pub tree_id_hex: String,
    pub tree_ids_hex: Vec<String>,
    pub conflict_labels: Vec<String>,
    pub operation_id_hex: String,
    pub local_jj_dir: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct VfsLookup {
    pub attr: FileAttributes,
    pub object_id_hex: String,
    pub symlink_target: Option<PathBuf>,
}

#[async_trait]
pub trait ReadOnlyVfsBackend: Send + Sync {
    /// Human-readable repository identifier within its source.
    fn repo_name(&self) -> &str;

    /// Resolves a full commit ID or unambiguous hex prefix (`>= 8` hex chars)
    /// without enumerating the entire repository commit graph.
    async fn resolve_commit(&self, id_or_prefix: &str) -> Result<String, JjError>;

    /// Resolves a `z-k` or hex change ID/prefix to its single visible
    /// `commit_id` when non-divergent (`Ok(None)` if divergent across multiple
    /// visible heads).
    async fn resolve_change(&self, change_or_prefix: &str) -> Result<Option<String>, JjError>;

    /// Returns visible bookmarks (`bookmark_name -> commit_id_hex`), supporting
    /// nested `/` names.
    async fn list_bookmarks(&self) -> Result<BTreeMap<String, String>, JjError>;

    /// Returns active workspaces, optionally filtered to a single `user`.
    async fn list_workspaces(
        &self,
        user_filter: Option<&str>,
    ) -> Result<Vec<VfsWorkspaceRef>, JjError>;

    /// Returns commits currently pinned on this host (workspace checkouts,
    /// bookmarks, and recently resolved commits) for bounded `ls commits/`
    /// directory listings.
    async fn list_pinned_commits(&self) -> Result<Vec<String>, JjError>;

    /// Returns the root `TreeId` hex for `commit_id_hex`.
    async fn commit_root_tree_id(&self, commit_id_hex: &str) -> Result<String, JjError>;

    /// Resolves metadata (`FileAttributes`, object ID, optional symlink target)
    /// for `rel_path` inside `commit_id_hex` without streaming file bytes when
    /// metadata headers exist.
    async fn lookup_in_commit(
        &self,
        commit_id_hex: &str,
        rel_path: &Path,
    ) -> Result<VfsLookup, JjError>;

    /// Lists direct children of `dir_path` inside `commit_id_hex`.
    async fn list_dir_in_commit(
        &self,
        commit_id_hex: &str,
        dir_path: &Path,
    ) -> Result<Vec<DirectoryEntry>, JjError>;

    /// Reads `[offset, offset + size)` of `rel_path` inside `commit_id_hex`
    /// without discarding `[0, offset)` prefix bytes on every ranged FUSE read.
    async fn read_file_range(
        &self,
        commit_id_hex: &str,
        rel_path: &Path,
        offset: u64,
        size: u32,
    ) -> Result<Box<[u8]>, JjError>;

    /// Reads the target path of a symlink at `rel_path` inside `commit_id_hex`.
    async fn read_symlink(&self, commit_id_hex: &str, rel_path: &Path) -> Result<PathBuf, JjError>;

    /// Computes relative repo paths that differ between `old_tree_hex` and
    /// `new_tree_hex`, skipping identical subtrees in $O(\text{changed paths})$
    /// with zero blob reads.
    async fn diff_trees(
        &self,
        old_tree_hex: &str,
        new_tree_hex: &str,
    ) -> Result<Vec<String>, JjError>;
}

/// Bridges any `ReadOnlyVfsBackend` commit subtree into the `VirtualFile`
/// trait.
pub struct BackendCommitNode {
    backend: Arc<dyn ReadOnlyVfsBackend>,
    commit_id_hex: String,
    rel_path: PathBuf,
}

impl BackendCommitNode {
    pub fn new(
        backend: Arc<dyn ReadOnlyVfsBackend>,
        commit_id_hex: impl Into<String>,
        rel_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            backend,
            commit_id_hex: commit_id_hex.into(),
            rel_path: rel_path.into(),
        }
    }
}

#[async_trait]
impl VirtualFile for BackendCommitNode {
    async fn read(&self) -> Result<Pin<Box<dyn futures::AsyncRead + Send>>, JjError> {
        let info = self
            .backend
            .lookup_in_commit(&self.commit_id_hex, &self.rel_path)
            .await?;
        if info.attr.file_type != FileType::File {
            return Err(JjError::NotAFile);
        }
        let len = info.attr.size.min(u32::MAX as u64) as u32;
        let bytes = self
            .backend
            .read_file_range(&self.commit_id_hex, &self.rel_path, 0, len)
            .await?;
        Ok(Box::pin(Cursor::new(bytes.into_vec())))
    }

    async fn read_range(&self, offset: u64, size: u32) -> Result<Box<[u8]>, JjError> {
        self.backend
            .read_file_range(&self.commit_id_hex, &self.rel_path, offset, size)
            .await
    }

    async fn list(&self) -> Result<DirectoryStream, JjError> {
        let entries = self
            .backend
            .list_dir_in_commit(&self.commit_id_hex, &self.rel_path)
            .await?;
        Ok(Box::pin(futures::stream::iter(entries)))
    }

    async fn read_link(&self) -> Result<PathBuf, JjError> {
        self.backend
            .read_symlink(&self.commit_id_hex, &self.rel_path)
            .await
    }

    async fn attributes(&self) -> Result<FileAttributes, JjError> {
        Ok(self
            .backend
            .lookup_in_commit(&self.commit_id_hex, &self.rel_path)
            .await?
            .attr)
    }

    async fn file_type(&self) -> Result<FileType, JjError> {
        Ok(self.attributes().await?.file_type)
    }
}

/// Virtual symlink node returning a static target `PathBuf` (used for `me`,
/// `changes/`, `bookmarks/`, and `users/<user>/<repo>/<ws>` views).
pub struct VirtualSymlinkNode {
    target: PathBuf,
    modified: SystemTime,
}

impl VirtualSymlinkNode {
    pub fn new(target: impl Into<PathBuf>) -> Self {
        Self {
            target: target.into(),
            modified: SystemTime::UNIX_EPOCH,
        }
    }
}

#[async_trait]
impl VirtualFile for VirtualSymlinkNode {
    async fn read_link(&self) -> Result<PathBuf, JjError> {
        Ok(self.target.clone())
    }

    async fn attributes(&self) -> Result<FileAttributes, JjError> {
        Ok(FileAttributes::symlink(
            self.target.as_os_str().len() as u64,
            self.modified,
        ))
    }

    async fn file_type(&self) -> Result<FileType, JjError> {
        Ok(FileType::Symlink)
    }
}

/// Returns `data[offset..offset + size]`, clamped to the end of `data`.
pub(crate) fn slice_range(data: &[u8], offset: u64, size: u32) -> Box<[u8]> {
    let start = usize::try_from(offset)
        .unwrap_or(usize::MAX)
        .min(data.len());
    let end = start.saturating_add(size as usize).min(data.len());
    data[start..end].into()
}

/// Decodes a jj reverse-hex (`z-k`) change ID prefix into lowercase `0-9a-f`
/// hex.
pub fn normalize_change_prefix_to_hex(input: &str) -> Option<String> {
    let trimmed = input.trim().to_ascii_lowercase();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.chars().all(|c| matches!(c, 'k'..='z')) {
        let mapped: String = trimmed
            .bytes()
            .map(|b| {
                let nibble = b'z' - b;
                char::from_digit(u32::from(nibble), 16).unwrap()
            })
            .collect();
        return Some(mapped);
    }
    if trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
        return Some(trimmed);
    }
    None
}

/// Encodes lowercase `0-9a-f` hex into jj's `z-k` reverse-hex display alphabet.
pub fn encode_hex_to_reverse_hex(hex_str: &str) -> Option<String> {
    let mut out = String::with_capacity(hex_str.len());
    for ch in hex_str.chars() {
        let digit = ch.to_digit(16)? as u8;
        out.push(char::from(b'z' - digit));
    }
    Some(out)
}
