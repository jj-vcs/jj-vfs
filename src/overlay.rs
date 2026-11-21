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

//! Durable on-disk per-workspace write overlay (`WorkspaceOverlay` +
//! `OverlayManager`) with `jj_lib::gitignore::GitIgnoreFile` split and
//! crash-recoverable `journal.log`.
//!
//! # Authority and Crash-Safety Ordering
//! Completed FUSE `write()`, `create()`, `unlink()`, `rename()`, `symlink()`,
//! and `setattr()` calls persist tracked mutations into
//! `overlays/<ws_id>/staged/` and append a newline-delimited JSON record to
//! `overlays/<ws_id>/journal.log` *before* acknowledging the FUSE request;
//! `fsync()` makes both durable. Replay applies a file or symlink record only
//! if its staged entry exists, so `rename()` writes the destination
//! record, moves or streams the bytes into place (`overlay/tmp/` plus
//! `rename(2)`), and whiteouts the source last. Ignored paths (`target/`,
//! `*.o`, swap files) write directly into `overlays/<ws_id>/ignored/` as
//! ordinary POSIX files and never enter `journal.log` or Commit Cloud uploads.
//!
//! # Concurrency and Merge Invariants
//! Each workspace owns an independent `WorkspaceOverlay` guarded by a
//! `parking_lot::RwLock`. A tracked mutation holds a `MutationGuard` from
//! before it touches `staged/` until its record is applied, and
//! `complete_snapshot` refuses while a guard is live or `dirty_seq`
//! moved, so it never deletes bytes that the snapshot did not upload. A
//! directory rename moves staged and ignored content; a directory that still
//! holds base-commit files returns `EXDEV` (as overlayfs does without
//! `redirect_dir`) so `mv` copies and deletes. A local-jj source checkout's own
//! `.jj` is exposed read-only; synthesized `.jj` dirs are writable. Staged
//! files live outside `$XDG_CACHE_HOME/jj-vfs/cas/` and are never evicted.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use dashmap::DashMap;
use futures::io::Cursor;
use jj_lib::gitignore::GitIgnoreFile;
use jj_lib::repo_path::RepoPath;
use jj_lib::repo_path::RepoPathBuf;
use parking_lot::RwLock;
use serde::Deserialize;
use serde::Serialize;
use vfs_proto::WorkspaceKey;

use crate::backend::ReadOnlyVfsBackend;
use crate::jj_error::JjError;
use crate::virtual_file::AttributeChanges;
use crate::virtual_file::DirectoryEntry;
use crate::virtual_file::DirectoryStream;
use crate::virtual_file::FileAttributes;
use crate::virtual_file::FileType;
use crate::virtual_file::VirtualFile;

/// Chunk size for streaming base-commit blobs into `overlay/tmp/` (hydration
/// and rename).
const COPY_CHUNK_BYTES: u32 = 1 << 20;

/// Maximum cached `.gitignore` directory entries per workspace before eviction.
const MAX_GITIGNORE_CACHE_ENTRIES: usize = 4096;

/// Makes `overlay/tmp/` names unique within this process.
static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JournalOp {
    Added,
    Modified,
    Deleted,
    Mkdir,
    Rmdir,
    Symlink,
    Chmod,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalRecord {
    pub op: JournalOp,
    pub rel_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    pub timestamp_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DirtyEntryState {
    File {
        staged_path: PathBuf,
        executable: bool,
    },
    Symlink {
        target: PathBuf,
    },
    Deleted,
}

#[derive(Default)]
struct OverlayMutableState {
    /// Tracked paths deleted relative to the base commit tree (whiteouts).
    deleted: BTreeSet<String>,
    /// Tracked files or symlinks currently materialized in `staged/`.
    dirty_files: BTreeMap<String, bool>,
    /// Tracked symlinks currently materialized in `staged/`.
    dirty_symlinks: BTreeMap<String, PathBuf>,
    /// Tracked directories explicitly created in `staged/`.
    dirty_dirs: BTreeSet<String>,
    /// Monotonic sequence incremented on every tracked mutation.
    dirty_seq: u64,
    /// Tracked mutations between `begin_mutation` and the drop of their
    /// `MutationGuard`.
    mutations_in_flight: u64,
    /// Wall-clock instant of most recent tracked mutation (for debounce).
    last_mutation: Option<Instant>,
}

/// Host directory served at `<workspace>/.jj`.
#[derive(Clone)]
struct JjDirBinding {
    host_root: PathBuf,
    /// The source checkout's own `.jj` (local-jj): jj in the mount must not
    /// rewrite its `working_copy/` or repo, so every mutation returns `EROFS`.
    read_only: bool,
}

pub struct WorkspaceOverlay {
    key: WorkspaceKey,
    overlay_root: PathBuf,
    staged_dir: PathBuf,
    ignored_dir: PathBuf,
    tmp_dir: PathBuf,
    journal_path: PathBuf,
    synthesized_jj_dir: RwLock<Option<JjDirBinding>>,
    /// `.gitignore` bytes per directory, tagged with the base commit they were
    /// read from. One entry per directory keeps the cache bounded while the
    /// base advances on every snapshot.
    gitignore_cache: DashMap<String, (String, Option<Vec<u8>>)>,
    state: RwLock<OverlayMutableState>,
}

/// Marks a tracked mutation in flight; see `WorkspaceOverlay::begin_mutation`.
struct MutationGuard<'a> {
    overlay: &'a WorkspaceOverlay,
}

impl Drop for MutationGuard<'_> {
    fn drop(&mut self) {
        let mut guard = self.overlay.state.write();
        guard.dirty_seq = guard.dirty_seq.wrapping_add(1);
        guard.mutations_in_flight -= 1;
    }
}

/// A file or symlink under `overlay/tmp/`, removed on drop unless renamed into
/// place first.
struct TmpPath {
    path: PathBuf,
}

impl Drop for TmpPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Where the bytes of a file or symlink that is being renamed come from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LeafOrigin {
    Ignored,
    Staged,
    Base,
}

impl WorkspaceOverlay {
    /// Opens or recovers the durable overlay directory for `key` by replaying
    /// `journal.log`.
    pub fn open_or_recover(overlays_root: &Path, key: WorkspaceKey) -> Result<Arc<Self>, JjError> {
        let ws_hash = workspace_storage_id(&key);
        let overlay_root = overlays_root.join(ws_hash);
        let staged_dir = overlay_root.join("staged");
        let ignored_dir = overlay_root.join("ignored");
        let tmp_dir = overlay_root.join("tmp");
        let journal_path = overlay_root.join("journal.log");
        std::fs::create_dir_all(&staged_dir)?;
        std::fs::create_dir_all(&ignored_dir)?;
        // Leftovers in tmp/ are partial copies from requests interrupted by a
        // crash; no journal record refers to them.
        let _ = std::fs::remove_dir_all(&tmp_dir);
        std::fs::create_dir_all(&tmp_dir)?;

        let mut mutable = OverlayMutableState::default();
        if journal_path.is_file() {
            let file = File::open(&journal_path)?;
            let reader = BufReader::new(file);
            for line in reader.lines() {
                let raw = line?;
                if raw.trim().is_empty() {
                    continue;
                }
                if let Ok(rec) = serde_json::from_str::<JournalRecord>(&raw) {
                    apply_journal_record(&mut mutable, &staged_dir, rec);
                    mutable.dirty_seq = mutable.dirty_seq.wrapping_add(1);
                }
            }
            if !mutable.deleted.is_empty()
                || !mutable.dirty_files.is_empty()
                || !mutable.dirty_symlinks.is_empty()
            {
                mutable.last_mutation = Some(Instant::now());
            }
        }

        Ok(Arc::new(Self {
            key,
            overlay_root,
            staged_dir,
            ignored_dir,
            tmp_dir,
            journal_path,
            synthesized_jj_dir: RwLock::new(None),
            gitignore_cache: DashMap::new(),
            state: RwLock::new(mutable),
        }))
    }

    pub fn key(&self) -> &WorkspaceKey {
        &self.key
    }

    pub fn overlay_root(&self) -> &Path {
        &self.overlay_root
    }

    pub fn staged_dir(&self) -> &Path {
        &self.staged_dir
    }

    pub fn ignored_dir(&self) -> &Path {
        &self.ignored_dir
    }

    pub fn journal_path(&self) -> &Path {
        &self.journal_path
    }

    /// Serves `<workspace>/.jj` read-write from `path` (a synthesized `.jj`
    /// owned by jjfsd).
    pub fn set_synthesized_jj_dir(&self, path: Option<PathBuf>) {
        *self.synthesized_jj_dir.write() = path.map(|host_root| JjDirBinding {
            host_root,
            read_only: false,
        });
    }

    /// Serves `<workspace>/.jj` read-only from `path`, for a `.jj` that another
    /// working copy owns (the source checkout of a local-jj repo).
    pub fn set_read_only_jj_dir(&self, path: PathBuf) {
        *self.synthesized_jj_dir.write() = Some(JjDirBinding {
            host_root: path,
            read_only: true,
        });
    }

    pub fn synthesized_jj_dir(&self) -> Option<PathBuf> {
        self.synthesized_jj_dir
            .read()
            .as_ref()
            .map(|binding| binding.host_root.clone())
    }

    /// Host path behind `.jj` or `.jj/<sub>` for a read.
    fn jj_read_path(&self, rel_str: &str) -> Result<PathBuf, JjError> {
        let jj_root = self.synthesized_jj_dir().ok_or(JjError::NotFound)?;
        Ok(join_jj_sub(jj_root, rel_str))
    }

    /// Host path behind `.jj/<sub>` for a mutation; `EROFS` when the binding is
    /// read-only.
    fn jj_write_path(&self, rel_str: &str) -> Result<PathBuf, JjError> {
        match self.synthesized_jj_dir.read().as_ref() {
            Some(binding) if !binding.read_only => {
                Ok(join_jj_sub(binding.host_root.clone(), rel_str))
            }
            _ => Err(JjError::ReadOnly),
        }
    }

    /// Marks a tracked mutation in flight until the returned guard drops.
    /// Bumping `dirty_seq` here makes a snapshot captured earlier fail
    /// `complete_snapshot`, and the in-flight count stops a snapshot captured
    /// after this point from deleting staged files that this mutation is still
    /// writing.
    fn begin_mutation(&self) -> MutationGuard<'_> {
        let mut guard = self.state.write();
        guard.dirty_seq = guard.dirty_seq.wrapping_add(1);
        guard.mutations_in_flight += 1;
        MutationGuard { overlay: self }
    }

    /// Returns `(dirty_seq, last_mutation)` if there are un-snapshotted tracked
    /// changes.
    pub fn dirty_status(&self) -> Option<(u64, Option<Instant>)> {
        let guard = self.state.read();
        if guard.deleted.is_empty()
            && guard.dirty_files.is_empty()
            && guard.dirty_symlinks.is_empty()
        {
            None
        } else {
            Some((guard.dirty_seq, guard.last_mutation))
        }
    }

    pub fn has_dirty_tracked_paths(&self) -> bool {
        self.dirty_status().is_some()
    }

    /// Captures a point-in-time snapshot of `(dirty_seq, dirty_map)` for
    /// `snapshot_workspace`.
    pub fn capture_dirty_snapshot(&self) -> (u64, BTreeMap<String, DirtyEntryState>) {
        let guard = self.state.read();
        let mut out = BTreeMap::new();
        for del in &guard.deleted {
            out.insert(del.clone(), DirtyEntryState::Deleted);
        }
        for (rel, exec) in &guard.dirty_files {
            out.insert(
                rel.clone(),
                DirtyEntryState::File {
                    staged_path: self.staged_dir.join(rel),
                    executable: *exec,
                },
            );
        }
        for (rel, target) in &guard.dirty_symlinks {
            out.insert(
                rel.clone(),
                DirtyEntryState::Symlink {
                    target: target.clone(),
                },
            );
        }
        (guard.dirty_seq, out)
    }

    /// Clears snapshotted staged entries and compacts `journal.log` when no
    /// tracked mutation started since the capture (`current_seq ==
    /// captured_seq`) and none is in flight, while leaving `overlay/ignored/`
    /// (`target/`, swap files) untouched. Returns `Ok(false)` and
    /// keeps everything for the next pass otherwise.
    pub fn complete_snapshot(&self, captured_seq: u64) -> Result<bool, JjError> {
        let mut guard = self.state.write();
        if guard.dirty_seq != captured_seq || guard.mutations_in_flight > 0 {
            return Ok(false);
        }
        for rel in guard.dirty_files.keys() {
            let _ = std::fs::remove_file(self.staged_dir.join(rel));
        }
        for rel in guard.dirty_symlinks.keys() {
            let _ = std::fs::remove_file(self.staged_dir.join(rel));
        }
        prune_empty_dirs(&self.staged_dir);
        guard.deleted.clear();
        guard.dirty_files.clear();
        guard.dirty_symlinks.clear();
        guard.dirty_dirs.clear();
        guard.last_mutation = None;
        if self.journal_path.exists() {
            std::fs::write(&self.journal_path, b"")?;
        }
        Ok(true)
    }

    fn append_journal(
        &self,
        guard: &mut OverlayMutableState,
        rec: JournalRecord,
        sync: bool,
    ) -> Result<(), JjError> {
        self.write_journal_line(&rec, sync)?;
        self.apply_recorded(guard, rec);
        Ok(())
    }

    /// Appends `rec` to `journal.log` without applying it to the in-memory
    /// maps.
    fn write_journal_line(&self, rec: &JournalRecord, sync: bool) -> Result<(), JjError> {
        let mut line = serde_json::to_vec(rec)
            .map_err(|e| std::io::Error::other(format!("journal serialize error: {e}")))?;
        line.push(b'\n');
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.journal_path)?;
        file.write_all(&line)?;
        if sync {
            file.sync_data()?;
        }
        Ok(())
    }

    /// Applies a record that is already in `journal.log` to the in-memory maps.
    fn apply_recorded(&self, guard: &mut OverlayMutableState, rec: JournalRecord) {
        apply_journal_record(guard, &self.staged_dir, rec);
        guard.dirty_seq = guard.dirty_seq.wrapping_add(1);
        guard.last_mutation = Some(Instant::now());
    }

    /// Moves the host file or symlink `src` to `staged/<rec.rel_path>` and
    /// records `rec` (an `Added` or `Symlink` record). The record is written
    /// first: replay applies it only if the staged entry exists, so a crash
    /// before the move leaves the destination unchanged.
    fn move_into_staged(
        &self,
        guard: &mut OverlayMutableState,
        src: &Path,
        rec: JournalRecord,
    ) -> Result<(), JjError> {
        let dst = self.staged_dir.join(&rec.rel_path);
        self.write_journal_line(&rec, false)?;
        if let Err(err) = std::fs::rename(src, &dst) {
            // `rec` is in the journal but did not happen. If the destination is
            // already a staged entry, replay would apply `rec` to it, so record
            // its current state again.
            if let Some(current) = current_dirty_record(guard, &rec.rel_path) {
                let _ = self.write_journal_line(&current, false);
            }
            return Err(err.into());
        }
        self.apply_recorded(guard, rec);
        Ok(())
    }

    /// Creates an empty file under `overlay/tmp/`.
    fn create_tmp_file(&self) -> Result<(TmpPath, File), JjError> {
        let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let path = self.tmp_dir.join(format!("{}.{seq}", std::process::id()));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        Ok((TmpPath { path }, file))
    }

    /// Creates a symlink to `target` under `overlay/tmp/`.
    fn create_tmp_symlink(&self, target: &Path) -> Result<TmpPath, JjError> {
        let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let path = self.tmp_dir.join(format!("{}.{seq}", std::process::id()));
        std::os::unix::fs::symlink(target, &path)?;
        Ok(TmpPath { path })
    }

    /// Streams the base-commit file `rel_str` into a new file under
    /// `overlay/tmp/` in `COPY_CHUNK_BYTES` reads, so large blobs are neither
    /// held in memory nor truncated.
    async fn copy_base_file_to_tmp(
        &self,
        backend: &Arc<dyn ReadOnlyVfsBackend>,
        base_commit_hex: &str,
        rel_str: &str,
        size_hint: u64,
        executable: bool,
    ) -> Result<TmpPath, JjError> {
        let (tmp, mut file) = self.create_tmp_file()?;
        let mut offset = 0u64;
        loop {
            let chunk = backend
                .read_file_range(
                    base_commit_hex,
                    Path::new(rel_str),
                    offset,
                    COPY_CHUNK_BYTES,
                )
                .await?;
            if chunk.is_empty() {
                break;
            }
            file.write_all(&chunk)?;
            offset += chunk.len() as u64;
            // A short read past the expected size is the end of the blob; a
            // short read before it may be a partial range reply, so keep
            // reading until empty.
            if chunk.len() < COPY_CHUNK_BYTES as usize && offset >= size_hint {
                break;
            }
        }
        drop(file);
        set_exec_perm(&tmp.path, executable)?;
        Ok(tmp)
    }

    /// Evaluates `.gitignore` chains from the workspace root down to
    /// `rel_path`.
    pub async fn is_path_ignored(
        &self,
        backend: &Arc<dyn ReadOnlyVfsBackend>,
        base_commit_hex: &str,
        rel_str: &str,
        is_dir: bool,
    ) -> Result<bool, JjError> {
        if rel_str.is_empty() || rel_str == ".jj" || rel_str.starts_with(".jj/") {
            return Ok(false);
        }
        // If already present in ignored/, keep routing to ignored/.
        if std::fs::symlink_metadata(self.ignored_dir.join(rel_str)).is_ok() {
            return Ok(true);
        }
        // If already staged as tracked or present in the base commit (and not
        // deleted), it is tracked!
        let not_deleted = {
            let guard = self.state.read();
            if guard.dirty_files.contains_key(rel_str)
                || guard.dirty_symlinks.contains_key(rel_str)
                || guard.dirty_dirs.contains(rel_str)
            {
                return Ok(false);
            }
            !guard.deleted.contains(rel_str)
        };
        if not_deleted
            && backend
                .lookup_in_commit(base_commit_hex, Path::new(rel_str))
                .await
                .is_ok()
        {
            return Ok(false);
        }

        let parts: Vec<&str> = rel_str.split('/').filter(|s| !s.is_empty()).collect();
        let mut ignore = GitIgnoreFile::empty();
        if let Some(root_gi) = self
            .read_gitignore_bytes(backend, base_commit_hex, "")
            .await?
        {
            ignore = ignore
                .chain(RepoPath::root(), Path::new(".gitignore"), &root_gi)
                .map_err(|e| std::io::Error::other(e.to_string()))?;
        }

        let mut prefix_acc = String::new();
        for (idx, seg) in parts.iter().enumerate() {
            if !prefix_acc.is_empty() {
                prefix_acc.push('/');
            }
            prefix_acc.push_str(seg);
            let is_last = idx + 1 == parts.len();
            let repo_path = RepoPathBuf::from_internal_string(prefix_acc.clone())
                .map_err(|_| JjError::InvalidPath)?;
            if !is_last {
                // Ancestor directory: if ignored and not tracked in base
                // commit, everything beneath is ignored!
                let tracked_dir = !self.is_deleted_in_state(&prefix_acc)
                    && backend
                        .lookup_in_commit(base_commit_hex, Path::new(&prefix_acc))
                        .await
                        .is_ok();
                if !tracked_dir && ignore.matches_dir(&repo_path) {
                    return Ok(true);
                }
                if let Some(sub_gi) = self
                    .read_gitignore_bytes(backend, base_commit_hex, &prefix_acc)
                    .await?
                {
                    let gi_fs_path = PathBuf::from(&prefix_acc).join(".gitignore");
                    ignore = ignore
                        .chain(&repo_path, &gi_fs_path, &sub_gi)
                        .map_err(|e| std::io::Error::other(e.to_string()))?;
                }
            } else {
                let matched = if is_dir {
                    ignore.matches_dir(&repo_path)
                } else {
                    ignore.matches_file(&repo_path)
                };
                return Ok(matched);
            }
        }
        Ok(false)
    }

    async fn read_gitignore_bytes(
        &self,
        backend: &Arc<dyn ReadOnlyVfsBackend>,
        base_commit_hex: &str,
        dir_rel: &str,
    ) -> Result<Option<Vec<u8>>, JjError> {
        let gi_rel = if dir_rel.is_empty() {
            ".gitignore".to_string()
        } else {
            format!("{dir_rel}/.gitignore")
        };
        {
            let guard = self.state.read();
            if guard.deleted.contains(&gi_rel) {
                return Ok(None);
            }
            if guard.dirty_files.contains_key(&gi_rel) {
                let bytes = std::fs::read(self.staged_dir.join(&gi_rel))?;
                return Ok(Some(bytes));
            }
        }
        if let Some(cached) = self.gitignore_cache.get(dir_rel)
            && cached.0 == base_commit_hex
        {
            return Ok(cached.1.clone());
        }
        let gi_path = Path::new(&gi_rel);
        let fetched = match backend.lookup_in_commit(base_commit_hex, gi_path).await {
            Ok(info) if info.attr.file_type == FileType::File => {
                let sz = info.attr.size.min(1024 * 1024) as u32;
                let data = backend
                    .read_file_range(base_commit_hex, gi_path, 0, sz)
                    .await?;
                Some(data.into_vec())
            }
            _ => None,
        };
        if self.gitignore_cache.len() >= MAX_GITIGNORE_CACHE_ENTRIES
            && !self.gitignore_cache.contains_key(dir_rel)
        {
            self.gitignore_cache.clear();
        }
        self.gitignore_cache.insert(
            dir_rel.to_string(),
            (base_commit_hex.to_string(), fetched.clone()),
        );
        Ok(fetched)
    }

    fn is_deleted_in_state(&self, rel_str: &str) -> bool {
        let guard = self.state.read();
        is_deleted_or_under_deleted(&guard.deleted, rel_str)
    }

    /// Resolves a `VirtualFile` node inside this workspace at `rel_path`,
    /// combining synthesized `.jj/`, `ignored/`, `staged/`, `deleted`
    /// whiteouts, and base commit.
    pub async fn get_virtual_entry(
        self: &Arc<Self>,
        backend: Arc<dyn ReadOnlyVfsBackend>,
        base_commit_hex: String,
        rel_path: &Path,
    ) -> Result<Box<dyn VirtualFile>, JjError> {
        let rel_str = normalize_rel_path(rel_path)?;
        if rel_str == ".jj" || rel_str.starts_with(".jj/") {
            if let Some(jj_root) = self.synthesized_jj_dir() {
                let sub = rel_str
                    .strip_prefix(".jj")
                    .unwrap_or("")
                    .trim_start_matches('/');
                let host_path = if sub.is_empty() {
                    jj_root
                } else {
                    jj_root.join(sub)
                };
                return Ok(Box::new(HostPassthroughNode::new(host_path)?));
            }
            return Err(JjError::NotFound);
        }

        Ok(Box::new(OverlayEntryNode {
            overlay: Arc::clone(self),
            backend,
            base_commit_hex,
            rel_str,
        }))
    }

    pub async fn attributes_at(
        &self,
        backend: &Arc<dyn ReadOnlyVfsBackend>,
        base_commit_hex: &str,
        rel_str: &str,
    ) -> Result<FileAttributes, JjError> {
        if rel_str.is_empty() {
            return Ok(FileAttributes::directory(SystemTime::now()));
        }
        if rel_str == ".jj" || rel_str.starts_with(".jj/") {
            if let Some(jj_root) = self.synthesized_jj_dir() {
                let sub = rel_str
                    .strip_prefix(".jj")
                    .unwrap_or("")
                    .trim_start_matches('/');
                let host_path = if sub.is_empty() {
                    jj_root
                } else {
                    jj_root.join(sub)
                };
                return host_attributes(&host_path);
            }
            return Err(JjError::NotFound);
        }

        let ignored_path = self.ignored_dir.join(rel_str);
        if let Ok(meta) = std::fs::symlink_metadata(&ignored_path) {
            return metadata_to_attributes(&ignored_path, &meta, None);
        }

        let staged_exec = {
            let guard = self.state.read();
            if guard.dirty_symlinks.contains_key(rel_str)
                || guard.dirty_files.contains_key(rel_str)
                || guard.dirty_dirs.contains(rel_str)
            {
                guard.dirty_files.get(rel_str).copied()
            } else if is_deleted_or_under_deleted(&guard.deleted, rel_str) {
                return Err(JjError::NotFound);
            } else {
                None
            }
        };

        let staged_path = self.staged_dir.join(rel_str);
        if let Ok(meta) = std::fs::symlink_metadata(&staged_path) {
            // If `staged_path` is an implicit parent directory or explicit
            // staged item
            if meta.is_dir()
                || staged_exec.is_some()
                || self.state.read().dirty_symlinks.contains_key(rel_str)
            {
                return metadata_to_attributes(&staged_path, &meta, staged_exec);
            }
        }

        if self.is_deleted_in_state(rel_str) {
            return Err(JjError::NotFound);
        }

        Ok(backend
            .lookup_in_commit(base_commit_hex, Path::new(rel_str))
            .await?
            .attr)
    }

    pub async fn read_range_at(
        &self,
        backend: &Arc<dyn ReadOnlyVfsBackend>,
        base_commit_hex: &str,
        rel_str: &str,
        offset: u64,
        size: u32,
    ) -> Result<Box<[u8]>, JjError> {
        if rel_str.is_empty() {
            return Err(JjError::NotAFile);
        }
        if rel_str == ".jj" || rel_str.starts_with(".jj/") {
            let jj_root = self.synthesized_jj_dir().ok_or(JjError::NotFound)?;
            let sub = rel_str
                .strip_prefix(".jj")
                .unwrap_or("")
                .trim_start_matches('/');
            let host_path = if sub.is_empty() {
                jj_root
            } else {
                jj_root.join(sub)
            };
            return pread_host_file(&host_path, offset, size);
        }

        let ignored_path = self.ignored_dir.join(rel_str);
        if ignored_path.is_file() {
            return pread_host_file(&ignored_path, offset, size);
        }

        let is_staged_file = self.state.read().dirty_files.contains_key(rel_str);
        if is_staged_file {
            return pread_host_file(&self.staged_dir.join(rel_str), offset, size);
        }
        if self.is_deleted_in_state(rel_str) {
            return Err(JjError::NotFound);
        }
        backend
            .read_file_range(base_commit_hex, Path::new(rel_str), offset, size)
            .await
    }

    pub async fn read_link_at(
        &self,
        backend: &Arc<dyn ReadOnlyVfsBackend>,
        base_commit_hex: &str,
        rel_str: &str,
    ) -> Result<PathBuf, JjError> {
        if rel_str == ".jj" || rel_str.starts_with(".jj/") {
            let jj_root = self.synthesized_jj_dir().ok_or(JjError::NotFound)?;
            let sub = rel_str
                .strip_prefix(".jj")
                .unwrap_or("")
                .trim_start_matches('/');
            let host_path = if sub.is_empty() {
                jj_root
            } else {
                jj_root.join(sub)
            };
            return Ok(std::fs::read_link(host_path)?);
        }
        let ignored_path = self.ignored_dir.join(rel_str);
        if let Ok(meta) = std::fs::symlink_metadata(&ignored_path)
            && meta.file_type().is_symlink()
        {
            return Ok(std::fs::read_link(&ignored_path)?);
        }
        if let Some(target) = self.state.read().dirty_symlinks.get(rel_str).cloned() {
            return Ok(target);
        }
        if self.is_deleted_in_state(rel_str) {
            return Err(JjError::NotFound);
        }
        backend
            .read_symlink(base_commit_hex, Path::new(rel_str))
            .await
    }

    pub async fn list_dir_at(
        &self,
        backend: &Arc<dyn ReadOnlyVfsBackend>,
        base_commit_hex: &str,
        rel_str: &str,
    ) -> Result<Vec<DirectoryEntry>, JjError> {
        if rel_str == ".jj" || rel_str.starts_with(".jj/") {
            let jj_root = self.synthesized_jj_dir().ok_or(JjError::NotFound)?;
            let sub = rel_str
                .strip_prefix(".jj")
                .unwrap_or("")
                .trim_start_matches('/');
            let host_path = if sub.is_empty() {
                jj_root
            } else {
                jj_root.join(sub)
            };
            return list_host_dir(&host_path);
        }

        let attr = self
            .attributes_at(backend, base_commit_hex, rel_str)
            .await?;
        if attr.file_type != FileType::Directory {
            return Err(JjError::NotADirectory);
        }

        let mut merged: BTreeMap<String, FileType> = BTreeMap::new();
        if rel_str.is_empty() && self.synthesized_jj_dir().is_some() {
            merged.insert(".jj".to_string(), FileType::Directory);
        }

        // 1. Base commit entries (unless directory itself was deleted and
        //    recreated)
        if !self.is_deleted_in_state(rel_str)
            && let Ok(base_entries) = backend
                .list_dir_in_commit(base_commit_hex, Path::new(rel_str))
                .await
        {
            let guard = self.state.read();
            for entry in base_entries {
                let child_rel = join_rel(rel_str, &entry.name);
                if !is_deleted_or_under_deleted(&guard.deleted, &child_rel) {
                    merged.insert(entry.name, entry.file_type);
                }
            }
        }

        // 2. Staged directory children
        let staged_sub = if rel_str.is_empty() {
            self.staged_dir.clone()
        } else {
            self.staged_dir.join(rel_str)
        };
        if staged_sub.is_dir() {
            let guard = self.state.read();
            for item in std::fs::read_dir(&staged_sub)? {
                let item = item?;
                let Some(name) = item.file_name().to_str().map(str::to_string) else {
                    continue;
                };
                let child_rel = join_rel(rel_str, &name);
                if guard.deleted.contains(&child_rel)
                    && !guard.dirty_files.contains_key(&child_rel)
                    && !guard.dirty_symlinks.contains_key(&child_rel)
                    && !guard.dirty_dirs.contains(&child_rel)
                {
                    continue;
                }
                let meta = item
                    .metadata()
                    .or_else(|_| std::fs::symlink_metadata(item.path()))?;
                let smeta = std::fs::symlink_metadata(item.path())?;
                let ft = if smeta.file_type().is_symlink() {
                    FileType::Symlink
                } else if meta.is_dir() {
                    FileType::Directory
                } else {
                    FileType::File
                };
                merged.insert(name, ft);
            }
        }

        // 3. Ignored directory children
        let ignored_sub = if rel_str.is_empty() {
            self.ignored_dir.clone()
        } else {
            self.ignored_dir.join(rel_str)
        };
        if ignored_sub.is_dir() {
            for item in std::fs::read_dir(&ignored_sub)? {
                let item = item?;
                let Some(name) = item.file_name().to_str().map(str::to_string) else {
                    continue;
                };
                let smeta = std::fs::symlink_metadata(item.path())?;
                let ft = if smeta.file_type().is_symlink() {
                    FileType::Symlink
                } else if smeta.is_dir() {
                    FileType::Directory
                } else {
                    FileType::File
                };
                merged.insert(name, ft);
            }
        }

        Ok(merged
            .into_iter()
            .map(|(name, file_type)| DirectoryEntry { name, file_type })
            .collect())
    }

    /// Hydrates a clean base-commit file into `overlay/staged/<rel_str>` on
    /// first write/truncate/chmod if not already staged. Callers that then
    /// modify the staged file must hold a `MutationGuard`.
    async fn ensure_staged_for_mutation(
        &self,
        backend: &Arc<dyn ReadOnlyVfsBackend>,
        base_commit_hex: &str,
        rel_str: &str,
        truncate: bool,
    ) -> Result<bool, JjError> {
        {
            let guard = self.state.read();
            if let Some(exec) = guard.dirty_files.get(rel_str).copied() {
                drop(guard);
                if truncate {
                    truncate_host_file(&self.staged_dir.join(rel_str))?;
                    let mut wguard = self.state.write();
                    self.append_journal(
                        &mut wguard,
                        JournalRecord {
                            op: JournalOp::Modified,
                            rel_path: rel_str.to_string(),
                            executable: Some(exec),
                            target: None,
                            timestamp_ms: now_ms(),
                        },
                        false,
                    )?;
                }
                return Ok(exec);
            }
            if is_deleted_or_under_deleted(&guard.deleted, rel_str) {
                return Err(JjError::NotFound);
            }
        }

        let info = backend
            .lookup_in_commit(base_commit_hex, Path::new(rel_str))
            .await?;
        if info.attr.file_type != FileType::File {
            return Err(JjError::NotAFile);
        }
        let dst = self.staged_dir.join(rel_str);
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = if truncate {
            let (tmp, _) = self.create_tmp_file()?;
            set_exec_perm(&tmp.path, info.attr.executable)?;
            tmp
        } else {
            self.copy_base_file_to_tmp(
                backend,
                base_commit_hex,
                rel_str,
                info.attr.size,
                info.attr.executable,
            )
            .await?
        };

        // Install the copy under the lock: a concurrent request may have staged
        // (and written to) the file, or deleted it, while this one was copying
        // the base blob.
        let mut guard = self.state.write();
        if let Some(exec) = guard.dirty_files.get(rel_str).copied() {
            if truncate {
                truncate_host_file(&dst)?;
                self.append_journal(
                    &mut guard,
                    JournalRecord {
                        op: JournalOp::Modified,
                        rel_path: rel_str.to_string(),
                        executable: Some(exec),
                        target: None,
                        timestamp_ms: now_ms(),
                    },
                    false,
                )?;
            }
            return Ok(exec);
        }
        if is_deleted_or_under_deleted(&guard.deleted, rel_str) {
            return Err(JjError::NotFound);
        }
        std::fs::rename(&tmp.path, &dst)?;
        self.append_journal(
            &mut guard,
            JournalRecord {
                op: JournalOp::Modified,
                rel_path: rel_str.to_string(),
                executable: Some(info.attr.executable),
                target: None,
                timestamp_ms: now_ms(),
            },
            false,
        )?;
        Ok(info.attr.executable)
    }

    pub async fn open_file(
        &self,
        backend: &Arc<dyn ReadOnlyVfsBackend>,
        base_commit_hex: &str,
        rel_path: &Path,
        write_access: bool,
        truncate: bool,
    ) -> Result<FileAttributes, JjError> {
        let rel_str = normalize_rel_path(rel_path)?;
        if rel_str.is_empty() {
            return Err(JjError::NotAFile);
        }
        if rel_str == ".jj" || rel_str.starts_with(".jj/") {
            let host_path = if write_access || truncate {
                self.jj_write_path(&rel_str)?
            } else {
                self.jj_read_path(&rel_str)?
            };
            if truncate && write_access {
                let _ = OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .open(&host_path)?;
            }
            return host_attributes(&host_path);
        }
        if !write_access && !truncate {
            return self.attributes_at(backend, base_commit_hex, &rel_str).await;
        }
        let ignored_path = self.ignored_dir.join(&rel_str);
        if ignored_path.is_file() {
            if truncate {
                let _ = OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .open(&ignored_path)?;
            }
            let meta = std::fs::symlink_metadata(&ignored_path)?;
            return metadata_to_attributes(&ignored_path, &meta, None);
        }
        // Truncating a staged file changes bytes that a running snapshot may
        // have uploaded.
        let _mutation = truncate.then(|| self.begin_mutation());
        let exec = self
            .ensure_staged_for_mutation(backend, base_commit_hex, &rel_str, truncate)
            .await?;
        let meta = std::fs::symlink_metadata(self.staged_dir.join(&rel_str))?;
        metadata_to_attributes(&self.staged_dir.join(&rel_str), &meta, Some(exec))
    }

    pub async fn create_file(
        &self,
        backend: &Arc<dyn ReadOnlyVfsBackend>,
        base_commit_hex: &str,
        rel_path: &Path,
        executable: bool,
        exclusive: bool,
    ) -> Result<FileAttributes, JjError> {
        let rel_str = normalize_rel_path(rel_path)?;
        if rel_str.is_empty() {
            return Err(JjError::InvalidPath);
        }
        if rel_str == ".jj" || rel_str.starts_with(".jj/") {
            let host_path = self.jj_write_path(&rel_str)?;
            if let Some(p) = host_path.parent() {
                std::fs::create_dir_all(p)?;
            }
            let mut opts = OpenOptions::new();
            opts.write(true).create(true);
            if exclusive {
                opts.create_new(true);
            } else {
                opts.truncate(true);
            }
            let _ = opts.open(&host_path)?;
            set_exec_perm(&host_path, executable)?;
            return host_attributes(&host_path);
        }

        if exclusive
            && self
                .attributes_at(backend, base_commit_hex, &rel_str)
                .await
                .is_ok()
        {
            return Err(JjError::AlreadyExists);
        }

        if self
            .is_path_ignored(backend, base_commit_hex, &rel_str, false)
            .await?
        {
            let dst = self.ignored_dir.join(&rel_str);
            if let Some(p) = dst.parent() {
                std::fs::create_dir_all(p)?;
            }
            std::fs::write(&dst, b"")?;
            set_exec_perm(&dst, executable)?;
            let meta = std::fs::symlink_metadata(&dst)?;
            return metadata_to_attributes(&dst, &meta, Some(executable));
        }

        let _mutation = self.begin_mutation();
        let dst = self.staged_dir.join(&rel_str);
        if let Some(p) = dst.parent() {
            std::fs::create_dir_all(p)?;
        }
        std::fs::write(&dst, b"")?;
        set_exec_perm(&dst, executable)?;
        let mut guard = self.state.write();
        self.append_journal(
            &mut guard,
            JournalRecord {
                op: JournalOp::Added,
                rel_path: rel_str.clone(),
                executable: Some(executable),
                target: None,
                timestamp_ms: now_ms(),
            },
            false,
        )?;
        drop(guard);
        let meta = std::fs::symlink_metadata(&dst)?;
        metadata_to_attributes(&dst, &meta, Some(executable))
    }

    pub async fn write_at(
        &self,
        backend: &Arc<dyn ReadOnlyVfsBackend>,
        base_commit_hex: &str,
        rel_path: &Path,
        offset: u64,
        data: &[u8],
    ) -> Result<u32, JjError> {
        let rel_str = normalize_rel_path(rel_path)?;
        if rel_str.is_empty() {
            return Err(JjError::NotAFile);
        }
        if rel_str == ".jj" || rel_str.starts_with(".jj/") {
            let f = OpenOptions::new()
                .write(true)
                .open(self.jj_write_path(&rel_str)?)?;
            f.write_all_at(data, offset)?;
            return Ok(data.len() as u32);
        }

        let ignored_path = self.ignored_dir.join(&rel_str);
        if ignored_path.is_file() {
            let f = OpenOptions::new().write(true).open(&ignored_path)?;
            f.write_all_at(data, offset)?;
            return Ok(data.len() as u32);
        }

        let _mutation = self.begin_mutation();
        let exec = self
            .ensure_staged_for_mutation(backend, base_commit_hex, &rel_str, false)
            .await?;
        let staged_path = self.staged_dir.join(&rel_str);
        let f = OpenOptions::new().write(true).open(&staged_path)?;
        f.write_all_at(data, offset)?;
        #[cfg(test)]
        test_hooks::after_staged_write(self);
        let mut guard = self.state.write();
        self.append_journal(
            &mut guard,
            JournalRecord {
                op: JournalOp::Modified,
                rel_path: rel_str,
                executable: Some(exec),
                target: None,
                timestamp_ms: now_ms(),
            },
            false,
        )?;
        Ok(data.len() as u32)
    }

    pub async fn fsync(&self, rel_path: &Path, datasync: bool) -> Result<(), JjError> {
        let rel_str = normalize_rel_path(rel_path)?;
        if rel_str.is_empty() {
            return Ok(());
        }
        // jj-lib calls `sync_data()` on `.jj` files before persisting them by
        // rename.
        if rel_str == ".jj" || rel_str.starts_with(".jj/") {
            let f = File::open(self.jj_read_path(&rel_str)?)?;
            if datasync {
                f.sync_data()?;
            } else {
                f.sync_all()?;
            }
            return Ok(());
        }
        let ignored_path = self.ignored_dir.join(&rel_str);
        if let Ok(meta) = std::fs::symlink_metadata(&ignored_path)
            && !meta.file_type().is_symlink()
        {
            let f = File::open(&ignored_path)?;
            if datasync {
                f.sync_data()?;
            } else {
                f.sync_all()?;
            }
            return Ok(());
        }
        let staged_path = self.staged_dir.join(&rel_str);
        if let Ok(meta) = std::fs::symlink_metadata(&staged_path)
            && !meta.file_type().is_symlink()
        {
            let f = File::open(&staged_path)?;
            if datasync {
                f.sync_data()?;
            } else {
                f.sync_all()?;
            }
        }
        if self.journal_path.is_file() {
            let jf = File::open(&self.journal_path)?;
            jf.sync_data()?;
        }
        Ok(())
    }

    pub async fn set_attributes(
        &self,
        backend: &Arc<dyn ReadOnlyVfsBackend>,
        base_commit_hex: &str,
        rel_path: &Path,
        changes: AttributeChanges,
    ) -> Result<FileAttributes, JjError> {
        let rel_str = normalize_rel_path(rel_path)?;
        if rel_str.is_empty() {
            if changes.size.is_some() {
                return Err(JjError::NotAFile);
            }
            return Ok(FileAttributes::directory(SystemTime::now()));
        }
        if rel_str == ".jj" || rel_str.starts_with(".jj/") {
            let host_path = self.jj_write_path(&rel_str)?;
            let meta = std::fs::symlink_metadata(&host_path)?;
            if let Some(m) = changes.mode
                && !meta.file_type().is_symlink()
            {
                std::fs::set_permissions(&host_path, std::fs::Permissions::from_mode(m & 0o777))?;
            }
            if let Some(sz) = changes.size {
                if meta.is_dir() {
                    return Err(JjError::NotAFile);
                }
                let f = OpenOptions::new().write(true).open(&host_path)?;
                f.set_len(sz)?;
            }
            set_host_times(&host_path, &changes)?;
            return host_attributes(&host_path);
        }

        if changes.is_empty() {
            return self.attributes_at(backend, base_commit_hex, &rel_str).await;
        }

        let ignored_path = self.ignored_dir.join(&rel_str);
        if let Ok(meta) = std::fs::symlink_metadata(&ignored_path) {
            if let Some(m) = changes.mode
                && !meta.is_dir()
                && !meta.file_type().is_symlink()
            {
                std::fs::set_permissions(
                    &ignored_path,
                    std::fs::Permissions::from_mode(m & 0o777),
                )?;
            }
            if let Some(sz) = changes.size {
                if meta.is_dir() {
                    return Err(JjError::NotAFile);
                }
                let f = OpenOptions::new().write(true).open(&ignored_path)?;
                f.set_len(sz)?;
            }
            set_host_times(&ignored_path, &changes)?;
            let meta = std::fs::symlink_metadata(&ignored_path)?;
            return metadata_to_attributes(&ignored_path, &meta, None);
        }

        let current = self
            .attributes_at(backend, base_commit_hex, &rel_str)
            .await?;
        match current.file_type {
            FileType::File => {}
            FileType::Directory if changes.size.is_some() => return Err(JjError::NotAFile),
            FileType::Symlink if changes.size.is_some() => return Err(JjError::InvalidPath),
            FileType::Directory | FileType::Symlink => {
                // jj records no mode or times for directories and symlinks.
                // Accept the change so `tar`, `cp -a`, `rsync -a`, and
                // `shutil.copytree` can restore metadata; a directory created
                // in this overlay keeps its times on the staged host dir.
                let staged_path = self.staged_dir.join(&rel_str);
                if let Ok(meta) = std::fs::symlink_metadata(&staged_path)
                    && meta.is_dir()
                {
                    set_host_times(&staged_path, &changes)?;
                    let meta = std::fs::symlink_metadata(&staged_path)?;
                    return metadata_to_attributes(&staged_path, &meta, None);
                }
                return Ok(current);
            }
        }

        // jj tracks only the executable bit, so a mode change that keeps it
        // records nothing.
        let exec_change = changes
            .mode
            .map(|m| (m & 0o111) != 0)
            .filter(|exec| *exec != current.executable);
        if changes.size.is_none() && exec_change.is_none() && !changes.has_times() {
            return Ok(current);
        }

        let _mutation = self.begin_mutation();
        let truncate_zero = changes.size == Some(0);
        let mut exec = self
            .ensure_staged_for_mutation(backend, base_commit_hex, &rel_str, truncate_zero)
            .await?;
        let staged_path = self.staged_dir.join(&rel_str);
        if let Some(sz) = changes.size
            && sz > 0
        {
            let f = OpenOptions::new().write(true).open(&staged_path)?;
            f.set_len(sz)?;
        }
        if let Some(new_exec) = exec_change {
            exec = new_exec;
            set_exec_perm(&staged_path, exec)?;
        }
        set_host_times(&staged_path, &changes)?;
        if changes.size.is_some() || exec_change.is_some() {
            let op = if changes.size.is_none() {
                JournalOp::Chmod
            } else {
                JournalOp::Modified
            };
            let mut guard = self.state.write();
            self.append_journal(
                &mut guard,
                JournalRecord {
                    op,
                    rel_path: rel_str,
                    executable: Some(exec),
                    target: None,
                    timestamp_ms: now_ms(),
                },
                false,
            )?;
        }
        let meta = std::fs::symlink_metadata(&staged_path)?;
        metadata_to_attributes(&staged_path, &meta, Some(exec))
    }

    pub async fn unlink(
        &self,
        backend: &Arc<dyn ReadOnlyVfsBackend>,
        base_commit_hex: &str,
        rel_path: &Path,
    ) -> Result<(), JjError> {
        let rel_str = normalize_rel_path(rel_path)?;
        if rel_str.is_empty() {
            return Err(JjError::InvalidPath);
        }
        if rel_str == ".jj" || rel_str.starts_with(".jj/") {
            std::fs::remove_file(self.jj_write_path(&rel_str)?)?;
            return Ok(());
        }

        let ignored_path = self.ignored_dir.join(&rel_str);
        if let Ok(meta) = std::fs::symlink_metadata(&ignored_path) {
            if meta.is_dir() && !meta.file_type().is_symlink() {
                return Err(JjError::NotAFile);
            }
            std::fs::remove_file(&ignored_path)?;
            return Ok(());
        }

        let attr = self
            .attributes_at(backend, base_commit_hex, &rel_str)
            .await?;
        if attr.file_type == FileType::Directory {
            return Err(JjError::NotAFile);
        }
        let _mutation = self.begin_mutation();
        let staged_path = self.staged_dir.join(&rel_str);
        if std::fs::symlink_metadata(&staged_path).is_ok() {
            let _ = std::fs::remove_file(&staged_path);
        }
        let mut guard = self.state.write();
        self.append_journal(
            &mut guard,
            JournalRecord {
                op: JournalOp::Deleted,
                rel_path: rel_str,
                executable: None,
                target: None,
                timestamp_ms: now_ms(),
            },
            false,
        )?;
        Ok(())
    }

    pub async fn mkdir(
        &self,
        backend: &Arc<dyn ReadOnlyVfsBackend>,
        base_commit_hex: &str,
        rel_path: &Path,
        _mode: u32,
    ) -> Result<FileAttributes, JjError> {
        let rel_str = normalize_rel_path(rel_path)?;
        if rel_str.is_empty() {
            return Err(JjError::AlreadyExists);
        }
        if rel_str == ".jj" || rel_str.starts_with(".jj/") {
            let host_path = self.jj_write_path(&rel_str)?;
            std::fs::create_dir(&host_path)?;
            return host_attributes(&host_path);
        }

        if self
            .attributes_at(backend, base_commit_hex, &rel_str)
            .await
            .is_ok()
        {
            return Err(JjError::AlreadyExists);
        }

        if self
            .is_path_ignored(backend, base_commit_hex, &rel_str, true)
            .await?
        {
            let dst = self.ignored_dir.join(&rel_str);
            std::fs::create_dir_all(&dst)?;
            return Ok(FileAttributes::directory(SystemTime::now()));
        }

        let _mutation = self.begin_mutation();
        let dst = self.staged_dir.join(&rel_str);
        std::fs::create_dir_all(&dst)?;
        let mut guard = self.state.write();
        self.append_journal(
            &mut guard,
            JournalRecord {
                op: JournalOp::Mkdir,
                rel_path: rel_str,
                executable: None,
                target: None,
                timestamp_ms: now_ms(),
            },
            false,
        )?;
        Ok(FileAttributes::directory(SystemTime::now()))
    }

    pub async fn rmdir(
        &self,
        backend: &Arc<dyn ReadOnlyVfsBackend>,
        base_commit_hex: &str,
        rel_path: &Path,
    ) -> Result<(), JjError> {
        let rel_str = normalize_rel_path(rel_path)?;
        if rel_str.is_empty() || rel_str == ".jj" {
            return Err(JjError::Busy);
        }
        if rel_str.starts_with(".jj/") {
            std::fs::remove_dir(self.jj_write_path(&rel_str)?)?;
            return Ok(());
        }

        let children = self.list_dir_at(backend, base_commit_hex, &rel_str).await?;
        if !children.is_empty() {
            return Err(JjError::DirectoryNotEmpty);
        }

        let ignored_path = self.ignored_dir.join(&rel_str);
        if ignored_path.is_dir() {
            std::fs::remove_dir(&ignored_path)?;
            // `create_dir_all` also makes `ignored/<dir>` as the parent of an
            // ignored child (`pkg/__pycache__/`) of a tracked dir, so the same
            // directory can still exist in `staged/` or the base commit and
            // needs its own removal below.
            match self.attributes_at(backend, base_commit_hex, &rel_str).await {
                Ok(attr) if attr.file_type == FileType::Directory => {}
                Ok(_) | Err(JjError::NotFound) => return Ok(()),
                Err(err) => return Err(err),
            }
        }

        let _mutation = self.begin_mutation();
        let staged_path = self.staged_dir.join(&rel_str);
        if staged_path.is_dir() {
            let _ = std::fs::remove_dir_all(&staged_path);
        }
        let mut guard = self.state.write();
        self.append_journal(
            &mut guard,
            JournalRecord {
                op: JournalOp::Rmdir,
                rel_path: rel_str,
                executable: None,
                target: None,
                timestamp_ms: now_ms(),
            },
            false,
        )?;
        Ok(())
    }

    pub async fn symlink(
        &self,
        backend: &Arc<dyn ReadOnlyVfsBackend>,
        base_commit_hex: &str,
        link_rel: &Path,
        target: &Path,
    ) -> Result<FileAttributes, JjError> {
        let rel_str = normalize_rel_path(link_rel)?;
        if rel_str.is_empty() {
            return Err(JjError::InvalidPath);
        }
        if rel_str == ".jj" || rel_str.starts_with(".jj/") {
            let host_path = self.jj_write_path(&rel_str)?;
            if let Some(p) = host_path.parent() {
                std::fs::create_dir_all(p)?;
            }
            std::os::unix::fs::symlink(target, &host_path)?;
            return host_attributes(&host_path);
        }

        if self
            .attributes_at(backend, base_commit_hex, &rel_str)
            .await
            .is_ok()
        {
            return Err(JjError::AlreadyExists);
        }

        if self
            .is_path_ignored(backend, base_commit_hex, &rel_str, false)
            .await?
        {
            let dst = self.ignored_dir.join(&rel_str);
            if let Some(p) = dst.parent() {
                std::fs::create_dir_all(p)?;
            }
            std::os::unix::fs::symlink(target, &dst)?;
            return Ok(FileAttributes::symlink(
                target.as_os_str().len() as u64,
                SystemTime::now(),
            ));
        }

        let target_str = target.to_str().ok_or(JjError::InvalidPath)?.to_string();
        let _mutation = self.begin_mutation();
        let dst = self.staged_dir.join(&rel_str);
        if let Some(p) = dst.parent() {
            std::fs::create_dir_all(p)?;
        }
        if std::fs::symlink_metadata(&dst).is_ok() {
            let _ = std::fs::remove_file(&dst);
        }
        std::os::unix::fs::symlink(target, &dst)?;
        let mut guard = self.state.write();
        self.append_journal(
            &mut guard,
            JournalRecord {
                op: JournalOp::Symlink,
                rel_path: rel_str,
                executable: None,
                target: Some(target_str),
                timestamp_ms: now_ms(),
            },
            false,
        )?;
        Ok(FileAttributes::symlink(
            target.as_os_str().len() as u64,
            SystemTime::now(),
        ))
    }

    pub async fn rename(
        &self,
        backend: &Arc<dyn ReadOnlyVfsBackend>,
        base_commit_hex: &str,
        old_rel: &Path,
        new_rel: &Path,
    ) -> Result<(), JjError> {
        let old_str = normalize_rel_path(old_rel)?;
        let new_str = normalize_rel_path(new_rel)?;
        if old_str.is_empty() || new_str.is_empty() {
            return Err(JjError::InvalidPath);
        }
        if old_str == ".jj" || new_str == ".jj" {
            return Err(JjError::Busy);
        }
        if old_str == new_str {
            let _ = self
                .attributes_at(backend, base_commit_hex, &old_str)
                .await?;
            return Ok(());
        }
        if new_str.starts_with(&format!("{old_str}/")) {
            return Err(JjError::InvalidPath);
        }
        if old_str.starts_with(".jj/") || new_str.starts_with(".jj/") {
            if old_str.starts_with(".jj/") && new_str.starts_with(".jj/") {
                let old_host = self.jj_write_path(&old_str)?;
                let new_host = self.jj_write_path(&new_str)?;
                if let Some(p) = new_host.parent() {
                    std::fs::create_dir_all(p)?;
                }
                std::fs::rename(old_host, new_host)?;
                return Ok(());
            }
            return Err(JjError::CrossDevice);
        }

        if let Some((parent_rel, _)) = new_str.rsplit_once('/') {
            let parent_attr = self
                .attributes_at(backend, base_commit_hex, parent_rel)
                .await?;
            if parent_attr.file_type != FileType::Directory {
                return Err(JjError::NotADirectory);
            }
        }

        let old_attr = self
            .attributes_at(backend, base_commit_hex, &old_str)
            .await?;
        let new_attr = self.attributes_at(backend, base_commit_hex, &new_str).await;
        match old_attr.file_type {
            FileType::File | FileType::Symlink => {
                self.rename_leaf(
                    backend,
                    base_commit_hex,
                    &old_str,
                    &new_str,
                    &old_attr,
                    new_attr,
                )
                .await
            }
            FileType::Directory => {
                self.rename_directory(backend, base_commit_hex, &old_str, &new_str, new_attr)
                    .await
            }
        }
    }

    async fn rename_leaf(
        &self,
        backend: &Arc<dyn ReadOnlyVfsBackend>,
        base_commit_hex: &str,
        old_str: &str,
        new_str: &str,
        old_attr: &FileAttributes,
        new_attr: Result<FileAttributes, JjError>,
    ) -> Result<(), JjError> {
        match new_attr {
            Ok(na) if na.file_type == FileType::Directory => return Err(JjError::NotAFile),
            Ok(_) | Err(JjError::NotFound) => {}
            Err(err) => return Err(err),
        }

        let is_staged = {
            let guard = self.state.read();
            guard.dirty_files.contains_key(old_str) || guard.dirty_symlinks.contains_key(old_str)
        };
        let origin = if std::fs::symlink_metadata(self.ignored_dir.join(old_str)).is_ok() {
            LeafOrigin::Ignored
        } else if is_staged {
            LeafOrigin::Staged
        } else {
            LeafOrigin::Base
        };

        if self
            .is_path_ignored(backend, base_commit_hex, new_str, false)
            .await?
        {
            let dst_ignored = self.ignored_dir.join(new_str);
            if let Some(p) = dst_ignored.parent() {
                std::fs::create_dir_all(p)?;
            }
            match origin {
                LeafOrigin::Ignored => {
                    std::fs::rename(self.ignored_dir.join(old_str), &dst_ignored)?;
                }
                LeafOrigin::Staged => {
                    let _mutation = self.begin_mutation();
                    let src_staged = self.staged_dir.join(old_str);
                    std::fs::rename(&src_staged, &dst_ignored)?;
                    let mut guard = self.state.write();
                    let del_rec = JournalRecord {
                        op: JournalOp::Deleted,
                        rel_path: old_str.to_string(),
                        executable: None,
                        target: None,
                        timestamp_ms: now_ms(),
                    };
                    if let Err(err) = self.append_journal(&mut guard, del_rec, false) {
                        let _ = std::fs::rename(&dst_ignored, &src_staged);
                        return Err(err);
                    }
                }
                LeafOrigin::Base => {
                    let _mutation = self.begin_mutation();
                    let tmp = match old_attr.file_type {
                        FileType::File => {
                            self.copy_base_file_to_tmp(
                                backend,
                                base_commit_hex,
                                old_str,
                                old_attr.size,
                                old_attr.executable,
                            )
                            .await?
                        }
                        FileType::Symlink => {
                            let target =
                                self.read_link_at(backend, base_commit_hex, old_str).await?;
                            self.create_tmp_symlink(&target)?
                        }
                        FileType::Directory => unreachable!(),
                    };
                    std::fs::rename(&tmp.path, &dst_ignored)?;
                    let mut guard = self.state.write();
                    self.append_journal(
                        &mut guard,
                        JournalRecord {
                            op: JournalOp::Deleted,
                            rel_path: old_str.to_string(),
                            executable: None,
                            target: None,
                            timestamp_ms: now_ms(),
                        },
                        false,
                    )?;
                }
            }
            return Ok(());
        }

        let _mutation = self.begin_mutation();
        let dst_rec = match old_attr.file_type {
            FileType::File => JournalRecord {
                op: JournalOp::Added,
                rel_path: new_str.to_string(),
                executable: Some(old_attr.executable),
                target: None,
                timestamp_ms: now_ms(),
            },
            FileType::Symlink => {
                let target = self.read_link_at(backend, base_commit_hex, old_str).await?;
                let target_str = target.to_str().ok_or(JjError::InvalidPath)?.to_string();
                JournalRecord {
                    op: JournalOp::Symlink,
                    rel_path: new_str.to_string(),
                    executable: None,
                    target: Some(target_str),
                    timestamp_ms: now_ms(),
                }
            }
            FileType::Directory => unreachable!(),
        };

        if let Some(p) = self.staged_dir.join(new_str).parent() {
            std::fs::create_dir_all(p)?;
        }

        match origin {
            LeafOrigin::Ignored => {
                let src_ignored = self.ignored_dir.join(old_str);
                let mut guard = self.state.write();
                self.move_into_staged(&mut guard, &src_ignored, dst_rec)?;
            }
            LeafOrigin::Staged => {
                let src_staged = self.staged_dir.join(old_str);
                let mut guard = self.state.write();
                self.move_into_staged(&mut guard, &src_staged, dst_rec)?;
                self.append_journal(
                    &mut guard,
                    JournalRecord {
                        op: JournalOp::Deleted,
                        rel_path: old_str.to_string(),
                        executable: None,
                        target: None,
                        timestamp_ms: now_ms(),
                    },
                    false,
                )?;
            }
            LeafOrigin::Base => {
                let tmp = match old_attr.file_type {
                    FileType::File => {
                        self.copy_base_file_to_tmp(
                            backend,
                            base_commit_hex,
                            old_str,
                            old_attr.size,
                            old_attr.executable,
                        )
                        .await?
                    }
                    FileType::Symlink => {
                        let target = self.read_link_at(backend, base_commit_hex, old_str).await?;
                        self.create_tmp_symlink(&target)?
                    }
                    FileType::Directory => unreachable!(),
                };
                let mut guard = self.state.write();
                if is_deleted_or_under_deleted(&guard.deleted, old_str) {
                    return Err(JjError::NotFound);
                }
                self.move_into_staged(&mut guard, &tmp.path, dst_rec)?;
                self.append_journal(
                    &mut guard,
                    JournalRecord {
                        op: JournalOp::Deleted,
                        rel_path: old_str.to_string(),
                        executable: None,
                        target: None,
                        timestamp_ms: now_ms(),
                    },
                    false,
                )?;
            }
        }
        Ok(())
    }

    async fn rename_directory(
        &self,
        backend: &Arc<dyn ReadOnlyVfsBackend>,
        base_commit_hex: &str,
        old_str: &str,
        new_str: &str,
        new_attr: Result<FileAttributes, JjError>,
    ) -> Result<(), JjError> {
        match new_attr {
            Ok(na) if na.file_type != FileType::Directory => return Err(JjError::NotADirectory),
            Ok(_) => {
                let children = self.list_dir_at(backend, base_commit_hex, new_str).await?;
                if !children.is_empty() {
                    return Err(JjError::DirectoryNotEmpty);
                }
            }
            Err(JjError::NotFound) => {}
            Err(err) => return Err(err),
        }

        // Directories still backed by the base commit tree return EXDEV (like
        // overlayfs without redirect_dir) so `mv` and `shutil.move` fall back
        // to copy + delete.
        if !self.is_deleted_in_state(old_str)
            && backend
                .lookup_in_commit(base_commit_hex, Path::new(old_str))
                .await
                .is_ok()
        {
            return Err(JjError::CrossDevice);
        }
        if !self.is_deleted_in_state(new_str)
            && backend
                .lookup_in_commit(base_commit_hex, Path::new(new_str))
                .await
                .is_ok()
        {
            return Err(JjError::CrossDevice);
        }

        let ignored_old = self.ignored_dir.join(old_str);
        let staged_old = self.staged_dir.join(old_str);
        let old_prefix = format!("{old_str}/");
        let has_tracked = staged_old.is_dir() || {
            let guard = self.state.read();
            guard.dirty_dirs.contains(old_str)
                || guard.dirty_dirs.iter().any(|d| d.starts_with(&old_prefix))
                || guard.dirty_files.keys().any(|k| k.starts_with(&old_prefix))
                || guard
                    .dirty_symlinks
                    .keys()
                    .any(|k| k.starts_with(&old_prefix))
        };
        let new_is_ignored = self
            .is_path_ignored(backend, base_commit_hex, new_str, true)
            .await?;

        if !has_tracked && ignored_old.is_dir() && new_is_ignored {
            let ignored_new = self.ignored_dir.join(new_str);
            if let Some(p) = ignored_new.parent() {
                std::fs::create_dir_all(p)?;
            }
            std::fs::rename(&ignored_old, &ignored_new)?;
            return Ok(());
        }

        if has_tracked == new_is_ignored {
            return Err(JjError::CrossDevice);
        }

        let _mutation = self.begin_mutation();
        let staged_new = self.staged_dir.join(new_str);
        if let Some(p) = staged_new.parent() {
            std::fs::create_dir_all(p)?;
        }

        let mut guard = self.state.write();
        let ts = now_ms();
        let mut leaf_records = Vec::new();
        for (rel, &exec) in &guard.dirty_files {
            if let Some(rest) = rel.strip_prefix(&old_prefix) {
                leaf_records.push(JournalRecord {
                    op: JournalOp::Added,
                    rel_path: format!("{new_str}/{rest}"),
                    executable: Some(exec),
                    target: None,
                    timestamp_ms: ts,
                });
            }
        }
        for (rel, tgt) in &guard.dirty_symlinks {
            if let Some(rest) = rel.strip_prefix(&old_prefix) {
                let target_str = tgt.to_str().ok_or(JjError::InvalidPath)?.to_string();
                leaf_records.push(JournalRecord {
                    op: JournalOp::Symlink,
                    rel_path: format!("{new_str}/{rest}"),
                    executable: None,
                    target: Some(target_str),
                    timestamp_ms: ts,
                });
            }
        }
        let mut dir_records = vec![JournalRecord {
            op: JournalOp::Mkdir,
            rel_path: new_str.to_string(),
            executable: None,
            target: None,
            timestamp_ms: ts,
        }];
        for d in &guard.dirty_dirs {
            if let Some(rest) = d.strip_prefix(&old_prefix) {
                dir_records.push(JournalRecord {
                    op: JournalOp::Mkdir,
                    rel_path: format!("{new_str}/{rest}"),
                    executable: None,
                    target: None,
                    timestamp_ms: ts,
                });
            }
        }

        for rec in &leaf_records {
            self.write_journal_line(rec, false)?;
        }

        if staged_old.is_dir() {
            std::fs::rename(&staged_old, &staged_new)?;
        } else {
            std::fs::create_dir_all(&staged_new)?;
        }

        let ignored_new = self.ignored_dir.join(new_str);
        if ignored_old.is_dir() {
            if let Some(p) = ignored_new.parent() {
                let _ = std::fs::create_dir_all(p);
            }
            if let Err(err) = std::fs::rename(&ignored_old, &ignored_new) {
                if staged_new.is_dir() {
                    let _ = std::fs::rename(&staged_new, &staged_old);
                }
                return Err(err.into());
            }
        } else if ignored_new.is_dir() {
            let _ = std::fs::remove_dir(&ignored_new);
        }

        for rec in leaf_records {
            self.apply_recorded(&mut guard, rec);
        }
        for rec in dir_records {
            self.append_journal(&mut guard, rec, false)?;
        }
        self.append_journal(
            &mut guard,
            JournalRecord {
                op: JournalOp::Rmdir,
                rel_path: old_str.to_string(),
                executable: None,
                target: None,
                timestamp_ms: ts,
            },
            false,
        )?;
        Ok(())
    }
}

/// Manages active `WorkspaceOverlay` instances across workspaces on this
/// daemon.
pub struct OverlayManager {
    overlays_root: PathBuf,
    overlays: DashMap<WorkspaceKey, Arc<WorkspaceOverlay>>,
}

impl OverlayManager {
    pub fn new(overlays_root: impl Into<PathBuf>) -> Result<Self, JjError> {
        let root = overlays_root.into();
        std::fs::create_dir_all(&root)?;
        Ok(Self {
            overlays_root: root,
            overlays: DashMap::new(),
        })
    }

    pub fn get_or_open(&self, key: &WorkspaceKey) -> Result<Arc<WorkspaceOverlay>, JjError> {
        if let Some(existing) = self.overlays.get(key) {
            return Ok(Arc::clone(&existing));
        }
        match self.overlays.entry(key.clone()) {
            dashmap::Entry::Occupied(occ) => Ok(Arc::clone(occ.get())),
            dashmap::Entry::Vacant(vac) => {
                let opened = WorkspaceOverlay::open_or_recover(&self.overlays_root, key.clone())?;
                vac.insert(Arc::clone(&opened));
                Ok(opened)
            }
        }
    }

    pub fn has_dirty_for_source(&self, alias: &str) -> bool {
        self.overlays
            .iter()
            .any(|item| item.key().source == alias && item.value().has_dirty_tracked_paths())
    }

    pub fn active_overlays(&self) -> Vec<Arc<WorkspaceOverlay>> {
        self.overlays
            .iter()
            .map(|r| Arc::clone(r.value()))
            .collect()
    }
}

struct OverlayEntryNode {
    overlay: Arc<WorkspaceOverlay>,
    backend: Arc<dyn ReadOnlyVfsBackend>,
    base_commit_hex: String,
    rel_str: String,
}

#[async_trait]
impl VirtualFile for OverlayEntryNode {
    async fn read(&self) -> Result<Pin<Box<dyn futures::AsyncRead + Send>>, JjError> {
        let attr = self.attributes().await?;
        if attr.file_type != FileType::File {
            return Err(JjError::NotAFile);
        }
        let bytes = self
            .overlay
            .read_range_at(
                &self.backend,
                &self.base_commit_hex,
                &self.rel_str,
                0,
                attr.size.min(u32::MAX as u64) as u32,
            )
            .await?;
        Ok(Box::pin(Cursor::new(bytes.into_vec())))
    }

    async fn read_range(&self, offset: u64, size: u32) -> Result<Box<[u8]>, JjError> {
        self.overlay
            .read_range_at(
                &self.backend,
                &self.base_commit_hex,
                &self.rel_str,
                offset,
                size,
            )
            .await
    }

    async fn list(&self) -> Result<DirectoryStream, JjError> {
        let entries = self
            .overlay
            .list_dir_at(&self.backend, &self.base_commit_hex, &self.rel_str)
            .await?;
        Ok(Box::pin(futures::stream::iter(entries)))
    }

    async fn read_link(&self) -> Result<PathBuf, JjError> {
        self.overlay
            .read_link_at(&self.backend, &self.base_commit_hex, &self.rel_str)
            .await
    }

    async fn attributes(&self) -> Result<FileAttributes, JjError> {
        self.overlay
            .attributes_at(&self.backend, &self.base_commit_hex, &self.rel_str)
            .await
    }

    async fn file_type(&self) -> Result<FileType, JjError> {
        Ok(self.attributes().await?.file_type)
    }
}

struct HostPassthroughNode {
    host_path: PathBuf,
}

impl HostPassthroughNode {
    fn new(host_path: PathBuf) -> Result<Self, JjError> {
        let _ = std::fs::symlink_metadata(&host_path).map_err(|_| JjError::NotFound)?;
        Ok(Self { host_path })
    }
}

#[async_trait]
impl VirtualFile for HostPassthroughNode {
    async fn read(&self) -> Result<Pin<Box<dyn futures::AsyncRead + Send>>, JjError> {
        let bytes = std::fs::read(&self.host_path)?;
        Ok(Box::pin(Cursor::new(bytes)))
    }

    async fn read_range(&self, offset: u64, size: u32) -> Result<Box<[u8]>, JjError> {
        pread_host_file(&self.host_path, offset, size)
    }

    async fn list(&self) -> Result<DirectoryStream, JjError> {
        let entries = list_host_dir(&self.host_path)?;
        Ok(Box::pin(futures::stream::iter(entries)))
    }

    async fn read_link(&self) -> Result<PathBuf, JjError> {
        Ok(std::fs::read_link(&self.host_path)?)
    }

    async fn attributes(&self) -> Result<FileAttributes, JjError> {
        host_attributes(&self.host_path)
    }

    async fn file_type(&self) -> Result<FileType, JjError> {
        Ok(self.attributes().await?.file_type)
    }
}

pub fn workspace_storage_id(key: &WorkspaceKey) -> String {
    let raw = format!(
        "{}\x1f{}\x1f{}\x1f{}",
        key.source, key.repo, key.user, key.workspace
    );
    // FNV-1a 128-bit digest for deterministic, filesystem-safe per-workspace
    // directory names
    let mut h1: u64 = 0xcbf29ce484222325;
    let mut h2: u64 = 0x84222325cbf29ce4;
    for &b in raw.as_bytes() {
        h1 ^= u64::from(b);
        h1 = h1.wrapping_mul(0x100000001b3);
        h2 = h2.wrapping_mul(0x100000001b3) ^ u64::from(b);
    }
    let safe_ws: String = key
        .workspace
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .take(24)
        .collect();
    format!("{safe_ws}_{h1:016x}{h2:016x}")
}

fn apply_journal_record(state: &mut OverlayMutableState, staged_dir: &Path, rec: JournalRecord) {
    match rec.op {
        JournalOp::Added | JournalOp::Modified | JournalOp::Chmod => {
            if std::fs::symlink_metadata(staged_dir.join(&rec.rel_path)).is_ok_and(|m| m.is_file())
            {
                state.deleted.remove(&rec.rel_path);
                state.dirty_symlinks.remove(&rec.rel_path);
                let exec = rec.executable.unwrap_or(false);
                state.dirty_files.insert(rec.rel_path, exec);
            }
        }
        JournalOp::Symlink => {
            if let Some(tgt) = rec.target
                && std::fs::symlink_metadata(staged_dir.join(&rec.rel_path))
                    .is_ok_and(|m| m.file_type().is_symlink())
            {
                state.deleted.remove(&rec.rel_path);
                state.dirty_files.remove(&rec.rel_path);
                state
                    .dirty_symlinks
                    .insert(rec.rel_path, PathBuf::from(tgt));
            }
        }
        JournalOp::Mkdir => {
            state.deleted.remove(&rec.rel_path);
            state.dirty_dirs.insert(rec.rel_path);
        }
        JournalOp::Deleted => {
            state.dirty_files.remove(&rec.rel_path);
            state.dirty_symlinks.remove(&rec.rel_path);
            state.deleted.insert(rec.rel_path);
        }
        JournalOp::Rmdir => {
            let prefix = format!("{}/", rec.rel_path);
            state.dirty_dirs.remove(&rec.rel_path);
            state.dirty_dirs.retain(|d| !d.starts_with(&prefix));
            state.dirty_files.retain(|k, _| !k.starts_with(&prefix));
            state.dirty_symlinks.retain(|k, _| !k.starts_with(&prefix));
            state.deleted.insert(rec.rel_path);
        }
    }
}

fn is_deleted_or_under_deleted(deleted: &BTreeSet<String>, rel_str: &str) -> bool {
    if rel_str.is_empty() {
        return false;
    }
    if deleted.contains(rel_str) {
        return true;
    }
    let mut cur = rel_str;
    while let Some((parent, _)) = cur.rsplit_once('/') {
        if deleted.contains(parent) {
            return true;
        }
        cur = parent;
    }
    false
}

fn normalize_rel_path(path: &Path) -> Result<String, JjError> {
    let mut parts = Vec::new();
    for comp in path.components() {
        match comp {
            std::path::Component::CurDir | std::path::Component::RootDir => {}
            std::path::Component::Normal(os) => {
                let s = os.to_str().ok_or(JjError::InvalidPath)?;
                if s.is_empty() || s.contains('\0') {
                    return Err(JjError::InvalidPath);
                }
                parts.push(s);
            }
            std::path::Component::ParentDir | std::path::Component::Prefix(_) => {
                return Err(JjError::InvalidPath);
            }
        }
    }
    Ok(parts.join("/"))
}

fn join_rel(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_string()
    } else {
        format!("{parent}/{name}")
    }
}

fn metadata_to_attributes(
    path: &Path,
    meta: &std::fs::Metadata,
    exec_override: Option<bool>,
) -> Result<FileAttributes, JjError> {
    let mtime = meta.modified().unwrap_or(UNIX_EPOCH);
    let ft = meta.file_type();
    if ft.is_symlink() {
        let tgt = std::fs::read_link(path)?;
        Ok(FileAttributes::symlink(tgt.as_os_str().len() as u64, mtime))
    } else if ft.is_dir() {
        Ok(FileAttributes::directory(mtime))
    } else {
        let exec = exec_override.unwrap_or_else(|| (meta.permissions().mode() & 0o111) != 0);
        Ok(FileAttributes::file(meta.len(), exec, mtime))
    }
}

fn host_attributes(path: &Path) -> Result<FileAttributes, JjError> {
    let meta = std::fs::symlink_metadata(path).map_err(|_| JjError::NotFound)?;
    metadata_to_attributes(path, &meta, None)
}

fn pread_host_file(path: &Path, offset: u64, size: u32) -> Result<Box<[u8]>, JjError> {
    let file = File::open(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            JjError::NotFound
        } else {
            JjError::IO(e)
        }
    })?;
    let len = file.metadata()?.len();
    if offset >= len || size == 0 {
        return Ok(Box::new([]));
    }
    let to_read = (len - offset).min(u64::from(size)) as usize;
    let mut buf = vec![0u8; to_read];
    let n = file.read_at(&mut buf, offset)?;
    buf.truncate(n);
    Ok(buf.into_boxed_slice())
}

fn list_host_dir(path: &Path) -> Result<Vec<DirectoryEntry>, JjError> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let smeta = std::fs::symlink_metadata(entry.path())?;
        let file_type = if smeta.file_type().is_symlink() {
            FileType::Symlink
        } else if smeta.is_dir() {
            FileType::Directory
        } else {
            FileType::File
        };
        out.push(DirectoryEntry { name, file_type });
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

fn join_jj_sub(jj_root: PathBuf, rel_str: &str) -> PathBuf {
    let sub = rel_str
        .strip_prefix(".jj")
        .unwrap_or("")
        .trim_start_matches('/');
    if sub.is_empty() {
        jj_root
    } else {
        jj_root.join(sub)
    }
}

fn current_dirty_record(state: &OverlayMutableState, rel_str: &str) -> Option<JournalRecord> {
    let timestamp_ms = now_ms();
    if let Some(&exec) = state.dirty_files.get(rel_str) {
        return Some(JournalRecord {
            op: JournalOp::Modified,
            rel_path: rel_str.to_string(),
            executable: Some(exec),
            target: None,
            timestamp_ms,
        });
    }
    if let Some(tgt) = state.dirty_symlinks.get(rel_str) {
        return tgt.to_str().map(|s| JournalRecord {
            op: JournalOp::Symlink,
            rel_path: rel_str.to_string(),
            executable: None,
            target: Some(s.to_string()),
            timestamp_ms,
        });
    }
    if state.dirty_dirs.contains(rel_str) {
        return Some(JournalRecord {
            op: JournalOp::Mkdir,
            rel_path: rel_str.to_string(),
            executable: None,
            target: None,
            timestamp_ms,
        });
    }
    if state.deleted.contains(rel_str) {
        return Some(JournalRecord {
            op: JournalOp::Deleted,
            rel_path: rel_str.to_string(),
            executable: None,
            target: None,
            timestamp_ms,
        });
    }
    None
}

fn prune_empty_dirs(root: &Path) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if std::fs::symlink_metadata(&path).is_ok_and(|m| m.is_dir()) {
            prune_empty_dirs(&path);
            let _ = std::fs::remove_dir(&path);
        }
    }
}

fn truncate_host_file(path: &Path) -> std::io::Result<()> {
    let f = OpenOptions::new().write(true).truncate(true).open(path)?;
    drop(f);
    Ok(())
}

fn set_host_times(path: &Path, changes: &AttributeChanges) -> std::io::Result<()> {
    if !changes.has_times() {
        return Ok(());
    }
    if std::fs::symlink_metadata(path)?.file_type().is_symlink() {
        return Ok(());
    }
    let mut times = std::fs::FileTimes::new();
    if let Some(atime) = changes.atime {
        times = times.set_accessed(atime);
    }
    if let Some(mtime) = changes.mtime {
        times = times.set_modified(mtime);
    }
    File::open(path)?.set_times(times)
}

fn set_exec_perm(path: &Path, executable: bool) -> std::io::Result<()> {
    let mode = if executable { 0o755 } else { 0o644 };
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod test_hooks {
    use std::cell::RefCell;

    use super::WorkspaceOverlay;

    type StagedWriteHook = Box<dyn FnOnce(&WorkspaceOverlay)>;

    thread_local! {
        static AFTER_STAGED_WRITE: RefCell<Option<StagedWriteHook>> = const { RefCell::new(None) };
    }

    pub fn set_once(hook: impl FnOnce(&WorkspaceOverlay) + 'static) {
        AFTER_STAGED_WRITE.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(hook));
        });
    }

    pub fn after_staged_write(overlay: &WorkspaceOverlay) {
        let hook = AFTER_STAGED_WRITE.with(|slot| slot.borrow_mut().take());
        if let Some(cb) = hook {
            cb(overlay);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use jj_lib::object_id::ObjectId as _;

    use super::*;
    use crate::backend::local::LocalRepoVfsBackend;
    use crate::cas::BlobCas;
    use crate::test_helpers::setup_test_repo;

    fn test_key(workspace: &str) -> WorkspaceKey {
        WorkspaceKey {
            source: "corp".into(),
            repo: "app".into(),
            user: "alice".into(),
            workspace: workspace.into(),
        }
    }

    #[tokio::test]
    async fn overlay_splits_gitignored_artifacts_and_recovers_journal_after_restart() {
        let (_ws_tmp, repo, cid) = setup_test_repo().await;
        let cas_tmp = tempfile::tempdir().unwrap();
        let cas = Arc::new(BlobCas::new(cas_tmp.path(), 0).unwrap());
        let backend: Arc<dyn ReadOnlyVfsBackend> = Arc::new(
            LocalRepoVfsBackend::from_readonly_repo("app", repo, None, cas),
        );
        let base_hex = cid.hex();

        let ovl_tmp = tempfile::tempdir().unwrap();
        let key = test_key("default");

        let ovl = WorkspaceOverlay::open_or_recover(ovl_tmp.path(), key.clone()).unwrap();

        // 1. Create `.gitignore` ignoring `target/` and `*.swp`
        ovl.create_file(&backend, &base_hex, Path::new(".gitignore"), false, true)
            .await
            .unwrap();
        ovl.write_at(
            &backend,
            &base_hex,
            Path::new(".gitignore"),
            0,
            b"target/\n*.swp\n",
        )
        .await
        .unwrap();

        // 2. Write ignored build outputs -> must go to `ignored/`, NOT
        //    `staged/` or `journal.log`!
        ovl.mkdir(&backend, &base_hex, Path::new("target"), 0o755)
            .await
            .unwrap();
        ovl.create_file(&backend, &base_hex, Path::new("target/out.o"), false, false)
            .await
            .unwrap();
        ovl.write_at(
            &backend,
            &base_hex,
            Path::new("target/out.o"),
            0,
            b"ELF_OBJ",
        )
        .await
        .unwrap();
        assert!(ovl.ignored_dir().join("target/out.o").is_file());
        assert!(!ovl.staged_dir().join("target/out.o").exists());

        // 3. Modify existing tracked `file1.txt`, chmod it executable, delete
        //    `dir/file2.txt`, and add symlink
        ovl.open_file(&backend, &base_hex, Path::new("file1.txt"), true, false)
            .await
            .unwrap();
        ovl.write_at(&backend, &base_hex, Path::new("file1.txt"), 6, b"OVERLAY!!")
            .await
            .unwrap();
        ovl.set_attributes(
            &backend,
            &base_hex,
            Path::new("file1.txt"),
            AttributeChanges {
                mode: Some(0o755),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        ovl.unlink(&backend, &base_hex, Path::new("dir/file2.txt"))
            .await
            .unwrap();
        ovl.symlink(
            &backend,
            &base_hex,
            Path::new("new_link"),
            Path::new("file1.txt"),
        )
        .await
        .unwrap();
        ovl.fsync(Path::new("file1.txt"), false).await.unwrap();

        let (_, dirty) = ovl.capture_dirty_snapshot();
        assert!(dirty.contains_key(".gitignore"));
        assert!(dirty.contains_key("file1.txt"));
        assert!(dirty.contains_key("dir/file2.txt"));
        assert!(dirty.contains_key("new_link"));
        assert!(!dirty.contains_key("target/out.o"));

        // 4. Simulate process crash/restart -> reopen from `journal.log` and
        //    verify full state!
        drop(ovl);
        let recovered = WorkspaceOverlay::open_or_recover(ovl_tmp.path(), key).unwrap();
        // Recovered dirty overlays must expose `last_mutation: Some(_)` so
        // auto-snapshot runs!
        let (rec_seq, rec_last_mut) = recovered.dirty_status().expect("expected dirty status");
        assert!(rec_seq > 0);
        assert!(rec_last_mut.is_some());

        let file1_attr = recovered
            .attributes_at(&backend, &base_hex, "file1.txt")
            .await
            .unwrap();
        assert!(file1_attr.executable);
        let file1_bytes = recovered
            .read_range_at(&backend, &base_hex, "file1.txt", 0, 32)
            .await
            .unwrap();
        assert_eq!(&*file1_bytes, b"hello OVERLAY!!");
        assert!(matches!(
            recovered
                .attributes_at(&backend, &base_hex, "dir/file2.txt")
                .await,
            Err(JjError::NotFound)
        ));
        assert_eq!(
            recovered
                .read_link_at(&backend, &base_hex, "new_link")
                .await
                .unwrap(),
            PathBuf::from("file1.txt")
        );
        // Ignored file also remains readable across restart and after snapshot
        // completion!
        let (seq, _) = recovered.capture_dirty_snapshot();
        assert!(recovered.complete_snapshot(seq).unwrap());
        assert!(!recovered.has_dirty_tracked_paths());
        assert_eq!(
            &*recovered
                .read_range_at(&backend, &base_hex, "target/out.o", 0, 16)
                .await
                .unwrap(),
            b"ELF_OBJ"
        );
    }

    #[tokio::test]
    async fn rename_preserves_source_on_error_streams_blobs_and_handles_directories() {
        use jj_lib::backend::CopyId;
        use jj_lib::backend::TreeValue;
        use jj_lib::merged_tree::MergedTree;
        use jj_lib::repo::Repo as _;
        use jj_lib::tree_builder::TreeBuilder;

        let (_ws_tmp, repo, cid) = setup_test_repo().await;
        // Add a multi-chunk (> COPY_CHUNK_BYTES) blob to the base commit to
        // verify streaming copy
        let store = repo.store().clone();
        let c1 = store.get_commit(&cid).unwrap();
        let tid = c1.tree_ids().as_resolved().unwrap().clone();
        let mut tb = TreeBuilder::new(Arc::clone(&store), tid);
        let big_len = (COPY_CHUNK_BYTES as usize) * 2 + 12345;
        let big_payload: Vec<u8> = (0..big_len).map(|i| (i % 251) as u8).collect();
        let big_rp = RepoPathBuf::from_internal_string("big.bin").unwrap();
        let big_fid = store
            .write_file(&big_rp, &mut &big_payload[..])
            .await
            .unwrap();
        tb.set(
            big_rp,
            TreeValue::File {
                id: big_fid,
                executable: true,
                copy_id: CopyId::placeholder(),
            },
        );
        let tid2 = tb.write_tree().await.unwrap();
        let mut tx = repo.start_transaction();
        let c2 = tx
            .repo_mut()
            .new_commit(vec![cid], MergedTree::resolved(Arc::clone(&store), tid2))
            .set_description("with big blob")
            .write()
            .await
            .unwrap();
        let repo2 = tx.commit("big blob").await.unwrap();

        let cas_tmp = tempfile::tempdir().unwrap();
        let cas = Arc::new(BlobCas::new(cas_tmp.path(), 0).unwrap());
        let backend: Arc<dyn ReadOnlyVfsBackend> = Arc::new(
            LocalRepoVfsBackend::from_readonly_repo("app", repo2, None, cas),
        );
        let base_hex = c2.id().hex();

        let ovl_tmp = tempfile::tempdir().unwrap();
        let key = test_key("rename-ws");
        let ovl = WorkspaceOverlay::open_or_recover(ovl_tmp.path(), key.clone()).unwrap();

        // 1. Renaming a file onto an existing directory fails with NotAFile
        //    (EISDIR) WITHOUT losing the source file!
        ovl.create_file(&backend, &base_hex, Path::new("save.tmp"), false, true)
            .await
            .unwrap();
        ovl.write_at(
            &backend,
            &base_hex,
            Path::new("save.tmp"),
            0,
            b"precious bytes",
        )
        .await
        .unwrap();
        let err = ovl
            .rename(&backend, &base_hex, Path::new("save.tmp"), Path::new("dir"))
            .await;
        assert!(matches!(err, Err(JjError::NotAFile)));
        assert_eq!(
            &*ovl
                .read_range_at(&backend, &base_hex, "save.tmp", 0, 64)
                .await
                .unwrap(),
            b"precious bytes"
        );

        // 2. Atomic editor save over existing base file (`rename(save.tmp,
        //    file1.txt)`)
        ovl.rename(
            &backend,
            &base_hex,
            Path::new("save.tmp"),
            Path::new("file1.txt"),
        )
        .await
        .unwrap();
        assert!(matches!(
            ovl.attributes_at(&backend, &base_hex, "save.tmp").await,
            Err(JjError::NotFound)
        ));
        assert_eq!(
            &*ovl
                .read_range_at(&backend, &base_hex, "file1.txt", 0, 64)
                .await
                .unwrap(),
            b"precious bytes"
        );

        // 3. Multi-chunk base blob rename (`big.bin` -> `big_moved.bin`)
        //    streams full payload
        ovl.rename(
            &backend,
            &base_hex,
            Path::new("big.bin"),
            Path::new("big_moved.bin"),
        )
        .await
        .unwrap();
        let moved_attr = ovl
            .attributes_at(&backend, &base_hex, "big_moved.bin")
            .await
            .unwrap();
        assert_eq!(moved_attr.size, big_len as u64);
        assert!(moved_attr.executable);
        let moved_bytes = ovl
            .read_range_at(&backend, &base_hex, "big_moved.bin", 0, big_len as u32)
            .await
            .unwrap();
        assert_eq!(&*moved_bytes, &big_payload[..]);

        // 4. Base-commit directory rename returns CrossDevice (EXDEV) for mv
        //    fallback
        let dir_err = ovl
            .rename(
                &backend,
                &base_hex,
                Path::new("dir"),
                Path::new("dir_moved"),
            )
            .await;
        assert!(matches!(dir_err, Err(JjError::CrossDevice)));
        assert_eq!(
            &*ovl
                .read_range_at(&backend, &base_hex, "dir/file2.txt", 0, 32)
                .await
                .unwrap(),
            b"hello content 2"
        );

        // 5. Staged directory rename (with nested file, symlink, and ignored
        //    twin) succeeds and recovers cleanly across journal replay!
        ovl.create_file(&backend, &base_hex, Path::new(".gitignore"), false, true)
            .await
            .unwrap();
        ovl.write_at(&backend, &base_hex, Path::new(".gitignore"), 0, b"*.o\n")
            .await
            .unwrap();
        ovl.mkdir(&backend, &base_hex, Path::new("pkg"), 0o755)
            .await
            .unwrap();
        ovl.create_file(&backend, &base_hex, Path::new("pkg/mod.rs"), false, true)
            .await
            .unwrap();
        ovl.write_at(
            &backend,
            &base_hex,
            Path::new("pkg/mod.rs"),
            0,
            b"pub mod sub;\n",
        )
        .await
        .unwrap();
        ovl.symlink(
            &backend,
            &base_hex,
            Path::new("pkg/mod_link"),
            Path::new("mod.rs"),
        )
        .await
        .unwrap();
        ovl.create_file(&backend, &base_hex, Path::new("pkg/cache.o"), false, true)
            .await
            .unwrap();
        ovl.write_at(&backend, &base_hex, Path::new("pkg/cache.o"), 0, b"OBJ")
            .await
            .unwrap();

        ovl.rename(&backend, &base_hex, Path::new("pkg"), Path::new("pkg_v2"))
            .await
            .unwrap();
        assert!(matches!(
            ovl.attributes_at(&backend, &base_hex, "pkg").await,
            Err(JjError::NotFound)
        ));
        assert_eq!(
            &*ovl
                .read_range_at(&backend, &base_hex, "pkg_v2/mod.rs", 0, 64)
                .await
                .unwrap(),
            b"pub mod sub;\n"
        );
        assert_eq!(
            &*ovl
                .read_range_at(&backend, &base_hex, "pkg_v2/cache.o", 0, 16)
                .await
                .unwrap(),
            b"OBJ"
        );

        drop(ovl);
        let recovered = WorkspaceOverlay::open_or_recover(ovl_tmp.path(), key).unwrap();
        assert_eq!(
            &*recovered
                .read_range_at(&backend, &base_hex, "pkg_v2/mod.rs", 0, 64)
                .await
                .unwrap(),
            b"pub mod sub;\n"
        );
        assert_eq!(
            recovered
                .read_link_at(&backend, &base_hex, "pkg_v2/mod_link")
                .await
                .unwrap(),
            PathBuf::from("mod.rs")
        );
        assert_eq!(
            &*recovered
                .read_range_at(&backend, &base_hex, "file1.txt", 0, 64)
                .await
                .unwrap(),
            b"precious bytes"
        );
    }

    #[tokio::test]
    async fn chmod_on_directories_and_rmdir_with_ignored_twin_succeed() {
        let (_ws_tmp, repo, cid) = setup_test_repo().await;
        let cas_tmp = tempfile::tempdir().unwrap();
        let cas = Arc::new(BlobCas::new(cas_tmp.path(), 0).unwrap());
        let backend: Arc<dyn ReadOnlyVfsBackend> = Arc::new(
            LocalRepoVfsBackend::from_readonly_repo("app", repo, None, cas),
        );
        let base_hex = cid.hex();

        let ovl_tmp = tempfile::tempdir().unwrap();
        let ovl = WorkspaceOverlay::open_or_recover(ovl_tmp.path(), test_key("dirs")).unwrap();

        // 1. chmod + utimes on base-commit directory `dir` and newly-created
        //    `mkdir` directory
        let base_dir_attr = ovl
            .set_attributes(
                &backend,
                &base_hex,
                Path::new("dir"),
                AttributeChanges {
                    mode: Some(0o755),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(base_dir_attr.file_type, FileType::Directory);

        ovl.mkdir(&backend, &base_hex, Path::new("extracted"), 0o755)
            .await
            .unwrap();
        let fixed_mtime = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let new_dir_attr = ovl
            .set_attributes(
                &backend,
                &base_hex,
                Path::new("extracted"),
                AttributeChanges {
                    mode: Some(0o700),
                    mtime: Some(fixed_mtime),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(new_dir_attr.file_type, FileType::Directory);
        assert_eq!(new_dir_attr.modified, fixed_mtime);

        // 2. Create an ignored child under tracked `dir`
        //    (`dir/__pycache__/mod.pyc`), remove child and `__pycache__`, then
        //    `rmdir dir` -> must NOT leave phantom `dir`!
        ovl.create_file(&backend, &base_hex, Path::new(".gitignore"), false, true)
            .await
            .unwrap();
        ovl.write_at(
            &backend,
            &base_hex,
            Path::new(".gitignore"),
            0,
            b"__pycache__/\n",
        )
        .await
        .unwrap();
        ovl.mkdir(&backend, &base_hex, Path::new("dir/__pycache__"), 0o755)
            .await
            .unwrap();
        ovl.create_file(
            &backend,
            &base_hex,
            Path::new("dir/__pycache__/m.pyc"),
            false,
            true,
        )
        .await
        .unwrap();
        assert!(ovl.ignored_dir().join("dir").is_dir());

        ovl.unlink(&backend, &base_hex, Path::new("dir/__pycache__/m.pyc"))
            .await
            .unwrap();
        ovl.rmdir(&backend, &base_hex, Path::new("dir/__pycache__"))
            .await
            .unwrap();
        ovl.unlink(&backend, &base_hex, Path::new("dir/file2.txt"))
            .await
            .unwrap();
        ovl.rmdir(&backend, &base_hex, Path::new("dir"))
            .await
            .unwrap();

        assert!(matches!(
            ovl.attributes_at(&backend, &base_hex, "dir").await,
            Err(JjError::NotFound)
        ));
        let root_entries = ovl.list_dir_at(&backend, &base_hex, "").await.unwrap();
        assert!(!root_entries.iter().any(|e| e.name == "dir"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn complete_snapshot_never_loses_inflight_or_concurrent_writes() {
        let (_ws_tmp, repo, cid) = setup_test_repo().await;
        let cas_tmp = tempfile::tempdir().unwrap();
        let cas = Arc::new(BlobCas::new(cas_tmp.path(), 0).unwrap());
        let backend: Arc<dyn ReadOnlyVfsBackend> = Arc::new(
            LocalRepoVfsBackend::from_readonly_repo("app", repo, None, cas),
        );
        let base_hex = cid.hex();

        let ovl_tmp = tempfile::tempdir().unwrap();
        let ovl = WorkspaceOverlay::open_or_recover(ovl_tmp.path(), test_key("race")).unwrap();

        // Seed initial dirty file (`seq_v1`)
        ovl.open_file(&backend, &base_hex, Path::new("file1.txt"), true, true)
            .await
            .unwrap();
        ovl.write_at(&backend, &base_hex, Path::new("file1.txt"), 0, b"v1")
            .await
            .unwrap();
        let (seq_v1, _) = ovl.capture_dirty_snapshot();

        // 1. Deterministic hook: run `complete_snapshot` right after
        //    `write_all_at` and BEFORE `append_journal` acquires
        //    `state.write()`. Both `seq_v1` and a mid-mutation capture must be
        //    rejected!
        test_hooks::set_once(move |in_flight_ovl| {
            assert!(!in_flight_ovl.complete_snapshot(seq_v1).unwrap());
            let (mid_seq, _) = in_flight_ovl.capture_dirty_snapshot();
            assert!(!in_flight_ovl.complete_snapshot(mid_seq).unwrap());
        });
        ovl.write_at(
            &backend,
            &base_hex,
            Path::new("file1.txt"),
            0,
            b"v2-preserved",
        )
        .await
        .unwrap();

        assert!(ovl.has_dirty_tracked_paths());
        assert_eq!(
            &*ovl
                .read_range_at(&backend, &base_hex, "file1.txt", 0, 64)
                .await
                .unwrap(),
            b"v2-preserved"
        );

        // 2. Concurrent multi-task writers + realistic snapshotter stress loop:
        //    Whenever `complete_snapshot` returns `Ok(true)`, record the exact
        //    bytes the snapshot read before completing. At the end, the final
        //    acknowledged write MUST either still be dirty in `staged/` or have
        //    been the exact payload uploaded by the last completed snapshot!
        let writer_ovl = Arc::clone(&ovl);
        let writer_backend = Arc::clone(&backend);
        let writer_base = base_hex.clone();
        let snap_ovl = Arc::clone(&ovl);
        let last_uploaded: Arc<parking_lot::Mutex<Option<Vec<u8>>>> =
            Arc::new(parking_lot::Mutex::new(None));
        let last_uploaded_snap = Arc::clone(&last_uploaded);

        let snap_task = tokio::spawn(async move {
            for _ in 0..250 {
                let (seq, dirty) = snap_ovl.capture_dirty_snapshot();
                let uploaded_bytes = match dirty.get("file1.txt") {
                    Some(DirtyEntryState::File { staged_path, .. }) => {
                        std::fs::read(staged_path).ok()
                    }
                    _ => None,
                };
                if let Some(bytes) = uploaded_bytes
                    && snap_ovl.complete_snapshot(seq).unwrap()
                {
                    *last_uploaded_snap.lock() = Some(bytes);
                }
                tokio::task::yield_now().await;
            }
        });

        let final_expected = "iter-0199-acknowledged";
        let write_task = tokio::spawn(async move {
            for i in 0..200u32 {
                let payload = format!("iter-{i:04}-acknowledged");
                writer_ovl
                    .write_at(
                        &writer_backend,
                        &writer_base,
                        Path::new("file1.txt"),
                        0,
                        payload.as_bytes(),
                    )
                    .await
                    .unwrap();
                if i % 5 == 0 {
                    writer_ovl
                        .set_attributes(
                            &writer_backend,
                            &writer_base,
                            Path::new("file1.txt"),
                            AttributeChanges {
                                mode: Some(if i % 10 == 0 { 0o755 } else { 0o644 }),
                                ..Default::default()
                            },
                        )
                        .await
                        .unwrap();
                }
            }
        });

        write_task.await.unwrap();
        snap_task.await.unwrap();

        if ovl.has_dirty_tracked_paths() {
            assert_eq!(
                &*ovl
                    .read_range_at(&backend, &base_hex, "file1.txt", 0, 64)
                    .await
                    .unwrap(),
                final_expected.as_bytes()
            );
        } else {
            assert_eq!(
                last_uploaded.lock().as_deref(),
                Some(final_expected.as_bytes()),
                "final acknowledged write was lost by a concurrent complete_snapshot!"
            );
        }
    }

    #[tokio::test]
    async fn read_only_jj_binding_rejects_mutations_and_manager_deduplicates_concurrent_opens() {
        let (_ws_tmp, repo, cid) = setup_test_repo().await;
        let cas_tmp = tempfile::tempdir().unwrap();
        let cas = Arc::new(BlobCas::new(cas_tmp.path(), 0).unwrap());
        let backend: Arc<dyn ReadOnlyVfsBackend> = Arc::new(
            LocalRepoVfsBackend::from_readonly_repo("app", repo, None, cas),
        );
        let base_hex = cid.hex();

        // 1. Concurrent `OverlayManager::get_or_open` returns a single shared
        //    `WorkspaceOverlay`
        let mgr_tmp = tempfile::tempdir().unwrap();
        let mgr = Arc::new(OverlayManager::new(mgr_tmp.path()).unwrap());
        let key = test_key("shared");
        let barrier = Arc::new(std::sync::Barrier::new(12));
        let mut handles = Vec::new();
        for _ in 0..12 {
            let m = Arc::clone(&mgr);
            let k = key.clone();
            let b = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                b.wait();
                m.get_or_open(&k).unwrap()
            }));
        }
        let opened: Vec<Arc<WorkspaceOverlay>> =
            handles.into_iter().map(|h| h.join().unwrap()).collect();
        for other in &opened[1..] {
            assert!(Arc::ptr_eq(&opened[0], other));
        }

        // 2. Read-only `.jj` binding (local-jj) serves reads and rejects
        //    mutations with ReadOnly
        let host_jj = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(host_jj.path().join("working_copy")).unwrap();
        std::fs::write(host_jj.path().join("working_copy/type"), b"local").unwrap();

        let ovl = &opened[0];
        ovl.set_read_only_jj_dir(host_jj.path().to_path_buf());
        assert_eq!(
            &*ovl
                .read_range_at(&backend, &base_hex, ".jj/working_copy/type", 0, 16)
                .await
                .unwrap(),
            b"local"
        );
        assert!(matches!(
            ovl.open_file(
                &backend,
                &base_hex,
                Path::new(".jj/working_copy/type"),
                true,
                false
            )
            .await,
            Err(JjError::ReadOnly)
        ));
        assert!(matches!(
            ovl.create_file(&backend, &base_hex, Path::new(".jj/lock"), false, false)
                .await,
            Err(JjError::ReadOnly)
        ));
        assert!(matches!(
            ovl.write_at(
                &backend,
                &base_hex,
                Path::new(".jj/working_copy/type"),
                0,
                b"vfs"
            )
            .await,
            Err(JjError::ReadOnly)
        ));
        assert!(matches!(
            ovl.unlink(&backend, &base_hex, Path::new(".jj/working_copy/type"))
                .await,
            Err(JjError::ReadOnly)
        ));
        assert!(matches!(
            ovl.rename(
                &backend,
                &base_hex,
                Path::new(".jj/working_copy/type"),
                Path::new(".jj/working_copy/type2"),
            )
            .await,
            Err(JjError::ReadOnly)
        ));

        // 3. Writable synthesized `.jj` binding allows writes, renames, and
        //    fsync
        let synth_jj = tempfile::tempdir().unwrap();
        ovl.set_synthesized_jj_dir(Some(synth_jj.path().to_path_buf()));
        ovl.create_file(&backend, &base_hex, Path::new(".jj/state.tmp"), false, true)
            .await
            .unwrap();
        ovl.write_at(&backend, &base_hex, Path::new(".jj/state.tmp"), 0, b"{}")
            .await
            .unwrap();
        ovl.fsync(Path::new(".jj/state.tmp"), true).await.unwrap();
        ovl.rename(
            &backend,
            &base_hex,
            Path::new(".jj/state.tmp"),
            Path::new(".jj/state.json"),
        )
        .await
        .unwrap();
        assert_eq!(
            std::fs::read(synth_jj.path().join("state.json")).unwrap(),
            b"{}"
        );

        // 4. `gitignore_cache` stays keyed per directory across advancing base
        //    commits and caps total entries at `MAX_GITIGNORE_CACHE_ENTRIES`.
        for epoch in 0..20 {
            let fake_commit = format!("{epoch:040x}");
            let _ = ovl
                .is_path_ignored(&backend, &fake_commit, "dir/new.txt", false)
                .await
                .unwrap();
        }
        assert_eq!(ovl.gitignore_cache.len(), 2);
    }
}
