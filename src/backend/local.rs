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

//! Local `jj_lib` and `gitoxide` (`gix`) implementation of
//! `ReadOnlyVfsBackend`.
//!
//! # Authority and Crash-Safety Ordering
//! Opens existing local `jj` workspaces (`Workspace::load`) or bare/non-bare
//! Git repositories (`gix::discover`) read-only without mutating `.jj/` or
//! `.git/`. Clean blobs inflate once into `BlobCas` via atomic rename so
//! subsequent FUSE ranged reads execute native `pread` syscalls in $O(1)$.
//!
//! # Concurrency and Merge Invariants
//! Each backend keeps one loaded `ReadonlyRepo` (sharing its commit index and
//! `Store` caches across FUSE operations) and reloads it only when the
//! operation heads change, which costs one `readdir` of `op_heads/heads` per
//! call. Plain Git repositories are mirrored into a `jj` repo under
//! `git-mirrors/<name>-<hash of the canonical Git dir>`, which re-imports Git
//! refs when the ref files change (polled at most once per second).
//! One `gix::ThreadSafeRepository` per backend answers blob-size header
//! queries.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::hash::DefaultHasher;
use std::hash::Hash as _;
use std::hash::Hasher as _;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use dashmap::DashSet;
use futures::AsyncReadExt as _;
use gix::prelude::HeaderExt as _;
use jj_lib::backend::CommitId;
use jj_lib::backend::FileId;
use jj_lib::backend::TreeId;
use jj_lib::backend::TreeValue;
use jj_lib::config::ConfigLayer;
use jj_lib::config::ConfigSource;
use jj_lib::config::StackedConfig;
use jj_lib::merge::Merge;
use jj_lib::merged_tree::MergedTree;
use jj_lib::object_id::HexPrefix;
use jj_lib::object_id::ObjectId as _;
use jj_lib::object_id::PrefixResolution;
use jj_lib::repo::ReadonlyRepo;
use jj_lib::repo::Repo as _;
use jj_lib::repo::RepoLoader;
use jj_lib::repo::StoreFactories;
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::settings::UserSettings;
use jj_lib::workspace::Workspace;
use jj_lib::workspace::default_working_copy_factories;
use parking_lot::Mutex;
use parking_lot::RwLock;

use crate::backend::ReadOnlyVfsBackend;
use crate::backend::VfsLookup;
use crate::backend::VfsWorkspaceRef;
use crate::backend::normalize_change_prefix_to_hex;
use crate::backend::slice_range;
use crate::cas::BlobCas;
use crate::jj_error::JjError;
use crate::virtual_file::DirectoryEntry;
use crate::virtual_file::FileAttributes;
use crate::virtual_file::FileType;

/// Minimum interval between scans of a mirrored Git repository's ref files.
const GIT_REFS_POLL_INTERVAL: Duration = Duration::from_secs(1);

pub struct LocalRepoVfsBackend {
    name: String,
    workspace_root: Option<PathBuf>,
    git_repo: Option<gix::ThreadSafeRepository>,
    git_mirror: Option<GitMirror>,
    cas: Arc<BlobCas>,
    pinned_commits: DashSet<String>,
    fixed_repo: Option<Arc<ReadonlyRepo>>,
    loader: OnceLock<RepoLoader>,
    cached_repo: RwLock<Option<Arc<ReadonlyRepo>>>,
    reload_lock: tokio::sync::Mutex<()>,
}

/// A plain Git repository mirrored into a `jj` repo under the cache directory.
struct GitMirror {
    mirror_dir: PathBuf,
    git_dir: PathBuf,
    common_dir: PathBuf,
    poll_interval: Duration,
    refs: Mutex<GitRefsPoll>,
}

#[derive(Default)]
struct GitRefsPoll {
    checked_at: Option<Instant>,
    observed: Option<u64>,
    imported: Option<u64>,
}

impl GitMirror {
    fn refs_stamp(&self) -> u64 {
        git_refs_stamp(&self.git_dir, &self.common_dir)
    }

    /// Reports whether the source Git refs changed since the last import,
    /// rescanning the ref files at most once per `poll_interval`.
    fn refs_changed(&self) -> bool {
        {
            let poll = self.refs.lock();
            if let Some(at) = poll.checked_at
                && at.elapsed() < self.poll_interval
            {
                return poll.observed != poll.imported;
            }
        }
        let stamp = self.refs_stamp();
        let mut poll = self.refs.lock();
        poll.checked_at = Some(Instant::now());
        poll.observed = Some(stamp);
        poll.observed != poll.imported
    }

    fn imported_stamp(&self) -> Option<u64> {
        self.refs.lock().imported
    }

    fn record_import(&self, stamp: u64) {
        let mut poll = self.refs.lock();
        poll.checked_at = Some(Instant::now());
        poll.observed = Some(stamp);
        poll.imported = Some(stamp);
    }
}

impl LocalRepoVfsBackend {
    fn new_inner(
        name: String,
        workspace_root: Option<PathBuf>,
        git_repo: Option<gix::ThreadSafeRepository>,
        cas: Arc<BlobCas>,
    ) -> Self {
        Self {
            name,
            workspace_root,
            git_repo,
            git_mirror: None,
            cas,
            pinned_commits: DashSet::new(),
            fixed_repo: None,
            loader: OnceLock::new(),
            cached_repo: RwLock::new(None),
            reload_lock: tokio::sync::Mutex::new(()),
        }
    }

    /// Opens a local `jj` workspace. The repository loads on first use and
    /// reloads only when its operation heads change.
    pub fn open_jj_workspace(
        name: impl Into<String>,
        workspace_root: impl Into<PathBuf>,
        cas: Arc<BlobCas>,
    ) -> Result<Self, JjError> {
        let root = workspace_root.into();
        let git_repo = discover_git_dir(&root).as_deref().and_then(open_git_handle);
        // Validate that the workspace loads cleanly upfront
        let loader = load_repo_loader(&root)?;
        let backend = Self::new_inner(name.into(), Some(root), git_repo, cas);
        let _ = backend.loader.set(loader);
        Ok(backend)
    }

    /// Wraps an in-memory or pre-loaded `Arc<ReadonlyRepo>` (ideal for unit
    /// tests).
    pub fn from_readonly_repo(
        name: impl Into<String>,
        repo: Arc<ReadonlyRepo>,
        git_dir: Option<PathBuf>,
        cas: Arc<BlobCas>,
    ) -> Self {
        let git_repo = git_dir.as_deref().and_then(open_git_handle);
        let mut backend = Self::new_inner(name.into(), None, git_repo, cas);
        seed_pinned_from_repo(&repo, &backend.pinned_commits);
        backend.fixed_repo = Some(repo);
        backend
    }

    /// Opens a plain bare or non-bare Git repository via `gitoxide` (`gix`)
    /// when no `.jj/` directory exists yet, importing refs into a `jj` mirror
    /// under `mirror_root/git-mirrors/` keyed by the canonical Git directory.
    /// The mirror is created on first use and re-imports refs whenever the
    /// source refs change.
    pub fn open_git_repo(
        name: impl Into<String>,
        git_path: impl Into<PathBuf>,
        mirror_root: &Path,
        cas: Arc<BlobCas>,
    ) -> Result<Self, JjError> {
        let raw_path = git_path.into();
        let gix_repo = gix::discover(&raw_path)
            .map_err(|e| std::io::Error::other(format!("failed to open Git repo: {e}")))?;
        let canonical_git_dir = canonicalize_or_keep(gix_repo.git_dir());
        let common_dir = canonicalize_or_keep(gix_repo.common_dir());
        let repo_name = name.into();
        let mirror_dir = mirror_root
            .join("git-mirrors")
            .join(git_mirror_dir_name(&repo_name, &canonical_git_dir));
        let mut backend = Self::new_inner(
            repo_name,
            Some(mirror_dir.clone()),
            Some(gix_repo.into_sync()),
            cas,
        );
        backend.git_mirror = Some(GitMirror {
            mirror_dir,
            git_dir: canonical_git_dir,
            common_dir,
            poll_interval: GIT_REFS_POLL_INTERVAL,
            refs: Mutex::new(GitRefsPoll::default()),
        });
        Ok(backend)
    }

    /// Returns the repository at its current operation head, reusing the cached
    /// load while the operation heads (and, for Git mirrors, the source refs)
    /// are unchanged.
    async fn current_repo(&self) -> Result<Arc<ReadonlyRepo>, JjError> {
        if let Some(repo) = &self.fixed_repo {
            return Ok(Arc::clone(repo));
        }
        if let Some(repo) = self.fresh_cached_repo().await? {
            return Ok(repo);
        }
        // One task reloads while concurrent FUSE operations wait for its
        // result.
        let _reload = self.reload_lock.lock().await;
        if let Some(repo) = self.fresh_cached_repo().await? {
            return Ok(repo);
        }
        let repo = match &self.git_mirror {
            Some(mirror) => self.refresh_git_mirror(mirror).await?,
            None => {
                let loader = self.loader.get().cloned().ok_or(JjError::NotFound)?;
                run_blocking(move || futures::executor::block_on(load_repo_at_head(&loader)))
                    .await?
            }
        };
        seed_pinned_from_repo(&repo, &self.pinned_commits);
        *self.cached_repo.write() = Some(Arc::clone(&repo));
        Ok(repo)
    }

    async fn fresh_cached_repo(&self) -> Result<Option<Arc<ReadonlyRepo>>, JjError> {
        let Some(repo) = self.cached_repo.read().clone() else {
            return Ok(None);
        };
        if let Some(mirror) = &self.git_mirror
            && mirror.refs_changed()
        {
            return Ok(None);
        }
        let op_heads =
            repo.op_heads_store().get_op_heads().await.map_err(|e| {
                std::io::Error::other(format!("failed to read operation heads: {e}"))
            })?;
        Ok((op_heads.as_slice() == std::slice::from_ref(repo.op_id())).then_some(repo))
    }

    /// Creates the Git mirror on first use and re-imports the source Git refs
    /// when they changed since the last import (or since daemon start).
    async fn refresh_git_mirror(&self, mirror: &GitMirror) -> Result<Arc<ReadonlyRepo>, JjError> {
        let stamp = mirror.refs_stamp();
        let import = mirror.imported_stamp() != Some(stamp);
        let loader = self.loader.get().cloned();
        let mirror_dir = mirror.mirror_dir.clone();
        let git_dir = mirror.git_dir.clone();
        let (loader, repo) = run_blocking(move || {
            futures::executor::block_on(sync_git_mirror(loader, &mirror_dir, &git_dir, import))
        })
        .await?;
        let _ = self.loader.set(loader);
        mirror.record_import(stamp);
        Ok(repo)
    }

    fn cas_blob_key(&self, file_id: &FileId) -> String {
        format!("local_{}_{}", self.name, file_id.hex())
    }

    /// Queries exact uncompressed blob size from Git's loose/pack object header
    /// via `gix` in $O(1)$ without inflating or streaming the file body!
    fn lookup_git_header_size(&self, file_id: &FileId) -> Option<u64> {
        let cas_key = self.cas_blob_key(file_id);
        if let Some(sz) = self.cas.blob_size(&cas_key) {
            return Some(sz);
        }
        let gix_repo = self.git_repo.as_ref()?.to_thread_local();
        let oid = gix::ObjectId::try_from(file_id.as_bytes()).ok()?;
        let header = gix_repo.objects.header(oid).ok()?;
        Some(header.size())
    }

    async fn hydrate_into_cas(
        &self,
        repo: &ReadonlyRepo,
        repo_path: &RepoPathBuf,
        file_id: &FileId,
    ) -> Result<(String, u64), JjError> {
        let cas_key = self.cas_blob_key(file_id);
        if let Some(sz) = self.cas.blob_size(&cas_key) {
            return Ok((cas_key, sz));
        }
        let buf = read_store_file(repo, repo_path, file_id).await?;
        let sz = self.cas.put_blob(&cas_key, &buf)?;
        Ok((cas_key, sz))
    }
}

async fn read_store_file(
    repo: &ReadonlyRepo,
    repo_path: &RepoPathBuf,
    file_id: &FileId,
) -> Result<Vec<u8>, JjError> {
    let mut reader = repo.store().read_file(repo_path, file_id).await?;
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    Ok(buf)
}

fn default_local_settings() -> Result<UserSettings, JjError> {
    let cfg_text = r#"
        user.name = "jj-vfs"
        user.email = "jj-vfs@localhost"
    "#;
    let mut config = StackedConfig::with_defaults();
    let layer = ConfigLayer::parse(ConfigSource::User, cfg_text)
        .map_err(|e| std::io::Error::other(format!("config parse error: {e}")))?;
    config.add_layer(layer);
    UserSettings::from_config(config)
        .map_err(|e| std::io::Error::other(format!("settings error: {e}")).into())
}

fn load_repo_loader(workspace_root: &Path) -> Result<RepoLoader, JjError> {
    let settings = default_local_settings()?;
    let ws = Workspace::load(
        &settings,
        workspace_root,
        &StoreFactories::default(),
        &default_working_copy_factories(),
    )
    .map_err(|e| std::io::Error::other(format!("failed to load jj workspace: {e:?}")))?;
    Ok(ws.repo_loader().clone())
}

async fn load_repo_at_head(loader: &RepoLoader) -> Result<Arc<ReadonlyRepo>, JjError> {
    loader
        .load_at_head()
        .await
        .map_err(|e| std::io::Error::other(format!("failed to load repo at head: {e:?}")).into())
}

/// Runs a repository load or Git import on the blocking pool: jj-lib does
/// synchronous file I/O there, and its transaction futures are not `Send`.
async fn run_blocking<T: Send + 'static>(
    task: impl FnOnce() -> Result<T, JjError> + Send + 'static,
) -> Result<T, JjError> {
    tokio::task::spawn_blocking(task)
        .await
        .map_err(|e| std::io::Error::other(format!("repository load task failed: {e}")))?
}

/// Opens the `jj` mirror of a Git repository (creating it on first use) and,
/// when `import` is set or the mirror is new, imports the Git refs into it.
async fn sync_git_mirror(
    loader: Option<RepoLoader>,
    mirror_dir: &Path,
    git_dir: &Path,
    import: bool,
) -> Result<(RepoLoader, Arc<ReadonlyRepo>), JjError> {
    let (loader, repo, import) = match loader {
        Some(loader) => {
            let repo = load_repo_at_head(&loader).await?;
            (loader, repo, import)
        }
        None if mirror_dir.join(".jj").exists() => {
            let loader = load_repo_loader(mirror_dir)?;
            let repo = load_repo_at_head(&loader).await?;
            (loader, repo, import)
        }
        None => {
            std::fs::create_dir_all(mirror_dir)?;
            let settings = default_local_settings()?;
            let (ws, repo) = Workspace::init_external_git(&settings, mirror_dir, git_dir)
                .await
                .map_err(|e| std::io::Error::other(format!("init_external_git failed: {e:?}")))?;
            (ws.repo_loader().clone(), repo, true)
        }
    };
    if !import {
        return Ok((loader, repo));
    }
    let import_options = jj_lib::git::GitImportOptions {
        abandon_unreachable_commits: false,
        record_synthetic_predecessors: false,
        remote_auto_track_bookmarks: std::collections::HashMap::new(),
    };
    let mut tx = repo.start_transaction();
    jj_lib::git::import_refs(tx.repo_mut(), &import_options)
        .await
        .map_err(|e| std::io::Error::other(format!("git import_refs failed: {e:?}")))?;
    let repo = if tx.repo().has_changes() {
        tx.commit("import git refs")
            .await
            .map_err(|e| std::io::Error::other(format!("commit git refs failed: {e:?}")))?
    } else {
        repo
    };
    Ok((loader, repo))
}

fn canonicalize_or_keep(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

/// Opens the Git object database once per backend for blob-size header queries.
fn open_git_handle(git_dir: &Path) -> Option<gix::ThreadSafeRepository> {
    gix::open(git_dir).ok().map(gix::Repository::into_sync)
}

/// Names a Git mirror directory `<name>-<FNV-1a hash of the canonical Git
/// dir>`, so two repositories with the same name never share a mirror. The hash
/// must stay stable across releases because mirrors persist on disk, so this
/// does not use `DefaultHasher`.
fn git_mirror_dir_name(repo_name: &str, canonical_git_dir: &Path) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &b in canonical_git_dir.as_os_str().as_encoded_bytes() {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    let safe_name: String = repo_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .take(32)
        .collect();
    format!("{safe_name}-{hash:016x}")
}

/// Fingerprints the names, sizes, and modification times of `HEAD`,
/// `packed-refs`, and every file under `refs/`, so adding, moving, or deleting
/// a ref changes the stamp.
fn git_refs_stamp(git_dir: &Path, common_dir: &Path) -> u64 {
    fn hash_entry(hasher: &mut DefaultHasher, path: &Path) {
        path.hash(hasher);
        if let Ok(meta) = std::fs::symlink_metadata(path) {
            meta.len().hash(hasher);
            meta.modified()
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .hash(hasher);
        }
    }
    let mut hasher = DefaultHasher::new();
    hash_entry(&mut hasher, &git_dir.join("HEAD"));
    hash_entry(&mut hasher, &common_dir.join("packed-refs"));
    let mut pending = vec![common_dir.join("refs")];
    while let Some(dir) = pending.pop() {
        hash_entry(&mut hasher, &dir);
        let Ok(read_dir) = std::fs::read_dir(&dir) else {
            continue;
        };
        let mut children: Vec<PathBuf> = read_dir.flatten().map(|e| e.path()).collect();
        children.sort();
        for child in children {
            if child.is_dir() {
                pending.push(child);
            } else {
                hash_entry(&mut hasher, &child);
            }
        }
    }
    hasher.finish()
}

fn discover_git_dir(workspace_root: &Path) -> Option<PathBuf> {
    let store_dir = workspace_root.join(".jj/repo/store");
    let target_file = store_dir.join("git_target");
    if let Ok(rel) = std::fs::read_to_string(&target_file) {
        let candidate = store_dir.join(rel.trim());
        if candidate.exists() {
            return Some(candidate);
        }
    }
    let colocated = workspace_root.join(".git");
    if colocated.exists() {
        return Some(colocated);
    }
    let internal_git = store_dir.join("git");
    if internal_git.exists() {
        return Some(internal_git);
    }
    None
}

fn seed_pinned_from_repo(repo: &ReadonlyRepo, pinned: &DashSet<String>) {
    pinned.insert(repo.store().root_commit_id().hex());
    for head in repo.view().heads() {
        pinned.insert(head.hex());
    }
    for wc_id in repo.view().wc_commit_ids().values() {
        pinned.insert(wc_id.hex());
    }
    for (_, target) in repo.view().local_bookmarks() {
        for add in target.added_ids() {
            pinned.insert(add.hex());
        }
    }
}

fn commit_mtime(commit: &jj_lib::commit::Commit) -> std::time::SystemTime {
    let ms = commit.committer().timestamp.timestamp.0;
    if ms >= 0 {
        UNIX_EPOCH + Duration::from_millis(ms as u64)
    } else {
        UNIX_EPOCH
    }
}

#[async_trait]
impl ReadOnlyVfsBackend for LocalRepoVfsBackend {
    fn repo_name(&self) -> &str {
        &self.name
    }

    async fn resolve_commit(&self, id_or_prefix: &str) -> Result<String, JjError> {
        let trimmed = id_or_prefix.trim().to_ascii_lowercase();
        if trimmed.len() < 8 {
            return Err(JjError::NotFound);
        }
        let repo = self.current_repo().await?;
        if let Some(full) = CommitId::try_from_hex(&trimmed)
            && full.as_bytes().len() == repo.store().commit_id_length()
        {
            let _ = repo
                .store()
                .get_commit_async(&full)
                .await
                .map_err(|_| JjError::NotFound)?;
            let hex = full.hex();
            self.pinned_commits.insert(hex.clone());
            return Ok(hex);
        }
        let prefix = HexPrefix::try_from_hex(&trimmed).ok_or(JjError::NotFound)?;
        match repo
            .index()
            .resolve_commit_id_prefix(&prefix)
            .map_err(|e| std::io::Error::other(format!("resolve_commit_id_prefix failed: {e:?}")))?
        {
            PrefixResolution::SingleMatch(id) => {
                let hex = id.hex();
                self.pinned_commits.insert(hex.clone());
                Ok(hex)
            }
            PrefixResolution::NoMatch | PrefixResolution::AmbiguousMatch => Err(JjError::NotFound),
        }
    }

    async fn resolve_change(&self, change_or_prefix: &str) -> Result<Option<String>, JjError> {
        let Some(hex_str) = normalize_change_prefix_to_hex(change_or_prefix) else {
            return Err(JjError::NotFound);
        };
        if hex_str.len() < 8 {
            return Err(JjError::NotFound);
        }
        let repo = self.current_repo().await?;
        let prefix = HexPrefix::try_from_hex(&hex_str).ok_or(JjError::NotFound)?;
        let resolved = match repo
            .resolve_change_id_prefix(&prefix)
            .map_err(|e| std::io::Error::other(format!("resolve_change_id_prefix: {e:?}")))?
        {
            PrefixResolution::SingleMatch(targets) => targets,
            PrefixResolution::NoMatch => return Err(JjError::NotFound),
            PrefixResolution::AmbiguousMatch => return Ok(None),
        };
        let targets: Vec<CommitId> = resolved
            .visible_with_offsets()
            .map(|(_, id)| id.clone())
            .collect();
        match targets.as_slice() {
            [] => Err(JjError::NotFound),
            [single] => {
                let hex = single.hex();
                self.pinned_commits.insert(hex.clone());
                Ok(Some(hex))
            }
            _ => Ok(None),
        }
    }

    async fn list_bookmarks(&self) -> Result<BTreeMap<String, String>, JjError> {
        let repo = self.current_repo().await?;
        let mut out = BTreeMap::new();
        for (name, target) in repo.view().local_bookmarks() {
            if let Some(Some(commit_id)) = target.as_resolved() {
                out.insert(name.as_str().to_string(), commit_id.hex());
            } else if let Some(first) = target.added_ids().next() {
                out.insert(name.as_str().to_string(), first.hex());
            }
        }
        Ok(out)
    }

    async fn list_workspaces(
        &self,
        user_filter: Option<&str>,
    ) -> Result<Vec<VfsWorkspaceRef>, JjError> {
        let local_user = "local";
        if let Some(u) = user_filter
            && u != local_user
        {
            return Ok(Vec::new());
        }
        let repo = self.current_repo().await?;
        let op_hex = repo.op_id().hex();
        let local_jj_dir = self.workspace_root.as_ref().map(|p| p.join(".jj"));
        let mut out = Vec::new();
        for (ws_name, commit_id) in repo.view().wc_commit_ids() {
            let commit = repo.store().get_commit_async(commit_id).await?;
            let tree_ids_hex: Vec<String> = commit.tree_ids().iter().map(|id| id.hex()).collect();
            let tree_hex = tree_ids_hex
                .first()
                .cloned()
                .unwrap_or_else(|| repo.store().empty_tree_id().hex());
            let conflict_labels = commit.tree().labels().as_slice().to_vec();
            out.push(VfsWorkspaceRef {
                user: local_user.to_string(),
                workspace_name: ws_name.as_str().to_string(),
                commit_id_hex: commit_id.hex(),
                tree_id_hex: tree_hex,
                tree_ids_hex,
                conflict_labels,
                operation_id_hex: op_hex.clone(),
                local_jj_dir: if ws_name.as_str() == "default" {
                    local_jj_dir.clone()
                } else {
                    None
                },
            });
        }
        Ok(out)
    }

    async fn list_pinned_commits(&self) -> Result<Vec<String>, JjError> {
        let _ = self.current_repo().await?;
        let mut sorted: Vec<String> = self.pinned_commits.iter().map(|r| r.clone()).collect();
        sorted.sort();
        Ok(sorted)
    }

    async fn commit_root_tree_id(&self, commit_id_hex: &str) -> Result<String, JjError> {
        let repo = self.current_repo().await?;
        let cid = CommitId::try_from_hex(commit_id_hex).ok_or(JjError::NotFound)?;
        let commit = repo
            .store()
            .get_commit_async(&cid)
            .await
            .map_err(|_| JjError::NotFound)?;
        Ok(commit
            .tree_ids()
            .iter()
            .next()
            .cloned()
            .unwrap_or_else(|| repo.store().empty_tree_id().clone())
            .hex())
    }

    async fn lookup_in_commit(
        &self,
        commit_id_hex: &str,
        rel_path: &Path,
    ) -> Result<VfsLookup, JjError> {
        let repo = self.current_repo().await?;
        let cid = CommitId::try_from_hex(commit_id_hex).ok_or(JjError::NotFound)?;
        let commit = repo
            .store()
            .get_commit_async(&cid)
            .await
            .map_err(|_| JjError::NotFound)?;
        let mtime = commit_mtime(&commit);
        let repo_path =
            RepoPathBuf::from_relative_path(rel_path).map_err(|_| JjError::InvalidPath)?;
        if repo_path.is_root() {
            let root_tid = commit
                .tree_ids()
                .as_resolved()
                .cloned()
                .unwrap_or_else(|| repo.store().empty_tree_id().clone());
            return Ok(VfsLookup {
                attr: FileAttributes::directory(mtime),
                object_id_hex: root_tid.hex(),
                symlink_target: None,
            });
        }

        let merged_val = commit.tree().path_value(&repo_path).await?;
        if let Some(opt_val) = merged_val.as_resolved() {
            match opt_val {
                None => {
                    // Check if `repo_path` is a non-empty directory in
                    // MergedTree
                    let trees = commit.tree().trees().await?;
                    if let Some(sub) = trees.sub_tree_recursive(&repo_path).await? {
                        let tid = sub.as_resolved().map(|t| t.id().hex()).unwrap_or_default();
                        return Ok(VfsLookup {
                            attr: FileAttributes::directory(mtime),
                            object_id_hex: tid,
                            symlink_target: None,
                        });
                    }
                    Err(JjError::NotFound)
                }
                Some(TreeValue::Tree(tid)) => Ok(VfsLookup {
                    attr: FileAttributes::directory(mtime),
                    object_id_hex: tid.hex(),
                    symlink_target: None,
                }),
                Some(TreeValue::File { id, executable, .. }) => {
                    let size = if let Some(hdr_sz) = self.lookup_git_header_size(id) {
                        hdr_sz
                    } else {
                        self.hydrate_into_cas(&repo, &repo_path, id).await?.1
                    };
                    Ok(VfsLookup {
                        attr: FileAttributes::file(size, *executable, mtime),
                        object_id_hex: id.hex(),
                        symlink_target: None,
                    })
                }
                Some(TreeValue::Symlink(sid)) => {
                    let target_str = repo.store().read_symlink(&repo_path, sid).await?;
                    let target = PathBuf::from(&target_str);
                    Ok(VfsLookup {
                        attr: FileAttributes::symlink(target_str.len() as u64, mtime),
                        object_id_hex: sid.hex(),
                        symlink_target: Some(target),
                    })
                }
                Some(TreeValue::GitSubmodule(sub_id)) => Ok(VfsLookup {
                    attr: FileAttributes::directory(mtime),
                    object_id_hex: sub_id.hex(),
                    symlink_target: None,
                }),
            }
        } else {
            // Materialize algebraic conflict markers into CAS on lookup
            let conflict_bytes = render_conflicted_file(&repo, &repo_path, &merged_val).await?;
            let key = format!(
                "conflict_{commit_id_hex}_{}",
                repo_path.as_internal_file_string()
            );
            let sz = self.cas.put_blob(&key, &conflict_bytes)?;
            Ok(VfsLookup {
                attr: FileAttributes::file(sz, false, mtime),
                object_id_hex: key,
                symlink_target: None,
            })
        }
    }

    async fn list_dir_in_commit(
        &self,
        commit_id_hex: &str,
        dir_path: &Path,
    ) -> Result<Vec<DirectoryEntry>, JjError> {
        let repo = self.current_repo().await?;
        let cid = CommitId::try_from_hex(commit_id_hex).ok_or(JjError::NotFound)?;
        let commit = repo
            .store()
            .get_commit_async(&cid)
            .await
            .map_err(|_| JjError::NotFound)?;
        let repo_path =
            RepoPathBuf::from_relative_path(dir_path).map_err(|_| JjError::InvalidPath)?;
        let root_tree = commit.tree();
        let trees = root_tree.trees().await?;
        let Some(sub_trees) = trees.sub_tree_recursive(&repo_path).await? else {
            let merged_val = root_tree.path_value(&repo_path).await?;
            return match merged_val.as_resolved() {
                Some(None) => Err(JjError::NotFound),
                _ => Err(JjError::NotADirectory),
            };
        };

        let mut seen = BTreeSet::new();
        let mut entries = Vec::new();
        for tree in sub_trees.iter() {
            for entry in tree.entries_non_recursive() {
                let comp = entry.name().to_owned();
                if !seen.insert(comp.as_internal_str().to_string()) {
                    continue;
                }
                let merged_val: Merge<Option<TreeValue>> =
                    sub_trees.map(|t| t.value(&comp).cloned());
                let file_type = match merged_val.as_resolved() {
                    Some(Some(TreeValue::Tree(_)) | Some(TreeValue::GitSubmodule(_))) => {
                        FileType::Directory
                    }
                    Some(Some(TreeValue::Symlink(_))) => FileType::Symlink,
                    _ => FileType::File,
                };
                entries.push(DirectoryEntry {
                    name: comp.as_internal_str().to_string(),
                    file_type,
                });
            }
        }
        Ok(entries)
    }

    async fn read_file_range(
        &self,
        commit_id_hex: &str,
        rel_path: &Path,
        offset: u64,
        size: u32,
    ) -> Result<Box<[u8]>, JjError> {
        let repo = self.current_repo().await?;
        let cid = CommitId::try_from_hex(commit_id_hex).ok_or(JjError::NotFound)?;
        let commit = repo
            .store()
            .get_commit_async(&cid)
            .await
            .map_err(|_| JjError::NotFound)?;
        let repo_path =
            RepoPathBuf::from_relative_path(rel_path).map_err(|_| JjError::InvalidPath)?;
        let merged_val = commit.tree().path_value(&repo_path).await?;

        let (cas_key, file_id) = if let Some(opt_val) = merged_val.as_resolved() {
            match opt_val {
                None => return Err(JjError::NotFound),
                Some(TreeValue::File { id, .. }) => (self.cas_blob_key(id), Some(id.clone())),
                _ => return Err(JjError::NotAFile),
            }
        } else {
            (
                format!(
                    "conflict_{commit_id_hex}_{}",
                    repo_path.as_internal_file_string()
                ),
                None,
            )
        };
        if let Some(slice) = self.cas.read_range(&cas_key, offset, size)? {
            return Ok(slice);
        }
        // The blob is not cached, or a concurrent insert evicted it after
        // hydration. Serve the range from the bytes read here so eviction can
        // never truncate a read.
        let bytes = match &file_id {
            Some(id) => read_store_file(&repo, &repo_path, id).await?,
            None => render_conflicted_file(&repo, &repo_path, &merged_val).await?,
        };
        if let Err(err) = self.cas.put_blob(&cas_key, &bytes) {
            tracing::warn!(key = %cas_key, error = %err, "failed to cache blob in CAS");
        }
        Ok(slice_range(&bytes, offset, size))
    }

    async fn read_symlink(&self, commit_id_hex: &str, rel_path: &Path) -> Result<PathBuf, JjError> {
        let info = self.lookup_in_commit(commit_id_hex, rel_path).await?;
        if info.attr.file_type != FileType::Symlink {
            return Err(JjError::NotASymlink);
        }
        info.symlink_target.ok_or(JjError::NotASymlink)
    }

    async fn diff_trees(
        &self,
        old_tree_hex: &str,
        new_tree_hex: &str,
    ) -> Result<Vec<String>, JjError> {
        if old_tree_hex == new_tree_hex {
            return Ok(Vec::new());
        }
        let repo = self.current_repo().await?;
        let old_tid = TreeId::try_from_hex(old_tree_hex).ok_or(JjError::NotFound)?;
        let new_tid = TreeId::try_from_hex(new_tree_hex).ok_or(JjError::NotFound)?;
        let old_tree = MergedTree::resolved(repo.store().clone(), old_tid);
        let new_tree = MergedTree::resolved(repo.store().clone(), new_tid);
        let mut changed = Vec::new();
        use futures::StreamExt as _;
        let mut stream = old_tree.diff_stream(&new_tree, &jj_lib::matchers::EverythingMatcher);
        while let Some(entry) = stream.next().await {
            changed.push(entry.path.as_internal_file_string().to_string());
        }
        Ok(changed)
    }
}

async fn render_conflicted_file(
    repo: &ReadonlyRepo,
    repo_path: &RepoPathBuf,
    merged_val: &Merge<Option<TreeValue>>,
) -> Result<Vec<u8>, JjError> {
    let mut out = Vec::new();
    out.extend_from_slice(b"<<<<<<< Conflict 1 of 1\n");
    for (idx, item) in merged_val.iter().enumerate() {
        if idx > 0 {
            out.extend_from_slice(b"+++++++ Side\n");
        }
        if let Some(TreeValue::File { id, .. }) = item {
            let mut reader = repo.store().read_file(repo_path, id).await?;
            reader.read_to_end(&mut out).await?;
            if !out.ends_with(b"\n") {
                out.push(b'\n');
            }
        }
    }
    out.extend_from_slice(b">>>>>>> Conflict 1 of 1 ends\n");
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::encode_hex_to_reverse_hex;
    use crate::test_helpers::setup_test_repo;

    #[tokio::test]
    async fn local_backend_resolves_metadata_without_populating_cas_on_git_repo() {
        let settings = default_local_settings().unwrap();
        let ws_dir = tempfile::tempdir().unwrap();
        let cas_dir = tempfile::tempdir().unwrap();
        let cas = Arc::new(BlobCas::new(cas_dir.path(), 10 * 1024 * 1024).unwrap());

        let (ws, repo) = Workspace::init_internal_git(&settings, ws_dir.path())
            .await
            .unwrap();
        let store = repo.store().clone();
        let mut tb =
            jj_lib::tree_builder::TreeBuilder::new(store.clone(), store.empty_tree_id().clone());
        let script_path = RepoPathBuf::from_relative_path(Path::new("bin/run.sh")).unwrap();
        let payload = b"#!/bin/sh\necho hello_jj_vfs_ranged_read\n";
        let fid = store
            .write_file(&script_path, &mut &payload[..])
            .await
            .unwrap();
        tb.set(
            script_path,
            TreeValue::File {
                id: fid,
                executable: true,
                copy_id: jj_lib::backend::CopyId::placeholder(),
            },
        );
        let tid = tb.write_tree().await.unwrap();
        let mut tx = repo.start_transaction();
        let commit = tx
            .repo_mut()
            .new_commit(
                vec![store.root_commit_id().clone()],
                MergedTree::resolved(store.clone(), tid),
            )
            .set_description("add executable script")
            .write()
            .await
            .unwrap();
        tx.repo_mut().set_local_bookmark_target(
            jj_lib::ref_name::RefName::new("feat/exec"),
            jj_lib::op_store::RefTarget::normal(commit.id().clone()),
        );
        tx.repo_mut()
            .set_wc_commit(ws.workspace_name().to_owned(), commit.id().clone())
            .unwrap();
        tx.commit("commit script").await.unwrap();

        let backend =
            LocalRepoVfsBackend::open_jj_workspace("demo", ws_dir.path(), cas.clone()).unwrap();

        // 1. Prefix resolution (>=12 chars) and change ID resolution
        let resolved = backend
            .resolve_commit(&commit.id().hex()[..12])
            .await
            .unwrap();
        assert_eq!(resolved, commit.id().hex());

        let rev_hex = encode_hex_to_reverse_hex(&commit.change_id().hex()).unwrap();
        let by_change = backend.resolve_change(&rev_hex[..12]).await.unwrap();
        assert_eq!(by_change, Some(commit.id().hex()));

        // 2. lookup_in_commit reads Git object header size WITHOUT inflating
        //    into BlobCas!
        assert_eq!(cas.stats().0, 0);
        let info = backend
            .lookup_in_commit(&resolved, Path::new("bin/run.sh"))
            .await
            .unwrap();
        assert_eq!(info.attr.size, payload.len() as u64);
        assert!(info.attr.executable);
        assert_eq!(
            cas.stats().0,
            0,
            "getattr must not hydrate file bytes into CAS"
        );

        // 3. read_file_range inflates once into CAS and serves exact middle
        //    bytes via pread
        let slice = backend
            .read_file_range(&resolved, Path::new("bin/run.sh"), 15, 12)
            .await
            .unwrap();
        assert_eq!(&*slice, b"hello_jj_vfs");
        assert_eq!(cas.stats().0, 1);

        // 4. Nested bookmark listing
        let bms = backend.list_bookmarks().await.unwrap();
        assert_eq!(bms.get("feat/exec"), Some(&commit.id().hex()));
    }

    #[tokio::test]
    async fn local_backend_supports_native_test_repo_and_tree_diffs() {
        let (_tmp, repo, cid) = setup_test_repo().await;
        let cas_dir = tempfile::tempdir().unwrap();
        let cas = Arc::new(BlobCas::new(cas_dir.path(), 0).unwrap());
        let backend = LocalRepoVfsBackend::from_readonly_repo("mem", repo.clone(), None, cas);

        let entries = backend
            .list_dir_in_commit(&cid.hex(), Path::new(""))
            .await
            .unwrap();
        assert_eq!(entries.len(), 3);

        let new_tree_hex = backend.commit_root_tree_id(&cid.hex()).await.unwrap();
        let empty_hex = repo.store().empty_tree_id().hex();
        let diff = backend.diff_trees(&empty_hex, &new_tree_hex).await.unwrap();
        assert_eq!(diff, vec!["dir/file2.txt", "file1.txt", "symlink"]);
    }

    #[tokio::test]
    async fn local_backend_rehydrates_on_cas_eviction_and_caches_repo_until_op_head_changes() {
        let settings = default_local_settings().unwrap();
        let ws_dir = tempfile::tempdir().unwrap();
        let cas_dir = tempfile::tempdir().unwrap();
        // Budget (20 bytes) is smaller than 2x file size (2 * 16 = 32 bytes),
        // so inserting B evicts A from CAS.
        let cas = Arc::new(BlobCas::new(cas_dir.path(), 20).unwrap());

        let (ws, repo) = Workspace::init_internal_git(&settings, ws_dir.path())
            .await
            .unwrap();
        let store = repo.store().clone();
        let mut tb =
            jj_lib::tree_builder::TreeBuilder::new(store.clone(), store.empty_tree_id().clone());
        let path_a = RepoPathBuf::from_relative_path(Path::new("a.txt")).unwrap();
        let path_b = RepoPathBuf::from_relative_path(Path::new("b.txt")).unwrap();
        let fid_a = store
            .write_file(&path_a, &mut &b"0123456789ABCDEF"[..])
            .await
            .unwrap();
        let fid_b = store
            .write_file(&path_b, &mut &b"fedcba9876543210"[..])
            .await
            .unwrap();
        tb.set(
            path_a,
            TreeValue::File {
                id: fid_a,
                executable: false,
                copy_id: jj_lib::backend::CopyId::placeholder(),
            },
        );
        tb.set(
            path_b,
            TreeValue::File {
                id: fid_b,
                executable: false,
                copy_id: jj_lib::backend::CopyId::placeholder(),
            },
        );
        let tid = tb.write_tree().await.unwrap();
        let mut tx = repo.start_transaction();
        let c1 = tx
            .repo_mut()
            .new_commit(
                vec![store.root_commit_id().clone()],
                MergedTree::resolved(store.clone(), tid),
            )
            .write()
            .await
            .unwrap();
        tx.repo_mut()
            .set_wc_commit(ws.workspace_name().to_owned(), c1.id().clone())
            .unwrap();
        let repo_after_c1 = tx.commit("c1").await.unwrap();

        let backend = LocalRepoVfsBackend::open_jj_workspace("evict", ws_dir.path(), cas).unwrap();

        // 8-char commit prefix resolves cleanly.
        let resolved = backend.resolve_commit(&c1.id().hex()[..8]).await.unwrap();
        assert_eq!(resolved, c1.id().hex());

        // Consecutive operations reuse the exact same `Arc<ReadonlyRepo>`.
        let r1 = backend.current_repo().await.unwrap();
        let r2 = backend.current_repo().await.unwrap();
        assert!(Arc::ptr_eq(&r1, &r2));

        // Reading B evicts A under the 20-byte CAS budget; reading A again must
        // re-hydrate rather than returning empty/truncated bytes.
        assert_eq!(
            &*backend
                .read_file_range(&resolved, Path::new("a.txt"), 4, 8)
                .await
                .unwrap(),
            b"456789AB"
        );
        assert_eq!(
            &*backend
                .read_file_range(&resolved, Path::new("b.txt"), 0, 16)
                .await
                .unwrap(),
            b"fedcba9876543210"
        );
        assert_eq!(
            &*backend
                .read_file_range(&resolved, Path::new("a.txt"), 10, 6)
                .await
                .unwrap(),
            b"ABCDEF"
        );

        // Advancing the workspace operation head causes `current_repo()` to
        // reload.
        let mut tx2 = repo_after_c1.start_transaction();
        tx2.repo_mut().set_local_bookmark_target(
            jj_lib::ref_name::RefName::new("fresh-bm"),
            jj_lib::op_store::RefTarget::normal(c1.id().clone()),
        );
        tx2.commit("set bookmark").await.unwrap();

        let bms = backend.list_bookmarks().await.unwrap();
        assert_eq!(bms.get("fresh-bm"), Some(&c1.id().hex()));
        let r3 = backend.current_repo().await.unwrap();
        assert!(!Arc::ptr_eq(&r1, &r3));
    }

    #[tokio::test]
    async fn local_git_mirrors_isolate_same_named_repos_and_refresh_on_ref_updates() {
        let root_one = tempfile::tempdir().unwrap();
        let root_two = tempfile::tempdir().unwrap();
        let mirror_root = tempfile::tempdir().unwrap();
        let cas_dir = tempfile::tempdir().unwrap();
        let cas = Arc::new(BlobCas::new(cas_dir.path(), 0).unwrap());

        let dir_one = root_one.path().join("app");
        let dir_two = root_two.path().join("app");
        std::fs::create_dir_all(&dir_one).unwrap();
        std::fs::create_dir_all(&dir_two).unwrap();

        let mut git_one = gix::init(&dir_one).unwrap();
        let mut git_two = gix::init(&dir_two).unwrap();
        let sig = || gix::actor::Signature {
            name: "Test".into(),
            email: "test@example.com".into(),
            time: gix::date::Time::new(1_700_000_000, 0),
        };

        let write_git_commit = |repo: &mut gix::Repository, branch: &str, body: &[u8]| -> String {
            let blob_id = repo.write_blob(body).unwrap().detach();
            let entry = gix::objs::tree::Entry {
                mode: gix::objs::tree::EntryKind::Blob.into(),
                filename: "id.txt".into(),
                oid: blob_id,
            };
            let tree = gix::objs::Tree {
                entries: vec![entry],
            };
            let tree_id = repo.write_object(&tree).unwrap().detach();
            let commit = gix::objs::Commit {
                tree: tree_id,
                parents: Default::default(),
                author: sig(),
                committer: sig(),
                encoding: None,
                message: "msg".into(),
                extra_headers: vec![],
            };
            let commit_oid = repo.write_object(&commit).unwrap().detach();
            let hex = commit_oid.to_hex().to_string();
            let refs_heads = repo.git_dir().join("refs/heads");
            std::fs::create_dir_all(&refs_heads).unwrap();
            std::fs::write(refs_heads.join(branch), format!("{hex}\n")).unwrap();
            hex
        };

        let c_one = write_git_commit(&mut git_one, "main", b"from-repo-one");
        let c_two = write_git_commit(&mut git_two, "main", b"from-repo-two");

        let mut backend_one =
            LocalRepoVfsBackend::open_git_repo("app", &dir_one, mirror_root.path(), cas.clone())
                .unwrap();
        if let Some(m) = &mut backend_one.git_mirror {
            m.poll_interval = Duration::ZERO;
        }
        let backend_two =
            LocalRepoVfsBackend::open_git_repo("app", &dir_two, mirror_root.path(), cas).unwrap();

        let bm_one = backend_one.list_bookmarks().await.unwrap();
        let bm_two = backend_two.list_bookmarks().await.unwrap();
        assert_eq!(bm_one.get("main"), Some(&c_one));
        assert_eq!(bm_two.get("main"), Some(&c_two));
        assert_ne!(c_one, c_two);

        assert_eq!(
            &*backend_one
                .read_file_range(&c_one, Path::new("id.txt"), 0, 64)
                .await
                .unwrap(),
            b"from-repo-one"
        );
        assert_eq!(
            &*backend_two
                .read_file_range(&c_two, Path::new("id.txt"), 0, 64)
                .await
                .unwrap(),
            b"from-repo-two"
        );

        // Create a new branch in `git_one` and verify `backend_one` imports it
        // on refresh.
        let c_next = write_git_commit(&mut git_one, "feature", b"from-feature-branch");
        let refreshed_bm = backend_one.list_bookmarks().await.unwrap();
        assert_eq!(refreshed_bm.get("feature"), Some(&c_next));
        assert_eq!(
            &*backend_one
                .read_file_range(&c_next, Path::new("id.txt"), 0, 64)
                .await
                .unwrap(),
            b"from-feature-branch"
        );
    }
}
