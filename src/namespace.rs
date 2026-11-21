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

//! Multi-source Option C path router (`NamespaceRouter`) and scoped mount
//! adapter (`SubtreePathMapper`) shared across Root and Scoped Direct FUSE
//! sessions.
//!
//! # Authority and Crash-Safety Ordering
//! `NamespaceRouter` resolves immutable commits, change symlinks, and bookmark
//! symlinks directly against `ReadOnlyVfsBackend` implementations registered in
//! `SourceRegistry`, while routing canonical workspace paths
//! (`<source>/repos/<repo>/users/<user>/<ws>/...`) through cached
//! `VfsWorkspaceRef` pointers updated atomically on snapshot and checkout
//! operations.
//!
//! # Concurrency and Merge Invariants
//! Both `<source>/repos/<repo>/workspaces/<ws>` and
//! `<source>/users/<user>/<repo>/<ws>` emit relative POSIX symlinks converging
//! on the single canonical workspace path `<source>/repos/<repo>/users/<user>/
//! <ws>` (`../users/<user>/<ws>` and `../../../repos/<repo>/users/<user>/
//! <ws>`), ensuring one kernel inode subtree per workspace inside Root and
//! Source mounts while Scoped Direct Mounts bind their interior slice directly
//! via `SubtreePathMapper`.

use std::collections::BTreeSet;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use vfs_proto::MountScope;
use vfs_proto::SourceKind;
use vfs_proto::WorkspaceKey;
use vfs_proto::encode_repo_segment;

use crate::backend::BackendCommitNode;
use crate::backend::VfsWorkspaceRef;
use crate::backend::VirtualSymlinkNode;
use crate::jj_error::JjError;
use crate::overlay::OverlayManager;
use crate::overlay::WorkspaceOverlay;
use crate::path_mapper::PathMapper;
use crate::registry::SourceRegistry;
use crate::registry::expand_tilde;
use crate::static_directory::StaticDirectory;
use crate::vfs::VirtualFilesystem;
use crate::virtual_file::AttributeChanges;
use crate::virtual_file::DirectoryEntry;
use crate::virtual_file::DirectoryStream;
use crate::virtual_file::FileAttributes;
use crate::virtual_file::FileType;
use crate::virtual_file::VirtualFile;

pub struct NamespaceRouter {
    registry: Arc<SourceRegistry>,
    overlays: Arc<OverlayManager>,
    workspace_heads: DashMap<WorkspaceKey, VfsWorkspaceRef>,
}

impl NamespaceRouter {
    pub fn new(registry: Arc<SourceRegistry>, overlays: Arc<OverlayManager>) -> Self {
        Self {
            registry,
            overlays,
            workspace_heads: DashMap::new(),
        }
    }

    pub fn registry(&self) -> &Arc<SourceRegistry> {
        &self.registry
    }

    pub fn overlays(&self) -> &Arc<OverlayManager> {
        &self.overlays
    }

    /// Canonicalizes a repo segment for `LocalJj` and `LocalGit` sources, where
    /// `SourceRegistry::resolve_repo` accepts either the checkout directory
    /// name or the source alias. Mapping both to the directory name ensures one
    /// `WorkspaceKey`, overlay directory, and cached head per workspace.
    pub fn canonicalize_repo_id(&self, source: &str, repo_seg: &str) -> String {
        if let Some(src) = self.registry.list_sources().get(source)
            && matches!(src.kind, SourceKind::LocalJj | SourceKind::LocalGit)
            && let Some(raw_path) = &src.path
        {
            let path = expand_tilde(raw_path);
            let expected_seg = path
                .file_name()
                .and_then(|s| s.to_str())
                .map(encode_repo_segment)
                .unwrap_or_else(|| source.to_string());
            if repo_seg == expected_seg || repo_seg == source {
                return expected_seg.replace("__", "/");
            }
        }
        repo_seg.replace("__", "/")
    }

    pub fn canonicalize_key(&self, key: &WorkspaceKey) -> WorkspaceKey {
        let repo_seg = encode_repo_segment(&key.repo);
        WorkspaceKey {
            source: key.source.clone(),
            repo: self.canonicalize_repo_id(&key.source, &repo_seg),
            user: key.user.clone(),
            workspace: key.workspace.clone(),
        }
    }

    /// Rewrites `MountScope` repo names to their canonical repo id so direct
    /// mounts of `<alias>:<alias>/default` and `<alias>:<dirname>/default`
    /// share one router prefix.
    pub fn canonicalize_mount_scope(&self, scope: &mut MountScope) {
        match scope {
            MountScope::Repo { source, repo } | MountScope::Workspace { source, repo, .. } => {
                let repo_seg = encode_repo_segment(repo);
                *repo = self.canonicalize_repo_id(source, &repo_seg);
            }
            MountScope::Root | MountScope::Source { .. } | MountScope::User { .. } => {}
        }
    }

    /// Updates or inserts the active head metadata for a workspace.
    pub fn set_workspace_head(&self, key: WorkspaceKey, head: VfsWorkspaceRef) {
        self.workspace_heads
            .insert(self.canonicalize_key(&key), head);
    }

    /// Returns cached head metadata for `key` if present without querying
    /// backends.
    pub fn cached_workspace_head(&self, key: &WorkspaceKey) -> Option<VfsWorkspaceRef> {
        self.workspace_heads.get(key).map(|r| r.clone())
    }

    /// Returns all workspaces currently cached in `NamespaceRouter`.
    pub fn active_workspace_heads(&self) -> Vec<(WorkspaceKey, VfsWorkspaceRef)> {
        self.workspace_heads
            .iter()
            .map(|item| (item.key().clone(), item.value().clone()))
            .collect()
    }

    /// Returns cached head metadata for `key`, or resolves it from the backend.
    pub async fn resolve_workspace_ref(
        &self,
        key: &WorkspaceKey,
    ) -> Result<VfsWorkspaceRef, JjError> {
        let key = self.canonicalize_key(key);
        if let Some(hit) = self.workspace_heads.get(&key) {
            return Ok(hit.clone());
        }
        let repo_seg = encode_repo_segment(&key.repo);
        let backend = self.registry.resolve_repo(&key.source, &repo_seg)?;
        let workspaces = backend.list_workspaces(Some(&key.user)).await?;
        let found = workspaces
            .into_iter()
            .find(|w| w.workspace_name == key.workspace && w.user == key.user)
            .ok_or(JjError::NotFound)?;
        let head = self.workspace_heads.entry(key).or_insert(found).clone();
        Ok(head)
    }

    /// Parses a router-interior path into `(WorkspaceKey,
    /// workspace_relative_path)` when `path` lies inside a canonical workspace
    /// tree.
    pub fn parse_workspace_path(
        &self,
        path: &Path,
    ) -> Result<Option<(WorkspaceKey, PathBuf)>, JjError> {
        let segs = split_clean_segments(path)?;
        let seg_refs: Vec<&str> = segs.iter().map(String::as_str).collect();
        match seg_refs.as_slice() {
            [source, "repos", repo, "users", user, ws, rest @ ..] => {
                let key = WorkspaceKey {
                    source: (*source).to_string(),
                    repo: self.canonicalize_repo_id(source, repo),
                    user: (*user).to_string(),
                    workspace: (*ws).to_string(),
                };
                Ok(Some((key, join_segments(rest))))
            }
            [source, "repos", repo, "workspaces", ws, rest @ ..] => {
                let user = self
                    .registry
                    .default_user_for_source(source)
                    .ok_or(JjError::NotFound)?;
                let key = WorkspaceKey {
                    source: (*source).to_string(),
                    repo: self.canonicalize_repo_id(source, repo),
                    user,
                    workspace: (*ws).to_string(),
                };
                Ok(Some((key, join_segments(rest))))
            }
            _ => Ok(None),
        }
    }

    async fn resolve_segments(&self, segs: &[&str]) -> Result<Box<dyn VirtualFile>, JjError> {
        match segs {
            [] => {
                let entries = self
                    .registry
                    .list_sources()
                    .into_keys()
                    .map(|name| DirectoryEntry {
                        name,
                        file_type: FileType::Directory,
                    })
                    .collect();
                Ok(Box::new(StaticDirectory::new(entries)))
            }
            [source] => {
                if !self.registry.has_source(source) {
                    return Err(JjError::NotFound);
                }
                let mut entries = Vec::with_capacity(3);
                if self.registry.default_user_for_source(source).is_some() {
                    entries.push(DirectoryEntry {
                        name: "me".to_string(),
                        file_type: FileType::Symlink,
                    });
                }
                entries.push(DirectoryEntry {
                    name: "repos".to_string(),
                    file_type: FileType::Directory,
                });
                entries.push(DirectoryEntry {
                    name: "users".to_string(),
                    file_type: FileType::Directory,
                });
                Ok(Box::new(StaticDirectory::new(entries)))
            }
            [source, "me"] => {
                let user = self
                    .registry
                    .default_user_for_source(source)
                    .ok_or(JjError::NotFound)?;
                Ok(Box::new(VirtualSymlinkNode::new(format!("users/{user}"))))
            }
            [source, "me", rest @ ..] => {
                let user = self
                    .registry
                    .default_user_for_source(source)
                    .ok_or(JjError::NotFound)?;
                let mut rewritten = Vec::with_capacity(2 + rest.len());
                rewritten.push(*source);
                rewritten.push("users");
                rewritten.push(user.as_str());
                rewritten.extend_from_slice(rest);
                Box::pin(self.resolve_segments(&rewritten)).await
            }
            [source, "repos"] => {
                let repos = self.registry.list_known_repos(source)?;
                let entries = repos
                    .into_iter()
                    .map(|name| DirectoryEntry {
                        name,
                        file_type: FileType::Directory,
                    })
                    .collect();
                Ok(Box::new(StaticDirectory::new(entries)))
            }
            [source, "repos", repo] => {
                let _ = self.registry.resolve_repo(source, repo)?;
                let entries = ["workspaces", "users", "commits", "changes", "bookmarks"]
                    .into_iter()
                    .map(|name| DirectoryEntry {
                        name: name.to_string(),
                        file_type: FileType::Directory,
                    })
                    .collect();
                Ok(Box::new(StaticDirectory::new(entries)))
            }
            [source, "repos", repo, "commits"] => {
                let backend = self.registry.resolve_repo(source, repo)?;
                let pinned = backend.list_pinned_commits().await?;
                let entries = pinned
                    .into_iter()
                    .map(|name| DirectoryEntry {
                        name,
                        file_type: FileType::Directory,
                    })
                    .collect();
                Ok(Box::new(StaticDirectory::new(entries)))
            }
            [source, "repos", repo, "commits", id_or_prefix, rest @ ..] => {
                let backend = self.registry.resolve_repo(source, repo)?;
                let commit_id = backend.resolve_commit(id_or_prefix).await?;
                Ok(Box::new(BackendCommitNode::new(
                    backend,
                    commit_id,
                    join_segments(rest),
                )))
            }
            [source, "repos", repo, "changes"] => {
                let _ = self.registry.resolve_repo(source, repo)?;
                Ok(Box::new(StaticDirectory::new(Vec::new())))
            }
            [source, "repos", repo, "changes", change_or_prefix] => {
                let backend = self.registry.resolve_repo(source, repo)?;
                let commit_id = backend
                    .resolve_change(change_or_prefix)
                    .await?
                    .ok_or(JjError::NotFound)?;
                Ok(Box::new(VirtualSymlinkNode::new(format!(
                    "../commits/{commit_id}"
                ))))
            }
            [
                source,
                "repos",
                repo,
                "changes",
                change_or_prefix,
                rest @ ..,
            ] => {
                let backend = self.registry.resolve_repo(source, repo)?;
                let commit_id = backend
                    .resolve_change(change_or_prefix)
                    .await?
                    .ok_or(JjError::NotFound)?;
                Ok(Box::new(BackendCommitNode::new(
                    backend,
                    commit_id,
                    join_segments(rest),
                )))
            }
            [source, "repos", repo, "bookmarks", rest @ ..] => {
                self.resolve_bookmarks(source, repo, rest).await
            }
            [source, "repos", repo, "workspaces"] => {
                let backend = self.registry.resolve_repo(source, repo)?;
                let default_user = self.registry.default_user_for_source(source);
                let workspaces = backend.list_workspaces(default_user.as_deref()).await?;
                let mut seen = BTreeSet::new();
                let mut entries = Vec::new();
                for ws in workspaces {
                    if seen.insert(ws.workspace_name.clone()) {
                        entries.push(DirectoryEntry {
                            name: ws.workspace_name,
                            file_type: FileType::Symlink,
                        });
                    }
                }
                Ok(Box::new(StaticDirectory::new(entries)))
            }
            [source, "repos", repo, "workspaces", ws_name] => {
                let user = self
                    .registry
                    .default_user_for_source(source)
                    .ok_or(JjError::NotFound)?;
                let canon_repo = self.canonicalize_repo_id(source, repo);
                let canon_seg = encode_repo_segment(&canon_repo);
                let key = WorkspaceKey {
                    source: (*source).to_string(),
                    repo: canon_repo,
                    user: user.clone(),
                    workspace: (*ws_name).to_string(),
                };
                let _ = self.resolve_workspace_ref(&key).await?;
                let target = if canon_seg == *repo {
                    format!("../users/{user}/{ws_name}")
                } else {
                    format!("../../{canon_seg}/users/{user}/{ws_name}")
                };
                Ok(Box::new(VirtualSymlinkNode::new(target)))
            }
            [source, "repos", repo, "workspaces", ws_name, rest @ ..] => {
                let user = self
                    .registry
                    .default_user_for_source(source)
                    .ok_or(JjError::NotFound)?;
                self.resolve_canonical_workspace(source, repo, &user, ws_name, rest)
                    .await
            }
            [source, "repos", repo, "users"] => {
                let backend = self.registry.resolve_repo(source, repo)?;
                let workspaces = backend.list_workspaces(None).await?;
                let mut users = BTreeSet::new();
                for ws in workspaces {
                    users.insert(ws.user);
                }
                if users.is_empty()
                    && let Some(u) = self.registry.default_user_for_source(source)
                {
                    users.insert(u);
                }
                let entries = users
                    .into_iter()
                    .map(|name| DirectoryEntry {
                        name,
                        file_type: FileType::Directory,
                    })
                    .collect();
                Ok(Box::new(StaticDirectory::new(entries)))
            }
            [source, "repos", repo, "users", user] => {
                let backend = self.registry.resolve_repo(source, repo)?;
                let workspaces = backend.list_workspaces(Some(user)).await?;
                let entries = workspaces
                    .into_iter()
                    .map(|ws| DirectoryEntry {
                        name: ws.workspace_name,
                        file_type: FileType::Directory,
                    })
                    .collect();
                Ok(Box::new(StaticDirectory::new(entries)))
            }
            [source, "repos", repo, "users", user, ws_name, rest @ ..] => {
                self.resolve_canonical_workspace(source, repo, user, ws_name, rest)
                    .await
            }
            [source, "users"] => {
                if !self.registry.has_source(source) {
                    return Err(JjError::NotFound);
                }
                let mut users = BTreeSet::new();
                if let Some(u) = self.registry.default_user_for_source(source) {
                    users.insert(u);
                }
                for entry in self.workspace_heads.iter() {
                    if entry.key().source == *source {
                        users.insert(entry.key().user.clone());
                    }
                }
                let entries = users
                    .into_iter()
                    .map(|name| DirectoryEntry {
                        name,
                        file_type: FileType::Directory,
                    })
                    .collect();
                Ok(Box::new(StaticDirectory::new(entries)))
            }
            [source, "users", _user] => {
                let repos = self.registry.list_known_repos(source)?;
                let entries = repos
                    .into_iter()
                    .map(|name| DirectoryEntry {
                        name,
                        file_type: FileType::Directory,
                    })
                    .collect();
                Ok(Box::new(StaticDirectory::new(entries)))
            }
            [source, "users", user, repo] => {
                let backend = self.registry.resolve_repo(source, repo)?;
                let workspaces = backend.list_workspaces(Some(user)).await?;
                let entries = workspaces
                    .into_iter()
                    .map(|ws| DirectoryEntry {
                        name: ws.workspace_name,
                        file_type: FileType::Symlink,
                    })
                    .collect();
                Ok(Box::new(StaticDirectory::new(entries)))
            }
            [source, "users", user, repo, ws_name] => {
                let canon_repo = self.canonicalize_repo_id(source, repo);
                let canon_seg = encode_repo_segment(&canon_repo);
                let key = WorkspaceKey {
                    source: (*source).to_string(),
                    repo: canon_repo,
                    user: (*user).to_string(),
                    workspace: (*ws_name).to_string(),
                };
                let _ = self.resolve_workspace_ref(&key).await?;
                Ok(Box::new(VirtualSymlinkNode::new(format!(
                    "../../../repos/{canon_seg}/users/{user}/{ws_name}"
                ))))
            }
            [source, "users", user, repo, ws_name, rest @ ..] => {
                self.resolve_canonical_workspace(source, repo, user, ws_name, rest)
                    .await
            }
            _ => Err(JjError::NotFound),
        }
    }

    async fn resolve_bookmarks(
        &self,
        source: &str,
        repo: &str,
        rest: &[&str],
    ) -> Result<Box<dyn VirtualFile>, JjError> {
        let backend = self.registry.resolve_repo(source, repo)?;
        let bookmarks = backend.list_bookmarks().await?;
        if rest.is_empty() {
            let entries = bookmark_children(&bookmarks, "");
            return Ok(Box::new(StaticDirectory::new(entries)));
        }

        let candidate = rest.join("/");
        if let Some(commit_id) = bookmarks.get(&candidate) {
            let ups = vec![".."; rest.len()].join("/");
            return Ok(Box::new(VirtualSymlinkNode::new(format!(
                "{ups}/commits/{commit_id}"
            ))));
        }

        // Check if `rest` traverses through a bookmark into its commit tree
        for depth in 1..rest.len() {
            let prefix = rest[..depth].join("/");
            if let Some(commit_id) = bookmarks.get(&prefix) {
                return Ok(Box::new(BackendCommitNode::new(
                    backend,
                    commit_id.clone(),
                    join_segments(&rest[depth..]),
                )));
            }
        }

        let dir_prefix = format!("{candidate}/");
        let children = bookmark_children(&bookmarks, &dir_prefix);
        if children.is_empty() {
            return Err(JjError::NotFound);
        }
        Ok(Box::new(StaticDirectory::new(children)))
    }

    async fn resolve_canonical_workspace(
        &self,
        source: &str,
        repo: &str,
        user: &str,
        ws_name: &str,
        rest: &[&str],
    ) -> Result<Box<dyn VirtualFile>, JjError> {
        let _ = self.registry.resolve_repo(source, repo)?;
        let key = WorkspaceKey {
            source: source.to_string(),
            repo: self.canonicalize_repo_id(source, repo),
            user: user.to_string(),
            workspace: ws_name.to_string(),
        };
        let canon_seg = encode_repo_segment(&key.repo);
        let ws_ref = self.resolve_workspace_ref(&key).await?;
        let backend = self.registry.resolve_repo(source, &canon_seg)?;
        let overlay = self.overlays.get_or_open(&key)?;
        if let Some(local_jj) = &ws_ref.local_jj_dir
            && overlay.synthesized_jj_dir().is_none()
        {
            overlay.set_synthesized_jj_dir(Some(local_jj.clone()));
        }
        overlay
            .get_virtual_entry(backend, ws_ref.commit_id_hex, &join_segments(rest))
            .await
    }

    /// Resolves `(overlay, backend, base_commit_hex, rel_path)` for a mutable
    /// workspace path, returning `Err(JjError::ReadOnly)` if `router_path` lies
    /// outside a workspace (such as `commits/`, `bookmarks/`, `changes/`, or
    /// static category directories).
    pub async fn resolve_mutable_workspace(
        &self,
        router_path: &Path,
    ) -> Result<
        (
            Arc<WorkspaceOverlay>,
            Arc<dyn crate::backend::ReadOnlyVfsBackend>,
            String,
            PathBuf,
        ),
        JjError,
    > {
        let Some((key, rel_path)) = self.parse_workspace_path(router_path)? else {
            return Err(JjError::ReadOnly);
        };
        let ws_ref = self.resolve_workspace_ref(&key).await?;
        let repo_seg = encode_repo_segment(&key.repo);
        let backend = self.registry.resolve_repo(&key.source, &repo_seg)?;
        let overlay = self.overlays.get_or_open(&key)?;
        if let Some(local_jj) = &ws_ref.local_jj_dir
            && overlay.synthesized_jj_dir().is_none()
        {
            overlay.set_synthesized_jj_dir(Some(local_jj.clone()));
        }
        Ok((overlay, backend, ws_ref.commit_id_hex, rel_path))
    }
}

#[async_trait]
impl PathMapper for NamespaceRouter {
    async fn get_entry(&self, path: &Path) -> Result<Box<dyn VirtualFile>, JjError> {
        let segs = split_clean_segments(path)?;
        let seg_refs: Vec<&str> = segs.iter().map(String::as_str).collect();
        self.resolve_segments(&seg_refs).await
    }
}

/// Scopes an interior slice of `NamespaceRouter` onto a dedicated FUSE
/// mountpoint.
pub struct SubtreePathMapper {
    router: Arc<NamespaceRouter>,
    scope: MountScope,
    router_prefix: PathBuf,
}

impl SubtreePathMapper {
    pub fn new(router: Arc<NamespaceRouter>, scope: MountScope, router_prefix: PathBuf) -> Self {
        Self {
            router,
            scope,
            router_prefix,
        }
    }

    pub fn router(&self) -> &Arc<NamespaceRouter> {
        &self.router
    }

    pub fn scope(&self) -> &MountScope {
        &self.scope
    }

    pub fn router_prefix(&self) -> &Path {
        &self.router_prefix
    }

    /// Maps a mount-relative path into its canonical `NamespaceRouter` path,
    /// handling `MountScope::User` (`<repo>/<ws>/...` ->
    /// `<source>/repos/<repo>/users/<user>/<ws>/...`) so Direct User Mounts
    /// never emit relative symlinks escaping the mount root.
    pub fn to_router_path(&self, mount_rel: &Path) -> Result<PathBuf, JjError> {
        if let MountScope::User { source, user } = &self.scope {
            let segs = split_clean_segments(mount_rel)?;
            return match segs.as_slice() {
                [] | [_] => Ok(self.router_prefix.join(mount_rel)),
                [repo, ws, rest @ ..] => {
                    let canon_repo = self.router.canonicalize_repo_id(source, repo);
                    let mut p = PathBuf::from(source)
                        .join("repos")
                        .join(encode_repo_segment(&canon_repo))
                        .join("users")
                        .join(user)
                        .join(ws);
                    for r in rest {
                        p.push(r);
                    }
                    Ok(p)
                }
            };
        }
        if self.router_prefix.as_os_str().is_empty() {
            Ok(mount_rel.to_path_buf())
        } else if mount_rel.as_os_str().is_empty() {
            Ok(self.router_prefix.clone())
        } else {
            Ok(self.router_prefix.join(mount_rel))
        }
    }
}

#[async_trait]
impl PathMapper for SubtreePathMapper {
    async fn get_entry(&self, path: &Path) -> Result<Box<dyn VirtualFile>, JjError> {
        if let MountScope::User { source, user } = &self.scope {
            let segs = split_clean_segments(path)?;
            if let [repo] = segs.as_slice() {
                // Inside a Direct User Mount (`<dir>/<repo>`), expose
                // workspaces as directories rather than `../../../../repos/...`
                // symlinks so lookups stay strictly inside `<dir>`.
                let backend = self.router.registry().resolve_repo(source, repo)?;
                let workspaces = backend.list_workspaces(Some(user)).await?;
                let entries = workspaces
                    .into_iter()
                    .map(|ws| DirectoryEntry {
                        name: ws.workspace_name,
                        file_type: FileType::Directory,
                    })
                    .collect();
                return Ok(Box::new(StaticDirectory::new(entries)));
            }
        }
        let full_path = self.to_router_path(path)?;
        self.router.get_entry(&full_path).await
    }
}

/// Full read-write `VirtualFilesystem` binding a `SubtreePathMapper` session to
/// `NamespaceRouter` and `WorkspaceOverlay`, enforcing `EROFS` on immutable
/// `commits/`/`bookmarks/`/`changes/` paths and `EXDEV` across distinct
/// workspaces.
pub struct RouterVfs {
    mapper: SubtreePathMapper,
}

impl RouterVfs {
    pub fn new(mapper: SubtreePathMapper) -> Self {
        Self { mapper }
    }
}

#[async_trait]
impl VirtualFilesystem for RouterVfs {
    async fn get_attributes(&self, path: &Path) -> Result<FileAttributes, JjError> {
        self.mapper.get_entry(path).await?.attributes().await
    }

    async fn read(&self, path: &Path, offset: u64, size: u32) -> Result<Box<[u8]>, JjError> {
        self.mapper
            .get_entry(path)
            .await?
            .read_range(offset, size)
            .await
    }

    async fn read_directory(&self, path: &Path) -> Result<DirectoryStream, JjError> {
        self.mapper.get_entry(path).await?.list().await
    }

    async fn read_link(&self, path: &Path) -> Result<PathBuf, JjError> {
        self.mapper.get_entry(path).await?.read_link().await
    }

    async fn open_file(
        &self,
        path: &Path,
        write_access: bool,
        truncate: bool,
    ) -> Result<FileAttributes, JjError> {
        if !write_access && !truncate {
            return self.get_attributes(path).await;
        }
        let router_path = self.mapper.to_router_path(path)?;
        let (overlay, backend, base_hex, rel_path) = self
            .mapper
            .router()
            .resolve_mutable_workspace(&router_path)
            .await?;
        overlay
            .open_file(&backend, &base_hex, &rel_path, write_access, truncate)
            .await
    }

    async fn create_file(
        &self,
        path: &Path,
        executable: bool,
        exclusive: bool,
    ) -> Result<FileAttributes, JjError> {
        let router_path = self.mapper.to_router_path(path)?;
        let (overlay, backend, base_hex, rel_path) = self
            .mapper
            .router()
            .resolve_mutable_workspace(&router_path)
            .await?;
        overlay
            .create_file(&backend, &base_hex, &rel_path, executable, exclusive)
            .await
    }

    async fn write(&self, path: &Path, offset: u64, data: &[u8]) -> Result<u32, JjError> {
        let router_path = self.mapper.to_router_path(path)?;
        let (overlay, backend, base_hex, rel_path) = self
            .mapper
            .router()
            .resolve_mutable_workspace(&router_path)
            .await?;
        overlay
            .write_at(&backend, &base_hex, &rel_path, offset, data)
            .await
    }

    async fn fsync(&self, path: &Path, datasync: bool) -> Result<(), JjError> {
        let router_path = self.mapper.to_router_path(path)?;
        if let Ok((overlay, _, _, rel_path)) = self
            .mapper
            .router()
            .resolve_mutable_workspace(&router_path)
            .await
        {
            overlay.fsync(&rel_path, datasync).await?;
        }
        Ok(())
    }

    async fn set_attributes(
        &self,
        path: &Path,
        changes: AttributeChanges,
    ) -> Result<FileAttributes, JjError> {
        let router_path = self.mapper.to_router_path(path)?;
        let (overlay, backend, base_hex, rel_path) = self
            .mapper
            .router()
            .resolve_mutable_workspace(&router_path)
            .await?;
        overlay
            .set_attributes(&backend, &base_hex, &rel_path, changes)
            .await
    }

    async fn unlink(&self, path: &Path) -> Result<(), JjError> {
        let router_path = self.mapper.to_router_path(path)?;
        let (overlay, backend, base_hex, rel_path) = self
            .mapper
            .router()
            .resolve_mutable_workspace(&router_path)
            .await?;
        overlay.unlink(&backend, &base_hex, &rel_path).await
    }

    async fn mkdir(&self, path: &Path, mode: u32) -> Result<FileAttributes, JjError> {
        let router_path = self.mapper.to_router_path(path)?;
        let (overlay, backend, base_hex, rel_path) = self
            .mapper
            .router()
            .resolve_mutable_workspace(&router_path)
            .await?;
        overlay.mkdir(&backend, &base_hex, &rel_path, mode).await
    }

    async fn rmdir(&self, path: &Path) -> Result<(), JjError> {
        let router_path = self.mapper.to_router_path(path)?;
        let (overlay, backend, base_hex, rel_path) = self
            .mapper
            .router()
            .resolve_mutable_workspace(&router_path)
            .await?;
        overlay.rmdir(&backend, &base_hex, &rel_path).await
    }

    async fn symlink(&self, link_path: &Path, target: &Path) -> Result<FileAttributes, JjError> {
        let router_path = self.mapper.to_router_path(link_path)?;
        let (overlay, backend, base_hex, rel_path) = self
            .mapper
            .router()
            .resolve_mutable_workspace(&router_path)
            .await?;
        overlay
            .symlink(&backend, &base_hex, &rel_path, target)
            .await
    }

    async fn rename(&self, old_path: &Path, new_path: &Path) -> Result<(), JjError> {
        let old_router = self.mapper.to_router_path(old_path)?;
        let new_router = self.mapper.to_router_path(new_path)?;
        let Some((old_key, old_rel)) = self.mapper.router().parse_workspace_path(&old_router)?
        else {
            return Err(JjError::ReadOnly);
        };
        let Some((new_key, new_rel)) = self.mapper.router().parse_workspace_path(&new_router)?
        else {
            return Err(JjError::ReadOnly);
        };
        if old_key != new_key {
            return Err(JjError::CrossDevice);
        }
        let (overlay, backend, base_hex, _) = self
            .mapper
            .router()
            .resolve_mutable_workspace(&old_router)
            .await?;
        overlay
            .rename(&backend, &base_hex, &old_rel, &new_rel)
            .await
    }
}

fn split_clean_segments(path: &Path) -> Result<Vec<String>, JjError> {
    let mut out = Vec::new();
    for comp in path.components() {
        match comp {
            std::path::Component::CurDir | std::path::Component::RootDir => {}
            std::path::Component::Normal(os) => {
                let s = os.to_str().ok_or(JjError::InvalidPath)?;
                if s.is_empty() || s.contains('\0') {
                    return Err(JjError::InvalidPath);
                }
                out.push(s.to_string());
            }
            std::path::Component::ParentDir | std::path::Component::Prefix(_) => {
                return Err(JjError::InvalidPath);
            }
        }
    }
    Ok(out)
}

fn join_segments(segs: &[&str]) -> PathBuf {
    let mut p = PathBuf::new();
    for s in segs {
        p.push(s);
    }
    p
}

fn bookmark_children(
    bookmarks: &std::collections::BTreeMap<String, String>,
    dir_prefix: &str,
) -> Vec<DirectoryEntry> {
    let mut dirs = BTreeSet::new();
    let mut links = BTreeSet::new();
    for name in bookmarks.keys() {
        let Some(rem) = name.strip_prefix(dir_prefix) else {
            continue;
        };
        if let Some((first, _)) = rem.split_once('/') {
            dirs.insert(first.to_string());
        } else if !rem.is_empty() {
            links.insert(rem.to_string());
        }
    }
    let mut entries = Vec::with_capacity(dirs.len() + links.len());
    for d in dirs {
        entries.push(DirectoryEntry {
            name: d,
            file_type: FileType::Directory,
        });
    }
    for l in links {
        if !entries.iter().any(|e| e.name == l) {
            entries.push(DirectoryEntry {
                name: l,
                file_type: FileType::Symlink,
            });
        }
    }
    entries
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use jj_lib::object_id::ObjectId as _;
    use jj_lib::repo::Repo as _;
    use vfs_proto::InstancePaths;
    use vfs_proto::SourceConfig;
    use vfs_proto::SourceKind;

    use super::*;
    use crate::backend::encode_hex_to_reverse_hex;
    use crate::cas::BlobCas;
    use crate::test_helpers::setup_test_repo;

    #[tokio::test]
    async fn namespace_router_resolves_commits_changes_bookmarks_and_workspaces() {
        let (ws_tmp, repo, cid) = setup_test_repo().await;
        let state_tmp = tempfile::tempdir().unwrap();
        let paths = InstancePaths::resolve(Some("ns-unit"), Some(state_tmp.path()));
        paths.ensure_dirs().unwrap();
        let cas = Arc::new(BlobCas::new(&paths.cache_dir, 0).unwrap());
        let registry = Arc::new(SourceRegistry::load(paths.clone(), cas).unwrap());

        let repo_name = ws_tmp
            .path()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        registry
            .add_source(
                "local".into(),
                SourceConfig {
                    kind: SourceKind::LocalJj,
                    url: None,
                    daemon_socket: None,
                    user: None,
                    auth_token_env: None,
                    auth_token_cmd: vec![],
                    path: Some(ws_tmp.path().to_path_buf()),
                    search_paths: vec![],
                    repos: BTreeMap::new(),
                    ephemeral: true,
                },
            )
            .unwrap();

        let overlays = Arc::new(OverlayManager::new(paths.state_dir.join("overlays")).unwrap());
        let router = Arc::new(NamespaceRouter::new(registry, overlays));

        // 1. Change ID reverse-hex symlink -> ../commits/<full_hex>
        // (`setup_test_repo` uses `0000...abcd`, sharing its first 28 zero hex
        // digits with root)
        let commit_obj = repo.store().get_commit(&cid).unwrap();
        let chg_rev = encode_hex_to_reverse_hex(&commit_obj.change_id().hex()).unwrap();
        let chg_entry = router
            .get_entry(Path::new(&format!(
                "local/repos/{repo_name}/changes/{chg_rev}"
            )))
            .await
            .unwrap();
        assert_eq!(
            chg_entry.read_link().await.unwrap(),
            PathBuf::from(format!("../commits/{}", cid.hex()))
        );

        // 2. Nested bookmark helper
        let mut bm = BTreeMap::new();
        bm.insert("main".to_string(), cid.hex());
        bm.insert("release/v1".to_string(), cid.hex());
        let root_bm = bookmark_children(&bm, "");
        assert_eq!(root_bm.len(), 2);
        let rel_bm = bookmark_children(&bm, "release/");
        assert_eq!(rel_bm.len(), 1);
        assert_eq!(rel_bm[0].name, "v1");
        assert_eq!(rel_bm[0].file_type, FileType::Symlink);

        // 3. Canonical workspace path parsing + LocalJj repo alias
        //    canonicalization
        let parsed = router
            .parse_workspace_path(Path::new(&format!(
                "local/repos/{repo_name}/users/local/default/src/lib.rs"
            )))
            .unwrap()
            .unwrap();
        assert_eq!(parsed.0.workspace, "default");
        assert_eq!(parsed.1, PathBuf::from("src/lib.rs"));

        let parsed_via_alias = router
            .parse_workspace_path(Path::new(
                "local/repos/local/users/local/default/src/lib.rs",
            ))
            .unwrap()
            .unwrap();
        assert_eq!(parsed_via_alias.0, parsed.0);

        // 4. Writing via `local:<alias>/default` and reading via
        //    `local:<dirname>/default` share one WorkspaceOverlay, and `.jj` on
        //    `local-jj` rejects writes with ReadOnly!
        let mut alias_scope = MountScope::parse(Some("local:local/default")).unwrap();
        router.canonicalize_mount_scope(&mut alias_scope);
        let dir_scope = MountScope::parse(Some(&format!("local:{repo_name}/default"))).unwrap();
        assert_eq!(alias_scope, dir_scope);

        let vfs_alias = RouterVfs::new(SubtreePathMapper::new(
            Arc::clone(&router),
            alias_scope.clone(),
            alias_scope.router_prefix(Some("local")),
        ));
        let vfs_dir = RouterVfs::new(SubtreePathMapper::new(
            Arc::clone(&router),
            dir_scope.clone(),
            dir_scope.router_prefix(Some("local")),
        ));

        vfs_alias
            .create_file(Path::new("shared.txt"), false, true)
            .await
            .unwrap();
        vfs_alias
            .write(Path::new("shared.txt"), 0, b"one-overlay")
            .await
            .unwrap();
        assert_eq!(
            &*vfs_dir.read(Path::new("shared.txt"), 0, 32).await.unwrap(),
            b"one-overlay"
        );
        assert_eq!(router.overlays().active_overlays().len(), 1);

        // `.jj` inside `local-jj` workspace is readable, but any write returns
        // `ReadOnly` (EROFS)
        let jj_attr = vfs_dir.get_attributes(Path::new(".jj")).await.unwrap();
        assert_eq!(jj_attr.file_type, FileType::Directory);
        assert!(matches!(
            vfs_dir
                .create_file(Path::new(".jj/working_copy/lock"), false, false)
                .await,
            Err(JjError::ReadOnly)
        ));
    }
}
