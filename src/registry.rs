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

//! Dynamic multi-source repository registry (`SourceRegistry`) with lazy
//! backend activation and `$XDG_CONFIG_HOME/jj-vfs/config.toml` persistence.
//!
//! # Authority and Crash-Safety Ordering
//! `add_source` and `remove_source` persist non-ephemeral source definitions
//! via `VfsConfigFile::save_atomic` before returning `Ok`. Registering a source
//! does *not* dial gRPC or open packfiles; `resolve_repo` instantiates backends
//! lazily on first path traversal into `<source>/repos/<repo>`.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::RwLock;
use vfs_proto::ConfiguredMount;
use vfs_proto::InstancePaths;
use vfs_proto::SourceConfig;
use vfs_proto::SourceKind;
use vfs_proto::VfsConfigFile;
use vfs_proto::encode_repo_segment;

use crate::backend::ReadOnlyVfsBackend;
use crate::backend::commit_cloud::CcSourceAuth;
use crate::backend::commit_cloud::CommitCloudVfsBackend;
use crate::backend::local::LocalRepoVfsBackend;
use crate::cas::BlobCas;
use crate::jj_error::JjError;

const MAX_UNVERIFIED_CC_REPOS: usize = 64;

struct RegistryState {
    persisted: VfsConfigFile,
    ephemeral_sources: BTreeMap<String, SourceConfig>,
}

impl RegistryState {
    fn get_source(&self, alias: &str) -> Option<&SourceConfig> {
        self.ephemeral_sources
            .get(alias)
            .or_else(|| self.persisted.sources.get(alias))
    }

    fn has_source(&self, alias: &str) -> bool {
        self.ephemeral_sources.contains_key(alias) || self.persisted.sources.contains_key(alias)
    }

    fn merged_sources(&self) -> BTreeMap<String, SourceConfig> {
        let mut merged = self.persisted.sources.clone();
        for (alias, cfg) in &self.ephemeral_sources {
            merged.insert(alias.clone(), cfg.clone());
        }
        merged
    }
}

fn is_valid_source_alias(alias: &str) -> bool {
    !alias.is_empty()
        && alias.trim() == alias
        && alias != "."
        && alias != ".."
        && !alias.contains('/')
        && !alias.contains(':')
        && !alias.contains('@')
        && !alias.chars().any(char::is_control)
}

fn is_valid_repo_segment(seg: &str) -> bool {
    !seg.is_empty() && seg != "." && seg != ".." && !seg.contains('/') && !seg.contains('\0')
}

pub struct SourceRegistry {
    paths: InstancePaths,
    cas: Arc<BlobCas>,
    state: RwLock<RegistryState>,
    active_repos: DashMap<(String, String), Arc<dyn ReadOnlyVfsBackend>>,
    active_cc_repos: DashMap<(String, String), Arc<CommitCloudVfsBackend>>,
}

impl SourceRegistry {
    pub fn load(paths: InstancePaths, cas: Arc<BlobCas>) -> Result<Self, JjError> {
        let mut cfg = VfsConfigFile::load_or_default(&paths.config_path)
            .map_err(|e| std::io::Error::other(format!("failed to load config.toml: {e}")))?;
        let mut ephemeral_sources = BTreeMap::new();
        cfg.sources.retain(|alias, src| {
            if src.ephemeral {
                ephemeral_sources.insert(alias.clone(), src.clone());
                false
            } else {
                true
            }
        });
        cas.set_max_bytes(cfg.daemon.max_cache_bytes);
        Ok(Self {
            paths,
            cas,
            state: RwLock::new(RegistryState {
                persisted: cfg,
                ephemeral_sources,
            }),
            active_repos: DashMap::new(),
            active_cc_repos: DashMap::new(),
        })
    }

    pub fn paths(&self) -> &InstancePaths {
        &self.paths
    }

    pub fn cas(&self) -> &Arc<BlobCas> {
        &self.cas
    }

    pub fn snapshot_debounce_ms(&self) -> u64 {
        self.state.read().persisted.daemon.snapshot_debounce_ms
    }

    pub fn configured_root_mount(&self) -> Option<PathBuf> {
        self.state.read().persisted.daemon.root_mount.clone()
    }

    pub fn configured_mounts(&self) -> Vec<ConfiguredMount> {
        self.state.read().persisted.mounts.clone()
    }

    pub fn record_persisted_mount(
        &self,
        target: Option<String>,
        mountpoint: PathBuf,
    ) -> Result<(), JjError> {
        let mut guard = self.state.write();
        let mut next = guard.persisted.clone();
        next.mounts.retain(|m| m.mountpoint != mountpoint);
        next.mounts.push(ConfiguredMount { target, mountpoint });
        next.save_atomic(&self.paths.config_path)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        guard.persisted = next;
        Ok(())
    }

    pub fn remove_persisted_mount(&self, mountpoint: &Path) -> Result<(), JjError> {
        let mut guard = self.state.write();
        if !guard
            .persisted
            .mounts
            .iter()
            .any(|m| m.mountpoint == mountpoint)
        {
            return Ok(());
        }
        let mut next = guard.persisted.clone();
        next.mounts.retain(|m| m.mountpoint != mountpoint);
        next.save_atomic(&self.paths.config_path)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        guard.persisted = next;
        Ok(())
    }

    pub fn list_sources(&self) -> BTreeMap<String, SourceConfig> {
        self.state.read().merged_sources()
    }

    pub fn has_source(&self, alias: &str) -> bool {
        self.state.read().has_source(alias)
    }

    pub fn default_user_for_source(&self, alias: &str) -> Option<String> {
        let guard = self.state.read();
        let src = guard.get_source(alias)?;
        match src.kind {
            SourceKind::CommitCloud => {
                Some(src.user.clone().unwrap_or_else(|| "default".to_string()))
            }
            SourceKind::LocalJj | SourceKind::LocalGit | SourceKind::LocalDir => {
                Some("local".to_string())
            }
        }
    }

    pub fn add_source(&self, alias: String, source_cfg: SourceConfig) -> Result<(), JjError> {
        if !is_valid_source_alias(&alias) {
            return Err(JjError::InvalidPath);
        }
        {
            let mut guard = self.state.write();
            if source_cfg.ephemeral {
                guard.ephemeral_sources.insert(alias.clone(), source_cfg);
            } else {
                let mut next = guard.persisted.clone();
                next.sources.insert(alias.clone(), source_cfg);
                next.save_atomic(&self.paths.config_path)
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                guard.persisted = next;
                guard.ephemeral_sources.remove(&alias);
            }
        }
        self.evict_cached_source(&alias);
        Ok(())
    }

    pub fn remove_source<F>(&self, alias: &str, force: bool, is_in_use: F) -> Result<(), JjError>
    where
        F: FnOnce(&str) -> bool,
    {
        if !self.has_source(alias) {
            return Err(JjError::NotFound);
        }
        if !force && is_in_use(alias) {
            return Err(JjError::Busy);
        }
        {
            let mut guard = self.state.write();
            if guard.persisted.sources.contains_key(alias) {
                let mut next = guard.persisted.clone();
                next.sources.remove(alias);
                next.save_atomic(&self.paths.config_path)
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                guard.persisted = next;
            }
            guard.ephemeral_sources.remove(alias);
        }
        self.evict_cached_source(alias);
        Ok(())
    }

    fn evict_cached_source(&self, alias: &str) {
        self.active_repos.retain(|(src, _), _| src != alias);
        self.active_cc_repos.retain(|(src, _), _| src != alias);
    }

    fn prune_unverified_cc_repos_if_needed(&self) {
        let unverified_count = self
            .active_cc_repos
            .iter()
            .filter(|e| !e.value().is_verified())
            .count();
        if unverified_count < MAX_UNVERIFIED_CC_REPOS {
            return;
        }
        let mut stale_keys = Vec::new();
        for entry in self.active_cc_repos.iter() {
            if !entry.value().is_verified() {
                stale_keys.push(entry.key().clone());
            }
        }
        for key in stale_keys {
            self.active_cc_repos.remove(&key);
            self.active_repos.remove(&key);
        }
    }

    pub fn has_active_repo(&self, alias: &str) -> bool {
        self.active_repos.iter().any(|entry| {
            if entry.key().0 != alias {
                return false;
            }
            if let Some(cc) = self.active_cc_repos.get(entry.key()) {
                cc.is_verified()
            } else {
                true
            }
        })
    }

    /// Lists repositories currently known under `source_alias` (`ls
    /// <source>/repos/`).
    pub fn list_known_repos(&self, source_alias: &str) -> Result<Vec<String>, JjError> {
        let src = self
            .state
            .read()
            .get_source(source_alias)
            .cloned()
            .ok_or(JjError::NotFound)?;

        let mut names = BTreeSet::new();
        match src.kind {
            SourceKind::CommitCloud => {
                for item in self.active_cc_repos.iter() {
                    if item.key().0 == source_alias && item.value().is_verified() {
                        names.insert(item.key().1.clone());
                    }
                }
            }
            SourceKind::LocalJj | SourceKind::LocalGit => {
                for item in self.active_repos.iter() {
                    if item.key().0 == source_alias {
                        names.insert(item.key().1.clone());
                    }
                }
                if let Some(p) = &src.path {
                    let repo_name = p
                        .file_name()
                        .and_then(|s| s.to_str())
                        .unwrap_or(source_alias)
                        .to_string();
                    names.insert(encode_repo_segment(&repo_name));
                }
            }
            SourceKind::LocalDir => {
                for item in self.active_repos.iter() {
                    if item.key().0 == source_alias {
                        names.insert(item.key().1.clone());
                    }
                }
                for key in src.repos.keys() {
                    names.insert(encode_repo_segment(key));
                }
                for dir in &src.search_paths {
                    let expanded = expand_tilde(dir);
                    if let Ok(rd) = std::fs::read_dir(expanded) {
                        for entry in rd.flatten() {
                            let p = entry.path();
                            if p.is_dir()
                                && (p.join(".jj").exists() || p.join(".git").exists())
                                && let Some(n) = p.file_name().and_then(|s| s.to_str())
                            {
                                names.insert(encode_repo_segment(n));
                            }
                        }
                    }
                }
            }
        }
        Ok(names.into_iter().collect())
    }

    pub fn resolve_cc_repo(
        &self,
        source_alias: &str,
        repo_segment: &str,
    ) -> Result<Option<Arc<CommitCloudVfsBackend>>, JjError> {
        let _ = self.resolve_repo(source_alias, repo_segment)?;
        Ok(self
            .active_cc_repos
            .get(&(source_alias.to_string(), repo_segment.to_string()))
            .map(|r| Arc::clone(&r)))
    }

    /// Lazily instantiates or returns the `ReadOnlyVfsBackend` for
    /// `(source_alias, repo_segment)`.
    pub fn resolve_repo(
        &self,
        source_alias: &str,
        repo_segment: &str,
    ) -> Result<Arc<dyn ReadOnlyVfsBackend>, JjError> {
        if !is_valid_repo_segment(repo_segment) {
            return Err(JjError::InvalidPath);
        }
        let cache_key = (source_alias.to_string(), repo_segment.to_string());
        if let Some(hit) = self.active_repos.get(&cache_key) {
            return Ok(Arc::clone(&hit));
        }

        let src = self
            .state
            .read()
            .get_source(source_alias)
            .cloned()
            .ok_or(JjError::NotFound)?;

        if src.kind == SourceKind::CommitCloud {
            let url = src.url.ok_or(JjError::InvalidPath)?;
            self.prune_unverified_cc_repos_if_needed();
            let decoded_repo_id = repo_segment.replace("__", "/");
            let cc = self
                .active_cc_repos
                .entry(cache_key.clone())
                .or_insert_with(|| {
                    Arc::new(CommitCloudVfsBackend::new(
                        source_alias,
                        decoded_repo_id,
                        url,
                        CcSourceAuth {
                            daemon_socket: src.daemon_socket.map(|p| expand_tilde(&p)),
                            user: src.user,
                            auth_token_env: src.auth_token_env,
                            auth_token_cmd: src.auth_token_cmd,
                        },
                        Arc::clone(&self.cas),
                    ))
                })
                .clone();
            let dyn_backend: Arc<dyn ReadOnlyVfsBackend> = cc;
            self.active_repos
                .insert(cache_key, Arc::clone(&dyn_backend));
            return Ok(dyn_backend);
        }

        let backend: Arc<dyn ReadOnlyVfsBackend> = match src.kind {
            SourceKind::CommitCloud => unreachable!(),
            SourceKind::LocalJj => {
                let path = expand_tilde(src.path.as_deref().ok_or(JjError::NotFound)?);
                let expected = path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .map(encode_repo_segment)
                    .unwrap_or_else(|| source_alias.to_string());
                if repo_segment != expected && repo_segment != source_alias {
                    return Err(JjError::NotFound);
                }
                Arc::new(LocalRepoVfsBackend::open_jj_workspace(
                    repo_segment,
                    path,
                    Arc::clone(&self.cas),
                )?)
            }
            SourceKind::LocalGit => {
                let path = expand_tilde(src.path.as_deref().ok_or(JjError::NotFound)?);
                let expected = path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .map(encode_repo_segment)
                    .unwrap_or_else(|| source_alias.to_string());
                if repo_segment != expected && repo_segment != source_alias {
                    return Err(JjError::NotFound);
                }
                Arc::new(LocalRepoVfsBackend::open_git_repo(
                    repo_segment,
                    path,
                    &self.paths.cache_dir,
                    Arc::clone(&self.cas),
                )?)
            }
            SourceKind::LocalDir => {
                let candidate_path =
                    find_local_dir_repo(&src, repo_segment).ok_or(JjError::NotFound)?;
                if candidate_path.join(".jj").exists() {
                    Arc::new(LocalRepoVfsBackend::open_jj_workspace(
                        repo_segment,
                        candidate_path,
                        Arc::clone(&self.cas),
                    )?)
                } else if candidate_path.join(".git").exists()
                    || candidate_path.join("HEAD").is_file()
                {
                    Arc::new(LocalRepoVfsBackend::open_git_repo(
                        repo_segment,
                        candidate_path,
                        &self.paths.cache_dir,
                        Arc::clone(&self.cas),
                    )?)
                } else {
                    return Err(JjError::NotFound);
                }
            }
        };

        let existing = self.active_repos.entry(cache_key).or_insert(backend);
        Ok(Arc::clone(&existing))
    }
}

fn find_local_dir_repo(src: &SourceConfig, repo_segment: &str) -> Option<PathBuf> {
    for (alias, path) in &src.repos {
        if encode_repo_segment(alias) == repo_segment {
            let expanded = expand_tilde(path);
            if expanded.exists() {
                return Some(expanded);
            }
        }
    }
    let decoded = repo_segment.replace("__", "/");
    for search_dir in &src.search_paths {
        let candidate = expand_tilde(search_dir).join(&decoded);
        if candidate.is_dir() && (candidate.join(".jj").exists() || candidate.join(".git").exists())
        {
            return Some(candidate);
        }
    }
    None
}

pub fn expand_tilde(path: &Path) -> PathBuf {
    if let Ok(stripped) = path.strip_prefix("~")
        && let Some(home) = std::env::var_os("HOME")
    {
        return PathBuf::from(home).join(stripped);
    }
    path.to_path_buf()
}

#[cfg(test)]
mod tests {
    use jj_lib::object_id::ObjectId as _;

    use super::*;
    use crate::test_helpers::setup_test_repo;

    #[tokio::test]
    async fn registry_persists_sources_lazily_activates_and_enforces_ebusy() {
        let state_tmp = tempfile::tempdir().unwrap();
        let paths = InstancePaths::resolve(Some("unit"), Some(state_tmp.path()));
        paths.ensure_dirs().unwrap();
        let cas = Arc::new(BlobCas::new(&paths.cache_dir, 0).unwrap());
        let reg = SourceRegistry::load(paths.clone(), cas.clone()).unwrap();

        // Add Commit Cloud source -> zero network calls at registration time!
        reg.add_source(
            "corp".into(),
            SourceConfig {
                kind: SourceKind::CommitCloud,
                url: Some("http://127.0.0.1:59999".into()),
                daemon_socket: None,
                user: Some("alice".into()),
                auth_token_env: None,
                auth_token_cmd: vec![],
                path: None,
                search_paths: vec![],
                repos: BTreeMap::new(),
                ephemeral: false,
            },
        )
        .unwrap();

        // Reload from disk config.toml to verify atomic persistence
        let reloaded = SourceRegistry::load(paths, cas).unwrap();
        assert!(reloaded.has_source("corp"));
        assert_eq!(
            reloaded.default_user_for_source("corp").as_deref(),
            Some("alice")
        );

        // Removing an in-use source without --force returns JjError::Busy
        // (EBUSY)
        let err = reloaded.remove_source("corp", false, |_| true).unwrap_err();
        assert!(matches!(err, JjError::Busy));
        reloaded.remove_source("corp", true, |_| true).unwrap();
        assert!(!reloaded.has_source("corp"));

        // Local jj workspace resolution
        let (ws_tmp, _repo, cid) = setup_test_repo().await;
        reloaded
            .add_source(
                "localws".into(),
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
        let backend = reloaded.resolve_repo("localws", "localws").unwrap();
        let info = backend
            .lookup_in_commit(&cid.hex(), Path::new("file1.txt"))
            .await
            .unwrap();
        assert_eq!(info.attr.size, 15);
    }

    #[test]
    fn registry_rejects_dot_aliases_and_hides_unverified_cc_probes() {
        let state_tmp = tempfile::tempdir().unwrap();
        let paths = InstancePaths::resolve(Some("reg-hardening"), Some(state_tmp.path()));
        paths.ensure_dirs().unwrap();
        let cas = Arc::new(BlobCas::new(&paths.cache_dir, 0).unwrap());
        let reg = SourceRegistry::load(paths.clone(), cas.clone()).unwrap();

        let mk_cc = |url: &str, ephemeral: bool| SourceConfig {
            kind: SourceKind::CommitCloud,
            url: Some(url.into()),
            daemon_socket: None,
            user: Some("alice".into()),
            auth_token_env: None,
            auth_token_cmd: vec![],
            path: None,
            search_paths: vec![],
            repos: BTreeMap::new(),
            ephemeral,
        };

        for bad in [".", "..", "", " lead", "trail ", "a/b", "a:b", "a@b"] {
            assert!(
                matches!(
                    reg.add_source(bad.into(), mk_cc("http://127.0.0.1:8080", true)),
                    Err(JjError::InvalidPath)
                ),
                "expected InvalidPath for alias '{bad}'"
            );
        }

        // 1. Persist `corp` to `config.toml`, then override `corp` with
        //    `--ephemeral` and add another persisted source `oss`. Reloading
        //    `config.toml` must still have the original persisted `corp` AND
        //    `oss`!
        reg.add_source("corp".into(), mk_cc("https://persisted.example.com", false))
            .unwrap();
        reg.add_source("corp".into(), mk_cc("http://127.0.0.1:9999", true))
            .unwrap();
        assert_eq!(
            reg.list_sources()
                .get("corp")
                .and_then(|s| s.url.as_deref()),
            Some("http://127.0.0.1:9999")
        );
        reg.add_source("oss".into(), mk_cc("https://oss.example.com", false))
            .unwrap();

        let reloaded = SourceRegistry::load(paths, cas).unwrap();
        assert_eq!(
            reloaded
                .list_sources()
                .get("corp")
                .and_then(|s| s.url.as_deref()),
            Some("https://persisted.example.com")
        );
        assert!(reloaded.has_source("oss"));

        // 2. Probing unverified Commit Cloud repo names (`.git`, `typo`) must
        //    NOT populate `list_known_repos` or `has_active_repo`.
        let _ = reloaded.resolve_repo("corp", ".git").unwrap();
        let _ = reloaded.resolve_repo("corp", "typo").unwrap();
        assert!(reloaded.list_known_repos("corp").unwrap().is_empty());
        assert!(!reloaded.has_active_repo("corp"));
    }
}
