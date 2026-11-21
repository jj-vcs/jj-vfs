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

//! Per-user `jjfsd` runtime (`DaemonRuntime`) coordinating `SourceRegistry`,
//! `NamespaceRouter`, `BlobCas`, concurrent FUSE background sessions, and
//! `control.sock` IPC commands.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::Notify;
use vfs_proto::ActiveMountInfo;
use vfs_proto::ControlRequest;
use vfs_proto::ControlResponse;
use vfs_proto::DEFAULT_MAX_CACHE_BYTES;
use vfs_proto::DaemonStatus;
use vfs_proto::InstancePaths;
use vfs_proto::MountScope;

use crate::cas::BlobCas;
use crate::fuse::JjFuse;
use crate::inode_map::InodeMap;
use crate::ipc_server::ControlHandler;
use crate::jj_error::JjError;
use crate::namespace::NamespaceRouter;
use crate::namespace::RouterVfs;
use crate::namespace::SubtreePathMapper;
use crate::overlay::OverlayManager;
use crate::registry::SourceRegistry;
use crate::registry::expand_tilde;

struct ActiveMountEntry {
    info: ActiveMountInfo,
    inode_map: Arc<InodeMap>,
    session: fuser::BackgroundSession,
}

struct MountReservation<'a> {
    mounting: &'a Mutex<BTreeSet<PathBuf>>,
    mountpoint: Option<PathBuf>,
}

impl Drop for MountReservation<'_> {
    fn drop(&mut self) {
        if let Some(mp) = self.mountpoint.take() {
            self.mounting.lock().remove(&mp);
        }
    }
}

pub struct DaemonRuntime {
    registry: Arc<SourceRegistry>,
    overlays: Arc<OverlayManager>,
    router: Arc<NamespaceRouter>,
    mounts: Mutex<BTreeMap<PathBuf, ActiveMountEntry>>,
    mounting: Mutex<BTreeSet<PathBuf>>,
}

impl DaemonRuntime {
    pub fn new(paths: InstancePaths) -> Result<Arc<Self>, JjError> {
        paths.ensure_dirs()?;
        let cas = Arc::new(BlobCas::new(&paths.cache_dir, DEFAULT_MAX_CACHE_BYTES)?);
        let overlays = Arc::new(OverlayManager::new(paths.state_dir.join("overlays"))?);
        let registry = Arc::new(SourceRegistry::load(paths, cas)?);
        let router = Arc::new(NamespaceRouter::new(
            Arc::clone(&registry),
            Arc::clone(&overlays),
        ));
        Ok(Arc::new(Self {
            registry,
            overlays,
            router,
            mounts: Mutex::new(BTreeMap::new()),
            mounting: Mutex::new(BTreeSet::new()),
        }))
    }

    pub fn registry(&self) -> &Arc<SourceRegistry> {
        &self.registry
    }

    pub fn overlays(&self) -> &Arc<OverlayManager> {
        &self.overlays
    }

    pub fn router(&self) -> &Arc<NamespaceRouter> {
        &self.router
    }

    /// Mounts `daemon.root_mount` and `[[mounts]]` entries from `config.toml`
    /// on startup.
    pub async fn mount_configured(&self) {
        if let Some(root_mp) = self.registry.configured_root_mount() {
            let expanded = expand_tilde(&root_mp);
            if let Err(err) = self.mount_target(None, expanded.clone(), false).await {
                tracing::warn!(mountpoint = %expanded.display(), error = %err, "failed to auto-mount root_mount");
            }
        }
        for cfg_mnt in self.registry.configured_mounts() {
            let expanded = expand_tilde(&cfg_mnt.mountpoint);
            if let Err(err) = self
                .mount_target(cfg_mnt.target, expanded.clone(), false)
                .await
            {
                tracing::warn!(mountpoint = %expanded.display(), error = %err, "failed to auto-mount configured entry");
            }
        }
    }

    /// Attaches either the Root Namespace Mount (`target == None`) or a Scoped
    /// Direct Mount (`target == Some(...)`) onto `mountpoint`.
    pub async fn mount_target(
        &self,
        target: Option<String>,
        mountpoint: PathBuf,
        persist: bool,
    ) -> Result<ActiveMountInfo, JjError> {
        let mut scope = MountScope::parse(target.as_deref()).map_err(|_| JjError::InvalidPath)?;
        let default_user = match &scope {
            MountScope::Root => None,
            MountScope::Source { source }
            | MountScope::User { source, .. }
            | MountScope::Repo { source, .. }
            | MountScope::Workspace { source, .. } => {
                if !self.registry.has_source(source) {
                    return Err(JjError::NotFound);
                }
                self.registry.default_user_for_source(source)
            }
        };
        self.router.canonicalize_mount_scope(&mut scope);
        let router_prefix = scope.router_prefix(default_user.as_deref());

        if is_stale_fuse_mount(&mountpoint) {
            detach_stale_fuse_mount(&mountpoint);
        }
        std::fs::create_dir_all(&mountpoint)?;
        let canon_mp = match std::fs::canonicalize(&mountpoint) {
            Ok(p) => p,
            Err(ref e) if e.raw_os_error() == Some(libc::ENOTCONN) => {
                detach_stale_fuse_mount(&mountpoint);
                std::fs::canonicalize(&mountpoint).unwrap_or(mountpoint)
            }
            Err(_) => mountpoint,
        };

        {
            let mounts = self.mounts.lock();
            if mounts.contains_key(&canon_mp) {
                return Err(JjError::AlreadyExists);
            }
            if !self.mounting.lock().insert(canon_mp.clone()) {
                return Err(JjError::AlreadyExists);
            }
        }
        let mut reservation = MountReservation {
            mounting: &self.mounting,
            mountpoint: Some(canon_mp.clone()),
        };

        let mapper = SubtreePathMapper::new(
            Arc::clone(&self.router),
            scope.clone(),
            router_prefix.clone(),
        );
        let vfs = Arc::new(RouterVfs::new(mapper));
        let inode_map = Arc::new(InodeMap::new());
        let fuse = JjFuse::with_inode_map(
            vfs,
            Arc::clone(&inode_map),
            tokio::runtime::Handle::current(),
        );

        let mp_for_mount = canon_mp.clone();
        let session = tokio::task::spawn_blocking(move || {
            let mut config = fuser::Config::default();
            config.mount_options = vec![fuser::MountOption::FSName("jjfs".to_string())];
            fuser::spawn_mount(fuse, &mp_for_mount, &config)
        })
        .await
        .map_err(|e| std::io::Error::other(format!("mount task panicked: {e}")))??;

        if persist {
            self.registry
                .record_persisted_mount(target, canon_mp.clone())?;
        }

        let info = ActiveMountInfo {
            mountpoint: canon_mp.clone(),
            scope,
            router_prefix,
        };
        let _ = &inode_map;
        {
            let mut mounts = self.mounts.lock();
            mounts.insert(
                canon_mp,
                ActiveMountEntry {
                    info: info.clone(),
                    inode_map,
                    session,
                },
            );
            if let Some(mp) = reservation.mountpoint.take() {
                self.mounting.lock().remove(&mp);
            }
        }
        Ok(info)
    }

    /// Detaches a single active FUSE mountpoint.
    pub async fn unmount_target(&self, mountpoint: &Path) -> Result<PathBuf, JjError> {
        let canon_mp =
            std::fs::canonicalize(mountpoint).unwrap_or_else(|_| mountpoint.to_path_buf());
        let removed = self
            .mounts
            .lock()
            .remove(&canon_mp)
            .ok_or(JjError::NotFound)?;
        let _ = self.registry.remove_persisted_mount(&canon_mp);
        tokio::task::spawn_blocking(move || drop(removed.session))
            .await
            .map_err(|e| std::io::Error::other(format!("unmount task panicked: {e}")))?;
        Ok(canon_mp)
    }

    /// Cleanly detaches all active FUSE sessions before daemon shutdown.
    pub async fn unmount_all(&self) {
        let entries: Vec<ActiveMountEntry> = {
            let mut guard = self.mounts.lock();
            std::mem::take(&mut *guard).into_values().collect()
        };
        if entries.is_empty() {
            return;
        }
        let _ = tokio::task::spawn_blocking(move || {
            for entry in entries {
                let _ = entry.inode_map.get_path(crate::inode_map::ROOT_INODE);
                drop(entry.session);
            }
        })
        .await;
    }

    fn is_source_mounted(&self, alias: &str) -> bool {
        let guard = self.mounts.lock();
        guard.values().any(|m| match &m.info.scope {
            MountScope::Root => false,
            MountScope::Source { source }
            | MountScope::User { source, .. }
            | MountScope::Repo { source, .. }
            | MountScope::Workspace { source, .. } => source == alias,
        })
    }

    pub fn active_mounts(&self) -> Vec<ActiveMountInfo> {
        self.mounts
            .lock()
            .values()
            .map(|m| m.info.clone())
            .collect()
    }
}

#[async_trait]
impl ControlHandler for DaemonRuntime {
    async fn handle(&self, req: ControlRequest, _shutdown: &Arc<Notify>) -> ControlResponse {
        match req {
            ControlRequest::Ping => ControlResponse::Pong {
                pid: std::process::id(),
            },
            ControlRequest::Status => {
                let paths = self.registry.paths();
                let (cached_blobs, cached_bytes) = self.registry.cas().stats();
                ControlResponse::DaemonStatus(Box::new(DaemonStatus {
                    pid: std::process::id(),
                    instance: paths.instance.clone(),
                    state_dir: paths.state_dir.clone(),
                    cache_dir: paths.cache_dir.clone(),
                    config_path: paths.config_path.clone(),
                    cached_blobs,
                    cached_bytes,
                    max_cache_bytes: self.registry.cas().max_bytes(),
                    sources: self.registry.list_sources(),
                    mounts: self.active_mounts(),
                }))
            }
            ControlRequest::Shutdown => {
                self.unmount_all().await;
                ControlResponse::Ok {
                    message: "jjfsd shutting down".to_string(),
                }
            }
            ControlRequest::AddSource { alias, config } => {
                match self.registry.add_source(alias.clone(), config) {
                    Ok(()) => ControlResponse::Ok {
                        message: format!("registered source '{alias}'"),
                    },
                    Err(e) => error_to_response(e),
                }
            }
            ControlRequest::RemoveSource { alias, force } => {
                match self.registry.remove_source(&alias, force, |src| {
                    self.is_source_mounted(src) || self.overlays.has_dirty_for_source(src)
                }) {
                    Ok(()) => ControlResponse::Ok {
                        message: format!("removed source '{alias}'"),
                    },
                    Err(e) => error_to_response(e),
                }
            }
            ControlRequest::ListSources => ControlResponse::Sources {
                sources: self.registry.list_sources(),
            },
            ControlRequest::Mount {
                target,
                mountpoint,
                persist,
            } => match self.mount_target(target, mountpoint, persist).await {
                Ok(info) => ControlResponse::Mounted(info),
                Err(e) => error_to_response(e),
            },
            ControlRequest::Unmount { mountpoint } => {
                match self.unmount_target(&mountpoint).await {
                    Ok(mp) => ControlResponse::Unmounted { mountpoint: mp },
                    Err(e) => error_to_response(e),
                }
            }
            ControlRequest::SnapshotWorkspace { .. } | ControlRequest::CheckoutTree { .. } => {
                ControlResponse::Error {
                    code: "unimplemented".to_string(),
                    message: "snapshot/checkout handlers attach in commits 8-9".to_string(),
                }
            }
        }
    }
}

pub fn error_to_response(err: JjError) -> ControlResponse {
    let code = match err {
        JjError::NotFound => "not_found",
        JjError::Busy => "ebusy",
        JjError::AlreadyExists => "already_exists",
        JjError::InvalidPath => "invalid_argument",
        JjError::ReadOnly => "erofs",
        JjError::PermissionDenied(_) => "eacces",
        _ => "internal",
    };
    ControlResponse::Error {
        code: code.to_string(),
        message: err.to_string(),
    }
}

fn is_stale_fuse_mount(path: &Path) -> bool {
    matches!(std::fs::metadata(path), Err(ref e) if e.raw_os_error() == Some(libc::ENOTCONN))
}

fn detach_stale_fuse_mount(path: &Path) {
    let status = std::process::Command::new("fusermount3")
        .arg("-uz")
        .arg(path)
        .status()
        .or_else(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                std::process::Command::new("fusermount")
                    .arg("-uz")
                    .arg(path)
                    .status()
            } else {
                Err(err)
            }
        });
    if let Err(err) = status {
        tracing::warn!(
            mountpoint = %path.display(),
            error = %err,
            "failed to detach stale FUSE mountpoint"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use jj_lib::object_id::ObjectId as _;
    use vfs_proto::SourceConfig;
    use vfs_proto::SourceKind;
    use vfs_proto::send_control_request;

    use super::*;
    use crate::ipc_server::serve_control_socket;
    use crate::test_helpers::setup_test_repo;

    fn fuse_available() -> bool {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/fuse")
            .is_ok()
    }

    #[tokio::test]
    async fn control_socket_registers_lists_and_stops_daemon_cleanly() {
        let state_dir = tempfile::tempdir().unwrap();
        let paths = InstancePaths::resolve(Some("ipc-test"), Some(state_dir.path()));
        let runtime = DaemonRuntime::new(paths.clone()).unwrap();
        let shutdown = Arc::new(Notify::new());

        let sock = paths.socket_path.clone();
        let rt_clone = Arc::clone(&runtime);
        let sd_clone = Arc::clone(&shutdown);
        let server_task = tokio::spawn(async move {
            serve_control_socket(&sock, rt_clone, sd_clone)
                .await
                .unwrap();
        });

        for _ in 0..40 {
            if paths.socket_path.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let resp = send_control_request(
            &paths.socket_path,
            &ControlRequest::AddSource {
                alias: "oss".into(),
                config: SourceConfig {
                    kind: SourceKind::CommitCloud,
                    url: Some("http://127.0.0.1:8080".into()),
                    daemon_socket: None,
                    user: Some("alice".into()),
                    auth_token_env: None,
                    auth_token_cmd: vec![],
                    path: None,
                    search_paths: vec![],
                    repos: BTreeMap::new(),
                    ephemeral: false,
                },
            },
        )
        .await
        .unwrap();
        assert!(matches!(resp, ControlResponse::Ok { .. }));

        let listed = send_control_request(&paths.socket_path, &ControlRequest::ListSources)
            .await
            .unwrap();
        match listed {
            ControlResponse::Sources { sources } => assert!(sources.contains_key("oss")),
            other => panic!("unexpected response: {other:?}"),
        }

        let stop = send_control_request(&paths.socket_path, &ControlRequest::Shutdown)
            .await
            .unwrap();
        assert!(matches!(stop, ControlResponse::Ok { .. }));
        server_task.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_root_and_scoped_direct_fuse_mounts_traverse_symlinks_and_prefixes() {
        if !fuse_available() {
            eprintln!("skipping FUSE test: /dev/fuse unavailable");
            return;
        }
        let (ws_a, _repo_a, cid_a) = setup_test_repo().await;
        let (ws_b, _repo_b, cid_b) = setup_test_repo().await;

        let state_dir = tempfile::tempdir().unwrap();
        let paths = InstancePaths::resolve(Some("multi-mnt"), Some(state_dir.path()));
        let runtime = DaemonRuntime::new(paths).unwrap();

        let repo_a_name = ws_a
            .path()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let repo_b_name = ws_b
            .path()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        runtime
            .registry()
            .add_source(
                "srca".into(),
                SourceConfig {
                    kind: SourceKind::LocalJj,
                    url: None,
                    daemon_socket: None,
                    user: None,
                    auth_token_env: None,
                    auth_token_cmd: vec![],
                    path: Some(ws_a.path().to_path_buf()),
                    search_paths: vec![],
                    repos: BTreeMap::new(),
                    ephemeral: true,
                },
            )
            .unwrap();

        runtime
            .registry()
            .add_source(
                "srcb".into(),
                SourceConfig {
                    kind: SourceKind::LocalJj,
                    url: None,
                    daemon_socket: None,
                    user: None,
                    auth_token_env: None,
                    auth_token_cmd: vec![],
                    path: Some(ws_b.path().to_path_buf()),
                    search_paths: vec![],
                    repos: BTreeMap::new(),
                    ephemeral: true,
                },
            )
            .unwrap();

        let root_mnt = tempfile::tempdir().unwrap();
        let direct_ws_mnt = tempfile::tempdir().unwrap();
        let direct_repo_mnt = tempfile::tempdir().unwrap();
        let direct_user_mnt = tempfile::tempdir().unwrap();

        runtime
            .mount_target(None, root_mnt.path().to_path_buf(), false)
            .await
            .unwrap();
        runtime
            .mount_target(
                Some(format!("srca:{repo_a_name}/default")),
                direct_ws_mnt.path().to_path_buf(),
                false,
            )
            .await
            .unwrap();
        runtime
            .mount_target(
                Some(format!("srcb:{repo_b_name}")),
                direct_repo_mnt.path().to_path_buf(),
                false,
            )
            .await
            .unwrap();
        runtime
            .mount_target(
                Some("srca:@local".to_string()),
                direct_user_mnt.path().to_path_buf(),
                false,
            )
            .await
            .unwrap();

        assert_eq!(runtime.active_mounts().len(), 4);

        // Wait for FUSE mounts to finish initializing
        for _ in 0..30 {
            if direct_ws_mnt.path().join("file1.txt").exists()
                && root_mnt.path().join("srca").exists()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        }

        // 1. Scoped Direct Workspace Mount: `<dir>/file1.txt` without
        //    [source]/[repo] prefix!
        assert_eq!(
            std::fs::read_to_string(direct_ws_mnt.path().join("file1.txt")).unwrap(),
            "hello content 1"
        );

        // 2. Root Namespace Mount: traverse via `me` symlink AND
        //    `users/local/<repo>/default` symlink!
        let via_me = root_mnt
            .path()
            .join("srca/me")
            .join(&repo_a_name)
            .join("default/dir/file2.txt");
        assert_eq!(std::fs::read_to_string(&via_me).unwrap(), "hello content 2");

        // Verify `workspaces/default` and `users/local/<repo>/default` resolve
        // to the SAME canonical inode!
        use std::os::unix::fs::MetadataExt as _;
        let canon_meta = std::fs::metadata(
            root_mnt
                .path()
                .join("srca/repos")
                .join(&repo_a_name)
                .join("users/local/default"),
        )
        .unwrap();
        let ws_alias_meta = std::fs::metadata(
            root_mnt
                .path()
                .join("srca/repos")
                .join(&repo_a_name)
                .join("workspaces/default"),
        )
        .unwrap();
        let user_alias_meta = std::fs::metadata(
            root_mnt
                .path()
                .join("srca/users/local")
                .join(&repo_a_name)
                .join("default"),
        )
        .unwrap();
        assert_eq!(canon_meta.ino(), ws_alias_meta.ino());
        assert_eq!(canon_meta.ino(), user_alias_meta.ino());

        // 3. Scoped Direct Repo Mount: resolve commit via 12-hex prefix
        //    (`commits/<12-hex>/file1.txt`)!
        let short_prefix = &cid_b.hex()[..12];
        assert_eq!(
            std::fs::read_to_string(
                direct_repo_mnt
                    .path()
                    .join("commits")
                    .join(short_prefix)
                    .join("file1.txt")
            )
            .unwrap(),
            "hello content 1"
        );

        // 4. Scoped Direct User Mount (`srca:@local` ->
        //    `<dir>/<repo>/default/file1.txt`)!
        assert_eq!(
            std::fs::read_to_string(
                direct_user_mnt
                    .path()
                    .join(&repo_a_name)
                    .join("default/file1.txt")
            )
            .unwrap(),
            "hello content 1"
        );

        // 5. Concurrent duplicate mount_target calls on the same mountpoint
        //    must serialize so only one mounts and the second fails with
        //    AlreadyExists (no stacked mount leak).
        let race_mnt = tempfile::tempdir().unwrap();
        let (r1, r2) = tokio::join!(
            runtime.mount_target(
                Some("srcb".to_string()),
                race_mnt.path().to_path_buf(),
                false,
            ),
            runtime.mount_target(
                Some("srcb".to_string()),
                race_mnt.path().to_path_buf(),
                false,
            ),
        );
        assert_eq!(r1.is_ok() as u8 + r2.is_ok() as u8, 1);
        assert!(
            matches!(r1, Err(JjError::AlreadyExists)) || matches!(r2, Err(JjError::AlreadyExists))
        );
        runtime.unmount_target(race_mnt.path()).await.unwrap();

        // 6. Removing an actively mounted source via ControlHandler without
        //    --force returns ebusy, but succeeds once unmounted even though
        //    `active_repos` was populated by reads above!
        let sd = Arc::new(Notify::new());
        let busy_resp = runtime
            .handle(
                ControlRequest::RemoveSource {
                    alias: "srcb".into(),
                    force: false,
                },
                &sd,
            )
            .await;
        assert!(matches!(busy_resp, ControlResponse::Error { ref code, .. } if code == "ebusy"));

        let _ = cid_a;
        runtime
            .unmount_target(direct_repo_mnt.path())
            .await
            .unwrap();
        let ok_resp = runtime
            .handle(
                ControlRequest::RemoveSource {
                    alias: "srcb".into(),
                    force: false,
                },
                &sd,
            )
            .await;
        assert!(matches!(ok_resp, ControlResponse::Ok { .. }));

        // Unmount remaining direct mounts and then unmount all
        runtime.unmount_target(direct_ws_mnt.path()).await.unwrap();
        assert_eq!(runtime.active_mounts().len(), 2);
        runtime.unmount_all().await;
        assert!(runtime.active_mounts().is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fuse_read_write_overlay_enforces_gitignore_erofs_and_exdev() {
        if !fuse_available() {
            eprintln!("skipping FUSE test: /dev/fuse unavailable");
            return;
        }
        let (ws_a, _repo_a, cid_a) = setup_test_repo().await;
        let (ws_b, _repo_b, _cid_b) = setup_test_repo().await;

        let state_dir = tempfile::tempdir().unwrap();
        let paths = InstancePaths::resolve(Some("rw-mnt"), Some(state_dir.path()));
        let runtime = DaemonRuntime::new(paths).unwrap();

        let repo_a_name = ws_a
            .path()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let repo_b_name = ws_b
            .path()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        for (alias, p) in [("srca", ws_a.path()), ("srcb", ws_b.path())] {
            runtime
                .registry()
                .add_source(
                    alias.into(),
                    SourceConfig {
                        kind: SourceKind::LocalJj,
                        url: None,
                        daemon_socket: None,
                        user: None,
                        auth_token_env: None,
                        auth_token_cmd: vec![],
                        path: Some(p.to_path_buf()),
                        search_paths: vec![],
                        repos: BTreeMap::new(),
                        ephemeral: true,
                    },
                )
                .unwrap();
        }

        let root_mnt = tempfile::tempdir().unwrap();
        let ws_mnt = tempfile::tempdir().unwrap();
        runtime
            .mount_target(None, root_mnt.path().to_path_buf(), false)
            .await
            .unwrap();
        runtime
            .mount_target(
                Some(format!("srca:{repo_a_name}/default")),
                ws_mnt.path().to_path_buf(),
                false,
            )
            .await
            .unwrap();

        for _ in 0..30 {
            if ws_mnt.path().join("file1.txt").exists() && root_mnt.path().join("srca").exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        }

        // 1. Write `.gitignore` and ignored `target/artifact.o` + tracked
        //    `src/main.rs` via real POSIX syscalls!
        std::fs::write(ws_mnt.path().join(".gitignore"), "target/\n").unwrap();
        std::fs::create_dir(ws_mnt.path().join("target")).unwrap();
        std::fs::write(ws_mnt.path().join("target/artifact.o"), b"BINARY_OBJ").unwrap();
        assert_eq!(
            std::fs::read(ws_mnt.path().join("target/artifact.o")).unwrap(),
            b"BINARY_OBJ"
        );

        std::fs::create_dir(ws_mnt.path().join("src")).unwrap();
        std::fs::write(ws_mnt.path().join("src/main.rs"), "fn main() {}\n").unwrap();
        std::fs::rename(
            ws_mnt.path().join("src/main.rs"),
            ws_mnt.path().join("src/lib.rs"),
        )
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(ws_mnt.path().join("src/lib.rs")).unwrap(),
            "fn main() {}\n"
        );
        assert!(!ws_mnt.path().join("src/main.rs").exists());

        // Modify + unlink existing base-commit files
        std::fs::write(ws_mnt.path().join("file1.txt"), "rewritten via fuse").unwrap();
        assert_eq!(
            std::fs::read_to_string(ws_mnt.path().join("file1.txt")).unwrap(),
            "rewritten via fuse"
        );
        std::fs::remove_file(ws_mnt.path().join("dir/file2.txt")).unwrap();
        assert!(!ws_mnt.path().join("dir/file2.txt").exists());
        std::fs::remove_dir(ws_mnt.path().join("dir")).unwrap();
        assert!(!ws_mnt.path().join("dir").exists());

        // Verify Root Mount simultaneously sees the exact same
        // WorkspaceOverlay!
        let root_ws_a = root_mnt
            .path()
            .join("srca/repos")
            .join(&repo_a_name)
            .join("users/local/default");
        assert_eq!(
            std::fs::read_to_string(root_ws_a.join("src/lib.rs")).unwrap(),
            "fn main() {}\n"
        );

        // 2. Writing inside immutable `commits/<id>/...` returns `EROFS`!
        let ro_commit_file = root_mnt
            .path()
            .join("srca/repos")
            .join(&repo_a_name)
            .join("commits")
            .join(cid_a.hex())
            .join("file1.txt");
        let ro_err = std::fs::write(&ro_commit_file, "mutate immutable").unwrap_err();
        assert_eq!(ro_err.raw_os_error(), Some(libc::EROFS));

        // 3. Cross-workspace rename inside Root Mount returns `EXDEV`!
        let root_ws_b = root_mnt
            .path()
            .join("srcb/repos")
            .join(&repo_b_name)
            .join("users/local/default");
        let xdev_err =
            std::fs::rename(root_ws_a.join("src/lib.rs"), root_ws_b.join("moved.rs")).unwrap_err();
        assert_eq!(xdev_err.raw_os_error(), Some(libc::EXDEV));

        // 4. chmod on tracked (`root_ws_b/dir`) and newly-created
        //    (`ws_mnt/src`) directories succeeds, staged directory rename
        //    (`src` -> `src_moved`) works, base-tree directory rename returns
        //    `EXDEV`, `RENAME_EXCHANGE` returns `EINVAL`, and `.jj` in local-jj
        //    is read-only (`EROFS`)!
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(
            ws_mnt.path().join("src"),
            std::fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        std::fs::set_permissions(
            root_ws_b.join("dir"),
            std::fs::Permissions::from_mode(0o755),
        )
        .unwrap();

        std::fs::rename(ws_mnt.path().join("src"), ws_mnt.path().join("src_moved")).unwrap();
        assert_eq!(
            std::fs::read_to_string(ws_mnt.path().join("src_moved/lib.rs")).unwrap(),
            "fn main() {}\n"
        );
        let base_dir_rename =
            std::fs::rename(root_ws_b.join("dir"), root_ws_b.join("dir_moved")).unwrap_err();
        assert_eq!(base_dir_rename.raw_os_error(), Some(libc::EXDEV));

        let c_a =
            std::ffi::CString::new(ws_mnt.path().join("file1.txt").to_str().unwrap()).unwrap();
        let c_b = std::ffi::CString::new(ws_mnt.path().join("src_moved/lib.rs").to_str().unwrap())
            .unwrap();
        // SAFETY: valid NUL-terminated paths passed to libc::renameat2.
        let rc = unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                c_a.as_ptr(),
                libc::AT_FDCWD,
                c_b.as_ptr(),
                libc::RENAME_EXCHANGE,
            )
        };
        assert_eq!(rc, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::EINVAL)
        );

        let jj_ro_err =
            std::fs::write(ws_mnt.path().join(".jj/working_copy/lock"), "nope").unwrap_err();
        assert_eq!(jj_ro_err.raw_os_error(), Some(libc::EROFS));

        runtime.unmount_all().await;
    }
}
