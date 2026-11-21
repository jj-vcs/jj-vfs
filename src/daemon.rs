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
use vfs_proto::CheckoutTreeResult;
use vfs_proto::ControlRequest;
use vfs_proto::ControlResponse;
use vfs_proto::DEFAULT_MAX_CACHE_BYTES;
use vfs_proto::DaemonStatus;
use vfs_proto::InstancePaths;
use vfs_proto::MountScope;
use vfs_proto::SnapshotResult;
use vfs_proto::WorkspaceKey;
use vfs_proto::encode_repo_segment;

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
use crate::snapshot::snapshot_workspace_cc;
use crate::snapshot::snapshot_workspace_local;
use crate::snapshot::sync_synthesized_vfs_state_file;

struct ActiveMountEntry {
    info: ActiveMountInfo,
    inode_map: Arc<InodeMap>,
    notifier: fuser::Notifier,
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
    workspace_locks: dashmap::DashMap<WorkspaceKey, Arc<tokio::sync::Mutex<()>>>,
    pending_cli_snapshots: dashmap::DashMap<WorkspaceKey, std::time::Instant>,
    skipped_bg_snapshots: dashmap::DashMap<WorkspaceKey, (String, u64)>,
}

#[derive(Clone, Debug)]
pub struct CheckoutTreeOptions {
    pub old_tree_id_hex: Option<String>,
    pub new_tree_id_hex: String,
    pub new_tree_ids_hex: Vec<String>,
    pub conflict_labels: Vec<String>,
    pub new_commit_id_hex: Option<String>,
    pub new_operation_id_hex: Option<String>,
    pub captured_seq: Option<u64>,
    pub publish_ws_state: bool,
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
            workspace_locks: dashmap::DashMap::new(),
            pending_cli_snapshots: dashmap::DashMap::new(),
            skipped_bg_snapshots: dashmap::DashMap::new(),
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
        let notifier = session.notifier();
        {
            let mut mounts = self.mounts.lock();
            mounts.insert(
                canon_mp,
                ActiveMountEntry {
                    info: info.clone(),
                    inode_map,
                    notifier,
                    session,
                },
            );
            if let Some(mp) = reservation.mountpoint.take() {
                self.mounting.lock().remove(&mp);
            }
        }
        Ok(info)
    }

    fn normalize_workspace_key(&self, mut key: WorkspaceKey) -> Result<WorkspaceKey, JjError> {
        if key.user.is_empty() {
            key.user = self
                .registry
                .default_user_for_source(&key.source)
                .ok_or(JjError::NotFound)?;
        }
        Ok(key)
    }

    fn workspace_lock(&self, key: &WorkspaceKey) -> Arc<tokio::sync::Mutex<()>> {
        self.workspace_locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Spawns the debounced ($2\text{ s}$ default) background snapshot worker
    /// task and the remote workspace head watcher.
    pub fn start_background_tasks(self: &Arc<Self>, shutdown: Arc<Notify>) {
        let rt = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_millis(250));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut ticks: u64 = 0;
            loop {
                tokio::select! {
                    _ = shutdown.notified() => break,
                    _ = ticker.tick() => {
                        ticks = ticks.wrapping_add(1);
                        let debounce = std::time::Duration::from_millis(
                            rt.registry.snapshot_debounce_ms().max(50),
                        );
                        for ovl in rt.overlays.active_overlays() {
                            let Some((dirty_seq, Some(last_mut))) = ovl.dirty_status() else {
                                continue;
                            };
                            if last_mut.elapsed() < debounce {
                                continue;
                            }
                            let Ok(norm_key) = rt.normalize_workspace_key(ovl.key().clone()) else {
                                continue;
                            };
                            if rt
                                .pending_cli_snapshots
                                .get(&norm_key)
                                .is_some_and(|t0| {
                                    t0.elapsed() < std::time::Duration::from_secs(15)
                                })
                            {
                                continue;
                            }
                            if let Some(cached) = rt.router.cached_workspace_head(&norm_key) {
                                if rt
                                    .skipped_bg_snapshots
                                    .get(&norm_key)
                                    .is_some_and(|s| {
                                        s.0 == cached.commit_id_hex && s.1 == dirty_seq
                                    })
                                {
                                    continue;
                                }
                                if cached.tree_ids_hex.len() > 1 {
                                    tracing::warn!(
                                        workspace = ?norm_key,
                                        "skipping background auto-snapshot on conflicted working-copy commit"
                                    );
                                    rt.skipped_bg_snapshots
                                        .insert(norm_key, (cached.commit_id_hex, dirty_seq));
                                    continue;
                                }
                            }
                            match rt.snapshot_workspace(norm_key.clone(), true).await {
                                Ok(_) => {
                                    rt.skipped_bg_snapshots.remove(&norm_key);
                                }
                                Err(err) => {
                                    tracing::warn!(
                                        workspace = ?norm_key,
                                        error = %err,
                                        "background debounced snapshot skipped or failed"
                                    );
                                    if let Some(head) = rt.router.cached_workspace_head(&norm_key) {
                                        rt.skipped_bg_snapshots
                                            .insert(norm_key, (head.commit_id_hex, dirty_seq));
                                    }
                                }
                            }
                        }

                        // Every 8 ticks (~2s), check clean mounted Commit Cloud workspaces for remote head updates
                        if ticks.is_multiple_of(8) {
                            rt.poll_remote_workspace_heads().await;
                        }
                    }
                }
            }
        });
    }

    async fn poll_remote_workspace_heads(&self) {
        for (key, cached_ref) in self.router.active_workspace_heads() {
            let ws_mutex = self.workspace_lock(&key);
            let Ok(_ws_guard) = ws_mutex.try_lock() else {
                continue;
            };
            if self
                .pending_cli_snapshots
                .get(&key)
                .is_some_and(|t0| t0.elapsed() < std::time::Duration::from_secs(15))
            {
                continue;
            }
            if self.router.cached_workspace_head(&key).as_ref() != Some(&cached_ref) {
                continue;
            }
            if let Ok(ovl) = self.overlays.get_or_open(&key)
                && ovl.has_dirty_tracked_paths()
            {
                continue;
            }
            let repo_seg = encode_repo_segment(&key.repo);
            let Ok(Some(cc)) = self.registry.resolve_cc_repo(&key.source, &repo_seg) else {
                continue;
            };
            use crate::backend::ReadOnlyVfsBackend as _;
            let Ok(remote_list) = cc.list_workspaces(Some(&key.user)).await else {
                continue;
            };
            if self.router.cached_workspace_head(&key).as_ref() != Some(&cached_ref) {
                continue;
            }
            if let Ok(ovl) = self.overlays.get_or_open(&key)
                && ovl.has_dirty_tracked_paths()
            {
                continue;
            }
            if let Some(remote_ws) = remote_list
                .into_iter()
                .find(|w| w.workspace_name == key.workspace && w.user == key.user)
                && (remote_ws.commit_id_hex != cached_ref.commit_id_hex
                    || remote_ws.tree_id_hex != cached_ref.tree_id_hex
                    || remote_ws.tree_ids_hex != cached_ref.tree_ids_hex)
            {
                let _ = self
                    .checkout_tree_locked(
                        key,
                        CheckoutTreeOptions {
                            old_tree_id_hex: Some(cached_ref.tree_id_hex),
                            new_tree_id_hex: remote_ws.tree_id_hex,
                            new_tree_ids_hex: remote_ws.tree_ids_hex,
                            conflict_labels: remote_ws.conflict_labels,
                            new_commit_id_hex: Some(remote_ws.commit_id_hex),
                            new_operation_id_hex: (!remote_ws.operation_id_hex.is_empty())
                                .then_some(remote_ws.operation_id_hex),
                            captured_seq: None,
                            publish_ws_state: false,
                        },
                    )
                    .await;
            }
        }
    }

    /// Executes an $O(k)$ dirty-path snapshot on `key`.
    pub async fn snapshot_workspace(
        &self,
        key: WorkspaceKey,
        publish_op: bool,
    ) -> Result<SnapshotResult, JjError> {
        let key = self.normalize_workspace_key(key)?;
        let ws_mutex = self.workspace_lock(&key);
        let _ws_guard = ws_mutex.lock().await;

        let ws_ref = self.router.resolve_workspace_ref(&key).await?;
        let overlay = self.overlays.get_or_open(&key)?;
        let dirty_paths: Vec<String> = overlay.capture_dirty_snapshot().1.into_keys().collect();
        let repo_seg = encode_repo_segment(&key.repo);

        if let Some(cc) = self.registry.resolve_cc_repo(&key.source, &repo_seg)? {
            let paths = self.registry.paths();
            let (res, updated_ref) = snapshot_workspace_cc(
                &paths.state_dir,
                &paths.socket_path,
                &key,
                &overlay,
                &cc,
                ws_ref,
                publish_op,
            )
            .await?;
            if publish_op {
                self.pending_cli_snapshots.remove(&key);
                self.skipped_bg_snapshots.remove(&key);
                self.router.set_workspace_head(key.clone(), updated_ref);
                if !dirty_paths.is_empty() {
                    let mut inval = dirty_paths;
                    inval.push(".jj/working_copy/vfs_state.json".to_string());
                    self.invalidate_workspace_paths(&key, &inval).await;
                }
            } else if res.captured_seq.is_some() {
                self.pending_cli_snapshots
                    .insert(key.clone(), std::time::Instant::now());
            }
            return Ok(res);
        }

        if let Some(local_jj) = ws_ref.local_jj_dir.clone() {
            let (res, updated_ref) =
                snapshot_workspace_local(&key, &overlay, &local_jj, ws_ref, publish_op).await?;
            if publish_op {
                self.pending_cli_snapshots.remove(&key);
                self.skipped_bg_snapshots.remove(&key);
                self.router.set_workspace_head(key.clone(), updated_ref);
                if !dirty_paths.is_empty() {
                    let mut inval = dirty_paths;
                    inval.push(".jj/working_copy/vfs_state.json".to_string());
                    self.invalidate_workspace_paths(&key, &inval).await;
                }
            } else if res.captured_seq.is_some() {
                self.pending_cli_snapshots
                    .insert(key.clone(), std::time::Instant::now());
            }
            return Ok(res);
        }

        Err(JjError::ReadOnly)
    }

    /// Updates workspace base tree/commit/operation pointers on `CheckoutTree`,
    /// diffs `old_tree` vs `new_tree` skipping identical subtrees ($0$ blob
    /// reads), and sends targeted `fuser::Notifier` invalidations across all
    /// covering mounts.
    pub async fn checkout_tree(
        &self,
        key: WorkspaceKey,
        old_tree_id_hex: Option<String>,
        new_tree_id_hex: String,
        new_commit_id_hex: Option<String>,
        new_operation_id_hex: Option<String>,
    ) -> Result<CheckoutTreeResult, JjError> {
        self.checkout_tree_with_options(
            key,
            CheckoutTreeOptions {
                old_tree_id_hex,
                new_tree_id_hex,
                new_tree_ids_hex: Vec::new(),
                conflict_labels: Vec::new(),
                new_commit_id_hex,
                new_operation_id_hex,
                captured_seq: None,
                publish_ws_state: true,
            },
        )
        .await
    }

    pub async fn checkout_tree_with_options(
        &self,
        key: WorkspaceKey,
        opts: CheckoutTreeOptions,
    ) -> Result<CheckoutTreeResult, JjError> {
        let key = self.normalize_workspace_key(key)?;
        let ws_mutex = self.workspace_lock(&key);
        let _ws_guard = ws_mutex.lock().await;
        self.checkout_tree_locked(key, opts).await
    }

    async fn checkout_tree_locked(
        &self,
        key: WorkspaceKey,
        opts: CheckoutTreeOptions,
    ) -> Result<CheckoutTreeResult, JjError> {
        let mut ws_ref = self.router.resolve_workspace_ref(&key).await?;
        let repo_seg = encode_repo_segment(&key.repo);
        let cc_opt = self.registry.resolve_cc_repo(&key.source, &repo_seg)?;

        if let Some(ref new_op) = opts.new_operation_id_hex
            && !new_op.is_empty()
            && !ws_ref.operation_id_hex.is_empty()
            && new_op != &ws_ref.operation_id_hex
            && is_strict_op_ancestor(
                cc_opt.as_ref(),
                ws_ref.local_jj_dir.as_deref(),
                new_op,
                &ws_ref.operation_id_hex,
            )
            .await
        {
            return Err(std::io::Error::other(format!(
                "refusing to revert workspace head from operation {} to older ancestor operation \
                 {}",
                ws_ref.operation_id_hex, new_op
            ))
            .into());
        }

        let resolved_commit_id_hex = if let Some(cid) = opts.new_commit_id_hex {
            cid
        } else if let Some(ref new_op) = opts.new_operation_id_hex
            && !new_op.is_empty()
            && new_op != &ws_ref.operation_id_hex
        {
            if let Some(ref cc) = cc_opt {
                cc.resolve_wc_commit_at_op(new_op, &key.workspace)
                    .await?
                    .ok_or_else(|| {
                        std::io::Error::other(format!(
                            "workspace '{}' missing from Commit Cloud view at operation {}",
                            key.workspace, new_op
                        ))
                    })?
            } else if let Some(ref local_jj) = ws_ref.local_jj_dir {
                resolve_local_wc_commit_at_op(local_jj, new_op, &key.workspace)
                    .await?
                    .ok_or_else(|| {
                        std::io::Error::other(format!(
                            "workspace '{}' missing from local view at operation {}",
                            key.workspace, new_op
                        ))
                    })?
            } else {
                ws_ref.commit_id_hex.clone()
            }
        } else {
            ws_ref.commit_id_hex.clone()
        };

        let mut completed_dirty_paths = Vec::new();
        if let Some(seq) = opts.captured_seq {
            let overlay = self.overlays.get_or_open(&key)?;
            completed_dirty_paths = overlay.capture_dirty_snapshot().1.into_keys().collect();
            let _ = overlay.complete_snapshot(seq)?;
        }
        self.pending_cli_snapshots.remove(&key);
        self.skipped_bg_snapshots.remove(&key);

        let served_old_tree = ws_ref.tree_id_hex.clone();
        let served_old_tree_ids = if ws_ref.tree_ids_hex.is_empty() {
            vec![served_old_tree.clone()]
        } else {
            ws_ref.tree_ids_hex.clone()
        };
        let effective_new_tree_ids = if opts.new_tree_ids_hex.is_empty() {
            vec![opts.new_tree_id_hex.clone()]
        } else {
            opts.new_tree_ids_hex
        };

        ws_ref.tree_id_hex = opts.new_tree_id_hex.clone();
        ws_ref.tree_ids_hex = effective_new_tree_ids.clone();
        ws_ref.conflict_labels = opts.conflict_labels;
        ws_ref.commit_id_hex = resolved_commit_id_hex.clone();
        if let Some(op_id) = opts.new_operation_id_hex {
            ws_ref.operation_id_hex = op_id;
        }

        let paths = self.registry.paths();
        sync_synthesized_vfs_state_file(&paths.state_dir, &paths.socket_path, &key, &ws_ref);
        self.router.set_workspace_head(key.clone(), ws_ref.clone());

        if opts.publish_ws_state
            && let Some(ref cc) = cc_opt
            && let (Ok(cid_b), Ok(op_b), Ok(tid_b)) = (
                hex::decode(&ws_ref.commit_id_hex),
                hex::decode(&ws_ref.operation_id_hex),
                hex::decode(&ws_ref.tree_id_hex),
            )
        {
            let all_tids: Vec<Vec<u8>> = ws_ref
                .tree_ids_hex
                .iter()
                .filter_map(|h| hex::decode(h).ok())
                .collect();
            if let Err(err) = cc
                .update_workspace_state(cc_common::workspace::WorkspaceState {
                    repo_id: cc.repo_id().to_string(),
                    user: key.user.clone(),
                    workspace_name: key.workspace.clone(),
                    commit_id: cid_b,
                    operation_id: op_b,
                    tree_id: tid_b,
                    sparse_patterns: Vec::new(),
                    tree_ids: all_tids,
                })
                .await
            {
                tracing::warn!(
                    workspace = ?key,
                    error = %err,
                    "failed to publish WorkspaceState during checkout_tree"
                );
            }
        }

        let mut changed_set = std::collections::BTreeSet::new();
        let mut diff_failed = false;
        match self.registry.resolve_repo(&key.source, &repo_seg) {
            Ok(backend) => {
                let mut old_candidates = vec![served_old_tree.clone()];
                if let Some(caller_old) = opts.old_tree_id_hex
                    && !old_candidates.contains(&caller_old)
                {
                    old_candidates.push(caller_old);
                }
                for extra_old in served_old_tree_ids {
                    if !old_candidates.contains(&extra_old) {
                        old_candidates.push(extra_old);
                    }
                }
                for old_t in &old_candidates {
                    for new_t in &effective_new_tree_ids {
                        if old_t == new_t {
                            continue;
                        }
                        match backend.diff_trees(old_t, new_t).await {
                            Ok(diff) => changed_set.extend(diff),
                            Err(err) => {
                                tracing::warn!(
                                    workspace = ?key,
                                    error = %err,
                                    "best-effort diff_trees failed after workspace head update"
                                );
                                diff_failed = true;
                            }
                        }
                    }
                }
            }
            Err(err) => {
                tracing::warn!(
                    workspace = ?key,
                    error = %err,
                    "failed to resolve backend for checkout invalidation"
                );
                diff_failed = true;
            }
        }

        let changed_paths: Vec<String> = changed_set.iter().cloned().collect();
        for p in completed_dirty_paths {
            changed_set.insert(p);
        }
        changed_set.insert(".jj/working_copy/vfs_state.json".to_string());
        let kernel_inval_paths: Vec<String> = changed_set.into_iter().collect();
        self.invalidate_workspace_paths(&key, &kernel_inval_paths)
            .await;
        if diff_failed {
            self.invalidate_workspace_root(&key).await;
        }

        Ok(CheckoutTreeResult {
            workspace: key,
            old_tree_id_hex: served_old_tree,
            new_tree_id_hex: opts.new_tree_id_hex,
            invalidated_paths: changed_paths,
            commit_id_hex: Some(resolved_commit_id_hex),
        })
    }

    /// Sends targeted kernel dentry (`inval_entry`) and page/attr
    /// (`inval_inode`) invalidations to every active FUSE mount whose scope
    /// covers `ws_key`.
    pub async fn invalidate_workspace_paths(&self, ws_key: &WorkspaceKey, rel_paths: &[String]) {
        if rel_paths.is_empty() {
            return;
        }
        let repo_seg = encode_repo_segment(&ws_key.repo);
        let canon_ws_prefix = PathBuf::from(&ws_key.source)
            .join("repos")
            .join(&repo_seg)
            .join("users")
            .join(&ws_key.user)
            .join(&ws_key.workspace);

        let batch = {
            let guard = self.mounts.lock();
            let mut collected = Vec::new();
            for entry in guard.values() {
                let mount_ws_rel = match &entry.info.scope {
                    MountScope::User { source, user }
                        if source == &ws_key.source && user == &ws_key.user =>
                    {
                        Some(PathBuf::from(&repo_seg).join(&ws_key.workspace))
                    }
                    _ => canon_ws_prefix
                        .strip_prefix(&entry.info.router_prefix)
                        .ok()
                        .map(Path::to_path_buf),
                };
                let Some(ws_prefix_in_mount) = mount_ws_rel else {
                    continue;
                };

                let mut items = Vec::new();
                for rel in rel_paths {
                    let mount_rel = if ws_prefix_in_mount.as_os_str().is_empty() {
                        PathBuf::from(rel)
                    } else {
                        ws_prefix_in_mount.join(rel)
                    };
                    if let Some(resolved) = entry.inode_map.lookup_path_inodes(&mount_rel) {
                        items.push(resolved);
                    }
                }
                if !items.is_empty() {
                    collected.push((entry.notifier.clone(), items));
                }
            }
            collected
        };

        if batch.is_empty() {
            return;
        }
        let _ = tokio::task::spawn_blocking(move || {
            for (notifier, items) in batch {
                for (child_opt, parent_ino, leaf) in items {
                    if let Some(child_ino) = child_opt {
                        let _ = notifier.inval_inode(child_ino, 0, 0);
                    }
                    let _ = notifier.inval_entry(parent_ino, std::ffi::OsStr::new(leaf.as_str()));
                    let _ = notifier.inval_inode(parent_ino, -1, 0);
                }
            }
        })
        .await;
    }

    async fn invalidate_workspace_root(&self, ws_key: &WorkspaceKey) {
        let repo_seg = encode_repo_segment(&ws_key.repo);
        let canon_ws_prefix = PathBuf::from(&ws_key.source)
            .join("repos")
            .join(&repo_seg)
            .join("users")
            .join(&ws_key.user)
            .join(&ws_key.workspace);

        let roots: Vec<(fuser::Notifier, fuser::INodeNo)> = {
            let guard = self.mounts.lock();
            let mut out = Vec::new();
            for entry in guard.values() {
                let mount_ws_rel = match &entry.info.scope {
                    MountScope::User { source, user }
                        if source == &ws_key.source && user == &ws_key.user =>
                    {
                        Some(PathBuf::from(&repo_seg).join(&ws_key.workspace))
                    }
                    _ => canon_ws_prefix
                        .strip_prefix(&entry.info.router_prefix)
                        .ok()
                        .map(Path::to_path_buf),
                };
                let Some(ws_prefix_in_mount) = mount_ws_rel else {
                    continue;
                };
                if ws_prefix_in_mount.as_os_str().is_empty() {
                    out.push((entry.notifier.clone(), crate::inode_map::ROOT_INODE));
                } else if let Some((Some(ws_ino), _, _)) =
                    entry.inode_map.lookup_path_inodes(&ws_prefix_in_mount)
                {
                    out.push((entry.notifier.clone(), ws_ino));
                }
            }
            out
        };

        if roots.is_empty() {
            return;
        }
        let _ = tokio::task::spawn_blocking(move || {
            for (notifier, ino) in roots {
                let _ = notifier.inval_inode(ino, -1, 0);
            }
        })
        .await;
    }

    async fn flush_all_dirty_overlays(&self) {
        for ovl in self.overlays.active_overlays() {
            if ovl.has_dirty_tracked_paths() {
                let _ = self.snapshot_workspace(ovl.key().clone(), true).await;
            }
        }
    }

    /// Detaches a single active FUSE mountpoint after flushing pending
    /// snapshots.
    pub async fn unmount_target(&self, mountpoint: &Path) -> Result<PathBuf, JjError> {
        self.flush_all_dirty_overlays().await;
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

    /// Cleanly flushes dirty snapshots and detaches all active FUSE sessions.
    pub async fn unmount_all(&self) {
        self.flush_all_dirty_overlays().await;
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
            ControlRequest::SnapshotWorkspace {
                workspace,
                publish_op,
            } => match self.snapshot_workspace(workspace, publish_op).await {
                Ok(res) => ControlResponse::Snapshotted(res),
                Err(e) => error_to_response(e),
            },
            ControlRequest::CheckoutTree {
                workspace,
                old_tree_id_hex,
                new_tree_id_hex,
                new_tree_ids_hex,
                conflict_labels,
                new_commit_id_hex,
                new_operation_id_hex,
                captured_seq,
            } => match self
                .checkout_tree_with_options(
                    workspace,
                    CheckoutTreeOptions {
                        old_tree_id_hex,
                        new_tree_id_hex,
                        new_tree_ids_hex,
                        conflict_labels,
                        new_commit_id_hex,
                        new_operation_id_hex,
                        captured_seq,
                        publish_ws_state: true,
                    },
                )
                .await
            {
                Ok(res) => ControlResponse::CheckedOut(res),
                Err(e) => error_to_response(e),
            },
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

async fn is_strict_op_ancestor(
    cc_opt: Option<&Arc<crate::backend::commit_cloud::CommitCloudVfsBackend>>,
    local_jj_dir: Option<&Path>,
    candidate_older_hex: &str,
    current_hex: &str,
) -> bool {
    if candidate_older_hex == current_hex {
        return false;
    }
    if let Some(cc) = cc_opt {
        let mut queue = std::collections::VecDeque::new();
        let mut visited = std::collections::HashSet::new();
        queue.push_back(current_hex.to_string());
        visited.insert(current_hex.to_string());
        while let Some(curr) = queue.pop_front() {
            if visited.len() > 64 {
                break;
            }
            let Ok(op) = cc.read_operation_proto(&curr).await else {
                break;
            };
            for p_bytes in op.parents {
                let p_hex = hex::encode(p_bytes);
                if p_hex == candidate_older_hex {
                    return true;
                }
                if visited.insert(p_hex.clone()) {
                    queue.push_back(p_hex);
                }
            }
        }
        return false;
    }
    if let Some(jj_dir) = local_jj_dir {
        let jj_owned = jj_dir.to_path_buf();
        let cand_owned = candidate_older_hex.to_string();
        let curr_owned = current_hex.to_string();
        return tokio::task::spawn_blocking(move || {
            futures::executor::block_on(is_local_op_strict_ancestor(
                &jj_owned,
                &cand_owned,
                &curr_owned,
            ))
        })
        .await
        .unwrap_or(false);
    }
    false
}

async fn is_local_op_strict_ancestor(
    local_jj_dir: &Path,
    candidate_older_hex: &str,
    current_hex: &str,
) -> bool {
    use jj_lib::config::ConfigLayer;
    use jj_lib::config::ConfigSource;
    use jj_lib::config::StackedConfig;
    use jj_lib::object_id::ObjectId as _;
    use jj_lib::op_store::OperationId;
    use jj_lib::repo::StoreFactories;
    use jj_lib::settings::UserSettings;
    use jj_lib::workspace::Workspace;
    use jj_lib::workspace::default_working_copy_factories;

    let Some(ws_root) = local_jj_dir.parent() else {
        return false;
    };
    let mut config = StackedConfig::with_defaults();
    if let Ok(layer) = ConfigLayer::parse(
        ConfigSource::User,
        "user.name = \"jj-vfs\"\nuser.email = \"jj-vfs@localhost\"\n",
    ) {
        config.add_layer(layer);
    }
    let Ok(settings) = UserSettings::from_config(config) else {
        return false;
    };
    let Ok(ws) = Workspace::load(
        &settings,
        ws_root,
        &StoreFactories::default(),
        &default_working_copy_factories(),
    ) else {
        return false;
    };
    let op_store = ws.repo_loader().op_store();
    let Some(start_id) = OperationId::try_from_hex(current_hex) else {
        return false;
    };
    let mut queue = std::collections::VecDeque::new();
    let mut visited = std::collections::HashSet::new();
    queue.push_back(start_id.clone());
    visited.insert(start_id);
    while let Some(curr) = queue.pop_front() {
        if visited.len() > 64 {
            break;
        }
        let Ok(op_data) = op_store.read_operation(&curr).await else {
            break;
        };
        for parent_id in op_data.parents {
            if parent_id.hex() == candidate_older_hex {
                return true;
            }
            if visited.insert(parent_id.clone()) {
                queue.push_back(parent_id);
            }
        }
    }
    false
}

async fn resolve_local_wc_commit_at_op(
    local_jj_dir: &Path,
    op_id_hex: &str,
    ws_name: &str,
) -> Result<Option<String>, JjError> {
    let jj_owned = jj_dir_to_buf(local_jj_dir);
    let op_owned = op_id_hex.to_string();
    let ws_owned = ws_name.to_string();
    tokio::task::spawn_blocking(move || {
        futures::executor::block_on(async move {
            use jj_lib::config::ConfigLayer;
            use jj_lib::config::ConfigSource;
            use jj_lib::config::StackedConfig;
            use jj_lib::object_id::ObjectId as _;
            use jj_lib::op_store::OperationId;
            use jj_lib::ref_name::WorkspaceNameBuf;
            use jj_lib::repo::StoreFactories;
            use jj_lib::settings::UserSettings;
            use jj_lib::workspace::Workspace;
            use jj_lib::workspace::default_working_copy_factories;

            let ws_root = jj_owned.parent().ok_or(JjError::InvalidPath)?;
            let mut config = StackedConfig::with_defaults();
            let layer = ConfigLayer::parse(
                ConfigSource::User,
                "user.name = \"jj-vfs\"\nuser.email = \"jj-vfs@localhost\"\n",
            )
            .map_err(|e| std::io::Error::other(format!("config parse error: {e}")))?;
            config.add_layer(layer);
            let settings = UserSettings::from_config(config)
                .map_err(|e| std::io::Error::other(format!("settings error: {e}")))?;
            let ws = Workspace::load(
                &settings,
                ws_root,
                &StoreFactories::default(),
                &default_working_copy_factories(),
            )
            .map_err(|e| std::io::Error::other(format!("load local workspace failed: {e:?}")))?;
            let op_id = OperationId::try_from_hex(&op_owned).ok_or(JjError::InvalidPath)?;
            let op_store = ws.repo_loader().op_store();
            let op_data = op_store
                .read_operation(&op_id)
                .await
                .map_err(|e| std::io::Error::other(format!("read_operation failed: {e:?}")))?;
            let view = op_store
                .read_view(&op_data.view_id)
                .await
                .map_err(|e| std::io::Error::other(format!("read_view failed: {e:?}")))?;
            let ws_key = WorkspaceNameBuf::from(ws_owned);
            Ok(view.wc_commit_ids.get(&ws_key).map(|cid| cid.hex()))
        })
    })
    .await
    .map_err(|e| std::io::Error::other(format!("resolve_local_wc_commit task panicked: {e}")))?
}

fn jj_dir_to_buf(p: &Path) -> PathBuf {
    p.to_path_buf()
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

    #[tokio::test(flavor = "multi_thread")]
    async fn debounced_background_snapshot_commits_dirty_files_without_tree_walk() {
        if !fuse_available() {
            eprintln!("skipping FUSE test: /dev/fuse unavailable");
            return;
        }
        let (ws_a, _repo_a, initial_cid) = setup_test_repo().await;

        let state_dir = tempfile::tempdir().unwrap();
        let paths = InstancePaths::resolve(Some("debounce-test"), Some(state_dir.path()));
        paths.ensure_dirs().unwrap();
        let mut cfg = vfs_proto::VfsConfigFile::default();
        cfg.daemon.snapshot_debounce_ms = 80;
        cfg.save_atomic(&paths.config_path).unwrap();

        let runtime = DaemonRuntime::new(paths).unwrap();
        let shutdown = Arc::new(Notify::new());
        runtime.start_background_tasks(Arc::clone(&shutdown));

        let repo_name = ws_a
            .path()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        runtime
            .registry()
            .add_source(
                "local".into(),
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

        let mnt = tempfile::tempdir().unwrap();
        runtime
            .mount_target(
                Some(format!("local:{repo_name}/default")),
                mnt.path().to_path_buf(),
                false,
            )
            .await
            .unwrap();

        for _ in 0..30 {
            if mnt.path().join("file1.txt").exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        }

        std::fs::write(mnt.path().join(".gitignore"), "target/\n").unwrap();
        std::fs::create_dir(mnt.path().join("target")).unwrap();
        std::fs::write(mnt.path().join("target/tmp.o"), b"NO_UPLOAD").unwrap();
        std::fs::write(mnt.path().join("file1.txt"), "snapshotted automatically").unwrap();

        // Wait for the 80ms debounce + 250ms background ticker to publish the
        // snapshot
        let ws_key = WorkspaceKey {
            source: "local".into(),
            repo: repo_name.clone(),
            user: "local".into(),
            workspace: "default".into(),
        };
        let mut snapshotted_cid = initial_cid.hex();
        for _ in 0..30 {
            let head = runtime
                .router()
                .resolve_workspace_ref(&ws_key)
                .await
                .unwrap();
            if head.commit_id_hex != initial_cid.hex() {
                snapshotted_cid = head.commit_id_hex;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        }
        assert_ne!(
            snapshotted_cid,
            initial_cid.hex(),
            "background debounce should commit new wc"
        );

        // Verify the committed tree has `file1.txt` == "snapshotted
        // automatically" and NO `target/tmp.o`!
        let backend = runtime
            .registry()
            .resolve_repo("local", &repo_name)
            .unwrap();
        let data = backend
            .read_file_range(&snapshotted_cid, Path::new("file1.txt"), 0, 64)
            .await
            .unwrap();
        assert_eq!(&*data, b"snapshotted automatically");
        assert!(matches!(
            backend
                .lookup_in_commit(&snapshotted_cid, Path::new("target/tmp.o"))
                .await,
            Err(JjError::NotFound)
        ));

        shutdown.notify_waiters();
        runtime.unmount_all().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn checkout_tree_invalidates_60s_kernel_cache_and_pins_unacknowledged_overlay_files() {
        if !fuse_available() {
            eprintln!("skipping FUSE test: /dev/fuse unavailable");
            return;
        }
        use jj_lib::backend::CopyId;
        use jj_lib::backend::TreeValue;
        use jj_lib::merged_tree::MergedTree;
        use jj_lib::repo::Repo as _;
        use jj_lib::repo_path::RepoPathBuf;
        use jj_lib::tree_builder::TreeBuilder;

        let (ws_a, repo_a, cid_v1) = setup_test_repo().await;

        // Build a second commit `cid_v2` in `repo_a` modifying ONLY `file1.txt`
        // (leaving `dir/file2.txt` untouched)
        let store = repo_a.store().clone();
        let c1 = store.get_commit(&cid_v1).unwrap();
        let tid_v1 = c1.tree_ids().as_resolved().unwrap().clone();
        let mut tb = TreeBuilder::new(Arc::clone(&store), tid_v1.clone());
        let file1_rp = RepoPathBuf::from_internal_string("file1.txt").unwrap();
        let new_fid = store
            .write_file(&file1_rp, &mut &b"updated via checkout_tree!"[..])
            .await
            .unwrap();
        tb.set(
            file1_rp,
            TreeValue::File {
                id: new_fid,
                executable: false,
                copy_id: CopyId::placeholder(),
            },
        );
        let tid_v2 = tb.write_tree().await.unwrap();

        let mut tx = repo_a.start_transaction();
        let c2 = tx
            .repo_mut()
            .new_commit(
                vec![cid_v1.clone()],
                MergedTree::resolved(Arc::clone(&store), tid_v2.clone()),
            )
            .set_description("v2 commit")
            .write()
            .await
            .unwrap();
        let committed_repo = tx.commit("add v2 commit").await.unwrap();

        // Configure tight 64-byte CAS eviction budget to test overlay pinning
        // simultaneously
        let state_dir = tempfile::tempdir().unwrap();
        let paths = InstancePaths::resolve(Some("inval-test"), Some(state_dir.path()));
        paths.ensure_dirs().unwrap();
        let mut cfg = vfs_proto::VfsConfigFile::default();
        cfg.daemon.max_cache_bytes = 64;
        cfg.daemon.snapshot_debounce_ms = 60_000;
        cfg.save_atomic(&paths.config_path).unwrap();

        let runtime = DaemonRuntime::new(paths).unwrap();
        let repo_name = ws_a
            .path()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        runtime
            .registry()
            .add_source(
                "local".into(),
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

        let root_mnt = tempfile::tempdir().unwrap();
        let ws_mnt = tempfile::tempdir().unwrap();
        runtime
            .mount_target(None, root_mnt.path().to_path_buf(), false)
            .await
            .unwrap();
        runtime
            .mount_target(
                Some(format!("local:{repo_name}/default")),
                ws_mnt.path().to_path_buf(),
                false,
            )
            .await
            .unwrap();

        for _ in 0..30 {
            if ws_mnt.path().join("file1.txt").exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        }

        // Warm 60s kernel page & dentry cache on BOTH mounts!
        let root_file1 = root_mnt
            .path()
            .join("local/repos")
            .join(&repo_name)
            .join("users/local/default/file1.txt");
        assert_eq!(
            std::fs::read_to_string(ws_mnt.path().join("file1.txt")).unwrap(),
            "hello content 1"
        );
        assert_eq!(
            std::fs::read_to_string(&root_file1).unwrap(),
            "hello content 1"
        );

        // Switch workspace to `c2` via `checkout_tree` -> must diff only
        // `file1.txt` and invalidate 60s kernel cache!
        let ws_key = WorkspaceKey {
            source: "local".into(),
            repo: repo_name.clone(),
            user: "local".into(),
            workspace: "default".into(),
        };
        let co_res = runtime
            .checkout_tree(
                ws_key.clone(),
                Some(tid_v1.hex()),
                tid_v2.hex(),
                Some(c2.id().hex()),
                Some(committed_repo.op_id().hex()),
            )
            .await
            .unwrap();
        assert_eq!(co_res.invalidated_paths, vec!["file1.txt".to_string()]);

        // Immediately (<10ms, well inside the 60s kernel TTL!) read both mounts
        // -> must see v2!
        assert_eq!(
            std::fs::read_to_string(ws_mnt.path().join("file1.txt")).unwrap(),
            "updated via checkout_tree!"
        );
        assert_eq!(
            std::fs::read_to_string(&root_file1).unwrap(),
            "updated via checkout_tree!"
        );

        // Write an un-snapshotted 120-byte file in `overlay/staged/` and churn
        // `BlobCas` past its 64-byte cap!
        let pinned_payload = "U".repeat(120);
        std::fs::write(ws_mnt.path().join("unacked.txt"), &pinned_payload).unwrap();
        for idx in 0..20 {
            runtime
                .registry()
                .cas()
                .put_blob(&format!("cold_blob_{idx}"), &[b'C'; 48])
                .unwrap();
        }
        assert!(runtime.registry().cas().stats().1 <= 64);
        // Unacknowledged staged file lives outside `BlobCas` and is NEVER
        // evicted!
        assert_eq!(
            std::fs::read_to_string(ws_mnt.path().join("unacked.txt")).unwrap(),
            pinned_payload
        );

        // Clean up overlay before unmount so local repo doesn't need to commit
        // `unacked.txt`
        std::fs::remove_file(ws_mnt.path().join("unacked.txt")).unwrap();
        runtime.unmount_all().await;
    }

    #[tokio::test]
    async fn checkout_tree_rejects_older_ancestor_op_and_updates_head_before_diff() {
        use jj_lib::repo::Repo as _;

        let (ws_a, repo_a, cid_v1) = setup_test_repo().await;
        let op_v1_hex = repo_a.op_id().hex();
        let c1 = repo_a.store().get_commit(&cid_v1).unwrap();
        let tid_v1_hex = c1.tree_ids().as_resolved().unwrap().hex();

        let mut tx = repo_a.start_transaction();
        let c2 = tx
            .repo_mut()
            .new_commit(vec![cid_v1.clone()], c1.tree())
            .set_description("child op commit")
            .write()
            .await
            .unwrap();
        tx.repo_mut()
            .set_wc_commit(
                jj_lib::ref_name::WorkspaceNameBuf::from("default"),
                c2.id().clone(),
            )
            .unwrap();
        let repo_v2 = tx.commit("advance to v2").await.unwrap();
        let op_v2_hex = repo_v2.op_id().hex();

        let state_dir = tempfile::tempdir().unwrap();
        let paths = InstancePaths::resolve(Some("op-guard-test"), Some(state_dir.path()));
        let runtime = DaemonRuntime::new(paths).unwrap();
        let repo_name = ws_a
            .path()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        runtime
            .registry()
            .add_source(
                "local".into(),
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

        let ws_key = WorkspaceKey {
            source: "local".into(),
            repo: repo_name,
            user: "local".into(),
            workspace: "default".into(),
        };
        let initial_ref = runtime
            .router()
            .resolve_workspace_ref(&ws_key)
            .await
            .unwrap();
        assert_eq!(initial_ref.operation_id_hex, op_v2_hex);

        // Attempting to revert from `op_v2` back to strict ancestor `op_v1`
        // must fail!
        let revert_err = runtime
            .checkout_tree(
                ws_key.clone(),
                Some(tid_v1_hex.clone()),
                tid_v1_hex.clone(),
                Some(cid_v1.hex()),
                Some(op_v1_hex),
            )
            .await
            .unwrap_err();
        assert!(revert_err.to_string().contains("refusing to revert"));
        assert_eq!(
            runtime
                .router()
                .resolve_workspace_ref(&ws_key)
                .await
                .unwrap()
                .commit_id_hex,
            c2.id().hex()
        );

        // Even if `old_tree_id_hex` is missing from store (causing best-effort
        // `diff_trees` to error), `checkout_tree` updates the workspace head
        // first and succeeds!
        let missing_old_tree = "ff".repeat(20);
        let co_ok = runtime
            .checkout_tree(
                ws_key.clone(),
                Some(missing_old_tree),
                tid_v1_hex,
                Some(cid_v1.hex()),
                Some(op_v2_hex),
            )
            .await
            .unwrap();
        assert_eq!(co_ok.commit_id_hex.as_deref(), Some(cid_v1.hex().as_str()));
        assert_eq!(
            runtime
                .router()
                .resolve_workspace_ref(&ws_key)
                .await
                .unwrap()
                .commit_id_hex,
            cid_v1.hex()
        );
    }
}
