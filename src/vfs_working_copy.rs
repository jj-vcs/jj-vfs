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

//! $O(k)$ zero-walk `jj_lib::working_copy::WorkingCopy` implementation
//! (`"vfs"`) communicating with `jjfsd` over `control.sock`.
//!
//! # Authority and Crash-Safety Ordering
//! `LockedVfsWorkingCopy::snapshot` sends `SnapshotWorkspace { publish_op:
//! false }` to upload the $k$ dirty tracked paths in `journal.log` and return
//! `(TreeId, captured_seq)` while keeping staged files on disk and leaving
//! `OpHeads` untouched for the caller's `ReadonlyRepo`. `jj_cli` then writes
//! the working-copy `Commit` and `Operation` inside its own transaction and
//! calls `LockedVfsWorkingCopy::finish(new_op_id)`, which sends `CheckoutTree`
//! (clearing `captured_seq`, advancing the mount head, and updating
//! `WorkspaceService` under the per-workspace lock) and persists
//! `vfs_state.json`.
//!
//! # Concurrency and Merge Invariants
//! `start_mutation` reloads `vfs_state.json` so concurrent daemon snapshots
//! surface through `old_operation_id` and `old_tree`. Multi-tree conflicted
//! `Merge<TreeId>` checkouts and `ConflictLabels` round-trip intact through
//! `vfs_state.json` and `CheckoutTree`; clean snapshots return conflicted trees
//! unchanged and dirty overlays on conflicted commits return an error rather
//! than collapsing conflict sides.

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use jj_lib::backend::TreeId;
use jj_lib::commit::Commit;
use jj_lib::conflict_labels::ConflictLabels;
use jj_lib::merge::Merge;
use jj_lib::merged_tree::MergedTree;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_store::OperationId;
use jj_lib::ref_name::WorkspaceName;
use jj_lib::ref_name::WorkspaceNameBuf;
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::settings::UserSettings;
use jj_lib::store::Store;
use jj_lib::working_copy::CheckoutError;
use jj_lib::working_copy::CheckoutStats;
use jj_lib::working_copy::LockedWorkingCopy;
use jj_lib::working_copy::ResetError;
use jj_lib::working_copy::SnapshotError;
use jj_lib::working_copy::SnapshotOptions;
use jj_lib::working_copy::SnapshotStats;
use jj_lib::working_copy::WorkingCopy;
use jj_lib::working_copy::WorkingCopyFactory;
use jj_lib::working_copy::WorkingCopyStateError;
use vfs_proto::ControlRequest;
use vfs_proto::ControlResponse;
use vfs_proto::InstancePaths;
use vfs_proto::VfsWorkingCopyStateFile;
use vfs_proto::WorkspaceKey;
use vfs_proto::send_control_request;

pub struct VfsWorkingCopy {
    store: Arc<Store>,
    state_path: PathBuf,
    socket_path: PathBuf,
    workspace_key: WorkspaceKey,
    workspace_name: WorkspaceNameBuf,
    operation_id: OperationId,
    tree: MergedTree,
    commit_id_hex: String,
    sparse_patterns: Vec<RepoPathBuf>,
}

impl VfsWorkingCopy {
    pub fn name() -> &'static str {
        "vfs"
    }
}

#[async_trait(?Send)]
impl WorkingCopy for VfsWorkingCopy {
    fn name(&self) -> &str {
        Self::name()
    }

    fn workspace_name(&self) -> &WorkspaceName {
        &self.workspace_name
    }

    fn operation_id(&self) -> &OperationId {
        &self.operation_id
    }

    fn tree(&self) -> Result<&MergedTree, WorkingCopyStateError> {
        Ok(&self.tree)
    }

    fn sparse_patterns(&self) -> Result<&[RepoPathBuf], WorkingCopyStateError> {
        Ok(&self.sparse_patterns)
    }

    async fn start_mutation(&self) -> Result<Box<dyn LockedWorkingCopy>, WorkingCopyStateError> {
        let (
            socket_path,
            workspace_key,
            workspace_name,
            operation_id,
            tree,
            commit_id_hex,
            sparse_patterns,
        ) = if self
            .state_path
            .join(VfsWorkingCopyStateFile::FILENAME)
            .is_file()
        {
            let loaded = load_parsed_vfs_state(&self.store, &self.state_path)?;
            (
                loaded.socket_path,
                loaded.workspace_key,
                loaded.workspace_name,
                loaded.operation_id,
                loaded.tree,
                loaded.commit_id_hex,
                loaded.sparse_patterns,
            )
        } else {
            (
                self.socket_path.clone(),
                self.workspace_key.clone(),
                self.workspace_name.clone(),
                self.operation_id.clone(),
                self.tree.clone(),
                self.commit_id_hex.clone(),
                self.sparse_patterns.clone(),
            )
        };

        Ok(Box::new(LockedVfsWorkingCopy {
            store: Arc::clone(&self.store),
            state_path: self.state_path.clone(),
            socket_path,
            workspace_key,
            old_workspace_name: workspace_name.clone(),
            workspace_name,
            old_operation_id: operation_id,
            old_tree: tree.clone(),
            current_tree: tree,
            commit_id_hex,
            checked_out: false,
            captured_snapshot_seq: None,
            old_sparse_patterns: sparse_patterns.clone(),
            sparse_patterns,
        }))
    }
}

pub struct LockedVfsWorkingCopy {
    store: Arc<Store>,
    state_path: PathBuf,
    socket_path: PathBuf,
    workspace_key: WorkspaceKey,
    old_workspace_name: WorkspaceNameBuf,
    workspace_name: WorkspaceNameBuf,
    old_operation_id: OperationId,
    old_tree: MergedTree,
    current_tree: MergedTree,
    commit_id_hex: String,
    checked_out: bool,
    captured_snapshot_seq: Option<u64>,
    old_sparse_patterns: Vec<RepoPathBuf>,
    sparse_patterns: Vec<RepoPathBuf>,
}

#[async_trait]
impl LockedWorkingCopy for LockedVfsWorkingCopy {
    fn old_operation_id(&self) -> &OperationId {
        &self.old_operation_id
    }

    fn old_tree(&self) -> &MergedTree {
        &self.old_tree
    }

    async fn snapshot(
        &mut self,
        _options: &SnapshotOptions,
    ) -> Result<(MergedTree, SnapshotStats), SnapshotError> {
        let sock = self.socket_path.clone();
        let ws_key = self.workspace_key.clone();
        let resp = cc_lib::util::run_on_tokio(async move {
            let r = send_control_request(
                &sock,
                &ControlRequest::SnapshotWorkspace {
                    workspace: ws_key,
                    publish_op: false,
                },
            )
            .await?;
            Ok(r)
        })
        .await
        .map_err(|err| SnapshotError::Other {
            message: format!("failed to contact jjfsd control socket: {err}"),
            err,
        })?;

        match resp {
            ControlResponse::Snapshotted(res) => {
                if res.dirty_paths_processed > 0 {
                    if !self.current_tree.tree_ids().is_resolved() {
                        return Err(SnapshotError::Other {
                            message: "cannot snapshot dirty files onto a conflicted working-copy \
                                      commit; resolve conflicts or run `jj new` first"
                                .to_string(),
                            err: "conflicted working-copy commit".into(),
                        });
                    }
                    let new_tid = TreeId::try_from_hex(&res.tree_id_hex).ok_or_else(|| {
                        SnapshotError::Other {
                            message: format!("invalid tree_id hex '{}'", res.tree_id_hex),
                            err: "invalid tree_id hex".into(),
                        }
                    })?;
                    self.current_tree = MergedTree::resolved(Arc::clone(&self.store), new_tid);
                    self.captured_snapshot_seq = res.captured_seq;
                }
                Ok((self.current_tree.clone(), SnapshotStats::default()))
            }
            ControlResponse::Error { message, .. } => Err(SnapshotError::Other {
                message: format!("jjfsd snapshot error: {message}"),
                err: message.into(),
            }),
            other => Err(SnapshotError::Other {
                message: format!("unexpected jjfsd snapshot response: {other:?}"),
                err: "unexpected control response".into(),
            }),
        }
    }

    async fn check_out(&mut self, commit: &Commit) -> Result<CheckoutStats, CheckoutError> {
        self.current_tree = commit.tree();
        self.commit_id_hex = commit.id().hex();
        self.checked_out = true;
        Ok(CheckoutStats::default())
    }

    fn rename_workspace(&mut self, new_workspace_name: WorkspaceNameBuf) {
        self.workspace_key.workspace = new_workspace_name.as_str().to_string();
        self.workspace_name = new_workspace_name;
    }

    async fn reset(&mut self, commit: &Commit) -> Result<(), ResetError> {
        self.current_tree = commit.tree();
        self.commit_id_hex = commit.id().hex();
        self.checked_out = true;
        Ok(())
    }

    async fn recover(&mut self, commit: &Commit) -> Result<(), ResetError> {
        self.current_tree = commit.tree();
        self.commit_id_hex = commit.id().hex();
        self.checked_out = true;
        Ok(())
    }

    fn sparse_patterns(&self) -> Result<&[RepoPathBuf], WorkingCopyStateError> {
        Ok(&self.sparse_patterns)
    }

    async fn set_sparse_patterns(
        &mut self,
        new_sparse_patterns: Vec<RepoPathBuf>,
    ) -> Result<CheckoutStats, CheckoutError> {
        self.sparse_patterns = new_sparse_patterns;
        Ok(CheckoutStats::default())
    }

    async fn finish(
        self: Box<Self>,
        new_operation_id: OperationId,
    ) -> Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
        let store = self.store;
        let state_path = self.state_path;
        let socket_path = self.socket_path;
        let workspace_key = self.workspace_key;
        let old_workspace_name = self.old_workspace_name;
        let workspace_name = self.workspace_name;
        let current_tree = self.current_tree;
        let old_tree = self.old_tree;
        let old_operation_id = self.old_operation_id;
        let mut commit_id_hex = self.commit_id_hex;
        let checked_out = self.checked_out;
        let captured_snapshot_seq = self.captured_snapshot_seq;
        let old_sparse_patterns = self.old_sparse_patterns;
        let sparse_patterns = self.sparse_patterns;

        let unchanged = new_operation_id == old_operation_id
            && current_tree.tree_ids_and_labels() == old_tree.tree_ids_and_labels()
            && !checked_out
            && captured_snapshot_seq.is_none()
            && workspace_name == old_workspace_name
            && sparse_patterns == old_sparse_patterns;
        if unchanged {
            return Ok(Box::new(VfsWorkingCopy {
                store,
                state_path,
                socket_path,
                workspace_key,
                workspace_name,
                operation_id: new_operation_id,
                tree: current_tree,
                commit_id_hex,
                sparse_patterns,
            }));
        }

        let new_op_hex = new_operation_id.hex();
        let tree_ids_hex: Vec<String> = current_tree.tree_ids().iter().map(|id| id.hex()).collect();
        let conflict_labels: Vec<String> = current_tree.labels().as_slice().to_vec();
        let primary_tree_hex = tree_ids_hex
            .first()
            .cloned()
            .unwrap_or_else(|| store.empty_tree_id().hex());
        let old_primary_tree_hex = old_tree.tree_ids().iter().next().map(|id| id.hex());

        let new_cid_for_notify = if checked_out || new_operation_id == old_operation_id {
            (!commit_id_hex.is_empty()).then(|| commit_id_hex.clone())
        } else {
            None
        };

        let sock_for_notify = socket_path.clone();
        let ws_for_notify = workspace_key.clone();
        let new_tree_for_notify = primary_tree_hex;
        let new_tree_ids_for_notify = tree_ids_hex.clone();
        let labels_for_notify = conflict_labels.clone();
        let new_op_for_notify = new_op_hex.clone();

        let notify_resp = cc_lib::util::run_on_tokio(async move {
            let resp = send_control_request(
                &sock_for_notify,
                &ControlRequest::CheckoutTree {
                    workspace: ws_for_notify,
                    old_tree_id_hex: old_primary_tree_hex,
                    new_tree_id_hex: new_tree_for_notify,
                    new_tree_ids_hex: new_tree_ids_for_notify,
                    conflict_labels: labels_for_notify,
                    new_commit_id_hex: new_cid_for_notify,
                    new_operation_id_hex: Some(new_op_for_notify),
                    captured_seq: captured_snapshot_seq,
                },
            )
            .await?;
            Ok(resp)
        })
        .await
        .map_err(|err| WorkingCopyStateError {
            message: format!("failed to notify jjfsd CheckoutTree over control socket: {err}"),
            err,
        })?;

        match notify_resp {
            ControlResponse::CheckedOut(co_res) => {
                if let Some(resolved_cid) = co_res.commit_id_hex
                    && !resolved_cid.is_empty()
                {
                    commit_id_hex = resolved_cid;
                }
            }
            ControlResponse::Error { message, .. } => {
                return Err(WorkingCopyStateError {
                    message: format!("jjfsd CheckoutTree failed: {message}"),
                    err: message.into(),
                });
            }
            other => {
                return Err(WorkingCopyStateError {
                    message: format!("unexpected jjfsd CheckoutTree response: {other:?}"),
                    err: "unexpected control response".into(),
                });
            }
        }

        let persisted = VfsWorkingCopyStateFile {
            socket_path: socket_path.clone(),
            workspace: workspace_key.clone(),
            operation_id_hex: new_op_hex,
            tree_ids_hex,
            conflict_labels,
            commit_id_hex: commit_id_hex.clone(),
            sparse_patterns: sparse_patterns
                .iter()
                .map(|p| p.as_internal_file_string().to_string())
                .collect(),
        };
        persisted
            .save_atomic(&state_path)
            .map_err(|err| WorkingCopyStateError {
                message: format!("failed to persist vfs_state.json: {err}"),
                err: Box::new(err),
            })?;

        Ok(Box::new(VfsWorkingCopy {
            store,
            state_path,
            socket_path,
            workspace_key,
            workspace_name,
            operation_id: new_operation_id,
            tree: current_tree,
            commit_id_hex,
            sparse_patterns,
        }))
    }
}

#[derive(Default)]
pub struct VfsWorkingCopyFactory;

impl VfsWorkingCopyFactory {
    pub fn new() -> Self {
        Self
    }
}

impl WorkingCopyFactory for VfsWorkingCopyFactory {
    fn init_working_copy(
        &self,
        store: Arc<Store>,
        _working_copy_path: PathBuf,
        state_path: PathBuf,
        operation_id: OperationId,
        workspace_name: WorkspaceNameBuf,
        settings: &UserSettings,
    ) -> Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
        let paths = InstancePaths::resolve(None, None);
        let user = cc_lib::cc_working_copy::resolve_workspace_user(settings);
        let ws_key = WorkspaceKey {
            source: "default".to_string(),
            repo: "default".to_string(),
            user,
            workspace: workspace_name.as_str().to_string(),
        };
        let empty_tid = store.empty_tree_id().clone();
        let root_cid_hex = store.root_commit_id().hex();
        let state = VfsWorkingCopyStateFile {
            socket_path: paths.socket_path.clone(),
            workspace: ws_key.clone(),
            operation_id_hex: operation_id.hex(),
            tree_ids_hex: vec![empty_tid.hex()],
            conflict_labels: Vec::new(),
            commit_id_hex: root_cid_hex.clone(),
            sparse_patterns: vec![String::new()],
        };
        state
            .save_atomic(&state_path)
            .map_err(|err| WorkingCopyStateError {
                message: format!("failed to initialize vfs_state.json: {err}"),
                err: Box::new(err),
            })?;
        let tree = MergedTree::resolved(Arc::clone(&store), empty_tid);
        Ok(Box::new(VfsWorkingCopy {
            store,
            state_path,
            socket_path: paths.socket_path,
            workspace_key: ws_key,
            workspace_name,
            operation_id,
            tree,
            commit_id_hex: root_cid_hex,
            sparse_patterns: vec![RepoPathBuf::root()],
        }))
    }

    fn load_working_copy(
        &self,
        store: Arc<Store>,
        _working_copy_path: PathBuf,
        state_path: PathBuf,
        _settings: &UserSettings,
    ) -> Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
        let parsed = load_parsed_vfs_state(&store, &state_path)?;
        Ok(Box::new(VfsWorkingCopy {
            store,
            state_path,
            socket_path: parsed.socket_path,
            workspace_key: parsed.workspace_key,
            workspace_name: parsed.workspace_name,
            operation_id: parsed.operation_id,
            tree: parsed.tree,
            commit_id_hex: parsed.commit_id_hex,
            sparse_patterns: parsed.sparse_patterns,
        }))
    }
}

struct ParsedVfsState {
    socket_path: PathBuf,
    workspace_key: WorkspaceKey,
    workspace_name: WorkspaceNameBuf,
    operation_id: OperationId,
    tree: MergedTree,
    commit_id_hex: String,
    sparse_patterns: Vec<RepoPathBuf>,
}

fn load_parsed_vfs_state(
    store: &Arc<Store>,
    state_path: &Path,
) -> Result<ParsedVfsState, WorkingCopyStateError> {
    let state = VfsWorkingCopyStateFile::load(state_path).map_err(|err| WorkingCopyStateError {
        message: format!("failed to load vfs_state.json: {err}"),
        err: Box::new(err),
    })?;
    let operation_id = OperationId::try_from_hex(&state.operation_id_hex).ok_or_else(|| {
        WorkingCopyStateError {
            message: format!("invalid operation_id hex '{}'", state.operation_id_hex),
            err: "invalid operation_id hex".into(),
        }
    })?;
    let parsed_tids: Vec<TreeId> = state
        .tree_ids_hex
        .iter()
        .filter_map(TreeId::try_from_hex)
        .collect();
    let tree = if parsed_tids.len() > 1 && parsed_tids.len() % 2 == 1 {
        let labels = if state.conflict_labels.len() == parsed_tids.len() {
            ConflictLabels::from_vec(state.conflict_labels)
        } else {
            ConflictLabels::unlabeled()
        };
        MergedTree::new(Arc::clone(store), Merge::from_vec(parsed_tids), labels)
    } else {
        let single = parsed_tids
            .into_iter()
            .next()
            .unwrap_or_else(|| store.empty_tree_id().clone());
        MergedTree::resolved(Arc::clone(store), single)
    };
    let sparse_patterns = if state.sparse_patterns.is_empty() {
        vec![RepoPathBuf::root()]
    } else {
        state
            .sparse_patterns
            .iter()
            .filter_map(|s| RepoPathBuf::from_internal_string(s.clone()).ok())
            .collect()
    };
    let workspace_name = WorkspaceNameBuf::from(state.workspace.workspace.clone());
    Ok(ParsedVfsState {
        socket_path: state.socket_path,
        workspace_key: state.workspace,
        workspace_name,
        operation_id,
        tree,
        commit_id_hex: state.commit_id_hex,
        sparse_patterns,
    })
}

#[cfg(test)]
mod tests {
    use jj_lib::config::ConfigLayer;
    use jj_lib::config::ConfigSource;
    use jj_lib::config::StackedConfig;
    use jj_lib::repo::Repo as _;

    use super::*;
    use crate::test_helpers::setup_test_repo;

    fn test_settings() -> UserSettings {
        let mut cfg = StackedConfig::with_defaults();
        cfg.add_layer(
            ConfigLayer::parse(
                ConfigSource::User,
                "user.name = \"Alice\"\nuser.email = \"alice@example.com\"\n",
            )
            .unwrap(),
        );
        UserSettings::from_config(cfg).unwrap()
    }

    #[tokio::test]
    async fn start_mutation_reloads_state_preserves_labels_and_finish_propagates_errors() {
        let (_ws_tmp, repo, cid) = setup_test_repo().await;
        let store = repo.store().clone();
        let settings = test_settings();
        let wc_tmp = tempfile::tempdir().unwrap();
        let state_dir = wc_tmp.path().join("wc_state");
        let dead_sock = wc_tmp.path().join("missing.sock");

        let factory = VfsWorkingCopyFactory::new();
        let wc = factory
            .init_working_copy(
                Arc::clone(&store),
                wc_tmp.path().to_path_buf(),
                state_dir.clone(),
                repo.op_id().clone(),
                WorkspaceNameBuf::from("default"),
                &settings,
            )
            .unwrap();

        // Simulate a concurrent daemon update writing a 3-side conflicted tree
        // + labels to vfs_state.json
        let c1 = store.get_commit(&cid).unwrap();
        let t1 = c1.tree_ids().as_resolved().unwrap().hex();
        let t_empty = store.empty_tree_id().hex();
        let updated_op_hex = "ab".repeat(repo.op_id().as_bytes().len());
        let external_state = VfsWorkingCopyStateFile {
            socket_path: dead_sock.clone(),
            workspace: WorkspaceKey {
                source: "corp".into(),
                repo: "webapp".into(),
                user: "alice@example.com".into(),
                workspace: "default".into(),
            },
            operation_id_hex: updated_op_hex.clone(),
            tree_ids_hex: vec![t1.clone(), t_empty.clone(), t1.clone()],
            conflict_labels: vec!["ours".into(), "base".into(), "theirs".into()],
            commit_id_hex: cid.hex(),
            sparse_patterns: vec![String::new()],
        };
        external_state.save_atomic(&state_dir).unwrap();

        // 1. `start_mutation` reloads the updated operation_id, 3-side
        //    Merge<TreeId>, and ConflictLabels!
        let locked = wc.start_mutation().await.unwrap();
        assert_eq!(locked.old_operation_id().hex(), updated_op_hex);
        assert_eq!(locked.old_tree().tree_ids().as_slice().len(), 3);
        assert_eq!(
            locked.old_tree().labels().as_slice(),
            &["ours", "base", "theirs"]
        );

        // 2. Clean no-op `finish(old_op)` skips `CheckoutTree` (succeeds even
        //    though `dead_sock` does not exist!)
        let unchanged_op = locked.old_operation_id().clone();
        let wc_after_noop = locked.finish(unchanged_op).await.unwrap();
        assert_eq!(wc_after_noop.operation_id().hex(), updated_op_hex);

        // 3. Mutating `finish(newer_op)` propagates `CheckoutTree` socket error
        //    as `WorkingCopyStateError`!
        let locked2 = wc_after_noop.start_mutation().await.unwrap();
        let newer_op =
            OperationId::try_from_hex("cd".repeat(repo.op_id().as_bytes().len())).unwrap();
        let finish_err = locked2.finish(newer_op).await;
        assert!(
            finish_err.is_err(),
            "finish() must propagate CheckoutTree errors"
        );
    }
}
