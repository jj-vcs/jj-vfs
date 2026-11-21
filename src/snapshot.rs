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

//! $O(k)$ dirty-path workspace snapshot engine, synthesized `.jj/` directory
//! provisioner, and Commit Cloud / local `jj_lib` commit publisher.
//!
//! # Authority and Crash-Safety Ordering
//! Background snapshots (`publish_op = true`) verify that the reconciled view's
//! working-copy commit and root tree still match `ws_ref` (and that `@` is
//! resolved with no descendants requiring rebase), then upload modified blobs,
//! symlinks, ancestor trees, commits, and operations before calling
//! `WorkspaceOverlay::complete_snapshot(captured_seq)`. Two-phase CLI snapshots
//! (`publish_op = false`) upload the $k$ dirty paths and return `(tree_id,
//! captured_seq)` while leaving staged files and `journal.log` intact until
//! `LockedVfsWorkingCopy::finish` commits the repository transaction and sends
//! `CheckoutTree`.
//!
//! # Concurrency and Merge Invariants
//! `rebuild_cc_subtree` traverses only ancestor directories of the $k$ paths
//! recorded in `journal.log`, leaving untouched repository subtrees unread and
//! `overlay/ignored/` build outputs off the network. Dirty overlays on
//! unresolved multi-tree conflicted commits return an error rather than
//! collapsing conflict sides.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use cc_common::EMPTY_TREE_ID_HEX;
use cc_common::backend::Commit as ProtoCommit;
use cc_common::backend::File as ProtoFile;
use cc_common::backend::Signature as ProtoSignature;
use cc_common::backend::Timestamp as ProtoTimestamp;
use cc_common::backend::TreeEntry as ProtoTreeEntry;
use cc_common::backend::TreeValue as ProtoTreeValueMsg;
use cc_common::backend::tree_value::Value as ProtoTreeValue;
use cc_common::op_store::CommitPredecessors as ProtoCommitPredecessors;
use cc_common::op_store::Operation as ProtoOperation;
use cc_common::op_store::OperationMetadata as ProtoOpMeta;
use cc_common::workspace::WorkspaceState as ProtoWorkspaceState;
use jj_lib::backend::CopyId;
use jj_lib::backend::TreeId;
use jj_lib::backend::TreeValue;
use jj_lib::config::ConfigLayer;
use jj_lib::config::ConfigSource;
use jj_lib::config::StackedConfig;
use jj_lib::merge::Merge;
use jj_lib::merged_tree::MergedTree;
use jj_lib::object_id::ObjectId as _;
use jj_lib::ref_name::WorkspaceNameBuf;
use jj_lib::repo::Repo as _;
use jj_lib::repo::StoreFactories;
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::settings::UserSettings;
use jj_lib::tree_builder::TreeBuilder;
use jj_lib::workspace::Workspace;
use jj_lib::workspace::default_working_copy_factories;
use vfs_proto::SnapshotResult;
use vfs_proto::VfsWorkingCopyStateFile;
use vfs_proto::WorkspaceKey;

use crate::backend::VfsWorkspaceRef;
use crate::backend::commit_cloud::CommitCloudVfsBackend;
use crate::jj_error::JjError;
use crate::overlay::DirtyEntryState;
use crate::overlay::WorkspaceOverlay;
use crate::overlay::workspace_storage_id;

/// Provisions `<state_dir>/workspaces/<ws_hash>/.jj/` for a Commit Cloud
/// workspace configured with `"commit_cloud"` stores and `"vfs"` working-copy
/// state.
pub fn ensure_synthesized_cc_jj_dir(
    state_dir: &Path,
    socket_path: &Path,
    key: &WorkspaceKey,
    cc_backend: &CommitCloudVfsBackend,
    ws_ref: &VfsWorkspaceRef,
) -> Result<PathBuf, JjError> {
    let ws_hash = workspace_storage_id(key);
    let jj_dir = state_dir.join("workspaces").join(ws_hash).join(".jj");
    let repo_dir = jj_dir.join("repo");
    let wc_dir = jj_dir.join("working_copy");

    let store_dir = repo_dir.join("store");
    let op_store_dir = repo_dir.join("op_store");
    let op_heads_dir = repo_dir.join("op_heads");
    let index_dir = repo_dir.join("index");
    let submodule_dir = repo_dir.join("submodule_store");

    for dir in [
        &store_dir,
        &op_store_dir,
        &op_heads_dir,
        &index_dir,
        &submodule_dir,
        &wc_dir,
    ] {
        std::fs::create_dir_all(dir)?;
    }

    write_if_missing(&store_dir.join("type"), b"commit_cloud")?;
    write_if_missing(&op_store_dir.join("type"), b"commit_cloud")?;
    write_if_missing(&op_heads_dir.join("type"), b"commit_cloud")?;
    write_if_missing(&index_dir.join("type"), b"commit_cloud")?;
    write_if_missing(&submodule_dir.join("type"), b"default")?;
    write_if_missing(&wc_dir.join("type"), b"vfs")?;

    let cc_cfg = cc_lib::util::CommitCloudConfig {
        server_url: cc_backend.server_url().to_string(),
        repo_id: cc_backend.repo_id().to_string(),
        project_id: None,
    };
    let cfg_toml = toml::to_string_pretty(&cc_cfg)
        .map_err(|e| std::io::Error::other(format!("toml serialize error: {e}")))?;
    std::fs::write(store_dir.join("config.toml"), cfg_toml)?;

    if !wc_dir.join(VfsWorkingCopyStateFile::FILENAME).is_file() {
        let tree_ids_hex = if ws_ref.tree_ids_hex.is_empty() {
            vec![ws_ref.tree_id_hex.clone()]
        } else {
            ws_ref.tree_ids_hex.clone()
        };
        let state = VfsWorkingCopyStateFile {
            socket_path: socket_path.to_path_buf(),
            workspace: key.clone(),
            operation_id_hex: ws_ref.operation_id_hex.clone(),
            tree_ids_hex,
            conflict_labels: ws_ref.conflict_labels.clone(),
            commit_id_hex: ws_ref.commit_id_hex.clone(),
            sparse_patterns: vec![String::new()],
        };
        state
            .save_atomic(&wc_dir)
            .map_err(|e| std::io::Error::other(format!("save vfs_state.json failed: {e}")))?;
    }

    Ok(jj_dir)
}

/// Updates `.jj/working_copy/vfs_state.json` when a daemon-side snapshot or
/// checkout advances heads.
pub fn sync_synthesized_vfs_state_file(
    state_dir: &Path,
    socket_path: &Path,
    key: &WorkspaceKey,
    ws_ref: &VfsWorkspaceRef,
) {
    let ws_hash = workspace_storage_id(key);
    let wc_dir = state_dir
        .join("workspaces")
        .join(ws_hash)
        .join(".jj/working_copy");
    if wc_dir.is_dir() {
        let sparse_patterns = VfsWorkingCopyStateFile::load(&wc_dir)
            .ok()
            .map(|s| s.sparse_patterns)
            .filter(|p| !p.is_empty())
            .unwrap_or_else(|| vec![String::new()]);
        let tree_ids_hex = if ws_ref.tree_ids_hex.is_empty() {
            vec![ws_ref.tree_id_hex.clone()]
        } else {
            ws_ref.tree_ids_hex.clone()
        };
        let state = VfsWorkingCopyStateFile {
            socket_path: socket_path.to_path_buf(),
            workspace: key.clone(),
            operation_id_hex: ws_ref.operation_id_hex.clone(),
            tree_ids_hex,
            conflict_labels: ws_ref.conflict_labels.clone(),
            commit_id_hex: ws_ref.commit_id_hex.clone(),
            sparse_patterns,
        };
        let _ = state.save_atomic(&wc_dir);
    }
}

/// Executes an $O(k)$ dirty-path snapshot against a Commit Cloud workspace.
pub async fn snapshot_workspace_cc(
    state_dir: &Path,
    socket_path: &Path,
    key: &WorkspaceKey,
    overlay: &Arc<WorkspaceOverlay>,
    cc_backend: &Arc<CommitCloudVfsBackend>,
    mut ws_ref: VfsWorkspaceRef,
    publish_op: bool,
) -> Result<(SnapshotResult, VfsWorkspaceRef), JjError> {
    let (captured_seq, dirty_map) = overlay.capture_dirty_snapshot();
    if dirty_map.is_empty() {
        return Ok((
            SnapshotResult {
                workspace: key.clone(),
                dirty_paths_processed: 0,
                tree_id_hex: ws_ref.tree_id_hex.clone(),
                commit_id_hex: Some(ws_ref.commit_id_hex.clone()),
                operation_id_hex: Some(ws_ref.operation_id_hex.clone()),
                captured_seq: None,
            },
            ws_ref,
        ));
    }

    if ws_ref.tree_ids_hex.len() > 1 {
        return Err(std::io::Error::other(
            "cannot snapshot dirty overlay onto conflicted working-copy commit; resolve conflicts \
             or run `jj new` first",
        )
        .into());
    }

    let dirty_count = dirty_map.len();
    if !publish_op {
        let new_tree_hex =
            rebuild_cc_subtree(cc_backend, "", &ws_ref.tree_id_hex, &dirty_map).await?;
        return Ok((
            SnapshotResult {
                workspace: key.clone(),
                dirty_paths_processed: dirty_count,
                tree_id_hex: new_tree_hex,
                commit_id_hex: None,
                operation_id_hex: None,
                captured_seq: Some(captured_seq),
            },
            ws_ref,
        ));
    }

    let (old_op_head, _old_op, mut view) = cc_backend.fetch_reconciled_view().await?;
    let expected_is_root =
        ws_ref.commit_id_hex.len() == 40 && ws_ref.commit_id_hex.bytes().all(|b| b == b'0');
    let old_wc_bytes = match view.wc_commit_ids.get(&key.workspace) {
        Some(bytes) => {
            let view_wc_hex = hex::encode(bytes);
            if view_wc_hex != ws_ref.commit_id_hex {
                return Err(std::io::Error::other(format!(
                    "skipping background snapshot: remote working-copy commit {} diverged from \
                     cached base {}",
                    view_wc_hex, ws_ref.commit_id_hex
                ))
                .into());
            }
            bytes.clone()
        }
        None => {
            if !expected_is_root {
                return Err(std::io::Error::other(format!(
                    "skipping background snapshot: workspace '{}' missing from reconciled view",
                    key.workspace
                ))
                .into());
            }
            vec![0u8; 20]
        }
    };
    let old_wc_hex = hex::encode(&old_wc_bytes);
    let old_commit = cc_backend.fetch_commit(&old_wc_hex).await?;
    if old_commit.root_tree_id.len() > 1 {
        return Err(std::io::Error::other(
            "cannot snapshot dirty overlay onto conflicted working-copy commit",
        )
        .into());
    }

    let is_root = old_wc_bytes.iter().all(|&b| b == 0);
    if !is_root {
        let old_commit_tree_hex = old_commit
            .root_tree_id
            .first()
            .map(hex::encode)
            .unwrap_or_else(|| EMPTY_TREE_ID_HEX.to_string());
        if old_commit_tree_hex != ws_ref.tree_id_hex {
            return Err(std::io::Error::other(format!(
                "skipping background snapshot: remote commit tree {} diverged from cached base \
                 tree {}",
                old_commit_tree_hex, ws_ref.tree_id_hex
            ))
            .into());
        }
        if !view.head_ids.contains(&old_wc_bytes) {
            return Err(std::io::Error::other(
                "skipping background snapshot: working-copy commit has visible descendants \
                 requiring rebase",
            )
            .into());
        }
        for other_head in view
            .head_ids
            .iter()
            .filter(|h| *h != &old_wc_bytes)
            .take(32)
        {
            if let Ok(other_commit) = cc_backend.fetch_commit(&hex::encode(other_head)).await
                && other_commit.parent_commit_ids.contains(&old_wc_bytes)
            {
                return Err(std::io::Error::other(
                    "skipping background snapshot: working-copy commit has visible descendants \
                     requiring rebase",
                )
                .into());
            }
        }
    }

    let new_tree_hex = rebuild_cc_subtree(cc_backend, "", &ws_ref.tree_id_hex, &dirty_map).await?;

    let now_millis = now_millis_i64();
    let sig = ProtoSignature {
        name: key.user.clone(),
        email: if key.user.contains('@') {
            key.user.clone()
        } else {
            format!("{}@localhost", key.user)
        },
        timestamp: Some(ProtoTimestamp {
            millis_since_epoch: now_millis,
            tz_offset: 0,
        }),
    };

    let new_tree_bytes = hex::decode(&new_tree_hex).map_err(|_| JjError::InvalidPath)?;
    let new_proto_commit = if is_root {
        ProtoCommit {
            commit_id: Vec::new(),
            parent_commit_ids: vec![vec![0u8; 20]],
            predecessors: Vec::new(),
            root_tree_id: vec![new_tree_bytes.clone()],
            change_id: derive_change_id(key, now_millis),
            description: String::new(),
            author: Some(sig.clone()),
            committer: Some(sig),
            ..Default::default()
        }
    } else {
        ProtoCommit {
            commit_id: Vec::new(),
            parent_commit_ids: old_commit.parent_commit_ids.clone(),
            predecessors: vec![old_wc_bytes.clone()],
            root_tree_id: vec![new_tree_bytes.clone()],
            conflict_labels: Vec::new(),
            change_id: old_commit.change_id.clone(),
            description: old_commit.description.clone(),
            author: old_commit.author.clone().or_else(|| Some(sig.clone())),
            committer: Some(sig),
            secure_sig: None,
            raw_git_commit: Vec::new(),
            external_ids: HashMap::new(),
        }
    };

    let new_cid_bytes = cc_backend.upload_commit(new_proto_commit).await?;
    let new_cid_hex = hex::encode(&new_cid_bytes);

    view.wc_commit_ids
        .insert(key.workspace.clone(), new_cid_bytes.clone());
    view.head_ids.retain(|h| h != &old_wc_bytes);
    if !view.head_ids.contains(&new_cid_bytes) {
        view.head_ids.push(new_cid_bytes.clone());
    }

    let predecessors = if is_root {
        Vec::new()
    } else {
        vec![ProtoCommitPredecessors {
            commit_id: new_cid_bytes.clone(),
            predecessor_ids: vec![old_wc_bytes],
        }]
    };

    let op = ProtoOperation {
        view_id: Vec::new(),
        parents: vec![old_op_head.clone()],
        metadata: Some(ProtoOpMeta {
            start_time_millis: now_millis,
            end_time_millis: now_millis,
            description: "snapshot working copy (jj-vfs)".to_string(),
            hostname: "localhost".to_string(),
            username: key.user.clone(),
            is_snapshot: true,
            workspace_name: Some(key.workspace.clone()),
            attributes: HashMap::new(),
        }),
        commit_predecessors: predecessors,
        commit_predecessors_set: true,
    };

    let ws_state = ProtoWorkspaceState {
        repo_id: cc_backend.repo_id().to_string(),
        user: key.user.clone(),
        workspace_name: key.workspace.clone(),
        commit_id: new_cid_bytes,
        operation_id: Vec::new(),
        tree_id: new_tree_bytes.clone(),
        sparse_patterns: Vec::new(),
        tree_ids: vec![new_tree_bytes],
    };

    let new_op_bytes = cc_backend
        .publish_snapshot_operation(old_op_head, view, op, Some(ws_state))
        .await?;
    let new_op_hex = hex::encode(new_op_bytes);

    ws_ref.commit_id_hex = new_cid_hex.clone();
    ws_ref.tree_id_hex = new_tree_hex.clone();
    ws_ref.tree_ids_hex = vec![new_tree_hex.clone()];
    ws_ref.conflict_labels.clear();
    ws_ref.operation_id_hex = new_op_hex.clone();

    sync_synthesized_vfs_state_file(state_dir, socket_path, key, &ws_ref);
    let _ = overlay.complete_snapshot(captured_seq)?;

    Ok((
        SnapshotResult {
            workspace: key.clone(),
            dirty_paths_processed: dirty_count,
            tree_id_hex: new_tree_hex,
            commit_id_hex: Some(new_cid_hex),
            operation_id_hex: Some(new_op_hex),
            captured_seq: None,
        },
        ws_ref,
    ))
}

/// Executes an $O(k)$ dirty-path snapshot against a local `jj_lib` workspace.
pub async fn snapshot_workspace_local(
    key: &WorkspaceKey,
    overlay: &Arc<WorkspaceOverlay>,
    local_jj_dir: &Path,
    ws_ref: VfsWorkspaceRef,
    publish_op: bool,
) -> Result<(SnapshotResult, VfsWorkspaceRef), JjError> {
    let key_owned = key.clone();
    let overlay_owned = Arc::clone(overlay);
    let jj_dir_owned = local_jj_dir.to_path_buf();

    tokio::task::spawn_blocking(move || {
        pollster::block_on(snapshot_workspace_local_inner(
            &key_owned,
            &overlay_owned,
            &jj_dir_owned,
            ws_ref,
            publish_op,
        ))
    })
    .await
    .map_err(|e| std::io::Error::other(format!("local snapshot task panicked: {e}")))?
}

async fn snapshot_workspace_local_inner(
    key: &WorkspaceKey,
    overlay: &Arc<WorkspaceOverlay>,
    local_jj_dir: &Path,
    mut ws_ref: VfsWorkspaceRef,
    publish_op: bool,
) -> Result<(SnapshotResult, VfsWorkspaceRef), JjError> {
    let (captured_seq, dirty_map) = overlay.capture_dirty_snapshot();
    if dirty_map.is_empty() {
        return Ok((
            SnapshotResult {
                workspace: key.clone(),
                dirty_paths_processed: 0,
                tree_id_hex: ws_ref.tree_id_hex.clone(),
                commit_id_hex: Some(ws_ref.commit_id_hex.clone()),
                operation_id_hex: Some(ws_ref.operation_id_hex.clone()),
                captured_seq: None,
            },
            ws_ref,
        ));
    }

    if ws_ref.tree_ids_hex.len() > 1 {
        return Err(std::io::Error::other(
            "cannot snapshot dirty overlay onto conflicted working-copy commit; resolve conflicts \
             or run `jj new` first",
        )
        .into());
    }

    let workspace_root = local_jj_dir.parent().ok_or(JjError::InvalidPath)?;
    let settings = default_snapshot_settings()?;
    let ws = Workspace::load(
        &settings,
        workspace_root,
        &StoreFactories::default(),
        &default_working_copy_factories(),
    )
    .map_err(|e| std::io::Error::other(format!("load local workspace failed: {e:?}")))?;
    let repo = ws
        .repo_loader()
        .load_at_head()
        .await
        .map_err(|e| std::io::Error::other(format!("load repo at head failed: {e:?}")))?;
    let store = repo.store().clone();

    let ws_name = WorkspaceNameBuf::from(key.workspace.clone());
    let old_commit_opt = if publish_op {
        let old_wc_id = repo
            .view()
            .get_wc_commit_id(&ws_name)
            .cloned()
            .ok_or(JjError::NotFound)?;
        if old_wc_id.hex() != ws_ref.commit_id_hex {
            return Err(std::io::Error::other(format!(
                "skipping background snapshot: local working-copy commit {} diverged from cached \
                 base {}",
                old_wc_id.hex(),
                ws_ref.commit_id_hex
            ))
            .into());
        }
        let old_commit = store.get_commit_async(&old_wc_id).await?;
        if !old_commit.tree_ids().is_resolved() {
            return Err(std::io::Error::other(
                "cannot snapshot dirty overlay onto conflicted working-copy commit",
            )
            .into());
        }
        let old_tree_hex = old_commit
            .tree_ids()
            .as_resolved()
            .map(|id| id.hex())
            .unwrap_or_default();
        if old_tree_hex != ws_ref.tree_id_hex {
            return Err(std::io::Error::other(format!(
                "skipping background snapshot: local commit tree {} diverged from cached base \
                 tree {}",
                old_tree_hex, ws_ref.tree_id_hex
            ))
            .into());
        }
        Some(old_commit)
    } else {
        None
    };

    let base_tree_id = TreeId::try_from_hex(&ws_ref.tree_id_hex).ok_or(JjError::InvalidPath)?;
    let mut builder = TreeBuilder::new(Arc::clone(&store), base_tree_id);

    for (rel_str, state) in &dirty_map {
        let repo_path =
            RepoPathBuf::from_internal_string(rel_str.clone()).map_err(|_| JjError::InvalidPath)?;
        match state {
            DirtyEntryState::Deleted => {
                builder.remove(repo_path);
            }
            DirtyEntryState::File {
                staged_path,
                executable,
            } => {
                let bytes = std::fs::read(staged_path)?;
                let fid = store.write_file(&repo_path, &mut &bytes[..]).await?;
                builder.set(
                    repo_path,
                    TreeValue::File {
                        id: fid,
                        executable: *executable,
                        copy_id: CopyId::placeholder(),
                    },
                );
            }
            DirtyEntryState::Symlink { target } => {
                let target_str = target.to_str().ok_or(JjError::InvalidPath)?;
                let sid = store.write_symlink(&repo_path, target_str).await?;
                builder.set(repo_path, TreeValue::Symlink(sid));
            }
        }
    }

    let new_tree_id = builder.write_tree().await?;
    let new_tree_hex = new_tree_id.hex();
    let dirty_count = dirty_map.len();

    if !publish_op {
        return Ok((
            SnapshotResult {
                workspace: key.clone(),
                dirty_paths_processed: dirty_count,
                tree_id_hex: new_tree_hex,
                commit_id_hex: None,
                operation_id_hex: None,
                captured_seq: Some(captured_seq),
            },
            ws_ref,
        ));
    }

    let old_commit = old_commit_opt.expect("verified when publish_op is true");
    let new_merged_tree = MergedTree::resolved(Arc::clone(&store), new_tree_id);

    let mut tx = repo.start_transaction();
    let rewritten = tx
        .repo_mut()
        .rewrite_commit(&old_commit)
        .set_tree(new_merged_tree)
        .write()
        .await?;
    tx.repo_mut()
        .set_wc_commit(ws_name, rewritten.id().clone())
        .map_err(|e| std::io::Error::other(format!("set_wc_commit failed: {e:?}")))?;
    tx.repo_mut()
        .rebase_descendants()
        .await
        .map_err(|e| std::io::Error::other(format!("rebase_descendants failed: {e:?}")))?;
    let committed_repo = tx
        .commit("snapshot working copy (jj-vfs)")
        .await
        .map_err(|e| std::io::Error::other(format!("tx commit failed: {e:?}")))?;

    let new_cid_hex = rewritten.id().hex();
    let new_op_hex = committed_repo.op_id().hex();
    let _ = Merge::resolved(());
    ws_ref.commit_id_hex = new_cid_hex.clone();
    ws_ref.tree_id_hex = new_tree_hex.clone();
    ws_ref.tree_ids_hex = vec![new_tree_hex.clone()];
    ws_ref.conflict_labels.clear();
    ws_ref.operation_id_hex = new_op_hex.clone();
    let _ = overlay.complete_snapshot(captured_seq)?;

    Ok((
        SnapshotResult {
            workspace: key.clone(),
            dirty_paths_processed: dirty_count,
            tree_id_hex: new_tree_hex,
            commit_id_hex: Some(new_cid_hex),
            operation_id_hex: Some(new_op_hex),
            captured_seq: None,
        },
        ws_ref,
    ))
}

async fn rebuild_cc_subtree(
    cc_backend: &Arc<CommitCloudVfsBackend>,
    rel_dir: &str,
    base_tree_hex: &str,
    subtree_dirty: &BTreeMap<String, DirtyEntryState>,
) -> Result<String, JjError> {
    let base_entries = cc_backend.fetch_tree(base_tree_hex).await?;
    let mut entries_map: BTreeMap<String, ProtoTreeEntry> = base_entries
        .iter()
        .map(|e| (e.name.clone(), e.clone()))
        .collect();

    let mut leaf_ops: BTreeMap<String, DirtyEntryState> = BTreeMap::new();
    let mut subdir_ops: BTreeMap<String, BTreeMap<String, DirtyEntryState>> = BTreeMap::new();

    for (rel, state) in subtree_dirty {
        if let Some((first, rest)) = rel.split_once('/') {
            subdir_ops
                .entry(first.to_string())
                .or_default()
                .insert(rest.to_string(), state.clone());
        } else {
            leaf_ops.insert(rel.clone(), state.clone());
        }
    }

    for (name, state) in &leaf_ops {
        match state {
            DirtyEntryState::Deleted => {
                entries_map.remove(name);
            }
            DirtyEntryState::File {
                staged_path,
                executable,
            } => {
                let bytes = std::fs::read(staged_path)?;
                let size_bytes = bytes.len() as u64;
                let fid = cc_backend.upload_blob(&bytes).await?;
                entries_map.insert(
                    name.clone(),
                    ProtoTreeEntry {
                        name: name.clone(),
                        value: Some(ProtoTreeValueMsg {
                            value: Some(ProtoTreeValue::File(ProtoFile {
                                id: fid,
                                executable: *executable,
                                copy_id: Vec::new(),
                                size_bytes: Some(size_bytes),
                            })),
                        }),
                    },
                );
            }
            DirtyEntryState::Symlink { target } => {
                let target_str = target.to_str().ok_or(JjError::InvalidPath)?;
                let sid = cc_backend.upload_symlink(target_str).await?;
                entries_map.insert(
                    name.clone(),
                    ProtoTreeEntry {
                        name: name.clone(),
                        value: Some(ProtoTreeValueMsg {
                            value: Some(ProtoTreeValue::SymlinkId(sid)),
                        }),
                    },
                );
            }
        }
    }

    for (sub_name, sub_dirty) in subdir_ops {
        if matches!(
            leaf_ops.get(&sub_name),
            Some(DirtyEntryState::File { .. } | DirtyEntryState::Symlink { .. })
        ) {
            continue;
        }
        let existing_sub_hex = entries_map
            .get(&sub_name)
            .and_then(|e| e.value.as_ref())
            .and_then(|v| v.value.as_ref())
            .and_then(|val| match val {
                ProtoTreeValue::TreeId(tid) => Some(hex::encode(tid)),
                _ => None,
            })
            .unwrap_or_else(|| EMPTY_TREE_ID_HEX.to_string());

        let child_rel = if rel_dir.is_empty() {
            sub_name.clone()
        } else {
            format!("{rel_dir}/{sub_name}")
        };
        let updated_sub_hex = Box::pin(rebuild_cc_subtree(
            cc_backend,
            &child_rel,
            &existing_sub_hex,
            &sub_dirty,
        ))
        .await?;
        if updated_sub_hex == EMPTY_TREE_ID_HEX {
            entries_map.remove(&sub_name);
        } else {
            let tid_bytes = hex::decode(&updated_sub_hex).map_err(|_| JjError::InvalidPath)?;
            entries_map.insert(
                sub_name.clone(),
                ProtoTreeEntry {
                    name: sub_name,
                    value: Some(ProtoTreeValueMsg {
                        value: Some(ProtoTreeValue::TreeId(tid_bytes)),
                    }),
                },
            );
        }
    }

    if entries_map.is_empty() && !rel_dir.is_empty() {
        return Ok(EMPTY_TREE_ID_HEX.to_string());
    }
    let tid = cc_backend
        .upload_tree(rel_dir, entries_map.into_values().collect())
        .await?;
    Ok(hex::encode(tid))
}

fn write_if_missing(path: &Path, content: &[u8]) -> std::io::Result<()> {
    if !path.is_file() {
        std::fs::write(path, content)?;
    }
    Ok(())
}

fn now_millis_i64() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn derive_change_id(key: &WorkspaceKey, millis: i64) -> Vec<u8> {
    let seed = format!(
        "{}:{}:{}:{}:{millis}",
        key.source, key.repo, key.user, key.workspace
    );
    let mut out = vec![0u8; 16];
    let mut h: u64 = 0xcbf29ce484222325;
    for (idx, b) in seed.bytes().enumerate() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x100000001b3);
        out[idx % 16] ^= (h >> ((idx % 8) * 8)) as u8;
    }
    if out.iter().all(|&b| b == 0) {
        out[0] = 1;
    }
    out
}

fn default_snapshot_settings() -> Result<UserSettings, JjError> {
    let cfg_text = r#"
        user.name = "jj-vfs"
        user.email = "jj-vfs@localhost"
        operation.username = "jj-vfs"
        operation.hostname = "localhost"
    "#;
    let mut config = StackedConfig::with_defaults();
    let layer = ConfigLayer::parse(ConfigSource::User, cfg_text)
        .map_err(|e| std::io::Error::other(format!("config parse error: {e}")))?;
    config.add_layer(layer);
    UserSettings::from_config(config)
        .map_err(|e| std::io::Error::other(format!("settings error: {e}")).into())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use cc_common::backend::backend_service_server::BackendService;
    use cc_common::backend::backend_service_server::BackendServiceServer;
    use cc_common::backend::*;
    use jj_lib::conflict_labels::ConflictLabels;
    use jj_lib::merge::Merge;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::ReceiverStream;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::Request;
    use tonic::Response;
    use tonic::Status;

    use super::*;
    use crate::backend::commit_cloud::CcSourceAuth;
    use crate::backend::commit_cloud::make_proto_file_entry;
    use crate::backend::commit_cloud::make_proto_tree_entry;
    use crate::backend::local::LocalRepoVfsBackend;
    use crate::cas::BlobCas;
    use crate::overlay::OverlayManager;
    use crate::test_helpers::setup_test_repo;

    struct TreeWriteMock {
        trees: parking_lot::Mutex<BTreeMap<Vec<u8>, Vec<ProtoTreeEntry>>>,
        next_id: AtomicUsize,
    }

    #[tonic::async_trait]
    impl BackendService for TreeWriteMock {
        type ReadFileStream = ReceiverStream<Result<ReadFileResponse, Status>>;

        async fn register_project(
            &self,
            _: Request<RegisterProjectRequest>,
        ) -> Result<Response<RegisterProjectResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }

        async fn register_repository(
            &self,
            _: Request<RegisterRepositoryRequest>,
        ) -> Result<Response<RegisterRepositoryResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }

        async fn read_commit(
            &self,
            _: Request<ReadCommitRequest>,
        ) -> Result<Response<ReadCommitResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }

        async fn write_commit(
            &self,
            _: Request<WriteCommitRequest>,
        ) -> Result<Response<WriteCommitResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }

        async fn list_project_commits(
            &self,
            _: Request<ListProjectCommitsRequest>,
        ) -> Result<Response<ListProjectCommitsResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }

        async fn list_commits(
            &self,
            _: Request<ListCommitsRequest>,
        ) -> Result<Response<ListCommitsResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }

        async fn resolve_commit_prefix(
            &self,
            _: Request<ResolveCommitPrefixRequest>,
        ) -> Result<Response<ResolveCommitPrefixResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }

        async fn resolve_change_prefix(
            &self,
            _: Request<ResolveChangePrefixRequest>,
        ) -> Result<Response<ResolveChangePrefixResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }

        async fn read_tree(
            &self,
            req: Request<ReadTreeRequest>,
        ) -> Result<Response<ReadTreeResponse>, Status> {
            let tid = req.into_inner().tree_id;
            let entries = self.trees.lock().get(&tid).cloned().unwrap_or_default();
            Ok(Response::new(ReadTreeResponse {
                tree_id: tid,
                entries,
                next_page_token: String::new(),
            }))
        }

        async fn write_tree(
            &self,
            req: Request<WriteTreeRequest>,
        ) -> Result<Response<WriteTreeResponse>, Status> {
            let inner = req.into_inner();
            let n = self.next_id.fetch_add(1, Ordering::Relaxed);
            let mut tid = vec![0xaa; 20];
            tid[0] = (n & 0xff) as u8;
            self.trees.lock().insert(tid.clone(), inner.entries);
            Ok(Response::new(WriteTreeResponse { tree_id: tid }))
        }

        async fn read_file(
            &self,
            _: Request<ReadFileRequest>,
        ) -> Result<Response<Self::ReadFileStream>, Status> {
            Err(Status::unimplemented("unused"))
        }

        async fn write_file(
            &self,
            _: Request<WriteFileRequest>,
        ) -> Result<Response<WriteFileResponse>, Status> {
            let n = self.next_id.fetch_add(1, Ordering::Relaxed);
            let mut fid = vec![0xbb; 20];
            fid[0] = (n & 0xff) as u8;
            Ok(Response::new(WriteFileResponse { file_id: fid }))
        }

        async fn write_file_stream(
            &self,
            _: Request<tonic::Streaming<WriteFileChunk>>,
        ) -> Result<Response<WriteFileResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }

        async fn read_symlink(
            &self,
            _: Request<ReadSymlinkRequest>,
        ) -> Result<Response<ReadSymlinkResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }

        async fn write_symlink(
            &self,
            _: Request<WriteSymlinkRequest>,
        ) -> Result<Response<WriteSymlinkResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
    }

    #[tokio::test]
    async fn rebuild_cc_subtree_preserves_file_replacing_directory() {
        let sub_tid = vec![0x11; 20];
        let base_tid = vec![0x22; 20];
        let mut initial_trees = BTreeMap::new();
        initial_trees.insert(
            sub_tid.clone(),
            vec![make_proto_file_entry("x", vec![0x01; 20], false, 3)],
        );
        initial_trees.insert(
            base_tid.clone(),
            vec![
                make_proto_tree_entry("a", sub_tid),
                make_proto_file_entry("keep.txt", vec![0x02; 20], false, 5),
            ],
        );
        let mock = TreeWriteMock {
            trees: parking_lot::Mutex::new(initial_trees),
            next_id: AtomicUsize::new(1),
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(BackendServiceServer::new(mock))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let tmp = tempfile::tempdir().unwrap();
        let cas = Arc::new(BlobCas::new(tmp.path().join("cas"), 1024 * 1024).unwrap());
        let cc = Arc::new(CommitCloudVfsBackend::new(
            "corp",
            "webapp",
            format!("http://{addr}"),
            CcSourceAuth::default(),
            cas,
        ));

        let staged_a = tmp.path().join("staged_a");
        std::fs::write(&staged_a, b"hi\n").unwrap();

        let mut dirty_map = BTreeMap::new();
        dirty_map.insert(
            "a".to_string(),
            DirtyEntryState::File {
                staged_path: staged_a,
                executable: false,
            },
        );
        dirty_map.insert("a/x".to_string(), DirtyEntryState::Deleted);

        let new_root_hex = rebuild_cc_subtree(&cc, "", &hex::encode(&base_tid), &dirty_map)
            .await
            .unwrap();
        let entries = cc.fetch_tree(&new_root_hex).await.unwrap();
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["a", "keep.txt"]);
        assert!(matches!(
            entries[0].value.as_ref().and_then(|v| v.value.as_ref()),
            Some(ProtoTreeValue::File(_))
        ));

        server_task.abort();
    }

    #[tokio::test]
    async fn local_conflicted_workspace_preserves_all_tree_sides_and_rejects_dirty_snapshot() {
        let (ws_tmp, repo, cid_base) = setup_test_repo().await;
        let store = repo.store().clone();
        let c_base = store.get_commit(&cid_base).unwrap();
        let tid_base = c_base.tree_ids().as_resolved().unwrap().clone();

        let file1_rp = RepoPathBuf::from_internal_string("file1.txt").unwrap();
        let fid_a = store
            .write_file(&file1_rp, &mut &b"side A\n"[..])
            .await
            .unwrap();
        let mut tb_a = TreeBuilder::new(Arc::clone(&store), tid_base.clone());
        tb_a.set(
            file1_rp.clone(),
            jj_lib::backend::TreeValue::File {
                id: fid_a,
                executable: false,
                copy_id: CopyId::placeholder(),
            },
        );
        let tid_a = tb_a.write_tree().await.unwrap();

        let fid_b = store
            .write_file(&file1_rp, &mut &b"side B\n"[..])
            .await
            .unwrap();
        let mut tb_b = TreeBuilder::new(Arc::clone(&store), tid_base.clone());
        tb_b.set(
            file1_rp,
            jj_lib::backend::TreeValue::File {
                id: fid_b,
                executable: false,
                copy_id: CopyId::placeholder(),
            },
        );
        let tid_b = tb_b.write_tree().await.unwrap();

        let conflicted_tree = MergedTree::new(
            Arc::clone(&store),
            Merge::from_vec(vec![tid_a.clone(), tid_base.clone(), tid_b.clone()]),
            ConflictLabels::from_vec(vec![
                "side 1".to_string(),
                "base".to_string(),
                "side 2".to_string(),
            ]),
        );

        let mut tx = repo.start_transaction();
        let c_merge = tx
            .repo_mut()
            .new_commit(vec![cid_base.clone()], conflicted_tree)
            .set_description("conflicted wc")
            .write()
            .await
            .unwrap();
        tx.repo_mut()
            .set_wc_commit(WorkspaceNameBuf::from("default"), c_merge.id().clone())
            .unwrap();
        let _committed = tx.commit("checkout conflicted wc").await.unwrap();

        let state_tmp = tempfile::tempdir().unwrap();
        let cas = Arc::new(BlobCas::new(state_tmp.path().join("cas"), 1024 * 1024).unwrap());
        let backend: Arc<dyn crate::backend::ReadOnlyVfsBackend> =
            Arc::new(LocalRepoVfsBackend::open_jj_workspace("repo", ws_tmp.path(), cas).unwrap());

        let listed = backend.list_workspaces(None).await.unwrap();
        let ws_ref = listed
            .into_iter()
            .find(|w| w.workspace_name == "default")
            .unwrap();
        assert_eq!(ws_ref.tree_id_hex, tid_a.hex());
        assert_eq!(
            ws_ref.tree_ids_hex,
            vec![tid_a.hex(), tid_base.hex(), tid_b.hex()]
        );
        assert_eq!(
            ws_ref.conflict_labels,
            vec![
                "side 1".to_string(),
                "base".to_string(),
                "side 2".to_string()
            ]
        );

        let overlays = OverlayManager::new(state_tmp.path().join("overlays")).unwrap();
        let key = WorkspaceKey {
            source: "local".into(),
            repo: "repo".into(),
            user: "local".into(),
            workspace: "default".into(),
        };
        let overlay = overlays.get_or_open(&key).unwrap();
        let local_jj = ws_tmp.path().join(".jj");

        // Clean overlay on conflicted `@` succeeds with 0 dirty paths and
        // preserves all 3 sides
        let (clean_res, clean_ref) =
            snapshot_workspace_local(&key, &overlay, &local_jj, ws_ref.clone(), false)
                .await
                .unwrap();
        assert_eq!(clean_res.dirty_paths_processed, 0);
        assert_eq!(clean_ref.tree_ids_hex.len(), 3);

        // Dirty overlay on conflicted `@` returns an explicit error and keeps
        // staged file intact
        overlay
            .create_file(
                &backend,
                &ws_ref.commit_id_hex,
                Path::new("new.txt"),
                false,
                false,
            )
            .await
            .unwrap();
        overlay
            .write_at(
                &backend,
                &ws_ref.commit_id_hex,
                Path::new("new.txt"),
                0,
                b"edit",
            )
            .await
            .unwrap();

        let err_cli = snapshot_workspace_local(&key, &overlay, &local_jj, ws_ref.clone(), false)
            .await
            .unwrap_err();
        assert!(err_cli.to_string().contains("conflicted"));
        let err_bg = snapshot_workspace_local(&key, &overlay, &local_jj, ws_ref.clone(), true)
            .await
            .unwrap_err();
        assert!(err_bg.to_string().contains("conflicted"));
        assert!(overlay.has_dirty_tracked_paths());
    }

    #[tokio::test]
    async fn two_phase_snapshot_keeps_overlay_and_background_skips_stale_base() {
        let (ws_tmp, repo, cid_v1) = setup_test_repo().await;
        let state_tmp = tempfile::tempdir().unwrap();
        let cas = Arc::new(BlobCas::new(state_tmp.path().join("cas"), 1024 * 1024).unwrap());
        let backend: Arc<dyn crate::backend::ReadOnlyVfsBackend> =
            Arc::new(LocalRepoVfsBackend::open_jj_workspace("repo", ws_tmp.path(), cas).unwrap());
        let ws_ref_v1 = backend
            .list_workspaces(None)
            .await
            .unwrap()
            .into_iter()
            .find(|w| w.workspace_name == "default")
            .unwrap();

        let overlays = OverlayManager::new(state_tmp.path().join("overlays")).unwrap();
        let key = WorkspaceKey {
            source: "local".into(),
            repo: "repo".into(),
            user: "local".into(),
            workspace: "default".into(),
        };
        let overlay = overlays.get_or_open(&key).unwrap();
        overlay
            .create_file(
                &backend,
                &ws_ref_v1.commit_id_hex,
                Path::new("added.txt"),
                false,
                false,
            )
            .await
            .unwrap();
        overlay
            .write_at(
                &backend,
                &ws_ref_v1.commit_id_hex,
                Path::new("added.txt"),
                0,
                b"two-phase",
            )
            .await
            .unwrap();

        let local_jj = ws_tmp.path().join(".jj");
        let (snap_res, unchanged_ref) =
            snapshot_workspace_local(&key, &overlay, &local_jj, ws_ref_v1.clone(), false)
                .await
                .unwrap();
        assert_eq!(snap_res.dirty_paths_processed, 1);
        let seq = snap_res
            .captured_seq
            .expect("publish_op=false must return captured_seq");
        assert!(
            overlay.has_dirty_tracked_paths(),
            "overlay must stay intact before finish()"
        );
        assert_eq!(unchanged_ref.tree_id_hex, ws_ref_v1.tree_id_hex);

        // Now move `@` externally in the repo to `c2` while `ws_ref_v1` still
        // points to `cid_v1`!
        let mut tx = repo.start_transaction();
        let c1 = repo.store().get_commit(&cid_v1).unwrap();
        let c2 = tx
            .repo_mut()
            .new_commit(vec![cid_v1.clone()], c1.tree())
            .set_description("external move")
            .write()
            .await
            .unwrap();
        tx.repo_mut()
            .set_wc_commit(WorkspaceNameBuf::from("default"), c2.id().clone())
            .unwrap();
        tx.commit("move wc externally").await.unwrap();

        let stale_err = snapshot_workspace_local(&key, &overlay, &local_jj, ws_ref_v1, true)
            .await
            .unwrap_err();
        assert!(stale_err.to_string().contains("diverged"));
        assert!(overlay.has_dirty_tracked_paths());

        assert!(overlay.complete_snapshot(seq).unwrap());
        assert!(!overlay.has_dirty_tracked_paths());
    }
}
