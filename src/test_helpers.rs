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

use std::path::Path;
use std::sync::Arc;

use jj_lib::backend::CommitId;
use jj_lib::backend::CopyId;
use jj_lib::backend::MillisSinceEpoch;
use jj_lib::backend::Signature;
use jj_lib::backend::Timestamp;
use jj_lib::backend::TreeValue;
use jj_lib::backend::{self};
use jj_lib::config::ConfigLayer;
use jj_lib::config::ConfigSource;
use jj_lib::config::StackedConfig;
use jj_lib::default_backend_factories::default_backend_factories;
use jj_lib::default_backend_factories::default_working_copy_factories;
use jj_lib::merge::Merge;
use jj_lib::merged_tree::MergedTree;
use jj_lib::merged_tree_builder::MergedTreeBuilder;
use jj_lib::ref_name::WorkspaceName;
use jj_lib::repo::ReadonlyRepo;
use jj_lib::repo::Repo as _;
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::settings::UserSettings;
use jj_lib::workspace::DefaultWorkspaceLoaderFactory;
use jj_lib::workspace::Workspace;
use jj_lib::workspace::WorkspaceLoaderFactory as _;

pub fn user_settings() -> UserSettings {
    let config_text = r#"
        user.name = "Test User"
        user.email = "test.user@example.com"
        operation.username = "test-username"
        operation.hostname = "host.example.com"
        debug.randomness-seed = 42
    "#;
    let mut config = StackedConfig::with_defaults();
    config.add_layer(ConfigLayer::parse(ConfigSource::User, config_text).unwrap());
    UserSettings::from_config_and_home_dir(config, None).unwrap()
}

pub async fn setup_test_repo() -> (tempfile::TempDir, Arc<ReadonlyRepo>, CommitId) {
    let settings = user_settings();
    let temp_dir = tempfile::tempdir().unwrap();
    let workspace_root = temp_dir.path().to_path_buf();

    // Initialize a simple workspace
    let (workspace, repo) =
        Workspace::init_simple(&settings, &workspace_root, WorkspaceName::DEFAULT)
            .await
            .unwrap();

    let store = repo.store().clone();

    // Create a tree with a file and a subdirectory containing a file
    let mut tree_builder = MergedTreeBuilder::new(MergedTree::resolved(
        store.clone(),
        store.empty_tree_id().clone(),
    ));

    let file1_path = RepoPathBuf::from_relative_path(Path::new("file1.txt")).unwrap();
    let file2_path = RepoPathBuf::from_relative_path(Path::new("dir/file2.txt")).unwrap();

    let file1_content = b"hello content 1";
    let file2_content = b"hello content 2";

    let file1_id = store
        .write_file(&file1_path, &mut &file1_content[..])
        .await
        .unwrap();
    tree_builder.set_or_remove(
        file1_path.clone(),
        Some(TreeValue::File {
            id: file1_id,
            executable: false,
            copy_id: CopyId::placeholder(),
        }),
    );

    let file2_id = store
        .write_file(&file2_path, &mut &file2_content[..])
        .await
        .unwrap();
    tree_builder.set_or_remove(
        file2_path.clone(),
        Some(TreeValue::File {
            id: file2_id,
            executable: false,
            copy_id: CopyId::placeholder(),
        }),
    );

    let symlink_path = RepoPathBuf::from_relative_path(Path::new("symlink")).unwrap();
    let symlink_id = store
        .write_symlink(&symlink_path, "file1.txt")
        .await
        .unwrap();
    tree_builder.set_or_remove(symlink_path, Some(TreeValue::Symlink(symlink_id)));

    let tree_id = tree_builder
        .write_tree()
        .await
        .unwrap()
        .tree_ids()
        .as_resolved()
        .unwrap()
        .clone();

    // Write a commit pointing to this tree
    let signature = Signature {
        name: "Test User".to_string(),
        email: "test.user@example.com".to_string(),
        timestamp: Timestamp {
            timestamp: MillisSinceEpoch(0),
            tz_offset: 0,
        },
    };
    let commit_data = backend::Commit {
        parents: vec![store.root_commit_id().clone()],
        predecessors: vec![],
        root_tree: Merge::resolved(tree_id),
        conflict_labels: Merge::resolved("".to_string()),
        change_id: backend::ChangeId::from_hex("0000000000000000000000000000abcd"),
        description: "test commit".to_string(),
        author: signature.clone(),
        committer: signature,
        secure_sig: None,
    };
    let commit = store.write_commit(commit_data, None).await.unwrap();

    // Update the working copy commit ID for the workspace name
    let mut tx = repo.start_transaction();
    tx.repo_mut().add_head(&commit).await.unwrap();
    tx.repo_mut()
        .set_wc_commit(workspace.workspace_name().to_owned(), commit.id().clone())
        .unwrap();
    let _repo = tx.commit("set working copy").await.unwrap();

    // Load the readonly repo at head
    let loaded_workspace = DefaultWorkspaceLoaderFactory
        .create(&workspace_root)
        .unwrap()
        .load(
            &settings,
            &default_backend_factories(),
            &default_working_copy_factories(),
        )
        .unwrap();
    let repo_loader = loaded_workspace.repo_loader();
    let readonly_repo = repo_loader.load_at_head(None).await.unwrap();

    (temp_dir, readonly_repo, commit.id().clone())
}
