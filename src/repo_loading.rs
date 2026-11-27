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

//! Loads the repository that the daemon serves.

use std::path::Path;
use std::sync::Arc;

use jj_lib::default_backend_factories::default_backend_factories;
use jj_lib::default_backend_factories::default_working_copy_factories;
use jj_lib::repo::ReadonlyRepo;
use jj_lib::repo::RepoLoaderError;
use jj_lib::settings::UserSettings;
use jj_lib::workspace::DefaultWorkspaceLoaderFactory;
use jj_lib::workspace::WorkspaceLoadError;
use jj_lib::workspace::WorkspaceLoaderFactory as _;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum LoadRepoError {
    #[error("Failed to load workspace: {0}")]
    Workspace(#[from] WorkspaceLoadError),

    #[error("Failed to load repo at head: {0}")]
    Repo(#[from] RepoLoaderError),
}

/// Loads the workspace at `workspace_root` and its repo at the head operation.
pub async fn load_repo_at_head(
    settings: &UserSettings,
    workspace_root: &Path,
) -> Result<Arc<ReadonlyRepo>, LoadRepoError> {
    let loaded_workspace = DefaultWorkspaceLoaderFactory.create(workspace_root)?.load(
        settings,
        &default_backend_factories(),
        &default_working_copy_factories(),
    )?;
    // Load through the workspace so that, if divergent op heads have to be
    // merged, the new operation records this workspace like any other.
    Ok(loaded_workspace.load_at_head().await?)
}

#[cfg(test)]
mod tests {
    use jj_lib::ref_name::WorkspaceName;

    use super::*;
    use crate::test_helpers::setup_test_repo;
    use crate::test_helpers::user_settings;

    /// When two operations start from the same base operation, the repo has
    /// two op heads. Loading it at head merges them into a new "reconcile"
    /// operation, which should record the workspace it was made in, like
    /// every other operation the workspace makes.
    #[tokio::test]
    async fn test_reconcile_operation_records_workspace_name() {
        let (temp_dir, repo, _commit) = setup_test_repo().await;

        // Commit two transactions that both start from `repo`'s operation.
        repo.start_transaction().commit("first").await.unwrap();
        repo.start_transaction().commit("second").await.unwrap();

        let merged_repo = load_repo_at_head(&user_settings(), temp_dir.path())
            .await
            .unwrap();
        let operation = merged_repo.operation();

        assert_eq!(
            operation.parent_ids().len(),
            2,
            "expected the head operation to reconcile the two divergent operations"
        );
        assert_eq!(
            operation.metadata().workspace_name.as_deref(),
            Some(WorkspaceName::DEFAULT)
        );
    }
}
