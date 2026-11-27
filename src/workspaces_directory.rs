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

use std::sync::Arc;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use jj_lib::repo::ReadonlyRepo;

use crate::jj_error::JjError;
use crate::virtual_file::DirectoryEntry;
use crate::virtual_file::DirectoryStream;
use crate::virtual_file::FileAttributes;
use crate::virtual_file::FileType;
use crate::virtual_file::VirtualFile;

/// A directory of the workspaces in the repository.
pub struct WorkspacesDirectory {
    repo: Arc<ReadonlyRepo>,
}

impl WorkspacesDirectory {
    pub fn new(repo: Arc<ReadonlyRepo>) -> Self {
        Self { repo }
    }
}

#[async_trait]
impl VirtualFile for WorkspacesDirectory {
    #[tracing::instrument(skip(self))]
    async fn list(&self) -> Result<DirectoryStream, JjError> {
        let view = self.repo.view();
        let commits: Vec<DirectoryEntry> = view
            .wc_commit_ids()
            .keys()
            .map(|workspace_id| DirectoryEntry {
                name: workspace_id.as_str().to_string(),
                file_type: FileType::Directory,
            })
            .collect();
        Ok(Box::pin(futures::stream::iter(commits)))
    }

    #[tracing::instrument(skip(self))]
    async fn attributes(&self) -> Result<FileAttributes, JjError> {
        Ok(FileAttributes {
            size: 0,
            file_type: FileType::Directory,
            created: UNIX_EPOCH,
            modified: UNIX_EPOCH,
        })
    }

    async fn file_type(&self) -> Result<FileType, JjError> {
        Ok(FileType::Directory)
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt as _;

    use super::*;
    use crate::test_helpers::setup_test_repo;

    #[tokio::test]
    async fn test_repo_workspaces() {
        let (_temp_dir, repo, _commit) = setup_test_repo().await;
        let workspaces_dir = WorkspacesDirectory::new(repo);
        let stream = workspaces_dir.list().await.unwrap();
        let files: Vec<DirectoryEntry> = stream.collect().await;

        // The repository has 1 workspace.
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].name, "default");
        assert!(matches!(files[0].file_type, FileType::Directory));
    }
}
