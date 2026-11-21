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

use std::time::UNIX_EPOCH;

use async_trait::async_trait;

use crate::jj_error::JjError;
use crate::virtual_file::DirectoryEntry;
use crate::virtual_file::DirectoryStream;
use crate::virtual_file::FileAttributes;
use crate::virtual_file::FileType;
use crate::virtual_file::VirtualFile;

pub struct StaticDirectory {
    children: Vec<DirectoryEntry>,
}

impl StaticDirectory {
    pub fn new(children: Vec<DirectoryEntry>) -> Self {
        Self { children }
    }
}

#[async_trait]
impl VirtualFile for StaticDirectory {
    async fn list(&self) -> Result<DirectoryStream, JjError> {
        Ok(Box::pin(futures::stream::iter(self.children.clone())))
    }

    async fn attributes(&self) -> Result<FileAttributes, JjError> {
        Ok(FileAttributes::directory(UNIX_EPOCH))
    }

    async fn file_type(&self) -> Result<FileType, JjError> {
        Ok(FileType::Directory)
    }
}
