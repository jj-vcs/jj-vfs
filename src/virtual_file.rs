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

use std::path::PathBuf;
use std::pin::Pin;
use std::time::SystemTime;

use async_trait::async_trait;
use futures::AsyncRead;
use futures::stream::BoxStream;

use crate::jj_error::JjError;

#[derive(Clone, Copy)]
pub enum FileType {
    File,
    Directory,
    Symlink,
}

pub struct FileAttributes {
    pub size: u64,
    pub file_type: FileType,
    pub created: SystemTime,
    pub modified: SystemTime,
}

#[derive(Clone)]
pub struct DirectoryEntry {
    pub name: String,
    pub file_type: FileType,
}

pub type DirectoryStream = BoxStream<'static, DirectoryEntry>;

/// This trait represents a file in our virtual file system. This can either be
/// a normal file you can read from or for example a directory, in which case
/// you can list its contents.
///
/// A `VirtualFile` is not meant to be created by the user, but instead returned
/// by a `PathMapper`. The underlying implementation of `VirtualFile` should
/// contain all the necessary logic for interacting with the underlying
/// filesystem (jj-lib in this case) to get the data for a file.
#[async_trait]
pub trait VirtualFile: Send + Sync {
    async fn read(&self) -> Result<Pin<Box<dyn AsyncRead + Send>>, JjError> {
        Err(JjError::NotAFile)
    }

    async fn list(&self) -> Result<DirectoryStream, JjError> {
        Err(JjError::NotADirectory)
    }

    async fn read_link(&self) -> Result<PathBuf, JjError> {
        Err(JjError::NotASymlink)
    }

    async fn attributes(&self) -> Result<FileAttributes, JjError>;
    async fn file_type(&self) -> Result<FileType, JjError>;
}
