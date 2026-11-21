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
use futures::io::AsyncReadExt as _;
use futures::stream::BoxStream;

use crate::jj_error::JjError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileType {
    File,
    Directory,
    Symlink,
}

#[derive(Clone, Debug)]
pub struct FileAttributes {
    pub size: u64,
    pub file_type: FileType,
    pub executable: bool,
    pub created: SystemTime,
    pub modified: SystemTime,
}

impl FileAttributes {
    pub fn file(size: u64, executable: bool, modified: SystemTime) -> Self {
        Self {
            size,
            file_type: FileType::File,
            executable,
            created: modified,
            modified,
        }
    }

    pub fn directory(modified: SystemTime) -> Self {
        Self {
            size: 0,
            file_type: FileType::Directory,
            executable: true,
            created: modified,
            modified,
        }
    }

    pub fn symlink(size: u64, modified: SystemTime) -> Self {
        Self {
            size,
            file_type: FileType::Symlink,
            executable: false,
            created: modified,
            modified,
        }
    }
}

#[derive(Clone, Debug)]
pub struct DirectoryEntry {
    pub name: String,
    pub file_type: FileType,
}

pub type DirectoryStream = BoxStream<'static, DirectoryEntry>;

/// Represents a virtual directory, file, or symlink resolved by a `PathMapper`.
#[async_trait]
pub trait VirtualFile: Send + Sync {
    async fn read(&self) -> Result<Pin<Box<dyn AsyncRead + Send>>, JjError> {
        Err(JjError::NotAFile)
    }

    /// Reads `[offset, offset + size)` directly without discarding prefix bytes
    /// when backed by ranged gRPC `ReadFile` or local `BlobCas` `pread`.
    async fn read_range(&self, offset: u64, size: u32) -> Result<Box<[u8]>, JjError> {
        let reader = self.read().await?;
        let mut skip_reader = reader.take(offset);
        futures::io::copy(&mut skip_reader, &mut futures::io::sink()).await?;
        let inner = skip_reader.into_inner();
        let mut content = Vec::with_capacity(size as usize);
        inner.take(size as u64).read_to_end(&mut content).await?;
        Ok(content.into_boxed_slice())
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
