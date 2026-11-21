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
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;

use crate::jj_error::JjError;
use crate::path_mapper::PathMapper;
use crate::virtual_file::DirectoryStream;
use crate::virtual_file::FileAttributes;

/// Middle-layer filesystem abstraction representing path-resolved VFS
/// operations between the FUSE transport (`JjFuse`) and the `PathMapper` /
/// overlay layers.
#[async_trait]
pub trait VirtualFilesystem {
    async fn get_attributes(&self, path: &Path) -> Result<FileAttributes, JjError>;
    async fn read(&self, path: &Path, offset: u64, size: u32) -> Result<Box<[u8]>, JjError>;
    async fn read_directory(&self, path: &Path) -> Result<DirectoryStream, JjError>;
    async fn read_link(&self, path: &Path) -> Result<PathBuf, JjError>;
}

pub struct PathMappedVfs<P: PathMapper> {
    path_mapper: Arc<P>,
}

impl<P: PathMapper> PathMappedVfs<P> {
    pub fn new(path_mapper: P) -> Self {
        Self {
            path_mapper: Arc::new(path_mapper),
        }
    }

    pub fn from_arc(path_mapper: Arc<P>) -> Self {
        Self { path_mapper }
    }
}

#[async_trait]
impl<P: PathMapper> VirtualFilesystem for PathMappedVfs<P> {
    #[tracing::instrument(skip(self))]
    async fn get_attributes(&self, path: &Path) -> Result<FileAttributes, JjError> {
        let virtual_file = self.path_mapper.get_entry(path).await?;
        virtual_file.attributes().await
    }

    #[tracing::instrument(skip(self))]
    async fn read(&self, path: &Path, offset: u64, size: u32) -> Result<Box<[u8]>, JjError> {
        let virtual_file = self.path_mapper.get_entry(path).await?;
        virtual_file.read_range(offset, size).await
    }

    #[tracing::instrument(skip(self))]
    async fn read_directory(&self, path: &Path) -> Result<DirectoryStream, JjError> {
        let virtual_file = self.path_mapper.get_entry(path).await?;
        virtual_file.list().await
    }

    #[tracing::instrument(skip(self))]
    async fn read_link(&self, path: &Path) -> Result<PathBuf, JjError> {
        let virtual_file = self.path_mapper.get_entry(path).await?;
        virtual_file.read_link().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::time::SystemTime;

    use async_trait::async_trait;
    use futures::StreamExt as _;
    use futures::io::Cursor;
    use futures::stream;

    use super::*;
    use crate::virtual_file::DirectoryEntry;
    use crate::virtual_file::DirectoryStream;
    use crate::virtual_file::FileType;
    use crate::virtual_file::VirtualFile;

    enum MockVirtualFile {
        File(Vec<u8>),
        Directory(HashMap<String, Arc<MockVirtualFile>>),
        Symlink(PathBuf),
    }

    #[async_trait]
    impl VirtualFile for Arc<MockVirtualFile> {
        async fn read(&self) -> Result<Pin<Box<dyn futures::AsyncRead + Send>>, JjError> {
            match &**self {
                MockVirtualFile::File(content) => Ok(Box::pin(Cursor::new(content.clone()))),
                MockVirtualFile::Directory(_) | MockVirtualFile::Symlink(_) => {
                    Err(JjError::NotAFile)
                }
            }
        }

        async fn read_range(&self, offset: u64, size: u32) -> Result<Box<[u8]>, JjError> {
            match &**self {
                MockVirtualFile::File(content) => {
                    let start = (offset as usize).min(content.len());
                    let end = start.saturating_add(size as usize).min(content.len());
                    Ok(content[start..end].to_vec().into_boxed_slice())
                }
                MockVirtualFile::Directory(_) | MockVirtualFile::Symlink(_) => {
                    Err(JjError::NotAFile)
                }
            }
        }

        async fn list(&self) -> Result<DirectoryStream, JjError> {
            match &**self {
                MockVirtualFile::Directory(entries) => {
                    let children = entries
                        .iter()
                        .map(|(name, file)| {
                            let file_type = match &**file {
                                MockVirtualFile::File(_) => FileType::File,
                                MockVirtualFile::Directory(_) => FileType::Directory,
                                MockVirtualFile::Symlink(_) => FileType::Symlink,
                            };
                            DirectoryEntry {
                                name: name.to_string(),
                                file_type,
                            }
                        })
                        .collect::<Vec<_>>();
                    let stream: DirectoryStream = Box::pin(stream::iter(children));
                    Ok(stream)
                }
                MockVirtualFile::File(_) | MockVirtualFile::Symlink(_) => {
                    Err(JjError::NotADirectory)
                }
            }
        }

        async fn read_link(&self) -> Result<PathBuf, JjError> {
            match &**self {
                MockVirtualFile::Symlink(target) => Ok(target.clone()),
                _ => Err(JjError::NotASymlink),
            }
        }

        async fn attributes(&self) -> Result<FileAttributes, JjError> {
            let now = SystemTime::now();
            match &**self {
                MockVirtualFile::File(content) => {
                    Ok(FileAttributes::file(content.len() as u64, false, now))
                }
                MockVirtualFile::Directory(_) => Ok(FileAttributes::directory(now)),
                MockVirtualFile::Symlink(target) => Ok(FileAttributes::symlink(
                    target.to_string_lossy().len() as u64,
                    now,
                )),
            }
        }

        async fn file_type(&self) -> Result<FileType, JjError> {
            match &**self {
                MockVirtualFile::File(_) => Ok(FileType::File),
                MockVirtualFile::Directory(_) => Ok(FileType::Directory),
                MockVirtualFile::Symlink(_) => Ok(FileType::Symlink),
            }
        }
    }

    struct MockPathMapper {
        root: Arc<MockVirtualFile>,
    }

    #[async_trait]
    impl PathMapper for MockPathMapper {
        async fn get_entry(&self, path: &Path) -> Result<Box<dyn VirtualFile>, JjError> {
            let mut current_entry = self.root.clone();
            for part in path.iter() {
                let part_str = part.to_str().ok_or(JjError::InvalidPath)?;
                if let MockVirtualFile::Directory(entries) = &*current_entry {
                    current_entry = entries.get(part_str).ok_or(JjError::NotFound)?.clone();
                } else {
                    return Err(JjError::NotFound);
                }
            }
            Ok(Box::new(current_entry))
        }
    }

    fn setup_test_vfs() -> PathMappedVfs<MockPathMapper> {
        let mut root_children = HashMap::new();
        root_children.insert(
            "file.txt".to_string(),
            Arc::new(MockVirtualFile::File(b"hello world".to_vec())),
        );
        root_children.insert(
            "symlink.txt".to_string(),
            Arc::new(MockVirtualFile::Symlink(PathBuf::from("file.txt"))),
        );

        let mut dir_children = HashMap::new();
        dir_children.insert(
            "nested.txt".to_string(),
            Arc::new(MockVirtualFile::File(b"nested content".to_vec())),
        );
        root_children.insert(
            "dir".to_string(),
            Arc::new(MockVirtualFile::Directory(dir_children)),
        );

        let root = Arc::new(MockVirtualFile::Directory(root_children));
        let mapper = MockPathMapper { root };

        PathMappedVfs::new(mapper)
    }

    #[tokio::test]
    async fn test_read_root_file_and_middle_range() {
        let fs = setup_test_vfs();
        let content = fs.read(Path::new("file.txt"), 0, 11).await.unwrap();
        assert_eq!(&*content, b"hello world");
        let middle = fs.read(Path::new("file.txt"), 6, 5).await.unwrap();
        assert_eq!(&*middle, b"world");
    }

    #[tokio::test]
    async fn test_read_nested_file() {
        let fs = setup_test_vfs();
        let content = fs.read(Path::new("dir/nested.txt"), 0, 14).await.unwrap();
        assert_eq!(&*content, b"nested content");
    }

    #[tokio::test]
    async fn test_list_directory() {
        let fs = setup_test_vfs();
        let entries: Vec<_> = fs
            .read_directory(Path::new("dir"))
            .await
            .unwrap()
            .collect()
            .await;

        assert_eq!(entries.len(), 1);
        let entry0 = entries.into_iter().next().unwrap();
        assert_eq!(entry0.name, "nested.txt");
        assert!(matches!(entry0.file_type, FileType::File));
    }

    #[tokio::test]
    async fn test_list_directory_not_a_directory() {
        let fs = setup_test_vfs();
        let result = fs.read_directory(Path::new("file.txt")).await;
        assert!(matches!(result, Err(JjError::NotADirectory)));
    }

    #[tokio::test]
    async fn test_list_directory_not_found() {
        let fs = setup_test_vfs();
        let result = fs.read_directory(Path::new("does_not_exist")).await;
        assert!(matches!(result, Err(JjError::NotFound)));
    }

    #[tokio::test]
    async fn test_read_link() {
        let fs = setup_test_vfs();
        let target = fs.read_link(Path::new("symlink.txt")).await.unwrap();
        assert_eq!(target, PathBuf::from("file.txt"));
    }

    #[tokio::test]
    async fn test_read_link_not_a_symlink() {
        let fs = setup_test_vfs();
        let result = fs.read_link(Path::new("file.txt")).await;
        assert!(matches!(result, Err(JjError::NotASymlink)));
    }
}
