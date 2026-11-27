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

//! Compatibility bridge adapting `jj-vfs`'s `ReadOnlyVfsBackend` and
//! `WorkspaceOverlay` to the upcoming `src/core_vfs` trait boundaries
//! documented in `src/core_vfs/README.md` (Project Birdstride).
//!
//! # Authority and Crash-Safety Ordering
//! Kept in `src/core_vfs_bridge.rs` (outside `src/core_vfs/`) so incoming
//! Birdstride `src/core_vfs/*.rs` extractions land without file conflicts.
//! Tracked writes flow exclusively through `WorkspaceOverlay` (`staged/` +
//! `journal.log`) before acknowledging FUSE operations, while ignored build
//! outputs live under `overlay/ignored/` and optionally open as
//! `FileDescriptor::NativeHandle` when `FUSE_PASSTHROUGH` is enabled.
//!
//! # Concurrency and Merge Invariants
//! Mirrors `core_vfs`'s 20-bit `WorkspaceId` + 44-bit local inode packing so
//! cross-workspace `EXDEV` checks run in $O(1)$ directly on `INodeNo`, and
//! implements `HydratingDirectory`'s three-state lazy machine (`NotHydrated` ->
//! `Hydrated` | `Error`) so `getattr` storms never trigger remote tree fetches.
//!
//! # FUSE, IPC, and RPC Boundaries
//! `ObjectDigest` carries an explicit algorithm tag (`Sha1` for Commit Cloud M1
//! 1:1 Git fidelity, `Sha256` for RBE CAS and future chunked Merkle roots),
//! while `CoreDirectoryBackend::register_file` defaults to
//! `JjError::Unsupported` (`ENOTSUP`) until remote-build CAS pre-registration
//! hooks are wired.

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::fs::OpenOptions;
use std::os::fd::OwnedFd;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use fuser::INodeNo;
use tokio::sync::Mutex;

use crate::backend::ReadOnlyVfsBackend;
use crate::jj_error::JjError;
use crate::overlay::WorkspaceOverlay;
use crate::virtual_file::AttributeChanges;
use crate::virtual_file::DirectoryEntry;
use crate::virtual_file::FileAttributes;
use crate::virtual_file::FileType;

/// Lower 44 bits of `INodeNo` carrying the workspace-local inode counter,
/// matching `src/core_vfs/README.md`.
pub const LOCAL_INO_BITS: u32 = 44;
pub const LOCAL_INO_MASK: u64 = (1u64 << LOCAL_INO_BITS) - 1;
pub const MAX_WORKSPACE_ID: u32 = (1u32 << (64 - LOCAL_INO_BITS)) - 1;

/// 20-bit workspace identifier packed into the upper bits of `INodeNo` (`0`
/// denotes the global read-only namespace outside any mutable workspace).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkspaceId(u32);

impl WorkspaceId {
    pub fn new(raw: u32) -> Result<Self, JjError> {
        if raw == 0 || raw > MAX_WORKSPACE_ID {
            return Err(JjError::InvalidPath);
        }
        Ok(Self(raw))
    }

    pub fn raw(self) -> u32 {
        self.0
    }
}

/// Encodes `(workspace_id, local_ino)` into a `core_vfs`-compatible `INodeNo`.
pub fn encode_inode(workspace_id: Option<WorkspaceId>, local_ino: u64) -> Result<INodeNo, JjError> {
    if local_ino == 0 || local_ino > LOCAL_INO_MASK {
        return Err(JjError::InvalidPath);
    }
    let upper = u64::from(workspace_id.map_or(0, WorkspaceId::raw)) << LOCAL_INO_BITS;
    Ok(INodeNo(upper | local_ino))
}

/// Decodes a `core_vfs`-compatible `INodeNo` into `(workspace_id, local_ino)`.
pub fn decode_inode(ino: INodeNo) -> (Option<WorkspaceId>, u64) {
    let raw_ws = (ino.0 >> LOCAL_INO_BITS) as u32;
    let ws = if raw_ws == 0 {
        None
    } else {
        Some(WorkspaceId(raw_ws))
    };
    (ws, ino.0 & LOCAL_INO_MASK)
}

/// Returns `true` when both inodes belong to the same mutable workspace
/// (enabling $O(1)$ cross-workspace `EXDEV` detection prior to rename/link
/// locking).
pub fn is_same_workspace(a: INodeNo, b: INodeNo) -> bool {
    let (ws_a, _) = decode_inode(a);
    let (ws_b, _) = decode_inode(b);
    ws_a.is_some() && ws_a == ws_b
}

/// Caller identity mirroring `core_vfs::UserContext` and
/// `vfs_proto::CallerContext`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UserContext {
    pub uid: u32,
    pub gid: u32,
    pub pid: u32,
}

impl UserContext {
    pub fn current_process() -> Self {
        let caller = vfs_proto::CallerContext::current_process();
        Self {
            uid: caller.uid,
            gid: caller.gid,
            pid: caller.pid.unwrap_or_else(std::process::id),
        }
    }
}

/// Hash algorithm tag keeping `core_vfs` backends independent of RBE CAS
/// SHA-256 vs Commit Cloud M1 Git SHA-1 object IDs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DigestAlgorithm {
    /// Canonical Git blob/tree SHA-1 (`20` bytes), used by Commit Cloud M1.
    GitBlobSha1,
    /// Raw SHA-256 content digest (`32` bytes), used by RBE CAS and future
    /// chunked Merkle roots.
    Sha256,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ObjectDigest {
    pub algorithm: DigestAlgorithm,
    pub bytes: Vec<u8>,
}

impl ObjectDigest {
    pub fn git_blob_sha1(data: &[u8]) -> Result<Self, JjError> {
        let id = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Blob, data)
            .map_err(|e| std::io::Error::other(format!("sha1 hash failed: {e}")))?;
        Ok(Self {
            algorithm: DigestAlgorithm::GitBlobSha1,
            bytes: id.as_bytes().to_vec(),
        })
    }

    pub fn from_hex(algorithm: DigestAlgorithm, hex_str: &str) -> Result<Self, JjError> {
        let bytes = hex::decode(hex_str).map_err(|_| JjError::InvalidPath)?;
        Ok(Self { algorithm, bytes })
    }

    pub fn to_hex(&self) -> String {
        hex::encode(&self.bytes)
    }
}

/// File handle dispatch matching `src/core_vfs/README.md`
/// (`FileBackend::open`).
#[derive(Debug)]
pub enum FileDescriptor {
    /// FUSE `read`/`write` route through `CoreFileBackend::read`/`write`
    /// (always used for unprivileged mounts and tracked Commit Cloud files).
    Stateless,
    /// Host OS file descriptor eligible for Linux kernel `FUSE_PASSTHROUGH`
    /// (`reply.open_backing(fd)`) when the mount enables passthrough.
    NativeHandle(OwnedFd),
}

#[derive(Debug)]
pub struct OpenResult {
    pub fd: FileDescriptor,
    pub can_cache: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpendirResult {
    pub directory_handle: u64,
    pub can_cache: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileSizeUpdate {
    Truncate(u64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisteredFileSpec {
    pub digest: ObjectDigest,
    pub size: u64,
    pub executable: bool,
    pub eager_prefetch: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoreDirEntry {
    pub ino: INodeNo,
    pub offset: u64,
    pub file_type: FileType,
    pub name: OsString,
}

/// Layer 1 `DirectoryBackend` boundary aligned with `src/core_vfs/README.md`.
#[async_trait]
pub trait CoreDirectoryBackend: Send + Sync {
    async fn is_empty(&self) -> Result<bool, JjError>;
    async fn lookup(&self, name: &OsStr) -> Result<FileAttributes, JjError>;
    async fn get_children_count(&self) -> Result<usize, JjError>;
    async fn opendir(&self) -> Result<OpendirResult, JjError>;
    async fn readdir(&self, offset: u64) -> Result<Vec<CoreDirEntry>, JjError>;
    async fn releasedir(&self, _handle: u64) -> Result<(), JjError> {
        Ok(())
    }
    async fn mknod(&self, name: &OsStr, executable: bool) -> Result<FileAttributes, JjError>;
    async fn mkdir(&self, name: &OsStr, mode: u32) -> Result<FileAttributes, JjError>;
    async fn symlink(&self, name: &OsStr, target: &Path) -> Result<FileAttributes, JjError>;
    async fn remove_child(&self, name: &OsStr, is_dir: bool) -> Result<(), JjError>;
    async fn register_file(
        &self,
        _name: &OsStr,
        _spec: RegisteredFileSpec,
    ) -> Result<FileAttributes, JjError> {
        Err(JjError::Unsupported)
    }
    async fn getattr(&self) -> Result<FileAttributes, JjError>;
    async fn setattr(&self, changes: AttributeChanges) -> Result<FileAttributes, JjError>;
    async fn get_digest(&self, algorithm: DigestAlgorithm) -> Result<ObjectDigest, JjError>;
}

/// Layer 1 `FileBackend` boundary aligned with `src/core_vfs/README.md`.
#[async_trait]
pub trait CoreFileBackend: Send + Sync {
    async fn open(&self, flags: i32, passthrough_enabled: bool) -> Result<OpenResult, JjError>;
    async fn read(&self, offset: u64, size: u32) -> Result<Box<[u8]>, JjError>;
    async fn write(&self, offset: u64, data: &[u8]) -> Result<u32, JjError>;
    async fn release(&self) -> Result<(), JjError> {
        Ok(())
    }
    async fn getattr(&self) -> Result<FileAttributes, JjError>;
    async fn setattr(&self, changes: AttributeChanges) -> Result<FileAttributes, JjError>;
    async fn fallocate(&self, _offset: u64, _length: u64) -> Result<(), JjError> {
        Err(JjError::Unsupported)
    }
    async fn get_digest(&self, algorithm: DigestAlgorithm) -> Result<ObjectDigest, JjError>;
    async fn snapshot_local_file(&self) -> Result<ObjectDigest, JjError>;
    fn is_dirty(&self) -> bool;
}

/// Layer 1 `SymlinkBackend` boundary aligned with `src/core_vfs/README.md`.
#[async_trait]
pub trait CoreSymlinkBackend: Send + Sync {
    async fn readlink(&self) -> Result<OsString, JjError>;
    async fn getattr(&self) -> Result<FileAttributes, JjError>;
    async fn setattr(&self, changes: AttributeChanges) -> Result<FileAttributes, JjError>;
}

/// Three-state lazy directory hydration machine mirroring
/// `HydratingDirectoryBackend` in `src/core_vfs/README.md`.
pub enum HydratingState<NotHydrated, Hydrated> {
    NotHydrated(NotHydrated),
    Hydrated(Hydrated),
    Error(String),
}

/// Lazy-hydrating read-only commit directory backed by `ReadOnlyVfsBackend`.
///
/// `getattr` serves cached directory attributes without issuing `ReadTree`
/// RPCs. The first `lookup`, `readdir`, or `is_empty` transitions `NotHydrated`
/// to `Hydrated` (or sticky `Error` if the commit/path does not exist).
pub struct HydratingCommitDirectory {
    not_hydrated_attr: FileAttributes,
    next_dir_handle: AtomicU64,
    state: Mutex<HydratingState<CommitDirSpec, HydratedCommitDir>>,
}

struct CommitDirSpec {
    backend: Arc<dyn ReadOnlyVfsBackend>,
    commit_id_hex: String,
    rel_dir: PathBuf,
}

struct HydratedCommitDir {
    backend: Arc<dyn ReadOnlyVfsBackend>,
    commit_id_hex: String,
    rel_dir: PathBuf,
    entries: BTreeMap<String, DirectoryEntry>,
}

impl HydratingCommitDirectory {
    pub fn new(
        backend: Arc<dyn ReadOnlyVfsBackend>,
        commit_id_hex: impl Into<String>,
        rel_dir: impl Into<PathBuf>,
        not_hydrated_attr: FileAttributes,
    ) -> Self {
        Self {
            not_hydrated_attr,
            next_dir_handle: AtomicU64::new(1),
            state: Mutex::new(HydratingState::NotHydrated(CommitDirSpec {
                backend,
                commit_id_hex: commit_id_hex.into(),
                rel_dir: rel_dir.into(),
            })),
        }
    }

    async fn ensure_hydrated<'a>(
        &'a self,
    ) -> Result<
        tokio::sync::MutexGuard<'a, HydratingState<CommitDirSpec, HydratedCommitDir>>,
        JjError,
    > {
        let mut guard = self.state.lock().await;
        match &*guard {
            HydratingState::Hydrated(_) => return Ok(guard),
            HydratingState::Error(msg) => {
                return Err(std::io::Error::other(msg.clone()).into());
            }
            HydratingState::NotHydrated(_) => {}
        }

        let prev = std::mem::replace(
            &mut *guard,
            HydratingState::Error("hydration interrupted".to_string()),
        );
        let HydratingState::NotHydrated(spec) = prev else {
            unreachable!();
        };

        match spec
            .backend
            .list_dir_in_commit(&spec.commit_id_hex, &spec.rel_dir)
            .await
        {
            Ok(list) => {
                let entries = list.into_iter().map(|e| (e.name.clone(), e)).collect();
                *guard = HydratingState::Hydrated(HydratedCommitDir {
                    backend: spec.backend,
                    commit_id_hex: spec.commit_id_hex,
                    rel_dir: spec.rel_dir,
                    entries,
                });
                Ok(guard)
            }
            Err(err) => {
                *guard = HydratingState::Error(err.to_string());
                Err(err)
            }
        }
    }
}

#[async_trait]
impl CoreDirectoryBackend for HydratingCommitDirectory {
    async fn is_empty(&self) -> Result<bool, JjError> {
        let guard = self.ensure_hydrated().await?;
        let HydratingState::Hydrated(dir) = &*guard else {
            unreachable!();
        };
        Ok(dir.entries.is_empty())
    }

    async fn lookup(&self, name: &OsStr) -> Result<FileAttributes, JjError> {
        let name_str = name.to_str().ok_or(JjError::InvalidPath)?;
        let guard = self.ensure_hydrated().await?;
        let HydratingState::Hydrated(dir) = &*guard else {
            unreachable!();
        };
        if !dir.entries.contains_key(name_str) {
            return Err(JjError::NotFound);
        }
        let child_path = dir.rel_dir.join(name_str);
        Ok(dir
            .backend
            .lookup_in_commit(&dir.commit_id_hex, &child_path)
            .await?
            .attr)
    }

    async fn get_children_count(&self) -> Result<usize, JjError> {
        let guard = self.ensure_hydrated().await?;
        let HydratingState::Hydrated(dir) = &*guard else {
            unreachable!();
        };
        Ok(dir.entries.len())
    }

    async fn opendir(&self) -> Result<OpendirResult, JjError> {
        let _ = self.ensure_hydrated().await?;
        let handle = self.next_dir_handle.fetch_add(1, Ordering::Relaxed);
        Ok(OpendirResult {
            directory_handle: handle,
            can_cache: true,
        })
    }

    async fn readdir(&self, offset: u64) -> Result<Vec<CoreDirEntry>, JjError> {
        let guard = self.ensure_hydrated().await?;
        let HydratingState::Hydrated(dir) = &*guard else {
            unreachable!();
        };
        let skip = offset as usize;
        let out = dir
            .entries
            .values()
            .enumerate()
            .skip(skip)
            .map(|(idx, entry)| CoreDirEntry {
                ino: INodeNo((idx as u64) + 2),
                offset: (idx as u64) + 1,
                file_type: entry.file_type,
                name: OsString::from(&entry.name),
            })
            .collect();
        Ok(out)
    }

    async fn mknod(&self, _name: &OsStr, _executable: bool) -> Result<FileAttributes, JjError> {
        Err(JjError::ReadOnly)
    }

    async fn mkdir(&self, _name: &OsStr, _mode: u32) -> Result<FileAttributes, JjError> {
        Err(JjError::ReadOnly)
    }

    async fn symlink(&self, _name: &OsStr, _target: &Path) -> Result<FileAttributes, JjError> {
        Err(JjError::ReadOnly)
    }

    async fn remove_child(&self, _name: &OsStr, _is_dir: bool) -> Result<(), JjError> {
        Err(JjError::ReadOnly)
    }

    async fn getattr(&self) -> Result<FileAttributes, JjError> {
        // Deliberately serves attributes without triggering directory
        // hydration.
        Ok(self.not_hydrated_attr.clone())
    }

    async fn setattr(&self, _changes: AttributeChanges) -> Result<FileAttributes, JjError> {
        Err(JjError::ReadOnly)
    }

    async fn get_digest(&self, algorithm: DigestAlgorithm) -> Result<ObjectDigest, JjError> {
        let guard = self.ensure_hydrated().await?;
        let HydratingState::Hydrated(dir) = &*guard else {
            unreachable!();
        };
        if algorithm == DigestAlgorithm::GitBlobSha1 {
            let info = dir
                .backend
                .lookup_in_commit(&dir.commit_id_hex, &dir.rel_dir)
                .await?;
            if info.object_id_hex.len() == 40 {
                return ObjectDigest::from_hex(algorithm, &info.object_id_hex);
            }
        }
        digest_directory_entries(dir.entries.values(), algorithm)
    }
}

/// Mutable workspace directory implementing `CoreDirectoryBackend` over
/// `WorkspaceOverlay` + `ReadOnlyVfsBackend`.
pub struct JjWorkspaceDirectoryBackend {
    overlay: Arc<WorkspaceOverlay>,
    backend: Arc<dyn ReadOnlyVfsBackend>,
    base_commit_hex: String,
    rel_dir: String,
    workspace_id: WorkspaceId,
    next_dir_handle: AtomicU64,
}

impl JjWorkspaceDirectoryBackend {
    pub fn new(
        overlay: Arc<WorkspaceOverlay>,
        backend: Arc<dyn ReadOnlyVfsBackend>,
        base_commit_hex: impl Into<String>,
        rel_dir: impl Into<String>,
        workspace_id: WorkspaceId,
    ) -> Self {
        Self {
            overlay,
            backend,
            base_commit_hex: base_commit_hex.into(),
            rel_dir: rel_dir.into().trim_matches('/').to_string(),
            workspace_id,
            next_dir_handle: AtomicU64::new(1),
        }
    }

    fn child_rel(&self, name: &OsStr) -> Result<String, JjError> {
        let name_str = name.to_str().ok_or(JjError::InvalidPath)?;
        if name_str.is_empty() || name_str.contains('/') || name_str == "." || name_str == ".." {
            return Err(JjError::InvalidPath);
        }
        Ok(if self.rel_dir.is_empty() {
            name_str.to_string()
        } else {
            format!("{}/{name_str}", self.rel_dir)
        })
    }
}

#[async_trait]
impl CoreDirectoryBackend for JjWorkspaceDirectoryBackend {
    async fn is_empty(&self) -> Result<bool, JjError> {
        let entries = self
            .overlay
            .list_dir_at(&self.backend, &self.base_commit_hex, &self.rel_dir)
            .await?;
        Ok(entries.is_empty())
    }

    async fn lookup(&self, name: &OsStr) -> Result<FileAttributes, JjError> {
        let child = self.child_rel(name)?;
        self.overlay
            .attributes_at(&self.backend, &self.base_commit_hex, &child)
            .await
    }

    async fn get_children_count(&self) -> Result<usize, JjError> {
        let entries = self
            .overlay
            .list_dir_at(&self.backend, &self.base_commit_hex, &self.rel_dir)
            .await?;
        Ok(entries.len())
    }

    async fn opendir(&self) -> Result<OpendirResult, JjError> {
        let _ = self
            .overlay
            .list_dir_at(&self.backend, &self.base_commit_hex, &self.rel_dir)
            .await?;
        let handle = self.next_dir_handle.fetch_add(1, Ordering::Relaxed);
        Ok(OpendirResult {
            directory_handle: handle,
            can_cache: false,
        })
    }

    async fn readdir(&self, offset: u64) -> Result<Vec<CoreDirEntry>, JjError> {
        let entries = self
            .overlay
            .list_dir_at(&self.backend, &self.base_commit_hex, &self.rel_dir)
            .await?;
        let mut out = Vec::new();
        for (idx, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            let ino = encode_inode(Some(self.workspace_id), (idx as u64) + 2)?;
            out.push(CoreDirEntry {
                ino,
                offset: (idx as u64) + 1,
                file_type: entry.file_type,
                name: OsString::from(entry.name),
            });
        }
        Ok(out)
    }

    async fn mknod(&self, name: &OsStr, executable: bool) -> Result<FileAttributes, JjError> {
        let child = self.child_rel(name)?;
        self.overlay
            .create_file(
                &self.backend,
                &self.base_commit_hex,
                Path::new(&child),
                executable,
                true,
            )
            .await
    }

    async fn mkdir(&self, name: &OsStr, mode: u32) -> Result<FileAttributes, JjError> {
        let child = self.child_rel(name)?;
        self.overlay
            .mkdir(
                &self.backend,
                &self.base_commit_hex,
                Path::new(&child),
                mode,
            )
            .await
    }

    async fn symlink(&self, name: &OsStr, target: &Path) -> Result<FileAttributes, JjError> {
        let child = self.child_rel(name)?;
        self.overlay
            .symlink(
                &self.backend,
                &self.base_commit_hex,
                Path::new(&child),
                target,
            )
            .await
    }

    async fn remove_child(&self, name: &OsStr, is_dir: bool) -> Result<(), JjError> {
        let child = self.child_rel(name)?;
        if is_dir {
            self.overlay
                .rmdir(&self.backend, &self.base_commit_hex, Path::new(&child))
                .await
        } else {
            self.overlay
                .unlink(&self.backend, &self.base_commit_hex, Path::new(&child))
                .await
        }
    }

    async fn getattr(&self) -> Result<FileAttributes, JjError> {
        self.overlay
            .attributes_at(&self.backend, &self.base_commit_hex, &self.rel_dir)
            .await
    }

    async fn setattr(&self, changes: AttributeChanges) -> Result<FileAttributes, JjError> {
        self.overlay
            .set_attributes(
                &self.backend,
                &self.base_commit_hex,
                Path::new(&self.rel_dir),
                changes,
            )
            .await
    }

    async fn get_digest(&self, algorithm: DigestAlgorithm) -> Result<ObjectDigest, JjError> {
        let entries = self
            .overlay
            .list_dir_at(&self.backend, &self.base_commit_hex, &self.rel_dir)
            .await?;
        digest_directory_entries(entries.iter(), algorithm)
    }
}

/// Mutable workspace file implementing `CoreFileBackend` over
/// `WorkspaceOverlay` + `ReadOnlyVfsBackend`.
///
/// Ignored files living in `overlay/ignored/` (such as compiler outputs under
/// `target/` or `out/`) return `FileDescriptor::NativeHandle` when
/// `passthrough_enabled` is `true`, and fall back to
/// `FileDescriptor::Stateless` on unprivileged per-user mounts without
/// `CAP_SYS_ADMIN`. Tracked files always return `FileDescriptor::Stateless` so
/// edits record into `journal.log`.
pub struct JjWorkspaceFileBackend {
    overlay: Arc<WorkspaceOverlay>,
    backend: Arc<dyn ReadOnlyVfsBackend>,
    base_commit_hex: String,
    rel_path: String,
}

impl JjWorkspaceFileBackend {
    pub fn new(
        overlay: Arc<WorkspaceOverlay>,
        backend: Arc<dyn ReadOnlyVfsBackend>,
        base_commit_hex: impl Into<String>,
        rel_path: impl Into<String>,
    ) -> Self {
        Self {
            overlay,
            backend,
            base_commit_hex: base_commit_hex.into(),
            rel_path: rel_path.into().trim_matches('/').to_string(),
        }
    }

    fn ignored_host_file(&self) -> Option<PathBuf> {
        let path = self.overlay.ignored_dir().join(&self.rel_path);
        path.is_file().then_some(path)
    }
}

#[async_trait]
impl CoreFileBackend for JjWorkspaceFileBackend {
    async fn open(&self, flags: i32, passthrough_enabled: bool) -> Result<OpenResult, JjError> {
        let acc = flags & libc::O_ACCMODE;
        let write_access = acc == libc::O_WRONLY || acc == libc::O_RDWR;
        let truncate = (flags & libc::O_TRUNC) != 0;
        let _ = self
            .overlay
            .open_file(
                &self.backend,
                &self.base_commit_hex,
                Path::new(&self.rel_path),
                write_access,
                truncate,
            )
            .await?;

        if passthrough_enabled && let Some(ignored_path) = self.ignored_host_file() {
            let mut opts = OpenOptions::new();
            match acc {
                libc::O_WRONLY => {
                    opts.write(true);
                }
                libc::O_RDWR => {
                    opts.read(true).write(true);
                }
                _ => {
                    opts.read(true);
                }
            }
            let file = opts.open(&ignored_path)?;
            return Ok(OpenResult {
                fd: FileDescriptor::NativeHandle(OwnedFd::from(file)),
                can_cache: true,
            });
        }

        Ok(OpenResult {
            fd: FileDescriptor::Stateless,
            can_cache: true,
        })
    }

    async fn read(&self, offset: u64, size: u32) -> Result<Box<[u8]>, JjError> {
        self.overlay
            .read_range_at(
                &self.backend,
                &self.base_commit_hex,
                &self.rel_path,
                offset,
                size,
            )
            .await
    }

    async fn write(&self, offset: u64, data: &[u8]) -> Result<u32, JjError> {
        self.overlay
            .write_at(
                &self.backend,
                &self.base_commit_hex,
                Path::new(&self.rel_path),
                offset,
                data,
            )
            .await
    }

    async fn getattr(&self) -> Result<FileAttributes, JjError> {
        self.overlay
            .attributes_at(&self.backend, &self.base_commit_hex, &self.rel_path)
            .await
    }

    async fn setattr(&self, changes: AttributeChanges) -> Result<FileAttributes, JjError> {
        self.overlay
            .set_attributes(
                &self.backend,
                &self.base_commit_hex,
                Path::new(&self.rel_path),
                changes,
            )
            .await
    }

    async fn get_digest(&self, algorithm: DigestAlgorithm) -> Result<ObjectDigest, JjError> {
        if let Some(ignored_path) = self.ignored_host_file() {
            let bytes = std::fs::read(ignored_path)?;
            return compute_content_digest(&bytes, algorithm);
        }
        if self.is_dirty() {
            let staged_path = self.overlay.staged_dir().join(&self.rel_path);
            let bytes = std::fs::read(staged_path)?;
            return compute_content_digest(&bytes, algorithm);
        }
        if algorithm == DigestAlgorithm::GitBlobSha1 {
            // Fast path: clean Commit Cloud / Git SHA-1 file already carries
            // its 20-byte (40-hex) canonical Git blob SHA-1 in tree metadata
            // with zero blob reads. Non-SHA-1 backends (e.g. jj native
            // SimpleBackend 64-byte BLAKE2b IDs) fall through and hash the file
            // bytes.
            let info = self
                .backend
                .lookup_in_commit(&self.base_commit_hex, Path::new(&self.rel_path))
                .await?;
            if info.object_id_hex.len() == 40 {
                return ObjectDigest::from_hex(algorithm, &info.object_id_hex);
            }
        }
        let attr = self.getattr().await?;
        let sz = attr.size.min(u32::MAX as u64) as u32;
        let bytes = self.read(0, sz).await?;
        compute_content_digest(&bytes, algorithm)
    }

    async fn snapshot_local_file(&self) -> Result<ObjectDigest, JjError> {
        self.overlay.fsync(Path::new(&self.rel_path), true).await?;
        self.get_digest(DigestAlgorithm::GitBlobSha1).await
    }

    fn is_dirty(&self) -> bool {
        self.overlay.is_tracked_dirty(&self.rel_path)
    }
}

/// Symbolic link node implementing `CoreSymlinkBackend` for both virtual
/// namespace links (`me`, `bookmarks/`, `users/`) and workspace symlinks.
pub enum JjSymlinkBackend {
    VirtualStatic {
        target: OsString,
        attr: FileAttributes,
    },
    Workspace {
        overlay: Arc<WorkspaceOverlay>,
        backend: Arc<dyn ReadOnlyVfsBackend>,
        base_commit_hex: String,
        rel_path: String,
    },
}

impl JjSymlinkBackend {
    pub fn virtual_static(target: impl Into<OsString>) -> Self {
        let target = target.into();
        let attr = FileAttributes::symlink(target.len() as u64, std::time::UNIX_EPOCH);
        Self::VirtualStatic { target, attr }
    }

    pub fn workspace(
        overlay: Arc<WorkspaceOverlay>,
        backend: Arc<dyn ReadOnlyVfsBackend>,
        base_commit_hex: impl Into<String>,
        rel_path: impl Into<String>,
    ) -> Self {
        Self::Workspace {
            overlay,
            backend,
            base_commit_hex: base_commit_hex.into(),
            rel_path: rel_path.into().trim_matches('/').to_string(),
        }
    }
}

#[async_trait]
impl CoreSymlinkBackend for JjSymlinkBackend {
    async fn readlink(&self) -> Result<OsString, JjError> {
        match self {
            Self::VirtualStatic { target, .. } => Ok(target.clone()),
            Self::Workspace {
                overlay,
                backend,
                base_commit_hex,
                rel_path,
            } => Ok(overlay
                .read_link_at(backend, base_commit_hex, rel_path)
                .await?
                .into_os_string()),
        }
    }

    async fn getattr(&self) -> Result<FileAttributes, JjError> {
        match self {
            Self::VirtualStatic { attr, .. } => Ok(attr.clone()),
            Self::Workspace {
                overlay,
                backend,
                base_commit_hex,
                rel_path,
            } => {
                overlay
                    .attributes_at(backend, base_commit_hex, rel_path)
                    .await
            }
        }
    }

    async fn setattr(&self, changes: AttributeChanges) -> Result<FileAttributes, JjError> {
        match self {
            Self::VirtualStatic { .. } => Err(JjError::ReadOnly),
            Self::Workspace {
                overlay,
                backend,
                base_commit_hex,
                rel_path,
            } => {
                overlay
                    .set_attributes(backend, base_commit_hex, Path::new(rel_path), changes)
                    .await
            }
        }
    }
}

fn compute_content_digest(
    data: &[u8],
    algorithm: DigestAlgorithm,
) -> Result<ObjectDigest, JjError> {
    match algorithm {
        DigestAlgorithm::GitBlobSha1 => ObjectDigest::git_blob_sha1(data),
        DigestAlgorithm::Sha256 => {
            // Pure-Rust SHA-256 via `gix` when enabled, or deterministic
            // explicit `Unsupported` if built without SHA-256 support.
            compute_sha256_digest(data)
        }
    }
}

fn compute_sha256_digest(data: &[u8]) -> Result<ObjectDigest, JjError> {
    // FIPS 180-4 SHA-256 so `jj-vfs` needs no extra crate dependency to compute
    // RBE CAS / `core_vfs` SHA-256 digests alongside Git SHA-1.
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];

    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut padded = Vec::with_capacity((data.len() + 9).div_ceil(64) * 64);
    padded.extend_from_slice(data);
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    for block in padded.as_chunks::<64>().0 {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                block[i * 4],
                block[i * 4 + 1],
                block[i * 4 + 2],
                block[i * 4 + 3],
            ]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);

            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }

    let mut out = Vec::with_capacity(32);
    for word in h {
        out.extend_from_slice(&word.to_be_bytes());
    }
    Ok(ObjectDigest {
        algorithm: DigestAlgorithm::Sha256,
        bytes: out,
    })
}

fn digest_directory_entries<'a>(
    entries: impl Iterator<Item = &'a DirectoryEntry>,
    algorithm: DigestAlgorithm,
) -> Result<ObjectDigest, JjError> {
    let mut canonical: Vec<(&str, u8)> = entries
        .map(|e| {
            let tag = match e.file_type {
                FileType::File => b'f',
                FileType::Directory => b'd',
                FileType::Symlink => b'l',
            };
            (e.name.as_str(), tag)
        })
        .collect();
    canonical.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let mut buf = Vec::new();
    for (name, tag) in canonical {
        buf.push(tag);
        buf.push(b'\t');
        buf.extend_from_slice(name.as_bytes());
        buf.push(0);
    }
    compute_content_digest(&buf, algorithm)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use jj_lib::object_id::ObjectId as _;
    use vfs_proto::WorkspaceKey;

    use super::*;
    use crate::backend::local::LocalRepoVfsBackend;
    use crate::cas::BlobCas;
    use crate::overlay::OverlayManager;
    use crate::test_helpers::setup_test_repo;

    struct CountingBackend {
        inner: Arc<dyn ReadOnlyVfsBackend>,
        list_calls: AtomicUsize,
        read_calls: AtomicUsize,
    }

    #[async_trait]
    impl ReadOnlyVfsBackend for CountingBackend {
        fn repo_name(&self) -> &str {
            self.inner.repo_name()
        }
        async fn resolve_commit(&self, id_or_prefix: &str) -> Result<String, JjError> {
            self.inner.resolve_commit(id_or_prefix).await
        }
        async fn resolve_change(&self, change_or_prefix: &str) -> Result<Option<String>, JjError> {
            self.inner.resolve_change(change_or_prefix).await
        }
        async fn list_bookmarks(&self) -> Result<BTreeMap<String, String>, JjError> {
            self.inner.list_bookmarks().await
        }
        async fn list_workspaces(
            &self,
            user_filter: Option<&str>,
        ) -> Result<Vec<crate::backend::VfsWorkspaceRef>, JjError> {
            self.inner.list_workspaces(user_filter).await
        }
        async fn list_pinned_commits(&self) -> Result<Vec<String>, JjError> {
            self.inner.list_pinned_commits().await
        }
        async fn commit_root_tree_id(&self, commit_id_hex: &str) -> Result<String, JjError> {
            self.inner.commit_root_tree_id(commit_id_hex).await
        }
        async fn lookup_in_commit(
            &self,
            commit_id_hex: &str,
            rel_path: &Path,
        ) -> Result<crate::backend::VfsLookup, JjError> {
            self.inner.lookup_in_commit(commit_id_hex, rel_path).await
        }
        async fn list_dir_in_commit(
            &self,
            commit_id_hex: &str,
            dir_path: &Path,
        ) -> Result<Vec<DirectoryEntry>, JjError> {
            self.list_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.list_dir_in_commit(commit_id_hex, dir_path).await
        }
        async fn read_file_range(
            &self,
            commit_id_hex: &str,
            rel_path: &Path,
            offset: u64,
            size: u32,
        ) -> Result<Box<[u8]>, JjError> {
            self.read_calls.fetch_add(1, Ordering::SeqCst);
            self.inner
                .read_file_range(commit_id_hex, rel_path, offset, size)
                .await
        }
        async fn read_symlink(
            &self,
            commit_id_hex: &str,
            rel_path: &Path,
        ) -> Result<PathBuf, JjError> {
            self.inner.read_symlink(commit_id_hex, rel_path).await
        }
        async fn diff_trees(
            &self,
            old_tree_hex: &str,
            new_tree_hex: &str,
        ) -> Result<Vec<String>, JjError> {
            self.inner.diff_trees(old_tree_hex, new_tree_hex).await
        }
    }

    #[test]
    fn workspace_inode_packing_round_trips_and_detects_cross_workspace_exdev() {
        assert!(WorkspaceId::new(0).is_err());
        assert!(WorkspaceId::new(MAX_WORKSPACE_ID + 1).is_err());
        let ws1 = WorkspaceId::new(1).unwrap();
        let ws2 = WorkspaceId::new(42).unwrap();

        let ro_ino = encode_inode(None, 7).unwrap();
        assert_eq!(decode_inode(ro_ino), (None, 7));

        let a = encode_inode(Some(ws1), 100).unwrap();
        let b = encode_inode(Some(ws1), LOCAL_INO_MASK).unwrap();
        let c = encode_inode(Some(ws2), 100).unwrap();
        assert_eq!(decode_inode(a), (Some(ws1), 100));
        assert_eq!(decode_inode(b), (Some(ws1), LOCAL_INO_MASK));
        assert_eq!(decode_inode(c), (Some(ws2), 100));

        assert!(is_same_workspace(a, b));
        assert!(!is_same_workspace(a, c));
        assert!(!is_same_workspace(ro_ino, ro_ino));
        assert!(encode_inode(Some(ws1), 0).is_err());
        assert!(encode_inode(Some(ws1), LOCAL_INO_MASK + 1).is_err());
    }

    #[test]
    fn sha256_matches_standard_fips_test_vectors() {
        let empty = compute_sha256_digest(b"").unwrap();
        assert_eq!(
            empty.to_hex(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        let abc = compute_sha256_digest(b"abc").unwrap();
        assert_eq!(
            abc.to_hex(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[tokio::test]
    async fn hydrating_commit_directory_defers_backend_calls_until_content_access() {
        let (ws_tmp, _repo, cid) = setup_test_repo().await;
        let state_tmp = tempfile::tempdir().unwrap();
        let cas = Arc::new(BlobCas::new(state_tmp.path().join("cas"), 1024 * 1024).unwrap());
        let local =
            Arc::new(LocalRepoVfsBackend::open_jj_workspace("repo", ws_tmp.path(), cas).unwrap());
        let counting = Arc::new(CountingBackend {
            inner: local,
            list_calls: AtomicUsize::new(0),
            read_calls: AtomicUsize::new(0),
        });
        let backend: Arc<dyn ReadOnlyVfsBackend> = counting.clone();

        let dir = HydratingCommitDirectory::new(
            Arc::clone(&backend),
            cid.hex(),
            "",
            FileAttributes::directory(std::time::UNIX_EPOCH),
        );

        // `getattr` must never hydrate.
        assert_eq!(dir.getattr().await.unwrap().file_type, FileType::Directory);
        assert_eq!(counting.list_calls.load(Ordering::SeqCst), 0);

        // First `readdir` hydrates once; subsequent `lookup`/`is_empty` reuse
        // the hydrated map.
        let entries = dir.readdir(0).await.unwrap();
        assert!(!entries.is_empty());
        assert!(!dir.is_empty().await.unwrap());
        assert_eq!(counting.list_calls.load(Ordering::SeqCst), 1);

        // Read-only commit directory rejects mutations with `EROFS` and
        // `register_file` with `ENOTSUP`.
        assert!(matches!(
            dir.mkdir(OsStr::new("new"), 0o755).await,
            Err(JjError::ReadOnly)
        ));
        assert!(matches!(
            dir.register_file(
                OsStr::new("pre"),
                RegisteredFileSpec {
                    digest: compute_sha256_digest(b"").unwrap(),
                    size: 0,
                    executable: false,
                    eager_prefetch: false,
                }
            )
            .await,
            Err(JjError::Unsupported)
        ));

        // Sticky error state on missing directory does not retry broken
        // hydrator repeatedly.
        let missing = HydratingCommitDirectory::new(
            backend,
            cid.hex(),
            "does_not_exist",
            FileAttributes::directory(std::time::UNIX_EPOCH),
        );
        assert!(missing.readdir(0).await.is_err());
        assert_eq!(counting.list_calls.load(Ordering::SeqCst), 2);
        assert!(missing.readdir(0).await.is_err());
        assert_eq!(counting.list_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn workspace_backends_bridge_clean_staged_and_passthrough_ignored_files() {
        let (ws_tmp, _repo, cid) = setup_test_repo().await;
        let state_tmp = tempfile::tempdir().unwrap();
        let cas = Arc::new(BlobCas::new(state_tmp.path().join("cas"), 1024 * 1024).unwrap());
        let local =
            Arc::new(LocalRepoVfsBackend::open_jj_workspace("repo", ws_tmp.path(), cas).unwrap());
        let counting = Arc::new(CountingBackend {
            inner: local,
            list_calls: AtomicUsize::new(0),
            read_calls: AtomicUsize::new(0),
        });
        let backend: Arc<dyn ReadOnlyVfsBackend> = counting.clone();

        let overlays = OverlayManager::new(state_tmp.path().join("overlays")).unwrap();
        let key = WorkspaceKey {
            source: "local".into(),
            repo: "repo".into(),
            user: "local".into(),
            workspace: "default".into(),
        };
        let overlay = overlays.get_or_open(&key).unwrap();
        let ws_id = WorkspaceId::new(3).unwrap();
        let cid_hex = cid.hex();

        let root_dir = JjWorkspaceDirectoryBackend::new(
            Arc::clone(&overlay),
            Arc::clone(&backend),
            &cid_hex,
            "",
            ws_id,
        );

        // 1. Add `.gitignore` ignoring `target/` and a tracked file via
        //    `CoreDirectoryBackend::mknod`.
        root_dir
            .mknod(OsStr::new(".gitignore"), false)
            .await
            .unwrap();
        let gi_file = JjWorkspaceFileBackend::new(
            Arc::clone(&overlay),
            Arc::clone(&backend),
            &cid_hex,
            ".gitignore",
        );
        gi_file.write(0, b"target/\n").await.unwrap();
        assert!(gi_file.is_dirty());
        let snap_digest = gi_file.snapshot_local_file().await.unwrap();
        assert_eq!(
            snap_digest,
            ObjectDigest::git_blob_sha1(b"target/\n").unwrap()
        );

        // 2. Clean tracked file from base commit returns `Stateless`, `is_dirty
        //    == false`, and produces a canonical 20-byte Git blob SHA-1 even on
        //    native 64-byte jj SimpleBackend repos.
        let clean_file = JjWorkspaceFileBackend::new(
            Arc::clone(&overlay),
            Arc::clone(&backend),
            &cid_hex,
            "file1.txt",
        );
        assert!(!clean_file.is_dirty());
        let open_clean = clean_file.open(libc::O_RDONLY, true).await.unwrap();
        assert!(matches!(open_clean.fd, FileDescriptor::Stateless));
        assert!(open_clean.can_cache);
        let clean_sha1 = clean_file
            .get_digest(DigestAlgorithm::GitBlobSha1)
            .await
            .unwrap();
        let clean_bytes = clean_file.read(0, 1024).await.unwrap();
        assert_eq!(clean_sha1.bytes.len(), 20);
        assert_eq!(
            clean_sha1,
            ObjectDigest::git_blob_sha1(&clean_bytes).unwrap()
        );

        // 3. Ignored build output under `target/app.o` stays off `is_dirty`,
        //    returns `Stateless` when passthrough is disabled, and returns
        //    `NativeHandle` when passthrough is enabled!
        root_dir.mkdir(OsStr::new("target"), 0o755).await.unwrap();
        let target_dir = JjWorkspaceDirectoryBackend::new(
            Arc::clone(&overlay),
            Arc::clone(&backend),
            &cid_hex,
            "target",
            ws_id,
        );
        target_dir.mknod(OsStr::new("app.o"), false).await.unwrap();
        let ignored_file = JjWorkspaceFileBackend::new(
            Arc::clone(&overlay),
            Arc::clone(&backend),
            &cid_hex,
            "target/app.o",
        );
        ignored_file.write(0, b"ELF_BYTES").await.unwrap();
        assert!(!ignored_file.is_dirty());

        let open_no_pt = ignored_file.open(libc::O_RDONLY, false).await.unwrap();
        assert!(matches!(open_no_pt.fd, FileDescriptor::Stateless));
        let open_pt = ignored_file.open(libc::O_RDONLY, true).await.unwrap();
        assert!(matches!(open_pt.fd, FileDescriptor::NativeHandle(_)));
        assert_eq!(
            ignored_file
                .get_digest(DigestAlgorithm::Sha256)
                .await
                .unwrap(),
            compute_sha256_digest(b"ELF_BYTES").unwrap()
        );

        // 4. Symlink backend covers both virtual namespace symlinks and
        //    workspace overlay symlinks.
        let vlink = JjSymlinkBackend::virtual_static("../../../repos/app/users/alice/default");
        assert_eq!(
            vlink.readlink().await.unwrap(),
            OsString::from("../../../repos/app/users/alice/default")
        );
        root_dir
            .symlink(OsStr::new("latest"), Path::new("file1.txt"))
            .await
            .unwrap();
        let ws_link = JjSymlinkBackend::workspace(
            Arc::clone(&overlay),
            Arc::clone(&backend),
            &cid_hex,
            "latest",
        );
        assert_eq!(
            ws_link.readlink().await.unwrap(),
            OsString::from("file1.txt")
        );

        // 5. Readdir entries encode `WorkspaceId` into every child inode.
        let listed = root_dir.readdir(0).await.unwrap();
        assert!(listed.iter().any(|e| e.name == OsStr::new("latest")));
        for entry in listed {
            assert_eq!(decode_inode(entry.ino).0, Some(ws_id));
        }
    }
}
