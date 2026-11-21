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

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use fuser::*;
use futures::StreamExt as _;
use futures::stream;

use crate::inode_map::InodeMap;
use crate::jj_error::JjError;
use crate::vfs::VirtualFilesystem;
use crate::virtual_file::AttributeChanges;
use crate::virtual_file::FileAttributes;

const TTL: Duration = Duration::from_secs(60);

macro_rules! reply_async {
    (
        $self:ident,
        $reply:ident,
        $ino:expr,
        $path:ident => $async_body:expr,
        $value:pat => $success_body:expr
    ) => {
        let inode_map = $self.inode_map.clone();
        let rt_handle = $self.rt_handle.clone();
        let ino = $ino;
        rt_handle.spawn(async move {
            match inode_map.get_path(ino) {
                Ok(mut path_buf) => {
                    let $path = &mut path_buf;
                    let res: Result<_, JjError> = $async_body.await;
                    match res {
                        Ok($value) => {
                            $success_body
                        }
                        Err(err) => {
                            tracing::debug!(path = %format_args!("./{}", path_buf.display()), error = %err, "FUSE operation failed");
                            $reply.error(err.into());
                        }
                    }
                }
                Err(err) => {
                    tracing::error!(ino = %ino, error = %err, "Failed to resolve path for inode");
                    $reply.error(err.into());
                }
            }
        });
    };
}

pub struct JjFuse<FS: VirtualFilesystem> {
    fs: Arc<FS>,
    inode_map: Arc<InodeMap>,
    rt_handle: tokio::runtime::Handle,
}

impl<FS: VirtualFilesystem> JjFuse<FS> {
    pub fn new(fs: Arc<FS>, rt_handle: tokio::runtime::Handle) -> Self {
        Self::with_inode_map(fs, Arc::new(InodeMap::new()), rt_handle)
    }

    pub fn with_inode_map(
        fs: Arc<FS>,
        inode_map: Arc<InodeMap>,
        rt_handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            fs,
            inode_map,
            rt_handle,
        }
    }

    pub fn inode_map(&self) -> &Arc<InodeMap> {
        &self.inode_map
    }
}

impl<FS: VirtualFilesystem + Send + Sync + 'static> Filesystem for JjFuse<FS> {
    #[tracing::instrument(level = "debug", skip_all)]
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        let _ = config.add_capabilities(
            fuser::InitFlags::FUSE_PARALLEL_DIROPS | fuser::InitFlags::FUSE_ASYNC_READ,
        );
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, _req, reply))]
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let fs = self.fs.clone();
        let inode_map = self.inode_map.clone();
        let name = name.to_os_string();
        reply_async!(
            self,
            reply,
            parent,
            parent_path => async move {
                let name_str = name.to_str().ok_or(JjError::InvalidPath)?;
                let child_ino = inode_map.get_ino(parent, name_str)?;
                parent_path.push(name_str);
                let attr = fs.get_attributes(parent_path).await?;
                Ok(attr.to_fuse(child_ino))
            },
            attr => reply.entry(&TTL, &attr, fuser::Generation(0))
        );
    }

    #[tracing::instrument(level = "debug", skip(self, _req, reply))]
    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let fs = self.fs.clone();
        reply_async!(
            self,
            reply,
            ino,
            path => async { Ok(fs.get_attributes(path).await?.to_fuse(ino)) },
            attr => reply.attr(&TTL, &attr)
        );
    }

    #[tracing::instrument(level = "debug", skip(self, _req, reply))]
    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let fs = self.fs.clone();
        reply_async!(
            self,
            reply,
            ino,
            path => async { fs.read(path, offset, size).await },
            content => reply.data(&content)
        );
    }

    #[tracing::instrument(level = "debug", skip(self, _req, reply))]
    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let fs = self.fs.clone();
        let inode_map = self.inode_map.clone();
        reply_async!(
            self,
            reply,
            ino,
            path => async {
                if offset < 1 && reply.add(ino, 1, FileType::Directory, ".") {
                    return Ok(());
                }

                if offset < 2 {
                    let parent_ino = inode_map.get_parent_ino(ino)?;
                    if reply.add(parent_ino, 2, FileType::Directory, "..") {
                        return Ok(());
                    }
                }

                let skip = offset.saturating_sub(2) as usize;
                let entries = fs.read_directory(path).await?;
                let mut stream = stream::iter(3..).zip(entries).skip(skip);
                while let Some((index, entry)) = stream.next().await {
                    let name = entry.name.as_str();
                    let file_type = entry.file_type.into();
                    let child_ino = inode_map.get_ino(ino, name)?;
                    if reply.add(child_ino, index, file_type, name) {
                        break;
                    }
                }
                Ok(())
            },
            () => reply.ok()
        );
    }

    #[tracing::instrument(level = "debug", skip(self, _req, reply))]
    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let fs = self.fs.clone();
        reply_async!(
            self,
            reply,
            ino,
            path => async move {
                let target = fs.read_link(path).await?;
                let bytes = target.as_os_str().as_bytes().to_vec();
                Ok(bytes)
            },
            target => reply.data(&target)
        );
    }

    #[tracing::instrument(level = "debug", skip(self, _req, reply))]
    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let fs = self.fs.clone();
        let acc = flags.0 & libc::O_ACCMODE;
        let write_access = acc == libc::O_WRONLY || acc == libc::O_RDWR;
        let truncate = (flags.0 & libc::O_TRUNC) != 0;
        reply_async!(
            self,
            reply,
            ino,
            path => async move {
                fs.open_file(path, write_access, truncate).await?;
                Ok(())
            },
            () => reply.opened(FileHandle(0), FopenFlags::empty())
        );
    }

    #[tracing::instrument(level = "debug", skip(self, _req, reply))]
    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let fs = self.fs.clone();
        let inode_map = self.inode_map.clone();
        let name = name.to_os_string();
        let executable = ((mode & !umask) & 0o111) != 0;
        let exclusive = (flags & libc::O_EXCL) != 0;
        reply_async!(
            self,
            reply,
            parent,
            parent_path => async move {
                let name_str = name.to_str().ok_or(JjError::InvalidPath)?;
                parent_path.push(name_str);
                let attr = fs.create_file(parent_path, executable, exclusive).await?;
                let child_ino = inode_map.get_ino(parent, name_str)?;
                Ok(attr.to_fuse(child_ino))
            },
            attr => {
                reply.created(&TTL, &attr, fuser::Generation(0), FileHandle(0), FopenFlags::empty())
            }
        );
    }

    #[tracing::instrument(level = "debug", skip(self, _req, data, reply))]
    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let fs = self.fs.clone();
        let owned = data.to_vec();
        reply_async!(
            self,
            reply,
            ino,
            path => async move { fs.write(path, offset, &owned).await },
            written => reply.written(written)
        );
    }

    #[tracing::instrument(level = "debug", skip(self, _req, reply))]
    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    #[tracing::instrument(level = "debug", skip(self, _req, reply))]
    fn fsync(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        let fs = self.fs.clone();
        reply_async!(
            self,
            reply,
            ino,
            path => async move { fs.fsync(path, datasync).await },
            () => reply.ok()
        );
    }

    #[tracing::instrument(level = "debug", skip(self, _req, reply))]
    fn fsyncdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        let fs = self.fs.clone();
        reply_async!(
            self,
            reply,
            ino,
            path => async move { fs.fsync(path, datasync).await },
            () => reply.ok()
        );
    }

    #[tracing::instrument(level = "debug", skip(self, _req, reply))]
    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let fs = self.fs.clone();
        let changes = AttributeChanges {
            mode,
            size,
            atime: atime.map(time_or_now_to_system_time),
            mtime: mtime.map(time_or_now_to_system_time),
        };
        reply_async!(
            self,
            reply,
            ino,
            path => async move {
                let attr = fs.set_attributes(path, changes).await?;
                Ok(attr.to_fuse(ino))
            },
            attr => reply.attr(&TTL, &attr)
        );
    }

    #[tracing::instrument(level = "debug", skip(self, _req, reply))]
    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let fs = self.fs.clone();
        let inode_map = self.inode_map.clone();
        let name = name.to_os_string();
        reply_async!(
            self,
            reply,
            parent,
            parent_path => async move {
                let name_str = name.to_str().ok_or(JjError::InvalidPath)?;
                parent_path.push(name_str);
                fs.unlink(parent_path).await?;
                inode_map.remove_entry(parent, name_str);
                Ok(())
            },
            () => reply.ok()
        );
    }

    #[tracing::instrument(level = "debug", skip(self, _req, reply))]
    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        let fs = self.fs.clone();
        let inode_map = self.inode_map.clone();
        let name = name.to_os_string();
        let effective_mode = mode & !umask;
        reply_async!(
            self,
            reply,
            parent,
            parent_path => async move {
                let name_str = name.to_str().ok_or(JjError::InvalidPath)?;
                parent_path.push(name_str);
                let attr = fs.mkdir(parent_path, effective_mode).await?;
                let child_ino = inode_map.get_ino(parent, name_str)?;
                Ok(attr.to_fuse(child_ino))
            },
            attr => reply.entry(&TTL, &attr, fuser::Generation(0))
        );
    }

    #[tracing::instrument(level = "debug", skip(self, _req, reply))]
    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let fs = self.fs.clone();
        let inode_map = self.inode_map.clone();
        let name = name.to_os_string();
        reply_async!(
            self,
            reply,
            parent,
            parent_path => async move {
                let name_str = name.to_str().ok_or(JjError::InvalidPath)?;
                parent_path.push(name_str);
                fs.rmdir(parent_path).await?;
                inode_map.remove_entry(parent, name_str);
                Ok(())
            },
            () => reply.ok()
        );
    }

    #[tracing::instrument(level = "debug", skip(self, _req, reply))]
    fn symlink(
        &self,
        _req: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        let fs = self.fs.clone();
        let inode_map = self.inode_map.clone();
        let name = link_name.to_os_string();
        let owned_target = target.to_path_buf();
        reply_async!(
            self,
            reply,
            parent,
            parent_path => async move {
                let name_str = name.to_str().ok_or(JjError::InvalidPath)?;
                parent_path.push(name_str);
                let attr = fs.symlink(parent_path, &owned_target).await?;
                let child_ino = inode_map.get_ino(parent, name_str)?;
                Ok(attr.to_fuse(child_ino))
            },
            attr => reply.entry(&TTL, &attr, fuser::Generation(0))
        );
    }

    #[tracing::instrument(level = "debug", skip(self, _req, reply))]
    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        if !flags.is_empty() {
            reply.error(Errno::EINVAL);
            return;
        }
        let fs = self.fs.clone();
        let inode_map = self.inode_map.clone();
        let old_name = name.to_os_string();
        let new_name = newname.to_os_string();
        let rt_handle = self.rt_handle.clone();
        rt_handle.spawn(async move {
            let res: Result<(), JjError> = async {
                let old_str = old_name.to_str().ok_or(JjError::InvalidPath)?;
                let new_str = new_name.to_str().ok_or(JjError::InvalidPath)?;
                let mut old_path = inode_map.get_path(parent)?;
                old_path.push(old_str);
                let mut new_path = inode_map.get_path(newparent)?;
                new_path.push(new_str);
                fs.rename(&old_path, &new_path).await?;
                inode_map.rename_entry(parent, old_str, newparent, new_str)?;
                Ok(())
            }
            .await;
            match res {
                Ok(()) => reply.ok(),
                Err(err) => reply.error(err.into()),
            }
        });
    }
}

impl FileAttributes {
    pub fn to_fuse(&self, ino: INodeNo) -> FileAttr {
        let caller = vfs_proto::CallerContext::current_process();
        FileAttr {
            ino,
            size: self.size,
            blocks: self.size.div_ceil(512),
            atime: SystemTime::now(),
            mtime: self.modified,
            ctime: self.modified,
            crtime: self.created,
            kind: self.file_type.into(),
            perm: match self.file_type {
                crate::virtual_file::FileType::Directory => 0o755,
                crate::virtual_file::FileType::Symlink => 0o777,
                crate::virtual_file::FileType::File => {
                    if self.executable {
                        0o755
                    } else {
                        0o644
                    }
                }
            },
            nlink: match self.file_type {
                crate::virtual_file::FileType::Directory => 2,
                _ => 1,
            },
            uid: caller.uid,
            gid: caller.gid,
            rdev: 0,
            flags: 0,
            blksize: 4096,
        }
    }
}

impl From<crate::virtual_file::FileType> for FileType {
    fn from(file_type: crate::virtual_file::FileType) -> Self {
        match file_type {
            crate::virtual_file::FileType::File => FileType::RegularFile,
            crate::virtual_file::FileType::Directory => FileType::Directory,
            crate::virtual_file::FileType::Symlink => FileType::Symlink,
        }
    }
}

fn time_or_now_to_system_time(t: TimeOrNow) -> SystemTime {
    match t {
        TimeOrNow::SpecificTime(st) => st,
        TimeOrNow::Now => SystemTime::now(),
    }
}

impl From<JjError> for Errno {
    fn from(value: JjError) -> Self {
        match value {
            JjError::NotFound => Errno::ENOENT,
            JjError::NotADirectory => Errno::ENOTDIR,
            JjError::NotAFile => Errno::EISDIR,
            JjError::NotASymlink | JjError::InvalidPath => Errno::EINVAL,
            JjError::ReadOnly => Errno::EROFS,
            JjError::PermissionDenied(_) => Errno::EACCES,
            JjError::CrossDevice => Errno::EXDEV,
            JjError::DirectoryNotEmpty => Errno::ENOTEMPTY,
            JjError::AlreadyExists => Errno::EEXIST,
            JjError::Busy => Errno::EBUSY,
            JjError::IO(e) => e
                .raw_os_error()
                .filter(|&code| code > 0)
                .map(Errno::from_i32)
                .unwrap_or_else(|| match e.kind() {
                    std::io::ErrorKind::NotFound => Errno::ENOENT,
                    std::io::ErrorKind::AlreadyExists => Errno::EEXIST,
                    std::io::ErrorKind::PermissionDenied => Errno::EACCES,
                    std::io::ErrorKind::DirectoryNotEmpty => Errno::ENOTEMPTY,
                    std::io::ErrorKind::IsADirectory => Errno::EISDIR,
                    std::io::ErrorKind::NotADirectory => Errno::ENOTDIR,
                    std::io::ErrorKind::InvalidInput => Errno::EINVAL,
                    std::io::ErrorKind::StorageFull => Errno::ENOSPC,
                    _ => Errno::EIO,
                }),
            _ => Errno::EIO,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_errors_preserve_host_errno_instead_of_collapsing_to_eio() {
        for raw in [
            libc::ENOSPC,
            libc::EEXIST,
            libc::ENAMETOOLONG,
            libc::ENOTEMPTY,
            libc::EACCES,
        ] {
            let err = JjError::IO(std::io::Error::from_raw_os_error(raw));
            assert_eq!(Errno::from(err), Errno::from_i32(raw));
        }
        let custom = JjError::IO(std::io::Error::other("synthetic failure"));
        assert_eq!(Errno::from(custom), Errno::EIO);
    }
}
