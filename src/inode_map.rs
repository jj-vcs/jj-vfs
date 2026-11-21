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

//! Sharded concurrent inode-to-path table (`InodeMap`) backing FUSE sessions.
//!
//! # Authority and Crash-Safety Ordering
//! Inode numbers are ephemeral per `JjFuse` session (`ROOT_INODE = 1`,
//! monotonically allocated via `AtomicU64`). Persistent identity lives in
//! repository object IDs and durable `WorkspaceOverlay` journals rather than
//! FUSE inode numbers.
//!
//! # Concurrency and Merge Invariants
//! Forward `(parent_ino, child_name) -> child_ino` and reverse `ino ->
//! NodeMeta` mappings live in sharded `DashMap` tables so 64+ concurrent
//! compiler/agent threads traverse disjoint directories without contending on a
//! single `Mutex`.
//!
//! # Cross-Process and RPC Boundaries
//! `lookup_path_inodes` resolves `(child_ino, parent_ino, leaf_name)` without
//! allocating new inodes so `fuser::Notifier` can invalidate kernel dentry and
//! page caches only for paths already known to the kernel.

use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use dashmap::DashMap;
use dashmap::Entry as DashEntry;
use fuser::INodeNo;
use ustr::Ustr;

use crate::jj_error::JjError;

pub const ROOT_INODE: INodeNo = INodeNo(1);

#[derive(Clone, Copy, Debug)]
struct NodeMeta {
    parent: INodeNo,
    name: Ustr,
}

pub struct InodeMap {
    nodes: DashMap<INodeNo, NodeMeta>,
    children: DashMap<(INodeNo, Ustr), INodeNo>,
    next_inode: AtomicU64,
}

impl InodeMap {
    pub fn new() -> Self {
        let nodes = DashMap::with_capacity(1024);
        nodes.insert(
            ROOT_INODE,
            NodeMeta {
                parent: ROOT_INODE,
                name: Ustr::from(""),
            },
        );

        Self {
            nodes,
            children: DashMap::with_capacity(1024),
            next_inode: AtomicU64::new(ROOT_INODE.0 + 1),
        }
    }

    #[tracing::instrument(skip(self))]
    pub fn get_path(&self, mut ino: INodeNo) -> Result<PathBuf, JjError> {
        let mut components = Vec::with_capacity(8);
        while ino != ROOT_INODE {
            let entry = self.nodes.get(&ino).ok_or(JjError::NotFound)?;
            components.push(entry.name);
            ino = entry.parent;
        }
        let total_len: usize = components.iter().map(|c| c.len()).sum::<usize>() + components.len();
        let mut path = PathBuf::with_capacity(total_len);
        path.extend(components.iter().rev());
        Ok(path)
    }

    #[tracing::instrument(skip(self))]
    pub fn get_parent_ino(&self, ino: INodeNo) -> Result<INodeNo, JjError> {
        Ok(self.nodes.get(&ino).ok_or(JjError::NotFound)?.parent)
    }

    #[tracing::instrument(skip(self))]
    pub fn get_ino(&self, parent: INodeNo, name: &str) -> Result<INodeNo, JjError> {
        if !self.nodes.contains_key(&parent) {
            return Err(JjError::NotFound);
        }
        let name_ustr = Ustr::from(name);
        if let Some(existing) = self.children.get(&(parent, name_ustr)) {
            return Ok(*existing);
        }
        match self.children.entry((parent, name_ustr)) {
            DashEntry::Occupied(occ) => Ok(*occ.get()),
            DashEntry::Vacant(vac) => {
                let ino = INodeNo(self.next_inode.fetch_add(1, Ordering::Relaxed));
                self.nodes.insert(
                    ino,
                    NodeMeta {
                        parent,
                        name: name_ustr,
                    },
                );
                vac.insert(ino);
                Ok(ino)
            }
        }
    }

    /// Looks up an existing child inode without allocating if absent.
    pub fn lookup_existing_ino(&self, parent: INodeNo, name: &str) -> Option<INodeNo> {
        let name_ustr = Ustr::from(name);
        self.children.get(&(parent, name_ustr)).map(|r| *r)
    }

    /// Resolves `(child_ino, parent_ino, leaf_name)` for a mount-relative path
    /// if the kernel has previously looked up those path segments.
    pub fn lookup_path_inodes(&self, rel_path: &Path) -> Option<(Option<INodeNo>, INodeNo, Ustr)> {
        let mut segments: Vec<&str> = Vec::new();
        for comp in rel_path.iter() {
            let s = comp.to_str()?;
            if !s.is_empty() && s != "." {
                segments.push(s);
            }
        }
        let leaf = Ustr::from(segments.last()?);
        let mut parent = ROOT_INODE;
        for seg in &segments[..segments.len() - 1] {
            parent = self.lookup_existing_ino(parent, seg)?;
        }
        let child = self.lookup_existing_ino(parent, leaf.as_str());
        Some((child, parent, leaf))
    }

    /// Updates inode parent/name mappings on in-mount `rename`.
    pub fn rename_entry(
        &self,
        parent: INodeNo,
        name: &str,
        newparent: INodeNo,
        newname: &str,
    ) -> Result<(), JjError> {
        if !self.nodes.contains_key(&newparent) {
            return Err(JjError::NotFound);
        }
        let old_key = (parent, Ustr::from(name));
        let new_name_ustr = Ustr::from(newname);
        let new_key = (newparent, new_name_ustr);

        // If destination already had an inode, detach its child mapping
        if let Some((_, displaced)) = self.children.remove(&new_key) {
            self.nodes.remove(&displaced);
        }

        if let Some((_, moved_ino)) = self.children.remove(&old_key) {
            self.nodes.insert(
                moved_ino,
                NodeMeta {
                    parent: newparent,
                    name: new_name_ustr,
                },
            );
            self.children.insert(new_key, moved_ino);
        }
        Ok(())
    }

    /// Removes a deleted directory entry from `(parent, name)` lookup tables.
    pub fn remove_entry(&self, parent: INodeNo, name: &str) -> Option<INodeNo> {
        let key = (parent, Ustr::from(name));
        let (_, ino) = self.children.remove(&key)?;
        self.nodes.remove(&ino);
        Some(ino)
    }
}

impl Default for InodeMap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_get_path() {
        let inodes = InodeMap::new();
        assert_eq!(inodes.get_path(ROOT_INODE).unwrap(), PathBuf::from(""));

        let foo_ino = inodes.get_ino(ROOT_INODE, "foo").unwrap();
        let bar_ino = inodes.get_ino(foo_ino, "bar.txt").unwrap();

        assert_eq!(inodes.get_path(foo_ino).unwrap(), PathBuf::from("foo"));
        assert_eq!(
            inodes.get_path(bar_ino).unwrap(),
            PathBuf::from("foo/bar.txt")
        );
    }

    #[test]
    fn test_get_ino() {
        let inodes = InodeMap::new();

        let foo_ino = inodes.get_ino(ROOT_INODE, "foo").unwrap();
        assert_eq!(foo_ino, INodeNo(2));
        assert_eq!(inodes.get_path(foo_ino).unwrap(), PathBuf::from("foo"));

        let foo_ino_again = inodes.get_ino(ROOT_INODE, "foo").unwrap();
        assert_eq!(foo_ino_again, INodeNo(2));

        let bar_ino = inodes.get_ino(foo_ino, "bar.txt").unwrap();
        assert_eq!(bar_ino, INodeNo(3));
        assert_eq!(
            inodes.get_path(bar_ino).unwrap(),
            PathBuf::from("foo/bar.txt")
        );

        let err = inodes.get_ino(INodeNo(999), "bar.txt");
        assert!(matches!(err, Err(JjError::NotFound)));
    }

    #[test]
    fn test_concurrent_sharded_allocations_are_unique_and_consistent() {
        let inodes = Arc::new(InodeMap::new());
        let mut handles = Vec::new();
        for t in 0..16 {
            let map = Arc::clone(&inodes);
            handles.push(std::thread::spawn(move || {
                let dir_name = format!("pkg_{}", t % 4);
                let dir_ino = map.get_ino(ROOT_INODE, &dir_name).unwrap();
                let file_name = format!("mod_{t}.rs");
                let file_ino = map.get_ino(dir_ino, &file_name).unwrap();
                assert_eq!(
                    map.get_path(file_ino).unwrap(),
                    PathBuf::from(format!("{dir_name}/{file_name}"))
                );
                file_ino
            }));
        }
        let mut allocated = std::collections::HashSet::new();
        for h in handles {
            assert!(allocated.insert(h.join().unwrap()));
        }
        assert_eq!(allocated.len(), 16);
    }

    #[test]
    fn test_inode_map_is_sync() {
        fn assert_sync<T: Sync>() {}
        assert_sync::<InodeMap>();
    }

    #[test]
    fn test_inode_map_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<InodeMap>();
    }
}
