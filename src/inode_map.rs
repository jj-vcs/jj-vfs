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

//! Sharded concurrent inode-to-path table (`InodeMap`) with kernel
//! lookup-count tracking and race-free `forget` eviction backing FUSE sessions.
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
//! single `Mutex`. Read/lookup paths take a shared read lock on
//! `eviction_lock`, while `forget` eviction takes the write lock only after a
//! node's kernel `lookup_count` drops to zero and re-checks both
//! `lookup_count == 0` and `child_count == 0` under that lock, preventing any
//! concurrent `lookup` from racing with removal.
//!
//! # Cross-Process and RPC Boundaries
//! `lookup_path_inodes` resolves `(child_ino, parent_ino, leaf_name)` without
//! allocating new inodes so `fuser::Notifier` can invalidate kernel dentry and
//! page caches only for paths already known to the kernel.

use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use dashmap::DashMap;
use dashmap::Entry as DashEntry;
use fuser::INodeNo;
use parking_lot::RwLock;
use ustr::Ustr;

use crate::jj_error::JjError;

pub const ROOT_INODE: INodeNo = INodeNo(1);

#[derive(Debug)]
struct NodeMeta {
    parent: INodeNo,
    name: Ustr,
    lookup_count: AtomicU64,
    child_count: AtomicUsize,
}

impl NodeMeta {
    fn new(parent: INodeNo, name: Ustr, initial_lookups: u64) -> Self {
        Self {
            parent,
            name,
            lookup_count: AtomicU64::new(initial_lookups),
            child_count: AtomicUsize::new(0),
        }
    }
}

fn saturating_sub_u64(atom: &AtomicU64, delta: u64) -> u64 {
    let mut cur = atom.load(Ordering::Acquire);
    loop {
        let next = cur.saturating_sub(delta);
        match atom.compare_exchange_weak(cur, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(prev) => return prev,
            Err(observed) => cur = observed,
        }
    }
}

fn saturating_sub_usize(atom: &AtomicUsize, delta: usize) -> usize {
    let mut cur = atom.load(Ordering::Acquire);
    loop {
        let next = cur.saturating_sub(delta);
        match atom.compare_exchange_weak(cur, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(prev) => return prev,
            Err(observed) => cur = observed,
        }
    }
}

pub struct InodeMap {
    nodes: DashMap<INodeNo, NodeMeta>,
    children: DashMap<(INodeNo, Ustr), INodeNo>,
    next_inode: AtomicU64,
    /// Shared on hot lookup/path-resolution reads; exclusive only when evicting
    /// a node whose `lookup_count` dropped to zero, renaming, or removing an
    /// entry so `lookup_ino` and `forget` never race.
    eviction_lock: RwLock<()>,
}

impl InodeMap {
    pub fn new() -> Self {
        let nodes = DashMap::with_capacity(1024);
        nodes.insert(ROOT_INODE, NodeMeta::new(ROOT_INODE, Ustr::from(""), 1));

        Self {
            nodes,
            children: DashMap::with_capacity(1024),
            next_inode: AtomicU64::new(ROOT_INODE.0 + 1),
            eviction_lock: RwLock::new(()),
        }
    }

    #[tracing::instrument(skip(self))]
    pub fn get_path(&self, mut ino: INodeNo) -> Result<PathBuf, JjError> {
        let _read = self.eviction_lock.read();
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
        let _read = self.eviction_lock.read();
        Ok(self.nodes.get(&ino).ok_or(JjError::NotFound)?.parent)
    }

    /// Returns or allocates the inode for `(parent, name)` without incrementing
    /// the kernel lookup count (used by `readdir`, which does not increment
    /// FUSE `nlookup`).
    #[tracing::instrument(skip(self))]
    pub fn get_ino(&self, parent: INodeNo, name: &str) -> Result<INodeNo, JjError> {
        self.get_or_insert_ino(parent, name, false)
    }

    /// Returns or allocates the inode for `(parent, name)` and atomically
    /// increments its kernel lookup count by `1` (used by `lookup`, `create`,
    /// `mkdir`, `symlink`, and `readdirplus`).
    #[tracing::instrument(skip(self))]
    pub fn lookup_ino(&self, parent: INodeNo, name: &str) -> Result<INodeNo, JjError> {
        self.get_or_insert_ino(parent, name, true)
    }

    fn get_or_insert_ino(
        &self,
        parent: INodeNo,
        name: &str,
        increment_lookup: bool,
    ) -> Result<INodeNo, JjError> {
        let _read = self.eviction_lock.read();
        if !self.nodes.contains_key(&parent) {
            return Err(JjError::NotFound);
        }
        let name_ustr = Ustr::from(name);
        if let Some(existing) = self.children.get(&(parent, name_ustr)) {
            let ino = *existing;
            drop(existing);
            if increment_lookup && let Some(meta) = self.nodes.get(&ino) {
                meta.lookup_count.fetch_add(1, Ordering::AcqRel);
            }
            return Ok(ino);
        }
        match self.children.entry((parent, name_ustr)) {
            DashEntry::Occupied(occ) => {
                let ino = *occ.get();
                drop(occ);
                if increment_lookup && let Some(meta) = self.nodes.get(&ino) {
                    meta.lookup_count.fetch_add(1, Ordering::AcqRel);
                }
                Ok(ino)
            }
            DashEntry::Vacant(vac) => {
                let ino = INodeNo(self.next_inode.fetch_add(1, Ordering::Relaxed));
                let initial = u64::from(increment_lookup);
                self.nodes
                    .insert(ino, NodeMeta::new(parent, name_ustr, initial));
                if let Some(parent_meta) = self.nodes.get(&parent) {
                    parent_meta.child_count.fetch_add(1, Ordering::AcqRel);
                }
                vac.insert(ino);
                Ok(ino)
            }
        }
    }

    /// Decrements the kernel lookup count for `ino` by `nlookup` (FUSE
    /// `FORGET` / `BATCH_FORGET`) and evicts `ino` (plus any orphaned ancestor
    /// directories) once both `lookup_count` and `child_count` reach `0`.
    pub fn forget(&self, ino: INodeNo, nlookup: u64) {
        if ino == ROOT_INODE || nlookup == 0 {
            return;
        }
        let reached_zero = {
            let _read = self.eviction_lock.read();
            let Some(meta) = self.nodes.get(&ino) else {
                return;
            };
            let prev = saturating_sub_u64(&meta.lookup_count, nlookup);
            prev <= nlookup
        };
        if !reached_zero {
            return;
        }

        let _write = self.eviction_lock.write();
        self.try_evict_locked(ino);
    }

    fn try_evict_locked(&self, mut current: INodeNo) {
        while current != ROOT_INODE {
            let candidate = match self.nodes.get(&current) {
                Some(meta)
                    if meta.lookup_count.load(Ordering::Acquire) == 0
                        && meta.child_count.load(Ordering::Acquire) == 0 =>
                {
                    (meta.parent, meta.name)
                }
                _ => break,
            };
            let (parent, name) = candidate;
            let key = (parent, name);
            let mapped_here = self.children.get(&key).is_some_and(|r| *r == current);
            if mapped_here {
                self.children.remove(&key);
            }
            if self.nodes.remove(&current).is_some()
                && let Some(parent_meta) = self.nodes.get(&parent)
            {
                let _ = saturating_sub_usize(&parent_meta.child_count, 1);
            }
            current = parent;
        }
    }

    /// Returns the total number of tracked inodes currently resident in memory
    /// (including `ROOT_INODE`).
    pub fn live_inode_count(&self) -> usize {
        self.nodes.len()
    }

    /// Returns the current kernel lookup count for `ino`, if resident.
    pub fn lookup_count(&self, ino: INodeNo) -> Option<u64> {
        let _read = self.eviction_lock.read();
        self.nodes
            .get(&ino)
            .map(|m| m.lookup_count.load(Ordering::Acquire))
    }

    /// Looks up an existing child inode without allocating if absent.
    pub fn lookup_existing_ino(&self, parent: INodeNo, name: &str) -> Option<INodeNo> {
        let _read = self.eviction_lock.read();
        let name_ustr = Ustr::from(name);
        self.children.get(&(parent, name_ustr)).map(|r| *r)
    }

    /// Resolves `(child_ino, parent_ino, leaf_name)` for a mount-relative path
    /// if the kernel has previously looked up those path segments.
    pub fn lookup_path_inodes(&self, rel_path: &Path) -> Option<(Option<INodeNo>, INodeNo, Ustr)> {
        let _read = self.eviction_lock.read();
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
            let seg_ustr = Ustr::from(seg);
            parent = self.children.get(&(parent, seg_ustr)).map(|r| *r)?;
        }
        let child = self.children.get(&(parent, leaf)).map(|r| *r);
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
        let _write = self.eviction_lock.write();
        if !self.nodes.contains_key(&newparent) {
            return Err(JjError::NotFound);
        }
        let old_key = (parent, Ustr::from(name));
        let new_name_ustr = Ustr::from(newname);
        let new_key = (newparent, new_name_ustr);

        // Detach displaced destination entry from the directory namespace; keep
        // its `NodeMeta` alive while the kernel still holds a lookup count.
        if let Some((_, displaced)) = self.children.remove(&new_key) {
            self.try_evict_locked(displaced);
        }

        if let Some((_, moved_ino)) = self.children.remove(&old_key) {
            if let Some(mut meta) = self.nodes.get_mut(&moved_ino) {
                let prev_parent = meta.parent;
                meta.parent = newparent;
                meta.name = new_name_ustr;
                drop(meta);
                if prev_parent != newparent {
                    if let Some(old_p) = self.nodes.get(&prev_parent) {
                        let _ = saturating_sub_usize(&old_p.child_count, 1);
                    }
                    if let Some(new_p) = self.nodes.get(&newparent) {
                        new_p.child_count.fetch_add(1, Ordering::AcqRel);
                    }
                }
            }
            self.children.insert(new_key, moved_ino);
        }
        Ok(())
    }

    /// Detaches a deleted directory entry from `(parent, name)` lookup tables,
    /// evicting `NodeMeta` immediately when no kernel lookup references remain
    /// (and deferring eviction to `forget` if still referenced by open
    /// handles).
    pub fn remove_entry(&self, parent: INodeNo, name: &str) -> Option<INodeNo> {
        let _write = self.eviction_lock.write();
        let key = (parent, Ustr::from(name));
        let (_, ino) = self.children.remove(&key)?;
        self.try_evict_locked(ino);
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
                let dir_ino = map.lookup_ino(ROOT_INODE, &dir_name).unwrap();
                let file_name = format!("mod_{t}.rs");
                let file_ino = map.lookup_ino(dir_ino, &file_name).unwrap();
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
    fn forget_evicts_zero_refcount_leaves_cascades_parents_and_preserves_open_unlinked() {
        let inodes = InodeMap::new();
        let dir_ino = inodes.lookup_ino(ROOT_INODE, "src").unwrap();
        let file_ino = inodes.lookup_ino(dir_ino, "lib.rs").unwrap();
        assert_eq!(inodes.lookup_ino(dir_ino, "lib.rs").unwrap(), file_ino);
        assert_eq!(inodes.lookup_count(file_ino), Some(2));
        assert_eq!(inodes.live_inode_count(), 3);

        // Forgetting parent while child is still live retains parent for child
        // path resolution.
        inodes.forget(dir_ino, 1);
        assert_eq!(inodes.lookup_count(dir_ino), Some(0));
        assert_eq!(
            inodes.get_path(file_ino).unwrap(),
            PathBuf::from("src/lib.rs")
        );

        // Partial forget leaves file resident.
        inodes.forget(file_ino, 1);
        assert_eq!(inodes.lookup_count(file_ino), Some(1));
        assert_eq!(inodes.live_inode_count(), 3);

        // Final forget evicts `file_ino` and cascades upward to `dir_ino`
        // (while never evicting `ROOT_INODE`).
        inodes.forget(file_ino, 1);
        assert_eq!(inodes.live_inode_count(), 1);
        assert!(inodes.lookup_existing_ino(ROOT_INODE, "src").is_none());
        inodes.forget(ROOT_INODE, 100);
        assert_eq!(inodes.live_inode_count(), 1);

        // Unlinking an inode that still has a kernel lookup count detaches its
        // dentry but keeps `get_path` valid until `forget` releases it.
        let open_ino = inodes.lookup_ino(ROOT_INODE, "open.tmp").unwrap();
        assert_eq!(inodes.remove_entry(ROOT_INODE, "open.tmp"), Some(open_ino));
        assert!(inodes.lookup_existing_ino(ROOT_INODE, "open.tmp").is_none());
        assert_eq!(
            inodes.get_path(open_ino).unwrap(),
            PathBuf::from("open.tmp")
        );
        inodes.forget(open_ino, 1);
        assert_eq!(inodes.live_inode_count(), 1);
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
