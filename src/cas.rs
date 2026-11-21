// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Content-addressed local disk cache (`BlobCas`) for clean immutable file
//! blobs.
//!
//! # Authority and Crash-Safety Ordering
//! `BlobCas` stores *only* clean immutable repository blobs
//! (`<cache_dir>/cas/<hh>/<sha256>`), writing and `fsync`ing a PID-and-counter
//! unique `.tmp` file before renaming into place so concurrent readers never
//! observe torn files. Workspace dirty edits live in `$XDG_STATE_HOME/jj-vfs/
//! overlays/` outside `BlobCas` and are never subject to eviction.
//!
//! # Concurrency and Merge Invariants
//! Ranged reads run `FileExt::read_at` (`pread`) outside any lock, allowing
//! arbitrary random-access offsets without re-inflating packfiles or discarding
//! prefix bytes. A single mutex guards the LRU recency index (`LruMap`); each
//! read or stat holds it only for an $O(\log n)$ touch, and LRU entries are
//! keyed by on-disk file name so the startup scan and runtime lookups agree.

use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs::File;
use std::fs::{self};
use std::hash::Hash;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use parking_lot::Mutex;

/// Least-recently-used map with weighted entries: a hash map from key to its
/// value, generation, and weight, plus a `BTreeMap` from generation to key. The
/// smallest generation is the least recently used entry, so get, insert, and
/// eviction each cost $O(\log n)$. `BlobCas` uses it with `V = ()` as the
/// recency index of on-disk blobs.
#[derive(Debug)]
pub(crate) struct LruMap<K, V> {
    entries: HashMap<K, LruEntry<V>>,
    order: BTreeMap<u64, K>,
    next_generation: u64,
    total_weight: u64,
}

#[derive(Debug)]
struct LruEntry<V> {
    value: V,
    generation: u64,
    weight: u64,
}

impl<K: Clone + Eq + Hash, V> LruMap<K, V> {
    pub(crate) fn new() -> Self {
        Self {
            entries: HashMap::new(),
            order: BTreeMap::new(),
            next_generation: 0,
            total_weight: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn total_weight(&self) -> u64 {
        self.total_weight
    }

    /// Returns the value for `key` and marks it as most recently used.
    pub(crate) fn get<Q>(&mut self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let generation = self.next_generation;
        let entry = self.entries.get_mut(key)?;
        self.next_generation += 1;
        if let Some(owned) = self.order.remove(&entry.generation) {
            self.order.insert(generation, owned);
        }
        entry.generation = generation;
        Some(&entry.value)
    }

    /// Marks `key` as most recently used. Returns `false` if `key` is not
    /// present.
    pub(crate) fn touch<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.get(key).is_some()
    }

    /// Inserts `key` as most recently used, replacing any previous value and
    /// weight.
    pub(crate) fn insert(&mut self, key: K, value: V, weight: u64) {
        let generation = self.next_generation;
        self.next_generation += 1;
        let entry = LruEntry {
            value,
            generation,
            weight,
        };
        if let Some(old) = self.entries.insert(key.clone(), entry) {
            self.order.remove(&old.generation);
            self.total_weight = self.total_weight.saturating_sub(old.weight);
        }
        self.order.insert(generation, key);
        self.total_weight = self.total_weight.saturating_add(weight);
    }

    /// Removes and returns the least recently used entry with its weight.
    pub(crate) fn pop_lru(&mut self) -> Option<(K, V, u64)> {
        let (_, key) = self.order.pop_first()?;
        let entry = self.entries.remove(&key)?;
        self.total_weight = self.total_weight.saturating_sub(entry.weight);
        Some((key, entry.value, entry.weight))
    }

    /// Iterates over keys from least to most recently used.
    #[allow(dead_code)]
    pub(crate) fn keys(&self) -> impl Iterator<Item = &K> {
        self.order.values()
    }
}

pub struct BlobCas {
    root: PathBuf,
    max_bytes: AtomicU64,
    tmp_seq: AtomicU64,
    lru: Mutex<LruMap<Arc<str>, ()>>,
}

impl BlobCas {
    pub fn new(cache_root: impl Into<PathBuf>, max_bytes: u64) -> io::Result<Self> {
        let root = cache_root.into().join("cas");
        fs::create_dir_all(&root)?;
        let cas = Self {
            root,
            max_bytes: AtomicU64::new(max_bytes),
            tmp_seq: AtomicU64::new(1),
            lru: Mutex::new(LruMap::new()),
        };
        cas.scan_existing()?;
        Ok(cas)
    }

    pub fn set_max_bytes(&self, max_bytes: u64) {
        self.max_bytes.store(max_bytes, Ordering::Relaxed);
        let _ = self.evict_if_needed();
    }

    pub fn max_bytes(&self) -> u64 {
        self.max_bytes.load(Ordering::Relaxed)
    }

    pub fn stats(&self) -> (usize, u64) {
        let guard = self.lru.lock();
        (guard.len(), guard.total_weight())
    }

    /// Maps a logical key to its SHA-256 hex digest, which serves as both its
    /// sharded `<hh>/<sha256>` on-disk file name and its LRU key so conflict
    /// paths never collide.
    fn blob_name(key: &str) -> String {
        sha256_hex(key.as_bytes())
    }

    fn name_path(&self, name: &str) -> PathBuf {
        let prefix = if name.len() >= 2 { &name[..2] } else { "00" };
        self.root.join(prefix).join(name)
    }

    fn blob_path(&self, key: &str) -> PathBuf {
        self.name_path(&Self::blob_name(key))
    }

    pub fn has_blob(&self, key: &str) -> bool {
        self.blob_path(key).is_file()
    }

    pub fn blob_size(&self, key: &str) -> Option<u64> {
        let name = Self::blob_name(key);
        let meta = fs::metadata(self.name_path(&name)).ok()?;
        let len = meta.len();
        self.touch_lru(&name, len);
        Some(len)
    }

    /// Reads `[offset, offset + size)` directly via `pread` (`read_at`) if
    /// cached.
    pub fn read_range(&self, key: &str, offset: u64, size: u32) -> io::Result<Option<Box<[u8]>>> {
        let name = Self::blob_name(key);
        let file = match File::open(self.name_path(&name)) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let total_len = file.metadata()?.len();
        self.touch_lru(&name, total_len);
        if offset >= total_len || size == 0 {
            return Ok(Some(Box::new([])));
        }
        let to_read = (total_len - offset).min(size as u64) as usize;
        let mut buf = vec![0u8; to_read];
        read_exact_at(&file, &mut buf, offset)?;
        Ok(Some(buf.into_boxed_slice()))
    }

    /// Persists a clean blob into the CAS via `fsync` + rename and enforces the
    /// byte budget.
    pub fn put_blob(&self, key: &str, data: &[u8]) -> io::Result<u64> {
        let name = Self::blob_name(key);
        let path = self.name_path(&name);
        if let Ok(meta) = fs::metadata(&path) {
            let len = meta.len();
            self.touch_lru(&name, len);
            return Ok(len);
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let seq = self.tmp_seq.fetch_add(1, Ordering::Relaxed);
        let tmp_path = path.with_extension(format!("tmp.{}.{seq}", std::process::id()));
        if let Err(err) =
            write_and_sync(&tmp_path, data).and_then(|()| fs::rename(&tmp_path, &path))
        {
            let _ = fs::remove_file(&tmp_path);
            return Err(err);
        }

        let size = data.len() as u64;
        self.lru.lock().insert(Arc::from(name), (), size);
        self.evict_if_needed()?;
        Ok(size)
    }

    fn touch_lru(&self, name: &str, size: u64) {
        let mut guard = self.lru.lock();
        if !guard.touch(name) {
            guard.insert(Arc::from(name), (), size);
        }
    }

    pub fn evict_if_needed(&self) -> io::Result<usize> {
        let limit = self.max_bytes.load(Ordering::Relaxed);
        if limit == 0 {
            return Ok(0);
        }
        let mut to_delete: Vec<Arc<str>> = Vec::new();
        {
            let mut guard = self.lru.lock();
            while guard.total_weight() > limit && guard.len() > 1 {
                if let Some((victim, (), _)) = guard.pop_lru() {
                    to_delete.push(victim);
                }
            }
        }
        let mut evicted = 0;
        for name in to_delete {
            let p = self.name_path(&name);
            if fs::remove_file(&p).is_ok() {
                evicted += 1;
            }
        }
        Ok(evicted)
    }

    fn scan_existing(&self) -> io::Result<()> {
        let mut guard = self.lru.lock();
        if !self.root.is_dir() {
            return Ok(());
        }
        for shard in fs::read_dir(&self.root)? {
            let shard = shard?;
            if !shard.path().is_dir() {
                continue;
            }
            for item in fs::read_dir(shard.path())? {
                let item = item?;
                let path = item.path();
                if is_temp_path(&path) {
                    let _ = fs::remove_file(&path);
                    continue;
                }
                if !path.is_file() {
                    continue;
                }
                if let (Some(name), Ok(meta)) =
                    (path.file_name().and_then(|s| s.to_str()), item.metadata())
                {
                    guard.insert(Arc::from(name), (), meta.len());
                }
            }
        }
        Ok(())
    }
}

fn write_and_sync(path: &Path, data: &[u8]) -> io::Result<()> {
    use std::io::Write as _;
    let mut file = File::create(path)?;
    file.write_all(data)?;
    file.sync_all()
}

fn sha256_hex(data: &[u8]) -> String {
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
    let mut state: [u32; 8] = [
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

    for chunk in padded.as_chunks::<64>().0 {
        let mut w = [0u32; 64];
        for (i, word) in chunk.as_chunks::<4>().0.iter().enumerate() {
            w[i] = u32::from_be_bytes(*word);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = state;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = h
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);
            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }
        for (st, add) in state.iter_mut().zip([a, b, c, d, e, f, g, h]) {
            *st = st.wrapping_add(add);
        }
    }
    let mut out = [0u8; 32];
    for (i, word) in state.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    hex::encode(out)
}

fn is_temp_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|s| s.contains(".tmp."))
}

#[cfg(unix)]
fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt as _;
    while !buf.is_empty() {
        let n = file.read_at(buf, offset)?;
        if n == 0 {
            break;
        }
        buf = &mut buf[n..];
        offset += n as u64;
    }
    Ok(())
}

#[cfg(not(unix))]
fn read_exact_at(mut file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    use std::io::Read;
    use std::io::Seek;
    use std::io::SeekFrom;
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cas_serves_o1_ranged_reads_and_evicts_lru_clean_blobs() {
        let dir = tempfile::tempdir().unwrap();
        let cas = BlobCas::new(dir.path(), 24).unwrap();

        cas.put_blob("aa1111", b"0123456789abcdef").unwrap(); // 16 bytes
        let slice = cas.read_range("aa1111", 4, 6).unwrap().unwrap();
        assert_eq!(&*slice, b"456789");

        // Insert second 16-byte blob (total 32 > 24 budget) -> evicts aa1111
        cas.put_blob("bb2222", b"fedcba9876543210").unwrap();
        assert!(!cas.has_blob("aa1111"));
        assert!(cas.has_blob("bb2222"));
        assert_eq!(cas.stats(), (1, 16));
    }

    #[test]
    fn lru_map_orders_by_recency_and_tracks_weights() {
        let mut lru = LruMap::new();
        lru.insert("a".to_string(), 1, 10);
        lru.insert("b".to_string(), 2, 20);
        lru.insert("c".to_string(), 3, 30);
        assert_eq!((lru.len(), lru.total_weight()), (3, 60));

        // Reading `a` makes `b` the least recently used entry.
        assert_eq!(lru.get("a"), Some(&1));
        assert!(!lru.touch("missing"));
        assert_eq!(lru.keys().collect::<Vec<_>>(), ["b", "c", "a"]);
        assert_eq!(lru.pop_lru(), Some(("b".to_string(), 2, 20)));

        // Re-inserting an existing key replaces its value and weight without
        // duplicating it.
        lru.insert("c".to_string(), 4, 5);
        assert_eq!((lru.len(), lru.total_weight()), (2, 15));
        assert_eq!(lru.pop_lru(), Some(("a".to_string(), 1, 10)));
        assert_eq!(lru.pop_lru(), Some(("c".to_string(), 4, 5)));
        assert_eq!(lru.pop_lru(), None);
        assert_eq!(lru.total_weight(), 0);
    }

    #[test]
    fn cas_lru_accounting_survives_restart_for_keys_with_punctuation() {
        let dir = tempfile::tempdir().unwrap();
        {
            let cas = BlobCas::new(dir.path(), 0).unwrap();
            cas.put_blob("local_jj-vfs_abc", b"0123456789").unwrap();
            cas.put_blob("local_jj-vfs_def", b"01234").unwrap();
            assert_eq!(cas.stats(), (2, 15));
        }
        // The startup scan sees sanitized file names; runtime touches of the
        // raw keys must map onto those entries instead of adding duplicates.
        let cas = BlobCas::new(dir.path(), 0).unwrap();
        assert_eq!(cas.stats(), (2, 15));
        assert_eq!(
            &*cas.read_range("local_jj-vfs_abc", 2, 3).unwrap().unwrap(),
            b"234"
        );
        assert_eq!(cas.blob_size("local_jj-vfs_def"), Some(5));
        assert_eq!(cas.stats(), (2, 15));

        // Recency follows reads: `def` was touched last, so `abc` is evicted
        // first.
        assert_eq!(
            cas.read_range("local_jj-vfs_def", 0, 1)
                .unwrap()
                .unwrap()
                .len(),
            1
        );
        cas.set_max_bytes(6);
        assert!(!cas.has_blob("local_jj-vfs_abc"));
        assert!(cas.has_blob("local_jj-vfs_def"));
        assert_eq!(cas.stats(), (1, 5));
    }

    #[test]
    fn cas_hashes_keys_without_conflict_path_collisions_and_cleans_stale_tmp_files() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            sha256_hex(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );

        let dir = tempfile::tempdir().unwrap();
        let cas = BlobCas::new(dir.path(), 0).unwrap();

        // Keys differing only in `/`, `_`, and `-` or exceeding 255 chars must
        // never collide.
        let k_slash = "conflict_1111_a/b.txt";
        let k_under = "conflict_1111_a_b.txt";
        let k_dash = "conflict_1111_a-b.txt";
        let k_long = format!("conflict_1111_{}", "nested/".repeat(60));
        cas.put_blob(k_slash, b"slash-body").unwrap();
        cas.put_blob(k_under, b"under-body").unwrap();
        cas.put_blob(k_dash, b"dash-body").unwrap();
        cas.put_blob(&k_long, b"long-body").unwrap();

        assert_eq!(
            &*cas.read_range(k_slash, 0, 64).unwrap().unwrap(),
            b"slash-body"
        );
        assert_eq!(
            &*cas.read_range(k_under, 0, 64).unwrap().unwrap(),
            b"under-body"
        );
        assert_eq!(
            &*cas.read_range(k_dash, 0, 64).unwrap().unwrap(),
            b"dash-body"
        );
        assert_eq!(
            &*cas.read_range(&k_long, 0, 64).unwrap().unwrap(),
            b"long-body"
        );

        // Plant a leftover `.tmp.` file inside a shard and verify startup scan
        // deletes it.
        let stale_shard = dir.path().join("cas/aa");
        fs::create_dir_all(&stale_shard).unwrap();
        let stale_tmp = stale_shard.join("deadbeef.tmp.99999.1");
        fs::write(&stale_tmp, b"partial").unwrap();
        assert!(stale_tmp.exists());

        let reopened = BlobCas::new(dir.path(), 0).unwrap();
        assert!(
            !stale_tmp.exists(),
            "startup scan must remove leftover .tmp. files"
        );
        assert_eq!(reopened.stats().0, 4);
    }
}
