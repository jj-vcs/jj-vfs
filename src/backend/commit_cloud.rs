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

//! Commit Cloud gRPC implementation (`CommitCloudVfsBackend`) of
//! `ReadOnlyVfsBackend` plus snapshot-publication primitives for
//! `WorkspaceOverlay`.
//!
//! # Authority and Crash-Safety Ordering
//! Connects to `jj-cc-server` over pooled HTTP/2 channels
//! (`cc_lib::util::connect_channel`) or through a local `jj-cc-daemon` Unix
//! domain socket (`x-jj-cc-upstream` metadata), falling back to direct HTTP/2
//! when a configured socket is stale. Snapshot uploads write child blobs and
//! symlinks before `WriteTree`, `WriteTree` before `WriteCommit`, and
//! `WriteCommit` before `PublishOperation` + `UpdateWorkspace`.
//!
//! # Concurrency and Merge Invariants
//! Immutable `Commit`, `Tree`, and `Symlink` payloads live in bounded `LruMap`
//! caches. `lookup_in_commit` answers `getattr`/`stat` from
//! `TreeEntry.File.size_bytes` with zero `ReadFile` RPCs, and `read_file_range`
//! streams `[offset, offset + size)` via ranged `ReadFileRequest` (or full-blob
//! CAS hydration when `size_bytes` is absent).

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::net::IpAddr;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use cc_common::backend::Commit as ProtoCommit;
use cc_common::backend::File as ProtoFile;
use cc_common::backend::ObjectPrefixStatus;
use cc_common::backend::ReadCommitRequest;
use cc_common::backend::ReadFileRequest;
use cc_common::backend::ReadSymlinkRequest;
use cc_common::backend::ReadTreeRequest;
use cc_common::backend::ResolveChangePrefixRequest;
use cc_common::backend::ResolveCommitPrefixRequest;
use cc_common::backend::TreeEntry as ProtoTreeEntry;
use cc_common::backend::TreeValue as ProtoTreeValWrap;
use cc_common::backend::WriteCommitRequest;
use cc_common::backend::WriteFileChunk;
use cc_common::backend::WriteFileRequest;
use cc_common::backend::WriteSymlinkRequest;
use cc_common::backend::WriteTreeRequest;
use cc_common::backend::backend_service_client::BackendServiceClient;
use cc_common::backend::tree_value::Value as ProtoTreeValue;
use cc_common::op_store::Operation as ProtoOperation;
use cc_common::op_store::PublishOperationRequest;
use cc_common::op_store::ReadOperationRequest;
use cc_common::op_store::ReadViewRequest;
use cc_common::op_store::ReconcileOpHeadsRequest;
use cc_common::op_store::View as ProtoView;
use cc_common::op_store::op_store_service_client::OpStoreServiceClient;
use cc_common::workspace::ListWorkspacesRequest;
use cc_common::workspace::UpdateWorkspaceRequest;
use cc_common::workspace::WorkspaceState as ProtoWorkspaceState;
use cc_common::workspace::workspace_service_client::WorkspaceServiceClient;
use cc_lib::daemon_client::UPSTREAM_HEADER;
use futures::StreamExt as _;
use hyper_util::rt::TokioIo;
use parking_lot::Mutex;
use parking_lot::RwLock;
use tokio::net::UnixStream;
use tonic::Code;
use tonic::Request;
use tonic::Status;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tonic::transport::Uri;
use tower::service_fn;

use crate::backend::ReadOnlyVfsBackend;
use crate::backend::VfsLookup;
use crate::backend::VfsWorkspaceRef;
use crate::backend::slice_range;
use crate::cas::BlobCas;
use crate::cas::LruMap;
use crate::jj_error::JjError;
use crate::virtual_file::DirectoryEntry;
use crate::virtual_file::FileAttributes;
use crate::virtual_file::FileType;

const EMPTY_GIT_TREE_HEX: &str = "4b825dc642cb6eb9a060e54bf8d69288fbee4904";
const ROOT_COMMIT_HEX: &str = "0000000000000000000000000000000000000000";
const STREAM_UPLOAD_THRESHOLD: usize = 512 * 1024;
const STREAM_CHUNK_BYTES: usize = 64 * 1024;
const TREE_PAGE_SIZE: u32 = 1000;
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_TOKEN_CMD_TTL: Duration = Duration::from_secs(45 * 60);
const MAX_COMMIT_CACHE_ENTRIES: usize = 4096;
const MAX_TREE_CACHE_ENTRIES: usize = 4096;
const MAX_TREE_CACHE_ITEMS: u64 = 250_000;
const MAX_SYMLINK_CACHE_ENTRIES: usize = 4096;
const MAX_PINNED_COMMITS: usize = 256;
const MAX_ANCESTRY_WALK: usize = 256;

#[derive(Clone, Debug, Default)]
pub struct CcSourceAuth {
    pub daemon_socket: Option<PathBuf>,
    pub user: Option<String>,
    pub auth_token_env: Option<String>,
    pub auth_token_cmd: Vec<String>,
}

#[derive(Clone)]
struct CachedCmdToken {
    token: String,
    fetched_at: Instant,
}

#[derive(Clone, Copy)]
struct CacheLimits {
    commits: usize,
    trees: usize,
    tree_items: u64,
    symlinks: usize,
    pinned_commits: usize,
}

impl Default for CacheLimits {
    fn default() -> Self {
        Self {
            commits: MAX_COMMIT_CACHE_ENTRIES,
            trees: MAX_TREE_CACHE_ENTRIES,
            tree_items: MAX_TREE_CACHE_ITEMS,
            symlinks: MAX_SYMLINK_CACHE_ENTRIES,
            pinned_commits: MAX_PINNED_COMMITS,
        }
    }
}

#[derive(Clone)]
struct PerSourceInterceptor {
    upstream_header: Option<MetadataValue<tonic::metadata::Ascii>>,
    bearer_token: Option<String>,
}

impl Interceptor for PerSourceInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        if let Some(up) = &self.upstream_header {
            req.metadata_mut().insert(UPSTREAM_HEADER, up.clone());
        }
        if let Some(raw) = &self.bearer_token {
            let trimmed = raw.trim();
            if !trimmed.is_empty() {
                let val = MetadataValue::try_from(format!("Bearer {trimmed}"))
                    .map_err(|e| Status::invalid_argument(format!("invalid bearer token: {e}")))?;
                req.metadata_mut().insert("authorization", val);
            }
        }
        Ok(req)
    }
}

type VfsBackendClient = BackendServiceClient<InterceptedService<Channel, PerSourceInterceptor>>;
type VfsOpStoreClient = OpStoreServiceClient<InterceptedService<Channel, PerSourceInterceptor>>;
type VfsWorkspaceClient = WorkspaceServiceClient<InterceptedService<Channel, PerSourceInterceptor>>;

fn build_backend_client(ch: Channel, interceptor: PerSourceInterceptor) -> VfsBackendClient {
    BackendServiceClient::with_interceptor(ch, interceptor)
        .max_decoding_message_size(cc_common::MAX_GRPC_MESSAGE_BYTES)
        .max_encoding_message_size(cc_common::MAX_GRPC_MESSAGE_BYTES)
}

fn build_op_store_client(ch: Channel, interceptor: PerSourceInterceptor) -> VfsOpStoreClient {
    OpStoreServiceClient::with_interceptor(ch, interceptor)
        .max_decoding_message_size(cc_common::MAX_GRPC_MESSAGE_BYTES)
        .max_encoding_message_size(cc_common::MAX_GRPC_MESSAGE_BYTES)
}

fn build_workspace_client(ch: Channel, interceptor: PerSourceInterceptor) -> VfsWorkspaceClient {
    WorkspaceServiceClient::with_interceptor(ch, interceptor)
        .max_decoding_message_size(cc_common::MAX_GRPC_MESSAGE_BYTES)
        .max_encoding_message_size(cc_common::MAX_GRPC_MESSAGE_BYTES)
}

/// Returns `true` when `url` uses `https://` or targets a loopback `http://` endpoint
/// (`localhost`, `127.0.0.0/8`, or `::1`), refusing cleartext Bearer
/// transmission elsewhere.
pub(crate) fn is_bearer_allowed_for_url(url: &str) -> bool {
    let Ok(uri) = url.parse::<Uri>() else {
        return false;
    };
    match uri.scheme_str() {
        Some(s) if s.eq_ignore_ascii_case("https") => true,
        Some(s) if s.eq_ignore_ascii_case("http") => {
            let Some(raw_host) = uri.host() else {
                return false;
            };
            let host = raw_host
                .strip_prefix('[')
                .and_then(|h| h.strip_suffix(']'))
                .unwrap_or(raw_host);
            host.eq_ignore_ascii_case("localhost")
                || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
        }
        _ => false,
    }
}

pub struct CommitCloudVfsBackend {
    source_alias: String,
    repo_id: String,
    server_url: String,
    auth: CcSourceAuth,
    cached_cmd_token: RwLock<Option<CachedCmdToken>>,
    token_refresh_lock: tokio::sync::Mutex<()>,
    token_cmd_ttl: Duration,
    connect_timeout: Duration,
    rpc_timeout: Duration,
    limits: CacheLimits,
    cas: Arc<BlobCas>,
    commit_cache: Mutex<LruMap<String, Arc<ProtoCommit>>>,
    tree_cache: Mutex<LruMap<String, Arc<Vec<ProtoTreeEntry>>>>,
    symlink_cache: Mutex<LruMap<String, String>>,
    pinned_commits: Mutex<LruMap<String, ()>>,
    uds_channel: Mutex<Option<Channel>>,
    uds_connect_lock: tokio::sync::Mutex<()>,
    verified: AtomicBool,
}

impl CommitCloudVfsBackend {
    pub fn new(
        source_alias: impl Into<String>,
        repo_id: impl Into<String>,
        server_url: impl Into<String>,
        auth: CcSourceAuth,
        cas: Arc<BlobCas>,
    ) -> Self {
        let mut pinned = LruMap::new();
        pinned.insert(ROOT_COMMIT_HEX.to_string(), (), 1);

        Self {
            source_alias: source_alias.into(),
            repo_id: repo_id.into(),
            server_url: server_url.into(),
            auth,
            cached_cmd_token: RwLock::new(None),
            token_refresh_lock: tokio::sync::Mutex::new(()),
            token_cmd_ttl: DEFAULT_TOKEN_CMD_TTL,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            rpc_timeout: DEFAULT_RPC_TIMEOUT,
            limits: CacheLimits::default(),
            cas,
            commit_cache: Mutex::new(LruMap::new()),
            tree_cache: Mutex::new(LruMap::new()),
            symlink_cache: Mutex::new(LruMap::new()),
            pinned_commits: Mutex::new(pinned),
            uds_channel: Mutex::new(None),
            uds_connect_lock: tokio::sync::Mutex::new(()),
            verified: AtomicBool::new(false),
        }
    }

    pub fn server_url(&self) -> &str {
        &self.server_url
    }

    pub fn repo_id(&self) -> &str {
        &self.repo_id
    }

    pub fn default_user(&self) -> &str {
        self.auth.user.as_deref().unwrap_or("default")
    }

    /// Reports whether at least one RPC confirmed that `repo_id` has real state
    /// on the server.
    pub fn is_verified(&self) -> bool {
        self.verified.load(Ordering::Relaxed)
    }

    fn mark_verified(&self) {
        self.verified.store(true, Ordering::Relaxed);
    }

    fn cas_blob_key(&self, file_id_hex: &str) -> String {
        format!("cc_{}_{}", self.source_alias, file_id_hex)
    }

    fn insert_commit_cache(&self, hex: String, commit: Arc<ProtoCommit>) {
        let mut guard = self.commit_cache.lock();
        guard.insert(hex, commit, 1);
        while guard.len() > self.limits.commits {
            guard.pop_lru();
        }
    }

    fn insert_tree_cache(&self, hex: String, entries: Arc<Vec<ProtoTreeEntry>>) {
        let weight = (entries.len() as u64).max(1);
        let mut guard = self.tree_cache.lock();
        guard.insert(hex, entries, weight);
        while guard.len() > self.limits.trees
            || (guard.total_weight() > self.limits.tree_items && guard.len() > 1)
        {
            guard.pop_lru();
        }
    }

    fn insert_symlink_cache(&self, hex: String, target: String) {
        let mut guard = self.symlink_cache.lock();
        guard.insert(hex, target, 1);
        while guard.len() > self.limits.symlinks {
            guard.pop_lru();
        }
    }

    fn pin_commit(&self, hex: String) {
        let mut guard = self.pinned_commits.lock();
        guard.insert(hex, (), 1);
        while guard.len() > self.limits.pinned_commits {
            guard.pop_lru();
        }
    }

    /// Resolves this source's configured Bearer token (`auth_token_env` or
    /// `auth_token_cmd`), never falling back to a process-global variable, and
    /// rejects cleartext non-loopback HTTP.
    async fn resolve_bearer_token(&self, force_refresh: bool) -> Result<Option<String>, JjError> {
        let token = if let Some(var) = self.auth.auth_token_env.as_deref() {
            match std::env::var(var) {
                Ok(val) if !val.trim().is_empty() => Some(val.trim().to_string()),
                _ if !self.auth.auth_token_cmd.is_empty() => {
                    self.resolve_cmd_token(force_refresh).await?
                }
                _ => {
                    tracing::warn!(
                        source = %self.source_alias,
                        env_var = %var,
                        "configured auth_token_env is unset or empty"
                    );
                    None
                }
            }
        } else if !self.auth.auth_token_cmd.is_empty() {
            self.resolve_cmd_token(force_refresh).await?
        } else {
            None
        };

        if token.is_some() && !is_bearer_allowed_for_url(&self.server_url) {
            tracing::warn!(
                source = %self.source_alias,
                url = %self.server_url,
                "refusing to send Bearer token over non-loopback plaintext HTTP"
            );
            return Err(JjError::PermissionDenied(format!(
                "refusing to send Bearer token over non-loopback plaintext URL '{}'; use https:// \
                 or loopback",
                self.server_url
            )));
        }
        Ok(token)
    }

    async fn resolve_cmd_token(&self, force_refresh: bool) -> Result<Option<String>, JjError> {
        if !force_refresh
            && let Some(cached) = self.cached_cmd_token.read().as_ref()
            && cached.fetched_at.elapsed() < self.token_cmd_ttl
        {
            return Ok(Some(cached.token.clone()));
        }

        let _guard = self.token_refresh_lock.lock().await;
        if !force_refresh
            && let Some(cached) = self.cached_cmd_token.read().as_ref()
            && cached.fetched_at.elapsed() < self.token_cmd_ttl
        {
            return Ok(Some(cached.token.clone()));
        }

        let cmd = self.auth.auth_token_cmd.clone();
        let alias = self.source_alias.clone();
        let fetched = tokio::time::timeout(
            self.rpc_timeout,
            tokio::task::spawn_blocking(move || run_token_cmd(&alias, &cmd)),
        )
        .await
        .map_err(|_| {
            tracing::warn!(source = %self.source_alias, "auth_token_cmd timed out");
            JjError::PermissionDenied(format!(
                "auth_token_cmd for source '{}' timed out after {:?}",
                self.source_alias, self.rpc_timeout
            ))
        })?
        .map_err(|e| JjError::PermissionDenied(format!("auth_token_cmd task panicked: {e}")))??;

        *self.cached_cmd_token.write() = Some(CachedCmdToken {
            token: fetched.clone(),
            fetched_at: Instant::now(),
        });
        Ok(Some(fetched))
    }

    async fn try_uds_channel(&self, sock_path: &Path) -> Result<Channel, JjError> {
        if let Some(existing) = self.uds_channel.lock().clone() {
            return Ok(existing);
        }
        let _guard = self.uds_connect_lock.lock().await;
        if let Some(existing) = self.uds_channel.lock().clone() {
            return Ok(existing);
        }
        let sp = sock_path.to_path_buf();
        let endpoint = Endpoint::try_from("http://[::]:50051")
            .map_err(|e| JjError::Grpc(e.to_string()))?
            .connect_timeout(self.connect_timeout)
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_timeout(Duration::from_secs(10))
            .keep_alive_while_idle(true);
        let connect_fut = endpoint.connect_with_connector(service_fn(move |_: Uri| {
            let p = sp.clone();
            async move {
                let stream = UnixStream::connect(p).await?;
                Ok::<_, std::io::Error>(TokioIo::new(stream))
            }
        }));
        let ch = tokio::time::timeout(self.connect_timeout, connect_fut)
            .await
            .map_err(|_| JjError::Grpc("UDS connect timed out".to_string()))?
            .map_err(|e| JjError::Grpc(e.to_string()))?;
        *self.uds_channel.lock() = Some(ch.clone());
        Ok(ch)
    }

    async fn direct_channel_and_interceptor(
        &self,
        bearer_token: Option<String>,
    ) -> Result<(Channel, PerSourceInterceptor, bool), JjError> {
        let server_url = self.server_url.clone();
        let ch = tokio::time::timeout(
            self.connect_timeout,
            cc_lib::util::run_on_tokio(
                async move { cc_lib::util::connect_channel(server_url).await },
            ),
        )
        .await
        .map_err(|_| {
            JjError::IO(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "connecting to Commit Cloud server '{}' timed out after {:?}",
                    self.server_url, self.connect_timeout
                ),
            ))
        })?
        .map_err(|e| JjError::Grpc(e.to_string()))?;
        Ok((
            ch,
            PerSourceInterceptor {
                upstream_header: None,
                bearer_token,
            },
            false,
        ))
    }

    async fn channel_and_interceptor(
        &self,
        force_token_refresh: bool,
    ) -> Result<(Channel, PerSourceInterceptor, bool), JjError> {
        let bearer_token = self.resolve_bearer_token(force_token_refresh).await?;
        if let Some(sock_path) = &self.auth.daemon_socket
            && sock_path.exists()
        {
            match self.try_uds_channel(sock_path).await {
                Ok(ch) => {
                    let up = MetadataValue::try_from(self.server_url.as_str())
                        .map_err(|e| JjError::Grpc(e.to_string()))?;
                    return Ok((
                        ch,
                        PerSourceInterceptor {
                            upstream_header: Some(up),
                            bearer_token,
                        },
                        true,
                    ));
                }
                Err(err) => {
                    tracing::warn!(
                        source = %self.source_alias,
                        socket = %sock_path.display(),
                        error = %err,
                        "failed to connect to jj-cc-daemon socket; falling back to direct server channel"
                    );
                }
            }
        }
        self.direct_channel_and_interceptor(bearer_token).await
    }

    fn timeout_error(&self) -> JjError {
        JjError::IO(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!(
                "Commit Cloud RPC to '{}' timed out after {:?}",
                self.server_url, self.rpc_timeout
            ),
        ))
    }

    async fn run_rpc<T, F, Fut>(&self, mut op: F) -> Result<T, JjError>
    where
        F: FnMut(Channel, PerSourceInterceptor) -> Fut,
        Fut: std::future::Future<Output = Result<T, Status>>,
    {
        let (ch, interceptor, via_uds) = self.channel_and_interceptor(false).await?;
        let first = tokio::time::timeout(self.rpc_timeout, op(ch, interceptor)).await;
        match first {
            Ok(Ok(val)) => Ok(val),
            Err(_) => {
                if via_uds {
                    *self.uds_channel.lock() = None;
                } else {
                    cc_lib::util::evict_cached_channel(&self.server_url);
                }
                Err(self.timeout_error())
            }
            Ok(Err(status))
                if status.code() == Code::Unauthenticated
                    && !self.auth.auth_token_cmd.is_empty() =>
            {
                tracing::info!(
                    source = %self.source_alias,
                    "Commit Cloud RPC returned Unauthenticated; refreshing auth_token_cmd"
                );
                let (ch2, int2, _) = self.channel_and_interceptor(true).await?;
                match tokio::time::timeout(self.rpc_timeout, op(ch2, int2)).await {
                    Ok(res) => res.map_err(JjError::from),
                    Err(_) => Err(self.timeout_error()),
                }
            }
            Ok(Err(status)) if via_uds && status.code() == Code::Unavailable => {
                tracing::warn!(
                    source = %self.source_alias,
                    error = %status,
                    "jj-cc-daemon UDS RPC returned Unavailable; retrying over direct channel"
                );
                *self.uds_channel.lock() = None;
                let bearer = self.resolve_bearer_token(false).await?;
                let (ch2, int2, _) = self.direct_channel_and_interceptor(bearer).await?;
                match tokio::time::timeout(self.rpc_timeout, op(ch2, int2)).await {
                    Ok(res) => res.map_err(JjError::from),
                    Err(_) => Err(self.timeout_error()),
                }
            }
            Ok(Err(status)) => Err(JjError::from(status)),
        }
    }

    pub async fn fetch_commit(&self, commit_id_hex: &str) -> Result<Arc<ProtoCommit>, JjError> {
        let norm = commit_id_hex.trim().to_ascii_lowercase();
        if norm.chars().all(|c| c == '0') && norm.len() == 40 {
            return Ok(Arc::new(ProtoCommit {
                commit_id: vec![0u8; 20],
                change_id: vec![0u8; 16],
                root_tree_id: vec![hex::decode(EMPTY_GIT_TREE_HEX).unwrap()],
                ..Default::default()
            }));
        }
        if let Some(hit) = self.commit_cache.lock().get(&norm).cloned() {
            return Ok(hit);
        }
        let raw_id = hex::decode(&norm).map_err(|_| JjError::NotFound)?;
        let repo_id = self.repo_id.clone();
        let resp = self
            .run_rpc(|ch, int| {
                let repo_id = repo_id.clone();
                let raw_id = raw_id.clone();
                async move {
                    let mut client = build_backend_client(ch, int);
                    Ok(client
                        .read_commit(ReadCommitRequest {
                            project_id: repo_id,
                            commit_id: raw_id,
                        })
                        .await?
                        .into_inner())
                }
            })
            .await?;
        let commit = Arc::new(resp.commit.ok_or(JjError::NotFound)?);
        self.mark_verified();
        self.insert_commit_cache(norm, Arc::clone(&commit));
        Ok(commit)
    }

    pub async fn fetch_tree(&self, tree_id_hex: &str) -> Result<Arc<Vec<ProtoTreeEntry>>, JjError> {
        let norm = tree_id_hex.trim().to_ascii_lowercase();
        if norm.is_empty() || norm == EMPTY_GIT_TREE_HEX {
            return Ok(Arc::new(Vec::new()));
        }
        if let Some(hit) = self.tree_cache.lock().get(&norm).cloned() {
            return Ok(hit);
        }
        let tree_bytes = hex::decode(&norm).map_err(|_| JjError::NotFound)?;
        let mut all_entries = Vec::new();
        let mut page_token = String::new();
        loop {
            let repo_id = self.repo_id.clone();
            let tid = tree_bytes.clone();
            let tok = page_token.clone();
            let resp = self
                .run_rpc(|ch, int| {
                    let repo_id = repo_id.clone();
                    let tid = tid.clone();
                    let tok = tok.clone();
                    async move {
                        let mut client = build_backend_client(ch, int);
                        Ok(client
                            .read_tree(ReadTreeRequest {
                                project_id: repo_id,
                                tree_id: tid,
                                path: String::new(),
                                page_size: TREE_PAGE_SIZE,
                                page_token: tok,
                            })
                            .await?
                            .into_inner())
                    }
                })
                .await?;
            all_entries.extend(resp.entries);
            if resp.next_page_token.is_empty() {
                break;
            }
            page_token = resp.next_page_token;
        }
        self.mark_verified();
        let arc = Arc::new(all_entries);
        self.insert_tree_cache(norm, Arc::clone(&arc));
        Ok(arc)
    }

    async fn fetch_symlink_target(&self, symlink_id_hex: &str) -> Result<String, JjError> {
        if let Some(hit) = self.symlink_cache.lock().get(symlink_id_hex).cloned() {
            return Ok(hit);
        }
        let bytes = hex::decode(symlink_id_hex).map_err(|_| JjError::NotFound)?;
        let repo_id = self.repo_id.clone();
        let resp = self
            .run_rpc(|ch, int| {
                let repo_id = repo_id.clone();
                let bytes = bytes.clone();
                async move {
                    let mut client = build_backend_client(ch, int);
                    Ok(client
                        .read_symlink(ReadSymlinkRequest {
                            project_id: repo_id,
                            symlink_id: bytes,
                        })
                        .await?
                        .into_inner())
                }
            })
            .await?;
        self.mark_verified();
        self.insert_symlink_cache(symlink_id_hex.to_string(), resp.target.clone());
        Ok(resp.target)
    }

    /// Reads the reconciled active `(operation_id, Operation, View)` from
    /// `OpStoreService`.
    pub async fn fetch_reconciled_view(
        &self,
    ) -> Result<(Vec<u8>, ProtoOperation, ProtoView), JjError> {
        let repo_id = self.repo_id.clone();
        let (op_head, op_opt, view_opt) = self
            .run_rpc(|ch, int| {
                let repo_id = repo_id.clone();
                async move {
                    let mut client = build_op_store_client(ch, int);
                    let op_head = client
                        .reconcile_op_heads(ReconcileOpHeadsRequest {
                            repo_id: repo_id.clone(),
                        })
                        .await?
                        .into_inner()
                        .op_head;
                    let op = client
                        .read_operation(ReadOperationRequest {
                            repo_id: repo_id.clone(),
                            operation_id: op_head.clone(),
                        })
                        .await?
                        .into_inner()
                        .operation;
                    let view = if let Some(ref o) = op {
                        client
                            .read_view(ReadViewRequest {
                                repo_id,
                                view_id: o.view_id.clone(),
                            })
                            .await?
                            .into_inner()
                            .view
                    } else {
                        None
                    };
                    Ok((op_head, op, view))
                }
            })
            .await?;
        let op = op_opt.ok_or(JjError::NotFound)?;
        let view = view_opt.ok_or(JjError::NotFound)?;
        if op_head.as_slice() != cc_common::ROOT_OPERATION_ID_BYTES
            || !view.head_ids.is_empty()
            || !view.wc_commit_ids.is_empty()
            || !view.local_bookmarks.is_empty()
        {
            self.mark_verified();
        }
        for hid in &view.head_ids {
            self.pin_commit(hex::encode(hid));
        }
        for wcid in view.wc_commit_ids.values() {
            self.pin_commit(hex::encode(wcid));
        }
        for target in view.local_bookmarks.values() {
            for add in &target.adds {
                self.pin_commit(hex::encode(&add.commit_id));
            }
        }
        Ok((op_head, op, view))
    }

    /// Uploads a modified file blob (using streaming chunks for payloads > 512
    /// KiB) and caches the clean content in `BlobCas`.
    pub async fn upload_blob(&self, data: &[u8]) -> Result<Vec<u8>, JjError> {
        let repo_id = self.repo_id.clone();
        let file_id = if data.len() <= STREAM_UPLOAD_THRESHOLD {
            let content = data.to_vec();
            self.run_rpc(|ch, int| {
                let repo_id = repo_id.clone();
                let content = content.clone();
                async move {
                    let mut client = build_backend_client(ch, int);
                    Ok(client
                        .write_file(WriteFileRequest {
                            project_id: repo_id,
                            content,
                        })
                        .await?
                        .into_inner()
                        .file_id)
                }
            })
            .await?
        } else {
            let chunks: Vec<WriteFileChunk> = data
                .chunks(STREAM_CHUNK_BYTES)
                .enumerate()
                .map(|(idx, slice)| WriteFileChunk {
                    project_id: if idx == 0 {
                        repo_id.clone()
                    } else {
                        String::new()
                    },
                    chunk: slice.to_vec(),
                })
                .collect();
            let fallback_payload = data.to_vec();
            self.run_rpc(|ch, int| {
                let chunks = chunks.clone();
                let repo_id = repo_id.clone();
                let fallback_payload = fallback_payload.clone();
                async move {
                    let mut client = build_backend_client(ch, int);
                    match client
                        .write_file_stream(futures::stream::iter(chunks))
                        .await
                    {
                        Ok(resp) => Ok(resp.into_inner().file_id),
                        Err(st) if st.code() == Code::Unimplemented => Ok(client
                            .write_file(WriteFileRequest {
                                project_id: repo_id,
                                content: fallback_payload,
                            })
                            .await?
                            .into_inner()
                            .file_id),
                        Err(st) => Err(st),
                    }
                }
            })
            .await?
        };
        self.mark_verified();
        let fid_hex = hex::encode(&file_id);
        let _ = self.cas.put_blob(&self.cas_blob_key(&fid_hex), data);
        Ok(file_id)
    }

    pub async fn upload_symlink(&self, target: &str) -> Result<Vec<u8>, JjError> {
        let repo_id = self.repo_id.clone();
        let target_owned = target.to_string();
        let sid = self
            .run_rpc(|ch, int| {
                let repo_id = repo_id.clone();
                let target_owned = target_owned.clone();
                async move {
                    let mut client = build_backend_client(ch, int);
                    Ok(client
                        .write_symlink(WriteSymlinkRequest {
                            project_id: repo_id,
                            target: target_owned,
                        })
                        .await?
                        .into_inner()
                        .symlink_id)
                }
            })
            .await?;
        self.mark_verified();
        self.insert_symlink_cache(hex::encode(&sid), target.to_string());
        Ok(sid)
    }

    pub async fn upload_tree(
        &self,
        rel_dir: &str,
        mut entries: Vec<ProtoTreeEntry>,
    ) -> Result<Vec<u8>, JjError> {
        entries.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
        let repo_id = self.repo_id.clone();
        let path = rel_dir.to_string();
        let req_entries = entries.clone();
        let tid = self
            .run_rpc(|ch, int| {
                let repo_id = repo_id.clone();
                let path = path.clone();
                let req_entries = req_entries.clone();
                async move {
                    let mut client = build_backend_client(ch, int);
                    Ok(client
                        .write_tree(WriteTreeRequest {
                            project_id: repo_id,
                            path,
                            entries: req_entries,
                        })
                        .await?
                        .into_inner()
                        .tree_id)
                }
            })
            .await?;
        self.mark_verified();
        let tid_hex = hex::encode(&tid);
        self.insert_tree_cache(tid_hex, Arc::new(entries));
        Ok(tid)
    }

    pub async fn upload_commit(&self, commit: ProtoCommit) -> Result<Vec<u8>, JjError> {
        let repo_id = self.repo_id.clone();
        let req_commit = commit.clone();
        let cid = self
            .run_rpc(|ch, int| {
                let repo_id = repo_id.clone();
                let req_commit = req_commit.clone();
                async move {
                    let mut client = build_backend_client(ch, int);
                    Ok(client
                        .write_commit(WriteCommitRequest {
                            project_id: repo_id,
                            commit: Some(req_commit),
                        })
                        .await?
                        .into_inner()
                        .commit_id)
                }
            })
            .await?;
        self.mark_verified();
        let mut stored = commit;
        stored.commit_id = cid.clone();
        let cid_hex = hex::encode(&cid);
        self.insert_commit_cache(cid_hex.clone(), Arc::new(stored));
        self.pin_commit(cid_hex);
        Ok(cid)
    }

    pub async fn publish_snapshot_operation(
        &self,
        old_op_head: Vec<u8>,
        view: ProtoView,
        operation: ProtoOperation,
        workspace_state: Option<ProtoWorkspaceState>,
    ) -> Result<Vec<u8>, JjError> {
        let repo_id = self.repo_id.clone();
        let new_op_id = self
            .run_rpc(|ch, int| {
                let repo_id = repo_id.clone();
                let view = view.clone();
                let operation = operation.clone();
                let old_op_head = old_op_head.clone();
                async move {
                    let mut op_client = build_op_store_client(ch, int);
                    Ok(op_client
                        .publish_operation(PublishOperationRequest {
                            repo_id,
                            view: Some(view),
                            operation: Some(operation),
                            old_op_head_ids: vec![old_op_head],
                        })
                        .await?
                        .into_inner()
                        .operation_id)
                }
            })
            .await?;
        self.mark_verified();

        if let Some(mut ws) = workspace_state {
            ws.operation_id = new_op_id.clone();
            self.run_rpc(|ch, int| {
                let ws = ws.clone();
                async move {
                    let mut ws_client = build_workspace_client(ch, int);
                    let _ = ws_client
                        .update_workspace(UpdateWorkspaceRequest {
                            workspace: Some(ws),
                        })
                        .await?;
                    Ok(())
                }
            })
            .await?;
        }
        Ok(new_op_id)
    }

    pub async fn update_workspace_state(
        &self,
        workspace: ProtoWorkspaceState,
    ) -> Result<(), JjError> {
        self.run_rpc(|ch, int| {
            let ws = workspace.clone();
            async move {
                let mut ws_client = build_workspace_client(ch, int);
                let _ = ws_client
                    .update_workspace(UpdateWorkspaceRequest {
                        workspace: Some(ws),
                    })
                    .await?;
                Ok(())
            }
        })
        .await?;
        self.mark_verified();
        Ok(())
    }

    pub async fn read_operation_proto(&self, op_id_hex: &str) -> Result<ProtoOperation, JjError> {
        let op_bytes = hex::decode(op_id_hex).map_err(|_| JjError::InvalidPath)?;
        let repo_id = self.repo_id.clone();
        let op = self
            .run_rpc(|ch, int| {
                let repo_id = repo_id.clone();
                let op_bytes = op_bytes.clone();
                async move {
                    let mut client = build_op_store_client(ch, int);
                    Ok(client
                        .read_operation(ReadOperationRequest {
                            repo_id,
                            operation_id: op_bytes,
                        })
                        .await?
                        .into_inner()
                        .operation)
                }
            })
            .await?;
        op.ok_or(JjError::NotFound)
    }

    pub async fn resolve_wc_commit_at_op(
        &self,
        op_id_hex: &str,
        ws_name: &str,
    ) -> Result<Option<String>, JjError> {
        let op = self.read_operation_proto(op_id_hex).await?;
        let repo_id = self.repo_id.clone();
        let view_id = op.view_id;
        let view = self
            .run_rpc(|ch, int| {
                let repo_id = repo_id.clone();
                let view_id = view_id.clone();
                async move {
                    let mut client = build_op_store_client(ch, int);
                    Ok(client
                        .read_view(ReadViewRequest { repo_id, view_id })
                        .await?
                        .into_inner()
                        .view)
                }
            })
            .await?
            .ok_or(JjError::NotFound)?;
        Ok(view.wc_commit_ids.get(ws_name).map(hex::encode))
    }

    async fn walk_to_entry(
        &self,
        root_tree_hex: &str,
        segments: &[&str],
    ) -> Result<ProtoTreeEntry, JjError> {
        if segments.is_empty() {
            return Err(JjError::InvalidPath);
        }
        let mut curr_tree = root_tree_hex.to_string();
        for (idx, seg) in segments.iter().enumerate() {
            let entries = self.fetch_tree(&curr_tree).await?;
            let found = entries
                .binary_search_by(|e| e.name.as_bytes().cmp(seg.as_bytes()))
                .ok()
                .map(|i| entries[i].clone())
                .or_else(|| entries.iter().find(|e| e.name == *seg).cloned())
                .ok_or(JjError::NotFound)?;
            if idx + 1 == segments.len() {
                return Ok(found);
            }
            match found.value.and_then(|v| v.value) {
                Some(ProtoTreeValue::TreeId(tid)) => {
                    curr_tree = hex::encode(tid);
                }
                _ => return Err(JjError::NotADirectory),
            }
        }
        Err(JjError::NotFound)
    }

    async fn stream_file_range_rpc(
        &self,
        file_id_bytes: Vec<u8>,
        offset: u64,
        max_bytes: u64,
    ) -> Result<Vec<u8>, JjError> {
        let repo_id = self.repo_id.clone();
        let bytes = self
            .run_rpc(|ch, int| {
                let repo_id = repo_id.clone();
                let file_id_bytes = file_id_bytes.clone();
                async move {
                    let mut client = build_backend_client(ch, int);
                    let mut stream = match client
                        .read_file(ReadFileRequest {
                            project_id: repo_id,
                            file_id: file_id_bytes,
                            offset,
                            max_bytes,
                        })
                        .await
                    {
                        Ok(resp) => resp.into_inner(),
                        Err(st) if st.code() == Code::OutOfRange => return Ok(Vec::new()),
                        Err(st) => return Err(st),
                    };
                    let mut buf = Vec::new();
                    while let Some(frame) = stream.next().await {
                        let chunk = match frame {
                            Ok(f) => f.chunk,
                            Err(st) if st.code() == Code::OutOfRange && buf.is_empty() => {
                                return Ok(Vec::new());
                            }
                            Err(st) => return Err(st),
                        };
                        if max_bytes > 0
                            && (buf.len() as u64).saturating_add(chunk.len() as u64) > max_bytes
                        {
                            return Err(Status::internal(format!(
                                "ReadFile response exceeded requested max_bytes ({max_bytes})"
                            )));
                        }
                        buf.extend_from_slice(&chunk);
                    }
                    Ok(buf)
                }
            })
            .await?;
        self.mark_verified();
        Ok(bytes)
    }

    /// Walks up to `MAX_ANCESTRY_WALK` parent commits from `tips` to determine
    /// which `candidates` are visible in the active repository view. Returns
    /// `(visible, truncated)`.
    async fn filter_visible_from_tips(
        &self,
        tips: &BTreeSet<Vec<u8>>,
        candidates: &BTreeSet<Vec<u8>>,
    ) -> Result<(Vec<Vec<u8>>, bool), JjError> {
        let mut visited: BTreeSet<Vec<u8>> = tips.clone();
        let mut queue: VecDeque<Vec<u8>> = tips.iter().cloned().collect();
        let mut visible: BTreeSet<Vec<u8>> = BTreeSet::new();
        let mut steps = 0usize;
        let mut truncated = false;

        while let Some(curr) = queue.pop_front() {
            if curr.iter().all(|&b| b == 0) {
                continue;
            }
            if candidates.contains(&curr) {
                visible.insert(curr.clone());
            }
            if steps >= MAX_ANCESTRY_WALK {
                truncated = true;
                break;
            }
            steps += 1;
            let commit = match self.fetch_commit(&hex::encode(&curr)).await {
                Ok(c) => c,
                Err(JjError::NotFound) => continue,
                Err(e) => return Err(e),
            };
            for parent in &commit.parent_commit_ids {
                if visited.insert(parent.clone()) {
                    queue.push_back(parent.clone());
                }
            }
        }
        Ok((visible.into_iter().collect(), truncated))
    }

    /// Computes the predecessor frontier of `commit_ids`: drops any candidate
    /// listed in another readable candidate's `predecessors`.
    async fn predecessor_frontier(&self, commit_ids: &[Vec<u8>]) -> Result<Vec<Vec<u8>>, JjError> {
        let mut readable = Vec::new();
        let mut superseded = BTreeSet::new();
        for cid in commit_ids {
            match self.fetch_commit(&hex::encode(cid)).await {
                Ok(commit) => {
                    for pred in &commit.predecessors {
                        superseded.insert(pred.clone());
                    }
                    readable.push(cid.clone());
                }
                Err(JjError::NotFound) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(readable
            .into_iter()
            .filter(|cid| !superseded.contains(cid))
            .collect())
    }

    async fn diff_tree_recursive(
        &self,
        prefix: &str,
        old_hex: &str,
        new_hex: &str,
        out: &mut BTreeSet<String>,
    ) -> Result<(), JjError> {
        if old_hex == new_hex {
            return Ok(());
        }
        let old_entries = self.fetch_tree(old_hex).await?;
        let new_entries = self.fetch_tree(new_hex).await?;
        let old_map: BTreeMap<&str, &ProtoTreeEntry> =
            old_entries.iter().map(|e| (e.name.as_str(), e)).collect();
        let new_map: BTreeMap<&str, &ProtoTreeEntry> =
            new_entries.iter().map(|e| (e.name.as_str(), e)).collect();
        let all_names: BTreeSet<&str> = old_map.keys().chain(new_map.keys()).copied().collect();

        for name in all_names {
            let rel = if prefix.is_empty() {
                name.to_string()
            } else {
                format!("{prefix}/{name}")
            };
            match (old_map.get(name), new_map.get(name)) {
                (Some(o), Some(n)) => {
                    let ov = o.value.as_ref().and_then(|v| v.value.as_ref());
                    let nv = n.value.as_ref().and_then(|v| v.value.as_ref());
                    match (ov, nv) {
                        (Some(ProtoTreeValue::TreeId(ot)), Some(ProtoTreeValue::TreeId(nt))) => {
                            Box::pin(self.diff_tree_recursive(
                                &rel,
                                &hex::encode(ot),
                                &hex::encode(nt),
                                out,
                            ))
                            .await?;
                        }
                        (Some(ProtoTreeValue::File(of)), Some(ProtoTreeValue::File(nf)))
                            if of.id == nf.id && of.executable == nf.executable => {}
                        (
                            Some(ProtoTreeValue::SymlinkId(os)),
                            Some(ProtoTreeValue::SymlinkId(ns)),
                        ) if os == ns => {}
                        _ => {
                            out.insert(rel);
                        }
                    }
                }
                _ => {
                    out.insert(rel);
                }
            }
        }
        Ok(())
    }
}

fn run_token_cmd(source_alias: &str, cmd: &[String]) -> Result<String, JjError> {
    let Some(prog) = cmd.first() else {
        return Err(JjError::PermissionDenied(
            "empty auth_token_cmd".to_string(),
        ));
    };
    let output = Command::new(prog).args(&cmd[1..]).output().map_err(|err| {
        tracing::warn!(
            source = %source_alias,
            program = %prog,
            error = %err,
            "failed to execute auth_token_cmd"
        );
        JjError::PermissionDenied(format!("failed to execute auth_token_cmd '{prog}': {err}"))
    })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        tracing::warn!(
            source = %source_alias,
            program = %prog,
            status = %output.status,
            stderr = %stderr.trim(),
            "auth_token_cmd exited with non-zero status"
        );
        return Err(JjError::PermissionDenied(format!(
            "auth_token_cmd '{prog}' failed ({}): {}",
            output.status,
            stderr.trim()
        )));
    }
    let stdout = String::from_utf8(output.stdout).map_err(|err| {
        tracing::warn!(
            source = %source_alias,
            program = %prog,
            error = %err,
            "auth_token_cmd produced non-UTF-8 stdout"
        );
        JjError::PermissionDenied(format!("auth_token_cmd '{prog}' produced invalid UTF-8"))
    })?;
    let trimmed = stdout.trim().to_string();
    if trimmed.is_empty() {
        tracing::warn!(
            source = %source_alias,
            program = %prog,
            "auth_token_cmd produced empty token"
        );
        return Err(JjError::PermissionDenied(format!(
            "auth_token_cmd '{prog}' returned an empty token"
        )));
    }
    Ok(trimmed)
}

fn proto_commit_mtime(commit: &ProtoCommit) -> SystemTime {
    let millis = commit
        .committer
        .as_ref()
        .and_then(|s| s.timestamp.as_ref())
        .map(|t| t.millis_since_epoch)
        .unwrap_or(0);
    if millis >= 0 {
        UNIX_EPOCH + Duration::from_millis(millis as u64)
    } else {
        UNIX_EPOCH
    }
}

fn split_rel_segments(path: &Path) -> Result<Vec<&str>, JjError> {
    let mut out = Vec::new();
    for comp in path.components() {
        match comp {
            std::path::Component::Normal(os) => {
                let s = os.to_str().ok_or(JjError::InvalidPath)?;
                out.push(s);
            }
            std::path::Component::CurDir => {}
            _ => return Err(JjError::InvalidPath),
        }
    }
    Ok(out)
}

#[async_trait]
impl ReadOnlyVfsBackend for CommitCloudVfsBackend {
    fn repo_name(&self) -> &str {
        &self.repo_id
    }

    async fn resolve_commit(&self, id_or_prefix: &str) -> Result<String, JjError> {
        let trimmed = id_or_prefix.trim().to_ascii_lowercase();
        if trimmed.len() < 8 || !trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(JjError::NotFound);
        }
        if trimmed.len() == 40 {
            let _ = self.fetch_commit(&trimmed).await?;
            self.pin_commit(trimmed.clone());
            return Ok(trimmed);
        }
        let repo_id = self.repo_id.clone();
        let req_pfx = trimmed.clone();
        let resp = self
            .run_rpc(|ch, int| {
                let repo_id = repo_id.clone();
                let req_pfx = req_pfx.clone();
                async move {
                    let mut client = build_backend_client(ch, int);
                    Ok(client
                        .resolve_commit_prefix(ResolveCommitPrefixRequest {
                            project_id: repo_id,
                            hex_prefix: req_pfx,
                        })
                        .await?
                        .into_inner())
                }
            })
            .await?;
        if resp.status == ObjectPrefixStatus::ObjectPrefixSingleMatch as i32
            && !resp.matched_commit_id.is_empty()
        {
            self.mark_verified();
            let hex = hex::encode(resp.matched_commit_id);
            self.pin_commit(hex.clone());
            Ok(hex)
        } else {
            Err(JjError::NotFound)
        }
    }

    async fn resolve_change(&self, change_or_prefix: &str) -> Result<Option<String>, JjError> {
        let trimmed = change_or_prefix.trim();
        if trimmed.len() < 8 {
            return Err(JjError::NotFound);
        }
        let repo_id = self.repo_id.clone();
        let req_pfx = trimmed.to_string();
        let resp = self
            .run_rpc(|ch, int| {
                let repo_id = repo_id.clone();
                let req_pfx = req_pfx.clone();
                async move {
                    let mut client = build_backend_client(ch, int);
                    Ok(client
                        .resolve_change_prefix(ResolveChangePrefixRequest {
                            project_id: repo_id,
                            prefix: req_pfx,
                        })
                        .await?
                        .into_inner())
                }
            })
            .await?;
        if resp.status != ObjectPrefixStatus::ObjectPrefixSingleMatch as i32
            || resp.commit_ids.is_empty()
        {
            return Err(JjError::NotFound);
        }

        let (_, _, view) = self.fetch_reconciled_view().await?;
        let visible_tips: BTreeSet<Vec<u8>> = view
            .head_ids
            .iter()
            .chain(view.wc_commit_ids.values())
            .cloned()
            .collect();

        if !visible_tips.is_empty() {
            let candidate_set: BTreeSet<Vec<u8>> = resp.commit_ids.iter().cloned().collect();
            let (visible, truncated) = self
                .filter_visible_from_tips(&visible_tips, &candidate_set)
                .await?;
            match visible.as_slice() {
                [only] => {
                    self.mark_verified();
                    let hex = hex::encode(only);
                    self.pin_commit(hex.clone());
                    return Ok(Some(hex));
                }
                [] if !truncated => return Err(JjError::NotFound),
                [] => {}
                _ => return Ok(None),
            }
        }

        let frontier = self.predecessor_frontier(&resp.commit_ids).await?;
        match frontier.as_slice() {
            [only] => {
                self.mark_verified();
                let hex = hex::encode(only);
                self.pin_commit(hex.clone());
                Ok(Some(hex))
            }
            [] => Err(JjError::NotFound),
            _ => Ok(None),
        }
    }

    async fn list_bookmarks(&self) -> Result<BTreeMap<String, String>, JjError> {
        let (_, _, view) = self.fetch_reconciled_view().await?;
        let mut out = BTreeMap::new();
        for (name, target) in view.local_bookmarks {
            if let Some(first_add) = target.adds.first() {
                out.insert(name, hex::encode(&first_add.commit_id));
            }
        }
        Ok(out)
    }

    async fn list_workspaces(
        &self,
        user_filter: Option<&str>,
    ) -> Result<Vec<VfsWorkspaceRef>, JjError> {
        let repo_id = self.repo_id.clone();
        let listed = self
            .run_rpc(|ch, int| {
                let repo_id = repo_id.clone();
                async move {
                    let mut ws_client = build_workspace_client(ch, int);
                    Ok(ws_client
                        .list_workspaces(ListWorkspacesRequest { repo_id })
                        .await?
                        .into_inner()
                        .workspaces)
                }
            })
            .await?;
        if !listed.is_empty() {
            self.mark_verified();
        }

        let mut results = Vec::new();
        let mut seen_names = BTreeSet::new();
        for ws in listed {
            if let Some(u) = user_filter
                && ws.user != u
            {
                continue;
            }
            seen_names.insert(ws.workspace_name.clone());
            let cid_hex = hex::encode(&ws.commit_id);
            let (tid_hex, tree_ids_hex, conflict_labels) = if !ws.tree_ids.is_empty() {
                let tids: Vec<String> = ws.tree_ids.iter().map(hex::encode).collect();
                let primary = if !ws.tree_id.is_empty() {
                    hex::encode(&ws.tree_id)
                } else {
                    tids[0].clone()
                };
                let labels = if tids.len() > 1 && !cid_hex.is_empty() {
                    self.fetch_commit(&cid_hex)
                        .await
                        .map(|c| c.conflict_labels.clone())
                        .unwrap_or_default()
                } else {
                    Vec::new()
                };
                (primary, tids, labels)
            } else if !ws.tree_id.is_empty() {
                let primary = hex::encode(&ws.tree_id);
                (primary.clone(), vec![primary], Vec::new())
            } else {
                let commit = self.fetch_commit(&cid_hex).await?;
                let tids: Vec<String> = if commit.root_tree_id.is_empty() {
                    vec![EMPTY_GIT_TREE_HEX.to_string()]
                } else {
                    commit.root_tree_id.iter().map(hex::encode).collect()
                };
                let primary = tids[0].clone();
                (primary, tids, commit.conflict_labels.clone())
            };
            self.pin_commit(cid_hex.clone());
            results.push(VfsWorkspaceRef {
                user: ws.user,
                workspace_name: ws.workspace_name,
                commit_id_hex: cid_hex,
                tree_id_hex: tid_hex,
                tree_ids_hex,
                conflict_labels,
                operation_id_hex: hex::encode(&ws.operation_id),
                local_jj_dir: None,
            });
        }

        // Also surface `View.wc_commit_ids` workspaces created with
        // `--working-copy-type local`
        let fallback_user = self.default_user().to_string();
        if user_filter.is_none() || user_filter == Some(fallback_user.as_str()) {
            let (op_head, _, view) = self.fetch_reconciled_view().await?;
            let op_hex = hex::encode(op_head);
            for (ws_name, cid_bytes) in view.wc_commit_ids {
                if seen_names.contains(&ws_name) {
                    continue;
                }
                let cid_hex = hex::encode(cid_bytes);
                let commit = match self.fetch_commit(&cid_hex).await {
                    Ok(c) => c,
                    Err(JjError::NotFound) => {
                        tracing::warn!(
                            repo_id = %self.repo_id,
                            workspace = %ws_name,
                            commit_id = %cid_hex,
                            "skipping view workspace whose commit was not found"
                        );
                        continue;
                    }
                    Err(err) => return Err(err),
                };
                let tree_ids_hex: Vec<String> = if commit.root_tree_id.is_empty() {
                    vec![EMPTY_GIT_TREE_HEX.to_string()]
                } else {
                    commit.root_tree_id.iter().map(hex::encode).collect()
                };
                let tid_hex = tree_ids_hex[0].clone();
                let conflict_labels = commit.conflict_labels.clone();
                self.pin_commit(cid_hex.clone());
                results.push(VfsWorkspaceRef {
                    user: fallback_user.clone(),
                    workspace_name: ws_name,
                    commit_id_hex: cid_hex,
                    tree_id_hex: tid_hex,
                    tree_ids_hex,
                    conflict_labels,
                    operation_id_hex: op_hex.clone(),
                    local_jj_dir: None,
                });
            }
        }
        Ok(results)
    }

    async fn list_pinned_commits(&self) -> Result<Vec<String>, JjError> {
        let _ = self.fetch_reconciled_view().await;
        let mut out: Vec<String> = self.pinned_commits.lock().keys().cloned().collect();
        out.sort();
        Ok(out)
    }

    async fn commit_root_tree_id(&self, commit_id_hex: &str) -> Result<String, JjError> {
        let commit = self.fetch_commit(commit_id_hex).await?;
        Ok(commit
            .root_tree_id
            .first()
            .map(hex::encode)
            .unwrap_or_else(|| EMPTY_GIT_TREE_HEX.to_string()))
    }

    async fn lookup_in_commit(
        &self,
        commit_id_hex: &str,
        rel_path: &Path,
    ) -> Result<VfsLookup, JjError> {
        let commit = self.fetch_commit(commit_id_hex).await?;
        let mtime = proto_commit_mtime(&commit);
        let root_tid_hex = commit
            .root_tree_id
            .first()
            .map(hex::encode)
            .unwrap_or_else(|| EMPTY_GIT_TREE_HEX.to_string());
        let segments = split_rel_segments(rel_path)?;
        if segments.is_empty() {
            return Ok(VfsLookup {
                attr: FileAttributes::directory(mtime),
                object_id_hex: root_tid_hex,
                symlink_target: None,
            });
        }

        let entry = self.walk_to_entry(&root_tid_hex, &segments).await?;
        match entry.value.and_then(|v| v.value).ok_or(JjError::NotFound)? {
            ProtoTreeValue::TreeId(tid) | ProtoTreeValue::GitSubmoduleId(tid) => Ok(VfsLookup {
                attr: FileAttributes::directory(mtime),
                object_id_hex: hex::encode(tid),
                symlink_target: None,
            }),
            ProtoTreeValue::File(ProtoFile {
                id,
                executable,
                size_bytes,
                ..
            }) => {
                let fid_hex = hex::encode(&id);
                let cas_key = self.cas_blob_key(&fid_hex);
                let size = if let Some(s) = size_bytes {
                    s
                } else if let Some(cached_sz) = self.cas.blob_size(&cas_key) {
                    cached_sz
                } else {
                    let bytes = self.stream_file_range_rpc(id, 0, 0).await?;
                    if let Err(err) = self.cas.put_blob(&cas_key, &bytes) {
                        tracing::warn!(
                            key = %cas_key,
                            error = %err,
                            "failed to cache hydrated blob in CAS during lookup"
                        );
                    }
                    bytes.len() as u64
                };
                Ok(VfsLookup {
                    attr: FileAttributes::file(size, executable, mtime),
                    object_id_hex: fid_hex,
                    symlink_target: None,
                })
            }
            ProtoTreeValue::SymlinkId(sid) => {
                let sid_hex = hex::encode(sid);
                let target = self.fetch_symlink_target(&sid_hex).await?;
                Ok(VfsLookup {
                    attr: FileAttributes::symlink(target.len() as u64, mtime),
                    object_id_hex: sid_hex,
                    symlink_target: Some(PathBuf::from(target)),
                })
            }
            ProtoTreeValue::ConflictId(cid) => Ok(VfsLookup {
                attr: FileAttributes::file(0, false, mtime),
                object_id_hex: hex::encode(cid),
                symlink_target: None,
            }),
        }
    }

    async fn list_dir_in_commit(
        &self,
        commit_id_hex: &str,
        dir_path: &Path,
    ) -> Result<Vec<DirectoryEntry>, JjError> {
        let commit = self.fetch_commit(commit_id_hex).await?;
        let root_tid_hex = commit
            .root_tree_id
            .first()
            .map(hex::encode)
            .unwrap_or_else(|| EMPTY_GIT_TREE_HEX.to_string());
        let segments = split_rel_segments(dir_path)?;
        let target_tid_hex = if segments.is_empty() {
            root_tid_hex
        } else {
            let entry = self.walk_to_entry(&root_tid_hex, &segments).await?;
            match entry.value.and_then(|v| v.value) {
                Some(ProtoTreeValue::TreeId(tid)) => hex::encode(tid),
                Some(ProtoTreeValue::GitSubmoduleId(_)) => return Ok(Vec::new()),
                _ => return Err(JjError::NotADirectory),
            }
        };

        let proto_entries = self.fetch_tree(&target_tid_hex).await?;
        let mut out = Vec::with_capacity(proto_entries.len());
        for item in proto_entries.iter() {
            let ft = match item.value.as_ref().and_then(|v| v.value.as_ref()) {
                Some(ProtoTreeValue::TreeId(_) | ProtoTreeValue::GitSubmoduleId(_)) => {
                    FileType::Directory
                }
                Some(ProtoTreeValue::SymlinkId(_)) => FileType::Symlink,
                _ => FileType::File,
            };
            out.push(DirectoryEntry {
                name: item.name.clone(),
                file_type: ft,
            });
        }
        Ok(out)
    }

    async fn read_file_range(
        &self,
        commit_id_hex: &str,
        rel_path: &Path,
        offset: u64,
        size: u32,
    ) -> Result<Box<[u8]>, JjError> {
        let commit = self.fetch_commit(commit_id_hex).await?;
        let root_tid_hex = commit
            .root_tree_id
            .first()
            .map(hex::encode)
            .unwrap_or_else(|| EMPTY_GIT_TREE_HEX.to_string());
        let segments = split_rel_segments(rel_path)?;
        let entry = self.walk_to_entry(&root_tid_hex, &segments).await?;
        let ProtoTreeValue::File(file_meta) =
            entry.value.and_then(|v| v.value).ok_or(JjError::NotAFile)?
        else {
            return Err(JjError::NotAFile);
        };
        if size == 0 {
            return Ok(Box::new([]));
        }

        let fid_hex = hex::encode(&file_meta.id);
        let cas_key = self.cas_blob_key(&fid_hex);
        if let Some(slice) = self.cas.read_range(&cas_key, offset, size)? {
            return Ok(slice);
        }

        match file_meta.size_bytes {
            None => {
                // Legacy server without `size_bytes`: hydrate the full blob
                // into CAS and slice locally so a proto3 server ignoring
                // `offset`/`max_bytes` cannot shift bytes.
                let full = self.stream_file_range_rpc(file_meta.id, 0, 0).await?;
                if let Err(err) = self.cas.put_blob(&cas_key, &full) {
                    tracing::warn!(
                        key = %cas_key,
                        error = %err,
                        "failed to cache hydrated blob in CAS"
                    );
                }
                Ok(slice_range(&full, offset, size))
            }
            Some(total_sz) => {
                if offset >= total_sz {
                    return Ok(Box::new([]));
                }
                if offset == 0 && total_sz <= size as u64 {
                    let full = self.stream_file_range_rpc(file_meta.id, 0, 0).await?;
                    if full.len() as u64 != total_sz {
                        return Err(JjError::Grpc(format!(
                            "ReadFile returned {} bytes for blob advertised as {total_sz} bytes",
                            full.len()
                        )));
                    }
                    if let Err(err) = self.cas.put_blob(&cas_key, &full) {
                        tracing::warn!(
                            key = %cas_key,
                            error = %err,
                            "failed to cache blob in CAS"
                        );
                    }
                    return Ok(slice_range(&full, 0, size));
                }

                let expected = (total_sz - offset).min(size as u64);
                let ranged = self
                    .stream_file_range_rpc(file_meta.id, offset, size as u64)
                    .await?;
                if ranged.is_empty() {
                    return Ok(Box::new([]));
                }
                if ranged.len() as u64 != expected {
                    return Err(JjError::Grpc(format!(
                        "ranged ReadFile returned {} bytes, expected {expected} bytes",
                        ranged.len()
                    )));
                }
                Ok(ranged.into_boxed_slice())
            }
        }
    }

    async fn read_symlink(&self, commit_id_hex: &str, rel_path: &Path) -> Result<PathBuf, JjError> {
        let info = self.lookup_in_commit(commit_id_hex, rel_path).await?;
        if info.attr.file_type != FileType::Symlink {
            return Err(JjError::NotASymlink);
        }
        info.symlink_target.ok_or(JjError::NotASymlink)
    }

    async fn diff_trees(
        &self,
        old_tree_hex: &str,
        new_tree_hex: &str,
    ) -> Result<Vec<String>, JjError> {
        let mut changed = BTreeSet::new();
        self.diff_tree_recursive("", old_tree_hex, new_tree_hex, &mut changed)
            .await?;
        Ok(changed.into_iter().collect())
    }
}

pub fn make_proto_file_entry(
    name: impl Into<String>,
    file_id: Vec<u8>,
    executable: bool,
    size_bytes: u64,
) -> ProtoTreeEntry {
    ProtoTreeEntry {
        name: name.into(),
        value: Some(ProtoTreeValWrap {
            value: Some(ProtoTreeValue::File(ProtoFile {
                id: file_id,
                executable,
                copy_id: Vec::new(),
                size_bytes: Some(size_bytes),
            })),
        }),
    }
}

pub fn make_proto_tree_entry(name: impl Into<String>, tree_id: Vec<u8>) -> ProtoTreeEntry {
    ProtoTreeEntry {
        name: name.into(),
        value: Some(ProtoTreeValWrap {
            value: Some(ProtoTreeValue::TreeId(tree_id)),
        }),
    }
}

pub fn make_proto_symlink_entry(name: impl Into<String>, symlink_id: Vec<u8>) -> ProtoTreeEntry {
    ProtoTreeEntry {
        name: name.into(),
        value: Some(ProtoTreeValWrap {
            value: Some(ProtoTreeValue::SymlinkId(symlink_id)),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use cc_common::backend::backend_service_server::BackendService;
    use cc_common::backend::backend_service_server::BackendServiceServer;
    use cc_common::backend::*;
    use cc_common::op_store::op_store_service_server::OpStoreService;
    use cc_common::op_store::op_store_service_server::OpStoreServiceServer;
    use cc_common::op_store::*;
    use cc_common::workspace::workspace_service_server::WorkspaceService;
    use cc_common::workspace::workspace_service_server::WorkspaceServiceServer;
    use cc_common::workspace::*;
    use dashmap::DashMap;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::ReceiverStream;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::Response;

    use super::*;
    use crate::backend::encode_hex_to_reverse_hex;

    #[derive(Default)]
    struct MockCcServer {
        read_file_calls: AtomicUsize,
        read_tree_calls: DashMap<String, usize>,
        commits: DashMap<Vec<u8>, Commit>,
        trees: DashMap<Vec<u8>, Vec<TreeEntry>>,
        blobs: DashMap<Vec<u8>, Vec<u8>>,
        symlinks: DashMap<Vec<u8>, String>,
        active_view: Mutex<Option<View>>,
        active_op_head: Mutex<Option<Vec<u8>>>,
        required_bearer: Mutex<Option<String>>,
        seen_auth_headers: Mutex<Vec<Option<String>>>,
        legacy_ignore_range: AtomicBool,
        read_commit_unavailable: AtomicBool,
        rpc_delay: Mutex<Option<Duration>>,
    }

    impl MockCcServer {
        #[allow(clippy::result_large_err)]
        fn check_auth<T>(&self, req: &Request<T>) -> Result<(), Status> {
            let hdr = req
                .metadata()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .map(ToOwned::to_owned);
            self.seen_auth_headers.lock().push(hdr.clone());
            if let Some(expected) = self.required_bearer.lock().as_deref() {
                let want = format!("Bearer {expected}");
                if hdr.as_deref() != Some(want.as_str()) {
                    return Err(Status::unauthenticated("token expired or invalid"));
                }
            }
            Ok(())
        }

        async fn maybe_delay(&self) {
            let delay = *self.rpc_delay.lock();
            if let Some(d) = delay {
                tokio::time::sleep(d).await;
            }
        }
    }

    #[derive(Clone, Default)]
    struct MockSvc(Arc<MockCcServer>);

    impl std::ops::Deref for MockSvc {
        type Target = MockCcServer;
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    async fn spawn_mock_server(srv: MockSvc) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(BackendServiceServer::new(srv.clone()))
                .add_service(OpStoreServiceServer::new(srv.clone()))
                .add_service(WorkspaceServiceServer::new(srv))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        addr
    }

    #[tonic::async_trait]
    impl BackendService for MockSvc {
        async fn register_project(
            &self,
            req: Request<RegisterProjectRequest>,
        ) -> Result<Response<RegisterProjectResponse>, Status> {
            self.check_auth(&req)?;
            Ok(Response::new(RegisterProjectResponse {
                project_id: req.into_inner().project_id.unwrap_or_else(|| "p1".into()),
            }))
        }

        async fn register_repository(
            &self,
            req: Request<RegisterRepositoryRequest>,
        ) -> Result<Response<RegisterRepositoryResponse>, Status> {
            self.check_auth(&req)?;
            let r = req.into_inner();
            Ok(Response::new(RegisterRepositoryResponse {
                repo_id: r.repo_id.unwrap_or_else(|| "r1".into()),
                project_id: if r.project_id.is_empty() {
                    "p1".into()
                } else {
                    r.project_id
                },
            }))
        }

        async fn read_commit(
            &self,
            req: Request<ReadCommitRequest>,
        ) -> Result<Response<ReadCommitResponse>, Status> {
            self.check_auth(&req)?;
            self.maybe_delay().await;
            if self.read_commit_unavailable.load(Ordering::SeqCst) {
                return Err(Status::unavailable("transient backend unavailable"));
            }
            let cid = req.into_inner().commit_id;
            let c = self
                .commits
                .get(&cid)
                .map(|r| r.clone())
                .ok_or_else(|| Status::not_found("commit not found"))?;
            Ok(Response::new(ReadCommitResponse { commit: Some(c) }))
        }

        async fn write_commit(
            &self,
            _req: Request<WriteCommitRequest>,
        ) -> Result<Response<WriteCommitResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }

        async fn list_project_commits(
            &self,
            _req: Request<ListProjectCommitsRequest>,
        ) -> Result<Response<ListProjectCommitsResponse>, Status> {
            Ok(Response::new(ListProjectCommitsResponse::default()))
        }

        async fn list_commits(
            &self,
            _req: Request<ListCommitsRequest>,
        ) -> Result<Response<ListCommitsResponse>, Status> {
            Ok(Response::new(ListCommitsResponse::default()))
        }

        async fn resolve_commit_prefix(
            &self,
            req: Request<ResolveCommitPrefixRequest>,
        ) -> Result<Response<ResolveCommitPrefixResponse>, Status> {
            self.check_auth(&req)?;
            self.maybe_delay().await;
            let pfx = req.into_inner().hex_prefix;
            let matches: Vec<Vec<u8>> = self
                .commits
                .iter()
                .filter(|e| hex::encode(e.key()).starts_with(&pfx))
                .map(|e| e.key().clone())
                .collect();
            match matches.as_slice() {
                [single] => Ok(Response::new(ResolveCommitPrefixResponse {
                    status: ObjectPrefixStatus::ObjectPrefixSingleMatch as i32,
                    matched_commit_id: single.clone(),
                })),
                [] => Ok(Response::new(ResolveCommitPrefixResponse {
                    status: ObjectPrefixStatus::ObjectPrefixNoMatch as i32,
                    matched_commit_id: Vec::new(),
                })),
                _ => Ok(Response::new(ResolveCommitPrefixResponse {
                    status: ObjectPrefixStatus::ObjectPrefixAmbiguousMatch as i32,
                    matched_commit_id: Vec::new(),
                })),
            }
        }

        async fn resolve_change_prefix(
            &self,
            req: Request<ResolveChangePrefixRequest>,
        ) -> Result<Response<ResolveChangePrefixResponse>, Status> {
            self.check_auth(&req)?;
            self.maybe_delay().await;
            let raw = req.into_inner().prefix;
            let hex_pfx = crate::backend::normalize_change_prefix_to_hex(&raw)
                .ok_or_else(|| Status::invalid_argument("bad change prefix"))?;
            let mut by_change: BTreeMap<Vec<u8>, Vec<Vec<u8>>> = BTreeMap::new();
            for entry in self.commits.iter() {
                if hex::encode(&entry.value().change_id).starts_with(&hex_pfx) {
                    by_change
                        .entry(entry.value().change_id.clone())
                        .or_default()
                        .push(entry.key().clone());
                }
            }
            match by_change.len() {
                0 => Ok(Response::new(ResolveChangePrefixResponse {
                    status: ObjectPrefixStatus::ObjectPrefixNoMatch as i32,
                    ..Default::default()
                })),
                1 => {
                    let (chg, mut cids) = by_change.into_iter().next().unwrap();
                    cids.sort();
                    Ok(Response::new(ResolveChangePrefixResponse {
                        status: ObjectPrefixStatus::ObjectPrefixSingleMatch as i32,
                        matched_change_id: chg,
                        commit_ids: cids,
                    }))
                }
                _ => Ok(Response::new(ResolveChangePrefixResponse {
                    status: ObjectPrefixStatus::ObjectPrefixAmbiguousMatch as i32,
                    ..Default::default()
                })),
            }
        }

        async fn read_tree(
            &self,
            req: Request<ReadTreeRequest>,
        ) -> Result<Response<ReadTreeResponse>, Status> {
            self.check_auth(&req)?;
            self.maybe_delay().await;
            let tid = req.into_inner().tree_id;
            *self.read_tree_calls.entry(hex::encode(&tid)).or_insert(0) += 1;
            let entries = self
                .trees
                .get(&tid)
                .map(|r| r.clone())
                .ok_or_else(|| Status::not_found("tree not found"))?;
            Ok(Response::new(ReadTreeResponse {
                tree_id: tid,
                entries,
                next_page_token: String::new(),
            }))
        }

        async fn write_tree(
            &self,
            _req: Request<WriteTreeRequest>,
        ) -> Result<Response<WriteTreeResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }

        type ReadFileStream = ReceiverStream<Result<ReadFileResponse, Status>>;

        async fn read_file(
            &self,
            req: Request<ReadFileRequest>,
        ) -> Result<Response<Self::ReadFileStream>, Status> {
            self.check_auth(&req)?;
            self.maybe_delay().await;
            self.read_file_calls.fetch_add(1, Ordering::SeqCst);
            let r = req.into_inner();
            let blob = self
                .blobs
                .get(&r.file_id)
                .map(|b| b.clone())
                .ok_or_else(|| Status::not_found("blob not found"))?;
            if self.legacy_ignore_range.load(Ordering::SeqCst) {
                let (tx, rx) = tokio::sync::mpsc::channel(2);
                let _ = tx.send(Ok(ReadFileResponse { chunk: blob })).await;
                return Ok(Response::new(ReceiverStream::new(rx)));
            }
            if (r.offset as usize) >= blob.len() && (!blob.is_empty() || r.offset > 0) {
                return Err(Status::out_of_range("offset at or past EOF"));
            }
            let start = (r.offset as usize).min(blob.len());
            let end = if r.max_bytes == 0 {
                blob.len()
            } else {
                start.saturating_add(r.max_bytes as usize).min(blob.len())
            };
            let (tx, rx) = tokio::sync::mpsc::channel(2);
            let _ = tx
                .send(Ok(ReadFileResponse {
                    chunk: blob[start..end].to_vec(),
                }))
                .await;
            Ok(Response::new(ReceiverStream::new(rx)))
        }

        async fn write_file(
            &self,
            _req: Request<WriteFileRequest>,
        ) -> Result<Response<WriteFileResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }

        async fn write_file_stream(
            &self,
            _req: Request<tonic::Streaming<WriteFileChunk>>,
        ) -> Result<Response<WriteFileResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }

        async fn read_symlink(
            &self,
            req: Request<ReadSymlinkRequest>,
        ) -> Result<Response<ReadSymlinkResponse>, Status> {
            self.check_auth(&req)?;
            let sid = req.into_inner().symlink_id;
            let target = self
                .symlinks
                .get(&sid)
                .map(|s| s.clone())
                .ok_or_else(|| Status::not_found("symlink not found"))?;
            Ok(Response::new(ReadSymlinkResponse { target }))
        }

        async fn write_symlink(
            &self,
            _req: Request<WriteSymlinkRequest>,
        ) -> Result<Response<WriteSymlinkResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }
    }

    #[tonic::async_trait]
    impl OpStoreService for MockSvc {
        async fn read_operation(
            &self,
            req: Request<ReadOperationRequest>,
        ) -> Result<Response<ReadOperationResponse>, Status> {
            self.check_auth(&req)?;
            Ok(Response::new(ReadOperationResponse {
                operation: Some(Operation {
                    view_id: vec![1u8; 16],
                    ..Default::default()
                }),
            }))
        }

        async fn write_operation(
            &self,
            _req: Request<WriteOperationRequest>,
        ) -> Result<Response<WriteOperationResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }

        async fn resolve_operation_prefix(
            &self,
            _req: Request<ResolveOperationPrefixRequest>,
        ) -> Result<Response<ResolveOperationPrefixResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }

        async fn read_view(
            &self,
            req: Request<ReadViewRequest>,
        ) -> Result<Response<ReadViewResponse>, Status> {
            self.check_auth(&req)?;
            let view = self.active_view.lock().clone().unwrap_or_default();
            Ok(Response::new(ReadViewResponse {
                view_id: vec![1u8; 16],
                view: Some(view),
            }))
        }

        async fn write_view(
            &self,
            _req: Request<WriteViewRequest>,
        ) -> Result<Response<WriteViewResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }

        async fn get_op_heads(
            &self,
            _req: Request<GetOpHeadsRequest>,
        ) -> Result<Response<GetOpHeadsResponse>, Status> {
            Ok(Response::new(GetOpHeadsResponse {
                op_head_ids: vec![vec![1u8; 16]],
            }))
        }

        async fn update_op_heads(
            &self,
            _req: Request<UpdateOpHeadsRequest>,
        ) -> Result<Response<UpdateOpHeadsResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }

        async fn reconcile_op_heads(
            &self,
            req: Request<ReconcileOpHeadsRequest>,
        ) -> Result<Response<ReconcileOpHeadsResponse>, Status> {
            self.check_auth(&req)?;
            let op_head = self
                .active_op_head
                .lock()
                .clone()
                .unwrap_or_else(|| vec![1u8; 16]);
            Ok(Response::new(ReconcileOpHeadsResponse { op_head }))
        }

        async fn publish_operation(
            &self,
            _req: Request<PublishOperationRequest>,
        ) -> Result<Response<PublishOperationResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }
    }

    #[tonic::async_trait]
    impl WorkspaceService for MockSvc {
        async fn get_workspace(
            &self,
            _req: Request<GetWorkspaceRequest>,
        ) -> Result<Response<GetWorkspaceResponse>, Status> {
            Ok(Response::new(GetWorkspaceResponse { workspace: None }))
        }

        async fn update_workspace(
            &self,
            _req: Request<UpdateWorkspaceRequest>,
        ) -> Result<Response<UpdateWorkspaceResponse>, Status> {
            Ok(Response::new(UpdateWorkspaceResponse { success: true }))
        }

        async fn list_workspaces(
            &self,
            req: Request<ListWorkspacesRequest>,
        ) -> Result<Response<ListWorkspacesResponse>, Status> {
            self.check_auth(&req)?;
            Ok(Response::new(ListWorkspacesResponse { workspaces: vec![] }))
        }

        async fn check_working_copy_changes(
            &self,
            _req: Request<CheckWorkingCopyChangesRequest>,
        ) -> Result<Response<CheckWorkingCopyChangesResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }

        async fn delete_workspace(
            &self,
            _req: Request<DeleteWorkspaceRequest>,
        ) -> Result<Response<DeleteWorkspaceResponse>, Status> {
            Ok(Response::new(DeleteWorkspaceResponse { success: true }))
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cc_backend_uses_metadata_only_getattr_ranged_reads_and_subtree_skip_diffs() {
        let srv = MockSvc::default();

        let fid = vec![0xaa; 20];
        let payload = b"0123456789_COMMIT_CLOUD_RANGED_SLICE_XYZ".to_vec();
        srv.blobs.insert(fid.clone(), payload.clone());

        let unchanged_sub_tid = vec![0x11; 20];
        srv.trees.insert(
            unchanged_sub_tid.clone(),
            vec![make_proto_file_entry(
                "deep.txt",
                fid.clone(),
                false,
                payload.len() as u64,
            )],
        );

        let root_t1 = vec![0x22; 20];
        srv.trees.insert(
            root_t1.clone(),
            vec![
                make_proto_file_entry("data.bin", fid.clone(), true, payload.len() as u64),
                make_proto_tree_entry("unchanged", unchanged_sub_tid.clone()),
            ],
        );

        let root_t2 = vec![0x33; 20];
        srv.trees.insert(
            root_t2.clone(),
            vec![
                make_proto_file_entry("changed.bin", vec![0xbb; 20], false, 99),
                make_proto_file_entry("data.bin", fid.clone(), true, payload.len() as u64),
                make_proto_tree_entry("unchanged", unchanged_sub_tid.clone()),
            ],
        );

        let commit_id = vec![0x44; 20];
        let change_id = vec![0x55; 16];
        srv.commits.insert(
            commit_id.clone(),
            Commit {
                commit_id: commit_id.clone(),
                change_id: change_id.clone(),
                root_tree_id: vec![root_t1.clone()],
                ..Default::default()
            },
        );

        let addr = spawn_mock_server(srv.clone()).await;

        let cas_dir = tempfile::tempdir().unwrap();
        let cas = Arc::new(BlobCas::new(cas_dir.path(), 10 * 1024 * 1024).unwrap());
        let backend = CommitCloudVfsBackend::new(
            "corp",
            "webapp",
            format!("http://{addr}"),
            CcSourceAuth::default(),
            cas,
        );

        // 1. Resolve commit prefix (including 8-char prefix) and z-k change
        //    prefix
        let full_hex = hex::encode(&commit_id);
        assert_eq!(
            backend.resolve_commit(&full_hex[..8]).await.unwrap(),
            full_hex
        );
        assert_eq!(
            backend.resolve_commit(&full_hex[..12]).await.unwrap(),
            full_hex
        );
        let zk = encode_hex_to_reverse_hex(&hex::encode(&change_id)).unwrap();
        assert_eq!(
            backend.resolve_change(&zk[..10]).await.unwrap(),
            Some(full_hex.clone())
        );

        // 2. lookup_in_commit answers getattr from TreeEntry.size_bytes with
        //    ZERO ReadFile calls!
        let info = backend
            .lookup_in_commit(&full_hex, Path::new("data.bin"))
            .await
            .unwrap();
        assert_eq!(info.attr.size, payload.len() as u64);
        assert!(info.attr.executable);
        assert_eq!(srv.read_file_calls.load(Ordering::SeqCst), 0);

        // 3. read_file_range(offset=11, size=12) streams ONLY the requested 12
        //    bytes!
        let slice = backend
            .read_file_range(&full_hex, Path::new("data.bin"), 11, 12)
            .await
            .unwrap();
        assert_eq!(&*slice, b"COMMIT_CLOUD");
        assert_eq!(srv.read_file_calls.load(Ordering::SeqCst), 1);

        // 4. diff_trees skips identical `unchanged` subtree without calling
        //    ReadTree on it!
        let diff = backend
            .diff_trees(&hex::encode(&root_t1), &hex::encode(&root_t2))
            .await
            .unwrap();
        assert_eq!(diff, vec!["changed.bin"]);
        assert!(
            !srv.read_tree_calls
                .contains_key(&hex::encode(&unchanged_sub_tid)),
            "diff_trees must skip identical subtree IDs without ReadTree RPCs"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resolve_change_picks_visible_ancestor_over_stale_snapshots_and_handles_divergence() {
        let srv = MockSvc::default();
        let empty_tree = hex::decode(EMPTY_GIT_TREE_HEX).unwrap();

        // Rewritten change X: X1 (0xff..), X2 (0xee..), X3 (0x11..), followed
        // by child head Y. Note X1 sorts LAST lexicographically (`0xff > 0xee >
        // 0x11`), so returning `.last()` would pick the oldest stale snapshot
        // X1 instead of visible X3!
        let chg_x = vec![0x21; 16];
        let x1 = vec![0xff; 20];
        let x2 = vec![0xee; 20];
        let x3 = vec![0x11; 20];
        let head_y = vec![0x33; 20];
        srv.commits.insert(
            x1.clone(),
            Commit {
                commit_id: x1.clone(),
                change_id: chg_x.clone(),
                root_tree_id: vec![empty_tree.clone()],
                ..Default::default()
            },
        );
        srv.commits.insert(
            x2.clone(),
            Commit {
                commit_id: x2.clone(),
                change_id: chg_x.clone(),
                predecessors: vec![x1.clone()],
                root_tree_id: vec![empty_tree.clone()],
                ..Default::default()
            },
        );
        srv.commits.insert(
            x3.clone(),
            Commit {
                commit_id: x3.clone(),
                change_id: chg_x.clone(),
                predecessors: vec![x2.clone()],
                root_tree_id: vec![empty_tree.clone()],
                ..Default::default()
            },
        );
        srv.commits.insert(
            head_y.clone(),
            Commit {
                commit_id: head_y.clone(),
                change_id: vec![0x22; 16],
                parent_commit_ids: vec![x3.clone()],
                root_tree_id: vec![empty_tree.clone()],
                ..Default::default()
            },
        );

        // Abandoned change Z (neither in `head_ids` nor an ancestor of
        // `head_ids`).
        let chg_z = vec![0x41; 16];
        let z1 = vec![0x71; 20];
        let z2 = vec![0x72; 20];
        srv.commits.insert(
            z1.clone(),
            Commit {
                commit_id: z1.clone(),
                change_id: chg_z.clone(),
                root_tree_id: vec![empty_tree.clone()],
                ..Default::default()
            },
        );
        srv.commits.insert(
            z2.clone(),
            Commit {
                commit_id: z2.clone(),
                change_id: chg_z.clone(),
                predecessors: vec![z1],
                root_tree_id: vec![empty_tree.clone()],
                ..Default::default()
            },
        );

        // Divergent change D: two visible heads D2a and D2b rewriting D1.
        let chg_d = vec![0x61; 16];
        let d1 = vec![0x80; 20];
        let d2a = vec![0x81; 20];
        let d2b = vec![0x82; 20];
        srv.commits.insert(
            d1.clone(),
            Commit {
                commit_id: d1.clone(),
                change_id: chg_d.clone(),
                root_tree_id: vec![empty_tree.clone()],
                ..Default::default()
            },
        );
        for d_live in [&d2a, &d2b] {
            srv.commits.insert(
                d_live.clone(),
                Commit {
                    commit_id: d_live.clone(),
                    change_id: chg_d.clone(),
                    predecessors: vec![d1.clone()],
                    root_tree_id: vec![empty_tree.clone()],
                    ..Default::default()
                },
            );
        }

        *srv.active_view.lock() = Some(View {
            head_ids: vec![head_y, d2a, d2b],
            ..Default::default()
        });

        let addr = spawn_mock_server(srv).await;
        let cas_dir = tempfile::tempdir().unwrap();
        let cas = Arc::new(BlobCas::new(cas_dir.path(), 0).unwrap());
        let backend = CommitCloudVfsBackend::new(
            "corp",
            "webapp",
            format!("http://{addr}"),
            CcSourceAuth::default(),
            cas,
        );

        let zk_x = encode_hex_to_reverse_hex(&hex::encode(&chg_x)).unwrap();
        assert_eq!(
            backend.resolve_change(&zk_x[..10]).await.unwrap(),
            Some(hex::encode(&x3))
        );

        let zk_z = encode_hex_to_reverse_hex(&hex::encode(&chg_z)).unwrap();
        assert!(matches!(
            backend.resolve_change(&zk_z[..10]).await,
            Err(JjError::NotFound)
        ));

        let zk_d = encode_hex_to_reverse_hex(&hex::encode(&chg_d)).unwrap();
        assert_eq!(backend.resolve_change(&zk_d[..10]).await.unwrap(), None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn per_source_auth_refuses_remote_http_refreshes_cmd_token_and_maps_eacces() {
        assert!(is_bearer_allowed_for_url("https://cc.example.com:443"));
        assert!(is_bearer_allowed_for_url("http://127.0.0.1:8080"));
        assert!(is_bearer_allowed_for_url("http://[::1]:8080"));
        assert!(is_bearer_allowed_for_url("http://localhost:8080"));
        assert!(!is_bearer_allowed_for_url("http://cc.example.com:8080"));
        assert!(!is_bearer_allowed_for_url("http://10.0.0.12:8080"));

        let cas_dir = tempfile::tempdir().unwrap();
        let cas = Arc::new(BlobCas::new(cas_dir.path(), 0).unwrap());

        // 1. Non-loopback plaintext HTTP must refuse to transmit a configured
        //    Bearer token.
        let remote_http = CommitCloudVfsBackend::new(
            "corp",
            "webapp",
            "http://cc.example.com:8080",
            CcSourceAuth {
                auth_token_cmd: vec!["sh".into(), "-c".into(), "printf secret-tok".into()],
                ..Default::default()
            },
            cas.clone(),
        );
        let err = remote_http.list_bookmarks().await.unwrap_err();
        assert!(matches!(err, JjError::PermissionDenied(_)));
        assert_eq!(fuser::Errno::from(err), fuser::Errno::EACCES);

        // 2. Failing `auth_token_cmd` surfaces `PermissionDenied` (`EACCES`)
        //    with stderr detail.
        let srv = MockSvc::default();
        *srv.required_bearer.lock() = Some("fresh-tok".to_string());
        let addr = spawn_mock_server(srv.clone()).await;

        let bad_cmd = CommitCloudVfsBackend::new(
            "corp",
            "webapp",
            format!("http://{addr}"),
            CcSourceAuth {
                auth_token_cmd: vec![
                    "sh".into(),
                    "-c".into(),
                    "echo helper_boom >&2; exit 2".into(),
                ],
                ..Default::default()
            },
            cas.clone(),
        );
        let bad_err = bad_cmd.list_bookmarks().await.unwrap_err();
        match &bad_err {
            JjError::PermissionDenied(msg) => assert!(msg.contains("helper_boom"), "{msg}"),
            other => panic!("expected PermissionDenied, got {other:?}"),
        }

        // 3. Unauthenticated source sends NO authorization header (even if
        //    server checks it), and server `Unauthenticated` maps to
        //    `PermissionDenied` (`EACCES`) with server msg.
        let unauth_backend = CommitCloudVfsBackend::new(
            "oss",
            "webapp",
            format!("http://{addr}"),
            CcSourceAuth::default(),
            cas.clone(),
        );
        let unauth_err = unauth_backend.list_bookmarks().await.unwrap_err();
        match &unauth_err {
            JjError::PermissionDenied(msg) => {
                assert!(msg.contains("token expired or invalid"), "{msg}");
            }
            other => panic!("expected PermissionDenied, got {other:?}"),
        }
        assert_eq!(srv.seen_auth_headers.lock().last(), Some(&None));

        // 4. `auth_token_cmd` refreshes automatically when server returns
        //    `Unauthenticated`!
        let tok_dir = tempfile::tempdir().unwrap();
        let tok_file = tok_dir.path().join("tok.txt");
        std::fs::write(&tok_file, "stale-tok\n").unwrap();
        let refresh_backend = CommitCloudVfsBackend::new(
            "corp",
            "webapp",
            format!("http://{addr}"),
            CcSourceAuth {
                auth_token_cmd: vec!["cat".into(), tok_file.to_str().unwrap().to_string()],
                ..Default::default()
            },
            cas,
        );
        // Prime cache with `stale-tok` (fails against `fresh-tok`).
        assert!(matches!(
            refresh_backend.list_bookmarks().await,
            Err(JjError::PermissionDenied(_))
        ));
        // Rotate token on disk: next call hits cached `stale-tok`, gets
        // `Unauthenticated`, refreshes via `spawn_blocking`, and succeeds on
        // retry!
        std::fs::write(&tok_file, "fresh-tok\n").unwrap();
        assert!(refresh_backend.list_bookmarks().await.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn backend_caches_and_pinned_commits_evict_within_bounds() {
        let srv = MockSvc::default();
        for i in 1u8..=8 {
            let tid = vec![i; 20];
            let sid = vec![i + 0x20; 20];
            let cid = vec![i + 0x40; 20];
            srv.symlinks.insert(sid.clone(), format!("target-{i}"));
            srv.trees
                .insert(tid.clone(), vec![make_proto_symlink_entry("lnk", sid)]);
            srv.commits.insert(
                cid.clone(),
                Commit {
                    commit_id: cid,
                    change_id: vec![i; 16],
                    root_tree_id: vec![tid],
                    ..Default::default()
                },
            );
        }
        *srv.active_op_head.lock() = Some(cc_common::ROOT_OPERATION_ID_BYTES.to_vec());

        let addr = spawn_mock_server(srv).await;
        let cas_dir = tempfile::tempdir().unwrap();
        let cas = Arc::new(BlobCas::new(cas_dir.path(), 0).unwrap());
        let mut backend = CommitCloudVfsBackend::new(
            "corp",
            "webapp",
            format!("http://{addr}"),
            CcSourceAuth::default(),
            cas,
        );
        backend.limits = CacheLimits {
            commits: 3,
            trees: 3,
            tree_items: 10,
            symlinks: 3,
            pinned_commits: 4,
        };

        for i in 1u8..=8 {
            let cid_hex = hex::encode(vec![i + 0x40; 20]);
            // `lookup_in_commit` populates commit, tree, and symlink caches
            // without pinning.
            let _ = backend
                .lookup_in_commit(&cid_hex, Path::new("lnk"))
                .await
                .unwrap();
        }
        assert!(backend.commit_cache.lock().len() <= 3);
        assert!(backend.tree_cache.lock().len() <= 3);
        assert!(backend.symlink_cache.lock().len() <= 3);
        assert_eq!(
            backend.list_pinned_commits().await.unwrap(),
            vec![ROOT_COMMIT_HEX.to_string()],
            "plain commit reads must not grow pinned_commits"
        );

        for i in 1u8..=8 {
            let cid_hex = hex::encode(vec![i + 0x40; 20]);
            let _ = backend.resolve_commit(&cid_hex).await.unwrap();
        }
        assert_eq!(backend.list_pinned_commits().await.unwrap().len(), 4);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn list_workspaces_never_substitutes_empty_tree_when_read_commit_fails() {
        let srv = MockSvc::default();
        let valid_tid = vec![0x31; 20];
        let valid_cid = vec![0x32; 20];
        let missing_cid = vec![0x33; 20];
        srv.commits.insert(
            valid_cid.clone(),
            Commit {
                commit_id: valid_cid.clone(),
                change_id: vec![0x01; 16],
                root_tree_id: vec![valid_tid.clone()],
                ..Default::default()
            },
        );
        let mut wc_commit_ids = std::collections::HashMap::new();
        wc_commit_ids.insert("good".to_string(), valid_cid);
        wc_commit_ids.insert("dangling".to_string(), missing_cid);
        *srv.active_view.lock() = Some(View {
            wc_commit_ids,
            ..Default::default()
        });

        let addr = spawn_mock_server(srv.clone()).await;
        let cas_dir = tempfile::tempdir().unwrap();
        let cas = Arc::new(BlobCas::new(cas_dir.path(), 0).unwrap());
        let backend = CommitCloudVfsBackend::new(
            "corp",
            "webapp",
            format!("http://{addr}"),
            CcSourceAuth::default(),
            cas,
        );

        // Missing commit in `view.wc_commit_ids` is skipped rather than given
        // `EMPTY_GIT_TREE_HEX`.
        let listed = backend.list_workspaces(None).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].workspace_name, "good");
        assert_eq!(listed[0].tree_id_hex, hex::encode(&valid_tid));

        // Transient `UNAVAILABLE` propagates as an error rather than inventing
        // `EMPTY_GIT_TREE_HEX`.
        let backend_unavail = CommitCloudVfsBackend::new(
            "corp",
            "webapp",
            format!("http://{addr}"),
            CcSourceAuth::default(),
            backend.cas.clone(),
        );
        srv.read_commit_unavailable.store(true, Ordering::SeqCst);
        assert!(backend_unavail.list_workspaces(None).await.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ranged_read_file_validates_lengths_maps_eof_and_hydrates_legacy_blobs() {
        let srv = MockSvc::default();
        let fid_sized = vec![0x91; 20];
        let fid_legacy = vec![0x92; 20];
        srv.blobs
            .insert(fid_sized.clone(), b"0123456789abcdef".to_vec());
        srv.blobs
            .insert(fid_legacy.clone(), b"LEGACY_FULL_BLOB_PAYLOAD".to_vec());

        let tid = vec![0x93; 20];
        srv.trees.insert(
            tid.clone(),
            vec![
                ProtoTreeEntry {
                    name: "legacy.txt".into(),
                    value: Some(ProtoTreeValWrap {
                        value: Some(ProtoTreeValue::File(ProtoFile {
                            id: fid_legacy,
                            executable: false,
                            copy_id: Vec::new(),
                            size_bytes: None,
                        })),
                    }),
                },
                make_proto_file_entry("sized.txt", fid_sized, false, 16),
            ],
        );
        let cid = vec![0x94; 20];
        srv.commits.insert(
            cid.clone(),
            Commit {
                commit_id: cid.clone(),
                change_id: vec![0x95; 16],
                root_tree_id: vec![tid],
                ..Default::default()
            },
        );

        let addr = spawn_mock_server(srv.clone()).await;
        let cas_dir = tempfile::tempdir().unwrap();
        let cas = Arc::new(BlobCas::new(cas_dir.path(), 1024 * 1024).unwrap());
        let backend = CommitCloudVfsBackend::new(
            "corp",
            "webapp",
            format!("http://{addr}"),
            CcSourceAuth::default(),
            cas,
        );
        let cid_hex = hex::encode(&cid);

        // 1. Read at/past EOF returns empty slice.
        assert!(
            backend
                .read_file_range(&cid_hex, Path::new("sized.txt"), 16, 8)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            backend
                .read_file_range(&cid_hex, Path::new("sized.txt"), 100, 8)
                .await
                .unwrap()
                .is_empty()
        );

        // 2. Legacy server ignoring `offset`/`max_bytes`:
        //    - `legacy.txt` (`size_bytes == None`) hydrates full blob into CAS
        //      and slices locally!
        //    - `sized.txt` (`size_bytes == Some(16)`) rejects
        //      oversized/un-sliced response!
        srv.legacy_ignore_range.store(true, Ordering::SeqCst);
        let legacy_slice = backend
            .read_file_range(&cid_hex, Path::new("legacy.txt"), 7, 9)
            .await
            .unwrap();
        assert_eq!(&*legacy_slice, b"FULL_BLOB");

        // Oversized chunk (`16 > max_bytes=4`) and shifted small read (`16 !=
        // expected 6`) both error!
        assert!(
            backend
                .read_file_range(&cid_hex, Path::new("sized.txt"), 2, 4)
                .await
                .is_err()
        );
        assert!(
            backend
                .read_file_range(&cid_hex, Path::new("sized.txt"), 10, 32)
                .await
                .is_err()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stale_uds_socket_falls_back_to_direct_and_hung_rpc_times_out() {
        let srv = MockSvc::default();
        let cid = vec![0xa1; 20];
        srv.commits.insert(
            cid.clone(),
            Commit {
                commit_id: cid.clone(),
                change_id: vec![0xa2; 16],
                root_tree_id: vec![hex::decode(EMPTY_GIT_TREE_HEX).unwrap()],
                ..Default::default()
            },
        );
        let addr = spawn_mock_server(srv.clone()).await;

        // Create a bound-then-closed Unix socket path so `sock_path.exists()`
        // is true, simulating a crashed `jj-cc-daemon`.
        let sock_dir = tempfile::tempdir().unwrap();
        let stale_sock = sock_dir.path().join("daemon.sock");
        {
            let _listener = std::os::unix::net::UnixListener::bind(&stale_sock).unwrap();
        }
        assert!(stale_sock.exists());

        let cas_dir = tempfile::tempdir().unwrap();
        let cas = Arc::new(BlobCas::new(cas_dir.path(), 0).unwrap());
        let mut backend = CommitCloudVfsBackend::new(
            "corp",
            "webapp",
            format!("http://{addr}"),
            CcSourceAuth {
                daemon_socket: Some(stale_sock),
                ..Default::default()
            },
            cas,
        );
        backend.rpc_timeout = Duration::from_millis(80);

        let cid_hex = hex::encode(&cid);
        assert_eq!(
            backend.commit_root_tree_id(&cid_hex).await.unwrap(),
            EMPTY_GIT_TREE_HEX
        );

        // Now stall server RPCs beyond `rpc_timeout` and verify deadline
        // enforcement.
        *srv.rpc_delay.lock() = Some(Duration::from_millis(300));
        let err = backend.resolve_commit(&cid_hex[..8]).await.unwrap_err();
        match err {
            JjError::IO(io_err) => assert_eq!(io_err.kind(), std::io::ErrorKind::TimedOut),
            other => panic!("expected TimedOut IO error, got {other:?}"),
        }
    }
}
