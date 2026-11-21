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

//! Control-socket IPC protocol, configuration schema, mount-scope grammar, and
//! caller-identity seam shared between `jjfsd`, `jj-vfs`, and `VfsWorkingCopy`.
//!
//! # Authority and Crash-Safety Ordering
//! Source and mount configurations persist atomically via temp-file rename into
//! `$XDG_CONFIG_HOME/jj-vfs/config.toml` before control responses return `Ok`,
//! while ephemeral sources (`ephemeral = true`) stay strictly in daemon memory.
//!
//! # Concurrency and Merge Invariants
//! Each `jjfsd` instance owns a dedicated Unix domain socket (`control.sock`)
//! and state directory (`--instance` or `--state-dir`). Concurrent CLI and
//! `WorkingCopy` clients multiplex independent newline-delimited JSON
//! request/response frames.
//!
//! # Cross-Process and RPC Boundaries
//! Control sockets live inside `0700` per-user runtime directories.
//! `CallerContext` records the OS caller UID/GID on internal VFS calls to
//! preserve a clean trait seam for future Kubernetes node-level daemonset
//! deployments.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;
use tokio::io::AsyncBufRead;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::UnixStream;

pub const DEFAULT_MAX_CACHE_BYTES: u64 = 20 * 1024 * 1024 * 1024;
pub const MAX_CONTROL_FRAME_BYTES: usize = 1024 * 1024;
pub const DEFAULT_CONTROL_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Error, Debug)]
pub enum ProtoError {
    #[error("invalid mount target '{0}': {1}")]
    InvalidTarget(String, String),
    #[error("control socket I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON protocol error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("TOML config error: {0}")]
    TomlDe(#[from] toml::de::Error),
    #[error("TOML serialize error: {0}")]
    TomlSer(#[from] toml::ser::Error),
    #[error("daemon error: {0}")]
    Daemon(String),
}

/// Carries caller identity across VFS operations (`Option A` per-user default,
/// preserving a seam for future Kubernetes node-level multi-tenant daemonsets).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallerContext {
    pub uid: u32,
    pub gid: u32,
    pub pid: Option<u32>,
}

impl CallerContext {
    pub fn current_process() -> Self {
        #[cfg(unix)]
        {
            // SAFETY: getuid and getgid are thread-safe, infallible POSIX
            // queries.
            let uid = unsafe { libc_uid() };
            let gid = unsafe { libc_gid() };
            Self {
                uid,
                gid,
                pid: Some(std::process::id()),
            }
        }
        #[cfg(not(unix))]
        {
            Self {
                uid: 1000,
                gid: 1000,
                pid: Some(std::process::id()),
            }
        }
    }
}

#[cfg(unix)]
unsafe fn libc_uid() -> u32 {
    unsafe extern "C" {
        fn getuid() -> u32;
    }
    unsafe { getuid() }
}

#[cfg(unix)]
unsafe fn libc_gid() -> u32 {
    unsafe extern "C" {
        fn getgid() -> u32;
    }
    unsafe { getgid() }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SourceKind {
    CommitCloud,
    LocalJj,
    LocalGit,
    LocalDir,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceConfig {
    pub kind: SourceKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub daemon_socket: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_token_env: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub auth_token_cmd: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub search_paths: Vec<PathBuf>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub repos: BTreeMap<String, PathBuf>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub ephemeral: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonSection {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_mount: Option<PathBuf>,
    #[serde(default = "default_max_cache_bytes")]
    pub max_cache_bytes: u64,
    #[serde(default = "default_snapshot_debounce_ms")]
    pub snapshot_debounce_ms: u64,
}

fn default_max_cache_bytes() -> u64 {
    DEFAULT_MAX_CACHE_BYTES
}

fn default_snapshot_debounce_ms() -> u64 {
    2000
}

impl Default for DaemonSection {
    fn default() -> Self {
        Self {
            root_mount: None,
            max_cache_bytes: DEFAULT_MAX_CACHE_BYTES,
            snapshot_debounce_ms: 2000,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfiguredMount {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    pub mountpoint: PathBuf,
}

/// Top-level `$XDG_CONFIG_HOME/jj-vfs/config.toml` representation.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VfsConfigFile {
    #[serde(default)]
    pub daemon: DaemonSection,
    #[serde(default)]
    pub sources: BTreeMap<String, SourceConfig>,
    #[serde(default)]
    pub mounts: Vec<ConfiguredMount>,
}

impl VfsConfigFile {
    pub fn load_or_default(path: &Path) -> Result<Self, ProtoError> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let raw = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&raw)?)
    }

    pub fn save_atomic(&self, path: &Path) -> Result<(), ProtoError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut filtered = self.clone();
        filtered.sources.retain(|_, cfg| !cfg.ephemeral);
        let serialized = toml::to_string_pretty(&filtered)?;
        let tmp = path.with_extension(format!("toml.tmp.{}", std::process::id()));
        if let Err(err) = write_and_sync_file(&tmp, serialized.as_bytes())
            .and_then(|()| std::fs::rename(&tmp, path))
        {
            let _ = std::fs::remove_file(&tmp);
            return Err(err.into());
        }
        if let Some(parent) = path.parent()
            && let Ok(dir) = std::fs::File::open(parent)
        {
            let _ = dir.sync_all();
        }
        Ok(())
    }
}

fn write_and_sync_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    let mut file = std::fs::File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

/// Parsed mount scope for `jj vfs mount [<target>] <mountpoint>`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MountScope {
    /// Root Namespace Mount (`jj vfs mount ~/jj`) -> prefix `""`
    Root,
    /// Direct Source Mount (`jj vfs mount corp ~/corp`) -> prefix `<source>`
    Source { source: String },
    /// Direct User Mount (`jj vfs mount corp:@alice ~/alice`) -> prefix
    /// `<source>/users/<user>`
    User { source: String, user: String },
    /// Direct Repo Mount (`jj vfs mount corp:webapp ~/mnt/webapp`) -> prefix
    /// `<source>/repos/<repo>`
    Repo { source: String, repo: String },
    /// Direct Workspace Mount (`jj vfs mount corp:webapp/default ~/work/app` or
    /// `corp:webapp@alice/default`)
    Workspace {
        source: String,
        repo: String,
        user: Option<String>,
        workspace: String,
    },
}

impl MountScope {
    /// Parses `<target>` grammar from `docs/design.md`:
    /// - `None` or `""` -> `MountScope::Root`
    /// - `<source>` -> `MountScope::Source`
    /// - `<source>:@<user>` -> `MountScope::User`
    /// - `<source>:<repo>` -> `MountScope::Repo`
    /// - `<source>:<repo>/<ws>` -> `MountScope::Workspace { user: None, .. }`
    /// - `<source>:<repo>@<user>/<ws>` -> `MountScope::Workspace { user:
    ///   Some(user), .. }`
    pub fn parse(target: Option<&str>) -> Result<Self, ProtoError> {
        let Some(raw) = target.map(str::trim).filter(|s| !s.is_empty()) else {
            return Ok(Self::Root);
        };
        if let Some((source, rest)) = raw.split_once(':') {
            validate_segment(source, raw)?;
            if rest.is_empty() {
                return Err(ProtoError::InvalidTarget(
                    raw.to_string(),
                    "missing target after ':'".to_string(),
                ));
            }
            if let Some(user) = rest.strip_prefix('@') {
                validate_segment(user, raw)?;
                if user.contains('/') {
                    return Err(ProtoError::InvalidTarget(
                        raw.to_string(),
                        "user scope '<source>:@<user>' must not contain '/'".to_string(),
                    ));
                }
                return Ok(Self::User {
                    source: source.to_string(),
                    user: user.to_string(),
                });
            }
            if let Some((repo_and_user, ws)) = rest.split_once('/') {
                validate_segment(ws, raw)?;
                if let Some((repo, user)) = repo_and_user.split_once('@') {
                    validate_segment(repo, raw)?;
                    validate_segment(user, raw)?;
                    return Ok(Self::Workspace {
                        source: source.to_string(),
                        repo: repo.to_string(),
                        user: Some(user.to_string()),
                        workspace: ws.to_string(),
                    });
                }
                validate_segment(repo_and_user, raw)?;
                return Ok(Self::Workspace {
                    source: source.to_string(),
                    repo: repo_and_user.to_string(),
                    user: None,
                    workspace: ws.to_string(),
                });
            }
            validate_segment(rest, raw)?;
            Ok(Self::Repo {
                source: source.to_string(),
                repo: rest.to_string(),
            })
        } else {
            validate_segment(raw, raw)?;
            Ok(Self::Source {
                source: raw.to_string(),
            })
        }
    }

    /// Maps this mount scope to its canonical interior path inside
    /// `NamespaceRouter`.
    pub fn router_prefix(&self, default_user: Option<&str>) -> PathBuf {
        match self {
            Self::Root => PathBuf::new(),
            Self::Source { source } => PathBuf::from(source),
            Self::User { source, user } => PathBuf::from(source).join("users").join(user),
            Self::Repo { source, repo } => PathBuf::from(source)
                .join("repos")
                .join(encode_repo_segment(repo)),
            Self::Workspace {
                source,
                repo,
                user,
                workspace,
            } => {
                let base = PathBuf::from(source)
                    .join("repos")
                    .join(encode_repo_segment(repo));
                if let Some(u) = user.as_deref().or(default_user) {
                    base.join("users").join(u).join(workspace)
                } else {
                    base.join("workspaces").join(workspace)
                }
            }
        }
    }
}

pub fn encode_repo_segment(repo: &str) -> String {
    repo.replace('/', "__")
}

fn validate_segment(seg: &str, full: &str) -> Result<(), ProtoError> {
    if seg.is_empty() || seg == "." || seg == ".." || seg.contains('\0') {
        return Err(ProtoError::InvalidTarget(
            full.to_string(),
            format!("invalid path segment '{seg}'"),
        ));
    }
    Ok(())
}

/// Identifies a canonical workspace across registered sources.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct WorkspaceKey {
    pub source: String,
    pub repo: String,
    pub user: String,
    pub workspace: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveMountInfo {
    pub mountpoint: PathBuf,
    pub scope: MountScope,
    pub router_prefix: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonStatus {
    pub pid: u32,
    pub instance: String,
    pub state_dir: PathBuf,
    pub cache_dir: PathBuf,
    pub config_path: PathBuf,
    pub cached_blobs: usize,
    pub cached_bytes: u64,
    pub max_cache_bytes: u64,
    pub sources: BTreeMap<String, SourceConfig>,
    pub mounts: Vec<ActiveMountInfo>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotResult {
    pub workspace: WorkspaceKey,
    pub dirty_paths_processed: usize,
    pub tree_id_hex: String,
    pub commit_id_hex: Option<String>,
    pub operation_id_hex: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub captured_seq: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckoutTreeResult {
    pub workspace: WorkspaceKey,
    pub old_tree_id_hex: String,
    pub new_tree_id_hex: String,
    pub invalidated_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_id_hex: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ControlRequest {
    Ping,
    Status,
    Shutdown,
    AddSource {
        alias: String,
        config: SourceConfig,
    },
    RemoveSource {
        alias: String,
        force: bool,
    },
    ListSources,
    Mount {
        target: Option<String>,
        mountpoint: PathBuf,
        persist: bool,
    },
    Unmount {
        mountpoint: PathBuf,
    },
    SnapshotWorkspace {
        workspace: WorkspaceKey,
    },
    CheckoutTree {
        workspace: WorkspaceKey,
        old_tree_id_hex: Option<String>,
        new_tree_id_hex: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        new_tree_ids_hex: Vec<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        conflict_labels: Vec<String>,
        new_commit_id_hex: Option<String>,
        new_operation_id_hex: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        captured_seq: Option<u64>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ControlResponse {
    Pong {
        pid: u32,
    },
    DaemonStatus(Box<DaemonStatus>),
    Sources {
        sources: BTreeMap<String, SourceConfig>,
    },
    Mounted(ActiveMountInfo),
    Unmounted {
        mountpoint: PathBuf,
    },
    Snapshotted(SnapshotResult),
    CheckedOut(CheckoutTreeResult),
    Ok {
        message: String,
    },
    Error {
        code: String,
        message: String,
    },
}

/// Resolves state, cache, config, and socket paths for `--instance` or
/// `--state-dir`.
#[derive(Clone, Debug)]
pub struct InstancePaths {
    pub instance: String,
    pub state_dir: PathBuf,
    pub cache_dir: PathBuf,
    pub config_path: PathBuf,
    pub socket_path: PathBuf,
}

impl InstancePaths {
    pub fn resolve(instance: Option<&str>, state_dir_override: Option<&Path>) -> Self {
        Self::resolve_with_env(instance, state_dir_override, |key| std::env::var_os(key))
    }

    #[doc(hidden)]
    pub fn resolve_with_env<F>(
        instance: Option<&str>,
        state_dir_override: Option<&Path>,
        mut env_lookup: F,
    ) -> Self
    where
        F: FnMut(&str) -> Option<OsString>,
    {
        let inst = instance
            .map(ToOwned::to_owned)
            .or_else(|| env_lookup("JJ_VFS_INSTANCE").and_then(|v| v.into_string().ok()))
            .unwrap_or_else(|| "default".to_string());

        if let Some(explicit) = state_dir_override
            .map(Path::to_path_buf)
            .or_else(|| env_lookup("JJ_VFS_STATE_DIR").map(PathBuf::from))
        {
            return Self {
                instance: inst,
                socket_path: explicit.join("control.sock"),
                cache_dir: explicit.join("cache"),
                config_path: explicit.join("config.toml"),
                state_dir: explicit,
            };
        }

        let home = env_lookup("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/tmp"));
        let xdg_state = env_lookup("XDG_STATE_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".local/state"));
        let xdg_cache = env_lookup("XDG_CACHE_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".cache"));
        let xdg_config = env_lookup("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".config"));
        let xdg_runtime = env_lookup("XDG_RUNTIME_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| xdg_state.clone());

        let suffix = if inst == "default" {
            PathBuf::from("jj-vfs")
        } else {
            PathBuf::from("jj-vfs").join(&inst)
        };

        let runtime_dir = xdg_runtime.join(&suffix);
        Self {
            instance: inst,
            state_dir: xdg_state.join(&suffix),
            cache_dir: xdg_cache.join(&suffix),
            config_path: xdg_config.join(&suffix).join("config.toml"),
            socket_path: runtime_dir.join("control.sock"),
        }
    }

    pub fn ensure_dirs(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.state_dir)?;
        std::fs::create_dir_all(&self.cache_dir)?;
        if let Some(cfg_parent) = self.config_path.parent() {
            std::fs::create_dir_all(cfg_parent)?;
        }
        if let Some(sock_parent) = self.socket_path.parent() {
            std::fs::create_dir_all(sock_parent)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(sock_parent, std::fs::Permissions::from_mode(0o700))?;
            }
        }
        Ok(())
    }
}

/// Reads a single newline-delimited UTF-8 frame up to `max_bytes` bytes,
/// rejecting oversized frames before allocating unbounded buffers.
pub async fn read_bounded_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    max_bytes: usize,
) -> std::io::Result<Option<String>> {
    let mut buf: Vec<u8> = Vec::new();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            if buf.is_empty() {
                return Ok(None);
            }
            if buf.last() == Some(&b'\r') {
                buf.pop();
            }
            return String::from_utf8(buf).map(Some).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid UTF-8: {e}"),
                )
            });
        }
        if let Some(pos) = available.iter().position(|&b| b == b'\n') {
            if buf.len().saturating_add(pos) > max_bytes {
                reader.consume(pos + 1);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("control frame exceeds {max_bytes} byte limit"),
                ));
            }
            buf.extend_from_slice(&available[..pos]);
            reader.consume(pos + 1);
            if buf.last() == Some(&b'\r') {
                buf.pop();
            }
            return String::from_utf8(buf).map(Some).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid UTF-8: {e}"),
                )
            });
        }
        if buf.len().saturating_add(available.len()) > max_bytes {
            let len = available.len();
            reader.consume(len);
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("control frame exceeds {max_bytes} byte limit"),
            ));
        }
        let len = available.len();
        buf.extend_from_slice(available);
        reader.consume(len);
    }
}

/// Sends a single `ControlRequest` over `socket_path` and awaits its
/// `ControlResponse` within `DEFAULT_CONTROL_TIMEOUT`.
pub async fn send_control_request(
    socket_path: &Path,
    request: &ControlRequest,
) -> Result<ControlResponse, ProtoError> {
    send_control_request_with_timeout(socket_path, request, DEFAULT_CONTROL_TIMEOUT).await
}

/// Sends a single `ControlRequest` over `socket_path` and awaits its
/// `ControlResponse` within `timeout`.
pub async fn send_control_request_with_timeout(
    socket_path: &Path,
    request: &ControlRequest,
    timeout: Duration,
) -> Result<ControlResponse, ProtoError> {
    tokio::time::timeout(timeout, async {
        let stream = UnixStream::connect(socket_path).await?;
        let (reader, mut writer) = stream.into_split();
        let mut payload = serde_json::to_vec(request)?;
        if payload.len() > MAX_CONTROL_FRAME_BYTES {
            return Err(ProtoError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("control request exceeds {MAX_CONTROL_FRAME_BYTES} byte limit"),
            )));
        }
        payload.push(b'\n');
        writer.write_all(&payload).await?;
        writer.flush().await?;

        let mut reader = BufReader::new(reader);
        let line = read_bounded_line(&mut reader, MAX_CONTROL_FRAME_BYTES)
            .await?
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "jjfsd closed control connection without replying",
                )
            })?;
        let resp: ControlResponse = serde_json::from_str(&line)?;
        Ok(resp)
    })
    .await
    .map_err(|_| {
        ProtoError::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!(
                "timed out after {timeout:?} waiting for jjfsd control socket at {}",
                socket_path.display()
            ),
        ))
    })?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_all_documented_mount_scopes() {
        assert_eq!(MountScope::parse(None).unwrap(), MountScope::Root);
        assert_eq!(
            MountScope::parse(Some("corp")).unwrap(),
            MountScope::Source {
                source: "corp".into()
            }
        );
        assert_eq!(
            MountScope::parse(Some("corp:@alice")).unwrap(),
            MountScope::User {
                source: "corp".into(),
                user: "alice".into()
            }
        );
        assert_eq!(
            MountScope::parse(Some("corp:webapp")).unwrap(),
            MountScope::Repo {
                source: "corp".into(),
                repo: "webapp".into()
            }
        );
        assert_eq!(
            MountScope::parse(Some("corp:webapp/default")).unwrap(),
            MountScope::Workspace {
                source: "corp".into(),
                repo: "webapp".into(),
                user: None,
                workspace: "default".into(),
            }
        );
        assert_eq!(
            MountScope::parse(Some("corp:webapp@bob/fix-auth")).unwrap(),
            MountScope::Workspace {
                source: "corp".into(),
                repo: "webapp".into(),
                user: Some("bob".into()),
                workspace: "fix-auth".into(),
            }
        );
    }

    #[test]
    fn router_prefixes_match_option_c_namespace() {
        let ws = MountScope::parse(Some("corp:webapp/default")).unwrap();
        assert_eq!(
            ws.router_prefix(Some("alice")),
            PathBuf::from("corp/repos/webapp/users/alice/default")
        );
        assert_eq!(
            ws.router_prefix(None),
            PathBuf::from("corp/repos/webapp/workspaces/default")
        );
        let user_view = MountScope::parse(Some("corp:@alice")).unwrap();
        assert_eq!(
            user_view.router_prefix(None),
            PathBuf::from("corp/users/alice")
        );
    }
    #[tokio::test]
    async fn read_bounded_line_caps_frame_size_and_send_control_request_times_out() {
        let ok_data = b"hello\nworld\n";
        let mut reader = BufReader::new(&ok_data[..]);
        assert_eq!(
            read_bounded_line(&mut reader, 8).await.unwrap().as_deref(),
            Some("hello")
        );
        assert_eq!(
            read_bounded_line(&mut reader, 8).await.unwrap().as_deref(),
            Some("world")
        );
        assert_eq!(read_bounded_line(&mut reader, 8).await.unwrap(), None);

        let too_long = b"0123456789\n";
        let mut reader = BufReader::new(&too_long[..]);
        let err = read_bounded_line(&mut reader, 5).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

        let dir = std::env::temp_dir().join(format!("vfs-proto-timeout-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let sock = dir.join("wedged.sock");
        let listener = tokio::net::UnixListener::bind(&sock).unwrap();
        let _accept_hold = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let res = send_control_request_with_timeout(
            &sock,
            &ControlRequest::Ping,
            Duration::from_millis(60),
        )
        .await;
        let _ = std::fs::remove_dir_all(&dir);
        match res {
            Err(ProtoError::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::TimedOut),
            other => panic!("expected TimedOut error, got {other:?}"),
        }
    }

    #[test]
    fn instance_paths_uses_xdg_runtime_dir_unless_state_dir_override_is_given() {
        let env = |k: &str| -> Option<OsString> {
            match k {
                "HOME" => Some("/home/alice".into()),
                "XDG_STATE_HOME" => Some("/xdg/state".into()),
                "XDG_CACHE_HOME" => Some("/xdg/cache".into()),
                "XDG_CONFIG_HOME" => Some("/xdg/config".into()),
                "XDG_RUNTIME_DIR" => Some("/run/user/1000".into()),
                _ => None,
            }
        };
        let default_paths = InstancePaths::resolve_with_env(None, None, env);
        assert_eq!(default_paths.state_dir, PathBuf::from("/xdg/state/jj-vfs"));
        assert_eq!(default_paths.cache_dir, PathBuf::from("/xdg/cache/jj-vfs"));
        assert_eq!(
            default_paths.config_path,
            PathBuf::from("/xdg/config/jj-vfs/config.toml")
        );
        assert_eq!(
            default_paths.socket_path,
            PathBuf::from("/run/user/1000/jj-vfs/control.sock")
        );

        let overridden =
            InstancePaths::resolve_with_env(None, Some(Path::new("/explicit/state")), env);
        assert_eq!(overridden.state_dir, PathBuf::from("/explicit/state"));
        assert_eq!(
            overridden.socket_path,
            PathBuf::from("/explicit/state/control.sock")
        );
        assert_eq!(
            overridden.config_path,
            PathBuf::from("/explicit/state/config.toml")
        );
    }
}
