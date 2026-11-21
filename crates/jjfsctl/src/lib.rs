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

//! Reusable CLI command definitions and control-socket client logic shared by
//! standalone `jj-vfs` (`jjfsctl`) and the integrated `jj vfs` custom
//! subcommand.

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::process::Stdio;
use std::time::Duration;

use clap::Args;
use clap::Subcommand;
use jjfsd::ipc_server::is_instance_lock_held;
use vfs_proto::ControlRequest;
use vfs_proto::ControlResponse;
use vfs_proto::InstancePaths;
use vfs_proto::MountScope;
use vfs_proto::SourceConfig;
use vfs_proto::SourceKind;
use vfs_proto::WorkspaceKey;
use vfs_proto::send_control_request;
use vfs_proto::send_control_request_with_timeout;

#[derive(Args, Clone, Debug)]
pub struct VfsCommandArgs {
    /// Named jjfsd instance (defaults to "default" or JJ_VFS_INSTANCE)
    #[arg(long, global = true)]
    pub instance: Option<String>,

    /// Explicit state directory override (defaults to XDG dirs or
    /// JJ_VFS_STATE_DIR)
    #[arg(long, global = true)]
    pub state_dir: Option<PathBuf>,

    #[command(subcommand)]
    pub command: VfsSubcommands,
}

#[derive(Subcommand, Clone, Debug)]
pub enum VfsSubcommands {
    /// Manage the per-user `jjfsd` background daemon
    Daemon {
        #[command(subcommand)]
        action: DaemonAction,
    },
    /// Register, inspect, or remove Commit Cloud and local repository sources
    Source {
        #[command(subcommand)]
        action: SourceAction,
    },
    /// Mount the multi-source root namespace or a scoped direct target (`jj vfs
    /// mount [<target>] <mountpoint>`)
    Mount {
        /// Either `[<mountpoint>]` (Root Namespace Mount) or `[<target>,
        /// <mountpoint>]`
        #[arg(required = true, num_args = 1..=2)]
        positional: Vec<String>,
        /// Persist mount entry in config.toml
        #[arg(long)]
        persist: bool,
    },
    /// Flush dirty workspace snapshots and detach a FUSE mountpoint
    Unmount { mountpoint: PathBuf },
    /// Trigger an immediate $O(k)$ dirty-path snapshot on a mounted workspace
    Snapshot {
        /// Target `<source>:<repo>/<ws>` or `<source>:<repo>@<user>/<ws>`
        target: String,
    },
    /// Print resolved instance directories and control socket path
    Paths {
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand, Clone, Debug)]
pub enum DaemonAction {
    /// Start `jjfsd` in the background if not already listening
    Start,
    /// Query live `jjfsd` PID, cache usage, registered sources, and active
    /// mounts
    Status {
        #[arg(long)]
        json: bool,
    },
    /// Gracefully flush mounts and stop `jjfsd`
    Stop,
}

#[derive(Subcommand, Clone, Debug)]
pub enum SourceAction {
    /// Register a Commit Cloud server or local repository source
    Add {
        alias: String,
        /// Remote Commit Cloud gRPC URL (e.g. http://127.0.0.1:8080)
        #[arg(long)]
        server: Option<String>,
        /// Local jj or Git repository path
        #[arg(long)]
        path: Option<PathBuf>,
        /// Local directory to scan for `.jj` / `.git` repositories
        #[arg(long = "search-path")]
        search_paths: Vec<PathBuf>,
        /// Treat `--path` as a plain Git repository (`local-git`)
        #[arg(long)]
        git: bool,
        /// Optional `jj-cc-daemon` Unix domain socket path
        #[arg(long)]
        daemon_socket: Option<PathBuf>,
        /// Default workspace owner email / username on this source
        #[arg(long)]
        user: Option<String>,
        /// Environment variable holding Bearer token for this source
        #[arg(long)]
        token_env: Option<String>,
        /// Keep source in daemon memory only without writing `config.toml`
        #[arg(long)]
        ephemeral: bool,
    },
    /// List registered sources
    List {
        #[arg(long)]
        json: bool,
    },
    /// Remove a registered source (returns EBUSY if mounted/dirty without
    /// `--force`)
    Remove {
        alias: String,
        #[arg(long)]
        force: bool,
    },
}

pub async fn execute_vfs_command(args: &VfsCommandArgs) -> anyhow::Result<()> {
    let explicit_state_dir = args.state_dir.as_deref().map(absolutize).transpose()?;
    let paths = InstancePaths::resolve(args.instance.as_deref(), explicit_state_dir.as_deref());
    match &args.command {
        VfsSubcommands::Paths { json } => {
            let max_msg = cc_common::MAX_GRPC_MESSAGE_BYTES;
            let pooled = cc_lib::util::cached_channel_count();
            if *json {
                println!(
                    "{}",
                    serde_json::json!({
                        "instance": paths.instance,
                        "state_dir": paths.state_dir,
                        "cache_dir": paths.cache_dir,
                        "config_path": paths.config_path,
                        "socket_path": paths.socket_path,
                        "max_grpc_message_bytes": max_msg,
                        "pooled_channels": pooled,
                    })
                );
            } else {
                println!("instance:    {}", paths.instance);
                println!("state_dir:   {}", paths.state_dir.display());
                println!("cache_dir:   {}", paths.cache_dir.display());
                println!("config_path: {}", paths.config_path.display());
                println!("socket_path: {}", paths.socket_path.display());
            }
            Ok(())
        }
        VfsSubcommands::Daemon { action } => match action {
            DaemonAction::Start => {
                let pid =
                    ensure_daemon_running_with_override(&paths, explicit_state_dir.as_deref())
                        .await?;
                println!(
                    "jjfsd (instance '{}', pid {pid}) is listening on {}",
                    paths.instance,
                    paths.socket_path.display()
                );
                Ok(())
            }
            DaemonAction::Status { json } => {
                match send_control_request(&paths.socket_path, &ControlRequest::Status).await {
                    Ok(ControlResponse::DaemonStatus(st)) => {
                        if *json {
                            println!("{}", serde_json::to_string_pretty(&st)?);
                        } else {
                            println!("Instance:     {} (pid {})", st.instance, st.pid);
                            println!("Socket:       {}", paths.socket_path.display());
                            println!(
                                "Blob CAS:     {} blobs ({} bytes / {} max)",
                                st.cached_blobs, st.cached_bytes, st.max_cache_bytes
                            );
                            println!("Sources ({}):", st.sources.len());
                            for (alias, cfg) in &st.sources {
                                println!("  - {alias} ({:?})", cfg.kind);
                            }
                            println!("Active mounts ({}):", st.mounts.len());
                            for m in &st.mounts {
                                println!("  - {} -> {:?}", m.mountpoint.display(), m.router_prefix);
                            }
                        }
                        Ok(())
                    }
                    Ok(ControlResponse::Error { message, .. }) => anyhow::bail!(message),
                    _ => {
                        if *json {
                            println!("{}", serde_json::json!({ "running": false }));
                        } else {
                            println!("jjfsd (instance '{}') is STOPPED", paths.instance);
                        }
                        Ok(())
                    }
                }
            }
            DaemonAction::Stop => {
                if !paths.socket_path.exists() {
                    println!("jjfsd is not running.");
                    return Ok(());
                }
                match send_control_request(&paths.socket_path, &ControlRequest::Shutdown).await {
                    Ok(ControlResponse::Ok { message }) => println!("{message}"),
                    Ok(ControlResponse::Error { message, .. }) => anyhow::bail!(message),
                    _ => println!("jjfsd stopped."),
                }
                Ok(())
            }
        },
        VfsSubcommands::Source { action } => match action {
            SourceAction::Add {
                alias,
                server,
                path,
                search_paths,
                git,
                daemon_socket,
                user,
                token_env,
                ephemeral,
            } => {
                let config = build_source_config(SourceAddOptions {
                    server: server.clone(),
                    path: path.clone(),
                    search_paths: search_paths.clone(),
                    git: *git,
                    daemon_socket: daemon_socket.clone(),
                    user: user.clone(),
                    token_env: token_env.clone(),
                    ephemeral: *ephemeral,
                })?;
                let _ = ensure_daemon_running_with_override(&paths, explicit_state_dir.as_deref())
                    .await?;
                let resp = send_control_request(
                    &paths.socket_path,
                    &ControlRequest::AddSource {
                        alias: alias.clone(),
                        config,
                    },
                )
                .await?;
                expect_ok(resp)
            }
            SourceAction::List { json } => {
                let _ = ensure_daemon_running_with_override(&paths, explicit_state_dir.as_deref())
                    .await?;
                let resp =
                    send_control_request(&paths.socket_path, &ControlRequest::ListSources).await?;
                match resp {
                    ControlResponse::Sources { sources } => {
                        if *json {
                            println!("{}", serde_json::to_string_pretty(&sources)?);
                        } else if sources.is_empty() {
                            println!("No sources registered.");
                        } else {
                            for (alias, cfg) in sources {
                                let detail = cfg
                                    .url
                                    .or_else(|| cfg.path.map(|p| p.display().to_string()))
                                    .unwrap_or_else(|| format!("{:?}", cfg.search_paths));
                                println!("{alias}\t{:?}\t{detail}", cfg.kind);
                            }
                        }
                        Ok(())
                    }
                    ControlResponse::Error { message, .. } => anyhow::bail!(message),
                    other => anyhow::bail!("unexpected response: {other:?}"),
                }
            }
            SourceAction::Remove { alias, force } => {
                let _ = ensure_daemon_running_with_override(&paths, explicit_state_dir.as_deref())
                    .await?;
                let resp = send_control_request(
                    &paths.socket_path,
                    &ControlRequest::RemoveSource {
                        alias: alias.clone(),
                        force: *force,
                    },
                )
                .await?;
                expect_ok(resp)
            }
        },
        VfsSubcommands::Mount {
            positional,
            persist,
        } => {
            let (target, mountpoint) = match positional.as_slice() {
                [mp] => (None, PathBuf::from(mp)),
                [tgt, mp] => (Some(tgt.clone()), PathBuf::from(mp)),
                _ => anyhow::bail!("expected `jj vfs mount [<target>] <mountpoint>`"),
            };
            let abs_mp = absolutize(&mountpoint)?;
            std::fs::create_dir_all(&abs_mp)?;
            let _ =
                ensure_daemon_running_with_override(&paths, explicit_state_dir.as_deref()).await?;
            let resp = send_control_request(
                &paths.socket_path,
                &ControlRequest::Mount {
                    target,
                    mountpoint: abs_mp,
                    persist: *persist,
                },
            )
            .await?;
            match resp {
                ControlResponse::Mounted(info) => {
                    println!("Mounted {:?} at {}", info.scope, info.mountpoint.display());
                    Ok(())
                }
                ControlResponse::Error { message, .. } => anyhow::bail!(message),
                other => anyhow::bail!("unexpected mount response: {other:?}"),
            }
        }
        VfsSubcommands::Unmount { mountpoint } => {
            let abs_mp = absolutize(mountpoint)?;
            let resp = send_control_request(
                &paths.socket_path,
                &ControlRequest::Unmount { mountpoint: abs_mp },
            )
            .await?;
            match resp {
                ControlResponse::Unmounted { mountpoint } => {
                    println!("Unmounted {}", mountpoint.display());
                    Ok(())
                }
                ControlResponse::Error { message, .. } => anyhow::bail!(message),
                other => anyhow::bail!("unexpected unmount response: {other:?}"),
            }
        }
        VfsSubcommands::Snapshot { target } => {
            let scope = MountScope::parse(Some(target))?;
            let MountScope::Workspace {
                source,
                repo,
                user,
                workspace,
            } = scope
            else {
                anyhow::bail!("snapshot target must name a workspace (<source>:<repo>/<ws>)");
            };
            let resp = send_control_request(
                &paths.socket_path,
                &ControlRequest::SnapshotWorkspace {
                    workspace: WorkspaceKey {
                        source,
                        repo,
                        user: user.unwrap_or_default(),
                        workspace,
                    },
                    publish_op: true,
                },
            )
            .await?;
            match resp {
                ControlResponse::Snapshotted(res) => {
                    println!(
                        "Snapshotted {} dirty path(s) -> tree {}",
                        res.dirty_paths_processed, res.tree_id_hex
                    );
                    Ok(())
                }
                ControlResponse::Error { message, .. } => anyhow::bail!(message),
                other => anyhow::bail!("unexpected snapshot response: {other:?}"),
            }
        }
    }
}

struct SourceAddOptions {
    server: Option<String>,
    path: Option<PathBuf>,
    search_paths: Vec<PathBuf>,
    git: bool,
    daemon_socket: Option<PathBuf>,
    user: Option<String>,
    token_env: Option<String>,
    ephemeral: bool,
}

fn build_source_config(opts: SourceAddOptions) -> anyhow::Result<SourceConfig> {
    let kind = if opts.server.is_some() {
        SourceKind::CommitCloud
    } else if !opts.search_paths.is_empty() {
        SourceKind::LocalDir
    } else if let Some(p) = &opts.path {
        if opts.git
            || (!p.join(".jj").exists() && (p.join(".git").exists() || p.join("HEAD").is_file()))
        {
            SourceKind::LocalGit
        } else {
            SourceKind::LocalJj
        }
    } else {
        anyhow::bail!("specify one of `--server <url>`, `--path <dir>`, or `--search-path <dir>`");
    };

    let norm_path = opts.path.as_deref().map(absolutize).transpose()?;
    let norm_daemon_sock = opts.daemon_socket.as_deref().map(absolutize).transpose()?;
    let norm_search: Vec<PathBuf> = opts
        .search_paths
        .into_iter()
        .map(|p| absolutize(&p))
        .collect::<anyhow::Result<_>>()?;

    Ok(SourceConfig {
        kind,
        url: opts.server,
        daemon_socket: norm_daemon_sock,
        user: opts.user,
        auth_token_env: opts.token_env,
        auth_token_cmd: Vec::new(),
        path: norm_path,
        search_paths: norm_search,
        repos: BTreeMap::new(),
        ephemeral: opts.ephemeral,
    })
}

fn expect_ok(resp: ControlResponse) -> anyhow::Result<()> {
    match resp {
        ControlResponse::Ok { message } => {
            println!("{message}");
            Ok(())
        }
        ControlResponse::Error { code, message } => anyhow::bail!("[{code}] {message}"),
        other => anyhow::bail!("unexpected control response: {other:?}"),
    }
}

fn absolutize(path: &Path) -> anyhow::Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    Ok(std::env::current_dir()?.join(path))
}

fn build_daemon_command(
    daemon_bin: &Path,
    instance: &str,
    state_dir_override: Option<&Path>,
) -> Command {
    let mut cmd = Command::new(daemon_bin);
    cmd.arg("--instance").arg(instance);
    // Pass `--state-dir` ONLY when an explicit override was supplied so `jjfsd`
    // resolves XDG directories (`XDG_RUNTIME_DIR`, `XDG_CONFIG_HOME`,
    // `XDG_CACHE_HOME`) identically.
    if let Some(explicit_state) = state_dir_override {
        cmd.arg("--state-dir").arg(explicit_state);
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;
        cmd.process_group(0);
    }
    cmd
}

fn read_log_tail(log_path: &Path) -> String {
    let Ok(raw) = std::fs::read_to_string(log_path) else {
        return String::new();
    };
    let lines: Vec<&str> = raw.lines().collect();
    let start = lines.len().saturating_sub(20);
    lines[start..].join("\n")
}

pub async fn ensure_daemon_running(paths: &InstancePaths) -> anyhow::Result<u32> {
    ensure_daemon_running_with_override(paths, None).await
}

pub async fn ensure_daemon_running_with_override(
    paths: &InstancePaths,
    state_dir_override: Option<&Path>,
) -> anyhow::Result<u32> {
    let daemon_bin = locate_jjfsd_binary()?;
    ensure_daemon_running_inner(paths, state_dir_override, &daemon_bin).await
}

async fn ensure_daemon_running_inner(
    paths: &InstancePaths,
    state_dir_override: Option<&Path>,
    daemon_bin: &Path,
) -> anyhow::Result<u32> {
    const PING_TIMEOUT: Duration = Duration::from_millis(500);

    if let Ok(ControlResponse::Pong { pid }) =
        send_control_request_with_timeout(&paths.socket_path, &ControlRequest::Ping, PING_TIMEOUT)
            .await
    {
        return Ok(pid);
    }

    paths.ensure_dirs()?;
    let log_path = paths.state_dir.join("jjfsd.log");
    let log_out = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .map_err(|e| anyhow::anyhow!("failed to open daemon log {}: {e}", log_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = log_out.set_permissions(std::fs::Permissions::from_mode(0o600));
    }
    let log_err = log_out.try_clone()?;

    let mut cmd = build_daemon_command(daemon_bin, &paths.instance, state_dir_override);
    cmd.stdin(Stdio::null())
        .stdout(Stdio::from(log_out))
        .stderr(Stdio::from(log_err));
    let mut child = cmd.spawn().map_err(|e| {
        anyhow::anyhow!(
            "failed to spawn jjfsd binary at {}: {e}",
            daemon_bin.display()
        )
    })?;

    let mut exited_status = None;
    for _ in 0..120 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        if let Ok(ControlResponse::Pong { pid }) = send_control_request_with_timeout(
            &paths.socket_path,
            &ControlRequest::Ping,
            PING_TIMEOUT,
        )
        .await
        {
            let _ = child.try_wait();
            return Ok(pid);
        }
        if exited_status.is_none() {
            exited_status = child.try_wait()?;
        }
        if let Some(status) = exited_status {
            // Another concurrent starter may hold `jjfsd.lock` while
            // initializing; keep waiting only while that lock is held.
            if !is_instance_lock_held(&paths.state_dir) {
                if let Ok(ControlResponse::Pong { pid }) = send_control_request_with_timeout(
                    &paths.socket_path,
                    &ControlRequest::Ping,
                    PING_TIMEOUT,
                )
                .await
                {
                    return Ok(pid);
                }
                let tail = read_log_tail(&log_path);
                if tail.is_empty() {
                    anyhow::bail!(
                        "jjfsd exited immediately with {status} before opening {}",
                        paths.socket_path.display()
                    );
                }
                anyhow::bail!(
                    "jjfsd exited immediately with {status} (log: {}):\n{tail}",
                    log_path.display()
                );
            }
        }
    }
    let tail = read_log_tail(&log_path);
    if tail.is_empty() {
        anyhow::bail!(
            "timed out waiting for jjfsd control socket at {}",
            paths.socket_path.display()
        );
    }
    anyhow::bail!(
        "timed out waiting for jjfsd control socket at {} (log: {}):\n{tail}",
        paths.socket_path.display(),
        log_path.display()
    )
}

fn locate_jjfsd_binary() -> anyhow::Result<PathBuf> {
    if let Some(explicit) = std::env::var_os("JJ_VFS_DAEMON_BIN") {
        return Ok(PathBuf::from(explicit));
    }
    if let Ok(exe) = std::env::current_exe()
        && let Some(dir) = exe.parent()
    {
        let sibling = dir.join("jjfsd");
        if sibling.is_file() {
            return Ok(sibling);
        }
        if let Some(parent) = dir.parent() {
            let parent_bin = parent.join("jjfsd");
            if parent_bin.is_file() {
                return Ok(parent_bin);
            }
        }
    }
    Ok(PathBuf::from("jjfsd"))
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use super::*;

    #[test]
    fn daemon_command_omits_state_dir_by_default_so_cli_and_daemon_agree_under_xdg_runtime_dir() {
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

        // 1. Default flow (no `--state-dir` override): CLI and spawned daemon
        //    must resolve identical socket, config, cache, and state paths
        //    under XDG.
        let cli_paths = InstancePaths::resolve_with_env(None, None, env);
        let cmd = build_daemon_command(Path::new("/usr/bin/jjfsd"), &cli_paths.instance, None);
        let argv: Vec<String> = cmd
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert_eq!(argv, vec!["--instance", "default"]);

        let daemon_paths = InstancePaths::resolve_with_env(Some("default"), None, env);
        assert_eq!(
            cli_paths.socket_path,
            PathBuf::from("/run/user/1000/jj-vfs/control.sock")
        );
        assert_eq!(cli_paths.socket_path, daemon_paths.socket_path);
        assert_eq!(cli_paths.config_path, daemon_paths.config_path);
        assert_eq!(cli_paths.cache_dir, daemon_paths.cache_dir);
        assert_eq!(cli_paths.state_dir, daemon_paths.state_dir);

        // 2. Explicit `--state-dir` override: CLI forwards `--state-dir` so
        //    both agree.
        let explicit = Path::new("/tmp/custom-vfs-state");
        let cli_override = InstancePaths::resolve_with_env(Some("dev"), Some(explicit), env);
        let cmd_override = build_daemon_command(
            Path::new("/usr/bin/jjfsd"),
            &cli_override.instance,
            Some(explicit),
        );
        let argv_override: Vec<String> = cmd_override
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            argv_override,
            vec!["--instance", "dev", "--state-dir", "/tmp/custom-vfs-state"]
        );
        let daemon_override = InstancePaths::resolve_with_env(Some("dev"), Some(explicit), env);
        assert_eq!(cli_override.socket_path, daemon_override.socket_path);
        assert_eq!(cli_override.config_path, daemon_override.config_path);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn ensure_daemon_running_captures_jjfsd_log_and_fails_fast_when_daemon_exits() {
        use std::os::unix::fs::PermissionsExt as _;

        let tmp = tempfile::tempdir().unwrap();
        let fake_bin = tmp.path().join("fake-jjfsd.sh");
        std::fs::write(
            &fake_bin,
            "#!/bin/sh\necho fatal_config_syntax_error >&2\nexit 17\n",
        )
        .unwrap();
        std::fs::set_permissions(&fake_bin, std::fs::Permissions::from_mode(0o755)).unwrap();

        let state_dir = tmp.path().join("state");
        let paths = InstancePaths::resolve(Some("failfast"), Some(&state_dir));
        let started = std::time::Instant::now();
        let err = ensure_daemon_running_inner(&paths, Some(&state_dir), &fake_bin)
            .await
            .unwrap_err();
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "expected fast failure on immediate child exit, took {:?}",
            started.elapsed()
        );
        let msg = format!("{err:#}");
        assert!(
            msg.contains("fatal_config_syntax_error"),
            "missing log tail in error: {msg}"
        );
        let log_contents = std::fs::read_to_string(state_dir.join("jjfsd.log")).unwrap();
        assert!(log_contents.contains("fatal_config_syntax_error"));
    }
}
