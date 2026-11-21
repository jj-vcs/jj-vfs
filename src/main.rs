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

//! Entry point for `jjfsd`: runs as the per-user multi-source daemon over
//! `$XDG_RUNTIME_DIR/jj-vfs/control.sock` (or `--instance` / `--state-dir`),
//! while retaining legacy 2-argument `<MOUNTPOINT> <WORKSPACE_ROOT>`
//! compatibility.

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use jj_lib::config::StackedConfig;
use jj_lib::repo::StoreFactories;
use jj_lib::settings::UserSettings;
use jj_lib::workspace::Workspace;
use jj_lib::workspace::default_working_copy_factories;
use jjfsd::daemon::DaemonRuntime;
use jjfsd::ipc_server::acquire_instance_lock;
use jjfsd::ipc_server::serve_control_socket_with_watch;
use jjfsd::path_mapper_all_commits::AllCommitsPathMapper;
use jjfsd::vfs::PathMappedVfs;
use tokio::sync::Notify;
use tokio::sync::watch;
use vfs_proto::InstancePaths;

#[derive(Parser, Debug)]
#[command(
    name = "jjfsd",
    about = "Multi-source virtual filesystem daemon for jj"
)]
struct Args {
    /// Isolated instance name (defaults to "default" or JJ_VFS_INSTANCE)
    #[arg(long)]
    instance: Option<String>,

    /// Explicit state directory override (defaults to XDG dirs or
    /// JJ_VFS_STATE_DIR)
    #[arg(long)]
    state_dir: Option<PathBuf>,

    /// Optional legacy positional arguments: `<MOUNTPOINT> <WORKSPACE_ROOT>`
    #[arg(trailing_var_arg = true)]
    legacy_args: Vec<String>,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    let args = Args::parse();
    let rt = tokio::runtime::Runtime::new()?;
    let _guard = rt.enter();

    if args.legacy_args.len() == 2 {
        run_legacy_single_mount(&rt, &args.legacy_args[0], Path::new(&args.legacy_args[1]))?;
        return Ok(());
    }
    if !args.legacy_args.is_empty() {
        anyhow::bail!(
            "unexpected positional arguments {:?}; run `jjfsd [--instance <name>]` or legacy \
             `jjfsd <MOUNTPOINT> <WORKSPACE_ROOT>`",
            args.legacy_args
        );
    }

    let paths = InstancePaths::resolve(args.instance.as_deref(), args.state_dir.as_deref());
    paths.ensure_dirs()?;
    let _instance_lock = acquire_instance_lock(&paths.state_dir)?;

    let runtime = DaemonRuntime::new(paths.clone())?;
    rt.block_on(runtime.mount_configured());
    let shutdown = Arc::new(Notify::new());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let sd_sig = Arc::clone(&shutdown);
    let sd_tx = shutdown_tx.clone();
    rt.spawn(async move {
        wait_for_shutdown().await;
        let _ = sd_tx.send(true);
        sd_sig.notify_waiters();
    });

    tracing::info!(
        instance = %paths.instance,
        socket = %paths.socket_path.display(),
        "jjfsd listening on control socket"
    );
    let res = rt.block_on(serve_control_socket_with_watch(
        &paths.socket_path,
        Arc::clone(&runtime),
        shutdown,
        shutdown_tx,
        shutdown_rx,
    ));
    rt.block_on(runtime.unmount_all());
    res?;
    Ok(())
}

fn run_legacy_single_mount(
    rt: &tokio::runtime::Runtime,
    mountpoint: &str,
    workspace_root: &Path,
) -> anyhow::Result<()> {
    let mut config = fuser::Config::default();
    config.mount_options = vec![
        fuser::MountOption::RO,
        fuser::MountOption::FSName("jjfs".to_string()),
    ];

    let settings = UserSettings::from_config(StackedConfig::with_defaults())?;
    let loaded_workspace = Workspace::load(
        &settings,
        workspace_root,
        &StoreFactories::default(),
        &default_working_copy_factories(),
    )?;
    let readonly_repo = rt.block_on(loaded_workspace.repo_loader().load_at_head())?;
    let mapper = AllCommitsPathMapper::new(readonly_repo);
    let filesystem = PathMappedVfs::new(mapper);

    println!("Mounting JjVfs at {mountpoint}...");
    let _session = fuser::spawn_mount(
        jjfsd::fuse::JjFuse::new(Arc::new(filesystem), rt.handle().clone()),
        mountpoint,
        &config,
    )?;
    rt.block_on(wait_for_shutdown());
    Ok(())
}

#[cfg(unix)]
async fn wait_for_shutdown() {
    use tokio::signal::unix::SignalKind;
    use tokio::signal::unix::signal;

    let mut sigint = signal(SignalKind::interrupt()).expect("failed to listen for SIGINT");
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to listen for SIGTERM");
    let mut sighup = signal(SignalKind::hangup()).expect("failed to listen for SIGHUP");

    tokio::select! {
        _ = sigint.recv() => tracing::info!("Received SIGINT, shutting down..."),
        _ = sigterm.recv() => tracing::info!("Received SIGTERM, shutting down..."),
        _ = sighup.recv() => tracing::info!("Received SIGHUP, shutting down..."),
    }
}
