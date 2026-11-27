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

use std::sync::Arc;

use jj_lib::config::StackedConfig;
use jj_lib::settings::UserSettings;
use jjfsd::path_mapper_all_commits::AllCommitsPathMapper;
use jjfsd::repo_loading::load_repo_at_head;
use jjfsd::vfs::PathMappedVfs;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <MOUNTPOINT> <WORKSPACE_ROOT>", args[0]);
        std::process::exit(1);
    }
    let mountpoint = &args[1];
    let workspace_root = std::path::Path::new(&args[2]);

    let mut config = fuser::Config::default();
    config.mount_options = vec![
        fuser::MountOption::RO,
        fuser::MountOption::FSName("jjfs".to_string()),
    ];

    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    let _guard = rt.enter();

    // Pass the home directory so that config paths starting with "~/" expand
    // the way they did before jj-lib 0.47, which looked it up implicitly.
    let settings = UserSettings::from_config_and_home_dir(
        StackedConfig::with_defaults(),
        std::env::home_dir(),
    )
    .expect("Failed to load user settings");

    let readonly_repo = rt
        .block_on(load_repo_at_head(&settings, workspace_root))
        .expect("Failed to load repo");

    let mapper = AllCommitsPathMapper::new(readonly_repo);
    let filesystem = PathMappedVfs::new(mapper);

    println!("Mounting JjVfs at {}...", mountpoint);
    let _session = fuser::spawn_mount(
        jjfsd::fuse::JjFuse::new(Arc::new(filesystem), rt.handle().clone()),
        mountpoint,
        &config,
    )
    .expect("Failed to mount filesystem");

    rt.block_on(wait_for_shutdown());
}

#[cfg(unix)]
async fn wait_for_shutdown() {
    use tokio::signal::unix::SignalKind;
    use tokio::signal::unix::signal;

    let mut sigint = signal(SignalKind::interrupt()).expect("failed to listen for SIGINT");
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to listen for SIGTERM");
    let mut sighup = signal(SignalKind::hangup()).expect("failed to listen for SIGHUP");

    tokio::select! {
        _ = sigint.recv() => println!("Received SIGINT, unmounting..."),
        _ = sigterm.recv() => println!("Received SIGTERM, unmounting..."),
        _ = sighup.recv() => println!("Received SIGHUP, unmounting..."),
    }
}
