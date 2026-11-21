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

//! Cross-platform workflow driver (`cargo xtask`) for building, smoke-testing,
//! and benchmarking `jj-vfs` across local repositories and `jj-cc-server`
//! peers.

use std::net::TcpListener;
use std::path::Path;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use anyhow::ensure;
use cc_common::backend::Commit as ProtoCommit;
use cc_common::backend::File as ProtoFile;
use cc_common::backend::HashAlgorithm;
use cc_common::backend::RegisterRepositoryRequest;
use cc_common::backend::Signature as ProtoSignature;
use cc_common::backend::Timestamp as ProtoTimestamp;
use cc_common::backend::TreeEntry as ProtoTreeEntry;
use cc_common::backend::TreeValue as ProtoTreeValueMsg;
use cc_common::backend::WriteCommitRequest;
use cc_common::backend::WriteFileRequest;
use cc_common::backend::WriteTreeRequest;
use cc_common::backend::backend_service_client::BackendServiceClient;
use cc_common::backend::tree_value::Value as ProtoTreeValue;
use cc_common::op_store::Operation as ProtoOperation;
use cc_common::op_store::OperationMetadata as ProtoOpMeta;
use cc_common::op_store::PublishOperationRequest;
use cc_common::op_store::ReadOperationRequest;
use cc_common::op_store::ReadViewRequest;
use cc_common::op_store::ReconcileOpHeadsRequest;
use cc_common::op_store::op_store_service_client::OpStoreServiceClient;
use cc_common::workspace::UpdateWorkspaceRequest;
use cc_common::workspace::WorkspaceState;
use cc_common::workspace::workspace_service_client::WorkspaceServiceClient;
use clap::Parser;
use clap::Subcommand;

#[derive(Parser, Debug)]
#[command(name = "cargo xtask", about = "Build, verify, and benchmark jj-vfs")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Verify workspace layout
    Check,
    /// Compile `jjfsd`, `jj-vfs`, and custom `jj` binaries
    Build {
        #[arg(long)]
        release: bool,
        #[arg(long)]
        export_bin: bool,
        #[arg(long)]
        offline: bool,
    },
    /// Run end-to-end smoke verification across two `jj-cc-server` peers and a
    /// local repository
    Smoke,
    /// Benchmark concurrent workspaces sharing one `jjfsd` daemon and `BlobCas`
    Bench {
        #[arg(long, default_value_t = 50)]
        workspaces: usize,
    },
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct DaemonCleanupGuard {
    bin_dir: PathBuf,
    state_dir: PathBuf,
    home_dir: PathBuf,
    mountpoints: Vec<PathBuf>,
    stopped: bool,
}

impl DaemonCleanupGuard {
    fn stop_now(&mut self) {
        if !self.stopped {
            let _ = run_ctl(
                &self.bin_dir,
                &self.state_dir,
                &self.home_dir,
                &["daemon", "stop"],
            );
            self.stopped = true;
        }
    }
}

impl Drop for DaemonCleanupGuard {
    fn drop(&mut self) {
        self.stop_now();
        for mp in &self.mountpoints {
            if mp.exists() {
                let _ = Command::new("fusermount3")
                    .args(["-uz"])
                    .arg(mp)
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
                let _ = Command::new("fusermount")
                    .args(["-uz"])
                    .arg(mp)
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Check => {
            println!("jj-vfs xtask workspace ready");
            Ok(())
        }
        Commands::Build {
            release,
            export_bin,
            offline,
        } => cmd_build(release, export_bin, offline),
        Commands::Smoke => cmd_smoke().await,
        Commands::Bench { workspaces } => cmd_bench(workspaces).await,
    }
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

fn target_root(root: &Path) -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join("target"))
}

fn cmd_build(release: bool, export_bin: bool, offline: bool) -> Result<()> {
    let root = repo_root();
    let mut args = vec!["build", "--bins"];
    if offline {
        args.push("--offline");
    }
    if release {
        args.push("--release");
    }
    let status = Command::new("cargo")
        .current_dir(&root)
        .args(&args)
        .status()
        .context("cargo build failed")?;
    if !status.success() {
        bail!("cargo build exited with {status}");
    }
    let profile = if release { "release" } else { "debug" };
    let target_dir = target_root(&root).join(profile);
    if export_bin {
        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir)?;
        for name in ["jjfsd", "jj-vfs", "jj"] {
            std::fs::copy(target_dir.join(name), bin_dir.join(name))?;
        }
        println!("Exported jjfsd, jj-vfs, and jj to {}", bin_dir.display());
    }
    Ok(())
}

fn ensure_debug_bins_built(root: &Path) -> Result<PathBuf> {
    let target_dir = target_root(root).join("debug");
    if !target_dir.join("jjfsd").is_file()
        || !target_dir.join("jj-vfs").is_file()
        || !target_dir.join("jj").is_file()
    {
        cmd_build(false, false, false)?;
    }
    Ok(target_dir)
}

fn find_cc_server_bin(root: &Path) -> Result<PathBuf> {
    if let Some(explicit) = std::env::var_os("JJ_CC_SERVER_BIN") {
        let p = PathBuf::from(explicit);
        if p.is_file() {
            return Ok(p);
        }
    }
    let cc_root = root.join("../jj-commit-cloud");
    let candidates = [
        cc_root.join("bin/jj-cc-server"),
        cc_root.join("target/release/jj-cc-server"),
        cc_root.join("target/debug/jj-cc-server"),
    ];
    for c in &candidates {
        if c.is_file() {
            return Ok(c.clone());
        }
    }
    bail!(
        "Could not locate `jj-cc-server` in sibling `{}`; run `cargo xtask build --export-bin` in \
         jj-commit-cloud first",
        cc_root.display()
    );
}

fn free_port() -> Result<u16> {
    Ok(TcpListener::bind("127.0.0.1:0")?.local_addr()?.port())
}

async fn spawn_cc_server(cc_server_bin: &Path) -> Result<(ChildGuard, String)> {
    let port = free_port()?;
    let url = format!("http://127.0.0.1:{port}");
    let child = Command::new(cc_server_bin)
        .args([
            "--host",
            "127.0.0.1",
            "--port",
            &port.to_string(),
            "--store-type",
            "memory",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to spawn jj-cc-server")?;
    let guard = ChildGuard(child);
    for _ in 0..50 {
        if BackendServiceClient::connect(url.clone()).await.is_ok() {
            return Ok((guard, url));
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    bail!("jj-cc-server at {url} did not become ready in time");
}

async fn seed_cc_repo_and_workspaces(
    server_url: &str,
    repo_id: &str,
    user: &str,
    workspace_names: &[String],
    shared_files: usize,
) -> Result<String> {
    let mut bclient = BackendServiceClient::connect(server_url.to_string()).await?;
    bclient
        .register_repository(RegisterRepositoryRequest {
            project_id: repo_id.to_string(),
            repo_id: Some(repo_id.to_string()),
            name: Some(repo_id.to_string()),
            hash_algorithm: HashAlgorithm::Sha1 as i32,
        })
        .await?;

    let mut entries = Vec::with_capacity(shared_files + 1);
    let gi_bytes = b"target/\n*.tmp\n".to_vec();
    let gi_fid = bclient
        .write_file(WriteFileRequest {
            project_id: repo_id.to_string(),
            content: gi_bytes.clone(),
        })
        .await?
        .into_inner()
        .file_id;
    entries.push(ProtoTreeEntry {
        name: ".gitignore".to_string(),
        value: Some(ProtoTreeValueMsg {
            value: Some(ProtoTreeValue::File(ProtoFile {
                id: gi_fid,
                executable: false,
                copy_id: Vec::new(),
                size_bytes: Some(gi_bytes.len() as u64),
            })),
        }),
    });

    for idx in 0..shared_files {
        let payload =
            format!("// shared file {idx} in {repo_id}\npub const ID_{idx}: usize = {idx};\n")
                .into_bytes();
        let sz = payload.len() as u64;
        let fid = bclient
            .write_file(WriteFileRequest {
                project_id: repo_id.to_string(),
                content: payload,
            })
            .await?
            .into_inner()
            .file_id;
        entries.push(ProtoTreeEntry {
            name: format!("shared_{idx:03}.rs"),
            value: Some(ProtoTreeValueMsg {
                value: Some(ProtoTreeValue::File(ProtoFile {
                    id: fid,
                    executable: false,
                    copy_id: Vec::new(),
                    size_bytes: Some(sz),
                })),
            }),
        });
    }
    entries.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
    let root_tid = bclient
        .write_tree(WriteTreeRequest {
            project_id: repo_id.to_string(),
            path: String::new(),
            entries,
        })
        .await?
        .into_inner()
        .tree_id;

    let sig = ProtoSignature {
        name: "Seeder".to_string(),
        email: user.to_string(),
        timestamp: Some(ProtoTimestamp {
            millis_since_epoch: 1_760_000_000_000,
            tz_offset: 0,
        }),
    };
    let base_cid = bclient
        .write_commit(WriteCommitRequest {
            project_id: repo_id.to_string(),
            commit: Some(ProtoCommit {
                parent_commit_ids: vec![vec![0u8; 20]],
                root_tree_id: vec![root_tid.clone()],
                change_id: vec![0x11; 16],
                description: format!("seed {repo_id}"),
                author: Some(sig.clone()),
                committer: Some(sig.clone()),
                ..Default::default()
            }),
        })
        .await?
        .into_inner()
        .commit_id;

    let mut oclient = OpStoreServiceClient::connect(server_url.to_string()).await?;
    let op_head = oclient
        .reconcile_op_heads(ReconcileOpHeadsRequest {
            repo_id: repo_id.to_string(),
        })
        .await?
        .into_inner()
        .op_head;
    let cur_op = oclient
        .read_operation(ReadOperationRequest {
            repo_id: repo_id.to_string(),
            operation_id: op_head.clone(),
        })
        .await?
        .into_inner()
        .operation
        .context("missing root operation")?;
    let mut view = oclient
        .read_view(ReadViewRequest {
            repo_id: repo_id.to_string(),
            view_id: cur_op.view_id,
        })
        .await?
        .into_inner()
        .view
        .context("missing root view")?;

    let mut head_ids = vec![base_cid.clone()];
    let mut ws_commits = Vec::with_capacity(workspace_names.len());
    for (idx, ws_name) in workspace_names.iter().enumerate() {
        let mut chg = vec![0x22; 16];
        chg[0] = (idx & 0xff) as u8;
        chg[1] = ((idx >> 8) & 0xff) as u8;
        let wc_cid = bclient
            .write_commit(WriteCommitRequest {
                project_id: repo_id.to_string(),
                commit: Some(ProtoCommit {
                    parent_commit_ids: vec![base_cid.clone()],
                    root_tree_id: vec![root_tid.clone()],
                    change_id: chg,
                    description: format!("wc {ws_name}"),
                    author: Some(sig.clone()),
                    committer: Some(sig.clone()),
                    ..Default::default()
                }),
            })
            .await?
            .into_inner()
            .commit_id;
        view.wc_commit_ids.insert(ws_name.clone(), wc_cid.clone());
        head_ids.push(wc_cid.clone());
        ws_commits.push((ws_name.clone(), wc_cid));
    }
    view.head_ids = head_ids;

    let new_op = oclient
        .publish_operation(PublishOperationRequest {
            repo_id: repo_id.to_string(),
            view: Some(view),
            operation: Some(ProtoOperation {
                parents: vec![op_head.clone()],
                metadata: Some(ProtoOpMeta {
                    start_time_millis: 1_760_000_000_000,
                    end_time_millis: 1_760_000_000_000,
                    description: "seed workspaces".to_string(),
                    hostname: "localhost".to_string(),
                    username: user.to_string(),
                    is_snapshot: false,
                    workspace_name: None,
                    attributes: Default::default(),
                }),
                commit_predecessors: Vec::new(),
                commit_predecessors_set: true,
                view_id: Vec::new(),
            }),
            old_op_head_ids: vec![op_head],
        })
        .await?
        .into_inner()
        .operation_id;

    let mut wclient = WorkspaceServiceClient::connect(server_url.to_string()).await?;
    for (ws_name, wc_cid) in ws_commits {
        wclient
            .update_workspace(UpdateWorkspaceRequest {
                workspace: Some(WorkspaceState {
                    repo_id: repo_id.to_string(),
                    user: user.to_string(),
                    workspace_name: ws_name,
                    commit_id: wc_cid,
                    operation_id: new_op.clone(),
                    tree_id: root_tid.clone(),
                    sparse_patterns: Vec::new(),
                    tree_ids: vec![root_tid.clone()],
                }),
            })
            .await?;
    }

    Ok(hex::encode(base_cid))
}

fn run_ctl(bin_dir: &Path, state_dir: &Path, home_dir: &Path, args: &[&str]) -> Result<String> {
    let mut path_env = bin_dir.display().to_string();
    if let Ok(existing) = std::env::var("PATH") {
        path_env = format!("{path_env}:{existing}");
    }
    let out = Command::new(bin_dir.join("jj-vfs"))
        .env("JJ_VFS_STATE_DIR", state_dir)
        .env("HOME", home_dir)
        .env("PATH", path_env)
        .args(args)
        .output()
        .with_context(|| format!("failed to run jj-vfs {args:?}"))?;
    if !out.status.success() {
        bail!(
            "jj-vfs {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

async fn cmd_smoke() -> Result<()> {
    let root = repo_root();
    let bin_dir = ensure_debug_bins_built(&root)?;
    let cc_server_bin = find_cc_server_bin(&root)?;

    let (_srv1, corp_url) = spawn_cc_server(&cc_server_bin).await?;
    let (_srv2, oss_url) = spawn_cc_server(&cc_server_bin).await?;

    let corp_base = seed_cc_repo_and_workspaces(
        &corp_url,
        "webapp",
        "alice@corp.local",
        &["default".to_string()],
        3,
    )
    .await?;
    let _oss_base = seed_cc_repo_and_workspaces(
        &oss_url,
        "cli-tool",
        "alice@oss.local",
        &["default".to_string()],
        2,
    )
    .await?;

    let tmp = tempfile::tempdir()?;
    let state_dir = tmp.path().join("state");
    let home_dir = tmp.path().join("home");
    let local_repo = tmp.path().join("local-notes");
    std::fs::create_dir_all(&home_dir)?;
    std::fs::create_dir_all(&local_repo)?;

    // Initialize a local jj repo using our custom `jj` binary
    let init_out = Command::new(bin_dir.join("jj"))
        .env("HOME", &home_dir)
        .args(["git", "init", local_repo.to_str().unwrap()])
        .output()?;
    if !init_out.status.success() {
        bail!(
            "jj git init failed: {}",
            String::from_utf8_lossy(&init_out.stderr)
        );
    }
    std::fs::write(local_repo.join("notes.md"), "offline local note\n")?;
    let _ = Command::new(bin_dir.join("jj"))
        .current_dir(&local_repo)
        .env("HOME", &home_dir)
        .env("JJ_USER", "Alice")
        .env("JJ_EMAIL", "alice@local")
        .args(["commit", "-m", "seed local note"])
        .output()?;

    let root_mnt = tmp.path().join("mnt-root");
    let corp_ws_mnt = tmp.path().join("mnt-corp-ws");
    run_ctl(&bin_dir, &state_dir, &home_dir, &["daemon", "start"])?;
    let mut daemon_guard = DaemonCleanupGuard {
        bin_dir: bin_dir.clone(),
        state_dir: state_dir.clone(),
        home_dir: home_dir.clone(),
        mountpoints: vec![corp_ws_mnt.clone(), root_mnt.clone()],
        stopped: false,
    };

    run_ctl(
        &bin_dir,
        &state_dir,
        &home_dir,
        &[
            "source",
            "add",
            "corp",
            "--server",
            &corp_url,
            "--user",
            "alice@corp.local",
        ],
    )?;
    run_ctl(
        &bin_dir,
        &state_dir,
        &home_dir,
        &[
            "source",
            "add",
            "oss",
            "--server",
            &oss_url,
            "--user",
            "alice@oss.local",
        ],
    )?;
    run_ctl(
        &bin_dir,
        &state_dir,
        &home_dir,
        &[
            "source",
            "add",
            "local",
            "--path",
            local_repo.to_str().unwrap(),
        ],
    )?;

    run_ctl(
        &bin_dir,
        &state_dir,
        &home_dir,
        &["mount", root_mnt.to_str().unwrap()],
    )?;
    run_ctl(
        &bin_dir,
        &state_dir,
        &home_dir,
        &[
            "mount",
            "corp:webapp/default",
            corp_ws_mnt.to_str().unwrap(),
        ],
    )?;

    // Wait for FUSE background session INIT handshake to complete
    for _ in 0..50 {
        if corp_ws_mnt.join("shared_000.rs").is_file() && root_mnt.join("oss").is_dir() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    // Verify reads across ALL THREE sources (`corp`, `oss`, `local`)!
    ensure!(
        corp_ws_mnt.join("shared_000.rs").is_file(),
        "corp workspace file missing"
    );
    ensure!(
        root_mnt
            .join("oss/repos/cli-tool/workspaces/default/shared_000.rs")
            .is_file(),
        "oss workspace file missing"
    );
    ensure!(
        root_mnt
            .join("corp/repos/webapp/commits")
            .join(&corp_base[..12])
            .join("shared_001.rs")
            .is_file(),
        "corp 12-hex commit prefix lookup missing"
    );
    let local_note = std::fs::read_to_string(
        root_mnt.join("local/repos/local-notes/workspaces/default/notes.md"),
    )?;
    ensure!(
        local_note == "offline local note\n",
        "unexpected local note content: {local_note}"
    );

    // Edit inside `corp:webapp/default`, write ignored `target/build.o`, and
    // snapshot via `jj diff`!
    std::fs::create_dir(corp_ws_mnt.join("target"))?;
    std::fs::write(corp_ws_mnt.join("target/build.o"), b"IGNORED_ARTIFACT")?;
    std::fs::write(
        corp_ws_mnt.join("feature.rs"),
        "pub fn smoke() -> bool { true }\n",
    )?;

    let diff_out = Command::new(bin_dir.join("jj"))
        .current_dir(&corp_ws_mnt)
        .env("HOME", &home_dir)
        .env("XDG_CONFIG_HOME", home_dir.join(".config"))
        .env_remove("JJ_CONFIG")
        .env("JJ_USER", "Alice")
        .env("JJ_EMAIL", "alice@corp.local")
        .args(["diff", "--git"])
        .output()?;
    if !diff_out.status.success() {
        bail!(
            "jj diff inside FUSE mount failed: {}",
            String::from_utf8_lossy(&diff_out.stderr)
        );
    }
    let diff_txt = String::from_utf8_lossy(&diff_out.stdout);
    ensure!(
        diff_txt.contains("feature.rs") && !diff_txt.contains("build.o"),
        "unexpected jj diff output: {diff_txt}"
    );

    daemon_guard.stop_now();
    println!(
        "SMOKE PASSED: 2 Commit Cloud servers + 1 local repo + Root & Scoped Direct FUSE mounts + \
         VfsWorkingCopy verified."
    );
    Ok(())
}

async fn cmd_bench(workspaces: usize) -> Result<()> {
    ensure!(workspaces >= 2, "--workspaces must be at least 2");
    let root = repo_root();
    let bin_dir = ensure_debug_bins_built(&root)?;
    let cc_server_bin = find_cc_server_bin(&root)?;

    let (_srv1, corp_url) = spawn_cc_server(&cc_server_bin).await?;
    let (_srv2, oss_url) = spawn_cc_server(&cc_server_bin).await?;

    let half = workspaces / 2;
    let corp_ws: Vec<String> = (0..half).map(|i| format!("ws-{i:03}")).collect();
    let oss_ws: Vec<String> = (half..workspaces).map(|i| format!("ws-{i:03}")).collect();

    let shared_files = 10;
    seed_cc_repo_and_workspaces(
        &corp_url,
        "bench-corp",
        "bench@corp.local",
        &corp_ws,
        shared_files,
    )
    .await?;
    seed_cc_repo_and_workspaces(
        &oss_url,
        "bench-oss",
        "bench@oss.local",
        &oss_ws,
        shared_files,
    )
    .await?;

    let tmp = tempfile::tempdir()?;
    let state_dir = tmp.path().join("state");
    let home_dir = tmp.path().join("home");
    let root_mnt = tmp.path().join("mnt-root");
    std::fs::create_dir_all(&home_dir)?;

    run_ctl(&bin_dir, &state_dir, &home_dir, &["daemon", "start"])?;
    let mut daemon_guard = DaemonCleanupGuard {
        bin_dir: bin_dir.clone(),
        state_dir: state_dir.clone(),
        home_dir: home_dir.clone(),
        mountpoints: vec![root_mnt.clone()],
        stopped: false,
    };

    run_ctl(
        &bin_dir,
        &state_dir,
        &home_dir,
        &[
            "source",
            "add",
            "corp",
            "--server",
            &corp_url,
            "--user",
            "bench@corp.local",
        ],
    )?;
    run_ctl(
        &bin_dir,
        &state_dir,
        &home_dir,
        &[
            "source",
            "add",
            "oss",
            "--server",
            &oss_url,
            "--user",
            "bench@oss.local",
        ],
    )?;

    let t_mount = Instant::now();
    run_ctl(
        &bin_dir,
        &state_dir,
        &home_dir,
        &["mount", root_mnt.to_str().unwrap()],
    )?;
    for _ in 0..50 {
        if root_mnt.join("corp").is_dir() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    ensure!(
        root_mnt.join("corp").is_dir(),
        "root mount did not become ready in time"
    );
    let root_mount_ms = t_mount.elapsed().as_secs_f64() * 1000.0;

    // Exercise all `workspaces` workspaces concurrently inside one daemon +
    // shared BlobCas!
    let t_work = Instant::now();
    let mut handles = Vec::with_capacity(workspaces);
    for idx in 0..workspaces {
        let ws_dir = if idx < half {
            root_mnt.join(format!(
                "corp/repos/bench-corp/users/bench@corp.local/ws-{idx:03}"
            ))
        } else {
            root_mnt.join(format!(
                "oss/repos/bench-oss/users/bench@oss.local/ws-{idx:03}"
            ))
        };
        handles.push(tokio::task::spawn_blocking(move || -> Result<()> {
            let s0 = std::fs::read_to_string(ws_dir.join("shared_000.rs"))?;
            let s5 = std::fs::read_to_string(ws_dir.join("shared_005.rs"))?;
            ensure!(
                !s0.is_empty() && !s5.is_empty(),
                "empty shared file read in {ws_dir:?}"
            );
            std::fs::write(
                ws_dir.join(format!("agent_{idx:03}.rs")),
                format!("pub const AGENT: usize = {idx};\n"),
            )?;
            Ok(())
        }));
    }
    for h in handles {
        h.await??;
    }
    let work_ms = t_work.elapsed().as_secs_f64() * 1000.0;

    // Exercise `jj diff --git` inside workspace 0 (`VfsWorkingCopy` O(k)
    // two-phase CLI snapshot)
    let ws0_dir = root_mnt.join("corp/repos/bench-corp/users/bench@corp.local/ws-000");
    let t_jj_snap = Instant::now();
    let jj_out = Command::new(bin_dir.join("jj"))
        .current_dir(&ws0_dir)
        .env("HOME", &home_dir)
        .env("XDG_CONFIG_HOME", home_dir.join(".config"))
        .env_remove("JJ_CONFIG")
        .env("JJ_USER", "Bench")
        .env("JJ_EMAIL", "bench@corp.local")
        .args(["diff", "--git"])
        .output()?;
    ensure!(
        jj_out.status.success(),
        "jj diff in ws-000 failed: {}",
        String::from_utf8_lossy(&jj_out.stderr)
    );
    ensure!(
        String::from_utf8_lossy(&jj_out.stdout).contains("agent_000.rs"),
        "expected agent_000.rs in ws-000 jj diff"
    );
    let jj_snap_ms = t_jj_snap.elapsed().as_secs_f64() * 1000.0;

    let status_raw = run_ctl(
        &bin_dir,
        &state_dir,
        &home_dir,
        &["daemon", "status", "--json"],
    )?;
    let st: serde_json::Value = serde_json::from_str(&status_raw)?;
    let cached_blobs = st["cached_blobs"].as_u64().unwrap_or(0);
    let cached_bytes = st["cached_bytes"].as_u64().unwrap_or(0);
    ensure!(
        cached_blobs >= 4 && cached_bytes > 0,
        "expected shared BlobCas hits across {workspaces} workspaces, got {cached_blobs} blobs \
         ({cached_bytes} bytes)"
    );
    ensure!(
        cached_blobs <= (shared_files * 2 + 4) as u64,
        "BlobCas failed to deduplicate shared blobs across {workspaces} workspaces: \
         {cached_blobs} blobs"
    );

    let t_flush = Instant::now();
    daemon_guard.stop_now();
    let flush_ms = t_flush.elapsed().as_secs_f64() * 1000.0;

    println!(
        "BENCH PASSED ({workspaces} workspaces across 2 Commit Cloud servers):\n  root_mount_ms:  \
         {root_mount_ms:.1} ms\n  {workspaces}-ws I/O wall: {work_ms:.1} ms ({:.2} ms/ws)\n  1-ws \
         jj diff:   {jj_snap_ms:.1} ms\n  {workspaces}-ws flush:    {flush_ms:.1} ms\n  shared \
         BlobCas: {cached_blobs} unique blobs ({cached_bytes} bytes cached across {workspaces} \
         workspaces)",
        work_ms / (workspaces as f64)
    );
    Ok(())
}
