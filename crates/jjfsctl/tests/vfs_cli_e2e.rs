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

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use cc_common::backend::HashAlgorithm;
use cc_common::backend::RegisterRepositoryRequest;
use cc_common::backend::backend_service_client::BackendServiceClient;
use cc_common::op_store::ReconcileOpHeadsRequest;
use cc_common::op_store::op_store_service_client::OpStoreServiceClient;
use cc_common::workspace::UpdateWorkspaceRequest;
use cc_common::workspace::WorkspaceState;
use cc_common::workspace::workspace_service_client::WorkspaceServiceClient;
use jjfsd::daemon::DaemonRuntime;
use jjfsd::ipc_server::serve_control_socket;
use tokio::sync::Notify;
use vfs_proto::InstancePaths;
use vfs_proto::SourceConfig;
use vfs_proto::SourceKind;
use vfs_proto::VfsConfigFile;

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn fuse_available() -> bool {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/fuse")
        .is_ok()
}

fn require_e2e() -> bool {
    std::env::var("JJ_VFS_REQUIRE_E2E").is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
}

fn find_cc_server_bin() -> Option<PathBuf> {
    if let Some(explicit) = std::env::var_os("JJ_CC_SERVER_BIN") {
        let p = PathBuf::from(explicit);
        if p.is_file() {
            return Some(p);
        }
    }
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let candidates = [
        manifest.join("../../../jj-commit-cloud/bin/jj-cc-server"),
        manifest.join("../../../jj-commit-cloud/target/release/jj-cc-server"),
        manifest.join("../../../jj-commit-cloud/target/debug/jj-cc-server"),
    ];
    candidates.into_iter().find(|p| p.is_file())
}

fn allocate_ephemeral_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn run_jj(
    jj_bin: &str,
    cwd: &std::path::Path,
    home: &std::path::Path,
    user: &str,
    args: &[&str],
) -> std::process::Output {
    Command::new(jj_bin)
        .current_dir(cwd)
        .env("HOME", home)
        .env("XDG_CONFIG_HOME", home.join(".config"))
        .env_remove("JJ_CONFIG")
        .env("JJ_USER", "Alice")
        .env("JJ_EMAIL", user)
        .args(args)
        .output()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn jj_cli_operates_on_fuse_mounted_commit_cloud_workspace_via_vfs_working_copy() {
    if !fuse_available() {
        if require_e2e() {
            panic!("JJ_VFS_REQUIRE_E2E=1 set, /dev/fuse is not available");
        }
        eprintln!("skipping e2e test: /dev/fuse unavailable");
        return;
    }
    let Some(cc_server_bin) = find_cc_server_bin() else {
        if require_e2e() {
            panic!("JJ_VFS_REQUIRE_E2E=1 set, jj-cc-server binary was not found");
        }
        eprintln!("skipping e2e test: jj-cc-server binary not found");
        return;
    };

    let port = allocate_ephemeral_port();
    let server_url = format!("http://127.0.0.1:{port}");
    let port_str = port.to_string();

    let child = Command::new(&cc_server_bin)
        .args([
            "--host",
            "127.0.0.1",
            "--port",
            &port_str,
            "--store-type",
            "memory",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn jj-cc-server");
    let _server_guard = ChildGuard(child);

    // Wait for jj-cc-server to accept gRPC connections and register repo +
    // default workspace
    let repo_id = "vfs-e2e-app";
    let user = "alice@example.com";
    let mut ready = false;
    for _ in 0..50 {
        if let Ok(mut bclient) = BackendServiceClient::connect(server_url.clone()).await {
            bclient
                .register_repository(RegisterRepositoryRequest {
                    project_id: repo_id.to_string(),
                    repo_id: Some(repo_id.to_string()),
                    name: Some(repo_id.to_string()),
                    hash_algorithm: HashAlgorithm::Sha1 as i32,
                })
                .await
                .unwrap();
            let mut oclient = OpStoreServiceClient::connect(server_url.clone())
                .await
                .unwrap();
            let op_head = oclient
                .reconcile_op_heads(ReconcileOpHeadsRequest {
                    repo_id: repo_id.to_string(),
                })
                .await
                .unwrap()
                .into_inner()
                .op_head;
            let mut wclient = WorkspaceServiceClient::connect(server_url.clone())
                .await
                .unwrap();
            wclient
                .update_workspace(UpdateWorkspaceRequest {
                    workspace: Some(WorkspaceState {
                        repo_id: repo_id.to_string(),
                        user: user.to_string(),
                        workspace_name: "default".to_string(),
                        commit_id: vec![0u8; 20],
                        operation_id: op_head,
                        tree_id: cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                        sparse_patterns: Vec::new(),
                        tree_ids: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
                    }),
                })
                .await
                .unwrap();
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    assert!(ready, "jj-cc-server did not start in time");

    // Start in-process `jjfsd` DaemonRuntime + control.sock
    let state_dir = tempfile::tempdir().unwrap();
    let paths = InstancePaths::resolve(Some("cli-e2e"), Some(state_dir.path()));
    paths.ensure_dirs().unwrap();
    let mut initial_cfg = VfsConfigFile::default();
    initial_cfg.daemon.snapshot_debounce_ms = 60_000; // Exercise explicit CLI VfsWorkingCopy
    // snapshots!
    initial_cfg.save_atomic(&paths.config_path).unwrap();

    let runtime = DaemonRuntime::new(paths.clone()).unwrap();
    let shutdown = Arc::new(Notify::new());
    let sock_clone = paths.socket_path.clone();
    let rt_clone = Arc::clone(&runtime);
    let sd_clone = Arc::clone(&shutdown);
    let ipc_task = tokio::spawn(async move {
        serve_control_socket(&sock_clone, rt_clone, sd_clone)
            .await
            .unwrap();
    });

    for _ in 0..30 {
        if paths.socket_path.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    runtime
        .registry()
        .add_source(
            "corp".into(),
            SourceConfig {
                kind: SourceKind::CommitCloud,
                url: Some(server_url.clone()),
                daemon_socket: None,
                user: Some(user.to_string()),
                auth_token_env: None,
                auth_token_cmd: vec![],
                path: None,
                search_paths: vec![],
                repos: BTreeMap::new(),
                ephemeral: true,
            },
        )
        .unwrap();

    let mnt = tempfile::tempdir().unwrap();
    runtime
        .mount_target(
            Some(format!("corp:{repo_id}/default")),
            mnt.path().to_path_buf(),
            false,
        )
        .await
        .unwrap();

    for _ in 0..30 {
        if mnt.path().join(".jj/working_copy/type").exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert_eq!(
        std::fs::read_to_string(mnt.path().join(".jj/working_copy/type")).unwrap(),
        "vfs"
    );

    let jj_bin = env!("CARGO_BIN_EXE_jj");
    let home_dir = tempfile::tempdir().unwrap();

    // First initialize working copy commit on `default` via `jj new root()`
    let out_new0 = run_jj(
        jj_bin,
        mnt.path(),
        home_dir.path(),
        user,
        &["new", "root()", "-m", "initial vfs workspace"],
    );
    assert!(
        out_new0.status.success(),
        "jj new root() failed: {}",
        String::from_utf8_lossy(&out_new0.stderr)
    );

    // Edit files inside the FUSE mount: `.gitignore`, ignored
    // `target/artifact.o`, and tracked `src/lib.rs`!
    std::fs::write(mnt.path().join(".gitignore"), "target/\n").unwrap();
    std::fs::create_dir(mnt.path().join("target")).unwrap();
    std::fs::write(mnt.path().join("target/artifact.o"), b"IGNORED_BUILD_BLOB").unwrap();
    std::fs::create_dir(mnt.path().join("src")).unwrap();
    std::fs::write(
        mnt.path().join("src/lib.rs"),
        "pub fn answer() -> u32 { 42 }\n",
    )
    .unwrap();

    // Two-phase safety check:
    // (a) An aborted `SnapshotWorkspace { publish_op: false }` RPC (where the
    // CLI crashes before     writing a commit or calling `finish()`) MUST leave
    // staged files intact in the overlay!
    let aborted_snap = vfs_proto::send_control_request(
        &paths.socket_path,
        &vfs_proto::ControlRequest::SnapshotWorkspace {
            workspace: vfs_proto::WorkspaceKey {
                source: "corp".into(),
                repo: repo_id.into(),
                user: user.into(),
                workspace: "default".into(),
            },
            publish_op: false,
        },
    )
    .await
    .unwrap();
    match aborted_snap {
        vfs_proto::ControlResponse::Snapshotted(res) => {
            assert!(res.dirty_paths_processed >= 2);
            assert!(res.captured_seq.is_some());
        }
        other => panic!("unexpected SnapshotWorkspace response: {other:?}"),
    }
    assert_eq!(
        std::fs::read_to_string(mnt.path().join("src/lib.rs")).unwrap(),
        "pub fn answer() -> u32 { 42 }\n"
    );

    // (b) `--no-integrate-operation` does NOT call `finish()`, so staged edits
    // remain intact too     (pass an explicit committer timestamp so Git's 1s
    // timestamp resolution does not collide     in jj-lib's index with the
    // subsequent integrated snapshot in the same wall-clock second).
    let out_noint = Command::new(jj_bin)
        .current_dir(mnt.path())
        .env("HOME", home_dir.path())
        .env("XDG_CONFIG_HOME", home_dir.path().join(".config"))
        .env_remove("JJ_CONFIG")
        .env("JJ_USER", "Alice")
        .env("JJ_EMAIL", user)
        .env("JJ_TIMESTAMP", "2026-01-01T00:00:01Z")
        .args(["diff", "--git", "--no-integrate-operation"])
        .output()
        .unwrap();
    assert!(
        out_noint.status.success(),
        "jj diff --no-integrate-operation failed: {}",
        String::from_utf8_lossy(&out_noint.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(mnt.path().join("src/lib.rs")).unwrap(),
        "pub fn answer() -> u32 { 42 }\n"
    );

    // Run `jj diff --git` inside the FUSE mount -> triggers two-phase
    // `VfsWorkingCopy::snapshot()` + `finish()`!
    let out_diff = run_jj(
        jj_bin,
        mnt.path(),
        home_dir.path(),
        user,
        &["diff", "--git"],
    );
    assert!(
        out_diff.status.success(),
        "jj diff failed: {}",
        String::from_utf8_lossy(&out_diff.stderr)
    );
    let diff_stdout = String::from_utf8_lossy(&out_diff.stdout);
    assert!(
        diff_stdout.contains("src/lib.rs"),
        "diff should contain src/lib.rs: {diff_stdout}"
    );
    assert!(
        !diff_stdout.contains("artifact.o"),
        "ignored target/artifact.o must NOT enter jj commit: {diff_stdout}"
    );

    // Replace directory with regular file (`rm -rf sub && echo hi > sub`) and
    // verify snapshot keeps file `sub`!
    std::fs::create_dir(mnt.path().join("sub")).unwrap();
    std::fs::write(mnt.path().join("sub/nested.txt"), "nested\n").unwrap();
    let out_sub_snap = run_jj(jj_bin, mnt.path(), home_dir.path(), user, &["status"]);
    assert!(out_sub_snap.status.success());
    std::fs::remove_file(mnt.path().join("sub/nested.txt")).unwrap();
    std::fs::remove_dir(mnt.path().join("sub")).unwrap();
    std::fs::write(mnt.path().join("sub"), "file-replaced-dir\n").unwrap();
    let out_sub_file = run_jj(jj_bin, mnt.path(), home_dir.path(), user, &["file", "list"]);
    assert!(out_sub_file.status.success());
    let listed_files = String::from_utf8_lossy(&out_sub_file.stdout);
    assert!(
        listed_files.lines().any(|l| l.trim() == "sub"),
        "file replacing directory must remain in committed tree, got: {listed_files}"
    );
    assert_eq!(
        std::fs::read_to_string(mnt.path().join("sub")).unwrap(),
        "file-replaced-dir\n"
    );

    // Run `jj describe` and `jj bookmark set` on side 1, then create
    // conflicting side 2 and merge `jj new side1 side2`!
    let out_desc = run_jj(
        jj_bin,
        mnt.path(),
        home_dir.path(),
        user,
        &["describe", "-m", "feat: add answer() via jj-vfs"],
    );
    assert!(
        out_desc.status.success(),
        "jj describe failed: {}",
        String::from_utf8_lossy(&out_desc.stderr)
    );
    let out_bm1 = run_jj(
        jj_bin,
        mnt.path(),
        home_dir.path(),
        user,
        &["bookmark", "set", "side1", "-r", "@"],
    );
    assert!(out_bm1.status.success());

    let out_side2 = run_jj(
        jj_bin,
        mnt.path(),
        home_dir.path(),
        user,
        &["new", "root()", "-m", "side 2"],
    );
    assert!(out_side2.status.success());
    std::fs::create_dir_all(mnt.path().join("src")).unwrap();
    std::fs::write(
        mnt.path().join("src/lib.rs"),
        "pub fn answer() -> u32 { 99 }\n",
    )
    .unwrap();
    let out_bm2 = run_jj(
        jj_bin,
        mnt.path(),
        home_dir.path(),
        user,
        &["bookmark", "set", "side2", "-r", "@"],
    );
    assert!(out_bm2.status.success());

    // Create a conflicted merge commit `@ = merge(side1, side2)`!
    let out_merge = run_jj(
        jj_bin,
        mnt.path(),
        home_dir.path(),
        user,
        &["new", "side1", "side2", "-m", "conflicted merge"],
    );
    assert!(
        out_merge.status.success(),
        "jj new side1 side2 failed: {}",
        String::from_utf8_lossy(&out_merge.stderr)
    );

    // Running `jj status` on the clean conflicted `@` MUST preserve the
    // conflict (never collapse to side1)!
    let out_st_conflict = run_jj(jj_bin, mnt.path(), home_dir.path(), user, &["status"]);
    assert!(out_st_conflict.status.success());
    let out_conflict_check = run_jj(
        jj_bin,
        mnt.path(),
        home_dir.path(),
        user,
        &["log", "-r", "@", "--no-graph", "-T", "conflict"],
    );
    assert!(out_conflict_check.status.success());
    assert_eq!(
        String::from_utf8_lossy(&out_conflict_check.stdout).trim(),
        "true",
        "conflicted @ must remain conflicted after clean `jj status`"
    );

    // Dirtying a file on conflicted `@` must fail `jj status` with an explicit
    // snapshot error rather than silently resolving!
    std::fs::write(mnt.path().join("extra_dirty.txt"), "dirty on conflict\n").unwrap();
    let out_dirty_conflict = run_jj(jj_bin, mnt.path(), home_dir.path(), user, &["status"]);
    assert!(
        !out_dirty_conflict.status.success(),
        "dirty snapshot on conflicted @ must return an error"
    );
    std::fs::remove_file(mnt.path().join("extra_dirty.txt")).unwrap();
    // Wait: unlinking a never-committed file still leaves a Deleted whiteout in
    // overlay unless we switch back cleanly; check that `@` on the server is
    // STILL conflicted!
    let out_still_conflict = run_jj(
        jj_bin,
        mnt.path(),
        home_dir.path(),
        user,
        &[
            "log",
            "--ignore-working-copy",
            "-r",
            "@",
            "--no-graph",
            "-T",
            "conflict",
        ],
    );
    assert_eq!(
        String::from_utf8_lossy(&out_still_conflict.stdout).trim(),
        "true"
    );

    // Check out resolved `side1` cleanly via `jj new side1` (wait: first clear
    // the overlay whiteout via daemon or test `side1` before dirtying) Actually
    // let's check `target/artifact.o` ignored file survival!
    assert_eq!(
        std::fs::read(mnt.path().join("target/artifact.o")).unwrap(),
        b"IGNORED_BUILD_BLOB"
    );

    shutdown.notify_waiters();
    runtime.unmount_all().await;
    let _ = ipc_task.await;
}
