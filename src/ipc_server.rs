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

//! Unix domain socket control plane (`control.sock`) servicing `jj-vfs` CLI
//! and `VfsWorkingCopy` RPCs inside `jjfsd`.

use std::fs::File;
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::UnixListener;
use tokio::net::UnixStream;
use tokio::sync::Notify;
use tokio::sync::watch;
use vfs_proto::ControlRequest;
use vfs_proto::ControlResponse;
use vfs_proto::MAX_CONTROL_FRAME_BYTES;
use vfs_proto::read_bounded_line;
use vfs_proto::send_control_request_with_timeout;

const STALE_SOCKET_PING_TIMEOUT: Duration = Duration::from_secs(2);
const CLIENT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const INSTANCE_LOCK_FILENAME: &str = "jjfsd.lock";

/// Exclusive advisory `flock` guard held by a live `jjfsd` instance for its
/// lifetime.
pub struct InstanceLock {
    _file: File,
}

/// Acquires a non-blocking exclusive lock on `<state_dir>/jjfsd.lock` before
/// any daemon initialization or socket unlink runs.
pub fn acquire_instance_lock(state_dir: &Path) -> std::io::Result<InstanceLock> {
    std::fs::create_dir_all(state_dir)?;
    let lock_path = state_dir.join(INSTANCE_LOCK_FILENAME);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)?;
    #[cfg(unix)]
    {
        use std::io::Seek as _;
        use std::io::SeekFrom;
        use std::io::Write as _;
        use std::os::unix::fs::PermissionsExt as _;
        use std::os::unix::io::AsRawFd as _;

        let _ = file.set_permissions(std::fs::Permissions::from_mode(0o600));
        // SAFETY: `file.as_raw_fd()` is an open descriptor owned by `file`.
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::WouldBlock {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AddrInUse,
                    format!(
                        "jjfsd is already running (lock held at {})",
                        lock_path.display()
                    ),
                ));
            }
            return Err(err);
        }
        let _ = file.set_len(0);
        let _ = file.seek(SeekFrom::Start(0));
        let _ = writeln!(file, "{}", std::process::id());
        let _ = file.flush();
    }
    Ok(InstanceLock { _file: file })
}

/// Reports whether another process currently holds `<state_dir>/jjfsd.lock`.
pub fn is_instance_lock_held(state_dir: &Path) -> bool {
    let lock_path = state_dir.join(INSTANCE_LOCK_FILENAME);
    let Ok(file) = OpenOptions::new().read(true).write(true).open(&lock_path) else {
        return false;
    };
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd as _;
        // SAFETY: `file.as_raw_fd()` is an open descriptor owned by `file`.
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            return std::io::Error::last_os_error().kind() == std::io::ErrorKind::WouldBlock;
        }
        // SAFETY: unlocks the probe lock just acquired on `file`.
        unsafe {
            let _ = libc::flock(file.as_raw_fd(), libc::LOCK_UN);
        }
        false
    }
    #[cfg(not(unix))]
    {
        let _ = file;
        false
    }
}

#[async_trait]
pub trait ControlHandler: Send + Sync + 'static {
    async fn handle(&self, req: ControlRequest, shutdown: &Arc<Notify>) -> ControlResponse;
}

/// Binds `socket_path` (removing stale socket files if no live daemon answers
/// `Ping`) and serves concurrent newline-delimited JSON control frames until
/// `shutdown` fires.
pub async fn serve_control_socket<H: ControlHandler>(
    socket_path: &Path,
    handler: Arc<H>,
    shutdown: Arc<Notify>,
) -> std::io::Result<()> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let sd_watch = Arc::clone(&shutdown);
    let tx_watch = shutdown_tx.clone();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let forwarder = tokio::spawn(async move {
        let notified = sd_watch.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let _ = ready_tx.send(());
        notified.await;
        let _ = tx_watch.send(true);
    });
    let _ = ready_rx.await;
    let res =
        serve_control_socket_with_watch(socket_path, handler, shutdown, shutdown_tx, shutdown_rx)
            .await;
    forwarder.abort();
    res
}

/// Serves `socket_path` using a stateful `watch` shutdown channel so shutdown
/// signals sent before or outside the `accept()` poll are never dropped.
pub async fn serve_control_socket_with_watch<H: ControlHandler>(
    socket_path: &Path,
    handler: Arc<H>,
    shutdown: Arc<Notify>,
    shutdown_tx: watch::Sender<bool>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> std::io::Result<()> {
    if *shutdown_rx.borrow_and_update() {
        return Ok(());
    }
    if socket_path.exists() {
        if matches!(
            send_control_request_with_timeout(
                socket_path,
                &ControlRequest::Ping,
                STALE_SOCKET_PING_TIMEOUT,
            )
            .await,
            Ok(ControlResponse::Pong { .. })
        ) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AddrInUse,
                format!(
                    "jjfsd is already running on control socket {}",
                    socket_path.display()
                ),
            ));
        }
        let _ = std::fs::remove_file(socket_path);
    }
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
        }
    }

    let listener = UnixListener::bind(socket_path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o600))?;
    }

    loop {
        if *shutdown_rx.borrow_and_update() {
            let _ = std::fs::remove_file(socket_path);
            return Ok(());
        }
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow_and_update() {
                    let _ = std::fs::remove_file(socket_path);
                    return Ok(());
                }
            }
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(pair) => pair,
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            "transient error accepting control socket connection; continuing"
                        );
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                };
                let h = Arc::clone(&handler);
                let sd = Arc::clone(&shutdown);
                let sd_tx = shutdown_tx.clone();
                tokio::spawn(async move {
                    let _ = handle_client_stream(stream, h, sd, sd_tx).await;
                });
            }
        }
    }
}

async fn handle_client_stream<H: ControlHandler>(
    stream: UnixStream,
    handler: Arc<H>,
    shutdown: Arc<Notify>,
    shutdown_tx: watch::Sender<bool>,
) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let cred = stream.peer_cred()?;
        // SAFETY: `geteuid` is an infallible POSIX query.
        let expected_uid = unsafe { libc::geteuid() };
        if cred.uid() != expected_uid {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!(
                    "rejected control client with uid {} (expected {expected_uid})",
                    cred.uid()
                ),
            ));
        }
    }

    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    loop {
        let next = tokio::time::timeout(
            CLIENT_IDLE_TIMEOUT,
            read_bounded_line(&mut reader, MAX_CONTROL_FRAME_BYTES),
        )
        .await;
        let line = match next {
            Err(_) => return Ok(()),
            Ok(Ok(None)) => return Ok(()),
            Ok(Ok(Some(l))) => l,
            Ok(Err(err)) if err.kind() == std::io::ErrorKind::InvalidData => {
                let resp = ControlResponse::Error {
                    code: "frame_too_large".to_string(),
                    message: err.to_string(),
                };
                let mut bytes = serde_json::to_vec(&resp).unwrap_or_default();
                bytes.push(b'\n');
                let _ = writer.write_all(&bytes).await;
                let _ = writer.flush().await;
                return Ok(());
            }
            Ok(Err(err)) => return Err(err),
        };
        if line.trim().is_empty() {
            continue;
        }
        let response = match serde_json::from_str::<ControlRequest>(&line) {
            Ok(ControlRequest::Ping) => ControlResponse::Pong {
                pid: std::process::id(),
            },
            Ok(req) => {
                let is_shutdown = matches!(req, ControlRequest::Shutdown);
                let resp = handler.handle(req, &shutdown).await;
                if is_shutdown {
                    let mut bytes = serde_json::to_vec(&resp).unwrap_or_default();
                    bytes.push(b'\n');
                    let _ = writer.write_all(&bytes).await;
                    let _ = writer.flush().await;
                    let _ = shutdown_tx.send(true);
                    shutdown.notify_waiters();
                    return Ok(());
                }
                resp
            }
            Err(e) => ControlResponse::Error {
                code: "invalid_request".to_string(),
                message: e.to_string(),
            },
        };
        let mut bytes = serde_json::to_vec(&response).unwrap_or_default();
        bytes.push(b'\n');
        writer.write_all(&bytes).await?;
        writer.flush().await?;
    }
}

#[cfg(test)]
mod tests {
    use vfs_proto::send_control_request;

    use super::*;

    struct EchoHandler;

    #[async_trait]
    impl ControlHandler for EchoHandler {
        async fn handle(&self, req: ControlRequest, _shutdown: &Arc<Notify>) -> ControlResponse {
            match req {
                ControlRequest::Shutdown => ControlResponse::Ok {
                    message: "bye".into(),
                },
                _ => ControlResponse::Ok {
                    message: "ok".into(),
                },
            }
        }
    }

    #[test]
    fn instance_lock_prevents_concurrent_starts_and_releases_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!is_instance_lock_held(dir.path()));
        let guard = acquire_instance_lock(dir.path()).unwrap();
        assert!(is_instance_lock_held(dir.path()));

        let second = acquire_instance_lock(dir.path());
        assert!(second.is_err());
        assert_eq!(second.err().unwrap().kind(), std::io::ErrorKind::AddrInUse);

        drop(guard);
        assert!(!is_instance_lock_held(dir.path()));
        let _reacquired = acquire_instance_lock(dir.path()).unwrap();
    }

    #[tokio::test]
    async fn control_socket_rejects_oversized_frames_and_honors_watch_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("control.sock");
        let shutdown = Arc::new(Notify::new());
        let (tx, rx) = watch::channel(false);
        let sock_bg = sock.clone();
        let sd_bg = Arc::clone(&shutdown);
        let task = tokio::spawn(async move {
            serve_control_socket_with_watch(&sock_bg, Arc::new(EchoHandler), sd_bg, tx, rx)
                .await
                .unwrap();
        });

        for _ in 0..40 {
            if sock.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Send an oversized line (> MAX_CONTROL_FRAME_BYTES) and verify server
        // replies with `frame_too_large` without crashing!
        {
            let mut raw = UnixStream::connect(&sock).await.unwrap();
            let mut giant = vec![b'x'; MAX_CONTROL_FRAME_BYTES + 64];
            giant.push(b'\n');
            raw.write_all(&giant).await.unwrap();
            raw.flush().await.unwrap();
            let mut rdr = BufReader::new(raw);
            let reply_line = read_bounded_line(&mut rdr, MAX_CONTROL_FRAME_BYTES)
                .await
                .unwrap()
                .unwrap();
            let parsed: ControlResponse = serde_json::from_str(&reply_line).unwrap();
            match parsed {
                ControlResponse::Error { code, .. } => assert_eq!(code, "frame_too_large"),
                other => panic!("expected frame_too_large, got {other:?}"),
            }
        }

        // Server must still be alive and answer normal Ping + Shutdown!
        let pong = send_control_request(&sock, &ControlRequest::Ping)
            .await
            .unwrap();
        assert!(matches!(pong, ControlResponse::Pong { .. }));
        let stop = send_control_request(&sock, &ControlRequest::Shutdown)
            .await
            .unwrap();
        assert!(matches!(stop, ControlResponse::Ok { .. }));
        task.await.unwrap();
    }
}
