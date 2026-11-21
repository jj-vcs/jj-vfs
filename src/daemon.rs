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

//! Per-user `jjfsd` runtime (`DaemonRuntime`) coordinating `SourceRegistry`,
//! `BlobCas`, FUSE background sessions, and `control.sock` IPC commands.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Notify;
use vfs_proto::ControlRequest;
use vfs_proto::ControlResponse;
use vfs_proto::DEFAULT_MAX_CACHE_BYTES;
use vfs_proto::DaemonStatus;
use vfs_proto::InstancePaths;

use crate::cas::BlobCas;
use crate::ipc_server::ControlHandler;
use crate::jj_error::JjError;
use crate::registry::SourceRegistry;

pub struct DaemonRuntime {
    registry: Arc<SourceRegistry>,
}

impl DaemonRuntime {
    pub fn new(paths: InstancePaths) -> Result<Arc<Self>, JjError> {
        paths.ensure_dirs()?;
        let cas = Arc::new(BlobCas::new(&paths.cache_dir, DEFAULT_MAX_CACHE_BYTES)?);
        let registry = Arc::new(SourceRegistry::load(paths, cas)?);
        Ok(Arc::new(Self { registry }))
    }

    pub fn registry(&self) -> &Arc<SourceRegistry> {
        &self.registry
    }
}

#[async_trait]
impl ControlHandler for DaemonRuntime {
    async fn handle(&self, req: ControlRequest, _shutdown: &Arc<Notify>) -> ControlResponse {
        match req {
            ControlRequest::Ping => ControlResponse::Pong {
                pid: std::process::id(),
            },
            ControlRequest::Status => {
                let paths = self.registry.paths();
                let (cached_blobs, cached_bytes) = self.registry.cas().stats();
                ControlResponse::DaemonStatus(Box::new(DaemonStatus {
                    pid: std::process::id(),
                    instance: paths.instance.clone(),
                    state_dir: paths.state_dir.clone(),
                    cache_dir: paths.cache_dir.clone(),
                    config_path: paths.config_path.clone(),
                    cached_blobs,
                    cached_bytes,
                    max_cache_bytes: self.registry.cas().max_bytes(),
                    sources: self.registry.list_sources(),
                    mounts: Vec::new(),
                }))
            }
            ControlRequest::Shutdown => ControlResponse::Ok {
                message: "jjfsd shutting down".to_string(),
            },
            ControlRequest::AddSource { alias, config } => {
                match self.registry.add_source(alias.clone(), config) {
                    Ok(()) => ControlResponse::Ok {
                        message: format!("registered source '{alias}'"),
                    },
                    Err(e) => error_to_response(e),
                }
            }
            ControlRequest::RemoveSource { alias, force } => {
                match self.registry.remove_source(&alias, force, |_| false) {
                    Ok(()) => ControlResponse::Ok {
                        message: format!("removed source '{alias}'"),
                    },
                    Err(e) => error_to_response(e),
                }
            }
            ControlRequest::ListSources => ControlResponse::Sources {
                sources: self.registry.list_sources(),
            },
            ControlRequest::Mount { .. }
            | ControlRequest::Unmount { .. }
            | ControlRequest::SnapshotWorkspace { .. }
            | ControlRequest::CheckoutTree { .. } => ControlResponse::Error {
                code: "unimplemented".to_string(),
                message: "mount/snapshot handlers attach in commits 6-8".to_string(),
            },
        }
    }
}

pub fn error_to_response(err: JjError) -> ControlResponse {
    let code = match err {
        JjError::NotFound => "not_found",
        JjError::Busy => "ebusy",
        JjError::InvalidPath => "invalid_argument",
        JjError::ReadOnly => "erofs",
        JjError::PermissionDenied(_) => "eacces",
        _ => "internal",
    };
    ControlResponse::Error {
        code: code.to_string(),
        message: err.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use vfs_proto::SourceConfig;
    use vfs_proto::SourceKind;
    use vfs_proto::send_control_request;

    use super::*;
    use crate::ipc_server::serve_control_socket;

    #[allow(dead_code)]
    fn fuse_available() -> bool {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/fuse")
            .is_ok()
    }

    #[tokio::test]
    async fn control_socket_registers_lists_and_stops_daemon_cleanly() {
        let state_dir = tempfile::tempdir().unwrap();
        let paths = InstancePaths::resolve(Some("ipc-test"), Some(state_dir.path()));
        let runtime = DaemonRuntime::new(paths.clone()).unwrap();
        let shutdown = Arc::new(Notify::new());

        let sock = paths.socket_path.clone();
        let rt_clone = Arc::clone(&runtime);
        let sd_clone = Arc::clone(&shutdown);
        let server_task = tokio::spawn(async move {
            serve_control_socket(&sock, rt_clone, sd_clone)
                .await
                .unwrap();
        });

        for _ in 0..40 {
            if paths.socket_path.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let resp = send_control_request(
            &paths.socket_path,
            &ControlRequest::AddSource {
                alias: "oss".into(),
                config: SourceConfig {
                    kind: SourceKind::CommitCloud,
                    url: Some("http://127.0.0.1:8080".into()),
                    daemon_socket: None,
                    user: Some("alice".into()),
                    auth_token_env: None,
                    auth_token_cmd: vec![],
                    path: None,
                    search_paths: vec![],
                    repos: BTreeMap::new(),
                    ephemeral: false,
                },
            },
        )
        .await
        .unwrap();
        assert!(matches!(resp, ControlResponse::Ok { .. }));

        let listed = send_control_request(&paths.socket_path, &ControlRequest::ListSources)
            .await
            .unwrap();
        match listed {
            ControlResponse::Sources { sources } => assert!(sources.contains_key("oss")),
            other => panic!("unexpected response: {other:?}"),
        }

        let stop = send_control_request(&paths.socket_path, &ControlRequest::Shutdown)
            .await
            .unwrap();
        assert!(matches!(stop, ControlResponse::Ok { .. }));
        server_task.await.unwrap();
    }
}
