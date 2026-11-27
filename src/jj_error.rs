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

use thiserror::Error;

#[derive(Error, Debug)]
pub enum JjError {
    #[error("Invalid path")]
    InvalidPath,

    #[error("Not found")]
    NotFound,

    #[error("Expected a directory")]
    NotADirectory,

    #[error("Expected a file")]
    NotAFile,

    #[error("Expected a symlink")]
    NotASymlink,

    #[error("Read-only filesystem path")]
    ReadOnly,

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Cross-workspace link or rename is not permitted")]
    CrossDevice,

    #[error("Directory is not empty")]
    DirectoryNotEmpty,

    #[error("Entry already exists")]
    AlreadyExists,

    #[error("Resource is busy")]
    Busy,

    #[error("Operation not supported")]
    Unsupported,

    #[error("{0}")]
    IO(#[from] std::io::Error),

    #[error("Underlying jj-lib error: {0}")]
    JjLibBackendError(#[from] jj_lib::backend::BackendError),

    #[error("Commit Cloud gRPC error: {0}")]
    Grpc(String),
}

impl From<tonic::Status> for JjError {
    fn from(status: tonic::Status) -> Self {
        match status.code() {
            tonic::Code::NotFound => Self::NotFound,
            tonic::Code::InvalidArgument => Self::InvalidPath,
            tonic::Code::AlreadyExists => Self::AlreadyExists,
            tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => {
                Self::PermissionDenied(format!("{}: {}", status.code(), status.message()))
            }
            _ => Self::Grpc(status.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grpc_auth_errors_map_to_permission_denied_with_server_message() {
        let unauth = JjError::from(tonic::Status::unauthenticated("token expired"));
        match &unauth {
            JjError::PermissionDenied(msg) => assert!(msg.contains("token expired"), "{msg}"),
            other => panic!("expected PermissionDenied, got {other:?}"),
        }
        assert_eq!(fuser::Errno::from(unauth), fuser::Errno::EACCES);

        let denied = JjError::from(tonic::Status::permission_denied("missing scope repo.read"));
        match &denied {
            JjError::PermissionDenied(msg) => {
                assert!(msg.contains("missing scope repo.read"), "{msg}");
            }
            other => panic!("expected PermissionDenied, got {other:?}"),
        }
        assert_eq!(fuser::Errno::from(denied), fuser::Errno::EACCES);
    }
}
