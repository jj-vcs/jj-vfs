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

    #[error("{0}")]
    IO(#[from] std::io::Error),

    #[error("Underlying jj-lib error: {0}")]
    JjLibBackendError(#[from] jj_lib::backend::BackendError),
}
