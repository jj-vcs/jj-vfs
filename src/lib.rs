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

// test_helpers.rs is also compiled into the integration tests as a module of
// that crate, so it names this crate as `jjfsd`, which works in both places.
#[cfg(test)]
extern crate self as jjfsd;

pub mod commit_tree_file;
pub mod commits_directory;
pub mod fuse;
pub mod inode_map;
pub mod jj_error;
pub mod path_mapper;
pub mod path_mapper_all_commits;
pub mod repo_loading;
pub mod static_directory;
#[cfg(test)]
pub mod test_helpers;
pub mod vfs;
pub mod virtual_file;
pub mod workspaces_directory;
