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

//! Custom `jj` CLI binary registering `VfsWorkingCopyFactory` (`"vfs"`),
//! `CommitCloudWorkingCopyFactory` (`"commit_cloud"`), `CommitCloud` storage
//! factories, and the `jj vfs` subcommand family.

use std::collections::HashMap;
use std::process::ExitCode;

use cc_lib::StoreFactoriesExt as _;
use cc_lib::cc_working_copy::CommitCloudWorkingCopy;
use cc_lib::cc_working_copy::CommitCloudWorkingCopyFactory;
use jj_cli::cli_util::CliRunner;
use jj_cli::cli_util::CommandHelper;
use jj_cli::command_error::CommandError;
use jj_cli::command_error::user_error;
use jj_cli::ui::Ui;
use jj_lib::config::ConfigLayer;
use jj_lib::config::ConfigSource;
use jj_lib::repo::StoreFactories;
use jj_lib::workspace::WorkingCopyFactories;
use jjfsctl::VfsCommandArgs;
use jjfsctl::execute_vfs_command;
use jjfsd::vfs_working_copy::VfsWorkingCopy;
use jjfsd::vfs_working_copy::VfsWorkingCopyFactory;

#[derive(clap::Subcommand, Clone, Debug)]
enum CustomCommands {
    /// Manage `jjfsd`, multi-source repositories, and FUSE mounts
    Vfs(VfsCommandArgs),
}

fn create_store_factories() -> StoreFactories {
    let mut factories = StoreFactories::empty();
    factories.add_commit_cloud();
    factories
}

fn create_working_copy_factories() -> WorkingCopyFactories {
    let mut factories = HashMap::new();
    factories.insert(
        CommitCloudWorkingCopy::name().to_string(),
        Box::new(CommitCloudWorkingCopyFactory::new())
            as Box<dyn jj_lib::working_copy::WorkingCopyFactory>,
    );
    factories.insert(
        VfsWorkingCopy::name().to_string(),
        Box::new(VfsWorkingCopyFactory::new()) as Box<dyn jj_lib::working_copy::WorkingCopyFactory>,
    );
    factories
}

fn default_vfs_config() -> ConfigLayer {
    ConfigLayer::parse(
        ConfigSource::Default,
        "revset-aliases.'trunk()' = \"root()\"\n",
    )
    .expect("default jj-vfs config layer should parse")
}

async fn run_custom_command(
    _ui: &mut Ui,
    _command_helper: &CommandHelper,
    command: CustomCommands,
) -> Result<(), CommandError> {
    match command {
        CustomCommands::Vfs(args) => execute_vfs_command(&args)
            .await
            .map_err(|e| user_error(format!("{e:#}"))),
    }
}

fn main() -> ExitCode {
    CliRunner::init()
        .add_extra_config(default_vfs_config())
        .add_store_factories(create_store_factories())
        .add_working_copy_factories(create_working_copy_factories())
        .add_subcommand(run_custom_command)
        .run()
        .into()
}
