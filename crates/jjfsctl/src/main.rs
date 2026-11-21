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

//! Control CLI (`jj-vfs`) for managing per-user `jjfsd` instances, registered
//! Commit Cloud and local repository sources, and FUSE mounts.

use std::path::PathBuf;

use clap::Parser;
use clap::Subcommand;
use vfs_proto::CallerContext;
use vfs_proto::InstancePaths;

#[derive(Parser, Debug)]
#[command(
    name = "jj-vfs",
    about = "Manage jjfsd virtual filesystem sources and mounts"
)]
struct Cli {
    /// Named jjfsd instance (defaults to "default" or JJ_VFS_INSTANCE)
    #[arg(long, global = true)]
    instance: Option<String>,

    /// Explicit state directory override (defaults to XDG paths or
    /// JJ_VFS_STATE_DIR)
    #[arg(long, global = true)]
    state_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Print resolved runtime paths and Commit Cloud wire digest parameters
    Paths {
        #[arg(long)]
        json: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let paths = InstancePaths::resolve(cli.instance.as_deref(), cli.state_dir.as_deref());
    let caller = CallerContext::current_process();
    match cli.command {
        Commands::Paths { json } => {
            let max_msg = cc_common::MAX_GRPC_MESSAGE_BYTES;
            let pooled = cc_lib::util::cached_channel_count();
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "instance": paths.instance,
                        "state_dir": paths.state_dir,
                        "cache_dir": paths.cache_dir,
                        "config_path": paths.config_path,
                        "socket_path": paths.socket_path,
                        "uid": caller.uid,
                        "max_grpc_message_bytes": max_msg,
                        "pooled_channels": pooled,
                    })
                );
            } else {
                println!("instance:    {}", paths.instance);
                println!("state_dir:   {}", paths.state_dir.display());
                println!("cache_dir:   {}", paths.cache_dir.display());
                println!("config_path: {}", paths.config_path.display());
                println!("socket_path: {}", paths.socket_path.display());
            }
        }
    }
    Ok(())
}
