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
    /// Check workspace health
    Check,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Check => {
            println!("jj-vfs xtask workspace ready");
            Ok(())
        }
    }
}
