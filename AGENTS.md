# Agent Instructions

Use these rules when reading, editing, testing, or reviewing code in [jj-vfs](https://github.com/jj-vcs/jj-vfs). When drafting or editing design documents, RFCs, or architecture guides under `docs/` or [README.md](README.md), also follow [docs/AGENTS.md](docs/AGENTS.md).

## Workspace Crates and Binary Surfaces

* **VFS Library ([src/lib.rs](src/lib.rs), `jj-vfs`):** Implements virtual filesystem inode mapping, path routing, tree/blob hydration, and `fuser::Filesystem` handlers backed by `jj_lib` repositories.
* **FUSE Daemon Binary ([src/main.rs](src/main.rs), `jjfsd`):** Mounts and serves on-demand virtual checkouts over Linux FUSE.

## Codebase and Comment Conventions

* **Match Implementation Scope:** Keep code changes focused on the requested behavior and active FUSE/backend surface without introducing speculative distributed machinery unless specified by the task or a design doc in `docs/`.
* **Module-Level `//!` Invariant Headers in Rust:** Every crate root (`lib.rs`) and core domain module must begin with a concise 15-to-25 line `//!` docblock stating facts that cannot be inferred from the local AST alone:
  * **Authority and Crash-Safety Ordering:** Write, journal, and snapshot ordering across local overlay files, `OpStore` commits, and working-copy state, and how interrupted operations recover.
  * **Concurrency and Invalidation Invariants:** Inode-table locking, kernel entry/attribute TTL assumptions, and cache invalidation ordering.
  * **FUSE, IPC, and RPC Boundaries:** Kernel FUSE reply contracts (`ENOENT`, `EROFS`, `EIO`), local socket authentication, and remote backend retry guarantees.
* **Proto-First IPC/RPC Contracts:** When defining `.proto` schemas for daemon control or remote backends, document idempotency, cursor invariants, and status codes on `service`, `rpc`, and `message` definitions rather than burying them in handler code.
* **Write "Why", Not "What" Comments:** Omit line-by-line comments that restate Rust syntax. Keep `AGENTS.md` files under 1,500 tokens with no manual line-number maps or perishable file lists.
* **Actionable CLI and Daemon Errors:** Teach [jj](https://github.com/jj-vcs/jj)'s mental model in user-facing errors - working-copy-as-a-commit (`@`), bookmarks, algebraic conflicts (`Merge<TreeId>`), and the operation log (`jj op log` and `jj undo`) - with runnable remediation commands.

## Version Control Hygiene With jj

* **No Co-Author or Attribution Trailers:** Commit messages carry no `Co-authored-by:` trailer and no other trailer that credits a tool, a coding agent, or a person. Every author of a commit must have signed the Google CLA, and the CLA check blocks the merge of a pull request whose commits name an author who has not, including a coding agent. Remove any such trailer that a tool adds by default; fix an existing commit with `jj describe` before pushing.
* **Auto-Format Modified Files With `jj fix`:** Run `jj fix -s @` - or `jj fix -s <base>` across a commit stack - after editing Rust source files so formatting stays scoped to modified files without introducing rebase conflicts, and reserve `cargo +nightly fmt --all` for standalone workspace-wide formatting commits. Configure `fix.tools.rustfmt` once per checkout to match [rustfmt.toml](rustfmt.toml) and CI with `jj config set --repo fix.tools.rustfmt '{ command = ["rustfmt", "+nightly", "--emit", "stdout"], patterns = ["glob:**/*.rs"] }'`.
* **Split and Describe Task Changes:** After changing tracked files, inspect `jj status` and run `jj split -m "<concise description>" -- <explicit paths>`. Pass each task path by name, including both paths for renames or moves, so unrelated changes stay in `@`.
