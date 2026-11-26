{
  # Development shell for jj-vfs, for the developers who use Nix.
  #
  #   nix develop          # or, with direnv: echo 'use flake' > .envrc && direnv allow
  #   cargo build
  #   cargo fmt --all      # nightly rustfmt, as in CI
  #
  # The Rust toolchain comes from rust-toolchain.toml, as it does for rustup
  # users: the latest stable release in flake.lock. Run `nix flake update` to
  # move to a newer one.
  description = "jj-vfs: FUSE virtual filesystem for jj, development shell";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    nixpkgs,
    rust-overlay,
    ...
  }: let
    systems = ["x86_64-linux" "aarch64-linux"];
    forAllSystems = f:
      nixpkgs.lib.genAttrs systems (system:
        f (import nixpkgs {
          inherit system;
          overlays = [(import rust-overlay)];
        }));
  in {
    devShells = forAllSystems (pkgs: let
      # Everything rust-toolchain.toml asks for, plus the sources and
      # rust-analyzer for editors.
      toolchain = (builtins.fromTOML (builtins.readFile ./rust-toolchain.toml)).toolchain;
      rust = pkgs.rust-bin.fromRustupToolchain (toolchain
        // {
          components = (toolchain.components or []) ++ ["rust-src" "rust-analyzer"];
        });

      # rustfmt.toml uses options that only nightly rustfmt applies, and CI
      # checks with cargo +nightly fmt. Listed before the stable toolchain so
      # that rustfmt and cargo-fmt resolve to it; RUSTFMT also covers a
      # cargo-fmt found in ~/.cargo/bin, which cargo searches before PATH.
      rustfmtNightly = pkgs.rust-bin.selectLatestNightlyWith (t: t.rustfmt);

      # The shell's sccache: its own local disk cache and server socket, and
      # no other SCCACHE_* setting from the environment or a config file
      # (such as a shared remote cache), except the size limit and logging.
      sccacheShell = pkgs.writeShellScriptBin "sccache" ''
        dir="''${DEVSHELL_SCCACHE_DIR:?run this through the development shell}"
        for v in "''${!SCCACHE_@}"; do
          case "$v" in
            SCCACHE_CACHE_SIZE | SCCACHE_IDLE_TIMEOUT | SCCACHE_LOG | SCCACHE_ERROR_LOG) ;;
            SCCACHE_*) unset "$v" ;;
          esac
        done
        export SCCACHE_CONF=/dev/null SCCACHE_DIR="$dir" SCCACHE_SERVER_UDS="$dir/server.sock"
        exec ${pkgs.sccache}/bin/sccache "$@"
      '';
    in {
      default = pkgs.mkShell {
        packages = [
          rustfmtNightly
          rust
          pkgs.cargo-nextest
          pkgs.git # for git in a pure (nix develop -i) shell
        ];

        RUSTFMT = "${rustfmtNightly}/bin/rustfmt";

        # sccache wraps every rustc call. An sccache client fails against a
        # server of another version (a protocol error), and by default all
        # of them meet on TCP port 4226, where another sccache (say, the
        # system's) may already run a server. So this shell's sccache gets a
        # server, socket, and local disk cache of its own version, in a 0700
        # directory that no other user can create or take over. Every shell
        # shares one server.
        #
        # Those settings live in the wrapper script above, not in the shell's
        # environment, and the shell's sccache is not on PATH. So any other
        # sccache that runs in the shell (a plain `sccache`, or one that a
        # cargo config names while the shell's sccache is off) keeps its own
        # server and cache. Statistics of the shell's sccache:
        # "$RUSTC_WRAPPER" --show-stats.
        #
        # The shell turns sccache on when RUSTC_WRAPPER is unset or names
        # sccache. It then overrides any rustc-wrapper in your cargo config,
        # because cargo ranks the environment variable first. An empty
        # RUSTC_WRAPPER (RUSTC_WRAPPER= nix develop) builds without it, and
        # any other wrapper in RUSTC_WRAPPER is left alone. Without HOME or an
        # absolute XDG_CACHE_HOME (as in nix develop -i), when the cache
        # directory or its parent is unsafe, or when the socket path would be
        # too long, the shell changes nothing.
        shellHook = ''
          case "''${RUSTC_WRAPPER-unset}" in
            unset | sccache | */sccache) _sc_root="''${XDG_CACHE_HOME:-''${HOME:+$HOME/.cache}}" ;;
            *) _sc_root= ;;
          esac
          case "$_sc_root" in /*) ;; *) _sc_root= ;; esac
          _sc_dir="$_sc_root/sccache-${pkgs.sccache.version}"
          if [ -n "$_sc_root" ] && [ "$(printf %s "$_sc_dir" | wc -c)" -le 90 ] \
            && mkdir -p "$_sc_root" 2>/dev/null && [ -O "$_sc_root" ] \
            && [ -z "$(find "$_sc_root" -maxdepth 0 -perm /022)" ] \
            && mkdir -p -m 0700 "$_sc_dir" 2>/dev/null \
            && [ ! -L "$_sc_dir" ] && [ -O "$_sc_dir" ] && chmod 0700 "$_sc_dir"; then
            export RUSTC_WRAPPER="${sccacheShell}/bin/sccache"
            export DEVSHELL_SCCACHE_DIR="$_sc_dir"
          fi
          unset _sc_root _sc_dir
        '';

        # Mounting needs fusermount3 from the system (setuid on NixOS:
        # programs.fuse), so it is not provided here.
      };
    });
  };
}
