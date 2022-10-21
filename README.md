# Kueue

A robust, user-level, work-stealing distributed task scheduler.

## Build and install

Install Rust

    curl https://sh.rustup.rs -sSf | sh

Build in release mode

    git clone git@gitlab.kruecke.net:fklemme/kueue_rust.git
    cd kueue_rust
    cargo build --release

Install binaries

    install target/release/kueue_client ~/.local/bin/kueue
    install target/release/kueue_server ~/.local/bin/kueue_server
    install target/release/kueue_worker ~/.local/bin/kueue_worker

Upon first start of any binary, a template config file is created at `~/.config/kueue/config.toml`.
Make sure that the shared secreted in the that file is the same on all systems.
