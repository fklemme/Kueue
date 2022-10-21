# Kueue

A robust, user-level, work-stealing distributed task scheduler.

## Build

Install Rust

    curl https://sh.rustup.rs -sSf | sh

Build and install Kueue client

    git clone git@gitlab.kruecke.net:fklemme/kueue_rust.git
    cd kueue_rust
    cargo build --release
    install target/release/kueue_client ~/.local/bin/kueue
