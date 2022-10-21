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
    install target/release/kueue_start_workers ~/.local/bin/kueue_start_workers

## Example configuration

Upon first start of any binary, a template config file is created at `~/.config/kueue/config.toml`.
Make sure that the shared secreted in the that file is the same on all systems.

    server_bind_address = "0.0.0.0"
    server_address = "ralab29"
    server_port = 11236
    shared_secret = "keep private!"

    [restart_workers]
    ssh_user = "klemmefn"
    hostnames = """
    rax11   rax17   rax19   rax32
    ralab04 ralab06 ralab07 ralab08
    ralab10 ralab11 ralab13 ralab14
    ralab16 ralab18 ralab22 ralab23
    ralab24 ralab25 ralab26 ralab27
    """
