# Kueue

A robust, user-level, work-stealing, and distributed task scheduler.

This tool is still in early development. More details and documentation will follow.

## Installation

The simplest way to obtain Kueue is by downloading it directly from [crates.io](https://crates.io/crates/kueue).
This can be achieved with the following two commands.

### Install Rust

Make sure you have a C/C++ compiler installed. Then, install Rust as usual.

    curl https://sh.rustup.rs -sSf | sh

The command is taken from the [Rust](https://www.rust-lang.org/tools/install) website.

### Install OpenSSL

You need to install OpenSSL headers as a dependency of Kueue. On Ubuntu, the following will suffice:

    sudo apt install pkg-config libssl-dev

### Install Kueue

Use Cargo (which is included in the Rust installation) to install Kueue.

    cargo install kueue

This will install `kueue` (the client), `kueue_server`, and `kueue_worker` into the `bin` folder of your Rust installation.

## Basic configuration

Upon first start of any Kueue binary, a template config file is created at `~/.config/kueue/config.toml`.
Make sure that the shared secret in that file is the same on all systems you want to use.

    [common]
    shared_secret = "keep private!"
    server_name = "ralab29"
    server_port = 11236
    log_level = "info"

    [server]
    address_bindings = "0.0.0.0 [::]"

## Restart workers

Kueue comes with a simple tool named `kueue_restart_workers` that checks the state of your workers and attempts to restart them if they went down.
To use the tool, add a new block to your `config.toml` like the following:

    [restart_workers]
    ssh_user = "klemmefn"
    hostnames = """
    rax11   rax17   rax19   rax32
    ralab04 ralab06 ralab07 ralab08
    ralab10 ralab11 ralab13 ralab14
    ralab16 ralab18 ralab22 ralab23
    ralab24 ralab25 ralab26 ralab27
    """
    sleep_minutes_before_recheck = 60

Currently, the tool uses your SSH key to connect to the workers and spawns the worker task in the background using [screen](https://linux.die.net/man/1/screen). Make sure that screen is installed on your workers and ssh login via key is possible. Then, you can use the tool like this:

    # Make sure your SSH key is loaded.
    eval `ssh-agent -s`
    ssh-add ~/.ssh/id_rsa
    # Spawn "restart_workers" in the background.
    screen kueue_restart_workers

Keep in mind that `kueue_restart_workers` is not required for Kueue to work but just a simple tool to make restarting workers simpler. You can also use any other strategy to start and restart your remote workers.
