# Kueue

A robust, user-level, work-stealing distributed task scheduler.

This tool is still in early development. More details and documentation will follow.

## Installation

The simplest way to obtain Kueue is by downloading it directly from [crates.io](https://crates.io/crates/kueue).
This can be achieved with the following two commands.

### Install Rust

Make sure you have a C/C++ compiler installed.

    curl https://sh.rustup.rs -sSf | sh

### Install Kueue

You might need to install OpenSSL headers as well.

    cargo install kueue

This will install `kueue` (the client), `kueue_server`, and `kueue_worker` into the `bin` folder of your Rust installation.

## Basic configuration

Upon first start of any Kueue binary, a template config file is created at `~/.config/kueue/config.toml`.
Make sure that the shared secret in that file is the same on all systems you want to use.

    log_level = "info"
    server_binds = "0.0.0.0 [::]"
    server_name = "ralab29"
    server_port = 11236
    shared_secret = "keep private!"

## Restart workers

Kueue comes with a simple tool that checks the state of your workers and attempts to restart them if they went down.
First, add a new block to your `config.toml` like the following:

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

Currently, the tool uses your SSH key to connect to the workers and spawns the worker task using [screen](https://linux.die.net/man/1/screen). Make sure that screen is installed on your workers and ssh login via key is possible. Then, you can use the tool like this:

    eval `ssh-agent -s`
    ssh-add ~/.ssh/id_rsa
    screen kueue_restart_workers
