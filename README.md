# Kueue

A robust, user-level, work-stealing, distributed task scheduler.

## Why Kueue?

Kueue has been developed in a university research environment. Often, scientific
experiments are conducted by running commercial tools or custom scripts multiple
times, while each execution requires a certain amount of hardware resources and
run time. At the same time, the available computing infrastructure is
heterogenous, ranging from a few dedicated servers to a bunch of lab
workstations that might reboot from time to time. In such an environment,
distributing your workload to different machines can be a cumbersome task:
Which machines are currently free? How many jobs can I start/schedule on each
machine? Have my scripts completed or did the machine reboot in the meantime?
Kueue tries to alleviate these tasks while keeping the usability as simple as
possible. Running a job with Kueue should be as easy as running it on the
command line on your local machine. In practice, running `./my_script.py` with
Kueue on any free machine can be achieved with a simple `kueue cmd ./my_script.py`.

### Kueue might be for you, when...

- You only have user previledges on available computing machines.
- A simple command-line interface suffices your needs.
- You are working on machines that reboot or shutdown regularily.
- A simple setup is more important than a rich set of features.

### Kueue might not be for you, when...

- You have root previledges and can install an established (multi-user) scheduling system instead.
- You have no shared filesystem and thus need to attach data to your jobs inquiries.

## Installation

The simplest way to obtain Kueue is by downloading it directly from
[crates.io](https://crates.io/crates/kueue). This requires you to install Rust
first, which also needs no root previledges if the basic dependencies are
already installed. By default, all files will be installed into your home
directory. In an environment with synchronized home directories, this means that
you usually only need to go through the installation process once.

### Installing Rust

Make sure you have a C/C++ compiler installed. Then, install
[Rust](https://www.rust-lang.org/tools/install) as usual.

    curl https://sh.rustup.rs -sSf | sh

### Installing OpenSSL

You need to install OpenSSL headers as a dependency of Kueue. On many systems,
it might already be installed. On Ubuntu, the following packages will suffice:

    sudo apt install pkg-config libssl-dev

### Installing Kueue

Finally, use Cargo (which is included in the Rust installation) to install Kueue.

    cargo install kueue

This will install `kueue` (the client), `kueue_server`, and `kueue_worker` into
the `bin` folder of your Rust installation.

## Basic configuration

Upon the first start of any Kueue binary, a template config file is created at
`~/.config/kueue/config.toml`. It is worthwile to look at the default settings
and adjust them to your needs. A description of all settings can be found in the
[documentation](https://docs.rs/kueue/latest/kueue_lib/config).

The most important settings are in the `[common_settings]` section. Make sure
that the `shared_secret` in your config is the same on all systems you want to
use. The same is probably true for `server_name` and `server_port`, which is
used by clients and workers to connect to your server.

    common_settings
    shared_secret = "keep private!"
    server_name = "ralab29"
    server_port = 11236

To get started, run `kueue_server` on the machine you want to use a the server,
and `kueue_worker` on all machines you want to execute jobs on. Note that these
programs start in foreground, so you might use a tool like
[screen](https://linux.die.net/man/1/screen) to send the processes to the
background and keep them alive while you're not logged in.

## Restart workers

Kueue comes with a simple tool named `kueue_restart_workers` that checks the
state of your workers and attempts to restart them if they went down. To use the
tool, add a new block to your `config.toml` like the following:

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

Currently, the tool uses your SSH key to connect to the workers and spawns the
worker task in the background using [screen](https://linux.die.net/man/1/screen).
Make sure that screen is installed on your workers and ssh login via key is
possible. Then, you can use the tool like this:

    # Make sure your SSH key is loaded.
    eval `ssh-agent -s`
    ssh-add ~/.ssh/id_rsa
    # Spawn "restart_workers" in the background.
    screen kueue_restart_workers

Keep in mind that `kueue_restart_workers` is not required for Kueue to work but
just a simple tool to make restarting workers simpler. You can also use any
other strategy to start and restart your remote workers.
