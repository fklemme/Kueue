// Shared code between all executables

pub mod constants {
    use std::net::Ipv4Addr;

    // TODO: Move to some kind of config.
    pub const DEFAULT_BIND_ADDR: Ipv4Addr = Ipv4Addr::new(0, 0, 0, 0);
    pub const DEFAULT_SERVER_ADDR: Ipv4Addr = Ipv4Addr::new(129,69,183,89);
    pub const DEFAULT_PORT: u16 = 11236;
}

pub mod messages;
pub mod structs;
