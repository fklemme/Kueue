// Shared code between all executables

pub mod constants {
    use std::net::Ipv4Addr;

    pub const DEFAULT_BIND_ADDR: Ipv4Addr = Ipv4Addr::new(0, 0, 0, 0);
    pub const DEFAULT_PORT: u16 = 8765;
}

pub mod messages;
pub mod structs;