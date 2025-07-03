#![no_std]

#[repr(C)]
#[derive(Copy, Clone)]
/// define 4 tuple of the socket as key
pub struct SockKey {
    pub remote_ip4: u32,
    pub local_ip4: u32,
    pub remote_port: u32,
    pub local_port: u32,
}

#[cfg(feature = "user")]
unsafe impl aya::Pod for SockKey {}
