#![no_std]

#[derive(Copy, Clone)]
#[repr(C)]
pub struct SockKey {
    pub sip4: u32,     // Source IP
    pub dip4: u32,     // Destination IP  
    pub family: u8,    // Protocol family
    pub sport: u32,    // Source port
    pub dport: u32,    // Destination port
}

#[cfg(feature = "user")]
unsafe impl aya::Pod for SockKey {}
