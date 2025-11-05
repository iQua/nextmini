pub mod interface;

#[cfg(target_os = "linux")]
pub mod reader_tso;
#[cfg(target_os = "linux")]
pub mod writer_tso;

#[cfg(not(target_os = "linux"))]
pub mod reader;
#[cfg(not(target_os = "linux"))]
pub mod writer;
