pub mod interface;

#[cfg(all(not(feature = "python-api"), target_os = "linux"))]
pub mod reader_tso;
#[cfg(all(not(feature = "python-api"), target_os = "linux"))]
pub mod writer_tso;

#[cfg(all(not(feature = "python-api"), not(target_os = "linux")))]
pub mod reader;
#[cfg(all(not(feature = "python-api"), not(target_os = "linux")))]
pub mod writer;
