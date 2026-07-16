pub mod block;
mod decoder;
mod encoder;
mod params;
pub mod stream;
#[doc(hidden)]
pub mod test_support;
#[cfg(test)]
mod validation;

pub use params::{MettleParams, OverheadRatio, ParamsError};
pub use stream::DecoderBuildError;
