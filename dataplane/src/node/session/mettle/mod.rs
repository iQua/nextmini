//! Internal METTLE codec core.
//!
//! This module is intentionally transport-agnostic. It does not depend on
//! message wire types, async runtime pieces, or block-FEC structures.

#![cfg_attr(not(test), allow(dead_code))]

pub(crate) mod decoder;
pub(crate) mod encoder;
pub(crate) mod hash;
pub(crate) mod params;
pub(crate) mod sim;
