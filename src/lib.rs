//! Wrap bytes IO in length prefixed framing. Length is little endian 24 bit unsigned integer.
#![warn(
    nonstandard_style,
    unreachable_pub,
    missing_debug_implementations,
    missing_docs,
    redundant_lifetimes,
    unsafe_code,
    non_local_definitions,
    clippy::needless_pass_by_value,
    clippy::needless_pass_by_ref_mut
)]

#[cfg(test)]
mod duplex;
mod framing;
mod util;

pub use framing::{FramedWithErrors, Uint24LELengthPrefixedFraming};
