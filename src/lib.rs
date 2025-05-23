#![deny(missing_docs)]
//! A simple key/value store.

use failure::Error;
pub use kv::KvStore;

/// abc
pub type Result<T> = std::result::Result<T, Error>;

mod kv;
