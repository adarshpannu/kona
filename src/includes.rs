// includes.rs

pub use log::{error, warn, info, debug};

pub const DATADIR: &str = "/Users/adarshrp/Projects/flare/src/data";

pub type NodeId = usize;
pub type ColId = usize;
pub type PartitionId = usize;

pub use crate::Context;
