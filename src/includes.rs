// includes.rs

pub use log::{error, warn, info, debug};

//pub const DATADIR: &str = "/Users/adarshrp/Projects/flare/data";
pub const DATADIR: &str = "/Users/adarshrp/Projects/tpch-data/sf0.01";
pub const TEMPDIR: &str = "/Users/adarshrp/Projects/flare/temp";
pub const GRAPHVIZDIR: &str = "/Users/adarshrp/Projects/flare";

pub type ColId = usize;
pub type QunId = usize;
pub type QBId = usize;
pub type PartitionId = usize;

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug, PartialOrd, Ord)]
pub struct QunCol(pub QunId, pub ColId);

pub type ColMap = std::collections::HashMap<ColId, ColId>;

pub use crate::Env;

pub use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TextFilePartition(pub u64, pub u64);

pub use std::mem::replace;

pub fn enquote(s: &String) -> String {
    format!("\"{}\"", s)
}

pub fn remove_quotes(s: &str) -> String {
    s.replace("\"", "")
}

pub fn stringify<E: std::fmt::Debug>(e: E) -> String {
    format!("xerror: {:?}", e)
}

pub fn yes_or_no(s: &str) -> Option<bool> {
    match s {
        "Y" | "YES" => Some(true),
        "N" | "NO" => Some(false),
        _ => None
    }
}
