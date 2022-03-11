// includes.rs

pub use log::{error, warn, info, debug};

//pub const DATADIR: &str = "/Users/adarshrp/Projects/flare/data";
pub const DATADIR: &str = "/Users/adarshrp/Projects/tpch-data/sf0.01";
pub const TEMPDIR: &str = "/Users/adarshrp/Projects/flare/temp";
pub const GRAPHVIZDIR: &str = "/Users/adarshrp/Projects/flare";

pub type FlowNodeId = usize;
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

pub use typed_arena::Arena;

pub type NodeArena = Arena<crate::flow::FlowNode>;

pub type QueryBlockLink = std::rc::Rc<std::cell::RefCell<crate::qgm::QueryBlock>>;

pub use std::mem::replace;
