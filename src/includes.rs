// includes.rs

pub use log::{error, warn, info, debug};

//pub const DATADIR: &str = "/Users/adarshrp/Projects/flare/data";
pub const DATADIR: &str = "/Users/adarshrp/Projects/tpch-data/sf0.01";
pub const TEMPDIR: &str = "/Users/adarshrp/Projects/flare/temp";
pub const GRAPHVIZDIR: &str = "/Users/adarshrp/Projects/flare";

pub type FlowNodeId = usize;
pub type ColId = usize;
pub type QunId = usize;
pub type PartitionId = usize;

pub type ColMap = std::collections::HashMap<ColId, usize>;

pub use crate::Env;
use crate::graph::NodeId;
use crate::row::DataType;

pub use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TextFilePartition(pub u64, pub u64);

pub use typed_arena::Arena;

pub type NodeArena = Arena<crate::flow::FlowNode>;

pub type QueryBlockLink = std::rc::Rc<std::cell::RefCell<crate::ast::QueryBlock>>;

macro_rules! mkrcrc {
    ($arg:expr) => {{
        Rc::new(RefCell::new($arg))
    }};
}

