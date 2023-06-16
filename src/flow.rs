// flow

use crate::{graph::POPKey, includes::*, pop::POPGraph, stage::StageGraph, POP};

#[derive(Debug, Serialize, Deserialize)]
pub struct Flow {
    pub id: usize,

    #[serde(skip)]
    pub stage_graph: StageGraph,
}
