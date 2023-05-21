// flow

use crate::{graph::POPKey, includes::*, pop::POPGraph, stage::StageGraph, POP};

#[derive(Debug, Serialize, Deserialize)]
pub struct Flow {
    pub pop_graph: POPGraph,
    pub stage_graph: StageGraph,
}

impl Flow {
    pub fn get_node(&self, pop_key: POPKey) -> &POP {
        &self.pop_graph.get(pop_key).value
    }
}
