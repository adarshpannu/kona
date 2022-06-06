// flow

use crate::{graph::*, includes::*, pop::*, stage::*};

#[derive(Debug, Serialize, Deserialize)]
pub struct Flow {
    pub pop_graph: POPGraph,
    pub stage_mgr: StageManager,
}

impl Flow {
    pub fn get_node(&self, pop_key: POPKey) -> &POP {
        &self.pop_graph.get(pop_key).value
    }
}

